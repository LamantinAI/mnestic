/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{atomic::Ordering, Arc, Mutex, MutexGuard},
    thread,
};

use miette::{bail, Result};
use tikv_client::{Snapshot, Transaction};
use tokio::runtime::Handle;

use super::{
    admission::WritePermit,
    blocking, ensure_blocking_context,
    error::{commit, is_unknown},
    scan::{BatchScanner, ValidityScanner},
    Inner,
};
use crate::{
    data::{tuple::Tuple, value::ValidityTs},
    runtime::relation::try_decode_tuple_from_kv,
    storage::StoreTx,
};

pub(super) struct TxState {
    pub inner: Arc<Inner>,
    pub tx: Transaction,
    pub snapshot: Snapshot,
    pub pending: BTreeSet<Vec<u8>>,
    changed: BTreeSet<Vec<u8>>,
    pub write: bool,
    pub failed: bool,
    pub finished: bool,
    _permit: Option<WritePermit>,
}

impl TxState {
    pub fn check(&self) -> Result<()> {
        ensure_blocking_context()?;
        self.inner.check_lease()?;
        if self.finished {
            bail!("TiKV transaction is already finished");
        }
        if self.failed {
            bail!("TiKV transaction contains a failed operation; abort required");
        }
        Ok(())
    }
    fn writable(&self) -> Result<()> {
        self.check()?;
        if !self.write {
            bail!("write attempted in a read-only TiKV transaction");
        }
        Ok(())
    }
}

impl Drop for TxState {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // tikv-client treats dropping an active transaction as a programming
        // error and panics. Query execution can return before commit (for
        // example, after a transient lock error), so explicitly release any
        // pessimistic locks and leave the client in a terminal state.
        if Handle::try_current().is_ok() {
            thread::scope(|scope| {
                let _ = scope.spawn(|| blocking(self.tx.rollback())).join();
            });
        } else {
            let _ = blocking(self.tx.rollback());
        }
    }
}

pub struct TiKvTx {
    pub(super) state: Arc<Mutex<TxState>>,
}

impl TiKvTx {
    pub(super) fn new(
        inner: Arc<Inner>,
        tx: Transaction,
        snapshot: Snapshot,
        write: bool,
        permit: Option<WritePermit>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(TxState {
                inner,
                tx,
                snapshot,
                pending: BTreeSet::new(),
                changed: BTreeSet::new(),
                write,
                failed: false,
                finished: false,
                _permit: permit,
            })),
        }
    }
    fn lock(&self) -> MutexGuard<'_, TxState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }
    fn modify(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut state = self.lock();
        let result = (|| {
            state.writable()?;
            let physical = state.inner.key(key)?;
            let changed = physical.clone();
            if let Some(value) = value {
                if value.len() > state.inner.config.max_value_bytes {
                    bail!("TiKV value exceeds configured limit");
                }
                blocking(state.tx.put(physical, value.to_vec()))?;
            } else {
                blocking(state.tx.delete(physical))?;
            }
            state.pending.insert(key.to_vec());
            state.changed.insert(changed);
            Ok(())
        })();
        state.failed |= result.is_err();
        result
    }
}

pub(super) fn response_too_large(error: &miette::Report) -> bool {
    let text = format!("{error:?}").to_ascii_lowercase();
    text.contains("message length too large")
        || text.contains("decoded message length")
        || text.contains("message exceeds maximum size")
}

impl<'s> StoreTx<'s> for TiKvTx {
    fn get(&self, key: &[u8], for_update: bool) -> Result<Option<Vec<u8>>> {
        let mut state = self.lock();
        state.check()?;
        let key = state.inner.key(key)?;
        if for_update {
            state.writable()?;
            let result = blocking(state.tx.get_for_update(key.clone()));
            state.failed |= result.is_err();
            let latest = result?;
            // Pessimistic get-for-update reads the server's current value and
            // bypasses the SDK write buffer. Own mutations must still win.
            if state.changed.contains(&key) {
                blocking(state.tx.get(key))
            } else {
                Ok(latest)
            }
        } else {
            blocking(state.tx.get(key))
        }
    }

    fn multi_get(&self, keys: &[Vec<u8>], for_update: bool) -> Result<Vec<Option<Vec<u8>>>> {
        let mut state = self.lock();
        state.check()?;
        if for_update {
            state.writable()?;
        }
        let physical = keys
            .iter()
            .map(|k| state.inner.key(k))
            .collect::<Result<Vec<_>>>()?;
        let unique = physical
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut values = BTreeMap::new();
        let mut offset = 0;
        let mut batch = state.inner.config.scan_batch_size as usize;
        while offset < unique.len() {
            let end = unique.len().min(offset + batch);
            let result = if for_update {
                blocking(state.tx.batch_get_for_update(unique[offset..end].to_vec()))
                    .map(|it| it.into_iter().collect::<Vec<_>>())
            } else {
                blocking(state.tx.batch_get(unique[offset..end].to_vec()))
                    .map(|it| it.into_iter().collect::<Vec<_>>())
            };
            match result {
                Ok(pairs) => {
                    for pair in pairs {
                        values.insert(Vec::<u8>::from(pair.0), pair.1);
                    }
                    offset = end;
                }
                Err(error) if batch > 1 && response_too_large(&error) => {
                    batch = (batch / 2).max(1);
                }
                Err(error) => {
                    state.failed |= for_update;
                    return Err(error);
                }
            }
        }
        if for_update {
            let changed = unique
                .iter()
                .filter(|k| state.changed.contains(*k))
                .cloned()
                .collect::<Vec<_>>();
            for key in &changed {
                values.remove(key);
            }
            for keys in changed.chunks(batch) {
                let pairs = blocking(state.tx.batch_get(keys.to_vec()))?;
                for pair in pairs {
                    values.insert(Vec::<u8>::from(pair.0), pair.1);
                }
            }
        }
        Ok(physical.iter().map(|k| values.get(k).cloned()).collect())
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.modify(key, Some(value))
    }
    fn supports_par_put(&self) -> bool {
        true
    }
    fn par_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.modify(key, Some(value))
    }
    fn del(&mut self, key: &[u8]) -> Result<()> {
        self.modify(key, None)
    }
    fn par_del(&self, key: &[u8]) -> Result<()> {
        self.modify(key, None)
    }
    fn exists(&self, key: &[u8], for_update: bool) -> Result<bool> {
        self.get(key, for_update).map(|v| v.is_some())
    }

    fn del_range_from_persisted(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        self.lock().writable()?;
        // Scan the start snapshot, not the transaction's buffered writes. Keep
        // both new keys and pending overwrites/deletes untouched, like RocksDB.
        let scanner = BatchScanner::new(self.state.clone(), lower, Some(upper), true);
        for item in scanner {
            let (key, _) = item?;
            let mut state = self.lock();
            state.writable()?;
            if !state.pending.contains(&key) {
                let physical = state.inner.key(&key)?;
                let changed = physical.clone();
                if let Err(error) = blocking(state.tx.delete(physical)) {
                    state.failed = true;
                    return Err(error);
                }
                state.changed.insert(changed);
            }
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let mut state = self.lock();
        state.check()?;
        if let Some(lease) = &state.inner.lease {
            lease.before_commit()?;
        }
        // Never replay a transaction or roll back an unknown commit outcome.
        // The SDK error is retained as the diagnostic's source.
        match commit(&mut state.tx) {
            Ok(()) => {
                state.finished = true;
                Ok(())
            }
            Err(error) if is_unknown(&error) => {
                state.finished = true;
                state.inner.faulted.store(true, Ordering::Release);
                if let Some(lease) = &state.inner.lease {
                    lease.invalidate();
                }
                Err(error)
            }
            Err(error) => {
                state.failed = true;
                Err(error)
            }
        }
    }

    fn range_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a>
    where
        's: 'a,
    {
        Box::new(self.range_scan(lower, upper).map(|pair| {
            let (k, v) = pair?;
            try_decode_tuple_from_kv(&k, &v, None)
        }))
    }
    fn range_skip_scan_tuple<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
        valid_at: ValidityTs,
    ) -> Box<dyn Iterator<Item = Result<Tuple>> + 'a> {
        Box::new(ValidityScanner::new(
            BatchScanner::new(self.state.clone(), lower, Some(upper), false),
            valid_at,
        ))
    }
    fn range_scan<'a>(
        &'a self,
        lower: &[u8],
        upper: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        Box::new(BatchScanner::new(
            self.state.clone(),
            lower,
            Some(upper),
            false,
        ))
    }
    fn range_count<'a>(&'a self, lower: &[u8], upper: &[u8]) -> Result<usize>
    where
        's: 'a,
    {
        self.range_scan(lower, upper)
            .try_fold(0, |count, item| item.map(|_| count + 1))
    }
    fn total_scan<'a>(&'a self) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>
    where
        's: 'a,
    {
        Box::new(BatchScanner::new(self.state.clone(), &[], None, false))
    }
}
