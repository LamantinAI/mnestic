/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! A lease admits one engine runtime; a separate token fences data transactions.
//! Heartbeats only update the lease row, avoiding conflicts with long data writes.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use miette::{bail, IntoDiagnostic, Result};
use tikv_client::{Transaction, TransactionClient};
use tokio::runtime::Handle;
use uuid::Uuid;

use super::{
    blocking,
    error::{commit, is_unknown},
    transaction_options, TiKvConfig,
};

struct LeaseState {
    client: TransactionClient,
    token: [u8; 16],
    fence_key: Vec<u8>,
    lease_key: Vec<u8>,
    ttl_ms: u64,
    deadline: AtomicI64,
    renewed: Mutex<Instant>,
    lost: AtomicBool,
}

pub(super) struct NamespaceLease {
    state: Arc<LeaseState>,
    stop: Sender<()>,
    worker: Option<JoinHandle<()>>,
}

fn decode(value: &[u8]) -> Result<([u8; 16], i64)> {
    if value.len() != 24 {
        bail!("invalid TiKV namespace lease record");
    }
    Ok((
        value[..16].try_into().unwrap(),
        i64::from_be_bytes(value[16..].try_into().unwrap()),
    ))
}

fn encode(token: &[u8; 16], deadline: i64) -> Vec<u8> {
    [token.as_slice(), &deadline.to_be_bytes()].concat()
}

impl NamespaceLease {
    pub(super) fn acquire(
        client: TransactionClient,
        prefix: &[u8],
        config: &TiKvConfig,
    ) -> Result<Self> {
        let renewed = Instant::now();
        let token = *Uuid::new_v4().as_bytes();
        let mut fence_key = prefix.to_vec();
        fence_key.extend_from_slice(b"\0fence");
        let mut lease_key = prefix.to_vec();
        lease_key.extend_from_slice(b"\0lease");
        let mut tx = blocking(client.begin_with_options(transaction_options(false)))?;
        let result = (|| {
            // Always lock the lease before the fence. A data transaction locks
            // only the fence, so its commit never waits for this lease row.
            let old = blocking(tx.get_for_update(lease_key.clone()))?;
            let now = blocking(client.current_timestamp())?.physical;
            if let Some(old) = old {
                if decode(&old)?.1 > now {
                    bail!("TiKV namespace is already owned by another runtime");
                }
            }
            let deadline = now
                .checked_add(i64::try_from(config.lease_ttl_ms).into_diagnostic()?)
                .ok_or_else(|| miette::miette!("TiKV lease TTL overflow"))?;
            blocking(tx.get_for_update(fence_key.clone()))?;
            blocking(tx.put(fence_key.clone(), token.to_vec()))?;
            blocking(tx.put(lease_key.clone(), encode(&token, deadline)))?;
            commit(&mut tx)?;
            Ok(deadline)
        })();
        let deadline = match result {
            Ok(deadline) => deadline,
            Err(error) => {
                if !is_unknown(&error) {
                    let _ = blocking(tx.rollback());
                }
                return Err(error);
            }
        };
        let state = Arc::new(LeaseState {
            client,
            token,
            fence_key,
            lease_key,
            ttl_ms: config.lease_ttl_ms,
            deadline: AtomicI64::new(deadline),
            renewed: Mutex::new(renewed),
            lost: AtomicBool::new(false),
        });
        let (stop, incoming) = mpsc::channel();
        let heartbeat = state.clone();
        let worker = match thread::Builder::new()
            .name("mnestic-tikv-lease".into())
            .spawn(move || {
                while incoming.recv_timeout(Duration::from_millis(heartbeat.ttl_ms / 3))
                    == Err(mpsc::RecvTimeoutError::Timeout)
                {
                    if heartbeat.renew().is_err() {
                        heartbeat.lost.store(true, Ordering::Release);
                        break;
                    }
                }
            }) {
            Ok(worker) => worker,
            Err(error) => {
                let _ = state.release();
                return Err(error).into_diagnostic();
            }
        };
        Ok(Self {
            state,
            stop,
            worker: Some(worker),
        })
    }

    pub(super) fn check(&self) -> Result<()> {
        if self.state.lost.load(Ordering::Acquire)
            || self
                .state
                .renewed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .elapsed()
                >= Duration::from_millis(self.state.ttl_ms)
        {
            self.invalidate();
            bail!("TiKV namespace lease lost; reopen the database to refresh engine caches");
        }
        Ok(())
    }

    pub(super) fn validate_transaction(&self, tx: &mut Transaction, write: bool) -> Result<()> {
        self.check()?;
        if tx.start_timestamp().physical >= self.state.deadline.load(Ordering::Acquire) {
            self.invalidate();
            bail!("TiKV namespace lease expired");
        }
        let token = if write {
            blocking(tx.get_for_update(self.state.fence_key.clone()))?
        } else {
            blocking(tx.get(self.state.fence_key.clone()))?
        };
        if token.as_deref() != Some(self.state.token.as_slice()) {
            self.invalidate();
            bail!("TiKV namespace ownership changed; reopen the database");
        }
        // Write transactions retain this fence until commit. A new owner must
        // change it, so an expired/paused owner cannot commit stale cached state.
        Ok(())
    }

    pub(super) fn before_commit(&self) -> Result<()> {
        self.check()?;
        let now = blocking(self.state.client.current_timestamp())?.physical;
        if now >= self.state.deadline.load(Ordering::Acquire) {
            self.invalidate();
            bail!("TiKV namespace lease expired before commit");
        }
        Ok(())
    }

    pub(super) fn invalidate(&self) {
        self.state.lost.store(true, Ordering::Release);
    }
}

impl LeaseState {
    fn renew(&self) -> Result<()> {
        if self.lost.load(Ordering::Acquire) {
            bail!("TiKV namespace lease already lost");
        }
        let renewed = Instant::now();
        let mut tx = blocking(self.client.begin_with_options(transaction_options(false)))?;
        let result = (|| {
            let value = blocking(tx.get_for_update(self.lease_key.clone()))?
                .ok_or_else(|| miette::miette!("TiKV namespace lease missing"))?;
            let (token, old_deadline) = decode(&value)?;
            let now = blocking(self.client.current_timestamp())?.physical;
            if token != self.token || old_deadline <= now {
                bail!("TiKV namespace lease changed or expired");
            }
            let deadline = now
                .checked_add(i64::try_from(self.ttl_ms).into_diagnostic()?)
                .ok_or_else(|| miette::miette!("TiKV lease TTL overflow"))?;
            blocking(tx.put(self.lease_key.clone(), encode(&self.token, deadline)))?;
            commit(&mut tx)?;
            self.deadline.store(deadline, Ordering::Release);
            *self.renewed.lock().unwrap_or_else(|p| p.into_inner()) = renewed;
            Ok(())
        })();
        if result.as_ref().is_err_and(|error| !is_unknown(error)) {
            let _ = blocking(tx.rollback());
        }
        result
    }

    fn release(&self) -> Result<()> {
        let mut tx = blocking(self.client.begin_with_options(transaction_options(false)))?;
        let result = (|| {
            if let Some(value) = blocking(tx.get_for_update(self.lease_key.clone()))? {
                if decode(&value)?.0 == self.token {
                    blocking(tx.put(self.lease_key.clone(), encode(&self.token, 0)))?;
                }
            }
            commit(&mut tx)?;
            Ok(())
        })();
        if result.as_ref().is_err_and(|error| !is_unknown(error)) {
            let _ = blocking(tx.rollback());
        }
        result
    }
}

impl Drop for NamespaceLease {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        if Handle::try_current().is_ok() {
            // Only the unusual drop-inside-async path needs a temporary cleanup
            // thread. Normal storage execution stays on its persistent worker.
            thread::scope(|scope| {
                let _ = scope.spawn(|| self.state.release()).join();
            });
        } else {
            let _ = self.state.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Storage, StoreTx, TiKvStorage};
    use std::{env, thread, time::Duration};

    fn config() -> TiKvConfig {
        TiKvConfig {
            end_points: vec![env::var("MNESTIC_TIKV_PD_ENDPOINT").unwrap()],
            namespace: Some(format!("fence-{}", Uuid::new_v4())),
            optimistic: true,
            ..Default::default()
        }
    }

    #[test]
    #[ignore = "requires a disposable TiKV cluster"]
    fn expired_owner_cannot_commit_after_takeover() {
        let options = config();
        let old = TiKvStorage::connect(options.clone()).unwrap();
        let mut data = old.transact(true).unwrap();
        data.put(b"stale", b"must not commit").unwrap();
        let lease = old.inner.lease.as_ref().unwrap();
        let mut tamper = blocking(
            lease
                .state
                .client
                .begin_with_options(transaction_options(false)),
        )
        .unwrap();
        blocking(tamper.put(lease.state.lease_key.clone(), encode(&lease.state.token, 0))).unwrap();
        blocking(tamper.commit()).unwrap();
        drop(tamper);
        let new = TiKvStorage::connect(options).unwrap();
        assert!(data.commit().is_err());
        drop(data);
        assert!(old.transact(false).is_err());
        assert_eq!(
            new.transact(false).unwrap().get(b"stale", false).unwrap(),
            None
        );
    }

    #[test]
    #[ignore = "requires a disposable TiKV cluster"]
    fn heartbeat_keeps_owner_and_transactions_retain_lease() {
        let mut options = config();
        options.request_timeout_ms = 300;
        options.lease_ttl_ms = 1500;
        let store = TiKvStorage::connect(options.clone()).unwrap();
        thread::sleep(Duration::from_millis(1800));
        let mut tx = store.transact(true).unwrap();
        tx.put(b"a", b"v").unwrap();
        drop(store);
        assert!(TiKvStorage::connect(options.clone()).is_err());
        tx.commit().unwrap();
        drop(tx);
        assert!(TiKvStorage::connect(options).is_ok());
    }
}
