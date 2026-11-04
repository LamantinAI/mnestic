/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::VecDeque,
    iter::FusedIterator,
    ops::Bound::{self, Excluded, Included, Unbounded},
    sync::{Arc, Mutex},
};

use miette::{miette, Result};

use super::{
    blocking,
    transaction::{response_too_large, TxState},
};
use crate::{
    data::{
        tuple::{check_key_for_validity, Tuple},
        value::ValidityTs,
    },
    runtime::relation::try_extend_tuple_from_v,
};

pub(super) struct BatchScanner {
    state: Arc<Mutex<TxState>>,
    lower: Bound<Vec<u8>>,
    upper: Option<Vec<u8>>,
    buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    batch: u32,
    exhausted: bool,
    persisted: bool,
}

impl BatchScanner {
    pub fn new(
        state: Arc<Mutex<TxState>>,
        lower: &[u8],
        upper: Option<&[u8]>,
        persisted: bool,
    ) -> Self {
        let batch = state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .inner
            .config
            .scan_batch_size;
        Self {
            state,
            lower: Included(lower.to_vec()),
            upper: upper.map(Vec::from),
            buffer: VecDeque::new(),
            batch,
            exhausted: upper.is_some_and(|u| lower >= u),
            persisted,
        }
    }

    pub fn seek(&mut self, key: Vec<u8>) {
        self.buffer.retain(|(k, _)| k >= &key);
        let advances = match &self.lower {
            Included(k) | Excluded(k) => key > *k,
            Unbounded => true,
        };
        if advances {
            self.lower = Included(key);
        }
    }

    fn fetch(&mut self) -> Result<()> {
        if let (Included(lower) | Excluded(lower), Some(upper)) = (&self.lower, &self.upper) {
            if lower >= upper {
                self.exhausted = true;
                return Ok(());
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        state.check()?;
        let lower = match &self.lower {
            Included(k) => Included(state.inner.key(k)?),
            Excluded(k) => Excluded(state.inner.key(k)?),
            Unbounded => Unbounded,
        };
        let upper = match &self.upper {
            Some(k) => Excluded(state.inner.key(k)?),
            None => state.inner.upper().map(Excluded).unwrap_or(Unbounded),
        };
        loop {
            let result = if self.persisted {
                blocking(
                    state
                        .snapshot
                        .scan((lower.clone(), upper.clone()), self.batch),
                )
                .map(|it| it.collect::<Vec<_>>())
            } else {
                blocking(state.tx.scan((lower.clone(), upper.clone()), self.batch))
                    .map(|it| it.collect::<Vec<_>>())
            };
            match result {
                Ok(pairs) => {
                    self.exhausted = pairs.len() < self.batch as usize;
                    for pair in pairs {
                        let key: Vec<u8> = pair.0.into();
                        let logical = key
                            .strip_prefix(state.inner.prefix.as_slice())
                            .ok_or_else(|| {
                                miette!("TiKV scan returned a key outside the database namespace")
                            })?
                            .to_vec();
                        self.lower = Excluded(logical.clone());
                        self.buffer.push_back((logical, pair.1));
                    }
                    return Ok(());
                }
                Err(error) if self.batch > 1 && response_too_large(&error) => {
                    self.batch = (self.batch / 2).max(1);
                }
                Err(error) => {
                    state.failed |= state.write;
                    return Err(error);
                }
            }
        }
    }
}

impl Iterator for BatchScanner {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(pair) = self.buffer.pop_front() {
            return Some(Ok(pair));
        }
        if self.exhausted {
            return None;
        }
        if let Err(error) = self.fetch() {
            self.buffer.clear();
            self.exhausted = true;
            return Some(Err(error));
        }
        self.buffer.pop_front().map(Ok)
    }
}
impl FusedIterator for BatchScanner {}

pub(super) struct ValidityScanner {
    raw: BatchScanner,
    valid_at: ValidityTs,
    failed: bool,
}
impl ValidityScanner {
    pub fn new(raw: BatchScanner, valid_at: ValidityTs) -> Self {
        Self {
            raw,
            valid_at,
            failed: false,
        }
    }
}
impl Iterator for ValidityScanner {
    type Item = Result<Tuple>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let (key, value) = match self.raw.next()? {
                Ok(pair) => pair,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let (tuple, bound) = check_key_for_validity(&key, self.valid_at, None);
            self.raw.seek(bound);
            if let Some(mut tuple) = tuple {
                if let Err(error) = try_extend_tuple_from_v(&mut tuple, &value) {
                    self.failed = true;
                    return Some(Err(error));
                }
                return Some(Ok(tuple));
            }
        }
    }
}
impl FusedIterator for ValidityScanner {}
