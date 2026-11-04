/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use lazy_static::lazy_static;
use miette::{bail, IntoDiagnostic, Result};
use tikv_client::{CheckLevel, TransactionClient, TransactionOptions};
use tokio::runtime::{Handle, Runtime};

use crate::{
    storage::{Storage, StoreTx},
    Db,
};

mod admission;
mod config;
mod error;
mod lease;
mod scan;
mod transaction;

use admission::WriterAdmission;
pub use config::{TiKvConfig, TiKvTlsConfig};
pub use error::TiKvCommitUnknown;
use lease::NamespaceLease;
pub use transaction::TiKvTx;

lazy_static! {
    static ref RT: Runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("starting Tokio runtime failed");
}

fn ensure_blocking_context() -> Result<()> {
    if Handle::try_current().is_ok() {
        bail!("TiKV synchronous storage must run on a blocking thread, outside a Tokio runtime");
    }
    Ok(())
}

fn blocking<T>(future: impl Future<Output = tikv_client::Result<T>>) -> Result<T> {
    ensure_blocking_context()?;
    RT.block_on(future).into_diagnostic()
}

fn transaction_options(optimistic: bool) -> TransactionOptions {
    let options = if optimistic {
        TransactionOptions::new_optimistic()
    } else {
        TransactionOptions::new_pessimistic()
    };
    // Cleanup is explicit. A failed rollback or an indeterminate commit must not
    // turn dropping the SDK handle into a second panic during error recovery.
    options.drop_check(CheckLevel::None)
}

/// Connect to a Storage engine backed by TiKV.
/// Experimental: network latency is material for graph traversals.
/// The legacy unprefixed constructor leaves single-runtime ownership to its caller.
pub fn new_cozo_tikv(pd_endpoints: Vec<String>, optimistic: bool) -> Result<Db<TiKvStorage>> {
    new_cozo_tikv_with_config(TiKvConfig {
        end_points: pd_endpoints,
        optimistic,
        ..Default::default()
    })
}

/// Open an explicitly configured TiKV database. A namespaced database holds a
/// renewable exclusive lease because engine caches are local to one runtime.
pub fn new_cozo_tikv_with_config(config: TiKvConfig) -> Result<Db<TiKvStorage>> {
    let db = Db::new(TiKvStorage::connect(config)?)?;
    db.initialize()?;
    Ok(db)
}

struct Inner {
    client: TransactionClient,
    config: TiKvConfig,
    prefix: Vec<u8>,
    lease: Option<NamespaceLease>,
    faulted: AtomicBool,
    writers: Arc<WriterAdmission>,
}

impl Inner {
    fn key(&self, key: &[u8]) -> Result<Vec<u8>> {
        let mut physical = self.prefix.clone();
        physical.extend_from_slice(key);
        // TiKV validates the memcomparable-encoded key, including APIv2's prefix.
        let size = physical
            .len()
            .saturating_add(if self.config.keyspace.is_some() { 4 } else { 0 });
        if (size / 8 + 1).saturating_mul(9) > TiKvConfig::MAX_ENCODED_KEY_BYTES {
            bail!("TiKV encoded key exceeds the 8192-byte server limit");
        }
        Ok(physical)
    }

    fn upper(&self) -> Option<Vec<u8>> {
        if self.prefix.is_empty() {
            None
        } else {
            let mut upper = self.prefix.clone();
            *upper.last_mut().unwrap() += 1;
            Some(upper)
        }
    }

    fn check_lease(&self) -> Result<()> {
        if self.faulted.load(Ordering::Acquire) {
            bail!("TiKV commit outcome was unknown; reopen the database before further use");
        }
        if let Some(lease) = &self.lease {
            lease.check()?;
        }
        Ok(())
    }
}

/// Storage engine based on TiKV's transactional API.
#[derive(Clone)]
pub struct TiKvStorage {
    inner: Arc<Inner>,
}

impl TiKvStorage {
    /// Connect without initializing Cozo relations, for storage integrations.
    pub fn connect(config: TiKvConfig) -> Result<Self> {
        config.validate()?;
        let client = blocking(TransactionClient::new_with_config(
            config.end_points.clone(),
            config.client_config(),
        ))?;
        let (prefix, lease) = if let Some(namespace) = &config.namespace {
            let mut base = b"\xffmnestic\x01".to_vec();
            base.push(namespace.len() as u8);
            base.extend_from_slice(namespace.as_bytes());
            let lease = NamespaceLease::acquire(client.clone(), &base, &config)?;
            base.push(1);
            (base, Some(lease))
        } else {
            (Vec::new(), None)
        };
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                config,
                prefix,
                lease,
                faulted: AtomicBool::new(false),
                writers: Arc::new(WriterAdmission::default()),
            }),
        })
    }
}

impl Storage<'_> for TiKvStorage {
    type Tx = TiKvTx;
    fn storage_kind(&self) -> &'static str {
        "tikv"
    }

    fn transact(&self, write: bool) -> Result<Self::Tx> {
        ensure_blocking_context()?;
        self.inner.check_lease()?;
        let permit = write.then(|| self.inner.writers.acquire());
        self.inner.check_lease()?;
        let mut tx = blocking(
            self.inner
                .client
                .begin_with_options(transaction_options(self.inner.config.optimistic)),
        )?;
        if let Some(lease) = &self.inner.lease {
            if let Err(error) = lease.validate_transaction(&mut tx, write) {
                let _ = blocking(tx.rollback());
                return Err(error);
            }
        }
        let snapshot = self
            .inner
            .client
            .snapshot(tx.start_timestamp(), transaction_options(true).read_only());
        Ok(TiKvTx::new(self.inner.clone(), tx, snapshot, write, permit))
    }

    fn range_compact(&self, _lower: &[u8], _upper: &[u8]) -> Result<()> {
        Ok(())
    }

    fn batch_put<'a>(
        &'a self,
        data: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
    ) -> Result<()> {
        // Preserve atomic publication. Large imports fail rather than exposing
        // partially restored relations; callers must size a restore explicitly.
        let mut tx = self.transact(true)?;
        for entry in data {
            let (key, value) = entry?;
            tx.put(&key, &value)?;
        }
        tx.commit()
    }
}
