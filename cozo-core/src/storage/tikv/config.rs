/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{path::PathBuf, time::Duration};

use miette::{bail, Result};
use serde_derive::{Deserialize, Serialize};
use tikv_client::Config;

/// TLS credentials used for both PD and TiKV connections.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TiKvTlsConfig {
    pub ca: PathBuf,
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// Connection and bounded RPC settings for the transactional TiKV backend.
/// A namespace is a physical key prefix, not an authorization boundary.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct TiKvConfig {
    #[serde(alias = "endpoints")]
    pub end_points: Vec<String>,
    pub optimistic: bool,
    /// Namespaced databases enforce one live runtime to protect process-local caches.
    /// None retains the legacy unprefixed layout and its caller-owned exclusivity.
    pub namespace: Option<String>,
    pub keyspace: Option<String>,
    pub tls: Option<TiKvTlsConfig>,
    pub request_timeout_ms: u64,
    pub max_decoding_bytes: usize,
    pub max_value_bytes: usize,
    pub scan_batch_size: u32,
    pub lease_ttl_ms: u64,
}

impl Default for TiKvConfig {
    fn default() -> Self {
        Self {
            end_points: Vec::new(),
            optimistic: false,
            namespace: None,
            keyspace: None,
            tls: None,
            request_timeout_ms: 3_000,
            max_decoding_bytes: 16 * 1024 * 1024,
            max_value_bytes: 6 * 1024 * 1024,
            scan_batch_size: 100,
            lease_ttl_ms: 60_000,
        }
    }
}

impl TiKvConfig {
    pub(crate) const MAX_ENCODED_KEY_BYTES: usize = 8192;

    pub(crate) fn validate(&self) -> Result<()> {
        if self.end_points.is_empty() || self.end_points.iter().any(|s| s.trim().is_empty()) {
            bail!("TiKV requires at least one nonempty PD endpoint");
        }
        if self
            .namespace
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.len() > 255)
        {
            bail!("TiKV namespace must contain between 1 and 255 UTF-8 bytes");
        }
        if self.keyspace.as_ref().is_some_and(|s| s.trim().is_empty()) {
            bail!("TiKV keyspace must not be empty");
        }
        if self.request_timeout_ms == 0 || self.scan_batch_size == 0 {
            bail!("TiKV timeout and scan batch size must be positive");
        }
        if self.lease_ttl_ms < self.request_timeout_ms.saturating_mul(4) {
            bail!("TiKV lease TTL must be at least four RPC timeouts");
        }
        if self.max_value_bytes == 0
            || self.max_value_bytes > 6 * 1024 * 1024
            || self.max_decoding_bytes < self.max_value_bytes.saturating_add(64 * 1024)
        {
            bail!("TiKV value limit must be 1..=6MiB; decoding must allow an additional 64KiB");
        }
        Ok(())
    }

    pub(crate) fn client_config(&self) -> Config {
        let mut config = Config::default()
            .with_timeout(Duration::from_millis(self.request_timeout_ms))
            .with_grpc_max_decoding_message_size(self.max_decoding_bytes);
        if let Some(tls) = &self.tls {
            config = config.with_security(&tls.ca, &tls.cert, &tls.key);
        }
        if let Some(keyspace) = &self.keyspace {
            config = config.with_keyspace(keyspace);
        }
        config
    }
}
