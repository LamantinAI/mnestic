/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use miette::{Diagnostic, IntoDiagnostic, Result};
use thiserror::Error;
use tikv_client::{Error as ClientError, Transaction};

use super::RT;

/// The primary commit response was lost: rollback or replay is unsafe.
#[derive(Debug, Error, Diagnostic)]
#[error("TiKV commit outcome unknown; reconcile by reopening, do not retry the operation")]
pub struct TiKvCommitUnknown {
    #[source]
    source: ClientError,
}

pub(super) fn commit(tx: &mut Transaction) -> Result<()> {
    match RT.block_on(tx.commit()) {
        Ok(_) => Ok(()),
        Err(source @ ClientError::UndeterminedError(_)) => Err(TiKvCommitUnknown { source }.into()),
        Err(error) => Err(error).into_diagnostic(),
    }
}

pub(super) fn is_unknown(error: &miette::Report) -> bool {
    error.downcast_ref::<TiKvCommitUnknown>().is_some()
}
