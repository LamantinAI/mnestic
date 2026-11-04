/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Admit a writer before acquiring its TiKV snapshot. In pessimistic mode,
//! waiting on a server lock after begin would otherwise retain an older start_ts.

use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
pub(super) struct WriterAdmission {
    active: Mutex<bool>,
    released: Condvar,
}

pub(super) struct WritePermit(Arc<WriterAdmission>);

impl WriterAdmission {
    pub fn acquire(self: &Arc<Self>) -> WritePermit {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        while *active {
            active = self
                .released
                .wait(active)
                .unwrap_or_else(|p| p.into_inner());
        }
        *active = true;
        WritePermit(self.clone())
    }
}

impl Drop for WritePermit {
    fn drop(&mut self) {
        *self.0.active.lock().unwrap_or_else(|p| p.into_inner()) = false;
        self.0.released.notify_one();
    }
}
