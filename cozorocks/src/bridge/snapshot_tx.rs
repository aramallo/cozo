/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cxx::*;

use crate::bridge::ffi::*;
use crate::bridge::iter::IterBuilder;
use crate::bridge::tx::PinSlice;

pub struct SnapshotTx {
    pub(crate) inner: UniquePtr<SnapshotTxBridge>,
}

// SAFETY: SnapshotTx is genuinely Sync. All underlying operations (DB::Get and
// DB::NewIterator with a snapshot) are documented as thread-safe in RocksDB.
// Each call to iterator() creates an independent IterBridge, so concurrent
// rayon threads can safely call get()/exists()/iterator() simultaneously.
unsafe impl Sync for SnapshotTx {}
unsafe impl Send for SnapshotTx {}

impl SnapshotTx {
    #[inline]
    pub fn get(&self, key: &[u8]) -> Result<Option<PinSlice>, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        let ret = self.inner.get(key, &mut status);
        match status.code {
            StatusCode::kOk => Ok(Some(PinSlice { inner: ret })),
            StatusCode::kNotFound => Ok(None),
            _ => Err(status),
        }
    }

    #[inline]
    pub fn exists(&self, key: &[u8]) -> Result<bool, RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.exists(key, &mut status);
        match status.code {
            StatusCode::kOk => Ok(true),
            StatusCode::kNotFound => Ok(false),
            _ => Err(status),
        }
    }

    #[inline]
    pub fn commit(&self) -> Result<(), RocksDbStatus> {
        let mut status = RocksDbStatus::default();
        self.inner.commit(&mut status);
        if status.is_ok() {
            Ok(())
        } else {
            Err(status)
        }
    }

    #[inline]
    pub fn iterator(&self) -> IterBuilder {
        IterBuilder {
            inner: self.inner.iterator(),
        }
        .auto_prefix_mode(true)
    }
}
