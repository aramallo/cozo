/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Archiving subsystem.
//!
//! Currently provides a Parquet importer used for restore (slice 2). Later
//! slices will add the manifest, watermark, replicator, and S3 upload.

#[cfg(feature = "archive")]
pub(crate) mod export;
#[cfg(feature = "archive")]
pub(crate) mod import;
#[cfg(feature = "archive")]
pub(crate) mod manifest;
#[cfg(feature = "archive")]
pub(crate) mod replicator;
#[cfg(feature = "archive")]
pub(crate) mod store;
#[cfg(feature = "archive")]
pub(crate) mod type_mapping;
