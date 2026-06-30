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

/// Arrow field-metadata key tagging a column with its originating cozo
/// `ColType` when the Arrow type alone is lossy. Currently only `Json` needs
/// it: a Json column is stored as Arrow `Utf8` holding the serialized JSON
/// text, and this tag tells the import path to parse that text back into a
/// `DataValue::Json` instead of leaving it as a plain string (which the
/// `Json` coercion arm would then re-wrap as a JSON *string* node).
#[cfg(feature = "archive")]
pub(crate) const COZO_COLTYPE_META_KEY: &str = "cozo:coltype";
/// Metadata value paired with [`COZO_COLTYPE_META_KEY`] for Json columns.
#[cfg(feature = "archive")]
pub(crate) const COZO_COLTYPE_JSON: &str = "json";

#[cfg(feature = "archive")]
pub(crate) mod manifest;
#[cfg(feature = "archive")]
pub(crate) mod replicator;
#[cfg(feature = "archive")]
pub(crate) mod store;
#[cfg(feature = "archive")]
pub(crate) mod type_mapping;
