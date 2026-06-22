/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Reading a local Parquet file into the shape that cozo's import path expects:
//! a header list (column names) and rows of `DataValue`.
//!
//! Schema mapping is purely positional within a row — each Arrow column's name
//! becomes a header, each batch row becomes a `Vec<DataValue>` of equal length.
//! Column-to-stored-relation matching by name is done one layer up by the sys
//! op handler.
//!
//! Slice 2 supports local filesystem URIs only (`file://...` or a bare path).
//! S3/object_store integration is deferred to slice 5.

use std::fs::File;
use std::path::{Path, PathBuf};

use miette::{bail, IntoDiagnostic, Result, WrapErr};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::archive::type_mapping::arrow_value_to_data;
use crate::data::value::DataValue;

/// Resolve a user-supplied URI string to a local filesystem path. Accepts:
/// - bare paths (`/tmp/foo.parquet`, `./foo.parquet`)
/// - `file://` URIs (`file:///tmp/foo.parquet`)
///
/// Anything with a non-`file` scheme is rejected with a clear error so users
/// know S3 etc. is not yet supported.
pub(crate) fn resolve_local_path(uri: &str) -> Result<PathBuf> {
    if let Some(rest) = uri.strip_prefix("file://") {
        // `file:///tmp/x` -> `/tmp/x`; `file://localhost/tmp/x` -> rejected
        // for simplicity (cozo doesn't need cross-host file URIs).
        let path = if let Some(after_host) = rest.strip_prefix("localhost/") {
            format!("/{after_host}")
        } else if let Some(stripped) = rest.strip_prefix('/') {
            format!("/{stripped}")
        } else {
            bail!("unsupported file URI shape: {uri}");
        };
        return Ok(PathBuf::from(path));
    }

    // Reject anything that looks like a non-file scheme (s3://, http://, etc.).
    // RFC 3986 scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    if let Some(colon) = uri.find(':') {
        let prefix = &uri[..colon];
        let is_scheme = !prefix.is_empty()
            && prefix.chars().next().unwrap().is_ascii_alphabetic()
            && prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.');
        if is_scheme && uri[colon..].starts_with("://") {
            bail!(
                "URI scheme '{prefix}' not supported by ::import_parquet yet; \
                only local files and `file://` URIs are accepted in this slice"
            );
        }
    }

    Ok(PathBuf::from(uri))
}

/// Output shape for a single Parquet file: ordered column names plus rows.
#[derive(Debug)]
pub(crate) struct ParquetData {
    pub(crate) headers: Vec<String>,
    pub(crate) rows: Vec<Vec<DataValue>>,
}

/// Read a Parquet file from the given local path and return its column names
/// and rows as cozo `DataValue`s. The Parquet schema's field order is
/// preserved.
pub(crate) fn read_parquet_local(path: &Path) -> Result<ParquetData> {
    let file = File::open(path)
        .into_diagnostic()
        .wrap_err_with(|| format!("failed to open Parquet file at {}", path.display()))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .into_diagnostic()
        .wrap_err_with(|| format!("invalid Parquet file at {}", path.display()))?;

    let schema = builder.schema().clone();
    let headers: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let reader = builder.build().into_diagnostic()?;

    let mut rows: Vec<Vec<DataValue>> = Vec::new();
    for batch_result in reader {
        let batch = batch_result.into_diagnostic()?;
        let n_rows = batch.num_rows();
        let n_cols = batch.num_columns();
        for r in 0..n_rows {
            let mut row = Vec::with_capacity(n_cols);
            for c in 0..n_cols {
                let arr = batch.column(c);
                row.push(arrow_value_to_data(arr, r).wrap_err_with(|| {
                    format!(
                        "while decoding column '{}' row {}",
                        headers[c], r
                    )
                })?);
            }
            rows.push(row);
        }
    }

    Ok(ParquetData { headers, rows })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    #[test]
    fn resolves_bare_path() {
        let p = resolve_local_path("/tmp/foo.parquet").unwrap();
        assert_eq!(p, PathBuf::from("/tmp/foo.parquet"));
    }

    #[test]
    fn resolves_relative_path() {
        let p = resolve_local_path("./foo.parquet").unwrap();
        assert_eq!(p, PathBuf::from("./foo.parquet"));
    }

    #[test]
    fn resolves_file_uri() {
        let p = resolve_local_path("file:///tmp/foo.parquet").unwrap();
        assert_eq!(p, PathBuf::from("/tmp/foo.parquet"));
    }

    #[test]
    fn rejects_s3_scheme() {
        let err = resolve_local_path("s3://bucket/key").unwrap_err().to_string();
        assert!(
            err.contains("s3"),
            "error should mention the rejected scheme; got: {err}"
        );
    }

    #[test]
    fn rejects_http_scheme() {
        let err = resolve_local_path("https://example.com/foo.parquet")
            .unwrap_err()
            .to_string();
        assert!(err.contains("https"), "got: {err}");
    }

    /// Round-trip helper: write an Arrow batch as Parquet, then read it back
    /// through `read_parquet_local` and return the decoded rows + headers.
    fn write_and_read(batch: RecordBatch) -> ParquetData {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        read_parquet_local(&path).unwrap()
    }

    #[test]
    fn reads_simple_parquet_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let pd = write_and_read(batch);
        assert_eq!(pd.headers, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(pd.rows.len(), 3);
        assert_eq!(pd.rows[0][0], DataValue::from(1i64));
        assert_eq!(pd.rows[2][1], DataValue::from("c"));
    }

    #[test]
    fn reads_parquet_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1i64), None, Some(3)])),
                Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
            ],
        )
        .unwrap();
        let pd = write_and_read(batch);
        assert_eq!(pd.rows[1][0], DataValue::Null);
        assert_eq!(pd.rows[2][1], DataValue::Null);
    }

    #[test]
    fn open_missing_file_errors() {
        let err = read_parquet_local(Path::new("/no/such/file.parquet"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Parquet") || err.contains("open"),
            "error should mention what failed; got: {err}"
        );
    }

    #[test]
    fn open_non_parquet_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("garbage.parquet");
        std::fs::write(&path, b"not a parquet file").unwrap();
        let err = read_parquet_local(&path).unwrap_err().to_string();
        assert!(
            err.contains("invalid") || err.contains("Parquet"),
            "error should indicate the file is invalid; got: {err}"
        );
    }
}
