/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Writing cozo rows out as Arrow-typed Parquet segments.
//!
//! Mirror of `archive::import`. Type mapping policy is symmetric: produce the
//! Arrow type the import path knows how to read back.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    ListBuilder, StringBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use miette::{bail, IntoDiagnostic, Result, WrapErr};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::data::relation::{ColType, ColumnDef};
use crate::data::value::DataValue;

/// Plan an Arrow schema from a cozo column-definition list. Each column maps
/// to one Arrow `Field`; types follow the same policy as
/// `arrow_value_to_data` in reverse.
pub(crate) fn arrow_schema_for_columns(columns: &[ColumnDef]) -> Result<Arc<Schema>> {
    let mut fields = Vec::with_capacity(columns.len());
    for col in columns {
        let dt = arrow_type_for(&col.typing.coltype)?;
        let mut field = Field::new(col.name.as_str(), dt, col.typing.nullable);
        // Json is carried as Arrow Utf8 (serialized JSON text). Tag the field so
        // the import path reconstructs a DataValue::Json rather than a plain
        // string — otherwise the Json coercion arm would re-wrap the text as a
        // JSON string node and the round-trip would double-encode.
        if matches!(col.typing.coltype, ColType::Json) {
            field = field.with_metadata(HashMap::from([(
                crate::archive::COZO_COLTYPE_META_KEY.to_string(),
                crate::archive::COZO_COLTYPE_JSON.to_string(),
            )]));
        }
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn arrow_type_for(c: &ColType) -> Result<ArrowDataType> {
    Ok(match c {
        ColType::Bool => ArrowDataType::Boolean,
        // cozo Int / Validity both round-trip through Int64. The import side
        // accepts integer widening, so writing the wider type is fine.
        ColType::Int | ColType::Validity => ArrowDataType::Int64,
        ColType::Float => ArrowDataType::Float64,
        ColType::String => ArrowDataType::Utf8,
        ColType::Bytes => ArrowDataType::Binary,
        ColType::Uuid => ArrowDataType::FixedSizeBinary(16),
        // Json is serialized to its JSON text and stored as Utf8; the field
        // carries a `cozo:coltype=json` tag (see arrow_schema_for_columns) so
        // the import path knows to parse it back rather than treat it as a
        // plain string.
        ColType::Json => ArrowDataType::Utf8,
        ColType::List { eltype, .. } => {
            let inner = arrow_type_for(&eltype.coltype)?;
            ArrowDataType::List(Arc::new(Field::new("item", inner, eltype.nullable)))
        }
        // Type coverage is deliberately limited. Anything not in the list above
        // yields a clear unsupported-type error rather than a surprise
        // round-trip failure.
        other => bail!(
            "exporting cozo type {:?} to Parquet is not supported; \
            supported types are: Bool, Int, Float, String, Bytes, Uuid, \
            Validity, Json, and List of those (except List of Json)",
            other
        ),
    })
}

/// Build a single `RecordBatch` from `rows`. Each `row` contains one
/// `DataValue` per column in `columns`.
pub(crate) fn rows_to_record_batch(
    columns: &[ColumnDef],
    rows: &[Vec<DataValue>],
) -> Result<RecordBatch> {
    let schema = arrow_schema_for_columns(columns)?;
    let mut col_arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());

    for (col_idx, col) in columns.iter().enumerate() {
        let arr = build_array_for(&col.typing.coltype, rows, col_idx).wrap_err_with(|| {
            format!(
                "while building Arrow array for column '{}' (type {:?})",
                col.name, col.typing.coltype
            )
        })?;
        col_arrays.push(arr);
    }

    RecordBatch::try_new(schema, col_arrays).into_diagnostic()
}

fn build_array_for(
    c: &ColType,
    rows: &[Vec<DataValue>],
    col_idx: usize,
) -> Result<ArrayRef> {
    match c {
        ColType::Bool => {
            let mut b = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                match &row[col_idx] {
                    DataValue::Bool(v) => b.append_value(*v),
                    DataValue::Null => b.append_null(),
                    other => bail!("expected Bool, got {:?}", other),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::Int | ColType::Validity => {
            let mut b = Int64Builder::with_capacity(rows.len());
            for row in rows {
                match &row[col_idx] {
                    DataValue::Null => b.append_null(),
                    v => match v.get_int() {
                        Some(i) => b.append_value(i),
                        None => bail!("expected Int (got {:?})", v),
                    },
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::Float => {
            let mut b = Float64Builder::with_capacity(rows.len());
            for row in rows {
                match &row[col_idx] {
                    DataValue::Null => b.append_null(),
                    v => match v.get_float() {
                        Some(f) => b.append_value(f),
                        None => bail!("expected Float (got {:?})", v),
                    },
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::String => {
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match &row[col_idx] {
                    DataValue::Str(s) => b.append_value(s.as_str()),
                    DataValue::Null => b.append_null(),
                    other => bail!("expected String, got {:?}", other),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::Bytes => {
            let mut b = BinaryBuilder::with_capacity(rows.len(), rows.len() * 16);
            for row in rows {
                match &row[col_idx] {
                    DataValue::Bytes(bs) => b.append_value(bs),
                    DataValue::Null => b.append_null(),
                    other => bail!("expected Bytes, got {:?}", other),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::Uuid => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
            for row in rows {
                match &row[col_idx] {
                    DataValue::Uuid(w) => b.append_value(w.0.as_bytes()).into_diagnostic()?,
                    DataValue::Null => b.append_null(),
                    other => bail!("expected Uuid, got {:?}", other),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::Json => {
            // Serialize each JSON value to its compact text form. The stored
            // value for a Json column is always DataValue::Json after coercion.
            let mut b = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                match &row[col_idx] {
                    DataValue::Json(j) => b.append_value(j.0.to_string()),
                    DataValue::Null => b.append_null(),
                    other => bail!("expected Json, got {:?}", other),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        ColType::List { eltype, .. } => {
            let inner_dt = arrow_type_for(&eltype.coltype)?;
            // Build the values array element-by-element using a sub-builder
            // on a temporary 1-row batch — the most ergonomic way to reuse
            // build_array_for recursively without inlining all the typed
            // arms here.
            let mut item_builder: ListBuilder<Box<dyn arrow::array::ArrayBuilder>> =
                ListBuilder::new(arrow::array::make_builder(&inner_dt, 0));
            for row in rows {
                match &row[col_idx] {
                    DataValue::List(l) => {
                        for v in l {
                            append_scalar(item_builder.values().as_mut(), &eltype.coltype, v)?;
                        }
                        item_builder.append(true);
                    }
                    DataValue::Null => item_builder.append(false),
                    other => bail!("expected List, got {:?}", other),
                }
            }
            Ok(Arc::new(item_builder.finish()))
        }
        other => bail!("export not implemented for cozo type {:?}", other),
    }
}

/// Append a single scalar value to a dynamic Arrow builder. Used by the List
/// builder above to recurse one level without re-typing the world.
fn append_scalar(
    b: &mut dyn arrow::array::ArrayBuilder,
    c: &ColType,
    v: &DataValue,
) -> Result<()> {
    use arrow::array::*;
    match c {
        ColType::Bool => {
            let bb = b.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
            match v {
                DataValue::Bool(x) => bb.append_value(*x),
                DataValue::Null => bb.append_null(),
                other => bail!("expected Bool in list, got {:?}", other),
            }
        }
        ColType::Int | ColType::Validity => {
            let ib = b.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            match v {
                DataValue::Null => ib.append_null(),
                x => match x.get_int() {
                    Some(i) => ib.append_value(i),
                    None => bail!("expected Int in list, got {:?}", x),
                },
            }
        }
        ColType::Float => {
            let fb = b.as_any_mut().downcast_mut::<Float64Builder>().unwrap();
            match v {
                DataValue::Null => fb.append_null(),
                x => match x.get_float() {
                    Some(f) => fb.append_value(f),
                    None => bail!("expected Float in list, got {:?}", x),
                },
            }
        }
        ColType::String => {
            let sb = b.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            match v {
                DataValue::Str(s) => sb.append_value(s.as_str()),
                DataValue::Null => sb.append_null(),
                other => bail!("expected String in list, got {:?}", other),
            }
        }
        ColType::Bytes => {
            let bb = b.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
            match v {
                DataValue::Bytes(bs) => bb.append_value(bs),
                DataValue::Null => bb.append_null(),
                other => bail!("expected Bytes in list, got {:?}", other),
            }
        }
        other => bail!("nested list element type {:?} not supported in slice 4", other),
    }
    Ok(())
}

/// Encode `rows` as a Parquet segment in memory. Returns the bytes plus the
/// SHA-256 of those bytes — the hash must match the SHA-256 the
/// destination object store will see, so we compute it on the in-memory
/// payload, *not* by re-reading after upload.
pub(crate) fn parquet_bytes_for_rows(
    columns: &[ColumnDef],
    rows: &[Vec<DataValue>],
) -> Result<(Vec<u8>, [u8; 32])> {
    let batch = rows_to_record_batch(columns, rows)?;
    let mut buf: Vec<u8> = Vec::new();
    // Snappy is a good default: cheap to compress and decompress, smaller than
    // none, ubiquitously supported by datalake readers (Spark, DuckDB, etc.).
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .into_diagnostic()?;
        writer.write(&batch).into_diagnostic()?;
        writer.close().into_diagnostic()?;
    }
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let digest = hasher.finalize();
    Ok((buf, digest.into()))
}

/// Convenience wrapper for the test suite + simple local-file paths: writes
/// the bytes from `parquet_bytes_for_rows` to `path` and returns the hash.
pub(crate) fn write_parquet_local(
    path: &Path,
    columns: &[ColumnDef],
    rows: &[Vec<DataValue>],
) -> Result<[u8; 32]> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).into_diagnostic().wrap_err_with(|| {
            format!("failed to create staging directory {}", parent.display())
        })?;
    }
    let (bytes, sha) = parquet_bytes_for_rows(columns, rows)?;
    let mut file = File::create(path).into_diagnostic().wrap_err_with(|| {
        format!("failed to create Parquet segment at {}", path.display())
    })?;
    use std::io::Write;
    file.write_all(&bytes).into_diagnostic()?;
    Ok(sha)
}

#[cfg(test)]
mod tests {
    use super::*;
    use smartstring::SmartString;
    use tempfile::tempdir;

    use crate::archive::import::read_parquet_local;
    use crate::data::relation::NullableColType;

    fn col(name: &str, t: ColType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: SmartString::from(name),
            typing: NullableColType { coltype: t, nullable },
            default_gen: None,
        }
    }

    #[test]
    fn round_trip_int_string_bool() {
        let cols = vec![
            col("id", ColType::Int, false),
            col("name", ColType::String, false),
            col("flag", ColType::Bool, true),
        ];
        let rows: Vec<Vec<DataValue>> = vec![
            vec![
                DataValue::from(1i64),
                DataValue::from("alice"),
                DataValue::from(true),
            ],
            vec![
                DataValue::from(2i64),
                DataValue::from("bob"),
                DataValue::Null,
            ],
        ];
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg.parquet");
        let hash = write_parquet_local(&path, &cols, &rows).unwrap();
        assert_eq!(hash.len(), 32);

        let pd = read_parquet_local(&path).unwrap();
        assert_eq!(pd.headers, vec!["id".to_string(), "name".to_string(), "flag".to_string()]);
        assert_eq!(pd.rows.len(), 2);
        assert_eq!(pd.rows[0][0], DataValue::from(1i64));
        assert_eq!(pd.rows[0][2], DataValue::from(true));
        assert_eq!(pd.rows[1][2], DataValue::Null);
    }

    #[test]
    fn round_trip_uuid() {
        let cols = vec![col("u", ColType::Uuid, false)];
        let id = uuid::Uuid::new_v4();
        let rows = vec![vec![DataValue::Uuid(crate::data::value::UuidWrapper(id))]];
        let dir = tempdir().unwrap();
        let path = dir.path().join("u.parquet");
        write_parquet_local(&path, &cols, &rows).unwrap();
        let pd = read_parquet_local(&path).unwrap();
        match &pd.rows[0][0] {
            DataValue::Uuid(w) => assert_eq!(w.0, id),
            other => panic!("expected Uuid, got {other:?}"),
        }
    }

    #[test]
    fn round_trip_json() {
        use crate::data::value::JsonData;
        let cols = vec![col("j", ColType::Json, true)];
        let v1 = serde_json::json!({"a": 1, "b": [true, "x"], "c": null});
        let rows: Vec<Vec<DataValue>> = vec![
            vec![DataValue::Json(JsonData(v1.clone()))],
            vec![DataValue::Null],
        ];
        let dir = tempdir().unwrap();
        let path = dir.path().join("j.parquet");
        write_parquet_local(&path, &cols, &rows).unwrap();

        let pd = read_parquet_local(&path).unwrap();
        assert_eq!(pd.rows.len(), 2);
        match &pd.rows[0][0] {
            DataValue::Json(j) => assert_eq!(j.0, v1),
            other => panic!("expected Json, got {other:?}"),
        }
        assert_eq!(pd.rows[1][0], DataValue::Null);
    }

    #[test]
    fn unsupported_type_errors() {
        // `Any` has no fixed Arrow representation, so it remains unsupported.
        let cols = vec![col("a", ColType::Any, false)];
        let rows = vec![vec![DataValue::from(1i64)]];
        let dir = tempdir().unwrap();
        let path = dir.path().join("a.parquet");
        let err = write_parquet_local(&path, &cols, &rows).unwrap_err().to_string();
        assert!(
            err.contains("not supported") || err.contains("Any"),
            "got: {err}"
        );
    }

    #[test]
    fn hash_is_deterministic_for_identical_input() {
        let cols = vec![col("id", ColType::Int, false)];
        let rows = vec![vec![DataValue::from(42i64)]];
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("a.parquet");
        let p2 = dir.path().join("b.parquet");
        let h1 = write_parquet_local(&p1, &cols, &rows).unwrap();
        let h2 = write_parquet_local(&p2, &cols, &rows).unwrap();
        // Parquet writers may include timestamps in metadata; if so this would
        // fail. If it does, we should switch to hashing only the row group
        // bytes, not the whole file. For now: confirm reality.
        // (Snappy compression is deterministic; arrow writer pads no random.)
        assert_eq!(h1, h2, "writing identical input twice must produce identical bytes");
    }
}
