/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! System relations and helpers used by the archive subsystem.
//!
//! Slice 3 introduces two relations:
//!
//! - `cozo_archive_config`: per-relation settings (currently just the timestamp
//!   column). Future slices will add bucket / region / encryption / etc.
//! - `cozo_archive_watermark`: per-relation `last_safe_commit_ts`. Slice 3
//!   advances this manually via `::archive_advance_watermark`. Slice 4 wires
//!   the replicator up so it advances automatically.
//!
//! `cozo_archive_segments` (the per-segment manifest) belongs to slice 4 and
//! is not created here.
//!
//! Names use a `cozo_` prefix rather than a leading underscore. Cozo treats
//! relations starting with `_` as transient temp stores (see
//! `Symbol::is_temp_store_name`), which is the wrong semantics for these
//! durable system relations.

use miette::{bail, Result};
use smartstring::{LazyCompact, SmartString};

use crate::data::expr::Expr;
use crate::data::relation::{
    ColType, ColumnDef, NullableColType, StoredRelationMetadata,
};
use crate::data::symb::Symbol;
use crate::data::value::DataValue;
use crate::parse::SourceSpan;
use crate::runtime::relation::InputRelationHandle;
use crate::runtime::transact::SessionTx;

/// Name of the per-relation archive configuration system relation.
pub(crate) const ARCHIVE_CONFIG_REL: &str = "cozo_archive_config";
/// Name of the per-relation archive watermark system relation.
pub(crate) const ARCHIVE_WATERMARK_REL: &str = "cozo_archive_watermark";
/// Name of the per-segment archive manifest system relation.
pub(crate) const ARCHIVE_SEGMENTS_REL: &str = "cozo_archive_segments";

fn col(name: &str, coltype: ColType, nullable: bool, default: Option<Expr>) -> ColumnDef {
    ColumnDef {
        name: SmartString::from(name),
        typing: NullableColType { coltype, nullable },
        default_gen: default,
    }
}

fn handle(
    name: &str,
    keys: Vec<ColumnDef>,
    non_keys: Vec<ColumnDef>,
) -> InputRelationHandle {
    let key_bindings: Vec<Symbol> = keys
        .iter()
        .map(|c| Symbol::new(c.name.as_str(), Default::default()))
        .collect();
    let dep_bindings: Vec<Symbol> = non_keys
        .iter()
        .map(|c| Symbol::new(c.name.as_str(), Default::default()))
        .collect();
    InputRelationHandle {
        name: Symbol::new(name, Default::default()),
        metadata: StoredRelationMetadata { keys, non_keys },
        key_bindings,
        dep_bindings,
        span: SourceSpan(0, 0),
    }
}

fn config_handle() -> InputRelationHandle {
    handle(
        ARCHIVE_CONFIG_REL,
        vec![col("relation", ColType::String, false, None)],
        vec![
            col("timestamp_column", ColType::String, false, None),
            // staging_dir is required only when running the replicator
            // (slice 4). Slice 3 use cases (config + manual watermark advance,
            // no replication) leave it null. Slice 5 generalises this from
            // a local path to a URI: file:// or s3://.
            col("staging_dir", ColType::String, true, None),
            // Slice 5: server-side encryption mode for S3 PUTs.
            // 'none' | 'sse-s3' | 'sse-kms'. Stored as String to allow future
            // values without a schema migration.
            col("encryption", ColType::String, true, None),
            // Slice 5: required iff encryption == 'sse-kms'.
            col("kms_key_arn", ColType::String, true, None),
            // Slice 6: per-segment row cap. When null, the replicator uses
            // DEFAULT_MAX_ROWS_PER_SEGMENT. Bounds encoding memory and keeps
            // segments in a size range datalake readers prefer.
            col("max_rows_per_segment", ColType::Int, true, None),
        ],
    )
}

/// Default per-segment row cap when `cozo_archive_config.max_rows_per_segment`
/// is null. 100k rows × ~500 bytes/row ≈ 50 MB Parquet — within the 100 MB–
/// 1 GB segment size most analytics tools (Spark, DuckDB, Trino) want.
pub(crate) const DEFAULT_MAX_ROWS_PER_SEGMENT: i64 = 100_000;

fn watermark_handle() -> InputRelationHandle {
    handle(
        ARCHIVE_WATERMARK_REL,
        vec![col("relation", ColType::String, false, None)],
        vec![col("last_safe_commit_ts", ColType::Int, false, None)],
    )
}

fn segments_handle() -> InputRelationHandle {
    handle(
        ARCHIVE_SEGMENTS_REL,
        vec![col("segment_id", ColType::Uuid, false, None)],
        vec![
            col("relation", ColType::String, false, None),
            col("file_path", ColType::String, false, None),
            col("lower_commit_ts", ColType::Int, false, None),
            col("upper_commit_ts", ColType::Int, false, None),
            col("key_count", ColType::Int, false, None),
            col("sha256", ColType::Bytes, false, None),
            // Slice 4 only ever writes 'uploaded' (= written to staging dir).
            // Slice 5 will add 'staged' (= local but not yet in S3) and
            // possibly 'aged_out' (= lifecycle-deleted).
            col("status", ColType::String, false, None),
            col("written_at", ColType::Int, false, None),
        ],
    )
}

/// Ensure all archive system relations exist in this transaction. Idempotent
/// — safe to call at the start of every archive sys op.
pub(crate) fn ensure_archive_system_relations(tx: &mut SessionTx<'_>) -> Result<()> {
    if !tx.relation_exists(ARCHIVE_CONFIG_REL)? {
        tx.create_relation(config_handle())?;
    }
    if !tx.relation_exists(ARCHIVE_WATERMARK_REL)? {
        tx.create_relation(watermark_handle())?;
    }
    if !tx.relation_exists(ARCHIVE_SEGMENTS_REL)? {
        tx.create_relation(segments_handle())?;
    }
    Ok(())
}

/// Decoded archive config for a single relation.
#[derive(Debug, Clone)]
pub(crate) struct ArchiveConfigRow {
    pub(crate) timestamp_column: SmartString<LazyCompact>,
    pub(crate) staging_dir: Option<SmartString<LazyCompact>>,
    /// One of 'none' | 'sse-s3' | 'sse-kms'; absent rows default to 'none'.
    pub(crate) encryption: SmartString<LazyCompact>,
    /// Required iff `encryption == "sse-kms"`.
    pub(crate) kms_key_arn: Option<SmartString<LazyCompact>>,
    /// Per-segment row cap. `None` means use `DEFAULT_MAX_ROWS_PER_SEGMENT`.
    pub(crate) max_rows_per_segment: Option<i64>,
}

impl ArchiveConfigRow {
    /// Cap to actually use, applying the default when the user didn't set one.
    pub(crate) fn effective_max_rows_per_segment(&self) -> i64 {
        self.max_rows_per_segment
            .unwrap_or(DEFAULT_MAX_ROWS_PER_SEGMENT)
    }
}

/// Read the full archive config for `relation`. Returns `None` if not configured.
pub(crate) fn get_config(
    tx: &SessionTx<'_>,
    relation: &str,
) -> Result<Option<ArchiveConfigRow>> {
    let cfg = tx.get_relation(ARCHIVE_CONFIG_REL, false)?;
    let key = cfg.encode_key_for_store(
        &[DataValue::Str(SmartString::from(relation))],
        Default::default(),
    )?;
    match tx.store_tx.get(&key, false)? {
        None => Ok(None),
        Some(v) => {
            let val_part = &v[crate::data::tuple::ENCODED_KEY_MIN_LEN..];
            let decoded: Vec<DataValue> = rmp_serde::from_slice(val_part).map_err(|e| {
                miette::miette!("failed to decode {ARCHIVE_CONFIG_REL} row: {e}")
            })?;
            let timestamp_column = match decoded.first() {
                Some(DataValue::Str(s)) => s.clone(),
                _ => bail!("{ARCHIVE_CONFIG_REL} row for '{relation}' is malformed"),
            };
            let staging_dir = match decoded.get(1) {
                Some(DataValue::Str(s)) => Some(s.clone()),
                Some(DataValue::Null) | None => None,
                _ => bail!("{ARCHIVE_CONFIG_REL} row for '{relation}' has malformed staging_dir"),
            };
            let encryption = match decoded.get(2) {
                Some(DataValue::Str(s)) => s.clone(),
                Some(DataValue::Null) | None => SmartString::from("none"),
                _ => bail!("{ARCHIVE_CONFIG_REL} row for '{relation}' has malformed encryption"),
            };
            let kms_key_arn = match decoded.get(3) {
                Some(DataValue::Str(s)) => Some(s.clone()),
                Some(DataValue::Null) | None => None,
                _ => bail!("{ARCHIVE_CONFIG_REL} row for '{relation}' has malformed kms_key_arn"),
            };
            let max_rows_per_segment = match decoded.get(4) {
                Some(DataValue::Null) | None => None,
                Some(d) => Some(d.get_int().ok_or_else(|| {
                    miette::miette!(
                        "{ARCHIVE_CONFIG_REL} row for '{relation}' has malformed max_rows_per_segment"
                    )
                })?),
            };
            Ok(Some(ArchiveConfigRow {
                timestamp_column,
                staging_dir,
                encryption,
                kms_key_arn,
                max_rows_per_segment,
            }))
        }
    }
}

/// Convenience accessor for just the timestamp column (the slice 3 callers).
pub(crate) fn get_timestamp_column(
    tx: &SessionTx<'_>,
    relation: &str,
) -> Result<Option<SmartString<LazyCompact>>> {
    Ok(get_config(tx, relation)?.map(|c| c.timestamp_column))
}

/// Read the watermark for `relation`. Returns `None` if no watermark has been
/// set yet.
pub(crate) fn get_watermark(tx: &SessionTx<'_>, relation: &str) -> Result<Option<i64>> {
    let wm = tx.get_relation(ARCHIVE_WATERMARK_REL, false)?;
    let key = wm.encode_key_for_store(
        &[DataValue::Str(SmartString::from(relation))],
        Default::default(),
    )?;
    match tx.store_tx.get(&key, false)? {
        None => Ok(None),
        Some(v) => {
            let val_part = &v[crate::data::tuple::ENCODED_KEY_MIN_LEN..];
            let decoded: Vec<DataValue> = rmp_serde::from_slice(val_part).map_err(|e| {
                miette::miette!("failed to decode _archive_watermark row: {e}")
            })?;
            match decoded.first() {
                Some(d) => d.get_int().ok_or_else(|| {
                    miette::miette!("_archive_watermark row has non-int last_safe_commit_ts")
                }).map(Some),
                None => bail!("_archive_watermark row for '{relation}' is empty"),
            }
        }
    }
}

/// Upsert a row into `cozo_archive_config`.
pub(crate) fn put_config(
    tx: &mut SessionTx<'_>,
    relation: &str,
    timestamp_column: &str,
    staging_dir: Option<&str>,
    encryption: Option<&str>,
    kms_key_arn: Option<&str>,
    max_rows_per_segment: Option<i64>,
) -> Result<()> {
    let cfg = tx.get_relation(ARCHIVE_CONFIG_REL, false)?;
    let key = cfg.encode_key_for_store(
        &[DataValue::Str(SmartString::from(relation))],
        Default::default(),
    )?;
    let to_str_val = |s: Option<&str>| match s {
        Some(v) => DataValue::Str(SmartString::from(v)),
        None => DataValue::Null,
    };
    let max_rows_val = match max_rows_per_segment {
        Some(n) => DataValue::from(n),
        None => DataValue::Null,
    };
    let val = cfg.encode_val_only_for_store(
        &[
            DataValue::Str(SmartString::from(timestamp_column)),
            to_str_val(staging_dir),
            to_str_val(encryption),
            to_str_val(kms_key_arn),
            max_rows_val,
        ],
        Default::default(),
    )?;
    tx.store_tx.put(&key, &val)?;
    Ok(())
}

/// Remove a relation's archive config (and its watermark, atomically — keeping
/// orphaned watermark rows confuses operators).
pub(crate) fn remove_config(tx: &mut SessionTx<'_>, relation: &str) -> Result<()> {
    let cfg = tx.get_relation(ARCHIVE_CONFIG_REL, false)?;
    let cfg_key = cfg.encode_key_for_store(
        &[DataValue::Str(SmartString::from(relation))],
        Default::default(),
    )?;
    tx.store_tx.del(&cfg_key)?;
    let wm = tx.get_relation(ARCHIVE_WATERMARK_REL, false)?;
    let wm_key = wm.encode_key_for_store(
        &[DataValue::Str(SmartString::from(relation))],
        Default::default(),
    )?;
    tx.store_tx.del(&wm_key)?;
    Ok(())
}

/// Set `cozo_archive_watermark` for the given relation. Slice 3 calls this
/// from the admin sys op `::archive_advance_watermark`; slice 4 calls it
/// from the replicator after a successful segment write.
pub(crate) fn set_watermark(
    tx: &mut SessionTx<'_>,
    relation: &str,
    last_safe_commit_ts: i64,
) -> Result<()> {
    let wm = tx.get_relation(ARCHIVE_WATERMARK_REL, false)?;
    let key = wm.encode_key_for_store(
        &[DataValue::Str(SmartString::from(relation))],
        Default::default(),
    )?;
    let val = wm.encode_val_only_for_store(
        &[DataValue::from(last_safe_commit_ts)],
        Default::default(),
    )?;
    tx.store_tx.put(&key, &val)?;
    Ok(())
}

/// Insert one segment row into `cozo_archive_segments`.
pub(crate) fn insert_segment(
    tx: &mut SessionTx<'_>,
    segment_id: uuid::Uuid,
    relation: &str,
    file_path: &str,
    lower_commit_ts: i64,
    upper_commit_ts: i64,
    key_count: i64,
    sha256: &[u8],
    status: &str,
    written_at: i64,
) -> Result<()> {
    let seg = tx.get_relation(ARCHIVE_SEGMENTS_REL, false)?;
    let key = seg.encode_key_for_store(
        &[DataValue::Uuid(crate::data::value::UuidWrapper(segment_id))],
        Default::default(),
    )?;
    let val = seg.encode_val_only_for_store(
        &[
            DataValue::Str(SmartString::from(relation)),
            DataValue::Str(SmartString::from(file_path)),
            DataValue::from(lower_commit_ts),
            DataValue::from(upper_commit_ts),
            DataValue::from(key_count),
            DataValue::Bytes(sha256.to_vec()),
            DataValue::Str(SmartString::from(status)),
            DataValue::from(written_at),
        ],
        Default::default(),
    )?;
    tx.store_tx.put(&key, &val)?;
    Ok(())
}
