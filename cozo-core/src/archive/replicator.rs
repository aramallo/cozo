/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! Manual-drain replicator (slices 4–6).
//!
//! Polling model: each `::replicate_pending` call scans the configured
//! relation, picks rows whose timestamp column is past the watermark,
//! groups them into one or more Parquet segments respecting the per-segment
//! row cap, writes each to the configured object store, records a manifest
//! entry per segment, and advances the watermark.
//!
//! Idempotent: a second call with no new rows is a no-op.
//!
//! ### Slice 6 — chunking semantics
//!
//! Within a single drain, due rows are sorted by timestamp ascending and
//! split into chunks. Chunk size targets `effective_max_rows_per_segment`,
//! but the chunk is extended forward until the next row's timestamp differs
//! from the chunk's last row — preserving the invariant that each chunk's
//! upper-ts strictly exceeds the prior chunk's. This makes per-chunk
//! watermark advancement safe: a row past the watermark is either in the
//! current chunk or in a later one (never "lost" because two rows shared
//! a ts at the boundary).
//!
//! All chunks land in one transaction. On failure mid-drain, the tx rolls
//! back and any segments already PUT to the object store become orphans —
//! the destination's lifecycle policy is expected to reap them.
//!
//! ### What this replicator still does *not* do
//! - Capture deletes. Rows removed via `:rm` directly (i.e. bypassing
//!   `::archive`) are not replicated.
//! - Bound the *collection* phase memory. The full set of due rows is held
//!   in memory before chunking. True streaming requires a column index on
//!   the timestamp; deferred to a future slice.

use miette::{bail, Result};
use uuid::Uuid;

use crate::archive::export::parquet_bytes_for_rows;
use crate::archive::manifest::{
    get_config, get_watermark, insert_segment, set_watermark, ArchiveConfigRow,
};
use crate::archive::store::{
    build_object_store, iam_probe, join_path, put_blocking,
};
use crate::data::relation::ColumnDef;
use crate::data::value::DataValue;
use crate::runtime::transact::SessionTx;

/// Fixed namespace for content-addressed segment UUIDs (`Uuid::new_v5` over the
/// Parquet SHA-256). Stable across processes so identical content always maps to
/// the same `segment_id` — making manifest inserts idempotent upserts.
const SEGMENT_NAMESPACE: Uuid = Uuid::from_u128(0x6f9b1d2e_7c3a_4b5e_9f01_a2b3c4d5e6f7);

/// Compute the exclusive end index of the next chunk starting at `start`.
/// Targets `cap` rows but extends past the cap until the next row's
/// timestamp differs — keeping ts-tied rows in one segment so the watermark
/// can advance to a clean boundary per chunk.
fn chunk_end(timestamps: &[i64], start: usize, cap: usize) -> usize {
    debug_assert!(start < timestamps.len());
    debug_assert!(cap >= 1);
    let n = timestamps.len();
    let mut end = (start + cap).min(n);
    if end < n {
        let last_ts = timestamps[end - 1];
        while end < n && timestamps[end] == last_ts {
            end += 1;
        }
    }
    end
}

/// Due rows for one drain, collected under a read snapshot (phase 1). Holds the
/// full qualifying set sorted by timestamp ascending. Memory cost is
/// O(qualifying rows); see the module note on the collection phase.
pub(crate) struct DueRows {
    pub(crate) cfg: ArchiveConfigRow,
    pub(crate) all_cols: Vec<ColumnDef>,
    pub(crate) cap: usize,
    pub(crate) old_watermark: i64,
    /// Sorted ascending, parallel to `rows`.
    pub(crate) timestamps: Vec<i64>,
    pub(crate) rows: Vec<Vec<DataValue>>,
}

/// One segment uploaded in phase 2, awaiting a manifest record in phase 3.
pub(crate) struct UploadedSegment {
    pub(crate) segment_id: Uuid,
    pub(crate) file_path: String,
    pub(crate) lower_ts: i64,
    pub(crate) upper_ts: i64,
    pub(crate) key_count: i64,
    pub(crate) sha: [u8; 32],
}

/// **Phase 1 (read snapshot).** Resolve config + watermark and collect the rows
/// whose timestamp exceeds the watermark, sorted ascending. Takes only a shared
/// `SessionTx` (a cheap read snapshot), so no write lock is held while the
/// caller goes on to perform network uploads.
///
/// Uses a timestamp index when one exists whose single leading-and-only indexed
/// column is the configured timestamp column (created via
/// `::index create rel:idx {ts_col}`), range-scanning from `watermark + 1`
/// upward in sorted order; otherwise falls back to a full scan + sort.
pub(crate) fn scan_due_rows(tx: &SessionTx<'_>, relation: &str) -> Result<DueRows> {
    let cfg = get_config(tx, relation)?
        .ok_or_else(|| miette::miette!("relation '{relation}' is not configured for archiving"))?;
    if cfg.staging_dir.is_none() {
        bail!(
            "relation '{relation}' has no staging_dir set; configure with \
            `::archive_config put '{relation}' '<col>' '<staging_dir>'`"
        );
    }
    let old_watermark = get_watermark(tx, relation)?.unwrap_or(i64::MIN);
    // The cap is validated >= 1 at `::archive_config put` time and the default
    // is positive, so a non-positive value here would be a corrupted system
    // relation — out of scope for runtime checks.
    let cap = cfg.effective_max_rows_per_segment() as usize;

    let target = tx.get_relation(relation, false)?;

    let all_cols: Vec<ColumnDef> = target
        .metadata
        .keys
        .iter()
        .chain(target.metadata.non_keys.iter())
        .cloned()
        .collect();
    let ts_idx = all_cols
        .iter()
        .position(|c| c.name.as_str() == cfg.timestamp_column.as_str())
        .ok_or_else(|| {
            miette::miette!(
                "configured timestamp column '{}' not present on '{relation}'",
                cfg.timestamp_column
            )
        })?;

    let n_base_keys = target.metadata.keys.len();
    let (timestamps, rows) = match find_ts_index(&target, cfg.timestamp_column.as_str()) {
        // Index path: range-scan the index from `old_watermark + 1` upward;
        // tuples arrive sorted by ts, so no in-memory sort is needed. For each
        // index tuple `[ts, base_keys…]` fetch the full row by key.
        Some(idx) => {
            let lower = vec![DataValue::from(old_watermark.saturating_add(1))];
            let upper = vec![DataValue::Bot];
            let mut timestamps: Vec<i64> = Vec::new();
            let mut rows: Vec<Vec<DataValue>> = Vec::new();
            for tup_res in idx.scan_bounded_prefix(tx, &[], &lower, &upper) {
                let idx_tup = tup_res?;
                let ts = idx_tup.first().and_then(|v| v.get_int()).ok_or_else(|| {
                    miette::miette!("index on '{relation}' has a non-integer leading column")
                })?;
                let base_key = &idx_tup[1..1 + n_base_keys];
                let full = target.get(tx, base_key)?.ok_or_else(|| {
                    miette::miette!(
                        "index for '{relation}' points at a row that no longer exists"
                    )
                })?;
                timestamps.push(ts);
                rows.push(full);
            }
            (timestamps, rows)
        }
        // Full-scan fallback: scan everything, filter ts > watermark, sort.
        None => {
            let mut due: Vec<(i64, Vec<DataValue>)> = Vec::new();
            for tup_res in target.scan_all(tx) {
                let tup = tup_res?;
                let ts = tup.get(ts_idx).and_then(|v| v.get_int()).ok_or_else(|| {
                    miette::miette!(
                        "row in '{relation}' missing or non-integer timestamp at column '{}'",
                        cfg.timestamp_column
                    )
                })?;
                if ts > old_watermark {
                    due.push((ts, tup));
                }
            }
            due.sort_by_key(|(ts, _)| *ts);
            due.into_iter().unzip()
        }
    };

    Ok(DueRows {
        cfg,
        all_cols,
        cap,
        old_watermark,
        timestamps,
        rows,
    })
}

/// Find an index on `target` usable for timestamp-ordered scanning: its key
/// columns must be exactly `[ts_col, <base key columns in order>]` (what
/// `::index create rel:idx {ts_col}` produces). Anything more elaborate falls
/// back to the full scan so the simple base-key slice `idx_tup[1..]` stays
/// correct.
#[cfg(feature = "archive")]
fn find_ts_index<'h>(
    target: &'h crate::runtime::relation::RelationHandle,
    ts_col: &str,
) -> Option<&'h crate::runtime::relation::RelationHandle> {
    let base_keys = &target.metadata.keys;
    for (idx, _extractor) in target.indices.values() {
        let keys = &idx.metadata.keys;
        if keys.len() != 1 + base_keys.len() {
            continue;
        }
        if keys[0].name.as_str() != ts_col {
            continue;
        }
        if keys[1..]
            .iter()
            .zip(base_keys.iter())
            .all(|(a, b)| a.name == b.name)
        {
            return Some(idx);
        }
    }
    None
}

/// **Phase 2 (no DB transaction).** Chunk the due rows (respecting the
/// per-segment cap + ts-tie rule), encode each chunk to Parquet, and PUT it to
/// the configured object store. Segments are **content-addressed**: the
/// `segment_id` and filename derive from the Parquet SHA-256, so re-uploading
/// identical content overwrites the same object (idempotent under retry and
/// concurrent drains). Returns the per-segment metadata and the new watermark
/// (max replicated ts). No DB lock is held across these network round-trips.
pub(crate) fn upload_due_rows(
    due: &DueRows,
    relation: &str,
) -> Result<(Vec<UploadedSegment>, i64)> {
    let (store, dst) = build_object_store(&due.cfg)?;
    iam_probe(&store, &dst)?;

    let mut segments = Vec::new();
    let mut watermark = due.old_watermark;
    let mut start: usize = 0;

    while start < due.rows.len() {
        let end = chunk_end(&due.timestamps, start, due.cap);
        let chunk = &due.rows[start..end];
        let lower_ts = due.timestamps[start];
        let upper_ts = due.timestamps[end - 1];

        // Encode this chunk in isolation; memory peak is O(chunk size).
        let (bytes, sha) = parquet_bytes_for_rows(&due.all_cols, chunk)?;

        // Content-addressed id + filename: deterministic from the bytes.
        let segment_id = Uuid::new_v5(&SEGMENT_NAMESPACE, &sha);
        let segment_filename = format!("{relation}-{segment_id}.parquet");
        let object_path = join_path(&dst.prefix, &segment_filename);
        put_blocking(&store, &object_path, bytes)?;

        segments.push(UploadedSegment {
            segment_id,
            file_path: dst.canonical_uri(&object_path),
            lower_ts,
            upper_ts,
            key_count: chunk.len() as i64,
            sha,
        });

        // By construction (sorted ts + tie-extending chunk_end) upper_ts is
        // strictly increasing across chunks and exceeds the prior watermark.
        debug_assert!(upper_ts > watermark);
        watermark = upper_ts;
        start = end;
    }

    Ok((segments, watermark))
}

/// **Phase 3 (write transaction).** Record one manifest row per uploaded
/// segment and advance the watermark to `max(existing, new_watermark)`. Manifest
/// inserts are idempotent upserts (content-addressed `segment_id`), so retrying
/// a drain whose phase-3 commit failed records each segment exactly once. The
/// watermark `max` tolerates a concurrent drain having advanced it further.
pub(crate) fn record_uploaded(
    tx: &mut SessionTx<'_>,
    relation: &str,
    segments: &[UploadedSegment],
    new_watermark: i64,
    written_at: i64,
) -> Result<()> {
    for s in segments {
        insert_segment(
            tx,
            s.segment_id,
            relation,
            &s.file_path,
            s.lower_ts,
            s.upper_ts,
            s.key_count,
            &s.sha,
            "uploaded",
            written_at,
        )?;
    }
    let current = get_watermark(tx, relation)?.unwrap_or(i64::MIN);
    set_watermark(tx, relation, current.max(new_watermark))?;
    Ok(())
}

#[cfg(test)]
mod chunk_end_tests {
    use super::chunk_end;

    #[test]
    fn cap_falls_on_a_clean_boundary() {
        // Distinct ts every row; cap=2 gives a clean cut at index 2.
        assert_eq!(chunk_end(&[1, 2, 3, 4, 5], 0, 2), 2);
        assert_eq!(chunk_end(&[1, 2, 3, 4, 5], 2, 2), 4);
        assert_eq!(chunk_end(&[1, 2, 3, 4, 5], 4, 2), 5);
    }

    #[test]
    fn cap_extends_through_ts_ties() {
        // Cap lands inside a ts-tie run; extend until the run ends.
        assert_eq!(chunk_end(&[1, 1, 1, 2, 3], 0, 2), 3);
        // All rows tied: must consume them all in one chunk.
        assert_eq!(chunk_end(&[7, 7, 7, 7, 7], 0, 2), 5);
    }

    #[test]
    fn cap_at_or_past_end() {
        assert_eq!(chunk_end(&[1, 2, 3], 0, 10), 3);
        assert_eq!(chunk_end(&[1, 2, 3], 2, 1), 3);
    }
}
