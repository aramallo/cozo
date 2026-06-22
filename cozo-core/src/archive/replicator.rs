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
    get_config, get_watermark, insert_segment, set_watermark,
};
use crate::archive::store::{
    build_object_store, iam_probe, join_path, put_blocking,
};
use crate::data::relation::ColumnDef;
use crate::data::value::{DataValue, ValidityTs};
use crate::runtime::transact::SessionTx;

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

/// Outcome of one drain call. Per-segment details (UUID, file path,
/// per-segment ts range) are deliberately not part of this struct — they
/// live in `cozo_archive_segments` and are queryable by manifest writes
/// having `written_at >= old_watermark`.
#[derive(Debug)]
pub(crate) struct DrainOutcome {
    pub(crate) rows_replicated: i64,
    pub(crate) segments_written: i64,
    pub(crate) old_watermark: i64,
    pub(crate) new_watermark: i64,
}

pub(crate) fn drain_relation(
    tx: &mut SessionTx<'_>,
    relation: &str,
    cur_vld: ValidityTs,
) -> Result<DrainOutcome> {
    // 1) Resolve config + watermark.
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

    // 2) Locate the timestamp column in the row layout.
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

    // 3) Full scan; filter to ts > old_watermark; collect (ts, row) pairs so
    //    the timestamp is extracted exactly once per row. Memory cost:
    //    O(qualifying rows). True streaming requires an index on the
    //    timestamp column — deferred to a future slice.
    let mut due: Vec<(i64, Vec<DataValue>)> = Vec::new();
    for tup_res in target.scan_all(tx) {
        let tup = tup_res?;
        let ts = tup
            .get(ts_idx)
            .and_then(|v| v.get_int())
            .ok_or_else(|| {
                miette::miette!(
                    "row in '{relation}' missing or non-integer timestamp at column '{}'",
                    cfg.timestamp_column
                )
            })?;
        if ts > old_watermark {
            due.push((ts, tup));
        }
    }

    if due.is_empty() {
        return Ok(DrainOutcome {
            rows_replicated: 0,
            segments_written: 0,
            old_watermark,
            new_watermark: old_watermark,
        });
    }

    // 4) Sort by ts ascending so per-chunk watermark advancement is safe.
    due.sort_by_key(|(ts, _)| *ts);
    let (timestamps, due_rows): (Vec<i64>, Vec<Vec<DataValue>>) = due.into_iter().unzip();

    // 5) Build the ObjectStore from the configured destination. Same code
    //    path serves file://, s3://, and any future backend.
    let (store, dst) = build_object_store(&cfg)?;
    iam_probe(&store, &dst)?;

    // 6) Walk sorted rows, emit one segment per chunk. `chunk_end` enforces
    //    the cap + ts-tie extension invariant (see its docstring).
    let total_rows = due_rows.len() as i64;
    let mut watermark = old_watermark;
    let mut segments_written: i64 = 0;
    let mut start: usize = 0;

    while start < due_rows.len() {
        let end = chunk_end(&timestamps, start, cap);
        let chunk = &due_rows[start..end];
        let chunk_lower_ts = timestamps[start];
        let chunk_upper_ts = timestamps[end - 1];

        // Encode this chunk's bytes in isolation. Memory peak is O(chunk
        // size), independent of total drain size.
        let (bytes, sha) = parquet_bytes_for_rows(&all_cols, chunk)?;

        let segment_id = Uuid::new_v4();
        let segment_filename = format!("{}-{}.parquet", relation, segment_id);
        let object_path = join_path(&dst.prefix, &segment_filename);
        put_blocking(&store, &object_path, bytes)?;

        let file_path_str = dst.canonical_uri(&object_path);

        insert_segment(
            tx,
            segment_id,
            relation,
            &file_path_str,
            chunk_lower_ts,
            chunk_upper_ts,
            chunk.len() as i64,
            &sha,
            "uploaded",
            cur_vld.0 .0,
        )?;

        // Advance the watermark per chunk. By construction (sorted ts +
        // tie-extending chunk_end), chunk_upper_ts > watermark.
        debug_assert!(chunk_upper_ts > watermark);
        watermark = chunk_upper_ts;
        set_watermark(tx, relation, watermark)?;
        segments_written += 1;

        start = end;
    }

    Ok(DrainOutcome {
        rows_replicated: total_rows,
        segments_written,
        old_watermark,
        new_watermark: watermark,
    })
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
