# Tutorial — archive cozo relations to S3

A worked example, end to end: define a relation, replicate it to durable
storage, archive (delete from cozo) the replicated rows, and restore later
when you need them.

This tutorial assumes you've built cozo with `--features archive`. If you
need real S3 round-trip in your tests, also enable `--features integration-s3`
and follow [TIGRIS.md](TIGRIS.md) for credential setup.

---

## The mental model

Cozo is the hot, queryable store. Old rows are too valuable to delete but too
cold to keep paying premium hot-storage prices for. Three pieces solve this:

1. **A commit-time timestamp on every row** (`commit_now()` default), so we
   can tell which rows have been around long enough to archive.
2. **A replicator** that writes those rows out as Arrow-typed Parquet
   segments to durable storage (local fs or S3-compatible).
3. **A guarded delete** (`::archive`) that only removes rows the replicator
   has confirmed are safe in storage.

Once a row is in storage, a downstream datalake (Iceberg, Delta, raw Parquet
pipeline, …) handles long-term retention, GDPR purges, compaction, and
analytic queries. Cozo never deletes from S3 — that's the datalake's job.

---

## Step 1 — Define the relation

The single requirement is a column that's auto-populated at commit time.
Cozo has a built-in for this: `default commit_now()`. The column is a
normal `Int` (microseconds since the epoch), queryable like any other
column.

```cozo
:create orders {
    id: Int =>
    customer: String,
    amount: Float,
    updated_at: Int default commit_now(),
}
```

Two important properties of `commit_now()`:

- **Uniform per script.** Every row written in a single script invocation
  gets the same timestamp. Useful for batched writes.
- **Re-stamped on update.** Even partial updates that don't bind
  `updated_at` cause it to be bumped. This is what makes the watermark
  guarantee work: an updated row counts as "new" until the replicator
  has seen the new version.

If you'd prefer a different column name (e.g. `last_modified`,
`commit_ts`), it's just a name — pass whatever you call it to
`::archive_config put` below.

---

## Step 2 — Configure archive

Tell cozo which column holds the timestamp and where to send the segments.

### Local filesystem (development, single-machine deployments)

```cozo
::archive_config put 'orders' 'updated_at' '/var/cozo/staging/orders'
```

Cozo creates the directory if it doesn't exist. Files are written as
`orders-<uuid>.parquet`.

### S3-compatible (production)

```cozo
::archive_config put 'orders' 'updated_at' 's3://my-bucket/orders/'
```

For server-side encryption, add the encryption mode (and KMS key ARN, if
applicable):

```cozo
-- SSE-S3 (S3-managed keys)
::archive_config put 'orders' 'updated_at' 's3://my-bucket/orders/' 'sse-s3'

-- SSE-KMS (customer-managed CMK)
::archive_config put 'orders' 'updated_at' 's3://my-bucket/orders/'
    'sse-kms' 'arn:aws:kms:us-east-1:000000000000:key/abc-123'
```

Credentials are **never** stored in cozo. The S3 client reads them from the
standard AWS env-var chain: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_REGION`, optional `AWS_ENDPOINT_URL` for non-AWS endpoints,
optional `AWS_SESSION_TOKEN`. In server deployments, prefer IAM instance
profiles / IRSA — same SDK chain, no env vars at all.

Confirm the configuration was stored:

```cozo
::archive_config get
-- relation | timestamp_column | staging_dir              | encryption | kms_key_arn | max_rows_per_segment
-- orders   | updated_at       | s3://my-bucket/orders/   | sse-s3     | <null>      | <null>
```

### Per-segment row cap (optional)

A drain produces one or more Parquet segments. By default, each segment
holds up to **100,000 rows**; a single drain that has more pending rows
splits them across multiple segments rather than producing one giant
file. This bounds encoding memory and keeps segments in the size range
analytics tools (Spark, DuckDB, Trino) prefer.

To override, pass an unquoted integer as the 6th positional argument:

```cozo
-- Smaller segments — useful for tiny test buckets or low-memory hosts.
::archive_config put 'orders' 'updated_at' 's3://my-bucket/orders/' 5000

-- Larger segments — fewer S3 PUTs at the cost of higher encoding peak.
::archive_config put 'orders' 'updated_at' 's3://my-bucket/orders/' 'sse-kms'
    'arn:aws:kms:us-east-1:000000000000:key/abc-...' 1000000
```

The parser tells string slots apart from the integer slot, so you can
skip the encryption / kms slots when you only need to set the cap.

When two rows share the same timestamp at a chunk boundary, the chunk
extends forward until the timestamp changes — so the watermark can
advance to a clean boundary even if you have many rows in one logical
commit (e.g. a large `:put` script).

---

## Step 3 — Write data, then replicate

Insert rows the normal way:

```cozo
?[id, customer, amount] <- [
    [1, 'alice', 12.50],
    [2, 'bob',   7.00],
    [3, 'carol', 99.99],
]
:put orders {id => customer, amount}
```

Inspect what's queued for replication:

```cozo
?[id, updated_at] := *orders{id, updated_at}
```

All three rows share one `updated_at` value (one script, one timestamp).

Now replicate:

```cozo
::replicate_pending 'orders'
-- status | rows_replicated | segments_written | old_watermark         | new_watermark
-- OK     | 3               | 1                | -9223372036854775808  | 1735689600000000
```

The response is a single status row regardless of how many segments were
written. Per-segment details (uuid, file path, ts range, sha) live in
`cozo_archive_segments` and are queryable as a normal cozo relation:

```cozo
?[seg, file, lower, upper, count] := *cozo_archive_segments{
    relation: 'orders',
    segment_id: seg, file_path: file,
    lower_commit_ts: lower, upper_commit_ts: upper, key_count: count
}
```

What happened internally:

1. Cozo scanned `orders` for rows whose `updated_at > current watermark`.
   On a fresh setup, the watermark is `i64::MIN`, so every row qualifies.
2. The matching rows were encoded into one Arrow-typed Parquet segment in
   memory.
3. SHA-256 of the bytes was computed.
4. (S3 only) The IAM probe ran — confirmed cozo's credentials cannot
   `DeleteObject`. See [TIGRIS.md](TIGRIS.md) if this fails on a non-AWS
   service.
5. The segment was uploaded.
6. A row was inserted into the `cozo_archive_segments` manifest.
7. The watermark advanced to the segment's max timestamp.

The segment is now durable. Inspect the manifest like any cozo relation:

```cozo
?[seg, count, status, sha] := *cozo_archive_segments{
    segment_id: seg, key_count: count, status, sha256: sha
}
```

Calling `::replicate_pending 'orders'` again is a no-op — there are no rows
past the watermark. Idempotent by construction.

---

## Step 4 — Archive (= delete from cozo)

Once replicated, you can safely remove the rows from cozo. `::archive`
takes a datalog query that produces the keys to consider, and deletes only
those whose timestamp is at or below the watermark:

```cozo
::archive orders { ?[id] := *orders{id} }
-- status | archived | skipped | missing | watermark
-- OK     | 3        | 0       | 0       | 1735689600000000
```

Three counters:

- **archived** — rows whose `updated_at <= watermark`; deleted.
- **skipped** — rows whose `updated_at > watermark`; not yet safe to delete.
  These weren't replicated yet (run `::replicate_pending` first).
- **missing** — rows the query named that don't exist in the relation
  (already archived, or never existed).

You can be selective about what to archive:

```cozo
-- Archive only the high-value orders.
::archive orders {
    ?[id] := *orders{id, amount}, amount > 100
}
```

Or limit by timestamp:

```cozo
-- Archive everything older than a specific cutoff.
::archive orders {
    ?[id] := *orders{id, updated_at}, updated_at < 1700000000000000
}
```

The watermark gate composes on top of whatever your query produces. Even if
the query says "archive everything," only replicated rows are deleted.

---

## Step 5 — Restore

Restoration is rare. When it's needed, point cozo's generic
`::import_parquet` at the segment's URI:

```cozo
-- Find the segment(s) you want to restore.
?[seg, file] := *cozo_archive_segments{
    segment_id: seg, file_path: file, relation: 'orders'
}

-- Then restore one back into a relation.
::import_parquet orders from 'file:///var/cozo/staging/orders/orders-<uuid>.parquet'
```

For S3 segments, the `file_path` in the manifest is the full `s3://` URI.
**Slice 5 of cozo doesn't yet support importing directly from `s3://`** —
you'd download the segment first (e.g. via `aws s3 cp`) and import from a
local path. This will be lifted in a future slice.

In a production setup, restore typically goes through your datalake (which
ingested the cozo segment earlier) rather than directly from cozo's raw
zone. The cozo manifest is for audit and "where can I find this row" lookups.

---

## Operational reference

### Inspect state at any time

The three system relations are queryable like any other cozo relation:

```cozo
-- Configured relations.
?[r, c, dir, enc] := *cozo_archive_config{
    relation: r, timestamp_column: c, staging_dir: dir, encryption: enc
}

-- Current watermarks.
?[r, ts] := *cozo_archive_watermark{
    relation: r, last_safe_commit_ts: ts
}

-- All segments produced so far.
?[seg, rel, file, count, ts_low, ts_high] := *cozo_archive_segments{
    segment_id: seg, relation: rel, file_path: file,
    key_count: count, lower_commit_ts: ts_low, upper_commit_ts: ts_high
}
```

### Force a watermark (admin escape hatch)

If you need to advance the watermark without running the replicator (e.g.
after a disaster-recovery scenario where you've already restored segments
out-of-band):

```cozo
::archive_advance_watermark 'orders' 1735689600000000
```

Use sparingly — it bypasses the "must be replicated" guarantee. Slice 4's
replicator is the production path.

### Tear down

```cozo
::archive_config remove 'orders'
```

Removes the config row and the watermark. Existing segments in storage are
left untouched (cozo never deletes from S3).

---

## Common pitfalls

**Forgetting `::replicate_pending` before `::archive`.** Without it, the
watermark is at its initial `i64::MIN` and `::archive` will report
everything as `skipped`. Run replicate first.

**`:rm` without `::archive`.** Direct `:rm` on a configured relation removes
the row from cozo without replicating it. The replicator is polling-based;
it only sees what's currently in the relation. If you `:rm` a row that
hasn't been replicated yet, the row is gone — no S3 copy, no audit trail.
Always use `::archive` for the replicate-then-delete flow.

**Schema changes after configuration.** Renaming the timestamp column or
changing its type doesn't update the `cozo_archive_config` row. Re-run
`::archive_config put` if you change the schema.

**Big drains.** A drain splits its rows into multiple segments, capped by
default at 100,000 rows each — encoding peak memory is O(cap), not O(due
rows). The *collection* phase still holds every qualifying row in memory
to sort by ts; relations with millions of pending rows should drain in
windows (advance the watermark to a midpoint, drain again) until a future
slice adds an index-driven streaming path.

**The IAM probe failing on non-AWS S3.** See [TIGRIS.md](TIGRIS.md).

---

## Quick reference: sys ops

| Op | Purpose |
|---|---|
| `::archive_config put '<rel>' '<ts_col>' [<uri>] [<enc>] [<kms>] [<max_rows>]` | Configure a relation. |
| `::archive_config get [<rel>]` | List configurations. |
| `::archive_config remove '<rel>'` | Remove configuration + watermark. |
| `::replicate_pending '<rel>'` | Drain pending rows; advance watermark. |
| `::archive_advance_watermark '<rel>' <ts>` | Admin-only watermark set. |
| `::archive <rel> { <query> }` | Guarded delete. |
| `::import_parquet <rel> from '<uri>'` | Generic Parquet importer. |
