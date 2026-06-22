# Archive subsystem

Continuously replicate cozo relations to durable storage, then prune old rows
from cozo with the guarantee that they have been replicated first.

Currently shipped:
1. `commit_now()` cozo-managed timestamps
2. generic `::import_parquet`
3. manifest, watermark, `::archive`
4. manual-drain replicator (local filesystem)
5. object-store backend (`s3://`, `file://`), encryption (none / sse-s3 /
  sse-kms), startup IAM probe
6. per-segment row caps with default 100,000 rows; sort + ts-tie-aware
  chunking so one drain produces multiple right-sized segments rather than
  one giant one

## Use this when

You want a relation in cozo to be the hot, queryable store, but eventually have
its older rows live in a datalake (Iceberg, Delta, Ducklake, raw Parquet) where they can be cheaply retained, queried by Spark/DuckDB/Trino, and managed for
compliance.

The cozo side gives you:

- a **replicated copy** of every committed row, written as Arrow-typed Parquet
- a **watermark** so cozo never deletes a row that hasn't been durably copied
- an **`::archive`** sys op that prunes the cozo copy of replicated rows
- a **`::import_parquet`** sys op for restoring rows back into cozo

The downstream datalake handles long-term retention, GDPR row-level deletes,
compaction, and analytic queries. Cozo never deletes from S3 — that's the
datalake's job.

## Quick start

```cozo
:create orders {
    id: Int =>
    customer: String,
    amount: Float,
    updated_at: Int default commit_now(),  -- cozo-managed commit timestamp
}

::archive_config put 'orders' 'updated_at' '/var/cozo/staging/orders'

-- write some rows
?[id, customer, amount] <- [[1, 'alice', 12.5], [2, 'bob', 7.0]]
    :put orders {id => customer, amount}

-- replicate (writes a Parquet segment to the staging dir, advances watermark)
::replicate_pending 'orders'

-- archive: delete cozo's copy of replicated rows
::archive orders { ?[id] := *orders{id} }
```

For an end-to-end walkthrough including S3 setup, encryption modes, and
restore: see [TUTORIAL.md](TUTORIAL.md).

For production setup on AWS S3 (bucket creation, KMS, IAM policies,
Object Lock, lifecycle, threat model): see [AWS_S3.md](AWS_S3.md).

For Tigris and other S3-compatible services without per-action IAM, plus
background on the `COZO_ARCHIVE_SKIP_IAM_PROBE` escape hatch: see
[TIGRIS.md](TIGRIS.md).

## Sys ops

| Op | Purpose |
|---|---|
| `::archive_config put '<rel>' '<ts_col>' ['<uri>'] ['<encryption>'] ['<kms_arn>'] [<max_rows_per_segment>]` | Configure a relation for archiving. URI is required to run the replicator; supports `s3://bucket/prefix/`, `file:///abs/path/`, and bare paths (slice 4 compat). Encryption is one of `none` / `sse-s3` / `sse-kms`. `max_rows_per_segment` is an unquoted integer; default 100,000. The parser distinguishes string from integer slots so middle slots can be skipped: `put 'r' 'ts' '/tmp/x' 50000`. |
| `::archive_config get [ '<rel>' ]` | List configurations. |
| `::archive_config remove '<rel>'` | Remove configuration and watermark. |
| `::replicate_pending '<rel>'` | Manual drain: write all rows past the watermark to a Parquet segment, advance the watermark. Idempotent. |
| `::archive_advance_watermark '<rel>' <ts>` | Admin/test op — set the watermark directly. Slice 4's replicator advances it; this is the manual escape hatch. |
| `::archive <rel> { <query> }` | Delete from `<rel>` the rows whose timestamp ≤ watermark. Returns `archived/skipped/missing` counts. |
| `::import_parquet <rel> from '<path>'` | Generic Parquet importer. Used for restore but useful elsewhere. |

## System relations

These are regular cozo relations and queryable as such. The `cozo_` prefix
distinguishes them from user relations (a leading `_` in cozo names a temp
store, which would not persist — these need to).

| Relation | Schema (keys → values) |
|---|---|
| `cozo_archive_config` | `relation` → `timestamp_column`, `staging_dir?`, `encryption?`, `kms_key_arn?`, `max_rows_per_segment?` |
| `cozo_archive_watermark` | `relation` → `last_safe_commit_ts` |
| `cozo_archive_segments` | `segment_id` → `relation`, `file_path`, `lower_commit_ts`, `upper_commit_ts`, `key_count`, `sha256`, `status`, `written_at` |

## Module layout

```
archive/
├── mod.rs           — feature gate + submodule wiring
├── manifest.rs      — system relation schemas + CRUD helpers
├── type_mapping.rs  — Arrow array  -> cozo DataValue (used by import)
├── import.rs        — Parquet file -> rows; URI resolution
├── export.rs        — rows         -> Parquet bytes; SHA-256 over bytes
├── store.rs         — URI parsing, ObjectStore building, sync wrappers, IAM probe
├── replicator.rs    — drain_relation: scan, filter, encode, PUT, advance watermark
└── README.md        — this file
```

The replicator uses a **polling model**: each `::replicate_pending` call scans
the relation, picks rows with `ts > watermark`, and writes one segment. It
does *not* subscribe to commit callbacks. Idempotent: a second call with no
new rows is a no-op.

## Behaviour and limits (slices 1–5)

- **Polling, not callback-driven.** A drain is O(N) over the relation's rows.
  For large relations, create your own user index on the timestamp column to
  let cozo seek by it instead of full-scanning. (An auto-index on the
  commit timestamp column is on the roadmap.)
- **Direct `:rm` is not captured.** The replicator only sees rows currently
  present in the relation. If you `:rm` a row directly (i.e., bypass
  `::archive`), the row is gone from cozo without ever being replicated.
  Use `::archive` for the replicate-then-delete flow.
- **Per-segment row cap, default 100,000.** A drain still drains every
  qualifying row, but splits them into multiple right-sized segments. Cap
  is configurable via the 6th positional arg of `::archive_config put`. The
  ts-tie extension keeps rows that share a timestamp in the same segment so
  the watermark can advance safely per chunk. Encoding memory peaks at
  O(cap), independent of total drain size.
- **Collection-phase memory is still O(qualifying rows).** The full
  qualifying set is held in memory during a drain to be sorted by ts before
  chunking. True streaming requires an index on the timestamp column —
  deferred to a future slice. For backlogs in the millions of rows this is
  measurable; for typical workloads it is not.
- **No HNSW / FTS / LSH support.** Relations with these specialized indices
  cannot currently be archived. The replicator and `::archive` both refuse
  with clear errors.
- **Triggers do not fire on archive.** `::archive` deletes via the raw store
  layer, intentionally — archive is a bulk-load semantic, not a logical
  change. Use `:rm` if you need rm-triggers to fire.

## Testing

```bash
# Default features (no archive code reachable):
cargo test -p cozo --lib

# With archive enabled (~256 tests):
cargo test -p cozo --lib --features archive
```

Test modules:

- `archive::type_mapping` and `archive::import` and `archive::export` — unit
  tests on the I/O primitives, using `tempfile` for staging.
- `runtime::tests::commit_now_*` — slice 1
- `runtime::tests::import_parquet_tests` — slice 2
- `runtime::tests::archive_tests` — slice 3
- `runtime::tests::replicate_tests` — slice 4 (includes the round-trip via
  `::import_parquet`)

## S3 / object-store configuration (slice 5)

Cozo never reads or stores credentials. The AWS SDK env-var chain
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
`AWS_ENDPOINT_URL` / `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT`,
`AWS_SESSION_TOKEN`) is the source of truth. Cozo bridges
`AWS_ENDPOINT_URL{_S3}` → `AWS_ENDPOINT` transparently so any of the
common naming conventions work.

Tested against real S3-compatible service: **Tigris** (smoke layer behind
`integration-s3` feature flag).

### IAM policy (recommended)

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::your-bucket",
      "arn:aws:s3:::your-bucket/your-prefix/*"
    ]
  }]
}
```

The startup IAM probe refuses any credential set that can `DeleteObject`.
This is enforced as an architectural rule: a compromised cozo process should
never be able to remove archived segments. To verify, cozo issues one
`DeleteObject` against a non-existent key and expects `AccessDenied`.

If your S3-compatible provider has only a coarse access-key model (read-only
vs read-write, no per-action scoping), set
`COZO_ARCHIVE_SKIP_IAM_PROBE=1` to bypass the probe. A loud warning is
printed every time the probe is skipped. Prefer providers that support
per-action IAM for any production-grade deployment.

## What's not yet implemented

- HNSW / FTS / LSH index support: relations with these indices cannot
  currently be archived.
- Auto-index on the commit-time column: the replicator does a full scan per
  drain. Users with large relations should create their own index on the
  configured timestamp column.
- Direct-`:rm` capture: rows removed by `:rm` (not by `::archive`) bypass
  replication. The replicator only sees rows currently present.
- Autonomous (background-thread) replication: only manual drain is supported
  today.
