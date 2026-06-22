# AWS S3 setup

How to configure AWS S3 for cozo's archive subsystem in production. Unlike
Tigris, AWS S3 enforces full per-action IAM, so the IAM probe works as
designed and the architectural rule "cozo cannot delete from S3" is enforced
at the credential level.

For the equivalent guide for Tigris (and any other S3-compatible service
without per-action IAM), see [TIGRIS.md](TIGRIS.md). For an end-to-end
walkthrough of the archive feature itself, see [TUTORIAL.md](TUTORIAL.md).

---

## Recommended posture (TL;DR)

For most production deployments:

- **One bucket** per cozo environment (`prod`, `staging`, …), or one
  bucket with separate prefixes per environment.
- **Default bucket encryption: SSE-KMS** with a customer-managed key.
- **Versioning enabled** on the bucket.
- **Two IAM principals:**
  - cozo's runtime principal (write-only): `PutObject` + `GetObject` +
    `ListBucket` only — the IAM probe enforces "no `DeleteObject`."
  - A separate principal for lifecycle / GDPR purges (used by your
    operations team or by a separate service, never loaded into cozo).
- **Lifecycle policy** to expire raw segments after your bounded landing-
  zone window (commonly 7–30 days), so they're cleaned up after the
  datalake has ingested.
- **Credentials provided via IAM instance profile / IRSA** (server
  deployments) or env-var chain (local dev). Never via cozo config.

For regulated workloads (HIPAA, SOC 2, GDPR with strict retention SLAs),
add Object Lock in Governance mode.

The rest of this doc walks through each piece.

---

## Step 1 — Create the bucket

```bash
# Replace with your desired name and region.
aws s3api create-bucket \
    --bucket cozo-archive-prod \
    --region us-east-1

# For regions other than us-east-1, the API requires a location constraint:
aws s3api create-bucket \
    --bucket cozo-archive-prod \
    --region eu-west-1 \
    --create-bucket-configuration LocationConstraint=eu-west-1
```

Use a unique, descriptive name. Bucket names are global — `cozo-archive`
won't work for you because it's already taken.

---

## Step 2 — Configure default encryption

The recommended setting is SSE-KMS with a customer-managed CMK. This gives
you separately auditable key access (CloudTrail logs every Decrypt event)
and the ability to revoke access by disabling the key.

### 2a. Create a KMS key

```bash
aws kms create-key \
    --description "Cozo archive encryption key (prod)" \
    --tags TagKey=purpose,TagValue=cozo-archive-prod

# Note the KeyId / Arn returned. Optionally give it a friendly alias:
aws kms create-alias \
    --alias-name alias/cozo-archive-prod \
    --target-key-id <key-id-from-above>
```

### 2b. Set the key policy

The default key policy gives the account root user full access. That's
fine to start. If you want to restrict who can use the key (e.g. only
the cozo IAM principal), update the key policy to grant `kms:GenerateDataKey`
+ `kms:Decrypt` to that principal specifically. Most deployments leave
the default and rely on bucket+IAM to scope access.

### 2c. Enable default encryption on the bucket

```bash
aws s3api put-bucket-encryption \
    --bucket cozo-archive-prod \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms",
                "KMSMasterKeyID": "arn:aws:kms:us-east-1:000000000000:key/abc-..."
            },
            "BucketKeyEnabled": true
        }]
    }'
```

`BucketKeyEnabled: true` reduces KMS API costs by amortising encryption
context across multiple objects in the same bucket. Standard
recommendation.

### 2d. Tell cozo to use SSE-KMS

```cozo
::archive_config put 'orders' 'updated_at' 's3://cozo-archive-prod/orders/'
    'sse-kms' 'arn:aws:kms:us-east-1:000000000000:key/abc-...'
```

Cozo will send `x-amz-server-side-encryption: aws:kms` and
`x-amz-server-side-encryption-aws-kms-key-id: <arn>` headers on every
PUT. AWS still applies the bucket default if cozo doesn't send the
header (so even with `'none'` configured, the data ends up encrypted at
rest), but explicit headers make the encryption choice auditable per
object.

### Alternative: SSE-S3

If you don't want to manage a KMS key, SSE-S3 (AES-256 with AWS-managed
keys) is the cheaper, simpler option:

```bash
aws s3api put-bucket-encryption \
    --bucket cozo-archive-prod \
    --server-side-encryption-configuration '{
        "Rules": [{
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "AES256"
            }
        }]
    }'
```

```cozo
::archive_config put 'orders' 'updated_at' 's3://cozo-archive-prod/orders/'
    'sse-s3'
```

Use SSE-S3 if you're not subject to "encryption keys must be customer-
controlled" compliance requirements. Use SSE-KMS otherwise.

---

## Step 3 — Enable versioning

Strong recommendation regardless of compliance posture. Versioning makes
deletes soft (a delete marker is added; prior versions remain) and turns
overwrites into versioned writes. Recoverable via the AWS console or
`aws s3api list-object-versions`.

```bash
aws s3api put-bucket-versioning \
    --bucket cozo-archive-prod \
    --versioning-configuration Status=Enabled
```

This costs you a small amount of additional storage (each version is
billed) but is invaluable when something goes wrong. Combine with the
lifecycle policy in Step 6 to expire old versions on a schedule.

---

## Step 4 — Configure Object Lock (optional, for compliance)

If you're subject to HIPAA, SOC 2 Type II, FINRA 17a-4, or similar, add
Object Lock in **Governance mode**.

⚠️ Object Lock can only be enabled at bucket creation time. If your
bucket already exists without Object Lock, you'll need to recreate it.
Enable from the start if you might need it.

```bash
aws s3api create-bucket \
    --bucket cozo-archive-prod \
    --region us-east-1 \
    --object-lock-enabled-for-bucket

# Set a default retention (objects are locked for N days unless explicitly
# overridden — Governance mode allows the override with a special permission).
aws s3api put-object-lock-configuration \
    --bucket cozo-archive-prod \
    --object-lock-configuration '{
        "ObjectLockEnabled": "Enabled",
        "Rule": {
            "DefaultRetention": {
                "Mode": "GOVERNANCE",
                "Days": 90
            }
        }
    }'
```

**Mode choice:**
- **Governance mode** (recommended): Locks objects against deletion for
  the retention period. Users with `s3:BypassGovernanceRetention` can
  delete (necessary for GDPR right-to-erasure). Use for most compliance
  scenarios.
- **Compliance mode**: Locks objects against deletion *even by the root
  account* until the retention expires. Cannot bypass. Use only if you
  have a specific WORM regulatory requirement (e.g. SEC 17a-4) and
  understand you're giving up GDPR compliance.

The cozo IAM principal does **not** get `s3:BypassGovernanceRetention`.
Only the GDPR-purge / lifecycle principal does, and only when actually
needed.

---

## Step 5 — Create the IAM principals

Two principals: one for cozo (write-only), one for lifecycle/purge
(separate, used outside cozo).

### 5a. Cozo runtime IAM policy

Save as `cozo-runtime-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::cozo-archive-prod"
    },
    {
      "Sid": "ReadWriteObjects",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::cozo-archive-prod/*"
    },
    {
      "Sid": "UseKmsKey",
      "Effect": "Allow",
      "Action": [
        "kms:GenerateDataKey",
        "kms:Decrypt"
      ],
      "Resource": "arn:aws:kms:us-east-1:000000000000:key/abc-..."
    }
  ]
}
```

The KMS statement is needed only if you're using SSE-KMS (Step 2). Drop
it for SSE-S3 / no encryption.

Notably absent: `s3:DeleteObject`, `s3:DeleteObjectVersion`,
`s3:DeleteBucket`, and `s3:BypassGovernanceRetention`. The cozo IAM
probe verifies this at runtime.

### 5b. Choose principal type

**For server deployments (recommended):**
- **EC2:** create an IAM role; attach the policy; assign the role as the
  EC2 instance profile.
- **ECS Fargate:** create a task role; attach the policy; specify it in
  the task definition.
- **EKS:** create a role with IRSA (IAM Roles for Service Accounts);
  attach the policy.
- **Lambda:** create an execution role; attach the policy.

These options give cozo credentials automatically rotated by AWS, with
no env vars or long-lived secrets to manage.

**For local development:**
- Create an IAM user; attach the policy; generate access keys.
- Store in `~/.aws/credentials` or env vars in `.env` (with
  `dotenvy::dotenv()` at test setup; `.env` in `.gitignore`).

### 5c. Lifecycle / GDPR-purge principal (separate)

Save as `cozo-purge-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucket",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::cozo-archive-prod"
    },
    {
      "Sid": "DeleteObjects",
      "Effect": "Allow",
      "Action": [
        "s3:DeleteObject",
        "s3:DeleteObjectVersion"
      ],
      "Resource": "arn:aws:s3:::cozo-archive-prod/*"
    },
    {
      "Sid": "BypassGovernanceForPurges",
      "Effect": "Allow",
      "Action": "s3:BypassGovernanceRetention",
      "Resource": "arn:aws:s3:::cozo-archive-prod/*"
    }
  ]
}
```

The `BypassGovernanceRetention` permission is only needed if you've
enabled Object Lock in Governance mode (Step 4). Drop it otherwise.

This principal is used by **lifecycle policies, GDPR-response tooling,
and operations runbooks** — never by cozo. Hand it to a small group of
trusted operators or to a dedicated automation user.

---

## Step 6 — Lifecycle policy (raw landing zone retention)

The architectural decision in slice 5 is that cozo writes to a "raw
landing zone" and the datalake takes ownership for long-term retention.
Cozo's segments are bounded — they age out of the bucket on a schedule
so you're not paying to store the same data twice.

```bash
cat > lifecycle.json <<'EOF'
{
    "Rules": [
        {
            "Id": "ExpireRawCozoSegments",
            "Status": "Enabled",
            "Filter": { "Prefix": "" },
            "Expiration": {
                "Days": 30
            },
            "NoncurrentVersionExpiration": {
                "NoncurrentDays": 30
            }
        }
    ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket cozo-archive-prod \
    --lifecycle-configuration file://lifecycle.json
```

Pick a window (7, 14, 30, 90 days) that's longer than your datalake
ingest SLA. If your ingest takes ≤ 24h, 7 days is plenty of slack. If
ingest is daily and might fall behind for a weekend, 14+. The rule of
thumb: ingest SLA × 3 minimum.

`NoncurrentVersionExpiration` cleans up old versions if Versioning is
enabled. Without it, versions accumulate forever.

If Object Lock is enabled, the lifecycle expiration only takes effect
after the retention period — Object Lock wins. So a 30-day Object Lock
retention + 30-day lifecycle means objects are deleted at ~60 days.
Combine carefully.

---

## Step 7 — Wire credentials to cozo

### Server deployment (instance profile / IRSA)

Nothing to do — the AWS SDK chain finds the role automatically. Verify
with:

```bash
aws sts get-caller-identity
# Should return the cozo runtime role's ARN.
```

### Local development (env vars)

In `.env` at the repo root (already in `.gitignore`):

```
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
COZO_TEST_S3_BUCKET=cozo-archive-prod
COZO_TEST_S3_PREFIX=smoke-tests/
```

For non-AWS regions, also set `AWS_REGION` to the right value (e.g.
`eu-west-1`). Cozo bridges `AWS_ENDPOINT_URL` → `AWS_ENDPOINT`
automatically — no special handling needed for AWS S3 (only matters for
non-AWS endpoints).

Configure cozo:

```cozo
::archive_config put 'orders' 'updated_at' 's3://cozo-archive-prod/orders/'
    'sse-kms' 'arn:aws:kms:us-east-1:000000000000:key/abc-...'
```

---

## Step 8 — Verify

### 8a. Smoke test (no cozo data involved)

Confirm credentials and basic access:

```bash
aws s3 ls s3://cozo-archive-prod/
# Should return successfully, even if the bucket is empty.
```

Confirm the IAM probe will pass:

```bash
aws s3api delete-object \
    --bucket cozo-archive-prod \
    --key __smoke-probe-$(uuidgen)
# EXPECTED: AccessDenied error.
# If it succeeds (returns nothing or 204), your runtime principal can
# delete and the IAM probe will refuse it. Fix: re-check that
# s3:DeleteObject is NOT in the runtime policy.
```

### 8b. End-to-end via cozo's smoke tests

```bash
cargo test -p cozo --lib --features integration-s3 smoke
```

All three tests should pass:
- `smoke_replicate_to_s3_round_trips_via_in_memory_bytes`
- `smoke_iam_probe_passes_for_well_scoped_credentials`
- `smoke_no_replication_when_no_new_rows`

If `smoke_iam_probe_passes_for_well_scoped_credentials` fails with
"role can DeleteObject," your runtime IAM policy still allows delete.
Audit it: it should match Step 5a exactly.

---

## Configuration matrix

| Scenario | Encryption | Versioning | Object Lock | Lifecycle | IAM probe | max_rows_per_segment |
|---|---|---|---|---|---|---|
| Dev / hobby | SSE-S3 | optional | no | optional | on | default (100k) |
| Standard production | SSE-KMS (CMK) | yes | no | yes (30d) | on | default (100k) |
| High-throughput production | SSE-KMS (CMK) | yes | no | yes (7d) | on | 500k–1M |
| HIPAA / SOC 2 | SSE-KMS (CMK) | yes | Governance (90d) | yes (longer than lock) | on | default (100k) |
| FINRA 17a-4 (WORM) | SSE-KMS (CMK) | yes | Compliance | not allowed | on | default (100k) |

The IAM probe stays on in every scenario — AWS S3 enforces per-action
IAM correctly, so there's never a reason to set
`COZO_ARCHIVE_SKIP_IAM_PROBE=1` for AWS deployments.

### Choosing `max_rows_per_segment`

The default of 100,000 rows produces ~10–50 MB Parquet segments for most
workloads — within the size analytics tools (Spark, DuckDB, Trino, AWS
Athena) read efficiently. Two reasons to override:

- **Smaller cap (e.g. 5,000–20,000):** very memory-constrained hosts
  (Lambda, small containers) where holding 100k Arrow rows + the encoded
  Parquet bytes for one chunk is too much.
- **Larger cap (e.g. 500,000–1,000,000):** high-throughput producers that
  drain frequently. Fewer segments per drain → fewer S3 PUT requests
  (cheaper) and fewer manifest rows (smaller manifest scans). Trade-off:
  a single segment's encoding peak grows linearly.

The per-relation column lives in `cozo_archive_config`; passed as the 6th
positional argument to `::archive_config put`.

---

## What you get vs what's left to you

**Cozo enforces:**
- Credentials are write-only (IAM probe).
- Server-side encryption headers are sent on every PUT (per config).
- SHA-256 of every segment is recorded in the manifest.
- Watermark gate: archive cannot delete a row that hasn't been replicated.
- TLS in transit (the SDK handles this).

**You enforce, via S3 / KMS / IAM configuration:**
- Default bucket encryption (so even buckets receiving "encryption=none"
  PUTs end up encrypted).
- Versioning, retention, Object Lock.
- KMS key access (key policy + grants).
- Network isolation (VPC endpoints — see below).
- CloudTrail / S3 access logging.
- Lifecycle retention windows.

**Out of scope for cozo:**
- GDPR purge tooling. Lives outside cozo, uses the separate purge
  principal from Step 5c.
- Datalake ingest. Whatever consumes cozo's segments (Iceberg, Delta,
  custom Spark job, …) is not a cozo concern.
- Cross-region replication. Configure on the bucket if needed.

---

## Optional: VPC endpoint (private network access)

If your cozo workload runs inside a VPC, route S3 traffic through a
gateway VPC endpoint to avoid public-internet egress:

```bash
aws ec2 create-vpc-endpoint \
    --vpc-id vpc-... \
    --service-name com.amazonaws.us-east-1.s3 \
    --route-table-ids rtb-...
```

S3 traffic from the VPC then takes a private path to S3 directly. Cheaper
(no NAT gateway costs) and arguably more secure (no traffic crosses the
public internet). Cozo doesn't need to know — the AWS SDK uses whatever
DNS resolution the VPC provides.

---

## Threat model — what this configuration protects against

With the recommended posture in place:

| Threat | Mitigated by |
|---|---|
| Compromised cozo process tries to delete archives | IAM probe (refuses to start) + write-only IAM (delete API would fail anyway) |
| Bug in cozo issues unintended PUTs | KMS audit log + S3 access logs (forensic trail) |
| Credentials leak from the cozo host | Short-lived role credentials (instance profile) + bounded lifecycle |
| Operator runs `aws s3 rm` by hand | They'd need the purge principal's credentials, not cozo's |
| Tampering with archive bytes in S3 | Segment SHA-256 in manifest + bucket versioning |
| GDPR right-to-erasure request | Purge principal + Object Lock Governance bypass (Step 5c) |
| Region-wide AWS outage | Cross-region replication (configured on the bucket, not in cozo) |

What this *doesn't* protect against:
- **Compromise of the AWS account itself.** If an attacker has root /
  account-admin access, they can do anything. Outside cozo's scope.
- **Datalake mishandling after ingest.** Once your datalake reads a
  cozo segment, retention is the datalake's responsibility. Cozo's
  bucket lifecycle is bounded.
