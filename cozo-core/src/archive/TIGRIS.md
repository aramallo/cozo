# Working with Tigris (and the IAM-probe escape hatch)

Cozo's archive subsystem ships an IAM probe that refuses to run if the
configured S3 credentials can `DeleteObject`. This is an architectural
guarantee: a compromised cozo process must not be able to delete archived
segments from durable storage.

The probe works against AWS S3 and any S3-compatible service that
implements per-action IAM. **It does not work against [Tigris][tigris]**
— and once you understand why, you can decide whether to live with the
escape hatch or move to a service with a finer permission model.

For AWS S3 setup with the probe enabled (the default, recommended for
all production AWS deployments), see [AWS_S3.md](AWS_S3.md). The rest of
this doc is Tigris-specific.

[tigris]: https://www.tigrisdata.com/

---

## TL;DR

- Tigris does not enforce per-action IAM scoping on access keys the way AWS
  does. A Tigris key with read-write access to a bucket can `DeleteObject`,
  whether or not that's listed in any policy you attach.
- Cozo's IAM probe correctly detects this and refuses to proceed.
- Set `COZO_ARCHIVE_SKIP_IAM_PROBE=1` to bypass the probe. This is the only
  knob; cozo prints a loud warning every time the probe is skipped.
- This is acceptable for development and for production deployments where
  Tigris's bucket-level versioning and your own application controls give
  you the durability guarantees you need. It is **not** the same security
  posture as the probe-on default on AWS S3.

---

## What the IAM probe does, and why

The smallest correctness property the archive subsystem promises is:

> An archived row is durably copied to storage *and* unreachable from cozo
> until/unless someone with elevated privileges restores it.

The "unreachable from cozo" half is what the IAM probe enforces. Cozo's
runtime credentials are the credentials your application uses every day.
If those credentials can delete from the archive bucket, then a
compromised cozo process — bug, supply-chain attack, malicious admin —
can wipe your archive copies. That collapses the archive into "data we
*used* to have."

The probe runs once per `::replicate_pending` call before any PUT happens:

1. Issue a `DeleteObject` against a key that almost certainly does not
   exist (`__cozo_iam_probe_<random-uuid>`).
2. Inspect the response:
   - **`AccessDenied` / `403`** → credentials *cannot* delete → continue.
   - **Success** (idempotent delete on a missing key returns 204 on most
     S3 implementations) → credentials *can* delete → refuse.
   - **`NoSuchKey` / `404`** → some implementations return this for
     missing keys; it also indicates the credentials had permission, so
     cozo refuses.
   - **Anything else** (network, signing, region mismatch) → surface as
     an unexpected error so the user can fix the underlying issue.

The cost is a single `DELETE` request per drain. The benefit is a hard
guarantee that your daily-driver credentials are write-only on the bucket.

The minimal IAM policy that satisfies the probe on AWS:

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

GDPR purges, lifecycle deletes, and any other operation that needs delete
access run under a *separate* IAM principal that's not loaded into the
cozo process. That's the architectural boundary.

---

## Why it fails on Tigris

Tigris's access-key model is coarser than AWS IAM:

- Keys are scoped to a bucket (or set of buckets).
- A key has a high-level role: read-only or read-write.
- Read-write keys can do everything S3-compatible — `PutObject`,
  `GetObject`, `ListBucket`, **and `DeleteObject`**, plus operations
  like `CopyObject` etc.

There is no concept of "read-write but not delete" at the access-key
level.

### Empirical results (confirmed against a real bucket)

Two policy shapes were tested directly against a Tigris access key with
the policy properly linked in the Tigris dashboard, against a real
bucket, with cozo's IAM probe enabled.

**Test 1 — Allow-list only:**

```json
{ "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
  "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"] }
```

Result: `DeleteObject` succeeds in ~370ms. Probe refuses.

**Test 2 — explicit Deny added:**

```json
[
  { "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"] },
  { "Effect": "Deny",
    "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion",
               "s3:DeleteObjectTagging", "s3:DeleteObjectVersionTagging",
               "s3:DeleteBucket"],
    "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"] }
]
```

Result: `DeleteObject` still succeeds in ~370ms. Probe refuses.

Explicit Deny is the strongest tool in the IAM grammar — on AWS it
overrides any Allow grant from any source. Tigris ignores it for
`DeleteObject` from a read-write access key.

### Interpretation

For at least the `DeleteObject` action, Tigris's IAM-shaped policy
attachment on access keys appears to be effectively advisory: neither
removing the action from the Allow list nor adding an explicit Deny
prevents the operation. The access key's underlying scope (bucket-level
read-write) is what's enforced.

This is not a bug in cozo's IAM probe — the probe is correctly
observing what the credentials can do at the wire level. It is a
property of Tigris's current enforcement model.

If Tigris adds per-action enforcement on access keys in the future, the
probe will pass automatically without any cozo changes. Until then,
Tigris users need the escape hatch.

---

## The escape hatch

Set the environment variable before running cozo:

```bash
export COZO_ARCHIVE_SKIP_IAM_PROBE=1
```

Cozo will skip the probe and print a `WARNING` to stderr on every drain:

```
cozo: WARNING — COZO_ARCHIVE_SKIP_IAM_PROBE is set; the configured S3
credentials are NOT being checked for the no-DeleteObject architectural
rule. A compromised cozo process could delete archived segments. Use only
on services with coarse permission models that cannot express per-action
scoping.
```

Accepted truthy values: `1`, `true`, `TRUE`, `yes`. Anything else (or
unset) keeps the probe active.

The variable affects *only* the IAM probe. Everything else (encryption
headers, sigv4 signing, sha256 integrity, the watermark gate, the
deny-credentials-in-config rule) is unchanged.

---

## What you give up

With the probe disabled, the architectural rule "cozo cannot delete
archived segments" is no longer enforced. The compromise scenarios become
real:

- **Bug or panic in cozo's own archive code** that calls `DeleteObject`
  on the wrong path. This is what the probe primarily defends against —
  a write-only credential turns such a bug into a no-op rather than a
  data-loss event. Without the probe, the bug deletes data.
- **Supply-chain compromise.** A malicious dependency that gets loaded
  into cozo's process can issue arbitrary S3 calls under the same
  credentials. Probe-on means it can write garbage but not erase real
  data. Probe-off means it can do both.
- **Operator mistake.** Anyone with shell access to the cozo host has
  the credentials in env vars. Probe-on means they can't `aws s3 rm`
  the archive even by hand. Probe-off makes that possible.

The probe doesn't prevent malicious uploads, replay attacks, or
exfiltration. It is a single, narrow defensive measure against the most
common archive-destroying foot-gun: code that has both write and delete
permission and unintentionally uses delete.

---

## What's still protected

The escape hatch only disables the probe. These continue to work as
designed:

- **Encryption at rest.** SSE-S3 / SSE-KMS headers are sent on every PUT
  if you've configured them.
- **Integrity.** SHA-256 of every segment is recorded in the manifest.
  If a segment in S3 is tampered with, restoring it will fail unless
  you bypass the integrity check explicitly.
- **No credentials in cozo state.** `::archive_config put` still rejects
  credential-shaped fields and refuses to store them in
  `cozo_archive_config`.
- **TLS in transit.** The S3 client refuses non-TLS endpoints unless
  you've explicitly opted in (and Tigris is TLS-only anyway).

So with the probe off you've reduced one layer of defense. You still
have the others.

---

## Compensating controls on Tigris

If you're using Tigris with the probe off, you can claw back the
guarantee — and in some cases exceed it — through Tigris's own features:

1. **Bucket-level deletion lock** (Tigris bucket setting). Disables
   actual object deletion at the bucket level. A `DeleteObject` API
   call on a real object is rejected by the bucket regardless of which
   key issued it.

   ⚠ Note on the probe interaction: cozo's IAM probe issues
   `DeleteObject` against a non-existent key. Per the S3 API,
   `DeleteObject` on a missing key returns success (it's idempotent),
   so the probe still reports the credentials as having delete
   permission even with bucket-level lock enabled. **This is a
   limitation of the probe, not a real safety gap.** Bucket-level lock
   protects existing objects, which is the property you actually care
   about.

2. **Bucket versioning.** Enable versioning on the archive bucket. A
   `DeleteObject` becomes a soft delete (a delete marker is added;
   the prior versions are still accessible). Recoverable via Tigris's
   versioning UI / API.

3. **Object Lock in Governance mode** (if Tigris supports it for your
   region/account). Locks objects against deletion for a retention
   period, with an explicit bypass permission for legitimate erasure
   cases. Compliance mode is too strict for GDPR — Governance is the
   right setting here.

4. **Tigris audit logs.** Enable bucket-level access logging. A
   compromise that issues deletes leaves a trail you can use for
   forensics, even when the deletes are blocked.

5. **Out-of-band replica.** Run a separate process — outside the cozo
   environment — that periodically `aws s3 sync`s the archive prefix
   to a second bucket under entirely separate credentials. Sidesteps
   the entire question by making the cozo-host's credentials
   irrelevant to long-term retention.

### Recommended posture on Tigris

`COZO_ARCHIVE_SKIP_IAM_PROBE=1` + bucket-level deletion lock + bucket
versioning is a strong, simple, defensible production setup. It is
*not* equivalent to IAM-based delete denial on AWS — it's a different
defense layer that protects the same property. In practice, bucket-
level lock is harder to misconfigure than IAM (one toggle vs a many-
field policy document) and applies uniformly across all keys.

Treat cozo-host credentials as compromisable, and let the bucket
configuration (not the credential's IAM) be the final defense against
archive deletion.

---

## Recommendations by deployment scenario

### Development and testing

`COZO_ARCHIVE_SKIP_IAM_PROBE=1` is fine. The data isn't real. Smoke tests
in this repo use this exact flag.

### Single-machine / hobby production

Probe off + bucket versioning is a defensible posture. The bucket can
recover from accidental deletes; cozo isn't your highest-blast-radius
process.

### Regulated / multi-tenant production

Use AWS S3 (or another service with per-action IAM enforcement). Keep
the probe on. The architectural guarantee matters here.

If you must use Tigris in this scenario, layer:
- probe off
- bucket versioning enabled
- Object Lock in Governance mode with a meaningful retention period
- out-of-band replica to a second bucket under separate credentials

That gets you back to "compromised cozo cannot lose archived data
within the retention window," at the cost of a more involved operational
setup.

---

## Comparison with other S3-compatible services

| Service                  | Per-action IAM? | Probe works? | Notes |
|--------------------------|-----------------|--------------|-------|
| AWS S3                   | yes             | yes          | The reference. Probe on by default. |
| Cloudflare R2            | yes (via API tokens with scoped permissions) | yes (with the right token scope) | Configure tokens to exclude `DeleteObject`. |
| MinIO                    | yes (full IAM) | yes | Set up a service account with PutObject + GetObject + ListBucket only. |
| Wasabi                   | yes             | yes | Similar to AWS. |
| Backblaze B2 (S3 mode)   | yes (via application keys with capability lists) | yes (with the right key capabilities) | Application key without `deleteFiles` capability satisfies the probe. |
| Tigris                   | no (policy attached but not enforced for DeleteObject) | no | Confirmed: neither Allow-list-only nor explicit Deny prevents DeleteObject from a read-write access key. Use the escape hatch + compensating controls. |
| Local filesystem         | n/a             | n/a (skipped) | The probe doesn't apply to non-S3 backends. |

If a service isn't on this list, the test is simple: create a key/token
with the recommended IAM policy, run a smoke test (or just
`::replicate_pending`), and see if the probe passes. If it does, you're
fine. If it doesn't, you have a coarse permission model and need either
the escape hatch or a different service.

---

## Possible future improvements

- **`::archive_diagnose` sys op** — return the resolved endpoint, region,
  encryption mode, and IAM-probe status as queryable rows. Triage on the
  next "why isn't replication working?" becomes a one-liner.
- **Probe modes** — instead of binary on/off, support `'enforce'`,
  `'warn'`, and `'skip'`. `'warn'` would log on every drain that the
  rule is being violated, even on services where it could be enforced —
  catches drift in IAM configuration over time.
- **Compensating-control assertions** — at config time, optionally
  verify that bucket versioning is enabled when the probe is off. Refuse
  to start without one of {probe-on, versioning-on, explicit
  acknowledgement}.

These are not implemented in slice 5. File an issue if any of them would
be useful in your deployment.
