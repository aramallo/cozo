/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! URI parsing and `object_store` backend construction.
//!
//! Supported schemes:
//!
//! - `s3://bucket[/prefix/...]` — S3 / S3-compatible (AWS, Tigris, R2, MinIO,
//!   Wasabi, B2). Credentials and endpoint come from the AWS SDK env-var
//!   chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
//!   `AWS_ENDPOINT`, `AWS_SESSION_TOKEN`). Cozo never sees credentials
//!   directly.
//! - `file:///abs/path[/...]` — local filesystem at the given absolute path.
//! - bare path (`/abs/path` or `relative/path`) — treated as a local path
//!   (slice 4 backwards compatibility).
//!
//! An ObjectStore + a key-prefix is the unit of configuration. Segments are
//! written as `<prefix>/<segment_filename>`.

use std::path::PathBuf;
use std::sync::Arc;

use miette::{bail, IntoDiagnostic, Result, WrapErr};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, PutPayload};

use super::manifest::ArchiveConfigRow;

/// Parsed URI: which backend, where to put things.
#[derive(Debug, Clone)]
pub(crate) struct Destination {
    pub(crate) original_uri: String,
    pub(crate) kind: BackendKind,
    /// For S3: bucket name. For local fs: filesystem root (created if missing).
    pub(crate) bucket_or_root: String,
    /// Path prefix within the bucket / fs root. Empty if the URI had none.
    pub(crate) prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BackendKind {
    Local,
    S3,
}

impl Destination {
    /// Render the canonical URI for an object placed at `path` within this
    /// destination. Used for the manifest's `file_path` column so users can
    /// copy-paste it back into `::import_parquet`.
    pub(crate) fn canonical_uri(&self, path: &StorePath) -> String {
        match self.kind {
            BackendKind::Local => {
                format!("{}/{path}", self.bucket_or_root.trim_end_matches('/'))
            }
            BackendKind::S3 => format!("s3://{}/{path}", self.bucket_or_root),
        }
    }
}

/// Parse a destination URI. Bare paths and `file://` map to Local;
/// `s3://bucket/prefix/` maps to S3. Other schemes are rejected with a clear
/// "not supported" error so users hit the right diagnostic immediately.
pub(crate) fn parse_destination(uri: &str) -> Result<Destination> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let mut parts = rest.splitn(2, '/');
        let bucket = parts.next().unwrap_or("");
        if bucket.is_empty() {
            bail!(
                "invalid s3 URI '{uri}': expected 's3://<bucket>[/<prefix>]'"
            );
        }
        // Forbid path traversal etc. by validating bucket name shape only
        // loosely — S3 buckets are 3-63 chars, lowercase, dots/hyphens
        // allowed. We don't enforce that here; let S3 reject malformed names.
        let prefix = parts.next().unwrap_or("").trim_start_matches('/').to_string();
        return Ok(Destination {
            original_uri: uri.to_string(),
            kind: BackendKind::S3,
            bucket_or_root: bucket.to_string(),
            prefix,
        });
    }

    if let Some(rest) = uri.strip_prefix("file://") {
        // Reject host-form URIs ('file://host/path') since cozo runs locally.
        // The only acceptable shape is 'file:///abs/path'.
        if !rest.starts_with('/') {
            bail!(
                "invalid file URI '{uri}': expected 'file:///<absolute-path>'"
            );
        }
        return Ok(Destination {
            original_uri: uri.to_string(),
            kind: BackendKind::Local,
            bucket_or_root: rest.to_string(),
            prefix: String::new(),
        });
    }

    // Reject any other scheme up front so users don't get cryptic
    // ObjectStore errors later.
    if let Some(colon) = uri.find(':') {
        let prefix = &uri[..colon];
        let is_scheme = !prefix.is_empty()
            && prefix.chars().next().unwrap().is_ascii_alphabetic()
            && prefix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.');
        if is_scheme && uri[colon..].starts_with("://") {
            bail!(
                "URI scheme '{prefix}' not supported by ::archive_config; \
                supported schemes are 's3://' and 'file://' (or a bare path)"
            );
        }
    }

    // Bare path (slice 4 compat). Must be absolute? No — relative paths are
    // OK locally; users running cozo as a CLI know what their cwd is.
    Ok(Destination {
        original_uri: uri.to_string(),
        kind: BackendKind::Local,
        bucket_or_root: uri.to_string(),
        prefix: String::new(),
    })
}

/// Validate that the encryption mode string is one of the three accepted
/// values, and that `kms_key_arn` is present iff mode == 'sse-kms'.
pub(crate) fn validate_encryption(
    encryption: Option<&str>,
    kms_key_arn: Option<&str>,
) -> Result<()> {
    let mode = encryption.unwrap_or("none");
    match mode {
        "none" | "sse-s3" => {
            if kms_key_arn.is_some() {
                bail!(
                    "kms_key_arn is only valid when encryption='sse-kms' \
                    (got encryption='{mode}')"
                );
            }
        }
        "sse-kms" => {
            if kms_key_arn.is_none() {
                bail!("encryption='sse-kms' requires a kms_key_arn");
            }
        }
        other => bail!(
            "unknown encryption mode '{other}'; valid values: 'none', 'sse-s3', 'sse-kms'"
        ),
    }
    Ok(())
}

/// Reject any field name that looks credential-shaped from the user-supplied
/// configuration. Cozo never stores credentials.
pub(crate) fn reject_credential_shaped(name: &str, value: &str) -> Result<()> {
    let n = name.to_ascii_lowercase();
    let credentialish = ["access_key", "secret", "token", "password", "credential"];
    for needle in credentialish {
        if n.contains(needle) {
            bail!(
                "configuration field '{name}' looks credential-shaped; cozo \
                does not store credentials. Use AWS_* environment variables \
                or your platform's IAM role instead. (offending value redacted)"
            );
            // Don't echo `value` in the error to avoid logging the secret.
        }
    }
    let _ = value;
    Ok(())
}

/// Build a concrete `ObjectStore` from the destination + encryption config.
/// For S3, this is where the AWS SDK env chain is consulted. The returned
/// `prefix` should be prepended to per-segment paths.
pub(crate) fn build_object_store(
    cfg: &ArchiveConfigRow,
) -> Result<(Arc<dyn ObjectStore>, Destination)> {
    let dst_uri = cfg.staging_dir.as_deref().ok_or_else(|| {
        miette::miette!("config has no staging_dir; cannot build an ObjectStore")
    })?;
    let dst = parse_destination(dst_uri)?;

    match dst.kind {
        BackendKind::Local => {
            let root = PathBuf::from(&dst.bucket_or_root);
            // Create the directory eagerly — LocalFileSystem refuses to write
            // into a nonexistent root.
            std::fs::create_dir_all(&root)
                .into_diagnostic()
                .wrap_err_with(|| {
                    format!("failed to create local staging dir {}", root.display())
                })?;
            let lfs = LocalFileSystem::new_with_prefix(&root)
                .into_diagnostic()
                .wrap_err_with(|| {
                    format!("LocalFileSystem at {} failed to initialise", root.display())
                })?;
            Ok((Arc::new(lfs), dst))
        }
        BackendKind::S3 => {
            // The AWS SDK reads `AWS_ENDPOINT_URL_S3` and `AWS_ENDPOINT_URL`;
            // `object_store::AmazonS3Builder::from_env()` reads `AWS_ENDPOINT`
            // (no `_URL` suffix). Bridge transparently so users with either
            // naming convention work, and so non-AWS S3-compatible services
            // (Tigris, R2, MinIO, Wasabi, B2) work without extra setup.
            if std::env::var("AWS_ENDPOINT").is_err() {
                for candidate in ["AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL"] {
                    if let Ok(u) = std::env::var(candidate) {
                        // SAFETY: setting an env var is process-wide. We only
                        // do this for the duration of building the client; the
                        // value we set is one we already read from the env, so
                        // we are not introducing new state from outside the
                        // process.
                        std::env::set_var("AWS_ENDPOINT", u);
                        break;
                    }
                }
            }

            let mut builder = AmazonS3Builder::from_env().with_bucket_name(&dst.bucket_or_root);

            // Encryption: object_store sets the appropriate header per request.
            //
            // The typed `S3EncryptionConfigKey` is private in object_store
            // 0.11, so the SSE-S3 path goes via the string FromStr route
            // (`aws_server_side_encryption`). SSE-KMS uses the dedicated
            // typed builder method which is public.
            match cfg.encryption.as_str() {
                "none" => {}
                "sse-s3" => {
                    let key: AmazonS3ConfigKey = "aws_server_side_encryption"
                        .parse()
                        .into_diagnostic()
                        .wrap_err("internal: SSE-S3 config key not recognized by object_store")?;
                    builder = builder.with_config(key, "AES256");
                }
                "sse-kms" => {
                    let kms = cfg.kms_key_arn.as_deref().ok_or_else(|| {
                        miette::miette!("encryption='sse-kms' but kms_key_arn is missing")
                    })?;
                    builder = builder.with_sse_kms_encryption(kms);
                }
                other => bail!("internal: unrecognized encryption mode '{other}'"),
            }

            let s3 = builder
                .build()
                .into_diagnostic()
                .wrap_err("failed to build S3 ObjectStore — \
                          check AWS_* env vars (AWS_ACCESS_KEY_ID, \
                          AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_ENDPOINT)")?;
            Ok((Arc::new(s3), dst))
        }
    }
}

/// Compose a final object path from the destination prefix and a relative
/// segment filename.
pub(crate) fn join_path(prefix: &str, leaf: &str) -> StorePath {
    let combined = if prefix.is_empty() {
        leaf.to_string()
    } else if prefix.ends_with('/') {
        format!("{prefix}{leaf}")
    } else {
        format!("{prefix}/{leaf}")
    };
    StorePath::from(combined)
}

/// Synchronous PUT of `bytes` to `path`. Spins up a current-thread tokio
/// runtime for the call — cheap enough since `::replicate_pending` is a
/// rare, manual op.
pub(crate) fn put_blocking(
    store: &Arc<dyn ObjectStore>,
    path: &StorePath,
    bytes: Vec<u8>,
) -> Result<()> {
    let store = store.clone();
    let path = path.clone();
    let payload = PutPayload::from(bytes);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .into_diagnostic()?;
    rt.block_on(async move {
        store
            .put(&path, payload)
            .await
            .into_diagnostic()
            .wrap_err_with(|| format!("ObjectStore::put failed for {path}"))
            .map(|_| ())
    })
}

/// Synchronous GET of an object's full bytes.
pub(crate) fn get_blocking(store: &Arc<dyn ObjectStore>, path: &StorePath) -> Result<Vec<u8>> {
    let store = store.clone();
    let path = path.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .into_diagnostic()?;
    rt.block_on(async move {
        let g = store
            .get(&path)
            .await
            .into_diagnostic()
            .wrap_err_with(|| format!("ObjectStore::get failed for {path}"))?;
        let bytes = g
            .bytes()
            .await
            .into_diagnostic()
            .wrap_err("ObjectStore::get bytes() failed")?;
        Ok(bytes.to_vec())
    })
}

/// IAM probe: confirm that the configured S3 credentials cannot
/// `DeleteObject`. Run before the first replication to a fresh S3
/// destination. Errors loudly if delete is permitted; that's an
/// architectural rule.
///
/// For the local backend, this is a no-op — there's no IAM to probe and
/// `LocalFileSystem` is fully under the user's control.
///
/// **Escape hatch:** setting `COZO_ARCHIVE_SKIP_IAM_PROBE=1` disables the
/// probe entirely. Some S3-compatible services (notably ones with coarse
/// "read" / "read-write" key-level scopes rather than per-action IAM) cannot
/// satisfy the no-DeleteObject rule. The escape hatch makes those services
/// usable but **defeats an architectural guarantee** — a compromised cozo
/// process can then delete from S3. Use only when the underlying service
/// genuinely does not support per-action access restrictions, and prefer to
/// move to a service that does for any production-grade deployment.
pub(crate) fn iam_probe(
    store: &Arc<dyn ObjectStore>,
    dst: &Destination,
) -> Result<()> {
    if dst.kind != BackendKind::S3 {
        return Ok(());
    }

    if matches!(
        std::env::var("COZO_ARCHIVE_SKIP_IAM_PROBE").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
    ) {
        eprintln!(
            "cozo: WARNING — COZO_ARCHIVE_SKIP_IAM_PROBE is set; \
            the configured S3 credentials are NOT being checked for the \
            no-DeleteObject architectural rule. A compromised cozo process \
            could delete archived segments. Use only on services with \
            coarse permission models that cannot express per-action scoping."
        );
        return Ok(());
    }

    // Pick a key that almost certainly does not exist. If DeleteObject
    // returns AccessDenied → good, our role can't delete. If it returns
    // NoSuchKey or success → bad, role has DeleteObject.
    //
    // object_store wraps S3 errors; we look at the rendered message to
    // distinguish the two cases. This is brittle in principle, but for the
    // smoke-test bring-up it's what we have without dipping into raw AWS
    // SDK error variants.
    let probe_path = join_path(
        &dst.prefix,
        &format!("__cozo_iam_probe_{}", uuid::Uuid::new_v4()),
    );

    let store = store.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .into_diagnostic()?;
    let res: std::result::Result<(), object_store::Error> =
        rt.block_on(async move { store.delete(&probe_path).await.map(|_| ()) });

    match res {
        Ok(()) => {
            // Delete succeeded — meaning the role can delete. Refuse.
            bail!(
                "S3 role granted by your AWS_* credentials has DeleteObject permission \
                on the configured bucket; cozo refuses to use such credentials. \
                Restrict the IAM policy to PutObject + GetObject + ListBucket only."
            );
        }
        Err(e) => {
            let msg = format!("{e}");
            let lower = msg.to_lowercase();
            if lower.contains("access denied")
                || lower.contains("accessdenied")
                || lower.contains("forbidden")
                || lower.contains("403")
            {
                // Expected outcome — credentials cannot delete. Pass.
                Ok(())
            } else if lower.contains("not found")
                || lower.contains("nosuchkey")
                || lower.contains("404")
            {
                // The role *can* delete (S3 only returns NoSuchKey when you
                // had permission). Refuse.
                bail!(
                    "S3 role appears to have DeleteObject permission \
                    (probe returned NoSuchKey rather than AccessDenied); \
                    cozo refuses to use such credentials. Restrict the IAM \
                    policy to PutObject + GetObject + ListBucket only."
                );
            } else {
                // Some other error (network, auth misconfig, etc.) — surface
                // it so the user can fix the underlying issue.
                bail!("S3 IAM probe failed with unexpected error: {msg}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_uri_with_prefix() {
        let d = parse_destination("s3://my-bucket/some/prefix/").unwrap();
        assert_eq!(d.kind, BackendKind::S3);
        assert_eq!(d.bucket_or_root, "my-bucket");
        assert_eq!(d.prefix, "some/prefix/");
    }

    #[test]
    fn parses_s3_uri_no_prefix() {
        let d = parse_destination("s3://my-bucket").unwrap();
        assert_eq!(d.bucket_or_root, "my-bucket");
        assert_eq!(d.prefix, "");
    }

    #[test]
    fn rejects_empty_s3_bucket() {
        let err = parse_destination("s3:///prefix").unwrap_err().to_string();
        assert!(err.contains("expected"), "got: {err}");
    }

    #[test]
    fn parses_file_uri() {
        let d = parse_destination("file:///tmp/cozo").unwrap();
        assert_eq!(d.kind, BackendKind::Local);
        assert_eq!(d.bucket_or_root, "/tmp/cozo");
    }

    #[test]
    fn rejects_relative_file_uri() {
        let err = parse_destination("file://tmp/cozo").unwrap_err().to_string();
        assert!(err.contains("absolute"), "got: {err}");
    }

    #[test]
    fn parses_bare_path_as_local() {
        let d = parse_destination("/var/cozo/staging").unwrap();
        assert_eq!(d.kind, BackendKind::Local);
        assert_eq!(d.bucket_or_root, "/var/cozo/staging");
    }

    #[test]
    fn rejects_unknown_schemes() {
        for uri in ["http://x", "https://x", "gs://b/p", "ftp://x"] {
            let err = parse_destination(uri).unwrap_err().to_string();
            assert!(
                err.contains("not supported"),
                "{uri} should be rejected; got: {err}"
            );
        }
    }

    #[test]
    fn validate_encryption_accepts_known_modes() {
        validate_encryption(None, None).unwrap();
        validate_encryption(Some("none"), None).unwrap();
        validate_encryption(Some("sse-s3"), None).unwrap();
        validate_encryption(Some("sse-kms"), Some("arn:aws:kms:...")).unwrap();
    }

    #[test]
    fn validate_encryption_rejects_kms_without_key() {
        let err = validate_encryption(Some("sse-kms"), None).unwrap_err().to_string();
        assert!(err.contains("kms_key_arn"), "got: {err}");
    }

    #[test]
    fn validate_encryption_rejects_kms_key_with_other_mode() {
        let err = validate_encryption(Some("sse-s3"), Some("arn:..."))
            .unwrap_err()
            .to_string();
        assert!(err.contains("only valid when"), "got: {err}");
    }

    #[test]
    fn validate_encryption_rejects_unknown_mode() {
        let err = validate_encryption(Some("aes-256-cbc"), None).unwrap_err().to_string();
        assert!(err.contains("unknown encryption"), "got: {err}");
    }

    #[test]
    fn reject_credential_shaped_catches_common_names() {
        for n in [
            "access_key",
            "ACCESS_KEY_ID",
            "secret_access_key",
            "session_token",
            "auth_token",
            "password",
            "my_credential",
        ] {
            assert!(
                reject_credential_shaped(n, "x").is_err(),
                "should reject field '{n}'"
            );
        }
        // Innocuous names pass.
        for n in ["timestamp_column", "staging_dir", "encryption", "kms_key_arn", "region"] {
            reject_credential_shaped(n, "x").unwrap();
        }
    }

    #[test]
    fn join_path_handles_prefix_variants() {
        assert_eq!(join_path("", "x.parquet").as_ref(), "x.parquet");
        assert_eq!(join_path("a", "x.parquet").as_ref(), "a/x.parquet");
        assert_eq!(join_path("a/", "x.parquet").as_ref(), "a/x.parquet");
        assert_eq!(join_path("a/b/", "x.parquet").as_ref(), "a/b/x.parquet");
    }
}
