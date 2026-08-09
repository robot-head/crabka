//! Object-store connection config types shared across Crabka.

/// Default threshold above which a segment upload changes from a single PUT to
/// a streaming multipart upload. The threshold is 100 MiB, well below AWS's
/// 5 GiB single-PUT cap.
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 100 * 1024 * 1024;

/// Default per-part size for multipart uploads. The size is 16 MiB. AWS
/// requires >= 5 MiB per non-final part and caps the parts at 10 000, so 16 MiB
/// scales past any real segment.
pub const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Selects and parameterises the object-store backend to construct.
#[derive(Clone, Debug)]
pub enum ObjectStoreConfig {
    /// Any S3-compatible endpoint, such as AWS S3, `MinIO`, or Cloudflare R2.
    S3(S3Config),
    /// Native Google Cloud Storage. It supports keyless GKE Workload Identity.
    Gcs(GcsConfig),
    /// Local filesystem rooted at `root`, for development and test.
    Local { root: std::path::PathBuf },
    /// In-process store, for tests.
    InMemory,
}

/// Connection and bucket parameters for an S3-compatible backend.
///
/// The credentials come either from `access_key_id` and `secret_access_key` or
/// from the standard AWS credential chain. When both fields are `None`,
/// `object_store` falls back to the environment-variable chain.
#[derive(Clone)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket. No leading slash and no trailing
    /// slash.
    pub prefix: Option<String>,
    /// AWS region. AWS S3 requires it. Use the placeholder `"us-east-1"` for
    /// `MinIO` and R2.
    pub region: String,
    /// Optional custom endpoint URL, for example `http://minio:9000` or an R2
    /// endpoint.
    pub endpoint: Option<String>,
    /// Optional explicit access key id. Without it, the backend falls back to
    /// the AWS credential chain.
    pub access_key_id: Option<String>,
    /// Optional explicit secret access key. Without it, the backend falls back
    /// to the AWS credential chain.
    pub secret_access_key: Option<String>,
    /// Allow plaintext HTTP. `MinIO` without TLS requires it.
    pub allow_http: bool,
    /// Files of at least this size upload with multipart. Defaults to
    /// [`DEFAULT_MULTIPART_THRESHOLD`].
    pub multipart_threshold: u64,
    /// Per-part size for multipart. Defaults to [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    pub multipart_chunk_size: usize,
}

impl std::fmt::Debug for S3Config {
    /// Redacts credential fields, so a stray `{:?}` call or tracing call never
    /// leaks them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |opt: &Option<String>| opt.as_ref().map(|_| "***");
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &redact(&self.access_key_id))
            .field("secret_access_key", &redact(&self.secret_access_key))
            .field("allow_http", &self.allow_http)
            .field("multipart_threshold", &self.multipart_threshold)
            .field("multipart_chunk_size", &self.multipart_chunk_size)
            .finish()
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: None,
            region: String::new(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            allow_http: false,
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
        }
    }
}

/// Connection and bucket parameters for native Google Cloud Storage.
///
/// If every credential field is `None`, the config selects Workload Identity or
/// ADC through the metadata server. This is the keyless GKE production path.
#[derive(Clone, PartialEq, Eq)]
pub struct GcsConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket. No leading slash and no trailing
    /// slash.
    pub prefix: Option<String>,
    /// Optional path to a service-account JSON key file.
    pub service_account_path: Option<String>,
    /// Optional inline service-account JSON key. It is mutually exclusive with
    /// the path.
    pub service_account_key: Option<String>,
    /// Optional path to an application-default-credentials JSON file.
    pub application_credentials_path: Option<String>,
    /// Optional custom GCS API base URL, for example `http://fake-gcs:4443`.
    pub endpoint: Option<String>,
    /// Allow plaintext HTTP. Emulators without TLS require it.
    pub allow_http: bool,
    /// Files of at least this size upload with resumable multipart. Defaults
    /// to [`DEFAULT_MULTIPART_THRESHOLD`].
    pub multipart_threshold: u64,
    /// Per-part size for multipart. Defaults to [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    pub multipart_chunk_size: usize,
}

impl std::fmt::Debug for GcsConfig {
    /// Redacts credential fields, so a stray `{:?}` call or tracing call never
    /// leaks them.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |opt: &Option<String>| opt.as_ref().map(|_| "***");
        f.debug_struct("GcsConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("service_account_path", &redact(&self.service_account_path))
            .field("service_account_key", &redact(&self.service_account_key))
            .field(
                "application_credentials_path",
                &redact(&self.application_credentials_path),
            )
            .field("endpoint", &self.endpoint)
            .field("allow_http", &self.allow_http)
            .field("multipart_threshold", &self.multipart_threshold)
            .field("multipart_chunk_size", &self.multipart_chunk_size)
            .finish()
    }
}

impl Default for GcsConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: None,
            service_account_path: None,
            service_account_key: None,
            application_credentials_path: None,
            endpoint: None,
            allow_http: false,
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn multipart_size_constants() {
        assert!(DEFAULT_MULTIPART_THRESHOLD == 100 * 1024 * 1024);
        assert!(DEFAULT_MULTIPART_CHUNK_SIZE == 16 * 1024 * 1024);
    }

    #[test]
    fn s3_config_default_uses_multipart_constants() {
        let cfg = S3Config::default();
        assert!(cfg.multipart_threshold == DEFAULT_MULTIPART_THRESHOLD);
        assert!(cfg.multipart_chunk_size == DEFAULT_MULTIPART_CHUNK_SIZE);
    }

    #[test]
    fn s3_config_debug_redacts_credentials() {
        let cfg = S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            access_key_id: Some("AKIASECRET".into()),
            secret_access_key: Some("supersecret".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("AKIASECRET"));
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("***"));
    }

    #[test]
    fn gcs_config_default_uses_multipart_constants() {
        let cfg = GcsConfig::default();
        assert!(cfg.multipart_threshold == DEFAULT_MULTIPART_THRESHOLD);
        assert!(cfg.multipart_chunk_size == DEFAULT_MULTIPART_CHUNK_SIZE);
    }

    #[test]
    fn gcs_config_debug_redacts_credentials() {
        let cfg = GcsConfig {
            bucket: "b".into(),
            service_account_key: Some("{\"private_key\":\"leak\"}".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("leak"));
        assert!(dbg.contains("***"));
    }

    #[test]
    fn object_store_config_debug_redacts_via_inner() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            secret_access_key: Some("supersecret".into()),
            ..Default::default()
        });
        assert!(!format!("{cfg:?}").contains("supersecret"));
    }
}
