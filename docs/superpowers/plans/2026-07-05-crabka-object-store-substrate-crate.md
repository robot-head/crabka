# `crabka-object-store` Substrate Crate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a new `crabka-object-store` crate that owns the object-store *construction* layer (typed config → `object_store::ObjectStore` handle) and make both `crabka-remote-storage` (KIP-405 tiered storage) and `crabka-blockstore` (observability) consume it, so the two stacks stop each owning a private copy of the builder wiring.

**Architecture:** Today the two stacks "share only the `object_store` dependency, not a substrate": `crabka-remote-storage` hand-builds `AmazonS3Builder`/`GoogleCloudStorageBuilder` from `S3Config`/`GcsConfig` in `s3.rs`/`gcs.rs`, and `crabka-blockstore` receives an already-built `Arc<dyn ObjectStore>`. This plan moves the config types and the builder wiring into one publishable crate exposing `build_object_store(&ObjectStoreConfig) -> Result<Arc<dyn ObjectStore>, ObjectStoreError>`. `remote-storage` routes its `from_s3_config`/`from_gcs_config` through it (and re-exports the moved types so downstream imports don't break); `blockstore` gains an additive `BlockStore::from_config`. **Scope is object-store PLUMBING only** — no data-representation code (verbatim Kafka segment bytes, Parquet/Arrow, key layout, DataFusion registration) moves.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `object_store` 0.13 (features `aws`, `gcp`, workspace-pinned), `thiserror`, `tokio`, `assert2` + `nextest` for tests, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-north-star-roadmap-design.md`](../specs/2026-07-05-crabka-north-star-roadmap-design.md) — Chapter 0, Milestone 3 (first increment).

---

## Invariants (do not violate — each is a real, verified risk)

1. **`object_store` is workspace-pinned at 0.13 (`aws`, `gcp`).** The new crate MUST declare `object_store = { workspace = true }` and never pin its own version. `parquet` 59 + the git-pinned DataFusion pass `ObjectStore`/`Path` across boundaries; a lone bump to 0.14 splits the graph and fails to compile (root `Cargo.toml` renovate hold; a prior auto-merged bump already broke `main` this way).
2. **The new crate must be publishable to crates.io.** `crabka-remote-storage` IS published; `crabka-blockstore` is `publish = false` (git DataFusion dep). Therefore **no `datafusion`/`parquet`/blockstore-only type may leak into the new crate's public API**, or it becomes unpublishable and breaks `remote-storage`'s release. Depend only on crates.io deps (`object_store`, `thiserror`).
3. **No data-representation code moves.** The verbatim-Kafka-bytes key layout (`segment_key`/`log_key`/`index_key` in `s3.rs`), the `block()` sync↔async bridge (`s3.rs`), Parquet/Arrow paths, and DataFusion `register_object_store` all STAY in their current crates. This milestone unifies construction only.
4. **Credential redaction must survive the move.** `S3Config`/`GcsConfig` hand-write `Debug` to redact credentials to `***`. Move the impls verbatim; never regress to `#[derive(Debug)]` on the secret-bearing structs. The redaction tests move with them.
5. **`remote-storage`'s public re-exports stay stable.** The broker imports `crabka_remote_storage::{S3Config, GcsConfig, DEFAULT_MULTIPART_THRESHOLD, DEFAULT_MULTIPART_CHUNK_SIZE, S3RemoteStorage, RemoteStorageManager, RemoteStorageError}`. After the move these must still resolve (via re-export from the new crate) so `cargo test --workspace` stays green.
6. **Auth / retry behavior is byte-identical.** The `AmazonS3Builder`/`GoogleCloudStorageBuilder` calls are moved verbatim (same credential-chain fallback, same absence of an explicit `RetryConfig`). No behavioral change.
7. **Every task leaves the workspace compiling and tests green** before its commit.

## Explicitly deferred (NOT in this plan — named so they aren't half-built)

- An `ObjectOps` trait + `mockall` seam + routing the crates' put/get/list/delete/multipart calls through it. (Next milestone: op unification.)
- `read_capped`, `ObjectStoreHandle` prefix projections (`key_prefix`/`base_url`/`object_prefix`).
- Replacing `remote-storage`'s `"not found:"` string-prefix `NotFound` upgrade with the structured `ObjectStoreError::NotFound`.
- Migrating the observability service crates (`metrics-service`, `observability`, `traces`, `profiles`) onto `build_object_store`.
- Any `RetryConfig` surface, and making broker log data columnar (Ch. 0 M4).

---

## File Structure

**New crate `crates/object-store/` (`crabka-object-store`):**
- `Cargo.toml` — publishable; deps `object_store` (workspace), `thiserror` (workspace).
- `src/lib.rs` — module wiring + public re-exports. One responsibility: the crate's public surface.
- `src/config.rs` — `S3Config`, `GcsConfig` (moved verbatim, redacting `Debug` + `Default`), `DEFAULT_MULTIPART_*` consts, and the `ObjectStoreConfig` enum. One responsibility: config types.
- `src/error.rs` — `ObjectStoreError` (thiserror) + `From<object_store::Error>`. One responsibility: the crate's error taxonomy.
- `src/build.rs` — `build_object_store` + private `build_s3`/`build_gcs`. One responsibility: config → handle construction.

**Modified — `crates/remote-storage/`:**
- `Cargo.toml` — add path dep on `crabka-object-store`.
- `src/s3.rs` — delete `S3Config` + `DEFAULT_MULTIPART_*` + their tests; rewrite `from_s3_config` to call `build_object_store`.
- `src/gcs.rs` — delete `GcsConfig` + its builder tests; rewrite `from_gcs_config` to call `build_object_store`.
- `src/lib.rs` — re-export the moved types from `crabka_object_store`.

**Modified — `crates/blockstore/`:**
- `Cargo.toml` — add path dep on `crabka-object-store`.
- `src/store.rs` — add additive `BlockStore::from_config`.

---

## Task 1: Scaffold the `crabka-object-store` crate

*Infrastructure task (no failing test — the workspace `members = ["crates/*"]` glob picks the crate up; the "test" is that it compiles and is wired into the workspace).*

**Files:**
- Create: `crates/object-store/Cargo.toml`
- Create: `crates/object-store/src/lib.rs`

- [ ] **Step 1: Create the crate manifest**

Create `crates/object-store/Cargo.toml`:

```toml
[package]
name = "crabka-object-store"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Unified object-store construction (typed config -> object_store handle) shared by Crabka's KIP-405 tiered storage and observability blockstore"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-object-store"
readme = "README.md"
keywords = ["kafka", "object-store", "s3", "gcs", "crabka"]
categories = ["database-implementations", "filesystem"]

[lints]
workspace = true

[dependencies]
object_store = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 2: Create the crate root**

Create `crates/object-store/src/lib.rs`:

```rust
//! `crabka-object-store` — unified object-store construction shared by Crabka's
//! KIP-405 tiered storage (`crabka-remote-storage`) and observability blockstore
//! (`crabka-blockstore`).
//!
//! Scope is the object-store access/plumbing layer only: turning a typed
//! [`ObjectStoreConfig`] into an `object_store::ObjectStore` handle. Data
//! representation (verbatim Kafka segment bytes vs Parquet blocks) stays in the
//! respective consumer crates.
```

- [ ] **Step 3: Create a placeholder README (crate declares `readme`)**

Create `crates/object-store/README.md`:

```markdown
# crabka-object-store

Unified object-store construction (typed config → `object_store` handle) shared
by Crabka's KIP-405 tiered storage and observability blockstore.
```

- [ ] **Step 4: Verify it builds and joins the workspace**

Run: `cargo build -p crabka-object-store`
Expected: compiles clean; `crabka-object-store` resolves as a workspace member.

- [ ] **Step 5: Commit**

```bash
git add crates/object-store/Cargo.toml crates/object-store/src/lib.rs crates/object-store/README.md
git commit -m "feat(object-store): scaffold crabka-object-store crate"
```

---

## Task 2: Move the config types (`S3Config`, `GcsConfig`, multipart constants)

Move `S3Config` (from `crates/remote-storage/src/s3.rs:84-161`), `GcsConfig` (from `crates/remote-storage/src/gcs.rs:41-123`), and the two multipart constants (`s3.rs:44-57`) **verbatim**, including the credential-redacting `Debug` and the placeholder `Default`. Add the `ObjectStoreConfig` enum. Leave the originals in `remote-storage` untouched for now (removed in Task 5) — the two same-named types coexist harmlessly in different crates until then.

**Files:**
- Create: `crates/object-store/src/config.rs`
- Modify: `crates/object-store/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/object-store/src/config.rs` with only its test module first:

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-object-store`
Expected: FAIL — `S3Config`, `GcsConfig`, `ObjectStoreConfig`, and the constants are not defined.

- [ ] **Step 3: Add the implementation above the test module**

Insert at the TOP of `crates/object-store/src/config.rs` (above the `#[cfg(test)]` module):

```rust
//! Object-store connection config types shared across Crabka.

/// Default threshold above which a segment upload switches from a single PUT to
/// a streaming multipart upload. 100 MiB (well below AWS's 5 GiB single-PUT cap).
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 100 * 1024 * 1024;

/// Default per-part size for multipart uploads. 16 MiB (AWS requires >= 5 MiB per
/// non-final part and caps parts at 10 000, so 16 MiB scales past any real segment).
pub const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 16 * 1024 * 1024;

/// Selects and parameterises the object-store backend to construct.
#[derive(Clone, Debug)]
pub enum ObjectStoreConfig {
    /// Any S3-compatible endpoint (AWS S3, MinIO, Cloudflare R2).
    S3(S3Config),
    /// Native Google Cloud Storage (supports keyless GKE Workload Identity).
    Gcs(GcsConfig),
    /// Local filesystem rooted at `root` (dev / test).
    Local { root: std::path::PathBuf },
    /// In-process store (tests).
    InMemory,
}

/// Connection / bucket parameters for an S3-compatible backend.
///
/// Either `access_key_id` + `secret_access_key` or the standard AWS credential
/// chain supplies credentials; when both fields are `None`, `object_store` falls
/// back to the environment-variable chain.
#[derive(Clone)]
pub struct S3Config {
    /// S3 bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket (no leading or trailing slash).
    pub prefix: Option<String>,
    /// AWS region (required by AWS S3; placeholder `"us-east-1"` for MinIO/R2).
    pub region: String,
    /// Optional custom endpoint URL (e.g. `http://minio:9000`, R2 endpoint).
    pub endpoint: Option<String>,
    /// Optional explicit access key id (falls back to the AWS credential chain).
    pub access_key_id: Option<String>,
    /// Optional explicit secret access key (falls back to the AWS credential chain).
    pub secret_access_key: Option<String>,
    /// Allow plaintext HTTP (required by MinIO without TLS).
    pub allow_http: bool,
    /// Files at least this large upload via multipart. Defaults to [`DEFAULT_MULTIPART_THRESHOLD`].
    pub multipart_threshold: u64,
    /// Per-part size for multipart. Defaults to [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    pub multipart_chunk_size: usize,
}

impl std::fmt::Debug for S3Config {
    /// Redacts credential fields so a stray `{:?}` / tracing call never leaks them.
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

/// Connection / bucket parameters for native Google Cloud Storage.
///
/// Leaving every credential field `None` selects Workload Identity / ADC (the
/// metadata server) — the keyless GKE production path.
#[derive(Clone, PartialEq, Eq)]
pub struct GcsConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Optional key prefix inside the bucket (no leading or trailing slash).
    pub prefix: Option<String>,
    /// Optional path to a service-account JSON key file.
    pub service_account_path: Option<String>,
    /// Optional inline service-account JSON key (mutually exclusive with the path).
    pub service_account_key: Option<String>,
    /// Optional path to an application-default-credentials JSON file.
    pub application_credentials_path: Option<String>,
    /// Optional custom GCS API base URL (e.g. `http://fake-gcs:4443`).
    pub endpoint: Option<String>,
    /// Allow plaintext HTTP (required by emulators without TLS).
    pub allow_http: bool,
    /// Files at least this large upload via resumable multipart. Defaults to [`DEFAULT_MULTIPART_THRESHOLD`].
    pub multipart_threshold: u64,
    /// Per-part size for multipart. Defaults to [`DEFAULT_MULTIPART_CHUNK_SIZE`].
    pub multipart_chunk_size: usize,
}

impl std::fmt::Debug for GcsConfig {
    /// Redacts credential fields so a stray `{:?}` / tracing call never leaks them.
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
```

- [ ] **Step 4: Wire the module into `lib.rs`**

Append to `crates/object-store/src/lib.rs`:

```rust
mod config;

pub use config::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, GcsConfig, ObjectStoreConfig,
    S3Config,
};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-object-store`
Expected: PASS — all six config tests green.

- [ ] **Step 6: Commit**

```bash
git add crates/object-store/src/config.rs crates/object-store/src/lib.rs
git commit -m "feat(object-store): move S3Config/GcsConfig + multipart consts into config module"
```

---

## Task 3: `ObjectStoreError` + `From<object_store::Error>`

**Files:**
- Create: `crates/object-store/src/error.rs`
- Modify: `crates/object-store/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/object-store/src/error.rs` with only its test module first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn not_found_maps_to_structured_variant() {
        let err = object_store::Error::NotFound {
            path: "tenant/block".to_string(),
            source: "missing".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(matches!(&mapped, ObjectStoreError::NotFound(p) if p.to_string() == "tenant/block"));
    }

    #[test]
    fn other_errors_map_to_backend() {
        let err = object_store::Error::Generic {
            store: "s",
            source: "boom".into(),
        };
        assert!(matches!(ObjectStoreError::from(err), ObjectStoreError::Backend(_)));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-object-store error::`
Expected: FAIL — `ObjectStoreError` is not defined.

- [ ] **Step 3: Add the implementation above the test module**

Insert at the TOP of `crates/object-store/src/error.rs`:

```rust
//! Error taxonomy for object-store construction and access.

use object_store::path::Path as ObjectPath;

/// Errors raised by the object-store substrate.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// The backend builder rejected the config (bad bucket/region/endpoint/credentials).
    #[error("invalid object store config: {0}")]
    InvalidConfig(String),
    /// A specific object was not found (structured so consumers can upgrade it to
    /// their own domain error without string-matching).
    #[error("object not found: {0}")]
    NotFound(ObjectPath),
    /// Any other backend error, stringified so the public surface stays stable.
    #[error("object store backend error: {0}")]
    Backend(String),
}

impl From<object_store::Error> for ObjectStoreError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, .. } => Self::NotFound(ObjectPath::from(path)),
            other => Self::Backend(other.to_string()),
        }
    }
}
```

- [ ] **Step 4: Wire the module into `lib.rs`**

Add to `crates/object-store/src/lib.rs` (module list + re-export):

```rust
mod error;

pub use error::ObjectStoreError;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-object-store error::`
Expected: PASS — both mapping tests green.

- [ ] **Step 6: Commit**

```bash
git add crates/object-store/src/error.rs crates/object-store/src/lib.rs
git commit -m "feat(object-store): add ObjectStoreError with structured NotFound mapping"
```

---

## Task 4: `build_object_store` — the config → handle constructor

Move the `AmazonS3Builder`/`GoogleCloudStorageBuilder` wiring (currently in `remote-storage`'s `from_s3_config` at `s3.rs:197-217` and `from_gcs_config` at `gcs.rs:138-160`) into a single free function.

**Files:**
- Create: `crates/object-store/src/build.rs`
- Modify: `crates/object-store/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/object-store/src/build.rs` with only its test module first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::config::{GcsConfig, ObjectStoreConfig, S3Config};

    #[test]
    fn inmemory_builds() {
        assert!(build_object_store(&ObjectStoreConfig::InMemory).is_ok());
    }

    #[tokio::test]
    async fn inmemory_round_trips() {
        let store = build_object_store(&ObjectStoreConfig::InMemory).unwrap();
        let path = object_store::path::Path::from("t/x");
        store
            .put(&path, object_store::PutPayload::from(b"hi".to_vec()))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert!(&got[..] == b"hi");
    }

    #[test]
    fn local_builds_against_tempdir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ObjectStoreConfig::Local {
            root: dir.path().to_path_buf(),
        };
        assert!(build_object_store(&cfg).is_ok());
    }

    #[test]
    fn s3_builds_with_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::S3(S3Config {
            bucket: "b".into(),
            region: "us-east-1".into(),
            endpoint: Some("http://minio:9000".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from crates/remote-storage/src/gcs.rs tests: with every credential
    // field None, the builder selects Workload Identity / ADC and constructs.
    #[test]
    fn gcs_workload_identity_builds() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }

    // Ported from gcs.rs tests: a custom endpoint + allow_http builds.
    #[test]
    fn gcs_honors_endpoint_and_allow_http() {
        let cfg = ObjectStoreConfig::Gcs(GcsConfig {
            bucket: "b".into(),
            endpoint: Some("http://fake-gcs:4443".into()),
            allow_http: true,
            ..Default::default()
        });
        assert!(build_object_store(&cfg).is_ok());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-object-store build::`
Expected: FAIL — `build_object_store` is not defined.

- [ ] **Step 3: Add the implementation above the test module**

Insert at the TOP of `crates/object-store/src/build.rs`:

```rust
//! Config -> `object_store::ObjectStore` handle construction.

use std::sync::Arc;

use object_store::{ClientOptions, ObjectStore};

use crate::{
    config::{GcsConfig, ObjectStoreConfig, S3Config},
    error::ObjectStoreError,
};

/// Build an `object_store` handle for `cfg`.
///
/// The builder wiring (credential chains, endpoints, `allow_http`) is identical
/// to the per-crate constructors it replaces.
///
/// # Errors
///
/// Returns [`ObjectStoreError::InvalidConfig`] if the backend builder rejects the
/// bucket / region / endpoint / credential combination.
pub fn build_object_store(
    cfg: &ObjectStoreConfig,
) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    match cfg {
        ObjectStoreConfig::S3(s3) => build_s3(s3),
        ObjectStoreConfig::Gcs(gcs) => build_gcs(gcs),
        ObjectStoreConfig::Local { root } => {
            let store = object_store::local::LocalFileSystem::new_with_prefix(root)
                .map_err(|e| ObjectStoreError::InvalidConfig(format!("local: {e}")))?;
            Ok(Arc::new(store))
        }
        ObjectStoreConfig::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
    }
}

fn build_s3(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        .with_allow_http(cfg.allow_http);
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let (Some(k), Some(s)) = (&cfg.access_key_id, &cfg.secret_access_key) {
        builder = builder.with_access_key_id(k).with_secret_access_key(s);
    }
    let store = builder
        .build()
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("S3 builder: {e}")))?;
    Ok(Arc::new(store))
}

fn build_gcs(cfg: &GcsConfig) -> Result<Arc<dyn ObjectStore>, ObjectStoreError> {
    let mut builder =
        object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(&cfg.bucket);
    if let Some(path) = &cfg.service_account_path {
        builder = builder.with_service_account_path(path);
    }
    if let Some(key) = &cfg.service_account_key {
        builder = builder.with_service_account_key(key);
    }
    if let Some(adc) = &cfg.application_credentials_path {
        builder = builder.with_application_credentials(adc);
    }
    if let Some(endpoint) = &cfg.endpoint {
        builder = builder.with_base_url(endpoint);
    }
    if cfg.allow_http {
        builder = builder.with_client_options(ClientOptions::new().with_allow_http(true));
    }
    let store = builder
        .build()
        .map_err(|e| ObjectStoreError::InvalidConfig(format!("GCS builder: {e}")))?;
    Ok(Arc::new(store))
}
```

- [ ] **Step 4: Wire the module into `lib.rs`**

Add to `crates/object-store/src/lib.rs`:

```rust
mod build;

pub use build::build_object_store;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-object-store`
Expected: PASS — all config, error, and build tests green.

- [ ] **Step 6: Lint the new crate before it gets consumers**

Run: `cargo clippy -p crabka-object-store -- -D warnings`
Expected: no warnings (pedantic is on via workspace lints).

- [ ] **Step 7: Commit**

```bash
git add crates/object-store/src/build.rs crates/object-store/src/lib.rs
git commit -m "feat(object-store): add build_object_store config->handle constructor"
```

---

## Task 5: Migrate `crabka-remote-storage` onto the substrate

Delete the now-duplicated `S3Config`/`GcsConfig`/`DEFAULT_MULTIPART_*` from `remote-storage`, route the two production constructors through `build_object_store`, and re-export the moved types so downstream (broker) imports keep working. The engine (copy/fetch/delete, `block()` bridge, key layout) is **untouched** — this keeps the byte semantics identical and the full existing test suite green.

**Files:**
- Modify: `crates/remote-storage/Cargo.toml`
- Modify: `crates/remote-storage/src/s3.rs`
- Modify: `crates/remote-storage/src/gcs.rs`
- Modify: `crates/remote-storage/src/lib.rs`

- [ ] **Step 1: Add the dependency**

In `crates/remote-storage/Cargo.toml`, under `[dependencies]`, add:

```toml
crabka-object-store = { version = "0.3.8", path = "../object-store" }
```

- [ ] **Step 2: Rewrite `from_s3_config` and delete the moved items in `s3.rs`**

In `crates/remote-storage/src/s3.rs`:

1. **Delete** `pub const DEFAULT_MULTIPART_THRESHOLD` and `pub const DEFAULT_MULTIPART_CHUNK_SIZE` (lines 44-57), the entire `pub struct S3Config { .. }` and its `impl std::fmt::Debug for S3Config` and `impl Default for S3Config` (lines 84-161), and (in the `#[cfg(test)] mod tests`) the tests `s3_config_debug_redacts_credentials` and `multipart_size_constants` (they now live in the substrate — Task 2). Keep the engine tests (`put_path_uses_multipart_above_threshold`, `multipart_flushes_partial_tail_chunk`, and the copy/fetch/delete tests).

2. **Add** to the imports at the top of the file:

```rust
use crabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectStoreConfig, S3Config,
    build_object_store,
};
```

3. **Replace** the body of `from_s3_config` with:

```rust
    pub fn from_s3_config(cfg: &S3Config) -> Result<Self, RemoteStorageError> {
        let store = build_object_store(&ObjectStoreConfig::S3(cfg.clone()))
            .map_err(|e| RemoteStorageError::InvalidArgument(e.to_string()))?;
        Ok(Self::with_store(store, cfg.prefix.clone())
            .with_multipart_tuning(cfg.multipart_threshold, cfg.multipart_chunk_size))
    }
```

(`with_store` and `with_multipart_tuning` are unchanged; their bodies still reference `DEFAULT_MULTIPART_THRESHOLD`/`DEFAULT_MULTIPART_CHUNK_SIZE`, now imported from the substrate.)

- [ ] **Step 3: Rewrite `from_gcs_config` and delete the moved items in `gcs.rs`**

In `crates/remote-storage/src/gcs.rs`:

1. **Delete** the entire `pub struct GcsConfig { .. }` and its `impl std::fmt::Debug for GcsConfig` and `impl Default for GcsConfig` (lines 41-123), and the GCS builder tests in its `#[cfg(test)] mod tests` (`gcs_workload_identity_builds` / `honors_endpoint_and_allow_http` / `rejects_conflicting_credentials` — they moved to the substrate in Task 4; if a differently-named subset exists, remove the ones that only exercised the builder).

2. **Replace** the imports block (currently `use object_store::{ClientOptions, gcp::GoogleCloudStorageBuilder};` and `use crate::{error::RemoteStorageError, s3::{DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, S3RemoteStorage}};`) with:

```rust
use crabka_object_store::{GcsConfig, ObjectStoreConfig, build_object_store};

use crate::{error::RemoteStorageError, s3::S3RemoteStorage};
```

3. **Replace** the body of `from_gcs_config` with:

```rust
    pub fn from_gcs_config(cfg: &GcsConfig) -> Result<Self, RemoteStorageError> {
        let store = build_object_store(&ObjectStoreConfig::Gcs(cfg.clone()))
            .map_err(|e| RemoteStorageError::InvalidArgument(e.to_string()))?;
        Ok(Self::with_store(store, cfg.prefix.clone())
            .with_multipart_tuning(cfg.multipart_threshold, cfg.multipart_chunk_size))
    }
```

- [ ] **Step 4: Fix the re-exports in `lib.rs`**

In `crates/remote-storage/src/lib.rs`:

- **Remove** line 100: `pub use gcs::GcsConfig;`
- **Replace** the `pub use s3::{ DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, S3Config, S3RemoteStorage };` block (lines 109-111) with:

```rust
pub use s3::S3RemoteStorage;
```

- **Add** (next to the other `pub use`s):

```rust
pub use crabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, GcsConfig, ObjectStoreConfig,
    S3Config,
};
```

- [ ] **Step 5: Run the full `remote-storage` suite to verify green (behavior preserved)**

Run: `cargo test -p crabka-remote-storage`
Expected: PASS — the existing InMemory-backed suite (copy-then-fetch, partial/ranged fetch, each index type, idempotent delete, cluster-prefix isolation, multipart threshold + partial tail) all stay green, proving construction moved without changing behavior.

- [ ] **Step 6: Commit**

```bash
git add crates/remote-storage/Cargo.toml crates/remote-storage/src/s3.rs crates/remote-storage/src/gcs.rs crates/remote-storage/src/lib.rs
git commit -m "refactor(remote-storage): construct object stores via crabka-object-store"
```

---

## Task 6: Workspace + downstream (broker) green gate

No new code — a verification checkpoint proving the re-exports kept every downstream importer (notably the broker) compiling and green. If anything fails, the fix is a missed re-export in `remote-storage/src/lib.rs` (Invariant 5).

**Files:** none expected (fix re-exports only if a break surfaces).

- [ ] **Step 1: Build the broker (primary downstream consumer)**

Run: `cargo build -p crabka-broker`
Expected: compiles clean. If it fails on `unresolved import crabka_remote_storage::{S3Config|GcsConfig|DEFAULT_MULTIPART_*}`, re-check Task 5 Step 4 (a re-export is missing) and fix, then re-run.

- [ ] **Step 2: Run the whole workspace test suite**

Run: `cargo test --workspace`
Expected: PASS across all crates.

- [ ] **Step 3: Commit (only if a re-export fix was needed)**

```bash
git add -A
git commit -m "fix(remote-storage): restore object-store re-exports for downstream crates"
```

(If nothing changed, skip this commit.)

---

## Task 7: Migrate `crabka-blockstore` onto the substrate (additive `from_config`)

Add a second genuine consumer. `BlockStore::from_config` builds the store via `build_object_store` while the caller keeps supplying the DataFusion base `Url` (a query-engine concern that stays in the consumer). The existing `new(store, base)` is untouched, so no data-representation code moves.

**Files:**
- Modify: `crates/blockstore/Cargo.toml`
- Modify: `crates/blockstore/src/store.rs`

- [ ] **Step 1: Add the dependency**

In `crates/blockstore/Cargo.toml`, under `[dependencies]`, add:

```toml
crabka-object-store = { path = "../object-store" }
```

- [ ] **Step 2: Write the failing test**

In `crates/blockstore/src/store.rs`, inside the existing `#[cfg(test)] mod tests` block (the one that already does `let base = url::Url::parse("memory:///").unwrap();`), add:

```rust
    #[tokio::test]
    async fn from_config_inmemory_builds_usable_store() {
        use crabka_object_store::ObjectStoreConfig;

        let base = url::Url::parse("memory:///").unwrap();
        let bs = BlockStore::from_config(&ObjectStoreConfig::InMemory, base).unwrap();
        let store = bs.object_store();
        let path = object_store::path::Path::from("t/x");
        store
            .put(&path, object_store::PutPayload::from(b"hi".to_vec()))
            .await
            .unwrap();
        let got = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert2::assert!(&got[..] == b"hi");
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p crabka-blockstore from_config_inmemory_builds_usable_store`
Expected: FAIL — `BlockStore::from_config` is not defined.

- [ ] **Step 4: Implement `from_config`**

In `crates/blockstore/src/store.rs`, add to the `impl BlockStore` block (e.g. just after `new`):

```rust
    /// Build a `BlockStore` whose object store is constructed from `cfg` via the
    /// shared `crabka-object-store` substrate. `base` remains the caller's
    /// `DataFusion` registration URL (a query-engine concern owned by the caller).
    ///
    /// # Errors
    ///
    /// Returns [`BlockStoreError::ObjectStore`] if the backend builder rejects `cfg`.
    pub fn from_config(
        cfg: &crabka_object_store::ObjectStoreConfig,
        base: Url,
    ) -> Result<Self> {
        let store = crabka_object_store::build_object_store(cfg)
            .map_err(|e| BlockStoreError::ObjectStore(e.to_string()))?;
        Ok(Self::new(store, base))
    }
```

(`Result` is the crate alias `crate::error::Result`; `BlockStoreError` is already imported in `store.rs`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p crabka-blockstore from_config_inmemory_builds_usable_store`
Expected: PASS.

- [ ] **Step 6: Run the full blockstore suite**

Run: `cargo test -p crabka-blockstore`
Expected: PASS — the new constructor is additive; everything else is unchanged.

- [ ] **Step 7: Commit**

```bash
git add crates/blockstore/Cargo.toml crates/blockstore/src/store.rs
git commit -m "feat(blockstore): add BlockStore::from_config via crabka-object-store"
```

---

## Task 8: Final gate — format, lint, full test sweep

**Files:** none (formatting only).

- [ ] **Step 1: Format**

Run: `cargo +nightly fmt`
Expected: reformats if needed (import layout, etc.).

- [ ] **Step 2: Format check**

Run: `cargo +nightly fmt --check`
Expected: no diff.

- [ ] **Step 3: Clippy across the workspace (pedantic, deny warnings)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 4: Full test sweep**

Run: `cargo nextest run --workspace` (or `cargo test --workspace` if nextest is unavailable)
Expected: PASS across all crates.

- [ ] **Step 5: Commit (only if formatting changed anything)**

```bash
git add -A
git commit -m "style(object-store): cargo +nightly fmt"
```

---

## Self-Review

**1. Spec coverage (Ch. 0 M3, first increment — "extract a shared substrate crate owning object_store construction, consumed by both the KIP-405 remote tier and blockstore"):**
- New crate owning construction → Tasks 1-4 (`crabka-object-store`: config, error, `build_object_store`). ✅
- Consumed by the KIP-405 remote tier → Task 5 (`remote-storage`'s `from_s3_config`/`from_gcs_config` route through `build_object_store`). ✅
- Consumed by blockstore → Task 7 (`BlockStore::from_config`). ✅
- "path/prefix layout … index conventions" from the roadmap prose are **data representation** and are explicitly deferred (Invariant 3 / Deferred list) — the honest first increment is construction only. Documented, not silently dropped. ✅

**2. Placeholder scan:** No `TBD`/`TODO`/"add error handling"/"write tests for the above". Every code step shows complete code; every run step shows the command and expected result. The only "move existing tests" instructions (Task 5) name the exact tests and file and show the transformed constructor call. ✅

**3. Type consistency:** `S3Config`/`GcsConfig`/`ObjectStoreConfig`/`ObjectStoreError`/`build_object_store` are defined in Tasks 2-4 and referenced with identical signatures in Tasks 5 and 7. `build_object_store(&ObjectStoreConfig) -> Result<Arc<dyn ObjectStore>, ObjectStoreError>` is consistent at every call site. `remote-storage` maps `ObjectStoreError -> RemoteStorageError::InvalidArgument`; `blockstore` maps `ObjectStoreError -> BlockStoreError::ObjectStore(String)` — both target real, pre-existing variants (verified in `error.rs` of each crate). `with_store`/`with_multipart_tuning` signatures are unchanged from the current `s3.rs`. ✅

**4. Invariant check:** New crate deps are `object_store` (workspace) + `thiserror` only — publishable, no DataFusion/Parquet leak (Invariants 1-2). No engine/key-layout/`block()`/Parquet code moves (Invariant 3). `Debug` redaction impls + their tests move verbatim (Invariant 4). `lib.rs` re-exports preserve `S3Config`/`GcsConfig`/`DEFAULT_MULTIPART_*`/`S3RemoteStorage` for the broker (Invariant 5), gated by Task 6. Builder calls moved verbatim, no `RetryConfig` introduced (Invariant 6). Each task ends green (Invariant 7). ✅
