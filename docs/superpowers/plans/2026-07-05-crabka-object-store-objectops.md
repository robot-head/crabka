# `ObjectOps` Op-Unification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `crabka-object-store` a shared, typed, mockable object-store *operation* surface (`ObjectOps` trait + a concrete `ObjectStoreClient`), and route `crabka-remote-storage`'s tiered-storage engine (put / multipart / ranged-get / get / delete) through it — centralising the multipart-threshold decision, and replacing the fragile `"not found:"` string-prefix match with a structured `ObjectStoreError::NotFound`.

**Architecture:** Plan 1 (`2026-07-05-crabka-object-store-substrate-crate.md`) unified object-store *construction*. This plan unifies *access*: one async op trait (`ObjectOps`) with a single concrete implementation over `Arc<dyn object_store::ObjectStore>`, so the put/multipart/get/get_range/head/list/delete logic lives once. `remote-storage`'s synchronous `RemoteStorageManager` engine keeps its own `block()`-style bridge (renamed `block_os`) at its boundary and calls the async `ObjectOps` methods through it; the multipart branch and the `object_store::Error` → structured error mapping move into the substrate. `remote-storage`'s public API, key layout, KIP range semantics, and behavior are all preserved and guarded by its existing test suite.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `object_store` 0.13 (workspace-pinned), `async-trait`, `bytes`, `futures`, `thiserror`, `mockall` (dev), `tokio`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-north-star-roadmap-design.md`](../specs/2026-07-05-crabka-north-star-roadmap-design.md) — Chapter 0 (op-unification; the `ObjectOps` seam deferred from plan 1). **Prerequisite:** plan `2026-07-05-crabka-object-store-substrate-crate.md` is merged (`crabka-object-store` exists with `config`, `error`, `build` modules; `remote-storage` and `blockstore` already depend on it).

---

## Invariants (do not violate)

1. **`object_store` stays workspace-pinned at 0.13.** New substrate deps (`async-trait`, `bytes`, `futures`, `mockall`) all use `{ workspace = true }`.
2. **The substrate stays publishable.** No `datafusion`/`parquet` leak; the new deps are all crates.io deps.
3. **`ObjectOps` must stay object-safe and `#[automock]`-able.** Multipart is exposed as `put_from_path(&self, key, src: &std::path::Path, threshold, chunk_size)` — NOT generic over `impl Read` — so the trait is dyn-safe and mockall can generate `MockObjectOps`.
4. **The substrate never blocks on the runtime.** `ObjectOps` is async; the `block_on` bridge lives ONLY in `remote-storage` (`block_os`), called from `spawn_blocking`. The substrate must contain no `block_on`/`block_in_place`.
5. **Behavior is byte-for-byte preserved in `remote-storage`.** Same object keys, same inclusive→half-open range math, same KIP `end < start` validation, same idempotent delete, same `SegmentNotFound` upgrade. The existing `remote-storage` suite is the regression net and must stay green.
6. **`remote-storage`'s public API is unchanged.** `S3RemoteStorage::with_store(Arc<dyn ObjectStore>, Option<String>)` and `with_multipart_tuning` keep their signatures; the broker's imports are untouched.
7. **Every task leaves the workspace compiling and green** before its commit.

## Scope boundary

- **In scope:** `ObjectOps` + `ObjectStoreClient` in the substrate; `remote-storage` engine adoption; structured `NotFound`.
- **Deferred (named, not half-built):** routing `crabka-blockstore`'s ~20 plain-op sites through `ObjectOps` (its Parquet paths need the raw `Arc<dyn ObjectStore>` and can't route); `read_capped`; making `remote-storage`'s engine generic over `ObjectOps` for mock-based unit tests (the concrete client + the existing InMemory suite cover it for now).

---

## File Structure

**Modified — `crates/object-store/`:**
- `Cargo.toml` — add `async-trait`, `bytes`, `futures` deps; `mockall` dev-dep.
- `src/error.rs` — add an `Io(#[from] std::io::Error)` variant (needed by `put_from_path`).
- `src/ops.rs` — **new** — `ObjectOps` trait (`#[cfg_attr(test, mockall::automock)]`) + concrete `ObjectStoreClient`. One responsibility: the shared op surface.
- `src/lib.rs` — export `ObjectOps`, `ObjectStoreClient`.

**Modified — `crates/remote-storage/`:**
- `src/s3.rs` — swap the `store: Arc<dyn ObjectStore>` field for `ops: ObjectStoreClient`; replace `put_path`/`put_path_multipart`/`put_bytes`/`block`/`map_object_store_error` with `ObjectOps` calls through a `block_os` bridge; structured `NotFound`.
- `src/error.rs` — add `From<crabka_object_store::ObjectStoreError> for RemoteStorageError`.

---

## Task 1: Add `ObjectOps` dependencies to the substrate

**Files:**
- Modify: `crates/object-store/Cargo.toml`

- [ ] **Step 1: Add the runtime + dev dependencies**

In `crates/object-store/Cargo.toml`, extend `[dependencies]` and `[dev-dependencies]`:

```toml
[dependencies]
object_store = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
bytes = { workspace = true }
futures = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
mockall = { workspace = true }
```

- [ ] **Step 2: Verify the crate still builds**

Run: `cargo build -p crabka-object-store`
Expected: compiles clean (new deps resolve from the workspace).

- [ ] **Step 3: Commit**

```bash
git add crates/object-store/Cargo.toml
git commit -m "build(object-store): add async-trait/bytes/futures + mockall for ObjectOps"
```

---

## Task 2: Add the `Io` variant to `ObjectStoreError`

`put_from_path` reads a local file, so the error type needs an I/O variant.

**Files:**
- Modify: `crates/object-store/src/error.rs`

- [ ] **Step 1: Write the failing test**

In `crates/object-store/src/error.rs`, add to the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn io_error_converts_via_from() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "nope");
        let err: ObjectStoreError = io.into();
        assert!(matches!(err, ObjectStoreError::Io(_)));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-object-store io_error_converts_via_from`
Expected: FAIL — `ObjectStoreError` has no `Io` variant / no `From<std::io::Error>`.

- [ ] **Step 3: Add the variant**

In `crates/object-store/src/error.rs`, add the variant to the enum (above `InvalidConfig`):

```rust
    /// A local-filesystem I/O failure (e.g. reading a segment file to upload).
    #[error("object store I/O error: {0}")]
    Io(#[from] std::io::Error),
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-object-store error::`
Expected: PASS — the `Io` conversion and the existing `NotFound`/`Backend` mapping tests are green.

- [ ] **Step 5: Commit**

```bash
git add crates/object-store/src/error.rs
git commit -m "feat(object-store): add ObjectStoreError::Io variant"
```

---

## Task 3: `ObjectOps` trait + concrete `ObjectStoreClient`

**Files:**
- Create: `crates/object-store/src/ops.rs`
- Modify: `crates/object-store/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/object-store/src/ops.rs` with only its test module first:

```rust
#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use assert2::assert;
    use object_store::{GetRange, path::Path};

    use super::*;

    fn client() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(object_store::memory::InMemory::new()))
    }

    #[tokio::test]
    async fn put_get_round_trips() {
        let c = client();
        let key = Path::from("a/b");
        c.put(&key, bytes::Bytes::from_static(b"hello")).await.unwrap();
        let got = c.get(&key).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn get_range_returns_slice() {
        let c = client();
        let key = Path::from("a/b");
        c.put(&key, bytes::Bytes::from_static(b"hello world")).await.unwrap();
        let got = c.get_range(&key, GetRange::Bounded(0..5)).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn get_missing_maps_to_not_found() {
        let c = client();
        let err = c.get(&Path::from("nope")).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn head_and_list_and_delete() {
        let c = client();
        let key = Path::from("p/x");
        c.put(&key, bytes::Bytes::from_static(b"1234")).await.unwrap();
        assert!(c.head(&key).await.unwrap().size == 4);
        let listed = c.list(Some(&Path::from("p"))).await.unwrap();
        assert!(listed.iter().any(|m| m.location == key));
        c.delete(&key).await.unwrap();
        assert!(matches!(c.get(&key).await.unwrap_err(), ObjectStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn put_from_path_single_put_below_threshold() {
        let c = client();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"tiny").unwrap();
        let key = Path::from("seg/small");
        c.put_from_path(&key, f.path(), 8, 4).await.unwrap();
        assert!(&c.get(&key).await.unwrap()[..] == b"tiny");
    }

    #[tokio::test]
    async fn put_from_path_multipart_above_threshold() {
        let c = client();
        let payload = vec![7u8; 20]; // 20 bytes, threshold 8, chunk 4 -> multipart
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&payload).unwrap();
        let key = Path::from("seg/big");
        c.put_from_path(&key, f.path(), 8, 4).await.unwrap();
        assert!(c.get(&key).await.unwrap()[..] == payload[..]);
    }

    #[tokio::test]
    async fn mock_seam_compiles_and_returns() {
        let mut mock = MockObjectOps::new();
        mock.expect_get()
            .returning(|_| Ok(bytes::Bytes::from_static(b"x")));
        let got = mock.get(&Path::from("k")).await.unwrap();
        assert!(&got[..] == b"x");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-object-store ops::`
Expected: FAIL — `ObjectOps`, `ObjectStoreClient`, `MockObjectOps` are not defined.

- [ ] **Step 3: Add the implementation above the test module**

Insert at the TOP of `crates/object-store/src/ops.rs`:

```rust
//! The shared object-store operation surface: an async, mockable [`ObjectOps`]
//! trait and its single concrete implementation [`ObjectStoreClient`] over
//! `object_store`. Consumers route their put/get/delete/multipart calls through
//! this so the operation logic (notably the multipart-threshold branch and the
//! `object_store::Error` -> [`ObjectStoreError`] mapping) lives in one place.

use std::sync::Arc;

use bytes::Bytes;
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStore as _, PutPayload, WriteMultipart, path::Path,
};

use crate::error::ObjectStoreError;

/// Async object-store operations. `Send + Sync` so it can be shared across tasks.
///
/// Kept dyn-safe and `#[automock]`-able: multipart upload is expressed as
/// [`ObjectOps::put_from_path`] over a filesystem path rather than a generic
/// reader, so the trait mocks cleanly for mutation-testable IO decision logic.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ObjectOps: Send + Sync {
    /// Single-PUT an in-memory payload.
    async fn put(&self, key: &Path, bytes: Bytes) -> Result<(), ObjectStoreError>;

    /// Upload a local file, choosing single-PUT below `threshold` bytes and
    /// streaming multipart (in `chunk_size` parts) at or above it.
    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
    ) -> Result<(), ObjectStoreError>;

    /// Fetch a whole object.
    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError>;

    /// Fetch a byte range of an object.
    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError>;

    /// Fetch object metadata (size, etag, …).
    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError>;

    /// List objects under an optional prefix.
    async fn list(&self, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError>;

    /// Delete an object.
    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError>;
}

/// The single concrete [`ObjectOps`] implementation, wrapping any
/// `object_store::ObjectStore` handle (e.g. one built by
/// [`build_object_store`](crate::build_object_store), or an
/// `object_store::memory::InMemory` in tests).
#[derive(Clone)]
pub struct ObjectStoreClient {
    inner: Arc<dyn object_store::ObjectStore>,
}

impl ObjectStoreClient {
    /// Wrap an existing object-store handle.
    #[must_use]
    pub fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl ObjectOps for ObjectStoreClient {
    async fn put(&self, key: &Path, bytes: Bytes) -> Result<(), ObjectStoreError> {
        self.inner.put(key, PutPayload::from_bytes(bytes)).await?;
        Ok(())
    }

    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
    ) -> Result<(), ObjectStoreError> {
        let len = std::fs::metadata(src)?.len();
        if len < threshold {
            let bytes = std::fs::read(src)?;
            self.inner.put(key, PutPayload::from(bytes)).await?;
            return Ok(());
        }
        let upload = self.inner.put_multipart(key).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
        let mut file = std::fs::File::open(src)?;
        let mut buf = vec![0u8; chunk_size];
        loop {
            let n = std::io::Read::read(&mut file, &mut buf)?;
            if n == 0 {
                break;
            }
            writer.write(&buf[..n]);
        }
        writer.finish().await?;
        Ok(())
    }

    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError> {
        Ok(self.inner.get(key).await?.bytes().await?)
    }

    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError> {
        let opts = GetOptions {
            range: Some(range),
            ..Default::default()
        };
        Ok(self.inner.get_opts(key, opts).await?.bytes().await?)
    }

    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        Ok(self.inner.head(key).await?)
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError> {
        use futures::stream::TryStreamExt as _;
        Ok(self.inner.list(prefix).try_collect::<Vec<_>>().await?)
    }

    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await?;
        Ok(())
    }
}
```

- [ ] **Step 4: Wire the module into `lib.rs`**

Add to `crates/object-store/src/lib.rs`:

```rust
mod ops;

pub use ops::{ObjectOps, ObjectStoreClient};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p crabka-object-store`
Expected: PASS — all `ops` tests (round-trips, ranged get, missing→NotFound, head/list/delete, single-PUT + multipart `put_from_path`, and the `MockObjectOps` seam) plus the existing config/error/build tests are green.

- [ ] **Step 6: Lint the crate**

Run: `cargo clippy -p crabka-object-store --all-targets -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add crates/object-store/src/ops.rs crates/object-store/src/lib.rs
git commit -m "feat(object-store): add ObjectOps trait + ObjectStoreClient impl"
```

---

## Task 4: Route `crabka-remote-storage`'s engine through `ObjectOps`

Swap the raw `store` handle for an `ObjectStoreClient`, bridge its async ops through a renamed `block_os`, move the multipart-threshold branch into the substrate call, and replace the `"not found:"` string-prefix match with the structured `ObjectStoreError::NotFound`. The existing `remote-storage` suite guards behavior.

**Files:**
- Modify: `crates/remote-storage/src/error.rs`
- Modify: `crates/remote-storage/src/s3.rs`

- [ ] **Step 1: Add `From<ObjectStoreError>` for `RemoteStorageError`**

In `crates/remote-storage/src/error.rs`, append (below the enum):

```rust
impl From<crabka_object_store::ObjectStoreError> for RemoteStorageError {
    fn from(err: crabka_object_store::ObjectStoreError) -> Self {
        use crabka_object_store::ObjectStoreError as E;
        match err {
            E::Io(e) => RemoteStorageError::Io(e),
            E::InvalidConfig(m) => RemoteStorageError::InvalidArgument(m),
            // Reachable only if a caller doesn't intercept NotFound first; the
            // engine methods below match NotFound explicitly before `.into()`.
            E::NotFound(p) => RemoteStorageError::Backend(format!("not found: {p}")),
            E::Backend(m) => RemoteStorageError::Backend(m),
        }
    }
}
```

- [ ] **Step 2: Replace the struct field, imports, and constructor in `s3.rs`**

In `crates/remote-storage/src/s3.rs`:

1. **Replace** the top-of-file `use` items. The old block is:

```rust
use std::{io::Read, path::Path, sync::Arc};

use bytes::Bytes;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart,
    path::Path as ObjectPath,
};
use tracing::instrument;

use crabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectStoreConfig, S3Config,
    build_object_store,
};
```

Replace it with:

```rust
use std::sync::Arc;

use object_store::{GetRange, ObjectStore, path::Path as ObjectPath};
use tracing::instrument;

use crabka_object_store::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_MULTIPART_THRESHOLD, ObjectOps, ObjectStoreClient,
    ObjectStoreConfig, ObjectStoreError, S3Config, build_object_store,
};
```

2. **Change** the struct field from `store: Arc<dyn ObjectStore>` to `ops: ObjectStoreClient`:

```rust
pub struct S3RemoteStorage {
    ops: ObjectStoreClient,
    /// Optional key prefix (joined with `/` to every object key).
    prefix: Option<String>,
    /// File-size threshold above which uploads switch to multipart.
    multipart_threshold: u64,
    /// Per-part size used by the multipart path.
    multipart_chunk_size: usize,
}
```

3. **Update** `with_store` to wrap the handle (public signature unchanged) and `from_s3_config` (unchanged logic, still uses `build_object_store` + `with_store`):

```rust
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: Option<String>) -> Self {
        Self {
            ops: ObjectStoreClient::new(store),
            prefix,
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunk_size: DEFAULT_MULTIPART_CHUNK_SIZE,
        }
    }
```

(`from_s3_config` and `with_multipart_tuning` are unchanged — `from_s3_config` still does `build_object_store(&ObjectStoreConfig::S3(cfg.clone()))` then `Self::with_store(store, cfg.prefix.clone()).with_multipart_tuning(...)`.)

- [ ] **Step 3: Replace the sync bridge and delete the old op helpers**

In `crates/remote-storage/src/s3.rs`:

1. **Delete** these three methods entirely: `put_path` (the `#[instrument]`'d fn ~257-274), `put_path_multipart` (~281-305), `put_bytes` (~307-311).

2. **Delete** the free function `map_object_store_error` (~324-336). (`index_filename` stays.)

3. **Replace** the `block` method with `block_os` (which bridges the substrate's `ObjectStoreError`):

```rust
    /// Run an async [`ObjectOps`] call to completion on the current Tokio
    /// runtime. Sync trait callers reach this through `spawn_blocking`, inside
    /// which `Handle::current()` is always available. The `block_on` bridge lives
    /// here (never in the substrate).
    fn block_os<T, F>(fut: F) -> Result<T, ObjectStoreError>
    where
        F: std::future::Future<Output = Result<T, ObjectStoreError>>,
    {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            ObjectStoreError::Backend(
                "S3RemoteStorage requires an active Tokio runtime; call from spawn_blocking".into(),
            )
        })?;
        tokio::task::block_in_place(|| handle.block_on(fut))
    }
```

- [ ] **Step 4: Rewrite the `RemoteStorageManager` engine methods**

In `crates/remote-storage/src/s3.rs`, replace the four trait-method bodies:

`copy_log_segment_data` — route puts through `ObjectOps`:

```rust
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let t = self.multipart_threshold;
        let c = self.multipart_chunk_size;
        Self::block_os(self.ops.put_from_path(&self.log_key(metadata), &data.log_segment, t, c))?;
        Self::block_os(self.ops.put_from_path(
            &self.index_key(metadata, IndexType::Offset),
            &data.offset_index,
            t,
            c,
        ))?;
        Self::block_os(self.ops.put_from_path(
            &self.index_key(metadata, IndexType::Timestamp),
            &data.time_index,
            t,
            c,
        ))?;
        if let Some(snap) = &data.producer_snapshot_index {
            Self::block_os(self.ops.put_from_path(
                &self.index_key(metadata, IndexType::ProducerSnapshot),
                snap,
                t,
                c,
            ))?;
        }
        Self::block_os(self.ops.put(
            &self.index_key(metadata, IndexType::LeaderEpoch),
            data.leader_epoch_index.clone(),
        ))?;
        if let Some(txn) = &data.transaction_index {
            Self::block_os(self.ops.put_from_path(
                &self.index_key(metadata, IndexType::Transaction),
                txn,
                t,
                c,
            ))?;
        }
        Ok(None)
    }
```

`fetch_log_segment` — keep the KIP range math; structured `NotFound`:

```rust
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = self.log_key(metadata);
        let range = match end_position {
            Some(end) => {
                if end < start_position {
                    return Err(RemoteStorageError::InvalidArgument(format!(
                        "end_position {end} < start_position {start_position}"
                    )));
                }
                // GetRange::Bounded is half-open [start, end); the trait contract
                // is inclusive end, so add 1 and saturate.
                GetRange::Bounded(u64::from(start_position)..u64::from(end).saturating_add(1))
            }
            None => GetRange::Offset(u64::from(start_position)),
        };
        match Self::block_os(self.ops.get_range(&key, range)) {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            )),
            Err(other) => Err(other.into()),
        }
    }
```

`fetch_index` — structured `NotFound`:

```rust
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let key = self.index_key(metadata, index_type);
        match Self::block_os(self.ops.get(&key)) {
            Ok(bytes) => Ok(bytes.to_vec()),
            Err(ObjectStoreError::NotFound(_)) => Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            )),
            Err(other) => Err(other.into()),
        }
    }
```

`delete_log_segment_data` — idempotent via structured `NotFound`:

```rust
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        for key in [
            self.log_key(metadata),
            self.index_key(metadata, IndexType::Offset),
            self.index_key(metadata, IndexType::Timestamp),
            self.index_key(metadata, IndexType::ProducerSnapshot),
            self.index_key(metadata, IndexType::LeaderEpoch),
            self.index_key(metadata, IndexType::Transaction),
        ] {
            match Self::block_os(self.ops.delete(&key)) {
                Ok(()) => {}
                // Idempotent: deleting an absent object succeeds.
                Err(ObjectStoreError::NotFound(_)) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
```

(Keep the `#[instrument(...)]` attributes above each method exactly as they are.)

- [ ] **Step 5: Fix the `s3.rs` test module for the moved constructor tests**

Two `#[cfg(test)]` tests in `s3.rs` reference the deleted engine internals or moved items. In `crates/remote-storage/src/s3.rs`'s `mod tests`:
- The engine tests (`rsm(...)` helper, copy-then-fetch, ranged fetch, each index type, idempotent delete, prefix isolation, and the multipart tests `put_path_uses_multipart_above_threshold` / `multipart_flushes_partial_tail_chunk`) all drive the **public** `RemoteStorageManager` trait via `S3RemoteStorage::with_store(...)`, so they keep working unchanged — the multipart path now runs through `ObjectOps::put_from_path` but is still exercised end-to-end. Leave them.
- If any test called the now-deleted private `put_path`/`put_bytes` directly (grep the test module for `\.put_path(`, `\.put_bytes(`, `\.put_path_multipart(`), rewrite it to drive the public trait method instead (e.g. `copy_log_segment_data`), since those private helpers no longer exist.

- [ ] **Step 6: Run the full `remote-storage` suite (regression net)**

Run: `cargo test -p crabka-remote-storage`
Expected: PASS — copy-then-fetch, partial/ranged fetch, each index type, idempotent delete, cluster-prefix isolation, and the multipart threshold + partial-tail tests all stay green, proving the op-routing preserved behavior and the `SegmentNotFound` upgrade still fires (now via structured `NotFound`).

- [ ] **Step 7: Commit**

```bash
git add crates/remote-storage/src/error.rs crates/remote-storage/src/s3.rs
git commit -m "refactor(remote-storage): route engine ops through ObjectOps + structured NotFound"
```

---

## Task 5: Workspace + downstream (broker) green gate

**Files:** none expected.

- [ ] **Step 1: Build the broker**

Run: `cargo build -p crabka-broker`
Expected: compiles clean — `S3RemoteStorage::with_store`/`from_s3_config`/`from_gcs_config` and all `remote-storage` re-exports are unchanged, so downstream is unaffected.

- [ ] **Step 2: Full workspace test suite**

Run: `cargo test --workspace`
Expected: PASS across all crates.

- [ ] **Step 3: Commit (only if a fix was required)**

```bash
git add -A
git commit -m "fix(remote-storage): downstream compatibility after ObjectOps adoption"
```

(Skip if nothing changed.)

---

## Task 6: Final gate — format, lint, full test sweep

**Files:** none (formatting only).

- [ ] **Step 1: Format**

Run: `cargo +nightly fmt`
Expected: reformats if needed.

- [ ] **Step 2: Format check**

Run: `cargo +nightly fmt --check`
Expected: no diff.

- [ ] **Step 3: Clippy across the workspace (pedantic, deny warnings)**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: no warnings. (Watch for a now-unused `object_store` import in `s3.rs` — if clippy flags one, remove it.)

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

**1. Spec coverage (Ch. 0 op-unification — the `ObjectOps` seam deferred from plan 1):**
- Shared typed op surface → Task 3 (`ObjectOps` trait + `ObjectStoreClient`, with the multipart-threshold branch centralised in `put_from_path`). ✅
- `remote-storage` routes its engine ops through it → Task 4 (copy/fetch/delete via `ObjectOps`, `block_os` bridge). ✅
- Structured `NotFound` replaces the string-prefix match → Task 4 (`Err(ObjectStoreError::NotFound(_))` arms in `fetch_log_segment`/`fetch_index`/`delete_log_segment_data`; `map_object_store_error` deleted). ✅
- `mockall` seam per workspace convention → Task 3 (`#[cfg_attr(test, mockall::automock)]` + a `MockObjectOps` test). ✅
- Blockstore op-routing + `read_capped` explicitly deferred (Scope boundary), not silently dropped. ✅

**2. Placeholder scan:** No `TBD`/`TODO`/"handle errors". Every code step shows complete code; every run step shows command + expected result. The one conditional (Task 4 Step 5 / Task 6 Step 3) names the exact grep / clippy signal and the concrete fix. ✅

**3. Type consistency:** `ObjectOps`/`ObjectStoreClient`/`ObjectStoreError` are defined in Task 3 and referenced identically in Task 4. `block_os<T, F>(fut: F) -> Result<T, ObjectStoreError>` matches every call site (`put`, `put_from_path`, `get`, `get_range`, `delete`). `put_from_path(key, &std::path::Path, threshold: u64, chunk_size: usize)` — call sites pass `&data.log_segment` (`&PathBuf` → `&Path`), `self.multipart_threshold: u64`, `self.multipart_chunk_size: usize`: consistent. `From<ObjectStoreError> for RemoteStorageError` targets real `RemoteStorageError` variants (`Io`/`InvalidArgument`/`Backend`, verified in `error.rs`). The concrete impl is named `ObjectStoreClient` (not `ObjectStore`) to avoid colliding with the `object_store::ObjectStore` trait imported in `s3.rs`. ✅

**4. Invariant check:** New deps workspace-pinned, crates.io-only (Invariants 1-2). `put_from_path` is path-based, so the trait stays object-safe / `#[automock]`-able (Invariant 3). `block_on` lives only in `remote-storage::block_os`; the substrate is pure-async (Invariant 4). Key layout, range math, `end < start` validation, idempotent delete, and `SegmentNotFound` upgrade preserved; existing suite is the guard (Invariant 5). `with_store`/`from_s3_config`/`with_multipart_tuning` signatures unchanged (Invariant 6). Each task ends green (Invariant 7). ✅
