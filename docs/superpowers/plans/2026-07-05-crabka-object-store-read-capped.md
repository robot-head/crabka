# `read_capped` — Consolidate the Buffered-Read OOM Guard

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the duplicated "`head()` an object, reject it if it exceeds a byte cap, then `get()` it" guard — which today exists as two verbatim copies in `crabka-blockstore` (`index.rs::load_with_cap` and `profile_index.rs::load_path_with_cap`) — into a single tested `read_capped` helper in `crabka-object-store`, and adopt it at both sites.

**Architecture:** This is a focused consolidation of a *safety* invariant, not a feature. Both blockstore index-snapshot loaders buffer a whole JSON object from shared object storage into memory before deserializing; each first `head()`s the object and errors if its size exceeds a per-signal cap (`MAX_INDEX_SNAPSHOT_BYTES` / `MAX_PROFILE_INDEX_SNAPSHOT_BYTES`), so a corrupt or malicious oversized snapshot cannot OOM the process. That guard is currently copy-pasted. This plan gives `crabka-object-store` a `read_capped(store, key, max_bytes) -> Bytes` helper (returning a structured `ObjectStoreError::TooLarge` on breach), and rewrites both blockstore sites to call it — mapping `TooLarge` back to the exact same `BlockStoreError::InvalidBlock` message per site so behavior is preserved.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `object_store` 0.13 (workspace-pinned), `bytes`, `thiserror`, `tokio`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-north-star-roadmap-design.md`](../specs/2026-07-05-crabka-north-star-roadmap-design.md) — Chapter 0 (the `read_capped` follow-up deferred from the ObjectOps plan). **Prerequisite:** plans `2026-07-05-crabka-object-store-substrate-crate.md` and `2026-07-05-crabka-object-store-objectops.md` are merged (`crabka-object-store` exists with `error`/`config`/`build`/`ops`; it depends on `bytes`; `blockstore` depends on `crabka-object-store`).

---

## Invariants

1. **Behavior preserved at both blockstore sites.** Oversize still yields `BlockStoreError::InvalidBlock` with the **exact same message text** (`"index snapshot ... exceeds cap of ... bytes"` / `"profile index snapshot ... exceeds cap of ... bytes"`); under-cap still returns the deserialized index; a missing object still surfaces as it does today. The existing blockstore tests are the regression net.
2. **Accepted minor change:** the `size` field recorded on the two `#[instrument]` load spans (via `tracing::Span::current().record("size", meta.size)`) is dropped, because the `head()` now happens inside `read_capped`. This is a debug-level tracing field, not behavior. Documented, intentional.
3. **`crabka-object-store` stays publishable** (no `datafusion`/`parquet`; `read_capped` uses only `object_store` + `bytes`).
4. **`read_capped` operates on the raw `Arc<dyn object_store::ObjectStore>`** (what the blockstore call sites hold) — it does NOT require `ObjectOps`, and it never `block_on`s.
5. **Every task leaves the workspace compiling and green** before its commit.

## Scope boundary

- **In scope:** `read_capped` + `ObjectStoreError::TooLarge` in the substrate; adoption at `index.rs::load_with_cap` and `profile_index.rs::load_path_with_cap`.
- **Not in scope:** `reader.rs`'s `head()` sites (those size a `ParquetObjectReader` — a parquet-streaming concern, not a whole-object capped read); routing blockstore's `index_snapshot` module or other plain-op sites through `ObjectOps` (ripples up through callers); any Parquet path.

---

## File Structure

**Modified — `crates/object-store/`:**
- `src/error.rs` — add an `ObjectStoreError::TooLarge { key, size, max_bytes }` variant.
- `src/read.rs` — **new** — the `read_capped` helper. One responsibility: capped buffered read.
- `src/lib.rs` — export `read_capped`.

**Modified — `crates/blockstore/`:**
- `src/index.rs` — `load_with_cap` calls `read_capped`.
- `src/profile_index.rs` — `load_path_with_cap` calls `read_capped`.

---

## Task 1: Add the `TooLarge` variant to `ObjectStoreError`

**Files:**
- Modify: `crates/object-store/src/error.rs`

- [ ] **Step 1: Write the failing test**

In `crates/object-store/src/error.rs`, add to the `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn too_large_display_includes_sizes() {
        let err = ObjectStoreError::TooLarge {
            key: object_store::path::Path::from("index/snapshot.json"),
            size: 1000,
            max_bytes: 256,
        };
        let msg = err.to_string();
        assert!(msg.contains("1000"));
        assert!(msg.contains("256"));
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p crabka-object-store too_large_display_includes_sizes`
Expected: FAIL — `ObjectStoreError` has no `TooLarge` variant.

- [ ] **Step 3: Add the variant**

In `crates/object-store/src/error.rs`, add to the enum (after `NotFound`):

```rust
    /// An object exceeded a caller-supplied size cap during a buffered read
    /// (guards against OOM on a corrupt or malicious oversized object).
    #[error("object `{key}` is {size} bytes, exceeds cap of {max_bytes} bytes")]
    TooLarge {
        /// The object that breached the cap.
        key: object_store::path::Path,
        /// The object's actual size in bytes.
        size: u64,
        /// The cap in bytes.
        max_bytes: u64,
    },
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-object-store error::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/object-store/src/error.rs
git commit -m "feat(object-store): add ObjectStoreError::TooLarge variant"
```

---

## Task 2: Add the `read_capped` helper

**Files:**
- Create: `crates/object-store/src/read.rs`
- Modify: `crates/object-store/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/object-store/src/read.rs` with only its test module first:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use object_store::{ObjectStore as _, PutPayload, path::Path};

    use super::*;

    fn store_with(key: &str, bytes: &'static [u8]) -> Arc<dyn object_store::ObjectStore> {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let s = store.clone();
        let k = Path::from(key);
        futures::executor::block_on(async move { s.put(&k, PutPayload::from(bytes)).await })
            .unwrap();
        store
    }

    #[tokio::test]
    async fn under_cap_returns_bytes() {
        let store = store_with("k", b"hello");
        let got = read_capped(&store, &Path::from("k"), 1024).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn over_cap_returns_too_large() {
        let store = store_with("k", b"hello world");
        let err = read_capped(&store, &Path::from("k"), 4).await.unwrap_err();
        assert!(matches!(
            err,
            ObjectStoreError::TooLarge { size: 11, max_bytes: 4, .. }
        ));
    }

    #[tokio::test]
    async fn missing_object_maps_to_not_found() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let err = read_capped(&store, &Path::from("absent"), 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-object-store read::`
Expected: FAIL — `read_capped` is not defined.

- [ ] **Step 3: Add the implementation above the test module**

Insert at the TOP of `crates/object-store/src/read.rs`:

```rust
//! Capped buffered reads: `head()` an object, reject it if it exceeds a byte
//! cap, then `get()` it. Centralises the OOM guard used before buffering a whole
//! object (e.g. an index snapshot) into memory.

use std::sync::Arc;

use bytes::Bytes;
use object_store::{ObjectStore as _, path::Path};

use crate::error::ObjectStoreError;

/// Read a whole object, rejecting it if its size exceeds `max_bytes`.
///
/// `head()`s first so an oversized object is refused *before* any bytes are
/// buffered — the guard against OOM on a corrupt or malicious object.
///
/// # Errors
///
/// - [`ObjectStoreError::TooLarge`] if the object is larger than `max_bytes`.
/// - [`ObjectStoreError::NotFound`] if the object does not exist.
/// - [`ObjectStoreError::Backend`] for any other backend failure.
pub async fn read_capped(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &Path,
    max_bytes: u64,
) -> Result<Bytes, ObjectStoreError> {
    let meta = store.head(key).await?;
    if meta.size > max_bytes {
        return Err(ObjectStoreError::TooLarge {
            key: key.clone(),
            size: meta.size,
            max_bytes,
        });
    }
    Ok(store.get(key).await?.bytes().await?)
}
```

- [ ] **Step 4: Add the `futures` dev-dependency (for the test's `block_on`)**

The test module uses `futures::executor::block_on`. `futures` is already a normal dependency of `crabka-object-store` (added in the ObjectOps plan), so it is available to tests too — no manifest change needed. (If `cargo test` reports `unresolved import futures`, confirm `futures = { workspace = true }` is under `[dependencies]` in `crates/object-store/Cargo.toml`.)

- [ ] **Step 5: Wire the module into `lib.rs`**

Add to `crates/object-store/src/lib.rs`:

```rust
mod read;

pub use read::read_capped;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p crabka-object-store read::`
Expected: PASS — under-cap returns bytes, over-cap returns `TooLarge { size: 11, max_bytes: 4 }`, missing maps to `NotFound`.

- [ ] **Step 7: Commit**

```bash
git add crates/object-store/src/read.rs crates/object-store/src/lib.rs
git commit -m "feat(object-store): add read_capped buffered-read OOM guard"
```

---

## Task 3: Adopt `read_capped` in `crabka-blockstore`'s two load paths

Rewrite both duplicated guards to call `read_capped`, mapping `TooLarge` back to the exact `BlockStoreError::InvalidBlock` message each site produces today. The existing blockstore tests guard behavior.

**Files:**
- Modify: `crates/blockstore/src/index.rs`
- Modify: `crates/blockstore/src/profile_index.rs`

- [ ] **Step 1: Rewrite `index.rs::load_with_cap`**

In `crates/blockstore/src/index.rs`, replace the body of `load_with_cap` (currently `head` → size-check → `get` → `bytes` → `from_slice`) with:

```rust
    async fn load_with_cap(
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        max_bytes: usize,
    ) -> Result<Self> {
        let path = Path::from(object_key);
        let bytes = crabka_object_store::read_capped(store, &path, max_bytes as u64)
            .await
            .map_err(|e| match e {
                crabka_object_store::ObjectStoreError::TooLarge { size, max_bytes, .. } => {
                    BlockStoreError::InvalidBlock(format!(
                        "index snapshot `{object_key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
                    ))
                }
                other => BlockStoreError::ObjectStore(other.to_string()),
            })?;
        Ok(serde_json::from_slice(&bytes)?)
    }
```

(The `#[instrument(...)]` attribute above `load_with_cap` stays, minus the now-unused `size` field — change its `fields(object_key = %object_key, size = tracing::field::Empty)` to `fields(object_key = %object_key)`. Leave `MAX_INDEX_SNAPSHOT_BYTES` and the public `load` wrapper unchanged.)

- [ ] **Step 2: Rewrite `profile_index.rs::load_path_with_cap`**

In `crates/blockstore/src/profile_index.rs`, replace the body of `load_path_with_cap` with:

```rust
    async fn load_path_with_cap(
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        max_bytes: usize,
    ) -> Result<Self> {
        let bytes = crabka_object_store::read_capped(store, path, max_bytes as u64)
            .await
            .map_err(|e| match e {
                crabka_object_store::ObjectStoreError::TooLarge { size, max_bytes, .. } => {
                    BlockStoreError::InvalidBlock(format!(
                        "profile index snapshot `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
                    ))
                }
                other => BlockStoreError::ObjectStore(other.to_string()),
            })?;
        Ok(serde_json::from_slice(&bytes)?)
    }
```

(Change its `#[instrument(...)]` `fields(path = %path, size = tracing::field::Empty)` to `fields(path = %path)`. Leave `load_with_cap`, `MAX_PROFILE_INDEX_SNAPSHOT_BYTES`, and the public `load` wrapper unchanged.)

- [ ] **Step 3: Remove now-unused imports if the compiler flags them**

`head`/`get` may no longer be called directly in these two files (check `index.rs` / `profile_index.rs` for other `store.head(`/`store.get(` uses first — `index.rs` still uses `store.put` in its save path, and other index files are untouched). If `PutPayload`, `ObjectStoreExt`, or an `ObjectMeta` import becomes unused *in these two files only*, remove it. Do not touch imports used by remaining code.

- [ ] **Step 4: Run the blockstore suite (regression net)**

Run: `cargo test -p crabka-blockstore`
Expected: PASS — including any oversize test (the `InvalidBlock` message text is byte-identical to before) and the under-cap load/round-trip tests. `MAX_INDEX_SNAPSHOT_BYTES`/`MAX_PROFILE_INDEX_SNAPSHOT_BYTES` constant tests unchanged.

- [ ] **Step 5: Commit**

```bash
git add crates/blockstore/src/index.rs crates/blockstore/src/profile_index.rs
git commit -m "refactor(blockstore): use crabka-object-store read_capped for snapshot loads"
```

---

## Task 4: Final gate — format, lint, full test sweep

**Files:** none (formatting only).

- [ ] **Step 1: Format**

Run: `cargo +nightly fmt`
Expected: reformats if needed.

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

**1. Spec coverage (Ch. 0 — the `read_capped` follow-up deferred from the ObjectOps plan):**
- Shared capped-read helper → Task 2 (`read_capped` in the substrate). ✅
- Both blockstore duplicates adopt it → Task 3 (`index.rs::load_with_cap`, `profile_index.rs::load_path_with_cap`). ✅
- The `reader.rs` parquet-sizing `head()`s and `index_snapshot` op-routing are explicitly out of scope (Scope boundary), not silently skipped. ✅

**2. Placeholder scan:** No `TBD`/`TODO`/"handle errors". Every code step shows complete code; every run step shows command + expected result. The one conditional (Task 3 Step 3) names the exact compiler signal and the concrete action. ✅

**3. Type consistency:** `read_capped(&Arc<dyn object_store::ObjectStore>, &Path, u64) -> Result<Bytes, ObjectStoreError>` is defined in Task 2 and called identically in both Task 3 sites (`max_bytes as u64`). `ObjectStoreError::TooLarge { size, max_bytes, .. }` is defined in Task 1 and destructured identically in both `map_err` arms. The preserved `BlockStoreError::InvalidBlock(...)` message strings match the originals verbatim (`index snapshot` / `profile index snapshot`). ✅

**4. Invariant check:** Both sites keep `InvalidBlock` + identical message on oversize; under-cap returns the deserialized index; existing suite guards it (Invariant 1). The dropped `size` span field is called out as an accepted minor change (Invariant 2). `read_capped` uses only `object_store` + `bytes`, keeping the crate publishable (Invariant 3), operates on the raw handle, never `block_on`s (Invariant 4). Each task ends green (Invariant 5). ✅
