# crabka-blockstore Implementation Plan (Logs Wedge — Phase 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `crabka-blockstore` — the signal-agnostic columnar block store (Parquet blocks on object storage + a label/series/block index + a DataFusion query facade) that every observability signal in the LGTM+P replacement will reuse, proven end-to-end on logs.

**Architecture:** A block is a tenant-scoped, time-bounded Parquet file written to `object_store`, carrying two mandatory columns (`series_fingerprint: UInt64`, `timestamp: Int64` nanos) plus arbitrary signal payload columns, sorted by `(series_fingerprint, timestamp)`. An in-memory `Index` maps label matchers → series fingerprints → candidate block keys (pruning *before* any scan), and is snapshotted to object storage. A `BlockStore` facade resolves a query through the index, registers the surviving blocks as a DataFusion Parquet table, and hands back a `SessionContext` the caller (later: `crabka-logql`) runs its plan against. Block-level pruning is ours; intra-block row-group pruning + projection/predicate pushdown is delegated to DataFusion's native Parquet reader.

**Tech Stack:** Rust 2024 · `datafusion` (git `main`, pinned) · `arrow` 59 · `parquet` 59 · `object_store` 0.13 · `tokio` · `thiserror` · `serde_json` (index snapshot) · `regex` (matcher resolution). Tests: `assert2`, `proptest`, `tempfile`, `object_store::memory::InMemory`.

## Global Constraints

- **No backwards compatibility.** Crabka is greenfield/undeployed. No `#[serde(default)]` shims, no V2-alongside-V1, no migration code. Change schemas/enums/wire formats freely. (Only Kafka wire compat matters — and this crate touches none of it.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn` workspace-wide. New code must be clippy-pedantic clean (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` are allowed workspace-wide). Run `cargo clippy -p crabka-blockstore --all-targets` and fix warnings before each commit.
- **Formatting:** run `cargo fmt -p crabka-blockstore` before every commit. (Do NOT run `cargo +nightly fmt --all` — it fails with OS error 206 / path-too-long in deep worktrees on Windows; always scope with `-p`.)
- **Assertions:** use `assert2::assert!` / `assert2::check!` in tests (workspace convention), `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`. This `main` revision depends on arrow 59 / parquet 59 / object_store 0.13.2, which unify with the workspace's existing `arrow = "59"` / `object_store = "0.13"` pins (same major → cargo unifies to one crate instance, so types cross the DataFusion boundary cleanly). Do **not** substitute a released `datafusion` (54.x depends on arrow 58 and will pull a second, incompatible arrow major).
- **Arrow/parquet/object_store version identity:** because all of arrow/parquet/object_store unify to a single instance, you may import them directly (`use arrow::...`, `use parquet::...`, `use object_store::...`) as the `remote-storage` crate does. If a type-mismatch error ever appears at the DataFusion boundary, switch that import to DataFusion's re-export (`datafusion::arrow`, `datafusion::parquet`) to force identity.
- **Fingerprint algorithm:** FNV-1a 64-bit over the canonical label string (see Task 2). We deliberately do **not** match Loki's labels-hash — API compatibility does not require index-file interop, and owning the hash keeps us free of Loki's internal format. (Spec §12 open question — resolved here as "own scheme".)

---

## Phase roadmap (context — only Phase 1 is detailed here)

This plan is Phase 1 of the logs wedge. Each later phase gets its own plan when we reach it; the crate seams were chosen so they compose:

1. **`crabka-blockstore`** *(this plan)* — columnar block format + index + DataFusion query facade. Independently testable.
2. **`crabka-logql`** — LogQL parser + planner that lowers a LogQL query onto SQL/DataFusion over a `BlockStore::scan_context` table + a `WAL-tail` table. Depends on Phase 1.
3. **`crabka-observability` (ingest + compactor)** — distributor endpoints (Loki push / OTLP logs / Kafka produce) → WAL topic; compactor consumer-group → `BlockWriter` blocks + `Index` snapshots. Depends on Phase 1.
4. **`crabka-observability` (querier)** — Loki HTTP API surface, hot/cold merge, `tail` websocket; differential-vs-Loki + Grafana integration tests. Depends on Phases 1–3.

---

## File structure (`crates/blockstore/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest; workspace deps |
| `src/lib.rs` | module decls + public re-exports + crate docs |
| `src/error.rs` | `BlockStoreError` enum + `Result` mapping helpers |
| `src/labels.rs` | `Labels`, `SeriesFingerprint`, fingerprint hashing |
| `src/matcher.rs` | `MatchOp`, `LabelMatcher` |
| `src/block.rs` | column-name constants, `BlockMeta`, schema validation |
| `src/writer.rs` | `BlockWriter` — RecordBatches → Parquet → object_store |
| `src/reader.rs` | `read_block` — object_store Parquet → RecordBatches |
| `src/index.rs` | `Index` (series dict + postings + block index) + snapshot serde |
| `src/store.rs` | `BlockStore` facade — resolve → prune → DataFusion `SessionContext` |

Each file has one responsibility; `store.rs` is the only file that depends on DataFusion's query layer, isolating the churn-prone surface.

---

### Task 1: Crate scaffold + workspace dependency wiring

**Files:**
- Create: `crates/blockstore/Cargo.toml`
- Create: `crates/blockstore/src/lib.rs`
- Modify: `Cargo.toml` (root — add `datafusion`, `parquet`, `url` to `[workspace.dependencies]`)

**Interfaces:**
- Produces: a compiling `crabka-blockstore` crate with `pub fn crate_smoke() -> bool` (placeholder, removed in Task 2) so there is a test to run.

- [ ] **Step 1: Add the three new workspace dependencies**

In root `Cargo.toml`, under `[workspace.dependencies]`, add (place near the existing `arrow`/`object_store` lines):

```toml
# Observability block store (crabka-blockstore): DataFusion query engine over
# Parquet blocks on object storage. Pinned to apache/datafusion `main` because
# that revision tracks arrow 59 / parquet 59 / object_store 0.13.2 — matching the
# workspace pins. The latest *released* datafusion (54.x) is on arrow 58 and would
# pull a second, incompatible arrow major.
datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }
parquet = { version = "59", default-features = false, features = ["arrow", "async", "object_store"] }
url = "2"
```

- [ ] **Step 2: Create `crates/blockstore/Cargo.toml`**

```toml
[package]
name = "crabka-blockstore"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Signal-agnostic columnar block store (Parquet on object storage + label/series index + DataFusion query) for Crabka observability"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-blockstore"
readme = "README.md"
keywords = ["observability", "datafusion", "parquet", "object-store", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
arrow = { workspace = true }
parquet = { workspace = true }
datafusion = { workspace = true }
object_store = { workspace = true }
url = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread"] }
futures = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
regex = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Create `crates/blockstore/src/lib.rs` with a placeholder**

```rust
//! Signal-agnostic columnar block store for Crabka observability.
//!
//! A *block* is a tenant-scoped, time-bounded Parquet file on object storage
//! with two mandatory columns (`series_fingerprint`, `timestamp`) plus arbitrary
//! signal payload columns. An [`Index`] prunes a query to candidate blocks before
//! any scan; DataFusion handles intra-block row-group pruning and pushdown.

/// Placeholder so the crate has something to test before Task 2 lands real types.
#[must_use]
pub fn crate_smoke() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn smoke() {
        assert!(crate_smoke());
    }
}
```

- [ ] **Step 4: Build and test**

Run: `cargo test -p crabka-blockstore`
Expected: compiles (the first build fetches + compiles DataFusion from git — this is slow, several minutes, normal) and `smoke` PASSES.

If the build fails with an arrow major mismatch (`expected struct arrow::... found struct arrow::...`), the datafusion rev is wrong — re-confirm the pinned rev tracks arrow 59.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add Cargo.toml Cargo.lock crates/blockstore/
git commit -m "feat(blockstore): scaffold crabka-blockstore crate + DataFusion dep"
```

---

### Task 2: Core types — labels, fingerprint, matchers, block meta, error

**Files:**
- Create: `crates/blockstore/src/error.rs`
- Create: `crates/blockstore/src/labels.rs`
- Create: `crates/blockstore/src/matcher.rs`
- Create: `crates/blockstore/src/block.rs`
- Modify: `crates/blockstore/src/lib.rs` (declare modules, re-export, remove placeholder)

**Interfaces:**
- Produces:
  - `BlockStoreError` (enum) + `pub type Result<T> = std::result::Result<T, BlockStoreError>`
  - `type SeriesFingerprint = u64`
  - `Labels` with `new()`, `insert(name, value)`, `get(&str) -> Option<&str>`, `iter()`, `len()`, `is_empty()`, `fingerprint() -> SeriesFingerprint`, `FromIterator<(String, String)>`, `Serialize`/`Deserialize`, `Clone`, `Debug`, `PartialEq`, `Eq`, `Default`
  - `enum MatchOp { Eq, Neq, Re, Nre }` (`Copy`)
  - `struct LabelMatcher { pub name: String, pub op: MatchOp, pub value: String }`
  - `const COL_FINGERPRINT: &str = "series_fingerprint"`, `const COL_TIMESTAMP: &str = "timestamp"`
  - `struct BlockMeta { pub tenant: String, pub object_key: String, pub min_ts: i64, pub max_ts: i64, pub row_count: usize, pub fingerprints: Vec<SeriesFingerprint> }` (`Serialize`/`Deserialize`, `Clone`, `Debug`, `PartialEq`, `Eq`)
  - `fn validate_block_schema(schema: &arrow::datatypes::Schema) -> Result<()>`

- [ ] **Step 1: Write the failing test for `labels.rs`**

Create `crates/blockstore/src/labels.rs` with only the tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn fingerprint_is_order_independent() {
        let mut a = Labels::new();
        a.insert("app", "api");
        a.insert("env", "prod");
        let mut b = Labels::new();
        b.insert("env", "prod");
        b.insert("app", "api");
        assert!(a.fingerprint() == b.fingerprint());
    }

    #[test]
    fn fingerprint_distinguishes_values() {
        let mut a = Labels::new();
        a.insert("app", "api");
        let mut b = Labels::new();
        b.insert("app", "web");
        assert!(a.fingerprint() != b.fingerprint());
    }

    #[test]
    fn get_and_iter_round_trip() {
        let mut l = Labels::new();
        l.insert("app", "api");
        assert!(l.get("app") == Some("api"));
        assert!(l.get("missing") == None);
        assert!(l.len() == 1);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib labels`
Expected: FAIL — `cannot find type Labels in this scope`.

- [ ] **Step 3: Implement `labels.rs`**

Prepend above the `tests` module:

```rust
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 64-bit fingerprint of a label set. Stable across process runs.
pub type SeriesFingerprint = u64;

/// An ordered set of `name -> value` labels identifying a series.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Labels(BTreeMap<String, String>);

impl Labels {
    #[must_use]
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// FNV-1a 64-bit hash over the canonical `name=value\n` form (BTreeMap keeps
    /// names sorted, so the hash is order-independent). Crabka-owned; not
    /// Loki-compatible (intentional — see plan Global Constraints).
    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut hash = OFFSET;
        let mut hash_bytes = |bytes: &[u8]| {
            for &b in bytes {
                hash ^= u64::from(b);
                hash = hash.wrapping_mul(PRIME);
            }
        };
        for (name, value) in &self.0 {
            hash_bytes(name.as_bytes());
            hash_bytes(b"=");
            hash_bytes(value.as_bytes());
            hash_bytes(b"\n");
        }
        hash
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib labels`
Expected: PASS (3 tests).

- [ ] **Step 5: Implement `error.rs`**

```rust
//! Error type for the block store.

/// Errors raised by the block store. Backend errors are stringified (matching
/// the `crabka-remote-storage` convention) rather than wrapping foreign types.
#[derive(Debug, thiserror::Error)]
pub enum BlockStoreError {
    #[error("object store error: {0}")]
    ObjectStore(String),

    #[error("parquet error: {0}")]
    Parquet(String),

    #[error("datafusion error: {0}")]
    DataFusion(String),

    #[error("invalid block: {0}")]
    InvalidBlock(String),

    #[error("index snapshot serialization error: {0}")]
    Serde(String),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, BlockStoreError>;

impl From<object_store::Error> for BlockStoreError {
    fn from(e: object_store::Error) -> Self {
        Self::ObjectStore(e.to_string())
    }
}

impl From<parquet::errors::ParquetError> for BlockStoreError {
    fn from(e: parquet::errors::ParquetError) -> Self {
        Self::Parquet(e.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for BlockStoreError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(e.to_string())
    }
}

impl From<serde_json::Error> for BlockStoreError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serde(e.to_string())
    }
}
```

- [ ] **Step 6: Implement `matcher.rs`**

```rust
//! Label matchers (mirrors the LogQL/PromQL matcher operators).

use serde::{Deserialize, Serialize};

/// Matcher operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchOp {
    /// `name="value"`
    Eq,
    /// `name!="value"`
    Neq,
    /// `name=~"regex"`
    Re,
    /// `name!~"regex"`
    Nre,
}

/// A single label matcher.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

impl LabelMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: MatchOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }
}
```

- [ ] **Step 7: Write the failing test for `block.rs`**

Create `crates/blockstore/src/block.rs` with the tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::assert;

    #[test]
    fn schema_with_required_columns_is_valid() {
        let schema = Schema::new(vec![
            Field::new(COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]);
        assert!(validate_block_schema(&schema).is_ok());
    }

    #[test]
    fn schema_missing_fingerprint_is_rejected() {
        let schema = Schema::new(vec![Field::new(COL_TIMESTAMP, DataType::Int64, false)]);
        assert!(validate_block_schema(&schema).is_err());
    }

    #[test]
    fn schema_with_wrong_timestamp_type_is_rejected() {
        let schema = Schema::new(vec![
            Field::new(COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(COL_TIMESTAMP, DataType::Utf8, false),
        ]);
        assert!(validate_block_schema(&schema).is_err());
    }
}
```

- [ ] **Step 8: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib block`
Expected: FAIL — `cannot find function validate_block_schema`.

- [ ] **Step 9: Implement `block.rs`**

Prepend above the `tests` module:

```rust
//! Block column conventions and per-block metadata.

use arrow::datatypes::{DataType, Schema};
use serde::{Deserialize, Serialize};

use crate::error::{BlockStoreError, Result};
use crate::labels::SeriesFingerprint;

/// Mandatory column: the series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";
/// Mandatory column: the event timestamp in nanoseconds (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

/// Metadata recorded for each written block; the [`crate::Index`] is built from
/// these. `object_key` is the `object_store` path the block was written to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub tenant: String,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub fingerprints: Vec<SeriesFingerprint>,
}

/// Validate that an Arrow schema carries the two mandatory columns with the
/// correct types. Payload columns (e.g. `line`) are unconstrained.
pub fn validate_block_schema(schema: &Schema) -> Result<()> {
    let fp = schema.column_with_name(COL_FINGERPRINT).ok_or_else(|| {
        BlockStoreError::InvalidBlock(format!("missing `{COL_FINGERPRINT}` column"))
    })?;
    if fp.1.data_type() != &DataType::UInt64 {
        return Err(BlockStoreError::InvalidBlock(format!(
            "`{COL_FINGERPRINT}` must be UInt64, got {:?}",
            fp.1.data_type()
        )));
    }
    let ts = schema.column_with_name(COL_TIMESTAMP).ok_or_else(|| {
        BlockStoreError::InvalidBlock(format!("missing `{COL_TIMESTAMP}` column"))
    })?;
    if ts.1.data_type() != &DataType::Int64 {
        return Err(BlockStoreError::InvalidBlock(format!(
            "`{COL_TIMESTAMP}` must be Int64, got {:?}",
            ts.1.data_type()
        )));
    }
    Ok(())
}
```

- [ ] **Step 10: Replace `lib.rs` to wire modules and re-export**

```rust
//! Signal-agnostic columnar block store for Crabka observability.
//!
//! A *block* is a tenant-scoped, time-bounded Parquet file on object storage
//! with two mandatory columns (`series_fingerprint`, `timestamp`) plus arbitrary
//! signal payload columns. An [`Index`] prunes a query to candidate blocks before
//! any scan; DataFusion handles intra-block row-group pruning and pushdown.

mod block;
mod error;
mod labels;
mod matcher;

pub use block::{BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_block_schema};
pub use error::{BlockStoreError, Result};
pub use labels::{Labels, SeriesFingerprint};
pub use matcher::{LabelMatcher, MatchOp};
```

- [ ] **Step 11: Run the full crate test suite**

Run: `cargo test -p crabka-blockstore`
Expected: PASS (labels + block tests; the old `smoke` test is gone).

- [ ] **Step 12: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): core types — labels, fingerprint, matchers, block meta, error"
```

---

### Task 3: `BlockWriter` — RecordBatches → Parquet → object_store

**Files:**
- Create: `crates/blockstore/src/writer.rs`
- Modify: `crates/blockstore/src/lib.rs` (declare + re-export)

**Interfaces:**
- Consumes: `validate_block_schema`, `BlockMeta`, `COL_FINGERPRINT`, `COL_TIMESTAMP`, `SeriesFingerprint`, `Result`.
- Produces:
  - `struct BlockWriter { /* store */ }` with `pub fn new(store: Arc<dyn object_store::ObjectStore>) -> Self`
  - `pub async fn write_block(&self, tenant: &str, object_key: &str, schema: arrow::datatypes::SchemaRef, batches: &[arrow::record_batch::RecordBatch]) -> Result<BlockMeta>` — writes a Parquet block at `object_key`; idempotent (same key overwrites identical content); computes `min_ts`/`max_ts`/`row_count`/distinct `fingerprints` by scanning the two mandatory columns.

- [ ] **Step 1: Write the failing test**

Create `crates/blockstore/src/writer.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::*;

    fn log_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    fn sample_batch(schema: &Arc<Schema>) -> RecordBatch {
        // Two series (fp 10, 20), timestamps 100..400, sorted by (fp, ts).
        let fp = UInt64Array::from(vec![10_u64, 10, 20, 20]);
        let ts = Int64Array::from(vec![100_i64, 200, 300, 400]);
        let line = StringArray::from(vec!["a", "b", "c", "d"]);
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(fp), Arc::new(ts), Arc::new(line)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_block_persists_object_and_returns_meta() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let schema = log_schema();
        let batch = sample_batch(&schema);

        let meta = writer
            .write_block("tenant-a", "blocks/tenant-a/b1.parquet", schema, &[batch])
            .await
            .unwrap();

        assert!(meta.tenant == "tenant-a");
        assert!(meta.object_key == "blocks/tenant-a/b1.parquet");
        assert!(meta.min_ts == 100);
        assert!(meta.max_ts == 400);
        assert!(meta.row_count == 4);
        let mut fps = meta.fingerprints.clone();
        fps.sort_unstable();
        assert!(fps == vec![10_u64, 20]);

        // The object actually exists in the store.
        let head = store.head(&Path::from("blocks/tenant-a/b1.parquet")).await;
        assert!(head.is_ok());
    }

    #[tokio::test]
    async fn write_block_rejects_schema_without_mandatory_columns() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store);
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();

        let err = writer.write_block("t", "k.parquet", schema, &[batch]).await;
        assert!(err.is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib writer`
Expected: FAIL — `cannot find type BlockWriter`.

- [ ] **Step 3: Implement `writer.rs`**

Prepend above the `tests` module:

```rust
//! Writes columnar blocks to object storage as Parquet.

use std::sync::Arc;

use arrow::array::{Array, Int64Array, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::ParquetObjectWriter;

use crate::block::{BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_block_schema};
use crate::error::{BlockStoreError, Result};
use crate::labels::SeriesFingerprint;

/// Writes Parquet blocks to an object store.
pub struct BlockWriter {
    store: Arc<dyn ObjectStore>,
}

impl BlockWriter {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Write `batches` (which must all match `schema` and be sorted by
    /// `(series_fingerprint, timestamp)`) as a single Parquet block at
    /// `object_key`. Returns the [`BlockMeta`] describing it.
    ///
    /// Idempotent: writing the same `object_key` with identical content
    /// overwrites it with identical bytes (the compactor relies on this).
    pub async fn write_block(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<BlockMeta> {
        validate_block_schema(&schema)?;

        let (min_ts, max_ts, row_count, fingerprints) = summarize(batches)?;

        let path = Path::from(object_key);
        let object_writer = ParquetObjectWriter::new(self.store.clone(), path);
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, None)?;
        for batch in batches {
            writer.write(batch).await?;
        }
        writer.close().await?;

        Ok(BlockMeta {
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            min_ts,
            max_ts,
            row_count,
            fingerprints,
        })
    }
}

/// Scan the mandatory columns to compute the block's time bounds, row count,
/// and the distinct set of series fingerprints it contains.
fn summarize(batches: &[RecordBatch]) -> Result<(i64, i64, usize, Vec<SeriesFingerprint>)> {
    use std::collections::BTreeSet;

    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut row_count = 0_usize;
    let mut fps: BTreeSet<SeriesFingerprint> = BTreeSet::new();

    for batch in batches {
        row_count += batch.num_rows();

        let ts = batch
            .column_by_name(COL_TIMESTAMP)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing timestamp column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlockStoreError::InvalidBlock("timestamp not Int64".into()))?;
        let fp = batch
            .column_by_name(COL_FINGERPRINT)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing fingerprint column".into()))?
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| BlockStoreError::InvalidBlock("fingerprint not UInt64".into()))?;

        for i in 0..batch.num_rows() {
            if !ts.is_null(i) {
                let v = ts.value(i);
                min_ts = min_ts.min(v);
                max_ts = max_ts.max(v);
            }
            if !fp.is_null(i) {
                fps.insert(fp.value(i));
            }
        }
    }

    if row_count == 0 {
        return Err(BlockStoreError::InvalidBlock("empty block".into()));
    }

    Ok((min_ts, max_ts, row_count, fps.into_iter().collect()))
}
```

- [ ] **Step 4: Add module to `lib.rs`**

Add `mod writer;` (after `mod reader;` placeholder is not yet present — just add `mod writer;`) and `pub use writer::BlockWriter;` to the re-export block.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib writer`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): BlockWriter — RecordBatches to Parquet on object_store"
```

---

### Task 4: `read_block` — object_store Parquet → RecordBatches (round-trip)

**Files:**
- Create: `crates/blockstore/src/reader.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `Result`.
- Produces: `pub async fn read_block(store: Arc<dyn object_store::ObjectStore>, object_key: &str) -> Result<Vec<arrow::record_batch::RecordBatch>>`.

- [ ] **Step 1: Write the failing round-trip test**

Create `crates/blockstore/src/reader.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;
    use crate::writer::BlockWriter;

    #[tokio::test]
    async fn write_then_read_round_trips_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![10_u64, 20])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        BlockWriter::new(store.clone())
            .write_block("t", "b.parquet", schema, &[batch])
            .await
            .unwrap();

        let out = read_block(store, "b.parquet").await.unwrap();
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 2);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib reader`
Expected: FAIL — `cannot find function read_block`.

- [ ] **Step 3: Implement `reader.rs`**

Prepend above the `tests` module:

```rust
//! Reads a Parquet block back from object storage into Arrow RecordBatches.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;

use crate::error::Result;

/// Read every RecordBatch from the Parquet block at `object_key`.
///
/// Used by tests and by tooling; the query path does *not* go through this — it
/// hands the block to DataFusion, which streams and prunes it. Kept simple.
pub async fn read_block(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = store.head(&path).await?;
    let reader = ParquetObjectReader::new(store, path).with_file_size(meta.size);
    let stream = ParquetRecordBatchStreamBuilder::new(reader).await?.build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}
```

> **Note on `meta.size`:** in `object_store` 0.13 `ObjectMeta::size` is `u64` and `ParquetObjectReader::with_file_size` takes `u64`. If a future bump changes the type, adjust the cast. Verify against the resolved `object_store` version in `Cargo.lock`.

- [ ] **Step 4: Add module to `lib.rs`**

Add `mod reader;` and `pub use reader::read_block;`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib reader`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): read_block round-trip reader"
```

---

### Task 5: `Index` — series dictionary, postings, matcher resolution, label APIs

**Files:**
- Create: `crates/blockstore/src/index.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `Labels`, `SeriesFingerprint`, `LabelMatcher`, `MatchOp`, `BlockMeta`, `Result`.
- Produces:
  - `struct Index` (`Default`, `Serialize`, `Deserialize`)
  - `pub fn new() -> Self`
  - `pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels)`
  - `pub fn add_block(&mut self, meta: &BlockMeta)`
  - `pub fn resolve(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<std::collections::BTreeSet<SeriesFingerprint>>`
  - `pub fn candidate_blocks(&self, tenant: &str, fps: &std::collections::BTreeSet<SeriesFingerprint>, min_ts: i64, max_ts: i64) -> Vec<String>`
  - `pub fn label_names(&self, tenant: &str) -> Vec<String>`
  - `pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String>`

- [ ] **Step 1: Write the failing tests**

Create `crates/blockstore/src/index.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;

    use super::*;
    use crate::block::BlockMeta;
    use crate::labels::Labels;
    use crate::matcher::{LabelMatcher, MatchOp};

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    fn seed() -> Index {
        let mut idx = Index::new();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]);
        let api_dev = labels(&[("app", "api"), ("env", "dev")]);
        let web_prod = labels(&[("app", "web"), ("env", "prod")]);
        idx.add_series("t", api_prod.fingerprint(), &api_prod);
        idx.add_series("t", api_dev.fingerprint(), &api_dev);
        idx.add_series("t", web_prod.fingerprint(), &web_prod);
        idx
    }

    #[test]
    fn resolve_eq_intersection() {
        let idx = seed();
        let want = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Eq, "prod"),
                ],
            )
            .unwrap();
        assert!(got == BTreeSet::from([want]));
    }

    #[test]
    fn resolve_neq_excludes() {
        let idx = seed();
        let got = idx
            .resolve(
                "t",
                &[
                    LabelMatcher::new("app", MatchOp::Eq, "api"),
                    LabelMatcher::new("env", MatchOp::Neq, "prod"),
                ],
            )
            .unwrap();
        let want = labels(&[("app", "api"), ("env", "dev")]).fingerprint();
        assert!(got == BTreeSet::from([want]));
    }

    #[test]
    fn resolve_regex_union() {
        let idx = seed();
        let got = idx
            .resolve("t", &[LabelMatcher::new("env", MatchOp::Re, "pro.*")])
            .unwrap();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        assert!(got == BTreeSet::from([api_prod, web_prod]));
    }

    #[test]
    fn resolve_unknown_tenant_is_empty() {
        let idx = seed();
        let got = idx
            .resolve("nope", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn candidate_blocks_prune_by_fp_and_time() {
        let mut idx = seed();
        let api_prod = labels(&[("app", "api"), ("env", "prod")]).fingerprint();
        let web_prod = labels(&[("app", "web"), ("env", "prod")]).fingerprint();
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b1.parquet".into(),
            min_ts: 0,
            max_ts: 100,
            row_count: 1,
            fingerprints: vec![api_prod],
        });
        idx.add_block(&BlockMeta {
            tenant: "t".into(),
            object_key: "b2.parquet".into(),
            min_ts: 200,
            max_ts: 300,
            row_count: 1,
            fingerprints: vec![web_prod],
        });

        // Want api_prod over [0,150] → only b1.
        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 0, 150);
        assert!(got == vec!["b1.parquet".to_string()]);

        // Time window misses b1 entirely.
        let got = idx.candidate_blocks("t", &BTreeSet::from([api_prod]), 500, 600);
        assert!(got.is_empty());
    }

    #[test]
    fn label_names_and_values() {
        let idx = seed();
        let mut names = idx.label_names("t");
        names.sort();
        assert!(names == vec!["app".to_string(), "env".to_string()]);
        let mut envs = idx.label_values("t", "env");
        envs.sort();
        assert!(envs == vec!["dev".to_string(), "prod".to_string()]);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib index`
Expected: FAIL — `cannot find type Index`.

- [ ] **Step 3: Implement `index.rs`**

Prepend above the `tests` module:

```rust
//! In-memory label/series/block index. Prunes a query to candidate blocks
//! before any Parquet scan. Snapshotted to object storage (see `store.rs`).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::error::{BlockStoreError, Result};
use crate::labels::{Labels, SeriesFingerprint};
use crate::matcher::{LabelMatcher, MatchOp};

/// One block's pruning footprint.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BlockEntry {
    object_key: String,
    min_ts: i64,
    max_ts: i64,
    fingerprints: BTreeSet<SeriesFingerprint>,
}

/// Per-tenant index.
#[derive(Default, Serialize, Deserialize)]
struct TenantIndex {
    /// fingerprint -> labels (series dictionary)
    series: HashMap<SeriesFingerprint, Labels>,
    /// (label_name, label_value) -> fingerprints (inverted postings)
    postings: HashMap<(String, String), BTreeSet<SeriesFingerprint>>,
    /// label_name -> distinct values seen
    values: HashMap<String, BTreeSet<String>>,
    blocks: Vec<BlockEntry>,
}

/// Multi-tenant index.
#[derive(Default, Serialize, Deserialize)]
pub struct Index {
    tenants: HashMap<String, TenantIndex>,
}

impl Index {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a series (idempotent for a given fingerprint).
    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        let t = self.tenants.entry(tenant.to_string()).or_default();
        if t.series.contains_key(&fp) {
            return;
        }
        t.series.insert(fp, labels.clone());
        for (name, value) in labels.iter() {
            t.postings
                .entry((name.clone(), value.clone()))
                .or_default()
                .insert(fp);
            t.values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    /// Register a written block.
    pub fn add_block(&mut self, meta: &BlockMeta) {
        let t = self.tenants.entry(meta.tenant.clone()).or_default();
        t.blocks.push(BlockEntry {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            fingerprints: meta.fingerprints.iter().copied().collect(),
        });
    }

    /// Resolve matchers to the set of matching series fingerprints (intersection
    /// across matchers). An empty matcher list is an error — LogQL requires at
    /// least one matcher that selects a non-empty set.
    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        if matchers.is_empty() {
            return Err(BlockStoreError::InvalidBlock(
                "at least one label matcher is required".into(),
            ));
        }
        let Some(t) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };

        let mut acc: Option<BTreeSet<SeriesFingerprint>> = None;
        for m in matchers {
            let matched = t.match_one(m)?;
            acc = Some(match acc {
                None => matched,
                Some(prev) => prev.intersection(&matched).copied().collect(),
            });
            if acc.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        Ok(acc.unwrap_or_default())
    }

    /// Candidate block keys for a tenant: blocks whose time range overlaps
    /// `[min_ts, max_ts]` AND that contain at least one of `fps`.
    #[must_use]
    pub fn candidate_blocks(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .filter(|b| b.fingerprints.iter().any(|fp| fps.contains(fp)))
            .map(|b| b.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .map(|t| t.values.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .and_then(|t| t.values.get(name))
            .map(|vs| vs.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl TenantIndex {
    /// Fingerprints matching a single matcher.
    fn match_one(&self, m: &LabelMatcher) -> Result<BTreeSet<SeriesFingerprint>> {
        match m.op {
            MatchOp::Eq => Ok(self
                .postings
                .get(&(m.name.clone(), m.value.clone()))
                .cloned()
                .unwrap_or_default()),
            MatchOp::Neq => {
                let excluded = self
                    .postings
                    .get(&(m.name.clone(), m.value.clone()))
                    .cloned()
                    .unwrap_or_default();
                Ok(self
                    .series
                    .keys()
                    .copied()
                    .filter(|fp| !excluded.contains(fp))
                    .collect())
            }
            MatchOp::Re | MatchOp::Nre => {
                let re = regex::Regex::new(&anchored(&m.value))
                    .map_err(|e| BlockStoreError::InvalidBlock(format!("bad regex: {e}")))?;
                let mut matched: BTreeSet<SeriesFingerprint> = BTreeSet::new();
                let mut seen_name = false;
                for ((name, value), fps) in &self.postings {
                    if name != &m.name {
                        continue;
                    }
                    seen_name = true;
                    if re.is_match(value) {
                        matched.extend(fps.iter().copied());
                    }
                }
                let _ = seen_name;
                if m.op == MatchOp::Re {
                    Ok(matched)
                } else {
                    Ok(self
                        .series
                        .keys()
                        .copied()
                        .filter(|fp| !matched.contains(fp))
                        .collect())
                }
            }
        }
    }
}

/// LogQL/PromQL regex matchers are fully anchored (`^(?:...)$`).
fn anchored(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

// Silence an unused-import lint if HashSet ends up unused after edits.
const _: fn() = || {
    let _: Option<HashSet<()>> = None;
    let _: Option<BTreeMap<(), ()>> = None;
};
```

> Remove the trailing `const _` guard if `HashSet`/`BTreeMap` are genuinely unused — it exists only so the first compile of this file doesn't fail on an unused import you may or may not need after clippy. Prefer deleting unused imports outright.

- [ ] **Step 4: Add module to `lib.rs`**

Add `mod index;` and `pub use index::Index;`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib index`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): Index — matcher resolution + block pruning + label APIs"
```

---

### Task 6: Index snapshot — serialize/deserialize via object_store

**Files:**
- Modify: `crates/blockstore/src/index.rs` (add `save`/`load`)

**Interfaces:**
- Produces (on `Index`):
  - `pub async fn save(&self, store: &Arc<dyn object_store::ObjectStore>, object_key: &str) -> Result<()>`
  - `pub async fn load(store: &Arc<dyn object_store::ObjectStore>, object_key: &str) -> Result<Index>`

- [ ] **Step 1: Write the failing test**

Append to the `tests` module in `index.rs`:

```rust
    #[tokio::test]
    async fn snapshot_round_trips() {
        use std::sync::Arc;

        use object_store::ObjectStore;
        use object_store::memory::InMemory;

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/snapshot.json").await.unwrap();

        let loaded = Index::load(&store, "index/snapshot.json").await.unwrap();
        let got = loaded
            .resolve("t", &[LabelMatcher::new("app", MatchOp::Eq, "api")])
            .unwrap();
        assert!(got.len() == 2);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib index::tests::snapshot_round_trips`
Expected: FAIL — `no function or associated item named save`.

- [ ] **Step 3: Implement `save`/`load`**

Add to the `impl Index` block:

```rust
    /// Persist the index as a JSON snapshot to object storage.
    pub async fn save(
        &self,
        store: &std::sync::Arc<dyn object_store::ObjectStore>,
        object_key: &str,
    ) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        let path = object_store::path::Path::from(object_key);
        store
            .put(&path, object_store::PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    /// Load an index JSON snapshot from object storage.
    pub async fn load(
        store: &std::sync::Arc<dyn object_store::ObjectStore>,
        object_key: &str,
    ) -> Result<Index> {
        let path = object_store::path::Path::from(object_key);
        let bytes = store.get(&path).await?.bytes().await?;
        let idx = serde_json::from_slice(&bytes)?;
        Ok(idx)
    }
```

> JSON is chosen for inspectability; the snapshot is internal (no Loki interop), so the format is free to change. If snapshots grow large, swap `serde_json` for `serde-wincode` (already a workspace dep) — no external compatibility constraint.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib index`
Expected: PASS (7 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): Index object-storage snapshot save/load"
```

---

### Task 7: `BlockStore` facade — resolve → prune → DataFusion `SessionContext`

**Files:**
- Create: `crates/blockstore/src/store.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `Index`, `BlockWriter`, `LabelMatcher`, `Result`, `BlockStoreError`.
- Produces:
  - `struct BlockStore { /* store, base url, index */ }`
  - `pub fn new(store: Arc<dyn object_store::ObjectStore>, base: url::Url) -> Self`
  - `pub fn writer(&self) -> BlockWriter`
  - `pub fn index(&self) -> &Index` / `pub fn index_mut(&mut self) -> &mut Index`
  - `pub async fn scan_context(&self, tenant: &str, matchers: &[LabelMatcher], min_ts: i64, max_ts: i64, schema: arrow::datatypes::SchemaRef) -> Result<(datafusion::prelude::SessionContext, String)>` — returns a `SessionContext` with the candidate blocks registered as a table (name returned as the `String`). When no blocks match, registers an empty table with `schema` so the caller still gets the right column shape.

This is the one task whose DataFusion wiring (`register_object_store`, `read_parquet`, `DataFrame::into_view`) churns between versions. **The test below pins the behavior.** If a call signature differs at the pinned rev, adapt it against the DataFusion 54/`main` docs and the `datafusion-examples/examples/parquet_index.rs` example at the same rev — do not change the test's asserted behavior.

- [ ] **Step 1: Write the failing end-to-end test**

Create `crates/blockstore/src/store.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use datafusion::arrow::array::AsArray;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    use super::*;
    use crate::labels::Labels;
    use crate::matcher::{LabelMatcher, MatchOp};

    fn log_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(crate::COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(crate::COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]))
    }

    async fn seeded_store() -> (BlockStore, SchemaRef) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut bs = BlockStore::new(object_store, base);
        let schema = log_schema();

        let mut api = Labels::new();
        api.insert("app", "api");
        let fp = api.fingerprint();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![fp, fp])),
                Arc::new(Int64Array::from(vec![100_i64, 200])),
                Arc::new(StringArray::from(vec!["hello", "world"])),
            ],
        )
        .unwrap();

        let meta = bs
            .writer()
            .write_block("t", "blocks/b1.parquet", schema.clone(), &[batch])
            .await
            .unwrap();
        bs.index_mut().add_series("t", fp, &api);
        bs.index_mut().add_block(&meta);
        (bs, schema)
    }

    #[tokio::test]
    async fn scan_returns_rows_for_matching_series() {
        let (bs, schema) = seeded_store().await;
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "api")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();

        let df = ctx
            .sql(&format!("SELECT line FROM {table} ORDER BY timestamp"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 2);

        let first = batches[0].column(0).as_string::<i32>().value(0);
        assert!(first == "hello");
    }

    #[tokio::test]
    async fn scan_with_no_matching_blocks_returns_empty_shape() {
        let (bs, schema) = seeded_store().await;
        // Matches no series.
        let matchers = [LabelMatcher::new("app", MatchOp::Eq, "absent")];

        let (ctx, table) = bs
            .scan_context("t", &matchers, 0, 1_000, schema)
            .await
            .unwrap();
        let df = ctx.sql(&format!("SELECT line FROM {table}")).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 0);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib store`
Expected: FAIL — `cannot find type BlockStore`.

- [ ] **Step 3: Implement `store.rs`**

Prepend above the `tests` module. The `scan_context` body uses three DataFusion entry points — `SessionContext::register_object_store`, `SessionContext::read_parquet`, and `DataFrame::into_view` — plus `MemTable` for the empty case:

```rust
//! The query facade: resolve a query through the index, register the surviving
//! blocks as a DataFusion table, and return a `SessionContext` the caller runs
//! its plan against.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::ObjectStore;
use url::Url;

use crate::error::Result;
use crate::index::Index;
use crate::matcher::LabelMatcher;
use crate::writer::BlockWriter;

/// Default registered table name returned by [`BlockStore::scan_context`].
const TABLE_NAME: &str = "logs";

/// Owns the object store + its base URL + the in-memory index.
pub struct BlockStore {
    store: Arc<dyn ObjectStore>,
    base: Url,
    index: Index,
}

impl BlockStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, base: Url) -> Self {
        Self {
            store,
            base,
            index: Index::new(),
        }
    }

    #[must_use]
    pub fn writer(&self) -> BlockWriter {
        BlockWriter::new(self.store.clone())
    }

    #[must_use]
    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut Index {
        &mut self.index
    }

    /// Resolve `matchers` to candidate blocks via the index, register those
    /// Parquet blocks as a DataFusion table, and return `(ctx, table_name)`.
    ///
    /// The caller appends its own `WHERE` (fingerprint / timestamp / line
    /// filters) — this facade only does block-level pruning. Intra-block
    /// row-group pruning + projection pushdown is DataFusion's job.
    pub async fn scan_context(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let fps = self.index.resolve(tenant, matchers)?;
        let keys = self.index.candidate_blocks(tenant, &fps, min_ts, max_ts);

        let ctx = SessionContext::new();
        ctx.register_object_store(&self.base, self.store.clone());

        if keys.is_empty() {
            // No data — register an empty table so the column shape is correct.
            let empty = MemTable::try_new(schema, vec![])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok((ctx, TABLE_NAME.to_string()));
        }

        // Build absolute URLs for each candidate block under the base URL.
        let paths: Vec<String> = keys
            .iter()
            .map(|k| format!("{}{}", self.base, k.trim_start_matches('/')))
            .collect();

        let df = ctx
            .read_parquet(paths, ParquetReadOptions::default())
            .await?;
        ctx.register_table(TABLE_NAME, df.into_view())?;
        Ok((ctx, TABLE_NAME.to_string()))
    }
}
```

> **Churn-point checklist (verify against the pinned datafusion rev if compile fails):**
> - `SessionContext::register_object_store(&self, &Url, Arc<dyn ObjectStore>)` — present in DF 54/main.
> - `SessionContext::read_parquet(impl IntoIterator<Item = String>, ParquetReadOptions) -> Result<DataFrame>` — accepts multiple explicit file paths.
> - `DataFrame::into_view(self) -> Arc<dyn TableProvider>`.
> - `MemTable::try_new(SchemaRef, Vec<Vec<RecordBatch>>)` lives at `datafusion::catalog::MemTable` (older paths: `datafusion::datasource::MemTable`).
> - URL join: `memory:///` + `blocks/b1.parquet` must yield a `ListingTableUrl` the registered store resolves. If `read_parquet` rejects the joined form, build paths with `ListingTableUrl::parse` and pass those instead.

- [ ] **Step 4: Add module to `lib.rs`**

Add `mod store;` and `pub use store::BlockStore;`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib store`
Expected: PASS (2 tests).

- [ ] **Step 6: Run the whole suite + clippy**

Run: `cargo test -p crabka-blockstore && cargo clippy -p crabka-blockstore --all-targets`
Expected: all tests PASS; no clippy warnings.

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-blockstore
git add crates/blockstore/
git commit -m "feat(blockstore): BlockStore facade — index-pruned DataFusion scan context"
```

---

### Task 8: Property test — block round-trip preserves all rows (whole-crate integration)

**Files:**
- Create: `crates/blockstore/tests/roundtrip_proptest.rs`

**Interfaces:**
- Consumes the public API: `BlockStore`, `BlockWriter`, `Labels`, `LabelMatcher`, `MatchOp`, `COL_FINGERPRINT`, `COL_TIMESTAMP`.

- [ ] **Step 1: Write the property test**

Create `crates/blockstore/tests/roundtrip_proptest.rs`:

```rust
//! Property: any set of (fingerprint, timestamp, line) rows written as a block
//! and queried back by an equality matcher returns exactly the rows whose
//! fingerprint matches, within the time window.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{BlockStore, COL_FINGERPRINT, COL_TIMESTAMP, LabelMatcher, Labels, MatchOp};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use proptest::prelude::*;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("line", DataType::Utf8, true),
    ]))
}

/// A row keyed by which of two apps (`api`/`web`) produced it.
fn arb_rows() -> impl Strategy<Value = Vec<(bool, i64, String)>> {
    proptest::collection::vec(
        (any::<bool>(), 0_i64..1_000_i64, "[a-z]{1,8}"),
        1..40,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn equality_matcher_returns_only_matching_series(rows in arb_rows()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let mut api = Labels::new();
            api.insert("app", "api");
            let api_fp = api.fingerprint();
            let mut web = Labels::new();
            web.insert("app", "web");
            let web_fp = web.fingerprint();

            // Sort rows by (fp, ts) as the writer requires.
            let mut sorted: Vec<(u64, i64, String)> = rows
                .iter()
                .map(|(is_api, ts, line)| {
                    (if *is_api { api_fp } else { web_fp }, *ts, line.clone())
                })
                .collect();
            sorted.sort_by_key(|(fp, ts, _)| (*fp, *ts));

            let expected_api: BTreeMap<i64, usize> = sorted
                .iter()
                .filter(|(fp, _, _)| *fp == api_fp)
                .fold(BTreeMap::new(), |mut m, (_, ts, _)| {
                    *m.entry(*ts).or_default() += 1;
                    m
                });
            let expected_api_count: usize = expected_api.values().sum();

            let fps = UInt64Array::from(sorted.iter().map(|(fp, _, _)| *fp).collect::<Vec<_>>());
            let tss = Int64Array::from(sorted.iter().map(|(_, ts, _)| *ts).collect::<Vec<_>>());
            let lines = StringArray::from(
                sorted.iter().map(|(_, _, l)| l.as_str()).collect::<Vec<_>>(),
            );
            let batch = RecordBatch::try_new(
                schema(),
                vec![Arc::new(fps), Arc::new(tss), Arc::new(lines)],
            )
            .unwrap();

            let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let base = url::Url::parse("memory:///").unwrap();
            let mut bs = BlockStore::new(store, base);
            let meta = bs
                .writer()
                .write_block("t", "b.parquet", schema(), &[batch])
                .await
                .unwrap();
            bs.index_mut().add_series("t", api_fp, &api);
            bs.index_mut().add_series("t", web_fp, &web);
            bs.index_mut().add_block(&meta);

            let (ctx, table) = bs
                .scan_context(
                    "t",
                    &[LabelMatcher::new("app", MatchOp::Eq, "api")],
                    i64::MIN,
                    i64::MAX,
                    schema(),
                )
                .await
                .unwrap();
            let df = ctx
                .sql(&format!(
                    "SELECT count(*) AS c FROM {table} WHERE series_fingerprint = {api_fp}"
                ))
                .await
                .unwrap();
            let out = df.collect().await.unwrap();
            let c = out[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);

            prop_assert_eq!(usize::try_from(c).unwrap(), expected_api_count);
            Ok(())
        })?;
    }
}
```

> The property test uses a fresh current-thread runtime per case (proptest closures are sync). This mirrors how the workspace drives async inside `proptest!` elsewhere. If the empty-`api`-rows case ever produces zero candidate blocks, `scan_context` still returns the empty-shape table, so `count(*)` is `0` and the assertion holds.

- [ ] **Step 2: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --test roundtrip_proptest`
Expected: PASS (64 cases).

- [ ] **Step 3: Final whole-crate gate**

Run: `cargo test -p crabka-blockstore && cargo clippy -p crabka-blockstore --all-targets && cargo fmt -p crabka-blockstore --check`
Expected: all PASS, no warnings, formatting clean.

- [ ] **Step 4: Commit**

```bash
git add crates/blockstore/
git commit -m "test(blockstore): property test — equality matcher returns only matching series"
```

---

## Self-review

**Spec coverage (against the §3.3 / §4 / §7 substrate responsibilities):**
- Columnar Parquet block format with mandatory `series_fingerprint` + `timestamp` columns → Tasks 2, 3.
- Object-storage block IO (write + read) → Tasks 3, 4.
- Two-level index (label/series postings + block index) + matcher resolution + block pruning → Task 5.
- Index persistence to object storage → Task 6.
- DataFusion query facade with index pruning + delegated Parquet pushdown ("`LogBlockTableProvider`" realized as a DataFusion Parquet view over pruned blocks) → Task 7.
- Label/series APIs (`labels`/`label values`) the Loki `/labels` endpoints need → Task 5.
- *Deferred (correctly, to later phases):* WAL-tail (hot) table, LogQL, ingest endpoints, compactor, Loki HTTP API, multi-tenancy enforcement via Crabka quotas/ACLs, bloom filters. These are Phases 2–4.

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every step has runnable code or an exact command. The single hand-wave (DataFusion scan wiring in Task 7) is explicitly bounded with a verify-against-rev checklist and a behavior-pinning test, not left vague.

**Type consistency:** `BlockMeta` fields (`tenant`/`object_key`/`min_ts`/`max_ts`/`row_count`/`fingerprints`) are identical across Tasks 2/3/5/7. `Index` method names (`add_series`/`add_block`/`resolve`/`candidate_blocks`/`label_names`/`label_values`/`save`/`load`) are stable across Tasks 5/6/7. `scan_context` signature matches between its definition (Task 7) and use (Tasks 7/8). `COL_FINGERPRINT`/`COL_TIMESTAMP` constants used consistently.

**Known risk (flagged, not hidden):** the DataFusion `main` pin tracks a moving branch; the Task 7 DataFusion entry-point signatures are the most likely to drift. Both are contained to `store.rs` + the pin line, with behavior-pinning tests so drift surfaces as a compile error against a green test, never as silent wrong results.
