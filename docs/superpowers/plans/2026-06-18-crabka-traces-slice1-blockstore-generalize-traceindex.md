# crabka-traces Slice 1 — Blockstore generalization + span block schema (nested-set) + `TraceIndex` (bloom)

> **COMPLETION STATUS (as-built):** Done and green. The `BlockIndex` trait, span
> block schema (incl. nested-set columns + DFS pre-order), `ShardedTraceBloom`,
> `TraceIndex`, the span-row builder, and the end-to-end pipeline test
> (`crates/blockstore/tests/trace_pipeline_e2e.rs`) are implemented and tested.
> **Two boxes left unchecked by design (Task 2 Steps 4–5):** `BlockStore` was *not*
> parameterized as `BlockStore<I: BlockIndex>`. The concrete index *is* named
> `SeriesIndex` and implements `BlockIndex` (alongside `TraceIndex`, and the profiles
> signal's `ProfileIndex`, which embeds `SeriesIndex`); the traces path uses
> `BlockWriter` + `TraceIndex` + `span_block_decl()` validation directly, so a generic
> `BlockStore<I>` facade was unnecessary and refactoring it would churn the shared
> logs/metrics/profiles path for no functional gain. Accepted deviation — see design
> spec §14.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize `crabka-blockstore` so a signal declares its own schema + index instead of assuming the mandatory `series_fingerprint`+`timestamp` columns. Extract a `BlockIndex` trait, make the existing logs/metrics index a `SeriesIndex` impl (no behavior change — regression-tested against the existing blockstore tests), and add a `TraceIndex` impl. Define the flattened span-per-row Arrow/Parquet span block schema (identity + the load-bearing **nested-set** structural columns + trace-denormalized + intrinsics + dedicated/promoted attr columns + typed-list generic attrs + nested events/links), and provide the **DFS pre-order nested-set builder** with interval-containment property tests. Wire a span-block write/read path through `BlockWriter` and the `TraceIndex` queries (candidate blocks by FNV-sharded `trace_id` bloom; tag-set pruning) so by-id retrieval is **index-less** (no global `trace_id → block` map).

**Architecture:** This slice is pure data layer on `crabka-blockstore` (no networking, no TraceQL, no Kafka). The existing concrete `Index` is split: the matcher→series→block logic becomes `SeriesIndex` behind a new `BlockIndex` trait; `BlockStore` becomes parameterized/`dyn` over `BlockIndex` so logs/metrics (`SeriesIndex`) and traces (`TraceIndex`) share one facade. The mandatory-column assumption moves from a hard `validate_block_schema` to a per-signal `BlockSchema` declaration. The span block is a deliberate Crabka flattening (TraceQL-semantic compat, **not** vParquet byte-format compat): one row per span, sorted/grouped by `trace_id`, carrying `nested_set_left/right/parent_id` (Int32, DFS pre-order over each trace's span tree). `TraceIndex` carries, per block, FNV-1 32-bit-sharded `trace_id` bloom filters (for index-less by-id locate: time/block prefilter → bloom test → Parquet row-group min/max binary search) plus per-block tag-name/value sets + blooms (search pruning + tag discovery). The bloom is implemented inline (FNV-seeded bit-array, Tempo's `0.01` FP default) — no external bloom crate — with a note to swap to parquet's `Sbbf` in production.

**Tech Stack:** Rust 2024 · `datafusion` (git `main`, pinned — see Global Constraints) · `arrow` 59 · `parquet` 59 · `object_store` 0.13 · `tokio` · `thiserror` · `serde` / `serde_json` (index snapshot) · `regex`. Tests: `assert2`, `proptest`, `tempfile`, `object_store::memory::InMemory`, `#[tokio::test]`.

## Global Constraints

- **No backwards compatibility.** Crabka is greenfield/undeployed. No `#[serde(default)]` shims, no V2-alongside-V1 enum variants, no migration code, no default-off feature gates. The `BlockIndex` extraction **replaces** the concrete `Index` — there is no "keep `Index` around for old snapshots." Wipe any local index snapshots during development. (Only Kafka wire compat matters — and this crate touches none of it.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe` (the inline bloom is a safe `Vec<u64>` bit-array).
- **Lints:** `clippy::pedantic` is `warn` workspace-wide (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` allowed). New code must be clippy-pedantic clean. Run `cargo clippy -p crabka-blockstore --all-targets` before each commit.
- **Formatting:** run `cargo fmt -p crabka-blockstore` before every commit. **NEVER** run `cargo +nightly fmt --all` — it fails with OS error 206 / path-too-long in deep worktrees on Windows; always scope with `-p`.
- **Assertions:** use `assert2::assert!` / `assert2::check!` in tests, `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`. This `main` revision tracks arrow 59 / parquet 59 / object_store 0.13.2, which unify with the workspace pins (same major → cargo unifies to one crate instance, so arrow types cross the DataFusion boundary cleanly). Do **not** substitute a released `datafusion` (54.x is on arrow 58 and pulls a second, incompatible arrow major).
- **Arrow version identity:** import `arrow`/`parquet` directly (`use arrow::...`, `use parquet::...`) as the existing blockstore does; all of arrow/parquet/object_store unify to one instance. If a type-mismatch error ever appears at the DataFusion boundary, switch that import to DataFusion's re-export (`datafusion::arrow`/`datafusion::parquet`) to force identity.
- **Regression-test the `SeriesIndex` extraction.** The `BlockIndex` refactor must be behavior-preserving for logs/metrics: every existing blockstore `index`/`store` test must keep passing unchanged (the test bodies move from calling `Index` to calling `SeriesIndex`, but the asserted behavior is identical). This is the safety net for the refactor.
- **FNV is the canonical sharder/hasher.** Bloom shard selection and the per-bloom hashes are FNV-1 32-bit over `trace_id` bytes (shard = `fnv1_32(trace_id) % shard_count`), matching Tempo's sharding choice. Crabka-owned bloom encoding (no Tempo block-format interop).
- **Span block format is a deliberate flattening.** One row per span, sorted/grouped by `trace_id`. This is TraceQL-semantic/API compat, **not** vParquet byte-format compat (greenfield — no block-format interop required).

---

## Dependency & slice roadmap

**Depends on:** `crabka-blockstore` *(as designed in `docs/superpowers/plans/2026-06-18-crabka-blockstore.md`)* — `BlockStore`, `BlockWriter`, `BlockMeta`, `Index` (becomes `SeriesIndex` here), `Labels`, `LabelMatcher`, `MatchOp`, `SeriesFingerprint`, `COL_FINGERPRINT`, `COL_TIMESTAMP`, `validate_block_schema`, `read_block`, `scan_context`. This slice **modifies** blockstore in place (extract trait, add `TraceIndex` + span schema). It adds **no** new crate; `crabka-traces` is *not* started here (no shared types module is needed — span-block column constants live in blockstore alongside the metrics signal's column constants, and the WAL `SpanRecord` belongs to slice 4).

**The 8 traces slices** (this plan = Slice 1; each later slice gets its own plan; commands use the slice's crate — `crabka-blockstore` here, `crabka-traceql` for 2–3, `crabka-traces` for 4–8):

1. **Blockstore generalization + span block schema + `TraceIndex`** *(this plan)* — `BlockIndex` trait; `Index`→`SeriesIndex`; relax mandatory columns; flattened span block (incl. **nested-set columns + DFS pre-order**); `TraceIndex` (FNV-sharded `trace_id` bloom + per-block tag sets/blooms); span-block write/read path. **Freezes:** `BlockIndex`, `SeriesIndex`, `TraceIndex`, the span-block column constants + `span_block_schema()`, `NestedSetBuilder` + `SpanNode`, and the `TraceIndex` query surface (`candidate_blocks_for_trace`, `prune_blocks_by_tag`, `tag_names`/`tag_values`).
2. **`crabka-traceql` core** — parser + planner + selectors + non-structural pushdown + the `SpanStructuralJoin` lowering for the **core** structural operators. Defines the `SpanStore` trait + pinned result types. **Consumes** this slice's nested-set columns (the join keys) + `TraceIndex` semantics.
3. **TraceQL completeness** — negated/union structural forms, pipeline aggregations, TraceQL metrics, tag discovery. Consumes the same nested-set columns + `TraceIndex` tag sets.
4. **Ingest service** — `distributor` → `trace_id`-partitioned WAL; `block-builder` consumer group → span blocks (calls **this slice's** `NestedSetBuilder` + `span_block_schema()` + `BlockWriter` + `TraceIndex`); `live-store` hot tier. Defines `SpanRecord`.
5. **Querier + Tempo HTTP API** — implements `SpanStore` as hot/cold UNION over **this slice's** `TraceIndex` by-id path + span blocks.
6. **Query-frontend** — search sharding/queueing.
7. **Metrics-generator** — span-metrics + service-graphs → remote_write.
8. **Hardening** — per-tenant limits, differential-vs-Tempo, Grafana integration.

---

## File structure (`crates/blockstore/` — modified + new files)

| File | Responsibility | Change |
|---|---|---|
| `src/lib.rs` | module decls + public re-exports | **modify** — re-export `BlockIndex`, `SeriesIndex`, `TraceIndex`, span constants, `NestedSetBuilder`, `SpanNode`, `BlockSchema` |
| `src/block_index.rs` | the `BlockIndex` trait + `BlockSchema` declaration | **create** |
| `src/index.rs` (logs/metrics) | the existing series index | **modify** — rename `Index` → `SeriesIndex`; `impl BlockIndex for SeriesIndex` |
| `src/block.rs` | column constants + schema validation | **modify** — relax `validate_block_schema` to validate against a declared `BlockSchema` |
| `src/store.rs` | `BlockStore` facade | **modify** — parameterize over `BlockIndex` (`BlockStore<I>`); keep `scan_context` |
| `src/span_schema.rs` | span-block column constants + `span_block_schema()` + enums (`SpanKind`, `StatusCode`) | **create** |
| `src/nested_set.rs` | `SpanNode` + `NestedSetBuilder` (DFS pre-order) + interval-containment property tests | **create** |
| `src/span_block.rs` | span-row builder: `(SpanRow)` → `RecordBatch` matching `span_block_schema()` (typed-list attrs, nested events/links) | **create** |
| `src/bloom.rs` | `ShardedTraceBloom` (FNV-1 32-bit sharded `trace_id` bloom) | **create** |
| `src/trace_index.rs` | `TraceIndex` (impl `BlockIndex`): per-block sharded bloom + tag-name/value sets + tag blooms; by-id candidate blocks; tag pruning; snapshot serde | **create** |

`store.rs` remains the only file depending on DataFusion's query layer. The nested-set DFS (`nested_set.rs`) and the bloom (`bloom.rs`) are pure-compute, fully unit/property-tested without IO.

---

### Task 1: Extract the `BlockIndex` trait + `BlockSchema` declaration

**Files:**
- Create: `crates/blockstore/src/block_index.rs`
- Modify: `crates/blockstore/src/block.rs` (relax `validate_block_schema` to take a `BlockSchema`)
- Modify: `crates/blockstore/src/lib.rs` (declare module + re-export)

**Interfaces:**
- Consumes: `Result`, `BlockStoreError`, `BlockMeta`, `SeriesFingerprint`, `arrow::datatypes::Schema`.
- Produces:
  - `pub trait BlockIndex: Default + serde::Serialize + serde::de::DeserializeOwned` with:
    - `fn add_block(&mut self, meta: &BlockMeta);`
    - `fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String>;` — time-only block prefilter shared by every signal.
    - `fn block_count(&self, tenant: &str) -> usize;`
  - `pub struct BlockSchema { pub required: Vec<RequiredColumn>, pub sort_key: Vec<String> }`
  - `pub struct RequiredColumn { pub name: String, pub data_type: arrow::datatypes::DataType, pub nullable: bool }`
  - `pub fn series_block_schema() -> BlockSchema` — the logs/metrics signal's declaration (`series_fingerprint: UInt64` + `timestamp: Int64`, sort key `[series_fingerprint, timestamp]`).
  - `pub fn validate_against(schema: &arrow::datatypes::Schema, decl: &BlockSchema) -> Result<()>` (in `block.rs`).

- [x] **Step 1: Write the failing test for `block_index.rs`**

Create `crates/blockstore/src/block_index.rs` with the tests first:

```rust
#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::assert;

    use super::*;
    use crate::block::validate_against;

    #[test]
    fn series_declaration_lists_mandatory_columns() {
        let decl = series_block_schema();
        let names: Vec<&str> = decl.required.iter().map(|c| c.name.as_str()).collect();
        assert!(names == vec!["series_fingerprint", "timestamp"]);
        assert!(decl.sort_key == vec!["series_fingerprint".to_string(), "timestamp".to_string()]);
    }

    #[test]
    fn validate_against_accepts_matching_schema() {
        let decl = series_block_schema();
        let schema = Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]);
        assert!(validate_against(&schema, &decl).is_ok());
    }

    #[test]
    fn validate_against_rejects_wrong_type() {
        let decl = series_block_schema();
        let schema = Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("timestamp", DataType::Utf8, false), // wrong
        ]);
        assert!(validate_against(&schema, &decl).is_err());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib block_index`
Expected: FAIL — `cannot find function series_block_schema` / `cannot find function validate_against`.

- [x] **Step 3: Implement `block_index.rs`**

Prepend above the `tests` module:

```rust
//! The `BlockIndex` trait: the pluggable per-signal index seam.
//!
//! Every signal (logs/metrics via [`crate::SeriesIndex`], traces via
//! [`crate::TraceIndex`]) implements `BlockIndex`. The trait carries only the
//! signal-agnostic surface the [`crate::BlockStore`] facade needs: register a
//! written block, and time-prefilter blocks for a tenant. Signal-specific
//! resolution (matcher→series for logs/metrics; `trace_id` bloom + tag pruning
//! for traces) lives on the concrete impls, not the trait — the facade calls the
//! concrete type's richer methods where it has a concrete `I`.

use arrow::datatypes::DataType;
use serde::{Serialize, de::DeserializeOwned};

use crate::block::BlockMeta;

/// One required column in a signal's [`BlockSchema`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl RequiredColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// A signal's declared block schema: the columns a block of this signal must
/// carry, plus the sort key its rows are ordered by. Replaces the old hardcoded
/// `series_fingerprint`+`timestamp` assumption — each signal declares its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSchema {
    pub required: Vec<RequiredColumn>,
    pub sort_key: Vec<String>,
}

/// The logs/metrics signal declaration: the two mandatory columns + their sort
/// key. Identical to the pre-generalization hardcoded assumption.
#[must_use]
pub fn series_block_schema() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(crate::block::COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(crate::block::COL_TIMESTAMP, DataType::Int64, false),
        ],
        sort_key: vec![
            crate::block::COL_FINGERPRINT.to_string(),
            crate::block::COL_TIMESTAMP.to_string(),
        ],
    }
}

/// The pluggable per-signal index. `BlockStore` is generic/`dyn` over this.
///
/// Bounds: every impl round-trips through the JSON snapshot (`Serialize` +
/// `DeserializeOwned`) and is constructible empty (`Default`).
pub trait BlockIndex: Default + Serialize + DeserializeOwned {
    /// Register a written block in the index.
    fn add_block(&mut self, meta: &BlockMeta);

    /// Time-only block prefilter: object keys whose `[min_ts, max_ts]` overlaps
    /// `[min_ts, max_ts]` for `tenant`. The signal-specific narrowing (series
    /// fingerprints / `trace_id` bloom / tag sets) happens on the concrete type
    /// on top of this.
    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String>;

    /// Number of blocks registered for `tenant`.
    fn block_count(&self, tenant: &str) -> usize;
}
```

- [x] **Step 4: Relax `validate_block_schema` in `block.rs`**

Replace the body of `validate_block_schema` (which hardcoded the two columns) with a thin wrapper over a new generic `validate_against`. Add to `block.rs` (keep the `COL_FINGERPRINT`/`COL_TIMESTAMP` constants):

```rust
use crate::block_index::BlockSchema;

/// Validate an Arrow schema against a signal's declared [`BlockSchema`]: every
/// required column must be present with the declared type. Extra payload columns
/// are unconstrained.
pub fn validate_against(schema: &Schema, decl: &BlockSchema) -> Result<()> {
    for col in &decl.required {
        let found = schema.column_with_name(&col.name).ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!("missing `{}` column", col.name))
        })?;
        if found.1.data_type() != &col.data_type {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be {:?}, got {:?}",
                col.name,
                col.data_type,
                found.1.data_type()
            )));
        }
    }
    Ok(())
}

/// Back-compat-free convenience: validate against the logs/metrics declaration.
/// (The span signal calls `validate_against(schema, &span_block_decl())`.)
pub fn validate_block_schema(schema: &Schema) -> Result<()> {
    validate_against(schema, &crate::block_index::series_block_schema())
}
```

> The existing `block.rs` tests (`schema_with_required_columns_is_valid`, etc.) call `validate_block_schema` and keep passing unchanged — it now delegates to `validate_against` with the series declaration.

- [x] **Step 5: Wire `lib.rs`**

Add `mod block_index;` and extend the re-export:

```rust
pub use block_index::{BlockIndex, BlockSchema, RequiredColumn, series_block_schema};
pub use block::{BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against, validate_block_schema};
```

- [x] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib block_index && cargo test -p crabka-blockstore --lib block`
Expected: PASS (3 new `block_index` tests + the existing `block` tests unchanged).

- [x] **Step 7: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): extract BlockIndex trait + per-signal BlockSchema declaration"
```

---

### Task 2: Rename `Index` → `SeriesIndex`, `impl BlockIndex`, parameterize `BlockStore`

**Files:**
- Modify: `crates/blockstore/src/index.rs` (rename type + impl the trait)
- Modify: `crates/blockstore/src/store.rs` (`BlockStore` → `BlockStore<I: BlockIndex>`)
- Modify: `crates/blockstore/src/lib.rs` (re-export `SeriesIndex`; drop `Index`)

**Interfaces:**
- Consumes: `BlockIndex`, `BlockMeta`, `Labels`, `LabelMatcher`, `MatchOp`, `SeriesFingerprint`, `Result`.
- Produces:
  - `pub struct SeriesIndex` (was `Index`) — same fields/methods (`new`, `add_series`, `add_block`, `resolve`, `candidate_blocks`, `label_names`, `label_values`, `save`, `load`), **plus** `impl BlockIndex for SeriesIndex`.
  - `pub struct BlockStore<I: BlockIndex>` with `new(store, base) -> Self`, `writer()`, `index() -> &I`, `index_mut() -> &mut I`, and (for `SeriesIndex` specifically) `scan_context(...)`. `BlockStore<SeriesIndex>` is the logs/metrics alias.

- [x] **Step 1: Rename + re-point existing tests (regression net)**

In `index.rs`, rename `pub struct Index` → `pub struct SeriesIndex`, `impl Index` → `impl SeriesIndex`, and update every `Index::new()` / `Index::load(...)` in the module's `tests` to `SeriesIndex::...`. **Do not change any asserted behavior** — these tests are the regression net for the refactor.

- [x] **Step 2: Run to verify the rename compiles + tests still pass**

Run: `cargo test -p crabka-blockstore --lib index`
Expected: PASS (all 7 existing index tests, now against `SeriesIndex`).

- [x] **Step 3: Implement `BlockIndex` for `SeriesIndex`**

Add to `index.rs` (the trait's `add_block`/`candidate_blocks` delegate to the existing inherent logic; the trait's `candidate_blocks` is the time-only prefilter, so it ignores fingerprints — the richer fingerprint-aware `candidate_blocks` inherent method stays for the logs/metrics scan path):

```rust
use crate::block_index::BlockIndex;

impl BlockIndex for SeriesIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        // Reuse the inherent method (already takes &BlockMeta).
        SeriesIndex::add_block(self, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        // Trait-level prefilter: time overlap only (no fingerprint set yet).
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .map(|b| b.object_key.clone())
            .collect()
    }

    fn block_count(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.blocks.len())
    }
}
```

> The inherent `SeriesIndex::candidate_blocks(tenant, &fps, min_ts, max_ts)` (fingerprint-aware) and the trait `BlockIndex::candidate_blocks(tenant, min_ts, max_ts)` (time-only) have different arities, so they coexist without a name clash — the inherent method wins at the `SeriesIndex`-typed call site in `store.rs::scan_context`. If the compiler reports ambiguity, disambiguate the trait call as `BlockIndex::candidate_blocks(self, ...)`.

- [ ] **Step 4: Parameterize `BlockStore`**

In `store.rs`, change `pub struct BlockStore { ... index: Index }` to:

```rust
use crate::block_index::BlockIndex;

pub struct BlockStore<I: BlockIndex> {
    store: Arc<dyn ObjectStore>,
    base: Url,
    index: I,
}

impl<I: BlockIndex> BlockStore<I> {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, base: Url) -> Self {
        Self { store, base, index: I::default() }
    }

    #[must_use]
    pub fn writer(&self) -> BlockWriter {
        BlockWriter::new(self.store.clone())
    }

    #[must_use]
    pub fn index(&self) -> &I {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut I {
        &mut self.index
    }
}
```

Keep `scan_context` but move it into an `impl BlockStore<SeriesIndex>` block (it is logs/metrics-specific — it calls the fingerprint-aware inherent `resolve`/`candidate_blocks`):

```rust
impl BlockStore<SeriesIndex> {
    pub async fn scan_context(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        // ... unchanged body from the blockstore plan Task 7 ...
    }
}
```

- [ ] **Step 5: Update `store.rs` tests + `lib.rs`**

In `store.rs` tests, change `BlockStore::new(...)` to `BlockStore::<SeriesIndex>::new(...)` (or add a `type LogStore = BlockStore<SeriesIndex>;` test alias). In `lib.rs`, replace `pub use index::Index;` with `pub use index::SeriesIndex;` and keep `pub use store::BlockStore;`.

- [x] **Step 6: Run the whole crate (regression gate)**

Run: `cargo test -p crabka-blockstore`
Expected: PASS — every pre-existing test green against `SeriesIndex` + `BlockStore<SeriesIndex>`; **no behavior change**.

- [x] **Step 7: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "refactor(blockstore): Index -> SeriesIndex impl BlockIndex; BlockStore<I> generic over index"
```

---

### Task 3: Span-block schema — column constants, enums, `span_block_schema()`

**Files:**
- Create: `crates/blockstore/src/span_schema.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef}`, `BlockSchema`, `RequiredColumn`.
- Produces:
  - Span column-name constants: `SCOL_TRACE_ID`, `SCOL_SPAN_ID`, `SCOL_PARENT_SPAN_ID`, `SCOL_NESTED_SET_LEFT`, `SCOL_NESTED_SET_RIGHT`, `SCOL_PARENT_ID`, `SCOL_ROOT_SERVICE_NAME`, `SCOL_ROOT_SPAN_NAME`, `SCOL_TRACE_START_NANO`, `SCOL_TRACE_DURATION_NANOS`, `SCOL_NAME`, `SCOL_KIND`, `SCOL_START_NANO`, `SCOL_DURATION_NANOS`, `SCOL_STATUS_CODE`, `SCOL_STATUS_MESSAGE`, `SCOL_ATTR_KEYS`, `SCOL_ATTR_IS_ARRAY`, `SCOL_ATTR_VALUE`, `SCOL_ATTR_VALUE_INT`, `SCOL_ATTR_VALUE_DOUBLE`, `SCOL_ATTR_VALUE_BOOL`, `SCOL_EVENTS`, `SCOL_LINKS`.
  - `pub enum SpanKind { Unspecified, Internal, Server, Client, Producer, Consumer }` (`Copy`; `as_i32()`/`from_i32(i32)->SpanKind`).
  - `pub enum StatusCode { Unset, Ok, Error }` (`Copy`; `as_i32()`/`from_i32(i32)->StatusCode`).
  - `pub fn span_block_schema() -> SchemaRef` — the flattened span-per-row Arrow schema.
  - `pub fn span_block_decl() -> BlockSchema` — the span signal's `BlockSchema` (required: `trace_id`/`start_unix_nano`; sort key `[trace_id, start_unix_nano]`).

- [x] **Step 1: Write the failing test**

Create `crates/blockstore/src/span_schema.rs`:

```rust
#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::assert;

    use super::*;

    #[test]
    fn identity_columns_are_fixed_size_binary() {
        let s = span_block_schema();
        assert!(s.column_with_name(SCOL_TRACE_ID).unwrap().1.data_type() == &DataType::FixedSizeBinary(16));
        assert!(s.column_with_name(SCOL_SPAN_ID).unwrap().1.data_type() == &DataType::FixedSizeBinary(8));
        assert!(s.column_with_name(SCOL_PARENT_SPAN_ID).unwrap().1.data_type() == &DataType::FixedSizeBinary(8));
    }

    #[test]
    fn nested_set_columns_are_int32() {
        let s = span_block_schema();
        for c in [SCOL_NESTED_SET_LEFT, SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID] {
            assert!(s.column_with_name(c).unwrap().1.data_type() == &DataType::Int32);
        }
    }

    #[test]
    fn generic_attr_value_is_list_of_utf8() {
        let s = span_block_schema();
        let (_, f) = s.column_with_name(SCOL_ATTR_VALUE).unwrap();
        match f.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::List(scalar) => assert!(scalar.data_type() == &DataType::Utf8),
                other => panic!("expected List<List<Utf8>>, inner {other:?}"),
            },
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn events_and_links_are_list_of_struct() {
        let s = span_block_schema();
        for c in [SCOL_EVENTS, SCOL_LINKS] {
            let (_, f) = s.column_with_name(c).unwrap();
            match f.data_type() {
                DataType::List(inner) => assert!(matches!(inner.data_type(), DataType::Struct(_))),
                other => panic!("expected List<Struct>, got {other:?}"),
            }
        }
    }

    #[test]
    fn kind_and_status_enums_round_trip_i32() {
        for k in [SpanKind::Unspecified, SpanKind::Internal, SpanKind::Server,
                  SpanKind::Client, SpanKind::Producer, SpanKind::Consumer] {
            assert!(SpanKind::from_i32(k.as_i32()) == k);
        }
        for s in [StatusCode::Unset, StatusCode::Ok, StatusCode::Error] {
            assert!(StatusCode::from_i32(s.as_i32()) == s);
        }
    }

    #[test]
    fn span_decl_sort_key_is_trace_id_then_start() {
        let d = span_block_decl();
        assert!(d.sort_key == vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()]);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib span_schema`
Expected: FAIL — `cannot find function span_block_schema`.

- [x] **Step 3: Implement `span_schema.rs`**

Prepend above the `tests` module. The generic-attribute model is per-row parallel arrays: `attr_keys: List<Utf8>` names each attribute on the span; `attr_is_array: List<Bool>` flags array attrs; and the four typed value columns are `List<List<T>>` (outer = per-attribute, inner = the value list; a scalar is a single-element inner list). For a given span row, exactly one of the four typed lists is populated per attribute (the others hold an empty inner list at that index), keyed positionally by `attr_keys`.

```rust
//! Flattened span-per-row block schema (a deliberate Crabka choice — TraceQL
//! semantic/API compat, NOT vParquet byte-format compat). One row per span,
//! sorted/grouped by `trace_id`. Trace- and resource-level fields are
//! denormalized onto every span row. The nested-set columns make structural
//! TraceQL ops cheap columnar predicates instead of tree walks.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

use crate::block_index::{BlockSchema, RequiredColumn};

// Identity (raw bytes).
pub const SCOL_TRACE_ID: &str = "trace_id";
pub const SCOL_SPAN_ID: &str = "span_id";
pub const SCOL_PARENT_SPAN_ID: &str = "parent_span_id";

// Structural / nested-set (the load-bearing columns).
pub const SCOL_NESTED_SET_LEFT: &str = "nested_set_left";
pub const SCOL_NESTED_SET_RIGHT: &str = "nested_set_right";
pub const SCOL_PARENT_ID: &str = "parent_id";

// Trace-denormalized (one value per trace, copied to every span row).
pub const SCOL_ROOT_SERVICE_NAME: &str = "root_service_name";
pub const SCOL_ROOT_SPAN_NAME: &str = "root_span_name";
pub const SCOL_TRACE_START_NANO: &str = "trace_start_unix_nano";
pub const SCOL_TRACE_DURATION_NANOS: &str = "trace_duration_nanos";

// Span intrinsics.
pub const SCOL_NAME: &str = "name";
pub const SCOL_KIND: &str = "kind";
pub const SCOL_START_NANO: &str = "start_unix_nano";
pub const SCOL_DURATION_NANOS: &str = "duration_nanos";
pub const SCOL_STATUS_CODE: &str = "status_code";
pub const SCOL_STATUS_MESSAGE: &str = "status_message";

// Generic attributes (typed LIST columns, array-aware). Positional by `attr_keys`.
pub const SCOL_ATTR_KEYS: &str = "attr_keys";
pub const SCOL_ATTR_IS_ARRAY: &str = "attr_is_array";
pub const SCOL_ATTR_VALUE: &str = "attr_value";
pub const SCOL_ATTR_VALUE_INT: &str = "attr_value_int";
pub const SCOL_ATTR_VALUE_DOUBLE: &str = "attr_value_double";
pub const SCOL_ATTR_VALUE_BOOL: &str = "attr_value_bool";

// Events & links (nested).
pub const SCOL_EVENTS: &str = "events";
pub const SCOL_LINKS: &str = "links";

/// OTLP span kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanKind {
    Unspecified,
    Internal,
    Server,
    Client,
    Producer,
    Consumer,
}

impl SpanKind {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        match self {
            SpanKind::Unspecified => 0,
            SpanKind::Internal => 1,
            SpanKind::Server => 2,
            SpanKind::Client => 3,
            SpanKind::Producer => 4,
            SpanKind::Consumer => 5,
        }
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => SpanKind::Internal,
            2 => SpanKind::Server,
            3 => SpanKind::Client,
            4 => SpanKind::Producer,
            5 => SpanKind::Consumer,
            _ => SpanKind::Unspecified,
        }
    }
}

/// OTLP status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusCode {
    Unset,
    Ok,
    Error,
}

impl StatusCode {
    #[must_use]
    pub fn as_i32(self) -> i32 {
        match self {
            StatusCode::Unset => 0,
            StatusCode::Ok => 1,
            StatusCode::Error => 2,
        }
    }

    #[must_use]
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => StatusCode::Ok,
            2 => StatusCode::Error,
            _ => StatusCode::Unset,
        }
    }
}

/// `List<T>` helper.
fn list_of(name: &str, inner: DataType, nullable: bool) -> DataType {
    DataType::List(Arc::new(Field::new(name, inner, nullable)))
}

/// `List<List<T>>` — per-attribute outer, value-list inner.
fn list_list_of(inner: DataType) -> DataType {
    list_of("item", list_of("item", inner, true), true)
}

/// `Struct` for one event: `name: Utf8`, `time_since_start_nano: Int64`,
/// plus the same typed-list attribute columns as a span (flattened).
fn event_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("time_since_start_nano", DataType::Int64, true),
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
    ]))
}

/// `Struct` for one link: linked `trace_id`/`span_id` raw bytes + attrs.
fn link_struct() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
    ]))
}

/// The flattened span-per-row Arrow schema.
#[must_use]
pub fn span_block_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // identity
        Field::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(SCOL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(SCOL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        // structural / nested-set
        Field::new(SCOL_NESTED_SET_LEFT, DataType::Int32, false),
        Field::new(SCOL_NESTED_SET_RIGHT, DataType::Int32, false),
        Field::new(SCOL_PARENT_ID, DataType::Int32, false),
        // trace-denormalized
        Field::new(SCOL_ROOT_SERVICE_NAME, DataType::Utf8, true),
        Field::new(SCOL_ROOT_SPAN_NAME, DataType::Utf8, true),
        Field::new(SCOL_TRACE_START_NANO, DataType::Int64, false),
        Field::new(SCOL_TRACE_DURATION_NANOS, DataType::Int64, false),
        // intrinsics
        Field::new(SCOL_NAME, DataType::Utf8, true),
        Field::new(SCOL_KIND, DataType::Int32, false),
        Field::new(SCOL_START_NANO, DataType::Int64, false),
        Field::new(SCOL_DURATION_NANOS, DataType::Int64, false),
        Field::new(SCOL_STATUS_CODE, DataType::Int32, false),
        Field::new(SCOL_STATUS_MESSAGE, DataType::Utf8, true),
        // generic attrs (typed lists, array-aware), positional by attr_keys
        Field::new(SCOL_ATTR_KEYS, list_of("item", DataType::Utf8, true), true),
        Field::new(SCOL_ATTR_IS_ARRAY, list_of("item", DataType::Boolean, true), true),
        Field::new(SCOL_ATTR_VALUE, list_list_of(DataType::Utf8), true),
        Field::new(SCOL_ATTR_VALUE_INT, list_list_of(DataType::Int64), true),
        Field::new(SCOL_ATTR_VALUE_DOUBLE, list_list_of(DataType::Float64), true),
        Field::new(SCOL_ATTR_VALUE_BOOL, list_list_of(DataType::Boolean), true),
        // nested events/links
        Field::new(SCOL_EVENTS, list_of("item", event_struct(), true), true),
        Field::new(SCOL_LINKS, list_of("item", link_struct(), true), true),
    ]))
}

/// The span signal's `BlockSchema` declaration: `trace_id` (the by-id locate
/// key) + `start_unix_nano` (the time prefilter key) are required; rows are
/// sorted by `[trace_id, start_unix_nano]` so a trace's spans are contiguous.
#[must_use]
pub fn span_block_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(SCOL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            RequiredColumn::new(SCOL_START_NANO, DataType::Int64, false),
        ],
        sort_key: vec![SCOL_TRACE_ID.to_string(), SCOL_START_NANO.to_string()],
    }
}
```

> **Arrow-builder note (pin in Task 5):** the inner `List` field name (`"item"`) and the `Struct` field names here must match what the span-row builder (Task 5) emits, or `RecordBatch::try_new` fails schema validation. The builder's output is the source of truth — if arrow 59 names an inner list field differently, align this schema to the builder. Pinned by Task 5's round-trip test.

- [x] **Step 4: Wire `lib.rs`**

Add `mod span_schema;` and re-export every `SCOL_*` constant + `SpanKind`, `StatusCode`, `span_block_schema`, `span_block_decl`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib span_schema`
Expected: PASS (6 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): flattened span-per-row block schema + SpanKind/StatusCode enums"
```

---

### Task 4: Nested-set DFS pre-order builder (the headline) + interval-containment property tests

**Files:**
- Create: `crates/blockstore/src/nested_set.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: nothing from blockstore (pure compute over raw `span_id`/`parent_span_id` bytes).
- Produces:
  - `pub struct SpanNode { pub span_id: [u8; 8], pub parent_span_id: Option<[u8; 8]> }`
  - `pub struct NestedSet { pub nested_set_left: i32, pub nested_set_right: i32, pub parent_id: i32 }`
  - `pub fn assign_nested_set(spans: &[SpanNode]) -> Vec<NestedSet>` — DFS pre-order (modified pre-order traversal) over each trace's span forest: ancestor `[left,right]` strictly contains every descendant; `parent_id(child) == parent.nested_set_left`; roots get `parent_id == 0` (the sentinel). Output is index-aligned to `spans`. Orphans (parent not present in the slice) are treated as roots. Counter starts at 1 (0 reserved as the no-parent sentinel).

- [x] **Step 1: Write the failing tests (hand-built trees + the invariants)**

Create `crates/blockstore/src/nested_set.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn sid(n: u8) -> [u8; 8] {
        [n, 0, 0, 0, 0, 0, 0, 0]
    }

    fn node(id: u8, parent: Option<u8>) -> SpanNode {
        SpanNode { span_id: sid(id), parent_span_id: parent.map(sid) }
    }

    // Tree:  1
    //       / \
    //      2   3
    //          |
    //          4
    fn sample_tree() -> Vec<SpanNode> {
        vec![
            node(1, None),
            node(2, Some(1)),
            node(3, Some(1)),
            node(4, Some(3)),
        ]
    }

    fn idx(spans: &[SpanNode], id: u8) -> usize {
        spans.iter().position(|s| s.span_id == sid(id)).unwrap()
    }

    #[test]
    fn root_has_sentinel_parent_id() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        assert!(ns[idx(&spans, 1)].parent_id == 0);
    }

    #[test]
    fn child_parent_id_equals_parent_left() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        let p_left = ns[idx(&spans, 3)].nested_set_left;
        assert!(ns[idx(&spans, 4)].parent_id == p_left);
        let root_left = ns[idx(&spans, 1)].nested_set_left;
        assert!(ns[idx(&spans, 2)].parent_id == root_left);
        assert!(ns[idx(&spans, 3)].parent_id == root_left);
    }

    #[test]
    fn ancestor_interval_strictly_contains_descendants() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        let r = ns[idx(&spans, 1)];
        for id in [2_u8, 3, 4] {
            let d = ns[idx(&spans, id)];
            assert!(r.nested_set_left < d.nested_set_left);
            assert!(d.nested_set_right < r.nested_set_right);
        }
        // 4 is under 3 but NOT under 2.
        let three = ns[idx(&spans, 3)];
        let two = ns[idx(&spans, 2)];
        let four = ns[idx(&spans, 4)];
        assert!(three.nested_set_left < four.nested_set_left && four.nested_set_right < three.nested_set_right);
        assert!(!(two.nested_set_left < four.nested_set_left && four.nested_set_right < two.nested_set_right));
    }

    #[test]
    fn orphan_is_treated_as_root() {
        // parent 99 absent → node 5 is a root.
        let spans = vec![node(5, Some(99))];
        let ns = assign_nested_set(&spans);
        assert!(ns[0].parent_id == 0);
        assert!(ns[0].nested_set_left < ns[0].nested_set_right);
    }

    #[test]
    fn left_lt_right_for_every_node() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        for n in &ns {
            assert!(n.nested_set_left < n.nested_set_right);
        }
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib nested_set`
Expected: FAIL — `cannot find type SpanNode` / `cannot find function assign_nested_set`.

- [x] **Step 3: Implement `nested_set.rs`**

Prepend above the `tests` module:

```rust
//! Nested-set (modified pre-order tree traversal) assignment over a trace's span
//! forest. Computed once at block-build; the integer intervals turn structural
//! TraceQL operators into columnar range/equality predicates.
//!
//! Invariants (property-tested):
//! - every node: `nested_set_left < nested_set_right`;
//! - ancestor `[left, right]` strictly contains every descendant's;
//! - `parent_id(child) == parent.nested_set_left`; roots get `parent_id == 0`.

use std::collections::HashMap;

/// One span's tree linkage (raw OTLP bytes). `parent_span_id == None` (or a
/// parent absent from the slice) makes it a root of the forest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpanNode {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
}

/// The three nested-set columns for one span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestedSet {
    pub nested_set_left: i32,
    pub nested_set_right: i32,
    pub parent_id: i32,
}

/// Assign nested-set intervals via DFS pre-order over the span forest. Output is
/// index-aligned to `spans`. The counter starts at 1; `0` is the no-parent
/// sentinel (so a root's children get `parent_id == root.nested_set_left >= 1`,
/// never colliding with the sentinel).
#[must_use]
pub fn assign_nested_set(spans: &[SpanNode]) -> Vec<NestedSet> {
    // span_id -> position in `spans`.
    let pos: HashMap<[u8; 8], usize> =
        spans.iter().enumerate().map(|(i, s)| (s.span_id, i)).collect();

    // children[i] = positions of i's children, in input order (stable DFS).
    let mut children: Vec<Vec<usize>> = vec![Vec::new(); spans.len()];
    let mut roots: Vec<usize> = Vec::new();
    for (i, s) in spans.iter().enumerate() {
        match s.parent_span_id.and_then(|p| pos.get(&p).copied()) {
            Some(parent_idx) if parent_idx != i => children[parent_idx].push(i),
            _ => roots.push(i), // no parent, or parent absent (orphan), or self-parent
        }
    }

    let mut out = vec![
        NestedSet { nested_set_left: 0, nested_set_right: 0, parent_id: 0 };
        spans.len()
    ];
    let mut counter: i32 = 1;

    // Iterative DFS to avoid recursion depth limits on deep traces.
    // Stack frames: (node_idx, parent_left, entered?).
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }
    let mut stack: Vec<Frame> = Vec::new();
    // Push roots in reverse so they're visited in input order.
    for &r in roots.iter().rev() {
        stack.push(Frame::Enter { idx: r, parent_left: 0 });
    }

    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter { idx, parent_left } => {
                let left = counter;
                counter += 1;
                out[idx].nested_set_left = left;
                out[idx].parent_id = parent_left;
                stack.push(Frame::Exit { idx });
                // Push children in reverse for input-order visitation.
                for &c in children[idx].iter().rev() {
                    stack.push(Frame::Enter { idx: c, parent_left: left });
                }
            }
            Frame::Exit { idx } => {
                out[idx].nested_set_right = counter;
                counter += 1;
            }
        }
    }

    out
}
```

> Roots get `parent_id == 0` because their `parent_left` frame value is the sentinel `0` (they are pushed with `parent_left: 0`). A child's `parent_id` is its parent's `left` (assigned before the child is entered), satisfying `parent_id(child) == parent.nested_set_left`.

- [x] **Step 4: Wire `lib.rs`**

Add `mod nested_set;` and `pub use nested_set::{NestedSet, SpanNode, assign_nested_set};`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib nested_set`
Expected: PASS (5 tests).

- [x] **Step 6: Property test — random forests preserve the interval-containment invariants**

Create `crates/blockstore/tests/nested_set_proptest.rs`:

```rust
//! Property: for any random span forest, `assign_nested_set` produces a valid
//! modified-pre-order labeling — left<right everywhere, and ancestor intervals
//! strictly contain descendant intervals (checked against the actual parent
//! chain), and parent_id == parent.nested_set_left.

use std::collections::HashMap;

use crabka_blockstore::{SpanNode, assign_nested_set};
use proptest::prelude::*;

fn sid(n: u32) -> [u8; 8] {
    let b = n.to_le_bytes();
    [b[0], b[1], b[2], b[3], 0, 0, 0, 0]
}

/// Build a random forest of `n` nodes where node i's parent is some j < i
/// (or None). This guarantees an acyclic forest with input order = a valid
/// topological order.
fn arb_forest() -> impl Strategy<Value = Vec<SpanNode>> {
    (1_usize..24)
        .prop_flat_map(|n| {
            // For each node i (1..n), pick parent in {None} ∪ {0..i}.
            let parents = (1..n)
                .map(|i| prop_oneof![Just(None), (0_usize..i).prop_map(Some)])
                .collect::<Vec<_>>();
            (Just(n), parents)
        })
        .prop_map(|(n, parents)| {
            let mut spans = vec![SpanNode { span_id: sid(0), parent_span_id: None }];
            for (i, parent) in parents.into_iter().enumerate() {
                let child = u32::try_from(i + 1).unwrap();
                spans.push(SpanNode {
                    span_id: sid(child),
                    parent_span_id: parent.map(|p| sid(u32::try_from(p).unwrap())),
                });
            }
            let _ = n;
            spans
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn nested_set_intervals_are_valid(spans in arb_forest()) {
        let ns = assign_nested_set(&spans);
        let by_id: HashMap<[u8; 8], usize> =
            spans.iter().enumerate().map(|(i, s)| (s.span_id, i)).collect();

        for n in &ns {
            prop_assert!(n.nested_set_left < n.nested_set_right);
        }

        // Walk each node's parent chain; every proper ancestor must strictly
        // contain the node's interval, and the immediate parent_id must equal
        // the parent's nested_set_left.
        for (i, s) in spans.iter().enumerate() {
            if let Some(p) = s.parent_span_id.and_then(|p| by_id.get(&p).copied()) {
                if p != i {
                    prop_assert_eq!(ns[i].parent_id, ns[p].nested_set_left);
                    // ancestor chain containment
                    let mut cur = Some(p);
                    while let Some(a) = cur {
                        prop_assert!(ns[a].nested_set_left < ns[i].nested_set_left);
                        prop_assert!(ns[i].nested_set_right < ns[a].nested_set_right);
                        cur = spans[a].parent_span_id.and_then(|pp| by_id.get(&pp).copied());
                    }
                } else {
                    prop_assert_eq!(ns[i].parent_id, 0);
                }
            } else {
                prop_assert_eq!(ns[i].parent_id, 0);
            }
        }
    }
}
```

- [x] **Step 7: Run the property test**

Run: `cargo test -p crabka-blockstore --test nested_set_proptest`
Expected: PASS (256 cases).

- [x] **Step 8: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): nested-set DFS pre-order builder + interval-containment property tests"
```

---

### Task 5: Span-row builder — `SpanRow` → `RecordBatch` matching `span_block_schema()`

**Files:**
- Create: `crates/blockstore/src/span_block.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: every `SCOL_*` constant, `span_block_schema`, `SpanKind`, `StatusCode`, `NestedSet`, `Result`, `BlockStoreError`.
- Produces:
  - `pub struct AttrValue` enum: `Str(Vec<String>)`, `Int(Vec<i64>)`, `Double(Vec<f64>)`, `Bool(Vec<bool>)` (a value list; scalar = single element).
  - `pub struct SpanAttr { pub key: String, pub is_array: bool, pub value: AttrValue }`
  - `pub struct SpanEvent { pub name: String, pub time_since_start_nano: i64, pub attrs: Vec<(String, String)> }`
  - `pub struct SpanLink { pub linked_trace_id: [u8;16], pub linked_span_id: [u8;8], pub attrs: Vec<(String, String)> }`
  - `pub struct SpanRow { pub trace_id:[u8;16], pub span_id:[u8;8], pub parent_span_id:Option<[u8;8]>, pub nested_set:NestedSet, pub root_service_name:Option<String>, pub root_span_name:Option<String>, pub trace_start_unix_nano:i64, pub trace_duration_nanos:i64, pub name:Option<String>, pub kind:SpanKind, pub start_unix_nano:i64, pub duration_nanos:i64, pub status_code:StatusCode, pub status_message:Option<String>, pub attrs:Vec<SpanAttr>, pub events:Vec<SpanEvent>, pub links:Vec<SpanLink> }`
  - `pub fn encode_span_rows(rows: &[SpanRow]) -> Result<arrow::record_batch::RecordBatch>` — builds a `RecordBatch` matching `span_block_schema()`.

- [x] **Step 1: Write the failing test**

Create `crates/blockstore/src/span_block.rs`:

```rust
#[cfg(test)]
mod tests {
    use arrow::array::{FixedSizeBinaryArray, Int32Array};
    use assert2::assert;

    use super::*;
    use crate::span_schema::{SCOL_KIND, SCOL_NESTED_SET_LEFT, SCOL_TRACE_ID, span_block_schema};

    fn tid() -> [u8; 16] {
        [1; 16]
    }

    fn sample_row(span: u8, parent: Option<u8>, left: i32) -> SpanRow {
        SpanRow {
            trace_id: tid(),
            span_id: [span; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            nested_set: NestedSet { nested_set_left: left, nested_set_right: left + 1, parent_id: 0 },
            root_service_name: Some("checkout".into()),
            root_span_name: Some("POST /pay".into()),
            trace_start_unix_nano: 1_000,
            trace_duration_nanos: 500,
            name: Some("db.query".into()),
            kind: SpanKind::Client,
            start_unix_nano: 1_100,
            duration_nanos: 50,
            status_code: StatusCode::Error,
            status_message: Some("timeout".into()),
            attrs: vec![SpanAttr {
                key: "http.method".into(),
                is_array: false,
                value: AttrValue::Str(vec!["GET".into()]),
            }],
            events: vec![SpanEvent {
                name: "exception".into(),
                time_since_start_nano: 10,
                attrs: vec![("exception.type".into(), "IOError".into())],
            }],
            links: vec![SpanLink {
                linked_trace_id: [2; 16],
                linked_span_id: [3; 8],
                attrs: vec![],
            }],
        }
    }

    #[test]
    fn encode_matches_schema_and_columns() {
        let rows = vec![sample_row(1, None, 1), sample_row(2, Some(1), 2)];
        let batch = encode_span_rows(&rows).unwrap();
        assert!(batch.schema() == span_block_schema());
        assert!(batch.num_rows() == 2);

        let tids = batch
            .column_by_name(SCOL_TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert!(tids.value(0) == &[1u8; 16]);

        let kinds = batch
            .column_by_name(SCOL_KIND)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(kinds.value(0) == SpanKind::Client.as_i32());

        let lefts = batch
            .column_by_name(SCOL_NESTED_SET_LEFT)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert!(lefts.value(1) == 2);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib span_block`
Expected: FAIL — `cannot find type SpanRow` / `cannot find function encode_span_rows`.

- [x] **Step 3: Implement `span_block.rs`**

Prepend above the `tests` module. This is the fiddliest Arrow code in the slice — building `FixedSizeBinary`, `List<Utf8>`, `List<List<T>>`, and `List<Struct>` columns. Build the scalar columns directly and the nested columns with `ListBuilder`/`StructBuilder`; verify builder method names + inner-field naming against the schema from Task 3.

```rust
//! Builds span-block `RecordBatch`es from in-memory [`SpanRow`]s, matching
//! [`crate::span_block_schema`]. Generic attributes are stored as parallel typed
//! lists (positional by `attr_keys`); a scalar value is a single-element inner
//! list. Events and links are nested `List<Struct>`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder,
    ListBuilder, StringBuilder, StructBuilder,
};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::record_batch::RecordBatch;

use crate::error::{BlockStoreError, Result};
use crate::nested_set::NestedSet;
use crate::span_schema::{
    SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
    SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SpanKind, StatusCode, span_block_schema,
};

/// A generic-attribute value list (scalar = single element).
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(Vec<String>),
    Int(Vec<i64>),
    Double(Vec<f64>),
    Bool(Vec<bool>),
}

/// One generic span/resource attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanAttr {
    pub key: String,
    pub is_array: bool,
    pub value: AttrValue,
}

/// A nested span event.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanEvent {
    pub name: String,
    pub time_since_start_nano: i64,
    pub attrs: Vec<(String, String)>,
}

/// A nested span link.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanLink {
    pub linked_trace_id: [u8; 16],
    pub linked_span_id: [u8; 8],
    pub attrs: Vec<(String, String)>,
}

/// One span row to encode.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub nested_set: NestedSet,
    pub root_service_name: Option<String>,
    pub root_span_name: Option<String>,
    pub trace_start_unix_nano: i64,
    pub trace_duration_nanos: i64,
    pub name: Option<String>,
    pub kind: SpanKind,
    pub start_unix_nano: i64,
    pub duration_nanos: i64,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    pub attrs: Vec<SpanAttr>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}

/// `List<Utf8>` builder for attribute keys (and event/link KV name lists).
fn new_str_list() -> ListBuilder<StringBuilder> {
    ListBuilder::new(StringBuilder::new())
}

/// Encode `rows` into a `RecordBatch` matching [`span_block_schema`].
pub fn encode_span_rows(rows: &[SpanRow]) -> Result<RecordBatch> {
    let mut trace_id = FixedSizeBinaryBuilder::new(16);
    let mut span_id = FixedSizeBinaryBuilder::new(8);
    let mut parent_span_id = FixedSizeBinaryBuilder::new(8);
    let mut ns_left = Int32Builder::new();
    let mut ns_right = Int32Builder::new();
    let mut parent_id = Int32Builder::new();
    let mut root_svc = StringBuilder::new();
    let mut root_name = StringBuilder::new();
    let mut trace_start = Int64Builder::new();
    let mut trace_dur = Int64Builder::new();
    let mut name = StringBuilder::new();
    let mut kind = Int32Builder::new();
    let mut start = Int64Builder::new();
    let mut dur = Int64Builder::new();
    let mut status = Int32Builder::new();
    let mut status_msg = StringBuilder::new();

    let mut attr_keys = new_str_list();
    let mut attr_is_array = ListBuilder::new(BooleanBuilder::new());
    let mut attr_value = ListBuilder::new(new_str_list());
    let mut attr_value_int = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut attr_value_double = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
    let mut attr_value_bool = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));

    let mut events = ListBuilder::new(new_event_struct_builder());
    let mut links = ListBuilder::new(new_link_struct_builder());

    for row in rows {
        trace_id
            .append_value(row.trace_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        span_id
            .append_value(row.span_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        match row.parent_span_id {
            Some(p) => parent_span_id
                .append_value(p)
                .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?,
            None => parent_span_id.append_null(),
        }
        ns_left.append_value(row.nested_set.nested_set_left);
        ns_right.append_value(row.nested_set.nested_set_right);
        parent_id.append_value(row.nested_set.parent_id);
        root_svc.append_option(row.root_service_name.as_deref());
        root_name.append_option(row.root_span_name.as_deref());
        trace_start.append_value(row.trace_start_unix_nano);
        trace_dur.append_value(row.trace_duration_nanos);
        name.append_option(row.name.as_deref());
        kind.append_value(row.kind.as_i32());
        start.append_value(row.start_unix_nano);
        dur.append_value(row.duration_nanos);
        status.append_value(row.status_code.as_i32());
        status_msg.append_option(row.status_message.as_deref());

        append_attrs(
            &row.attrs,
            &mut attr_keys,
            &mut attr_is_array,
            &mut attr_value,
            &mut attr_value_int,
            &mut attr_value_double,
            &mut attr_value_bool,
        );
        append_events(&mut events, &row.events);
        append_links(&mut links, &row.links);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(ns_left.finish()),
        Arc::new(ns_right.finish()),
        Arc::new(parent_id.finish()),
        Arc::new(root_svc.finish()),
        Arc::new(root_name.finish()),
        Arc::new(trace_start.finish()),
        Arc::new(trace_dur.finish()),
        Arc::new(name.finish()),
        Arc::new(kind.finish()),
        Arc::new(start.finish()),
        Arc::new(dur.finish()),
        Arc::new(status.finish()),
        Arc::new(status_msg.finish()),
        Arc::new(attr_keys.finish()),
        Arc::new(attr_is_array.finish()),
        Arc::new(attr_value.finish()),
        Arc::new(attr_value_int.finish()),
        Arc::new(attr_value_double.finish()),
        Arc::new(attr_value_bool.finish()),
        Arc::new(events.finish()),
        Arc::new(links.finish()),
    ];

    RecordBatch::try_new(span_block_schema(), columns)
        .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))
}

/// Append one row's attribute lists, keeping the four typed value lists
/// positionally aligned to `attr_keys`: at attribute position `j`, exactly one
/// typed list holds the value(s); the other three hold an empty inner list.
fn append_attrs(
    attrs: &[SpanAttr],
    keys: &mut ListBuilder<StringBuilder>,
    is_array: &mut ListBuilder<BooleanBuilder>,
    s: &mut ListBuilder<ListBuilder<StringBuilder>>,
    i: &mut ListBuilder<ListBuilder<Int64Builder>>,
    d: &mut ListBuilder<ListBuilder<Float64Builder>>,
    b: &mut ListBuilder<ListBuilder<BooleanBuilder>>,
) {
    for a in attrs {
        keys.values().append_value(&a.key);
        is_array.values().append_value(a.is_array);
        match &a.value {
            AttrValue::Str(v) => {
                for x in v {
                    s.values().values().append_value(x);
                }
                s.values().append(true);
                i.values().append(true);
                d.values().append(true);
                b.values().append(true);
            }
            AttrValue::Int(v) => {
                for &x in v {
                    i.values().values().append_value(x);
                }
                s.values().append(true);
                i.values().append(true);
                d.values().append(true);
                b.values().append(true);
            }
            AttrValue::Double(v) => {
                for &x in v {
                    d.values().values().append_value(x);
                }
                s.values().append(true);
                i.values().append(true);
                d.values().append(true);
                b.values().append(true);
            }
            AttrValue::Bool(v) => {
                for &x in v {
                    b.values().values().append_value(x);
                }
                s.values().append(true);
                i.values().append(true);
                d.values().append(true);
                b.values().append(true);
            }
        }
    }
    keys.append(true);
    is_array.append(true);
    s.append(true);
    i.append(true);
    d.append(true);
    b.append(true);
}

fn new_event_struct_builder() -> StructBuilder {
    StructBuilder::new(
        Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("time_since_start_nano", DataType::Int64, true),
            Field::new(
                SCOL_ATTR_KEYS,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                SCOL_ATTR_VALUE,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
            Box::new(new_str_list()),
            Box::new(ListBuilder::new(new_str_list())),
        ],
    )
}

fn new_link_struct_builder() -> StructBuilder {
    StructBuilder::new(
        Fields::from(vec![
            Field::new("linked_trace_id", DataType::FixedSizeBinary(16), true),
            Field::new("linked_span_id", DataType::FixedSizeBinary(8), true),
            Field::new(
                SCOL_ATTR_KEYS,
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                SCOL_ATTR_VALUE,
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ))),
                true,
            ),
        ]),
        vec![
            Box::new(FixedSizeBinaryBuilder::new(16)),
            Box::new(FixedSizeBinaryBuilder::new(8)),
            Box::new(new_str_list()),
            Box::new(ListBuilder::new(new_str_list())),
        ],
    )
}

fn append_events(events: &mut ListBuilder<StructBuilder>, rows: &[SpanEvent]) {
    let sb = events.values();
    for e in rows {
        sb.field_builder::<StringBuilder>(0).unwrap().append_value(&e.name);
        sb.field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(e.time_since_start_nano);
        append_kv(sb, &e.attrs);
        sb.append(true);
    }
    events.append(true);
}

fn append_links(links: &mut ListBuilder<StructBuilder>, rows: &[SpanLink]) {
    let sb = links.values();
    for l in rows {
        sb.field_builder::<FixedSizeBinaryBuilder>(0)
            .unwrap()
            .append_value(l.linked_trace_id)
            .unwrap();
        sb.field_builder::<FixedSizeBinaryBuilder>(1)
            .unwrap()
            .append_value(l.linked_span_id)
            .unwrap();
        append_kv(sb, &l.attrs);
        sb.append(true);
    }
    links.append(true);
}

/// Append the `attr_keys: List<Utf8>` + `attr_value: List<List<Utf8>>` KV pair
/// inside an event/link struct (fields 2 and 3).
fn append_kv(sb: &mut StructBuilder, attrs: &[(String, String)]) {
    let keys = sb.field_builder::<ListBuilder<StringBuilder>>(2).unwrap();
    for (k, _) in attrs {
        keys.values().append_value(k);
    }
    keys.append(true);
    let vals = sb
        .field_builder::<ListBuilder<ListBuilder<StringBuilder>>>(3)
        .unwrap();
    for (_, v) in attrs {
        vals.values().values().append_value(v);
        vals.values().append(true);
    }
    vals.append(true);
}
```

> **Arrow-builder verification (do this if compile/test fails — verify against arrow 59):** `StructBuilder::field_builder::<T>(i)`, `ListBuilder::values()` (returns the inner builder; for a `ListBuilder<ListBuilder<T>>` you call `.values().values()` to reach the scalar builder), `FixedSizeBinaryBuilder::append_value(impl AsRef<[u8]>) -> Result<(), ArrowError>`, and the `append(true)`/`append_null()`/`append_option(..)` conventions are arrow-59 API. If a builder method name or the inner list field name (`"item"`) differs, align the impl AND the Task-3 schema to the builder's actual output (the builder is the source of truth) — keep the *behavior* (`batch.schema() == span_block_schema()` + the column-value asserts) the test pins. The schema-equality assertion is the tripwire that catches any inner-field-name drift.

- [x] **Step 4: Wire `lib.rs`**

Add `mod span_block;` and `pub use span_block::{AttrValue, SpanAttr, SpanEvent, SpanLink, SpanRow, encode_span_rows};`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib span_block`
Expected: PASS.

- [x] **Step 6: Round-trip integration test — write a span block through `BlockWriter` and read it back**

Create `crates/blockstore/tests/span_block_roundtrip.rs`:

```rust
//! A span block written through `BlockWriter` (validated against the span
//! declaration) reads back with the same row count and trace_id column.

use std::sync::Arc;

use arrow::array::FixedSizeBinaryArray;
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{
    AttrValue, BlockWriter, NestedSet, SpanAttr, SpanKind, SpanRow, StatusCode, encode_span_rows,
    read_block, span_block_decl, span_block_schema, validate_against,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn row(trace: u8, span: u8, left: i32) -> SpanRow {
    SpanRow {
        trace_id: [trace; 16],
        span_id: [span; 8],
        parent_span_id: None,
        nested_set: NestedSet { nested_set_left: left, nested_set_right: left + 1, parent_id: 0 },
        root_service_name: Some("svc".into()),
        root_span_name: Some("root".into()),
        trace_start_unix_nano: 100,
        trace_duration_nanos: 10,
        name: Some("op".into()),
        kind: SpanKind::Server,
        start_unix_nano: 100,
        duration_nanos: 5,
        status_code: StatusCode::Ok,
        status_message: None,
        attrs: vec![SpanAttr {
            key: "k".into(),
            is_array: false,
            value: AttrValue::Int(vec![7]),
        }],
        events: vec![],
        links: vec![],
    }
}

#[tokio::test]
async fn span_block_validates_and_round_trips() {
    let batch = encode_span_rows(&[row(1, 1, 1), row(1, 2, 2)]).unwrap();
    // The span block satisfies its own declaration.
    validate_against(&batch.schema(), &span_block_decl()).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    BlockWriter::new(store.clone())
        .write_block("tenant", "blocks/spans.parquet", span_block_schema(), &[batch])
        .await
        .unwrap();

    let back = read_block(store, "blocks/spans.parquet").await.unwrap();
    let total: usize = back.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total, 2);
    let tids = back[0]
        .column_by_name("trace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(tids.value(0), &[1u8; 16]);
}
```

> **`BlockWriter::write_block` note:** the blockstore plan's `write_block` calls `validate_block_schema` (the series declaration) + scans `series_fingerprint`/`timestamp` to summarize. For span blocks those columns are absent, so this slice must make `write_block` declaration-aware. Add a `write_block_with_decl(tenant, key, schema, &[batch], decl: &BlockSchema, summarize: SummaryColumns)` where `SummaryColumns { id_col, ts_col }` names the columns to scan for `fingerprints`/time bounds (`{series_fingerprint, timestamp}` for series; `{trace_id, start_unix_nano}` for spans — the `fingerprints` field of `BlockMeta` holds, for spans, the FNV-1 64-bit hashes of the distinct `trace_id`s so the existing `BlockMeta.fingerprints: Vec<u64>` shape is reused unchanged). Keep `write_block` as the series-declaration convenience wrapper. **If this is mechanically large, split it into its own task before Task 6** — the file set (`writer.rs`) does not overlap Tasks 6/7, so it can be a parallel sibling. Pin it with this round-trip test.

- [x] **Step 7: Run the round-trip test**

Run: `cargo test -p crabka-blockstore --test span_block_roundtrip`
Expected: PASS.

- [x] **Step 8: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): span-row builder (typed-list attrs + nested events/links) + write/read round-trip"
```

---

### Task 6: `ShardedTraceBloom` — FNV-1 32-bit sharded `trace_id` bloom

**Files:**
- Create: `crates/blockstore/src/bloom.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `serde::{Serialize, Deserialize}`.
- Produces:
  - `pub struct ShardedTraceBloom` (`Clone`, `Serialize`, `Deserialize`) — `shard_count` blooms; each a bit-array sized from `(expected_items, fp_rate)`.
  - `pub fn new(shard_count: usize, expected_items_per_shard: usize, fp_rate: f64) -> Self`
  - `pub fn with_tempo_defaults(expected_items: usize) -> Self` (`shard_count` chosen so each shard targets ~`100 KiB`; `fp_rate = 0.01`).
  - `pub fn insert(&mut self, trace_id: &[u8; 16])`
  - `pub fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool` (no false negatives; ~`fp_rate` false positives).
  - `pub fn shard_of(&self, trace_id: &[u8; 16]) -> usize` (`fnv1_32(trace_id) % shard_count`).
  - free fn `pub fn fnv1_32(bytes: &[u8]) -> u32`.

- [x] **Step 1: Write the failing tests**

Create `crates/blockstore/src/bloom.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn tid(n: u8) -> [u8; 16] {
        let mut t = [0u8; 16];
        t[0] = n;
        t[15] = n.wrapping_mul(7);
        t
    }

    #[test]
    fn no_false_negatives() {
        let mut b = ShardedTraceBloom::new(8, 64, 0.01);
        for n in 0..64u8 {
            b.insert(&tid(n));
        }
        for n in 0..64u8 {
            assert!(b.maybe_contains(&tid(n)));
        }
    }

    #[test]
    fn false_positive_rate_is_bounded() {
        let mut b = ShardedTraceBloom::new(16, 256, 0.01);
        for n in 0..=255u8 {
            b.insert(&tid(n));
        }
        // Probe 4096 ids that were never inserted; FP rate should be well under 5%.
        let mut fp = 0usize;
        let mut probes = 0usize;
        for n in 256u32..4352 {
            let mut t = [0u8; 16];
            t[0..4].copy_from_slice(&n.to_le_bytes());
            t[15] = 0xAB;
            probes += 1;
            if b.maybe_contains(&t) {
                fp += 1;
            }
        }
        let rate = fp as f64 / probes as f64;
        assert!(rate < 0.05);
    }

    #[test]
    fn shard_is_fnv_mod_count() {
        let b = ShardedTraceBloom::new(16, 64, 0.01);
        let t = tid(42);
        assert!(b.shard_of(&t) == (fnv1_32(&t) as usize) % 16);
    }

    #[test]
    fn fnv1_32_is_stable() {
        // FNV-1 32-bit of the single byte 0x00: offset*prime ^ 0  (FNV-1 order:
        // multiply THEN xor). offset=2166136261, prime=16777619.
        let h = fnv1_32(&[0u8]);
        let expected = 2166136261u32.wrapping_mul(16777619) ^ 0u32;
        assert!(h == expected);
    }

    #[test]
    fn snapshot_round_trips() {
        let mut b = ShardedTraceBloom::new(4, 32, 0.01);
        b.insert(&tid(1));
        let json = serde_json::to_vec(&b).unwrap();
        let back: ShardedTraceBloom = serde_json::from_slice(&json).unwrap();
        assert!(back.maybe_contains(&tid(1)));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib bloom`
Expected: FAIL — `cannot find type ShardedTraceBloom`.

- [x] **Step 3: Implement `bloom.rs`**

Prepend above the `tests` module. The bloom is a classic `k`-hash bit-array per shard; the `k` hashes are derived from two FNV-1 32-bit hashes (double-hashing: `h_i = h1 + i*h2`). No external bloom crate — keeps the slice self-contained and `unsafe`-free.

```rust
//! Per-block, FNV-sharded `trace_id` bloom for INDEX-LESS by-id retrieval. There
//! is no global `trace_id -> block` map: a by-id lookup time/block-prefilters,
//! then bloom-tests each surviving block, then does a Parquet row-group min/max
//! binary search inside the candidates. Shard = `fnv1_32(trace_id) % shard_count`
//! (matches Tempo's sharding). Crabka-owned encoding — no Tempo interop.

use serde::{Deserialize, Serialize};

/// FNV-1 32-bit hash (multiply-then-xor order; Tempo's `trace_id` sharder).
#[must_use]
pub fn fnv1_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;
    let mut hash = OFFSET;
    for &b in bytes {
        hash = hash.wrapping_mul(PRIME);
        hash ^= u32::from(b);
    }
    hash
}

/// FNV-1a 32-bit (xor-then-multiply) — the independent second hash for
/// double-hashing the `k` bloom probes.
fn fnv1a_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// One bloom shard: a bit-array + the number of hash probes `k`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BloomShard {
    bits: Vec<u64>,
    num_bits: u64,
    k: u32,
}

impl BloomShard {
    fn new(expected_items: usize, fp_rate: f64) -> Self {
        let n = expected_items.max(1) as f64;
        // Optimal m and k for a target FP rate.
        let m = (-(n * fp_rate.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2))
            .ceil()
            .max(64.0);
        let k = ((m / n) * std::f64::consts::LN_2).round().max(1.0) as u32;
        let num_bits = m as u64;
        let words = ((num_bits + 63) / 64) as usize;
        Self { bits: vec![0u64; words], num_bits, k }
    }

    fn probes(&self, trace_id: &[u8; 16]) -> impl Iterator<Item = u64> + '_ {
        let h1 = u64::from(fnv1_32(trace_id));
        let h2 = u64::from(fnv1a_32(trace_id)) | 1; // odd, so it strides the space
        let num_bits = self.num_bits;
        (0..u64::from(self.k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % num_bits)
    }

    fn insert(&mut self, trace_id: &[u8; 16]) {
        for bit in self.probes(trace_id) {
            self.bits[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
    }

    fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool {
        self.probes(trace_id)
            .all(|bit| self.bits[(bit / 64) as usize] & (1u64 << (bit % 64)) != 0)
    }
}

/// A `trace_id` bloom split across `shard_count` shards (FNV-1 32-bit sharder).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardedTraceBloom {
    shards: Vec<BloomShard>,
}

impl ShardedTraceBloom {
    #[must_use]
    pub fn new(shard_count: usize, expected_items_per_shard: usize, fp_rate: f64) -> Self {
        let shard_count = shard_count.max(1);
        Self {
            shards: (0..shard_count)
                .map(|_| BloomShard::new(expected_items_per_shard, fp_rate))
                .collect(),
        }
    }

    /// Tempo defaults: `fp_rate = 0.01`; `shard_count` sized so each shard's
    /// bit-array targets ~100 KiB given `expected_items` total. With `0.01` FP a
    /// bloom needs ~9.6 bits/item, so ~100 KiB (819 200 bits) per shard holds
    /// ~85 000 items; pick `shard_count = ceil(expected_items / 85_000)`.
    #[must_use]
    pub fn with_tempo_defaults(expected_items: usize) -> Self {
        const ITEMS_PER_100KIB_SHARD: usize = 85_000;
        let shard_count = expected_items.div_ceil(ITEMS_PER_100KIB_SHARD).max(1);
        let per_shard = expected_items.div_ceil(shard_count).max(1);
        Self::new(shard_count, per_shard, 0.01)
    }

    #[must_use]
    pub fn shard_of(&self, trace_id: &[u8; 16]) -> usize {
        (fnv1_32(trace_id) as usize) % self.shards.len()
    }

    pub fn insert(&mut self, trace_id: &[u8; 16]) {
        let s = self.shard_of(trace_id);
        self.shards[s].insert(trace_id);
    }

    #[must_use]
    pub fn maybe_contains(&self, trace_id: &[u8; 16]) -> bool {
        let s = self.shard_of(trace_id);
        self.shards[s].maybe_contains(trace_id)
    }
}
```

> **Production swap (flagged, not done here):** Tempo uses a split-block bloom filter (SBBF). parquet 59 ships `parquet::bloom_filter::Sbbf`, and `fastbloom` is the well-known crate. This inline FNV double-hash bloom keeps the slice self-contained, dependency-free, and property-testable; swapping to `Sbbf` later is a localized change behind `ShardedTraceBloom` (the public method set — `insert`/`maybe_contains`/`shard_of` — stays). Do **not** add an external bloom dep in this slice.

- [x] **Step 4: Wire `lib.rs`**

Add `mod bloom;` and `pub use bloom::{ShardedTraceBloom, fnv1_32};`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib bloom`
Expected: PASS (5 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): ShardedTraceBloom — FNV-1 32-bit sharded trace_id bloom for index-less by-id"
```

---

### Task 7: `TraceIndex` (impl `BlockIndex`) — sharded bloom + tag sets/blooms + by-id candidates + tag pruning + snapshot

**Files:**
- Create: `crates/blockstore/src/trace_index.rs`
- Modify: `crates/blockstore/src/lib.rs`

**Interfaces:**
- Consumes: `BlockIndex`, `BlockMeta`, `ShardedTraceBloom`, `Result`, `BlockStoreError`, `serde`.
- Produces:
  - `pub struct TraceIndex` (`Default`, `Serialize`, `Deserialize`, `impl BlockIndex`).
  - `pub struct TraceBlockStats { pub object_key:String, pub min_ts:i64, pub max_ts:i64, pub bloom:ShardedTraceBloom, pub tag_names:BTreeSet<String>, pub tag_values:BTreeMap<String,BTreeSet<String>> }` — the per-block footprint the block-builder (slice 4) computes and registers.
  - `pub fn new() -> Self`
  - `pub fn add_trace_block(&mut self, tenant: &str, stats: TraceBlockStats)`
  - `pub fn candidate_blocks_for_trace(&self, tenant:&str, trace_id:&[u8;16], min_ts:i64, max_ts:i64) -> Vec<String>` — the **index-less by-id locate**: time prefilter → bloom test (NO global map).
  - `pub fn prune_blocks_by_tag(&self, tenant:&str, tag:&str, value:Option<&str>, min_ts:i64, max_ts:i64) -> Vec<String>` — blocks whose tag set (and optional value set) can contain `tag`(`=value`); for TraceQL search pruning.
  - `pub fn tag_names(&self, tenant:&str, min_ts:i64, max_ts:i64) -> Vec<String>` and `pub fn tag_values(&self, tenant:&str, tag:&str, min_ts:i64, max_ts:i64) -> Vec<String>` — tag discovery (union across blocks in window).
  - `pub async fn save(&self, store:&Arc<dyn ObjectStore>, key:&str) -> Result<()>` / `pub async fn load(store:&Arc<dyn ObjectStore>, key:&str) -> Result<TraceIndex>`.

- [x] **Step 1: Write the failing tests**

Create `crates/blockstore/src/trace_index.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use assert2::assert;

    use super::*;
    use crate::bloom::ShardedTraceBloom;

    fn tid(n: u8) -> [u8; 16] {
        let mut t = [0u8; 16];
        t[0] = n;
        t
    }

    fn stats(key: &str, min: i64, max: i64, traces: &[u8], tags: &[(&str, &str)]) -> TraceBlockStats {
        let mut bloom = ShardedTraceBloom::new(8, 64, 0.01);
        for &n in traces {
            bloom.insert(&tid(n));
        }
        let mut tag_names = BTreeSet::new();
        let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (k, v) in tags {
            tag_names.insert((*k).to_string());
            tag_values.entry((*k).to_string()).or_default().insert((*v).to_string());
        }
        TraceBlockStats {
            object_key: key.to_string(),
            min_ts: min,
            max_ts: max,
            bloom,
            tag_names,
            tag_values,
        }
    }

    fn seed() -> TraceIndex {
        let mut idx = TraceIndex::new();
        idx.add_trace_block("t", stats("b1", 0, 100, &[1, 2], &[("service.name", "api")]));
        idx.add_trace_block("t", stats("b2", 200, 300, &[3], &[("service.name", "web")]));
        idx
    }

    #[test]
    fn by_id_locate_uses_bloom_and_time_no_global_map() {
        let idx = seed();
        // trace 1 is in b1's bloom and time window.
        let got = idx.candidate_blocks_for_trace("t", &tid(1), 0, 1_000);
        assert!(got == vec!["b1".to_string()]);
        // trace 3 is in b2 only.
        let got = idx.candidate_blocks_for_trace("t", &tid(3), 0, 1_000);
        assert!(got == vec!["b2".to_string()]);
        // time window excludes b1.
        let got = idx.candidate_blocks_for_trace("t", &tid(1), 500, 1_000);
        assert!(got.is_empty());
    }

    #[test]
    fn tag_pruning_keeps_only_blocks_that_can_contain_the_tag_value() {
        let idx = seed();
        let got = idx.prune_blocks_by_tag("t", "service.name", Some("api"), 0, 1_000);
        assert!(got == vec!["b1".to_string()]);
        let got = idx.prune_blocks_by_tag("t", "service.name", Some("web"), 0, 1_000);
        assert!(got == vec!["b2".to_string()]);
        // value absent from all blocks → no candidates.
        let got = idx.prune_blocks_by_tag("t", "service.name", Some("nope"), 0, 1_000);
        assert!(got.is_empty());
    }

    #[test]
    fn tag_discovery_unions_blocks_in_window() {
        let idx = seed();
        let names = idx.tag_names("t", 0, 1_000);
        assert!(names == vec!["service.name".to_string()]);
        let mut vals = idx.tag_values("t", "service.name", 0, 1_000);
        vals.sort();
        assert!(vals == vec!["api".to_string(), "web".to_string()]);
    }

    #[test]
    fn block_index_trait_prefilter_is_time_only() {
        use crate::block_index::BlockIndex;
        let idx = seed();
        let mut got = BlockIndex::candidate_blocks(&idx, "t", 0, 1_000);
        got.sort();
        assert!(got == vec!["b1".to_string(), "b2".to_string()]);
        assert!(idx.block_count("t") == 2);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-blockstore --lib trace_index`
Expected: FAIL — `cannot find type TraceIndex`.

- [x] **Step 3: Implement `trace_index.rs`**

Prepend above the `tests` module:

```rust
//! The traces `BlockIndex` impl. There is NO global `trace_id -> block` map.
//! Each block carries (a) a sharded `trace_id` bloom for index-less by-id locate,
//! and (b) tag-name/value sets for TraceQL search pruning + tag discovery. The
//! by-id path is time/block prefilter -> bloom test -> (caller does Parquet
//! row-group min/max binary search inside the surviving blocks).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::block::BlockMeta;
use crate::block_index::BlockIndex;
use crate::bloom::ShardedTraceBloom;
use crate::error::Result;

/// The per-block footprint the block-builder (slice 4) computes and registers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceBlockStats {
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bloom: ShardedTraceBloom,
    pub tag_names: BTreeSet<String>,
    pub tag_values: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Default, Serialize, Deserialize)]
struct TenantTraceIndex {
    blocks: Vec<TraceBlockStats>,
}

/// Traces index: per-tenant lists of per-block stats. Snapshotted to object
/// storage alongside the blocks.
#[derive(Default, Serialize, Deserialize)]
pub struct TraceIndex {
    tenants: HashMap<String, TenantTraceIndex>,
}

impl TraceIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a written span block.
    pub fn add_trace_block(&mut self, tenant: &str, stats: TraceBlockStats) {
        self.tenants
            .entry(tenant.to_string())
            .or_default()
            .blocks
            .push(stats);
    }

    /// INDEX-LESS by-id locate: blocks whose time range overlaps `[min_ts,
    /// max_ts]` AND whose bloom says `trace_id` may be present. No global map.
    #[must_use]
    pub fn candidate_blocks_for_trace(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .filter(|b| b.bloom.maybe_contains(trace_id))
            .map(|b| b.object_key.clone())
            .collect()
    }

    /// Search pruning: blocks whose tag set contains `tag` (and, if `value` is
    /// given, whose value set for `tag` contains it), within the time window.
    #[must_use]
    pub fn prune_blocks_by_tag(
        &self,
        tenant: &str,
        tag: &str,
        value: Option<&str>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .filter(|b| {
                if !b.tag_names.contains(tag) {
                    return false;
                }
                match value {
                    None => true,
                    Some(v) => b.tag_values.get(tag).is_some_and(|vs| vs.contains(v)),
                }
            })
            .map(|b| b.object_key.clone())
            .collect()
    }

    /// Tag discovery: union of tag names across blocks in the time window.
    #[must_use]
    pub fn tag_names(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out: BTreeSet<String> = BTreeSet::new();
        for b in &t.blocks {
            if b.min_ts <= max_ts && b.max_ts >= min_ts {
                out.extend(b.tag_names.iter().cloned());
            }
        }
        out.into_iter().collect()
    }

    /// Tag discovery: union of values for `tag` across blocks in the window.
    #[must_use]
    pub fn tag_values(&self, tenant: &str, tag: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out: BTreeSet<String> = BTreeSet::new();
        for b in &t.blocks {
            if b.min_ts <= max_ts && b.max_ts >= min_ts {
                if let Some(vs) = b.tag_values.get(tag) {
                    out.extend(vs.iter().cloned());
                }
            }
        }
        out.into_iter().collect()
    }

    /// Persist the index as a JSON snapshot to object storage.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        store
            .put(&Path::from(key), object_store::PutPayload::from(bytes))
            .await?;
        Ok(())
    }

    /// Load an index JSON snapshot from object storage.
    pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<TraceIndex> {
        let bytes = store.get(&Path::from(key)).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

impl BlockIndex for TraceIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        // The trait-level `add_block` only has time bounds + key (no bloom/tags).
        // The block-builder uses `add_trace_block` with the full `TraceBlockStats`;
        // this trait method registers a bloom-less, tag-less placeholder so the
        // facade's generic path still time-prefilters. (Slice 4 always calls
        // `add_trace_block`.)
        self.tenants
            .entry(meta.tenant.clone())
            .or_default()
            .blocks
            .push(TraceBlockStats {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom: ShardedTraceBloom::new(1, 1, 0.01),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            });
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .map(|b| b.object_key.clone())
            .collect()
    }

    fn block_count(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.blocks.len())
    }
}
```

- [x] **Step 4: Wire `lib.rs`**

Add `mod trace_index;` and `pub use trace_index::{TraceBlockStats, TraceIndex};`. Confirm `BlockStore<TraceIndex>` type-checks (the facade is generic over `BlockIndex`; the traces querier in slice 5 will add a `BlockStore<TraceIndex>` inherent `impl` with a span-aware scan method).

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-blockstore --lib trace_index`
Expected: PASS (4 tests).

- [x] **Step 6: Snapshot round-trip test**

Append to the `tests` module in `trace_index.rs`:

```rust
    #[tokio::test]
    async fn snapshot_round_trips() {
        use object_store::ObjectStore;
        use object_store::memory::InMemory;

        let idx = seed();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        idx.save(&store, "index/traces.json").await.unwrap();
        let loaded = TraceIndex::load(&store, "index/traces.json").await.unwrap();
        let got = loaded.candidate_blocks_for_trace("t", &tid(1), 0, 1_000);
        assert!(got == vec!["b1".to_string()]);
    }
```

- [x] **Step 7: Run the snapshot test**

Run: `cargo test -p crabka-blockstore --lib trace_index::tests::snapshot_round_trips`
Expected: PASS.

- [x] **Step 8: Commit**

```bash
cargo fmt -p crabka-blockstore
cargo clippy -p crabka-blockstore --all-targets
git add crates/blockstore/
git commit -m "feat(blockstore): TraceIndex — sharded trace_id bloom + tag sets for index-less by-id + search pruning"
```

---

### Task 8: Whole-crate integration — span block + nested-set + TraceIndex end-to-end + final gate

**Files:**
- Create: `crates/blockstore/tests/trace_pipeline_e2e.rs`

**Interfaces:**
- Consumes the full public surface: `assign_nested_set`, `SpanNode`, `encode_span_rows`, `SpanRow`, `BlockWriter`, `read_block`, `span_block_schema`, `TraceIndex`, `TraceBlockStats`, `ShardedTraceBloom`, `SpanKind`, `StatusCode`, `NestedSet`, `AttrValue`, `SpanAttr`.

- [x] **Step 1: Write the end-to-end test**

This exercises the slice's headline path: build a trace's spans → compute nested-set via DFS → encode a span block → write through `BlockWriter` → register a `TraceBlockStats` (bloom + tags) in `TraceIndex` → by-id locate the block via the bloom (no global map) → read it back and confirm the nested-set columns satisfy interval containment.

Create `crates/blockstore/tests/trace_pipeline_e2e.rs`:

```rust
//! End-to-end (slice 1): DFS nested-set -> span block -> BlockWriter -> TraceIndex
//! bloom locate -> read back. Proves the by-id path is index-less (bloom only).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::Int32Array;
use crabka_blockstore::{
    AttrValue, BlockWriter, NestedSet, ShardedTraceBloom, SpanAttr, SpanKind, SpanNode, SpanRow,
    StatusCode, TraceBlockStats, TraceIndex, assign_nested_set, encode_span_rows, read_block,
    span_block_schema,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

fn sid(n: u8) -> [u8; 8] {
    [n, 0, 0, 0, 0, 0, 0, 0]
}

#[tokio::test]
async fn trace_block_built_indexed_and_located_by_id() {
    let trace_id = [9u8; 16];

    // Trace: root(1) -> child(2) -> grandchild(3).
    let nodes = vec![
        SpanNode { span_id: sid(1), parent_span_id: None },
        SpanNode { span_id: sid(2), parent_span_id: Some(sid(1)) },
        SpanNode { span_id: sid(3), parent_span_id: Some(sid(2)) },
    ];
    let ns = assign_nested_set(&nodes);

    let rows: Vec<SpanRow> = nodes
        .iter()
        .zip(&ns)
        .enumerate()
        .map(|(i, (node, nset))| SpanRow {
            trace_id,
            span_id: node.span_id,
            parent_span_id: node.parent_span_id,
            nested_set: *nset,
            root_service_name: Some("checkout".into()),
            root_span_name: Some("POST /pay".into()),
            trace_start_unix_nano: 1_000,
            trace_duration_nanos: 300,
            name: Some(format!("span-{i}")),
            kind: SpanKind::Server,
            start_unix_nano: 1_000 + i as i64,
            duration_nanos: 10,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![SpanAttr {
                key: "service.name".into(),
                is_array: false,
                value: AttrValue::Str(vec!["checkout".into()]),
            }],
            events: vec![],
            links: vec![],
        })
        .collect();

    let batch = encode_span_rows(&rows).unwrap();

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    BlockWriter::new(store.clone())
        .write_block("tenant", "blocks/t.parquet", span_block_schema(), &[batch])
        .await
        .unwrap();

    // Build the TraceIndex footprint (what slice 4's block-builder will do).
    let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
    bloom.insert(&trace_id);
    let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    tag_values
        .entry("service.name".into())
        .or_default()
        .insert("checkout".into());
    let mut idx = TraceIndex::new();
    idx.add_trace_block(
        "tenant",
        TraceBlockStats {
            object_key: "blocks/t.parquet".into(),
            min_ts: 1_000,
            max_ts: 1_300,
            bloom,
            tag_names: BTreeSet::from(["service.name".to_string()]),
            tag_values,
        },
    );

    // INDEX-LESS by-id locate: the bloom (not a global map) finds the block.
    let candidates = idx.candidate_blocks_for_trace("tenant", &trace_id, 0, 10_000);
    assert_eq!(candidates, vec!["blocks/t.parquet".to_string()]);

    // A different trace_id the bloom never saw must not (almost surely) match.
    let other = [42u8; 16];
    assert!(idx.candidate_blocks_for_trace("tenant", &other, 0, 10_000).is_empty());

    // Read the located block back; nested-set columns satisfy containment.
    let back = read_block(store, &candidates[0]).await.unwrap();
    let b = &back[0];
    let left = b
        .column_by_name("nested_set_left")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let right = b
        .column_by_name("nested_set_right")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    // Root (row 0) interval strictly contains the grandchild (row 2).
    assert!(left.value(0) < left.value(2) && right.value(2) < right.value(0));
}
```

- [x] **Step 2: Run the end-to-end test**

Run: `cargo test -p crabka-blockstore --test trace_pipeline_e2e`
Expected: PASS.

> If `candidate_blocks_for_trace` for the `other` id flakes to non-empty (a bloom false positive against a single-item bloom is vanishingly unlikely but possible), pick a different `other` constant — the assertion documents intent; a 1-in-N FP is acceptable bloom behavior and not a regression. With `with_tempo_defaults(1)` the bloom is sized for one item, so FP is far below 1%.

- [x] **Step 3: Final whole-crate gate**

Run: `cargo test -p crabka-blockstore && cargo clippy -p crabka-blockstore --all-targets && cargo fmt -p crabka-blockstore --check`
Expected: all PASS (every pre-existing `SeriesIndex`/`BlockStore` test + all new span/nested-set/bloom/trace-index tests), no clippy warnings, formatting clean.

- [x] **Step 4: Commit**

```bash
git add crates/blockstore/
git commit -m "test(blockstore): slice-1 end-to-end — DFS nested-set -> span block -> TraceIndex bloom locate"
```

---

## Self-review

**Spec coverage (against §3.3 Decision A + §4 data model + §11 Slice 1):**
- Extract `BlockIndex` trait; relax mandatory `series_fingerprint`+`timestamp` to a per-signal `BlockSchema` declaration → Task 1.
- Existing logs/metrics index becomes `SeriesIndex` (impl `BlockIndex`), **no behavior change, regression-tested**; `BlockStore` parameterized over `BlockIndex` → Task 2.
- Flattened span-per-row block schema — identity (FixedSizeBinary), nested-set columns (Int32), trace-denormalized, intrinsics, generic typed-list attrs (`List<List<T>>`, `is_array`, scalar=single-element list), nested events/links (`List<Struct>`) → Tasks 3, 5. (Dedicated/promoted attribute columns are *configuration* applied at block-build by slice 4's block-builder over this schema's column set — the schema supports promoted columns as ordinary extra dict-encoded columns; the promotion *policy* is slice 4. Flagged below.)
- Nested-set DFS pre-order builder (the headline) + interval-containment property tests → Task 4.
- `TraceIndex` (impl `BlockIndex`): FNV-1 32-bit sharded `trace_id` bloom for **index-less** by-id (time/block prefilter → bloom test → caller's Parquet row-group min/max binary search; **no global `trace_id → block` map**) + per-block tag-name/value sets for search pruning + tag discovery → Tasks 6, 7.
- Span-block write/read path through `BlockWriter` + `TraceIndex` queries; end-to-end → Tasks 5, 8.

**Deviations flagged (not hidden):**
1. **Attribute promotion policy** is slice 4, not here. The span schema carries the *generic* typed-list attr columns; a *promoted* attr is just an extra dict-encoded column the block-builder hoists at write time. This slice provides the column-set the promotion writes into (ordinary Arrow columns alongside `span_block_schema()`), but the per-tenant promotion config + the hoisting logic belong to the block-builder (§5.3, slice 4). The schema is open to extra columns (Parquet/DataFusion read by name), so promotion needs no schema change.
2. **The Parquet row-group min/max binary search** inside a bloom-surviving block is the *caller's* final by-id step (slice 5 querier), driven by DataFusion's native page statistics on the `trace_id` column. This slice delivers the locate-to-block half (bloom + time); the within-block half is a DataFusion predicate the querier pushes down. Pinning it needs a registered Parquet table (the querier's job), so it is correctly deferred — flagged so a reviewer doesn't expect the binary search here.
3. **`write_block` declaration-awareness** (Task 5 Step 6 note): the blockstore plan's `write_block` hardcodes the series columns for its summary scan. Span blocks need a declaration-aware variant (`write_block_with_decl` / a `SummaryColumns { id_col, ts_col }`). This is called out as a potential standalone task (non-overlapping file set, `writer.rs`) if mechanically large; it is pinned by the Task 5 round-trip + Task 8 e2e tests rather than left vague.
4. **Bloom implementation** is an inline FNV double-hash bit-array, not Tempo's SBBF. Deliberate: self-contained, dependency-free, property-testable; production swap to `parquet::bloom_filter::Sbbf` or `fastbloom` is a localized change behind `ShardedTraceBloom`'s frozen method set. Flagged in Task 6.

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every step has runnable code or an exact command. The hand-waves are bounded: (a) the arrow-59 `List<Struct>`/`List<List<T>>` builder method names (Task 5) — pinned by `batch.schema() == span_block_schema()` + column-value asserts and a verify-against-arrow-59 note; (b) the `BlockStore<I>` generic refactor's possible trait-vs-inherent `candidate_blocks` ambiguity (Task 2) — flagged with the disambiguation. Neither fabricates a signature whose behavior isn't test-pinned.

**Type consistency:** `BlockIndex` method set (`add_block`/`candidate_blocks`/`block_count`) identical across the trait (Task 1) and both impls (Tasks 2, 7). `NestedSet` fields (`nested_set_left`/`nested_set_right`/`parent_id`) identical across Tasks 4, 5, 8. `SpanRow`/`SpanAttr`/`AttrValue`/`SpanEvent`/`SpanLink` field sets identical between definition (Task 5) and use (Tasks 5, 8). `SCOL_*` constants defined once (Task 3), referenced unchanged in Tasks 5, 8. `ShardedTraceBloom` method set (`new`/`with_tempo_defaults`/`insert`/`maybe_contains`/`shard_of`) consistent between Tasks 6 and 7/8. `TraceBlockStats` field set identical between Task 7 definition and Task 8 construction.

**Known risk (flagged, not hidden):** the `Index` → `SeriesIndex` rename + `BlockStore<I>` parameterization (Task 2) touches `index.rs` and `store.rs` together — it is the one task that genuinely depends on Task 1 and cannot run in a parallel batch with it. Tasks 3, 4, 6 (`span_schema.rs`, `nested_set.rs`, `bloom.rs`) are pure-new files with no overlap and **can be dispatched as a parallel subagent batch** after Task 1 lands; Tasks 5 and 7 depend on 3/4 and 6 respectively; Task 8 depends on all. The regression net (every pre-existing blockstore test staying green through Task 2) is the guard that the `BlockIndex` extraction is behavior-preserving for logs/metrics.
