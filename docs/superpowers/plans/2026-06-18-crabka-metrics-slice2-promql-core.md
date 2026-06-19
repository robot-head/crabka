# crabka-metrics Slice 2 — `crabka-promql` core (parser + operator pattern + selectors + rate-family + aggregations + binary ops + `.test` harness)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **This slice is the biggest in the metrics program and may itself be executed as sub-batches.** It is organized into five phases (A–E). Within a phase, tasks whose **Files** sets do not overlap may be dispatched as a parallel subagent batch (per `CLAUDE.md`); tasks that share a file (`lib.rs`, `engine.rs`) or genuinely depend on an earlier task's output must run sequentially. Each phase ends at a green whole-crate gate. The recommended batching is called out at the head of each phase.

**Goal:** Build the core of the PromQL engine — parse PromQL with `promql-parser`, lower the AST onto a DataFusion `LogicalPlan` using the GreptimeDB-proven custom range-vector operator pattern (`SeriesDivide`/`SeriesNormalize`/`InstantManipulate`/`RangeManipulate` + the `RangeArray` Arrow array), implement the rate-family ScalarUDFs (the byte-exact counter-reset + extrapolation algorithm), the core aggregations (`sum`/`avg`/`min`/`max`/`count` with `by`/`without`), and binary ops (arithmetic + comparison with `on`/`ignoring` one-to-one matching and the `bool` modifier) — then drive it from `query_instant`/`query_range` over a step grid honoring lookback-delta + staleness, assemble `QueryResult`, and verify with a Prometheus `.test` conformance harness over an `InMemoryMetricStore`. The long-tail function catalog, `histogram_quantile`, and subqueries are deferred to Slice 3.

**Architecture:** A query crate `crabka-promql` that depends on DataFusion (same git pin as blockstore) and `promql-parser`. The engine is generic over a `MetricStore` trait that yields a DataFusion `SessionContext` with float and/or histogram tables registered for a (tenant, matchers, time-range) scan — production wires this to `crabka-blockstore::BlockStore::scan_context` (Slice 5), but this slice ships an `InMemoryMetricStore` test impl so the engine is independently testable. Range-vector semantics have no native DataFusion equivalent, so we reimplement them as four `UserDefinedLogicalNodeCore` operators each with a matching `ExecutionPlan` + `RecordBatchStream`, fed by a custom list-like `RangeArray` Arrow array (each cell is a slice of a contiguous backing array = "the samples in this step's lookback window"). `rate`/`increase`/`delta`/`irate`/`idelta` are `ScalarUDF`s over the `RangeArray`-paired (timestamps, values) columns, not UDAFs. The planner recurses the AST into a `LogicalPlan`; `query_instant`/`query_range` build the step grid, execute the plan, and assemble a Prometheus-shaped `QueryResult`.

**Tech Stack:** Rust 2024 · `datafusion` (git `main`, pinned — see Global Constraints) · `arrow` 59 · `promql-parser` 0.10 · `async-trait` · `tokio` · `futures` · `thiserror`. Depends on `crabka-blockstore` (types: `LabelMatcher`, `MatchOp`, `Labels`, `SeriesFingerprint`) and `crabka-metrics` (Slice 1: `NativeHistogram`, schema builders, codecs, `COL_FINGERPRINT`/`COL_TIMESTAMP`). Tests: `assert2`, `proptest`, `tokio` (`macros`, `rt-multi-thread`).

## Global Constraints

- **No backwards compatibility.** Crabka is greenfield/undeployed. No `#[serde(default)]` shims, no V2-alongside-V1 enum variants, no migration code, no default-off feature gates. Change schemas/enums/interfaces freely. (Only Kafka wire compat matters — and this crate touches none of it.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe` — including in the custom `RangeArray` (build it on safe arrow buffer/offset APIs).
- **Lints:** `clippy::pedantic` is `warn` workspace-wide (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` allowed). New code must be clippy-pedantic clean. Run `cargo clippy -p crabka-promql --all-targets` before each commit.
- **Formatting:** run `cargo fmt -p crabka-promql` before every commit. **NEVER** run `cargo +nightly fmt --all` — it fails with OS error 206 / path-too-long in deep worktrees on Windows; always scope with `-p`.
- **Assertions:** use `assert2::assert!` / `assert2::check!` in tests, `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`. This `main` revision tracks arrow 59 / parquet 59 / object_store 0.13.2, which unify with the workspace pins (same major → cargo unifies to one crate instance, so arrow types cross the DataFusion boundary cleanly). Do **not** substitute a released `datafusion` (54.x is on arrow 58 and pulls a second, incompatible arrow major). `promql-parser = "0.10"` is parser-only (no arrow/DataFusion deps) — it cannot cause a version clash.
- **Arrow version identity:** import `arrow` directly (`use arrow::...`) as blockstore/metrics do; all of arrow/parquet/object_store unify to one instance. If a type-mismatch error ever appears at the DataFusion boundary, switch that import to DataFusion's re-export (`datafusion::arrow`) to force identity.
- **Churn-prone DataFusion-internal traits.** `UserDefinedLogicalNodeCore`, `ExecutionPlan`, `RecordBatchStream`, `ScalarUDFImpl`, and the `Array`/`ArrayData` plumbing for a custom array change shape between DataFusion/arrow revisions. **Do not fabricate exact trait method signatures.** Where this plan shows operator/UDF/array scaffolding it gives the *struct shape, field set, and a behavior-pinning test*, plus a **"verify against datafusion rev `0838a4d` / the GreptimeDB `src/promql/src/extension_plan/` source"** note. The test pins behavior; if a trait method's signature differs at the pinned rev, adapt the impl to satisfy the test — never change the asserted behavior. The reference implementation to mirror is GreptimeDB's `promql` crate (`extension_plan/{series_divide,normalize,instant_manipulate,range_manipulate}.rs` + `range_array.rs` + `functions/`), Apache-2.0; read it at a commit whose DataFusion is close to our pin and translate, do not copy verbatim.
- **Counter-reset/extrapolation is the #1 correctness trap.** `rate`/`increase` must match Prometheus byte-for-byte: reset-correct on any decrease, `avgDurationBetweenSamples = sampledInterval / (n-1)`, `1.1×` boundary-extension threshold, extrapolation capped at half the average interval on each side, positive-counter zero-anchor clamp (if the extrapolated start goes below zero for an all-non-negative series, clamp the left extension to the distance to zero). Implement against spec §6.2 and pin with literal-value tests drawn from Prometheus's own `functions.test`.

---

## Dependency & slice roadmap

**Depends on:**
- `crabka-blockstore` (built — logs wedge Phase 1): `BlockStore`, `BlockStore::scan_context(tenant, &[LabelMatcher], min_ts: i64, max_ts: i64, schema: SchemaRef) -> Result<(SessionContext, String)>`, `Index`, `Labels`, `SeriesFingerprint = u64`, `LabelMatcher { name, op, value }`, `MatchOp { Eq, Neq, Re, Nre }`. Mandatory block columns `series_fingerprint: UInt64`, `timestamp: Int64`. **This slice consumes only the *types* (`LabelMatcher`/`MatchOp`/`Labels`/`SeriesFingerprint`)** — the `BlockStore`-backed `MetricStore` impl lands in Slice 5; here we ship `InMemoryMetricStore`.
- `crabka-metrics` (built — Slice 1): `NativeHistogram`, `BucketSpan`, `ResetHint`, `float_sample_schema()` / `native_histogram_schema()`, `encode/decode_native_histograms`, `encode/decode_float_samples`, `COL_FINGERPRINT` / `COL_TIMESTAMP`.

**The 8 metrics slices** (this plan = Slice 2; each later slice gets its own plan):

1. **Data layer** — block schemas + native-histogram codec + symbol table. *(built)*
2. **`crabka-promql` core** *(this plan)* — parser + operator pattern + selectors + rate-family + aggregations + binary ops + `.test` harness.
3. **Query completeness** — `histogram_quantile` (classic + native), full function catalog, subqueries, `@`/`offset` general form. **Reuses this slice's operators (`SeriesDivide`/`SeriesNormalize`/`InstantManipulate`/`RangeManipulate`/`RangeArray`), the `InMemoryMetricStore` test double, and the `.test` DSL harness (`crabka_promql::testkit::{run_test_file(&TestFile), run_test_path(&str)}`) — those public names are frozen here.**
4. **Ingest service** — remote_write v1/v2 + OTLP + Kafka produce + distributor + HA dedup + compactor.
5. **Querier + Prometheus HTTP API** + hot/cold merge. **Replaces `InMemoryMetricStore` with a `BlockStore`-backed `MetricStore` — the trait is frozen here.**
6. **Query-frontend** — split / shard / cache.
7. **Ruler** — recording + alerting + rule API.
8. **Hardening** — multi-tenancy/limits, remote_read, prometheus/compliance + differential-vs-Mimir.

---

## Shared cross-slice contract (frozen here — later slices interlock on these exact names)

```rust
// ---- trait + scan result (Slice 5 reimplements MetricStore over BlockStore) ----
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
    async fn scan(&self, tenant: &str, matchers: &[LabelMatcher], start_ms: i64, end_ms: i64)
        -> Result<ScanResult, PromqlError>;
    async fn label_names(&self, tenant: &str, matchers: &[LabelMatcher], start_ms: i64, end_ms: i64)
        -> Result<Vec<String>, PromqlError>;
    async fn label_values(&self, tenant: &str, name: &str, matchers: &[LabelMatcher], start_ms: i64, end_ms: i64)
        -> Result<Vec<String>, PromqlError>;
    async fn series(&self, tenant: &str, matchers: &[LabelMatcher], start_ms: i64, end_ms: i64)
        -> Result<Vec<Labels>, PromqlError>;
}

pub struct ScanResult {
    pub ctx: SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,
}

// ---- engine ----
pub struct EngineOpts { pub lookback_delta_ms: i64 /* default 300_000 */, pub max_samples: usize }
pub struct PromqlEngine<S: MetricStore> { /* store: Arc<S>, opts: EngineOpts */ }
impl<S: MetricStore> PromqlEngine<S> {
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self;
    pub async fn query_instant(&self, tenant: &str, query: &str, time_ms: i64)
        -> Result<QueryResult, PromqlError>;
    pub async fn query_range(&self, tenant: &str, query: &str, start_ms: i64, end_ms: i64, step_ms: i64)
        -> Result<QueryResult, PromqlError>;
}

// ---- result model ----
pub enum SampleValue { Float(f64), Histogram(NativeHistogram) }
pub struct InstantSample { pub labels: Labels, pub ts_ms: i64, pub value: SampleValue }
pub struct RangeSeries { pub labels: Labels, pub samples: Vec<(i64, SampleValue)> }
pub enum QueryResult {
    Scalar { ts_ms: i64, value: f64 },
    InstantVector(Vec<InstantSample>),
    RangeMatrix(Vec<RangeSeries>),
    Str { ts_ms: i64, value: String },
}

// ---- error ----
pub enum PromqlError { Parse(String), Plan(String), Exec(String), Store(String), Unsupported(String) }

// ---- internal custom DataFusion operators (Slice 3 reuses) ----
//   SeriesDivide, SeriesNormalize, InstantManipulate, RangeManipulate
//     (each UserDefinedLogicalNodeCore + matching ExecutionPlan + stream)
//   RangeArray (custom Arrow array)
//   rate-family ScalarUDFs: rate / increase / delta / irate / idelta
// ---- parse entry ----
//   promql_parser::parser::parse(&str) -> Result<promql_parser::parser::Expr, String>
// ---- .test DSL harness (Slice 3 drives the full corpus through these) ----
//   pub mod testkit {
//       pub async fn run_test_file(file: &TestFile) -> Result<(), PromqlError>;
//       pub async fn run_test_path(path: &str)  -> Result<(), PromqlError>;
//   }
//   (both also re-exported at the crate root: crabka_promql::{run_test_file, run_test_path})
```

---

## File structure (`crates/promql/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest; workspace deps |
| `src/lib.rs` | module decls + public re-exports + crate docs |
| `src/error.rs` | `PromqlError` enum + `From` conversions |
| `src/result.rs` | `QueryResult`, `InstantSample`, `RangeSeries`, `SampleValue` |
| `src/store.rs` | `MetricStore` trait, `ScanResult` |
| `src/in_memory.rs` | `InMemoryMetricStore` test impl (float + histogram DF tables) |
| `src/range_array.rs` | the custom `RangeArray` Arrow array |
| `src/extension/mod.rs` | operator module wiring + `PromqlExtensionPlanner` |
| `src/extension/series_divide.rs` | `SeriesDivide` node + exec + stream |
| `src/extension/normalize.rs` | `SeriesNormalize` node + exec + stream |
| `src/extension/instant_manipulate.rs` | `InstantManipulate` node + exec + stream |
| `src/extension/range_manipulate.rs` | `RangeManipulate` node + exec + stream |
| `src/functions/mod.rs` | UDF registry wiring |
| `src/functions/extrapolate.rs` | the shared counter-reset + extrapolation core |
| `src/functions/rate.rs` | `rate`/`increase`/`delta`/`irate`/`idelta` ScalarUDFs |
| `src/planner/mod.rs` | `PromqlPlanner` entry + AST recursion |
| `src/planner/selector.rs` | instant + range (matrix) vector selector lowering |
| `src/planner/aggregate.rs` | `sum`/`avg`/`min`/`max`/`count` with `by`/`without` |
| `src/planner/binary.rs` | arithmetic + comparison binary ops + vector matching |
| `src/engine.rs` | `PromqlEngine`, `EngineOpts`, step grid, `QueryResult` assembly |
| `src/conformance/mod.rs` | `.test` DSL parser + runner harness |
| `tests/testdata/` | vendored Prometheus `.test` cases (Apache-2.0 attribution) |
| `tests/conformance.rs` | runs the vendored `.test` files through the harness |

`src/extension/` and `src/functions/` isolate the churn-prone DataFusion-internal surface from the planner and engine.

---

## Phase A — scaffold + result model + `MetricStore` + `InMemoryMetricStore`

> **Batching:** Task A1 (scaffold) must land first (creates `Cargo.toml` + `lib.rs`). Then A2 (`error.rs`), A3 (`result.rs`), A4 (`store.rs`) touch disjoint files and may run as a parallel batch — but each must also append one re-export line to `lib.rs`, so either serialize the `lib.rs` edits or have the integrating agent merge them. A5 (`InMemoryMetricStore`) depends on A4 + A3. Recommended: A1 → {A2, A3, A4 in parallel, lib.rs merged by reviewer} → A5.

### Task A1: Crate scaffold + workspace wiring

**Files:**
- Create: `crates/promql/Cargo.toml`
- Create: `crates/promql/src/lib.rs`
- Modify: root `Cargo.toml` (add `promql-parser` to `[workspace.dependencies]`; `datafusion`/`parquet`/`url` already added by the blockstore plan)

**Interfaces:**
- Produces: a compiling `crabka-promql` crate with `pub fn crate_smoke() -> bool` (placeholder, removed in A2).

- [ ] **Step 1: Add the `promql-parser` workspace dependency**

In root `Cargo.toml`, under `[workspace.dependencies]`, add (near the `datafusion` line the blockstore plan introduced):

```toml
# crabka-promql: faithful Prometheus-3.8 PromQL grammar port, parser-only (no
# arrow/DataFusion deps, so it cannot clash with the datafusion arrow pin). We
# supply our own PromQL->DataFusion planner.
promql-parser = "0.10"
```

> If `datafusion`/`parquet`/`url` are not yet present in `[workspace.dependencies]` (blockstore not landed in this tree), add them exactly as the blockstore plan Task 1 specifies.

- [ ] **Step 2: Create `crates/promql/Cargo.toml`**

```toml
[package]
name = "crabka-promql"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "PromQL engine (parser integration + PromQL->DataFusion planner + range-vector operators) for Crabka's Prometheus/Mimir-equivalent metrics backend"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-promql"
readme = "README.md"
keywords = ["observability", "prometheus", "promql", "datafusion", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
crabka-blockstore = { path = "../blockstore", version = "0.3.7" }
crabka-metrics = { path = "../metrics", version = "0.3.7" }
arrow = { workspace = true }
datafusion = { workspace = true }
promql-parser = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread"] }
futures = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

> Add `crabka-blockstore` and `crabka-metrics` to `[workspace.dependencies]` too if they are not already there (they are introduced by their own slice plans with `path`/`version`). If those crates are not yet present in this tree, the build will fail at this step — land Slices 1 (metrics) and the blockstore plan first.

- [ ] **Step 3: Create `crates/promql/src/lib.rs` with a placeholder**

```rust
//! PromQL engine for Crabka's Prometheus/Grafana-Mimir-equivalent metrics backend.
//!
//! Parses PromQL with `promql-parser`, lowers the AST onto a DataFusion
//! `LogicalPlan` via custom range-vector operators (`SeriesDivide`,
//! `SeriesNormalize`, `InstantManipulate`, `RangeManipulate` + the `RangeArray`
//! Arrow array), and evaluates instant/range queries over a step grid.

/// Placeholder so the crate has something to test before Task A2 lands real types.
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

Run: `cargo test -p crabka-promql`
Expected: compiles (first build fetches + compiles DataFusion from git — slow, several minutes, normal) and `smoke` PASSES.

If the build fails with an arrow major mismatch (`expected struct arrow::... found struct arrow::...`), the datafusion rev is wrong — re-confirm the pinned rev tracks arrow 59.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add Cargo.toml Cargo.lock crates/promql/
git commit -m "feat(promql): scaffold crabka-promql crate + promql-parser dep"
```

---

### Task A2: `PromqlError`

**Files:**
- Create: `crates/promql/src/error.rs`
- Modify: `crates/promql/src/lib.rs` (declare module, re-export, remove placeholder)

**Interfaces:**
- Produces:
  - `pub enum PromqlError { Parse(String), Plan(String), Exec(String), Store(String), Unsupported(String) }` (`Debug`, `thiserror::Error`)
  - `impl From<datafusion::error::DataFusionError> for PromqlError` → `Exec`
  - `pub type Result<T> = std::result::Result<T, PromqlError>` (internal alias)

- [ ] **Step 1: Write the failing test**

Create `crates/promql/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let pe: PromqlError = dfe.into();
        assert!(matches!(pe, PromqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = PromqlError::Unsupported("histogram_quantile".into());
        assert!(format!("{e}").contains("unsupported"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib error`
Expected: FAIL — `cannot find type PromqlError`.

- [ ] **Step 3: Implement `error.rs`**

Prepend above the `tests` module:

```rust
//! The crate's error type. Categories map to Prometheus HTTP `errorType`s in a
//! later slice (`Parse`/`Plan` -> `bad_data`, `Exec` -> `execution`,
//! `Store` -> `internal`, `Unsupported` -> `not_implemented`).

/// Errors raised by the PromQL engine. Foreign errors are stringified.
#[derive(Debug, thiserror::Error)]
pub enum PromqlError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Exec(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}

/// Internal convenience alias.
pub type Result<T> = std::result::Result<T, PromqlError>;

impl From<datafusion::error::DataFusionError> for PromqlError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Exec(e.to_string())
    }
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Replace the placeholder body of `lib.rs` (remove `crate_smoke` + its test) with:

```rust
mod error;

pub use error::PromqlError;
pub(crate) use error::Result;
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib error`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): PromqlError type + DataFusion conversion"
```

---

### Task A3: Result model — `QueryResult`, `InstantSample`, `RangeSeries`, `SampleValue`

**Files:**
- Create: `crates/promql/src/result.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `crabka_blockstore::Labels`, `crabka_metrics::NativeHistogram`.
- Produces:
  - `pub enum SampleValue { Float(f64), Histogram(NativeHistogram) }` (`Clone`, `Debug`, `PartialEq`)
  - `pub struct InstantSample { pub labels: Labels, pub ts_ms: i64, pub value: SampleValue }` (`Clone`, `Debug`, `PartialEq`)
  - `pub struct RangeSeries { pub labels: Labels, pub samples: Vec<(i64, SampleValue)> }` (`Clone`, `Debug`, `PartialEq`)
  - `pub enum QueryResult { Scalar { ts_ms: i64, value: f64 }, InstantVector(Vec<InstantSample>), RangeMatrix(Vec<RangeSeries>), Str { ts_ms: i64, value: String } }` (`Clone`, `Debug`, `PartialEq`)
  - `impl QueryResult { pub fn result_type(&self) -> &'static str }` → `"scalar"`/`"vector"`/`"matrix"`/`"string"`

- [ ] **Step 1: Write the failing test**

Create `crates/promql/src/result.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_blockstore::Labels;

    #[test]
    fn result_type_strings_match_prometheus() {
        assert!(QueryResult::Scalar { ts_ms: 0, value: 1.0 }.result_type() == "scalar");
        assert!(QueryResult::InstantVector(vec![]).result_type() == "vector");
        assert!(QueryResult::RangeMatrix(vec![]).result_type() == "matrix");
        assert!(QueryResult::Str { ts_ms: 0, value: "x".into() }.result_type() == "string");
    }

    #[test]
    fn instant_sample_holds_float_and_histogram() {
        let mut l = Labels::new();
        l.insert("__name__", "up");
        let s = InstantSample { labels: l, ts_ms: 1000, value: SampleValue::Float(1.0) };
        assert!(s.value == SampleValue::Float(1.0));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib result`
Expected: FAIL — `cannot find type QueryResult`.

- [ ] **Step 3: Implement `result.rs`**

Prepend above the `tests` module:

```rust
//! The Prometheus-shaped query result model. A later slice serializes these to
//! the HTTP API's `data.resultType` + `result` shapes byte-for-byte.

use crabka_blockstore::Labels;
use crabka_metrics::NativeHistogram;

/// A single sample value: a float or a native histogram.
#[derive(Clone, Debug, PartialEq)]
pub enum SampleValue {
    Float(f64),
    Histogram(NativeHistogram),
}

/// One labeled point (instant-vector element).
#[derive(Clone, Debug, PartialEq)]
pub struct InstantSample {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: SampleValue,
}

/// One labeled series of points (matrix element).
#[derive(Clone, Debug, PartialEq)]
pub struct RangeSeries {
    pub labels: Labels,
    pub samples: Vec<(i64, SampleValue)>,
}

/// A PromQL evaluation result.
#[derive(Clone, Debug, PartialEq)]
pub enum QueryResult {
    Scalar { ts_ms: i64, value: f64 },
    InstantVector(Vec<InstantSample>),
    RangeMatrix(Vec<RangeSeries>),
    Str { ts_ms: i64, value: String },
}

impl QueryResult {
    /// Prometheus `data.resultType` string.
    #[must_use]
    pub fn result_type(&self) -> &'static str {
        match self {
            QueryResult::Scalar { .. } => "scalar",
            QueryResult::InstantVector(_) => "vector",
            QueryResult::RangeMatrix(_) => "matrix",
            QueryResult::Str { .. } => "string",
        }
    }
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Add `mod result;` and `pub use result::{InstantSample, QueryResult, RangeSeries, SampleValue};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib result`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): QueryResult / InstantSample / RangeSeries / SampleValue model"
```

---

### Task A4: `MetricStore` trait + `ScanResult`

**Files:**
- Create: `crates/promql/src/store.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `crabka_blockstore::{LabelMatcher, Labels}`, `datafusion::prelude::SessionContext`, `PromqlError`.
- Produces:
  - `pub struct ScanResult { pub ctx: SessionContext, pub float_table: Option<String>, pub histogram_table: Option<String> }`
  - `#[async_trait::async_trait] pub trait MetricStore: Send + Sync { ... }` with exactly the four methods from the Shared cross-slice contract.

- [ ] **Step 1: Write the failing test**

Create `crates/promql/src/store.rs` (a trivial in-test impl proves the trait is object-shaped and the signatures compile):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_blockstore::Labels;
    use datafusion::prelude::SessionContext;

    struct Empty;

    #[async_trait::async_trait]
    impl MetricStore for Empty {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<ScanResult, PromqlError> {
            Ok(ScanResult { ctx: SessionContext::new(), float_table: None, histogram_table: None })
        }
        async fn label_names(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }
        async fn label_values(
            &self,
            _tenant: &str,
            _name: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<String>, PromqlError> {
            Ok(vec![])
        }
        async fn series(
            &self,
            _tenant: &str,
            _matchers: &[crabka_blockstore::LabelMatcher],
            _start_ms: i64,
            _end_ms: i64,
        ) -> Result<Vec<Labels>, PromqlError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe_and_default_returns_none_tables() {
        let s: std::sync::Arc<dyn MetricStore> = std::sync::Arc::new(Empty);
        let r = s.scan("t", &[], 0, 1).await.unwrap();
        assert!(r.float_table.is_none());
        assert!(r.histogram_table.is_none());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib store`
Expected: FAIL — `cannot find type MetricStore`.

- [ ] **Step 3: Implement `store.rs`**

Prepend above the `tests` module:

```rust
//! The data-access seam. The engine is generic over `MetricStore`; production
//! wires it to `crabka_blockstore::BlockStore::scan_context` (Slice 5), tests use
//! `InMemoryMetricStore`. `scan` yields a DataFusion `SessionContext` with the
//! float and/or histogram tables registered for the (tenant, matchers, range).

use crabka_blockstore::{LabelMatcher, Labels};
use datafusion::prelude::SessionContext;

use crate::error::PromqlError;

/// The result of a leaf scan: a `SessionContext` with up to two tables
/// registered (float samples and native histograms). A table name is `None`
/// when no series of that kind matched.
pub struct ScanResult {
    pub ctx: SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,
}

/// Resolves PromQL matchers to DataFusion tables over a tenant's metric data.
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
    /// Register float + histogram tables for the matched series in `[start_ms, end_ms]`.
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError>;

    /// Distinct label names across the matched series.
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;

    /// Distinct values of `name` across the matched series.
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;

    /// Label sets of the matched series.
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError>;
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Add `mod store;` and `pub use store::{MetricStore, ScanResult};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib store`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-promql
cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): MetricStore trait + ScanResult"
```

---

### Task A5: `InMemoryMetricStore`

**Files:**
- Create: `crates/promql/src/in_memory.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `MetricStore`, `ScanResult`, `crabka_metrics::{float_sample_schema, native_histogram_schema, encode_float_samples, encode_native_histograms, NativeHistogram, COL_FINGERPRINT, COL_TIMESTAMP}`, `crabka_blockstore::{Labels, LabelMatcher, MatchOp, SeriesFingerprint}`, DataFusion `MemTable`.
- Produces:
  - `pub struct InMemoryMetricStore` (`Default`)
  - `pub fn new() -> Self`
  - `pub fn push_float(&mut self, tenant: &str, labels: Labels, ts_ms: i64, value: f64)`
  - `pub fn push_histogram(&mut self, tenant: &str, labels: Labels, ts_ms: i64, hist: NativeHistogram)`
  - the `MetricStore` impl: builds `MemTable`s from the in-memory samples whose fingerprint matches the matchers and whose ts ∈ `[start_ms, end_ms]`, registers them as `floats`/`histograms` tables, returns their names (or `None` when empty).

> **Matcher semantics:** evaluate matchers against each series' `Labels` directly (Eq/Neq exact; Re/Nre anchored regex `^(?:..)$` — reuse the same anchoring blockstore's `Index` uses). A `__name__` matcher matches the `__name__` label. This is the in-memory analog of `blockstore::Index::resolve`; correctness here is what makes the `.test` harness trustworthy.

- [ ] **Step 1: Write the failing test**

Create `crates/promql/src/in_memory.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_blockstore::{LabelMatcher, Labels, MatchOp};
    use datafusion::arrow::array::AsArray;

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    #[tokio::test]
    async fn scan_filters_by_matcher_and_time_and_registers_float_table() {
        let mut s = InMemoryMetricStore::new();
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1000, 1.0);
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 2000, 1.0);
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1000, 0.0);
        s.push_float("t", lbls(&[("__name__", "down")]), 1000, 9.0);

        let m = [
            LabelMatcher::new("__name__", MatchOp::Eq, "up"),
            LabelMatcher::new("job", MatchOp::Eq, "a"),
        ];
        let r = s.scan("t", &m, 0, 1500).await.unwrap();
        let table = r.float_table.clone().unwrap();
        assert!(r.histogram_table.is_none());

        // job=a, ts<=1500 -> exactly one row.
        let df = r.ctx.sql(&format!("SELECT count(*) AS c FROM {table}")).await.unwrap();
        let out = df.collect().await.unwrap();
        let c = out[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
        assert!(c == 1);
    }

    #[tokio::test]
    async fn scan_with_no_match_returns_none_tables() {
        let mut s = InMemoryMetricStore::new();
        s.push_float("t", lbls(&[("__name__", "up")]), 1000, 1.0);
        let m = [LabelMatcher::new("__name__", MatchOp::Eq, "absent")];
        let r = s.scan("t", &m, 0, 5000).await.unwrap();
        assert!(r.float_table.is_none());
        assert!(r.histogram_table.is_none());
    }

    #[tokio::test]
    async fn label_values_returns_distinct_for_name() {
        let mut s = InMemoryMetricStore::new();
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 1, 1.0);
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 1, 1.0);
        let m = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
        let mut v = s.label_values("t", "job", &m, 0, 10).await.unwrap();
        v.sort();
        assert!(v == vec!["a".to_string(), "b".to_string()]);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib in_memory`
Expected: FAIL — `cannot find type InMemoryMetricStore`.

- [ ] **Step 3: Implement `in_memory.rs`**

Prepend above the `tests` module. The store holds raw samples and builds a `MemTable` per scan; column order matches `crabka_metrics::float_sample_schema()` / `native_histogram_schema()`.

```rust
//! In-memory `MetricStore` used by the conformance harness and engine tests.
//! Holds raw `(fingerprint, labels, ts_ms, value|hist)` rows and, per scan,
//! filters by matcher + time and materializes DataFusion `MemTable`s whose
//! schema matches the Slice-1 block schemas.

use std::collections::HashMap;
use std::sync::Arc;

use crabka_blockstore::{LabelMatcher, Labels, MatchOp, SeriesFingerprint};
use crabka_metrics::{
    NativeHistogram, encode_float_samples, encode_native_histograms, float_sample_schema,
    native_histogram_schema,
};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::error::PromqlError;
use crate::store::{MetricStore, ScanResult};

#[derive(Clone)]
struct FloatRow {
    fp: SeriesFingerprint,
    labels: Labels,
    ts_ms: i64,
    value: f64,
}

#[derive(Clone)]
struct HistRow {
    fp: SeriesFingerprint,
    labels: Labels,
    ts_ms: i64,
    hist: NativeHistogram,
}

/// In-memory metric store keyed by tenant.
#[derive(Default)]
pub struct InMemoryMetricStore {
    floats: HashMap<String, Vec<FloatRow>>,
    hists: HashMap<String, Vec<HistRow>>,
}

impl InMemoryMetricStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_float(&mut self, tenant: &str, labels: Labels, ts_ms: i64, value: f64) {
        let fp = labels.fingerprint();
        self.floats
            .entry(tenant.to_string())
            .or_default()
            .push(FloatRow { fp, labels, ts_ms, value });
    }

    pub fn push_histogram(&mut self, tenant: &str, labels: Labels, ts_ms: i64, hist: NativeHistogram) {
        let fp = labels.fingerprint();
        self.hists
            .entry(tenant.to_string())
            .or_default()
            .push(HistRow { fp, labels, ts_ms, hist });
    }
}

/// LogQL/PromQL regexes are fully anchored.
fn matches(labels: &Labels, m: &LabelMatcher) -> Result<bool, PromqlError> {
    let actual = labels.get(&m.name).unwrap_or("");
    Ok(match m.op {
        MatchOp::Eq => actual == m.value,
        MatchOp::Neq => actual != m.value,
        MatchOp::Re | MatchOp::Nre => {
            let re = regex_anchored(&m.value)?;
            let hit = re.is_match(actual);
            if m.op == MatchOp::Re { hit } else { !hit }
        }
    })
}

fn regex_anchored(pattern: &str) -> Result<regex::Regex, PromqlError> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|e| PromqlError::Plan(format!("bad regex `{pattern}`: {e}")))
}

fn all_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool, PromqlError> {
    for m in matchers {
        if !matches(labels, m)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[async_trait::async_trait]
impl MetricStore for InMemoryMetricStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError> {
        let ctx = SessionContext::new();

        // ---- floats ----
        let mut float_rows: Vec<(u64, i64, f64)> = Vec::new();
        if let Some(rows) = self.floats.get(tenant) {
            for r in rows {
                if r.ts_ms >= start_ms && r.ts_ms <= end_ms && all_match(&r.labels, matchers)? {
                    float_rows.push((r.fp, r.ts_ms, r.value));
                }
            }
        }
        float_rows.sort_by_key(|(fp, ts, _)| (*fp, *ts));
        let float_table = if float_rows.is_empty() {
            None
        } else {
            let batch = encode_float_samples(&float_rows)
                .map_err(|e| PromqlError::Store(e.to_string()))?;
            let mt = MemTable::try_new(float_sample_schema(), vec![vec![batch]])?;
            ctx.register_table("floats", Arc::new(mt))?;
            Some("floats".to_string())
        };

        // ---- histograms ----
        let mut hist_rows: Vec<(u64, i64, NativeHistogram)> = Vec::new();
        if let Some(rows) = self.hists.get(tenant) {
            for r in rows {
                if r.ts_ms >= start_ms && r.ts_ms <= end_ms && all_match(&r.labels, matchers)? {
                    hist_rows.push((r.fp, r.ts_ms, r.hist.clone()));
                }
            }
        }
        hist_rows.sort_by_key(|(fp, ts, _)| (*fp, *ts));
        let histogram_table = if hist_rows.is_empty() {
            None
        } else {
            let batch = encode_native_histograms(&hist_rows)
                .map_err(|e| PromqlError::Store(e.to_string()))?;
            let mt = MemTable::try_new(native_histogram_schema(), vec![vec![batch]])?;
            ctx.register_table("histograms", Arc::new(mt))?;
            Some("histograms".to_string())
        };

        Ok(ScanResult { ctx, float_table, histogram_table })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        let mut names = std::collections::BTreeSet::new();
        for labels in self.matched_series(tenant, matchers, start_ms, end_ms)? {
            for (k, _) in labels.iter() {
                names.insert(k.clone());
            }
        }
        Ok(names.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        let mut values = std::collections::BTreeSet::new();
        for labels in self.matched_series(tenant, matchers, start_ms, end_ms)? {
            if let Some(v) = labels.get(name) {
                values.insert(v.to_string());
            }
        }
        Ok(values.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError> {
        self.matched_series(tenant, matchers, start_ms, end_ms)
    }
}

impl InMemoryMetricStore {
    /// Distinct label sets matching the matchers within the time window.
    fn matched_series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError> {
        use std::collections::BTreeMap;
        let mut by_fp: BTreeMap<SeriesFingerprint, Labels> = BTreeMap::new();
        if let Some(rows) = self.floats.get(tenant) {
            for r in rows {
                if r.ts_ms >= start_ms && r.ts_ms <= end_ms && all_match(&r.labels, matchers)? {
                    by_fp.entry(r.fp).or_insert_with(|| r.labels.clone());
                }
            }
        }
        if let Some(rows) = self.hists.get(tenant) {
            for r in rows {
                if r.ts_ms >= start_ms && r.ts_ms <= end_ms && all_match(&r.labels, matchers)? {
                    by_fp.entry(r.fp).or_insert_with(|| r.labels.clone());
                }
            }
        }
        Ok(by_fp.into_values().collect())
    }
}
```

> **Dev-dependency note:** `regex` is already a workspace dependency; add `regex = { workspace = true }` to `crabka-promql`'s `[dependencies]` (the matcher anchoring needs it). Add it in this task's `Cargo.toml` edit.

- [ ] **Step 4: Wire into `lib.rs`**

Add `mod in_memory;` and `pub use in_memory::InMemoryMetricStore;`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib in_memory`
Expected: PASS (3 tests).

- [ ] **Step 6: Phase A gate + commit**

```bash
cargo test -p crabka-promql && cargo clippy -p crabka-promql --all-targets && cargo fmt -p crabka-promql --check
git add crates/promql/ Cargo.toml
git commit -m "feat(promql): InMemoryMetricStore test impl over DataFusion MemTables"
```

---

## Phase B — `RangeArray` + the four custom operators

> **Batching:** B1 (`RangeArray`) is the foundation for B5 (`RangeManipulate`) and the rate UDFs (Phase C). B2/B3/B4 (`SeriesDivide`/`SeriesNormalize`/`InstantManipulate`) touch disjoint files and may run as a parallel batch after B1, each adding one `mod` line to `src/extension/mod.rs` (merge those edits). B5 depends on B1. **This is the churn-prone phase: every operator/array task carries a "verify against datafusion rev `0838a4d` / GreptimeDB `extension_plan/`" note and a behavior-pinning test. Implement the trait methods to satisfy the test; do not invent signatures.** Recommended: B1 → {B2, B3, B4 in parallel} → B5.

### Task B1: `RangeArray` — the custom range-vector Arrow array

**Files:**
- Create: `crates/promql/src/range_array.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct RangeArray { /* values: ArrayRef, ranges: Vec<(u32,u32)> i.e. (offset,len) */ }`
  - `pub fn from_ranges(values: ArrayRef, ranges: impl IntoIterator<Item = (u32, u32)>) -> Result<RangeArray, ArrowError>` — validates each `offset+len <= values.len()`.
  - `pub fn len(&self) -> usize` / `pub fn is_empty(&self) -> bool`
  - `pub fn values(&self) -> &ArrayRef`
  - `pub fn get(&self, index: usize) -> Option<ArrayRef>` — returns `values.slice(offset, len)` for cell `index`.
  - `pub fn ranges(&self) -> &[(u32, u32)]`
  - `pub fn into_dict_array(self) -> DictionaryArray<...>` **OR** a `to_list_field()` representation chosen to match how GreptimeDB pipes `RangeArray` columns through DataFusion — **decide this against the reference** (see note); the *public surface above is frozen*, the in-Arrow encoding is the implementation detail.

> **Why a custom array.** A range vector at eval timestamp `t` is "the samples in `(t-range, t]`". Representing this as ordinary rows explodes the row count. `RangeArray` stores one contiguous backing `values` array plus a list of `(offset, len)` windows, so each cell is a zero-copy slice. DataFusion has no equivalent. **Reference: GreptimeDB `src/promql/src/range_array.rs`.** Its trick is to encode the ranges in the Arrow *type system* so the array survives passing through DataFusion's `RecordBatch`/UDF machinery (it builds on a `DictionaryArray<Int64Type>` whose keys pack `offset`/`length` into an `i64`, with the windowed values as the dictionary values). Mirror that encoding so `range_manipulate` can emit a `RangeArray` column and the rate UDFs can read it back — but keep the public methods above stable. **`unsafe_code = "forbid"`: build only on safe arrow constructors (`DictionaryArray::try_new`, `Array::slice`).**

- [ ] **Step 1: Write the failing behavior test**

Create `crates/promql/src/range_array.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Float64Builder};
    use assert2::assert;

    use super::*;

    #[test]
    fn windows_slice_the_backing_array() {
        // backing values: [10, 11, 12, 13, 14]
        let mut b = Float64Builder::new();
        for v in [10.0, 11.0, 12.0, 13.0, 14.0] {
            b.append_value(v);
        }
        let values = Arc::new(b.finish()) as arrow::array::ArrayRef;

        // two windows: [0..3) = {10,11,12} and [2..5) = {12,13,14}
        let ra = RangeArray::from_ranges(values, [(0_u32, 3_u32), (2, 3)]).unwrap();
        assert!(ra.len() == 2);

        let w0 = ra.get(0).unwrap();
        let w0 = w0.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((0..w0.len()).map(|i| w0.value(i)).collect::<Vec<_>>() == vec![10.0, 11.0, 12.0]);

        let w1 = ra.get(1).unwrap();
        let w1 = w1.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((0..w1.len()).map(|i| w1.value(i)).collect::<Vec<_>>() == vec![12.0, 13.0, 14.0]);
    }

    #[test]
    fn out_of_bounds_window_is_rejected() {
        let values = Arc::new(Float64Array::from(vec![1.0, 2.0])) as arrow::array::ArrayRef;
        // offset 1 + len 5 overruns the 2-element backing array.
        assert!(RangeArray::from_ranges(values, [(1_u32, 5_u32)]).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib range_array`
Expected: FAIL — `cannot find type RangeArray`.

- [ ] **Step 3: Implement `range_array.rs`**

Prepend above the `tests` module. The struct keeps `values` + `ranges`; `get` is `values.slice`. The DataFusion-column encoding (`into_dict_array` / `from` a column) is added in Step 5 once B5 needs it — Step 3 lands only the slice-window behavior the test pins.

```rust
//! `RangeArray` — a list-like view where each cell is a slice (window) of a
//! single contiguous backing array. Represents range vectors (the samples in a
//! step's lookback window) without row explosion. Reference:
//! GreptimeDB `src/promql/src/range_array.rs`.

use arrow::array::{Array, ArrayRef};
use arrow::error::ArrowError;

/// A view over `values` partitioned into `(offset, len)` windows.
#[derive(Clone, Debug)]
pub struct RangeArray {
    values: ArrayRef,
    ranges: Vec<(u32, u32)>,
}

impl RangeArray {
    /// Build from a backing array + windows; validates each window fits.
    pub fn from_ranges(
        values: ArrayRef,
        ranges: impl IntoIterator<Item = (u32, u32)>,
    ) -> Result<Self, ArrowError> {
        let ranges: Vec<(u32, u32)> = ranges.into_iter().collect();
        let total = values.len();
        for &(off, len) in &ranges {
            let end = off as usize + len as usize;
            if end > total {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "range window [{off}, {end}) overruns backing array of len {total}"
                )));
            }
        }
        Ok(Self { values, ranges })
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    #[must_use]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    #[must_use]
    pub fn ranges(&self) -> &[(u32, u32)] {
        &self.ranges
    }

    /// The windowed slice for cell `index`, or `None` if out of bounds.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<ArrayRef> {
        let &(off, len) = self.ranges.get(index)?;
        Some(self.values.slice(off as usize, len as usize))
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib range_array`
Expected: PASS (2 tests).

- [ ] **Step 5: Add the DataFusion-column encoding (the GreptimeDB trick) + its round-trip test**

The operators in B5 / the UDFs in Phase C must pass a `RangeArray` as an Arrow column inside a `RecordBatch`. Add `into_dict_array(self) -> DictionaryArray<Int64Type>` and `try_from_dict_array(arr: &DictionaryArray<Int64Type>) -> Result<RangeArray, ArrowError>` that pack each `(offset, len)` into the dictionary *keys* (e.g. `key = (offset as i64) << 32 | len as i64`) with `values` as the dictionary *values*, exactly as GreptimeDB does. Append this test:

```rust
    #[test]
    fn dict_array_round_trips_through_recordbatch_column() {
        use std::sync::Arc;
        use arrow::array::Float64Array;

        let values = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])) as arrow::array::ArrayRef;
        let ra = RangeArray::from_ranges(values, [(0_u32, 2_u32), (1, 3)]).unwrap();
        let dict = ra.clone().into_dict_array();
        let back = RangeArray::try_from_dict_array(&dict).unwrap();
        assert!(back.ranges() == ra.ranges());
    }
```

> **Verify against datafusion rev `0838a4d` / GreptimeDB `range_array.rs`:** the exact `DictionaryArray<Int64Type>` constructor (`try_new(keys, values)`), the key-packing layout, and the `DataType::Dictionary(Box::new(Int64), Box::new(value_type))` field declaration are the churn surface. Implement `into_dict_array`/`try_from_dict_array` to satisfy the round-trip test; if the constructor signature differs at the pin, adapt it — keep the test's asserted `ranges()` equality. Document the chosen key-packing in a `//` comment so B5/Phase C read it consistently.

- [ ] **Step 6: Wire into `lib.rs` + commit**

Add `mod range_array;` and `pub use range_array::RangeArray;`.

```bash
cargo test -p crabka-promql --lib range_array
cargo fmt -p crabka-promql && cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): RangeArray — windowed view + DataFusion dict-array encoding"
```

---

### Task B2: `SeriesDivide` operator

**Files:**
- Create: `crates/promql/src/extension/mod.rs` (module wiring — first operator creates it)
- Create: `crates/promql/src/extension/series_divide.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct SeriesDivide { /* tag_columns: Vec<String>, input: LogicalPlan */ }` implementing `UserDefinedLogicalNodeCore`.
  - `pub struct SeriesDivideExec { /* tag_columns, input: Arc<dyn ExecutionPlan>, metric: ExecutionPlanMetricsSet */ }` implementing `ExecutionPlan`, whose stream partitions an already-`(tags..., timestamp)`-sorted input into contiguous per-series runs (it does not reorder; it splits batch boundaries so no emitted batch straddles two series).
  - The stream type `SeriesDivideStream` implementing `RecordBatchStream` + `Stream<Item = Result<RecordBatch>>`.

> **What it does (spec §6.1).** Given a batch sorted by series-identifying tag columns then timestamp, emit sub-batches each containing exactly one series. Downstream operators (`SeriesNormalize`, `InstantManipulate`, `RangeManipulate`) assume one-series-per-batch. **Reference: GreptimeDB `extension_plan/series_divide.rs`.**

- [ ] **Step 1: Write the failing behavior test**

Create `crates/promql/src/extension/series_divide.rs` with a test that builds a two-series input batch, runs the exec, and asserts each output batch is single-series:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;

    use super::*;

    fn input_batch() -> RecordBatch {
        // series "a": ts 1,2 ; series "b": ts 1 — already sorted by (job, ts).
        let job = StringArray::from(vec!["a", "a", "b"]);
        let ts = Int64Array::from(vec![1_i64, 2, 1]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(job), Arc::new(ts)]).unwrap()
    }

    #[tokio::test]
    async fn divides_into_single_series_batches() {
        let batch = input_batch();
        let schema = batch.schema();
        let mem =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema.clone(), None).unwrap();
        let exec = SeriesDivideExec::new(vec!["job".to_string()], mem);

        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        // Every emitted batch must contain exactly one distinct `job`.
        for b in &out {
            let job = b
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let first = job.value(0);
            assert!((0..job.len()).all(|i| job.value(i) == first));
        }
        // Total rows preserved.
        let total: usize = out.iter().map(RecordBatch::num_rows).sum();
        assert!(total == 3);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib extension::series_divide`
Expected: FAIL — `cannot find type SeriesDivideExec`.

- [ ] **Step 3: Implement `series_divide.rs`**

Implement the `UserDefinedLogicalNodeCore` node (`SeriesDivide`) and the `ExecutionPlan` (`SeriesDivideExec`) + stream. The structure below is the *shape*; fill the trait methods to satisfy the test against the pinned rev.

```rust
//! `SeriesDivide` — split a batch sorted by `(tag_columns..., timestamp)` into
//! contiguous single-series sub-batches. Reference:
//! GreptimeDB `extension_plan/series_divide.rs`.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchemaRef, Result as DfResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

/// Logical node: partition the input into per-series batches.
#[derive(PartialEq, Eq, Hash, PartialOrd, Debug)]
pub struct SeriesDivide {
    pub tag_columns: Vec<String>,
    pub input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for SeriesDivide {
    fn name(&self) -> &str {
        "SeriesDivide"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromSeriesDivide: tags={:?}", self.tag_columns)
    }

    // NOTE: the exact `with_exprs_and_inputs` (older: `from_template`) signature
    // is the churn point — verify against datafusion rev 0838a4d.
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        Ok(Self {
            tag_columns: self.tag_columns.clone(),
            input: inputs.swap_remove(0),
        })
    }
}

/// Physical node.
#[derive(Debug)]
pub struct SeriesDivideExec {
    tag_columns: Vec<String>,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl SeriesDivideExec {
    #[must_use]
    pub fn new(tag_columns: Vec<String>, input: Arc<dyn ExecutionPlan>) -> Self {
        // Derive PlanProperties from `input` (eq-properties, partitioning,
        // execution mode) — verify the constructor against the pinned rev.
        let properties = input.properties().clone();
        Self { tag_columns, input, properties }
    }

    /// Find the row indices where the series identity changes (run boundaries).
    fn boundaries(&self, batch: &RecordBatch) -> Vec<usize> {
        let cols: Vec<&dyn Array> = self
            .tag_columns
            .iter()
            .filter_map(|c| batch.column_by_name(c))
            .map(AsRef::as_ref)
            .collect();
        let mut bounds = vec![0usize];
        for row in 1..batch.num_rows() {
            let changed = cols.iter().any(|col| {
                // Row-level inequality via the arrow comparison kernel; verify the
                // exact `arrow::array::Array` equality helper at the pinned rev.
                !arrow::array::Array::slice(*col, row - 1, 1)
                    .eq(&arrow::array::Array::slice(*col, row, 1))
            });
            if changed {
                bounds.push(row);
            }
        }
        bounds.push(batch.num_rows());
        bounds
    }
}

impl DisplayAs for SeriesDivideExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PromSeriesDivideExec: tags={:?}", self.tag_columns)
    }
}

// `impl ExecutionPlan for SeriesDivideExec { ... }` — implement:
//   name, as_any, properties/schema, children, with_new_children, execute.
//   `execute` returns a `SendableRecordBatchStream` whose poll slices each input
//   batch at `self.boundaries(&batch)` and yields single-series sub-batches via
//   `batch.slice(start, end - start)`.
// The exact ExecutionPlan trait method set (e.g. `properties()` vs the older
// `output_partitioning`/`output_ordering`) changed across DF versions — VERIFY
// against datafusion rev 0838a4d and GreptimeDB `series_divide.rs`. Keep the
// behavior the test pins (single-series output batches, all rows preserved).
```

> **Verify against datafusion rev `0838a4d` / GreptimeDB `series_divide.rs`:** the `UserDefinedLogicalNodeCore` method set (`with_exprs_and_inputs` vs `from_template`), the `ExecutionPlan` method set (`properties()`/`PlanProperties` vs separate `output_partitioning`/`execution_mode`), and the row-equality kernel are all churn points. **Verify the in-memory source constructor against datafusion rev `0838a4d`** — `MemoryExec` was replaced by `MemorySourceConfig`/`DataSourceExec` in DF46, so the test uses `MemorySourceConfig::try_new_exec(...)` (returns an `Arc<dyn ExecutionPlan>` directly — no outer `Arc::new`). The arrow per-row equality in `boundaries` is illustrative — use whatever safe comparison the pinned arrow exposes (e.g. `arrow::compute::kernels::cmp` or a `make_comparator`). Do not change the test's asserted behavior.

- [ ] **Step 4: Wire `extension/mod.rs` + `lib.rs`**

Create `crates/promql/src/extension/mod.rs` with `pub mod series_divide;` and a re-export; add `mod extension;` to `lib.rs` and `pub use extension::series_divide::{SeriesDivide, SeriesDivideExec};`.

- [ ] **Step 5: Run to verify it passes + commit**

```bash
cargo test -p crabka-promql --lib extension::series_divide
cargo fmt -p crabka-promql && cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): SeriesDivide operator (logical node + exec + stream)"
```

---

### Task B3: `SeriesNormalize` operator

**Files:**
- Create: `crates/promql/src/extension/normalize.rs`
- Modify: `crates/promql/src/extension/mod.rs`, `crates/promql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct SeriesNormalize { /* offset_ms: i64, time_index: String, need_filter_out_nan: bool, input: LogicalPlan */ }` (`UserDefinedLogicalNodeCore`).
  - `pub struct SeriesNormalizeExec` (`ExecutionPlan`) + stream: for each single-series batch, (1) apply the `offset` by adding `offset_ms` to the timestamp column, (2) sort by timestamp ascending, (3) optionally drop rows whose value is NaN.

> **What it does (spec §6.1).** One series per batch in, normalized out: `offset`/`@` applied, time-sorted, NaNs dropped. Runs *after* `SeriesDivide`. **Reference: GreptimeDB `extension_plan/normalize.rs`.** The `@`-modifier general form is Slice 3; here implement the constant `offset` add + sort + NaN filter (the `.test` cases this slice vendors don't need `@`).

- [ ] **Step 1: Write the failing test**

Test: feed one series with timestamps out of order and a NaN value; assert output is sorted ascending and the NaN row is gone when `need_filter_out_nan` is set.

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use assert2::assert;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn sorts_by_time_and_drops_nan() {
        let ts = Int64Array::from(vec![300_i64, 100, 200]);
        let val = Float64Array::from(vec![3.0, f64::NAN, 2.0]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(val)]).unwrap();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let exec = SeriesNormalizeExec::new(0, "timestamp".into(), true, mem);
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();

        let merged = arrow::compute::concat_batches(&out[0].schema(), &out).unwrap();
        let ts = merged
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // NaN row (ts=100) dropped; remaining sorted ascending: 200, 300.
        assert!((0..ts.len()).map(|i| ts.value(i)).collect::<Vec<_>>() == vec![200, 300]);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib extension::normalize`
Expected: FAIL — `cannot find type SeriesNormalizeExec`.

- [ ] **Step 3: Implement `normalize.rs`**

Mirror the `SeriesDivide` shape (logical node + exec + stream). In the stream: add `offset_ms` to the time column (arrow `add_scalar`), build a sort permutation by timestamp (`arrow::compute::sort_to_indices` + `take`), then if `need_filter_out_nan` filter the value column with `arrow::compute::filter` against a `!is_nan` boolean mask. Provide the full real code for the stream's transform fn; keep the trait-impl method set behind the same "verify against rev `0838a4d` / GreptimeDB `normalize.rs`" note as B2.

- [ ] **Step 4: Wire + run + commit**

Add to `extension/mod.rs` + `lib.rs`. `cargo test -p crabka-promql --lib extension::normalize` → PASS.

```bash
cargo fmt -p crabka-promql && cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): SeriesNormalize operator (offset + sort + NaN filter)"
```

---

### Task B4: `InstantManipulate` operator

**Files:**
- Create: `crates/promql/src/extension/instant_manipulate.rs`
- Modify: `crates/promql/src/extension/mod.rs`, `crates/promql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct InstantManipulate { /* start_ms, end_ms, step_ms, lookback_delta_ms, time_index, field_column, input */ }` (`UserDefinedLogicalNodeCore`).
  - `pub struct InstantManipulateExec` (`ExecutionPlan`) + stream: for a single time-sorted series, for each grid point `t` in `[start, end]` step `step`, select the most recent sample with `ts <= t` and `ts > t - lookback_delta` (the staleness/lookback rule); emit one row per grid point that has a valid sample (none if stale).

> **What it does (spec §6.1, §6.3).** Instant-vector selection on the step grid honoring the 5m-default lookback-delta and staleness. **Reference: GreptimeDB `extension_plan/instant_manipulate.rs`.** Staleness markers (stale-NaN bit pattern) terminate a series — for this slice, treat a NaN value at-or-before `t` within the lookback as "no value" (do not carry it forward).

- [ ] **Step 1: Write the failing test**

Test: one series at ts {0, 60_000}, value {1, 2}, lookback 300_000, grid start=0 end=120_000 step=60_000. Expect grid points 0→1, 60_000→2, 120_000→2 (carried forward within lookback).

- [ ] **Step 2: Run to verify it fails** — `cannot find type InstantManipulateExec`.

- [ ] **Step 3: Implement `instant_manipulate.rs`**

Logical node + exec + stream mirroring B2/B3. The per-series stream transform: collect the (ts, value) pairs, then for each grid `t` binary-search the last `ts <= t`; if it exists and `t - ts < lookback_delta_ms` and the value is not NaN, emit `(t, value)`. Output schema = input schema (timestamp replaced by grid `t`). Provide full real code for the selection fn; keep trait methods behind the "verify against rev `0838a4d`" note.

- [ ] **Step 4: Wire + run + commit** (`feat(promql): InstantManipulate operator (step-grid lookback selection)`).

---

### Task B5: `RangeManipulate` operator (emits `RangeArray` columns)

**Files:**
- Create: `crates/promql/src/extension/range_manipulate.rs`
- Modify: `crates/promql/src/extension/mod.rs`, `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `RangeArray` (B1).
- Produces:
  - `pub struct RangeManipulate { /* start_ms, end_ms, step_ms, range_ms, time_index, field_columns, input */ }` (`UserDefinedLogicalNodeCore`).
  - `pub struct RangeManipulateExec` (`ExecutionPlan`) + stream: for a single time-sorted series, for each grid point `t`, compute the window `(t - range_ms, t]` and emit a row whose `timestamp` and each `field_column` are **`RangeArray` columns** (one window-cell per grid point) — folding `(ts, value)` into range vectors per spec §6.1 (left-open, right-closed).
  - `pub fn build_extended_range_schema(input: &Schema, time_index: &str, field_columns: &[String]) -> SchemaRef` — the output schema where the time + field columns become the `RangeArray` dict-encoded type.

> **What it does (spec §6.1).** Materialize range vectors as `RangeArray` columns so the rate-family UDFs (Phase C) consume `(timestamp_range, value_range)` pairs. Range vectors are **left-open, right-closed `(t-range, t]`**. **Reference: GreptimeDB `extension_plan/range_manipulate.rs`.** The output has both a `RangeArray` timestamp column and a `RangeArray` per value column, plus the (non-range) aligned grid timestamp.

- [ ] **Step 1: Write the failing test**

Test: one series ts {0, 30_000, 60_000} val {1, 2, 3}; grid start=0 end=60_000 step=30_000 range=60_000. For grid `t=60_000`, window `(0, 60_000]` = {30_000→2, 60_000→3} (left-open excludes ts=0). Build the exec, collect, downcast the value column to a `RangeArray` via `RangeArray::try_from_dict_array`, assert cell for `t=60_000` holds `[2.0, 3.0]`.

- [ ] **Step 2: Run to verify it fails** — `cannot find type RangeManipulateExec`.

- [ ] **Step 3: Implement `range_manipulate.rs`**

Logical node + exec + stream. The per-series transform: build, for each grid `t`, the `(offset, len)` window into the backing (ts, value) arrays using the left-open/right-closed bound (`ts > t - range && ts <= t`); construct a `RangeArray` for the timestamp column and each field column via `RangeArray::from_ranges`, then `into_dict_array()` to put them in the output `RecordBatch`. Provide full real code for the windowing fn (the bound logic is correctness-critical and not a churn point); keep trait methods behind the "verify against rev `0838a4d` / GreptimeDB `range_manipulate.rs`" note.

- [ ] **Step 4: Wire + run + commit** (`feat(promql): RangeManipulate operator emitting RangeArray columns`).

- [ ] **Step 5: Phase B gate**

Run: `cargo test -p crabka-promql && cargo clippy -p crabka-promql --all-targets && cargo fmt -p crabka-promql --check`
Expected: all PASS. Commit any fmt/clippy fixups.

---

## Phase C — selectors + rate-family

> **Batching:** C1 (extrapolation core) has no deps beyond std and lands first. C2 (rate UDFs) depends on C1 + `RangeArray`. C3 (planner scaffold + parse entry) is independent of C1/C2 and may run in parallel with C1. C4 (selector lowering) depends on C3 + the operators (Phase B) + the store. C5 (rate lowering wiring) depends on C2 + C4. Recommended: {C1, C3 in parallel} → C2 → C4 → C5.

### Task C1: The counter-reset + extrapolation core (the #1 correctness trap)

**Files:**
- Create: `crates/promql/src/functions/mod.rs` (module wiring)
- Create: `crates/promql/src/functions/extrapolate.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub(crate) enum RangeFn { Rate, Increase, Delta }` (the gauge/counter distinction: `Delta` skips reset correction).
  - `pub(crate) fn extrapolated_rate(timestamps: &[i64], values: &[f64], range_start_ms: i64, range_end_ms: i64, range_ms: i64, kind: RangeFn) -> Option<f64>` — the exact Prometheus `extrapolatedRate`/`rate`/`increase`/`delta` algorithm. Returns `None` for `< 2` samples (Prometheus emits no point).
  - `pub(crate) fn instant_delta(timestamps: &[i64], values: &[f64], kind: IrateFn) -> Option<f64>` for `irate`/`idelta` (last two samples).
  - `pub(crate) enum IrateFn { Irate, Idelta }`.

> **The algorithm (spec §6.2 — match Prometheus byte-for-byte):**
> 1. Need ≥ 2 samples in the window; else `None`.
> 2. For counters (`Rate`/`Increase`): walk samples, sum `previous` whenever the current value < previous (a reset), i.e. `resultValue = last - first + correction`, where each decrease adds the pre-decrease value. For gauges (`Delta`): `resultValue = last - first` (no correction).
> 3. `sampledInterval = (lastTs - firstTs)` in seconds; `averageDurationBetweenSamples = sampledInterval / (n - 1)`.
> 4. **Extrapolation to the window edges:** `durationToStart = (firstTs - rangeStart)` sec, `durationToEnd = (rangeEnd - lastTs)` sec. If `durationToStart >= averageDurationBetweenSamples * 1.1`, clamp `durationToStart = averageDurationBetweenSamples / 2` (and same for end). **Positive-counter zero-anchor clamp:** for counters, if all values ≥ 0 and the extrapolated start would imply a value below 0, clamp `durationToStart` to the time it would take to reach 0 at the average rate (`durationToZero`).
> 5. `extrapolateToInterval = sampledInterval + durationToStart + durationToEnd`; `resultValue *= extrapolateToInterval / sampledInterval`.
> 6. For `Rate`: `resultValue /= range_seconds`. For `Increase`/`Delta`: no division.

- [ ] **Step 1: Write the failing literal-value tests (drawn from Prometheus `functions.test`)**

Create `crates/promql/src/functions/extrapolate.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn approx(a: f64, b: f64) {
        assert!((a - b).abs() < 1e-9, "expected {b}, got {a}");
    }

    #[test]
    fn rate_of_clean_counter_matches_prometheus() {
        // Counter 0,1,2,3,4 at 0,60,120,180,240s; rate over [0,300]s window.
        // Prometheus: increase extrapolated then /300. With evenly-spaced points,
        // avgInterval=60s; durationToStart=0, durationToEnd=(300-240)=60 but
        // 60 >= 60*1.1? no -> kept. extrapolateToInterval = 240 + 0 + 60 = 300.
        // resultValue = (4-0) * 300/240 = 5.0 ; rate = 5/300 = 0.016666...
        let ts: Vec<i64> = vec![0, 60_000, 120_000, 180_000, 240_000];
        let vs = vec![0.0, 1.0, 2.0, 3.0, 4.0];
        let r = extrapolated_rate(&ts, &vs, 0, 300_000, 300_000, RangeFn::Rate).unwrap();
        approx(r, 5.0 / 300.0);
    }

    #[test]
    fn increase_reset_correction() {
        // Counter resets: 1,2,1 -> total increase = (2-1) + 1 = 2 (the drop to 1
        // adds the pre-reset value 2). With 3 evenly spaced points the extrapolation
        // fills the window symmetrically.
        let ts: Vec<i64> = vec![0, 60_000, 120_000];
        let vs = vec![1.0, 2.0, 1.0];
        let r = extrapolated_rate(&ts, &vs, 0, 120_000, 120_000, RangeFn::Increase).unwrap();
        // sampledInterval=120s, corrected delta = 2.0; both edge durations 0 ->
        // extrapolateToInterval=120 -> resultValue = 2.0.
        approx(r, 2.0);
    }

    #[test]
    fn delta_is_gauge_no_reset_correction() {
        // Gauge 5 -> 3 over the window: delta = 3 - 5 = -2 (no correction).
        let ts: Vec<i64> = vec![0, 60_000];
        let vs = vec![5.0, 3.0];
        let r = extrapolated_rate(&ts, &vs, 0, 60_000, 60_000, RangeFn::Delta).unwrap();
        approx(r, -2.0);
    }

    #[test]
    fn single_sample_yields_none() {
        assert!(extrapolated_rate(&[0], &[1.0], 0, 60_000, 60_000, RangeFn::Rate).is_none());
    }

    #[test]
    fn irate_uses_last_two() {
        // irate = (last - prev) / (lastTs - prevTs) seconds. 0,1,3 at 0,60,90s.
        let ts: Vec<i64> = vec![0, 60_000, 90_000];
        let vs = vec![0.0, 1.0, 3.0];
        let r = instant_delta(&ts, &vs, IrateFn::Irate).unwrap();
        approx(r, (3.0 - 1.0) / 30.0);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --lib functions::extrapolate`
Expected: FAIL — `cannot find function extrapolated_rate`.

- [ ] **Step 3: Implement `extrapolate.rs`**

```rust
//! The counter-reset + extrapolation core shared by `rate`/`increase`/`delta`
//! (and the instant `irate`/`idelta`). Matches Prometheus's
//! `promql/functions.go` `extrapolatedRate`/`instantValue` byte-for-byte. This is
//! the #1 PromQL correctness trap — do not "simplify".

/// Which range function is being computed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RangeFn {
    /// Per-second counter rate (reset-corrected, divided by range seconds).
    Rate,
    /// Total counter increase over the range (reset-corrected, not divided).
    Increase,
    /// Gauge delta over the range (no reset correction, not divided).
    Delta,
}

/// Which instant function is being computed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IrateFn {
    Irate,
    Idelta,
}

/// Prometheus `extrapolatedRate`. `timestamps`/`values` are the samples in the
/// window `(range_start_ms, range_end_ms]` sorted ascending; `range_ms` is the
/// selector range. Returns `None` for fewer than two samples.
pub(crate) fn extrapolated_rate(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }

    let is_counter = matches!(kind, RangeFn::Rate | RangeFn::Increase);

    // (2) corrected delta.
    let mut result = values[n - 1] - values[0];
    if is_counter {
        for w in values.windows(2) {
            if w[1] < w[0] {
                // reset: add the value before the drop.
                result += w[0];
            }
        }
    }

    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];

    // (3) intervals in seconds.
    let sampled_interval = (last_ts - first_ts) as f64 / 1000.0;
    if sampled_interval == 0.0 {
        return None;
    }
    let average_duration = sampled_interval / (n as f64 - 1.0);

    // (4) extrapolation to window edges.
    let mut duration_to_start = (first_ts - range_start_ms) as f64 / 1000.0;
    let duration_to_end = (range_end_ms - last_ts) as f64 / 1000.0;

    let extrapolation_threshold = average_duration * 1.1;
    let mut extrapolate_to_interval = sampled_interval;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration / 2.0;
    }
    // positive-counter zero-anchor clamp.
    if is_counter && result > 0.0 && !values.is_empty() && values[0] >= 0.0 {
        let duration_to_zero = sampled_interval * (values[0] / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    extrapolate_to_interval += duration_to_start;

    let duration_to_end = if duration_to_end >= extrapolation_threshold {
        average_duration / 2.0
    } else {
        duration_to_end
    };
    extrapolate_to_interval += duration_to_end;

    // (5) scale.
    let factor = extrapolate_to_interval / sampled_interval;
    result *= factor;

    // (6) per-second for rate.
    if kind == RangeFn::Rate {
        let range_seconds = range_ms as f64 / 1000.0;
        if range_seconds == 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}

/// Prometheus `instantValue` for `irate`/`idelta` — uses only the last two
/// samples; `irate` divides by the inter-sample seconds, `idelta` does not.
pub(crate) fn instant_delta(timestamps: &[i64], values: &[f64], kind: IrateFn) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let prev_v = values[n - 2];
    let last_v = values[n - 1];
    let mut result = last_v - prev_v;
    // counter reset on the last pair (irate only): treat last as the increase.
    if kind == IrateFn::Irate && last_v < prev_v {
        result = last_v;
    }
    if kind == IrateFn::Irate {
        let dt = (timestamps[n - 1] - timestamps[n - 2]) as f64 / 1000.0;
        if dt == 0.0 {
            return None;
        }
        result /= dt;
    }
    Some(result)
}
```

> **Verify the zero-anchor clamp against Prometheus `functions.go` at the same tag the `.test` corpus is pinned to** (the exact `durationToZero = sampledInterval * (counterCorrection / resultValue)` form changed subtly across Prometheus 2.x→3.x). The vendored `functions.test` cases (Phase E) are the ground truth; if a vendored case disagrees with this code, the code is wrong — fix it here, not the test.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-promql --lib functions::extrapolate`
Expected: PASS (5 tests).

- [ ] **Step 5: Wire `functions/mod.rs` + `lib.rs` + commit**

Create `crates/promql/src/functions/mod.rs` with `pub(crate) mod extrapolate;`; add `mod functions;` to `lib.rs`.

```bash
cargo fmt -p crabka-promql && cargo clippy -p crabka-promql --all-targets
git add crates/promql/
git commit -m "feat(promql): counter-reset + extrapolation core (rate/increase/delta/irate/idelta)"
```

---

### Task C2: rate-family `ScalarUDF`s over `RangeArray` columns

**Files:**
- Create: `crates/promql/src/functions/rate.rs`
- Modify: `crates/promql/src/functions/mod.rs`, `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `extrapolate::{extrapolated_rate, instant_delta, RangeFn, IrateFn}`, `RangeArray`.
- Produces:
  - `pub(crate) fn rate_udf() -> ScalarUDF` (and `increase_udf`, `delta_udf`, `irate_udf`, `idelta_udf`) — each a `ScalarUDF` whose `invoke` takes two `RangeArray`-dict columns `(timestamp_range, value_range)`, the **per-row aligned grid-timestamp column** (a plain `Int64Array`, one `rangeEnd = t` per grid cell, emitted by `RangeManipulate` at line 1506), and a single scalar literal `range_ms`, and returns a `Float64Array` (one value per grid cell; null where the algorithm returns `None`). **Boundaries are computed per cell:** `range_end = grid_ts[i]`, `range_start = grid_ts[i] - range_ms`. (Threading a single broadcast `range_end_ms` scalar would be correct only for an instant query's single grid point and would corrupt the extrapolation edges of every other grid point in a range query — hence the per-row grid-timestamp column.)
  - `pub(crate) fn register_rate_udfs(ctx: &SessionContext)` — registers all five under their PromQL names.

> The UDF reads each cell with `RangeArray::try_from_dict_array(...).get(i)`, downcasts the timestamp window to `Int64Array` and the value window to `Float64Array`, reads the aligned grid timestamp `t = grid_ts.value(i)`, computes `range_start = t - range_ms` / `range_end = t` for that cell, calls the Phase-C1 core, and appends the result (or null). **Reference: GreptimeDB `functions/{rate,increase,delta,idelta}.rs`.**

- [ ] **Step 1: Write the failing test** — call `rate_udf().invoke_*` (verify the exact invoke entry point at the pinned rev) with a hand-built **two-cell** `RangeArray` (two grid points `t` with *differing* edge gaps, so the per-cell `range_end` is actually exercised — a single-cell test would not catch a broadcast-scalar boundary bug) plus the matching two-element grid-timestamp `Int64Array`, and assert the output `Float64Array` matches the per-cell C1 literal values.

- [ ] **Step 2: Run to verify it fails** — `cannot find function rate_udf`.

- [ ] **Step 3: Implement `rate.rs`**

Build each UDF with `ScalarUDF::new_from_impl(...)` over a `struct RateUdf { kind: RangeFn }` implementing `ScalarUDFImpl` (`name`, `signature`, `return_type` = `Float64`, `invoke_with_args`). The `invoke` body decodes the two `RangeArray` columns and the per-row grid-timestamp `Int64Array`, reads the `range_ms` scalar literal, loops cells computing `range_end = grid_ts.value(i)` and `range_start = range_end - range_ms` for each cell, calls `extrapolated_rate(.., range_start, range_end, range_ms, kind)`, builds a `Float64Array` with nulls for `None`. Provide the full real decode/loop/build code; keep the `ScalarUDFImpl` trait method set + the `invoke_with_args` signature behind a "verify against datafusion rev `0838a4d`" note (the invoke signature `ScalarFunctionArgs` vs `&[ColumnarValue]` is the churn point).

- [ ] **Step 4: Run + wire + commit** (`feat(promql): rate-family ScalarUDFs over RangeArray columns`).

---

### Task C3: Planner scaffold + parse entry + context plumbing

**Files:**
- Create: `crates/promql/src/planner/mod.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `promql_parser::parser::{parse, Expr}`, `MetricStore`, `EngineOpts`-like context.
- Produces:
  - `pub(crate) struct PlannerContext { pub tenant: String, pub start_ms: i64, pub end_ms: i64, pub step_ms: i64, pub lookback_delta_ms: i64, pub eval_range_ms: i64 }`
  - `pub(crate) struct PromqlPlanner<'a, S: MetricStore> { store: &'a S, ctx: PlannerContext }`
  - `pub(crate) async fn plan(&self, expr: &Expr) -> Result<PlannedQuery, PromqlError>` — recursion entry (dispatches on `Expr` variant; only the variants this slice supports are wired, others → `PromqlError::Unsupported`).
  - `pub(crate) enum PlannedQuery { Scalar(f64), Str(String), DataFusion { ctx: SessionContext, plan: LogicalPlan, value_kind: ValueKind } }`; `pub(crate) enum ValueKind { InstantVector, RangeVector }`.
  - `pub fn parse_promql(query: &str) -> Result<Expr, PromqlError>` — wraps `promql_parser::parser::parse`, mapping its `String` error to `PromqlError::Parse`.

- [ ] **Step 1: Write the failing test** — `parse_promql("up")` is `Ok`; `parse_promql("up {{{")` is `Err(PromqlError::Parse(_))`; an unsupported expr (e.g. a subquery `up[5m:1m]`) plans to `PromqlError::Unsupported`.

- [ ] **Step 2: Run to verify it fails** — `cannot find function parse_promql`.

- [ ] **Step 3: Implement `planner/mod.rs`** — the `parse_promql` wrapper, the context/planner structs, and a `plan` that matches on `Expr::{NumberLiteral, StringLiteral, VectorSelector, MatrixSelector, Call, AggregateExpr, BinaryExpr, ParenExpr, UnaryExpr}` — `NumberLiteral`/`StringLiteral` resolve immediately, the rest delegate to the per-construct modules (added in C4/C5/Phase D) or return `Unsupported` until those land. Provide the full real scaffold; stub the delegations with `todo!`-free `Unsupported` returns so the crate compiles and the test passes.

- [ ] **Step 4: Wire `lib.rs` (`pub use planner::parse_promql;`) + run + commit** (`feat(promql): planner scaffold + promql-parser entry`).

---

### Task C4: Instant + range (matrix) vector selector lowering

**Files:**
- Create: `crates/promql/src/planner/selector.rs`
- Modify: `crates/promql/src/planner/mod.rs`

**Interfaces:**
- Consumes: `MetricStore::scan`, `ScanResult`, the operators `SeriesDivide`/`SeriesNormalize`/`InstantManipulate`/`RangeManipulate`, `crabka_blockstore::{LabelMatcher, MatchOp}`, `promql_parser::parser::{VectorSelector, MatrixSelector}`.
- Produces:
  - `pub(crate) async fn plan_vector_selector(planner, vs: &VectorSelector) -> Result<PlannedQuery, PromqlError>` — converts the selector's matchers (including the `__name__` from the metric name) to `LabelMatcher`s, calls `store.scan(tenant, matchers, start-lookback, end)`, registers the float table, and builds the logical plan `floats -> SeriesDivide -> SeriesNormalize -> InstantManipulate` (the instant-vector pipeline).
  - `pub(crate) async fn plan_matrix_selector(planner, ms: &MatrixSelector) -> Result<PlannedQuery, PromqlError>` — same scan + `SeriesDivide -> SeriesNormalize -> RangeManipulate` (the range-vector pipeline; `range_ms` from the selector).
  - `pub(crate) fn promql_matchers_to_blockstore(vs: &VectorSelector) -> Result<Vec<LabelMatcher>, PromqlError>` — maps `promql_parser`'s `Matcher`/`MatchOp` to blockstore's `LabelMatcher`/`MatchOp` (the metric name becomes a `__name__` Eq matcher).

- [ ] **Step 1: Write the failing test** — build an `InMemoryMetricStore` with one series `up{job="a"}` at ts {0, 60_000}, plan `up{job="a"}` via the planner at a fixed grid, execute the resulting `LogicalPlan` against the `ScanResult`'s `ctx`, and assert two grid points come back. (Drive via a small test helper that executes a `PlannedQuery::DataFusion` plan and collects.)

- [ ] **Step 2: Run to verify it fails** — `cannot find function plan_vector_selector`.

- [ ] **Step 3: Implement `selector.rs`**

`promql_matchers_to_blockstore`: iterate `vs.matchers.matchers`, map `MatchOp::Equal→Eq`, `NotEqual→Neq`, `Re→Re`, `NotRe→Nre`; if `vs.name` is `Some(n)` push `__name__ Eq n`. Then `scan`, take `float_table`, and assemble the `LogicalPlan` by wrapping the registered `TableScan` in `Extension(SeriesDivide{...})` → `Extension(SeriesNormalize{...})` → `Extension(InstantManipulate{...})` using `LogicalPlanBuilder` + `LogicalPlan::Extension`. Provide the full real construction; keep the `LogicalPlan::Extension`/`Extension { node: Arc::new(...) }` wrapping behind a "verify against rev `0838a4d`" note. Wire `plan_vector_selector`/`plan_matrix_selector` into the `plan` dispatch in `planner/mod.rs`.

- [ ] **Step 4: Run + commit** (`feat(promql): instant + matrix vector selector lowering`).

---

### Task C5: Wire the rate-family calls into the planner

**Files:**
- Modify: `crates/promql/src/planner/mod.rs` (extend `Call` dispatch), `crates/promql/src/functions/mod.rs`
- Create: `crates/promql/src/planner/call.rs`

**Interfaces:**
- Consumes: `plan_matrix_selector`, `register_rate_udfs`, the rate UDFs.
- Produces:
  - `pub(crate) async fn plan_call(planner, call: &promql_parser::parser::Call) -> Result<PlannedQuery, PromqlError>` — for `rate`/`increase`/`delta`/`irate`/`idelta`: plan the single matrix-selector arg, register the rate UDFs on its `ctx`, and project the `RangeArray` timestamp + value columns **plus the aligned grid-timestamp column** (emitted by `RangeManipulate`) and the `range_ms` literal through the matching UDF, yielding an instant vector. Unsupported function names → `PromqlError::Unsupported(name)`.

- [ ] **Step 1: Write the failing tests** — (a) **instant grid point:** `InMemoryMetricStore` with a counter `http_requests_total` 0,1,2,3,4 at 60s spacing; `query_instant`-style drive of `rate(http_requests_total[5m])` at the right time; assert the single returned value ≈ the C1 literal (`5/300`). (b) **range query (≥2 grid points):** drive `rate(http_requests_total[5m])` over a range with **at least two grid points whose window edge gaps differ** (e.g. one grid `t` landing exactly on the last sample, another `t` with a non-zero `durationToEnd`), and assert each grid point's value matches the per-cell C1 extrapolation — this exercises the per-row grid-timestamp boundary and would catch a broadcast-scalar `range_end` regression that the single-grid-point test (a) cannot. (Use a test helper executing the `PlannedQuery`; full `query_instant`/`query_range` lands in Phase D/E — here drive the plan directly.)

- [ ] **Step 2: Run to verify it fails** — `cannot find function plan_call`.

- [ ] **Step 3: Implement `call.rs`** — match the call name, plan the matrix arg, `register_rate_udfs(&ctx)`, build a projection that calls the UDF over the timestamp+value `RangeArray` columns + the aligned grid-timestamp column + the `range_ms` literal, return `PlannedQuery::DataFusion { value_kind: InstantVector }`. Wire into `plan`'s `Call` arm.

- [ ] **Step 4: Phase C gate + commit**

```bash
cargo test -p crabka-promql && cargo clippy -p crabka-promql --all-targets && cargo fmt -p crabka-promql --check
git add crates/promql/
git commit -m "feat(promql): lower rate-family calls onto matrix selector + UDF projection"
```

---

## Phase D — aggregations + binary ops + engine

> **Batching:** D1 (aggregations) and D2 (binary ops) touch disjoint files (`planner/aggregate.rs` vs `planner/binary.rs`) and may run as a parallel batch, each extending the `plan` dispatch in `planner/mod.rs` (merge those edits). D3 (engine + `QueryResult` assembly) depends on D1 + D2 + all of Phase C. Recommended: {D1, D2 in parallel} → D3.

### Task D1: Core aggregations — `sum`/`avg`/`min`/`max`/`count` with `by`/`without`

**Files:**
- Create: `crates/promql/src/planner/aggregate.rs`
- Modify: `crates/promql/src/planner/mod.rs`

**Interfaces:**
- Consumes: the instant-vector `PlannedQuery` of the aggregate's inner expr, `promql_parser::parser::AggregateExpr`.
- Produces:
  - `pub(crate) async fn plan_aggregate(planner, agg: &AggregateExpr) -> Result<PlannedQuery, PromqlError>` — plan the inner expr to an instant vector, then build a DataFusion `Aggregate` grouping by the `by` labels (or all-labels-minus-`without`-minus-`__name__`) with the matching aggregate function (`sum`/`avg`/`min`/`max`/`count`). `without` always drops `__name__`; `by ()` collapses to a single group; the result drops `__name__` (aggregations don't carry a metric name).

> **Grouping semantics (spec §6.2):** `by (l...)` groups by exactly those labels; `without (l...)` groups by all labels except those + `__name__`. `count` counts series per group (`count(*)`), the others reduce the value column. This slice does the five plain aggregations only — `topk`/`bottomk`/`quantile`/`stddev`/`count_values`/`group` are Slice 3.

- [ ] **Step 1: Write the failing test** — two series `up{job="a"}=1`, `up{job="b"}=1` at one ts; plan `sum(up)` → one result value `2`; plan `sum by (job) (up)` → two results each `1`; plan `count(up)` → `2`. Drive via the test helper.

- [ ] **Step 2: Run to verify it fails** — `cannot find function plan_aggregate`.

- [ ] **Step 3: Implement `aggregate.rs`** — resolve the grouping label set from `agg.modifier` (`AggModifier::By`/`Without`), build the `GROUP BY` exprs + the aggregate expr via `LogicalPlanBuilder::aggregate`, and project out `__name__`. Provide the full real code; keep the `LogicalPlanBuilder::aggregate(group_exprs, agg_exprs)` shape behind a "verify against rev `0838a4d`" note. Wire into `plan`'s `AggregateExpr` arm.

- [ ] **Step 4: Run + commit** (`feat(promql): core aggregations with by/without`).

---

### Task D2: Binary ops — arithmetic + comparison + vector matching + `bool`

**Files:**
- Create: `crates/promql/src/planner/binary.rs`
- Modify: `crates/promql/src/planner/mod.rs`

**Interfaces:**
- Consumes: planned instant-vectors / scalars of the two operands, `promql_parser::parser::{BinaryExpr, token}`.
- Produces:
  - `pub(crate) async fn plan_binary(planner, be: &BinaryExpr) -> Result<PlannedQuery, PromqlError>` covering:
    - **scalar ⊗ scalar** → constant-fold to `PlannedQuery::Scalar`.
    - **vector ⊗ scalar** / **scalar ⊗ vector** → project the arithmetic/comparison over the vector's value column.
    - **vector ⊗ vector** (one-to-one) → join the two instant vectors on the matching label set (`on(l...)` = those labels; `ignoring(l...)` = all labels except those + `__name__`; default = all labels except `__name__`), apply the op to the paired values.
    - arithmetic (`+ - * / % ^ atan2`), comparison (`== != > < >= <=`); without `bool` a comparison **filters** (drops non-matching rows, keeps LHS value + labels); with `bool` it yields `1`/`0`.
  - `group_left`/`group_right` (many-to-one) is **Slice 3** — return `Unsupported` for them here.

> **Precedence/associativity** is handled by `promql-parser`'s AST (we recurse it), so the planner does not re-implement precedence — but the result-label rules (comparison-without-bool keeps LHS labels; arithmetic drops `__name__`) must be exact (spec §6.2).

- [ ] **Step 1: Write the failing tests** — (a) `2 * 3` → scalar `6`; (b) `up * 2` over `up{job="a"}=1` → `2` with labels preserved; (c) `a + b` for `a{x="1"}=10`, `b{x="1"}=5` matched on `x` → `15`; (d) `a > bool 0` → `1`; (e) `a > 100` (no bool) → empty (filtered).

- [ ] **Step 2: Run to verify it fails** — `cannot find function plan_binary`.

- [ ] **Step 3: Implement `binary.rs`** — dispatch on operand kinds; for vector⊗vector build a DataFusion `JOIN` on the matching-label columns then project the op; for comparison build either a `FILTER` (no `bool`) or a `CASE WHEN ... THEN 1 ELSE 0` (with `bool`); drop `__name__` for arithmetic. Provide the full real code; keep the join/`when`-builder shapes behind a "verify against rev `0838a4d`" note. Wire into `plan`'s `BinaryExpr` arm.

- [ ] **Step 4: Run + commit** (`feat(promql): binary ops — arithmetic/comparison + on/ignoring matching + bool`).

---

### Task D3: `PromqlEngine` — step grid, execution, `QueryResult` assembly

**Files:**
- Create: `crates/promql/src/engine.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `parse_promql`, `PromqlPlanner`, `PlannedQuery`, `MetricStore`, the result model.
- Produces:
  - `pub struct EngineOpts { pub lookback_delta_ms: i64, pub max_samples: usize }`; `impl Default for EngineOpts` (`lookback_delta_ms: 300_000`, `max_samples: 50_000_000`).
  - `pub struct PromqlEngine<S: MetricStore> { store: Arc<S>, opts: EngineOpts }`
  - `pub fn new(store: Arc<S>, opts: EngineOpts) -> Self`
  - `pub async fn query_instant(&self, tenant: &str, query: &str, time_ms: i64) -> Result<QueryResult, PromqlError>` — parse, plan with a single-point grid (`start=end=time_ms`, `step=0`), execute, assemble (`Scalar` for scalar exprs, `InstantVector` for vector exprs, `Str` for string exprs).
  - `pub async fn query_range(&self, tenant: &str, query: &str, start_ms: i64, end_ms: i64, step_ms: i64) -> Result<QueryResult, PromqlError>` — parse, plan over the step grid, execute, assemble `RangeMatrix` (or `Scalar`-broadcast for scalar exprs per Prometheus). Enforces `max_samples` (error `PromqlError::Exec` past the cap).
  - `pub(crate) fn collect_to_result(...)` — turn collected `RecordBatch`es (fingerprint, timestamp, value/histogram + label columns) into `InstantSample`/`RangeSeries`, reconstructing `Labels` from the carried label columns.

> **Label reconstruction:** the leaf scan tables carry only `series_fingerprint` + `timestamp` + value; the engine needs the label sets to build `Labels` in the result. Resolve them via `MetricStore::series(tenant, matchers, ...)` keyed by fingerprint (the in-memory store and the blockstore-backed store both expose this). Cache the fingerprint→Labels map per query.

- [ ] **Step 1: Write the failing end-to-end tests**

Create `crates/promql/src/engine.rs` tests:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_blockstore::Labels;

    use super::*;
    use crate::in_memory::InMemoryMetricStore;
    use crate::result::{QueryResult, SampleValue};

    fn lbls(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    fn engine_with(store: InMemoryMetricStore) -> PromqlEngine<InMemoryMetricStore> {
        PromqlEngine::new(Arc::new(store), EngineOpts::default())
    }

    #[tokio::test]
    async fn instant_selector_returns_latest_within_lookback() {
        let mut s = InMemoryMetricStore::new();
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 0, 1.0);
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 60_000, 1.0);
        let e = engine_with(s);
        let r = e.query_instant("t", "up", 120_000).await.unwrap();
        match r {
            QueryResult::InstantVector(v) => {
                assert!(v.len() == 1);
                assert!(v[0].value == SampleValue::Float(1.0));
                assert!(v[0].labels.get("job") == Some("a"));
            }
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn instant_sum_aggregates() {
        let mut s = InMemoryMetricStore::new();
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "a")]), 0, 1.0);
        s.push_float("t", lbls(&[("__name__", "up"), ("job", "b")]), 0, 1.0);
        let e = engine_with(s);
        let r = e.query_instant("t", "sum(up)", 0).await.unwrap();
        match r {
            QueryResult::InstantVector(v) => {
                assert!(v.len() == 1);
                assert!(v[0].value == SampleValue::Float(2.0));
            }
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn scalar_literal_query() {
        let e = engine_with(InMemoryMetricStore::new());
        let r = e.query_instant("t", "42", 1000).await.unwrap();
        assert!(r == QueryResult::Scalar { ts_ms: 1000, value: 42.0 });
    }

    #[tokio::test]
    async fn range_query_builds_matrix() {
        let mut s = InMemoryMetricStore::new();
        for (ts, v) in [(0_i64, 1.0), (60_000, 1.0), (120_000, 1.0)] {
            s.push_float("t", lbls(&[("__name__", "up")]), ts, v);
        }
        let e = engine_with(s);
        let r = e.query_range("t", "up", 0, 120_000, 60_000).await.unwrap();
        match r {
            QueryResult::RangeMatrix(m) => {
                assert!(m.len() == 1);
                assert!(m[0].samples.len() == 3);
            }
            other => panic!("expected matrix, got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type PromqlEngine`.

- [ ] **Step 3: Implement `engine.rs`** — `query_instant`/`query_range` parse + plan + execute (`ctx.execute_logical_plan(plan).collect()` — verify entry point) + `collect_to_result`. Provide the full real assembly code; keep the plan-execution entry point behind a "verify against rev `0838a4d`" note.

- [ ] **Step 4: Wire `lib.rs`** — `pub use engine::{EngineOpts, PromqlEngine};`.

- [ ] **Step 5: Phase D gate + commit**

```bash
cargo test -p crabka-promql && cargo clippy -p crabka-promql --all-targets && cargo fmt -p crabka-promql --check
git add crates/promql/
git commit -m "feat(promql): PromqlEngine — step grid + execution + QueryResult assembly"
```

---

## Phase E — the Prometheus `.test` conformance harness

> **Batching:** E1 (DSL parser) and E2 (vendoring the `.test` files + attribution) touch disjoint files and may run in parallel. E3 (the runner + the integration test) depends on E1 + E2 + the engine (Phase D). Recommended: {E1, E2 in parallel} → E3.

### Task E1: The `.test` DSL parser

**Files:**
- Create: `crates/promql/src/conformance/mod.rs`
- Modify: `crates/promql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct TestFile { pub statements: Vec<Statement> }`
  - `pub enum Statement { Load { step_ms: i64, series: Vec<LoadSeries> }, EvalInstant { at_ms: i64, expr: String, expect: Vec<ExpectLine>, fail: bool }, EvalRange { start_ms: i64, end_ms: i64, step_ms: i64, expr: String, expect: Vec<ExpectLine>, fail: bool }, Clear }`
  - `pub struct LoadSeries { pub metric: String, pub values: Vec<SampleSpec> }` where `SampleSpec` encodes the expanding-point syntax (`1+2x3` = 1,3,5,7; `1x3` = 1,1,1,1; `_` = gap; `stale`).
  - `pub struct ExpectLine { pub metric: String, pub value: f64 }`
  - `pub fn parse_test_file(src: &str) -> Result<TestFile, PromqlError>` — parses the `load`/`eval instant at`/`eval range from..to step..`/`clear` DSL, the `{label="v"}` series syntax, the expanding-point value syntax, and the `eval_fail`/`expect fail` forms.

> **Scope:** implement the *legacy* assertion form (`eval instant at <dur> <expr>` followed by indented `<metric> <value>` lines) which the pinned-tag corpus uses; the float-only subset (native-histogram literals are Slice 3). Duration parsing: `5m`→`300_000`, `1h`→`3_600_000`, etc. Expanding points: `start(+step)x count` and `startxcount`, `_` = missing, `stale` = stale marker.

- [ ] **Step 1: Write the failing tests** — `parse_test_file` of a small inline DSL string with one `load 1m`, one series `metric{a="b"} 0+1x4`, one `eval instant at 3m metric{a="b"}` + expect `metric{a="b"} 3` parses to the expected structs; the expanding-point `0+1x4` expands to `[0,1,2,3,4]`; `clear` parses.

- [ ] **Step 2: Run to verify it fails** — `cannot find function parse_test_file`.

- [ ] **Step 3: Implement `conformance/mod.rs`** — a line-oriented parser: split into statements on `load`/`eval`/`clear` keywords, parse the series lines + expanding-point values, parse durations. Provide the full real parser code (this is plain Rust string parsing — no churn surface).

- [ ] **Step 4: Wire `lib.rs` (`pub use conformance::{parse_test_file, TestFile, Statement};`) + run + commit** (`feat(promql): Prometheus .test DSL parser`).

---

### Task E2: Vendor 2–3 Prometheus `.test` cases

**Files:**
- Create: `crates/promql/tests/testdata/aggregators.test` (subset, vendored)
- Create: `crates/promql/tests/testdata/functions.test` (subset, vendored — must include the rate/increase/delta cases that pin Phase C1)
- Create: `crates/promql/tests/testdata/ATTRIBUTION.md` (Apache-2.0 notice + the pinned Prometheus tag + commit SHA + upstream path)

**Interfaces:**
- Produces: vendored test corpus + provenance.

- [ ] **Step 1: Vendor the files** — copy the **subset** of `promql/promqltest/testdata/aggregators.test` and `promql/promqltest/testdata/functions.test` (rate/increase/delta/irate/idelta + sum/avg/min/max/count blocks only — strip cases using features deferred to Slice 3: `histogram_quantile`, subqueries, `topk`, `@`, native histograms) from a **pinned Prometheus release tag** (record the exact tag, e.g. `v3.x.y`, and its commit SHA). Keep the upstream comment headers.

- [ ] **Step 2: Write `ATTRIBUTION.md`**

```markdown
# Vendored Prometheus PromQL conformance tests

These `.test` files are a **subset** of Prometheus's PromQL test corpus, copied
verbatim (cases using Slice-3 features removed) from:

- Upstream: https://github.com/prometheus/prometheus
- Path: `promql/promqltest/testdata/{aggregators,functions}.test`
- Pinned tag: `<TAG>` (commit `<SHA>`)

Prometheus is licensed under the Apache License 2.0. The full license text is in
the upstream `LICENSE` file. These files retain their original copyright; they are
used here unmodified except for the removal of test cases exercising features not
yet implemented in `crabka-promql` (tracked for Slice 3).
```

> Replace `<TAG>`/`<SHA>` with the exact pinned values at vendoring time. The full 21-file corpus is wired in Slice 3 — this slice vendors only the two subsets the harness needs to be credible.

- [ ] **Step 3: Commit** (`test(promql): vendor Prometheus aggregators/functions .test subsets (Apache-2.0)`).

---

### Task E3: The conformance runner + integration test

**Files:**
- Create: `crates/promql/src/conformance/runner.rs`
- Create: `crates/promql/tests/conformance.rs`
- Modify: `crates/promql/src/conformance/mod.rs`, `crates/promql/src/lib.rs`

**Interfaces:**
- Consumes: `TestFile`/`Statement`, `InMemoryMetricStore`, `PromqlEngine`, `QueryResult`.
- Produces (all under `pub mod testkit`, re-exported at the crate root — **frozen names Slice 3 interlocks on**):
  - `pub async fn testkit::run_test_file(file: &TestFile) -> Result<(), PromqlError>` — interprets statements: `Load` pushes the expanded `(metric labels, ts, value)` rows into an `InMemoryMetricStore` (ts derived from the `load` step and the value index); `EvalInstant` runs `query_instant` and asserts the `InstantVector` matches the `expect` lines (label set + value, order-independent, float tolerance `1e-9`); `EvalRange` runs `query_range`; `Clear` resets the store; `fail` statements assert an error. Returns the first mismatch as `PromqlError::Exec`.
  - `pub async fn testkit::run_test_path(path: &str) -> Result<(), PromqlError>` — thin convenience wrapper: read the file at `path`, `parse_test_file`, then `run_test_file`. (Slice 3's corpus driver calls this path-based form.)
  - `pub(crate) fn metric_to_labels(metric: &str) -> Labels` — parse `name{a="b",c="d"}` into `Labels` (with `__name__`).

- [ ] **Step 1: Write the failing integration test**

Create `crates/promql/tests/conformance.rs`:

```rust
//! Runs the vendored Prometheus `.test` subsets through the engine via the
//! in-memory store. The headline conformance signal for Slice 2.

use crabka_promql::testkit::run_test_path;

async fn run_file(path: &str) {
    // `run_test_path` = read + `parse_test_file` + `run_test_file(&TestFile)`.
    run_test_path(path).await.expect("conformance");
}

#[tokio::test]
async fn aggregators_subset_conforms() {
    run_file("tests/testdata/aggregators.test").await;
}

#[tokio::test]
async fn functions_subset_conforms() {
    run_file("tests/testdata/functions.test").await;
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-promql --test conformance`
Expected: FAIL — `cannot find function run_test_path` / unresolved `crabka_promql::testkit` (then, once wired, real conformance failures to drive fixes against).

- [ ] **Step 3: Implement `runner.rs`** — the statement interpreter (`run_test_file(&TestFile)`) + the `run_test_path(&str)` read+parse+run wrapper + `metric_to_labels` + the `InstantVector`/`expect` comparison (build a `BTreeMap<Labels, f64>` from each side and compare with tolerance). Provide the full real code. Expose both under a `pub mod testkit` (whose `run_test_file`/`run_test_path` are the frozen names Slice 3 consumes); wire it into `conformance/mod.rs` and re-export `testkit` (and `run_test_file`/`run_test_path` at the crate root) from `lib.rs`.

- [ ] **Step 4: Iterate to green**

Run the conformance test; each real failure is a planner/operator/UDF bug — fix it in the relevant Phase B–D file (the `.test` corpus is ground truth, never edit the vendored cases to pass). Re-run until both files conform. If a vendored case turns out to need a Slice-3 feature missed in the E2 pruning, remove that single case and note it in `ATTRIBUTION.md`.

- [ ] **Step 5: Final whole-crate gate + commit**

```bash
cargo test -p crabka-promql && cargo clippy -p crabka-promql --all-targets && cargo fmt -p crabka-promql --check
git add crates/promql/
git commit -m "test(promql): Prometheus .test conformance harness over InMemoryMetricStore"
```

---

## Self-review

**Spec coverage (against §6 PromQL engine + §11 Slice 2):**
- `promql-parser` integration + parse entry (`promql_parser::parser::parse`) → Tasks A1, C3.
- The four custom operators (`SeriesDivide`/`SeriesNormalize`/`InstantManipulate`/`RangeManipulate`) each as `UserDefinedLogicalNodeCore` + `ExecutionPlan` + stream → Tasks B2, B3, B4, B5.
- The `RangeArray` custom Arrow array (windowed view + DataFusion dict-array column encoding, left-open/right-closed range vectors) → Tasks B1, B5.
- rate-family as `ScalarUDF`s with the exact counter-reset + extrapolation algorithm (reset-correct on decrease; `avgInterval = sampledInterval/(n-1)`; `1.1×` threshold; half-interval cap; positive-counter zero-anchor clamp) → Tasks C1, C2, C5; pinned by literal-value tests + the vendored `functions.test`.
- Instant + matrix (range) vector selector lowering honoring lookback-delta + staleness → Tasks B4, C4.
- Core aggregations `sum`/`avg`/`min`/`max`/`count` with `by`/`without` (`without` drops `__name__`) → Task D1.
- Binary ops (arithmetic + comparison, `on`/`ignoring` one-to-one matching, `bool` modifier, comparison-filter vs `bool` semantics; precedence delegated to the parser AST) → Task D2.
- `query_instant` + `query_range` over a step grid + `QueryResult` assembly → Task D3.
- `MetricStore` trait + `InMemoryMetricStore` building float/histogram DataFusion tables → Tasks A4, A5.
- Prometheus `.test` conformance harness (DSL parser: `load`/`eval instant at`/`eval range`/expanding-points/`clear`; 2 vendored subsets, Apache-2.0 attribution, pinned tag) → Tasks E1, E2, E3.
- **Frozen cross-slice contract** (`MetricStore`/`ScanResult`/`PromqlEngine`/`EngineOpts`/`QueryResult`/`InstantSample`/`RangeSeries`/`SampleValue`/`PromqlError` + the operator/UDF/`RangeArray` names + the `.test` DSL) defined at the exact signatures the prompt pins → §"Shared cross-slice contract" + the task interfaces.

**Deferred (correctly, to Slice 3):** `histogram_quantile` (classic + native) + native accessors; the long-tail function catalog (`topk`/`bottomk`/`quantile`/`stddev`/`count_values`/`group`, `_over_time` family, `label_replace`/`label_join`, `clamp*`, `predict_linear`); subqueries (`expr[range:res]`); the general `@`/`offset` form (only constant `offset` add is wired here); `group_left`/`group_right` many-to-one matching; native-histogram `.test` literals; the full 21-file corpus. Each is flagged at its task (C3/D2 return `PromqlError::Unsupported` for unimplemented constructs, so the boundary is enforced, not silent).

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every code-bearing step ships complete, runnable real code (`error.rs`, `result.rs`, `store.rs`, `in_memory.rs`, `range_array.rs`, `extrapolate.rs`, the DSL parser) or — for the churn-prone DataFusion-internal surfaces (`UserDefinedLogicalNodeCore`, `ExecutionPlan`, `RecordBatchStream`, `ScalarUDFImpl`, the dict-array column encoding, the `LogicalPlan::Extension`/`aggregate`/join builders, the plan-execution entry point) — the **struct shape + field set + a behavior-pinning test + an explicit "verify against datafusion rev `0838a4d` / GreptimeDB `extension_plan/` source" note**, mirroring how the Slice 1 / blockstore plans bound their arrow/DataFusion hand-waves. No trait method signature is fabricated as fact.

**Type consistency:** `MetricStore`'s four method signatures are identical across A4 (definition), A5 (impl), C4 (consumer), and D3 (engine). `ScanResult` fields (`ctx`/`float_table`/`histogram_table`) are stable A4↔A5↔C4. `PromqlError` variants (`Parse`/`Plan`/`Exec`/`Store`/`Unsupported`) are the single error type across all tasks. `RangeArray`'s public surface (`from_ranges`/`get`/`len`/`ranges`/`into_dict_array`/`try_from_dict_array`) is fixed in B1 and consumed unchanged in B5/C2. `QueryResult`/`InstantSample`/`RangeSeries`/`SampleValue` defined once (A3) and assembled in D3. The `RangeFn`/`IrateFn` enums + `extrapolated_rate`/`instant_delta` signatures are stable C1↔C2. The frozen public names match the prompt's pinned contract exactly.

**Known risks (flagged, not hidden):**
1. **DataFusion-internal trait churn** (the four operators, the UDF invoke signature, the dict-array encoding, the logical/physical plan builders, the execute entry point) — the single largest risk. Contained to `src/extension/`, `src/functions/rate.rs`, `src/range_array.rs`, and the per-construct planner builders, each behind a behavior-pinning test + a verify-against-rev note. Drift surfaces as a compile error against a green test, never as silent wrong results.
2. **Counter-reset/extrapolation fidelity** — the #1 correctness trap. Triple-guarded: literal-value unit tests in C1, the UDF round-trip in C2, and the vendored `functions.test` in E3 (ground truth). The zero-anchor clamp form is explicitly flagged to verify against the Prometheus tag the corpus is pinned to.
3. **Slice executability** — this slice is large; the phase batching (A→B→C→D→E, with the noted intra-phase parallel batches) keeps each sub-batch's file sets disjoint per `CLAUDE.md`, and each phase ends at a green whole-crate gate so a sub-batch can be reviewed and merged before the next starts.
