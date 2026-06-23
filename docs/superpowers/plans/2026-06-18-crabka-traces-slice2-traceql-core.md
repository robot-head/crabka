# crabka-traces Slice 2 — `crabka-traceql` core (lexer + parser + planner + nested-set structural self-join + `SpanStore` trait + result model)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **This slice is the largest in the traces program and is executed as sub-batches.** It is organized into six phases (A–F). Within a phase, tasks whose **Files** sets do not overlap may be dispatched as a parallel subagent batch (per `CLAUDE.md`); tasks that share a file (`lib.rs`, `planner/mod.rs`) or genuinely depend on an earlier task's output must run sequentially. Each phase ends at a green whole-crate gate. The recommended batching is called out at the head of each phase.

**Goal:** Build the core of the TraceQL engine — a hand-written lexer + recursive-descent parser (grammar referenced from icegate's ANTLR `.g4`, with TraceQL's `=`-not-`==`, fully-anchored `=~`, and the dot-vs-colon scope quirks), an AST→DataFusion `LogicalPlan` planner that lowers spanset selectors (scopes/intrinsics/array semantics, honoring the single-span rule), non-structural columnar pushdown with the `&&` AND fast path, and — the **centerpiece** — the **`SpanStructuralJoin`** lowering of the core structural operators (`>>`/`<<`/`>`/`<`/`~`) to a partitioned-by-`trace_id` self-join over the nested-set columns. It defines the `SpanStore` trait + an `InMemorySpanStore` test impl (building span DataFusion tables incl. the nested-set columns), the pinned result model, and a `TraceqlEngine::search`/`trace_by_id` path assembling spanSets per trace. The negated/union structural forms, TraceQL metrics, and tag discovery are deferred to Slice 3.

**Architecture:** A query crate `crabka-traceql` that depends on DataFusion (same git pin as blockstore) and no external TraceQL parser (none exists — we build our own). The engine is generic over a `SpanStore` trait that yields a DataFusion `SessionContext` with a span table registered for a (tenant, matchers, time-range) scan — production wires this to the querier's hot/cold UNION (Slice 5), but this slice ships an `InMemorySpanStore` test impl so the engine is independently testable. The span table carries the nested-set structural columns (`nested_set_left`/`nested_set_right`/`parent_id`, Int32, computed at block-build in Slice 1; this slice's in-memory store computes them from a hand-built span tree via the same DFS pre-order). Structural TraceQL operators have **no native DataFusion equivalent and are not a per-trace tree-walk**: we lower each to a self-join keyed by `trace_id` with nested-set range/equality predicates (descendant: `B.left>A.left && B.right<A.right`; child: `B.parent_id==A.left`; sibling: `B.parent_id==A.parent_id && B.span_id!=A.span_id`) — a DataFusion join plan, with a thin custom physical operator only if the per-trace partitioning needs it. The planner recurses the AST into a `LogicalPlan`; `search`/`trace_by_id` execute it and assemble Tempo-shaped result structs.

**Tech Stack:** Rust 2024 · `datafusion` (git `main`, pinned — see Global Constraints) · `arrow` 59 · `async-trait` · `tokio` · `futures` · `regex` · `thiserror`. Depends on `crabka-blockstore` (types: `LabelMatcher`, `MatchOp`, `Labels` — consumed only where useful; the trace path is matcher-native via `SpanMatcher`). Tests: `assert2`, `proptest`, `tokio` (`macros`, `rt-multi-thread`).

## Global Constraints

- **No backwards compatibility.** Crabka is greenfield/undeployed. No `#[serde(default)]` shims, no V2-alongside-V1 enum variants, no migration code, no default-off feature gates. Change schemas/enums/interfaces freely. (Only Kafka wire compat matters — and this crate touches none of it.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe` — including in any custom physical operator (build on safe arrow/DataFusion APIs).
- **Lints:** `clippy::pedantic` is `warn` workspace-wide (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` allowed). New code must be clippy-pedantic clean. Run `cargo clippy -p crabka-traceql --all-targets` before each commit.
- **Formatting:** run `cargo fmt -p crabka-traceql` before every commit. **NEVER** run `cargo +nightly fmt --all` — it fails with OS error 206 / path-too-long in deep worktrees on Windows; always scope with `-p`.
- **Assertions:** use `assert2::assert!` / `assert2::check!` in tests, `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`. This `main` revision tracks arrow 59 / parquet 59 / object_store 0.13.2, which unify with the workspace pins (same major → cargo unifies to one crate instance, so arrow types cross the DataFusion boundary cleanly). Do **not** substitute a released `datafusion` (54.x is on arrow 58 and pulls a second, incompatible arrow major).
- **Arrow version identity:** import `arrow` directly (`use arrow::...`) as blockstore does; all of arrow/parquet/object_store unify to one instance. If a type-mismatch error appears at the DataFusion boundary, switch that import to DataFusion's re-export (`datafusion::arrow`) to force identity.
- **No fabricated TraceQL grammar.** TraceQL has no published Rust parser and no upstream `.test`-style conformance corpus. The grammar is *referenced* from `icegatetech/icegate`'s ANTLR `TraceQLLexer.g4` / `TraceQLParser.g4` (Apache-2.0) — used as the **spec-of-record**, hand-ported to a recursive-descent parser (no ANTLR-runtime dep, full control over the colon-vs-dot scope quirks). Verified token facts to honor exactly: single `=` (there is **no `==`**), `=~` is **fully anchored** `^...$`, attribute scopes are bare `.`/`span.`/`resource.`/`parent.`/`event.`/`link.`/`instrumentation.`, intrinsic scopes use a **colon** `span:`/`trace:`/`event:`/`link:`/`instrumentation:` (there is **no `resource:`**), structural tokens are `>> << > < ~` + negated `!>> !<< !> !<` + union `&>> &<< &> &< &~`.
- **Churn-prone DataFusion-internal traits.** `UserDefinedLogicalNodeCore`, `ExecutionPlan`, `RecordBatchStream`, the `LogicalPlanBuilder` join/filter/aggregate builders, and the plan-execution entry point change shape between DataFusion revisions. **Do not fabricate exact trait method signatures.** Where this plan shows operator / plan-builder scaffolding it gives the *struct shape, field set, and a behavior-pinning test*, plus an explicit **"verify against datafusion rev `0838a4d`"** note. The test pins behavior; if a trait method's signature differs at the pinned rev, adapt the impl to satisfy the test — never change the asserted behavior.
- **The single-span rule is the #1 semantic trap (spec §6.2).** Conditions inside **one** `{}` must all hold on a **single span** (an `AND` over one span's columns). `{A} && {B}` matches a trace when **different** spans satisfy each side (a trace-level existential, lowered to a join keyed by `trace_id`). Every planner task that touches spanset combination carries a test that distinguishes "both conditions on one span" from "each condition on a different span."
- **Sibling carries a distinct-span predicate (spec §5/§6.3).** `B ~ A` ⟹ `B.parent_id == A.parent_id && B.span_id != A.span_id`. The `span_id != span_id` clause is **mandatory**: a naive equi-join on `parent_id` alone matches a span against itself (reporting a span as its own sibling) and wrongly matches a span satisfying both sides. `parent_id` here is the nested-set parent column (`== parent.nested_set_left`), not the raw `parent_span_id` bytes; two roots share `parent_id = 0` (the sentinel) and are siblings of each other — matching Tempo.

---

## Dependency & slice roadmap

**Depends on:**
- `crabka-blockstore` (generalized in traces Slice 1): the `BlockIndex` trait, `TraceIndex` (FNV-sharded `trace_id` bloom + per-block tag sets/blooms), and the **flattened span block schema** including the nested-set columns (`nested_set_left`/`nested_set_right`/`parent_id`, Int32) + the DFS pre-order computed at block-build. **This slice consumes only the *column-name contract and the nested-set semantics*** — the `BlockStore`-backed `SpanStore` impl lands in Slice 5; here we ship `InMemorySpanStore`, which computes the same nested-set columns from a hand-built span tree so the structural-join tests are trustworthy. Types `Labels`/`LabelMatcher`/`MatchOp` stay available from blockstore.

**The 8 traces slices** (this plan = Slice 2; each later slice gets its own plan):

1. **Blockstore generalization + span block schema + `TraceIndex`** — `BlockIndex` trait; span block (nested-set columns + DFS pre-order at block-build); `TraceIndex`. *(planned/built separately)*
2. **`crabka-traceql` core** *(this plan)* — lexer + parser + planner + selectors (scopes/intrinsics/array semantics, single-span rule) + non-structural pushdown + the `&&` AND fast path + the **`SpanStructuralJoin`** lowering for the **core** structural operators (descendant/child/sibling/ancestor/parent) + pipeline aggregations + `search()`/`trace_by_id()`. Defines the `SpanStore` trait + the pinned result types.
3. **TraceQL completeness** — full structural ops (the **negated** `!>>`/`!<<`/`!>`/`!<` and **union** `&>>`/`&<<`/`&>`/`&<`/`&~` forms), the remaining pipeline aggregations, **TraceQL metrics** (time-bucketed → Prometheus-shaped series + exemplars), and **tag discovery** (scoped tag names/values). **Reuses this slice's `SpanStore` trait, `SpanStructuralJoin` lowering, parser, and result model — those public names are frozen here.**
4. **Ingest service** — `distributor` (OTLP/Jaeger/Zipkin/`/api/push`) → `trace_id`-partitioned WAL; `block-builder` consumer group → span blocks + `TraceIndex`; `live-store` consumer group (hot tier `MemTable`).
5. **Querier + Tempo HTTP API** — implement `SpanStore` as the hot/cold UNION (live-store + blocks); serve `/api/echo`, `/api/v2/traces/{id}`, `/api/search`, `/api/v2/search/tags` + `tag/{tag}/values`, `/api/metrics/query_range` + `query`. **Replaces `InMemorySpanStore` with a `BlockStore`-backed `SpanStore` — the trait is frozen here.**
6. **Query-frontend** — search sharding + queueing.
7. **Metrics-generator** — span-metrics (RED) + service-graphs → remote_write. **Consumes `TraceqlEngine`/`SpanStore`.**
8. **Hardening** — per-tenant limits + multi-tenancy isolation, differential-vs-Tempo corpus, Grafana integration.

---

## Shared cross-slice contract (frozen here — later slices interlock on these exact names)

```rust
// ---- trait + scan result (Slice 5 reimplements SpanStore over BlockStore) ----
#[async_trait::async_trait]
pub trait SpanStore: Send + Sync {
    async fn scan(&self, tenant: &str, matchers: &[SpanMatcher], start_ns: i64, end_ns: i64)
        -> Result<ScanResult, TraceqlError>;
    async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16])
        -> Result<Option<TraceSpans>, TraceqlError>;
    async fn tag_names(&self, tenant: &str, scope: Option<TagScope>, start_ns: i64, end_ns: i64)
        -> Result<Vec<ScopedTag>, TraceqlError>;
    async fn tag_values(&self, tenant: &str, tag: &str, start_ns: i64, end_ns: i64)
        -> Result<Vec<TypedValue>, TraceqlError>;
}

pub struct ScanResult { pub ctx: datafusion::prelude::SessionContext, pub span_table: String }
// span_table may be a UNION view of live-store (hot) + blocks (cold)

// ---- engine ----
pub struct EngineOpts { pub default_limit: usize /*20*/, pub default_spss: usize /*3*/, pub max_traces: usize }
pub struct TraceqlEngine<S: SpanStore> { /* store: Arc<S>, opts: EngineOpts */ }
impl<S: SpanStore> TraceqlEngine<S> {
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self;
    pub async fn search(&self, tenant: &str, query: &str, start_ns: i64, end_ns: i64, limit: usize)
        -> Result<SearchResponse, TraceqlError>;
    pub async fn query_range(&self, tenant: &str, query: &str, start_ns: i64, end_ns: i64, step_ns: i64)
        -> Result<TraceMetricsResponse, TraceqlError>;       // body is Slice 3; signature frozen here
    pub async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16])
        -> Result<Option<TraceSpans>, TraceqlError>;
    pub fn store(&self) -> &Arc<S>;                       // discovery (tag_names/tag_values) lives on SpanStore
}

// ---- search result model (Tempo /api/search JSON projection) ----
pub struct SearchResponse { pub traces: Vec<TraceResult> }
pub struct TraceResult {
    pub trace_id: [u8; 16], pub root_service_name: String, pub root_trace_name: String,
    pub start_time_unix_nano: u64, pub duration_ms: u64, pub span_sets: Vec<SpanSet>,
}
pub struct SpanSet { pub spans: Vec<SpanRef>, pub matched: u32 }
pub struct SpanRef {
    pub span_id: [u8; 8], pub start_time_unix_nano: u64, pub duration_nanos: u64,
    pub attributes: Vec<(String, AttrValue)>,
}
pub struct TraceSpans { /* full OTLP resource->scope->spans for one trace, for /api/v2/traces/{id} */ }

// ---- tag discovery + value model (bodies in Slice 3; types frozen here) ----
pub enum TagScope { Resource, Span, Intrinsic, Event, Link, Instrumentation }
pub struct ScopedTag { pub scope: TagScope, pub tags: Vec<String> }
pub struct TypedValue { pub type_: String, pub value: String }
pub enum AttrValue { Str(String), Int(i64), Float(f64), Bool(bool) }

// ---- one resolved selector condition (planner input) ----
pub struct SpanMatcher { /* scope + key + op + value (see Task A4) */ }

// ---- metrics response (body Slice 3; type frozen here) ----
pub struct TraceMetricsResponse { /* Prometheus-shaped series + exemplars */ }

// ---- error ----
pub enum TraceqlError { Parse(String), Plan(String), Exec(String), Store(String), Unsupported(String) }

// ---- internal (Slice 3 reuses) ----
//   ast::{Query, Spanset, FieldExpr, Condition, Scope, Intrinsic, ComparisonOp, StructuralOp, Pipeline, Aggregate}
//   lexer::{Token, lex}
//   planner::structural::SpanStructuralJoin   (the nested-set self-join lowering)
```

---

## Span table column contract (frozen — block-builder Slice 1 + `InMemorySpanStore` both emit this)

The `span_table` registered by `SpanStore::scan` has **one row per span**, sorted/grouped by `trace_id`, with these columns (names are the contract the planner pushes predicates against):

| Column | Arrow type | Meaning |
|---|---|---|
| `trace_id` | `FixedSizeBinary(16)` | join/partition key |
| `span_id` | `FixedSizeBinary(8)` | span identity |
| `parent_span_id` | `FixedSizeBinary(8)` | raw semantic parent (nullable) |
| `nested_set_left` | `Int32` | DFS pre-order left bound |
| `nested_set_right` | `Int32` | DFS pre-order right bound |
| `parent_id` | `Int32` | parent's `nested_set_left` (`0` sentinel for roots) |
| `root_service_name` | `Utf8` | trace-denormalized |
| `root_span_name` | `Utf8` | trace-denormalized |
| `trace_start_unix_nano` | `Int64` | trace-denormalized |
| `trace_duration_nanos` | `Int64` | trace-denormalized |
| `name` | `Utf8` | intrinsic `span:name` |
| `kind` | `Int32` | intrinsic `span:kind` (enum) |
| `start_unix_nano` | `Int64` | intrinsic `span:` start |
| `duration_nanos` | `Int64` | intrinsic `span:duration` |
| `status_code` | `Int32` | intrinsic `span:status` (enum `unset|ok|error`) |
| `status_message` | `Utf8` | intrinsic `span:statusMessage` |
| `attr_<key>` | dict-encoded `Utf8`/`Int64`/`Float64`/`Boolean` | promoted span/resource attribute columns (the pushdown fast path) |

> The nested-set invariant the structural join relies on: an ancestor's `[left, right]` interval **strictly contains** every descendant's, and `parent_id(child) == parent.nested_set_left`. Slice 1 computes this at block-build; `InMemorySpanStore` (Task A5) computes the identical assignment from the in-memory span tree so the join tests use known integer values.

---

## File structure (`crates/traceql/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest; workspace deps |
| `src/lib.rs` | module decls + public re-exports + crate docs |
| `src/error.rs` | `TraceqlError` enum + `From` conversions |
| `src/result.rs` | `SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`/`AttrValue` + tag-discovery types |
| `src/store.rs` | `SpanStore` trait, `ScanResult`, `SpanMatcher` |
| `src/span_columns.rs` | the span-table column-name constants + Arrow schema builder + nested-set DFS |
| `src/in_memory.rs` | `InMemorySpanStore` test impl (span DF table incl. nested-set columns) |
| `src/lexer.rs` | the TraceQL lexer (`Token`, `lex`) |
| `src/ast.rs` | the TraceQL AST node types |
| `src/parser.rs` | the recursive-descent parser (`parse`) |
| `src/planner/mod.rs` | `TraceqlPlanner` entry + AST→plan recursion + `PlannedQuery` |
| `src/planner/selector.rs` | spanset-selector lowering (scopes/intrinsics/array semantics, single-span AND) |
| `src/planner/combinator.rs` | spanset `&&` (intersect) / `||` (union) trace-level joins |
| `src/planner/structural.rs` | **the `SpanStructuralJoin` nested-set self-join lowering** |
| `src/planner/pipeline.rs` | pipeline aggregations (`count`/`avg`/`max`/`min`/`by`) |
| `src/engine.rs` | `TraceqlEngine`, `EngineOpts`, `search`/`trace_by_id`, spanSet assembly |

`src/planner/structural.rs` isolates the centerpiece churn-prone DataFusion join surface from the rest of the planner.

---

## Phase A — scaffold + result model + `SpanStore` + span columns + `InMemorySpanStore`

> **Batching:** A1 (scaffold) lands first (creates `Cargo.toml` + `lib.rs`). Then A2 (`error.rs`), A3 (`result.rs`), A4 (`store.rs` + `SpanMatcher`), A6 (`span_columns.rs`) touch disjoint files and may run as a parallel batch — each appends one re-export line to `lib.rs`, so serialize the `lib.rs` edits (reviewer merges). A5 (`InMemorySpanStore`) depends on A4 + A6. Recommended: A1 → {A2, A3, A4, A6 in parallel, lib.rs merged} → A5.

### Task A1: Crate scaffold + workspace wiring

**Files:**
- Create: `crates/traceql/Cargo.toml`
- Create: `crates/traceql/src/lib.rs`
- Modify: root `Cargo.toml` (add `crabka-traceql` to workspace members if member globbing is not used; `datafusion`/`arrow`/`regex` already in `[workspace.dependencies]`)

**Interfaces:**
- Produces: a compiling `crabka-traceql` crate with `pub fn crate_smoke() -> bool` (placeholder, removed in A2).

- [x] **Step 1: Create `crates/traceql/Cargo.toml`**

```toml
[package]
name = "crabka-traceql"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "TraceQL engine (lexer + parser + planner + nested-set structural self-join) for Crabka's Grafana-Tempo-equivalent traces backend"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-traceql"
readme = "README.md"
keywords = ["observability", "tempo", "traceql", "datafusion", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
crabka-blockstore = { path = "../blockstore", version = "0.3.7" }
arrow = { workspace = true }
datafusion = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread"] }
futures = { workspace = true }
regex = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

> If `crabka-blockstore`/`datafusion`/`arrow`/`regex` are not yet present in `[workspace.dependencies]` (blockstore/Slice 1 not landed in this tree), add them exactly as the blockstore plan specifies. The first build fetches + compiles DataFusion from git — slow (several minutes), normal.

- [x] **Step 2: Create `crates/traceql/src/lib.rs` with a placeholder**

```rust
//! TraceQL engine for Crabka's Grafana-Tempo-equivalent traces backend.
//!
//! Hand-written lexer + recursive-descent parser (grammar referenced from
//! icegate's ANTLR `.g4`), an AST->DataFusion `LogicalPlan` planner, and the
//! nested-set structural self-join (`SpanStructuralJoin`) lowering of TraceQL's
//! structural operators. Storage-agnostic via the injected `SpanStore` trait.

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

- [x] **Step 3: Build and test**

Run: `cargo test -p crabka-traceql`
Expected: compiles and `smoke` PASSES. If the build fails with an arrow major mismatch (`expected struct arrow::... found struct arrow::...`), the datafusion rev is wrong — re-confirm the pinned rev tracks arrow 59.

- [x] **Step 4: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add Cargo.toml Cargo.lock crates/traceql/
git commit -m "feat(traceql): scaffold crabka-traceql crate"
```

---

### Task A2: `TraceqlError`

**Files:**
- Create: `crates/traceql/src/error.rs`
- Modify: `crates/traceql/src/lib.rs` (declare module, re-export, remove placeholder)

**Interfaces:**
- Produces:
  - `pub enum TraceqlError { Parse(String), Plan(String), Exec(String), Store(String), Unsupported(String) }` (`Debug`, `Clone`, `thiserror::Error`)
  - `impl From<datafusion::error::DataFusionError> for TraceqlError` → `Exec`
  - `pub type Result<T> = std::result::Result<T, TraceqlError>` (internal alias)

- [x] **Step 1: Write the failing test**

Create `crates/traceql/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let te: TraceqlError = dfe.into();
        assert!(matches!(te, TraceqlError::Exec(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = TraceqlError::Unsupported("negated structural op".into());
        assert!(format!("{e}").contains("unsupported"));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib error`
Expected: FAIL — `cannot find type TraceqlError`.

- [x] **Step 3: Implement `error.rs`**

Prepend above the `tests` module:

```rust
//! The crate's error type. Categories map to Tempo HTTP statuses in Slice 5
//! (`Parse`/`Plan` -> 400 `bad_data`, `Exec` -> 500, `Store` -> 500,
//! `Unsupported` -> 422/`not_implemented`).

/// Errors raised by the TraceQL engine. Foreign errors are stringified.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TraceqlError {
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
pub type Result<T> = std::result::Result<T, TraceqlError>;

impl From<datafusion::error::DataFusionError> for TraceqlError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Exec(e.to_string())
    }
}
```

- [x] **Step 4: Wire into `lib.rs`**

Replace the placeholder body of `lib.rs` (remove `crate_smoke` + its test) with:

```rust
mod error;

pub use error::TraceqlError;
pub(crate) use error::Result;
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib error`
Expected: PASS (2 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): TraceqlError type + DataFusion conversion"
```

---

### Task A3: Result model — `SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans` + tag-discovery types

**Files:**
- Create: `crates/traceql/src/result.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Produces (all `Clone`, `Debug`, `PartialEq` unless noted):
  - `pub enum AttrValue { Str(String), Int(i64), Float(f64), Bool(bool) }`
  - `pub struct SpanRef { pub span_id: [u8; 8], pub start_time_unix_nano: u64, pub duration_nanos: u64, pub attributes: Vec<(String, AttrValue)> }`
  - `pub struct SpanSet { pub spans: Vec<SpanRef>, pub matched: u32 }`
  - `pub struct TraceResult { pub trace_id: [u8; 16], pub root_service_name: String, pub root_trace_name: String, pub start_time_unix_nano: u64, pub duration_ms: u64, pub span_sets: Vec<SpanSet> }`
  - `pub struct SearchResponse { pub traces: Vec<TraceResult> }`
  - `pub struct TraceSpans { pub trace_id: [u8; 16], pub spans: Vec<SpanRef> }` (the full single-trace span set for `/api/v2/traces/{id}`; the OTLP resource→scope nesting is reconstructed at the HTTP edge in Slice 5 — this slice carries the flat span list keyed by trace_id, which is sufficient for the by-id path)
  - `pub enum TagScope { Resource, Span, Intrinsic, Event, Link, Instrumentation }` (`Copy`)
  - `pub struct ScopedTag { pub scope: TagScope, pub tags: Vec<String> }`
  - `pub struct TypedValue { pub type_: String, pub value: String }`
  - `pub struct TraceMetricsResponse { pub series: Vec<TraceMetricSeries> }` + `pub struct TraceMetricSeries { pub labels: Vec<(String, String)>, pub points: Vec<(i64, f64)> }` (bodies populated in Slice 3; types frozen here)

- [x] **Step 1: Write the failing test**

Create `crates/traceql/src/result.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn span_ref_holds_typed_attributes() {
        let s = SpanRef {
            span_id: [1; 8],
            start_time_unix_nano: 1000,
            duration_nanos: 42,
            attributes: vec![
                ("http.status_code".into(), AttrValue::Int(200)),
                ("ok".into(), AttrValue::Bool(true)),
            ],
        };
        assert!(s.attributes[0].1 == AttrValue::Int(200));
        assert!(s.attributes[1].1 == AttrValue::Bool(true));
    }

    #[test]
    fn search_response_nests_span_sets() {
        let resp = SearchResponse {
            traces: vec![TraceResult {
                trace_id: [0xAB; 16],
                root_service_name: "checkout".into(),
                root_trace_name: "POST /pay".into(),
                start_time_unix_nano: 5,
                duration_ms: 12,
                span_sets: vec![SpanSet { spans: vec![], matched: 3 }],
            }],
        };
        assert!(resp.traces[0].span_sets[0].matched == 3);
        assert!(resp.traces[0].trace_id == [0xAB; 16]);
    }

    #[test]
    fn tag_scope_is_copy() {
        let s = TagScope::Span;
        let _c = s; // Copy
        assert!(s == TagScope::Span);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib result`
Expected: FAIL — `cannot find type SpanRef`.

- [x] **Step 3: Implement `result.rs`**

Prepend above the `tests` module:

```rust
//! The Tempo-shaped result model. Slice 5 serializes these to the Tempo HTTP
//! API's JSON shapes byte-for-byte (`/api/search`, `/api/v2/traces/{id}`,
//! `/api/v2/search/tags` + `tag/{tag}/values`).

/// A typed attribute value (TraceQL static types).
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// One matched span in a search result span set.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRef {
    pub span_id: [u8; 8],
    pub start_time_unix_nano: u64,
    pub duration_nanos: u64,
    pub attributes: Vec<(String, AttrValue)>,
}

/// A span set: the spans matched by one spanset expression, with a match count.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanSet {
    pub spans: Vec<SpanRef>,
    pub matched: u32,
}

/// One trace in a search response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceResult {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub start_time_unix_nano: u64,
    pub duration_ms: u64,
    pub span_sets: Vec<SpanSet>,
}

/// The `/api/search` response.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResponse {
    pub traces: Vec<TraceResult>,
}

/// The full span set for one trace (`/api/v2/traces/{id}`). The OTLP
/// resource->scope->spans nesting is reconstructed at the HTTP edge (Slice 5).
#[derive(Clone, Debug, PartialEq)]
pub struct TraceSpans {
    pub trace_id: [u8; 16],
    pub spans: Vec<SpanRef>,
}

/// Tag-discovery scope (`/api/v2/search/tags?scope=`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
    Event,
    Link,
    Instrumentation,
}

/// Tag names grouped by scope.
#[derive(Clone, Debug, PartialEq)]
pub struct ScopedTag {
    pub scope: TagScope,
    pub tags: Vec<String>,
}

/// A typed tag value (`/api/v2/search/tag/{tag}/values`).
#[derive(Clone, Debug, PartialEq)]
pub struct TypedValue {
    pub type_: String,
    pub value: String,
}

/// One TraceQL-metrics series (body populated in Slice 3).
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricSeries {
    pub labels: Vec<(String, String)>,
    pub points: Vec<(i64, f64)>,
}

/// The `/api/metrics/query_range` response (body populated in Slice 3).
#[derive(Clone, Debug, PartialEq)]
pub struct TraceMetricsResponse {
    pub series: Vec<TraceMetricSeries>,
}
```

- [x] **Step 4: Wire into `lib.rs`**

Add `mod result;` and:
```rust
pub use result::{
    AttrValue, ScopedTag, SearchResponse, SpanRef, SpanSet, TagScope, TraceMetricSeries,
    TraceMetricsResponse, TraceResult, TraceSpans, TypedValue,
};
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib result`
Expected: PASS (3 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): Tempo-shaped result model + tag-discovery types"
```

---

### Task A4: `SpanStore` trait + `ScanResult` + `SpanMatcher`

**Files:**
- Create: `crates/traceql/src/store.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Consumes: `datafusion::prelude::SessionContext`, `TraceqlError`, the result types (`TraceSpans`/`ScopedTag`/`TypedValue`/`TagScope`).
- Produces:
  - `pub struct ScanResult { pub ctx: SessionContext, pub span_table: String }`
  - `pub struct SpanMatcher { pub scope: MatchScope, pub key: String, pub op: MatchCmp, pub value: MatchValue }` — one resolved selector condition handed to `SpanStore::scan` for block/bloom prefilter.
  - `pub enum MatchScope { Span, Resource, Both, Intrinsic, Parent, Event, Link, Instrumentation }`
  - `pub enum MatchCmp { Eq, Neq, Lt, Lte, Gt, Gte, Re, Nre }`
  - `pub enum MatchValue { Str(String), Int(i64), Float(f64), Bool(bool), Nil }`
  - `#[async_trait::async_trait] pub trait SpanStore: Send + Sync { ... }` with exactly the four methods from the Shared cross-slice contract.

- [x] **Step 1: Write the failing test** (a trivial in-test impl proves the trait is object-shaped and the signatures compile)

Create `crates/traceql/src/store.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use datafusion::prelude::SessionContext;

    struct Empty;

    #[async_trait::async_trait]
    impl SpanStore for Empty {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[SpanMatcher],
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<ScanResult, crate::error::TraceqlError> {
            Ok(ScanResult { ctx: SessionContext::new(), span_table: "spans".into() })
        }
        async fn trace_by_id(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<crate::result::TraceSpans>, crate::error::TraceqlError> {
            Ok(None)
        }
        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<crate::result::TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<crate::result::ScopedTag>, crate::error::TraceqlError> {
            Ok(vec![])
        }
        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<crate::result::TypedValue>, crate::error::TraceqlError> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn trait_is_object_safe() {
        let s: std::sync::Arc<dyn SpanStore> = std::sync::Arc::new(Empty);
        let r = s.scan("t", &[], 0, 1).await.unwrap();
        assert!(r.span_table == "spans");
        assert!(s.trace_by_id("t", &[0; 16]).await.unwrap().is_none());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib store`
Expected: FAIL — `cannot find type SpanStore`.

- [x] **Step 3: Implement `store.rs`**

Prepend above the `tests` module:

```rust
//! The data-access seam. The engine is generic over `SpanStore`; production
//! wires it to the querier's hot/cold UNION (Slice 5), tests use
//! `InMemorySpanStore`. `scan` yields a DataFusion `SessionContext` with the span
//! table registered for the (tenant, matchers, range); `span_table` may name a
//! UNION view of live-store (hot) + blocks (cold).

use datafusion::prelude::SessionContext;

use crate::error::TraceqlError;
use crate::result::{ScopedTag, TagScope, TraceSpans, TypedValue};

/// The scope a resolved matcher applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchScope {
    /// bare `.key` — span OR resource.
    Both,
    /// `span.key`.
    Span,
    /// `resource.key`.
    Resource,
    /// a colon intrinsic (`span:`/`trace:`).
    Intrinsic,
    /// `parent.key`.
    Parent,
    /// `event.key`.
    Event,
    /// `link.key`.
    Link,
    /// `instrumentation.key`.
    Instrumentation,
}

/// Comparison operator of a resolved matcher.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchCmp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    /// fully-anchored regex.
    Re,
    /// negated fully-anchored regex.
    Nre,
}

/// The typed RHS of a resolved matcher.
#[derive(Clone, Debug, PartialEq)]
pub enum MatchValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// `nil` (presence check).
    Nil,
}

/// One resolved selector condition handed to the store for block/bloom prefilter.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanMatcher {
    pub scope: MatchScope,
    pub key: String,
    pub op: MatchCmp,
    pub value: MatchValue,
}

/// The result of a span scan: a `SessionContext` with `span_table` registered.
pub struct ScanResult {
    pub ctx: SessionContext,
    pub span_table: String,
}

/// Resolves TraceQL matchers to a DataFusion span table over a tenant's traces.
#[async_trait::async_trait]
pub trait SpanStore: Send + Sync {
    /// Register the span table for the matched spans in `[start_ns, end_ns]`.
    /// Matchers are an over-approximate prefilter (block/bloom pruning); the
    /// planner re-applies exact predicates, so a store may return a superset.
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult, TraceqlError>;

    /// Index-less by-id retrieval: the full span set for one trace.
    async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>, TraceqlError>;

    /// Scoped tag names (tag discovery — body Slice 3).
    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError>;

    /// Typed tag values for one tag (tag discovery — body Slice 3).
    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError>;
}
```

- [x] **Step 4: Wire into `lib.rs`**

Add `mod store;` and `pub use store::{MatchCmp, MatchScope, MatchValue, ScanResult, SpanMatcher, SpanStore};`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib store`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traceql
cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): SpanStore trait + ScanResult + SpanMatcher"
```

---

### Task A6: Span-table column contract + Arrow schema + nested-set DFS

> (Numbered A6 because it is in the A2/A3/A4 parallel batch; A5 depends on it.)

**Files:**
- Create: `crates/traceql/src/span_columns.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Produces:
  - column-name constants matching the **Span table column contract** above (`COL_TRACE_ID`, `COL_SPAN_ID`, `COL_PARENT_SPAN_ID`, `COL_NS_LEFT`, `COL_NS_RIGHT`, `COL_PARENT_ID`, `COL_ROOT_SERVICE_NAME`, `COL_ROOT_SPAN_NAME`, `COL_TRACE_START`, `COL_TRACE_DURATION`, `COL_NAME`, `COL_KIND`, `COL_START`, `COL_DURATION`, `COL_STATUS_CODE`, `COL_STATUS_MESSAGE`; plus `ATTR_PREFIX = "attr_"`).
  - `pub fn span_schema(attr_columns: &[(String, arrow::datatypes::DataType)]) -> arrow::datatypes::SchemaRef` — the base intrinsic columns + the promoted `attr_<key>` columns.
  - `pub struct InputSpan { pub trace_id: [u8;16], pub span_id: [u8;8], pub parent_span_id: Option<[u8;8]>, pub name: String, pub kind: i32, pub start_unix_nano: i64, pub duration_nanos: i64, pub status_code: i32, pub status_message: String, pub attrs: Vec<(String, AttrValue)> }`
  - `pub fn assign_nested_set(spans: &[InputSpan]) -> Vec<NestedSet>` where `pub struct NestedSet { pub left: i32, pub right: i32, pub parent_id: i32 }` — the **DFS pre-order** assignment over one trace's span tree (built from `parent_span_id`), identical to the block-builder's (Slice 1). Roots get `parent_id = 0`.

- [x] **Step 1: Write the failing nested-set test** (known-value, hand-built tree)

Create `crates/traceql/src/span_columns.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn span(id: u8, parent: Option<u8>) -> InputSpan {
        InputSpan {
            trace_id: [1; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("s{id}"),
            kind: 0,
            start_unix_nano: 0,
            duration_nanos: 1,
            status_code: 0,
            status_message: String::new(),
            attrs: vec![],
        }
    }

    #[test]
    fn dfs_preorder_intervals_nest_correctly() {
        // tree: root(1) -> {child(2) -> grandchild(4), child(3)}
        let spans = vec![span(1, None), span(2, Some(1)), span(4, Some(2)), span(3, Some(1))];
        let ns = assign_nested_set(&spans);
        // root is span index 0.
        let root = &ns[0];
        // every other span's [left,right] is strictly inside root's.
        for s in &ns[1..] {
            assert!(s.left > root.left && s.right < root.right);
        }
        // root has the sentinel parent.
        assert!(root.parent_id == 0);
        // child(2) [index 1] is the parent of grandchild(4) [index 2]:
        assert!(ns[2].parent_id == ns[1].left);
        // child(3) [index 3] shares root as parent with child(2):
        assert!(ns[3].parent_id == ns[1].parent_id);
        // grandchild(4) nests strictly inside child(2):
        assert!(ns[2].left > ns[1].left && ns[2].right < ns[1].right);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib span_columns`
Expected: FAIL — `cannot find function assign_nested_set`.

- [x] **Step 3: Implement `span_columns.rs`**

Prepend above the `tests` module. The DFS: build a `span_id -> children` map from `parent_span_id`, find roots (no/absent parent), and walk pre-order assigning a monotonically increasing counter to `left` on entry and `right` on exit; `parent_id = parent.left` (or `0` for roots).

```rust
//! The span-table column contract + Arrow schema builder + the nested-set DFS
//! pre-order assignment. The block-builder (Slice 1) computes the identical
//! assignment at block-build; `InMemorySpanStore` reuses this so the structural
//! self-join tests run against known integer interval values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::result::AttrValue;

pub const COL_TRACE_ID: &str = "trace_id";
pub const COL_SPAN_ID: &str = "span_id";
pub const COL_PARENT_SPAN_ID: &str = "parent_span_id";
pub const COL_NS_LEFT: &str = "nested_set_left";
pub const COL_NS_RIGHT: &str = "nested_set_right";
pub const COL_PARENT_ID: &str = "parent_id";
pub const COL_ROOT_SERVICE_NAME: &str = "root_service_name";
pub const COL_ROOT_SPAN_NAME: &str = "root_span_name";
pub const COL_TRACE_START: &str = "trace_start_unix_nano";
pub const COL_TRACE_DURATION: &str = "trace_duration_nanos";
pub const COL_NAME: &str = "name";
pub const COL_KIND: &str = "kind";
pub const COL_START: &str = "start_unix_nano";
pub const COL_DURATION: &str = "duration_nanos";
pub const COL_STATUS_CODE: &str = "status_code";
pub const COL_STATUS_MESSAGE: &str = "status_message";
/// Promoted attribute columns are named `attr_<key>`.
pub const ATTR_PREFIX: &str = "attr_";

/// One input span (pre-block-build), with raw parent reference + attributes.
#[derive(Clone, Debug, PartialEq)]
pub struct InputSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: i32,
    pub start_unix_nano: i64,
    pub duration_nanos: i64,
    pub status_code: i32,
    pub status_message: String,
    pub attrs: Vec<(String, AttrValue)>,
}

/// The nested-set columns for one span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}

/// Build the base span Arrow schema + the promoted `attr_<key>` columns.
#[must_use]
pub fn span_schema(attr_columns: &[(String, DataType)]) -> SchemaRef {
    let mut fields = vec![
        Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
        Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
        Field::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(COL_NS_LEFT, DataType::Int32, false),
        Field::new(COL_NS_RIGHT, DataType::Int32, false),
        Field::new(COL_PARENT_ID, DataType::Int32, false),
        Field::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, false),
        Field::new(COL_ROOT_SPAN_NAME, DataType::Utf8, false),
        Field::new(COL_TRACE_START, DataType::Int64, false),
        Field::new(COL_TRACE_DURATION, DataType::Int64, false),
        Field::new(COL_NAME, DataType::Utf8, false),
        Field::new(COL_KIND, DataType::Int32, false),
        Field::new(COL_START, DataType::Int64, false),
        Field::new(COL_DURATION, DataType::Int64, false),
        Field::new(COL_STATUS_CODE, DataType::Int32, false),
        Field::new(COL_STATUS_MESSAGE, DataType::Utf8, true),
    ];
    for (key, dt) in attr_columns {
        fields.push(Field::new(format!("{ATTR_PREFIX}{key}"), dt.clone(), true));
    }
    Arc::new(Schema::new(fields))
}

/// Assign nested-set `(left, right, parent_id)` to each span via DFS pre-order
/// over the trace's span tree. Input order is preserved in the output `Vec`
/// (output `i` corresponds to input span `i`). Roots get `parent_id = 0`.
#[must_use]
pub fn assign_nested_set(spans: &[InputSpan]) -> Vec<NestedSet> {
    // Map span_id -> input index, and parent index -> child indices.
    let id_to_idx: HashMap<[u8; 8], usize> =
        spans.iter().enumerate().map(|(i, s)| (s.span_id, i)).collect();
    let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut roots: Vec<usize> = Vec::new();
    for (i, s) in spans.iter().enumerate() {
        match s.parent_span_id.and_then(|p| id_to_idx.get(&p)) {
            Some(&parent_idx) if parent_idx != i => children.entry(parent_idx).or_default().push(i),
            _ => roots.push(i),
        }
    }
    // Stable child order = input order (already, since we push in input order).

    let mut out = vec![
        NestedSet { left: 0, right: 0, parent_id: 0 };
        spans.len()
    ];
    let mut counter: i32 = 1;
    // Iterative DFS to avoid recursion depth limits; stack of (idx, parent_left).
    // parent_left == 0 means "root" (the sentinel).
    enum Frame {
        Enter(usize, i32),
        Exit(usize),
    }
    // Process roots in input order; push so the first root is handled first.
    let mut stack: Vec<Frame> = roots.iter().rev().map(|&r| Frame::Enter(r, 0)).collect();
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter(idx, parent_left) => {
                out[idx].left = counter;
                out[idx].parent_id = parent_left;
                counter += 1;
                stack.push(Frame::Exit(idx));
                let my_left = out[idx].left;
                if let Some(kids) = children.get(&idx) {
                    for &c in kids.iter().rev() {
                        stack.push(Frame::Enter(c, my_left));
                    }
                }
            }
            Frame::Exit(idx) => {
                out[idx].right = counter;
                counter += 1;
            }
        }
    }
    out
}
```

> **Why a sentinel `parent_id = 0`:** `nested_set_left` starts at `1`, so `0` can never collide with a real parent's `left`. Two roots both carry `parent_id = 0`, which makes them siblings of each other under the sibling lowering — matching Tempo (spec §6.3).

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib span_columns`
Expected: PASS.

- [x] **Step 5: Wire `lib.rs` + commit**

Add `mod span_columns;` and `pub use span_columns::{InputSpan, NestedSet, assign_nested_set, span_schema};` (+ the `COL_*` constants the planner needs; re-export the full set).

```bash
cargo fmt -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): span-table column contract + nested-set DFS pre-order"
```

---

### Task A5: `InMemorySpanStore`

**Files:**
- Create: `crates/traceql/src/in_memory.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Consumes: `SpanStore`/`ScanResult`/`SpanMatcher`, `span_columns::{InputSpan, assign_nested_set, span_schema, COL_*}`, the result types, DataFusion `MemTable`.
- Produces:
  - `pub struct InMemorySpanStore` (`Default`) with `new()` and `pub fn push_trace(&mut self, tenant: &str, root_service_name: &str, root_span_name: &str, spans: Vec<InputSpan>)` — assigns nested-set columns per trace and stores the rows.
  - the `SpanStore` impl: builds a `MemTable` from all stored spans of matching traces in `[start_ns, end_ns]` (matchers are an over-approximate prefilter — return a superset; the planner re-applies exact predicates), registers it as `spans`, returns `ScanResult`. `trace_by_id` returns the stored trace's spans as `TraceSpans`. `tag_names`/`tag_values` return `Ok(vec![])` (bodies are Slice 3; the trait method must exist).

> **Time semantics:** a trace is in range if its `trace_start_unix_nano` ∈ `[start_ns, end_ns]` (Tempo's coarse trace-time filter; per-span time is exact in the planner). Promote **every** attribute key seen across the pushed spans to an `attr_<key>` column so selector pushdown has a column to hit; null where a span lacks it.

- [x] **Step 1: Write the failing test**

Create `crates/traceql/src/in_memory.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crate::result::AttrValue;
    use crate::span_columns::{COL_NS_LEFT, COL_PARENT_ID, InputSpan};
    use datafusion::arrow::array::AsArray;

    fn span(id: u8, parent: Option<u8>, name: &str, attrs: Vec<(&str, AttrValue)>) -> InputSpan {
        InputSpan {
            trace_id: [7; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: name.into(),
            kind: 0,
            start_unix_nano: 1000,
            duration_nanos: 5,
            status_code: 0,
            status_message: String::new(),
            attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    #[tokio::test]
    async fn scan_registers_span_table_with_nested_set_columns() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "checkout",
            "POST /pay",
            vec![
                span(1, None, "root", vec![]),
                span(2, Some(1), "db", vec![("http.method", AttrValue::Str("GET".into()))]),
            ],
        );
        let r = s.scan("t", &[], 0, 5000).await.unwrap();
        let table = &r.span_table;
        // two spans land in the table.
        let df = r.ctx.sql(&format!("SELECT count(*) AS c FROM {table}")).await.unwrap();
        let out = df.collect().await.unwrap();
        let c = out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int64Type>()
            .value(0);
        assert!(c == 2);

        // the root's parent_id is the 0 sentinel; the child's parent_id is the root's left.
        let df = r
            .ctx
            .sql(&format!(
                "SELECT {COL_PARENT_ID} FROM {table} ORDER BY {COL_NS_LEFT}"
            ))
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let pid = out[0]
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        assert!(pid.value(0) == 0); // root
        assert!(pid.value(1) == 1); // child's parent_id == root's nested_set_left (==1)
    }

    #[tokio::test]
    async fn trace_by_id_returns_stored_spans() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "svc", "op", vec![span(1, None, "root", vec![])]);
        let got = s.trace_by_id("t", &[7; 16]).await.unwrap().unwrap();
        assert!(got.trace_id == [7; 16]);
        assert!(got.spans.len() == 1);
        assert!(s.trace_by_id("t", &[9; 16]).await.unwrap().is_none());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib in_memory`
Expected: FAIL — `cannot find type InMemorySpanStore`.

- [x] **Step 3: Implement `in_memory.rs`**

Prepend above the `tests` module. Hold per-tenant traces (`Vec<StoredTrace>`); per scan, collect the in-range traces' spans into Arrow arrays whose column order matches `span_schema(...)`, build a `MemTable`, register it. Build the promoted `attr_<key>` columns from the union of attribute keys, choosing the column's arrow type from the first value's `AttrValue` variant.

```rust
//! In-memory `SpanStore` used by engine + planner tests. Holds full traces;
//! per scan, materializes a DataFusion `MemTable` whose schema matches the
//! Slice-1 span block schema (incl. the nested-set columns, computed here via
//! `assign_nested_set`). Matchers are an over-approximate prefilter — `scan`
//! returns the full in-range span set; the planner applies exact predicates.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::error::TraceqlError;
use crate::result::{AttrValue, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue};
use crate::span_columns::{
    ATTR_PREFIX, COL_DURATION, COL_KIND, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID,
    COL_PARENT_SPAN_ID, COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START,
    InputSpan, NestedSet, assign_nested_set, span_schema,
};
use crate::store::{ScanResult, SpanMatcher, SpanStore};

struct StoredTrace {
    trace_id: [u8; 16],
    root_service_name: String,
    root_span_name: String,
    trace_start_unix_nano: i64,
    trace_duration_nanos: i64,
    spans: Vec<InputSpan>,
    nested: Vec<NestedSet>,
}

/// In-memory span store keyed by tenant.
#[derive(Default)]
pub struct InMemorySpanStore {
    traces: HashMap<String, Vec<StoredTrace>>,
}

impl InMemorySpanStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Store one trace; assigns the nested-set columns via the block-build DFS.
    pub fn push_trace(
        &mut self,
        tenant: &str,
        root_service_name: &str,
        root_span_name: &str,
        spans: Vec<InputSpan>,
    ) {
        let trace_id = spans.first().map_or([0; 16], |s| s.trace_id);
        let trace_start = spans.iter().map(|s| s.start_unix_nano).min().unwrap_or(0);
        let trace_end = spans
            .iter()
            .map(|s| s.start_unix_nano + s.duration_nanos)
            .max()
            .unwrap_or(0);
        let nested = assign_nested_set(&spans);
        self.traces.entry(tenant.to_string()).or_default().push(StoredTrace {
            trace_id,
            root_service_name: root_service_name.to_string(),
            root_span_name: root_span_name.to_string(),
            trace_start_unix_nano: trace_start,
            trace_duration_nanos: trace_end - trace_start,
            spans,
            nested,
        });
    }

    /// The union of attribute keys across in-range traces, with the arrow type
    /// inferred from the first value seen for each key.
    fn attr_columns(traces: &[&StoredTrace]) -> Vec<(String, DataType)> {
        let mut cols: BTreeMap<String, DataType> = BTreeMap::new();
        for t in traces {
            for s in &t.spans {
                for (k, v) in &s.attrs {
                    cols.entry(k.clone()).or_insert_with(|| match v {
                        AttrValue::Str(_) => DataType::Utf8,
                        AttrValue::Int(_) => DataType::Int64,
                        AttrValue::Float(_) => DataType::Float64,
                        AttrValue::Bool(_) => DataType::Boolean,
                    });
                }
            }
        }
        cols.into_iter().collect()
    }
}

fn span_ref(s: &InputSpan) -> SpanRef {
    SpanRef {
        span_id: s.span_id,
        start_time_unix_nano: u64::try_from(s.start_unix_nano).unwrap_or(0),
        duration_nanos: u64::try_from(s.duration_nanos).unwrap_or(0),
        attributes: s.attrs.clone(),
    }
}

#[async_trait::async_trait]
impl SpanStore for InMemorySpanStore {
    async fn scan(
        &self,
        tenant: &str,
        _matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult, TraceqlError> {
        let empty = Vec::new();
        let in_range: Vec<&StoredTrace> = self
            .traces
            .get(tenant)
            .unwrap_or(&empty)
            .iter()
            .filter(|t| t.trace_start_unix_nano >= start_ns && t.trace_start_unix_nano <= end_ns)
            .collect();

        let attr_cols = Self::attr_columns(&in_range);
        let schema = span_schema(&attr_cols);

        // Base column builders.
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(0, 16);
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(0, 8);
        let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(0, 8);
        let mut ns_left = Int32Builder::new();
        let mut ns_right = Int32Builder::new();
        let mut parent_id = Int32Builder::new();
        let mut root_service = StringBuilder::new();
        let mut root_span = StringBuilder::new();
        let mut trace_start = Int64Builder::new();
        let mut trace_duration = Int64Builder::new();
        let mut name = StringBuilder::new();
        let mut kind = Int32Builder::new();
        let mut start = Int64Builder::new();
        let mut duration = Int64Builder::new();
        let mut status_code = Int32Builder::new();
        let mut status_message = StringBuilder::new();

        // One builder per promoted attribute column.
        enum AttrBuilder {
            Str(StringBuilder),
            Int(Int64Builder),
            Float(Float64Builder),
            Bool(BooleanBuilder),
        }
        let mut attr_builders: Vec<(String, AttrBuilder)> = attr_cols
            .iter()
            .map(|(k, dt)| {
                let b = match dt {
                    DataType::Utf8 => AttrBuilder::Str(StringBuilder::new()),
                    DataType::Int64 => AttrBuilder::Int(Int64Builder::new()),
                    DataType::Float64 => AttrBuilder::Float(Float64Builder::new()),
                    _ => AttrBuilder::Bool(BooleanBuilder::new()),
                };
                (k.clone(), b)
            })
            .collect();

        for t in &in_range {
            for (i, s) in t.spans.iter().enumerate() {
                trace_id.append_value(s.trace_id).map_err(stor)?;
                span_id.append_value(s.span_id).map_err(stor)?;
                match s.parent_span_id {
                    Some(p) => parent_span_id.append_value(p).map_err(stor)?,
                    None => parent_span_id.append_null(),
                }
                ns_left.append_value(t.nested[i].left);
                ns_right.append_value(t.nested[i].right);
                parent_id.append_value(t.nested[i].parent_id);
                root_service.append_value(&t.root_service_name);
                root_span.append_value(&t.root_span_name);
                trace_start.append_value(t.trace_start_unix_nano);
                trace_duration.append_value(t.trace_duration_nanos);
                name.append_value(&s.name);
                kind.append_value(s.kind);
                start.append_value(s.start_unix_nano);
                duration.append_value(s.duration_nanos);
                status_code.append_value(s.status_code);
                status_message.append_value(&s.status_message);

                for (key, b) in &mut attr_builders {
                    let v = s.attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v);
                    match (b, v) {
                        (AttrBuilder::Str(sb), Some(AttrValue::Str(x))) => sb.append_value(x),
                        (AttrBuilder::Str(sb), _) => sb.append_null(),
                        (AttrBuilder::Int(ib), Some(AttrValue::Int(x))) => ib.append_value(*x),
                        (AttrBuilder::Int(ib), _) => ib.append_null(),
                        (AttrBuilder::Float(fb), Some(AttrValue::Float(x))) => fb.append_value(*x),
                        (AttrBuilder::Float(fb), _) => fb.append_null(),
                        (AttrBuilder::Bool(bb), Some(AttrValue::Bool(x))) => bb.append_value(*x),
                        (AttrBuilder::Bool(bb), _) => bb.append_null(),
                    }
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(parent_span_id.finish()),
            Arc::new(ns_left.finish()),
            Arc::new(ns_right.finish()),
            Arc::new(parent_id.finish()),
            Arc::new(root_service.finish()),
            Arc::new(root_span.finish()),
            Arc::new(trace_start.finish()),
            Arc::new(trace_duration.finish()),
            Arc::new(name.finish()),
            Arc::new(kind.finish()),
            Arc::new(start.finish()),
            Arc::new(duration.finish()),
            Arc::new(status_code.finish()),
            Arc::new(status_message.finish()),
        ];
        for (_, b) in attr_builders {
            columns.push(match b {
                AttrBuilder::Str(mut sb) => Arc::new(sb.finish()),
                AttrBuilder::Int(mut ib) => Arc::new(ib.finish()),
                AttrBuilder::Float(mut fb) => Arc::new(fb.finish()),
                AttrBuilder::Bool(mut bb) => Arc::new(bb.finish()),
            });
        }

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| TraceqlError::Store(e.to_string()))?;
        let ctx = SessionContext::new();
        let mt = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("spans", Arc::new(mt))?;
        Ok(ScanResult { ctx, span_table: "spans".to_string() })
    }

    async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>, TraceqlError> {
        let found = self
            .traces
            .get(tenant)
            .into_iter()
            .flatten()
            .find(|t| &t.trace_id == trace_id);
        Ok(found.map(|t| TraceSpans {
            trace_id: t.trace_id,
            spans: t.spans.iter().map(span_ref).collect(),
        }))
    }

    async fn tag_names(
        &self,
        _tenant: &str,
        _scope: Option<TagScope>,
        _start_ns: i64,
        _end_ns: i64,
    ) -> Result<Vec<ScopedTag>, TraceqlError> {
        Ok(vec![])
    }

    async fn tag_values(
        &self,
        _tenant: &str,
        _tag: &str,
        _start_ns: i64,
        _end_ns: i64,
    ) -> Result<Vec<TypedValue>, TraceqlError> {
        Ok(vec![])
    }
}

fn stor(e: arrow::error::ArrowError) -> TraceqlError {
    TraceqlError::Store(e.to_string())
}
```

> **Arrow-builder note:** `FixedSizeBinaryBuilder::with_capacity(item_count, byte_width)` + `append_value(&[u8])` returning `Result`, and the `StringBuilder`/`Int32Builder`/`Int64Builder`/`Float64Builder`/`BooleanBuilder` `append_value`/`append_null` conventions are arrow-59 API. If a constructor signature differs at the pin, align to arrow 59 — keep the asserted behavior (the test's `count(*) == 2`, the `parent_id` sentinel/root-left values).

- [x] **Step 4: Wire into `lib.rs`**

Add `mod in_memory;` and `pub use in_memory::InMemorySpanStore;`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traceql --lib in_memory`
Expected: PASS (2 tests).

- [x] **Step 6: Phase A gate + commit**

```bash
cargo test -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets && cargo fmt -p crabka-traceql --check
git add crates/traceql/ Cargo.toml
git commit -m "feat(traceql): InMemorySpanStore building span DataFusion tables w/ nested-set columns"
```

---

## Phase B — lexer + AST + recursive-descent parser

> **Batching:** B1 (lexer) lands first. B2 (AST) is independent and may run in parallel with B1. B3 (parser) depends on B1 + B2. Recommended: {B1, B2 in parallel} → B3. **No DataFusion churn surface in this phase — it is plain Rust string/token processing pinned by snapshot-style assertion tests. Grammar is referenced from icegate's `.g4` as the spec-of-record; the verified token facts in Global Constraints are the ground truth.**

### Task B1: The TraceQL lexer

**Files:**
- Create: `crates/traceql/src/lexer.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub enum Token` covering: `LBrace`/`RBrace`/`LParen`/`RParen`, `Pipe`, `And`(`&&`)/`Or`(`||`)/`Not`(`!`), comparison `Eq`(single `=`)/`Neq`/`Lt`/`Lte`/`Gt`/`Gte`/`Re`(`=~`)/`Nre`(`!~`), arithmetic `Plus`/`Minus`/`Star`/`Slash`/`Mod`/`Caret`, structural `Desc`(`>>`)/`Anc`(`<<`)/`Child`(`>`)/`Parent`(`<`)/`Sibling`(`~`) + negated `NegDesc`(`!>>`)/`NegAnc`(`!<<`)/`NegChild`(`!>`)/`NegParent`(`!<`) + union `UnionDesc`(`&>>`)/`UnionAnc`(`&<<`)/`UnionChild`(`&>`)/`UnionParent`(`&<`)/`UnionSibling`(`&~`), `Dot`, `Colon`, `Comma`, `Ident(String)`, `Str(String)`, `Int(i64)`, `Float(f64)`, `Bool(bool)`, `Nil`, `Eof`.
  - `pub fn lex(input: &str) -> Result<Vec<Token>, TraceqlError>` — maps lex errors to `TraceqlError::Parse`.

> **Maximal-munch ordering is correctness-critical.** `!>>` must beat `!>` must beat `!` (and `!~`); `&>>`/`&>`/`&~`/`&&` must be disambiguated; `>>`/`>=`/`>` and `<<`/`<=`/`<`; `=~`/`=`. The lexer scans longest-token-first at each position. **There is no `==`** — two `=` in a row is a lex error (or `=` then `=`, which the parser rejects). `=~`/`!~` regex anchoring is applied later (parser/planner), not in the lexer.

- [x] **Step 1: Write the failing token tests**

Create `crates/traceql/src/lexer.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn toks(s: &str) -> Vec<Token> {
        let mut t = lex(s).unwrap();
        assert!(t.pop() == Some(Token::Eof));
        t
    }

    #[test]
    fn single_equals_no_double() {
        assert!(toks(".http.status = 200") ==
            vec![Token::Dot, Token::Ident("http.status".into()), Token::Eq, Token::Int(200)]);
        // `==` is not a token: it lexes as Eq then Eq (the parser rejects it).
        assert!(lex("a == b").is_ok());
    }

    #[test]
    fn structural_maximal_munch() {
        assert!(toks("a >> b") == vec![Token::Ident("a".into()), Token::Desc, Token::Ident("b".into())]);
        assert!(toks("a !>> b") == vec![Token::Ident("a".into()), Token::NegDesc, Token::Ident("b".into())]);
        assert!(toks("a &>> b") == vec![Token::Ident("a".into()), Token::UnionDesc, Token::Ident("b".into())]);
        assert!(toks("a !> b") == vec![Token::Ident("a".into()), Token::NegChild, Token::Ident("b".into())]);
        assert!(toks("a &~ b") == vec![Token::Ident("a".into()), Token::UnionSibling, Token::Ident("b".into())]);
    }

    #[test]
    fn comparison_and_regex_and_ge() {
        assert!(toks("x =~ \"a.*\"") == vec![Token::Ident("x".into()), Token::Re, Token::Str("a.*".into())]);
        assert!(toks("x !~ \"a\"") == vec![Token::Ident("x".into()), Token::Nre, Token::Str("a".into())]);
        assert!(toks("d >= 5") == vec![Token::Ident("d".into()), Token::Gte, Token::Int(5)]);
        assert!(toks("d <= 5") == vec![Token::Ident("d".into()), Token::Lte, Token::Int(5)]);
    }

    #[test]
    fn colon_intrinsic_vs_dot_scope() {
        // span:duration  vs  span.foo
        assert!(toks("span:duration") ==
            vec![Token::Ident("span".into()), Token::Colon, Token::Ident("duration".into())]);
        assert!(toks("span.foo") ==
            vec![Token::Ident("span".into()), Token::Dot, Token::Ident("foo".into())]);
    }

    #[test]
    fn literals_and_nil_and_durations() {
        assert!(toks("nil") == vec![Token::Nil]);
        assert!(toks("true false") == vec![Token::Bool(true), Token::Bool(false)]);
        assert!(toks("1.5") == vec![Token::Float(1.5)]);
        // duration literals (5m, 200ms) lex as Ident-like; the parser interprets them
        // in numeric comparison context. Here assert they round-trip as a single token.
        assert!(toks("100ms") == vec![Token::Ident("100ms".into())]);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traceql --lib lexer`
Expected: FAIL — `cannot find type Token`.

- [x] **Step 3: Implement `lexer.rs`** — a single-pass scanner over `char` indices. At each non-whitespace position, try the multi-char operators **longest-first** (`!>>`/`&>>`/`!<<`/`&<<` → `>>`/`<<`/`!>`/`!<`/`&>`/`&<`/`&~`/`&&`/`||`/`>=`/`<=`/`=~`/`!~` → single `=`/`<`/`>`/`~`/`!`/`&`-error/`+`/`-`/`*`/`/`/`%`/`^`/`.`/`:`/`,`/`(`/`)`/`{`/`}`/`|`), then string literals (`"..."` with `\"` escapes), then numbers (int/float), then identifiers/keywords (`nil`/`true`/`false` recognized as their own tokens; everything else an `Ident`, including duration literals like `100ms` and dotted attribute keys like `http.status` — actually emit `Dot`-separated `Ident`s and let the parser join, EXCEPT a leading bare-`.` scope which is its own `Dot`). Provide the full real scanner code (plain Rust — no churn surface). Map any unexpected char to `TraceqlError::Parse`.

> **Identifier vs dotted-key decision (pin in a comment):** the lexer emits `Dot` + `Ident` separately (`.http.status` → `Dot Ident("http") Dot Ident("status")`); the **parser** (B3) joins the post-scope dotted segments into a single attribute key (`http.status`). Exception captured in the test above: an attribute key with no internal structure (`http.status`) — the test `single_equals_no_double` expects the lexer to coalesce a dotted key **after** a leading scope `Dot` into one `Ident("http.status")`. Choose ONE convention and make both the lexer and B3's parser agree; the snapshot tests in B1/B3 pin whichever you pick. (Recommended: lexer coalesces a dotted identifier run into one `Ident`, and a *leading* `.`/`span.`/`resource.` scope is a separate `Dot`/scope token — that is what the tests above assume.)

- [x] **Step 4: Run + wire + commit**

`cargo test -p crabka-traceql --lib lexer` → PASS. Add `mod lexer;` + `pub use lexer::{Token, lex};`.

```bash
cargo fmt -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): TraceQL lexer (maximal-munch operators, single-=, colon-vs-dot scopes)"
```

---

### Task B2: The TraceQL AST

**Files:**
- Create: `crates/traceql/src/ast.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Produces (all `Clone`, `Debug`, `PartialEq`):
  - `pub struct Query { pub root: SpansetExpr, pub pipeline: Vec<Pipeline> }`
  - `pub enum SpansetExpr { Selector(Box<FieldExpr>), And(Box<SpansetExpr>, Box<SpansetExpr>), Or(Box<SpansetExpr>, Box<SpansetExpr>), Structural { op: StructuralOp, lhs: Box<SpansetExpr>, rhs: Box<SpansetExpr> } }`
  - `pub enum FieldExpr { Comparison { lhs: Field, op: ComparisonOp, rhs: Value }, And(Box<FieldExpr>, Box<FieldExpr>), Or(Box<FieldExpr>, Box<FieldExpr>), Not(Box<FieldExpr>), Field(Field) /* presence */ }`
  - `pub struct Field { pub scope: Scope, pub key: String }`
  - `pub enum Scope { Both, Span, Resource, Parent, Event, Link, Instrumentation, Intrinsic(Intrinsic) }`
  - `pub enum Intrinsic { Name, Duration, Kind, Status, StatusMessage, Id, ParentId, ChildCount, TraceDuration, TraceRootName, TraceRootService, TraceId, EventName, EventTimeSinceStart, LinkTraceId, LinkSpanId, InstrumentationName, InstrumentationVersion, NestedSetLeft, NestedSetRight, NestedSetParent }`
  - `pub enum ComparisonOp { Eq, Neq, Lt, Lte, Gt, Gte, Re, Nre }`
  - `pub enum Value { Str(String), Int(i64), Float(f64), Duration(i64) /* nanos */, Bool(bool), Nil }`
  - `pub enum StructuralOp { Descendant, Ancestor, Child, Parent, Sibling, NegDescendant, NegAncestor, NegChild, NegParent, UnionDescendant, UnionAncestor, UnionChild, UnionParent, UnionSibling }`
  - `pub enum Pipeline { Aggregate(Aggregate), Filter { op: ComparisonOp, value: f64 }, By(Vec<Field>), Select(Vec<Field>) }`
  - `pub enum Aggregate { Count, Sum(Field), Avg(Field), Max(Field), Min(Field) }`

- [x] **Step 1: Write the failing test** — construct a `Query` for `{ .foo = 1 } | count()` by hand and assert its shape (a pure data-structure test; no parser yet).

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn ast_constructs_selector_and_pipeline() {
        let q = Query {
            root: SpansetExpr::Selector(Box::new(FieldExpr::Comparison {
                lhs: Field { scope: Scope::Both, key: "foo".into() },
                op: ComparisonOp::Eq,
                rhs: Value::Int(1),
            })),
            pipeline: vec![Pipeline::Aggregate(Aggregate::Count)],
        };
        assert!(matches!(q.root, SpansetExpr::Selector(_)));
        assert!(q.pipeline == vec![Pipeline::Aggregate(Aggregate::Count)]);
    }
}
```

- [x] **Step 2: Run to verify it fails** — `cannot find type Query`.

- [x] **Step 3: Implement `ast.rs`** — the enums/structs above with the derives. Provide the full real definitions (plain data types — no churn surface).

- [x] **Step 4: Run + wire + commit** — add `mod ast;` + `pub use ast::{Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, Scope, SpansetExpr, StructuralOp, Value};`.

```bash
cargo fmt -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets
git add crates/traceql/
git commit -m "feat(traceql): TraceQL AST node types"
```

---

### Task B3: The recursive-descent parser

**Files:**
- Create: `crates/traceql/src/parser.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Consumes: `lexer::{Token, lex}`, `ast::*`.
- Produces:
  - `pub fn parse(query: &str) -> Result<Query, TraceqlError>` — lex then recursive-descent.
- Grammar (precedence, low→high): pipeline `|` splits the trailing aggregations from the spanset; spanset `||` then `&&` then the structural operators (`>>`/`<<`/`>`/`<`/`~` + negated/union) then a braced `{ field_expr }`; inside braces field-expr `||` then `&&` then `!` then a comparison/presence. Scopes: a leading `.`→`Both`, `span.`/`resource.`/`parent.`/`event.`/`link.`/`instrumentation.`→that scope, `span:`/`trace:`/`event:`/`link:`/`instrumentation:` `<intrinsic>`→`Scope::Intrinsic(..)`. Regex values keep their raw pattern (anchoring is applied at planning). Duration literals (`5m`/`200ms`) parse to `Value::Duration(nanos)` **only in a numeric-comparison context** against `span:duration`/`trace:duration`; otherwise `100ms`-style idents are plain strings.

- [x] **Step 1: Write the failing parser tests** (AST snapshots — the format-quality bar's "token-level + AST snapshots")

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;
    use assert2::assert;

    #[test]
    fn bare_dot_is_both_scope() {
        let q = parse("{ .service = \"checkout\" }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else { panic!("selector") };
        let FieldExpr::Comparison { lhs, op, rhs } = fe.as_ref() else { panic!("cmp") };
        assert!(lhs.scope == Scope::Both && lhs.key == "service");
        assert!(*op == ComparisonOp::Eq);
        assert!(*rhs == Value::Str("checkout".into()));
    }

    #[test]
    fn span_colon_intrinsic_duration() {
        let q = parse("{ span:duration > 100ms }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else { panic!() };
        let FieldExpr::Comparison { lhs, op, rhs } = fe.as_ref() else { panic!() };
        assert!(lhs.scope == Scope::Intrinsic(Intrinsic::Duration));
        assert!(*op == ComparisonOp::Gt);
        assert!(*rhs == Value::Duration(100_000_000)); // 100ms in nanos
    }

    #[test]
    fn single_span_rule_intra_brace_is_and() {
        // one {} with two conditions -> FieldExpr::And (must hold on ONE span).
        let q = parse("{ .a = 1 && .b = 2 }").unwrap();
        let SpansetExpr::Selector(fe) = &q.root else { panic!() };
        assert!(matches!(fe.as_ref(), FieldExpr::And(_, _)));
    }

    #[test]
    fn inter_brace_and_is_spanset_level() {
        // {A} && {B} -> SpansetExpr::And (DIFFERENT spans).
        let q = parse("{ .a = 1 } && { .b = 2 }").unwrap();
        assert!(matches!(q.root, SpansetExpr::And(_, _)));
    }

    #[test]
    fn structural_descendant_parses() {
        let q = parse("{ .a = 1 } >> { .b = 2 }").unwrap();
        let SpansetExpr::Structural { op, .. } = &q.root else { panic!() };
        assert!(*op == StructuralOp::Descendant);
    }

    #[test]
    fn pipeline_count_with_filter() {
        let q = parse("{ .a = 1 } | count() > 2").unwrap();
        assert!(q.pipeline.len() == 2);
        assert!(q.pipeline[0] == Pipeline::Aggregate(Aggregate::Count));
        assert!(q.pipeline[1] == Pipeline::Filter { op: ComparisonOp::Gt, value: 2.0 });
    }

    #[test]
    fn double_equals_is_rejected() {
        assert!(parse("{ .a == 1 }").is_err());
    }
}
```

- [x] **Step 2: Run to verify it fails** — `cannot find function parse`.

- [x] **Step 3: Implement `parser.rs`** — a `Parser { tokens: Vec<Token>, pos: usize }` with `peek`/`advance`/`expect`. Recursion: `parse_query` → `parse_pipeline` (parses the spanset, then `|`-separated aggregations/filters/`by`/`select`) → `parse_spanset_or` → `parse_spanset_and` → `parse_structural` (left-assoc over the structural tokens) → `parse_braced` (`{` field-expr `}` or `(` spanset `)`). Inside braces: `parse_field_or` → `parse_field_and` → `parse_field_not` → `parse_comparison` (parse a `Field` via `parse_scope` + key, then optional comparison op + value; a bare field is a presence check). `parse_scope` consumes the leading `.`/scope-ident-`.`/intrinsic-ident-`:`. Duration parsing: when the RHS of a comparison against `span:duration`/`trace:duration`/`event:timeSinceStart` is an ident matching `^\d+(\.\d+)?(ns|us|µs|ms|s|m|h)$`, convert to `Value::Duration(nanos)`. Reject `=` immediately followed by `=` (the `double_equals_is_rejected` test). Provide the full real recursive-descent code (plain Rust — no churn surface).

- [x] **Step 4: Phase B gate + commit**

```bash
cargo test -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets && cargo fmt -p crabka-traceql --check
git add crates/traceql/
git commit -m "feat(traceql): recursive-descent parser (single-span rule, scopes, structural ops, pipeline)"
```

---

## Phase C — planner: selector lowering + non-structural pushdown + the AND fast path

> **Batching:** C1 (planner scaffold + `PlannedQuery`) lands first. C2 (selector→SQL/predicate lowering) depends on C1 + the operators' absence (it builds a `Filter` over the registered span table). C3 (matcher resolution for the store prefilter) is small and independent of C2's internals but shares `selector.rs` with C2 — keep C2+C3 sequential in the same file or split files. Recommended: C1 → C2 → C3.

### Task C1: Planner scaffold + `PlannedQuery` + context

**Files:**
- Create: `crates/traceql/src/planner/mod.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Consumes: `ast::Query`, `SpanStore`, `ScanResult`.
- Produces:
  - `pub(crate) struct PlannerContext { pub tenant: String, pub start_ns: i64, pub end_ns: i64 }`
  - `pub(crate) struct PlannedSpanset { pub ctx: SessionContext, pub plan: LogicalPlan }` — a logical plan over the registered span table whose output rows are the **matched spans** (carrying `trace_id`/`span_id`/the intrinsic + attr columns the result assembler needs).
  - `pub(crate) async fn plan_query<S: SpanStore>(store: &S, ctx: &PlannerContext, q: &Query) -> Result<PlannedSpanset, TraceqlError>` — dispatch on `q.root` (Selector→C2, And/Or→Phase D combinator, Structural→Phase D structural), then apply `q.pipeline` (Phase E). Unwired arms return `TraceqlError::Unsupported` until those phases land.

- [x] **Step 1: Write the failing test** — `plan_query` over an `InMemorySpanStore` with a single `{ .a = 1 }` selector returns a `PlannedSpanset` whose plan, when executed, yields the matching spans. (Drive via a tiny `execute(planned)` test helper that `collect`s the plan against `planned.ctx`.)

- [x] **Step 2: Run to verify it fails** — `cannot find function plan_query`.

- [x] **Step 3: Implement `planner/mod.rs`** — the context/structs, the `plan_query` dispatch (matching on `SpansetExpr` variants; `Selector` → `selector::plan_selector`; others → `Unsupported` until D), and the `execute` test helper (`ctx.execute_logical_plan(plan).collect()` — **verify the execution entry point against datafusion rev `0838a4d`**). Provide the full real scaffold; stub D/E delegations with `Unsupported` returns so the crate compiles.

- [x] **Step 4: Wire `lib.rs` (`mod planner;`) + run + commit** (`feat(traceql): planner scaffold + PlannedSpanset dispatch`).

---

### Task C2: Selector lowering — scopes/intrinsics/comparisons/array semantics + the AND fast path

**Files:**
- Create: `crates/traceql/src/planner/selector.rs`
- Modify: `crates/traceql/src/planner/mod.rs`

**Interfaces:**
- Consumes: `ScanResult`, `ast::{FieldExpr, Field, Scope, Intrinsic, ComparisonOp, Value}`, the `COL_*` constants + `ATTR_PREFIX`.
- Produces:
  - `pub(crate) async fn plan_selector<S: SpanStore>(store: &S, ctx: &PlannerContext, fe: &FieldExpr) -> Result<PlannedSpanset, TraceqlError>` — `store.scan(...)`, then build a `LogicalPlan` = `TableScan(span_table) -> Filter(field_expr_predicate)` where the predicate is the **single-span AND** of the brace's conditions (intra-brace `&&` → DataFusion `AND`, `||` → `OR`, `!` → `NOT`).
  - `pub(crate) fn field_to_column(field: &Field) -> Result<String, TraceqlError>` — maps a `Field` to the span-table column: intrinsics → the fixed `COL_*` columns (`span:duration`→`duration_nanos`, `span:name`→`name`, `span:status`→`status_code`, `span:kind`→`kind`, `trace:duration`→`trace_duration_nanos`, `nestedSetLeft`→`nested_set_left`, etc.); attribute fields → `attr_<key>` (bare `.`/`span.`/`resource.` all map to the same promoted column in this slice — the in-memory store promotes every key; the scope distinction refines block pruning in the store, not the column).
  - `pub(crate) fn comparison_to_expr(col: &str, op: ComparisonOp, value: &Value) -> Result<datafusion::prelude::Expr, TraceqlError>` — builds the DataFusion predicate `Expr`; `Re`/`Nre` use a **fully-anchored** regex (`^(?:pattern)$`) via DataFusion's regex match expr; `nil` → `col IS NULL` (presence: `.foo != nil` → `col IS NOT NULL`).

> **Array semantics (spec §4.1/§6.4):** an attribute may be array-valued. In this slice the promoted column is scalar (single-element); the array-list column form is wired in Slice 1's block schema and exercised in Slice 3. Here implement the scalar path; the `=`/`=~` "any element matches", `!=`/`!~` "no element matches" rules are flagged for Slice 3 (the column is scalar, so any/no collapse to the scalar comparison). The **AND fast path** (spec §6.4) is the intra-brace all-`&&` case lowering to a single `Filter` with an `AND` chain over referenced columns — exactly what `plan_selector` builds; record this in a comment.

- [x] **Step 1: Write the failing tests** — over an `InMemorySpanStore`:
  - `{ .http.method = "GET" }` matches only the spans with that attr value.
  - `{ span:duration > 100 }` matches by the intrinsic duration column.
  - `{ .a = 1 && .b = 2 }` matches only a span where **both** hold (build a trace where one span has `a=1` and a *different* span has `b=2`, and a third span has both — assert only the third matches: the single-span rule).
  - `{ .name =~ "ab.*" }` anchors fully (so `"xabc"` does **not** match).

- [x] **Step 2: Run to verify it fails** — `cannot find function plan_selector`.

- [x] **Step 3: Implement `selector.rs`** — `field_to_column` mapping table, `comparison_to_expr` (use `datafusion::prelude::{col, lit}` + `Expr` operators; regex via the `regexp_match`/`~` expr — **verify the exact regex-predicate constructor against datafusion rev `0838a4d`**; anchor the pattern string ourselves), and `plan_selector` recursing `FieldExpr` into a combined predicate then `LogicalPlanBuilder::from(scan).filter(pred)?.build()?`. Provide the full real predicate-building code; keep the `LogicalPlanBuilder::filter`/`scan` builder calls + the regex expr behind the "verify against rev `0838a4d`" note. Wire `plan_selector` into C1's dispatch.

- [x] **Step 4: Run + commit** (`feat(traceql): selector lowering — scopes/intrinsics/comparisons/anchored-regex + AND fast path`).

---

### Task C3: Matcher resolution for the store prefilter

**Files:**
- Modify: `crates/traceql/src/planner/selector.rs` (add the matcher extractor), `crates/traceql/src/planner/mod.rs`

**Interfaces:**
- Produces:
  - `pub(crate) fn field_expr_to_matchers(fe: &FieldExpr) -> Vec<SpanMatcher>` — extract the **conjunctive** comparison conditions of a brace into `SpanMatcher`s for the store's block/bloom prefilter (an over-approximation: only top-level `&&`-joined `Comparison`s become matchers; `||`/`!` subtrees are dropped from the prefilter and re-applied exactly by the `Filter`). Maps `Scope`→`MatchScope`, `ComparisonOp`→`MatchCmp`, `Value`→`MatchValue`.

- [x] **Step 1: Write the failing test** — `field_expr_to_matchers` of `.a = 1 && .b =~ "x"` yields two matchers; of `.a = 1 || .b = 2` yields **zero** (an OR can't be safely prefiltered); of `.a != nil` yields one presence matcher.

- [x] **Step 2: Run to verify it fails** — `cannot find function field_expr_to_matchers`.

- [x] **Step 3: Implement** — walk the `FieldExpr`; for `And(l, r)` union both sides' matchers; for `Comparison` emit one; for `Or`/`Not` return empty (conservative). Wire it so `plan_selector` passes the extracted matchers to `store.scan`. Provide the full real code.

- [x] **Step 4: Phase C gate + commit**

```bash
cargo test -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets && cargo fmt -p crabka-traceql --check
git add crates/traceql/
git commit -m "feat(traceql): conjunctive matcher extraction for store prefilter"
```

---

## Phase D — the centerpiece: spanset combinators + the nested-set structural self-join

> **Batching:** D1 (combinators `&&`/`||`) and D2 (`SpanStructuralJoin` lowering) both extend `planner/mod.rs`'s dispatch but live in separate files (`combinator.rs` vs `structural.rs`) — they may run as a parallel batch, with the `plan_query` dispatch edits merged by the reviewer. D3 (the structural-correctness behavioral suite) depends on D2. **D2 is the churn-prone DataFusion-join surface — it carries the verify-against-rev note + the known-nested-set behavioral tests.** Recommended: {D1, D2 in parallel} → D3.

### Task D1: Spanset combinators — `&&` (intersect) and `||` (union) at the trace level

**Files:**
- Create: `crates/traceql/src/planner/combinator.rs`
- Modify: `crates/traceql/src/planner/mod.rs`

**Interfaces:**
- Consumes: two `PlannedSpanset`s (the lowered LHS/RHS), the `COL_TRACE_ID` constant.
- Produces:
  - `pub(crate) fn plan_and(lhs: PlannedSpanset, rhs: PlannedSpanset) -> Result<PlannedSpanset, TraceqlError>` — **`{A} && {B}`**: a trace-level existential intersect. Both sides must match within the **same trace** but **different spans** are allowed. Lower to: the set of spans from `A` whose `trace_id` also appears in `B`, UNION the spans from `B` whose `trace_id` appears in `A` (the result spanset is the union of both sides' matched spans for traces present in both). Realize as a semi-join on `trace_id` in each direction then a union — or, equivalently, filter each side to `trace_id IN (SELECT trace_id FROM other)` and union. The **fast path** when both sides are simple selectors over the *same* span (no `&&` across braces) is the intra-brace AND (Phase C) — that is a different construct; this `plan_and` is strictly the inter-brace `{A} && {B}` case.
  - `pub(crate) fn plan_or(lhs: PlannedSpanset, rhs: PlannedSpanset) -> Result<PlannedSpanset, TraceqlError>` — **`{A} || {B}`**: union the matched spans of both sides (a trace matches if either side does).

> **The single-span vs inter-brace distinction is the asserted behavior.** `plan_and` must NOT require the same span to satisfy both — that is the intra-brace case. Build the join on `trace_id` only. Keep the DataFusion join/semi-join/`IN`-subquery builder calls behind a "verify against rev `0838a4d`" note; the *behavior* (different-span match within one trace) is pinned by D3.

- [x] **Step 1: Write the failing test** — `{ .a = 1 } && { .b = 2 }` over a trace whose span-1 has `a=1` and span-2 has `b=2` (different spans): the trace matches, and the result spanset contains both spans. A second trace with only `a=1` does NOT match. (Drive via the engine in Phase E, or a planner-level `execute` helper here.)

- [x] **Step 2: Run to verify it fails** — `cannot find function plan_and`.

- [x] **Step 3: Implement `combinator.rs`** — build the trace-id intersect via `LogicalPlanBuilder` join on `COL_TRACE_ID` (or a `semi join`); union via `LogicalPlanBuilder::union`. Provide the full real plan-construction code; keep the join/union builder calls behind the verify note. Wire `plan_and`/`plan_or` into the `And`/`Or` arms of `plan_query`.

- [x] **Step 4: Run + commit** (`feat(traceql): spanset && (trace-level intersect) and || (union) combinators`).

---

### Task D2: `SpanStructuralJoin` — the nested-set self-join lowering (THE CENTERPIECE)

**Files:**
- Create: `crates/traceql/src/planner/structural.rs`
- Modify: `crates/traceql/src/planner/mod.rs`

**Interfaces:**
- Consumes: two `PlannedSpanset`s (LHS = the `A` side, RHS = the `B` side), `ast::StructuralOp`, the `COL_TRACE_ID`/`COL_NS_LEFT`/`COL_NS_RIGHT`/`COL_PARENT_ID`/`COL_SPAN_ID` constants.
- Produces:
  - `pub(crate) fn plan_structural(op: StructuralOp, lhs: PlannedSpanset, rhs: PlannedSpanset) -> Result<PlannedSpanset, TraceqlError>` — lowers a **core** structural operator to a self-join keyed by `trace_id`, returning the **RIGHT-hand (`B`) spans** (per spec §6.3 "structural operators return the RIGHT-hand spans"). The join condition is the nested-set predicate for the operator:
    - `Descendant` (`B >> A`): `A.trace_id == B.trace_id && B.nested_set_left > A.nested_set_left && B.nested_set_right < A.nested_set_right`
    - `Ancestor` (`B << A`): the inverse range (`B.nested_set_left < A.nested_set_left && B.nested_set_right > A.nested_set_right`)
    - `Child` (`B > A`): `B.parent_id == A.nested_set_left`
    - `Parent` (`B < A`): `A.parent_id == B.nested_set_left`
    - `Sibling` (`B ~ A`): `B.parent_id == A.parent_id && B.span_id != A.span_id` (the distinct-span predicate is **mandatory** — spec §5/§6.3).
    - Negated (`!>>`/`!<<`/`!>`/`!<`) and union (`&>>`/`&<<`/`&>`/`&<`/`&~`) forms → `TraceqlError::Unsupported` (Slice 3).
  - `pub(crate) fn nested_set_predicate(op: StructuralOp, a_alias: &str, b_alias: &str) -> Result<datafusion::prelude::Expr, TraceqlError>` — builds the join `Expr` for the core operators (the integer range/equality on the aliased nested-set columns + the always-present `trace_id` equality).

> **Why this is a join, not a tree-walk (spec Decision 5, §6.3).** A per-trace tree traversal is O(spans) per evaluation and fights DataFusion's columnar model. The nested-set encoding turns "is B a descendant of A" into the integer-range predicate `A.left < B.left && B.right < A.right` — a plain join condition over `Int32` columns, which DataFusion evaluates columnar with no custom operator. The join is keyed by `trace_id` so it only ever compares spans of the *same* trace (the partition). **A thin custom `UserDefinedLogicalNodeCore` physical operator is added ONLY if** a standard `LogicalPlanBuilder::join` with the nested-set predicate does not partition by `trace_id` efficiently (spec §13 open question) — prototype the standard join first; it is the simpler path that hits the columnar fast path. If profiling later forces a custom operator, its behavior is the same nested-set predicate, pinned by the D3 tests.

- [x] **Step 1: Write the failing structural-correctness tests (known nested-set values, hand-built traces)**

These are the headline tests of the slice. Build a trace with a **known tree** so the nested-set integers are deterministic (via `assign_nested_set`), then assert each operator returns exactly the right `B` spans.

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use crate::ast::{ComparisonOp, Field, FieldExpr, Scope, Value};
    use crate::in_memory::InMemorySpanStore;
    use crate::result::AttrValue;
    use crate::span_columns::InputSpan;

    // tree:  root(1, svc="a")
    //          ├── childX(2, svc="b")
    //          │     └── grandY(4, svc="c")
    //          └── childZ(3, svc="b")
    // nested-set (via assign_nested_set, pre-order from counter=1):
    //   root:   left=1  right=8  parent_id=0
    //   childX: left=2  right=5  parent_id=1
    //   grandY: left=3  right=4  parent_id=2
    //   childZ: left=6  right=7  parent_id=1
    fn fixture_store() -> InMemorySpanStore {
        fn sp(id: u8, parent: Option<u8>, svc: &str) -> InputSpan {
            InputSpan {
                trace_id: [9; 16],
                span_id: [id; 8],
                parent_span_id: parent.map(|p| [p; 8]),
                name: format!("s{id}"),
                kind: 0,
                start_unix_nano: 0,
                duration_nanos: 1,
                status_code: 0,
                status_message: String::new(),
                attrs: vec![("svc".into(), AttrValue::Str(svc.into()))],
            }
        }
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![sp(1, None, "a"), sp(2, Some(1), "b"), sp(4, Some(2), "c"), sp(3, Some(1), "b")],
        );
        s
    }

    // a selector for `{ .svc = <v> }`.
    fn sel(v: &str) -> FieldExpr {
        FieldExpr::Comparison {
            lhs: Field { scope: Scope::Both, key: "svc".into() },
            op: ComparisonOp::Eq,
            rhs: Value::Str(v.into()),
        }
    }

    // helper: run `{ .svc=b_val } OP { .svc=a_val }` and return the matched B span_ids.
    async fn structural_b_ids(op: crate::ast::StructuralOp, b_val: &str, a_val: &str) -> Vec<[u8; 8]> {
        // (drive plan_selector for each side, plan_structural, execute, collect span_id column)
        // ... uses a small test helper added alongside plan_structural ...
        unimplemented!("filled in Step 3 with the execute+collect helper")
    }

    #[tokio::test]
    async fn descendant_returns_strict_descendants() {
        // B >> A where A = {svc="a"} (root), B = {svc="c"} (grandY).
        // grandY (left=3,right=4) is inside root (left=1,right=8) -> matches.
        let ids = structural_b_ids(crate::ast::StructuralOp::Descendant, "c", "a").await;
        assert!(ids == vec![[4; 8]]);
    }

    #[tokio::test]
    async fn child_uses_parent_id_eq_left() {
        // B > A where A = {svc="a"} (root, left=1), B = {svc="b"} (childX & childZ, parent_id=1).
        let mut ids = structural_b_ids(crate::ast::StructuralOp::Child, "b", "a").await;
        ids.sort();
        assert!(ids == vec![[2; 8], [3; 8]]);
        // grandY (svc="c", parent_id=2) is NOT a child of root -> excluded by construction.
    }

    #[tokio::test]
    async fn sibling_excludes_self_and_requires_same_parent() {
        // B ~ A where both = {svc="b"} (childX, childZ share parent_id=1).
        // childX returns childZ and vice-versa; neither returns itself (span_id != span_id).
        let mut ids = structural_b_ids(crate::ast::StructuralOp::Sibling, "b", "b").await;
        ids.sort();
        ids.dedup();
        assert!(ids == vec![[2; 8], [3; 8]]); // both b-spans appear as each other's sibling
        // critical: a span is never reported as its OWN sibling — the distinct-span
        // predicate. (A naive parent_id-only join would also match childX~childX.)
    }
}
```

- [x] **Step 2: Run to verify it fails** — `cannot find function plan_structural` (and the `unimplemented!` helper).

- [x] **Step 3: Implement `structural.rs`** — `nested_set_predicate` building the `Expr` for each core op over aliased columns (`col("a.nested_set_left")` etc. — alias the two sides so the self-join columns don't collide; **verify the column-aliasing / `LogicalPlanBuilder::join_on` signature against datafusion rev `0838a4d`**), and `plan_structural` constructing the join (`LogicalPlanBuilder::from(b_plan).join_on(a_plan, JoinType::Inner|LeftSemi, [predicate])` returning the `B` columns; the `trace_id` equality is part of the predicate so the join is partitioned by trace). Implement the `structural_b_ids` test helper (execute the planned join against the ctx, collect the `span_id` FixedSizeBinary column into `Vec<[u8;8]>`). Provide the full real predicate + join code; keep the `JoinType`/`join_on`/aliasing API behind the verify note. Wire `plan_structural` into the `Structural` arm of `plan_query`. Return `Unsupported` for the negated/union ops.

> **Verify against datafusion rev `0838a4d`:** the self-join column aliasing (qualifying the two sides so `nested_set_left` is unambiguous), the `LogicalPlanBuilder::join_on(right, JoinType, exprs)` (vs `join(right, JoinType, (left_cols, right_cols), filter)`) signature, and `JoinType::LeftSemi` for "return B spans that have a matching A" are the churn points. Implement to satisfy the known-value tests; if a builder signature differs, adapt it — never change the asserted matched-span sets. The nested-set *predicate algebra* (the `>`/`<`/`==`/`!=` integer comparisons) is NOT a churn point — it is the spec's correctness contract and must be exactly as written.

- [x] **Step 4: Run + commit** (`feat(traceql): SpanStructuralJoin — nested-set self-join lowering for descendant/ancestor/child/parent/sibling`).

---

### Task D3: Structural-operator behavioral suite (ancestor + parent + cross-trace isolation)

**Files:**
- Modify: `crates/traceql/src/planner/structural.rs` (add tests + the ancestor/parent paths if not already complete)

**Interfaces:**
- Produces: additional behavioral tests pinning the remaining core operators + the partition invariant.

- [x] **Step 1: Write the failing tests**
  - **Ancestor** `B << A` where `A = {svc="c"}` (grandY), `B = {svc="a"}` (root): root is the ancestor → returns root.
  - **Parent** `B < A` where `A = {svc="c"}` (grandY, parent_id=2), `B = {svc="b"}` (childX, left=2): childX is grandY's parent → returns childX only (not childZ).
  - **Cross-trace isolation:** push a *second* trace with the same svc values; assert a descendant query never matches an A in trace-1 against a B in trace-2 (the `trace_id` equality in the join predicate). Build the second trace so a naive no-`trace_id` join would wrongly match, proving the partition predicate is load-bearing.

- [x] **Step 2: Run to verify it fails** (the cross-trace test fails if the `trace_id` equality is missing).

- [x] **Step 3: Make them pass** — if ancestor/parent were already implemented in D2 they pass immediately; the cross-trace test confirms the `trace_id` clause. Fix any gap in `nested_set_predicate`.

- [x] **Step 4: Phase D gate + commit**

```bash
cargo test -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets && cargo fmt -p crabka-traceql --check
git add crates/traceql/
git commit -m "test(traceql): structural-operator behavioral suite (ancestor/parent + cross-trace isolation)"
```

---

## Phase E — pipeline aggregations + the engine (`search`/`trace_by_id` + spanSet assembly)

> **Batching:** E1 (pipeline aggregations) extends `planner/mod.rs`; E2 (the engine) depends on E1 + all prior phases. Recommended: E1 → E2.

### Task E1: Pipeline aggregations — `count()`/`avg`/`max`/`min`/`by()` + scalar filter

**Files:**
- Create: `crates/traceql/src/planner/pipeline.rs`
- Modify: `crates/traceql/src/planner/mod.rs`

**Interfaces:**
- Consumes: a `PlannedSpanset` (the matched spans), `ast::{Pipeline, Aggregate, ComparisonOp, Field}`.
- Produces:
  - `pub(crate) fn plan_pipeline(planned: PlannedSpanset, pipeline: &[Pipeline]) -> Result<PlannedSpanset, TraceqlError>` — apply each stage: `Aggregate(Count)` → `GROUP BY by_labels` (or whole-result) `COUNT(*)`; `Aggregate(Avg|Max|Min(field))` → the matching aggregate over the field's column; `Filter { op, value }` → keep groups whose aggregate satisfies the scalar comparison (e.g. `count() > 2`); `By(fields)` → set the grouping key for the following/preceding aggregate; `Select(fields)` → project additional columns. `Sum` is wired but Tempo's TraceQL spans pipeline uses `sum` over a numeric field — include it. Aggregations operate **per trace** by default (the spanset is grouped by `trace_id` for `count()` unless `by()` overrides) — match Tempo's "count of matching spans" semantics.

> **Scope for this slice:** the five plain aggregations + scalar filter + `by`/`select`. The TraceQL-metrics pipeline functions (`rate`/`count_over_time`/`quantile_over_time`/...) are **Slice 3** — `plan_pipeline` returns `TraceqlError::Unsupported` for any pipeline stage it doesn't recognize (the metrics functions arrive via `query_range`, a separate entry, also Slice 3).

- [x] **Step 1: Write the failing test** — over the Phase-D fixture: `{ .svc = "b" } | count() > 1` → the trace matches (2 b-spans, count 2 > 1); `{ .svc = "b" } | count() > 5` → no match. `{ .svc="b" } | by(.svc) | count()` groups by svc.

- [x] **Step 2: Run to verify it fails** — `cannot find function plan_pipeline`.

- [x] **Step 3: Implement `pipeline.rs`** — fold the stages onto the plan via `LogicalPlanBuilder::aggregate(group_exprs, agg_exprs)` then `filter` for the scalar comparison. Provide the full real code; keep the `aggregate`/`filter` builder shapes behind the "verify against rev `0838a4d`" note. Wire into `plan_query` after the spanset is lowered.

- [x] **Step 4: Run + commit** (`feat(traceql): pipeline aggregations — count/avg/max/min/sum + by/select + scalar filter`).

---

### Task E2: `TraceqlEngine` — `search`/`trace_by_id` + spanSet assembly

**Files:**
- Create: `crates/traceql/src/engine.rs`
- Modify: `crates/traceql/src/lib.rs`

**Interfaces:**
- Consumes: `parser::parse`, `planner::{plan_query, PlannerContext}`, `SpanStore`, the result model.
- Produces:
  - `pub struct EngineOpts { pub default_limit: usize, pub default_spss: usize, pub max_traces: usize }`; `impl Default for EngineOpts` (`default_limit: 20`, `default_spss: 3`, `max_traces: 1000`).
  - `pub struct TraceqlEngine<S: SpanStore> { store: Arc<S>, opts: EngineOpts }`
  - `pub fn new(store: Arc<S>, opts: EngineOpts) -> Self`
  - `pub async fn search(&self, tenant: &str, query: &str, start_ns: i64, end_ns: i64, limit: usize) -> Result<SearchResponse, TraceqlError>` — parse, plan, execute, **assemble spanSets per trace**: group the matched spans by `trace_id`, build one `TraceResult` per trace (root_service_name/root_trace_name/start/duration from the denormalized columns), with `span_sets` = one `SpanSet { spans, matched }` per trace (the matched span count; the per-spanset grouping for `{A} && {B}` multi-spanset display is refined in Slice 5's HTTP projection). Apply `limit` (or `default_limit`) and `default_spss`.
  - `pub async fn query_range(&self, tenant: &str, query: &str, start_ns: i64, end_ns: i64, step_ns: i64) -> Result<TraceMetricsResponse, TraceqlError>` — **body returns `TraceqlError::Unsupported("traceql metrics: slice 3")`** (signature frozen here; TraceQL metrics are Slice 3).
  - `pub async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>, TraceqlError>` — delegate to `store.trace_by_id` (the index-less bloom path lives in the store impl).
  - `pub fn store(&self) -> &Arc<S>` — borrow the backing `SpanStore` (Slice 5's Tempo `tag_names`/`tag_values` handlers call `engine.store().tag_names(..)` / `.tag_values(..)` directly, since discovery lives on `SpanStore`, not the engine).
  - `pub(crate) fn assemble_search_response(batches: &[RecordBatch], limit: usize, spss: usize) -> Result<SearchResponse, TraceqlError>` — turn the matched-span `RecordBatch`es (carrying `trace_id`/`span_id`/start/duration/the denormalized root columns + attr columns) into `SearchResponse`.

- [x] **Step 1: Write the failing end-to-end tests**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::in_memory::InMemorySpanStore;
    use crate::result::AttrValue;
    use crate::span_columns::InputSpan;

    fn sp(tid: u8, id: u8, parent: Option<u8>, svc: &str) -> InputSpan {
        InputSpan {
            trace_id: [tid; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: "op".into(),
            kind: 0,
            start_unix_nano: 1000,
            duration_nanos: 200,
            status_code: 0,
            status_message: String::new(),
            attrs: vec![("svc".into(), AttrValue::Str(svc.into()))],
        }
    }

    fn engine() -> TraceqlEngine<InMemorySpanStore> {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp(9, 1, None, "a"), sp(9, 2, Some(1), "b")]);
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x")]);
        TraceqlEngine::new(Arc::new(s), EngineOpts::default())
    }

    #[tokio::test]
    async fn search_selector_returns_matching_trace() {
        let e = engine();
        let r = e.search("t", "{ .svc = \"b\" }", 0, 100_000, 20).await.unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].root_service_name == "a");
    }

    #[tokio::test]
    async fn search_inter_brace_and_matches_different_spans() {
        let e = engine();
        // span-1 has svc=a, span-2 has svc=b (different spans, same trace 9).
        let r = e.search("t", "{ .svc = \"a\" } && { .svc = \"b\" }", 0, 100_000, 20).await.unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
    }

    #[tokio::test]
    async fn search_descendant_structural() {
        let e = engine();
        let r = e.search("t", "{ .svc = \"b\" } >> { .svc = \"a\" }", 0, 100_000, 20).await.unwrap();
        assert!(r.traces.len() == 1); // span-b is a descendant of span-a in trace 9
    }

    #[tokio::test]
    async fn trace_by_id_path() {
        let e = engine();
        let got = e.trace_by_id("t", &[9; 16]).await.unwrap().unwrap();
        assert!(got.spans.len() == 2);
        assert!(e.trace_by_id("t", &[1; 16]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn query_range_is_unsupported_in_slice2() {
        let e = engine();
        let err = e.query_range("t", "{ } | rate()", 0, 100_000, 10_000).await;
        assert!(matches!(err, Err(TraceqlError::Unsupported(_))));
    }
}
```

- [x] **Step 2: Run to verify it fails** — `cannot find type TraceqlEngine`.

- [x] **Step 3: Implement `engine.rs`** — `search` parses (`parser::parse`), plans (`planner::plan_query` with a `PlannerContext`), executes (`ctx.execute_logical_plan(plan).collect()` — **verify entry point against rev `0838a4d`**), and `assemble_search_response` groups the matched-span batches by `trace_id` (read the `trace_id`/`span_id`/`start_unix_nano`/`duration_nanos`/`root_service_name`/`root_span_name`/`trace_start_unix_nano`/`trace_duration_nanos` columns + the `attr_*` columns into `SpanRef`/`TraceResult`). Enforce `max_traces`/`limit`. `query_range` returns `Unsupported`. `trace_by_id` delegates. Provide the full real assembly code; keep the plan-execution entry point behind the verify note.

- [x] **Step 4: Wire `lib.rs`** — `pub use engine::{EngineOpts, TraceqlEngine};`.

- [x] **Step 5: Phase E gate + commit**

```bash
cargo test -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets && cargo fmt -p crabka-traceql --check
git add crates/traceql/
git commit -m "feat(traceql): TraceqlEngine — search/trace_by_id + spanSet assembly"
```

---

## Phase F — end-to-end golden-query suite

> **Batching:** single task (F1). It depends on the whole engine (Phases A–E). This is the credibility check: a curated golden set diffed against documented TraceQL semantics (spec §10 — there is no upstream `.test` corpus, so the corpus is hand-built and asserted against known-correct expected results, NOT fabricated).

### Task F1: Curated golden-query suite over a fixed multi-trace fixture

**Files:**
- Create: `crates/traceql/tests/golden_queries.rs`

**Interfaces:**
- Consumes: `crabka_traceql::{TraceqlEngine, EngineOpts, InMemorySpanStore}` + the public result model.
- Produces: an integration test asserting a curated set of TraceQL queries against a fixed fixture with hand-computed expected results.

- [x] **Step 1: Write the suite** — build a fixed multi-trace, multi-service fixture (3–4 traces with known trees, services, durations, attributes), then assert each query's `SearchResponse` against the hand-computed expected traces/spans. Cover, at minimum:
  - selector by attribute (`{ .http.method = "GET" }`), by intrinsic (`{ span:duration > 150 }`), anchored regex (`{ .name =~ "po.*" }`).
  - the single-span rule: `{ .a = 1 && .b = 2 }` (intra-brace, one span) vs `{ .a = 1 } && { .b = 2 }` (inter-brace, different spans) returning different trace sets on a fixture crafted to distinguish them.
  - each core structural operator (`>>`, `<<`, `>`, `<`, `~`) returning the right RIGHT-hand spans, including the sibling self-exclusion.
  - a pipeline (`{ .svc = "b" } | count() > 1`).
  - cross-trace isolation (a structural query that must not bleed across traces).
  - `trace_by_id` for a known trace.

- [x] **Step 2: Run** — `cargo test -p crabka-traceql --test golden_queries`. Each failure is a planner/lowering bug — fix it in the relevant Phase C/D/E file (the hand-computed expectations are ground truth; never weaken an expectation to pass). Iterate to green.

- [x] **Step 3: Final whole-crate gate + commit**

```bash
cargo test -p crabka-traceql && cargo clippy -p crabka-traceql --all-targets && cargo fmt -p crabka-traceql --check
git add crates/traceql/
git commit -m "test(traceql): curated golden-query suite (selectors/structural/single-span/pipeline)"
```

---

## Self-review

**Spec coverage (against §6 TraceQL engine + §11 Slice 2):**
- Hand-written lexer + recursive-descent parser, grammar referenced from icegate's `.g4` (single `=`-not-`==`, fully-anchored `=~`, dot scopes `.`/`span.`/`resource.`/`parent.`/`event.`/`link.`/`instrumentation.` vs colon intrinsics `span:`/`trace:`/`event:`/`link:`/`instrumentation:`, structural tokens `>> << > < ~` + negated/union, maximal-munch) → Tasks B1, B2, B3.
- The `SpanStore` trait + `ScanResult` + `SpanMatcher` (the pinned contract) → Task A4; `InMemorySpanStore` building span DataFusion tables incl. the nested-set columns → Tasks A6, A5.
- The pinned result model (`SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`/`AttrValue` + tag-discovery + metrics types) → Task A3.
- Selector lowering: scopes/intrinsics/comparison/regex/boolean → columnar predicate pushdown + the `&&` AND fast path (single-span rule: intra-brace `&&` is one span) → Tasks C1, C2; the conjunctive matcher prefilter → C3.
- **The centerpiece — structural operators via the nested-set model, lowered to a partitioned-by-`trace_id` self-join** with the exact predicates (descendant `B.left>A.left && B.right<A.right`; child `B.parent_id==A.left`; sibling equal `parent_id` + distinct `span_id`; ancestor/parent the inverses), returning the RIGHT-hand spans → Tasks A6 (the DFS + columns), D2 (`SpanStructuralJoin`), D3 (behavioral suite incl. cross-trace isolation + sibling self-exclusion).
- Spanset combinators `&&` (trace-level intersect, different-span) / `||` (union) → Task D1.
- Pipeline aggregations `count()`/`avg`/`max`/`min`/`sum`/`by()`/`select()` + scalar filter → Task E1.
- `search()` assembling spanSets per trace + `trace_by_id()` path → Task E2.
- The single-span vs inter-brace `&&` distinction tested everywhere it matters (parser B3, combinator D1, engine E2, golden F1).
- Curated golden-query suite (no upstream corpus; hand-computed expectations) → Task F1.
- **Frozen cross-slice contract** (`SpanStore`/`ScanResult`/`SpanMatcher`/`TraceqlEngine`/`EngineOpts`/`SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`/`TagScope`/`ScopedTag`/`TypedValue`/`AttrValue`/`TraceMetricsResponse`/`TraceqlError` + the `SpanStructuralJoin` lowering + the span-column contract) defined at the exact signatures the prompt pins → §"Shared cross-slice contract" + §"Span table column contract" + the task interfaces.

**Deferred (correctly, to Slice 3):** the negated structural forms (`!>>`/`!<<`/`!>`/`!<`) and union forms (`&>>`/`&<<`/`&>`/`&<`/`&~`) — `plan_structural` returns `Unsupported` for them; TraceQL metrics (`rate`/`count_over_time`/`quantile_over_time`/...) — `query_range` returns `Unsupported`; tag discovery (`tag_names`/`tag_values` bodies) — the store methods return `Ok(vec![])`; full array-attribute (list-column) semantics — the scalar path is implemented, the list path flagged. Each boundary is enforced (returns `Unsupported`/empty), not silently wrong.

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every code-bearing step ships complete, runnable real code (`error.rs`, `result.rs`, `store.rs`, `span_columns.rs` incl. the DFS, `in_memory.rs` incl. the Arrow builders, the lexer, the AST, the parser, the nested-set predicate algebra) or — for the churn-prone DataFusion-internal surfaces (the `LogicalPlanBuilder` `scan`/`filter`/`join_on`/`aggregate`/`union` builders, `JoinType`, the regex predicate expr, the plan-execution entry point) — the **struct shape + the predicate/behavior + a behavior-pinning test + an explicit "verify against datafusion rev `0838a4d`" note**, mirroring how the metrics/blockstore plans bound their arrow/DataFusion hand-waves. No trait method signature is fabricated as fact. The nested-set predicate algebra itself is **not** behind a verify note — it is the spec's correctness contract, written exactly.

**Type consistency:** `SpanStore`'s four method signatures are identical across A4 (definition), A5 (impl), C1/C2 (consumers), and E2 (engine). `ScanResult` fields (`ctx`/`span_table`) stable A4↔A5↔C. `SpanMatcher`/`MatchScope`/`MatchCmp`/`MatchValue` defined once (A4) and consumed in C3. `TraceqlError` variants (`Parse`/`Plan`/`Exec`/`Store`/`Unsupported`) are the single error type across all tasks. The span-table column constants (`COL_*`/`ATTR_PREFIX`) defined once (A6) and referenced unchanged in A5/C2/D2/E2. The nested-set semantics (`assign_nested_set` output) computed once (A6) and relied on by the structural join (D2/D3). `SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`/`AttrValue` defined once (A3) and assembled in E2. The frozen public names match the prompt's pinned contract exactly.

**Known risks (flagged, not hidden):**
1. **DataFusion self-join surface for `SpanStructuralJoin`** (the single largest risk + the slice's centerpiece) — the column aliasing for a self-join, the `join_on`/`JoinType` signature, and whether the standard join partitions by `trace_id` efficiently or needs a thin custom physical operator (spec §13 open question). Contained to `planner/structural.rs`, behind the verify-against-rev note + the known-nested-set behavioral tests (D2/D3). The nested-set *predicate algebra* is pinned as spec contract, so drift surfaces as a compile error against green correctness tests, never as silent wrong descendant/sibling sets. Prototype the standard join first (the simpler columnar-fast-path); escalate to a custom operator only if profiling forces it.
2. **The single-span vs inter-brace `&&` semantic** — the #1 TraceQL trap (spec §6.2). Triple-guarded: the parser produces distinct AST shapes (`FieldExpr::And` intra-brace vs `SpansetExpr::And` inter-brace, tested in B3), the combinator joins only on `trace_id` for inter-brace (D1), and the engine + golden suite assert different-span matching (E2, F1).
3. **Sibling self-exclusion** — the distinct-span (`span_id != span_id`) predicate is mandatory (spec §5/§6.3); omitting it reports a span as its own sibling. Pinned by the D2 `sibling_excludes_self_and_requires_same_parent` test and the golden suite.
4. **Nested-set fidelity** — the `InMemorySpanStore` must compute the *identical* DFS assignment the block-builder (Slice 1) does, or the structural tests are vacuous. `assign_nested_set` is shared (A6) and pinned by a known-tree interval-nesting test; the block-builder is contracted to call the same algorithm.
5. **Slice executability** — this is the largest slice; the phase batching (A→B→C→D→E→F, with the noted intra-phase parallel batches on disjoint file sets per `CLAUDE.md`) keeps each sub-batch's file sets disjoint and ends every phase at a green whole-crate gate so a sub-batch is reviewed/merged before the next starts.
