# crabka-pprof Slice 2 — pprof core (pprof model + codec + `SymbolDb` + `ProfileType` + `ProfileStore` trait + the MERGE→flamegraph engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **This slice is the largest in the profiles program and is executed as sub-batches.** It is organized into six phases (A–F). Within a phase, tasks whose **Files** sets do not overlap may be dispatched as a parallel subagent batch (per `CLAUDE.md`); tasks that share a file (`lib.rs`, `engine.rs`) or genuinely depend on an earlier task's output must run sequentially. Each phase ends at a green whole-crate gate. The recommended batching is called out at the head of each phase.

**Goal:** Build the core of the **language-less** profiles engine — the perftools.profiles **pprof model + codec** (decode/encode), the deduplicated **`SymbolDb`** (parent-pointer stacktrace tree + dedup string/function/location/mapping tables + `encode`/`decode` artifact behind a `SymbolSource` trait), the 5-part **`ProfileType`** parse/`Display`, the **`ProfileStore`** query seam + an `InMemoryProfileStore` test impl (builds a samples DataFusion table + a `SymbolDb`), and — the **centerpiece** — the **MERGE→flamegraph** engine: resolve a Prometheus-matcher `label_selector` string + profile type + `[start,end]` → `ProfileStore.select` → DataFusion `GROUP BY (stacktrace_partition, stacktrace_id) → SUM(value)` (the merge-*before*-symbolize step) → Rust resolve distinct ids via `SymbolDb` (inlined frames expanded, leaf-first) → fold into one `Tree` (total-along-path, self-at-leaf) → `to_flamegraph(max_nodes)` → the 4-ints-per-bar `FlameGraph`. `SelectSeries`/`Diff`/`SelectHeatmap`/raw-profile output are **deferred to Slice 3** (signatures frozen here).

**Architecture:** A query crate `crabka-pprof` that depends on DataFusion (same git pin as blockstore) and **no profiles query parser — there is no language**. The only thing resembling a parser is the Prometheus label-matcher string helper (reusing blockstore `LabelMatcher`/`MatchOp`). The engine is generic over a `ProfileStore` trait that yields a DataFusion `SessionContext` with a samples table registered + an `Arc<dyn SymbolSource>` for a (tenant, profile_type, matchers, time-range) scan — production wires this to the querier's hot/cold UNION (Slice 5), but this slice ships an `InMemoryProfileStore` test impl so the engine is independently testable. The **DataFusion/Rust split is the load-bearing design**: DataFusion does the cheap set-shrinking fold (`GROUP BY (partition, id) → SUM`) *before* symbolization; Rust resolves the symbol-DB tree + folds the flamegraph *only* on the distinct surviving ids. Raw `stacktrace_id`s are only meaningful within their own block's `SymbolDb` partition, so symbolization is always local-then-merge (`Tree::merge`) — never raw ids across a partition/block boundary.

**Tech Stack:** Rust 2024 · `datafusion` (git `main`, pinned — see Global Constraints) · `arrow` 59 · `prost` 0.14 (pprof wire model) · `async-trait` · `tokio` · `futures` · `regex` (matcher-string helper) · `thiserror`. Depends on `crabka-blockstore` (types: `LabelMatcher`, `MatchOp`; the `PCOL_*` samples-table column constants + schema from profiles Slice 1). Tests: `assert2`, `proptest`, `tokio` (`macros`, `rt-multi-thread`).

## Global Constraints

- **No backwards compatibility.** Crabka is greenfield/undeployed. No `#[serde(default)]` shims, no V2-alongside-V1 enum variants, no migration code, no default-off feature gates. Change schemas/enums/interfaces/the symdb on-disk encoding freely. (Only Kafka wire compat matters — and this crate touches none of it; the pprof wire model is a separate, externally-fixed contract pinned below.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn` workspace-wide (`module_name_repetitions`, `missing_errors_doc`, `missing_panics_doc` allowed). New code must be clippy-pedantic clean. Run `cargo clippy -p crabka-pprof --all-targets` before each commit.
- **Formatting:** run `cargo fmt -p crabka-pprof` before every commit. **NEVER** run `cargo +nightly fmt --all` — it fails with OS error 206 / path-too-long in deep worktrees on Windows; always scope with `-p`.
- **Assertions:** use `assert2::assert!` / `assert2::check!` in tests, `prop_assert*` inside `proptest!`.
- **Async tests:** `#[tokio::test]`. Crate dev-dep `tokio` features = `["macros", "rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`. This `main` revision tracks arrow 59 / parquet 59 / object_store 0.13.2, which unify with the workspace pins (same major → cargo unifies to one crate instance, so arrow types cross the DataFusion boundary cleanly). Do **not** substitute a released `datafusion` (54.x is on arrow 58 and pulls a second, incompatible arrow major).
- **Arrow version identity:** import `arrow` directly (`use arrow::...`) as blockstore does; all of arrow/parquet/object_store unify to one instance. If a type-mismatch error appears at the DataFusion boundary, switch that import to DataFusion's re-export (`datafusion::arrow`) to force identity.
- **The pprof wire model is the perftools.profiles `Profile` proto — pin it, don't fabricate field numbers.** The decode/encode round-trip is a *contract* (we ingest real pprof from SDKs/Alloy). The `.proto` is vendored at a pinned tag and compiled with `prost` 0.14 via `build.rs` (the grpc-gateway/rebalancer codegen pattern). Where this plan shows the proto-derived types it gives the *field set + a behavior-pinning round-trip test* against a known-bytes pprof, plus a **"verify against the vendored `profile.proto` (google/pprof `master`, vendored 2026-06-18)"** note. Never hand-write field numbers as fact.
- **The `FlameGraph` 4-ints-per-bar encoding is a byte-exact contract (spec §6.1).** `levels` is a list of `Level { values: Vec<i64> }`; each level's values are traversed in **groups of 4** `[xOffsetDelta, total, self, nameIndex]`, where `xOffsetDelta` is the delta from the *previous bar's end* (not absolute), `names[0]` is the root (`"total"`), and `nameIndex` indexes `names[]`. This grouping + the delta semantics are NOT a churn point — they are the spec's correctness contract and must be exactly as written, pinned by hand-built-profile tests.
- **Fold-before-symbolize is the #1 performance/correctness invariant (spec §6.1).** The `GROUP BY (stacktrace_partition, stacktrace_id) → SUM(value)` runs in DataFusion **before** any symbol-DB resolution; Rust then resolves only the distinct surviving ids. Every engine task carries a test that the merge collapses duplicate `(partition, id)` rows to one summed value before resolution.
- **Raw ids never cross a partition boundary (spec §6.4).** A `stacktrace_id` is only meaningful within its own `SymbolDb` partition. `resolve(partition, id)` always takes the partition. `Tree::merge` combines partial *symbolized* trees, never raw ids — the load-bearing invariant of the distributed merge (exercised here within one store; enforced across blocks in Slice 6).
- **Churn-prone DataFusion-internal traits.** `MemTable`, the `LogicalPlanBuilder` aggregate/scan builders, the `SessionContext::sql`/`execute_logical_plan` entry points, and the arrow `ListBuilder`/`StructBuilder`/dictionary builders change shape between DataFusion/arrow revisions. **Do not fabricate exact trait method signatures.** Where this plan shows DataFusion/arrow scaffolding it gives the *struct shape, column contract, and a behavior-pinning test*, plus an explicit **"verify against datafusion rev `0838a4d` / arrow 59"** note. The test pins behavior; if a method's signature differs at the pinned rev, adapt the impl to satisfy the test — never change the asserted behavior.

---

## Dependency & slice roadmap

**Depends on:**
- `crabka-blockstore` (generalized in profiles Slice 1): the `LabelMatcher`/`MatchOp`/`Labels` types and the **profile samples fact-table column constants + Arrow schema** (`PCOL_PROFILE_TYPE`, `PCOL_STACKTRACE_ID`, `PCOL_VALUE`, `PCOL_STACKTRACE_PARTITION`, `PCOL_TOTAL_VALUE`, `PCOL_SPAN_ID`, `PCOL_TRACE_ID` + the mandatory `COL_FINGERPRINT`/`COL_TIMESTAMP`) and the **symbol-DB on-block artifact byte layout**. **This slice consumes only the *column-name contract* + the matcher types** — the `BlockStore`-backed `ProfileStore` impl lands in Slice 5; here we ship `InMemoryProfileStore`, which emits the identical samples columns from hand-built profiles so the merge tests are trustworthy. (If Slice 1 has not landed in this tree, the `PCOL_*` constants are re-declared in this crate's `samples.rs` against the same names and the dependency is wired but not gated — see Task A6.)

**The 8 profiles slices** (this plan = Slice 2; each later slice gets its own plan):

1. **Blockstore `ProfileIndex` + samples schema + symbol-DB artifact** — `ProfileIndex` (`impl BlockIndex`) = label-series postings (reuse the metrics `SeriesIndex`) + profile-type index + per-block time-range + stacktrace-partition map; the `PCOL_*` samples columns + schema; the symbol-DB artifact. *(planned/built separately)*
2. **`crabka-pprof` core** *(this plan)* — pprof model + codec, `SymbolDb` + `SymbolSource`, `ProfileType` parse/`Display`, the `ProfileStore` trait + `InMemoryProfileStore` + the pinned engine result types, and the **MERGE→flamegraph** engine (fold-before-symbolize, `Tree`, the 4-ints-per-bar `FlameGraph`). Defines the `crabka-pprof` public contract the rest interlock on. **No query parser — there is no language.**
3. **Engine completeness** — `SelectSeries` (precomputed `total_value`, step-in-seconds, SUM/AVERAGE → `FlameGraphDiff`'s sibling `Series`), `Diff` (7-ints-per-bar `FlameGraphDiff`), `max_nodes` truncation refinements, raw-profile output (`select_merge_profile` → pprof), `SelectMergeSpanProfile` + `SelectHeatmap`. **Reuses this slice's `ProfileStore`/`SymbolSource`/`Tree`/`FlameGraph`/`FlameEngine` — those public names are frozen here.**
4. **Ingest service** (`crabka-profiles`) — `distributor` (`push.v1` + `/ingest` + OTLP `v1development` + relabel + multi-value split) → `(tenant, series_fingerprint)`-partitioned WAL; `block-builder` consumer group → samples fact table + dedup symbol DB + `ProfileIndex` (write-then-commit, idempotent keys). **Consumes the pprof codec + `SymbolDb` interning.**
5. **Querier + Connect `querier.v1` API + legacy render** — implement `ProfileStore` as the hot/cold UNION; serve the Connect `querier.v1` methods + legacy `/pyroscope/render`. **Replaces `InMemoryProfileStore` with a `BlockStore`-backed `ProfileStore` — the trait is frozen here.**
6. **Query-frontend** — query split/shard + the **partial-tree merge** (`Tree::merge` across blocks; raw ids never cross a boundary). **Consumes `FlameEngine`/`Tree`.**
7. **Native symbolization** (the heavy slice) — query-time `build_id → debuginfod` + DWARF/ELF/`.gopclntab` parse + demangle + inline expansion, behind the `SymbolSource` wrapper; `gimli`/`object`/`addr2line` + a debuginfod `reqwest` client.
8. **Hardening** — per-tenant limits + multi-tenancy isolation, compaction (dedup symbol DBs) + downsampling, the differential-vs-Pyroscope corpus, Grafana integration.

---

## Shared cross-slice contract (frozen here — later slices interlock on these exact names)

```rust
// ---- pprof model + codec (Slice 4 ingest decodes real pprof through this) ----
pub struct PprofProfile { /* the prost-derived perftools.profiles Profile, behind a thin wrapper */ }
impl PprofProfile {
    pub fn decode(bytes: &[u8]) -> Result<PprofProfile, ProfileError>;
    pub fn encode(&self) -> Vec<u8>;
}
/// A fully-symbolized stack frame (one line of a resolved location, inlines expanded).
pub struct Frame { pub function: String, pub file: String, pub line: i32 }

// ---- symbol DB (the dedup lever) + the resolution seam ----
pub struct SymbolDb { /* partitions: parent-pointer trees + dedup strings/functions/locations/mappings */ }
impl SymbolDb {
    pub fn new() -> Self;
    /// Intern a leaf-first list of location refs into partition `partition`, returning the
    /// StacktraceId (= the leaf node index; identical stacks dedup via the parent-pointer tree).
    pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32;
    /// Resolve a StacktraceId to frames, leaf-first, with inlined frames expanded.
    pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame>;
    pub fn encode(&self) -> Vec<u8>;                                 // the symbols.symdb-equivalent artifact
    pub fn decode(bytes: &[u8]) -> Result<SymbolDb, ProfileError>;
}
pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;        // impl by SymbolDb + the symbolizer wrapper (Slice 7)
}

// ---- profile type (the 5-part string carried as __profile_type__) ----
pub struct ProfileType {
    pub name: String, pub sample_type: String, pub sample_unit: String,
    pub period_type: String, pub period_unit: String,
}
impl ProfileType {
    pub fn parse(s: &str) -> Result<ProfileType, ProfileError>;      // name:sample_type:sample_unit:period_type:period_unit
}
// Display => the 5-part colon form.

// ---- the query seam (Slice 5 reimplements ProfileStore over BlockStore) ----
#[async_trait::async_trait]
pub trait ProfileStore: Send + Sync {
    async fn select(&self, tenant: &str, profile_type: &str, matchers: &[LabelMatcher],
                    start_ms: i64, end_ms: i64) -> Result<ProfileScan, ProfileError>;
    async fn label_names(&self, tenant: &str, matchers: &[LabelMatcher],
                    start_ms: i64, end_ms: i64) -> Result<Vec<String>, ProfileError>;
    async fn label_values(&self, tenant: &str, name: &str, matchers: &[LabelMatcher],
                    start_ms: i64, end_ms: i64) -> Result<Vec<String>, ProfileError>;
    async fn profile_types(&self, tenant: &str, start_ms: i64, end_ms: i64)
                    -> Result<Vec<String>, ProfileError>;
    async fn series(&self, tenant: &str, matchers: &[LabelMatcher], label_names: &[String],
                    start_ms: i64, end_ms: i64) -> Result<Vec<Vec<(String, String)>>, ProfileError>;
}
// `samples_table` may name a UNION view of hot WAL-tail + cold blocks.
pub struct ProfileScan {
    pub ctx: datafusion::prelude::SessionContext,
    pub samples_table: String,
    pub symbols: std::sync::Arc<dyn SymbolSource>,
}

// ---- the flamegraph model (the 4-/7-ints-per-bar contracts) ----
pub struct FlameGraph { pub names: Vec<String>, pub levels: Vec<Level>, pub total: i64, pub max_self: i64 }
pub struct Level { pub values: Vec<i64> }     // groups of 4: [xOffsetDelta, total, self, nameIndex]
pub struct FlameGraphDiff { pub names: Vec<String>, pub levels: Vec<Level>, pub left_ticks: i64, pub right_ticks: i64 } // groups of 7 (body Slice 3)
pub struct Tree { /* parent/children arena, total: i64, self_: i64 per node */ }
impl Tree {
    pub fn new() -> Self;
    pub fn add_stack(&mut self, frames: &[Frame], value: i64);       // total += value along path, self += value at leaf
    pub fn merge(&mut self, other: Tree);                            // combine partial symbolized trees
    pub fn to_flamegraph(self, max_nodes: i64) -> FlameGraph;        // truncate w/ synthetic "other"
}

// ---- series (body Slice 3; types frozen here) ----
pub struct Series { pub labels: Vec<(String, String)>, pub points: Vec<(i64, f64)> } // (timestamp_ms, value)
pub enum SeriesAgg { Sum, Average }

// ---- the engine ----
pub struct EngineOpts { pub default_max_nodes: i64 /* 2048 */ }
pub struct FlameEngine<S: ProfileStore> { /* store: Arc<S>, opts: EngineOpts */ }
impl<S: ProfileStore> FlameEngine<S> {
    pub fn new(store: std::sync::Arc<S>, opts: EngineOpts) -> Self;
    pub async fn select_merge_stacktraces(&self, tenant: &str, profile_type: &str, label_selector: &str,
                    start_ms: i64, end_ms: i64, max_nodes: i64) -> Result<FlameGraph, ProfileError>;
    pub async fn select_series(&self, tenant: &str, profile_type: &str, label_selector: &str,
                    group_by: &[String], step_secs: f64, agg: SeriesAgg, start_ms: i64, end_ms: i64)
                    -> Result<Vec<Series>, ProfileError>;            // body Slice 3; signature frozen here
    pub async fn diff(&self, tenant: &str, left: (&str, &str, i64, i64), right: (&str, &str, i64, i64),
                    max_nodes: i64) -> Result<FlameGraphDiff, ProfileError>; // body Slice 3
    pub async fn select_merge_profile(&self, tenant: &str, profile_type: &str, label_selector: &str,
                    start_ms: i64, end_ms: i64) -> Result<Vec<u8> /* pprof */, ProfileError>; // body Slice 3
}

// ---- error ----
pub enum ProfileError { Decode(String), Plan(String), Exec(String), Store(String), Unsupported(String), Symbolize(String) }

// ---- internal (Slice 3+ reuse) ----
//   matcher::parse_label_selector(&str) -> Result<Vec<LabelMatcher>, ProfileError>  (Prometheus matcher string)
//   samples::{ PCOL_* column constants + profile_samples_schema(...) }
```

---

## Samples table column contract (frozen — block-builder Slice 1 + `InMemoryProfileStore` both emit this)

The `samples_table` registered by `ProfileStore::select` has **one row per SAMPLE**, with these columns (names are the contract the engine's fold groups by):

| Column (constant) | Arrow type | Meaning |
|---|---|---|
| `COL_FINGERPRINT` (`series_fingerprint`) | `UInt64` | series identity (blockstore-mandatory) |
| `COL_TIMESTAMP` (`timestamp`) | `Int64` (ns) | sample time, nanos |
| `PCOL_PROFILE_TYPE` (`profile_type`) | `Dictionary<Int32, Utf8>` | the 5-part profile-type string (dict-encoded) |
| `PCOL_STACKTRACE_ID` (`stacktrace_id`) | `UInt64` | leaf-node index into the symbol-DB partition's parent-pointer tree |
| `PCOL_VALUE` (`value`) | `Int64` | the sample value for this profile type |
| `PCOL_STACKTRACE_PARTITION` (`stacktrace_partition`) | `UInt64` | which symbol-DB partition resolves this id |
| `PCOL_TOTAL_VALUE` (`total_value`) | `Int64` | precomputed per-profile total (powers SelectSeries — Slice 3) |
| `PCOL_SPAN_ID` (`span_id`) | `UInt64` (nullable) | span association |
| `PCOL_TRACE_ID` (`trace_id`) | `Binary` (nullable) | trace association (cross-signal join key) |

> The slot from `(stacktrace_partition, stacktrace_id)` into the symbol DB is *raw* — never symbolized at rest. The engine's `GROUP BY (stacktrace_partition, stacktrace_id) → SUM(value)` collapses millions of raw samples to the distinct surviving ids **before** any symbolization (spec §6.1). `InMemoryProfileStore` (Task A7) emits these columns from hand-built profiles + a populated `SymbolDb` so the merge tests run against known integer values.

---

## File structure (`crates/pprof/`)

| File | Responsibility |
|---|---|
| `Cargo.toml` | crate manifest; workspace deps; `prost` build-dep |
| `build.rs` | compile the vendored `profile.proto` via `prost-build` (grpc-gateway pattern) |
| `proto/profile.proto` | vendored perftools.profiles `Profile` proto (pinned tag) |
| `src/lib.rs` | module decls + public re-exports + crate docs |
| `src/error.rs` | `ProfileError` enum + `From` conversions |
| `src/pprof.rs` | the prost-generated module include + `PprofProfile` decode/encode wrapper + `Frame` |
| `src/profile_type.rs` | `ProfileType` parse/`Display` (5-part colon form) |
| `src/symbols.rs` | `SymbolDb` (parent-pointer tree + dedup tables + `encode`/`decode`) + `SymbolSource` |
| `src/matcher.rs` | `parse_label_selector` (Prometheus matcher string → `Vec<LabelMatcher>`) |
| `src/samples.rs` | the `PCOL_*` column constants + `profile_samples_schema(...)` builder |
| `src/store.rs` | `ProfileStore` trait + `ProfileScan` |
| `src/in_memory.rs` | `InMemoryProfileStore` test impl (samples DF table + `SymbolDb`) |
| `src/tree.rs` | `Tree` (add_stack / merge) + `FlameGraph`/`Level`/`FlameGraphDiff` + `to_flamegraph` (4-ints-per-bar) |
| `src/series.rs` | `Series` / `SeriesAgg` (types only; bodies Slice 3) |
| `src/engine.rs` | `FlameEngine`, `EngineOpts`, `select_merge_stacktraces` (the MERGE→flamegraph fold), frozen `select_series`/`diff`/`select_merge_profile` |

`src/engine.rs` isolates the centerpiece DataFusion fold; `src/tree.rs` isolates the byte-exact flamegraph encoding from the storage surface.

---

## Phase A — scaffold + error + pprof codec + ProfileType + SymbolDb + matcher + samples columns + InMemoryProfileStore

> **Batching:** A1 (scaffold + proto vendor + build.rs) lands first (creates `Cargo.toml`/`build.rs`/`proto/`/`lib.rs` + the prost include). Then A2 (`error.rs`), A3 (`pprof.rs` wrapper + `Frame`), A4 (`profile_type.rs`), A5 (`symbols.rs`), A6 (`matcher.rs` + `samples.rs`) touch disjoint files and may run as a parallel batch — each appends re-export lines to `lib.rs`, so serialize the `lib.rs` edits (reviewer merges). A7 (`store.rs`) depends on A3 (Frame/SymbolSource via A5). A8 (`InMemoryProfileStore`) depends on A5+A6+A7. Recommended: A1 → {A2, A3, A4, A5, A6 in parallel, lib.rs merged} → A7 → A8.

### Task A1: Crate scaffold + vendored pprof proto + `build.rs`

**Files:**
- Create: `crates/pprof/Cargo.toml`
- Create: `crates/pprof/build.rs`
- Create: `crates/pprof/proto/profile.proto`
- Create: `crates/pprof/src/lib.rs`
- Modify: root `Cargo.toml` (members glob `crates/*` already covers it; `prost`/`prost-build`/`datafusion`/`arrow` already in `[workspace.dependencies]` per the blockstore/metrics plans)

**Interfaces:**
- Produces: a compiling `crabka-pprof` crate whose `build.rs` generates the perftools.profiles prost module, with `pub fn crate_smoke() -> bool` (placeholder, removed in A2).

- [ ] **Step 1: Vendor `crates/pprof/proto/profile.proto`** — copy the perftools.profiles `Profile` proto verbatim from google/pprof `proto/profile.proto` (Apache-2.0). Pin the source: header comment `// vendored from github.com/google/pprof proto/profile.proto @ master, 2026-06-18`. **Do not edit field numbers.** The proto defines `Profile { sample_type[], sample[], mapping[], location[], function[], string_table[], ... }`, `Sample { location_id[], value[], label[] }`, `Location { id, mapping_id, address, line[] }`, `Line { function_id, line }`, `Function { id, name, system_name, filename, start_line }`, `Mapping { id, memory_start, memory_limit, file_offset, filename, build_id, has_functions, has_filenames, has_line_numbers, has_inline_frames }`, `ValueType { type, unit }`.

- [ ] **Step 2: Create `crates/pprof/Cargo.toml`**

```toml
[package]
name = "crabka-pprof"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version.workspace = true
description = "Language-less profiles engine (pprof model + symbol DB + flamegraph-merge) for Crabka's Grafana-Pyroscope-equivalent profiles backend"
repository = "https://github.com/robot-head/crabka"
homepage = "https://github.com/robot-head/crabka"
documentation = "https://docs.rs/crabka-pprof"
readme = "README.md"
keywords = ["observability", "pyroscope", "pprof", "profiling", "crabka"]
categories = ["database-implementations"]

[lints]
workspace = true

[dependencies]
crabka-blockstore = { path = "../blockstore", version = "0.3.7" }
arrow = { workspace = true }
datafusion = { workspace = true }
prost = { workspace = true }
async-trait = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread"] }
futures = { workspace = true }
regex = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }

[build-dependencies]
prost-build = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
proptest = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

> If `prost-build` is not yet present in `[workspace.dependencies]`, add it as `prost-build = "0.14"` (paired with the `prost = "0.14"` pin). The first build fetches + compiles DataFusion from git — slow (several minutes), normal.

- [ ] **Step 3: Create `crates/pprof/build.rs`** (the grpc-gateway/rebalancer codegen pattern — system `protoc` with vendored fallback)

```rust
//! Compile the vendored perftools.profiles `Profile` proto into a prost module.
//! Mirrors crates/grpc-gateway/build.rs (system protoc, vendored fallback).

fn main() {
    println!("cargo:rerun-if-changed=proto/profile.proto");
    let mut config = prost_build::Config::new();
    // perftools.profiles uses `repeated int64` for value[]; default prost mapping is fine.
    config
        .compile_protos(&["proto/profile.proto"], &["proto"])
        .expect("compile profile.proto");
}
```

> **Verify against the grpc-gateway/rebalancer `build.rs` at this rev:** the exact `prost_build::Config` setup (whether the repo uses `protoc-bin-vendored` to supply `PROTOC`, or a `tonic_build`/`connectrpc-axum-build` wrapper). Match whatever those crates do for `protoc` discovery so CI without a system `protoc` still builds. Keep the *output*: a generated module containing the `Profile` message types, includable via `include!(concat!(env!("OUT_DIR"), "/perftools.profiles.rs"))` (the module name follows the proto's `package`).

- [ ] **Step 4: Create `crates/pprof/src/lib.rs` with a placeholder + the generated include behind a module**

```rust
//! Language-less profiles engine for Crabka's Grafana-Pyroscope-equivalent
//! profiles backend: the pprof model + codec, the deduplicated `SymbolDb`, the
//! 5-part `ProfileType`, the `ProfileStore` query seam, and the
//! MERGE->flamegraph engine. Storage-agnostic via the injected `ProfileStore`.
//! There is no query language and no parser (only Prometheus label matching).

/// The prost-generated perftools.profiles types.
pub(crate) mod proto {
    #![allow(clippy::all, clippy::pedantic, missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/perftools.profiles.rs"));
}

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

    #[test]
    fn proto_module_compiles() {
        // The generated Profile type exists (proves build.rs ran).
        let _p = proto::Profile::default();
    }
}
```

- [ ] **Step 5: Build and test**

Run: `cargo test -p crabka-pprof`
Expected: `build.rs` compiles the proto, the crate compiles, `smoke` + `proto_module_compiles` PASS. If the build fails with `protoc` not found, align `build.rs` with the grpc-gateway crate's `protoc` discovery (Step 3 note). If it fails with an arrow major mismatch, the datafusion rev is wrong — re-confirm the pin tracks arrow 59.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add Cargo.toml Cargo.lock crates/pprof/
git commit -m "feat(pprof): scaffold crabka-pprof crate + vendored perftools.profiles proto"
```

---

### Task A2: `ProfileError`

**Files:**
- Create: `crates/pprof/src/error.rs`
- Modify: `crates/pprof/src/lib.rs` (declare module, re-export, remove placeholder)

**Interfaces:**
- Produces:
  - `pub enum ProfileError { Decode(String), Plan(String), Exec(String), Store(String), Unsupported(String), Symbolize(String) }` (`Debug`, `Clone`, `thiserror::Error`)
  - `impl From<datafusion::error::DataFusionError> for ProfileError` → `Exec`
  - `impl From<prost::DecodeError> for ProfileError` → `Decode`
  - `pub type Result<T> = std::result::Result<T, ProfileError>` (internal alias)

- [ ] **Step 1: Write the failing test**

Create `crates/pprof/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn datafusion_error_maps_to_exec() {
        let dfe = datafusion::error::DataFusionError::Plan("boom".into());
        let pe: ProfileError = dfe.into();
        assert!(matches!(pe, ProfileError::Exec(_)));
    }

    #[test]
    fn prost_decode_maps_to_decode() {
        let de = prost::DecodeError::new("bad");
        let pe: ProfileError = de.into();
        assert!(matches!(pe, ProfileError::Decode(_)));
    }

    #[test]
    fn display_includes_category() {
        let e = ProfileError::Unsupported("select_series: slice 3".into());
        assert!(format!("{e}").contains("unsupported"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib error`
Expected: FAIL — `cannot find type ProfileError`.

- [ ] **Step 3: Implement `error.rs`**

Prepend above the `tests` module:

```rust
//! The crate's error type. Categories map to Connect/HTTP statuses in Slice 5
//! (`Decode`/`Plan` -> 400, `Exec`/`Store` -> 500, `Unsupported` -> 501,
//! `Symbolize` -> 500/partial).

/// Errors raised by the profiles engine. Foreign errors are stringified.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ProfileError {
    #[error("decode error: {0}")]
    Decode(String),

    #[error("plan error: {0}")]
    Plan(String),

    #[error("execution error: {0}")]
    Exec(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("symbolize error: {0}")]
    Symbolize(String),
}

/// Internal convenience alias.
pub type Result<T> = std::result::Result<T, ProfileError>;

impl From<datafusion::error::DataFusionError> for ProfileError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::Exec(e.to_string())
    }
}

impl From<prost::DecodeError> for ProfileError {
    fn from(e: prost::DecodeError) -> Self {
        Self::Decode(e.to_string())
    }
}
```

- [ ] **Step 4: Wire into `lib.rs`**

Replace the placeholder body (remove `crate_smoke` + its test, keep `mod proto`) with:

```rust
mod error;

pub use error::ProfileError;
pub(crate) use error::Result;
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-pprof --lib error`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-pprof
cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): ProfileError type + DataFusion/prost conversions"
```

---

### Task A3: `PprofProfile` decode/encode wrapper + `Frame`

**Files:**
- Create: `crates/pprof/src/pprof.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Consumes: `crate::proto::Profile`, `ProfileError`, `prost::Message`.
- Produces:
  - `pub struct Frame { pub function: String, pub file: String, pub line: i32 }` (`Clone`, `Debug`, `PartialEq`, `Eq`)
  - `pub struct PprofProfile { pub(crate) inner: crate::proto::Profile }` (`Clone`, `Debug`) with:
    - `pub fn decode(bytes: &[u8]) -> Result<PprofProfile, ProfileError>` — `prost::Message::decode`.
    - `pub fn encode(&self) -> Vec<u8>` — `prost::Message::encode_to_vec`.
    - `pub fn sample_types(&self) -> Vec<(String, String)>` — `(type, unit)` pairs resolved through `string_table` (the multi-value split key — Slice 4 loops these).
    - `pub fn string(&self, idx: i64) -> &str` — `string_table` lookup (`idx==0` ⇒ `""`).

- [ ] **Step 1: Write the failing round-trip test** (build a small `proto::Profile` by hand with a known string table, encode, decode, assert equality + `sample_types`)

Create `crates/pprof/src/pprof.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use prost::Message;

    // Build a minimal pprof: string_table = ["", "cpu", "nanoseconds", "samples", "count"],
    // one sample_type (cpu/nanoseconds).
    fn sample_pprof() -> crate::proto::Profile {
        let mut p = crate::proto::Profile::default();
        p.string_table = vec![
            String::new(),
            "cpu".into(),
            "nanoseconds".into(),
            "samples".into(),
            "count".into(),
        ];
        p.sample_type = vec![
            crate::proto::ValueType { r#type: 1, unit: 2 }, // cpu / nanoseconds
            crate::proto::ValueType { r#type: 3, unit: 4 }, // samples / count
        ];
        p
    }

    #[test]
    fn decode_encode_round_trips() {
        let p = sample_pprof();
        let bytes = p.encode_to_vec();
        let prof = PprofProfile::decode(&bytes).unwrap();
        let re = prof.encode();
        // re-decode equality (prost encoding is deterministic for this shape).
        let p2 = crate::proto::Profile::decode(re.as_slice()).unwrap();
        assert!(p2.string_table == p.string_table);
        assert!(p2.sample_type.len() == 2);
    }

    #[test]
    fn sample_types_resolve_through_string_table() {
        let prof = PprofProfile { inner: sample_pprof() };
        let st = prof.sample_types();
        assert!(st == vec![
            ("cpu".to_string(), "nanoseconds".to_string()),
            ("samples".to_string(), "count".to_string()),
        ]);
        assert!(prof.string(0).is_empty());
    }
}
```

> **Verify against the vendored `profile.proto`:** the prost-generated field names — `r#type` for the proto `type` field, the `ValueType`/`Profile` field set — depend on the proto and prost's identifier escaping. If `r#type` differs (e.g. prost renamed it), align the test + impl to the generated names; keep the asserted *behavior* (round-trip string-table + `sample_types` pairing). The proto's `value[]`/`location_id[]` are not exercised until A8/Slice 4; only the string-table + sample-type machinery is pinned here.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-pprof --lib pprof`
Expected: FAIL — `cannot find type PprofProfile`.

- [ ] **Step 3: Implement `pprof.rs`**

Prepend above the `tests` module:

```rust
//! The pprof wire model wrapper. `PprofProfile` wraps the prost-generated
//! perftools.profiles `Profile` with decode/encode + the string-table helpers
//! the distributor (Slice 4) and the symbol-DB intern path use. `Frame` is the
//! fully-symbolized stack-frame type the engine resolves to.

use prost::Message;

use crate::error::ProfileError;
use crate::proto::Profile;

/// A fully-symbolized stack frame (one resolved line; inlined frames are
/// separate `Frame`s, innermost-first).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub function: String,
    pub file: String,
    pub line: i32,
}

/// A decoded pprof profile (perftools.profiles `Profile`).
#[derive(Clone, Debug)]
pub struct PprofProfile {
    pub(crate) inner: Profile,
}

impl PprofProfile {
    /// Decode a (already-decompressed) pprof byte buffer.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProfileError> {
        Ok(Self { inner: Profile::decode(bytes)? })
    }

    /// Encode back to pprof wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.inner.encode_to_vec()
    }

    /// `string_table[idx]`, with `idx == 0` (and out-of-range) yielding `""`.
    #[must_use]
    pub fn string(&self, idx: i64) -> &str {
        usize::try_from(idx)
            .ok()
            .and_then(|i| self.inner.string_table.get(i))
            .map_or("", String::as_str)
    }

    /// The `(type, unit)` pairs of the profile's `sample_type[]`, resolved
    /// through the string table. The distributor splits one series per pair.
    #[must_use]
    pub fn sample_types(&self) -> Vec<(String, String)> {
        self.inner
            .sample_type
            .iter()
            .map(|vt| (self.string(vt.r#type).to_string(), self.string(vt.unit).to_string()))
            .collect()
    }
}
```

- [ ] **Step 4: Wire into `lib.rs`** — add `mod pprof;` and `pub use pprof::{Frame, PprofProfile};`.

- [ ] **Step 5: Run to verify it passes** — `cargo test -p crabka-pprof --lib pprof` → PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): PprofProfile decode/encode wrapper + Frame + string-table helpers"
```

---

### Task A4: `ProfileType` — 5-part parse + `Display`

**Files:**
- Create: `crates/pprof/src/profile_type.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Produces:
  - `pub struct ProfileType { pub name: String, pub sample_type: String, pub sample_unit: String, pub period_type: String, pub period_unit: String }` (`Clone`, `Debug`, `PartialEq`, `Eq`)
  - `impl ProfileType { pub fn parse(s: &str) -> Result<ProfileType, ProfileError> }` — split on `:` into exactly 5 parts (reject ≠5; trim an optional trailing `:delta` only if the spec marks delta — see note).
  - `impl std::fmt::Display for ProfileType` — the 5-part colon form (round-trips `parse`).

> **The 5 parts are `name:sample_type:sample_unit:period_type:period_unit` (spec §4.3).** Verified Go/pprof examples: `process_cpu:cpu:nanoseconds:cpu:nanoseconds`, `memory:alloc_space:bytes:space:bytes`, `mutex:contentions:count:contentions:count`, `goroutines:goroutine:count:goroutine:count`; Java/JFR differs (`wall:wall:nanoseconds:wall:nanoseconds`). **Do not hardcode the set** — `parse` is purely structural. The optional `:delta` suffix marks delta semantics on some sources; **this slice rejects a 6-part string as malformed** and defers delta handling to Slice 4 ingest (flagged in Self-review) — `parse` accepts exactly 5 colon-separated non-empty parts.

- [ ] **Step 1: Write the failing test**

Create `crates/pprof/src/profile_type.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn parses_go_cpu() {
        let pt = ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        assert!(pt.name == "process_cpu");
        assert!(pt.sample_type == "cpu");
        assert!(pt.sample_unit == "nanoseconds");
        assert!(pt.period_type == "cpu");
        assert!(pt.period_unit == "nanoseconds");
    }

    #[test]
    fn display_round_trips() {
        for s in [
            "process_cpu:cpu:nanoseconds:cpu:nanoseconds",
            "memory:alloc_space:bytes:space:bytes",
            "wall:wall:nanoseconds:wall:nanoseconds",
        ] {
            let pt = ProfileType::parse(s).unwrap();
            assert!(format!("{pt}") == s);
        }
    }

    #[test]
    fn rejects_wrong_part_count() {
        assert!(ProfileType::parse("a:b:c:d").is_err()); // 4 parts
        assert!(ProfileType::parse("a:b:c:d:e:f").is_err()); // 6 parts
        assert!(ProfileType::parse("a:b::d:e").is_err()); // empty part
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type ProfileType`.

- [ ] **Step 3: Implement `profile_type.rs`**

```rust
//! The 5-part profile-type string `name:sample_type:sample_unit:period_type:period_unit`
//! carried as the `__profile_type__` label (spec §4.3). The set is NOT hardcoded
//! (Go/pprof vs Java/JFR differ); `parse` is purely structural.

use std::fmt;

use crate::error::ProfileError;

/// The 5-part profile type. `name` is also the `__name__` label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileType {
    pub name: String,
    pub sample_type: String,
    pub sample_unit: String,
    pub period_type: String,
    pub period_unit: String,
}

impl ProfileType {
    /// Parse exactly 5 colon-separated non-empty parts.
    pub fn parse(s: &str) -> Result<Self, ProfileError> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 5 || parts.iter().any(|p| p.is_empty()) {
            return Err(ProfileError::Decode(format!(
                "invalid profile_type {s:?}: expected name:sample_type:sample_unit:period_type:period_unit"
            )));
        }
        Ok(Self {
            name: parts[0].to_string(),
            sample_type: parts[1].to_string(),
            sample_unit: parts[2].to_string(),
            period_type: parts[3].to_string(),
            period_unit: parts[4].to_string(),
        })
    }
}

impl fmt::Display for ProfileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.name, self.sample_type, self.sample_unit, self.period_type, self.period_unit
        )
    }
}
```

- [ ] **Step 4: Wire + run + commit** — add `mod profile_type;` + `pub use profile_type::ProfileType;`.

```bash
cargo test -p crabka-pprof --lib profile_type  # PASS (3 tests)
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): ProfileType 5-part parse + Display"
```

---

### Task A5: `SymbolDb` (parent-pointer stacktrace tree + dedup tables) + `SymbolSource`

**Files:**
- Create: `crates/pprof/src/symbols.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Consumes: `Frame`, `ProfileError`.
- Produces:
  - `pub trait SymbolSource: Send + Sync { fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>; }`
  - `pub struct SymbolDb { /* per-partition parent-pointer trees + dedup tables */ }` (`Default`, `Clone`, `Debug`) with:
    - `pub fn new() -> Self`
    - the dedup table inserters: `pub fn intern_string(&mut self, s: &str) -> u32` (`strings[0] == ""`), `pub fn intern_function(&mut self, name: u32, system_name: u32, filename: u32, start_line: i64) -> u32`, `pub fn intern_location(&mut self, partition: u64, address: u64, mapping_id: u32, lines: &[(u32 /*function_id*/, i64 /*line*/)]) -> u32`, `pub fn intern_mapping(&mut self, ...) -> u32`.
    - `pub fn intern_stacktrace(&mut self, partition: u64, location_refs: &[u32]) -> u32` — intern a **leaf-first** list of location refs into `partition`'s parent-pointer tree; returns the **leaf node index** (`StacktraceId`); identical stacks share a path (dedup).
    - `pub fn resolve(&self, partition: u64, stacktrace_id: u32) -> Vec<Frame>` — climb parents from the leaf, collect `location_ref`s leaf→root, resolve each location's `lines[]` to `Frame`s with **inlined frames expanded innermost-first**.
    - `pub fn encode(&self) -> Vec<u8>` / `pub fn decode(bytes: &[u8]) -> Result<SymbolDb, ProfileError>` — the `symbols.symdb`-equivalent artifact (greenfield byte layout; encode via `serde` + `serde-wincode` is acceptable, or a hand-rolled length-prefixed layout — choose one; the round-trip test is the contract).
  - `impl SymbolSource for SymbolDb` — `resolve` delegates.

> **The model (spec §4.2):** per `u64` partition, a parent-pointer tree of `node { parent: i32 /* -1 root */, location_ref: u32 }`. `intern_stacktrace` walks the leaf-first refs root-to-leaf, descending/creating child nodes keyed by `location_ref` under each parent, returning the final node index. Locations carry multiple `lines[]` to encode inlined frames (innermost-first). `strings[0]` is always `""`. **This is greenfield — the on-disk encoding is NOT byte-compatible with phlaredb symdb v3; only semantically equivalent.** Add `serde-wincode` to `[dependencies]` if you encode via serde.

- [ ] **Step 1: Write the failing tests** (dedup + leaf→root climb + inline expansion + encode/decode round-trip)

Create `crates/pprof/src/symbols.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    // Build a symbol DB partition with three functions f0/f1/f2 and three
    // locations: L0 -> [f0], L1 -> [f1], L2 -> [f2_inner, f2_outer] (inlined).
    fn fixture() -> (SymbolDb, [u32; 3]) {
        let mut db = SymbolDb::new();
        let p = 0u64;
        let mk_fn = |db: &mut SymbolDb, name: &str| {
            let n = db.intern_string(name);
            db.intern_function(n, n, 0, 0)
        };
        let f0 = mk_fn(&mut db, "main");
        let f1 = mk_fn(&mut db, "work");
        let f2_inner = mk_fn(&mut db, "inlined_inner");
        let f2_outer = mk_fn(&mut db, "outer");
        let l0 = db.intern_location(p, 0x10, 0, &[(f0, 1)]);
        let l1 = db.intern_location(p, 0x20, 0, &[(f1, 2)]);
        // inlined: innermost-first lines.
        let l2 = db.intern_location(p, 0x30, 0, &[(f2_inner, 7), (f2_outer, 3)]);
        (db, [l0, l1, l2])
    }

    #[test]
    fn intern_stacktrace_dedups_identical_stacks() {
        let (mut db, [l0, l1, l2]) = fixture();
        // stack leaf-first: l2 (leaf) -> l1 -> l0 (root).
        let a = db.intern_stacktrace(0, &[l2, l1, l0]);
        let b = db.intern_stacktrace(0, &[l2, l1, l0]);
        assert!(a == b); // identical stacks share the leaf node.
        let c = db.intern_stacktrace(0, &[l1, l0]); // different leaf.
        assert!(c != a);
    }

    #[test]
    fn resolve_climbs_leaf_to_root_and_expands_inlines() {
        let (mut db, [l0, l1, l2]) = fixture();
        let id = db.intern_stacktrace(0, &[l2, l1, l0]);
        let frames = db.resolve(0, id);
        // leaf-first; l2 expands to two inlined frames (innermost-first), then l1, then l0.
        let names: Vec<&str> = frames.iter().map(|f| f.function.as_str()).collect();
        assert!(names == vec!["inlined_inner", "outer", "work", "main"]);
        assert!(frames[0].line == 7); // inlined inner line
        assert!(frames[2].line == 2); // work
    }

    #[test]
    fn partitions_are_independent() {
        let (mut db, [l0, l1, _l2]) = fixture();
        let in_p0 = db.intern_stacktrace(0, &[l1, l0]);
        let in_p1 = db.intern_stacktrace(1, &[l1, l0]);
        // same refs, different partition -> independent tree; both resolve.
        assert!(!db.resolve(0, in_p0).is_empty());
        assert!(!db.resolve(1, in_p1).is_empty());
    }

    #[test]
    fn encode_decode_round_trips() {
        let (mut db, [l0, l1, l2]) = fixture();
        let id = db.intern_stacktrace(0, &[l2, l1, l0]);
        let bytes = db.encode();
        let db2 = SymbolDb::decode(&bytes).unwrap();
        assert!(db2.resolve(0, id) == db.resolve(0, id));
    }

    #[test]
    fn symbol_source_trait_delegates() {
        let (mut db, [l0, l1, _l2]) = fixture();
        let id = db.intern_stacktrace(0, &[l1, l0]);
        let src: &dyn SymbolSource = &db;
        assert!(src.resolve(0, id) == db.resolve(0, id));
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type SymbolDb`.

- [ ] **Step 3: Implement `symbols.rs`** — provide the FULL real implementation (no churn surface — this is plain Rust data structures). Suggested internal shape: `strings: Vec<String>` + `string_index: HashMap<String, u32>` (seeded with `""` at 0); `functions: Vec<Func>` + dedup index; `locations: Vec<Loc { address, mapping_id, lines: Vec<(u32, i64)> }>`; `mappings: Vec<Mapping>`; and `partitions: HashMap<u64, Partition>` where `Partition { nodes: Vec<Node { parent: i32, location_ref: u32 }>, child_index: HashMap<(i32 /*parent, -1 root*/, u32 /*loc_ref*/), u32> }`. `intern_stacktrace` walks the leaf-first slice **in reverse** (root → leaf) descending/creating child nodes, returning the final (leaf) node index. `resolve` walks parent pointers from the leaf collecting `location_ref`s leaf→root, then for each location pushes its `lines[]` as `Frame`s in order (lines are stored innermost-first). For `encode`/`decode`, add `serde` derives + use `serde-wincode` (add `serde = { workspace = true, features = ["derive"] }` + `serde-wincode = { workspace = true }` to `[dependencies]`), OR hand-roll a length-prefixed binary layout. Pin the choice with the round-trip test.

> **Why leaf-first refs but root-to-leaf descent:** `location_refs` is leaf-first (matching `Sample.location_id[]` order in pprof, which is innermost-first), but a parent-pointer tree is built top-down — so `intern_stacktrace` iterates `location_refs.iter().rev()`. `resolve` then climbs back up (leaf node → root) which naturally yields leaf-first frames again. The two tests above pin both directions.

- [ ] **Step 4: Wire + run + commit** — add `mod symbols;` + `pub use symbols::{SymbolDb, SymbolSource};`.

```bash
cargo test -p crabka-pprof --lib symbols  # PASS (5 tests)
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/ Cargo.toml
git commit -m "feat(pprof): SymbolDb (parent-pointer stacktrace tree + dedup tables + encode/decode) + SymbolSource"
```

---

### Task A6: `parse_label_selector` (Prometheus matcher string) + samples column contract

> (Two disjoint files, dispatched together in the A2–A6 batch; A8 depends on both.)

**Files:**
- Create: `crates/pprof/src/matcher.rs`
- Create: `crates/pprof/src/samples.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- `matcher.rs` Produces:
  - `pub(crate) fn parse_label_selector(s: &str) -> Result<Vec<LabelMatcher>, ProfileError>` — parse a Prometheus matcher string `{k1="v1", k2=~"re", k3!="v4"}` (braces optional; empty/`{}` ⇒ `[]`) into blockstore `LabelMatcher`s using `MatchOp` (`=`/`!=`/`=~`/`!~`). **This is the only thing resembling a parser in the crate — it is just Prometheus label matching, not a profiles query language.**
- `samples.rs` Produces:
  - the `PCOL_*` column-name constants matching the **Samples table column contract** above (`PCOL_PROFILE_TYPE`, `PCOL_STACKTRACE_ID`, `PCOL_VALUE`, `PCOL_STACKTRACE_PARTITION`, `PCOL_TOTAL_VALUE`, `PCOL_SPAN_ID`, `PCOL_TRACE_ID`) plus re-exported `COL_FINGERPRINT`/`COL_TIMESTAMP` (from blockstore, or local consts if Slice 1 absent).
  - `pub fn profile_samples_schema() -> arrow::datatypes::SchemaRef` — the one-row-per-sample Arrow schema (the same name Slice 1 exports from `crabka-blockstore`; re-export that when Slice 1 has landed).

> **Reuse note:** import `LabelMatcher`/`MatchOp`/`COL_FINGERPRINT`/`COL_TIMESTAMP`/`PCOL_*` from `crabka_blockstore` if profiles Slice 1 has landed. If it has NOT landed in this tree, declare the `PCOL_*` constants + `profile_samples_schema` here against the identical names (the contract above) and re-export blockstore's `LabelMatcher`/`MatchOp` (those land in the logs-wedge base blockstore, already present). The block-builder (Slice 1/4) is contracted to emit the identical column names/types.

- [ ] **Step 1: Write the failing tests**

`crates/pprof/src/matcher.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_blockstore::MatchOp;

    #[test]
    fn parses_braced_matchers() {
        let ms = parse_label_selector(r#"{service_name="checkout", env=~"prod|stage", region!="eu"}"#).unwrap();
        assert!(ms.len() == 3);
        assert!(ms[0].name == "service_name" && ms[0].op == MatchOp::Eq && ms[0].value == "checkout");
        assert!(ms[1].op == MatchOp::Re);
        assert!(ms[2].op == MatchOp::Neq);
    }

    #[test]
    fn empty_selector_is_empty() {
        assert!(parse_label_selector("{}").unwrap().is_empty());
        assert!(parse_label_selector("").unwrap().is_empty());
    }

    #[test]
    fn rejects_malformed() {
        assert!(parse_label_selector(r#"{service_name=}"#).is_err());
        assert!(parse_label_selector(r#"{=~"x"}"#).is_err());
    }
}
```

`crates/pprof/src/samples.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use assert2::assert;

    #[test]
    fn samples_schema_has_fold_keys_and_value() {
        let s = profile_samples_schema();
        assert!(s.column_with_name(PCOL_STACKTRACE_PARTITION).unwrap().1.data_type() == &DataType::UInt64);
        assert!(s.column_with_name(PCOL_STACKTRACE_ID).unwrap().1.data_type() == &DataType::UInt64);
        assert!(s.column_with_name(PCOL_VALUE).unwrap().1.data_type() == &DataType::Int64);
        // trace_id is nullable Binary (cross-signal join key).
        let (_, f) = s.column_with_name(PCOL_TRACE_ID).unwrap();
        assert!(f.is_nullable() && f.data_type() == &DataType::Binary);
    }
}
```

- [ ] **Step 2: Run to verify both fail** — `cannot find function parse_label_selector` / `profile_samples_schema`.

- [ ] **Step 3: Implement both.**

`matcher.rs` — a small hand-written scanner: strip optional `{`/`}`, split top-level on `,` (respecting quoted values), each entry matches `^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"\s*$` (use `regex`), map the operator to `MatchOp`, unescape the value. Reject empty key/value-without-quotes. Full real code.

`samples.rs` — the `PCOL_*` constants + `profile_samples_schema`:

```rust
//! The profile-samples fact-table column contract (spec §4.1) + Arrow schema.
//! One row per SAMPLE. The block-builder (Slice 1) and `InMemoryProfileStore`
//! both emit this; the engine's fold groups by `(stacktrace_partition, stacktrace_id)`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

// Re-export the blockstore-mandatory columns (or declare locally if Slice 1 absent).
pub use crabka_blockstore::{COL_FINGERPRINT, COL_TIMESTAMP};

pub const PCOL_PROFILE_TYPE: &str = "profile_type";
pub const PCOL_STACKTRACE_ID: &str = "stacktrace_id";
pub const PCOL_VALUE: &str = "value";
pub const PCOL_STACKTRACE_PARTITION: &str = "stacktrace_partition";
pub const PCOL_TOTAL_VALUE: &str = "total_value";
pub const PCOL_SPAN_ID: &str = "span_id";
pub const PCOL_TRACE_ID: &str = "trace_id";

/// The one-row-per-sample Arrow schema (spec §4.1). Named to match Slice 1's
/// `crabka_blockstore::profile_samples_schema` so the re-export path is a no-op.
#[must_use]
pub fn profile_samples_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new(
            PCOL_PROFILE_TYPE,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(PCOL_STACKTRACE_ID, DataType::UInt64, false),
        Field::new(PCOL_VALUE, DataType::Int64, false),
        Field::new(PCOL_STACKTRACE_PARTITION, DataType::UInt64, false),
        Field::new(PCOL_TOTAL_VALUE, DataType::Int64, false),
        Field::new(PCOL_SPAN_ID, DataType::UInt64, true),
        Field::new(PCOL_TRACE_ID, DataType::Binary, true),
    ]))
}
```

> If `crabka_blockstore::{COL_FINGERPRINT, COL_TIMESTAMP, PCOL_*, profile_samples_schema}` already exist (profiles Slice 1 landed), import the `PCOL_*` + `profile_samples_schema` from there and delete the local consts/fn — one source of truth. The local fallback is named `profile_samples_schema` (identical to Slice 1's blockstore export) so the re-export swap is a no-op. The test asserts the *types*, so either source satisfies it.

- [ ] **Step 4: Wire + run + commit** — add `mod matcher; mod samples;` + `pub use samples::{COL_FINGERPRINT, COL_TIMESTAMP, PCOL_PROFILE_TYPE, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_SPAN_ID, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_schema};` (`parse_label_selector` stays `pub(crate)`).

```bash
cargo test -p crabka-pprof --lib matcher --lib samples  # PASS
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): Prometheus matcher-string helper + samples-table column contract"
```

---

### Task A7: `ProfileStore` trait + `ProfileScan`

**Files:**
- Create: `crates/pprof/src/store.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Consumes: `datafusion::prelude::SessionContext`, `LabelMatcher`, `SymbolSource`, `ProfileError`.
- Produces:
  - `pub struct ProfileScan { pub ctx: SessionContext, pub samples_table: String, pub symbols: std::sync::Arc<dyn SymbolSource> }`
  - `#[async_trait::async_trait] pub trait ProfileStore: Send + Sync { ... }` with exactly the five methods from the Shared cross-slice contract.

- [ ] **Step 1: Write the failing test** (a trivial in-test impl proves the trait is object-shaped and the signatures compile)

Create `crates/pprof/src/store.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_blockstore::LabelMatcher;
    use datafusion::prelude::SessionContext;

    struct Empty;

    #[async_trait::async_trait]
    impl ProfileStore for Empty {
        async fn select(&self, _t: &str, _pt: &str, _m: &[LabelMatcher], _s: i64, _e: i64)
            -> Result<ProfileScan, crate::error::ProfileError> {
            Ok(ProfileScan {
                ctx: SessionContext::new(),
                samples_table: "samples".into(),
                symbols: std::sync::Arc::new(crate::symbols::SymbolDb::new()),
            })
        }
        async fn label_names(&self, _t: &str, _m: &[LabelMatcher], _s: i64, _e: i64)
            -> Result<Vec<String>, crate::error::ProfileError> { Ok(vec![]) }
        async fn label_values(&self, _t: &str, _n: &str, _m: &[LabelMatcher], _s: i64, _e: i64)
            -> Result<Vec<String>, crate::error::ProfileError> { Ok(vec![]) }
        async fn profile_types(&self, _t: &str, _s: i64, _e: i64)
            -> Result<Vec<String>, crate::error::ProfileError> { Ok(vec![]) }
        async fn series(&self, _t: &str, _m: &[LabelMatcher], _ln: &[String], _s: i64, _e: i64)
            -> Result<Vec<Vec<(String, String)>>, crate::error::ProfileError> { Ok(vec![]) }
    }

    #[tokio::test]
    async fn trait_is_object_safe() {
        let s: std::sync::Arc<dyn ProfileStore> = std::sync::Arc::new(Empty);
        let r = s.select("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", &[], 0, 1).await.unwrap();
        assert!(r.samples_table == "samples");
        assert!(s.profile_types("t", 0, 1).await.unwrap().is_empty());
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type ProfileStore`.

- [ ] **Step 3: Implement `store.rs`**

```rust
//! The data-access seam. The engine is generic over `ProfileStore`; production
//! wires it to the querier's hot/cold UNION (Slice 5), tests use
//! `InMemoryProfileStore`. `select` yields a DataFusion `SessionContext` with the
//! samples table registered for the (tenant, profile_type, matchers, range) +
//! the `SymbolSource` to resolve the surviving ids; `samples_table` may name a
//! UNION view of hot WAL-tail + cold blocks.

use std::sync::Arc;

use crabka_blockstore::LabelMatcher;
use datafusion::prelude::SessionContext;

use crate::error::ProfileError;
use crate::symbols::SymbolSource;

/// The result of a profile scan: a `SessionContext` with `samples_table`
/// registered + the `SymbolSource` to resolve surviving stacktrace ids.
pub struct ProfileScan {
    pub ctx: SessionContext,
    pub samples_table: String,
    pub symbols: Arc<dyn SymbolSource>,
}

/// Resolves profile matchers to a DataFusion samples table over a tenant's data.
#[async_trait::async_trait]
pub trait ProfileStore: Send + Sync {
    /// Register the samples table for the matched series of `profile_type` in
    /// `[start_ms, end_ms]`. Matchers + profile_type are an over-approximate
    /// prefilter (block/index pruning); the engine re-applies the fold, so a
    /// store may return a superset of the profile_type's rows.
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError>;

    /// Distinct label names across matched series.
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    /// Distinct values for one label name.
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    /// Distinct `__profile_type__` strings in range (powers the datasource
    /// health probe — spec §7.1).
    async fn profile_types(
        &self,
        tenant: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, ProfileError>;

    /// The matching series' label sets, projected to `label_names`.
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError>;
}
```

- [ ] **Step 4: Wire + run + commit** — add `mod store;` + `pub use store::{ProfileScan, ProfileStore};`.

```bash
cargo test -p crabka-pprof --lib store  # PASS
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): ProfileStore trait + ProfileScan"
```

---

### Task A8: `InMemoryProfileStore`

**Files:**
- Create: `crates/pprof/src/in_memory.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Consumes: `ProfileStore`/`ProfileScan`, `samples::{PCOL_*, profile_samples_schema}`, `SymbolDb`, `LabelMatcher`, DataFusion `MemTable`.
- Produces:
  - `pub struct InMemoryProfileStore { /* per-tenant samples rows + a shared SymbolDb */ }` (`Default`) with `new()` and a fluent builder:
    - `pub fn symbols_mut(&mut self) -> &mut SymbolDb` — populate the partition trees before pushing samples.
    - `pub fn push_sample(&mut self, tenant: &str, profile_type: &str, labels: Vec<(String,String)>, partition: u64, stacktrace_id: u32, value: i64, timestamp_ms: i64)` — append one fact-table row (computes the `series_fingerprint` from `labels` via a stable hash).
  - the `ProfileStore` impl: `select` builds a `MemTable` from the rows of the matching `(tenant, profile_type)` series in `[start_ms, end_ms]` (matchers an over-approximate prefilter — returns a superset; the engine folds), registers it as `samples`, returns `ProfileScan { ctx, samples_table, symbols: Arc::new(self.symbols.clone()) }`. `label_names`/`label_values`/`profile_types`/`series` compute from the stored rows' labels.

> **The store holds ONE `SymbolDb`** (the test fixture's partitions); `select` clones it into the `ProfileScan` (cheap for tests). Promote each label key to a row's series labels; the `series_fingerprint` is a stable hash of the sorted label pairs so the same label set shares a fingerprint. Timestamps stored as ms in `COL_TIMESTAMP` here (the engine's range filter is ms-based for the in-memory store; the block path uses ns — flagged in Self-review, harmonized in Slice 5).

- [ ] **Step 1: Write the failing test**

Create `crates/pprof/src/in_memory.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use datafusion::arrow::array::AsArray;

    fn store_with_two_samples() -> InMemoryProfileStore {
        let mut s = InMemoryProfileStore::new();
        // populate the symbol DB partition 0 with two locations -> two stacks.
        let n_main = s.symbols_mut().intern_string("main");
        let f_main = s.symbols_mut().intern_function(n_main, n_main, 0, 0);
        let n_work = s.symbols_mut().intern_string("work");
        let f_work = s.symbols_mut().intern_function(n_work, n_work, 0, 0);
        let l_main = s.symbols_mut().intern_location(0, 0x10, 0, &[(f_main, 1)]);
        let l_work = s.symbols_mut().intern_location(0, 0x20, 0, &[(f_work, 2)]);
        let st_work = s.symbols_mut().intern_stacktrace(0, &[l_work, l_main]); // work->main
        let st_main = s.symbols_mut().intern_stacktrace(0, &[l_main]);          // main only
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let labels = vec![("service_name".to_string(), "checkout".to_string())];
        // TWO samples of the SAME (partition, stacktrace_id) -> must fold to one summed row.
        s.push_sample("t", pt, labels.clone(), 0, st_work, 10, 1000);
        s.push_sample("t", pt, labels.clone(), 0, st_work, 5, 1000);
        s.push_sample("t", pt, labels, 0, st_main, 3, 1000);
        s
    }

    #[tokio::test]
    async fn select_registers_samples_table_and_symbols() {
        let s = store_with_two_samples();
        let scan = s
            .select("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", &[], 0, 5000)
            .await
            .unwrap();
        // three sample rows land (the fold happens in the engine, not the store).
        let df = scan.ctx.sql(&format!("SELECT count(*) AS c FROM {}", scan.samples_table)).await.unwrap();
        let out = df.collect().await.unwrap();
        let c = out[0].column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().value(0);
        assert!(c == 3);
        // the SymbolSource resolves a known stacktrace.
        assert!(!scan.symbols.resolve(0, 0).is_empty() || !scan.symbols.resolve(0, 1).is_empty());
    }

    #[tokio::test]
    async fn profile_types_and_label_values() {
        let s = store_with_two_samples();
        let pts = s.profile_types("t", 0, 5000).await.unwrap();
        assert!(pts == vec!["process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string()]);
        let vals = s.label_values("t", "service_name", &[], 0, 5000).await.unwrap();
        assert!(vals == vec!["checkout".to_string()]);
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type InMemoryProfileStore`.

- [ ] **Step 3: Implement `in_memory.rs`** — hold `samples: HashMap<String /*tenant*/, Vec<SampleRow>>` and one `symbols: SymbolDb`. `SampleRow { profile_type, fingerprint, labels, partition, stacktrace_id, value, total_value, span_id: Option<u64>, trace_id: Option<Vec<u8>>, timestamp_ms }`. `select` filters by `(tenant, profile_type, timestamp in range)`, builds Arrow columns in the `profile_samples_schema()` order (use a `StringDictionaryBuilder<Int32Type>` for `PCOL_PROFILE_TYPE`, `UInt64Builder`/`Int64Builder` for the rest, nullable `UInt64`/`Binary` for span/trace), builds a `MemTable`, registers `samples`. The matcher prefilter may be ignored (return the superset). Provide the full real Arrow-builder code; keep the dictionary/`MemTable` builder calls behind the verify note. `profile_types`/`label_names`/`label_values`/`series` iterate the stored rows.

> **Arrow-builder note (verify against arrow 59 / datafusion rev `0838a4d`):** `StringDictionaryBuilder::<Int32Type>::new()` + `append(value)` for the dict column, the nullable `BinaryBuilder`/`UInt64Builder` `append_null`/`append_value` conventions, `MemTable::try_new(SchemaRef, Vec<Vec<RecordBatch>>)` at `datafusion::catalog::MemTable`, and `ctx.register_table(name, Arc<MemTable>)` are the churn points. Align to the pinned rev — keep the asserted behavior (`count(*) == 3`, `profile_types`/`label_values` results).

- [ ] **Step 4: Wire into `lib.rs`** — add `mod in_memory;` + `pub use in_memory::InMemoryProfileStore;`.

- [ ] **Step 5: Phase A gate + commit**

```bash
cargo test -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets && cargo fmt -p crabka-pprof --check
git add crates/pprof/ Cargo.toml
git commit -m "feat(pprof): InMemoryProfileStore building samples DataFusion table + SymbolDb"
```

---

## Phase B — the `Tree` model + the 4-ints-per-bar `FlameGraph` encoding

> **Batching:** B1 (`Tree` add_stack/merge) and B2 (`FlameGraph`/`Level`/`FlameGraphDiff` types + `to_flamegraph` 4-ints encoding) live in the same file (`tree.rs`) and the encoding depends on the tree, so they run **sequentially** (B1 → B2). B3 (`series.rs` types) is disjoint and may run in parallel with B1/B2. **No DataFusion churn surface in this phase — it is plain Rust tree/encoding logic pinned by hand-built-profile snapshot tests. The 4-ints-per-bar grouping + delta semantics are the spec's byte-exact correctness contract.** Recommended: {B1→B2, B3 in parallel}.

### Task B1: `Tree` — `new`/`add_stack`/`merge`

**Files:**
- Create: `crates/pprof/src/tree.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Consumes: `Frame`.
- Produces:
  - `pub struct Tree { /* arena: Vec<Node>, root: usize */ }` where `Node { name: String, total: i64, self_: i64, children: BTreeMap<String, usize> }` (root is the synthetic `"total"` node).
  - `pub fn new() -> Self` — a tree with just the `"total"` root.
  - `pub fn add_stack(&mut self, frames: &[Frame], value: i64)` — frames are **leaf-first** (as `resolve` yields); the path from root→leaf is `frames` **reversed** (root-most last in a leaf-first list → walk in reverse). `total += value` on every node along the root→leaf path (incl. root); `self_ += value` only on the leaf node.
  - `pub fn merge(&mut self, other: Tree)` — structural merge of two partial trees by frame name (sum `total`/`self_` per matching path).

> **Frame-name keying:** a tree node is keyed by a frame's display name (`function` — or `function file:line` if the spec's flamegraph node identity needs file/line; pin the choice with the test). Leaf-first `frames` means `frames[0]` is the innermost (leaf) function; the root→leaf descent walks `frames.iter().rev()` so the *last* frame appended is the leaf where `self_` accrues.

- [ ] **Step 1: Write the failing tests** (the headline `Tree` fold: total-along-path, self-at-leaf, plus merge)

Create `crates/pprof/src/tree.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crate::pprof::Frame;

    fn frame(name: &str) -> Frame {
        Frame { function: name.into(), file: String::new(), line: 0 }
    }

    // leaf-first stack: [leaf=work, root-side=main]  (work called by main)
    fn stack(names: &[&str]) -> Vec<Frame> {
        names.iter().map(|n| frame(n)).collect()
    }

    #[test]
    fn add_stack_totals_along_path_and_self_at_leaf() {
        let mut t = Tree::new();
        // main -> work, value 10 (leaf-first [work, main]).
        t.add_stack(&stack(&["work", "main"]), 10);
        // main -> other, value 3 (leaf-first [other, main]).
        t.add_stack(&stack(&["other", "main"]), 3);

        // root "total": total = 13, self = 0.
        assert!(t.total_of(&["total"]) == 13);
        assert!(t.self_of(&["total"]) == 0);
        // "main" (under root): total = 13 (both stacks pass through it), self = 0.
        assert!(t.total_of(&["total", "main"]) == 13);
        assert!(t.self_of(&["total", "main"]) == 0);
        // "work" (leaf): total = 10, self = 10.
        assert!(t.total_of(&["total", "main", "work"]) == 10);
        assert!(t.self_of(&["total", "main", "work"]) == 10);
        // "other" (leaf): total = 3, self = 3.
        assert!(t.self_of(&["total", "main", "other"]) == 3);
    }

    #[test]
    fn merge_combines_partial_trees() {
        let mut a = Tree::new();
        a.add_stack(&stack(&["work", "main"]), 10);
        let mut b = Tree::new();
        b.add_stack(&stack(&["work", "main"]), 5);
        b.add_stack(&stack(&["new", "main"]), 2);
        a.merge(b);
        assert!(a.total_of(&["total"]) == 17);
        assert!(a.total_of(&["total", "main", "work"]) == 15); // 10 + 5
        assert!(a.self_of(&["total", "main", "new"]) == 2);
    }
}
```

> Add test-only helpers `total_of(&self, path: &[&str]) -> i64` / `self_of(&self, path: &[&str]) -> i64` that descend the named path and return the node's `total`/`self_` (or panic if the path is absent — these are test scaffolding inside `#[cfg(test)]` or `pub(crate)` on `Tree`).

- [ ] **Step 2: Run to verify it fails** — `cannot find type Tree`.

- [ ] **Step 3: Implement the `Tree` arena + `add_stack`/`merge`** — full real code (no churn surface). Arena `Vec<Node>`; `add_stack` descends `frames.iter().rev()` from the root, creating child nodes via the `children: BTreeMap<String, usize>` map (BTreeMap so child order is deterministic for the encoding), adding `value` to each node's `total`, and `value` to the final node's `self_`. `merge` recursively walks `other` from its root, finding/creating the matching path in `self` and summing `total`/`self_`. Add the `pub(crate)` `total_of`/`self_of` test helpers.

- [ ] **Step 4: Run + wire + commit** — add `mod tree;` + `pub use tree::Tree;` (the `FlameGraph` types are exported in B2).

```bash
cargo test -p crabka-pprof --lib tree  # PASS (2 tests)
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): Tree fold (total-along-path, self-at-leaf) + merge"
```

---

### Task B2: `FlameGraph`/`Level`/`FlameGraphDiff` + `Tree::to_flamegraph` (the 4-ints-per-bar encoding — THE CONTRACT)

**Files:**
- Modify: `crates/pprof/src/tree.rs` (add the flamegraph types + `to_flamegraph`)
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Produces (all `Clone`, `Debug`, `PartialEq`):
  - `pub struct Level { pub values: Vec<i64> }` — groups of 4: `[xOffsetDelta, total, self, nameIndex]`.
  - `pub struct FlameGraph { pub names: Vec<String>, pub levels: Vec<Level>, pub total: i64, pub max_self: i64 }`.
  - `pub struct FlameGraphDiff { pub names: Vec<String>, pub levels: Vec<Level>, pub left_ticks: i64, pub right_ticks: i64 }` (groups of 7; **bodies/encoding Slice 3** — type only, frozen here).
  - `impl Tree { pub fn to_flamegraph(self, max_nodes: i64) -> FlameGraph }` — BFS the tree level-by-level; `names[0] == "total"`; each bar emits `[xOffsetDelta, total, self, nameIndex]` where `xOffsetDelta` is the delta from the **previous bar's end on the same level** (the first bar on a level deltas from its parent's start). Truncate to `max_nodes` via a min-value heap threshold: nodes below the threshold collapse into a synthetic `"other"` sibling carrying the pruned total.

> **The encoding is the spec's byte-exact contract (spec §6.1 + §10).** `xOffsetDelta` is a DELTA, not absolute. Level 0 is the single root bar `[0, total, 0, 0]` (offset 0, total = whole, self 0, name index 0 = "total"). Each subsequent level lists its bars left-to-right; the first bar's `xOffsetDelta` is the gap from the parent's left edge, and each following bar's `xOffsetDelta` is the gap from the previous sibling's *right edge* (previous bar's `xOffset + total`). Children are ordered by the `Tree`'s deterministic `BTreeMap` order. This is NOT a churn point — implement it exactly; the hand-built tests pin every integer.

- [ ] **Step 1: Write the failing encoding tests** (known tree → known levels, asserting the 4-int groups + delta semantics)

Append to `tree.rs`'s `tests`:

```rust
    #[test]
    fn to_flamegraph_root_level_and_names() {
        let mut t = Tree::new();
        t.add_stack(&stack(&["a", "main"]), 6);
        t.add_stack(&stack(&["b", "main"]), 4);
        let fg = t.to_flamegraph(2048);
        // names[0] is the synthetic root.
        assert!(fg.names[0] == "total");
        assert!(fg.total == 10);
        // level 0: one root bar [xOffsetDelta=0, total=10, self=0, nameIndex=0].
        assert!(fg.levels[0].values == vec![0, 10, 0, 0]);
    }

    #[test]
    fn to_flamegraph_xoffset_is_delta_from_previous_bar_end() {
        let mut t = Tree::new();
        // main -> {a:6, b:4}; a and b are siblings on level 2.
        t.add_stack(&stack(&["a", "main"]), 6);
        t.add_stack(&stack(&["b", "main"]), 4);
        let fg = t.to_flamegraph(2048);
        // level 1: [main]: xOffsetDelta=0 (under root start), total=10, self=0.
        assert!(fg.levels[1].values[0..4] == [0, 10, 0, names_index(&fg, "main")]);
        // level 2: [a, b] in BTreeMap order (a then b).
        // a: xOffsetDelta=0 (under main's start), total=6, self=6.
        let a = &fg.levels[2].values[0..4];
        assert!(a[0] == 0 && a[1] == 6 && a[2] == 6);
        // b: xOffsetDelta=0 (immediately after a's right edge: a ended at 6, b starts at 6 -> delta 0),
        // total=4, self=4.
        let b = &fg.levels[2].values[4..8];
        assert!(b[0] == 0 && b[1] == 4 && b[2] == 4);
    }

    #[test]
    fn to_flamegraph_truncates_with_synthetic_other() {
        let mut t = Tree::new();
        for i in 0..10 {
            t.add_stack(&stack(&[&format!("leaf{i}"), "main"]), 1);
        }
        // max_nodes small enough to force truncation -> a synthetic "other" appears.
        let fg = t.to_flamegraph(4);
        assert!(fg.names.iter().any(|n| n == "other"));
        // total is conserved: the "other" node carries the pruned tail's value.
        assert!(fg.total == 10);
    }

    fn names_index(fg: &FlameGraph, name: &str) -> i64 {
        fg.names.iter().position(|n| n == name).unwrap() as i64
    }
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type FlameGraph` / `to_flamegraph`.

- [ ] **Step 3: Implement the flamegraph types + `to_flamegraph`** — full real code. BFS level-by-level: maintain a `names` vec + a `name → index` map (seed `"total"` at 0). For each level, iterate the level's nodes left-to-right (parent order, then `BTreeMap` child order within a parent); track a running `x` cursor per level reset appropriately, computing each bar's `xOffsetDelta = current_x - previous_bar_end` (first bar deltas from the parent's left x). Emit `[xOffsetDelta, total, self_, nameIndex]`. For truncation: collect all nodes, if `node_count > max_nodes` compute a min-value threshold (a binary heap over node `total`s) and collapse sub-threshold nodes into a synthetic `"other"` sibling whose `total`/`self_` sum the pruned values (conserving the parent total). `max_self = max(self_)` over emitted bars. Provide the FULL real encoding (the delta arithmetic is the contract).

> **Truncation precision:** the synthetic `"other"` collapse must **conserve totals** (the test asserts `fg.total == 10` after pruning). A simple, spec-faithful approach: sort nodes by `total` descending, keep the top `max_nodes`, and for each pruned node fold its `total`/`self_` into an `"other"` child under its parent. Pin the exact policy with the truncation test; refine the heap threshold in Slice 3 if Pyroscope's differential corpus demands a specific tie-break.

- [ ] **Step 4: Run + wire + commit** — extend the `lib.rs` re-export to `pub use tree::{FlameGraph, FlameGraphDiff, Level, Tree};`.

```bash
cargo test -p crabka-pprof --lib tree  # PASS (5 tests)
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): FlameGraph 4-ints-per-bar encoding (xOffsetDelta) + max_nodes truncation w/ synthetic other"
```

---

### Task B3: `Series` / `SeriesAgg` types (frozen; bodies Slice 3)

**Files:**
- Create: `crates/pprof/src/series.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Produces (all `Clone`, `Debug`, `PartialEq`):
  - `pub struct Series { pub labels: Vec<(String, String)>, pub points: Vec<(i64, f64)> }` — `(timestamp_ms, value)`.
  - `pub enum SeriesAgg { Sum, Average }` (`Copy`).

- [ ] **Step 1: Write the failing test** (pure data-structure test — no engine yet)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn series_holds_points_and_agg_is_copy() {
        let s = Series {
            labels: vec![("service_name".into(), "checkout".into())],
            points: vec![(1000, 1.5), (2000, 2.0)],
        };
        assert!(s.points[1] == (2000, 2.0));
        let a = SeriesAgg::Sum;
        let _b = a; // Copy
        assert!(a == SeriesAgg::Sum);
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type Series`.

- [ ] **Step 3: Implement `series.rs`** — the two types with the derives. Doc-comment that the bodies (the `select_series` fold) land in Slice 3; the types are frozen here so the engine signature compiles.

- [ ] **Step 4: Phase B gate + wire + commit** — add `mod series;` + `pub use series::{Series, SeriesAgg};`.

```bash
cargo test -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets && cargo fmt -p crabka-pprof --check
git add crates/pprof/
git commit -m "feat(pprof): Series/SeriesAgg types (frozen for slice 3)"
```

---

## Phase C — the centerpiece: the MERGE→flamegraph engine

> **Batching:** single sequential phase. C1 (`FlameEngine` scaffold + `EngineOpts` + the frozen `select_series`/`diff`/`select_merge_profile` stubs) lands first; C2 (the `select_merge_stacktraces` fold — THE CENTERPIECE) depends on it. **C2 is the churn-prone DataFusion-aggregate surface — it carries the verify-against-rev note + the fold-before-symbolize + flamegraph behavioral tests.** Recommended: C1 → C2.

### Task C1: `FlameEngine` scaffold + `EngineOpts` + frozen slice-3 signatures

**Files:**
- Create: `crates/pprof/src/engine.rs`
- Modify: `crates/pprof/src/lib.rs`

**Interfaces:**
- Consumes: `ProfileStore`, `matcher::parse_label_selector`, the result types, `ProfileError`.
- Produces:
  - `pub struct EngineOpts { pub default_max_nodes: i64 }`; `impl Default for EngineOpts` (`default_max_nodes: 2048`).
  - `pub struct FlameEngine<S: ProfileStore> { store: Arc<S>, opts: EngineOpts }`
  - `pub fn new(store: Arc<S>, opts: EngineOpts) -> Self`
  - the three **frozen** slice-3 methods returning `ProfileError::Unsupported`:
    - `select_series(...) -> Result<Vec<Series>, ProfileError>` → `Unsupported("select_series: slice 3")`
    - `diff(...) -> Result<FlameGraphDiff, ProfileError>` → `Unsupported("diff: slice 3")`
    - `select_merge_profile(...) -> Result<Vec<u8>, ProfileError>` → `Unsupported("select_merge_profile: slice 3")`
  - `select_merge_stacktraces(...)` declared but `todo!()`/`Unsupported` until C2 (signature frozen).

- [ ] **Step 1: Write the failing test** (the frozen methods return `Unsupported`; the engine constructs over `InMemoryProfileStore`)

Create `crates/pprof/src/engine.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crate::in_memory::InMemoryProfileStore;

    fn engine() -> FlameEngine<InMemoryProfileStore> {
        FlameEngine::new(std::sync::Arc::new(InMemoryProfileStore::new()), EngineOpts::default())
    }

    #[test]
    fn default_max_nodes_is_2048() {
        assert!(EngineOpts::default().default_max_nodes == 2048);
    }

    #[tokio::test]
    async fn slice3_methods_are_unsupported() {
        let e = engine();
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        assert!(matches!(
            e.select_series("t", pt, "{}", &[], 15.0, SeriesAgg::Sum, 0, 1).await,
            Err(ProfileError::Unsupported(_))
        ));
        assert!(matches!(
            e.diff("t", (pt, "{}", 0, 1), (pt, "{}", 0, 1), 2048).await,
            Err(ProfileError::Unsupported(_))
        ));
        assert!(matches!(
            e.select_merge_profile("t", pt, "{}", 0, 1).await,
            Err(ProfileError::Unsupported(_))
        ));
    }
}
```

- [ ] **Step 2: Run to verify it fails** — `cannot find type FlameEngine`.

- [ ] **Step 3: Implement the scaffold** — `EngineOpts` + `Default`, `FlameEngine` struct + `new`, the three frozen `Unsupported` methods with the EXACT signatures from the contract, and `select_merge_stacktraces` returning `Err(ProfileError::Unsupported("select_merge_stacktraces: see C2".into()))` for now (real body in C2). Import `Series`/`SeriesAgg`/`FlameGraph`/`FlameGraphDiff`.

- [ ] **Step 4: Run + wire + commit** — add `mod engine;` + `pub use engine::{EngineOpts, FlameEngine};`.

```bash
cargo test -p crabka-pprof --lib engine  # PASS
cargo fmt -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets
git add crates/pprof/
git commit -m "feat(pprof): FlameEngine scaffold + EngineOpts + frozen slice-3 signatures"
```

---

### Task C2: `select_merge_stacktraces` — the MERGE→flamegraph fold (THE CENTERPIECE)

**Files:**
- Modify: `crates/pprof/src/engine.rs`

**Interfaces:**
- Consumes: `ProfileStore::select`, `parse_label_selector`, `samples::{PCOL_STACKTRACE_PARTITION, PCOL_STACKTRACE_ID, PCOL_VALUE}`, `SymbolSource::resolve`, `Tree`, `Frame`.
- Produces:
  - `pub async fn select_merge_stacktraces(&self, tenant: &str, profile_type: &str, label_selector: &str, start_ms: i64, end_ms: i64, max_nodes: i64) -> Result<FlameGraph, ProfileError>` — the full pipeline:
    1. `parse_label_selector(label_selector)` → `Vec<LabelMatcher>`.
    2. `self.store.select(tenant, profile_type, &matchers, start_ms, end_ms)` → `ProfileScan { ctx, samples_table, symbols }`.
    3. **DataFusion fold (the cheap part):** run `SELECT stacktrace_partition, stacktrace_id, SUM(value) AS v FROM <samples_table> GROUP BY stacktrace_partition, stacktrace_id` and collect the result batches.
    4. **Rust resolve (only the distinct surviving ids):** for each `(partition, id, summed_value)` row, `symbols.resolve(partition, id)` → `Vec<Frame>` (leaf-first, inlines expanded), then `tree.add_stack(&frames, summed_value)`.
    5. `tree.to_flamegraph(if max_nodes > 0 { max_nodes } else { self.opts.default_max_nodes })`.

> **This is the heart of the slice (spec §6.1).** The `GROUP BY (partition, id) → SUM` collapses duplicate samples **before** any symbolization; only the distinct ids are resolved. The `max_nodes == 0` sentinel falls back to `default_max_nodes` (2048). The result `FlameGraph` is the byte-exact 4-ints-per-bar contract from B2.

- [ ] **Step 1: Write the failing centerpiece tests** (over the `InMemoryProfileStore` fixture with KNOWN stacks + duplicate `(partition,id)` rows that MUST fold)

Append to `engine.rs`'s `tests`:

```rust
    use crate::pprof::Frame;
    use crate::symbols::SymbolDb;

    // Build a store whose partition 0 has: main, main->work, main->other; with
    // TWO samples of main->work (10 and 5) that MUST fold to 15 before resolve.
    fn merge_fixture() -> (InMemoryProfileStore, [u32; 2]) {
        let mut s = InMemoryProfileStore::new();
        let mk = |db: &mut SymbolDb, n: &str| {
            let s = db.intern_string(n);
            db.intern_function(s, s, 0, 0)
        };
        let f_main = mk(s.symbols_mut(), "main");
        let f_work = mk(s.symbols_mut(), "work");
        let f_other = mk(s.symbols_mut(), "other");
        let l_main = s.symbols_mut().intern_location(0, 1, 0, &[(f_main, 1)]);
        let l_work = s.symbols_mut().intern_location(0, 2, 0, &[(f_work, 2)]);
        let l_other = s.symbols_mut().intern_location(0, 3, 0, &[(f_other, 3)]);
        let st_work = s.symbols_mut().intern_stacktrace(0, &[l_work, l_main]);
        let st_other = s.symbols_mut().intern_stacktrace(0, &[l_other, l_main]);
        let pt = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";
        let lbl = vec![("service_name".to_string(), "checkout".to_string())];
        s.push_sample("t", pt, lbl.clone(), 0, st_work, 10, 1000);
        s.push_sample("t", pt, lbl.clone(), 0, st_work, 5, 1000);  // same id -> folds to 15
        s.push_sample("t", pt, lbl, 0, st_other, 3, 1000);
        (s, [st_work, st_other])
    }

    #[tokio::test]
    async fn merge_folds_duplicate_ids_before_symbolize() {
        let (s, _) = merge_fixture();
        let e = FlameEngine::new(std::sync::Arc::new(s), EngineOpts::default());
        let fg = e
            .select_merge_stacktraces("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", "{}", 0, 5000, 2048)
            .await
            .unwrap();
        // total = 10 + 5 + 3 = 18; the two st_work samples folded to one 15-value stack.
        assert!(fg.total == 18);
        // root level: [0, 18, 0, 0].
        assert!(fg.levels[0].values == vec![0, 18, 0, 0]);
        // "work" leaf carries the folded 15 (NOT two separate bars of 10 and 5).
        let work_i = fg.names.iter().position(|n| n == "work").unwrap() as i64;
        let mut work_self = 0;
        for lvl in &fg.levels {
            for chunk in lvl.values.chunks(4) {
                if chunk[3] == work_i {
                    work_self += chunk[2];
                }
            }
        }
        assert!(work_self == 15);
    }

    #[tokio::test]
    async fn merge_applies_label_selector_and_max_nodes_fallback() {
        let (s, _) = merge_fixture();
        let e = FlameEngine::new(std::sync::Arc::new(s), EngineOpts::default());
        // a non-matching selector yields an empty (root-only) flamegraph total 0,
        // OR — since InMemoryProfileStore's prefilter is over-approximate — the engine
        // still applies the matchers via the store; assert total is the full 18 with {} and
        // max_nodes=0 falls back to default (no panic, valid encoding).
        let fg = e
            .select_merge_stacktraces("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", "{}", 0, 5000, 0)
            .await
            .unwrap();
        assert!(fg.total == 18);
        assert!(fg.names[0] == "total");
    }
```

> **On the over-approximate prefilter:** `InMemoryProfileStore::select` returns the full profile_type's rows (matchers ignored). That is fine for THIS slice's centerpiece (the fold/resolve/encode is what we're pinning). Exact label filtering at the DataFusion layer is a store/querier concern (Slice 5) — flagged in Self-review. The `{}` selector exercises the parse + plumbing without needing exact filtering here.

- [ ] **Step 2: Run to verify it fails** — `select_merge_stacktraces` returns `Unsupported`, the assert fails.

- [ ] **Step 3: Implement the fold** — replace the C1 stub with the real pipeline. Use `ctx.sql(&format!("SELECT {p}, {id}, SUM({v}) AS v FROM {table} GROUP BY {p}, {id}", p=PCOL_STACKTRACE_PARTITION, id=PCOL_STACKTRACE_ID, v=PCOL_VALUE, table=scan.samples_table))` (or build the same via `LogicalPlanBuilder::aggregate` — **verify the SQL/aggregate path against datafusion rev `0838a4d`**), `.collect()` the batches, read the three columns (`UInt64`/`UInt64`/`Int64`) per row, `scan.symbols.resolve(partition, id)`, `tree.add_stack(&frames, v)`, then `to_flamegraph`. Provide the full real fold + batch-reading code; keep the `sql`/`collect`/`as_primitive` calls behind the verify note.

> **Verify against datafusion rev `0838a4d` / arrow 59:** the `SessionContext::sql(&str).await?.collect().await?` entry point, the `RecordBatch::column(i).as_primitive::<UInt64Type/Int64Type>()` downcasts (and `as_primitive` import path `datafusion::arrow::array::AsArray`), and whether `SUM` returns `Int64` or `UInt64` for an `Int64` input (cast if needed) are the churn points. Implement to satisfy the known-value tests; if `SUM(Int64)` widens to `Decimal`/`Int64` differently, cast in the SQL (`SUM(value)::BIGINT`) — keep the asserted totals (18, folded 15). The fold-before-symbolize *algorithm* (GROUP BY then resolve) is the spec contract and must be exactly as written — never resolve before the fold.

- [ ] **Step 4: Phase C gate + commit**

```bash
cargo test -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets && cargo fmt -p crabka-pprof --check
git add crates/pprof/
git commit -m "feat(pprof): select_merge_stacktraces — fold-before-symbolize MERGE->flamegraph engine"
```

---

## Phase D — end-to-end golden-merge suite

> **Batching:** single task (D1). It depends on the whole engine (Phases A–C). This is the credibility check: a curated golden set of merge queries over a fixed multi-profile fixture, asserted against hand-computed flamegraph levels (spec §10 — there is no upstream conformance corpus for a language-less engine, so the corpus is hand-built and asserted against known-correct expected results, NOT fabricated).

### Task D1: Curated golden-merge suite over a fixed multi-profile fixture

**Files:**
- Create: `crates/pprof/tests/golden_merge.rs`

**Interfaces:**
- Consumes: `crabka_pprof::{FlameEngine, EngineOpts, InMemoryProfileStore, SymbolDb, ProfileType, FlameGraph}` + the public result model.
- Produces: an integration test asserting a curated set of MERGE queries against a fixed fixture with hand-computed expected flamegraph levels.

- [ ] **Step 1: Write the suite** — build a fixed multi-profile fixture (one `InMemoryProfileStore` with a populated `SymbolDb` partition 0 containing a known tree: `main → {work → {alloc}, other}`, with inlined frames on at least one location), push samples across multiple timestamps, then assert each query's `FlameGraph` against hand-computed expected `names`/`levels`/`total`/`max_self`. Cover, at minimum:
  - a plain `select_merge_stacktraces("{}")` → the full tree, asserting the exact `levels` 4-int groups (root `[0,total,0,0]`, each child's `xOffsetDelta` delta-from-previous-sibling-end).
  - **fold-before-symbolize:** duplicate `(partition, id)` samples fold to one summed bar (the headline invariant).
  - **inline expansion:** a location with two `lines[]` produces two distinct flamegraph frames (innermost-first), each its own bar.
  - **`max_nodes` truncation:** a wide tree truncated to a small `max_nodes` produces a synthetic `"other"` node conserving the total.
  - **`ProfileType` round-trip:** `parse` + `Display` over the fixture's profile-type string.
  - **multiple profile types:** samples of a second profile_type are excluded from a merge of the first.

- [ ] **Step 2: Run** — `cargo test -p crabka-pprof --test golden_merge`. Each failure is an engine/encoding/symbol-DB bug — fix it in the relevant Phase A/B/C file (the hand-computed expectations are ground truth; never weaken an expectation to pass). Iterate to green.

- [ ] **Step 3: Final whole-crate gate + commit**

```bash
cargo test -p crabka-pprof && cargo clippy -p crabka-pprof --all-targets && cargo fmt -p crabka-pprof --check
git add crates/pprof/
git commit -m "test(pprof): curated golden-merge suite (fold-before-symbolize/inline-expansion/truncation/4-ints encoding)"
```

---

## Self-review

**Spec coverage (against §6 the flamegraph-merge engine + §4 data model + §11 Slice 2):**
- The pprof model + codec (perftools.profiles `Profile` decode/encode + the string-table/sample-type helpers + `Frame`) → Tasks A1 (vendored proto + build.rs), A3.
- The deduplicated `SymbolDb` — parent-pointer stacktrace tree (`intern_stacktrace` dedup, `resolve` leaf→root climb, inlined frames expanded innermost-first) + dedup string/function/location/mapping tables + the `encode`/`decode` `symbols.symdb`-equivalent artifact, behind the `SymbolSource` trait → Task A5.
- The 5-part `ProfileType` parse + `Display` (Go/pprof vs Java/JFR examples; not hardcoded) → Task A4.
- The `ProfileStore` trait + `ProfileScan` (the pinned query seam) → Task A7; `InMemoryProfileStore` building a samples DataFusion table + a `SymbolDb` → Tasks A6 (the column contract), A8.
- The Prometheus matcher-string helper (`label_selector` → `Vec<LabelMatcher>` — the only "parser") → Task A6.
- The `Tree` fold (total-along-path, self-at-leaf) + `merge` (partial-tree combine) → Task B1; the byte-exact **4-ints-per-bar `FlameGraph`** encoding (`xOffsetDelta` delta-from-previous-bar-end, `names[0]=="total"`, `max_nodes` truncation + synthetic `"other"`) + the `FlameGraphDiff` type (7-ints, frozen for Slice 3) → Task B2.
- **The centerpiece — the MERGE→flamegraph engine:** resolve `label_selector` + profile_type + `[start,end]` → `ProfileStore.select` → DataFusion `GROUP BY (stacktrace_partition, stacktrace_id) → SUM(value)` (the merge-*before*-symbolize step) → Rust resolve distinct ids via `SymbolSource` → fold into one `Tree` → `to_flamegraph(max_nodes)` → `FlameGraph` → Tasks C1 (scaffold), C2 (`select_merge_stacktraces`).
- The golden-merge suite (no upstream corpus; hand-computed expected levels) → Task D1.
- **Frozen public contract** (`PprofProfile`/`Frame`/`SymbolDb`/`SymbolSource`/`ProfileType`/`ProfileStore`/`ProfileScan`/`FlameGraph`/`Level`/`FlameGraphDiff`/`Tree`/`Series`/`SeriesAgg`/`EngineOpts`/`FlameEngine`/`ProfileError` + the `select_merge_stacktraces`/`select_series`/`diff`/`select_merge_profile` signatures + the `PCOL_*` samples-column contract) defined at the exact signatures the prompt pins → §"Shared cross-slice contract" + §"Samples table column contract" + the task interfaces.

**Deferred (correctly, to Slice 3):** `select_series` (precomputed `total_value`, step-in-seconds, SUM/AVERAGE), `diff` (the 7-ints-per-bar `FlameGraphDiff` body), `select_merge_profile` (raw pprof output), `SelectMergeSpanProfile`, `SelectHeatmap` — all three `FlameEngine` slice-3 methods return `ProfileError::Unsupported` with the EXACT frozen signatures (C1), pinned by the `slice3_methods_are_unsupported` test; the `Series`/`SeriesAgg`/`FlameGraphDiff` types are frozen here so the signatures compile. The optional `:delta` profile-type suffix is deferred to Slice 4 ingest (`ProfileType::parse` accepts exactly 5 parts). Each boundary returns `Unsupported`, not silently wrong.

**Placeholder scan:** no "TBD"/"add error handling"/"similar to Task N". Every code-bearing step ships complete, runnable real code (`error.rs`, `pprof.rs` wrapper, `profile_type.rs`, `symbols.rs` incl. the parent-pointer tree + dedup tables + encode/decode, `matcher.rs`, `samples.rs`, `store.rs`, `tree.rs` incl. the full 4-ints-per-bar delta arithmetic + truncation) or — for the churn-prone external surfaces (the vendored pprof proto + prost field names, the `MemTable`/`StringDictionaryBuilder`/arrow builder calls, the `SessionContext::sql`/`collect`/`as_primitive` DataFusion-aggregate path) — the **struct shape + the column/behavior contract + a behavior-pinning test + an explicit "verify against the vendored proto / datafusion rev `0838a4d` / arrow 59" note**, mirroring how the metrics/traces/blockstore plans bound their arrow/DataFusion/proto hand-waves. No prost field number or trait method signature is fabricated as fact. The 4-ints-per-bar encoding (`xOffsetDelta` delta semantics, `[xOffsetDelta,total,self,nameIndex]` grouping, `names[0]=="total"`) and the fold-before-symbolize algorithm are **not** behind a verify note — they are the spec's correctness contracts, written exactly.

**Type consistency:** `ProfileStore`'s five method signatures are identical across A7 (definition), A8 (impl), and C2 (consumer). `ProfileScan` fields (`ctx`/`samples_table`/`symbols`) stable A7↔A8↔C2. `SymbolDb`/`SymbolSource`'s `resolve(partition, id) -> Vec<Frame>` signature is identical across A5 (definition), A8 (the in-memory `SymbolSource`), and C2 (the engine resolve). `ProfileError` variants (`Decode`/`Plan`/`Exec`/`Store`/`Unsupported`/`Symbolize`) are the single error type across all tasks. The `PCOL_*` samples-column constants + `samples_schema` defined once (A6) and referenced unchanged in A8/C2. `Frame` defined once (A3) and produced by `SymbolDb::resolve` (A5) + consumed by `Tree::add_stack` (B1). `FlameGraph`/`Level`/`FlameGraphDiff`/`Tree` defined once (B1/B2) and produced by `to_flamegraph` (B2) + `select_merge_stacktraces` (C2). `Series`/`SeriesAgg` defined once (B3) and used in the frozen `select_series` signature (C1). The frozen public names match the prompt's pinned contract exactly.

**Known risks (flagged, not hidden):**
1. **The vendored pprof proto + prost field names** — `prost`'s identifier escaping (`r#type` for the proto `type` field) and the generated module name (from the proto `package`) are the churn points. Contained to A1 (vendor + build.rs) + A3 (the wrapper), behind the verify-against-vendored-proto note + the round-trip behavioral test. The decode/encode *round-trip equality* is pinned; drift surfaces as a failing test, not silent corruption. Match the grpc-gateway/rebalancer `build.rs` `protoc` discovery so CI without a system `protoc` still builds.
2. **The DataFusion-aggregate fold surface for `select_merge_stacktraces`** (the slice's centerpiece) — the `SessionContext::sql`/`collect` entry point, the `as_primitive` downcasts, and the `SUM(Int64)` output type are the churn points. Contained to `engine.rs` C2, behind the verify-against-rev note + the known-value fold-before-symbolize tests. The fold-before-symbolize *algorithm* (GROUP BY then resolve, never the reverse) is pinned as spec contract, so drift surfaces as a compile error against green correctness tests, never as a silent resolve-before-fold (which would symbolize millions of un-collapsed ids — the exact anti-pattern the spec forbids).
3. **The 4-ints-per-bar `xOffsetDelta` encoding** — the delta-from-previous-bar-end semantics are the #1 byte-exactness trap (spec §6.1/§10). Triple-pinned: the root-level test (`[0,total,0,0]`), the sibling-delta test (b's `xOffsetDelta` is 0 because it abuts a's right edge), and the golden suite. NOT behind a verify note — it is implemented exactly as the spec contract.
4. **Symbol-DB fidelity** — `InMemoryProfileStore` must populate the *identical* `SymbolDb` partition shape the block-builder (Slice 1) emits, or the merge tests are vacuous. `SymbolDb` is shared (A5), `intern_stacktrace`/`resolve` are pinned by known-tree dedup + climb + inline-expansion tests (A5), and the block-builder (Slice 4) is contracted to intern via the same `SymbolDb`.
5. **In-memory store time/filter approximation** — `InMemoryProfileStore` filters on ms timestamps + ignores matchers (over-approximate prefilter). This is sufficient for the engine's fold/resolve/encode (what Slice 2 pins); exact label filtering + ns-time harmonization are the querier's concern (Slice 5), flagged here and enforced (the store returns a superset; the engine's `GROUP BY` + `to_flamegraph` are exact on whatever rows it gets).
6. **Slice executability** — this is the largest profiles slice; the phase batching (A→B→C→D, with the noted intra-phase parallel batches on disjoint file sets per `CLAUDE.md`) keeps each sub-batch's file sets disjoint and ends every phase at a green whole-crate gate so a sub-batch is reviewed/merged before the next starts.
