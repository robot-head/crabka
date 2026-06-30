# crabka-profiles Slice 5 — Querier + Connect `querier.v1` API + legacy render + hot/cold merge

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the **querier role** — a concrete `crabka-pprof::ProfileStore` impl (`CrabkaProfileStore`) that merges the *hot* WAL-tail (in-memory recent samples + their symbols, as a DataFusion `MemTable` + a `SymbolSource`) with the *cold* profile blocks (blockstore scan via `ProfileIndex` + the block `SymbolDb`), UNION-ed in `select()`; `label_names`/`label_values`/`profile_types`/`series` come from the `ProfileIndex` ∪ the hot tier. On top of that, the **Connect `querier.v1.QuerierService` API** (`connectrpc-axum` + `connectrpc-axum-build` codegen, reusing the grpc-gateway/rebalancer pattern; tenant via `X-Scope-OrgID`; `start`/`end` are **unix MILLIS**) that drives `FlameEngine`, **plus** the legacy HTTP `GET /pyroscope/render` + `/pyroscope/render-diff` flamebearer endpoints the Profiles Drilldown app uses. The `FlameGraph` proto encoding (groups-of-4) and the flamebearer JSON shape (`"single"`/`"double"`) are first-class fidelity tests — the byte-equality analog. Plus the `crabka-profiles --target querier` role binary.

**Architecture:** Three layers, bottom-up.

1. **`CrabkaProfileStore`** (`store.rs`): implements the `ProfileStore` trait. `select()` registers the cold profile-block samples (`BlockStore::scan_context`, restricted by the `ProfileIndex` label/profile-type prune + the time/block prefilter, **and `__profile_type__` = the requested type**) **and** the hot WAL-tail batches into one `SessionContext`, then builds a **UNION ALL view** split at the **block-builder frontier** (the committed block-builder offset surfaced as a per-tenant `min_ns` cut) so a sample sealed into a block is not also counted from the WAL-tail. The returned `ProfileScan.symbols` is a **`CompositeSymbolSource`** that routes `(partition, stacktrace_id)` to the correct block `SymbolDb` or the hot tier's `SymbolDb` by partition — raw ids never cross a block boundary (the load-bearing invariant). `label_names`/`label_values`/`profile_types`/`series` union the per-block `ProfileIndex` postings + profile-type index with the hot tier's live labels.
2. **Connect + legacy HTTP API** (`http/`): a `connectrpc-axum` `QuerierService` builder + an axum `Router` for the legacy render endpoints, tenant via `X-Scope-OrgID`. The Connect methods (`ProfileTypes` — **also the datasource health probe, no separate `/ready`** — `LabelNames`, `LabelValues` whose response field is **`names`**, `Series`, `SelectMergeStacktraces`, `SelectSeries`, `SelectMergeSpanProfile`, `Diff`, `SelectMergeProfile`, `GetProfileStats`) project the `crabka-pprof` engine + `ProfileStore` results into the `querier.v1` proto. `/pyroscope/render` + `/pyroscope/render-diff` project the same engine results into the flamebearer `"single"`/`"double"` JSON. **The `FlameGraph` 4-ints-per-bar proto encoding and the flamebearer JSON shape are the byte-equality analog** and are tested with exact assertions.
3. **Role binary** (`bin/crabka-profiles.rs`): `--target querier` wires the WAL-tail head (the hot tier) + blockstore + frontier into `CrabkaProfileStore`, builds the `FlameEngine`, and serves the Connect API + legacy render router on the configured listen address.

**Tech Stack:** Rust 2024 · `datafusion` (git pin below) · `arrow` 59 · `tokio` · `axum` 0.8 (`http1`,`tokio`) · `connectrpc-axum` + `connectrpc-axum-build` (build.rs codegen) · `prost` 0.14 (the `querier.v1` proto) · `serde`/`serde_json` (flamebearer) · `async-trait` · `thiserror` · `tracing` · `crabka-pprof` (Slices 2–3) · `crabka-blockstore` · `crabka-profiles` Slice 4 (WAL-tail head handle + frontier). Tests: `assert2`, `tower::ServiceExt::oneshot` (in-process router), `object_store::memory::InMemory` (test blockstore), `crabka-broker` in-process test-support (`#[ignore]` e2e).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire formats/role flags freely; no shims, no migration code, no default-off gates. (Only Kafka wire compat matters — this slice consumes the WAL-tail handle and blockstore; it adds no new Kafka surface.) **Pyroscope Connect/legacy wire fidelity is the one external contract this slice owns.**
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean (`module_name_repetitions`/`missing_errors_doc`/`missing_panics_doc` allowed workspace-wide). Run `cargo clippy -p crabka-profiles --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-profiles` before every commit (never `cargo +nightly fmt --all` — OS error 206 / path-too-long in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests.
- **Async tests:** `#[tokio::test]`. Dev-dep `tokio` features `["macros","rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`, `arrow` 59. Same instance as blockstore/pprof — types cross the DataFusion boundary without conversion.
- **Pyroscope wire fidelity is the contract.** Response bodies must match Pyroscope exactly:
  - **`querier.v1` Connect** — `POST /querier.v1.QuerierService/<Method>` with `application/proto`; `start`/`end` are **unix MILLIS** (`int64`). `LabelValues` response field is **`names`** (not `values`). `ProfileTypes` doubles as the datasource health probe — there is **no separate `/ready`**.
  - **`FlameGraph`** — `levels` is a list of `Level { values }`; each level's values are traversed in **groups of 4**: `[xOffsetDelta, total, self, nameIndex]` where `xOffsetDelta` is the delta from the *previous bar's end* (not absolute), `nameIndex` indexes `names[]`, and `names[0]` is the root (`"total"`). This 4-ints-per-bar encoding must match byte-for-byte. The diff form is **groups of 7**: `[xOffLeft, totalLeft, selfLeft, xOffRight, totalRight, selfRight, nameIndex]` + `left_ticks`/`right_ticks`.
  - **Flamebearer JSON** (legacy render) — `{ flamebearer: { names[], levels[][], numTicks, maxSelf }, metadata: { format: "single" (4/bar) | "double" (7/bar), spyName, sampleRate, units, name } }`.
  - **Errors** — `connectrpc-axum`'s `ConnectError` envelope for the Connect surface (`code`/`message`); a plain-text body + status code for the legacy render endpoints. The byte-exact assertions are the `FlameGraph` int sequence and the flamebearer JSON object.
  - The `crabka-pprof` engine already owns the `FlameGraph`/`FlameGraphDiff` encoding (Slices 2–3). This slice's job is the **proto/JSON projection** of those structs — one `flamegraph_to_proto`/`flamegraph_to_flamebearer` helper each, behavior-pinned by tests.

---

## Dependency & slice roadmap

**Depends on:**
- **`crabka-pprof` (Slices 2–3)** — provides `ProfileStore` (trait), `ProfileScan`, `SymbolSource` (trait), `SymbolDb`, `ProfileType`, `FlameEngine<S>`, `EngineOpts`, `FlameGraph`/`Level`/`FlameGraphDiff`, `Tree`, `Series`/`SeriesAgg`, `Frame`, `PprofProfile`, `ProfileError`, and the Prometheus-matcher-string helper. This slice *consumes* that contract verbatim (see "Shared contract" below) and implements `ProfileStore` against it.
- **`crabka-blockstore`** — the generalized `BlockStore` parameterized over `BlockIndex`; `ProfileIndex` (impl `BlockIndex`) with the label-series postings + profile-type index (`__profile_type__` → series) + per-block time-range + stacktrace-partition map; `BlockStore::scan_context`; `Labels`/`LabelMatcher`/`MatchOp`; the profile samples fact-table column constants (`COL_FINGERPRINT`, `COL_TIMESTAMP`, `PCOL_PROFILE_TYPE`, `PCOL_STACKTRACE_ID`, `PCOL_VALUE`, `PCOL_STACKTRACE_PARTITION`, `PCOL_TOTAL_VALUE`, `PCOL_SPAN_ID`, `PCOL_TRACE_ID`) + the samples schema accessor + the symbol-DB on-block artifact (Slice 1).
- **`crabka-profiles` Slice 4 (ingest)** — `ProfileRecord` (the WAL record) + `PROFILES_WAL_TOPIC = "__crabka_profiles_wal"` + the **WAL-tail head handle** (the hot tier: recent samples exposed as DataFusion-ready Arrow batches matching the cold samples schema, plus a hot-tier `SymbolDb`, rebuildable from offsets) + the per-tenant **block-builder frontier** the block-builder commits (consumer-group committed offset / sealed-block `max_ns`).

**The 8 profiles slices** (this plan = Slice 5; each gets its own plan):

1. Blockstore `ProfileIndex` + profile samples schema + symbol-DB artifact.
2. `crabka-pprof` core — pprof model + codec + `SymbolDb` + `ProfileType` + `ProfileStore` trait + engine result types + MERGE→flamegraph.
3. Engine completeness — `SelectSeries`, `Diff` (7-ints-per-bar), `max_nodes` truncation + synthetic `"other"`, `SelectMergeProfile` (→ pprof), `SelectMergeSpanProfile`, `SelectHeatmap`.
4. Ingest service — distributor (`push.v1` + `/ingest` + OTLP) → `(tenant, series_fingerprint)`-partitioned WAL; block-builder → samples fact table + dedup symbol DB + `ProfileIndex`; WAL-tail head (hot tier).
5. **Querier + Connect `querier.v1` API + legacy render + hot/cold merge** *(this plan)*.
6. Query-frontend — query split/shard + the partial-tree merge (`Tree::merge`; raw ids never cross a block boundary) + select-series shard-merge.
7. Native symbolization — query-time `build_id → debuginfod` + DWARF/ELF/`.gopclntab` + lazy resolve behind the `SymbolSource` wrapper.
8. Hardening — per-tenant limits + multi-tenancy isolation, compaction (dedup symbol DBs) + downsampling, differential-vs-Pyroscope + Grafana integration.

---

## Shared contract (consume exactly — do not redefine)

From `crabka-pprof` (Slices 2–3). This slice depends on these signatures unchanged:

```rust
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

// `samples_table` is a UNION view of live WAL-tail (hot) + blocks (cold).
pub struct ProfileScan {
    pub ctx: datafusion::prelude::SessionContext,
    pub samples_table: String,
    pub symbols: std::sync::Arc<dyn SymbolSource>,
}

pub trait SymbolSource: Send + Sync {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame>;
}
pub struct Frame { pub function: String, pub file: String, pub line: i32 }

pub struct FlameEngine<S: ProfileStore> { /* store, opts */ }
pub struct EngineOpts { pub default_max_nodes: i64 /* 2048 */ }
impl<S: ProfileStore> FlameEngine<S> {
    pub fn new(store: std::sync::Arc<S>, opts: EngineOpts) -> Self;
    pub async fn select_merge_stacktraces(&self, tenant: &str, profile_type: &str,
                    label_selector: &str, start_ms: i64, end_ms: i64, max_nodes: i64)
                    -> Result<FlameGraph, ProfileError>;
    pub async fn select_series(&self, tenant: &str, profile_type: &str, label_selector: &str,
                    group_by: &[String], step_secs: f64, agg: SeriesAgg,
                    start_ms: i64, end_ms: i64) -> Result<Vec<Series>, ProfileError>;
    pub async fn diff(&self, tenant: &str, left: (&str, &str, i64, i64),
                    right: (&str, &str, i64, i64), max_nodes: i64)
                    -> Result<FlameGraphDiff, ProfileError>;
    pub async fn select_merge_profile(&self, tenant: &str, profile_type: &str,
                    label_selector: &str, start_ms: i64, end_ms: i64)
                    -> Result<Vec<u8> /* raw pprof */, ProfileError>;
    pub fn store(&self) -> &std::sync::Arc<S>;   // label_names/values/profile_types/series live on ProfileStore
}

pub struct FlameGraph { pub names: Vec<String>, pub levels: Vec<Level>, pub total: i64, pub max_self: i64 }
pub struct Level { pub values: Vec<i64> }       // groups of 4: [xOffsetDelta, total, self, nameIndex]
pub struct FlameGraphDiff { pub names: Vec<String>, pub levels: Vec<Level>,
    pub left_ticks: i64, pub right_ticks: i64 } // levels groups of 7
pub struct Series { pub labels: Vec<(String, String)>, pub points: Vec<(i64, f64)> } // (ts_ms, value)
pub enum SeriesAgg { Sum, Average }
pub enum ProfileError { Decode(String), Plan(String), Exec(String), Store(String),
    Unsupported(String), Symbolize(String) }
// label_selector is a Prometheus matcher STRING parsed to Vec<LabelMatcher> by a crabka-pprof helper.
```

> **Verify-before-use (do not fabricate):** the exact field names / enum discriminants of the result types and the `ProfileStore`/`FlameEngine`/`SymbolSource` method shapes are owned by Slices 2–3. Before Tasks 2–8, run `cargo doc -p crabka-pprof --no-deps` (or read `crates/pprof/src/lib.rs` re-exports) and reconcile. If a name differs (e.g. `start_ms` vs `start_time_ms`, the real `Frame`/`Level` internals, `FlameEngine::store()` presence, `SymbolSource::resolve` arg order), adapt the **mapping code and tests together** — keep the asserted *proto/flamebearer* exact (that is the contract this slice owns); the Rust field names bend to pprof. If `FlameEngine` does not expose `store()`, hold `Arc<CrabkaProfileStore>` in `AppState` alongside the engine and call `label_names`/`label_values`/`profile_types`/`series` directly (prefer this to a cross-crate edit).

**From Slice 4 (the WAL-tail head handle + frontier — verify against Slice 4 before Task 2/3):**

```rust
// Slice 4 exposes a query-side handle to the hot tier. Expected shape:
pub struct HeadHandle { /* Arc to the in-memory recent-samples store */ }
impl HeadHandle {
    // Hot sample rows for a tenant in [start_ms, end_ms], matching the SAME
    // profile samples fact-table schema as cold blocks, as Arrow batches.
    pub async fn sample_batches(&self, tenant: &str, profile_type: &str,
        start_ms: i64, end_ms: i64) -> Result<Vec<arrow::record_batch::RecordBatch>, HeadError>;
    // The hot tier's symbol DB (one SymbolSource for all hot partitions).
    pub fn symbols(&self, tenant: &str) -> std::sync::Arc<dyn crabka_pprof::SymbolSource>;
    // Live labels/profile-types observed in the hot tier.
    pub async fn label_names(&self, tenant: &str) -> Vec<String>;
    pub async fn label_values(&self, tenant: &str, name: &str) -> Vec<String>;
    pub async fn profile_types(&self, tenant: &str) -> Vec<String>;
    pub async fn series(&self, tenant: &str, label_names: &[String]) -> Vec<Vec<(String, String)>>;
    // oldest/newest hot sample ns + whether any data exists (for GetProfileStats).
    pub async fn stats(&self, tenant: &str) -> (bool /*data_ingested*/, i64 /*oldest_ns*/, i64 /*newest_ns*/);
    // per-partition committed offsets → frontier; the hot tier owns ns >= frontier.
    pub fn block_builder_frontier_ns(&self, tenant: &str) -> i64;
}
```

> The head handle's exact method names are owned by Slice 4. Treat the block above as the *expected* surface; reconcile against `crates/profiles/src/head.rs` (or equivalent) before Task 2. If Slice 4 exposes only a raw `MemTable`/`Arc<RwLock<…>>` rather than these accessors, add a thin query-side handle in **this** slice's `head.rs` wrapping it — flag it. The querier must NOT mutate the head (it is fed by Slice 4's consumer loop); it only reads. **The hot symbol partitions MUST use a disjoint partition-id namespace from any cold block** (e.g. a reserved high bit), or `CompositeSymbolSource` routing collides — confirm Slice 4's hot-partition allocation; if it reuses block-local ids, the composite must key on `(source_tag, partition)` instead of `partition` alone (flag in Task 4).

---

## File structure (`crates/profiles/` — extends the Slice 4 crate)

| File | Responsibility |
|---|---|
| `build.rs` | add the `querier.v1` proto to the codegen set (reuse the gateway/rebalancer build.rs pattern) |
| `proto/querier/v1/querier.proto` | the `querier.v1.QuerierService` + `FlameGraph`/`Level`/`ProfileType`/`Series` messages |
| `src/lib.rs` | add `pub mod querier;` + the `pb_querier` codegen include + re-exports (existing Slice-4 modules unchanged) |
| `src/querier/mod.rs` | querier module decls + `QuerierConfig` |
| `src/querier/head.rs` | `HotTier` — thin read-side wrapper over Slice 4's head handle (sample batches, symbols, labels, stats, frontier) |
| `src/querier/store.rs` | `CrabkaProfileStore` — the `ProfileStore` impl (cold+hot UNION + frontier split; `CompositeSymbolSource`; label/type/series union) |
| `src/querier/connect/mod.rs` | `querier_router()` + `AppState` + `X-Scope-OrgID` extractor + the `QuerierService` builder wiring |
| `src/querier/connect/encode.rs` | proto projections: `flamegraph_to_proto` (groups-of-4), `flamegraph_diff_to_proto` (groups-of-7), `series_to_proto`, `profile_type_to_proto` |
| `src/querier/connect/handlers.rs` | the Connect method handlers (`ProfileTypes`/`LabelNames`/`LabelValues`/`Series`/`SelectMergeStacktraces`/`SelectSeries`/`SelectMergeSpanProfile`/`Diff`/`SelectMergeProfile`/`GetProfileStats`) |
| `src/querier/render/mod.rs` | legacy `GET /pyroscope/render` + `/pyroscope/render-diff` axum handlers |
| `src/querier/render/flamebearer.rs` | `FlameGraph`→`"single"` + `FlameGraphDiff`→`"double"` flamebearer JSON |
| `src/bin/crabka-profiles.rs` | role binary `--target querier` (extends the Slice-4 binary's `match target`) |

`store.rs` + `head.rs` are the only files touching DataFusion's query layer; `connect/encode.rs` + `render/flamebearer.rs` are the only files owning Pyroscope wire-shape serialization. This keeps the two churn-prone surfaces (DataFusion UNION + composite symbols, Pyroscope proto/JSON) each in one place.

---

### Task 1: Crate deps + `querier.v1` proto + codegen + querier module scaffold

**Files:**
- Modify: `crates/profiles/Cargo.toml`
- Create: `crates/profiles/build.rs` (or modify if Slice 4 already has one)
- Create: `crates/profiles/proto/querier/v1/querier.proto`
- Modify: `crates/profiles/src/lib.rs`
- Create: `crates/profiles/src/querier/mod.rs`

**Interfaces:**
- Produces: a compiling `crabka-profiles` with the `querier.v1` prost+Connect codegen, a `querier` module + `QuerierConfig`, and a smoke test.

- [ ] **Step 1: Add the Slice-5 dependencies to `crates/profiles/Cargo.toml`**

Append to `[dependencies]` (Slice 4 already has `arrow`, `thiserror`, `serde`, `tokio`, `axum`, `crabka-blockstore`, `prost`, `connectrpc-axum`):

```toml
datafusion = { workspace = true }
crabka-pprof = { path = "../pprof" }
connectrpc-axum = { workspace = true }
serde_json = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
```

Append to `[build-dependencies]`:

```toml
connectrpc-axum-build = { workspace = true }
```

Append to `[dev-dependencies]`:

```toml
tower = { workspace = true }            # ServiceExt::oneshot for in-process router tests
http-body-util = "0.1"                  # collect router response bodies
object_store = { workspace = true }     # InMemory store behind a test BlockStore
prost = { workspace = true }            # decode proto response bodies in tests
```

> `connectrpc-axum`/`connectrpc-axum-build` are already workspace deps (used by grpc-gateway + rebalancer). If `http-body-util` is not yet a workspace dep, add `http-body-util = "0.1"` to root `[workspace.dependencies]`. If Slice 4 already added `connectrpc-axum`/`prost` for the `push.v1` ingest door, do not duplicate.

- [ ] **Step 2: Write the `querier.v1` proto**

Create `crates/profiles/proto/querier/v1/querier.proto`. **Verify field numbers against the pinned Pyroscope tag** (`github.com/mirror.gcr.io/grafana/pyroscope/api/gen/proto/.../querier/v1/querier.proto`) — do not fabricate. The shape below is the Grafana-minimum surface + the spec §7.1 methods; pin the exact numbers from the upstream `.proto` before compiling.

```proto
syntax = "proto3";
package querier.v1;

// VERIFY all field numbers against the pinned Pyroscope querier.proto tag.
service QuerierService {
  rpc ProfileTypes(ProfileTypesRequest) returns (ProfileTypesResponse);
  rpc LabelNames(LabelNamesRequest) returns (LabelNamesResponse);
  rpc LabelValues(LabelValuesRequest) returns (LabelValuesResponse);
  rpc Series(SeriesRequest) returns (SeriesResponse);
  rpc SelectMergeStacktraces(SelectMergeStacktracesRequest) returns (SelectMergeStacktracesResponse);
  rpc SelectMergeSpanProfile(SelectMergeSpanProfileRequest) returns (SelectMergeSpanProfileResponse);
  rpc SelectSeries(SelectSeriesRequest) returns (SelectSeriesResponse);
  rpc SelectMergeProfile(SelectMergeProfileRequest) returns (Profile); // google.v1.Profile (raw pprof)
  rpc Diff(DiffRequest) returns (DiffResponse);
  rpc GetProfileStats(GetProfileStatsRequest) returns (GetProfileStatsResponse);
}

message ProfileType {
  string ID = 1;
  string name = 2;
  string sample_type = 3;
  string sample_unit = 4;
  string period_type = 5;
  string period_unit = 6;
}

message ProfileTypesRequest { int64 start = 1; int64 end = 2; }  // unix MILLIS
message ProfileTypesResponse { repeated ProfileType profile_types = 1; }

message LabelNamesRequest { repeated string matchers = 1; int64 start = 2; int64 end = 3; }
message LabelNamesResponse { repeated string names = 1; }

message LabelValuesRequest { string name = 1; repeated string matchers = 2; int64 start = 3; int64 end = 4; }
message LabelValuesResponse { repeated string names = 1; } // NOTE: field is `names`, not `values`

message SeriesRequest { repeated string matchers = 1; repeated string label_names = 2; int64 start = 3; int64 end = 4; }
message Labels { repeated LabelPair labels = 1; }
message LabelPair { string name = 1; string value = 2; }
message SeriesResponse { repeated Labels labels_set = 1; }

message Level { repeated int64 values = 1; }
message FlameGraph { repeated string names = 1; repeated Level levels = 2; int64 total = 3; int64 max_self = 4; }
message FlameGraphDiff { repeated string names = 1; repeated Level levels = 2;
  int64 total = 3; int64 max_self = 4; int64 left_ticks = 5; int64 right_ticks = 6; }

message SelectMergeStacktracesRequest {
  string profile_typeID = 1;
  string label_selector = 2;
  int64 start = 3;
  int64 end = 4;
  optional int64 max_nodes = 5; // default 2048
  // format / stack_trace_selector / profile_id_selector — add if pinned upstream
}
message SelectMergeStacktracesResponse { FlameGraph flamegraph = 1; }

message SelectMergeSpanProfileRequest {
  string profile_typeID = 1; string label_selector = 2; int64 start = 3; int64 end = 4;
  repeated string span_selector = 5; optional int64 max_nodes = 6;
}
message SelectMergeSpanProfileResponse { FlameGraph flamegraph = 1; }

message SelectSeriesRequest {
  string profile_typeID = 1; string label_selector = 2; int64 start = 3; int64 end = 4;
  repeated string group_by = 5; double step = 6; // SECONDS
  TimeSeriesAggregationType aggregation = 7;      // SUM=0 / AVERAGE=1
}
enum TimeSeriesAggregationType { TIME_SERIES_AGGREGATION_TYPE_SUM = 0; TIME_SERIES_AGGREGATION_TYPE_AVERAGE = 1; }
message Point { double value = 1; int64 timestamp = 2; } // ts MILLIS
message TimeSeries { repeated LabelPair labels = 1; repeated Point points = 2; }
message SelectSeriesResponse { repeated TimeSeries series = 1; }

message DiffRequest { SelectMergeStacktracesRequest left = 1; SelectMergeStacktracesRequest right = 2; }
message DiffResponse { FlameGraphDiff flamegraph = 1; }

message SelectMergeProfileRequest { string profile_typeID = 1; string label_selector = 2; int64 start = 3; int64 end = 4; }
message Profile { bytes raw = 1; } // PLACEHOLDER for google.v1.Profile — VERIFY: import the pprof proto instead

message GetProfileStatsRequest {}
message GetProfileStatsResponse { bool data_ingested = 1; int64 oldest_profile_time = 2; int64 newest_profile_time = 3; }
```

> **Verify-notes (do before codegen):**
> - **Field numbers + the `Profile` return type are the churn-prone surface.** Pin the Pyroscope `querier.proto` tag and copy the exact field numbers; `SelectMergeProfile` returns `google.v1.Profile` (the pprof proto from Slice 2) — if Slice 2 vendors that proto, `import` it here rather than the `Profile { bytes }` placeholder. The placeholder keeps codegen compiling; the by-the-wire shape is pinned in Task 6's handler test by decoding the bytes through `crabka_pprof::PprofProfile`.
> - `LabelValuesResponse.names` (NOT `values`) — this is a spec-called-out gotcha; keep it.
> - `start`/`end` are **MILLIS** on every request — do not convert to ns at the proto edge; the engine takes ms.

- [ ] **Step 3: Add the proto to `build.rs`**

If Slice 4 already has a `build.rs` (for the `push.v1` ingest proto), append the querier proto to its proto list. Otherwise create `crates/profiles/build.rs` mirroring `crates/grpc-gateway/build.rs`:

```rust
//! Generates Connect-RPC server stubs + prost message types from the profiles
//! `.proto` set. Prefers a system `protoc`; falls back to a vendored fetch only
//! when none is found (keeps `--offline` working with system protoc).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        // Slice 4 ingest proto(s) go here too if present.
        "proto/querier/v1/querier.proto",
    ];
    let mut builder = connectrpc_axum_build::compile_protos(&protos, &["proto"]);
    if !system_protoc_available() {
        builder = builder.fetch_protoc(None, None)?;
    }
    builder.compile()?;
    for p in protos {
        println!("cargo:rerun-if-changed={p}");
    }
    Ok(())
}

fn system_protoc_available() -> bool {
    if std::env::var_os("PROTOC").is_some() {
        return true;
    }
    std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
}
```

- [ ] **Step 4: Include the codegen in `lib.rs` + create `querier/mod.rs`**

Add to `lib.rs` (leaving Slice-4 modules/re-exports intact):

```rust
pub mod querier;

/// Generated `querier.v1` protobuf + Connect server stubs (codegen output in
/// `OUT_DIR/querier.v1.rs`, produced by `build.rs`).
#[allow(clippy::pedantic, clippy::style)]
pub mod pb_querier {
    include!(concat!(env!("OUT_DIR"), "/querier.v1.rs"));
}
```

> **Verify:** the generated file name is the proto `package` name (`querier.v1` → `querier.v1.rs`), matching gateway's `crabka.gateway.v1.rs`. Confirm the emitted file name in `OUT_DIR` after the first build; adjust the `include!` path if the codegen uses a different name.

Create `crates/profiles/src/querier/mod.rs`:

```rust
//! The querier role: a `ProfileStore` over hot (WAL-tail head) + cold (profile
//! blocks), the Connect `querier.v1` API that drives the FlameEngine, and the
//! legacy `/pyroscope/render` flamebearer endpoints. Slices 1–4 must be present
//! for this module to do useful work.

pub mod connect;
pub mod head;
pub mod render;
pub mod store;

/// Static configuration for the querier role.
#[derive(Clone, Debug)]
pub struct QuerierConfig {
    /// HTTP listen address for the Connect + legacy-render API.
    pub listen_addr: std::net::SocketAddr,
    /// Default `max_nodes` for flamegraph truncation when a request omits it.
    pub default_max_nodes: i64,
}

impl Default for QuerierConfig {
    fn default() -> Self {
        Self {
            listen_addr: ([0, 0, 0, 0], 4040).into(), // Pyroscope's default query port
            default_max_nodes: 2048,
        }
    }
}
```

> Pyroscope's default HTTP listen port is `4040`; matching it lets an existing Grafana Pyroscope datasource point at Crabka unchanged.

- [ ] **Step 5: Smoke test**

Add to `querier/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn default_config_matches_pyroscope_defaults() {
        let c = QuerierConfig::default();
        assert!(c.listen_addr.port() == 4040);
        assert!(c.default_max_nodes == 2048);
    }

    #[test]
    fn querier_v1_codegen_is_present() {
        // The generated builder type exists — proves the proto compiled.
        let _ = std::any::type_name::<crate::pb_querier::ProfileType>();
        assert!(true);
    }
}
```

- [ ] **Step 6: Build + test**

Run: `cargo test -p crabka-profiles --lib querier::tests`
Expected: compiles (first build runs `protoc` + pulls pprof/blockstore/datafusion — slow, normal), both tests PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/ Cargo.toml Cargo.lock
git commit -m "feat(profiles): querier.v1 proto + codegen + querier module scaffold"
```

---

### Task 2: `HotTier` — read-side wrapper over the Slice-4 head handle (structure + behavior-pin)

**Files:**
- Create: `crates/profiles/src/querier/head.rs`

**Interfaces:**
- Consumes: Slice 4's `HeadHandle` (`sample_batches`/`symbols`/`label_names`/`label_values`/`profile_types`/`series`/`stats`/`block_builder_frontier_ns`), blockstore profile samples schema accessor, `crabka-pprof::SymbolSource`.
- Produces:
  - `struct HotTier { handle: HeadHandle }` with:
    - `new(handle: HeadHandle) -> Self`
    - `async fn sample_batches(&self, tenant: &str, profile_type: &str, start_ms: i64, end_ms: i64) -> Result<Vec<RecordBatch>, HotError>` — hot sample rows matching the **same profile samples fact-table schema as cold blocks** (so the UNION typechecks)
    - `fn symbols(&self, tenant: &str) -> Arc<dyn SymbolSource>` — the hot tier's symbol source
    - `async fn label_names`/`label_values`/`profile_types`/`series` (live discovery)
    - `async fn stats(&self, tenant: &str) -> (bool, i64, i64)`
    - `fn frontier_ns(&self, tenant: &str) -> i64`
  - `enum HotError` (`thiserror`).

This task has **one churn-prone seam**: Slice 4's head handle API. Structure + a behavior-pin test now (against a fake handle / a directly-seeded head if Slice 4 exposes a test constructor); the live consumer-fed handle is exercised by the `#[ignore]` e2e (Task 8).

- [ ] **Step 1: Write the failing test**

Create `crates/profiles/src/querier/head.rs` with tests first. **Verify Slice 4's `HeadHandle` test/seed constructor before running** — the test assumes `HeadHandle::for_test()` that lets you push samples; adapt to Slice 4's real seed path (or wrap a fake behind a `HeadSource` trait if Slice 4 only offers the consumer-fed constructor).

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    // ADAPT: build a HeadHandle seeded with two samples of profile_type
    // "process_cpu:cpu:nanoseconds:cpu:nanoseconds" @ 2000ms, 3000ms, frontier=1500ms.
    async fn seeded() -> HotTier {
        unimplemented!("seed a head handle: 2 cpu samples @2000,3000ms; frontier 1500ms")
    }

    #[tokio::test]
    async fn sample_batches_returns_hot_rows_in_window() {
        let hot = seeded().await;
        let batches = hot
            .sample_batches("t", "process_cpu:cpu:nanoseconds:cpu:nanoseconds", 0, 10_000)
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows == 2);
    }

    #[tokio::test]
    async fn frontier_is_the_block_builder_committed_cut() {
        let hot = seeded().await;
        assert!(hot.frontier_ns("t") == 1500);
    }

    #[tokio::test]
    async fn stats_reports_oldest_and_newest() {
        let hot = seeded().await;
        let (ingested, oldest, newest) = hot.stats("t").await;
        assert!(ingested);
        assert!(oldest <= newest);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib querier::head`
Expected: FAIL — `cannot find type HotTier` (then `unimplemented!` in the seed helper).

- [ ] **Step 3: Implement `head.rs`**

```rust
//! Read-side wrapper over Slice 4's WAL-tail head handle: the *hot* half of the
//! hot/cold merge. The querier never mutates the head (Slice 4's consumer loop
//! owns writes); this type only projects it for `select`/label discovery/stats.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use crabka_pprof::SymbolSource;

use crate::head::HeadHandle; // Slice 4 (verify path)

/// Errors surfacing from the hot tier (Arrow projection / handle failures).
#[derive(Debug, thiserror::Error)]
pub enum HotError {
    #[error("head error: {0}")]
    Source(String),
}

/// The hot tier seen by the querier.
pub struct HotTier {
    handle: HeadHandle,
}

impl HotTier {
    #[must_use]
    pub fn new(handle: HeadHandle) -> Self {
        Self { handle }
    }

    /// Hot sample rows for `tenant`/`profile_type` in `[start_ms, end_ms]`, as
    /// Arrow batches matching the profile samples fact-table schema (so the
    /// cold/hot UNION typechecks).
    pub async fn sample_batches(
        &self,
        tenant: &str,
        profile_type: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RecordBatch>, HotError> {
        self.handle
            .sample_batches(tenant, profile_type, start_ms, end_ms)
            .await
            .map_err(|e| HotError::Source(e.to_string()))
    }

    /// The hot tier's symbol source (covers all hot stacktrace partitions).
    #[must_use]
    pub fn symbols(&self, tenant: &str) -> Arc<dyn SymbolSource> {
        self.handle.symbols(tenant)
    }

    pub async fn label_names(&self, tenant: &str) -> Vec<String> {
        self.handle.label_names(tenant).await
    }

    pub async fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.handle.label_values(tenant, name).await
    }

    pub async fn profile_types(&self, tenant: &str) -> Vec<String> {
        self.handle.profile_types(tenant).await
    }

    pub async fn series(&self, tenant: &str, label_names: &[String]) -> Vec<Vec<(String, String)>> {
        self.handle.series(tenant, label_names).await
    }

    /// `(data_ingested, oldest_ns, newest_ns)` from the hot tier (for GetProfileStats).
    pub async fn stats(&self, tenant: &str) -> (bool, i64, i64) {
        self.handle.stats(tenant).await
    }

    /// The per-tenant block-builder frontier (ns): the hot tier owns `ns >= frontier`.
    #[must_use]
    pub fn frontier_ns(&self, tenant: &str) -> i64 {
        self.handle.block_builder_frontier_ns(tenant)
    }
}
```

> **Verify-notes (do before this compiles):**
> - `HeadHandle`'s module path + method names — confirm against Slice 4 (`crate::head::HeadHandle` is the *expected* path). If the handle is `Arc<RwLock<Head>>` with no query methods, add the read methods to Slice 4's head in this task (additive, greenfield) OR introduce a `HeadSource` trait here that the real handle and the test fake both impl, and store `Box<dyn HeadSource>`.
> - `sample_batches` MUST emit the exact profile samples fact-table schema (Slice 1's accessor + the `PCOL_*` constants). If Slice 4's head stores `ProfileRecord`s rather than Arrow, encode them here via the same Slice-1 encoder the block-builder uses — reuse, do not re-implement the schema. The frontier unit (ns) vs the request unit (ms) mismatch is handled in `store.rs` (Task 3), not here.
> - The hot symbol partitions must be in a disjoint namespace from cold block partitions (see the Slice-4 verify-note) — if not, the composite source (Task 4) keys on `(source, partition)`.

- [ ] **Step 4: Make the test pass**

Wire `seeded()` to Slice 4's head seed path (or the `HeadSource` fake). Run `cargo test -p crabka-profiles --lib querier::head`.
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): HotTier — read-side wrapper over the WAL-tail head handle"
```

---

### Task 3: `CrabkaProfileStore::select` — cold + hot UNION (frontier split, no double-count)

**Files:**
- Create: `crates/profiles/src/querier/store.rs`

**Interfaces:**
- Consumes: `crabka-pprof::{ProfileStore, ProfileScan, SymbolSource, ProfileError}`, `crabka-blockstore::{BlockStore, ProfileIndex, LabelMatcher, MatchOp}` + the `PCOL_PROFILE_TYPE` constant + the samples schema accessor, `HotTier`.
- Produces:
  - `struct CrabkaProfileStore { blockstore: Arc<BlockStore>, hot: Arc<HotTier>, samples_schema: SchemaRef }`
  - `CrabkaProfileStore::new(blockstore, hot) -> Self`
  - the beginning of `#[async_trait] impl ProfileStore for CrabkaProfileStore` — `select()` only (the other four methods land in Task 5).
  - free fns `register_hot_memtable`, `register_union`, `profile_type_matcher(profile_type) -> LabelMatcher`.

The **UNION wiring is the churn-prone DataFusion surface.** Structure it as: (a) build the cold label matchers (`matchers` + a `__profile_type__` = `profile_type` `LabelMatcher` — the `ProfileIndex` profile-type/label prune happens inside `scan_context`); (b) get the cold `(SessionContext, cold_table)` from `BlockStore::scan_context`, restricting cold to `ts < frontier`; (c) register the hot batches (`hot_samples` MemTable) into the *same* `SessionContext`, restricting hot to `ts >= frontier`; (d) register a `UNION ALL` view (`samples_union`) over cold+hot; (e) return `ProfileScan { ctx, samples_table, symbols }` where `symbols` is the `CompositeSymbolSource` (built in Task 4 — Task 3 returns the hot `symbols` only and is upgraded in Task 4). Behavior-pin the no-double-count property with a test.

- [ ] **Step 1: Write the failing test (no broker — hot tier seeded directly)**

Create `crates/profiles/src/querier/store.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use crabka_blockstore::LabelMatcher;
    use crabka_pprof::ProfileStore;

    use super::*;

    const CPU: &str = "process_cpu:cpu:nanoseconds:cpu:nanoseconds";

    // Seed one COLD sample (cpu @ ts=1000ms, service=api, value=10) into a
    // BlockStore over InMemory, indexed via ProfileIndex + a 1-node SymbolDb.
    // Mirror Slice-1 block-build + index calls.
    async fn blockstore_with_one_cold_sample() -> Arc<crabka_blockstore::BlockStore> {
        unimplemented!("seed one cold cpu sample @ts=1000ms, service=api, value=10")
    }

    // Seed the hot tier with the SAME sample @1000ms (dup, must be cut by the
    // frontier) AND a hot-only sample @2000ms. Frontier seeded at 1500ms.
    async fn hot_with_dup_and_hot() -> Arc<crate::querier::head::HotTier> {
        unimplemented!("seed hot tier: dup@1000ms (cut) + hot@2000ms; frontier 1500ms")
    }

    fn svc_api() -> Vec<LabelMatcher> {
        vec![LabelMatcher::new("service_name", crabka_blockstore::MatchOp::Eq, "api")]
    }

    #[tokio::test]
    async fn select_unions_cold_and_hot_without_double_count() {
        let blockstore = blockstore_with_one_cold_sample().await;
        let hot = hot_with_dup_and_hot().await;

        let store = CrabkaProfileStore::new(blockstore, hot);
        let scan = store.select("t", CPU, &svc_api(), 0, 10_000).await.unwrap();

        let df = scan
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {}", scan.samples_table))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        // cold @1000 (one) + hot @2000 (one) = 2; the hot dup @1000 is excluded
        // by the frontier split (frontier=1500: cold owns <1500, hot owns >=1500).
        assert!(c == 2);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib querier::store`
Expected: FAIL — `cannot find type CrabkaProfileStore` (then `unimplemented!` in seeds).

- [ ] **Step 3: Implement `store.rs` (select + helpers; other trait methods stubbed)**

```rust
//! `CrabkaProfileStore`: the `ProfileStore` the FlameEngine plans against.
//! `select` registers the cold profile-block samples and the hot WAL-tail
//! batches into one `SessionContext` and UNIONs them, split at the per-tenant
//! block-builder frontier so a sample sealed into a block is not also counted
//! from the hot tier. The returned `symbols` routes (partition, id) to the right
//! block SymbolDb or the hot SymbolDb (Task 4). label/type/series union: Task 5.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use crabka_blockstore::{BlockStore, LabelMatcher, MatchOp};
use crabka_pprof::{ProfileError, ProfileScan, ProfileStore, SymbolSource};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::querier::head::HotTier;
use crabka_blockstore::PCOL_PROFILE_TYPE; // Slice 1 constant (verify name/path)
use crate::profile_samples_schema; // Slice 1 accessor (verify name/path)

/// The querier's `ProfileStore`.
pub struct CrabkaProfileStore {
    blockstore: Arc<BlockStore>,
    hot: Arc<HotTier>,
    samples_schema: SchemaRef,
}

impl CrabkaProfileStore {
    #[must_use]
    pub fn new(blockstore: Arc<BlockStore>, hot: Arc<HotTier>) -> Self {
        Self {
            blockstore,
            hot,
            samples_schema: profile_samples_schema(),
        }
    }

    pub(crate) fn err(e: impl std::fmt::Display) -> ProfileError {
        ProfileError::Store(e.to_string())
    }
}

#[async_trait]
impl ProfileStore for CrabkaProfileStore {
    async fn select(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ProfileScan, ProfileError> {
        // Frontier is ns; the query unit here is ms. Convert once.
        let frontier_ms = self.hot.frontier_ns(tenant) / 1_000_000;
        let cold_max = (frontier_ms - 1).min(end_ms); // cold owns ms < frontier
        let hot_min = frontier_ms.max(start_ms); // hot owns ms >= frontier

        // The profile-type selector is a mandatory matcher on top of the user's.
        let mut cold_matchers = matchers.to_vec();
        cold_matchers.push(profile_type_matcher(profile_type));

        // COLD: ProfileIndex prunes inside scan_context (label/profile-type + time/block).
        let (ctx, cold_table) = self
            .blockstore
            .scan_context(tenant, &cold_matchers, start_ms, cold_max)
            .await
            .map_err(Self::err)?;

        // HOT: hot batches for ms >= frontier, registered into the SAME ctx.
        let hot_batches = self
            .hot
            .sample_batches(tenant, profile_type, hot_min, end_ms)
            .await
            .map_err(Self::err)?;
        register_hot_memtable(&ctx, "hot_samples", self.samples_schema.clone(), hot_batches)?;

        // UNION ALL view over cold + hot.
        let samples_table = register_union(&ctx, "samples_union", &cold_table, "hot_samples").await?;

        // Task 3 returns the hot symbols only; Task 4 upgrades this to the composite.
        let symbols: Arc<dyn SymbolSource> = self.hot.symbols(tenant);
        Ok(ProfileScan { ctx, samples_table, symbols })
    }

    async fn label_names(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        unimplemented!("label_names — Task 5")
    }

    async fn label_values(
        &self,
        _tenant: &str,
        _name: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        unimplemented!("label_values — Task 5")
    }

    async fn profile_types(
        &self,
        _tenant: &str,
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        unimplemented!("profile_types — Task 5")
    }

    async fn series(
        &self,
        _tenant: &str,
        _matchers: &[LabelMatcher],
        _label_names: &[String],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        unimplemented!("series — Task 5")
    }
}

/// The mandatory `__profile_type__ = <profile_type>` matcher every `select` adds.
#[must_use]
pub fn profile_type_matcher(profile_type: &str) -> LabelMatcher {
    LabelMatcher::new("__profile_type__", MatchOp::Eq, profile_type)
}

/// Register hot sample batches as a `MemTable` under `name`.
pub(crate) fn register_hot_memtable(
    ctx: &SessionContext,
    name: &str,
    schema: SchemaRef,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<(), ProfileError> {
    let partitions = if batches.is_empty() { vec![] } else { vec![batches] };
    let table = MemTable::try_new(schema, partitions).map_err(CrabkaProfileStore::err)?;
    ctx.register_table(name, Arc::new(table))
        .map_err(CrabkaProfileStore::err)?;
    Ok(())
}

/// Register a `UNION ALL` view over `cold` + `hot` and return its name.
pub(crate) async fn register_union(
    ctx: &SessionContext,
    view_name: &str,
    cold_table: &str,
    hot_table: &str,
) -> Result<String, ProfileError> {
    let sql = format!(
        "CREATE VIEW {view_name} AS \
         SELECT * FROM {cold_table} UNION ALL SELECT * FROM {hot_table}"
    );
    ctx.sql(&sql).await.map_err(CrabkaProfileStore::err)?;
    Ok(view_name.to_string())
}
```

> **Churn-point checklist (verify against the pinned datafusion rev + blockstore/pprof APIs if compile fails):**
> - `BlockStore::scan_context(&self, tenant, &[LabelMatcher], start_ms, end_ms) -> Result<(SessionContext, String)>` — the generalized signature (Slice 1). Confirm the arg order + the time unit (the samples `timestamp` is ns per §4.1, but `scan_context`'s window args may be ms or ns — match what Slice 1/4 use; if ns, multiply `start_ms`/`cold_max`/`hot_min` by 1e6 here, and have `HotTier::sample_batches` take ns too). **Pin the unit in the test seed.**
> - `LabelMatcher::new(name, MatchOp, value)` — confirm the constructor (blockstore Slice 1). If `__profile_type__` is a dedicated dict column (`PCOL_PROFILE_TYPE`) rather than a postings label, the profile-type prune may already be inside `scan_context`; either way `profile_type_matcher` is the input and the cold scan must restrict to that type.
> - `MemTable::try_new(SchemaRef, Vec<Vec<RecordBatch>>)` at `datafusion::catalog::MemTable` (older path `datafusion::datasource::MemTable`).
> - `CREATE VIEW … UNION ALL …` then query by name — if `CREATE VIEW` over registered tables is unsupported at the pin, build the union via `ctx.table(cold).await?.union(ctx.table(hot).await?)?.into_view()` + `register_table`. The **behavior** (UNION ALL, no dedup, frontier split prevents double-count) is what the test pins.
> - `ProfileScan { ctx, samples_table, symbols }` + `ProfileError::Store(String)` — match Slice 2's real definitions.
> - `profile_samples_schema()` — Slice 1's accessor; confirm the exact path/name. The hot MemTable schema MUST equal the cold table's schema or the UNION fails to typecheck.

- [ ] **Step 4: Make the test pass**

Wire the two seed helpers (mirror Slice-1 block-build + `ProfileIndex` + `SymbolDb` calls for cold; Slice-4 head seed for hot). Run `cargo test -p crabka-profiles --lib querier::store::tests::select_unions_cold_and_hot_without_double_count`.
Expected: PASS (`c == 2`, not 3).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): CrabkaProfileStore::select — cold+hot UNION with frontier split"
```

---

### Task 4: `CompositeSymbolSource` — per-partition routing across block + hot symbol DBs

**Files:**
- Modify: `crates/profiles/src/querier/store.rs`

**Interfaces:**
- Consumes: `crabka-pprof::{SymbolSource, Frame}`, `crabka-blockstore::BlockStore` (per-block `SymbolDb` accessor), `HotTier::symbols`.
- Produces:
  - `struct CompositeSymbolSource { sources: Vec<Arc<dyn SymbolSource>>, /* partition → source-index map */ }` implementing `SymbolSource`.
  - the upgraded `select()` body returning `Arc::new(CompositeSymbolSource::...)` as `ProfileScan.symbols`.
  - `async fn build_symbol_source(&self, tenant, &cold_matchers, start_ms, end_ms) -> Arc<dyn SymbolSource>` on `CrabkaProfileStore`.

This is the **load-bearing distributed-merge invariant**: a `stacktrace_id` is only meaningful within its own block's symbol DB, so `resolve(partition, id)` must route to the *exact* symbol source that owns that partition (the surviving cold blocks' `SymbolDb`s + the hot tier's `SymbolDb`), never a different block's. The composite holds a `partition → source` map built from the candidate blocks' stacktrace-partition maps + the hot partitions.

- [ ] **Step 1: Write the failing test**

Add to `store.rs` tests:

```rust
    #[tokio::test]
    async fn composite_resolves_each_partition_against_its_own_db() {
        // Two cold blocks: block A owns partition 1 (id 5 -> frame "a::f"),
        // block B owns partition 2 (id 5 -> frame "b::g"); the hot tier owns
        // partition 100 (id 5 -> frame "hot::h"). Same id 5 in all three — the
        // composite MUST route by partition, not collide.
        let blockstore = blockstore_two_blocks_distinct_partitions().await;
        let hot = hot_with_partition_100().await;
        let store = CrabkaProfileStore::new(blockstore, hot);

        let scan = store.select("t", CPU, &[], 0, 10_000).await.unwrap();
        let s = &scan.symbols;
        assert!(s.resolve(1, 5)[0].function == "a::f");
        assert!(s.resolve(2, 5)[0].function == "b::g");
        assert!(s.resolve(100, 5)[0].function == "hot::h");
        assert!(s.resolve(999, 5).is_empty()); // unknown partition → empty (not a panic)
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib querier::store::tests::composite`
Expected: FAIL — `select` returns the hot symbols only; partitions 1/2 resolve empty.

- [ ] **Step 3: Implement `CompositeSymbolSource` + upgrade `select`**

```rust
use std::collections::HashMap;

/// Routes `resolve(partition, id)` to the symbol source that owns `partition`.
/// Each cold block contributes its own `SymbolDb` (its stacktrace-partition map
/// lists the partitions it owns); the hot tier contributes one source for its
/// (disjoint) partitions. A `stacktrace_id` is only meaningful within its own
/// block's symbol DB, so routing by partition is the correctness invariant of
/// the distributed merge.
pub struct CompositeSymbolSource {
    sources: Vec<Arc<dyn SymbolSource>>,
    by_partition: HashMap<u64, usize>, // partition → index into `sources`
}

impl CompositeSymbolSource {
    #[must_use]
    pub fn new(entries: Vec<(Arc<dyn SymbolSource>, Vec<u64>)>) -> Self {
        let mut sources = Vec::with_capacity(entries.len());
        let mut by_partition = HashMap::new();
        for (src, partitions) in entries {
            let idx = sources.len();
            sources.push(src);
            for p in partitions {
                by_partition.insert(p, idx); // disjoint namespaces ⇒ last-wins is safe
            }
        }
        Self { sources, by_partition }
    }
}

impl SymbolSource for CompositeSymbolSource {
    fn resolve(&self, partition: u64, id: u32) -> Vec<Frame> {
        match self.by_partition.get(&partition) {
            Some(&idx) => self.sources[idx].resolve(partition, id),
            None => Vec::new(), // unknown partition → empty (defensive; never panic)
        }
    }
}
```

Add the builder on `CrabkaProfileStore` and call it in `select` (replace the `let symbols = self.hot.symbols(tenant);` line):

```rust
impl CrabkaProfileStore {
    /// Build the composite symbol source for the candidate cold blocks + the hot
    /// tier, keyed by stacktrace partition.
    async fn build_symbol_source(
        &self,
        tenant: &str,
        cold_matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Arc<dyn SymbolSource>, ProfileError> {
        let mut entries: Vec<(Arc<dyn SymbolSource>, Vec<u64>)> = Vec::new();

        // COLD: each candidate block contributes its SymbolDb + the partitions it owns.
        let blocks = self
            .blockstore
            .candidate_blocks(tenant, cold_matchers, start_ms, end_ms)
            .await
            .map_err(Self::err)?;
        for block in blocks {
            let symdb = self
                .blockstore
                .load_symbol_db(tenant, &block)
                .await
                .map_err(Self::err)?; // Arc<dyn SymbolSource> (the block SymbolDb)
            let partitions = self
                .blockstore
                .block_partitions(tenant, &block)
                .await
                .map_err(Self::err)?; // Vec<u64> from the stacktrace-partition map
            entries.push((symdb, partitions));
        }

        // HOT: one source for its (disjoint) partitions.
        let hot_syms = self.hot.symbols(tenant);
        let hot_partitions = self.hot_partitions(tenant).await; // Vec<u64>
        entries.push((hot_syms, hot_partitions));

        Ok(Arc::new(CompositeSymbolSource::new(entries)))
    }

    async fn hot_partitions(&self, _tenant: &str) -> Vec<u64> {
        // ADAPT: ask HotTier for the partitions it currently owns. If Slice 4's
        // head exposes them, surface via HotTier; else derive from the hot
        // sample batches' PCOL_STACKTRACE_PARTITION distinct values.
        Vec::new()
    }
}
```

> **Verify-notes:**
> - `BlockStore::candidate_blocks(tenant, &[LabelMatcher], start_ms, end_ms) -> Result<Vec<BlockKey>>`, `BlockStore::load_symbol_db(tenant, &BlockKey) -> Result<Arc<dyn SymbolSource>>`, `BlockStore::block_partitions(tenant, &BlockKey) -> Result<Vec<u64>>` are the **expected** Slice-1 surfaces (the symbol-DB artifact loader + the stacktrace-partition map). Confirm the names; if `scan_context` does not expose the surviving block list, add a `candidate_blocks` accessor to `crabka-blockstore` in this task (it is the same prune `scan_context` runs internally) — flag it as a small blockstore addition. The `SymbolDb` must impl `SymbolSource` (Slice 2 says it does) and `load_symbol_db` returns it behind the trait.
> - **Disjoint partition namespaces** — if Slice 4's hot partitions can collide with a block's partition ids, change `by_partition` to key on `(source_tag, partition)` and thread a `source_tag` through the samples table so the engine knows which source a row's partition belongs to. Confirm Slice 4's hot-partition allocation (the Task-2 verify-note). The headline test seeds disjoint partitions (1/2/100), so it pins routing either way.
> - `hot_partitions` — surface the hot tier's owned partitions from `HotTier` (add a method) or derive from the distinct `PCOL_STACKTRACE_PARTITION` in the hot batches. Flag if Slice 4 must expose it.

- [ ] **Step 4: Wire `select` to use the composite**

In `select`, replace `let symbols: Arc<dyn SymbolSource> = self.hot.symbols(tenant);` with:

```rust
        let symbols = self
            .build_symbol_source(tenant, &cold_matchers, start_ms, cold_max)
            .await?;
```

- [ ] **Step 5: Make the test pass**

Wire `blockstore_two_blocks_distinct_partitions` + `hot_with_partition_100`. Run `cargo test -p crabka-profiles --lib querier::store::tests::composite`.
Expected: PASS (each partition resolves against its own DB; unknown → empty).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): CompositeSymbolSource — per-partition routing (raw ids never cross a block)"
```

---

### Task 5: `CrabkaProfileStore` discovery — `label_names`/`label_values`/`profile_types`/`series` (ProfileIndex ∪ hot)

**Files:**
- Modify: `crates/profiles/src/querier/store.rs`

**Interfaces:**
- Consumes: `crabka-blockstore::ProfileIndex` (per-block label postings + profile-type index), `HotTier` discovery methods, `crabka-pprof::ProfileError`.
- Produces: real `label_names`/`label_values`/`profile_types`/`series` bodies on `CrabkaProfileStore`.

Each unions the per-block `ProfileIndex` (filtered by `matchers`/time where applicable) with the hot tier's live discovery, deduped + sorted. `profile_types` returns the distinct `__profile_type__` 5-part strings; `series` returns each matching series' label set restricted to `label_names`.

- [ ] **Step 1: Write the failing tests**

Add to `store.rs` tests:

```rust
    #[tokio::test]
    async fn profile_types_union_cold_and_hot() {
        // cold block has cpu; hot tier has alloc_space.
        let blockstore = blockstore_with_cpu_type().await;
        let hot = hot_with_alloc_type().await;
        let store = CrabkaProfileStore::new(blockstore, hot);

        let mut types = store.profile_types("t", 0, 10_000).await.unwrap();
        types.sort();
        assert!(types.iter().any(|t| t.starts_with("process_cpu:")));
        assert!(types.iter().any(|t| t.starts_with("memory:alloc_space:")));
    }

    #[tokio::test]
    async fn label_values_union_cold_and_hot_sorted_dedup() {
        // cold service_name=api; hot service_name=web AND api (api is a dup).
        let blockstore = blockstore_service_api().await;
        let hot = hot_service_web_and_api().await;
        let store = CrabkaProfileStore::new(blockstore, hot);

        let vals = store.label_values("t", "service_name", &[], 0, 10_000).await.unwrap();
        assert!(vals == vec!["api".to_string(), "web".to_string()]); // sorted + deduped
    }

    #[tokio::test]
    async fn label_names_union_cold_and_hot() {
        let blockstore = blockstore_service_api().await; // service_name
        let hot = hot_with_extra_label().await; // pod
        let store = CrabkaProfileStore::new(blockstore, hot);

        let names = store.label_names("t", &[], 0, 10_000).await.unwrap();
        assert!(names.iter().any(|n| n == "service_name"));
        assert!(names.iter().any(|n| n == "pod"));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib querier::store::tests::profile_types`
Expected: FAIL — `unimplemented!("profile_types — Task 5")`.

- [ ] **Step 3: Implement the four discovery methods**

Replace the Task-3 stubs:

```rust
    async fn label_names(
        &self,
        tenant: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        use std::collections::BTreeSet;
        let mut set: BTreeSet<String> = self.blockstore.index().label_names(tenant).into_iter().collect();
        set.extend(self.hot.label_names(tenant).await);
        Ok(set.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        _matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        use std::collections::BTreeSet;
        let mut set: BTreeSet<String> =
            self.blockstore.index().label_values(tenant, name).into_iter().collect();
        set.extend(self.hot.label_values(tenant, name).await);
        Ok(set.into_iter().collect())
    }

    async fn profile_types(
        &self,
        tenant: &str,
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, ProfileError> {
        use std::collections::BTreeSet;
        // The ProfileIndex profile-type index keys are the distinct 5-part strings.
        let mut set: BTreeSet<String> = self.blockstore.index().profile_types(tenant).into_iter().collect();
        set.extend(self.hot.profile_types(tenant).await);
        Ok(set.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Vec<(String, String)>>, ProfileError> {
        use std::collections::BTreeSet;
        let mut set: BTreeSet<Vec<(String, String)>> = BTreeSet::new();
        for s in self.blockstore.index().series(tenant, matchers, label_names) {
            set.insert(s);
        }
        for s in self.hot.series(tenant, label_names).await {
            set.insert(s);
        }
        Ok(set.into_iter().collect())
    }
```

> **Verify-notes:**
> - `BlockStore::index() -> &ProfileIndex` + `ProfileIndex::label_names(tenant) -> Vec<String>`, `label_values(tenant, name) -> Vec<String>`, `profile_types(tenant) -> Vec<String>`, `series(tenant, &[LabelMatcher], &[String]) -> Vec<Vec<(String,String)>>` are the **expected** Slice-1 discovery surfaces (the reused metrics `SeriesIndex` postings + the profile-type index). Confirm the names; if the `ProfileIndex` reuses the metrics `SeriesIndex` accessors verbatim, call those. **If a discovery accessor is missing, add it to `ProfileIndex` in this task** (it is a thin postings projection) — flag it.
> - The time-window filter is dropped here (`_start_ms`/`_end_ms`) — the in-memory `ProfileIndex` is not time-sharded at this granularity; tighten with a per-block time-range prune if Slice 1 exposes it (flag, not required for the headline tests).
> - `series` matcher-filtering depends on `ProfileIndex::series` honoring matchers; the hot `series` is unfiltered (the hot tier is small) — flag.

- [ ] **Step 4: Make the tests pass**

Wire the discovery seed helpers. Run `cargo test -p crabka-profiles --lib querier::store`.
Expected: PASS (select + composite + discovery tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): CrabkaProfileStore discovery — ProfileIndex ∪ hot labels/types/series"
```

---

### Task 6: `FlameGraph` proto projection + Connect handlers (the byte-equality analog)

**Files:**
- Create: `crates/profiles/src/querier/connect/mod.rs`
- Create: `crates/profiles/src/querier/connect/encode.rs`
- Create: `crates/profiles/src/querier/connect/handlers.rs`

**Interfaces:**
- Consumes: `crabka-pprof::{FlameEngine, FlameGraph, Level, FlameGraphDiff, Series, SeriesAgg, ProfileType, ProfileError}`, the `pb_querier` codegen, `connectrpc-axum::message::{ConnectRequest, ConnectResponse, ConnectError}`.
- Produces:
  - `encode.rs`: `fn flamegraph_to_proto(&FlameGraph) -> pb_querier::FlameGraph` (groups-of-4 preserved), `fn flamegraph_diff_to_proto(&FlameGraphDiff) -> pb_querier::FlameGraphDiff` (groups-of-7), `fn series_to_proto(&[Series]) -> Vec<pb_querier::TimeSeries>`, `fn profile_type_to_proto(&str) -> pb_querier::ProfileType`, `fn err_to_connect(ProfileError) -> ConnectError`.
  - `connect/mod.rs`: `AppState { engine: Arc<FlameEngine<CrabkaProfileStore>>, store: Arc<CrabkaProfileStore>, cfg: QuerierConfig }`, `tenant_of(&HeaderMap) -> String`, `querier_service_router(state) -> Router`.
  - `handlers.rs`: the ten Connect method handlers.

- [ ] **Step 1: Write the failing exact-proto tests (the groups-of-4 byte-equality analog)**

Create `crates/profiles/src/querier/connect/encode.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pprof::{FlameGraph, FlameGraphDiff, Level};

    use super::*;

    #[test]
    fn flamegraph_levels_are_groups_of_four_preserved() {
        // names[0]="total" (root); one child bar.
        let fg = FlameGraph {
            names: vec!["total".into(), "main".into()],
            // level 0: the root [xOff=0,total=10,self=0,name=0]
            // level 1: one child [xOff=0,total=10,self=10,name=1]
            levels: vec![
                Level { values: vec![0, 10, 0, 0] },
                Level { values: vec![0, 10, 10, 1] },
            ],
            total: 10,
            max_self: 10,
        };
        let p = flamegraph_to_proto(&fg);
        assert!(p.names == vec!["total".to_string(), "main".to_string()]);
        // groups-of-4 sequence carried VERBATIM — this is the contract.
        assert!(p.levels[0].values == vec![0, 10, 0, 0]);
        assert!(p.levels[1].values == vec![0, 10, 10, 1]);
        assert!(p.total == 10 && p.max_self == 10);
    }

    #[test]
    fn flamegraph_diff_levels_are_groups_of_seven_with_ticks() {
        let d = FlameGraphDiff {
            names: vec!["total".into()],
            levels: vec![Level { values: vec![0, 10, 0, 0, 20, 0, 0] }], // 7/bar
            left_ticks: 10,
            right_ticks: 20,
        };
        let p = flamegraph_diff_to_proto(&d);
        assert!(p.levels[0].values == vec![0, 10, 0, 0, 20, 0, 0]);
        assert!(p.left_ticks == 10 && p.right_ticks == 20);
    }

    #[test]
    fn profile_type_to_proto_splits_five_parts() {
        let p = profile_type_to_proto("process_cpu:cpu:nanoseconds:cpu:nanoseconds");
        assert!(p.name == "process_cpu");
        assert!(p.sample_type == "cpu");
        assert!(p.sample_unit == "nanoseconds");
        assert!(p.period_type == "cpu");
        assert!(p.period_unit == "nanoseconds");
        assert!(p.ID == "process_cpu:cpu:nanoseconds:cpu:nanoseconds");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib querier::connect::encode`
Expected: FAIL — `cannot find function flamegraph_to_proto`.

- [ ] **Step 3: Implement `encode.rs`**

```rust
//! `querier.v1` proto projections. The `FlameGraph` groups-of-4 / `FlameGraphDiff`
//! groups-of-7 int sequence is the byte-equality analog for this signal, so all
//! proto serialization funnels through here. The engine already produced the
//! correct int sequence (Slices 2–3); this is a faithful struct→proto copy.

use connectrpc_axum::message::ConnectError;
use crabka_pprof::{FlameGraph, FlameGraphDiff, ProfileError, ProfileType, Series};

use crate::pb_querier;

/// `crabka_pprof::FlameGraph` → `querier.v1.FlameGraph`. The level `values`
/// (groups of 4: `[xOffsetDelta, total, self, nameIndex]`) are carried VERBATIM.
#[must_use]
pub fn flamegraph_to_proto(fg: &FlameGraph) -> pb_querier::FlameGraph {
    pb_querier::FlameGraph {
        names: fg.names.clone(),
        levels: fg
            .levels
            .iter()
            .map(|l| pb_querier::Level { values: l.values.clone() })
            .collect(),
        total: fg.total,
        max_self: fg.max_self,
    }
}

/// `crabka_pprof::FlameGraphDiff` → `querier.v1.FlameGraphDiff` (groups of 7).
#[must_use]
pub fn flamegraph_diff_to_proto(d: &FlameGraphDiff) -> pb_querier::FlameGraphDiff {
    pb_querier::FlameGraphDiff {
        names: d.names.clone(),
        levels: d
            .levels
            .iter()
            .map(|l| pb_querier::Level { values: l.values.clone() })
            .collect(),
        // total/max_self are derivable; carry 0 unless the upstream proto requires them.
        total: 0,
        max_self: 0,
        left_ticks: d.left_ticks,
        right_ticks: d.right_ticks,
    }
}

/// `[Series]` → `[querier.v1.TimeSeries]` (points carry ts MILLIS + value).
#[must_use]
pub fn series_to_proto(series: &[Series]) -> Vec<pb_querier::TimeSeries> {
    series
        .iter()
        .map(|s| pb_querier::TimeSeries {
            labels: s
                .labels
                .iter()
                .map(|(name, value)| pb_querier::LabelPair {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            points: s
                .points
                .iter()
                .map(|(ts_ms, value)| pb_querier::Point { value: *value, timestamp: *ts_ms })
                .collect(),
        })
        .collect()
}

/// 5-part profile-type string → `querier.v1.ProfileType`. Reuses `ProfileType::parse`.
#[must_use]
pub fn profile_type_to_proto(s: &str) -> pb_querier::ProfileType {
    // ProfileType::parse owns the 5-part split (Slice 2). Fall back to a raw
    // split if parse fails (the ID is always the original string).
    let pt = ProfileType::parse(s).ok();
    let (name, sample_type, sample_unit, period_type, period_unit) = pt.map_or_else(
        || (String::new(), String::new(), String::new(), String::new(), String::new()),
        |p| (p.name, p.sample_type, p.sample_unit, p.period_type, p.period_unit),
    );
    pb_querier::ProfileType {
        ID: s.to_string(),
        name,
        sample_type,
        sample_unit,
        period_type,
        period_unit,
    }
}

/// `ProfileError` → Connect error envelope (code + message).
#[must_use]
pub fn err_to_connect(e: ProfileError) -> ConnectError {
    use connectrpc_axum::message::ConnectCode;
    let (code, msg) = match e {
        ProfileError::Decode(m) | ProfileError::Plan(m) | ProfileError::Unsupported(m) => {
            (ConnectCode::InvalidArgument, m)
        }
        ProfileError::Exec(m) | ProfileError::Store(m) | ProfileError::Symbolize(m) => {
            (ConnectCode::Internal, m)
        }
    };
    ConnectError::new(code, msg)
}
```

> **Verify-notes:**
> - `pb_querier::FlameGraph`/`Level`/`FlameGraphDiff`/`TimeSeries`/`Point`/`LabelPair`/`ProfileType` field names come from the codegen — confirm against the generated `querier.v1.rs` (prost lowercases + snake_cases; `profile_typeID`→`profile_type_id`, `ID`→`id`/`r#ID` depending on the proto). Adjust the field accessors to the generated names; the **int sequence** is the contract.
> - `ConnectError::new(ConnectCode, msg)` + the `ConnectCode` variants — confirm against `connectrpc-axum`'s `message` module (the gateway uses `ConnectError` in `handlers.rs`). If the constructor differs, match it; keep the code mapping (invalid-arg for client errors, internal for exec/store).
> - `ProfileType::parse` + its public fields — confirm against Slice 2 (`name`/`sample_type`/`sample_unit`/`period_type`/`period_unit`).

- [ ] **Step 4: Create `connect/mod.rs` (state + tenant + service router)**

```rust
//! Connect `querier.v1.QuerierService` wiring for the querier role.

pub mod encode;
pub mod handlers;

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderMap;
use crabka_pprof::FlameEngine;

use crate::querier::store::CrabkaProfileStore;
use crate::querier::QuerierConfig;

/// Shared handler state: the FlameEngine + the store (for discovery RPCs) + cfg.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<FlameEngine<CrabkaProfileStore>>,
    pub store: Arc<CrabkaProfileStore>,
    pub cfg: QuerierConfig,
}

/// Tenant from `X-Scope-OrgID`; falls back to `"anonymous"` (single-tenant mode).
#[must_use]
pub fn tenant_of(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

/// Build the Connect `querier.v1.QuerierService` router (reuses the gateway/
/// rebalancer `<Service>ServiceBuilder` codegen pattern).
#[must_use]
pub fn querier_service_router(state: AppState) -> Router {
    crate::pb_querier::querier_v1_connect::QuerierServiceBuilder::<()>::new()
        .profile_types(handlers::profile_types)
        .label_names(handlers::label_names)
        .label_values(handlers::label_values)
        .series(handlers::series)
        .select_merge_stacktraces(handlers::select_merge_stacktraces)
        .select_merge_span_profile(handlers::select_merge_span_profile)
        .select_series(handlers::select_series)
        .select_merge_profile(handlers::select_merge_profile)
        .diff(handlers::diff)
        .get_profile_stats(handlers::get_profile_stats)
        .build()
        .layer(axum::Extension(state))
}
```

> **Verify:** the generated builder module + type name (`querier_v1_connect::QuerierServiceBuilder` vs the gateway's `gateway_connect::GatewayServiceBuilder`) and the per-method setter names (snake_case of the RPC) come from `connectrpc-axum-build`. Confirm against the generated `querier.v1.rs` (search for `Builder`); the gateway pattern is `pb::<pkg>_connect::<Service>ServiceBuilder::<()>::new().<rpc>(handler)…build().layer(Extension(state))`.

- [ ] **Step 5: Implement `handlers.rs`**

```rust
//! Connect `querier.v1` method handlers — thin adapters: proto in, drive the
//! FlameEngine / ProfileStore, proto out. Tenant via `X-Scope-OrgID`; `start`/
//! `end` are unix MILLIS.

use axum::Extension;
use axum::http::HeaderMap;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use crabka_pprof::SeriesAgg;

use crate::pb_querier;
use crate::querier::connect::encode::{
    err_to_connect, flamegraph_diff_to_proto, flamegraph_to_proto, profile_type_to_proto,
    series_to_proto,
};
use crate::querier::connect::{tenant_of, AppState};

/// `ProfileTypes` — also the datasource health probe (no separate `/ready`).
pub async fn profile_types(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    req: ConnectRequest<pb_querier::ProfileTypesRequest>,
) -> Result<ConnectResponse<pb_querier::ProfileTypesResponse>, ConnectError> {
    let tenant = tenant_of(&headers);
    let m = req.0;
    let types = state
        .store
        .profile_types(&tenant, m.start, m.end)
        .await
        .map_err(err_to_connect)?;
    let profile_types = types.iter().map(|t| profile_type_to_proto(t)).collect();
    Ok(ConnectResponse(pb_querier::ProfileTypesResponse { profile_types }))
}

/// `SelectMergeStacktraces` → `FlameGraph` (the groups-of-4 contract).
pub async fn select_merge_stacktraces(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    req: ConnectRequest<pb_querier::SelectMergeStacktracesRequest>,
) -> Result<ConnectResponse<pb_querier::SelectMergeStacktracesResponse>, ConnectError> {
    let tenant = tenant_of(&headers);
    let m = req.0;
    let max_nodes = m.max_nodes.unwrap_or(state.cfg.default_max_nodes);
    let fg = state
        .engine
        .select_merge_stacktraces(&tenant, &m.profile_type_id, &m.label_selector, m.start, m.end, max_nodes)
        .await
        .map_err(err_to_connect)?;
    Ok(ConnectResponse(pb_querier::SelectMergeStacktracesResponse {
        flamegraph: Some(flamegraph_to_proto(&fg)),
    }))
}

/// `SelectSeries` — step in SECONDS, SUM/AVERAGE.
pub async fn select_series(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    req: ConnectRequest<pb_querier::SelectSeriesRequest>,
) -> Result<ConnectResponse<pb_querier::SelectSeriesResponse>, ConnectError> {
    let tenant = tenant_of(&headers);
    let m = req.0;
    let agg = match m.aggregation {
        x if x == pb_querier::TimeSeriesAggregationType::Average as i32 => SeriesAgg::Average,
        _ => SeriesAgg::Sum,
    };
    let series = state
        .engine
        .select_series(&tenant, &m.profile_type_id, &m.label_selector, &m.group_by, m.step, agg, m.start, m.end)
        .await
        .map_err(err_to_connect)?;
    Ok(ConnectResponse(pb_querier::SelectSeriesResponse { series: series_to_proto(&series) }))
}

/// `Diff` — two MERGEs → `FlameGraphDiff` (groups-of-7).
pub async fn diff(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    req: ConnectRequest<pb_querier::DiffRequest>,
) -> Result<ConnectResponse<pb_querier::DiffResponse>, ConnectError> {
    let tenant = tenant_of(&headers);
    let m = req.0;
    let l = m.left.ok_or_else(|| ConnectError::new(connectrpc_axum::message::ConnectCode::InvalidArgument, "left required".into()))?;
    let r = m.right.ok_or_else(|| ConnectError::new(connectrpc_axum::message::ConnectCode::InvalidArgument, "right required".into()))?;
    let max_nodes = l.max_nodes.unwrap_or(state.cfg.default_max_nodes);
    let d = state
        .engine
        .diff(
            &tenant,
            (&l.profile_type_id, &l.label_selector, l.start, l.end),
            (&r.profile_type_id, &r.label_selector, r.start, r.end),
            max_nodes,
        )
        .await
        .map_err(err_to_connect)?;
    Ok(ConnectResponse(pb_querier::DiffResponse { flamegraph: Some(flamegraph_diff_to_proto(&d)) }))
}

/// `SelectMergeProfile` → raw pprof bytes (google.v1.Profile).
pub async fn select_merge_profile(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    req: ConnectRequest<pb_querier::SelectMergeProfileRequest>,
) -> Result<ConnectResponse<pb_querier::Profile>, ConnectError> {
    let tenant = tenant_of(&headers);
    let m = req.0;
    let raw = state
        .engine
        .select_merge_profile(&tenant, &m.profile_type_id, &m.label_selector, m.start, m.end)
        .await
        .map_err(err_to_connect)?;
    // VERIFY: if `Profile` is google.v1.Profile, decode `raw` via prost into it;
    // the placeholder `Profile { raw }` carries the bytes (Task-1 verify-note).
    Ok(ConnectResponse(pb_querier::Profile { raw }))
}

/// `GetProfileStats` — from the hot tier (+ cold min/max if exposed).
pub async fn get_profile_stats(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    _req: ConnectRequest<pb_querier::GetProfileStatsRequest>,
) -> Result<ConnectResponse<pb_querier::GetProfileStatsResponse>, ConnectError> {
    let tenant = tenant_of(&headers);
    let (data_ingested, oldest, newest) = state.store.profile_stats(&tenant).await;
    Ok(ConnectResponse(pb_querier::GetProfileStatsResponse {
        data_ingested,
        oldest_profile_time: oldest,
        newest_profile_time: newest,
    }))
}

// label_names / label_values / series / select_merge_span_profile follow the same
// shape: tenant_of → parse matchers → store/engine call → proto. label_selector
// strings are parsed by crabka-pprof's matcher helper inside the engine; the
// discovery RPCs take `matchers: repeated string` (Prometheus matcher strings) —
// parse each via the same helper before calling store.{label_names,label_values,series}.
```

> **Verify-notes:**
> - The generated request field names (`profile_typeID` → `profile_type_id`, `max_nodes` `optional` → `Option<i64>`) come from prost — confirm against `querier.v1.rs` and fix the `.field` accesses. The `ConnectRequest`/`ConnectResponse` tuple-struct `.0` access matches the gateway (`let msg = req.0;`).
> - `select_merge_span_profile` reuses `select_merge_stacktraces`'s shape with the `span_selector` (the engine's `SelectMergeSpanProfile` path, Slice 3); wire it the same way.
> - `LabelNames`/`LabelValues`/`Series` take `repeated string matchers` — parse each matcher string to a `LabelMatcher` via `crabka_pprof`'s Prometheus-matcher helper before calling `store.label_names`/`label_values`/`series`. `LabelValuesResponse.names` (not `values`).
> - `CrabkaProfileStore::profile_stats(tenant) -> (bool, i64, i64)` — add a thin method delegating to `HotTier::stats` (+ cold block min/max ts if the `ProfileIndex` exposes a per-block time range); flag the cold half if Slice 1 lacks it.

- [ ] **Step 6: Run to verify encode tests pass + add a handler proto-decode test**

Run: `cargo test -p crabka-profiles --lib querier::connect::encode`
Expected: PASS (groups-of-4, groups-of-7, profile-type split).

Add an in-process Connect handler test (in `connect/mod.rs` tests) that builds an `AppState` over a store seeded with one cpu series, POSTs `application/proto` to `/querier.v1.QuerierService/SelectMergeStacktraces`, and decodes the `FlameGraph` proto body — asserting `names[0] == "total"` and `levels[0].values.len() % 4 == 0`. Use `tower::ServiceExt::oneshot` + `prost::Message::encode/decode` (mirror the gateway's `serve` test). Run: `cargo test -p crabka-profiles --lib querier::connect`.

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): querier.v1 Connect handlers + FlameGraph proto projection (groups-of-4)"
```

---

### Task 7: Legacy `/pyroscope/render` + `/pyroscope/render-diff` flamebearer endpoints

**Files:**
- Create: `crates/profiles/src/querier/render/mod.rs`
- Create: `crates/profiles/src/querier/render/flamebearer.rs`

**Interfaces:**
- Consumes: `AppState`, `tenant_of`, `crabka-pprof::{FlameEngine, FlameGraph, FlameGraphDiff}`.
- Produces:
  - `flamebearer.rs`: `fn flamegraph_to_flamebearer(&FlameGraph, meta: &RenderMeta) -> Value` (`"single"`, 4/bar), `fn flamegraph_diff_to_flamebearer(&FlameGraphDiff, meta: &RenderMeta) -> Value` (`"double"`, 7/bar), `struct RenderMeta { spy_name, sample_rate, units, name }`.
  - `render/mod.rs`: `render_router(state) -> Router` + the two axum handlers + `parse_render_query(&str) -> (profile_type, selector)`.

- [ ] **Step 1: Write the failing exact-JSON tests (flamebearer shape = the byte-equality analog)**

Create `crates/profiles/src/querier/render/flamebearer.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_pprof::{FlameGraph, FlameGraphDiff, Level};
    use serde_json::json;

    use super::*;

    fn meta() -> RenderMeta {
        RenderMeta { spy_name: "gospy".into(), sample_rate: 100, units: "samples".into(), name: "app".into() }
    }

    #[test]
    fn single_flamebearer_shape_is_exact() {
        let fg = FlameGraph {
            names: vec!["total".into(), "main".into()],
            levels: vec![Level { values: vec![0, 10, 0, 0] }, Level { values: vec![0, 10, 10, 1] }],
            total: 10,
            max_self: 10,
        };
        let got = flamegraph_to_flamebearer(&fg, &meta());
        let want = json!({
            "flamebearer": {
                "names": ["total", "main"],
                "levels": [[0, 10, 0, 0], [0, 10, 10, 1]],
                "numTicks": 10,
                "maxSelf": 10
            },
            "metadata": {
                "format": "single",
                "spyName": "gospy",
                "sampleRate": 100,
                "units": "samples",
                "name": "app"
            }
        });
        assert!(got == want, "got={got}");
    }

    #[test]
    fn double_flamebearer_is_format_double_seven_per_bar() {
        let d = FlameGraphDiff {
            names: vec!["total".into()],
            levels: vec![Level { values: vec![0, 10, 0, 0, 20, 0, 0] }],
            left_ticks: 10,
            right_ticks: 20,
        };
        let got = flamegraph_diff_to_flamebearer(&d, &meta());
        assert!(got["metadata"]["format"] == "double");
        assert!(got["flamebearer"]["levels"][0] == json!([0, 10, 0, 0, 20, 0, 0]));
        assert!(got["flamebearer"]["numTicks"] == 30); // left+right
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-profiles --lib querier::render::flamebearer`
Expected: FAIL — `cannot find function flamegraph_to_flamebearer`.

- [ ] **Step 3: Implement `flamebearer.rs`**

```rust
//! Legacy flamebearer JSON (the Profiles Drilldown app surface). The flamebearer
//! object shape is the byte-equality analog for the legacy endpoints, so all
//! flamebearer serialization funnels through here.
//! - single → `{ flamebearer:{names,levels[[4/bar]],numTicks,maxSelf}, metadata:{format:"single",…} }`
//! - double → 7-ints-per-bar levels + `format:"double"`.

use crabka_pprof::{FlameGraph, FlameGraphDiff};
use serde_json::{json, Value};

/// Flamebearer `metadata` block (carried from the render query / sample-type config).
pub struct RenderMeta {
    pub spy_name: String,
    pub sample_rate: i64,
    pub units: String,
    pub name: String,
}

/// `FlameGraph` → flamebearer `"single"` (4 ints/bar). `levels` is the level
/// list with each level's flat `values` carried VERBATIM as a nested array.
#[must_use]
pub fn flamegraph_to_flamebearer(fg: &FlameGraph, meta: &RenderMeta) -> Value {
    let levels: Vec<Value> = fg.levels.iter().map(|l| json!(l.values)).collect();
    json!({
        "flamebearer": {
            "names": fg.names,
            "levels": levels,
            "numTicks": fg.total,
            "maxSelf": fg.max_self
        },
        "metadata": {
            "format": "single",
            "spyName": meta.spy_name,
            "sampleRate": meta.sample_rate,
            "units": meta.units,
            "name": meta.name
        }
    })
}

/// `FlameGraphDiff` → flamebearer `"double"` (7 ints/bar).
#[must_use]
pub fn flamegraph_diff_to_flamebearer(d: &FlameGraphDiff, meta: &RenderMeta) -> Value {
    let levels: Vec<Value> = d.levels.iter().map(|l| json!(l.values)).collect();
    json!({
        "flamebearer": {
            "names": d.names,
            "levels": levels,
            "numTicks": d.left_ticks + d.right_ticks,
            "maxSelf": 0
        },
        "metadata": {
            "format": "double",
            "spyName": meta.spy_name,
            "sampleRate": meta.sample_rate,
            "units": meta.units,
            "name": meta.name
        }
    })
}
```

> **Verify-notes:**
> - The flamebearer `metadata` keys (`spyName`/`sampleRate`/`units`/`name`/`format`) + the `"single"`/`"double"` discriminants are pinned against the Pyroscope flamebearer schema; cross-check against a real `cp-pyroscope` `/pyroscope/render?format=json` body in Slice 8's differential test. `numTicks` for `"double"` is left+right ticks — confirm against the upstream sum convention.
> - Pyroscope render also supports `format=dot` (the engine's `format=DOT`); not in scope here (the Drilldown app uses JSON). Flag if needed.

- [ ] **Step 4: Implement `render/mod.rs` (router + handlers + query parse)**

```rust
//! Legacy `GET /pyroscope/render` + `/pyroscope/render-diff`.

pub mod flamebearer;

use std::collections::HashMap;

use axum::extract::{Extension, Query};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};

use crate::querier::connect::{tenant_of, AppState};
use flamebearer::{flamegraph_diff_to_flamebearer, flamegraph_to_flamebearer, RenderMeta};

/// `GET /pyroscope/render` — `?query=<profile_typeID>{selectors}&from&until&maxNodes&format=json`.
pub async fn render(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    let Some(query) = params.get("query") else {
        return (StatusCode::BAD_REQUEST, "missing query").into_response();
    };
    let (profile_type, selector) = parse_render_query(query);
    let (from_ms, until_ms) = render_window(&params);
    let max_nodes = params.get("maxNodes").and_then(|s| s.parse().ok()).unwrap_or(state.cfg.default_max_nodes);

    match state
        .engine
        .select_merge_stacktraces(&tenant, &profile_type, &selector, from_ms, until_ms, max_nodes)
        .await
    {
        Ok(fg) => Json(flamegraph_to_flamebearer(&fg, &render_meta(&profile_type))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// `GET /pyroscope/render-diff` — server-side diff (`"double"`, 7 ints/bar).
pub async fn render_diff(
    Extension(state): Extension<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    // leftQuery/leftFrom/leftUntil + rightQuery/rightFrom/rightUntil (VERIFY param names).
    let Some((lt, ls, lf, lu)) = diff_side(&params, "left") else {
        return (StatusCode::BAD_REQUEST, "missing left").into_response();
    };
    let Some((rt, rs, rf, ru)) = diff_side(&params, "right") else {
        return (StatusCode::BAD_REQUEST, "missing right").into_response();
    };
    let max_nodes = params.get("maxNodes").and_then(|s| s.parse().ok()).unwrap_or(state.cfg.default_max_nodes);

    match state
        .engine
        .diff(&tenant, (&lt, &ls, lf, lu), (&rt, &rs, rf, ru), max_nodes)
        .await
    {
        Ok(d) => Json(flamegraph_diff_to_flamebearer(&d, &render_meta(&lt))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Build the legacy render router.
#[must_use]
pub fn render_router(state: AppState) -> Router {
    Router::new()
        .route("/pyroscope/render", get(render))
        .route("/pyroscope/render-diff", get(render_diff))
        .layer(axum::Extension(state))
}

/// Split `cpu:...{service="api"}` into `(profile_type, selector)`. The selector
/// (incl. `{}`) is a Prometheus matcher string the engine parses.
#[must_use]
pub fn parse_render_query(query: &str) -> (String, String) {
    match query.find('{') {
        Some(i) => (query[..i].to_string(), query[i..].to_string()),
        None => (query.to_string(), "{}".to_string()),
    }
}

fn render_window(params: &HashMap<String, String>) -> (i64, i64) {
    // Pyroscope `from`/`until` accept relative (`now-1h`) or unix; the engine
    // takes MILLIS. VERIFY: parse relative forms (reuse a Slice-4 helper) — here
    // unix-secs/ms only; flag relative-time parse.
    let parse = |k: &str, dflt: i64| {
        params.get(k).and_then(|s| s.parse::<i64>().ok()).map_or(dflt, |v| {
            if v < 10_000_000_000 { v * 1000 } else { v } // secs → ms heuristic
        })
    };
    (parse("from", i64::MIN), parse("until", i64::MAX))
}

fn render_meta(profile_type: &str) -> RenderMeta {
    RenderMeta {
        spy_name: "unknown".into(),
        sample_rate: 100,
        units: "samples".into(),
        name: profile_type.split(':').next().unwrap_or("app").to_string(),
    }
}

fn diff_side(params: &HashMap<String, String>, side: &str) -> Option<(String, String, i64, i64)> {
    let q = params.get(&format!("{side}Query"))?;
    let (t, s) = parse_render_query(q);
    let f = params.get(&format!("{side}From")).and_then(|x| x.parse().ok())?;
    let u = params.get(&format!("{side}Until")).and_then(|x| x.parse().ok())?;
    Some((t, s, f, u))
}
```

> **Verify-notes:**
> - `/pyroscope/render` query params + the relative-time (`now-1h`) parsing are pinned against Pyroscope's render handler; the `from`/`until` secs↔ms heuristic + the `render-diff` `leftQuery`/`rightQuery` param names need a cross-check against the real handler (Slice 8 differential) — flag relative-time parse as a follow-on.
> - `RenderMeta` defaults (`spyName`/`sampleRate`) are placeholders; the real values come from the profile's sample-type config stored at ingest — flag; the flamebearer *shape* is the in-scope pin.

- [ ] **Step 5: Run + add an in-process render test**

Run: `cargo test -p crabka-profiles --lib querier::render`
Expected: PASS (flamebearer shape tests). Add an in-process `oneshot` test (in `render/mod.rs` tests) seeding a store with one cpu series, GET `/pyroscope/render?query=process_cpu:...{}&from=0&until=10&format=json`, asserting `body["metadata"]["format"] == "single"` and `body["flamebearer"]["names"][0] == "total"`.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-profiles
cargo clippy -p crabka-profiles --all-targets
git add crates/profiles/
git commit -m "feat(profiles): legacy /pyroscope/render + render-diff flamebearer endpoints"
```

---

### Task 8: Role binary `crabka-profiles --target querier` + merged router + end-to-end `#[ignore]` + whole-crate gate

**Files:**
- Modify: `crates/profiles/src/querier/connect/mod.rs` (add `querier_router` merging Connect + render)
- Modify: `crates/profiles/src/bin/crabka-profiles.rs`
- Create: `crates/profiles/tests/querier_e2e.rs`

**Interfaces:**
- Consumes: `QuerierConfig`, `HotTier`, `CrabkaProfileStore`, `FlameEngine`, `querier_service_router`, `render_router`, the Slice-4 binary's `--target` dispatch + head builder, the broker/gateway `serve` pattern (plaintext axum serve).
- Produces: `querier_router(state) -> Router` (merges the Connect service + the legacy render router) + the `querier` arm of the role binary.

- [ ] **Step 1: Add `querier_router` merging both surfaces**

In `connect/mod.rs`:

```rust
/// The full querier HTTP surface: the Connect `querier.v1` service + the legacy
/// `/pyroscope/render` endpoints, sharing one `AppState`.
#[must_use]
pub fn querier_router(state: AppState) -> Router {
    querier_service_router(state.clone()).merge(crate::querier::render::render_router(state))
}
```

> `Router::merge` combines the two route sets; both carry the same `Extension(AppState)` layer. Confirm no route overlap (Connect paths are `/querier.v1.QuerierService/*`, render is `/pyroscope/*`).

- [ ] **Step 2: Add the `querier` arm to the role binary**

Extend the Slice-4 `match target` in `crates/profiles/src/bin/crabka-profiles.rs`:

```rust
// In the existing `match target.as_str()`:
        "querier" => run_querier(querier_config_from_env()).await,
```

Add the run fn:

```rust
use std::sync::Arc;

use crabka_pprof::{EngineOpts, FlameEngine};
use crabka_profiles::querier::connect::{querier_router, AppState};
use crabka_profiles::querier::head::HotTier;
use crabka_profiles::querier::store::CrabkaProfileStore;
use crabka_profiles::querier::QuerierConfig;

fn querier_config_from_env() -> QuerierConfig {
    QuerierConfig::default() // override listen_addr from env if set (flagged)
}

async fn run_querier(config: QuerierConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Cold blockstore — built from object-store config (env). VERIFY against
    // crabka_blockstore::BlockStore::new(store, base_url) + ProfileIndex load.
    let blockstore = Arc::new(build_blockstore_from_env().await?);

    // Hot tier — the WAL-tail head handle. In a split deployment the querier
    // connects to the head service; single-binary it shares the in-process head.
    let hot = Arc::new(HotTier::new(acquire_head_handle().await?));

    let store = Arc::new(CrabkaProfileStore::new(blockstore, hot));
    let engine = Arc::new(FlameEngine::new(
        store.clone(),
        EngineOpts { default_max_nodes: config.default_max_nodes },
    ));
    let app = querier_router(AppState { engine, store, cfg: config.clone() });

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "querier API listening (Connect + render)");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn build_blockstore_from_env() -> Result<crabka_blockstore::BlockStore, Box<dyn std::error::Error>> {
    todo!("construct BlockStore + load ProfileIndex from env-configured object store")
}

async fn acquire_head_handle()
-> Result<crabka_profiles::head::HeadHandle, Box<dyn std::error::Error>> {
    todo!("connect to / share the Slice-4 WAL-tail head and return its query handle")
}
```

> The binary has two `todo!()`s (`build_blockstore_from_env`, `acquire_head_handle`) gated on Slice 4's object-store config + head handle-acquisition surface. They are **wiring**, not logic — fill them when Slice 4's store-construction + head are available. Everything testable (head/store/engine/router) is exercised by Tasks 2–7 unit tests + the `#[ignore]` e2e below. Keep the binary compiling; `--target querier` fails loudly until Slice 4 wiring lands.

- [ ] **Step 3: Write the `#[ignore]` end-to-end test**

Create `crates/profiles/tests/querier_e2e.rs`. Boots an in-process broker (`crabka-broker` test-support), produces a handful of `ProfileRecord`s to the WAL topic, starts a Slice-4 head consumer, waits for it to catch up, then drives the router and asserts `SelectMergeStacktraces` returns a flamegraph for the produced series and `/pyroscope/render` returns a `"single"` flamebearer. Gated `#[ignore]` because it needs a broker.

```rust
//! End-to-end: produce ProfileRecords → head fills → querier.v1 SelectMergeStacktraces
//! + /pyroscope/render return them. Requires an in-process broker; run with `--ignored`.

#![cfg(test)]

use assert2::assert;

// NOTE: depends on Slice 4's ProfileRecord encode + PROFILES_WAL_TOPIC + the head
// consumer + producing to a Crabka broker. Wire those when available.

#[tokio::test]
#[ignore = "requires an in-process broker + Slice 4 ProfileRecord produce + head"]
async fn produce_then_merge_and_render_round_trip() {
    // 1. start in-process broker (crabka-broker test-support::start()).
    // 2. create __crabka_profiles_wal; produce ProfileRecords for a cpu series
    //    {service_name=api}, profile_type process_cpu:..., keyed by hash(tenant,fp).
    // 3. start the Slice-4 head consumer over the broker; wait until it has the
    //    series (bounded poll).
    // 4. HotTier over the head handle + empty-cold blockstore → CrabkaProfileStore
    //    → FlameEngine → querier_router.
    // 5. POST /querier.v1.QuerierService/SelectMergeStacktraces {profile_typeID,
    //    label_selector="{service_name=\"api\"}", start, end} → decode FlameGraph,
    //    assert names[0]="total", levels[0].values.len() % 4 == 0.
    // 6. GET /pyroscope/render?query=process_cpu:...{service_name="api"}&from&until
    //    → assert metadata.format="single".
    assert!(true); // Skeleton — fill from Tasks 6/7 want-bodies + a produce step.
}
```

> The e2e is a **skeleton with an explicit fill-list**, not fabricated passing code — it pins the *integration contract* (produce → head → merge/render) and is `#[ignore]`d so CI is green without a broker. When Slice 4's produce + head paths are in hand, flesh out steps 1–6; the assertions reuse Tasks 6/7's want-shapes.

- [ ] **Step 4: Build the binary + run non-ignored tests + whole-crate gate**

Run: `cargo build -p crabka-profiles --bin crabka-profiles`
Expected: compiles (the two `todo!()` wiring fns compile; `--target querier` would panic at runtime until Slice 4 wiring — acceptable for this slice).
Run: `cargo test -p crabka-profiles && cargo clippy -p crabka-profiles --all-targets && cargo fmt -p crabka-profiles --check`
Expected: all non-`#[ignore]` tests PASS, no warnings, formatting clean.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-profiles
git add crates/profiles/
git commit -m "feat(profiles): crabka-profiles --target querier binary + merged router + e2e skeleton"
```

---

## Self-review

**Spec coverage (against §6.5 `ProfileStore` boundary, §6.4 cross-block correctness, §7.1 Connect `querier.v1`, §7.2 legacy flamebearer, §11 Slice 5):**
- **Querier `ProfileStore` impl** (`CrabkaProfileStore`) merging cold `BlockStore::scan_context` + hot `HotTier`, UNION-ed, split at the block-builder frontier to avoid double-count → Tasks 2 (hot), 3 (select). The no-double-count property is the headline test (`c == 2`, not 3).
- **Cross-block correctness — raw ids never cross a boundary** (§6.4): `CompositeSymbolSource` routes `resolve(partition, id)` to the owning block `SymbolDb` / hot `SymbolDb` → Task 4. Pinned by the same-id-in-three-partitions test.
- **`label_names`/`label_values`/`profile_types`/`series`** from `ProfileIndex` ∪ hot, deduped/sorted → Task 5.
- **Connect `querier.v1.QuerierService`**, tenant via `X-Scope-OrgID`, start/end MILLIS → Tasks 1 (proto+codegen), 6 (handlers+router). `ProfileTypes` doubles as the health probe (no `/ready`); `LabelValues` response field is `names`.
- **`SelectMergeStacktraces`/`SelectSeries`/`Diff`/`SelectMergeProfile`/`SelectMergeSpanProfile`/`GetProfileStats`** drive `FlameEngine` → Task 6.
- **`FlameGraph` groups-of-4 + `FlameGraphDiff` groups-of-7 proto encoding** — the byte-equality analog → Task 6 (`flamegraph_to_proto`/`flamegraph_diff_to_proto` with verbatim int-sequence assertions).
- **Legacy `/pyroscope/render` + `/pyroscope/render-diff`** flamebearer `"single"`/`"double"` JSON → Task 7. The flamebearer object shape is the byte-exact assertion.
- **Role binary `--target querier`** + merged Connect+render router → Task 8.
- **Real-broker produce → head → merge/render** via in-process broker, `#[ignore]`d → Task 8 e2e.

**Placeholder scan / flagged deviations (honest):**
- **`querier.v1` proto field numbers + the `Profile` (google.v1.Profile) return type** are flagged verify-against-pinned-Pyroscope-tag — the proto compiles with a `Profile { bytes }` placeholder, and the by-wire pprof shape is pinned by decoding through `crabka_pprof::PprofProfile` in the handler test; field numbers are NOT fabricated (copy from the pinned tag).
- **`hot_partitions` / disjoint-partition-namespace** — the composite keys on `partition` assuming disjoint hot/cold namespaces; if Slice 4 reuses block-local ids, the flagged fallback keys on `(source, partition)`. The headline test seeds disjoint partitions so routing is pinned either way.
- **`RenderMeta` defaults + relative-time (`now-1h`) parse** in the legacy render path are flagged placeholders (the real `spyName`/`sampleRate` come from the ingest sample-type config; relative-time is a follow-on) — the flamebearer *shape* is the in-scope byte-exact pin.
- **Time-window filter on discovery** (`label_names`/etc. drop `_start_ms`/`_end_ms`) — the in-memory `ProfileIndex` isn't time-sharded at that granularity; flagged, tightened if Slice 1 exposes a per-block range.
- **Binary `build_blockstore_from_env` / `acquire_head_handle`** are `todo!()` wiring fns gated on Slice 4's object-store config + head handle — flagged as wiring, not logic; all logic is unit-tested broker-free.
- **The e2e test** is an `#[ignore]`d skeleton with an explicit fill-list (produce → head → merge/render), not fabricated passing assertions.

**Churn-prone surfaces — structured + behavior-pinned, not fabricated (per CLAUDE.md):**
- **`querier.v1` proto + `connectrpc-axum-build` codegen** (Task 1) — pinned by the `querier_v1_codegen_is_present` smoke + the Task-6 handler proto-decode test; the builder/setter names + generated field names are flagged verify-against-`querier.v1.rs`, the build.rs mirrors the gateway/rebalancer pattern verbatim.
- **DataFusion UNION of hot+cold** (Task 3 `register_union`/`register_hot_memtable`) — pinned by the `c == 2` no-double-count test; `CREATE VIEW … UNION ALL` vs `DataFrame::union(...).into_view()` fallback both flagged; hot MemTable schema must equal the cold table schema; the frontier ns↔ms unit is flagged with a "pin in the seed" note.
- **`CompositeSymbolSource` routing** (Task 4) — pinned by the same-id-across-partitions test; `candidate_blocks`/`load_symbol_db`/`block_partitions` flagged as expected Slice-1 surfaces (possible small additions).
- **`FlameGraph`→proto / →flamebearer** (Tasks 6/7) — isolated in `encode.rs`/`flamebearer.rs`, the int-sequence + JSON-object shape byte-pinned; the engine already owns the encoding (Slices 2–3), this is a faithful projection.
- **Connect handler signature** (`Extension(AppState)` + `ConnectRequest<T>` → `Result<ConnectResponse<T>, ConnectError>`) — taken verbatim from `crates/grpc-gateway/src/handlers.rs::send`; `ConnectError::new`/`ConnectCode` flagged verify-against-`connectrpc-axum`.
- **`crabka-pprof` contract** (`ProfileStore`/`ProfileScan`/`SymbolSource`/`FlameEngine`/result types/`ProfileError`) — consumed verbatim from the shared contract; every spot where a field name might differ (`Level`/`Frame` internals, `FlameEngine::store()`, `EngineOpts` fields, `ProfileType` fields, `SymbolSource::resolve` arg order) carries an explicit "adapt the Rust, keep the proto/JSON" verify-note.
- **Slice-4 head handle** (Task 2 `HotTier`) — isolated behind one wrapper with a behavior-pin test; if Slice 4 lacks query accessors, the `HeadSource`-trait fallback is flagged.

**Type consistency:** `HotTier`/`HotError` consistent across Tasks 2/3/4/5/8. `CrabkaProfileStore::new(blockstore, hot)` stable Tasks 3/4/5/6/8. `AppState { engine, store, cfg }`/`tenant_of`/`querier_service_router`/`querier_router` stable Tasks 6/7/8. `flamegraph_to_proto`/`flamegraph_diff_to_proto`/`series_to_proto`/`profile_type_to_proto`/`err_to_connect` defined once (Task 6), used by handlers and pinned by the encode tests. `flamegraph_to_flamebearer`/`flamegraph_diff_to_flamebearer`/`RenderMeta` defined once (Task 7). `profile_type_matcher`/`register_union`/`register_hot_memtable`/`CompositeSymbolSource` stable Tasks 3/4. `QuerierConfig` fields stable Tasks 1/6/8.

**Known risk (flagged, not hidden):** the two genuine cross-slice unknowns are (a) Slice 4's exact `ProfileRecord`/`HeadHandle` API + `PROFILES_WAL_TOPIC` + block-builder-frontier surface + hot-partition allocation, and (b) Slice 1/2/3's exact `ProfileIndex` discovery + symbol-DB-loader + stacktrace-partition accessors, the `pb_querier` generated names, `FlameEngine::store()`, and `ProfileType`/`FlameGraph`/`Level` shapes. Both are contained to clearly-marked seams (`HotTier`, `build_symbol_source`, `CompositeSymbolSource`, `flamegraph_to_proto`, `flamegraph_to_flamebearer`, `err_to_connect`, the store-accessor decision) with verify-notes and behavior-pinning tests, so any drift surfaces as a localized compile error against green tests — never silent wrong results.
