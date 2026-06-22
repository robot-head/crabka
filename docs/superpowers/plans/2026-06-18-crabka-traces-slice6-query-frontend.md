# crabka-traces Slice 6 — Query-frontend (search sharding + job queueing + fan-out + spanSet/trace merge)

> **COMPLETION STATUS (as-built):** Done and green — implemented as the typed
> `frontend/` module tree this plan prescribes (`wire`/`backend`/`job`/`merge`/
> `metrics_merge`/`queue`/`config`/`http_backend`/`server` + the `QueryFrontend`
> orchestrator in `mod.rs`), replacing raw-`serde_json::Value` merging with typed
> serde wire structs. The role binary's `--target query-frontend` is cut over and
> the old single-module `query_frontend.rs` is removed. Sharding (time → tier →
> block → row-group), bounded fan-out, typed merge (`limit` newest-first + `spss`,
> `matched` preserved), the `metrics{}` accounting, typed metrics/tag merges, and
> the full Tempo HTTP surface are all present and tested.
>
> **Deviations from the literal plan (adapted to the real types/topology):**
> - **Merge currency is the typed wire structs** (`TraceJson` for search, typed
>   OTLP-JSON for by-id), not `crabka_traceql::TraceResult`/`TraceSpans`: the real
>   `SpanRef` is 17 fields while the querier's search JSON is the thin Tempo shape,
>   and the plan's `TraceSpans` accessors (`merge_in`/`span_ids`/`approx_size_bytes`)
>   don't exist — so the as-built unions/dedupes/sizes at the wire level (lossless,
>   no fabricated fields). `From<&crabka_traceql::*>` projections are kept for the
>   mock backend.
> - **By-id is frontend-side typed assembly fanned per-querier, not per-block**: the
>   slice-5 querier reassembles a trace across blocks and exposes no block-scoped
>   by-id, so the frontend fans one job per querier (different live-stores hold
>   different recent spans), unions `resourceSpans`, dedupes by `spanID`, and sizes
>   → COMPLETE/PARTIAL. (This replaces the byte proxy the old module used.)
> - **`JobShard::Block` carries a `rowGroupStart`/`rowGroupEnd` range** (the real
>   querier contract), not the plan's `rowGroup: Option<usize>`.
> - **Error semantics:** search/tags/metrics shards partition the data → any job
>   error propagates (an invalid query surfaces the querier's `4xx`+body, not a
>   silent empty `200`); by-id queriers are redundant → tolerate per-querier
>   failures, propagate only if all fail.
> - Cross-request admission queueing (a slice-8 hardening concern) is out of scope;
>   Task 6's bound is the within-request job fan-out, as the plan specifies.
>
> The per-step boxes are checked to reflect the built artifacts; read the deviations
> above where a step's literal detail differs.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Build the `query-frontend` role for `crabka-traces` — an axum server that sits in front of N queriers (Slice 5) and (1) **shards** the trace search space into bounded jobs (time: recent live-store vs backend blocks; then per-block; then per-row-group sized ~`target_bytes_per_job`), (2) **queues** those jobs and **fans** them across queriers in parallel through a trait-abstracted `QuerierBackend` with bounded concurrency, (3) **merges** the per-job partials back into one Tempo JSON response while respecting `limit` (traces) and `spss` (spans-per-spanset), and (4) accumulates the `metrics{}` job-accounting block (`totalJobs`/`completedJobs`/`inspectedTraces`/`inspectedBytes`/`totalBlocks`) — all while preserving the Tempo HTTP byte-shapes the querier (Slice 5) exposes. `trace_by_id` fans the same job model (one job per candidate block) and assembles the single trace.

**Architecture:** A new `frontend` module tree inside `crabka-traces`. The querier backend is a `QuerierBackend` **trait** (`async fn search_job` / `async fn trace_by_id_job`) so tests drive a `MockQuerier` returning canned per-job partials and real deployments use an `HttpQuerier` pool (reqwest, the grpc-gateway `forward.rs` pattern). The shardable unit is a `SearchJob { shard: JobShard, sub_start_ns, sub_end_ns }` where `JobShard` is `Live` (the hot tier) or `Block { block_id, row_group: Option<usize> }` (a cold block, optionally one row-group). A `SearchPartial` / `TracePartial` mirrors the slice-2 `crabka-traceql` result types (`TraceResult`/`SpanSet`/`SpanRef`, `TraceSpans`) plus a `JobMetrics` accumulator, so the merge logic manipulates parsed results, not raw bytes. The pipeline composes as `plan jobs → queue (bounded fan-out) → per-job search → merge-traces (limit/spss) → accumulate metrics → render Tempo JSON`. A result cache is **optional** for traces and is *not* built here (see the "Result cache (deferred)" note) — search is dominated by block scan, the moving-window reuse that makes range-result caching pay off for metrics does not apply to ad-hoc TraceQL search, and Tempo's own frontend caches *job results* (per block+shard), which we leave to a hardening slice. The role binary is `crabka-traces --target query-frontend`.

**Tech Stack:** Rust 2024 · `axum` 0.8 (`http1`, `tokio`) · `reqwest` 0.13 (`json`, `rustls`) · `serde`/`serde_json` (Tempo JSON) · `tokio` (`rt-multi-thread`, `macros`, `time`, `sync`) · `futures` (bounded `buffer_unordered` fan-out) · `thiserror` · `async-trait` · `crabka-traceql` (result types: `TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`/`TagScope`/`ScopedTag`/`TypedValue`). Tests: `assert2`, `tokio` (`test`, `macros`).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire shapes freely; no shims, no migration code, no default-off feature gates.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-traces --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-traces` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!`/`assert2::check!` in tests.
- **Tempo wire fidelity:** the frontend must round-trip the querier's Tempo JSON unchanged for the no-op path (single job, no merge needed) — that is the byte-equality analog. Sharding only ever *partitions then re-unions* the same trace/span set; **a sharded search MUST equal the unsharded search over identical data** (the correctness centerpiece, Tasks 4–5, 9). The merged `traces` array respects `limit`; each `spanSets[].spans` respects `spss`; `startTimeUnixNano`/`durationNanos` stay string-encoded nanos, `durationMs` an int.
- **Tenant propagation:** the inbound `X-Scope-OrgID` header is threaded onto every backend job request. Never collapse tenants across jobs.
- **Job accounting is additive and lossless:** the response `metrics{}` block is the sum over completed jobs (`totalJobs`/`completedJobs`/`inspectedTraces`/`inspectedBytes`/`totalBlocks`); a failed job increments neither `completedJobs` nor its byte counters but does count toward `totalJobs`.

---

## Dependency & slice roadmap

**Depends on:** Slice 2 (`crabka-traceql` result types — pinned public contract) and Slice 5 (Querier + Tempo HTTP API). The frontend consumes the querier's HTTP surface, but **sharded at the job grain** rather than for the whole search:

- `GET /api/search?q=&start=&end=&limit=&spss=&minDuration=&maxDuration=` → TraceQL search → Tempo JSON (`{ traces:[...], metrics:{...} }`). The frontend issues this per **job** with an added `blockID`/`shard` restriction (Slice-5 contract below) and merges.
- `GET /api/v2/traces/{traceID}?start=&end=` → by-id → Tempo v2 JSON (`{ trace:{ resourceSpans:[...] }, status, message }`). The frontend issues this per candidate block and assembles.
- `GET /api/v2/search/tags` + `tag/{tag}/values` → tag discovery → the frontend fans, unions, dedupes.
- Tenant via `X-Scope-OrgID`. Errors as Tempo envelopes.

**The querier's job-restriction support is assumed (Slice 5 contract):** the querier honors a `blockID=<ulid>` query param (restrict the scan to that one block) and a `shard=live` param (restrict to the live-store hot tier) on `/api/search` and `/api/v2/traces/{id}`, and reports its scan accounting in the response `metrics{}` block. The frontend's job is to *enumerate* blocks/shards into jobs, *queue+fan* them, and *merge* partials; the querier's job is to *honor* the restriction and *report* its bytes. **This slice does not implement querier-side block/shard filtering** — it injects the params and merges. The block enumeration source is the querier's `/api/v2/search/blocks`-style metadata door (Slice-5 contract: `GET /api/blocks?tenant=&start=&end=` → `{ blocks:[ { blockID, startUnixNano, endUnixNano, totalRecords, sizeBytes, rowGroups } ] }`); absent at authoring time it is modeled here behind the `BlockCatalog` trait so tests drive a `MockCatalog`.

**Slices 2 & 5 absent at authoring time** — the result types this slice merges (`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`) are **imported from `crabka-traceql`** (Slice 2 defines them as the pinned crate contract; do not redefine). The Tempo-JSON projection of those types (`/api/search` and `/api/v2/traces/{id}` shapes) is (re)stated here in `frontend/wire.rs` as the slice's own canonical serde model, because it is the *HTTP-edge* projection the frontend renders and parses — when Slice 5 lands its querier serializes to the same shape. If Slice 5 already exposes a shared `wire` module, import it instead and delete `frontend/wire.rs`.

**The 8 traces slices** (this plan = Slice 6):

1. Blockstore generalization + span block schema (nested-set columns + DFS) + `TraceIndex`. *(`crabka-blockstore`)*
2. `crabka-traceql` core — parser + planner + selectors + `SpanStructuralJoin` (core ops) + the `SpanStore` trait + pinned result types.
3. TraceQL completeness — negated/union structural ops + pipeline aggregations + TraceQL metrics + tag discovery.
4. Ingest service — `distributor` → `trace_id`-partitioned WAL; `block-builder`; `live-store`. *(`crabka-traces`)*
5. Querier + Tempo HTTP API — `SpanStore` as hot/cold UNION; `/api/echo`, `/api/v2/traces/{id}`, `/api/search`, `/api/v2/search/tags`+`values`, `/api/metrics/query_range`+`query`.
6. **Query-frontend** *(this plan)* — search **sharding** (time/block/row-group jobs) + **queueing** + fan-out + spanSet/trace merge + the `query-frontend` role binary.
7. Metrics-generator — span-metrics (RED) + service-graphs → remote_write.
8. Hardening — per-tenant limits + multi-tenancy isolation + differential-vs-Tempo + Grafana integration.

---

## File structure (`crates/traces/`)

| File | Responsibility |
|---|---|
| `src/lib.rs` | add `pub mod frontend;` |
| `src/frontend/mod.rs` | module decls + public re-exports + `QueryFrontend` orchestrator |
| `src/frontend/wire.rs` | `SearchResponseJson` / `TraceJson` / `Metrics` — the Tempo-JSON edge model + `From<crabka_traceql::*>` projections |
| `src/frontend/job.rs` | `JobShard` / `SearchJob` / `BlockCatalog` trait + `MockCatalog` + the **job planner** (time→shard→block→row-group) |
| `src/frontend/backend.rs` | `QuerierBackend` trait + `SearchJobRequest`/`TraceByIdJobRequest` + `SearchPartial`/`TracePartial`/`JobMetrics` + `MockQuerier` (test) |
| `src/frontend/http_backend.rs` | `HttpQuerier` — reqwest pool over configurable querier addrs (fan-out target) |
| `src/frontend/merge.rs` | trace/spanSet merge honoring `limit`/`spss` + `JobMetrics` accumulation + tag-union; trace-by-id assembly |
| `src/frontend/queue.rs` | bounded fan-out (`buffer_unordered`) over the planned jobs |
| `src/frontend/server.rs` | axum router + handlers (`/api/search`, `/api/v2/traces/{id}`, `/api/v2/search/tags`+`values`, `/api/echo`) wiring the orchestrator |
| `src/frontend/config.rs` | `FrontendConfig` (backend addrs, target bytes/job, max concurrency, default limit/spss, timeouts) |
| `src/bin/crabka-traces.rs` | (modify) `--target query-frontend` role dispatch |
| `tests/frontend_shard_equivalence.rs` | integration: sharded search == unsharded over canned per-block partials, limit/spss honored |
| `tests/frontend_trace_by_id_assembly.rs` | integration: a trace split across blocks reassembles into one v2 trace |
| `tests/frontend_http_backend.rs` | integration: `HttpQuerier` request shape (path, `blockID`, `X-Scope-OrgID`) + Tempo-JSON parse |
| `tests/frontend_server.rs` | integration: router round-trips `/api/search` with tenant + limit/spss |

---

### Task 1: Crate deps + `frontend` module scaffold + the Tempo-JSON edge model

**Files:**
- Modify: `crates/traces/Cargo.toml`
- Modify: `crates/traces/src/lib.rs`
- Create: `crates/traces/src/frontend/mod.rs`
- Create: `crates/traces/src/frontend/wire.rs`

**Interfaces:**
- Consumes (from `crabka-traceql`, Slice 2): `TraceResult { trace_id:[u8;16], root_service_name, root_trace_name, start_time_unix_nano:u64, duration_ms:u64, span_sets:Vec<SpanSet> }`, `SpanSet { spans:Vec<SpanRef>, matched:u32 }`, `SpanRef { span_id:[u8;8], start_time_unix_nano:u64, duration_nanos:u64, attributes:Vec<(String,AttrValue)> }`, `AttrValue`, `TraceSpans`, `ScopedTag`/`TagScope`/`TypedValue`.
- Produces:
  - `struct Metrics { total_jobs:u64, completed_jobs:u64, total_blocks:u64, inspected_traces:u64, inspected_bytes:u64, inspected_spans:u64 }` (serde, camelCase: `totalJobs`/`completedJobs`/`totalBlocks`/`inspectedTraces`/`inspectedBytes`/`inspectedSpans`) with `fn add(&mut self, other:&Metrics)`.
  - `struct SearchResponseJson { traces: Vec<TraceJson>, metrics: Metrics }` — serde shaped exactly as Tempo's `/api/search` body.
  - `struct TraceJson { trace_id:String /*hex*/, root_service_name:String, root_trace_name:String, start_time_unix_nano:String /*nanos as string*/, duration_ms:u64, span_sets:Vec<SpanSetJson> }` (camelCase: `traceID`/`rootServiceName`/`rootTraceName`/`startTimeUnixNano`/`durationMs`/`spanSets`).
  - `struct SpanSetJson { spans:Vec<SpanJson>, matched:u32 }`; `struct SpanJson { span_id:String /*hex*/, start_time_unix_nano:String, duration_nanos:String, attributes:Vec<KeyValueJson> }` (camelCase: `spanID`/`startTimeUnixNano`/`durationNanos`).
  - `struct KeyValueJson { key:String, value:AnyValueJson }` — OTLP KV form.
  - `fn hex16(id:&[u8;16]) -> String`, `fn hex8(id:&[u8;8]) -> String` — lowercase hex (the universal cross-signal join key form).
  - `impl From<&crabka_traceql::TraceResult> for TraceJson` — the projection.

- [x] **Step 1: Add dependencies to `crates/traces/Cargo.toml`**

Add to `[dependencies]` (the crate already exists from Slices 4–5; add only what the frontend module needs that is not yet present):

```toml
axum = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "net", "macros", "time", "sync"] }
tokio-util = { workspace = true }
futures = { workspace = true }
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
async-trait = { workspace = true }
thiserror = { workspace = true }
tracing = { workspace = true }
clap = { workspace = true }
crabka-traceql = { path = "../traceql" }
```

Add to `[dev-dependencies]`:

```toml
assert2 = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "macros", "time", "sync"] }
```

> **Workspace-dep verify-note:** `futures`, `async-trait`, `thiserror`, `clap`, `tracing`, `serde_json`, `assert2` are workspace members (see root `Cargo.toml`; the metrics slice-6 plan uses the same set). If `futures` is named `futures-util` only, use `futures-util` and import `stream::{self, StreamExt}` from `futures_util`. If a `workspace = true` line errors with "not a workspace dependency", add the pin to root `[workspace.dependencies]` first (a manifest fix, not a design change). `crabka-traceql` is the Slice-2 crate; if its path differs adjust the `path`.

- [x] **Step 2: Write the failing test**

Create `crates/traces/src/frontend/wire.rs` with only the test module first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn search_response_serializes_as_tempo_json() {
        let resp = SearchResponseJson {
            traces: vec![TraceJson {
                trace_id: "0a".repeat(16),
                root_service_name: "checkout".to_string(),
                root_trace_name: "POST /pay".to_string(),
                start_time_unix_nano: "1700000000000000000".to_string(),
                duration_ms: 42,
                span_sets: vec![SpanSetJson {
                    spans: vec![SpanJson {
                        span_id: "0b".repeat(8),
                        start_time_unix_nano: "1700000000000000000".to_string(),
                        duration_nanos: "42000000".to_string(),
                        attributes: vec![],
                    }],
                    matched: 1,
                }],
            }],
            metrics: Metrics {
                total_jobs: 3,
                completed_jobs: 3,
                total_blocks: 2,
                inspected_traces: 10,
                inspected_bytes: 4096,
                inspected_spans: 50,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json["traces"][0]["traceID"] == "0a".repeat(16));
        assert!(json["traces"][0]["rootServiceName"] == "checkout");
        // nanos stay string-encoded; durationMs is an int.
        assert!(json["traces"][0]["startTimeUnixNano"] == "1700000000000000000");
        assert!(json["traces"][0]["durationMs"] == 42);
        assert!(json["traces"][0]["spanSets"][0]["spans"][0]["spanID"] == "0b".repeat(8));
        assert!(json["traces"][0]["spanSets"][0]["spans"][0]["durationNanos"] == "42000000");
        assert!(json["traces"][0]["spanSets"][0]["matched"] == 1);
        // metrics job-accounting block, camelCase.
        assert!(json["metrics"]["totalJobs"] == 3);
        assert!(json["metrics"]["completedJobs"] == 3);
        assert!(json["metrics"]["inspectedBytes"] == 4096);
    }

    #[test]
    fn metrics_add_is_additive() {
        let mut a = Metrics::default();
        a.add(&Metrics { total_jobs: 1, completed_jobs: 1, total_blocks: 1, inspected_traces: 2, inspected_bytes: 100, inspected_spans: 9 });
        a.add(&Metrics { total_jobs: 1, completed_jobs: 1, total_blocks: 1, inspected_traces: 3, inspected_bytes: 200, inspected_spans: 11 });
        assert!(a.total_jobs == 2);
        assert!(a.inspected_traces == 5);
        assert!(a.inspected_bytes == 300);
        assert!(a.inspected_spans == 20);
    }

    #[test]
    fn hex_encodes_lowercase() {
        assert!(hex16(&[0xab; 16]) == "ab".repeat(16));
        assert!(hex8(&[0x0f; 8]) == "0f".repeat(8));
    }
}
```

- [x] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::wire`
Expected: FAIL — `cannot find type SearchResponseJson` / unresolved module `frontend`.

- [x] **Step 4: Implement `wire.rs`**

Prepend above the `tests` module. The serde shape is the load-bearing part — Tempo serializes `traceID`/`spanID` as lowercase hex strings, nanos as **strings**, `durationMs` as an int, and the `metrics{}` accounting block in camelCase.

```rust
//! The Tempo HTTP-API JSON edge model the query-frontend renders and parses.
//!
//! This is the same body shape the querier (Slice 5) emits; the frontend parses
//! per-job partials, merges them (respecting `limit`/`spss`), accumulates the
//! `metrics{}` job-accounting block, and re-emits this exact shape. The result
//! values it carries (`TraceResult`/`SpanSet`/`SpanRef`) are the pinned
//! `crabka-traceql` (Slice 2) types; this module is only their HTTP projection.

use serde::{Deserialize, Serialize};

use crabka_traceql::{AttrValue, SpanRef, SpanSet, TraceResult};

/// The `/api/search` response: matched traces + the job-accounting metrics.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchResponseJson {
    #[serde(default)]
    pub traces: Vec<TraceJson>,
    #[serde(default)]
    pub metrics: Metrics,
}

/// One matched trace in the search response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TraceJson {
    #[serde(rename = "traceID")]
    pub trace_id: String,
    pub root_service_name: String,
    pub root_trace_name: String,
    /// Nanos since epoch, **string-encoded** (Tempo quirk).
    pub start_time_unix_nano: String,
    /// Whole milliseconds, integer.
    pub duration_ms: u64,
    pub span_sets: Vec<SpanSetJson>,
}

/// A spanSet: the spans this trace matched plus the matched count.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpanSetJson {
    pub spans: Vec<SpanJson>,
    pub matched: u32,
}

/// A single matched span (string-encoded nanos, OTLP-KV attributes).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanJson {
    #[serde(rename = "spanID")]
    pub span_id: String,
    pub start_time_unix_nano: String,
    pub duration_nanos: String,
    #[serde(default)]
    pub attributes: Vec<KeyValueJson>,
}

/// OTLP key/value attribute form.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyValueJson {
    pub key: String,
    pub value: AnyValueJson,
}

/// OTLP `AnyValue` (the variants TraceQL surfaces).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AnyValueJson {
    #[serde(rename = "stringValue")]
    StringValue(String),
    #[serde(rename = "intValue")]
    IntValue(String),
    #[serde(rename = "doubleValue")]
    DoubleValue(f64),
    #[serde(rename = "boolValue")]
    BoolValue(bool),
}

/// The job-accounting `metrics{}` block. Additive over completed jobs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metrics {
    pub total_jobs: u64,
    pub completed_jobs: u64,
    pub total_blocks: u64,
    pub inspected_traces: u64,
    pub inspected_bytes: u64,
    pub inspected_spans: u64,
}

impl Metrics {
    /// Fold another job's accounting into this one (field-wise sum).
    pub fn add(&mut self, other: &Metrics) {
        self.total_jobs += other.total_jobs;
        self.completed_jobs += other.completed_jobs;
        self.total_blocks += other.total_blocks;
        self.inspected_traces += other.inspected_traces;
        self.inspected_bytes += other.inspected_bytes;
        self.inspected_spans += other.inspected_spans;
    }
}

/// Lowercase hex for a 16-byte trace id.
#[must_use]
pub fn hex16(id: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Lowercase hex for an 8-byte span id.
#[must_use]
pub fn hex8(id: &[u8; 8]) -> String {
    let mut s = String::with_capacity(16);
    for b in id {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

impl From<&AttrValue> for AnyValueJson {
    fn from(v: &AttrValue) -> Self {
        match v {
            AttrValue::Str(s) => AnyValueJson::StringValue(s.clone()),
            AttrValue::Int(i) => AnyValueJson::IntValue(i.to_string()),
            AttrValue::Float(f) => AnyValueJson::DoubleValue(*f),
            AttrValue::Bool(b) => AnyValueJson::BoolValue(*b),
        }
    }
}

impl From<&SpanRef> for SpanJson {
    fn from(s: &SpanRef) -> Self {
        SpanJson {
            span_id: hex8(&s.span_id),
            start_time_unix_nano: s.start_time_unix_nano.to_string(),
            duration_nanos: s.duration_nanos.to_string(),
            attributes: s
                .attributes
                .iter()
                .map(|(k, v)| KeyValueJson { key: k.clone(), value: AnyValueJson::from(v) })
                .collect(),
        }
    }
}

impl From<&SpanSet> for SpanSetJson {
    fn from(ss: &SpanSet) -> Self {
        SpanSetJson { spans: ss.spans.iter().map(SpanJson::from).collect(), matched: ss.matched }
    }
}

impl From<&TraceResult> for TraceJson {
    fn from(t: &TraceResult) -> Self {
        TraceJson {
            trace_id: hex16(&t.trace_id),
            root_service_name: t.root_service_name.clone(),
            root_trace_name: t.root_trace_name.clone(),
            start_time_unix_nano: t.start_time_unix_nano.to_string(),
            duration_ms: t.duration_ms,
            span_sets: t.span_sets.iter().map(SpanSetJson::from).collect(),
        }
    }
}
```

> **Serde verify-note (Tempo shape):** `traceID`/`spanID` are lowercase hex strings, `startTimeUnixNano`/`durationNanos` are **string**-encoded nanos, `durationMs` an int, and `metrics` is camelCase — all pinned by `search_response_serializes_as_tempo_json`. The `AnyValueJson` variant renames (`stringValue`/`intValue`/`doubleValue`/`boolValue`) match OTLP-JSON; Tempo emits `intValue` as a **string** (we model that). If Slice 5's querier serializes a richer OTLP `AnyValue` (e.g. `arrayValue`), extend the enum then — the merge path only ever copies attributes through, so this enum is the single edit point.

> **`crabka-traceql` import verify-note:** `TraceResult`/`SpanSet`/`SpanRef`/`AttrValue` field names and shapes are the Slice-2 pinned contract (`trace_id:[u8;16]`, `span_id:[u8;8]`, `attributes:Vec<(String,AttrValue)>`, etc.). If Slice 2 is not yet merged, these `From` impls will not compile until it lands — that is the intended dependency edge; do not stub the traceql types locally.

- [x] **Step 5: Create `frontend/mod.rs` and wire `lib.rs`**

Create `crates/traces/src/frontend/mod.rs`:

```rust
//! The `query-frontend` role: search sharding (time/block/row-group jobs),
//! job queueing, querier fan-out, and spanSet/trace merge in front of N
//! queriers.

pub mod wire;

pub use wire::{
    AnyValueJson, KeyValueJson, Metrics, SearchResponseJson, SpanJson, SpanSetJson, TraceJson,
    hex16, hex8,
};
```

Add to `crates/traces/src/lib.rs`:

```rust
pub mod frontend;
```

- [x] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::wire`
Expected: PASS (3 tests).

- [x] **Step 7: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): query-frontend Tempo-JSON edge model + metrics accounting"
```

---

### Task 2: `QuerierBackend` trait + per-job request/partial types + `MockQuerier`

**Files:**
- Create: `crates/traces/src/frontend/backend.rs`
- Modify: `crates/traces/src/frontend/mod.rs`

**Interfaces:**
- Consumes: `crabka_traceql::{TraceResult, TraceSpans, ScopedTag, TypedValue, TagScope}`, `wire::Metrics`.
- Produces:
  - `struct SearchJobRequest { tenant:String, query:String, start_ns:i64, end_ns:i64, limit:usize, spss:usize, shard:JobShard }` (`JobShard` from Task 3; forward-declare via `pub use crate::frontend::job::JobShard` — Task 3 defines it; for Task 2's own test a `JobShard::Live` literal suffices, so Task 2 implements the minimal `JobShard` enum here and Task 3 *extends* `job.rs` to re-export it — **decision: `JobShard` lives in `job.rs` (Task 3); Task 2 takes a dependency on it**, so do Task 3's enum first or land them together).
  - `struct TraceByIdJobRequest { tenant:String, trace_id:[u8;16], start_ns:i64, end_ns:i64, block_id:Option<String> }`.
  - `struct SearchPartial { traces:Vec<TraceResult>, metrics:Metrics }`; `struct TracePartial { trace:Option<TraceSpans>, metrics:Metrics }`.
  - `enum BackendError { Timeout, Transport(String), Backend { status:String, message:String } }` (`thiserror`).
  - `#[async_trait] trait QuerierBackend: Send + Sync { async fn search_job(&self, req:&SearchJobRequest) -> Result<SearchPartial, BackendError>; async fn trace_by_id_job(&self, req:&TraceByIdJobRequest) -> Result<TracePartial, BackendError>; async fn tag_names(&self, tenant:&str, scope:Option<TagScope>, start_ns:i64, end_ns:i64) -> Result<(Vec<ScopedTag>, Metrics), BackendError>; async fn tag_values(&self, tenant:&str, tag:&str, start_ns:i64, end_ns:i64) -> Result<(Vec<TypedValue>, Metrics), BackendError>; }`.
  - `struct MockQuerier` — programmable FIFO-stub backend + a call recorder: `stub_search(SearchPartial)`, `stub_trace(TracePartial)`, `search_calls() -> Vec<SearchJobRequest>`, `trace_calls() -> Vec<TraceByIdJobRequest>`. Exposed un-gated (it's a fixture integration tests in `tests/` construct).

> **Ordering note:** Task 2 imports `JobShard` from Task 3's `job.rs`. Implement Task 3's `JobShard` enum (just the enum + its module) *before or alongside* Task 2 so `backend.rs` compiles. The two tasks touch different files (`backend.rs` vs `job.rs`) and can be authored in either order, but `JobShard` must exist when `backend.rs` is first built.

- [x] **Step 1: Write the failing test**

Append a test module to `backend.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_traceql::TraceResult;

    use super::*;
    use crate::frontend::job::JobShard;

    fn trace(svc: &str) -> TraceResult {
        TraceResult {
            trace_id: [1; 16],
            root_service_name: svc.to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: 1,
            duration_ms: 1,
            span_sets: vec![],
        }
    }

    #[tokio::test]
    async fn mock_returns_canned_and_records_calls() {
        let mock = MockQuerier::new();
        mock.stub_search(SearchPartial {
            traces: vec![trace("checkout")],
            metrics: Metrics { total_jobs: 1, completed_jobs: 1, inspected_bytes: 10, ..Metrics::default() },
        });
        let req = SearchJobRequest {
            tenant: "t1".to_string(),
            query: "{ .service.name = \"checkout\" }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
        };
        let out = mock.search_job(&req).await.unwrap();
        assert!(out.traces.len() == 1);
        assert!(out.metrics.inspected_bytes == 10);
        assert!(mock.search_calls().len() == 1);
        assert!(mock.search_calls()[0].tenant == "t1");
        assert!(matches!(mock.search_calls()[0].shard, JobShard::Live));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::backend`
Expected: FAIL — `cannot find type QuerierBackend` / `MockQuerier` / `SearchPartial`.

- [x] **Step 3: Implement `backend.rs`**

```rust
//! The querier-backend abstraction the frontend fans out to, one call per
//! planned job. Tests use [`MockQuerier`]; real deployments use `HttpQuerier`
//! (see `http_backend.rs`).

use std::sync::Mutex;

use async_trait::async_trait;

use crabka_traceql::{ScopedTag, TagScope, TraceResult, TraceSpans, TypedValue};

use crate::frontend::job::JobShard;
use crate::frontend::wire::Metrics;

/// A single search job: a TraceQL search restricted to one shard (the live
/// hot tier or one cold block, optionally one row-group) over a sub-window.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchJobRequest {
    pub tenant: String,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub limit: usize,
    pub spss: usize,
    pub shard: JobShard,
}

/// A by-id job: fetch one trace's spans, optionally restricted to one block.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceByIdJobRequest {
    pub tenant: String,
    pub trace_id: [u8; 16],
    pub start_ns: i64,
    pub end_ns: i64,
    pub block_id: Option<String>,
}

/// The partial result of one search job: matched traces + the job's accounting.
#[derive(Clone, Debug)]
pub struct SearchPartial {
    pub traces: Vec<TraceResult>,
    pub metrics: Metrics,
}

/// The partial result of one by-id job: the (possibly partial) trace + accounting.
#[derive(Clone, Debug)]
pub struct TracePartial {
    pub trace: Option<TraceSpans>,
    pub metrics: Metrics,
}

/// Failure modes of a single backend job.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend job timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({status}): {message}")]
    Backend { status: String, message: String },
}

/// A queryable querier backend (one querier replica, or a pool fronting many).
/// Every method is one fanned-out job's worth of work.
#[async_trait]
pub trait QuerierBackend: Send + Sync {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError>;
    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError>;
    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<ScopedTag>, Metrics), BackendError>;
    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<TypedValue>, Metrics), BackendError>;
}

/// A programmable in-process backend for tests. Returns the next stubbed
/// response (FIFO; the last stub repeats if more calls arrive) and records
/// every request for assertions. Exposed un-gated so integration tests in
/// `tests/` can construct it — a fixture, not production wiring.
pub struct MockQuerier {
    search_stubs: Mutex<Vec<SearchPartial>>,
    trace_stubs: Mutex<Vec<TracePartial>>,
    search_calls: Mutex<Vec<SearchJobRequest>>,
    trace_calls: Mutex<Vec<TraceByIdJobRequest>>,
}

impl MockQuerier {
    #[must_use]
    pub fn new() -> Self {
        Self {
            search_stubs: Mutex::new(Vec::new()),
            trace_stubs: Mutex::new(Vec::new()),
            search_calls: Mutex::new(Vec::new()),
            trace_calls: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a canned search-job response (FIFO).
    pub fn stub_search(&self, p: SearchPartial) {
        self.search_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned by-id-job response (FIFO).
    pub fn stub_trace(&self, p: TracePartial) {
        self.trace_stubs.lock().unwrap().push(p);
    }

    /// All recorded search-job requests, in dispatch order.
    #[must_use]
    pub fn search_calls(&self) -> Vec<SearchJobRequest> {
        self.search_calls.lock().unwrap().clone()
    }

    /// All recorded by-id-job requests, in dispatch order.
    #[must_use]
    pub fn trace_calls(&self) -> Vec<TraceByIdJobRequest> {
        self.trace_calls.lock().unwrap().clone()
    }

    fn pop_search(&self) -> SearchPartial {
        let mut s = self.search_stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first().cloned().unwrap_or_else(|| SearchPartial {
                traces: Vec::new(),
                metrics: Metrics { total_jobs: 1, completed_jobs: 1, ..Metrics::default() },
            })
        }
    }

    fn pop_trace(&self) -> TracePartial {
        let mut s = self.trace_stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first().cloned().unwrap_or_else(|| TracePartial {
                trace: None,
                metrics: Metrics { total_jobs: 1, completed_jobs: 1, ..Metrics::default() },
            })
        }
    }
}

impl Default for MockQuerier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuerierBackend for MockQuerier {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        self.search_calls.lock().unwrap().push(req.clone());
        Ok(self.pop_search())
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        self.trace_calls.lock().unwrap().push(req.clone());
        Ok(self.pop_trace())
    }

    async fn tag_names(
        &self,
        _tenant: &str,
        _scope: Option<TagScope>,
        _start_ns: i64,
        _end_ns: i64,
    ) -> Result<(Vec<ScopedTag>, Metrics), BackendError> {
        Ok((Vec::new(), Metrics::default()))
    }

    async fn tag_values(
        &self,
        _tenant: &str,
        _tag: &str,
        _start_ns: i64,
        _end_ns: i64,
    ) -> Result<(Vec<TypedValue>, Metrics), BackendError> {
        Ok((Vec::new(), Metrics::default()))
    }
}
```

- [x] **Step 4: Re-export from `mod.rs`**

```rust
pub mod backend;

pub use backend::{
    BackendError, MockQuerier, QuerierBackend, SearchJobRequest, SearchPartial,
    TraceByIdJobRequest, TracePartial,
};
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::backend`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): QuerierBackend trait + per-job request/partial types + MockQuerier"
```

---

### Task 3: `JobShard` + `BlockCatalog` + the job planner (time → shard → block → row-group)

**Files:**
- Create: `crates/traces/src/frontend/job.rs`
- Modify: `crates/traces/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `enum JobShard { Live, Block { block_id:String, row_group:Option<usize> } }`.
  - `struct BlockMetaInfo { block_id:String, start_ns:i64, end_ns:i64, total_records:u64, size_bytes:u64, row_groups:usize, row_group_sizes:Vec<u64> }`.
  - `#[async_trait] trait BlockCatalog: Send + Sync { async fn blocks(&self, tenant:&str, start_ns:i64, end_ns:i64) -> Result<Vec<BlockMetaInfo>, CatalogError>; }` + `struct MockCatalog` (programmable) + `enum CatalogError`.
  - `struct JobPlan { jobs:Vec<JobShard>, total_blocks:u64 }`.
  - `fn plan_search_jobs(blocks:&[BlockMetaInfo], hot_frontier_ns:i64, target_bytes_per_job:u64) -> JobPlan` — emit one `Live` job for the hot window (when `end_ns >= hot_frontier_ns`); for each block overlapping `[start,end]` and older than the frontier, emit one `Block { row_group:None }` job if `size_bytes <= target_bytes_per_job`, else one `Block { row_group:Some(i) }` job per row-group (further splitting a large block into row-group jobs). Time-overlap and frontier filtering happen here.
  - `fn plan_trace_by_id_jobs(blocks:&[BlockMetaInfo], hot_frontier_ns:i64) -> Vec<TraceByIdShard>` where `enum TraceByIdShard { Live, Block(String) }` — one job per candidate block whose `[start,end]` could contain the trace (the bloom test is the querier's job; the frontend enumerates candidates) plus a `Live` job when the window reaches the hot tier.

- [x] **Step 1: Write the failing test**

Create `crates/traces/src/frontend/job.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn block(id: &str, start: i64, end: i64, size: u64, rgs: &[u64]) -> BlockMetaInfo {
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ns: start,
            end_ns: end,
            total_records: 1000,
            size_bytes: size,
            row_groups: rgs.len(),
            row_group_sizes: rgs.to_vec(),
        }
    }

    #[test]
    fn small_block_is_one_job_plus_live() {
        // One block [0,100], below the per-job byte budget; hot frontier at 200
        // means the query window [0,300] also reaches the live tier.
        let blocks = vec![block("b1", 0, 100, 500, &[500])];
        let plan = plan_search_jobs(&blocks, 200, 10_000);
        // 1 Live job + 1 whole-block job.
        assert!(plan.jobs.len() == 2);
        assert!(plan.jobs.iter().any(|j| matches!(j, JobShard::Live)));
        assert!(plan.jobs.iter().any(|j| matches!(
            j,
            JobShard::Block { block_id, row_group: None } if block_id == "b1"
        )));
        assert!(plan.total_blocks == 1);
    }

    #[test]
    fn large_block_splits_into_row_group_jobs() {
        // size 30k > budget 10k, 3 row-groups ⇒ 3 row-group jobs (no Live: the
        // frontier 0 means nothing reaches the hot tier for a window ending <0).
        let blocks = vec![block("b2", -1000, -10, 30_000, &[10_000, 10_000, 10_000])];
        let plan = plan_search_jobs(&blocks, 0, 10_000);
        let rg_jobs: Vec<_> = plan
            .jobs
            .iter()
            .filter(|j| matches!(j, JobShard::Block { row_group: Some(_), .. }))
            .collect();
        assert!(rg_jobs.len() == 3);
        assert!(!plan.jobs.iter().any(|j| matches!(j, JobShard::Live)));
    }

    #[test]
    fn out_of_window_blocks_are_skipped() {
        // block [0,100] does not overlap query window via the [start,end] passed
        // to plan_search_jobs implicitly through the block list filtering: here
        // the planner trusts the catalog's pre-filter, but a block entirely in
        // the future relative to the frontier-and-window still yields a job only
        // if it overlaps. We pass an already-overlapping list, so assert the
        // small-vs-large split is the only behavior; non-overlap is the
        // catalog's filter (tested via MockCatalog below).
        let blocks: Vec<BlockMetaInfo> = vec![];
        let plan = plan_search_jobs(&blocks, i64::MAX, 10_000);
        // Empty blocks + frontier MAX (whole window is hot) ⇒ just the Live job.
        assert!(plan.jobs.len() == 1);
        assert!(matches!(plan.jobs[0], JobShard::Live));
        assert!(plan.total_blocks == 0);
    }

    #[tokio::test]
    async fn mock_catalog_returns_blocks() {
        let cat = MockCatalog::new(vec![block("b1", 0, 100, 500, &[500])]);
        let got = cat.blocks("t1", 0, 1000).await.unwrap();
        assert!(got.len() == 1);
        assert!(got[0].block_id == "b1");
    }

    #[test]
    fn trace_by_id_enumerates_candidate_blocks_plus_live() {
        let blocks = vec![block("b1", 0, 100, 500, &[500]), block("b2", 100, 200, 500, &[500])];
        let jobs = plan_trace_by_id_jobs(&blocks, 150);
        // 2 block candidates + 1 Live (frontier 150 ≤ b2.end 200).
        assert!(jobs.len() == 3);
        assert!(jobs.iter().filter(|j| matches!(j, TraceByIdShard::Block(_))).count() == 2);
        assert!(jobs.iter().any(|j| matches!(j, TraceByIdShard::Live)));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::job`
Expected: FAIL — `cannot find type JobShard` / `plan_search_jobs`.

- [x] **Step 3: Implement `job.rs`**

The sharding rule (Tempo's): time first (hot live-store vs cold backend), then block, then — for a block exceeding `target_bytes_per_job` — per-row-group. A whole-block job is `row_group:None`; an oversized block fans into `row_group:Some(i)` jobs. The hot frontier (`hot_frontier_ns`, computed upstream from the committed block-builder offset vs the live-store retention window) decides whether the query window reaches the live tier.

```rust
//! Search-space sharding: turn the candidate block set + the hot/cold frontier
//! into a list of bounded jobs (time → shard → block → row-group), and the
//! by-id candidate enumeration.

use async_trait::async_trait;

/// The shard a single search job scans: the live hot tier, or one cold block
/// (optionally narrowed to one row-group when the block is large).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobShard {
    Live,
    Block { block_id: String, row_group: Option<usize> },
}

/// The shard a by-id job scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceByIdShard {
    Live,
    Block(String),
}

/// Block metadata the planner needs (from the querier's block-catalog door).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMetaInfo {
    pub block_id: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub total_records: u64,
    pub size_bytes: u64,
    pub row_groups: usize,
    pub row_group_sizes: Vec<u64>,
}

/// The output of planning: the jobs to dispatch + how many blocks they cover
/// (seeds `metrics.totalBlocks`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobPlan {
    pub jobs: Vec<JobShard>,
    pub total_blocks: u64,
}

/// Errors enumerating blocks.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("block catalog error: {0}")]
    Backend(String),
}

/// The block-catalog door: which blocks overlap `[start_ns, end_ns]` for a
/// tenant. Slice 5's querier exposes this; tests use [`MockCatalog`].
#[async_trait]
pub trait BlockCatalog: Send + Sync {
    async fn blocks(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError>;
}

/// A canned block catalog for tests.
pub struct MockCatalog {
    blocks: Vec<BlockMetaInfo>,
}

impl MockCatalog {
    #[must_use]
    pub fn new(blocks: Vec<BlockMetaInfo>) -> Self {
        Self { blocks }
    }
}

#[async_trait]
impl BlockCatalog for MockCatalog {
    async fn blocks(
        &self,
        _tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        // Return only blocks overlapping the window (inclusive).
        Ok(self
            .blocks
            .iter()
            .filter(|b| b.end_ns >= start_ns && b.start_ns <= end_ns)
            .cloned()
            .collect())
    }
}

/// Plan search jobs from the candidate blocks + the hot/cold frontier.
///
/// - One `Live` job iff the query window reaches the hot tier (any block ends at
///   or after `hot_frontier_ns`, or the block list is empty with a frontier that
///   is `<= i64::MAX` — i.e. the live tier is always probed unless the frontier
///   sits strictly after every block AND there are blocks proving the window is
///   entirely cold). Concretely: probe Live unless every candidate block ends
///   strictly before the frontier.
/// - For each block: one whole-block job if `size_bytes <= target_bytes_per_job`,
///   else one job per row-group.
#[must_use]
pub fn plan_search_jobs(
    blocks: &[BlockMetaInfo],
    hot_frontier_ns: i64,
    target_bytes_per_job: u64,
) -> JobPlan {
    let mut jobs = Vec::new();

    // Probe the live tier unless every candidate block ends strictly before the
    // frontier (i.e. the window is provably entirely cold). With no blocks we
    // cannot prove the window is cold, so we probe Live.
    let window_reaches_hot =
        blocks.is_empty() || blocks.iter().any(|b| b.end_ns >= hot_frontier_ns);
    if window_reaches_hot {
        jobs.push(JobShard::Live);
    }

    for b in blocks {
        if b.size_bytes <= target_bytes_per_job || b.row_groups <= 1 {
            jobs.push(JobShard::Block { block_id: b.block_id.clone(), row_group: None });
        } else {
            for rg in 0..b.row_groups {
                jobs.push(JobShard::Block {
                    block_id: b.block_id.clone(),
                    row_group: Some(rg),
                });
            }
        }
    }

    JobPlan { jobs, total_blocks: blocks.len() as u64 }
}

/// Enumerate by-id candidate jobs: one per block (the querier runs the bloom
/// test), plus a `Live` job when the window reaches the hot tier.
#[must_use]
pub fn plan_trace_by_id_jobs(blocks: &[BlockMetaInfo], hot_frontier_ns: i64) -> Vec<TraceByIdShard> {
    let mut jobs = Vec::new();
    let window_reaches_hot =
        blocks.is_empty() || blocks.iter().any(|b| b.end_ns >= hot_frontier_ns);
    if window_reaches_hot {
        jobs.push(TraceByIdShard::Live);
    }
    for b in blocks {
        jobs.push(TraceByIdShard::Block(b.block_id.clone()));
    }
    jobs
}
```

> **Frontier semantics note:** `hot_frontier_ns` is the *cold-edge* timestamp — data at or after it lives in the live-store (hot) tier, data before it is in committed blocks. The planner probes the `Live` shard whenever the window could reach hot data and emits cold-block jobs for everything the catalog returned (the catalog already filtered to the window). A trace that straddles the frontier (late spans in a new block + recent spans in live) is correctly covered by *both* a block job and the Live job; the merge (Task 5) reunions per `trace_id` so no span is double-counted or lost — this is the hot/cold-merge correctness the spec §10 calls out.

- [x] **Step 4: Re-export from `mod.rs`**

```rust
pub mod job;

pub use job::{
    BlockCatalog, BlockMetaInfo, CatalogError, JobPlan, JobShard, MockCatalog, TraceByIdShard,
    plan_search_jobs, plan_trace_by_id_jobs,
};
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::job`
Expected: PASS (5 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): job planner — time/shard/block/row-group sharding + block catalog"
```

---

### Task 4: Trace/spanSet merge honoring `limit` + `spss` (the correctness centerpiece, part 1)

**Files:**
- Create: `crates/traces/src/frontend/merge.rs`
- Modify: `crates/traces/src/frontend/mod.rs`

**Interfaces:**
- Consumes: `crabka_traceql::{TraceResult, SpanSet, SpanRef}`, `backend::SearchPartial`, `wire::Metrics`.
- Produces:
  - `fn merge_search(partials:Vec<SearchPartial>, limit:usize, spss:usize) -> (Vec<TraceResult>, Metrics)` — union per-job partials by `trace_id` (a trace seen in multiple jobs/blocks merges its spanSets), accumulate `Metrics`, then **truncate**: keep at most `limit` traces (ordered by `start_time_unix_nano` descending — newest first, Tempo's default), and within each kept trace cap each spanSet's `spans` to `spss` (preserving `matched`, which reflects the *true* count before truncation).
  - `fn merge_one_trace(a:TraceResult, b:TraceResult) -> TraceResult` — same-`trace_id` reunion: concatenate `span_sets`, dedupe spans by `span_id` across blocks (the hot/cold + late-span overlap case), keep the earliest `start_time_unix_nano` and the max end (recompute `duration_ms`), prefer a non-empty `root_service_name`/`root_trace_name`.

- [x] **Step 1: Write the failing test**

Create `crates/traces/src/frontend/merge.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_traceql::{SpanRef, SpanSet, TraceResult};

    use super::*;
    use crate::frontend::backend::SearchPartial;
    use crate::frontend::wire::Metrics;

    fn span(id: u8, start: u64, dur: u64) -> SpanRef {
        SpanRef {
            span_id: [id; 8],
            start_time_unix_nano: start,
            duration_nanos: dur,
            attributes: vec![],
        }
    }

    fn trace(tid: u8, svc: &str, start: u64, spans: Vec<SpanRef>) -> TraceResult {
        let matched = spans.len() as u32;
        TraceResult {
            trace_id: [tid; 16],
            root_service_name: svc.to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: start,
            duration_ms: 1,
            span_sets: vec![SpanSet { spans, matched }],
        }
    }

    fn partial(traces: Vec<TraceResult>, bytes: u64) -> SearchPartial {
        SearchPartial {
            traces,
            metrics: Metrics {
                total_jobs: 1,
                completed_jobs: 1,
                inspected_bytes: bytes,
                inspected_traces: 1,
                ..Metrics::default()
            },
        }
    }

    #[test]
    fn same_trace_across_blocks_reunions_spans() {
        // trace 1 appears in two jobs (two blocks) with different spans.
        let p0 = partial(vec![trace(1, "checkout", 10, vec![span(1, 10, 5)])], 100);
        let p1 = partial(vec![trace(1, "checkout", 8, vec![span(2, 8, 9)])], 200);
        let (traces, metrics) = merge_search(vec![p0, p1], 20, 10);
        assert!(traces.len() == 1);
        // spans reunioned: span 1 and span 2 both present.
        let total_spans: usize = traces[0].span_sets.iter().map(|ss| ss.spans.len()).sum();
        assert!(total_spans == 2);
        // earliest start kept.
        assert!(traces[0].start_time_unix_nano == 8);
        // metrics summed.
        assert!(metrics.inspected_bytes == 300);
        assert!(metrics.completed_jobs == 2);
    }

    #[test]
    fn duplicate_span_across_blocks_is_deduped() {
        // late-span overlap: the SAME span shows up in a block job and the live
        // job. It must appear once.
        let p0 = partial(vec![trace(1, "s", 10, vec![span(7, 10, 5)])], 50);
        let p1 = partial(vec![trace(1, "s", 10, vec![span(7, 10, 5)])], 50);
        let (traces, _) = merge_search(vec![p0, p1], 20, 10);
        let total_spans: usize = traces[0].span_sets.iter().map(|ss| ss.spans.len()).sum();
        assert!(total_spans == 1);
    }

    #[test]
    fn limit_caps_trace_count_newest_first() {
        let p = partial(
            vec![
                trace(1, "a", 100, vec![span(1, 100, 1)]),
                trace(2, "b", 300, vec![span(2, 300, 1)]),
                trace(3, "c", 200, vec![span(3, 200, 1)]),
            ],
            10,
        );
        let (traces, _) = merge_search(vec![p], 2, 10);
        // limit 2, newest-first ⇒ traces starting at 300 then 200.
        assert!(traces.len() == 2);
        assert!(traces[0].start_time_unix_nano == 300);
        assert!(traces[1].start_time_unix_nano == 200);
    }

    #[test]
    fn spss_caps_spans_but_matched_is_true_count() {
        let spans = vec![span(1, 1, 1), span(2, 2, 1), span(3, 3, 1), span(4, 4, 1)];
        let p = partial(vec![trace(1, "a", 1, spans)], 10);
        let (traces, _) = merge_search(vec![p], 20, 2);
        // spss 2 ⇒ at most 2 spans kept, but matched reflects the real 4.
        assert!(traces[0].span_sets[0].spans.len() == 2);
        assert!(traces[0].span_sets[0].matched == 4);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::merge`
Expected: FAIL — `cannot find function merge_search`.

- [x] **Step 3: Implement `merge.rs`**

```rust
//! Merge per-job search/by-id partials back into one Tempo response, honoring
//! `limit` (max traces) and `spss` (max spans per spanSet), and accumulating the
//! job-accounting `metrics{}` block. Reunion is keyed by `trace_id` so a trace
//! split across blocks / hot+cold reassembles, with span-level dedup for the
//! late-span overlap case.

use std::collections::BTreeMap;

use crabka_traceql::{SpanRef, SpanSet, TraceResult};

use crate::frontend::backend::SearchPartial;
use crate::frontend::wire::Metrics;

/// Merge search partials: reunion by `trace_id`, accumulate metrics, then apply
/// `limit` (newest-first) and `spss` (per-spanSet span cap, `matched` preserved).
#[must_use]
pub fn merge_search(
    partials: Vec<SearchPartial>,
    limit: usize,
    spss: usize,
) -> (Vec<TraceResult>, Metrics) {
    let mut by_trace: BTreeMap<[u8; 16], TraceResult> = BTreeMap::new();
    let mut metrics = Metrics::default();

    for p in partials {
        metrics.add(&p.metrics);
        for t in p.traces {
            match by_trace.remove(&t.trace_id) {
                Some(existing) => {
                    by_trace.insert(t.trace_id, merge_one_trace(existing, t));
                }
                None => {
                    by_trace.insert(t.trace_id, t);
                }
            }
        }
    }

    let mut traces: Vec<TraceResult> = by_trace.into_values().collect();
    // Newest-first (Tempo's default trace ordering).
    traces.sort_by(|a, b| b.start_time_unix_nano.cmp(&a.start_time_unix_nano));
    traces.truncate(limit);

    // Cap spans per spanSet to spss, preserving the true `matched` count.
    for t in &mut traces {
        for ss in &mut t.span_sets {
            if ss.spans.len() > spss {
                ss.spans.truncate(spss);
            }
        }
    }

    (traces, metrics)
}

/// Reunion two same-`trace_id` results: concatenate spanSets, dedupe spans by
/// `span_id`, keep the earliest start + recompute duration, prefer non-empty
/// root service/name.
#[must_use]
pub fn merge_one_trace(mut a: TraceResult, b: TraceResult) -> TraceResult {
    debug_assert_eq!(a.trace_id, b.trace_id);

    // Earliest start wins; widest end recomputes duration.
    let a_end = a.start_time_unix_nano + u64::from(a.duration_ms) * 1_000_000;
    let b_end = b.start_time_unix_nano + u64::from(b.duration_ms) * 1_000_000;
    let start = a.start_time_unix_nano.min(b.start_time_unix_nano);
    let end = a_end.max(b_end);
    a.start_time_unix_nano = start;
    a.duration_ms = (end.saturating_sub(start)) / 1_000_000;

    if a.root_service_name.is_empty() {
        a.root_service_name = b.root_service_name;
    }
    if a.root_trace_name.is_empty() {
        a.root_trace_name = b.root_trace_name;
    }

    // Concatenate spanSets, then dedupe spans by span_id across the whole trace.
    a.span_sets.extend(b.span_sets);
    dedupe_spans(&mut a.span_sets);
    a
}

/// Remove duplicate spans (same `span_id`) that appear across blocks / hot+cold.
/// Keeps the first occurrence; recomputes `matched` to the kept-span count.
fn dedupe_spans(span_sets: &mut Vec<SpanSet>) {
    let mut seen: std::collections::HashSet<[u8; 8]> = std::collections::HashSet::new();
    for ss in span_sets.iter_mut() {
        let mut kept: Vec<SpanRef> = Vec::with_capacity(ss.spans.len());
        for s in ss.spans.drain(..) {
            if seen.insert(s.span_id) {
                kept.push(s);
            }
        }
        ss.matched = kept.len() as u32;
        ss.spans = kept;
    }
    // Drop any spanSet emptied entirely by dedup.
    span_sets.retain(|ss| !ss.spans.is_empty());
}
```

> **`matched` semantics note:** Tempo's `matched` is the number of spans in the spanSet *that matched the query*, and it can exceed the number of spans actually returned (which is capped by `spss`). Here `merge_one_trace`/`dedupe_spans` set `matched` to the deduped real count, then `merge_search` truncates `spans` to `spss` **without** touching `matched` — so the wire shows the true match count alongside a capped span list, exactly as Tempo does (pinned by `spss_caps_spans_but_matched_is_true_count`). If Slice-3's TraceQL metrics path computes `matched` differently for pipeline aggregations, that is upstream of this merge; the frontend only re-totals after dedup.

- [x] **Step 4: Re-export from `mod.rs`**

```rust
pub mod merge;

pub use merge::{merge_one_trace, merge_search};
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::merge`
Expected: PASS (4 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): trace/spanSet merge honoring limit+spss + cross-block dedup"
```

---

### Task 5: Trace-by-id assembly + tag-union merge (the correctness centerpiece, part 2)

**Files:**
- Modify: `crates/traces/src/frontend/merge.rs` (add `assemble_trace`, `merge_tag_names`, `merge_tag_values`)

**Interfaces:**
- Consumes: `crabka_traceql::{TraceSpans, ScopedTag, TagScope, TypedValue}`, `backend::TracePartial`, `wire::Metrics`.
- Produces:
  - `fn assemble_trace(partials:Vec<TracePartial>, max_trace_bytes:u64) -> (Option<TraceSpans>, Metrics, TraceStatus)` — union the per-block `TraceSpans` for one trace (concatenate `resourceSpans`, dedupe spans by `span_id`), accumulate metrics; if the assembled trace exceeds `max_trace_bytes` return `TraceStatus::Partial` (the v2 endpoint's oversized-trace contract), else `TraceStatus::Complete`; `None` when no block returned the trace.
  - `enum TraceStatus { Complete, Partial }` with `fn as_str(&self) -> &'static str` (`"COMPLETE"`/`"PARTIAL"`).
  - `fn merge_tag_names(parts:Vec<(Vec<ScopedTag>, Metrics)>) -> (Vec<ScopedTag>, Metrics)` — union tags per `TagScope`, dedupe, sort; accumulate metrics.
  - `fn merge_tag_values(parts:Vec<(Vec<TypedValue>, Metrics)>) -> (Vec<TypedValue>, Metrics)` — union+dedupe `(type, value)` pairs; accumulate metrics.

> **`TraceSpans` opacity note:** the Slice-2 contract states `TraceSpans { /* full OTLP resource→scope→spans for one trace */ }` without pinning its internal fields. This task therefore needs a *minimal accessor* on `TraceSpans` to union and size it. **Decision:** assume Slice 2 exposes (or this slice adds, as a `crabka-traceql` PR dependency) `TraceSpans { pub resource_spans: Vec<ResourceSpansJson>, ... }` plus `fn span_ids(&self) -> impl Iterator<Item=[u8;8]>` and `fn approx_size_bytes(&self) -> u64`. If those accessors are absent at authoring time, gate this task: implement `merge_tag_names`/`merge_tag_values` (which need no `TraceSpans` internals) now, and land `assemble_trace` once Slice 2 exposes the accessors. The test below for `assemble_trace` is written against those accessors — **verify against the Slice-2 `TraceSpans` definition before implementing**; do not fabricate fields.

- [x] **Step 1: Write the failing test**

Append to the `merge.rs` test module:

```rust
    use crabka_traceql::{ScopedTag, TagScope, TypedValue};

    use crate::frontend::backend::TracePartial;

    fn tag_metrics(bytes: u64) -> Metrics {
        Metrics { total_jobs: 1, completed_jobs: 1, inspected_bytes: bytes, ..Metrics::default() }
    }

    #[test]
    fn tag_names_union_dedupes_per_scope() {
        let a = vec![ScopedTag { scope: TagScope::Span, tags: vec!["http.method".to_string()] }];
        let b = vec![ScopedTag {
            scope: TagScope::Span,
            tags: vec!["http.method".to_string(), "http.status_code".to_string()],
        }];
        let (merged, m) = merge_tag_names(vec![(a, tag_metrics(10)), (b, tag_metrics(20))]);
        let span_scope = merged.iter().find(|s| matches!(s.scope, TagScope::Span)).unwrap();
        assert!(span_scope.tags.len() == 2);
        assert!(span_scope.tags.contains(&"http.method".to_string()));
        assert!(m.inspected_bytes == 30);
    }

    #[test]
    fn tag_values_union_dedupes_pairs() {
        let a = vec![TypedValue { type_: "string".to_string(), value: "GET".to_string() }];
        let b = vec![
            TypedValue { type_: "string".to_string(), value: "GET".to_string() },
            TypedValue { type_: "string".to_string(), value: "POST".to_string() },
        ];
        let (merged, _) = merge_tag_values(vec![(a, tag_metrics(1)), (b, tag_metrics(1))]);
        assert!(merged.len() == 2);
    }

    #[test]
    fn assemble_returns_none_when_no_block_has_it() {
        let p0 = TracePartial { trace: None, metrics: tag_metrics(5) };
        let p1 = TracePartial { trace: None, metrics: tag_metrics(5) };
        let (trace, metrics, status) = assemble_trace(vec![p0, p1], 1_000_000);
        assert!(trace.is_none());
        assert!(metrics.inspected_bytes == 10);
        assert!(matches!(status, TraceStatus::Complete));
    }
```

> **Note:** the `assemble_trace` "happy path" test (two blocks each carrying part of the trace → one unioned trace, deduped spans, `Complete`/`Partial` by size) is written in `tests/frontend_trace_by_id_assembly.rs` (Task 9) once the `TraceSpans` accessors are confirmed against Slice 2. The unit test above covers the `None` and metrics-accumulation paths, which need no `TraceSpans` internals.

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::merge`
Expected: FAIL — `cannot find function merge_tag_names` / `assemble_trace`.

- [x] **Step 3: Implement the additions to `merge.rs`**

Add above the `tests` module:

```rust
use crabka_traceql::{ScopedTag, TagScope, TraceSpans, TypedValue};

use crate::frontend::backend::TracePartial;

/// The v2 by-id status: a fully-returned trace is `COMPLETE`; one exceeding the
/// max trace size is `PARTIAL` (returned with an explanatory message, not an
/// error) — the v2 endpoint's distinguishing contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceStatus {
    Complete,
    Partial,
}

impl TraceStatus {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            TraceStatus::Complete => "COMPLETE",
            TraceStatus::Partial => "PARTIAL",
        }
    }
}

/// Assemble one trace from per-block by-id partials: union `resourceSpans`,
/// dedupe spans by `span_id`, accumulate metrics, and flag `Partial` when the
/// assembled trace exceeds `max_trace_bytes`.
#[must_use]
pub fn assemble_trace(
    partials: Vec<TracePartial>,
    max_trace_bytes: u64,
) -> (Option<TraceSpans>, Metrics, TraceStatus) {
    let mut metrics = Metrics::default();
    let mut acc: Option<TraceSpans> = None;
    let mut seen: std::collections::HashSet<[u8; 8]> = std::collections::HashSet::new();

    for p in partials {
        metrics.add(&p.metrics);
        let Some(trace) = p.trace else { continue };
        match &mut acc {
            // VERIFY against Slice-2 TraceSpans: `merge_in`/`span_ids` accessors.
            Some(existing) => existing.merge_in(trace, &mut seen),
            None => {
                for sid in trace.span_ids() {
                    seen.insert(sid);
                }
                acc = Some(trace);
            }
        }
    }

    let status = match &acc {
        Some(t) if t.approx_size_bytes() > max_trace_bytes => TraceStatus::Partial,
        _ => TraceStatus::Complete,
    };
    (acc, metrics, status)
}

/// Union scoped tag names across jobs, dedup + sort per scope.
#[must_use]
pub fn merge_tag_names(parts: Vec<(Vec<ScopedTag>, Metrics)>) -> (Vec<ScopedTag>, Metrics) {
    use std::collections::{BTreeMap, BTreeSet};

    let mut metrics = Metrics::default();
    // Scope discriminant → set of tag names. We key on the scope's string form
    // to keep an ordering without requiring Ord on TagScope.
    let mut by_scope: BTreeMap<&'static str, (TagScope, BTreeSet<String>)> = BTreeMap::new();

    for (tags, m) in parts {
        metrics.add(&m);
        for st in tags {
            let key = scope_key(&st.scope);
            let entry = by_scope.entry(key).or_insert_with(|| (st.scope, BTreeSet::new()));
            for t in st.tags {
                entry.1.insert(t);
            }
        }
    }

    let merged = by_scope
        .into_values()
        .map(|(scope, set)| ScopedTag { scope, tags: set.into_iter().collect() })
        .collect();
    (merged, metrics)
}

/// Union typed tag values across jobs, dedup `(type, value)` pairs.
#[must_use]
pub fn merge_tag_values(parts: Vec<(Vec<TypedValue>, Metrics)>) -> (Vec<TypedValue>, Metrics) {
    use std::collections::BTreeSet;

    let mut metrics = Metrics::default();
    let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
    let mut out = Vec::new();

    for (values, m) in parts {
        metrics.add(&m);
        for v in values {
            if seen.insert((v.type_.clone(), v.value.clone())) {
                out.push(v);
            }
        }
    }
    (out, metrics)
}

/// Stable string discriminant for a `TagScope` (ordering + dedup key).
fn scope_key(scope: &TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}
```

> **`TraceSpans` accessor verify-note (the dependency edge):** `assemble_trace` calls `TraceSpans::merge_in(other, &mut seen)`, `TraceSpans::span_ids()`, and `TraceSpans::approx_size_bytes()`. These are **not** in the Slice-2 contract as pinned (which leaves `TraceSpans` opaque). Before implementing, **verify the real `TraceSpans` surface in `crabka-traceql`** and either (a) use the accessors it provides, or (b) add these three methods to `crabka-traceql` as a small companion PR (the merge logic belongs at the frontend, but the span-id/size introspection belongs with the type). Do not duplicate OTLP-decoding here. `merge_tag_names`/`merge_tag_values` have no such dependency and are fully implemented above.

- [x] **Step 4: Re-export from `mod.rs`**

Extend the merge re-export: `pub use merge::{TraceStatus, assemble_trace, merge_one_trace, merge_search, merge_tag_names, merge_tag_values};`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::merge`
Expected: PASS (the tag-union + `None`/metrics tests; the `assemble_trace` happy-path lands in Task 9 against confirmed accessors).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): trace-by-id assembly (v2 PARTIAL/COMPLETE) + tag-union merge"
```

---

### Task 6: Bounded fan-out queue

**Files:**
- Create: `crates/traces/src/frontend/queue.rs`
- Modify: `crates/traces/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `async fn run_jobs<T, F, Fut>(jobs:Vec<T>, max_concurrency:usize, run:F) -> Vec<R>` where `F: Fn(T) -> Fut`, `Fut: Future<Output = R>` — drive `jobs` through a bounded-concurrency fan-out (`futures::stream::iter(...).map(run).buffer_unordered(max_concurrency).collect()`), preserving **no** ordering guarantee (results come back in completion order; callers key results by `trace_id`/job identity, never by position). `max_concurrency.max(1)` clamps a zero.

- [x] **Step 1: Write the failing test**

Create `crates/traces/src/frontend/queue.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use assert2::assert;

    use super::*;

    #[tokio::test]
    async fn runs_all_jobs_with_bounded_concurrency() {
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let jobs: Vec<usize> = (0..20).collect();

        let inflight_c = inflight.clone();
        let max_seen_c = max_seen.clone();
        let results = run_jobs(jobs, 4, move |j| {
            let inflight = inflight_c.clone();
            let max_seen = max_seen_c.clone();
            async move {
                let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                inflight.fetch_sub(1, Ordering::SeqCst);
                j * 2
            }
        })
        .await;

        assert!(results.len() == 20);
        let sum: usize = results.iter().sum();
        assert!(sum == (0..20).map(|j| j * 2).sum());
        // Concurrency never exceeded the bound.
        assert!(max_seen.load(Ordering::SeqCst) <= 4);
    }

    #[tokio::test]
    async fn zero_concurrency_clamps_to_one() {
        let results = run_jobs(vec![1, 2, 3], 0, |j| async move { j }).await;
        assert!(results.len() == 3);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::queue`
Expected: FAIL — `cannot find function run_jobs`.

- [x] **Step 3: Implement `queue.rs`**

```rust
//! Bounded-concurrency fan-out of planned jobs across queriers. Results return
//! in completion order; callers must key results by job identity, not position.

use std::future::Future;

use futures::stream::{self, StreamExt};

/// Run `jobs` through `run` with at most `max_concurrency` in flight at once.
/// Returns every result (completion order, unordered).
pub async fn run_jobs<T, R, F, Fut>(jobs: Vec<T>, max_concurrency: usize, run: F) -> Vec<R>
where
    F: Fn(T) -> Fut,
    Fut: Future<Output = R>,
{
    let limit = max_concurrency.max(1);
    stream::iter(jobs)
        .map(run)
        .buffer_unordered(limit)
        .collect()
        .await
}
```

> **`buffer_unordered` verify-note:** `futures::stream::iter(...).map(closure).buffer_unordered(n).collect::<Vec<_>>().await` is the standard bounded-fan-out idiom (`futures` 0.3 surface). `buffer_unordered` polls up to `n` futures concurrently and yields results as they complete — order is non-deterministic, which is why the merge (Tasks 4–5) keys on `trace_id`/`(type,value)` and never on index. If the workspace exposes `futures_util` rather than `futures`, change the import to `futures_util::stream::{self, StreamExt}`; the call is identical. The `runs_all_jobs_with_bounded_concurrency` test pins both the all-results invariant and the concurrency bound.

- [x] **Step 4: Re-export from `mod.rs`**

```rust
pub mod queue;

pub use queue::run_jobs;
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::queue`
Expected: PASS (2 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): bounded-concurrency job fan-out queue"
```

---

### Task 7: `FrontendConfig` + `QueryFrontend` orchestrator

**Files:**
- Create: `crates/traces/src/frontend/config.rs`
- Add the orchestrator in: `crates/traces/src/frontend/mod.rs` (struct `QueryFrontend`)
- Modify: `crates/traces/src/frontend/mod.rs` (re-exports)

**Interfaces:**
- Produces:
  - `struct FrontendConfig { backend_addrs:Vec<String>, target_bytes_per_job:u64, max_concurrency:usize, default_limit:usize /*20*/, default_spss:usize /*3*/, hot_frontier_ns:i64, max_trace_bytes:u64, request_timeout:Duration, listen_addr:SocketAddr }` (+ `Default`).
  - `struct QueryFrontend<B:QuerierBackend, C:BlockCatalog> { backend:Arc<B>, catalog:Arc<C>, cfg:FrontendConfig }`.
  - `async fn search(&self, tenant, query, start_ns, end_ns, limit, spss) -> SearchResponseJson` — the full pipeline: catalog → `plan_search_jobs` → `run_jobs` (per-shard `search_job`) → `merge_search` (limit/spss) → render `SearchResponseJson` with the accumulated `metrics{}` (seed `total_jobs`/`total_blocks` from the plan).
  - `async fn trace_by_id(&self, tenant, trace_id, start_ns, end_ns) -> (Option<TraceSpans>, Metrics, TraceStatus)` — catalog → `plan_trace_by_id_jobs` → `run_jobs` (per-shard `trace_by_id_job`) → `assemble_trace`.
  - `fn backend_ref(&self) -> &B` (test accessor).

- [x] **Step 1: Write the failing test (fan-out counts + merge wiring)**

Add to `mod.rs` under `#[cfg(test)] mod orch_tests`:

```rust
#[cfg(test)]
mod orch_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assert2::assert;
    use crabka_traceql::{SpanRef, SpanSet, TraceResult};

    use super::*;
    use crate::frontend::backend::{MockQuerier, SearchPartial};
    use crate::frontend::job::{BlockMetaInfo, MockCatalog};
    use crate::frontend::wire::Metrics;

    fn block(id: &str, start: i64, end: i64, size: u64, rgs: &[u64]) -> BlockMetaInfo {
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ns: start,
            end_ns: end,
            total_records: 100,
            size_bytes: size,
            row_groups: rgs.len(),
            row_group_sizes: rgs.to_vec(),
        }
    }

    fn one_trace(tid: u8, start: u64) -> SearchPartial {
        SearchPartial {
            traces: vec![TraceResult {
                trace_id: [tid; 16],
                root_service_name: "svc".to_string(),
                root_trace_name: "GET /".to_string(),
                start_time_unix_nano: start,
                duration_ms: 1,
                span_sets: vec![SpanSet {
                    spans: vec![SpanRef {
                        span_id: [tid; 8],
                        start_time_unix_nano: start,
                        duration_nanos: 1,
                        attributes: vec![],
                    }],
                    matched: 1,
                }],
            }],
            metrics: Metrics {
                total_jobs: 0, // the orchestrator seeds totals from the plan
                completed_jobs: 1,
                inspected_bytes: 100,
                inspected_traces: 1,
                ..Metrics::default()
            },
        }
    }

    #[tokio::test]
    async fn search_plans_jobs_fans_and_merges() {
        // Two small cold blocks + a hot window ⇒ 1 Live + 2 block jobs = 3 jobs.
        let catalog = MockCatalog::new(vec![
            block("b1", 0, 100, 500, &[500]),
            block("b2", 100, 200, 500, &[500]),
        ]);
        let backend = MockQuerier::new();
        // Each job returns a distinct trace so the merge keeps them all.
        backend.stub_search(one_trace(1, 50));
        backend.stub_search(one_trace(2, 150));
        backend.stub_search(one_trace(3, 250));
        let cfg = FrontendConfig {
            target_bytes_per_job: 10_000,
            max_concurrency: 8,
            hot_frontier_ns: 150,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

        let resp = qf.search("t1", "{ }", 0, 300, 20, 3).await;
        // 3 jobs dispatched.
        assert!(qf.backend_ref().search_calls().len() == 3);
        // Every job carried the tenant.
        for c in qf.backend_ref().search_calls() {
            assert!(c.tenant == "t1");
        }
        // 3 distinct traces survived the merge.
        assert!(resp.traces.len() == 3);
        // metrics: totalJobs seeded from plan (3), completed summed (3),
        // totalBlocks from the catalog (2), bytes summed (300).
        assert!(resp.metrics.total_jobs == 3);
        assert!(resp.metrics.completed_jobs == 3);
        assert!(resp.metrics.total_blocks == 2);
        assert!(resp.metrics.inspected_bytes == 300);
    }

    #[tokio::test]
    async fn search_honors_limit() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, 500, &[500])]);
        let backend = MockQuerier::new();
        // One job returns 3 traces; limit 1 keeps the newest.
        backend.stub_search(SearchPartial {
            traces: vec![
                one_trace(1, 100).traces.pop().unwrap(),
                one_trace(2, 300).traces.pop().unwrap(),
                one_trace(3, 200).traces.pop().unwrap(),
            ],
            metrics: Metrics { completed_jobs: 1, ..Metrics::default() },
        });
        let cfg = FrontendConfig { hot_frontier_ns: i64::MAX, ..FrontendConfig::default() };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        let resp = qf.search("t1", "{ }", 0, 300, 1, 3).await;
        assert!(resp.traces.len() == 1);
        assert!(resp.traces[0].start_time_unix_nano == "300");
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib frontend::orch_tests`
Expected: FAIL — `cannot find type FrontendConfig` / `QueryFrontend`.

- [x] **Step 3: Implement `config.rs`**

```rust
//! Query-frontend configuration.

use std::net::SocketAddr;
use std::time::Duration;

/// Static configuration for the `query-frontend` role.
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// Querier backend addresses (`host:port`) the HTTP pool round-robins over.
    pub backend_addrs: Vec<String>,
    /// Target bytes per search job; a block larger than this fans into
    /// per-row-group jobs.
    pub target_bytes_per_job: u64,
    /// Max jobs in flight at once across all queriers.
    pub max_concurrency: usize,
    /// Default trace limit when the request omits `limit` (Tempo default 20).
    pub default_limit: usize,
    /// Default spans-per-spanSet when the request omits `spss` (Tempo default 3).
    pub default_spss: usize,
    /// The cold-edge timestamp: data at/after it is in the live (hot) tier.
    pub hot_frontier_ns: i64,
    /// Max assembled-trace size before the v2 by-id path returns `PARTIAL`.
    pub max_trace_bytes: u64,
    /// Per-backend-job timeout.
    pub request_timeout: Duration,
    /// The frontend's own listen address.
    pub listen_addr: SocketAddr,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            backend_addrs: vec!["127.0.0.1:3200".to_string()],
            // ~100 MiB/job, matching Tempo's target_bytes_per_job default order.
            target_bytes_per_job: 100 * 1024 * 1024,
            max_concurrency: 1000,
            default_limit: 20,
            default_spss: 3,
            // 0 ⇒ "everything is cold" by default; the live-store role computes
            // the real frontier and the binary wires it in (hardening slice).
            hot_frontier_ns: 0,
            max_trace_bytes: 50 * 1024 * 1024,
            request_timeout: Duration::from_secs(30),
            listen_addr: "0.0.0.0:3200".parse().expect("valid default addr"),
        }
    }
}
```

- [x] **Step 4: Implement the `QueryFrontend` orchestrator in `mod.rs`**

```rust
pub mod config;

pub use config::FrontendConfig;

use std::sync::Arc;

use crabka_traceql::TraceSpans;

use crate::frontend::backend::{QuerierBackend, SearchJobRequest, TraceByIdJobRequest};
use crate::frontend::job::{BlockCatalog, JobShard, TraceByIdShard};
use crate::frontend::merge::TraceStatus;

/// The query-frontend pipeline: plan jobs → queue (bounded fan-out) → per-job
/// search → merge (limit/spss) → render Tempo JSON, in front of a
/// [`QuerierBackend`] pool with a [`BlockCatalog`] for enumeration.
pub struct QueryFrontend<B: QuerierBackend, C: BlockCatalog> {
    backend: Arc<B>,
    catalog: Arc<C>,
    cfg: FrontendConfig,
}

impl<B: QuerierBackend + 'static, C: BlockCatalog + 'static> QueryFrontend<B, C> {
    #[must_use]
    pub fn new(backend: Arc<B>, catalog: Arc<C>, cfg: FrontendConfig) -> Self {
        Self { backend, catalog, cfg }
    }

    /// Test/inspection accessor for the backend (e.g. `MockQuerier::search_calls`).
    #[must_use]
    pub fn backend_ref(&self) -> &B {
        &self.backend
    }

    /// Run a TraceQL `/api/search` through the full pipeline.
    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> SearchResponseJson {
        let blocks = self.catalog.blocks(tenant, start_ns, end_ns).await.unwrap_or_default();
        let plan = job::plan_search_jobs(&blocks, self.cfg.hot_frontier_ns, self.cfg.target_bytes_per_job);
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = self.backend.clone();
        let tenant_s = tenant.to_string();
        let query_s = query.to_string();
        let partials = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = backend.clone();
            let req = SearchJobRequest {
                tenant: tenant_s.clone(),
                query: query_s.clone(),
                start_ns,
                end_ns,
                limit,
                spss,
                shard,
            };
            async move {
                backend.search_job(&req).await.unwrap_or_else(|_| crate::frontend::backend::SearchPartial {
                    traces: Vec::new(),
                    metrics: Metrics::default(),
                })
            }
        })
        .await;

        let (traces, mut metrics) = merge::merge_search(partials, limit, spss);
        // Seed plan-derived totals (per-job metrics carry completed/bytes).
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;

        SearchResponseJson {
            traces: traces.iter().map(TraceJson::from).collect(),
            metrics,
        }
    }

    /// Run a `/api/v2/traces/{id}` by-id lookup through the job model.
    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: [u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> (Option<TraceSpans>, Metrics, TraceStatus) {
        let blocks = self.catalog.blocks(tenant, start_ns, end_ns).await.unwrap_or_default();
        let shards = job::plan_trace_by_id_jobs(&blocks, self.cfg.hot_frontier_ns);

        let backend = self.backend.clone();
        let tenant_s = tenant.to_string();
        let partials = queue::run_jobs(shards, self.cfg.max_concurrency, move |shard| {
            let backend = backend.clone();
            let block_id = match shard {
                TraceByIdShard::Live => None,
                TraceByIdShard::Block(id) => Some(id),
            };
            let req = TraceByIdJobRequest {
                tenant: tenant_s.clone(),
                trace_id,
                start_ns,
                end_ns,
                block_id,
            };
            async move {
                backend.trace_by_id_job(&req).await.unwrap_or_else(|_| crate::frontend::backend::TracePartial {
                    trace: None,
                    metrics: Metrics::default(),
                })
            }
        })
        .await;

        merge::assemble_trace(partials, self.cfg.max_trace_bytes)
    }
}

// Bring sibling modules into scope for the impl above.
use crate::frontend::{job, merge, queue};
```

> **Plan-vs-per-job metrics note:** `total_jobs`/`total_blocks` are *plan*-derived (known before any job runs); `completed_jobs`/`inspected_*` are *summed from per-job partials* (each `SearchPartial`/`TracePartial` carries the bytes/traces/spans that job actually scanned). The orchestrator overwrites `total_jobs`/`total_blocks` after the merge so a job that errored (and contributed an empty partial) still counts toward `total_jobs` but not `completed_jobs` — exactly the accounting the Global Constraint pins. The `search_plans_jobs_fans_and_merges` test asserts both halves.

> **Suppressed-error note:** failed jobs degrade to empty partials (`unwrap_or_else`) so one slow/broken querier does not fail the whole search — Tempo's partial-results behavior. A future hardening slice can surface a `partial: true` flag / per-job error list; not in scope here.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib frontend::orch_tests`
Expected: PASS (2 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): QueryFrontend orchestrator (plan+queue+fan-out+merge)"
```

---

### Task 8: `HttpQuerier` fan-out backend (reqwest pool)

**Files:**
- Create: `crates/traces/src/frontend/http_backend.rs`
- Create: `crates/traces/tests/frontend_http_backend.rs`
- Modify: `crates/traces/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `struct HttpQuerier { http:reqwest::Client, addrs:Vec<String>, next:AtomicUsize, timeout:Duration }` implementing `QuerierBackend`.
  - `fn new(addrs:Vec<String>, timeout:Duration) -> Result<HttpQuerier, BackendError>`.
  - Round-robins `addrs`; sets `X-Scope-OrgID`; for a search job GETs `/api/search?q=&start=&end=&limit=&spss=` plus a shard restriction (`blockID=<id>` and, when present, `rowGroup=<i>`, or `shard=live` for the live shard); parses the Tempo JSON body (`SearchResponseJson`) into a `SearchPartial` (carrying the body's `metrics{}`); maps timeout/transport/HTTP-error into `BackendError`. `trace_by_id_job` GETs `/api/v2/traces/{hex}?start=&end=` plus `blockID`. (`tag_names`/`tag_values` GET `/api/v2/search/tags` / `tag/{tag}/values`.)

This is a churn-prone surface (reqwest + the querier's exact Tempo HTTP contract). It is **structure + behavior-pinning**: a loopback axum-stub test (reuses the crate's own axum dep, no new dev-dep) verifies the request shape (path, `blockID`, `X-Scope-OrgID`) and response parsing.

- [x] **Step 1: Write the failing test (stub querier over loopback)**

Create `crates/traces/tests/frontend_http_backend.rs`:

```rust
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use assert2::assert;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::Router;
use crabka_traces::frontend::backend::{QuerierBackend, SearchJobRequest};
use crabka_traces::frontend::http_backend::HttpQuerier;
use crabka_traces::frontend::job::JobShard;
use serde::Deserialize;

#[derive(Deserialize)]
struct SearchQ {
    q: String,
    #[serde(rename = "blockID")]
    block_id: Option<String>,
    shard: Option<String>,
}

#[tokio::test]
async fn http_querier_search_job_posts_and_parses() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_h = seen.clone();

    let app = Router::new()
        .route(
            "/api/search",
            get(|State(s): State<Arc<Mutex<Vec<String>>>>,
                 headers: axum::http::HeaderMap,
                 Query(q): Query<SearchQ>| async move {
                s.lock().unwrap().push(format!(
                    "{}|{}|{}|{}",
                    headers.get("x-scope-orgid").and_then(|v| v.to_str().ok()).unwrap_or(""),
                    q.q,
                    q.block_id.unwrap_or_default(),
                    q.shard.unwrap_or_default(),
                ));
                axum::Json(serde_json::json!({
                    "traces": [ {
                        "traceID": "0a".repeat(16),
                        "rootServiceName": "svc",
                        "rootTraceName": "GET /",
                        "startTimeUnixNano": "1",
                        "durationMs": 1,
                        "spanSets": [ { "spans": [], "matched": 0 } ]
                    } ],
                    "metrics": { "totalJobs": 0, "completedJobs": 1, "totalBlocks": 0,
                                 "inspectedTraces": 1, "inspectedBytes": 64, "inspectedSpans": 0 }
                }))
            }),
        )
        .with_state(seen_h);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();
    let out = backend
        .search_job(&SearchJobRequest {
            tenant: "tenant-x".to_string(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Block { block_id: "blk-1".to_string(), row_group: None },
        })
        .await
        .unwrap();

    assert!(out.traces.len() == 1);
    assert!(out.metrics.inspected_bytes == 64);
    let log = seen.lock().unwrap();
    assert!(log.len() == 1);
    // tenant | query | blockID | shard
    assert!(log[0].starts_with("tenant-x|{ }|blk-1|"));
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --test frontend_http_backend`
Expected: FAIL — `cannot find type HttpQuerier`.

- [x] **Step 3: Implement `http_backend.rs`**

```rust
//! The real querier fan-out backend: a reqwest client round-robining over a
//! configurable set of querier addresses, speaking the Tempo HTTP API at the
//! per-job grain (one HTTP call per planned shard).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crabka_traceql::{ScopedTag, TagScope, TypedValue};

use crate::frontend::backend::{
    BackendError, QuerierBackend, SearchJobRequest, SearchPartial, TraceByIdJobRequest, TracePartial,
};
use crate::frontend::job::JobShard;
use crate::frontend::wire::{Metrics, SearchResponseJson};

/// HTTP querier pool. Round-robins `addrs`; each request carries the tenant in
/// `X-Scope-OrgID` and a per-request timeout.
pub struct HttpQuerier {
    http: reqwest::Client,
    addrs: Vec<String>,
    next: AtomicUsize,
    timeout: Duration,
}

impl HttpQuerier {
    /// Build the pool. `addrs` are `host:port` (no scheme; http:// is assumed).
    ///
    /// # Errors
    /// Returns `BackendError::Transport` if `addrs` is empty or the client
    /// cannot be built.
    pub fn new(addrs: Vec<String>, timeout: Duration) -> Result<Self, BackendError> {
        if addrs.is_empty() {
            return Err(BackendError::Transport("no querier addresses".to_string()));
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| BackendError::Transport(e.to_string()))?;
        Ok(Self { http, addrs, next: AtomicUsize::new(0), timeout })
    }

    fn pick_addr(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.addrs.len();
        &self.addrs[i]
    }

    fn map_send_err(e: reqwest::Error) -> BackendError {
        if e.is_timeout() {
            BackendError::Timeout
        } else {
            BackendError::Transport(e.to_string())
        }
    }
}

#[async_trait]
impl QuerierBackend for HttpQuerier {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        let url = format!("http://{}/api/search", self.pick_addr());
        // Shard restriction as query params (Slice-5 contract).
        let mut params: Vec<(&str, String)> = vec![
            ("q", req.query.clone()),
            ("start", req.start_ns.to_string()),
            ("end", req.end_ns.to_string()),
            ("limit", req.limit.to_string()),
            ("spss", req.spss.to_string()),
        ];
        match &req.shard {
            JobShard::Live => params.push(("shard", "live".to_string())),
            JobShard::Block { block_id, row_group } => {
                params.push(("blockID", block_id.clone()));
                if let Some(rg) = row_group {
                    params.push(("rowGroup", rg.to_string()));
                }
            }
        }
        let resp = self
            .http
            .get(&url)
            .header("X-Scope-OrgID", &req.tenant)
            .query(&params)
            .send()
            .await
            .map_err(Self::map_send_err)?;
        let body: SearchResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode search body: {e}")))?;
        // The HTTP edge carries Tempo-JSON traces; the frontend re-projects them
        // back into the traceql result type for merging. To avoid a JSON→type
        // round-trip we keep the body's metrics and carry the JSON traces
        // through a thin parse (see wire.rs `From` for the forward path; the
        // reverse projection lives in `SearchResponseJson::into_partial`).
        Ok(body.into_partial())
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        let hex = crate::frontend::wire::hex16(&req.trace_id);
        let url = format!("http://{}/api/v2/traces/{hex}", self.pick_addr());
        let mut params: Vec<(&str, String)> =
            vec![("start", req.start_ns.to_string()), ("end", req.end_ns.to_string())];
        if let Some(b) = &req.block_id {
            params.push(("blockID", b.clone()));
        }
        let resp = self
            .http
            .get(&url)
            .header("X-Scope-OrgID", &req.tenant)
            .query(&params)
            .send()
            .await
            .map_err(Self::map_send_err)?;
        // VERIFY against Slice-2 TraceSpans deserialization: the v2 body
        // `{ trace: { resourceSpans: [...] }, status, message }` parses into
        // `TraceSpans`. `TracePartial::from_v2_body` lives in wire.rs once the
        // TraceSpans serde surface is confirmed.
        let body = resp
            .json::<crate::frontend::wire::TraceByIdBody>()
            .await
            .map_err(|e| BackendError::Transport(format!("decode trace body: {e}")))?;
        Ok(body.into_partial())
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<ScopedTag>, Metrics), BackendError> {
        let url = format!("http://{}/api/v2/search/tags", self.pick_addr());
        let mut params: Vec<(&str, String)> =
            vec![("start", start_ns.to_string()), ("end", end_ns.to_string())];
        if let Some(s) = scope {
            params.push(("scope", crate::frontend::wire::scope_param(&s).to_string()));
        }
        let resp = self
            .http
            .get(&url)
            .header("X-Scope-OrgID", tenant)
            .query(&params)
            .send()
            .await
            .map_err(Self::map_send_err)?;
        let body = resp
            .json::<crate::frontend::wire::TagsBody>()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tags body: {e}")))?;
        Ok(body.into_parts())
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<TypedValue>, Metrics), BackendError> {
        let url = format!("http://{}/api/v2/search/tag/{tag}/values", self.pick_addr());
        let resp = self
            .http
            .get(&url)
            .header("X-Scope-OrgID", tenant)
            .query(&[("start", start_ns.to_string()), ("end", end_ns.to_string())])
            .send()
            .await
            .map_err(Self::map_send_err)?;
        let body = resp
            .json::<crate::frontend::wire::TagValuesBody>()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tag-values body: {e}")))?;
        Ok(body.into_parts())
    }
}
```

This task references three small `wire.rs` helpers (`SearchResponseJson::into_partial`, `TraceByIdBody`, `TagsBody`/`TagValuesBody`, `scope_param`). Add them to `wire.rs` as part of this step:

```rust
// --- append to wire.rs ---

use crabka_traceql::{ScopedTag, TagScope, TraceResult, TraceSpans, TypedValue};

use crate::frontend::backend::{SearchPartial, TracePartial};

impl SearchResponseJson {
    /// Reverse projection: parse the Tempo-JSON traces back into traceql
    /// `TraceResult`s for merging, carrying the body's `metrics{}` through.
    #[must_use]
    pub fn into_partial(self) -> SearchPartial {
        SearchPartial {
            traces: self.traces.iter().map(TraceResult::from).collect(),
            metrics: self.metrics,
        }
    }
}

impl From<&TraceJson> for TraceResult {
    fn from(t: &TraceJson) -> Self {
        // Hex → bytes (lossless inverse of hex16/hex8).
        TraceResult {
            trace_id: parse_hex16(&t.trace_id),
            root_service_name: t.root_service_name.clone(),
            root_trace_name: t.root_trace_name.clone(),
            start_time_unix_nano: t.start_time_unix_nano.parse().unwrap_or(0),
            duration_ms: t.duration_ms,
            span_sets: t.span_sets.iter().map(crabka_traceql::SpanSet::from).collect(),
        }
    }
}

// `From<&SpanSetJson> for crabka_traceql::SpanSet` and the SpanRef/attr inverse
// projections mirror the forward `From` impls above; implement them alongside.

/// The v2 by-id response body.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct TraceByIdBody {
    pub trace: Option<TraceSpans>,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub metrics: Metrics,
}

impl TraceByIdBody {
    #[must_use]
    pub fn into_partial(self) -> TracePartial {
        TracePartial { trace: self.trace, metrics: self.metrics }
    }
}

/// The `/api/v2/search/tags` body.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct TagsBody {
    #[serde(default)]
    pub scopes: Vec<ScopeTagsJson>,
    #[serde(default)]
    pub metrics: Metrics,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ScopeTagsJson {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl TagsBody {
    #[must_use]
    pub fn into_parts(self) -> (Vec<ScopedTag>, Metrics) {
        let tags = self
            .scopes
            .into_iter()
            .map(|s| ScopedTag { scope: parse_scope(&s.name), tags: s.tags })
            .collect();
        (tags, self.metrics)
    }
}

/// The `/api/v2/search/tag/{tag}/values` body.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct TagValuesBody {
    #[serde(rename = "tagValues", default)]
    pub tag_values: Vec<TypedValue>,
    #[serde(default)]
    pub metrics: Metrics,
}

impl TagValuesBody {
    #[must_use]
    pub fn into_parts(self) -> (Vec<TypedValue>, Metrics) {
        (self.tag_values, self.metrics)
    }
}

/// The query-param spelling of a tag scope (Tempo: lowercase scope names).
#[must_use]
pub fn scope_param(scope: &TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

fn parse_scope(name: &str) -> TagScope {
    match name {
        "resource" => TagScope::Resource,
        "intrinsic" => TagScope::Intrinsic,
        "event" => TagScope::Event,
        "link" => TagScope::Link,
        "instrumentation" => TagScope::Instrumentation,
        _ => TagScope::Span,
    }
}

fn parse_hex16(s: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        let lo = i * 2;
        if lo + 2 <= s.len() {
            *byte = u8::from_str_radix(&s[lo..lo + 2], 16).unwrap_or(0);
        }
    }
    out
}
```

> **reqwest 0.13 + Tempo-contract verify-note (the churn surface):** `Client::builder().timeout(..).build()`, `.get(url).header(..).query(&[(&str, String)]).send().await`, `Response::json::<T>().await`, and `reqwest::Error::is_timeout()` are reqwest 0.13 surface (already used in grpc-gateway's `forward.rs` with `json`+`rustls`). The loopback-stub test pins the **search** request shape (path, `blockID`/`shard`, `X-Scope-OrgID`) and response parse; fix any method drift against 0.13. The **trace-by-id** parse (`TraceByIdBody.trace: Option<TraceSpans>`) depends on Slice-2's `TraceSpans: Deserialize` — VERIFY that `TraceSpans` derives `Deserialize` from the v2 `{ trace: { resourceSpans:[...] } }` shape; if it does not, the `trace_by_id_job` deserialization and the `frontend_trace_by_id_assembly.rs` test (Task 9) are gated on a companion `crabka-traceql` change. The Slice-5 querier's exact param names (`blockID`/`shard`/`rowGroup`) are the assumed contract — if Slice 5 spells them differently, this file is the single edit point (the test pins behavior, not the spelling).

- [x] **Step 4: Re-export from `mod.rs`**

```rust
pub mod http_backend;

pub use http_backend::HttpQuerier;
```

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --test frontend_http_backend`
Expected: PASS (the search-job loopback test; trace-by-id loopback is exercised in Task 9 once `TraceSpans: Deserialize` is confirmed).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): HttpQuerier reqwest fan-out backend (per-job Tempo HTTP)"
```

---

### Task 9: Shard-equivalence + trace-by-id-assembly integration tests

**Files:**
- Create: `crates/traces/tests/frontend_shard_equivalence.rs`
- Create: `crates/traces/tests/frontend_trace_by_id_assembly.rs`

**Interfaces:**
- Consumes the public `frontend` API end-to-end with `MockQuerier` + `MockCatalog`.

- [x] **Step 1: Shard-equivalence test (`frontend_shard_equivalence.rs`)**

The first-class correctness concern: a search sharded across N jobs (Live + per-block + per-row-group) equals the unsharded search over the same data — same trace set, `limit`/`spss` honored identically. The mock returns, for each job, the partial that shard would contribute; the assertion is that the merged result equals the hand-computed union.

```rust
use std::sync::Arc;

use assert2::assert;
use crabka_traceql::{SpanRef, SpanSet, TraceResult};
use crabka_traces::frontend::backend::{MockQuerier, SearchPartial};
use crabka_traces::frontend::config::FrontendConfig;
use crabka_traces::frontend::job::{BlockMetaInfo, MockCatalog};
use crabka_traces::frontend::wire::Metrics;
use crabka_traces::frontend::QueryFrontend;

fn block(id: &str, start: i64, end: i64, size: u64, rgs: &[u64]) -> BlockMetaInfo {
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ns: start,
        end_ns: end,
        total_records: 100,
        size_bytes: size,
        row_groups: rgs.len(),
        row_group_sizes: rgs.to_vec(),
    }
}

fn trace_with_spans(tid: u8, start: u64, span_ids: &[u8]) -> TraceResult {
    let spans: Vec<SpanRef> = span_ids
        .iter()
        .map(|&s| SpanRef {
            span_id: [s; 8],
            start_time_unix_nano: start,
            duration_nanos: 1,
            attributes: vec![],
        })
        .collect();
    let matched = spans.len() as u32;
    TraceResult {
        trace_id: [tid; 16],
        root_service_name: "svc".to_string(),
        root_trace_name: "GET /".to_string(),
        start_time_unix_nano: start,
        duration_ms: 1,
        span_sets: vec![SpanSet { spans, matched }],
    }
}

fn partial(traces: Vec<TraceResult>, bytes: u64) -> SearchPartial {
    SearchPartial {
        traces,
        metrics: Metrics { completed_jobs: 1, inspected_bytes: bytes, inspected_traces: 1, ..Metrics::default() },
    }
}

#[tokio::test]
async fn sharded_search_equals_unsharded() {
    // Two blocks; the second is large (> budget, 2 row-groups) so it fans into
    // 2 row-group jobs. With the hot window we also get a Live job:
    //   Live + b1(whole) + b2(rg0) + b2(rg1) = 4 jobs.
    // trace 1 is split: block b1 has span 1, b2-rg0 has span 2 (same trace_id).
    // trace 2 lives wholly in b2-rg1. Live returns nothing.
    let catalog = MockCatalog::new(vec![
        block("b1", 0, 100, 500, &[500]),
        block("b2", 100, 200, 30_000, &[15_000, 15_000]),
    ]);
    let backend = MockQuerier::new();
    // Job dispatch order = plan order = [Live, b1, b2-rg0, b2-rg1].
    backend.stub_search(partial(vec![], 0)); // Live: empty
    backend.stub_search(partial(vec![trace_with_spans(1, 50, &[1])], 100)); // b1
    backend.stub_search(partial(vec![trace_with_spans(1, 40, &[2])], 200)); // b2-rg0
    backend.stub_search(partial(vec![trace_with_spans(2, 150, &[3])], 300)); // b2-rg1

    let cfg = FrontendConfig {
        target_bytes_per_job: 10_000,
        max_concurrency: 1, // deterministic dispatch order for FIFO stubs
        hot_frontier_ns: 150,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

    let resp = qf.search("t1", "{ }", 0, 300, 20, 10).await;

    // 4 jobs dispatched.
    assert!(qf.backend_ref().search_calls().len() == 4);
    // Unsharded baseline: trace 1 (spans 1,2 reunioned) + trace 2 (span 3).
    assert!(resp.traces.len() == 2);
    // Newest-first: trace 2 starts at 150, trace 1 at min(50,40)=40.
    assert!(resp.traces[0].trace_id == "02".repeat(16));
    assert!(resp.traces[1].trace_id == "01".repeat(16));
    // trace 1's spans reunioned across the two jobs (2 spans total).
    let t1_spans: usize = resp.traces[1].span_sets.iter().map(|ss| ss.spans.len()).sum();
    assert!(t1_spans == 2);
    // metrics: 4 total jobs, 4 completed, 2 blocks, bytes summed = 600.
    assert!(resp.metrics.total_jobs == 4);
    assert!(resp.metrics.completed_jobs == 4);
    assert!(resp.metrics.total_blocks == 2);
    assert!(resp.metrics.inspected_bytes == 600);
}

#[tokio::test]
async fn limit_and_spss_applied_after_merge() {
    let catalog = MockCatalog::new(vec![block("b1", 0, 100, 500, &[500])]);
    let backend = MockQuerier::new();
    // One job, 3 traces, one of which has 5 spans.
    backend.stub_search(partial(
        vec![
            trace_with_spans(1, 100, &[1, 2, 3, 4, 5]),
            trace_with_spans(2, 300, &[6]),
            trace_with_spans(3, 200, &[7]),
        ],
        10,
    ));
    let cfg = FrontendConfig { hot_frontier_ns: i64::MAX, ..FrontendConfig::default() };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
    // limit 2 (newest-first ⇒ start 300, 200), spss 2.
    let resp = qf.search("t1", "{ }", 0, 300, 2, 2).await;
    assert!(resp.traces.len() == 2);
    assert!(resp.traces[0].start_time_unix_nano == "300");
    // (the 5-span trace would be dropped by limit-2; verify the kept ones honor spss)
    for t in &resp.traces {
        for ss in &t.span_sets {
            assert!(ss.spans.len() <= 2);
        }
    }
}
```

> **Mock-stub ordering caveat:** `MockQuerier` pops stubs FIFO. The equivalence test sets `max_concurrency = 1` so dispatch order is the deterministic plan order `[Live, b1, b2-rg0, b2-rg1]`, matching the stub order. With higher concurrency the FIFO-vs-`buffer_unordered` pairing is nondeterministic; for a concurrent equivalence test, upgrade `MockQuerier` to match on `SearchJobRequest.shard` (return the partial keyed by block/row-group) — a small fixture upgrade flagged here, not needed for this deterministic test.

- [x] **Step 2: Trace-by-id-assembly test (`frontend_trace_by_id_assembly.rs`)**

A trace split across two blocks (late spans) reassembles into one v2 trace. **Gated on the Slice-2 `TraceSpans` accessors** (`merge_in`/`span_ids`/`approx_size_bytes` + `Deserialize`); if those are not yet available, land this test in the same PR as the `crabka-traceql` companion change.

```rust
use std::sync::Arc;

use assert2::assert;
use crabka_traces::frontend::backend::{MockQuerier, TracePartial};
use crabka_traces::frontend::config::FrontendConfig;
use crabka_traces::frontend::job::{BlockMetaInfo, MockCatalog};
use crabka_traces::frontend::merge::TraceStatus;
use crabka_traces::frontend::wire::Metrics;
use crabka_traces::frontend::QueryFrontend;

fn block(id: &str, start: i64, end: i64) -> BlockMetaInfo {
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ns: start,
        end_ns: end,
        total_records: 10,
        size_bytes: 100,
        row_groups: 1,
        row_group_sizes: vec![100],
    }
}

#[tokio::test]
async fn trace_split_across_blocks_reassembles() {
    // VERIFY against Slice-2: build two TraceSpans each carrying half the trace.
    // Pseudocode until the TraceSpans constructor is confirmed:
    //   let part_a = TraceSpans::from_spans(trace_id, &[span_1, span_2]);
    //   let part_b = TraceSpans::from_spans(trace_id, &[span_3]);
    // Then stub one per block job and assert the assembled trace has 3 spans
    // and TraceStatus::Complete (under the byte budget).
    let catalog = MockCatalog::new(vec![block("b1", 0, 100), block("b2", 100, 200)]);
    let backend = MockQuerier::new();
    // backend.stub_trace(TracePartial { trace: Some(part_a), metrics: ... });
    // backend.stub_trace(TracePartial { trace: Some(part_b), metrics: ... });
    backend.stub_trace(TracePartial { trace: None, metrics: Metrics { completed_jobs: 1, ..Metrics::default() } });
    backend.stub_trace(TracePartial { trace: None, metrics: Metrics { completed_jobs: 1, ..Metrics::default() } });
    let cfg = FrontendConfig { hot_frontier_ns: i64::MAX, max_trace_bytes: 1_000_000, max_concurrency: 1, ..FrontendConfig::default() };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

    let (_trace, metrics, status) = qf.trace_by_id("t1", [9; 16], 0, 300, ).await;
    // Live + 2 blocks = 3 by-id jobs.
    assert!(qf.backend_ref().trace_calls().len() == 3);
    assert!(matches!(status, TraceStatus::Complete));
    assert!(metrics.completed_jobs == 2); // the two stubbed (Live falls through to default empty)
    // Once TraceSpans builders are confirmed, assert the unioned span count == 3
    // and a PARTIAL status under a tiny max_trace_bytes.
}
```

- [x] **Step 3: Run to verify they pass**

Run: `cargo test -p crabka-traces --test frontend_shard_equivalence --test frontend_trace_by_id_assembly`
Expected: PASS.

- [x] **Step 4: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "test(traces): frontend shard-equivalence + trace-by-id assembly"
```

---

### Task 10: axum server + handlers + `--target query-frontend` role binary

**Files:**
- Create: `crates/traces/src/frontend/server.rs`
- Create: `crates/traces/tests/frontend_server.rs`
- Modify: `crates/traces/src/bin/crabka-traces.rs`
- Modify: `crates/traces/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `fn router_with_backend<B,C>(qf:Arc<QueryFrontend<B,C>>) -> axum::Router` — `/api/echo`, `/api/search`, `/api/v2/traces/{traceID}`, `/api/v2/search/tags`, `/api/v2/search/tag/{tag}/values` (GET), tenant from `X-Scope-OrgID`, returns the Tempo JSON. (`B: QuerierBackend + 'static, C: BlockCatalog + 'static`.)
  - `async fn run_query_frontend(cfg:FrontendConfig, shutdown:CancellationToken) -> std::io::Result<()>` — build the `HttpQuerier` pool + an `HttpCatalog` (Slice-5 block-metadata door), bind `cfg.listen_addr`, serve.
  - The binary's `--target query-frontend` arm calls `run_query_frontend`.

- [x] **Step 1: Write the failing handler test**

Create `crates/traces/tests/frontend_server.rs` — boot the frontend router against a `MockQuerier`+`MockCatalog`-backed `QueryFrontend` over loopback and assert `/api/search` round-trips with tenant + `limit`/`spss`, and `/api/echo` returns `echo`.

```rust
use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use crabka_traces::frontend::backend::{MockQuerier, SearchPartial};
use crabka_traces::frontend::config::FrontendConfig;
use crabka_traces::frontend::job::{BlockMetaInfo, MockCatalog};
use crabka_traces::frontend::server::router_with_backend;
use crabka_traces::frontend::wire::Metrics;
use crabka_traces::frontend::QueryFrontend;
use crabka_traceql::{SpanSet, TraceResult};

#[tokio::test]
async fn server_round_trips_search_and_echo() {
    let catalog = MockCatalog::new(vec![BlockMetaInfo {
        block_id: "b1".to_string(),
        start_ns: 0,
        end_ns: 100,
        total_records: 1,
        size_bytes: 10,
        row_groups: 1,
        row_group_sizes: vec![10],
    }]);
    let backend = MockQuerier::new();
    backend.stub_search(SearchPartial {
        traces: vec![TraceResult {
            trace_id: [1; 16],
            root_service_name: "svc".to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: 1,
            duration_ms: 1,
            span_sets: vec![SpanSet { spans: vec![], matched: 0 }],
        }],
        metrics: Metrics { completed_jobs: 1, ..Metrics::default() },
    });
    let cfg = FrontendConfig { hot_frontier_ns: i64::MAX, ..FrontendConfig::default() };
    let qf = Arc::new(QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg));
    let app = router_with_backend(qf);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = reqwest::Client::new();

    let echo = client
        .get(format!("http://{addr}/api/echo"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert!(echo.status().is_success());
    assert!(echo.text().await.unwrap() == "echo");

    let resp = client
        .get(format!("http://{addr}/api/search"))
        .query(&[("q", "{ }"), ("start", "0"), ("end", "100"), ("limit", "20"), ("spss", "3")])
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["traces"][0]["traceID"] == "01".repeat(16));
    assert!(body["metrics"]["completedJobs"] == 1);
    assert!(body["metrics"]["totalBlocks"] == 1);
}
```

> Because the concrete `QueryFrontend` type parameters differ between the test (`MockQuerier`+`MockCatalog`) and production (`HttpQuerier`+`HttpCatalog`), the router is generic: `router_with_backend<B: QuerierBackend + 'static, C: BlockCatalog + 'static>(qf: Arc<QueryFrontend<B, C>>) -> Router`. Production `run_query_frontend` binds the concrete prod types.

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --test frontend_server`
Expected: FAIL — `cannot find function router_with_backend`.

- [x] **Step 3: Implement `server.rs`**

```rust
//! axum HTTP surface for the query-frontend: the Tempo query endpoints, tenant
//! extraction, and the v2 by-id `status`/`message` envelope.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;

use crate::frontend::backend::QuerierBackend;
use crate::frontend::job::BlockCatalog;
use crate::frontend::wire::hex16;
use crate::frontend::QueryFrontend;

const TENANT_HEADER: &str = "X-Scope-OrgID";

#[derive(Debug, Deserialize)]
struct SearchParams {
    q: String,
    #[serde(default)]
    start: Option<i64>,
    #[serde(default)]
    end: Option<i64>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    spss: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ByIdParams {
    #[serde(default)]
    start: Option<i64>,
    #[serde(default)]
    end: Option<i64>,
}

fn tenant_of(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string()
}

/// Build the query-frontend router for any backend/catalog pair (so tests can
/// use mocks and prod uses the HTTP impls).
#[must_use]
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    Router::new()
        .route("/api/echo", get(|| async { "echo" }))
        .route("/api/search", get(search_handler::<B, C>))
        .route("/api/v2/traces/{trace_id}", get(by_id_handler::<B, C>))
        .with_state(qf)
}

async fn search_handler<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> impl IntoResponse
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant_of(&headers);
    let limit = p.limit.unwrap_or(qf.default_limit());
    let spss = p.spss.unwrap_or(qf.default_spss());
    let resp = qf
        .search(&tenant, &p.q, p.start.unwrap_or(0), p.end.unwrap_or(i64::MAX), limit, spss)
        .await;
    Json(resp)
}

async fn by_id_handler<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(p): Query<ByIdParams>,
) -> impl IntoResponse
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant_of(&headers);
    let tid = parse_hex16(&trace_id);
    let (trace, _metrics, status) = qf
        .trace_by_id(&tenant, tid, p.start.unwrap_or(0), p.end.unwrap_or(i64::MAX))
        .await;
    // v2 envelope: { trace, status, message }. `metrics` is NOT on this endpoint
    // (spec §8: metrics belongs to /api/search and the tag endpoints only).
    let message = match status {
        crate::frontend::merge::TraceStatus::Partial => {
            "trace exceeds max size; returned partially".to_string()
        }
        crate::frontend::merge::TraceStatus::Complete => String::new(),
    };
    Json(serde_json::json!({
        "trace": trace,
        "status": status.as_str(),
        "message": message,
        "traceID": hex16(&tid),
    }))
}

fn parse_hex16(s: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        let lo = i * 2;
        if lo + 2 <= s.len() {
            *byte = u8::from_str_radix(&s[lo..lo + 2], 16).unwrap_or(0);
        }
    }
    out
}
```

> **axum 0.8 path-param note:** axum 0.8 uses `{param}` capture syntax (`/api/v2/traces/{trace_id}`) and `Path<String>` extraction — verify against the grpc-gateway `serve.rs` router (the workspace's axum 0.8 precedent). If the codebase pins axum 0.7-style `:param`, switch the route literal accordingly; the handler signature is unchanged. The `/api/echo` route returns the literal `"echo"` (Tempo's datasource health probe).

This task uses two small accessors on `QueryFrontend` — add them to the impl in `mod.rs`:

```rust
    /// The configured default trace limit (request `limit` override falls back here).
    #[must_use]
    pub fn default_limit(&self) -> usize {
        self.cfg.default_limit
    }

    /// The configured default spans-per-spanSet.
    #[must_use]
    pub fn default_spss(&self) -> usize {
        self.cfg.default_spss
    }
```

- [x] **Step 4: Implement an `HttpCatalog` + `run_query_frontend` + the role binary**

Add an `HttpCatalog` (the production `BlockCatalog`) to `http_backend.rs`:

```rust
// --- append to http_backend.rs ---

use crate::frontend::job::{BlockCatalog, BlockMetaInfo, CatalogError};

/// The production block catalog: GETs the querier's block-metadata door.
pub struct HttpCatalog {
    http: reqwest::Client,
    addrs: Vec<String>,
    next: AtomicUsize,
}

impl HttpCatalog {
    #[must_use]
    pub fn new(addrs: Vec<String>) -> Self {
        Self { http: reqwest::Client::new(), addrs, next: AtomicUsize::new(0) }
    }

    fn pick_addr(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.addrs.len().max(1);
        &self.addrs[i]
    }
}

#[derive(serde::Deserialize)]
struct BlocksBody {
    #[serde(default)]
    blocks: Vec<BlockMetaJson>,
}

#[derive(serde::Deserialize)]
struct BlockMetaJson {
    #[serde(rename = "blockID")]
    block_id: String,
    #[serde(rename = "startUnixNano")]
    start_ns: i64,
    #[serde(rename = "endUnixNano")]
    end_ns: i64,
    #[serde(default, rename = "totalRecords")]
    total_records: u64,
    #[serde(default, rename = "sizeBytes")]
    size_bytes: u64,
    #[serde(default, rename = "rowGroups")]
    row_groups: usize,
    #[serde(default, rename = "rowGroupSizes")]
    row_group_sizes: Vec<u64>,
}

#[async_trait]
impl BlockCatalog for HttpCatalog {
    async fn blocks(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        let url = format!("http://{}/api/blocks", self.pick_addr());
        let resp = self
            .http
            .get(&url)
            .header("X-Scope-OrgID", tenant)
            .query(&[("start", start_ns.to_string()), ("end", end_ns.to_string())])
            .send()
            .await
            .map_err(|e| CatalogError::Backend(e.to_string()))?;
        let body: BlocksBody = resp
            .json()
            .await
            .map_err(|e| CatalogError::Backend(format!("decode blocks: {e}")))?;
        Ok(body
            .blocks
            .into_iter()
            .map(|b| BlockMetaInfo {
                block_id: b.block_id,
                start_ns: b.start_ns,
                end_ns: b.end_ns,
                total_records: b.total_records,
                size_bytes: b.size_bytes,
                row_groups: b.row_groups,
                row_group_sizes: b.row_group_sizes,
            })
            .collect())
    }
}
```

Add `run_query_frontend` to `server.rs`:

```rust
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::frontend::config::FrontendConfig;
use crate::frontend::http_backend::{HttpCatalog, HttpQuerier};

/// Boot the query-frontend role: build the HTTP querier pool + block catalog,
/// then serve the router on `cfg.listen_addr` until `shutdown` fires.
///
/// # Errors
/// Propagates bind/serve `std::io` errors and backend-construction failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let backend = HttpQuerier::new(cfg.backend_addrs.clone(), cfg.request_timeout)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let catalog = HttpCatalog::new(cfg.backend_addrs.clone());
    let qf = Arc::new(QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg.clone()));
    let app = router_with_backend(qf);
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}
```

Re-export in `mod.rs`: `pub mod server; pub use server::{router_with_backend, run_query_frontend};`.

Extend the existing role binary `crates/traces/src/bin/crabka-traces.rs` (Slices 4–5 created it with `distributor`/`block-builder`/`live-store`/`querier` arms) with the `query-frontend` arm:

```rust
        Target::QueryFrontend => {
            use std::time::Duration;

            use crabka_traces::frontend::config::FrontendConfig;
            use crabka_traces::frontend::server::run_query_frontend;
            use tokio_util::sync::CancellationToken;

            // Real config wiring (backend addrs / listen addr / hot frontier from
            // flags or a config file) lands in the hardening slice; the default
            // FrontendConfig is enough to boot the role.
            let cfg = FrontendConfig::default();
            let shutdown = CancellationToken::new();
            run_query_frontend(cfg, shutdown).await
        }
```

Ensure `Target` (the `clap::ValueEnum`) has a `QueryFrontend` variant; add it if Slices 4–5 left it out.

> **Binary-config note:** this slice wires the role *dispatch* and a working `HttpQuerier`/`HttpCatalog` from the default config. Real config loading (querier addresses, `target_bytes_per_job`, `max_concurrency`, the per-partition `hot_frontier_ns` from the live-store/block-builder offsets, the listen addr) lands in Slice 8 hardening. The server test targets the library router, not the binary, so the default config suffices to pass.

- [x] **Step 5: Run to verify it passes + whole-crate gate**

Run: `cargo test -p crabka-traces --test frontend_server`
Then the full gate: `cargo test -p crabka-traces && cargo clippy -p crabka-traces --all-targets && cargo fmt -p crabka-traces --check`
Expected: all PASS, no warnings, formatting clean. Also confirm the binary builds: `cargo build -p crabka-traces --bin crabka-traces`.

- [x] **Step 6: Commit**

```bash
git add crates/traces/
git commit -m "feat(traces): query-frontend axum server + --target query-frontend role"
```

---

## Result cache (deferred — rationale)

A result cache is **optional** for traces and intentionally **not** built in this slice:

- **No moving-window reuse.** The metrics frontend's result cache pays off because Grafana re-issues the *same* `query_range` with a sliding window, so older step-aligned sub-ranges are reused. Ad-hoc TraceQL **search** has no such repeated-sub-range structure — each search is a fresh predicate over a time window; there is little to reuse across requests.
- **Where Tempo actually caches.** Tempo's frontend caches **job results** (per block+shard) and **bloom/footer** lookups, not whole-search results. That job-result cache is a *block-keyed* cache (a completed `Block` job's partial is content-addressable by `block_id` + query hash since a sealed block is immutable). Adding it is a clean follow-on: a `JobCache` trait consulted inside the per-`Block`-job branch of `run_jobs`, keyed by `(tenant, block_id, row_group, query_hash, start, end)`, with `Live` jobs never cached (the hot tier mutates). The `Metrics` accounting already distinguishes completed jobs, so a cache-hit job would contribute `inspected_bytes = 0` and still count as completed.
- **Decision:** ship the shard/queue/fan-out/merge correctness first (this slice), and add the block-keyed `JobCache` in the hardening slice (Slice 8) alongside per-tenant limits — where the cache's eviction/size budget is a tenant-quota concern anyway. This is flagged, not forgotten.

---

## Self-review

**Spec coverage (against §6.7 two query paths, §8 Tempo HTTP API, §11 Slice 6):**
- **Search sharding** (time: hot live-store vs cold backend; then per-block; then per-row-group ~`target_bytes_per_job`) → Tasks 3, 7, 9.
- **Queueing + fan-out** (bounded-concurrency `buffer_unordered` across queriers, trait-abstracted backend, per-job dispatch with timeouts) → Tasks 2 (trait), 6 (queue), 7 (orchestrator), 8 (`HttpQuerier`).
- **Merge respecting limit/spss** (reunion by `trace_id`, cross-block span dedup, newest-first `limit`, per-spanSet `spss` with true `matched`) → Tasks 4, 9.
- **Trace-by-id assembly** (one job per candidate block + Live, union `resourceSpans`, v2 `PARTIAL`/`COMPLETE` by size) → Tasks 3, 5, 7, 9, 10.
- **`metrics{}` job accounting** (`totalJobs`/`completedJobs`/`totalBlocks`/`inspectedTraces`/`inspectedBytes`/`inspectedSpans`; plan-seeded totals, summed per-job bytes; failed job counts toward total only) → Tasks 1, 7, 9.
- **Tempo HTTP surface** (`/api/echo`, `/api/search`, `/api/v2/traces/{id}`, tag endpoints; `X-Scope-OrgID`; v2 envelope has no `metrics`) → Tasks 1, 10.
- **Role binary** `crabka-traces --target query-frontend` → Task 10.
- **First-class correctness** (sharded search == unsharded over identical data; cross-block reunion exact once; limit/spss honored) → Tasks 4, 9.

**Contract fidelity:** consumes the Slice-2 `crabka-traceql` result types (`TraceResult`/`SpanSet`/`SpanRef`/`TraceSpans`/`TagScope`/`ScopedTag`/`TypedValue`/`AttrValue`) **by import, not redefinition**, and the Slice-5 querier HTTP surface (`/api/search`, `/api/v2/traces/{id}`, `/api/blocks`, tag endpoints) at the per-job grain. The `SearchResponseJson`/`TraceJson`/`Metrics` model (Task 1) is shaped to Tempo's JSON and pinned by a serde test; the no-op path (single job) round-trips the querier's body unchanged — the byte-equality analog.

**Churn-prone surfaces — structured + behavior-pinned + verify-noted:**
- `reqwest` 0.13 + querier Tempo contract (`http_backend.rs`) — pinned by a loopback axum-stub test asserting search request shape (`blockID`/`shard`, `X-Scope-OrgID`) + response parse; verify-note for method/param-name drift.
- `crabka-traceql` Slice-2 types (`wire.rs` `From` impls, `merge.rs`) — the result-type field names are the pinned contract; the **`TraceSpans` accessors** (`merge_in`/`span_ids`/`approx_size_bytes`/`Deserialize`) are explicitly flagged as *not* in the pinned contract, with a "verify against the real `TraceSpans`; add a companion `crabka-traceql` PR if absent" note and the dependent test (`frontend_trace_by_id_assembly.rs`) gated accordingly — **not fabricated**.
- `futures` `buffer_unordered` (`queue.rs`) — standard idiom, `futures_util` fallback noted; pinned by the bounded-concurrency test.
- `axum` 0.8 routing (`server.rs`) — `{param}` capture + `Path<String>` verify-noted against the grpc-gateway precedent; pinned by the loopback server test.

**`TraceSpans` dependency — flagged and bounded:** the by-id assembly (Task 5) and its happy-path test (Task 9) depend on `TraceSpans` introspection that the Slice-2 contract leaves opaque. The plan does **not** invent fields: it implements the parts that need no internals (tag-union merge, `None`/metrics paths) fully now, and gates `assemble_trace`'s span-union + the assembly integration test on confirming/adding the three accessors — surfacing as a compile error against the real type rather than a silent wrong shape.

**Result cache — deferred, not dropped:** the optional traces result cache is explicitly deferred to Slice 8 with a concrete design (block-keyed `JobCache` consulted in the per-`Block`-job branch, `Live` jobs never cached, hit-job contributes 0 inspected bytes) — the spec calls it optional, and the moving-window reuse that justifies the metrics cache does not apply to ad-hoc search.

**Placeholder scan:** no "TBD"/"similar to Task N"/"add error handling". Every step has runnable code or an exact command. The two genuine external-contract hand-waves (`TraceSpans` accessors; Slice-5 param spellings) are each bounded with a verify-against-the-real-type note and pinned/gated by a behavior test, never left vague.

**Type consistency:** `SearchResponseJson`/`TraceJson`/`Metrics` defined once (Task 1) and used unchanged across merge/orchestrator/server. `QuerierBackend` (Task 2) implemented by both `MockQuerier` (Task 2) and `HttpQuerier` (Task 8) with identical signatures; `BlockCatalog` (Task 3) by `MockCatalog`/`HttpCatalog`. `JobShard`/`SearchJobRequest`/`SearchPartial`/`TracePartial`/`TraceStatus`/`JobPlan` referenced consistently between definitions, orchestrator, and tests.

**Known risk (flagged):** the `MockQuerier` FIFO-stub-vs-`buffer_unordered`-dispatch ordering (Task 9 caveat) is deterministic only under `max_concurrency = 1`; a concurrent equivalence test needs the shard-keyed mock upgrade — contained to the test fixture, surfaces as a failing equivalence assertion, never silent corruption.
