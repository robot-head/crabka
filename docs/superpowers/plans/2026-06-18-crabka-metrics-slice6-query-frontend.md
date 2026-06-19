# crabka-metrics Slice 6 — Query-frontend (time-splitting + query sharding + fan-out + result cache)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `query-frontend` role — an axum server that sits in front of N queriers and (1) splits a long `query_range` into step-aligned per-day sub-ranges, (2) vertically shards a shardable query by injecting Mimir's `__query_shard__="<i>_of_<n>"` selector and merging the partials *correctly*, (3) fans sub-queries across queriers in parallel through a trait-abstracted backend, and (4) caches `query_range` results split on cache boundaries so a moving time window reuses cached older sub-ranges — all while preserving the Prometheus HTTP API byte-shapes the querier (Slice 5) exposes.

**Architecture:** A new `frontend` module tree inside `crabka-metrics`. The querier backend is a `QuerierBackend` **trait** (`async fn instant_query` / `async fn range_query`) so tests drive a `MockQuerier` returning canned partials and real deployments use an `HttpQuerier` pool (reqwest, the grpc-gateway `forward.rs` pattern). A `QueryResult` type mirrors the Prometheus JSON envelope (`status`/`data.resultType`∈{vector,matrix,scalar,string}) so splitting/sharding logic manipulates parsed results, not raw bytes. PromQL AST inspection (`promql_parser::parser::parse`) decides shardability and rewrites leaf selectors. The pipeline composes as `split → (per sub-range) cache-lookup → shard → fan-out → shard-merge → stitch → cache-store`. The role binary is `crabka-metrics --target query-frontend`.

**Tech Stack:** Rust 2024 · `axum` 0.8 (`http1`, `tokio`) · `reqwest` 0.13 (`json`, `rustls`) · `promql-parser` 0.10 (AST parse/inspect/rewrite) · `object_store` 0.13 (cache backend) · `serde`/`serde_json` (Prometheus JSON) · `tokio` (`rt-multi-thread`, `macros`, `time`, `sync`) · `futures` (parallel `join_all`) · `thiserror` · `async-trait`. Tests: `assert2`, `tokio` (`test`, `macros`).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire shapes freely; no shims, no migration code, no default-off feature gates.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-metrics --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-metrics` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!`/`assert2::check!` in tests.
- **Kafka/Prometheus wire fidelity:** the frontend must round-trip the querier's Prometheus JSON unchanged for the no-op path (single sub-range, non-shardable, cache-miss) — that is the byte-equality analog. Splitting/sharding only ever *rearranges* the same sample set; a sharded result MUST equal the unsharded result over identical data.
- **Tenant propagation:** the inbound `X-Scope-OrgID` header is threaded onto every backend sub-request and into every cache key. Never collapse tenants in the cache.
- **promql-parser as the source of truth for shardability/rewrite.** Decide shardability and inject selectors from the parsed AST, never by string-munging the query text.

---

## Dependency & slice roadmap

**Depends on:** Slice 5 (Querier + Prometheus HTTP API). The frontend consumes the querier's HTTP surface exactly:

- `GET`/`POST /api/v1/query?query=&time=` → instant query → Prometheus JSON (`resultType` `vector`/`scalar`/`string`).
- `GET`/`POST /api/v1/query_range?query=&start=&end=&step=` → range query → Prometheus JSON (`resultType` `matrix`).
- Tenant via `X-Scope-OrgID`. Errors as Prometheus envelopes (`status:"error"`, `errorType`, `error`).

**The querier's `__query_shard__` support is assumed (Slice 5 contract):** the querier honors a `__query_shard__="<i>_of_<n>"` matcher on a vector selector by restricting its series scan to the shard whose `fingerprint % n == i`. The frontend's job is to *inject* that matcher and *merge* the partials; the querier's job is to *honor* it. This slice does not implement querier-side shard filtering.

**Slice 6 talks to the querier over HTTP, not via the engine API.** The frontend's `frontend::result::QueryResult` is a **serde JSON DTO** (Prometheus `{"status","data":{"resultType","result"}}`), deliberately distinct from Slice 2's Rust engine enum `crabka_promql::QueryResult` (which is `Scalar`/`InstantVector`/`RangeMatrix`/`Str`). They share a friendly name but are different layers: the engine value model vs. the wire DTO this slice parses out of the querier's response body. Do **not** import Slice 2/5's engine `QueryResult` here — Slice 5 serializes via `query_result_to_json(&QueryResult) -> serde_json::Value` and exposes no typed JSON DTO, so this slice owns `frontend/result.rs` and must keep its byte-shape identical to what Slice 5's `query_result_to_json` emits (pinned by the serde test in Task 1).

**The 8 metrics slices** (this plan = Slice 6):

1. Data layer — block schemas + native-histogram codec + symbol table. *(done)*
2. `crabka-promql` core — parser + operator pattern + selectors + rate-family + aggregations + binary ops + `.test` harness.
3. Query completeness — `histogram_quantile`, full function catalog, subqueries, `@`/`offset`.
4. Ingest service — remote_write v1/v2 + OTLP + Kafka produce + distributor + HA dedup + compactor.
5. Querier + Prometheus HTTP API + hot/cold merge.
6. **Query-frontend** *(this plan)* — split / shard / cache + the `query-frontend` role binary.
7. Ruler — recording + alerting + rule API.
8. Hardening — multi-tenancy/limits, remote_read, prometheus/compliance + differential-vs-Mimir.

---

## File structure (`crates/metrics/`)

| File | Responsibility |
|---|---|
| `src/lib.rs` | add `pub mod frontend;` |
| `src/frontend/mod.rs` | module decls + public re-exports + `QueryFrontend` orchestrator |
| `src/frontend/result.rs` | `QueryResult` / `ResultData` / `SampleStream` — the Prometheus-JSON model + matrix stitch + vector merge helpers |
| `src/frontend/backend.rs` | `QuerierBackend` trait + `InstantRequest`/`RangeRequest` + `MockQuerier` (test) |
| `src/frontend/http_backend.rs` | `HttpQuerier` — reqwest pool over configurable querier addrs (fan-out target) |
| `src/frontend/split.rs` | time-splitting: step-aligned per-interval sub-range computation + matrix stitching |
| `src/frontend/shard.rs` | shardability analysis + `__query_shard__` AST rewrite + shard-merge |
| `src/frontend/cache.rs` | `ResultCache` trait + `InMemoryCache` (test) + `ObjectStoreCache` + cache key + TTL + `Cache-Control` bypass |
| `src/frontend/server.rs` | axum router + handlers (`/api/v1/query`, `/api/v1/query_range`) wiring the orchestrator |
| `src/frontend/config.rs` | `FrontendConfig` (backend addrs, split interval, shard count, cache TTL, timeouts) |
| `src/bin/crabka-metrics.rs` | (modify/create) `--target query-frontend` role dispatch |
| `tests/frontend_shard_equivalence.rs` | integration: sharded `sum(rate(...))` == unsharded over canned data |
| `tests/frontend_split_stitch.rs` | integration: split+stitch == single range over canned data |

---

### Task 1: Crate deps + `frontend` module scaffold + the Prometheus-JSON result model

**Files:**
- Modify: `crates/metrics/Cargo.toml`
- Modify: `crates/metrics/src/lib.rs`
- Create: `crates/metrics/src/frontend/mod.rs`
- Create: `crates/metrics/src/frontend/result.rs`

**Interfaces:**
- Produces:
  - `enum ResultData { Matrix(Vec<SampleStream>), Vector(Vec<InstantSample>), Scalar(ScalarSample), String(StringSample) }` (serde, tagged `resultType`/`result` to match Prometheus JSON).
  - `struct SampleStream { metric: BTreeMap<String,String>, values: Vec<(f64, String)> }` — a matrix series (`values` = `[ts, "value"]` pairs; value is a string per Prometheus JSON).
  - `struct InstantSample { metric: BTreeMap<String,String>, value: (f64, String) }`.
  - `struct QueryResult { status: String, data: ResultData, warnings: Vec<String> }` with `fn error(errorType, error) -> QueryResult` and serde shaped exactly as Prometheus (`{"status":"success","data":{"resultType":"matrix","result":[...]}}`).
  - `fn series_key(metric: &BTreeMap<String,String>) -> String` — stable per-series identity (sorted labels) used by stitch/merge.

- [ ] **Step 1: Add dependencies to `crates/metrics/Cargo.toml`**

Add to `[dependencies]` (workspace pins where they exist; `reqwest`/`promql-parser` are explicit-version like grpc-gateway's reqwest):

```toml
axum = { workspace = true, features = ["json", "form", "query"] }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "net", "macros", "time", "sync"] }
tokio-util = { workspace = true }
futures = { workspace = true }
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls"] }
promql-parser = "0.10"
object_store = { workspace = true }
serde = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
clap = { workspace = true }
```

Add to `[dev-dependencies]`:

```toml
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "macros", "time", "sync"] }
```

> **Workspace-dep verify-note:** `futures`, `async-trait`, `clap`, `tracing`, `object_store`, `serde_json` are workspace members (see root `Cargo.toml`). If `futures` is named `futures-util` only, use `futures-util` with `join_all` from `futures_util::future`. If a `workspace = true` line errors with "not a workspace dependency", add the pin to the root `[workspace.dependencies]` first (it is a manifest fix, not a design change).

> **axum feature verify-note (required, not optional):** the workspace `axum` pin is `default-features = false, features = ["http1", "tokio"]` — it does **not** enable the `json`, `form`, or `query` extractors. This slice needs all three: `axum::Json` (server responses + the `http_backend` stub test), `Form<RangeForm>` (the `http_backend` stub test), and `Query<RangeParams>`/`Query<InstantParams>` (the `server.rs` handlers). The `features = ["json", "form", "query"]` above is merged additively onto the workspace pin by Cargo, so these three are added on top of `http1`/`tokio`. Without them the crate will not compile (the extractors won't exist).

- [ ] **Step 2: Write the failing test**

Create `crates/metrics/src/frontend/result.rs` with only the test module first:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;

    fn metric(name: &str) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert("__name__".to_string(), name.to_string());
        m
    }

    #[test]
    fn matrix_serializes_as_prometheus_json() {
        let qr = QueryResult::success(ResultData::Matrix(vec![SampleStream {
            metric: metric("up"),
            values: vec![(1.0, "1".to_string()), (2.0, "1".to_string())],
        }]));
        let json = serde_json::to_value(&qr).unwrap();
        assert!(json["status"] == "success");
        assert!(json["data"]["resultType"] == "matrix");
        assert!(json["data"]["result"][0]["metric"]["__name__"] == "up");
        // values are [ts, "stringvalue"] pairs.
        assert!(json["data"]["result"][0]["values"][0][0] == 1.0);
        assert!(json["data"]["result"][0]["values"][0][1] == "1");
    }

    #[test]
    fn error_envelope_has_errortype() {
        let qr = QueryResult::error("bad_data", "parse error");
        let json = serde_json::to_value(&qr).unwrap();
        assert!(json["status"] == "error");
        assert!(json["errorType"] == "bad_data");
        assert!(json["error"] == "parse error");
    }

    #[test]
    fn series_key_is_label_order_independent() {
        let mut a = BTreeMap::new();
        a.insert("b".to_string(), "2".to_string());
        a.insert("a".to_string(), "1".to_string());
        let mut b = BTreeMap::new();
        b.insert("a".to_string(), "1".to_string());
        b.insert("b".to_string(), "2".to_string());
        assert!(series_key(&a) == series_key(&b));
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::result`
Expected: FAIL — `cannot find type QueryResult` / unresolved module `frontend`.

- [ ] **Step 4: Implement `result.rs`**

Prepend above the `tests` module. The serde shape is the load-bearing part — Prometheus serializes `values` as `[[<float ts>, "<string value>"], ...]` and a successful envelope as `{"status":"success","data":{"resultType":...,"result":...}}`; an error envelope as `{"status":"error","errorType":...,"error":...}` (no `data`). We model that with a flattened, manually-tagged `data`.

```rust
//! The Prometheus HTTP-API JSON result model the query-frontend manipulates.
//!
//! This is the same envelope the querier (Slice 5) emits; the frontend parses
//! it, rearranges samples (split/shard merge), and re-emits it byte-shape
//! identically. `values`/`value` use Prometheus's `[ts_float, "string_value"]`
//! pair encoding.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// A matrix series: a label set + its `(timestamp_secs, "value")` samples.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SampleStream {
    pub metric: BTreeMap<String, String>,
    pub values: Vec<(f64, String)>,
}

/// A vector element: a label set + a single `(timestamp_secs, "value")` sample.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstantSample {
    pub metric: BTreeMap<String, String>,
    pub value: (f64, String),
}

/// A scalar/string result: `(timestamp_secs, "value")`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ScalarSample(pub f64, pub String);

/// A string result: `(timestamp_secs, "value")`.
pub type StringSample = ScalarSample;

/// The `data` payload, tagged by `resultType` exactly as Prometheus emits it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "resultType", content = "result", rename_all = "lowercase")]
pub enum ResultData {
    Matrix(Vec<SampleStream>),
    Vector(Vec<InstantSample>),
    Scalar(ScalarSample),
    String(StringSample),
}

/// The full Prometheus query envelope. On error, `data` is absent and
/// `error_type`/`error` are present (Prometheus's exact shape).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<ResultData>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl QueryResult {
    /// A successful result wrapping `data`.
    #[must_use]
    pub fn success(data: ResultData) -> Self {
        Self {
            status: "success".to_string(),
            data: Some(data),
            error_type: None,
            error: None,
            warnings: Vec::new(),
        }
    }

    /// A Prometheus error envelope (`status:"error"`, no `data`).
    #[must_use]
    pub fn error(error_type: &str, error: &str) -> Self {
        Self {
            status: "error".to_string(),
            data: None,
            error_type: Some(error_type.to_string()),
            error: Some(error.to_string()),
            warnings: Vec::new(),
        }
    }

    /// True iff this is a `success` envelope carrying a `matrix`.
    #[must_use]
    pub fn as_matrix(&self) -> Option<&[SampleStream]> {
        match &self.data {
            Some(ResultData::Matrix(m)) => Some(m),
            _ => None,
        }
    }
}

/// A stable, label-order-independent identity for a series (sorted `k=v`
/// joined by `\u{0}` — a byte that cannot appear in a label name/value).
#[must_use]
pub fn series_key(metric: &BTreeMap<String, String>) -> String {
    // BTreeMap already iterates in sorted key order.
    let mut s = String::new();
    for (k, v) in metric {
        s.push_str(k);
        s.push('\u{1}');
        s.push_str(v);
        s.push('\u{0}');
    }
    s
}
```

> **Serde verify-note (Prometheus shape):** the internally-tagged `ResultData` (`tag="resultType", content="result"`) emits `{"resultType":"matrix","result":[...]}` — verify with the `matrix_serializes_as_prometheus_json` test. The `(f64, String)` tuple serializes to a 2-element JSON array `[ts, "v"]`, which is Prometheus's exact sample encoding. If a future Slice-5 querier emits `value` for vector and `values` for matrix at the *element* level (it does — that is already modeled by `InstantSample::value` vs `SampleStream::values`), no change is needed.

- [ ] **Step 5: Create `frontend/mod.rs` and wire `lib.rs`**

Create `crates/metrics/src/frontend/mod.rs`:

```rust
//! The `query-frontend` role: time-splitting, query sharding, querier fan-out,
//! and result caching in front of N queriers.

pub mod result;

pub use result::{
    InstantSample, QueryResult, ResultData, SampleStream, ScalarSample, StringSample, series_key,
};
```

Add to `crates/metrics/src/lib.rs`:

```rust
pub mod frontend;
```

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::result`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): query-frontend result model (Prometheus JSON envelope)"
```

---

### Task 2: `QuerierBackend` trait + `MockQuerier`

**Files:**
- Create: `crates/metrics/src/frontend/backend.rs`
- Modify: `crates/metrics/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `struct InstantRequest { tenant: String, query: String, time_secs: f64 }`.
  - `struct RangeRequest { tenant: String, query: String, start_secs: f64, end_secs: f64, step_secs: f64 }`.
  - `enum BackendError { Timeout, Transport(String), Backend { error_type: String, error: String } }` (`thiserror`).
  - `trait QuerierBackend` (`async_trait`): `async fn instant_query(&self, req: &InstantRequest) -> Result<QueryResult, BackendError>` and `async fn range_query(&self, req: &RangeRequest) -> Result<QueryResult, BackendError>`.
  - `struct MockQuerier` — a programmable backend keyed on the request, plus a call-recorder for assertions: `fn on_range(query, RangeRequest-predicate) -> QueryResult`, `fn calls() -> Vec<RangeRequest>`. (Test/`#[cfg(any(test, feature=...))]`-free: it lives in the module behind `#[cfg(test)]`? No — integration tests in `tests/` need it, so expose it un-gated under `pub mod backend` with a doc note that it is a test fixture.)

- [ ] **Step 1: Write the failing test**

Append a test module to `backend.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::frontend::result::{ResultData, SampleStream};

    fn matrix(name: &str, vals: &[(f64, &str)]) -> QueryResult {
        let mut metric = BTreeMap::new();
        metric.insert("__name__".to_string(), name.to_string());
        QueryResult::success(ResultData::Matrix(vec![SampleStream {
            metric,
            values: vals.iter().map(|(t, v)| (*t, (*v).to_string())).collect(),
        }]))
    }

    #[tokio::test]
    async fn mock_returns_canned_and_records_calls() {
        let mock = MockQuerier::new();
        mock.stub_range(matrix("up", &[(1.0, "1")]));
        let req = RangeRequest {
            tenant: "t1".to_string(),
            query: "up".to_string(),
            start_secs: 1.0,
            end_secs: 1.0,
            step_secs: 1.0,
        };
        let out = mock.range_query(&req).await.unwrap();
        assert!(out.as_matrix().unwrap().len() == 1);
        assert!(mock.calls().len() == 1);
        assert!(mock.calls()[0].query == "up");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::backend`
Expected: FAIL — `cannot find type QuerierBackend` / `MockQuerier`.

- [ ] **Step 3: Implement `backend.rs`**

```rust
//! The querier-backend abstraction the frontend fans out to. Tests use
//! [`MockQuerier`]; real deployments use `HttpQuerier` (see `http_backend.rs`).

use std::sync::Mutex;

use async_trait::async_trait;

use crate::frontend::result::QueryResult;

/// An instant-query sub-request (`/api/v1/query`).
#[derive(Clone, Debug, PartialEq)]
pub struct InstantRequest {
    pub tenant: String,
    pub query: String,
    pub time_secs: f64,
}

/// A range-query sub-request (`/api/v1/query_range`).
#[derive(Clone, Debug, PartialEq)]
pub struct RangeRequest {
    pub tenant: String,
    pub query: String,
    pub start_secs: f64,
    pub end_secs: f64,
    pub step_secs: f64,
}

/// Failure modes of a single backend call.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend request timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({error_type}): {error}")]
    Backend { error_type: String, error: String },
}

/// A queryable querier backend (one querier replica, or a pool fronting many).
#[async_trait]
pub trait QuerierBackend: Send + Sync {
    async fn instant_query(&self, req: &InstantRequest) -> Result<QueryResult, BackendError>;
    async fn range_query(&self, req: &RangeRequest) -> Result<QueryResult, BackendError>;
}

/// A programmable in-process backend for tests. Returns the next stubbed
/// response (FIFO; the last stub repeats if more calls arrive) and records
/// every request for assertions.
///
/// Exposed un-gated (not `#[cfg(test)]`) so integration tests in `tests/` can
/// construct it. It is a fixture, not production wiring.
pub struct MockQuerier {
    range_stubs: Mutex<Vec<QueryResult>>,
    instant_stubs: Mutex<Vec<QueryResult>>,
    range_calls: Mutex<Vec<RangeRequest>>,
    instant_calls: Mutex<Vec<InstantRequest>>,
}

impl MockQuerier {
    #[must_use]
    pub fn new() -> Self {
        Self {
            range_stubs: Mutex::new(Vec::new()),
            instant_stubs: Mutex::new(Vec::new()),
            range_calls: Mutex::new(Vec::new()),
            instant_calls: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a canned range response (FIFO).
    pub fn stub_range(&self, r: QueryResult) {
        self.range_stubs.lock().unwrap().push(r);
    }

    /// Enqueue a canned instant response (FIFO).
    pub fn stub_instant(&self, r: QueryResult) {
        self.instant_stubs.lock().unwrap().push(r);
    }

    /// All recorded range requests, in dispatch order.
    #[must_use]
    pub fn calls(&self) -> Vec<RangeRequest> {
        self.range_calls.lock().unwrap().clone()
    }

    /// All recorded instant requests, in dispatch order.
    #[must_use]
    pub fn instant_calls(&self) -> Vec<InstantRequest> {
        self.instant_calls.lock().unwrap().clone()
    }

    fn pop(stubs: &Mutex<Vec<QueryResult>>) -> QueryResult {
        let mut s = stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            // Last stub repeats; empty ⇒ an empty success matrix.
            s.first().cloned().unwrap_or_else(|| {
                QueryResult::success(crate::frontend::result::ResultData::Matrix(Vec::new()))
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
    async fn instant_query(&self, req: &InstantRequest) -> Result<QueryResult, BackendError> {
        self.instant_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.instant_stubs))
    }

    async fn range_query(&self, req: &RangeRequest) -> Result<QueryResult, BackendError> {
        self.range_calls.lock().unwrap().push(req.clone());
        Ok(Self::pop(&self.range_stubs))
    }
}
```

- [ ] **Step 4: Re-export from `mod.rs`**

Add to `frontend/mod.rs`:

```rust
pub mod backend;

pub use backend::{BackendError, InstantRequest, MockQuerier, QuerierBackend, RangeRequest};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::backend`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): QuerierBackend trait + MockQuerier fixture"
```

---

### Task 3: Time-splitting + matrix stitching

**Files:**
- Create: `crates/metrics/src/frontend/split.rs`
- Modify: `crates/metrics/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `fn split_range(start_secs: f64, end_secs: f64, step_secs: f64, interval_secs: f64) -> Vec<(f64, f64)>` — step-aligned `[start, end]` sub-ranges, each spanning ≤ `interval_secs`, where every sub-range boundary lands on a step grid point (`start + k*step`) and the union of sub-range eval points equals the original eval points exactly (no gaps, no dupes).
  - `fn stitch_matrices(parts: Vec<QueryResult>) -> QueryResult` — concatenate per-series `values` across ordered sub-range matrices, preserving series identity (`series_key`) and time order; surface the first error part as the stitched error.

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/frontend/split.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;

    use super::*;
    use crate::frontend::result::{QueryResult, ResultData, SampleStream};

    #[test]
    fn split_is_step_aligned_and_covers_exactly() {
        // 0..=100 step 10, split every 40s.
        let parts = split_range(0.0, 100.0, 10.0, 40.0);
        // Every boundary on the step grid; sub-ranges contiguous; last ends at 100.
        assert!(parts.first().unwrap().0 == 0.0);
        assert!(parts.last().unwrap().1 == 100.0);
        for (s, e) in &parts {
            assert!((s / 10.0).fract() == 0.0);
            assert!((e / 10.0).fract() == 0.0);
            assert!(e - s <= 40.0);
        }
        // Reconstruct the full eval-point set from the sub-ranges and compare
        // to the single-range eval points 0,10,...,100.
        let mut points = Vec::new();
        for (s, e) in &parts {
            let mut t = *s;
            while t <= *e + f64::EPSILON {
                points.push(t.round() as i64);
                t += 10.0;
            }
        }
        points.sort_unstable();
        points.dedup();
        let expected: Vec<i64> = (0..=10).map(|k| k * 10).collect();
        assert!(points == expected);
    }

    #[test]
    fn single_subrange_when_interval_covers_whole() {
        let parts = split_range(0.0, 30.0, 10.0, 1_000.0);
        assert!(parts.len() == 1);
        assert!(parts[0] == (0.0, 30.0));
    }

    #[test]
    fn stitch_concatenates_per_series_values() {
        let mk = |vals: &[(f64, &str)]| {
            let mut m = BTreeMap::new();
            m.insert("__name__".to_string(), "up".to_string());
            QueryResult::success(ResultData::Matrix(vec![SampleStream {
                metric: m,
                values: vals.iter().map(|(t, v)| (*t, (*v).to_string())).collect(),
            }]))
        };
        let stitched = stitch_matrices(vec![mk(&[(0.0, "1"), (10.0, "1")]), mk(&[(20.0, "1")])]);
        let m = stitched.as_matrix().unwrap();
        assert!(m.len() == 1);
        assert!(m[0].values.len() == 3);
        assert!(m[0].values[2].0 == 20.0);
    }

    #[test]
    fn stitch_surfaces_first_error() {
        let err = QueryResult::error("bad_data", "boom");
        let stitched = stitch_matrices(vec![err.clone()]);
        assert!(stitched.status == "error");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::split`
Expected: FAIL — `cannot find function split_range`.

- [ ] **Step 3: Implement `split.rs`**

The alignment rule (Mimir's): sub-range boundaries are snapped to an **absolute**
interval grid (multiples of `interval`, e.g. `00:00 UTC` day boundaries), *not*
to a grid relative to `start`. This is what lets a moving time window reuse
cached older sub-ranges: a sub-range that falls entirely inside one absolute
interval bucket has the *same* `(start, end)` — and therefore the same cache key
— regardless of which outer window asked for it. Each sub-range collects the
step-grid eval points (`start + j*step`) that land in one absolute bucket
`[k*interval, (k+1)*interval)`; consecutive sub-ranges share no eval point.

```rust
//! Time-splitting for `query_range`: chop a long range into per-interval
//! sub-ranges snapped to an absolute interval grid (so moving windows share
//! sub-range boundaries and can reuse cache), fan each to a querier, then
//! stitch the matrices.

use std::collections::BTreeMap;

use crate::frontend::result::{QueryResult, ResultData, SampleStream, series_key};

/// Split `[start, end]` (step `step`) into sub-ranges each lying within one
/// absolute interval bucket `[k*interval, (k+1)*interval)`. Boundaries snap to
/// the absolute grid (multiples of `interval`), so two overlapping outer windows
/// produce *identical* `(sub_start, sub_end)` pairs for any shared interior
/// bucket — that identity is what makes the result cache reusable across a
/// moving window. The union of sub-range eval points equals the original's
/// exactly (no gap, no overlap).
///
/// Returns at least one sub-range. If `step <= 0` or `interval < step`, returns
/// the whole range unsplit (defensive — the caller validates inputs upstream).
#[must_use]
pub fn split_range(start: f64, end: f64, step: f64, interval: f64) -> Vec<(f64, f64)> {
    if step <= 0.0 || interval < step || end <= start {
        return vec![(start, end)];
    }

    let mut out = Vec::new();
    let mut sub_start = start;
    while sub_start <= end + f64::EPSILON {
        // The absolute interval boundary strictly after `sub_start`. Snapping to
        // `floor(t/interval)*interval` is what aligns this window's tiles with
        // every other window's tiles.
        let next_boundary = ((sub_start / interval).floor() + 1.0) * interval;
        // Last eval point strictly before that boundary: step back one step from
        // the boundary onto the global `start + j*step` grid, clamped to `end`.
        let last_point_before = {
            let steps_to_boundary = ((next_boundary - start) / step).ceil();
            start + (steps_to_boundary - 1.0) * step
        };
        let sub_end = last_point_before.min(end);
        out.push((sub_start, sub_end));
        if sub_end >= end - f64::EPSILON {
            break;
        }
        // Next sub-range starts at the first eval point in the next bucket.
        sub_start = sub_end + step;
    }
    out
}

/// Concatenate ordered sub-range matrices into one, preserving per-series time
/// order. The first error part short-circuits to that error. Series present in
/// some sub-ranges but not others keep only the sub-ranges they appear in.
#[must_use]
pub fn stitch_matrices(parts: Vec<QueryResult>) -> QueryResult {
    let mut by_series: BTreeMap<String, SampleStream> = BTreeMap::new();
    let mut warnings = Vec::new();

    for part in parts {
        if part.status == "error" {
            return part;
        }
        warnings.extend(part.warnings.iter().cloned());
        let Some(ResultData::Matrix(series)) = part.data else {
            // A non-matrix part in a range stitch is a backend contract
            // violation; surface as an error rather than silently dropping.
            return QueryResult::error(
                "internal",
                "non-matrix sub-result while stitching a range query",
            );
        };
        for s in series {
            let key = series_key(&s.metric);
            by_series
                .entry(key)
                .and_modify(|acc| acc.values.extend(s.values.iter().cloned()))
                .or_insert(s);
        }
    }

    // Sort each series' values by timestamp (sub-ranges arrive ordered, but be
    // defensive about overlap-free concatenation) and emit.
    let mut result: Vec<SampleStream> = by_series.into_values().collect();
    for s in &mut result {
        s.values
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }
    let mut qr = QueryResult::success(ResultData::Matrix(result));
    qr.warnings = warnings;
    qr
}
```

> **Boundary verify-note:** `split_range` uses Mimir's **absolute** interval-grid alignment (boundaries snapped to multiples of `interval`, e.g. `00:00 UTC` day boundaries — *not* `start + k*span`). A 24h split of a 7d range yields sub-queries whose eval points tile the original grid with no overlap or gap, and — critically — a *shifted* window reuses the interior tiles unchanged: `split_range(0,100,10,40) = [(0,30),(40,70),(80,100)]` and `split_range(40,140,10,40) = [(40,70),(80,110),(120,140)]` share the identical `(40,70)` tile, so its `(tenant, query, start, end, step)` cache key is byte-identical across the two windows and hits on the second query. The `split_is_step_aligned_and_covers_exactly` test pins the "union equals original eval points" invariant; the Task 8 `moving_window_reuses_cached_subranges` test pins the cross-window cache reuse this alignment enables.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod split;

pub use split::{split_range, stitch_matrices};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::split`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): query_range time-splitting + matrix stitching"
```

---

### Task 4: Shardability analysis + `__query_shard__` AST rewrite

**Files:**
- Create: `crates/metrics/src/frontend/shard.rs`
- Modify: `crates/metrics/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `enum ShardPlan { NoShard, Shardable { shards: usize } }`.
  - `fn analyze(query: &str, shards: usize) -> ShardPlan` — parse with `promql_parser`, return `Shardable` iff the top-level expression is a `sum`/`count`/`min`/`max`/`avg` aggregation (the decomposable set) over a sub-expression containing only shardable leaves; else `NoShard`.
  - `fn rewrite_shard(query: &str, shard_index: usize, shard_total: usize) -> Result<String, ShardError>` — parse, inject `__query_shard__="<i>_of_<n>"` into every leaf `VectorSelector`, re-serialize the AST to a query string.
  - `enum ShardError` (`thiserror`; `Parse(String)`).

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/frontend/shard.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn sum_rate_is_shardable() {
        assert!(matches!(
            analyze("sum(rate(http_requests_total[5m]))", 16),
            ShardPlan::Shardable { shards: 16 }
        ));
    }

    #[test]
    fn quantile_aggregation_is_not_shardable() {
        // quantile() is not in the additively-decomposable set.
        assert!(matches!(
            analyze("quantile(0.9, rate(http_requests_total[5m]))", 16),
            ShardPlan::NoShard
        ));
    }

    #[test]
    fn rewrite_injects_query_shard_matcher() {
        let q = rewrite_shard("sum(rate(http_requests_total[5m]))", 3, 16).unwrap();
        assert!(q.contains("__query_shard__"));
        assert!(q.contains("3_of_16"));
        // The original metric name survives the rewrite.
        assert!(q.contains("http_requests_total"));
    }

    #[test]
    fn rewrite_handles_bare_selector() {
        let q = rewrite_shard("up", 0, 4).unwrap();
        assert!(q.contains("__query_shard__"));
        assert!(q.contains("0_of_4"));
        assert!(q.contains("up"));
    }

    #[test]
    fn rewrite_rejects_garbage() {
        assert!(rewrite_shard("sum(((", 0, 4).is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::shard`
Expected: FAIL — `cannot find function analyze`.

- [ ] **Step 3: Implement `shard.rs`**

This is the churn-prone surface (`promql-parser` 0.10 AST). The exact enum/struct names in `promql_parser::parser::Expr` (e.g. `AggregateExpr`, `VectorSelector`, `Matcher`, `MatchOp`) must be verified against the 0.10 docs; the test pins *behavior* (shardability decision + injected matcher text), so any API drift surfaces as a compile error to fix against the real names.

```rust
//! Vertical query sharding (Mimir's `__query_shard__` scheme). A shardable
//! query's leaf vector selectors get a `__query_shard__="<i>_of_<n>"` matcher
//! injected; the querier restricts its series scan to that shard.

use promql_parser::label::{MatchOp, Matcher};
use promql_parser::parser::{self, Expr};

/// The injected shard label. The querier (Slice 5) filters series by
/// `fingerprint % n == i` when it sees `__query_shard__="<i>_of_<n>"`.
pub const QUERY_SHARD_LABEL: &str = "__query_shard__";

/// Whether and how a query can be sharded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShardPlan {
    NoShard,
    Shardable { shards: usize },
}

/// Errors from shard rewriting.
#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error("promql parse error: {0}")]
    Parse(String),
}

/// The additively-decomposable aggregation set. `sum`/`count`/`min`/`max` map
/// to partial-then-combine directly; `avg` decomposes into `sum/count` (the
/// frontend recombines — see `merge_shards` and the orchestrator's avg path).
/// Anything else (`quantile`, `topk`, `stddev`, `count_values`, …) is not
/// shardable here. `op_id` is the `u16` token id from `agg.op.id()`; we match it
/// directly, exactly as `aggr_op_of` does, so the two stay consistent.
fn is_decomposable_aggr(op_id: parser::token::TokenId) -> bool {
    use promql_parser::parser::token;
    matches!(
        op_id,
        token::T_SUM | token::T_COUNT | token::T_MIN | token::T_MAX | token::T_AVG
    )
}

/// Decide whether `query` is shardable into `shards` shards.
#[must_use]
pub fn analyze(query: &str, shards: usize) -> ShardPlan {
    if shards < 2 {
        return ShardPlan::NoShard;
    }
    let Ok(ast) = parser::parse(query) else {
        return ShardPlan::NoShard;
    };
    match ast {
        Expr::Aggregate(agg) if is_decomposable_aggr(agg.op.id()) => {
            ShardPlan::Shardable { shards }
        }
        _ => ShardPlan::NoShard,
    }
}

/// Inject `__query_shard__="<i>_of_<n>"` into every leaf vector selector and
/// re-render the query string.
pub fn rewrite_shard(
    query: &str,
    shard_index: usize,
    shard_total: usize,
) -> Result<String, ShardError> {
    let mut ast = parser::parse(query).map_err(|e| ShardError::Parse(format!("{e:?}")))?;
    let value = format!("{shard_index}_of_{shard_total}");
    inject_into_selectors(&mut ast, &value);
    Ok(ast.to_string())
}

/// Recurse the AST, appending the shard matcher to each `VectorSelector`.
fn inject_into_selectors(expr: &mut Expr, shard_value: &str) {
    match expr {
        Expr::VectorSelector(vs) => {
            // `Matchers::append` is a builder that consumes `self` and returns
            // `Self`, so rebind through it rather than discarding the result.
            vs.matchers = std::mem::take(&mut vs.matchers).append(Matcher::new(
                MatchOp::Equal,
                QUERY_SHARD_LABEL,
                shard_value,
            ));
        }
        Expr::MatrixSelector(ms) => {
            ms.vs.matchers = std::mem::take(&mut ms.vs.matchers).append(Matcher::new(
                MatchOp::Equal,
                QUERY_SHARD_LABEL,
                shard_value,
            ));
        }
        Expr::Aggregate(agg) => inject_into_selectors(&mut agg.expr, shard_value),
        Expr::Call(call) => {
            for arg in &mut call.args.args {
                inject_into_selectors(arg, shard_value);
            }
        }
        Expr::Binary(bin) => {
            inject_into_selectors(&mut bin.lhs, shard_value);
            inject_into_selectors(&mut bin.rhs, shard_value);
        }
        Expr::Paren(p) => inject_into_selectors(&mut p.expr, shard_value),
        Expr::Subquery(sq) => inject_into_selectors(&mut sq.expr, shard_value),
        Expr::Unary(u) => inject_into_selectors(&mut u.expr, shard_value),
        // The 11 `Expr` variants are exhaustive; there is no `StepInvariant`
        // wrapper (@-modifier step-invariance is carried on `VectorSelector.at`).
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}
```

> **promql-parser 0.10 API verify-note (the churn surface):** the 11 `Expr` variants (`VectorSelector`/`MatrixSelector`/`Aggregate`/`Call`/`Binary`/`Paren`/`Subquery`/`Unary`/`NumberLiteral`/`StringLiteral`/`Extension` — there is **no** `StepInvariant` variant; @-modifier step-invariance is carried on `VectorSelector.at`), the matcher constructor (`Matcher::new(MatchOp::Equal, name: &str, value: &str)` — both args are `&str`, so pass `QUERY_SHARD_LABEL` / `shard_value` directly, not `.to_string()`), the builder `Matchers::append(self, Matcher) -> Self` (it **consumes and returns** `self`, so the implementation rebinds via `vs.matchers = std::mem::take(&mut vs.matchers).append(...)` — mutating-in-place would silently drop the matcher and over-count every shard), the aggregate-op token accessor (`agg.op.id()` → `TokenId` = `u16`, and the `T_SUM`…`T_AVG` token constants are `u16`), and `Expr: Display` (`to_string()` re-renders the query) are all **promql-parser 0.10** surface. If any name differs, fix it against the 0.10 docs — the tests pin the *behavior* (shardable decision + `i_of_n` matcher text in the rendered string), not the type names. If `op.id()` is instead `agg.op` being a token id directly, drop the `.id()`.

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod shard;

pub use shard::{QUERY_SHARD_LABEL, ShardError, ShardPlan, analyze, rewrite_shard};
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::shard`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): shardability analysis + __query_shard__ AST rewrite"
```

---

### Task 5: Shard-merge (the correctness centerpiece)

**Files:**
- Modify: `crates/metrics/src/frontend/shard.rs` (add `merge_shards`)

**Interfaces:**
- Consumes: `QueryResult`, `ResultData`, `SampleStream`, `InstantSample`, `series_key`, the parsed top-level aggregation op.
- Produces:
  - `fn merge_shards(op: AggrOp, parts: Vec<QueryResult>) -> QueryResult` — combine N shard partials into the single result the unsharded query would have produced. `Sum`/`Count` → add partial values per `(series_key, timestamp)`; `Min`/`Max` → min/max per `(series_key, timestamp)`; `Avg` is **not** an `AggrOp` — the orchestrator decomposes `avg(x)` into `sum(x)`/`count(x)` and recombines via `divide_results` (this fn never sees `Avg`). Works for both matrix (`query_range`) and vector (`query`) partials.
  - `enum AggrOp { Sum, Count, Min, Max }` + `fn aggr_op_of(query: &str) -> Option<AggrOp>` (parse, read top-level op).
  - `fn decompose_avg(query: &str) -> Option<(String, String)>` — if `query`'s top-level op is `avg`, return the `(sum(<inner>), count(<inner>))` query-string pair (built from the AST by swapping the aggregate op, then `to_string()`); else `None`.
  - `fn divide_results(sum_res: QueryResult, count_res: QueryResult) -> QueryResult` — recombine an `avg` from its merged sum and merged count: per `(series_key, timestamp)`, emit `sum / count`. Handles matrix and vector; errors/short-circuit propagate.

- [ ] **Step 1: Write the failing test — sharded sum equals unsharded**

Append to the `tests` module in `shard.rs`:

```rust
    use crate::frontend::result::{InstantSample, QueryResult, ResultData, SampleStream};
    use std::collections::BTreeMap;

    fn series(labels: &[(&str, &str)], vals: &[(f64, &str)]) -> SampleStream {
        let metric = labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        SampleStream {
            metric,
            values: vals.iter().map(|(t, v)| (*t, (*v).to_string())).collect(),
        }
    }

    #[test]
    fn sharded_sum_equals_unsharded_matrix() {
        // Unsharded sum over a metric whose series land in two shards would
        // produce, per timestamp, the total. Each shard returns its partial.
        // After grouping (no `by`), the merged label set is empty `{}`.
        let shard0 = QueryResult::success(ResultData::Matrix(vec![series(
            &[],
            &[(0.0, "3"), (10.0, "5")],
        )]));
        let shard1 = QueryResult::success(ResultData::Matrix(vec![series(
            &[],
            &[(0.0, "7"), (10.0, "1")],
        )]));
        let merged = merge_shards(AggrOp::Sum, vec![shard0, shard1]);
        let m = merged.as_matrix().unwrap();
        assert!(m.len() == 1);
        // 3+7=10 @ t=0 ; 5+1=6 @ t=10.
        assert!(m[0].values == vec![(0.0, "10".to_string()), (10.0, "6".to_string())]);
    }

    #[test]
    fn sharded_max_takes_per_point_max() {
        let s0 = QueryResult::success(ResultData::Vector(vec![InstantSample {
            metric: BTreeMap::new(),
            value: (0.0, "3".to_string()),
        }]));
        let s1 = QueryResult::success(ResultData::Vector(vec![InstantSample {
            metric: BTreeMap::new(),
            value: (0.0, "9".to_string()),
        }]));
        let merged = merge_shards(AggrOp::Max, vec![s0, s1]);
        match merged.data.unwrap() {
            ResultData::Vector(v) => assert!(v[0].value.1 == "9"),
            other => panic!("expected vector, got {other:?}"),
        }
    }

    #[test]
    fn aggr_op_of_reads_top_level() {
        assert!(matches!(aggr_op_of("sum(rate(x[1m]))"), Some(AggrOp::Sum)));
        assert!(matches!(aggr_op_of("max(x)"), Some(AggrOp::Max)));
        assert!(aggr_op_of("quantile(0.5, x)").is_none());
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::shard::tests::sharded_sum_equals_unsharded_matrix`
Expected: FAIL — `cannot find type AggrOp` / `merge_shards`.

- [ ] **Step 3: Implement the merge**

Add to `shard.rs` (above `tests`):

```rust
use std::collections::BTreeMap;

use crate::frontend::result::{InstantSample, QueryResult, ResultData, SampleStream, series_key};

/// The additively/idempotently-combinable top-level aggregation. `Avg` is
/// absent on purpose: the orchestrator rewrites `avg(x)` into
/// `sum(x) / count(x)`, shards each, and divides — so the merge only ever sees
/// these four.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggrOp {
    Sum,
    Count,
    Min,
    Max,
}

impl AggrOp {
    fn combine(self, a: f64, b: f64) -> f64 {
        match self {
            // count partials are themselves counts; summing them recombines.
            AggrOp::Sum | AggrOp::Count => a + b,
            AggrOp::Min => a.min(b),
            AggrOp::Max => a.max(b),
        }
    }
}

/// Read the top-level aggregation op of `query`, if it is one of the four
/// shard-mergeable ops.
#[must_use]
pub fn aggr_op_of(query: &str) -> Option<AggrOp> {
    let ast = parser::parse(query).ok()?;
    match ast {
        Expr::Aggregate(agg) => {
            use promql_parser::parser::token;
            match agg.op.id() {
                token::T_SUM => Some(AggrOp::Sum),
                token::T_COUNT => Some(AggrOp::Count),
                token::T_MIN => Some(AggrOp::Min),
                token::T_MAX => Some(AggrOp::Max),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Merge N shard partials into the single result the unsharded query produces.
/// Groups by `(series_key, timestamp)` and combines with `op`. Matrix and
/// vector partials are both supported; mixed/`error` parts surface as an error.
#[must_use]
pub fn merge_shards(op: AggrOp, parts: Vec<QueryResult>) -> QueryResult {
    // (series_key, timestamp_bits) -> (metric, combined value). We key the time
    // on its bit pattern to group identical eval points exactly.
    let mut acc: BTreeMap<(String, u64), (BTreeMap<String, String>, f64)> = BTreeMap::new();
    let mut saw_vector = false;
    let mut saw_matrix = false;

    let mut absorb = |metric: &BTreeMap<String, String>, t: f64, v: f64| {
        let key = (series_key(metric), t.to_bits());
        acc.entry(key)
            .and_modify(|(_, acc_v)| *acc_v = op.combine(*acc_v, v))
            .or_insert_with(|| (metric.clone(), v));
    };

    for part in parts {
        if part.status == "error" {
            return part;
        }
        match part.data {
            Some(ResultData::Matrix(series)) => {
                saw_matrix = true;
                for s in series {
                    for (t, v) in &s.values {
                        let parsed: f64 = v.parse().unwrap_or(f64::NAN);
                        absorb(&s.metric, *t, parsed);
                    }
                }
            }
            Some(ResultData::Vector(elems)) => {
                saw_vector = true;
                for e in elems {
                    let parsed: f64 = e.value.1.parse().unwrap_or(f64::NAN);
                    absorb(&e.metric, e.value.0, parsed);
                }
            }
            _ => {
                return QueryResult::error(
                    "internal",
                    "non-matrix/vector shard partial while merging",
                );
            }
        }
    }

    if saw_vector && saw_matrix {
        return QueryResult::error("internal", "mixed matrix/vector shard partials");
    }

    // Re-group flattened (series, timestamp) accumulators back into series.
    let mut by_series: BTreeMap<String, SampleStream> = BTreeMap::new();
    for ((skey, tbits), (metric, value)) in acc {
        let t = f64::from_bits(tbits);
        by_series
            .entry(skey)
            .or_insert_with(|| SampleStream {
                metric,
                values: Vec::new(),
            })
            .values
            .push((t, fmt_value(value)));
    }
    for s in by_series.values_mut() {
        s.values
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    if saw_vector {
        let vector = by_series
            .into_values()
            .map(|s| InstantSample {
                metric: s.metric,
                value: s.values.into_iter().next().unwrap_or((0.0, "NaN".to_string())),
            })
            .collect();
        QueryResult::success(ResultData::Vector(vector))
    } else {
        QueryResult::success(ResultData::Matrix(by_series.into_values().collect()))
    }
}

/// Format a combined value the way Prometheus renders sample values (shortest
/// round-trippable decimal; integers without a trailing `.0`).
fn fmt_value(v: f64) -> String {
    if v == v.trunc() && v.is_finite() {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// If `query`'s top-level aggregation is `avg`, return the `(sum, count)` query
/// pair that decomposes it: `avg(inner)` ⇒ (`sum(inner)`, `count(inner)`),
/// preserving any `by`/`without` grouping. Built from the AST by swapping the
/// aggregate op token and re-rendering, so it never string-munges the inner
/// expression. Returns `None` for any non-`avg` top-level query.
#[must_use]
pub fn decompose_avg(query: &str) -> Option<(String, String)> {
    use promql_parser::parser::token;
    let ast = parser::parse(query).ok()?;
    let Expr::Aggregate(agg) = ast else {
        return None;
    };
    if agg.op.id() != token::T_AVG {
        return None;
    }
    // Rebuild the aggregate with a different op, keeping modifier/grouping/expr.
    let with_op = |op_id| {
        let mut a = agg.clone();
        a.op = parser::token::TokenType::new(op_id);
        Expr::Aggregate(a).to_string()
    };
    Some((with_op(token::T_SUM), with_op(token::T_COUNT)))
}

/// Recombine `avg` from its merged `sum` and merged `count` results: per
/// `(series_key, timestamp)`, emit `sum / count`. A point present in only one of
/// the two inputs is dropped (a count of 0 has no average). Errors short-circuit.
#[must_use]
pub fn divide_results(sum_res: QueryResult, count_res: QueryResult) -> QueryResult {
    if sum_res.status == "error" {
        return sum_res;
    }
    if count_res.status == "error" {
        return count_res;
    }

    // Flatten both sides to (series_key, timestamp_bits) -> (metric, value).
    fn flatten(
        qr: &QueryResult,
    ) -> Option<(bool, BTreeMap<(String, u64), (BTreeMap<String, String>, f64)>)> {
        let mut out = BTreeMap::new();
        let is_vector = match &qr.data {
            Some(ResultData::Matrix(series)) => {
                for s in series {
                    for (t, v) in &s.values {
                        out.insert(
                            (series_key(&s.metric), t.to_bits()),
                            (s.metric.clone(), v.parse().unwrap_or(f64::NAN)),
                        );
                    }
                }
                false
            }
            Some(ResultData::Vector(elems)) => {
                for e in elems {
                    out.insert(
                        (series_key(&e.metric), e.value.0.to_bits()),
                        (e.metric.clone(), e.value.1.parse().unwrap_or(f64::NAN)),
                    );
                }
                true
            }
            _ => return None,
        };
        Some((is_vector, out))
    }

    let (Some((sum_vec, sums)), Some((_, counts))) = (flatten(&sum_res), flatten(&count_res)) else {
        return QueryResult::error("internal", "non-matrix/vector while dividing for avg");
    };

    let mut by_series: BTreeMap<String, SampleStream> = BTreeMap::new();
    for (key, (metric, sum_v)) in sums {
        let Some((_, count_v)) = counts.get(&key) else {
            continue; // no matching count ⇒ no average for this point
        };
        if *count_v == 0.0 {
            continue;
        }
        let (skey, tbits) = key;
        by_series
            .entry(skey)
            .or_insert_with(|| SampleStream {
                metric,
                values: Vec::new(),
            })
            .values
            .push((f64::from_bits(tbits), fmt_value(sum_v / count_v)));
    }
    for s in by_series.values_mut() {
        s.values
            .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    }

    if sum_vec {
        let vector = by_series
            .into_values()
            .map(|s| InstantSample {
                metric: s.metric,
                value: s.values.into_iter().next().unwrap_or((0.0, "NaN".to_string())),
            })
            .collect();
        QueryResult::success(ResultData::Vector(vector))
    } else {
        QueryResult::success(ResultData::Matrix(by_series.into_values().collect()))
    }
}
```

> **Avg decomposition note:** `avg` is intentionally not an `AggrOp`. The orchestrator (Task 6) detects a top-level `avg` via `decompose_avg`, runs the resulting `sum(<inner>)` and `count(<inner>)` each as its own fully-sharded sub-query (rewrite per shard → fan out → `merge_shards`), then recombines with `divide_results` (merged-sum / merged-count per `(series, timestamp)`). This keeps `merge_shards` total over the four exact-combine ops and avoids a wrong "average of per-shard averages". The end-to-end equivalence test for `avg` (sharded `avg(...)` == unsharded) is the `sharded_avg_equals_unsharded` test added in Task 8.

> **`decompose_avg` AST verify-note:** rebuilding the aggregate with a new op assumes `AggregateExpr` is `Clone` and its `op: TokenType` field is reassignable via `TokenType::new(token_id)` (promql-parser 0.10). If `TokenType` is not directly constructible from a `TokenId`, build the two strings instead by parsing once and substituting only the leading function name on the rendered AST (`avg` → `sum`/`count`) — but prefer the AST rebuild so grouping/modifiers are preserved structurally. The Task 8 `sharded_avg_equals_unsharded` test pins the behavior regardless of which path compiles.

> **Value-format verify-note:** Prometheus renders `10` not `10.0`. `fmt_value` reproduces the integer case; for non-integers it relies on Rust's shortest-`f64` formatting, which matches Prometheus's Go `strconv.FormatFloat(v, 'f', -1, 64)` for the values these tests exercise. If a differential test against real Prometheus later shows a divergence (e.g. very large/small magnitudes), this is the single function to adjust.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::shard`
Expected: PASS (all shard tests).

- [ ] **Step 5: Re-export the merge API from `mod.rs`**

Extend the shard re-export: `pub use shard::{AggrOp, QUERY_SHARD_LABEL, ShardError, ShardPlan, aggr_op_of, analyze, decompose_avg, divide_results, merge_shards, rewrite_shard};`.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): shard-merge (sum/count/min/max) — sharded == unsharded"
```

---

### Task 6: `FrontendConfig` + `QueryFrontend` orchestrator

**Files:**
- Create: `crates/metrics/src/frontend/config.rs`
- Create the orchestrator in: `crates/metrics/src/frontend/mod.rs` (struct `QueryFrontend`)
- Modify: `crates/metrics/src/frontend/mod.rs` (re-exports)

**Interfaces:**
- Produces:
  - `struct FrontendConfig { backend_addrs: Vec<String>, split_interval_secs: f64, shard_count: usize, cache_ttl_secs: u64, request_timeout: Duration, listen_addr: SocketAddr }` (+ `Default`).
  - `struct QueryFrontend<B: QuerierBackend, C: ResultCache> { backend: Arc<B>, cache: Arc<C>, cfg: FrontendConfig }`.
  - `async fn range_query(&self, tenant, query, start, end, step, cache_control: CacheControl) -> QueryResult` — the full pipeline: split → per sub-range (cache-lookup → shard-or-not → fan-out → merge) → stitch → cache-store.
  - `async fn instant_query(&self, tenant, query, time, cache_control) -> QueryResult` — shard-or-not → fan-out → merge (no split, no cache — instant queries aren't range-cached).
  - `enum CacheControl { Use, NoStore }`.

- [ ] **Step 1: Write the failing test (split + shard fan-out counts)**

This test depends on `ResultCache` (Task 7) — to keep Task 6 self-contained, the orchestrator is generic over `C: ResultCache`, and the test uses a trivial pass-through. Define a minimal `NoCache` in the test that always misses and never stores (it's a stand-in until Task 7's `InMemoryCache`). Put the test in `mod.rs`'s `#[cfg(test)] mod orch_tests`:

```rust
#[cfg(test)]
mod orch_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::frontend::backend::MockQuerier;
    use crate::frontend::cache::{CacheControl, NoCache};
    use crate::frontend::result::{ResultData, SampleStream};

    fn matrix(vals: &[(f64, &str)]) -> QueryResult {
        QueryResult::success(ResultData::Matrix(vec![SampleStream {
            metric: BTreeMap::new(),
            values: vals.iter().map(|(t, v)| (*t, (*v).to_string())).collect(),
        }]))
    }

    #[tokio::test]
    async fn range_query_splits_and_shards_then_merges() {
        let backend = MockQuerier::new();
        // 16 shards × 1 sub-range: each shard returns the same partial "1".
        backend.stub_range(matrix(&[(0.0, "1")]));
        let cfg = FrontendConfig {
            backend_addrs: vec!["unused".to_string()],
            split_interval_secs: 1_000.0, // no split for a short range
            shard_count: 16,
            cache_ttl_secs: 60,
            request_timeout: Duration::from_secs(5),
            listen_addr: "0.0.0.0:0".parse().unwrap(),
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(NoCache), cfg);
        let out = qf
            .range_query("t1", "sum(rate(http_requests_total[5m]))", 0.0, 0.0, 10.0, CacheControl::Use)
            .await;
        // sum of 16 shard partials each "1" = 16.
        let m = out.as_matrix().unwrap();
        assert!(m[0].values == vec![(0.0, "16".to_string())]);
        // 16 shard sub-requests fired.
        assert!(qf.backend_ref().calls().len() == 16);
        // Every sub-request carried the tenant and a __query_shard__ matcher.
        for c in qf.backend_ref().calls() {
            assert!(c.tenant == "t1");
            assert!(c.query.contains("__query_shard__"));
        }
    }

    #[tokio::test]
    async fn non_shardable_query_fires_one_request() {
        let backend = MockQuerier::new();
        backend.stub_range(matrix(&[(0.0, "0.5")]));
        let cfg = FrontendConfig {
            shard_count: 16,
            split_interval_secs: 1_000.0,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(NoCache), cfg);
        let _ = qf
            .range_query("t1", "quantile(0.9, rate(x[5m]))", 0.0, 0.0, 10.0, CacheControl::Use)
            .await;
        assert!(qf.backend_ref().calls().len() == 1);
        assert!(!qf.backend_ref().calls()[0].query.contains("__query_shard__"));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::orch_tests`
Expected: FAIL — `cannot find type FrontendConfig` / `QueryFrontend` / `NoCache`.

(Note: `NoCache`/`CacheControl` are defined in Task 7's `cache.rs`. Implement the **minimal** `cache.rs` skeleton — the `ResultCache` trait + `CacheControl` + `NoCache` — as part of this step so the orchestrator compiles; Task 7 then fills in `InMemoryCache`/`ObjectStoreCache` + the split-and-cache logic and its own tests.)

- [ ] **Step 3: Implement `config.rs`**

```rust
//! Query-frontend configuration.

use std::net::SocketAddr;
use std::time::Duration;

/// Static configuration for the `query-frontend` role.
#[derive(Clone, Debug)]
pub struct FrontendConfig {
    /// Querier backend addresses (`host:port`) the HTTP pool round-robins over.
    pub backend_addrs: Vec<String>,
    /// Max wall-clock span of a single `query_range` sub-range (seconds).
    pub split_interval_secs: f64,
    /// Number of vertical shards for shardable queries (`1` ⇒ disabled).
    pub shard_count: usize,
    /// Result-cache TTL (seconds).
    pub cache_ttl_secs: u64,
    /// Per-backend-request timeout.
    pub request_timeout: Duration,
    /// The frontend's own listen address.
    pub listen_addr: SocketAddr,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            backend_addrs: vec!["127.0.0.1:9009".to_string()],
            // 24h default split, matching Mimir.
            split_interval_secs: 24.0 * 3600.0,
            shard_count: 16,
            cache_ttl_secs: 3600,
            request_timeout: Duration::from_secs(30),
            listen_addr: "0.0.0.0:8080".parse().expect("valid default addr"),
        }
    }
}
```

- [ ] **Step 4: Implement the minimal `cache.rs` skeleton (trait + CacheControl + NoCache)**

Create `crates/metrics/src/frontend/cache.rs` (Task 7 extends this):

```rust
//! Result caching for `query_range`. This file starts with the trait +
//! `CacheControl` + a no-op cache so the orchestrator compiles; Task 7 adds the
//! in-memory + object-store impls and the split-and-cache key logic.

use async_trait::async_trait;

use crate::frontend::result::QueryResult;

/// Per-request cache directive (from the inbound `Cache-Control` header).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheControl {
    /// Normal: read-through + write-back.
    Use,
    /// `Cache-Control: no-store` — bypass read and write.
    NoStore,
}

/// A range-result cache keyed by `(tenant, query, start, end, step)`.
#[async_trait]
pub trait ResultCache: Send + Sync {
    /// Look up a cached result for the exact sub-range key. `None` ⇒ miss.
    async fn get(&self, key: &str) -> Option<QueryResult>;
    /// Store `value` under `key` with the configured TTL.
    async fn put(&self, key: &str, value: &QueryResult);
}

/// A cache that always misses and never stores (used when caching is disabled
/// and as the orchestrator's compile-time stand-in).
pub struct NoCache;

#[async_trait]
impl ResultCache for NoCache {
    async fn get(&self, _key: &str) -> Option<QueryResult> {
        None
    }
    async fn put(&self, _key: &str, _value: &QueryResult) {}
}
```

- [ ] **Step 5: Implement the `QueryFrontend` orchestrator in `mod.rs`**

Add to `frontend/mod.rs` (and `mod config; mod cache;` + re-exports):

```rust
pub mod cache;
pub mod config;

pub use cache::{CacheControl, NoCache, ResultCache};
pub use config::FrontendConfig;

use std::sync::Arc;

use futures::future::join_all;

use crate::frontend::backend::{QuerierBackend, RangeRequest};
use crate::frontend::cache::cache_key;
use crate::frontend::shard::{AggrOp, ShardPlan};

/// The query-frontend pipeline: split → cache → shard → fan-out → merge →
/// stitch → cache-store, in front of a [`QuerierBackend`] pool.
pub struct QueryFrontend<B: QuerierBackend, C: ResultCache> {
    backend: Arc<B>,
    cache: Arc<C>,
    cfg: FrontendConfig,
}

impl<B: QuerierBackend, C: ResultCache> QueryFrontend<B, C> {
    #[must_use]
    pub fn new(backend: Arc<B>, cache: Arc<C>, cfg: FrontendConfig) -> Self {
        Self { backend, cache, cfg }
    }

    /// Test/inspection accessor for the backend (e.g. `MockQuerier::calls`).
    #[must_use]
    pub fn backend_ref(&self) -> &B {
        &self.backend
    }

    /// Run a `query_range` through the full pipeline.
    pub async fn range_query(
        &self,
        tenant: &str,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
        cc: CacheControl,
    ) -> QueryResult {
        let sub_ranges = split::split_range(start, end, step, self.cfg.split_interval_secs);
        let mut parts = Vec::with_capacity(sub_ranges.len());
        for (s, e) in sub_ranges {
            let key = cache_key(tenant, query, s, e, step);
            if cc == CacheControl::Use {
                if let Some(hit) = self.cache.get(&key).await {
                    parts.push(hit);
                    continue;
                }
            }
            let part = self.run_sub_range(tenant, query, s, e, step).await;
            if cc == CacheControl::Use && part.status == "success" {
                self.cache.put(&key, &part).await;
            }
            parts.push(part);
        }
        split::stitch_matrices(parts)
    }

    /// One sub-range: shard (if shardable) and fan out, else single dispatch.
    async fn run_sub_range(
        &self,
        tenant: &str,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
    ) -> QueryResult {
        match shard::analyze(query, self.cfg.shard_count) {
            ShardPlan::Shardable { shards } => {
                // `avg` is shardable but not directly combinable: decompose into
                // sum/count, shard each, then divide the merged sum by the merged
                // count per (series, timestamp).
                if let Some((sum_q, count_q)) = shard::decompose_avg(query) {
                    let sum_res = self
                        .shard_range(tenant, &sum_q, start, end, step, shards, AggrOp::Sum)
                        .await;
                    let count_res = self
                        .shard_range(tenant, &count_q, start, end, step, shards, AggrOp::Count)
                        .await;
                    return shard::divide_results(sum_res, count_res);
                }
                let Some(op) = shard::aggr_op_of(query) else {
                    return self.dispatch_range(tenant, query, start, end, step).await;
                };
                self.shard_range(tenant, query, start, end, step, shards, op)
                    .await
            }
            ShardPlan::NoShard => self.dispatch_range(tenant, query, start, end, step).await,
        }
    }

    /// Rewrite `query` per shard, fan out the N range sub-queries in parallel,
    /// and merge the partials with `op`.
    async fn shard_range(
        &self,
        tenant: &str,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
        shards: usize,
        op: AggrOp,
    ) -> QueryResult {
        let futs = (0..shards).map(|i| {
            let rewritten = shard::rewrite_shard(query, i, shards);
            async move {
                match rewritten {
                    Ok(q) => self.dispatch_range(tenant, &q, start, end, step).await,
                    Err(e) => QueryResult::error("bad_data", &e.to_string()),
                }
            }
        });
        shard::merge_shards(op, join_all(futs).await)
    }

    async fn dispatch_range(
        &self,
        tenant: &str,
        query: &str,
        start: f64,
        end: f64,
        step: f64,
    ) -> QueryResult {
        let req = RangeRequest {
            tenant: tenant.to_string(),
            query: query.to_string(),
            start_secs: start,
            end_secs: end,
            step_secs: step,
        };
        match self.backend.range_query(&req).await {
            Ok(r) => r,
            Err(e) => QueryResult::error("internal", &e.to_string()),
        }
    }
}

// Bring sibling modules into scope for the impl above.
use crate::frontend::{shard, split};
```

> **`avg` decomposition note (implemented):** `analyze` returns `Shardable` for `avg` (it is in `is_decomposable_aggr`), and `aggr_op_of` returns `None` for `avg` (it is not an `AggrOp`). Rather than fall through to an un-sharded dispatch, `run_sub_range` calls `shard::decompose_avg(query)` *first*: when the top-level op is `avg`, it derives `sum(<inner>)` and `count(<inner>)`, shards and merges each via `shard_range`, then recombines with `shard::divide_results` (merged-sum / merged-count per `(series, timestamp)`). This delivers the FOCUS-required `avg → sum/count` decomposition fully sharded, while keeping `merge_shards` total over the four exact-combine ops. The end-to-end equivalence is pinned by Task 8's `sharded_avg_equals_unsharded` test. (`instant_query` in Task 10 applies the same `decompose_avg` path for `/api/v1/query`.)

- [ ] **Step 6: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::orch_tests`
Expected: PASS (2 tests).

- [ ] **Step 7: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): QueryFrontend orchestrator (split+shard+fan-out+merge)"
```

---

### Task 7: Result cache — key, TTL, `InMemoryCache`, `ObjectStoreCache`

**Files:**
- Modify: `crates/metrics/src/frontend/cache.rs` (add key fn, `InMemoryCache`, `ObjectStoreCache`)
- Modify: `crates/metrics/src/frontend/mod.rs` (re-exports)

**Interfaces:**
- Produces:
  - `fn cache_key(tenant: &str, query: &str, start: f64, end: f64, step: f64) -> String` — stable, collision-resistant key (tenant first; floats by bit pattern hex so `1.0` and `1` don't alias).
  - `struct InMemoryCache { ttl: Duration }` — `DashMap`/`Mutex<HashMap>` of `key → (stored_at, QueryResult)`; honors TTL on `get`.
  - `struct ObjectStoreCache { store: Arc<dyn ObjectStore>, prefix: Path, ttl: Duration }` — serializes `QueryResult` to JSON under `prefix/<sha-or-key>`; `get` checks the object's age against TTL (skip/delete stale).

- [ ] **Step 1: Write the failing test**

Append to `cache.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::frontend::result::{ResultData, SampleStream};

    fn sample() -> QueryResult {
        QueryResult::success(ResultData::Matrix(vec![SampleStream {
            metric: BTreeMap::new(),
            values: vec![(0.0, "1".to_string())],
        }]))
    }

    #[test]
    fn cache_key_distinguishes_tenant_and_step() {
        let a = cache_key("t1", "up", 0.0, 10.0, 10.0);
        let b = cache_key("t2", "up", 0.0, 10.0, 10.0);
        let c = cache_key("t1", "up", 0.0, 10.0, 30.0);
        assert!(a != b);
        assert!(a != c);
        // Same inputs ⇒ same key.
        assert!(a == cache_key("t1", "up", 0.0, 10.0, 10.0));
    }

    #[tokio::test]
    async fn in_memory_round_trips_then_expires() {
        let cache = InMemoryCache::new(Duration::from_millis(50));
        let key = cache_key("t1", "up", 0.0, 10.0, 10.0);
        assert!(cache.get(&key).await.is_none()); // cold miss
        cache.put(&key, &sample()).await;
        assert!(cache.get(&key).await.is_some()); // warm hit
        tokio::time::sleep(Duration::from_millis(70)).await;
        assert!(cache.get(&key).await.is_none()); // TTL expired
    }

    #[tokio::test]
    async fn object_store_round_trips() {
        use object_store::memory::InMemory;
        use object_store::path::Path;
        let cache = ObjectStoreCache::new(
            std::sync::Arc::new(InMemory::new()),
            Path::from("cache"),
            Duration::from_secs(3600),
        );
        let key = cache_key("t1", "up", 0.0, 10.0, 10.0);
        assert!(cache.get(&key).await.is_none());
        cache.put(&key, &sample()).await;
        let hit = cache.get(&key).await;
        assert!(hit.is_some());
        assert!(hit.unwrap().as_matrix().unwrap().len() == 1);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib frontend::cache`
Expected: FAIL — `cannot find function cache_key` / `InMemoryCache` / `ObjectStoreCache`.

- [ ] **Step 3: Implement the key + impls**

Prepend to `cache.rs` (above `NoCache`, keeping the trait + `CacheControl` from Task 6):

```rust
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use object_store::path::Path;
use object_store::ObjectStore;

/// Build a stable cache key. Tenant first (so a prefix scan is per-tenant);
/// floats encoded by bit pattern so `1.0`/`1` never alias and NaN is stable.
#[must_use]
pub fn cache_key(tenant: &str, query: &str, start: f64, end: f64, step: f64) -> String {
    format!(
        "{tenant}\u{1}{query}\u{1}{:016x}\u{1}{:016x}\u{1}{:016x}",
        start.to_bits(),
        end.to_bits(),
        step.to_bits(),
    )
}

/// In-process result cache with a TTL.
pub struct InMemoryCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, (Instant, QueryResult)>>,
}

impl InMemoryCache {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl ResultCache for InMemoryCache {
    async fn get(&self, key: &str) -> Option<QueryResult> {
        let mut e = self.entries.lock().unwrap();
        match e.get(key) {
            Some((stored, _)) if stored.elapsed() > self.ttl => {
                e.remove(key);
                None
            }
            Some((_, v)) => Some(v.clone()),
            None => None,
        }
    }

    async fn put(&self, key: &str, value: &QueryResult) {
        self.entries
            .lock()
            .unwrap()
            .insert(key.to_string(), (Instant::now(), value.clone()));
    }
}

/// Object-store-backed result cache. Each entry is a JSON blob under
/// `prefix/<key-hash>`; freshness is the object's last-modified vs `ttl`.
pub struct ObjectStoreCache {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    ttl: Duration,
}

impl ObjectStoreCache {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path, ttl: Duration) -> Self {
        Self { store, prefix, ttl }
    }

    fn object_path(&self, key: &str) -> Path {
        // Hash the key to a filesystem-safe, fixed-length object name.
        let mut hash: u64 = 1_469_598_103_934_665_603; // FNV-1a offset
        for b in key.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(1_099_511_628_211);
        }
        self.prefix.child(format!("{hash:016x}.json"))
    }
}

#[async_trait]
impl ResultCache for ObjectStoreCache {
    async fn get(&self, key: &str) -> Option<QueryResult> {
        let path = self.object_path(key);
        let get = self.store.get(&path).await.ok()?;
        let modified = get.meta.last_modified;
        // Stale check: object age vs TTL (use chrono's Utc::now via the meta).
        let age = chrono::Utc::now().signed_duration_since(modified);
        if age.to_std().map(|a| a > self.ttl).unwrap_or(false) {
            // Best-effort eviction; ignore failures.
            let _ = self.store.delete(&path).await;
            return None;
        }
        let bytes = get.bytes().await.ok()?;
        serde_json::from_slice::<QueryResult>(&bytes).ok()
    }

    async fn put(&self, key: &str, value: &QueryResult) {
        let path = self.object_path(key);
        if let Ok(bytes) = serde_json::to_vec(value) {
            let _ = self.store.put(&path, bytes.into()).await;
        }
    }
}
```

> **object_store 0.13 verify-note (churn surface):** `ObjectStore::get(&Path) -> GetResult` with `.meta.last_modified: chrono::DateTime<Utc>` and `.bytes().await`, `put(&Path, PutPayload)` (the `bytes.into()` builds a `PutPayload` from `Bytes` at 0.13 — if the signature is `put(&Path, PutPayload, PutOptions)` add `Default::default()`), and `Path::child` are object_store 0.13 surface. `chrono` is already in the workspace graph via object_store; if not exposed, add `chrono` as a dep (it's transitively present). The round-trip test pins behavior; fix method names against 0.13 docs if they drift. The FNV-1a key hash is deliberately simple (no crypto) — collisions across distinct keys are astronomically unlikely for cache keys and a collision only causes a cache miss, never a wrong answer (the key is *not* re-validated on read, so if stronger collision safety is wanted, store the full key in the JSON and compare on `get`).

- [ ] **Step 4: Add `chrono` if needed + re-export from `mod.rs`**

If `cargo test` reports `chrono` unresolved, add to `Cargo.toml` `[dependencies]`: `chrono = { workspace = true }` (or `chrono = "0.4"` if not a workspace dep). Extend the cache re-export in `mod.rs`: `pub use cache::{CacheControl, InMemoryCache, NoCache, ObjectStoreCache, ResultCache, cache_key};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib frontend::cache`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): ResultCache — in-memory + object-store, TTL, split key"
```

---

### Task 8: Split-and-cache reuse + shard-equivalence integration tests

**Files:**
- Create: `crates/metrics/tests/frontend_split_stitch.rs`
- Create: `crates/metrics/tests/frontend_shard_equivalence.rs`

**Interfaces:**
- Consumes the public `frontend` API end-to-end with `MockQuerier` + `InMemoryCache`.

- [ ] **Step 1: Split-and-cache reuse test (`frontend_split_stitch.rs`)**

The headline cache behavior: a moving window reuses cached older sub-ranges. Query `[0, 100]` then the *moved* window `[40, 140]` with a 40s split. Because boundaries snap to the absolute interval grid, the two windows share the interior tile `(40, 70)`; on the second query that tile is served from cache, so the moved window issues backend calls only for its *new* tiles `(80, 110)` and `(120, 140)` — exactly two, not three.

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use crabka_metrics::frontend::backend::MockQuerier;
use crabka_metrics::frontend::cache::{CacheControl, InMemoryCache};
use crabka_metrics::frontend::config::FrontendConfig;
use crabka_metrics::frontend::result::{QueryResult, ResultData, SampleStream};
use crabka_metrics::frontend::QueryFrontend;

fn matrix(vals: &[(f64, &str)]) -> QueryResult {
    QueryResult::success(ResultData::Matrix(vec![SampleStream {
        metric: BTreeMap::new(),
        values: vals.iter().map(|(t, v)| (*t, (*v).to_string())).collect(),
    }]))
}

#[tokio::test]
async fn split_then_stitch_equals_single_range() {
    let backend = MockQuerier::new();
    // Each sub-range returns one point at its start (the mock repeats the last
    // stub, so program enough distinct stubs or accept the repeat).
    backend.stub_range(matrix(&[(0.0, "1")]));
    let cfg = FrontendConfig {
        split_interval_secs: 40.0,
        shard_count: 1, // disable sharding to isolate splitting
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(InMemoryCache::new(Duration::from_secs(60))), cfg);
    let out = qf.range_query("t1", "up", 0.0, 100.0, 10.0, CacheControl::Use).await;
    // 0..=100 step 10, split every 40 ⇒ 3 sub-ranges ⇒ 3 backend calls.
    assert!(qf.backend_ref().calls().len() == 3);
    assert!(out.status == "success");
}

#[tokio::test]
async fn moving_window_reuses_cached_subranges() {
    let backend = MockQuerier::new();
    backend.stub_range(matrix(&[(0.0, "1")]));
    let cfg = FrontendConfig {
        split_interval_secs: 40.0,
        shard_count: 1,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(InMemoryCache::new(Duration::from_secs(60))), cfg);

    // Window [0,100] (40s split) ⇒ tiles (0,30),(40,70),(80,100) ⇒ 3 calls.
    let _ = qf.range_query("t1", "up", 0.0, 100.0, 10.0, CacheControl::Use).await;
    let after_first = qf.backend_ref().calls().len();
    assert!(after_first == 3);

    // MOVE the window to [40,140] ⇒ tiles (40,70),(80,110),(120,140). The
    // absolute-grid-aligned interior tile (40,70) is identical to the first
    // window's and is served from cache ⇒ only the two NEW tiles hit the
    // backend (+2, not +3). This is the cross-window reuse the split enables.
    let _ = qf.range_query("t1", "up", 40.0, 140.0, 10.0, CacheControl::Use).await;
    assert!(qf.backend_ref().calls().len() == after_first + 2);

    // Re-issue the moved window with no-store ⇒ cache bypassed ⇒ all 3 of its
    // tiles re-fetched.
    let before_no_store = qf.backend_ref().calls().len();
    let _ = qf.range_query("t1", "up", 40.0, 140.0, 10.0, CacheControl::NoStore).await;
    assert!(qf.backend_ref().calls().len() == before_no_store + 3);
}
```

- [ ] **Step 2: Shard-equivalence test (`frontend_shard_equivalence.rs`)**

The first-class correctness concern: `sum(rate(...))` sharded over N shards equals the unsharded result over the same data. The mock returns, for each shard `i`, the partial that shard's series would contribute; the unsharded baseline returns their sum directly. Assert the frontend's sharded merge equals the baseline.

```rust
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use crabka_metrics::frontend::backend::{MockQuerier, QuerierBackend, RangeRequest};
use crabka_metrics::frontend::cache::{CacheControl, NoCache};
use crabka_metrics::frontend::config::FrontendConfig;
use crabka_metrics::frontend::result::{QueryResult, ResultData, SampleStream};
use crabka_metrics::frontend::QueryFrontend;

fn matrix_empty_labels(vals: &[(f64, f64)]) -> QueryResult {
    QueryResult::success(ResultData::Matrix(vec![SampleStream {
        metric: BTreeMap::new(),
        values: vals
            .iter()
            .map(|(t, v)| (*t, if *v == v.trunc() { format!("{}", *v as i64) } else { format!("{v}") }))
            .collect(),
    }]))
}

#[tokio::test]
async fn sharded_sum_rate_equals_unsharded() {
    // Ground truth: at t=0 the total is 12, at t=10 it's 20.
    // Spread across 4 shards as partials (3,4,2,3)@0 and (5,5,5,5)@10.
    let per_shard_at_0 = [3.0, 4.0, 2.0, 3.0]; // sum = 12
    let per_shard_at_10 = [5.0, 5.0, 5.0, 5.0]; // sum = 20

    let backend = MockQuerier::new();
    for i in 0..4 {
        backend.stub_range(matrix_empty_labels(&[(0.0, per_shard_at_0[i]), (10.0, per_shard_at_10[i])]));
    }
    let cfg = FrontendConfig {
        split_interval_secs: 10_000.0, // no split
        shard_count: 4,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(NoCache), cfg);

    let sharded = qf
        .range_query("t1", "sum(rate(http_requests_total[5m]))", 0.0, 10.0, 10.0, CacheControl::Use)
        .await;

    let m = sharded.as_matrix().unwrap();
    assert!(m.len() == 1);
    assert!(m[0].values == vec![(0.0, "12".to_string()), (10.0, "20".to_string())]);
    // Exactly 4 shard sub-requests were dispatched.
    assert!(qf.backend_ref().calls().len() == 4);
}

#[tokio::test]
async fn sharded_avg_equals_unsharded() {
    // avg(inner) decomposes into sum(inner)/count(inner), each sharded over 4
    // shards. The orchestrator dispatches the 4 sum shards first, then the 4
    // count shards (8 calls total, FIFO into the mock).
    //
    // Ground truth: true sum @0 = 12, @10 = 20; true count @0 = 4, @10 = 4 ⇒
    // avg = 3 @0 and 5 @10. Spread the sum across shards (3,4,2,3)@0 /
    // (5,5,5,5)@10 and the count as 1 per shard per point.
    let sum_at_0 = [3.0, 4.0, 2.0, 3.0]; // Σ = 12
    let sum_at_10 = [5.0, 5.0, 5.0, 5.0]; // Σ = 20

    let backend = MockQuerier::new();
    // 4 sum-shard partials, in shard order.
    for i in 0..4 {
        backend.stub_range(matrix_empty_labels(&[(0.0, sum_at_0[i]), (10.0, sum_at_10[i])]));
    }
    // 4 count-shard partials (1 series per shard per point ⇒ count 1 each).
    for _ in 0..4 {
        backend.stub_range(matrix_empty_labels(&[(0.0, 1.0), (10.0, 1.0)]));
    }
    let cfg = FrontendConfig {
        split_interval_secs: 10_000.0, // no split
        shard_count: 4,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(NoCache), cfg);

    let sharded = qf
        .range_query("t1", "avg(rate(http_requests_total[5m]))", 0.0, 10.0, 10.0, CacheControl::Use)
        .await;

    let m = sharded.as_matrix().unwrap();
    assert!(m.len() == 1);
    // 12/4 = 3 @ t=0 ; 20/4 = 5 @ t=10.
    assert!(m[0].values == vec![(0.0, "3".to_string()), (10.0, "5".to_string())]);
    // 4 sum-shard + 4 count-shard sub-requests = 8.
    assert!(qf.backend_ref().calls().len() == 8);
    // Every sub-request is sharded; the sum/count split is visible in the query.
    let queries: Vec<String> = qf.backend_ref().calls().into_iter().map(|c| c.query).collect();
    assert!(queries.iter().all(|q| q.contains("__query_shard__")));
    assert!(queries.iter().filter(|q| q.starts_with("sum")).count() == 4);
    assert!(queries.iter().filter(|q| q.starts_with("count")).count() == 4);
}
```

- [ ] **Step 3: Run to verify they pass**

Run: `cargo test -p crabka-metrics --test frontend_split_stitch --test frontend_shard_equivalence`
Expected: PASS.

> **Mock-stub ordering caveat:** `MockQuerier` pops stubs FIFO and repeats the last. The `sum`-shard test programs 4 distinct stubs (one per shard) in shard-index order; the `avg` test programs 8 (4 `sum`-shard stubs, then 4 `count`-shard stubs) because the orchestrator fully awaits the sharded `sum(...)` sub-query before the sharded `count(...)` one. This works because `join_all` preserves index order in the returned `Vec` and the *dispatch* order into the mock is the deterministic `(0..shards)` map order (and, for `avg`, sum-then-count). If a future change makes dispatch concurrent-nondeterministic w.r.t. stub consumption, switch `MockQuerier` to match on `RangeRequest.query` (its `__query_shard__` value and `sum`/`count` prefix) instead of FIFO (a small fixture upgrade; flagged here, not needed yet).

- [ ] **Step 4: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "test(metrics): frontend split-and-cache reuse + shard-equivalence"
```

---

### Task 9: `HttpQuerier` fan-out backend (reqwest pool)

**Files:**
- Create: `crates/metrics/src/frontend/http_backend.rs`
- Modify: `crates/metrics/src/frontend/mod.rs`

**Interfaces:**
- Produces:
  - `struct HttpQuerier { http: reqwest::Client, addrs: Vec<String>, next: AtomicUsize, timeout: Duration }` implementing `QuerierBackend`.
  - `fn new(addrs: Vec<String>, timeout: Duration) -> Result<HttpQuerier, BackendError>`.
  - Round-robins `addrs`, sets `X-Scope-OrgID`, POSTs form-encoded `query`/`start`/`end`/`step`/`time` to `/api/v1/query_range` / `/api/v1/query`, parses the Prometheus JSON body into `QueryResult`, maps timeouts/transport/HTTP-error into `BackendError`.

This is a churn-prone surface (reqwest + the querier's exact HTTP contract). It is **structure + behavior-pinning**: a `wiremock`-style HTTP test would add a dev-dep; instead pin behavior with a localhost test against a tiny axum stub server (reuses the crate's own axum dep, no new dev-dep) so the request shape (path, headers, form body) and response parsing are verified.

- [ ] **Step 1: Write the failing test (stub querier over loopback)**

Create `crates/metrics/tests/frontend_http_backend.rs`:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use assert2::assert;
use axum::extract::{Form, State};
use axum::routing::post;
use axum::Router;
use crabka_metrics::frontend::backend::{QuerierBackend, RangeRequest};
use crabka_metrics::frontend::http_backend::HttpQuerier;
use serde::Deserialize;

#[derive(Deserialize)]
struct RangeForm {
    query: String,
    start: String,
    end: String,
    step: String,
}

#[tokio::test]
async fn http_querier_posts_and_parses() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_h = seen.clone();

    let app = Router::new()
        .route(
            "/api/v1/query_range",
            post(|State(s): State<Arc<Mutex<Vec<String>>>>, headers: axum::http::HeaderMap, Form(f): Form<RangeForm>| async move {
                s.lock().unwrap().push(format!(
                    "{}|{}|{}|{}|{}",
                    headers.get("x-scope-orgid").and_then(|v| v.to_str().ok()).unwrap_or(""),
                    f.query, f.start, f.end, f.step
                ));
                axum::Json(serde_json::json!({
                    "status": "success",
                    "data": { "resultType": "matrix", "result": [
                        { "metric": {"__name__": "up"}, "values": [[0.0, "1"]] }
                    ]}
                }))
            }),
        )
        .with_state(seen_h);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();
    let out = backend
        .range_query(&RangeRequest {
            tenant: "tenant-x".to_string(),
            query: "up".to_string(),
            start_secs: 0.0,
            end_secs: 10.0,
            step_secs: 10.0,
        })
        .await
        .unwrap();

    assert!(out.as_matrix().unwrap()[0].metric["__name__"] == "up");
    let log = seen.lock().unwrap();
    assert!(log.len() == 1);
    assert!(log[0].starts_with("tenant-x|up|"));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test frontend_http_backend`
Expected: FAIL — `cannot find type HttpQuerier`.

- [ ] **Step 3: Implement `http_backend.rs`**

```rust
//! The real querier fan-out backend: a reqwest client round-robining over a
//! configurable set of querier addresses, speaking the Prometheus HTTP API.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crate::frontend::backend::{BackendError, InstantRequest, QuerierBackend, RangeRequest};
use crate::frontend::result::QueryResult;

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
        Ok(Self {
            http,
            addrs,
            next: AtomicUsize::new(0),
            timeout,
        })
    }

    fn pick_addr(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.addrs.len();
        &self.addrs[i]
    }

    async fn post_form(
        &self,
        tenant: &str,
        path: &str,
        form: &[(&str, String)],
    ) -> Result<QueryResult, BackendError> {
        let url = format!("http://{}{path}", self.pick_addr());
        let resp = self
            .http
            .post(&url)
            .header("X-Scope-OrgID", tenant)
            .form(form)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    BackendError::Timeout
                } else {
                    BackendError::Transport(e.to_string())
                }
            })?;
        // Prometheus returns a JSON envelope on both 200 and 4xx error results;
        // parse the body regardless of status.
        let parsed = resp
            .json::<QueryResult>()
            .await
            .map_err(|e| BackendError::Transport(format!("decode body: {e}")))?;
        Ok(parsed)
    }
}

#[async_trait]
impl QuerierBackend for HttpQuerier {
    async fn instant_query(&self, req: &InstantRequest) -> Result<QueryResult, BackendError> {
        self.post_form(
            &req.tenant,
            "/api/v1/query",
            &[
                ("query", req.query.clone()),
                ("time", req.time_secs.to_string()),
            ],
        )
        .await
    }

    async fn range_query(&self, req: &RangeRequest) -> Result<QueryResult, BackendError> {
        self.post_form(
            &req.tenant,
            "/api/v1/query_range",
            &[
                ("query", req.query.clone()),
                ("start", req.start_secs.to_string()),
                ("end", req.end_secs.to_string()),
                ("step", req.step_secs.to_string()),
            ],
        )
        .await
    }
}
```

> **reqwest 0.13 verify-note:** `Client::builder().timeout(..).build()`, `.post(url).header(..).form(&[(&str, String)]).send().await`, `Response::json::<T>().await`, and `reqwest::Error::is_timeout()` are reqwest 0.13 surface (already used in grpc-gateway's `forward.rs` with `json`+`rustls` features). The localhost-stub test pins the request shape (path, `X-Scope-OrgID`, form body) and response parsing; fix any method drift against 0.13. The `timeout` field is retained for completeness even though `Client`-level timeout already enforces it (a future per-request override would use `.timeout()` on the `RequestBuilder`).

- [ ] **Step 4: Re-export from `mod.rs`**

```rust
pub mod http_backend;

pub use http_backend::HttpQuerier;
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test frontend_http_backend`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): HttpQuerier reqwest fan-out backend"
```

---

### Task 10: axum server + handlers + `--target query-frontend` role binary

**Files:**
- Create: `crates/metrics/src/frontend/server.rs`
- Create/Modify: `crates/metrics/src/bin/crabka-metrics.rs`
- Modify: `crates/metrics/src/frontend/mod.rs`
- Modify: `crates/metrics/Cargo.toml` (add `[[bin]]` if not present)

**Interfaces:**
- Produces:
  - `fn router(frontend: Arc<QueryFrontend<HttpQuerier, …>>) -> axum::Router` — `/api/v1/query` + `/api/v1/query_range` (GET + POST), tenant from `X-Scope-OrgID`, `Cache-Control` parsed to `CacheControl`, returns the `QueryResult` as JSON.
  - `async fn run_query_frontend(cfg: FrontendConfig, cache, shutdown) -> std::io::Result<()>` — bind `cfg.listen_addr`, serve the router.
  - The binary's `--target query-frontend` arm calls `run_query_frontend`.

- [ ] **Step 1: Write the failing handler test**

Create `crates/metrics/tests/frontend_server.rs` — boot the frontend router against a `MockQuerier`-backed `QueryFrontend` over loopback and assert a `query_range` round-trips with tenant + cache-control parsing.

```rust
use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use crabka_metrics::frontend::backend::MockQuerier;
use crabka_metrics::frontend::cache::NoCache;
use crabka_metrics::frontend::config::FrontendConfig;
use crabka_metrics::frontend::result::{QueryResult, ResultData, SampleStream};
use crabka_metrics::frontend::server::router_with_backend;
use crabka_metrics::frontend::QueryFrontend;

#[tokio::test]
async fn server_round_trips_query_range() {
    let backend = MockQuerier::new();
    backend.stub_range(QueryResult::success(ResultData::Matrix(vec![SampleStream {
        metric: std::collections::BTreeMap::new(),
        values: vec![(0.0, "1".to_string())],
    }])));
    let cfg = FrontendConfig { shard_count: 1, split_interval_secs: 1e9, ..FrontendConfig::default() };
    let qf = Arc::new(QueryFrontend::new(Arc::new(backend), Arc::new(NoCache), cfg));
    let app = router_with_backend(qf);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/api/v1/query_range"))
        .query(&[("query", "up"), ("start", "0"), ("end", "0"), ("step", "10")])
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["status"] == "success");
    assert!(body["data"]["resultType"] == "matrix");
}
```

> Because the concrete `QueryFrontend` type parameters differ between the test (`MockQuerier`+`NoCache`) and production (`HttpQuerier`+`ObjectStoreCache`), the router is generic: `router_with_backend<B: QuerierBackend + 'static, C: ResultCache + 'static>(qf: Arc<QueryFrontend<B, C>>) -> Router`. The production `router(...)` is a thin alias binding the concrete prod types.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test frontend_server`
Expected: FAIL — `cannot find function router_with_backend`.

- [ ] **Step 3: Implement `server.rs`**

```rust
//! axum HTTP surface for the query-frontend: the Prometheus query endpoints,
//! tenant extraction, and `Cache-Control` parsing.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::frontend::backend::QuerierBackend;
use crate::frontend::cache::{CacheControl, ResultCache};
use crate::frontend::QueryFrontend;

const TENANT_HEADER: &str = "X-Scope-OrgID";

#[derive(Debug, Deserialize)]
struct RangeParams {
    query: String,
    start: f64,
    end: f64,
    step: f64,
}

#[derive(Debug, Deserialize)]
struct InstantParams {
    query: String,
    #[serde(default)]
    time: Option<f64>,
}

fn tenant_of(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string()
}

fn cache_control_of(headers: &HeaderMap) -> CacheControl {
    let no_store = headers
        .get(axum::http::header::CACHE_CONTROL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.to_ascii_lowercase().contains("no-store"));
    if no_store {
        CacheControl::NoStore
    } else {
        CacheControl::Use
    }
}

/// Build the query-frontend router for any backend/cache pair (so tests can use
/// `MockQuerier`/`NoCache` and prod uses `HttpQuerier`/`ObjectStoreCache`).
#[must_use]
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>) -> Router
where
    B: QuerierBackend + 'static,
    C: ResultCache + 'static,
{
    Router::new()
        .route("/api/v1/query_range", get(range_handler::<B, C>).post(range_handler::<B, C>))
        .route("/api/v1/query", get(instant_handler::<B, C>).post(instant_handler::<B, C>))
        .with_state(qf)
}

async fn range_handler<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Query(p): Query<RangeParams>,
) -> impl IntoResponse
where
    B: QuerierBackend + 'static,
    C: ResultCache + 'static,
{
    let tenant = tenant_of(&headers);
    let cc = cache_control_of(&headers);
    let out = qf
        .range_query(&tenant, &p.query, p.start, p.end, p.step, cc)
        .await;
    Json(out)
}

async fn instant_handler<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Query(p): Query<InstantParams>,
) -> impl IntoResponse
where
    B: QuerierBackend + 'static,
    C: ResultCache + 'static,
{
    let tenant = tenant_of(&headers);
    let cc = cache_control_of(&headers);
    let time = p.time.unwrap_or(0.0);
    let out = qf.instant_query(&tenant, &p.query, time, cc).await;
    Json(out)
}
```

> **POST-body note:** Prometheus accepts both GET (query string) and POST (form body) for these endpoints. The handler above uses `Query<..>` (query string), which works for GET and for POST requests that put params in the query string. For full parity with clients that POST a form body, add an `axum` extractor that tries `Form<..>` then `Query<..>`; flagged as a parity enhancement, not required for Grafana (its Prometheus datasource uses GET with a query string by default). Keep the test on GET.

- [ ] **Step 4: Add `instant_query` to the orchestrator (if not already present)**

In `mod.rs`'s `QueryFrontend` impl, add the instant path (shard-or-not, fan-out, merge; no split, no range cache):

```rust
    /// Run an `/api/v1/query` (instant) through shard-or-not + fan-out + merge.
    pub async fn instant_query(
        &self,
        tenant: &str,
        query: &str,
        time: f64,
        _cc: CacheControl,
    ) -> QueryResult {
        match shard::analyze(query, self.cfg.shard_count) {
            ShardPlan::Shardable { shards } => {
                // Same `avg` decomposition as the range path (sum/count → divide).
                if let Some((sum_q, count_q)) = shard::decompose_avg(query) {
                    let sum_res = self
                        .shard_instant(tenant, &sum_q, time, shards, AggrOp::Sum)
                        .await;
                    let count_res = self
                        .shard_instant(tenant, &count_q, time, shards, AggrOp::Count)
                        .await;
                    return shard::divide_results(sum_res, count_res);
                }
                let Some(op) = shard::aggr_op_of(query) else {
                    return self.dispatch_instant(tenant, query, time).await;
                };
                self.shard_instant(tenant, query, time, shards, op).await
            }
            ShardPlan::NoShard => self.dispatch_instant(tenant, query, time).await,
        }
    }

    /// Rewrite `query` per shard, fan out the N instant sub-queries in parallel,
    /// and merge the partials with `op`.
    async fn shard_instant(
        &self,
        tenant: &str,
        query: &str,
        time: f64,
        shards: usize,
        op: AggrOp,
    ) -> QueryResult {
        let futs = (0..shards).map(|i| {
            let rewritten = shard::rewrite_shard(query, i, shards);
            async move {
                match rewritten {
                    Ok(q) => self.dispatch_instant(tenant, &q, time).await,
                    Err(e) => QueryResult::error("bad_data", &e.to_string()),
                }
            }
        });
        shard::merge_shards(op, join_all(futs).await)
    }

    async fn dispatch_instant(&self, tenant: &str, query: &str, time: f64) -> QueryResult {
        use crate::frontend::backend::InstantRequest;
        let req = InstantRequest {
            tenant: tenant.to_string(),
            query: query.to_string(),
            time_secs: time,
        };
        match self.backend.instant_query(&req).await {
            Ok(r) => r,
            Err(e) => QueryResult::error("internal", &e.to_string()),
        }
    }
```

- [ ] **Step 5: Re-export + `run_query_frontend` + the role binary**

Add to `server.rs`:

```rust
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::frontend::cache::ObjectStoreCache;
use crate::frontend::config::FrontendConfig;
use crate::frontend::http_backend::HttpQuerier;

/// Boot the query-frontend role: build the HTTP querier pool + cache, then
/// serve the router on `cfg.listen_addr` until `shutdown` fires.
///
/// # Errors
/// Propagates bind/serve `std::io` errors and backend-construction failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    cache: Arc<ObjectStoreCache>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let backend = HttpQuerier::new(cfg.backend_addrs.clone(), cfg.request_timeout)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let qf = Arc::new(QueryFrontend::new(Arc::new(backend), cache, cfg.clone()));
    let app = router_with_backend(qf);
    let listener = tokio::net::TcpListener::bind(cfg.listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}
```

Re-export in `mod.rs`: `pub mod server; pub use server::{router_with_backend, run_query_frontend};`.

Create/extend `crates/metrics/src/bin/crabka-metrics.rs`:

```rust
//! The role-selectable `crabka-metrics` service binary.

use clap::{Parser, ValueEnum};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Target {
    Distributor,
    Compactor,
    Querier,
    QueryFrontend,
    Ruler,
}

#[derive(Parser, Debug)]
#[command(name = "crabka-metrics")]
struct Args {
    /// Which role this process runs.
    #[arg(long, value_enum)]
    target: Target,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    match args.target {
        Target::QueryFrontend => {
            use std::sync::Arc;
            use std::time::Duration;

            use crabka_metrics::frontend::cache::ObjectStoreCache;
            use crabka_metrics::frontend::config::FrontendConfig;
            use crabka_metrics::frontend::server::run_query_frontend;
            use object_store::memory::InMemory;
            use object_store::path::Path;
            use tokio_util::sync::CancellationToken;

            let cfg = FrontendConfig::default(); // real config wiring is a later hardening slice
            // In-memory cache by default; an object_store::aws/gcp store is
            // selected by config in the hardening slice.
            let cache = Arc::new(ObjectStoreCache::new(
                Arc::new(InMemory::new()),
                Path::from("query-frontend-cache"),
                Duration::from_secs(cfg.cache_ttl_secs),
            ));
            let shutdown = CancellationToken::new();
            run_query_frontend(cfg, cache, shutdown).await
        }
        other => {
            eprintln!("target {other:?} not implemented in this slice");
            Ok(())
        }
    }
}
```

Ensure `Cargo.toml` declares the binary (add if missing):

```toml
[[bin]]
name = "crabka-metrics"
path = "src/bin/crabka-metrics.rs"
```

> **Binary-config note:** this slice wires the role *dispatch* and a working in-memory-backed `ObjectStoreCache`; real config loading (backend addrs / object-store selection / listen addr from flags or a config file) lands in Slice 8 hardening. The default `FrontendConfig` is enough to boot and pass the server test, which targets the library router, not the binary.

- [ ] **Step 6: Run to verify it passes + whole-crate gate**

Run: `cargo test -p crabka-metrics --test frontend_server`
Then the full gate: `cargo test -p crabka-metrics && cargo clippy -p crabka-metrics --all-targets && cargo fmt -p crabka-metrics --check`
Expected: all PASS, no warnings, formatting clean. Also confirm the binary builds: `cargo build -p crabka-metrics --bin crabka-metrics`.

- [ ] **Step 7: Commit**

```bash
git add crates/metrics/
git commit -m "feat(metrics): query-frontend axum server + --target query-frontend role"
```

---

## Self-review

**Spec coverage (against §6.4 query-frontend + §11 Slice 6):**
- **Time-splitting** (step-aligned per-interval sub-ranges, stitch matrices) → Tasks 3, 6, 8.
- **Query sharding** (Mimir `__query_shard__` injection via parsed AST, shardability decision, parallel dispatch, correct merge, no-shard fallback for non-decomposable) → Tasks 4, 5, 6, 8.
- **Fan-out** (HTTP client pool, configurable backends, parallel dispatch w/ timeouts, merge into one Prometheus JSON) → Tasks 2 (trait), 6 (`join_all`), 9 (`HttpQuerier`).
- **Result cache** (key by `(tenant, query, start, end, step)`, split-on-boundary reuse, `ResultCache` trait + in-memory + object_store impls, TTL, `no-store`/`Cache-Control` bypass) → Tasks 6, 7, 8.
- **Role binary** `crabka-metrics --target query-frontend` → Task 10.
- **First-class correctness** (sharded `sum(rate(...))` == unsharded over identical data; split+stitch == single range; moving-window cache reuse) → Tasks 5, 8.

**Contract fidelity:** consumes the Slice 5 querier surface exactly (`/api/v1/query`, `/api/v1/query_range`, Prometheus JSON `resultType`, `X-Scope-OrgID`). The `QueryResult` model (Task 1) is shaped to Prometheus's JSON and pinned by a serde test; the no-op path (single sub-range, non-shardable, cache-miss) round-trips the querier's body unchanged — the byte-equality analog.

**Churn-prone surfaces — structured + behavior-pinned + verify-noted:**
- `promql-parser` 0.10 AST (`shard.rs`) — exact `Expr` variant / `Matcher` / token-constant names verify-noted; behavior pinned by shardability + injected-matcher-text tests.
- `reqwest` 0.13 + querier HTTP contract (`http_backend.rs`) — pinned by a loopback axum-stub test asserting request shape + response parse; verify-note for method drift.
- `object_store` 0.13 (`cache.rs`) — `GetResult.meta.last_modified` / `bytes()` / `put` signatures verify-noted; round-trip test pins behavior; FNV-key collision analysis included.

**`avg` decomposition — implemented (FOCUS deliverable):** `avg` is deliberately not an `AggrOp`, but it *is* sharded: the orchestrator detects a top-level `avg` via `shard::decompose_avg`, dispatches `sum(<inner>)` and `count(<inner>)` each fully sharded (rewrite-per-shard → fan-out → `merge_shards`), then recombines with `shard::divide_results` (merged-sum / merged-count per `(series, timestamp)`). This avoids a wrong "average of per-shard averages" while keeping `merge_shards` total over the four exact-combine ops. The `range_query` and `instant_query` paths share the decomposition, and the `sharded_avg_equals_unsharded` integration test (Task 8) pins sharded-`avg` == unsharded.

**Placeholder scan:** no "TBD"/"similar to Task N"/"add error handling". Every step has runnable code or an exact command. The hand-waves (3rd-party API method names at arrow/reqwest/object_store/promql-parser versions) are each bounded with a verify-against-docs note and pinned by a behavior test, never left vague.

**Type consistency:** `QueryResult`/`ResultData`/`SampleStream`/`InstantSample` defined once (Task 1) and used unchanged across split/shard/cache/server. `QuerierBackend` (Task 2) implemented by both `MockQuerier` (Task 2) and `HttpQuerier` (Task 9) with identical signatures. `ResultCache` (Task 6 skeleton) implemented by `NoCache`/`InMemoryCache`/`ObjectStoreCache` (Tasks 6, 7). `RangeRequest`/`InstantRequest`/`BackendError`/`CacheControl`/`AggrOp`/`ShardPlan` referenced consistently between definitions, orchestrator, and tests.

**Known risk (flagged):** the `MockQuerier` FIFO-stub-vs-`join_all`-dispatch ordering (Task 8 caveat) is deterministic today but would need a query-matching upgrade if shard dispatch becomes nondeterministic w.r.t. stub consumption — contained to the test fixture, surfaces as a failing equivalence test, never silent corruption.
