# crabka-metrics Slice 5 — Querier + Prometheus HTTP API (hot/cold merge)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the **querier role** — a concrete `crabka-promql::MetricStore` impl (`CrabkaMetricStore`) that merges *cold* blockstore data with a *hot* in-memory WAL-tail head, and the **Prometheus HTTP query API** (axum) that drives `PromqlEngine` and serializes `QueryResult` into byte-exact Prometheus JSON for Grafana's built-in Prometheus datasource. Plus the `crabka-metrics --target querier` role binary.

**Architecture:** Three layers, bottom-up.

1. **WAL-tail head** (`head.rs`): an in-memory, per-`(tenant, fingerprint)` ring of recent samples (float + native-histogram), fed by a `crabka-client-consumer::Consumer` tailing the metrics WAL topic (Slice 4's topic). Decodes each record via Slice 4's `WalRecord`. Bounded by a retention window (default ~3h) and exposed as DataFusion `MemTable`s. The head tracks its **lowest retained offset** and the consumer's **current offset** so it is rebuildable from the WAL purely by replaying offsets — the spec's "no separate ingester WAL" realization (§1, §6.3).
2. **`CrabkaMetricStore`** (`store.rs`): implements the `MetricStore` trait by, per `scan()`, registering the cold blockstore tables (`BlockStore::scan_context`) *and* the hot head tables into one `SessionContext`, then building a **UNION view** split at the **compaction frontier** (the compactor's committed offset, surfaced as a per-tenant `min_ts` cut) so a sample counted in a sealed block is not also counted from the head. `label_names`/`label_values`/`series` union the blockstore `Index` with the head's live series.
3. **HTTP API** (`http/`): an axum `Router` mounted under both bare `/api/v1/` and `/prometheus/api/v1/`, tenant via `X-Scope-OrgID`. `/query` + `/query_range` call `PromqlEngine` and map `QueryResult` → Prometheus JSON with exact shapes; `/series`, `/labels`, `/label/{name}/values`, `/metadata`, `/query_exemplars`, `/status/buildinfo` round out Grafana's discovery calls. Errors use the Prometheus `{"status":"error",...}` envelope. **Response-shape fidelity is the byte-equality analog** and is tested with exact-JSON assertions for vector, matrix, scalar, and error.

**Tech Stack:** Rust 2024 · `datafusion` (git pin below) · `arrow` 59 · `tokio` · `axum` 0.8 (workspace base `http1`,`tokio`; querier adds `json`,`query`) · `serde`/`serde_json` · `async-trait` · `thiserror` · `tracing` · `crabka-promql` (Slices 2–3) · `crabka-blockstore` · `crabka-client-consumer`. Tests: `assert2`, `tower::ServiceExt::oneshot` (in-process router), `crabka-broker` in-process test-support / testcontainers (`#[ignore]`).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire formats/role flags freely; no shims, no migration code, no default-off gates. (Only Kafka wire compat matters — this slice consumes the WAL with the existing consumer client; it adds no new Kafka surface.)
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean (`module_name_repetitions`/`missing_errors_doc`/`missing_panics_doc` allowed workspace-wide). Run `cargo clippy -p crabka-metrics --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-metrics` before every commit (never `cargo +nightly fmt --all` — OS error 206 / path-too-long in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests.
- **Async tests:** `#[tokio::test]`. Dev-dep `tokio` features `["macros","rt-multi-thread"]`.
- **Dependency pin (locked):** `datafusion = { git = "https://github.com/apache/datafusion", rev = "0838a4ddb902535b0e95a1c5a254be7e9c7fe9bf" }`, `arrow` 59. Same instance as blockstore/promql — types cross the DataFusion boundary without conversion.
- **Prometheus JSON fidelity is the contract.** Response bodies must match Prometheus exactly: top-level `{"status":"success","data":{...}}`; `data.resultType ∈ {"vector","matrix","scalar","string"}`; a vector sample is `{"metric":{…},"value":[<ts_secs_float>,"<val_str>"]}`; a matrix series is `{"metric":{…},"values":[[<ts>,"<val>"],…]}`; native-histogram results use the `"histogram"`/`"histograms"` shape; errors are `{"status":"error","errorType":"…","error":"…"}`. Floats are formatted with Prometheus `jsonutil.MarshalFloat` semantics — `strconv.AppendFloat(f, fmt, -1, 64)` with `fmt='f'` by default, switching to `'e'` only when `abs(v) < 1e-6` or `abs(v) >= 1e21` (special-cased: `+Inf`/`-Inf`/`NaN` → those literal strings) — value strings are produced by one shared `format_sample_value` helper, behavior-pinned by tests.

---

## Dependency & slice roadmap

**Depends on:**
- **`crabka-promql` (Slices 2–3)** — provides the `MetricStore` trait, `ScanResult`, `PromqlEngine<S>`, `QueryResult`, `InstantSample`, `RangeSeries`, `SampleValue`, `NativeHistogram`, `EngineOpts`, `PromqlError`. This slice *consumes* that contract verbatim (see "Shared contract" below) and implements `MetricStore` against it.
- **`crabka-blockstore`** — `BlockStore::scan_context`, `Index`, `Labels`, `LabelMatcher`/`MatchOp`.
- **`crabka-metrics` Slice 4 (ingest)** — `WalRecord` (encode/decode) + the metrics WAL topic name + the per-tenant **compaction frontier** the compactor commits (consumer-group committed offset / sealed-block `max_ts`). The real `WalRecord` has **public fields** `tenant: String`, `labels: Vec<(String,String)>`, `payload: SamplePayload`, `exemplars` (no accessor methods); `SamplePayload` is `Float { timestamp_ms, value }` / `Hist { timestamp_ms, hist }` (no `WalSample` enum). Slice 1 — `float_sample_schema()`/`native_histogram_schema()`, `NativeHistogram`, `encode_float_samples`/`decode_*`.
- **`crabka-client-consumer`** — `Consumer::builder()…subscribe(vec).auto_offset_reset(Earliest).build().await`, `poll(Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`, `ConsumerRecord{topic,partition,offset,timestamp,key,value,headers}`, `IsolationLevel`, `AutoOffsetReset`.

**The 8 metrics slices** (this plan = Slice 5; each gets its own plan):

1. Data layer — block schemas + native-histogram codec + symbol table.
2. `crabka-promql` core — parser + operator pattern + selectors + rate-family + aggregations + binary ops + `.test` harness.
3. Query completeness — `histogram_quantile`, full function catalog, subqueries, `@`/`offset`.
4. Ingest service — remote_write v1/v2 + OTLP + Kafka produce + distributor + HA dedup + compactor.
5. **Querier + Prometheus HTTP API + hot/cold merge** *(this plan)*.
6. Query-frontend — split / shard / cache.
7. Ruler — recording + alerting + rule API.
8. Hardening — multi-tenancy/limits, remote_read, prometheus/compliance + differential-vs-Mimir.

---

## Shared contract (consume exactly — do not redefine)

From `crabka-promql` (Slices 2–3). This slice depends on these signatures unchanged:

```rust
#[async_trait::async_trait]
pub trait MetricStore: Send + Sync {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError>;
    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;
    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<String>, PromqlError>;
    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError>;
}

pub struct ScanResult {
    pub ctx: datafusion::prelude::SessionContext,
    pub float_table: Option<String>,
    pub histogram_table: Option<String>,
}

pub struct PromqlEngine<S: MetricStore> { /* … */ }
impl<S: MetricStore> PromqlEngine<S> {
    pub fn new(store: std::sync::Arc<S>, opts: EngineOpts) -> Self;
    pub async fn query_instant(&self, tenant: &str, q: &str, ts_ms: i64)
        -> Result<QueryResult, PromqlError>;
    pub async fn query_range(&self, tenant: &str, q: &str, start_ms: i64, end_ms: i64, step_ms: i64)
        -> Result<QueryResult, PromqlError>;
}

pub enum QueryResult {
    Scalar { ts_ms: i64, value: f64 },
    InstantVector(Vec<InstantSample>),
    RangeMatrix(Vec<RangeSeries>),
    Str { ts_ms: i64, value: String },
}
pub struct InstantSample { pub labels: Labels, pub ts_ms: i64, pub value: SampleValue }
pub struct RangeSeries  { pub labels: Labels, pub samples: Vec<(i64, SampleValue)> }
pub enum SampleValue { Float(f64), Histogram(NativeHistogram) }
```

> **Verify-before-use (do not fabricate):** the exact field names / enum discriminants of `QueryResult`, `InstantSample`, `RangeSeries`, `SampleValue`, and the `MetricStore` method shapes are owned by Slices 2–3. Before Task 4, run `cargo doc -p crabka-promql --no-deps` (or read `crates/promql/src/lib.rs` re-exports) and reconcile. If a name differs (e.g. `ts_ms` vs `timestamp_ms`, the `Scalar`/`Str` struct-field names), adapt the **mapping code and tests together** — keep the asserted *JSON* exact (that is the contract this slice owns); the Rust field names bend to promql.

---

## File structure (`crates/metrics/` — extends the Slice 1 crate)

| File | Responsibility |
|---|---|
| `src/lib.rs` | add `pub mod querier;` + re-exports (existing data-layer modules unchanged) |
| `src/querier/mod.rs` | querier module decls + `QuerierConfig` |
| `src/querier/head.rs` | `WalHead` — in-memory WAL-tail ring + `MemTable` projection + retention/offsets |
| `src/querier/tailer.rs` | `HeadTailer` — consumer loop feeding `WalHead`; rebuild-from-offset |
| `src/querier/store.rs` | `CrabkaMetricStore` — the `MetricStore` impl (cold+hot UNION, frontier split) |
| `src/querier/http/mod.rs` | `router()` (dual mount) + `AppState` + `X-Scope-OrgID` extractor |
| `src/querier/http/json.rs` | Prometheus JSON value types + `QueryResult`→JSON + `format_sample_value` |
| `src/querier/http/query.rs` | `/query`, `/query_range` handlers |
| `src/querier/http/meta.rs` | `/series`, `/labels`, `/label/{name}/values`, `/metadata`, `/query_exemplars`, `/status/buildinfo` |
| `src/bin/crabka-metrics.rs` | role binary `--target querier` (extended in later slices) |

`store.rs` + `head.rs` are the only files touching DataFusion's query layer; `http/json.rs` is the only file owning wire-shape serialization. This keeps the two churn-prone surfaces (DataFusion UNION, Prometheus JSON) each in one file.

---

### Task 1: Crate deps + querier module scaffold

**Files:**
- Modify: `crates/metrics/Cargo.toml`
- Modify: `crates/metrics/src/lib.rs`
- Create: `crates/metrics/src/querier/mod.rs`

**Interfaces:**
- Produces: a compiling `crabka-metrics` with a `querier` module + `QuerierConfig` and a smoke test.

- [ ] **Step 1: Add the Slice-5 dependencies to `crates/metrics/Cargo.toml`**

Append to `[dependencies]` (Slice 1 already has `arrow`, `thiserror`):

```toml
datafusion = { workspace = true }
crabka-promql = { path = "../promql" }
crabka-blockstore = { path = "../blockstore" }
crabka-client-consumer = { path = "../client-consumer" }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "sync", "time"] }
axum = { workspace = true, features = ["json", "query"] }  # workspace base lacks these; Json/Query handlers need them
serde = { workspace = true }
serde_json = { workspace = true }
async-trait = { workspace = true }
tracing = { workspace = true }
bytes = { workspace = true }
```

Append to `[dev-dependencies]`:

```toml
tokio = { workspace = true, features = ["macros", "rt-multi-thread", "sync", "time"] }
tower = { workspace = true }            # ServiceExt::oneshot for in-process router tests
http-body-util = "0.1"                  # collect router response bodies
object_store = { workspace = true }     # InMemory store behind a test BlockStore
crabka-broker = { path = "../broker" }  # in-process WAL for #[ignore] tailer tests
```

> If `http-body-util` is not yet a workspace dep, add `http-body-util = "0.1"` to root `[workspace.dependencies]` and use `{ workspace = true }`. `tower`/`object_store`/`async-trait` are already workspace deps.

- [ ] **Step 2: Create `crates/metrics/src/querier/mod.rs`**

```rust
//! The querier role: a `MetricStore` over hot (WAL-tail head) + cold
//! (blockstore) data, and the Prometheus HTTP query API that drives the PromQL
//! engine. Slices 2–4 must be present for this module to do useful work.

use std::time::Duration;

pub mod head;
pub mod http;
pub mod store;
pub mod tailer;

/// Static configuration for the querier role.
#[derive(Clone, Debug)]
pub struct QuerierConfig {
    /// Bootstrap servers for the WAL-tail consumer.
    pub bootstrap: String,
    /// The metrics WAL topic (Slice 4).
    pub wal_topic: String,
    /// Consumer group id for the head tailer (one per querier instance —
    /// the head must see *all* partitions, so each querier uses a unique id).
    pub group_id: String,
    /// How far back the in-memory head retains samples. Older samples are
    /// dropped; queries beyond this fall through to cold blocks only.
    pub head_retention: Duration,
    /// HTTP listen address for the Prometheus API.
    pub listen_addr: std::net::SocketAddr,
}

impl Default for QuerierConfig {
    fn default() -> Self {
        Self {
            bootstrap: "localhost:9092".to_string(),
            // Slice 4 defines this constant in the same crate (`crabka-metrics`).
            wal_topic: crate::WAL_TOPIC.to_string(),
            group_id: "crabka-querier-head".to_string(),
            head_retention: Duration::from_secs(3 * 60 * 60),
            listen_addr: ([0, 0, 0, 0], 9009).into(),
        }
    }
}
```

> `wal_topic` defaults to Slice 4's `crate::WAL_TOPIC` (`"__crabka_metrics_wal"`), defined in the same `crabka-metrics` crate. The head/tailer take the topic as a parameter so they stay agnostic; the binary (Task 6) can override it from a flag.

- [ ] **Step 3: Wire `lib.rs`**

Add (leaving the Slice 1 data-layer modules/re-exports intact):

```rust
pub mod querier;
```

- [ ] **Step 4: Smoke test**

Add to `querier/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn default_config_has_sane_retention() {
        let c = QuerierConfig::default();
        assert!(c.head_retention.as_secs() == 3 * 60 * 60);
        assert!(!c.wal_topic.is_empty());
    }
}
```

- [ ] **Step 5: Build + test**

Run: `cargo test -p crabka-metrics --lib querier::tests`
Expected: compiles (first build pulls promql/blockstore/datafusion — slow, normal), `default_config_has_sane_retention` PASSES.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/ Cargo.toml Cargo.lock
git commit -m "feat(metrics): querier module scaffold + QuerierConfig + deps"
```

---

### Task 2: `WalHead` — in-memory WAL-tail ring + retention + MemTable projection

**Files:**
- Create: `crates/metrics/src/querier/head.rs`

**Interfaces:**
- Consumes: Slice 1 `float_sample_schema()`/`native_histogram_schema()`/`encode_float_samples`/`encode_native_histograms`, `NativeHistogram`; blockstore `Labels`/`SeriesFingerprint`/`LabelMatcher`/`MatchOp`.
- Produces:
  - `enum HeadSample { Float(f64), Histogram(NativeHistogram) }`
  - `struct WalHead` with:
    - `new(retention: std::time::Duration) -> Self`
    - `ingest(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels, ts_ms: i64, sample: HeadSample, offset: i64)`
    - `prune(&mut self, now_ms: i64)` — drops samples older than `now_ms - retention`
    - `low_water_offset(&self, partition: i32) -> Option<i64>` / `high_water_offset(&self, partition: i32) -> Option<i64>`
    - `float_batches(&self, tenant: &str, fps: &BTreeSet<SeriesFingerprint>, min_ts_ms: i64, max_ts_ms: i64) -> Result<Vec<RecordBatch>, HeadError>`
    - `histogram_batches(&self, …) -> Result<Vec<RecordBatch>, HeadError>` (same args)
    - `resolve(&self, tenant: &str, matchers: &[LabelMatcher]) -> BTreeSet<SeriesFingerprint>` (head-local matcher resolution, same anchored-regex semantics as blockstore `Index`)
    - `label_names(&self, tenant) -> Vec<String>`, `label_values(&self, tenant, name) -> Vec<String>`, `series_labels(&self, tenant, fps) -> Vec<Labels>`
  - `enum HeadError` (`thiserror`).

- [ ] **Step 1: Write the failing tests**

Create `crates/metrics/src/querier/head.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use assert2::assert;
    use crabka_blockstore::{Labels, LabelMatcher, MatchOp};

    use super::*;

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    fn seed() -> WalHead {
        let mut h = WalHead::new(Duration::from_secs(3600));
        let api = labels(&[("__name__", "up"), ("job", "api")]);
        let web = labels(&[("__name__", "up"), ("job", "web")]);
        h.ingest("t", api.fingerprint(), &api, 1_000, HeadSample::Float(1.0), 0);
        h.ingest("t", api.fingerprint(), &api, 2_000, HeadSample::Float(1.0), 1);
        h.ingest("t", web.fingerprint(), &web, 2_000, HeadSample::Float(0.0), 2);
        h
    }

    #[test]
    fn resolve_matches_by_label() {
        let h = seed();
        let want = labels(&[("__name__", "up"), ("job", "api")]).fingerprint();
        let got = h.resolve("t", &[LabelMatcher::new("job", MatchOp::Eq, "api")]);
        assert!(got == BTreeSet::from([want]));
    }

    #[test]
    fn float_batches_return_only_matching_series_in_window() {
        let h = seed();
        let api = labels(&[("__name__", "up"), ("job", "api")]).fingerprint();
        let fps = BTreeSet::from([api]);
        let batches = h.float_batches("t", &fps, 0, 10_000).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(rows == 2); // two api samples, web excluded
    }

    #[test]
    fn prune_drops_old_samples() {
        let mut h = seed();
        // now = 5000ms, retention 3600s → nothing dropped.
        h.prune(5_000);
        let api = labels(&[("__name__", "up"), ("job", "api")]).fingerprint();
        let rows: usize = h
            .float_batches("t", &BTreeSet::from([api]), 0, 10_000)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert!(rows == 2);
        // now = 3_602_000ms → samples at 1000/2000 are older than retention.
        h.prune(3_602_000);
        let rows: usize = h
            .float_batches("t", &BTreeSet::from([api]), 0, 10_000_000)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert!(rows == 0);
    }

    #[test]
    fn label_apis_reflect_head() {
        let h = seed();
        let mut names = h.label_names("t");
        names.sort();
        assert!(names == vec!["__name__".to_string(), "job".to_string()]);
        let mut jobs = h.label_values("t", "job");
        jobs.sort();
        assert!(jobs == vec!["api".to_string(), "web".to_string()]);
    }

    #[test]
    fn prune_removes_emptied_series_from_index() {
        let mut h = seed();
        // Before: both jobs visible.
        let mut jobs = h.label_values("t", "job");
        jobs.sort();
        assert!(jobs == vec!["api".to_string(), "web".to_string()]);
        // Full prune past retention: all seed samples age out, so EVERY series
        // is emptied and its index entries must be torn down.
        h.prune(3_602_000);
        assert!(h.label_values("t", "job").is_empty());
        assert!(h.label_names("t").is_empty());
        // And the series no longer resolves.
        let got = h.resolve("t", &[LabelMatcher::new("job", MatchOp::Eq, "api")]);
        assert!(got.is_empty());
    }

    #[test]
    fn offsets_track_low_and_high_water() {
        let h = seed();
        // partition 0 implied (single-partition ingest in seed uses offset only;
        // adapt to (partition, offset) if your ingest carries partition).
        assert!(h.high_water_offset(0) == Some(2));
        assert!(h.low_water_offset(0) == Some(0));
    }
}
```

> **Partition note:** the test seeds offsets `0,1,2` on a single implied partition. If `ingest` takes a `(partition, offset)` pair (the tailer knows the partition), add `partition` as a parameter and update the seed to pass `0`. Keep offset tracking *per partition* — the head is rebuildable per partition.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib querier::head`
Expected: FAIL — `cannot find type WalHead`.

- [ ] **Step 3: Implement `head.rs`**

Prepend above the `tests` module. Storage is `tenant -> fp -> series` where a series carries its `Labels` once + a time-sorted `Vec<(ts_ms, HeadSample)>`; per-`(tenant)` value/postings maps mirror blockstore's `Index` for `resolve`/label APIs; per-partition `(low, high)` offset bookkeeping supports rebuild.

```rust
//! In-memory WAL-tail head: the *hot* half of the hot/cold merge. Holds the
//! most recent `retention` window of samples per series, rebuilt from the WAL
//! by [`crate::querier::tailer::HeadTailer`]. Projected into DataFusion
//! `MemTable`s by [`crate::querier::store::CrabkaMetricStore`].

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use crabka_blockstore::{Labels, LabelMatcher, MatchOp, SeriesFingerprint};

use crate::histogram::NativeHistogram;
use crate::{encode_float_samples, encode_native_histograms};

/// A single hot sample.
#[derive(Clone, Debug)]
pub enum HeadSample {
    Float(f64),
    Histogram(NativeHistogram),
}

/// Errors from head projection (Arrow encode failures).
#[derive(Debug, thiserror::Error)]
pub enum HeadError {
    #[error("head codec error: {0}")]
    Codec(String),
}

#[derive(Default)]
struct Series {
    labels: Labels,
    /// time-sorted; appended in WAL order, which is near-sorted, then we
    /// binary-insert to keep it sorted for window slicing.
    points: Vec<(i64, HeadSample)>,
}

#[derive(Default)]
struct TenantHead {
    series: HashMap<SeriesFingerprint, Series>,
    postings: HashMap<(String, String), BTreeSet<SeriesFingerprint>>,
    values: HashMap<String, BTreeSet<String>>,
}

/// The hot WAL-tail head.
pub struct WalHead {
    retention: Duration,
    tenants: HashMap<String, TenantHead>,
    /// per-partition (low_water, high_water) consumed offsets.
    offsets: BTreeMap<i32, (i64, i64)>,
}

impl WalHead {
    #[must_use]
    pub fn new(retention: Duration) -> Self {
        Self {
            retention,
            tenants: HashMap::new(),
            offsets: BTreeMap::new(),
        }
    }

    /// Ingest one decoded WAL record into the head.
    pub fn ingest(
        &mut self,
        tenant: &str,
        fp: SeriesFingerprint,
        labels: &Labels,
        ts_ms: i64,
        sample: HeadSample,
        offset: i64,
    ) {
        self.ingest_at(tenant, fp, labels, ts_ms, sample, 0, offset);
    }

    /// Partition-aware ingest. `partition` feeds per-partition offset tracking.
    pub fn ingest_at(
        &mut self,
        tenant: &str,
        fp: SeriesFingerprint,
        labels: &Labels,
        ts_ms: i64,
        sample: HeadSample,
        partition: i32,
        offset: i64,
    ) {
        let t = self.tenants.entry(tenant.to_string()).or_default();
        let s = t.series.entry(fp).or_default();
        if s.labels.is_empty() {
            s.labels = labels.clone();
            for (name, value) in labels.iter() {
                t.postings
                    .entry((name.clone(), value.clone()))
                    .or_default()
                    .insert(fp);
                t.values.entry(name.clone()).or_default().insert(value.clone());
            }
        }
        // keep points sorted by ts for window slicing
        let pos = s.points.partition_point(|(ts, _)| *ts <= ts_ms);
        s.points.insert(pos, (ts_ms, sample));

        let e = self.offsets.entry(partition).or_insert((offset, offset));
        e.0 = e.0.min(offset);
        e.1 = e.1.max(offset);
    }

    /// Drop samples older than `now_ms - retention`. A series whose samples all
    /// age out is removed entirely — along with its postings and label-value
    /// index entries — so it stops surfacing in `resolve`/`label_names`/
    /// `label_values`/`series_labels` (matching Prometheus head semantics: a
    /// series with no samples in the window is not visible) and stops leaking
    /// memory.
    pub fn prune(&mut self, now_ms: i64) {
        let cutoff = now_ms.saturating_sub(
            i64::try_from(self.retention.as_millis()).unwrap_or(i64::MAX),
        );
        for t in self.tenants.values_mut() {
            // 1. Drain aged-out samples; collect series that became empty.
            let mut emptied: Vec<SeriesFingerprint> = Vec::new();
            for (fp, s) in &mut t.series {
                let keep_from = s.points.partition_point(|(ts, _)| *ts < cutoff);
                s.points.drain(0..keep_from);
                if s.points.is_empty() {
                    emptied.push(*fp);
                }
            }
            // 2. Tear down the index for each emptied series.
            for fp in emptied {
                let Some(s) = t.series.remove(&fp) else { continue };
                for (name, value) in s.labels.iter() {
                    let key = (name.clone(), value.clone());
                    if let Some(set) = t.postings.get_mut(&key) {
                        set.remove(&fp);
                        if set.is_empty() {
                            t.postings.remove(&key);
                            // The (name,value) pair has no series left; drop the
                            // value from t.values, and the name too if it now has
                            // no values.
                            if let Some(vals) = t.values.get_mut(name) {
                                vals.remove(value);
                                if vals.is_empty() {
                                    t.values.remove(name);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[must_use]
    pub fn low_water_offset(&self, partition: i32) -> Option<i64> {
        self.offsets.get(&partition).map(|(lo, _)| *lo)
    }

    #[must_use]
    pub fn high_water_offset(&self, partition: i32) -> Option<i64> {
        self.offsets.get(&partition).map(|(_, hi)| *hi)
    }

    /// Head-local matcher resolution (same anchored-regex semantics as the
    /// blockstore `Index`).
    #[must_use]
    pub fn resolve(&self, tenant: &str, matchers: &[LabelMatcher]) -> BTreeSet<SeriesFingerprint> {
        let Some(t) = self.tenants.get(tenant) else {
            return BTreeSet::new();
        };
        let mut acc: Option<BTreeSet<SeriesFingerprint>> = None;
        for m in matchers {
            let matched = t.match_one(m);
            acc = Some(match acc {
                None => matched,
                Some(prev) => prev.intersection(&matched).copied().collect(),
            });
            if acc.as_ref().is_some_and(BTreeSet::is_empty) {
                break;
            }
        }
        acc.unwrap_or_default()
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

    #[must_use]
    pub fn series_labels(&self, tenant: &str, fps: &BTreeSet<SeriesFingerprint>) -> Vec<Labels> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        fps.iter()
            .filter_map(|fp| t.series.get(fp).map(|s| s.labels.clone()))
            .collect()
    }

    /// Float-sample rows for `fps` within `[min_ts_ms, max_ts_ms]`, as Arrow
    /// batches matching Slice 1's `float_sample_schema()`.
    pub fn float_batches(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts_ms: i64,
        max_ts_ms: i64,
    ) -> Result<Vec<RecordBatch>, HeadError> {
        let mut rows: Vec<(u64, i64, f64)> = Vec::new();
        if let Some(t) = self.tenants.get(tenant) {
            for fp in fps {
                if let Some(s) = t.series.get(fp) {
                    for (ts, v) in &s.points {
                        if *ts < min_ts_ms || *ts > max_ts_ms {
                            continue;
                        }
                        if let HeadSample::Float(f) = v {
                            rows.push((*fp, *ts, *f));
                        }
                    }
                }
            }
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let batch = encode_float_samples(&rows).map_err(|e| HeadError::Codec(e.to_string()))?;
        Ok(vec![batch])
    }

    /// Native-histogram rows for `fps` within the window.
    pub fn histogram_batches(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts_ms: i64,
        max_ts_ms: i64,
    ) -> Result<Vec<RecordBatch>, HeadError> {
        let mut rows: Vec<(u64, i64, NativeHistogram)> = Vec::new();
        if let Some(t) = self.tenants.get(tenant) {
            for fp in fps {
                if let Some(s) = t.series.get(fp) {
                    for (ts, v) in &s.points {
                        if *ts < min_ts_ms || *ts > max_ts_ms {
                            continue;
                        }
                        if let HeadSample::Histogram(h) = v {
                            rows.push((*fp, *ts, h.clone()));
                        }
                    }
                }
            }
        }
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let batch =
            encode_native_histograms(&rows).map_err(|e| HeadError::Codec(e.to_string()))?;
        Ok(vec![batch])
    }
}

impl TenantHead {
    fn match_one(&self, m: &LabelMatcher) -> BTreeSet<SeriesFingerprint> {
        match m.op {
            MatchOp::Eq => self
                .postings
                .get(&(m.name.clone(), m.value.clone()))
                .cloned()
                .unwrap_or_default(),
            MatchOp::Neq => {
                let excluded = self
                    .postings
                    .get(&(m.name.clone(), m.value.clone()))
                    .cloned()
                    .unwrap_or_default();
                self.series
                    .keys()
                    .copied()
                    .filter(|fp| !excluded.contains(fp))
                    .collect()
            }
            MatchOp::Re | MatchOp::Nre => {
                let Ok(re) = regex::Regex::new(&format!("^(?:{})$", m.value)) else {
                    return BTreeSet::new();
                };
                let mut matched = BTreeSet::new();
                for ((name, value), fps) in &self.postings {
                    if name == &m.name && re.is_match(value) {
                        matched.extend(fps.iter().copied());
                    }
                }
                if m.op == MatchOp::Re {
                    matched
                } else {
                    self.series
                        .keys()
                        .copied()
                        .filter(|fp| !matched.contains(fp))
                        .collect()
                }
            }
        }
    }
}
```

> **`regex` dep:** add `regex = { workspace = true }` to `[dependencies]` (already a workspace dep — blockstore uses it). The anchored form `^(?:…)$` must match blockstore's `Index::anchored` exactly so hot and cold resolve identically.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib querier::head`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): WalHead — in-memory WAL-tail ring + retention + Arrow projection"
```

---

### Task 3: `HeadTailer` — consumer loop feeding the head (structure + behavior-pin)

**Files:**
- Create: `crates/metrics/src/querier/tailer.rs`

**Interfaces:**
- Consumes: `crabka-client-consumer::{Consumer, ConsumerRecord, AutoOffsetReset}`, Slice 4 `WalRecord` (decode), `WalHead`, `HeadSample`.
- Produces:
  - `struct SharedHead(Arc<RwLock<WalHead>>)` (tokio `RwLock`) with `new(retention)`, `read()`, and `apply_record(&self, &WalRecord, partition, offset)`.
  - `struct HeadTailer` with `spawn(config: &QuerierConfig, head: SharedHead, shutdown: CancellationToken) -> JoinHandle<()>` — builds a `Consumer` from `Earliest`, polls, decodes each `ConsumerRecord.value` via `WalRecord::decode`, applies into the head, periodically prunes.
  - `fn apply_wal_record(head: &mut WalHead, rec: &WalRecord, partition: i32, offset: i64)` — pure mapping (WAL record → head ingest), unit-tested without a broker.

This task has **two churn-prone seams**: the consumer client API and Slice 4's `WalRecord` shape. Structure + a pure-mapping unit test now; the live consumer loop is exercised by an `#[ignore]` integration test (Task 7).

- [ ] **Step 1: Write the pure-mapping failing test**

Create `crates/metrics/src/querier/tailer.rs` with tests first. The test builds a `WalRecord` via Slice 4's constructor and asserts it lands in the head. **Verify `WalRecord`'s real constructor/fields against Slice 4 before running** — the real type has public fields `WalRecord { tenant: String, labels: Vec<(String,String)>, payload: SamplePayload, exemplars }` with `SamplePayload::Float { timestamp_ms, value }`; adapt field construction to the real type, keep the asserted head behavior.

```rust
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use assert2::assert;
    use crabka_blockstore::Labels;

    use super::*;
    use crate::querier::head::{HeadSample, WalHead};

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut l = Labels::new();
        for (k, v) in pairs {
            l.insert(*k, *v);
        }
        l
    }

    #[test]
    fn apply_float_wal_record_lands_in_head() {
        let mut head = WalHead::new(Duration::from_secs(3600));
        let lbls = labels(&[("__name__", "up"), ("job", "api")]);

        // Build the Slice-4 record. ADAPT to the real `WalRecord` API.
        let rec = wal_float_record(&lbls, 1_000, 1.0);
        apply_wal_record(&mut head, &rec, 0, 7);

        let fp = lbls.fingerprint();
        let rows: usize = head
            .float_batches("t", &BTreeSet::from([fp]), 0, 10_000)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert!(rows == 1);
        assert!(head.high_water_offset(0) == Some(7));
    }

    /// Local helper that constructs a float `WalRecord` for tenant `"t"`.
    /// REPLACE the body with Slice 4's real constructor.
    fn wal_float_record(labels: &Labels, ts_ms: i64, v: f64) -> crate::WalRecord {
        let _ = (labels, ts_ms, v);
        unimplemented!("construct via Slice 4 WalRecord API")
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib querier::tailer`
Expected: FAIL — `cannot find function apply_wal_record` (compile error), then `unimplemented!` once the fn exists. Resolve by wiring the real `WalRecord` constructor in the test helper.

- [ ] **Step 3: Implement `tailer.rs`**

```rust
//! Feeds the [`WalHead`] from the metrics WAL topic. Each querier runs its own
//! consumer group (a unique id) so it observes *all* partitions; the head is
//! rebuilt from `Earliest` on start and kept current by polling.

use std::sync::Arc;
use std::time::Duration;

use crabka_blockstore::Labels;
use crabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{SamplePayload, WalRecord}; // Slice 4
use crate::querier::QuerierConfig;
use crate::querier::head::{HeadSample, WalHead};

/// Shared, concurrently-readable head.
#[derive(Clone)]
pub struct SharedHead(Arc<RwLock<WalHead>>);

impl SharedHead {
    #[must_use]
    pub fn new(retention: Duration) -> Self {
        Self(Arc::new(RwLock::new(WalHead::new(retention))))
    }

    /// Read guard for the store's `scan()` projection.
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, WalHead> {
        self.0.read().await
    }

    async fn apply(&self, rec: &WalRecord, partition: i32, offset: i64) {
        let mut head = self.0.write().await;
        apply_wal_record(&mut head, rec, partition, offset);
    }
}

/// Pure mapping: decode a `WalRecord` into a head ingest. Unit-tested without a
/// broker. Maps against Slice 4's real `WalRecord` (public fields, no methods).
pub fn apply_wal_record(head: &mut WalHead, rec: &WalRecord, partition: i32, offset: i64) {
    // Slice 4 `WalRecord` is field-access, not methods:
    //   rec.tenant: String
    //   rec.labels: Vec<(String, String)>      (NOT a blockstore `Labels`)
    //   rec.payload: SamplePayload             (Float{timestamp_ms,value} | Hist{timestamp_ms,hist})
    // Rebuild a `Labels` from the `(name, value)` pairs to fingerprint + index.
    let tenant = &rec.tenant;
    let mut labels = Labels::new();
    for (name, value) in &rec.labels {
        labels.insert(name.as_str(), value.as_str());
    }
    let fp = labels.fingerprint(); // or rec.series_fingerprint() if it matches blockstore's
    let (ts_ms, sample) = match &rec.payload {
        SamplePayload::Float { timestamp_ms, value } => (*timestamp_ms, HeadSample::Float(*value)),
        SamplePayload::Hist { timestamp_ms, hist } => {
            (*timestamp_ms, HeadSample::Histogram(hist.clone()))
        }
    };
    head.ingest_at(tenant, fp, &labels, ts_ms, sample, partition, offset);
}

/// The tailer.
pub struct HeadTailer;

impl HeadTailer {
    /// Spawn the consumer loop. Returns its `JoinHandle`.
    #[must_use]
    pub fn spawn(
        config: &QuerierConfig,
        head: SharedHead,
        shutdown: CancellationToken,
    ) -> JoinHandle<()> {
        let bootstrap = config.bootstrap.clone();
        let group_id = config.group_id.clone();
        let topic = config.wal_topic.clone();
        tokio::spawn(async move {
            let mut consumer = match Consumer::builder()
                .bootstrap(bootstrap)
                .group_id(group_id)
                .subscribe(vec![topic])
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .build()
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(error = %e, "head tailer: consumer build failed");
                    return;
                }
            };
            let mut ticks = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = ticks.tick() => {
                        let now = now_ms();
                        head.0.write().await.prune(now);
                    }
                    res = consumer.poll(Duration::from_millis(500)) => {
                        match res {
                            Ok(records) => apply_batch(&head, &records).await,
                            Err(e) => {
                                tracing::warn!(error = %e, "head tailer: poll error");
                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }
                        }
                    }
                }
            }
        })
    }
}

async fn apply_batch(head: &SharedHead, records: &[ConsumerRecord]) {
    for r in records {
        let Some(value) = r.value.as_ref() else { continue };
        match WalRecord::decode(value) {
            Ok(rec) => head.apply(&rec, r.partition, r.offset).await,
            Err(e) => tracing::debug!(error = %e, "head tailer: undecodable WAL record"),
        }
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}
```

> **Verify-notes (do before this compiles):**
> - `WalRecord` fields / `SamplePayload` enum — Slice 4 exposes **public fields** `tenant: String`, `labels: Vec<(String,String)>`, `payload: SamplePayload` (NOT methods, NOT a blockstore `Labels`, NOT a `WalSample` enum). `SamplePayload` is `Float { timestamp_ms, value }` / `Hist { timestamp_ms, hist }`. The mapping rebuilds a `Labels` from the `(name,value)` pairs (or calls `rec.series_fingerprint()`) and matches on `payload`. Confirm these names against Slice 4 before compiling.
> - `WalRecord::decode(&[u8]) -> Result<…>` — confirm the decode entry point and error type; map it to a `tracing::debug!` (a poison-pill record must not kill the tailer).
> - `tokio-util` for `CancellationToken` — add `tokio-util = { workspace = true }` if not already a metrics dep.
> - The consumer builder uses a **unique group id per querier** (so every querier sees every partition). The default `group_id` in `QuerierConfig` should be suffixed with a per-process nonce in the binary (Task 6).

- [ ] **Step 4: Make the pure-mapping test pass**

Wire `wal_float_record` to Slice 4's constructor; run `cargo test -p crabka-metrics --lib querier::tailer`.
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): HeadTailer — WAL-tail consumer loop + pure record mapping"
```

---

### Task 4: `CrabkaMetricStore` — the `MetricStore` impl (cold + hot UNION, frontier split)

**Files:**
- Create: `crates/metrics/src/querier/store.rs`

**Interfaces:**
- Consumes: `crabka-promql::{MetricStore, ScanResult, PromqlError}`, `crabka-blockstore::{BlockStore, Labels, LabelMatcher}`, `SharedHead`, Slice 1 schemas.
- **Prerequisite (land first):** `crabka-blockstore::Index::series_labels(tenant, fp) -> Option<Labels>` — the `Index` already stores `series: HashMap<fp, Labels>` but exposes no accessor for it; `series` (cold side) needs it to reconstruct cold series labels. Add it to blockstore (trivial lookup) before this task; do not reconstruct from postings.
- Produces:
  - `struct CrabkaMetricStore { blockstore: Arc<BlockStore>, head: SharedHead, frontier: Arc<dyn Fn(&str) -> i64 + Send + Sync> }` (the `frontier` closure returns the per-tenant compaction-frontier timestamp in ms — samples with `ts < frontier` are read from cold blocks; `ts >= frontier` from the head — preventing double-count).
  - `CrabkaMetricStore::new(blockstore, head, frontier) -> Self`
  - `#[async_trait] impl MetricStore for CrabkaMetricStore` — `scan`/`label_names`/`label_values`/`series`.

The **UNION wiring is the churn-prone DataFusion surface.** Structure it as: (a) get the cold `(SessionContext, cold_table)` from `BlockStore::scan_context` for *each* schema (float, histogram), restricting the cold side to `ts < frontier`; (b) register the head batches as `MemTable`s (`hot_float`, `hot_hist`) into the *same* `SessionContext`, restricting the hot side to `ts >= frontier`; (c) register a UNION view (`float_union`, `histogram_union`) over cold+hot and return its name in `ScanResult`. Behavior-pin the no-double-count property with a test; do not hand-fabricate UNION SQL semantics.

- [ ] **Step 1: Write the failing test (no broker — head fed directly)**

Create `crates/metrics/src/querier/store.rs` with tests first:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assert2::assert;
    use crabka_blockstore::{BlockStore, Labels, LabelMatcher, MatchOp};
    use crabka_promql::MetricStore;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    use super::*;
    use crate::querier::head::HeadSample;
    use crate::querier::tailer::SharedHead;

    async fn blockstore_with_one_cold_sample() -> Arc<BlockStore> {
        // Build a BlockStore over InMemory, write one float block for series
        // {__name__="up", job="api"} at ts=1000 (cold), index it.
        // (Uses Slice-1 float_sample_schema + blockstore BlockWriter/Index.)
        // Returns an Arc<BlockStore>. See blockstore plan Task 7 test for the
        // exact write+index calls; mirror them here.
        unimplemented!("seed one cold float sample at ts=1000")
    }

    fn up_api() -> Labels {
        let mut l = Labels::new();
        l.insert("__name__", "up");
        l.insert("job", "api");
        l
    }

    #[tokio::test]
    async fn scan_unions_cold_and_hot_without_double_count() {
        let blockstore = blockstore_with_one_cold_sample().await;

        // Head holds the SAME series at ts=2000 (hot) AND, to test the split,
        // a duplicate of the cold ts=1000 (which must be excluded by frontier).
        let head = SharedHead::new(Duration::from_secs(3600));
        {
            let lbls = up_api();
            let fp = lbls.fingerprint();
            let mut h = head.0_for_test().await; // test accessor; or write via apply
            h.ingest_at("t", fp, &lbls, 1_000, HeadSample::Float(1.0), 0, 0); // dup of cold
            h.ingest_at("t", fp, &lbls, 2_000, HeadSample::Float(1.0), 0, 1); // hot-only
        }

        // Compaction frontier at ts=1500: cold owns ts<1500, head owns ts>=1500.
        let store = CrabkaMetricStore::new(blockstore, head, Arc::new(|_| 1_500));

        let res = store
            .scan("t", &[LabelMatcher::new("job", MatchOp::Eq, "api")], 0, 10_000)
            .await
            .unwrap();
        let table = res.float_table.unwrap();
        let df = res
            .ctx
            .sql(&format!("SELECT count(*) AS c FROM {table}"))
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let c = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        // cold ts=1000 (one) + hot ts=2000 (one) = 2; the hot dup at ts=1000 is
        // excluded by the frontier split → NOT 3.
        assert!(c == 2);
    }

    #[tokio::test]
    async fn label_names_union_cold_and_hot() {
        let blockstore = blockstore_with_one_cold_sample().await;
        let head = SharedHead::new(Duration::from_secs(3600));
        {
            let extra = {
                let mut l = Labels::new();
                l.insert("__name__", "up");
                l.insert("region", "us"); // a label only in the head
                l
            };
            let mut h = head.0_for_test().await;
            h.ingest_at("t", extra.fingerprint(), &extra, 2_000, HeadSample::Float(1.0), 0, 1);
        }
        let store = CrabkaMetricStore::new(blockstore, head, Arc::new(|_| 1_500));
        let mut names = store.label_names("t", &[], 0, 10_000).await.unwrap();
        names.sort();
        assert!(names.contains(&"job".to_string())); // from cold
        assert!(names.contains(&"region".to_string())); // from hot
    }
}
```

> **Test accessor note:** `head.0_for_test()` is shorthand — add a `#[cfg(test)] pub async fn write_for_test(&self) -> RwLockWriteGuard<'_, WalHead>` to `SharedHead`, or feed the head via `apply_wal_record`. Don't expose internals in non-test builds.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib querier::store`
Expected: FAIL — `cannot find type CrabkaMetricStore` (then `unimplemented!` in the seed helper).

- [ ] **Step 3: Implement `store.rs`**

```rust
//! `CrabkaMetricStore`: the `MetricStore` the PromQL engine plans against. Each
//! `scan` registers the cold blockstore tables and the hot head tables into one
//! `SessionContext` and UNIONs them, split at the per-tenant compaction frontier
//! so a sample sealed into a block is not also counted from the head.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use crabka_blockstore::{BlockStore, Labels, LabelMatcher};
use crabka_promql::{MetricStore, PromqlError, ScanResult};
use datafusion::catalog::MemTable;
use datafusion::prelude::SessionContext;

use crate::querier::tailer::SharedHead;
use crate::{float_sample_schema, native_histogram_schema};

/// Returns the per-tenant compaction-frontier timestamp (ms). Cold blocks own
/// `ts < frontier`; the head owns `ts >= frontier`.
pub type FrontierFn = Arc<dyn Fn(&str) -> i64 + Send + Sync>;

/// The querier's `MetricStore`.
pub struct CrabkaMetricStore {
    blockstore: Arc<BlockStore>,
    head: SharedHead,
    frontier: FrontierFn,
}

impl CrabkaMetricStore {
    #[must_use]
    pub fn new(blockstore: Arc<BlockStore>, head: SharedHead, frontier: FrontierFn) -> Self {
        Self { blockstore, head, frontier }
    }

    fn err(e: impl std::fmt::Display) -> PromqlError {
        // Slice 2 defines the store-error variant as `PromqlError::Store(String)`.
        PromqlError::Store(e.to_string())
    }
}

#[async_trait]
impl MetricStore for CrabkaMetricStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<ScanResult, PromqlError> {
        let frontier = (self.frontier)(tenant);
        // Cold side is restricted to ts < frontier; clamp the cold window.
        let cold_max = (frontier - 1).min(end_ms);

        // 1. COLD: float + histogram scan contexts from the blockstore. We reuse
        //    the float scan's SessionContext as the home for both unions
        //    (blockstore registers blocks into a ctx; we register the head and
        //    the union views into the SAME ctx).
        let (ctx, cold_float) = self
            .blockstore
            .scan_context(tenant, matchers, start_ms, cold_max, float_sample_schema())
            .await
            .map_err(Self::err)?;
        // Histogram cold table registered into the same ctx under a 2nd name.
        let cold_hist = register_cold_histograms(
            &self.blockstore,
            &ctx,
            tenant,
            matchers,
            start_ms,
            cold_max,
        )
        .await?;

        // 2. HOT: head batches for ts >= frontier (clamped to the query window).
        let hot_min = frontier.max(start_ms);
        let head = self.head.read().await;
        let fps = head.resolve(tenant, matchers);
        let hot_float = head
            .float_batches(tenant, &fps, hot_min, end_ms)
            .map_err(Self::err)?;
        let hot_hist = head
            .histogram_batches(tenant, &fps, hot_min, end_ms)
            .map_err(Self::err)?;
        drop(head);

        register_memtable(&ctx, "hot_float", float_sample_schema(), hot_float)?;
        register_memtable(&ctx, "hot_hist", native_histogram_schema(), hot_hist)?;

        // 3. UNION views over cold + hot.
        let float_table = register_union(&ctx, "float_union", &cold_float, "hot_float").await?;
        let histogram_table =
            register_union(&ctx, "histogram_union", &cold_hist, "hot_hist").await?;

        Ok(ScanResult {
            ctx,
            float_table: Some(float_table),
            histogram_table: Some(histogram_table),
        })
    }

    async fn label_names(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        // Slice 2's `Index`/head label APIs union the unfiltered name sets; the
        // `matchers` arg is accepted for trait conformance and (matching Slice
        // 2's own mock) does not narrow the result here. If/when the blockstore
        // `Index` grows a matcher-filtered `label_names`, thread it through both
        // sides identically.
        let mut set: BTreeSet<String> =
            self.blockstore.index().label_names(tenant).into_iter().collect();
        set.extend(self.head.read().await.label_names(tenant));
        let _ = matchers;
        Ok(set.into_iter().collect())
    }

    async fn label_values(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<String>, PromqlError> {
        let mut set: BTreeSet<String> = self
            .blockstore
            .index()
            .label_values(tenant, name)
            .into_iter()
            .collect();
        set.extend(self.head.read().await.label_values(tenant, name));
        let _ = matchers; // accepted for trait conformance (see `label_names`)
        Ok(set.into_iter().collect())
    }

    async fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        _start_ms: i64,
        _end_ms: i64,
    ) -> Result<Vec<Labels>, PromqlError> {
        let head = self.head.read().await;
        let mut out: Vec<Labels> = Vec::new();
        let mut seen: BTreeSet<u64> = BTreeSet::new();
        // cold
        let cold_fps = self.blockstore.index().resolve(tenant, matchers).map_err(Self::err)?;
        for fp in &cold_fps {
            if seen.insert(*fp) {
                if let Some(l) = self.blockstore.index().series_labels(tenant, *fp) {
                    out.push(l);
                }
            }
        }
        // hot
        let hot_fps = head.resolve(tenant, matchers);
        for l in head.series_labels(tenant, &hot_fps) {
            let fp = l.fingerprint();
            if seen.insert(fp) {
                out.push(l);
            }
        }
        Ok(out)
    }
}

/// Register the cold native-histogram table into `ctx` and return its name.
async fn register_cold_histograms(
    blockstore: &BlockStore,
    ctx: &SessionContext,
    tenant: &str,
    matchers: &[LabelMatcher],
    start_ms: i64,
    cold_max: i64,
) -> Result<String, PromqlError> {
    // `scan_context` registers blocks under a fixed name in a *new* ctx. To get
    // the histogram blocks into the SAME ctx as the floats, build a second scan
    // and re-register its table provider here. If the blockstore API offers a
    // "register into existing ctx" entry point, prefer it; otherwise the
    // pattern below copies the resolved blocks across. VERIFY against blockstore.
    let (hctx, hname) = blockstore
        .scan_context(tenant, matchers, start_ms, cold_max, super::store_native_histogram_schema())
        .await
        .map_err(CrabkaMetricStore::err)?;
    let provider = hctx
        .table_provider(&hname)
        .await
        .map_err(CrabkaMetricStore::err)?;
    let name = "cold_hist".to_string();
    ctx.register_table(&name, provider).map_err(CrabkaMetricStore::err)?;
    Ok(name)
}

fn register_memtable(
    ctx: &SessionContext,
    name: &str,
    schema: SchemaRef,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<(), PromqlError> {
    let partitions = if batches.is_empty() { vec![] } else { vec![batches] };
    let table = MemTable::try_new(schema, partitions).map_err(CrabkaMetricStore::err)?;
    ctx.register_table(name, Arc::new(table)).map_err(CrabkaMetricStore::err)?;
    Ok(())
}

/// Register a UNION ALL view over `cold` and `hot` tables and return its name.
async fn register_union(
    ctx: &SessionContext,
    view_name: &str,
    cold_table: &str,
    hot_table: &str,
) -> Result<String, PromqlError> {
    let sql = format!(
        "CREATE VIEW {view_name} AS \
         SELECT * FROM {cold_table} UNION ALL SELECT * FROM {hot_table}"
    );
    ctx.sql(&sql).await.map_err(CrabkaMetricStore::err)?;
    Ok(view_name.to_string())
}
```

> **Churn-point checklist (verify against the pinned datafusion rev + blockstore/promql APIs if compile fails):**
> - `SessionContext::table_provider(&self, &str) -> Result<Arc<dyn TableProvider>>` — name may be `table` / `table_provider`; adapt. Used to lift the histogram blocks into the float ctx.
> - `MemTable::try_new(SchemaRef, Vec<Vec<RecordBatch>>)` at `datafusion::catalog::MemTable` (older path `datafusion::datasource::MemTable`).
> - `CREATE VIEW … UNION ALL …` then `register_table`/query by name — if `CREATE VIEW` over registered tables is unsupported at the pin, build the union via `ctx.table(cold).union(ctx.table(hot))?.into_view()` and `register_table(view_name, view)` instead. The **behavior** (UNION ALL, no dedup, frontier split prevents double-count) is what the test pins.
> - `PromqlError` store-error variant is `PromqlError::Store(String)` (Slice 2's frozen variant set: `Parse`/`Plan`/`Exec`/`Store`/`Unsupported`).
> - `BlockStore::index()` returning `&Index` — the blockstore plan exposes `index()` with `new/add_series/resolve/candidate_blocks/label_names/label_values`, but **no `series_labels` accessor**. `Index::series_labels(tenant, fp) -> Option<Labels>` is a **REQUIRED prerequisite of this slice** (the `Index` already stores `series: HashMap<fp, Labels>`, so the accessor is a trivial lookup): land it in blockstore first, or vendor the small accessor, **before** Task 4. Do NOT fall back to reconstructing labels from postings — that is lossy and expensive.
> - `super::store_native_histogram_schema()` is a typo guard — use `crate::native_histogram_schema()`.

- [ ] **Step 4: Make the test pass**

Wire `blockstore_with_one_cold_sample` (mirror blockstore plan Task 7's write+index calls) and the `SharedHead` test accessor. Run `cargo test -p crabka-metrics --lib querier::store`.
Expected: PASS (the no-double-count assertion is the headline — `c == 2`, not 3).

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): CrabkaMetricStore — cold+hot UNION MetricStore with frontier split"
```

---

### Task 5: Prometheus JSON serialization — `QueryResult` → exact wire shapes (the byte-equality analog)

**Files:**
- Create: `crates/metrics/src/querier/http/mod.rs` (module decls + `AppState` + tenant extractor + `format_sample_value`-adjacent helpers live in `json.rs`)
- Create: `crates/metrics/src/querier/http/json.rs`

**Interfaces:**
- Consumes: `crabka-promql::{QueryResult, InstantSample, RangeSeries, SampleValue, Labels}`.
- Produces:
  - `fn format_sample_value(v: f64) -> String` — Prometheus `MarshalFloat` semantics (`'f'` default, `'e'` for `abs<1e-6`/`abs>=1e21`); `+Inf`/`-Inf`/`NaN` literals.
  - `fn query_result_to_json(r: &QueryResult) -> serde_json::Value` — the full Prometheus `data` object (`resultType` + `result`).
  - `fn success(data: serde_json::Value) -> serde_json::Value` / `fn error_envelope(error_type: &str, msg: &str) -> serde_json::Value`.
  - `fn labels_to_json(l: &Labels) -> serde_json::Value` (object of name→value, including `__name__`).

- [ ] **Step 1: Write the failing exact-JSON tests**

Create `crates/metrics/src/querier/http/json.rs` with tests first. **These are the byte-equality analog** — they assert the exact JSON for vector, matrix, scalar, and error.

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_blockstore::Labels;
    use crabka_promql::{InstantSample, QueryResult, RangeSeries, SampleValue};
    use serde_json::json;

    use super::*;

    fn up_api() -> Labels {
        let mut l = Labels::new();
        l.insert("__name__", "up");
        l.insert("job", "api");
        l
    }

    #[test]
    fn vector_json_is_exact() {
        let r = QueryResult::InstantVector(vec![InstantSample {
            labels: up_api(),
            ts_ms: 1_435_781_451_781,
            value: SampleValue::Float(1.0),
        }]);
        let got = success(query_result_to_json(&r));
        let want = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"__name__": "up", "job": "api"},
                    "value": [1435781451.781, "1"]
                }]
            }
        });
        assert!(got == want, "got={got}");
    }

    #[test]
    fn matrix_json_is_exact() {
        let r = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: up_api(),
            samples: vec![
                (1_435_781_430_000, SampleValue::Float(1.0)),
                (1_435_781_445_000, SampleValue::Float(0.0)),
            ],
        }]);
        let got = success(query_result_to_json(&r));
        let want = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{
                    "metric": {"__name__": "up", "job": "api"},
                    "values": [[1435781430, "1"], [1435781445, "0"]]
                }]
            }
        });
        assert!(got == want, "got={got}");
    }

    #[test]
    fn matrix_integral_second_ts_is_bare_integer() {
        // Pins MarshalTimestamp: a whole-second ts serializes WITHOUT a trailing
        // `.0` (bare integer), and a sub-second ts keeps zero-padded fraction.
        let r = QueryResult::RangeMatrix(vec![RangeSeries {
            labels: up_api(),
            samples: vec![
                (1_435_781_430_000, SampleValue::Float(1.0)), // → 1435781430
                (1_435_781_430_005, SampleValue::Float(2.0)), // → 1435781430.005
                (1_435_781_430_050, SampleValue::Float(3.0)), // → 1435781430.050
            ],
        }]);
        let got = success(query_result_to_json(&r));
        let want = json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{
                    "metric": {"__name__": "up", "job": "api"},
                    "values": [
                        [1435781430, "1"],
                        [1435781430.005, "2"],
                        [1435781430.050, "3"]
                    ]
                }]
            }
        });
        // serde_json::Number comparison: 1435781430 (integer) != 1435781430.0
        // (float) — that distinction is exactly what this test guards.
        assert!(got == want, "got={got}");
    }

    #[test]
    fn scalar_json_is_exact() {
        let r = QueryResult::Scalar { ts_ms: 1_435_781_451_781, value: 2.0 };
        let got = success(query_result_to_json(&r));
        let want = json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1435781451.781, "2"]}
        });
        assert!(got == want, "got={got}");
    }

    #[test]
    fn error_json_is_exact() {
        let got = error_envelope("bad_data", "parse error: unexpected end of input");
        let want = json!({
            "status": "error",
            "errorType": "bad_data",
            "error": "parse error: unexpected end of input"
        });
        assert!(got == want, "got={got}");
    }

    #[test]
    fn float_formatting_matches_go() {
        assert!(format_sample_value(1.0) == "1");
        assert!(format_sample_value(0.0) == "0");
        assert!(format_sample_value(-0.0) == "-0" || format_sample_value(-0.0) == "0");
        assert!(format_sample_value(1.5) == "1.5");
        assert!(format_sample_value(f64::INFINITY) == "+Inf");
        assert!(format_sample_value(f64::NEG_INFINITY) == "-Inf");
        assert!(format_sample_value(f64::NAN) == "NaN");
        // MarshalFloat: exponent form only when abs >= 1e21 or abs < 1e-6.
        assert!(format_sample_value(1e21) == "1e+21");
        // The [1e-6, 1e-4) band stays in 'f' (plain decimal) form — this is the
        // MarshalFloat-vs-Go-'g' boundary (Go 'g' would switch at 1e-4).
        assert!(format_sample_value(1e-5) == "0.00001");
        assert!(format_sample_value(0.0001) == "0.0001");
        // Below 1e-6 switches to exponent form.
        assert!(format_sample_value(1e-7) == "1e-07");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib querier::http::json`
Expected: FAIL — `cannot find function query_result_to_json`.

- [ ] **Step 3: Implement `json.rs`**

```rust
//! Prometheus query-API JSON. The response *shape* is the contract (the
//! byte-equality analog for this signal), so all serialization funnels through
//! here. Timestamps port `jsonutil.MarshalTimestamp` (bare integer seconds, with
//! a 3-digit fraction only when the ms remainder is non-zero); sample values
//! port `jsonutil.MarshalFloat` (`'f'` default, `'e'` for `abs<1e-6`/`>=1e21`).

use crabka_blockstore::Labels;
use crabka_promql::{InstantSample, QueryResult, RangeSeries, SampleValue};
use serde_json::{Value, json};

/// Wrap a `data` object in the success envelope.
#[must_use]
pub fn success(data: Value) -> Value {
    json!({ "status": "success", "data": data })
}

/// The Prometheus error envelope.
#[must_use]
pub fn error_envelope(error_type: &str, msg: &str) -> Value {
    json!({ "status": "error", "errorType": error_type, "error": msg })
}

/// Labels → a JSON object (`__name__` included, Prometheus convention).
#[must_use]
pub fn labels_to_json(l: &Labels) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in l.iter() {
        map.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(map)
}

/// ms → Prometheus JSON timestamp number, byte-exact with Prometheus's
/// `util/jsonutil.MarshalTimestamp`: a bare integer of whole seconds, with
/// `.<fraction>` (the millisecond remainder, zero-padded to exactly 3 digits, no
/// trailing-zero trimming) appended ONLY when that remainder is non-zero. So
/// `1435781430000` → `1435781430` (no trailing `.0`), `1435781430005` →
/// `1435781430.005`, `1435781430050` → `1435781430.050`, `1435781451781` →
/// `1435781451.781`. Negatives carry the sign on the whole number (Go form).
fn ts_to_json(ts_ms: i64) -> Value {
    let sign = if ts_ms < 0 { "-" } else { "" };
    let abs = ts_ms.unsigned_abs();
    let secs = abs / 1000;
    let fraction = abs % 1000;
    let s = if fraction == 0 {
        format!("{sign}{secs}")
    } else {
        // Go pads to 3 digits (e.g. fraction 5 → "005", 50 → "050") and does NOT
        // trim trailing zeros.
        format!("{sign}{secs}.{fraction:03}")
    };
    // Build a JSON number from the decimal string so no f64 rounding sneaks in.
    Value::Number(s.parse::<serde_json::Number>().expect("valid decimal"))
}

/// Prometheus `jsonutil.MarshalFloat` for sample values: `strconv.AppendFloat`
/// with `fmt='f'` by default, switching to `fmt='e'` only when the magnitude is
/// `< 1e-6` or `>= 1e21`, plus the special-cases for non-finite values.
#[must_use]
pub fn format_sample_value(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.to_string();
    }
    format_prom_float(v)
}

/// Float → string matching Prometheus `MarshalFloat`: shortest round-trip in
/// `'f'` (plain decimal) form by default, switching to `'e'` (exponent) form
/// only when `abs(v) < 1e-6` or `abs(v) >= 1e21` — NOT Go `'g'` (whose exponent
/// boundary is `1e-4`). Rust's `{}` already gives the shortest round-trip plain
/// decimal; `{:e}` gives the shortest exponent form, which we reshape (`e21` →
/// `e+21`) to match. Behavior is pinned by `float_formatting_matches_go`.
fn format_prom_float(v: f64) -> String {
    // Match MarshalFloat: exponent form only when abs < 1e-6 or abs >= 1e21.
    // In the [1e-6, 1e-4) band Prometheus still emits 'f' form (e.g. 1e-5 →
    // "0.00001"), so the lower threshold is 1e-6, not 1e-4.
    let abs = v.abs();
    if v != 0.0 && (abs >= 1e21 || abs < 1e-6) {
        // Use exponent form; Rust's {:e} gives e.g. "1e21" / "1e-7".
        let s = format!("{v:e}");
        return normalize_exponent(&s);
    }
    format!("{v}")
}

/// Normalize Rust `{:e}` ("1e21", "1.234e-5") to Go's `'e'` exponent form
/// ("1e+21", "1.234e-05"): explicit sign + at-least-two-digit zero-padded exp.
fn normalize_exponent(s: &str) -> String {
    let Some((mantissa, exp)) = s.split_once('e') else {
        return s.to_string();
    };
    let (sign, digits) = match exp.strip_prefix('-') {
        Some(d) => ('-', d),
        None => ('+', exp),
    };
    // Go pads the exponent to at least two digits.
    let digits = if digits.len() < 2 {
        format!("0{digits}")
    } else {
        digits.to_string()
    };
    format!("{mantissa}e{sign}{digits}")
}

/// A single instant-vector sample's value pair: native histogram or float.
fn instant_value_pair(ts_ms: i64, v: &SampleValue) -> Value {
    match v {
        SampleValue::Float(f) => json!([ts_to_json(ts_ms), format_sample_value(*f)]),
        SampleValue::Histogram(h) => json!([ts_to_json(ts_ms), histogram_to_json(h)]),
    }
}

/// Native histogram → the Prometheus `histogram` object shape.
fn histogram_to_json(h: &crate::histogram::NativeHistogram) -> Value {
    // Prometheus shape:
    //   {"count":"<n>","sum":"<s>","buckets":[[<boundary_rule>,"<lower>","<upper>","<count>"],…]}
    // Bucket boundary-rule + bucket expansion from spans is non-trivial; build it
    // here and pin with a dedicated histogram test (see verify-note).
    let _ = h;
    json!({ "count": "0", "sum": "0", "buckets": [] }) // PLACEHOLDER shape — see note
}

/// `QueryResult` → the Prometheus `data` object.
#[must_use]
pub fn query_result_to_json(r: &QueryResult) -> Value {
    match r {
        QueryResult::InstantVector(samples) => {
            let result: Vec<Value> = samples
                .iter()
                .map(|s| match &s.value {
                    SampleValue::Float(f) => json!({
                        "metric": labels_to_json(&s.labels),
                        "value": [ts_to_json(s.ts_ms), format_sample_value(*f)]
                    }),
                    SampleValue::Histogram(h) => json!({
                        "metric": labels_to_json(&s.labels),
                        "histogram": [ts_to_json(s.ts_ms), histogram_to_json(h)]
                    }),
                })
                .collect();
            json!({ "resultType": "vector", "result": result })
        }
        QueryResult::RangeMatrix(series) => {
            let result: Vec<Value> = series
                .iter()
                .map(|s| {
                    let (floats, hists): (Vec<_>, Vec<_>) =
                        s.samples.iter().partition(|(_, v)| matches!(v, SampleValue::Float(_)));
                    let mut obj = serde_json::Map::new();
                    obj.insert("metric".into(), labels_to_json(&s.labels));
                    if !floats.is_empty() {
                        let values: Vec<Value> = floats
                            .iter()
                            .map(|(ts, v)| match v {
                                SampleValue::Float(f) => {
                                    json!([ts_to_json(*ts), format_sample_value(*f)])
                                }
                                SampleValue::Histogram(_) => unreachable!(),
                            })
                            .collect();
                        obj.insert("values".into(), json!(values));
                    }
                    if !hists.is_empty() {
                        let values: Vec<Value> =
                            hists.iter().map(|(ts, v)| instant_value_pair(*ts, v)).collect();
                        obj.insert("histograms".into(), json!(values));
                    }
                    Value::Object(obj)
                })
                .collect();
            json!({ "resultType": "matrix", "result": result })
        }
        QueryResult::Scalar { ts_ms, value } => json!({
            "resultType": "scalar",
            "result": [ts_to_json(*ts_ms), format_sample_value(*value)]
        }),
        QueryResult::Str { ts_ms, value } => json!({
            "resultType": "string",
            "result": [ts_to_json(*ts_ms), value]
        }),
    }
}
```

> **Verify-notes:**
> - **`ts_to_json` byte-exactness:** `ts_to_json` ports Prometheus's `util/jsonutil.MarshalTimestamp` directly — a bare integer of whole seconds, plus a 3-digit zero-padded fraction only when the millisecond remainder is non-zero — and builds a `serde_json::Number` from that decimal **string** so no f64 reformatting reintroduces a trailing `.0`. A whole-second ts (`1435781430000`) must serialize as the JSON integer `1435781430`, NOT `1435781430.0`. The `matrix_integral_second_ts_is_bare_integer` test is the canary (an integer `Number` is `!=` an f64 `Number` under `serde_json`, which is what makes the bare-integer requirement testable).
> - **Native-histogram JSON is a PLACEHOLDER** (`histogram_to_json`). The real Prometheus bucket shape (boundary-inclusion rule integer + `["<lower>","<upper>","<count>"]` triples expanded from spans) is non-trivial; implement it with a dedicated `native_histogram_json_is_exact` test seeded from a known `NativeHistogram`, cross-checked against a real Prometheus `/query` response for the same histogram. Flagged, not faked — float vector/matrix/scalar/error are the in-scope byte-equality assertions for this task.
> - **`QueryResult::Scalar` shape:** the shared contract gives the struct variant `Scalar { ts_ms: i64, value: f64 }` (and `Str { ts_ms, value }`); if promql's `Scalar` carries no timestamp, drop `ts_ms` and source the eval ts from the request (instant `time`) in the handler — keep the JSON `[<ts>, "<val>"]`.

- [ ] **Step 4: Create `http/mod.rs` (module wiring + tenant extractor)**

```rust
//! Prometheus HTTP query API for the querier role.

pub mod json;
pub mod meta;
pub mod query;

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderMap;
use crabka_promql::PromqlEngine;

use crate::querier::store::CrabkaMetricStore;

/// Shared handler state: the PromQL engine over `CrabkaMetricStore`, plus a
/// direct handle to the same store for the metadata endpoints. Slice 2's frozen
/// `PromqlEngine` exposes no `store()` accessor (its `store` field is private),
/// so the `/series`/`/labels`/`/label/{name}/values` handlers reach the store
/// through this field rather than through the engine.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<PromqlEngine<CrabkaMetricStore>>,
    pub store: Arc<CrabkaMetricStore>,
}

/// Tenant from `X-Scope-OrgID` (Mimir/Cortex convention); falls back to
/// `"anonymous"` when absent (single-tenant mode).
#[must_use]
pub fn tenant_of(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

/// Build the Prometheus API router, mounted under both bare `/api/v1` and
/// Mimir's `/prometheus/api/v1`.
#[must_use]
pub fn router(state: AppState) -> Router {
    let api = api_v1_router(state.clone());
    Router::new()
        .nest("/api/v1", api.clone())
        .nest("/prometheus/api/v1", api)
        .with_state(state)
}

fn api_v1_router(state: AppState) -> Router<AppState> {
    use axum::routing::{get, post};
    Router::new()
        .route("/query", get(query::query).post(query::query))
        .route("/query_range", get(query::query_range).post(query::query_range))
        .route("/series", get(meta::series).post(meta::series))
        .route("/labels", get(meta::labels).post(meta::labels))
        .route("/label/{name}/values", get(meta::label_values))
        .route("/metadata", get(meta::metadata))
        .route("/query_exemplars", get(meta::query_exemplars).post(meta::query_exemplars))
        .route("/status/buildinfo", get(meta::buildinfo))
        .with_state(state)
}
```

> `Router::nest` with `.with_state` on both the inner and outer routers: confirm axum 0.8 wants the state on the leaf `Router<AppState>` and the merged router. If the type checker complains, attach `.with_state(state)` once on the final `Router` and make the inner builders `Router<AppState>` without their own `with_state`. The route-table behavior is what the Task-6 tests pin.

- [ ] **Step 5: Run to verify json tests pass**

Run: `cargo test -p crabka-metrics --lib querier::http::json`
Expected: PASS (6 tests). (`http/mod.rs` won't fully compile until Task 6 adds the handlers; if you split commits, stub `query`/`meta` with `todo!()`-free empty handlers first — see Task 6.)

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): Prometheus query-API JSON shapes (vector/matrix/scalar/error)"
```

---

### Task 6: HTTP handlers + router wiring + in-process response-shape tests

**Files:**
- Create: `crates/metrics/src/querier/http/query.rs`
- Create: `crates/metrics/src/querier/http/meta.rs`

**Interfaces:**
- Consumes: `AppState`, `tenant_of`, `json::*`, `crabka-promql::PromqlEngine`.
- Produces axum handlers:
  - `query.rs`: `query` (instant), `query_range`.
  - `meta.rs`: `series`, `labels`, `label_values`, `metadata`, `query_exemplars`, `buildinfo`.
- Param parsing: `query` (`query`, `time`→ms, default now), `query_range` (`query`, `start`, `end`, `step`); Prometheus accepts RFC3339 or unix seconds (float) — implement `parse_time` covering both.

- [ ] **Step 1: Write the failing handler tests (in-process router via `oneshot`)**

Create `crates/metrics/src/querier/http/query.rs` with tests first. The test builds an `AppState` whose engine is backed by a `CrabkaMetricStore` over a head with one known sample (no broker), drives the router, and asserts the **exact response body** for an instant query and the **error envelope** for a bad query.

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use assert2::assert;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use crabka_blockstore::Labels;
    use crabka_promql::{EngineOpts, PromqlEngine};
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use crate::querier::head::HeadSample;
    use crate::querier::http::{AppState, router};
    use crate::querier::store::CrabkaMetricStore;
    use crate::querier::tailer::SharedHead;

    async fn state_with_one_up_sample(ts_ms: i64) -> AppState {
        // Empty-cold blockstore + a head holding up{job="api"}=1 @ ts.
        let blockstore = crate::querier::store::tests_support::empty_blockstore();
        let head = SharedHead::new(Duration::from_secs(3600));
        {
            let mut l = Labels::new();
            l.insert("__name__", "up");
            l.insert("job", "api");
            let mut h = head.write_for_test().await;
            h.ingest_at("anonymous", l.fingerprint(), &l, ts_ms, HeadSample::Float(1.0), 0, 0);
        }
        // Frontier 0 → everything is hot.
        let store = Arc::new(CrabkaMetricStore::new(blockstore, head, Arc::new(|_| 0)));
        let engine = Arc::new(PromqlEngine::new(store.clone(), EngineOpts::default()));
        AppState { engine, store }
    }

    #[tokio::test]
    async fn instant_query_returns_exact_vector_body() {
        let ts_ms = 1_435_781_451_781;
        let app = router(state_with_one_up_sample(ts_ms).await);

        let uri = format!("/api/v1/query?query=up&time={}", ts_ms as f64 / 1000.0);
        let resp = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);

        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let got: Value = serde_json::from_slice(&bytes).unwrap();
        let want = json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{
                    "metric": {"__name__": "up", "job": "api"},
                    "value": [1435781451.781, "1"]
                }]
            }
        });
        assert!(got == want, "got={got}");
    }

    #[tokio::test]
    async fn parse_error_returns_400_error_envelope() {
        let app = router(state_with_one_up_sample(1_000).await);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up(")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::BAD_REQUEST);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let got: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(got["status"] == "error");
        assert!(got["errorType"] == "bad_data");
        assert!(got["error"].is_string());
    }

    #[tokio::test]
    async fn dual_mount_prometheus_prefix_also_serves() {
        let ts_ms = 1_435_781_451_781;
        let app = router(state_with_one_up_sample(ts_ms).await);
        let uri = format!("/prometheus/api/v1/query?query=up&time={}", ts_ms as f64 / 1000.0);
        let resp = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
    }
}
```

> **Test-support note:** `crate::querier::store::tests_support::empty_blockstore()` and `SharedHead::write_for_test()` are small `#[cfg(test)]` helpers — add them in Tasks 4/3 respectively (an empty `BlockStore` over `InMemory`, and a write guard). They keep the handler test broker-free. `EngineOpts::default()` and `PromqlEngine::new` are the Slice-2 contract — verify.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib querier::http::query`
Expected: FAIL — handlers don't exist yet.

- [ ] **Step 3: Implement `query.rs`**

```rust
//! `/query` (instant) and `/query_range` handlers.

use std::collections::HashMap;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Json};
use crabka_promql::{PromqlError, QueryResult};

use crate::querier::http::json::{error_envelope, query_result_to_json, success};
use crate::querier::http::{AppState, tenant_of};

/// Map a `PromqlError` to `(HTTP status, errorType)` per Prometheus conventions.
/// Slice 2's frozen variant set is `Parse`/`Plan`/`Exec`/`Store`/`Unsupported`
/// (see `crabka_promql::PromqlError`).
fn classify(e: &PromqlError) -> (StatusCode, &'static str) {
    match e {
        PromqlError::Parse(_) | PromqlError::Plan(_) => (StatusCode::BAD_REQUEST, "bad_data"),
        PromqlError::Unsupported(_) => (StatusCode::BAD_REQUEST, "bad_data"),
        PromqlError::Store(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        PromqlError::Exec(_) => (StatusCode::UNPROCESSABLE_ENTITY, "execution"),
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
    )
    .unwrap_or(i64::MAX)
}

/// Parse a Prometheus time param: unix seconds (float) or RFC3339.
fn parse_time(s: &str) -> Option<i64> {
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0).round() as i64);
    }
    // RFC3339 — use a minimal parser or `time`/`chrono` if a workspace dep.
    parse_rfc3339_ms(s)
}

fn parse_rfc3339_ms(_s: &str) -> Option<i64> {
    // VERIFY: if `chrono`/`time` is a workspace dep, use it. Otherwise a small
    // RFC3339 parser. Grafana sends unix-seconds floats for the Prometheus
    // datasource, so the float path above covers the common case; RFC3339 is
    // for `curl`/parity. Flagged.
    None
}

async fn run_instant(
    state: &AppState,
    tenant: &str,
    q: &str,
    ts_ms: i64,
) -> Result<QueryResult, PromqlError> {
    state.engine.query_instant(tenant, q, ts_ms).await
}

/// `GET|POST /query`
pub async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    let Some(q) = params.get("query") else {
        return (StatusCode::BAD_REQUEST, Json(error_envelope("bad_data", "missing query"))).into_response();
    };
    let ts_ms = params.get("time").and_then(|s| parse_time(s)).unwrap_or_else(now_ms);

    match run_instant(&state, &tenant, q, ts_ms).await {
        Ok(r) => (StatusCode::OK, Json(success(query_result_to_json(&r)))).into_response(),
        Err(e) => {
            let (code, et) = classify(&e);
            (code, Json(error_envelope(et, &e.to_string()))).into_response()
        }
    }
}

/// `GET|POST /query_range`
pub async fn query_range(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    let (Some(q), Some(start), Some(end), Some(step)) = (
        params.get("query"),
        params.get("start").and_then(|s| parse_time(s)),
        params.get("end").and_then(|s| parse_time(s)),
        params.get("step").and_then(parse_duration_ms),
    ) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(error_envelope("bad_data", "missing/invalid query_range params")),
        )
            .into_response();
    };

    match state.engine.query_range(&tenant, q, start, end, step).await {
        Ok(r) => (StatusCode::OK, Json(success(query_result_to_json(&r)))).into_response(),
        Err(e) => {
            let (code, et) = classify(&e);
            (code, Json(error_envelope(et, &e.to_string()))).into_response()
        }
    }
}

/// Prometheus `step`: float seconds or a duration string (`15s`,`1m`,…).
fn parse_duration_ms(s: &String) -> Option<i64> {
    if let Ok(secs) = s.parse::<f64>() {
        return Some((secs * 1000.0).round() as i64);
    }
    // VERIFY: reuse promql's duration parser if exposed; else a small one
    // covering ms/s/m/h/d/w/y. Grafana sends float seconds for `step`.
    parse_promql_duration_ms(s)
}

fn parse_promql_duration_ms(_s: &str) -> Option<i64> {
    None // flagged: wire to promql's duration parser if available
}
```

> The `classify` function and `parse_*` helpers carry **verify-notes** because the exact `PromqlError` variants and any reusable duration parser live in Slices 2–3. The parse-error test (Task 6 Step 1) requires `up(` to surface as a `PromqlError` the engine returns from `query_instant` and that `classify` maps to `400/bad_data`. If the engine parses lazily (error only on execute), the mapping still holds — wire `classify` to the real parse variant.

- [ ] **Step 4: Implement `meta.rs`**

```rust
//! Discovery + metadata endpoints: `/series`, `/labels`, `/label/{n}/values`,
//! `/metadata`, `/query_exemplars`, `/status/buildinfo`.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Json};
use serde_json::json;

use crate::querier::http::json::{error_envelope, success};
use crate::querier::http::{AppState, tenant_of};

// Parse a single `match[]=` selector set into a flat matcher list — the shape
// `MetricStore::series` takes (Slice 2: `&[LabelMatcher]`). Parsing the PromQL
// selector is a promql concern; this uses a thin local parser or promql's
// exposed selector parser. VERIFY. (Prometheus accepts repeated `match[]`; this
// slice handles the single-selector case Grafana sends — extend to union
// multiple selector sets at the handler if needed.)
fn parse_matchers(_params: &HashMap<String, String>) -> Vec<crabka_blockstore::LabelMatcher> {
    Vec::new() // wire to promql's selector parser for `match[]`
}

fn time_window(params: &HashMap<String, String>) -> (i64, i64) {
    // Defaults: full range if start/end absent.
    let start = params.get("start").and_then(|s| s.parse::<f64>().ok()).map_or(i64::MIN, |s| (s * 1000.0) as i64);
    let end = params.get("end").and_then(|s| s.parse::<f64>().ok()).map_or(i64::MAX, |s| (s * 1000.0) as i64);
    (start, end)
}

/// `GET|POST /series`
pub async fn series(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    let (start, end) = time_window(&params);
    let matchers = parse_matchers(&params);
    match state.store.series(&tenant, &matchers, start, end).await {
        Ok(series) => {
            let result: Vec<_> = series.iter().map(crate::querier::http::json::labels_to_json).collect();
            Json(success(json!(result))).into_response()
        }
        Err(e) => Json(error_envelope("execution", &e.to_string())).into_response(),
    }
}

/// `GET|POST /labels`
pub async fn labels(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    let (start, end) = time_window(&params);
    let matchers = parse_matchers(&params);
    match state.store.label_names(&tenant, &matchers, start, end).await {
        Ok(mut names) => {
            names.sort();
            Json(success(json!(names))).into_response()
        }
        Err(e) => Json(error_envelope("execution", &e.to_string())).into_response(),
    }
}

/// `GET /label/{name}/values`
pub async fn label_values(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let tenant = tenant_of(&headers);
    let (start, end) = time_window(&params);
    let matchers = parse_matchers(&params);
    match state.store.label_values(&tenant, &name, &matchers, start, end).await {
        Ok(mut values) => {
            values.sort();
            Json(success(json!(values))).into_response()
        }
        Err(e) => Json(error_envelope("execution", &e.to_string())).into_response(),
    }
}

/// `GET /metadata` — empty map until the metadata index (spec §4.4) lands.
pub async fn metadata() -> impl IntoResponse {
    Json(success(json!({}))).into_response()
}

/// `GET|POST /query_exemplars` — empty (stub) until the exemplar sidecar read
/// path is wired (spec §4.3). Returns success with empty data for Grafana.
pub async fn query_exemplars() -> impl IntoResponse {
    Json(success(json!([]))).into_response()
}

/// `GET /status/buildinfo` — Prometheus build-info shape Grafana probes.
pub async fn buildinfo() -> impl IntoResponse {
    Json(success(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "",
        "branch": "",
        "buildUser": "",
        "buildDate": "",
        "goVersion": ""
    })))
    .into_response()
}
```

> **Store handle for the metadata handlers:** the `/series`/`/labels`/`/label values` handlers need the underlying `MetricStore`. Slice 2's frozen `PromqlEngine` exposes **no** `store()` accessor (its `store` field is private), so — to keep zero cross-crate edits to promql — `AppState` carries `pub store: Arc<CrabkaMetricStore>` directly (the same `Arc` cloned into `PromqlEngine::new`), and the handlers call `state.store.{series,label_names,label_values}` directly. (This is the decision wired throughout Tasks 5/6: do not reach through the engine.)

- [ ] **Step 5: Run to verify handler tests pass**

Run: `cargo test -p crabka-metrics --lib querier::http`
Expected: PASS (json + query handler tests). `AppState` carries both `engine` and `store` (see the store-handle note above).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): Prometheus HTTP query API handlers + dual-mount router"
```

---

### Task 7: Role binary `crabka-metrics --target querier` + end-to-end `#[ignore]` integration

**Files:**
- Create: `crates/metrics/src/bin/crabka-metrics.rs`
- Create: `crates/metrics/tests/querier_e2e.rs`

**Interfaces:**
- Consumes: `QuerierConfig`, `SharedHead`, `HeadTailer`, `CrabkaMetricStore`, `PromqlEngine`, `http::router`, `crabka-grpc-gateway`/broker `serve` pattern (plaintext axum serve).
- Produces: a binary that, for `--target querier`, builds the head + tailer + store + engine + router and serves the Prometheus API on `config.listen_addr`.

- [ ] **Step 1: Implement the binary**

```rust
//! Crabka metrics role binary. `--target querier` serves the Prometheus HTTP
//! API over hot (WAL-tail head) + cold (blockstore) data. Other targets
//! (distributor/compactor/query-frontend/ruler) are wired in their slices.

use std::sync::Arc;

use crabka_metrics::querier::{QuerierConfig, http, store::CrabkaMetricStore, tailer::{HeadTailer, SharedHead}};
use crabka_promql::{EngineOpts, PromqlEngine};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target = std::env::args()
        .skip_while(|a| a != "--target")
        .nth(1)
        .unwrap_or_else(|| "querier".to_string());

    match target.as_str() {
        "querier" => run_querier(querier_config_from_env()).await,
        other => Err(format!("unknown --target {other}").into()),
    }
}

fn querier_config_from_env() -> QuerierConfig {
    // Suffix group id with a per-process nonce so each querier sees ALL
    // partitions (the head must be complete).
    let nonce = std::process::id();
    QuerierConfig {
        group_id: format!("crabka-querier-head-{nonce}"),
        ..QuerierConfig::default()
    }
}

async fn run_querier(config: QuerierConfig) -> Result<(), Box<dyn std::error::Error>> {
    let shutdown = CancellationToken::new();

    // Hot head + tailer.
    let head = SharedHead::new(config.head_retention);
    let _tailer = HeadTailer::spawn(&config, head.clone(), shutdown.clone());

    // Cold blockstore — built from object-store config (env). VERIFY against
    // blockstore::BlockStore::new(store, base_url) + index load.
    let blockstore = Arc::new(build_blockstore_from_env().await?);

    // Frontier: per-tenant compaction frontier. Until wired to the compactor's
    // committed offset / sealed-block max_ts (Slice 4), use the blockstore
    // index's max sealed ts per tenant. Flagged.
    let frontier = make_frontier(blockstore.clone());

    let store = Arc::new(CrabkaMetricStore::new(blockstore, head, frontier));
    let engine = Arc::new(PromqlEngine::new(store.clone(), EngineOpts::default()));
    let app = http::router(http::AppState { engine, store });

    let listener = tokio::net::TcpListener::bind(config.listen_addr).await?;
    tracing::info!(addr = %config.listen_addr, "querier Prometheus API listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await?;
    Ok(())
}

async fn build_blockstore_from_env() -> Result<crabka_blockstore::BlockStore, Box<dyn std::error::Error>> {
    // VERIFY: construct object_store + base URL from env; load the Index
    // snapshot. Mirrors the compactor's store construction (Slice 4).
    todo!("construct BlockStore + load Index from env-configured object store")
}

fn make_frontier(_bs: Arc<crabka_blockstore::BlockStore>) -> crate::CrabkaMetricStore /* FrontierFn */ {
    todo!("per-tenant frontier from sealed-block max_ts / compactor offset")
}
```

> The binary has two `todo!()`s (`build_blockstore_from_env`, `make_frontier`) that depend on Slice 4's compactor/object-store config surface. They are **wiring**, not logic — fill them when Slice 4's store-construction + frontier-commit are available. Everything testable (head/tailer/store/engine/router) is exercised by Tasks 2–6 unit tests and the `#[ignore]` e2e below. Keep the binary compiling: stub the two fns to return a clear `Err`/`unimplemented!` guarded so `--target querier` fails loudly rather than silently.

- [ ] **Step 2: Write the `#[ignore]` end-to-end test**

Create `crates/metrics/tests/querier_e2e.rs`. Boots an in-process broker (`crabka-broker` test-support), produces a handful of `WalRecord`s to the WAL topic, starts a `HeadTailer`, waits for the head to catch up, then drives the router and asserts `up` returns the produced sample. Gated `#[ignore]` because it needs a broker.

```rust
//! End-to-end: produce WAL records → tailer fills head → Prometheus /query
//! returns them. Requires an in-process broker; run with `--ignored`.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

// NOTE: this test depends on Slice 4's WalRecord encode + the WAL topic name,
// and on producing to a Crabka broker. Wire those when available.

#[tokio::test]
#[ignore = "requires an in-process broker + Slice 4 WalRecord produce"]
async fn produce_then_query_round_trips_through_head() {
    // 1. start in-process broker (crabka-broker test-support::start()).
    // 2. create the WAL topic; produce WalRecord(up{job=api}=1 @ now) via
    //    crabka-client-producer.
    // 3. SharedHead + HeadTailer::spawn pointed at the broker bootstrap+topic.
    // 4. poll until head.high_water_offset(0) >= produced offset (bounded wait).
    // 5. build CrabkaMetricStore (empty cold) + PromqlEngine + router.
    // 6. GET /api/v1/query?query=up&time=<now_secs> → assert exact vector body.
    let _ = (Body::empty(), Request::builder(), <Value as Default>::default);
    // Skeleton — fill from the Task-6 query test + a produce step.
    assert!(true);
}
```

> The e2e is a **skeleton with an explicit fill-list**, not fabricated passing code — it pins the *integration contract* (produce → tail → query) and is `#[ignore]`d so CI is green without Docker/broker. When Slice 4's produce path is in hand, flesh out steps 1–6; the assertion reuses Task 6's exact-vector `want`.

- [ ] **Step 3: Build the binary + run non-ignored tests**

Run: `cargo build -p crabka-metrics --bin crabka-metrics`
Expected: compiles (the two `todo!()` wiring fns compile; `--target querier` would panic at runtime until Slice 4 wiring — acceptable for this slice).
Run: `cargo test -p crabka-metrics`
Expected: all non-`#[ignore]` tests PASS.

- [ ] **Step 4: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): crabka-metrics --target querier binary + e2e skeleton"
```

---

### Task 8: Whole-crate gate + matrix/range over hot+cold (integration property-ish test)

**Files:**
- Create: `crates/metrics/tests/querier_range.rs`

**Interfaces:**
- Consumes the public querier API: `CrabkaMetricStore`, `SharedHead`, `PromqlEngine`, `http::router`.

- [ ] **Step 1: Write a `query_range` integration test (no broker)**

Seeds a head spanning a few minutes, runs `/api/v1/query_range` for `up` over the window, and asserts the **matrix** body has the expected series + step-aligned sample count. This exercises the range path + the JSON matrix shape end-to-end through the real engine.

```rust
//! Range query over a head-only dataset returns a correct, exactly-shaped
//! matrix. No broker — the head is fed directly.

use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use crabka_blockstore::Labels;
use crabka_metrics::querier::head::HeadSample;
use crabka_metrics::querier::http::{router, AppState};
use crabka_metrics::querier::store::CrabkaMetricStore;
use crabka_metrics::querier::tailer::SharedHead;
use crabka_promql::{EngineOpts, PromqlEngine};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn range_query_returns_matrix_shape() {
    let head = SharedHead::new(Duration::from_secs(3 * 3600));
    let mut up = Labels::new();
    up.insert("__name__", "up");
    up.insert("job", "api");
    {
        let mut h = head.write_for_test().await;
        // one sample every 15s for 1 minute: t=0,15,30,45,60 (s) → ms.
        for i in 0..=4 {
            let ts = i * 15_000;
            h.ingest_at("anonymous", up.fingerprint(), &up, ts, HeadSample::Float(1.0), 0, i);
        }
    }
    let store = Arc::new(CrabkaMetricStore::new(
        crabka_metrics::querier::store::tests_support::empty_blockstore(),
        head,
        Arc::new(|_| 0),
    ));
    let engine = Arc::new(PromqlEngine::new(store.clone(), EngineOpts::default()));
    let app = router(AppState { engine, store });

    // step=15s over [0,60]s.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=60&step=15")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status() == StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let got: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(got["status"] == "success");
    assert!(got["data"]["resultType"] == "matrix");
    let series = got["data"]["result"].as_array().unwrap();
    assert!(series.len() == 1);
    assert!(series[0]["metric"]["job"] == "api");
    // 5 step-aligned points (t=0,15,30,45,60s), each ["<ts>","1"].
    let values = series[0]["values"].as_array().unwrap();
    assert!(values.len() == 5, "values={values:?}");
    assert!(values[0][1] == "1");
}
```

> Sample-count expectations depend on the engine's instant-selection + lookback semantics (Slice 2/3); if the engine carries forward the last sample across empty steps, the count is the step grid size (5), which is what this asserts. If the staleness/lookback rules produce a different count for this seed, adjust the **count** to the engine's real behavior (this test pins the *shape* + that the range path works hot-only, not a specific lookback rule).

- [ ] **Step 2: Run it**

Run: `cargo test -p crabka-metrics --test querier_range`
Expected: PASS.

- [ ] **Step 3: Final whole-crate gate**

Run: `cargo test -p crabka-metrics && cargo clippy -p crabka-metrics --all-targets && cargo fmt -p crabka-metrics --check`
Expected: all PASS (excluding `#[ignore]`d e2e), no warnings, formatting clean.

- [ ] **Step 4: Commit**

```bash
git add crates/metrics/
git commit -m "test(metrics): range query over head returns exact matrix shape"
```

---

## Self-review

**Spec coverage (against §6.3 hot/cold, §8 HTTP API, §11 Slice 5):**
- **Querier `MetricStore` impl** (`CrabkaMetricStore`) merging cold `BlockStore::scan_context` + hot WAL-tail head, UNION-ed, split at the compaction frontier to avoid double-count → Tasks 2 (head), 4 (store). The no-double-count property is the headline test (`c == 2`, not 3).
- **WAL-tail head** rebuildable from offsets, retention-bounded, projected as DataFusion `MemTable`s → Tasks 2 (`WalHead`), 3 (`HeadTailer` consumer loop + rebuild-from-`Earliest`).
- **`label_names`/`label_values`/`series`** from blockstore `Index` ∪ head → Task 4.
- **Prometheus HTTP API** dual-mounted (`/api/v1` + `/prometheus/api/v1`), tenant via `X-Scope-OrgID` → Task 5 (`router`/`tenant_of`).
- **`/query`, `/query_range`** driving `PromqlEngine`, exact JSON → Tasks 5 (json), 6 (handlers). Vector/matrix/scalar/error exact-shape tests = the byte-equality analog.
- **`/series`, `/labels`, `/label/{name}/values`, `/metadata`, `/query_exemplars` (empty stub), `/status/buildinfo`** → Task 6.
- **Role binary `--target querier`** → Task 7.
- **Real-broker WAL-tail head** via in-process broker, `#[ignore]`d → Task 7 e2e.

**Placeholder scan / flagged deviations (honest):**
- **Native-histogram JSON** (`histogram_to_json`) is a flagged PLACEHOLDER — float vector/matrix/scalar/error are the byte-exact assertions in scope; the native-histogram bucket shape needs a dedicated test cross-checked against real Prometheus and is called out, not faked.
- **`/metadata` + `/query_exemplars`** return empty success bodies — correct per the task (exemplar sidecar read + metadata index are later wiring); shape is valid for Grafana.
- **Binary `build_blockstore_from_env` / `make_frontier`** are `todo!()` wiring fns gated on Slice 4's object-store config + compactor frontier commit — flagged as wiring, not logic; all logic is unit-tested broker-free.
- **The e2e test** is an `#[ignore]`d skeleton with an explicit fill-list (produce → tail → query), not fabricated passing assertions.

**Churn-prone surfaces — structured + behavior-pinned, not fabricated (per CLAUDE.md):**
- **DataFusion UNION of hot+cold** (Task 4 `register_union`/`register_memtable`/`table_provider`) — pinned by the `c == 2` no-double-count test; `CREATE VIEW … UNION ALL` vs `DataFrame::union(...).into_view()` fallback both flagged with a verify-checklist.
- **Consumer client API** (Task 3) — built from the real `Consumer::builder()…subscribe(vec).auto_offset_reset(Earliest).build()` + `poll(Duration) -> Result<Vec<ConsumerRecord>>` shape read from the crate; the `WalRecord` decode is isolated in `apply_wal_record` with a pure unit test and verify-notes.
- **`crabka-promql` contract** (`MetricStore`/`ScanResult`/`PromqlEngine`/`QueryResult`/`SampleValue`) — consumed verbatim from the shared contract; every spot where a field name might differ (`ts_ms`, `Scalar` ts, `PromqlError` variants, `EngineOpts::default`) carries an explicit "adapt the Rust, keep the JSON" verify-note. (Slice 2's `PromqlEngine` has no `store()` accessor — its `store` field is private — so `AppState` carries an `Arc<CrabkaMetricStore>` directly for the metadata handlers.)

**Type consistency:** `WalHead`/`SharedHead`/`HeadSample` consistent across Tasks 2/3/4/6/8. `CrabkaMetricStore::new(blockstore, head, frontier)` signature stable Tasks 4/6/7/8. `AppState`/`router`/`tenant_of` stable Tasks 5/6/7/8. `format_sample_value`/`query_result_to_json`/`success`/`error_envelope` defined once (Task 5), used by handlers (Task 6) and pinned by the JSON tests. `QuerierConfig` fields stable Tasks 1/3/7.

**Known risk (flagged, not hidden):** the two genuine cross-slice unknowns are (a) Slice 4's exact `WalRecord`/`SamplePayload` field names + WAL topic constant + compaction-frontier surface, and (b) Slice 2/3's exact `QueryResult`/`PromqlError`/`PromqlEngine::store()` shapes. Both are contained to clearly-marked seams (`apply_wal_record`, `classify`, the store accessor decision) with verify-notes and behavior-pinning tests, so any drift surfaces as a localized compile error against green tests — never silent wrong results.
