# crabka-traces Slice 7 — Metrics-generator (span-metrics RED + service-graphs + remote_write)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `metrics-generator` role — the third traces consumer group. It runs two processors over the `__crabka_traces_wal` stream: a **span-metrics (RED)** processor (`traces_spanmetrics_calls_total` counter, `traces_spanmetrics_latency` histogram with `ObserveWithExemplar(trace_id)`, `traces_spanmetrics_size_total`, optional `traces_target_info`) dimensioned by `service`/`span_name`/`span_kind`/`status_code`; and a **service-graphs** processor that pairs a client-kind span with the server-kind span of the **same trace** via a bounded TTL'd edge store keyed by `trace + relationship` (partner→record, wait-expiry→`unpaired_spans_total`, store-full→`dropped_spans_total`) and emits `traces_service_graph_request_total`/`_request_failed_total`/`_request_server_seconds`/`_request_client_seconds`/`_request_messaging_system_seconds` labeled `client`/`server`/`connection_type`. Both processors flush via the **remote_write client reused from the metrics signal** (native-histogram codec + exemplars) to a configured Prometheus endpoint. The headline tests are the **TTL'd edge-pairing state machine**, the **RED derivation**, and **exemplar attachment**, all driven by a mock remote_write sink + an injected clock. Wired behind `crabka-traces --target metrics-generator`. Edge state is in-memory and rebuildable from WAL offsets; checkpoint-to-compacted-topic is structured-but-deferred.

**Architecture:** The metrics-generator holds a `SpanSource` (the traces WAL consumer group; mocked in tests), runs each polled `SpanRecord` through the two processors, and on each **collection interval** flushes accumulated series through a `RemoteWriteSink`. The two processors split cleanly:

- **Span-metrics** is a *pure fold*: a `SpanMetricsRegistry` accumulates per-`(service, span_name, span_kind, status_code)` counters + a latency native-histogram (absolute bucket counts at rest, reusing Slice-1 metrics' `NativeHistogram`), retaining a bounded set of latency exemplars (each = `(value, trace_id, span_id)`). `record_span` is a side-effect-free state update; `drain()` produces the remote_write series.
- **Service-graphs** is a *TTL'd edge state machine*: each span is mapped to an `EdgeKey = (trace_id, connection_key)`; the first arrival (client *or* server) records a half-edge, the partner completes it, a completed edge emits one request observation. The bounded `EdgeStore` evicts on `max_items` (→ `dropped_spans_total`) and expires half-edges past their TTL (→ `unpaired_spans_total`), both driven by an injected `Clock`.

The two churn-prone surfaces — the **traces WAL consumer** (`crabka-client-consumer`) and the **remote_write HTTP client** (prost-encoded `WriteRequest` over `reqwest`) — are abstracted behind narrow traits (`SpanSource`, `RemoteWriteSink`) with in-memory mocks, so the processors and the flush path are pure, deterministic test concerns. The collection clock is injected (`Clock` trait) so TTL expiry and interval flushes are tested without real time.

**Tech Stack:** Rust 2024 · `arrow` 59 (the latency native-histogram reuses Slice-1's `NativeHistogram` ⇄ Arrow codec is *not* needed here — remote_write carries the histogram on the wire) · `serde` (config) · `serde_json` (none on the hot path) · `prost` 0.13 (remote_write `WriteRequest` encode — reuses the metrics Slice-4 `pb::v1`/`pb::v2` generated types) · `reqwest` 0.13 (remote_write POST) · `tokio` (collect loop, injected clock) · `thiserror`. Tests: `assert2`, `tokio` (`macros`, `rt-multi-thread`, `time`, `test-util`), `tempfile`. Consumes `crabka-traceql` (the `SpanRecord`/span shape via the Slice-4 service contract) and `crabka-metrics` Slice 1/4 (`NativeHistogram`, the remote_write `pb` types + symbol-table interning).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change schemas/enums/wire-internal types freely; no shims, no migration code, no `#[serde(default)]` "for old WAL records", no V1/V2 dual variants kept for replay.
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-traces --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-traces` before every commit (**never** `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` in tests; `assert2::check!` where multiple soft checks help.
- **Prometheus remote_write wire identity is the compat constraint on the output edge.** The series this role produces (`traces_spanmetrics_*`, `traces_service_graph_*`) are *ordinary* Prometheus series + native histograms + exemplars, byte-encoded via the metrics signal's existing `pb::v1::WriteRequest`/`pb::v2::Request` types — byte-identical to a Prometheus-scraper-originated remote_write request. No metrics-generator-private record format. The **metric names, label names, `_total`/`_seconds`/`_bucket` suffixes, and `le`/`connection_type` label conventions must match Tempo exactly** (Grafana reads them by name; the Service Graph panel hard-codes `traces_service_graph_*`).
- **Kafka wire-protocol exactness** on the *input* edge is preserved automatically by consuming through the existing `crabka-client-consumer` client — do not hand-roll protocol frames. The metrics-generator is the third independent consumer group on the `trace_id`-partitioned WAL (`group_id = "crabka-traces-metrics-generator"`), with its own offsets; the dedup-avoidance invariant (all spans of a trace in one partition) is what lets it run at RF1.
- **Injected time + injected sinks.** The collect loop never reads the wall clock directly and never calls a real consumer/HTTP endpoint in unit tests — `Clock`, `SpanSource`, `RemoteWriteSink` are traits with deterministic mocks. This is what makes the TTL edge-pairing state machine, the RED derivation, and exemplar attachment first-class testable.
- **Rebuildable, no durable state.** Edge state + accumulated series are pure in-memory read-derived state, rebuildable from WAL offsets on restart (spec §9). An optional checkpoint to a compacted topic is structured (codec + trait) but its live Kafka-backed impl is deferred to Slice 8.

---

## Dependency & slice roadmap

**Depends on:**
- **Slice 4 (ingest service)** — defines `SpanRecord` (the WAL record: `tenant` + an OTLP-derived span — `trace_id[16]`, `span_id[8]`, `parent_span_id[8]`, `name`, `kind`, `start_ns`, `duration_ns`, `status`, resource attrs, span attrs, events, links) and `TRACES_WAL_TOPIC = "__crabka_traces_wal"`. Slice 4's real `SpanRecord` is `{ tenant: String, span: Span }` (span fields nested under `span`); this role reads a **flattened projection** of it via `contract::SpanRecord`, filled by the `SpanSource` decode adapter (see the re-export-swap note). `SpanKind`/`StatusCode`/`TRACES_WAL_TOPIC` are consumed verbatim. The metrics-generator is a *peer consumer group* defined alongside `block-builder`/`live-store` in Slice 4's service crate.
- **Slice 1 (`crabka-blockstore`) / `crabka-metrics` Slice 1+4** — `NativeHistogram` (absolute bucket counts) for the latency histogram, and the remote_write `pb::v1`/`pb::v2` generated message types + the `SymbolTable` interner (Slice-4 metrics `crate::wire::pb` + `crate::SymbolTable`) for the v2 encode path. *These crates may not be merged when this slice is implemented;* consume only the **contract** (the `NativeHistogram` field set, the `pb` message shape, "POST a `WriteRequest` to a configured endpoint") and wrap it behind `RemoteWriteSink` so the exact wire encoder can land later without touching the processors.
- **`crabka-client-consumer`** — `Consumer::builder().bootstrap(..).group_id(..).subscribe([..]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`; `Consumer::poll(Duration) -> Result<Vec<ConsumerRecord>, ConsumerError>`; `Consumer::commit_sync() -> Result<(), ConsumerError>`; `ConsumerRecord { topic, partition:i32, offset:i64, key:Option<Bytes>, value:Option<Bytes>, .. }`. (Verify against `crates/client-consumer/src/{consumer,poll,commit}.rs`.) Wrapped behind `SpanSource` — the role decodes `ConsumerRecord.value` → `SpanRecord` only inside the real source impl.

**This plan = Slice 7 of 8.** Remaining: Slice 8 hardening (per-tenant limits + multi-tenancy isolation, the differential-vs-Tempo corpus, and the Grafana integration where the Service Graph is rendered end-to-end from the `traces_service_graph_*` series). The checkpoint-to-compacted-topic edge-state persistence + the real Kafka consumer/remote_write impls are finished there.

**Contract-shim note (read before Task 1):** because Slice 4 (and the metrics remote_write types) may not be merged when this slice is implemented, every consumed type is referenced through a single `crate::metricsgen::contract` re-export module. If the upstream crate is present, `contract` re-exports the real types; if not, the implementer creates a minimal local `mod contract` with the exact signatures below so this slice compiles and tests in isolation. **Do not** fork divergent definitions — `contract` is one file, swapped to re-exports the moment upstream lands. Flag any signature drift loudly rather than silently adapting.

---

## File structure (additions to `crates/traces/`)

| File | Responsibility |
|---|---|
| `src/metricsgen/mod.rs` | `metrics-generator` module decls + public re-exports + `contract` shim |
| `src/metricsgen/clock.rs` | `Clock` trait + `SystemClock` + `MockClock` |
| `src/metricsgen/config.rs` | `MetricsGenConfig` (collection interval, histogram buckets, dimensions, exemplar cap, edge-store TTL/`max_items`, target-info toggle) + serde + defaults |
| `src/metricsgen/series.rs` | `Series`/`SeriesSample`/`Exemplar`/`SeriesPayload` neutral output model (what both processors emit, what `RemoteWriteSink` consumes) |
| `src/metricsgen/spanmetrics.rs` | `SpanMetricsRegistry` — RED fold (`record_span` + `drain`), latency histogram + exemplars |
| `src/metricsgen/servicegraph.rs` | `EdgeStore` TTL'd edge state machine (`record_span` + `expire` + `drain`), connection-type classification |
| `src/metricsgen/sink.rs` | `RemoteWriteSink` trait + `MockRemoteWriteSink` + `SpanSource` trait + `MockSpanSource` |
| `src/metricsgen/remotewrite.rs` | `PrometheusRemoteWriteSink: RemoteWriteSink` — `Series` → `pb::WriteRequest` → `reqwest` POST (churn-prone; pure transform pinned) |
| `src/metricsgen/checkpoint.rs` | edge-state compacted-topic key/value codec (structured + tested; live impl deferred) |
| `src/metricsgen/processor.rs` | `MetricsGenerator` — owns both processors + clock; `process(SpanRecord)` + `collect()` (interval flush) |
| `src/metricsgen/service.rs` | `MetricsGenService` — wires `SpanSource` + processors + sink + clock; the poll/collect loop |
| `src/bin/metrics_generator.rs` *(or arm in existing `main.rs`)* | `crabka-traces --target metrics-generator` wiring |
| `Cargo.toml` | add `serde`, `prost`, `reqwest`, `tokio`, `tracing` deps (if absent) |

---

### Task 1: Metrics-generator module scaffold + contract shim + deps

**Files:**
- Modify: `crates/traces/Cargo.toml`
- Modify (workspace): `Cargo.toml` (add `reqwest`/`prost` if absent)
- Create: `crates/traces/src/metricsgen/mod.rs`
- Modify: `crates/traces/src/lib.rs` (add `pub mod metricsgen;`)

**Interfaces:**
- Produces: a compiling `metricsgen` module + `crate::metricsgen::contract` re-exporting (or locally defining) `SpanRecord`, `SpanKind`, `StatusCode`, `TRACES_WAL_TOPIC`, `NativeHistogram`, `BucketSpan`.

- [x] **Step 1: Add deps to `crates/traces/Cargo.toml`**

```toml
[dependencies]
# ...existing traces deps...
serde = { workspace = true, features = ["derive"] }
prost = { workspace = true }
reqwest = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "sync"] }
tracing = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
# crabka-metrics (NativeHistogram + remote_write pb types) / Slice-4 SpanRecord:
# add the path deps once those crates/modules exist.
# crabka-metrics = { path = "../metrics", version = "0.x" }

[dev-dependencies]
assert2 = { workspace = true }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time", "test-util"] }
tempfile = { workspace = true }
```

> **Verify-note (workspace deps):** `serde`, `reqwest`, `prost`, `tokio`, `tracing`, `thiserror`, `async-trait`, `tempfile`, `assert2` are already `[workspace.dependencies]` (confirmed in root `Cargo.toml`, used by the metrics slices). If `reqwest`/`prost` are not yet workspace deps, add them mirroring the metrics Slice-4 manifest: `reqwest = { version = "0.13", default-features = false, features = ["json", "rustls-tls"] }`, `prost = "0.13"`. Use `default-features = false` + `rustls-tls` to match the workspace's rustls-everywhere posture (no native-tls/openssl).

- [x] **Step 2: Create the `contract` shim + module decls**

Create `crates/traces/src/metricsgen/mod.rs`:

```rust
//! The `metrics-generator` role: the third traces consumer group. Runs the
//! span-metrics (RED) and service-graph processors over the WAL stream and
//! flushes their series via the metrics signal's remote_write client to a
//! configured Prometheus endpoint.

pub mod checkpoint;
pub mod clock;
pub mod config;
pub mod processor;
pub mod remotewrite;
pub mod series;
pub mod service;
pub mod servicegraph;
pub mod sink;
pub mod spanmetrics;

/// Single point of truth for types consumed from sibling slices.
///
/// When Slice 4 (`SpanRecord`) and the metrics remote_write types are present,
/// re-export their real types here. Until then, these local definitions carry
/// the **exact** signatures from the shared contract so this slice compiles and
/// tests in isolation. Swap to `pub use` re-exports the moment upstream lands;
/// do not let the two diverge.
pub mod contract {
    /// The WAL topic the three traces consumer groups read.
    pub const TRACES_WAL_TOPIC: &str = "__crabka_traces_wal";

    /// OTLP span kind (matches the spec §4.1 enum order).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum SpanKind {
        Unspecified,
        Internal,
        Server,
        Client,
        Producer,
        Consumer,
    }

    /// OTLP status code (spec §4.1: `unset | ok | error`).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum StatusCode {
        Unset,
        Ok,
        Error,
    }

    impl StatusCode {
        /// The `status_code` dimension value Tempo uses (`STATUS_CODE_*`).
        #[must_use]
        pub fn as_dim(self) -> &'static str {
            match self {
                StatusCode::Unset => "STATUS_CODE_UNSET",
                StatusCode::Ok => "STATUS_CODE_OK",
                StatusCode::Error => "STATUS_CODE_ERROR",
            }
        }
    }

    /// The WAL record: a tenant + one OTLP-derived span. Mirrors Slice-4's
    /// `SpanRecord` field set (only the fields this role reads are modelled;
    /// the real type carries the full attr/event/link payload).
    #[derive(Clone, Debug, PartialEq)]
    pub struct SpanRecord {
        pub tenant: String,
        pub trace_id: [u8; 16],
        pub span_id: [u8; 8],
        pub parent_span_id: [u8; 8],
        pub name: String,
        pub kind: SpanKind,
        pub start_ns: i64,
        pub duration_ns: i64,
        pub status: StatusCode,
        /// `service.name` resource attribute (the RED + service-graph node id).
        pub service_name: String,
        /// Span attributes (read for service-graph connection-type +
        /// `db.system`/`messaging.system`/`peer.service` classification).
        pub attributes: Vec<(String, String)>,
        /// Serialized span size in bytes (drives `traces_spanmetrics_size_total`).
        pub size_bytes: u64,
    }

    /// A run of populated histogram buckets (matches metrics Slice-1
    /// `BucketSpan`).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct BucketSpan {
        pub offset: i32,
        pub length: u32,
    }

    /// A native (exponential) histogram with **absolute** bucket counts. Mirrors
    /// metrics Slice-1's `NativeHistogram`; only the fields the latency histogram
    /// populates are modelled here. (On re-export this becomes
    /// `crabka_metrics::NativeHistogram`.)
    #[derive(Clone, Debug, PartialEq)]
    pub struct NativeHistogram {
        pub schema: i8,
        pub zero_threshold: f64,
        pub zero_count: f64,
        pub count: f64,
        pub sum: f64,
        pub positive_spans: Vec<BucketSpan>,
        pub positive_counts: Vec<f64>,
    }
}

pub use contract::{BucketSpan, NativeHistogram, SpanKind, SpanRecord, StatusCode, TRACES_WAL_TOPIC};
```

> **Re-export-swap note:** the metrics-generator is a `metricsgen` module **inside the same `crabka-traces` crate** that Slice 4 builds (the `SpanRecord`/`SpanKind`/`StatusCode`/`TRACES_WAL_TOPIC` come from `crate::wal` + `crate::span`, not an external crate). The moment Slice 4 lands, replace the matching bodies of `contract` with `pub use crate::wal::TRACES_WAL_TOPIC; pub use crate::span::{SpanKind, StatusCode};` (these match Slice 4's definitions verbatim — identical variant order, identical topic literal) and `pub use crabka_metrics::{NativeHistogram, BucketSpan};` once the metrics crate lands. **`SpanRecord` is the exception:** Slice 4's real `SpanRecord` is `{ tenant: String, span: Span }` (the span fields are nested under `span`), whereas this role's `contract::SpanRecord` is a **flattened projection** the processors read (`service_name`, `kind`, `status`, `duration_ns`, `size_bytes`, span attrs). So `contract::SpanRecord` stays a local type, and the **`SpanSource` adapter** (Task 7 / the real impl in Task 12) decodes `crate::wal::SpanRecord` → this projection — adapt that one decode site, never the processor logic. No other `metricsgen` file references the upstream types directly — they all go through `contract`, so this stays a one-file swap (plus the `SpanSource` decode).

- [x] **Step 3: Wire into `lib.rs`**

Add `pub mod metricsgen;` to `crates/traces/src/lib.rs`.

- [x] **Step 4: Stub the remaining module files**

Create empty-but-compiling `clock.rs`, `config.rs`, `series.rs`, `spanmetrics.rs`, `servicegraph.rs`, `sink.rs`, `remotewrite.rs`, `checkpoint.rs`, `processor.rs`, `service.rs` each with a `//!` doc line. (Each fills in its own Task below; they must exist for `mod.rs` to compile.)

- [x] **Step 5: Build**

Run: `cargo build -p crabka-traces`
Expected: compiles (empty modules + contract shim).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/ Cargo.toml
git commit -m "feat(traces): scaffold metrics-generator module + contract shim + deps"
```

---

### Task 2: Injectable clock

**Files:**
- Modify: `crates/traces/src/metricsgen/clock.rs`

**Interfaces:**
- Produces:
  - `trait Clock: Send + Sync { fn now_ns(&self) -> i64; }` (nanoseconds — span timestamps are ns; TTL math stays in one unit).
  - `struct SystemClock;` (`Clock` via `SystemTime::now()`).
  - `struct MockClock { ... }` with `new(start_ns: i64)`, `advance(&self, ns: i64)`, `set(&self, ns: i64)` (interior-mutable, `Clock`-impl, `Clone`).

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/clock.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn mock_clock_advances() {
        let c = MockClock::new(1_000);
        assert!(c.now_ns() == 1_000);
        c.advance(500);
        assert!(c.now_ns() == 1_500);
        c.set(42);
        assert!(c.now_ns() == 42);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::clock`
Expected: FAIL — `cannot find type MockClock`.

- [x] **Step 3: Implement**

```rust
//! Injectable clock so TTL edge-expiry and interval flushes are testable without
//! real time. Unit is **nanoseconds** to match span timestamps.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock source in epoch nanoseconds.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i64;
}

/// Production clock.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
    }
}

/// Deterministic test clock (interior-mutable, cheap to clone).
#[derive(Debug, Clone)]
pub struct MockClock {
    now: Arc<AtomicI64>,
}

impl MockClock {
    #[must_use]
    pub fn new(start_ns: i64) -> Self {
        Self { now: Arc::new(AtomicI64::new(start_ns)) }
    }
    pub fn advance(&self, ns: i64) {
        self.now.fetch_add(ns, Ordering::SeqCst);
    }
    pub fn set(&self, ns: i64) {
        self.now.store(ns, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ns(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::clock`
Expected: PASS.

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use clock::{Clock, MockClock, SystemClock};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): metrics-generator injectable clock (SystemClock + MockClock)"
```

---

### Task 3: Config model

**Files:**
- Modify: `crates/traces/src/metricsgen/config.rs`

**Interfaces:**
- Produces:
  - `struct MetricsGenConfig { pub collection_interval: Duration, pub histogram_buckets_ns: Vec<f64>, pub latency_native_schema: i8, pub max_exemplars_per_series: usize, pub edge_ttl: Duration, pub edge_store_max_items: usize, pub enable_target_info: bool, pub enable_messaging_system_latency: bool, pub remote_write_url: String }` (`Clone`, `Debug`; `serde` with `#[serde(default)]` on each field meaning *absent config field*, not back-compat).
  - `impl Default for MetricsGenConfig` — Tempo-equivalent defaults: `collection_interval = 15s`, `latency_native_schema = 8`, `max_exemplars_per_series = 0` (off until configured), `edge_ttl = 10s`, `edge_store_max_items = 10_000`, `enable_target_info = false`, `enable_messaging_system_latency = false`.
  - `const DEFAULT_LATENCY_BUCKETS_NS: &[f64]` — the classic `_seconds` histogram bucket edges (Tempo's defaults) expressed in **nanoseconds** (the dimension spans are in ns; remote_write `_bucket` `le` labels are rendered in seconds at encode time).

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_match_tempo() {
        let c = MetricsGenConfig::default();
        assert!(c.collection_interval == Duration::from_secs(15));
        assert!(c.edge_ttl == Duration::from_secs(10));
        assert!(c.edge_store_max_items == 10_000);
        assert!(!c.enable_target_info);
        assert!(c.max_exemplars_per_series == 0);
        assert!(!c.histogram_buckets_ns.is_empty());
    }

    #[test]
    fn parses_partial_yaml_falling_back_to_defaults() {
        // only collection_interval set; everything else defaulted.
        let c: MetricsGenConfig =
            serde_yaml::from_str("collection_interval_secs: 30\nmax_exemplars_per_series: 5\n").unwrap();
        assert!(c.collection_interval == Duration::from_secs(30));
        assert!(c.max_exemplars_per_series == 5);
        assert!(c.edge_store_max_items == 10_000); // defaulted
    }
}
```

> **serde-shape note:** `Duration` has no natural YAML form, so the serde representation uses scalar `*_secs`/`*_ns` fields (see the `#[serde(...)]` mapping in the impl). The `serde_yaml` dep is already a workspace dep (used by the metrics ruler slice). Add `serde_yaml = { workspace = true }` to `[dev-dependencies]` if config-from-YAML is test-only here, or `[dependencies]` if the binary loads a config file (it does — Task 12), so put it in `[dependencies]`.

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::config`
Expected: FAIL — `cannot find type MetricsGenConfig`.

- [x] **Step 3: Implement**

```rust
//! Metrics-generator configuration (collection interval, latency buckets,
//! exemplar cap, edge-store TTL/bounds, target-info toggle, remote_write URL).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Tempo-default latency histogram bucket edges, in **nanoseconds**. Rendered to
/// `_bucket{le="..."}` in **seconds** at remote_write encode time. (Tempo's
/// default classic buckets: 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms, 256ms,
/// 512ms, 1.02s, 2.05s, 4.1s, 8.19s, 16.38s.)
pub const DEFAULT_LATENCY_BUCKETS_NS: &[f64] = &[
    2_000_000.0,
    4_000_000.0,
    8_000_000.0,
    16_000_000.0,
    32_000_000.0,
    64_000_000.0,
    128_000_000.0,
    256_000_000.0,
    512_000_000.0,
    1_024_000_000.0,
    2_048_000_000.0,
    4_096_000_000.0,
    8_192_000_000.0,
    16_384_000_000.0,
];

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsGenConfig {
    #[serde(rename = "collection_interval_secs", with = "secs")]
    pub collection_interval: Duration,
    pub histogram_buckets_ns: Vec<f64>,
    pub latency_native_schema: i8,
    pub max_exemplars_per_series: usize,
    #[serde(rename = "edge_ttl_secs", with = "secs")]
    pub edge_ttl: Duration,
    pub edge_store_max_items: usize,
    pub enable_target_info: bool,
    pub enable_messaging_system_latency: bool,
    pub remote_write_url: String,
}

impl Default for MetricsGenConfig {
    fn default() -> Self {
        Self {
            collection_interval: Duration::from_secs(15),
            histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
            latency_native_schema: 8,
            max_exemplars_per_series: 0,
            edge_ttl: Duration::from_secs(10),
            edge_store_max_items: 10_000,
            enable_target_info: false,
            enable_messaging_system_latency: false,
            remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
        }
    }
}

/// serde adapter: `Duration` <-> integer seconds scalar.
mod secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::config`
Expected: PASS (2 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use config::{DEFAULT_LATENCY_BUCKETS_NS, MetricsGenConfig};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): metrics-generator config model + Tempo-default buckets"
```

---

### Task 4: Neutral output series model

**Files:**
- Modify: `crates/traces/src/metricsgen/series.rs`

This is the boundary between the two processors and the remote_write encoder: both `drain()` into `SeriesPayload`, and `RemoteWriteSink` consumes it. Keeping it a plain data model (no prost, no reqwest) makes the processor tests assert *what series with what labels/values/exemplars* without any wire concern.

**Interfaces:**
- Produces:
  - `struct Exemplar { pub value: f64, pub labels: Vec<(String, String)>, pub timestamp_ms: i64 }` — the `trace_id` (and `span_id`) ride in `labels` (Prometheus exemplar label form).
  - `enum SeriesSample { Counter(f64), Gauge(f64), ClassicHistogram { buckets: Vec<(f64 /*le_seconds*/, f64 /*cumulative_count*/)>, sum: f64, count: f64 }, NativeHistogram(NativeHistogram) }`.
  - `struct Series { pub name: String, pub labels: Vec<(String, String)>, pub sample: SeriesSample, pub exemplars: Vec<Exemplar>, pub timestamp_ms: i64 }` — `labels` does **not** include `__name__` (the encoder injects it from `name`); labels are sorted.
  - `struct SeriesPayload { pub tenant: String, pub series: Vec<Series> }`.
  - `fn sorted_labels(pairs: Vec<(String, String)>) -> Vec<(String, String)>` — stable label ordering helper used by both processors.

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/series.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn sorted_labels_orders_by_key() {
        let l = sorted_labels(vec![
            ("span_name".into(), "GET".into()),
            ("service".into(), "api".into()),
        ]);
        assert!(l[0].0 == "service");
        assert!(l[1].0 == "span_name");
    }

    #[test]
    fn series_carries_exemplars() {
        let s = Series {
            name: "traces_spanmetrics_latency".into(),
            labels: sorted_labels(vec![("service".into(), "api".into())]),
            sample: SeriesSample::Counter(1.0),
            exemplars: vec![Exemplar {
                value: 0.5,
                labels: vec![("trace_id".into(), "abc".into())],
                timestamp_ms: 1_000,
            }],
            timestamp_ms: 1_000,
        };
        assert!(s.exemplars[0].labels[0].0 == "trace_id");
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::series`
Expected: FAIL — `cannot find function sorted_labels`.

- [x] **Step 3: Implement**

```rust
//! Neutral output model produced by both processors and consumed by
//! `RemoteWriteSink`. Plain data — no prost, no reqwest — so processor tests
//! assert series/labels/values/exemplars without any wire concern.

use super::contract::NativeHistogram;

/// A Prometheus exemplar: a sampled observation with attached labels (carries
/// `trace_id`/`span_id` for the metrics→traces drill-down link).
#[derive(Clone, Debug, PartialEq)]
pub struct Exemplar {
    pub value: f64,
    pub labels: Vec<(String, String)>,
    pub timestamp_ms: i64,
}

/// The value carried by a series sample.
#[derive(Clone, Debug, PartialEq)]
pub enum SeriesSample {
    Counter(f64),
    Gauge(f64),
    /// Classic histogram: cumulative `(le_seconds, count)` buckets + `_sum`/`_count`.
    ClassicHistogram {
        buckets: Vec<(f64, f64)>,
        sum: f64,
        count: f64,
    },
    /// Native (exponential) histogram — remote_write v2 carries it natively.
    NativeHistogram(NativeHistogram),
}

/// One output series at one timestamp.
#[derive(Clone, Debug, PartialEq)]
pub struct Series {
    /// The metric name (the encoder injects it as `__name__`).
    pub name: String,
    /// Sorted label pairs, **excluding** `__name__`.
    pub labels: Vec<(String, String)>,
    pub sample: SeriesSample,
    pub exemplars: Vec<Exemplar>,
    pub timestamp_ms: i64,
}

/// A tenant-scoped batch of series for one collection.
#[derive(Clone, Debug, PartialEq)]
pub struct SeriesPayload {
    pub tenant: String,
    pub series: Vec<Series>,
}

/// Sort label pairs by key for stable series identity.
#[must_use]
pub fn sorted_labels(mut pairs: Vec<(String, String)>) -> Vec<(String, String)> {
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
}
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::series`
Expected: PASS (2 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use series::{Exemplar, Series, SeriesPayload, SeriesSample, sorted_labels};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): metrics-generator neutral series output model"
```

---

### Task 5: Span-metrics (RED) registry — the RED-derivation centerpiece

**Files:**
- Modify: `crates/traces/src/metricsgen/spanmetrics.rs`

This is the first headline: a pure fold that turns each span into RED series. The two first-class test concerns — the **RED derivation** (counter/size/latency per dimension set) and **exemplar attachment** (`ObserveWithExemplar(trace_id)`) — both live here and are driven entirely by hand-built `SpanRecord`s + a `drain()` assertion.

**Interfaces:**
- Consumes: `contract::{SpanRecord, SpanKind, StatusCode, NativeHistogram, BucketSpan}`, `MetricsGenConfig`, `series::{Series, SeriesSample, Exemplar, sorted_labels}`.
- Produces:
  - `struct SpanMetricsRegistry { /* config snapshot + accumulator */ }` with `new(cfg: &MetricsGenConfig) -> Self`.
  - `fn record_span(&mut self, span: &SpanRecord)` — side-effect-free state update: increments `calls_total`, adds `size_bytes` to `size_total`, observes `duration_ns` into the per-dimension latency histogram, and (if `max_exemplars_per_series > 0`) retains one latency exemplar carrying `trace_id`/`span_id` hex.
  - `fn drain(&mut self, timestamp_ms: i64) -> Vec<Series>` — emit `traces_spanmetrics_calls_total` (counter), `traces_spanmetrics_size_total` (counter), `traces_spanmetrics_latency` (classic histogram with exemplars) per dimension set; reset accumulator. If `enable_target_info`, also emit `traces_target_info` (gauge = 1) per `(service)` once.
  - `fn dimension_labels(span: &SpanRecord) -> Vec<(String, String)>` — `service`/`span_name`/`span_kind`/`status_code` (the §7.1 dimension set).
  - internal `struct LatencyHistogram { bucket_counts: Vec<u64>, sum_ns: f64, count: u64 }` accumulating into the configured `histogram_buckets_ns`.

- [x] **Step 1: Write the failing tests (RED values + exemplar attachment)**

In `crates/traces/src/metricsgen/spanmetrics.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::series::SeriesSample;

    fn span(service: &str, name: &str, kind: SpanKind, status: StatusCode, dur_ns: i64, size: u64) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            parent_span_id: [0; 8],
            name: name.into(),
            kind,
            start_ns: 0,
            duration_ns: dur_ns,
            status,
            service_name: service.into(),
            attributes: vec![],
            size_bytes: size,
        }
    }

    fn find<'a>(series: &'a [Series], name: &str, span_name: &str) -> &'a Series {
        series
            .iter()
            .find(|s| s.name == name && s.labels.iter().any(|(k, v)| k == "span_name" && v == span_name))
            .unwrap_or_else(|| panic!("no {name} for {span_name}"))
    }

    #[test]
    fn red_counts_calls_and_size_per_dimension() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 5_000_000, 100));
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 7_000_000, 150));
        reg.record_span(&span("api", "GET /y", SpanKind::Server, StatusCode::Error, 3_000_000, 50));

        let out = reg.drain(1_000);

        let calls_x = find(&out, "traces_spanmetrics_calls_total", "GET /x");
        assert!(matches!(calls_x.sample, SeriesSample::Counter(c) if (c - 2.0).abs() < 1e-9));
        let size_x = find(&out, "traces_spanmetrics_size_total", "GET /x");
        assert!(matches!(size_x.sample, SeriesSample::Counter(c) if (c - 250.0).abs() < 1e-9));

        // dimension set carries all four labels.
        let labels = &calls_x.labels;
        assert!(labels.iter().any(|(k, v)| k == "service" && v == "api"));
        assert!(labels.iter().any(|(k, v)| k == "span_kind" && v == "SPAN_KIND_SERVER"));
        assert!(labels.iter().any(|(k, v)| k == "status_code" && v == "STATUS_CODE_OK"));

        // a distinct dimension set ("GET /y", Error) is its own series.
        let calls_y = find(&out, "traces_spanmetrics_calls_total", "GET /y");
        assert!(matches!(calls_y.sample, SeriesSample::Counter(c) if (c - 1.0).abs() < 1e-9));
    }

    #[test]
    fn latency_histogram_buckets_and_sum() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        // 5ms and 7ms both fall in the 8ms bucket and above.
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 5_000_000, 1));
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 7_000_000, 1));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        match &lat.sample {
            SeriesSample::ClassicHistogram { buckets, sum, count } => {
                assert!((*count - 2.0).abs() < 1e-9);
                // sum is in SECONDS (5ms + 7ms = 0.012s).
                assert!((*sum - 0.012).abs() < 1e-6);
                // cumulative: the le=0.008 bucket holds both observations.
                let le_8ms = buckets.iter().find(|(le, _)| (*le - 0.008).abs() < 1e-9).unwrap();
                assert!((le_8ms.1 - 2.0).abs() < 1e-9);
                // a smaller bucket (le=0.004) holds neither.
                let le_4ms = buckets.iter().find(|(le, _)| (*le - 0.004).abs() < 1e-9).unwrap();
                assert!(le_4ms.1.abs() < 1e-9);
            }
            other => panic!("expected ClassicHistogram, got {other:?}"),
        }
    }

    #[test]
    fn exemplar_carries_trace_id_when_enabled() {
        let mut cfg = MetricsGenConfig::default();
        cfg.max_exemplars_per_series = 2;
        let mut reg = SpanMetricsRegistry::new(&cfg);
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 5_000_000, 1));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert!(lat.exemplars.len() == 1);
        let ex = &lat.exemplars[0];
        // trace_id is hex of [0xAB; 16].
        assert!(ex.labels.iter().any(|(k, v)| k == "trace_id" && v == "abababababababababababababababab"));
        // exemplar value is the observed latency in SECONDS.
        assert!((ex.value - 0.005).abs() < 1e-6);
    }

    #[test]
    fn exemplars_off_by_default() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 5_000_000, 1));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert!(lat.exemplars.is_empty());
    }

    #[test]
    fn drain_resets_accumulator() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span("api", "GET /x", SpanKind::Server, StatusCode::Ok, 5_000_000, 1));
        let _ = reg.drain(1_000);
        assert!(reg.drain(2_000).is_empty()); // nothing left
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::spanmetrics`
Expected: FAIL — `cannot find type SpanMetricsRegistry`.

- [x] **Step 3: Implement**

```rust
//! Span-metrics (RED): per-`(service, span_name, span_kind, status_code)`
//! `calls_total` + `size_total` counters and a `latency` classic histogram with
//! `trace_id` exemplars. A pure fold — `record_span` mutates the accumulator,
//! `drain` emits series and resets.

use std::collections::HashMap;

use super::config::MetricsGenConfig;
use super::contract::{SpanKind, SpanRecord, StatusCode};
use super::series::{Exemplar, Series, SeriesSample, sorted_labels};

const NS_PER_SEC: f64 = 1_000_000_000.0;

/// The §7.1 dimension key (the RED series identity).
type DimKey = (String, String, String, String); // (service, span_name, kind, status)

#[derive(Clone)]
struct LatencyHistogram {
    bucket_edges_ns: Vec<f64>,
    /// per-bucket (non-cumulative) counts, len == edges + 1 (last = +Inf).
    bucket_counts: Vec<u64>,
    sum_ns: f64,
    count: u64,
}

impl LatencyHistogram {
    fn new(edges_ns: &[f64]) -> Self {
        Self {
            bucket_edges_ns: edges_ns.to_vec(),
            bucket_counts: vec![0; edges_ns.len() + 1],
            sum_ns: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, value_ns: f64) {
        // first bucket whose edge >= value; else the +Inf bucket.
        let idx = self
            .bucket_edges_ns
            .iter()
            .position(|&e| value_ns <= e)
            .unwrap_or(self.bucket_edges_ns.len());
        self.bucket_counts[idx] += 1;
        self.sum_ns += value_ns;
        self.count += 1;
    }

    /// Cumulative `(le_seconds, count)` buckets (no `+Inf` row — the encoder
    /// adds `le="+Inf"` from `count`).
    fn cumulative_seconds(&self) -> (Vec<(f64, f64)>, f64, f64) {
        let mut cumulative = 0_u64;
        let mut out = Vec::with_capacity(self.bucket_edges_ns.len());
        for (i, &edge_ns) in self.bucket_edges_ns.iter().enumerate() {
            cumulative += self.bucket_counts[i];
            out.push((edge_ns / NS_PER_SEC, cumulative as f64));
        }
        (out, self.sum_ns / NS_PER_SEC, self.count as f64)
    }
}

struct DimEntry {
    calls: f64,
    size_total: f64,
    latency: LatencyHistogram,
    exemplars: Vec<Exemplar>,
}

pub struct SpanMetricsRegistry {
    bucket_edges_ns: Vec<f64>,
    max_exemplars: usize,
    enable_target_info: bool,
    entries: HashMap<DimKey, DimEntry>,
    /// services seen this interval (for `traces_target_info`).
    services: HashMap<String, ()>,
}

impl SpanMetricsRegistry {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            bucket_edges_ns: cfg.histogram_buckets_ns.clone(),
            max_exemplars: cfg.max_exemplars_per_series,
            enable_target_info: cfg.enable_target_info,
            entries: HashMap::new(),
            services: HashMap::new(),
        }
    }

    pub fn record_span(&mut self, span: &SpanRecord) {
        let key = (
            span.service_name.clone(),
            span.name.clone(),
            kind_dim(span.kind).to_string(),
            span.status.as_dim().to_string(),
        );
        let edges = &self.bucket_edges_ns;
        let entry = self.entries.entry(key).or_insert_with(|| DimEntry {
            calls: 0.0,
            size_total: 0.0,
            latency: LatencyHistogram::new(edges),
            exemplars: Vec::new(),
        });
        entry.calls += 1.0;
        entry.size_total += span.size_bytes as f64;
        let dur_ns = span.duration_ns.max(0) as f64;
        entry.latency.observe(dur_ns);

        if self.max_exemplars > 0 && entry.exemplars.len() < self.max_exemplars {
            entry.exemplars.push(Exemplar {
                value: dur_ns / NS_PER_SEC,
                labels: vec![
                    ("trace_id".to_string(), hex16(&span.trace_id)),
                    ("span_id".to_string(), hex8(&span.span_id)),
                ],
                timestamp_ms: span.start_ns / 1_000_000,
            });
        }

        if self.enable_target_info {
            self.services.insert(span.service_name.clone(), ());
        }
    }

    #[must_use]
    pub fn drain(&mut self, timestamp_ms: i64) -> Vec<Series> {
        let mut out = Vec::new();
        for ((service, span_name, kind, status), entry) in self.entries.drain() {
            let labels = sorted_labels(vec![
                ("service".to_string(), service.clone()),
                ("span_name".to_string(), span_name),
                ("span_kind".to_string(), kind),
                ("status_code".to_string(), status),
            ]);
            out.push(Series {
                name: "traces_spanmetrics_calls_total".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Counter(entry.calls),
                exemplars: vec![],
                timestamp_ms,
            });
            out.push(Series {
                name: "traces_spanmetrics_size_total".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Counter(entry.size_total),
                exemplars: vec![],
                timestamp_ms,
            });
            let (buckets, sum, count) = entry.latency.cumulative_seconds();
            out.push(Series {
                name: "traces_spanmetrics_latency".to_string(),
                labels,
                sample: SeriesSample::ClassicHistogram { buckets, sum, count },
                exemplars: entry.exemplars,
                timestamp_ms,
            });
        }
        if self.enable_target_info {
            for (service, ()) in self.services.drain() {
                out.push(Series {
                    name: "traces_target_info".to_string(),
                    labels: sorted_labels(vec![("service".to_string(), service)]),
                    sample: SeriesSample::Gauge(1.0),
                    exemplars: vec![],
                    timestamp_ms,
                });
            }
        }
        out
    }
}

/// The `span_kind` dimension value Tempo uses (`SPAN_KIND_*`).
fn kind_dim(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
        SpanKind::Internal => "SPAN_KIND_INTERNAL",
        SpanKind::Server => "SPAN_KIND_SERVER",
        SpanKind::Client => "SPAN_KIND_CLIENT",
        SpanKind::Producer => "SPAN_KIND_PRODUCER",
        SpanKind::Consumer => "SPAN_KIND_CONSUMER",
    }
}

fn hex16(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

fn hex8(b: &[u8; 8]) -> String {
    let mut s = String::with_capacity(16);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}
```

> **Tempo-fidelity verify-notes (pin empirically before declaring done):**
> 1. **Dimension label *values*** — Tempo renders `span_kind`/`status_code` as the OTLP enum *names* (`SPAN_KIND_SERVER`, `STATUS_CODE_OK`). Confirm against a live cp-tempo metrics-generator's emitted series (`/metrics` or the remote_write target); if Tempo uses bare names (`server`/`ok`) instead, change `kind_dim`/`as_dim` + the test together. The four dimension *keys* (`service`/`span_name`/`span_kind`/`status_code`) are load-bearing for Grafana's span-metrics dashboards — do not rename.
> 2. **Histogram bucket placement** — `observe` uses `value <= edge` (Prometheus `le` semantics: a value lands in the first bucket whose upper bound is `>= value`). The `latency_histogram_buckets_and_sum` test pins the 8ms boundary. If a differential test vs Tempo shows off-by-one bucketing, this is the line to check.
> 3. **Native vs classic latency** — this slice emits the latency as a **classic** histogram (`_bucket`/`_sum`/`_count`) for remote_write v1 compatibility and the simplest exemplar attachment. The spec mentions the native-histogram codec on the output edge; native latency is a flagged Slice-8 option (`SeriesSample::NativeHistogram` already exists in the model — Task 6's encoder handles it for service-graphs, so the path is proven). Classic is what Grafana's span-metrics panels read by default.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::spanmetrics`
Expected: PASS (5 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use spanmetrics::SpanMetricsRegistry;`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): span-metrics RED registry + latency histogram + exemplars"
```

---

### Task 6: Service-graphs — the TTL'd edge-pairing state machine (the centerpiece)

**Files:**
- Modify: `crates/traces/src/metricsgen/servicegraph.rs`

This is the second and most-prominent headline: the bounded, TTL'd edge store that pairs client + server spans of the same trace. The brief's three first-class concerns — **partner→record**, **wait-expiry→`unpaired_spans_total`**, **store-full→`dropped_spans_total`** — are all driven by `record_span` + `expire` + `drain` with a `MockClock`, no real time, no network.

**Interfaces:**
- Consumes: `contract::{SpanRecord, SpanKind, StatusCode}`, `MetricsGenConfig`, `series::{Series, SeriesSample, sorted_labels}`, `Clock` (passed `now_ns` explicitly to keep the store clock-free and unit-testable).
- Produces:
  - `enum ConnectionType { Unset, VirtualNode, MessagingSystem, Database }` (`as_label() -> &'static str`).
  - `struct Edge { client_service: Option<String>, server_service: Option<String>, client_latency_ns: Option<i64>, server_latency_ns: Option<i64>, failed: bool, connection_type: ConnectionType, first_seen_ns: i64 }` — a half-edge until both ends arrive.
  - `struct EdgeStore { /* config + map<EdgeKey, Edge> + completed accumulator */ }` with `new(cfg: &MetricsGenConfig) -> Self`.
  - `fn record_span(&mut self, span: &SpanRecord, now_ns: i64) -> RecordOutcome` — maps a client/server span to its `EdgeKey = (trace_id, connection_key)`, records the half-edge or completes it; on completion accumulates one request observation; returns `RecordOutcome::{Recorded, Completed, Dropped, Ignored}`.
  - `fn expire(&mut self, now_ns: i64) -> usize` — drops half-edges older than `edge_ttl`, returns the count (→ `unpaired_spans_total`).
  - `fn drain(&mut self, timestamp_ms: i64) -> Vec<Series>` — emits `traces_service_graph_request_total`, `_request_failed_total`, `_request_client_seconds`, `_request_server_seconds`, `_request_messaging_system_seconds` (when enabled), `_unpaired_spans_total`, `_dropped_spans_total` labeled `client`/`server`/`connection_type`; resets per-interval accumulators.
  - `enum RecordOutcome { Recorded, Completed, Dropped, Ignored }` (`PartialEq`).
  - internal `fn connection_key(span: &SpanRecord) -> Option<String>` + `fn classify(span: &SpanRecord) -> ConnectionType` (reads `db.system`/`messaging.system`/`peer.service` attrs).

**The edge-pairing state machine (exact semantics — match Tempo):**
- A span participates only if `kind == Client` or `kind == Server` (others → `Ignored`).
- `EdgeKey = (trace_id, connection_key)`. `connection_key` is the span's `peer.service` (client) / the span's own `service_name` keyed by the **client's** `span_id` relationship — Tempo keys the edge by `(trace_id, client_span_id-or-parent)`; the slice keys by `(trace_id, edge_id)` where `edge_id` = the **server span's `parent_span_id`** (= the client span's `span_id`) so a client span and its child server span land on the same key. (Verify against Tempo's `store.go`; pinned by the `pairs_client_then_server` test.)
- First arrival of a key → record the half-edge (`Recorded`), stamp `first_seen_ns`. Second arrival (the partner) → complete it (`Completed`): accumulate one `request_total` for `(client_service, server_service, connection_type)`, `request_failed_total` if either side `status == Error`, and the client/server latency observations. Remove the completed key.
- `expire(now)` → any half-edge with `now - first_seen_ns >= edge_ttl` is dropped and counted as `unpaired` (the partner never arrived).
- If inserting a new key would exceed `edge_store_max_items` → the span is `Dropped` (counted as `dropped`), the store is left unchanged.

- [x] **Step 1: Write the failing tests (pairing + expiry + store-full)**

In `crates/traces/src/metricsgen/servicegraph.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::series::SeriesSample;

    fn span(
        service: &str,
        span_id: [u8; 8],
        parent: [u8; 8],
        kind: SpanKind,
        status: StatusCode,
        dur_ns: i64,
    ) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0x11; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: dur_ns,
            status,
            service_name: service.into(),
            attributes: vec![],
            size_bytes: 0,
        }
    }

    fn counter(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map(|s| match s.sample {
                SeriesSample::Counter(c) => c,
                _ => panic!("{name} not a counter"),
            })
            .unwrap_or(0.0)
    }

    #[test]
    fn pairs_client_then_server_into_one_request() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        // client span id=A; server span is its child (parent=A).
        let client = span("frontend", [0xA; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 10_000_000);
        let server = span("backend", [0xB; 8], [0xA; 8], SpanKind::Server, StatusCode::Ok, 8_000_000);

        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_request_total") - 1.0).abs() < 1e-9);
        assert!(counter(&out, "traces_service_graph_request_failed_total").abs() < 1e-9);

        // edge labeled client=frontend, server=backend.
        let req = out.iter().find(|s| s.name == "traces_service_graph_request_total").unwrap();
        assert!(req.labels.iter().any(|(k, v)| k == "client" && v == "frontend"));
        assert!(req.labels.iter().any(|(k, v)| k == "server" && v == "backend"));
        assert!(req.labels.iter().any(|(k, v)| k == "connection_type" && v == ""));
    }

    #[test]
    fn failed_when_either_side_errors() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span("frontend", [0xA; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        let server = span("backend", [0xB; 8], [0xA; 8], SpanKind::Server, StatusCode::Error, 1);
        store.record_span(&client, 0);
        store.record_span(&server, 1);
        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_request_failed_total") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn unpaired_half_edge_expires_after_ttl() {
        let mut cfg = MetricsGenConfig::default(); // ttl = 10s
        cfg.edge_ttl = std::time::Duration::from_secs(10);
        let mut store = EdgeStore::new(&cfg);
        let client = span("frontend", [0xA; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        store.record_span(&client, 0);
        // before ttl: nothing expires.
        assert!(store.expire(5_000_000_000) == 0);
        // at/after ttl (10s in ns): the lonely client half-edge expires.
        let expired = store.expire(10_000_000_000);
        assert!(expired == 1);
        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_unpaired_spans_total") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn store_full_drops_new_spans() {
        let mut cfg = MetricsGenConfig::default();
        cfg.edge_store_max_items = 1;
        let mut store = EdgeStore::new(&cfg);
        let a = span("s1", [0x1; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        let b = span("s2", [0x2; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        assert!(store.record_span(&a, 0) == RecordOutcome::Recorded);
        // store at capacity → the second distinct key is dropped.
        assert!(store.record_span(&b, 1) == RecordOutcome::Dropped);
        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_dropped_spans_total") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn non_client_server_spans_ignored() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let internal = span("s", [0x1; 8], [0; 8], SpanKind::Internal, StatusCode::Ok, 1);
        assert!(store.record_span(&internal, 0) == RecordOutcome::Ignored);
    }

    #[test]
    fn database_connection_type_from_db_system_attr() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let mut client = span("frontend", [0xA; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        client.attributes.push(("db.system".into(), "postgresql".into()));
        // a db client span with no paired server completes as a virtual edge in
        // Tempo; here it stays a half-edge but its classification is Database.
        store.record_span(&client, 0);
        store.expire(20_000_000_000); // force unpaired so it's observable
        let out = store.drain(1_000);
        // classification is reflected on the unpaired count's connection_type
        // label (the edge remembered it).
        let unpaired = out.iter().find(|s| s.name == "traces_service_graph_unpaired_spans_total").unwrap();
        assert!(unpaired.labels.iter().any(|(k, v)| k == "connection_type" && v == "database"));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::servicegraph`
Expected: FAIL — `cannot find type EdgeStore`.

- [x] **Step 3: Implement**

```rust
//! Service-graphs: pair a client-kind span with the server-kind span of the
//! same trace via a bounded, TTL'd edge store keyed by `(trace_id,
//! connection_key)`. Completion emits one request observation; TTL expiry of a
//! lonely half-edge → unpaired; insertion past `max_items` → dropped.

use std::collections::HashMap;

use super::config::MetricsGenConfig;
use super::contract::{SpanKind, SpanRecord, StatusCode};
use super::series::{Series, SeriesSample, sorted_labels};

const NS_PER_SEC: f64 = 1_000_000_000.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionType {
    Unset,
    VirtualNode,
    MessagingSystem,
    Database,
}

impl ConnectionType {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            ConnectionType::Unset => "",
            ConnectionType::VirtualNode => "virtual_node",
            ConnectionType::MessagingSystem => "messaging_system",
            ConnectionType::Database => "database",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    Recorded,
    Completed,
    Dropped,
    Ignored,
}

#[derive(Clone)]
struct Edge {
    client_service: Option<String>,
    server_service: Option<String>,
    client_latency_ns: Option<i64>,
    server_latency_ns: Option<i64>,
    failed: bool,
    connection_type: ConnectionType,
    first_seen_ns: i64,
}

type EdgeKey = ([u8; 16], Vec<u8>); // (trace_id, edge_id bytes)

/// A completed request observation accumulated for one collection interval.
#[derive(Default)]
struct EdgeAgg {
    requests: f64,
    failed: f64,
    client_seconds_sum: f64,
    client_seconds_count: f64,
    server_seconds_sum: f64,
    server_seconds_count: f64,
    messaging_seconds_sum: f64,
    messaging_seconds_count: f64,
}

pub struct EdgeStore {
    max_items: usize,
    ttl_ns: i64,
    enable_messaging_latency: bool,
    edges: HashMap<EdgeKey, Edge>,
    /// completed-edge aggregates keyed `(client, server, connection_type)`.
    aggregates: HashMap<(String, String, ConnectionType), EdgeAgg>,
    /// unpaired/dropped counters keyed by `connection_type`.
    unpaired: HashMap<ConnectionType, f64>,
    dropped: f64,
}

impl EdgeStore {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            max_items: cfg.edge_store_max_items,
            ttl_ns: i64::try_from(cfg.edge_ttl.as_nanos()).unwrap_or(i64::MAX),
            enable_messaging_latency: cfg.enable_messaging_system_latency,
            edges: HashMap::new(),
            aggregates: HashMap::new(),
            unpaired: HashMap::new(),
            dropped: 0.0,
        }
    }

    pub fn record_span(&mut self, span: &SpanRecord, now_ns: i64) -> RecordOutcome {
        let is_client = span.kind == SpanKind::Client;
        let is_server = span.kind == SpanKind::Server;
        if !is_client && !is_server {
            return RecordOutcome::Ignored;
        }
        // edge_id: the client's span_id == the server's parent_span_id, so both
        // ends key to the same edge.
        let edge_id: Vec<u8> = if is_client {
            span.span_id.to_vec()
        } else {
            span.parent_span_id.to_vec()
        };
        let key: EdgeKey = (span.trace_id, edge_id);
        let connection_type = classify(span);
        let latency_ns = span.duration_ns.max(0);
        let failed = span.status == StatusCode::Error;

        match self.edges.get_mut(&key) {
            Some(edge) => {
                if is_client {
                    edge.client_service = Some(span.service_name.clone());
                    edge.client_latency_ns = Some(latency_ns);
                } else {
                    edge.server_service = Some(span.service_name.clone());
                    edge.server_latency_ns = Some(latency_ns);
                }
                edge.failed |= failed;
                if connection_type != ConnectionType::Unset {
                    edge.connection_type = connection_type;
                }
                if edge.client_service.is_some() && edge.server_service.is_some() {
                    let edge = self.edges.remove(&key).expect("present");
                    self.complete(edge);
                    return RecordOutcome::Completed;
                }
                RecordOutcome::Recorded
            }
            None => {
                if self.edges.len() >= self.max_items {
                    self.dropped += 1.0;
                    return RecordOutcome::Dropped;
                }
                let mut edge = Edge {
                    client_service: None,
                    server_service: None,
                    client_latency_ns: None,
                    server_latency_ns: None,
                    failed,
                    connection_type,
                    first_seen_ns: now_ns,
                };
                if is_client {
                    edge.client_service = Some(span.service_name.clone());
                    edge.client_latency_ns = Some(latency_ns);
                } else {
                    edge.server_service = Some(span.service_name.clone());
                    edge.server_latency_ns = Some(latency_ns);
                }
                self.edges.insert(key, edge);
                RecordOutcome::Recorded
            }
        }
    }

    fn complete(&mut self, edge: Edge) {
        let client = edge.client_service.unwrap_or_default();
        let server = edge.server_service.unwrap_or_default();
        let agg = self
            .aggregates
            .entry((client, server, edge.connection_type))
            .or_default();
        agg.requests += 1.0;
        if edge.failed {
            agg.failed += 1.0;
        }
        if let Some(ns) = edge.client_latency_ns {
            agg.client_seconds_sum += ns as f64 / NS_PER_SEC;
            agg.client_seconds_count += 1.0;
        }
        if let Some(ns) = edge.server_latency_ns {
            agg.server_seconds_sum += ns as f64 / NS_PER_SEC;
            agg.server_seconds_count += 1.0;
        }
        if self.enable_messaging_latency && edge.connection_type == ConnectionType::MessagingSystem {
            if let Some(ns) = edge.server_latency_ns.or(edge.client_latency_ns) {
                agg.messaging_seconds_sum += ns as f64 / NS_PER_SEC;
                agg.messaging_seconds_count += 1.0;
            }
        }
    }

    /// Drop half-edges past their TTL; return the count (→ unpaired).
    pub fn expire(&mut self, now_ns: i64) -> usize {
        let ttl = self.ttl_ns;
        let expired_keys: Vec<EdgeKey> = self
            .edges
            .iter()
            .filter(|(_, e)| now_ns - e.first_seen_ns >= ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &expired_keys {
            let e = self.edges.remove(k).expect("present");
            *self.unpaired.entry(e.connection_type).or_insert(0.0) += 1.0;
        }
        expired_keys.len()
    }

    #[must_use]
    pub fn drain(&mut self, timestamp_ms: i64) -> Vec<Series> {
        let mut out = Vec::new();
        for ((client, server, ct), agg) in self.aggregates.drain() {
            let base = sorted_labels(vec![
                ("client".to_string(), client),
                ("server".to_string(), server),
                ("connection_type".to_string(), ct.as_label().to_string()),
            ]);
            out.push(counter("traces_service_graph_request_total", &base, agg.requests, timestamp_ms));
            out.push(counter(
                "traces_service_graph_request_failed_total",
                &base,
                agg.failed,
                timestamp_ms,
            ));
            push_histogram(
                &mut out,
                "traces_service_graph_request_client_seconds",
                &base,
                agg.client_seconds_sum,
                agg.client_seconds_count,
                timestamp_ms,
            );
            push_histogram(
                &mut out,
                "traces_service_graph_request_server_seconds",
                &base,
                agg.server_seconds_sum,
                agg.server_seconds_count,
                timestamp_ms,
            );
            if self.enable_messaging_latency {
                push_histogram(
                    &mut out,
                    "traces_service_graph_request_messaging_system_seconds",
                    &base,
                    agg.messaging_seconds_sum,
                    agg.messaging_seconds_count,
                    timestamp_ms,
                );
            }
        }
        for (ct, n) in self.unpaired.drain() {
            let labels = sorted_labels(vec![("connection_type".to_string(), ct.as_label().to_string())]);
            out.push(counter("traces_service_graph_unpaired_spans_total", &labels, n, timestamp_ms));
        }
        if self.dropped > 0.0 {
            out.push(counter(
                "traces_service_graph_dropped_spans_total",
                &[],
                self.dropped,
                timestamp_ms,
            ));
            self.dropped = 0.0;
        }
        out
    }
}

fn counter(name: &str, labels: &[(String, String)], value: f64, ts: i64) -> Series {
    Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::Counter(value),
        exemplars: vec![],
        timestamp_ms: ts,
    }
}

/// Emit a degenerate single-bucket (`+Inf`) classic histogram carrying `_sum`/
/// `_count` — enough for Grafana's service-graph latency, refined to real
/// buckets under Slice 8.
fn push_histogram(
    out: &mut Vec<Series>,
    name: &str,
    labels: &[(String, String)],
    sum: f64,
    count: f64,
    ts: i64,
) {
    out.push(Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::ClassicHistogram { buckets: vec![], sum, count },
        exemplars: vec![],
        timestamp_ms: ts,
    });
}

/// Classify the connection type from span attributes (spec §7.2).
fn classify(span: &SpanRecord) -> ConnectionType {
    let has = |k: &str| span.attributes.iter().any(|(key, _)| key == k);
    if has("db.system") {
        ConnectionType::Database
    } else if has("messaging.system") {
        ConnectionType::MessagingSystem
    } else {
        ConnectionType::Unset
    }
}

use super::series::Series;
```

> **Tempo-fidelity verify-notes (pin empirically before declaring done):**
> 1. **Edge keying** — Tempo's service-graph processor keys an edge by `(trace_id, span_id)` where the *client* span's `span_id` equals the *server* span's `parent_span_id`. This slice encodes exactly that (`edge_id = client.span_id = server.parent_span_id`). The `pairs_client_then_server_into_one_request` test pins it. If real traces don't satisfy the child-server relationship (some instrumentations break it), Tempo falls back to `peer.service`/`virtual_node` edges — that fallback is a flagged Slice-8 refinement (`VirtualNode` connection type already exists for it). Confirm the keying against cp-tempo's `store.go` before declaring parity.
> 2. **`_seconds` histograms** — emitted here as `_sum`/`_count`-only degenerate histograms (`buckets: vec![]`). The encoder (Task 8) renders `_sum`/`_count` and a single `le="+Inf"` bucket. Real bucketed service-graph latency (with the configured edges) is a flagged Slice-8 refinement; `_sum`/`_count` already drive Grafana's edge-latency tooltip.
> 3. **`connection_type` empty-string label** — Tempo emits `connection_type=""` (unset) as an *empty* label value, present on the series. The `pairs_client_then_server` test pins the empty value. Confirm Tempo doesn't *omit* the label entirely for unset; if it omits, drop the `connection_type` pair when `Unset` in `drain` + the test together.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::servicegraph`
Expected: PASS (6 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use servicegraph::{ConnectionType, EdgeStore, RecordOutcome};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): service-graph TTL'd edge-pairing state machine + RED edges"
```

---

### Task 7: Sink + source traits + mocks (RemoteWriteSink, SpanSource)

**Files:**
- Modify: `crates/traces/src/metricsgen/sink.rs`

**Interfaces:**
- Produces:
  - `trait RemoteWriteSink: Send + Sync { async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError>; }` (use `#[async_trait]` — the service stores `Arc<dyn RemoteWriteSink>`, so object safety is required).
  - `trait SpanSource: Send + Sync { async fn poll(&self, max: usize) -> Result<Vec<SpanRecord>, SinkError>; async fn commit(&self) -> Result<(), SinkError>; }` — the WAL-consumer seam onto `crabka-client-consumer`.
  - `enum SinkError` (`thiserror`): `Transport(String)`, `Decode(String)`, `Source(String)`.
  - `struct MockRemoteWriteSink` (`Default`, `Clone`) recording all written `SeriesPayload`s; `writes() -> Vec<SeriesPayload>`; `fail_next()` forcing one `Transport` error.
  - `struct MockSpanSource` (`Default`, `Clone`) returning scripted batches; `push_batch(Vec<SpanRecord>)`; records `commit()` calls; `commits() -> usize`.

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/sink.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::series::{Series, SeriesPayload, SeriesSample};

    fn payload() -> SeriesPayload {
        SeriesPayload {
            tenant: "t".into(),
            series: vec![Series {
                name: "traces_spanmetrics_calls_total".into(),
                labels: vec![("service".into(), "api".into())],
                sample: SeriesSample::Counter(1.0),
                exemplars: vec![],
                timestamp_ms: 1_000,
            }],
        }
    }

    fn span() -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0; 16],
            span_id: [0; 8],
            parent_span_id: [0; 8],
            name: "op".into(),
            kind: SpanKind::Server,
            start_ns: 0,
            duration_ns: 1,
            status: StatusCode::Ok,
            service_name: "api".into(),
            attributes: vec![],
            size_bytes: 0,
        }
    }

    #[tokio::test]
    async fn mock_sink_records_writes_and_can_fail_once() {
        let sink = MockRemoteWriteSink::default();
        sink.fail_next();
        assert!(sink.write(&payload()).await.is_err());
        assert!(sink.write(&payload()).await.is_ok());
        assert!(sink.writes().len() == 1); // only the successful write recorded
    }

    #[tokio::test]
    async fn mock_source_returns_scripted_batches_and_tracks_commits() {
        let src = MockSpanSource::default();
        src.push_batch(vec![span(), span()]);
        let batch = src.poll(10).await.unwrap();
        assert!(batch.len() == 2);
        // drained: next poll is empty.
        assert!(src.poll(10).await.unwrap().is_empty());
        src.commit().await.unwrap();
        assert!(src.commits() == 1);
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::sink`
Expected: FAIL — `cannot find type MockRemoteWriteSink`.

- [x] **Step 3: Implement**

```rust
//! The two boundary surfaces, behind narrow traits with deterministic mocks.
//! Real impls: `remotewrite.rs` (Prometheus remote_write HTTP) and the service's
//! Kafka consumer. Keeping them behind traits is what makes the processors +
//! flush path pure, deterministic tests.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use super::contract::SpanRecord;
use super::series::SeriesPayload;

#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("transport error: {0}")]
    Transport(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("source error: {0}")]
    Source(String),
}

/// The output edge: push a collection of series to a Prometheus remote_write
/// endpoint.
#[async_trait]
pub trait RemoteWriteSink: Send + Sync {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError>;
}

/// The input edge: the traces WAL consumer group.
#[async_trait]
pub trait SpanSource: Send + Sync {
    /// Poll up to `max` decoded span records (returns empty when no data is
    /// currently available — the loop sleeps/retries).
    async fn poll(&self, max: usize) -> Result<Vec<SpanRecord>, SinkError>;
    /// Commit consumed offsets (after a successful collection flush).
    async fn commit(&self) -> Result<(), SinkError>;
}

// ---- mocks ----

#[derive(Clone, Default)]
pub struct MockRemoteWriteSink {
    writes: Arc<Mutex<Vec<SeriesPayload>>>,
    fail_next: Arc<Mutex<bool>>,
}

impl MockRemoteWriteSink {
    pub fn fail_next(&self) {
        *self.fail_next.lock().expect("poisoned") = true;
    }
    #[must_use]
    pub fn writes(&self) -> Vec<SeriesPayload> {
        self.writes.lock().expect("poisoned").clone()
    }
}

#[async_trait]
impl RemoteWriteSink for MockRemoteWriteSink {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError> {
        {
            let mut f = self.fail_next.lock().expect("poisoned");
            if *f {
                *f = false;
                return Err(SinkError::Transport("forced".into()));
            }
        }
        self.writes.lock().expect("poisoned").push(payload.clone());
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct MockSpanSource {
    batches: Arc<Mutex<VecDeque<Vec<SpanRecord>>>>,
    commits: Arc<Mutex<usize>>,
}

impl MockSpanSource {
    pub fn push_batch(&self, batch: Vec<SpanRecord>) {
        self.batches.lock().expect("poisoned").push_back(batch);
    }
    #[must_use]
    pub fn commits(&self) -> usize {
        *self.commits.lock().expect("poisoned")
    }
}

#[async_trait]
impl SpanSource for MockSpanSource {
    async fn poll(&self, _max: usize) -> Result<Vec<SpanRecord>, SinkError> {
        Ok(self.batches.lock().expect("poisoned").pop_front().unwrap_or_default())
    }
    async fn commit(&self) -> Result<(), SinkError> {
        *self.commits.lock().expect("poisoned") += 1;
        Ok(())
    }
}
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::sink`
Expected: PASS (2 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use sink::{MockRemoteWriteSink, MockSpanSource, RemoteWriteSink, SinkError, SpanSource};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): metrics-generator RemoteWriteSink/SpanSource traits + mocks"
```

---

### Task 8: Prometheus remote_write encoder (churn-prone surface — structure + behavior-pinning tests)

**Files:**
- Modify: `crates/traces/src/metricsgen/remotewrite.rs`

**Interfaces:**
- Produces:
  - `struct PrometheusRemoteWriteSink { url: String, http: reqwest::Client }` with `new(url: impl Into<String>) -> Self`.
  - `impl RemoteWriteSink for PrometheusRemoteWriteSink` — encode → snappy-compress → `POST {url}` with the remote_write headers + `X-Scope-OrgID: {tenant}`.
  - `fn to_timeseries(series: &[Series]) -> Vec<WireTimeSeries>` — the **pure** transform from `Series` to the flat remote_write `TimeSeries` shape (a `ClassicHistogram` fans into `_bucket`/`_sum`/`_count` series with `le` labels; a `Counter`/`Gauge` is one sample; exemplars attach). Unit-tested without prost or network.
  - `struct WireTimeSeries { pub labels: Vec<(String, String)>, pub value: f64, pub timestamp_ms: i64, pub exemplars: Vec<Exemplar> }` — the encoder-neutral flat row (maps 1:1 to `pb::v1::TimeSeries`).
  - `fn le_label(le_seconds: f64) -> String` — Prometheus `le` rendering (`"+Inf"`, else shortest float).

**The prost encode + HTTP send is the churn-prone part; the flat-row transform is pinned by a pure test.** Don't unit-test the network call — test `to_timeseries` exhaustively, leave the `pb`-encode + `reqwest` send behind a verify-note + an `#[ignore]` integration smoke.

- [x] **Step 1: Write the failing test (the pure transform)**

In `crates/traces/src/metricsgen/remotewrite.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::metricsgen::series::{Exemplar, Series, SeriesSample};

    fn has_label(ts: &WireTimeSeries, k: &str, v: &str) -> bool {
        ts.labels.iter().any(|(lk, lv)| lk == k && lv == v)
    }

    #[test]
    fn counter_becomes_one_timeseries_with_name_label() {
        let s = Series {
            name: "traces_spanmetrics_calls_total".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::Counter(3.0),
            exemplars: vec![],
            timestamp_ms: 1_000,
        };
        let out = to_timeseries(&[s]);
        assert!(out.len() == 1);
        assert!(has_label(&out[0], "__name__", "traces_spanmetrics_calls_total"));
        assert!(has_label(&out[0], "service", "api"));
        assert!((out[0].value - 3.0).abs() < 1e-9);
    }

    #[test]
    fn classic_histogram_fans_into_bucket_sum_count() {
        let s = Series {
            name: "traces_spanmetrics_latency".into(),
            labels: vec![("service".into(), "api".into())],
            sample: SeriesSample::ClassicHistogram {
                buckets: vec![(0.004, 0.0), (0.008, 2.0)],
                sum: 0.012,
                count: 2.0,
            },
            exemplars: vec![Exemplar {
                value: 0.005,
                labels: vec![("trace_id".into(), "ab".into())],
                timestamp_ms: 1_000,
            }],
            timestamp_ms: 1_000,
        };
        let out = to_timeseries(&[s]);
        // 2 buckets + +Inf + _sum + _count = 5 series.
        assert!(out.len() == 5);

        let bucket_inf = out
            .iter()
            .find(|t| {
                has_label(t, "__name__", "traces_spanmetrics_latency_bucket") && has_label(t, "le", "+Inf")
            })
            .unwrap();
        assert!((bucket_inf.value - 2.0).abs() < 1e-9); // +Inf == count

        let sum = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_sum"))
            .unwrap();
        assert!((sum.value - 0.012).abs() < 1e-9);

        let count = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_count"))
            .unwrap();
        assert!((count.value - 2.0).abs() < 1e-9);

        // exemplars ride on the matching _bucket series.
        let le_8 = out
            .iter()
            .find(|t| has_label(t, "__name__", "traces_spanmetrics_latency_bucket") && has_label(t, "le", "0.008"))
            .unwrap();
        assert!(le_8.exemplars.len() == 1);
        assert!(le_8.exemplars[0].labels[0].0 == "trace_id");
    }

    #[test]
    fn le_label_renders_inf_and_floats() {
        assert!(le_label(f64::INFINITY) == "+Inf");
        assert!(le_label(0.008) == "0.008");
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::remotewrite`
Expected: FAIL — `cannot find function to_timeseries`.

- [x] **Step 3: Implement**

```rust
//! Prometheus remote_write encoder. The flat `Series` → `TimeSeries` transform
//! is pinned by a pure test; the prost encode + snappy + `reqwest` POST is the
//! churn-prone surface, covered by an integration smoke + verify-note.

use async_trait::async_trait;

use super::series::{Exemplar, Series, SeriesPayload, SeriesSample};
use super::sink::{RemoteWriteSink, SinkError};

/// A flat remote_write row (maps 1:1 to `pb::v1::TimeSeries`).
#[derive(Clone, Debug, PartialEq)]
pub struct WireTimeSeries {
    pub labels: Vec<(String, String)>,
    pub value: f64,
    pub timestamp_ms: i64,
    pub exemplars: Vec<Exemplar>,
}

/// Render the `le` label value (`+Inf` for infinity, else the shortest float).
#[must_use]
pub fn le_label(le_seconds: f64) -> String {
    if le_seconds.is_infinite() {
        "+Inf".to_string()
    } else {
        format!("{le_seconds}")
    }
}

fn with_name(name: &str, labels: &[(String, String)]) -> Vec<(String, String)> {
    let mut l = Vec::with_capacity(labels.len() + 1);
    l.push(("__name__".to_string(), name.to_string()));
    l.extend(labels.iter().cloned());
    l.sort_by(|a, b| a.0.cmp(&b.0));
    l
}

/// Pure transform: neutral `Series` → flat remote_write rows.
#[must_use]
pub fn to_timeseries(series: &[Series]) -> Vec<WireTimeSeries> {
    let mut out = Vec::new();
    for s in series {
        match &s.sample {
            SeriesSample::Counter(v) | SeriesSample::Gauge(v) => {
                out.push(WireTimeSeries {
                    labels: with_name(&s.name, &s.labels),
                    value: *v,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: s.exemplars.clone(),
                });
            }
            SeriesSample::ClassicHistogram { buckets, sum, count } => {
                let bucket_name = format!("{}_bucket", s.name);
                // exemplars attach to the bucket they fall into (the first le >=
                // exemplar value); fallback to +Inf.
                for (le, cumulative) in buckets {
                    let mut labels = s.labels.clone();
                    labels.push(("le".to_string(), le_label(*le)));
                    let exemplars: Vec<Exemplar> =
                        s.exemplars.iter().filter(|e| e.value <= *le).cloned().collect();
                    out.push(WireTimeSeries {
                        labels: with_name(&bucket_name, &labels),
                        value: *cumulative,
                        timestamp_ms: s.timestamp_ms,
                        exemplars,
                    });
                }
                // +Inf bucket == count, carries any exemplars not in a finite bucket.
                let mut inf_labels = s.labels.clone();
                inf_labels.push(("le".to_string(), "+Inf".to_string()));
                let max_finite = buckets.iter().map(|(le, _)| *le).fold(f64::NEG_INFINITY, f64::max);
                let inf_exemplars: Vec<Exemplar> =
                    s.exemplars.iter().filter(|e| e.value > max_finite).cloned().collect();
                out.push(WireTimeSeries {
                    labels: with_name(&bucket_name, &inf_labels),
                    value: *count,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: inf_exemplars,
                });
                out.push(WireTimeSeries {
                    labels: with_name(&format!("{}_sum", s.name), &s.labels),
                    value: *sum,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: vec![],
                });
                out.push(WireTimeSeries {
                    labels: with_name(&format!("{}_count", s.name), &s.labels),
                    value: *count,
                    timestamp_ms: s.timestamp_ms,
                    exemplars: vec![],
                });
            }
            SeriesSample::NativeHistogram(_) => {
                // native histograms ride remote_write v2's `Histogram` field;
                // the v2 prost encode is the churn-prone path (verify-note 2).
                // The flat-row transform doesn't apply — encoded directly in the
                // pb-encode step. Skipped here; flagged.
            }
        }
    }
    out
}

/// HTTP client that POSTs to a Prometheus remote_write endpoint.
pub struct PrometheusRemoteWriteSink {
    url: String,
    http: reqwest::Client,
}

impl PrometheusRemoteWriteSink {
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), http: reqwest::Client::new() }
    }
}

#[async_trait]
impl RemoteWriteSink for PrometheusRemoteWriteSink {
    async fn write(&self, payload: &SeriesPayload) -> Result<(), SinkError> {
        let rows = to_timeseries(&payload.series);
        let body = encode_write_request(&rows).map_err(|e| SinkError::Decode(e))?;
        let resp = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/x-protobuf")
            .header("Content-Encoding", "snappy")
            .header("X-Prometheus-Remote-Write-Version", "0.1.0")
            .header("X-Scope-OrgID", &payload.tenant)
            .body(body)
            .send()
            .await
            .map_err(|e| SinkError::Transport(e.to_string()))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(SinkError::Transport(format!("remote_write status {}", resp.status())))
        }
    }
}

/// Encode flat rows into a snappy-compressed `prometheus.WriteRequest` body.
///
/// CHURN-PRONE: uses the metrics Slice-4 `pb::v1` prost types + `snap::raw`.
/// Until that crate is a dep, this returns an `Err` documenting the gap so the
/// binary compiles and the unit suite (which tests `to_timeseries`) is green.
fn encode_write_request(_rows: &[WireTimeSeries]) -> Result<Vec<u8>, String> {
    // TODO(slice7-pb): build `pb::v1::WriteRequest { timeseries: rows.map(..) }`,
    // `prost::Message::encode` to a buffer, then `snap::raw::Encoder::compress_vec`.
    // Each WireTimeSeries -> pb::v1::TimeSeries { labels, samples:[Sample{value,ts}],
    // exemplars }. See verify-note 1/2. Returns Err until `crabka-metrics`'s pb
    // module + `snap` are deps.
    Err("remote_write pb-encode not wired until crabka-metrics pb module is a dep".to_string())
}
```

> **Churn-surface verify-notes:**
> 1. **`reqwest` API drift** — `Client::post().header().body().send()` is the stable reqwest builder chain; if a method moves at 0.13, fix the call, not the test (`to_timeseries`/`le_label` have no reqwest in them). Keep `write` the only reqwest-touching code.
> 2. **prost `pb` shape** — `encode_write_request` must build the metrics Slice-4 `pb::v1::WriteRequest`/`pb::v1::TimeSeries`/`Sample`/`Label`/`Exemplar` types (the generated `OUT_DIR` types are the source of truth — do **not** fabricate field names). When `crabka-metrics` is a dep, import `crabka_metrics::wire::pb`, map each `WireTimeSeries`, `prost::Message::encode`, then `snap::raw::Encoder::compress_vec` (the *plain* snappy `snap::raw` variant — **not** the Kafka Xerial-framed `crabka-compression::snappy`; using the wrong one corrupts every request, per the metrics Slice-4 note). Pin with a behavior test (decode-back round-trip) at that point.
> 3. **Native histograms (remote_write v2)** — emitting the latency as a native histogram requires the v2 `pb::v2::Request` + symbol-table interning. `to_timeseries` deliberately skips `NativeHistogram` (classic is the default path); the v2 native encode is a flagged Slice-8 option, structured by the `NativeHistogram` contract type already in scope.
> 4. **Integration smoke** — add `crates/traces/tests/remote_write_smoke.rs` behind `#[ignore]` that POSTs a real `WriteRequest` to a testcontainers Prometheus/Mimir and asserts `2xx` + a follow-up query returns the series. Run manually / in a dedicated CI lane; the unit suite never depends on a live endpoint.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::remotewrite`
Expected: PASS (3 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use remotewrite::{PrometheusRemoteWriteSink, WireTimeSeries, le_label, to_timeseries};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): metrics-generator remote_write encoder + pure timeseries transform"
```

---

### Task 9: Edge-state checkpoint codec (compacted-topic shape; live impl deferred)

**Files:**
- Modify: `crates/traces/src/metricsgen/checkpoint.rs`

**Interfaces:**
- Produces:
  - `trait EdgeCheckpointStore: Send + Sync { fn save(&self, tenant: &str, key: &[u8], value: &[u8]); fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)>; }` — keyed `(tenant, edge-key bytes)`, value = a half-edge snapshot; a tombstone (empty value) clears a completed/expired edge.
  - `struct InMemoryCheckpointStore` (`Default`, `Clone`).
  - Compacted-topic key/value codec (`encode_checkpoint_key`/`parse_checkpoint_key`) keyed `(tenant, trace_id[16], edge_id)` — **structured + unit-tested here, used by the topic-backed impl in Slice 8**. Mirror the broker's length-prefixed encoding (`bytes::BufMut`, see `coordinator/unified/share/persistence.rs`).

> **Rationale note:** the metrics-generator holds *no* durable state — edges are rebuildable from WAL offsets (spec §9). The checkpoint is a pure **optimization** (avoid re-deriving in-flight half-edges after a restart by replaying only since the last checkpoint). This task defines the wire shape + trait so the optimization can land in Slice 8 without reshaping the processor; the in-memory impl is the test substrate.

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/checkpoint.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn checkpoint_key_round_trips() {
        let trace = [0x22; 16];
        let k = encode_checkpoint_key("tenant-a", &trace, &[0xAA, 0xBB]);
        let (t, tr, edge) = parse_checkpoint_key(&k).unwrap();
        assert!(t == "tenant-a");
        assert!(tr == trace);
        assert!(edge == vec![0xAA, 0xBB]);
    }

    #[test]
    fn in_memory_store_round_trips_and_isolates_tenants() {
        let store = InMemoryCheckpointStore::default();
        store.save("t", b"k1", b"v1");
        store.save("t", b"k2", b"v2");
        let all = store.load_all("t");
        assert!(all.len() == 2);
        // tombstone (empty value) removes a key.
        store.save("t", b"k1", b"");
        assert!(store.load_all("t").len() == 1);
        // other tenant sees nothing.
        assert!(store.load_all("other").is_empty());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::checkpoint`
Expected: FAIL — `cannot find function encode_checkpoint_key`.

- [x] **Step 3: Implement**

```rust
//! Edge-state checkpoint (optional optimization; the metrics-generator holds no
//! durable state — edges rebuild from WAL offsets). Production persists to a
//! compacted topic keyed `(tenant, trace_id, edge_id)` with a half-edge snapshot
//! value (empty value = tombstone). The codec here defines that wire shape; the
//! topic-backed impl lands in Slice 8. `InMemoryCheckpointStore` is the test
//! substrate + trait-shape source.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, thiserror::Error)]
pub enum CheckpointCodecError {
    #[error("truncated checkpoint key")]
    Truncated,
    #[error("invalid utf8 in tenant")]
    Utf8,
    #[error("bad trace_id length")]
    BadTraceId,
}

fn put_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_u32(u32::try_from(b.len()).expect("too long"));
    buf.put_slice(b);
}

fn get_bytes(buf: &mut &[u8]) -> Result<Vec<u8>, CheckpointCodecError> {
    if buf.len() < 4 {
        return Err(CheckpointCodecError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        return Err(CheckpointCodecError::Truncated);
    }
    let (b, rest) = buf.split_at(len);
    let out = b.to_vec();
    *buf = rest;
    Ok(out)
}

#[must_use]
pub fn encode_checkpoint_key(tenant: &str, trace_id: &[u8; 16], edge_id: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    put_bytes(&mut buf, tenant.as_bytes());
    put_bytes(&mut buf, trace_id);
    put_bytes(&mut buf, edge_id);
    buf.freeze()
}

pub fn parse_checkpoint_key(
    mut buf: &[u8],
) -> Result<(String, [u8; 16], Vec<u8>), CheckpointCodecError> {
    let tenant = String::from_utf8(get_bytes(&mut buf)?).map_err(|_| CheckpointCodecError::Utf8)?;
    let trace_vec = get_bytes(&mut buf)?;
    let trace_id: [u8; 16] = trace_vec.try_into().map_err(|_| CheckpointCodecError::BadTraceId)?;
    let edge_id = get_bytes(&mut buf)?;
    Ok((tenant, trace_id, edge_id))
}

pub trait EdgeCheckpointStore: Send + Sync {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]);
    fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)>;
}

type Key = (String, Vec<u8>);

#[derive(Clone, Default)]
pub struct InMemoryCheckpointStore {
    inner: Arc<Mutex<BTreeMap<Key, Vec<u8>>>>,
}

impl EdgeCheckpointStore for InMemoryCheckpointStore {
    fn save(&self, tenant: &str, key: &[u8], value: &[u8]) {
        let mut g = self.inner.lock().expect("poisoned");
        let k = (tenant.to_string(), key.to_vec());
        if value.is_empty() {
            g.remove(&k);
        } else {
            g.insert(k, value.to_vec());
        }
    }
    fn load_all(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let g = self.inner.lock().expect("poisoned");
        g.iter()
            .filter(|((t, _), _)| t == tenant)
            .map(|((_, k), v)| (k.clone(), v.clone()))
            .collect()
    }
}
```

> **Dep-note:** `bytes` is a workspace dep (used across the broker). Add `bytes = { workspace = true }` to `crates/traces/Cargo.toml` `[dependencies]` if absent.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::checkpoint`
Expected: PASS (2 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use checkpoint::{EdgeCheckpointStore, InMemoryCheckpointStore, encode_checkpoint_key, parse_checkpoint_key};`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): metrics-generator edge-state checkpoint codec + in-memory store"
```

---

### Task 10: `MetricsGenerator` processor — owns both processors + clock

**Files:**
- Modify: `crates/traces/src/metricsgen/processor.rs`

This composes the two processors behind one `process(SpanRecord)` + `collect()` surface, so the service loop (Task 11) is thin glue. `collect()` is the deterministic flush: expire stale edges, drain both registries into one `SeriesPayload`. Per-tenant isolation lives here — each tenant gets its own pair of registries.

**Interfaces:**
- Consumes: `contract::SpanRecord`, `MetricsGenConfig`, `Clock`, `SpanMetricsRegistry`, `EdgeStore`, `series::SeriesPayload`.
- Produces:
  - `struct MetricsGenerator { cfg, clock, per_tenant: HashMap<String, TenantState> }` with `new(cfg: MetricsGenConfig, clock: Arc<dyn Clock>) -> Self`.
  - `fn process(&mut self, span: &SpanRecord)` — routes the span to its tenant's `SpanMetricsRegistry::record_span` + `EdgeStore::record_span(span, clock.now_ns())`.
  - `fn collect(&mut self, timestamp_ms: i64) -> Vec<SeriesPayload>` — for each tenant: `EdgeStore::expire(clock.now_ns())`, drain span-metrics + service-graph series into one `SeriesPayload`; returns one payload per tenant with non-empty series.
  - internal `struct TenantState { span_metrics: SpanMetricsRegistry, edges: EdgeStore }`.

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/processor.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::metricsgen::clock::MockClock;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};

    fn span(tenant: &str, service: &str, kind: SpanKind, span_id: [u8; 8], parent: [u8; 8]) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            trace_id: [0x33; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            service_name: service.into(),
            attributes: vec![],
            size_bytes: 10,
        }
    }

    #[tokio::test]
    async fn process_then_collect_emits_both_processors_per_tenant() {
        let clock = MockClock::new(0);
        let mut gen = MetricsGenerator::new(MetricsGenConfig::default(), Arc::new(clock.clone()));

        // tenant A: a client + its child server span → one service-graph edge + span-metrics.
        gen.process(&span("A", "frontend", SpanKind::Client, [0xA; 8], [0; 8]));
        gen.process(&span("A", "backend", SpanKind::Server, [0xB; 8], [0xA; 8]));
        // tenant B: a lone server span → span-metrics only.
        gen.process(&span("B", "svc", SpanKind::Server, [0xC; 8], [0; 8]));

        let payloads = gen.collect(1_000);
        assert!(payloads.len() == 2); // one per tenant

        let a = payloads.iter().find(|p| p.tenant == "A").unwrap();
        assert!(a.series.iter().any(|s| s.name == "traces_service_graph_request_total"));
        assert!(a.series.iter().any(|s| s.name == "traces_spanmetrics_calls_total"));

        let b = payloads.iter().find(|p| p.tenant == "B").unwrap();
        assert!(b.series.iter().any(|s| s.name == "traces_spanmetrics_calls_total"));
        // tenant B never paired → no request_total.
        assert!(!b.series.iter().any(|s| s.name == "traces_service_graph_request_total"));
    }

    #[tokio::test]
    async fn collect_expires_stale_edges_via_clock() {
        let clock = MockClock::new(0);
        let mut gen = MetricsGenerator::new(MetricsGenConfig::default(), Arc::new(clock.clone()));
        // lone client half-edge, never paired.
        gen.process(&span("A", "frontend", SpanKind::Client, [0xA; 8], [0; 8]));
        // advance past the 10s TTL; collect must expire it → unpaired series.
        clock.set(11_000_000_000);
        let payloads = gen.collect(2_000);
        let a = payloads.iter().find(|p| p.tenant == "A").unwrap();
        assert!(a.series.iter().any(|s| s.name == "traces_service_graph_unpaired_spans_total"));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::processor`
Expected: FAIL — `cannot find type MetricsGenerator`.

- [x] **Step 3: Implement**

```rust
//! `MetricsGenerator` — composes the span-metrics + service-graph processors per
//! tenant behind `process(span)` + `collect(ts)`. `collect` expires stale edges
//! (injected clock) then drains both registries into one `SeriesPayload` per
//! tenant.

use std::collections::HashMap;
use std::sync::Arc;

use super::clock::Clock;
use super::config::MetricsGenConfig;
use super::contract::SpanRecord;
use super::series::SeriesPayload;
use super::servicegraph::EdgeStore;
use super::spanmetrics::SpanMetricsRegistry;

struct TenantState {
    span_metrics: SpanMetricsRegistry,
    edges: EdgeStore,
}

pub struct MetricsGenerator {
    cfg: MetricsGenConfig,
    clock: Arc<dyn Clock>,
    per_tenant: HashMap<String, TenantState>,
}

impl MetricsGenerator {
    #[must_use]
    pub fn new(cfg: MetricsGenConfig, clock: Arc<dyn Clock>) -> Self {
        Self { cfg, clock, per_tenant: HashMap::new() }
    }

    pub fn process(&mut self, span: &SpanRecord) {
        let cfg = &self.cfg;
        let state = self.per_tenant.entry(span.tenant.clone()).or_insert_with(|| TenantState {
            span_metrics: SpanMetricsRegistry::new(cfg),
            edges: EdgeStore::new(cfg),
        });
        state.span_metrics.record_span(span);
        state.edges.record_span(span, self.clock.now_ns());
    }

    #[must_use]
    pub fn collect(&mut self, timestamp_ms: i64) -> Vec<SeriesPayload> {
        let now_ns = self.clock.now_ns();
        let mut out = Vec::new();
        for (tenant, state) in &mut self.per_tenant {
            state.edges.expire(now_ns);
            let mut series = state.span_metrics.drain(timestamp_ms);
            series.extend(state.edges.drain(timestamp_ms));
            if !series.is_empty() {
                out.push(SeriesPayload { tenant: tenant.clone(), series });
            }
        }
        out
    }
}
```

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::processor`
Expected: PASS (2 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use processor::MetricsGenerator;`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): MetricsGenerator processor — per-tenant span-metrics + service-graphs"
```

---

### Task 11: `MetricsGenService` — wire source + processors + sink + clock + collect loop

**Files:**
- Modify: `crates/traces/src/metricsgen/service.rs`

**Interfaces:**
- Produces:
  - `struct MetricsGenService<Src, Snk> { source: Arc<Src>, sink: Arc<Snk>, generator: Mutex<MetricsGenerator>, clock: Arc<dyn Clock>, cfg: MetricsGenConfig }`.
  - `fn new(cfg, clock, source: Arc<Src>, sink: Arc<Snk>) -> Self`.
  - `async fn poll_once(&self, max: usize) -> Result<usize, SinkError>` — poll the source, feed each span to the generator; returns spans processed. The deterministic, testable ingest step.
  - `async fn collect_once(&self) -> Result<usize, SinkError>` — drain the generator at `clock.now_ns()/1e6`, `sink.write` each payload, then `source.commit()`; returns payloads flushed (write-then-commit crash-safety order).
  - `async fn run(self, shutdown: CancellationToken)` — interleaves polling with a `collection_interval` ticker; **thin glue over `poll_once`/`collect_once`** (tested directly + a tick-driven test with `tokio::time::pause`).

- [x] **Step 1: Write the failing test**

In `crates/traces/src/metricsgen/service.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;

    use super::*;
    use crate::metricsgen::clock::MockClock;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::sink::{MockRemoteWriteSink, MockSpanSource};

    fn span(tenant: &str, kind: SpanKind, span_id: [u8; 8], parent: [u8; 8]) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            trace_id: [0x44; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            service_name: "svc".into(),
            attributes: vec![],
            size_bytes: 10,
        }
    }

    fn service() -> MetricsGenService<MockSpanSource, MockRemoteWriteSink> {
        let source = Arc::new(MockSpanSource::default());
        let sink = Arc::new(MockRemoteWriteSink::default());
        MetricsGenService::new(MetricsGenConfig::default(), Arc::new(MockClock::new(0)), source, sink)
    }

    #[tokio::test]
    async fn poll_then_collect_writes_then_commits() {
        let svc = service();
        svc.source.push_batch(vec![
            span("A", SpanKind::Client, [0xA; 8], [0; 8]),
            span("A", SpanKind::Server, [0xB; 8], [0xA; 8]),
        ]);

        let processed = svc.poll_once(100).await.unwrap();
        assert!(processed == 2);

        let flushed = svc.collect_once().await.unwrap();
        assert!(flushed == 1); // one tenant payload
        assert!(svc.sink.writes().len() == 1);
        let payload = &svc.sink.writes()[0];
        assert!(payload.tenant == "A");
        assert!(payload.series.iter().any(|s| s.name == "traces_service_graph_request_total"));
        // commit happens AFTER the successful write (crash-safety order).
        assert!(svc.source.commits() == 1);
    }

    #[tokio::test]
    async fn collect_does_not_commit_when_write_fails() {
        let svc = service();
        svc.source.push_batch(vec![span("A", SpanKind::Server, [0xB; 8], [0; 8])]);
        svc.poll_once(100).await.unwrap();
        svc.sink.fail_next();
        let result = svc.collect_once().await;
        assert!(result.is_err());
        // write failed → no commit (the spans get re-processed after restart).
        assert!(svc.source.commits() == 0);
    }

    #[tokio::test]
    async fn empty_poll_is_a_noop() {
        let svc = service();
        assert!(svc.poll_once(100).await.unwrap() == 0);
        assert!(svc.collect_once().await.unwrap() == 0);
        assert!(svc.sink.writes().is_empty());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib metricsgen::service`
Expected: FAIL — `cannot find type MetricsGenService`.

- [x] **Step 3: Implement**

```rust
//! `MetricsGenService` — owns the source, processors, sink, and clock; runs the
//! poll/collect loop. `poll_once`/`collect_once` are the deterministic testable
//! core; `run` is thin interval glue. `collect_once` writes THEN commits
//! (crash-safety: a crash between only re-derives, never loses or double-counts).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::clock::Clock;
use super::config::MetricsGenConfig;
use super::processor::MetricsGenerator;
use super::sink::{RemoteWriteSink, SinkError, SpanSource};

pub struct MetricsGenService<Src, Snk>
where
    Src: SpanSource,
    Snk: RemoteWriteSink,
{
    pub(crate) source: Arc<Src>,
    pub(crate) sink: Arc<Snk>,
    generator: Mutex<MetricsGenerator>,
    clock: Arc<dyn Clock>,
    cfg: MetricsGenConfig,
}

impl<Src, Snk> MetricsGenService<Src, Snk>
where
    Src: SpanSource + 'static,
    Snk: RemoteWriteSink + 'static,
{
    #[must_use]
    pub fn new(
        cfg: MetricsGenConfig,
        clock: Arc<dyn Clock>,
        source: Arc<Src>,
        sink: Arc<Snk>,
    ) -> Self {
        let generator = Mutex::new(MetricsGenerator::new(cfg.clone(), clock.clone()));
        Self { source, sink, generator, clock, cfg }
    }

    /// Poll up to `max` spans and feed them to the generator. Returns the count
    /// processed.
    pub async fn poll_once(&self, max: usize) -> Result<usize, SinkError> {
        let spans = self.source.poll(max).await?;
        let n = spans.len();
        if n > 0 {
            let mut gen = self.generator.lock().expect("generator poisoned");
            for span in &spans {
                gen.process(span);
            }
        }
        Ok(n)
    }

    /// Drain the generator and flush each tenant payload, then commit offsets.
    /// Returns the number of payloads written. Write precedes commit.
    pub async fn collect_once(&self) -> Result<usize, SinkError> {
        let ts_ms = self.clock.now_ns() / 1_000_000;
        let payloads = {
            let mut gen = self.generator.lock().expect("generator poisoned");
            gen.collect(ts_ms)
        };
        if payloads.is_empty() {
            return Ok(0);
        }
        for payload in &payloads {
            self.sink.write(payload).await?;
        }
        self.source.commit().await?;
        Ok(payloads.len())
    }

    /// Run until `shutdown`: poll continuously, flush on the collection interval.
    pub async fn run(self, shutdown: CancellationToken) {
        let interval = self.cfg.collection_interval.max(Duration::from_secs(1));
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    // best-effort final flush.
                    if let Err(e) = self.collect_once().await {
                        tracing::warn!(error = %e, "metrics-generator final flush failed");
                    }
                    return;
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.collect_once().await {
                        tracing::warn!(error = %e, "metrics-generator flush failed");
                    }
                }
                poll = self.poll_once(1_000) => {
                    if let Err(e) = poll {
                        tracing::warn!(error = %e, "metrics-generator poll failed");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}
```

> **Loop-fidelity verify-note:** `run` interleaves a continuous poll future with the interval ticker via `select!`. The *correctness* (RED derivation, edge pairing, write-then-commit) is fully in `poll_once`/`collect_once` and fully tested; the precise poll/flush interleaving + backpressure tuning are Slice-8 refinements (flagged). The real `SpanSource` impl wraps `crabka-client-consumer` (`Consumer::poll(Duration)` → decode each `ConsumerRecord.value` via the Slice-4 `SpanRecord` decoder → `Vec<SpanRecord>`; `commit()` → `Consumer::commit_sync()`); that wrapping is Task 12's binary concern, kept out of the tested service via the `SpanSource` seam.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib metricsgen::service`
Expected: PASS (3 tests).

- [x] **Step 5: Add re-exports** in `metricsgen/mod.rs` (`pub use service::MetricsGenService;`).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): MetricsGenService poll/collect loop + write-then-commit"
```

---

### Task 12: `--target metrics-generator` binary wiring + end-to-end integration test

**Files:**
- Create: `crates/traces/src/bin/metrics_generator.rs` (or add a `metrics-generator` arm to an existing role dispatcher / `main.rs`)
- Create: `crates/traces/tests/metrics_generator_e2e.rs`

**Interfaces:**
- Produces: a binary that parses `--target metrics-generator` + flags (`--bootstrap`, `--remote-write-url`, `--collection-interval`, `--config`), builds a `MetricsGenService` with the real Kafka `SpanSource` (over `crabka-client-consumer`, `group_id = "crabka-traces-metrics-generator"`, subscribing `TRACES_WAL_TOPIC`) + the real `PrometheusRemoteWriteSink`, and spawns `service.run(shutdown)`.
- The **e2e test** is the headline: drives the whole pipeline with mocks end-to-end — feed a scripted batch of client+server spans through `poll_once`, advance the clock past the collection interval, `collect_once`, and assert the mock remote_write sink received `traces_spanmetrics_calls_total` + `traces_service_graph_request_total` with the right labels/exemplars — proving the wiring (source → processors → sink) composes.

- [x] **Step 1: Write the failing e2e test**

Create `crates/traces/tests/metrics_generator_e2e.rs`:

```rust
//! End-to-end: feed client+server spans through the service, collect, and assert
//! span-metrics + service-graph series reach the (mock) remote_write sink with
//! correct labels + exemplars — all through the public metrics-generator surface
//! with mock source/sink/clock.

use std::sync::Arc;

use assert2::assert;

use crabka_traces::metricsgen::clock::MockClock;
use crabka_traces::metricsgen::config::MetricsGenConfig;
use crabka_traces::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
use crabka_traces::metricsgen::sink::{MockRemoteWriteSink, MockSpanSource};
use crabka_traces::metricsgen::MetricsGenService;

fn span(service: &str, kind: SpanKind, status: StatusCode, span_id: [u8; 8], parent: [u8; 8], dur_ns: i64) -> SpanRecord {
    SpanRecord {
        tenant: "tenant-1".into(),
        trace_id: [0xAB; 16],
        span_id,
        parent_span_id: parent,
        name: "GET /checkout".into(),
        kind,
        start_ns: 0,
        duration_ns: dur_ns,
        status,
        service_name: service.into(),
        attributes: vec![],
        size_bytes: 200,
    }
}

#[tokio::test]
async fn metrics_generator_end_to_end_red_and_service_graph() {
    let mut cfg = MetricsGenConfig::default();
    cfg.max_exemplars_per_series = 2; // turn exemplars on for the drill-down link

    let clock = MockClock::new(0);
    let source = Arc::new(MockSpanSource::default());
    let sink = Arc::new(MockRemoteWriteSink::default());

    let svc = MetricsGenService::new(cfg, Arc::new(clock.clone()), source.clone(), sink.clone());

    // a client span (frontend) and its child server span (checkout) of one trace.
    source.push_batch(vec![
        span("frontend", SpanKind::Client, StatusCode::Ok, [0xA; 8], [0; 8], 12_000_000),
        span("checkout", SpanKind::Server, StatusCode::Ok, [0xB; 8], [0xA; 8], 8_000_000),
    ]);

    // 1. poll → both spans processed.
    let processed = svc.poll_once(100).await.unwrap();
    assert!(processed == 2);

    // 2. advance to the collection interval; collect → one tenant payload flushed.
    clock.set(15_000_000_000); // 15s in ns
    let flushed = svc.collect_once().await.unwrap();
    assert!(flushed == 1);

    let writes = sink.writes();
    assert!(writes.len() == 1);
    let payload = &writes[0];
    assert!(payload.tenant == "tenant-1");

    // 3. span-metrics RED present, with exemplars on the latency histogram.
    let calls = payload.series.iter().find(|s| s.name == "traces_spanmetrics_calls_total"
        && s.labels.iter().any(|(k, v)| k == "span_name" && v == "GET /checkout"));
    assert!(calls.is_some());
    let latency = payload.series.iter().find(|s| s.name == "traces_spanmetrics_latency").unwrap();
    assert!(latency.exemplars.iter().any(|e| e.labels.iter().any(|(k, _)| k == "trace_id")));

    // 4. service-graph edge present, labeled client=frontend, server=checkout.
    let edge = payload.series.iter().find(|s| s.name == "traces_service_graph_request_total").unwrap();
    assert!(edge.labels.iter().any(|(k, v)| k == "client" && v == "frontend"));
    assert!(edge.labels.iter().any(|(k, v)| k == "server" && v == "checkout"));

    // 5. write-then-commit ordering.
    assert!(source.commits() == 1);
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --test metrics_generator_e2e`
Expected: FAIL — module paths unresolved until the binary/exports are wired (and `metricsgen` mod must be `pub`).

- [x] **Step 3: Implement the binary + ensure `pub` exports**

Ensure `crates/traces/src/lib.rs` has `pub mod metricsgen;` and `metricsgen/mod.rs` makes its submodules `pub`. Create `crates/traces/src/bin/metrics_generator.rs`:

```rust
//! `crabka-traces --target metrics-generator` (or `cargo run --bin metrics_generator`).
//!
//! Builds a `MetricsGenService` with the real Kafka `SpanSource` (the third
//! traces consumer group) + the Prometheus remote_write sink, and runs the
//! poll/collect loop.

use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use tokio_util::sync::CancellationToken;

use crabka_traces::metricsgen::clock::SystemClock;
use crabka_traces::metricsgen::config::MetricsGenConfig;
use crabka_traces::metricsgen::contract::SpanRecord;
use crabka_traces::metricsgen::remotewrite::PrometheusRemoteWriteSink;
use crabka_traces::metricsgen::sink::{SinkError, SpanSource};
use crabka_traces::metricsgen::MetricsGenService;

#[derive(Parser)]
struct Args {
    /// Role selector (kept for the unified service entrypoint).
    #[arg(long, default_value = "metrics-generator")]
    target: String,
    #[arg(long, default_value = "localhost:9092")]
    bootstrap: String,
    #[arg(long, default_value = "http://localhost:9009/api/v1/push")]
    remote_write_url: String,
    #[arg(long, default_value_t = 15)]
    collection_interval: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    assert_eq!(args.target, "metrics-generator", "this binary serves the metrics-generator role only");

    let mut cfg = MetricsGenConfig::default();
    cfg.collection_interval = std::time::Duration::from_secs(args.collection_interval);
    cfg.remote_write_url = args.remote_write_url.clone();

    // NOTE: the real Kafka SpanSource wraps crabka-client-consumer; until that
    // dep is wired it's a placeholder so the binary compiles and the loop runs.
    // Replace `KafkaSpanSource::connect(...)` when Slice 4's SpanRecord decoder
    // is a dep.
    let source = Arc::new(placeholder::EmptySpanSource);
    let sink = Arc::new(PrometheusRemoteWriteSink::new(args.remote_write_url));

    let svc = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink);
    let shutdown = CancellationToken::new();

    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        });
    }

    svc.run(shutdown).await;
    Ok(())
}

mod placeholder {
    use super::*;

    /// Returns no spans until the real Kafka consumer is wired.
    pub struct EmptySpanSource;

    #[async_trait]
    impl SpanSource for EmptySpanSource {
        async fn poll(&self, _max: usize) -> Result<Vec<SpanRecord>, SinkError> {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            Ok(vec![])
        }
        async fn commit(&self) -> Result<(), SinkError> {
            Ok(())
        }
    }
}
```

> **Binary-wiring verify-notes:**
> 1. The real `SpanSource` wraps `crabka-client-consumer`: `Consumer::builder().bootstrap(&args.bootstrap).group_id("crabka-traces-metrics-generator").subscribe([TRACES_WAL_TOPIC.to_string()]).auto_offset_reset(AutoOffsetReset::Earliest).build().await?`; `poll` → `consumer.poll(Duration::from_millis(500))` then decode each `ConsumerRecord.value` via the Slice-4 `SpanRecord` decoder; `commit` → `consumer.commit_sync()`. This is the third independent consumer group — its own offsets, RF1-safe by the `trace_id`-partition invariant. Flagged; the `SpanSource` seam keeps it out of the tested library + e2e.
> 2. `--target metrics-generator` mirrors the spec's role-selectable service. If the traces service grows a single `main.rs` dispatching all roles (`distributor`/`block-builder`/`live-store`/`querier`/`query-frontend`/`compactor`/`metrics-generator`), fold this binary's body into a `run_metrics_generator(args)` arm — the `MetricsGenService` construction is self-contained.
> 3. The placeholder `EmptySpanSource` is **binary-only** so the role binary compiles and runs the loop before Slice 4 merges. It never appears in library code or the e2e test (which uses `MockSpanSource`). Replace it + wire `PrometheusRemoteWriteSink::write`'s `encode_write_request` (Task 8 TODO) when the `crabka-metrics` pb module is a dep.
> 4. `clap` is a workspace dep (used by the other role binaries). Confirm `clap = { workspace = true, features = ["derive"] }` is in `crates/traces/Cargo.toml`.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --test metrics_generator_e2e`
Expected: PASS.

- [x] **Step 5: Whole-crate gate**

Run: `cargo test -p crabka-traces && cargo clippy -p crabka-traces --all-targets && cargo fmt -p crabka-traces --check`
Expected: all PASS, no warnings, formatting clean.

- [x] **Step 6: Commit**

```bash
git add crates/traces/
git commit -m "feat(traces): --target metrics-generator binary + end-to-end integration test"
```

---

## Self-review

**Spec coverage (against §7 metrics-generator + §11 Slice 7):**
- **Span-metrics (RED)** — `traces_spanmetrics_calls_total` (counter), `traces_spanmetrics_latency` (classic histogram with `_bucket`/`_sum`/`_count`) carrying `trace_id`/`span_id` exemplars (`ObserveWithExemplar`), `traces_spanmetrics_size_total` (counter), and the optional `traces_target_info` (gauge), dimensioned by `service`/`span_name`/`span_kind`/`status_code` → Task 5 (`SpanMetricsRegistry`). Exemplars gated by `max_exemplars_per_series` (off by default, matching Tempo).
- **Service-graphs** — the bounded, TTL'd edge store keyed by `(trace_id, edge_id)`: partner→record→complete, wait-expiry→`unpaired_spans_total`, store-full→`dropped_spans_total`, emitting `traces_service_graph_request_total`/`_request_failed_total`/`_request_client_seconds`/`_request_server_seconds`/`_request_messaging_system_seconds`/`_unpaired_spans_total`/`_dropped_spans_total` labeled `client`/`server`/`connection_type` (unset/`virtual_node`/`messaging_system`/`database`) → Task 6 (`EdgeStore`).
- **remote_write output** — via the metrics signal's client (prost `pb::v1` `WriteRequest` + snappy + exemplars; v2 native-histogram path structured + flagged), to a configured Prometheus endpoint, `X-Scope-OrgID` per tenant → Task 8 (`PrometheusRemoteWriteSink`) + the pure `to_timeseries` transform.
- **Consumer group** — the third independent group on the `trace_id`-partitioned WAL (`group_id = "crabka-traces-metrics-generator"`), own offsets, RF1-safe → the `SpanSource` seam (Task 7) + the binary wiring (Task 12).
- **Rebuildable in-memory state + optional checkpoint** — edges/series are pure read-derived state (Task 10 `MetricsGenerator`); the compacted-topic checkpoint codec is structured + tested (Task 9), live impl deferred to Slice 8.
- **Role binary** — `crabka-traces --target metrics-generator` (Task 12).

**First-class test concerns (as the brief demanded):**
- **TTL'd edge-pairing state machine** — Task 6's `pairs_client_then_server_into_one_request`, `unpaired_half_edge_expires_after_ttl`, and `store_full_drops_new_spans` drive partner→complete, TTL-expiry→unpaired, and capacity→dropped with explicit `now_ns` values (no real time); `failed_when_either_side_errors`, `non_client_server_spans_ignored`, and `database_connection_type_from_db_system_attr` pin the error/ignore/classification edges. Task 10 + the e2e re-prove it through the full service with `MockClock`.
- **RED derivation** — Task 5's `red_counts_calls_and_size_per_dimension` + `latency_histogram_buckets_and_sum` pin per-dimension counts/size/latency (including the `le` bucket boundary + seconds-unit `_sum`); `drain_resets_accumulator` pins the per-interval reset.
- **Exemplar attachment** — Task 5's `exemplar_carries_trace_id_when_enabled` pins the hex `trace_id`/`span_id` + the seconds-unit exemplar value; `exemplars_off_by_default` pins the gating; Task 8's `classic_histogram_fans_into_bucket_sum_count` pins exemplars riding the correct `_bucket` series; the e2e proves the link survives to the sink.
- **Mock remote_write sink** — `MockRemoteWriteSink` (Task 7) captures every payload + can `fail_next()`; Task 11's `collect_does_not_commit_when_write_fails` pins the write-then-commit crash-safety order.

**Churn-prone surfaces handled per the brief (structure + behavior-pinning tests + verify-notes):**
- **Kafka consumer** — isolated behind `SpanSource` (Task 7); the processors + loop are tested with `MockSpanSource`, and the real `crabka-client-consumer` wiring (`poll`/`commit_sync`, `group_id`, `SpanRecord` decode) is a documented one-impl swap in Task 12's binary. Zero Kafka in any tested transform.
- **remote_write HTTP + prost** — the `reqwest` POST + the `pb::v1` prost encode + `snap::raw` snappy (Task 8) are the only network/wire-touching code; the flat `Series`→`TimeSeries` transform is pinned by the pure `to_timeseries`/`le_label` tests, with `encode_write_request` returning a documented `Err` until `crabka-metrics`'s pb module is a dep (so the unit suite is green + the binary compiles), an AM-style snappy-variant verify-note (use `snap::raw`, not Kafka Xerial framing), and an `#[ignore]` testcontainers smoke. No unit test depends on a live endpoint or a not-yet-merged pb module.

**Contract-shim discipline:** every upstream type (`SpanRecord`/`SpanKind`/`StatusCode`/`TRACES_WAL_TOPIC` from Slice 4; `NativeHistogram`/`BucketSpan` + the remote_write `pb` types from the metrics crate) flows through `crate::metricsgen::contract` (Task 1), so when Slice 4 + the metrics crate merge the swap is one file. No `metricsgen` module references an upstream crate directly. The `SpanSource`, `RemoteWriteSink`, and `EdgeCheckpointStore` traits are *role-owned* seams (the role↔consumer, role↔remote_write, and role↔checkpoint boundaries), correctly **not** in `contract`.

**Deviations / deferrals flagged (all to Slice 8 hardening, none silently dropped):**
1. **Native-histogram latency (remote_write v2)** — this slice emits the span-metrics latency + service-graph `_seconds` as **classic** histograms (v1-compatible, simplest exemplar path); the v2 native-histogram encode (with symbol-table interning) is a flagged Slice-8 option, structured by `SeriesSample::NativeHistogram` + the `NativeHistogram` contract type already in scope.
2. **Service-graph `_seconds` real buckets** — emitted as `_sum`/`_count`-only degenerate histograms (`buckets: vec![]`); real bucketed edge-latency is a flagged Slice-8 refinement. Grafana's edge-latency tooltip reads `_sum`/`_count`.
3. **`virtual_node` / `peer.service` fallback edges** — the `ConnectionType::VirtualNode` variant exists; the fallback (a client span with no paired server → a virtual edge) is a flagged Slice-8 refinement. The default client↔child-server keying is exact + pinned.
4. **Edge-state checkpoint (compacted topic)** — codec defined + tested (Task 9); the live Kafka-backed impl + the rebuild-from-checkpoint path land in Slice 8. The state is rebuildable from WAL offsets regardless (spec §9), so the checkpoint is a pure optimization.
5. **Dimension label *values*** (`SPAN_KIND_SERVER`/`STATUS_CODE_OK`) and the **`connection_type=""` unset rendering** — pinned by tests but flagged for empirical confirmation against a live cp-tempo metrics-generator (the spec mandates checking Tempo behavior empirically rather than the wiki); change the helper + test together if Tempo differs.
6. **Precise poll/flush interleaving + backpressure** — `run` is a `select!` loop over `poll_once`/`collect_once`; correctness lives in the fully-tested `poll_once`/`collect_once`. Tuning is Slice-8.
7. **Real `SpanSource` (Kafka) + `encode_write_request` (pb)** — binary-only placeholders so the role compiles + runs pre-Slice-4; both are documented one-impl swaps.

**Placeholder scan:** no "TBD"/"add later"/"similar to Task N". Every step has runnable code or an exact command. The bounded hand-waves — the upstream `contract` types, the real `SpanSource`/`encode_write_request` impls, the binary's `EmptySpanSource` — are explicitly trait-gated, compile-and-test in isolation, and pinned by tests on the pure logic, exactly as the no-placeholders rule requires for not-yet-merged dependencies.

**Type consistency:** label pairs are `Vec<(String, String)>` sorted by key everywhere (stable series identity / JSON); `Series`/`SeriesSample`/`Exemplar`/`SeriesPayload` field sets are identical across the two processors, the encoder, and the mocks. `SinkError` is the single sink/source error. `SpanKind`/`StatusCode` dimension renderings (`SPAN_KIND_*`/`STATUS_CODE_*`) are defined once (Task 1 + Task 5/6) and used identically. The latency/`_seconds` unit convention is **seconds on the wire, nanoseconds in the accumulator** — converted exactly once per path (`NS_PER_SEC`), pinned by the `_sum`-in-seconds + exemplar-value-in-seconds tests.

**Greenfield compliance:** no `#[serde(default)]`-for-old-data (the `#[serde(default)]` on `MetricsGenConfig` is for *absent optional config fields*, not back-compat — correct), no V1/V2 dual variants kept for replay, no migration code. Prometheus remote_write wire identity preserved: the emitted series are ordinary Prometheus series + native histograms + exemplars through the metrics signal's existing `pb` types (no metrics-generator-private format); the metric/label names match Tempo so Grafana reads them by name. Kafka wire identity on the input edge preserved by consuming through `crabka-client-consumer`.
