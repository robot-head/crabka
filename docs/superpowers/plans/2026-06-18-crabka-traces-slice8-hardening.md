# crabka-traces Slice 8 — Hardening (per-tenant limits, multi-tenancy isolation, TraceQL conformance gate, differential-vs-Tempo + Grafana integration)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the traces backend production-faithful at its multi-tenant edges and prove it is a drop-in Tempo replacement. Add per-tenant limits/quotas (traces-per-search, spans-per-trace, ingest rate, max attribute size) with **Tempo-shaped** errors and a YAML runtime-overrides file; harden tenant isolation so org A can never observe org B's traces/tags/spans through the HTTP API; wire the Slice 3 TraceQL golden corpus as a CI conformance gate with a per-file coverage report; and build the two external-system differential suites — **differential-vs-real-Tempo** (testcontainers) and **Grafana** (built-in Tempo datasource → Crabka, including the Service-Graph-via-the-metrics-backend path). The two headline tests are (1) end-to-end tenant isolation through the Tempo HTTP API with two `X-Scope-OrgID`s across every read surface and (2) query-corpus equality vs real Tempo over identically-ingested OTLP traces.

**Architecture:** This slice adds **no new TraceQL or storage semantics** — it is a hardening band around the Slice 4 distributor (ingest), the Slice 5 querier + Tempo HTTP API, the Slice 6 query-frontend, the Slice 7 metrics-generator, and the Slice 2/3 `TraceqlEngine`. New code lives in three areas of `crabka-traces`: (a) a `limits` module (a per-tenant `Limits` struct, a YAML `OverridesProvider` modeled on Tempo's `overrides.yaml` / `per_tenant_override_config`, and enforcement points wired into the distributor write path and the querier read path, reusing the broker's `TokenBucket` for the two *rate* limits); (b) HTTP error-mapping that projects `LimitError` onto the **Tempo** error envelope and status codes; (c) the black-box differential + Grafana harnesses over the compiled Tempo HTTP server + Docker containers. Tenant isolation is **not** a new mechanism — it is the assertion that every existing key (WAL partition key, span-block/`TraceIndex` object key, live-store map key, quota bucket key, metrics-generator edge-store key) is already `(tenant, …)`-prefixed; this slice adds the tests that prove it and fixes any leak they expose. The external suites (Tempo, Grafana) are black-box harnesses over the compiled HTTP server + Docker containers, all `#[ignore]`, run in a dedicated CI job.

**Tech Stack:** Rust 2024 · `arrow` 59 · `axum` 0.8 (handlers + error envelope, reuse the Slice 5 router) · `serde_yaml` 0.9 + `serde` (overrides file) · the broker `TokenBucket` (KIP-73, via a path dep / thin re-export) · `dashmap` 6 (per-tenant bucket cache) · `thiserror`. Tests: `assert2`; `reqwest` 0.13 + `tokio` for in-process HTTP drive; `testcontainers` 0.27 + `testcontainers-modules` 0.15 for the Docker differential suites; `serde_json` for response diffing; `opentelemetry-proto` 0.32 (build identical OTLP push payloads for both backends).

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change the `Limits` schema, the overrides YAML shape, and any error-body shape freely; no shims, no migration code, no `#[serde(default)]` "to keep old configs readable".
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-traces --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-traces` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests.
- **Kafka wire compat is the only Kafka contract that must not drift.** This slice touches no Kafka bytes. The **Tempo HTTP** byte-exactness is the *analog* constraint here: error bodies, status codes, and JSON response shapes must match Tempo exactly (that is what the differential suites verify). The Tempo error body is a plain-text/JSON `{ "status": "error", "error": "<message>" }`-style envelope — pin it against the real container in Task 11, do not invent it.
- **Docker/external-system tests are `#[ignore]`.** Every test that needs a running Tempo or Grafana container is annotated `#[ignore = "requires Docker"]` and lives behind the dedicated CI job (`traces-differential`), never in the default `cargo test --workspace` path. Reuse the Confluent-image rationale and bootstrap-retry patterns from `crates/client-core/tests/integration.rs`.
- **Reuse, don't reinvent, the token bucket.** Per-tenant *rate* limits (ingest rate, in spans/sec) use the broker's `crabka_broker::throttle::TokenBucket` (`new()`, `set_rate(u64)`, `try_consume(u64) -> u64` granted; rate-0 ⇒ unthrottled, granting the full request). Do not write a second rate limiter. The *count/length* limits (max traces-per-search, max spans-per-trace, max attribute size) are plain comparisons, not buckets.
- **Tempo limit parity.** Limit names mirror Tempo's `overrides` block where one exists: `max_search_duration` / `max_bytes_per_trace` (`max_traces_per_user`-adjacent) / `ingestion_rate_limit_bytes` / `max_bytes_per_tag_values_query`. We adopt the *semantics* and the *4xx/429 mapping*, not byte-for-byte config-key names where the spec (§9) names them differently; each field carries a doc-comment naming the Tempo analog.

---

## Dependency & slice roadmap

**Depends on:**
- **Slice 1** (`crabka-blockstore` generalization) — the `BlockIndex` trait, `TraceIndex`, the flattened span block schema. ✅ planned. This slice does not touch block bytes; it consumes the `TraceIndex` only transitively through the querier.
- **Slice 2/3** (`crabka-traceql`) — the `TraceqlEngine`/`SpanStore` the limits cap and the conformance gate exercise. **Consumed via contract** (see Shared Contract below); if Slice 3 is unlanded, the conformance-gate task points at whatever subset of the corpus the harness currently passes and records the rest in `KNOWN_UNSUPPORTED` with a reason.
- **Slice 4** (ingest service) — the `distributor` write path (`decode → tenant-route → hash(trace_id) → produce`) where ingest-rate / spans-per-trace / attribute-size limits are enforced; the `SpanRecord` WAL record used to build identical OTLP payloads for the differential suites.
- **Slice 5** (querier + Tempo HTTP API) — the axum router, the `X-Scope-OrgID` tenancy extractor, the Tempo-shaped JSON response/error envelope, and the `SpanStore` impl (`CrabkaSpanStore`). **This slice extends that router's error envelope** (adds `429`/`400`/`422` limit errors) and asserts isolation through it.
- **Slice 6** (query-frontend) — the search-sharding/queue layer the `max_search_duration` / traces-per-search caps are applied in front of (the frontend is where Tempo enforces `max_search_duration`).
- **Slice 7** (metrics-generator) — the `traces_service_graph_*` / `traces_spanmetrics_*` series the Grafana Service-Graph integration test reads back from the metrics backend.

**Shared Contract (consume, do not re-derive).** The following are assumed to exist from earlier slices; each task that touches them lists the exact item it consumes. If an item is missing because its slice is unlanded, the task creates a **minimal local trait/shim with a single `todo!()`-free in-memory impl for tests** and flags it in the task's "Contract gap" note — never a silent stub.

| Contract item | From | This slice consumes it as |
|---|---|---|
| `tenant_of(&HeaderMap) -> String` (resolves `X-Scope-OrgID`, default `"anonymous"`) | Slice 5 | the key prefix every limit/isolation test asserts on |
| `TraceqlEngine::{search, query_range, trace_by_id}` + `EngineOpts { default_limit, default_spss, max_traces }` | Slice 2/3 | the body the limits cap (traces-per-search via `limit`/`max_traces`) and the corpus drives |
| `SpanStore::{scan, trace_by_id, tag_names, tag_values}` + `CrabkaSpanStore` (hot/cold UNION) | Slice 2 def / Slice 5 impl | the tenant-scoped source every isolation assertion reads through |
| axum `Router` + Tempo JSON envelope + Tempo error envelope | Slice 5 | extended with new error variants (`429`/`400`/`422`) |
| distributor write path hook (`SpanRecord` batch, per-tenant, before WAL append) | Slice 4 | the enforcement point for ingest-rate / spans-per-trace / attribute-size limits |
| `SpanRecord` (WAL record: tenant + OTLP-derived span) encode/decode | Slice 4 | building identical OTLP push payloads for the differential suites |
| `crabka_broker::throttle::TokenBucket` | broker | the per-tenant ingest-rate bucket |

**The 8 traces slices** (this plan = Slice 8, the last): 1 blockstore generalization + span schema + `TraceIndex` · 2 `crabka-traceql` core · 3 TraceQL completeness · 4 ingest · 5 querier + Tempo HTTP API · 6 query-frontend · 7 metrics-generator · **8 hardening (this plan)**.

---

## File structure (`crates/traces/`)

| File | Responsibility |
|---|---|
| `src/limits/mod.rs` | `Limits` struct + `LimitError` (Tempo-shaped) + module re-exports |
| `src/limits/overrides.rs` | `OverridesProvider` — load Tempo-style `overrides.yaml`, resolve per-tenant `Limits` (tenant override merged over defaults) |
| `src/limits/enforce.rs` | enforcement helpers: ingest-side (`IngestEnforcer`) + query-side (`QueryEnforcer`); ingest-rate uses `TokenBucket` |
| `src/http/error.rs` | `LimitError` → Tempo HTTP status/body projection (extends the Slice 5 envelope) |
| `tests/limits_overrides.rs` | unit/integration: YAML load + per-tenant resolution + enforcement decisions through real HTTP |
| `tests/tenant_isolation.rs` | **headline** — two-`X-Scope-OrgID` end-to-end isolation through the Tempo HTTP API (in-process, no Docker) |
| `tests/traceql_conformance.rs` | the Slice-3 golden corpus gate + per-file pass/fail coverage report |
| `tests/diff_tempo.rs` | `#[ignore]` **headline** differential vs real Tempo (testcontainers) |
| `tests/grafana_integration.rs` | `#[ignore]` Grafana + built-in Tempo datasource → Crabka (echo/trace-view/Search/TraceQL/Service-Graph) |
| `tests/support/traces_server.rs` | shared in-process server boot + two-tenant seed helpers (path-included by the integration tests) |
| `tests/support/diff_corpus.rs` | the shared OTLP seed dataset + TraceQL/by-id corpus + a `assert_trace_query_equal` JSON differ (path-included by the Docker suites) |

---

### Task 1: `Limits` model + Tempo-shaped `LimitError`

**Files:**
- Create: `crates/traces/src/limits/mod.rs`
- Modify: `crates/traces/src/lib.rs` (add `pub mod limits;` + re-exports)
- Modify: `crates/traces/Cargo.toml` (add `serde` with `derive`, `thiserror`)

**Interfaces:**
- Produces:
  - `struct Limits` (`Clone`, `Debug`, `PartialEq`, `serde::Deserialize`, `serde::Serialize`) with fields, each carrying a doc-comment naming its Tempo analog:
    - `ingestion_rate_spans_per_sec: f64` (Tempo `ingestion_rate_limit_bytes` analog; `0.0` ⇒ unlimited)
    - `ingestion_burst_spans: u64` (Tempo `ingestion_burst_size_bytes` analog)
    - `max_traces_per_search: u64` (the `/api/search` `limit` ceiling; `0` ⇒ unlimited) — distinct from the **per-request** `limit` query-param, this is the per-tenant *cap* on it
    - `max_spans_per_trace: u64` (Tempo `max_bytes_per_trace` analog, counted in spans not bytes; `0` ⇒ unlimited)
    - `max_attribute_bytes: u64` (max UTF-8 byte length of any single attribute key **or** string value; `0` ⇒ unlimited)
    - `max_search_duration_secs: u64` (Tempo `max_search_duration`; the `(end-start)` ceiling for `/api/search` and `/api/metrics/query_range`; `0` ⇒ unlimited)
  - `impl Default for Limits` — generous Tempo-default-ish values (`ingestion_rate_spans_per_sec: 100_000.0`, `ingestion_burst_spans: 100_000`, `max_traces_per_search: 1000`, `max_spans_per_trace: 200_000`, `max_attribute_bytes: 2048`, `max_search_duration_secs: 0` ⇒ unlimited).
  - `enum LimitError` (`thiserror`) with the variants below, each carrying the over-limit value + the cap, and each mapping to a Tempo status:
    - `IngestionRateExceeded { rate: f64, observed: f64 }` → **429**
    - `MaxSpansPerTrace { limit: u64, observed: u64 }` → **400** (Tempo rejects an oversized trace at ingest with `TRACE_TOO_LARGE`)
    - `AttributeTooLong { limit: u64, observed: u64 }` → **400**
    - `TracesPerSearchExceeded { limit: u64, requested: u64 }` → **400** (Tempo clamps `limit` / 400s on an out-of-range `limit`)
    - `SearchDurationExceeded { limit_secs: u64, observed_secs: u64 }` → **400** (Tempo `range specified … exceeds … max_search_duration` is a 400)
  - `impl LimitError { pub fn http_status(&self) -> u16; pub fn message(&self) -> String }` — `message` is the human string Tempo puts in the error envelope (e.g. `"trace exceeds max spans per trace (200000)"`); `http_status` is the Tempo status. **Pin the exact strings against the real Tempo container in Task 11** — until then use the structurally-correct messages here and note the verify-against-rev in the doc-comment.

> **Tempo status mapping note:** Tempo returns **429** only for the *rate* limit (ingestion). The *size/count* limits (oversized trace, over-long attribute, over-range search, out-of-range `limit`) are **400** with a descriptive body — Tempo does not use 422 for these (that mapping is a Prometheus/Mimir convention; do not copy it here). This is a deliberate divergence from the metrics-slice-8 status map; the differential suite (Task 4) verifies the real status codes.

- [x] **Step 1: Write the failing test**

Create `crates/traces/src/limits/mod.rs` with only a `tests` module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let l = Limits::default();
        assert!(l.ingestion_rate_spans_per_sec > 0.0);
        assert!(l.max_spans_per_trace >= 100_000);
        assert!(l.max_attribute_bytes == 2048);
        assert!(l.max_search_duration_secs == 0); // unlimited by default
    }

    #[test]
    fn limit_errors_carry_tempo_status() {
        let rate = LimitError::IngestionRateExceeded { rate: 100_000.0, observed: 120_000.0 };
        assert!(rate.http_status() == 429);

        let big = LimitError::MaxSpansPerTrace { limit: 200_000, observed: 200_001 };
        assert!(big.http_status() == 400);

        let attr = LimitError::AttributeTooLong { limit: 2048, observed: 5000 };
        assert!(attr.http_status() == 400);

        let dur = LimitError::SearchDurationExceeded { limit_secs: 3600, observed_secs: 7200 };
        assert!(dur.http_status() == 400);
    }

    #[test]
    fn limit_error_message_names_the_cap() {
        let big = LimitError::MaxSpansPerTrace { limit: 200_000, observed: 200_001 };
        assert!(big.message().contains("200000"));
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib limits`
Expected: FAIL — `cannot find type Limits`.

- [x] **Step 3: Implement `Limits` + `LimitError`**

Prepend above `tests`. Define `Limits` with the fields/`Default` above, and `LimitError` with `thiserror`, the carried fields, and the two `impl` methods. Map statuses exactly: `IngestionRateExceeded` ⇒ 429; everything else ⇒ 400. `message()` formats the cap into the string (verify-against-Tempo note in the doc-comment). Add `serde` (with `derive`) + `thiserror` to `Cargo.toml`.

- [x] **Step 4: Wire into `lib.rs`** — `pub mod limits;` and `pub use limits::{Limits, LimitError};`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib limits`
Expected: PASS (3 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): per-tenant Limits model + Tempo-shaped LimitError"
```

---

### Task 2: `OverridesProvider` — Tempo-style `overrides.yaml`

**Files:**
- Create: `crates/traces/src/limits/overrides.rs`
- Modify: `crates/traces/src/limits/mod.rs` (declare submodule + re-export)
- Modify: `crates/traces/Cargo.toml` (add `serde_yaml`)

**Interfaces:**
- Consumes: `Limits` (Task 1), `serde_yaml`.
- Produces:
  - `struct OverridesProvider { defaults: Limits, per_tenant: HashMap<String, Limits> }` (`Clone`, `Debug`).
  - `impl OverridesProvider`:
    - `pub fn new(defaults: Limits) -> Self`
    - `pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError>` — parse the Tempo per-tenant-override shape (top-level `overrides: { <tenant>: { …partial Limits… } }`); a tenant's entry overrides only the fields it names, the rest fall back to `defaults`.
    - `pub fn for_tenant(&self, tenant: &str) -> &Limits` — returns the tenant's resolved `Limits`, or `&self.defaults` if unlisted.
  - `enum OverridesError` (`thiserror`): `Yaml(String)`.

> **Tempo runtime parity:** Tempo's `per_tenant_override_config` file keys limits under `overrides:` per-tenant and merges over the static config defaults. We model defaults as a struct (not a second YAML layer) and let each tenant's YAML map be a *partial* `Limits` via an internal `PartialLimits` mirror (every field `Option<…>`, `#[serde(default)]`) that then merges field-by-field onto `defaults`. (The no-back-compat rule bans `#[serde(default)]` used as a *compat* shim for old schemas; using it to express "this tenant only overrides some fields" is a legitimate partial-config pattern, not a migration. Note this in a code comment so a future reader doesn't flag it.)

- [x] **Step 1: Write the failing test**

Create `crates/traces/src/limits/overrides.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const YAML: &str = r#"
overrides:
  tenant-a:
    ingestion_rate_spans_per_sec: 500
    max_spans_per_trace: 1000
  tenant-b:
    max_attribute_bytes: 64
"#;

    #[test]
    fn tenant_override_merges_over_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let a = p.for_tenant("tenant-a");
        assert!(a.ingestion_rate_spans_per_sec == 500.0);
        assert!(a.max_spans_per_trace == 1000);
        // unspecified field falls back to default
        assert!(a.max_attribute_bytes == Limits::default().max_attribute_bytes);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let b = p.for_tenant("tenant-b");
        assert!(b.max_attribute_bytes == 64);
        assert!(b.ingestion_rate_spans_per_sec == Limits::default().ingestion_rate_spans_per_sec);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(*p.for_tenant("tenant-z") == Limits::default());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib overrides`
Expected: FAIL — `cannot find type OverridesProvider`.

- [x] **Step 3: Implement `overrides.rs`**

Define an internal `#[derive(Deserialize)] struct PartialLimits` with every field `Option<…>` (and `#[serde(default)]`), a `struct RuntimeFile { #[serde(default)] overrides: HashMap<String, PartialLimits> }`, `from_yaml` parsing into that and merging each `PartialLimits` onto a clone of `defaults` (a `fn merge(base: &Limits, p: &PartialLimits) -> Limits`). `for_tenant` returns the precomputed resolved `Limits`. Map `serde_yaml::Error` → `OverridesError::Yaml(e.to_string())`.

- [x] **Step 4: Wire into `mod.rs`** — `mod overrides; pub use overrides::{OverridesError, OverridesProvider};` and re-export from `lib.rs`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib overrides`
Expected: PASS (3 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): Tempo-style overrides.yaml OverridesProvider"
```

---

### Task 3: Limit enforcement — ingest side (rate + spans-per-trace + attribute size), query side (traces-per-search + search duration)

**Files:**
- Create: `crates/traces/src/limits/enforce.rs`
- Modify: `crates/traces/src/limits/mod.rs` (declare submodule + re-export)
- Modify: `crates/traces/Cargo.toml` (add `crabka-broker` path dep for `TokenBucket` + `dashmap`)

**Interfaces:**
- Consumes: `Limits`, `LimitError` (Task 1); `crabka_broker::throttle::TokenBucket`; the Slice 4 `SpanRecord` (read its attrs for the attribute-size check).
- Produces:
  - `struct IngestEnforcer` holding a `DashMap<String /*tenant*/, Arc<TokenBucket>>` for the per-tenant ingest-rate bucket.
  - `impl IngestEnforcer`:
    - `pub fn new() -> Self`
    - `pub fn check_span_rate(&self, limits: &Limits, tenant: &str, n_spans: u64) -> Result<(), LimitError>` — `0.0` rate ⇒ Ok; else get-or-create the tenant bucket at `ingestion_rate_spans_per_sec` (rounded to `u64`, burst `ingestion_burst_spans`) via `set_rate` on creation, `try_consume(n_spans)`; if granted `< n_spans` ⇒ `IngestionRateExceeded`.
    - `pub fn check_trace_size(limits: &Limits, spans_in_trace: u64) -> Result<(), LimitError>` — `spans_in_trace > max_spans_per_trace` (when nonzero) ⇒ `MaxSpansPerTrace`. (Associated fn — no state.)
    - `pub fn check_attributes(limits: &Limits, attrs: &[(String, String)]) -> Result<(), LimitError>` — any attribute key **or** string value whose UTF-8 byte length exceeds `max_attribute_bytes` (when nonzero) ⇒ `AttributeTooLong`. (Associated fn — no state. The caller flattens a `SpanRecord`'s resource+span string attrs into `(key, value)` pairs first; non-string values are exempt.)
  - `struct QueryEnforcer` (stateless; associated fns):
    - `pub fn check_search_limit(limits: &Limits, requested: u64) -> Result<(), LimitError>` — `requested > max_traces_per_search` (when nonzero) ⇒ `TracesPerSearchExceeded`.
    - `pub fn check_search_duration(limits: &Limits, start_ns: i64, end_ns: i64) -> Result<(), LimitError>` — `(end_ns - start_ns) / 1_000_000_000 > max_search_duration_secs` (when nonzero) ⇒ `SearchDurationExceeded`.

> **TokenBucket reuse note:** `crabka_broker::throttle::TokenBucket` is the KIP-73 bucket (`new()`, `set_rate(u64)` seeds a one-second burst at the new rate, `try_consume(u64) -> u64` granted; rate-0 grants the full request). It meters in whatever integer unit you set the rate in; here the unit is *spans*, rate = `ingestion_rate_spans_per_sec` rounded to `u64`, and `set_rate(burst)` seeds the burst (set the rate to `ingestion_burst_spans` on creation so the first burst is `ingestion_burst_spans`, then the steady-state refill is `ingestion_rate_spans_per_sec`/sec — match the metrics-slice-8 mapping: `set_rate` once at the *burst* to seed `available`, and store the steady `rate` for refills if the bucket exposes a separate refill rate; if `TokenBucket` couples burst==rate, seed at `max(rate, burst)` and document the approximation). If `crabka-broker` is too heavy/cyclic a dep, lift `throttle/bucket.rs` into a tiny `crabka-throttle` crate and depend on that from both — but **prefer the path dep** unless a cycle appears, and note the choice in the commit. The pure arithmetic (`plan_consume`) is already unit-tested in the broker, so this task tests only the *mapping* (limit → bucket config → decision), not the bucket math.

- [x] **Step 1: Write the failing test**

Create `crates/traces/src/limits/enforce.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::limits::Limits;

    fn limits_with(spans: u64, attr_bytes: u64) -> Limits {
        Limits { max_spans_per_trace: spans, max_attribute_bytes: attr_bytes, ..Limits::default() }
    }

    #[test]
    fn trace_size_cap_rejects_over_limit() {
        let l = limits_with(100, 2048);
        assert!(IngestEnforcer::check_trace_size(&l, 100).is_ok());   // == 100, ok
        assert!(matches!(IngestEnforcer::check_trace_size(&l, 101),
                         Err(LimitError::MaxSpansPerTrace { .. })));  // > 100
    }

    #[test]
    fn zero_trace_size_cap_is_unlimited() {
        let l = limits_with(0, 2048);
        assert!(IngestEnforcer::check_trace_size(&l, 5_000_000).is_ok());
    }

    #[test]
    fn attribute_size_cap_enforced() {
        let l = limits_with(0, 4);
        let ok = vec![("ab".to_string(), "cd".to_string())];
        let bad_key = vec![("toolong".to_string(), "x".to_string())];
        let bad_val = vec![("a".to_string(), "toolong".to_string())];
        assert!(IngestEnforcer::check_attributes(&l, &ok).is_ok());
        assert!(matches!(IngestEnforcer::check_attributes(&l, &bad_key),
                         Err(LimitError::AttributeTooLong { .. })));
        assert!(matches!(IngestEnforcer::check_attributes(&l, &bad_val),
                         Err(LimitError::AttributeTooLong { .. })));
    }

    #[test]
    fn ingest_rate_bucket_eventually_rejects() {
        let e = IngestEnforcer::new();
        let l = Limits {
            ingestion_rate_spans_per_sec: 100.0,
            ingestion_burst_spans: 100,
            ..Limits::default()
        };
        // First burst within budget.
        assert!(e.check_span_rate(&l, "t", 100).is_ok());
        // Immediately over: budget exhausted, no refill yet.
        assert!(e.check_span_rate(&l, "t", 100).is_err());
    }

    #[test]
    fn search_limit_and_duration_caps() {
        let l = Limits { max_traces_per_search: 1000, max_search_duration_secs: 3600,
                         ..Limits::default() };
        assert!(QueryEnforcer::check_search_limit(&l, 1000).is_ok());
        assert!(matches!(QueryEnforcer::check_search_limit(&l, 1001),
                         Err(LimitError::TracesPerSearchExceeded { .. })));
        let ns = 1_000_000_000_000_000_000_i64;
        // 2h window > 1h cap
        assert!(matches!(
            QueryEnforcer::check_search_duration(&l, ns, ns + 7_200_000_000_000),
            Err(LimitError::SearchDurationExceeded { .. })));
        // 30m window ok
        assert!(QueryEnforcer::check_search_duration(&l, ns, ns + 1_800_000_000_000).is_ok());
    }
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --lib enforce`
Expected: FAIL — `cannot find type IngestEnforcer`.

- [x] **Step 3: Implement `enforce.rs`**

Implement `IngestEnforcer` (with `DashMap` bucket cache, `new()`), `QueryEnforcer`, and the five check methods exactly per the interfaces. For `check_span_rate`: round `ingestion_rate_spans_per_sec` to `u64`; get-or-create the tenant's `TokenBucket`, `set_rate` on creation (seeds the burst); `try_consume(n)`; granted `< n` ⇒ error. Add `crabka-broker` (path) + `dashmap` to `Cargo.toml`.

- [x] **Step 4: Wire into `mod.rs`** — `mod enforce; pub use enforce::{IngestEnforcer, QueryEnforcer};` + re-export from `lib.rs`.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --lib enforce`
Expected: PASS (5 tests).

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): per-tenant limit enforcement (ingest rate/trace-size/attr-size + search caps)"
```

---

### Task 4: `LimitError` → Tempo HTTP envelope + enforcement wired into the live write/read paths

**Files:**
- Create: `crates/traces/src/http/error.rs` (the `LimitError` → `Response` projection)
- Modify: the distributor write handler (Slice 4) — call `IngestEnforcer` before WAL append.
- Modify: the query handlers (Slice 5) + the query-frontend search entry (Slice 6) — call `QueryEnforcer` in `/api/search` + `/api/metrics/query_range`.
- Modify: server boot to accept an `OverridesProvider`.
- Create: `crates/traces/tests/limits_overrides.rs` (drives limits through real HTTP).
- Modify: `crates/traces/Cargo.toml` (`[dev-dependencies]` `reqwest`, `tokio`, `serde_json`).

**Interfaces:**
- Consumes: Task 1 `LimitError`, Task 2 `OverridesProvider`, Task 3 enforcers, the Slice 5 Tempo error envelope, the Task 6 server boot.
- Produces:
  - `http/error.rs`: `pub fn limit_error_response(err: &LimitError) -> axum::response::Response` — status from `err.http_status()`, body in the **Tempo** error shape (the same envelope the Slice 5 router already emits for other 4xx; reuse it, don't fork). 429 carries the retriable semantics Tempo uses.
  - Enforcement at the live edges + a test asserting **Tempo-shaped error bodies** end-to-end:
    - over-long attribute push → `400` with the descriptive body.
    - oversized trace (spans > `max_spans_per_trace`) push → `400`.
    - over-rate push → `429`.
    - `/api/search` with `limit` > `max_traces_per_search` → `400`.
    - `/api/search` with `(end-start)` > `max_search_duration` → `400`.

> **Contract gap note:** the exact Tempo error-body *string* is pinned against the real container in Task 11; here assert `(status, has-error-field)` and the descriptive substring, not byte-equality with a guessed message. The status codes are the firm contract; the message strings firm up after the differential run.

- [x] **Step 1: Write the failing test**

Create `crates/traces/tests/limits_overrides.rs`. Boot the server with an `OverridesProvider` carrying tight caps for `tenant-tight`; drive each over-limit case over real HTTP and assert `(status, body has "error")`:

```rust
mod support;
use assert2::check;
use support::traces_server as srv;

#[tokio::test]
async fn over_limit_requests_return_tempo_shaped_errors() {
    let overrides = r#"
overrides:
  tenant-tight:
    ingestion_rate_spans_per_sec: 1
    ingestion_burst_spans: 1
    max_spans_per_trace: 2
    max_attribute_bytes: 4
    max_search_duration_secs: 60
"#;
    let s = srv::start_in_process_with_overrides(overrides).await;

    // over-long attribute -> 400
    let (st, body) = srv::push_otlp_expect_error(
        &s.base_url, "tenant-tight",
        &srv::one_span_with_attr("toolongvalue")).await;
    check!(st == 400 && body.get("error").is_some());

    // oversized trace (3 spans > cap 2) -> 400
    let (st, _) = srv::push_otlp_expect_error(
        &s.base_url, "tenant-tight", &srv::trace_with_n_spans(3)).await;
    check!(st == 400);

    // search range > 60s -> 400
    let (st, _) = srv::search_expect_error(
        &s.base_url, "tenant-tight", "{}", 0, 3_600_000_000_000, 20).await;
    check!(st == 400);

    // burst then over-rate -> 429
    let _ = srv::push_otlp(&s.base_url, "tenant-tight", &srv::trace_with_n_spans(1)).await;
    let (st, _) = srv::push_otlp_expect_error(
        &s.base_url, "tenant-tight", &srv::trace_with_n_spans(1)).await;
    check!(st == 429);
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --test limits_overrides`
Expected: FAIL — `support::traces_server` / enforcement not wired; over-limit requests currently succeed (`200`/`204`). (The `support::traces_server` boot helper is authored in Task 5 Step 1; cross-reference noted — write that helper first, or stub a minimal boot here and converge.)

- [x] **Step 3: Implement `http/error.rs` + wire enforcement into the live handlers**

Implement `limit_error_response`. Call `IngestEnforcer::check_attributes` + `check_trace_size` + `check_span_rate` in the distributor pre-WAL hook (compute `spans_in_trace` per `trace_id` group in the request before append; flatten string attrs for the size check); `QueryEnforcer::check_search_limit` + `check_search_duration` in the `/api/search` + `/api/metrics/query_range` entry. Convert `LimitError` → response via `limit_error_response`. Thread the `OverridesProvider` from server boot into both hooks.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --test limits_overrides`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "feat(traces): enforce per-tenant limits at live write/read edges with Tempo error bodies"
```

---

### Task 5: **Headline** — multi-tenant isolation end-to-end through the Tempo HTTP API

**Files:**
- Create: `crates/traces/tests/support/traces_server.rs` (shared in-process boot + two-tenant seed + OTLP push helpers)
- Create: `crates/traces/tests/tenant_isolation.rs`
- Modify: `crates/traces/Cargo.toml` (`[dev-dependencies]` `reqwest`, `tokio`, `serde_json`, `opentelemetry-proto`)

**Interfaces:**
- `support::traces_server`:
  - `pub async fn start_in_process() -> TestServer` — boots the traces service (distributor + live-store + querier roles in-process, in-memory/tempdir WAL + blockstore, the Slice 5 router) on an ephemeral port; returns `{ base_url, _guard }`.
  - `pub async fn start_in_process_with_overrides(yaml: &str) -> TestServer` — same, with an `OverridesProvider` loaded from `yaml` (used by Task 4).
  - `pub async fn push_otlp(base: &str, tenant: &str, traces: &OtlpTracesPayload)` — `POST /v1/traces` (HTTP-protobuf) for `tenant` via `X-Scope-OrgID`; builds the OTLP `ExportTraceServiceRequest` via `opentelemetry-proto`.
  - builders `one_span_with_attr(val)`, `trace_with_n_spans(n)`, `trace(trace_id, service, root_name, spans)` returning `OtlpTracesPayload` — deterministic OTLP fixtures.
  - read helpers, each issuing the HTTP request with the tenant header and returning parsed JSON: `search(base, tenant, q, start_ns, end_ns, limit)`, `trace_by_id(base, tenant, trace_id_hex)`, `search_tags(base, tenant, scope)`, `tag_values(base, tenant, tag)`.
  - error variants `push_otlp_expect_error(...) -> (u16, serde_json::Value)`, `search_expect_error(...) -> (u16, serde_json::Value)` (used by Task 4).

> **Contract gap note:** if the Slice 4/5 in-process boot isn't available, this helper assembles it from the public role constructors `crabka-traces` exposes; if those are absent, the task spins the `axum::Router` directly over an in-memory `CrabkaSpanStore` (live-store only) + `TraceqlEngine` and drives writes through the distributor entry fn. Either way: **real HTTP over a real socket** (so `X-Scope-OrgID` goes through the genuine extractor), not a function-call shortcut — the whole point is to exercise the tenancy boundary as Grafana would.

- [x] **Step 1: Write the failing isolation test**

Create `crates/traces/tests/tenant_isolation.rs`. Seed **two** tenants with *deliberately colliding* trace identity (same `trace_id` bytes, same service name, different root-span name + a tenant-A-only attribute), then assert A cannot see B and vice versa across **every** read surface:

```rust
mod support;
use assert2::{assert, check};
use support::traces_server::{self as srv};

#[tokio::test]
async fn tenants_are_fully_isolated_across_all_read_surfaces() {
    let s = srv::start_in_process().await;

    // SAME trace_id bytes in both tenants, plus an attribute unique to A.
    let tid = [0xAB_u8; 16];
    let a_trace = srv::trace(tid, "checkout", "POST /a",
        &[srv::span_with_attr("tenant_only", "A")]);
    let b_trace = srv::trace(tid, "checkout", "POST /b",
        &[srv::span_with_attr("plain", "x")]);
    srv::push_otlp(&s.base_url, "tenant-a", &a_trace).await;
    srv::push_otlp(&s.base_url, "tenant-b", &b_trace).await;

    let hex = "abababababababababababababababab";
    let (full_lo, full_hi) = (0_i64, i64::MAX);

    // 1) trace_by_id: A sees root "POST /a", B sees "POST /b" — never each other's,
    //    even though the trace_id bytes are identical.
    let ta = srv::trace_by_id(&s.base_url, "tenant-a", hex).await;
    let tb = srv::trace_by_id(&s.base_url, "tenant-b", hex).await;
    check!(root_name(&ta) == "POST /a");
    check!(root_name(&tb) == "POST /b");

    // 2) search: A's result set never contains B's root name and vice versa.
    let sa = srv::search(&s.base_url, "tenant-a", "{}", full_lo, full_hi, 20).await;
    let sb = srv::search(&s.base_url, "tenant-b", "{}", full_lo, full_hi, 20).await;
    check!(root_names(&sa) == vec!["POST /a".to_string()]);
    check!(root_names(&sb) == vec!["POST /b".to_string()]);

    // 3) search/tags: A has the `tenant_only` tag, B does NOT.
    let tags_a = srv::search_tags(&s.base_url, "tenant-a", "span").await;
    let tags_b = srv::search_tags(&s.base_url, "tenant-b", "span").await;
    check!(tag_list(&tags_a).contains(&"tenant_only".to_string()));
    check!(!tag_list(&tags_b).contains(&"tenant_only".to_string()));

    // 4) tag/{tag}/values: B never sees A's `tenant_only=A`.
    let va = srv::tag_values(&s.base_url, "tenant-b", "tenant_only").await;
    check!(value_list(&va).is_empty());

    // 5) TraceQL select on the A-only attribute returns nothing for B.
    let qb = srv::search(&s.base_url, "tenant-b", "{ .tenant_only = \"A\" }",
        full_lo, full_hi, 20).await;
    assert!(root_names(&qb).is_empty());
}
```

(Provide the small `root_name`/`root_names`/`tag_list`/`value_list`/`span_with_attr` helpers in the test file or `support`.)

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --test tenant_isolation`
Expected: FAIL — `support::traces_server` / boot not yet present (or, if present, a real leak surfaces — fix it).

- [x] **Step 3: Implement `support::traces_server` + fix any leak**

Build the boot/seed/push/read helpers. Run the test. **If it reveals a real isolation leak** (a `TraceIndex`/live-store/block/edge-store key that isn't tenant-prefixed), fix the offending key in the earlier-slice code (the isolation boundary is the product requirement; this test is its enforcement) and note the fix in the commit. The colliding-`trace_id` case is the sharpest probe: it forces the by-id bloom + row-group path to be tenant-scoped *before* the bloom test, not after.

- [x] **Step 4: Add a per-tenant quota-isolation assertion**

Append a test: boot with an `OverridesProvider` setting a tiny `ingestion_rate_spans_per_sec` for `tenant-a` only, push enough to throttle A (expect `429`), and confirm `tenant-b` is unaffected at the same instant. Proves quotas are bucketed per tenant.

- [x] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-traces --test tenant_isolation`
Expected: PASS.

- [x] **Step 6: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "test(traces): headline multi-tenant isolation across all read surfaces + per-tenant quota"
```

---

### Task 6: TraceQL conformance gate + per-file coverage report

**Files:**
- Create: `crates/traces/tests/traceql_conformance.rs` (or in `crabka-traceql` if the corpus harness lives there — **place it beside the harness from Slice 3**; this task wires it as a *gate* + report).
- Modify: CI workflow (a `traces-conformance` job) — see Task 9.

**Interfaces:**
- Consumes: the Slice-3 curated golden TraceQL corpus + its harness (load a span-tree fixture → run a TraceQL query → compare against the documented-expected spanSets / by-id result). There is **no** upstream TraceQL `.test`-style corpus (the spec §10 says so), so this gates the *curated* golden set Slice 3 built, not a vendored upstream corpus.
- Produces:
  - A test that **runs every golden case** in the corpus and **fails if any case fails** (the gate).
  - A **coverage report**: per-file (per-fixture) pass/fail (and pass-count/total within a file), written to `target/traceql-conformance-report.txt` (and printed under `--nocapture`), so a regression names the fixture. Use a known-pass allowlist *only if* a case is legitimately unsupported (e.g. an experimental TraceQL-metrics function behind a flag) — list those explicitly with a reason, not a blanket skip.

> **If Slice 3 is unlanded:** this task still lands the *gate wiring + report*, pointed at whatever subset of the corpus the harness currently passes, with a `KNOWN_UNSUPPORTED: &[(&str, &str)]` list (each entry justified — e.g. the negated/union structural forms or a TraceQL-metrics function not yet implemented). The gate then enforces "no regression below the current line", and later work shrinks the list to empty. Flag this as a Contract gap if applicable.

- [x] **Step 1: Write the failing test**

```rust
// crates/traces/tests/traceql_conformance.rs  (or crabka-traceql/tests/)
use assert2::assert;

/// Cases we knowingly don't pass yet (each MUST carry a reason).
const KNOWN_UNSUPPORTED: &[(&str, &str)] = &[
    // ("metrics_quantile_over_time.case", "experimental TraceQL-metrics fn, feature-gated"),
];

#[test]
fn full_traceql_golden_corpus_passes() {
    let report = crabka_traceql::testkit::run_corpus_dir(
        "tests/testdata/traceql", // the Slice-3 curated golden set
    );
    report.write_to("target/traceql-conformance-report.txt").unwrap();
    let failing: Vec<_> = report
        .cases
        .iter()
        .filter(|c| !c.passed && !KNOWN_UNSUPPORTED.iter().any(|(n, _)| c.name.ends_with(n)))
        .collect();
    assert!(failing.is_empty(), "traceql golden regressions: {failing:?}");
}
```

- [x] **Step 2: Run to verify it fails (or passes if the harness is complete)**

Run: `cargo test -p crabka-traces --test traceql_conformance -- --nocapture` (or `-p crabka-traceql`).
Expected: FAIL — `run_corpus_dir`/`Report::write_to` missing, **or** a real conformance gap surfaces.

- [x] **Step 3: Implement the report API on the harness + the gate**

Add `run_corpus_dir(dir) -> Report` and `Report { cases: Vec<CaseResult { name, passed, passed_assertions, total_assertions }>, write_to(path) }` to the Slice 3 harness — the `pub mod testkit` in `crabka-traceql` that also hosts `run_golden_file` (small addition — per-case iteration + a text writer). Ensure the curated golden set covers selectors (scope/intrinsic/array semantics, the single-span rule), the core + negated + union structural operators, pipeline aggregations, TraceQL metrics, and the by-id path.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --test traceql_conformance -- --nocapture`
Expected: PASS; `target/traceql-conformance-report.txt` lists every case.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/ docs/
git commit -m "test(traces): TraceQL golden corpus conformance gate + per-file coverage report"
```

---

### Task 7: Shared differential corpus + JSON differ (`#[ignore]` infra, no container yet)

**Files:**
- Create: `crates/traces/tests/support/diff_corpus.rs` (path-included by Tasks 8/9)
- Modify: `crates/traces/Cargo.toml` (`[dev-dependencies]` `reqwest`, `serde_json`, `testcontainers`, `testcontainers-modules`, `opentelemetry-proto`)

**Interfaces:**
- Produces (pure, Docker-free, so it can be unit-tested without containers):
  - `pub struct SeedTrace { trace_id: [u8;16], service: &'static str, root_name: &'static str, spans: Vec<SeedSpan> }` and `pub fn seed_dataset() -> Vec<SeedTrace>` — a deterministic dataset exercising a multi-level span tree (so descendant/child/sibling ops have something to match), a server↔client pair (so service-graph has an edge), an error span, an event, a link, and an array attribute. **Timestamps are fixed wall-clock constants** so by-id + search are deterministic across both backends.
  - `pub fn to_otlp(traces: &[SeedTrace]) -> OtlpTracesPayload` — build the OTLP `ExportTraceServiceRequest` (via `opentelemetry-proto`) once, so the *identical* bytes go to both Tempo and Crabka.
  - `pub fn search_corpus() -> Vec<SearchCase>` where `SearchCase { name, traceql: &'static str }` — a representative TraceQL corpus: a bare attribute selector, an intrinsic (`span:status = error`), a descendant `{...} >> {...}`, a child `{...} > {...}`, a sibling `{...} ~ {...}`, a negated form, a pipeline `| count()`, a `| by(...)`, and a TraceQL-metrics `| rate()`.
  - `pub fn by_id_corpus() -> Vec<[u8;16]>` — the seed `trace_id`s, for the by-id equality check.
  - `pub fn normalize_search(resp: &serde_json::Value) -> serde_json::Value` — canonicalize a Tempo `/api/search` response: sort `traces` by `traceID`, sort each `spanSets[].spans` by `spanID`, **drop the volatile `metrics` object** (`inspectedTraces`/`inspectedBytes`/`totalBlocks` differ between engines), drop volatile timing fields.
  - `pub fn normalize_trace(resp: &serde_json::Value) -> serde_json::Value` — canonicalize a `/api/v2/traces/{id}` response: sort `resourceSpans` and nested `spans` deterministically (by `spanID`), normalize attribute ordering; keep `status` (`COMPLETE`/`PARTIAL`) since that is semantic.
  - `pub fn assert_trace_query_equal(name: &str, a: &serde_json::Value, b: &serde_json::Value)` — `normalize_*` both, assert structural equality, on mismatch print a unified-ish diff naming the case.
- A **self-test** (Docker-free) proving the normalizers behave: two search responses that differ only in `traces`/`spans` order (and in the dropped `metrics` object) compare **equal**; a genuine root-name/span-set difference compares **unequal**.

- [x] **Step 1: Write the failing self-test**

Create `crates/traces/tests/diff_corpus_selftest.rs`:

```rust
#[path = "support/diff_corpus.rs"]
mod diff_corpus;
use assert2::{assert, check};
use diff_corpus::*;
use serde_json::json;

#[test]
fn normalize_search_is_order_and_metrics_insensitive() {
    let a = json!({"traces":[
        {"traceID":"02","rootServiceName":"s","rootTraceName":"b","spanSets":[]},
        {"traceID":"01","rootServiceName":"s","rootTraceName":"a","spanSets":[]}],
        "metrics":{"inspectedTraces":7,"inspectedBytes":999}});
    let b = json!({"traces":[
        {"traceID":"01","rootServiceName":"s","rootTraceName":"a","spanSets":[]},
        {"traceID":"02","rootServiceName":"s","rootTraceName":"b","spanSets":[]}],
        "metrics":{"inspectedTraces":3,"inspectedBytes":12}});
    check!(normalize_search(&a) == normalize_search(&b)); // order + metrics dropped
}

#[test]
fn real_search_difference_is_detected() {
    let a = json!({"traces":[{"traceID":"01","rootTraceName":"a","spanSets":[]}]});
    let b = json!({"traces":[{"traceID":"01","rootTraceName":"DIFFERENT","spanSets":[]}]});
    check!(normalize_search(&a) != normalize_search(&b));
}

#[test]
fn corpus_is_nonempty_and_covers_key_operators() {
    let q = search_corpus();
    check!(q.iter().any(|c| c.traceql.contains(">>")));   // descendant
    check!(q.iter().any(|c| c.traceql.contains("~")));    // sibling
    check!(q.iter().any(|c| c.traceql.contains("count(")));
    assert!(!seed_dataset().is_empty());
    assert!(!by_id_corpus().is_empty());
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-traces --test diff_corpus_selftest`
Expected: FAIL — `diff_corpus` module absent.

- [x] **Step 3: Implement `support/diff_corpus.rs`**

Implement the seed dataset, `to_otlp`, the search/by-id corpora, both normalizers, and `assert_trace_query_equal`. Drop the `metrics` object and volatile timing in `normalize_search`; sort deterministically.

- [x] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-traces --test diff_corpus_selftest`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "test(traces): shared differential OTLP corpus + Tempo JSON differ (Docker-free)"
```

---

### Task 8: **Headline** — differential vs real Tempo (testcontainers, `#[ignore]`)

**Files:**
- Create: `crates/traces/tests/diff_tempo.rs`

**Interfaces:**
- Consumes: Task 5 in-process Crabka server; Task 7 corpus + differ; `testcontainers` `mirror.gcr.io/grafana/tempo`.
- Produces (`#[ignore = "requires Docker"]`) — the headline external test:
  - Boot Crabka in-process; start `mirror.gcr.io/grafana/tempo:<pinned tag>` in **monolithic single-binary mode** (`-target=all`, filesystem/local blocks backend, a mounted `tempo.yaml`); push the **identical** OTLP `ExportTraceServiceRequest` bytes (`to_otlp(seed_dataset())`) to **both** via `POST /v1/traces` (`X-Scope-OrgID: diff`); run `search_corpus()` against both `/api/search` and `by_id_corpus()` against both `/api/v2/traces/{id}`; `assert_trace_query_equal` per case.

> **Harness structure & data loading (explicit, since this is a Docker suite):**
> - **Container:** `GenericImage::new("mirror.gcr.io/grafana/tempo", "<pinned tag>")` with cmd `-target=all -config.file=/etc/tempo.yaml`, a bind-mounted minimal `tempo.yaml` (`storage.trace.backend: local`, a short `block.flush_check_period`/`complete_block_timeout` so blocks flush fast, OTLP receiver on `4318`, a fixed single tenant). `WaitFor` on Tempo's `/ready` (poll the mapped HTTP port until `200`). Map the HTTP `3200` and OTLP `4318` ports.
> - **Data load:** build the OTLP `ExportTraceServiceRequest` once via `to_otlp(seed_dataset())`, serialize to protobuf, `POST /v1/traces` (`Content-Type: application/x-protobuf`, `X-Scope-OrgID: diff`) to **both** Tempo (`http://localhost:<mapped 4318>/v1/traces`) and Crabka — identical bytes, two destinations — guaranteeing identical input.
> - **Settle / flush:** both engines serve recent traces from the hot tier immediately; for cases that must read from a flushed block, either keep the corpus within the live-store/ingester window (simplest, deterministic — **default**) or poll until a by-id query returns the trace on both sides (bounded, ~15s, mirroring the `client-core` bootstrap-retry pattern). Do not assume instantaneous visibility.
> - **Assert:** for each `SearchCase`, fetch from both, `assert_trace_query_equal(case.name, crabka_json, tempo_json)`. For each `by_id` trace, fetch `/api/v2/traces/{id}` from both and `assert_trace_query_equal`. A `TEMPO_KNOWN_DIVERGENCE: &[(&str,&str)]` list (each entry justified) covers any Tempo-specific volatile metadata not already dropped by `normalize_*` (e.g. Tempo's `rootServiceName` for a root-less partial trace, or a `metrics` field shape).
> - **Documented limitation:** TraceQL-metrics (`| rate()`) result parity is asserted only if both sides expose the same Prometheus-shaped series JSON on `/api/metrics/query_range`; otherwise that case is in `TEMPO_KNOWN_DIVERGENCE` with a reason. The by-id + search-spanSets equality is the firm headline.
> - **Why headline:** Tempo is the system Crabka claims to replace; corpus equality over identical OTLP input is the strongest single correctness signal in the slice. Keep this the most carefully curated corpus. **Also pin the real Tempo error-body strings here** (push an oversized trace / out-of-range `limit` against the container, capture the exact 4xx body, and feed those literals back into Task 1's `LimitError::message()` + Task 4's assertions) — this closes the "verify-against-Tempo" notes left in Tasks 1/4.

- [x] **Step 1: Write the `#[ignore]` test + mount config**

Create `crates/traces/tests/diff_tempo.rs` (embed the minimal `tempo.yaml` as a string written to a `tempfile` and bind-mounted). `#[tokio::test]` + `#[ignore = "requires Docker"]`.

- [x] **Step 2: Run to verify ignored-by-default + runnable**

Run (default): `cargo test -p crabka-traces --test diff_tempo` → reports `0 run, 1 ignored`.
Run (with Docker): `cargo test -p crabka-traces --test diff_tempo -- --ignored --nocapture` → PASS (or surfaces a real divergence to fix).

- [x] **Step 3: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "test(traces): headline differential vs real Tempo (ignored, testcontainers)"
```

---

### Task 9: Grafana integration — Tempo datasource + Service-Graph end-to-end (`#[ignore]`)

**Files:**
- Create: `crates/traces/tests/grafana_integration.rs`

**Interfaces:**
- Consumes: Task 5 in-process Crabka (querier + Tempo HTTP API); Task 7 seed dataset; `testcontainers` `mirror.gcr.io/grafana/grafana`. For the Service-Graph leg, Crabka's **metrics backend** (the metrics signal's querier) holding the `traces_service_graph_*` series the Slice 7 metrics-generator emitted — boot a minimal metrics querier in-process (or assert the series via the metrics signal's in-process server if available; flag a Contract gap if Slice 7 / the metrics querier is unlanded and assert only the Tempo-datasource legs).
- Produces (`#[ignore = "requires Docker"]`):
  - Boot Crabka in-process (seed `seed_dataset()`); start `mirror.gcr.io/grafana/grafana:<pinned tag>` with a **provisioned built-in Tempo datasource** whose `url` points at the Crabka Tempo-API base URL, plus (for Service Graph) a **provisioned Prometheus datasource** pointing at Crabka's metrics querier. Drive Grafana's datasource **proxy/Explore query API** for each leg and assert each renders.

> **Harness structure & data loading (explicit):**
> - **Datasource provisioning:** mount a `datasources.yaml` (`apiVersion: 1`) with (a) a `tempo` datasource (`url: http://host.docker.internal:<crabka_tempo_port>`, a fixed `uid`, `httpHeaderName1: X-Scope-OrgID` / `httpHeaderValue1: grafana`), and (b) a `prometheus` datasource (`url: http://host.docker.internal:<crabka_metrics_port>`, the same tenant header) so the Service Graph can read `traces_service_graph_*`. Set `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Admin` so the test calls the API without login.
> - **Host reach:** start Crabka bound to `0.0.0.0` and pass the container `--add-host=host.docker.internal:host-gateway` (Linux) so the datasource URLs resolve; document this platform-specific knob.
> - **Drive — the five legs Grafana's Tempo datasource exercises (spec §8):**
>   1. **Echo / health:** `GET` the Tempo datasource health (Grafana hits `/api/echo` → expect `200 "echo"`).
>   2. **Trace view (by-id):** `POST /api/ds/query` with a Tempo `traceql`/`traceId` query for a seeded `trace_id`; assert the returned frame has the trace's spans.
>   3. **Search (tags):** a `nativeSearch`/`tags=` query; assert non-empty `traces` frames.
>   4. **TraceQL:** a `traceql` query (`{ span:status = error }`); assert it returns the seeded error span.
>   5. **Service Graph:** `POST /api/ds/query` against the **Prometheus** datasource for `traces_service_graph_request_total`; assert the seed's client↔server pair produced an edge series (non-empty frames). This is the loop-closing leg — traces → metrics-generator → metrics backend → Grafana.
> - **Scope:** one query per leg is sufficient; this is an integration smoke proving the full Grafana → datasource → Crabka path renders (the spec's "assert they render"), not a second differential corpus.

- [x] **Step 1: Write the `#[ignore]` test**

Create `crates/traces/tests/grafana_integration.rs` with the provisioning + the five drive legs above; `#[tokio::test]` + `#[ignore = "requires Docker"]`. If the metrics querier / Slice 7 generator is unlanded, gate leg 5 behind a Contract-gap `cfg`/early-return with a logged note and keep legs 1–4 asserting.

- [x] **Step 2: Run to verify ignored + runnable**

Run (default): `cargo test -p crabka-traces --test grafana_integration` → `0 run, 1 ignored`.
Run (Docker): `cargo test -p crabka-traces --test grafana_integration -- --ignored --nocapture` → PASS.

- [x] **Step 3: Commit**

```bash
cargo fmt -p crabka-traces
cargo clippy -p crabka-traces --all-targets
git add crates/traces/
git commit -m "test(traces): Grafana built-in Tempo datasource + Service-Graph integration (ignored, Docker)"
```

---

### Task 10: CI wiring — `traces-differential` job + conformance gate; final whole-crate gate

**Files:**
- Modify: the CI workflow (`.github/workflows/*.yml`) — add two jobs.
- Modify: `docs/` (a short note in the slice's section of the plan dir if the repo tracks a CI matrix; otherwise none).

**Interfaces:**
- Produces:
  - A **`traces-conformance`** CI job (Linux): `cargo test -p crabka-traces --test traceql_conformance` (and the `crabka-traceql` harness tests) — runs on every PR as a **gate**; uploads `target/traceql-conformance-report.txt` as an artifact.
  - A **`traces-differential`** CI job (Linux, Docker available): `cargo test -p crabka-traces -- --ignored` scoped to the two Docker suites (`diff_tempo`, `grafana_integration`) — runs on a schedule + on-demand label (not every PR, to keep PR latency low), mirroring how the repo gates other Docker-heavy suites (`client-core-integration`). Document that these never run in the default `cargo test --workspace`.

- [x] **Step 1: Add the two CI jobs**

Add `traces-conformance` (every PR) and `traces-differential` (scheduled/labeled, Docker-enabled runner) to the workflow, following the existing job patterns (toolchain, cache, `--ignored` invocation).

- [x] **Step 2: Verify locally**

Run the default suite (no Docker): `cargo test -p crabka-traces` → all non-ignored pass, the two Docker suites report ignored.
Run the gate: `cargo test -p crabka-traces --test traceql_conformance` → PASS.

- [x] **Step 3: Final whole-crate gate**

Run: `cargo test -p crabka-traces && cargo clippy -p crabka-traces --all-targets && cargo fmt -p crabka-traces --check`
Expected: all PASS, no warnings, formatting clean. (Docker suites remain ignored.)

- [x] **Step 4: Commit**

```bash
cargo fmt -p crabka-traces
git add .github/ crates/traces/ docs/
git commit -m "ci(traces): conformance gate + dedicated traces-differential Docker job"
```

---

## Self-review

**Spec coverage (against §8 API, §9 limits/multi-tenancy, §10 testing, §11 Slice 8):**
- Per-tenant limits/quotas (ingest rate, max spans-per-trace, max attribute size, traces-per-search, max search duration) on the token-bucket where it fits → Tasks 1–3, enforced live in Task 4. Token-bucket reused for the *rate* limit only (Task 3 note); count/length caps are plain comparisons (correct — a bucket would be wrong for a one-shot cap).
- Per-tenant overrides YAML (Tempo `overrides.yaml`) → Task 2.
- **Tempo-shaped** errors `429`/`400` → Task 1 (`LimitError` status/message) + Task 4 (end-to-end body assertions), with the explicit note that Tempo uses 400 (not Prometheus's 422) for size/range caps — a deliberate divergence from metrics slice 8, verified against the real container in Task 8.
- Multi-tenancy isolation (by-id/search/tags/values/TraceQL-select/quota; tenant-prefixed keys) **end-to-end via two `X-Scope-OrgID`s** → Task 5 (**headline**), with a *colliding-`trace_id`* probe that forces the by-id bloom path to be tenant-scoped before the bloom test, and the quota-per-tenant assertion in Step 4.
- TraceQL conformance gate + per-file coverage report in CI → Tasks 6 + 10 (the curated golden set from Slice 3, since the spec §10 notes there is no upstream TraceQL `.test` corpus).
- Differential vs real Tempo (testcontainers) **as the equality headline** → Task 8 (`#[ignore]`), feeding identical OTLP bytes to both (one-push-two-destinations).
- Grafana integration (built-in Tempo datasource → Crabka, all five legs: echo, trace view, Search, TraceQL, **Service-Graph via the metrics backend**) → Task 9 (`#[ignore]`).
- Dedicated CI job for the Docker/external suites; default `cargo test` never touches Docker → Task 10.

**Headlines are explicit and end-to-end.** Tenant isolation (Task 5) drives **real HTTP over a real socket** with two org IDs across *every* read surface — and uses *identical `trace_id` bytes* in both tenants so the assertion can only pass if isolation happens before the bloom/row-group lookup, not after — plus per-tenant quota. Differential-vs-Tempo (Task 8) feeds **identical OTLP push bytes** to both systems (one-push-two-destinations) and asserts by-id + search-spanSet equality through `normalize_*`/`assert_trace_query_equal`. Both are called out as headline in their task titles and the spec-coverage list.

**`#[ignore]` discipline.** Every external-system test (Tasks 8/9) is `#[ignore = "requires Docker"]`, each task's run-step verifies `0 run, N ignored` by default and `--ignored` under Docker, and Task 10 isolates them in a `traces-differential` job off the PR path. The Docker-free differ self-test (Task 7) and the in-process isolation/limits/conformance tests (Tasks 4/5/6) run in the default suite, so the meat of the slice has fast CI coverage without Docker.

**Contract consumption, not re-derivation.** The querier Tempo HTTP API, `TraceqlEngine`/`SpanStore`/`CrabkaSpanStore`, the `tenant_of` resolver, distributor write hook, `SpanRecord`, the Slice 7 service-graph series, and the broker `TokenBucket` are all consumed from earlier slices/crates via the Shared Contract table; each task names the exact item and carries a "Contract gap" fallback (a minimal in-memory impl, never a silent stub) for the case where the dependency slice is unlanded — including the Slice-7-unlanded path that gates Task 9's Service-Graph leg. No new TraceQL or storage semantics are invented here — this is a hardening band.

**No-back-compat respected.** No `#[serde(default)]`-as-compat-shim, no version variants, no migration. The single `#[serde(default)]` use (Task 2, `PartialLimits`) is the *partial-config mechanism* (a tenant overrides only named fields), explicitly distinguished in-task from a compat shim, with a code comment so a reviewer doesn't misflag it.

**Placeholder scan.** Every in-process task (1–7) has a failing-test → run-fails-with-expected → real-code → run-passes → commit cycle with concrete `cargo test -p crabka-traces …` commands and assert2 assertions. The Docker tasks (8/9), where literal code would be guesswork against live container behavior, instead provide a **fully specified harness structure** — exact image/tag knobs, the one-push-two-destinations data-load mechanism, the settle/flush strategy, the per-leg drive, the assertion, and an explicit known-divergence list — which is the honest level of detail for a black-box external suite, not a placeholder. The two deferred string-literal pins (Tempo error-body messages in Tasks 1/4) are explicitly closed by the Task 8 container run rather than fabricated.

**Type/name consistency.** The `Limits` field set is identical across Tasks 1/2/3/4 and every test (`ingestion_rate_spans_per_sec`, `ingestion_burst_spans`, `max_traces_per_search`, `max_spans_per_trace`, `max_attribute_bytes`, `max_search_duration_secs`). `LimitError` variants and their `http_status` mapping are defined once (Task 1) and asserted unchanged in Tasks 3/4. The `IngestEnforcer`/`QueryEnforcer` method set is consistent between Tasks 3 and 4. The `support::traces_server` and `support::diff_corpus` helper signatures are fixed in Tasks 5/7 and consumed unchanged in Tasks 4/8/9.

**Known risks (flagged).** (1) The `crabka-broker` path dep for `TokenBucket` could introduce a heavy/cyclic dependency into `crabka-traces`; Task 3's note gives the escape hatch (lift `bucket.rs` into a tiny `crabka-throttle` crate) and prefers the path dep unless a cycle appears. (2) `TokenBucket`'s burst-vs-refill coupling (`set_rate` resets `available` to the rate) means the spans/sec-vs-burst split is approximate; Task 3 documents seeding at `max(rate, burst)` and notes it. (3) Docker-host reachability for Grafana (Task 9) is platform-specific (`host.docker.internal` + `--add-host`); flagged with the exact knob. (4) Tempo internal-metadata + volatile `metrics` noise (Task 8) is contained by `normalize_*` + an explicit `TEMPO_KNOWN_DIVERGENCE` list rather than loosening the differ. (5) The Tempo status map (429 rate, 400 size/range) **differs** from the metrics slice's (429/422/400); called out in Task 1 and verified live in Task 8 so the divergence is intentional and tested.
