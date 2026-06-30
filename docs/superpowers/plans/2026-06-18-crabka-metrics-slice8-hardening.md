# crabka-metrics Slice 8 — Hardening (multi-tenancy/limits, remote_read, cardinality, conformance + differential-vs-Mimir)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the metrics backend production-faithful at its multi-tenant edges and prove it against the real ecosystem. Add per-tenant limits/quotas (ingestion rate, active series, label/sample/range caps) with Prometheus-shaped errors and a YAML runtime-overrides file; harden tenant isolation so org A can never observe org B; implement `remote_read` (`POST /api/v1/read`); add the three Mimir cardinality APIs; wire the PromQL `.test` corpus as a CI gate with a per-file coverage report; and build the three external-system differential suites — `prometheus/compliance`, **differential-vs-real-Mimir**, and **Grafana**. The two headline tests are (1) end-to-end tenant isolation through the HTTP API with two `X-Scope-OrgID`s and (2) query-corpus equality vs real Mimir over identically-ingested data.

**Architecture:** This slice adds **no new query semantics** — it is a hardening band around the Slice 4 distributor (ingest), the Slice 5 querier + Prometheus HTTP API, and the Slice 2/3 `PromqlEngine`. New code lives in three areas of `crabka-metrics`: (a) a `limits` module (a per-tenant `Limits` struct, a YAML `OverridesProvider` modeled on Mimir's `runtime.yaml`, and enforcement points wired into the distributor write path and the querier read path, reusing the broker's `TokenBucket` token-bucket — lifted into a shared `crabka-throttle` crate and given an independent-burst knob — for the *rate* limit); (b) a `wire::remote_read` module (Prometheus `ReadRequest`/`ReadResponse` protobuf + snappy-block, translating matchers+range into a `PromqlEngine` series query); (c) HTTP handlers for `/api/v1/read` and the three `/api/v1/cardinality/*` endpoints, which read from the same `Index` the querier uses. Tenant isolation is **not** a new mechanism — it is the assertion that every existing key (WAL partition key, block/index object key, HA-tracker key, quota bucket key, in-memory head map key) is already `(tenant, …)`-prefixed; this slice adds the tests that prove it and fixes any leak they expose. The external suites (`prometheus`, Mimir, Grafana) are black-box harnesses over the compiled HTTP server + Docker containers, all `#[ignore]`, run in a dedicated CI job.

**Tech Stack:** Rust 2024 · `arrow` 59 · `axum` 0.8 (handlers, reuse Slice 5 router) · `prost` 0.14 + `prost-build` (remote_read protobuf) · `snap` 1 (snappy-block) · `serde_yaml` 0.9 + `serde` (overrides file) · the broker `TokenBucket` (KIP-73, via a thin re-export) · `thiserror`. Tests: `assert2`; `reqwest` 0.13 + `tokio` for in-process HTTP drive; `testcontainers` 0.27 + `testcontainers-modules` 0.15 for the Docker differential suites; `serde_json` for response diffing.

## Global Constraints

- **No backwards compatibility.** Greenfield/undeployed. Change the `Limits` schema, the overrides YAML shape, the remote_read translation, and any error-body shape freely; no shims, no migration code, no `#[serde(default)]` "to keep old configs readable".
- **`unsafe_code = "forbid"`** workspace-wide. No `unsafe`.
- **Lints:** `clippy::pedantic` is `warn`. New code clippy-pedantic clean. Run `cargo clippy -p crabka-metrics --all-targets` before each commit.
- **Formatting:** `cargo fmt -p crabka-metrics` before every commit (never `cargo +nightly fmt --all` — OS error 206 in deep worktrees on Windows; always `-p`).
- **Assertions:** `assert2::assert!` / `assert2::check!` in tests.
- **Kafka wire compat is the only external contract that must not drift.** This slice touches no Kafka bytes. The Prometheus/Mimir HTTP byte-exactness is the *analog* constraint here: status codes and `data.resultType` shapes must match Prometheus/Mimir exactly (that is what the differential suites verify). **Error-body shape is per-surface**, not uniform: the **query API** (`/query`, `/query_range`, `/series`) returns the Prometheus JSON `errorType` envelope; the **push API** (`/api/v1/push`) returns Mimir's **plain-text `err-mimir-*` bodies** (no JSON `errorType`). Match the right shape for each surface (Task 7) — do not assert the query envelope on the push path.
- **Docker/external-system tests are `#[ignore]`.** Every test that needs a running Prometheus, Mimir, or Grafana container is annotated `#[ignore = "requires Docker"]` and lives behind the dedicated CI job (`metrics-differential`), never in the default `cargo test --workspace` path. Reuse the Confluent-image rationale and bootstrap-retry patterns from `crates/client-core/tests/integration.rs`.
- **Reuse, don't reinvent, the token bucket.** Per-tenant *rate* limits (ingestion rate) use the broker's `TokenBucket` semantics (KIP-73 `plan_consume`: `capped = min(available+refill, rate)`, `grant = min(requested, capped)`), lifted into a shared `crabka-throttle` crate and extended with `set_rate_with_burst(rate, burst)` so Mimir's independent `ingestion_burst_size` is honored (Task 3). Do not write a second rate limiter. The *count/length* limits (max active series, max label length, max samples-per-query, max series-per-query, max range) are plain comparisons, not buckets.

---

## Dependency & slice roadmap

**Depends on:**
- **Slice 1** (data layer) — `crabka-metrics` crate, block schemas, `SymbolTable`. ✅ planned.
- **Slice 2/3** (`crabka-promql`) — the `PromqlEngine`/query entry point this slice queries for `remote_read` and that the limits cap. **Consumed via contract** (see Shared Contract below); if Slice 3 is unlanded, the `remote_read` and `.test`-gate tasks stub the engine behind the documented trait and the task notes say so.
- **Slice 4** (ingest service) — the distributor write path (`validate → HA dedup → tenant-route → produce`) where ingestion-rate / series / label limits are enforced; the `Index` the cardinality APIs read.
- **Slice 5** (querier + Prometheus HTTP API) — the axum router, `X-Scope-OrgID` tenancy extractor, the Prometheus-shaped JSON response/error envelope, and the `Index`. **This slice extends that router** (adds `/api/v1/read`, `/cardinality/*`) and that error envelope (adds `429`/`422` limit errors).

**Shared Contract (consume, do not re-derive).** The following are assumed to exist from earlier slices; each task that touches them lists the exact item it consumes. If an item is missing because its slice is unlanded, the task creates a **minimal local trait/shim with a single `todo!()`-free in-memory impl for tests** and flags it in the task's "Contract gap" note — never a silent stub.

| Contract item | From | This slice consumes it as |
|---|---|---|
| `TenantId` (newtype over `X-Scope-OrgID`, default `"anonymous"`) + axum extractor | Slice 5 | the key prefix every limit/isolation test asserts on |
| `PromqlEngine` query entry: `query_instant`/`query_range` + a `series(matchers, start, end) -> Vec<Labels>` selector | Slice 2/3/5 | the body of `remote_read` + the source for the `.test` gate |
| `Index` with `label_names(tenant)`, `label_values(tenant, name)`, `series_for(tenant, matchers)` | Slice 4/5 | the source for the three cardinality APIs |
| axum `Router` + Prometheus JSON envelope (`ApiResponse::{success,error}`, `errorType`) | Slice 5 | extended with new routes + new error variants |
| distributor write path hook (per-request, per-tenant, before WAL append) | Slice 4 | the enforcement point for ingest limits |
| `Labels` / `SeriesFingerprint` | Slice 1/blockstore | label-length validation + isolation keys |

**The 8 metrics slices** (this plan = Slice 8, the last): 1 data layer · 2 promql core · 3 query completeness · 4 ingest · 5 querier+HTTP · 6 query-frontend · 7 ruler · **8 hardening (this plan)**.

---

## File structure (`crates/metrics/`)

| File | Responsibility |
|---|---|
| `src/limits/mod.rs` | `Limits` struct + `LimitError` (Prometheus-shaped) + module re-exports |
| `src/limits/overrides.rs` | `OverridesProvider` — load Mimir-style `runtime.yaml`, resolve per-tenant `Limits` (tenant override merged over defaults) |
| `src/limits/enforce.rs` | enforcement helpers: ingest-side (`check_ingest`) + query-side (`check_query`); ingestion-rate uses `TokenBucket` |
| `src/wire/remote_read.rs` | Prometheus `ReadRequest`/`ReadResponse` protobuf glue + snappy-block + matcher/range → query translation |
| `src/wire/prometheus.proto` | vendored Prometheus `remote.proto` + `types.proto` subset (remote_read messages) |
| `src/http/read.rs` | `POST /api/v1/read` axum handler |
| `src/http/cardinality.rs` | `/api/v1/cardinality/{label_names,label_values,active_series}` handlers |
| `build.rs` | `prost-build` compile of `prometheus.proto` (remote_read) |
| `tests/limits_overrides.rs` | unit/integration: YAML load + per-tenant resolution + enforcement decisions |
| `tests/tenant_isolation.rs` | **headline** — two-`X-Scope-OrgID` end-to-end isolation through the HTTP API (in-process, no Docker) |
| `tests/remote_read.rs` | remote_read protobuf round-trip + SAMPLES response correctness (in-process) |
| `tests/cardinality_api.rs` | cardinality endpoints over a seeded `Index` (in-process) |
| `tests/promql_conformance.rs` | the `.test` corpus gate + per-file pass/fail coverage report |
| `tests/diff_prometheus.rs` | `#[ignore]` prometheus/compliance black-box harness vs reference Prometheus |
| `tests/diff_mimir.rs` | `#[ignore]` **headline** differential vs real Mimir (testcontainers) |
| `tests/grafana_integration.rs` | `#[ignore]` Grafana + built-in Prometheus datasource → Crabka |
| `tests/support/metrics_server.rs` | shared in-process server boot + two-tenant seed helpers (path-included by the integration tests) |
| `tests/support/diff_corpus.rs` | the shared query corpus + a `assert_query_equal` JSON differ (path-included by the Docker suites) |

---

### Task 1: `Limits` model + Prometheus-shaped `LimitError`

**Files:**
- Create: `crates/metrics/src/limits/mod.rs`
- Modify: `crates/metrics/src/lib.rs` (add `pub mod limits;`)
- Modify: `crates/metrics/Cargo.toml` (add `serde` with `derive`)

**Interfaces:**
- Produces:
  - `struct Limits` (`Clone`, `Debug`, `PartialEq`, `serde::Deserialize`, `serde::Serialize`) with fields, all Mimir-named:
    - `ingestion_rate: f64` (samples/sec; `0.0` ⇒ unlimited)
    - `ingestion_burst_size: u64`
    - `max_global_series_per_user: u64` (active series per tenant; `0` ⇒ unlimited)
    - `max_label_name_length: u64`, `max_label_value_length: u64`
    - `max_fetched_chunk_bytes_per_query` *(out of scope — omit)*; instead: `max_samples_per_query: u64`, `max_fetched_series_per_query: u64`
    - `max_query_lookback: Duration` (serde via humantime-ish secs — see note), `max_query_length: Duration` (max range span)
  - `impl Default for Limits` — generous Mimir-default-ish values (e.g. `ingestion_rate: 10_000.0`, `max_global_series_per_user: 150_000`, `max_label_name_length: 1024`, `max_label_value_length: 2048`, `max_samples_per_query: 50_000_000`, `max_fetched_series_per_query: 100_000`, `max_query_lookback: 0` ⇒ unlimited, `max_query_length: 0` ⇒ unlimited).
  - `enum LimitError` (`thiserror`) with the variants below, each carrying the over-limit value + the cap, and each mapping to a Prometheus status:
    - `IngestionRateExceeded` → **429**
    - `MaxSeriesPerUser` → **400** (`bad_data`; Mimir's per-user series limit is a non-retriable validation error returned as HTTP 400, *not* 429 — only the ingestion-rate/request-rate limits are 429)
    - `LabelNameTooLong` / `LabelValueTooLong` → **400**
    - `SamplesPerQueryExceeded` / `SeriesPerQueryExceeded` → **422** (`execution`)
    - `QueryLookbackExceeded` / `QueryRangeTooLong` → **422**
  - `impl LimitError { pub fn http_status(&self) -> u16; pub fn error_type(&self) -> &'static str; pub fn message(&self) -> String }` — `error_type` is the Prometheus `errorType` (`"bad_data"` for 400/422 client errs, `"execution"` for the query caps; match Prometheus: 422 carries `errorType:"execution"`, 400 carries `errorType:"bad_data"`).

> **Duration serde note:** store the two `Duration` caps as `u64` seconds in the struct (`max_query_lookback_secs`, `max_query_length_secs`) to keep `serde_yaml` trivial and `PartialEq` exact; expose `fn max_query_lookback(&self) -> Duration` accessors. `0` means "unlimited". This avoids a humantime dep and keeps the YAML readable (`max_query_length: 86400`).

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/limits/mod.rs` with only a `tests` module:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn default_limits_are_generous_and_finite() {
        let l = Limits::default();
        assert!(l.ingestion_rate > 0.0);
        assert!(l.max_global_series_per_user >= 100_000);
        assert!(l.max_label_name_length == 1024);
    }

    #[test]
    fn limit_errors_carry_prometheus_status_and_type() {
        let rate = LimitError::IngestionRateExceeded { rate: 10_000.0, observed: 12_000.0 };
        assert!(rate.http_status() == 429);

        let series = LimitError::SeriesPerQueryExceeded { limit: 100, observed: 101 };
        assert!(series.http_status() == 422);
        assert!(series.error_type() == "execution");

        let label = LimitError::LabelValueTooLong { limit: 2048, observed: 5000 };
        assert!(label.http_status() == 400);
        assert!(label.error_type() == "bad_data");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib limits`
Expected: FAIL — `cannot find type Limits`.

- [ ] **Step 3: Implement `Limits` + `LimitError`**

Prepend above `tests`. Define `Limits` with the fields/`Default` above (using the `_secs` duration form), and `LimitError` with `thiserror`, the carried fields, and the three `impl` methods. Map statuses exactly: `IngestionRateExceeded` ⇒ 429; `MaxSeriesPerUser`/`LabelNameTooLong`/`LabelValueTooLong` ⇒ 400 (`bad_data`); the four query caps ⇒ 422 (`execution`). Add `serde` (with `derive`) to `Cargo.toml`.

- [ ] **Step 4: Wire into `lib.rs`** — `pub mod limits;` and `pub use limits::{Limits, LimitError};`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib limits`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): per-tenant Limits model + Prometheus-shaped LimitError"
```

---

### Task 2: `OverridesProvider` — Mimir-style `runtime.yaml`

**Files:**
- Create: `crates/metrics/src/limits/overrides.rs`
- Modify: `crates/metrics/src/limits/mod.rs` (declare submodule + re-export)

**Interfaces:**
- Consumes: `Limits` (Task 1), `serde_yaml`.
- Produces:
  - `struct OverridesProvider { defaults: Limits, per_tenant: HashMap<String, Limits> }` (`Clone`, `Debug`).
  - `impl OverridesProvider`:
    - `pub fn new(defaults: Limits) -> Self`
    - `pub fn from_yaml(yaml: &str) -> Result<Self, OverridesError>` — parse the Mimir runtime shape (top-level `overrides: { <tenant>: { …partial Limits… } }`, plus an optional top-level `defaults:`); a tenant's entry overrides only the fields it names, the rest fall back to `defaults`.
    - `pub fn for_tenant(&self, tenant: &str) -> &Limits` — returns the tenant's resolved `Limits`, or `&self.defaults` if unlisted.
  - `enum OverridesError` (`thiserror`): `Yaml(String)`.

> **Mimir runtime parity:** Mimir's `runtime.yaml` keys limits under `overrides:` per-tenant and merges over the static config defaults. We model defaults as a struct (not a second YAML layer) and let each tenant's YAML map be a *partial* `Limits` via `#[serde(default)]` on an internal `PartialLimits` mirror that then merges field-by-field onto `defaults`. (The no-back-compat rule bans `#[serde(default)]` used as a *compat* shim for old logs/schemas; using it to express "this tenant only overrides some fields" is a legitimate partial-config pattern, not a compat shim — it is the mechanism, not a migration. Note this in a code comment so a future reader doesn't flag it.)

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/limits/overrides.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    const YAML: &str = r#"
overrides:
  tenant-a:
    ingestion_rate: 500
    max_global_series_per_user: 1000
  tenant-b:
    max_label_value_length: 64
"#;

    #[test]
    fn tenant_override_merges_over_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let a = p.for_tenant("tenant-a");
        assert!(a.ingestion_rate == 500.0);
        assert!(a.max_global_series_per_user == 1000);
        // unspecified field falls back to default
        assert!(a.max_label_name_length == Limits::default().max_label_name_length);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let b = p.for_tenant("tenant-b");
        assert!(b.max_label_value_length == 64);
        assert!(b.ingestion_rate == Limits::default().ingestion_rate);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(*p.for_tenant("tenant-z") == Limits::default());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib overrides`
Expected: FAIL — `cannot find type OverridesProvider`.

- [ ] **Step 3: Implement `overrides.rs`**

Define an internal `#[derive(Deserialize)] struct PartialLimits` with every field `Option<…>` (and `#[serde(default)]`), a `struct RuntimeFile { #[serde(default)] overrides: HashMap<String, PartialLimits> }`, `from_yaml` parsing into that and merging each `PartialLimits` onto a clone of `defaults` (a `fn merge(base: &Limits, p: &PartialLimits) -> Limits`). `for_tenant` returns the precomputed resolved `Limits`. Map `serde_yaml::Error` → `OverridesError::Yaml(e.to_string())`.

- [ ] **Step 4: Wire into `mod.rs`** — `mod overrides; pub use overrides::{OverridesError, OverridesProvider};` and re-export from `lib.rs`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib overrides`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): Mimir-style runtime.yaml OverridesProvider"
```

---

### Task 3: Limit enforcement — ingest side (rate + series + label length), query side (samples/series/range/lookback)

**Files:**
- Create: `crates/metrics/src/limits/enforce.rs`
- Modify: `crates/metrics/src/limits/mod.rs` (declare submodule + re-export)
- Modify: `crates/metrics/Cargo.toml` (add `crabka-throttle` path dep for `TokenBucket` — the lifted bucket crate, see note)
- Create: `crates/throttle/` (`crabka-throttle`) — lift `bucket.rs` (+ `plan_consume`) out of `crabka-broker`, add `set_rate_with_burst`; re-export from `crabka-broker` so the broker keeps `crabka_broker::throttle::TokenBucket`.

**Interfaces:**
- Consumes: `Limits`, `LimitError` (Task 1); `crabka_throttle::TokenBucket` (with `set_rate_with_burst(rate, burst)`); `Labels`.
- Produces:
  - `struct IngestEnforcer` holding a `DashMap<String /*tenant*/, Arc<TokenBucket>>` for the per-tenant ingestion-rate bucket and a `DashMap<String, u64>` (or an injected active-series counter) for active-series accounting.
  - `impl IngestEnforcer`:
    - `pub fn check_sample_rate(&self, limits: &Limits, tenant: &str, n_samples: u64) -> Result<(), LimitError>` — `0.0` rate ⇒ Ok; else get-or-create the tenant bucket at refill `ingestion_rate` (as `u64` samples/sec) with capacity `ingestion_burst_size` via `set_rate_with_burst(rate, burst)` (the burst is **independent** of the rate, matching Mimir), `try_consume(n_samples)`; if granted `< n_samples` ⇒ `IngestionRateExceeded`.
    - `pub fn check_active_series(&self, limits: &Limits, tenant: &str, would_add: u64, current: u64) -> Result<(), LimitError>` — `current + would_add > max_global_series_per_user` (when nonzero) ⇒ `MaxSeriesPerUser`.
    - `pub fn check_labels(limits: &Limits, labels: &Labels) -> Result<(), LimitError>` — any label name longer than `max_label_name_length` ⇒ `LabelNameTooLong`; value too long ⇒ `LabelValueTooLong`. (Associated fn — no state.)
  - `struct QueryEnforcer` (stateless; associated fns):
    - `pub fn check_range(limits: &Limits, start_ms: i64, end_ms: i64, now_ms: i64) -> Result<(), LimitError>` — `(end-start) > max_query_length` ⇒ `QueryRangeTooLong`; `(now-start) > max_query_lookback` ⇒ `QueryLookbackExceeded` (both only when the cap is nonzero).
    - `pub fn check_series_count(limits: &Limits, selected: u64) -> Result<(), LimitError>` — `> max_fetched_series_per_query` ⇒ `SeriesPerQueryExceeded`.
    - `pub fn check_sample_count(limits: &Limits, processed: u64) -> Result<(), LimitError>` — `> max_samples_per_query` ⇒ `SamplesPerQueryExceeded`.

> **TokenBucket reuse note:** `crabka_broker::throttle::TokenBucket` is the KIP-73 bucket (`new()`, `set_rate(u64)`, `try_consume(u64) -> u64` granted). It meters in whatever integer unit you set the rate in; here the unit is *samples* and refill rate = `ingestion_rate` rounded to `u64`. **Burst caveat:** the existing `set_rate(rate)` unconditionally seeds `available = rate` (`crates/broker/src/throttle/bucket.rs:47-51`, `self.available.store(new_rate)`) — there is **no** API to set the burst capacity independently of the rate, so a naive reuse would silently ignore `ingestion_burst_size` whenever `burst != rate`. Since Mimir's `ingestion_burst_size` **is** independent of `ingestion_rate`, this task must extend the bucket with a `set_rate_with_burst(rate, burst)` that seeds `available = burst` while keeping `rate` as the refill — and because that extension changes broker code, lift `bucket.rs` (+ `plan_consume`) into a tiny `crabka-throttle` crate first and depend on that from both `crabka-broker` and `crabka-metrics` (the note already contemplated this lift; the burst knob makes it required, not optional). The pure refill arithmetic (`plan_consume`) is already unit-tested in the broker, so this task tests the *mapping* (limit → bucket config → decision) **including a `burst != rate` case**, not the refill math.

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/limits/enforce.rs`:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::limits::Limits;

    fn limits_with(series: u64, name_len: u64, val_len: u64) -> Limits {
        Limits { max_global_series_per_user: series, max_label_name_length: name_len,
                 max_label_value_length: val_len, ..Limits::default() }
    }

    #[test]
    fn active_series_cap_rejects_over_limit() {
        let e = IngestEnforcer::new();
        let l = limits_with(100, 1024, 2048);
        assert!(e.check_active_series(&l, "t", 1, 99).is_ok());   // 99+1 == 100, ok
        assert!(e.check_active_series(&l, "t", 1, 100).is_err()); // 100+1 > 100
    }

    #[test]
    fn zero_series_cap_is_unlimited() {
        let e = IngestEnforcer::new();
        let l = limits_with(0, 1024, 2048);
        assert!(e.check_active_series(&l, "t", 1_000_000, 5_000_000).is_ok());
    }

    #[test]
    fn label_length_caps_enforced() {
        let l = limits_with(0, 4, 4);
        let ok = vec![("ab".to_string(), "cd".to_string())];
        let bad_name = vec![("toolong".to_string(), "x".to_string())];
        let bad_val = vec![("a".to_string(), "toolong".to_string())];
        assert!(IngestEnforcer::check_labels(&l, &ok).is_ok());
        assert!(matches!(IngestEnforcer::check_labels(&l, &bad_name),
                         Err(LimitError::LabelNameTooLong { .. })));
        assert!(matches!(IngestEnforcer::check_labels(&l, &bad_val),
                         Err(LimitError::LabelValueTooLong { .. })));
    }

    #[test]
    fn ingestion_rate_bucket_eventually_rejects() {
        let e = IngestEnforcer::new();
        let l = Limits { ingestion_rate: 100.0, ingestion_burst_size: 100, ..Limits::default() };
        // First burst within budget.
        assert!(e.check_sample_rate(&l, "t", 100).is_ok());
        // Immediately over: budget exhausted, no refill yet.
        assert!(e.check_sample_rate(&l, "t", 100).is_err());
    }

    #[test]
    fn ingestion_burst_is_independent_of_rate() {
        // burst (1000) > rate (100): the bucket must honor the larger burst capacity,
        // not silently clamp the initial budget to `rate`. (set_rate_with_burst, not set_rate.)
        let e = IngestEnforcer::new();
        let l = Limits { ingestion_rate: 100.0, ingestion_burst_size: 1000, ..Limits::default() };
        // A single 500-sample push fits the 1000 burst even though rate is only 100/s.
        assert!(e.check_sample_rate(&l, "t", 500).is_ok());
        // Drain the rest of the burst, then the next push (no refill yet) is rejected.
        assert!(e.check_sample_rate(&l, "t", 500).is_ok());
        assert!(e.check_sample_rate(&l, "t", 1).is_err());
    }

    #[test]
    fn query_range_and_lookback_caps() {
        let l = Limits { max_query_length_secs: 3600, max_query_lookback_secs: 86_400,
                         ..Limits::default() };
        let now = 1_000_000_000_000_i64;
        // 2h range > 1h cap
        assert!(matches!(QueryEnforcer::check_range(&l, now - 7_200_000, now, now),
                         Err(LimitError::QueryRangeTooLong { .. })));
        // start 2 days ago > 1 day lookback
        assert!(matches!(QueryEnforcer::check_range(&l, now - 172_800_000, now - 172_799_000, now),
                         Err(LimitError::QueryLookbackExceeded { .. })));
    }

    #[test]
    fn query_count_caps() {
        let l = Limits { max_fetched_series_per_query: 10, max_samples_per_query: 1000,
                         ..Limits::default() };
        assert!(QueryEnforcer::check_series_count(&l, 11).is_err());
        assert!(QueryEnforcer::check_sample_count(&l, 1001).is_err());
        assert!(QueryEnforcer::check_series_count(&l, 10).is_ok());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib enforce`
Expected: FAIL — `cannot find type IngestEnforcer`.

- [ ] **Step 3: Implement `enforce.rs`**

Implement `IngestEnforcer` (with `DashMap` bucket cache, `new()`), `QueryEnforcer`, and the seven check methods exactly per the interfaces. First lift `bucket.rs` into a `crabka-throttle` crate and add `set_rate_with_burst(rate, burst)` (seeds `available = burst`, keeps `rate` as refill). For `check_sample_rate`: round `ingestion_rate` to `u64`; get-or-create the tenant's `TokenBucket`, `set_rate_with_burst(rate, ingestion_burst_size)` on creation (seeds the independent burst); `try_consume(n)`. Add `crabka-throttle` (path) + `dashmap` to `Cargo.toml` dev/normal deps as needed.

- [ ] **Step 4: Wire into `mod.rs`** — `mod enforce; pub use enforce::{IngestEnforcer, QueryEnforcer};` + re-export from `lib.rs`.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib enforce`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): per-tenant limit enforcement (ingest rate/series/labels + query caps)"
```

---

### Task 4: remote_read protobuf + snappy-block (`POST /api/v1/read`)

**Files:**
- Create: `crates/metrics/src/wire/prometheus.proto` (vendored remote_read subset)
- Create: `crates/metrics/build.rs` (`prost-build`)
- Create: `crates/metrics/src/wire/remote_read.rs`
- Modify: `crates/metrics/src/lib.rs` (`pub mod wire;`)
- Modify: `crates/metrics/Cargo.toml` (add `prost`, `snap`, `[build-dependencies] prost-build`)

**Interfaces:**
- Consumes: the `PromqlEngine` `series(matchers, start_ms, end_ms)` selector + sample fetch (Shared Contract); `snap::raw::{Encoder,Decoder}` (snappy-block); generated `prost` types.
- Produces (in `remote_read.rs`):
  - The generated module include (`prometheus.rs`) exposing `ReadRequest`, `ReadResponse`, `Query`, `LabelMatcher` (with `r#type` ∈ EQ/NEQ/RE/NRE), `QueryResult`, `TimeSeries`, `Label`, `Sample`.
  - `pub fn decode_read_request(snappy_body: &[u8]) -> Result<ReadRequest, RemoteReadError>` — snappy-**block** decompress then `ReadRequest::decode`.
  - `pub fn encode_read_response(resp: &ReadResponse) -> Result<Vec<u8>, RemoteReadError>` — `prost::encode` then snappy-block compress.
  - `pub fn matchers_to_selectors(q: &Query) -> Result<(Vec<(MatchOp, String, String)>, i64, i64), RemoteReadError>` — translate proto `LabelMatcher`s + `start_timestamp_ms`/`end_timestamp_ms` into the engine's matcher form.
  - `pub fn series_to_timeseries(series: Vec<(Labels, Vec<(i64, f64)>)>) -> QueryResult` — assemble the SAMPLES-type response (sorted labels, sorted samples).
  - `enum RemoteReadError` (`thiserror`): `Snappy(String)`, `Decode(String)`, `UnsupportedMatcher`, `Engine(String)`.
- **Scope note (documented limitation):** implement the **SAMPLES** response path (`ReadResponse.results`, the v1 read format). `STREAMED_XOR_CHUNKS` (the `Accept-Encoding: …; chunked` Thanos/streaming path) is **out of scope for this slice** — emit SAMPLES only and document that Crabka advertises no `accepted_response_types` for chunked. This matches the spec's "(or at least SAMPLES with a documented limitation)".

> **Proto vendoring:** vendor the minimal `prometheus/remote.proto` + `types.proto` messages needed for remote_read (Apache-2.0; keep the license header + a `// vendored from prometheus/prometheus@<tag>` attribution line). `build.rs` runs `prost_build::compile_protos(&["src/wire/prometheus.proto"], &["src/wire"])`. Pin the same Prometheus tag the `.test` corpus pins (Task 8) so messages and conformance agree.

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/src/wire/remote_read.rs` with a `tests` module that builds a `ReadRequest` in code, encodes it (`encode` + snappy), decodes it back via `decode_read_request`, and asserts the matchers + time range survive; plus a `series_to_timeseries` test asserting label/sample ordering:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use prost::Message;

    use super::*;

    #[test]
    fn read_request_snappy_round_trips() {
        let req = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: vec![LabelMatcher {
                    r#type: label_matcher::Type::Eq as i32,
                    name: "__name__".into(),
                    value: "http_requests_total".into(),
                }],
                hints: None,
            }],
            accepted_response_types: vec![],
        };
        let raw = req.encode_to_vec();
        let snappy = snap::raw::Encoder::new().compress_vec(&raw).unwrap();
        let back = decode_read_request(&snappy).unwrap();
        assert!(back.queries.len() == 1);
        let (sel, start, end) = matchers_to_selectors(&back.queries[0]).unwrap();
        assert!(start == 1000 && end == 2000);
        assert!(sel[0].1 == "__name__" && sel[0].2 == "http_requests_total");
    }

    #[test]
    fn samples_response_is_sorted() {
        let series = vec![(
            vec![("__name__".to_string(), "x".to_string())],
            vec![(2_i64, 2.0_f64), (1_i64, 1.0_f64)],
        )];
        let qr = series_to_timeseries(series);
        let ts = &qr.timeseries[0];
        assert!(ts.samples[0].timestamp == 1); // sorted ascending
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --lib remote_read`
Expected: FAIL — proto module / `decode_read_request` missing (build.rs not yet present).

- [ ] **Step 3: Implement proto + build.rs + `remote_read.rs`**

Vendor `prometheus.proto`, add `build.rs`, implement the four functions + the error enum. Sort labels and samples in `series_to_timeseries`. snappy via `snap::raw`.

- [ ] **Step 4: Wire into `lib.rs`** — `pub mod wire;` + re-export `wire::remote_read` public items.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --lib remote_read`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): remote_read protobuf + snappy-block (SAMPLES path)"
```

---

### Task 5: `/api/v1/read` axum handler + `/api/v1/cardinality/*` handlers

**Files:**
- Create: `crates/metrics/src/http/read.rs`
- Create: `crates/metrics/src/http/cardinality.rs`
- Modify: the Slice 5 router registration module (add the four routes) — **flag a Contract gap if the Slice 5 router isn't present**; otherwise extend it.
- Modify: `crates/metrics/src/lib.rs` / `http/mod.rs`

**Interfaces:**
- Consumes: Slice 5's axum `Router`, `TenantId` extractor, `ApiResponse` envelope; the `PromqlEngine` series+sample fetch; the `Index` (`label_names`, `label_values`, `series_for`); Task 4's `remote_read` functions; Task 3's `QueryEnforcer` (apply `check_range`/`check_series_count` inside the read handler).
- Produces:
  - `read.rs`: `async fn remote_read(tenant, body) -> Response` — read raw body, `decode_read_request`, for each `Query`: `matchers_to_selectors` → `QueryEnforcer::check_range` (tenant limits) → engine `series` fetch (tenant-scoped) → `series_to_timeseries` → assemble `ReadResponse` → `encode_read_response` → respond `200` with `Content-Type: application/x-protobuf`, `Content-Encoding: snappy`. Limit/decode errors map to the Prometheus error envelope (`422`/`400`).
  - `cardinality.rs`: three handlers returning Mimir's JSON shapes:
    - `GET /api/v1/cardinality/label_names` → `{ "label_values_count_total": N, "label_names_count": M, "cardinality": [ {"label_name": …, "label_values_count": …}, … ] }` (top-N by `label_values_count`, honoring `?limit=`).
    - `GET /api/v1/cardinality/label_values?label_names=foo` → Mimir's nested shape: `{ "series_count_total": N, "labels": [ {"label_name":"foo","label_values_count":…,"series_count":…,"cardinality":[ {"label_value":…,"series_count":…}, … ]}, … ] }` (top-level `series_count_total` + per-label `labels[]`, each carrying its own nested `cardinality[]` of `{label_value, series_count}`).
    - `GET /api/v1/cardinality/active_series?selector=…` → `{ "data": [ {"__name__":"up","job":"…", …}, … ] }` (Mimir's active-series shape: each array element is the label map **directly** — no `"metric"` wrapper — and there is **no** `"status"` field; series from `Index::series_for`, tenant-scoped).
- **Tenancy:** every handler resolves `tenant` from the `TenantId` extractor and passes it to every `Index`/engine call. The isolation test (Task 6) is what proves these are wired right.

- [ ] **Step 1: Write the failing test** (in-process, no Docker)

Create `crates/metrics/tests/cardinality_api.rs` that boots the in-process server (Task 6's `support::metrics_server`), seeds one tenant with a handful of series across two metric names, hits `/api/v1/cardinality/label_names`, `/label_values?label_names=__name__`, and `/active_series?selector=…`, and asserts the counts + JSON shape against Mimir's exact shapes: for `label_values` assert on the top-level `series_count_total` and the nested `labels[].cardinality[]` (`{label_value, series_count}`); for `active_series` assert each element of `data[]` is a flat label map (e.g. `data[0]["__name__"]`) with no `metric` key and no `status` field. Also `tests/remote_read.rs` that POSTs a snappy `ReadRequest` for a seeded metric and asserts the decoded `ReadResponse` has the expected samples.

(These depend on the `support::metrics_server` boot helper — write that helper first in Task 6 Step 1, or stub a minimal boot here and converge. Cross-reference noted.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test cardinality_api`
Expected: FAIL — routes 404 / handlers absent.

- [ ] **Step 3: Implement the handlers + register routes**

Implement `read.rs` and `cardinality.rs`; register all four routes on the Slice 5 router (both `/api/v1/` and `/prometheus/api/v1/` prefixes, matching the spec §8). Wire `QueryEnforcer` into the read path.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test cardinality_api --test remote_read`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): /api/v1/read + /api/v1/cardinality/* handlers"
```

---

### Task 6: **Headline** — multi-tenant isolation end-to-end through the HTTP API

**Files:**
- Create: `crates/metrics/tests/support/metrics_server.rs` (shared in-process boot + two-tenant seed)
- Create: `crates/metrics/tests/tenant_isolation.rs`
- Modify: `crates/metrics/Cargo.toml` (`[dev-dependencies]` `reqwest`, `tokio`, `serde_json`)

**Interfaces:**
- `support::metrics_server`:
  - `pub async fn start_in_process() -> TestServer` — boots the metrics service (distributor + querier roles in-process, in-memory/tempdir WAL+blockstore, the Slice 5 router) on an ephemeral port; returns `{ base_url, _guard }`.
  - `pub async fn push_samples(base: &str, tenant: &str, series: &[(Labels, Vec<(i64,f64)>)])` — remote_write v1 (or native produce) of samples for `tenant` via `X-Scope-OrgID`.
  - helpers `query(base, tenant, promql)`, `series(base, tenant, match)`, `labels(base, tenant)`, `label_values(base, tenant, name)` — each issues the HTTP request with the tenant header and returns parsed JSON.

> **Contract gap note:** if the Slice 4/5 in-process boot isn't available, this helper assembles it from the public role constructors `crabka-metrics` exposes; if those are absent, the task spins the `axum::Router` directly over an in-memory `Index` + `PromqlEngine` and drives writes through the distributor entry fn. Either way: **real HTTP over a real socket** (so `X-Scope-OrgID` goes through the genuine extractor), not a function call shortcut — the whole point is to exercise the tenancy boundary as a client would.

- [ ] **Step 1: Write the failing isolation test**

Create `crates/metrics/tests/tenant_isolation.rs`. Seed **two** tenants with *deliberately colliding* series identity (same metric name + same labels, different values), plus a tenant-A-only label, then assert A cannot see B and vice versa across **every** read surface:

```rust
mod support;
use assert2::{assert, check};
use support::metrics_server::{self as srv};

#[tokio::test]
async fn tenants_are_fully_isolated_across_all_read_surfaces() {
    let s = srv::start_in_process().await;

    // Same metric name in both tenants, plus a label unique to A.
    let a_series = vec![
        (labels(&[("__name__", "http_requests_total"), ("tenant_only", "A")]), vec![(1000, 1.0)]),
    ];
    let b_series = vec![
        (labels(&[("__name__", "http_requests_total")]), vec![(1000, 999.0)]),
    ];
    srv::push_samples(&s.base_url, "tenant-a", &a_series).await;
    srv::push_samples(&s.base_url, "tenant-b", &b_series).await;

    // 1) query: A sees value 1.0, B sees 999.0 — never each other's.
    let qa = srv::query(&s.base_url, "tenant-a", "http_requests_total").await;
    let qb = srv::query(&s.base_url, "tenant-b", "http_requests_total").await;
    check!(scalar_of(&qa) == 1.0);
    check!(scalar_of(&qb) == 999.0);

    // 2) labels: A has `tenant_only`, B does NOT.
    let la = srv::labels(&s.base_url, "tenant-a").await;
    let lb = srv::labels(&s.base_url, "tenant-b").await;
    check!(label_list(&la).contains(&"tenant_only".to_string()));
    check!(!label_list(&lb).contains(&"tenant_only".to_string()));

    // 3) label_values: B never sees A's `tenant_only=A`.
    let va = srv::label_values(&s.base_url, "tenant-b", "tenant_only").await;
    check!(value_list(&va).is_empty());

    // 4) series: count is per-tenant (1 each), no bleed.
    check!(series_count(&srv::series(&s.base_url, "tenant-a", "http_requests_total").await) == 1);

    // 5) cardinality/active_series: per-tenant.
    // 6) remote_read for A returns only A's samples.
    // (assert both — see helpers)
}
```

(Provide the small `labels`/`scalar_of`/`label_list`/`value_list`/`series_count` helpers in the test file or `support`.)

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test tenant_isolation`
Expected: FAIL — `support::metrics_server` / boot not yet present (or, if present, a real leak surfaces — fix it).

- [ ] **Step 3: Implement `support::metrics_server` + fix any leak**

Build the boot/seed/query helpers. Run the test. **If it reveals a real isolation leak** (an `Index`/head/block/HA-tracker key that isn't tenant-prefixed), fix the offending key in the earlier-slice code (the isolation boundary is the product requirement; this test is its enforcement) and note the fix in the commit.

- [ ] **Step 4: Add a per-tenant quota-isolation assertion**

Append a test: set a tiny `ingestion_rate` override for `tenant-a` only (via an `OverridesProvider` the server loads), push enough to throttle A (expect `429`), and confirm `tenant-b` is unaffected at the same instant. Proves quotas are bucketed per tenant.

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test tenant_isolation`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "test(metrics): headline multi-tenant isolation across all read surfaces + per-tenant quota"
```

---

### Task 7: Limit enforcement wired into the live write/read paths (per-surface HTTP error-body parity)

**Files:**
- Modify: the distributor write handler (Slice 4) — call `IngestEnforcer` before WAL append.
- Modify: the query handlers (Slice 5) — call `QueryEnforcer` in `/query_range`, `/series`.
- Create: `crates/metrics/tests/limits_overrides.rs` (drives limits through real HTTP).
- Modify: server boot to accept an `OverridesProvider`.

**Interfaces:**
- Consumes: Task 3 enforcers, Task 1 `LimitError` (→ HTTP status/body), Task 2 `OverridesProvider`, the server boot from Task 6.
- Produces: enforcement at the live edges + a test asserting error bodies end-to-end, **split by surface** (the two surfaces have different body shapes in real Mimir):
  - **QUERY API** (`/query_range`, `/series`) — the Prometheus JSON `errorType` envelope, byte-matched to Mimir:
    - query exceeding `max_query_length` → `422` `{"status":"error","errorType":"execution","error":…}`.
  - **PUSH path** (`/api/v1/push`) — Mimir's push validation errors are **plain-text bodies carrying `err-mimir-*` codes**, *not* the JSON `errorType` envelope (that envelope is the query-API shape only). Assert `(status, body_text)`, matching Mimir's plain-text `err-mimir-*` form (or Crabka's own documented push-error body — drop the "matches Mimir exactly" claim for the push surface if Crabka diverges):
    - over-long label → `400`, body carries `err-mimir-label-value-too-long` (plain text, no JSON `errorType`).
    - over-rate push → `429` (retriable), plain-text rate-limit body.
    - over-`max_global_series_per_user` push → `400` (`bad_data`; non-retriable validation, matching Mimir's per-user series limit), plain-text `err-mimir-max-series-per-user` body.

- [ ] **Step 1: Write the failing test**

Create `crates/metrics/tests/limits_overrides.rs`. Boot the server with an `OverridesProvider` carrying tight caps for `tenant-tight`; drive each over-limit case over real HTTP and assert the per-surface body shape: the QUERY-API case asserts `(status, body.errorType)` against the JSON envelope, while the PUSH-path cases assert `(status, body_text)` against Mimir's plain-text `err-mimir-*` body (`push_expect_error` returns the raw body string, not an `errorType`). Example skeleton:

```rust
mod support;
use assert2::check;
use support::metrics_server as srv;

#[tokio::test]
async fn over_limit_requests_return_prometheus_shaped_errors() {
    let overrides = r#"
overrides:
  tenant-tight:
    ingestion_rate: 1
    ingestion_burst_size: 1
    max_label_value_length: 4
    max_query_length: 60
"#;
    let s = srv::start_in_process_with_overrides(overrides).await;

    // PUSH path: label too long -> 400 with a plain-text err-mimir-* body
    // (push errors are NOT the JSON errorType envelope — that is the query API only).
    let (st, body) = srv::push_expect_error(&s.base_url, "tenant-tight",
        &[(labels(&[("__name__","m"),("x","toolong")]), vec![(1,1.0)])]).await;
    check!(st == 400);
    check!(body.contains("err-mimir-label-value-too-long"));

    // QUERY API: range > 60s -> 422 execution, JSON errorType envelope
    let (st, ty) = srv::query_range_expect_error(&s.base_url, "tenant-tight",
        "m", 0.0, 3600.0, 15).await;
    check!(st == 422 && ty == "execution");

    // PUSH path: burst then over-rate -> 429 (plain-text rate body)
    let _ = srv::push_samples(&s.base_url, "tenant-tight",
        &[(labels(&[("__name__","m")]), vec![(1,1.0)])]).await;
    let (st, _body) = srv::push_expect_error(&s.base_url, "tenant-tight",
        &[(labels(&[("__name__","m")]), vec![(2,1.0)])]).await;
    check!(st == 429);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test limits_overrides`
Expected: FAIL — enforcement not wired; over-limit requests currently succeed (200/204).

- [ ] **Step 3: Wire enforcement into the live handlers**

Call `IngestEnforcer::check_labels` + `check_active_series` + `check_sample_rate` in the distributor pre-WAL hook; `QueryEnforcer::check_range`/`check_series_count`/`check_sample_count` in the query handlers. Convert `LimitError` → response via `http_status()`/`error_type()`/`message()` and the Slice 5 envelope.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test limits_overrides`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "feat(metrics): enforce per-tenant limits at live write/read edges with Prometheus error bodies"
```

---

### Task 8: PromQL `.test` conformance gate + per-file coverage report

**Files:**
- Create: `crates/metrics/tests/promql_conformance.rs` (or in `crabka-promql` if the `.test` harness lives there — **place it beside the harness from Slice 2/3**; this task wires it as a *gate* + report).
- Modify: CI workflow (a `metrics-conformance` job) — see Task 11.

**Interfaces:**
- Consumes: the `.test` DSL harness built in Slice 2/3 (load / eval-instant / eval-range / `expect` assertions) and the vendored `promql/promqltest/testdata/` corpus (pinned Prometheus tag).
- Produces:
  - A test that **runs every `.test` file** in the corpus dir and **fails if any file fails** (the gate).
  - A **coverage report**: per-file pass/fail (and pass-count/total within a file), written to `target/promql-conformance-report.txt` (and printed under `--nocapture`), so a regression names the file. Use a known-pass allowlist *only if* a file is legitimately unsupported (e.g. an experimental-function file behind a feature flag) — list those explicitly with a reason, not a blanket skip.

> **If Slice 3 is unlanded:** this task still lands the *gate wiring + report*, pointed at whatever subset of the corpus the harness currently passes, with a `KNOWN_UNSUPPORTED: &[&str]` list (each entry justified). The gate then enforces "no regression below the current line", and later slices shrink the list to empty. Flag this as a Contract gap if applicable.

- [ ] **Step 1: Write the failing test**

```rust
// crates/metrics/tests/promql_conformance.rs  (or crabka-promql/tests/)
use assert2::assert;

/// Files we knowingly don't pass yet (each MUST carry a reason).
const KNOWN_UNSUPPORTED: &[(&str, &str)] = &[
    // ("double_exponential_smoothing.test", "experimental fn tier, feature-gated"),
];

#[test]
fn full_promql_test_corpus_passes() {
    let report = crabka_promql::testkit::run_corpus_dir(
        "tests/testdata/promql", // vendored
    );
    // Write the per-file report.
    report.write_to("target/promql-conformance-report.txt").unwrap();
    let failing: Vec<_> = report
        .files
        .iter()
        .filter(|f| !f.passed && !KNOWN_UNSUPPORTED.iter().any(|(n, _)| f.name.ends_with(n)))
        .collect();
    assert!(failing.is_empty(), "promql .test regressions: {failing:?}");
}
```

- [ ] **Step 2: Run to verify it fails (or passes if the harness is complete)**

Run: `cargo test -p crabka-metrics --test promql_conformance -- --nocapture` (or `-p crabka-promql`).
Expected: FAIL — `run_corpus_dir`/`Report::write_to` missing, **or** a real conformance gap surfaces.

- [ ] **Step 3: Implement the report API on the harness + the gate**

Add **public** `crabka_promql::testkit::run_corpus_dir(dir) -> Report` and `pub struct Report { pub files: Vec<FileResult { name, passed, passed_cases, total_cases }> }` with `Report::write_to(path)` to the Slice 2/3 harness `testkit` module (small addition — the per-file iteration + a text writer). These must be `pub` in `crabka-promql` so this slice's `crabka-metrics` test crate can import them. Vendor the corpus if not already (Apache-2.0 attribution).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test promql_conformance -- --nocapture`
Expected: PASS; `target/promql-conformance-report.txt` lists every file.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/ docs/
git commit -m "test(metrics): PromQL .test conformance gate + per-file coverage report"
```

---

### Task 9: Shared differential corpus + JSON differ (`#[ignore]` infra, no container yet)

**Files:**
- Create: `crates/metrics/tests/support/diff_corpus.rs` (path-included by Tasks 10/12/13)
- Modify: `crates/metrics/Cargo.toml` (`[dev-dependencies]` `reqwest`, `serde_json`, `testcontainers`, `testcontainers-modules`)

**Interfaces:**
- Produces (pure, Docker-free, so it can be unit-tested without containers):
  - `pub struct SeedPoint { metric: &'static str, labels: &[(&str,&str)], samples: &[(i64, f64)] }` and `pub fn seed_dataset() -> Vec<SeedPoint>` — a deterministic dataset exercising counters, gauges, a classic histogram (`_bucket`/`_sum`/`_count`), and a native histogram.
  - `pub fn query_corpus() -> Vec<QueryCase>` where `QueryCase { name, promql, kind: Instant|Range { start, end, step } }` — a representative corpus: `rate()`, `sum by`, `histogram_quantile`, `increase`, a binary op with `on/group_left`, `topk`, `_over_time`, an `@`/`offset`, a subquery.
  - `pub fn normalize(resp: &serde_json::Value) -> serde_json::Value` — canonicalize a Prometheus response for comparison: sort `data.result` by metric labelset, round float values to a fixed epsilon (NaN-aware), drop volatile fields. (NaN/`+Inf`/`-Inf` handled per Prometheus's string encoding.)
  - `pub fn assert_query_equal(name: &str, a: &serde_json::Value, b: &serde_json::Value)` — `normalize` both, assert structural equality, on mismatch print a unified-ish diff naming the query.
- A **self-test** (Docker-free) proving `normalize`/`assert_query_equal` behave: two responses that differ only in `result` order (and within float epsilon) compare **equal**; a genuine value difference compares **unequal**.

- [ ] **Step 1: Write the failing self-test**

Create `crates/metrics/tests/diff_corpus_selftest.rs`:

```rust
#[path = "support/diff_corpus.rs"]
mod diff_corpus;
use assert2::{assert, check};
use diff_corpus::*;
use serde_json::json;

#[test]
fn normalize_is_order_and_epsilon_insensitive() {
    let a = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x","a":"1"},"value":[0.0,"1.0000001"]},
        {"metric":{"__name__":"x","a":"2"},"value":[0.0,"2.0"]}]}});
    let b = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x","a":"2"},"value":[0.0,"2.0"]},
        {"metric":{"__name__":"x","a":"1"},"value":[0.0,"1.0"]}]}});
    check!(normalize(&a) == normalize(&b)); // order + epsilon
}

#[test]
fn real_value_difference_is_detected() {
    let a = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x"},"value":[0.0,"1.0"]}]}});
    let b = json!({"status":"success","data":{"resultType":"vector","result":[
        {"metric":{"__name__":"x"},"value":[0.0,"5.0"]}]}});
    check!(normalize(&a) != normalize(&b));
}

#[test]
fn corpus_is_nonempty_and_covers_key_functions() {
    let q = query_corpus();
    check!(q.iter().any(|c| c.promql.contains("rate(")));
    check!(q.iter().any(|c| c.promql.contains("histogram_quantile")));
    assert!(!seed_dataset().is_empty());
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-metrics --test diff_corpus_selftest`
Expected: FAIL — `diff_corpus` module absent.

- [ ] **Step 3: Implement `support/diff_corpus.rs`**

Implement the seed dataset, query corpus, `normalize` (float-epsilon + label-sort + NaN-aware), and `assert_query_equal`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-metrics --test diff_corpus_selftest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "test(metrics): shared differential query corpus + Prometheus JSON differ (Docker-free)"
```

---

### Task 10: prometheus/compliance black-box harness vs reference Prometheus (`#[ignore]`)

**Files:**
- Create: `crates/metrics/tests/diff_prometheus.rs`

**Interfaces:**
- Consumes: Task 6's in-process Crabka server; Task 9's corpus + differ; `testcontainers` `mirror.gcr.io/prom/prometheus`.
- Produces (`#[ignore = "requires Docker"]`):
  - A test that (1) boots Crabka in-process, (2) starts a `mirror.gcr.io/prom/prometheus:<pinned>` container configured with **remote_write receiver enabled** (`--web.enable-remote-write-receiver`) **and** a scrape config disabled (we push, not scrape), (3) remote_writes `seed_dataset()` to **both** Crabka (`/api/v1/push`) and the Prometheus container (`/api/v1/write`), (4) waits for ingestion, (5) runs `query_corpus()` against both `/api/v1/query`(`_range`) and `assert_query_equal` per case.

> **Harness structure & data loading (explicit, since this is a Docker suite):**
> - **Container:** `GenericImage::new("mirror.gcr.io/prom/prometheus", "<pinned tag>")` with cmd args `--web.enable-remote-write-receiver`, `--enable-feature=native-histograms` (so native-histogram cases match), a `WaitFor::message_on_stderr("Server is ready to receive web requests")`. Map `9090`.
> - **Data load:** build remote_write `WriteRequest` protobuf from `seed_dataset()` (reuse the Slice 4 v1 encoder, or a tiny local encoder), snappy-block, POST to both targets with identical bytes (Prometheus URL `http://localhost:<mapped>/api/v1/write`; Crabka `…/api/v1/push`, `X-Scope-OrgID: compliance`). One write, two destinations — guarantees identical input.
> - **Settle:** poll `/api/v1/query?query=up` style readiness, or sleep-with-retry on the first corpus query until both return non-empty (bounded, ~10s, mirroring the `client-core` bootstrap-retry pattern).
> - **Assert:** for each `QueryCase`, fetch from both, `assert_query_equal(case.name, crabka_json, prom_json)`. Known divergences (if any: e.g. `@ end()` wall-clock, `time()`/`timestamp()` of "now") are pinned to fixed `@` timestamps in the corpus so results are deterministic.
> - **Documented limitation:** native-histogram JSON encoding parity is asserted only if both sides emit the same native-histogram JSON object shape; otherwise that case is in a `PROM_KNOWN_DIVERGENCE` list with a reason.

- [ ] **Step 1: Write the `#[ignore]` test**

Create `crates/metrics/tests/diff_prometheus.rs` with the harness above; `#[tokio::test]` + `#[ignore = "requires Docker"]`.

- [ ] **Step 2: Run to verify it is skipped by default + runs under `--ignored`**

Run (default): `cargo test -p crabka-metrics --test diff_prometheus` → reports `0 run, 1 ignored`.
Run (with Docker): `cargo test -p crabka-metrics --test diff_prometheus -- --ignored --nocapture` → PASS (or surfaces a real divergence to fix).

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "test(metrics): prometheus/compliance black-box differential harness (ignored, Docker)"
```

---

### Task 11: **Headline** — differential vs real Mimir (testcontainers, `#[ignore]`)

**Files:**
- Create: `crates/metrics/tests/diff_mimir.rs`

**Interfaces:**
- Consumes: Task 6 in-process Crabka; Task 9 corpus + differ; `testcontainers` `mirror.gcr.io/grafana/mimir`.
- Produces (`#[ignore = "requires Docker"]`) — the headline external test:
  - Boot Crabka in-process; start `mirror.gcr.io/grafana/mimir:<pinned>` in **monolithic single-binary mode** (`-target=all`, filesystem blocks backend, multitenancy disabled or a fixed `X-Scope-OrgID`) with a minimal config file mounted; remote_write `seed_dataset()` to **both**; run `query_corpus()` against both `/prometheus/api/v1/query`(`_range`); `assert_query_equal` per case.

> **Harness structure & data loading (explicit):**
> - **Container:** `mirror.gcr.io/grafana/mimir:<pinned tag>` started with `-target=all` and a mounted `mimir.yaml` (filesystem `blocks_storage`, `common.storage.backend: filesystem`, short `-blocks-storage.tsdb.head-compaction-interval`, multitenancy with a single `X-Scope-OrgID: diff`). `WaitFor` on Mimir's `/ready` (poll the mapped HTTP port until 200). Mimir's push endpoint is `POST /api/v1/push` with `X-Scope-OrgID`.
> - **Data load:** identical remote_write bytes to Crabka and Mimir (same one-write-two-destinations approach as Task 10). Use a fixed wall-clock base for sample timestamps so `@`/`time()` cases are deterministic.
> - **Compaction wait:** Mimir serves recent samples from its ingester head immediately; for cases that depend on block compaction, either keep the corpus within the head window or trigger/await Mimir's head compaction. Default: keep the corpus in the head window (simplest, deterministic).
> - **Assert:** per `QueryCase`, `assert_query_equal(case.name, crabka_json, mimir_json)`. `MIMIR_KNOWN_DIVERGENCE` list (each entry justified) covers any Mimir-specific metadata (e.g. Mimir injects `__mimir__` internal labels or query-stats headers we strip in `normalize`).
> - **Why headline:** Mimir is the system Crabka claims to replace; corpus equality over identical input is the strongest single correctness signal in the slice. Keep this test the most carefully curated corpus.

- [ ] **Step 1: Write the `#[ignore]` test + mount config**

Create `crates/metrics/tests/diff_mimir.rs` (embed the minimal `mimir.yaml` as a string written to a `tempfile` and bind-mounted). `#[tokio::test]` + `#[ignore = "requires Docker"]`.

- [ ] **Step 2: Run to verify ignored-by-default + runnable**

Run (default): `cargo test -p crabka-metrics --test diff_mimir` → `0 run, 1 ignored`.
Run (Docker): `cargo test -p crabka-metrics --test diff_mimir -- --ignored --nocapture` → PASS (or a real divergence to fix).

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "test(metrics): headline differential vs real Mimir (ignored, testcontainers)"
```

---

### Task 12: Grafana integration (`#[ignore]`)

**Files:**
- Create: `crates/metrics/tests/grafana_integration.rs`

**Interfaces:**
- Consumes: Task 6 in-process Crabka; `testcontainers` `mirror.gcr.io/grafana/grafana`.
- Produces (`#[ignore = "requires Docker"]`):
  - Boot Crabka in-process (seed a known metric); start `mirror.gcr.io/grafana/grafana:<pinned>` with a **provisioned Prometheus datasource** whose `url` points at the Crabka base URL (Grafana must reach the host — use `host.docker.internal` or run Crabka bound to the container-visible address); drive Grafana's datasource **proxy/Explore query API** (`POST /api/ds/query` or the datasource proxy `/api/datasources/proxy/uid/<uid>/api/v1/query`) for a couple of corpus queries; assert the response renders (status `success`, non-empty frames).

> **Harness structure & data loading (explicit):**
> - **Datasource provisioning:** mount a `datasources.yaml` (`apiVersion: 1`, a `prometheus` datasource, `url: http://host.docker.internal:<crabka_port>`, `isDefault: true`, a fixed `uid`, and `httpHeaderName1: X-Scope-OrgID` / `httpHeaderValue1: grafana` so Grafana sends the tenant header). Set `GF_AUTH_ANONYMOUS_ENABLED=true`, `GF_AUTH_ANONYMOUS_ORG_ROLE=Admin` so the test calls the API without login.
> - **Host reach:** start Crabka bound to `0.0.0.0` and pass the container `--add-host=host.docker.internal:host-gateway` (Linux) so the datasource URL resolves; document this as the platform-specific knob.
> - **Drive:** `POST /api/ds/query` with a Prometheus query payload referencing the datasource `uid`; assert HTTP 200 + the result frames are non-empty and carry the seeded series. (This proves the full Grafana → Prometheus-datasource → Crabka path renders, the spec's "assert they render".)
> - **Scope:** one instant + one range query is sufficient; this is an integration smoke, not a second differential corpus.

- [ ] **Step 1: Write the `#[ignore]` test**

Create `crates/metrics/tests/grafana_integration.rs` with the provisioning + drive above; `#[ignore = "requires Docker"]`.

- [ ] **Step 2: Run to verify ignored + runnable**

Run (default): `cargo test -p crabka-metrics --test grafana_integration` → `0 run, 1 ignored`.
Run (Docker): `cargo test -p crabka-metrics --test grafana_integration -- --ignored --nocapture` → PASS.

- [ ] **Step 3: Commit**

```bash
cargo fmt -p crabka-metrics
cargo clippy -p crabka-metrics --all-targets
git add crates/metrics/
git commit -m "test(metrics): Grafana built-in Prometheus datasource integration (ignored, Docker)"
```

---

### Task 13: CI wiring — `metrics-differential` job + conformance gate; final whole-crate gate

**Files:**
- Modify: the CI workflow (`.github/workflows/*.yml`) — add two jobs.
- Modify: `docs/` (a short note in the slice's section of the spec/plan dir if the repo tracks a CI matrix; otherwise none).

**Interfaces:**
- Produces:
  - A **`metrics-conformance`** CI job (Linux): `cargo test -p crabka-metrics --test promql_conformance` (and the `crabka-promql` harness tests) — runs on every PR as a **gate**; uploads `target/promql-conformance-report.txt` as an artifact.
  - A **`metrics-differential`** CI job (Linux, Docker available): `cargo test -p crabka-metrics -- --ignored` scoped to the three Docker suites (`diff_prometheus`, `diff_mimir`, `grafana_integration`) — runs on a schedule + on-demand label (not every PR, to keep PR latency low), mirroring how the repo gates other Docker-heavy suites (`client-core-integration`). Document that these never run in the default `cargo test --workspace`.

- [ ] **Step 1: Add the two CI jobs**

Add `metrics-conformance` (every PR) and `metrics-differential` (scheduled/labeled, `services`/Docker-enabled runner) to the workflow, following the existing job patterns (toolchain, cache, `--ignored` invocation).

- [ ] **Step 2: Verify locally**

Run the default suite (no Docker): `cargo test -p crabka-metrics` → all non-ignored pass, the three Docker suites report ignored.
Run the gate: `cargo test -p crabka-metrics --test promql_conformance` → PASS.

- [ ] **Step 3: Final whole-crate gate**

Run: `cargo test -p crabka-metrics && cargo clippy -p crabka-metrics --all-targets && cargo fmt -p crabka-metrics --check`
Expected: all PASS, no warnings, formatting clean. (Docker suites remain ignored.)

- [ ] **Step 4: Commit**

```bash
cargo fmt -p crabka-metrics
git add .github/ crates/metrics/ docs/
git commit -m "ci(metrics): conformance gate + dedicated metrics-differential Docker job"
```

---

## Self-review

**Spec coverage (against §8 API, §9 limits/HA, §10 testing, §11 Slice 8):**
- Per-tenant limits/quotas (ingestion rate, max series, label-name/value length, samples-per-query, query lookback/range, series-per-query) on the token-bucket where it fits → Tasks 1–3, enforced live in Task 7. Token-bucket reused for the *rate* limits only (Task 3 note); count/length caps are plain comparisons (correct — a bucket would be wrong for a one-shot cap).
- Per-tenant overrides YAML (Mimir runtime.yaml) → Task 2.
- Prometheus-shaped errors `429`/`422`/`400` → Task 1 (`LimitError` status/type) + Task 7 (end-to-end body assertions). Ingestion-rate ⇒ 429 (retriable); per-user series limit ⇒ 400 `bad_data` (non-retriable validation, matching Mimir); label-length ⇒ 400 `bad_data`; query caps ⇒ 422 `execution`.
- Multi-tenancy isolation (read/labels/values/series/exemplars/quota; tenant-prefixed keys) **end-to-end via two `X-Scope-OrgID`s** → Task 6 (**headline**), with the quota-per-tenant assertion in Step 4.
- remote_read `POST /api/v1/read` (ReadRequest/Response protobuf + snappy, SAMPLES path) → Tasks 4 (wire) + 5 (handler); STREAMED_XOR_CHUNKS explicitly out of scope with a documented limitation (matches the spec's "or at least SAMPLES").
- Cardinality APIs (`label_names`/`label_values`/`active_series`) from the `Index` → Task 5.
- PromQL `.test` conformance gate + per-file coverage report in CI → Tasks 8 + 13.
- prometheus/compliance black-box harness → Task 10 (`#[ignore]`).
- Differential vs real Mimir (testcontainers) **as the equality headline** → Task 11 (`#[ignore]`).
- Grafana integration (built-in Prometheus datasource → Crabka) → Task 12 (`#[ignore]`).
- Dedicated CI job for the Docker/external suites; default `cargo test` never touches Docker → Task 13.

**Headlines are explicit and end-to-end.** Tenant isolation (Task 6) drives **real HTTP over a real socket** with two org IDs across *every* read surface plus per-tenant quota — not a function-call shortcut. Differential-vs-Mimir (Task 11) feeds **identical remote_write bytes** to both systems (one-write-two-destinations) and asserts corpus equality through `normalize`/`assert_query_equal`. Both are called out as headline in their task titles and the spec-coverage list.

**`#[ignore]` discipline.** Every external-system test (Tasks 10/11/12) is `#[ignore = "requires Docker"]`, each task's run-step verifies `0 run, N ignored` by default and `--ignored` under Docker, and Task 13 isolates them in a `metrics-differential` job off the PR path. The Docker-free `normalize`/corpus self-test (Task 9) and the in-process isolation/limits/cardinality/remote_read tests (Tasks 5/6/7) run in the default suite, so the meat of the slice has fast CI coverage without Docker.

**Contract consumption, not re-derivation.** The querier HTTP API, `PromqlEngine`, `Index`, `TenantId` extractor, distributor write hook, and the broker `TokenBucket` are all consumed from earlier slices/crates via the Shared Contract table; each task names the exact item and carries a "Contract gap" fallback (a minimal in-memory trait impl, never a silent stub) for the case where the dependency slice is unlanded. No new query semantics are invented here — this is a hardening band.

**No-back-compat respected.** No `#[serde(default)]`-as-compat-shim, no version variants, no migration. The single `#[serde(default)]` use (Task 2, `PartialLimits`) is the *partial-config mechanism* (a tenant overrides only named fields), explicitly distinguished in-task from a compat shim, with a code comment so a reviewer doesn't misflag it.

**Placeholder scan.** Every task has a failing-test → run-fails-with-expected → real-code → run-passes → commit cycle with concrete `cargo test -p crabka-metrics …` commands and assert2 assertions. The Docker tasks (10/11/12), where literal code would be guesswork against live container behavior, instead provide a **fully specified harness structure** — exact image/tag knobs, the one-write-two-destinations data-load mechanism, the settle/wait strategy, the assertion, and an explicit known-divergence list — which is the honest level of detail for a black-box external suite, not a placeholder.

**Type/name consistency.** `Limits` field set is identical across Tasks 1/2/3/7 and every test (`ingestion_rate`, `max_global_series_per_user`, `max_label_name_length`/`value`, `max_fetched_series_per_query`, `max_samples_per_query`, `max_query_length_secs`, `max_query_lookback_secs`). `LimitError` variants and their `http_status`/`error_type` mapping are defined once (Task 1) and asserted unchanged in Tasks 3/7. The remote_read function set (`decode_read_request`/`encode_read_response`/`matchers_to_selectors`/`series_to_timeseries`) is consistent between Tasks 4 and 5. The `support::metrics_server` and `support::diff_corpus` helper signatures are fixed in Tasks 6/9 and consumed unchanged in Tasks 5/7/10/11/12.

**Known risks (flagged).** (1) `crabka-metrics` needs `TokenBucket` but the existing `crabka_broker` bucket has no independent-burst knob (`set_rate` clamps the initial budget to `rate`), so honoring Mimir's independent `ingestion_burst_size` requires both a code change and avoiding a heavy/cyclic dep on the broker. Task 3 resolves both by lifting `bucket.rs` into a tiny `crabka-throttle` crate and adding `set_rate_with_burst(rate, burst)` there (broker re-exports it, so its public path is unchanged); a `burst != rate` test pins the new behavior. (2) Docker-host reachability for Grafana (Task 12) is platform-specific (`host.docker.internal` + `--add-host`); flagged with the exact knob. (3) Mimir/Prometheus internal-label and header noise (Tasks 10/11) is contained by `normalize` + an explicit per-suite `KNOWN_DIVERGENCE` list rather than loosening the differ. (4) The `.test` corpus + remote_read proto must pin the **same** Prometheus tag (Tasks 4/8) — called out in both tasks.
