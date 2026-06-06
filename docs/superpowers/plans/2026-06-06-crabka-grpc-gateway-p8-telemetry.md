# Crabka gRPC Gateway P8 — Telemetry (Prometheus + OTLP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** Instrument the gateway with Prometheus metrics on a `/metrics` scrape endpoint and OTLP distributed tracing across send→dedup→(forward)→txn→commit and the webhook in/out paths — reusing the broker's telemetry stack by **factoring its generic OTLP init into a shared `crabka-telemetry` crate** (behavior-preserving, like P5's crabka-authz).

**Architecture:** A new leaf crate `crabka-telemetry` holds the generic OTLP pipeline (`OtlpConfig`/`OtlpProtocol`/`TelemetryError`/`TelemetryGuard`/`init`), parameterized by service/tracer name + filters (the broker's `request_span`/`api_name` stay broker-local — they use Kafka api-keys). The broker re-exports the generic bits + updates its init call (byte-exact). The gateway gets a `GatewayMetrics` (prometheus-client, `crabka_gateway_*`) behind a **global `OnceLock`** accessor (`metrics()`), so increment sites don't thread a handle; a `/metrics` router renders it. The bin calls `crabka_telemetry::init` (OTLP off by default) and `#[tracing::instrument]` decorates the hot paths.

**Tech Stack:** `prometheus-client` 0.24 (metrics — mirror `broker/src/metrics.rs`/`metrics_server.rs`), `opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp`/`tracing-opentelemetry` 0.32–0.33 (OTLP — factored into `crabka-telemetry`). **Behavior-preserving broker change** (the telemetry factor); broker tests are the guardrail.

**Out of scope / deferred:** per-request HTTP/gRPC trace-context EXTRACTION from inbound headers (the spans are created locally; W3C `traceparent` propagation IN is a follow-up — `set_text_map_propagator` is installed so it composes later); the operator CR surfacing the OTLP env (P9).

---

## Execution constraints (every task)

- **Worktree:** `/Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25`. Prefix Bash with `cd /Users/mattstone/git/crabka/.claude/worktrees/intelligent-fermat-f80f25 && ...`, use `git -C <worktree>`.
- **Branch:** `claude/gateway-p8`, **stacked on `claude/gateway-p7`** (#416 — unmerged; P8 instruments P7's outbound). PR bases on #416, or rebases onto `main` if #416 merges first. Assert HEAD == `claude/gateway-p8` before commit.
- **Git identity:** `git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit ...` (never `git config`). Stage `Cargo.lock`.
- **Broker change is behavior-preserving ONLY** (the telemetry factor) — Kafka wire bytes + the broker test suite must stay green. Each task ends GREEN: `cargo test -p <crate>`, `cargo clippy -p <crate> --all-targets -- -D warnings`, `cargo fmt --check`.

## Confirmed APIs (investigated)

- Broker `telemetry.rs` (the source to factor): `OtlpProtocol{Grpc,HttpProtobuf}`, `TelemetryError(#[from] opentelemetry_otlp::ExporterBuildError)`, `OtlpConfig { endpoint, protocol, sample_ratio, service_name, service_version, service_instance_id, timeout }` + `from_env(get, instance_id, version) -> Option<Self>` (currently defaults service_name to `"crabka-broker"`), `build_exporter`/`resource`/`sampler` (private), `TelemetryGuard{shutdown}`, `init(otlp, default_filter) -> Result<TelemetryGuard, TelemetryError>` (hardcodes `provider.tracer("crabka-broker")` + an `otel_filter` defaulting to `info,{REQUEST_TARGET}=debug,crabka_log=info`). **Kafka-specific (stays in broker):** `REQUEST_TARGET`, `request_span(api_key,...)`, `api_name` (uses `crabka_protocol::ApiKey`).
- Metrics: `prometheus-client` 0.24 — `Registry::with_prefix("crabka_gateway")`, `Family<Label, Counter>` (labeled), `Counter`, `Gauge`, `Histogram::new(buckets)`; `registry.register(name, help, metric.clone())`; increment `family.get_or_create(&label).inc()` / `counter.inc()` / `gauge.set(i64)` / `hist.observe(f64)`; render `prometheus_client::encoding::text::encode(&mut String, &registry)`; label sets derive `EncodeLabelSet`. Mirror `crates/broker/src/{metrics.rs,metrics_server.rs}`.
- Telemetry deps (workspace): `opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp`/`opentelemetry-proto` 0.32, `tracing-opentelemetry` 0.33, `prometheus-client` 0.24, `tracing`/`tracing-subscriber` (gateway already has `tracing`+`tracing-subscriber`).
- Gateway instrument sites: `handlers::send` (sends_total + produce_latency), `dedup/mod.rs::{dedup_produce (hit), txn_write (commit/abort)}`, `forward.rs::Forwarder::forward` + `forward_handler` (forward_total{outcome}), `dedup/store.rs::run_ownership` (owned_partitions gauge), `webhook.rs::{webhook_handler, produce_handler}` (webhook_in_total{result}), `outbound.rs::{deliver_one (out_total + retries + dead_letter), run_subscription (active gauge)}`. App assembly in `bin/gateway.rs` (`.merge(...)`), tracing init `tracing_subscriber::fmt()...init()`.

## Crate/file map

- **Create** `crates/telemetry/` (Cargo.toml, src/lib.rs) — the factored generic OTLP pipeline. Add to workspace.
- **Modify** `crates/broker/src/telemetry.rs` (re-export generic from crabka-telemetry; keep Kafka-specific; the broker bin's `init`/`from_env` call site), `crates/broker/Cargo.toml` (+crabka-telemetry; drop now-unused otel deps if fully moved — or keep). The 10 generic telemetry tests move to crabka-telemetry.
- **Create** `crates/grpc-gateway/src/metrics.rs` (`GatewayMetrics` + global `metrics()` + `router()`).
- **Modify** `crates/grpc-gateway/src/{lib.rs, handlers.rs, dedup/mod.rs, dedup/store.rs, forward.rs, webhook.rs, outbound.rs, bin/gateway.rs}` + `Cargo.toml` (+prometheus-client, +crabka-telemetry, +opentelemetry deps).
- **Create** `crates/grpc-gateway/tests/metrics.rs`.

## Batches

- **Batch A:** Task 1 (`crabka-telemetry` crate) ∥ Task 3 (gateway `metrics.rs` module + deps). Disjoint.
- **Batch B:** Task 2 (broker re-export, needs T1) ∥ Task 4 (gateway instrumentation, needs T3). Disjoint crates.
- **Batch C:** Task 5 (gateway bin: `/metrics` router + OTLP init + `#[instrument]`) — needs T1+T3+T4.
- **Batch D:** Task 6 (tests) — needs T5.

---

## Task 1: `crabka-telemetry` crate (factor generic OTLP)

**Files:** Create `crates/telemetry/Cargo.toml`, `crates/telemetry/src/lib.rs`. Modify root `Cargo.toml` (members glob already covers `crates/*` — verify).

- [ ] **Step 1: Cargo.toml** — `name = "crabka-telemetry"`, version/edition/etc. workspace; deps: `opentelemetry = { workspace = true, features = ["trace"] }`, `opentelemetry_sdk = { workspace = true, features = ["trace"] }`, `opentelemetry-otlp = { workspace = true, features = ["trace","grpc-tonic","http-proto","reqwest-blocking-client"] }`, `tracing-opentelemetry = { workspace = true }`, `tracing = { workspace = true }`, `tracing-subscriber = { workspace = true, features = ["env-filter"] }`, `thiserror = { workspace = true }`; dev-dep `assert2`. (NO `crabka-protocol` — that stays in the broker for `api_name`.)

- [ ] **Step 2: `src/lib.rs`** — move `OtlpProtocol`, `TelemetryError`, `OtlpConfig` (+ `from_env`/`build_exporter`/`resource`/`sampler`), `TelemetryGuard`, and `init` from broker `telemetry.rs` VERBATIM, with these PARAMETERIZATIONS so it's service-agnostic:
  - `OtlpConfig::from_env(get, service_instance_id, service_version, default_service_name: &str)` — replace the hardcoded `"crabka-broker"` default with the `default_service_name` param.
  - `init(otlp: Option<OtlpConfig>, fmt_default_filter: &str, otel_default_filter: &str, tracer_name: &str) -> Result<TelemetryGuard, TelemetryError>` — the fmt layer uses `RUST_LOG`/`fmt_default_filter`; the otel layer uses `CRABKA_OTLP_FILTER`/`otel_default_filter` (keep the `CRABKA_OTLP_FILTER` env override logic, but the DEFAULT string is the `otel_default_filter` param); `provider.tracer(tracer_name)` instead of the hardcoded `"crabka-broker"`. Keep `set_text_map_propagator(TraceContextPropagator::new())` + `set_tracer_provider`.
  - Drop `REQUEST_TARGET`/`request_span`/`api_name` (they stay in the broker — Kafka-specific). The `otel_filter` helper becomes a free fn taking the default string + the env getter.
  - Move the GENERIC tests (`disabled_when_no_env`, `enabled_by_*`, `grpc_is_the_default_protocol`, `sdk_disabled_overrides_endpoint`, `endpoint_precedence`, `sample_ratio`, `service_name_and_timeout`, `protocol_parse_variants`) — updating `from_env` calls to pass a `default_service_name` (e.g. `"crabka-broker"` so assertions hold, or change the asserted default). Do NOT move `api_name_*`/`request_span_*` tests.

- [ ] **Step 3: Gates + commit.** `cargo test -p crabka-telemetry` (moved tests pass), clippy, fmt. Commit `feat(telemetry): crabka-telemetry crate (generic OTLP pipeline factored from broker)`. Stage `crates/telemetry/**` + root `Cargo.toml` + `Cargo.lock`.

---

## Task 2: Broker re-exports `crabka-telemetry` (behavior-preserving)

**Files:** Modify `crates/broker/src/telemetry.rs`, `crates/broker/Cargo.toml`, and the broker's `init`/`from_env` call site (the bin).

- [ ] **Step 1: Dep.** `crates/broker/Cargo.toml`: add `crabka-telemetry = { version = "0.2", path = "../telemetry" }`. (Leave the existing `opentelemetry*`/`tracing-opentelemetry` deps if `request_span`/`api_name` or other broker code still needs them; otherwise they're now transitive — keep to avoid churn unless clippy flags unused.)

- [ ] **Step 2: `telemetry.rs`** — replace the moved items with re-exports + keep the Kafka-specific bits:

```rust
pub use crabka_telemetry::{OtlpConfig, OtlpProtocol, TelemetryError, TelemetryGuard, init};

pub const REQUEST_TARGET: &str = "crabka_broker::request";
// ... keep request_span(...) + api_name(...) + their tests verbatim ...
```

(`request_span`/`api_name` keep their `opentelemetry`/`crabka_protocol`/`tracing` uses.)

- [ ] **Step 3: Update the broker's init call.** Grep `telemetry::init(` + `OtlpConfig::from_env(` in `crates/broker/src` (the bin / startup). Update them to the new signatures, passing the SAME values the broker used before so behavior is identical: `OtlpConfig::from_env(get, broker_id, version, "crabka-broker")` and `init(otlp, <broker's old fmt default_filter>, &format!("info,{}=debug,crabka_log=info", REQUEST_TARGET), "crabka-broker")`. (The old hardcoded otel filter was `info,{REQUEST_TARGET}=debug,crabka_log=info`; pass exactly that.)

- [ ] **Step 4: Gates + commit.** `cargo build -p crabka-broker`; `cargo test -p crabka-broker --lib` (green — the moved generic telemetry tests now live in crabka-telemetry; the broker keeps `api_name`/`request_span` tests). clippy `--all-targets -D warnings`; fmt. Commit `refactor(broker): use crabka-telemetry for the OTLP pipeline (behavior-preserving)`. Stage broker changes + `Cargo.lock`.

---

## Task 3: Gateway metrics module (Prometheus)

**Files:** Modify `crates/grpc-gateway/Cargo.toml`; create `crates/grpc-gateway/src/metrics.rs`; modify `src/lib.rs`.

- [ ] **Step 1: Dep.** `grpc-gateway/Cargo.toml`: `prometheus-client = { workspace = true }`.

- [ ] **Step 2: `metrics.rs`** — `GatewayMetrics` (mirror `broker/src/metrics.rs`) holding a `Registry` (prefix `crabka_gateway`) + the §10 metric handles, behind a global accessor:

```rust
//! Gateway Prometheus metrics. A process-global `GatewayMetrics` (lazy) so any
//! code path can record without threading a handle; `/metrics` renders it.

use std::sync::OnceLock;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ResultLabel { pub result: String }
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct OutcomeLabel { pub outcome: String }
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct KindLabel { pub kind: String }

pub struct GatewayMetrics {
    pub registry: Registry,
    sends_total: Family<ResultLabel, Counter>,
    produce_latency_seconds: Histogram,
    dedup_hits_total: Counter,
    forward_total: Family<OutcomeLabel, Counter>,
    txn_total: Family<KindLabel, Counter>,
    active_subscriptions: Gauge,
    owned_partitions: Gauge,
    webhook_in_total: Family<ResultLabel, Counter>,
    webhook_out_total: Family<ResultLabel, Counter>,
    webhook_retries_total: Counter,
    dead_letter_total: Counter,
}

impl GatewayMetrics {
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("crabka_gateway");
        let sends_total = Family::<ResultLabel, Counter>::default();
        registry.register("sends", "Produce-path send results", sends_total.clone());
        let produce_latency_seconds = Histogram::new(
            [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0].into_iter());
        registry.register("produce_latency_seconds", "Produce latency (seconds)", produce_latency_seconds.clone());
        // ... register the remaining 9 the same way (dedup_hits, forward, txn,
        //     active_subscriptions, owned_partitions, webhook_in, webhook_out,
        //     webhook_retries, dead_letter) ...
        Self { registry, sends_total, produce_latency_seconds, dedup_hits_total, forward_total,
            txn_total, active_subscriptions, owned_partitions, webhook_in_total, webhook_out_total,
            webhook_retries_total, dead_letter_total }
    }

    // Recorder helpers (so call sites stay one-liners):
    pub fn record_send(&self, result: &str) { self.sends_total.get_or_create(&ResultLabel{result:result.into()}).inc(); }
    pub fn observe_produce_latency(&self, secs: f64) { self.produce_latency_seconds.observe(secs); }
    pub fn record_dedup_hit(&self) { self.dedup_hits_total.inc(); }
    pub fn record_forward(&self, outcome: &str) { self.forward_total.get_or_create(&OutcomeLabel{outcome:outcome.into()}).inc(); }
    pub fn record_txn(&self, kind: &str) { self.txn_total.get_or_create(&KindLabel{kind:kind.into()}).inc(); }
    pub fn set_owned_partitions(&self, n: i64) { self.owned_partitions.set(n); }
    pub fn inc_active_subscriptions(&self) { self.active_subscriptions.inc(); }
    pub fn dec_active_subscriptions(&self) { self.active_subscriptions.dec(); }
    pub fn record_webhook_in(&self, result: &str) { self.webhook_in_total.get_or_create(&ResultLabel{result:result.into()}).inc(); }
    pub fn record_webhook_out(&self, result: &str) { self.webhook_out_total.get_or_create(&ResultLabel{result:result.into()}).inc(); }
    pub fn record_webhook_retry(&self) { self.webhook_retries_total.inc(); }
    pub fn record_dead_letter(&self) { self.dead_letter_total.inc(); }
}

impl Default for GatewayMetrics { fn default() -> Self { Self::new() } }

static METRICS: OnceLock<GatewayMetrics> = OnceLock::new();
/// Process-global metrics (lazy). Safe to call before the bin inits anything.
#[must_use]
pub fn metrics() -> &'static GatewayMetrics { METRICS.get_or_init(GatewayMetrics::new) }

/// `/metrics` router (renders the global registry).
#[must_use]
pub fn router() -> axum::Router {
    axum::Router::new().route("/metrics", axum::routing::get(render))
}

async fn render() -> impl axum::response::IntoResponse {
    let mut buf = String::new();
    match prometheus_client::encoding::text::encode(&mut buf, &metrics().registry) {
        Ok(()) => (axum::http::StatusCode::OK,
            [("content-type", "application/openmetrics-text; version=1.0.0; charset=utf-8")], buf).into_response(),
        Err(e) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response(),
    }
}
```

> VERIFY prometheus-client 0.24 APIs against `crates/broker/src/metrics.rs`: `Histogram::new(impl Iterator<Item=f64>)`, `Gauge` `set(i64)`/`inc`/`dec`, `Family::get_or_create`, the `EncodeLabelSet` derive import path, `encoding::text::encode`. Add a unit test: `metrics().record_send("ok")` then encode the registry and assert the output contains `crabka_gateway_sends_total`.

- [ ] **Step 3:** `lib.rs` `pub mod metrics;`. Gates + commit `feat(gateway): GatewayMetrics (prometheus-client) + /metrics router`.

---

## Task 4: Instrument the code paths

**Files:** Modify `crates/grpc-gateway/src/{handlers.rs, dedup/mod.rs, dedup/store.rs, forward.rs, webhook.rs, outbound.rs}`. Each site calls `crate::metrics::metrics().record_*(...)` (no handle threading).

- [ ] `handlers::send` — per record: time the `produce` call (`Instant::now()` → `observe_produce_latency(elapsed.as_secs_f64())`) and `record_send(result)` with `result` ∈ {`ok`,`deduplicated`,`unauthorized`,`error`} (derive from the `Result`/`RecordOutcome.deduplicated`).
- `dedup/mod.rs::dedup_produce` — on the fast-path map HIT, `record_dedup_hit()`. `txn_write` — `record_txn("commit")` after `commit_transaction` succeeds; `record_txn("abort")` in the abort path.
- `forward.rs::Forwarder::forward` — `record_forward(outcome)` with `outcome` ∈ {`ok`,`unavailable`,`unauthorized`,`forward_error`} on each return arm. (`forward_handler` is the receiver; the origin's `forward` is the right counter site.)
- `dedup/store.rs::run_ownership` — after each assignment change, `set_owned_partitions(self.owned.read().len() as i64)`.
- `webhook.rs` — `record_webhook_in(result)` in `webhook_handler`/`produce_handler` on each return ({`ok`,`unauthenticated`,`too_large`,`not_found`,`unauthorized`,`bad_request`,`error`}).
- `outbound.rs::deliver_one` — `record_webhook_out("delivered")` on 2xx; `record_webhook_retry()` before each backoff sleep; on exhaustion `record_dead_letter()` + `record_webhook_out("dead_letter")` (or `dropped`). `run_subscription` — `inc_active_subscriptions()` at start, `dec_active_subscriptions()` before return (RAII guard or explicit on all exit paths).
- [ ] Gates + commit `feat(gateway): record telemetry metrics across send/dedup/forward/ownership/webhook`.

---

## Task 5: Binary wiring (/metrics + OTLP init + spans)

**Files:** Modify `crates/grpc-gateway/Cargo.toml` (+crabka-telemetry, +opentelemetry deps for the guard type), `crates/grpc-gateway/src/bin/gateway.rs`; add `#[tracing::instrument]` on hot paths.

- [ ] **Deps:** `crabka-telemetry = { version = "0.2", path = "../telemetry" }` (+ the bin uses `crabka_telemetry::{OtlpConfig, init}`).
- [ ] **`/metrics` router:** `.merge(crabka_grpc_gateway::metrics::router())` in the `app` assembly.
- [ ] **OTLP init:** replace the bin's `tracing_subscriber::fmt()...init()` with `let otlp = crabka_telemetry::OtlpConfig::from_env(|k| std::env::var(k).ok(), &args.client_id, env!("CARGO_PKG_VERSION"), "crabka-grpc-gateway"); let _telemetry = crabka_telemetry::init(otlp, "crabka_grpc_gateway=info,info", "info,gateway::audit=debug", "crabka-grpc-gateway").expect("telemetry init");` — keep the guard alive for the process; call `_telemetry.shutdown()` on graceful exit (after `serve` returns). Must run inside the tokio runtime (it already is — `#[tokio::main]`). (Remove the old `tracing_subscriber::fmt().init()`; `init` installs the subscriber.)
- [ ] **Spans:** add `#[tracing::instrument(skip_all, fields(...))]` on `handlers::send`, `dedup::dedup_produce`, `Forwarder::forward`, `outbound::deliver_one`, `webhook::webhook_handler` (lightweight, `skip_all` to avoid logging bodies/secrets). These emit OTLP spans when OTLP is enabled, zero-cost otherwise.
- [ ] Gates + commit `feat(gateway): wire /metrics + OTLP tracing into the binary`.

---

## Task 6: Tests

**Files:** Create `crates/grpc-gateway/tests/metrics.rs`.

- [ ] Tests: (1) `metrics_router_renders` — drive `metrics::router()` via `tower::oneshot` GET `/metrics` ⇒ 200, body contains `crabka_gateway_` metric names. (2) `send_increments_sends_total` — record a send via the global (or drive `handlers::send` over a broker as `tests/wire.rs` does) then GET `/metrics` and assert `crabka_gateway_sends_total` is present + incremented. (3) `webhook_out_metrics` (optional, if cheap) — after an outbound delivery test, assert `crabka_gateway_webhook_out_total`/`dead_letter_total` appear. Keep it light — the global `OnceLock` is shared across tests in a binary, so assert presence/monotonic-increase, not exact values.
- [ ] Gates. Commit `test(gateway): /metrics endpoint + metric increments`.

---

## Final review + finish

Final review: (1) **broker behavior-preserving** — the telemetry factor is byte-exact (init passes the same tracer name/filters; broker tests green; OTLP still off-by-default); (2) **metrics correctness** — each §10 metric increments at the right site with the right labels; the global `OnceLock` registry is the one `/metrics` renders; (3) **OTLP off by default** — no env ⇒ identical behavior, only the `fmt` layer; (4) **no secret/body logging** in the `#[instrument]` spans (`skip_all`); (5) no cycle (`crabka-telemetry` is a leaf); (6) the guard shuts down on exit (flush). Then finish the branch (push + PR stacked on #416 / rebased to main).

## Self-review notes (author)

- **Spec coverage (§10):** all listed metrics ✓ (`gateway_sends_total{result}`, `dedup_hits`, `produce_latency_seconds`, `forward_total{outcome}`, `txn_total{kind}`, `active_subscriptions`, `owned_partitions`, `webhook_in/out_total{result}`, `webhook_retries_total`, `dead_letter_total`); `/metrics` ✓; OTLP spans across send→dedup→forward→txn + webhook ✓ (via `#[instrument]` + the OTLP layer); "matches the broker's stack" ✓ (same prometheus-client + the shared `crabka-telemetry`).
- **Global metrics** (`OnceLock`) avoids threading a handle through `ProduceCore`/`DedupEngine`/`Forwarder`/outbound — far less invasive; prometheus-client handles are Arc-backed so the global is cheap + the `/metrics` render sees all increments.
- **Broker factor** is the second behavior-preserving broker change (after P5's crabka-authz), by explicit user choice; the Kafka-specific `request_span`/`api_name` stay in the broker, only the generic OTLP pipeline moves.
- **Trace-context propagation IN** (W3C `traceparent` from inbound gRPC/HTTP) is deferred — spans are created locally; the propagator is installed so it composes when added.
- **Greenfield:** no compat shims; OTLP off by default (opt-in via env), metrics always on (cheap).
