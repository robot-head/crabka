# Slice 42 — Crabka core: OTLP distributed tracing (design)

**Date:** 2026-05-23
**Status:** Implemented
**Phase:** 6 (Observability). Continues 39 (metrics exporter), 40 (`metricsConfig`), 41 (logging). Crabka-core slice; the operator-surfacing follow-up (`Kafka.spec.tracing`) is a later slice.

## Goal

Give the broker an OpenTelemetry tracing pipeline that batch-exports spans
over OTLP (gRPC `:4317` or HTTP/protobuf `:4318`) to a collector, plus a
per-request server span so a trace shows the API, version, client, and peer
of each handled request alongside whatever the existing handlers log. OTLP
is **off by default** and driven entirely by environment variables, so a
broker without OTLP env behaves exactly as before.

## Why broker-side root spans (not record-header propagation)

Apache Kafka has no broker-side OTel instrumentation. Distributed tracing in
the Kafka ecosystem lives on the **client** side (the OpenTelemetry Kafka
interceptors put a W3C `traceparent` in **record headers**, not in the RPC
request header). The Kafka request header (`RequestHeader.json`,
`flexibleVersions: none` on its fields) carries no trace-context field, so
there is nothing to extract from an inbound request to continue a client's
trace at the RPC boundary.

This slice therefore emits **server root spans per request**. Linking a
broker span to the producing client's trace would require parsing
`traceparent` out of each record's headers inside the Produce path — a
deeper, hot-path-sensitive feature deferred to a follow-up (see Out of
scope). The pipeline installed here is exactly what that follow-up would
build on.

## Module: `crabka_broker::telemetry`

A single new lib module owns the whole pipeline; the broker bin calls it.

- `OtlpConfig::from_env(get, instance_id, version) -> Option<Self>` — a pure,
  injectable env resolver (the `get` closure is the only I/O), returning
  `None` when OTLP is disabled. Fully unit-tested without touching the global
  subscriber.
- `init(otlp, default_filter) -> Result<TelemetryGuard, _>` — installs the
  global subscriber: always a stdout `fmt` layer (the existing `RUST_LOG`
  behaviour), and when `otlp` is `Some`, a `tracing-opentelemetry` layer
  feeding a batch OTLP exporter. Returns a guard whose `shutdown()` flushes
  the final batch before the process exits.
- `request_span(api_key, api_version, correlation_id, client_id, peer)` —
  builds the per-request span (see below).
- `api_name(api_key) -> &'static str` — Kafka API name table for span names.

### Enabling / configuration (env)

OTLP turns **on** when any endpoint is set or it is explicitly enabled, and
is force-**off** by `OTEL_SDK_DISABLED`. Crabka-specific vars take precedence
over the standard OTel vars so the operator follow-up has a stable surface:

| Setting        | Crabka var                  | Standard OTel fallback                                            | Default |
|----------------|-----------------------------|------------------------------------------------------------------|---------|
| Enable         | `CRABKA_OTLP_ENABLED=true`  | (any endpoint var being set)                                      | off     |
| Endpoint       | `CRABKA_OTLP_ENDPOINT`      | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` → `OTEL_EXPORTER_OTLP_ENDPOINT` | per-protocol localhost |
| Protocol       | `CRABKA_OTLP_PROTOCOL`      | `OTEL_EXPORTER_OTLP_PROTOCOL` (`grpc` \| `http/protobuf`)         | `grpc`  |
| Sample ratio   | `CRABKA_OTLP_SAMPLE_RATIO`  | `OTEL_TRACES_SAMPLER_ARG`                                         | `1.0`   |
| Service name   | —                           | `OTEL_SERVICE_NAME`                                               | `crabka-broker` |
| Export timeout | `CRABKA_OTLP_TIMEOUT_SECS`  | `OTEL_EXPORTER_OTLP_TIMEOUT_SECS`                                 | `10`    |
| Disable        | —                           | `OTEL_SDK_DISABLED=true`                                          | —       |

Resource attributes: `service.name`, `service.version` (crate version),
`service.instance.id` (broker id). Sampler is
`ParentBased(TraceIdRatioBased(ratio))` so a sampled upstream decision is
honoured if/when record-header propagation lands.

## Per-request span = a dedicated `DEBUG` target

The hot path must stay free when OTLP is off. The request span is emitted on
the dedicated target `crabka_broker::request` at `DEBUG`:

- The `fmt` layer keeps the operator's `RUST_LOG` (default `info`), which
  does **not** enable that target — so request spans never reach stdout and
  never materialise on a no-OTLP broker (a disabled-level check).
- The OTLP layer gets its **own** per-layer filter
  (`info,crabka_broker::request=debug,crabka_log=info`, overridable via
  `CRABKA_OTLP_FILTER`). With per-layer filters, a span is created if **any**
  layer enables it — so request spans exist only when the OTLP layer is
  present, and only it records them.

In the dispatch loop, span construction is itself guarded by
`tracing::enabled!(target: REQUEST_TARGET, DEBUG)`, so the extra header parse
needed to populate span fields never runs unless OTLP is on.

### Span shape (OTel semantic conventions)

Name = the API name (`Produce`, `Fetch`, …) via the `otel.name` field;
`otel.kind = server`; attributes `messaging.system=kafka`,
`kafka.api_key`, `kafka.api_version`, `kafka.correlation_id`,
`messaging.kafka.client_id`, `network.peer.address`.

## Dispatch instrumentation (uniform, additive)

`serve_connection_stream` runs one logical request per loop iteration via a
chain of 30 inline `handle_*_frame(&broker, &frame, &auth, &peer).await`
intercept arms plus a generic `dispatch_one(&broker, &frame).await` fallback
(and a synchronous SASL path). Rather than refactor that 700-line loop, the
span is built once at the top of the iteration and attached to each handler
future with `.instrument(req_span.clone())` (and `req_span.in_scope(..)` for
the sync SASL path). This is purely additive — control flow is unchanged, and
when `req_span` is disabled the `.instrument`/`.clone` calls are no-ops. Every
api_key — inline or generic — gets a span; there is no coverage gap.

## Dependencies

`opentelemetry` 0.32, `opentelemetry_sdk` 0.32, `opentelemetry-otlp` 0.32
(`grpc-tonic` + `http-proto` + `reqwest-blocking-client`),
`tracing-opentelemetry` 0.33. The 0.32 line lines up with the tonic 0.14 /
prost 0.14 / reqwest 0.13 stack already in the graph, so the lockfile grows
by only 7 crates (opentelemetry{,-http,-otlp,-proto,_sdk}, tonic-types,
tracing-opentelemetry), all Apache-2.0 / MIT.

### Runtime note

The batch span processor exports on a dedicated thread via
`futures_executor::block_on`. The gRPC (tonic) exporter therefore requires
the provider be **built inside a tokio runtime** so it can capture the
runtime handle — `telemetry::init` is called from the broker's
`#[tokio::main]`, which satisfies this. The HTTP/protobuf transport uses the
blocking reqwest client and has no such requirement.

## Tests

11 lib unit tests in `telemetry`: env resolution (disabled/enabled paths,
endpoint precedence across CRABKA/OTel vars, `OTEL_SDK_DISABLED` override,
protocol parsing + defaults, sample-ratio parse/clamp, service-name/timeout
overrides), the `api_name` table, and a `request_span` test that drives a
capturing `tracing` `Layer` (scoped via `with_default`, no global state) to
assert `otel.name=Produce`, `otel.kind=server`, and `kafka.api_key=0`. The
existing broker unit + integration suites exercise the instrumented dispatch
path unchanged (spans disabled), confirming the additive `.instrument`
wrapping is behaviour-preserving.

## Out of scope (deferred)

- **Record-header `traceparent` extraction** in Produce/Fetch to link broker
  spans into client traces — the actual cross-process "distributed" link.
- **OTLP metrics / logs signals** — only traces are wired; metrics stay on
  the slice-39 Prometheus endpoint.
- **Operator surfacing** (`Kafka.spec.tracing` → env injection +
  collector wiring) — a follow-up operator slice.
- **Span events for response error codes / per-partition outcomes** — the
  span captures request metadata + timing + nested handler events; richer
  per-response attributes can follow when there is demand.
