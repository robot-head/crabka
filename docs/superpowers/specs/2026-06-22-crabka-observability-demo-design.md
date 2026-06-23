# Crabka full-signal observability demo — design

- **Status:** Approved (brainstorming) → ready for implementation plan
- **Date:** 2026-06-22
- **Author:** Matthew Stone (with Claude)
- **Topic:** A `docker compose` demo that stands up Grafana over Crabka's four
  observability backends (metrics, traces, logs, profiles), with Crabka
  exporting all four of its own signals into those backends, plus a purpose-built
  Kafka-Streams demo app — built on Crabka's own `client-streams` — that runs its
  event bus on Crabka and is instrumented for all four signals via Alloy.

## 1. Goal

Produce a single `docker compose up` fixture that demonstrates Crabka as a
self-contained observability platform. The demo must show, end to end and live
in Grafana:

1. **Crabka observing Crabka.** The broker and the four observability backend
   services emit their own metrics, traces, logs, and profiles, which are
   ingested by Crabka's own backends and viewed in Grafana.
2. **A real workload on Crabka.** A purpose-built Rust orders-analytics pipeline,
   written against `crabka-client-streams`, runs its Kafka traffic on
   `crabka-broker` and is fully instrumented for all four signals.
3. **One collector.** Grafana Alloy collects every signal from both sources and
   writes to the four Crabka backends.
4. **One Grafana.** Four provisioned datasources (Prometheus, Tempo, Loki,
   Pyroscope) plus starter dashboards, all pointed at Crabka.

The punchline is that **one `crabka-broker` is triple-duty**: the demo app's
business event bus, the write-ahead-log substrate for all four telemetry
backends, and a self-observed subject.

## 2. Non-goals / scope boundaries

- **Not CI-gated.** Per-signal wire compatibility with Grafana is already proven
  by the existing differential and `grafana_e2e` integration tests
  (`crates/integration-tests/tests/grafana_e2e.rs`,
  `crates/*/tests/*_differential.rs`). This fixture is a manual showcase; the
  README documents a manual smoke check. No new CI job.
- **Single-node broker.** One broker (combined controller+broker roles). The WAL
  dogfooding story does not need HA; a 3-node cluster is out of scope.
- **No backwards-compatibility shims.** Greenfield per `CLAUDE.md`. Where the
  fixture needs schema/format choices, pick one.
- **Crabka self-profiles are CPU + heap only.** No off-CPU/lock/async profiling
  for Crabka's own processes in v1.
- **No new query-language or backend features**, with one small exception: to
  make the object store **uniform** across all four backends (§4.3), `crabka-profiles`
  and the metrics binaries gain `--object-store-url` S3 support via
  `object_store::parse_url_opts(&url, std::env::vars())` — the exact pattern
  `crabka-observability` already uses. This is a localized object-store wiring
  change, not query/format work.

## 3. Architecture

Two telemetry **sources**, one **collector** (Alloy), four Crabka **backends**
that persist through Crabka's own broker (WAL) and a shared MinIO object store
(blocks), one **Grafana**.

```
SOURCE A: crabka components                 SOURCE B: orders-analytics demo app
  crabka-broker                               Rust pipeline on crabka-client-streams
  + metrics/traces/logs/profiles services     (StreamsBuilder: windowed orders agg
  (every process self-instrumented:            + KTable enrichment), produces &
   /metrics, OTLP traces, JSON logs,           consumes on crabka-broker:9092
   /debug/pprof/{profile,heap})               (self-instrumented, same pattern)
         │                                            │
         └───────────────────────┬────────────────────┘
                                 ▼
                          Grafana Alloy
        prometheus.scrape  /metrics            → prometheus.remote_write → crabka-metrics
        otelcol.receiver.otlp (traces)         → otelcol.exporter.otlp   → crabka-traces
        loki.source (container/stdout logs)    → loki.write              → crabka-logs
        pyroscope.scrape /debug/pprof/*        → pyroscope.write         → crabka-profiles
                                 │
   ┌────────────────┬───────────┼────────────┬─────────────────┐
 crabka-metrics   crabka-traces  crabka-logs   crabka-profiles
 (Prom/Mimir)     (Tempo)        (Loki)        (Pyroscope)
   each backend:  distributor ──[WAL]──▶ block-builder/compactor ──[blocks]──▶ querier
                       │                                                          │
                       └────────────── WAL = crabka-broker (Kafka topics) ───────┘
                                          blocks = MinIO (shared S3 bucket, per-signal prefix)
                                 ▲
                              Grafana
              4 datasources (Prometheus/Tempo/Loki/Pyroscope) → the four queriers
              + provisioned dashboards (crabka-self + demo-app) + Explore
```

**Why Alloy collects Crabka too.** Three of Crabka's four self-signals are
pull/collector-shaped: metrics need a scraper to remote-write, logs are stdout
JSON that needs tailing, profiles need `pyroscope.scrape`. Only traces push
directly (broker OTLP). Routing everything through one Alloy keeps a single
mental model — *everything → Alloy → Crabka backends → Grafana* — and lets Alloy
attach uniform resource attributes.

## 4. Components

### 4.1 crabka-broker (triple duty)

- One container, combined `controller,broker` roles, Kafka on `:9092`.
- Already self-instruments three signals (no change needed beyond config):
  - **Metrics:** Prometheus `/metrics` on `:9404`
    (`crates/broker/src/bin/broker.rs:96`).
  - **Traces:** OTLP span export via `crabka-telemetry`, enabled by setting
    `CRABKA_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT`
    (`crates/telemetry/src/lib.rs`, wired at `broker.rs:140-147`).
  - **Logs:** structured-JSON to stdout via the telemetry `fmt` layer.
- **New:** gains the in-process profiler admin routes (§5).
- Hosts both the demo app's business topics and the four `__crabka_*_wal`
  telemetry WAL topics.

### 4.2 The four backends

Each backend runs its faithful role graph from a single shared Crabka image,
selecting the role via `command:`. WAL = broker Kafka topics; blocks = MinIO.

| Signal | Binary | Roles run | Ingest API | Query API (Grafana datasource) |
|---|---|---|---|---|
| Metrics | `crabka-metrics` (+ `crabka-metrics-service` for read path) | distributor, compactor, querier | Prometheus remote-write `POST /api/v1/push`; OTLP `/otlp/v1/metrics` | Prometheus HTTP API `/api/v1/query*` → **Prometheus** DS |
| Traces | `crabka-traces` | distributor, block-builder, querier | OTLP `/v1/traces` (gRPC 4317 / HTTP 4318) | Tempo HTTP `/api/v2/traces/{id}`, `/api/search` → **Tempo** DS |
| Logs | `crabka-logs` (**new binary**, §5.1) over `crabka-observability` | distributor, compactor, querier | Loki push `POST /loki/api/v1/push`; OTLP `/v1/logs` | Loki HTTP `/loki/api/v1/query_range`, `/labels` → **Loki** DS |
| Profiles | `crabka-profiles` | distributor, block-builder, querier | Pyroscope `push.v1.PusherService/Push`, legacy `/ingest` | Pyroscope `querier.v1.QuerierService` + `/pyroscope/render` → **Pyroscope** DS |

Role enums confirmed in code: metrics `Target::{Distributor,Compactor,Querier,
QueryFrontend,Ruler}` (`crates/metrics/src/bin/crabka-metrics.rs`); traces
`Target::{Distributor,BlockBuilder,LiveStore,Querier,…}`
(`crates/traces/src/bin/crabka-traces.rs`); profiles
`Target::{Distributor,BlockBuilder,Querier,QueryFrontend,Compactor,Symbolizer}`
(`crates/profiles/src/bin/crabka-profiles.rs`, default `--listen 127.0.0.1:4040`,
`--bootstrap 127.0.0.1:9092`); logs `Role::{Distributor,Compactor,Querier}`
(`crates/observability/src/lib.rs:101`).

> **Implementation note — metrics binary split.** Ingest/compaction live in
> `crabka-metrics`; the read path (`Querier`/`QueryFrontend`/`Ruler`) is served by
> `crabka-metrics-service` (`crates/metrics-service/src/main.rs`). The plan must
> pick the correct binary+target per role and confirm each role's `--listen`
> default and required WAL/object-store flags. Both binaries ship in the shared
> image, so this is a `command:` decision, not a packaging one.

### 4.3 MinIO (shared object store) — uniform across all four backends

- One MinIO container; one bucket (`crabka-blocks`) with a per-signal prefix
  (`s3://crabka-blocks/metrics`, `/traces`, `/logs`, `/profiles`). A small
  bootstrap step creates the bucket on startup.
- Each backend points `--object-store-url s3://crabka-blocks/<signal>` at MinIO.
  S3 endpoint + credentials come from env consumed by the `object_store` crate:
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`,
  `AWS_ALLOW_HTTP=true`, `AWS_REGION` (placeholder `us-east-1` for MinIO).
- **Uniformity is a deliberate choice and required a small code change.** Today
  only `crabka-traces` and `crabka-observability` (logs) accept an object-store
  URL; `crabka-profiles` and `crabka-metrics`/`crabka-metrics-service` are
  local-FS-only (`LocalFileSystem::new_with_prefix`). The plan adds
  `--object-store-url` + `object_store::parse_url_opts(&url, std::env::vars())`
  to profiles and the metrics binaries (mirroring observability), and switches
  `crabka-traces` to `parse_url_opts` so the MinIO endpoint/credential env is
  applied consistently. This avoids cross-container filesystem-sharing fragility
  and is the real S3 path a production deployment would use.

### 4.4 Grafana Alloy (the single collector)

One Alloy container with four collection pipelines, each fed by both sources:

- **Metrics:** `prometheus.scrape` of every Crabka process `/metrics` (broker
  `:9404` + each service's admin port) **and** the demo app `/metrics` →
  `prometheus.remote_write` to `crabka-metrics` distributor `/api/v1/push`.
- **Traces:** `otelcol.receiver.otlp` (gRPC/HTTP) receiving spans pushed by
  every Crabka process and the demo app → `otelcol.exporter.otlp` to
  `crabka-traces` distributor `:4317`.
- **Logs:** `loki.source` reading container stdout (Docker log files / the
  `loki.source.docker` discovery) → `loki.write` to `crabka-logs` distributor
  `/loki/api/v1/push`. Crabka's JSON log lines carry fields natively.
- **Profiles:** `pyroscope.scrape` of every Crabka process and the demo app at
  `/debug/pprof/profile` (CPU) and `/debug/pprof/heap` (heap) →
  `pyroscope.write` to `crabka-profiles` distributor.

Config lives at `demo/observability/alloy/config.alloy`.

### 4.5 Grafana

- One container, anonymous admin, provisioned from
  `demo/observability/grafana/provisioning/`:
  - **Datasources** (`datasources/crabka.yaml`): Prometheus → metrics querier,
    Tempo → traces querier, Loki → logs querier, Pyroscope → profiles querier.
    Trace-to-logs / trace-to-profiles correlations wired where the field maps are
    trivial.
  - **Dashboards** (`dashboards/*.json`): a **"Crabka observes Crabka"** board
    (broker + service throughput, request latencies, WAL lag, self CPU/heap
    flamegraphs, self logs) and a **demo-app** board (orders processed, window
    emit rate, end-to-end trace exemplars, consumer lag, app CPU/heap).

### 4.6 Orders-analytics demo app (`crates/observability-demo-app`)

A purpose-built Rust workload, idiomatic to Crabka, modeled closely on
`crates/client-streams/examples/protobuf_pipeline.rs` and the `order` proto
domain (`crates/client-streams/examples/proto/order.proto`). Values are
**Protobuf** `Order` messages encoded with `SchemaSerde<Order, ProtobufSerde<Order>>`
and registered against a **Crabka schema registry** (§4.7) — the same path the
protobuf example uses, so the demo dogfoods Crabka's schema-registry too.

- **Producer task** generates a synthetic stream of proto `Order` events
  (varied category/amount, occasional anomalous records to produce error spans and
  warn logs), framed in the Confluent wire format via the schema registry, keyed
  by category → input topic on `crabka-broker`.
- **Tuning — order volume.** The producer's target emit rate is configurable via
  `CRABKA_DEMO_ORDERS_PER_SEC` (env var, default `50`), settable in
  `docker-compose.yml` without a rebuild. This is the demo's primary load lever:
  it scales how hard the pipeline runs and therefore how much telemetry (span,
  log, metric, and profile volume) every stage emits — dial it down for
  low-memory hosts, up to show the system under sustained load. A value of `0`
  pauses production (useful for inspecting a quiescent pipeline). The producer
  paces to the target rate rather than emitting a fixed total, so the dashboards
  stay live indefinitely.
- **Streams topology** built via `StreamsApp` + `StreamsBuilder`
  (`app.streams_builder()`): consume proto `Order` from the input topic,
  `group_by_key` (category) → `count`/aggregate into a state store → `to_stream`
  → output topic. This exercises proto deserialize, the state store + changelog,
  and serialization — generating meaningful CPU/heap profiles and per-record
  trace spans. (Topology uses only the verified DSL surface; tumbling-window
  aggregation is an optional enhancement if the `windowed_by`/`TimeWindows`
  constructors are confirmed during implementation.)
- **Consumer task** reads the aggregated output (drives end-to-end traces and
  consumer-lag metrics).
- **Instrumentation reuses Crabka's own libraries** (the same pattern as the
  backends — see §5): `crabka-telemetry` for OTLP traces + JSON logs, a
  `/metrics` Prometheus endpoint, and the in-process profiler routes.
- May run as 1–3 containers (producer / streams-processor / consumer) or a single
  multi-task process; the plan picks based on how cleanly the three roles
  separate. Default target: a single image, multiple `command:` roles.

### 4.7 Schema registry

- One `crabka-schema-registry` container (Confluent-compatible HTTP API on
  `:8081`, run with `--bootstrap-servers broker:9092 --schemas-topic-rf 1` for the
  single-node cluster), backed by `crabka-broker` — another Crabka component in
  the loop.
- The demo app's producer and Streams consumer both resolve proto schemas
  against it (`StreamsApp::builder().schema_registry("http://schema-registry:8081")`).
- Self-instrumentation reality (verified): the binary already emits **structured
  JSON logs** via `crabka-logfmt` to stdout (so Alloy ships its logs for free) but
  does **not** depend on `crabka-telemetry` and exposes no `/metrics`. The plan
  adds the profiler admin server (§5.2) for its profiles signal; wiring its
  traces/metrics is optional and out of the critical path (logs alone are enough
  to show it in Grafana).

## 5. Self-instrumentation: one pattern everywhere

The broker, the four backend services, and the demo app are instrumented
**identically**, using Crabka's own libraries. This is the design's unifying
idea and keeps Alloy's config uniform.

| Signal | Mechanism | Surface |
|---|---|---|
| Traces | `crabka-telemetry` OTLP exporter, enabled by `CRABKA_OTLP_ENDPOINT` | OTLP push → Alloy `:4317` |
| Logs | `crabka-telemetry` structured-JSON `fmt` layer | stdout → Alloy `loki.source` |
| Metrics | `prometheus-client` (broker's existing pattern) | `GET /metrics` on an admin port |
| Profiles | in-process profiler (new, §5.2) | `GET /debug/pprof/profile` (CPU), `GET /debug/pprof/heap` (heap) |

### 5.1 New `crabka-logs` binary

The logs backend is complete but **library-only** — `crabka-observability`
exports `build_service_router`, `loki_router`, `distributor_router`, the `Role`
enum, and `ServiceConfig`/`ServiceDependencies` (`crates/observability/src/lib.rs`),
and the differential tests drive them directly, but there is no entrypoint.

Add a thin `crabka-logs` binary (a `[[bin]]` in `crabka-observability`, or a
small wrapper crate) that mirrors `crates/metrics-service/src/main.rs`: parse a
`--target {distributor,compactor,querier}` (the `Role` enum), build a
`ServiceConfig` (listen addr, WAL bootstrap = broker, object-store URL = MinIO,
index prefixes), call `build_service_router`, and serve with graceful shutdown.
Loki default port `3100`.

### 5.2 In-process profiler module

A shared helper (e.g. `crabka-telemetry::profiling` or a small new module reused
by all binaries) that exposes an **admin HTTP server** carrying both
`/metrics` and the pprof routes, so Alloy scrapes one port per process:

- **CPU:** the `pprof` crate (SIGPROF/ITIMER sampling) rendered to pprof protobuf
  at `GET /debug/pprof/profile?seconds=N`.
- **Heap:** jemalloc sampling via `tikv-jemallocator` as the global allocator
  with `MALLOC_CONF=prof:true,prof_active:true`, dumped as pprof protobuf at
  `GET /debug/pprof/heap` via the `jemalloc_pprof` crate.

Profiles are symbolized in-process, so they arrive at `crabka-profiles`
pre-symbolized (the backend's `Symbolizer` role / debuginfod is not needed for
the demo). **The demo image must retain debug symbols** for the profiled
binaries so flamegraphs are readable (no `strip`).

### 5.3 `heap-profiling` cargo feature

A `#[global_allocator]` swap must be compiled in and must **not** ship in
normal/bench/prod builds (jemalloc would perturb the Strimzi benchmark pipeline).
So heap profiling is gated behind a **default-off `heap-profiling` cargo
feature** that (a) sets jemalloc as the global allocator and (b) enables the
`/debug/pprof/heap` route. The demo image builds with `--features heap-profiling`;
everything else is untouched.

This is a deliberate, accepted exception to `CLAUDE.md`'s "no feature flags that
gate new behavior" rule: that rule targets runtime/Kafka-compat behavior gates,
whereas this is the standard Rust idiom for a build-time allocator/observability
selection. CPU profiling has no allocator dependency and is always available.

### 5.4 Single Crabka Docker image

One multi-stage `demo/observability/Dockerfile` that `cargo build --release
--features heap-profiling` produces every binary needed —
`crabka-broker`, `crabka-metrics`, `crabka-metrics-service`, `crabka-traces`,
`crabka-logs`, `crabka-profiles`, and the demo app — into a single runtime image.
Compose selects role/binary per service via `command:`. Build retains debug
symbols for readable profiles.

### 5.5 Packaging & publish flags

`release-plz` publishes workspace crates to crates.io. The demo crate and the
observability **backend** crates must never be published; this work makes that
explicit. Dependency direction was checked so no publishable product crate
(broker, cli, operator, rebalancer, grpc-gateway, schema-registry, audit) breaks.

- **New crate** `crates/observability-demo-app` → `publish = false`.
- **New `crabka-logs` binary** lives in `crabka-observability` (already
  `publish = false`); if instead a separate wrapper crate, that wrapper is
  `publish = false`.
- **Flip to `publish = false`** (currently publishable; every dependent is itself
  an observability crate — verified no product crate depends on them):
  `crabka-metrics`, `crabka-metrics-service`, `crabka-promql`, `crabka-logql`,
  and `crabka-observability-spike`.
- **Already `publish = false`** (no change): `crabka-observability`,
  `crabka-traces`, `crabka-traceql`, `crabka-profiles`, `crabka-pprof`,
  `crabka-blockstore`.
- **Deliberately kept publishable** — shared instrumentation/util/core libs that
  publishable product binaries depend on, *not* observability backends:
  - `crabka-telemetry` — `crabka-broker` + `crabka-grpc-gateway` depend on it.
  - `crabka-logfmt` — "structured-JSON tracing log formatter shared across Crabka
    services"; `operator`/`replicator`/`schema-registry`/`telemetry` depend on it.
  - `crabka-log` — core partition-log storage; `broker`/`raft`/`audit` depend on
    it (named log-* but unrelated to logs observability).

Net effect: the LGTM+P backends and the demo are non-publishable, while the
shared libs product binaries rely on stay intact.

## 6. Signal routing (end to end)

| Signal | Source emits | Alloy stage | Backend ingest | Backend query | Grafana DS |
|---|---|---|---|---|---|
| Metrics | `/metrics` (Prom) | `prometheus.scrape` → `remote_write` | `crabka-metrics` `/api/v1/push` | `/api/v1/query*` | Prometheus |
| Traces | OTLP push | `otelcol.receiver.otlp` → `exporter.otlp` | `crabka-traces` `:4317` `/v1/traces` | `/api/v2/traces`, `/api/search` | Tempo |
| Logs | JSON stdout | `loki.source.docker` → `loki.write` | `crabka-logs` `/loki/api/v1/push` | `/loki/api/v1/query_range` | Loki |
| Profiles | `/debug/pprof/*` | `pyroscope.scrape` → `pyroscope.write` | `crabka-profiles` Push/`/ingest` | `querier.v1` / `/pyroscope/render` | Pyroscope |

## 7. Repository layout

```
demo/observability/
  docker-compose.yml          # the whole stack
  Dockerfile                  # single crabka all-binaries + demo-app image
  README.md                   # quick start + manual smoke checklist + what to open
  alloy/
    config.alloy              # 4 pipelines, both sources → 4 backends
  grafana/
    provisioning/
      datasources/crabka.yaml # Prometheus/Tempo/Loki/Pyroscope → crabka queriers
      dashboards/
        dashboards.yaml        # provider
        crabka-self.json       # "Crabka observes Crabka"
        demo-app.json          # orders-analytics
  minio/
    bootstrap.sh              # create the crabka-blocks bucket
crates/observability-demo-app/
  Cargo.toml
  build.rs                    # prost/protox proto codegen (Order)
  proto/order.proto          # proto Order domain
  src/...                     # producer + StreamsApp pipeline + consumer, instrumented
```

(The stack also runs a `crabka-schema-registry` container, §4.7 — no new repo
files; it ships in the same image.)

## 8. How it runs (manual)

1. `cd demo/observability && docker compose up --build`
   (optionally set `CRABKA_DEMO_ORDERS_PER_SEC` in `docker-compose.yml` first to
   scale the demo's order volume / telemetry load up or down).
2. Wait for healthchecks (broker → backends → Alloy → demo app → Grafana).
3. Open Grafana at `http://localhost:3000`.
4. Explore each datasource and open the two provisioned dashboards.
5. README documents the smoke check: each datasource returns Crabka-originated
   data (broker self-metrics, broker self-traces, broker JSON logs, broker
   CPU+heap flamegraphs) **and** demo-app data for all four signals.

## 9. Risks & open questions (resolve in the plan)

- **Metrics binary/role split** (§4.2 note) — confirm exact binary+target+flags
  per metrics role.
- **Object-store consistency across roles** — block-builder writes, querier
  reads via MinIO; confirm refresh cadence makes recently ingested data appear
  within the demo's patience window (queriers poll a manifest/WAL head).
- **Profile readability** — verify in-process `pprof`/`jemalloc_pprof` output
  symbolizes against a non-stripped release image; confirm Alloy `pyroscope.scrape`
  accepts the pprof endpoints (godeltaprof vs raw pprof format).
- **Resource footprint** — ~18–19 containers (incl. schema-registry); document a minimum Docker memory
  (likely ≥ 6–8 GB) in the README; offer a trimmed profile if needed. Note that
  `CRABKA_DEMO_ORDERS_PER_SEC` (§4.6) is the first knob to turn down on a
  constrained host.
- **Alloy config drift** — Alloy river/`.alloy` syntax changes across versions;
  pin the Alloy image tag.
- **Broker OTLP self-export loop** — the broker exports its own spans to Alloy →
  `crabka-traces`; ensure trace/log volume from the telemetry path itself does
  not create a runaway feedback loop (sampling / exclude self-ingest spans if
  needed).

## 10. Verification

Manual only (per §2). The README's smoke checklist is the acceptance test:
all four datasources show both Crabka-self and demo-app data after
`docker compose up`. No new automated CI job.
