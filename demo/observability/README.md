# Crabka full-signal observability demo

One `docker compose up` brings up Grafana over Crabka's four observability
backends (metrics, traces, logs, profiles). Crabka exports all four of its own
signals into those backends, and an instrumented `crabka-client-streams` orders
pipeline runs its Kafka traffic on Crabka.

A single `crabka-broker` is triple-duty: the demo app's event bus, the
write-ahead log for all four telemetry backends, and a self-observed subject.
One Grafana Alloy collects every signal from both sources (Crabka's own
processes and the demo app) and writes to the backends, which persist through
the broker (WAL) and a shared MinIO bucket (blocks).

## Run

By default this **pulls the prebuilt image** from GHCR — no local build needed:

```bash
cd demo/observability
docker compose up -d
```

Then open Grafana at <http://localhost:3000> (anonymous admin).

Data appears a few minutes after startup, as Alloy collects signals and the
block-builders flush their first blocks (give it ~3–5 min on a cold start). The
queriers refresh their indexes automatically.

> **Known cold-boot caveat.** Under continuous jemalloc heap profiling, a WAL
> block-builder (most often `logs-compactor` or `profiles-block-builder`) can
> intermittently stall at its consumer group-join on a cold boot — a tokio
> runtime-level lost wakeup that also freezes the in-process timers, so it can't
> self-recover. If a signal's panel is still empty after ~5 minutes, restart
> that service: `docker compose restart logs-compactor` /
> `docker compose restart profiles-block-builder` (a fresh process almost always
> clears it; a bad-luck boot may need a second try). This is a tracked
> follow-up; it does not affect metrics/traces, and the data path itself is
> correct once the consumer joins.

Tune the load with `CRABKA_DEMO_ORDERS_PER_SEC` on the `demo-produce` service
(default `50`; `0` pauses production). Lower it on a constrained host. Plan on
**≥ 8 GB** of Docker memory (~20 containers).

The service binaries run under the jemalloc allocator with heap profiling
active (`MALLOC_CONF=prof:true,prof_active:true`), so both CPU and heap
flamegraphs are available.

### Rebuild from source

To build the image locally instead of pulling it (e.g. to try local changes),
build + tag it under the same name from the **repo root**, then start as usual
— Compose uses the local image when present:

```bash
docker build -f demo/observability/Dockerfile -t ghcr.io/robot-head/crabka-demo:latest .
cd demo/observability && docker compose up -d
```

Maintainers publish the prebuilt image with the **publish-demo-image** GitHub
Actions workflow (Actions → *publish-demo-image* → *Run workflow* → image tag).

## What you should see

- **Explore → Crabka Metrics** (Prometheus): `{job="broker"}` — the broker's own metrics.
- **Explore → Crabka Logs** (Loki): `{service_name="crabka-logs"}`, `{service_name="observability-demo-app"}` — JSON logs.
- **Explore → Crabka Traces** (Tempo): TraceQL `{}` — broker + demo-app spans.
- **Explore → Crabka Profiles** (Pyroscope): service `broker` / `observability-demo-app` — CPU + heap flamegraphs.
- The **“Crabka observes Crabka”** dashboard (folder *Crabka*) shows one panel per signal.

## Dashboards & alerts

Every Crabka service exports Prometheus metrics on its admin port `:9404`
(`/metrics`): the broker via its metrics server, and the four observability
services (metrics/logs/traces/profiles) via the shared profiling-admin server,
across all roles. Alloy scrapes them with a `job` label per compose service, so
the dashboards/alerts select per service and role.

Provisioned dashboards (folder *Crabka*):

- **Crabka — Overview** — fleet liveness, ingest/query rate and error ratio per
  subsystem, broker throughput.
- **Crabka — Broker** — Kafka throughput, produce/fetch, partitions, ISR &
  controller health, and FedRAMP-MLA audit pipeline.
- **Crabka — Metrics / Logs / Traces / Profiles** — per-subsystem RED: ingest
  rate/bytes/errors/latency (distributor) and query rate/errors/p99 latency by
  route (querier), plus WAL append failures and per-role liveness.

Provisioned Grafana-managed alerts (folder *Crabka Alerts*,
`grafana/provisioning/alerting/`): broker (no active controller, offline
partitions, under-min-ISR, under-replicated, audit write failures) and
observability (service down; per-subsystem ingest error ratio > 5%; per-subsystem
query p99 > 5s). To see one fire, stop a service
(`docker compose stop traces-querier`) — **Observability service down** moves to
Firing within ~1 minute; `docker compose start traces-querier` resolves it.

## Smoke check (all four signals)

```bash
# metrics (Prometheus API) — the broker's own request counter
curl -s -H 'X-Scope-OrgID: demo' \
  'http://localhost:9090/api/v1/query?query=crabka_broker_api_requests_total' | head -c 200
# logs (Loki labels)
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:3100/loki/api/v1/labels'
# traces (TraceQL search — any service)
curl -s -H 'X-Scope-OrgID: demo' \
  'http://localhost:3200/api/search?q=%7B%20.service.name%20!%3D%20%22%22%20%7D' | head -c 200
# profiles (Pyroscope) — lists process_cpu + memory (heap) types
curl -s -H 'X-Scope-OrgID: demo' -X POST -H 'content-type: application/json' \
  'http://localhost:4040/querier.v1.QuerierService/ProfileTypes' -d '{}' | head -c 200
```

## Layout

- `docker-compose.yml` — the stack
- `Dockerfile` — single image with every Crabka binary + the demo app
- `alloy/config.alloy` — Alloy collects all four signals from both sources
- `grafana/provisioning/` — datasources, dashboards (overview + broker + one per subsystem), and alert rules
- `minio/bootstrap.sh` — creates one bucket per signal (`crabka-metrics`, `crabka-traces`, `crabka-logs`, `crabka-profiles`)
