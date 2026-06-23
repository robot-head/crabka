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

```bash
cd demo/observability
docker compose up --build      # first run builds the crabka-demo image (long)
```

Then open Grafana at <http://localhost:3000> (anonymous admin).

Tune the load with `CRABKA_DEMO_ORDERS_PER_SEC` on the `demo-produce` service
(default `50`; `0` pauses production). Lower it on a constrained host. Plan on
**≥ 8 GB** of Docker memory (~20 containers).

## What you should see

- **Explore → Crabka Metrics** (Prometheus): `{job="broker"}` — the broker's own metrics.
- **Explore → Crabka Logs** (Loki): `{service_name="crabka-logs"}`, `{service_name="observability-demo-app"}` — JSON logs.
- **Explore → Crabka Traces** (Tempo): TraceQL `{}` — broker + demo-app spans.
- **Explore → Crabka Profiles** (Pyroscope): service `broker` / `observability-demo-app` — CPU + heap flamegraphs.
- The **“Crabka observes Crabka”** dashboard (folder *Crabka*) shows one panel per signal.

## Smoke check (all four signals)

```bash
# metrics (Prometheus API)
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:9090/api/v1/query?query=up' | head -c 200
# logs (Loki labels)
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:3100/loki/api/v1/labels'
# traces (TraceQL search)
curl -s -H 'X-Scope-OrgID: demo' 'http://localhost:3200/api/search?q=%7B%7D' | head -c 200
# profiles (Pyroscope label names)
curl -s -H 'X-Scope-OrgID: demo' -X POST -H 'content-type: application/json' \
  'http://localhost:4040/querier.v1.QuerierService/LabelNames' -d '{}' | head -c 200
```

## Layout

- `docker-compose.yml` — the stack
- `Dockerfile` — single image with every Crabka binary + the demo app
- `alloy/config.alloy` — Alloy collects all four signals from both sources
- `grafana/provisioning/` — datasources + starter dashboard
- `minio/bootstrap.sh` — creates the `crabka-blocks` bucket
