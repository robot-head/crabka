# Crabka full-signal observability demo

One `docker compose up` brings up Grafana over Crabka's four observability
backends (metrics, traces, logs, profiles). Crabka exports all four of its own
signals into those backends, and an instrumented `crabka-client-streams` orders
pipeline runs its Kafka traffic on Crabka.

A single `crabka-broker` is triple-duty: the demo app's event bus, the
write-ahead log for all four telemetry backends, and a self-observed subject.
One Grafana Alloy collects every signal from both sources (Crabka's own
processes and the demo app) and writes to the backends, which persist through
the broker (WAL) and a shared RustFS bucket set (blocks).

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

Tune the load with `CRABKA_DEMO_ORDERS_PER_SEC` on the `demo-produce` service
(default `50`; `0` pauses production). Lower it on a constrained host. Plan on
**≥ 8 GB** of Docker memory (~21 containers).

The service binaries run under the jemalloc allocator with heap profiling active
at a coarse sample rate (`lg_prof_sample:25`, roughly one sample per 32 MiB) and
with short dirty/muzzy decay so freed profiler/query pages return to the OS
promptly. This keeps heap flamegraphs representative of long-lived allocations
without the profiler's own backtrace bookkeeping dominating querier RSS.
CPU pprof collection stays enabled but uses 5-second windows per 60-second
scrape to keep profiler overhead bounded.
Alloy's eBPF profiler is CPU-only for native code; Rust memory profiles in this
demo therefore come from the services' `/debug/pprof/heap` endpoint.

If a reused RustFS volume grows far beyond the logical bucket sizes shown in the
RustFS dashboard, reset the fixture with `docker compose down -v` and start it
again. Older demo revisions rewrote large logs/traces/profiles index objects on
the same S3 keys; RustFS can leave those old physical parts on disk even after
the latest S3 object is small or deleted. Current images use append-style log
shards and immutable trace/profile index snapshots to avoid new overwrite churn.

When local Docker disk use looks high, check the named volumes separately from
the live S3-visible object sizes:

```bash
docker system df -v
docker volume ls --filter name=crabka-observability-demo
```

The demo's persistent footprint is normally dominated by
`crabka-observability-demo_rustfs-data` and `crabka-observability-demo_broker-data`.
If a host was used with older demo images, RustFS may still contain many
UUID-named backend directories below legacy keys such as
`crabka-traces/index/traces.json`, `crabka-profiles/index/profiles.json`, and
`crabka-logs/logs/tenant=demo/index/logs/manifest.json`. Those directories are
old overwrite generations, not additional logical S3 objects. Recreating the
compose volumes is the reliable way to reclaim them. The old MinIO fixture used
`crabka-observability-demo_minio-data`; after switching to RustFS, that volume is
not used by this compose file and can be removed if no older checkout still
needs it.

The traces block-builder has its own replay cap
(`CRABKA_TRACES_BLOCK_BUILDER_MEM`, default `4g`) and a lower flush size
(`CRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_RECORDS`, default `5000`). Cold starts
can replay a burst of broker self-traces before steady state, so these settings
avoid a replay-time OOM while keeping the normal RSS small.

### Rebuild from source

To build the image locally instead of pulling it (e.g. to try local changes),
build + tag it under the same name from the **repo root** with melange + apko,
then start as usual — Compose uses the local image when present:

```bash
go install chainguard.dev/melange@latest
go install chainguard.dev/apko@latest

mkdir -p packages .melange-cache
melange keygen melange.rsa
melange build packaging/melange/crabka-demo.yaml \
  --source-dir . \
  --signing-key melange.rsa \
  --arch x86_64 \
  --runner docker \
  --cache-dir "$PWD/.melange-cache" \
  --out-dir packages/

apko build packaging/apko/crabka-demo.yaml \
  ghcr.io/robot-head/crabka-demo:latest \
  crabka-demo.tar \
  --arch x86_64 \
  --repository-append "$PWD/packages" \
  --keyring-append "$PWD/melange.rsa.pub"

docker load < crabka-demo.tar
cd demo/observability && docker compose up -d
```

Maintainers publish the prebuilt image with the **publish-demo-image** GitHub
Actions workflow (Actions → *publish-demo-image* → *Run workflow* → image tag).

## What you should see

- **Explore → Crabka Metrics** (Prometheus): `{job="broker"}` — the broker's own metrics.
- **Explore → Crabka Logs** (Loki): `{service_name="crabka-logs"}`, `{service_name="observability-demo-app"}` — JSON logs.
- **Explore → Crabka Traces** (Tempo): TraceQL `{}` — broker + demo-app spans.
- **Explore → Crabka Profiles** (Pyroscope): Crabka services — CPU + heap flamegraphs; demo app roles — CPU flamegraphs.
- The **“Crabka observes Crabka”** dashboard (folder *Crabka*) shows one panel
  per signal plus querier heap flamegraphs.

## Dashboards & alerts

Every Crabka service exports Prometheus metrics on its admin port `:9404`
(`/metrics`): the broker via its metrics server, and the four observability
services (metrics/logs/traces/profiles) via the shared profiling-admin server,
across all roles. Alloy scrapes them with a `job` label per compose service, so
the dashboards/alerts select per service and role.

Provisioned dashboards (folder *Crabka*):

- **Crabka — Overview** — fleet liveness, ingest/query rate and error ratio per
  subsystem, broker throughput.
- **Crabka — Runtime Resources** — container CPU, working-set memory, memory
  limit ratio, CPU throttling, and top memory users across Crabka plus Grafana,
  Alloy, cAdvisor, and RustFS.
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
- `../../packaging/melange/crabka-demo.yaml` — builds the all-in-one demo APK package
- `../../packaging/apko/crabka-demo.yaml` — assembles the demo OCI image from that package
- `alloy/config.alloy` — Alloy collects all four signals from both sources and
  scrapes cAdvisor container resource metrics
- `grafana/provisioning/` — datasources, dashboards (overview + broker + one per subsystem), and alert rules
- `rustfs/bootstrap.sh` — creates one bucket per signal (`crabka-metrics`, `crabka-traces`, `crabka-logs`, `crabka-profiles`)
