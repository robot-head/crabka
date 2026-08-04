# Crabka full-signal observability demo

One `docker compose up` brings up Grafana over Crabka's four observability
backends (metrics, traces, logs, profiles). Crabka exports all four of its own
signals into those backends, and an instrumented `crabka-client-streams` orders
pipeline runs its Kafka traffic on Crabka.

`crabka-gres`, the PostgreSQL-compatible SQL engine whose write-ahead log lives
in the same broker, runs alongside them and exports a per-query trace
waterfall. See [Gres query traces](#gres-query-traces).

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
(default `50`; `0` pauses production), and the SQL load with
`CRABKA_GRES_WORKLOAD_INTERVAL` on `gres-workload` (default `5` seconds between
passes). Lower both on a constrained host. Plan on **≥ 8 GB** of Docker memory
(~23 containers).

Gres listens on `localhost:5433` as the `demo` tenant, with the SQL password
from `CRABKA_GRES_PASSWORD` (default `demo`):

```bash
PGPASSWORD=demo psql 'host=localhost port=5433 user=demo dbname=demo' \
  -c 'SELECT count(*) FROM demo_orders'
```

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

- **Explore → Crabka Metrics** (Prometheus): `{job=”broker”}` — the broker's own
  metrics; `{__name__=~”crabka_demo_.*”}` — the demo app's business metrics
  (orders by category × region × payment method, order value, per-stage
  processing latency, outcomes).
- **Explore → Crabka Logs** (Loki): `{service_name=”broker”}`,
  `{service_name=~”demo-.*”}` — JSON logs.
- **Explore → Crabka Traces** (Tempo): TraceQL `{}` — broker + demo-app spans.
  **Cross-service distributed traces:** search `{ name = “produce_order” }` (or
  open any `demo-produce` trace) — each traced order is one trace spanning
  **demo-produce → demo-consume**, carried by a W3C `traceparent` Kafka record
  header through the broker. The consumer side shows the `process_order` span
  with its `validate → enrich → fraud_check → fulfill` child stages. The
  observability services also self-instrument their ingest/compaction: a
  `*_ingest` span on each distributor and a `*_block_build` / `*_compaction`
  span on each block-builder/compactor, linked across the WAL so you can see a
  distributor → broker (WAL) → block-builder trace. Gres contributes a
  per-query waterfall — search `{ resource.service.name = "gres" }` — described
  in [Gres query traces](#gres-query-traces).
- **Explore → Crabka Profiles** (Pyroscope): Crabka services — CPU + heap flamegraphs; demo app roles — CPU flamegraphs.
- The **”Crabka observes Crabka”** dashboard (folder *Crabka*) shows one panel
  per signal plus querier heap flamegraphs; the **”Crabka — Orders Demo”**
  dashboard visualises the demo pipeline's business metrics.

## Gres query traces

`crabka-gres` is a PostgreSQL-compatible SQL engine whose write-ahead log is a
topic on the same broker. The demo runs one in single-node substrate mode on
`localhost:5433`; `gres-setup` writes the tenant's registry record first (gres
refuses to start without one), and `gres-workload` drives a small SQL loop
against it (insert, aggregate, point read, periodic delete) so there is always
a fresh trace to open. The **Crabka — Gres Query Traces** dashboard is the
quickest way in; TraceQL `{ resource.service.name = "gres" }` in Explore works
too.

Both services need a demo image built after gres joined it. If `gres-setup`
reports `unrecognized subcommand 'gres'`, or `gres` cannot find `crabka-gres`,
the local image predates the change — [rebuild it from
source](#rebuild-from-source).

One statement produces a waterfall roughly like this:

```text
gres.session                     the pgwire connection
└─ gres.statement                one frontend Query or Execute
   ├─ pg.parse.sql
   └─ SELECT demo_orders         the engine's statement span (see the naming note)
      └─ pg.select
         ├─ pg.timestamp.read    read timestamp acquisition
         ├─ gres.wal_append      durable produce into the broker
         └─ gres.exec_read       the executor body
```

The statement span carries `db.query.summary`, `db.operation.name`,
`db.collection.name`, `db.namespace` and `pg.table_id`; `pg.select` carries
`pg.read_ts` and the MVCC snapshot bounds; `gres.wal_append` carries
`pg.wal.frames` / `bytes` / `first_offset` / `last_offset`. A write adds
`pg.write` and `pg.commit`, and `pg.commit` carries `pg.gate_wait_ms` — time
spent waiting for the group-commit gate, usually the first field to look at
when commits are slow.

`pg.blocking_worker`, `pg.scan`, `pg.read_context` and the contended-row-lock
spans sit at `TRACE` and are off by default; add
`crabka_pgexec::exec=trace` to `CRABKA_OTLP_FILTER` on the `gres` service to
see them.

**Naming note.** The statement spans set `otel.name` to the query summary, so
they export as `SELECT demo_orders`, not as `db.statement`. Select them by
attribute — `{ span.db.system.name = "postgresql" }` — never by span name. The
other span names in the tree are fixed and safe to match: `gres.session`,
`gres.parse`, `pg.select`, `pg.write`, `pg.commit`, `pg.scan`,
`pg.blocking_worker`, `gres.exec_read`, `pg.route`, `pg.timestamp_scatter`,
`pg.prewrite`, `pg.resolve`, `gres.wal_append`, `gres.wal_apply`, `kv.apply`,
`wal.chunk`, `tso.grant`, `range.barrier`. A multi-range cluster adds
`gres.range_rpc` / `gres.range_serve` for the cross-node hop; the demo is
single-node and shows neither (see [Single node only](#single-node-only)).

### Joining your own trace

Append a sqlcommenter comment carrying a W3C `traceparent` and gres makes your
span the parent of the whole query tree:

```bash
PGPASSWORD=demo psql 'host=localhost port=5433 user=demo dbname=demo' -c \
  "SELECT count(*) FROM demo_orders /*traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'*/"
```

Then search that trace id in Explore → Crabka Traces. The comment is `00-`,
a 32-hex trace id, a 16-hex span id, and 2 hex flags; `01` means sampled. It is
ignored by the SQL parser, so the statement runs unchanged, and a malformed one
is dropped silently rather than failing the query. OTel-instrumented Postgres
drivers emit exactly this shape on every statement, which is what makes an
application request and the queries it caused land in one trace.
`SET crabka.traceparent = '…'` does the same thing for a whole session, and is
the only channel that works for the extended protocol, where `Execute` carries
no SQL to comment on.

Gres re-derives the sampling decision from the incoming trace id at its own
ratio, so a client cannot force export by setting the sampled flag on every
statement. Because the ratio is a pure function of the trace id, a client and
gres configured alike still agree and traces stay whole.

### Verbatim SQL is off by default

`CRABKA_OTLP_SQL_TEXT=true` (exposed as `CRABKA_GRES_OTLP_SQL_TEXT` in `.env`)
attaches the statement as sent — literals included — as `db.query.text`. It is
off because it is the one setting here that exports personal data or secrets:
`INSERT INTO users VALUES ('123-45-6789', …)`, `ALTER ROLE app PASSWORD …`.
Anything that reaches the collector reaches everyone who can read the trace
backend. With it off, spans still carry `db.query.summary`
(`"SELECT demo_orders"`), `db.operation.name`, `db.collection.name`,
`db.namespace` and `pg.table_id` — enough to group and attribute latency
without reproducing a single literal.

### Sampling

Every other service here head-samples at 5% (`CRABKA_OTLP_SAMPLE_RATIO` in the
`x-otlp-env` anchor), because the traces pipeline traces its own span ingest and
the resulting feedback loop diverges at 1.0. Gres is a database, not a trace
backend: its spans never re-enter their own ingest path, so it overrides the
anchor and samples at 1.0. Sampling gres at 5% would discard 19 of every 20
query waterfalls.

### Single node only

The demo runs one gres that owns every range. That exercises the whole tree
except the cross-node hop; a multi-range cluster needs mTLS material shared
between the computes (`--range-tls-cert` / `--range-tls-key` / `--range-tls-ca`
/ `--range-tls-server-name`, plus `--ranges`, `--host-ranges` and
`--range-listen`). Nothing in the repo generates that material outside the Rust
test harnesses, so a cluster profile would have to grow its own certificate
step — a failure mode a demo that must start on the first try does not need.

## Dashboards & alerts

Every Crabka service exports Prometheus metrics on its admin port `:9404`
(`/metrics`): the broker via its metrics server, and the four observability
services (metrics/logs/traces/profiles) via the shared profiling-admin server,
across all roles. Alloy scrapes them with a `job` label per compose service, so
the dashboards/alerts select per service and role.

Provisioned dashboards (folder *Crabka*):

- **Crabka — Overview** — fleet liveness, ingest/query rate and error ratio per
  subsystem, broker throughput.
- **Crabka — Orders Demo** — the demo pipeline's business metrics
  (`crabka_demo_*`): orders produced/processed by category × region × payment
  method, order-value distribution, per-stage processing latency, and
  fulfilled / fraud-rejected / anomalous outcomes.
- **Crabka — Runtime Resources** — container CPU, working-set memory, memory
  limit ratio, CPU throttling, and top memory users across Crabka plus Grafana,
  Alloy, cAdvisor, and RustFS.
- **Crabka — Broker** — Kafka throughput, produce/fetch, partitions, ISR &
  controller health, and FedRAMP-MLA audit pipeline.
- **Crabka — Metrics / Logs / Traces / Profiles** — per-subsystem RED: ingest
  rate/bytes/errors/latency (distributor) and query rate/errors/p99 latency by
  route (querier), plus WAL append failures and per-role liveness.
- **Crabka — Gres Query Traces** — the SQL engine's query waterfall: recent
  traces, statement spans, slow and failed statements, executor reads, commits
  and WAL appends, plus a panel explaining how to join your own trace.

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
# gres query traces (TraceQL) — statement spans, matched by attribute not name
curl -s -H 'X-Scope-OrgID: demo' --get 'http://localhost:3200/api/search' \
  --data-urlencode 'q={ resource.service.name = "gres" && span.db.system.name = "postgresql" }' \
  | head -c 200
```

## Layout

- `docker-compose.yml` — the stack
- `../../packaging/melange/crabka-demo.yaml` — builds the all-in-one demo APK package
- `../../packaging/apko/crabka-demo.yaml` — assembles the demo OCI image from that package
- `alloy/config.alloy` — Alloy collects all four signals from both sources and
  scrapes cAdvisor container resource metrics
- `grafana/provisioning/` — datasources, dashboards (overview + broker + one per subsystem + gres query traces), and alert rules
- `rustfs/bootstrap.sh` — creates one bucket per signal (`crabka-metrics`, `crabka-traces`, `crabka-logs`, `crabka-profiles`)
- `gres/workload.sh` — the SQL loop that keeps gres producing query traces
