# Crabka full-signal observability demo

One `docker compose up` command starts Grafana over Crabka's four observability
backends: metrics, traces, logs, and profiles. Crabka exports all four of its own
signals into those backends. An instrumented `crabka-client-streams` orders
pipeline runs its Kafka traffic on Crabka.

`crabka-gres` runs with them and exports a per-query trace waterfall. It is the
PostgreSQL-compatible SQL engine whose write-ahead log is in the same broker.
See [Gres query traces](#gres-query-traces).

One `crabka-broker` does three jobs. It is the event bus of the demo app, the
write-ahead log for all four telemetry backends, and a self-observed subject.
One Grafana Alloy collects every signal from both sources, which are Crabka's own
processes and the demo app. Alloy writes to the backends. The backends persist
their data through the broker as a WAL and through a shared RustFS bucket set as
blocks.

## Run

By default, this **pulls the prebuilt image** from GHCR. You do not need a local
build:

```bash
cd demo/observability
docker compose up -d
```

Then open Grafana at <http://localhost:3000>. It uses anonymous admin access.

Data appears a few minutes after startup, when Alloy collects signals and the
block-builders flush their first blocks. Allow about 3 to 5 minutes on a cold
start. The queriers refresh their indexes automatically.

Tune the load with `CRABKA_DEMO_ORDERS_PER_SEC` on the `demo-produce` service.
The default is `50`, and `0` pauses production. Tune the SQL load with
`CRABKA_GRES_WORKLOAD_INTERVAL` on `gres-workload`, which defaults to `5`
seconds between passes. Lower both values on a constrained host. Plan for
**≥ 8 GB** of Docker memory, because the demo runs about 23 containers.

Gres listens on `localhost:5433` as the `demo` tenant. It takes the SQL password
from `CRABKA_GRES_PASSWORD`, which defaults to `demo`:

```bash
PGPASSWORD=demo psql 'host=localhost port=5433 user=demo dbname=demo' \
  -c 'SELECT count(*) FROM demo_orders'
```

The service binaries run under the jemalloc allocator. Heap profiling is active
at a coarse sample rate, `lg_prof_sample:25`, which is about one sample per
32 MiB. The dirty and muzzy decay periods are short, so freed profiler and query
pages return to the OS quickly. Heap flamegraphs therefore represent long-lived
allocations, and the profiler's own backtrace bookkeeping does not dominate
querier RSS.

CPU pprof collection stays enabled, but it uses 5-second windows per 60-second
scrape to limit profiler overhead. Alloy's eBPF profiler collects CPU data only
for native code. The Rust memory profiles in this demo therefore come from the
`/debug/pprof/heap` endpoint of the services.

If a reused RustFS volume grows far beyond the logical bucket sizes in the
RustFS dashboard, reset the fixture. Run `docker compose down -v`, then start the
demo again. Older demo revisions rewrote large logs, traces, and profiles index
objects on the same S3 keys. RustFS can keep those old physical parts on disk
after the latest S3 object becomes small or deleted. Current images use
append-style log shards and immutable trace and profile index snapshots, which
prevent new overwrite churn.

When local Docker disk use is high, check the named volumes separately from
the live S3-visible object sizes:

```bash
docker system df -v
docker volume ls --filter name=crabka-observability-demo
```

`crabka-observability-demo_rustfs-data` and
`crabka-observability-demo_broker-data` usually hold most of the demo's
persistent data. If a host ran older demo images, RustFS can still contain many
UUID-named backend directories below legacy keys. Examples are
`crabka-traces/index/traces.json`, `crabka-profiles/index/profiles.json`, and
`crabka-logs/logs/tenant=demo/index/logs/manifest.json`. Those directories are
old overwrite generations, not more logical S3 objects. To reclaim them
reliably, create the compose volumes again.

The old MinIO fixture used `crabka-observability-demo_minio-data`. This compose
file does not use that volume after the change to RustFS, so you can delete it
if no older checkout needs it.

The traces block-builder has its own replay cap in
`CRABKA_TRACES_BLOCK_BUILDER_MEM`, which defaults to `4g`. It also has a lower
flush size in `CRABKA_TRACES_BLOCK_BUILDER_FLUSH_MAX_RECORDS`, which defaults to
`5000`. A cold start can replay a burst of broker self-traces before steady
state. These settings prevent a replay-time OOM and keep the normal RSS small.

### Rebuild from source

To build the image locally and not pull it, for example to test local changes,
build and tag it under the same name from the **repo root** with melange and
apko. Then start the demo as usual. Compose uses the local image when it is
present:

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
Actions workflow. The path is Actions → *publish-demo-image* → *Run workflow* →
image tag.

## What you should see

- **Explore → Crabka Metrics** (Prometheus). `{job=”broker”}` gives the broker's
  own metrics. `{__name__=~”crabka_demo_.*”}` gives the demo app's business
  metrics: orders by category × region × payment method, order value, per-stage
  processing latency, and outcomes.
- **Explore → Crabka Logs** (Loki). `{service_name=”broker”}` and
  `{service_name=~”demo-.*”}` give JSON logs.
- **Explore → Crabka Traces** (Tempo). TraceQL `{}` gives broker and demo-app
  spans. **Cross-service distributed traces:** search `{ name = “produce_order” }`,
  or open any `demo-produce` trace. Each traced order is one trace across
  **demo-produce → demo-consume**. A W3C `traceparent` Kafka record header
  carries it through the broker. The consumer side shows the `process_order` span
  with its `validate → enrich → fraud_check → fulfill` child stages. The
  observability services also instrument their own ingest and compaction. Each
  distributor has a `*_ingest` span, and each block-builder or compactor has a
  `*_block_build` or `*_compaction` span. These spans link across the WAL, so you
  can see a distributor → broker (WAL) → block-builder trace. Gres contributes a
  per-query waterfall. Search `{ resource.service.name = "gres" }`.
  [Gres query traces](#gres-query-traces) describes it.
- **Explore → Crabka Profiles** (Pyroscope). The Crabka services give CPU and heap flamegraphs. The demo app roles give CPU flamegraphs.
- The **"Crabka observes Crabka"** dashboard in folder *Crabka* shows one panel
  per signal and querier heap flamegraphs. The **"Crabka — Orders Demo"**
  dashboard shows the demo pipeline's business metrics.

## Gres query traces

`crabka-gres` is a PostgreSQL-compatible SQL engine whose write-ahead log is a
topic on the same broker. The demo runs one instance in single-node substrate
mode on `localhost:5433`. `gres-setup` writes the tenant's registry record first,
because gres refuses to start without one. `gres-workload` then drives a small
SQL loop against gres with an insert, an aggregate, a point read, and a periodic
delete, so there is always a fresh trace to open. The **Crabka — Gres Query
Traces** dashboard is the quickest way to start. TraceQL
`{ resource.service.name = "gres" }` in Explore also works.

Both services need a demo image that was built after gres joined it. If
`gres-setup` reports `unrecognized subcommand 'gres'`, or if `gres` cannot find
`crabka-gres`, the local image is older than that change. [Rebuild it from
source](#rebuild-from-source).

One statement produces a waterfall similar to this:

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
`db.collection.name`, `db.namespace` and `pg.table_id`. `pg.select` carries
`pg.read_ts` and the MVCC snapshot bounds. `gres.wal_append` carries
`pg.wal.frames` / `bytes` / `first_offset` / `last_offset`. A write adds
`pg.write` and `pg.commit`. `pg.commit` carries `pg.gate_wait_ms`, the time that
the commit waited for the group-commit gate. Examine this field first when
commits are slow.

`pg.blocking_worker`, `pg.scan`, `pg.read_context` and the contended-row-lock
spans are at the `TRACE` level and are off by default. To see them, add
`crabka_pgexec::exec=trace` to `CRABKA_OTLP_FILTER` on the `gres` service.

**Naming note.** The statement spans set `otel.name` to the query summary, so
they export as `SELECT demo_orders` and not as `db.statement`. Select them by
attribute with `{ span.db.system.name = "postgresql" }`. Never select them by
span name. The other span names in the tree are fixed and safe to match:
`gres.session`, `gres.parse`, `pg.select`, `pg.write`, `pg.commit`, `pg.scan`,
`pg.blocking_worker`, `gres.exec_read`, `pg.route`, `pg.timestamp_scatter`,
`pg.prewrite`, `pg.resolve`, `gres.wal_append`, `gres.wal_apply`, `kv.apply`,
`wal.chunk`, `tso.grant`, `range.barrier`. A multi-range cluster adds
`gres.range_rpc` / `gres.range_serve` for the cross-node hop. The demo is
single-node and shows neither. See [Single node only](#single-node-only).

### Joining your own trace

Append a sqlcommenter comment with a W3C `traceparent`. Gres then makes your
span the parent of the whole query tree:

```bash
PGPASSWORD=demo psql 'host=localhost port=5433 user=demo dbname=demo' -c \
  "SELECT count(*) FROM demo_orders /*traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'*/"
```

Then search for that trace id in Explore → Crabka Traces. The comment is `00-`,
a 32-hex trace id, a 16-hex span id, and 2 hex flags. `01` means sampled. The SQL
parser ignores the comment, so the statement runs unchanged. Gres drops a
malformed comment silently and does not fail the query.

OTel-instrumented Postgres drivers emit exactly this shape on every statement,
so an application request and the queries that it causes go into one trace.
`SET crabka.traceparent = '…'` does the same for a whole session. It is the only
channel that works for the extended protocol, where `Execute` carries no SQL to
comment on.

Gres derives the sampling decision again from the incoming trace id at its own
ratio, so a client cannot force export when it sets the sampled flag on every
statement. The ratio is a pure function of the trace id, so a client and a gres
with the same configuration still agree, and traces stay whole.

### Verbatim SQL is off by default

`CRABKA_OTLP_SQL_TEXT=true` attaches the statement as sent, with the literals,
as `db.query.text`. The `.env` file exposes this setting as
`CRABKA_GRES_OTLP_SQL_TEXT`. It is off because it is the one setting here that
exports personal data or secrets, for example
`INSERT INTO users VALUES ('123-45-6789', …)` and `ALTER ROLE app PASSWORD …`.
Anything that reaches the collector reaches everyone who can read the trace
backend. With the setting off, spans still carry `db.query.summary`
(`"SELECT demo_orders"`), `db.operation.name`, `db.collection.name`,
`db.namespace` and `pg.table_id`. These fields are enough to group and attribute
latency without a single literal.

### Sampling

Every other service here head-samples at 5% with `CRABKA_OTLP_SAMPLE_RATIO` in
the `x-otlp-env` anchor. The traces pipeline traces its own span ingest, and the
feedback loop diverges at 1.0. Gres is a database, not a trace backend, and its
spans never re-enter their own ingest path. Gres therefore overrides the anchor
and samples at 1.0. A 5% sample of gres would discard 19 of every 20 query
waterfalls.

### Single node only

The demo runs one gres that owns every range. This exercises the whole tree
except the cross-node hop. A multi-range cluster needs mTLS material shared
between the computes: `--range-tls-cert` / `--range-tls-key` / `--range-tls-ca`
/ `--range-tls-server-name`, plus `--ranges`, `--host-ranges` and
`--range-listen`. Nothing in the repository generates that material outside the
Rust test harnesses. A cluster profile would therefore need its own certificate
step. That step is one more failure mode, and this demo must start on the first
try.

## Dashboards & alerts

Every Crabka service exports Prometheus metrics on its admin port `:9404` at
`/metrics`. The broker exports them through its metrics server. The four
observability services for metrics, logs, traces, and profiles export them
through the shared profiling-admin server, in all roles. Alloy scrapes them with
a `job` label per compose service, so the dashboards and alerts select per
service and per role.

Provisioned dashboards in folder *Crabka*:

- **Crabka — Overview**: fleet liveness, ingest and query rate, error ratio per
  subsystem, and broker throughput.
- **Crabka — Orders Demo**: the demo pipeline's business metrics
  (`crabka_demo_*`). Orders produced and processed by category × region ×
  payment method, order-value distribution, per-stage processing latency, and
  fulfilled / fraud-rejected / anomalous outcomes.
- **Crabka — Runtime Resources**: container CPU, working-set memory, memory
  limit ratio, CPU throttling, and top memory users across Crabka plus Grafana,
  Alloy, cAdvisor, and RustFS.
- **Crabka — Broker**: Kafka throughput, produce and fetch, partitions, ISR and
  controller health, and the FedRAMP-MLA audit pipeline.
- **Crabka — Metrics / Logs / Traces / Profiles**: per-subsystem RED. The
  distributor gives ingest rate, bytes, errors, and latency. The querier gives
  query rate, errors, and p99 latency by route. The dashboards also show WAL
  append failures and per-role liveness.
- **Crabka — Gres Query Traces**: the SQL engine's query waterfall. It shows
  recent traces, statement spans, slow and failed statements, executor reads,
  commits and WAL appends, plus a panel that explains how to join your own trace.

Provisioned Grafana-managed alerts are in folder *Crabka Alerts* and
`grafana/provisioning/alerting/`. The broker alerts are no active controller,
offline partitions, under-min-ISR, under-replicated, and audit write failures.
The observability alerts are service down, per-subsystem ingest error
ratio > 5%, and per-subsystem query p99 > 5s. To see one alert fire, stop a service
with `docker compose stop traces-querier`. **Observability service down** then
moves to Firing within about 1 minute. `docker compose start traces-querier`
resolves it.

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

- `docker-compose.yml`: the stack
- `../../packaging/melange/crabka-demo.yaml`: builds the all-in-one demo APK package
- `../../packaging/apko/crabka-demo.yaml`: assembles the demo OCI image from that package
- `alloy/config.alloy`: Alloy collects all four signals from both sources and
  scrapes cAdvisor container resource metrics
- `grafana/provisioning/`: datasources, the dashboards for the overview, the broker, one per subsystem, and the gres query traces, and the alert rules
- `rustfs/bootstrap.sh`: creates one bucket per signal (`crabka-metrics`, `crabka-traces`, `crabka-logs`, `crabka-profiles`)
- `gres/workload.sh`: the SQL loop that makes gres produce query traces
