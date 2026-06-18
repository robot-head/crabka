# Crabka as a Grafana Observability Backend — Logs Wedge (Loki Replacement)

**Status:** Design / approved for planning
**Date:** 2026-06-18
**Scope of this spec:** the **logs** signal only (Loki replacement). Metrics
(Mimir), traces (Tempo), and profiles (Pyroscope) get their own specs; this
document designs the shared substrate they will reuse and explains how they
generalize, but does not specify them.

## 1. Goal & thesis

Make Crabka a unified observability backend that can replace Grafana's
**Mimir + Loki + Tempo + Pyroscope** stack and act as a datasource for all four
signals — starting with logs.

**Thesis.** Those four products look distinct but have converged on one
architecture: OTLP/push ingest → append to a WAL → compact into object-storage
blocks → an index → a querier speaking a signal-specific dialect
(PromQL / LogQL / TraceQL / profile queries). Crabka already owns the
storage/ingest half of that shape — a log-structured append path, KIP-405
object-storage tiering, an Arrow/columnar engine, and an existing OTLP ingest
path (KIP-714 client metrics). The expensive, differentiated part is the four
query dialects and their wire-compatible API surfaces.

The design makes "one system replaces four" an **architecture, not a bundle**:
a single columnar block store + DataFusion query engine, with each signal adding
only a thin query-dialect front-end and a wire-compatible HTTP API. This is the
exact pattern Crabka already uses for Kafka — *be a drop-in, not a fork*.

The observability stack is itself a **Crabka client**: it uses the broker as its
durable write-ahead log over the ordinary Kafka protocol. It eats its own dog
food.

## 2. Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | Ambition | Full LGTM+P replacement, decomposed by signal |
| 2 | First wedge | **Logs / Loki** — cleanest storage mapping, most tractable query language, validates the full pipeline with least query-engine risk |
| 3 | Storage representation | **Unified columnar block store** — WAL topic → compactor → Arrow/Parquet blocks + shared label/series index in object storage. Logs are the first tenant of a format all four signals reuse |
| 4 | Grafana integration | **Loki HTTP API emulation** — Grafana's built-in Loki datasource points at Crabka unmodified; inherits the whole Loki ecosystem (LogCLI, alerting, Promtail/Alloy). Implies we parse + execute LogQL. (A native plugin is a possible later addition; out of scope.) |
| 5 | Query engine | **DataFusion** as the shared execution substrate (the IOx/InfluxDB-3.0 shape: DataFusion + Parquet + `object_store`). LogQL — and later PromQL/TraceQL/profiles — lower onto DataFusion logical plans via a custom `TableProvider` with pushdown |
| 6 | Process model | **Separate role-selectable service** (`-target distributor\|compactor\|querier`), mirroring Loki/Mimir deployment and Crabka's existing separate services (gateway, schema-registry, operator). Uses Crabka-the-broker as its WAL |

## 3. Architecture

```
  Promtail / Alloy / Vector / OTel Collector        Grafana (built-in Loki datasource)
  any Kafka producer                                         │  LogQL over HTTP
        │  Loki push API / OTLP logs / Kafka produce         │
        ▼                                                    ▼
  ┌──────────────┐         ┌──────────────┐         ┌──────────────────────┐
  │ DISTRIBUTOR  │ produce │  WAL TOPIC   │ consume │       QUERIER        │
  │ validate,    ├────────▶│ (Crabka      │────────▶│ Loki HTTP API        │
  │ tenant-route │  Kafka  │  broker)     │  group  │ LogQL→DataFusion plan │
  └──────────────┘         │ short retn.  │         │ hot: WAL tail (live) │
                           └──────┬───────┘         │ cold: blocks (hist.) │
                                  │ consume                │  ▲ pushdown    │
                                  ▼ (group)                │  │             │
                           ┌──────────────┐                │  │ TableProvider│
                           │  COMPACTOR   │  object_store  │  │             │
                           │ rows→columnar├───────────────▶│ blocks + index│
                           │ blocks+index │   S3 / GCS     └──┴─────────────┘
                           └──────────────┘                       ▲
                                  │ blocks + label/series index   │
                                  └────────────────────────────────┘
                                         object storage (shared substrate)
```

### 3.1 Components

| Component | Role | Reuse vs. net-new |
|---|---|---|
| **Distributor** | Terminate Loki-push / OTLP-logs / Kafka-produce; validate; map `X-Scope-OrgID` → tenant; write to WAL | Net-new endpoints; reuses Crabka produce path, quotas, ACLs |
| **WAL topic** | Short-retention durable buffer | 100% reuse — a Kafka topic |
| **Compactor** | Consumer group: roll WAL rows → columnar blocks + index → object storage | Net-new; reuses `object_store` + consumer-group offsets for crash-safety |
| **Block store** | Signal-agnostic columnar block format + index + `TableProvider` w/ pushdown | **Net-new — the shared substrate** |
| **Querier** | Serve Loki HTTP API; LogQL→DataFusion; merge hot (WAL tail) + cold (blocks) | Net-new front-end; reuses DataFusion + block store |

### 3.2 Hot/cold split

Live tailing and very-recent queries read the **WAL topic tail directly** (a
Kafka consumer over uncompacted data); historical queries hit **blocks**. The
querier merges them with a `UNION`, split at the compactor's committed offset /
max-compacted timestamp per partition. This is Loki/Mimir's
ingester-vs-store-gateway split — but the "ingester" is free because it is just
a Kafka consumer on a Crabka topic.

### 3.3 Crate layout

- `crabka-blockstore` — signal-agnostic columnar blocks + two-level index +
  `object_store` IO + DataFusion `TableProvider` with pushdown. The shared half.
- `crabka-logql` — LogQL parser + planner (lowers to DataFusion). The
  signal-specific front-end.
- `crabka-observability` — the role-selectable service binary
  (`-target distributor|compactor|querier`), wiring blockstore + logql + a
  Kafka client.

The split is deliberate: adding metrics later means `crabka-promql` + a
Prometheus-API surface drop in beside `crabka-logql` on the *same*
`crabka-blockstore` and the *same* service skeleton.

## 4. Data model

### 4.1 Block format

A block is **tenant-scoped + time-bounded**, stored as **Parquet** on object
storage (native columnar-on-object-store format; DataFusion reads it with
projection/predicate pushdown and row-group pruning for free). Rows sorted by
`(series_fingerprint, timestamp)` so each series' lines are contiguous —
excellent compression and cheap range scans.

| Column | Meaning |
|---|---|
| `series_fingerprint` | hash of the stream label-set (defines the series) |
| `timestamp` (ns) | log line time |
| `line` | the log body |
| `structured_metadata` | map column — Loki's high-cardinality per-line attrs |

The full label-set lives once in a **series dictionary**
(`fingerprint → {labels}`), not repeated per row. This is the choice that
generalizes: metrics swap `line` for `value:f64`; traces store span columns keyed
by `trace_id`; profiles store sample columns — same block skeleton, different
payload columns.

### 4.2 Index (two-level, TSDB-style; object storage + querier cache)

- **Label index** — inverted: `(label_name, value) → posting list of series
  fingerprints`, plus the series dictionary. Resolves LogQL matchers
  `{app="api", env="prod"}` → a set of fingerprints.
- **Block index** — `(tenant, time-range, fingerprint) → block object key(s)`.

Query planning = matchers → fingerprints (label index) → candidate blocks (block
index) → Parquet scan with pushdown.

*Future:* per-block bloom filters on line tokens to accelerate `|= "needle"`
(Loki's bloom-compactor trick). Out of scope for the wedge.

## 5. Ingest

Three doors, all landing in the same WAL topic:

- **Loki push API** (`POST /loki/api/v1/push`, JSON + snappy-protobuf) — drop-in
  for Promtail / Alloy / Vector / Fluentbit.
- **OTLP logs** (`POST /v1/logs` HTTP + gRPC) — resource/scope attrs → stream
  labels; record attrs → structured metadata; body → `line`.
- **Native Kafka produce** — produce straight to the logs topic; labels carried
  in headers. Free (it's just Kafka) and a genuine differentiator: no other Loki
  ingests via the Kafka protocol natively.

WAL records are partitioned by `(tenant, series_fingerprint)` hash, preserving
per-series order (Kafka per-partition ordering); compactor parallelism = partition
count.

## 6. Multi-tenancy

`X-Scope-OrgID` → tenant id, mapped onto Crabka's existing primitives: tenant →
**topic namespace + ACL principal + quota entity**. Crabka's token-bucket quotas
and ACL machinery provide per-tenant rate limiting and isolation that Loki/Mimir
built from scratch. Block/index object keys are tenant-prefixed.

## 7. Query path (LogQL → DataFusion)

LogQL has two shapes, both lowering onto DataFusion:

- **Log queries** — `{app="api"} |= "error" | json | status >= 500` → lines.
- **Metric queries** — `sum by (status) (rate({app="api"} |= "error" [5m]))` →
  time series (matrix).

### 7.1 Lowering pipeline

```
LogQL string
   │  crabka-logql parser → AST
   ▼
Stream selector matchers ─► label index ─► series fingerprints ─► block index ─► candidate blocks
   │                                                                                   │ (pruning)
   ▼                                                                                   ▼
DataFusion LogicalPlan:
   TableScan(LogBlockTableProvider, pushdown = {block list, time range, fingerprints})
     └─ Filter   line filters:  |="x"→contains(line,"x")   |~"re"→regexp_match(line,"re")
     └─ Project  parsers:        | json / | logfmt → extract fields into columns (scalar UDFs)
     └─ Filter   label filters:  | status >= 500
     └─ Aggregate (metric only)  range agg rate()/count_over_time() → stepped time-window group-by
     └─ Aggregate (metric only)  vector agg sum by (status) → group over labels
   ▼
Execute → serialize to Loki's exact JSON: {resultType: "streams"|"matrix", result:[...]}
```

The **matchers → fingerprints → blocks** chain is the performance story: touch
the cached label index to prune to a handful of blocks *before* any Parquet scan,
then push the time range + line filters into the scan so row-group statistics
skip most data.

### 7.2 Hot/cold merge

The planner runs two table providers and `UNION`s them: `LogBlockTableProvider`
(cold, blocks) up to the compaction frontier, and `WalTailTableProvider` (hot, a
Kafka consumer materializing the uncompacted WAL tail into Arrow) after it. The
frontier = the compactor's committed offset per partition — no double-count, no
gap.

### 7.3 Endpoints

- `query_range` / `query` → the lowering above (streams or matrix).
- `labels` / `label/{name}/values` / `series` → served directly from the label
  index, no block scan.
- `tail` (websocket) → pure `WalTailTableProvider` stream — live, never touches
  blocks.

### 7.4 Result-shape fidelity

The "byte-equality" analog for this layer. Grafana's Loki datasource expects
exact JSON shapes (`status`, `data.resultType`, `data.result[].stream`/`.values`
vs `.metric`/`.values`). Matching that contract precisely is what makes the
built-in datasource "just work."

### 7.5 LogQL subset for the wedge

**In:** all matcher ops (`=` `!=` `=~` `!~`); line filters (`|=` `!=` `|~`
`!~`); `json` + `logfmt` parsers; comparison label filters;
`rate` / `count_over_time` / `bytes_over_time`; `sum` / `count` / `min` / `max`
/ `avg` with `by`/`without`.
**Out (later):** `pattern` / `regexp` / `unwrap`, quantile ranges, `ip()`, binary
ops between sub-queries, `label_replace`.

## 8. Error handling

- **Distributor** — malformed labels/timestamps → Loki-contract `4xx`; quota
  exceeded → `429`; WAL backpressure → `503`. At-least-once into the WAL
  (`acks=all`). Partial-push failures reported per Loki's response contract.
- **Compactor** — crash-safety via consumer-group offsets (resume from last
  commit). **Idempotent block writes**: block object key is deterministic from
  `(tenant, partition, offset-range, time-window)`, so reprocessing after a crash
  overwrites identical bytes. Strict ordering: **write block + index to object
  storage, then commit offsets** — a crash between only re-does work, never loses
  it. Object-store errors → retry with backoff.
- **Querier** — block-read failure → Loki-style partial result + warning;
  per-tenant limits (max series / query length / bytes scanned) → `4xx`. The
  hot/cold frontier guarantees correctness under compactor lag: if the compactor
  falls behind, the WAL tail simply covers more — never a gap or a double-count.

## 9. Testing strategy

Mirrors Crabka's differential-testing ethos:

- **Differential conformance vs. real Loki** *(headline)* — ingest identical data
  into Loki and Crabka, run a LogQL query corpus against both, assert equal
  results. The byte-equality analog that proves "drop-in."
- **Grafana integration** (testcontainers) — real Grafana, built-in Loki
  datasource pointed at Crabka, drive Explore queries, assert they render.
- **Compactor crash-recovery** — kill mid-block, restart, assert no loss / no dup
  / identical block keys.
- **Hot/cold merge** — query spanning the compaction frontier returns each line
  exactly once.
- **Multi-tenant isolation** — tenant A cannot see B's series/labels/lines; quota
  enforced.
- LogQL parser/planner snapshots; DataFusion golden-plan + pushdown assertions.
- *(Stretch, fits Crabka's stateright program:* model the compactor's
  offset-commit-vs-block-durability ordering for no-loss/no-dup.)

## 10. Scope

**In (logs wedge MVP):**
- 3 ingest doors (Loki push, OTLP logs, Kafka produce)
- WAL topic + compactor + Parquet blocks + two-level index in object storage
- DataFusion querier serving the Loki HTTP API (`query_range`, `query`, `labels`,
  `label/{name}/values`, `series`, `tail`)
- The LogQL subset in §7.5
- Multi-tenancy via `X-Scope-OrgID` → Crabka tenant/quota/ACL
- Role-selectable binary (distributor / compactor / querier)
- Grafana built-in Loki datasource works end-to-end
- Differential-vs-Loki + Grafana integration tests

**Out (explicitly deferred):**
- Bloom-filter line acceleration
- Full LogQL surface (§7.5 "out" list)
- Loki ruler / recording rules / alerting
- Native Grafana plugin & cross-signal correlation
- Advanced retention / lifecycle policies (basic compaction + object lifecycle
  only)
- Ring / sharding autoscaling (the wedge leans on Kafka partitioning for
  parallelism)
- The other three signals (own specs)

## 11. Generalization to all four signals

Why full replacement is an architecture and not a bundle. Everything *below* the
front-end — block store, index, compactor, querier skeleton, service binary,
multi-tenancy, object-store IO — is built once in the logs wedge and reused.

| Signal | Front-end crate | API emulated | Block payload | Index key | Existing Crabka reuse |
|---|---|---|---|---|---|
| **Logs** (wedge) | `crabka-logql` | Loki HTTP | `line`, metadata | series fingerprint | produce, quotas, `object_store` |
| **Metrics** | `crabka-promql` | Prometheus HTTP | `value:f64` | series fingerprint | **KIP-714 OTLP ingest + `prometheus_sink` already exist** |
| **Traces** | `crabka-traceql` | Tempo HTTP | span fields | `trace_id` + span index | OTLP ingest path |
| **Profiles** | `crabka-pprof` | Pyroscope HTTP | sample/stack | profile-type + symbol index | OTLP/pprof |

Each later signal is **one front-end crate + one API-surface module + one block
schema**. LogQL's metric-query aggregation is a literal down-payment on PromQL.

## 12. Open questions for planning

- **Series fingerprint algorithm** — match Loki's labels hash, or own scheme?
  (Only matters if we want index-file interop with Loki tooling; the API-compat
  path does not require it.)
- **Block sizing / compaction cadence** — target block size, time-window
  granularity, and how aggressively to compact small blocks (the
  small-block/L0→L1 compaction question).
- **Querier parallelism for the wedge** — single querier reading partitions
  concurrently vs. an early sharded read path. MVP leans on Kafka partition
  fan-out.
- **OTLP logs vs. Loki-push label mapping** — exact rules for which OTLP
  attributes become indexed stream labels vs. structured metadata (cardinality
  policy).
