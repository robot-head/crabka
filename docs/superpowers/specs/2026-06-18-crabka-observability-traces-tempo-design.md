# Crabka as a Grafana Observability Backend — Traces Signal (Grafana-Tempo Replacement)

**Status:** Implemented (all 8 slices) — see §14 for as-built notes
**Date:** 2026-06-18
**Scope of this spec:** the **traces** signal — a *full* Grafana-Tempo-equivalent
distributed-tracing backend (not an MVP). Covers OTLP/Jaeger/Zipkin ingest, the
Kafka-native ingest-storage pipeline (distributor → WAL → block-builder +
live-store + metrics-generator), span blocks on object storage, the complete
TraceQL language (selectors, structural operators, pipelines, and TraceQL
metrics), the metrics-generator (span-metrics RED + service-graphs →
remote_write), the full Tempo HTTP API that Grafana's built-in Tempo datasource
speaks, and multi-tenancy.

This is the third signal in the LGTM+P replacement. It reuses the shared
substrate designed for logs (`crabka-blockstore`) and the role-selectable service
skeleton, and follows the same "emulate the wire/HTTP contract, don't fork the
product" pattern. See the sibling specs:
[2026-06-18-crabka-observability-logs-design.md](2026-06-18-crabka-observability-logs-design.md)
and
[2026-06-18-crabka-observability-metrics-mimir-design.md](2026-06-18-crabka-observability-metrics-mimir-design.md).

## 1. Goal & thesis

Replace Grafana Tempo and serve as Grafana's traces datasource, by emulating
Tempo's *external* surfaces (the Tempo HTTP API, OTLP/Jaeger/Zipkin push) on
Crabka's substrate — the Kafka log as the durable ingest WAL, `crabka-blockstore`
for columnar Parquet span blocks on object storage, and DataFusion for query. We
reproduce Tempo's *contracts* (TraceQL semantics, API shapes, metric names), not
its block byte-format or internal components.

**The load-bearing realization.** Tempo 3.0's GA architecture is *already
Kafka-native* — its "ingest storage" mode is the default. The **distributor**
shards spans by `trace_id` into Kafka partitions; the **block-builder**,
**live-store**, and **metrics-generator** are three independent Kafka consumer
groups (each owns its own offsets), each running at replication-factor 1 because
Kafka itself provides durability. The standalone *ingester* component is gone.
Crabka provides the *exact* substrate Tempo 3.0 assumes — the WAL topic *is*
Tempo's Kafka ingest topic, partitioning by `trace_id` *is* Tempo's partitioning,
and RF1 is safe because the Crabka broker replicates the partition. This is a
near-1:1 mapping — **the best fit of the four signals.**

**The invariant to preserve.** `hash(trace_id) → partition` routes *all* spans of
a trace to one partition. That is what lets the three consumer groups run at RF1
with no cross-consumer deduplication: each group sees every span of a trace
exactly once, in one partition, with independent offsets. Preserving this hashing
is non-negotiable — it is the dedup-avoidance invariant the whole pipeline rests
on.

## 2. Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | Ambition | **Full** Tempo replacement (not an MVP) |
| 2 | TraceQL engine | **Our own** parser + planner. No published Rust TraceQL parser exists on crates.io; adapt `icegatetech/icegate`'s ANTLR `TraceQLLexer.g4`/`TraceQLParser.g4` (Apache-2.0) as the grammar *reference*. Lower onto DataFusion (shared substrate) |
| 3 | Storage | `crabka-blockstore` (shared with logs/metrics), **generalized** behind a `BlockIndex` trait (Decision A). Logs/metrics keep their `SeriesIndex`; traces add a `TraceIndex`; blockstore stops assuming mandatory `series_fingerprint`+`timestamp` columns — each signal declares its own schema + index |
| 4 | Span block format | **Crabka choice: a flattened span-per-row Parquet** (denormalized trace+resource columns), sorted/grouped by `trace_id`. *Not* vParquet byte-format compatible — greenfield. We need TraceQL-semantic/API compat, not block-format compat. Real Tempo vParquet4 is one-row-per-trace nested; we flatten |
| 5 | Structural operators | The **nested-set model** (`nested_set_left`/`right`/`parent_id`, Int32, DFS-preorder, computed at block-build), lowered to a **partitioned self-join** keyed by `trace_id` with nested-set range/equality predicates — *not* a per-trace tree-walk operator. Joins are siblings-aware: a span is never its own sibling, so the **sibling** lowering carries a distinct-span predicate (`B.span_id != A.span_id`) on top of equal `parent_id`, matching Tempo's "different span sharing the same parent." This is the centerpiece |
| 6 | Dedicated attribute columns | Copy vParquet5's **fully-dynamic attribute promotion** (configured resource/span attrs hoisted into their own dict-encoded columns at block-build = the pushdown fast path). Drop vParquet4's hardcoded HTTP columns |
| 7 | metrics-generator | **In scope** (Decision A). span-metrics (RED) + service-graphs, emitted via the **remote_write client reused from the metrics signal** into Crabka's metrics backend |
| 8 | Grafana integration | **Tempo HTTP API emulation** → Grafana's built-in Tempo datasource, unmodified. Service Graph is *not* a Tempo endpoint — it is `traces_service_graph_*` series in the metrics backend, queried by Grafana directly |
| 9 | Process model | Role-selectable service (`distributor`/`block-builder`/`live-store`/`querier`/`query-frontend`/`compactor`/`metrics-generator`); uses the Crabka broker as its ingest WAL |

## 3. Architecture

### 3.1 Tempo 3.0 component → Crabka realization

| Tempo component | Crabka realization |
|---|---|
| **Distributor** (OTLP/Jaeger/Zipkin ingest, hash `trace_id` → partition) | `distributor` role — terminates the push protocols, hashes `trace_id` → WAL partition, ACKs after the Kafka write |
| **Kafka ingest topic** | **The Crabka broker** — the WAL topic *is* Tempo's ingest topic; partition replication *is* Tempo's RF; partitioning by `trace_id` preserves the dedup-avoidance invariant |
| **Block-builder** (consumer group → Parquet → object store → commit) | `block-builder` role — consumes the WAL, groups spans by `trace_id` over a window, writes span Parquet blocks + `TraceIndex`, commits offsets (write-then-commit, idempotent keys) |
| **Live-store** (consumer group serving recent traces) | `live-store` role — the **hot tier**, an in-memory recent-traces store (assembled by `trace_id`, ~30–60 min), exposed as a DataFusion `MemTable`; rebuildable purely from offsets |
| **Querier** | `querier` role — DataFusion over `crabka-blockstore` (cold) **UNION** live-store (hot) |
| **Query-frontend** | `query-frontend` role — shard/queue search across time + block + row-group jobs |
| **Compactor** | `compactor` role — merges/recompacts span blocks (and the late-span merge) |
| **Metrics-generator** (span-metrics + service-graphs → remote_write) | `metrics-generator` role — third consumer group; emits RED + service-graph series via the metrics signal's remote_write client into Crabka's metrics backend |
| **Overrides / limits** | per-tenant config on Crabka's quota/ACL machinery |

### 3.2 The Kafka-native pipeline

```
  OTel Collector / Jaeger agent / Zipkin reporter         Grafana (built-in Tempo datasource)
  any OTLP/Jaeger/Zipkin source                                  │  TraceQL over HTTP
        │  /v1/traces · /api/v2/spans · Jaeger gRPC/Thrift        │
        ▼                                                         ▼
  ┌──────────────┐  hash(trace_id)  ┌──────────────┐      ┌──────────────────────┐
  │ DISTRIBUTOR  │ ───── produce ──▶│  WAL TOPIC   │      │       QUERIER        │
  │ decode+route │   (one trace →   │ (Crabka      │      │ Tempo HTTP API       │
  │ ACK on Kafka │    one partition)│  broker, RF1)│      │ TraceQL→DataFusion   │
  └──────────────┘                  └──────┬───────┘      │ hot: live-store      │
                                           │ consume      │ cold: span blocks    │
                  ┌────────────────────────┼──────────────┴───────────┐
                  │ (group)                │ (group)                   │ (group)
                  ▼                        ▼                           ▼
          ┌──────────────┐        ┌──────────────┐           ┌──────────────────┐
          │ BLOCK-BUILDER│        │  LIVE-STORE  │           │ METRICS-GENERATOR│
          │ group by     │        │ recent traces│           │ span-metrics(RED)│
          │ trace_id →   │ object │ (hot tier,   │           │ + service-graphs │
          │ span Parquet │ _store │  MemTable)   │           │ → remote_write   │
          │ + TraceIndex │───────▶│ rebuildable  │           │ → metrics backend│
          └──────────────┘ S3/GCS └──────────────┘           └──────────────────┘
                  │ blocks + TraceIndex (bloom + tag sets)
                  └──────────────────────────────────────────▶ object storage (shared substrate)
```

Three consumer groups, **independent offsets**, all reading the same
`trace_id`-partitioned WAL. Because every span of a trace lands in one partition,
no group needs cross-partition dedup.

### 3.3 Crate layout

- `crabka-blockstore` *(shared with logs/metrics; generalized in slice 1)* — the
  concrete index is extracted behind a **`BlockIndex` trait**. Logs/metrics keep a
  `SeriesIndex` (impl `BlockIndex`); traces add a `TraceIndex` (impl `BlockIndex`).
  `BlockStore` is parameterized/dyn over `BlockIndex`. The mandatory
  `series_fingerprint`+`timestamp` columns become signal-declared; existing
  `BlockStore`/`BlockWriter`/`BlockMeta`/`scan_context` and `Labels`/`LabelMatcher`/
  `MatchOp` stay available.
- `crabka-traceql` *(slices 2–3)* — the TraceQL engine: our own parser (grammar
  referenced from icegate's `.g4`), the planner, the **nested-set structural
  self-join** lowering (`SpanStructuralJoin`), the pipeline-aggregation lowering,
  and TraceQL metrics → time-bucketed Prometheus-shaped series.
- `crabka-traces` *(slices 4–8)* — the role-selectable service binary wiring
  blockstore + traceql + a Kafka client, plus the wire surfaces (OTLP/Jaeger/Zipkin
  ingest, the Tempo HTTP API, metrics-generator → remote_write).

### 3.4 Reuse vs net-new

**Reuse (from the codebase):** OTLP trace decode via `opentelemetry-proto` 0.32
trace types (the same dependency the metrics signal uses —
[client_metrics/otlp.rs](crates/broker/src/client_metrics/otlp.rs) is the decode
precedent); the axum 0.8 TLS-aware serve patterns from
[grpc-gateway/serve.rs](crates/grpc-gateway/src/serve.rs) and
[metrics_server.rs](crates/broker/src/metrics_server.rs); the **remote_write
client + native-histogram/exemplar codec from the metrics signal** (the
metrics-generator's only output path); `object_store` 0.13; Crabka's token-bucket
quotas + ACLs for per-tenant limits; consumer-group offsets for crash-safety.

**Net-new:** the TraceQL parser + planner, the nested-set structural self-join,
the flattened span block schema, the `TraceIndex` (sharded `trace_id` bloom + tag
sets), the index-less by-id retrieval path, the live-store hot tier, the
distributor's three push protocols, and the metrics-generator's span-metrics +
service-graphs processors.

## 4. Data model

Span blocks are **tenant-scoped + time-bounded** Parquet on object storage,
**one row per span**, sorted/grouped by `trace_id` so each trace's spans are
contiguous. This is a *deliberate flattening* of Tempo's vParquet4 (which is one
*row per trace*, nested `Trace → ResourceSpans → ScopeSpans → Spans`): we
denormalize trace- and resource-level fields onto every span row. We are
compatible with TraceQL semantics and the Tempo API, **not** with the vParquet
byte format (greenfield — no block-format interop is required).

### 4.1 Span block columns

**Identity (raw bytes):**
- `trace_id: FixedSizeBinary[16]`, `span_id: FixedSizeBinary[8]`,
  `parent_span_id: FixedSizeBinary[8]` (the raw *semantic* parent reference).

**Structural / nested-set (the load-bearing columns):**
- `nested_set_left: Int32`, `nested_set_right: Int32`, `parent_id: Int32`.
  Computed at block-build via a **DFS pre-order over each trace's span tree**
  (modified pre-order traversal): an ancestor's `[left, right]` interval strictly
  contains every descendant's; `parent_id(child) == parent.nested_set_left`. These
  integer columns are what make structural TraceQL ops cheap columnar predicates
  instead of tree walks. TraceQL exposes them as the `nestedSetLeft`/`Right`/
  `Parent` intrinsics.

**Trace-denormalized (one value per trace, copied to every span row):**
- `root_service_name: Utf8`, `root_span_name: Utf8`,
  `trace_start_unix_nano: Int64`, `trace_duration_nanos: Int64`.

**Span intrinsics:**
- `name: Utf8`, `kind: Int` (enum `unspecified|internal|server|client|producer|
  consumer`), `start_unix_nano: Int64`, `duration_nanos: Int64`,
  `status_code: Int` (enum `unset|ok|error`), `status_message: Utf8`.

**Dedicated attribute columns (the pushdown fast path):**
- A configurable set of resource/span attributes hoisted at block-build into their
  own **dict-encoded columns**. We copy vParquet5's *fully-dynamic* promotion model
  (configured attrs → physical column at write time, queries transparent), **not**
  vParquet4's hardcoded `HttpMethod`/`HttpUrl`/`HttpStatusCode` columns. Promotion
  is configuration, not code.

**Generic attributes (typed LIST columns, array-aware):**
- Each attribute is `Attribute { Key, IsArray: Bool, Value: List<Utf8>,
  ValueInt: List<Int64>, ValueDouble: List<Float64>, ValueBool: List<Bool> }`
  (per Tempo's wire model). A **scalar value is a single-element list.** This makes
  array attributes first-class; TraceQL array semantics (§6) match `=`/`=~` if
  *any* element matches and `!=`/`!~` if *no* element matches.

**Events & links (nested):**
- `events: List<Struct<...>>` and `links: List<Struct<...>>` — first-class and
  queryable. Events carry `Event.time_since_start_nano` (relative to span start);
  links carry the linked `traceID`/`spanID`. TraceQL reaches them via the `event:`/
  `link:` scopes.

### 4.2 The `TraceIndex` (per block, on object storage + querier cache)

`TraceIndex` is a `BlockIndex` impl. There is **no global `trace_id → block` map.**
Instead each block carries:

- **(a) Sharded `trace_id` bloom filters** for **index-less by-id retrieval.**
  Shard = `FNV-1 32-bit hash(trace_id) mod shard_count` (hash, *not* raw bytes;
  matches Tempo's sharding). Default bloom FP rate `0.01`, shard size `100 KiB`.
  By-id lookup = time/block prefilter → per-block bloom test → Parquet row-group
  min/max binary search over the `trace_id` column page statistics (first 16 bytes
  in page stats) + a string-in predicate. The per-block `trace_id → row-group`
  index file is **off by default** (matches Tempo: bloom + row-group min/max is the
  default locate path).
- **(b) Per-block tag-name / tag-value sets + blooms** — for TraceQL search
  pruning and tag discovery (the `/api/v2/search/tags` and `tag/{tag}/values`
  endpoints). Lets the planner skip blocks that cannot contain a referenced
  tag/value before any Parquet scan.

### 4.3 Trace-level metadata

Carried denormalized on span rows (root service/name, trace start/duration) plus,
where the API needs it, a per-trace `ServiceStats` rollup
(`service → {SpanCount, ErrorCount}`) computed at block-build. `TraceID` is stored
both as raw `FixedSizeBinary[16]` and surfaced as hex (`TraceIDText`) at the API
edge — `trace_id` hex is the universal cross-signal join key.

## 5. Ingest

The distributor terminates four push doors, all landing in the traces WAL topic.

### 5.1 Push doors

- **OTLP traces** — `POST /v1/traces` (HTTP-protobuf) **and** OTLP gRPC
  (`4317`/`4318`). The primary door; decode via `opentelemetry-proto` 0.32 trace
  types.
- **Jaeger** — gRPC (`14250`) + Thrift (`thrift_compact` `6831`, `thrift_http`
  `14268`).
- **Zipkin** — `POST /api/v2/spans` (`9411`).
- **Tempo-native** — `POST /api/push`.

(Receiver ports match Tempo's defaults so existing Collectors/agents reconfigure
trivially.)

### 5.2 WAL record & partitioning

`SpanRecord` = tenant + one OTLP-derived span (`trace_id[16]`, `span_id[8]`,
`parent_span_id[8]`, `name`, `kind`, `start_ns`, `duration_ns`, `status`, resource
attrs, span attrs, events, links). Encoded via serde + `serde-wincode` (workspace
convention). The WAL topic is `__crabka_traces_wal` (or per-tenant); **partition
key = `hash(trace_id)`**, sending all spans of a trace to one partition — the RF1
dedup-avoidance invariant. The distributor ACKs the push **after** the Kafka write
is acknowledged.

### 5.3 Block-builder (consumer group)

Consumes the WAL, groups spans by `trace_id` over a **flush window**, and writes
span Parquet blocks (via blockstore `BlockWriter`) + `TraceIndex` updates → object
storage → commits offsets. A trace is flushed when its window closes — "complete
enough," *not* true completeness. **Late spans** (arriving after a trace's window
closed) land in a *later* block; the read path and compactor merge a trace's spans
across blocks. The nested-set columns are computed here, via the DFS pre-order over
each flushed trace's span tree. Write-then-commit + deterministic idempotent block
keys make a mid-flush crash re-do work, never lose or double-count it.

### 5.4 Live-store (consumer group) — the hot tier

Consumes the same WAL, assembling **recent traces** (by `trace_id`) in memory and
exposing them as a DataFusion `MemTable` (~30–60 min of hot data). Serves the
recent-trace fraction of search and by-id lookups without touching object storage.
It is pure read-path state: **rebuildable from offsets** on restart, holding no
durability of its own. (This is Tempo's live-store, which replaced the old
ingester's query role.)

## 6. TraceQL engine (`crabka-traceql`)

We build our own parser and planner. The grammar is *referenced* from
`icegatetech/icegate`'s ANTLR `TraceQLLexer.g4`/`TraceQLParser.g4` (Apache-2.0);
icegate also has a DataFusion planner that returns `NotImplemented` for structural
ops — useful as a starting skeleton, but the structural engine is ours. There is
**no** TraceQL equivalent of `promql-parser` on crates.io and **no** upstream
`.test`-style conformance corpus.

### 6.1 Grammar (lexer-verified tokens)

- **Comparison:** single `=` (EQ — there is **no `==`**), `!=`, `<`, `<=`, `>`,
  `>=`. Regex `=~` (RE, **fully anchored** `^...$`), `!~` (NRE).
- **Boolean:** `&&` (AND), `||` (OR), `!` (NOT). Arithmetic `+ - * / % ^`.
- **Presence:** nil checks (`.foo != nil`).
- **Scopes (attribute):** bare `.foo` = span **and** resource; `span.`, `resource.`,
  `parent.`, `event.`, `link.`, `instrumentation.`.
- **Scopes (intrinsic) use a COLON:** `span:`, `trace:`, `event:`, `link:`,
  `instrumentation:`. **There is no `resource:` intrinsic.**
- **Intrinsics:** `span:name`/`duration`/`kind`/`status`/`statusMessage`/`id`/
  `parentID`/`childCount`; `trace:duration`/`rootName`/`rootService`/`id`;
  `event:name`/`timeSinceStart`; `link:traceID`/`spanID`;
  `instrumentation:name`/`version`; and the structural `nestedSetLeft`/`Right`/
  `Parent`.

### 6.2 The single-span rule (critical)

Conditions inside **one** `{}` must hold on a **single span**. `{A} && {B}` matches
a trace when **different** spans satisfy each side. This distinction drives the
whole planner: intra-brace conditions are an `AND` over one span's columns;
inter-brace `&&` is a trace-level existential join.

### 6.3 Structural operators — the centerpiece

Structural operators relate spans by tree position and **return the RIGHT-hand
spans:**

- `>>` descendant, `<<` ancestor, `>` child, `<` parent, `~` sibling;
- negated `!>>`, `!<<`, `!>`, `!<`;
- union (both sides returned) `&>>`, `&<<`, `&>`, `&<`, `&~`.

These are realized via the **nested-set model**, *not* a per-trace tree-walk:

- **descendant** `B >> A` ⟹ `B.nested_set_left > A.nested_set_left &&
  B.nested_set_right < A.nested_set_right`
- **child / parent** `B > A` ⟹ `B.parent_id == A.nested_set_left`
- **sibling** `B ~ A` ⟹ `B.parent_id == A.parent_id && B.span_id != A.span_id` —
  a sibling is a *different* span sharing the same parent. The distinct-span
  predicate is mandatory: a naive equi-join on `parent_id` alone matches a span
  against *itself* (so a span would be reported as its own sibling) and wrongly
  matches a span that satisfies both `{condA}` and `{condB}`. This mirrors Tempo,
  whose sibling operator returns spans matching `{condB}` that have at least one
  *other* span matching `{condA}` under the same parent. Here `parent_id` is the
  nested-set parent column (`== parent.nested_set_left`), not the raw
  `parent_span_id` bytes; two roots share `parent_id = 0` (the sentinel) and are
  therefore treated as siblings of each other — consistent with Tempo. The
  same-trace requirement is already guaranteed by the partitioned-by-`trace_id`
  join.

The planner lowers each structural operator to a **partitioned self-join keyed by
`trace_id`**, with the nested-set range/equality as the join condition — integer
predicates, columnar, DataFusion-native joins. Call this lowering
`SpanStructuralJoin` (a DataFusion join plan, possibly with a thin custom physical
operator for the per-trace partitioning). The nested-set columns + the DFS are
built once at block-build (slice 1); slice 2 builds the join lowering for the core
operators (descendant/child/sibling/ancestor/parent); slice 3 adds the negated and
union forms.

### 6.4 Non-structural conditions & the AND fast path

Non-structural conditions are **columnar predicate pushdown** against the dedicated
attribute columns + intrinsic columns, with **bloom/tag-set block pruning** in
front. The storage/fetch contract mirrors Tempo's `SpansetFetcher`: an `AND`-only
query (`&&` of conditions on one span) is the **fast path** — a 3-level pushdown
(`KeepColumnChunk`/`KeepPage`/`KeepValue`: strings via dictionary-page skipping,
numerics via page min/max), reading only referenced columns and intersecting. `||`
forces unions. Generic-attribute (list) columns apply the array semantics from
§4.1.

### 6.5 Pipelines & aggregations

`| count()`, `| avg(f)`, `| max(f)`, `| min(f)`, `| sum(f)`, scalar filters
(`> N`), `| by(...)`, `| select(...)`, `coalesce()`, `with(...)`. These lower to
DataFusion aggregations over the matched spanSets.

### 6.6 TraceQL metrics

`| rate()`, `| count_over_time()`, `| sum/min/max/avg_over_time()`,
`| quantile_over_time(span:duration, .99, .9, .5)`, `| histogram_over_time()`,
`| compare()`, `| topk(n)`, `| bottomk(n)`, with `by(...)`. These lower to
**time-bucketed aggregation** producing **Prometheus-shaped series with `trace_id`
exemplars**, which powers `/api/metrics/query_range` and `/api/metrics/query`.
Exemplars require a configured `max_exemplars`. (Per-function maturity is flagged
experimental where Tempo flags it: `rate`/`count_over_time` are older;
`quantile`/`histogram_over_time` and the `avg/min/max_over_time` family are newer.)

### 6.7 Two query paths, one engine

- **By-id retrieval** is the **index-less bloom path** (§4.2a): time/block
  prefilter → bloom test → row-group binary search → assemble the full
  resource→scope→spans tree for `/api/v2/traces/{id}`.
- **Search** is the **full pipeline** (selectors → structural joins → pipeline
  aggregations), returning `spanSets` per trace for `/api/search`.

### 6.8 The `SpanStore` boundary

`crabka-traceql` is storage-agnostic via an injected `SpanStore`. The querier
(slice 5) implements it as the hot/cold UNION. Pinned public contract:

```rust
#[async_trait]
pub trait SpanStore: Send + Sync {
    async fn scan(&self, tenant: &str, matchers: &[SpanMatcher],
                  start_ns: i64, end_ns: i64) -> Result<ScanResult, TraceqlError>;
    async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16])
                  -> Result<Option<TraceSpans>, TraceqlError>;
    async fn tag_names(&self, tenant: &str, scope: Option<TagScope>,
                  start_ns: i64, end_ns: i64) -> Result<Vec<ScopedTag>, TraceqlError>;
    async fn tag_values(&self, tenant: &str, tag: &str,
                  start_ns: i64, end_ns: i64) -> Result<Vec<TypedValue>, TraceqlError>;
}

pub struct ScanResult { pub ctx: SessionContext, pub span_table: String }
// span_table may be a UNION view of live-store (hot) + blocks (cold)

pub struct TraceqlEngine<S: SpanStore> { /* store, opts */ }
pub struct EngineOpts { pub default_limit: usize /*20*/,
                        pub default_spss: usize /*3*/, pub max_traces: usize }

impl<S: SpanStore> TraceqlEngine<S> {
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self;
    pub async fn search(&self, tenant: &str, query: &str,
                  start_ns: i64, end_ns: i64, limit: usize)
                  -> Result<SearchResponse, TraceqlError>;
    pub async fn query_range(&self, tenant: &str, query: &str,
                  start_ns: i64, end_ns: i64, step_ns: i64)
                  -> Result<TraceMetricsResponse, TraceqlError>;
    pub async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16])
                  -> Result<Option<TraceSpans>, TraceqlError>;
}
```

Result types (`SearchResponse`/`TraceResult`/`SpanSet`/`SpanRef`, `TraceSpans`,
`TagScope`/`ScopedTag`/`TypedValue`/`AttrValue`, `SpanMatcher`,
`TraceMetricsResponse`, `TraceqlError`) are pinned in §3.3's crate contract; the
HTTP layer (§8) is a pure projection of them onto the Tempo JSON shapes. Slice 2
defines the trait + core engine; slices 3/5/7 consume it.

## 7. Metrics-generator (`metrics-generator` role)

The third consumer group. It runs the span-metrics and service-graph processors
and emits everything via the **remote_write client reused from the metrics signal**
(native-histogram codec + exemplars) to a configured Prometheus endpoint — Crabka's
own metrics backend. It produces **series**, not a Tempo endpoint.

### 7.1 Span-metrics (RED)

Per span, emit:

- `traces_spanmetrics_calls_total` — counter.
- `traces_spanmetrics_latency` — histogram (`_bucket`/`_sum`/`_count`); **each
  observation carries the span's `trace_id` as an exemplar** (`ObserveWithExemplar`)
  — this is the metrics→traces drill-down link.
- `traces_spanmetrics_size_total` — counter.
- `traces_target_info` — optional gauge (when target-info is enabled).

Dimensions: `service`, `span_name`, `span_kind`, `status_code` (`status_message`
off by default).

### 7.2 Service-graphs

Pair a **client-kind** span with the **server-kind** span of the **same trace** via
a bounded, **TTL'd edge store** keyed by `trace + relationship`, bounded by
`max_items`:

- partner arrives → record the edge;
- wait expiry → `traces_service_graph_unpaired_spans_total`;
- store full → `traces_service_graph_dropped_spans_total`.

Emit `traces_service_graph_request_total`, `_request_failed_total`,
`_request_server_seconds` (histogram), `_request_client_seconds` (histogram),
`_request_messaging_system_seconds` (histogram), `_unpaired_spans_total`,
`_dropped_spans_total`. Labels: `client`, `server`, `connection_type` (one of
unset / `virtual_node` / `messaging_system` / `database`).

**Service Graph is rendered by Grafana** from a linked Prometheus datasource over
these `traces_service_graph_*` series — it is *not* a Tempo HTTP endpoint. This is
the loop-closing point: traces feed series back into the metrics signal, where
Grafana reads them.

## 8. Tempo HTTP API surface

Served to Grafana's **built-in Tempo datasource** (which speaks the native Tempo
API *only* — never Jaeger/Zipkin *query*). Tenant via `X-Scope-OrgID`. Response
shapes must match Tempo exactly — the byte-equality analog for this layer.

- **`GET /api/echo`** → `200 "echo"` — the datasource health/test probe.
- **`GET /api/v2/traces/{traceID}`** — the by-id path. Returns the **v2 shape**
  `{ trace: { resourceSpans: [...] }, status: "...", message: "..." }`
  (OTLP-JSON wrapped), **not** the v1 `"batches"` quirk. There is **no** `metrics`
  object on this endpoint — `metrics` belongs only to `/api/search` and the
  `/api/v2/search/tags`/`values` endpoints. The v2 endpoint's distinguishing
  feature is that an oversized trace (one exceeding the max trace size) returns
  `status: "PARTIAL"` with an explanatory `message` rather than erroring; a
  fully-returned trace is `status: "COMPLETE"`. `Accept` header controls format;
  params `start`/`end` (epoch **seconds**).
- **`GET /api/search`** — `q=` (TraceQL) **or** `tags=` (legacy logfmt search);
  `minDuration`/`maxDuration` (Go durations); `start`/`end` (epoch seconds,
  required for backend search); `limit` (default 20); `spss` (default 3). Response:
  ```json
  { "traces": [ { "traceID", "rootServiceName", "rootTraceName",
                  "startTimeUnixNano" /*string nanos*/, "durationMs" /*int*/,
                  "spanSets": [ { "spans": [ { "spanID",
                       "startTimeUnixNano" /*string*/, "durationNanos" /*string*/,
                       "attributes" /*OTLP KV form*/ } ],
                     "matched" /*int*/ } ] } ],
    "metrics": { "totalBlocks", "inspectedTraces", "inspectedBytes", ... } }
  ```
- **`GET /api/v2/search/tags`** — `?scope=&start=&end=&q=` →
  `{ "scopes": [ { "name", "tags": [...] } ], "metrics": {...} }`. Scopes:
  `resource`/`span`/`intrinsic`/`event`/`link`/`instrumentation`.
- **`GET /api/v2/search/tag/{tag}/values`** — `?q=` →
  `{ "tagValues": [ { "type", "value" } ], "metrics": {...} }` (`type` = the
  TraceQL static type of the value).
- **`GET /api/metrics/query_range`** — `?q=&start=&end=&step=&exemplars=` (TraceQL
  metrics; Prometheus-like series; exemplars gated by `max_exemplars`).
- **`GET /api/metrics/query`** — instant TraceQL metrics.
- **`GET /ready`**, **`GET /status`** — operational probes.

The minimum surface Grafana's Tempo datasource exercises is `/api/echo`,
`/api/v2/traces/{id}`, `/api/search` (q + tags), `/api/v2/search/tags` +
`tag/{tag}/values`, and `/api/metrics/query_range`. All of it is a projection of
the `crabka-traceql` result types (§6.8).

## 9. Error handling, limits, multi-tenancy

- **Multi-tenancy:** `X-Scope-OrgID` → tenant id → Crabka's topic namespace + ACL
  principal + quota entity. Block/index object keys are tenant-prefixed; the WAL
  is `trace_id`-partitioned within a tenant.
- **Per-tenant limits** on Crabka token-bucket quotas: max traces per search, max
  spans per trace, ingest rate, max attribute size → Tempo-shaped `4xx`/`429`.
- **Crash-safety:** block-builder consumer-group offsets + **write-then-commit**
  with deterministic idempotent block keys (write block + `TraceIndex`, *then*
  commit offsets) — a crash between only re-does work. The **live-store** and
  **metrics-generator** hold no durable state: both are fully rebuildable from
  WAL offsets.
- **Late-span correctness:** because a trace can flush across multiple windows,
  the read path reassembles a trace from all its blocks (+ live-store); the
  compactor merges them. No span is lost; by-id and search both see the union.

## 10. Testing strategy

Mirrors Crabka's differential-testing ethos.

- **Differential vs. real Tempo** *(headline)* — ingest identical OTLP traces into
  Tempo and Crabka (testcontainers), run a TraceQL query corpus against both,
  assert equal results (by-id, search, structural ops, pipelines, TraceQL metrics).
  The byte-equality analog that proves "drop-in."
- **Curated golden-query corpus** — there is **no** upstream TraceQL `.test`
  corpus (unlike Prometheus's `promqltest`); build a curated golden set diffed
  against documented TraceQL semantics, plus the differential check above.
- **TraceQL parser/planner snapshots** — token-level (the `=`-not-`==`, anchored
  `=~`, colon-vs-dot scopes), AST snapshots, and DataFusion golden-plan +
  pushdown assertions (especially the `SpanStructuralJoin` lowering).
- **Nested-set correctness** — DFS pre-order assigns intervals such that
  ancestor `[left,right]` contains every descendant; structural-operator behavioral
  tests against hand-built span trees.
- **By-id index-less path** — bloom FP behavior, row-group min/max binary search,
  cross-block trace reassembly (late spans).
- **Block-builder crash-recovery** — kill mid-flush, restart, assert no loss / no
  dup / identical block keys.
- **Hot/cold merge** — a search spanning the live-store/block frontier returns each
  span exactly once.
- **Metrics-generator** — span-metrics RED values + exemplars; service-graph edge
  pairing, TTL expiry → unpaired, store-full → dropped; remote_write round-trip into
  the metrics backend.
- **Grafana integration** (testcontainers) — real Grafana, built-in Tempo
  datasource pointed at Crabka; drive Explore TraceQL queries + trace view; the
  Service Graph rendered from the linked Prometheus datasource.
- **Multi-tenant isolation** — tenant A cannot see B's traces/tags/spans; quotas
  enforced.

## 11. Scope & implementation slices

Full Tempo means scope-IN is everything above. The slices below are the plan's
phasing; each is independently testable and gets its own `writing-plans` plan when
reached. Per-task file sets are non-overlapping where noted, enabling parallel
subagent batches.

1. **Blockstore generalization + span block schema + `TraceIndex`** — extract the
   `BlockIndex` trait; existing index becomes `SeriesIndex`; relax mandatory
   `series_fingerprint`+`timestamp`. Define the flattened span block (incl. the
   **nested-set columns + the DFS pre-order** at block-build) and the `TraceIndex`
   (FNV-sharded `trace_id` bloom for index-less by-id + per-block tag sets/blooms).
2. **`crabka-traceql` core** — our parser (grammar referenced from icegate's
   `.g4`), the planner, selectors (scopes/intrinsics/array semantics, the
   single-span rule), non-structural pushdown + the `AND` fast path, and the
   **`SpanStructuralJoin`** lowering for the **core** structural operators
   (descendant/child/sibling/ancestor/parent). Defines the `SpanStore` trait + the
   pinned result types.
3. **TraceQL completeness** — full structural ops (the **negated** `!>>`/`!<<`/
   `!>`/`!<` and **union** `&>>`/`&<<`/`&>`/`&<`/`&~` forms), pipeline aggregations,
   **TraceQL metrics** (time-bucketed → Prometheus-shaped series + exemplars), and
   **tag discovery** (scoped tag names/values).
4. **Ingest service** — the `distributor` (OTLP/Jaeger/Zipkin/`/api/push`) →
   `trace_id`-partitioned WAL; the `block-builder` consumer group → span blocks +
   `TraceIndex`; the `live-store` consumer group (hot tier `MemTable`, rebuildable).
5. **Querier + Tempo HTTP API** — implement `SpanStore` as the hot/cold UNION
   (live-store + blocks); serve `/api/echo`, `/api/v2/traces/{id}`, `/api/search`,
   `/api/v2/search/tags` + `tag/{tag}/values`, `/api/metrics/query_range` + `query`,
   `/ready`, `/status`.
6. **Query-frontend** — search **sharding** (time: recent live-store vs backend
   blocks; + block + row-group jobs ~`target_bytes_per_job`) and **queueing** across
   queriers.
7. **Metrics-generator** — span-metrics (RED, latency exemplars) + service-graphs
   (TTL'd edge store) → **remote_write** into Crabka's metrics backend (reusing the
   metrics signal's client).
8. **Hardening** — per-tenant limits + multi-tenancy isolation, the
   **differential-vs-Tempo** corpus, and **Grafana integration** (Tempo datasource +
   Service Graph end-to-end).

## 12. Relation to the four-signal vision

Traces is the **third tenant** of `crabka-blockstore`, and the one that *forces*
the generalization the logs spec promised: by needing a fundamentally different
index (`TraceIndex` = `trace_id` bloom + tag sets, *not* a series fingerprint
index) and a non-mandatory schema (span columns, no `series_fingerprint`+
`timestamp`), it justifies extracting the **`BlockIndex` trait** — the same
pluggable seam that **profiles / Pyroscope** will reuse for its profile-type +
symbol index. `crabka-traceql` sits beside `crabka-logql` and `crabka-promql` on
the same DataFusion substrate and the same role-selectable service skeleton.

And it **closes the loop**: the metrics-generator feeds `traces_spanmetrics_*` and
`traces_service_graph_*` series back into the **metrics** backend, where Grafana
reads them — so the traces signal is simultaneously a producer to, and the
exemplar-source for, the metrics signal. `trace_id` (hex) is the universal join
key: metrics→traces via latency exemplars, traces→logs/metrics/profiles via Grafana
datasource correlation (no Tempo endpoint), all keyed by `trace_id`.

| Signal | Front-end crate | API emulated | Block payload | Index impl |
|---|---|---|---|---|
| **Logs** | `crabka-logql` | Loki HTTP | `line`, metadata | `SeriesIndex` |
| **Metrics** | `crabka-promql` | Prometheus HTTP | float / native-hist / exemplar | `SeriesIndex` |
| **Traces** | `crabka-traceql` | Tempo HTTP | flattened span + nested-set | **`TraceIndex`** |
| **Profiles** | `crabka-pprof` | Pyroscope HTTP | sample/stack | (profile-type + symbol, *reuses `BlockIndex`*) |

## 13. Open questions for planning

- **`SpanStructuralJoin` physical operator** — does the per-trace partitioned
  self-join need a *custom* DataFusion physical operator, or can it be expressed as
  a standard partitioned hash/range join with a `trace_id` partition key plus
  nested-set predicates? Slice 2 should prototype both and pick the simpler one that
  hits the columnar fast path.
- **icegate grammar port** — port the `.g4` to a hand-written recursive-descent
  parser, or generate from ANTLR? Recommend hand-written (no ANTLR-runtime dep, full
  control over the colon-vs-dot scope quirks), using the `.g4` purely as the
  spec-of-record.
- **Flush-window vs late-span trade-off** — window length sets the
  completeness/latency balance and how often a trace splits across blocks. Tune
  against the compaction-merge cost; revisit under load.
- **Live-store retention window** — the ~30–60 min hot-tier size, and how the
  hot/cold frontier is computed per partition (committed block-builder offset vs.
  max-compacted timestamp).
- **Attribute promotion policy** — which resource/span attrs to promote to
  dedicated columns by default, and whether promotion is per-tenant configurable
  (vParquet5's dynamic model allows it).
- **Bloom shard count / FP tuning** — `shard_count`, `0.01` FP, and `100 KiB` shard
  size are Tempo's defaults; revisit against real `trace_id` cardinality and block
  sizes.
- **TraceQL metrics maturity gating** — which TraceQL-metrics functions to ship
  behind an experimental flag, mirroring Tempo's per-version maturity.

## 14. As-built notes (implementation deviations)

The traces signal is implemented across `crates/blockstore`, `crates/traceql`, and
`crates/traces` and is green (build + unit/integration tests; the
differential-vs-Tempo and Grafana legs are `#[ignore]`, Docker-gated). The
implementation honors the spec's *contracts* (TraceQL semantics, Tempo API shapes,
the nested-set structural model, the `trace_id`-partitioned WAL dedup-avoidance
invariant). The following deliberate deviations from the original design/plans
stand as-built:

- **`BlockIndex` seam without a generic `BlockStore`.** The pluggable per-signal
  index trait (`BlockIndex`) exists; the logs/metrics index (`SeriesIndex`) and
  `TraceIndex` both implement it, and the profiles signal's `ProfileIndex` embeds
  `SeriesIndex` — this is the load-bearing generalization the spec required.
  `BlockStore` is **not** parameterized over `BlockIndex`: the traces write/read
  path uses `BlockWriter` + `TraceIndex` + declared `span_block_decl()` schema
  validation directly, so a generic `BlockStore<I>` facade was unnecessary and would
  have churned the shared logs/metrics/profiles path for no functional gain.
- **TraceQL-metrics maturity flag omitted (open question resolved).** Per the
  project rule against default-off feature gates for new behavior, the
  "experimental" TraceQL-metrics functions (`histogram_over_time`, `compare`,
  `topk`/`bottomk`, the `quantile`/`avg`/`min`/`max_over_time` family) ship
  **always-on** rather than behind a cargo feature. Closes the §13 maturity-gating
  open question: no gate.
- **Query-frontend is the typed `frontend/` module tree.** It is built as the
  planned module tree (`wire`/`backend`/`job`/`merge`/`metrics_merge`/`queue`/
  `config`/`http_backend`/`server` + the `QueryFrontend` orchestrator) with a
  `QuerierBackend`/`BlockCatalog` trait seam (`MockQuerier`/`MockCatalog` for tests,
  `HttpQuerier`/`TraceIndexCatalog` in production). It shards (time → tier → block →
  row-group), fans jobs out with bounded concurrency, and merges over **typed serde
  wire structs** (not raw `serde_json::Value`), enforcing `limit` (newest-first) and
  `spss` and accumulating the `metrics{}` job-accounting block. The merge currency is
  the typed Tempo-JSON wire model rather than the richer `crabka-traceql` result
  types, because the querier's search JSON is the thin Tempo shape (lossless to union
  at the wire level). **Trace-by-id is frontend-side typed assembly**: it fans one
  job per querier (the querier reassembles a trace across blocks and exposes no
  block-scoped by-id; different queriers' live-stores hold different recent spans),
  unions the v2 `resourceSpans`, dedupes by `spanID`, and sizes the result to
  COMPLETE/PARTIAL. Shard failures propagate for the data-partitioning queries
  (search/tags/metrics); by-id tolerates per-querier failure and fails only if every
  querier does.
- **SQL-string planner.** `crabka-traceql` lowers to DataFusion by emitting SQL
  (incl. the nested-set structural self-join predicate algebra) rather than building
  `LogicalPlan`s programmatically; the structural predicates match the spec exactly.
- **Tempo HTTP API in one module.** The querier serves the full Tempo surface from a
  single `querier/http/mod.rs` rather than split `json`/`traces`/`search`/`metrics`
  files. Same wire surface.
- **Conformance-corpus harness limits.** The `.case` golden-corpus DSL covers
  selectors, typed comparisons, structural (core/negated/union), pipelines, and the
  TraceQL-metrics families. Tag-discovery and array any/none semantics are exercised
  by inline `#[cfg(test)]` unit tests but not by the `.case` corpus (the DSL has no
  `tag_names`/`tag_values` case kind and the shared fixture has no repeated-value
  attributes); extending the harness is the only way to fold those into the corpus.
- **Service Graph differential leg.** The `#[ignore]` Grafana test provisions a
  Prometheus datasource and asserts the metrics-generator emits
  `traces_service_graph_request_total` with the right edge labels, but the live
  metrics-generator → Prometheus `remote_write` → Grafana round-trip is not stood up
  in-container (documented in the test, not faked).
