# Crabka as a Grafana Observability Backend — Metrics Signal (Grafana-Mimir Replacement)

**Status:** Design / approved for planning
**Date:** 2026-06-18
**Scope of this spec:** the **metrics** signal — a *full* Grafana-Mimir-equivalent
metrics backend (not an MVP). Covers float samples and native histograms,
exemplars, the full PromQL language, remote_write v1/v2 + OTLP ingest,
multi-tenancy with HA dedup, out-of-order ingestion, query-frontend
splitting/sharding, the ruler (recording + alerting rule *evaluation*), and the
complete Prometheus/Mimir HTTP API. Bundled **Alertmanager is out of scope** — it
is its own sub-project; the ruler dispatches to any Alertmanager-API endpoint.

This is the second signal in the LGTM+P replacement. It reuses the shared
substrate designed for logs (`crabka-blockstore`) and follows the same
"emulate the wire contract, don't fork the product" pattern. See the logs spec:
[2026-06-18-crabka-observability-logs-design.md](2026-06-18-crabka-observability-logs-design.md).

## 1. Goal & thesis

Replace Grafana Mimir and serve as Grafana's metrics datasource, by emulating
Mimir's *external* surfaces (Prometheus HTTP API, remote_write v1/v2, ruler API,
cardinality/tenant APIs) on Crabka's substrate — the Kafka log as the durable
WAL, `crabka-blockstore` for columnar Parquet blocks on object storage, and
DataFusion for query. We reproduce Mimir's *contracts*, not its internal
components.

**The load-bearing realization.** Mimir's heaviest machinery — the ingester's
write-ahead log and its 3× replication — is exactly what a Kafka log already is.
The WAL topic *is* the ingester WAL; Crabka partition replication *is* the
ingester replication factor. The querier rebuilds its in-memory head purely by
replaying WAL offsets, so there is no separate ingester WAL to manage or
replicate.

## 2. Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | Ambition | **Full** Mimir replacement (not an MVP) |
| 2 | PromQL engine | `promql-parser` (faithful Prometheus-3.8 grammar port, parser-only — no DataFusion/arrow deps) + **our own** PromQL→DataFusion planner. Symmetric with the LogQL decision; sidesteps the DataFusion-version coupling that vendoring a full engine (e.g. GreptimeDB's) would impose |
| 3 | Data model | Float samples **and** native (exponential) histograms **and** exemplars. Classic histograms/summaries are ordinary float series (no special support) |
| 4 | Boundary | Ruler (recording + alerting rule *evaluation*) **in**; bundled multi-tenant Alertmanager **out** (own sub-project) |
| 5 | Storage | `crabka-blockstore` (shared with logs) with metric-specific block schemas |
| 6 | Query engine | DataFusion (shared substrate), following the GreptimeDB-proven custom-operator pattern for range-vector semantics |
| 7 | Integration | Prometheus/Mimir HTTP API emulation → Grafana's built-in Prometheus datasource, unmodified |
| 8 | Process model | Role-selectable service (`distributor`/`compactor`/`querier`/`query-frontend`/`ruler`); uses the Crabka broker as its WAL |

## 3. Architecture

### 3.1 Mimir component → Crabka realization

| Mimir component | Crabka realization |
|---|---|
| **Distributor** (validate, HA dedup, tenant split) | `distributor` role — terminates remote_write/OTLP, HA-dedup **before** the WAL append, writes to the WAL topic |
| **Ingester** (head + WAL + replication) | **The Crabka broker** — WAL topic *is* the WAL; partition replication *is* the ingester RF; the head is an in-memory structure the querier rebuilds from the WAL tail |
| **Store-gateway + Querier** | `querier` role — DataFusion over `crabka-blockstore`, merging hot (WAL-tail head) + cold (blocks) |
| **Query-frontend / scheduler** | `query-frontend` role — time-splitting, query sharding (by series), result caching |
| **Compactor** | `compactor` role — WAL consumer-group → columnar blocks + index/exemplar sidecar → object storage |
| **Ruler** | `ruler` role — per-tenant recording + alerting rule evaluation (PromQL on a schedule) |
| **HA tracker KV** | a **compacted Crabka topic** keyed `(tenant, cluster) → (elected __replica__, lease ts)` |
| **Alertmanager** | out of scope — own sub-project (ruler dispatches to any Alertmanager API) |
| **Overrides / limits** | per-tenant config on Crabka's quota/ACL machinery |

### 3.2 Crate layout

- `crabka-blockstore` *(shared with logs)* — extended with three metric block
  schemas (float, native-histogram, exemplar sidecar) + a symbol table. Stays
  signal-agnostic; metrics register different payload columns.
- `crabka-promql` — `promql-parser` integration + the PromQL→DataFusion planner +
  the custom range-vector operators (`SeriesDivide`/`SeriesNormalize`/
  `InstantManipulate`/`RangeManipulate` + the `RangeArray` Arrow array) +
  rate-family/histogram UDFs + the Prometheus `.test` conformance harness.
- `crabka-metrics` — the role-selectable service wiring blockstore + promql + a
  Kafka client, plus the wire surfaces (remote_write v1/v2, OTLP metrics,
  Prometheus HTTP API). Internal `wire` module owns the remote_write protobuf
  types + content negotiation.

### 3.3 Reuse vs net-new

**Reuse (from the codebase):** OTLP `MetricsData` decode
([client_metrics/otlp.rs](crates/broker/src/client_metrics/otlp.rs)) —
extended for exponential histograms; `prometheus-client` 0.25 + OpenMetrics
exposition; the axum 0.8 TLS-aware serve patterns from
[grpc-gateway/serve.rs](crates/grpc-gateway/src/serve.rs) and
[metrics_server.rs](crates/broker/src/metrics_server.rs);
`opentelemetry-proto` 0.32 (`gen-tonic-messages`, `metrics`).

**Net-new:** the PromQL engine, TSDB block schemas, remote_write v1/v2, the
Prometheus HTTP query API, exemplar storage, the ruler, query-frontend sharding.

## 4. Data model

Series identity reuses blockstore's `Labels`/`SeriesFingerprint`, plus a per-block
**symbol table** for string interning (remote_write v2 sends interned symbols; we
keep them interned at rest).

Three block schemas on blockstore's signal-agnostic substrate (each = mandatory
`series_fingerprint` + `timestamp` + payload):

### 4.1 Float-sample block
Payload `value: Float64`. Counters, gauges, and *classic* histogram/summary
series (`_bucket{le}`/`_sum`/`_count`/`{quantile}`) — these need zero special
support.

### 4.2 Native-histogram block
Store **absolute** bucket counts (decode the wire deltas at ingest — Prometheus
delta-encodes only to shrink varints, and deltas fight Parquet's RLE/dictionary
codecs). Retain `is_float`/`schema`/`reset_hint` so the exact wire `oneof`
(integer vs float histogram) is re-emitted faithfully on read-back.

```
schema:Int8  is_float:Bool  reset_hint:Int8  zero_threshold:Float64  zero_count:Float64
count:Float64  sum:Float64
positive_spans:List<Struct<offset:Int32,length:UInt32>>   positive_counts:List<Float64>
negative_spans:List<Struct<offset:Int32,length:UInt32>>   negative_counts:List<Float64>
custom_values:List<Float64>   // only when schema == -53 (NHCB)
start_timestamp_ms:Int64 (nullable)
```

Notes from the wire model: `schema ∈ {-53 (NHCB)} ∪ [-4, 8]`; integer histograms
carry delta-encoded counts (first element absolute), float histograms carry
absolute counts; for NHCB only the positive side + `custom_values` are used
(negative side and zero bucket unused).

### 4.3 Exemplar sidecar block
```
series_fingerprint  timestamp  value:Float64  trace_id:Utf8  span_id:Utf8  labels:Map<Utf8,Utf8>
```
`trace_id`/`span_id` are promoted to dedicated columns (the dominant
metrics→traces join key; normalize OTLP's `bytes` form and Prometheus's label
form into one column). Sparse, sorted by `(fingerprint, timestamp)`, enforces the
128-codepoint exemplar-label cap at ingest. Served by `/api/v1/query_exemplars`.

### 4.4 Staleness & metadata
Staleness markers stored as Prometheus's stale-NaN bit pattern; the querier
terminates a series at a stale marker (earlier than lookback expiry) and never
surfaces it as a value. Metric metadata (type/help/unit from remote_write v2 /
OTLP) lives in a per-tenant metadata index → `/api/v1/metadata`.

## 5. Ingest

Three doors, all landing in the metrics WAL topic.

### 5.1 remote_write v1 + v2 (`/api/v1/push`)
Dispatch on the Content-Type `proto=` param (`prometheus.WriteRequest` vs
`io.prometheus.write.v2.Request`), snappy-**block** decompress (framed MUST NOT be
used). v1 is the stable target; v2 is `2.0-rc.4`/experimental (build it, expect
churn).

- **v2 symbol table:** `symbols[0]` MUST be `""`; `labels_refs` are even-length
  uint32 pairs into `symbols`; resolve + validate all refs. Native histograms,
  exemplars (field 4), and `Metadata` (type/help/unit) are carried. Created/start
  timestamps live on `Sample.start_timestamp` (#3) / `Histogram.start_timestamp`
  (#17), **not** as a top-level field.
- **v2 response contract:** emit `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written`
  on every `204` — their absence on a 2xx signals "v2 unsupported" and the sender
  downgrades.
- **Status codes:** `400` invalid/non-retriable · `415` unsupported · `429`
  retriable · `5xx` retriable; idempotent under partial-write retries.

### 5.2 OTLP metrics (`/otlp/v1/metrics`, HTTP-protobuf + gRPC)
Extend [otlp.rs](crates/broker/src/client_metrics/otlp.rs) decode to the
full type mapping: monotonic `Sum`→counter(`_total`), non-monotonic `Sum`→gauge,
`Gauge`→gauge, `Histogram`→classic, **`ExponentialHistogram`→native** (scale↔schema
clamp: downscale losslessly if `scale>8`, drop if it can't fit; fix the
lower-vs-upper-boundary off-by-one offset), `Summary`→summary. Delta-temporality
sums accumulate to cumulative (Prometheus has no delta). Name/label normalization
behind a `translation_strategy` enum (default `UnderscoreEscapingWithSuffixes` for
tooling parity); resource attrs → `target_info` gauge.

### 5.3 Native Kafka produce
Samples produced straight to the WAL topic — free, and unique to Crabka.

### 5.4 Distributor pipeline (before the WAL append)
validate (per-tenant sample/label limits) → **HA dedup** (inspect the request's
first series' `cluster` + `__replica__` labels; consult the compacted HA-tracker
topic for the elected replica per `(tenant, cluster)`; non-elected → drop and
return **HTTP 202** so the loser doesn't retry; strip `__replica__` before
writing) → tenant-route → produce to the WAL topic, partitioned by
`(tenant, series_fingerprint)` so per-series order is preserved.

### 5.5 Out-of-order handling (after the WAL)
The Kafka log is offset-ordered but timestamp-OOO-tolerant — it carries
already-deduplicated, replica-stripped records that may be timestamp-OOO. The
compactor sorts by `(fingerprint, timestamp)` when building a block, so OOO
*within* a window is free; cross-window OOO within the tenant's
`out_of_order_time_window` produces overlapping blocks the querier merges; beyond
the window → `too-old-sample` rejection. This places stateful HA coordination at
the write edge (needs a shared KV) and cheap offset-vs-timestamp reconciliation at
the read/compaction edge (the log already gives order) — mirroring why Prometheus
writes its WBL *after* ingestion.

## 6. PromQL engine

**Parse:** `promql_parser::parser::parse(query) -> Expr` (promql-parser 0.10.0,
grammar-faithful to Prometheus 3.8). **Plan:** recurse the AST into a DataFusion
`LogicalPlan`, following the GreptimeDB-proven custom-operator pattern (adapted to
our DataFusion git-main pin) — range-vector semantics have no native DataFusion
equivalent.

### 6.1 Custom operators
Each a `UserDefinedLogicalNodeCore` + matching `ExecutionPlan` + stream:
- **SeriesDivide** — partition a multi-series batch into per-series groups by
  label identity.
- **SeriesNormalize** — apply `offset`/`@`, sort by time, drop NaNs; one series
  per batch.
- **InstantManipulate** — instant-vector selection on the step grid honoring the
  lookback-delta (5m default) and staleness markers.
- **RangeManipulate** — materialize range vectors by folding `(timestamp, value)`
  into **`RangeArray`** columns: one sub-array per aligned eval timestamp's
  lookback window.

**`RangeArray`** — a custom list-like Arrow array where each cell is a *slice* of
an underlying contiguous array ("the samples in this step's window"),
representing range vectors as one columnar value without row explosion. DataFusion
has no equivalent; we reimplement it. Range vectors are left-open, right-closed
`(t-range, t]`.

### 6.2 Functions
- `rate`/`increase`/`delta`/`irate`/`idelta` are **ScalarUDFs** over the
  `RangeArray`-paired args (not UDAFs), implementing the exact counter-reset +
  extrapolation algorithm — reset-correct on any decrease,
  `avgInterval = sampledInterval/(n-1)`, `1.1×` boundary threshold, half-interval
  extrapolation cap, positive-counter zero-anchor clamp. **The #1 correctness
  trap** — match byte-for-byte. `delta`/`idelta`/`deriv` are gauge-only (no reset
  correction).
- Aggregations: `sum`/`avg`/`min`/`max`/`count`/`group`/`stddev`/`stdvar` +
  param ops `topk`/`bottomk`/`quantile`/`count_values`, with `by`/`without`
  (`without` always drops `__name__`; `topk`/`bottomk` keep original labels;
  `min`/`max`/`stddev`/`stdvar`/`topk`/`bottomk`/`quantile` ignore histograms).
- Binary ops with full vector matching (`on`/`ignoring`, `group_left`/
  `group_right`, `bool`), correct PromQL precedence (`^` right-assoc > `*/%
  atan2` > `+-` > comparisons > `and unless` > `or`).
- `histogram_quantile` — classic path (`le`-bucket fold, forced-monotonic, linear
  interp) **and** native path (operate on native-histogram columns); native
  accessors `histogram_count`/`sum`/`avg`/`fraction`.
- Subqueries (`expr[range:resolution]`, resolution defaults to the global eval
  interval), `@`/`offset` (combine order-independently), the `_over_time` family,
  `label_replace`/`label_join`, `clamp*`, `predict_linear`. The experimental
  function tier (incl. `double_exponential_smoothing` — the renamed
  `holt_winters`) sits behind a feature flag.

### 6.3 Leaf data source
The planner's `TableScan` resolves matchers → `blockstore.scan_context` (index
prune → blocks) **UNION** the WAL-tail head (hot recent samples) — the same
hot/cold merge as logs.

### 6.4 Query-frontend role
Split range queries into step-aligned sub-ranges, **shard by series** (Mimir's
vertical sharding via a `__query_shard__` selector), fan sub-queries across
queriers in parallel, cache results (a Crabka object-store/topic-backed cache).

## 7. The ruler

Per-tenant **rule groups** (recording + alerting), exposed via the
Prometheus/Mimir ruler API (`/prometheus/config/v1/rules` CRUD, `/api/v1/rules`,
`/api/v1/alerts`).

- **Evaluation:** on each group's interval, run its PromQL via the querier.
  **Recording rules** write results back as new series by *producing to the
  metrics WAL topic* — derived series are first-class and queryable, no special
  path. **Alerting rules** evaluate, track `pending → firing` per the `for:`
  duration, and dispatch firing alerts to a configured Alertmanager-API endpoint
  (external today; a future `crabka-alertmanager` sub-project later).
- **State** (alert pending/firing, last-eval) lives in a compacted per-tenant
  topic — rebuildable.
- **Sharding:** rule groups distributed across ruler instances by
  `(tenant, group)` hash.

## 8. HTTP API surface

Served under both bare `/api/v1/` and Mimir's `/prometheus/api/v1/` prefix; tenant
via `X-Scope-OrgID`. Response shapes must match Prometheus exactly
(`status`/`data.resultType` of `vector`/`matrix`/`scalar`/`string`, `warnings`,
`error`/`errorType`) — the byte-equality analog.

- **Query:** `/query`, `/query_range`, `/query_exemplars`, `/series`, `/labels`,
  `/label/{name}/values`, `/metadata`, `/status/*`, `/format_query`,
  `/parse_query`.
- **Cardinality (Mimir):** `/cardinality/{label_names,label_values,active_series}`.
- **remote_read:** `/api/v1/read` (federation/Thanos parity).
- **Write:** `/api/v1/push` (remote_write v1/v2), `/otlp/v1/metrics`.
- **Ruler:** `/prometheus/config/v1/rules`, `/api/v1/rules`, `/api/v1/alerts`.

## 9. Error handling, limits, HA

- **Ingest:** `400`/`415`/`429`/`5xx` per §5.1; idempotent under retries.
- **Per-tenant limits** on Crabka token-bucket quotas: ingestion rate, max series,
  max label length, max samples/series per query → Prometheus-shaped `4xx`/`422`.
- **HA:** leader election via the compacted HA-tracker topic with `failover`/
  `update` leases.
- **Crash-safety:** compactor consumer-group offsets + deterministic idempotent
  block keys (write block+index, *then* commit); the head is always rebuildable
  from WAL offsets.

## 10. Testing

- **PromQL conformance via Prometheus's own `.test` corpus** *(headline)* — the
  21 files in `promql/promqltest/testdata/` (`aggregators`, `functions`,
  `histograms`, `native_histograms`, `at_modifier`, `staleness`, `subquery`,
  `operators`, …). Write the **first Rust harness** for the `.test` DSL
  (load / eval-instant / eval-range, expanding-point syntax, native-histogram
  literals, both legacy and new `expect` assertion forms). Pin to a Prometheus
  tag; vendor the files (Apache-2.0, keep attribution).
- **prometheus/compliance** PromQL Compliance Test Harness — black-box our HTTP
  server vs a reference Prometheus over identical data.
- **Differential vs real Mimir** (testcontainers) — remote_write identical data to
  both, assert query-corpus equality.
- **Wire round-trips** — remote_write v1/v2 decode; native-histogram int/float +
  delta-decode; OTLP exponential-histogram scale-clamp + offset.
- **Grafana integration** — built-in Prometheus datasource pointed at Crabka.
- Counter-reset/extrapolation, HA-dedup, OOO, and staleness behavioral tests.

## 11. Scope & implementation slices

Full Mimir means scope-IN is everything above. The slices below are the plan's
phasing; each is independently testable and gets its own `writing-plans` plan when
reached.

1. **Blockstore metrics schemas** — float + native-histogram + exemplar block
   types + symbol table.
2. **`crabka-promql` core** — parser integration + the operator pattern
   (`SeriesDivide`/`Normalize`/`Instant`/`Range` + `RangeArray`) + selectors +
   rate-family + aggregations + binary ops + the `.test` harness. *(The big one —
   likely sub-sliced.)*
3. **Query completeness** — `histogram_quantile` (classic + native), full function
   catalog, subqueries, `@`/`offset`.
4. **Ingest service** — remote_write v1/v2 + OTLP + Kafka produce + distributor +
   HA dedup + compactor.
5. **Querier + Prometheus HTTP API** + hot/cold merge.
6. **Query-frontend** — split / shard / cache.
7. **Ruler** — recording + alerting + rule API.
8. **Hardening** — multi-tenancy/limits, remote_read, prometheus/compliance +
   differential-vs-Mimir.

## 12. Relation to the four-signal vision

Metrics is the second tenant of `crabka-blockstore`; `crabka-promql` sits beside
`crabka-logql` on the same DataFusion substrate and the same role-selectable
service skeleton. This validates the generalization claimed in the logs spec §11:
each signal = one query-dialect front-end + one wire-compatible API + one block
schema, all on the shared substrate.

## 13. Open questions for planning

- **`RangeArray` ownership** — reimplement from scratch vs. adapt GreptimeDB's
  `range_array.rs` (Apache-2.0, but coupled to their DataFusion pin). Recommend
  clean-room against our git-main DataFusion, learning from their structure.
- **remote_write v2 churn** — pin to `2.0-rc.4` proto; revisit when it stabilizes.
- **Symbol-table scope** — per-block vs per-tenant-global interning, and how it
  interacts with blockstore's existing series dictionary.
- **Head/ingester boundary** — keep the in-memory head inside the `querier` role
  vs. a dedicated `ingester` role for very high cardinality. MVP of the full build
  keeps it in the querier; revisit under load.
- **Exemplar retention** — bounded ring (Prometheus/Mimir style) vs. full block
  retention; the sidecar block supports either.
- **`.test` assertion-format migration** — the DSL is mid-migration on Prometheus
  `main`; pin to a tagged release to get a single assertion form.
