# Crabka as a Grafana Observability Backend — Profiles Signal (Grafana-Pyroscope Replacement)

**Status:** Design / approved for planning
**Date:** 2026-06-18
**Scope of this spec:** the **profiles** signal — a *full* Grafana-Pyroscope-equivalent
continuous-profiling backend (not an MVP). Covers pprof / OTLP-profiles / legacy-SDK
ingest, the Kafka-WAL ingest-storage pipeline (distributor → WAL → block-builder),
deduplicated symbol-DB profile blocks on object storage, the **language-less** query
model (a label selector + a profile type + an aggregation — no LogQL/PromQL/TraceQL
analog), the flamegraph-merge engine, the full Pyroscope query API that Grafana's
built-in Pyroscope datasource speaks (Connect `querier.v1` + the legacy `/pyroscope/render`
flamebearer surface for the Profiles Drilldown app), native query-time symbolization
(debuginfod + DWARF/ELF/`.gopclntab`), and multi-tenancy.

This is the **fourth and final** signal in the LGTM+P replacement. It reuses the shared
substrate designed for logs (`crabka-blockstore`), the pluggable `BlockIndex` seam that
traces extracted, the label-postings machinery the metrics signal built, and the
role-selectable service skeleton — and follows the same "emulate the wire/API contract,
don't fork the product" pattern. See the sibling specs:
[2026-06-18-crabka-observability-logs-design.md](2026-06-18-crabka-observability-logs-design.md),
[2026-06-18-crabka-observability-metrics-mimir-design.md](2026-06-18-crabka-observability-metrics-mimir-design.md),
and
[2026-06-18-crabka-observability-traces-tempo-design.md](2026-06-18-crabka-observability-traces-tempo-design.md).

## 1. Goal & thesis

Replace Grafana Pyroscope and serve as Grafana's profiles datasource, by emulating
Pyroscope's *external* surfaces (the Connect `querier.v1.QuerierService` API, the
`push.v1.PusherService` + legacy `/ingest` push doors, the experimental OTLP-profiles
door, and the legacy `/pyroscope/render` flamebearer endpoints) on Crabka's substrate —
the Kafka log as the durable ingest WAL, `crabka-blockstore` for columnar Parquet profile
blocks + a deduplicated symbol DB on object storage, and DataFusion for the cheap
fold-by-stacktrace step. We reproduce Pyroscope's *contracts* (the profile-type strings,
the `FlameGraph` 4-ints-per-bar encoding, the Connect method/field shapes, the flamebearer
JSON), not its block byte-format or internal components.

**The honest divergence — be explicit about this.** Unlike Tempo 3.0 (whose GA
architecture is *already* Kafka-native and gave the traces signal a near-1:1 mapping),
**Pyroscope v2 is NOT Kafka-native.** Pyroscope v2's GA storage pipeline is
`distributor → direct gRPC → diskless segment-writer → object storage`, coordinated by a
**Raft metastore** (the only stateful component), with an **object-storage DLQ** as the
durability fallback. There is no Kafka in that path. **Crabka deliberately diverges:** we
route profiles through the **Crabka broker as a Kafka WAL** — exactly as logs, metrics,
and traces do — because (a) it keeps the profiles signal consistent with the other three on
the shared substrate, and (b) it aligns us with the *ingest-storage* design that Mimir and
Tempo already validate (durability is the broker's partition replication; consumers replay
offsets). And we **replace Pyroscope's Raft metastore** with the **blockstore `ProfileIndex`
(`impl BlockIndex`) + Crabka's existing KRaft** — the block-discovery/metadata role the Raft
metastore plays is served by the per-block index + the broker's own consensus, so we add no
new consensus system. This is **defensible and differentiated, but it is a divergence, not a
match** — we do not claim "matches Pyroscope's architecture." We claim wire/API/semantic
compatibility.

**The defining difference from the other three signals.** Profiles has **no query
*language***. There is no LogQL, no PromQL, no TraceQL analog — and so there is **no
parser, no grammar, no `.test` conformance corpus**. A profiles "query" is just **a label
selector + a profile type + an aggregation** (merge-to-flamegraph, select-series, or diff),
issued over a Connect-RPC API. The engineering weight therefore moves off a language and
onto two things: **(1) the deduplicated symbol-DB data model** (the ~60%-of-block-size
dedup lever) and **(2) the flamegraph-merge engine** (fold raw stacktrace IDs → symbolized
tree → the 4-ints-per-bar `FlameGraph`). The final heavy slice is **native symbolization**
(query-time debuginfod + DWARF/ELF resolution for unsymbolized eBPF profiles).

## 2. Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | Ambition | **Full** Pyroscope replacement (not an MVP) |
| 2 | Query model | **No query language.** A query = label selector + profile type + aggregation over Connect-RPC. The engine weight is the **symbol-DB data model** + the **flamegraph-merge engine**. No parser, no grammar, no conformance corpus |
| 3 | Ingest substrate | **Crabka broker as the Kafka WAL** — a *deliberate divergence* from Pyroscope v2 (distributor → direct gRPC → diskless segment-writer → object store, coordinated by a Raft metastore, with an object-store DLQ). Consistent with logs/metrics/traces; aligns with Mimir/Tempo ingest-storage. Durability = partition replication; consumers replay offsets |
| 4 | Metastore replacement | **No Raft metastore.** Block discovery/metadata is the blockstore **`ProfileIndex` (`impl BlockIndex`) + Crabka KRaft** — we reuse the consensus the broker already has rather than adding Pyroscope's standalone Raft |
| 5 | Storage | `crabka-blockstore` (shared with logs/metrics/traces), reusing the **`BlockIndex` trait** that traces extracted. Add a `ProfileIndex` (`impl BlockIndex`) = label-series postings (**reuse the metrics `SeriesIndex` label-postings machinery**) + a profile-type index (`__profile_type__` → series) + per-block time-range + a stacktrace-partition map |
| 6 | Profile block format | **Crabka choice: a flattened samples fact table** — *one row per SAMPLE* — plus a deduplicated **symbol DB**. *Not* phlaredb byte-format compatible (phlaredb is one-row-per-profile with nested `Samples[]`; greenfield). We need semantic/API compat, not block-format compat |
| 7 | Profile types | **Not hardcoded.** `profile_type` = the 5-part `name:sample_type:sample_unit:period_type:period_unit` string carried as the `__profile_type__` label (Go/pprof and Java/JFR differ; discover from data) |
| 8 | Symbolization | **In scope** (the final heavy slice). SDK/pprof + Alloy-eBPF arrive **pre-symbolized**; OTel-native-eBPF arrive **unsymbolized** (build_id + address) and are symbolized **lazily at query time** via debuginfod + DWARF/ELF/`.gopclntab` parse + demangle |
| 9 | Process model | Role-selectable service (`distributor`/`block-builder`/`querier`/`query-frontend`/`compactor`/`symbolizer`); uses the Crabka broker as its ingest WAL |
| 10 | Grafana integration | **Connect `querier.v1` emulation** → Grafana's built-in Pyroscope datasource, unmodified, **plus** the legacy `/pyroscope/render` + `/pyroscope/render-diff` flamebearer endpoints the Profiles Drilldown app uses |

## 3. Architecture

### 3.1 Pyroscope component → Crabka realization

The mapping is honest about the divergence: where Pyroscope v2 pushes direct gRPC into a
segment-writer coordinated by a Raft metastore, Crabka inserts its Kafka WAL and uses the
`ProfileIndex` + KRaft in place of the metastore.

| Pyroscope component | Crabka realization |
|---|---|
| **Distributor** (pprof/OTLP push, validate, relabel, multi-value split, shard by labels) | `distributor` role — terminates the push doors, validates + relabels, splits multi-value pprof into one series per sample type, shards by labels → **WAL** (not direct gRPC) |
| **Segment-writer + direct gRPC + object-store DLQ** *(Pyroscope's diskless write path)* | **The Crabka broker — the Kafka WAL.** *The deliberate divergence.* The WAL topic replaces direct-gRPC + segment-writer + DLQ; durability is partition replication, not an object-store dead-letter queue |
| **Raft metastore** *(Pyroscope's only stateful component)* | **Gone — replaced by the blockstore `ProfileIndex` + Crabka KRaft.** Block discovery is the per-block `ProfileIndex`; cluster metadata/consensus is the broker's existing KRaft |
| **Block-builder** | `block-builder` role — consumes the WAL, builds the samples fact table + the deduplicated symbol DB, writes the block + `ProfileIndex` to object storage, commits offsets (write-then-commit, idempotent keys) |
| **Querier** | `querier` role — filter samples → DataFusion `GROUP BY (stacktrace_partition, stacktrace_id) → SUM(value)` → symbolize the surviving distinct ids via the symbol DB → fold into a flamegraph; UNION hot WAL-tail + cold blocks |
| **Query-frontend** | `query-frontend` role — split/shard queries; merge **partial symbolized trees** from queriers/blocks (raw ids never cross a block boundary) |
| **Compactor** | `compactor` role — merge/recompact profile blocks + dedup symbol DBs; downsampling |
| **Symbolizer** | `symbolizer` role — the final heavy slice: query-time `build_id → debuginfod` + DWARF/ELF/`.gopclntab` resolution for unsymbolized native/eBPF profiles |
| **Overrides / limits** | per-tenant config on Crabka's quota/ACL machinery |

### 3.2 The Kafka-WAL pipeline (the divergence, drawn)

```
  SDKs (/ingest)   Alloy pyroscope.write (push.v1)   OTel (v1development)      Grafana (built-in Pyroscope datasource)
  language agents  Connect Push                       OTLP profiles            │  Connect querier.v1 + legacy /render
        │  POST /ingest?...   │  /push.v1.PusherService/Push  │ /v1development/profiles   │
        ▼                     ▼                               ▼                          ▼
  ┌──────────────────────────────────────────────┐     ┌──────────────────────────────────────┐
  │                DISTRIBUTOR                     │     │                QUERIER                 │
  │ decode (pprof/JFR/OTLP) · relabel · require    │     │ querier.v1 + /pyroscope/render         │
  │ service_name+__name__ · multi-value split →    │     │ ProfileStore: hot WAL-tail ∪ cold      │
  │ ONE SERIES PER SAMPLE TYPE · shard by labels   │     │ DataFusion GROUP BY stacktrace → SUM   │
  └───────────────────────┬──────────────────────┘     │ → symdb resolve → fold → FlameGraph    │
                          │ produce                      └────────────────────┬──────────────────┘
                          ▼                                                   │ consume (cold blocks)
                  ┌───────────────┐                                           │
                  │   WAL TOPIC   │  __crabka_profiles_wal                     │
                  │ (Crabka       │  partition key = hash(tenant, series_fp)   │
                  │  broker, RF≥1)│ ───────────────── consume ─────────────────┤
                  └───────┬───────┘                                           │
                          │ (consumer group)                                  │
                          ▼                                                   ▼
                  ┌────────────────────────────────┐              ┌────────────────────────┐
                  │          BLOCK-BUILDER          │   object     │  ProfileIndex + KRaft  │
                  │ samples fact table (1 row/SAMPLE)│   _store     │ (replaces Pyroscope's  │
                  │ + DEDUP SYMBOL DB (~60% of size) │─────────────▶│  Raft metastore)       │
                  │ + ProfileIndex → commit offsets  │  S3/GCS      └────────────────────────┘
                  └────────────────────────────────┘
```

A single consumer group on the `series_fingerprint`-partitioned WAL drives the
block-builder; the querier reads cold blocks from object storage and unions the hot WAL
tail. No Raft metastore; no segment-writer; no object-store DLQ.

### 3.3 Crate layout

- `crabka-blockstore` *(shared with logs/metrics/traces; the `BlockIndex` trait already
  extracted by traces)* — slice 1 adds a **`ProfileIndex` (`impl BlockIndex`)** =
  label-series postings (**reuse the metrics `SeriesIndex` label-postings machinery**) + a
  **profile-type index** (`__profile_type__` → series) + per-block time-range + a
  **stacktrace-partition map**. Slice 1 also defines the **profile samples fact-table
  column constants + schema** and the **symbol-DB on-block artifact**. Existing
  `BlockStore`/`BlockWriter`/`BlockMeta`/`scan_context`, `Labels`/`LabelMatcher`/`MatchOp`,
  `COL_FINGERPRINT = "series_fingerprint"` (UInt64), `COL_TIMESTAMP = "timestamp"` (Int64)
  stay available.
- `crabka-pprof` *(slices 2–3)* — the **language-less** engine: the pprof model + codec,
  the `SymbolDb` (parent-pointer stacktrace tree + dedup tables), the `ProfileStore` query
  boundary, the `ProfileType` parser, and the flamegraph-merge / select-series / diff
  engine. **No query parser — there is no language.**
- `crabka-profiles` *(slices 4–8)* — the role-selectable service binary wiring blockstore +
  pprof + a Kafka client, plus the wire surfaces (the `push.v1` + `/ingest` + OTLP-profiles
  ingest doors, the Connect `querier.v1` API, the legacy `/pyroscope/render` flamebearer
  endpoints, and the query-time symbolizer).

### 3.4 Reuse vs net-new

**Reuse (from the codebase):** the `BlockIndex` trait + `BlockStore`/`BlockWriter`/
`scan_context` substrate from logs/metrics/traces; the **metrics `SeriesIndex`
label-postings** machinery (the `ProfileIndex`'s label dimension is exactly a series
postings index); the Connect-RPC server pattern from
[grpc-gateway/build.rs](crates/grpc-gateway/build.rs) (`connectrpc-axum-build` codegen,
system-`protoc`-with-vendored-fallback) and the `connectrpc-axum` serve patterns from
[grpc-gateway/src/serve.rs](crates/grpc-gateway/src/serve.rs) + the rebalancer service;
`object_store` 0.13; the DataFusion pin; Crabka's token-bucket quotas + ACLs for per-tenant
limits; consumer-group offsets for crash-safety; `serde` + `serde-wincode` for the WAL
record.

**Net-new:** the pprof model + codec, the `SymbolDb` (parent-pointer stacktrace tree + the
dedup string/function/location/mapping tables + the `symbols.symdb`-equivalent artifact),
the samples fact-table schema, the `ProfileIndex` (profile-type index + stacktrace-partition
map on top of the reused label postings), the flamegraph-merge engine (the
`Tree`/`FlameGraph`/`FlameGraphDiff` model + the 4-/7-ints-per-bar encodings), the
distributor's three push doors + multi-value split, the legacy flamebearer projection, and
the query-time native symbolizer (debuginfod + DWARF/ELF/`.gopclntab`).

## 4. Data model

A profile block is **tenant-scoped + time-bounded** and has two parts on object storage:
**(a)** a **flattened samples fact table** — *one row per SAMPLE* (a CRABKA choice; phlaredb
stores one row per profile with nested `Samples[]`, we flatten for a columnar
DataFusion-native fold) — and **(b)** a **deduplicated symbol DB** (the
`symbols.symdb`-equivalent; symbols are ~60% of a block's size, so dedup is the dominant
size lever). We are compatible with Pyroscope's semantics / profile-type strings / API,
**not** with the phlaredb byte format (greenfield — no block-format interop required).

### 4.1 Samples fact table (one row per sample)

Mandatory blockstore columns plus the profile payload:

| Column (constant) | Arrow type | Meaning |
|---|---|---|
| `COL_FINGERPRINT` (`series_fingerprint`) | `UInt64` | series identity (blockstore-mandatory; reuses the label-postings fingerprint) |
| `COL_TIMESTAMP` (`timestamp`) | `Int64` (ns) | sample time, nanos |
| `PCOL_PROFILE_TYPE` | `Dictionary<Utf8>` | the 5-part profile-type string (dict-encoded) |
| `PCOL_STACKTRACE_ID` | `UInt64` | leaf-node index into the symbol-DB partition's parent-pointer tree |
| `PCOL_VALUE` | `Int64` | the sample value for this profile type |
| `PCOL_STACKTRACE_PARTITION` | `UInt64` | which symbol-DB partition resolves this stacktrace id |
| `PCOL_TOTAL_VALUE` | `Int64` | precomputed per-profile total (powers SelectSeries without a re-fold) |
| `PCOL_SPAN_ID` | `UInt64` (nullable) | span association (span-scoped profiling) |
| `PCOL_TRACE_ID` | `Binary` (nullable) | trace association — the cross-signal join key |

The slot from `(stacktrace_partition, stacktrace_id)` into the symbol DB is *raw* — never
symbolized at rest. Symbolization happens at query time, after the cheap fold, and only for
the distinct surviving ids.

### 4.2 The symbol DB (the dedup lever)

Per **`UInt64` partition** (the `stacktrace_partition` value), a **parent-pointer stacktrace
tree** plus dedup tables. This is the `symbols.symdb`-equivalent artifact.

- **Stacktrace tree** — `node { parent: i32, location_ref: i32 }`. The `stacktrace_id` is the
  **leaf node index**. To resolve: climb parents from the leaf, collecting `location_ref`s,
  yielding the stack **leaf→root**. Identical stacks share the same path automatically (the
  intern step dedups). (Matches phlaredb symdb's `node{p int32 parent, r int32
  location-ref}`; the on-disk encoding is greenfield — we are not byte-compatible with
  symdb v3's `sym1` group-varint layout, only semantically equivalent.)
- **Dedup tables** (all string fields are indices into `strings`, with `strings[0] == ""`):
  - `locations(id, address, mapping_id, lines[] { function_id, line })` — multiple `lines[]`
    per location encode **inlined frames** (leaf-first / innermost-first).
  - `functions(id, name, system_name, filename, start_line)` — `name`/`system_name`/`filename`
    are string indices.
  - `mappings(id, memory_start, memory_limit, file_offset, filename, build_id, has_functions,
    has_filenames, has_line_numbers, has_inline_frames)` — `filename`/`build_id` are string
    indices; `has_functions == false` marks an **unsymbolized** mapping (native/eBPF) to be
    resolved at query time (§8).
  - `strings[]` with `strings[0] == ""`.

### 4.3 Profile types & labels

`profile_type` is the **5-part** string `name:sample_type:sample_unit:period_type:period_unit`
(with an optional `:delta` suffix where the source marks delta semantics). It is carried as
the `__profile_type__` label, and `name` is also the `__name__` label. **Do not hardcode the
set** — Go/pprof and Java/JFR differ. Verified examples (Go/pprof): `process_cpu:cpu:nanoseconds:cpu:nanoseconds`,
`memory:alloc_space:bytes:space:bytes`, `mutex:contentions:count:contentions:count`,
`goroutines:goroutine:count:goroutine:count`; Java/JFR differs (e.g.
`wall:wall:nanoseconds:wall:nanoseconds`).

**Multi-value split at ingest.** A pprof carries a list of `sample_type[]`; each sample's
`value[]` is element-wise aligned to it. At ingest the distributor splits a multi-value
pprof into **one series per sample type** (a Go heap profile yields 4 series:
alloc_objects/alloc_space/inuse_objects/inuse_space). This is exactly phlaredb's
`CreateProfileLabels` looping `sample_type`.

**Labels:** `__profile_type__`, `__name__`, `__period_type__`, `__period_unit__`,
`service_name` (inject `unknown_service` if empty), `__session_id__` (a per-agent session
identifier, **cardinality-capped via modulo-hash** at ingest), plus any user labels.

## 5. Ingest

The distributor terminates three push doors (the SDK door, the Alloy/Connect door, and the
experimental OTLP door) — **both `push.v1` and `/ingest` are mandatory; neither alone
suffices** (SDKs speak `/ingest`, Alloy's `pyroscope.write` speaks `push.v1`). All land in
the profiles WAL topic.

### 5.1 Push doors

- **Connect `push.v1.PusherService/Push`** (`POST /push.v1.PusherService/Push`) — the
  Alloy `pyroscope.write` door. `PushRequest { series[] RawProfileSeries { labels[]
  LabelPair @1, samples[] RawSample { raw_profile: bytes @1 /*gzipped pprof*/, ID: str @2 }
  @2 } @1 }` → `PushResponse {}` (**empty**). `__name__` is the metric name; `__profile_type__`
  is the 5-part string.
- **Legacy HTTP `POST /ingest`** — the language-SDK door.
  `?name=app{labels}&from&until&sampleRate(100)&spyName&units&aggregationType(sum)&format`.
  `format` ∈ `pprof`/`jfr`/`trie`/`tree`/`lines`/`speedscope`/`groups`; empty/unknown →
  `groups` (folded). Multipart `pprof`: the `profile` part + a `sample_type_config` JSON part
  (`{units, display-name, aggregation, cumulative, sampled}`). `jfr`: the `jfr` part + a
  `labels` part.
- **Experimental OTLP profiles** — `opentelemetry.proto.collector.profiles.v1development.ProfilesService/Export`
  at both the Connect path and the OTLP/HTTP path `/v1development/profiles`. Uses the
  interned `ProfilesDictionary { mapping_table, location_table, function_table, link_table,
  string_table, attribute_table, stack_table }`; `Sample { stack_index, attribute_indices,
  link_index, values[], timestamps_unix_nano[] }`; `Stack { location_indices }`. This proto
  **churns hard — pin a specific commit** and behavior-pin it with a round-trip test (do not
  fabricate field numbers; verify against the pinned rev).

### 5.2 Distributor pipeline (before the WAL append)

`relabel_configs` (Prometheus-style relabeling) → **require `service_name` + `__name__`**
(inject `unknown_service` when `service_name` is empty) → decode (pprof / JFR / OTLP) →
**multi-value split → one series per sample type** → enforce per-tenant limits
(`MaxLabelNamesPerSeries`, name/value length, `__session_id__` modulo-hash cardinality cap,
ingestion rate) → shard by labels → produce to the WAL topic. The push is ACKed **after** the
Kafka write is acknowledged.

### 5.3 WAL record & partitioning

`ProfileRecord` = tenant + the series `Labels` + `profile_type: String` + the decoded
payload (`samples [(stacktrace_location_refs, value, span_id, trace_id)]` + the profile's
**symbol set to merge into the block symbol DB**). Encoded via `serde` + `serde-wincode`
(workspace convention). The WAL topic is `__crabka_profiles_wal`; **partition key =
`hash(tenant, series_fingerprint)`** so a series' samples stay together and per-series order
is preserved (unlike traces, profiles need no `trace_id` co-location — there is no
cross-span structural join here).

### 5.4 Block-builder (consumer group)

Consumes the WAL, and over a flush window builds **(a)** the samples fact table (one row per
sample) and **(b)** a per-block `SymbolDb` by interning each record's symbol set
(deduplicating strings/functions/locations/mappings and the parent-pointer stacktrace
tree), then writes the block (via blockstore `BlockWriter`) + the symbol-DB artifact +
`ProfileIndex` updates → object storage → commits offsets. **Write-then-commit + deterministic
idempotent block keys** make a mid-flush crash re-do work, never lose or double-count it.

## 6. The flamegraph-merge engine (`crabka-pprof`)

The engine is the heart of this signal, because there is no language. The key choice is the
**DataFusion/Rust split**: DataFusion does the cheap, set-shrinking fold *before*
symbolization; Rust does the symbol-DB tree resolution + flamegraph fold *only* on the
distinct surviving ids.

### 6.1 MERGE → flamegraph (`SelectMergeStacktraces`)

1. Resolve the selector + profile type + time range via the `ProfileIndex` → candidate
   blocks (+ the hot WAL tail).
2. **DataFusion does the cheap part:** over the matched sample rows in `[start, end]`,
   `GROUP BY (stacktrace_partition, stacktrace_id) → SUM(value)`. This collapses millions of
   raw samples to the distinct surviving stacktrace ids **before any symbolization**
   (= phlaredb's `SampleAppender` summing by raw id per partition).
3. **Rust resolves only the distinct surviving ids** via the symbol-DB parent-pointer tree
   (`resolve(partition, id)` → `Vec<Frame>`, leaf-first, **inlined frames expanded** —
   multiple `Line[]` per location), and folds each `(frames, summed_value)` into one `Tree`:
   `total += value` along the whole root→leaf path, `self += value` only at the leaf.
4. **Encode** the `Tree` to a `FlameGraph { names[], levels[], total, max_self }`, truncated
   to `max_nodes` (default **2048**) with a synthetic `"other"` node for the pruned tail
   (a min-value heap threshold).

`FlameGraph.levels` is a list of `Level { values: Vec<i64> }`; each `Level`'s values are
traversed in **groups of 4**: `[xOffsetDelta, total, self, nameIndex]`, where `xOffsetDelta`
is the delta from the *previous bar's end* (not absolute), and `nameIndex` indexes `names[]`
(`names[0]` is the root, `"total"`). This 4-ints-per-bar encoding is a *contract* — it must
match byte-for-byte.

### 6.2 SELECT SERIES (`SelectSeries`)

Read the **precomputed `total_value` per profile** (the `PCOL_TOTAL_VALUE` column — no
re-fold needed), then DataFusion `GROUP BY group_by, floor(time/step) → SUM` (or `AVERAGE`).
**`step` is in SECONDS** (a `float64` on the wire). The result is `Series { labels,
points[] { value, timestamp_ms } }`. (Internally the engine uses `Series { labels:
Vec<(String,String)>, points: Vec<(i64, f64)> }` = `(timestamp_ms, value)`.)

### 6.3 DIFF (`Diff`)

Two MERGEs (`left` + `right`, each a `SelectMergeStacktracesRequest`) resolved
independently, aligned (zero-value placeholders so child sets match), then encoded as a
`FlameGraphDiff` whose `levels` are traversed in **groups of 7**:
`[xOffLeft, totalLeft, selfLeft, xOffRight, totalRight, selfRight, nameIndex]`, plus
`left_ticks` and `right_ticks`. (Internally `FlameGraphDiff { names, levels, left_ticks,
right_ticks }`.)

### 6.4 Cross-block correctness (raw ids never cross a boundary)

A `stacktrace_id` is only meaningful **within its own block's symbol DB**. Therefore each
block (and each querier replica) resolves **locally** → a **partial *symbolized* `Tree`**,
and the querier/query-frontend merges **partial trees** (`Tree::merge`) — never raw ids
across block boundaries. This is the load-bearing invariant of the distributed merge.

### 6.5 The `ProfileStore` boundary (the query seam)

`crabka-pprof` is storage-agnostic via an injected `ProfileStore`. The querier (slice 5)
implements it as the hot/cold UNION. Pinned public contract:

```rust
#[async_trait]
pub trait ProfileStore: Send + Sync {
    async fn select(&self, tenant: &str, profile_type: &str, matchers: &[LabelMatcher],
                    start_ms: i64, end_ms: i64) -> Result<ProfileScan, ProfileError>;
    async fn label_names(&self, tenant: &str, matchers: &[LabelMatcher],
                    start_ms: i64, end_ms: i64) -> Result<Vec<String>, ProfileError>;
    async fn label_values(&self, tenant: &str, name: &str, matchers: &[LabelMatcher],
                    start_ms: i64, end_ms: i64) -> Result<Vec<String>, ProfileError>;
    async fn profile_types(&self, tenant: &str, start_ms: i64, end_ms: i64)
                    -> Result<Vec<String>, ProfileError>;
    async fn series(&self, tenant: &str, matchers: &[LabelMatcher], label_names: &[String],
                    start_ms: i64, end_ms: i64) -> Result<Vec<Vec<(String,String)>>, ProfileError>;
}

// `samples_table` may be a UNION view of live WAL-tail (hot) + blocks (cold).
pub struct ProfileScan {
    pub ctx: datafusion::prelude::SessionContext,
    pub samples_table: String,
    pub symbols: std::sync::Arc<dyn SymbolSource>,
}

pub struct FlameEngine<S: ProfileStore> { /* store, opts */ }
pub struct EngineOpts { pub default_max_nodes: i64 /* 2048 */ }

impl<S: ProfileStore> FlameEngine<S> {
    pub fn new(store: std::sync::Arc<S>, opts: EngineOpts) -> Self;
    pub async fn select_merge_stacktraces(&self, tenant: &str, profile_type: &str,
                    label_selector: &str, start_ms: i64, end_ms: i64, max_nodes: i64)
                    -> Result<FlameGraph, ProfileError>;
    pub async fn select_series(&self, tenant: &str, profile_type: &str, label_selector: &str,
                    group_by: &[String], step_secs: f64, agg: SeriesAgg,
                    start_ms: i64, end_ms: i64) -> Result<Vec<Series>, ProfileError>;
    pub async fn diff(&self, tenant: &str, left: (&str, &str, i64, i64),
                    right: (&str, &str, i64, i64), max_nodes: i64)
                    -> Result<FlameGraphDiff, ProfileError>;
    pub async fn select_merge_profile(&self, tenant: &str, profile_type: &str,
                    label_selector: &str, start_ms: i64, end_ms: i64)
                    -> Result<Vec<u8> /* raw pprof */, ProfileError>;
}
```

Supporting types pinned in §3.3's crate contract: the pprof model (`PprofProfile`
decode/encode; internal `Frame { function: String, file: String, line: i32 }`); `SymbolDb`
(`intern_stacktrace(partition, &[u32]) -> u32`, `resolve(partition, id) -> Vec<Frame>`,
`encode()/decode()`) behind a `SymbolSource: Send + Sync` trait (impl by `SymbolDb` and by
the symbolizer wrapper); `ProfileType { name, sample_type, sample_unit, period_type,
period_unit }` with `parse(&str)` + `Display` (the 5-part colon form); `Tree`
(`add_stack(&[Frame], i64)`, `merge(Tree)`, `to_flamegraph(max_nodes) -> FlameGraph`);
`FlameGraph` / `Level` / `FlameGraphDiff`; `Series` / `SeriesAgg { Sum, Average }`;
`ProfileError { Decode, Plan, Exec, Store, Unsupported, Symbolize }`. **`label_selector` is a
Prometheus matcher STRING** parsed to `Vec<LabelMatcher>` by a small helper in `crabka-pprof`
(reusing blockstore `LabelMatcher`/`MatchOp`) — this is the closest thing to a "parser," and
it is just Prometheus label matching, not a profiles query language.

## 7. API surface

Two surfaces, both projections of the `crabka-pprof` engine + `ProfileStore` results. Tenant
via `X-Scope-OrgID`. The Connect API uses `POST /querier.v1.QuerierService/<Method>` with
`application/proto`; `start`/`end` are **unix MILLIS** (`int64`). Built via
`connectrpc-axum` + `connectrpc-axum-build` codegen reusing the grpc-gateway/rebalancer
pattern ([grpc-gateway/build.rs](crates/grpc-gateway/build.rs)).

### 7.1 Connect `querier.v1.QuerierService`

- **`ProfileTypes`** `{start, end}` → `{ profile_types[] ProfileType }`. **Also the datasource
  health probe** — Grafana's config-test hits this; **there is no separate `/ready`.**
- **`LabelNames`** `{matchers[], start, end}` → `{ names[] }`.
- **`LabelValues`** `{name, matchers[], start, end}` → `{ names[] }` (response field is
  **`names`**, *not* `values`).
- **`Series`** `{matchers[], label_names[], start, end}` → `{ labels_set }`.
- **`SelectMergeStacktraces`** `{profile_typeID, label_selector, start, end, max_nodes
  (default 2048), format (FLAMEGRAPH=1/TREE=2/DOT=3), stack_trace_selector, profile_id_selector}`
  → `{ flamegraph: FlameGraph, tree: bytes, dot: string }`.
- **`SelectMergeSpanProfile`** (+ `span_selector`) → `{ flamegraph, tree }`.
- **`SelectMergeProfile`** → `google.v1.Profile` (raw pprof).
- **`SelectSeries`** `{profile_typeID, label_selector, start, end, group_by[], step (SECONDS,
  float64), aggregation (SUM=0/AVERAGE=1), stack_trace_selector, limit, exemplar_type}` →
  `{ series[] }`.
- **`SelectHeatmap`**.
- **`Diff`** `{left, right}` (both `SelectMergeStacktracesRequest`) → `{ flamegraph:
  FlameGraphDiff }`.
- **`GetProfileStats`** → `{ data_ingested: bool, oldest_profile_time, newest_profile_time }`.
- **`AnalyzeQuery`**.

**The minimum surface Grafana's built-in Pyroscope datasource exercises** is
`ProfileTypes` + `LabelNames` + `LabelValues` + `SelectMergeStacktraces` + `SelectSeries`
(it does **not** call `Series` / `SelectMergeProfile` / `Diff` / `GetProfileStats` /
`AnalyzeQuery`). The flamegraph maps onto Grafana's nested-set dataframe (level/value/self/label).

### 7.2 Legacy flamebearer endpoints (the Profiles Drilldown app)

The Profiles Drilldown app (`grafana-pyroscope-app`) uses legacy HTTP, not Connect:

- **`GET /pyroscope/render`** — `?query=<profile_typeID>{selectors}&from&until&format=json|dot&maxNodes&groupBy`.
- **`GET /pyroscope/render-diff`** — server-side diff (flamebearer `"double"`, 7 ints/bar).

The flamebearer JSON is `{ flamebearer: { names[], levels[][], numTicks, maxSelf },
metadata: { format: "single" (4/bar) | "double" (7/bar), spyName, sampleRate, units, name } }`.

### 7.3 Ingest doors (served by the distributor role)

`POST /push.v1.PusherService/Push`, `POST /ingest`, and the OTLP-profiles
`/v1development/profiles` (+ Connect `ProfilesService/Export`) — all per §5.1.

## 8. Native symbolization (the heavy slice)

**Pre-symbolized sources** (SDK/pprof + Alloy-eBPF) arrive with `Function`/`Line` tables
populated and `Mapping.has_functions == true` — nothing to do.

**Unsymbolized sources** (OTel-native-eBPF) arrive as **File-ID + offset** (file-id =
SHA-256 of head+tail+size, truncated to 16 bytes) with `Mapping.has_functions == false`. The
block-builder stores the **raw addresses + `build_id`** when `has_functions == false`. The
backend symbolizes **lazily at query time** (skip ever-viewed-never), via:

1. `build_id` → **debuginfod** (default `https://debuginfod.elfutils.org/`), cached;
2. parse **ELF / DWARF / `.gopclntab`** and **demangle** to recover `function`/`file`/`line`;
3. expand inlined frames.

The symbolizer is wrapped behind the same `SymbolSource` trait the in-block `SymbolDb`
implements, so the engine resolves uniformly. Interpreted/JIT runtimes (Python/JVM/…) are
always pre-symbolized in-agent and need no backend resolution. **Honesty note:** OSS native
symbolization upstream is partial/evolving (Pyroscope issue #3715 — customer-code coverage is
incomplete; the default scope is system/OSS binaries). We implement the system/OSS path and
the lazy-resolve plumbing; broader customer-code symbolization (exec-upload + addr2line) is a
follow-on, not claimed here.

Dependencies (slice 7): `gimli` / `object` / `addr2line` (DWARF/ELF/`.gopclntab`) + a
debuginfod HTTP client (`reqwest`).

## 9. Error handling, limits, multi-tenancy

- **Multi-tenancy:** `X-Scope-OrgID` → tenant id → Crabka's topic namespace + ACL principal +
  quota entity. Block/index/symbol-DB object keys are tenant-prefixed; the WAL is
  `(tenant, series_fingerprint)`-partitioned within a tenant.
- **Per-tenant limits** on Crabka token-bucket quotas: max series, label-name/value limits,
  ingestion rate, `max_nodes`, query range, and the `__session_id__` cardinality cap
  (modulo-hash) → Pyroscope-shaped `4xx`/`429`.
- **Crash-safety:** block-builder consumer-group offsets + **write-then-commit** with
  deterministic idempotent block keys (write block + symbol DB + `ProfileIndex`, *then*
  commit offsets) — a crash between only re-does work. The querier's hot tier holds no durable
  state: it is **rebuildable from WAL offsets**.

## 10. Testing strategy

Mirrors Crabka's differential-testing ethos. Note the **absence** of a conformance corpus
(there is no profiles query language and no upstream `.test`-style corpus) — the headline is
the differential check, not a language conformance harness.

- **Differential vs. real Pyroscope** *(headline)* — push identical pprof/OTLP profiles into
  Pyroscope and Crabka (testcontainers), run a query corpus (ProfileTypes / LabelNames /
  LabelValues / SelectMergeStacktraces / SelectSeries / Diff) against both, assert equal
  results. The byte-equality analog that proves "drop-in."
- **FlameGraph encoding** — `xOffsetDelta` (delta-from-previous-bar-end, not absolute),
  4-ints-per-bar grouping, `names[0]` root, `max_nodes` truncation + synthetic `"other"`;
  the 7-ints-per-bar diff form; the flamebearer `"single"`/`"double"` projection.
- **Symbol-DB correctness** — `intern_stacktrace` dedup (identical stacks share a path),
  `resolve` climbs parents leaf→root, **inlined frames** expand (multiple `Line[]` per
  location, innermost-first), `encode`/`decode` round-trip.
- **Engine semantics** — fold-before-symbolize (`GROUP BY stacktrace_id → SUM` precedes
  resolution); `Tree` total-along-path / self-at-leaf; SelectSeries reads precomputed
  `total_value`, step in SECONDS, SUM vs AVERAGE; Diff alignment with zero-value placeholders;
  **cross-block partial-tree merge** (raw ids never cross a boundary).
- **Profile-type parsing** — the 5-part `name:sample_type:sample_unit:period_type:period_unit`
  parse + `Display`; Go/pprof vs Java/JFR examples; multi-value split → one series per sample
  type (Go heap → 4 series).
- **Ingest doors** — `push.v1` (gzipped-pprof `raw_profile`), `/ingest`
  (pprof/jfr/.../groups + multipart `sample_type_config`), OTLP `v1development` decode (pinned
  rev, behavior-pinning round-trip — not fabricated).
- **Block-builder crash-recovery** — kill mid-flush, restart, assert no loss / no dup /
  identical block + symbol-DB keys.
- **Hot/cold merge** — a query spanning the WAL-tail/block frontier counts each sample once.
- **Native symbolization** — `has_functions == false` → debuginfod (mocked) → DWARF/ELF
  resolve + demangle + inline expansion; lazy (never-viewed → never resolved); cache hit/miss.
- **Grafana integration** (testcontainers) — real Grafana, built-in Pyroscope datasource
  pointed at Crabka (config-test via `ProfileTypes`); drive Explore Profiles + the Profiles
  Drilldown app (`/pyroscope/render`).
- **Multi-tenant isolation** — tenant A cannot see B's profiles/labels/symbols; quotas
  enforced.

## 11. Scope & implementation slices

Full Pyroscope means scope-IN is everything above. The slices below are the plan's phasing;
each is independently testable and gets its own `writing-plans` plan when reached. Per-task
file sets are non-overlapping where noted, enabling parallel subagent batches.

1. **Blockstore `ProfileIndex` + profile samples schema + symbol-DB artifact** — add the
   `ProfileIndex` (`impl BlockIndex`) = label-series postings (reuse the metrics
   `SeriesIndex` label-postings) + profile-type index (`__profile_type__` → series) +
   per-block time-range + stacktrace-partition map. Define the samples fact-table column
   constants (`PCOL_*`) + schema and the symbol-DB on-block artifact.
2. **`crabka-pprof` core** — the pprof model + codec, the `SymbolDb` (parent-pointer tree +
   dedup tables + `encode`/`decode` + `SymbolSource`), the `ProfileType` parser, the
   `ProfileStore` trait + the pinned engine result types, and the **MERGE → flamegraph**
   engine (fold-before-symbolize, `Tree`, the 4-ints-per-bar `FlameGraph`). **No query
   parser — there is no language.**
3. **Engine completeness** — `SelectSeries` (precomputed `total_value`, step-in-seconds,
   SUM/AVERAGE), `Diff` (7-ints-per-bar `FlameGraphDiff`), `max_nodes` truncation + synthetic
   `"other"`, raw-profile output (`SelectMergeProfile` → pprof), and `SelectMergeSpanProfile` +
   `SelectHeatmap`.
4. **Ingest service** — the `distributor` (`push.v1` + `/ingest` + OTLP `v1development`
   profiles + `relabel_configs` + require `service_name`/`__name__` + multi-value split) →
   `(tenant, series_fingerprint)`-partitioned WAL; the `block-builder` consumer group → samples
   fact table + dedup symbol DB + `ProfileIndex` (write-then-commit, idempotent keys).
5. **Querier + Connect `querier.v1` API + legacy render** — implement `ProfileStore` as the
   hot/cold UNION (WAL tail + blocks); serve the Connect `querier.v1` methods (incl.
   `ProfileTypes` as the health probe — no `/ready`) + the legacy `/pyroscope/render` +
   `/pyroscope/render-diff` flamebearer endpoints.
6. **Query-frontend** — query split/shard + the **partial-tree merge** (each block/replica
   resolves locally → partial symbolized `Tree`; the frontend `Tree::merge`s — raw ids never
   cross a block boundary) + select-series shard-merge.
7. **Native symbolization** (the heavy slice) — query-time `build_id → debuginfod` + DWARF/ELF/
   `.gopclntab` parse + demangle + inline expansion, lazy resolve (skip never-viewed), behind
   the `SymbolSource` wrapper; `gimli`/`object`/`addr2line` + a debuginfod `reqwest` client.
8. **Hardening** — per-tenant limits + multi-tenancy isolation, compaction (dedup symbol DBs)
   + downsampling, the **differential-vs-Pyroscope** corpus, and **Grafana integration**
   (Pyroscope datasource + Profiles Drilldown end-to-end).

## 12. Relation to the four-signal vision

Profiles is the **fourth and final tenant** of `crabka-blockstore`, and it lands cleanly on
the seams the earlier signals carved:

- It **reuses the `BlockIndex` trait** that *traces* extracted — the `ProfileIndex` is just
  another `impl BlockIndex`, with a non-mandatory schema (the samples fact table, not a
  `series_fingerprint`+`timestamp`-only shape). The generalization the logs spec promised and
  traces forced now pays off a second time with no new seam.
- It **reuses the label-postings machinery** that *metrics* built — the `ProfileIndex`'s label
  dimension *is* a `SeriesIndex`-style postings index, with a profile-type index +
  stacktrace-partition map layered on top.
- `crabka-pprof` sits beside `crabka-logql`, `crabka-promql`, and `crabka-traceql` on the same
  DataFusion substrate and the same role-selectable service skeleton — but it is the **odd one
  out: a LANGUAGE-LESS engine.** Where the other three are "parser + planner + custom
  operators," profiles is "symbol-DB + flamegraph-merge." The shared claim still holds: each
  signal = one front-end + one wire-compatible API + one block schema, all on the shared
  substrate.

And it **completes LGTM+P.** `trace_id` (hex) remains the universal cross-signal join key:
profiles carry `PCOL_TRACE_ID`/`PCOL_SPAN_ID`, so a trace span links to the profile captured
during it, and Grafana correlates profiles↔traces↔logs↔metrics by `trace_id` at the datasource
layer.

| Signal | Front-end crate | API emulated | Block payload | Index impl |
|---|---|---|---|---|
| **Logs** | `crabka-logql` | Loki HTTP | `line`, metadata | `SeriesIndex` |
| **Metrics** | `crabka-promql` | Prometheus HTTP | float / native-hist / exemplar | `SeriesIndex` |
| **Traces** | `crabka-traceql` | Tempo HTTP | flattened span + nested-set | `TraceIndex` |
| **Profiles** | **`crabka-pprof`** *(language-less)* | **Pyroscope Connect + legacy** | **samples fact table + dedup symbol DB** | **`ProfileIndex`** |

## 13. Open questions for planning

- **The metastore-replacement boundary** — Pyroscope's Raft metastore tracks block
  membership/compaction state centrally; we spread that across the per-block `ProfileIndex` +
  KRaft. The open question is whether a *global* per-tenant block list needs its own small
  index (a compacted topic?) for fast block-discovery at query time, or whether
  time/label-prefiltered object-store listing suffices. Slice 5 should measure block-discovery
  latency before deciding.
- **Symbol-DB dedup scope** — per-block symbol DBs are simple but duplicate common
  system-library symbols across blocks; a per-tenant shared symbol DB would dedup harder but
  adds a cross-block dependency. The compactor (slice 8) vertical-merges block symbol DBs;
  whether to go further (tenant-global interning) is open — measure the dedup ratio first.
- **Flush-window vs. block count** — the block-builder window sets the
  completeness/latency/block-count balance; tune against compaction-merge cost under load.
- **`__session_id__` cardinality cap** — the modulo-hash bucket count trades session-level
  drill-down fidelity against series cardinality; pick a default and make it per-tenant
  configurable.
- **OTLP-profiles rev pin** — the `v1development` proto churns hard; pin a specific commit and
  schedule a revisit, with a behavior-pinning round-trip test (never fabricate field numbers).
- **Native-symbolization scope** — ship the system/OSS-binary path (debuginfod default) first;
  customer-code symbolization (exec-upload + addr2line, issue #3715) is a follow-on. Decide
  whether the `symbolizer` role caches resolved symbols back into object storage (a symbol-DB
  rewrite) or only in an in-memory/LRU query cache.
- **`SelectHeatmap` fidelity** — heatmap shape/semantics are less load-bearing for the Grafana
  minimum surface; confirm the exact response shape against the pinned Pyroscope tag before
  investing in slice 3.
