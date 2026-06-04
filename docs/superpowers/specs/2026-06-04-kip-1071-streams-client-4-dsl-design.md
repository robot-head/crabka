# KIP-1071 Streams Client — Sub-project #4 (first slice): Stateful DSL (KStream/KTable)

**Date:** 2026-06-04
**Status:** Design approved, pending spec review
**Scope:** The fourth sub-project of the Crabka Streams client-runtime program — first slice.
**Builds on:** #1 (membership + byte-exact topology builder), #2 (Processor API + broker runtime), #3 (state stores + changelog — merged). Roadmap: `2026-06-03-kip-1071-streams-client-membership-design.md` §1.

## 1. Context

#1–#3 delivered the **Processor-API** layer: a typed `Topology` builder that serializes
byte-for-byte to the JVM 4.x `StreamsGroupHeartbeat.Topology` wire shape (#1), a
`Processor`/`ProcessorContext` execution engine + broker-backed `KafkaStreams`
runtime (#2), and in-memory `KeyValueStore` state stores with changelog
produce/restore (#3). Node names in that layer are **caller-provided**.

#4 adds the **high-level DSL** (`KStream`/`KTable`) that most Kafka Streams apps
are written against. The DSL's defining challenge is **interop byte-exactness**:
the JVM DSL auto-generates node names from a global counter
(`KSTREAM-SOURCE-0000000000`, …) and runs an **optimizer** over the logical graph
before building the physical topology. To share a streams group with a JVM DSL
app, a Crabka DSL app must reproduce those names + optimizer rewrites exactly.

This spec covers the **first slice** of #4: the **stateless `KStream` DSL (4a)**
plus **`KTable` + aggregations (4b)**, targeting the **full optimizer**
(`optimization=all`), validated against **empirically captured JVM 4.x fixtures**.
Joins (4c) and windowing (4d) are deferred to later slices.

## 2. Goal and non-goals

### Goal

A `dsl` module in `crabka-client-streams` that lets a Rust app write a fluent
KStream/KTable topology which **compiles to the existing Processor-API `Topology`**
(and thus runs on the #2/#3 runtime unchanged), producing a wire `Topology`
**byte-identical to the JVM 4.x DSL** (with `optimization=all`) for the supported
operator set.

Concretely:

1. **Stateless `KStream` (4a):** `stream`, `mapValues`, `map`, `selectKey`,
   `filter`, `filterNot`, `flatMap`, `flatMapValues`, `peek`, `foreach`, `to`,
   `repartition`, `merge`, `split`/`branch`.
2. **`KTable` + aggregations (4b):** `groupByKey`/`groupBy` → `KGroupedStream` →
   `count`/`reduce`/`aggregate` → `KTable`; `table()`; `KTable::toStream`,
   `mapValues`, `filter`. Materialized over #3's `KeyValueStore` + changelog.
3. **Optimizer (`optimization=all`):** `MERGE_REPARTITION_TOPICS` and
   `REUSE_KTABLE_SOURCE_TOPICS` (the only passes that apply without joins/windows).
4. **Execution:** count/reduce/aggregate run correctly via the existing runtime.

### Non-goals (deferred)

- **Joins** — KStream-KTable, KTable-KTable, windowed KStream-KStream → **#4c**.
  This includes full `Change<old,new>` KTable propagation (needed for KTable-KTable
  correctness); the first slice forwards materialized new values only.
- **Windowing** — time/session windows + window/session stores (#3 deferred these) → **#4d**.
- **Record caching / suppression** (KIP-328), **foreign-key joins** — later.
- **Interactive queries** over DSL stores → #6. **EOS** → #7.
- **Optimizer passes** beyond the two in scope — others (self-join, etc.) require
  joins/windowing.

## 3. Architecture (Approach A: logical graph → optimizer → lower)

The DSL is a **topology-construction layer only**. It builds a logical `GraphNode`
DAG mirroring the JVM `InternalStreamsBuilder`, runs the optimizer over it, then
**lowers** the optimized DAG to the typed Processor-API `Topology`
(`add_source`/`add_processor`/`add_sink`/`add_state_store`/`add_repartition_topic`).
The existing grouping + `to_wire()` + runtime do the rest, so **execution comes for
free**.

### 3.1 Module layout (`crates/client-streams/src/dsl/`)

```
dsl/
  mod.rs          re-exports (StreamsBuilder, KStream, KTable, KGroupedStream, Grouped, Materialized, Repartitioned)
  builder.rs      StreamsBuilder + InternalStreamsBuilder (name counter, graph root, build/build_optimized)
  graph.rs        logical GraphNode model + edges + optimizer flags
  kstream.rs      KStream<K,V> + stateless ops (4a)
  kgrouped.rs     KGroupedStream<K,V> → count/reduce/aggregate (4b)
  ktable.rs       KTable<K,V> + toStream/mapValues/filter (4b)
  config.rs       Grouped, Materialized, Repartitioned (Consumed/Produced reused from processor::serde)
  optimizer.rs    MERGE_REPARTITION_TOPICS + REUSE_KTABLE_SOURCE_TOPICS
  lower.rs        optimized DAG → Processor-API Topology
  processors/
    mod.rs
    stateless.rs  MapValues/Map/SelectKey/Filter/FlatMap/FlatMapValues/Peek/Foreach/Merge/Branch
    aggregate.rs  KStreamAggregate (count/reduce/aggregate)
    table.rs      KTableSource, KTableToStream, KTableMapValues, KTableFilter
```

`lib.rs` adds `pub mod dsl;` + re-exports. The DSL is **additive** — it does not
change the Processor-API layer, so #1's golden frame stays green.

### 3.2 Handle model

`KStream<K,V>`, `KGroupedStream<K,V>`, `KTable<K,V>` are lightweight typed handles,
each holding a shared `Rc<RefCell<InternalStreamsBuilder>>` + the logical `NodeId`
they emit from + `PhantomData<fn() -> (K, V)>`. Each op records a new `GraphNode`
(auto-named at call time) as a child of its source node and returns a new handle.
Construction is single-threaded; `build()` produces the same `Arc<BuiltTopology>`
the runtime already consumes (so the DSL feeds `KafkaStreams::builder().topology(..)`).

DSL closures lower into generic `Processor` impls, so they carry the Processor-API
bounds: `Fn(..) + Clone + Send + 'static`. Key/value types are `K, V: 'static`
(serde-able at the source/sink/repartition/materialization boundaries via the
config objects, which carry `Serde` impls — same model as #3).

### 3.3 Config objects (`dsl/config.rs`)

Reuse `Consumed<KS,VS>` (source serdes) and `Produced<KS,VS>` (sink serdes) from
`processor::serde`. Add:

- `Grouped<KS,VS>` — `with(ks, vs)` + optional `name(n)` (names the repartition
  topic for `groupBy`).
- `Materialized<KS,VS>` — `as(store_name)` and/or `with(ks, vs)`, plus a
  `with_logging(bool)` flag (caching deferred). Drives the #3 store + changelog.
- `Repartitioned<KS,VS>` — `with(ks, vs)` + optional `name(n)` + `num_partitions(n)`
  for explicit `repartition()`.

## 4. Auto-naming, logical graph & optimizer (the byte-exact core)

### 4.1 Logical GraphNode model (`dsl/graph.rs`)

A node per JVM `GraphNode` kind needed for 4a+4b:

- `StreamSource { topics, consumed }` (from `stream()`).
- `StatelessProcessor { kind, supplier }` — `kind ∈ {MapValues, Map, SelectKey,
  Filter{negate}, FlatMap, FlatMapValues, Peek, Foreach, Merge, Branch}`.
- `StreamSink { topic, produced }` (from `to()`).
- `Repartition { topic_name, serdes, num_partitions }` (from `repartition()`/`groupBy`).
- `Aggregate { kind: Count|Reduce|Aggregate, supplier, store }` (from `KGroupedStream`).
- `TableSource { topic, consumed, store, reuse_source_for_changelog }` (from `table()`).
- `TableProcessor { kind: ToStream|MapValues|Filter, supplier, store? }`.

Each node carries: `id: NodeId`, `name: String` (assigned at construction),
`predecessors: Vec<NodeId>`, `children: Vec<NodeId>`, and the JVM optimizer flags
used to decide rewrites: `key_changing_operation`, `repartition_required`,
`value_changing`, `merge_node`, `mergeable`.

### 4.2 Auto-naming counter

`InternalStreamsBuilder` holds a single `index: usize`. Names are produced by
`new_processor_name(prefix) = format!("{prefix}{index:010}")`, incrementing `index`
**at op-call time** (before the optimizer runs — matching the JVM, where the
optimizer may drop nodes but never renumbers). The prefix constants are ported
verbatim from JVM 4.x (`KStreamImpl`/`KTableImpl`/`KGroupedStreamImpl`):

| Op | Prefix |
|---|---|
| source | `KSTREAM-SOURCE-` |
| sink (`to`) | `KSTREAM-SINK-` |
| `filter`/`filterNot` | `KSTREAM-FILTER-` |
| `mapValues` | `KSTREAM-MAPVALUES-` |
| `map` | `KSTREAM-MAP-` |
| `selectKey` (and `map`'s key-select) | `KSTREAM-KEY-SELECT-` |
| `flatMap` | `KSTREAM-FLATMAP-` |
| `flatMapValues` | `KSTREAM-FLATMAPVALUES-` |
| `peek` | `KSTREAM-PEEK-` |
| `foreach` | `KSTREAM-FOREACH-` |
| `merge` | `KSTREAM-MERGE-` |
| `split`/`branch` | `KSTREAM-BRANCH-` / `KSTREAM-BRANCHCHILD-` |
| `count`/`aggregate` | `KSTREAM-AGGREGATE-` |
| `reduce` | `KSTREAM-REDUCE-` |
| aggregate store | `KSTREAM-AGGREGATE-STATE-STORE-` |
| reduce store | `KSTREAM-REDUCE-STATE-STORE-` |
| `table()` source | `KTABLE-SOURCE-` |
| `KTable::toStream` | `KTABLE-TOSTREAM-` |
| `KTable::mapValues` | `KTABLE-MAPVALUES-` |
| `KTable::filter` | `KTABLE-FILTER-` |
| repartition topic | `<base>-repartition` (base = `Grouped`/`Repartitioned` name or the key-select node name) |

The exact prefix strings + the increment order are the byte-exactness crux. The
empirically captured JVM fixtures (§6) are what confirm each is correct — any
mismatch is caught by the golden-frame diff.

### 4.3 Optimizer (`dsl/optimizer.rs`)

Run by `build_optimized(app_id)` over the logical DAG before lowering:

1. **`MERGE_REPARTITION_TOPICS`** — port of the JVM
   `InternalStreamsBuilder.maybeOptimizeRepartitionOperations`: when a single
   key-changing operation (`selectKey`/`map`/`groupBy`) feeds multiple downstream
   operations that each require a repartition, collapse them into **one** shared
   repartition topic (instead of one per downstream). Also folds a `selectKey`
   immediately followed by `groupByKey` into a single repartition.
2. **`REUSE_KTABLE_SOURCE_TOPICS`** — port of
   `maybeReuseSourceTopicForChangelog`: a `table()` materialized store uses its
   **source topic as the changelog** (no separate `<app>-<store>-changelog`), and
   the store is marked non-creating (the broker won't auto-create a changelog).

`build(app_id)` (no optimization, JVM default `NO_OPTIMIZATION`) skips both passes —
the straight lowering of the logical DAG. Both passes operate on the DAG only;
names already assigned are preserved (merged repartition nodes drop the redundant
siblings, keeping the lowest-numbered name, as the JVM does).

### 4.4 Lowering (`dsl/lower.rs`)

Walk the optimized DAG in the JVM `writeToTopology` order — BFS from source nodes,
with insertion-order (`buildPriority`) as the tiebreak — and emit the corresponding
pre-named Processor-API call for each node:

- `StreamSource` → `add_source(name, topics, Consumed)`.
- `StatelessProcessor` → `add_processor(name, supplier, [predecessor])`.
- `StreamSink` → `add_sink(name, topic, [predecessor], Produced)`.
- `Repartition` → `add_repartition_topic(topic)` + the sink/source(/filter)
  processors the JVM emits for a repartition.
- `Aggregate` → `add_processor(name, aggregate_supplier, [predecessor])` +
  `add_state_store(store_name, ks, vs, [name])`.
- `TableSource` → `add_source` + `add_processor(KTABLE-SOURCE, ..)` +
  `add_state_store` (changelog = source topic when reuse is on).
- `TableProcessor` → `add_processor` (+ `add_state_store` when materialized).

Then the existing `build()` → grouping → `to_wire()` produces the wire bytes, and
the existing runtime executes it.

## 5. Execution: DSL ops → Processor impls (`dsl/processors/`)

Lowering supplies each node an executable `ProcessorSupplier`; the DSL adds **no
new erasure machinery** — these are ordinary typed `Processor` impls over the #2a
erased graph.

### 5.1 Stateless (4a)

Generic wrappers around the user's `Fn + Clone + Send + 'static`:

- `MapValuesProcessor<K,V,V2>{ f }`: `forward(Record::new(k, (self.f)(&v), ts))`.
- `MapProcessor<K,V,K2,V2>{ f }`, `SelectKeyProcessor<K,V,K2>{ f }`.
- `FilterProcessor<K,V>{ predicate, negate }`: forward when `predicate(&k,&v) != negate`.
- `FlatMapProcessor`, `FlatMapValuesProcessor` (forward each produced record).
- `PeekProcessor` (side effect, then forward unchanged), `ForeachProcessor`
  (terminal, no forward).
- `MergeProcessor` (pass-through node with N predecessors).
- `BranchProcessor` (routes to a child index by predicate; `split`/`branch`).

### 5.2 Stateful (4b) over #3's `KeyValueStore`

The store is instantiated by lowering via `add_state_store` with the `Materialized`
serdes; changelog produce/restore come free from #3.

- `KStreamAggregateProcessor<K,V,VA>{ initializer, aggregator, store_name }`:
  `old = store.get(&k).unwrap_or_else(initializer); new = aggregator(&k,&v,old);
  store.put(k.clone(), new.clone()); forward(Record::new(k, new, ts))`.
  - **count** = `initializer: || 0i64`, `aggregator: |_k,_v,acc| acc + 1`.
  - **reduce** = `initializer: || first_value`, `aggregator: |_k,v,acc| reducer(&acc,&v)`.
- `KTableSourceProcessor<K,V>`: `table()` materializes each source record into the
  store (`store.put`) and forwards downstream. With `REUSE_KTABLE_SOURCE_TOPICS`
  the store's changelog **is** the source topic.
- `KTableToStreamProcessor`: forwards store updates as a stream (`toStream`).
- `KTableMapValuesProcessor`, `KTableFilterProcessor`: materialized KTable
  transforms; `filter` emits a tombstone (`None` value) when a row stops matching
  (reads prior store state to decide).

**KTable value semantics:** the first slice forwards the materialized **new value**
per update (sufficient for `groupBy→count→toStream→to` and
`table()→mapValues/filter→toStream`). Full JVM `Change<old,new>` propagation
(needed for KTable-KTable joins) is deferred to **#4c** and noted as such.

## 6. JVM capture harness & golden-frame validation

### 6.1 Capture harness (`crates/client-streams/tests/jvm-capture/`)

A minimal Kafka-Streams **4.x** app (Gradle, run with the local JDK + Kafka
harness) that, for each representative topology, builds the DSL with
`StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG = "all"` and `group.protocol=streams`,
and captures the real `StreamsGroupHeartbeatRequest.Topology` (apiKey 88) — by
pointing at a throwaway broker with request-byte logging or by reflecting
`StreamsGroupHeartbeatRequestManager.buildRequestData()`. Each captured `Topology`
is serialized to `crates/client-streams/tests/testdata/golden/dsl/<name>.topology.json`
(same field-for-field JSON shape as the existing `single_source_sink.topology.json`).
A `README.md` documents the procedure + the optimization config used (mirrors the
existing golden README). This is a **manual/offline capture step** run once per
fixture; **CI never runs the JVM** — it byte-compares the Rust DSL output against
the committed fixtures. Capturing the fixtures is a deliverable of this slice (it
requires the local JDK+Kafka harness; topology capture is control-plane only, so it
works even where data replication does not).

### 6.2 Golden-frame tests (`tests/dsl_golden_frame.rs`)

One fixture per representative topology; each asserts
`builder.build_optimized("app").to_wire()` equals the committed JVM fixture
(field-for-field):

1. **Stateless chain:** `stream → mapValues → filter → to`.
2. **Key-change + aggregation:** `stream → selectKey → groupByKey → count → toStream → to`.
3. **Repartition merge:** `stream → selectKey → {count, reduce}` (two aggregations
   sharing one repartition topic — exercises `MERGE_REPARTITION_TOPICS`).
4. **Table source reuse:** `table(topic, Materialized) → mapValues → toStream → to`
   (exercises `REUSE_KTABLE_SOURCE_TOPICS` — store changelog = source topic).
5. **Branch/merge:** `stream → split/branch(p) → merge → to`.

## 7. Testing strategy (gates)

1. **DSL unit tests** — each operator records the correct logical node + auto-name;
   the name counter advances in the right order; `Grouped`/`Materialized` thread
   serdes correctly.
2. **Optimizer unit tests** — before/after-DAG assertions for each pass (repartition
   merge collapses N→1; table reuse rewires the changelog to the source topic).
3. **Golden-frame tests** (§6.2) — byte-exact wire `Topology` vs JVM fixtures
   (optimized). A `none`-path test covers un-optimized lowering.
4. **`TopologyTestDriver` execution tests** — a counting/reducing DSL app: pipe
   inputs, assert outputs reflect accumulated state, inspect the store via
   `get_key_value_store`.
5. **Broker integration** — a counting `KafkaStreams` DSL app end-to-end (produce →
   assert output counts), plus restart-restore (fresh instance restores from the
   changelog — reuses #3).
6. **Regression** — the existing #1 Processor-API golden frame stays green (DSL does
   not change the physical layer); #2/#3 runtime tests stay green.

## 8. Errors & edge cases

- DSL misuse that the type system can't catch (e.g. an empty topology, a sink with
  no predecessor) surfaces as the existing `TopologyError` from `build()`.
- `groupBy`/`map`/`selectKey` set `key_changing_operation` → a repartition is
  inserted before the next stateful op (JVM-faithful); `groupByKey` on an unchanged
  key inserts none.
- A `Materialized` without an explicit name gets the auto-generated store name
  (`KSTREAM-AGGREGATE-STATE-STORE-<id>`); with a name, the user's name is used (and
  the changelog is `<app>-<name>-changelog`).

## 9. Success criteria

- The KStream/KTable operator set (§2) compiles to **byte-exact wire topologies
  matching committed JVM 4.x fixtures** (optimize=all) for the 5 representative
  topologies (§6.2).
- `count`/`reduce`/`aggregate` execute correctly via `TopologyTestDriver` and a
  broker integration test (incl. restart-restore via #3's changelog).
- `cargo test -p crabka-client-streams` green (DSL unit + optimizer + golden +
  test-driver + integration + doctests); the existing #1 Processor-API golden frame
  still green; `cargo clippy --workspace --all-targets -- -D warnings` and
  `cargo fmt --check` clean; `cargo build --workspace` builds.
- The JVM capture harness + committed DSL fixtures + README are in place.
- A documented DSL example in `lib.rs` (a counting app tested via
  `TopologyTestDriver`).

## 10. Open points for the plan

- **Exact repartition node shape.** The JVM emits a specific set of nodes for a
  repartition (a sink to the repartition topic, then a source from it, sometimes a
  filter for null keys). The plan must reproduce the exact node names + order; the
  fixtures are the ground truth — capture topology #2/#3 early to pin this down.
- **`writeToTopology` order.** Confirm the BFS/insertion-priority order the JVM uses
  to write nodes, since it affects subtopology grouping order. Validate against the
  fixtures.
- **`split`/`branch` naming.** The 4.x `split().branch(..).defaultBranch()` API
  assigns `KSTREAM-BRANCH-`/`KSTREAM-BRANCHCHILD-` names in a specific order;
  capture a branch fixture to pin it.
- **`reduce` initializer.** JVM `reduce` has no explicit initializer (first value
  seeds the store). Confirm the store-seeding behavior matches (no spurious
  initializer record).
