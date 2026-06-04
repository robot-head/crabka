# KIP-1071 Streams Client #4 (first slice) — Stateful DSL (KStream/KTable) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `dsl` module in `crabka-client-streams` providing a fluent KStream/KTable DSL (stateless ops + groupBy/count/reduce/aggregate + table) that compiles to the existing Processor-API `Topology`, reproducing the JVM 4.x DSL's auto-naming + optimizer (`MERGE_REPARTITION_TOPICS`, `REUSE_KTABLE_SOURCE_TOPICS`) byte-for-byte.

**Architecture:** DSL ops build a logical `GraphNode` DAG (JVM `InternalStreamsBuilder`-style, auto-named from a global counter at call time); `build_optimized()` runs the optimizer passes then *lowers* the DAG to the typed Processor-API builder (`add_source`/`add_processor`/`add_sink`/`add_state_store`/`add_repartition_topic`) → existing `to_wire()` + runtime. Byte-exactness is gated against empirically-captured JVM 4.x golden fixtures.

**Tech Stack:** Rust 2024; reuses `processor::serde` (`Consumed`/`Produced`/`Serde`), the `#3` `KeyValueStore`, and the `#2` runtime. JVM capture harness uses Kafka-Streams 4.x (Gradle + local JDK).

**Spec:** `docs/superpowers/specs/2026-06-04-kip-1071-streams-client-4-dsl-design.md`.
**Branch:** `worktree-streams-4-dsl` (off `main`).

---

## Existing Processor-API surface (verbatim — the DSL lowers to this)

- `Topology::add_source<K,V,KS,VS>(name: impl Into<String>, topics: impl IntoIterator<Item=impl Into<String>>, consumed: Consumed<KS,VS>) -> NodeHandle<K,V>` where `K,V: Any+Send+Clone`, `KS: Serde<K>+Clone`, `VS: Serde<V>+Clone`.
- `Topology::add_processor<KIn,VIn,KOut,VOut,S,P,I>(name, supplier: S, parents: I) -> NodeHandle<KOut,VOut>` where `S: ProcessorSupplier<KIn,VIn,KOut,VOut>+Clone`, `I: IntoIterator<Item=P>`, `P: Borrow<NodeHandle<KIn,VIn>>`. There are blanket `ProcessorSupplier` impls for `Fn()->P where P: Processor<..>` and for `Fn()->Box<dyn Processor<..>>` (from PR #383).
- `Topology::add_sink<K,V,KS,VS,P,I>(name, topic, parents: I, produced: Produced<KS,VS>)`.
- `Topology::add_state_store<K,V,KS,VS>(name, key_serde: KS, value_serde: VS, processors: impl IntoIterator<Item=impl Into<String>>) -> &mut Self`.
- `Topology::add_repartition_topic<S: Into<String>>(name: S) -> &mut Self`.
- `Topology::build<S: Into<String>>(app_id) -> Result<BuiltTopology, TopologyError>`; `BuiltTopology::to_wire() -> WireTopology`.
- `NodeHandle<K,V>` has a public `.name: String` (cloneable). `Processor<KIn,VIn,KOut,VOut>: Send+'static { fn init; fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,KOut,VOut>, record: Record<KIn,VIn>); fn close }`. `ProcessorContext::forward(Record<KOut,VOut>)`, `get_state_store::<K2,V2>(name) -> Option<&mut dyn KeyValueStore<K2,V2>>`.
- `processor::serde`: `Consumed<KS,VS>::with(ks,vs)` (fields `key_serde`,`value_serde`), `Produced<KS,VS>::with(ks,vs)`, `trait Serde<T>: Send+Sync+'static { fn serialize(&self,&T)->Bytes; fn deserialize(&self,&[u8])->Result<T,SerdeError> }`, `StringSerde`, `I64Serde`, `BytesSerde`.

---

## File structure

```
crates/client-streams/src/dsl/
  mod.rs                 re-exports
  config.rs              Grouped, Materialized, Repartitioned
  graph.rs               NodeId, GraphNode + GraphNodeKind + flags, LogicalGraph container
  builder.rs             InternalStreamsBuilder (counter+graph), StreamsBuilder (stream/table/build/build_optimized)
  kstream.rs             KStream<K,V> (4a stateless ops)
  kgrouped.rs            KGroupedStream<K,V> (count/reduce/aggregate)
  ktable.rs              KTable<K,V> (toStream/mapValues/filter)
  optimizer.rs           merge_repartition_topics + reuse_ktable_source_topics
  lower.rs               LogicalGraph -> Topology
  names.rs               JVM name-prefix constants
  processors/
    mod.rs
    stateless.rs         MapValues/Map/SelectKey/Filter/FlatMap/FlatMapValues/Peek/Foreach/Merge/Branch
    aggregate.rs         KStreamAggregate (count/reduce/aggregate)
    table.rs             KTableSource/KTableToStream/KTableMapValues/KTableFilter
crates/client-streams/src/lib.rs                          +pub mod dsl + re-exports + doc example
crates/client-streams/tests/dsl_golden_frame.rs           NEW golden tests
crates/client-streams/tests/dsl_execution.rs              NEW TopologyTestDriver exec tests
crates/client-streams/tests/dsl_integration.rs            NEW broker integration (count + restart-restore)
crates/client-streams/tests/testdata/golden/dsl/*.json    captured JVM fixtures
crates/client-streams/tests/jvm-capture/                  Gradle harness + README
```

**Batching for parallel execution:** all tasks touch the single `client-streams` crate and largely the new `dsl/` submodules. Run **sequentially** (shared `cargo` build + many tasks edit `dsl/mod.rs` / `lib.rs`). The capture harness (Task 3) is independent of the Rust tasks and *may* run in parallel with Task 4–5 if a separate agent owns only `tests/jvm-capture/` + `testdata/golden/dsl/`.

---

## Task 1: DSL module scaffold + config objects + name constants

**Files:** Create `dsl/mod.rs`, `dsl/config.rs`, `dsl/names.rs`; modify `lib.rs`.

- [ ] **Step 1: failing test** — append to `dsl/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

    #[test]
    fn grouped_materialized_repartitioned_carry_serdes_and_names() {
        let g = Grouped::with(StringSerde, I64Serde).with_name("g1");
        check!(g.name.as_deref() == Some("g1"));
        let m = Materialized::with(StringSerde, I64Serde).as_store("counts");
        check!(m.store_name.as_deref() == Some("counts"));
        check!(m.logging);
        let r = Repartitioned::with(StringSerde, I64Serde).with_name("rp").num_partitions(4);
        check!(r.name.as_deref() == Some("rp"));
        check!(r.partitions == Some(4));
    }
}
```

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib dsl::config`

- [ ] **Step 3: implement** `dsl/names.rs` (exact JVM 4.x prefixes — `org.apache.kafka.streams.kstream.internals.KStreamImpl`/`KTableImpl`/`KGroupedStreamImpl`):

```rust
//! JVM 4.x DSL node-name prefixes (ported verbatim). The auto-name is
//! `format!("{PREFIX}{index:010}")`; `index` increments at op-call time.
pub(crate) const SOURCE: &str = "KSTREAM-SOURCE-";
pub(crate) const SINK: &str = "KSTREAM-SINK-";
pub(crate) const FILTER: &str = "KSTREAM-FILTER-";
pub(crate) const MAPVALUES: &str = "KSTREAM-MAPVALUES-";
pub(crate) const MAP: &str = "KSTREAM-MAP-";
pub(crate) const KEY_SELECT: &str = "KSTREAM-KEY-SELECT-";
pub(crate) const FLATMAP: &str = "KSTREAM-FLATMAP-";
pub(crate) const FLATMAPVALUES: &str = "KSTREAM-FLATMAPVALUES-";
pub(crate) const PEEK: &str = "KSTREAM-PEEK-";
pub(crate) const FOREACH: &str = "KSTREAM-FOREACH-";
pub(crate) const MERGE: &str = "KSTREAM-MERGE-";
pub(crate) const BRANCH: &str = "KSTREAM-BRANCH-";
pub(crate) const BRANCHCHILD: &str = "KSTREAM-BRANCHCHILD-";
pub(crate) const AGGREGATE: &str = "KSTREAM-AGGREGATE-";
pub(crate) const REDUCE: &str = "KSTREAM-REDUCE-";
pub(crate) const AGGREGATE_STORE: &str = "KSTREAM-AGGREGATE-STATE-STORE-";
pub(crate) const REDUCE_STORE: &str = "KSTREAM-REDUCE-STATE-STORE-";
pub(crate) const TABLE_SOURCE: &str = "KTABLE-SOURCE-";
pub(crate) const TABLE_TOSTREAM: &str = "KTABLE-TOSTREAM-";
pub(crate) const TABLE_MAPVALUES: &str = "KTABLE-MAPVALUES-";
pub(crate) const TABLE_FILTER: &str = "KTABLE-FILTER-";
pub(crate) const REPARTITION_SUFFIX: &str = "-repartition";
```

Implement `dsl/config.rs`:

```rust
//! DSL config objects mirroring JVM `Grouped`/`Materialized`/`Repartitioned`.
//! `Consumed`/`Produced` are reused from `crate::processor::serde`.
use crate::processor::serde::Serde;

/// Serdes (+ optional repartition name) for `groupBy`/`groupByKey`.
pub struct Grouped<KS, VS> {
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) name: Option<String>,
}
impl<KS, VS> Grouped<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self { key_serde, value_serde, name: None }
    }
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// Store name + serdes + logging flag for a materialized KTable.
pub struct Materialized<KS, VS> {
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) store_name: Option<String>,
    pub(crate) logging: bool,
}
impl<KS, VS> Materialized<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self { key_serde, value_serde, store_name: None, logging: true }
    }
    #[must_use]
    pub fn as_store(mut self, name: impl Into<String>) -> Self {
        self.store_name = Some(name.into());
        self
    }
    #[must_use]
    pub fn with_logging(mut self, on: bool) -> Self {
        self.logging = on;
        self
    }
}

/// Serdes + optional name/partitions for an explicit `repartition()`.
pub struct Repartitioned<KS, VS> {
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) name: Option<String>,
    pub(crate) partitions: Option<i32>,
}
impl<KS, VS> Repartitioned<KS, VS> {
    pub fn with(key_serde: KS, value_serde: VS) -> Self {
        Self { key_serde, value_serde, name: None, partitions: None }
    }
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
    #[must_use]
    pub fn num_partitions(mut self, n: i32) -> Self {
        self.partitions = Some(n);
        self
    }
}

// Note: KS/VS are constrained to `Serde<_>` at the op call sites, not here.
#[allow(dead_code)]
fn _serde_bound_doc<K, V, KS: Serde<K>, VS: Serde<V>>() {}
```

`dsl/mod.rs`:

```rust
//! High-level KStream/KTable DSL (sub-project #4). Compiles to the Processor-API
//! `Topology` via a logical graph + optimizer + lowering.
pub mod config;
pub(crate) mod names;
pub use config::{Grouped, Materialized, Repartitioned};
```

`lib.rs`: add `pub mod dsl;` and re-export `pub use dsl::{Grouped, Materialized, Repartitioned};`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): config objects + JVM name prefixes`.

---

## Task 2: Logical graph model + name counter + StreamsBuilder shell

**Files:** Create `dsl/graph.rs`, `dsl/builder.rs`; modify `dsl/mod.rs`.

- [ ] **Step 1: failing test** — append to `dsl/builder.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::StringSerde;
    use assert2::check;

    #[test]
    fn counter_assigns_jvm_names_in_call_order() {
        let mut b = InternalStreamsBuilder::new();
        check!(b.new_processor_name(crate::dsl::names::SOURCE) == "KSTREAM-SOURCE-0000000000");
        check!(b.new_processor_name(crate::dsl::names::MAPVALUES) == "KSTREAM-MAPVALUES-0000000001");
        check!(b.new_processor_name(crate::dsl::names::FILTER) == "KSTREAM-FILTER-0000000002");
    }

    #[test]
    fn stream_records_a_source_node() {
        let builder = StreamsBuilder::new();
        let _s = builder.stream(["in"], crate::processor::serde::Consumed::with(StringSerde, StringSerde));
        let g = builder.internal.borrow();
        check!(g.graph.nodes.len() == 1);
        check!(matches!(g.graph.nodes[0].kind, GraphNodeKind::StreamSource { .. }));
        check!(g.graph.nodes[0].name == "KSTREAM-SOURCE-0000000000");
    }
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `dsl/graph.rs`:

```rust
//! Logical DSL graph: a `GraphNode` per JVM `GraphNode`, auto-named at build
//! time, optimized then lowered to the Processor-API `Topology`.
use std::any::Any;

pub(crate) type NodeId = usize;

/// A boxed `ProcessorSupplier`-producing thunk captured at op-call time. The
/// lowering calls it to attach the processor to the Processor-API builder.
/// Erased here because each op has different K/V types; the lowering closure
/// (built at op time) holds the typed `add_processor` call.
pub(crate) type LowerFn = Box<dyn FnOnce(&mut crate::topology::Topology, &LowerCtx) + Send>;

/// Per-node lowering context: the Processor-API `NodeHandle` names of this
/// node's already-lowered predecessors, plus the app id.
pub(crate) struct LowerCtx {
    pub app_id: String,
}

pub(crate) enum GraphNodeKind {
    StreamSource { topics: Vec<String> },
    StatelessProcessor { repartition_required: bool },
    StreamSink { topic: String },
    Repartition { topic: String, partitions: Option<i32> },
    Aggregate { store_name: String, changelog: bool },
    TableSource { topic: String, store_name: String, reuse_source_for_changelog: bool },
    TableProcessor { store_name: Option<String> },
}

pub(crate) struct GraphNode {
    pub id: NodeId,
    pub name: String,
    pub kind: GraphNodeKind,
    pub predecessors: Vec<NodeId>,
    pub children: Vec<NodeId>,
    // JVM optimizer flags:
    pub key_changing_operation: bool,
    pub merge_node: bool,
    /// The typed lowering thunk (None for source/sink which lower structurally).
    pub lower: Option<LowerFn>,
    /// Erased payload some passes inspect (e.g. repartition serdes). Boxed Any.
    pub aux: Option<Box<dyn Any + Send>>,
}

#[derive(Default)]
pub(crate) struct LogicalGraph {
    pub nodes: Vec<GraphNode>,
}

impl LogicalGraph {
    pub fn add(&mut self, name: String, kind: GraphNodeKind, predecessors: Vec<NodeId>) -> NodeId {
        let id = self.nodes.len();
        for &p in &predecessors {
            self.nodes[p].children.push(id);
        }
        self.nodes.push(GraphNode {
            id, name, kind, predecessors, children: Vec::new(),
            key_changing_operation: false, merge_node: false, lower: None, aux: None,
        });
        id
    }
}
```

Implement `dsl/builder.rs`:

```rust
//! `StreamsBuilder` (public) + `InternalStreamsBuilder` (graph + name counter).
use std::cell::RefCell;
use std::rc::Rc;

use crate::dsl::graph::{GraphNodeKind, LogicalGraph, NodeId};
use crate::processor::serde::{Consumed, Serde};

pub(crate) struct InternalStreamsBuilder {
    pub graph: LogicalGraph,
    index: usize,
}

impl InternalStreamsBuilder {
    pub fn new() -> Self {
        Self { graph: LogicalGraph::default(), index: 0 }
    }
    /// JVM `InternalStreamsBuilder.newProcessorName`: `prefix + %010d` then ++.
    pub fn new_processor_name(&mut self, prefix: &str) -> String {
        let n = format!("{prefix}{:010}", self.index);
        self.index += 1;
        n
    }
}

/// The DSL entry point. Build a topology, then `build(app_id)` /
/// `build_optimized(app_id)` to get an `Arc<BuiltTopology>` for the runtime.
pub struct StreamsBuilder {
    pub(crate) internal: Rc<RefCell<InternalStreamsBuilder>>,
}

impl StreamsBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { internal: Rc::new(RefCell::new(InternalStreamsBuilder::new())) }
    }

    /// Source a `KStream` from one or more topics.
    pub fn stream<K, V, KS, VS>(
        &self,
        topics: impl IntoIterator<Item = impl Into<String>>,
        consumed: Consumed<KS, VS>,
    ) -> crate::dsl::kstream::KStream<K, V>
    where
        K: std::any::Any + Send + Clone,
        V: std::any::Any + Send + Clone,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        let mut g = self.internal.borrow_mut();
        let name = g.new_processor_name(crate::dsl::names::SOURCE);
        let id = g.graph.add(name, GraphNodeKind::StreamSource { topics: topics.clone() }, vec![]);
        // store the consumed serdes for lowering (Task 5 wires the lower thunk)
        drop(g);
        crate::dsl::kstream::KStream::new(Rc::clone(&self.internal), id, consumed_lower(consumed, topics))
    }
}

impl Default for StreamsBuilder {
    fn default() -> Self { Self::new() }
}

// Placeholder hook the source lowering uses; Task 5 replaces with the real
// source lower thunk (add_source with the consumed serdes).
fn consumed_lower<K, V, KS: Serde<K> + Clone, VS: Serde<V> + Clone>(
    _consumed: Consumed<KS, VS>,
    _topics: Vec<String>,
) -> () {}
```

NOTE: the `consumed_lower` placeholder + the `KStream::new` signature are refined in Task 4/5 once `KStream` and the lowering exist. For Task 2, make `KStream` a minimal stub (`dsl/kstream.rs` with `pub struct KStream<K,V>{ builder, node, _pd }` + `new`) so this compiles; Task 4 fleshes it out. Add `pub(crate) mod graph; pub(crate) mod builder; pub mod kstream;` + `pub use builder::StreamsBuilder;` to `dsl/mod.rs`; re-export `StreamsBuilder` from `lib.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): logical graph + name counter + StreamsBuilder`.

---

## Task 3: JVM capture harness + capture the 5 golden fixtures (ground truth)

**Files:** Create `tests/jvm-capture/` (Gradle project + `README.md`), `tests/testdata/golden/dsl/*.topology.json`.

This establishes the byte-exact ground truth BEFORE the lowering is implemented, so the golden tests (Tasks 5/8/9/10/11) assert against real JVM output.

- [ ] **Step 1: build the harness.** Create a minimal Gradle Kafka-Streams 4.x app at `tests/jvm-capture/` with a `build.gradle` depending on `org.apache.kafka:kafka-streams:4.0.0` (match the cp-kafka version Crabka targets) and a `Capture.java` that, for each of the 5 named topologies below, sets:

```java
props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
props.put("group.protocol", "streams");
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
```

and captures the `StreamsGroupHeartbeatRequest.Topology`. Easiest capture path: construct the `Topology`, call `new TopologyMetadata(...)` or reflect `StreamsGroupHeartbeatRequestManager.buildRequestData()` against a throwaway broker; OR point the app at a Crabka broker with request-byte logging and grab the apiKey-88 frame. Serialize each captured `Topology` to JSON matching the field shape of the existing `tests/testdata/golden/dsl/../single_source_sink.topology.json` (subtopology ids as strings; topic arrays in the JVM's emitted order; include `state_changelog_topics`, `repartition_source_topics`, `repartition_sink_topics`, `copartition_groups`).

The 5 topologies (build each with the JVM DSL):
1. `stateless_chain` — `stream("in") .mapValues(v->v) .filter((k,v)->true) .to("out")`.
2. `count` — `stream("in").selectKey((k,v)->k).groupByKey().count().toStream().to("out")`.
3. `repartition_merge` — `stream("in").selectKey((k,v)->k)` then BOTH `.groupByKey().count()` and `.groupByKey().reduce((a,b)->a)` (two aggregations off one selectKey).
4. `table_reuse` — `table("in", Materialized.as("store")).mapValues(v->v).toStream().to("out")`.
5. `branch_merge` — `stream("in").split().branch((k,v)->true).branch((k,v)->false)` then merge the branches and `.to("out")`.

- [ ] **Step 2: capture + commit fixtures.** Run the harness; commit `tests/testdata/golden/dsl/{stateless_chain,count,repartition_merge,table_reuse,branch_merge}.topology.json`. Update `tests/testdata/golden/README.md` (or a new `dsl/README.md`) documenting the capture procedure + `optimization=all`.

- [ ] **Step 3: BLOCKED fallback.** If no JDK/Gradle is available in the environment, capture is not possible here: commit the harness source + README, and (a) report BLOCKED with the missing tooling, OR (b) per the user's empirical-capture decision, request the fixtures be captured out-of-band. Do NOT hand-fabricate fixtures and call them "captured." (Hand-derived interim frames are acceptable ONLY if explicitly labeled `*.hand-derived.json` and flagged.)

- [ ] **Step 4: commit** `test(streams-dsl): JVM 4.x DSL capture harness + golden fixtures`.

---

## Task 4: KStream stateless ops (record logical nodes)

**Files:** Modify `dsl/kstream.rs`, `dsl/graph.rs`; create `dsl/processors/mod.rs`, `dsl/processors/stateless.rs`.

- [ ] **Step 1: failing test** — append to `dsl/kstream.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::builder::StreamsBuilder;
    use crate::processor::serde::{Consumed, Produced, StringSerde};
    use assert2::check;

    #[test]
    fn stateless_chain_records_named_nodes() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .map_values(|v: &String| v.to_uppercase())
            .filter(|_k: &String, _v: &String| true)
            .to("out", Produced::with(StringSerde, StringSerde));
        let g = b.internal.borrow();
        let names: Vec<&str> = g.graph.nodes.iter().map(|n| n.name.as_str()).collect();
        check!(names == vec![
            "KSTREAM-SOURCE-0000000000",
            "KSTREAM-MAPVALUES-0000000001",
            "KSTREAM-FILTER-0000000002",
            "KSTREAM-SINK-0000000003",
        ]);
    }

    #[test]
    fn select_key_marks_key_changing() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .select_key(|_k: &String, v: &String| v.clone());
        let g = b.internal.borrow();
        check!(g.graph.nodes[1].key_changing_operation);
    }
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.** `dsl/processors/stateless.rs` — generic `Processor` impls (each wraps a `Clone+Send` closure). Example (the rest follow the same shape):

```rust
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

pub(crate) struct MapValuesProcessor<V, V2, F> {
    pub f: F,
    pub _pd: std::marker::PhantomData<fn(V) -> V2>,
}
impl<K, V, V2, F> Processor<K, V, K, V2> for MapValuesProcessor<V, V2, F>
where
    K: Send + 'static,
    V: 'static,
    V2: Send + 'static,
    F: Fn(&V) -> V2 + Send + 'static,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, V2>, r: Record<K, V>) {
        ctx.forward(Record::new(r.key, (self.f)(&r.value), r.timestamp));
    }
}
```

Add `FilterProcessor{ predicate, negate }`, `MapProcessor{f}`, `SelectKeyProcessor{f}`, `FlatMapProcessor{f}` (forward each), `FlatMapValuesProcessor{f}`, `PeekProcessor{f}` (side effect then forward `r`), `ForeachProcessor{f}` (no forward), `MergeProcessor` (forward `r` unchanged). All `#[derive(Clone)]`-able (closures are `Clone`). `dsl/processors/mod.rs`: `pub(crate) mod stateless;`.

`dsl/kstream.rs` — the handle + ops. Each op borrows the builder, mints a name, adds a node with the right `kind`/`key_changing_operation`, stores a **lower thunk** (built here, capturing the typed closure + the predecessor node id; Task 5 consumes it), returns a new `KStream`:

```rust
use std::cell::RefCell;
use std::rc::Rc;
use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::{GraphNodeKind, NodeId};

pub struct KStream<K, V> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) node: NodeId,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> KStream<K, V>
where
    K: std::any::Any + Send + Clone,
    V: std::any::Any + Send + Clone,
{
    pub(crate) fn new(builder: Rc<RefCell<InternalStreamsBuilder>>, node: NodeId) -> Self {
        Self { builder, node, _pd: std::marker::PhantomData }
    }

    #[must_use]
    pub fn map_values<V2, F>(&self, f: F) -> KStream<K, V2>
    where
        V2: std::any::Any + Send + Clone,
        F: Fn(&V) -> V2 + Clone + Send + 'static,
    {
        let id = {
            let mut g = self.builder.borrow_mut();
            let name = g.new_processor_name(crate::dsl::names::MAPVALUES);
            g.graph.add(name, GraphNodeKind::StatelessProcessor { repartition_required: false }, vec![self.node])
            // Task 5 attaches the lower thunk: add_processor(name, move || MapValuesProcessor{f}, [parent_handle])
        };
        KStream::new(Rc::clone(&self.builder), id)
    }
    // ... filter/filter_not/map/select_key/flat_map/flat_map_values/peek/foreach/to/merge/repartition
}
```

For `map`/`select_key`/`groupBy`: set `key_changing_operation = true` on the node. `to(topic, Produced)` adds a `StreamSink` node (terminal, returns `()`). `merge(other)` adds a `Merge` node with `predecessors = [self.node, other.node]` and `merge_node = true`. `repartition(Repartitioned)` adds a `Repartition` node.

The **lower thunks**: since each op knows its K/V/closure, store a boxed `FnOnce(&mut Topology, &predecessor_handle_names) -> NodeHandle-name`. Implementation detail resolved in Task 5; for Task 4, record the node + flags + keep the closure in the node's `aux`/`lower` (boxed). Keep Task 4 to graph-recording + naming; assert names/flags only.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): KStream stateless ops record named logical nodes`.

---

## Task 5: Lowering (stateless) + build + first golden frame

**Files:** Create `dsl/lower.rs`; modify `dsl/builder.rs`, `dsl/kstream.rs`; create `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: failing test** — `tests/dsl_golden_frame.rs`:

```rust
#![cfg(not(target_os = "windows"))]
use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{Consumed, Produced, StringSerde};

/// Compare the DSL's wire Topology (field-for-field) against the captured JVM fixture.
fn assert_matches_fixture(wire: &crabka_client_streams::topology::WireTopology, fixture: &str) {
    let expected: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(format!(
            "tests/testdata/golden/dsl/{fixture}.topology.json")).unwrap()).unwrap();
    let actual = serde_json::to_value(wire).unwrap();
    assert_eq!(actual, expected, "wire topology != JVM fixture {fixture}");
}

#[test]
fn stateless_chain_matches_jvm() {
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .map_values(|v: &String| v.clone())
        .filter(|_k: &String, _v: &String| true)
        .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stateless_chain");
}
```

(Requires `WireTopology: Serialize` — add `#[derive(serde::Serialize)]` to the wire type if absent, gated so it doesn't change the encoder. If the fixture JSON shape differs from `serde_json::to_value(wire)`, write a small field-mapping the test uses, mirroring how `single_source_sink` is asserted in `golden_frame.rs` — prefer field-by-field assertions over raw JSON if simpler.)

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `dsl/lower.rs` — walk the (un-optimized for now) graph in source-first BFS order; for each node, call the typed lowering thunk that op-time stored, threading the predecessor `NodeHandle`s by name. Provide `lower(graph, app_id) -> Topology`. Wire `StreamsBuilder::build(app_id)` = lower (no optimizer) → `Topology::build(app_id)`; `build_optimized(app_id)` (Task 9/10) runs the optimizer first.

The lowering thunk approach: each `KStream` op, when it creates its node, also pushes a `LowerFn` into the node that calls the matching `Topology::add_*` with the op's closure + name, and records the resulting `NodeHandle` name in a `HashMap<NodeId,String>` the lowering threads. Source nodes lower via the `Consumed` captured in Task 2's `stream()` (move it into the node's `aux`/lower thunk now). Sink nodes lower via `add_sink`.

- [ ] **Step 4: run → PASS (vs fixture); clippy; fmt; commit** `feat(streams-dsl): lower stateless graph + StreamsBuilder::build + golden frame`.

---

## Task 6: Aggregate + table processors (execution)

**Files:** Create `dsl/processors/aggregate.rs`, `dsl/processors/table.rs`; modify `dsl/processors/mod.rs`.

- [ ] **Step 1: failing test** — append to `dsl/processors/aggregate.rs` a unit test driving the processor directly with a fake store, asserting count accumulation. (Reuse the #3 store-in-a-`StoreRegistry` test pattern from `processor/graph.rs` tests — build a `Dispatch` with a `StoreRegistry` holding an `InMemoryKeyValueStore::<String,i64>`, run `process` twice, assert forwarded value is 2 and the store holds 2.)

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `dsl/processors/aggregate.rs`:

```rust
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Generic aggregator over a #3 KeyValueStore. count = init `||0`, agg `|_,_,a|a+1`;
/// reduce = init `|| <first value>`, agg `|_,v,a| reducer(&a,&v)`.
pub(crate) struct KStreamAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub init: I,
    pub agg: A,
    pub _pd: std::marker::PhantomData<fn(K, V) -> VA>,
}
impl<K, V, VA, I, A> Processor<K, V, K, VA> for KStreamAggregateProcessor<K, V, VA, I, A>
where
    K: Clone + Send + 'static,
    V: 'static,
    VA: Clone + Send + 'static,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VA>, r: Record<K, V>) {
        let store = ctx.get_state_store::<K, VA>(&self.store_name).expect("agg store");
        let old = store.get(&r.key).unwrap_or_else(|| (self.init)());
        let new = (self.agg)(&r.key, &r.value, old);
        store.put(r.key.clone(), new.clone());
        ctx.forward(Record::new(r.key, new, r.timestamp));
    }
}
```

`dsl/processors/table.rs`: `KTableSourceProcessor<K,V>{store_name}` (put each record into the store + forward), `KTableToStreamProcessor` (forward `r`), `KTableMapValuesProcessor<V,V2,F>{f, store_name}`, `KTableFilterProcessor<K,V,P>{predicate, store_name}` (forward `r` when matching; forward `Record::new(k, tombstone)` when a previously-present key stops matching — read prior store state). Mark `mod.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): aggregate + table processors over #3 stores`.

---

## Task 7: KGroupedStream + KTable DSL ops

**Files:** Create `dsl/kgrouped.rs`, `dsl/ktable.rs`; modify `dsl/kstream.rs`, `dsl/builder.rs`, `dsl/mod.rs`.

- [ ] **Step 1: failing test** — append to `dsl/kgrouped.rs`:

```rust
#[cfg(test)]
mod tests {
    use crate::dsl::builder::StreamsBuilder;
    use crate::processor::serde::{Consumed, Grouped, I64Serde, StringSerde};
    use assert2::check;

    #[test]
    fn count_records_aggregate_node_and_store() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .group_by_key(Grouped::with(StringSerde, StringSerde))
            .count(crate::dsl::Materialized::with(StringSerde, I64Serde).as_store("counts"));
        let g = b.internal.borrow();
        check!(g.graph.nodes.iter().any(|n|
            matches!(&n.kind, crate::dsl::graph::GraphNodeKind::Aggregate { store_name, .. } if store_name == "counts")));
    }
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.** `KStream::group_by_key(Grouped) -> KGroupedStream` (no repartition if key unchanged; if a preceding op set `key_changing_operation`, insert a `Repartition` node first). `KStream::group_by(f, Grouped)` = `select_key(f)` + `group_by_key`. `KGroupedStream::count(Materialized)`, `reduce(f, Materialized)`, `aggregate(init, agg, Materialized)` each add an `Aggregate` node (name from `AGGREGATE`/`REDUCE`), set its `store_name` (from `Materialized::store_name` or auto `KSTREAM-AGGREGATE-STATE-STORE-<id>`), attach the lower thunk (add_processor + add_state_store), and return a `KTable<K,VA>`. `StreamsBuilder::table(topic, Consumed, Materialized) -> KTable` adds a `TableSource` node. `KTable::to_stream()`, `map_values(f, Materialized)`, `filter(p, Materialized)`, `KTable::to_stream().to(..)`. Re-export `KGroupedStream`, `KTable` from `dsl/mod.rs` + `lib.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): KGroupedStream + KTable ops`.

---

## Task 8: Lower stateful nodes + execution test + count golden frame

**Files:** Modify `dsl/lower.rs`; create `tests/dsl_execution.rs`; modify `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: failing tests.** (a) `tests/dsl_execution.rs` — a counting topology via `TopologyTestDriver`: pipe `"a","a","b"`, assert outputs `1,2,1` and `get_key_value_store("counts")` holds `a=2,b=1`. (b) add to `dsl_golden_frame.rs`: `count_matches_jvm` building topology #2 and asserting vs the `count` fixture.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** lowering for `Aggregate`/`TableSource`/`TableProcessor`/`Repartition` nodes: `Aggregate` → `add_processor(name, agg_supplier, [parent]) ; add_state_store(store_name, ks, vs, [name])`. `Repartition` → `add_repartition_topic(topic)` + the JVM repartition node set (sink to repartition topic, source from it; match the captured fixture's exact node names/order — iterate against the fixture). `TableSource` → `add_source` + `add_processor(KTABLE-SOURCE)` + `add_state_store`. Thread predecessor handles by name.

- [ ] **Step 4: run → PASS (exec + count fixture); clippy; fmt; commit** `feat(streams-dsl): lower stateful nodes + count execution + golden`.

---

## Task 9: Optimizer — MERGE_REPARTITION_TOPICS

**Files:** Create `dsl/optimizer.rs`; modify `dsl/builder.rs`, `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: failing test.** Unit test in `optimizer.rs`: build the logical graph for topology #3 (selectKey → {count, reduce}); run `merge_repartition_topics`; assert the two repartition nodes collapsed to one (shared topic). Plus `repartition_merge_matches_jvm` golden test (build via `build_optimized("app")`, assert vs `repartition_merge` fixture).

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `dsl/optimizer.rs::merge_repartition_topics(graph)` — port of `InternalStreamsBuilder.maybeOptimizeRepartitionOperations`: group repartition nodes by their nearest key-changing ancestor; for each group with >1, keep one repartition topic (lowest-numbered name) and rewire the others' children to it; drop the redundant repartition nodes. `StreamsBuilder::build_optimized(app_id)` runs this (+ Task 10's pass) before lowering. Iterate the node shaping until the golden matches the fixture.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): MERGE_REPARTITION_TOPICS optimizer pass`.

---

## Task 10: Optimizer — REUSE_KTABLE_SOURCE_TOPICS

**Files:** Modify `dsl/optimizer.rs`, `dsl/lower.rs`, `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: failing test.** Unit test: build topology #4 (table → mapValues → toStream → to); run `reuse_ktable_source_topics`; assert the table store's changelog == the source topic (no separate `app-store-changelog`) + the store is marked non-creating. Plus `table_reuse_matches_jvm` golden test vs the `table_reuse` fixture.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `dsl/optimizer.rs::reuse_ktable_source_topics(graph)` — port of `maybeReuseSourceTopicForChangelog`: for each `TableSource` whose source topic can serve as the changelog, set `reuse_source_for_changelog = true` and point the store's changelog at the source topic; lowering then omits the separate changelog topic (the broker won't auto-create one). Iterate vs the fixture.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): REUSE_KTABLE_SOURCE_TOPICS optimizer pass`.

---

## Task 11: split/branch + merge + branch golden frame

**Files:** Modify `dsl/kstream.rs`, `dsl/processors/stateless.rs`, `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: failing test.** `branch_merge_matches_jvm` golden test (build topology #5 via the DSL `split()/branch()/...` + `merge`), assert vs the `branch_merge` fixture. Plus an execution test (records route to the right branch).

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `KStream::split() -> BranchedKStream`; `BranchedKStream::branch(predicate) -> KStream` (adds a `KSTREAM-BRANCH-`/`KSTREAM-BRANCHCHILD-` node pair per the JVM order — pin against the fixture); `BranchProcessor` routes by predicate to the matching child index. `merge` already exists (Task 4). Iterate node naming/order vs the `branch_merge` fixture.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): split/branch + branch golden frame`.

---

## Task 12: Broker integration (DSL count + restart-restore)

**Files:** Create `tests/dsl_integration.rs`.

- [ ] **Step 1: integration test** (`#![cfg(not(target_os = "windows"))]`, reuse the #3 `boot`/`finalize_streams_version`/`create_topic` helpers): a counting DSL `KafkaStreams` app over `dsl-in`→`dsl-out` with a `counts` store. Produce `["a","a","b"]`; assert outputs `a→1,a→2,b→1`. Then restart-restore: close, start a fresh instance same `application_id`, produce another `"a"`, assert output `3` (restored from changelog).

- [ ] **Step 2: run → PASS** (`cargo test -p crabka-client-streams --test dsl_integration -- --nocapture`). Debug real failures; do not weaken.

- [ ] **Step 3: commit** `test(streams-dsl): DSL count + restart-restore broker integration`.

---

## Task 13: Docs + final verification

**Files:** Modify `lib.rs`.

- [ ] **Step 1:** add a `## DSL` doc section to `lib.rs` — a counting DSL app (`StreamsBuilder::new().stream(..).group_by_key(..).count(..).to_stream().to(..)`) tested via `TopologyTestDriver` + `get_key_value_store` (runnable doctest).
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` (dsl unit + optimizer + golden + execution + integration + doctests + existing #1/#2/#3 tests + the #1 golden frame); `cargo fmt -p crabka-client-streams -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-dsl): DSL example + #4 verification`.

---

## Self-review

**Spec coverage:** §3.1 layout → Tasks 1,2,4,6,7,9,10. §3.2 handle model → Task 2/4. §3.3 config → Task 1. §4.1 graph → Task 2. §4.2 counter+prefixes → Tasks 1,2. §4.3 optimizer → Tasks 9,10. §4.4 lowering → Tasks 5,8. §5 execution → Tasks 6,7,8,12. §6 capture+golden → Tasks 3,5,8,9,10,11. §7 testing → Tasks 5,8,9,10,11,12,13. §9 success criteria → Task 13. ✓

**Empirical-fixture note (not a placeholder):** the exact byte shapes for repartition node sets, `writeToTopology` order, and branch naming are intentionally validated against the **captured JVM fixtures** (Task 3), not hardcoded here — per the spec's "empirical JVM capture" decision and §10 open points. Tasks 5/8/9/10/11 each follow a capture→implement→match→iterate loop against a committed fixture. This is the agreed ground-truth strategy, flagged explicitly.

**Type consistency:** `StreamsBuilder`/`InternalStreamsBuilder`/`new_processor_name` (T2) → `KStream::{map_values,filter,select_key,to,merge,group_by_key,...}` (T4/T7) → `KGroupedStream::{count,reduce,aggregate}` / `KTable::{to_stream,map_values,filter}` (T7) → `Grouped`/`Materialized`/`Repartitioned` (T1) → `build`/`build_optimized` (T5/T9/T10) → processors `MapValuesProcessor`/`KStreamAggregateProcessor`/`KTableSourceProcessor` (T4/T6). Names consistent.

**Risk:** Task 3 (JVM capture) is the critical-path dependency + the only task needing JDK/Gradle; if blocked, the byte-exact golden tasks can't be fully validated (the algorithm still ports, but ground truth is missing). Sequence Task 3 early; if BLOCKED, escalate before investing in Tasks 5–11's byte-exact iteration.
