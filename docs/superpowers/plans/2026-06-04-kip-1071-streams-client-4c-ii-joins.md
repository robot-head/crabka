# KIP-1071 Streams Client #4c-ii — KStream-KTable join — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KStream::join`/`left_join` against a materialized `KTable` (inner + left), with copartition declaration in the wire topology, byte-exact vs JVM 4.1.

**Architecture:** The stream-side `KStreamKTableJoinProcessor` looks up the table's materialized store per record (`emit_on_miss` distinguishes inner/left). The wire topology gets a `copartition_group` (new builder `add_copartition_group` → `grouping.rs` → the existing `wire.rs::copartition_group` int16 encoder) and the join is connected to the table's store (new `connect_processor_store`) so they union into one subtopology. A captured `stream_table_join` golden frame pins the bytes.

**Tech Stack:** Rust 2024; extends #4 DSL + #4c-i `Change`. JVM capture via the Docker Kafka-Streams 4.1 harness.

**Spec:** `docs/superpowers/specs/2026-06-04-kip-1071-streams-client-4c-ii-joins-design.md`.
**Branch:** `streams-4c-joins` (stacked on `streams-4c-change`; worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`).

---

## Current shapes (verbatim)

- `topology/node.rs`: `pub(crate) struct StoreEntry { name, processors: Vec<String>, changelog_override: Option<String> }`; `NodeRegistry { …, pub stores: Vec<StoreEntry>, … }`; `add_store(name, processors, changelog_override)`.
- `topology/grouping.rs`: `pub(crate) struct GroupTopics { id, source_topics, repartition_source_topics, repartition_sink_topics, changelog_stores, … }` (NO `copartition_groups` field yet). The union-find groups nodes into subtopologies; topic sets are filled per group.
- `topology/wire.rs`: `pub(crate) fn copartition_group(sources: &[String], repartition: &[String], members: &[String]) -> CopartitionGroup` — maps each member topic name to its int16 index in the sorted `sources` / `repartition` arrays (already correct + unit-tested). `subtopology()` builds each wire `Subtopology` (currently `copartition_groups: Vec::new()`).
- `dsl/ktable.rs`: `pub struct KTable<K,V> { builder, node, store_name: Option<String>, _pd }`; `KTable::new(builder, node, store_name)`. NO source-topic field.
- `dsl/builder.rs` `table()`: records a `TableSource` node, registers the store with the source topic; returns `KTable::new(.., Some(store_name))`.
- `dsl/kstream.rs`: stateless ops + `to_table` + the lower-thunk pattern (`add_processor::<KIn,VIn,KOut,VOut,_,_,_>(name, supplier, [NodeHandle::<K,Vparent>::from_name(state.handle_name[&parent])])`). `key_changing` lineage bit on `KStream`. `KGroupedStream::record_repartition` is the repartition-lowering reference.
- `Topology::{add_processor, add_source, add_sink, add_state_store, add_state_store_no_changelog, add_repartition_topic}`. `get_state_store::<K,V>(name)` on `ProcessorContext`.

## File structure

```
dsl/processors/join.rs   NEW — KStreamKTableJoinProcessor
dsl/processors/mod.rs    + pub(crate) mod join;
dsl/kstream.rs           + join() / left_join()
dsl/ktable.rs            + source_topic field + pub(crate) store_name()/source_topic() accessors
dsl/builder.rs           table() threads source_topic into KTable
dsl/graph.rs             + a Join node kind (or reuse StatelessProcessor + a copartition aux)
dsl/lower.rs             (no change if thunks self-contained)
dsl/names.rs             + KSTREAM-JOIN- prefix const
topology/node.rs         + NodeRegistry.copartition_groups + add_copartition_group + connect_processor_store helper
topology/builder.rs      + Topology::add_copartition_group + connect_processor_store
topology/grouping.rs     + GroupTopics.copartition_groups (member-topic lists per subtopology)
topology/wire.rs         subtopology() populates copartition_groups via copartition_group()
tests/jvm-capture/.../Capture.java  + stream_table_join topology
tests/testdata/golden/dsl/stream_table_join.topology.json  NEW
tests/dsl_golden_frame.rs  + join golden
tests/dsl_execution.rs     + inner/left exec tests
lib.rs                     join doc note
```

**Batching:** sequential. Task 4's capture (`jvm-capture/` + `testdata`) is independent of the Rust tasks.

---

## Task 1: Copartition declaration → grouping → wire

**Files:** `topology/node.rs`, `topology/builder.rs`, `topology/grouping.rs`, `topology/wire.rs`.

- [ ] **Step 1: failing test** — in `topology/builder.rs` tests, build a 2-source topology with a copartition group and assert the wire emits it:
```rust
#[test]
fn copartition_group_emitted_in_wire() {
    use crate::processor::serde::{Consumed, Produced, BytesSerde};
    let mut t = Topology::new();
    let a = t.add_source("sa", ["left"], Consumed::with(BytesSerde, BytesSerde));
    let b = t.add_source("sb", ["right"], Consumed::with(BytesSerde, BytesSerde));
    // a processor consuming both so they're in one subtopology
    t.add_sink("snk", "out", [&a, &b], Produced::with(BytesSerde, BytesSerde));
    t.add_copartition_group(["left", "right"]);
    let wire = t.build("app").unwrap().to_wire();
    let sub = &wire.subtopologies[0];
    // sorted source_topics: ["left","right"] → indices [0,1]
    assert_eq!(sub.copartition_groups.len(), 1);
    assert_eq!(sub.copartition_groups[0].source_topics, vec![0i16, 1i16]);
}
```
(Confirm `add_sink` accepts multiple parents `[&a, &b]` — it takes `IntoIterator<Item=Borrow<NodeHandle>>`. Confirm the serde-mirror `WireSubtopology.copartition_groups[].source_topics` field path; adjust the assertion to the real wire-mirror shape — read `wire.rs`'s `WireCopartitionGroup`.)

- [ ] **Step 2: run → FAIL** (`add_copartition_group` missing; `copartition_groups` empty).

- [ ] **Step 3: implement.**
  1. `node.rs`: add `pub copartition_groups: Vec<Vec<String>>` to `NodeRegistry` (init empty); `pub fn add_copartition_group(&mut self, topics: Vec<String>) { self.copartition_groups.push(topics); }`.
  2. `builder.rs`: `pub fn add_copartition_group(&mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> &mut Self { self.reg.add_copartition_group(topics.into_iter().map(Into::into).collect()); self }`.
  3. `grouping.rs`: add `pub copartition_groups: Vec<Vec<String>>` to `GroupTopics`. After the per-group topic sets are filled, for each registered copartition group, find the subtopology whose `source_topics`/`repartition_source_topics` contain the group's member topics, and push the member list into that `GroupTopics.copartition_groups`. (A group's members are all in one subtopology by construction — the copartitioned join.)
  4. `wire.rs` `subtopology()`: replace `copartition_groups: Vec::new()` with: for each member list in the group's `copartition_groups`, call `copartition_group(&sorted_sources, &sorted_repartition_sources, members)` and collect. Use the SAME sorted arrays the function already builds for `source_topics`/`repartition_source_topics` (so indices line up). Add `copartition_groups` to the serde mirror `WireSubtopology` (mirror `WireCopartitionGroup{ source_topics: Vec<i16>, source_topic_regex: Vec<i16>, repartition_source_topics: Vec<i16> }`) with a `From` projection — check whether the mirror already has it (the #1 wire types may already include `copartition_groups`; the serde mirror added in #4-T5 may need the field).

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams): copartition group declaration → wire`.

---

## Task 2: connect_processor_store

**Files:** `topology/node.rs`, `topology/builder.rs`.

- [ ] **Step 1: failing test** — in `builder.rs` tests: a store created connected to processor P1; connect P2; assert both P1 and P2 are in the store's connected list (and — if easy — that grouping unions them). Minimal:
```rust
#[test]
fn connect_processor_store_adds_to_connected_list() {
    let mut t = Topology::new();
    // (build a topology with a store connected to one processor, then connect a second)
    // assert via reg or via the store's StoreEntry.processors containing both names.
}
```
(Adapt to the real builder API — you may assert through `to_wire()`'s changelog/subtopology placement, or expose a `#[cfg(test)]` accessor on the registry. Simplest: after `add_state_store("s", ks, vs, ["p1"])` then `connect_processor_store("p2", "s")`, the `NodeRegistry`'s `StoreEntry` for "s" has `processors == ["p1","p2"]`.)

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.**
  - `node.rs`: `pub fn connect_processor_store(&mut self, processor: &str, store: &str) { if let Some(e) = self.stores.iter_mut().find(|e| e.name == store) { if !e.processors.iter().any(|p| p == processor) { e.processors.push(processor.to_string()); } } }`.
  - `builder.rs`: `pub fn connect_processor_store(&mut self, processor: &str, store: &str) -> &mut Self { self.reg.connect_processor_store(processor, store); self }`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams): connect_processor_store (join↔table store)`.

---

## Task 3: Join processor + KTable accessors

**Files:** Create `dsl/processors/join.rs`; modify `dsl/processors/mod.rs`, `dsl/ktable.rs`, `dsl/builder.rs`, `dsl/names.rs`.

- [ ] **Step 1: failing test** — append to `dsl/processors/join.rs` a unit test driving the join processor against a `StoreRegistry` holding the table store (mirror `dsl/processors/aggregate.rs`'s test harness — `Dispatch{buffer,children,output,record_ctx,stores}` + `ProcessorContext::new`):
```rust
// inner: store has ("a",10); pipe ("a", 1) → forward joiner(1,10); pipe ("b",2) → no forward.
// left:  store has ("a",10); pipe ("a", 1) → forward joiner(1,Some(10)); pipe ("b",2) → forward joiner(2,None).
```
Concretely build `KStreamKTableJoinProcessor{ table_store:"t".into(), joiner: |v:&i64, vt:Option<&i64>| v + vt.copied().unwrap_or(0), emit_on_miss:<bool>, _pd:PhantomData }`, seed the store, run `process`, assert the forwarded value (downcast `i64`) and buffer length.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.**
  `dsl/processors/join.rs`:
```rust
use std::marker::PhantomData;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;
type Marker<T> = PhantomData<fn() -> T>;

#[allow(dead_code)]
pub(crate) struct KStreamKTableJoinProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    pub joiner: F,          // LEFT form: Fn(&V, Option<&VT>) -> VO
    pub emit_on_miss: bool, // false = inner, true = left
    pub _pd: Marker<(K, V, VT, VO)>,
}
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinProcessor<K, V, VT, VO, F>
where K: std::any::Any+Send+Clone, V: 'static, VT: 'static, VO: std::any::Any+Send+Clone,
      F: Fn(&V, Option<&VT>) -> VO + Send + 'static {
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, V>) {
        let key = r.key.expect("join key");
        let vt = ctx.get_state_store::<K, VT>(&self.table_store).and_then(|s| s.get(&key));
        if vt.is_some() || self.emit_on_miss {
            let out = (self.joiner)(&r.value, vt.as_ref());
            ctx.forward(Record::new(Some(key), out, r.timestamp));
        }
    }
}
```
  `dsl/processors/mod.rs`: `pub(crate) mod join;`.
  `dsl/names.rs`: `pub(crate) const JOIN: &str = "KSTREAM-JOIN-";` (verify vs fixture later; not wire-visible).
  `dsl/ktable.rs`: add `source_topic: Option<String>` to `KTable`; update `KTable::new` to take it (or add `KTable::new_with_source`); add `pub(crate) fn store_name(&self) -> Option<&str>` and `pub(crate) fn source_topic(&self) -> Option<&str>`. Update ALL `KTable::new(..)` call sites (in `builder.rs` `table()`, `kgrouped.rs` aggregate/count/reduce, `kstream.rs` `to_table`, `ktable.rs` map_values/filter/to_stream) to pass the source topic where known (`table()` → its source topic; others → `None`).
  `dsl/builder.rs` `table()`: thread the source topic into the returned `KTable`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): KStream-KTable join processor + KTable accessors`.

---

## Task 4: join/left_join DSL ops + capture + golden + execution

**Files:** `dsl/kstream.rs`, `dsl/graph.rs`; `tests/jvm-capture/.../Capture.java`, `tests/testdata/golden/dsl/stream_table_join.topology.json` (NEW), `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`.

- [ ] **Step 1: capture the JVM fixture FIRST.** Add a `stream_table_join` method to `Capture.java`:
  `builder.stream("left").join(builder.table("right", Materialized.as("store")), (v, vt) -> v + vt).to("out");` (String serdes, app id "app", optimization=all). Run `tests/jvm-capture/run.sh --gradle` (Docker Kafka-Streams 4.1; see README). Commit `tests/testdata/golden/dsl/stream_table_join.topology.json`. NOTE its shape: how many subtopologies; `source_topics` (sorted `["left","right"]`?); `copartition_groups` indices; `state_changelog_topics` (`app-store-changelog`). If Docker capture fails, report BLOCKED with the error — do NOT fabricate.

- [ ] **Step 2: failing golden test** — `tests/dsl_golden_frame.rs`:
```rust
#[test]
fn stream_table_join_matches_jvm() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let table = b.table::<String,String,_,_>("right", Consumed::with(StringSerde, StringSerde),
        Materialized::with(StringSerde, StringSerde).as_store("store"));
    b.stream(["left"], Consumed::with(StringSerde, StringSerde))
        .join(&table, |v: &String, vt: &String| format!("{v}{vt}"))
        .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "stream_table_join");
}
```
(`drop(table)` before `build` if `Rc::try_unwrap` needs it — match how other tests handle held handles.)

- [ ] **Step 3: implement** `KStream::join`/`left_join` in `dsl/kstream.rs`:
  - `join<VT,VO,F>(&self, table: &KTable<K,VT>, joiner: F) -> KStream<K,VO>` where `F: Fn(&K_unused?,&V,&VT)…` — actually per spec `F: Fn(&V,&VT)->VO + Clone+Send+'static`; wrap to left form `move |v, opt: Option<&VT>| joiner(v, opt.expect("inner join hit"))`, `emit_on_miss=false`.
  - `left_join<VT,VO,F>(&self, table, joiner)` where `F: Fn(&V, Option<&VT>)->VO + Clone+Send+'static`; `emit_on_miss=true`.
  - Both: get `table_store = table.store_name().expect("join requires a materialized table")`; `table_src = table.source_topic()` (for the copartition group). If `self.key_changing`, insert a repartition first (reuse `KGroupedStream::record_repartition`-style lowering; the repartitioned topic becomes the stream-side copartition member); else the stream's source topic is the member — thread the stream's source topic the same way the table's is (you may need a `source_topic` lineage bit on `KStream` too, OR derive it; for the no-key-change fixture the stream source is the `stream([...])` topic — add a `pub(crate) fn source_topic()` lineage to `KStream` mirroring KTable, set by `stream()`).
  - Mint `join_name = new_processor_name(names::JOIN)`; record a `Join` node (predecessor = stream/repartition node); thunk:
    `add_processor::<K, V, K, VO, _, _, _>(join_name, move || KStreamKTableJoinProcessor{ table_store, joiner: left_form.clone(), emit_on_miss, _pd }, [stream_parent])`
    `+ topology.connect_processor_store(&join_name, &table_store)`
    `+ topology.add_copartition_group([stream_member, table_src])`.
    Record `state.handle_name[join_id] = h.name`. Return `KStream<K,VO>`.
  - `dsl/graph.rs`: add a `Join` variant to `GraphNodeKind` if a distinct kind helps (or reuse `StatelessProcessor` — the lowering is thunk-driven, so a generic kind is fine; but a `Join` kind documents intent).
  Iterate against the `stream_table_join` fixture until byte-match (the copartition indices + subtopology placement are the byte-exact bits).

- [ ] **Step 4: execution tests** — `tests/dsl_execution.rs`:
  - `dsl_stream_table_inner_join_executes`: build the join topology via the DSL; `TopologyTestDriver`; pipe `right` records first (populate the store: `("k","T")`), then `left` records (`("k","S")` → output `"ST"`; `("x","S2")` with no table entry → NO output). Assert.
  - `dsl_stream_table_left_join_executes`: same but `left_join(|v, opt| format!("{v}{}", opt.cloned().unwrap_or_default()))`; `("x","S2")` → output `"S2"` (table miss → None).

- [ ] **Step 5: run → golden + exec PASS; the 6 prior goldens still byte-match; clippy; fmt; commit** `feat(streams-dsl): KStream-KTable join + golden`.

---

## Task 5: Docs + final verification

**Files:** `lib.rs`.

- [ ] **Step 1:** add a short `## Joins` note to `lib.rs` docs (a sentence: `KStream::join`/`left_join` against a materialized KTable; stream-side lookup; copartition required). Prose, no new doctest.
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` (join processor unit + copartition/connect unit + join golden + the 6 prior goldens byte-identical + inner/left exec + all #1/#2/#3/#4/#4c-i tests + doctests); `cargo fmt -p crabka-client-streams -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-dsl): join note + #4c-ii verification`.

---

## Self-review

**Spec coverage:** §3 join processor → Task 3. §4 connect_processor_store → Task 2. §5 copartition declaration+grouping+wire → Task 1. §6 DSL ops + lowering (+ KTable/KStream source-topic accessors, repartition-if-key-changed) → Tasks 3, 4. §7 capture + golden → Task 4. §8 testing → Tasks 1–5. §9 success criteria → Task 5. ✓

**Empirical-fixture note (not a placeholder):** Task 4's join subtopology placement + copartition indices + the `KSTREAM-JOIN-` counter are validated against the **captured** `stream_table_join` JVM fixture (Step 1 captures first), per the program's empirical-capture strategy. The byte-exact bits are pinned by the fixture, not guessed.

**Type consistency:** `add_copartition_group`/`connect_processor_store` (T1/T2) → `KStreamKTableJoinProcessor{table_store, joiner: Fn(&V,Option<&VT>)->VO, emit_on_miss}` + `KTable::source_topic()`/`store_name()` (T3) → `join`/`left_join` thunks call `add_processor` + `connect_processor_store` + `add_copartition_group` (T4). The joiner is always stored in the left form `Fn(&V,Option<&VT>)->VO`; inner wraps via `expect`. Consistent.

**Known risks:** (1) the stream-side copartition member when the key is unchanged needs the stream's source topic — add a `source_topic` lineage to `KStream` (set by `stream()`), mirroring the KTable accessor. (2) `KTable::new` gains a `source_topic` param → every call site must be updated (Task 3 lists them). (3) the join subtopology placement (one subtopology for the copartitioned join) is pinned by the fixture — iterate Task 4 against it.
