# KIP-1071 Streams Client — Sub-project #4c-ii: KStream-KTable join

**Date:** 2026-06-04
**Status:** Design approved, pending spec review
**Scope:** Second sub-slice of the joins+windowing program — the KStream-KTable join (inner + left).
**Builds on:** #4 DSL (merged, `main` e4472cd8) + #4c-i `Change` semantics (PR #388). Branch `streams-4c-joins` (stacked on `streams-4c-change`).

## 1. Context & program decomposition

The joins+windowing program: **4c-i** (Change + `to_table`, PR #388) → **4c-ii** (this
spec — KStream-KTable join) → **4c-iii** (KTable-KTable join) → **4d** (windowing +
window/session stores + windowed KStream-KStream join). KStream-KTable is the
first join: it establishes the shared join infrastructure — copartition
declaration, cross-side store lookup, join repartition, join golden frames — that
4c-iii reuses.

## 2. Goal & non-goals

### Goal
A KStream-KTable join in the DSL:
```rust
KStream<K,V>::join<VT,VO,F>(&self, table: &KTable<K,VT>, joiner: F) -> KStream<K,VO>
    where F: Fn(&V, &VT) -> VO + Clone + Send + 'static;          // inner
KStream<K,V>::left_join<VT,VO,F>(&self, table: &KTable<K,VT>, joiner: F) -> KStream<K,VO>
    where F: Fn(&V, Option<&VT>) -> VO + Clone + Send + 'static;  // left
```
- The **stream side drives**: each stream record looks up the table's materialized
  store by key; **inner** emits `joiner(v, vt)` only on a hit, **left** always emits.
- **Copartition group** declared (stream source + table source) → wire `copartition_groups`.
- **Repartition** inserted if the stream's key was changed upstream.
- Byte-exact vs JVM 4.1 (a captured join golden frame).

### Non-goals (deferred)
- **KTable-KTable** join → 4c-iii. **Windowed KStream-KStream** join → 4d.
- **GlobalKTable** joins, **foreign-key** joins, self/N-way joins → later.
- The joined table **must be materialized** (have a store); non-materialized table
  joins are out of scope (require a `Materialized`/`table()`/`to_table` KTable).
- Table *updates* do not produce join output (KStream-KTable semantics — only
  stream records drive).

## 3. Join processor (`dsl/processors/join.rs`)

One processor, parameterized by the join mode:
```rust
pub(crate) struct KStreamKTableJoinProcessor<K, V, VT, VO, F> {
    pub table_store: String,
    pub joiner: F,            // stored in LEFT form: Fn(&V, Option<&VT>) -> VO
    pub emit_on_miss: bool,   // false = inner, true = left
    pub _pd: Marker<(K, V, VT, VO)>,
}
impl<K, V, VT, VO, F> Processor<K, V, K, VO> for KStreamKTableJoinProcessor<…>
where K: Any+Send+Clone, V: 'static, VT: 'static, VO: Any+Send+Clone,
      F: Fn(&V, Option<&VT>) -> VO + Send + 'static {
    fn process(&mut self, ctx, r: Record<K, V>) {
        let key = r.key.expect("join key");
        let vt = ctx.get_state_store::<K, VT>(&self.table_store).and_then(|s| s.get(&key));
        if vt.is_some() || self.emit_on_miss {
            ctx.forward(Record::new(Some(key), (self.joiner)(&r.value, vt.as_ref()), r.timestamp));
        }
    }
}
```
- `join` (inner): `emit_on_miss = false`; the user's `Fn(&V,&VT)->VO` is wrapped as
  `move |v, opt| inner(v, opt.expect("inner join hit"))` (only reached on a hit).
- `left_join`: `emit_on_miss = true`; the user's `Fn(&V, Option<&VT>) -> VO` directly.

The table value type is `VT` (the KTable's store holds `VT` — not `Change`, since
the store is the materialized current value; #4c-i kept stores holding `V`).

## 4. Cross-side store connection (byte-exactness)

#2/#3's runtime instantiates **all** stores into the one per-task `StoreRegistry`,
so `get_state_store(table_store)` resolves at runtime. But the **wire topology**
must *connect* the join processor to the table's store — the JVM lists the join in
the store's connected-processors, which (a) unions the join + table source into one
subtopology and (b) places the table's changelog there. New builder method:
```rust
Topology::connect_processor_store(&mut self, processor_name: &str, store_name: &str) -> &mut Self
```
mirrors JVM `InternalTopologyBuilder.connectProcessorAndStateStores`. It appends
`processor_name` to the store's connected-processor list in `node.rs`'s
`StoreEntry`; `grouping.rs` already unions store-connected processors into one
subtopology, so the join + table land together byte-exactly.

## 5. Copartition declaration (`topology/builder.rs`, `node.rs`, `grouping.rs`)

The JVM records a copartition group when joining. New:
```rust
Topology::add_copartition_group(&mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> &mut Self
```
records a set of source/repartition topic names that must be copartitioned (stored
in the `NodeRegistry`). `grouping.rs`: for each subtopology, map each copartition
group's topics (that live in that subtopology) to **int16 indices** into the sorted
`source_topics` / `source_topic_regex` / `repartition_source_topics` arrays, and
emit `GroupTopics.copartition_groups`. `wire.rs` already encodes this (the
`copartition_group()` fn built in #1, previously unused). **Copartition validation**
(equal partition counts) is the broker's job (KIP-1071 broker-side already
validates the received topology); the client only *declares* the group.

## 6. DSL ops & lowering (`dsl/kstream.rs`, `dsl/graph.rs`, `dsl/lower.rs`)

`join`/`left_join` on `KStream<K,V>` (where the parent forwards `V`; the table is a
`&KTable<K,VT>` carrying its store name):
- Mint a join processor name (`KSTREAM-JOIN-` prefix — confirm vs the fixture;
  not wire-visible so latitude).
- If the stream's `key_changing` bit is set (from #4), insert a `Repartition` node
  first (reuse #4's repartition lowering); else none.
- Record a `Join` graph node (predecessor = the stream/repartition node).
- Lower thunk: `add_processor(join_name, KStreamKTableJoinProcessor{…}, [stream_parent])`
  + `connect_processor_store(join_name, table_store)` (§4)
  + `add_copartition_group([stream_source_or_repartition, table_source])` (§5).
- Return `KStream<K,VO>`.

The `KTable<K,VT>` handle must expose its **store name** + its **source topic**
(for the copartition group) to the join lowering — add `pub(crate)` accessors on
`KTable` if not already present (the store name is already tracked; the source
topic may need threading from `table()`).

## 7. JVM capture & golden frames

Add a join topology to `tests/jvm-capture/Capture.java`:
`builder.stream("left").join(builder.table("right", Materialized.as("store")), (v, vt) -> v + vt).to("out")`
(String serdes), capture via the Docker Kafka-Streams 4.1 harness → commit
`testdata/golden/dsl/stream_table_join.topology.json`. Expected shape (pinned by the
capture): one subtopology with `source_topics: ["left","right"]` (sorted),
`copartition_groups: [[indices of left,right]]`, `state_changelog_topics:
["app-store-changelog"]`. The **6 existing golden frames stay byte-identical**.

## 8. Testing strategy (gates)

1. **Join processor unit tests** — inner (hit→emit, miss→no emit), left (hit→emit,
   miss→emit with `None`).
2. **Copartition/grouping unit test** — `add_copartition_group` → wire
   `copartition_groups` has the right sorted int16 indices.
3. **Golden** — `stream_table_join` byte-matches the JVM fixture; the 6 prior
   golden frames stay byte-identical.
4. **Execution** (`TopologyTestDriver`) — populate the table (`pipe_input` `right`
   records), then pipe `left` records: inner emits only matched; left emits all
   (table value `None` when absent). Assert outputs.
5. **Regression** — all #1/#2/#3/#4/#4c-i tests stay green.

## 9. Success criteria
- `KStream::join`/`left_join` against a materialized KTable work (execution) and the
  join topology byte-matches captured JVM 4.1 output (incl. `copartition_groups`).
- The 6 prior golden frames unchanged.
- `cargo test -p crabka-client-streams` green; `cargo clippy --workspace
  --all-targets -- -D warnings` + `cargo fmt --check` clean; `cargo build
  --workspace`.
- A documented join example/doctest in `lib.rs`.

## 10. Open points for the plan
- **Join + table subtopology placement** — confirm via the fixture that the join +
  table source + the table's KTABLE-SOURCE land in ONE subtopology (the store
  connection unions them). The stream source "left" and table source "right" are
  both external; the fixture pins whether they're one subtopology (copartitioned
  join) or two.
- **`KSTREAM-JOIN-` counter position** — not wire-visible, but the store/changelog
  + copartition indices are; the fixture is the oracle.
- **KTable source-topic accessor** — the join needs the table's source topic for the
  copartition group; confirm `table()` records it on the `KTable` handle (thread it
  if missing).
- **Repartition-before-join** — the no-key-change path (no repartition) is the
  captured fixture; the key-change path (repartition) is covered by an execution
  test + can get its own fixture later if byte-exactness there is needed.
