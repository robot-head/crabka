# KIP-1071 Streams Client — Sub-project #4c-iii: KTable-KTable join (inner/left/outer)

**Date:** 2026-06-04
**Status:** Design approved, pending spec review
**Scope:** Third sub-slice of the joins+windowing program — the KTable-KTable join.
**Builds on:** #4 DSL + 4c-i `Change` (merged) + 4c-ii KStream-KTable join (PR #389, on `main`). Branch `streams-4c-ktable-join` (stacked on `streams-4c-joins`).

## 1. Context & program decomposition

Joins+windowing: 4c-i (Change/toTable, merged) → 4c-ii (KStream-KTable join, PR #389)
→ **4c-iii** (this spec — KTable-KTable join) → 4d (windowing + windowed
KStream-KStream join). 4c-ii built the shared join infrastructure (copartition
declaration, `connect_processor_store`, the join-processor pattern); 4c-iii reuses
it for the symmetric KTable-KTable join.

## 2. Goal & non-goals

### Goal
KTable-KTable join (inner/left/outer) in the DSL:
```rust
KTable<K,VA>::join<VB,VR,F>(&self, other: &KTable<K,VB>, joiner: F) -> KTable<K,VR>      // inner
    where F: Fn(&VA, &VB) -> VR + Clone + Send + Sync + 'static;
KTable<K,VA>::left_join<VB,VR,F>(&self, other, joiner) -> KTable<K,VR>                    // left
    where F: Fn(&VA, Option<&VB>) -> VR + …;
KTable<K,VA>::outer_join<VB,VR,F>(&self, other, joiner) -> KTable<K,VR>                   // outer
    where F: Fn(Option<&VA>, Option<&VB>) -> VR + …;
```
- A change on **either** input recomputes the join against the other side's current
  value and emits a `Change<VR>` to the result KTable.
- Both inputs must be **materialized** (have a store + source topic).
- **Copartition group** declared (`[A_source, B_source]`).
- The result KTable is **unmaterialized** (no result store/changelog — the JVM
  default value-getter).
- Byte-exact vs JVM 4.1 (a captured `ktable_ktable_join` golden frame).

### Non-goals (deferred)
- **Materialized** join result (a result store/changelog) — value-getter only.
- **Windowed** joins → 4d; GlobalKTable joins, foreign-key joins, self-join → later.
- Joining a **non-materialized** KTable (derived KTable without a store).

## 3. Architecture — dual processors + merger

A KTable-KTable join is symmetric → two join processors + a merger:
```
A node ──► JoinThis  (on A-change, reads B store)  ──┐
                                                      ├──► merger ──► result KTable<K,VR>
B node ──► JoinOther (on B-change, reads A store)  ──┘
```
Both processors connect to **both** source stores (4c-ii `connect_processor_store`)
→ grouping unions A, B, and the join into **one subtopology** (copartitioned). The
merger is a passthrough node with the two join processors as predecessors (like
`merge`); it *is* the result KTable's underlying node.

## 4. Join processors & `Change` merge (`dsl/processors/ktable_join.rs`)

A unified **result rule** parameterized by required-ness flags handles all three
join types. `JoinKind { a_required: bool, b_required: bool }` — inner `{true,true}`,
left (`a.left_join(b)`) `{true,false}`, outer `{false,false}`. The joiner is stored
in **outer form** `Fn(Option<&VA>, Option<&VB>) -> VR`:
```rust
fn result(a: Option<&VA>, b: Option<&VB>) -> Option<VR> =
    if (a.is_some() || !a_required) && (b.is_some() || !b_required) && (a.is_some() || b.is_some())
    { Some(joiner(a, b)) } else { None }   // None = no row (tombstone if a row existed)
```
Two processors (each connected to both stores):
- **`KTableKTableJoinThisProcessor`** (`Processor<K, Change<VA>, K, Change<VR>>`):
  on A's `Change{oldA,newA}` for `k`, `bCur = bStore.get(k)`; forward
  `Change{ old: result(oldA, bCur), new: result(newA, bCur) }`.
- **`KTableKTableJoinOtherProcessor`** (`Processor<K, Change<VB>, K, Change<VR>>`):
  on B's `Change{oldB,newB}`, `aCur = aStore.get(k)`; forward
  `Change{ old: result(aCur, oldB), new: result(aCur, newB) }`.

Both share the joiner + flags; they differ only in which side varies (its `Change`)
vs which is read current (the other store), and the joiner arg order (`This` calls
`joiner(a_change, b_cur)`; `Other` calls `joiner(a_cur, b_change)`). Each forwards
only when `old`/`new` is not `None→None` (suppresses no-op). A row dropping out
(`new: None`, `old: Some`) is a tombstone, propagated via 4c-i `Change`.

The **merger** reuses a passthrough `Change<VR>` processor (forwards its record
unchanged) with two predecessors — the JVM `KTableKTableJoinMerger`.

## 5. DSL ops & lowering (`dsl/ktable.rs`, `dsl/graph.rs`, `dsl/lower.rs`, `dsl/names.rs`)

`join`/`left_join`/`outer_join` on `KTable<K,VA>` (parent forwards `Change<VA>`;
`other: &KTable<K,VB>` forwards `Change<VB>`):
- Require both materialized: `a_store = self.store_name().expect(..)`,
  `b_store = other.store_name().expect(..)`; `a_src = self.source_topic()`,
  `b_src = other.source_topic()` (for the copartition group).
- Wrap the user joiner to outer form (inner: both `expect`; left: `a.expect`; outer:
  direct).
- Mint names (`KTABLE-JOINTHIS-`/`KTABLE-JOINOTHER-`/`KTABLE-MERGE-` — confirm vs the
  fixture; not wire-visible).
- Record nodes: `JoinThis` (pred = self.node), `JoinOther` (pred = other.node),
  `merger` (preds = [JoinThis, JoinOther]). Thunks:
  - JoinThis: `add_processor::<K, Change<VA>, K, Change<VR>, …>(name, || KTableKTableJoinThisProcessor{…}, [self_handle])`
    + `connect_processor_store(name, a_store)` + `connect_processor_store(name, b_store)`.
  - JoinOther: same with B parent + both store connections.
  - merger: `add_processor::<K, Change<VR>, K, Change<VR>, …>(merge_name, || passthrough, [joinThis_handle, joinOther_handle])`.
  - `add_copartition_group([a_src, b_src])` (when both Some).
- Return `KTable::new(.., merger_node, store_name: None, source_topic: None)` — the
  result is an unmaterialized `KTable<K,VR>` rooted at the merger.

## 6. JVM capture & golden frames

Add to `tests/jvm-capture/Capture.java`:
`builder.table("a", Materialized.as("sa")).join(builder.table("b", Materialized.as("sb")), (va, vb) -> va + vb).toStream().to("out");`
(String serdes, app id "app", optimization=all). Capture via the Docker
Kafka-Streams 4.1 harness → `testdata/golden/dsl/ktable_ktable_join.topology.json`.
Expected (pinned by capture): one subtopology, `source_topics: ["a","b"]`,
`copartition_groups: [[0,1]]`, `state_changelog_topics` = **both** source stores'
changelogs (`a`/`b` via source-topic reuse under optimization=all, or
`app-sa-changelog`/`app-sb-changelog` — the fixture decides), **no result
changelog**. The **7 prior golden frames stay byte-identical**.

## 7. Testing strategy (gates)

1. **Join-processor unit tests** — the `result()` rule for inner/left/outer
   (present/absent combinations); `JoinThis`/`JoinOther` emit the right `Change`
   (incl. tombstone when a row drops out).
2. **Golden** — `ktable_ktable_join` byte-matches the JVM fixture; the 7 prior
   golden frames stay byte-identical.
3. **Execution** (`TopologyTestDriver`): inner (pipe A & B for `k` → joined; remove
   one side → tombstone in the result store/output); left (A present, B absent →
   emit with `None`); outer (only B present → emit). Drive via `table()` sources +
   `toStream` output (or a downstream materialized op to inspect the result store).
4. **Regression** — all #1/#2/#3/#4/#4c-i/#4c-ii tests stay green.

## 8. Success criteria
- `KTable::join`/`left_join`/`outer_join` work (execution: inner/left/outer +
  tombstone) and the join topology byte-matches captured JVM 4.1 output (incl.
  copartition + both source changelogs, no result changelog).
- The 7 prior golden frames unchanged.
- `cargo test -p crabka-client-streams` green; `cargo clippy --workspace
  --all-targets -- -D warnings` + `cargo fmt --check` clean; `cargo build
  --workspace`.
- A documented KTable-KTable join example/note in `lib.rs`.

## 9. Open points for the plan
- **Subtopology placement** — confirm via the fixture that A, B, and both join
  processors land in ONE subtopology (the double store-connection unions them).
- **Result changelog** — confirm the fixture has NO result changelog (unmaterialized
  default); if the JVM materializes by default, adjust.
- **Changelog naming under optimization=all** — the source stores may reuse their
  source topics as changelogs (`a`/`b`) like 4c-ii's `stream_table_join` (golden uses
  `build_optimized`); the fixture is the oracle. Confirm whether the golden test uses
  `build` or `build_optimized`.
- **Merger node** — confirm a single merger (vs the two join processors feeding the
  result directly); not wire-visible, but the result KTable must root at one node so
  downstream ops have a single parent. A merger node is the clean choice.
- **`KTABLE-JOIN*` name prefixes / counter** — not wire-visible; the store/changelog
  + copartition indices are; the fixture pins them.
