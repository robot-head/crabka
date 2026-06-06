# GlobalKTable + stream-globaltable join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GlobalKTable` + the stream–globaltable join (`join_global`/`left_join_global`) to the DSL, including the real-runtime global consumer that materializes the global store from all partitions of the source topic.

**Architecture:** A `GlobalKTable` is a fully-replicated lookup table — every instance reads all partitions of the source topic into a shared global KV store (no changelog; the source topic is the truth). A `KStream` joins it by an arbitrary key derived per-record (no repartition/copartition). The global store lives in a shared `GlobalStateManager` populated by a global consumer before tasks process; join processors read it through the `ProcessorContext`.

**Tech Stack:** Rust, `async-trait`, `tokio`; reuses #3 `KeyValueBytesStore`/`StoreRegistry`, #4 DSL lowering, the JVM-capture golden harness.

**Branch / worktree:** `streams-global-table` (branched from `main`) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-06-kip-1071-streams-client-global-table-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-global-table` before each commit; commit `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push until asked.

---

## File Structure

**New files:**
- `src/dsl/global_table.rs` — `GlobalKTable<K,V>` handle.
- `src/dsl/processors/global_join.rs` — `KStreamGlobalTableJoinProcessor` + the global-update processor.
- `src/runtime/global.rs` — `GlobalStateManager` + the global consumer (bootstrap + live).
- `tests/testdata/golden/dsl/global_table_join.topology.json` — golden #15.

**Modified files:**
- `src/dsl/builder.rs` — `StreamsBuilder::global_table`.
- `src/dsl/kstream.rs` — `join_global`/`left_join_global` + shared lowering.
- `src/dsl/graph.rs` — `GraphNodeKind::GlobalSource` (or equivalent, sized to capture).
- `src/dsl/names.rs` — `GLOBALTABLE_SOURCE` / `GLOBALTABLE_PROCESSOR` / store prefixes.
- `src/topology/builder.rs` — `Topology::add_global_store` + instantiate the global store separately.
- `src/topology/node.rs` — a "global store/source" marker on `NodeRegistry`.
- `src/topology/grouping.rs` + `src/topology/wire.rs` — global subtopology emission (**capture-first**).
- `src/processor/api.rs` + `src/processor/erased.rs` (dispatch) — `get_global_kv_store` access.
- `src/runtime/app.rs` — wire the `GlobalStateManager` into `KafkaStreamsApp` startup.
- `src/test_driver.rs` — populate/expose the global store for execution tests.
- `src/lib.rs` — re-export `GlobalKTable`.
- `tests/jvm-capture/.../Capture.java` + `run.sh` — fixture #15.
- `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`.

## Execution phases

- **G-i** (T1–T5): DSL + topology + golden + TestDriver execution. The global store is populated in-process (no real consumer).
- **G-ii** (T6–T9): real global consumer + `GlobalStateManager` + `ProcessorContext` wiring + broker e2e.

T1 is **capture-first** (CONTROLLER runs Docker). Because the KIP-1071 wire has no global field, the exact subtopology shape is unknown until T1; downstream wire tasks build to match the captured fixture.

---

## Phase G-i

## Task 1: JVM capture — pin the global-store wire shape (CONTROLLER, capture-first)

**Files:**
- Modify: `tests/jvm-capture/src/main/java/crabka/capture/Capture.java`, `tests/jvm-capture/run.sh`
- Create: `tests/testdata/golden/dsl/global_table_join.topology.json`

> **Capture-first:** the KIP-1071 `StreamsGroupHeartbeatRequest.Topology` is `{epoch, subtopologies[]}` with no global-stores field. How the JVM encodes a global store is unknown — capture it before writing any Rust wire code.

- [ ] **Step 1: Add the capture topology.** In `Capture.java`, add (mirroring the existing `streamTableJoin()`):

```java
/** 15. global_table_join: a KStream joined to a GlobalKTable by a key-mapper. */
static Topology globalTableJoin() {
    StreamsBuilder b = new StreamsBuilder();
    GlobalKTable<String, String> g = b.globalTable("global");
    b.<String, String>stream("in")
        .join(g, (k, v) -> v, (sv, gv) -> sv + gv)   // key-mapper maps stream value → global key
        .to("out");
    return b.build(optimizedProps());
}
```
Register it: `write(outDir, "global_table_join", globalTableJoin());` and bump the "Wrote N fixtures" count + the run.sh fixture-name list (currently 14 → 15). Imports: `import org.apache.kafka.streams.kstream.GlobalKTable;`.

- [ ] **Step 2: Run the capture (CONTROLLER).**
```
cd /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl/crates/client-streams/tests/jvm-capture
./run.sh --gradle
```
Writes `tests/testdata/golden/dsl/global_table_join.topology.json`.

- [ ] **Step 3: Read + record the wire shape.** Inspect the fixture:
```
python3 -c "import json,sys; d=json.load(open('../testdata/golden/dsl/global_table_join.topology.json')); print(json.dumps(d,indent=1))"
```
Record in the plan/commit message: (a) how many subtopologies; (b) which subtopology carries the `global` source topic and whether it has `state_changelog_topics` (expected: none); (c) the global store/source naming; (d) whether the join's stream subtopology references the global topic at all. **This shape drives Tasks 2–3's wire emission.**

- [ ] **Step 4: Commit the capture.**
```
git -C <worktree> add crates/client-streams/tests/jvm-capture/ crates/client-streams/tests/testdata/golden/dsl/global_table_join.topology.json
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams): capture JVM global_table_join topology (golden #15, capture-first)"
```

---

## Task 2: `GlobalKTable` type + global KV store + `add_global_store` wire

**Files:**
- Create: `src/dsl/global_table.rs`
- Modify: `src/dsl/mod.rs`, `src/topology/node.rs`, `src/topology/builder.rs`, `src/topology/grouping.rs`, `src/topology/wire.rs`, `src/dsl/names.rs`, `src/lib.rs`

- [ ] **Step 1: `GlobalKTable<K,V>` handle** (`src/dsl/global_table.rs`). Mirror the thin parts of `KTable` (builder `Rc` + node id + store name); no ops beyond holding identity.

```rust
//! `GlobalKTable<K,V>`: a fully-replicated lookup table. A join target only — no
//! aggregations, no `to_stream`. Built by `StreamsBuilder::global_table`; consumed
//! by `KStream::join_global`/`left_join_global`.
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::NodeId;

pub struct GlobalKTable<K, V> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) node: NodeId,
    pub(crate) store_name: String,
    pub(crate) source_topic: String,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> GlobalKTable<K, V> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        store_name: String,
        source_topic: String,
    ) -> Self {
        Self { builder, node, store_name, source_topic, _pd: PhantomData }
    }
    pub(crate) fn store_name(&self) -> &str { &self.store_name }
}
```
Register `pub mod global_table;` in `src/dsl/mod.rs`; `pub use dsl::global_table::GlobalKTable;`-style re-export in `src/lib.rs`.

- [ ] **Step 2: name prefixes** (`src/dsl/names.rs`). Add the JVM global prefixes **as observed in the T1 capture** (the JVM uses e.g. `KSTREAM-SOURCE-`/`KTABLE-SOURCE-` for global source + a global store name). Add constants matching the capture, e.g.:
```rust
pub(crate) const GLOBALTABLE_SOURCE: &str = "<from capture>";
pub(crate) const GLOBALTABLE_PROCESSOR: &str = "<from capture>";
```

- [ ] **Step 3: `NodeRegistry` global marker** (`src/topology/node.rs`). **T1 CAPTURE RESULT (ground truth):** a GlobalKTable is **invisible** in the wire — NO source topic, NO changelog, NO subtopology. BUT the global node group **consumes a subtopology index**: declared first, it takes index 0, so the stream subtopology is emitted as `subtopology_id "1"` (a normal single-subtopology app is "0"). So the marker records the global source/store/processor as a **distinct node group that is assigned an index by the grouping pass but excluded from the emitted `subtopologies`**. Minimal: a `global_groups: Vec<GlobalGroup>` (`{ source_topic, store_name, processor }`) the grouping pass numbers + the wire layer skips.

- [ ] **Step 4: `Topology::add_global_store`** (`src/topology/builder.rs`). Mirror `add_state_store` but: register a global KV store factory (a `KeyValueBytesStore<K,V>` with **empty changelog topic** — no changelog), and record the global source/store/processor as a global node group (not a normal store entry). The global store goes into a SEPARATE store map (`global_store_factories`) so per-task `instantiate` does NOT build it (G-ii builds it in the `GlobalStateManager`); for G-i the TestDriver builds it directly.

```rust
pub fn add_global_store<K, V, KS, VS>(
    &mut self,
    store_name: impl Into<String>,
    source_name: impl Into<String>,
    topic: impl Into<String>,
    processor_name: impl Into<String>,
    key_serde: KS, value_serde: VS,
) -> &mut Self
where K: Send + 'static, V: Send + 'static, KS: Serde<K> + Clone, VS: Serde<V> + Clone { /* ... */ }
```

- [ ] **Step 5: wire emission** (`src/topology/grouping.rs` + `src/topology/wire.rs`). The grouping pass assigns the global node group an index (global-first → 0); the wire layer **excludes** global groups from `subtopologies` while **preserving the index bump** (the stream subtopology emits as id "1"). Match `global_table_join.topology.json` byte-for-byte: a single subtopology `{id:"1", source_topics:["in"], all-others empty}`.

- [ ] **Step 6: unit test** the wire shape against the fixture is deferred to T5's golden; here add a focused `NodeRegistry`/`add_global_store` unit test (store registered in the global list, no changelog entry produced). `cargo test -p crabka-client-streams --lib global` + clippy/fmt.

- [ ] **Step 7: Commit** `feat(streams): GlobalKTable type + global KV store + add_global_store wire`.

---

## Task 3: `StreamsBuilder::global_table` + lowering

**Files:**
- Modify: `src/dsl/builder.rs`, `src/dsl/graph.rs`

- [ ] **Step 1: `global_table`** (`src/dsl/builder.rs`). Mirror `table()` but: mint the global source/store/processor names at the JVM counter positions (per the T1 capture's name indices), add a `GraphNodeKind::GlobalSource { topic, store_name, source_name, processor_name }` logical node, and in its lowering thunk call `state.topology.add_global_store::<K,V,KS,VS>(...)`. Return a `GlobalKTable<K,V>`.

```rust
pub fn global_table<K, V, KS, VS>(
    &self, topic: impl Into<String>, consumed: Consumed<KS, VS>, materialized: Materialized<KS, VS>,
) -> crate::dsl::global_table::GlobalKTable<K, V>
where K: Any + Send + Sync + Clone, V: Any + Send + Clone,
      KS: Serde<K> + Clone + 'static, VS: Serde<V> + Clone + 'static { /* mint names; add node; thunk → add_global_store; return GlobalKTable */ }
```

- [ ] **Step 2: golden-name alignment.** Build the topology with just `global_table("global", …)` and assert (via a temporary debug or the T5 golden) the global source/store/processor names match the capture. Tune the name minting order to match.

- [ ] **Step 3: unit test** that `global_table` returns a handle with the right `store_name`/`source_topic`. `cargo test` + clippy/fmt.

- [ ] **Step 4: Commit** `feat(streams-dsl): StreamsBuilder::global_table + lowering`.

---

## Task 4: stream-globaltable join processor + `join_global`/`left_join_global`

**Files:**
- Create: `src/dsl/processors/global_join.rs`
- Modify: `src/dsl/processors/mod.rs`, `src/dsl/kstream.rs`, `src/processor/api.rs`

- [ ] **Step 1: the join processor** (`src/dsl/processors/global_join.rs`). The processor reads the global store via a NEW context accessor `get_global_kv_store::<GK,VG>(name)` (added in this task as a thin shim that, in G-i, reads the same per-task `StoreRegistry` where the TestDriver placed the global store; G-ii repoints it at the shared global registry). Per record: `gk = (key_mapper)(&k, &v)`; look up; inner → emit `joiner(&v, &vg)` on hit; left → emit `joiner(&v, opt)` always. Keep the stream key + timestamp.

```rust
pub(crate) struct KStreamGlobalTableJoinProcessor<K, V, GK, VG, VR, KM, J> {
    pub store_name: String,
    pub key_mapper: KM,            // Fn(&K,&V)->GK
    pub joiner: J,                 // Fn(&V, Option<&VG>)->VR
    pub emit_on_miss: bool,        // false=inner, true=left
    pub _pd: PhantomData<fn() -> (K, V, GK, VG, VR)>,
}
#[async_trait]
impl<K,V,GK,VG,VR,KM,J> Processor<K,V,K,VR> for KStreamGlobalTableJoinProcessor<...>
where K: Any+Send+Sync+Clone, V: Any+Send+Clone, GK: Any+Send+Sync, VG: Any+Send+Clone,
      VR: Any+Send+Clone, KM: Fn(&K,&V)->GK+Send+'static, J: Fn(&V,Option<&VG>)->VR+Send+'static {
    async fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,K,VR>, r: Record<K,V>) {
        let k = r.key.expect("global join requires a non-null stream key");
        let gk = (self.key_mapper)(&k, &r.value);
        let looked = { let s = ctx.get_global_kv_store::<GK,VG>(&self.store_name).expect("global store"); s.get(&gk).await };
        match (looked, self.emit_on_miss) {
            (Some(vg), _) => ctx.forward(Record::new(Some(k), (self.joiner)(&r.value, Some(&vg)), r.timestamp)),
            (None, true)  => ctx.forward(Record::new(Some(k), (self.joiner)(&r.value, None), r.timestamp)),
            (None, false) => {}
        }
    }
}
```
Note the global store is read-only here (`get`), so a `&dyn KeyValueStore<GK,VG>` read accessor suffices; the scoped borrow drops before `forward`.

- [ ] **Step 2: `get_global_kv_store` accessor** (`src/processor/api.rs`). In G-i, alias it to the per-task `get_kv` (the TestDriver registers the global store in the per-task registry). G-ii repoints it at the shared registry. Document the G-ii TODO.

- [ ] **Step 3: `join_global`/`left_join_global`** (`src/dsl/kstream.rs`). Mirror `join_table_impl` BUT: no `key_changing` assertion (lookup key is derived, not the stream key), no copartition declaration, and `connect_processor_store` is NOT used (the global store is accessed via the global registry, not a copartitioned subtopology). The join processor node is a plain stateless processor wired to the stream's parent.

```rust
pub fn join_global<GK,VG,VR,KM,J>(&self, global:&GlobalKTable<GK,VG>, key_mapper:KM, joiner:J) -> KStream<K,VR>
where ..., J: Fn(&V,&VG)->VR+... { let jf = move |v:&V,opt:Option<&VG>| joiner(v,opt.expect("inner")); self.join_global_impl(global,key_mapper,jf,false) }
pub fn left_join_global<GK,VG,VR,KM,J>(&self, global:&GlobalKTable<GK,VG>, key_mapper:KM, joiner:J) -> KStream<K,VR>
where ..., J: Fn(&V,Option<&VG>)->VR+... { self.join_global_impl(global,key_mapper,joiner,true) }
```

- [ ] **Step 4: unit/processor test** the join processor (seed a per-task `KeyValueBytesStore` as the "global" store; inner hit/miss + left hit/miss; key-mapper maps to a non-stream key). `cargo test -p crabka-client-streams --lib global_join` + clippy/fmt.

- [ ] **Step 5: Commit** `feat(streams-dsl): stream-globaltable join processor + join_global/left_join_global`.

---

## Task 5: golden #15 + TopologyTestDriver execution

**Files:**
- Modify: `src/test_driver.rs`, `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`, `src/lib.rs`

- [ ] **Step 1: TestDriver global-store support** (`src/test_driver.rs`). `instantiate` must build the global store(s) into the per-task registry (so the join processor + a `pipe_input("global", …)` to the global topic both reach it). Add: when piping to a global source topic, the record is written into the global store (the global-update processor path) AND the join sees it. Provide a `pipe_global(topic, k, v)` helper or route the existing `pipe_input` through the global-update processor.

- [ ] **Step 2: golden test** (`tests/dsl_golden_frame.rs`). `global_table_join_matches_jvm`: build the Rust equivalent of T1's `globalTableJoin()` and `assert_matches_fixture(&wire, "global_table_join")`. Also assert the **14 prior goldens stay byte-identical** (they already run in the suite).

```rust
#[test]
fn global_table_join_matches_jvm() {
    use crabka_client_streams::{Consumed, Materialized, StringSerde, Produced};
    let b = StreamsBuilder::new();
    let g = b.global_table::<String,String,_,_>("global", Consumed::with(StringSerde,StringSerde), Materialized::with(StringSerde,StringSerde));
    b.stream(["in"], Consumed::with(StringSerde,StringSerde))
        .join_global(&g, |_k:&String, v:&String| v.clone(), |sv:&String, gv:&String| format!("{sv}{gv}"))
        .to("out", Produced::with(StringSerde,StringSerde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "global_table_join");
}
```

- [ ] **Step 3: execution tests** (`tests/dsl_execution.rs`): (a) inner join — pipe to `global` then `in`; hit emits `joiner`, miss skips; (b) left join — miss emits `joiner(v, None)`; (c) key-mapper to a non-stream key resolves against the global store; (d) a later `global` update is seen by a subsequent `in` record.

- [ ] **Step 4: final G-i verify + commit.** `cargo test -p crabka-client-streams` (all green, **15 goldens byte-identical**), `--doc`, `cargo clippy --all-targets -D warnings`, `cargo fmt --check`. Commit `test(streams-dsl): global_table_join golden #15 + TestDriver execution`.

---

## Phase G-ii — real global consumer

## Task 6: `GlobalStateManager` (shared global store registry)

**Files:**
- Create: `src/runtime/global.rs`
- Modify: `src/runtime/mod.rs`, `src/topology/builder.rs`

- [ ] **Step 1: `GlobalStateManager`.** Holds the instantiated global stores in a shared structure (`Arc<tokio::sync::Mutex<StoreRegistry>>` or an `Arc`-of-store map) built from the `global_store_factories` recorded in T2. One per `KafkaStreamsApp`. Expose `get_global::<K,V>(name)` for the dispatch + `apply(store, k_bytes, v_bytes)` for the consumer.

- [ ] **Step 2: instantiate the global stores** from the `BuiltTopology`'s global factories (separate from per-task `instantiate`). Unit test: build a global store, `apply` some records, `get` returns them.

- [ ] **Step 3: Commit** `feat(streams-runtime): GlobalStateManager shared global store registry`.

## Task 7: global consumer (bootstrap-all-partitions + live)

**Files:**
- Modify: `src/runtime/global.rs`, `src/runtime/io.rs` (if a "list partitions / read all" fetch is needed)

- [ ] **Step 1: bootstrap.** For each global store, read **all partitions** of its source topic from offset 0 to end-of-log (a `RecordFetcher` extension or loop over partitions), deserialize via the store's serdes, `apply` into the store. Block until end-of-log on every partition.
- [ ] **Step 2: live updates.** After bootstrap, keep consuming and `apply`-ing (a background task). For the e2e test a single bootstrap pass + periodic poll is sufficient.
- [ ] **Step 3: test** (in-memory fetcher with 2 partitions of the global topic → store has all records). Commit `feat(streams-runtime): global consumer bootstrap-all-partitions + live updates`.

## Task 8: `ProcessorContext` global access + `KafkaStreamsApp` wiring

**Files:**
- Modify: `src/processor/api.rs`, `src/processor/erased.rs` (dispatch), `src/runtime/app.rs`, `src/runtime/task.rs`

- [ ] **Step 1: dispatch carries the shared global registry.** Thread the `GlobalStateManager` (Arc) into the per-task `Dispatch`/`ProcessorContext` so `get_global_kv_store` reads the SHARED registry (replacing the G-i per-task alias). Keep G-i's TestDriver path working (TestDriver supplies a `GlobalStateManager` too).
- [ ] **Step 2: `KafkaStreamsApp` startup.** Build the `GlobalStateManager`, run the global consumer's bootstrap **before** `apply_assignment` starts task processing; share the manager into every `StreamTask`'s dispatch.
- [ ] **Step 3: integration test** (in-process): an app with a global store + a stream join; bootstrap the global store from a scripted fetcher; process a stream record; assert the join output. Commit `feat(streams-runtime): ProcessorContext global-store access + app wiring`.

## Task 9: broker e2e + docs + final verification

**Files:**
- Modify: `tests/` (a broker integration test), `src/lib.rs`

- [ ] **Step 1: broker e2e.** Produce to the global topic (all partitions) + the stream topic against a real broker; run the app; assert the join output topic. Mirror the existing DSL broker integration test (4-T12).
- [ ] **Step 2: docs.** `lib.rs` prose: GlobalKTable (fully-replicated, no copartition), `join_global`/`left_join_global`, the global consumer.
- [ ] **Step 3: final verify.** `cargo test -p crabka-client-streams` + `--doc` + `cargo clippy --all-targets -D warnings` + `cargo fmt --check`. All green; **15 goldens byte-identical**. Commit `test(streams): global-table broker e2e + docs + final verification`.

---

## Done criteria
- `GlobalKTable` + `global_table()` + `join_global`/`left_join_global` work; lookup key derived per-record (no repartition/copartition).
- Wire `global_table_join` golden #15 byte-matches JVM 4.1; **14 prior goldens byte-identical**.
- Real global consumer bootstraps the shared global store from all partitions before tasks process; join reads it via `ProcessorContext`; broker e2e passes.
- Full suite + doctests + clippy `--all-targets -D warnings` + fmt green.

## Notes for the implementer
- **T1 is capture-first and gates Tasks 2–3's wire emission** — do not write the global subtopology shape before reading the fixture. The spec's structure (own subtopology, no changelog) is the *expected* shape; the fixture is ground truth.
- The global join differs from `join_table`: **no** `key_changing` assert, **no** copartition group, **no** `connect_processor_store` — the global store is reached through the shared global registry, not a copartitioned subtopology.
- Global store = `KeyValueBytesStore<K,V>` with an **empty changelog topic** (never flushes; the source topic is the truth).
