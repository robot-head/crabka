# KIP-1071 Streams Client #4c-iii — KTable-KTable join — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KTable::join`/`left_join`/`outer_join` (KTable-KTable join) producing an unmaterialized result KTable, byte-exact vs JVM 4.1.

**Architecture:** Two join processors (`JoinThis` reads B's store on an A-change; `JoinOther` reads A's store on a B-change), each applying a unified `result()` rule (`JoinKind{a_required,b_required}` + an outer-form joiner), feeding a passthrough **merger** (the result KTable's node). Each join processor connects to the store it reads; the merger's predecessor edges union A, B, and the join into one copartitioned subtopology. A captured `ktable_ktable_join` golden frame pins the bytes.

**Tech Stack:** Rust 2024; extends #4 DSL + 4c-i `Change` + 4c-ii join infra. JVM capture via the Docker Kafka-Streams 4.1 harness.

**Spec:** `docs/superpowers/specs/2026-06-04-kip-1071-streams-client-4c-iii-ktable-join-design.md`.
**Branch:** `streams-4c-ktable-join` (stacked on `streams-4c-joins`; worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`).

---

## Reuse (from 4c-i / 4c-ii — verbatim)

- `Change<V>` (`dsl/processors/change.rs`): `{ old: Option<V>, new: Option<V> }`, `update`/`tombstone`/`is_tombstone`/`map`.
- KTable nodes forward `Record<K, Change<V>>`; stores hold `V`.
- `dsl/processors/stateless.rs::MergeProcessor<K,V>: Processor<K,V,K,V>` — forwards `r` unchanged. **Reuse with `V = Change<VR>` as the merger** (no new merger processor needed).
- `dsl/ktable.rs`: `KTable<K,V> { builder, node, store_name: Option<String>, source_topic: Option<String>, _pd }`; `KTable::new(builder, node, store_name, source_topic)`; `store_name()`/`source_topic()` accessors. KTable ops attach lower thunks; the lower-thunk pattern reconstructs a parent via `crate::topology::NodeHandle::<K, Change<Vparent>>::from_name(state.handle_name[&parent_id].clone())`.
- `Topology::{connect_processor_store(processor, store), add_copartition_group(topics), add_processor::<KIn,VIn,KOut,VOut,_,_,_>(name, supplier, parents)}`. `ProcessorContext::get_state_store::<K,V>(name)`.
- 4c-ii's `dsl/kstream.rs::join_impl` is the structural reference for a join lowering (mint name, record node, thunk: add_processor + connect_processor_store + add_copartition_group).

## File structure

```
dsl/processors/ktable_join.rs   NEW — result() rule + KTableKTableJoinThis/OtherProcessor
dsl/processors/mod.rs           + pub(crate) mod ktable_join;
dsl/ktable.rs                   + join() / left_join() / outer_join() + join_impl
dsl/graph.rs                    (reuse StatelessProcessor kind for join/merge nodes)
dsl/names.rs                    + KTABLE-JOINTHIS-/JOINOTHER-/MERGE- prefixes
tests/jvm-capture/.../Capture.java  + ktableKtableJoin topology
tests/testdata/golden/dsl/ktable_ktable_join.topology.json  NEW
tests/dsl_golden_frame.rs       + ktable_ktable_join golden
tests/dsl_execution.rs          + inner/left/outer + tombstone exec tests
lib.rs                          KTable-KTable join doc note
```

**Batching:** sequential. Task 3's capture (`jvm-capture/` + `testdata`) is independent.

---

## Task 1: Join processors + result rule

**Files:** Create `dsl/processors/ktable_join.rs`; modify `dsl/processors/mod.rs`.

- [ ] **Step 1: failing test** — append to `dsl/processors/ktable_join.rs` (mirror `dsl/processors/aggregate.rs`'s `Dispatch`+`StoreRegistry` harness; seed the OTHER store):
```rust
// JoinKind unit: result(a,b) for inner/left/outer present/absent combos.
// JoinThis: store "b" has ("k", "B"); process A-change {old:None,new:Some("A")} for "k"
//   inner → forward Change{old:None, new:Some("AB")}.
//   process A-change {old:Some("A"), new:None} → Change{old:Some("AB"), new:None} (tombstone).
// (Use joiner |a:Option<&String>, b:Option<&String>| format!("{}{}", a.cloned().unwrap_or_default(), b.cloned().unwrap_or_default()).)
```
Concretely: build a `StoreRegistry` with store `"b"` (`InMemoryKeyValueStore::<String,String>`), seed `("k","B")`; build `KTableKTableJoinThisProcessor{ other_store:"b".into(), joiner, kind: JoinKind::inner(), _pd }`; run `process` with `Record::new(Some("k".into()), Change::update(None, "A".into()), 0)`; assert the forwarded `Change<String>` (downcast) has `new == Some("AB")`. Add a left + outer assertion (e.g. inner with absent B → no forward; left A-present B-absent → emit; outer B-only → emit).

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib dsl::processors::ktable_join`

- [ ] **Step 3: implement** `dsl/processors/ktable_join.rs`:
```rust
use std::marker::PhantomData;
use crate::dsl::processors::change::Change;
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;
type Marker<T> = PhantomData<fn() -> T>;

#[derive(Clone, Copy)]
pub(crate) struct JoinKind { pub a_required: bool, pub b_required: bool }
impl JoinKind {
    pub fn inner() -> Self { Self { a_required: true,  b_required: true } }
    pub fn left()  -> Self { Self { a_required: true,  b_required: false } } // a.left_join(b)
    pub fn outer() -> Self { Self { a_required: false, b_required: false } }
}

/// `Some(VR)` iff the join row exists for (a,b) under `kind`; else `None` (tombstone).
fn result<VA, VB, VR, F>(kind: JoinKind, joiner: &F, a: Option<&VA>, b: Option<&VB>) -> Option<VR>
where F: Fn(Option<&VA>, Option<&VB>) -> VR {
    let present = (a.is_some() || !kind.a_required)
        && (b.is_some() || !kind.b_required)
        && (a.is_some() || b.is_some());
    if present { Some(joiner(a, b)) } else { None }
}

/// A-side change: vary A, read B current. `Processor<K, Change<VA>, K, Change<VR>>`.
#[allow(dead_code)]
pub(crate) struct KTableKTableJoinThisProcessor<K, VA, VB, VR, F> {
    pub other_store: String, // B's store
    pub joiner: F,           // Fn(Option<&VA>, Option<&VB>) -> VR
    pub kind: JoinKind,
    pub _pd: Marker<(K, VA, VB, VR)>,
}
impl<K, VA, VB, VR, F> Processor<K, Change<VA>, K, Change<VR>> for KTableKTableJoinThisProcessor<K, VA, VB, VR, F>
where K: std::any::Any+Send+Clone, VA: 'static, VB: 'static, VR: std::any::Any+Send+Clone,
      F: Fn(Option<&VA>, Option<&VB>) -> VR + Send + 'static {
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<VR>>, r: Record<K, Change<VA>>) {
        let key = r.key.expect("join key");
        let b_cur = ctx.get_state_store::<K, VB>(&self.other_store).and_then(|s| s.get(&key));
        let old = result(self.kind, &self.joiner, r.value.old.as_ref(), b_cur.as_ref());
        let new = result(self.kind, &self.joiner, r.value.new.as_ref(), b_cur.as_ref());
        if old.is_some() || new.is_some() {
            ctx.forward(Record::new(Some(key), Change { old, new }, r.timestamp));
        }
    }
}

/// B-side change: read A current, vary B. `Processor<K, Change<VB>, K, Change<VR>>`.
#[allow(dead_code)]
pub(crate) struct KTableKTableJoinOtherProcessor<K, VA, VB, VR, F> {
    pub other_store: String, // A's store
    pub joiner: F,
    pub kind: JoinKind,
    pub _pd: Marker<(K, VA, VB, VR)>,
}
impl<K, VA, VB, VR, F> Processor<K, Change<VB>, K, Change<VR>> for KTableKTableJoinOtherProcessor<K, VA, VB, VR, F>
where K: std::any::Any+Send+Clone, VA: 'static, VB: 'static, VR: std::any::Any+Send+Clone,
      F: Fn(Option<&VA>, Option<&VB>) -> VR + Send + 'static {
    fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<VR>>, r: Record<K, Change<VB>>) {
        let key = r.key.expect("join key");
        let a_cur = ctx.get_state_store::<K, VA>(&self.other_store).and_then(|s| s.get(&key));
        let old = result(self.kind, &self.joiner, a_cur.as_ref(), r.value.old.as_ref());
        let new = result(self.kind, &self.joiner, a_cur.as_ref(), r.value.new.as_ref());
        if old.is_some() || new.is_some() {
            ctx.forward(Record::new(Some(key), Change { old, new }, r.timestamp));
        }
    }
}
```
`dsl/processors/mod.rs`: `pub(crate) mod ktable_join;`. (`+ Sync` on the joiner is added at the DSL supplier call site in Task 2, not here.)

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): KTable-KTable join processors + result rule`.

---

## Task 2: join/left_join/outer_join DSL ops + lowering + execution

**Files:** `dsl/ktable.rs`, `dsl/names.rs`; `tests/dsl_execution.rs`.

- [ ] **Step 1: failing execution test** — `tests/dsl_execution.rs`:
```rust
#[test]
fn dsl_ktable_ktable_inner_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let ta = b.table::<String,String,_,_>("a", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sa"));
    let tb = b.table::<String,String,_,_>("b", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sb"));
    ta.join(&tb, |va: &String, vb: &String| format!("{va}{vb}")).to_stream().to("out", Produced::with(StringSerde, StringSerde));
    drop(ta); drop(tb);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // A then B for "k" → joined "AB"
    d.pipe_input("a", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "A".to_string(), 0);
    // no B yet → inner emits nothing
    assert_eq!(d.read_output("out", Produced::with(StringSerde, StringSerde)), None);
    d.pipe_input("b", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "B".to_string(), 1);
    assert_eq!(d.read_output("out", Produced::with(StringSerde, StringSerde)), Some((Some("k".to_string()), "AB".to_string())));
}
```
Plus `dsl_ktable_ktable_left_join_executes` (A present, B absent → `left_join(|va, ob| format!("{va}{}", ob.cloned().unwrap_or_default()))` emits `"A"`) and `dsl_ktable_ktable_outer_join_executes` (only B → `outer_join(|oa, ob| format!("{}{}", oa.cloned().unwrap_or_default(), ob.cloned().unwrap_or_default()))` emits `"B"`). ADJUST to the real `table`/`to_stream`/`to`/`pipe_input` signatures.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** in `dsl/ktable.rs`:
  - `dsl/names.rs`: `pub(crate) const KTABLE_JOIN_THIS: &str = "KTABLE-JOINTHIS-"; pub(crate) const KTABLE_JOIN_OTHER: &str = "KTABLE-JOINOTHER-"; pub(crate) const KTABLE_MERGE: &str = "KTABLE-MERGE-";` (`#[allow(dead_code)]` if flagged; not wire-visible).
  - `KTable::join<VB,VR,F>(&self, other: &KTable<K,VB>, joiner: F) -> KTable<K,VR>` where `VB: Any+Send+Clone, VR: Any+Send+Clone, F: Fn(&VA,&VB)->VR + Clone+Send+Sync+'static`. Wrap: `let jf = move |a: Option<&VA>, b: Option<&VB>| joiner(a.expect("inner a"), b.expect("inner b"));` → `join_impl(other, jf, JoinKind::inner())`.
  - `left_join` → `F: Fn(&VA, Option<&VB>)->VR`; wrap `move |a, b| joiner(a.expect("left a"), b)`; `JoinKind::left()`.
  - `outer_join` → `F: Fn(Option<&VA>, Option<&VB>)->VR`; direct; `JoinKind::outer()`.
  - Shared `fn join_impl<VB,VR,JF>(&self, other: &KTable<K,VB>, jf: JF, kind: JoinKind) -> KTable<K,VR>` where `JF: Fn(Option<&VA>,Option<&VB>)->VR + Clone+Send+Sync+'static`:
    - `let a_store = self.store_name().expect("KTable-KTable join: left table must be materialized").to_string();`
    - `let b_store = other.store_name().expect("...: right table must be materialized").to_string();`
    - `let a_src = self.source_topic().map(str::to_string); let b_src = other.source_topic().map(str::to_string);`
    - mint `join_this = new_processor_name(KTABLE_JOIN_THIS)`, `join_other = new_processor_name(KTABLE_JOIN_OTHER)`, `merge = new_processor_name(KTABLE_MERGE)`.
    - record nodes: `this_id` (pred = `self.node`), `other_id` (pred = `other.node`), `merge_id` (preds = `[this_id, other_id]`).
    - thunks (capture `jf.clone()`, store names, kind):
      - this: `let h = state.topology.add_processor::<K, Change<VA>, K, Change<VR>, _, _, _>(join_this, move || KTableKTableJoinThisProcessor{ other_store: b_store2.clone(), joiner: jf2.clone(), kind, _pd: PhantomData }, [NodeHandle::<K,Change<VA>>::from_name(state.handle_name[&self_node].clone())]); state.topology.connect_processor_store(&join_this, &b_store2); state.handle_name.insert(this_id, h.name);`
      - other: symmetric — parent `other.node` (`NodeHandle<K, Change<VB>>`), processor reads `a_store`, `connect_processor_store(join_other, a_store)`.
      - merge: `add_processor::<K, Change<VR>, K, Change<VR>, _, _, _>(merge, || MergeProcessor{...}, [NodeHandle::<K,Change<VR>>::from_name(this_handle), NodeHandle::<K,Change<VR>>::from_name(other_handle)])` — reconstruct from `state.handle_name[&this_id]` / `[&other_id]`. (MergeProcessor is `dsl::processors::stateless::MergeProcessor`.)
      - `if let (Some(a), Some(bb)) = (&a_src, &b_src) { state.topology.add_copartition_group([a.clone(), bb.clone()]); }`
    - return `KTable::new(Rc::clone(&self.builder), merge_id, None, None)` (unmaterialized result rooted at the merger). The graph nodes use `GraphNodeKind::StatelessProcessor{repartition_required:false}` (or a `Join` kind if one exists — reuse Stateless).
  - NOTE on the thunk ordering: the merger thunk runs AFTER the two join thunks (node-id order: this_id < other_id < merge_id), so `state.handle_name[&this_id]`/`[&other_id]` are populated when the merger lowers. The lowering driver runs thunks in id order — guaranteed.

- [ ] **Step 4: run → exec PASS (inner/left/outer); the 7 prior goldens still pass; clippy; fmt; commit** `feat(streams-dsl): KTable-KTable join (inner/left/outer) execution`.

---

## Task 3: Capture + golden frame

**Files:** `tests/jvm-capture/.../Capture.java`; `tests/testdata/golden/dsl/ktable_ktable_join.topology.json` (NEW); `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: capture FIRST.** Add `ktableKtableJoin` to `Capture.java`:
  `builder.table("a", Materialized.as("sa")).join(builder.table("b", Materialized.as("sb")), (va, vb) -> va + vb).toStream().to("out");` (String serdes, app id "app", optimization=all). Run `cd crates/client-streams/tests/jvm-capture && ./run.sh --gradle` (Docker Kafka-Streams 4.1). Commit `tests/testdata/golden/dsl/ktable_ktable_join.topology.json`. NOTE: subtopology count (expect 1); `source_topics` (`["a","b"]`); `copartition_groups` indices; `state_changelog_topics` (both stores — `a`/`b` via source reuse, or `app-sa-changelog`/`app-sb-changelog`); confirm NO result changelog. If Docker capture fails, report BLOCKED with the exact error — do NOT fabricate.

- [ ] **Step 2: failing golden test** — `tests/dsl_golden_frame.rs`:
```rust
#[test]
fn ktable_ktable_join_matches_jvm() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let ta = b.table::<String,String,_,_>("a", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sa"));
    let tb = b.table::<String,String,_,_>("b", Consumed::with(StringSerde, StringSerde), Materialized::with(StringSerde, StringSerde).as_store("sb"));
    ta.join(&tb, |va: &String, vb: &String| format!("{va}{vb}")).to_stream().to("out", Produced::with(StringSerde, StringSerde));
    drop(ta); drop(tb);
    // use build OR build_optimized to match the fixture (the table changelog reuse under optimization=all may require build_optimized — match the captured fixture).
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "ktable_ktable_join");
}
```
Use `build_optimized` if the fixture's changelogs are the reused source topics (`a`/`b`); use `build` if they're `app-sa-changelog`/`app-sb-changelog`. Match the fixture.

- [ ] **Step 3: iterate** the lowering (store connections, copartition members, subtopology placement) until `ktable_ktable_join_matches_jvm` byte-matches. If A's or B's changelog lands in the wrong subtopology, the store-connection union is off — verify `connect_processor_store(join_this, b_store)` + `connect_processor_store(join_other, a_store)` + the merger preds union everything; add a connection if the fixture shows the join connected to its own side's store too.

- [ ] **Step 4: run → golden PASS (+ 7 prior byte-identical); clippy; fmt; commit** `feat(streams-dsl): KTable-KTable join golden frame`.

---

## Task 4: Docs + final verification

**Files:** `lib.rs`.

- [ ] **Step 1:** add a short `KTable-KTable join` prose note to `lib.rs` docs (`KTable::join`/`left_join`/`outer_join` against another materialized KTable; a change on either side recomputes; result is an unmaterialized KTable; inputs must be copartitioned + materialized). No new doctest.
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` (join-processor units + inner/left/outer exec + ktable_ktable_join golden + the 8 golden frames total [7 prior byte-identical + ktable_ktable_join] + all prior tests + doctests); `cargo fmt -p crabka-client-streams -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-dsl): KTable-KTable join note + #4c-iii verification`.

---

## Self-review

**Spec coverage:** §3 dual-processor+merger → Tasks 1,2. §4 result rule + processors → Task 1. §5 DSL ops + lowering (connect_processor_store + copartition + unmaterialized result) → Task 2. §6 capture + golden → Task 3. §7 testing → Tasks 1-4. §8 success criteria → Task 4. ✓

**Empirical-fixture note (not a placeholder):** Task 3's subtopology placement + copartition indices + changelog names + build-vs-build_optimized are validated against the **captured** `ktable_ktable_join` JVM fixture (Step 1 captures first). The byte-exact bits are pinned by the fixture.

**Correction vs spec:** the spec said `connect_processor_store ×4`; this plan uses **×2** (each join processor connects to the store it reads — the other side's), since the merger's predecessor edges union the rest into one subtopology. Task 3 iterates against the fixture and adds a connection only if the changelog placement requires it.

**Type consistency:** `JoinKind`+`result()`+`KTableKTableJoinThis/OtherProcessor` (T1) → `join`/`left_join`/`outer_join`+`join_impl` wrap the joiner to outer form `Fn(Option<&VA>,Option<&VB>)->VR` + lower 3 nodes reusing `MergeProcessor` for the merger (T2) → golden (T3). `Change<VR>` is the inter-node value of the join processors + merger; the result `KTable<K,VR>` roots at the merger (unmaterialized). Consistent.

**Known risk:** the merger reuses `MergeProcessor<K, Change<VR>>` — confirm `MergeProcessor` forwards `Record` unchanged regardless of `V` (it does). The thunk id-order (this < other < merge) guarantees the merger's parent handles are populated when it lowers.
