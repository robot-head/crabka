# KIP-1071 Streams Client #4c-i — KTable `Change<old,new>` propagation + `to_table` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a KTable internally a `Change<old,new>` change-stream (fixing `filter` tombstones + correct downstream KTable ops), and add `KStream::to_table`.

**Architecture:** Every KTable node forwards `Record<K, Change<V>>` (`Change<V> { old: Option<V>, new: Option<V> }`); **state stores still hold `V`** (only the inter-node forwarded value is `Change`), so the changelog + wire topology are byte-unchanged. Boundary processors convert: source/aggregate/`to_table` emit `Change`; `to_stream` extracts `new`. `to_table` adds a materialized store → one new golden frame.

**Tech Stack:** Rust 2024; extends #4's `dsl/` module. JVM capture via the existing Docker Kafka-Streams 4.1 harness.

**Spec:** `docs/superpowers/specs/2026-06-04-kip-1071-streams-client-4c-change-design.md`.
**Branch:** `streams-4c-change` (stacked on `worktree-streams-4-dsl`; worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`).

---

## Current shapes (verbatim — what we refactor)

All KTable processors currently forward `Record<K, V>` (new-value only):
- `KTableSourceProcessor<K,V>: Processor<K,V,K,V>` — `store.put(k,v); forward(k,v)`.
- `KTableToStreamProcessor<K,V>: Processor<K,V,K,V>` — `forward(r)`.
- `KTableMapValuesProcessor<K,V,V2,F>: Processor<K,V,K,V2>` — materialized map.
- `KTableMapValuesViewProcessor<K,V,V2,F>: Processor<K,V,K,V2>` — non-materialized.
- `KTableFilterProcessor<K,V,P>: Processor<K,V,K,V>` — matching → put+forward; non-matching → `store.delete`, **no forward**.
- `KStreamAggregateProcessor<K,V,VA,I,A>: Processor<K,V,K,VA>` — `forward(k, new)`.
- `KStreamReduceProcessor<K,V,R>: Processor<K,V,K,V>` — `forward(k, new)`.

The lowering thunks (in `dsl/builder.rs` `table()`, `dsl/kgrouped.rs` `count/reduce/aggregate`, `dsl/ktable.rs` `map_values/filter/to_stream`) instantiate these via `add_processor::<KIn,VIn,KOut,VOut,_,_,_>(name, supplier, [parent])`, reconstructing the parent handle with `NodeHandle::<K, VParent>::from_name(state.handle_name[&parent_id].clone())`. **These thunks must change `VParent`/`VOut` to the `Change<…>` types so connected nodes agree at runtime** (the erased graph forwards `Box<dyn Any>`; a type mismatch is a runtime `ProcessorError::Downcast`, not a compile error — so the existing execution tests are the consistency gate).

## File structure

```
dsl/processors/change.rs   NEW — Change<V>
dsl/processors/table.rs    refactor KTableSource/ToStream/MapValues/View/Filter to Change
dsl/processors/aggregate.rs refactor KStreamAggregate/Reduce to forward Change
dsl/processors/mod.rs      + pub(crate) mod change;
dsl/builder.rs             table() thunk → Change types
dsl/kgrouped.rs            count/reduce/aggregate thunks → Change types
dsl/ktable.rs              map_values/filter/to_stream thunks → Change types
dsl/kstream.rs             + to_table() op + thunk
tests/dsl_execution.rs     + tombstone-propagation + to_table exec tests
tests/dsl_golden_frame.rs  + to_table golden
tests/jvm-capture/.../Capture.java + to_table topology
tests/testdata/golden/dsl/to_table.topology.json  NEW fixture
lib.rs                     Change/to_table doc note
```

**Batching:** sequential (single crate; Tasks 2–3 are one interconnected refactor). Task 4's capture (`jvm-capture/` + `testdata`) is independent and may run alongside Task 2–3 if a separate agent owns only those paths.

---

## Task 1: `Change<V>` type

**Files:** Create `dsl/processors/change.rs`; modify `dsl/processors/mod.rs`.

- [ ] **Step 1: failing test** — append to `dsl/processors/change.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn change_update_and_tombstone() {
        let upd = Change::update(Some(1), 2);
        check!(upd.old == Some(1));
        check!(upd.new == Some(2));
        check!(!upd.is_tombstone());
        let tomb: Change<i64> = Change::tombstone(Some(5));
        check!(tomb.new.is_none());
        check!(tomb.is_tombstone());
        // map applies to both sides
        let mapped = Change::update(Some(1), 2).map(|v| v.to_string());
        check!(mapped.old == Some("1".to_string()));
        check!(mapped.new == Some("2".to_string()));
    }
}
```

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib dsl::processors::change`

- [ ] **Step 3: implement** (prepend above the test module):
```rust
//! `Change<V>` — the (old, new) value a `KTable` propagates internally.
//! `new == None` is a tombstone (the key was deleted / stopped matching).
//! State stores hold `V`; only the inter-node forwarded value is `Change<V>`.

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Change<V> {
    pub old: Option<V>,
    pub new: Option<V>,
}

impl<V> Change<V> {
    pub fn update(old: Option<V>, new: V) -> Self {
        Self { old, new: Some(new) }
    }
    pub fn tombstone(old: Option<V>) -> Self {
        Self { old, new: None }
    }
    pub fn is_tombstone(&self) -> bool {
        self.new.is_none()
    }
    /// Map both sides through `f` (used by KTable `map_values`).
    pub fn map<V2>(self, f: impl Fn(&V) -> V2) -> Change<V2> {
        Change { old: self.old.as_ref().map(&f), new: self.new.as_ref().map(&f) }
    }
}
```
Add `pub(crate) mod change;` to `dsl/processors/mod.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): Change<V> type`.

---

## Task 2: Refactor KTable processors to `Change` (the atomic refactor)

**Files:** Modify `dsl/processors/table.rs`, `dsl/processors/aggregate.rs`, `dsl/builder.rs`, `dsl/kgrouped.rs`, `dsl/ktable.rs`.

This switches every KTable processor's inter-node value type to `Change<V>` and updates the lowering thunks so connected nodes agree. **Gate: the existing execution tests (`dsl_count_executes`, `dsl_reduce_executes`, `dsl_table_map_values_executes`, `dsl_ktable_filter_executes`, etc.) and the 5 golden frames all still pass.**

- [ ] **Step 1: refactor the processors** (`table.rs` + `aggregate.rs`). Use `crate::dsl::processors::change::Change`. The stores stay `<K, V>` (hold the materialized value). New signatures + bodies:

```rust
// KTableSourceProcessor: stream V in → Change<V> out.
impl<K, V> Processor<K, V, K, Change<V>> for KTableSourceProcessor<K, V>
where K: Any+Send+Clone, V: Any+Send+Clone {
    fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,K,Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("KTable source key");
        let store = ctx.get_state_store::<K, V>(&self.store_name).expect("source store");
        let old = store.get(&key);
        store.put(key.clone(), r.value.clone());
        ctx.forward(Record::new(Some(key), Change::update(old, r.value), r.timestamp));
    }
}

// KTableToStreamProcessor: Change<V> in → V out (extract new; DROP tombstones).
impl<K, V> Processor<K, Change<V>, K, V> for KTableToStreamProcessor<K, V>
where K: Any+Send+Clone, V: Any+Send+Clone {
    fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,K,V>, r: Record<K, Change<V>>) {
        if let Some(new) = r.value.new {
            ctx.forward(Record::new(r.key, new, r.timestamp));
        } // tombstone (new == None) → dropped (documented; spec §2 non-goal)
    }
}

// KTableMapValuesProcessor (materialized): Change<V> in → Change<V2> out.
impl<K, V, V2, F> Processor<K, Change<V>, K, Change<V2>> for KTableMapValuesProcessor<K, V, V2, F>
where K: Any+Send+Clone, V: 'static, V2: Any+Send+Clone, F: Fn(&V)->V2 + Send + 'static {
    fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,K,Change<V2>>, r: Record<K, Change<V>>) {
        let key = r.key.expect("map_values key");
        let mapped = r.value.map(|v| (self.f)(v));
        let store = ctx.get_state_store::<K, V2>(&self.store_name).expect("mv store");
        match &mapped.new {
            Some(nv) => { store.put(key.clone(), nv.clone()); }
            None => { store.delete(&key); }
        }
        ctx.forward(Record::new(Some(key), mapped, r.timestamp));
    }
}

// KTableMapValuesViewProcessor (non-materialized): Change<V> in → Change<V2> out, no store.
impl<K, V, V2, F> Processor<K, Change<V>, K, Change<V2>> for KTableMapValuesViewProcessor<K, V, V2, F>
where K: Any+Send+Clone, V: 'static, V2: Any+Send+Clone, F: Fn(&V)->V2 + Send + 'static {
    fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,K,Change<V2>>, r: Record<K, Change<V>>) {
        let mapped = r.value.map(|v| (self.f)(v));
        ctx.forward(Record::new(r.key, mapped, r.timestamp));
    }
}

// KTableFilterProcessor: Change<V> in → Change<V> out (EMITS tombstones).
impl<K, V, P> Processor<K, Change<V>, K, Change<V>> for KTableFilterProcessor<K, V, P>
where K: Any+Send+Clone, V: Any+Send+Clone, P: Fn(&K,&V)->bool + Send + 'static {
    fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,K,Change<V>>, r: Record<K, Change<V>>) {
        let key = r.key.expect("filter key");
        // Apply the predicate to each side; a side that fails becomes None.
        let old_p = r.value.old.filter(|v| (self.predicate)(&key, v));
        let new_p = r.value.new.filter(|v| (self.predicate)(&key, v));
        let store = ctx.get_state_store::<K, V>(&self.store_name).expect("filter store");
        match &new_p {
            Some(nv) => { store.put(key.clone(), nv.clone()); }
            None => { store.delete(&key); }
        }
        // Forward only when something changed downstream: new_p present, OR a
        // tombstone for a row that previously passed (old_p present).
        if new_p.is_some() || old_p.is_some() {
            ctx.forward(Record::new(Some(key), Change { old: old_p, new: new_p }, r.timestamp));
        }
    }
}
```
For `aggregate.rs`: `KStreamAggregateProcessor: Processor<K, V, K, Change<VA>>` — `let old = store.get(&key); let new = (self.agg)(&key,&r.value, old.clone().unwrap_or_else(||(self.init)())); store.put(key.clone(), new.clone()); forward(Record::new(Some(key), Change::update(old, new), r.timestamp))`. `KStreamReduceProcessor: Processor<K, V, K, Change<V>>` — same shape (old from store, new = reduce, forward `Change::update(old, new)`).

- [ ] **Step 2: update the lowering thunks** so connected KTable nodes use the `Change` types. In each thunk, the value type the node FORWARDS is now `Change<V>`; reconstruct the parent handle with the parent's ACTUAL forwarded type:
  - `builder.rs table()`: `add_processor::<K, V, K, Change<V>, _, _, _>(name, || KTableSourceProcessor{...}, [src_handle])`; record the node's handle as carrying `Change<V>`.
  - `kgrouped.rs count/aggregate`: `add_processor::<K, V, K, Change<VA>, ...>`; `reduce`: `::<K, V, K, Change<V>, ...>`.
  - `ktable.rs map_values/filter`: parent handle is `NodeHandle::<K, Change<Vparent>>::from_name(...)`; `add_processor::<K, Change<Vparent>, K, Change<Vout>, ...>`.
  - `ktable.rs to_stream`: parent `NodeHandle::<K, Change<V>>`; `add_processor::<K, Change<V>, K, V, ...>` (extract new).
  The rule: a `KTable<K,V>` handle's underlying graph node forwards `Change<V>`; any op consuming a KTable reconstructs its parent as `NodeHandle<K, Change<Vparent>>`. The public handle types are unchanged; only the thunks' explicit type args change.

- [ ] **Step 3: update the processor unit tests** in `table.rs`/`aggregate.rs` to the `Change` shapes (the forwarded value is now `Change<V>`; downcast to `Change<i64>` etc. and assert `.new`/`.old`; the filter test asserts a tombstone is forwarded for a previously-passing row that now fails).

- [ ] **Step 4: run the GATE.** `cargo test -p crabka-client-streams` — the existing execution tests (`dsl_count_executes`, `dsl_reduce_executes`, `dsl_table_map_values_executes`, etc.) MUST pass (they go `…→to_stream→to`, so `Change` is extracted to the same output values), AND the 5 golden frames MUST byte-match (topology unchanged). If a `…Downcast` runtime error appears, a thunk's `Change` type doesn't match a connected node — fix the type args. `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt`.

- [ ] **Step 5: commit** `refactor(streams-dsl): KTable processors propagate Change<old,new>`.

---

## Task 3: Tombstone-propagation execution tests

**Files:** Modify `tests/dsl_execution.rs`.

- [ ] **Step 1: failing tests** — add:
```rust
#[test]
fn dsl_ktable_filter_tombstone_propagates_downstream() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, StringSerde};
    let b = StreamsBuilder::new();
    // table → filter(v != "drop") → map_values_materialized(identity into "view")
    b.table::<String,String,_,_>("in", Consumed::with(StringSerde, StringSerde),
            Materialized::with(StringSerde, StringSerde).as_store("src"))
        .filter(|_k: &String, v: &String| v != "drop",
            Materialized::with(StringSerde, StringSerde).as_store("filt"))
        .map_values_materialized(|v: &String| v.clone(),
            Materialized::with(StringSerde, StringSerde).as_store("view"));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "keep".to_string(), 0);
    // "k" present everywhere
    assert_eq!(d.get_key_value_store::<String,String>("view").unwrap().get(&"k".to_string()), Some("keep".to_string()));
    // now update "k" to a value that fails the filter → tombstone must delete it from BOTH filt + view stores
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "drop".to_string(), 1);
    assert_eq!(d.get_key_value_store::<String,String>("filt").unwrap().get(&"k".to_string()), None);
    assert_eq!(d.get_key_value_store::<String,String>("view").unwrap().get(&"k".to_string()), None);
}
```
(ADJUST `table`/`filter`/`map_values_materialized` to the real signatures. The point: a row that stops matching the filter is **deleted from the downstream materialized store** — proving the tombstone propagated, not just dropped at the filter.)

- [ ] **Step 2: run → it should PASS if Task 2 is correct** (the tombstone propagates). If it FAILS (view store still has "k"), the filter isn't forwarding tombstones or map_values isn't applying them — fix Task 2.

- [ ] **Step 3: commit** `test(streams-dsl): KTable filter tombstone propagation`.

---

## Task 4: `KStream::to_table` + golden frame

**Files:** Modify `dsl/kstream.rs`, `dsl/processors/table.rs`, `dsl/processors/mod.rs`; `tests/jvm-capture/.../Capture.java`, `tests/testdata/golden/dsl/to_table.topology.json` (NEW), `tests/dsl_golden_frame.rs`, `tests/dsl_execution.rs`.

- [ ] **Step 1: capture the JVM fixture FIRST** (ground truth). Add a `to_table` method to `tests/jvm-capture/.../Capture.java`:
  `builder.stream("in").toTable(Materialized.as("store")).toStream().to("out");` (Serdes.String). Run the harness (`tests/jvm-capture/run.sh` — Docker Kafka-Streams 4.1; see its README) → commit `tests/testdata/golden/dsl/to_table.topology.json`. Note the store name + counter index it produces (e.g. `KSTREAM-TOTABLE-STATE-STORE-000…`) and whether it adds a repartition (it should NOT for an unchanged key).

- [ ] **Step 2: failing golden test** — add to `tests/dsl_golden_frame.rs`:
```rust
#[test]
fn to_table_matches_jvm() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Materialized, Produced, StringSerde};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .to_table(Materialized::with(StringSerde, StringSerde).as_store("store"))
        .to_stream()
        .to("out", Produced::with(StringSerde, StringSerde));
    let wire = b.build("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "to_table");
}
```

- [ ] **Step 3: implement** `KStreamToTableProcessor<K,V>: Processor<K, V, K, Change<V>>` in `table.rs` (per record: `old = store.get(k); store.put(k, v); forward(Change::update(old, v))`). `KStream::to_table<KS,VS>(Materialized<KS,VS>) -> KTable<K,V>` in `kstream.rs`: mint store name (`Materialized.store_name` or auto from the counter — match the fixture's `KSTREAM-TOTABLE-STATE-STORE-` prefix; add the const to `names.rs`); record a node + thunk: `add_processor::<K, V, K, Change<V>, _, _, _>(name, || KStreamToTableProcessor{store}, [parent])` + `add_state_store(store_name, ks, vs, [name])`; return `KTable<K,V>`. Iterate the store-name/counter until `to_table_matches_jvm` byte-matches.

- [ ] **Step 4: execution test** — add to `tests/dsl_execution.rs` `dsl_to_table_executes`: `stream → to_table(store) → to_stream → to("out")`; pipe `(k, "a")` then `(k, "b")`; assert outputs `"a"` then `"b"` and the store holds `"b"` (last-write-wins).

- [ ] **Step 5: run → both PASS; clippy; fmt; commit** `feat(streams-dsl): KStream::to_table + golden frame`.

---

## Task 5: Docs + final verification (regression gate)

**Files:** Modify `lib.rs`.

- [ ] **Step 1:** add a short `## KTable change semantics` note to `lib.rs` docs (a sentence on tombstones + a `to_table` mention; a runnable doctest is optional — the existing DSL doctest already covers count). Keep it brief.
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` — ALL green: the new `change` unit + tombstone + `to_table` exec + `to_table` golden + **the 5 existing golden frames byte-identical** + all #1/#2/#3/#4 execution/integration/doctests. `cargo fmt -p crabka-client-streams -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-dsl): KTable change-semantics note + #4c-i verification`.

---

## Self-review

**Spec coverage:** §3 Change model → Task 1. §4 refactored processors (source/aggregate/reduce/mapValues/filter/toStream) → Task 2. tombstone propagation → Tasks 2+3. §5 to_table → Task 4. §6 capture + to_table golden + 5-golden regression → Tasks 4, 5. §7 testing → Tasks 1–5. §8 success criteria → Task 5. ✓

**Placeholder note (not a placeholder):** Task 4's `to_table` store-name/counter index is validated against the **captured** JVM fixture (Step 1 captures it first), per the program's empirical-capture strategy — the exact name is pinned by the fixture, not guessed. Flagged, not silent.

**Type consistency:** `Change<V>` (T1) → processor sigs `Processor<K, Change<V>, K, Change<V2>>` + stores `<K,V>` (T2) → thunks reconstruct `NodeHandle<K, Change<Vparent>>` (T2) → `to_table`/`KStreamToTableProcessor: Processor<K,V,K,Change<V>>` (T4). The public `KStream<K,V>`/`KTable<K,V>` handle types are unchanged throughout; only the processors' + thunks' inter-node value type becomes `Change<…>`. Consistent.

**Known risk:** Task 2 is an atomic multi-file refactor where a thunk `Change`-type mismatch is a *runtime* downcast error, not a compile error — the existing execution + golden tests are the gate (Step 4). The implementer must run the full suite, not just compile.
