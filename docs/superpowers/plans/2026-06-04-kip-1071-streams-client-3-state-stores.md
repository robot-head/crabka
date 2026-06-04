# KIP-1071 Streams Client #3 — State Stores + Changelog — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an in-memory `KeyValueStore<K,V>` with changelog backing (produce-on-write + restore-on-assignment), per-task store registry, `ProcessorContext::get_state_store`, and the `Processor::init`/`close` lifecycle — unlocking stateful processing.

**Architecture:** Stores are typed (`KeyValueStore<K,V>`) but held erased (`Box<dyn StateStore>`, downcast via `Any`) in a per-task registry the graph driver lends to each `ProcessorContext` — the same erased+downcast pattern as #2a records. The in-memory store keeps `HashMap<Bytes,Bytes>` + **boxed** serdes + a changelog buffer; `put`/`delete` buffer changelog entries the `StreamTask` produces within the existing flush-before-commit barrier; stores are restored by replaying the changelog on task assignment.

**Tech Stack:** Rust 2024, `bytes`, `std::any`. Builds on merged #2 (Processor API + `StreamTask` runtime). No new deps.

**Spec:** `docs/superpowers/specs/2026-06-04-kip-1071-streams-client-3-state-stores-design.md`.
**Branch:** `claude/streams-3-state-stores` (stacked on #2b `claude/streams-2b-runtime`).

---

## File structure

```
crates/client-streams/src/
  store/                  NEW
    mod.rs                re-exports (StateStore, KeyValueStore)
    api.rs                StateStore (Any + name/flush/close/as_any_mut) + KeyValueStore<K,V>
    memory.rs             InMemoryKeyValueStore<K,V> (HashMap<Bytes,Bytes> + boxed serdes + changelog buffer)
    registry.rs           StoreRegistry (name → Box<dyn StateStore>) + get_state_store downcast helper
  processor/erased.rs     +Dispatch.stores: &mut StoreRegistry
  processor/api.rs        +ProcessorContext::get_state_store::<K2,V2>(name)
  processor/node.rs       +ErasedNode::init/close (default no-op); ProcessorNode forwards to Processor::init/close
  processor/graph.rs      +Graph.stores; pipe lends &mut stores; init_processors/close_processors/drain_changelogs
  topology/builder.rs     add_state_store typed (serdes) + StoreFactory; instantiate builds the registry
  runtime/task.rs         StreamTask::restore + changelog produce in process_once + init/close
  runtime/thread.rs       apply_assignment: restore+init on create, close on remove
  test_driver.rs          +init_processors + get_key_value_store
  lib.rs                  +pub mod store; re-exports
  tests/state_store_integration.rs  NEW: stateful + restart-restore
```

## Reference (current shapes — verbatim)

- `processor/erased.rs`: `pub(crate) struct Dispatch<'a> { pub buffer: &'a mut VecDeque<(usize, ErasedRecord)>, pub children: &'a [usize], pub output: &'a mut Vec<OutputRecord>, pub record_ctx: &'a RecordContext }`.
- `processor/api.rs`: `pub struct ProcessorContext<'ctx, 'd, KOut, VOut> { dispatch: &'ctx mut Dispatch<'d>, _pd }`; `pub(crate) fn new(dispatch: &'ctx mut Dispatch<'d>)`; `forward`; `record_context`. `Processor::{init(&mut self, &mut ProcessorContext), process, close}`.
- `processor/node.rs`: `pub(crate) trait ErasedNode: Send { fn process(&mut self, &mut Dispatch<'_>, ErasedRecord) -> Result<(), ProcessorError>; }`. `ProcessorNode<KIn,VIn,KOut,VOut> { name, inner: Box<dyn Processor<...>> }`.
- `processor/graph.rs`: `pub(crate) struct Graph { pub nodes: Vec<Box<dyn ErasedNode>>, pub children: Vec<Vec<usize>>, pub sources: Vec<GraphSource>, pub output: Vec<OutputRecord> }`; `pipe(&mut self, topic, key, value, ts)`; `take_output`. `pipe`'s drain loop already does `let node = &mut self.nodes[idx]; let out = &mut self.output; let mut d = Dispatch { ... };` (disjoint fields).
- `topology/builder.rs`: `add_state_store<S,I,T>(name, processors)` calls `self.reg.add_store(&name, procs)`. `BuiltTopology::instantiate(&self) -> Result<Graph, ProcessorError>` builds nodes from `NodeSpec` + factories. `NodeRegistry.stores: Vec<(String, Vec<String>)>` records (store_name, connected_processors).
- `processor/serde.rs`: `pub trait Serde<T>: Send + Sync + 'static { fn serialize(&self, &T)->Bytes; fn deserialize(&self, &[u8])->Result<T,SerdeError>; }` (object-safe).
- `runtime/io.rs`: `RecordFetcher::fetch(&self, topic, partition, offset) -> Result<FetchBatch, _>`; `FetchBatch { records: Vec<FetchedRec{offset,key,value,timestamp}> }`. `RecordProducer::{send(topic,key,value), flush}`.

---

## Task 1: Store traits

**Files:** Create `store/api.rs`, `store/mod.rs`; modify `lib.rs`.

- [ ] **Step 1: scaffold.** `store/mod.rs`:
```rust
//! State stores + changelog backing (sub-project #3).
pub mod api;
pub use api::{KeyValueStore, StateStore};
```
`lib.rs`: add `pub mod store;` and `pub use store::{KeyValueStore, StateStore};`.

- [ ] **Step 2: implement** `store/api.rs` (no separate test — exercised by Task 2):
```rust
//! Store traits. `StateStore` is object-safe (held erased in the registry);
//! `KeyValueStore<K,V>` is the typed get/put/delete surface.

use std::any::Any;

/// Lifecycle + identity + changelog hooks for any store. Object-safe so
/// heterogeneous stores live in one registry; `as_any_mut` enables the typed
/// downcast in `get_state_store`. The changelog methods are on the trait (every
/// #3 store is changelog-logged) so the erased registry can restore/drain via
/// `&mut dyn StateStore` without naming the concrete type.
pub trait StateStore: Any + Send {
    fn name(&self) -> &str;
    /// Flush pending state (no-op for in-memory — the changelog is durability).
    fn flush(&mut self);
    fn close(&mut self);
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// The store's changelog topic (`<app>-<store>-changelog`).
    fn changelog_topic(&self) -> &str;
    /// Drain buffered changelog entries (key bytes, value bytes or None=tombstone).
    fn take_changelog(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>)>;
    /// Apply a changelog record during restore (updates state, does NOT re-log).
    fn apply_changelog(&mut self, key: bytes::Bytes, value: Option<bytes::Bytes>);
    /// Toggle changelog logging (off during restore, on during processing).
    fn set_logging(&mut self, on: bool);
}

/// A keyed store. Implemented by the in-memory store; the typed view a processor
/// gets from [`ProcessorContext::get_state_store`](crate::ProcessorContext).
pub trait KeyValueStore<K, V>: StateStore {
    fn get(&self, key: &K) -> Option<V>;
    fn put(&mut self, key: K, value: V);
    fn delete(&mut self, key: &K) -> Option<V>;
}
```

- [ ] **Step 3:** `cargo build -p crabka-client-streams` + clippy clean (add `#[allow(dead_code)]` if flagged before Task 2 uses them). fmt. Commit `feat(streams-client): state store traits`.

---

## Task 2: InMemoryKeyValueStore

**Files:** Create `store/memory.rs`; modify `store/mod.rs`.

- [ ] **Step 1: failing test** — append to `store/memory.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

    fn store() -> InMemoryKeyValueStore<String, i64> {
        InMemoryKeyValueStore::new("s".into(), Box::new(StringSerde), Box::new(I64Serde), "s-changelog".into())
    }

    #[test]
    fn put_get_delete_and_changelog_buffer() {
        let mut s = store();
        s.put("a".into(), 1);
        s.put("a".into(), 2);
        check!(s.get(&"a".to_string()) == Some(2));
        check!(s.delete(&"a".to_string()) == Some(2));
        check!(s.get(&"a".to_string()) == None);
        // changelog buffer: put a=1, put a=2, delete a (None)
        let cl = s.take_changelog();
        check!(cl.len() == 3);
        check!(cl[2].1.is_none()); // tombstone for the delete
        check!(s.take_changelog().is_empty()); // drained
    }

    #[test]
    fn apply_changelog_restores_without_re_logging() {
        let mut s = store();
        s.apply_changelog(b"k".to_vec().into(), Some(bytes::Bytes::from_static(&[0,0,0,0,0,0,0,7])));
        check!(s.get(&"k".to_string()) == Some(7));
        check!(s.take_changelog().is_empty()); // restore does NOT log
        s.apply_changelog(b"k".to_vec().into(), None); // tombstone
        check!(s.get(&"k".to_string()) == None);
    }
}
```

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib store::memory`

- [ ] **Step 3: implement** `store/memory.rs`:
```rust
//! In-memory key/value store: `HashMap<Bytes,Bytes>` + boxed serdes + a
//! changelog buffer. Serdes are boxed (not type params) so the concrete type is
//! `InMemoryKeyValueStore<K,V>` — what `get_state_store::<K,V>` downcasts to.

use std::any::Any;
use std::collections::HashMap;

use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::{KeyValueStore, StateStore};

pub struct InMemoryKeyValueStore<K, V> {
    name: String,
    changelog_topic: String,
    map: HashMap<Bytes, Bytes>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> InMemoryKeyValueStore<K, V> {
    #[must_use]
    pub fn new(name: String, key_serde: Box<dyn Serde<K>>, value_serde: Box<dyn Serde<V>>, changelog_topic: String) -> Self {
        Self { name, changelog_topic, map: HashMap::new(), key_serde, value_serde, changelog: Vec::new(), logging: true }
    }
}

impl<K: 'static, V: 'static> StateStore for InMemoryKeyValueStore<K, V> {
    fn name(&self) -> &str { &self.name }
    fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn changelog_topic(&self) -> &str { &self.changelog_topic }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> { std::mem::take(&mut self.changelog) }
    fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value { Some(v) => { self.map.insert(key, v); } None => { self.map.remove(&key); } }
    }
    fn set_logging(&mut self, on: bool) { self.logging = on; }
}

impl<K: 'static, V: 'static> KeyValueStore<K, V> for InMemoryKeyValueStore<K, V> {
    fn get(&self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(key);
        self.map.get(&kb).map(|vb| self.value_serde.deserialize(vb).expect("store value deserialize"))
    }
    fn put(&mut self, key: K, value: V) {
        let kb = self.key_serde.serialize(&key);
        let vb = self.value_serde.serialize(&value);
        self.map.insert(kb.clone(), vb.clone());
        if self.logging { self.changelog.push((kb, Some(vb))); }
    }
    fn delete(&mut self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(key);
        let prev = self.map.remove(&kb).map(|vb| self.value_serde.deserialize(&vb).expect("store value deserialize"));
        if self.logging { self.changelog.push((kb, None)); }
        prev
    }
}
```
Add to `store/mod.rs`: `pub mod memory;` + `pub use memory::InMemoryKeyValueStore;`. Re-export from `lib.rs`.

- [ ] **Step 4: run → PASS (2); clippy; fmt; commit** `feat(streams-client): in-memory KV store + changelog buffer`.

---

## Task 3: StoreRegistry + downcast

**Files:** Create `store/registry.rs`; modify `store/mod.rs`.

- [ ] **Step 1: failing test** — append to `store/registry.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::memory::InMemoryKeyValueStore;
    use crate::store::api::KeyValueStore;
    use assert2::check;

    #[test]
    fn register_and_downcast_typed_store() {
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(InMemoryKeyValueStore::<String, i64>::new(
            "counts".into(), Box::new(StringSerde), Box::new(I64Serde), "c-changelog".into())));
        let s = reg.get_kv::<String, i64>("counts").unwrap();
        s.put("x".into(), 5);
        check!(s.get(&"x".to_string()) == Some(5));
        // wrong types → None
        check!(reg.get_kv::<i64, i64>("counts").is_none());
        check!(reg.get_kv::<String, i64>("missing").is_none());
    }
}
```

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib store::registry`

- [ ] **Step 3: implement** `store/registry.rs`:
```rust
//! Per-task registry of erased stores + the typed downcast used by
//! `get_state_store`.

use std::collections::HashMap;

use crate::store::api::{KeyValueStore, StateStore};
use crate::store::memory::InMemoryKeyValueStore;

#[derive(Default)]
pub(crate) struct StoreRegistry {
    stores: HashMap<String, Box<dyn StateStore>>,
}

impl StoreRegistry {
    pub fn insert(&mut self, store: Box<dyn StateStore>) {
        self.stores.insert(store.name().to_string(), store);
    }

    /// Typed mutable access: downcast the erased store to the in-memory KV store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_kv<K: 'static, V: 'static>(&mut self, name: &str) -> Option<&mut dyn KeyValueStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store.as_any_mut().downcast_mut::<InMemoryKeyValueStore<K, V>>()?;
        Some(concrete as &mut dyn KeyValueStore<K, V>)
    }

    /// All stores (for restore + changelog drain), as concrete in-memory stores
    /// is not possible erased — expose name + a per-store closure instead.
    pub fn names(&self) -> Vec<String> { self.stores.keys().cloned().collect() }

    /// Mutable erased access by name (for restore/drain via store-specific calls).
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Box<dyn StateStore>> {
        self.stores.get_mut(name)
    }

    pub fn is_empty(&self) -> bool { self.stores.is_empty() }
}
```
NOTE: restore/drain (Tasks 4/7) need to call `take_changelog`/`apply_changelog`/`changelog_topic` which are on the concrete `InMemoryKeyValueStore`, not `StateStore`. Add those methods to the `StateStore` trait OR add a second object-safe trait `LoggedStore { fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)>; fn apply_changelog(&mut self, Bytes, Option<Bytes>); fn changelog_topic(&self) -> &str; fn set_logging(&mut self, bool); }` that `StateStore: LoggedStore` (or a supertrait), impl'd by the in-memory store. **Choose: make these methods part of `StateStore`** (every #3 store is logged) — add `take_changelog`/`apply_changelog`/`changelog_topic`/`set_logging` to the `StateStore` trait in Task 1's `api.rs` and impl on the in-memory store (move them from inherent methods to the trait). Update Task 1/2 accordingly when you reach here, OR add a `pub(crate) trait LoggedStore`. Pick the cleaner one; the registry then exposes `get_mut(name)` returning `&mut dyn StateStore` with those methods available.

Add `pub(crate) mod registry;` to `store/mod.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-client): store registry + typed downcast`.

---

## Task 4: ProcessorContext store access + ErasedNode init/close + Graph integration

**Files:** Modify `processor/erased.rs`, `processor/api.rs`, `processor/node.rs`, `processor/graph.rs`.

This wires stores into the erased graph. The biggest task.

- [ ] **Step 1: failing test** — append to `processor/graph.rs` tests a stateful processor that counts via a store:
```rust
    #[test]
    fn stateful_processor_accumulates_via_store() {
        use crate::processor::api::{Processor, ProcessorContext};
        use crate::processor::node::{ProcessorNode, SinkNode, SourceNode};
        use crate::processor::record::Record;
        use crate::processor::serde::{I64Serde, StringSerde};
        use crate::store::memory::InMemoryKeyValueStore;
        use crate::store::registry::StoreRegistry;

        struct Counter;
        impl Processor<String, String, String, i64> for Counter {
            fn process(&mut self, ctx: &mut ProcessorContext<String, i64>, r: Record<String, String>) {
                let store = ctx.get_state_store::<String, i64>("counts").expect("counts");
                let n = store.get(&r.value).unwrap_or(0) + 1;
                store.put(r.value.clone(), n);
                ctx.forward(Record::new(Some(r.value), n, r.timestamp));
            }
        }
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(InMemoryKeyValueStore::<String, i64>::new(
            "counts".into(), Box::new(StringSerde), Box::new(I64Serde), "counts-changelog".into())));
        let src = SourceNode::new("src".into(), StringSerde, StringSerde);
        let proc = Box::new(ProcessorNode::new("c".into(), || Box::new(Counter))) as Box<dyn ErasedNode>;
        let sink = Box::new(SinkNode::new("out".into(), "out".into(), StringSerde, I64Serde)) as Box<dyn ErasedNode>;
        let mut graph = Graph {
            nodes: vec![proc, sink], children: vec![vec![1], vec![]],
            sources: vec![GraphSource { topic: "in".into(), deserialize: Box::new(move |k,v,t| src.deserialize(k,v,t)), children: vec![0] }],
            output: Vec::new(), stores,
        };
        graph.pipe("in", None, b"a", 0).unwrap();
        graph.pipe("in", None, b"a", 1).unwrap();
        let out = graph.take_output();
        // second "a" → count 2; the in-i64 output value is 8 BE bytes of 2
        check!(out.last().unwrap().value.as_ref().unwrap().as_ref() == [0,0,0,0,0,0,0,2]);
    }
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.**
1. `erased.rs` — add to `Dispatch`: `pub stores: &'a mut crate::store::registry::StoreRegistry,`.
2. `api.rs` — add to `ProcessorContext` impl:
```rust
    /// Access a connected state store, typed. `None` if absent or the K/V types
    /// don't match. Fetch it per-record (do not hold across `process` calls).
    pub fn get_state_store<K2: 'static, V2: 'static>(&mut self, name: &str)
        -> Option<&mut dyn crate::store::api::KeyValueStore<K2, V2>> {
        self.dispatch.stores.get_kv::<K2, V2>(name)
    }
```
3. `node.rs` — add to `ErasedNode` (default no-op so source/sink inherit):
```rust
    fn init(&mut self, _dispatch: &mut Dispatch<'_>) -> Result<(), ProcessorError> { Ok(()) }
    fn close(&mut self) {}
```
   and impl on `ProcessorNode` (forward to the typed `Processor::init`/`close`): `init` builds a `ProcessorContext::new(dispatch)` and calls `self.inner.init(&mut ctx)`; `close` calls `self.inner.close()`.
4. `graph.rs`:
   - Add `pub stores: StoreRegistry` to `Graph` (and `use crate::store::registry::StoreRegistry;`).
   - In `pipe`'s drain loop, add `stores: &mut self.stores` to the `Dispatch { ... }` (a 4th disjoint self-field alongside `nodes[idx]`, `output`). Construct as `let stores = &mut self.stores;` before the loop body's borrow OR inside (it's a disjoint field from `nodes`/`output`/`children` — should compile; if the existing `mem::take(children)` + `node`/`out` locals pattern needs `stores` added as another local `let st = &mut self.stores;`, do that).
   - Add `init_processors(&mut self) -> Result<(), ProcessorError>`: for each node index, build a `Dispatch { buffer: &mut VecDeque::new(), children: &[], output: &mut Vec::new(), record_ctx: &placeholder, stores: &mut self.stores }` and call `node.init(&mut d)`. (placeholder `RecordContext { topic: String::new(), partition: -1, offset: -1, timestamp: -1 }`.) Same disjoint-field care.
   - Add `close_processors(&mut self)`: call `node.close()` for each.
   - Add `drain_changelogs(&mut self) -> Vec<(String /*changelog topic*/, bytes::Bytes /*key*/, Option<bytes::Bytes> /*value*/)>`: for each store name, `self.stores.get_mut(name)` → take its changelog (via the `StateStore`/`LoggedStore` methods from Task 3) → for each entry emit `(store.changelog_topic().to_string(), key, value)`.
   - Add `restore_apply(&mut self, store_name, key, value)` and `set_logging(on)` helpers delegating to the registry stores (for Task 7).

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-client): store access in ProcessorContext + graph init/close/changelog`.

---

## Task 5: Typed add_state_store + store factories + instantiate

**Files:** Modify `topology/builder.rs` (+ `processor/factory.rs` if factories live there); migrate the #2a builder test.

- [ ] **Step 1: failing test** — in `builder.rs` tests, build a stateful topology, instantiate, pipe, and assert the store accumulated:
```rust
    #[test]
    fn instantiate_builds_stores_and_processes_statefully() {
        use crate::processor::serde::I64Serde;
        struct Counter;
        impl crate::processor::api::Processor<String, String, String, i64> for Counter {
            fn process(&mut self, ctx: &mut crate::processor::api::ProcessorContext<String, i64>, r: crate::processor::record::Record<String, String>) {
                let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                let n = s.get(&r.value).unwrap_or(0) + 1;
                s.put(r.value.clone(), n);
                ctx.forward(crate::processor::record::Record::new(Some(r.value), n, r.timestamp));
            }
        }
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_state_store("counts", StringSerde, I64Serde, ["c"]);
        t.add_processor("c", || Box::new(Counter), ["src"]);
        t.add_sink("out", "out", ["c"], StringSerde, I64Serde);
        let built = t.build("app").unwrap();
        // wire topology still has the changelog topic (golden frame contract)
        check!(built.to_wire().subtopologies.iter().any(|s| s.state_changelog_topics.iter().any(|c| c.name == "app-counts-changelog")));
        let mut g = built.instantiate().unwrap();
        g.pipe("in", None, b"x", 0).unwrap();
        g.pipe("in", None, b"x", 1).unwrap();
        check!(g.take_output().last().unwrap().value.as_ref().unwrap().as_ref() == [0,0,0,0,0,0,0,2]);
    }
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.**
1. Replace `add_state_store<S,I,T>(name, processors)` with the typed form:
```rust
pub fn add_state_store<K, V, KS, VS>(&mut self, name: impl Into<String>, key_serde: KS, value_serde: VS, processors: impl IntoIterator<Item = impl Into<String>>) -> &mut Self
where K: 'static, V: 'static, KS: Serde<K> + Clone, VS: Serde<V> + Clone
{
    let name = name.into();
    let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
    self.reg.add_store(&name, procs);   // UNCHANGED structural side (wire topology)
    // record a store factory keyed by name; the changelog topic uses the app id at build().
    self.store_factories.insert(name, Box::new(move |app_id: &str, store_name: &str| {
        Box::new(InMemoryKeyValueStore::<K, V>::new(
            store_name.to_string(), Box::new(key_serde.clone()), Box::new(value_serde.clone()),
            format!("{app_id}-{store_name}-changelog"))) as Box<dyn StateStore>
    }) as StoreFactory);
    self
}
```
   where `Topology` gains `store_factories: HashMap<String, StoreFactory>` and `type StoreFactory = Box<dyn Fn(&str, &str) -> Box<dyn StateStore> + Send + Sync>`.
2. In `build()`, move the store factories into `BuiltTopology` (a new `store_factories: HashMap<String, StoreFactory>` field + the `(store_name -> connected_processor_names)` map from `reg.stores`). Keep the wire/grouping path untouched.
3. In `BuiltTopology::instantiate()`, after building the graph's nodes, build the `StoreRegistry`: for each store factory, instantiate `factory(&self.application_id, store_name)` and `registry.insert(...)`. Set `graph.stores = registry`. (For #2's whole-graph-per-task model, instantiate ALL stores into the one graph.)

- [ ] **Step 4: migrate #2a's builder test** `build_with_processor_store_and_repartition` — change `t.add_state_store("store", ["proc"])` → `t.add_state_store("store", StringSerde, StringSerde, ["proc"])` (the test only checks the wire changelog topic, which is unchanged). Grep for any other `add_state_store(` caller + migrate.

- [ ] **Step 5: run → PASS (incl. golden_frame unchanged); clippy; fmt; commit** `feat(streams-client): typed add_state_store + instantiate builds store registry`.

---

## Task 6: TopologyTestDriver store support

**Files:** Modify `test_driver.rs`.

- [ ] **Step 1: failing test** — `test_driver.rs` tests: a counting topology, pipe inputs, read outputs, and inspect the store:
```rust
    #[test]
    fn stateful_count_and_store_inspection() {
        use crate::processor::serde::I64Serde;
        struct Counter;
        impl Processor<String, String, String, i64> for Counter {
            fn process(&mut self, ctx: &mut ProcessorContext<String, i64>, r: Record<String, String>) {
                let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                let n = s.get(&r.value).unwrap_or(0) + 1; s.put(r.value.clone(), n);
                ctx.forward(Record::new(Some(r.value), n, r.timestamp));
            }
        }
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_state_store("counts", StringSerde, I64Serde, ["c"]);
        t.add_processor("c", || Box::new(Counter), ["src"]);
        t.add_sink("out", "out", ["c"], StringSerde, I64Serde);
        let mut d = TopologyTestDriver::new(&t.build("app").unwrap()).unwrap();
        d.pipe_input("in", &StringSerde, &StringSerde, None, "a".to_string(), 0);
        d.pipe_input("in", &StringSerde, &StringSerde, None, "a".to_string(), 1);
        check!(d.read_output("out", &StringSerde, &I64Serde) == Some((None, 1)));
        check!(d.read_output("out", &StringSerde, &I64Serde) == Some((None, 2)));
        let store = d.get_key_value_store::<String, i64>("counts").unwrap();
        check!(store.get(&"a".to_string()) == Some(2));
    }
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** in `test_driver.rs`:
- In `new`, after instantiating the graph, call `graph.init_processors()` (so `init` runs before piping).
- In `pipe_bytes`, after `graph.pipe` + collecting sink output, also call `graph.drain_changelogs()` and DROP the changelog entries (the test driver doesn't need a broker; restore is a no-op with fresh stores). (Or collect them into an output topic keyed by the changelog topic name — but simplest: drop them, since `get_key_value_store` inspects the live store directly.)
- Add `pub fn get_key_value_store<K: 'static, V: 'static>(&mut self, name: &str) -> Option<&mut dyn KeyValueStore<K, V>> { self.graph.stores.get_kv::<K, V>(name) }`. (`TopologyTestDriver` holds the single `Graph`; expose its registry.)

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-client): TopologyTestDriver store support + get_key_value_store`.

---

## Task 7: StreamTask restore + changelog produce + init/close

**Files:** Modify `runtime/task.rs`.

- [ ] **Step 1: failing test** — extend `runtime/task.rs` tests (reuse the #2b fakes: `OneShot` fetcher, `CollectProducer`, `MemStore`) with a stateful counter topology. Assert: (a) processing produces sink + **changelog** records to the producer; (b) `restore` from a scripted changelog seeds the store. Write a test `stateful_task_produces_changelog_and_restores`:
```rust
    // fetcher returns one record ("a") for ("in",0); producer collects all sends;
    // build a stateful counting topology + task; process_once → producer got both
    // an "out" record AND an "app-counts-changelog" record. Then a second task with
    // a fetcher scripted to return the changelog record on restore → store has the value.
```
(Mirror the #2b task tests; the counter topology comes from `built()` with a store. For restore: call `task.restore(&changelog_fetcher).await` where the fetcher returns the changelog batch for the changelog topic, then assert via the graph's store that the value is present — expose a test accessor or assert indirectly via processing.)

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** in `task.rs`:
- Add `restore(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError>`: for each store in `self.graph.stores`, set logging off, read its changelog topic (the store's `changelog_topic()`) partition = the task's partition, from offset 0 to the high-watermark (loop `fetcher.fetch` advancing the offset until an empty batch), calling `self.graph.restore_apply(store_name, key, value)` per record; set logging back on. (The task knows its partition from its assigned source partitions — use the partition index; all the task's stores share that partition.)
- In `process_once`, after the per-record graph run + sink produce, call `self.graph.drain_changelogs()` and `self.producer.send(topic, Some(key), value)` for each entry (value None = tombstone → send a null value). This happens before `commit` (flush barrier covers it).
- Add `init(&mut self) -> Result<(), StreamsClientError>`: `self.graph.init_processors().map_err(runtime)?`.
- Add `close_processors(&mut self)`: `self.graph.close_processors()`.
- NOTE: the task needs its partition for restore/changelog. `StreamTask::new` takes `Vec<TopicPartition>` (the assigned source partitions); use `sources[0].partition` as the task partition (all co-partitioned). Store it.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-client): StreamTask restore + changelog produce + init`.

---

## Task 8: StreamThread wiring + broker integration

**Files:** Modify `runtime/thread.rs`; create `tests/state_store_integration.rs`.

- [ ] **Step 1: thread wiring.** In `runtime/thread.rs` `apply_assignment`, when CREATING a task: after `seek_to_start`, call `task.restore(fetcher).await?` then `task.init()?`. (apply_assignment needs the `fetcher` — thread it through; `apply_assignment` is called from the supervisor in `app.rs` which has the fetcher. Add a `fetcher: &dyn RecordFetcher` param to `apply_assignment`, or store an `Arc<dyn RecordFetcher>` on the thread.) When REMOVING a task: `task.close_processors()` before the existing `commit`. Extend the existing thread unit test to assert a created task restored/inited (a stateful assignment).
- In `app.rs`, pass the fetcher to `apply_assignment` (the supervisor owns `fetcher`).

- [ ] **Step 2: broker integration test** — `tests/state_store_integration.rs` (`#![cfg(not(target_os = "windows"))]`). Reuse the #2b `boot`/`finalize_streams_version`/`create_topic` helpers. A counting app over `stream-in` → `stream-out` with a `counts` store:
  - create `stream-in`, `stream-out` (the broker auto-creates the changelog topic from the topology).
  - produce keys `["a","a","b"]` to `stream-in`.
  - run the counting `KafkaStreams` app; read `stream-out`; assert counts (a→1, a→2, b→1).
  - **restart-restore:** `streams.close()`, then start a FRESH `KafkaStreams` on the same `application_id`; produce another `"a"`; assert the output is `3` (not 1) — proving the store restored from the changelog.
- Debug real failures (don't weaken). If restore is broken (count resets to 1), that's a real bug — report it.

- [ ] **Step 3:** `cargo test -p crabka-client-streams --test state_store_integration -- --nocapture` PASS; clippy; fmt; commit `test(streams-client): stateful + restart-restore broker integration`.

---

## Task 9: Docs + final verification

**Files:** Modify `lib.rs`.

- [ ] **Step 1:** Add a `## State stores` doc section to `lib.rs` — a counting `Processor` using `get_state_store`, tested via `TopologyTestDriver` with `get_key_value_store`. `no_run` or runnable (the test-driver path is runnable — prefer a runnable doctest mirroring Task 6).
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` (store units + graph + builder + test-driver + task + thread + #2 runtime + golden_frame + integration tests + doctests); `cargo fmt -p crabka-client-streams -- --check`; `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-client): state store example + #3 verification`.

---

## Self-review

**Spec coverage:**
- §3.1 store traits → Task 1. §3.2 in-memory store → Task 2. §3.3 registry+downcast → Task 3. §3.4 get_state_store → Task 4. §3.5 typed add_state_store + factory → Task 5. §3.6 graph (stores, init/close, drain) → Task 4. ✓
- §4 runtime (restore, changelog produce, init/close, thread wiring) → Tasks 7, 8. ✓
- §5 TopologyTestDriver (init + get_key_value_store) → Task 6. ✓
- §7 testing (store units, test-driver stateful, restore unit, broker stateful + restart-restore) → Tasks 2,3,6,7,8. ✓
- §9 success criteria (golden frame unchanged, docs example) → Tasks 5, 9. ✓

**Placeholder note:** Task 3 flags a decision (changelog methods on `StateStore` vs a separate `LoggedStore` trait) with a clear recommendation (put them on `StateStore`) — resolve it there and update Tasks 1/2's trait/impl accordingly. Task 7/8's tests are described structurally (reusing the #2b fakes/helpers verbatim) rather than fully transcribed, because they're 1:1 adaptations of the committed #2b task/integration tests with a stateful topology — the assertions (changelog produced; restart resumes the count) are exact. These are flagged, not silent.

**Type consistency:** `StateStore`/`KeyValueStore` (T1) → `InMemoryKeyValueStore::new(name, Box<dyn Serde<K>>, Box<dyn Serde<V>>, changelog_topic)` (T2) → `StoreRegistry::{insert, get_kv::<K,V>}` (T3) → `ctx.get_state_store::<K,V>` / `Dispatch.stores` / `Graph.stores` (T4) → `add_state_store<K,V,KS,VS>` + `StoreFactory` + `instantiate` (T5) → `driver.get_key_value_store::<K,V>` (T6) → `task.restore/init`, `graph.drain_changelogs` (T7) → `apply_assignment(fetcher)` (T8). All consistent.

**Known risk:** Task 4's four-disjoint-field borrow in `pipe`/`init_processors` (`nodes[idx]`, `output`, `children`, `stores`) and the `get_state_store` reborrow lifetime are the trickiest; the per-task TDD catches compile issues. The changelog-methods-on-StateStore decision (Task 3) ripples to Tasks 1/2 — resolve early.
