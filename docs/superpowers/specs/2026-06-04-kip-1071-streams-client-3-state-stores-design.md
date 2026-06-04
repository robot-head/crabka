# KIP-1071 Streams Client — Sub-project #3: State stores + changelog backing

**Date:** 2026-06-04
**Status:** Design approved, pending spec review
**Scope:** The third sub-project of the Crabka Streams client-runtime program.
**Builds on:** #1 (membership, merged) + #2 (Processor API + stateless runtime —
#2a merged PR #380, #2b PR #382). Roadmap: `2026-06-03-kip-1071-streams-client-membership-design.md` §1.

## 1. Context

#2 delivered the stateless execution engine: a typed `Processor` API over a
`dyn Any`-erased graph (#2a) and a broker-backed `KafkaStreams` runtime
(`StreamThread`/`StreamTask`, fetch→process→produce→commit, at-least-once) (#2b).
Processors are stateless — `ProcessorContext` has `forward` + `record_context`
but no store access, and `Processor::init`/`close` exist but are **not invoked**.

#3 adds **state**: an in-memory `KeyValueStore<K,V>`, changelog backing
(produce-on-write + restore-on-assignment), a per-task store registry,
`ProcessorContext::get_state_store`, and the `init`/`close` lifecycle. This
unlocks stateful processing (count/reduce/aggregate, KTable materialization) at
the Processor-API level; the DSL that builds on it is #4.

The structural groundwork exists: the #2a builder's `add_state_store(name,
processors)` records store→processor connections (`node.rs stores`) and emits the
`<app>-<store>-changelog` topic into the wire `Topology` (the broker auto-creates
it). #3 makes those stores real.

## 2. Goal and non-goals

### Goal
- An in-memory **`KeyValueStore<K,V>`** (get/put/delete) backed by a changelog
  topic for durability.
- **`ProcessorContext::get_state_store::<K,V>(name)`** typed, mutable store
  access from a stateful processor.
- **Changelog**: every `put`/`delete` is produced to `<app>-<store>-changelog`
  (at-least-once, within the existing flush-before-commit barrier); stores are
  **restored** by replaying the changelog on task assignment.
- **`Processor::init`/`close` invoked** by the runtime (the piece #2 deferred).
- The `TopologyTestDriver` extended to instantiate stores + inspect them.

### Non-goals (deferred)
- **Window/session stores** — later slice (windowed keys, retention, segments;
  mostly used by the windowed DSL #4).
- **Persistent/RocksDB backend** — in-memory only; the changelog provides
  durability (a persistent local store that skips full replay on restart is an
  optimization for later).
- **Standby/warmup store replication** (#5) — #3 restores **active**-task stores
  only.
- **EOS / transactional stores** (#7) — at-least-once (a crash between flush and
  commit re-applies uncommitted records to the store; the known stateful
  at-least-once double-count).
- **Interactive queries** (#6) — `TopologyTestDriver::get_key_value_store` is a
  test-only inspection, not IQ.
- **`range`/`all` iteration** — core is get/put/delete.
- **Record cache / changelog dedup** (KIP-63) — #3 logs every `put`.

## 3. Store-access model (the core)

Confirmed during brainstorming: **typed public store API over a `dyn Any`-erased
per-task registry**, fetched per-record (mirrors #2a's record model + Rust borrow
constraints — a processor can't hold `&mut store` across `process()` calls).

### 3.1 Store traits (`store/api.rs`)
```rust
/// Object-safe lifecycle for any store (held erased in the registry).
pub trait StateStore: std::any::Any + Send {
    fn name(&self) -> &str;
    fn flush(&mut self);   // no-op for in-memory (changelog is the durability path)
    fn close(&mut self);
}
pub trait KeyValueStore<K, V>: StateStore {
    fn get(&self, key: &K) -> Option<V>;
    fn put(&mut self, key: K, value: V);
    fn delete(&mut self, key: &K) -> Option<V>;
}
```

### 3.2 In-memory store (`store/memory.rs`)
`InMemoryKeyValueStore<K, V>` holds `map: HashMap<Bytes, Bytes>` +
`key_serde: Box<dyn Serde<K>>` + `value_serde: Box<dyn Serde<V>>` +
`changelog_buffer: Vec<(Bytes, Option<Bytes>)>` + `name` + a `logging: bool`
flag (off during restore). **The serdes are boxed trait objects (not type
params)** so the concrete type is `InMemoryKeyValueStore<K,V>` — which is what
`get_state_store::<K,V>` downcasts to (it only knows `K,V`, not the serde types).
`Serde<T>` is object-safe (`serialize(&T)->Bytes`, `deserialize(&[u8])->Result<T>`).
- `put(k,v)`: `kb = ks.serialize(&k); vb = vs.serialize(&v); map.insert(kb, vb)`;
  if `logging`, `changelog_buffer.push((kb, Some(vb)))`.
- `delete(k)`: serialize `kb`, `map.remove(&kb)`, return the deserialized prior
  value; if `logging`, push `(kb, None)` (tombstone).
- `get(k)`: serialize `kb`, `map.get(&kb).map(|vb| vs.deserialize(vb))`.
- Internal bytes representation avoids a `K: Hash` bound leak and is uniform with
  changelog/restore + a future persistent backend.
- `apply_changelog(kb, vb)` (restore path, `logging = false`): `map.insert`/`remove`.
- `take_changelog() -> Vec<(Bytes, Option<Bytes>)>`: drain the buffer for the task.

### 3.3 Registry + erased access (`store/registry.rs`)
`StoreRegistry { stores: HashMap<String, Box<dyn StateStore>> }`. `get_mut(name)
-> Option<&mut dyn StateStore>`. Downcast: `dyn StateStore: Any`, so
`get_state_store` downcasts to the concrete `InMemoryKeyValueStore<K,V,…>` and
coerces to `&mut dyn KeyValueStore<K,V>`. Absent/type-mismatch → `None`.

### 3.4 ProcessorContext access (`processor/api.rs`)
`Dispatch` gains `stores: &mut StoreRegistry`. New method (generic over the
**store's** `K2,V2`, independent of the processor's `KOut,VOut`):
```rust
impl ProcessorContext<'_, '_, KOut, VOut> {
    pub fn get_state_store<K2: 'static, V2: 'static>(&mut self, name: &str)
        -> Option<&mut dyn KeyValueStore<K2, V2>> { /* registry downcast */ }
}
```

### 3.5 Builder (`topology/builder.rs`)
`add_state_store` evolves from #2a's untyped `(name, processors)` to typed:
```rust
pub fn add_state_store<K, V, KS, VS>(&mut self, name, key_serde: KS, value_serde: VS, processors)
where K: …+Clone, V: …+Clone, KS: Serde<K>+Clone, VS: Serde<V>+Clone
```
It still calls the structural `reg.add_store(name, processors)` (UNCHANGED — feeds
grouping + the changelog topic in the wire `Topology`, so the golden frame holds)
**and** records a `StoreFactory` (like the node factories) that instantiates an
`InMemoryKeyValueStore<K,V,…>` per task, carrying the changelog topic name
`<app>-<store>-changelog`. The one #2a builder test using `add_state_store` is
migrated to add serdes.

### 3.6 Graph (`processor/graph.rs`)
`Graph` gains a `StoreRegistry`. `instantiate()` builds the stores (from the
factories for stores whose connected processors are in this subtopology) into the
registry. `pipe()` lends `&mut self.stores` into the `Dispatch` (a fourth disjoint
self-field alongside `nodes[idx]`, `output` — the disjoint-field borrow #2a
already uses). New: `init_processors(&mut self)` / `close_processors(&mut self)`
drive `ErasedNode::init`/`close` over the processor nodes with a store-carrying
context; `restore_apply(name, kb, vb)` and `drain_changelogs() -> Vec<(topic,
key, value)>` for the task.

`ErasedNode` gains `init(&mut self, &mut Dispatch)` + `close(&mut self)`;
`ProcessorNode` forwards to the typed `Processor::init`/`close`; source/sink
nodes no-op.

## 4. Runtime wiring (`runtime/task.rs`, `thread.rs`)

`StreamTask` (already owns the `Graph` + producer + offset store):
- **`restore(fetcher, partition)`** — for each store in `graph.stores`, read its
  changelog topic partition `0 → high-watermark` (via the #2b `RecordFetcher`,
  with `logging=false`), applying each `(key, value)` (null = delete). Called once
  at task creation, before processing.
- **`init`** — after restore, call `graph.init_processors()` (invokes
  `Processor::init` with store access).
- **`process_once`** (extend) — after piping the fetched batch (graph updates
  stores + buffers changelog entries) and producing sink outputs, **drain each
  store's changelog buffer** and produce the entries to `<store-changelog-topic>`
  at the task's partition (key/value bytes; null value = tombstone). The existing
  flush-before-commit barrier covers both sink + changelog records.
- **`close`** — `graph.close_processors()` + a final changelog drain/flush/commit.

`StreamThread::apply_assignment` calls `restore` + `init` when **creating** a task
and `close` when **removing** one (before the existing commit-then-drop).

**At-least-once ordering:** produce (sink + changelog) → flush → commit source
offsets. Restore reads the full changelog (latest state); reprocessing uncommitted
source records re-applies them (documented at-least-once double-count; EOS is #7).

## 5. TopologyTestDriver (`test_driver.rs`)

Extended to support stores deterministically (no broker):
- `TopologyTestDriver::new` instantiates the graph's stores (already via
  `instantiate`), and calls `init_processors` before piping.
- `pipe_input` drains changelog buffers into an in-memory per-topic collector
  (same as sink output — repartition/changelog both just "produced"); restore is
  a no-op in the test driver (fresh stores).
- **`get_key_value_store::<K,V>(name) -> Option<&mut dyn KeyValueStore<K,V>>`** —
  inspect a store's contents after piping (mirrors the JVM
  `TopologyTestDriver.getKeyValueStore`). The primary stateful-logic assertion.

## 6. Errors

`get_state_store` returns `None` on absent/type-mismatch (caller `.expect()`s,
JVM-like). Restore fetch failures surface as `StreamsClientError::Runtime` from
task creation (logged; retried on the next rebalance). Changelog produce failures
are caught by the same flush barrier as sink output (#2b's receiver-tracking
producer).

## 7. Testing strategy (gates)

1. **Store units** — `InMemoryKeyValueStore`: put/get/delete; changelog-buffer
   accumulation (put → entry, delete → tombstone); serde round-trip; restore
   `apply_changelog` builds the map.
2. **`TopologyTestDriver` stateful tests** (primary) — a counting processor using
   `get_state_store`: pipe inputs, assert outputs reflect accumulated counts; use
   `get_key_value_store` to assert final store contents; assert `init` ran before
   the first record.
3. **Restore unit test** — a store restored from a scripted changelog (via the
   #2b `RecordFetcher` fake) ends up correct — deterministic, no broker.
4. **In-process broker integration** — a counting `KafkaStreams` app: produce
   input → assert output counts → assert the changelog topic received the writes.
   Plus the decisive **restart-restore** case: stop, start a fresh instance on the
   same group → it restores from the changelog (counts continue, not reset).

## 8. Open points for the plan

- `get_state_store` return type — `Option` (caller `.expect()`) vs `Result`; lean
  `Option` (JVM-like).
- Changelog drain cadence — per-record vs per-batch; lean per-batch in
  `process_once`.
- Restore high-watermark detection — `ListOffsets(latest)` end offset, restore
  until the fetch offset reaches it (vs fetch-until-empty). Lean: ListOffsets end.
- Store-access control — `get_state_store` by name from the task registry
  (lenient); the connection list drives instantiation, not access control, in #3.
- Which subtopology instantiates a store — the store's connected processors
  determine its subtopology; #2's whole-graph-per-task means each task's registry
  holds the stores for its subtopology (filter factories by connected-processor
  membership).

## 9. Success criteria

- `cargo test -p crabka-client-streams` green: store units + test-driver
  stateful/count + restore unit + in-process broker stateful + restart-restore +
  doctest.
- `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --check`
  clean.
- A documented counting example (`lib.rs`) with a `TopologyTestDriver` test that
  inspects the store.
- #2a golden-frame byte test still passes (wire `Topology` unchanged after the
  `add_state_store` migration); #2 runtime tests still pass.
