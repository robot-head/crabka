# KIP-1071 Streams Client #4d-i — async execution path + pluggable store backend (Turso) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the streams-client execution path async and introduce a pluggable state-store backend (`Turso` production / in-memory test) under a byte-level seam, with no change to behavior or wire topology.

**Architecture:** A byte-level `ByteKeyValueStore` backend trait (`InMemoryBytes` BTreeMap / `TursoBytes` SQL) sits under a single typed `KeyValueBytesStore<K,V>` wrapper (serdes + changelog), which the registry downcasts to. The `Processor`/`ErasedNode`/store traits and the graph driver loop become async (`async-trait`); `forward` stays sync. `TopologyTestDriver` stays synchronous via an internal `pollster::block_on`.

**Tech Stack:** Rust 2024; `async-trait`, `turso = "0.6"`, `pollster`. Extends #2a/#2b/#3/#4 + 4c.

**Spec:** `docs/superpowers/specs/2026-06-04-kip-1071-streams-client-4d-i-async-pluggable-stores-design.md`.
**Branch:** `streams-4d-async-stores` (stacked on `streams-4c-ktable-join` / PR #390; rebase onto `main` once #390 merges). Worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

---

## Current signatures (verbatim — what we transform)

- `processor/api.rs`: `pub trait Processor<KIn,VIn,KOut,VOut>: Send + 'static { fn init(&mut self, _ctx: &mut ProcessorContext<'_,'_,KOut,VOut>) {} fn process(&mut self, ctx, record: Record<KIn,VIn>); fn close(&mut self) {} }`. `ProcessorContext::{forward, get_state_store::<K2,V2>(name) -> Option<&mut dyn KeyValueStore<K2,V2>>, record_context}`. Blanket `Processor for Box<dyn Processor<…>>`. `ProcessorSupplier::get -> Box<dyn Processor<…>>`.
- `processor/node.rs`: `pub(crate) trait ErasedNode: Send { fn init(&mut self, _d: &mut Dispatch<'_>) -> Result<(),ProcessorError> {Ok(())} fn close(&mut self) {} fn process(&mut self, d: &mut Dispatch<'_>, record: ErasedRecord) -> Result<(),ProcessorError>; }`. `ProcessorNode::process` downcasts then `self.inner.process(&mut ctx, record)`. `SinkNode`/`SourceNode` also impl/serve.
- `processor/graph.rs`: `Graph::pipe(&mut self, topic, key, value, ts) -> Result<(),ProcessorError>` (drains a buffer, calls `node.process(&mut d, rec)`); `init_processors`; `drain_changelogs` (calls `store.take_changelog`); `restore_apply` (calls `store.apply_changelog`); `set_logging`.
- `store/api.rs`: `trait StateStore: Any+Send { name; flush; close; as_any_mut; changelog_topic; take_changelog -> Vec<(Bytes,Option<Bytes>)>; apply_changelog(key,value); set_logging(on) }`. `trait KeyValueStore<K,V>: StateStore { get(&self,&K)->Option<V>; put(&mut self,K,V); delete(&mut self,&K)->Option<V> }`.
- `store/memory.rs`: `InMemoryKeyValueStore<K,V> { name, changelog_topic, map: HashMap<Bytes,Bytes>, key_serde: Box<dyn Serde<K>>, value_serde: Box<dyn Serde<V>>, changelog: Vec<(Bytes,Option<Bytes>)>, logging: bool }` + `new(name, ks, vs, changelog_topic)`.
- `store/registry.rs`: `get_kv::<K,V>(name) -> Option<&mut dyn KeyValueStore<K,V>>` downcasts `as_any_mut()` to `InMemoryKeyValueStore<K,V>`.
- `topology/builder.rs`: `type StoreFactory = Box<dyn Fn(&str,&str)->Box<dyn StateStore> + Send + Sync>;` (line 12); factory built in `add_state_store_inner` (line ~402); `BuiltTopology::instantiate` (line 674) loops `store_factories` (line 760) building each store.
- `runtime/task.rs`: `process_once` (async) calls `self.graph.pipe(...)`; `restore` (async) calls `self.graph.restore_apply(...)`.
- `runtime/app.rs`: `KafkaStreams::start(...)` (line 41, async) instantiates the graph per task.
- `test_driver.rs`: `TopologyTestDriver::{new, pipe_input, pipe_bytes (calls self.graph.pipe), get_key_value_store::<K,V> -> Option<&mut dyn KeyValueStore<K,V>>, read_output}`.

**Processor impls to convert (count via `grep -c "impl.*Processor<"`):** `stateless.rs` (9), `table.rs` (6), `aggregate.rs` (2), `join.rs` (1), `ktable_join.rs` (2) = **20 production impls**; plus inline test processors in `api.rs`, `graph.rs`, `node.rs`, `test_driver.rs`, `builder.rs`. **Store-touching impls** (need `.await` on store calls): `join.rs`, `aggregate.rs`, `table.rs`, `ktable_join.rs`. `stateless.rs` impls only need the async signature (no store calls).

## File structure

```
crates/client-streams/Cargo.toml          + async-trait, turso="0.6", pollster (dev or normal)
src/store/byte.rs            NEW — ByteKeyValueStore trait + InMemoryBytes (Task 1 sync → Task 2 async)
src/store/turso.rs           NEW — TursoBytes backend (Task 3)
src/store/kv.rs              NEW — KeyValueBytesStore<K,V> wrapper (replaces memory.rs role)
src/store/memory.rs          DELETE (folded into kv.rs + byte.rs)
src/store/registry.rs        downcast → KeyValueBytesStore<K,V>
src/store/api.rs             KeyValueStore/StateStore async (Task 2)
src/store/backend.rs         NEW — StoreBackend enum + open logic (Task 4)
src/store/mod.rs             module wiring
src/processor/api.rs         Processor async (Task 2)
src/processor/node.rs        ErasedNode async (Task 2)
src/processor/graph.rs       pipe/init/restore_apply async (Task 2)
src/dsl/processors/*.rs      20 impls → async (Task 2)
src/test_driver.rs           block_on the async graph; sync store-inspection accessor (Task 2)
src/runtime/{task.rs,app.rs} await graph.pipe; thread StoreBackend (Task 2/4)
src/topology/builder.rs      StoreFactory gains backend (Task 4)
tests/store_backends.rs      NEW — backend contract test (Task 3)
tests/turso_runtime.rs       NEW — Turso e2e + restart-restore (Task 5)
```

**Batching:** strictly sequential — Task 2 is one atomic compile unit (the async flip won't compile until every impl is converted). Task 0 (spike) gates Task 3's backend choice but not Tasks 1–2.

---

## Task 0: Turso spike (gating)

**Files:** `crates/client-streams/Cargo.toml`; `crates/client-streams/tests/turso_spike.rs` (NEW, **throwaway** — deleted in Task 3 once `TursoBytes` subsumes it).

- [ ] **Step 1: add the dependency.** In `crates/client-streams/Cargo.toml` under `[dependencies]` add `turso = "0.6"` and `pollster = "0.4"`; under `[dev-dependencies]` ensure `tokio = { workspace = true, features = ["macros", "rt", "rt-multi-thread"] }` is present (it is — existing `#[tokio::test]`s use it).

- [ ] **Step 2: write the spike test** — `crates/client-streams/tests/turso_spike.rs`:
```rust
//! THROWAWAY spike (deleted in Task 3): proves turso 0.6 fits our constraints —
//! Connection: Send, futures resolve under tokio .await, ordered BLOB range scan.
use turso::Builder;

fn assert_send<T: Send>(_: &T) {}

#[tokio::test]
async fn turso_send_await_and_ordered_range() {
    // (a) open an in-memory DB; (b) Connection usable under tokio .await
    let db = Builder::new_local(":memory:").build().await.unwrap();
    let conn = db.connect().unwrap();
    // (c) Connection: Send (required — the spawned StreamTask future holds the store)
    assert_send(&conn);

    conn.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)", ())
        .await
        .unwrap();
    for (k, v) in [(&[1u8, 0][..], b"a"), (&[1u8, 2][..], b"b"), (&[2u8, 0][..], b"c")] {
        conn.execute(
            "INSERT INTO kv (k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (k, &v[..]),
        )
        .await
        .unwrap();
    }
    // (d) ordered half-open range [0x0100, 0x0200) → exactly the two 0x01.. rows, in key order
    let mut rows = conn
        .query(
            "SELECT k, v FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k",
            (&[1u8, 0][..], &[2u8, 0][..]),
        )
        .await
        .unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        let v: Vec<u8> = row.get_value(1).unwrap().try_into().unwrap();
        got.push(v);
    }
    assert_eq!(got, vec![b"a".to_vec(), b"b".to_vec()]);
}
```

- [ ] **Step 3: run the spike.** Run: `cargo test -p crabka-client-streams --test turso_spike -- --nocapture`. Expected: PASS. **If `assert_send(&conn)` fails to compile** (`Connection: !Send`) OR the `.await`s hang/error under tokio: STOP and report BLOCKED — the fallback is `TursoBytes` on a dedicated per-connection thread or `rusqlite`, decided before Task 3. Record the exact turso API names that worked (`Builder::new_local`, `db.connect()`, `conn.execute`, `conn.query`, `rows.next()`, `row.get_value(i)`, `TryInto<Vec<u8>>`) — Task 3 reuses them; adjust if the 0.6 API differs.

- [ ] **Step 4: commit.**
```bash
git -C <worktree> add crates/client-streams/Cargo.toml crates/client-streams/tests/turso_spike.rs Cargo.lock
git -C <worktree> -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "spike(streams): verify turso 0.6 (Send + tokio-await + ordered BLOB range)"
```

---

## Task 1: byte-store seam (synchronous refactor, no behavior change)

Refactor `InMemoryKeyValueStore<K,V>` into `KeyValueBytesStore<K,V>` + a **synchronous** `ByteKeyValueStore` trait with an `InMemoryBytes` (BTreeMap) backend. Pure refactor — async comes in Task 2. All existing tests stay green.

**Files:** Create `src/store/byte.rs`, `src/store/kv.rs`; modify `src/store/mod.rs`, `src/store/registry.rs`; delete `src/store/memory.rs`; update direct constructors in `src/dsl/processors/ktable_join.rs` (test), `src/store/registry.rs` (test), `src/processor/graph.rs` (test), `src/topology/builder.rs` (factory + tests), `src/test_driver.rs` (none — uses registry).

- [ ] **Step 1: write `src/store/byte.rs`** (sync trait + BTreeMap backend):
```rust
//! Byte-level pluggable KV backend. The typed `KeyValueBytesStore<K,V>` sits on
//! top; backends (`InMemoryBytes`, later `TursoBytes`) are swapped underneath.
use std::collections::BTreeMap;

use bytes::Bytes;

/// Object-safe raw-byte KV backend. `range` is half-open `[lo, hi)` in memcmp
/// (lexicographic) key order — used by 4d-ii's window store; KV stores don't call it.
pub(crate) trait ByteKeyValueStore: Send {
    fn get(&self, key: &[u8]) -> Option<Bytes>;
    fn put(&mut self, key: Bytes, value: Bytes);
    fn delete(&mut self, key: &[u8]) -> Option<Bytes>;
    #[allow(dead_code)] // used by 4d-ii window store
    fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)>;
}

/// In-memory backend over a `BTreeMap` (ordered → serves `range`).
#[derive(Default)]
pub(crate) struct InMemoryBytes {
    map: BTreeMap<Bytes, Bytes>,
}

impl ByteKeyValueStore for InMemoryBytes {
    fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map.get(key).cloned()
    }
    fn put(&mut self, key: Bytes, value: Bytes) {
        self.map.insert(key, value);
    }
    fn delete(&mut self, key: &[u8]) -> Option<Bytes> {
        self.map.remove(key)
    }
    fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        self.map
            .range(Bytes::copy_from_slice(lo)..Bytes::copy_from_slice(hi))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn inmemory_put_get_delete_range_ordered() {
        let mut s = InMemoryBytes::default();
        s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a"));
        s.put(Bytes::from_static(&[1, 2]), Bytes::from_static(b"b"));
        s.put(Bytes::from_static(&[2, 0]), Bytes::from_static(b"c"));
        check!(s.get(&[1, 2]) == Some(Bytes::from_static(b"b")));
        let r = s.range(&[1, 0], &[2, 0]);
        check!(r.len() == 2);
        check!(r[0].1 == Bytes::from_static(b"a")); // ordered
        check!(s.delete(&[1, 0]) == Some(Bytes::from_static(b"a")));
        check!(s.get(&[1, 0]) == None);
    }
}
```

- [ ] **Step 2: write `src/store/kv.rs`** — the typed wrapper (moves all serde/changelog logic out of `memory.rs`):
```rust
//! `KeyValueBytesStore<K,V>`: the single typed store the registry holds and
//! downcasts to. Serde + changelog-buffer logic over a pluggable `ByteKeyValueStore`.
use std::any::Any;

use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::{KeyValueStore, StateStore};
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};

pub struct KeyValueBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> KeyValueBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        backend: Box<dyn ByteKeyValueStore>,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self { name, changelog_topic, backend, key_serde, value_serde, changelog: Vec::new(), logging: true }
    }

    /// Convenience constructor for tests: an in-memory-backed store.
    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(name, Box::new(InMemoryBytes::default()), key_serde, value_serde, changelog_topic)
    }
}

impl<K: 'static, V: 'static> StateStore for KeyValueBytesStore<K, V> {
    fn name(&self) -> &str { &self.name }
    fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn changelog_topic(&self) -> &str { &self.changelog_topic }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> { std::mem::take(&mut self.changelog) }
    fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value {
            Some(v) => self.backend.put(key, v),
            None => { self.backend.delete(&key); }
        }
    }
    fn set_logging(&mut self, on: bool) { self.logging = on; }
}

impl<K: 'static, V: 'static> KeyValueStore<K, V> for KeyValueBytesStore<K, V> {
    fn get(&self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(key);
        self.backend.get(&kb).map(|vb| self.value_serde.deserialize(&vb).expect("store value deserialize"))
    }
    fn put(&mut self, key: K, value: V) {
        let kb = self.key_serde.serialize(&key);
        let vb = self.value_serde.serialize(&value);
        self.backend.put(kb.clone(), vb.clone());
        if self.logging { self.changelog.push((kb, Some(vb))); }
    }
    fn delete(&mut self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(key);
        let prev = self.backend.delete(&kb).map(|vb| self.value_serde.deserialize(&vb).expect("store value deserialize"));
        if self.logging { self.changelog.push((kb, None)); }
        prev
    }
}
```
Move `memory.rs`'s two unit tests (`put_get_delete_and_changelog_buffer`, `apply_changelog_restores_without_re_logging`) into `kv.rs`'s `#[cfg(test)] mod tests`, replacing `InMemoryKeyValueStore::new(...)` with `KeyValueBytesStore::in_memory(...)`.

- [ ] **Step 3: rewire `src/store/mod.rs`** — replace `pub mod memory;` (or `pub(crate) mod memory;`) with `pub(crate) mod byte;` + `pub mod kv;`. Keep `pub mod api; pub(crate) mod registry;`. Re-export if `memory` was re-exported (check `mod.rs` + `lib.rs` for `InMemoryKeyValueStore` re-exports and repoint to `KeyValueBytesStore`).

- [ ] **Step 4: update `src/store/registry.rs`** — change the downcast target:
```rust
use crate::store::kv::KeyValueBytesStore;
// ...
pub fn get_kv<K: 'static, V: 'static>(&mut self, name: &str) -> Option<&mut dyn KeyValueStore<K, V>> {
    let store = self.stores.get_mut(name)?;
    let concrete = store.as_any_mut().downcast_mut::<KeyValueBytesStore<K, V>>()?;
    Some(concrete as &mut dyn KeyValueStore<K, V>)
}
```
Update the registry's own test to `KeyValueBytesStore::in_memory(...)`.

- [ ] **Step 5: delete `src/store/memory.rs`.** `git rm crates/client-streams/src/store/memory.rs`.

- [ ] **Step 6: fix the direct-constructor call sites.** Replace every `InMemoryKeyValueStore::<K,V>::new(name, ks, vs, cl)` with `KeyValueBytesStore::<K,V>::in_memory(name, ks, vs, cl)` and the import `crate::store::memory::InMemoryKeyValueStore` → `crate::store::kv::KeyValueBytesStore`. Sites: `src/processor/graph.rs` test (`stateful_processor_accumulates_via_store`), `src/store/registry.rs` test, `src/dsl/processors/ktable_join.rs` test (`make_stores_with_b`), and the factory in `src/topology/builder.rs` line ~405 (the `Box::new(InMemoryKeyValueStore::<K,V>::new(...))` inside the `StoreFactory` closure) + any builder tests. Find them all: `grep -rn "InMemoryKeyValueStore" crates/client-streams/src`.

- [ ] **Step 7: build + test → green.** Run: `cargo test -p crabka-client-streams`. Expected: all pass (pure refactor — 8 goldens byte-identical, 26 execution tests, lib tests). Run `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` + `cargo fmt -p crabka-client-streams`.

- [ ] **Step 8: commit.** `feat(streams-store): byte-store seam (KeyValueBytesStore + ByteKeyValueStore/InMemoryBytes)`.

---

## Task 2: async flip (atomic — traits, loop, all processors, test driver)

Make the store traits, `ByteKeyValueStore`, `Processor`, `ErasedNode`, the graph loop, and all 20 processor impls async. `forward` stays sync. This is **one compile unit** — it does not compile until every piece is converted, so do it in one task and run the suite at the end. TDD here = the existing suite is the test; we make a sweeping change and prove green.

**Files:** `Cargo.toml` (async-trait), `src/store/api.rs`, `src/store/byte.rs`, `src/store/kv.rs`, `src/processor/api.rs`, `src/processor/node.rs`, `src/processor/graph.rs`, `src/dsl/processors/{stateless,table,aggregate,join,ktable_join}.rs`, `src/test_driver.rs`, `src/runtime/task.rs`, and every inline test processor.

- [ ] **Step 1: add async-trait.** `Cargo.toml` `[dependencies]`: `async-trait = "0.1"`.

- [ ] **Step 2: async store traits** — `src/store/api.rs`:
```rust
use async_trait::async_trait;
// ...
#[async_trait]
pub trait StateStore: Any + Send {
    fn name(&self) -> &str;
    async fn flush(&mut self);
    fn close(&mut self);
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn changelog_topic(&self) -> &str;
    fn take_changelog(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>)>; // sync: drains a Vec
    async fn apply_changelog(&mut self, key: bytes::Bytes, value: Option<bytes::Bytes>);
    fn set_logging(&mut self, on: bool);
}

#[async_trait]
pub trait KeyValueStore<K: Send, V: Send>: StateStore {
    async fn get(&self, key: &K) -> Option<V>;
    async fn put(&mut self, key: K, value: V);
    async fn delete(&mut self, key: &K) -> Option<V>;
}
```
(`K: Send, V: Send` are required by `#[async_trait]` for the boxed `Send` futures that borrow them.)

- [ ] **Step 3: async byte backend** — `src/store/byte.rs`: add `use async_trait::async_trait;`, annotate the trait `#[async_trait]`, make `get`/`put`/`delete`/`range` `async fn`, and annotate `impl ByteKeyValueStore for InMemoryBytes` with `#[async_trait]` (bodies unchanged — they just become ready futures). Update the byte.rs test to `#[tokio::test]` + `.await`.

- [ ] **Step 4: async typed wrapper** — `src/store/kv.rs`: annotate `impl StateStore` and `impl KeyValueStore` with `#[async_trait]`; `flush`/`apply_changelog` and `get`/`put`/`delete` become `async fn`, awaiting `self.backend.*`. Example:
```rust
async fn get(&self, key: &K) -> Option<V> {
    let kb = self.key_serde.serialize(key);
    match self.backend.get(&kb).await {
        Some(vb) => Some(self.value_serde.deserialize(&vb).expect("store value deserialize")),
        None => None,
    }
}
async fn put(&mut self, key: K, value: V) {
    let kb = self.key_serde.serialize(&key);
    let vb = self.value_serde.serialize(&value);
    self.backend.put(kb.clone(), vb.clone()).await;
    if self.logging { self.changelog.push((kb, Some(vb))); }
}
```
Update kv.rs tests to `#[tokio::test]` + `.await` on store calls.

- [ ] **Step 5: async `Processor`** — `src/processor/api.rs`:
```rust
use async_trait::async_trait;

#[async_trait]
pub trait Processor<KIn: Send, VIn: Send, KOut: Send, VOut: Send>: Send + 'static {
    async fn init(&mut self, _ctx: &mut ProcessorContext<'_, '_, KOut, VOut>) {}
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, KOut, VOut>, record: Record<KIn, VIn>);
    async fn close(&mut self) {}
}

#[async_trait]
impl<KIn, VIn, KOut, VOut> Processor<KIn, VIn, KOut, VOut> for Box<dyn Processor<KIn, VIn, KOut, VOut>>
where KIn: Send + 'static, VIn: Send + 'static, KOut: Send + 'static, VOut: Send + 'static {
    async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, KOut, VOut>) { (**self).init(ctx).await; }
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, KOut, VOut>, record: Record<KIn, VIn>) { (**self).process(ctx, record).await; }
    async fn close(&mut self) { (**self).close().await; }
}
```
`ProcessorContext::forward` stays **sync**; `get_state_store` is unchanged (returns `&mut dyn KeyValueStore<K2,V2>` — its methods are now async, callers await). Add `K2: Send, V2: Send` bounds to `get_state_store` to match the trait. Update the `KOut/VOut` bounds on `ProcessorContext`'s impl block to include `Send` (already `Any+Send+Clone`). Convert the inline test processors (`Upper`, `Noop`) to `#[async_trait] impl ... { async fn process(...) }` and the tests to `#[tokio::test]` with `.process(...).await`.

- [ ] **Step 6: async `ErasedNode`** — `src/processor/node.rs`:
```rust
use async_trait::async_trait;

#[async_trait]
pub(crate) trait ErasedNode: Send {
    async fn init(&mut self, _dispatch: &mut Dispatch<'_>) -> Result<(), ProcessorError> { Ok(()) }
    fn close(&mut self) {}
    async fn process(&mut self, dispatch: &mut Dispatch<'_>, record: ErasedRecord) -> Result<(), ProcessorError>;
}
```
`ProcessorNode`'s `impl ErasedNode` becomes `#[async_trait]`; `process` awaits the inner processor: `self.inner.process(&mut ctx, record).await;` (the downcast logic above it is unchanged). `init` awaits `self.inner.init(&mut ctx).await`. `SinkNode`'s `process` is `async fn` (body unchanged — no await needed, it just serializes). `SourceNode` is unaffected (not an `ErasedNode`). Convert node.rs inline test processor + tests to `#[tokio::test]` + `.await`.

- [ ] **Step 7: async graph loop** — `src/processor/graph.rs`:
```rust
pub async fn pipe(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], timestamp: i64) -> Result<(), ProcessorError> {
    // ... seed buffer unchanged ...
    while let Some((idx, rec)) = buffer.pop_front() {
        let children = std::mem::take(&mut self.children[idx]);
        let res = {
            let node = &mut self.nodes[idx];
            let out = &mut self.output;
            let stores = &mut self.stores;
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: out, record_ctx: &rc, stores };
            node.process(&mut d, rec).await
        };
        self.children[idx] = children;
        res?;
    }
    Ok(())
}
```
`init_processors` → `async fn`, `node.init(&mut d).await?`. `restore_apply` → `async fn`, `store.apply_changelog(key, value).await`. `set_logging`/`drain_changelogs`/`close_processors` stay sync (`take_changelog`/`set_logging`/`close` are sync). Convert graph.rs inline tests (`drives_source_processor_sink`, `unknown_topic_is_ignored`, `stateful_processor_accumulates_via_store`) to `#[tokio::test]` + `graph.pipe(...).await`.

- [ ] **Step 8: convert the 20 production processor impls.** For each, add `use async_trait::async_trait;` (once per file), put `#[async_trait]` on each `impl Processor`, make `process` (and any `init`) `async fn`, and add `Send` to the generic value-type bounds the compiler now requires (e.g. `VA: Send + 'static`). For **store-touching** impls, await store calls. The exact sites:
  - **`stateless.rs` (9 impls — no store calls):** mechanical — `#[async_trait]` + `async fn process`. No `.await` inside (only `ctx.forward`, which stays sync). Add `Send` to value generics if the compiler flags them.
  - **`aggregate.rs` (2 impls — `KStreamAggregateProcessor`, `KStreamReduceProcessor`):** each does `ctx.get_state_store::<K,VA>(store).get(&k)` then `.put(...)`. Rewrite as `let s = ctx.get_state_store::<K,VA>(&self.store).expect(..); let old = s.get(&key).await; ...; s.put(key, new).await;`. Hold the `s` borrow across the awaits (single task — fine), then drop it before `ctx.forward(...)`.
  - **`table.rs` (6 impls — TableSource/toTable, count/reduce/aggregate-table, map_values_materialized, filter):** same shape — `get(&k).await` / `put(k,v).await` / `delete(&k).await`. Convert each store call to `.await`.
  - **`join.rs` (1 — `KStreamKTableJoinProcessor`):** `ctx.get_state_store::<K,VT>(&self.table_store).and_then(|s| s.get(&key))` → `s.get(&key).await`. Note `and_then` over an async call: restructure to `let vt = match ctx.get_state_store::<K,VT>(&self.table_store) { Some(s) => s.get(&key).await, None => None };`.
  - **`ktable_join.rs` (2 — JoinThis/JoinOther):** `ctx.get_state_store::<K,VB>(&self.other_store).and_then(|s| s.get(&key))` → same `match`-then-`.await` restructure (the `result()` rule + `forward` logic stays sync after the lookup).

  **Worked example — `aggregate.rs` count-style impl:**
```rust
#[async_trait]
impl<K, VIn, VA, ...> Processor<K, VIn, K, Change<VA>> for KStreamAggregateProcessor<...>
where K: Any + Send + Clone, VIn: Send + 'static, VA: Any + Send + Clone, ... {
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>, r: Record<K, VIn>) {
        let key = r.key.expect("agg key");
        let s = ctx.get_state_store::<K, VA>(&self.store).expect("agg store");
        let old = s.get(&key).await;
        let new = (self.aggregator)(&key, &r.value, old.as_ref());
        s.put(key.clone(), new.clone()).await;
        ctx.forward(Record::new(Some(key), Change::update(old, new), r.timestamp));
    }
}
```
(Adapt to each impl's real field/closure names — read each impl first; the shape is identical.)

- [ ] **Step 9: async runtime** — `src/runtime/task.rs`: `self.graph.pipe(...)` → `self.graph.pipe(...).await`; `self.graph.restore_apply(...)` → `.await`; `self.graph.init_processors()` (if called) → `.await`. (`process_once`/`restore` are already `async fn`.)

- [ ] **Step 10: test driver stays sync via `block_on`** — `src/test_driver.rs`:
```rust
fn pipe_bytes(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8], timestamp: i64) {
    // ... queue setup unchanged ...
    while let Some((t, k, v, ts)) = queue.pop_front() {
        let _ = pollster::block_on(self.graph.pipe(&t, k.as_deref(), &v, ts));
        // ... take_output / loop-back unchanged ...
        let _ = self.graph.drain_changelogs();
    }
}
```
`TopologyTestDriver::new` calls `graph.init_processors()` → `pollster::block_on(graph.init_processors())?`. **Store inspection:** `get_key_value_store` returns `&mut dyn KeyValueStore<K,V>` whose `get` is now async — so direct `store.get(&k)` in sync tests breaks. Add a sync helper:
```rust
/// Test-only synchronous store read (block_on the async store).
pub fn store_get<K: Send + 'static, V: Send + 'static>(&mut self, store: &str, key: &K) -> Option<V> {
    let s = self.graph.stores.get_kv::<K, V>(store)?;
    pollster::block_on(s.get(key))
}
```
Convert the test-driver inline tests (`stateful_count_and_store_inspection` line 325) and the **dsl_execution / dsl_golden store-inspection assertions** that call `d.get_key_value_store(..).get(..)` to `d.store_get::<K,V>("name", &key)`. Convert the inline test processors (`Upper`/`DropEmpty`/`Identity`/`Counter`) to `#[async_trait]` + `async fn process` + `.await` on store calls. Find affected execution tests: `grep -rn "get_key_value_store\|get_state_store" crates/client-streams/tests`.

- [ ] **Step 11: build the crate iteratively.** Run `cargo build -p crabka-client-streams` repeatedly, fixing each `async`/`.await`/`Send`-bound error the compiler surfaces (the compiler is the checklist — it will name every unconverted impl and missing `.await`). Then `cargo test -p crabka-client-streams`.
Expected at the end: **all green** — 8 goldens byte-identical (topology unchanged), 26 execution tests, all lib + integration tests. Run `cargo clippy -p crabka-client-streams --all-targets -- -D warnings` (watch `clippy::large_futures` — if a processor future trips the 16KB lint, box it) + `cargo fmt`.

- [ ] **Step 12: commit.** `refactor(streams)!: async execution path (Processor/ErasedNode/stores async via async-trait)`.

---

## Task 3: Turso backend + contract test

**Files:** Create `src/store/turso.rs`; modify `src/store/mod.rs`; create `tests/store_backends.rs`; delete `tests/turso_spike.rs`.

- [ ] **Step 1: write the backend contract test FIRST** — `crates/client-streams/tests/store_backends.rs`:
```rust
//! Both ByteKeyValueStore backends must satisfy the same contract.
//! (InMemoryBytes is pub(crate); this test lives in tests/ so expose a thin
//! test ctor — see Step 3. If keeping backends pub(crate), move this to a
//! `#[cfg(test)]` module in src/store/byte.rs instead.)
```
Since `ByteKeyValueStore`/`InMemoryBytes`/`TursoBytes` are `pub(crate)`, write the contract test as a `#[cfg(test)] mod tests` **inside `src/store/turso.rs`** (and a mirror for in-memory already exists in `byte.rs`). The shared contract:
```rust
async fn contract(mut s: Box<dyn ByteKeyValueStore>) {
    assert_eq!(s.get(b"k").await, None);
    s.put(Bytes::from_static(b"k"), Bytes::from_static(b"v1")).await;
    s.put(Bytes::from_static(b"k"), Bytes::from_static(b"v2")).await; // upsert
    assert_eq!(s.get(b"k").await, Some(Bytes::from_static(b"v2")));
    s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a")).await;
    s.put(Bytes::from_static(&[1, 9]), Bytes::from_static(b"b")).await;
    let r = s.range(&[1, 0], &[1, 5]).await; // half-open → only [1,0]
    assert_eq!(r.len(), 1);
    assert_eq!(s.delete(b"k").await, Some(Bytes::from_static(b"v2")));
    assert_eq!(s.get(b"k").await, None);
}
#[tokio::test] async fn inmemory_contract() { contract(Box::new(InMemoryBytes::default())).await; }
#[tokio::test] async fn turso_contract() { contract(Box::new(TursoBytes::open_in_memory().await)).await; }
```

- [ ] **Step 2: run → FAIL** (`TursoBytes` undefined). `cargo test -p crabka-client-streams --lib store::turso`.

- [ ] **Step 3: implement `src/store/turso.rs`** (reuse the Task 0 spike's verified API):
```rust
//! Turso-backed `ByteKeyValueStore`. One table `kv(k BLOB PRIMARY KEY, v BLOB)`
//! per store; UPSERT on put; half-open ordered range scan. Async-native (no block_on).
use async_trait::async_trait;
use bytes::Bytes;
use turso::Connection;

use crate::store::byte::ByteKeyValueStore;

// NOTE (from the Task 0 spike): turso 0.6 `Value` has NO `TryInto<Vec<u8>>`.
// Extract a BLOB column by matching `turso::Value::Blob(b)` (or `.as_blob()`):
fn blob(v: turso::Value) -> Vec<u8> {
    match v {
        turso::Value::Blob(b) => b,
        other => panic!("expected BLOB column, got {other:?}"),
    }
}

pub(crate) struct TursoBytes {
    conn: Connection,
}

impl TursoBytes {
    pub(crate) async fn open(path: &str) -> Self {
        let db = turso::Builder::new_local(path).build().await.expect("open turso db");
        let conn = db.connect().expect("turso connect");
        // Clean-slate: the changelog is source of truth; restore replays into an empty table.
        conn.execute("DROP TABLE IF EXISTS kv", ()).await.expect("drop kv");
        conn.execute("CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB NOT NULL)", ()).await.expect("create kv");
        Self { conn }
    }
    #[cfg(test)]
    pub(crate) async fn open_in_memory() -> Self { Self::open(":memory:").await }
}

#[async_trait]
impl ByteKeyValueStore for TursoBytes {
    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mut rows = self.conn.query("SELECT v FROM kv WHERE k = ?1", (key,)).await.expect("turso get");
        let row = rows.next().await.expect("turso row")?;
        let v = blob(row.get_value(0).expect("turso v"));
        Some(Bytes::from(v))
    }
    async fn put(&mut self, key: Bytes, value: Bytes) {
        self.conn.execute(
            "INSERT INTO kv (k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key.to_vec(), value.to_vec()),
        ).await.expect("turso put");
    }
    async fn delete(&mut self, key: &[u8]) -> Option<Bytes> {
        let prev = self.get(key).await;
        self.conn.execute("DELETE FROM kv WHERE k = ?1", (key,)).await.expect("turso delete");
        prev
    }
    async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut rows = self.conn.query(
            "SELECT k, v FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k", (lo, hi),
        ).await.expect("turso range");
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.expect("turso range row") {
            let k = blob(row.get_value(0).expect("k"));
            let v = blob(row.get_value(1).expect("v"));
            out.push((Bytes::from(k), Bytes::from(v)));
        }
        out
    }
}
```
Adjust API calls if the Task 0 spike found different turso 0.6 names/param-binding forms. Add `pub(crate) mod turso;` to `src/store/mod.rs`.

- [ ] **Step 4: run → PASS.** `cargo test -p crabka-client-streams --lib store::`. Both `inmemory_contract` + `turso_contract` green. Then full `cargo test -p crabka-client-streams`. Delete the spike: `git rm crates/client-streams/tests/turso_spike.rs`. clippy + fmt.

- [ ] **Step 5: commit.** `feat(streams-store): Turso ByteKeyValueStore backend + backend contract test`.

---

## Task 4: backend selection + wiring + clean-slate restore

**Files:** Create `src/store/backend.rs`; modify `src/topology/builder.rs` (StoreFactory signature), `src/runtime/app.rs` + `src/runtime/task.rs` (thread the backend), `src/test_driver.rs` (default InMemory).

- [ ] **Step 1: `src/store/backend.rs`** — the selector + a byte-backend opener:
```rust
//! Which storage engine backs the state stores. `InMemory` is the test default
//! and a valid production option; `Turso` persists under a state dir.
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::turso::TursoBytes;

#[derive(Clone, Debug)]
pub enum StoreBackend {
    InMemory,
    Turso { state_dir: std::path::PathBuf },
}

impl Default for StoreBackend {
    fn default() -> Self { Self::InMemory }
}

impl StoreBackend {
    /// Open a byte backend for one store. `app_id`/`store` form the Turso file path.
    pub(crate) async fn open(&self, app_id: &str, store: &str) -> Box<dyn ByteKeyValueStore> {
        match self {
            Self::InMemory => Box::new(InMemoryBytes::default()),
            Self::Turso { state_dir } => {
                let dir = state_dir.join(app_id);
                std::fs::create_dir_all(&dir).expect("create state dir");
                let path = dir.join(format!("{store}.db"));
                Box::new(TursoBytes::open(path.to_str().expect("utf8 path")).await)
            }
        }
    }
}
```
Add `pub mod backend;` to `src/store/mod.rs` and re-export `StoreBackend` from `lib.rs`.

- [ ] **Step 2: change the `StoreFactory` to build the typed store from a backend.** In `src/topology/builder.rs`, the factory currently captures the serdes and builds `KeyValueBytesStore::in_memory(...)` (after Task 1). Change its signature so instantiation supplies the opened backend:
```rust
type StoreFactory = Box<dyn Fn(&str /*store*/, String /*changelog*/, Box<dyn ByteKeyValueStore>) -> Box<dyn StateStore> + Send + Sync>;
```
The closure (in `add_state_store_inner`) becomes:
```rust
Box::new(move |store_name: &str, changelog: String, backend: Box<dyn ByteKeyValueStore>| {
    Box::new(KeyValueBytesStore::<K, V>::new(
        store_name.to_string(), backend, Box::new(key_serde.clone()), Box::new(value_serde.clone()), changelog,
    )) as Box<dyn StateStore>
})
```

- [ ] **Step 3: thread the backend through `instantiate`.** `BuiltTopology::instantiate` is sync and builds stores synchronously (line 760 loop). It must now (a) be async (opening Turso is async) and (b) take a `&StoreBackend`. Change to:
```rust
pub(crate) async fn instantiate(&self, backend: &StoreBackend, app_id: &str) -> Result<Graph, ProcessorError> {
    // ... existing node/source build ...
    for (store_name, factory) in &self.store_factories {
        let changelog = /* existing changelog-name derivation */;
        let bytes = backend.open(app_id, store_name).await;
        let store = factory(store_name, changelog, bytes);
        graph.stores.insert(store);
    }
    Ok(graph)
}
```
Update the builder's own `#[tokio::test]`s that call `built.instantiate()` → `built.instantiate(&StoreBackend::InMemory, "app").await`.

- [ ] **Step 4: test driver passes `InMemory`.** `TopologyTestDriver::new`: `pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app"))?` then `pollster::block_on(graph.init_processors())?`. (App id "app" is fine — the in-memory backend ignores it.)

- [ ] **Step 5: runtime passes the configured backend.** In `src/runtime/app.rs` `KafkaStreams::start`, accept/derive a `StoreBackend` (add a parameter or a field on the streams config; default `Turso { state_dir }` from a config dir, or `InMemory`). Thread it to wherever `instantiate` is called per task (`runtime/task.rs` or `app.rs`): `built.instantiate(&backend, &app_id).await?`. Keep the existing broker-integration test working — if it constructs `KafkaStreams` directly, pass `StoreBackend::InMemory` there unless Step-5-of-Task-5 switches it to Turso.

- [ ] **Step 6: build + test → green.** `cargo test -p crabka-client-streams`. The clean-slate `DROP TABLE` in `TursoBytes::open` (Task 3) already gives empty-store-then-replay restore semantics, so `restore` (which calls `apply_changelog`) rebuilds correctly. clippy + fmt.

- [ ] **Step 7: commit.** `feat(streams): StoreBackend selection (InMemory|Turso) threaded through instantiate`.

---

## Task 5: Turso end-to-end + restart-restore integration

**Files:** Create `crates/client-streams/tests/turso_runtime.rs` (mirror the existing `tests/state_store_integration.rs` harness, but with `StoreBackend::Turso` under a `tempfile` dir).

- [ ] **Step 1: read the existing harness.** Read `crates/client-streams/tests/state_store_integration.rs` (the `stateful_count_and_restart_restore` test) — it drives the runtime with mock I/O traits + a stateful count and asserts restart-restore. Reuse its mock fetcher/producer/offset-store setup.

- [ ] **Step 2: write the Turso e2e test** — `tests/turso_runtime.rs`: same topology + mocks as `state_store_integration.rs`, but instantiate the task/graph with `StoreBackend::Turso { state_dir: <tempdir> }`. Assert: (a) piping two records for key "a" produces counts 1,2 with state on Turso; (b) **restart-restore** — drop the graph/task, re-instantiate with the SAME `state_dir` (so `TursoBytes::open` DROPs + recreates → empty), run `restore` from the changelog mock, confirm the count resumes at the restored value and the next record yields the correct increment. Use `tempfile::tempdir()` (add `tempfile` to `[dev-dependencies]` if absent — check first).

- [ ] **Step 3: run → PASS.** `cargo test -p crabka-client-streams --test turso_runtime`. Then the full suite `cargo test -p crabka-client-streams`. clippy + fmt.

- [ ] **Step 4: commit.** `test(streams): Turso end-to-end + restart-restore integration`.

---

## Task 6: docs + final verification

**Files:** `src/lib.rs`.

- [ ] **Step 1: doc note.** Add a short crate-overview paragraph to `src/lib.rs`: the execution path is async; state stores are pluggable via `ByteKeyValueStore` (`InMemoryBytes` for tests / `Turso` for the runtime), selected by `StoreBackend`; processors `await` store access; the changelog remains the source of truth (stores are rebuildable caches). No new doctest (or a `compile`-only snippet if one fits).

- [ ] **Step 2: full verification.**
  - `cargo test -p crabka-client-streams` — lib + execution (26) + **8 golden frames byte-identical** + backend contract + turso e2e + doctests, all green.
  - `cargo clippy --workspace --all-targets -- -D warnings` (watch `clippy::large_futures`).
  - `cargo fmt -p crabka-client-streams -- --check`.
  - `cargo build --workspace`.

- [ ] **Step 3: commit.** `docs(streams): async + pluggable-store note + #4d-i verification`.

---

## Self-review

**Spec coverage:** §3 async path → Task 2 (+ async traits in Tasks 1–2). §4 byte-store seam → Task 1 (sync) + Task 2 (async). §5 backend selection + DB layout + clean-slate restore → Tasks 3–4. §6 spike + deps → Task 0 + Cargo edits. §7 testing (spike, contract, regression, e2e) → Tasks 0/3/5 + the green gate in every task. §8 success criteria → Task 6. ✓

**Empirical-gate note (not a placeholder):** Task 0's spike result (turso `Connection: Send` + tokio-await + ordered range) is empirically determined; if it fails, the `ByteKeyValueStore` seam localizes the fallback (dedicated thread / rusqlite) to `src/store/turso.rs` with no change above the seam. The exact turso 0.6 API names in Tasks 0/3 are pinned by the spike.

**Decomposition note:** Task 2 is deliberately one atomic task (the async flip doesn't compile partially). Tasks 1, 3, 4, 5 are independently green-able. Task 1 (sync byte seam) is separated from Task 2 (async flip) so the new abstraction lands as a behavior-preserving refactor before the sweeping async change.

**Type consistency:** `ByteKeyValueStore` (byte.rs) → `KeyValueBytesStore<K,V>` wrapper (kv.rs) → registry downcast target (registry.rs) → factory output (builder.rs) → `StoreBackend::open` supplies the `Box<dyn ByteKeyValueStore>` (backend.rs). `Processor`/`ErasedNode`/`KeyValueStore`/`StateStore` all gain `#[async_trait]` consistently; `forward`/`take_changelog`/`set_logging`/`close`/`changelog_topic`/`name` stay sync throughout. `instantiate(&StoreBackend, &str) -> async` is called with `block_on` in the test driver and `.await` in the runtime — consistent.

**Known risks flagged for the implementer:** (1) `#[async_trait]` + the two-lifetime `ProcessorContext<'ctx,'d>` — if elided-lifetime desugaring fights the explicit `<'_,'_>`, name the lifetimes in the impl. (2) `and_then(|s| s.get(k))` over async store calls must become `match { Some(s) => s.get(k).await, None => None }` (join.rs, ktable_join.rs). (3) `clippy::large_futures` (16KB) may fire on a fat processor future — box it. (4) holding the `&mut dyn KeyValueStore` borrow across `.await` is fine (single task), but drop it before `ctx.forward(...)` to avoid a borrow conflict.
