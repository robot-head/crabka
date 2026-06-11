# Record Caching (statestore.cache.max.bytes) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Kafka Streams' record cache (`statestore.cache.max.bytes`) to `crabka-client-streams` — a per-store write-back LRU over a shared thread byte budget that deduplicates downstream emits (forward-on-evict / flush-on-commit) and serves cache-first reads, for KV, Window, and Session stores.

**Architecture:** A new `store/cache/` core (`LruCacheEntry` → `NamedCache` → `ThreadCache`) plus three caching store wrappers. Forward-suppression uses a `TupleForwarder` seam in materializing processors (mirrors JVM `TimestampedTupleForwarder`): when a store is cached the immediate `ctx.forward` is suppressed and the cache's typed flush listener forwards the deduped `Change` at commit, rooted at the store's owning node via the existing punctuation `Dispatch` mechanism. Cache size 0 (the `TopologyTestDriver` default) leaves stores unwrapped, so every existing golden is unchanged.

**Tech Stack:** Rust 2024, `async_trait`, `tokio`, `bytes::Bytes`, `bon` builders. Ground truth = ported Apache Kafka `NamedCacheTest`/`ThreadCacheTest`/`Caching*StoreTest` + one Kafka-Streams 4.1.0 Docker capture.

**Spec:** `docs/superpowers/specs/2026-06-11-kip-1071-streams-client-record-caching-design.md`

---

## Execution batches (parallel where file sets are disjoint)

- **Batch 1 (parallel):** Task 1 (cache core), Task 2 (config plumbing). Disjoint files.
- **Batch 2 (parallel):** Task 3 (KV wrapper), Task 4 (Window wrapper), Task 5 (Session wrapper). All depend on Batch 1; disjoint files (`cache/kv.rs`, `cache/window.rs`, `cache/session.rs`).
- **Batch 3 (sequential):** Task 6 (TupleForwarder + node↔store map + `graph.flush_caches` + registry wrapping), then Task 7 (wire materializing processors + `task.commit`/`close`). Share `graph.rs`/`registry.rs`/processors.
- **Batch 4:** Task 8 (Docker golden + CI + regression guard).

All tasks run under: `cargo test -p crabka-client-streams`. Final gate per spec §8: `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test -p crabka-client-streams`; `cargo build --workspace`.

---

## Task 1: Cache core — `LruCacheEntry`, `NamedCache`, `ThreadCache`

**Files:**
- Create: `crates/client-streams/src/store/cache/mod.rs`
- Create: `crates/client-streams/src/store/cache/entry.rs`
- Create: `crates/client-streams/src/store/cache/named.rs`
- Create: `crates/client-streams/src/store/cache/thread.rs`
- Modify: `crates/client-streams/src/store/mod.rs` (add `pub(crate) mod cache;`)

Ground truth: Apache Kafka `org.apache.kafka.streams.state.internals.NamedCache`, `ThreadCache`, `LRUCacheEntry`, and their tests `NamedCacheTest` / `ThreadCacheTest`.

- [ ] **Step 1: Write the failing test for `LruCacheEntry` byte sizing**

In `entry.rs`, add the type and a `#[cfg(test)]` test. The size formula is ported verbatim from Kafka's `LRUCacheEntry.size()`:
`value.len + 8 (timestamp) + 8 (offset) + 4 (partition) + topic.len + headers bytes + key.len + NODE overhead`. Kafka's per-entry overhead constants are: `48` (LRUNode references) + `key.length`. Use this struct:

```rust
//! A single record-cache entry: value bytes (None = tombstone), dirty flag, and
//! the record context needed to forward the entry downstream on flush.
use bytes::Bytes;

use crate::processor::record::RecordContext;

#[derive(Clone, Debug)]
pub(crate) struct LruCacheEntry {
    pub value: Option<Bytes>,
    pub dirty: bool,
    pub context: RecordContext,
}

impl LruCacheEntry {
    pub fn new(value: Option<Bytes>, dirty: bool, context: RecordContext) -> Self {
        Self { value, dirty, context }
    }

    /// Heap footprint used for ThreadCache budget accounting. Ports Kafka's
    /// `LRUCacheEntry.size()`: value bytes + record-context overhead. The key
    /// length is added by `NamedCache` (it owns the key). Context overhead =
    /// 8 (timestamp) + 8 (offset) + 4 (partition) + topic bytes. (Crabka's
    /// `RecordContext` has no headers field — see `src/processor/record.rs`.)
    pub fn value_size(&self) -> usize {
        let v = self.value.as_ref().map_or(0, Bytes::len);
        let ctx = 8 + 8 + 4 + self.context.topic.len();
        v + ctx
    }
}
```

Test:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::processor::record::RecordContext;

    #[test]
    fn value_size_counts_value_and_context() {
        // RecordContext { topic, partition, offset, timestamp }
        let ctx = RecordContext { topic: "t".into(), partition: 0, offset: 5, timestamp: 100 };
        let e = LruCacheEntry::new(Some(Bytes::from_static(b"abcd")), true, ctx);
        // 4 (value) + 8 + 8 + 4 + 1 (topic "t") = 25
        assert_eq!(e.value_size(), 25);
    }
}
```

> NOTE: `RecordContext` (`src/processor/record.rs`) is `{ topic: String, partition: i32, offset: i64, timestamp: i64 }` — construct it directly (no helper needed).

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-client-streams cache::entry -- --nocapture`
Expected: FAIL (module/type not found).

- [ ] **Step 3: Implement `entry.rs`** as shown above (+ any `RecordContext` helpers). Add `pub(crate) mod entry;` to `cache/mod.rs`. Add `pub(crate) mod cache;` to `store/mod.rs`.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-client-streams cache::entry`
Expected: PASS.

- [ ] **Step 5: Write the failing `NamedCache` tests (ported `NamedCacheTest`)**

In `named.rs`, define `NamedCache` and a `FlushListener` alias, then port these scenarios as tests:
- `put_get`: put 3 keys, `get` returns them; size = sum of (key.len + value_size).
- `lru_eviction_order`: access key A then B then C; `evict` removes the **least-recently-used** (A); a `get` on B promotes it.
- `dirty_flush_in_insertion_order`: put A,B,C all dirty; `flush(listener)` calls the listener for A,B,C **in insertion order**, then all are clean.
- `evict_flushes_dirty_head`: evicting a dirty head calls the listener once for that key before removal.
- `tombstone`: `delete(k)` stores `value=None` dirty; `get(k)` returns `Some(entry with value None)` (cache hit, tombstone).

```rust
//! Per-store LRU cache: doubly-linked LRU over `Bytes -> LruCacheEntry`, with a
//! dirty-key set in insertion order. Ports Apache Kafka `NamedCache`.
use std::collections::HashMap;

use bytes::Bytes;

use crate::store::cache::entry::LruCacheEntry;

/// Called per dirty entry on flush/evict: `(key, entry)`. The store wrapper
/// supplies this to write-through + forward.
pub(crate) type FlushListener<'a> = dyn FnMut(&Bytes, &LruCacheEntry) + 'a;

pub(crate) struct NamedCache {
    name: String,
    map: HashMap<Bytes, Node>,
    head: Option<Bytes>, // most-recently-used
    tail: Option<Bytes>, // least-recently-used (evicted first)
    dirty_order: Vec<Bytes>,
    size_bytes: usize,
}
// Node holds entry + prev/next keys for the LRU list. (Implement with key links;
// a HashMap<Bytes,Node> + head/tail keys avoids unsafe.)
```

Implement `new(name)`, `get(&self, &Bytes) -> Option<&LruCacheEntry>` (and a `get_promote(&mut self,..)` that moves to head), `put(key, entry)`, `delete(key, context)`, `flush(&mut self, &mut FlushListener)`, `evict(&mut self, &mut FlushListener) -> usize` (frees & returns bytes), `size_bytes()`, `len()`. Entry size in `size_bytes` = `key.len() + entry.value_size()`.

- [ ] **Step 6: Run to verify the NamedCache tests fail**, then implement, then verify pass.

Run: `cargo test -p crabka-client-streams cache::named`
Expected: FAIL → (implement) → PASS.

- [ ] **Step 7: Write the failing `ThreadCache` tests (ported `ThreadCacheTest`)**

In `thread.rs`:
- `over_budget_evicts_lru`: `max_bytes` small; put entries across two named caches until total > budget; assert `maybe_evict` drove eviction so `total <= max_bytes` and the LRU-oldest entries were evicted first (listener recorded them).
- `zero_budget_is_noop_passthrough`: with `max_bytes = 0`, this type isn't instantiated (the wrapper isn't created) — assert the constructor rejects/ô is never called with 0 OR returns an `is_enabled()==false`. (Pick: `ThreadCache::new(0)` returns a cache with `enabled()==false`; wrappers check `enabled()` and skip caching.)

```rust
//! Thread-wide record cache: a budget shared across NamedCaches. Ports Kafka
//! `ThreadCache`. One per task; budget = statestore.cache.max.bytes / task_count.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::store::cache::named::NamedCache;

pub(crate) struct ThreadCache {
    caches: HashMap<String, Arc<Mutex<NamedCache>>>,
    max_bytes: usize,
}

impl ThreadCache {
    pub fn new(max_bytes: usize) -> Self { Self { caches: HashMap::new(), max_bytes } }
    pub fn enabled(&self) -> bool { self.max_bytes > 0 }
    pub fn register(&mut self, name: &str) -> Arc<Mutex<NamedCache>> { /* insert + return */ }
    /// Total bytes across all named caches.
    pub fn total_bytes(&self) -> usize { /* sum */ }
    /// Evict LRU across caches (oldest-first) until total <= max_bytes, running
    /// each evicted dirty entry through its cache's flush listener.
    pub fn maybe_evict(&mut self, listeners: &mut FlushListenerMap) { /* ... */ }
}
```

> NOTE: cross-cache LRU ordering in Kafka picks the cache whose head is globally oldest. For the deterministic test, a simpler correct policy is acceptable as long as `over_budget_evicts_lru` passes: evict from the cache that just exceeded budget first, then others. Document the chosen policy in a doc comment.

- [ ] **Step 8: Run to verify ThreadCache tests fail**, implement, verify pass.

Run: `cargo test -p crabka-client-streams cache::thread`
Expected: FAIL → PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/client-streams/src/store/cache crates/client-streams/src/store/mod.rs crates/client-streams/src/processor/record.rs
git commit -m "feat(client-streams): record-cache core (LruCacheEntry/NamedCache/ThreadCache)"
```

---

## Task 2: Config — `Materialized::with_caching` + `cache_max_bytes` plumbing

**Files:**
- Modify: `crates/client-streams/src/dsl/config.rs` (add `caching: bool` to `Materialized` + `with_caching`)
- Modify: `crates/client-streams/src/streams_app.rs` (add `cache_max_bytes` to the `bon` builder, default `10_485_760`)
- Modify: `crates/client-streams/src/runtime/app.rs` (where `KafkaStreams`/`KafkaStreams::builder()` lives) — thread `cache_max_bytes` to the graph/task
- Modify: `crates/client-streams/src/test_driver.rs` (force `cache_max_bytes = 0`)

> This task only PLUMBS the number and the per-store flag; behavior arrives in Batch 3. Tests assert the flag/value round-trip. Disjoint from Task 1's files.

- [ ] **Step 1: Failing test for `Materialized::with_caching`**

In `config.rs` tests:

```rust
#[test]
fn materialized_caching_defaults_on_and_toggles() {
    use crate::processor::serde::{StringSerde, I64Serde};
    let m = Materialized::with(StringSerde, I64Serde);
    assert!(m.caching_enabled());            // default true
    let m = m.with_caching(false);
    assert!(!m.caching_enabled());
}
```

- [ ] **Step 2: Run, expect FAIL** (`with_caching`/`caching_enabled` missing).

Run: `cargo test -p crabka-client-streams materialized_caching_defaults`

- [ ] **Step 3: Implement.** Add `pub(crate) caching: bool` to `Materialized` (default `true` in `with`), and:

```rust
/// Enable/disable the record cache for this store (JVM
/// `Materialized.withCachingEnabled/Disabled`). Default enabled.
#[must_use]
pub fn with_caching(mut self, on: bool) -> Self { self.caching = on; self }

#[must_use]
pub fn caching_enabled(&self) -> bool { self.caching }
```

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Plumb `cache_max_bytes`.** Add to the `StreamsApp` `bon` builder:

```rust
/// Total record-cache budget in bytes (statestore.cache.max.bytes). Default 10 MiB.
#[builder(default = 10_485_760)] cache_max_bytes: i64,
```

Store it on `StreamsApp`, pass to `KafkaStreams::builder().cache_max_bytes(..)`, and store on the graph/task as `cache_max_bytes: i64`. In `test_driver.rs`, build the graph with `cache_max_bytes = 0`.

- [ ] **Step 6: Failing test that the test driver disables caching.**

Add a test asserting `TopologyTestDriver`'s built graph reports `cache_max_bytes == 0` (expose a `pub(crate) fn cache_max_bytes(&self) -> i64` on the graph or task for the test).

Run: `cargo test -p crabka-client-streams test_driver_disables_cache`
Expected: FAIL → (implement) → PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/client-streams/src/dsl/config.rs crates/client-streams/src/streams_app.rs crates/client-streams/src/runtime crates/client-streams/src/test_driver.rs
git commit -m "feat(client-streams): plumb statestore.cache.max.bytes + Materialized::with_caching"
```

---

## Task 3: `CachingKeyValueStore`

**Files:**
- Create: `crates/client-streams/src/store/cache/kv.rs`
- Modify: `crates/client-streams/src/store/cache/mod.rs` (export)

Ground truth: Apache Kafka `CachingKeyValueStore` + `CachingKeyValueStoreTest`. The wrapper sits at the **byte** level, wrapping the inner `KeyValueBytesStore`'s byte backend semantics, and is consulted by the typed store path. It holds `Arc<Mutex<NamedCache>>` (from `ThreadCache::register`) and a write-through closure to the underlying byte store.

Design seam: implement caching at the byte layer so it is reusable across K/V types. Expose:

```rust
//! Byte-level caching wrapper over a ByteKeyValueStore. Cache-first reads,
//! write-back puts, merged range. Ports Kafka CachingKeyValueStore.
pub(crate) struct CachingKeyValueStore {
    cache: Arc<Mutex<NamedCache>>,
    inner: Box<dyn ByteKeyValueStore>,
    // record context for the current process() call, set by the typed store path
    // before each put (so the cached entry carries offset/ts/partition/topic).
}
```

- [ ] **Step 1: Failing tests (ported `CachingKeyValueStoreTest`)**
  - `get_returns_cached_before_underlying`: put k→v (cached, not flushed); underlying still empty; `get(k)` returns v.
  - `flush_writes_through_and_invokes_listener`: put k→v; `flush(listener)`; underlying now has v; listener saw `(k, Some(v))` once.
  - `tombstone_hides_underlying`: underlying has k→v0; `delete(k)` (cached tombstone); `get(k)` returns None.
  - `range_merges_cache_and_underlying_cache_wins`: underlying {a→1, c→3}; cache {b→2, a→9}; `range(a..=c)` yields a→9, b→2, c→3 in key order.

- [ ] **Step 2: Run, expect FAIL.** `cargo test -p crabka-client-streams cache::kv`

- [ ] **Step 3: Implement `CachingKeyValueStore`** with `get`/`put`/`delete`/`range`/`flush(listener)` over `cache` + `inner`, using `MergedSortedCacheIterator` semantics (cache wins, tombstones skip). Add `pub(crate) mod kv;` to `cache/mod.rs`.

- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/store/cache/kv.rs crates/client-streams/src/store/cache/mod.rs
git commit -m "feat(client-streams): CachingKeyValueStore (cache-first reads, write-back, merged range)"
```

---

## Task 4: `CachingWindowStore`

**Files:**
- Create: `crates/client-streams/src/store/cache/window.rs`
- Modify: `crates/client-streams/src/store/cache/mod.rs` (export)

Ground truth: Apache Kafka `CachingWindowStore` + `CachingWindowStoreTest`. Cache key = the existing window key-schema bytes (`src/store/window_schema.rs` — reuse its `(key, window_start, seqnum)` encode/decode; do NOT invent a new layout). Mirror Task 3 but over the window byte store and `fetch(key, timeFrom, timeTo)` / `fetchAll(timeFrom, timeTo)` merged iterators.

- [ ] **Step 1: Failing tests (ported `CachingWindowStoreTest`)**: `fetch_returns_cached`, `flush_writes_through`, `fetch_merges_cache_and_underlying_in_window_order`. Use `window_schema` to build cache keys.
- [ ] **Step 2: Run, expect FAIL.** `cargo test -p crabka-client-streams cache::window`
- [ ] **Step 3: Implement** over the window byte store + `window_schema` key bytes.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/store/cache/window.rs crates/client-streams/src/store/cache/mod.rs
git commit -m "feat(client-streams): CachingWindowStore (window-schema-keyed cache, merged fetch)"
```

---

## Task 5: `CachingSessionStore`

**Files:**
- Create: `crates/client-streams/src/store/cache/session.rs`
- Modify: `crates/client-streams/src/store/cache/mod.rs` (export)

Ground truth: Apache Kafka `CachingSessionStore` + `CachingSessionStoreTest`. Cache key = the session key-schema bytes (`src/store/session_schema.rs` — reuse its `(key, end, start)` encode/decode). Mirror Task 4 but over `findSessions(key, earliestEnd, latestStart)`.

- [ ] **Step 1: Failing tests (ported `CachingSessionStoreTest`)**: `find_sessions_returns_cached`, `flush_writes_through`, `find_sessions_merges_cache_and_underlying`.
- [ ] **Step 2: Run, expect FAIL.** `cargo test -p crabka-client-streams cache::session`
- [ ] **Step 3: Implement** over the session byte store + `session_schema` key bytes.
- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/store/cache/session.rs crates/client-streams/src/store/cache/mod.rs
git commit -m "feat(client-streams): CachingSessionStore (session-schema-keyed cache, merged findSessions)"
```

---

## Task 6: `TupleForwarder` + node↔store ownership + `graph.flush_caches` + registry wrapping

**Files:**
- Create: `crates/client-streams/src/dsl/processors/tuple_forwarder.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs` (export)
- Modify: `crates/client-streams/src/store/registry.rs` (wrap on materialize when `cache_max_bytes>0 && caching`)
- Modify: `crates/client-streams/src/processor/graph.rs` (`flush_caches`, hold `ThreadCache`, node↔store map, node-rooted flush dispatch mirroring `fire_schedule`)

- [ ] **Step 1: Failing test — `TupleForwarder` suppresses when cached**

In `tuple_forwarder.rs`:

```rust
//! Forward-suppression seam for materializing processors. Mirrors JVM
//! `TimestampedTupleForwarder`: when the backing store is cached, the immediate
//! downstream forward is suppressed (the cache flush listener forwards instead);
//! otherwise it forwards `Change::update(old, new)` immediately.
use crate::dsl::processors::change::Change;
use crate::processor::api::ProcessorContext;
use crate::processor::record::Record;

pub(crate) struct TupleForwarder {
    cached: bool,
}

impl TupleForwarder {
    /// Resolve `cached` from the context's store registry at processor `init`.
    pub fn resolve(cached: bool) -> Self { Self { cached } }

    pub fn maybe_forward<K, VA>(
        &self,
        ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>,
        key: K,
        old: Option<VA>,
        new: VA,
        ts: i64,
    ) where
        K: std::any::Any + Send + Clone,
        VA: std::any::Any + Send + Clone,
    {
        if self.cached {
            return; // cache flush listener will forward the deduped Change
        }
        ctx.forward(Record::new(Some(key), Change::update(old, new), ts));
    }
}
```

Test: `maybe_forward` with `cached=false` forwards one record (assert via a `Dispatch` capturing output, following the existing `aggregate.rs` test harness pattern); with `cached=true` forwards nothing.

- [ ] **Step 2: Run, expect FAIL.** `cargo test -p crabka-client-streams tuple_forwarder`
- [ ] **Step 3: Implement** `tuple_forwarder.rs`; export from `processors/mod.rs`.
- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Failing test — registry wraps a materialized KV store when caching enabled**

In `registry.rs`, add the wrap-on-insert path: when the graph's `cache_max_bytes > 0` and the `Materialized.caching` flag is set, the store the registry instantiates is backed by a `CachingKeyValueStore` (register a `NamedCache` in the task `ThreadCache`) and the typed flush listener is recorded against the store's owning node. Test: build a registry with `cache_max_bytes>0`, insert a cached KV store, assert it reports `is_cached()==true`; with `cache_max_bytes==0`, `is_cached()==false`.

- [ ] **Step 6: Run, expect FAIL → implement → PASS.**

- [ ] **Step 7: Failing test — `graph.flush_caches` forwards deduped Change rooted at the owning node**

Build a minimal graph: source → aggregate(count, cached store) → capture sink. Pipe two records for key `a` WITHOUT a commit; assert no output yet (suppressed). Call `graph.flush_caches()`; assert exactly one output `Change{old:None, new:2}` for `a` at the last record's context. Implement `flush_caches` to, for each cached store, look up its owning node index, reconstruct a `Dispatch` rooted there (copy the `fire_schedule` pattern at `graph.rs:130-165`), and run the store's typed flush listener pushing erased `Change` records into `children`.

```rust
/// Flush every cached store: forward each dirty entry's deduped `Change`
/// downstream (rooted at the store's owning node) and write it through to the
/// underlying store. Mirrors `fire_schedule`'s node-rooted Dispatch. Called from
/// the task commit/close path before the producer commits.
pub async fn flush_caches(&mut self) -> Result<(), ProcessorError> { /* ... */ }
```

- [ ] **Step 8: Run, expect FAIL → implement → PASS.** `cargo test -p crabka-client-streams flush_caches`

- [ ] **Step 9: Commit**

```bash
git add crates/client-streams/src/dsl/processors/tuple_forwarder.rs crates/client-streams/src/dsl/processors/mod.rs crates/client-streams/src/store/registry.rs crates/client-streams/src/processor/graph.rs
git commit -m "feat(client-streams): TupleForwarder seam + cached-store registry wrapping + graph.flush_caches"
```

---

## Task 7: Wire materializing processors + task commit/close flush

**Files:**
- Modify: `crates/client-streams/src/dsl/processors/aggregate.rs` (`KStreamAggregate`, `KStreamReduce`)
- Modify: `crates/client-streams/src/dsl/processors/table.rs` (`KTableSource`)
- Modify: `crates/client-streams/src/dsl/processors/table_aggregate.rs` (`KTableAggregate`)
- Modify: `crates/client-streams/src/dsl/processors/window_aggregate.rs`
- Modify: `crates/client-streams/src/dsl/processors/session_aggregate.rs`
- Modify: `crates/client-streams/src/dsl/processors/sliding_window_aggregate.rs`
- Modify: `crates/client-streams/src/runtime/task.rs` (call `flush_caches` in `commit` + `close`)

For EACH materializing processor: add a `forwarder: TupleForwarder` field, resolve it in `init` (`TupleForwarder::resolve(ctx.store_is_cached(&store_name))` — add `store_is_cached` to `ProcessorContext`/registry), and replace the existing `ctx.forward(Record::new(Some(key), Change::update(old, new), ts))` tail with `self.forwarder.maybe_forward(ctx, key, old, new, ts)`. The store `get`/`put` calls are UNCHANGED (a cached store's typed view writes into the `NamedCache`).

Pattern (apply to `aggregate.rs` `KStreamAggregateProcessor`, then repeat for each):

```rust
// field:
pub forwarder: TupleForwarder,
// init:
async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<VA>>) {
    self.forwarder = TupleForwarder::resolve(ctx.store_is_cached(&self.store_name));
}
// process tail (replacing the ctx.forward call):
self.forwarder.maybe_forward(ctx, key, old, new, r.timestamp);
```

- [ ] **Step 1: Failing behavioral test — cached aggregate dedups across commit**

In `aggregate.rs` tests (or a new `tests/` module), build an aggregate with a cached store; pipe key `a` three times; assert NO output before flush; call the graph flush; assert ONE `Change{old:None, new:3}`. Build the same with cache=0; assert THREE outputs (today's behavior). This is the per-processor regression+behavior gate.

- [ ] **Step 2: Run, expect FAIL.** `cargo test -p crabka-client-streams aggregate`
- [ ] **Step 3: Implement the `TupleForwarder` swap in all six processor files** + `ProcessorContext::store_is_cached` + the constructors/suppliers that build these processors (set `forwarder` to an uninitialized default; it's resolved in `init`). Update the DSL lowering call sites if the processor constructors gained a field (use `TupleForwarder::resolve(false)` as the struct default so non-`init` construction still compiles).
- [ ] **Step 4: Run, expect PASS.**

- [ ] **Step 5: Failing test — `task.commit` flushes caches before commit**

In `task.rs` tests, drive a task with a cached store, process records, call `commit()`; assert the cache was flushed (output emitted + underlying store written + offsets committed, in that order). Also assert `close()` flushes.

- [ ] **Step 6: Run, expect FAIL.** `cargo test -p crabka-client-streams task`
- [ ] **Step 7: Implement** — in `task.commit()` call `self.graph.flush_caches().await?` then `drain_changelogs`/sink-produce (reuse the punctuation drain path) BEFORE the existing producer commit; in `task.close()` flush then close.
- [ ] **Step 8: Run, expect PASS.**

- [ ] **Step 9: Regression guard — full suite green (disabled-path invariant)**

Run: `cargo test -p crabka-client-streams`
Expected: PASS — every existing golden unchanged (all run cache=0 via the test driver).

- [ ] **Step 10: Commit**

```bash
git add crates/client-streams/src/dsl/processors crates/client-streams/src/runtime/task.rs crates/client-streams/src/processor
git commit -m "feat(client-streams): wire materializing processors to TupleForwarder + flush caches on commit/close"
```

---

## Task 8: Docker emit-dedup golden + CI + final gate

**Files:**
- Create: `crates/client-streams/tests/jvm-capture/.../RecordCacheTopology.java`
- Create: `crates/client-streams/tests/testdata/record_cache/commit_dedup.json`
- Create: `crates/client-streams/tests/record_cache_golden.rs`
- Modify: `.github/workflows/ci.yml` (add `record_cache_golden` to the crate's llvm-cov `--test` list)

- [ ] **Step 1: Author `RecordCacheTopology.java`** — a `count`/`reduce` into a materialized table with `statestore.cache.max.bytes` set large (no mid-batch eviction) and `commit.interval.ms` controlled; feed a fixed batch of repeated-key updates; emit the output topic. Build against `apache/kafka:4.1.0` (single-broker Streams capture works on Mac — the emit-final/kgrouped precedent).

- [ ] **Step 2: Capture `commit_dedup.json`** — the output-topic records (only the final value per key after one commit). Document the exact docker command in a header comment, mirroring existing `tests/testdata/*` capture docs.

- [ ] **Step 3: Write `record_cache_golden.rs`** — build the same topology in crabka with a **caching-enabled** `TopologyTestDriver` (the test-only ctor from Task 2), pipe the same inputs, call explicit `commit()`/flush, and assert the output equals `commit_dedup.json` byte-for-byte.

- [ ] **Step 4: Run, expect PASS.** `cargo test -p crabka-client-streams --test record_cache_golden`

- [ ] **Step 5: Add `record_cache_golden` to CI llvm-cov `--test` list** in `.github/workflows/ci.yml` (per the per-crate-integration coverage convention — otherwise codecov reports 0% for the patch).

- [ ] **Step 6: Final verification gate**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-client-streams
cargo build --workspace
```
(Touch suspect-but-unchanged files to defeat the clippy cache; check the real `$?`.)

- [ ] **Step 7: Commit**

```bash
git add crates/client-streams/tests .github/workflows/ci.yml
git commit -m "test(client-streams): record-cache commit-dedup JVM golden + CI coverage"
```

---

## Self-review notes (coverage map)

- Spec §3.1 emit-dedup → Tasks 6–8 (TupleForwarder + flush_caches + golden).
- Spec §3.2 read-your-writes → Tasks 3–5 (cache-first get/range/fetch/findSessions).
- Spec §3.3 eviction + byte sizing → Task 1 (`ThreadCache::maybe_evict`, `LruCacheEntry::value_size`).
- Spec §3.4 disabled path → Task 2 (cache=0 in test driver) + Task 7 step 9 regression guard.
- Spec §4.7 config → Task 2.
- Spec §4 store wrappers → Tasks 3–5.
- Spec §4.5 TupleForwarder + typed flush listener + node rooting → Tasks 6–7.
- Spec §4.6 EOS flush ordering → Task 7 (flush before producer commit).
- Spec §5 testing → unit ports in Tasks 1/3/4/5; Docker golden in Task 8; regression guard in Task 7.
