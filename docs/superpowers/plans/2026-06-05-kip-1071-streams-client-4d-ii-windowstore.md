# KIP-1071 Streams Client #4d-ii — WindowStore + windowed aggregations — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `windowedBy(TimeWindows).{count,reduce,aggregate}` (tumbling + hopping) producing a `KTable<Windowed<K>, V>`, backed by a window store over the 4d-i byte backend, byte-exact vs JVM 4.1.

**Architecture:** A `WindowBytesStore<K,V>` (second typed store over the existing async `ByteKeyValueStore`) encodes JVM `WindowKeySchema` keys (`key‖windowStart:8BE‖seq:4BE=0`) + `ValueAndTimestamp` values (`ts:8BE‖agg`), using `range` for fetch. A `KStreamWindowAggregateProcessor` emits a `Change<VA>` per matched window (emit-on-update; no closing). `windowed_by` on `KGroupedStream` lowers like the non-windowed aggregate but registers a *windowed* store whose changelog wire config is `compact,delete` + `retention.ms`.

**Tech Stack:** Rust 2024; async-trait; extends 4d-i. JVM capture via Docker Kafka-Streams 4.1.

**Spec:** `docs/superpowers/specs/2026-06-05-kip-1071-streams-client-4d-ii-windowstore-design.md`.
**Branch:** `streams-4d-ii-windowstore` (stacked on `streams-4d-async-stores` / PR #391; rebase onto `main` once #391 merges). Worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

---

## Current signatures (verbatim — what we extend)

- `store/byte.rs`: `#[async_trait] pub(crate) trait ByteKeyValueStore: Send + Sync { async fn get(&self,&[u8])->Option<Bytes>; async fn put(&mut self,Bytes,Bytes); async fn delete(&mut self,&[u8])->Option<Bytes>; async fn range(&self,lo:&[u8],hi:&[u8])->Vec<(Bytes,Bytes)> }` (half-open `[lo,hi)`, memcmp order). `InMemoryBytes` + `TursoBytes` impl it.
- `store/kv.rs`: `KeyValueBytesStore<K,V> { name, changelog_topic, backend: Box<dyn ByteKeyValueStore>, key_serde: Box<dyn Serde<K>>, value_serde: Box<dyn Serde<V>>, changelog: Vec<(Bytes,Option<Bytes>)>, logging: bool }` + `pub(crate) fn new(name, backend, key_serde, value_serde, changelog_topic)`. Implements async `StateStore` + `KeyValueStore<K,V>`.
- `store/api.rs`: `#[async_trait] pub trait StateStore: Any+Send { fn name; async fn flush; fn close; fn as_any_mut; fn changelog_topic; fn take_changelog->Vec<(Bytes,Option<Bytes>)>; async fn apply_changelog(key,value); fn set_logging }`. `#[async_trait] pub trait KeyValueStore<K:Send+Sync,V:Send>: StateStore { async fn get(&self,&K)->Option<V>; async fn put(&mut self,K,V); async fn delete(&mut self,&K)->Option<V> }`.
- `store/registry.rs`: `pub fn get_kv<K:Send+Sync+'static,V:Send+'static>(&mut self,name)->Option<&mut dyn KeyValueStore<K,V>>` = `self.stores.get_mut(name)?.as_any_mut().downcast_mut::<KeyValueBytesStore<K,V>>()?`. `pub fn insert(Box<dyn StateStore>)`. `pub fn get_mut(name)->Option<&mut dyn StateStore>`.
- `store/backend.rs`: `StoreBackend::{InMemory, Turso{state_dir}}`; `pub(crate) async fn open(&self, app_id, store) -> Box<dyn ByteKeyValueStore>`.
- `processor/api.rs`: `#[async_trait] pub trait Processor<KIn:Send,VIn:Send,KOut:Send,VOut:Send>: Send+'static { async fn init; async fn process(&mut self, ctx: &mut ProcessorContext<'_,'_,KOut,VOut>, record: Record<KIn,VIn>); async fn close }`. `ProcessorContext::get_state_store<K2:Send+Sync+'static,V2:Send+'static>(name)->Option<&mut dyn KeyValueStore<K2,V2>>` = `self.dispatch.stores.get_kv::<K2,V2>(name)`. `forward(Record)` is sync.
- `processor/serde.rs`: `pub trait Serde<T>: Send+Sync+'static { fn serialize(&self,&T)->Bytes; fn deserialize(&self,&[u8])->Result<T,SerdeError> }`. `I64Serde` = `i64::to_be_bytes`/`from_be_bytes`. `StringSerde`. `Produced::with(ks,vs)` / `Consumed::with(ks,vs)`.
- `dsl/kgrouped.rs`: `KGroupedStream<K,V> { builder, parent: NodeId, key_changing_upstream: bool, grouped_name, repartition_lower: Option<RepartitionLowerFn>, _pd }` with `impl where K: Any+Send+Sync+Clone, V: Any+Send+Clone`. `count(Materialized)->KTable<K,i64>` / `reduce(reducer,M)->KTable<K,V>` / `aggregate(init,agg,M)->KTable<K,VA>`. Private `aggregate_inner`/`lower_aggregate`/`lower_reduce`/`record_repartition(g,store_name,parent,key_changing,rp_lower)->NodeId` (mints FILTER+SINK+SOURCE names + Repartition node when key-changing). `mint_store_name(builder,&materialized,prefix)`. The aggregate thunk: `g.new_processor_name(names::AGGREGATE)`, `g.graph.add(name, GraphNodeKind::Aggregate{store_name, changelog: logging}, [agg_parent])`, thunk `add_processor::<K,V,K,Change<VA>,_,_,_>(name, ||KStreamAggregateProcessor{store_name,init,agg,_pd}, [parent])` + `add_state_store::<K,VA,KS,VS>(store, ks, vs, [h.name])` (or `add_state_store_no_changelog`), `handle_name.insert(agg_id, h.name)`. Returns `KTable::new(builder, agg_id, Some(store_name), None)`.
- `dsl/processors/aggregate.rs`: `KStreamAggregateProcessor<K,V,VA,I,A> { store_name, init, agg, _pd }` (`Processor<K,V,K,Change<VA>>`, async); reads `ctx.get_state_store::<K,VA>(&store).get(&key).await`, seeds with `init` when None, `agg(&key,&value,seed)`, `put`, forwards `Change::update(old,new)`. `KStreamReduceProcessor<K,V,R>` (first value seeds).
- `topology/node.rs`: `pub(crate) struct StoreEntry { pub name, pub processors: Vec<String>, pub changelog_override: Option<String> }`; `NodeRegistry::add_store(name, procs, changelog_override)`.
- `topology/grouping.rs`: `GroupTopics.changelog_stores: Vec<(String /*store*/, Option<String> /*changelog_override*/)>`, populated `g.changelog_stores.push((store.name.clone(), store.changelog_override.clone()))`.
- `topology/wire.rs`: `changelog_topic_configs() -> [("cleanup.policy","compact"),("message.timestamp.type","CreateTime")]`; `repartition_topic_configs()` for contrast; `state_changelog_topics` maps each `(store, changelog_override)` to a `TopicInfo { name: override.unwrap_or(<app>-<store>-changelog), partitions:0, replication_factor:-1, topic_configs: changelog_topic_configs() }` sorted by name. `topic_configs([(k,v)…])` helper.
- `topology/builder.rs`: `type StoreFactory = Box<dyn Fn(&str /*store*/, String /*changelog*/, Box<dyn ByteKeyValueStore>) -> Box<dyn StateStore> + Send + Sync>`; `store_factories: HashMap<String,(Option<String>,StoreFactory)>`; `add_state_store::<K,V,KS,VS>(name,ks,vs,procs)` → `add_state_store_inner(…, changelog_override=None)`; `add_state_store_no_changelog`; `instantiate(&backend, app_id)` (async) opens each backend + builds via factory.
- `dsl/names.rs`: `AGGREGATE="KSTREAM-AGGREGATE-"`, `AGGREGATE_STORE="KSTREAM-AGGREGATE-STATE-STORE-"`, `REDUCE="KSTREAM-REDUCE-"`, `REDUCE_STORE="KSTREAM-REDUCE-STATE-STORE-"`. `new_processor_name(prefix)` = `format!("{prefix}{:010}", index++)`.

## File structure

```
dsl/windows.rs               NEW — Window, Windowed<K>, TimeWindows (windows_for), TimeWindowedSerde
store/window_schema.rs       NEW — WindowKeySchema key codec + ValueAndTimestamp value codec (byte fns)
store/window.rs              NEW — WindowStore<K,V> trait + WindowBytesStore<K,V>
store/api.rs                 + WindowStore is declared in window.rs (re-exported); api.rs unchanged
store/registry.rs            + get_window::<K,V>
store/mod.rs                 + mod window; mod window_schema;
processor/api.rs             + ProcessorContext::get_window_store::<K,V>
dsl/processors/window_aggregate.rs  NEW — KStreamWindowAggregateProcessor
dsl/windowed_kgrouped.rs     NEW — TimeWindowedKGroupedStream<K,V> + count/reduce/aggregate + lowering
dsl/kgrouped.rs              + windowed_by(TimeWindows)
dsl/mod.rs / lib.rs          re-export Window, Windowed, TimeWindows, TimeWindowedSerde
topology/node.rs             StoreEntry + windowed_retention_ms; add_store gains it
topology/builder.rs          + add_window_store::<K,V,KS,VS>(name,ks,vs,size_ms,grace_ms,procs)
topology/grouping.rs         changelog_stores carries windowed_retention_ms
topology/wire.rs             + windowed_changelog_topic_configs(retention_ms); per-store choice
tests/jvm-capture/.../Capture.java   + windowedCount topology
tests/testdata/golden/dsl/windowed_count.topology.json   NEW
tests/dsl_golden_frame.rs    + windowed_count golden
tests/dsl_execution.rs       + windowed tumbling/hopping count/reduce/aggregate
lib.rs                       windowed-aggregation doc note
```

**Batching:** Task 1 + Task 2 are disjoint (`dsl/windows.rs` vs `store/window_schema.rs`) → parallel. Then sequential: 3 (window store, needs 2) → 4 (wire config) → 5 (processor, needs 3) → 6 (DSL, needs 1,4,5) → 7 (golden, needs 6) → 8 (docs).

---

## Task 1: window types + `windows_for` + `Windowed<K>` + `TimeWindowedSerde`

**Files:** Create `src/dsl/windows.rs`; modify `src/dsl/mod.rs` (+ `pub mod windows;`), `src/lib.rs` (re-export).

- [ ] **Step 1: failing tests** — in `src/dsl/windows.rs` `#[cfg(test)] mod tests`:
```rust
#[test]
fn windows_for_tumbling_one_window() {
    let w = TimeWindows::of_size(10);
    assert_eq!(w.windows_for(0), vec![0]);
    assert_eq!(w.windows_for(9), vec![0]);
    assert_eq!(w.windows_for(10), vec![10]);
    assert_eq!(w.windows_for(25), vec![20]);
}
#[test]
fn windows_for_hopping_overlaps() {
    let w = TimeWindows::of_size(10).advance_by(5);
    // t=12: start0 = max(0,12-10+5)/5*5 = (7/5)*5 = 5; emit 5,10 (15>12 stops)
    assert_eq!(w.windows_for(12), vec![5, 10]);
    assert_eq!(w.windows_for(0), vec![0]); // start0 = max(0,-5)/5*5 = 0
}
#[test]
fn time_windowed_serde_round_trips_output_format() {
    use crate::processor::serde::{Serde, StringSerde};
    let s = TimeWindowedSerde::new(StringSerde, 10);
    let wk = Windowed { key: "k".to_string(), window: Window { start: 20, end: 30 } };
    let b = s.serialize(&wk);
    // layout: "k" (1 byte) ‖ 20i64 BE (8 bytes) = 9 bytes
    assert_eq!(b.len(), 9);
    assert_eq!(&b[1..9], &20i64.to_be_bytes());
    let back = s.deserialize(&b).unwrap();
    assert_eq!(back.key, "k");
    assert_eq!(back.window, Window { start: 20, end: 30 }); // end = start + size
}
```

- [ ] **Step 2: run → FAIL.** `cargo test -p crabka-client-streams --lib dsl::windows`

- [ ] **Step 3: implement** `src/dsl/windows.rs`:
```rust
//! Time windows + the `Windowed<K>` output key + a windowed output serde.
use bytes::{BufMut, Bytes, BytesMut};

use crate::processor::serde::{Serde, SerdeError};

/// A half-open time window `[start, end)` (epoch millis).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Window {
    pub start: i64,
    pub end: i64,
}

/// An aggregation key tagged with its window — the output key of a windowed
/// aggregation (`KTable<Windowed<K>, V>`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Windowed<K> {
    pub key: K,
    pub window: Window,
}

/// Tumbling / hopping time windows (epoch-aligned). `advance_ms == size_ms` is
/// tumbling; `advance_ms < size_ms` is hopping. `grace_ms` is recorded for the
/// changelog retention computation (window closing itself is deferred).
#[derive(Debug, Clone, Copy)]
pub struct TimeWindows {
    pub size_ms: i64,
    pub advance_ms: i64,
    pub grace_ms: i64,
}

impl TimeWindows {
    /// Tumbling window of `size_ms` (advance == size, grace 0).
    #[must_use]
    pub fn of_size(size_ms: i64) -> Self {
        assert!(size_ms > 0, "window size must be > 0");
        Self { size_ms, advance_ms: size_ms, grace_ms: 0 }
    }
    /// Hopping: advance by `advance_ms` (must be `0 < advance_ms <= size_ms`).
    #[must_use]
    pub fn advance_by(mut self, advance_ms: i64) -> Self {
        assert!(advance_ms > 0 && advance_ms <= self.size_ms, "0 < advance <= size");
        self.advance_ms = advance_ms;
        self
    }
    /// Set the grace period (only affects changelog retention here).
    #[must_use]
    pub fn grace(mut self, grace_ms: i64) -> Self {
        assert!(grace_ms >= 0, "grace must be >= 0");
        self.grace_ms = grace_ms;
        self
    }

    /// The window starts a timestamp `t` falls into (JVM `TimeWindows.windowsFor`).
    #[must_use]
    pub fn windows_for(&self, t: i64) -> Vec<i64> {
        let mut start = (std::cmp::max(0, t - self.size_ms + self.advance_ms) / self.advance_ms)
            * self.advance_ms;
        let mut out = Vec::new();
        while start <= t {
            out.push(start);
            start += self.advance_ms;
        }
        out
    }
}

/// `Serde<Windowed<K>>` producing the JVM **output-topic** format:
/// `inner_key_bytes ‖ windowStart : 8-byte BE` (no end, no seqnum). Carries the
/// window `size` so `deserialize` can reconstruct `end = start + size`.
#[derive(Debug, Clone, Copy)]
pub struct TimeWindowedSerde<KS> {
    inner: KS,
    size_ms: i64,
}

impl<KS> TimeWindowedSerde<KS> {
    #[must_use]
    pub fn new(inner: KS, size_ms: i64) -> Self {
        Self { inner, size_ms }
    }
}

impl<K, KS> Serde<Windowed<K>> for TimeWindowedSerde<KS>
where
    K: Send + Sync + 'static,
    KS: Serde<K>,
{
    fn serialize(&self, value: &Windowed<K>) -> Bytes {
        let kb = self.inner.serialize(&value.key);
        let mut b = BytesMut::with_capacity(kb.len() + 8);
        b.extend_from_slice(&kb);
        b.put_i64(value.window.start); // 8-byte BE
        b.freeze()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Windowed<K>, SerdeError> {
        if bytes.len() < 8 {
            return Err(SerdeError(format!("windowed key too short: {}", bytes.len())));
        }
        let split = bytes.len() - 8;
        let key = self.inner.deserialize(&bytes[..split])?;
        let start = i64::from_be_bytes(bytes[split..].try_into().expect("8 bytes"));
        Ok(Windowed { key, window: Window { start, end: start + self.size_ms } })
    }
}

#[cfg(test)]
mod tests { /* the Step-1 tests */ }
```
Add `pub mod windows;` to `src/dsl/mod.rs` and re-export from `src/lib.rs`: `pub use dsl::windows::{TimeWindowedSerde, TimeWindows, Window, Windowed};`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): TimeWindows + Windowed<K> + TimeWindowedSerde`.

---

## Task 2: `WindowKeySchema` key + `ValueAndTimestamp` value byte codecs

**Files:** Create `src/store/window_schema.rs`; modify `src/store/mod.rs` (+ `pub(crate) mod window_schema;`).

- [ ] **Step 1: failing tests** — `src/store/window_schema.rs` tests:
```rust
#[test]
fn store_key_layout_and_window_start() {
    let k = store_key(b"k", 0x0102);
    // "k"(1) ‖ 0x0102 as 8-byte BE ‖ 0u32 BE(4) = 13 bytes
    assert_eq!(k.len(), 13);
    assert_eq!(&k[1..9], &0x0102i64.to_be_bytes());
    assert_eq!(&k[9..13], &[0, 0, 0, 0]);
    assert_eq!(window_start_of(&k), 0x0102);
    assert_eq!(key_bytes_of(&k), b"k");
}
#[test]
fn value_wrap_unwrap() {
    let v = wrap_value(7, &99i64.to_be_bytes());
    assert_eq!(&v[0..8], &7i64.to_be_bytes()); // ts prefix
    let (ts, raw) = unwrap_value(&v);
    assert_eq!(ts, 7);
    assert_eq!(raw, &99i64.to_be_bytes());
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `src/store/window_schema.rs`:
```rust
//! JVM-exact windowed store/changelog byte codecs.
//! Store/changelog KEY  : `key_bytes ‖ windowStart:8B BE ‖ seqnum:4B BE` (seqnum 0 for aggregations).
//! Store/changelog VALUE: `recordTs:8B BE ‖ serialized aggregate` (ValueAndTimestamp); None = tombstone.
use bytes::{BufMut, Bytes, BytesMut};

const TS_SIZE: usize = 8;
const SEQ_SIZE: usize = 4;
pub(crate) const SUFFIX_SIZE: usize = TS_SIZE + SEQ_SIZE; // 12

/// `WindowKeySchema.toStoreKeyBinary(key, windowStart, seqnum=0)`.
pub(crate) fn store_key(key_bytes: &[u8], window_start: i64) -> Bytes {
    let mut b = BytesMut::with_capacity(key_bytes.len() + SUFFIX_SIZE);
    b.extend_from_slice(key_bytes);
    b.put_i64(window_start); // 8B BE
    b.put_u32(0); // seqnum = 0 (retainDuplicates=false)
    b.freeze()
}

/// The windowStart encoded in a composite store key.
pub(crate) fn window_start_of(store_key: &[u8]) -> i64 {
    let n = store_key.len();
    i64::from_be_bytes(store_key[n - SUFFIX_SIZE..n - SEQ_SIZE].try_into().expect("8 bytes"))
}

/// The serialized inner-key bytes of a composite store key.
pub(crate) fn key_bytes_of(store_key: &[u8]) -> &[u8] {
    &store_key[..store_key.len() - SUFFIX_SIZE]
}

/// `ValueAndTimestampSerializer`: `recordTs:8B BE ‖ raw`.
pub(crate) fn wrap_value(record_ts: i64, raw: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(TS_SIZE + raw.len());
    b.put_i64(record_ts);
    b.extend_from_slice(raw);
    b.freeze()
}

/// Split a wrapped value into `(recordTs, raw_aggregate_bytes)`.
pub(crate) fn unwrap_value(wrapped: &[u8]) -> (i64, &[u8]) {
    let ts = i64::from_be_bytes(wrapped[..TS_SIZE].try_into().expect("8 bytes"));
    (ts, &wrapped[TS_SIZE..])
}

#[cfg(test)]
mod tests { /* Step-1 tests */ }
```
Add `pub(crate) mod window_schema;` to `src/store/mod.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-store): WindowKeySchema + ValueAndTimestamp byte codecs`.

---

## Task 3: `WindowStore<K,V>` trait + `WindowBytesStore<K,V>` + registry/context access

**Files:** Create `src/store/window.rs`; modify `src/store/mod.rs`, `src/store/registry.rs`, `src/processor/api.rs`.

- [ ] **Step 1: failing test** — `src/store/window.rs` tests (mirror `kv.rs`/aggregate.rs harness):
```rust
#[tokio::test]
async fn put_fetch_single_and_range() {
    use crate::processor::serde::{I64Serde, StringSerde};
    let mut s = WindowBytesStore::<String, i64>::in_memory(
        "w".into(), Box::new(StringSerde), Box::new(I64Serde), "app-w-changelog".into(),
    );
    s.put("k".to_string(), 0, 1, 5).await;
    s.put("k".to_string(), 0, 2, 7).await;   // same window updates
    s.put("k".to_string(), 10, 9, 11).await;  // different window
    assert_eq!(s.fetch_single(&"k".to_string(), 0).await, Some((7, 2)));   // (storedTs, value)
    assert_eq!(s.fetch_single(&"k".to_string(), 10).await, Some((11, 9)));
    assert_eq!(s.fetch_single(&"k".to_string(), 99).await, None);
    let r = s.fetch(&"k".to_string(), 0, 10).await; // both windows, ordered
    assert_eq!(r, vec![(0, 2), (10, 9)]);
    // changelog buffered: 3 puts → composite keys
    assert_eq!(s.take_changelog().len(), 3);
}
```

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `src/store/window.rs`:
```rust
//! Window store over the byte backend: composite `WindowKeySchema` keys +
//! `ValueAndTimestamp` values. A second typed store beside `KeyValueBytesStore`.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::window_schema::{key_bytes_of, store_key, unwrap_value, window_start_of, wrap_value};

/// Typed windowed store: keyed by `(K, windowStart)`, holding `V` + a record
/// timestamp. `fetch_single` returns `(storedTs, V)` so the aggregator can
/// compute `newTs = max(recordTs, storedTs)`.
#[async_trait]
pub trait WindowStore<K: Send + Sync, V: Send>: StateStore {
    async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)>;
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)>;
    async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64);
}

pub struct WindowBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> WindowBytesStore<K, V> {
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
    #[must_use]
    pub fn in_memory(name: String, key_serde: Box<dyn Serde<K>>, value_serde: Box<dyn Serde<V>>, changelog_topic: String) -> Self {
        Self::new(name, Box::new(InMemoryBytes::default()), key_serde, value_serde, changelog_topic)
    }
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for WindowBytesStore<K, V> {
    fn name(&self) -> &str { &self.name }
    async fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn changelog_topic(&self) -> &str { &self.changelog_topic }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> { std::mem::take(&mut self.changelog) }
    async fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value {
            Some(v) => self.backend.put(key, v).await,
            None => { self.backend.delete(&key).await; }
        }
    }
    fn set_logging(&mut self, on: bool) { self.logging = on; }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> WindowStore<K, V> for WindowBytesStore<K, V> {
    async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)> {
        let kb = self.key_serde.serialize(key);
        let sk = store_key(&kb, window_start);
        let wrapped = self.backend.get(&sk).await?;
        let (ts, raw) = unwrap_value(&wrapped);
        Some((ts, self.value_serde.deserialize(raw).expect("window value deserialize")))
    }
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)> {
        let kb = self.key_serde.serialize(key);
        let lo = store_key(&kb, time_from);
        let hi = store_key(&kb, time_to.saturating_add(1)); // half-open → include time_to
        let mut out = Vec::new();
        for (k, wrapped) in self.backend.range(&lo, &hi).await {
            // Guard prefix collisions: keep only exact inner-key matches.
            if key_bytes_of(&k) != kb.as_ref() { continue; }
            let (_ts, raw) = unwrap_value(&wrapped);
            out.push((window_start_of(&k), self.value_serde.deserialize(raw).expect("window value deserialize")));
        }
        out
    }
    async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64) {
        let kb = self.key_serde.serialize(&key);
        let sk = store_key(&kb, window_start);
        let raw = self.value_serde.serialize(&value);
        let wrapped = wrap_value(record_ts, &raw);
        self.backend.put(sk.clone(), wrapped.clone()).await;
        if self.logging { self.changelog.push((sk, Some(wrapped))); }
    }
}

#[cfg(test)]
mod tests { /* Step-1 test */ }
```
Add `pub mod window;` to `src/store/mod.rs`. **Registry** (`src/store/registry.rs`): add
```rust
pub fn get_window<K: Send + Sync + 'static, V: Send + 'static>(&mut self, name: &str) -> Option<&mut dyn crate::store::window::WindowStore<K, V>> {
    let store = self.stores.get_mut(name)?;
    let concrete = store.as_any_mut().downcast_mut::<crate::store::window::WindowBytesStore<K, V>>()?;
    Some(concrete as &mut dyn crate::store::window::WindowStore<K, V>)
}
```
**ProcessorContext** (`src/processor/api.rs`): add
```rust
pub fn get_window_store<K2: Send + Sync + 'static, V2: Send + 'static>(&mut self, name: &str) -> Option<&mut dyn crate::store::window::WindowStore<K2, V2>> {
    self.dispatch.stores.get_window::<K2, V2>(name)
}
```

- [ ] **Step 4: run → PASS; full suite green; clippy; fmt; commit** `feat(streams-store): WindowStore + WindowBytesStore + get_window access`.

---

## Task 4: `add_window_store` builder + windowed changelog wire config

**Files:** `src/topology/node.rs` (`StoreEntry`), `src/topology/builder.rs`, `src/topology/grouping.rs`, `src/topology/wire.rs`.

- [ ] **Step 1: failing test** — `src/topology/wire.rs` tests (or a builder test): build a `Topology` with one window store via `add_window_store` and assert its changelog `TopicInfo.topic_configs` = `[("cleanup.policy","compact,delete"),("message.timestamp.type","CreateTime"),("retention.ms","86460000")]`. Sketch:
```rust
#[test]
fn windowed_store_changelog_config_is_compact_delete_with_retention() {
    // size 60_000, grace 0 → retention 60_000 + 0 + 86_400_000 = 86_460_000
    // build a minimal topology: source → processor(p) → window store(w) attached to p
    // (use Topology::new(); add_source; add_processor; add_window_store("w", StringSerde, I64Serde, 60_000, 0, ["p"]); build; to_wire)
    // then find the "w" changelog TopicInfo and assert its sorted topic_configs.
}
```
(Model it on the existing changelog-config test in wire.rs / grouping.rs; use the real builder API.)

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.**
  - `node.rs`: `StoreEntry` gains `pub windowed_retention_ms: Option<i64>` (None = KV changelog, Some = windowed). `add_store(name, procs, changelog_override)` keeps its signature but sets `windowed_retention_ms: None`; add `add_window_store(name, procs, changelog_override, retention_ms)` (or extend `add_store` with a param) setting `Some(retention_ms)`.
  - `builder.rs`: add
    ```rust
    pub fn add_window_store<K, V, KS, VS>(&mut self, name: impl Into<String>, key_serde: KS, value_serde: VS, size_ms: i64, grace_ms: i64, processors: impl IntoIterator<Item = impl Into<String>>) -> &mut Self
    where K: Send + 'static, V: Send + 'static, KS: Serde<K> + Clone, VS: Serde<V> + Clone {
        let name = name.into();
        let retention_ms = size_ms + grace_ms + 86_400_000; // windowstore.changelog.additional.retention.ms default = 1 day
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg.add_window_store(name.clone(), procs, None, retention_ms);
        self.store_factories.insert(name.clone(), (None, Box::new(move |store_name: &str, changelog: String, backend: Box<dyn ByteKeyValueStore>| {
            Box::new(crate::store::window::WindowBytesStore::<K, V>::new(store_name.to_string(), backend, Box::new(key_serde.clone()), Box::new(value_serde.clone()), changelog)) as Box<dyn StateStore>
        })));
        self
    }
    ```
    (mirror `add_state_store_inner`'s registration; the factory builds a `WindowBytesStore` instead of `KeyValueBytesStore`.)
  - `grouping.rs`: change `changelog_stores` to `Vec<(String, Option<String>, Option<i64>)>` and push `(store.name.clone(), store.changelog_override.clone(), store.windowed_retention_ms)`.
  - `wire.rs`: add `fn windowed_changelog_topic_configs(retention_ms: i64) -> Vec<KeyValue> { topic_configs([("cleanup.policy","compact,delete"),("message.timestamp.type","CreateTime"),("retention.ms", &retention_ms.to_string())]) }` — **NB sorted by key**: `cleanup.policy` < `message.timestamp.type` < `retention.ms` (alphabetical ✓). In `state_changelog_topics`, choose per store: `let topic_configs = match windowed_retention_ms { Some(r) => windowed_changelog_topic_configs(*r), None => changelog_topic_configs() };`. (The `topic_configs` helper takes `[(&str,&str);N]`; for the dynamic retention string, build the `Vec<KeyValue>` directly or pass an owned string — adjust the helper to accept `&str` from a local `let ret = retention_ms.to_string();`.)

- [ ] **Step 4: run → PASS; 8 goldens still byte-identical (KV stores unaffected); clippy; fmt; commit** `feat(streams): add_window_store + windowed changelog wire config (compact,delete + retention.ms)`.

---

## Task 5: `KStreamWindowAggregateProcessor`

**Files:** Create `src/dsl/processors/window_aggregate.rs`; modify `src/dsl/processors/mod.rs`.

- [ ] **Step 1: failing test** — `window_aggregate.rs` tests (mirror `aggregate.rs`'s harness, but a `WindowBytesStore` + `Processor<K, V, Windowed<K>, Change<VA>>`): seed a `StoreRegistry` with a `WindowBytesStore::<String,i64>::in_memory("w", …)`; build a count processor (`init=||0`, `agg=|_,_,a| a+1`, `TimeWindows::of_size(10)`); process `(k="a", v="x", ts=3)` then `(k="a", v="x", ts=7)`; assert two forwarded `Record<Windowed<String>, Change<i64>>` with key window `[0,10)` and `new` = 1 then 2; process `(ts=12)` → key window `[10,20)`, `new` = 1.

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement** `src/dsl/processors/window_aggregate.rs`:
```rust
//! Windowed aggregation processor: emit-on-every-update (no window closing).
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::windows::{TimeWindows, Window, Windowed};
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

type Marker<T> = PhantomData<fn() -> T>;

#[allow(dead_code)]
pub(crate) struct KStreamWindowAggregateProcessor<K, V, VA, I, A> {
    pub store_name: String,
    pub windows: TimeWindows,
    pub init: I,
    pub agg: A,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A> Processor<K, V, Windowed<K>, Change<VA>>
    for KStreamWindowAggregateProcessor<K, V, VA, I, A>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>, r: Record<K, V>) {
        let key = r.key.expect("windowed aggregate requires a non-null key");
        let size = self.windows.size_ms;
        for ws in self.windows.windows_for(r.timestamp) {
            let (old, new, new_ts) = {
                let store = ctx
                    .get_window_store::<K, VA>(&self.store_name)
                    .expect("window store not found");
                let prior = store.fetch_single(&key, ws).await; // Option<(storedTs, VA)>
                let old = prior.as_ref().map(|(_ts, v)| v.clone());
                let seed = old.clone().unwrap_or_else(|| (self.init)());
                let new = (self.agg)(&key, &r.value, seed);
                let new_ts = std::cmp::max(r.timestamp, prior.map_or(i64::MIN, |(ts, _)| ts));
                store.put(key.clone(), ws, new.clone(), new_ts).await;
                (old, new, new_ts)
            };
            ctx.forward(Record::new(
                Some(Windowed { key: key.clone(), window: Window { start: ws, end: ws + size } }),
                Change::update(old, new),
                new_ts,
            ));
        }
    }
}

#[cfg(test)]
mod tests { /* Step-1 test */ }
```
Add `pub(crate) mod window_aggregate;` to `src/dsl/processors/mod.rs`.

- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): windowed aggregate processor (emit-on-update)`.

---

## Task 6: `windowed_by` DSL + `TimeWindowedKGroupedStream` + lowering

**Files:** Create `src/dsl/windowed_kgrouped.rs`; modify `src/dsl/kgrouped.rs`, `src/dsl/mod.rs`; add execution tests in `tests/dsl_execution.rs`.

- [ ] **Step 1: failing execution test** — `tests/dsl_execution.rs`:
```rust
#[test]
fn dsl_windowed_count_tumbling_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, Grouped, I64Serde, Materialized, Produced, StringSerde, TimeWindowedSerde, TimeWindows};
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by(TimeWindows::of_size(10))
        .count(Materialized::with(StringSerde, I64Serde).as_store("w"))
        .to_stream()
        .to("out", Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde));
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "x".to_string(), 3);
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "x".to_string(), 7);
    d.pipe_input("in", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "x".to_string(), 12);
    let s = TimeWindowedSerde::new(StringSerde, 10);
    // window [0,10): count 1 then 2; window [10,20): count 1
    assert_eq!(d.read_output("out", Produced::with(s, I64Serde)), Some((Some(Windowed{key:"k".into(),window:Window{start:0,end:10}}), 1)));
    assert_eq!(d.read_output("out", Produced::with(s, I64Serde)), Some((Some(Windowed{key:"k".into(),window:Window{start:0,end:10}}), 2)));
    assert_eq!(d.read_output("out", Produced::with(s, I64Serde)), Some((Some(Windowed{key:"k".into(),window:Window{start:10,end:20}}), 1)));
}
```
Plus `dsl_windowed_count_hopping_executes` (`TimeWindows::of_size(10).advance_by(5)`, a record at ts=12 emits to windows `[5,15)` and `[10,20)`), a `reduce` and an `aggregate` variant. ADJUST to the real `group_by_key`/`to_stream`/`read_output` shapes (the `Windowed`/`Window` types are re-exported from `crate`).

- [ ] **Step 2: run → FAIL.**

- [ ] **Step 3: implement.**
  - `dsl/kgrouped.rs`: add `pub fn windowed_by(self, windows: crate::dsl::windows::TimeWindows) -> crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream<K, V>` — moves `self`'s fields (builder, parent, key_changing_upstream, grouped_name, repartition_lower) into the new handle plus `windows`.
  - `dsl/windowed_kgrouped.rs` (NEW): `TimeWindowedKGroupedStream<K,V>` holding the same fields + `windows: TimeWindows`. Methods `count(Materialized)->KTable<Windowed<K>,i64>`, `reduce(reducer,M)->KTable<Windowed<K>,V>`, `aggregate(init,agg,M)->KTable<Windowed<K>,VA>`. Lowering mirrors `kgrouped.rs::lower_aggregate` with these differences:
    - **store name + count burn:** `let store_name = mint_store_name(&builder, &materialized, names::AGGREGATE_STORE);` and, on the **count** path only, when `materialized.store_name.is_none()`, call `g.new_processor_name(names::AGGREGATE_STORE)` once more to burn the index (JVM `count` compat). (Confirm exact placement against the Task-7 fixture.)
    - **processor:** `add_processor::<K, V, Windowed<K>, Change<VA>, _, _, _>(agg_name, move || KStreamWindowAggregateProcessor { store_name, windows, init, agg, _pd }, [parent])` — note `KOut = Windowed<K>`.
    - **store registration:** `state.topology.add_window_store::<K, VA, KS, VS>(store_name, key_serde, value_serde, windows.size_ms, windows.grace_ms, [h.name()])` (always logging in this slice — `Materialized::with_logging(false)` for windowed is out of scope; if a no-logging path is desired, mirror `add_state_store_no_changelog`, but default to `add_window_store`).
    - **repartition:** reuse `KGroupedStream::record_repartition` logic (key-changing → repartition before the windowed aggregate). Since `record_repartition` is a private assoc fn on `KGroupedStream`, either make it `pub(crate)` and call it, or duplicate the small body. Prefer making it `pub(crate)`.
    - returns `KTable::new(builder, agg_id, Some(store_name), None)` typed `KTable<Windowed<K>, VA>` (the `KTable` parent forwards `Change<VA>` keyed by `Windowed<K>`; downstream `to_stream()` reconstructs `NodeHandle<Windowed<K>, Change<VA>>`).
  - `dsl/mod.rs`: `pub mod windowed_kgrouped;`.
  - **Note (erasure):** the aggregate node's `KOut = Windowed<K>`, so `to_stream()` on the result must reconstruct the parent as `NodeHandle::<Windowed<K>, Change<VA>>` — `KTable<Windowed<K>,V>::to_stream` already uses `NodeHandle::<K, Change<V>>` generically (K is `Windowed<K>` here), so it works without change. A type mismatch is a RUNTIME downcast error → the execution test is the gate.

- [ ] **Step 4: run → execution PASS (tumbling/hopping count + reduce + aggregate); the 8 goldens still byte-identical; clippy; fmt; commit** `feat(streams-dsl): windowedBy(TimeWindows) count/reduce/aggregate`.

---

## Task 7: capture `windowed_count` golden (#9)

**Files:** `tests/jvm-capture/.../Capture.java`; `tests/testdata/golden/dsl/windowed_count.topology.json` (NEW); `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: capture FIRST.** Add `windowedCount()` to `Capture.java`:
  ```java
  static Topology windowedCount() {
      StreamsBuilder b = new StreamsBuilder();
      b.<String, String>stream("in")
          .groupByKey()
          .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(java.time.Duration.ofSeconds(60)))
          .count()
          .toStream()
          .to("out");
      return b.build(optimizedProps());
  }
  ```
  Register it in `main()` (`write(outDir, "windowed_count", windowedCount());`) + bump the count message. Run `cd crates/client-streams/tests/jvm-capture && ./run.sh --gradle`. Commit `testdata/golden/dsl/windowed_count.topology.json`. NOTE the captured: store name (with the **count burn** — likely `KSTREAM-AGGREGATE-STATE-STORE-0000000003`), the single changelog `TopicInfo` with `cleanup.policy=compact,delete` + `retention.ms=86460000` (60s + 0 + 1 day) + `message.timestamp.type=CreateTime`, one subtopology `source_topics:["in"]`. If Docker capture fails, report BLOCKED with the exact error — do NOT fabricate.

- [ ] **Step 2: failing golden test** — `tests/dsl_golden_frame.rs`:
  ```rust
  #[test]
  fn windowed_count_matches_jvm() {
      use crabka_client_streams::{Grouped, I64Serde, Materialized, StringSerde, TimeWindows};
      let b = StreamsBuilder::new();
      b.stream(["in"], Consumed::with(StringSerde, StringSerde))
          .group_by_key(Grouped::with(StringSerde, StringSerde))
          .windowed_by(TimeWindows::of_size(60_000))
          .count(Materialized::with(StringSerde, I64Serde)) // UNNAMED → store auto-named (with burn)
          .to_stream()
          .to("out", Produced::with(crabka_client_streams::TimeWindowedSerde::new(StringSerde, 60_000), I64Serde));
      let wire = b.build_optimized("app").unwrap().to_wire(); // build vs build_optimized — match the fixture
      assert_matches_fixture(&wire, "windowed_count");
  }
  ```

- [ ] **Step 3: iterate** the store-name index (the burn) + the changelog config until `windowed_count_matches_jvm` byte-matches. If the store index is off by one, the burn placement is wrong (it fires on the `count`-no-name path); if the changelog config differs, fix `windowed_changelog_topic_configs`/the retention arithmetic. Use `build` vs `build_optimized` to match the fixture (the count fixture uses `build_optimized`).

- [ ] **Step 4: run → golden PASS (+ 8 prior byte-identical); clippy; fmt; commit** `feat(streams-dsl): windowed_count golden frame`.

---

## Task 8: docs + final verification

**Files:** `src/lib.rs`.

- [ ] **Step 1:** add a short `Windowed aggregations` note to `lib.rs` docs: `KGroupedStream::windowed_by(TimeWindows::of_size(..))` then `count`/`reduce`/`aggregate` → `KTable<Windowed<K>, V>`; tumbling vs hopping (`advance_by`); the result keys carry the window; read with `TimeWindowedSerde`; emit-on-update (window closing/grace deferred). No new doctest (or a `compile`-only snippet).

- [ ] **Step 2: full verification.**
  - `cargo test -p crabka-client-streams` (windows + window_schema + window store units + windowed aggregate unit + windowed execution + `windowed_count` golden + the **8 prior goldens byte-identical** + all prior tests + doctests).
  - `cargo clippy --workspace --all-targets -- -D warnings`.
  - `cargo fmt -p crabka-client-streams -- --check`.
  - `cargo build --workspace`.

- [ ] **Step 3: commit** `docs(streams-dsl): windowed-aggregation note + #4d-ii verification`.

---

## Self-review

**Spec coverage:** §3 storage → Tasks 2,3. §4 types + serde → Task 1. §5 DSL + processor + lowering → Tasks 5,6. §6 windowed changelog wire config → Task 4. §7 capture + golden → Task 7. §8 testing → Tasks 1-7. §9 success → Task 8. ✓

**Empirical-fixture note (not a placeholder):** Task 7's store-name index (the count burn) + the exact `retention.ms` are validated against the **captured** JVM fixture (Step 1 captures first). The byte-exact bits are pinned by the fixture; `KSTREAM-AGGREGATE-STATE-STORE-0000000003` / `86460000` are the expected values, confirmed on capture.

**Type consistency:** `TimeWindows`/`Window`/`Windowed<K>`/`TimeWindowedSerde` (T1) → `store_key`/`wrap_value`/`window_start_of`/`unwrap_value` (T2) → `WindowStore<K,V>`/`WindowBytesStore<K,V>`/`get_window`/`get_window_store` (T3) → `add_window_store`/`windowed_changelog_topic_configs`/`StoreEntry.windowed_retention_ms`/`changelog_stores: Vec<(String,Option<String>,Option<i64>)>` (T4) → `KStreamWindowAggregateProcessor<K,V,VA,I,A>: Processor<K,V,Windowed<K>,Change<VA>>` (T5) → `windowed_by`/`TimeWindowedKGroupedStream::{count,reduce,aggregate}` lowering `add_processor::<K,V,Windowed<K>,Change<VA>>` + `add_window_store::<K,VA,KS,VS>` + count burn (T6) → golden (T7). Consistent.

**Known risks (for the implementer):** (1) the **count burn** — exact index pinned by the fixture; only fires on `count` with an unnamed store. (2) `changelog_stores` tuple widening to 3-arity ripples grouping.rs's test (`vec![("store",None)]` → `vec![("store",None,None)]`). (3) `windowed_changelog_topic_configs` builds a `Vec<KeyValue>` with a dynamic `retention.ms` string — the `topic_configs([(&str,&str);N])` helper needs an owned-string variant or build the Vec inline; keep keys sorted (`cleanup.policy` < `message.timestamp.type` < `retention.ms`). (4) `record_repartition` must become `pub(crate)` to be reused from `windowed_kgrouped.rs` (or duplicate). (5) `Windowed<K>` flows erased as `KOut`; `to_stream`/sink reconstruct `NodeHandle<Windowed<K>, Change<VA>>` — a mismatch is a runtime downcast, so the execution + golden tests are the gate. (6) the `build` vs `build_optimized` choice in the golden test is the fixture's call.
