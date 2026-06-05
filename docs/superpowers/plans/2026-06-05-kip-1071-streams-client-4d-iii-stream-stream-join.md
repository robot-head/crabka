# KIP-1071 Streams Client #4d-iii — windowed KStream-KStream join — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `KStream::join`/`left_join`/`outer_join(other, joiner, JoinWindows)` — the windowed stream-stream join — producing a `KStream<K,VO>`, byte-exact vs JVM 4.1.

**Architecture:** A `JoinWindowBytesStore` (retainDuplicates window store — raw values, per-store incrementing seqnum, fetch-all) over the 4d-i byte backend. Two such stores (one per side) + dual `KStreamKStreamJoinProcessor`s (put-own + swapped fetch-other + per-match emit) + a merge, mirroring 4c-iii's KTable-KTable dual+merge. left/outer add a shared outer-join KV store + an `Arc<Mutex>` stream-time tracker + window-close-driven null-result emission (KIP-633), stream-time-only.

**Tech Stack:** Rust 2024; async-trait; extends 4d-ii (window store + `WindowKeySchema`) + 4c-ii/iii (copartition, `connect_processor_store`, dual+merge). JVM capture via Docker Kafka-Streams 4.1.

**Spec:** `docs/superpowers/specs/2026-06-05-kip-1071-streams-client-4d-iii-stream-stream-join-design.md`.
**Branch:** `streams-4d-iii-stream-join` (stacked on `streams-4d-ii-windowstore` / PR #396; rebase onto `main` once #396 merges). Worktree `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`.

---

## Current signatures (verbatim — what we extend)

- `store/window_schema.rs`: `pub(crate) fn store_key(key_bytes: &[u8], window_start: i64) -> Bytes` does `b.extend(key_bytes); b.put_i64(window_start); b.put_u32(0)` (seqnum hardcoded 0). `window_start_of`/`key_bytes_of`/`wrap_value`/`unwrap_value`, `SUFFIX_SIZE=12`.
- `store/window.rs`: `WindowStore<K,V>` trait (`fetch_single`/`fetch`/`put`); `WindowBytesStore<K,V> { name, changelog_topic, backend: Box<dyn ByteKeyValueStore>, key_serde, value_serde, changelog: Vec<(Bytes,Option<Bytes>)>, logging }`; `pub(crate) fn new(...)` + `pub fn in_memory(...)`. `put` calls `store_key(&kb, ws)` (line 139), `fetch` calls it at 118-119, `fetch_single` at 105 — these become `store_key(&kb, ws, 0)`.
- `store/registry.rs`: `get_kv::<K,V>` / `get_window::<K,V>(name)` downcast `as_any_mut().downcast_mut::<KeyValueBytesStore<K,V>>()` / `WindowBytesStore<K,V>`. `insert(Box<dyn StateStore>)`, `get_mut(name)`.
- `store/byte.rs`: `#[async_trait] pub(crate) trait ByteKeyValueStore: Send+Sync { get/put/delete/range }`. `InMemoryBytes`.
- `processor/api.rs`: `ProcessorContext::{get_state_store, get_window_store}::<K2:Send+Sync,V2:Send>(name)`. `#[async_trait] Processor<KIn:Send,VIn:Send,KOut:Send,VOut:Send>`; `forward(Record)` sync.
- `topology/node.rs`: `StoreEntry { pub name, pub processors: Vec<String>, pub changelog_override: Option<String>, pub windowed_retention_ms: Option<i64> }` (line 39-45); `add_store(name, procs, changelog_override)` sets `windowed_retention_ms: None`; `add_window_store(name, procs, changelog_override, retention_ms)` sets `Some(retention_ms)`.
- `topology/grouping.rs`: `GroupTopics.changelog_stores: Vec<(String, Option<String>, Option<i64>)>` (line 26), pushed `(store.name, store.changelog_override, store.windowed_retention_ms)` (line 131).
- `topology/wire.rs`: `changelog_topic_configs() -> [("cleanup.policy","compact"),("message.timestamp.type","CreateTime")]`; `windowed_changelog_topic_configs(retention_ms) -> [("cleanup.policy","compact,delete"),("message.timestamp.type","CreateTime"),("retention.ms",retention)]`; `state_changelog_topics` maps each `(store, override, windowed_retention_ms)` → `TopicInfo { topic_configs: match windowed_retention_ms { Some(r) => windowed_changelog_topic_configs(*r), None => changelog_topic_configs() }, … }`.
- `topology/builder.rs`: `add_window_store::<K,V,KS,VS>(name, ks, vs, size_ms, grace_ms, procs)` (retention = size+grace+86_400_000); `add_state_store`; `connect_processor_store(processor, store)`; `add_copartition_group(topics)`; `instantiate(&backend, app_id)`.
- `dsl/kstream.rs`: `KStream<K,V> { builder, node, key_changing, source_topic, _pd }`; `join<VT,VO,F>(&self, table: &KTable<K,VT>, joiner) -> KStream<K,VO>` / `left_join` / `join_impl` (lines 467-560) — mints `names::JOIN`, records a node, thunk: `add_processor::<K,V,K,VO>(name, ||KStreamKTableJoinProcessor{table_store,joiner,emit_on_miss,_pd}, [parent])` + `connect_processor_store(h.name(), &store)` + `add_copartition_group([stream_member, table_src])`.
- `dsl/ktable.rs` `join_impl` (4c-iii): the **dual-processor + merge** lowering template — mints `KTABLE_JOIN_THIS`/`KTABLE_JOIN_OTHER`/`KTABLE_MERGE`; records 3 nodes (`this_id` pred=self.node, `other_id` pred=other.node, `merge_id` preds=[this,other]); each join thunk `add_processor` + `connect_processor_store`; merge thunk reuses `stateless::MergeProcessor` + `add_copartition_group`. **Read this as the structural template.**
- `dsl/processors/ktable_join.rs`: `JoinKind { a_required, b_required }` + `inner()`/`left()`/`outer()` + `result(kind, joiner, a, b) -> Option<VR>`. **Reuse `JoinKind` + the result-rule idea.**
- `dsl/names.rs`: `MERGE="KSTREAM-MERGE-"`, `JOIN="KSTREAM-JOIN-"`, `AGGREGATE_STORE="KSTREAM-AGGREGATE-STATE-STORE-"`.

## File structure

```
store/window_schema.rs       store_key gains seqnum param (agg callers pass 0)
store/join_window.rs   NEW   JoinWindowStore<K,V> + JoinWindowBytesStore<K,V> (raw, incrementing seqnum, fetch-all)
store/registry.rs            + get_join_window::<K,V>
store/mod.rs                 + pub mod join_window;
processor/api.rs             + get_join_window_store::<K,V>
topology/node.rs             StoreEntry.windowed_retention_ms → ChangelogKind enum {Kv|AggWindow|JoinWindow}
topology/grouping.rs         changelog_stores carries ChangelogKind
topology/wire.rs             + join_window_changelog_topic_configs (delete + retention); per-kind dispatch
topology/builder.rs          + add_join_window_store; add_window_store/add_state_store set their ChangelogKind
dsl/windows.rs               + JoinWindows{before_ms,after_ms,grace_ms}
dsl/processors/stream_join.rs NEW  KStreamKStreamJoinProcessor + (Phase C) outer-store buffering + emit-on-close
dsl/processors/outer_join_store.rs NEW (Phase C) shared-outer-store key/value codecs + TimeTracker
dsl/kstream.rs               + join/left_join/outer_join (windowed, JoinWindows) + join lowering
dsl/names.rs                 + KSTREAM-JOINTHIS-/JOINOTHER-/WINDOWED-/OUTERSHARED- (exact set pinned by capture)
tests/jvm-capture/.../Capture.java  + streamStreamJoin (B) + streamStreamOuterJoin (C)
tests/testdata/golden/dsl/{stream_stream_join,stream_stream_outer_join}.topology.json  NEW
tests/dsl_golden_frame.rs    + both goldens
tests/dsl_execution.rs       + inner/dup/swap (B) + left/outer (C)
lib.rs                       stream-stream-join note (C)
```

**Phasing:** A (store) → B (inner) → C (left/outer). Each phase ends green. A1+A2 are disjoint-ish but sequential (A2 uses A1's codec). Capture tasks (B4, C4) are owned by the controller (Docker).

---

# PHASE A — the retainDuplicates join window store

## Task A1: seqnum param on the window-key codec

**Files:** `src/store/window_schema.rs`; `src/store/window.rs` (3 call sites).

- [ ] **Step 1: failing test** — append to `window_schema.rs` tests:
```rust
#[test]
fn store_key_encodes_seqnum() {
    let k0 = store_key(b"k", 5, 0);
    let k1 = store_key(b"k", 5, 1);
    assert_eq!(&k0[k0.len() - 4..], &[0, 0, 0, 0]);
    assert_eq!(&k1[k1.len() - 4..], &0u32.wrapping_add(1).to_be_bytes());
    assert!(k1 > k0); // seqnum sorts after, same (key,ts)
    assert_eq!(window_start_of(&k1), 5);
    assert_eq!(key_bytes_of(&k1), b"k");
}
```
- [ ] **Step 2: run → FAIL** (arity mismatch). `cargo test -p crabka-client-streams --lib store::window_schema`
- [ ] **Step 3: implement** — change `store_key` signature:
```rust
pub(crate) fn store_key(key_bytes: &[u8], window_start: i64, seqnum: u32) -> Bytes {
    let mut b = BytesMut::with_capacity(key_bytes.len() + SUFFIX_SIZE);
    b.extend_from_slice(key_bytes);
    b.put_i64(window_start);
    b.put_u32(seqnum);
    b.freeze()
}
```
Update the doc comment (seqnum is the per-record value for retainDuplicates, 0 for aggregations). In `src/store/window.rs`, change the 3 `store_key(&kb, ws)` call sites to `store_key(&kb, ws, 0)` (`fetch_single` line ~105, `fetch` lines ~118-119, `put` line ~139). (`hi = store_key(&kb, time_to.saturating_add(1), 0)`.)
- [ ] **Step 4: run → PASS** (`store::window_schema` + `store::window` both green); clippy; fmt; **commit** `feat(streams-store): seqnum param on WindowKeySchema store_key`.

## Task A2: `JoinWindowStore` + `JoinWindowBytesStore`

**Files:** Create `src/store/join_window.rs`; modify `src/store/mod.rs`, `src/store/registry.rs`, `src/processor/api.rs`.

- [ ] **Step 1: failing test** — `src/store/join_window.rs` tests:
```rust
#[tokio::test]
async fn put_keeps_duplicates_and_fetch_returns_all() {
    use crate::processor::serde::StringSerde;
    let mut s = JoinWindowBytesStore::<String, String>::in_memory(
        "j".into(), Box::new(StringSerde), Box::new(StringSerde), "app-j-changelog".into());
    s.put("k".into(), 5, "a".into()).await;
    s.put("k".into(), 5, "b".into()).await;  // SAME (key, ts) → kept as a duplicate (seqnum increments)
    s.put("k".into(), 7, "c".into()).await;
    // fetch [5,7] returns all three, in (ts, seqnum) order
    assert_eq!(s.fetch(&"k".to_string(), 5, 7).await, vec![(5, "a".to_string()), (5, "b".to_string()), (7, "c".to_string())]);
    assert_eq!(s.fetch(&"k".to_string(), 5, 5).await, vec![(5, "a".to_string()), (5, "b".to_string())]);
    assert_eq!(s.take_changelog().len(), 3); // raw value, composite key w/ distinct seqnums
}
```
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** `src/store/join_window.rs` (mirror `window.rs` but raw value + seqnum + fetch-all):
```rust
//! retainDuplicates window store for stream-stream joins: composite
//! `WindowKeySchema` keys with a per-store incrementing seqnum + RAW values
//! (no ValueAndTimestamp wrap). `fetch` returns every duplicate in a range.
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::window_schema::{key_bytes_of, store_key, window_start_of};

#[async_trait]
pub trait JoinWindowStore<K: Send + Sync, V: Send>: StateStore {
    /// Insert a record at `timestamp` (a fresh seqnum keeps duplicates distinct).
    async fn put(&mut self, key: K, timestamp: i64, value: V);
    /// Every record with windowStart in `[time_from, time_to]`, in (ts, seqnum) order.
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)>;
}

pub struct JoinWindowBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
    seqnum: u32, // per-store monotonic, mask 0x7FFF_FFFF (RocksDBWindowStore parity)
}

impl<K: 'static, V: 'static> JoinWindowBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(name: String, backend: Box<dyn ByteKeyValueStore>, key_serde: Box<dyn Serde<K>>, value_serde: Box<dyn Serde<V>>, changelog_topic: String) -> Self {
        Self { name, changelog_topic, backend, key_serde, value_serde, changelog: Vec::new(), logging: true, seqnum: 0 }
    }
    #[must_use]
    pub fn in_memory(name: String, key_serde: Box<dyn Serde<K>>, value_serde: Box<dyn Serde<V>>, changelog_topic: String) -> Self {
        Self::new(name, Box::new(InMemoryBytes::default()), key_serde, value_serde, changelog_topic)
    }
    fn next_seqnum(&mut self) -> u32 {
        let s = self.seqnum;
        self.seqnum = (self.seqnum + 1) & 0x7FFF_FFFF;
        s
    }
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for JoinWindowBytesStore<K, V> {
    fn name(&self) -> &str { &self.name }
    async fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any { self }
    fn changelog_topic(&self) -> &str { &self.changelog_topic }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> { std::mem::take(&mut self.changelog) }
    async fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value { Some(v) => self.backend.put(key, v).await, None => { self.backend.delete(&key).await; } }
    }
    fn set_logging(&mut self, on: bool) { self.logging = on; }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> JoinWindowStore<K, V> for JoinWindowBytesStore<K, V> {
    async fn put(&mut self, key: K, timestamp: i64, value: V) {
        let kb = self.key_serde.serialize(&key);
        let seq = self.next_seqnum();
        let sk = store_key(&kb, timestamp, seq);
        let raw = self.value_serde.serialize(&value); // RAW — no ValueAndTimestamp wrap
        self.backend.put(sk.clone(), raw.clone()).await;
        if self.logging { self.changelog.push((sk, Some(raw))); }
    }
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)> {
        let kb = self.key_serde.serialize(key);
        let lo = store_key(&kb, time_from, 0);
        let hi = store_key(&kb, time_to.saturating_add(1), 0);
        let mut out = Vec::new();
        for (k, raw) in self.backend.range(&lo, &hi).await {
            if key_bytes_of(&k) != kb.as_ref() { continue; } // prefix-collision guard
            out.push((window_start_of(&k), self.value_serde.deserialize(&raw).expect("join window value deserialize")));
        }
        out
    }
}

#[cfg(test)]
mod tests { /* Step-1 test */ }
```
`src/store/mod.rs`: `pub mod join_window;`. `src/store/registry.rs`: add `get_join_window` (mirror `get_window`):
```rust
pub fn get_join_window<K: Send + Sync + 'static, V: Send + 'static>(&mut self, name: &str) -> Option<&mut dyn crate::store::join_window::JoinWindowStore<K, V>> {
    let store = self.stores.get_mut(name)?;
    let concrete = store.as_any_mut().downcast_mut::<crate::store::join_window::JoinWindowBytesStore<K, V>>()?;
    Some(concrete as &mut dyn crate::store::join_window::JoinWindowStore<K, V>)
}
```
`src/processor/api.rs`: add `get_join_window_store::<K2,V2>(name)` mirroring `get_window_store`, delegating to `self.dispatch.stores.get_join_window::<K2,V2>(name)`.
- [ ] **Step 4: run → PASS; full suite green; clippy; fmt; commit** `feat(streams-store): JoinWindowBytesStore (retainDuplicates window store)`.

## Task A3: `ChangelogKind` enum + `add_join_window_store` + delete-only changelog config

**Files:** `src/topology/node.rs`, `src/topology/grouping.rs`, `src/topology/wire.rs`, `src/topology/builder.rs`.

- [ ] **Step 1: failing test** — `wire.rs` tests (mirror the existing windowed-changelog test at ~line 526):
```rust
#[test]
fn join_window_changelog_is_delete_only_with_retention() {
    use crate::topology::node::ChangelogKind;
    let groups = vec![GroupTopics {
        // ... mirror the existing test's GroupTopics literal ...
        changelog_stores: vec![("j".into(), None, ChangelogKind::JoinWindow { retention_ms: 86_520_000 })],
        ..Default::default() // or the explicit fields the struct needs
    }];
    let topo = to_wire(&groups, "app");
    let cl = &topo.subtopologies[0].state_changelog_topics[0];
    // sorted by key: cleanup.policy < message.timestamp.type < retention.ms
    assert_eq!(cl.topic_configs[0].key, "cleanup.policy");
    assert_eq!(cl.topic_configs[0].value, "delete"); // NOT compact,delete
    assert_eq!(cl.topic_configs[2].key, "retention.ms");
    assert_eq!(cl.topic_configs[2].value, "86520000");
}
```
(86_520_000 = before 60s + after 60s + grace 0 + 1 day = 60000+60000+0+86400000.)
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement.**
  - `node.rs`: replace `pub windowed_retention_ms: Option<i64>` on `StoreEntry` with `pub changelog_kind: ChangelogKind`, and add:
    ```rust
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum ChangelogKind {
        Kv,                               // cleanup.policy=compact
        AggWindow { retention_ms: i64 },  // compact,delete + retention.ms
        JoinWindow { retention_ms: i64 }, // delete + retention.ms
    }
    ```
    `add_store(...)` sets `changelog_kind: ChangelogKind::Kv`; `add_window_store(..., retention_ms)` sets `AggWindow { retention_ms }`; add `add_join_window_store(name, procs, changelog_override, retention_ms)` setting `JoinWindow { retention_ms }`.
  - `grouping.rs`: `changelog_stores: Vec<(String, Option<String>, ChangelogKind)>`; push `(store.name, store.changelog_override, store.changelog_kind)`. Update the grouping test literal `(.., None)`→`(.., ChangelogKind::Kv)`.
  - `wire.rs`: add `fn join_window_changelog_topic_configs(retention_ms: i64) -> Vec<KeyValue> { vec![ KeyValue{key:"cleanup.policy".into(), value:"delete".into(), ..d}, KeyValue{key:"message.timestamp.type".into(), value:"CreateTime".into(), ..d}, KeyValue{key:"retention.ms".into(), value:retention_ms.to_string(), ..d} ] }` (keys already sorted). In `state_changelog_topics`, replace the `match windowed_retention_ms` with `match changelog_kind { ChangelogKind::Kv => changelog_topic_configs(), ChangelogKind::AggWindow{retention_ms} => windowed_changelog_topic_configs(*retention_ms), ChangelogKind::JoinWindow{retention_ms} => join_window_changelog_topic_configs(*retention_ms) }`. Update the existing wire tests' `changelog_stores` literals to the enum (`None`→`Kv`, `Some(86_460_000)`→`AggWindow{retention_ms:86_460_000}`).
  - `builder.rs`: add `pub fn add_join_window_store<K,V,KS,VS>(&mut self, name, key_serde, value_serde, before_ms, after_ms, grace_ms, processors) -> &mut Self` (mirror `add_window_store`): `retention_ms = before_ms + after_ms + grace_ms + 86_400_000`; `self.reg.add_join_window_store(name, procs, None, retention_ms)`; factory builds `JoinWindowBytesStore::<K,V>::new(...)`.
- [ ] **Step 4: run → the new test PASSES + the 9 goldens stay byte-identical (KV + agg-window stores unaffected — only the enum spelling changed, not the bytes) + all prior tests green; clippy; fmt; commit** `feat(streams): ChangelogKind enum + add_join_window_store (delete-only changelog)`.

---

# PHASE B — inner windowed stream-stream join

## Task B1: `JoinWindows`

**Files:** `src/dsl/windows.rs`; `src/lib.rs` (re-export).

- [ ] **Step 1: failing test** — `windows.rs` tests:
```rust
#[test]
fn join_windows_before_after_size() {
    let w = JoinWindows::of(10);
    assert_eq!((w.before_ms, w.after_ms, w.grace_ms), (10, 10, 0));
    assert_eq!(w.size(), 20);
    let a = JoinWindows::of(10).before(3).after(7).grace(5);
    assert_eq!((a.before_ms, a.after_ms, a.grace_ms), (3, 7, 5));
    assert_eq!(a.size(), 10);
}
```
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** in `src/dsl/windows.rs`:
```rust
/// Symmetric-or-asymmetric join window: a record at `t` matches the other side's
/// records with timestamp in `[t - before_ms, t + after_ms]`.
#[derive(Debug, Clone, Copy)]
pub struct JoinWindows {
    pub before_ms: i64,
    pub after_ms: i64,
    pub grace_ms: i64,
}
impl JoinWindows {
    #[must_use] pub fn of(time_difference_ms: i64) -> Self {
        assert!(time_difference_ms >= 0, "time difference must be >= 0");
        Self { before_ms: time_difference_ms, after_ms: time_difference_ms, grace_ms: 0 }
    }
    #[must_use] pub fn before(mut self, before_ms: i64) -> Self { self.before_ms = before_ms; self }
    #[must_use] pub fn after(mut self, after_ms: i64) -> Self { self.after_ms = after_ms; self }
    #[must_use] pub fn grace(mut self, grace_ms: i64) -> Self { assert!(grace_ms >= 0); self.grace_ms = grace_ms; self }
    #[must_use] pub fn size(&self) -> i64 { self.before_ms + self.after_ms }
}
```
Re-export `JoinWindows` from `lib.rs` (next to `TimeWindows`).
- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): JoinWindows`.

## Task B2: `KStreamKStreamJoinProcessor` (inner) + `JoinKind`

**Files:** Create `src/dsl/processors/stream_join.rs`; modify `src/dsl/processors/mod.rs`.

- [ ] **Step 1: failing test** — `stream_join.rs` tests (seed TWO `JoinWindowBytesStore`s in the registry; build a THIS processor reading the other store; assert per-match emit + the swap). Mirror `window_aggregate.rs`'s harness. Concretely: store "this"/"other"; THIS processor puts into "this", fetches "other" `[t-before, t+after]`; seed "other" with B-records; process an A-record → assert one forward per match at `max(ts)`. Include a duplicate case (two matches → two forwards).
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** `src/dsl/processors/stream_join.rs`:
```rust
//! Windowed stream-stream join processors (one per side) — inner path.
//! Each: put own record into its own JoinWindowStore, fetch the OTHER store over
//! the (side-swapped) window, emit `joiner(a,b)` per match at max(tThis, tOther).
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::ktable_join::JoinKind; // reuse {a_required,b_required}
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

type Marker<T> = PhantomData<fn() -> T>;

/// `side_left = true` → the THIS processor (drains stream A): own=A store, other=B store,
/// fetch `[t-before, t+after]`, joiner(Some(a_current), Some(b_fetched)).
/// `side_left = false` → the OTHER processor (drains stream B): own=B store, other=A store,
/// fetch `[t-after, t+before]` (SWAPPED), joiner(Some(a_fetched), Some(b_current)).
#[allow(dead_code)]
pub(crate) struct KStreamKStreamJoinProcessor<K, VThis, VOther, VO, F> {
    pub own_store: String,
    pub other_store: String,
    pub fetch_before: i64,   // THIS: windows.before ; OTHER: windows.after
    pub fetch_after: i64,    // THIS: windows.after  ; OTHER: windows.before
    pub joiner: F,           // outer form Fn(Option<&VA>, Option<&VB>) -> VO
    pub side_left: bool,     // which side this processor drains
    pub _pd: Marker<(K, VThis, VOther, VO)>,
}

#[async_trait]
impl<K, VThis, VOther, VA, VB, VO, F> Processor<K, VThis, K, VO>
    for KStreamKStreamJoinProcessor<K, VThis, VOther, VO, F>
where
    K: std::any::Any + Send + Sync + Clone,
    VThis: std::any::Any + Send + Sync + Clone,
    VOther: std::any::Any + Send + Sync + Clone,
    VA: 'static, VB: 'static, VO: std::any::Any + Send + Clone,
    F: Fn(Option<&VA>, Option<&VB>) -> VO + Send + 'static,
    // (VThis/VOther map to VA/VB by side; see the joiner-call note below)
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, VO>, r: Record<K, VThis>) {
        let key = r.key.expect("stream-stream join requires a non-null key");
        let t = r.timestamp;
        // 1) put own record into own store
        {
            let own = ctx.get_join_window_store::<K, VThis>(&self.own_store).expect("own join store");
            own.put(key.clone(), t, r.value.clone()).await;
        }
        // 2) fetch the other store over the side-swapped window
        let matches: Vec<(i64, VOther)> = {
            let other = ctx.get_join_window_store::<K, VOther>(&self.other_store).expect("other join store");
            other.fetch(&key, t - self.fetch_before, t + self.fetch_after).await
        };
        // 3) emit per match at max(t, t_other), joiner in canonical A-then-B order
        for (t_other, v_other) in matches {
            // side_left: a=this(VThis≡VA), b=other(VOther≡VB); else a=other, b=this.
            // The DSL builds `joiner` so the type args line up; see Task B3.
            let out = /* call self.joiner with (Some(a), Some(b)) per side */ todo_call_joiner(self, &r.value, &v_other);
            ctx.forward(Record::new(Some(key.clone()), out, std::cmp::max(t, t_other)));
        }
    }
}
```
**NOTE on the joiner type plumbing (resolve in implementation):** the cleanest is to make the processor monomorphic per side with the joiner already specialized to `Fn(&VThis, &VOther) -> VO` (the DSL wraps the outer-form joiner + `JoinKind` into a per-side closure that calls the user joiner with the correct A/B order and `Some(..)`). So the processor field is `joiner: Fn(&VThis, &VOther) -> VO` and `process` calls `(self.joiner)(&r.value, &v_other)`. Drop the `VA/VB` generics; the DSL supplies the per-side specialized closure. (This avoids the `VA/VB` mapping gymnastics above — prefer it.) Add `pub(crate) mod stream_join;` to `dsl/processors/mod.rs`.
- [ ] **Step 4: run → PASS (per-match emit + duplicates + max-ts); clippy; fmt; commit** `feat(streams-dsl): KStreamKStreamJoin processor (inner)`.

## Task B3: `KStream::join` (windowed) DSL + lowering

**Files:** `src/dsl/kstream.rs`, `src/dsl/names.rs`; `tests/dsl_execution.rs`.

- [ ] **Step 1: failing execution test** — `tests/dsl_execution.rs`:
```rust
#[test]
fn dsl_stream_stream_inner_join_executes() {
    use crabka_client_streams::dsl::StreamsBuilder;
    use crabka_client_streams::{Consumed, JoinWindows, Produced, StringSerde};
    let b = StreamsBuilder::new();
    let left = b.stream(["left"], Consumed::with(StringSerde, StringSerde));
    let right = b.stream(["right"], Consumed::with(StringSerde, StringSerde));
    left.join(&right, |a: &String, c: &String| format!("{a}{c}"), JoinWindows::of(10))
        .to("out", Produced::with(StringSerde, StringSerde));
    drop(right);
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // A at t=5 (no B yet → no match)
    d.pipe_input("left", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "a".to_string(), 5);
    assert_eq!(d.read_output("out", Produced::with(StringSerde, StringSerde)), None);
    // B at t=8 (within [5-10,5+10]) → join "ab"
    d.pipe_input("right", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "b".to_string(), 8);
    assert_eq!(d.read_output("out", Produced::with(StringSerde, StringSerde)), Some((Some("k".to_string()), "ab".to_string())));
    // B at t=20 (outside [5-10,5+10] of the A at 5) → no match
    d.pipe_input("right", Consumed::with(StringSerde, StringSerde), Some("k".to_string()), "c".to_string(), 20);
    assert_eq!(d.read_output("out", Produced::with(StringSerde, StringSerde)), None);
}
```
Plus a `dsl_stream_stream_join_swap_asymmetric` test (`JoinWindows::of(10).before(0).after(20)` — proves the swap: an A at t=0 matches a B at t=15 (within after=20) but a B at t=0 does NOT match an A at t=15 unless within the swapped window) and a `dsl_stream_stream_join_duplicates` test (two A's at the same ts + one B → two outputs). ADJUST to the real `stream`/`to`/`read_output` shapes.
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement.**
  - `dsl/names.rs`: add the join prefixes the capture pins — start with `KSTREAM_JOINTHIS = "KSTREAM-JOINTHIS-"`, `KSTREAM_JOINOTHER = "KSTREAM-JOINOTHER-"`, `KSTREAM_WINDOWED = "KSTREAM-WINDOWED-"` (and reuse `MERGE`). The exact set + store-name derivation is finalized in Task B4 against the fixture.
  - `dsl/kstream.rs`: add `pub fn join<V2,VO,F>(&self, other: &KStream<K,V2>, joiner: F, windows: JoinWindows) -> KStream<K,VO>` where `V2: Any+Send+Sync+Clone, VO: Any+Send+Clone, F: Fn(&V,&V2)->VO + Clone+Send+Sync+'static`. Build the two per-side specialized closures from `joiner`: `this_closure = move |a:&V, b:&V2| joiner(a,b)`, `other_closure = move |a:&V, b:&V2| joiner(a,b)` (same user fn; the per-side store wiring + fetch-swap give correctness). Lower like `dsl/ktable.rs::join_impl` (the 4c-iii dual+merge template) but: mint two **JoinWindow store** names; record THIS node (pred=self.node) + OTHER node (pred=other.node) + MERGE node (preds=[this,other]); each join thunk `add_processor::<K, Vside, K, VO, _,_,_>(name, || KStreamKStreamJoinProcessor{ own_store, other_store, fetch_before, fetch_after, joiner: side_closure, side_left, _pd }, [parent])`; register the two stores via `state.topology.add_join_window_store::<K, Vside, KS?, VS?>(store, key_serde, value_serde, windows.before_ms, windows.after_ms, windows.grace_ms, [h.name()])`; `connect_processor_store` each processor to BOTH stores (own + other); merge thunk reuses `stateless::MergeProcessor::<K, VO>`; `add_copartition_group([self.source_topic, other.source_topic])` when both Some. THIS uses `fetch_before=windows.before_ms, fetch_after=windows.after_ms, side_left=true`; OTHER uses `fetch_before=windows.after_ms, fetch_after=windows.before_ms` (**swapped**), `side_left=false`. Return `KStream::new(builder, merge_id)` (a `KStream<K,VO>`).
  - **Serdes for the window stores:** the join stores need serdes for K + each side's value. `KStream` doesn't currently carry value serdes (sources do via `Consumed`). For the test/golden, use `StringSerde`-typed values; thread the serdes through the join op signature (add `Consumed`/serde params, OR require the stream to carry them). Simplest for this slice: the `join` op takes the serdes implicitly via a `StreamJoined`-like param OR reuses the source `Consumed`. **Confirm the existing KStream-KTable `join` (4c-ii) serde threading and mirror it** (it pulls the table's store serdes; here both sides need serdes — add a minimal `StreamJoined::with(k, v1, v2)` param to `join`, mirroring `Grouped`/`Materialized`).
  - Both inputs must be copartitioned; a key-changed stream must `.repartition(..)` first (reuse 4c-ii's eager-panic convention — if `self.key_changing || other.key_changing`, panic with a clear message).
- [ ] **Step 4: run → inner exec PASS (join/swap/duplicates); the 9 goldens still byte-identical; clippy; fmt; commit** `feat(streams-dsl): KStream-KStream inner join (JoinWindows)`.

## Task B4: inner golden (`stream_stream_join`)

**Files:** `Capture.java`; `testdata/golden/dsl/stream_stream_join.topology.json` (NEW); `tests/dsl_golden_frame.rs`; possibly `dsl/names.rs`/`dsl/kstream.rs` (store-name index fixes).

- [ ] **Step 1: capture FIRST.** Add `streamStreamJoin()` to `Capture.java`:
  `b.stream("left", Consumed.with(Serdes.String(), Serdes.String())).join(b.stream("right", Consumed.with(Serdes.String(), Serdes.String())), (a,c)->a+c, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(60)), StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())).to("out", Produced.with(Serdes.String(), Serdes.String()));` Register in `main()` + bump the fixture count. Run `cd crates/client-streams/tests/jvm-capture && ./run.sh --gradle`. Commit `testdata/golden/dsl/stream_stream_join.topology.json`. NOTE: one subtopology; `source_topics: ["left","right"]`; TWO `state_changelog_topics` (the two window stores) each `cleanup.policy=delete` + `retention.ms=86520000` (60+60+0+1day); `copartition_groups: [[0,1]]`; NO outer store. The exact store NAMES + indices are the oracle. If Docker capture fails, report BLOCKED with the error — do NOT fabricate.
- [ ] **Step 2: failing golden test** — `tests/dsl_golden_frame.rs`: build the same topology (`left.join(&right, |a,c| format!("{a}{c}"), JoinWindows::of(60_000))` with the `StreamJoined`/serde param), `b.build_optimized("app").unwrap().to_wire()`, `assert_matches_fixture(&wire, "stream_stream_join")` (use `build` vs `build_optimized` to match the fixture).
- [ ] **Step 3: iterate** the store names + counter indices (mint/burn the `KSTREAM-WINDOWED-`/join names to land the two window-store names exactly where the fixture has them) + the changelog config until byte-match. The wire exposes only stores/changelogs/copartition, so the internal processor decomposition is free — match the store names + the two `delete` changelog configs + copartition.
- [ ] **Step 4: run → golden PASS (+ 9 prior byte-identical); clippy; fmt; commit** `feat(streams-dsl): stream_stream_join (inner) golden frame`.

---

# PHASE C — left/outer (window-close emission)

## Task C1: shared outer-join store codecs + `TimeTracker`

**Files:** Create `src/dsl/processors/outer_join_store.rs`.

- [ ] **Step 1: failing tests** — `outer_join_store.rs` tests: the composite key `outer_key(ts, side, key_bytes)` = `ts:8BE ‖ side:1 ‖ key_bytes` round-trips + sorts by ts then side; the `LeftOrRight<VA,VB>` tagged value serializes `0x00‖left_bytes` / `0x01‖right_bytes` and round-trips; `TimeTracker` updates `stream_time`/`min_time`.
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** `src/dsl/processors/outer_join_store.rs`:
```rust
//! Shared outer-join store codecs + the stream-time tracker (KIP-633 left/outer).
use bytes::{BufMut, Bytes, BytesMut};

/// `TimestampedKeyAndJoinSide` byte key: `ts:8BE ‖ side:1 ‖ key_bytes` (sorts by time).
pub(crate) fn outer_key(ts: i64, side_left: bool, key_bytes: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(9 + key_bytes.len());
    b.put_i64(ts);
    b.put_u8(if side_left { 0 } else { 1 });
    b.extend_from_slice(key_bytes);
    b.freeze()
}
pub(crate) fn outer_key_ts(k: &[u8]) -> i64 { i64::from_be_bytes(k[..8].try_into().expect("8")) }
pub(crate) fn outer_key_side_left(k: &[u8]) -> bool { k[8] == 0 }
pub(crate) fn outer_key_key_bytes(k: &[u8]) -> &[u8] { &k[9..] }

/// Tagged value: `0x00 ‖ left` or `0x01 ‖ right` (the unmatched record's value).
pub(crate) fn outer_value_left(raw: &[u8]) -> Bytes { let mut b = BytesMut::with_capacity(1 + raw.len()); b.put_u8(0); b.extend_from_slice(raw); b.freeze() }
pub(crate) fn outer_value_right(raw: &[u8]) -> Bytes { let mut b = BytesMut::with_capacity(1 + raw.len()); b.put_u8(1); b.extend_from_slice(raw); b.freeze() }
pub(crate) fn outer_value_decode(v: &[u8]) -> (bool /*is_left*/, &[u8]) { (v[0] == 0, &v[1..]) }

/// Shared per-join stream-time tracker.
#[derive(Debug, Default)]
pub(crate) struct TimeTracker { pub stream_time: i64, pub min_time: i64 }
impl TimeTracker {
    pub fn advance(&mut self, ts: i64) { self.stream_time = self.stream_time.max(ts); }
    pub fn update_min(&mut self, ts: i64) { if self.min_time == 0 || ts < self.min_time { self.min_time = ts; } }
}
```
(Match the JVM `LeftOrRightValue`/`TimestampedKeyAndJoinSide` byte layout if exactness is wanted; else this Crabka-internal layout suffices since the outer store is secondary state — pin against the outer-join fixture in Task C4.)
- [ ] **Step 4: run → PASS; clippy; fmt; commit** `feat(streams-dsl): outer-join store codecs + TimeTracker`.

## Task C2: emit-on-close in the join processor

**Files:** `src/dsl/processors/stream_join.rs` (extend the processor for left/outer).

- [ ] **Step 1: failing test** — `stream_join.rs` tests: an outer/left processor with a shared `Arc<Mutex<TimeTracker>>` + a shared outer KV store; process an A-record with no B-match → it is buffered (no eager emit while the window is open); then advance stream-time past the close threshold (a later A-record) → the buffered `joiner(a, None)` is emitted on the close scan.
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** — extend `KStreamKStreamJoinProcessor` with optional left/outer fields: `kind: JoinKind`, `outer_store: Option<String>`, `tracker: Option<Arc<Mutex<TimeTracker>>>`. After the inner emit loop:
  - `needs_outer = self.kind has the unmatched side enabled && matches.is_empty()`.
  - Bump `tracker.stream_time = max(stream_time, t)` (if tracker present).
  - If `needs_outer`: if outer store empty OR `t + self.fetch_after < stream_time` → emit the null-padded result eagerly (`joiner(Some(a), None)` for a left-side record / `joiner(None, Some(b))` for a right-side); else buffer into the outer store at `outer_key(t, side_left, kb)` with `outer_value_left/right(raw)` + `tracker.update_min(t)`.
  - **close scan**: iterate the outer store (via a `range`/`all` over the KV store) in key order; for each `(ts, side, k)` with `tracker.min_time + lookback + grace < stream_time` (lookback = `self.windows_after` for left-side / `self.windows_before` for right-side — store the raw `before/after/grace` on the processor), decode the value, emit `joiner(Some(a),None)`/`joiner(None,Some(b))` at `ts`, delete the entry; stop at the first still-open entry. (The outer store is a KV store; access it via `ctx.get_state_store::<OuterKey, OuterVal>`… — since the key/value are raw bytes, use a `BytesSerde`-typed KV store, i.e. `get_state_store::<Bytes, Bytes>` over a store registered with `BytesSerde`, and iterate with a `range`/scan. **The KV store needs a range/scan accessor** — if `KeyValueStore` lacks `range`, add a minimal `range`/`all` to it (the byte backend already has `range`); see the note below.)
  **NOTE — KV range:** the close-scan needs to iterate the outer store in key order. `KeyValueStore<K,V>` currently has `get/put/delete` only. Add `async fn range(&self, lo: &K, hi: &K) -> Vec<(K,V)>` (or an `all()`), backed by `ByteKeyValueStore::range`, to `KeyValueStore` + `KeyValueBytesStore` (small addition; the byte backend already supports it). Use it for the close scan over `[ts=0.., ts=stream_time+1)`.
- [ ] **Step 4: run → PASS (buffer-then-close-emit); clippy; fmt; commit** `feat(streams-dsl): stream-stream join emit-on-close (left/outer)`.

## Task C3: `left_join`/`outer_join` DSL + lowering

**Files:** `src/dsl/kstream.rs`, `src/dsl/names.rs`; `tests/dsl_execution.rs`.

- [ ] **Step 1: failing execution tests** — `tests/dsl_execution.rs`: `dsl_stream_stream_left_join_executes` (A with no B → `joiner(a, None)` = "anull" emitted only after a later record advances stream-time past `t + after + grace`); `dsl_stream_stream_outer_join_executes` (a B with no A → `joiner(None, b)` on close). Use `left_join`/`outer_join` with `JoinWindows::of(10)`; pipe records to advance stream-time and assert the null-padded results appear on close.
- [ ] **Step 2: run → FAIL.**
- [ ] **Step 3: implement** — `dsl/kstream.rs`: add `left_join<V2,VO,F: Fn(&V,Option<&V2>)->VO>` and `outer_join<V2,VO,F: Fn(Option<&V>,Option<&V2>)->VO>` mirroring `join`, but: wrap the joiner to outer form + set `JoinKind::left()`/`outer()`; in the lowering, additionally mint the `KSTREAM-OUTERSHARED-` store name + create one `Arc<Mutex<TimeTracker>>` (clone into both processor suppliers); register the shared outer store via `add_state_store::<Bytes,Bytes,_,_>(outer_store, BytesSerde, BytesSerde, [this, other])` (a KV store → compact changelog); `connect_processor_store` both processors to the outer store; pass `kind`, `Some(outer_store)`, `Some(tracker.clone())`, and the raw `before/after/grace` to each `KStreamKStreamJoinProcessor`. `dsl/names.rs`: `KSTREAM_OUTERSHARED = "KSTREAM-OUTERSHARED-"` (+ `KSTREAM-OUTERTHIS-`/`KSTREAM-OUTEROTHER-` if the fixture shows them).
- [ ] **Step 4: run → left/outer exec PASS; the 9 goldens still byte-identical; clippy; fmt; commit** `feat(streams-dsl): KStream-KStream left/outer join (window-close emission)`.

## Task C4: outer golden (`stream_stream_outer_join`)

**Files:** `Capture.java`; `testdata/golden/dsl/stream_stream_outer_join.topology.json` (NEW); `tests/dsl_golden_frame.rs`.

- [ ] **Step 1: capture FIRST.** Add `streamStreamOuterJoin()` to `Capture.java` (the `streamStreamJoin` topology but `outerJoin`). Run `./run.sh --gradle`. Commit the fixture. NOTE: the two window-store changelogs (`delete` + retention) PLUS the shared outer-store changelog (config pinned by the capture — likely `compact,delete` or `compact`) + copartition. If Docker fails, report BLOCKED.
- [ ] **Step 2: failing golden test** + **Step 3: iterate** the outer-store name/index + its changelog config + (if the JVM uses `KSTREAM-OUTERTHIS-/OUTEROTHER-` processor names that consume counter indices) the store-name burns, until byte-match.
- [ ] **Step 4: run → golden PASS (+ 9 prior byte-identical); clippy; fmt; commit** `feat(streams-dsl): stream_stream_outer_join golden frame`.

## Task C5: docs + final verification

**Files:** `src/lib.rs`.

- [ ] **Step 1:** add a `Windowed stream-stream join` note to `lib.rs` (`KStream::join`/`left_join`/`outer_join` with `JoinWindows`; both sides buffered in retainDuplicates window stores; inner emits per match; left/outer emit null-padded results on window close (stream-time-driven); copartitioned).
- [ ] **Step 2: full verification.** `cargo test -p crabka-client-streams` (join-store units + JoinWindows + inner/swap/dup + left/outer execution + `stream_stream_join` + `stream_stream_outer_join` goldens + **11 golden frames total [9 prior byte-identical]** + all prior tests + doctests); `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt -p crabka-client-streams -- --check`; `cargo build --workspace`.
- [ ] **Step 3: commit** `docs(streams-dsl): stream-stream-join note + #4d-iii verification`.

---

## Self-review

**Spec coverage:** §3 JoinWindows + seqnum + JoinWindowBytesStore → A1,A2,B1. §4 inner processors → B2,B3. §5 left/outer (outer store + tracker + emit-on-close) → C1,C2,C3. §6 DSL + JoinWindow changelog variant → A3,B3,C3. §7 capture/golden → B4,C4. §8 testing → all. §9 success → C5. §10 phasing → A/B/C. ✓

**Empirical-fixture notes (not placeholders):** store names + counter indices (B4/C4) and the shared-outer-store changelog config (C4) are validated against captured JVM fixtures (capture-first steps). The `KSTREAM-WINDOWED-`/`OUTERSHARED-` name set + burns are pinned by the fixture.

**Type consistency:** `store_key(.., seqnum)` (A1) → `JoinWindowBytesStore`/`JoinWindowStore`/`get_join_window`/`get_join_window_store` (A2) → `ChangelogKind{Kv|AggWindow|JoinWindow}` + `add_join_window_store` + `join_window_changelog_topic_configs` (A3) → `JoinWindows{before,after,grace}` (B1) → `KStreamKStreamJoinProcessor` (per-side specialized `Fn(&VThis,&VOther)->VO` joiner) (B2) → `KStream::join` lowering with 2 join-window stores + merge + copartition (B3) → inner golden (B4) → `outer_key`/`LeftOrRight`/`TimeTracker` (C1) → emit-on-close + `JoinKind` + `Arc<Mutex<TimeTracker>>` + `KeyValueStore::range` (C2) → `left_join`/`outer_join` + shared outer store (C3) → outer golden (C4). Consistent.

**Known risks (for the implementer):** (1) **the before/after swap** for the OTHER processor — the single easiest bug; the asymmetric-`.before`/`.after` execution test (B3) is the guard. (2) **joiner type plumbing** — make the processor hold a per-side specialized `Fn(&VThis,&VOther)->VO` (the DSL wraps the user joiner + side), not the raw outer-form generic, to avoid VA/VB mapping gymnastics. (3) **`ChangelogKind` ripple** — the `Option<i64>`→enum change touches node.rs + grouping.rs + wire.rs + ~4 wire tests + the grouping test; the 9 goldens must stay byte-identical (the enum is a spelling change, not a byte change, for KV+agg stores). (4) **`KeyValueStore::range`** is a new method needed by the close scan (C2) — add it backed by `ByteKeyValueStore::range`. (5) **the `Arc<Mutex<TimeTracker>>`** must be captured by BOTH processor suppliers (one Arc created in the lowering, cloned into each supplier closure). (6) **emit-on-close determinism** — Crabka is stream-time-only (no wall-clock); execution tests advance stream-time via later records, not wall-clock. (7) **outer-store serde** — registered as a `Bytes`/`Bytes` KV store (`BytesSerde`) so the composite key/tagged value are raw; the close scan uses `KeyValueStore::<Bytes,Bytes>::range`.
