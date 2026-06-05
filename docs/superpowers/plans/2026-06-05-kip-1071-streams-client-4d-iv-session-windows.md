# KIP-1071 Streams Client #4d-iv — Session Windows Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add session windows (`SessionWindows`) + session aggregations (`count` / `reduce` / `aggregate`) to the KIP-1071 Streams DSL, byte-exact against JVM Kafka-Streams 4.1.

**Architecture:** A third typed store (`SessionBytesStore<K,V>`) over the pluggable byte backend (beside `KeyValueBytesStore` and `WindowBytesStore`), with a `SessionKeySchema` codec (`key‖end:8BE‖start:8BE`, end-first). A session aggregate processor implements the JVM merge: each record merges all sessions within `gap`, tombstones the merged-away sessions, and emits the new merged session. The DSL `SessionWindowedKGroupedStream` mirrors `TimeWindowedKGroupedStream`, producing `KTable<Windowed<K>, VA>`.

**Tech Stack:** Rust, `async-trait`, `bytes`, `tokio`; reuses 4d-i pluggable `ByteKeyValueStore`, 4d-ii `Windowed<K>`/`Window`/window-store templates, 4c `Change<V>`.

**Branch / worktree:** `streams-4d-iv-session-windows` (stacked on `streams-4d-iii-stream-join` / PR #399) in `/Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl`. Spec: `docs/superpowers/specs/2026-06-05-kip-1071-streams-client-4d-iv-session-windows-design.md`.

**Git discipline:** all git via `git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl …`; assert branch `== streams-4d-iv-session-windows` before each commit; commit with `-c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com"`; never `git config`; no push.

**Key design decision — no new `ChangelogKind`:** the JVM session-store changelog is the same `WindowedChangelogTopicConfig` as a window store (`compact,delete` + `retention.ms`); only the retention *value* differs (`gap + grace + 86_400_000`). So session stores **reuse `ChangelogKind::AggWindow`** via the existing `NodeRegistry::add_window_store`; only a new typed `Topology::add_session_store` (which builds a `SessionBytesStore` factory) is needed. The Phase C golden validates this; if 4.1's session changelog differs, add a `ChangelogKind::Session` variant then.

**Value layout:** session stores hold the **raw** serialized aggregate (no `ValueAndTimestamp` wrap — the session end is the time). Confirmed against the capture in Phase C.

---

## File Structure

**New files:**
- `crates/client-streams/src/store/session_schema.rs` — `SessionKeySchema` byte codec.
- `crates/client-streams/src/store/session.rs` — `SessionStore` trait + `SessionBytesStore<K,V>`.
- `crates/client-streams/src/dsl/processors/session_aggregate.rs` — the two merge processors.
- `crates/client-streams/src/dsl/session_windowed_kgrouped.rs` — `SessionWindowedKGroupedStream` terminal ops + lowering.
- `crates/client-streams/tests/testdata/golden/dsl/session_count.topology.json` — JVM capture (Phase C).

**Modified files:**
- `src/store/mod.rs` — register `session_schema` + `session` modules.
- `src/store/registry.rs` — `get_session` downcast.
- `src/processor/api.rs` — `get_session_store` context accessor.
- `src/topology/builder.rs` — `add_session_store`.
- `src/dsl/windows.rs` — `SessionWindows` + `SessionWindowedSerde`.
- `src/dsl/processors/mod.rs` — register `session_aggregate`.
- `src/dsl/kgrouped.rs` — `windowed_by_session`.
- `src/dsl/mod.rs` + `src/lib.rs` — re-export `SessionWindows`, `SessionWindowedSerde`; lib prose.
- `tests/dsl_execution.rs` — session execution tests.
- `tests/dsl_golden_frame.rs` — `session_count_matches_jvm`.
- `tests/jvm-capture/src/main/java/crabka/capture/Capture.java` + `run.sh` — fixture #12.

## Execution batches (non-overlapping file sets per batch → parallel dispatch)

- **Batch 1 (parallel):** Task 1 (`session_schema.rs`) ∥ Task 2 (`windows.rs`).
- **Batch 2:** Task 3 (`session.rs` + registry/api/builder/store-mod) — needs Task 1.
- **Batch 3:** Task 4 (`session_aggregate.rs`) — needs Tasks 2, 3.
- **Batch 4:** Task 5 (`session_windowed_kgrouped.rs` + kgrouped/mod/lib) — needs Task 4.
- **Batch 5:** Task 6 (execution tests) — needs Task 5.
- **Batch 6 (Phase C):** Task 7 (capture + golden) then Task 8 (docs + final verify) — need Task 6.

---

## Task 1: `SessionKeySchema` byte codec

**Files:**
- Create: `crates/client-streams/src/store/session_schema.rs`
- Modify: `crates/client-streams/src/store/mod.rs`

- [ ] **Step 1: Register the module.** In `src/store/mod.rs`, add after the `pub(crate) mod registry;` / `pub mod window;` block a line (keep alphabetical-ish with the others):

```rust
pub(crate) mod session_schema;
```

(Place it next to `pub(crate) mod window_schema;`.)

- [ ] **Step 2: Write the failing test + implementation together** (single new file). Create `src/store/session_schema.rs`:

```rust
//! JVM-exact session-store byte codec (`SessionKeySchema`).
//! Store/changelog KEY: `key_bytes ‖ end:8B BE ‖ start:8B BE` (END first, so the
//! store sorts by `(key, end, start)` — the merge fetch scans by session end).
//! VALUE: the raw serialized aggregate (session stores are not
//! `ValueAndTimestamp`-wrapped; the session end carries the time).
use bytes::{BufMut, Bytes, BytesMut};

const TS_SIZE: usize = 8;
const SUFFIX_SIZE: usize = TS_SIZE * 2; // end(8) + start(8)

/// `SessionKeySchema.toBinary(key, start, end)` → `key ‖ end:8BE ‖ start:8BE`.
pub(crate) fn session_key(key_bytes: &[u8], start: i64, end: i64) -> Bytes {
    let mut b = BytesMut::with_capacity(key_bytes.len() + SUFFIX_SIZE);
    b.extend_from_slice(key_bytes);
    b.put_i64(end);
    b.put_i64(start);
    b.freeze()
}

/// The session END encoded in a composite key (`k[len-16 .. len-8]`).
pub(crate) fn session_end_of(k: &[u8]) -> i64 {
    let n = k.len();
    i64::from_be_bytes(k[n - SUFFIX_SIZE..n - TS_SIZE].try_into().expect("8 bytes"))
}

/// The session START encoded in a composite key (`k[len-8 .. len]`).
pub(crate) fn session_start_of(k: &[u8]) -> i64 {
    let n = k.len();
    i64::from_be_bytes(k[n - TS_SIZE..].try_into().expect("8 bytes"))
}

/// The serialized inner-key bytes of a composite session key.
pub(crate) fn session_key_bytes_of(k: &[u8]) -> &[u8] {
    &k[..k.len() - SUFFIX_SIZE]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_layout_end_first() {
        let k = session_key(b"k", 5, 9); // start=5, end=9
        assert_eq!(k.len(), 17); // "k"(1) ‖ end:8 ‖ start:8
        assert_eq!(&k[1..9], &9i64.to_be_bytes()); // end first
        assert_eq!(&k[9..17], &5i64.to_be_bytes()); // start second
        assert_eq!(session_end_of(&k), 9);
        assert_eq!(session_start_of(&k), 5);
        assert_eq!(session_key_bytes_of(&k), b"k");
    }

    #[test]
    fn sorts_by_end_then_start() {
        // Same key: higher END sorts after (end is the dominant 8-byte field).
        let lo = session_key(b"k", 0, 5);
        let hi = session_key(b"k", 0, 7);
        assert!(hi > lo);
        // Same key + end: higher START sorts after.
        let a = session_key(b"k", 3, 9);
        let b = session_key(b"k", 4, 9);
        assert!(b > a);
    }
}
```

- [ ] **Step 3: Run tests.** Run: `cargo test -p crabka-client-streams session_schema -- --include-ignored`
Expected: `session_key_layout_end_first` + `sorts_by_end_then_start` PASS.

- [ ] **Step 4: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/store/session_schema.rs crates/client-streams/src/store/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-store): SessionKeySchema byte codec (key‖end‖start, end-first)"
```

---

## Task 2: `SessionWindows` + `SessionWindowedSerde`

**Files:**
- Modify: `crates/client-streams/src/dsl/windows.rs` (append; the file already holds `TimeWindows`, `JoinWindows`, `Window`, `Windowed`, `TimeWindowedSerde`)
- Modify: `crates/client-streams/src/dsl/mod.rs`, `crates/client-streams/src/lib.rs` (re-exports)

- [ ] **Step 1: Append `SessionWindows` + `SessionWindowedSerde` + tests** to `src/dsl/windows.rs` (above the existing `#[cfg(test)] mod tests` block — put the impls before tests, then add the test fns inside the existing tests module):

Impl block (insert after the `JoinWindows` impl, before the `TimeWindowedSerde` struct):

```rust
/// Session windows: records for a key form one session while they stay within
/// `gap_ms` of each other (inactivity gap). A session window `[start, end]` is
/// defined by data, not epoch-aligned. `grace_ms` only affects changelog
/// retention here (window closing is deferred, as in the other windowing slices).
#[derive(Debug, Clone, Copy)]
pub struct SessionWindows {
    pub gap_ms: i64,
    pub grace_ms: i64,
}

impl SessionWindows {
    /// Inactivity gap of `gap_ms` (grace 0). `gap_ms > 0`.
    #[must_use]
    pub fn of_inactivity_gap(gap_ms: i64) -> Self {
        assert!(gap_ms > 0, "session gap must be > 0");
        Self { gap_ms, grace_ms: 0 }
    }
    /// Set the grace period (only affects changelog retention here).
    #[must_use]
    pub fn grace(mut self, grace_ms: i64) -> Self {
        assert!(grace_ms >= 0, "grace must be >= 0");
        self.grace_ms = grace_ms;
        self
    }
}

/// `Serde<Windowed<K>>` producing the JVM session **output-topic** format:
/// `inner_key_bytes ‖ end:8B BE ‖ start:8B BE` (both bounds in the bytes; distinct
/// from `TimeWindowedSerde`, which encodes only the start and derives `end`).
#[derive(Debug, Clone, Copy)]
pub struct SessionWindowedSerde<KS> {
    inner: KS,
}

impl<KS> SessionWindowedSerde<KS> {
    #[must_use]
    pub fn new(inner: KS) -> Self {
        Self { inner }
    }
}

impl<K, KS> Serde<Windowed<K>> for SessionWindowedSerde<KS>
where
    K: Send + Sync + 'static,
    KS: Serde<K>,
{
    fn serialize(&self, value: &Windowed<K>) -> Bytes {
        let kb = self.inner.serialize(&value.key);
        let mut b = BytesMut::with_capacity(kb.len() + 16);
        b.extend_from_slice(&kb);
        b.put_i64(value.window.end);
        b.put_i64(value.window.start);
        b.freeze()
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Windowed<K>, SerdeError> {
        if bytes.len() < 16 {
            return Err(SerdeError(format!("session key too short: {}", bytes.len())));
        }
        let split = bytes.len() - 16;
        let key = self.inner.deserialize(&bytes[..split])?;
        let end = i64::from_be_bytes(bytes[split..split + 8].try_into().expect("8 bytes"));
        let start = i64::from_be_bytes(bytes[split + 8..].try_into().expect("8 bytes"));
        Ok(Windowed { key, window: Window { start, end } })
    }
}
```

Tests (add inside the existing `mod tests`):

```rust
    #[test]
    fn session_windows_gap_and_grace() {
        let w = SessionWindows::of_inactivity_gap(60_000);
        assert_eq!((w.gap_ms, w.grace_ms), (60_000, 0));
        let g = SessionWindows::of_inactivity_gap(60_000).grace(5);
        assert_eq!((g.gap_ms, g.grace_ms), (60_000, 5));
    }

    #[test]
    fn session_windowed_serde_round_trips_end_then_start() {
        use crate::processor::serde::{Serde, StringSerde};
        let s = SessionWindowedSerde::new(StringSerde);
        let wk = Windowed { key: "k".to_string(), window: Window { start: 5, end: 9 } };
        let b = s.serialize(&wk);
        assert_eq!(b.len(), 17); // "k"(1) ‖ end:8 ‖ start:8
        assert_eq!(&b[1..9], &9i64.to_be_bytes()); // end first
        assert_eq!(&b[9..17], &5i64.to_be_bytes()); // start second
        let back = s.deserialize(&b).unwrap();
        assert_eq!(back.key, "k");
        assert_eq!(back.window, Window { start: 5, end: 9 });
    }
```

- [ ] **Step 2: Re-export.** In `src/dsl/mod.rs` change the windows re-export line to include the new types:

```rust
pub use windows::{
    JoinWindows, SessionWindowedSerde, SessionWindows, TimeWindowedSerde, TimeWindows, Window,
    Windowed,
};
```

In `src/lib.rs`, find the public re-export line listing `TimeWindowedSerde, TimeWindows, Window, Windowed` (around line 309-310) and add `SessionWindows, SessionWindowedSerde` to it, e.g.:

```rust
    SessionWindowedSerde, SessionWindows, StreamJoined, StreamsBuilder, TimeWindowedSerde,
    TimeWindows, Window, Windowed,
```

(Keep the existing items; just insert the two `Session*` names in alphabetical position. Match the exact surrounding names already present.)

- [ ] **Step 3: Run tests.** Run: `cargo test -p crabka-client-streams --lib windows::tests`
Expected: the two new tests + existing windows tests PASS.

- [ ] **Step 4: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/windows.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): SessionWindows + SessionWindowedSerde (key‖end‖start output)"
```

---

## Task 3: `SessionStore` + `SessionBytesStore` + registry/context/builder plumbing

**Files:**
- Create: `crates/client-streams/src/store/session.rs`
- Modify: `crates/client-streams/src/store/mod.rs`, `src/store/registry.rs`, `src/processor/api.rs`, `src/topology/builder.rs`

This mirrors `WindowBytesStore` (`src/store/window.rs`) exactly, changing the key codec to `SessionKeySchema`, the value to **raw** (no `ValueAndTimestamp`), and the access methods to `find_sessions` / `put(start,end)` / `remove(start,end)`.

- [ ] **Step 1: Register the module.** In `src/store/mod.rs` add (next to `pub mod window;`):

```rust
pub mod session;
```

- [ ] **Step 2: Create `src/store/session.rs`** with the trait, the store, and tests:

```rust
//! Session store over the byte backend: `SessionKeySchema` keys (`key‖end‖start`)
//! + raw aggregate values. The third typed store beside `KeyValueBytesStore` and
//! `WindowBytesStore`. Supports the JVM session-merge fetch (`find_sessions`).
use std::any::Any;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::byte::{ByteKeyValueStore, InMemoryBytes};
use crate::store::session_schema::{
    session_end_of, session_key, session_key_bytes_of, session_start_of,
};

/// Typed session store keyed by `(K, start, end)`. `find_sessions` returns the
/// merge candidates for a record: sessions whose `[start, end]` overlaps the
/// inactivity gap window `[earliest_end, latest_start]`.
#[async_trait]
pub trait SessionStore<K: Send + Sync, V: Send>: StateStore {
    /// Sessions for `key` with `end >= earliest_end && start <= latest_start`,
    /// returned as `(start, end, value)` in store order (end asc, then start asc).
    async fn find_sessions(
        &self,
        key: &K,
        earliest_end: i64,
        latest_start: i64,
    ) -> Vec<(i64, i64, V)>;
    async fn put(&mut self, key: K, start: i64, end: i64, value: V);
    async fn remove(&mut self, key: &K, start: i64, end: i64);
}

pub struct SessionBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backend: Box<dyn ByteKeyValueStore>,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> SessionBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        backend: Box<dyn ByteKeyValueStore>,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            backend,
            key_serde,
            value_serde,
            changelog: Vec::new(),
            logging: true,
        }
    }

    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(
            name,
            Box::new(InMemoryBytes::default()),
            key_serde,
            value_serde,
            changelog_topic,
        )
    }
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for SessionBytesStore<K, V> {
    fn name(&self) -> &str {
        &self.name
    }
    async fn flush(&mut self) {}
    fn close(&mut self) {}
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn changelog_topic(&self) -> &str {
        &self.changelog_topic
    }
    fn take_changelog(&mut self) -> Vec<(Bytes, Option<Bytes>)> {
        std::mem::take(&mut self.changelog)
    }
    async fn apply_changelog(&mut self, key: Bytes, value: Option<Bytes>) {
        match value {
            Some(v) => self.backend.put(key, v).await,
            None => {
                self.backend.delete(&key).await;
            }
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> SessionStore<K, V> for SessionBytesStore<K, V> {
    async fn find_sessions(
        &self,
        key: &K,
        earliest_end: i64,
        latest_start: i64,
    ) -> Vec<(i64, i64, V)> {
        let kb = self.key_serde.serialize(key);
        // Lower bound: smallest qualifying end (clamped to 0 — stored ends are
        // non-negative epoch millis; a negative earliest_end means "all qualify").
        let lo = session_key(&kb, 0, earliest_end.max(0));
        // Upper bound: past every entry for this key prefix.
        let hi = session_key(&kb, i64::MAX, i64::MAX);
        let mut out = Vec::new();
        for (k, raw) in self.backend.range(&lo, &hi).await {
            if session_key_bytes_of(&k) != kb.as_ref() {
                continue; // guard prefix collisions with a different key
            }
            let end = session_end_of(&k);
            let start = session_start_of(&k);
            if end >= earliest_end && start <= latest_start {
                out.push((
                    start,
                    end,
                    self.value_serde
                        .deserialize(&raw)
                        .expect("session value deserialize"),
                ));
            }
        }
        out
    }

    async fn put(&mut self, key: K, start: i64, end: i64, value: V) {
        let kb = self.key_serde.serialize(&key);
        let sk = session_key(&kb, start, end);
        let raw = self.value_serde.serialize(&value);
        self.backend.put(sk.clone(), raw.clone()).await;
        if self.logging {
            self.changelog.push((sk, Some(raw)));
        }
    }

    async fn remove(&mut self, key: &K, start: i64, end: i64) {
        let kb = self.key_serde.serialize(key);
        let sk = session_key(&kb, start, end);
        self.backend.delete(&sk).await;
        if self.logging {
            self.changelog.push((sk, None));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    fn store() -> SessionBytesStore<String, i64> {
        SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-s-changelog".into(),
        )
    }

    #[tokio::test]
    async fn put_find_remove_and_changelog() {
        let mut s = store();
        s.put("k".to_string(), 0, 10, 1).await; // session [0,10]
        s.put("k".to_string(), 50, 60, 2).await; // session [50,60]
        // gap=20 around ts=15 → earliest_end=15-20=-5, latest_start=15+20=35 →
        // only [0,10] qualifies (end 10 >= -5, start 0 <= 35); [50,60] start 50 > 35.
        let found = s.find_sessions(&"k".to_string(), -5, 35).await;
        assert_eq!(found, vec![(0, 10, 1)]);
        // remove [0,10]
        s.remove(&"k".to_string(), 0, 10).await;
        assert_eq!(s.find_sessions(&"k".to_string(), -5, 35).await, vec![]);
        // changelog: put, put, remove → 3 entries (last is a tombstone)
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 3);
        assert!(cl[2].1.is_none());
    }

    #[tokio::test]
    async fn find_sessions_returns_store_order_end_then_start() {
        let mut s = store();
        s.put("k".to_string(), 0, 30, 1).await;
        s.put("k".to_string(), 0, 10, 2).await;
        // both qualify for earliest_end=0, latest_start=100; store order = end asc.
        let found = s.find_sessions(&"k".to_string(), 0, 100).await;
        assert_eq!(found, vec![(0, 10, 2), (0, 30, 1)]);
    }

    #[tokio::test]
    async fn other_key_prefix_is_not_returned() {
        let mut s = store();
        s.put("k".to_string(), 0, 10, 1).await;
        s.put("kk".to_string(), 0, 10, 9).await; // longer key sharing the "k" prefix
        let found = s.find_sessions(&"k".to_string(), 0, 100).await;
        assert_eq!(found, vec![(0, 10, 1)]);
    }

    #[tokio::test]
    async fn restore_via_changelog_rebuilds_sessions() {
        let mut s = store();
        s.put("k".to_string(), 0, 10, 1).await;
        s.put("k".to_string(), 50, 60, 2).await;
        s.remove(&"k".to_string(), 0, 10).await; // a tombstone in the changelog
        let cl = s.take_changelog();
        // Clean-slate restore: replay the changelog into a fresh store.
        let mut s2 = store();
        for (k, v) in cl {
            s2.apply_changelog(k, v).await;
        }
        // [0,10] was removed; only [50,60] survives.
        assert_eq!(
            s2.find_sessions(&"k".to_string(), 0, 100).await,
            vec![(50, 60, 2)]
        );
    }
}
```

- [ ] **Step 3: Add the registry downcast.** In `src/store/registry.rs`, after `get_join_window` (around line 61) add:

```rust
    /// Typed mutable access: downcast the erased store to the session store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_session<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::session::SessionStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::session::SessionBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::session::SessionStore<K, V>)
    }
```

- [ ] **Step 4: Add the context accessor.** In `src/processor/api.rs`, after `get_join_window_store` (around line 168) add:

```rust
    /// Access a connected session store, typed. `None` if absent or the K/V types
    /// don't match. Fetch it per-record (do not hold across `process` calls).
    pub fn get_session_store<K2: Send + Sync + 'static, V2: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::session::SessionStore<K2, V2>> {
        self.dispatch.stores.get_session::<K2, V2>(name)
    }
```

(Match the surrounding method style; the `self.dispatch.stores` path is what `get_join_window_store` uses.)

- [ ] **Step 5: Add the typed builder.** In `src/topology/builder.rs`, after `add_join_window_store` (find it after `add_window_store`, ~line 495+) add a parallel method:

```rust
    /// Register a session state store connected to the given processors.
    ///
    /// Like [`add_window_store`] but for session stores. Reuses the windowed
    /// (`compact,delete`) changelog config; the `retention.ms` is derived from
    /// `gap_ms + grace_ms + 86_400_000` (JVM `windowstore.changelog.additional.
    /// retention.ms` default of 1 day). The store holds the raw aggregate
    /// (`SessionBytesStore`).
    ///
    /// [`add_window_store`]: Topology::add_window_store
    pub fn add_session_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        gap_ms: i64,
        grace_ms: i64,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let retention_ms = gap_ms + grace_ms + 86_400_000;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        // Session changelog == windowed changelog (compact,delete + retention);
        // reuse the AggWindow ChangelogKind via add_window_store.
        self.reg.add_window_store(&name, procs, None, retention_ms);
        self.store_factories.insert(
            name.clone(),
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::session::SessionBytesStore::<K, V>::new(
                            store_name.to_string(),
                            backend,
                            Box::new(key_serde.clone()),
                            Box::new(value_serde.clone()),
                            changelog,
                        )) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }
```

- [ ] **Step 6: Run tests.** Run: `cargo test -p crabka-client-streams session::tests` then `cargo test -p crabka-client-streams --lib registry`
Expected: `put_find_remove_and_changelog`, `find_sessions_returns_store_order_end_then_start`, `other_key_prefix_is_not_returned` PASS; registry tests still PASS. Also `cargo build -p crabka-client-streams` (the api.rs + builder.rs additions compile).

- [ ] **Step 7: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/store/session.rs crates/client-streams/src/store/mod.rs crates/client-streams/src/store/registry.rs crates/client-streams/src/processor/api.rs crates/client-streams/src/topology/builder.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-store): SessionStore + SessionBytesStore + get_session/add_session_store"
```

---

## Task 4: Session aggregate + reduce processors (the merge engine)

**Files:**
- Create: `crates/client-streams/src/dsl/processors/session_aggregate.rs`
- Modify: `crates/client-streams/src/dsl/processors/mod.rs`

Mirrors `src/dsl/processors/window_aggregate.rs` but implements the JVM session merge.

- [ ] **Step 1: Register the module.** In `src/dsl/processors/mod.rs` add (next to `pub(crate) mod window_aggregate;`):

```rust
pub(crate) mod session_aggregate;
```

- [ ] **Step 2: Create `src/dsl/processors/session_aggregate.rs`:**

```rust
//! Session-window aggregation processors: JVM session-merge, emit-on-update.
//!
//! On each record the processor finds all sessions within the inactivity gap,
//! merges them (and the record) into one `[minStart, maxEnd]` session, removes
//! the merged-away sessions (emitting a tombstone per the now-stale session key),
//! and emits the new merged session. No window closing / suppression.
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::dsl::processors::change::Change;
use crate::dsl::windows::{Window, Windowed};
use crate::processor::api::{Processor, ProcessorContext};
use crate::processor::record::Record;

/// Variance-neutral marker for multi-param processor structs.
type Marker<T> = PhantomData<fn() -> T>;

/// General session aggregation (`count` / `aggregate`). `init` seeds the
/// accumulator, `agg` folds the new record, `merger` combines two session
/// aggregates when sessions merge.
#[allow(dead_code)]
pub(crate) struct KStreamSessionAggregateProcessor<K, V, VA, I, A, M> {
    pub store_name: String,
    pub gap_ms: i64,
    pub init: I,
    pub agg: A,
    pub merger: M,
    pub _pd: Marker<(K, V, VA)>,
}

#[async_trait]
impl<K, V, VA, I, A, M> Processor<K, V, Windowed<K>, Change<VA>>
    for KStreamSessionAggregateProcessor<K, V, VA, I, A, M>
where
    K: std::any::Any + Send + Sync + Clone,
    V: Send + 'static,
    VA: std::any::Any + Send + Clone,
    I: Fn() -> VA + Send + 'static,
    A: Fn(&K, &V, VA) -> VA + Send + 'static,
    M: Fn(&K, VA, VA) -> VA + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<VA>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("session aggregate requires a non-null key");
        let ts = r.timestamp;
        let gap = self.gap_ms;

        // 1. find merge candidates: sessions overlapping [ts-gap, ts+gap].
        let cands: Vec<(i64, i64, VA)> = {
            let store = ctx
                .get_session_store::<K, VA>(&self.store_name)
                .expect("session store not found");
            store.find_sessions(&key, ts - gap, ts + gap).await
        };

        // 2. merge: fold candidate aggregates via the merger, then the record.
        let mut new_start = ts;
        let mut new_end = ts;
        let mut acc = (self.init)();
        for (s, e, v) in &cands {
            acc = (self.merger)(&key, acc, v.clone());
            new_start = new_start.min(*s);
            new_end = new_end.max(*e);
        }
        acc = (self.agg)(&key, &r.value, acc);

        // 3. remove + tombstone each merged-away session (its key is now stale).
        for (s, e, v) in &cands {
            {
                let store = ctx
                    .get_session_store::<K, VA>(&self.store_name)
                    .expect("session store not found");
                store.remove(&key, *s, *e).await;
            }
            ctx.forward(Record::new(
                Some(Windowed { key: key.clone(), window: Window { start: *s, end: *e } }),
                Change::tombstone(Some(v.clone())),
                *e,
            ));
        }

        // 4. put + emit the new merged session.
        {
            let store = ctx
                .get_session_store::<K, VA>(&self.store_name)
                .expect("session store not found");
            store.put(key.clone(), new_start, new_end, acc.clone()).await;
        }
        ctx.forward(Record::new(
            Some(Windowed { key: key.clone(), window: Window { start: new_start, end: new_end } }),
            Change::update(None, acc),
            new_end,
        ));
    }
}

/// Session reduce: keeps the public value type `V` (no `init`/sentinel). The
/// first contribution seeds; folding old sessions + the new record uses
/// `reducer`. The merge structure is identical to the aggregate processor.
#[allow(dead_code)]
pub(crate) struct KStreamSessionReduceProcessor<K, V, R> {
    pub store_name: String,
    pub gap_ms: i64,
    pub reducer: R,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V, R> Processor<K, V, Windowed<K>, Change<V>> for KStreamSessionReduceProcessor<K, V, R>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
    R: Fn(&V, &V) -> V + Send + 'static,
{
    async fn process(
        &mut self,
        ctx: &mut ProcessorContext<'_, '_, Windowed<K>, Change<V>>,
        r: Record<K, V>,
    ) {
        let key = r.key.expect("session reduce requires a non-null key");
        let ts = r.timestamp;
        let gap = self.gap_ms;

        let cands: Vec<(i64, i64, V)> = {
            let store = ctx
                .get_session_store::<K, V>(&self.store_name)
                .expect("session store not found");
            store.find_sessions(&key, ts - gap, ts + gap).await
        };

        let mut new_start = ts;
        let mut new_end = ts;
        let mut acc: Option<V> = None;
        for (s, e, v) in &cands {
            acc = Some(match acc {
                None => v.clone(),
                Some(a) => (self.reducer)(&a, v),
            });
            new_start = new_start.min(*s);
            new_end = new_end.max(*e);
        }
        let acc: V = match acc {
            None => r.value.clone(),
            Some(a) => (self.reducer)(&a, &r.value),
        };

        for (s, e, v) in &cands {
            {
                let store = ctx
                    .get_session_store::<K, V>(&self.store_name)
                    .expect("session store not found");
                store.remove(&key, *s, *e).await;
            }
            ctx.forward(Record::new(
                Some(Windowed { key: key.clone(), window: Window { start: *s, end: *e } }),
                Change::tombstone(Some(v.clone())),
                *e,
            ));
        }

        {
            let store = ctx
                .get_session_store::<K, V>(&self.store_name)
                .expect("session store not found");
            store.put(key.clone(), new_start, new_end, acc.clone()).await;
        }
        ctx.forward(Record::new(
            Some(Windowed { key: key.clone(), window: Window { start: new_start, end: new_end } }),
            Change::update(None, acc),
            new_end,
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::marker::PhantomData;

    use super::*;
    use crate::processor::api::ProcessorContext;
    use crate::processor::erased::{Dispatch, ErasedRecord};
    use crate::processor::record::{Record, RecordContext};
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::registry::StoreRegistry;
    use crate::store::session::SessionBytesStore;

    fn registry() -> StoreRegistry {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-s-changelog".into(),
        )));
        stores
    }

    #[tokio::test]
    async fn merge_within_gap_tombstones_then_updates() {
        let mut stores = registry();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };

        let mut proc = KStreamSessionAggregateProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            merger: |_k: &String, a: i64, b: i64| a + b,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };

        // record 1 @ ts=0 → new session [0,0] count 1, no candidates → one update.
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 0)).await;
        }
        assert_eq!(buffer.len(), 1);
        let (_, rec) = buffer.pop_front().unwrap();
        let ch = rec.value.downcast::<Change<i64>>().unwrap();
        let wk = rec.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(wk.window, Window { start: 0, end: 0 });
        assert_eq!((ch.old, ch.new), (None, Some(1)));

        // record 2 @ ts=30 (within gap 60 of session [0,0]) → merge:
        //   tombstone [0,0], update merged [0,30] count 2.
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 30)).await;
        }
        assert_eq!(buffer.len(), 2);
        let (_, tomb) = buffer.pop_front().unwrap();
        let tch = tomb.value.downcast::<Change<i64>>().unwrap();
        let tkey = tomb.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(tkey.window, Window { start: 0, end: 0 });
        assert!(tch.is_tombstone());
        let (_, upd) = buffer.pop_front().unwrap();
        let uch = upd.value.downcast::<Change<i64>>().unwrap();
        let ukey = upd.key.unwrap().downcast::<Windowed<String>>().unwrap();
        assert_eq!(ukey.window, Window { start: 0, end: 30 });
        assert_eq!((uch.old, uch.new), (None, Some(2)));
    }

    #[tokio::test]
    async fn beyond_gap_two_independent_sessions() {
        let mut stores = registry();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };
        let mut proc = KStreamSessionAggregateProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            merger: |_k: &String, a: i64, b: i64| a + b,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };
        for ts in [0i64, 200] {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), ts)).await;
        }
        // No merge: two updates, no tombstone (200 is > gap 60 from [0,0]).
        assert_eq!(buffer.len(), 2);
        for (_, rec) in buffer.drain(..) {
            assert!(!rec.value.downcast::<Change<i64>>().unwrap().is_tombstone());
        }
    }

    #[tokio::test]
    async fn three_way_bridge_merge() {
        let mut stores = registry();
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };
        let mut proc = KStreamSessionAggregateProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            init: || 0i64,
            agg: |_k: &String, _v: &String, a: i64| a + 1,
            merger: |_k: &String, a: i64, b: i64| a + b,
            _pd: PhantomData::<fn() -> (String, String, i64)>,
        };
        // ts=0 → [0,0]; ts=100 → [100,100] (not within gap of [0,0]); ts=50 bridges
        // both → merged [0,100] count 3 + tombstones for [0,0] and [100,100].
        for ts in [0i64, 100] {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), ts)).await;
        }
        buffer.clear(); // discard the two initial updates
        {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<i64>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), "x".into(), 50)).await;
        }
        // Deterministic emit order: tombstone candidates in store order (end asc) —
        // [0,0] then [100,100] — then the merged update [0,100] = 3.
        assert_eq!(buffer.len(), 3);
        let (_, t0) = buffer.pop_front().unwrap();
        assert!(t0.value.downcast::<Change<i64>>().unwrap().is_tombstone());
        assert_eq!(t0.key.unwrap().downcast::<Windowed<String>>().unwrap().window, Window { start: 0, end: 0 });
        let (_, t1) = buffer.pop_front().unwrap();
        assert!(t1.value.downcast::<Change<i64>>().unwrap().is_tombstone());
        assert_eq!(t1.key.unwrap().downcast::<Windowed<String>>().unwrap().window, Window { start: 100, end: 100 });
        let (_, upd) = buffer.pop_front().unwrap();
        assert_eq!(upd.key.unwrap().downcast::<Windowed<String>>().unwrap().window, Window { start: 0, end: 100 });
        assert_eq!(upd.value.downcast::<Change<i64>>().unwrap().new, Some(3));
    }

    #[tokio::test]
    async fn reduce_first_seeds_then_folds() {
        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(SessionBytesStore::<String, String>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(StringSerde),
            "app-s-changelog".into(),
        )));
        let children = [0usize];
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let mut output = Vec::new();
        let rc = RecordContext { topic: "in".into(), partition: 0, offset: 0, timestamp: 0 };
        let mut proc = KStreamSessionReduceProcessor {
            store_name: "s".into(),
            gap_ms: 60,
            reducer: |a: &String, b: &String| format!("{a}{b}"),
            _pd: PhantomData::<fn() -> (String, String)>,
        };
        // "x"@0 seeds [0,0]="x" (one update). "y"@30 merges → tombstone [0,0] then
        // update [0,30]="xy". Drain in order and check the final merged update.
        for (v, ts) in [("x", 0i64), ("y", 30)] {
            let mut d = Dispatch { buffer: &mut buffer, children: &children, output: &mut output, record_ctx: &rc, stores: &mut stores };
            let mut ctx = ProcessorContext::<'_, '_, Windowed<String>, Change<String>>::new(&mut d);
            proc.process(&mut ctx, Record::new(Some("a".into()), v.into(), ts)).await;
        }
        // buffer = [update[0,0]="x", tombstone[0,0], update[0,30]="xy"].
        assert_eq!(buffer.len(), 3);
        let (_, last) = buffer.pop_back().unwrap();
        assert_eq!(last.key.unwrap().downcast::<Windowed<String>>().unwrap().window, Window { start: 0, end: 30 });
        assert_eq!(last.value.downcast::<Change<String>>().unwrap().new, Some("xy".to_string()));
    }
}
```

- [ ] **Step 3: Run tests.** Run: `cargo test -p crabka-client-streams session_aggregate::tests`
Expected: `merge_within_gap_tombstones_then_updates`, `beyond_gap_two_independent_sessions` PASS.

- [ ] **Step 4: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/processors/session_aggregate.rs crates/client-streams/src/dsl/processors/mod.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): session aggregate + reduce processors (JVM merge, emit-on-update)"
```

---

## Task 5: `SessionWindowedKGroupedStream` + `windowed_by_session`

**Files:**
- Create: `crates/client-streams/src/dsl/session_windowed_kgrouped.rs`
- Modify: `crates/client-streams/src/dsl/mod.rs`, `src/dsl/kgrouped.rs`

This mirrors `src/dsl/windowed_kgrouped.rs` (read it as the template), swapping `TimeWindows`→`SessionWindows`, the window processors→session processors, `add_window_store`→`add_session_store`, and `aggregate` gains a `merger`.

- [ ] **Step 1: Register the module.** In `src/dsl/mod.rs` add (next to `pub mod windowed_kgrouped;`):

```rust
pub mod session_windowed_kgrouped;
```

- [ ] **Step 2: Create `src/dsl/session_windowed_kgrouped.rs`:**

```rust
//! `SessionWindowedKGroupedStream<K,V>`: the handle between
//! `KGroupedStream::windowed_by_session(SessionWindows)` and a terminal session
//! aggregation (`count`/`reduce`/`aggregate`). The session analogue of
//! [`crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream`]: same grouped
//! lineage + the [`SessionWindows`] spec; terminal ops emit `Windowed<K>` keys and
//! materialize a **session store** (`add_session_store`). Result is a
//! `KTable<Windowed<K>, _>` (always logged in this slice).
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::Materialized;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::{KGroupedStream, RepartitionLowerFn, mint_store_name};
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::session_aggregate::{
    KStreamSessionAggregateProcessor, KStreamSessionReduceProcessor,
};
use crate::dsl::windows::{SessionWindows, Windowed};
use crate::processor::serde::Serde;
use crate::topology::NodeHandle;

/// Handle produced by [`KGroupedStream::windowed_by_session`].
///
/// [`KGroupedStream::windowed_by_session`]: crate::dsl::kgrouped::KGroupedStream::windowed_by_session
pub struct SessionWindowedKGroupedStream<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    parent: NodeId,
    key_changing_upstream: bool,
    #[allow(dead_code)]
    grouped_name: Option<String>,
    repartition_lower: Option<RepartitionLowerFn>,
    windows: SessionWindows,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> SessionWindowedKGroupedStream<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        parent: NodeId,
        key_changing_upstream: bool,
        grouped_name: Option<String>,
        repartition_lower: Option<RepartitionLowerFn>,
        windows: SessionWindows,
    ) -> Self {
        Self {
            builder,
            parent,
            key_changing_upstream,
            grouped_name,
            repartition_lower,
            windows,
            _pd: PhantomData,
        }
    }

    /// `count`: count records per session → `KTable<Windowed<K>, i64>`.
    pub fn count<KS, VS>(self, materialized: Materialized<KS, VS>) -> KTable<Windowed<K>, i64>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        // JVM `count` burns an extra store-name counter index when unnamed.
        if materialized.store_name.is_none() {
            self.builder.borrow_mut().new_processor_name(names::AGGREGATE_STORE);
        }
        self.lower_aggregate::<KS, VS, i64, _, _, _>(
            materialized,
            store_name,
            || 0i64,
            |_k: &K, _v: &V, acc: i64| acc + 1,
            |_k: &K, a: i64, b: i64| a + b,
        )
    }

    /// `aggregate`: general session aggregation with `init` + `agg` + the session
    /// `merger` (combines two session aggregates on merge).
    pub fn aggregate<KS, VS, VA, I, A, M>(
        self,
        init: I,
        agg: A,
        merger: M,
        materialized: Materialized<KS, VS>,
    ) -> KTable<Windowed<K>, VA>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
        M: Fn(&K, VA, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        self.lower_aggregate::<KS, VS, VA, I, A, M>(materialized, store_name, init, agg, merger)
    }

    /// `reduce`: combine values per session with `reducer` → `KTable<Windowed<K>, V>`.
    pub fn reduce<KS, VS, R>(
        self,
        reducer: R,
        materialized: Materialized<KS, VS>,
    ) -> KTable<Windowed<K>, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, names::REDUCE_STORE);
        self.lower_reduce::<KS, VS, R>(materialized, store_name, reducer)
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    fn lower_aggregate<KS, VS, VA, I, A, M>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        agg: A,
        merger: M,
    ) -> KTable<Windowed<K>, VA>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
        M: Fn(&K, VA, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let Materialized { key_serde, value_serde, .. } = materialized;
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let windows = self.windows;
        let mut g = self.builder.borrow_mut();
        let agg_parent = KGroupedStream::<K, V>::record_repartition(
            &mut g, &store_name, parent, key_changing, rp_lower,
        );

        let agg_name = g.new_processor_name(names::AGGREGATE);
        let agg_id = g.graph.add(
            agg_name.clone(),
            GraphNodeKind::Aggregate { store_name: store_name.clone(), changelog: true },
            vec![agg_parent],
        );
        let store_for_thunk = store_name.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&agg_parent].clone());
            let store_for_proc = store_for_thunk.clone();
            let init = init.clone();
            let agg = agg.clone();
            let merger = merger.clone();
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<VA>, _, _, _>(
                    agg_name.clone(),
                    move || KStreamSessionAggregateProcessor {
                        store_name: store_for_proc.clone(),
                        gap_ms: windows.gap_ms,
                        init: init.clone(),
                        agg: agg.clone(),
                        merger: merger.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_session_store::<K, VA, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                windows.gap_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), agg_id, Some(store_name), None)
    }

    #[allow(clippy::too_many_lines)]
    fn lower_reduce<KS, VS, R>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        reducer: R,
    ) -> KTable<Windowed<K>, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let Materialized { key_serde, value_serde, .. } = materialized;
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let windows = self.windows;
        let mut g = self.builder.borrow_mut();
        let agg_parent = KGroupedStream::<K, V>::record_repartition(
            &mut g, &store_name, parent, key_changing, rp_lower,
        );

        let red_name = g.new_processor_name(names::REDUCE);
        let red_id = g.graph.add(
            red_name.clone(),
            GraphNodeKind::Aggregate { store_name: store_name.clone(), changelog: true },
            vec![agg_parent],
        );
        let store_for_thunk = store_name.clone();
        g.graph.nodes[red_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&agg_parent].clone());
            let store_for_proc = store_for_thunk.clone();
            let reducer = reducer.clone();
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<V>, _, _, _>(
                    red_name.clone(),
                    move || KStreamSessionReduceProcessor {
                        store_name: store_for_proc.clone(),
                        gap_ms: windows.gap_ms,
                        reducer: reducer.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_session_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                windows.gap_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            state.handle_name.insert(red_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), red_id, Some(store_name), None)
    }
}
```

- [ ] **Step 3: Add `windowed_by_session`** to `src/dsl/kgrouped.rs` — directly after the existing `windowed_by` method (around line 184):

```rust
    /// `windowedBy(SessionWindows)`: switch to a session aggregation. Moves the
    /// grouped lineage into a [`SessionWindowedKGroupedStream`], which exposes
    /// session `count`/`reduce`/`aggregate` producing `KTable<Windowed<K>, _>`.
    /// (Distinct method name because Rust cannot overload `windowed_by` by the
    /// window-spec argument type as the JVM does.)
    #[must_use]
    pub fn windowed_by_session(
        mut self,
        windows: crate::dsl::windows::SessionWindows,
    ) -> crate::dsl::session_windowed_kgrouped::SessionWindowedKGroupedStream<K, V> {
        crate::dsl::session_windowed_kgrouped::SessionWindowedKGroupedStream::new(
            Rc::clone(&self.builder),
            self.parent,
            self.key_changing_upstream,
            self.grouped_name.take(),
            self.repartition_lower.take(),
            windows,
        )
    }
```

- [ ] **Step 4: Build + run.** Run: `cargo build -p crabka-client-streams` then `cargo test -p crabka-client-streams --lib`
Expected: compiles; existing lib tests still PASS. (No new test here — the DSL is exercised by the execution + golden tests in Tasks 6/7.)

- [ ] **Step 5: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/dsl/session_windowed_kgrouped.rs crates/client-streams/src/dsl/mod.rs crates/client-streams/src/dsl/kgrouped.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-dsl): SessionWindowedKGroupedStream + windowed_by_session"
```

---

## Task 6: Session execution tests

**Files:**
- Modify: `crates/client-streams/tests/dsl_execution.rs`

Use the existing windowed execution tests in this file as the harness template (find a `windowed`/`TopologyTestDriver`-based test and mirror its setup: `StreamsBuilder`, `build`/`build_optimized`, a `TopologyTestDriver`, `pipe_input`, `read_output`). Read the top of `dsl_execution.rs` for the exact helper names before writing.

- [ ] **Step 1: Add the session execution tests.** Append to `tests/dsl_execution.rs` (adapt the input/output helper calls to the real ones in this file — e.g. the windowed count test's `TopologyTestDriver` usage; the `Windowed<String>` output key carries `window.start`/`window.end`):

```rust
#[test]
fn dsl_session_count_merges_within_gap() {
    use crabka_client_streams::{Consumed, Grouped, I64Serde, Materialized, StringSerde};
    use crabka_client_streams::{SessionWindows, Windowed};
    // groupByKey + session(gap 60).count(). Two records for "a" at t=0 and t=30 are
    // within the gap → one merged session [0,30] count 2 (plus a tombstone for the
    // intermediate [0,0]). A record at t=200 starts a new session [200,200] count 1.
    let b = crabka_client_streams::StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by_session(SessionWindows::of_inactivity_gap(60))
        .count(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to("out", crabka_client_streams::Produced::with(
            crabka_client_streams::SessionWindowedSerde::new(StringSerde),
            I64Serde,
        ));
    let topo = b.build("app").unwrap();
    let mut driver = crabka_client_streams::TopologyTestDriver::new(&topo);
    // Pipe records (adapt to the real pipe API in this test file).
    driver.pipe_input("in", "a", "x", 0);
    driver.pipe_input("in", "a", "x", 30);
    driver.pipe_input("in", "a", "x", 200);
    let out: Vec<(Windowed<String>, Option<i64>)> = driver.read_output_windowed_session("out");
    // The final state for "a": session [0,30] → 2, session [200,200] → 1.
    // Intermediate tombstone for [0,0] is in the stream but toStream drops tombstones.
    assert!(out.iter().any(|(w, v)| w.window == crabka_client_streams::Window { start: 0, end: 30 } && *v == Some(2)));
    assert!(out.iter().any(|(w, v)| w.window == crabka_client_streams::Window { start: 200, end: 200 } && *v == Some(1)));
}

#[test]
fn dsl_session_count_separate_beyond_gap() {
    use crabka_client_streams::{Consumed, Grouped, I64Serde, Materialized, StringSerde};
    use crabka_client_streams::{SessionWindows, Window, Windowed};
    let b = crabka_client_streams::StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by_session(SessionWindows::of_inactivity_gap(60))
        .count(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to("out", crabka_client_streams::Produced::with(
            crabka_client_streams::SessionWindowedSerde::new(StringSerde),
            I64Serde,
        ));
    let topo = b.build("app").unwrap();
    let mut driver = crabka_client_streams::TopologyTestDriver::new(&topo);
    driver.pipe_input("in", "a", "x", 0);
    driver.pipe_input("in", "a", "x", 500);
    let out: Vec<(Windowed<String>, Option<i64>)> = driver.read_output_windowed_session("out");
    assert!(out.iter().any(|(w, v)| w.window == Window { start: 0, end: 0 } && *v == Some(1)));
    assert!(out.iter().any(|(w, v)| w.window == Window { start: 500, end: 500 } && *v == Some(1)));
}
```

> **Implementer note:** `dsl_execution.rs` already has helpers for piping input and reading windowed output for the time-window tests. **Reuse them** — do not invent `pipe_input`/`read_output_windowed_session` if differently named. If a session-output reader doesn't exist, add a small helper mirroring the time-windowed one but deserializing with `SessionWindowedSerde`. The assertions (merged `[0,30]→2`; separate `[0,0]→1`,`[500,500]→1`) are the contract; adapt the plumbing to this file's actual API.

- [ ] **Step 2: Run tests.** Run: `cargo test -p crabka-client-streams --test dsl_execution session`
Expected: `dsl_session_count_merges_within_gap`, `dsl_session_count_separate_beyond_gap` PASS.

- [ ] **Step 3: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/tests/dsl_execution.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-dsl): session window execution tests (merge-within-gap, separate-beyond-gap)"
```

---

## Task 7: `session_count` golden capture (Phase C — controller runs Docker)

**Files:**
- Modify: `crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/Capture.java`, `tests/jvm-capture/run.sh`
- Create: `crates/client-streams/tests/testdata/golden/dsl/session_count.topology.json` (written by the Docker capture)
- Modify: `crates/client-streams/tests/dsl_golden_frame.rs`

> **This task runs the Docker JVM capture — the controller (not a subagent) executes the `run.sh` step**, since it requires Docker on the host.

- [ ] **Step 1: Add the Java fixture #12.** In `Capture.java`, register it in `main` (after `write(outDir, "stream_stream_outer_join", streamStreamOuterJoin());`):

```java
        write(outDir, "session_count", sessionCount());
```

and bump the completion message to `12 fixtures`. Add the method (next to `windowedCount()`):

```java
    /**
     * 12. session_count: stream -> groupByKey -> windowedBy(SessionWindows gap 60s)
     * -> count -> toStream -> to. Session store; changelog cleanup.policy=compact,delete
     * + retention.ms = gap + grace + 1day. Pins the session store name + changelog.
     */
    static Topology sessionCount() {
        StreamsBuilder b = new StreamsBuilder();
        b.<String, String>stream("in")
            .groupByKey()
            .windowedBy(org.apache.kafka.streams.kstream.SessionWindows.ofInactivityGapWithNoGrace(
                java.time.Duration.ofSeconds(60)))
            .count()
            .toStream()
            .to("out");
        return b.build(optimizedProps());
    }
```

Update the `run.sh` header comment + the `--gradle`/`--javac` fixture-count comments from 11 to 12 (add `session_count` to the list).

- [ ] **Step 2: Run the Docker capture (CONTROLLER).** Run: `cd crates/client-streams/tests/jvm-capture && ./run.sh --gradle`
Expected: writes `../testdata/golden/dsl/session_count.topology.json` and rewrites the other 11 (which must stay byte-identical — verify with `git status --short` showing only the new file untracked).

- [ ] **Step 3: Inspect the capture + reconcile the store name/changelog.** Read `session_count.topology.json`. Confirm: one subtopology, `source_topics: ["in"]`, one `state_changelog_topics` entry. Note its **name** (expected `app-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog`) and **configs** (expected `compact,delete` + `message.timestamp.type=CreateTime` + `retention.ms=86460000`). If the store name prefix or the changelog config differs from the windowed assumption, tune the DSL: the store-name minting lives in `session_windowed_kgrouped.rs::count` (the `AGGREGATE_STORE` prefix + the name-burn) and the changelog config in `add_session_store` (`builder.rs`). If the config differs from `compact,delete`+retention, add a `ChangelogKind::Session` variant in `node.rs`/`wire.rs` and switch `add_session_store` to it. Also confirm whether the changelog **value** is raw vs `ValueAndTimestamp` — if the JVM session store wraps, switch `SessionBytesStore` to `wrap_value`/`unwrap_value` (from `window_schema.rs`).

- [ ] **Step 4: Add the golden frame test.** In `tests/dsl_golden_frame.rs` (mirror `windowed_count_matches_jvm`):

```rust
#[test]
fn session_count_matches_jvm() {
    use crabka_client_streams::{Grouped, I64Serde, Materialized, SessionWindowedSerde, SessionWindows};
    // Mirrors Capture.java `sessionCount()`:
    //   stream("in").groupByKey().windowedBy(SessionWindows gap 60s).count().toStream().to("out")
    // Session store → KSTREAM-AGGREGATE-STATE-STORE-0000000001 (with the count
    // name-burn), changelog compact,delete + retention 86_460_000 (gap 60s + 0 grace
    // + 1 day). No selectKey → no repartition.
    let b = StreamsBuilder::new();
    b.stream(["in"], Consumed::with(StringSerde, StringSerde))
        .group_by_key(Grouped::with(StringSerde, StringSerde))
        .windowed_by_session(SessionWindows::of_inactivity_gap(60_000))
        .count(Materialized::with(StringSerde, I64Serde))
        .to_stream()
        .to("out", Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "session_count");
}
```

- [ ] **Step 5: Run goldens.** Run: `cargo test -p crabka-client-streams --test dsl_golden_frame`
Expected: `session_count_matches_jvm` PASS **and all 11 prior goldens PASS** (`12 passed`). If `session_count` fails, diff the actual vs fixture and tune per Step 3 until byte-identical.

- [ ] **Step 6: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/tests/jvm-capture/src/main/java/crabka/capture/Capture.java crates/client-streams/tests/jvm-capture/run.sh crates/client-streams/tests/testdata/golden/dsl/session_count.topology.json crates/client-streams/tests/dsl_golden_frame.rs
# plus any tuning to session_windowed_kgrouped.rs / builder.rs / node.rs / wire.rs / session.rs from Step 3
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-dsl): session_count golden (#12) captured from JVM 4.1"
```

---

## Task 8: Docs + final verification

**Files:**
- Modify: `crates/client-streams/src/lib.rs` (crate-doc prose)

- [ ] **Step 1: Add a session-window paragraph** to the `lib.rs` crate docs, after the windowed-aggregation paragraph (the one ending "…the key carries the window start.") and before the stream-stream-join paragraph:

```rust
//! [`KGroupedStream::windowed_by_session`] groups records into data-driven
//! **session windows**: records for a key form one session `[start, end]` while
//! they stay within an inactivity [`SessionWindows`] gap. Terminal `count` /
//! `reduce` / `aggregate` (the latter taking a session merger) yield a
//! [`KTable`]`<`[`Windowed`]`<K>, V>`. Each record merges every session within the
//! gap into one `[minStart, maxEnd]` session — emitting a tombstone for each
//! merged-away session and the new merged session (KIP session semantics,
//! emit-on-update). The session store keys by `key‖end‖start` (a third typed store
//! over the pluggable backend); read the output with [`SessionWindowedSerde`].
```

(Adjust the intra-doc links to ones that resolve — `SessionWindows` / `SessionWindowedSerde` / `Windowed` / `KTable` are all re-exported.)

- [ ] **Step 2: Final verification (full slice).** Run, in order:

```
cargo test -p crabka-client-streams
cargo test -p crabka-client-streams --doc
cargo clippy -p crabka-client-streams --all-targets -- -D warnings
cargo fmt -p crabka-client-streams --check
```

Expected: all green; `dsl_golden_frame` shows `12 passed` (11 prior byte-identical + `session_count`); `dsl_execution` includes the two session tests; no clippy warnings; fmt clean.

- [ ] **Step 3: Commit.**

```bash
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl add crates/client-streams/src/lib.rs
git -C /Users/mattstone/git/crabka/.claude/worktrees/streams-4-dsl -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs(streams-dsl): document session windows + final 4d-iv verification"
```

---

## Done criteria

- `SessionWindows` + `windowed_by_session` + session `count`/`reduce`/`aggregate` produce `KTable<Windowed<K>, _>`.
- JVM session merge: each record merges in-gap sessions, tombstones merged-away sessions, emits the merged session (execution tests prove merge-within-gap and separate-beyond-gap).
- `session_count` golden byte-matches JVM 4.1; **11 prior goldens byte-identical**.
- `SessionBytesStore` is the third typed store over `ByteKeyValueStore`; `SessionKeySchema` = `key‖end‖start`; `SessionWindowedSerde` output = `key‖end‖start`.
- Full suite + doctests + clippy `--all-targets` + fmt all green.
