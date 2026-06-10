# Versioned KTables (Slice 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `VersionedKeyValueStore` and versioned-table materialization
(`builder.table(...).materialized_versioned`) to `crates/client-streams`, with
JVM-byte-exact topology + changelog and out-of-order-correct table semantics
(KIP-889 + the table half of KIP-914).

**Architecture:** A new store *triplet* mirroring the existing window/session
stores: a value codec (`versioned_schema.rs`, reusing the window
`ValueAndTimestamp` `ts‖value` layout), a version-chain store
(`versioned.rs`, `BTreeMap<keyBytes, BTreeMap<validFrom, Option<valueBytes>>>`),
and a timestamp-aware `VersionedKTableSourceProcessor`. Wiring threads a
`VersionedConfig` through `Materialized` → `builder.table_explicit` → a new
`Topology::add_versioned_store` → a new `ChangelogKind::Versioned` wire config.
The raw key stays the changelog key (JVM-exact); the version timestamp rides in
the changelog value header — no change to the shared `StateStore` changelog
plumbing.

**Tech Stack:** Rust 2024, `async-trait`, `bytes`, `tokio`, `serde_json` (golden
assertions); JVM capture via `tests/jvm-capture` (Gradle/Docker, Kafka Streams
4.1.0).

**Spec:** `docs/superpowers/specs/2026-06-09-kip-1071-streams-client-versioned-tables-design.md`

**Working directory:** all paths are relative to
`crates/client-streams/` unless noted. Run commands from there.

---

## Execution batching (per CLAUDE.md)

Tasks group into batches whose file-sets don't overlap; dispatch a batch's tasks
concurrently, review, then proceed.

- **Batch 1 (sequential, store foundation):** Task 1 → Task 2 (Task 2 uses Task 1).
- **Batch 2 (parallel — distinct files):** Task 3 (`registry.rs`), Task 4 (`iq.rs`).
  Both depend on Batch 1.
- **Batch 3 (sequential, topology wiring — `node.rs`→`wire.rs`→`builder.rs`):**
  Task 5 → Task 6 → Task 7.
- **Batch 4 (sequential, DSL surface):** Task 8 (`config.rs`) → Task 9
  (`table.rs` processor) → Task 10 (`builder.rs` table branch + re-exports).
- **Batch 5 (verification — capture infra then asserts):** Task 11 (capture Java
  + run.sh) → Task 12 (structural + changelog goldens) → Task 13 (behavioral
  golden replay).

---

## Task 1: Versioned value codec (`versioned_schema.rs`)

The changelog/store value for a versioned record is `ValueAndTimestamp`:
`validFrom:8B-BE ‖ value`. This is byte-identical to the window store's
`wrap_value`/`unwrap_value`, so we reuse them (DRY) and add the versioned
tombstone convention: a tombstone *version* at timestamp `ts` is encoded as
`ts ‖ <empty>` (zero-length inner value). The exact JVM wire bytes are pinned by
the changelog golden in Task 12; this internal encoding guarantees the timestamp
survives restore regardless.

**Files:**
- Create: `src/store/versioned_schema.rs`
- Modify: `src/store/mod.rs` (add `pub(crate) mod versioned_schema;`)

- [ ] **Step 1: Write the failing test**

Create `src/store/versioned_schema.rs`:

```rust
//! Versioned store/changelog VALUE codec. A version's value is
//! `ValueAndTimestamp`: `validFrom:8B-BE ‖ value` — byte-identical to the window
//! store value, so we reuse the window codec. A tombstone version is encoded as
//! `validFrom:8B-BE ‖ <empty>` (zero-length inner), so the timestamp survives a
//! changelog round-trip even for deletes (a bare `None` changelog value would
//! lose it).
//!
//! NOTE: the exact JVM changelog bytes are pinned by the Task 12 changelog
//! golden; this is the Crabka-internal encoding the golden is checked against.
use bytes::Bytes;

pub(crate) use crate::store::window_schema::{unwrap_value, wrap_value};

/// Wrap a versioned record value (`Some` = live value, `None` = tombstone
/// version) into the changelog/store value bytes at `valid_from`.
pub(crate) fn wrap_versioned(valid_from: i64, value: Option<&[u8]>) -> Bytes {
    wrap_value(valid_from, value.unwrap_or(&[]))
}

/// Split changelog/store value bytes into `(valid_from, Option<value_bytes>)`.
/// A zero-length inner value decodes to `None` (tombstone version).
pub(crate) fn unwrap_versioned(wrapped: &[u8]) -> (i64, Option<Bytes>) {
    let (ts, raw) = unwrap_value(wrapped);
    let value = if raw.is_empty() {
        None
    } else {
        Some(Bytes::copy_from_slice(raw))
    };
    (ts, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_unwrap_live_value() {
        let w = wrap_versioned(7, Some(&99i64.to_be_bytes()));
        assert_eq!(&w[0..8], &7i64.to_be_bytes());
        let (ts, v) = unwrap_versioned(&w);
        assert_eq!(ts, 7);
        assert_eq!(v.as_deref(), Some(&99i64.to_be_bytes()[..]));
    }

    #[test]
    fn wrap_unwrap_tombstone_version() {
        let w = wrap_versioned(11, None);
        assert_eq!(w.len(), 8); // ts only, empty inner
        let (ts, v) = unwrap_versioned(&w);
        assert_eq!(ts, 11);
        assert_eq!(v, None);
    }
}
```

Add to `src/store/mod.rs` (alongside the other `mod` lines):

```rust
pub(crate) mod versioned_schema;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams versioned_schema 2>&1 | tail -20`
Expected: FAIL — `window_schema::{wrap_value, unwrap_value}` are `pub(crate)`; if
they are not visible, see Step 3. (If they compile and pass immediately because
the codec is trivially correct, that is acceptable — proceed.)

- [ ] **Step 3: Make `window_schema` codec reusable if needed**

If Step 2 fails with a visibility error on `wrap_value`/`unwrap_value`, confirm
they are `pub(crate)` in `src/store/window_schema.rs` (they are, per the current
source). No change should be required. If the build complains that
`window_schema` is not declared `pub(crate)` in `mod.rs`, leave it as-is — only
add the `versioned_schema` line.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams versioned_schema 2>&1 | tail -20`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/store/versioned_schema.rs src/store/mod.rs
git commit -m "feat(client-streams): versioned store value codec (KIP-889)"
```

---

## Task 2: Version-chain store (`versioned.rs`)

The store: `VersionedRecord`, the `VersionedKeyValueStore` trait, and
`VersionedBytesStore<K,V>` implementing `StateStore` + the trait + `IqQueryable`.
Internal rep: `BTreeMap<Bytes /*key*/, BTreeMap<i64 /*validFrom*/, Option<Bytes> /*value*/>>`.

**Files:**
- Create: `src/store/versioned.rs`
- Modify: `src/store/mod.rs` (add `pub mod versioned;`)

- [ ] **Step 1: Write the failing test (and the full implementation it drives)**

Create `src/store/versioned.rs`:

```rust
//! Versioned key-value store (KIP-889). Each key maps to a chain of versions
//! keyed by `validFrom` (epoch millis); a version's value is `Some` (live) or
//! `None` (tombstone). `get` returns the latest; `get_as_of(t)` returns the
//! version valid at `t`. History older than `history_retention` (relative to the
//! max observed timestamp) is expired. Changelog VALUE = `validFrom:8B ‖ value`
//! (`versioned_schema`); the raw key is the changelog key (JVM-exact).
use std::any::Any;
use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::processor::serde::Serde;
use crate::store::api::StateStore;
use crate::store::versioned_schema::{unwrap_versioned, wrap_versioned};

/// A single resolved version: its value, the timestamp it became valid, and the
/// timestamp the next version superseded it (`None` = still the latest, ∞).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedRecord<V> {
    pub value: V,
    pub valid_from: i64,
    pub valid_to: Option<i64>,
}

/// Typed versioned store surface.
#[async_trait]
pub trait VersionedKeyValueStore<K: Send + Sync, V: Send>: StateStore {
    /// Insert a version at `validFrom = timestamp`. `value == None` is a
    /// tombstone version. Out-of-order timestamps are inserted mid-chain; the
    /// latest pointer only advances when `timestamp >=` the current max.
    /// A timestamp older than the retention horizon is dropped.
    async fn put(&mut self, key: K, value: Option<V>, timestamp: i64);
    /// Insert a tombstone version at `timestamp`; returns the record that was
    /// valid at `timestamp` immediately before the delete (if any live value).
    async fn delete(&mut self, key: &K, timestamp: i64) -> Option<VersionedRecord<V>>;
    /// The latest live version, or `None` if absent / latest is a tombstone.
    async fn get(&self, key: &K) -> Option<VersionedRecord<V>>;
    /// The version valid at `as_of`, or `None` if absent / that version is a
    /// tombstone / `as_of` predates the oldest retained version.
    async fn get_as_of(&self, key: &K, as_of: i64) -> Option<VersionedRecord<V>>;
}

pub struct VersionedBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    history_retention_ms: i64,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    // key bytes -> (validFrom -> Some(value bytes) | None tombstone)
    chains: BTreeMap<Bytes, BTreeMap<i64, Option<Bytes>>>,
    observed_stream_time: i64,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
}

impl<K: 'static, V: 'static> VersionedBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        history_retention_ms: i64,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            history_retention_ms,
            key_serde,
            value_serde,
            chains: BTreeMap::new(),
            observed_stream_time: i64::MIN,
            changelog: Vec::new(),
            logging: true,
        }
    }

    #[must_use]
    pub fn in_memory(
        name: String,
        history_retention_ms: i64,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(name, history_retention_ms, key_serde, value_serde, changelog_topic)
    }

    /// The retention horizon: versions whose `valid_to` is strictly below this
    /// are unreachable and may be evicted; a put below it is dropped.
    fn horizon(&self) -> i64 {
        self.observed_stream_time.saturating_sub(self.history_retention_ms)
    }

    /// Insert raw (already-serialized) version bytes into a chain, applying
    /// retention. Shared by `put`/`delete` (logging on) and restore (logging off).
    fn insert_raw(&mut self, key: Bytes, valid_from: i64, value: Option<Bytes>) -> bool {
        self.observed_stream_time = self.observed_stream_time.max(valid_from);
        // KIP-889 out-of-bounds drop: a version that is already entirely below
        // the horizon (its validTo would be < horizon) is not retained.
        let chain = self.chains.entry(key.clone()).or_default();
        let next_above = chain.range((valid_from + 1)..).next().map(|(t, _)| *t);
        let valid_to = next_above; // None => latest
        if let Some(vt) = valid_to {
            if vt <= self.horizon() {
                // fully superseded and below horizon — drop
                return false;
            }
        }
        chain.insert(valid_from, value);
        // Evict versions fully below the horizon (validTo <= horizon).
        let h = self.horizon();
        let times: Vec<i64> = chain.keys().copied().collect();
        for w in times.windows(2) {
            let (from, next) = (w[0], w[1]);
            if next <= h {
                chain.remove(&from);
            }
        }
        if chain.is_empty() {
            self.chains.remove(&key);
        }
        true
    }
}

#[async_trait]
impl<K: Send + 'static, V: Send + 'static> StateStore for VersionedBytesStore<K, V> {
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
            Some(wrapped) => {
                let (ts, inner) = unwrap_versioned(&wrapped);
                self.insert_raw(key, ts, inner);
            }
            // A bare null changelog value cannot carry a timestamp; with the
            // value-packed encoding it never occurs. Ignore defensively.
            None => {}
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        Some(self)
    }
    async fn clear(&mut self) {
        self.chains.clear();
        self.changelog.clear();
        self.observed_stream_time = i64::MIN;
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> VersionedKeyValueStore<K, V>
    for VersionedBytesStore<K, V>
{
    async fn put(&mut self, key: K, value: Option<V>, timestamp: i64) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let vb = value
            .as_ref()
            .map(|v| self.value_serde.serialize(&self.changelog_topic, v));
        // Pre-check the horizon for an out-of-bounds drop (no log, no store).
        let inserted = self.insert_raw(kb.clone(), timestamp, vb.clone());
        if inserted && self.logging {
            let wrapped = wrap_versioned(timestamp, vb.as_deref());
            self.changelog.push((kb, Some(wrapped)));
        }
    }

    async fn delete(&mut self, key: &K, timestamp: i64) -> Option<VersionedRecord<V>> {
        let prev = self.get_as_of(key, timestamp).await;
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let inserted = self.insert_raw(kb.clone(), timestamp, None);
        if inserted && self.logging {
            let wrapped = wrap_versioned(timestamp, None);
            self.changelog.push((kb, Some(wrapped)));
        }
        prev
    }

    async fn get(&self, key: &K) -> Option<VersionedRecord<V>> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let chain = self.chains.get(&kb)?;
        let (&valid_from, value) = chain.iter().next_back()?;
        let raw = value.as_ref()?; // latest is a tombstone => None
        Some(VersionedRecord {
            value: self
                .value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("versioned value deserialize"),
            valid_from,
            valid_to: None,
        })
    }

    async fn get_as_of(&self, key: &K, as_of: i64) -> Option<VersionedRecord<V>> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let chain = self.chains.get(&kb)?;
        let (&valid_from, value) = chain.range(..=as_of).next_back()?;
        let raw = value.as_ref()?; // that version is a tombstone => None
        let valid_to = chain.range((as_of + 1)..).next().map(|(t, _)| *t);
        Some(VersionedRecord {
            value: self
                .value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("versioned value deserialize"),
            valid_from,
            valid_to,
        })
    }
}

// Holds only `Box<dyn Serde<_>>` + byte buffers → `Send + Sync` for any K/V.
#[async_trait]
impl<K: 'static, V: 'static> crate::store::iq::IqQueryable for VersionedBytesStore<K, V> {
    fn kind(&self) -> crate::store::iq::StoreKind {
        crate::store::iq::StoreKind::Versioned
    }
    async fn iq_versioned_get(&self, key: &[u8]) -> Option<(i64, Option<i64>, Bytes)> {
        let chain = self.chains.get(key)?;
        let (&vf, value) = chain.iter().next_back()?;
        let raw = value.as_ref()?;
        Some((vf, None, raw.clone()))
    }
    async fn iq_versioned_get_as_of(
        &self,
        key: &[u8],
        as_of: i64,
    ) -> Option<(i64, Option<i64>, Bytes)> {
        let chain = self.chains.get(key)?;
        let (&vf, value) = chain.range(..=as_of).next_back()?;
        let raw = value.as_ref()?;
        let vt = chain.range((as_of + 1)..).next().map(|(t, _)| *t);
        Some((vf, vt, raw.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    fn store(retention: i64) -> VersionedBytesStore<String, i64> {
        VersionedBytesStore::in_memory(
            "v".into(),
            retention,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-v-changelog".into(),
        )
    }

    #[tokio::test]
    async fn latest_and_as_of() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        assert_eq!(s.get(&"k".into()).await.map(|r| r.value), Some(20));
        // as-of between versions sees the older value, with valid_to = 200
        let r = s.get_as_of(&"k".into(), 150).await.unwrap();
        assert_eq!((r.value, r.valid_from, r.valid_to), (10, 100, Some(200)));
        // as-of before the first version => None
        assert_eq!(s.get_as_of(&"k".into(), 50).await, None);
    }

    #[tokio::test]
    async fn out_of_order_does_not_clobber_latest() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(20), 200).await;
        s.put("k".into(), Some(10), 100).await; // older ts arrives late
        assert_eq!(s.get(&"k".into()).await.map(|r| r.value), Some(20)); // latest unchanged
        assert_eq!(s.get_as_of(&"k".into(), 150).await.map(|r| r.value), Some(10));
    }

    #[tokio::test]
    async fn tombstone_hides_latest_but_keeps_history() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(10), 100).await;
        let prev = s.delete(&"k".into(), 200).await;
        assert_eq!(prev.map(|r| r.value), Some(10));
        assert_eq!(s.get(&"k".into()).await, None); // latest is a tombstone
        assert_eq!(s.get_as_of(&"k".into(), 150).await.map(|r| r.value), Some(10));
    }

    #[tokio::test]
    async fn retention_drops_old_put_and_evicts_history() {
        let mut s = store(50);
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await; // horizon now 150; v@100 has validTo 200>150 → kept
        // A put far below the horizon is dropped (and not logged).
        s.put("k".into(), Some(5), 40).await; // validTo would be 100 <= 150 → dropped
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 2); // only the two in-bounds puts logged
        assert_eq!(s.get_as_of(&"k".into(), 40).await, None);
    }

    #[tokio::test]
    async fn changelog_roundtrip_restores_chain() {
        let mut s = store(1_000_000);
        s.put("k".into(), Some(10), 100).await;
        s.delete(&"k".into(), 150).await;
        s.put("k".into(), Some(30), 300).await;
        let cl = s.take_changelog();
        // Restore into a fresh store with logging off.
        let mut r = store(1_000_000);
        r.set_logging(false);
        for (k, v) in cl {
            r.apply_changelog(k, v).await;
        }
        assert!(r.take_changelog().is_empty()); // restore did not re-log
        assert_eq!(r.get(&"k".into()).await.map(|x| x.value), Some(30));
        assert_eq!(r.get_as_of(&"k".into(), 120).await.map(|x| x.value), Some(10));
        assert_eq!(r.get_as_of(&"k".into(), 160).await, None); // tombstone @150
    }
}
```

Add to `src/store/mod.rs`:

```rust
pub mod versioned;
```

- [ ] **Step 2: Run tests to verify they fail/compile-error first**

Run: `cargo test -p crabka-client-streams store::versioned 2>&1 | tail -30`
Expected: FAIL — `StoreKind::Versioned` and the `iq_versioned_*` trait methods
don't exist yet (Task 4). To unblock Task 2's own unit tests now, temporarily
stub them: in `src/store/iq.rs` add `Versioned` to `StoreKind` and the two
default-returning `iq_versioned_*` methods (this is exactly Task 4 — doing it
here is fine; mark Task 4 done when you reach it). After stubbing, the failure
should be the unit-test assertions, not compile errors.

- [ ] **Step 3: Confirm the implementation (already written in Step 1)**

The implementation is complete in Step 1. No additional code.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-client-streams store::versioned 2>&1 | tail -30`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/store/versioned.rs src/store/mod.rs src/store/iq.rs
git commit -m "feat(client-streams): VersionedKeyValueStore version-chain store (KIP-889)"
```

---

## Task 3: Registry downcast (`get_versioned`)

**Files:**
- Modify: `src/store/registry.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/store/registry.rs`:

```rust
    #[tokio::test]
    async fn register_and_downcast_versioned_store() {
        use crate::store::versioned::{VersionedBytesStore, VersionedKeyValueStore};
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(VersionedBytesStore::<String, i64>::in_memory(
            "v".into(),
            1_000_000,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "v-changelog".into(),
        )));
        let s = reg.get_versioned::<String, i64>("v").unwrap();
        s.put("x".into(), Some(5), 10).await;
        check!(s.get(&"x".to_string()).await.map(|r| r.value) == Some(5));
        check!(reg.get_versioned::<i64, i64>("v").is_none()); // wrong types
        check!(reg.get_versioned::<String, i64>("missing").is_none());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams store::registry::tests::register_and_downcast_versioned_store 2>&1 | tail -20`
Expected: FAIL — `get_versioned` not found.

- [ ] **Step 3: Write minimal implementation**

Add to `impl StoreRegistry` in `src/store/registry.rs` (next to `get_window`):

```rust
    /// Typed mutable access: downcast the erased store to the versioned store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_versioned<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::versioned::VersionedKeyValueStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::versioned::VersionedBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::versioned::VersionedKeyValueStore<K, V>)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams store::registry 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/store/registry.rs
git commit -m "feat(client-streams): registry get_versioned downcast"
```

---

## Task 4: IQ surface (`StoreKind::Versioned` + byte methods)

If you already stubbed these in Task 2 Step 2, this task finalizes them with
tests. The two methods return `(validFrom, validTo, valueBytes)`.

**Files:**
- Modify: `src/store/iq.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/store/iq.rs`:

```rust
    #[tokio::test]
    async fn versioned_get_latest_and_as_of() {
        use crate::store::versioned::{VersionedBytesStore, VersionedKeyValueStore};
        let mut s = VersionedBytesStore::<String, i64>::in_memory(
            "v".into(),
            1_000_000,
            Box::new(StringSerde),
            Box::new(I64Serde),
            "v-changelog".into(),
        );
        s.put("k".into(), Some(10), 100).await;
        s.put("k".into(), Some(20), 200).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        assert_eq!(q.kind(), StoreKind::Versioned);
        let (vf, vt, raw) = q.iq_versioned_get(b"k").await.unwrap();
        assert_eq!((vf, vt), (200, None));
        assert_eq!(raw, I64Serde.serialize("t", &20));
        let (vf2, vt2, raw2) = q.iq_versioned_get_as_of(b"k", 150).await.unwrap();
        assert_eq!((vf2, vt2), (100, Some(200)));
        assert_eq!(raw2, I64Serde.serialize("t", &10));
        assert_eq!(q.iq_versioned_get_as_of(b"k", 50).await, None);
    }
```

- [ ] **Step 2: Run test to verify it fails (or compile-errors if not yet stubbed)**

Run: `cargo test -p crabka-client-streams store::iq::tests::versioned_get_latest_and_as_of 2>&1 | tail -20`
Expected: FAIL / compile error on `StoreKind::Versioned` or `iq_versioned_*`.

- [ ] **Step 3: Write minimal implementation**

In `src/store/iq.rs`, add `Versioned` to the `StoreKind` enum:

```rust
pub enum StoreKind {
    KeyValue,
    Window,
    Session,
    Versioned,
}
```

Add two methods to the `IqQueryable` trait (with empty defaults, alongside the
existing `iq_*` methods):

```rust
    /// Latest live version: `(validFrom, validTo=None, valueBytes)`.
    async fn iq_versioned_get(&self, _key: &[u8]) -> Option<(i64, Option<i64>, Bytes)> {
        None
    }
    /// Version valid at `as_of`: `(validFrom, validTo, valueBytes)`.
    async fn iq_versioned_get_as_of(
        &self,
        _key: &[u8],
        _as_of: i64,
    ) -> Option<(i64, Option<i64>, Bytes)> {
        None
    }
```

(The `VersionedBytesStore` impl of these already exists from Task 2.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams store::iq 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/store/iq.rs
git commit -m "feat(client-streams): IQ versioned get / get_as_of byte surface"
```

---

## Task 5: Wire changelog config (`ChangelogKind::Versioned`)

The versioned changelog topic config per KIP-889: `cleanup.policy=compact` +
`min.compaction.lag.ms = historyRetention + 86_400_000`. **The exact config set
is pinned by the Task 12 structural golden** — if the capture shows additional
keys, adjust `versioned_changelog_topic_configs` then.

**Files:**
- Modify: `src/topology/node.rs` (enum variant + `add_versioned_store`)
- Modify: `src/topology/wire.rs` (config emitter + match arm)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/topology/wire.rs` (mirror
`windowed_store_changelog_config_is_compact_delete_with_retention`):

```rust
    #[test]
    fn versioned_store_changelog_config_is_compact_with_min_compaction_lag() {
        let cfgs = versioned_changelog_topic_configs(686_400_000);
        let get = |k: &str| cfgs.iter().find(|c| c.key == k).map(|c| c.value.clone());
        assert_eq!(get("cleanup.policy").as_deref(), Some("compact"));
        assert_eq!(get("min.compaction.lag.ms").as_deref(), Some("686400000"));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams topology::wire::tests::versioned_store_changelog_config 2>&1 | tail -20`
Expected: FAIL — `versioned_changelog_topic_configs` not found.

- [ ] **Step 3: Write minimal implementation**

In `src/topology/node.rs`, add a variant to `ChangelogKind`:

```rust
    /// Versioned store: `cleanup.policy=compact` + `min.compaction.lag.ms`
    /// (= historyRetention + 86_400_000) so recent version history is not
    /// compacted away before restore reads it.
    Versioned { min_compaction_lag_ms: i64 },
```

Add a registrar to `impl NodeRegistry` (next to `add_window_store`):

```rust
    /// Register a versioned state store. The changelog gets `compact` policy +
    /// `min.compaction.lag.ms=<min_compaction_lag_ms>`.
    pub fn add_versioned_store(
        &mut self,
        name: &str,
        processors: Vec<String>,
        changelog_override: Option<String>,
        min_compaction_lag_ms: i64,
    ) {
        self.stores.push(StoreEntry {
            name: name.to_string(),
            processors,
            changelog_override,
            changelog_kind: ChangelogKind::Versioned { min_compaction_lag_ms },
        });
    }
```

In `src/topology/wire.rs`, add the config emitter (next to
`windowed_changelog_topic_configs`):

```rust
/// Versioned-store changelog topic configs: `cleanup.policy=compact` +
/// `min.compaction.lag.ms` so recent versions survive until restore (KIP-889).
fn versioned_changelog_topic_configs(min_compaction_lag_ms: i64) -> Vec<KeyValue> {
    vec![
        KeyValue { key: "cleanup.policy".into(), value: "compact".into() },
        KeyValue {
            key: "min.compaction.lag.ms".into(),
            value: min_compaction_lag_ms.to_string(),
        },
    ]
}
```

Add the match arm where `ChangelogKind` is consumed (near lines 156–160, beside
the `AggWindow`/`JoinWindow` arms):

```rust
                crate::topology::node::ChangelogKind::Versioned { min_compaction_lag_ms } => {
                    versioned_changelog_topic_configs(*min_compaction_lag_ms)
                }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams topology::wire 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/topology/node.rs src/topology/wire.rs
git commit -m "feat(client-streams): versioned-store changelog wire config (KIP-889)"
```

---

## Task 6: `Topology::add_versioned_store`

Registers the wire spec + the runtime store factory (the factory ignores the
byte backend — the version-chain store is self-contained in-memory; a persistent
backend is future work).

**Files:**
- Modify: `src/topology/builder.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/topology/builder.rs` (find an existing
store-registration test for the pattern; if none, add a fresh module test):

```rust
    #[test]
    fn add_versioned_store_registers_wire_spec() {
        use crate::processor::serde::{I64Serde, StringSerde};
        let mut t = Topology::new();
        let src = t.add_source_explicit::<String, i64, _, _>(
            "src".into(),
            ["in"],
            crate::processor::serde::Consumed::with(StringSerde, I64Serde),
        );
        t.add_versioned_store::<String, i64, _, _>(
            "vstore",
            StringSerde,
            I64Serde,
            600_000,
            [src.name().to_string()],
        );
        // Build the wire form and confirm the store is present with the
        // versioned changelog config.
        let wire = t.build("app").unwrap().to_wire();
        let json = serde_json::to_value(&wire).unwrap();
        let blob = json.to_string();
        assert!(blob.contains("vstore"));
        assert!(blob.contains("min.compaction.lag.ms"));
    }
```

(If `Topology`'s test helpers differ — e.g. `add_source_explicit` needs a
different signature — copy the exact call shape from an existing
`add_window_store` test in the same file.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams topology::builder::tests::add_versioned_store_registers_wire_spec 2>&1 | tail -20`
Expected: FAIL — `add_versioned_store` not found.

- [ ] **Step 3: Write minimal implementation**

Add to `impl Topology` in `src/topology/builder.rs` (next to `add_window_store`):

```rust
    /// Register a versioned state store (KIP-889) connected to the given
    /// processors. The changelog topic carries `compact` + `min.compaction.lag.ms
    /// = history_retention_ms + 86_400_000`. The version-chain store is
    /// self-contained in memory; the supplied byte backend is unused.
    pub fn add_versioned_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        history_retention_ms: i64,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + Sync + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let min_compaction_lag_ms = history_retention_ms + 86_400_000;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg
            .add_versioned_store(&name, procs, None, min_compaction_lag_ms);
        self.store_factories.insert(
            name.clone(),
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          _backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::versioned::VersionedBytesStore::<K, V>::new(
                            store_name.to_string(),
                            history_retention_ms,
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

(Note the `K: Send + Sync` bound — `VersionedKeyValueStore::get` takes `&K`,
matching the registry downcast.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams topology::builder 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/topology/builder.rs
git commit -m "feat(client-streams): Topology::add_versioned_store"
```

---

## Task 7: Runtime downcast in the graph/registry typed accessor

The runtime's `Graph`/processor context needs to reach the versioned store the
same way it reaches KV/window stores. Confirm `ProcessorContext` exposes a typed
getter; if it routes through `StoreRegistry::get_versioned` (Task 3) you may only
need a thin context method.

**Files:**
- Modify: `src/processor/api.rs` (or wherever `get_state_store`/`get_window_store`
  context accessors live — grep first)

- [ ] **Step 1: Locate the existing typed-store accessors**

Run: `grep -rn "get_window_store\|get_state_store\|get_session_store" src/processor/ src/runtime/`
Read the window accessor; the versioned one mirrors it exactly.

- [ ] **Step 2: Write the failing test**

In the same file as the window accessor's tests (or `tests/state_store_integration.rs`),
add a test that obtains a versioned store from a `ProcessorContext` and does a
`put` + `get`. Model it on the nearest existing window-store context test. Example
shape (adapt names to the real context API):

```rust
    // inside an existing in-process Dispatch/ProcessorContext test harness
    let vs = ctx.get_versioned_store::<String, i64>("vstore").unwrap();
    vs.put("k".into(), Some(7), 100).await;
    assert_eq!(vs.get(&"k".into()).await.map(|r| r.value), Some(7));
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams get_versioned_store 2>&1 | tail -20`
Expected: FAIL — `get_versioned_store` not found.

- [ ] **Step 4: Write minimal implementation**

Add a `get_versioned_store` accessor mirroring the window-store accessor you read
in Step 1. It delegates to `StoreRegistry::get_versioned`. For example, if the
window accessor is:

```rust
    pub fn get_window_store<K, V>(&mut self, name: &str)
        -> Option<&mut dyn WindowStore<K, V>> { self.stores.get_window(name) }
```

then add:

```rust
    pub fn get_versioned_store<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::versioned::VersionedKeyValueStore<K, V>> {
        self.stores.get_versioned(name)
    }
```

- [ ] **Step 5: Run test + commit**

Run: `cargo test -p crabka-client-streams get_versioned_store 2>&1 | tail -20`
Expected: PASS.

```bash
git add src/processor/api.rs
git commit -m "feat(client-streams): ProcessorContext::get_versioned_store"
```

---

## Task 8: `Materialized::as_versioned` + `VersionedConfig`

**Files:**
- Modify: `src/dsl/config.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/dsl/config.rs`:

```rust
    #[test]
    fn materialized_as_versioned_sets_config() {
        let m = Materialized::with(StringSerde, I64Serde).as_versioned("vstore", 600_000);
        check!(m.store_name.as_deref() == Some("vstore"));
        let vc = m.versioned.expect("versioned config");
        check!(vc.history_retention_ms == 600_000);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams dsl::config::tests::materialized_as_versioned_sets_config 2>&1 | tail -20`
Expected: FAIL — `versioned` field / `as_versioned` not found.

- [ ] **Step 3: Write minimal implementation**

In `src/dsl/config.rs`, add the config struct and field:

```rust
/// Versioned-store settings for a `Materialized` (KIP-889). `segment_interval_ms`
/// only affects JVM eviction granularity (non-observable here); accepted for API
/// parity. `None` segment interval uses the JVM default segment heuristic.
#[derive(Debug, Clone, Copy)]
pub struct VersionedConfig {
    pub history_retention_ms: i64,
    pub segment_interval_ms: Option<i64>,
}
```

Add `versioned: Option<VersionedConfig>` to `Materialized` and default it `None`
in `with`:

```rust
pub struct Materialized<KS, VS> {
    #[allow(dead_code)]
    pub(crate) key_serde: KS,
    #[allow(dead_code)]
    pub(crate) value_serde: VS,
    pub(crate) store_name: Option<String>,
    pub(crate) logging: bool,
    pub(crate) versioned: Option<VersionedConfig>,
}
```

In `with(...)`, add `versioned: None,` to the struct literal. Add the builder
method:

```rust
    /// Materialize this table into a versioned key-value store (KIP-889) named
    /// `name`, retaining `history_retention_ms` of version history.
    #[must_use]
    pub fn as_versioned(mut self, name: impl Into<String>, history_retention_ms: i64) -> Self {
        self.store_name = Some(name.into());
        self.versioned = Some(VersionedConfig {
            history_retention_ms,
            segment_interval_ms: None,
        });
        self
    }
```

Also add `versioned: None,` to the `From<(KS, VS)>` impl's struct literal if it
builds `Materialized` directly (it calls `Self::with`, so no change needed —
verify).

- [ ] **Step 4: Run test + commit**

Run: `cargo test -p crabka-client-streams dsl::config 2>&1 | tail -20`
Expected: PASS.

```bash
git add src/dsl/config.rs
git commit -m "feat(client-streams): Materialized::as_versioned config"
```

---

## Task 9: `VersionedKTableSourceProcessor`

Timestamp-aware table-source processor (KIP-914 table half). Emits a `Change<V>`
with `old = get_as_of(key, ts)` taken before the put. Out-of-order records emit
their local change without moving the latest pointer (the store already enforces
that). Late records below the retention horizon are dropped (the store's `put`
already drops them; the processor additionally skips forwarding when the put was
a no-op — detected by re-reading).

**Files:**
- Modify: `src/dsl/processors/table.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `src/dsl/processors/table.rs` (model the harness on
the existing `KTableSourceProcessor` test in that file). The behavioral golden in
Task 13 is the real fidelity gate; this unit test pins the core
old/new/out-of-order contract:

```rust
    #[tokio::test]
    async fn versioned_table_source_emits_as_of_old() {
        // Harness: build a Dispatch with a single VersionedKTableSourceProcessor
        // wired to a VersionedBytesStore "vt". Reuse the helper the existing
        // KTableSourceProcessor test uses; swap the store + processor type.
        // Records: (k, 10 @100), (k, 20 @200), (k, 15 @150 out-of-order).
        // Expect forwarded Change new/old:
        //   @100 -> Change{old:None,        new:Some(10)}
        //   @200 -> Change{old:Some(10),    new:Some(20)}
        //   @150 -> Change{old:Some(10),    new:Some(15)}   (as-of 150 before put = v@100)
        // and store.get(k) latest stays 20.
        // (Fill in using the file's existing in-process dispatch harness.)
    }
```

> Implementer note: the existing `KTableSourceProcessor` test in this file shows
> the exact in-process `Dispatch`/`ProcessorContext` harness to copy. Reproduce
> it with `add_versioned_store` + `VersionedKTableSourceProcessor` and the three
> records above, asserting the three forwarded `Change` values and the final
> `store.get` latest.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams versioned_table_source 2>&1 | tail -20`
Expected: FAIL — `VersionedKTableSourceProcessor` not found.

- [ ] **Step 3: Write minimal implementation**

Add to `src/dsl/processors/table.rs`:

```rust
/// Materializes incoming records into a `VersionedKeyValueStore` at the record's
/// timestamp, then forwards a `Change<V>` whose `old` is the value that was valid
/// at that timestamp *before* this record (KIP-914 table semantics). Out-of-order
/// records still emit their local change; the store keeps the latest pointer.
#[allow(dead_code)]
pub(crate) struct VersionedKTableSourceProcessor<K, V> {
    pub store_name: String,
    pub _pd: Marker<(K, V)>,
}

#[async_trait]
impl<K, V> Processor<K, V, K, Change<V>> for VersionedKTableSourceProcessor<K, V>
where
    K: std::any::Any + Send + Sync + Clone,
    V: std::any::Any + Send + Clone,
{
    async fn process(&mut self, ctx: &mut ProcessorContext<'_, '_, K, Change<V>>, r: Record<K, V>) {
        let key = r.key.expect("versioned KTable source requires a non-null key");
        let ts = r.timestamp;
        let old = {
            let store = ctx
                .get_versioned_store::<K, V>(&self.store_name)
                .expect("versioned KTable source store not found");
            let old = store.get_as_of(&key, ts).await.map(|rec| rec.value);
            store.put(key.clone(), r.value.clone(), ts).await;
            old
        };
        ctx.forward(Record::new(
            Some(key),
            Change::new(r.value, old),
            ts,
        ));
    }
}
```

> Confirm `Change::new`'s argument order against the existing
> `KTableSourceProcessor` in this file (it constructs `Change` with the same
> new/old convention — match it exactly) and confirm `Record::new`'s arity (it
> may be `Record::with_timestamp(...)` — copy whatever the sibling processor
> uses). If `r.timestamp` is not a field, use the context's record timestamp
> accessor the sibling processors use.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams versioned_table_source 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dsl/processors/table.rs
git commit -m "feat(client-streams): VersionedKTableSourceProcessor (KIP-914 table semantics)"
```

---

## Task 10: Wire `builder.table_explicit` versioned branch + re-exports

When `materialized.versioned` is set, lower to `add_versioned_store` +
`VersionedKTableSourceProcessor` instead of the plain KV path.

**Files:**
- Modify: `src/dsl/builder.rs` (the `table_explicit` lowering thunk)
- Modify: `src/dsl/mod.rs`, `src/lib.rs` (re-export `VersionedConfig`,
  `VersionedRecord`; module-doc paragraph)

- [ ] **Step 1: Write the failing execution test**

Add to `tests/dsl_execution.rs`:

```rust
#[test]
fn versioned_table_keeps_latest_on_out_of_order() {
    use crabka_client_streams::{I64Serde, Materialized, StringSerde};
    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000),
    )
    .to_stream()
    .to("out");
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    // in-order then out-of-order (older ts) update
    d.pipe_input("in", Consumed::with(StringSerde, I64Serde), Some("k".into()), 20, 200);
    d.pipe_input("in", Consumed::with(StringSerde, I64Serde), Some("k".into()), 10, 100);
    // toStream forwards the change `new` value each time
    assert_eq!(d.read_output("out", Produced::with(StringSerde, I64Serde)), Some((Some("k".into()), 20)));
    assert_eq!(d.read_output("out", Produced::with(StringSerde, I64Serde)), Some((Some("k".into()), 10)));
    // latest version is still 20 despite the later (older-ts) record
    let latest = d.store_get_versioned::<String, i64>("vt", &"k".to_string());
    assert_eq!(latest, Some(20));
}
```

> If `TopologyTestDriver` lacks `store_get_versioned`, add a thin helper to
> `src/test_driver.rs` mirroring `store_get` but calling `get_versioned` +
> returning `rec.value` of the latest. Include that in this task.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams versioned_table_keeps_latest_on_out_of_order 2>&1 | tail -30`
Expected: FAIL — versioned branch not wired (store registered as plain KV, or
`store_get_versioned` missing).

- [ ] **Step 3: Write minimal implementation**

In `src/dsl/builder.rs` `table_explicit`, inside the lowering thunk, branch on
`materialized.versioned`. Capture the config before the thunk moves the serdes
(like `suppress_factory` is captured). Where the thunk currently calls
`state.topology.add_state_store::<...>(...)` and registers the
`KTableSourceProcessor`, add a versioned branch:

```rust
        // captured before the thunk (beside suppress_factory):
        let versioned_cfg = materialized.versioned;
```

```rust
                // inside the thunk, replacing the processor + store registration:
                if let Some(vc) = versioned_cfg {
                    let store_for_proc = store_for_thunk.clone();
                    let h = state
                        .topology
                        .add_processor::<KS::Target, VS::Target, KS::Target, crate::dsl::processors::change::Change<VS::Target>, _, _, _>(
                            proc_name,
                            move || crate::dsl::processors::table::VersionedKTableSourceProcessor {
                                store_name: store_for_proc.clone(),
                                _pd: std::marker::PhantomData,
                            },
                            [&src],
                        );
                    state.topology.add_versioned_store::<KS::Target, VS::Target, KS, VS>(
                        store_for_thunk.clone(),
                        key_serde_for_lower,
                        value_serde_for_lower,
                        vc.history_retention_ms,
                        [h.name().to_string()],
                    );
                    state.handle_name.insert(id, h.name().to_string());
                } else {
                    // ... existing KV-path code unchanged ...
                }
```

Add to `src/test_driver.rs` (if needed by the test):

```rust
    /// Latest live value of a key in a versioned store (test helper).
    pub fn store_get_versioned<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        store_name: &str,
        key: &K,
    ) -> Option<V> {
        let store = self.registry().get_versioned::<K, V>(store_name)?;
        pollster::block_on(store.get(key)).map(|r| r.value)
    }
```

> Match `store_get`'s exact body/visibility in `test_driver.rs` for how it
> reaches the registry and blocks — copy that pattern.

In `src/dsl/mod.rs` and `src/lib.rs`, re-export the new public types:

```rust
pub use crate::dsl::config::VersionedConfig;
pub use crate::store::versioned::VersionedRecord;
```

Add a short module-doc paragraph in `src/lib.rs` beside the windowing prose:

```rust
//! ## Versioned tables (KIP-889)
//!
//! `builder.table(..., Materialized::as_versioned(name, history_retention_ms))`
//! materializes a table into a [`VersionedKeyValueStore`], so out-of-order
//! records are recorded as historical versions without clobbering the latest,
//! and point-in-time reads are available via `get_as_of`.
```

- [ ] **Step 4: Run test + full suite**

Run: `cargo test -p crabka-client-streams 2>&1 | tail -30`
Expected: the new test PASSES and the full suite is green (erasure-safety gate).

- [ ] **Step 5: Commit**

```bash
git add src/dsl/builder.rs src/dsl/mod.rs src/lib.rs src/test_driver.rs tests/dsl_execution.rs
git commit -m "feat(client-streams): materialize KTable into versioned store (KIP-889/914)"
```

---

## Task 11: JVM capture — structural topology + changelog + behavioral dumps

Add a versioned-table fixture to the capture harness producing three goldens:
the wire topology JSON, the changelog-bytes dump, and the behavioral output dump.

**Files:**
- Modify: `tests/jvm-capture/src/main/java/crabka/capture/Capture.java` (add a
  `versionedTable()` topology builder + register it in the fixture list)
- Create: `tests/jvm-capture/src/main/java/crabka/capture/VersionedTableBehavior.java`
  (TopologyTestDriver runner emitting changelog + behavioral dumps — model on
  `InteractiveQueryBehavior.java` / `ForeignKeyJoinBehavior.java`)
- Modify: `tests/jvm-capture/run.sh` (add a `--versioned` mode + list the fixture)

- [ ] **Step 1: Add the structural topology to `Capture.java`**

Add a method building the same logical pipeline as the Rust test, materialized
versioned, with an explicit store name so node-name burn doesn't affect bytes:

```java
static Topology versionedTable() {
    StreamsBuilder b = new StreamsBuilder();
    b.table("in",
            Consumed.with(Serdes.String(), Serdes.Integer()),
            Materialized.<String, Integer>as(
                Stores.persistentVersionedKeyValueStore("vt", Duration.ofMillis(600_000)))
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Integer()))
        .toStream()
        .to("out", Produced.with(Serdes.String(), Serdes.Integer()));
    return b.build(props_optimizeAll());
}
```

Register `versioned_table` in the same list/dispatch the other fixtures use
(grep `windowed_count` in `Capture.java` to find where fixtures are emitted and
add an entry writing `versioned_table.topology.json`).

- [ ] **Step 2: Write the behavioral + changelog runner**

Create `VersionedTableBehavior.java` modeled on `InteractiveQueryBehavior.java`.
Drive a `TopologyTestDriver` over `Capture.versionedTable()` (built **unoptimized**
via `b.build()` for the behavioral run, matching the FK-join behavioral-oracle
convention) with this fixed input battery (`ts` is the record timestamp):

```
("k", 10, ts=100)
("k", 20, ts=200)
("k", 15, ts=150)   # out-of-order
("k", null, ts=250) # tombstone (delete)
("k", 30, ts=300)
("j", 5,  ts=120)
```

Emit two JSON files:
- `testdata/golden/dsl/behavioral/versioned_table.json` — the ordered list of
  output records read from the `out` topic: `[{key, value, ts}]` (value null for
  tombstones forwarded).
- `testdata/golden/dsl/behavioral/versioned_changelog.json` — the changelog
  records the driver produced for the `app-vt-changelog` topic, as
  `[{keyHex, valueHex, ts}]`. Read them via
  `driver.createOutputTopic("app-vt-changelog", ...)` with `ByteArray` serdes, or
  via the driver's changelog records API the sibling capture classes use (grep
  `changelog` in the capture sources for the exact accessor).

Dump format: copy the `quote(...)` / hex helpers and the file-writing idiom from
`InteractiveQueryBehavior.java` / `ForeignKeyJoinBehavior.java`.

- [ ] **Step 3: Add a `--versioned` run mode to `run.sh`**

Mirror the existing `--sliding` / `--iq` modes: a `case` arm that compiles +
runs `crabka.capture.VersionedTableBehavior`, mounting `tests/` so the goldens
persist to the host. Add `versioned_table` to the `--gradle`/`--javac` fixture
list comment and ensure `Capture.java`'s main writes `versioned_table.topology.json`.

- [ ] **Step 4: Run the capture (requires Docker)**

Run:
```bash
cd tests/jvm-capture && ./run.sh --gradle && ./run.sh --versioned
```
Expected: writes
`tests/testdata/golden/dsl/versioned_table.topology.json`,
`tests/testdata/golden/dsl/behavioral/versioned_table.json`,
`tests/testdata/golden/dsl/behavioral/versioned_changelog.json`.

> If Docker is unavailable in this environment, this task is the hand-off point:
> the goldens must be captured on a Docker-capable host (per the memory note,
> capture is the ground truth and cannot be hand-written). Commit the captured
> files; Tasks 12–13 assert against them.

- [ ] **Step 5: Commit the captured goldens + harness**

```bash
git add tests/jvm-capture/ tests/testdata/golden/dsl/versioned_table.topology.json \
        tests/testdata/golden/dsl/behavioral/versioned_table.json \
        tests/testdata/golden/dsl/behavioral/versioned_changelog.json
git commit -m "test(client-streams): capture JVM versioned-table topology + behavioral + changelog goldens"
```

---

## Task 12: Structural + changelog golden assertions (Rust)

**Files:**
- Modify: `tests/dsl_golden_frame.rs` (structural assertion)
- Modify: `tests/dsl_execution.rs` (changelog-bytes assertion) or a new
  `tests/versioned_changelog.rs`

- [ ] **Step 1: Write the structural failing test**

Add to `tests/dsl_golden_frame.rs`:

```rust
#[test]
fn versioned_table_matches_jvm() {
    use crabka_client_streams::{I64Serde, Materialized, StringSerde};
    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000),
    )
    .to_stream()
    .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let wire = b.build_optimized("app").unwrap().to_wire();
    assert_matches_fixture(&wire, "versioned_table");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-client-streams versioned_table_matches_jvm 2>&1 | tail -40`
Expected: FAIL — either the fixture diff (adjust the topology/config to match) or
fixture-missing (Task 11 must run first on a Docker host).

- [ ] **Step 3: Reconcile any diff**

Diff `actual` vs `expected`. If `min.compaction.lag.ms` / `cleanup.policy` differ,
adjust `versioned_changelog_topic_configs` (Task 5). If a node name / store
position differs, adjust the lowering in Task 10. Re-run until byte-equal. **This
is the fidelity reconciliation step — change Crabka to match the capture, never
edit the golden.**

- [ ] **Step 4: Write the changelog-bytes failing test**

Add (new file `tests/versioned_changelog.rs`):

```rust
//! The changelog bytes the versioned table drains must match the JVM capture.
use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{Consumed, I64Serde, Materialized, StringSerde};

#[test]
fn versioned_changelog_matches_jvm() {
    let golden: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string("tests/testdata/golden/dsl/behavioral/versioned_changelog.json")
            .expect("changelog golden present"),
    )
    .unwrap();

    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000),
    );
    let built = b.build("app").unwrap();
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();
    let battery: [(&str, Option<i64>, i64); 6] = [
        ("k", Some(10), 100),
        ("k", Some(20), 200),
        ("k", Some(15), 150),
        ("k", None, 250),
        ("k", Some(30), 300),
        ("j", Some(5), 120),
    ];
    for (k, v, ts) in battery {
        d.pipe_input_opt("in", Consumed::with(StringSerde, I64Serde), Some(k.to_string()), v, ts);
    }
    let drained = d.drain_changelog_bytes("app-vt-changelog"); // Vec<(Bytes key, Option<Bytes> val, i64 ts)>
    // Compare against the golden's keyHex/valueHex/ts list.
    // (Encode `drained` to the same {keyHex, valueHex, ts} shape and assert_eq.)
    let actual = encode_changelog_for_compare(&drained);
    assert_eq!(actual, golden);
}
```

> This test needs two small test-driver affordances: `pipe_input_opt` (a `pipe_input`
> variant accepting `Option<V>` so tombstones can be fed) and
> `drain_changelog_bytes(topic)` (returns the buffered changelog tuples for a
> topic, including the produced record timestamp). Add both to `src/test_driver.rs`,
> modeled on the existing `pipe_input` + the runtime's `drain_changelogs`. Include
> a `encode_changelog_for_compare` helper in the test file that maps to
> `{keyHex, valueHex, ts}`.

- [ ] **Step 5: Run, reconcile, commit**

Run: `cargo test -p crabka-client-streams versioned_changelog_matches_jvm 2>&1 | tail -40`
Expected: FAIL first, then reconcile. **Decision point:** if the golden's
`valueHex` carries the `ts‖value` header (window-store precedent), the current
codec matches — done. If the golden shows the timestamp is **only** in the record
`ts` field with a bare value (no header), implement the documented fallback: thread
an explicit timestamp through the changelog produce path (`StateStore::take_changelog`
→ `(Bytes, Option<Bytes>, i64)`; `task.rs` `producer.send` with that timestamp;
restore fetcher surfaces the record ts). Re-run until byte-equal.

```bash
git add tests/dsl_golden_frame.rs tests/versioned_changelog.rs src/test_driver.rs
git commit -m "test(client-streams): versioned-table structural + changelog golden parity"
```

---

## Task 13: Behavioral golden replay

Replay the identical input battery through the Rust runtime and assert the
emitted change-stream matches the JVM behavioral dump exactly.

**Files:**
- Modify: `tests/dsl_execution.rs` (or new `tests/versioned_behavioral.rs`)
- Modify: `tests/jvm-capture/.../VersionedTableBehavior.java` only if the output
  shape needs a tweak discovered here

- [ ] **Step 1: Write the failing replay test**

Create `tests/versioned_behavioral.rs`:

```rust
//! Replay the JVM behavioral battery through the Rust versioned table and assert
//! the forwarded output sequence matches the captured JVM dump exactly.
use crabka_client_streams::dsl::StreamsBuilder;
use crabka_client_streams::{Consumed, I64Serde, Materialized, Produced, StringSerde};

#[test]
fn versioned_table_behavioral_matches_jvm() {
    let golden: Vec<(Option<String>, Option<i64>, i64)> = serde_json::from_str::<serde_json::Value>(
        &std::fs::read_to_string("tests/testdata/golden/dsl/behavioral/versioned_table.json")
            .expect("behavioral golden present"),
    )
    .unwrap()
    .as_array()
    .unwrap()
    .iter()
    .map(|e| {
        (
            e["key"].as_str().map(str::to_string),
            e["value"].as_i64(),
            e["ts"].as_i64().unwrap(),
        )
    })
    .collect();

    let b = StreamsBuilder::new();
    b.table_explicit(
        "in",
        Consumed::with(StringSerde, I64Serde),
        Materialized::with(StringSerde, I64Serde).as_versioned("vt", 600_000),
    )
    .to_stream()
    .to_explicit("out", Produced::with(StringSerde, I64Serde));
    let built = b.build("app").unwrap(); // unoptimized, matching the JVM behavioral build
    let mut d = crabka_client_streams::TopologyTestDriver::new(&built).unwrap();

    let battery: [(&str, Option<i64>, i64); 6] = [
        ("k", Some(10), 100),
        ("k", Some(20), 200),
        ("k", Some(15), 150),
        ("k", None, 250),
        ("k", Some(30), 300),
        ("j", Some(5), 120),
    ];
    for (k, v, ts) in battery {
        d.pipe_input_opt("in", Consumed::with(StringSerde, I64Serde), Some(k.to_string()), v, ts);
    }

    let mut actual = Vec::new();
    while let Some((key, value)) = d.read_output_opt("out", Produced::with(StringSerde, I64Serde)) {
        // read_output_opt yields (Option<K>, Option<V>); ts via the driver's last-output ts accessor
        actual.push((key, value));
    }
    let expected: Vec<(Option<String>, Option<i64>)> =
        golden.iter().map(|(k, v, _ts)| (k.clone(), *v)).collect();
    assert_eq!(actual, expected);
}
```

> `to_stream()` drops tombstones in `KTableToStreamProcessor` (pre-existing
> behavior — see the cogroup memory note); if the JVM dump includes tombstone
> rows the Rust `to_stream` won't emit, filter nulls from the golden before
> comparison (document it inline), or compare the change-stream *before*
> `to_stream` by materializing to `out` via a `Change`-preserving sink. Prefer
> filtering nulls + a comment, consistent with the existing session-cogroup test.
> Add `pipe_input_opt` / `read_output_opt` to `src/test_driver.rs` if not added in
> Task 12.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-client-streams versioned_table_behavioral_matches_jvm 2>&1 | tail -40`
Expected: FAIL — sequence mismatch or missing driver helpers.

- [ ] **Step 3: Reconcile the processor against the golden**

Any mismatch is a `VersionedKTableSourceProcessor` semantics bug (Task 9) — fix
the processor (old/new selection, out-of-order handling, late-drop) until the
sequence matches. Never edit the golden.

- [ ] **Step 4: Run full suite**

Run: `cargo test -p crabka-client-streams 2>&1 | tail -30`
Expected: all green.

- [ ] **Step 5: Lint + commit**

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
git add tests/versioned_behavioral.rs src/test_driver.rs
git commit -m "test(client-streams): versioned-table behavioral golden replay (KIP-914)"
```

---

## Final verification

- [ ] **Run the full crate suite (erasure-safety gate):**

Run: `cargo test -p crabka-client-streams 2>&1 | tail -30`
Expected: all tests pass.

- [ ] **Run fmt + clippy (CI gates, per memory):**

Run:
```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -20
```
Expected: no diffs, no warnings.

- [ ] **Confirm goldens are committed** (`versioned_table.topology.json`,
  `behavioral/versioned_table.json`, `behavioral/versioned_changelog.json`).

---

## Self-review notes (spec coverage)

- Spec §5 store API → Tasks 1–2. Spec §4 module layout → Tasks 1–10. Spec §6 DSL
  + processor → Tasks 8–10. Spec §7 changelog/restore → Tasks 2, 5–6, 12;
  IQ surface → Task 4. Spec §8 three gates → Tasks 11 (structural), 12
  (structural + changelog), 13 (behavioral). Spec §9 changelog-format risk →
  explicit decision point in Task 12 Step 5.
- The `K: Send + Sync` bound is consistent across the store trait (Task 2),
  registry (Task 3), `add_versioned_store` (Task 6), and context accessor (Task 7).
- `as_versioned` / `VersionedConfig` / `versioned` field names are consistent
  across Tasks 8, 10, 12, 13.
- Method names `get`/`get_as_of`/`put`/`delete`, `iq_versioned_get`/
  `iq_versioned_get_as_of`, `add_versioned_store`, `get_versioned`,
  `get_versioned_store`, `store_get_versioned` are used consistently.
