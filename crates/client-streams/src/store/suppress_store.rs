//! Suppress buffer as a registered, changelog-backed state store.
//!
//! The suppress store does NOT sit over the pluggable `ByteKeyValueStore`
//! backend, unlike the other typed stores
//! [`WindowBytesStore`](crate::store::window) and
//! [`SessionBytesStore`](crate::store::session). A `KTable.suppress(...)` buffer
//! evicts by `buffer_time`, not by key order. So its internal storage is a
//! time-ordered `BTreeMap` keyed by `(buffer_time, seq)`, with a side `index`
//! from `key_bytes` to slot for replace-by-key. The store keeps serialized keys
//! and values, tracks byte-size limits, and writes a JVM-compatible changelog.
//!
//! The changelog KEY is the serialized record-key bytes. The changelog VALUE is
//! the JVM `BufferValue` payload. A buffer eviction logs a `(key_bytes, None)`
//! tombstone.
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
};

use async_trait::async_trait;
use bytes::Bytes;
use crabka_units::prelude::*;

use crate::{
    dsl::processors::change::Change,
    processor::serde::Serde,
    store::{
        api::StateStore,
        suppress_bufval::{SuppressRecordCtx, deserialize_buffer_change, serialize_buffer_change},
    },
};

/// Typed, time-ordered suppress buffer.
///
/// `put` buffers a [`Change<V>`] under a `buffer_time` and replaces by key.
/// `evict_while` and `evict_oldest` drain the earliest-closing entries and log
/// tombstones, so the processor can forward them.
// `pub(crate)`: the trait surfaces `Change<V>` (a crate-internal type) and the
// suppress store is a built-in DSL mechanism the suppress processor reaches via
// `ctx.get_suppress_store` — never a user-facing custom-processor store.
#[async_trait]
pub(crate) trait SuppressStore<K: Send + Sync, V: Send>: StateStore {
    /// Buffers or replaces the pending change for `key` at `buffer_time`.
    ///
    /// `ctx` is the source record context. The store carries it into the
    /// changelog VALUE.
    async fn put(&mut self, key: K, buffer_time: i64, change: Change<V>, ctx: SuppressRecordCtx);
    /// Pops every entry with `buffer_time <= threshold`.
    ///
    /// The entries come back in `(buffer_time, seq)` order as
    /// `(key, change, record_ts)`. This method logs a tombstone for each popped
    /// entry.
    async fn evict_while(&mut self, threshold: i64) -> Vec<(K, Change<V>, i64)>;
    /// Pops the single lowest-`(buffer_time, seq)` entry for emit-early overflow.
    ///
    /// Returns `None` if the buffer is empty. This method logs a tombstone for
    /// the popped entry.
    async fn evict_oldest(&mut self) -> Option<(K, Change<V>, i64)>;
    fn len(&self) -> usize;
    /// Paired with [`len`](Self::len) for `clippy::len_without_is_empty`.
    ///
    /// The processor reads `len` and `byte_size` for the caps, never emptiness.
    /// Only the store's own tests call this method.
    #[allow(dead_code)]
    fn is_empty(&self) -> bool;
    /// Total buffered size, the JVM `maxBytes`-cap accounting unit.
    ///
    /// Each entry counts `key_bytes.len() + new_bytes.len()`.
    fn byte_size(&self) -> ByteSize;
}

/// One buffered entry.
///
/// `new_bytes` and `old_bytes` are the serializable sides of the buffered
/// `Change`. The store recovers them on eviction. `record_ts` is the forwarded
/// timestamp. The store builds the changelog VALUE once at `put` and drains it on
/// evict, so it does not keep the `prior` and `ctx` that VALUE needs per entry.
struct Entry {
    key_bytes: Bytes,
    new_bytes: Option<Bytes>,
    old_bytes: Option<Bytes>,
    record_ts: i64,
}

/// Per-entry `maxBytes` accounting unit: serialized key + serialized new value.
fn entry_size(key_bytes: &Bytes, new_bytes: Option<&Bytes>) -> usize {
    key_bytes.len() + new_bytes.map_or(0, Bytes::len)
}

/// Typed suppress store that holds the key serde and the value serde.
///
/// The task registers this store in its state-store registry, and the suppress
/// processor looks it up there.
pub struct SuppressBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    logging: bool,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    /// Ordered by `(buffer_time, seq)`. `seq` separates equal buffer times.
    entries: BTreeMap<(i64, u64), Entry>,
    /// `key_bytes -> slot`, for replace-by-key.
    index: HashMap<Bytes, (i64, u64)>,
    seq: u64,
    byte_size: usize,
    changelog: Vec<(Bytes, Option<Bytes>)>,
}

impl<K: 'static, V: 'static> SuppressBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            logging: true,
            key_serde,
            value_serde,
            entries: BTreeMap::new(),
            index: HashMap::new(),
            seq: 0,
            byte_size: 0,
            changelog: Vec::new(),
        }
    }

    /// The public ctor that the tests and the DSL use.
    ///
    /// Suppress has no pluggable backend, so `in_memory` and `new` have the same
    /// body.
    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
    ) -> Self {
        Self::new(name, key_serde, value_serde, changelog_topic)
    }

    /// Removes the current slot for a key, if there is one, and subtracts its
    /// size.
    ///
    /// Returns the `new_bytes` of the removed entry. That is the value the store
    /// buffered for the key before. It is the `prior` for the JVM `-2` alias rule.
    fn remove_existing(&mut self, key_bytes: &Bytes) -> Option<Bytes> {
        let slot = self.index.remove(key_bytes)?;
        let entry = self.entries.remove(&slot).expect("indexed slot present");
        self.byte_size -= entry_size(&entry.key_bytes, entry.new_bytes.as_ref());
        entry.new_bytes
    }

    /// Inserts a fresh slot at `(buffer_time, seq)` and accounts its size.
    fn insert_slot(&mut self, buffer_time: i64, entry: Entry) {
        let slot = (buffer_time, self.seq);
        self.seq += 1;
        self.index.insert(entry.key_bytes.clone(), slot);
        self.byte_size += entry_size(&entry.key_bytes, entry.new_bytes.as_ref());
        self.entries.insert(slot, entry);
    }

    /// Pops one slot.
    ///
    /// This method drops the slot from `entries` and `index`, subtracts its size,
    /// logs a tombstone if logging is on, and rebuilds the typed
    /// `(K, Change<V>, record_ts)`.
    fn pop_slot(&mut self, slot: (i64, u64)) -> (K, Change<V>, i64) {
        let entry = self.entries.remove(&slot).expect("slot present");
        self.index.remove(&entry.key_bytes);
        self.byte_size -= entry_size(&entry.key_bytes, entry.new_bytes.as_ref());
        if self.logging {
            self.changelog.push((entry.key_bytes.clone(), None));
        }
        let key = self
            .key_serde
            .deserialize(&self.changelog_topic, &entry.key_bytes)
            .expect("suppress key deserialize");
        let change = Change {
            old: entry.old_bytes.map(|b| {
                self.value_serde
                    .deserialize(&self.changelog_topic, &b)
                    .expect("old value deserialize")
            }),
            new: entry.new_bytes.map(|b| {
                self.value_serde
                    .deserialize(&self.changelog_topic, &b)
                    .expect("new value deserialize")
            }),
        };
        (key, change, entry.record_ts)
    }
}

#[async_trait]
impl<K: 'static, V: 'static> StateStore for SuppressBytesStore<K, V> {
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
        // Restore is silent: re-build the buffer WITHOUT pushing to `self.changelog`.
        match value {
            Some(v) => {
                let d = deserialize_buffer_change(&v);
                // Replace-by-key: drop any prior slot for this key (the changelog
                // KEY == the serialized key bytes) before inserting the fresh slot.
                self.remove_existing(&key);
                let entry = Entry {
                    key_bytes: key,
                    new_bytes: d.new.map(Bytes::from),
                    old_bytes: d.old.map(Bytes::from),
                    record_ts: d.ctx.timestamp,
                };
                self.insert_slot(d.buffer_time, entry);
            }
            None => {
                self.remove_existing(&key);
            }
        }
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
    async fn clear(&mut self) {
        // No pluggable backend: wipe the in-memory time-ordered buffer + index +
        // accounting + the changelog buffer (re-restore replays the committed log).
        self.entries.clear();
        self.index.clear();
        self.seq = 0;
        self.byte_size = 0;
        self.changelog.clear();
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> SuppressStore<K, V> for SuppressBytesStore<K, V> {
    async fn put(&mut self, key: K, buffer_time: i64, change: Change<V>, ctx: SuppressRecordCtx) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let new_bytes = change
            .new
            .as_ref()
            .map(|v| self.value_serde.serialize(&self.changelog_topic, v));
        let old_bytes = change
            .old
            .as_ref()
            .map(|v| self.value_serde.serialize(&self.changelog_topic, v));
        // prior = the value previously buffered for this key; on first buffering
        // there is none, so prior IS the incoming change's `old` (matching the JVM
        // reference-identity rule where the first put's prior == oldValue, which
        // makes the `-2` "old same as prior" changelog sentinel fire).
        let existing_new = self.remove_existing(&kb);
        let prior_bytes = existing_new.or_else(|| old_bytes.clone());

        let entry = Entry {
            key_bytes: kb.clone(),
            new_bytes: new_bytes.clone(),
            old_bytes: old_bytes.clone(),
            record_ts: ctx.timestamp,
        };
        self.insert_slot(buffer_time, entry);

        if self.logging {
            let value = serialize_buffer_change(
                &ctx,
                prior_bytes.as_deref(),
                old_bytes.as_deref(),
                new_bytes.as_deref(),
                buffer_time,
            );
            self.changelog.push((kb, Some(value)));
        }
    }

    async fn evict_while(&mut self, threshold: i64) -> Vec<(K, Change<V>, i64)> {
        let mut out = Vec::new();
        while let Some((&slot, _)) = self.entries.iter().next() {
            if slot.0 > threshold {
                break;
            }
            out.push(self.pop_slot(slot));
        }
        out
    }

    async fn evict_oldest(&mut self) -> Option<(K, Change<V>, i64)> {
        let (&slot, _) = self.entries.iter().next()?;
        Some(self.pop_slot(slot))
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn byte_size(&self) -> ByteSize {
        // The running total is kept as a `usize` so the incremental add/subtract
        // stays exact; the dimension is put back on at the accessor.
        ByteSize::from_bytes(self.byte_size.try_into().unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    fn store() -> SuppressBytesStore<String, i64> {
        SuppressBytesStore::<String, i64>::in_memory(
            "sup".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-sup-changelog".into(),
        )
    }

    fn ctx(timestamp: i64) -> SuppressRecordCtx {
        SuppressRecordCtx {
            topic: "in".into(),
            partition: 0,
            offset: 0,
            timestamp,
        }
    }

    /// The suppress store does not override the cache/IQ `StateStore` defaults,
    /// so it exercises the default trait-method bodies in `store::api`:
    /// `as_iq` → None, `enable_cache_erased`/`is_cached_erased` → false,
    /// `flush_cache_into` → no-op, and `take_changelog_ts` wraps `take_changelog`
    /// with a `None` timestamp.
    #[tokio::test]
    async fn uses_statestore_defaults_for_cache_and_iq() {
        use std::sync::{Arc, Mutex};

        use crate::store::cache::named::NamedCache;
        let mut s = store();

        // Not interactively queryable, not cache-aware.
        assert!(s.as_iq().is_none());
        assert!(!s.is_cached_erased());
        let cache = Arc::new(Mutex::new(NamedCache::new("sup".into())));
        assert!(
            !s.enable_cache_erased(cache),
            "suppress store is not cache-aware"
        );
        assert!(
            !s.is_cached_erased(),
            "still not cached after the no-op enable"
        );

        // The default flush_cache_into forwards nothing even with a staged entry.
        s.put("a".into(), 30, Change::update(None, 1), ctx(30))
            .await;
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty(), "no record cache → no forwarded change");

        // take_changelog_ts wraps each changelog entry with a None timestamp.
        let cl_ts = s.take_changelog_ts();
        assert_eq!(cl_ts.len(), 1);
        assert!(cl_ts[0].2.is_none(), "default timestamp is None");
        // The wrapped take drained the buffer.
        assert!(s.take_changelog().is_empty());

        // set_record_context default is a no-op (must not panic).
        s.set_record_context(crate::processor::record::RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        });
    }

    #[tokio::test]
    async fn put_then_evict_while_in_buffer_time_order() {
        let mut s = store();
        s.put("a".into(), 30, Change::update(None, 1), ctx(30))
            .await;
        s.put("b".into(), 10, Change::update(None, 2), ctx(10))
            .await;
        check!(s.len() == 2);
        check!(!s.is_empty());
        check!(s.byte_size() > ByteSize::ZERO);
        // threshold 30 drains both, earliest buffer_time first.
        let out = s.evict_while(30).await;
        check!(out.len() == 2);
        check!(out[0].0 == "b" && out[0].1.new == Some(2) && out[0].2 == 10);
        check!(out[1].0 == "a" && out[1].1.new == Some(1) && out[1].2 == 30);
        check!(s.len() == 0);
        check!(s.is_empty());
        check!(s.byte_size() == ByteSize::ZERO);
    }

    #[tokio::test]
    async fn replace_by_key_keeps_one_entry_with_latest_change() {
        let mut s = store();
        s.put("k".into(), 10, Change::update(None, 1), ctx(10))
            .await;
        s.put("k".into(), 10, Change::update(Some(1), 2), ctx(12))
            .await;
        check!(s.len() == 1);
        let out = s.evict_while(10).await;
        check!(out.len() == 1);
        check!(out[0].1.new == Some(2));
        check!(out[0].1.old == Some(1));
    }

    #[tokio::test]
    async fn evict_oldest_pops_lowest_buffer_time_then_none() {
        let mut s = store();
        s.put("a".into(), 30, Change::update(None, 1), ctx(30))
            .await;
        s.put("b".into(), 10, Change::update(None, 2), ctx(10))
            .await;
        let first = s.evict_oldest().await.unwrap();
        check!(first.0 == "b" && first.1.new == Some(2));
        let second = s.evict_oldest().await.unwrap();
        check!(second.0 == "a" && second.1.new == Some(1));
        check!(s.evict_oldest().await.is_none());
    }

    #[tokio::test]
    async fn changelog_records_puts_then_tombstones_on_evict() {
        let mut s = store();
        s.put("a".into(), 10, Change::update(None, 1), ctx(10))
            .await;
        s.put("b".into(), 20, Change::update(None, 2), ctx(20))
            .await;
        let cl = s.take_changelog();
        check!(cl.len() == 2);
        check!(cl.iter().all(|(_, v)| v.is_some()));
        // evicting both logs a None tombstone keyed by the serialized key bytes.
        let _ = s.evict_while(20).await;
        let cl = s.take_changelog();
        check!(cl.len() == 2);
        check!(cl.iter().all(|(_, v)| v.is_none()));
        check!(cl[0].0.as_ref() == b"a");
        check!(cl[1].0.as_ref() == b"b");
    }

    #[tokio::test]
    async fn apply_changelog_round_trips_then_tombstone_removes() {
        // Produce a put changelog entry from a source store.
        let mut src = store();
        src.put("k".into(), 42, Change::update(Some(7), 9), ctx(42))
            .await;
        let cl = src.take_changelog();
        check!(cl.len() == 1);
        let (key, value) = cl.into_iter().next().unwrap();

        // Restore into a FRESH store; apply_changelog must be silent.
        let mut dst = store();
        dst.apply_changelog(key.clone(), value).await;
        check!(dst.len() == 1);
        check!(dst.take_changelog().is_empty()); // restore re-emits nothing
        let out = dst.evict_while(42).await;
        check!(out.len() == 1);
        check!(out[0].0 == "k");
        check!(out[0].1.new == Some(9));
        check!(out[0].1.old == Some(7));
        check!(out[0].2 == 42);

        // Re-buffer, then a None tombstone removes it.
        dst.apply_changelog(
            key.clone(),
            Some(serialize_buffer_change(
                &ctx(42),
                None,
                Some(&7i64.to_be_bytes()),
                Some(&9i64.to_be_bytes()),
                42,
            )),
        )
        .await;
        check!(dst.len() == 1);
        dst.apply_changelog(key, None).await;
        check!(dst.is_empty());
    }

    #[tokio::test]
    async fn logging_off_suppresses_changelog() {
        let mut s = store();
        s.set_logging(false);
        s.put("k".into(), 10, Change::update(None, 1), ctx(10))
            .await;
        check!(s.take_changelog().is_empty());
        let _ = s.evict_while(10).await;
        check!(s.take_changelog().is_empty());
    }

    /// Restart-restore at the golden's scenario.
    ///
    /// A windowed-key suppress buffer survives `take_changelog` → fresh store →
    /// `apply_changelog`. The buffer has the `until_window_closes` shape and
    /// `Windowed<String>` keys from `TimeWindowedSerde`. The restored buffered
    /// windows still emit their final value when stream-time closes them. Task
    /// restore drives exactly this drain and replay over the registered store.
    #[tokio::test]
    async fn windowed_buffer_restores_and_emits_on_close() {
        use crate::dsl::windows::{TimeWindowedSerde, Window, Windowed};

        fn win_store() -> SuppressBytesStore<Windowed<String>, i64> {
            SuppressBytesStore::<Windowed<String>, i64>::in_memory(
                "sup".into(),
                Box::new(TimeWindowedSerde::new(StringSerde, millis(10))),
                Box::new(I64Serde),
                "app-sup-changelog".into(),
            )
        }
        fn wk(key: &str, start: i64) -> Windowed<String> {
            Windowed {
                key: key.into(),
                window: Window {
                    start,
                    end: start + 10,
                },
            }
        }

        // Source store buffers two windows' final values (buffer_time = window end).
        let mut src = win_store();
        src.put(wk("a", 0), 10, Change::update(None, 2), ctx(5))
            .await;
        src.put(wk("b", 20), 30, Change::update(None, 7), ctx(25))
            .await;
        let changelog = src.take_changelog();
        check!(changelog.len() == 2);

        // Restart: a FRESH store replays the changelog (silently — no re-emit).
        let mut restored = win_store();
        for (key, value) in changelog {
            restored.apply_changelog(key, value).await;
        }
        check!(restored.len() == 2);
        check!(restored.take_changelog().is_empty());

        // Closing window [0,10) (threshold 10) emits a@[0,10)'s final value (2);
        // b@[20,30) stays buffered until its own close.
        let closed = restored.evict_while(10).await;
        check!(closed.len() == 1);
        check!(closed[0].0 == wk("a", 0));
        check!(closed[0].1.new == Some(2));
        check!(restored.len() == 1);
        // Raising the threshold closes b@[20,30) too.
        let rest = restored.evict_while(30).await;
        check!(rest.len() == 1);
        check!(rest[0].0 == wk("b", 20));
        check!(rest[0].1.new == Some(7));
    }
}
