//! `CachingSessionStore`: session-schema-keyed write-back caching wrapper over a
//! session byte store. Ports Kafka `CachingSessionStore`.
//!
//! The cache key is the full `SessionKeySchema` composite (`inner_key ‖ end ‖
//! start`) — the exact bytes the underlying [`SessionBytesStore`] persists — so
//! the cache and the inner store share one key space. Writes are write-back:
//! [`put`](CachingSessionStore::put) only stages a dirty entry in the cache; it
//! is pushed THROUGH to the inner store on [`flush`](CachingSessionStore::flush).
//! Reads via [`find_sessions`](CachingSessionStore::find_sessions) merge the
//! cache over the inner store: cache entries win on key collision and a cached
//! tombstone (dirty `None`) hides the inner value.
//!
//! ## Key reuse
//!
//! Cache keys are produced/decoded exclusively through
//! [`session_schema`](crate::store::session_schema): callers pass the composite
//! bytes (built with `session_key`), and `find_sessions` decodes the inner key,
//! `end`, and `start` back out with `session_key_bytes_of` / `session_end_of` /
//! `session_start_of`. No bespoke key layout is introduced here.
//!
//! ## Inner mutability / locking
//!
//! [`ByteKeyValueStore`]'s `put`/`delete` take `&mut self`, so `inner` is held
//! behind a `tokio::sync::Mutex` — its async methods are awaited while the guard
//! is held, which a `std::sync::Mutex` guard cannot do (`await_holding_lock`).
//! The cache, whose ops are synchronous, uses a plain `std::sync::Mutex` whose
//! guard is always dropped before any `.await`. All public methods take `&self`
//! so the store can be shared. This mirrors the sibling `CachingKeyValueStore`.
//!
//! ## Merged `find_sessions`
//!
//! [`NamedCache::range`] yields the staged entries whose composite key falls in
//! the session-key range in ascending memcmp order, so the merge enumerates cache
//! candidates directly off the cache (no shadow key set). Cache entries win on key
//! collision and a cached tombstone hides the inner value. Same approach as
//! `CachingKeyValueStore`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::processor::record::RecordContext;
use crate::store::byte::ByteKeyValueStore;
use crate::store::cache::entry::LruCacheEntry;
use crate::store::cache::named::NamedCache;
use crate::store::session_schema::{
    session_end_of, session_key, session_key_bytes_of, session_start_of,
};

pub(crate) struct CachingSessionStore {
    cache: Arc<Mutex<NamedCache>>,
    inner: AsyncMutex<Box<dyn ByteKeyValueStore>>,
    /// Cache name, captured so `clear` can rebuild an empty [`NamedCache`]
    /// under the same identity (mirrors `CachingKeyValueStore`).
    name: String,
}

impl CachingSessionStore {
    pub fn new(cache: Arc<Mutex<NamedCache>>, inner: Box<dyn ByteKeyValueStore>) -> Self {
        Self {
            cache,
            inner: AsyncMutex::new(inner),
            name: String::new(),
        }
    }

    /// Like [`new`](Self::new) but records the cache's name for `clear`.
    pub fn with_name(
        cache: Arc<Mutex<NamedCache>>,
        inner: Box<dyn ByteKeyValueStore>,
        name: String,
    ) -> Self {
        Self {
            cache,
            inner: AsyncMutex::new(inner),
            name,
        }
    }

    /// Cache-first single session-key read: a cache hit (including a dirty `None`
    /// tombstone) wins; otherwise fall through to the inner store.
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::copy_from_slice(key);
        let cached = {
            let mut cache = self.cache.lock().unwrap();
            cache.get_promote(&key).map(|e| e.value.clone())
        };
        match cached {
            Some(value) => value,
            None => self.inner.lock().await.get(&key).await,
        }
    }

    /// Merged raw session-key range `[lo, hi)`: inner overlaid with the cache.
    /// Cache wins on key collision; a cached tombstone hides the inner value.
    /// Returns key-sorted `(session_key, value)`.
    pub async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        let mut merged: BTreeMap<Bytes, Bytes> = {
            let inner = self.inner.lock().await;
            inner.range(lo, hi).await.into_iter().collect()
        };
        let cached = {
            let cache = self.cache.lock().unwrap();
            cache.range(lo, hi)
        };
        for (k, e) in cached {
            match e.value {
                Some(v) => {
                    merged.insert(k, v);
                }
                None => {
                    merged.remove(&k);
                }
            }
        }
        merged.into_iter().collect()
    }

    /// Merged unbounded scan: every inner entry overlaid with the cache. Cache
    /// wins on key collision; a cached tombstone hides the inner value. Returns
    /// key-sorted `(session_key, value)`. Backs `find_closed_sessions`.
    pub async fn scan_all(&self) -> Vec<(Bytes, Bytes)> {
        let mut merged: BTreeMap<Bytes, Bytes> = {
            let inner = self.inner.lock().await;
            inner.scan_all().await.into_iter().collect()
        };
        let cached = {
            let cache = self.cache.lock().unwrap();
            cache.all()
        };
        for (k, e) in cached {
            match e.value {
                Some(v) => {
                    merged.insert(k, v);
                }
                None => {
                    merged.remove(&k);
                }
            }
        }
        merged.into_iter().collect()
    }

    /// Write straight through to the inner store, bypassing the cache (restore
    /// path; mirrors `CachingKeyValueStore::put_inner`).
    pub async fn put_inner(&self, key: Bytes, value: Bytes) {
        self.inner.lock().await.put(key, value).await;
    }

    /// Delete straight through to the inner store, bypassing the cache (restore).
    pub async fn delete_inner(&self, key: &[u8]) {
        self.inner.lock().await.delete(key).await;
    }

    /// Clear both the cache layer and the inner store (EOS rollback reset).
    pub async fn clear(&self) {
        {
            let mut cache = self.cache.lock().unwrap();
            *cache = NamedCache::new(self.name.clone());
        }
        self.inner.lock().await.clear().await;
    }

    /// Write-back put: stage a dirty entry in the cache keyed by the full session
    /// composite bytes (`inner_key ‖ end ‖ start`, built via
    /// [`session_key`](crate::store::session_schema::session_key)). The inner
    /// store is not touched until [`flush`](Self::flush).
    #[allow(clippy::unused_async)]
    pub async fn put(&self, session_key_bytes: Bytes, value: Bytes, ctx: RecordContext) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(
            session_key_bytes,
            LruCacheEntry::new(Some(value), true, ctx),
        );
    }

    /// Write-back remove: stage a dirty tombstone (`None`) for the session.
    #[allow(clippy::unused_async)]
    pub async fn remove(&self, session_key_bytes: Bytes, ctx: RecordContext) {
        let mut cache = self.cache.lock().unwrap();
        cache.delete(session_key_bytes, ctx);
    }

    /// Cache-first session merge fetch: sessions for `key` whose
    /// `end >= earliest_end && start <= latest_start`, returned as
    /// `(start, end, value)` in store order (end asc, then start asc).
    ///
    /// The cache is overlaid on the inner store: a cached live value wins over
    /// the inner value on key collision, and a cached tombstone hides the inner
    /// value entirely. Only entries whose decoded inner-key bytes equal `key` are
    /// considered, guarding against prefix collisions with a different key.
    pub async fn find_sessions(
        &self,
        key: &[u8],
        earliest_end: i64,
        latest_start: i64,
    ) -> Vec<(i64, i64, Bytes)> {
        // Composite-key range covering every session for `key`. The lower bound
        // clamps `earliest_end` to 0 (stored ends are non-negative epoch millis;
        // a negative `earliest_end` means "all qualify"), mirroring
        // `SessionBytesStore::find_sessions`.
        let lo = session_key(key, 0, earliest_end.max(0));
        let hi = session_key(key, i64::MAX, i64::MAX);

        // Seed the merged view from the inner store. Ascending composite-key order
        // is end-then-start order, matching the inner `find_sessions` ordering.
        let mut merged: BTreeMap<Bytes, Bytes> = {
            let inner = self.inner.lock().await;
            inner
                .range(&lo, &hi)
                .await
                .into_iter()
                .filter(|(k, _)| session_key_bytes_of(k) == key)
                .collect()
        };

        // Overlay the cache entries staged in `[lo, hi)`, collected under the lock
        // and dropped before any await. Restrict to keys whose inner-key bytes
        // equal `key` to skip prefix collisions. Cache wins; a tombstone hides the
        // inner value.
        let cached = {
            let cache = self.cache.lock().unwrap();
            cache.range(&lo, &hi)
        };
        for (k, e) in cached {
            if session_key_bytes_of(&k) != key {
                continue;
            }
            match e.value {
                // Live value: cache wins over the inner store.
                Some(v) => {
                    merged.insert(k, v);
                }
                // Tombstone: hide the inner value.
                None => {
                    merged.remove(&k);
                }
            }
        }

        // Decode each surviving composite key into (start, end), filtering on the
        // merge bounds. BTreeMap iteration is ascending composite-key order.
        merged
            .into_iter()
            .filter_map(|(k, v)| {
                let end = session_end_of(&k);
                let start = session_start_of(&k);
                (end >= earliest_end && start <= latest_start).then_some((start, end, v))
            })
            .collect()
    }

    /// Flush: drain dirty entries in insertion order, write each THROUGH to the
    /// inner store (`put` the value, `delete` on a tombstone — keyed by the
    /// session composite bytes), clear dirty, and return the drained entries so
    /// the caller can forward them downstream.
    pub async fn flush(&self) -> Vec<(Bytes, LruCacheEntry)> {
        let mut collected: Vec<(Bytes, LruCacheEntry)> = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            let mut listener =
                |k: &Bytes, e: &LruCacheEntry| collected.push((k.clone(), e.clone()));
            cache.flush(&mut listener);
        } // cache guard dropped before any await
        {
            let mut inner = self.inner.lock().await;
            for (k, e) in &collected {
                match &e.value {
                    Some(v) => inner.put(k.clone(), v.clone()).await,
                    None => {
                        inner.delete(k).await;
                    }
                }
            }
        }
        collected
    }

    /// Flush dirty entries in insertion order, capturing the inner OLD value
    /// BEFORE each write-through. For each entry: read `old = inner.get(&k)`,
    /// write the new value through (`put` / `delete` on a tombstone), and return
    /// `(session_store_key, old, new, context)`. `old`/`new` are the raw aggregate
    /// value bytes (`None` = absent / tombstone). Mirrors
    /// `CachingKeyValueStore::flush_with_old`; the typed `SessionBytesStore` decodes
    /// the session key + deserializes the values to build the deduped downstream
    /// `Change`.
    pub async fn flush_with_old(
        &self,
    ) -> Vec<(Bytes, Option<Bytes>, Option<Bytes>, RecordContext)> {
        // Drain dirty entries under the cache lock, dropping the guard before any
        // await.
        let mut dirty: Vec<(Bytes, LruCacheEntry)> = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            let mut listener = |k: &Bytes, e: &LruCacheEntry| dirty.push((k.clone(), e.clone()));
            cache.flush(&mut listener);
        }
        let mut out = Vec::with_capacity(dirty.len());
        {
            let mut inner = self.inner.lock().await;
            for (k, e) in dirty {
                let old = inner.get(&k).await;
                match &e.value {
                    Some(v) => inner.put(k.clone(), v.clone()).await,
                    None => {
                        inner.delete(&k).await;
                    }
                }
                out.push((k, old, e.value, e.context));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::byte::InMemoryBytes;
    use assert2::check;

    fn ctx() -> RecordContext {
        RecordContext {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    fn cache() -> Arc<Mutex<NamedCache>> {
        Arc::new(Mutex::new(NamedCache::new("s".to_string())))
    }

    fn b(v: &'static [u8]) -> Bytes {
        Bytes::from_static(v)
    }

    /// A session put into the cache only (inner empty) is returned by
    /// `find_sessions`.
    #[tokio::test]
    async fn find_sessions_returns_cached() {
        let store = CachingSessionStore::new(cache(), Box::new(InMemoryBytes::default()));

        // Session [0, 10] for key "k", value "v".
        let sk = session_key(b"k", 0, 10);
        store.put(sk, b(b"v"), ctx()).await;

        let found = store.find_sessions(b"k", 0, 100).await;
        assert_eq!(found, vec![(0, 10, b(b"v"))]);
    }

    /// `flush` returns the drained dirty entry and writes it through to the inner
    /// store (verified by re-reading the now-clean store).
    #[tokio::test]
    async fn flush_writes_through_and_returns_entries() {
        let store = CachingSessionStore::new(cache(), Box::new(InMemoryBytes::default()));

        let sk = session_key(b"k", 0, 10);
        store.put(sk.clone(), b(b"v"), ctx()).await;

        let flushed = store.flush().await;
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, sk);
        assert_eq!(flushed[0].1.value, Some(b(b"v")));

        // Inner now has the write-through value; the cache is clean, so this
        // exercises the inner-store read path of the merge.
        let found = store.find_sessions(b"k", 0, 100).await;
        assert_eq!(found, vec![(0, 10, b(b"v"))]);
    }

    /// `find_sessions` merges cache + inner over the session-key range: results
    /// come back in session-key (end-then-start) order and the cache wins on a
    /// colliding session key.
    #[tokio::test]
    async fn find_sessions_merges_cache_and_underlying() {
        // Seed the inner store with two sessions for "k":
        //   [0, 10] -> "i1"   (cache will override this one)
        //   [0, 30] -> "i2"   (inner-only, survives)
        let mut inner = InMemoryBytes::default();
        inner.put(session_key(b"k", 0, 10), b(b"i1")).await;
        inner.put(session_key(b"k", 0, 30), b(b"i2")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));

        // Cache:
        //   [0, 10] -> "c1"   (overlaps inner's [0,10] — cache wins)
        //   [0, 20] -> "c2"   (cache-only)
        store.put(session_key(b"k", 0, 10), b(b"c1"), ctx()).await;
        store.put(session_key(b"k", 0, 20), b(b"c2"), ctx()).await;

        let found = store.find_sessions(b"k", 0, 100).await;
        assert_eq!(
            found,
            vec![
                (0, 10, b(b"c1")), // cache wins over inner's [0,10] -> i1
                (0, 20, b(b"c2")), // cache-only
                (0, 30, b(b"i2")), // inner-only; end asc puts it last
            ]
        );
    }

    /// A cached tombstone hides an inner session from `find_sessions`.
    #[tokio::test]
    async fn tombstone_hides_underlying_session() {
        let mut inner = InMemoryBytes::default();
        inner.put(session_key(b"k", 0, 10), b(b"i1")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));

        // Sanity: visible before the remove.
        assert_eq!(
            store.find_sessions(b"k", 0, 100).await,
            vec![(0, 10, b(b"i1"))]
        );

        store.remove(session_key(b"k", 0, 10), ctx()).await;
        assert!(store.find_sessions(b"k", 0, 100).await.is_empty());
    }

    /// Sessions belonging to a different key sharing a byte prefix are excluded.
    #[tokio::test]
    async fn other_key_prefix_is_not_returned() {
        let store = CachingSessionStore::new(cache(), Box::new(InMemoryBytes::default()));
        store.put(session_key(b"k", 0, 10), b(b"a"), ctx()).await;
        store.put(session_key(b"kk", 0, 10), b(b"b"), ctx()).await; // "k" prefix

        let found = store.find_sessions(b"k", 0, 100).await;
        assert_eq!(found, vec![(0, 10, b(b"a"))]);
    }

    /// `get` is cache-first: a staged value wins, and a miss falls through to the
    /// inner store.
    #[tokio::test]
    async fn get_is_cache_first_then_falls_through() {
        let mut inner = InMemoryBytes::default();
        let sk = session_key(b"k", 0, 10);
        inner.put(sk.clone(), b(b"inner")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));

        // Cache miss falls through to inner.
        assert_eq!(store.get(&sk).await, Some(b(b"inner")));

        // Staged value wins over inner.
        store.put(sk.clone(), b(b"cached"), ctx()).await;
        assert_eq!(store.get(&sk).await, Some(b(b"cached")));

        // Genuinely-absent key returns None.
        assert_eq!(store.get(&session_key(b"k", 0, 99)).await, None);
    }

    /// `range` overlays the cache on the inner store over a raw key range: cache
    /// wins on collision and a cached tombstone hides the inner value.
    #[tokio::test]
    async fn range_merges_cache_over_inner_with_tombstone() {
        let mut inner = InMemoryBytes::default();
        let k0 = session_key(b"k", 0, 10);
        let k1 = session_key(b"k", 0, 20);
        let k2 = session_key(b"k", 0, 30);
        inner.put(k0.clone(), b(b"i0")).await;
        inner.put(k1.clone(), b(b"i1")).await;
        inner.put(k2.clone(), b(b"i2")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));

        store.put(k1.clone(), b(b"c1"), ctx()).await; // cache wins over i1
        store.remove(k2.clone(), ctx()).await; // tombstone hides i2

        let lo = session_key(b"k", 0, 0);
        let hi = session_key(b"k", i64::MAX, i64::MAX);
        let r = store.range(&lo, &hi).await;
        assert_eq!(r, vec![(k0, b(b"i0")), (k1, b(b"c1"))]);
    }

    /// `scan_all` overlays the full cache on the full inner store: cache wins on
    /// collision, a cache-only entry is added, and a cached tombstone hides the
    /// inner value.
    #[tokio::test]
    async fn scan_all_merges_cache_and_underlying() {
        let mut inner = InMemoryBytes::default();
        let k0 = session_key(b"k", 0, 10);
        let k1 = session_key(b"k", 0, 20);
        let k3 = session_key(b"k", 0, 40);
        inner.put(k0.clone(), b(b"i0")).await;
        inner.put(k1.clone(), b(b"i1")).await;
        inner.put(k3.clone(), b(b"i3")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));

        store.put(k1.clone(), b(b"c1"), ctx()).await; // cache wins
        let k2 = session_key(b"k", 0, 30);
        store.put(k2.clone(), b(b"c2"), ctx()).await; // cache-only
        store.remove(k3.clone(), ctx()).await; // tombstone hides inner i3

        let r = store.scan_all().await;
        assert_eq!(r, vec![(k0, b(b"i0")), (k1, b(b"c1")), (k2, b(b"c2"))]);
    }

    /// `put_inner` / `delete_inner` bypass the cache (no dirty entry staged).
    #[tokio::test]
    async fn put_and_delete_inner_bypass_the_cache() {
        let store = CachingSessionStore::new(cache(), Box::new(InMemoryBytes::default()));
        let sk = session_key(b"k", 0, 10);

        store.put_inner(sk.clone(), b(b"v")).await;
        assert_eq!(store.get(&sk).await, Some(b(b"v")));
        assert!(store.flush().await.is_empty());

        store.delete_inner(&sk).await;
        assert_eq!(store.get(&sk).await, None);
        assert!(store.flush().await.is_empty());
    }

    /// `clear` empties both the cache layer and the inner store.
    #[tokio::test]
    async fn clear_empties_cache_and_inner() {
        let mut inner = InMemoryBytes::default();
        inner.put(session_key(b"k", 0, 10), b(b"i")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));
        store.put(session_key(b"k", 0, 20), b(b"c"), ctx()).await; // staged dirty

        store.clear().await;

        check!(store.find_sessions(b"k", 0, 1000).await.is_empty());
        check!(store.scan_all().await.is_empty());
        check!(store.flush().await.is_empty());
    }

    /// `flush` deletes a staged tombstone through to the inner store.
    #[tokio::test]
    async fn flush_deletes_tombstone_through() {
        let mut inner = InMemoryBytes::default();
        let sk = session_key(b"k", 0, 10);
        inner.put(sk.clone(), b(b"old")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));

        store.remove(sk.clone(), ctx()).await;
        let flushed = store.flush().await;
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].1.value, None);

        // Inner value deleted through.
        assert_eq!(store.get(&sk).await, None);
    }

    /// `flush_with_old` captures the inner OLD value before writing the staged new
    /// value through.
    #[tokio::test]
    async fn flush_with_old_returns_inner_old_then_writes_through() {
        let mut inner = InMemoryBytes::default();
        let sk = session_key(b"k", 0, 10);
        inner.put(sk.clone(), b(b"old")).await;
        let store = CachingSessionStore::new(cache(), Box::new(inner));
        // Stage a new value for the same session key.
        store.put(sk.clone(), b(b"new"), ctx()).await;

        let drained = store.flush_with_old().await;
        assert_eq!(drained.len(), 1);
        let (k, old, new, _ctx) = &drained[0];
        assert_eq!(k, &sk);
        assert_eq!(old.as_ref(), Some(&b(b"old"))); // inner OLD captured pre-write
        assert_eq!(new.as_ref(), Some(&b(b"new")));
        // Write-through landed; the (now-clean) merge reads the new value.
        assert_eq!(
            store.find_sessions(b"k", 0, 100).await,
            vec![(0, 10, b(b"new"))]
        );
    }
}
