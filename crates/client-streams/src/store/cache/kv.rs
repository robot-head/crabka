//! `CachingKeyValueStore`: byte-level write-back caching wrapper over a
//! `ByteKeyValueStore`. Ports Kafka `CachingKeyValueStore`.
//!
//! Reads are cache-first: a cache hit (including a tombstone, i.e. a dirty
//! `None`) wins over the underlying store. Writes are write-back: `put`/`delete`
//! only stage a dirty entry in the cache and are pushed THROUGH to the inner
//! store on `flush`. `flush` also returns the drained dirty entries so the
//! caller can forward them downstream (Kafka's flush listener).
//!
//! ## Inner mutability / locking
//!
//! [`ByteKeyValueStore`]'s `put`/`delete` take `&mut self`, so `inner` is held
//! behind a `tokio::sync::Mutex` — its async methods are awaited while the guard
//! is held, which a `std::sync::Mutex` guard cannot do (`await_holding_lock`).
//! The cache, whose ops are synchronous, uses a plain `std::sync::Mutex` whose
//! guard is always dropped before any `.await`. All public methods take `&self`
//! so the store can be shared.
//!
//! ## Merged `range`
//!
//! [`NamedCache::range`] yields the staged entries whose key falls in `[lo, hi)`
//! in ascending memcmp order, so the merge enumerates cache candidates directly
//! off the cache (no shadow key set). Cache entries win on key collision and a
//! cached tombstone hides the inner value.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    processor::record::RecordContext,
    store::{
        byte::ByteKeyValueStore,
        cache::{entry::LruCacheEntry, named::NamedCache},
    },
};

pub(crate) struct CachingKeyValueStore {
    cache: Arc<Mutex<NamedCache>>,
    inner: AsyncMutex<Box<dyn ByteKeyValueStore>>,
    /// Cache name, captured so `clear` can rebuild an empty [`NamedCache`]
    /// (which has no in-place reset) under the same identity.
    name: String,
}

impl CachingKeyValueStore {
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

    /// Cache-first read: a cache hit (including a tombstone, a dirty `None`)
    /// wins; otherwise fall through to the inner store.
    pub async fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::copy_from_slice(key);
        // Take the cached value out of the guard before any await.
        let cached = {
            let mut cache = self.cache.lock().unwrap();
            cache.get_promote(&key).map(|e| e.value.clone())
        };
        match cached {
            // Cache hit: `Some(value)` is a live value, `None` is a tombstone —
            // both authoritative, so we never consult the inner store.
            Some(value) => value,
            // Cache miss: fall through to the underlying store.
            None => self.inner.lock().await.get(&key).await,
        }
    }

    /// Write-back put: stage a dirty entry in the cache carrying its context.
    /// The inner store is not touched until `flush`.
    ///
    /// `async` is part of the store-wrapper contract (the typed `put` path is
    /// async end to end) even though a pure cache write needs no `.await`.
    #[allow(clippy::unused_async)]
    pub async fn put(&self, key: Bytes, value: Bytes, ctx: RecordContext) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(key, LruCacheEntry::new(Some(value), true, ctx));
    }

    /// Write-back delete: stage a dirty tombstone (`None`) in the cache.
    #[allow(clippy::unused_async)]
    pub async fn delete(&self, key: Bytes, ctx: RecordContext) {
        let mut cache = self.cache.lock().unwrap();
        cache.delete(key, ctx);
    }

    /// Merged range over `[lo, hi)`: the cache layer is overlaid on the inner
    /// store. Cache entries win on key collision, and a cached tombstone hides
    /// the inner value (the key is omitted). Returns key-sorted `(key, value)`.
    pub async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        // Seed the merged view from the inner store.
        let mut merged: BTreeMap<Bytes, Bytes> = {
            let inner = self.inner.lock().await;
            inner.range(lo, hi).await.into_iter().collect()
        };
        // Overlay the cache entries staged in `[lo, hi)`, collected under the lock
        // and dropped before any await. Cache wins; a tombstone hides the inner
        // value.
        let cached = {
            let cache = self.cache.lock().unwrap();
            cache.range(lo, hi)
        };
        for (k, e) in cached {
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
        merged.into_iter().collect()
    }

    /// Flush: drain dirty entries in insertion order, write each THROUGH to the
    /// inner store (`put` the value, `delete` on a tombstone), clear dirty, and
    /// return the drained entries so the caller can forward them downstream.
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

    /// Write straight through to the inner store, bypassing the cache. Used by
    /// the restore path (`apply_changelog`), which replays the committed
    /// changelog into the state below the cache without staging dirty entries.
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

    /// Merged unbounded scan: every inner entry overlaid with the cache. Cache
    /// entries win on key collision and a cached tombstone hides the inner value.
    /// Returns key-sorted `(key, value)`.
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

    /// Flush dirty entries in insertion order, capturing the inner OLD value
    /// BEFORE each write-through. For each entry: read `old = inner.get(&k)`,
    /// then write the new value through (`put` / `delete` on a tombstone), and
    /// return `(key, old, new, context)`. `old`/`new` are `Option<Bytes>`
    /// (`None` = absent / tombstone). The typed store uses `old` (the
    /// last-committed value) to build the deduped downstream `Change` and the
    /// context to stamp the forwarded record.
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

    #[tokio::test]
    async fn get_returns_cached_before_underlying() {
        let inner = InMemoryBytes::default();
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        store.put(b(b"k"), b(b"v"), ctx()).await;

        // Cached only — not yet flushed to inner.
        assert_eq!(store.get(b"k").await, Some(b(b"v")));
    }

    #[tokio::test]
    async fn flush_writes_through_and_returns_entries() {
        let store = CachingKeyValueStore::new(cache(), Box::new(InMemoryBytes::default()));

        store.put(b(b"k"), b(b"v"), ctx()).await;
        let flushed = store.flush().await;

        assert_eq!(flushed.len(), 1);
        assert_eq!(&flushed[0].0, &b(b"k"));
        assert_eq!(&flushed[0].1.value, &Some(b(b"v")));

        // Inner now has the write-through value; serve it from the (now-clean)
        // cache or inner — either way `get` returns it.
        assert_eq!(store.get(b"k").await, Some(b(b"v")));
    }

    #[tokio::test]
    async fn tombstone_hides_underlying() {
        // Seed inner with k -> v0.
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"k"), b(b"v0")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        // Sanity: inner value is visible before the delete.
        assert_eq!(store.get(b"k").await, Some(b(b"v0")));

        store.delete(b(b"k"), ctx()).await;
        // Cached tombstone hides the inner value.
        assert_eq!(store.get(b"k").await, None);

        // After flush, the inner store no longer has the key.
        store.flush().await;
        assert_eq!(store.get(b"k").await, None);
    }

    #[tokio::test]
    async fn range_merges_cache_and_underlying_cache_wins() {
        // Inner: a -> 1, c -> 3.
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"a"), b(b"1")).await;
        inner.put(b(b"c"), b(b"3")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        // Cache: b -> 2 (new), a -> 9 (overrides inner).
        store.put(b(b"b"), b(b"2"), ctx()).await;
        store.put(b(b"a"), b(b"9"), ctx()).await;

        // Range [a, d) covers a, b, c.
        let r = store.range(b"a", b"d").await;
        assert_eq!(
            r,
            vec![
                (b(b"a"), b(b"9")), // cache wins over inner's a -> 1
                (b(b"b"), b(b"2")), // cache-only
                (b(b"c"), b(b"3")), // inner-only
            ]
        );
    }

    #[tokio::test]
    async fn range_tombstone_omits_key() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"a"), b(b"1")).await;
        inner.put(b(b"b"), b(b"2")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        store.delete(b(b"a"), ctx()).await;

        let r = store.range(b"a", b"c").await;
        assert_eq!(r, vec![(b(b"b"), b(b"2"))]);
    }

    /// A cache miss falls through to the inner store (the `None` arm of `get`).
    #[tokio::test]
    async fn get_falls_through_to_inner_on_miss() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"k"), b(b"inner")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        // Nothing staged in the cache for "k": the read falls through to inner.
        assert_eq!(store.get(b"k").await, Some(b(b"inner")));
        // A genuinely-absent key returns None from inner.
        assert_eq!(store.get(b"missing").await, None);
    }

    /// `scan_all` overlays the full cache on the full inner store: cache wins on
    /// collision and a cached tombstone hides the inner value.
    #[tokio::test]
    async fn scan_all_merges_cache_and_underlying() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"a"), b(b"1")).await;
        inner.put(b(b"b"), b(b"2")).await;
        inner.put(b(b"c"), b(b"3")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        store.put(b(b"b"), b(b"9"), ctx()).await; // overrides inner b -> 2
        store.put(b(b"d"), b(b"4"), ctx()).await; // cache-only
        store.delete(b(b"c"), ctx()).await; // tombstone hides inner c

        let r = store.scan_all().await;
        assert_eq!(
            r,
            vec![
                (b(b"a"), b(b"1")), // inner-only
                (b(b"b"), b(b"9")), // cache wins
                (b(b"d"), b(b"4")), // cache-only
            ]
        );
    }

    /// `put_inner` / `delete_inner` write straight through, bypassing the cache:
    /// no dirty entry is staged (a subsequent `flush` drains nothing).
    #[tokio::test]
    async fn put_and_delete_inner_bypass_the_cache() {
        let store = CachingKeyValueStore::new(cache(), Box::new(InMemoryBytes::default()));

        store.put_inner(b(b"k"), b(b"v")).await;
        // Visible via the cache-first read (falls through to inner) ...
        assert_eq!(store.get(b"k").await, Some(b(b"v")));
        // ... and no dirty entry was staged.
        assert!(store.flush().await.is_empty());

        store.delete_inner(b"k").await;
        assert_eq!(store.get(b"k").await, None);
        assert!(store.flush().await.is_empty());
    }

    /// `clear` empties both the cache layer (dropping staged dirty entries) and
    /// the inner store.
    #[tokio::test]
    async fn clear_empties_cache_and_inner() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"a"), b(b"1")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));
        store.put(b(b"b"), b(b"2"), ctx()).await; // staged dirty

        store.clear().await;

        // Both the staged entry and the inner value are gone.
        assert_eq!(store.get(b"a").await, None);
        assert_eq!(store.get(b"b").await, None);
        assert_eq!(store.scan_all().await.is_empty(), true);
        assert_eq!(store.flush().await.is_empty(), true);
    }

    /// `flush_with_old` reports `old = None` for a key with no prior inner value
    /// and `old = Some(..)` for one that does, then writes both through.
    #[tokio::test]
    async fn flush_with_old_distinguishes_absent_and_present_inner() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"present"), b(b"old")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        store.put(b(b"present"), b(b"new"), ctx()).await; // has inner old
        store.put(b(b"fresh"), b(b"v"), ctx()).await; // no inner old

        let mut drained = store.flush_with_old().await;
        // Sort by key for a deterministic assertion (insertion order is preserved
        // by the cache, but make the test independent of it).
        drained.sort_by(|a, b| a.0.cmp(&b.0));
        let actual = drained
            .iter()
            .map(|(key, old, new, _)| (key.clone(), old.clone(), new.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                (b(b"fresh"), None, Some(b(b"v"))),
                (b(b"present"), Some(b(b"old")), Some(b(b"new"))),
            ]
        );

        // Both write-throughs landed.
        assert_eq!(store.get(b"present").await, Some(b(b"new")));
        assert_eq!(store.get(b"fresh").await, Some(b(b"v")));
    }

    /// `flush_with_old` on a tombstone returns `new = None` and deletes the inner
    /// value through (the tombstone arm of the write-through).
    #[tokio::test]
    async fn flush_with_old_tombstone_deletes_through() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"k"), b(b"old")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        store.delete(b(b"k"), ctx()).await;

        let drained = store.flush_with_old().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(&drained[0].0, &b(b"k"));
        assert_eq!(&drained[0].1, &Some(b(b"old")));
        assert_eq!(&drained[0].2, &None);

        assert_eq!(store.get(b"k").await, None);
    }
}
