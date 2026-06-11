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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::processor::record::RecordContext;
use crate::store::byte::ByteKeyValueStore;
use crate::store::cache::entry::LruCacheEntry;
use crate::store::cache::named::NamedCache;

pub(crate) struct CachingKeyValueStore {
    cache: Arc<Mutex<NamedCache>>,
    inner: AsyncMutex<Box<dyn ByteKeyValueStore>>,
}

impl CachingKeyValueStore {
    pub fn new(cache: Arc<Mutex<NamedCache>>, inner: Box<dyn ByteKeyValueStore>) -> Self {
        Self {
            cache,
            inner: AsyncMutex::new(inner),
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
        assert_eq!(flushed[0].0, b(b"k"));
        assert_eq!(flushed[0].1.value, Some(b(b"v")));

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
}
