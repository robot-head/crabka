//! `CachingKeyValueStore`, a byte-level write-back caching wrapper over a
//! `ByteKeyValueStore`. It ports the Kafka `CachingKeyValueStore`.
//!
//! Reads are cache-first. A cache hit wins over the underlying store, and this
//! includes a tombstone, which is a dirty `None`. Writes are write-back.
//! `put` and `delete` only stage a dirty entry in the cache, and `flush` pushes
//! that entry THROUGH to the inner store. `flush` also returns the drained dirty
//! entries, so the caller can forward them downstream. This matches Kafka's
//! flush listener.
//!
//! ## Inner mutability / locking
//!
//! The `put` and `delete` of [`ByteKeyValueStore`] take `&mut self`, so `inner`
//! sits behind a `tokio::sync::Mutex`. The code awaits its async methods while
//! it holds the guard, and a `std::sync::Mutex` guard cannot do that, as
//! `await_holding_lock` shows. The cache operations are synchronous, so the
//! cache uses a plain `std::sync::Mutex`, and the code always drops that guard
//! before any `.await`. All public methods take `&self`, so the store can be
//! shared.
//!
//! ## Merged `range`
//!
//! [`NamedCache::range`] gives the staged entries whose key falls in `[lo, hi)`,
//! in ascending memcmp order. The merge therefore reads the cache candidates
//! directly off the cache and needs no shadow key set. A cache entry wins on a
//! key collision, and a cached tombstone hides the inner value.

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
    /// The cache name. The store captures it so `clear` can rebuild an empty
    /// [`NamedCache`] under the same identity, because a [`NamedCache`] has no
    /// in-place reset.
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

    /// Like [`new`](Self::new), but it records the cache's name for `clear`.
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

    /// Cache-first read. A cache hit wins, and this includes a tombstone, which
    /// is a dirty `None`. Without a cache hit, the read falls through to the
    /// inner store.
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

    /// Write-back put. It stages a dirty entry in the cache with its context.
    /// The inner store stays untouched until `flush`.
    pub fn put(&self, key: Bytes, value: Bytes, ctx: RecordContext) -> std::future::Ready<()> {
        let mut cache = self.cache.lock().unwrap();
        cache.put(key, LruCacheEntry::new(Some(value), true, ctx));
        std::future::ready(())
    }

    /// Write-back delete. It stages a dirty tombstone, which is a `None`, in the
    /// cache.
    pub fn delete(&self, key: Bytes, ctx: RecordContext) -> std::future::Ready<()> {
        let mut cache = self.cache.lock().unwrap();
        cache.delete(key, ctx);
        std::future::ready(())
    }

    /// Merged range over `[lo, hi)`, with the cache layer over the inner store.
    ///
    /// A cache entry wins on a key collision. A cached tombstone hides the inner
    /// value, and the result omits that key. The method returns key-sorted
    /// `(key, value)` pairs.
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

    /// Flush the cache.
    ///
    /// The method drains the dirty entries in insertion order and writes each one
    /// THROUGH to the inner store: `put` for a value and `delete` for a
    /// tombstone. It then clears the dirty set and returns the drained entries,
    /// so the caller can forward them downstream.
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

    /// Write straight through to the inner store and bypass the cache.
    ///
    /// The restore path (`apply_changelog`) uses this method. It replays the
    /// committed changelog into the state below the cache and stages no dirty
    /// entries.
    pub async fn put_inner(&self, key: Bytes, value: Bytes) {
        self.inner.lock().await.put(key, value).await;
    }

    /// Delete straight through to the inner store and bypass the cache. The
    /// restore path uses this method.
    pub async fn delete_inner(&self, key: &[u8]) {
        self.inner.lock().await.delete(key).await;
    }

    /// Clear both the cache layer and the inner store. This is the EOS rollback
    /// reset.
    pub async fn clear(&self) {
        {
            let mut cache = self.cache.lock().unwrap();
            *cache = NamedCache::new(self.name.clone());
        }
        self.inner.lock().await.clear().await;
    }

    /// Merged unbounded scan, with the cache over every inner entry.
    ///
    /// A cache entry wins on a key collision, and a cached tombstone hides the
    /// inner value. The method returns key-sorted `(key, value)` pairs.
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

    /// Flush the dirty entries in insertion order and capture the inner OLD
    /// value BEFORE each write-through.
    ///
    /// For each entry the method reads `old = inner.get(&k)`, then writes the new
    /// value through with `put`, or with `delete` for a tombstone. It returns
    /// `(key, old, new, context)`. `old` and `new` are `Option<Bytes>`, where
    /// `None` means absent or a tombstone. The typed store uses `old`, the
    /// last-committed value, to build the deduped downstream `Change`, and it
    /// uses the context to stamp the forwarded record.
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

    /// A cache miss falls through to the inner store, which is the `None` arm of
    /// `get`.
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

    /// `scan_all` puts the full cache over the full inner store. The cache wins
    /// on a collision, and a cached tombstone hides the inner value.
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

    /// `put_inner` and `delete_inner` write straight through and bypass the
    /// cache. They stage no dirty entry, so a later `flush` drains nothing.
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

    /// `clear` empties both the cache layer, which drops the staged dirty
    /// entries, and the inner store.
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
        assert!(store.scan_all().await.is_empty());
        // The cleared cache has no dirty entries to flush.
        assert!(store.flush().await.is_empty());
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
        assert_eq!(drained.len(), 2);

        let fresh = &drained[0];
        assert_eq!(fresh.0, b(b"fresh"));
        assert_eq!(fresh.1, None); // no prior inner value
        assert_eq!(fresh.2, Some(b(b"v")));

        let present = &drained[1];
        assert_eq!(present.0, b(b"present"));
        assert_eq!(present.1, Some(b(b"old"))); // prior inner value captured
        assert_eq!(present.2, Some(b(b"new")));

        // Both write-throughs landed.
        assert_eq!(store.get(b"present").await, Some(b(b"new")));
        assert_eq!(store.get(b"fresh").await, Some(b(b"v")));
    }

    /// `flush_with_old` on a tombstone returns `new = None` and deletes the
    /// inner value through. This is the tombstone arm of the write-through.
    #[tokio::test]
    async fn flush_with_old_tombstone_deletes_through() {
        let mut inner = InMemoryBytes::default();
        inner.put(b(b"k"), b(b"old")).await;
        let store = CachingKeyValueStore::new(cache(), Box::new(inner));

        store.delete(b(b"k"), ctx()).await;

        let drained = store.flush_with_old().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, b(b"k"));
        assert_eq!(drained[0].1, Some(b(b"old"))); // inner OLD captured
        assert_eq!(drained[0].2, None); // tombstone

        assert_eq!(store.get(b"k").await, None);
    }
}
