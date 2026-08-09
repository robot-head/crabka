//! `CachingWindowStore`: window-schema-keyed write-back cache layered over a
//! windowed byte store.
//!
//! This module ports Kafka `CachingWindowStore`. The cache is a [`NamedCache`]
//! keyed by the *windowed store-key bytes*, which are exactly the
//! [`window_schema::store_key`](crate::store::window_schema::store_key) layout
//! `key ‖ windowStart:8B BE ‖ seqnum:4B BE`. The cache and the inner byte store
//! share one key space, so `fetch` can merge the two over a windowed-key byte
//! range. Cache values are the *wrapped* value bytes `recordTs ‖ serialized`
//! that the inner store also holds, so a flush can write an entry through
//! verbatim.
//!
//! ## Inner store
//! The wrapped store is the raw [`ByteKeyValueStore`] backend, keyed by windowed
//! store-key bytes. It is not the typed
//! [`WindowBytesStore`](crate::store::window::WindowBytesStore). The cache works
//! purely in bytes, which matches Kafka, where `CachingWindowStore` wraps a
//! `WindowStore<Bytes, Bytes>`. `put` and `delete` on the backend take
//! `&mut self` and are `async`, so a `tokio::sync::Mutex` holds the inner store
//! and its guard is await-safe. The wrapper exposes `&self` methods.
//!
//! ## Locking / async
//! The `cache` is a `std::sync::Mutex`, and its guard is never held across
//! `.await`. `flush` collects the dirty entries under the cache lock, releases
//! the lock, then writes each entry through to the inner store. The `inner` store
//! uses an async-aware `tokio::sync::Mutex`, so its guard may legally span the
//! `async` backend calls.
//!
//! ## Ordered iteration
//! An ordered map backs [`NamedCache`], so it exposes two ascending-memcmp scans:
//! [`range`](NamedCache::range), bounded by `[lo, hi)`, and
//! [`all`](NamedCache::all), unbounded. The wrapper drives `fetch` off `range`
//! over the windowed-key bounds, and `fetch_all` off `all`. It then filters each
//! result by key prefix and window start. There is no parallel key index.

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
        window_schema::{key_bytes_of, store_key, window_start_of},
    },
};

/// Write-back caching wrapper over a windowed byte store. The cache maps
/// windowed store-key bytes (`window_schema::store_key`) to wrapped value bytes.
pub(crate) struct CachingWindowStore {
    cache: Arc<Mutex<NamedCache>>,
    inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>>,
    /// Cache name, captured so `clear` can rebuild an empty [`NamedCache`]
    /// under the same identity. This matches `CachingKeyValueStore`.
    name: String,
}

impl CachingWindowStore {
    pub fn new(
        cache: Arc<Mutex<NamedCache>>,
        inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>>,
    ) -> Self {
        Self {
            cache,
            inner,
            name: String::new(),
        }
    }

    /// Like [`new`](Self::new) but records the cache's name for `clear`.
    pub fn with_name(
        cache: Arc<Mutex<NamedCache>>,
        inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>>,
        name: String,
    ) -> Self {
        Self { cache, inner, name }
    }

    /// Cache-first single windowed-store-key read.
    ///
    /// A cache hit wins, including a dirty `None` tombstone. Otherwise the read
    /// falls through to the inner store.
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

    /// Merged raw windowed-key range `[lo, hi)`, with the cache over the inner.
    ///
    /// Cache entries win on a key collision, and a cached tombstone hides the
    /// inner value. This method returns key-sorted `(store_key, wrapped_value)`
    /// pairs. It backs the typed store's `fetch`, `fetch_with_ts`, and IQ range
    /// reads.
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

    /// Merged unbounded scan, with the cache over every inner entry.
    ///
    /// The cache wins on a key collision, and a cached tombstone hides the inner
    /// value. This method returns key-sorted `(store_key, wrapped_value)` pairs.
    /// It backs the typed store's `fetch_all_in_range` and range-scan IQ reads.
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

    /// Write straight through to the inner store and bypass the cache.
    ///
    /// This is the restore path. It matches `CachingKeyValueStore::put_inner`.
    pub async fn put_inner(&self, key: Bytes, value: Bytes) {
        self.inner.lock().await.put(key, value).await;
    }

    /// Delete straight through to the inner store and bypass the cache (restore).
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

    /// Write-back a windowed entry into the cache.
    ///
    /// `key_schema_bytes` is the windowed store-key
    /// `store_key(key, windowStart, seqnum)`. `value` is the wrapped value
    /// `recordTs ‖ serialized`. The cache marks the entry dirty, and `flush`
    /// later writes it through.
    pub fn put(&self, key_schema_bytes: Bytes, value: Bytes, ctx: RecordContext) {
        let mut cache = self.cache.lock().unwrap();
        cache.put(key_schema_bytes, LruCacheEntry::new(Some(value), true, ctx));
    }

    /// Tombstone a windowed key in the cache with a dirty `None`.
    ///
    /// The tombstone hides any inner value until `flush` deletes it through.
    pub fn delete(&self, key_schema_bytes: Bytes, ctx: RecordContext) {
        let mut cache = self.cache.lock().unwrap();
        cache.delete(key_schema_bytes, ctx);
    }

    /// Cache-first windowed fetch for a single key over window starts in
    /// `[time_from, time_to]`.
    ///
    /// This method merges cache entries with the inner store over the
    /// windowed-key byte range. Cache values win on a collision, and cache
    /// tombstones hide the inner value. It returns
    /// `(windowed_store_key, wrapped_value)` pairs in ascending windowed-key
    /// memcmp order, which matches the inner store's order.
    pub async fn fetch(&self, key: &[u8], time_from: i64, time_to: i64) -> Vec<(Bytes, Bytes)> {
        let lo = store_key(key, time_from, 0);
        let hi = store_key(key, time_to.saturating_add(1), 0);

        // Snapshot cache entries whose windowed key falls in [lo, hi) and whose
        // inner key prefix matches `key` (guard against prefix collisions). The
        // range is collected under the lock and the guard dropped before any await.
        let cached: Vec<(Bytes, LruCacheEntry)> = {
            let cache = self.cache.lock().unwrap();
            cache
                .range(&lo, &hi)
                .into_iter()
                .filter(|(k, _)| key_bytes_of(k) == key)
                .collect()
        };

        let from_inner: Vec<(Bytes, Bytes)> = self
            .inner
            .lock()
            .await
            .range(&lo, &hi)
            .await
            .into_iter()
            .filter(|(k, _)| key_bytes_of(k) == key)
            .collect();

        merge(cached, from_inner)
    }

    /// Cache-first fetch across ALL keys for window starts in
    /// `[time_from, time_to]`.
    ///
    /// This method matches Kafka `fetchAll`. It merges the cache and the inner
    /// store in windowed-key order, then filters by window start. The window
    /// start is a suffix of the key, so this is a filtered full scan. The cache
    /// wins on a collision, and tombstones hide the inner value.
    pub async fn fetch_all(&self, time_from: i64, time_to: i64) -> Vec<(Bytes, Bytes)> {
        let in_range = |k: &[u8]| {
            let ws = window_start_of(k);
            ws >= time_from && ws <= time_to
        };

        // Snapshot all cache entries whose window start is in range. The full
        // ordered scan is collected under the lock and the guard dropped before
        // any await.
        let cached: Vec<(Bytes, LruCacheEntry)> = {
            let cache = self.cache.lock().unwrap();
            cache
                .all()
                .into_iter()
                .filter(|(k, _)| in_range(k))
                .collect()
        };

        let from_inner: Vec<(Bytes, Bytes)> = self
            .inner
            .lock()
            .await
            .scan_all()
            .await
            .into_iter()
            .filter(|(k, _)| in_range(k))
            .collect();

        merge(cached, from_inner)
    }

    /// Drain the dirty entries in cache insertion order.
    ///
    /// This method writes each entry through to the inner store. It puts the
    /// wrapped value, or deletes on a tombstone. It then returns the entries, so
    /// the caller can forward them downstream and to the changelog.
    pub async fn flush(&self) -> Vec<(Bytes, LruCacheEntry)> {
        let mut collected: Vec<(Bytes, LruCacheEntry)> = Vec::new();
        {
            let mut cache = self.cache.lock().unwrap();
            let mut listener =
                |k: &Bytes, e: &LruCacheEntry| collected.push((k.clone(), e.clone()));
            cache.flush(&mut listener);
        }
        let mut inner = self.inner.lock().await;
        for (k, e) in &collected {
            // No windowed-key decoding is needed for write-through: the inner
            // byte store is keyed by the same `window_schema::store_key` bytes
            // the cache holds, so the key forwards verbatim.
            match &e.value {
                Some(v) => inner.put(k.clone(), v.clone()).await,
                None => {
                    inner.delete(k).await;
                }
            }
        }
        drop(inner);
        collected
    }

    /// Flush the dirty entries in insertion order.
    ///
    /// This method captures the inner OLD wrapped value BEFORE each
    /// write-through. For each entry it reads `old = inner.get(&k)`, writes the
    /// new value through with `put`, or with `delete` on a tombstone, and returns
    /// `(windowed_store_key, old, new, context)`. `old` and `new` are the
    /// *wrapped* value bytes `recordTs ‖ serialized`, and `None` means absent or
    /// tombstone. This method matches `CachingKeyValueStore::flush_with_old`. The
    /// typed `WindowBytesStore` decodes the windowed key and unwraps the values
    /// to build the deduped downstream `Change`.
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

/// Merge the cached entries with the inner entries.
///
/// The cache wins, and a tombstone hides the inner value. The result is
/// `(windowed_key, wrapped_value)` pairs in ascending windowed-key order.
fn merge(
    cached: Vec<(Bytes, LruCacheEntry)>,
    from_inner: Vec<(Bytes, Bytes)>,
) -> Vec<(Bytes, Bytes)> {
    use std::collections::BTreeMap;
    // BTreeMap over Bytes keys yields ascending memcmp order — the same order
    // the inner byte store ranges in.
    let mut merged: BTreeMap<Bytes, Bytes> = from_inner.into_iter().collect();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{byte::InMemoryBytes, window_schema::wrap_value};

    fn ctx() -> RecordContext {
        RecordContext {
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        }
    }

    fn store() -> CachingWindowStore {
        let cache = Arc::new(Mutex::new(NamedCache::new("w".to_string())));
        let inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>> =
            Arc::new(AsyncMutex::new(Box::new(InMemoryBytes::default())));
        CachingWindowStore::new(cache, inner)
    }

    // A wrapped value (recordTs ‖ raw) keyed by a windowed store-key.
    fn wrapped(record_ts: i64, raw: &[u8]) -> Bytes {
        wrap_value(record_ts, raw)
    }

    async fn inner_put(s: &CachingWindowStore, k: Bytes, v: Bytes) {
        s.inner.lock().await.put(k, v).await;
    }

    async fn inner_get(s: &CachingWindowStore, k: &Bytes) -> Option<Bytes> {
        s.inner.lock().await.get(k).await
    }

    #[tokio::test]
    async fn fetch_returns_cached() {
        let s = store();
        let key = b"a";
        let sk = store_key(key, 0, 0);
        let val = wrapped(5, b"v1");
        s.put(sk.clone(), val.clone(), ctx());

        // Inner is empty; fetch must return the cached entry.
        let got = s.fetch(key, 0, 100).await;
        assert_eq!(got, vec![(sk, val)]);
    }

    #[tokio::test]
    async fn flush_writes_through_and_returns_entries() {
        let s = store();
        let key = b"a";
        let sk = store_key(key, 0, 0);
        let val = wrapped(5, b"v1");
        s.put(sk.clone(), val.clone(), ctx());

        let flushed = s.flush().await;
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, sk);
        assert_eq!(flushed[0].1.value, Some(val.clone()));

        // Inner now contains the written-through entry.
        assert_eq!(inner_get(&s, &sk).await, Some(val));
    }

    #[tokio::test]
    async fn flush_deletes_tombstone_through() {
        let s = store();
        let key = b"a";
        let sk = store_key(key, 0, 0);
        // Seed the inner store, then tombstone in the cache and flush.
        inner_put(&s, sk.clone(), wrapped(1, b"old")).await;
        s.delete(sk.clone(), ctx());

        let flushed = s.flush().await;
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].1.value, None);

        assert_eq!(
            inner_get(&s, &sk).await,
            None,
            "tombstone deleted the inner value"
        );
    }

    #[tokio::test]
    async fn fetch_merges_cache_and_underlying_in_window_order() {
        let s = store();
        let key = b"a";

        // Inner holds the first window and the last window.
        let win_lo = store_key(key, 0, 0);
        let win_mid = store_key(key, 10, 0);
        let win_hi = store_key(key, 20, 0);
        inner_put(&s, win_lo.clone(), wrapped(1, b"inner-lo")).await;
        inner_put(&s, win_hi.clone(), wrapped(2, b"inner-hi")).await;

        // Cache holds a NEW middle window, plus an UPDATED value for the
        // overlapping first window (cache must win on collision).
        let cache_lo = wrapped(9, b"cache-lo");
        let cache_mid = wrapped(9, b"cache-mid");
        s.put(win_lo.clone(), cache_lo.clone(), ctx());
        s.put(win_mid.clone(), cache_mid.clone(), ctx());

        // Single-key fetch over [0, 20]: merged, cache-wins, window-key order.
        let got = s.fetch(key, 0, 20).await;
        assert_eq!(
            got,
            vec![
                (win_lo.clone(), cache_lo.clone()),   // cache wins over inner-lo
                (win_mid.clone(), cache_mid.clone()), // cache-only middle window
                (win_hi.clone(), wrapped(2, b"inner-hi")), // inner-only last window
            ]
        );

        // fetch_all over the same start range returns the same merged set.
        let got_all = s.fetch_all(0, 20).await;
        assert_eq!(
            got_all,
            vec![
                (win_lo, cache_lo),
                (win_mid, cache_mid),
                (win_hi, wrapped(2, b"inner-hi")),
            ]
        );
    }

    #[tokio::test]
    async fn fetch_tombstone_hides_inner() {
        let s = store();
        let key = b"a";
        let sk = store_key(key, 0, 0);
        inner_put(&s, sk.clone(), wrapped(1, b"inner")).await;
        s.delete(sk.clone(), ctx());

        let got = s.fetch(key, 0, 100).await;
        assert!(got.is_empty(), "cache tombstone hides the inner value");
    }

    #[tokio::test]
    async fn fetch_all_filters_by_window_start() {
        let s = store();
        // Two keys, window starts 0 and 50.
        let a0 = store_key(b"a", 0, 0);
        let b50 = store_key(b"b", 50, 0);
        s.put(a0.clone(), wrapped(1, b"a0"), ctx());
        inner_put(&s, b50.clone(), wrapped(2, b"b50")).await;

        // Range [0, 10] excludes window 50.
        let got = s.fetch_all(0, 10).await;
        assert_eq!(got, vec![(a0.clone(), wrapped(1, b"a0"))]);

        // Range [0, 50] includes both, in windowed-key order (a@0 < b@50).
        let got_both = s.fetch_all(0, 50).await;
        assert_eq!(
            got_both,
            vec![(a0, wrapped(1, b"a0")), (b50, wrapped(2, b"b50"))]
        );
    }

    #[tokio::test]
    async fn flush_with_old_returns_inner_old_then_writes_through() {
        let s = store();
        let key = b"a";
        let sk = store_key(key, 0, 0);
        // Seed a committed value in the inner store.
        inner_put(&s, sk.clone(), wrapped(1, b"old")).await;
        // Stage a new value in the cache.
        s.put(sk.clone(), wrapped(2, b"new"), ctx());

        let drained = s.flush_with_old().await;
        assert_eq!(drained.len(), 1);
        let (k, old, new, _ctx) = &drained[0];
        assert_eq!(k, &sk);
        assert_eq!(old.as_ref(), Some(&wrapped(1, b"old"))); // inner OLD captured pre-write
        assert_eq!(new.as_ref(), Some(&wrapped(2, b"new")));
        // Write-through landed in the inner store.
        assert_eq!(inner_get(&s, &sk).await, Some(wrapped(2, b"new")));
    }

    /// `flush_with_old` on a tombstone returns `new = None` and deletes the inner
    /// wrapped value through. This is the tombstone arm.
    #[tokio::test]
    async fn flush_with_old_tombstone_deletes_through() {
        let s = store();
        let key = b"a";
        let sk = store_key(key, 0, 0);
        inner_put(&s, sk.clone(), wrapped(1, b"old")).await;
        s.delete(sk.clone(), ctx());

        let drained = s.flush_with_old().await;
        assert_eq!(drained.len(), 1);
        let (k, old, new, _ctx) = &drained[0];
        assert_eq!(k, &sk);
        assert_eq!(old.as_ref(), Some(&wrapped(1, b"old"))); // inner OLD captured
        assert_eq!(new.as_ref(), None); // tombstone
        assert_eq!(inner_get(&s, &sk).await, None); // deleted through
    }

    /// `range` overlays the cache on the inner store over a raw windowed-key
    /// range. The cache wins on a collision, and a cached tombstone hides the
    /// inner value. `range` backs the typed store's `fetch` and IQ range reads.
    #[tokio::test]
    async fn range_merges_cache_over_inner_with_tombstone() {
        let s = store();
        let key = b"a";
        let k0 = store_key(key, 0, 0);
        let k1 = store_key(key, 10, 0);
        let k2 = store_key(key, 20, 0);
        inner_put(&s, k0.clone(), wrapped(1, b"i0")).await;
        inner_put(&s, k1.clone(), wrapped(1, b"i1")).await;
        inner_put(&s, k2.clone(), wrapped(1, b"i2")).await;

        s.put(k1.clone(), wrapped(9, b"c1"), ctx()); // cache wins over i1
        s.delete(k2.clone(), ctx()); // tombstone hides i2

        let lo = store_key(key, 0, 0);
        let hi = store_key(key, i64::MAX, 0);
        let r = s.range(&lo, &hi).await;
        assert_eq!(r, vec![(k0, wrapped(1, b"i0")), (k1, wrapped(9, b"c1"))]);
    }

    /// `scan_all` overlays the full cache on the full inner store. The cache wins
    /// on a collision, a cache-only entry is added, and a cached tombstone hides
    /// the inner value.
    #[tokio::test]
    async fn scan_all_merges_cache_and_underlying() {
        let s = store();
        let key = b"a";
        let k0 = store_key(key, 0, 0);
        let k1 = store_key(key, 10, 0);
        let k3 = store_key(key, 30, 0);
        inner_put(&s, k0.clone(), wrapped(1, b"i0")).await;
        inner_put(&s, k1.clone(), wrapped(1, b"i1")).await;
        inner_put(&s, k3.clone(), wrapped(1, b"i3")).await;

        s.put(k1.clone(), wrapped(9, b"c1"), ctx()); // cache wins
        let k2 = store_key(key, 20, 0);
        s.put(k2.clone(), wrapped(9, b"c2"), ctx()); // cache-only
        s.delete(k3.clone(), ctx()); // tombstone hides inner i3

        let r = s.scan_all().await;
        assert_eq!(
            r,
            vec![
                (k0, wrapped(1, b"i0")),
                (k1, wrapped(9, b"c1")),
                (k2, wrapped(9, b"c2")),
            ]
        );
    }

    /// `put_inner` and `delete_inner` bypass the cache and stage no dirty entry.
    #[tokio::test]
    async fn put_and_delete_inner_bypass_the_cache() {
        let s = store();
        let sk = store_key(b"a", 0, 0);

        s.put_inner(sk.clone(), wrapped(1, b"v")).await;
        assert_eq!(s.get(&sk).await, Some(wrapped(1, b"v")));
        assert!(s.flush().await.is_empty());

        s.delete_inner(&sk).await;
        assert_eq!(s.get(&sk).await, None);
        assert!(s.flush().await.is_empty());
    }

    /// `clear` empties both the cache layer and the inner store.
    #[tokio::test]
    async fn clear_empties_cache_and_inner() {
        let s = store();
        let k0 = store_key(b"a", 0, 0);
        inner_put(&s, k0.clone(), wrapped(1, b"i")).await;
        s.put(store_key(b"a", 10, 0), wrapped(9, b"c"), ctx()); // staged dirty

        s.clear().await;

        assert!(s.scan_all().await.is_empty());
        assert!(s.flush().await.is_empty());
        assert_eq!(s.get(&k0).await, None);
    }
}
