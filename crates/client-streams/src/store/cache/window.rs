//! `CachingWindowStore`: window-schema-keyed write-back cache layered over a
//! windowed byte store.
//!
//! Ports Kafka `CachingWindowStore`. The cache is a [`NamedCache`] keyed by the
//! *windowed store-key bytes* — exactly the
//! [`window_schema::store_key`](crate::store::window_schema::store_key) layout
//! (`key ‖ windowStart:8B BE ‖ seqnum:4B BE`) — so the cache and the inner byte
//! store share one key space and `fetch` can merge the two over a windowed-key
//! byte range. Cache values are the *wrapped* value bytes
//! (`recordTs ‖ serialized`) the inner store also holds, so a flushed entry can
//! be written through verbatim.
//!
//! ## Inner store
//! The wrapped store is the raw [`ByteKeyValueStore`] backend (keyed by windowed
//! store-key bytes), not the typed
//! [`WindowBytesStore`](crate::store::window::WindowBytesStore): the cache works
//! purely in bytes, mirroring Kafka where `CachingWindowStore` wraps a
//! `WindowStore<Bytes, Bytes>`. `put`/`delete` on the backend take `&mut self`
//! and are `async`, so the inner store is held behind a `tokio::sync::Mutex`
//! whose guard is await-safe; the wrapper exposes `&self` methods.
//!
//! ## Locking / async
//! The `cache` is a `std::sync::Mutex` whose guard is never held across `.await`:
//! `flush` collects the dirty entries under the cache lock, releases it, then
//! writes each through to the inner store. The `inner` store uses an async-aware
//! `tokio::sync::Mutex` so its guard may legally span the `async` backend calls.
//!
//! ## Ordered iteration
//! [`NamedCache`] is a pure LRU with only point `get` — it exposes no ordered
//! range iterator. The wrapper therefore keeps its own ordered `key_index` (a
//! `BTreeSet` of the windowed store-keys it has written) to drive the range/all
//! merges. The index is advisory: a key may sit in the index but have been
//! evicted from the cache, in which case `cache.get` returns `None` and the
//! window is served from the inner store instead.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::processor::record::RecordContext;
use crate::store::byte::ByteKeyValueStore;
use crate::store::cache::entry::LruCacheEntry;
use crate::store::cache::named::NamedCache;
use crate::store::window_schema::{key_bytes_of, store_key, window_start_of};

/// Write-back caching wrapper over a windowed byte store. The cache holds
/// windowed store-key bytes (`window_schema::store_key`) → wrapped value bytes.
pub(crate) struct CachingWindowStore {
    cache: Arc<Mutex<NamedCache>>,
    inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>>,
    /// Ordered set of windowed store-keys written through this wrapper, used to
    /// drive range/all merges over the point-only [`NamedCache`].
    key_index: Mutex<BTreeSet<Bytes>>,
}

impl CachingWindowStore {
    pub fn new(
        cache: Arc<Mutex<NamedCache>>,
        inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>>,
    ) -> Self {
        Self {
            cache,
            inner,
            key_index: Mutex::new(BTreeSet::new()),
        }
    }

    /// Write-back a windowed entry into the cache. `key_schema_bytes` is the
    /// windowed store-key (`store_key(key, windowStart, seqnum)`); `value` is the
    /// wrapped value (`recordTs ‖ serialized`). The entry is marked dirty so it
    /// is later written through on `flush`.
    pub fn put(&self, key_schema_bytes: Bytes, value: Bytes, ctx: RecordContext) {
        self.key_index
            .lock()
            .unwrap()
            .insert(key_schema_bytes.clone());
        let mut cache = self.cache.lock().unwrap();
        cache.put(key_schema_bytes, LruCacheEntry::new(Some(value), true, ctx));
    }

    /// Tombstone a windowed key in the cache (dirty `None`), hiding any inner
    /// value until flush deletes it through.
    pub fn delete(&self, key_schema_bytes: Bytes, ctx: RecordContext) {
        self.key_index
            .lock()
            .unwrap()
            .insert(key_schema_bytes.clone());
        let mut cache = self.cache.lock().unwrap();
        cache.delete(key_schema_bytes, ctx);
    }

    /// Cache-first windowed fetch for a single key over window starts in
    /// `[time_from, time_to]`. Merges cache entries with the inner store over the
    /// windowed-key byte range; cache values win on collision and cache
    /// tombstones hide the inner value. Results are returned as
    /// `(windowed_store_key, wrapped_value)` in ascending windowed-key (memcmp)
    /// order, matching the inner store's ordering.
    pub async fn fetch(&self, key: &[u8], time_from: i64, time_to: i64) -> Vec<(Bytes, Bytes)> {
        let lo = store_key(key, time_from, 0);
        let hi = store_key(key, time_to.saturating_add(1), 0);

        // Snapshot cache entries whose windowed key falls in [lo, hi) and whose
        // inner key prefix matches `key` (guard against prefix collisions).
        let cached = self.snapshot_cache_range(&lo, &hi, Some(key));

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
    /// `[time_from, time_to]`. Mirrors Kafka `fetchAll`: a windowed-key-ordered
    /// merge of cache + inner, filtered by window start (a suffix of the key, so
    /// this is a filtered full scan). Cache wins on collision, tombstones hide
    /// the inner value.
    pub async fn fetch_all(&self, time_from: i64, time_to: i64) -> Vec<(Bytes, Bytes)> {
        let in_range = |k: &[u8]| {
            let ws = window_start_of(k);
            ws >= time_from && ws <= time_to
        };

        // Snapshot all cache entries whose window start is in range.
        let cached = self.snapshot_cache_filtered(&in_range);

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

    /// Drain dirty entries (cache insertion order), writing each through to the
    /// inner store (put wrapped value, or delete on tombstone), and return them
    /// so the caller can forward them downstream / to the changelog.
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

    /// Snapshot cache entries whose key is in `[lo, hi)` (memcmp), optionally
    /// requiring the inner key prefix to equal `key_prefix`. Driven by the
    /// ordered `key_index`; entries evicted from the cache are skipped.
    fn snapshot_cache_range(
        &self,
        lo: &Bytes,
        hi: &Bytes,
        key_prefix: Option<&[u8]>,
    ) -> Vec<(Bytes, LruCacheEntry)> {
        let index = self.key_index.lock().unwrap();
        let cache = self.cache.lock().unwrap();
        index
            .range(lo.clone()..hi.clone())
            .filter(|k| key_prefix.is_none_or(|p| key_bytes_of(k) == p))
            .filter_map(|k| cache.get(k).map(|e| (k.clone(), e.clone())))
            .collect()
    }

    /// Snapshot every live cache entry whose key passes `pred`. Driven by the
    /// ordered `key_index`; entries evicted from the cache are skipped.
    fn snapshot_cache_filtered(
        &self,
        pred: &impl Fn(&[u8]) -> bool,
    ) -> Vec<(Bytes, LruCacheEntry)> {
        let index = self.key_index.lock().unwrap();
        let cache = self.cache.lock().unwrap();
        index
            .iter()
            .filter(|k| pred(k))
            .filter_map(|k| cache.get(k).map(|e| (k.clone(), e.clone())))
            .collect()
    }
}

/// Merge cached entries (cache wins, tombstone hides inner) with inner entries,
/// returning `(windowed_key, wrapped_value)` in ascending windowed-key order.
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
    use crate::store::byte::InMemoryBytes;
    use crate::store::window_schema::wrap_value;

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
}
