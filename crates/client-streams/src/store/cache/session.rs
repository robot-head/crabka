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
//! `NamedCache` exposes no key iterator, so the store keeps its own `cached_keys`
//! shadow set (a superset of the composite keys it has staged) to enumerate
//! candidate cache keys within a session-key range. Each candidate is reconciled
//! against the live cache via `get`: an entry the shared `ThreadCache` budget
//! evicted since it was staged probe-misses and is pruned from the shadow,
//! leaving the inner value to stand. Same approach as `CachingKeyValueStore`.

use std::collections::{BTreeMap, BTreeSet};
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
    /// Superset of composite session keys this store has staged in the shared
    /// `cache`. `NamedCache` exposes no key iterator, so `find_sessions`
    /// enumerates candidate cache keys here and reconciles each against the live
    /// cache (an entry may have been evicted by the shared `ThreadCache` budget).
    /// Kept in memcmp order so range scans match the inner store's key ordering.
    cached_keys: Mutex<BTreeSet<Bytes>>,
}

impl CachingSessionStore {
    pub fn new(cache: Arc<Mutex<NamedCache>>, inner: Box<dyn ByteKeyValueStore>) -> Self {
        Self {
            cache,
            inner: AsyncMutex::new(inner),
            cached_keys: Mutex::new(BTreeSet::new()),
        }
    }

    /// Write-back put: stage a dirty entry in the cache keyed by the full session
    /// composite bytes (`inner_key ‖ end ‖ start`, built via
    /// [`session_key`](crate::store::session_schema::session_key)). The inner
    /// store is not touched until [`flush`](Self::flush).
    #[allow(clippy::unused_async)]
    pub async fn put(&self, session_key_bytes: Bytes, value: Bytes, ctx: RecordContext) {
        self.cached_keys
            .lock()
            .unwrap()
            .insert(session_key_bytes.clone());
        let mut cache = self.cache.lock().unwrap();
        cache.put(
            session_key_bytes,
            LruCacheEntry::new(Some(value), true, ctx),
        );
    }

    /// Write-back remove: stage a dirty tombstone (`None`) for the session.
    #[allow(clippy::unused_async)]
    pub async fn remove(&self, session_key_bytes: Bytes, ctx: RecordContext) {
        self.cached_keys
            .lock()
            .unwrap()
            .insert(session_key_bytes.clone());
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

        // Candidate cache keys in range, then reconcile each against the live
        // cache (it may have been evicted by the shared budget). Restrict to keys
        // whose inner-key bytes equal `key` to skip prefix collisions.
        let candidates: Vec<Bytes> = {
            let keys = self.cached_keys.lock().unwrap();
            keys.range(lo.clone()..hi.clone())
                .filter(|k| session_key_bytes_of(k) == key)
                .cloned()
                .collect()
        };
        let mut stale: Vec<Bytes> = Vec::new();
        {
            let cache = self.cache.lock().unwrap();
            for k in candidates {
                match cache.get(&k) {
                    // Live value: cache wins over the inner store.
                    Some(e) => match &e.value {
                        Some(v) => {
                            merged.insert(k, v.clone());
                        }
                        // Tombstone: hide the inner value.
                        None => {
                            merged.remove(&k);
                        }
                    },
                    // Evicted from the shared cache since we staged it: drop from
                    // the shadow set and let the inner value (already merged) stand.
                    None => stale.push(k),
                }
            }
        }
        if !stale.is_empty() {
            let mut keys = self.cached_keys.lock().unwrap();
            for k in stale {
                keys.remove(&k);
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
}
