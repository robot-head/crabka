//! Session store over the byte backend — `SessionKeySchema` keys (`key‖end‖start`),
//! raw aggregate values. Third typed store beside `KeyValueBytesStore` and
//! `WindowBytesStore`. Supports the JVM session-merge fetch (`find_sessions`).
use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    processor::{record::RecordContext, serde::Serde},
    store::{
        api::StateStore,
        byte::{ByteKeyValueStore, InMemoryBytes},
        cache::{named::NamedCache, session::CachingSessionStore},
        session_schema::{session_end_of, session_key, session_key_bytes_of, session_start_of},
    },
};

/// The session store's backing: either a plain boxed byte store or a record-cache
/// wrapper over it. `Cached` is opted into via [`SessionBytesStore::enable_cache`];
/// uncached stores keep today's behavior exactly. Mirrors `kv::Backing`.
enum Backing {
    Plain(Box<dyn ByteKeyValueStore>),
    Cached(CachingSessionStore),
}

impl Backing {
    async fn range(&self, lo: &[u8], hi: &[u8]) -> Vec<(Bytes, Bytes)> {
        match self {
            Backing::Plain(b) => b.range(lo, hi).await,
            Backing::Cached(c) => c.range(lo, hi).await,
        }
    }
    async fn scan_all(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            Backing::Plain(b) => b.scan_all().await,
            Backing::Cached(c) => c.scan_all().await,
        }
    }

    /// Processing-path write. Plain: direct put. Cached: write-back put carrying
    /// the record context (changelog deferred to flush).
    async fn put(&mut self, key: Bytes, value: Bytes, ctx: RecordContext) {
        match self {
            Backing::Plain(b) => b.put(key, value).await,
            Backing::Cached(c) => c.put(key, value, ctx).await,
        }
    }

    /// Processing-path remove (tombstone). Plain: direct delete. Cached:
    /// write-back tombstone carrying the context.
    async fn remove(&mut self, key: Bytes, ctx: RecordContext) {
        match self {
            Backing::Plain(b) => {
                b.delete(&key).await;
            }
            Backing::Cached(c) => c.remove(key, ctx).await,
        }
    }

    /// Restore-path write (below the cache; never stages a dirty entry).
    async fn apply(&mut self, key: Bytes, value: Option<Bytes>) {
        match (self, value) {
            (Backing::Plain(b), Some(v)) => b.put(key, v).await,
            (Backing::Plain(b), None) => {
                b.delete(&key).await;
            }
            (Backing::Cached(c), Some(v)) => c.put_inner(key, v).await,
            (Backing::Cached(c), None) => c.delete_inner(&key).await,
        }
    }

    async fn clear(&mut self) {
        match self {
            Backing::Plain(b) => b.clear().await,
            Backing::Cached(c) => c.clear().await,
        }
    }
}

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
    /// Every session across ALL keys whose `end <= close_time`, as
    /// `(key, start, end, value)`. Backs emit-final's closed-session scan.
    async fn find_closed_sessions(&self, close_time: i64) -> Vec<(K, i64, i64, V)>;
}

pub struct SessionBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backing: Backing,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
    /// Set via [`StateStore::set_record_context`]; attached to the next cached
    /// write so the deduped `Change` can be forwarded with the right context on
    /// flush. Only meaningful when `backing` is `Cached`.
    pending_ctx: Option<RecordContext>,
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
            backing: Backing::Plain(backend),
            key_serde,
            value_serde,
            changelog: Vec::new(),
            logging: true,
            pending_ctx: None,
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

    /// Wrap this store's backend in a record cache (moves the backend into a
    /// [`CachingSessionStore`]). The caller supplies the [`NamedCache`] registered
    /// in the task's `ThreadCache`. Re-wrapping an already-cached store is a no-op.
    pub(crate) fn enable_cache(&mut self, cache: Arc<Mutex<NamedCache>>) {
        if !matches!(self.backing, Backing::Plain(_)) {
            return; // already cached
        }
        let placeholder = Backing::Plain(Box::new(InMemoryBytes::default()));
        let Backing::Plain(backend) = std::mem::replace(&mut self.backing, placeholder) else {
            unreachable!("guarded by the matches! above")
        };
        self.backing = Backing::Cached(CachingSessionStore::with_name(
            cache,
            backend,
            self.name.clone(),
        ));
    }

    /// Whether this store's backend has been wrapped in a record cache.
    #[must_use]
    pub(crate) fn is_cached(&self) -> bool {
        matches!(self.backing, Backing::Cached(_))
    }

    /// The context to stamp on the next cached write: the stashed
    /// [`set_record_context`](StateStore::set_record_context) if present, else a
    /// default rooted at the changelog topic.
    fn write_ctx(&self) -> RecordContext {
        self.pending_ctx.clone().unwrap_or(RecordContext {
            topic: self.changelog_topic.clone(),
            partition: 0,
            offset: 0,
            timestamp: 0,
        })
    }
}

#[async_trait]
impl<K: Send + 'static, V: Send + 'static> StateStore for SessionBytesStore<K, V> {
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
        // Restore writes go BELOW the cache (straight to the inner store) so they
        // don't stage dirty entries that would be re-logged on the next flush.
        self.backing.apply(key, value).await;
    }
    fn set_logging(&mut self, on: bool) {
        self.logging = on;
    }
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        Some(self)
    }
    fn set_record_context(&mut self, ctx: RecordContext) {
        self.pending_ctx = Some(ctx);
    }
    #[allow(private_interfaces)]
    fn enable_cache_erased(&mut self, cache: Arc<Mutex<NamedCache>>) -> bool {
        self.enable_cache(cache);
        true
    }
    fn is_cached_erased(&self) -> bool {
        self.is_cached()
    }
    #[allow(private_interfaces)]
    async fn flush_cache_into(
        &mut self,
        buffer: &mut std::collections::VecDeque<(usize, crate::processor::erased::ErasedRecord)>,
        children: &[usize],
    ) {
        use crate::{
            dsl::{
                processors::change::Change,
                windows::{Window, Windowed},
            },
            processor::erased::ErasedRecord,
        };
        let Backing::Cached(cache) = &self.backing else {
            return;
        };
        // Vec<(session_key_bytes, old, new, ctx)>. Session values are the raw
        // aggregate bytes (no `ValueAndTimestamp` wrap).
        let drained = cache.flush_with_old().await;
        for (sk, old_vb, new_vb, ctx) in drained {
            // Changelog logs the raw session store-key + the new value, exactly as
            // the uncached `put` path does.
            if self.logging {
                self.changelog.push((sk.clone(), new_vb.clone()));
            }
            // The session key is self-contained: it carries both start and end.
            let session_start = session_start_of(&sk);
            let session_end = session_end_of(&sk);
            let key_bytes = session_key_bytes_of(&sk);
            for &child in children {
                let key: K = self
                    .key_serde
                    .deserialize(&self.changelog_topic, key_bytes)
                    .expect("flush_cache_into session key deserialize");
                let old: Option<V> = old_vb.as_ref().map(|b| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, b)
                        .expect("flush_cache_into session old value deserialize")
                });
                let new: Option<V> = new_vb.as_ref().map(|b| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, b)
                        .expect("flush_cache_into session new value deserialize")
                });
                let windowed = Windowed {
                    key,
                    window: Window {
                        start: session_start,
                        end: session_end,
                    },
                };
                let change = Change { old, new };
                buffer.push_back((
                    child,
                    ErasedRecord::new(Some(Box::new(windowed)), Box::new(change), ctx.timestamp),
                ));
            }
        }
    }
    async fn clear(&mut self) {
        self.backing.clear().await;
        self.changelog.clear();
    }
}

// `SessionBytesStore` holds only `Box<dyn Serde<_>>` + byte buffers, so it is
// `Send + Sync` for any `K`/`V` — no `Sync` bound needed on the impl.
#[async_trait::async_trait]
impl<K: 'static, V: 'static> crate::store::iq::IqQueryable for SessionBytesStore<K, V> {
    fn kind(&self) -> crate::store::iq::StoreKind {
        crate::store::iq::StoreKind::Session
    }
    async fn iq_session_fetch_key(&self, key: &[u8]) -> Vec<((i64, i64), bytes::Bytes)> {
        let lo = session_key(key, 0, 0);
        let hi = session_key(key, i64::MAX, i64::MAX);
        let mut out = Vec::new();
        for (k, raw) in self.backing.range(&lo, &hi).await {
            if session_key_bytes_of(&k) != key {
                continue;
            }
            let start = session_start_of(&k);
            let end = session_end_of(&k);
            out.push(((start, end), bytes::Bytes::copy_from_slice(&raw)));
        }
        out
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
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        // Lower bound: smallest qualifying end (clamped to 0 — stored ends are
        // non-negative epoch millis; a negative earliest_end means "all qualify").
        let lo = session_key(&kb, 0, earliest_end.max(0));
        // Upper bound: past every entry for this key prefix.
        let hi = session_key(&kb, i64::MAX, i64::MAX);
        let mut out = Vec::new();
        for (k, raw) in self.backing.range(&lo, &hi).await {
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
                        .deserialize(&self.changelog_topic, &raw)
                        .expect("session value deserialize"),
                ));
            }
        }
        out
    }

    async fn put(&mut self, key: K, start: i64, end: i64, value: V) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let sk = session_key(&kb, start, end);
        let raw = self.value_serde.serialize(&self.changelog_topic, &value);
        match &self.backing {
            // Plain: write through + buffer the changelog now (today's behavior).
            Backing::Plain(_) => {
                self.backing
                    .put(sk.clone(), raw.clone(), self.write_ctx())
                    .await;
                if self.logging {
                    self.changelog.push((sk, Some(raw)));
                }
            }
            // Cached: write-back only. The changelog record is deferred to
            // `flush_cache_into` (the changelog store sits below the cache).
            Backing::Cached(_) => {
                let ctx = self.write_ctx();
                self.backing.put(sk, raw, ctx).await;
            }
        }
    }

    async fn remove(&mut self, key: &K, start: i64, end: i64) {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let sk = session_key(&kb, start, end);
        let cached = matches!(self.backing, Backing::Cached(_));
        let ctx = self.write_ctx();
        self.backing.remove(sk.clone(), ctx).await;
        // Plain logs the tombstone now; cached defers it to flush.
        if self.logging && !cached {
            self.changelog.push((sk, None));
        }
    }

    async fn find_closed_sessions(&self, close_time: i64) -> Vec<(K, i64, i64, V)> {
        let mut out = Vec::new();
        for (k, raw) in self.backing.scan_all().await {
            let end = session_end_of(&k);
            if end > close_time {
                continue;
            }
            let key = self
                .key_serde
                .deserialize(&self.changelog_topic, session_key_bytes_of(&k))
                .expect("session key deserialize");
            let value = self
                .value_serde
                .deserialize(&self.changelog_topic, &raw)
                .expect("session value deserialize");
            out.push((key, session_start_of(&k), end, value));
        }
        out
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
    async fn find_closed_sessions_scans_by_end() {
        let mut s = SessionBytesStore::<String, i64>::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-s-changelog".into(),
        );
        // (key, start, end, value)
        s.put("a".into(), 0, 5, 1).await;
        s.put("b".into(), 2, 8, 2).await;
        s.put("a".into(), 20, 30, 3).await;

        // end <= 8 → the first two (sort to make order-independent).
        let mut got = s.find_closed_sessions(8).await;
        got.sort();
        assert_eq!(
            got,
            vec![("a".to_string(), 0, 5, 1), ("b".to_string(), 2, 8, 2)]
        );
        // end <= 4 → none.
        assert!(s.find_closed_sessions(4).await.is_empty());
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

    // ── Record-cache tests (mirror kv.rs) ───────────────────────────────────

    fn cached_store() -> SessionBytesStore<String, i64> {
        let mut s = store();
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("s".into()))));
        s
    }

    fn ctx_at(ts: i64) -> RecordContext {
        RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: ts,
        }
    }

    #[tokio::test]
    async fn cached_session_store_reads_your_writes() {
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        // Two cached puts to the SAME session key [0, 10].
        s.put("a".into(), 0, 10, 1).await;
        s.put("a".into(), 0, 10, 2).await;
        // Cache-first read sees the latest staged write (value 2).
        assert_eq!(
            s.find_sessions(&"a".to_string(), 0, 100).await,
            vec![(0, 10, 2)]
        );
        // No changelog buffered while writes only touch the cache.
        assert!(s.take_changelog().is_empty());
    }

    #[tokio::test]
    async fn flush_cache_into_emits_deduped_session_change() {
        use crate::dsl::{
            processors::change::Change,
            windows::{Window, Windowed},
        };
        let mut s = cached_store();
        // Seed a committed value (flush it through + clear the changelog buffer).
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 10, 1).await;
        let mut seed = std::collections::VecDeque::new();
        s.flush_cache_into(&mut seed, &[0]).await;
        let _ = s.take_changelog();

        // Two staged writes for the same session key [0, 10] under context ts=7.
        s.set_record_context(ctx_at(7));
        s.put("a".into(), 0, 10, 2).await;
        s.put("a".into(), 0, 10, 3).await;

        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[7]).await;
        let (child, rec) = &buffer[0];
        // Key downcasts to Windowed<String> carrying the encoded start/end.
        let key = rec
            .key
            .as_ref()
            .unwrap()
            .downcast_ref::<Windowed<String>>()
            .unwrap();
        // Value downcasts to Change<i64> { old = committed (1), new = latest (3) }.
        let change = rec.value.downcast_ref::<Change<i64>>().unwrap();
        assert_eq!(buffer.len(), 1);
        assert_eq!(*child, 7);
        assert_eq!(rec.timestamp, 7);
        assert_eq!(
            key,
            &Windowed {
                key: "a".into(),
                window: Window { start: 0, end: 10 },
            }
        );
        assert_eq!(
            change,
            &Change {
                old: Some(1),
                new: Some(3)
            }
        );

        // Changelog buffered the RAW session store-key + the latest value.
        let cl = s.take_changelog();
        assert_eq!(
            cl,
            vec![(
                session_key(
                    &StringSerde.serialize("app-s-changelog", &"a".to_string()),
                    0,
                    10
                ),
                Some(I64Serde.serialize("app-s-changelog", &3)),
            )]
        );

        // Inner store now holds the write-through value.
        assert_eq!(
            s.find_sessions(&"a".to_string(), 0, 100).await,
            vec![(0, 10, 3)]
        );
    }

    /// Cached `find_closed_sessions` routes through `Backing::Cached::scan_all`,
    /// overlaying the cache across all keys (read-your-writes before flush).
    #[tokio::test]
    async fn cached_find_closed_sessions_overlays_cache() {
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 5, 1).await;
        s.put("b".into(), 2, 8, 2).await;
        s.put("a".into(), 20, 30, 3).await;

        let mut got = s.find_closed_sessions(8).await;
        got.sort();
        assert_eq!(
            got,
            vec![("a".to_string(), 0, 5, 1), ("b".to_string(), 2, 8, 2)]
        );
        assert!(s.find_closed_sessions(4).await.is_empty());
        // Reads only touched the cache; no changelog buffered yet.
        assert!(s.take_changelog().is_empty());
    }

    /// On a cached store, `remove` stages a write-back tombstone (deferring its
    /// changelog) that hides the inner session, then flushes the deduped
    /// tombstone Change + changelog record.
    #[tokio::test]
    async fn cached_remove_stages_tombstone_and_flushes() {
        use crate::dsl::processors::change::Change;
        let mut s = cached_store();
        // Seed + flush a committed session [0, 10] = 1.
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 10, 1).await;
        let mut seed = std::collections::VecDeque::new();
        s.flush_cache_into(&mut seed, &[0]).await;
        let _ = s.take_changelog();

        // remove stages a tombstone (cached → no immediate changelog).
        s.set_record_context(ctx_at(7));
        s.remove(&"a".to_string(), 0, 10).await;
        assert!(s.take_changelog().is_empty());
        assert!(s.find_sessions(&"a".to_string(), 0, 100).await.is_empty());

        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        let change = buffer[0].1.value.downcast_ref::<Change<i64>>().unwrap();
        assert_eq!(buffer.len(), 1);
        assert_eq!(
            change,
            &Change {
                old: Some(1),
                new: None
            }
        );
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 1);
        assert!(cl[0].1.is_none()); // changelog tombstone
    }

    /// `apply_changelog` on a cached store writes BELOW the cache (no dirty entry
    /// → empty cache flush), and a `None` deletes through.
    #[tokio::test]
    async fn cached_apply_changelog_goes_below_cache() {
        let mut s = cached_store();
        let sk = session_key(
            &StringSerde.serialize("app-s-changelog", &"a".to_string()),
            0,
            10,
        );
        s.apply_changelog(sk.clone(), Some(I64Serde.serialize("app-s-changelog", &9)))
            .await;
        assert_eq!(
            s.find_sessions(&"a".to_string(), 0, 100).await,
            vec![(0, 10, 9)]
        );
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty());
        assert!(s.take_changelog().is_empty());

        // None apply deletes through.
        s.apply_changelog(sk, None).await;
        assert!(s.find_sessions(&"a".to_string(), 0, 100).await.is_empty());
    }

    /// `clear` on a cached store empties both the cache layer and inner store.
    #[tokio::test]
    async fn cached_clear_empties_everything() {
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 10, 1).await;
        StateStore::clear(&mut s).await;
        assert!(s.find_sessions(&"a".to_string(), 0, 100).await.is_empty());
        assert!(s.find_closed_sessions(i64::MAX).await.is_empty());
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty());
    }

    /// `enable_cache` is idempotent: re-wrapping an already-cached store is a
    /// no-op.
    #[tokio::test]
    async fn enable_cache_is_idempotent() {
        let mut s = store();
        assert!(!s.is_cached());
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("s".into()))));
        assert!(s.is_cached());
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("s".into()))));
        assert!(s.is_cached());
    }

    /// Plain-store lifecycle: `set_logging(false)` suppresses the changelog,
    /// `flush`/`close` are no-ops, and `StateStore::clear` wipes the store + its
    /// buffered changelog.
    #[tokio::test]
    async fn plain_store_lifecycle_logging_flush_close_clear() {
        let mut s = store();
        s.set_logging(false);
        s.put("a".into(), 0, 10, 1).await;
        assert!(
            s.take_changelog().is_empty(),
            "logging off suppresses the changelog"
        );
        // Read still works; flush/close are no-ops.
        assert_eq!(
            s.find_sessions(&"a".to_string(), 0, 100).await,
            vec![(0, 10, 1)]
        );
        s.flush().await;
        s.close();

        // Re-enable logging and write, then clear wipes state + changelog.
        s.set_logging(true);
        s.put("b".into(), 0, 10, 2).await;
        StateStore::clear(&mut s).await;
        assert!(s.find_sessions(&"b".to_string(), 0, 100).await.is_empty());
        assert!(s.take_changelog().is_empty());
    }

    #[tokio::test]
    async fn plain_session_store_unchanged() {
        let mut s = store();
        s.put("a".into(), 0, 10, 1).await;
        assert_eq!(
            s.find_sessions(&"a".to_string(), 0, 100).await,
            vec![(0, 10, 1)]
        );
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 1);
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty());
    }
}
