//! Window store over the byte backend: composite `WindowKeySchema` keys +
//! `ValueAndTimestamp` values. A second typed store beside `KeyValueBytesStore`.
use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    processor::{record::RecordContext, serde::Serde},
    store::{
        api::StateStore,
        byte::{ByteKeyValueStore, InMemoryBytes},
        cache::{named::NamedCache, window::CachingWindowStore},
        window_schema::{key_bytes_of, store_key, unwrap_value, window_start_of, wrap_value},
    },
};

/// The window store's backing: either a plain boxed byte store or a record-cache
/// wrapper over it. `Cached` is opted into via [`WindowBytesStore::enable_cache`];
/// uncached stores keep today's behavior exactly. Mirrors `kv::Backing`.
enum Backing {
    Plain(Box<dyn ByteKeyValueStore>),
    Cached(CachingWindowStore),
}

impl Backing {
    /// Cache-first single-key read when `Cached`; direct otherwise.
    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        match self {
            Backing::Plain(b) => b.get(key).await,
            Backing::Cached(c) => c.get(key).await,
        }
    }
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
            Backing::Cached(c) => c.put(key, value, ctx),
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

/// Typed windowed store keyed by `(K, windowStart)`, holding `V` + a record
/// timestamp. `fetch_single` returns `(storedTs, V)` so the aggregator can compute
/// `newTs = max(recordTs, storedTs)`.
#[async_trait]
pub trait WindowStore<K: Send + Sync, V: Send>: StateStore {
    async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)>;
    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)>;
    /// Like `fetch`, but also returns each window's stored record timestamp:
    /// `(windowStart, recordTs, value)`. Used by the sliding-window aggregator,
    /// which needs `windowMaxRecordTimestamp` to place left/right windows.
    async fn fetch_with_ts(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, i64, V)>;
    /// Every window across ALL keys whose `windowStart` is in `[start_from,
    /// start_to]`, as `(key, windowStart, recordTs, value)`. Backs emit-final's
    /// closed-window scan (the byte layout is key-prefixed, so this is a filtered
    /// full scan, mirroring the JVM `fetchAll`).
    async fn fetch_all_in_range(&self, start_from: i64, start_to: i64) -> Vec<(K, i64, i64, V)>;
    async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64);
}

pub struct WindowBytesStore<K, V> {
    name: String,
    changelog_topic: String,
    backing: Backing,
    key_serde: Box<dyn Serde<K>>,
    value_serde: Box<dyn Serde<V>>,
    changelog: Vec<(Bytes, Option<Bytes>)>,
    logging: bool,
    /// Window size in ms. The windowed store-key bytes encode only the window
    /// START (and seqnum), not the size/end, so the store must know its size to
    /// reconstruct `end = start + window_size_ms` for the downstream
    /// [`Windowed`](crate::dsl::windows::Windowed) key forwarded on cache flush.
    window_size_ms: i64,
    /// Set via [`StateStore::set_record_context`]; attached to the next cached
    /// write so the deduped `Change` can be forwarded with the right context on
    /// flush. Only meaningful when `backing` is `Cached`.
    pending_ctx: Option<RecordContext>,
}

impl<K: 'static, V: 'static> WindowBytesStore<K, V> {
    #[must_use]
    pub(crate) fn new(
        name: String,
        backend: Box<dyn ByteKeyValueStore>,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
        window_size_ms: i64,
    ) -> Self {
        Self {
            name,
            changelog_topic,
            backing: Backing::Plain(backend),
            key_serde,
            value_serde,
            changelog: Vec::new(),
            logging: true,
            window_size_ms,
            pending_ctx: None,
        }
    }

    #[must_use]
    pub fn in_memory(
        name: String,
        key_serde: Box<dyn Serde<K>>,
        value_serde: Box<dyn Serde<V>>,
        changelog_topic: String,
        window_size_ms: i64,
    ) -> Self {
        Self::new(
            name,
            Box::new(InMemoryBytes::default()),
            key_serde,
            value_serde,
            changelog_topic,
            window_size_ms,
        )
    }

    /// Wrap this store's backend in a record cache (moves the backend into a
    /// [`CachingWindowStore`]). The caller supplies the [`NamedCache`] registered
    /// in the task's `ThreadCache`. Re-wrapping an already-cached store is a no-op.
    pub(crate) fn enable_cache(&mut self, cache: Arc<Mutex<NamedCache>>) {
        if !matches!(self.backing, Backing::Plain(_)) {
            return; // already cached
        }
        let placeholder = Backing::Plain(Box::new(InMemoryBytes::default()));
        let Backing::Plain(backend) = std::mem::replace(&mut self.backing, placeholder) else {
            unreachable!("guarded by the matches! above")
        };
        let inner: Arc<AsyncMutex<Box<dyn ByteKeyValueStore>>> = Arc::new(AsyncMutex::new(backend));
        self.backing = Backing::Cached(CachingWindowStore::with_name(
            cache,
            inner,
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
impl<K: Send + 'static, V: Send + 'static> StateStore for WindowBytesStore<K, V> {
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
        // Vec<(store_key_bytes, old_wrapped, new_wrapped, ctx)>. The values are the
        // *wrapped* bytes (`recordTs ‖ serialized`) the inner store holds.
        let drained = cache.flush_with_old().await;
        for (sk, old_wrapped, new_wrapped, ctx) in drained {
            // Changelog logs the raw windowed store-key + the new wrapped value,
            // exactly as the uncached `put` path does.
            if self.logging {
                self.changelog.push((sk.clone(), new_wrapped.clone()));
            }
            // Decode the windowed store-key once: inner key bytes + window start.
            let window_start = window_start_of(&sk);
            let key_bytes = key_bytes_of(&sk);
            for &child in children {
                let key: K = self
                    .key_serde
                    .deserialize(&self.changelog_topic, key_bytes)
                    .expect("flush_cache_into window key deserialize");
                // Unwrap each wrapped value to its raw aggregate bytes, then
                // deserialize. `None` (tombstone / absent) stays `None`.
                let old: Option<V> = old_wrapped.as_ref().map(|w| {
                    let (_ts, raw) = unwrap_value(w);
                    self.value_serde
                        .deserialize(&self.changelog_topic, raw)
                        .expect("flush_cache_into window old value deserialize")
                });
                let new: Option<V> = new_wrapped.as_ref().map(|w| {
                    let (_ts, raw) = unwrap_value(w);
                    self.value_serde
                        .deserialize(&self.changelog_topic, raw)
                        .expect("flush_cache_into window new value deserialize")
                });
                let windowed = Windowed {
                    key,
                    window: Window {
                        start: window_start,
                        end: window_start + self.window_size_ms,
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

// `WindowBytesStore` holds only `Box<dyn Serde<_>>` + byte buffers, so it is
// `Send + Sync` for any `K`/`V` — no `Sync` bound needed on the impl.
#[async_trait::async_trait]
impl<K: Send + 'static, V: Send + 'static> crate::store::iq::IqQueryable
    for WindowBytesStore<K, V>
{
    fn kind(&self) -> crate::store::iq::StoreKind {
        crate::store::iq::StoreKind::Window
    }
    async fn iq_window_fetch_single(&self, key: &[u8], window_start: i64) -> Option<bytes::Bytes> {
        let sk = store_key(key, window_start, 0);
        let wrapped = self.backing.get(&sk).await?;
        let (_ts, raw) = unwrap_value(&wrapped);
        Some(bytes::Bytes::copy_from_slice(raw))
    }
    async fn iq_window_fetch(
        &self,
        key: &[u8],
        time_from: i64,
        time_to: i64,
    ) -> Vec<(i64, bytes::Bytes)> {
        let lo = store_key(key, time_from, 0);
        let hi = store_key(key, time_to.saturating_add(1), 0);
        let mut out = Vec::new();
        for (k, wrapped) in self.backing.range(&lo, &hi).await {
            if key_bytes_of(&k) != key {
                continue;
            }
            let (_ts, raw) = unwrap_value(&wrapped);
            out.push((window_start_of(&k), bytes::Bytes::copy_from_slice(raw)));
        }
        out
    }

    async fn iq2_execute(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Box<dyn Any + Send>, crate::store::iq::Iq2Failure> {
        use crate::store::iq::{Iq2Failure, Iq2Query};

        // Serialize any key boxes to bytes up front (before any `.await`) so
        // `query` is dropped before the async phase. `dyn Any` deref works for
        // both `Send` and `Send+Sync` boxes.
        let ser = |b: &dyn Any| -> Result<bytes::Bytes, Iq2Failure> {
            let k = b.downcast_ref::<K>().ok_or(Iq2Failure::KeyTypeMismatch)?;
            Ok(self.key_serde.serialize(&self.changelog_topic, k))
        };

        match query {
            Iq2Query::WindowKey {
                key,
                from_ts,
                to_ts,
            } => {
                let kb = ser(&**key)?;
                // Window starts are non-negative and encoded big-endian *signed*,
                // so a negative lower bound (e.g. the `i64::MIN` default of an
                // unbounded `WindowKeyQuery`) would sort above `0x00..` and make
                // the byte range empty. Clamp the lower start to 0.
                let from = (*from_ts).max(0);
                let to = *to_ts;

                let lo = store_key(&kb, from, 0);
                let hi = store_key(&kb, to.saturating_add(1), 0);
                let mut out: Vec<(i64, V)> = Vec::new();
                for (sk, wrapped) in self.backing.range(&lo, &hi).await {
                    if key_bytes_of(&sk) != kb.as_ref() {
                        continue;
                    }
                    let (_ts, raw) = unwrap_value(&wrapped);
                    out.push((
                        window_start_of(&sk),
                        self.value_serde
                            .deserialize(&self.changelog_topic, raw)
                            .expect("iqv2 window value deserialize"),
                    ));
                }
                Ok(Box::new(out))
            }
            Iq2Query::WindowRange {
                lo,
                hi,
                from_ts,
                to_ts,
            } => {
                let lo_b = match lo {
                    Some(b) => Some(ser(&**b)?),
                    None => None,
                };
                let hi_b = match hi {
                    Some(b) => Some(ser(&**b)?),
                    None => None,
                };
                let from = *from_ts;
                let to = *to_ts;

                let mut out: Vec<((K, i64), V)> = Vec::new();
                for (sk, wrapped) in self.backing.scan_all().await {
                    let ws = window_start_of(&sk);
                    if ws < from || ws > to {
                        continue;
                    }
                    let kbytes = key_bytes_of(&sk);
                    if lo_b.as_ref().is_some_and(|l| kbytes < l.as_ref()) {
                        continue;
                    }
                    if hi_b.as_ref().is_some_and(|h| kbytes > h.as_ref()) {
                        continue;
                    }
                    let key = self
                        .key_serde
                        .deserialize(&self.changelog_topic, kbytes)
                        .expect("iqv2 window range key deserialize");
                    let (_ts, raw) = unwrap_value(&wrapped);
                    let value = self
                        .value_serde
                        .deserialize(&self.changelog_topic, raw)
                        .expect("iqv2 window range value deserialize");
                    out.push(((key, ws), value));
                }
                Ok(Box::new(out))
            }
            _ => Err(Iq2Failure::UnknownQueryType),
        }
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> WindowStore<K, V> for WindowBytesStore<K, V> {
    async fn fetch_single(&self, key: &K, window_start: i64) -> Option<(i64, V)> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let sk = store_key(&kb, window_start, 0);
        let wrapped = self.backing.get(&sk).await?;
        let (ts, raw) = unwrap_value(&wrapped);
        Some((
            ts,
            self.value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("window value deserialize"),
        ))
    }

    async fn fetch(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, V)> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let lo = store_key(&kb, time_from, 0);
        let hi = store_key(&kb, time_to.saturating_add(1), 0);
        let mut out = Vec::new();
        for (k, wrapped) in self.backing.range(&lo, &hi).await {
            // guard prefix collisions: only return entries whose inner key matches
            if key_bytes_of(&k) != kb.as_ref() {
                continue;
            }
            let (_ts, raw) = unwrap_value(&wrapped);
            out.push((
                window_start_of(&k),
                self.value_serde
                    .deserialize(&self.changelog_topic, raw)
                    .expect("window value deserialize"),
            ));
        }
        out
    }

    async fn fetch_with_ts(&self, key: &K, time_from: i64, time_to: i64) -> Vec<(i64, i64, V)> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let lo = store_key(&kb, time_from, 0);
        let hi = store_key(&kb, time_to.saturating_add(1), 0);
        let mut out = Vec::new();
        for (k, wrapped) in self.backing.range(&lo, &hi).await {
            if key_bytes_of(&k) != kb.as_ref() {
                continue;
            }
            let (ts, raw) = unwrap_value(&wrapped);
            out.push((
                window_start_of(&k),
                ts,
                self.value_serde
                    .deserialize(&self.changelog_topic, raw)
                    .expect("window value deserialize"),
            ));
        }
        out
    }

    async fn fetch_all_in_range(&self, start_from: i64, start_to: i64) -> Vec<(K, i64, i64, V)> {
        let mut out = Vec::new();
        for (k, wrapped) in self.backing.scan_all().await {
            let ws = window_start_of(&k);
            if ws < start_from || ws > start_to {
                continue;
            }
            let key = self
                .key_serde
                .deserialize(&self.changelog_topic, key_bytes_of(&k))
                .expect("window key deserialize");
            let (ts, raw) = unwrap_value(&wrapped);
            let value = self
                .value_serde
                .deserialize(&self.changelog_topic, raw)
                .expect("window value deserialize");
            out.push((key, ws, ts, value));
        }
        out
    }

    async fn put(&mut self, key: K, window_start: i64, value: V, record_ts: i64) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let sk = store_key(&kb, window_start, 0);
        let raw = self.value_serde.serialize(&self.changelog_topic, &value);
        let wrapped = wrap_value(record_ts, &raw);
        match &self.backing {
            // Plain: write through + buffer the changelog now (today's behavior).
            Backing::Plain(_) => {
                self.backing
                    .put(sk.clone(), wrapped.clone(), self.write_ctx())
                    .await;
                if self.logging {
                    self.changelog.push((sk, Some(wrapped)));
                }
            }
            // Cached: write-back only. The changelog record is deferred to
            // `flush_cache_into` (the changelog store sits below the cache).
            Backing::Cached(_) => {
                let ctx = self.write_ctx();
                self.backing.put(sk, wrapped, ctx).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    #[tokio::test]
    async fn put_fetch_single_and_range() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            10,
        );
        s.put("k".to_string(), 0, 1, 5).await;
        s.put("k".to_string(), 0, 2, 7).await;
        s.put("k".to_string(), 10, 9, 11).await;
        assert_eq!(s.fetch_single(&"k".to_string(), 0).await, Some((7, 2)));
        assert_eq!(s.fetch_single(&"k".to_string(), 10).await, Some((11, 9)));
        assert_eq!(s.fetch_single(&"k".to_string(), 99).await, None);
        assert_eq!(
            s.fetch(&"k".to_string(), 0, 10).await,
            vec![(0, 2), (10, 9)]
        );
        assert_eq!(s.take_changelog().len(), 3);
    }

    #[tokio::test]
    async fn fetch_with_ts_returns_window_start_and_record_ts() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            10,
        );
        s.put("k".into(), 0, 10, 5).await; // window start 0, value 10, recordTs 5
        s.put("k".into(), 10, 20, 17).await; // window start 10, value 20, recordTs 17
        let got = s.fetch_with_ts(&"k".to_string(), 0, 10).await;
        assert_eq!(got, vec![(0, 5, 10), (10, 17, 20)]); // (windowStart, recordTs, value)
    }

    #[tokio::test]
    async fn fetch_all_in_range_scans_across_keys() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "app-w-changelog".into(),
            10,
        );
        // Two keys, three windows. windowStart ∈ {0, 0, 10}.
        s.put("a".into(), 0, 1, 5).await;
        s.put("b".into(), 0, 7, 6).await;
        s.put("a".into(), 10, 9, 12).await;

        // Range [0,0] returns both windowStart==0 entries (sort to make order-independent).
        let mut got = s.fetch_all_in_range(0, 0).await;
        got.sort();
        assert_eq!(
            got,
            vec![("a".to_string(), 0, 5, 1), ("b".to_string(), 0, 6, 7)]
        );

        // Range [0,10] returns all three.
        assert_eq!(s.fetch_all_in_range(0, 10).await.len(), 3);
        // Range above everything returns nothing.
        assert!(s.fetch_all_in_range(11, 100).await.is_empty());
    }

    #[tokio::test]
    async fn iq2_window_key_and_range() {
        use crate::store::iq::{Iq2Query, IqQueryable};
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            1000,
        );
        s.put("a".into(), 0, 10, 5).await;
        s.put("a".into(), 1000, 20, 1005).await;
        s.put("b".into(), 0, 30, 6).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();

        // WindowKeyQuery: key "a", starts in [0,1000], ascending.
        let wk = q
            .iq2_execute(&Iq2Query::WindowKey {
                key: Box::new("a".to_string()),
                from_ts: 0,
                to_ts: 1000,
            })
            .await
            .unwrap();
        assert_eq!(
            *wk.downcast::<Vec<(i64, i64)>>().unwrap(),
            vec![(0, 10), (1000, 20)]
        );

        // WindowRangeQuery: all keys, starts in [0,0] → a@0 and b@0, ascending by key.
        let wr = q
            .iq2_execute(&Iq2Query::WindowRange {
                lo: None,
                hi: None,
                from_ts: 0,
                to_ts: 0,
            })
            .await
            .unwrap();
        assert_eq!(
            *wr.downcast::<Vec<((String, i64), i64)>>().unwrap(),
            vec![(("a".to_string(), 0), 10), (("b".to_string(), 0), 30)]
        );

        // WindowRangeQuery: key range [b, b] only.
        let wr_b = q
            .iq2_execute(&Iq2Query::WindowRange {
                lo: Some(Box::new("b".to_string())),
                hi: Some(Box::new("b".to_string())),
                from_ts: 0,
                to_ts: 2000,
            })
            .await
            .unwrap();
        assert_eq!(
            *wr_b.downcast::<Vec<((String, i64), i64)>>().unwrap(),
            vec![(("b".to_string(), 0), 30)]
        );
    }

    // Regression: `WindowKeyQuery::with_key(k)` with no explicit time range
    // defaults to `from_ts = i64::MIN`. Window starts are encoded big-endian
    // *signed*, so an unclamped `i64::MIN` lower bound (`0x80..`) sorts ABOVE
    // every non-negative start (`0x00..`) under memcmp and the byte range comes
    // back empty — the "all windows for this key" query must still return them.
    #[tokio::test]
    async fn iq2_window_key_default_bounds_returns_all_windows() {
        use crate::store::iq::{Iq2Query, IqQueryable};
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            1000,
        );
        s.put("a".into(), 0, 10, 5).await;
        s.put("a".into(), 1000, 20, 1005).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();
        let wk = q
            .iq2_execute(&Iq2Query::WindowKey {
                key: Box::new("a".to_string()),
                from_ts: i64::MIN,
                to_ts: i64::MAX,
            })
            .await
            .unwrap();
        assert_eq!(
            *wk.downcast::<Vec<(i64, i64)>>().unwrap(),
            vec![(0, 10), (1000, 20)]
        );
    }

    // ── Record-cache tests (mirror kv.rs) ───────────────────────────────────

    fn cached_store(window_size_ms: i64) -> WindowBytesStore<String, i64> {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            window_size_ms,
        );
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("w".into()))));
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
    async fn cached_window_store_reads_your_writes() {
        let mut s = cached_store(10);
        s.set_record_context(ctx_at(0));
        // Two cached puts to the SAME windowed key (window start 0).
        s.put("a".into(), 0, 1, 5).await;
        s.put("a".into(), 0, 2, 7).await;
        // Cache-first read sees the latest staged write (value 2, recordTs 7).
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, Some((7, 2)));
        // No changelog buffered while writes only touch the cache.
        assert!(s.take_changelog().is_empty());
    }

    #[tokio::test]
    async fn flush_cache_into_emits_deduped_windowed_change() {
        use crate::dsl::{
            processors::change::Change,
            windows::{Window, Windowed},
        };
        let mut s = cached_store(10);
        // Seed a committed value (flush it through + clear the changelog buffer).
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 1, 5).await;
        let mut seed = std::collections::VecDeque::new();
        s.flush_cache_into(&mut seed, &[0]).await;
        let _ = s.take_changelog();

        // Two staged writes for the same window key under context ts=7.
        s.set_record_context(ctx_at(7));
        s.put("a".into(), 0, 2, 11).await;
        s.put("a".into(), 0, 3, 13).await;

        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[7]).await;
        // Exactly ONE deduped record for the single fake child index 7.
        assert_eq!(buffer.len(), 1);
        let (child, rec) = &buffer[0];
        assert_eq!(*child, 7);
        // Timestamp comes from the dirty entry's record context.
        assert_eq!(rec.timestamp, 7);
        // Key downcasts to Windowed<String> with start 0 / end = start + size.
        let key = rec
            .key
            .as_ref()
            .unwrap()
            .downcast_ref::<Windowed<String>>()
            .unwrap();
        assert_eq!(key.key, "a");
        assert_eq!(key.window, Window { start: 0, end: 10 }); // end = start + window_size
        // Value downcasts to Change<i64> { old = committed (1), new = latest (3) }.
        let change = rec.value.downcast_ref::<Change<i64>>().unwrap();
        assert_eq!(change.old, Some(1));
        assert_eq!(change.new, Some(3));

        // Changelog buffered the RAW windowed store-key + the latest wrapped value.
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 1);
        assert_eq!(
            cl[0].0,
            store_key(
                &StringSerde.serialize("w-changelog", &"a".to_string()),
                0,
                0
            )
        );
        assert_eq!(
            cl[0].1,
            Some(wrap_value(13, &I64Serde.serialize("w-changelog", &3)))
        );

        // Inner store now holds the write-through value.
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, Some((13, 3)));
    }

    #[tokio::test]
    async fn cached_sliding_window_uses_true_size_not_retention_span_for_key_end() {
        // Regression: a CACHED sliding aggregate/reduce builds its window store
        // with a retention basis of `2 * timeDifferenceMs` but a true window size
        // of `1 * timeDifferenceMs`. The flushed `Windowed` key end must be
        // `start + timeDifferenceMs`, NOT `start + 2*timeDifferenceMs`. Before the
        // fix the store was constructed with the retention span as its window size,
        // so the end was doubled (this assertion would see `end == 20`).
        use crate::dsl::{
            processors::change::Change,
            windows::{Window, Windowed},
        };

        // D = 10 (the real sliding window size = time_difference_ms); the retention
        // basis a sliding aggregate passes is 2*D = 20. The store's window_size_ms
        // is the TRUE size (D = 10), which is what `add_window_store` now forwards.
        const D: i64 = 10;
        let mut s = cached_store(D);

        // Stage a cached put for a window starting at ws = 0.
        let ws = 0;
        s.set_record_context(ctx_at(7));
        s.put("a".into(), ws, 1, 7).await;

        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert_eq!(buffer.len(), 1);
        let (_child, rec) = &buffer[0];
        let key = rec
            .key
            .as_ref()
            .unwrap()
            .downcast_ref::<Windowed<String>>()
            .unwrap();
        assert_eq!(key.key, "a");
        // end == ws + D (10), NOT ws + 2*D (20).
        assert_eq!(
            key.window,
            Window {
                start: ws,
                end: ws + D
            }
        );
        let change = rec.value.downcast_ref::<Change<i64>>().unwrap();
        assert_eq!(change.new, Some(1));
    }

    /// Cached `fetch` / `fetch_with_ts` route through `Backing::Cached::range`
    /// and serve read-your-writes (the cache overlay) before any flush; cache
    /// wins over a colliding inner window.
    #[tokio::test]
    async fn cached_window_store_fetch_overlays_cache() {
        let mut s = cached_store(10);
        s.set_record_context(ctx_at(0));
        // Stage three windows for "a": starts 0, 10, 20.
        s.put("a".into(), 0, 1, 5).await;
        s.put("a".into(), 10, 2, 15).await;
        s.put("a".into(), 20, 3, 25).await;

        // fetch over [0, 20] returns all three in window order (range overlay).
        assert_eq!(
            s.fetch(&"a".to_string(), 0, 20).await,
            vec![(0, 1), (10, 2), (20, 3)]
        );
        // fetch_with_ts surfaces each window's stored record ts.
        assert_eq!(
            s.fetch_with_ts(&"a".to_string(), 0, 20).await,
            vec![(0, 5, 1), (10, 15, 2), (20, 25, 3)]
        );
        // Narrowed range excludes window 20.
        assert_eq!(
            s.fetch(&"a".to_string(), 0, 10).await,
            vec![(0, 1), (10, 2)]
        );
    }

    /// Cached `fetch_all_in_range` routes through `Backing::Cached::scan_all`,
    /// overlaying the cache across all keys.
    #[tokio::test]
    async fn cached_window_store_fetch_all_overlays_cache() {
        let mut s = cached_store(10);
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 1, 5).await;
        s.put("b".into(), 50, 2, 55).await;

        // [0, 10] excludes b@50.
        assert_eq!(
            s.fetch_all_in_range(0, 10).await,
            vec![("a".to_string(), 0, 5, 1)]
        );
        // [0, 50] includes both, in windowed-key order (a@0 < b@50).
        assert_eq!(
            s.fetch_all_in_range(0, 50).await,
            vec![("a".to_string(), 0, 5, 1), ("b".to_string(), 50, 55, 2),]
        );
    }

    /// `apply_changelog` on a cached window store writes BELOW the cache (no
    /// dirty entry → an empty cache flush) and a `None` deletes through.
    #[tokio::test]
    async fn cached_window_store_apply_changelog_goes_below_cache() {
        let mut s = cached_store(10);
        let sk = store_key(
            &StringSerde.serialize("w-changelog", &"a".to_string()),
            0,
            0,
        );
        let wrapped = wrap_value(7, &I64Serde.serialize("w-changelog", &9));
        s.apply_changelog(sk.clone(), Some(wrapped)).await;
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, Some((7, 9)));
        // Restored below the cache: nothing to forward, no changelog buffered.
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty());
        assert!(s.take_changelog().is_empty());

        // None apply deletes through the inner store.
        s.apply_changelog(sk, None).await;
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, None);
    }

    /// `clear` on a cached window store empties the cache layer and the inner
    /// store and drops the buffered changelog.
    #[tokio::test]
    async fn cached_window_store_clear_empties_everything() {
        let mut s = cached_store(10);
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 0, 1, 5).await;
        StateStore::clear(&mut s).await;
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, None);
        assert!(s.fetch_all_in_range(i64::MIN, i64::MAX).await.is_empty());
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty());
    }

    /// `enable_cache` is idempotent: re-wrapping an already-cached store is a
    /// no-op.
    #[tokio::test]
    async fn enable_cache_is_idempotent() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            10,
        );
        assert!(!s.is_cached());
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("w".into()))));
        assert!(s.is_cached());
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("w".into()))));
        assert!(s.is_cached());
    }

    #[tokio::test]
    async fn plain_window_store_unchanged() {
        // A non-cached store logs the changelog immediately; flush_cache_into is a
        // no-op (no cache).
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            10,
        );
        s.put("a".into(), 0, 1, 5).await;
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, Some((5, 1)));
        let cl = s.take_changelog();
        assert_eq!(cl.len(), 1);
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        assert!(buffer.is_empty());
    }

    /// Plain-store lifecycle: `set_logging(false)` suppresses the changelog,
    /// `apply_changelog` restores below the store (Some then None deletes),
    /// `flush`/`close` are no-ops, and `StateStore::clear` wipes state +
    /// changelog.
    #[tokio::test]
    async fn plain_window_store_lifecycle_and_apply_changelog() {
        let mut s = WindowBytesStore::<String, i64>::in_memory(
            "w".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "w-changelog".into(),
            10,
        );
        // Logging off → no changelog buffered.
        s.set_logging(false);
        s.put("a".into(), 0, 1, 5).await;
        assert!(s.take_changelog().is_empty());

        // apply_changelog restores below the store without re-logging.
        let sk = store_key(
            &StringSerde.serialize("w-changelog", &"b".to_string()),
            0,
            0,
        );
        s.apply_changelog(
            sk.clone(),
            Some(wrap_value(7, &I64Serde.serialize("w-changelog", &9))),
        )
        .await;
        assert_eq!(s.fetch_single(&"b".to_string(), 0).await, Some((7, 9)));
        assert!(s.take_changelog().is_empty());
        // None apply deletes through.
        s.apply_changelog(sk, None).await;
        assert_eq!(s.fetch_single(&"b".to_string(), 0).await, None);

        // flush/close are no-ops; clear wipes the remaining "a" window + changelog.
        s.flush().await;
        s.close();
        StateStore::clear(&mut s).await;
        assert_eq!(s.fetch_single(&"a".to_string(), 0).await, None);
        assert!(s.take_changelog().is_empty());
    }
}
