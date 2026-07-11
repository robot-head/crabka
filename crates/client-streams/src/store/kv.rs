//! `KeyValueBytesStore<K,V>`: the single typed store the registry holds and
//! downcasts to. Serde + changelog-buffer logic over a pluggable `ByteKeyValueStore`.
use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    processor::{record::RecordContext, serde::Serde},
    store::{
        api::{KeyValueStore, StateStore},
        byte::{ByteKeyValueStore, InMemoryBytes},
        cache::{kv::CachingKeyValueStore, named::NamedCache},
    },
};

/// The store's backing: either a plain boxed byte store or a record-cache
/// wrapper over it. `Cached` is opted into via [`KeyValueBytesStore::enable_cache`];
/// uncached stores keep today's behavior exactly.
enum Backing {
    Plain(Box<dyn ByteKeyValueStore>),
    // Constructed via `enable_cache`, which build-time cache wiring
    // (`BuiltTopology::instantiate`) calls for materialized KV stores.
    Cached(CachingKeyValueStore),
}

impl Backing {
    /// Cache-first when `Cached` (read-your-writes); direct otherwise.
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
    async fn approx_len(&self) -> u64 {
        match self {
            Backing::Plain(b) => b.approx_len().await,
            // No cheap merged count; the cache+inner overlay is materialized.
            Backing::Cached(c) => c.scan_all().await.len() as u64,
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

    /// Processing-path delete. Plain: returns the previous value. Cached:
    /// write-back tombstone; the previous value (for the typed return) is read
    /// cache-first before staging the tombstone.
    async fn delete(&mut self, key: Bytes, ctx: RecordContext) -> Option<Bytes> {
        match self {
            Backing::Plain(b) => b.delete(&key).await,
            Backing::Cached(c) => {
                let prev = c.get(&key).await;
                c.delete(key, ctx).await;
                prev
            }
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

pub struct KeyValueBytesStore<K, V> {
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

impl<K: 'static, V: 'static> KeyValueBytesStore<K, V> {
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

    /// Wrap this store's backend in a record cache (moves the backend into a
    /// [`CachingKeyValueStore`]). The caller supplies the [`NamedCache`]
    /// registered in the task's `ThreadCache`. Idempotent-ish: re-wrapping an
    /// already-cached store is a no-op.
    pub(crate) fn enable_cache(&mut self, cache: Arc<Mutex<NamedCache>>) {
        if !matches!(self.backing, Backing::Plain(_)) {
            return; // already cached
        }
        // Move the backend out, leaving a throwaway behind, then replace with the
        // cache wrapper. Safe because we just confirmed `Plain`.
        let placeholder = Backing::Plain(Box::new(InMemoryBytes::default()));
        let Backing::Plain(backend) = std::mem::replace(&mut self.backing, placeholder) else {
            unreachable!("guarded by the matches! above")
        };
        self.backing = Backing::Cached(CachingKeyValueStore::with_name(
            cache,
            backend,
            self.name.clone(),
        ));
    }

    /// Whether this store's backend has been wrapped in a record cache via
    /// [`enable_cache`](Self::enable_cache). Used by the erased
    /// [`StateStore::is_cached_erased`] hook (so a materializing processor can
    /// suppress its immediate forward) and by build-time wiring tests.
    #[must_use]
    pub(crate) fn is_cached(&self) -> bool {
        matches!(self.backing, Backing::Cached(_))
    }

    /// Convenience constructor for tests: an in-memory-backed store.
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

    /// The context to stamp on the next cached write: the stashed
    /// [`set_record_context`](StateStore::set_record_context) if present, else a
    /// default rooted at the changelog topic. Processing always sets context
    /// first, so the fallback is only hit in tests that put without one.
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
impl<K: Send + 'static, V: Send + 'static> StateStore for KeyValueBytesStore<K, V> {
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
        use crate::{dsl::processors::change::Change, processor::erased::ErasedRecord};
        let Backing::Cached(cache) = &self.backing else {
            return;
        };
        let drained = cache.flush_with_old().await; // Vec<(kb, old_vb, new_vb, ctx)>
        for (kb, old_vb, new_vb, ctx) in drained {
            // Changelog is buffered at flush (the changelog store sits below the
            // cache, so cached puts defer their changelog records until here).
            if self.logging {
                self.changelog.push((kb.clone(), new_vb.clone()));
            }
            // Re-build the typed key + `Change<V>` PER child from the cloneable
            // byte buffers (mirrors `pipe`'s per-child re-deserialize and
            // `forward`'s per-child clone): `Box<dyn Any>` / `Change<V>` for an
            // unbounded `V` is not cloneable, so each child gets its own fresh
            // deserialization. `children` is empty → the dirty entry is still
            // written through + changelog-buffered above, just not forwarded.
            for &child in children {
                let k: K = self
                    .key_serde
                    .deserialize(&self.changelog_topic, &kb)
                    .expect("flush_cache_into key deserialize");
                let old: Option<V> = old_vb.as_ref().map(|b| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, b)
                        .expect("flush_cache_into old value deserialize")
                });
                let new: Option<V> = new_vb.as_ref().map(|b| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, b)
                        .expect("flush_cache_into new value deserialize")
                });
                let change = Change { old, new };
                buffer.push_back((
                    child,
                    ErasedRecord::new(Some(Box::new(k)), Box::new(change), ctx.timestamp),
                ));
            }
        }
    }
    async fn clear(&mut self) {
        self.backing.clear().await;
        self.changelog.clear();
    }
}

// The store struct holds only `Box<dyn Serde<_>>` + byte buffers (no bare `K`/`V`
// fields), so it is `Send + Sync` for *any* `K`/`V`. `K: Send + V: Send` are
// required so `Box<Option<V>>` / `Box<Vec<(K,V)>>` can be returned as
// `Box<dyn Any + Send>` from `iq2_execute`.
#[async_trait::async_trait]
impl<K: Send + 'static, V: Send + 'static> crate::store::iq::IqQueryable
    for KeyValueBytesStore<K, V>
{
    fn kind(&self) -> crate::store::iq::StoreKind {
        crate::store::iq::StoreKind::KeyValue
    }
    async fn iq_kv_get(&self, key: &[u8]) -> Option<bytes::Bytes> {
        self.backing.get(key).await
    }
    async fn iq_kv_range(&self, lo: &[u8], hi: &[u8]) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        // JVM `range` is inclusive `[lo, hi]`; the byte backend is half-open
        // `[lo, hi)`. `hi ++ 0x00` is the least key strictly greater than `hi`,
        // so `[lo, hi ++ 0x00)` == inclusive `[lo, hi]`.
        let mut hi_succ = hi.to_vec();
        hi_succ.push(0);
        self.backing.range(lo, &hi_succ).await
    }
    async fn iq_kv_all(&self) -> Vec<(bytes::Bytes, bytes::Bytes)> {
        self.backing.scan_all().await
    }
    async fn iq_kv_approx_count(&self) -> u64 {
        self.backing.approx_len().await
    }

    async fn iq2_execute(
        &self,
        query: &crate::store::iq::Iq2Query,
    ) -> Result<Box<dyn Any + Send>, crate::store::iq::Iq2Failure> {
        use crate::store::iq::{Iq2Failure, Iq2Query};
        let ser = |b: &Box<dyn Any + Send + Sync>| -> Result<bytes::Bytes, Iq2Failure> {
            let k = b.downcast_ref::<K>().ok_or(Iq2Failure::KeyTypeMismatch)?;
            Ok(self.key_serde.serialize(&self.changelog_topic, k))
        };
        match query {
            Iq2Query::Key { key } => {
                let kb = ser(key)?;
                let out: Option<V> = self.backing.get(&kb).await.map(|vb| {
                    self.value_serde
                        .deserialize(&self.changelog_topic, &vb)
                        .expect("iqv2 kv value deserialize")
                });
                Ok(Box::new(out))
            }
            Iq2Query::Range { lo, hi, descending } => {
                let lo_b = match lo {
                    Some(b) => Some(ser(b)?),
                    None => None,
                };
                let hi_b = match hi {
                    Some(b) => Some(ser(b)?),
                    None => None,
                };
                let mut rows: Vec<(K, V)> = Vec::new();
                for (kb, vb) in self.backing.scan_all().await {
                    if lo_b.as_ref().is_some_and(|l| kb.as_ref() < l.as_ref()) {
                        continue;
                    }
                    if hi_b.as_ref().is_some_and(|h| kb.as_ref() > h.as_ref()) {
                        continue;
                    }
                    rows.push((
                        self.key_serde
                            .deserialize(&self.changelog_topic, &kb)
                            .expect("iqv2 kv range key deserialize"),
                        self.value_serde
                            .deserialize(&self.changelog_topic, &vb)
                            .expect("iqv2 kv range value deserialize"),
                    ));
                }
                if *descending {
                    rows.reverse();
                }
                Ok(Box::new(rows))
            }
            _ => Err(Iq2Failure::UnknownQueryType),
        }
    }
}

#[async_trait]
impl<K: Send + Sync + 'static, V: Send + 'static> KeyValueStore<K, V> for KeyValueBytesStore<K, V> {
    async fn get(&self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        self.backing.get(&kb).await.map(|vb| {
            self.value_serde
                .deserialize(&self.changelog_topic, &vb)
                .expect("store value deserialize")
        })
    }
    async fn put(&mut self, key: K, value: V) {
        let kb = self.key_serde.serialize(&self.changelog_topic, &key);
        let vb = self.value_serde.serialize(&self.changelog_topic, &value);
        match &self.backing {
            // Plain: write through + buffer the changelog now (today's behavior).
            Backing::Plain(_) => {
                self.backing
                    .put(kb.clone(), vb.clone(), self.write_ctx())
                    .await;
                if self.logging {
                    self.changelog.push((kb, Some(vb)));
                }
            }
            // Cached: write-back only. The changelog record is deferred to
            // `flush_cache_changes` (the changelog store sits below the cache).
            Backing::Cached(_) => {
                let ctx = self.write_ctx();
                self.backing.put(kb, vb, ctx).await;
            }
        }
    }
    async fn delete(&mut self, key: &K) -> Option<V> {
        let kb = self.key_serde.serialize(&self.changelog_topic, key);
        let ctx = self.write_ctx();
        let cached = matches!(self.backing, Backing::Cached(_));
        let prev = self.backing.delete(kb.clone(), ctx).await.map(|vb| {
            self.value_serde
                .deserialize(&self.changelog_topic, &vb)
                .expect("store value deserialize")
        });
        // Plain logs the tombstone now; cached defers it to flush.
        if self.logging && !cached {
            self.changelog.push((kb, None));
        }
        prev
    }
    async fn range(&self, lo: &K, hi: &K) -> Vec<(K, V)> {
        let lo_b = self.key_serde.serialize(&self.changelog_topic, lo);
        let hi_b = self.key_serde.serialize(&self.changelog_topic, hi);
        self.backing
            .range(&lo_b, &hi_b)
            .await
            .into_iter()
            .map(|(kb, vb)| {
                (
                    self.key_serde
                        .deserialize(&self.changelog_topic, &kb)
                        .expect("kv range key deserialize"),
                    self.value_serde
                        .deserialize(&self.changelog_topic, &vb)
                        .expect("kv range value deserialize"),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};

    fn store() -> KeyValueBytesStore<String, i64> {
        KeyValueBytesStore::in_memory(
            "s".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "s-changelog".into(),
        )
    }

    fn cached_store() -> KeyValueBytesStore<String, i64> {
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
    async fn put_get_delete_and_changelog_buffer() {
        let mut s = store();
        s.put("a".into(), 1).await;
        s.put("a".into(), 2).await;
        check!(s.get(&"a".to_string()).await == Some(2));
        check!(s.delete(&"a".to_string()).await == Some(2));
        check!(s.get(&"a".to_string()).await == None);
        let cl = s.take_changelog();
        check!((cl.len(), cl[2].1.is_none()) == (3, true));
        check!(s.take_changelog().is_empty());
    }

    #[tokio::test]
    async fn range_returns_ordered_half_open() {
        use bytes::Bytes;

        use crate::processor::serde::BytesSerde;
        let mut s = KeyValueBytesStore::<Bytes, Bytes>::in_memory(
            "r".into(),
            Box::new(BytesSerde),
            Box::new(BytesSerde),
            "r-cl".into(),
        );
        s.put(Bytes::from_static(&[1, 0]), Bytes::from_static(b"a"))
            .await;
        s.put(Bytes::from_static(&[1, 5]), Bytes::from_static(b"b"))
            .await;
        s.put(Bytes::from_static(&[2, 0]), Bytes::from_static(b"c"))
            .await;
        let r = s
            .range(&Bytes::from_static(&[1, 0]), &Bytes::from_static(&[2, 0]))
            .await; // [lo, hi)
        assert2::assert!(
            r == vec![
                (Bytes::from_static(&[1, 0]), Bytes::from_static(b"a")),
                (Bytes::from_static(&[1, 5]), Bytes::from_static(b"b")),
            ]
        );
    }

    #[tokio::test]
    async fn iq2_key_and_range() {
        use crate::store::iq::{Iq2Failure, Iq2Query, IqQueryable};
        let mut s = store();
        s.put("a".into(), 1).await;
        s.put("b".into(), 2).await;
        s.put("c".into(), 3).await;
        let q: &dyn IqQueryable = s.as_iq().unwrap();

        // KeyQuery hit / miss.
        let got = q
            .iq2_execute(&Iq2Query::Key {
                key: Box::new("b".to_string()),
            })
            .await
            .unwrap();
        assert2::assert!(*got.downcast::<Option<i64>>().unwrap() == Some(2));
        let miss = q
            .iq2_execute(&Iq2Query::Key {
                key: Box::new("z".to_string()),
            })
            .await
            .unwrap();
        assert2::assert!(*miss.downcast::<Option<i64>>().unwrap() == None);

        // RangeQuery inclusive [a,b] ascending.
        let r = q
            .iq2_execute(&Iq2Query::Range {
                lo: Some(Box::new("a".to_string())),
                hi: Some(Box::new("b".to_string())),
                descending: false,
            })
            .await
            .unwrap();
        assert2::assert!(
            *r.downcast::<Vec<(String, i64)>>().unwrap()
                == vec![("a".to_string(), 1), ("b".to_string(), 2)]
        );

        // Unbounded both sides, descending → all, reversed.
        let all_desc = q
            .iq2_execute(&Iq2Query::Range {
                lo: None,
                hi: None,
                descending: true,
            })
            .await
            .unwrap();
        assert2::assert!(
            *all_desc.downcast::<Vec<(String, i64)>>().unwrap()
                == vec![
                    ("c".to_string(), 3),
                    ("b".to_string(), 2),
                    ("a".to_string(), 1)
                ]
        );

        // Wrong key type → KeyTypeMismatch.
        let bad = q
            .iq2_execute(&Iq2Query::Key {
                key: Box::new(7_i64),
            })
            .await;
        assert2::assert!(bad.err() == Some(Iq2Failure::KeyTypeMismatch));
    }

    #[tokio::test]
    async fn apply_changelog_restores_without_re_logging() {
        let mut s = store();
        s.apply_changelog(
            b"k".to_vec().into(),
            Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 7])),
        )
        .await;
        check!(s.get(&"k".to_string()).await == Some(7));
        check!(s.take_changelog().is_empty());
        s.apply_changelog(b"k".to_vec().into(), None).await;
        check!(s.get(&"k".to_string()).await == None);
    }

    #[tokio::test]
    async fn cached_store_reads_your_writes() {
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 1).await;
        s.put("a".into(), 2).await;
        // Cache-first read sees the latest staged write.
        check!(s.get(&"a".to_string()).await == Some(2));
        // The inner backend has NOT been written yet (write-back deferred to flush).
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        check!(buffer.len() == 1);
    }

    #[tokio::test]
    async fn cached_store_defers_changelog_until_flush() {
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 1).await;
        s.put("a".into(), 2).await;
        // No changelog buffered while writes only touch the cache.
        check!(s.take_changelog().is_empty());
        // Flushing the cache buffers exactly one deduped changelog record.
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        let cl = s.take_changelog();
        check!(
            (cl.len(), cl[0].0.as_ref(), cl[0].1.as_deref())
                == (
                    1,
                    StringSerde
                        .serialize("s-changelog", &"a".to_string())
                        .as_ref(),
                    Some(I64Serde.serialize("s-changelog", &2).as_ref())
                )
        );
    }

    #[tokio::test]
    async fn flush_cache_into_emits_deduped_change() {
        use crate::dsl::processors::change::Change;
        let mut s = cached_store();
        // Seed committed a=1 (flush it through + clear the changelog buffer).
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 1).await;
        let mut seed = std::collections::VecDeque::new();
        s.flush_cache_into(&mut seed, &[0]).await;
        let _ = s.take_changelog();

        // Two staged writes for the same key under context ts=7.
        s.set_record_context(ctx_at(7));
        s.put("a".into(), 2).await;
        s.put("a".into(), 3).await;
        // Flush into a single fake child index 7.
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[7]).await;
        let (child, rec) = &buffer[0];
        let key = rec.key.as_ref().unwrap().downcast_ref::<String>().unwrap();
        let change = rec.value.downcast_ref::<Change<i64>>().unwrap();
        // old = last-committed value (1); new = deduped latest (3).
        check!(
            (buffer.len(), *child, rec.timestamp, key.as_str(), change)
                == (
                    1,
                    7,
                    7,
                    "a",
                    &Change {
                        old: Some(1),
                        new: Some(3)
                    }
                )
        );

        // Inner store now holds the write-through value.
        check!(s.get(&"a".to_string()).await == Some(3));
    }

    /// Cached routing for `range` / `scan_all` / `approx_len` / IQ reads serves
    /// read-your-writes through the cache overlay before any flush.
    #[tokio::test]
    async fn cached_store_range_scan_and_count_overlay() {
        use crate::store::iq::IqQueryable;
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 1).await;
        s.put("b".into(), 2).await;
        s.put("c".into(), 3).await;

        // Typed range routes through Backing::Cached::range (cache overlay).
        let r = s.range(&"a".to_string(), &"c".to_string()).await; // [a, c)
        check!(r == vec![("a".to_string(), 1), ("b".to_string(), 2)]);

        // IQ all + approx-count route through Backing::Cached::scan_all.
        check!(s.iq_kv_all().await.len() == 3);
        check!(s.iq_kv_approx_count().await == 3);
        // IQ get routes cache-first.
        check!(s.iq_kv_get(b"b").await == Some(I64Serde.serialize("s-changelog", &2)));
    }

    /// On a cached store, typed `delete` returns the prior cached value (read
    /// cache-first before staging the tombstone).
    #[tokio::test]
    async fn cached_store_delete_returns_prev_and_stages_tombstone() {
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 5).await;

        // delete sees the staged value as its return, without touching the
        // changelog (cached defers it to flush).
        check!(s.delete(&"a".to_string()).await == Some(5));
        check!(s.take_changelog().is_empty());
        check!(s.get(&"a".to_string()).await == None); // tombstone hides it

        // Flushing emits the deduped tombstone Change + changelog record.
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        let cl = s.take_changelog();
        check!((cl.len(), cl[0].1.is_none()) == (1, true));
    }

    /// `apply_changelog` on a cached store writes BELOW the cache (no dirty entry
    /// staged → nothing forwarded on the next cache flush), and a `None` value
    /// deletes through the inner store.
    #[tokio::test]
    async fn cached_store_apply_changelog_goes_below_cache() {
        let mut s = cached_store();
        s.apply_changelog(
            b"k".to_vec().into(),
            Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 7])),
        )
        .await;
        check!(s.get(&"k".to_string()).await == Some(7));
        // Restored below the cache: no dirty entry, so the cache flush is empty.
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        check!(buffer.is_empty());
        check!(s.take_changelog().is_empty());

        // A None apply deletes through the inner store.
        s.apply_changelog(b"k".to_vec().into(), None).await;
        check!(s.get(&"k".to_string()).await == None);
    }

    /// `clear` on a cached store empties both the cache layer and the inner store
    /// and drops the buffered changelog.
    #[tokio::test]
    async fn cached_store_clear_empties_everything() {
        use crate::store::iq::IqQueryable;
        let mut s = cached_store();
        s.set_record_context(ctx_at(0));
        s.put("a".into(), 1).await;
        StateStore::clear(&mut s).await;
        check!(s.get(&"a".to_string()).await == None);
        check!(s.iq_kv_all().await.is_empty());
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        check!(buffer.is_empty());
    }

    /// `enable_cache` is idempotent: re-wrapping an already-cached store is a
    /// no-op (the guard returns early).
    #[tokio::test]
    async fn enable_cache_is_idempotent() {
        let mut s = store();
        check!(!s.is_cached());
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("s".into()))));
        check!(s.is_cached());
        // Second call hits the early-return guard; still cached, no panic.
        s.enable_cache(Arc::new(Mutex::new(NamedCache::new("s".into()))));
        check!(s.is_cached());
        check!(s.is_cached_erased());
    }

    #[tokio::test]
    async fn plain_store_unchanged() {
        // A non-cached store logs the changelog immediately and reads from the
        // backend (no flush_cache_into needed).
        let mut s = store();
        s.put("a".into(), 1).await;
        check!(s.get(&"a".to_string()).await == Some(1));
        let cl = s.take_changelog();
        check!(
            cl == vec![(
                StringSerde.serialize("s-changelog", &"a".to_string()),
                Some(I64Serde.serialize("s-changelog", &1)),
            )]
        );
        // No cache → flush_cache_into forwards nothing.
        let mut buffer = std::collections::VecDeque::new();
        s.flush_cache_into(&mut buffer, &[0]).await;
        check!(buffer.is_empty());
        // flush / close are no-ops (in-memory durability is the changelog) and
        // must not disturb the stored value.
        s.flush().await;
        s.close();
        check!(s.get(&"a".to_string()).await == Some(1));
    }
}
