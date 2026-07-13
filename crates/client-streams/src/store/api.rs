//! Store traits. `StateStore` is object-safe (held erased in the registry) and
//! carries the changelog hooks (every #3 store is changelog-logged, so the
//! erased registry can restore/drain via `&mut dyn StateStore`).
//! `KeyValueStore<K,V>` is the typed get/put/delete surface.

use std::any::Any;

use async_trait::async_trait;

/// Lifecycle + identity + changelog hooks for any store.
#[async_trait]
pub trait StateStore: Any + Send {
    fn name(&self) -> &str;
    /// Flush pending state (no-op for in-memory — the changelog is durability).
    async fn flush(&mut self);
    fn close(&mut self);
    /// Typed downcast hook (used by `get_state_store`).
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// The store's changelog topic (`<app>-<store>-changelog`).
    fn changelog_topic(&self) -> &str;
    /// Drain buffered changelog entries (key bytes, value bytes or None=tombstone).
    fn take_changelog(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>)>;
    /// Like `take_changelog`, but each entry also carries an optional explicit
    /// changelog RECORD timestamp. `None` = use the producer default (send-time).
    /// Versioned stores override this to emit the version timestamp (KIP-889);
    /// the default wraps `take_changelog` with `None`.
    fn take_changelog_ts(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>, Option<i64>)> {
        self.take_changelog()
            .into_iter()
            .map(|(k, v)| (k, v, None))
            .collect()
    }
    /// Apply a changelog record during restore (updates state, does NOT re-log).
    async fn apply_changelog(&mut self, key: bytes::Bytes, value: Option<bytes::Bytes>);
    /// Like `apply_changelog`, but carries the changelog record's timestamp.
    /// Default ignores it and delegates. Versioned stores override to insert the
    /// version at this timestamp.
    async fn apply_changelog_ts(
        &mut self,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        _timestamp: i64,
    ) {
        self.apply_changelog(key, value).await;
    }
    /// Toggle changelog logging (off during restore, on during processing).
    fn set_logging(&mut self, on: bool);
    /// IQ read view, if this store is interactively queryable. Default `None`.
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        None
    }
    /// Stash the current record's context so a caching store can attach it to the
    /// next write (forwarded with the deduped `Change` on flush). Default no-op.
    fn set_record_context(&mut self, _ctx: crate::processor::record::RecordContext) {}
    /// Erased record-cache hook: wrap this store's backend in the supplied
    /// internal `NamedCache` (registered in the task's `ThreadCache`) and return `true` if
    /// this store kind is cache-aware. Lets `instantiate` enable caching on a
    /// materialized KV store without knowing its `K`/`V`. Default `false` (not
    /// cacheable) — window/session stores keep the default until their caching
    /// lands. KV stores override this to delegate to their typed `enable_cache`.
    ///
    /// `NamedCache` is crate-internal store plumbing; this method is reachable on
    /// the `pub` trait but never meaningfully callable from outside the crate.
    #[allow(private_interfaces)]
    fn enable_cache_erased(
        &mut self,
        _cache: std::sync::Arc<std::sync::Mutex<crate::store::cache::named::NamedCache>>,
    ) -> bool {
        false
    }
    /// Erased query: whether this store's record cache is enabled (its backend has
    /// been wrapped via [`enable_cache_erased`](Self::enable_cache_erased)). Lets a
    /// materializing processor decide — without knowing `K`/`V` — whether to
    /// suppress its immediate downstream forward (the cache flush forwards the
    /// deduped `Change`). Default `false` (not cached / not cache-aware). KV stores
    /// override this.
    fn is_cached_erased(&self) -> bool {
        false
    }
    /// Flush this store's record cache (if any): write dirty entries through to the
    /// underlying store, buffer their changelog records, and push the deduped
    /// downstream `Record<K, Change<V>>` into `buffer` — one boxed copy PER child in
    /// `children` (mirroring `ProcessorContext::forward`'s per-child clone). Default:
    /// no cache → no-op.
    ///
    /// `ErasedRecord` is crate-internal graph plumbing; this method is reachable
    /// on the `pub` trait but never meaningfully callable from outside the crate.
    #[allow(private_interfaces)]
    async fn flush_cache_into(
        &mut self,
        _buffer: &mut std::collections::VecDeque<(usize, crate::processor::erased::ErasedRecord)>,
        _children: &[usize],
    ) {
    }
    /// Wipe every entry (and any buffered changelog) — used by the EOS rollback
    /// path to reset the store to a clean slate before re-restoring from the
    /// committed changelog.
    async fn clear(&mut self);
}

/// A keyed store. Implemented by the in-memory store; the typed view a processor
/// gets from `ProcessorContext::get_state_store`.
///
/// `K: Send + Sync` because `get`/`delete` take `&K` and the boxed store future
/// must be `Send` (the whole execution chain — `Graph::pipe` → processors → store
/// ops — runs inside `tokio::spawn`); a `&K` is only `Send` when `K: Sync`.
#[async_trait]
pub trait KeyValueStore<K: Send + Sync, V: Send>: StateStore {
    async fn get(&self, key: &K) -> Option<V>;
    async fn put(&mut self, key: K, value: V);
    async fn delete(&mut self, key: &K) -> Option<V>;
    /// Half-open range scan `[lo, hi)` in memcmp (lexicographic) key order.
    async fn range(&self, lo: &K, hi: &K) -> Vec<(K, V)>;
}
