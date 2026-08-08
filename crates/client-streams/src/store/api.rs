//! Store traits.
//!
//! `StateStore` is object-safe, so the registry holds it erased. It carries the
//! changelog hooks. Every #3 store is changelog-logged, so the erased registry
//! can restore and drain through `&mut dyn StateStore`. `KeyValueStore<K,V>` is
//! the typed get/put/delete surface.

use std::any::Any;

use async_trait::async_trait;

/// Lifecycle, identity, and changelog hooks for any store.
#[async_trait]
pub trait StateStore: Any + Send {
    fn name(&self) -> &str;
    /// Flush pending state. This is a no-op for in-memory, where the changelog
    /// gives durability.
    async fn flush(&mut self);
    fn close(&mut self);
    /// Typed downcast hook that `get_state_store` uses.
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// The store's changelog topic (`<app>-<store>-changelog`).
    fn changelog_topic(&self) -> &str;
    /// Drain the buffered changelog entries: key bytes, and value bytes or None
    /// for a tombstone.
    fn take_changelog(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>)>;
    /// Like `take_changelog`, but each entry also carries an optional explicit
    /// changelog RECORD timestamp. `None` means use the producer default, which
    /// is send-time. Versioned stores override this method to emit the version
    /// timestamp (KIP-889). The default wraps `take_changelog` with `None`.
    fn take_changelog_ts(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>, Option<i64>)> {
        self.take_changelog()
            .into_iter()
            .map(|(k, v)| (k, v, None))
            .collect()
    }
    /// Apply a changelog record during restore. It updates state and does NOT
    /// re-log.
    async fn apply_changelog(&mut self, key: bytes::Bytes, value: Option<bytes::Bytes>);
    /// Like `apply_changelog`, but it carries the changelog record's timestamp.
    /// The default ignores the timestamp and delegates. Versioned stores override
    /// this method to insert the version at this timestamp.
    async fn apply_changelog_ts(
        &mut self,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        _timestamp: i64,
    ) {
        self.apply_changelog(key, value).await;
    }
    /// Toggle changelog logging. It is off during restore and on during
    /// processing.
    fn set_logging(&mut self, on: bool);
    /// IQ read view, if this store is interactively queryable. Default `None`.
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        None
    }
    /// Stash the current record's context, so a caching store can attach it to
    /// the next write. The flush forwards that context with the deduped `Change`.
    /// The default is a no-op.
    fn set_record_context(&mut self, _ctx: crate::processor::record::RecordContext) {}
    /// Erased record-cache hook.
    ///
    /// This method wraps the store's backend in the supplied internal
    /// `NamedCache`, which is registered in the task's `ThreadCache`. It returns
    /// `true` when this store kind is cache-aware. It lets `instantiate` enable
    /// caching on a materialized KV store and never learn the store's `K` or `V`.
    /// The default is `false`, which means not cacheable. Window and session
    /// stores keep the default until their caching lands. KV stores override this
    /// method and delegate to their typed `enable_cache`.
    ///
    /// `NamedCache` is crate-internal store plumbing. This method is reachable on
    /// the `pub` trait, but no caller outside the crate can use it.
    #[allow(private_interfaces)]
    fn enable_cache_erased(
        &mut self,
        _cache: std::sync::Arc<std::sync::Mutex<crate::store::cache::named::NamedCache>>,
    ) -> bool {
        false
    }
    /// Erased query for whether this store's record cache is enabled.
    ///
    /// The cache is enabled when
    /// [`enable_cache_erased`](Self::enable_cache_erased) has wrapped the
    /// backend. A materializing processor uses this method to decide whether to
    /// suppress its immediate downstream forward, and it never learns `K` or `V`.
    /// The cache flush forwards the deduped `Change`. The default is `false`,
    /// which means not cached and not cache-aware. KV stores override this
    /// method.
    fn is_cached_erased(&self) -> bool {
        false
    }
    /// Flush this store's record cache, if it has one.
    ///
    /// This method writes the dirty entries through to the underlying store,
    /// buffers their changelog records, and pushes the deduped downstream
    /// `Record<K, Change<V>>` into `buffer`. It pushes one boxed copy PER child
    /// in `children`, which matches the per-child clone in
    /// `ProcessorContext::forward`. The default has no cache, so it is a no-op.
    ///
    /// `ErasedRecord` is crate-internal graph plumbing. This method is reachable
    /// on the `pub` trait, but no caller outside the crate can use it.
    #[allow(private_interfaces)]
    async fn flush_cache_into(
        &mut self,
        _buffer: &mut std::collections::VecDeque<(usize, crate::processor::erased::ErasedRecord)>,
        _children: &[usize],
    ) {
    }
    /// Wipe every entry and any buffered changelog. The EOS rollback path uses
    /// this method to reset the store to a clean slate before it re-restores from
    /// the committed changelog.
    async fn clear(&mut self);
}

/// A keyed store. The in-memory store implements it, and a processor gets this
/// typed view from `ProcessorContext::get_state_store`.
///
/// `K` needs `Send + Sync` because `get` and `delete` take `&K`, and the boxed
/// store future must be `Send`. The whole execution chain runs inside
/// `tokio::spawn`: `Graph::pipe`, then the processors, then the store ops. A
/// `&K` is only `Send` when `K: Sync`.
#[async_trait]
pub trait KeyValueStore<K: Send + Sync, V: Send>: StateStore {
    async fn get(&self, key: &K) -> Option<V>;
    async fn put(&mut self, key: K, value: V);
    async fn delete(&mut self, key: &K) -> Option<V>;
    /// Half-open range scan `[lo, hi)` in memcmp (lexicographic) key order.
    async fn range(&self, lo: &K, hi: &K) -> Vec<(K, V)>;
}
