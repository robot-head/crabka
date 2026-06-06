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
    /// Apply a changelog record during restore (updates state, does NOT re-log).
    async fn apply_changelog(&mut self, key: bytes::Bytes, value: Option<bytes::Bytes>);
    /// Toggle changelog logging (off during restore, on during processing).
    fn set_logging(&mut self, on: bool);
    /// IQ read view, if this store is interactively queryable. Default `None`.
    fn as_iq(&self) -> Option<&dyn crate::store::iq::IqQueryable> {
        None
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
