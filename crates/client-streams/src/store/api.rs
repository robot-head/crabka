//! Store traits. `StateStore` is object-safe (held erased in the registry) and
//! carries the changelog hooks (every #3 store is changelog-logged, so the
//! erased registry can restore/drain via `&mut dyn StateStore`).
//! `KeyValueStore<K,V>` is the typed get/put/delete surface.

use std::any::Any;

/// Lifecycle + identity + changelog hooks for any store.
pub trait StateStore: Any + Send {
    fn name(&self) -> &str;
    /// Flush pending state (no-op for in-memory — the changelog is durability).
    fn flush(&mut self);
    fn close(&mut self);
    /// Typed downcast hook (used by `get_state_store`).
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// The store's changelog topic (`<app>-<store>-changelog`).
    fn changelog_topic(&self) -> &str;
    /// Drain buffered changelog entries (key bytes, value bytes or None=tombstone).
    fn take_changelog(&mut self) -> Vec<(bytes::Bytes, Option<bytes::Bytes>)>;
    /// Apply a changelog record during restore (updates state, does NOT re-log).
    fn apply_changelog(&mut self, key: bytes::Bytes, value: Option<bytes::Bytes>);
    /// Toggle changelog logging (off during restore, on during processing).
    fn set_logging(&mut self, on: bool);
}

/// A keyed store. Implemented by the in-memory store; the typed view a processor
/// gets from `ProcessorContext::get_state_store`.
pub trait KeyValueStore<K, V>: StateStore {
    fn get(&self, key: &K) -> Option<V>;
    fn put(&mut self, key: K, value: V);
    fn delete(&mut self, key: &K) -> Option<V>;
}
