//! Per-task registry of erased stores + the typed downcast used by
//! `get_state_store`.

use std::collections::HashMap;

use crate::store::{
    api::{KeyValueStore, StateStore},
    kv::KeyValueBytesStore,
};

#[derive(Default)]
pub(crate) struct StoreRegistry {
    stores: HashMap<String, Box<dyn StateStore>>,
}

impl StoreRegistry {
    pub fn insert(&mut self, store: Box<dyn StateStore>) {
        self.stores.insert(store.name().to_string(), store);
    }

    /// Typed mutable access: downcast the erased store to the in-memory KV store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_kv<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn KeyValueStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<KeyValueBytesStore<K, V>>()?;
        Some(concrete as &mut dyn KeyValueStore<K, V>)
    }

    /// Store names (for restore + changelog drain, which iterate every store).
    pub fn names(&self) -> Vec<String> {
        self.stores.keys().cloned().collect()
    }

    /// Iterate every store mutably without allocating a `Vec<String>` of names
    /// and re-looking-up each one. Used on the per-record changelog-drain hot
    /// path (`Graph::drain_changelogs`).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut dyn StateStore> {
        self.stores.values_mut().map(std::convert::AsMut::as_mut)
    }

    /// Typed mutable access: downcast the erased store to the window store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_window<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::window::WindowStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::window::WindowBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::window::WindowStore<K, V>)
    }

    /// Typed mutable access: downcast the erased store to the join-window store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_join_window<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::join_window::JoinWindowStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::join_window::JoinWindowBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::join_window::JoinWindowStore<K, V>)
    }

    /// Typed mutable access: downcast the erased store to the session store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_session<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::session::SessionStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::session::SessionBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::session::SessionStore<K, V>)
    }

    /// Typed mutable access: downcast the erased store to the versioned store
    /// of the requested types. `None` if absent or the types don't match.
    pub fn get_versioned<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::versioned::VersionedKeyValueStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::versioned::VersionedBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::versioned::VersionedKeyValueStore<K, V>)
    }

    /// Typed mutable access: downcast the erased store to the suppress store
    /// of the requested types. `None` if absent or the types don't match.
    pub(crate) fn get_suppress<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut dyn crate::store::suppress_store::SuppressStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        let concrete = store
            .as_any_mut()
            .downcast_mut::<crate::store::suppress_store::SuppressBytesStore<K, V>>()?;
        Some(concrete as &mut dyn crate::store::suppress_store::SuppressStore<K, V>)
    }

    /// Typed mutable access: downcast the erased store to the join-grace buffer
    /// store of the requested types. `None` if absent or the types don't match.
    pub(crate) fn get_join_grace<K: Send + Sync + 'static, V: Send + 'static>(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::join_grace_buffer::JoinGraceBufferStore<K, V>> {
        let store = self.stores.get_mut(name)?;
        store
            .as_any_mut()
            .downcast_mut::<crate::store::join_grace_buffer::JoinGraceBufferStore<K, V>>()
    }

    /// Mutable access to the FK subscription store. `None` if absent / wrong type.
    pub(crate) fn get_fk_subscription(
        &mut self,
        name: &str,
    ) -> Option<&mut crate::store::fk_subscription::SubscriptionBytesStore> {
        let store = self.stores.get_mut(name)?;
        store
            .as_any_mut()
            .downcast_mut::<crate::store::fk_subscription::SubscriptionBytesStore>()
    }

    /// Mutable erased access by name — the `StateStore` trait surface
    /// (`changelog_topic` / `take_changelog` / `apply_changelog` / `set_logging`) is
    /// available on the returned `&mut dyn StateStore`.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut dyn StateStore> {
        self.stores.get_mut(name).map(std::convert::AsMut::as_mut)
    }

    /// IQ read view for the named store, if present and queryable.
    pub(crate) fn iq_get(&self, name: &str) -> Option<&dyn crate::store::iq::IqQueryable> {
        self.stores.get(name).and_then(|s| s.as_iq())
    }

    /// Whether the named KV store has its record cache enabled, via the erased
    /// [`StateStore::is_cached_erased`] hook (no `K`/`V` needed). `false` for an
    /// absent store or a store kind that isn't cache-aware. Used by
    /// [`ProcessorContext::store_is_cached`] so a materializing processor can
    /// suppress its immediate forward, and by build-time cache-wiring tests.
    ///
    /// [`ProcessorContext::store_is_cached`]: crate::processor::api::ProcessorContext::store_is_cached
    pub(crate) fn kv_is_cached(&self, name: &str) -> bool {
        self.stores.get(name).is_some_and(|s| s.is_cached_erased())
    }

    /// Enable the record cache on the named store via the erased
    /// [`StateStore::enable_cache_erased`] hook (no `K`/`V` needed). Returns
    /// `true` if the store is present AND cache-aware (a KV store); `false` for an
    /// absent store or a store kind whose caching hasn't landed yet (window/
    /// session), so the caller can skip rooting a `cache_owner` entry for it.
    pub(crate) fn enable_cache(
        &mut self,
        name: &str,
        cache: std::sync::Arc<std::sync::Mutex<crate::store::cache::named::NamedCache>>,
    ) -> bool {
        self.stores
            .get_mut(name)
            .is_some_and(|s| s.enable_cache_erased(cache))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        processor::serde::{I64Serde, StringSerde},
        store::kv::KeyValueBytesStore,
    };

    #[tokio::test]
    async fn register_and_downcast_versioned_store() {
        use crate::store::versioned::VersionedBytesStore;
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(VersionedBytesStore::<String, i64>::in_memory(
            "v".into(),
            secs(1_000),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "v-changelog".into(),
        )));
        let s = reg.get_versioned::<String, i64>("v").unwrap();
        s.put("x".into(), Some(5), 10).await;
        check!(s.get(&"x".to_string()).await.map(|r| r.value) == Some(5));
        check!(reg.get_versioned::<i64, i64>("v").is_none());
        check!(reg.get_versioned::<String, i64>("missing").is_none());
    }

    #[tokio::test]
    async fn register_and_downcast_typed_store() {
        let mut reg = StoreRegistry::default();
        reg.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "counts".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "c-changelog".into(),
        )));
        let s = reg.get_kv::<String, i64>("counts").unwrap();
        s.put("x".into(), 5).await;
        check!(s.get(&"x".to_string()).await == Some(5));
        // wrong types → None
        check!(reg.get_kv::<i64, i64>("counts").is_none());
        check!(reg.get_kv::<String, i64>("missing").is_none());
    }
}
