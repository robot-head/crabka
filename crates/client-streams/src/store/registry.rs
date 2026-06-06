//! Per-task registry of erased stores + the typed downcast used by
//! `get_state_store`.

use std::collections::HashMap;

use crate::store::api::{KeyValueStore, StateStore};
use crate::store::kv::KeyValueBytesStore;

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

    /// Mutable erased access by name — the `StateStore` trait surface
    /// (`changelog_topic` / `take_changelog` / `apply_changelog` / `set_logging`) is
    /// available on the returned `&mut dyn StateStore`.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut dyn StateStore> {
        self.stores.get_mut(name).map(std::convert::AsMut::as_mut)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::store::kv::KeyValueBytesStore;
    use assert2::check;

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
