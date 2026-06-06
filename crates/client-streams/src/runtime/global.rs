//! `GlobalStateManager`: the shared, fully-replicated global stores for a
//! `KafkaStreams` instance. Built once from the topology's global store factories;
//! populated by the global consumer (reading all partitions of each global source
//! topic) and read by stream-globaltable join processors. One per app, shared via
//! `Arc` into every task's dispatch.
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::store::backend::StoreBackend;
use crate::store::registry::StoreRegistry;

#[derive(Clone, Default)]
pub(crate) struct GlobalStateManager {
    stores: Arc<Mutex<StoreRegistry>>,
    /// `store_name -> source_topic`, so the consumer knows which topic feeds each store.
    topics: Arc<HashMap<String, String>>,
}

impl GlobalStateManager {
    /// Build the global stores from the topology's global factories.
    /// `topic_for` maps each global store name to its source topic (the consumer reads it).
    // Consumed by the global consumer / dispatch wiring in T7/T8.
    #[allow(dead_code)]
    pub(crate) async fn build(
        factories: &HashMap<String, (Option<String>, crate::topology::builder::StoreFactory)>,
        topic_for: HashMap<String, String>,
        backend: &StoreBackend,
        app_id: &str,
    ) -> Self {
        let mut reg = StoreRegistry::default();
        for (name, (changelog_override, factory)) in factories {
            let changelog = changelog_override.clone().unwrap_or_default(); // global store: no changelog
            let bytes = backend.open(app_id, name).await;
            reg.insert(factory(name, changelog, bytes));
        }
        Self {
            stores: Arc::new(Mutex::new(reg)),
            topics: Arc::new(topic_for),
        }
    }

    /// Apply one consumed record into the named global store (raw bytes — the
    /// consumer's path). `value = None` deletes (tombstone).
    // Consumed by the global consumer in T7.
    #[allow(dead_code)]
    pub(crate) async fn apply(&self, store: &str, key: Bytes, value: Option<Bytes>) {
        let mut g = self.stores.lock().await;
        if let Some(s) = g.get_mut(store) {
            s.apply_changelog(key, value).await;
        }
    }

    /// Typed read for a join lookup. Returns an owned value (clones out from under
    /// the lock) so no borrow escapes the guard.
    // Consumed by the stream-globaltable join dispatch wiring in T8.
    #[allow(dead_code)]
    pub(crate) async fn get<K: Send + Sync + 'static, V: Send + 'static>(
        &self,
        store: &str,
        key: &K,
    ) -> Option<V> {
        let mut g = self.stores.lock().await;
        let s = g.get_kv::<K, V>(store)?;
        s.get(key).await
    }

    /// The `(store_name, source_topic)` pairs the consumer must bootstrap.
    // Consumed by the global consumer in T7.
    #[allow(dead_code)]
    pub(crate) fn store_topics(&self) -> &HashMap<String, String> {
        &self.topics
    }

    /// Whether there are no global stores (the common case — skip the consumer).
    // Consumed by the dispatch wiring in T8.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::serde::{Consumed, StringSerde};
    use crate::topology::Topology;
    use assert2::check;

    /// Build a one-entry `GlobalStateManager` over a `KeyValueBytesStore<String,String>`
    /// named "g", fed by source topic "gtopic", using the real `add_global_store`
    /// build path. Returns the manager (factories + store->topic map come straight
    /// from `BuiltTopology`).
    async fn one_store_manager() -> GlobalStateManager {
        let mut t = Topology::new();
        t.add_global_store::<String, String, _, _>(
            "g",
            "gsrc",
            "gtopic",
            "gproc",
            Consumed::with(StringSerde, StringSerde),
        );
        // A topology needs a non-global source/sink to build (global is invisible).
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_sink(
            "snk",
            "out",
            [&src],
            crate::processor::serde::Produced::with(StringSerde, StringSerde),
        );
        let built = t.build("app").unwrap();
        GlobalStateManager::build(
            built.global_store_factories_for_test(),
            built.global_store_topics(),
            &StoreBackend::InMemory,
            "app",
        )
        .await
    }

    #[tokio::test]
    async fn build_apply_get_round_trip() {
        let mgr = one_store_manager().await;
        // The consumer's write path: raw consumed bytes via apply_changelog.
        mgr.apply("g", Bytes::from("k"), Some(Bytes::from("v")))
            .await;
        // The join read path: typed get clones the value out from under the lock.
        let got: Option<String> = mgr.get::<String, String>("g", &"k".to_string()).await;
        check!(got == Some("v".to_string()));
    }

    #[tokio::test]
    async fn tombstone_removes_entry() {
        let mgr = one_store_manager().await;
        mgr.apply("g", Bytes::from("k"), Some(Bytes::from("v")))
            .await;
        check!(mgr.get::<String, String>("g", &"k".to_string()).await == Some("v".to_string()));
        // value = None is a tombstone delete.
        mgr.apply("g", Bytes::from("k"), None).await;
        check!(mgr.get::<String, String>("g", &"k".to_string()).await == None);
    }

    #[tokio::test]
    async fn store_topics_maps_store_to_source_topic() {
        let mgr = one_store_manager().await;
        let topics = mgr.store_topics();
        check!(topics.get("g") == Some(&"gtopic".to_string()));
        check!(!mgr.is_empty());
    }
}
