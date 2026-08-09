//! `GlobalStateManager`, the shared and fully-replicated global stores for a
//! `KafkaStreams` instance.
//!
//! The manager is built once from the topology's global store factories. The
//! global consumer fills it and reads all partitions of each global source
//! topic. The stream-globaltable join processors read it. There is one manager
//! per app, shared into every task's dispatch through an `Arc`.
use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::{
    error::StreamsClientError,
    runtime::io::{IsolationLevel, RecordFetcher},
    store::{backend::StoreBackend, registry::StoreRegistry},
};

#[derive(Clone, Default)]
pub(crate) struct GlobalStateManager {
    stores: Arc<Mutex<StoreRegistry>>,
    /// `store_name -> source_topic`, so the consumer knows which topic feeds
    /// each store.
    topics: Arc<HashMap<String, String>>,
}

impl GlobalStateManager {
    /// Build the global stores from the topology's global factories.
    ///
    /// `topic_for` maps each global store name to its source topic, and the
    /// consumer reads that map.
    #[tracing::instrument(
        name = "streams.global.build",
        level = "info",
        skip_all,
        fields(app_id = %app_id, stores = factories.len()),
    )]
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

    /// Apply one consumed record into the named global store. This is the
    /// consumer's path and it takes raw bytes. A `value` of `None` is a
    /// tombstone and deletes the entry.
    pub(crate) async fn apply(&self, store: &str, key: Bytes, value: Option<Bytes>) {
        let mut g = self.stores.lock().await;
        if let Some(s) = g.get_mut(store) {
            s.apply_changelog(key, value).await;
        }
    }

    /// Typed write, for the test and driver path. It mirrors `apply` but takes a
    /// typed K and V. The `TopologyTestDriver`'s `pipe_global` injects values
    /// straight into the shared store this way.
    pub(crate) async fn put<K: Send + Sync + 'static, V: Send + 'static>(
        &self,
        store: &str,
        key: K,
        value: V,
    ) {
        let mut g = self.stores.lock().await;
        if let Some(s) = g.get_kv::<K, V>(store) {
            s.put(key, value).await;
        }
    }

    /// Typed read for a join lookup. It returns an owned value, cloned out from
    /// under the lock, so no borrow escapes the guard.
    pub(crate) async fn get<K: Send + Sync + 'static, V: Send + 'static>(
        &self,
        store: &str,
        key: &K,
    ) -> Option<V> {
        let mut g = self.stores.lock().await;
        let s = g.get_kv::<K, V>(store)?;
        s.get(key).await
    }

    /// The `(store_name, source_topic)` pairs that the consumer must
    /// bootstrap.
    // `bootstrap`/`poll_once` iterate `self.topics` directly; this accessor exists
    // for the unit test only.
    #[allow(dead_code)]
    pub(crate) fn store_topics(&self) -> &HashMap<String, String> {
        &self.topics
    }

    /// Whether there are no global stores. This is the common case, and the
    /// caller then skips the consumer.
    // The thread guards live-poll on `global_offsets.is_empty()`, not this; test-only.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.topics.is_empty()
    }

    /// Bootstrap every global store.
    ///
    /// For each (store, source topic) pair the method reads all partitions from
    /// offset 0 to the end of the log and applies each record. It returns the
    /// per-`(topic, partition)` next-offset map, so a live poll can resume. It
    /// blocks until it drains every partition.
    #[tracing::instrument(
        name = "streams.global.bootstrap",
        level = "info",
        skip_all,
        fields(stores = self.topics.len()),
        err,
    )]
    pub(crate) async fn bootstrap(
        &self,
        fetcher: &dyn RecordFetcher,
    ) -> Result<HashMap<(String, i32), i64>, StreamsClientError> {
        // Clone (store, topic) pairs so no borrow of `self.topics` is held across
        // the awaits below.
        let store_topics: Vec<(String, String)> = self
            .topics
            .iter()
            .map(|(s, t)| (s.clone(), t.clone()))
            .collect();

        let mut offsets: HashMap<(String, i32), i64> = HashMap::new();
        for (store, topic) in &store_topics {
            for partition in fetcher.partitions(topic).await? {
                let mut offset: i64 = 0;
                loop {
                    let batch = fetcher
                        .fetch(topic, partition, offset, IsolationLevel::ReadUncommitted)
                        .await?;
                    if batch.records.is_empty() {
                        break;
                    }
                    let mut advanced = false;
                    for rec in &batch.records {
                        self.apply(
                            store,
                            rec.key.clone().unwrap_or_default(),
                            rec.value.clone(),
                        )
                        .await;
                        let next = rec.offset + 1;
                        if next > offset {
                            offset = next;
                            advanced = true;
                        }
                    }
                    // Infinite-loop guard: stop if no record advanced the offset.
                    if !advanced {
                        break;
                    }
                }
                offsets.insert((topic.clone(), partition), offset);
            }
        }
        Ok(offsets)
    }

    /// One live-update pass from the given resume offsets.
    ///
    /// The method fetches the new records on each `(topic, partition)`, applies
    /// them, and advances the offsets in place. It fetches one batch per
    /// partition and does not read to the end of the log, so the caller must
    /// repeat the call.
    #[tracing::instrument(
        name = "streams.global.poll_once",
        level = "debug",
        skip_all,
        fields(partitions = offsets.len()),
        err,
    )]
    pub(crate) async fn poll_once(
        &self,
        fetcher: &dyn RecordFetcher,
        offsets: &mut HashMap<(String, i32), i64>,
    ) -> Result<(), StreamsClientError> {
        // Map each topic back to its store so applies target the right store.
        let topic_to_store: HashMap<&String, &String> =
            self.topics.iter().map(|(s, t)| (t, s)).collect();

        // Snapshot the keys so we don't borrow `offsets` while mutating it.
        let keys: Vec<(String, i32)> = offsets.keys().cloned().collect();
        for (topic, partition) in keys {
            let Some(store) = topic_to_store.get(&topic).copied() else {
                continue;
            };
            let offset = offsets[&(topic.clone(), partition)];
            let batch = fetcher
                .fetch(&topic, partition, offset, IsolationLevel::ReadUncommitted)
                .await?;
            let mut next = offset;
            for rec in &batch.records {
                self.apply(
                    store,
                    rec.key.clone().unwrap_or_default(),
                    rec.value.clone(),
                )
                .await;
                if rec.offset + 1 > next {
                    next = rec.offset + 1;
                }
            }
            offsets.insert((topic, partition), next);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use assert2::check;

    use super::*;
    use crate::{
        processor::serde::{Consumed, StringSerde},
        runtime::io::{FetchBatch, FetchedRec},
        topology::{NodeHandle, Topology},
    };

    /// Build a one-entry `GlobalStateManager` over a
    /// `KeyValueBytesStore<String,String>` named "g", fed by the source topic
    /// "gtopic", through the real `add_global_store` build path.
    ///
    /// The helper returns the manager. The factories and the store-to-topic map
    /// come straight from `BuiltTopology`.
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        let built = t.build("app").unwrap();
        GlobalStateManager::build(
            built.global_store_factories(),
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

    // ─── global-consumer fakes ──────────────────────────────────────────────

    /// A multi-partition fetcher. It returns the scripted batch for a given
    /// `(topic, partition, offset)` once and then an empty batch. It overrides
    /// `partitions`, so `bootstrap` reads every partition.
    struct ScriptedFetcher {
        scripts: StdMutex<HashMap<(String, i32, i64), FetchBatch>>,
        partitions: HashMap<String, Vec<i32>>,
    }

    impl ScriptedFetcher {
        fn new(
            scripts: Vec<((String, i32, i64), FetchBatch)>,
            partitions: Vec<(&str, Vec<i32>)>,
        ) -> Self {
            Self {
                scripts: StdMutex::new(scripts.into_iter().collect()),
                partitions: partitions
                    .into_iter()
                    .map(|(t, ps)| (t.to_string(), ps))
                    .collect(),
            }
        }

        /// Add a scripted batch after construction, or replace one, for
        /// `poll_once`.
        fn script(&self, key: (String, i32, i64), batch: FetchBatch) {
            self.scripts.lock().unwrap().insert(key, batch);
        }
    }

    #[async_trait::async_trait]
    impl RecordFetcher for ScriptedFetcher {
        async fn fetch(
            &self,
            t: &str,
            p: i32,
            o: i64,
            _isolation: IsolationLevel,
        ) -> Result<FetchBatch, StreamsClientError> {
            Ok(self
                .scripts
                .lock()
                .unwrap()
                .remove(&(t.to_string(), p, o))
                .unwrap_or_default())
        }

        async fn partitions(&self, topic: &str) -> Result<Vec<i32>, StreamsClientError> {
            Ok(self
                .partitions
                .get(topic)
                .cloned()
                .unwrap_or_else(|| vec![0]))
        }
    }

    /// One record `(key, value)` on `(topic, partition, offset)`.
    fn one_rec(offset: i64, key: &str, value: &str) -> FetchBatch {
        FetchBatch {
            records: vec![FetchedRec {
                offset,
                key: Some(Bytes::from(key.to_string())),
                value: Some(Bytes::from(value.to_string())),
                timestamp: -1,
            }],
        }
    }

    /// Build a `GlobalStateManager` with one global store "g" fed by the topic
    /// "global". The consumer tests use it, and they script the topic
    /// "global".
    async fn global_topic_manager() -> GlobalStateManager {
        let mut t = Topology::new();
        t.add_global_store::<String, String, _, _>(
            "g",
            "gsrc",
            "global",
            "gproc",
            Consumed::with(StringSerde, StringSerde),
        );
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        let built = t.build("app").unwrap();
        GlobalStateManager::build(
            built.global_store_factories(),
            built.global_store_topics(),
            &StoreBackend::InMemory,
            "app",
        )
        .await
    }

    #[tokio::test]
    async fn bootstrap_reads_all_partitions() {
        let mgr = global_topic_manager().await;
        // Partition 0 carries "a"→"A"; partition 1 carries "b"→"B".
        let fetcher = ScriptedFetcher::new(
            vec![
                (("global".into(), 0, 0), one_rec(0, "a", "A")),
                (("global".into(), 1, 0), one_rec(0, "b", "B")),
            ],
            vec![("global", vec![0, 1])],
        );

        let offsets = mgr.bootstrap(&fetcher).await.unwrap();

        // Both partitions materialized into the single fully-replicated store.
        check!(mgr.get::<String, String>("g", &"a".to_string()).await == Some("A".to_string()));
        check!(mgr.get::<String, String>("g", &"b".to_string()).await == Some("B".to_string()));

        // Next-offset is one past the single record on each partition.
        check!(offsets.get(&("global".to_string(), 0)) == Some(&1));
        check!(offsets.get(&("global".to_string(), 1)) == Some(&1));
    }

    #[tokio::test]
    async fn poll_once_applies_new_record() {
        let mgr = global_topic_manager().await;
        let fetcher = ScriptedFetcher::new(
            vec![
                (("global".into(), 0, 0), one_rec(0, "a", "A")),
                (("global".into(), 1, 0), one_rec(0, "b", "B")),
            ],
            vec![("global", vec![0, 1])],
        );
        let mut offsets = mgr.bootstrap(&fetcher).await.unwrap();
        check!(mgr.get::<String, String>("g", &"a".to_string()).await == Some("A".to_string()));

        // A new record arrives on (global, 0, 1): "a"→"A2".
        fetcher.script(("global".into(), 0, 1), one_rec(1, "a", "A2"));
        mgr.poll_once(&fetcher, &mut offsets).await.unwrap();

        check!(mgr.get::<String, String>("g", &"a".to_string()).await == Some("A2".to_string()));
        // The offset for partition 0 advanced; partition 1 is unchanged.
        check!(offsets.get(&("global".to_string(), 0)) == Some(&2));
        check!(offsets.get(&("global".to_string(), 1)) == Some(&1));
    }
}
