//! Owns the active `StreamTask`s and drives poll/commit. Reconciles to the
//! membership's active assignment.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StreamsClientError;
use crate::membership::StreamsAssignment;
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};
use crate::runtime::task::StreamTask;
use crate::topology::BuiltTopology;

pub(crate) struct StreamThread {
    tasks: HashMap<(String, i32), StreamTask>,
    /// Shared fetcher reference kept for restore (replaying changelog on task creation).
    fetcher: Arc<dyn RecordFetcher>,
    /// Storage backend to use when instantiating new task graphs.
    backend: crate::store::backend::StoreBackend,
    /// Application ID passed to `instantiate` for changelog-name derivation and
    /// backend path construction.
    application_id: String,
}

impl StreamThread {
    pub fn new(
        fetcher: Arc<dyn RecordFetcher>,
        backend: crate::store::backend::StoreBackend,
        application_id: String,
    ) -> Self {
        Self {
            tasks: HashMap::new(),
            fetcher,
            backend,
            application_id,
        }
    }

    #[cfg(test)]
    #[must_use]
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Reconcile active tasks to `assignment.active`. standby/warmup ignored.
    pub async fn apply_assignment(
        &mut self,
        assignment: &StreamsAssignment,
        topology: &BuiltTopology,
        producer: &Arc<dyn RecordProducer>,
        store: &Arc<dyn OffsetStore>,
    ) -> Result<(), StreamsClientError> {
        // Desired (subtopology_id, partition) -> the owning TaskAssignment.
        let mut desired: HashMap<(String, i32), &crate::membership::TaskAssignment> =
            HashMap::new();
        for ta in &assignment.active {
            for &p in &ta.partitions {
                desired.insert((ta.subtopology_id.clone(), p), ta);
            }
        }

        // Drop removed: close processors, commit, then drop.
        let to_remove: Vec<(String, i32)> = self
            .tasks
            .keys()
            .filter(|k| !desired.contains_key(*k))
            .cloned()
            .collect();
        for k in to_remove {
            if let Some(mut t) = self.tasks.remove(&k) {
                t.close_processors();
                t.commit().await?;
            }
        }

        // Add new.
        for (key, ta) in desired {
            if self.tasks.contains_key(&key) {
                continue;
            }
            let graph = topology
                .instantiate(&self.backend, &self.application_id)
                .await
                .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
            let sources: Vec<crate::membership::TopicPartition> = ta
                .source_topic_partitions
                .iter()
                .filter(|tp| tp.partition == key.1)
                .cloned()
                .collect();
            let mut task = StreamTask::new(
                key.0.clone(),
                graph,
                sources,
                Arc::clone(producer),
                Arc::clone(store),
            );
            // Seek positions to committed offsets (or earliest) BEFORE restore so
            // that normal processing knows where to start after restore completes.
            task.seek_to_start().await?;
            // Restore state stores from changelog, then initialise processors.
            task.restore(&*self.fetcher).await?;
            task.init().await?;
            self.tasks.insert(key, task);
        }
        Ok(())
    }

    pub async fn poll_all(
        &mut self,
        fetcher: &dyn RecordFetcher,
    ) -> Result<(), StreamsClientError> {
        for task in self.tasks.values_mut() {
            task.process_once(fetcher).await?;
        }
        Ok(())
    }

    pub async fn commit_all(&mut self) -> Result<(), StreamsClientError> {
        for task in self.tasks.values_mut() {
            task.commit().await?;
        }
        Ok(())
    }

    /// Commit + drop all tasks (on Fenced / shutdown).
    pub async fn close_all(&mut self) -> Result<(), StreamsClientError> {
        self.commit_all().await?;
        self.tasks.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::{StreamsAssignment, TaskAssignment, TopicPartition};
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::{Consumed, I64Serde, Produced, StringSerde};
    use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
    use crate::topology::Topology;
    use assert2::check;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    // ─── stateless Upper processor ────────────────────────────────────────────

    struct Upper;
    #[async_trait::async_trait]
    impl Processor<String, String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink(
            "out",
            "out",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
        t.build("app").unwrap()
    }

    // ─── stateful Counter processor ───────────────────────────────────────────

    struct Counter;
    #[async_trait::async_trait]
    impl Processor<String, String, String, i64> for Counter {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, i64>,
            r: Record<String, String>,
        ) {
            let n = {
                let store = ctx.get_state_store::<String, i64>("counts").unwrap();
                let n = store.get(&r.value).await.unwrap_or(0) + 1;
                store.put(r.value.clone(), n).await;
                n
            };
            ctx.forward(Record::new(Some(r.value), n, r.timestamp));
        }
    }

    fn stateful_built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c], Produced::with(StringSerde, I64Serde));
        t.build("app").unwrap()
    }

    // ─── fakes ────────────────────────────────────────────────────────────────

    /// Returns one batch at its scripted (topic, partition, offset), then empty.
    struct ScriptedFetcher {
        scripts: StdMutex<HashMap<(String, i32, i64), FetchBatch>>,
    }

    impl ScriptedFetcher {
        fn new(scripts: Vec<((String, i32, i64), FetchBatch)>) -> Self {
            Self {
                scripts: StdMutex::new(scripts.into_iter().collect()),
            }
        }
    }

    #[async_trait::async_trait]
    impl RecordFetcher for ScriptedFetcher {
        async fn fetch(
            &self,
            t: &str,
            p: i32,
            o: i64,
        ) -> Result<FetchBatch, crate::StreamsClientError> {
            Ok(self
                .scripts
                .lock()
                .unwrap()
                .remove(&(t.to_string(), p, o))
                .unwrap_or_default())
        }
    }

    struct OneShot {
        batch: StdMutex<Option<FetchBatch>>,
    }

    #[async_trait::async_trait]
    impl RecordFetcher for OneShot {
        async fn fetch(
            &self,
            _t: &str,
            _p: i32,
            _o: i64,
        ) -> Result<FetchBatch, crate::StreamsClientError> {
            Ok(self.batch.lock().unwrap().take().unwrap_or_default())
        }
    }

    type SentRecord = (
        String,
        Option<i32>,
        Option<bytes::Bytes>,
        Option<bytes::Bytes>,
    );

    #[derive(Default)]
    struct CollectProducer {
        /// (topic, partition, key, value)
        sent: StdMutex<Vec<SentRecord>>,
        flushes: StdMutex<u32>,
    }

    #[async_trait::async_trait]
    impl RecordProducer for CollectProducer {
        async fn send(
            &self,
            topic: &str,
            partition: Option<i32>,
            k: Option<bytes::Bytes>,
            v: Option<bytes::Bytes>,
        ) -> Result<(), crate::StreamsClientError> {
            self.sent
                .lock()
                .unwrap()
                .push((topic.to_string(), partition, k, v));
            Ok(())
        }

        async fn flush(&self) -> Result<(), crate::StreamsClientError> {
            *self.flushes.lock().unwrap() += 1;
            Ok(())
        }
    }

    #[derive(Default)]
    struct MemStore {
        committed: StdMutex<HashMap<(String, i32), i64>>,
    }

    #[async_trait::async_trait]
    impl OffsetStore for MemStore {
        async fn committed(
            &self,
            t: &str,
            p: i32,
        ) -> Result<Option<i64>, crate::StreamsClientError> {
            Ok(self
                .committed
                .lock()
                .unwrap()
                .get(&(t.to_string(), p))
                .copied())
        }

        async fn earliest(&self, _t: &str, _p: i32) -> Result<i64, crate::StreamsClientError> {
            Ok(0)
        }

        async fn commit(
            &self,
            offs: &[(String, i32, i64)],
        ) -> Result<(), crate::StreamsClientError> {
            let mut m = self.committed.lock().unwrap();
            for (t, p, o) in offs {
                m.insert((t.clone(), *p), *o);
            }
            Ok(())
        }
    }

    fn assignment() -> StreamsAssignment {
        StreamsAssignment {
            active: vec![TaskAssignment {
                subtopology_id: "0".into(),
                partitions: vec![0],
                source_topic_partitions: vec![TopicPartition {
                    topic: "in".into(),
                    partition: 0,
                }],
            }],
            standby: vec![],
            warmup: vec![],
        }
    }

    fn empty_fetcher() -> Arc<dyn RecordFetcher> {
        // Returns empty for all fetches; used when restore has nothing to replay.
        Arc::new(ScriptedFetcher::new(vec![])) as Arc<dyn RecordFetcher>
    }

    // ─── tests ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn apply_assignment_creates_task_polls_commits() {
        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = built();
        let mut thread = StreamThread::new(
            empty_fetcher(),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
        );
        thread
            .apply_assignment(&assignment(), &built, &producer, &store)
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        let fetcher = OneShot {
            batch: StdMutex::new(Some(FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some("k".into()),
                    value: Some("hi".into()),
                    timestamp: -1,
                }],
            })),
        };
        thread.poll_all(&fetcher).await.unwrap();
        thread.commit_all().await.unwrap();
        check!(
            producer_c
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, _p, _k, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref()))
        );
        check!(
            store_c
                .committed
                .lock()
                .unwrap()
                .get(&("in".to_string(), 0))
                == Some(&1)
        );

        // empty assignment → task removed (close_processors + committed on the way out)
        thread
            .apply_assignment(&StreamsAssignment::default(), &built, &producer, &store)
            .await
            .unwrap();
        check!(thread.task_count() == 0);
    }

    /// Verify that `apply_assignment` replays changelog records into the task's
    /// store during restore, so that the first `process_once` continues from the
    /// restored count rather than from zero.
    #[tokio::test]
    async fn stateful_apply_assignment_restores_store_from_changelog() {
        // Changelog: key="a", value=i64 BE 7 at offset 0 on "app-counts-changelog".
        let cl_key = bytes::Bytes::copy_from_slice(b"a");
        let cl_val = bytes::Bytes::copy_from_slice(&7i64.to_be_bytes());
        let restore_fetcher: Arc<dyn RecordFetcher> = Arc::new(ScriptedFetcher::new(vec![(
            ("app-counts-changelog".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some(cl_key),
                    value: Some(cl_val),
                    timestamp: -1,
                }],
            },
        )]));

        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = stateful_built();

        let mut thread = StreamThread::new(
            Arc::clone(&restore_fetcher),
            crate::store::backend::StoreBackend::InMemory,
            "app".into(),
        );
        thread
            .apply_assignment(&assignment(), &built, &producer, &store)
            .await
            .unwrap();
        check!(thread.task_count() == 1);

        // Now process one "a" record.  Restored count is 7, so output must be 8.
        let process_fetcher = ScriptedFetcher::new(vec![(
            ("in".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: None,
                    value: Some("a".into()),
                    timestamp: -1,
                }],
            },
        )]);
        thread.poll_all(&process_fetcher).await.unwrap();
        thread.commit_all().await.unwrap();

        let sent = producer_c.sent.lock().unwrap();
        check!(
            sent.iter()
                .any(|(t, _p, _k, v)| t == "out"
                    && v.as_deref() == Some(8i64.to_be_bytes().as_ref())),
            "after restore with N=7, processing 'a' must emit count = 8"
        );
    }
}
