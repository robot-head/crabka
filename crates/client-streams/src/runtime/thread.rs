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
}

impl StreamThread {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
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

        // Drop removed (commit first).
        let to_remove: Vec<(String, i32)> = self
            .tasks
            .keys()
            .filter(|k| !desired.contains_key(*k))
            .cloned()
            .collect();
        for k in to_remove {
            if let Some(mut t) = self.tasks.remove(&k) {
                t.commit().await?;
            }
        }

        // Add new.
        for (key, ta) in desired {
            if self.tasks.contains_key(&key) {
                continue;
            }
            let graph = topology
                .instantiate()
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
            task.seek_to_start().await?;
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
    use crate::processor::serde::{Consumed, Produced, StringSerde};
    use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
    use crate::topology::Topology;
    use assert2::check;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_processor(
            "up",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["src"],
        );
        t.add_sink(
            "out",
            "out",
            ["up"],
            Produced::with(StringSerde, StringSerde),
        );
        t.build("app").unwrap()
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

    #[derive(Default)]
    struct CollectProducer {
        sent: StdMutex<Vec<(String, Option<bytes::Bytes>)>>,
        flushes: StdMutex<u32>,
    }

    #[async_trait::async_trait]
    impl RecordProducer for CollectProducer {
        async fn send(
            &self,
            topic: &str,
            _k: Option<bytes::Bytes>,
            v: Option<bytes::Bytes>,
        ) -> Result<(), crate::StreamsClientError> {
            self.sent.lock().unwrap().push((topic.to_string(), v));
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

    #[tokio::test]
    async fn apply_assignment_creates_task_polls_commits() {
        let producer_c = Arc::new(CollectProducer::default());
        let store_c = Arc::new(MemStore::default());
        let producer: Arc<dyn RecordProducer> = Arc::clone(&producer_c) as _;
        let store: Arc<dyn OffsetStore> = Arc::clone(&store_c) as _;
        let built = built();
        let mut thread = StreamThread::new();
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
                .any(|(t, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref()))
        );
        check!(
            store_c
                .committed
                .lock()
                .unwrap()
                .get(&("in".to_string(), 0))
                == Some(&1)
        );

        // empty assignment → task removed (committed on the way out)
        thread
            .apply_assignment(&StreamsAssignment::default(), &built, &producer, &store)
            .await
            .unwrap();
        check!(thread.task_count() == 0);
    }
}
