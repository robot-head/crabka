//! A `StreamTask` = one active task `(subtopology_id, partition)`. Owns the
//! instantiated graph + per-partition fetch offsets. At-least-once: produce →
//! flush → commit.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StreamsClientError;
use crate::membership::TopicPartition;
use crate::processor::graph::Graph;
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};

#[allow(dead_code)]
pub(crate) struct StreamTask {
    subtopology_id: String,
    graph: Graph,
    positions: HashMap<(String, i32), i64>,
    pending: HashMap<(String, i32), i64>,
    producer: Arc<dyn RecordProducer>,
    store: Arc<dyn OffsetStore>,
}

impl StreamTask {
    #[allow(dead_code)]
    pub fn new(
        subtopology_id: String,
        graph: Graph,
        sources: Vec<TopicPartition>,
        producer: Arc<dyn RecordProducer>,
        store: Arc<dyn OffsetStore>,
    ) -> Self {
        let positions = sources
            .into_iter()
            .map(|tp| ((tp.topic, tp.partition), 0))
            .collect();
        Self {
            subtopology_id,
            graph,
            positions,
            pending: HashMap::new(),
            producer,
            store,
        }
    }

    #[allow(dead_code)]
    #[must_use]
    pub fn subtopology_id(&self) -> &str {
        &self.subtopology_id
    }

    /// Seek each assigned partition to its committed offset, or `earliest` if
    /// none (auto.offset.reset = earliest).
    #[allow(dead_code)]
    pub async fn seek_to_start(&mut self) -> Result<(), StreamsClientError> {
        let keys: Vec<(String, i32)> = self.positions.keys().cloned().collect();
        for (topic, partition) in keys {
            let start = match self.store.committed(&topic, partition).await? {
                Some(o) => o,
                None => self.store.earliest(&topic, partition).await?,
            };
            self.positions.insert((topic, partition), start);
        }
        Ok(())
    }

    /// Fetch one batch per assigned partition; pipe through the graph; produce
    /// sink outputs; advance offsets.
    #[allow(dead_code)]
    pub async fn process_once(
        &mut self,
        fetcher: &dyn RecordFetcher,
    ) -> Result<(), StreamsClientError> {
        let keys: Vec<(String, i32)> = self.positions.keys().cloned().collect();
        for (topic, partition) in keys {
            let offset = self.positions[&(topic.clone(), partition)];
            let batch = fetcher.fetch(&topic, partition, offset).await?;
            for rec in &batch.records {
                self.graph
                    .pipe(
                        &topic,
                        rec.key.as_deref(),
                        rec.value.as_deref().unwrap_or(&[]),
                        rec.timestamp,
                    )
                    .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
                for out in self.graph.take_output() {
                    self.producer.send(&out.topic, out.key, out.value).await?;
                }
            }
            let next = batch.next_offset(offset);
            self.positions.insert((topic.clone(), partition), next);
            self.pending.insert((topic, partition), next);
        }
        Ok(())
    }

    /// At-least-once commit: flush producer THEN commit advanced source offsets.
    #[allow(dead_code)]
    pub async fn commit(&mut self) -> Result<(), StreamsClientError> {
        self.producer.flush().await?;
        if self.pending.is_empty() {
            return Ok(());
        }
        let offsets: Vec<(String, i32, i64)> = self
            .pending
            .iter()
            .map(|((t, p), o)| (t.clone(), *p, *o))
            .collect();
        self.store.commit(&offsets).await?;
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::TopicPartition;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::StringSerde;
    use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
    use crate::topology::Topology;
    use assert2::check;
    use std::collections::HashMap;
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
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "up",
            || {
                Box::new(Upper)
                    as Box<dyn crate::processor::api::Processor<String, String, String, String>>
            },
            ["src"],
        );
        t.add_sink("out", "out", ["up"], StringSerde, StringSerde);
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

    #[tokio::test]
    async fn processes_batch_produces_and_commits() {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());
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
        let mut task = StreamTask::new(
            "0".into(),
            built().instantiate().unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
        );
        task.seek_to_start().await.unwrap(); // no committed → earliest (0)
        task.process_once(&fetcher).await.unwrap(); // fetch+pipe+produce
        task.commit().await.unwrap(); // flush + commit
        check!(
            producer
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|(t, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref()))
        );
        check!(*producer.flushes.lock().unwrap() >= 1);
        check!(store.committed.lock().unwrap().get(&("in".to_string(), 0)) == Some(&1)); // next offset after offset 0
    }
}
