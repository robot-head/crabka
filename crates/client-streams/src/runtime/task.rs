//! A `StreamTask` = one active task `(subtopology_id, partition)`. Owns the
//! instantiated graph + per-partition fetch offsets. At-least-once: produce →
//! flush → commit.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::StreamsClientError;
use crate::membership::TopicPartition;
use crate::processor::graph::Graph;
use crate::runtime::io::{OffsetStore, RecordFetcher, RecordProducer};

pub(crate) struct StreamTask {
    // Stored for logging / debugging; no non-debug caller at present.
    #[allow(dead_code)]
    subtopology_id: String,
    graph: Graph,
    /// The co-partitioned partition index for all source + changelog topics.
    partition: i32,
    positions: HashMap<(String, i32), i64>,
    pending: HashMap<(String, i32), i64>,
    producer: Arc<dyn RecordProducer>,
    store: Arc<dyn OffsetStore>,
}

impl StreamTask {
    pub fn new(
        subtopology_id: String,
        graph: Graph,
        sources: Vec<TopicPartition>,
        producer: Arc<dyn RecordProducer>,
        store: Arc<dyn OffsetStore>,
    ) -> Self {
        let partition = sources.first().map_or(0, |tp| tp.partition);
        let positions = sources
            .into_iter()
            .map(|tp| ((tp.topic, tp.partition), 0))
            .collect();
        Self {
            subtopology_id,
            graph,
            partition,
            positions,
            pending: HashMap::new(),
            producer,
            store,
        }
    }

    /// Call `Processor::init` on every node in the graph.
    pub fn init(&mut self) -> Result<(), StreamsClientError> {
        self.graph
            .init_processors()
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }

    /// Call `Processor::close` on every node in the graph.
    pub fn close_processors(&mut self) {
        self.graph.close_processors();
    }

    /// Restore each store from its changelog topic (reads from offset 0 until
    /// an empty batch). Changelog logging is disabled for the duration.
    pub async fn restore(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError> {
        self.graph.set_logging(false);
        let names = self.graph.stores.names();
        for name in names {
            let changelog_topic = {
                let store = self.graph.stores.get_mut(&name).expect("store in registry");
                store.changelog_topic().to_string()
            };
            let mut offset: i64 = 0;
            loop {
                let batch = fetcher
                    .fetch(&changelog_topic, self.partition, offset)
                    .await?;
                if batch.records.is_empty() {
                    break;
                }
                let mut advanced = false;
                for rec in &batch.records {
                    self.graph.restore_apply(
                        &name,
                        rec.key.clone().unwrap_or_default(),
                        rec.value.clone(),
                    );
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
        }
        self.graph.set_logging(true);
        Ok(())
    }

    /// Seek each assigned partition to its committed offset, or `earliest` if
    /// none (auto.offset.reset = earliest).
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
    /// sink outputs AND changelog entries; then flush + commit on the next
    /// `commit()` call. At-least-once ordering: sink produce → changelog
    /// produce → flush → commit.
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
                    // Sink / repartition output: key-hash routing (partition = None).
                    self.producer
                        .send(&out.topic, None, out.key, out.value)
                        .await?;
                }
            }
            // Drain changelog entries AFTER all sink output for this partition
            // but BEFORE the flush/commit barrier (at-least-once).
            // Changelog sends are pinned to self.partition so restore() can
            // read them back by fetching only the task partition.
            for (cl_topic, key, value) in self.graph.drain_changelogs() {
                self.producer
                    .send(&cl_topic, Some(self.partition), Some(key), value)
                    .await?;
            }
            let next = batch.next_offset(offset);
            self.positions.insert((topic.clone(), partition), next);
            self.pending.insert((topic, partition), next);
        }
        Ok(())
    }

    /// At-least-once commit: flush producer THEN commit advanced source offsets.
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

    /// Test-only: typed read from a KV store by name.
    #[cfg(test)]
    fn store_get_i64(&mut self, name: &str, key: &String) -> Option<i64> {
        self.graph
            .stores
            .get_kv::<String, i64>(name)
            .and_then(|s| s.get(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::TopicPartition;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::{Consumed, I64Serde, Produced, StringSerde};
    use crate::runtime::io::{FetchBatch, FetchedRec, OffsetStore, RecordFetcher, RecordProducer};
    use crate::topology::Topology;
    use assert2::check;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    // --- stateful topology helpers ---

    struct Counter;
    impl Processor<String, String, String, i64> for Counter {
        fn process(&mut self, ctx: &mut ProcessorContext<String, i64>, r: Record<String, String>) {
            let store = ctx.get_state_store::<String, i64>("counts").unwrap();
            let n = store.get(&r.value).unwrap_or(0) + 1;
            store.put(r.value.clone(), n);
            ctx.forward(Record::new(Some(r.value), n, r.timestamp));
        }
    }

    fn stateful_built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_state_store("counts", StringSerde, I64Serde, ["c"]);
        t.add_processor("c", || Counter, ["src"]);
        t.add_sink("out", "out", ["c"], Produced::with(StringSerde, I64Serde));
        t.build("app").unwrap()
    }

    /// A fetcher that returns different batches per (topic, offset) key.
    /// Unscripted combinations return an empty batch.
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

    // ---

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
            || {
                Box::new(Upper)
                    as Box<dyn crate::processor::api::Processor<String, String, String, String>>
            },
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
                .any(|(t, _p, _k, v)| t == "out" && v.as_deref() == Some(b"HI".as_ref()))
        );
        check!(*producer.flushes.lock().unwrap() >= 1);
        check!(store.committed.lock().unwrap().get(&("in".to_string(), 0)) == Some(&1)); // next offset after offset 0
    }

    #[tokio::test]
    async fn stateful_task_produces_changelog_and_restores() {
        // ── (a) process: emit sink record AND changelog record ──────────────
        let producer_a = std::sync::Arc::new(CollectProducer::default());
        let store_a = std::sync::Arc::new(MemStore::default());
        // OneShot gives one "a" record on ("in", 0, 0); all other fetches return empty.
        let fetcher_a = ScriptedFetcher::new(vec![(
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
        let mut task_a = StreamTask::new(
            "0".into(),
            stateful_built().instantiate().unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer_a) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store_a) as std::sync::Arc<dyn OffsetStore>,
        );
        task_a.init().unwrap();
        task_a.process_once(&fetcher_a).await.unwrap();
        task_a.commit().await.unwrap();

        {
            let sent_a = producer_a.sent.lock().unwrap();
            let out_topics: Vec<&str> = sent_a.iter().map(|(t, _p, _k, _v)| t.as_str()).collect();
            check!(
                out_topics.contains(&"out"),
                "sink record must be produced to 'out'"
            );
            check!(
                out_topics.contains(&"app-counts-changelog"),
                "changelog record must be produced to 'app-counts-changelog'"
            );
        } // drop sent_a before any await

        // ── (b) restore: seed store from changelog, then verify count is N+1 ──
        // Changelog record: key = "a" (UTF-8), value = i64 BE 5.
        let cl_key = bytes::Bytes::copy_from_slice(b"a");
        let cl_val = bytes::Bytes::copy_from_slice(&5i64.to_be_bytes());

        let producer_b = std::sync::Arc::new(CollectProducer::default());
        let store_b = std::sync::Arc::new(MemStore::default());
        // Script: changelog returns one record at offset 0, then empty at offset 1.
        // Source "in" has no records (empty) at offset 0.
        let fetcher_b = ScriptedFetcher::new(vec![(
            ("app-counts-changelog".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some(cl_key),
                    value: Some(cl_val),
                    timestamp: -1,
                }],
            },
        )]);
        let mut task_b = StreamTask::new(
            "0".into(),
            stateful_built().instantiate().unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer_b) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store_b) as std::sync::Arc<dyn OffsetStore>,
        );
        task_b.restore(&fetcher_b).await.unwrap();

        // Direct accessor: store should have "a" → 5 from changelog restore.
        check!(
            task_b.store_get_i64("counts", &"a".to_string()) == Some(5),
            "restore must seed the store with the changelog value"
        );

        // Also verify: one more process_once with "a" emits count = 6 (N+1).
        let fetcher_b2 = ScriptedFetcher::new(vec![(
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
        task_b.process_once(&fetcher_b2).await.unwrap();
        let sent_b = producer_b.sent.lock().unwrap();
        // The "out" sink emits i64 value = 6 (big-endian)
        check!(
            sent_b
                .iter()
                .any(|(t, _p, _k, v)| t == "out"
                    && v.as_deref() == Some(6i64.to_be_bytes().as_ref())),
            "after restore with N=5, processing 'a' must emit count = 6"
        );
    }

    /// Regression test: changelog sends must be pinned to the task partition
    /// (matching the JVM `RecordCollector` behaviour). Sink sends must keep
    /// key-hash routing (partition == None).
    ///
    /// Uses a non-zero task partition (2) so the test is discriminating:
    /// a bug that passes `None` will fail the changelog assertion, and
    /// a bug that passes `Some(2)` for sink output will fail the sink assertion.
    #[tokio::test]
    async fn changelog_sends_pin_task_partition() {
        const TASK_PARTITION: i32 = 2;

        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());
        let fetcher = ScriptedFetcher::new(vec![(
            ("in".to_string(), TASK_PARTITION, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: None,
                    value: Some("x".into()),
                    timestamp: -1,
                }],
            },
        )]);

        let mut task = StreamTask::new(
            "0".into(),
            stateful_built().instantiate().unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: TASK_PARTITION,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
        );
        task.init().unwrap();
        task.process_once(&fetcher).await.unwrap();

        let sent = producer.sent.lock().unwrap();

        // Sink record (topic "out") must use key-hash routing: partition == None.
        let sink_rec = sent
            .iter()
            .find(|(t, _p, _k, _v)| t == "out")
            .expect("sink record must be produced to 'out'");
        check!(
            sink_rec.1.is_none(),
            "sink send must use key-hash routing (partition None), got {:?}",
            sink_rec.1
        );

        // Changelog record must be pinned to the task partition.
        let cl_rec = sent
            .iter()
            .find(|(t, _p, _k, _v)| t == "app-counts-changelog")
            .expect("changelog record must be produced to 'app-counts-changelog'");
        check!(
            cl_rec.1 == Some(TASK_PARTITION),
            "changelog send must be pinned to task partition {TASK_PARTITION}, got {:?}",
            cl_rec.1
        );
    }
}
