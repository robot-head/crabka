//! A `StreamTask` = one active task `(subtopology_id, partition)`. Owns the
//! instantiated graph + per-partition fetch offsets. At-least-once: produce →
//! flush → commit.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::StreamsClientError;
use crate::membership::TopicPartition;
use crate::processor::graph::Graph;
use crate::runtime::eos::ProcessingGuarantee;
use crate::runtime::io::{
    BeginTxnGate, IsolationLevel, OffsetStore, RecordFetcher, RecordProducer,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskRole {
    Active,
    Standby,
    Warmup,
}

pub(crate) struct StreamTask {
    // Stored for logging / debugging; no non-debug caller at present.
    #[allow(dead_code)]
    pub(crate) subtopology_id: String,
    pub(crate) graph: Graph,
    /// The co-partitioned partition index for all source + changelog topics.
    pub(crate) partition: i32,
    positions: HashMap<(String, i32), i64>,
    /// The source topics this task consumes. A store whose changelog topic is one
    /// of these is a `REUSE_KTABLE_SOURCE_TOPICS` reuse-source store, whose
    /// changelog write-back is suppressed (it would loop back onto the source).
    source_topics: HashSet<String>,
    pending: HashMap<(String, i32), i64>,
    producer: Arc<dyn RecordProducer>,
    pub(crate) store: Arc<dyn OffsetStore>,
    pub(crate) role: TaskRole,
    pub(crate) changelog_offsets: HashMap<String, i64>,
    /// Delivery guarantee for this task. Under [`ProcessingGuarantee::ExactlyOnceV2`]
    /// the changelog restore reads `READ_COMMITTED` so aborted writes are excluded.
    pub(crate) guarantee: ProcessingGuarantee,
}

impl StreamTask {
    pub fn new(
        subtopology_id: String,
        graph: Graph,
        sources: Vec<TopicPartition>,
        producer: Arc<dyn RecordProducer>,
        store: Arc<dyn OffsetStore>,
        role: TaskRole,
        guarantee: ProcessingGuarantee,
    ) -> Self {
        let partition = sources.first().map_or(0, |tp| tp.partition);
        let source_topics: HashSet<String> = sources.iter().map(|tp| tp.topic.clone()).collect();
        let positions = sources
            .into_iter()
            .map(|tp| ((tp.topic, tp.partition), 0))
            .collect();
        Self {
            subtopology_id,
            graph,
            partition,
            positions,
            source_topics,
            pending: HashMap::new(),
            producer,
            store,
            role,
            changelog_offsets: HashMap::new(),
            guarantee,
        }
    }

    /// Read-only access to this task's store registry (for interactive queries).
    pub(crate) fn registry(&self) -> &crate::store::registry::StoreRegistry {
        &self.graph.stores
    }

    /// Snapshot the task's consumed source offsets as an `IQv2` `Position`
    /// (topic → partition → next-offset). Used to tag query results.
    pub(crate) fn position(&self) -> crate::runtime::iqv2::request::Position {
        use std::collections::BTreeMap;
        let mut m: BTreeMap<String, BTreeMap<i32, i64>> = BTreeMap::new();
        for ((topic, p), off) in &self.positions {
            m.entry(topic.clone()).or_default().insert(*p, *off);
        }
        crate::runtime::iqv2::request::Position(m)
    }

    /// Call `Processor::init` on every node in the graph.
    #[tracing::instrument(
        name = "streams.task.init",
        level = "info",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition),
        err,
    )]
    pub async fn init(&mut self) -> Result<(), StreamsClientError> {
        self.graph
            .init_processors()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))
    }

    /// Clean close: flush record caches (emitting their buffered deduped changes +
    /// changelog through the still-live processor chain) BEFORE calling
    /// `Processor::close` on every node — mirrors the JVM `StreamTask.closeClean`
    /// (flush state stores → close processors). The flush must precede the processor
    /// close because forwarding routes through child `process` calls.
    ///
    /// Close is infallible, so a flush error is logged and swallowed (the partition
    /// is being revoked anyway; the thread still commits offsets afterwards). After
    /// this, the cache is clean, so the subsequent `commit()` flush is a no-op.
    #[tracing::instrument(
        name = "streams.task.close_processors",
        level = "info",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition),
    )]
    pub async fn close_processors(&mut self) {
        if let Err(e) = self.flush_caches().await {
            tracing::warn!(error = %e, "flush_caches failed during task close; continuing");
        }
        self.graph.close_processors().await;
    }

    /// Restore each store from its changelog topic (reads from offset 0 until
    /// an empty batch). Changelog logging is disabled for the duration.
    ///
    /// Under [`ProcessingGuarantee::ExactlyOnceV2`] the changelog is read at
    /// `READ_COMMITTED` so aborted writes (records from a transaction that later
    /// aborted) are excluded — the restored store reflects only committed state.
    /// At-least-once reads `READ_UNCOMMITTED` (behaviour unchanged).
    #[tracing::instrument(
        name = "streams.task.restore",
        level = "info",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition, guarantee = ?self.guarantee),
        err,
    )]
    pub async fn restore(&mut self, fetcher: &dyn RecordFetcher) -> Result<(), StreamsClientError> {
        let isolation = if self.guarantee == ProcessingGuarantee::ExactlyOnceV2 {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::ReadUncommitted
        };
        self.graph.set_logging(false);
        let names = self.graph.stores.names();
        for name in names {
            let changelog_topic = {
                let store = self.graph.stores.get_mut(&name).expect("store in registry");
                store.changelog_topic().to_string()
            };
            let mut offset = *self
                .changelog_offsets
                .entry(changelog_topic.clone())
                .or_insert(0);
            loop {
                let batch = fetcher
                    .fetch(&changelog_topic, self.partition, offset, isolation)
                    .await?;
                if batch.records.is_empty() {
                    break;
                }
                let mut advanced = false;
                for rec in &batch.records {
                    self.graph
                        .restore_apply(
                            &name,
                            rec.key.clone().unwrap_or_default(),
                            rec.value.clone(),
                            rec.timestamp,
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
            self.changelog_offsets.insert(changelog_topic, offset);
        }
        self.graph.set_logging(true);
        Ok(())
    }

    /// Incrementally restore standby/warmup task by fetching a single batch
    /// from each store's changelog topic.
    #[tracing::instrument(
        name = "streams.task.restore_step",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition, role = ?self.role),
        err,
    )]
    pub async fn restore_step(
        &mut self,
        fetcher: &dyn RecordFetcher,
    ) -> Result<(), StreamsClientError> {
        let isolation = if self.guarantee == ProcessingGuarantee::ExactlyOnceV2 {
            IsolationLevel::ReadCommitted
        } else {
            IsolationLevel::ReadUncommitted
        };
        self.graph.set_logging(false);
        let names = self.graph.stores.names();
        for name in names {
            let changelog_topic = {
                let store = self.graph.stores.get_mut(&name).expect("store in registry");
                store.changelog_topic().to_string()
            };
            let offset = *self
                .changelog_offsets
                .entry(changelog_topic.clone())
                .or_insert(0);
            let batch = fetcher
                .fetch(&changelog_topic, self.partition, offset, isolation)
                .await?;
            let mut next_offset = offset;
            for rec in &batch.records {
                self.graph
                    .restore_apply(
                        &name,
                        rec.key.clone().unwrap_or_default(),
                        rec.value.clone(),
                        rec.timestamp,
                    )
                    .await;
                if rec.offset + 1 > next_offset {
                    next_offset = rec.offset + 1;
                }
            }
            self.changelog_offsets.insert(changelog_topic, next_offset);
        }
        self.graph.set_logging(true);
        Ok(())
    }

    /// Compute cumulative restored and end offsets across all stores' changelog partitions.
    #[tracing::instrument(
        name = "streams.task.compute_changelog_offsets",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition, role = ?self.role),
        err,
    )]
    pub async fn compute_changelog_offsets(&mut self) -> Result<(i64, i64), StreamsClientError> {
        let mut current_sum = 0;
        let mut end_sum = 0;
        let names = self.graph.stores.names();
        for name in names {
            let changelog_topic = {
                let store = self.graph.stores.get_mut(&name).expect("store in registry");
                store.changelog_topic().to_string()
            };
            let end_offset = self.store.latest(&changelog_topic, self.partition).await?;
            let current_offset = if self.role == TaskRole::Active {
                end_offset
            } else {
                *self
                    .changelog_offsets
                    .entry(changelog_topic.clone())
                    .or_insert(0)
            };
            current_sum += current_offset;
            end_sum += end_offset;
        }
        Ok((current_sum, end_sum))
    }

    /// Roll back to the last committed state after a txn abort: rewind source
    /// positions to committed offsets, wipe stores, re-restore from the committed
    /// changelog. Reuses [`seek_to_start`](Self::seek_to_start) + [`restore`](Self::restore).
    #[tracing::instrument(
        name = "streams.task.rollback",
        level = "info",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition),
        err,
    )]
    pub async fn rollback(
        &mut self,
        fetcher: &dyn RecordFetcher,
    ) -> Result<(), StreamsClientError> {
        self.pending.clear();
        self.seek_to_start().await?; // positions ← committed (or earliest)
        self.graph.clear_stores().await;
        self.restore(fetcher).await?; // replay committed changelog
        Ok(())
    }
    /// Seek each assigned partition to its committed offset, or `earliest` if
    /// none (auto.offset.reset = earliest).
    #[tracing::instrument(
        name = "streams.task.seek_to_start",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition),
        err,
    )]
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
    ///
    /// `begin_gate` is `Some` only under EOS-v2: the task calls
    /// [`BeginTxnGate::ensure_begun`] right before its first produced record so
    /// the thread opens a transaction lazily — an interval that fetches no
    /// records produces nothing and opens no transaction (no empty-txn churn).
    /// Under at-least-once `begin_gate` is `None` and this is a no-op gate.
    #[tracing::instrument(
        name = "streams.task.process_once",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition, eos = begin_gate.is_some()),
        err,
    )]
    pub async fn process_once(
        &mut self,
        fetcher: &dyn RecordFetcher,
        mut begin_gate: Option<&mut dyn BeginTxnGate>,
    ) -> Result<(), StreamsClientError> {
        let keys: Vec<(String, i32)> = self.positions.keys().cloned().collect();
        for (topic, partition) in keys {
            let offset = self.positions[&(topic.clone(), partition)];
            // Source records: normal processing reads READ_UNCOMMITTED.
            let batch = fetcher
                .fetch(&topic, partition, offset, IsolationLevel::ReadUncommitted)
                .await?;
            // An empty batch advances nothing and produces nothing — skip it so
            // the EOS begin-gate is not tripped by an idle partition.
            if batch.records.is_empty() {
                continue;
            }
            // EOS: open the transaction lazily before the first produced record
            // of this interval. Idempotent across partitions (begins once).
            if let Some(gate) = begin_gate.as_deref_mut() {
                gate.ensure_begun().await?;
            }
            for rec in &batch.records {
                self.graph
                    .pipe(
                        &topic,
                        rec.key.as_deref(),
                        rec.value.as_deref().unwrap_or(&[]),
                        rec.timestamp,
                    )
                    .await
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
            for (cl_topic, key, value, ts_opt) in self.graph.drain_changelogs(&self.source_topics) {
                self.producer
                    .send_with_timestamp(&cl_topic, Some(self.partition), Some(key), value, ts_opt)
                    .await?;
            }
            // Fire any due STREAM_TIME punctuators after this partition's batch,
            // at the graph's current stream-time. Their forwarded records flow
            // through the same sink-produce + changelog-drain path as records.
            self.punctuate_stream_time().await?;
            let next = batch.next_offset(offset);
            self.positions.insert((topic.clone(), partition), next);
            self.pending.insert((topic, partition), next);
        }
        Ok(())
    }

    /// Fire all due `STREAM_TIME` punctuators at the graph's current stream-time,
    /// producing any forwarded sink output + changelog entries. Driven at the end
    /// of each `process_once` batch.
    #[tracing::instrument(
        name = "streams.task.punctuate_stream_time",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition, stream_time = self.graph.stream_time),
        err,
    )]
    pub async fn punctuate_stream_time(&mut self) -> Result<(), StreamsClientError> {
        self.graph
            .punctuate_stream_time(self.graph.stream_time)
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
        self.drain_punctuation_output().await
    }

    /// Fire all due `WALL_CLOCK_TIME` punctuators at `now_ms`, producing any
    /// forwarded sink output + changelog entries. Driven between polls by the
    /// `StreamThread` wall-clock tick.
    #[tracing::instrument(
        name = "streams.task.punctuate_wall_clock",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition, now_ms),
        err,
    )]
    pub async fn punctuate_wall_clock(&mut self, now_ms: i64) -> Result<(), StreamsClientError> {
        self.graph
            .punctuate_wall_clock(now_ms)
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
        self.drain_punctuation_output().await
    }

    /// Route punctuator-forwarded records through the same producer plumbing
    /// that `process_once` uses for record output: sink sends use key-hash
    /// routing (partition None); changelog sends pin the task partition.
    async fn drain_punctuation_output(&mut self) -> Result<(), StreamsClientError> {
        for out in self.graph.take_output() {
            self.producer
                .send(&out.topic, None, out.key, out.value)
                .await?;
        }
        for (cl_topic, key, value, ts_opt) in self.graph.drain_changelogs(&self.source_topics) {
            self.producer
                .send_with_timestamp(&cl_topic, Some(self.partition), Some(key), value, ts_opt)
                .await?;
        }
        Ok(())
    }

    /// The source offsets advanced since the last commit (for the thread's txn).
    /// The thread (not the task) drives the EOS commit, so it reads pending
    /// offsets here, folds them into `send_offsets_to_transaction`, and clears
    /// them via [`clear_pending`](Self::clear_pending) once the txn commits.
    pub fn pending_offsets(&self) -> Vec<(String, i32, i64)> {
        self.pending
            .iter()
            .map(|((t, p), o)| (t.clone(), *p, *o))
            .collect()
    }

    /// Clear pending after the thread's EOS txn commit succeeds.
    pub fn clear_pending(&mut self) {
        self.pending.clear();
    }

    /// Flush every cached materialized store: write dirty entries through to the
    /// underlying store, buffer their changelog records, and forward the deduped
    /// `Change`s downstream — then route the resulting sink output + changelog
    /// entries to the producer (same plumbing the punctuation path uses).
    ///
    /// A no-op when no store is cached (`cache_owner` empty, e.g. the
    /// `cache_max_bytes = 0` test-driver path): `flush_caches` forwards nothing,
    /// so the drain produces nothing.
    #[tracing::instrument(
        name = "streams.task.flush_caches",
        level = "debug",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition),
        err,
    )]
    pub(crate) async fn flush_caches(&mut self) -> Result<(), StreamsClientError> {
        self.graph
            .flush_caches()
            .await
            .map_err(|e| StreamsClientError::Runtime(e.to_string()))?;
        self.drain_punctuation_output().await
    }

    /// At-least-once commit: flush record caches (emitting their deduped changes +
    /// changelog) → flush producer → commit advanced source offsets. The cache
    /// flush + its sink/changelog drain happen BEFORE the producer flush/commit so
    /// the forwarded records are part of the committed batch (under EOS-v2, part of
    /// the transaction the thread commits after this call).
    #[tracing::instrument(
        name = "streams.task.commit",
        level = "info",
        skip_all,
        fields(subtopology = %self.subtopology_id, partition = self.partition),
        err,
    )]
    pub async fn commit(&mut self) -> Result<(), StreamsClientError> {
        self.flush_caches().await?;
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
    pub(crate) async fn store_get_i64(&mut self, name: &str, key: &String) -> Option<i64> {
        match self.graph.stores.get_kv::<String, i64>(name) {
            Some(s) => s.get(key).await,
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::TopicPartition;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::{I64Serde, StringSerde};
    use crate::runtime::io::{
        FetchBatch, FetchedRec, IsolationLevel, OffsetStore, RecordFetcher, RecordProducer,
    };
    use crate::topology::{NodeHandle, Topology};
    use assert2::check;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    // --- stateful topology helpers ---

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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c]);
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
            _isolation: IsolationLevel,
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink("out", "out", [&up]);
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
            _isolation: IsolationLevel,
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
        latest: StdMutex<HashMap<(String, i32), i64>>,
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

        async fn latest(&self, t: &str, p: i32) -> Result<i64, crate::StreamsClientError> {
            Ok(self
                .latest
                .lock()
                .unwrap()
                .get(&(t.to_string(), p))
                .copied()
                .unwrap_or(0))
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
            built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            ProcessingGuarantee::AtLeastOnce,
        );
        task.seek_to_start().await.unwrap(); // no committed → earliest (0)
        task.process_once(&fetcher, None).await.unwrap(); // fetch+pipe+produce
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
            stateful_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer_a) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store_a) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            ProcessingGuarantee::AtLeastOnce,
        );
        task_a.init().await.unwrap();
        task_a.process_once(&fetcher_a, None).await.unwrap();
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
            stateful_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer_b) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store_b) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            ProcessingGuarantee::AtLeastOnce,
        );
        task_b.restore(&fetcher_b).await.unwrap();

        // Direct accessor: store should have "a" → 5 from changelog restore.
        check!(
            task_b.store_get_i64("counts", &"a".to_string()).await == Some(5),
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
        task_b.process_once(&fetcher_b2, None).await.unwrap();
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

    // ── stream-time punctuation driven from process_once ─────────────────────

    struct EmitTs;
    #[async_trait::async_trait]
    impl crate::processor::punctuation::Punctuator<String, i64> for EmitTs {
        async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, ts: i64) {
            ctx.forward(Record::new(None, ts, ts));
        }
    }

    /// Schedules a `STREAM_TIME` punctuator (interval 10ms) in `init`; no-op on
    /// records (so any sink output is from the punctuator, not the record).
    struct StreamTimeScheduler;
    #[async_trait::async_trait]
    impl Processor<String, String, String, i64> for StreamTimeScheduler {
        async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
            ctx.schedule(
                std::time::Duration::from_millis(10),
                crate::processor::punctuation::PunctuationType::StreamTime,
                EmitTs,
            );
        }
        async fn process(
            &mut self,
            _ctx: &mut ProcessorContext<'_, '_, String, i64>,
            _r: Record<String, String>,
        ) {
        }
    }

    fn stream_time_punct_built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let p = t.add_processor("p", || StreamTimeScheduler, [&src]);
        t.add_sink("out", "out", [&p]);
        t.build("app").unwrap()
    }

    /// `process_once` must fire due `STREAM_TIME` punctuators after the batch, at
    /// the graph's current stream-time, and produce their forwarded output. We
    /// feed one record at ts=25 (> the 10ms interval base of `i64::MIN`+10), so the
    /// punctuator fires once with value = stream-time (25) and the sink emits it.
    #[tokio::test]
    async fn process_once_fires_stream_time_punctuation() {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());
        let fetcher = ScriptedFetcher::new(vec![(
            ("in".to_string(), 0, 0),
            FetchBatch {
                records: vec![FetchedRec {
                    offset: 0,
                    key: Some("k".into()),
                    value: Some("v".into()),
                    timestamp: 25,
                }],
            },
        )]);
        let mut task = StreamTask::new(
            "0".into(),
            stream_time_punct_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            crate::runtime::eos::ProcessingGuarantee::AtLeastOnce,
        );
        task.init().await.unwrap(); // schedules the punctuator (base i64::MIN)
        task.process_once(&fetcher, None).await.unwrap(); // pipe ts=25 → stream-time=25 → fire

        let sent = producer.sent.lock().unwrap();
        check!(
            sent.iter()
                .any(|(t, _p, _k, v)| t == "out"
                    && v.as_deref() == Some(25i64.to_be_bytes().as_ref())),
            "stream-time punctuator must fire from process_once and emit value=25, got {sent:?}"
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
            stateful_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: TASK_PARTITION,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            ProcessingGuarantee::AtLeastOnce,
        );
        task.init().await.unwrap();
        task.process_once(&fetcher, None).await.unwrap();

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

    #[tokio::test]
    async fn restore_step_replays_increments_and_advances_offsets() {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());

        let cl_key = bytes::Bytes::copy_from_slice(b"a");
        let cl_val = bytes::Bytes::copy_from_slice(&12i64.to_be_bytes());
        let fetcher = ScriptedFetcher::new(vec![(
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

        let mut task = StreamTask::new(
            "0".into(),
            stateful_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Standby,
            ProcessingGuarantee::AtLeastOnce,
        );

        // Run a single restore step.
        task.restore_step(&fetcher).await.unwrap();

        // Check store state.
        check!(
            task.store_get_i64("counts", &"a".to_string()).await == Some(12),
            "restore_step must replay changelog record to store"
        );
        // Check updated offset.
        check!(
            task.changelog_offsets.get("app-counts-changelog") == Some(&1),
            "restore_step must advance tracked offset to 1"
        );
    }

    #[tokio::test]
    async fn compute_changelog_offsets_calculates_correct_sums() {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());

        // Configure end offset as 15.
        store
            .latest
            .lock()
            .unwrap()
            .insert(("app-counts-changelog".to_string(), 0), 15);

        let mut task = StreamTask::new(
            "0".into(),
            stateful_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Warmup,
            ProcessingGuarantee::AtLeastOnce,
        );

        // Warmup: current offset is tracked changelog offset (initially 0).
        let (curr, end) = task.compute_changelog_offsets().await.unwrap();
        check!(curr == 0);
        check!(end == 15);

        // Advance tracked offset to 10.
        task.changelog_offsets
            .insert("app-counts-changelog".to_string(), 10);
        let (curr, end) = task.compute_changelog_offsets().await.unwrap();
        check!(curr == 10);
        check!(end == 15);

        // Active: current offset equals end offset (lag is 0).
        task.role = TaskRole::Active;
        let (curr, end) = task.compute_changelog_offsets().await.unwrap();
        check!(curr == 15);
        check!(end == 15);
    }

    /// A fetcher that returns DIFFERENT changelog batches depending on the
    /// requested [`IsolationLevel`]. For the `app-counts-changelog` topic at
    /// offset 0:
    /// - `ReadUncommitted` returns `[committed("a"→5), aborted("b"→99)]`
    /// - `ReadCommitted`   returns `[committed("a"→5)]` only (the aborted write
    ///   from a rolled-back transaction is excluded, mirroring the broker's LSO
    ///   filtering).
    ///
    /// All other fetches return empty.
    struct IsolationFetcher;

    impl IsolationFetcher {
        fn changelog_value(n: i64) -> bytes::Bytes {
            bytes::Bytes::copy_from_slice(&n.to_be_bytes())
        }
    }

    #[async_trait::async_trait]
    impl RecordFetcher for IsolationFetcher {
        async fn fetch(
            &self,
            t: &str,
            p: i32,
            o: i64,
            isolation: IsolationLevel,
        ) -> Result<FetchBatch, crate::StreamsClientError> {
            if t == "app-counts-changelog" && p == 0 && o == 0 {
                let committed = FetchedRec {
                    offset: 0,
                    key: Some(bytes::Bytes::copy_from_slice(b"a")),
                    value: Some(Self::changelog_value(5)),
                    timestamp: -1,
                };
                let aborted = FetchedRec {
                    offset: 1,
                    key: Some(bytes::Bytes::copy_from_slice(b"b")),
                    value: Some(Self::changelog_value(99)),
                    timestamp: -1,
                };
                let records = match isolation {
                    // READ_COMMITTED excludes the aborted write.
                    IsolationLevel::ReadCommitted => vec![committed],
                    // READ_UNCOMMITTED sees both.
                    IsolationLevel::ReadUncommitted => vec![committed, aborted],
                };
                Ok(FetchBatch { records })
            } else {
                Ok(FetchBatch::default())
            }
        }
    }

    /// Build a stateful `Counter`-topology task with the given guarantee, restore
    /// it from the [`IsolationFetcher`] changelog, and return the task so the
    /// caller can inspect the restored `counts` store.
    async fn restore_counter_task(guarantee: ProcessingGuarantee) -> StreamTask {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());
        let mut task = StreamTask::new(
            "0".into(),
            stateful_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            guarantee,
        );
        task.restore(&IsolationFetcher).await.unwrap();
        task
    }

    /// EOS-v2 restore reads the changelog at `READ_COMMITTED`, so the aborted
    /// write ("b"→99) is excluded — only the committed record ("a"→5) seeds the
    /// store.
    #[tokio::test]
    async fn eos_restore_reads_committed_only() {
        let mut task = restore_counter_task(ProcessingGuarantee::ExactlyOnceV2).await;
        check!(
            task.store_get_i64("counts", &"a".to_string()).await == Some(5),
            "EOS restore must seed the committed changelog record"
        );
        check!(
            task.store_get_i64("counts", &"b".to_string()).await == None,
            "EOS restore (READ_COMMITTED) must exclude the aborted write"
        );
    }

    /// At-least-once restore reads the changelog at `READ_UNCOMMITTED`, so it sees
    /// BOTH records (the committed "a"→5 and the "aborted" "b"→99). This pins the
    /// non-EOS behaviour as unchanged.
    #[tokio::test]
    async fn alo_restore_reads_uncommitted_sees_both() {
        let mut task = restore_counter_task(ProcessingGuarantee::AtLeastOnce).await;
        check!(
            task.store_get_i64("counts", &"a".to_string()).await == Some(5),
            "ALO restore must seed the committed changelog record"
        );
        check!(
            task.store_get_i64("counts", &"b".to_string()).await == Some(99),
            "ALO restore (READ_UNCOMMITTED) must see the uncommitted write too"
        );
    }

    /// Build a minimal task seeded with the given source partitions (all starting
    /// at offset 0). Uses the stateless `Upper` topology so no stores are needed.
    async fn make_test_task(sources: Vec<TopicPartition>) -> StreamTask {
        let producer = std::sync::Arc::new(CollectProducer::default());
        let store = std::sync::Arc::new(MemStore::default());
        StreamTask::new(
            "0".into(),
            built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 0)
                .await
                .unwrap(),
            sources,
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            ProcessingGuarantee::AtLeastOnce,
        )
    }

    #[tokio::test]
    async fn position_reflects_seeded_source_partitions() {
        // A task seeded with one source partition starts at offset 0.
        let task = make_test_task(vec![TopicPartition {
            topic: "in".into(),
            partition: 2,
        }])
        .await;
        let pos = task.position();
        assert_eq!(pos.offset("in", 2), Some(0));
        assert_eq!(pos.offset("in", 9), None);
    }

    // ── record-cache flush on commit (sub-task 3e) ───────────────────────────

    use crate::dsl::processors::change::Change;

    /// A `Change<i64>` serde encoding just the `new` side (8 bytes BE) so the
    /// downstream sink has bytes to emit. Never deserialized in these tests.
    #[derive(Clone)]
    struct ChangeI64Serde;
    impl crate::processor::serde::Serde<Change<i64>> for ChangeI64Serde {
        fn serialize(&self, _topic: &str, v: &Change<i64>) -> bytes::Bytes {
            bytes::Bytes::copy_from_slice(&v.new.unwrap_or(0).to_be_bytes())
        }
        fn deserialize(
            &self,
            _topic: &str,
            _bytes: &[u8],
        ) -> Result<Change<i64>, crate::processor::serde::SerdeError> {
            unreachable!("Change<i64> sink is never deserialized in these tests")
        }
    }

    /// A materializing processor that writes the cached "counts" store on every
    /// record but forwards NOTHING on `process` — so the ONLY path a `Change`
    /// reaches the downstream sink is `Graph::flush_caches` (the deduped emit on
    /// commit). This mirrors a cached `KTable` aggregate whose emit-on-update is
    /// suppressed: the sink sees one deduped change per flush, not one per record.
    struct StoreWriterNoForward;
    #[async_trait::async_trait]
    impl Processor<String, String, String, Change<i64>> for StoreWriterNoForward {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, Change<i64>>,
            r: Record<String, String>,
        ) {
            let store = ctx.get_state_store::<String, i64>("counts").unwrap();
            let n = store.get(&r.value).await.unwrap_or(0) + 1;
            store.put(r.value.clone(), n).await;
            // Deliberately no ctx.forward — the change is emitted only at flush.
        }
    }

    /// `source "in" → StoreWriterNoForward(materializes cached "counts") → sink "out"`.
    /// The "counts" store is marked cache-eligible; with `cache_max_bytes > 0` the
    /// store buffers writes and defers both its changelog AND the downstream
    /// `Change` emit to `flush_caches`.
    fn cached_writer_built() -> crate::topology::BuiltTopology {
        use crate::processor::serde::{I64Serde, Produced};
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor("c", || StoreWriterNoForward, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink_explicit::<String, Change<i64>, _, _, _, _>(
            "out",
            "out",
            [&c],
            Produced::with(StringSerde, ChangeI64Serde),
        );
        t.mark_store_caching("counts", true);
        t.build("app").unwrap()
    }

    /// An [`OffsetStore`] + [`RecordProducer`] pair that share one ordered event
    /// log, so a test can assert the relative order of producer sends vs offset
    /// commits across the two trait objects. `send`/`send_with_timestamp` push a
    /// `produce:<topic>` event; `commit` pushes `commit-offsets`.
    #[derive(Clone, Default)]
    struct OrderLog(std::sync::Arc<StdMutex<Vec<String>>>);

    struct LoggingProducer {
        log: OrderLog,
    }
    #[async_trait::async_trait]
    impl RecordProducer for LoggingProducer {
        async fn send(
            &self,
            topic: &str,
            _partition: Option<i32>,
            _k: Option<bytes::Bytes>,
            _v: Option<bytes::Bytes>,
        ) -> Result<(), crate::StreamsClientError> {
            self.log.0.lock().unwrap().push(format!("produce:{topic}"));
            Ok(())
        }
        async fn flush(&self) -> Result<(), crate::StreamsClientError> {
            self.log.0.lock().unwrap().push("flush".to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct LoggingStore {
        log: OrderLog,
        committed: StdMutex<HashMap<(String, i32), i64>>,
    }
    #[async_trait::async_trait]
    impl OffsetStore for LoggingStore {
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
        async fn latest(&self, _t: &str, _p: i32) -> Result<i64, crate::StreamsClientError> {
            Ok(0)
        }
        async fn commit(
            &self,
            offs: &[(String, i32, i64)],
        ) -> Result<(), crate::StreamsClientError> {
            self.log
                .0
                .lock()
                .unwrap()
                .push("commit-offsets".to_string());
            let mut m = self.committed.lock().unwrap();
            for (t, p, o) in offs {
                m.insert((t.clone(), *p), *o);
            }
            Ok(())
        }
    }

    /// A cached materialized store must buffer its writes (no sink output / no
    /// changelog) until commit, then `commit()` must flush the cache — emitting
    /// exactly ONE deduped sink record + ONE changelog record — BEFORE committing
    /// source offsets (so under EOS the forwarded records join the committed txn).
    #[tokio::test]
    async fn commit_flushes_record_cache_before_offset_commit() {
        let log = OrderLog::default();
        let producer = std::sync::Arc::new(LoggingProducer { log: log.clone() });
        let store = std::sync::Arc::new(LoggingStore {
            log: log.clone(),
            committed: StdMutex::new(HashMap::new()),
        });

        // Two records for the SAME key on consecutive offsets.
        let fetcher = ScriptedFetcher::new(vec![
            (
                ("in".to_string(), 0, 0),
                FetchBatch {
                    records: vec![FetchedRec {
                        offset: 0,
                        key: None,
                        value: Some("k".into()),
                        timestamp: 1,
                    }],
                },
            ),
            (
                ("in".to_string(), 0, 1),
                FetchBatch {
                    records: vec![FetchedRec {
                        offset: 1,
                        key: None,
                        value: Some("k".into()),
                        timestamp: 2,
                    }],
                },
            ),
        ]);

        let mut task = StreamTask::new(
            "0".into(),
            cached_writer_built()
                .instantiate(&crate::store::backend::StoreBackend::InMemory, "app", 1024)
                .await
                .unwrap(),
            vec![TopicPartition {
                topic: "in".into(),
                partition: 0,
            }],
            std::sync::Arc::clone(&producer) as std::sync::Arc<dyn RecordProducer>,
            std::sync::Arc::clone(&store) as std::sync::Arc<dyn OffsetStore>,
            TaskRole::Active,
            ProcessingGuarantee::AtLeastOnce,
        );
        task.init().await.unwrap();

        // Pipe both records (two separate process_once polls, no commit yet).
        task.process_once(&fetcher, None).await.unwrap();
        task.process_once(&fetcher, None).await.unwrap();

        // BEFORE commit: cached writes are buffered — nothing produced yet.
        check!(
            log.0.lock().unwrap().is_empty(),
            "cached store must defer all sink + changelog output until flush, got {:?}",
            log.0.lock().unwrap()
        );

        task.commit().await.unwrap();

        let events = log.0.lock().unwrap().clone();
        let emitted: Vec<&String> = events
            .iter()
            .filter(|e| e.starts_with("produce:"))
            .collect();
        // Exactly one deduped sink emit + one changelog emit (two puts → one entry).
        check!(
            emitted
                .iter()
                .filter(|e| e.as_str() == "produce:out")
                .count()
                == 1,
            "exactly one deduped sink record expected, got {events:?}"
        );
        check!(
            emitted
                .iter()
                .filter(|e| e.as_str() == "produce:app-counts-changelog")
                .count()
                == 1,
            "exactly one deduped changelog record expected, got {events:?}"
        );

        // Ordering: every produce (flushed cache output) precedes the offset commit.
        let commit_idx = events
            .iter()
            .position(|e| e == "commit-offsets")
            .expect("offsets must be committed");
        let last_produce_idx = events
            .iter()
            .rposition(|e| e.starts_with("produce:"))
            .expect("cache flush must produce at least one record");
        check!(
            last_produce_idx < commit_idx,
            "cache-flushed sink + changelog output must be produced BEFORE the offset commit, got {events:?}"
        );
    }
}
