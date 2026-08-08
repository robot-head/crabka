//! The instantiated, runnable processor graph for one subtopology and partition.
//!
//! The driver loop is not recursive. A node's `forward` appends
//! `(child_idx, ErasedRecord)` to a buffer, and the driver drains that buffer, so
//! there is no `&mut` aliasing across nodes.

use std::collections::VecDeque;

use crabka_units::prelude::*;

use super::{
    erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError},
    node::ErasedNode,
    record::RecordContext,
};
use crate::store::registry::StoreRegistry;

/// Closure type that [`GraphSource`] uses to deserialize raw bytes into an
/// [`ErasedRecord`]. The alias keeps the `GraphSource` field legible.
type DeserializeFn =
    Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<ErasedRecord, ProcessorError> + Send>;

/// A source: the topic it reads, a closure that deserializes `(key,value,ts)`
/// into an erased record, and the node indices it feeds.
pub(crate) struct GraphSource {
    pub topic: String,
    pub deserialize: DeserializeFn,
    pub children: Vec<usize>,
}

/// One subtopology's runnable graph at a single partition.
pub(crate) struct Graph {
    pub nodes: Vec<Box<dyn ErasedNode>>,
    pub children: Vec<Vec<usize>>,
    pub sources: Vec<GraphSource>,
    pub output: Vec<OutputRecord>,
    pub stores: StoreRegistry,
    /// The app-wide, fully-replicated global stores, shared across tasks and
    /// lent into each dispatch. This field is empty by default, until the app
    /// runtime or `TopologyTestDriver` fills it. Stream-globaltable joins read
    /// it.
    pub globals: crate::runtime::global::GlobalStateManager,
    /// Live punctuation schedules that `ProcessorContext::schedule` registers,
    /// tagged by node index. Each dispatch borrows this field and writes to it.
    /// `punctuate_stream_time` fires the schedules.
    pub schedules: Vec<crate::processor::punctuation::ScheduleEntry>,
    /// Observed max record timestamp, the stream-time. A stream-time schedule
    /// stamps its first fire from this base. It starts at `i64::MIN`.
    pub stream_time: i64,
    /// The last wall-clock value seen. A wall-clock schedule stamps its first
    /// fire from this base. It starts at `0`, and
    /// [`Graph::punctuate_wall_clock`] reads it.
    pub wall_clock: i64,
    /// Total record-cache budget for this graph's stores, the JVM
    /// `statestore.cache.max.bytes`. A zero budget disables caching and gives
    /// emit-on-update, which matches the JVM `TopologyTestDriver` default. The
    /// value threads from [`StreamsApp`](crate::StreamsApp) to
    /// [`KafkaStreams`](crate::KafkaStreams) to `instantiate`.
    /// [`Graph::cache_max_bytes`] reads it.
    // Config-only for now; the record cache that consumes this budget lands in a
    // later task, so the field/accessor are read only by tests until then.
    #[allow(dead_code)]
    pub cache_max_bytes: ByteSize,
    /// Store name to owning node index. `flush_caches` uses it to root each
    /// cached store's forwarded changes at the node that materializes the store.
    /// This map is empty in production until build-time population lands
    /// (sub-task 3b-ii). Tests fill it manually for now.
    // Build-time population + the production flush_caches call site land in later
    // record-caching sub-tasks; read only by tests until then.
    #[allow(dead_code)]
    pub(crate) cache_owner: std::collections::HashMap<String, usize>,
    /// The per-task record cache that owns every cached store's [`NamedCache`].
    /// `instantiate` builds it with the graph's `cache_max_bytes` budget and
    /// registers one named cache per materialized KV store. Eviction and flush
    /// both route through it. It is empty and has a zero budget when caching is
    /// disabled.
    // Eviction wiring (over-budget forwarding) lands in a later record-caching
    // sub-task; held here so the per-store NamedCaches share one budget.
    #[allow(dead_code)]
    pub(crate) cache: crate::store::cache::thread::ThreadCache,
}

impl Graph {
    /// The record-cache budget threaded into this graph, the JVM
    /// `statestore.cache.max.bytes`. A zero budget disables caching.
    #[allow(dead_code)] // consumed by the record cache in a later task; tests assert it now
    pub(crate) fn cache_max_bytes(&self) -> ByteSize {
        self.cache_max_bytes
    }

    /// Feed one record that arrives on `topic`.
    ///
    /// This method runs the graph to completion and appends the sink outputs to
    /// `self.output`. It ignores an unknown topic.
    pub async fn pipe(
        &mut self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        timestamp: i64,
    ) -> Result<(), ProcessorError> {
        self.stream_time = self.stream_time.max(timestamp);
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        let rc = RecordContext {
            topic: topic.to_string(),
            partition: 0,
            offset: 0,
            timestamp,
        };

        // Seed: for each source on this topic, push one erased record per child
        // (re-deserialize per child — `Box<dyn Any>` is not cloneable).
        for src in &self.sources {
            if src.topic == topic {
                for &child in &src.children {
                    let rec = (src.deserialize)(key, value, timestamp)?;
                    buffer.push_back((child, rec));
                }
            }
        }

        self.drain(buffer, &rc).await
    }

    /// Drive the buffer of `(node_idx, ErasedRecord)` to completion and append
    /// the sink outputs to `self.output`.
    ///
    /// `pipe` shares this method for record processing, and `fire_schedule`
    /// shares it for punctuator-forwarded records.
    async fn drain(
        &mut self,
        mut buffer: VecDeque<(usize, ErasedRecord)>,
        rc: &RecordContext,
    ) -> Result<(), ProcessorError> {
        // `mem::take` the child list so we can borrow `self.nodes` and
        // `self.output` as disjoint fields while the node processes.
        while let Some((idx, rec)) = buffer.pop_front() {
            // Take this node's child list out temporarily to satisfy the borrow
            // checker: `self.children[idx]` and `self.nodes[idx]` are disjoint,
            // but rustc can't see through the index.
            let children = std::mem::take(&mut self.children[idx]);
            let res = {
                // Bind disjoint fields as separate locals so rustc can see
                // they don't alias: nodes[idx], output, and stores are three
                // distinct fields of `self`.
                // Copy the i64 clocks into locals FIRST so reading them doesn't
                // conflict with the `&mut self.schedules` borrow below.
                let (st, wc) = (self.stream_time, self.wall_clock);
                let node = &mut self.nodes[idx];
                let out = &mut self.output;
                let stores = &mut self.stores;
                let scheds = &mut self.schedules;
                let mut d = Dispatch {
                    buffer: &mut buffer,
                    children: &children,
                    output: out,
                    record_ctx: rc,
                    stores,
                    globals: &self.globals,
                    node_idx: idx,
                    schedules: scheds,
                    sched_stream_time: st,
                    sched_wall_clock: wc,
                };
                node.process(&mut d, rec).await
            };
            self.children[idx] = children;
            res?;
        }
        Ok(())
    }

    /// Fire one schedule's punctuator at its node, then drain any records that
    /// the punctuator forwarded. `ts` is the timestamp passed to `punctuate`.
    async fn fire_schedule(&mut self, sched_idx: usize, ts: i64) -> Result<(), ProcessorError> {
        let node_idx = self.schedules[sched_idx].node_idx;
        let rc = RecordContext {
            topic: String::new(),
            partition: -1,
            offset: -1,
            timestamp: ts,
        };
        let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
        // Take the punctuator out so the Dispatch can borrow `self.schedules`
        // (for re-scheduling) without aliasing the entry being fired.
        let mut punct = std::mem::replace(
            &mut self.schedules[sched_idx].punctuator,
            Box::new(NoopPunctuator),
        );
        let children = std::mem::take(&mut self.children[node_idx]);
        {
            let (st, wc) = (self.stream_time, self.wall_clock);
            let out = &mut self.output;
            let stores = &mut self.stores;
            let scheds = &mut self.schedules;
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &children,
                output: out,
                record_ctx: &rc,
                stores,
                globals: &self.globals,
                node_idx,
                schedules: scheds,
                sched_stream_time: st,
                sched_wall_clock: wc,
            };
            punct.fire(&mut d, ts).await;
        }
        self.children[node_idx] = children;
        self.schedules[sched_idx].punctuator = punct;
        self.drain(buffer, &rc).await
    }

    /// Flush every cached store and forward its deduped changes downstream.
    ///
    /// Each set of changes is rooted at the store's owning node, which matches
    /// `fire_schedule`. The task commit and close path calls this method before
    /// the producer commit. It flushes in ascending owning-node order and drains
    /// after each store, so a downstream cached store sees an upstream store's
    /// forwarded changes before the downstream store flushes. This is the
    /// chained-KTable order.
    pub(crate) async fn flush_caches(&mut self) -> Result<(), ProcessorError> {
        let rc = RecordContext {
            topic: String::new(),
            partition: -1,
            offset: -1,
            timestamp: self.stream_time,
        };
        // Collect (node_idx, store_name) and flush in ascending node order so a
        // chained downstream cached store sees its upstream's forwarded changes
        // before it flushes.
        let mut owners: Vec<(usize, String)> = self
            .cache_owner
            .iter()
            .map(|(n, &i)| (i, n.clone()))
            .collect();
        owners.sort_by_key(|(i, _)| *i);
        for (node_idx, name) in owners {
            let mut buffer: VecDeque<(usize, ErasedRecord)> = VecDeque::new();
            // Take the owning node's children out so the store flush can borrow
            // `self.stores` mutably without aliasing `self.children` (mirrors the
            // `mem::take` in `fire_schedule`).
            let children = std::mem::take(&mut self.children[node_idx]);
            if let Some(store) = self.stores.get_mut(&name) {
                store.flush_cache_into(&mut buffer, &children).await;
            }
            self.children[node_idx] = children;
            // `drain` re-borrows `self`; the store borrow above is finished here.
            self.drain(buffer, &rc).await?;
        }
        Ok(())
    }

    /// Fire all due `STREAM_TIME` schedules at the current stream-time, each one
    /// at most once. This method first raises stream-time to
    /// `max(.., stream_time)`.
    pub async fn punctuate_stream_time(&mut self, stream_time: i64) -> Result<(), ProcessorError> {
        self.stream_time = self.stream_time.max(stream_time);
        let now = self.stream_time;
        self.punctuate(
            crate::processor::punctuation::PunctuationType::StreamTime,
            now,
        )
        .await
    }

    /// Fire all due `WALL_CLOCK_TIME` schedules at `now_ms`, each one at most
    /// once. This method sets `self.wall_clock = now_ms` first, so a `schedule()`
    /// call from a punctuator, or from a later `process`, stamps its base from
    /// the current clock.
    pub async fn punctuate_wall_clock(&mut self, now_ms: i64) -> Result<(), ProcessorError> {
        self.wall_clock = now_ms;
        self.punctuate(
            crate::processor::punctuation::PunctuationType::WallClockTime,
            now_ms,
        )
        .await
    }

    /// Shared firing pass over every due schedule of type `ty`.
    ///
    /// The pass fires each due schedule at `now`, AT MOST ONCE each, with no
    /// catch-up loop. It then resyncs each fired entry's `next_time`. The fired
    /// value is `now`, the current clock, which matches the JVM capture for both
    /// stream-time and wall-clock punctuation.
    async fn punctuate(
        &mut self,
        ty: crate::processor::punctuation::PunctuationType,
        now: i64,
    ) -> Result<(), ProcessorError> {
        self.schedules.retain(|e| !e.is_cancelled());
        let n = self.schedules.len();
        for i in 0..n {
            if self.schedules[i].ty != ty || self.schedules[i].is_cancelled() {
                continue;
            }
            let next = self.schedules[i].next_time;
            if now >= next {
                // The schedule timeline is in epoch milliseconds, so the interval
                // extent crosses into that coordinate space here.
                let interval = self.schedules[i].interval.millis_i64();
                self.fire_schedule(i, now).await?; // value = now, fire AT MOST ONCE
                // Resync: if we fell more than one interval behind, jump to
                // `now + interval`; else advance by one interval. Saturating to
                // stay overflow-safe when `next` is near `i64::MIN` (a stream
                // schedule's first `next_time` is `MIN + interval`). The
                // comparison `now - next >= interval` is rewritten as the
                // overflow-safe `now >= next + interval`.
                self.schedules[i].next_time = if now >= next.saturating_add(interval) {
                    now.saturating_add(interval)
                } else {
                    next.saturating_add(interval)
                };
            }
        }
        Ok(())
    }

    pub fn take_output(&mut self) -> Vec<OutputRecord> {
        std::mem::take(&mut self.output)
    }

    /// Call `init` on every node in index order. A node that does not override
    /// `ErasedNode::init`, such as a sink or a source, gets the default no-op.
    #[tracing::instrument(
        name = "streams.graph.init_processors",
        level = "info",
        skip_all,
        fields(nodes = self.nodes.len()),
        err,
    )]
    pub async fn init_processors(&mut self) -> Result<(), ProcessorError> {
        let n = self.nodes.len();
        for idx in 0..n {
            let mut buffer = VecDeque::new();
            let mut output = Vec::new();
            let rc = RecordContext {
                topic: String::new(),
                partition: -1,
                offset: -1,
                timestamp: -1,
            };
            // Copy the i64 clocks into locals FIRST so reading them doesn't
            // conflict with the `&mut self.schedules` borrow below.
            let (st, wc) = (self.stream_time, self.wall_clock);
            let node = &mut self.nodes[idx];
            let stores = &mut self.stores;
            let globals = &self.globals;
            let scheds = &mut self.schedules;
            let mut d = Dispatch {
                buffer: &mut buffer,
                children: &[],
                output: &mut output,
                record_ctx: &rc,
                stores,
                globals,
                node_idx: idx,
                schedules: scheds,
                sched_stream_time: st,
                sched_wall_clock: wc,
            };
            node.init(&mut d).await?;
        }
        Ok(())
    }

    /// Call `close` on every node (in index order).
    #[tracing::instrument(
        name = "streams.graph.close_processors",
        level = "info",
        skip_all,
        fields(nodes = self.nodes.len()),
    )]
    pub async fn close_processors(&mut self) {
        for node in &mut self.nodes {
            node.close().await;
        }
    }

    /// Drain every store's changelog buffer into `(changelog_topic, key, value)`.
    ///
    /// A store whose changelog topic is one of `reuse_source_topics` is a
    /// **reuse-source** store. The `REUSE_KTABLE_SOURCE_TOPICS` optimizer points a
    /// `builder.table_explicit(topic, …)` store's changelog at its own source
    /// `topic` instead of at a derived `<app>-<store>-changelog`. This method
    /// still drains that buffer, so the buffer cannot grow unbounded, but it does
    /// **not** re-produce the entries. The source topic already IS the changelog,
    /// and a re-produce onto it would feed the source node an endless re-emit
    /// loop. This matches the JVM, which marks such source-table stores
    /// `loggingDisabled`.
    pub fn drain_changelogs(
        &mut self,
        reuse_source_topics: &std::collections::HashSet<String>,
    ) -> Vec<(String, bytes::Bytes, Option<bytes::Bytes>, Option<i64>)> {
        let mut out = Vec::new();
        // Iterate stores directly (no per-record `names()` Vec + re-lookup) and
        // only materialise the changelog topic String when a store actually has
        // entries to drain — most records touch a single store.
        for store in self.stores.iter_mut() {
            let entries = store.take_changelog_ts();
            if entries.is_empty() {
                continue;
            }
            let topic = store.changelog_topic();
            if reuse_source_topics.contains(topic) {
                continue; // reuse-source store: drained, but never re-produced
            }
            for (k, v, ts) in entries {
                out.push((topic.to_string(), k, v, ts));
            }
        }
        out
    }

    /// Restore one changelog record into a named store. The caller is
    /// responsible for the logging-off path.
    pub async fn restore_apply(
        &mut self,
        store_name: &str,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        timestamp: i64,
    ) {
        if let Some(store) = self.stores.get_mut(store_name) {
            store.apply_changelog_ts(key, value, timestamp).await;
        }
    }

    /// Toggle changelog logging on every store. It is off during restore and on
    /// during processing.
    pub fn set_logging(&mut self, on: bool) {
        for name in self.stores.names() {
            if let Some(s) = self.stores.get_mut(&name) {
                s.set_logging(on);
            }
        }
    }

    /// Wipe every state store (for EOS rollback before re-restore).
    #[tracing::instrument(name = "streams.graph.clear_stores", level = "debug", skip_all)]
    pub async fn clear_stores(&mut self) {
        for name in self.stores.names() {
            if let Some(s) = self.stores.get_mut(&name) {
                s.clear().await;
            }
        }
    }
}

/// Placeholder swapped into a `ScheduleEntry` while the real punctuator is out
/// to fire. The swap lets the firing `Dispatch` borrow `self.schedules` and not
/// alias the entry. `fire` is never called on this placeholder.
struct NoopPunctuator;
#[async_trait::async_trait]
impl crate::processor::punctuation::ErasedPunctuator for NoopPunctuator {
    async fn fire(&mut self, _d: &mut Dispatch<'_>, _ts: i64) {}
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use async_trait::async_trait;

    use super::*;
    use crate::processor::{
        api::{Processor, ProcessorContext},
        node::{ErasedNode, ProcessorNode, SinkNode, SourceNode},
        record::Record,
        serde::StringSerde,
    };

    struct Upper;
    #[async_trait]
    impl Processor<String, String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[tokio::test]
    async fn drives_source_processor_sink() {
        // nodes: index 0 = processor "up", index 1 = sink "out"
        let up = Box::new(ProcessorNode::new("up".into(), &(|| Upper))) as Box<dyn ErasedNode>;
        let sink = Box::new(SinkNode::new(
            "out".into(),
            "out-topic".into(),
            StringSerde,
            StringSerde,
        )) as Box<dyn ErasedNode>;
        let src = SourceNode::new("src".into(), StringSerde, StringSerde);
        let source = GraphSource {
            topic: "in".into(),
            deserialize: Box::new(move |k, v, ts| src.deserialize(k, v, ts)),
            children: vec![0], // source feeds node 0 (up)
        };
        let mut graph = Graph {
            nodes: vec![up, sink],
            children: vec![vec![1], vec![]], // up -> sink ; sink -> none
            sources: vec![source],
            output: Vec::new(),
            stores: crate::store::registry::StoreRegistry::default(),
            globals: crate::runtime::global::GlobalStateManager::default(),
            schedules: Vec::new(),
            stream_time: i64::MIN,
            wall_clock: 0,
            cache_max_bytes: ByteSize::ZERO,
            cache_owner: std::collections::HashMap::new(),
            cache: crate::store::cache::thread::ThreadCache::new(ByteSize::ZERO),
        };
        graph.pipe("in", Some(b"k"), b"hi", 7).await.unwrap();
        let out = graph.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out-topic");
        check!(out[0].value.as_ref().unwrap().as_ref() == b"HI");
    }

    #[tokio::test]
    async fn unknown_topic_is_ignored() {
        let mut graph = Graph {
            nodes: vec![],
            children: vec![],
            sources: vec![],
            output: Vec::new(),
            stores: crate::store::registry::StoreRegistry::default(),
            globals: crate::runtime::global::GlobalStateManager::default(),
            schedules: Vec::new(),
            stream_time: i64::MIN,
            wall_clock: 0,
            cache_max_bytes: ByteSize::ZERO,
            cache_owner: std::collections::HashMap::new(),
            cache: crate::store::cache::thread::ThreadCache::new(ByteSize::ZERO),
        };
        graph.pipe("nope", None, b"x", 0).await.unwrap();
        check!(graph.take_output().is_empty());
    }

    #[tokio::test]
    async fn stateful_processor_accumulates_via_store() {
        use crate::{
            processor::{
                api::{Processor, ProcessorContext},
                node::{ProcessorNode, SinkNode, SourceNode},
                record::Record,
                serde::{I64Serde, StringSerde},
            },
            store::{kv::KeyValueBytesStore, registry::StoreRegistry},
        };

        struct Counter;
        #[async_trait]
        impl Processor<String, String, String, i64> for Counter {
            async fn process(
                &mut self,
                ctx: &mut ProcessorContext<'_, '_, String, i64>,
                r: Record<String, String>,
            ) {
                let n = {
                    let store = ctx
                        .get_state_store::<String, i64>("counts")
                        .expect("counts store not found");
                    let n = store.get(&r.value).await.unwrap_or(0) + 1;
                    store.put(r.value.clone(), n).await;
                    n
                };
                ctx.forward(Record::new(Some(r.value), n, r.timestamp));
            }
        }

        let mut stores = StoreRegistry::default();
        stores.insert(Box::new(KeyValueBytesStore::<String, i64>::in_memory(
            "counts".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "counts-changelog".into(),
        )));

        // nodes: index 0 = counter processor, index 1 = sink
        let proc_node = Box::new(ProcessorNode::new(
            "counter".into(),
            &(|| Box::new(Counter) as Box<dyn Processor<String, String, String, i64>>),
        )) as Box<dyn ErasedNode>;
        let sink_node = Box::new(SinkNode::new(
            "out".into(),
            "out-topic".into(),
            StringSerde,
            I64Serde,
        )) as Box<dyn ErasedNode>;
        let src_node = SourceNode::new("src".into(), StringSerde, StringSerde);
        let source = GraphSource {
            topic: "in".into(),
            deserialize: Box::new(move |k, v, ts| src_node.deserialize(k, v, ts)),
            children: vec![0],
        };

        let mut graph = Graph {
            nodes: vec![proc_node, sink_node],
            children: vec![vec![1], vec![]],
            sources: vec![source],
            output: Vec::new(),
            stores,
            globals: crate::runtime::global::GlobalStateManager::default(),
            schedules: Vec::new(),
            stream_time: i64::MIN,
            wall_clock: 0,
            cache_max_bytes: ByteSize::ZERO,
            cache_owner: std::collections::HashMap::new(),
            cache: crate::store::cache::thread::ThreadCache::new(ByteSize::ZERO),
        };

        // pipe "in"/"a" twice — counter should accumulate to 2
        graph.pipe("in", Some(b"k"), b"a", 1).await.unwrap();
        graph.pipe("in", Some(b"k"), b"a", 2).await.unwrap();

        let out = graph.take_output();
        check!(out.len() == 2);
        // last output value bytes should be big-endian i64(2) = [0,0,0,0,0,0,0,2]
        check!(out[1].value.as_ref().unwrap().as_ref() == [0u8, 0, 0, 0, 0, 0, 0, 2]);
    }

    #[tokio::test]
    async fn stream_time_punctuator_fires_once_at_current_stream_time() {
        use std::time::Duration;

        use crate::processor::{
            punctuation::{PunctuationType, Punctuator},
            serde::I64Serde,
        };

        // A punctuator that, when fired at `ts`, forwards a record whose value is
        // that fired timestamp (`Record::new(None, ts, ts)`), so the sink emits
        // the i64 value we can assert on.
        struct EmitTs;
        #[async_trait]
        impl Punctuator<String, i64> for EmitTs {
            async fn punctuate(
                &mut self,
                ctx: &mut ProcessorContext<'_, '_, String, i64>,
                ts: i64,
            ) {
                ctx.forward(Record::new(None, ts, ts));
            }
        }

        // A processor that schedules a STREAM_TIME punctuator (interval 10ms) in
        // `init`, and is otherwise a no-op on records.
        struct Scheduler;
        #[async_trait]
        impl Processor<String, String, String, i64> for Scheduler {
            async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
                ctx.schedule(
                    Duration::from_millis(10),
                    PunctuationType::StreamTime,
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

        // nodes: index 0 = scheduler processor, index 1 = sink
        let proc_node =
            Box::new(ProcessorNode::new("proc".into(), &(|| Scheduler))) as Box<dyn ErasedNode>;
        let sink_node = Box::new(SinkNode::new(
            "out".into(),
            "out-topic".into(),
            StringSerde,
            I64Serde,
        )) as Box<dyn ErasedNode>;
        let src_node = SourceNode::new("src".into(), StringSerde, StringSerde);
        let source = GraphSource {
            topic: "in".into(),
            deserialize: Box::new(move |k, v, ts| src_node.deserialize(k, v, ts)),
            children: vec![0],
        };

        let mut graph = Graph {
            nodes: vec![proc_node, sink_node],
            children: vec![vec![1], vec![]], // proc -> sink ; sink -> none
            sources: vec![source],
            output: Vec::new(),
            stores: crate::store::registry::StoreRegistry::default(),
            globals: crate::runtime::global::GlobalStateManager::default(),
            schedules: Vec::new(),
            stream_time: i64::MIN,
            wall_clock: 0,
            cache_max_bytes: ByteSize::ZERO,
            cache_owner: std::collections::HashMap::new(),
            cache: crate::store::cache::thread::ThreadCache::new(ByteSize::ZERO),
        };

        // init schedules the punctuator: stream base i64::MIN -> next = MIN + 10.
        graph.init_processors().await.unwrap();
        // a record at ts=5 (no forward in `process`).
        graph.pipe("in", Some(b"k"), b"v", 5).await.unwrap();
        // punctuate at stream-time 25: now=25 >= next=MIN+10 -> fire ONCE with
        // value=now=25; next resyncs to 35 (now - next >= interval).
        graph.punctuate_stream_time(25).await.unwrap();

        let out = graph.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out-topic");
        // value = i64(25) big-endian
        check!(out[0].value.as_ref().unwrap().as_ref() == 25i64.to_be_bytes());
    }

    #[tokio::test]
    async fn flush_caches_roots_and_forwards_deduped_change() {
        use std::sync::{Arc, Mutex};

        use crate::{
            dsl::processors::change::Change,
            processor::{
                record::RecordContext,
                serde::{I64Serde, StringSerde},
            },
            store::{cache::named::NamedCache, kv::KeyValueBytesStore, registry::StoreRegistry},
        };

        // A recording child: stashes every `Change<i64>` it receives so the test
        // can assert flush_caches forwarded the deduped change here.
        type Recorded = Arc<Mutex<Vec<(Option<String>, Change<i64>, i64)>>>;
        struct Recorder(Recorded);
        #[async_trait]
        impl Processor<String, Change<i64>, String, i64> for Recorder {
            async fn process(
                &mut self,
                _ctx: &mut ProcessorContext<'_, '_, String, i64>,
                r: Record<String, Change<i64>>,
            ) {
                self.0.lock().unwrap().push((r.key, r.value, r.timestamp));
            }
        }

        // The owning (materializing) node — a no-op; flush_caches roots the
        // forwarded change at it and pushes into its children.
        struct Owner;
        #[async_trait]
        impl Processor<String, i64, String, i64> for Owner {
            async fn process(
                &mut self,
                _ctx: &mut ProcessorContext<'_, '_, String, i64>,
                _r: Record<String, i64>,
            ) {
            }
        }

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let rec_for_node = recorded.clone();

        // nodes: index 0 = owner (materializes "store"), index 1 = recorder.
        let owner =
            Box::new(ProcessorNode::new("owner".into(), &(|| Owner))) as Box<dyn ErasedNode>;
        let rec_node = Box::new(ProcessorNode::new(
            "recorder".into(),
            &(move || Recorder(rec_for_node.clone())),
        )) as Box<dyn ErasedNode>;

        // Register a cached String->i64 store named "store".
        let mut stores = StoreRegistry::default();
        let mut kv = KeyValueBytesStore::<String, i64>::in_memory(
            "store".into(),
            Box::new(StringSerde),
            Box::new(I64Serde),
            "store-changelog".into(),
        );
        kv.enable_cache(Arc::new(Mutex::new(NamedCache::new("store".into()))));
        stores.insert(Box::new(kv));

        let mut graph = Graph {
            nodes: vec![owner, rec_node],
            children: vec![vec![1], vec![]], // owner -> recorder ; recorder -> none
            sources: vec![],
            output: Vec::new(),
            stores,
            globals: crate::runtime::global::GlobalStateManager::default(),
            schedules: Vec::new(),
            stream_time: i64::MIN,
            wall_clock: 0,
            cache_max_bytes: kibibytes(1),
            cache_owner: std::collections::HashMap::new(),
            cache: crate::store::cache::thread::ThreadCache::new(ByteSize::ZERO),
        };
        // node 0 owns "store".
        graph.cache_owner.insert("store".into(), 0);

        // Stage dirty entries directly in the store: two writes for the same key
        // under context ts=7 (deduped to new=3), plus a distinct key "b"=9.
        {
            let store = graph
                .stores
                .get_kv::<String, i64>("store")
                .expect("store not found / wrong type");
            store.set_record_context(RecordContext {
                topic: "t".into(),
                partition: 0,
                offset: 0,
                timestamp: 7,
            });
            store.put("a".into(), 1).await;
            store.put("a".into(), 3).await; // dedupes to new=3 (old=None: never committed)
            store.put("b".into(), 9).await;
        }

        graph.flush_caches().await.unwrap();

        let got = recorded.lock().unwrap();
        // One emit per dirty key (a, b), each a deduped Change forwarded to the child.
        check!(got.len() == 2);
        let by_key = |k: &str| got.iter().find(|(key, _, _)| key.as_deref() == Some(k));
        let a = by_key("a").expect("change for key a");
        check!(a.1.old == None); // never committed before this flush
        check!(a.1.new == Some(3)); // deduped latest
        check!(a.2 == 7); // forwarded with the dirty entry's context timestamp
        let b = by_key("b").expect("change for key b");
        check!(b.1.new == Some(9));
    }
}
