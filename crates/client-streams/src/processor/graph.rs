//! The instantiated, runnable processor graph for one subtopology + partition.
//! Non-recursive driver loop: a node's `forward` appends `(child_idx,
//! ErasedRecord)` to a buffer the driver drains, so there is no `&mut` aliasing
//! across nodes.

use std::collections::VecDeque;

use super::erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError};
use super::node::ErasedNode;
use super::record::RecordContext;
use crate::store::registry::StoreRegistry;

/// Closure type used by [`GraphSource`] to deserialize raw bytes into an
/// [`ErasedRecord`]. Aliased here to keep the `GraphSource` field legible.
type DeserializeFn =
    Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<ErasedRecord, ProcessorError> + Send>;

/// A source: which topic it reads, a closure that deserializes `(key,value,ts)`
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
    /// The app-wide, fully-replicated global stores (shared across tasks),
    /// lent into each dispatch. Default-empty until the app wiring (T8b) or the
    /// `TopologyTestDriver` populates it; stream-globaltable joins read it.
    pub globals: crate::runtime::global::GlobalStateManager,
    /// Live punctuation schedules registered via `ProcessorContext::schedule`,
    /// tagged by node index. Written here (lent into each dispatch); fired by the
    /// driver in T4.
    // read by ProcessorContext::schedule + Graph firing in T4
    #[allow(dead_code)]
    pub schedules: Vec<crate::processor::punctuation::ScheduleEntry>,
    /// Observed max record timestamp (stream-time); the base a stream-time
    /// schedule stamps its first fire from. Init `i64::MIN`.
    // read by ProcessorContext::schedule + Graph firing in T4
    #[allow(dead_code)]
    pub stream_time: i64,
    /// Last wall-clock value seen; the base a wall-clock schedule stamps its
    /// first fire from. Init `0`.
    // read by ProcessorContext::schedule + Graph firing in T4
    #[allow(dead_code)]
    pub wall_clock: i64,
}

impl Graph {
    /// Feed one record arriving on `topic`; runs the graph to completion,
    /// appending sink outputs to `self.output`. Unknown topics are ignored.
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

        // Drain. `mem::take` the child list so we can borrow `self.nodes` and
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
                    record_ctx: &rc,
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

    pub fn take_output(&mut self) -> Vec<OutputRecord> {
        std::mem::take(&mut self.output)
    }

    /// Call `init` on every node in index order. Nodes that don't override
    /// `ErasedNode::init` (sink, source) get the default no-op.
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
    pub async fn close_processors(&mut self) {
        for node in &mut self.nodes {
            node.close().await;
        }
    }

    /// Drain every store's changelog buffer → `(changelog_topic, key, value)`.
    pub fn drain_changelogs(&mut self) -> Vec<(String, bytes::Bytes, Option<bytes::Bytes>)> {
        let mut out = Vec::new();
        for name in self.stores.names() {
            if let Some(store) = self.stores.get_mut(&name) {
                let topic = store.changelog_topic().to_string();
                for (k, v) in store.take_changelog() {
                    out.push((topic.clone(), k, v));
                }
            }
        }
        out
    }

    /// Restore one changelog record into a named store (logging-off path is
    /// the caller's responsibility).
    pub async fn restore_apply(
        &mut self,
        store_name: &str,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
    ) {
        if let Some(store) = self.stores.get_mut(store_name) {
            store.apply_changelog(key, value).await;
        }
    }

    /// Toggle changelog logging on every store (off during restore, on during
    /// processing).
    pub fn set_logging(&mut self, on: bool) {
        for name in self.stores.names() {
            if let Some(s) = self.stores.get_mut(&name) {
                s.set_logging(on);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::node::{ErasedNode, ProcessorNode, SinkNode, SourceNode};
    use crate::processor::record::Record;
    use crate::processor::serde::StringSerde;
    use assert2::check;
    use async_trait::async_trait;

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
        };
        graph.pipe("nope", None, b"x", 0).await.unwrap();
        check!(graph.take_output().is_empty());
    }

    #[tokio::test]
    async fn stateful_processor_accumulates_via_store() {
        use crate::processor::api::{Processor, ProcessorContext};
        use crate::processor::node::{ProcessorNode, SinkNode, SourceNode};
        use crate::processor::record::Record;
        use crate::processor::serde::{I64Serde, StringSerde};
        use crate::store::kv::KeyValueBytesStore;
        use crate::store::registry::StoreRegistry;

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
        };

        // pipe "in"/"a" twice — counter should accumulate to 2
        graph.pipe("in", Some(b"k"), b"a", 1).await.unwrap();
        graph.pipe("in", Some(b"k"), b"a", 2).await.unwrap();

        let out = graph.take_output();
        check!(out.len() == 2);
        // last output value bytes should be big-endian i64(2) = [0,0,0,0,0,0,0,2]
        check!(out[1].value.as_ref().unwrap().as_ref() == [0u8, 0, 0, 0, 0, 0, 0, 2]);
    }
}
