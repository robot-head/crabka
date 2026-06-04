//! The instantiated, runnable processor graph for one subtopology + partition.
//! Non-recursive driver loop: a node's `forward` appends `(child_idx,
//! ErasedRecord)` to a buffer the driver drains, so there is no `&mut` aliasing
//! across nodes.

use std::collections::VecDeque;

use super::erased::{Dispatch, ErasedRecord, OutputRecord, ProcessorError};
use super::node::ErasedNode;
use super::record::RecordContext;

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
}

impl Graph {
    /// Feed one record arriving on `topic`; runs the graph to completion,
    /// appending sink outputs to `self.output`. Unknown topics are ignored.
    pub fn pipe(
        &mut self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        timestamp: i64,
    ) -> Result<(), ProcessorError> {
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
                // Borrow `nodes` and `output` as two independent `&mut` fields.
                let node = &mut self.nodes[idx];
                let out = &mut self.output;
                let mut d = Dispatch {
                    buffer: &mut buffer,
                    children: &children,
                    output: out,
                    record_ctx: &rc,
                };
                node.process(&mut d, rec)
            };
            self.children[idx] = children;
            res?;
        }
        Ok(())
    }

    pub fn take_output(&mut self) -> Vec<OutputRecord> {
        std::mem::take(&mut self.output)
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

    #[test]
    fn drives_source_processor_sink() {
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
        };
        graph.pipe("in", Some(b"k"), b"hi", 7).unwrap();
        let out = graph.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out-topic");
        check!(out[0].value.as_ref().unwrap().as_ref() == b"HI");
    }

    #[test]
    fn unknown_topic_is_ignored() {
        let mut graph = Graph {
            nodes: vec![],
            children: vec![],
            sources: vec![],
            output: Vec::new(),
        };
        graph.pipe("nope", None, b"x", 0).unwrap();
        check!(graph.take_output().is_empty());
    }
}
