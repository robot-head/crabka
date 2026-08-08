//! `ColumnarTopology`: a linear or branching graph whose edges carry
//! `DataFrame`s.
//!
//! A source binds a topic list and a `BatchCodec`. An operator is a `BuiltinOp`
//! or a custom `ColumnarProcessor`. A sink binds an output topic and a
//! `BatchCodec`. v1 supports linear chains and fan-out from any node. It has no
//! batch joins.

use std::sync::Arc;

use super::{
    codec::BatchCodec,
    operator::{BuiltinOp, ColumnarProcessor},
};

/// Opaque handle to a node. The `add_*` methods return it, and the caller passes
/// it back as a parent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnarNode(usize);

enum NodeKind {
    Source {
        topics: Vec<String>,
        codec: Arc<dyn BatchCodec>,
    },
    Operator {
        make: Arc<dyn Fn() -> Box<dyn ColumnarProcessor> + Send + Sync>,
    },
    Sink {
        topic: String,
        codec: Arc<dyn BatchCodec>,
    },
}

struct NodeDef {
    name: String,
    kind: NodeKind,
    parents: Vec<ColumnarNode>,
}

/// A columnar topology under construction.
#[derive(Default)]
pub struct ColumnarTopology {
    nodes: Vec<NodeDef>,
}

impl ColumnarTopology {
    /// Create an empty topology.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a source node that binds `topics` to `codec`.
    pub fn add_source(
        &mut self,
        name: &str,
        topics: impl IntoIterator<Item = impl Into<String>>,
        codec: impl BatchCodec,
    ) -> ColumnarNode {
        self.push(
            name,
            NodeKind::Source {
                topics: topics.into_iter().map(Into::into).collect(),
                codec: Arc::new(codec),
            },
            vec![],
        )
    }

    /// Add a built-in operator node fed by `parent`.
    ///
    /// Each `run_batch` builds a fresh operator instance, because operators are
    /// stateless in v1. A built topology can therefore run more than once.
    pub fn add_operator(
        &mut self,
        name: &str,
        op: BuiltinOp,
        parent: ColumnarNode,
    ) -> ColumnarNode {
        let make = move || -> Box<dyn ColumnarProcessor> { Box::new(op.clone()) };
        self.push(
            name,
            NodeKind::Operator {
                make: Arc::new(make),
            },
            vec![parent],
        )
    }

    /// Add a sink node fed by `parent`. It writes to `topic` with `codec`.
    pub fn add_sink(
        &mut self,
        name: &str,
        topic: &str,
        codec: impl BatchCodec,
        parent: ColumnarNode,
    ) -> ColumnarNode {
        self.push(
            name,
            NodeKind::Sink {
                topic: topic.into(),
                codec: Arc::new(codec),
            },
            vec![parent],
        )
    }

    fn push(&mut self, name: &str, kind: NodeKind, parents: Vec<ColumnarNode>) -> ColumnarNode {
        let id = ColumnarNode(self.nodes.len());
        self.nodes.push(NodeDef {
            name: name.into(),
            kind,
            parents,
        });
        id
    }

    /// Validate the graph.
    ///
    /// The rules are: all names are unique, a source has no parent, a non-source
    /// has ≥1 parent, and the graph has ≥1 source and ≥1 sink.
    ///
    /// # Errors
    /// Returns a message describing the first structural problem found.
    pub fn validate(&self) -> Result<(), String> {
        use std::collections::HashSet;
        let mut names = HashSet::new();
        let (mut sources, mut sinks) = (0u32, 0u32);
        for n in &self.nodes {
            if !names.insert(n.name.clone()) {
                return Err(format!("duplicate node name `{}`", n.name));
            }
            match n.kind {
                NodeKind::Source { .. } => {
                    sources += 1;
                    if !n.parents.is_empty() {
                        return Err(format!("source `{}` has a parent", n.name));
                    }
                }
                NodeKind::Sink { .. } => {
                    sinks += 1;
                    if n.parents.is_empty() {
                        return Err(format!("sink `{}` has no parent", n.name));
                    }
                }
                NodeKind::Operator { .. } => {
                    if n.parents.is_empty() {
                        return Err(format!("operator `{}` has no parent", n.name));
                    }
                }
            }
        }
        if sources == 0 {
            return Err("topology has no source".into());
        }
        if sinks == 0 {
            return Err("topology has no sink".into());
        }
        Ok(())
    }

    /// Source topics in declaration order. The runtime bridge, Task 11, reads
    /// them.
    #[must_use]
    pub fn source_topics(&self) -> Vec<String> {
        self.nodes
            .iter()
            .flat_map(|n| match &n.kind {
                NodeKind::Source { topics, .. } => topics.clone(),
                _ => vec![],
            })
            .collect()
    }
}

use std::collections::HashMap;

use super::{
    codec::{BatchError, ConsumedRecord, ProduceRecord},
    operator::ColumnarContext,
};

/// A built, runnable columnar topology. v1 runs it as one task. It is cheap to
/// construct.
pub struct BuiltColumnarTopology<'t> {
    topo: &'t ColumnarTopology,
}

impl ColumnarTopology {
    /// Validate and wrap for execution.
    ///
    /// # Errors
    /// Returns the validation error message if the graph is structurally invalid.
    #[tracing::instrument(
        name = "streams.columnar.build",
        level = "info",
        skip_all,
        fields(nodes = self.nodes.len()),
        err,
    )]
    pub fn build(&self) -> Result<BuiltColumnarTopology<'_>, String> {
        self.validate()?;
        Ok(BuiltColumnarTopology { topo: self })
    }
}

impl BuiltColumnarTopology<'_> {
    /// Run one batch of records that arrived on `topic` through the graph.
    ///
    /// Returns everything the sinks want produced, as `(sink_topic, record)`
    /// pairs.
    ///
    /// # Errors
    /// Returns `BatchError` if any codec or operator fails.
    #[tracing::instrument(
        name = "streams.columnar.run_batch",
        level = "debug",
        skip_all,
        fields(topic = %topic, records = records.len()),
        err,
    )]
    pub fn run_batch(
        &self,
        topic: &str,
        records: &[ConsumedRecord],
    ) -> Result<Vec<(String, ProduceRecord)>, BatchError> {
        let mut frames: HashMap<usize, Vec<::polars::prelude::DataFrame>> = HashMap::new();
        let mut produced = Vec::new();

        for (idx, node) in self.topo.nodes.iter().enumerate() {
            let inputs: Vec<::polars::prelude::DataFrame> = match &node.kind {
                NodeKind::Source { topics, codec } => {
                    if topics.iter().any(|t| t == topic) && !records.is_empty() {
                        vec![codec.decode(records)?]
                    } else {
                        vec![]
                    }
                }
                _ => node
                    .parents
                    .iter()
                    .flat_map(|p| frames.get(&p.0).cloned().unwrap_or_default())
                    .collect(),
            };

            match &node.kind {
                NodeKind::Source { .. } => {
                    frames.insert(idx, inputs);
                }
                NodeKind::Operator { make } => {
                    let mut proc = make();
                    let mut out = Vec::new();
                    for batch in inputs {
                        let mut ctx = ColumnarContext::new();
                        proc.process(&mut ctx, batch)?;
                        out.extend(ctx.take());
                    }
                    frames.insert(idx, out);
                }
                NodeKind::Sink {
                    topic: sink_topic,
                    codec,
                } => {
                    for batch in inputs {
                        for rec in codec.encode(&batch)? {
                            produced.push((sink_topic.clone(), rec));
                        }
                    }
                }
            }
        }
        Ok(produced)
    }
}

#[cfg(test)]
mod tests {
    use ::polars::prelude::*;
    use assert2::check;

    use super::*;
    use crate::{
        columnar::{
            serde::polars::PolarsIpcSerde,
            topology::{
                codec::{BlobCodec, ConsumedRecord},
                operator::BuiltinOp,
            },
        },
        processor::serde::Serde,
    };

    #[test]
    fn builds_and_validates_linear_topology() {
        let mut t = ColumnarTopology::new();
        let src = t.add_source("src", ["in"], BlobCodec::default());
        let op = t.add_operator("flt", BuiltinOp::Filter(col("amount").gt(lit(0))), src);
        t.add_sink("out", "out", BlobCodec::default(), op);
        check!(t.validate().is_ok());
        check!(t.source_topics() == vec!["in".to_string()]);
    }

    #[test]
    fn rejects_empty_topology() {
        let t = ColumnarTopology::new();
        check!(t.validate().is_err());
    }

    #[test]
    fn rejects_duplicate_names() {
        let mut t = ColumnarTopology::new();
        let s = t.add_source("dup", ["in"], BlobCodec::default());
        t.add_sink("dup", "out", BlobCodec::default(), s);
        check!(t.validate().is_err());
    }

    #[test]
    fn run_batch_filters_blob_records_end_to_end() {
        let mut t = ColumnarTopology::new();
        let src = t.add_source("src", ["in"], BlobCodec::default());
        let op = t.add_operator("flt", BuiltinOp::Filter(col("amount").gt(lit(4))), src);
        t.add_sink("out", "out", BlobCodec::default(), op);
        let built = t.build().unwrap();

        let df = df!("amount" => [1_i64, 5, 9]).unwrap();
        let rec = ConsumedRecord {
            key: None,
            value: PolarsIpcSerde.serialize("", &df),
            timestamp: 0,
            partition: 0,
            offset: 0,
        };
        let out = built.run_batch("in", &[rec]).unwrap();
        check!(out.len() == 1);
        check!(out[0].0 == "out");
        let back = PolarsIpcSerde.deserialize("", &out[0].1.value).unwrap();
        check!(back.height() == 2); // amounts 5 and 9
    }

    #[test]
    fn built_topology_runs_multiple_batches() {
        // A built topology must be reusable across batches (operators are rebuilt
        // per `run_batch`), not consumed after the first run.
        let mut t = ColumnarTopology::new();
        let src = t.add_source("src", ["in"], BlobCodec::default());
        let op = t.add_operator("flt", BuiltinOp::Filter(col("amount").gt(lit(4))), src);
        t.add_sink("out", "out", BlobCodec::default(), op);
        let built = t.build().unwrap();

        let mk = |amounts: &[i64]| {
            let df = df!("amount" => amounts.to_vec()).unwrap();
            vec![ConsumedRecord {
                key: None,
                value: PolarsIpcSerde.serialize("", &df),
                timestamp: 0,
                partition: 0,
                offset: 0,
            }]
        };

        let first = built.run_batch("in", &mk(&[1, 5, 9])).unwrap();
        let second = built.run_batch("in", &mk(&[7, 2])).unwrap();
        check!(first.len() == 1);
        check!(second.len() == 1);
        check!(
            PolarsIpcSerde
                .deserialize("", &first[0].1.value)
                .unwrap()
                .height()
                == 2
        );
        check!(
            PolarsIpcSerde
                .deserialize("", &second[0].1.value)
                .unwrap()
                .height()
                == 1
        );
    }

    #[test]
    fn run_batch_ignores_non_matching_source_topic() {
        // A record arriving for a topic no source declares produces nothing.
        let mut t = ColumnarTopology::new();
        let src = t.add_source("src", ["in"], BlobCodec::default());
        t.add_sink("out", "out", BlobCodec::default(), src);
        let built = t.build().unwrap();

        let df = df!("amount" => [1_i64]).unwrap();
        let rec = ConsumedRecord {
            key: None,
            value: PolarsIpcSerde.serialize("", &df),
            timestamp: 0,
            partition: 0,
            offset: 0,
        };
        check!(built.run_batch("other-topic", &[rec]).unwrap().is_empty());
    }
}
