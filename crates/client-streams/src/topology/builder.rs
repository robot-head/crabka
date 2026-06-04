//! Topology builder: public Processor-API surface.

use std::any::{Any, TypeId, type_name};
use std::collections::{BTreeMap, HashMap};

use crabka_protocol::owned::streams_group_heartbeat_request::Topology as WireTopology;

use super::grouping::group_nodes;
use super::node::{NodeKind, NodeRegistry};
use super::wire::to_wire;
use crate::processor::api::ProcessorSupplier;
use crate::processor::erased::ProcessorError;
use crate::processor::factory::{FactoryKind, MakeDeser, NodeFactory};
use crate::processor::graph::{Graph, GraphSource};
use crate::processor::node::{ErasedNode, ProcessorNode, SinkNode, SourceNode};
use crate::processor::serde::Serde;

// ──────────────────────────────────────────────────────────────────────────────
// TopologyError
// ──────────────────────────────────────────────────────────────────────────────

/// Error building a topology (bad node graph, invalid configuration, etc.).
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("node {node} references unknown predecessor {predecessor}")]
    UnknownPredecessor { node: String, predecessor: String },
    #[error("topology has no source nodes")]
    Empty,
    /// Build-time type mismatch: boxed to keep enum size small.
    #[error("{0}")]
    TypeMismatch(Box<TypeMismatchDetail>),
}

/// Detail for [`TopologyError::TypeMismatch`], boxed so the enum stays small.
#[derive(Debug)]
pub struct TypeMismatchDetail {
    pub parent: String,
    pub parent_kind: &'static str,
    pub produces: String,
    pub child: String,
    pub child_kind: &'static str,
    pub expects: String,
}

impl std::fmt::Display for TypeMismatchDetail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "topology wiring type error: {child_kind} `{child}` expects `{expects}`,\n  \
             but its parent {parent_kind} `{parent}` forwards `{produces}`\n  \
             = help: a node's output type (KOut, VOut) must match every child's input type (KIn, VIn)\n  \
             = note: checked at build() because the Processor API wires nodes by name;\n          \
             use the typed DSL (sub-project #4) for compile-time wiring safety",
            child_kind = self.child_kind,
            child = self.child,
            expects = self.expects,
            parent_kind = self.parent_kind,
            parent = self.parent,
            produces = self.produces,
        )
    }
}

// Cloneable error subset (minus TypeMismatch — only occurs during build, not stored).
#[derive(Debug, Clone)]
enum StoredError {
    DuplicateNode(String),
    UnknownPredecessor { node: String, predecessor: String },
    Empty,
}

impl From<StoredError> for TopologyError {
    fn from(e: StoredError) -> Self {
        match e {
            StoredError::DuplicateNode(n) => TopologyError::DuplicateNode(n),
            StoredError::UnknownPredecessor { node, predecessor } => {
                TopologyError::UnknownPredecessor { node, predecessor }
            }
            StoredError::Empty => TopologyError::Empty,
        }
    }
}

impl From<TopologyError> for StoredError {
    fn from(e: TopologyError) -> Self {
        match e {
            TopologyError::DuplicateNode(n) => StoredError::DuplicateNode(n),
            TopologyError::UnknownPredecessor { node, predecessor } => {
                StoredError::UnknownPredecessor { node, predecessor }
            }
            TopologyError::Empty | TopologyError::TypeMismatch(_) => StoredError::Empty,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Topology
// ──────────────────────────────────────────────────────────────────────────────

/// A Processor-API topology under construction. Node insertion order is
/// significant — it determines subtopology indices (JVM-matching).
#[derive(Default)]
pub struct Topology {
    reg: NodeRegistry,
    error: Option<StoredError>,
    factories: HashMap<String, NodeFactory>,
}

impl std::fmt::Debug for Topology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Topology")
            .field("reg", &self.reg)
            .field("error", &self.error)
            .field(
                "factories",
                &format!("<{} factories>", self.factories.len()),
            )
            .finish()
    }
}

impl Topology {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a source node reading the given external topics.
    ///
    /// `key_serde` / `value_serde` are used to deserialize incoming bytes into
    /// typed `Record<K, V>` values at runtime.
    pub fn add_source<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        key_serde: KS,
        value_serde: VS,
    ) -> &mut Self
    where
        K: Any + Send + Clone,
        V: Any + Send + Clone,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        let r = self.reg.add_source(&name, topics.clone());
        self.record(r);

        // Build the make_deser factory closure.
        let make_deser: MakeDeser = {
            let ks = key_serde;
            let vs = value_serde;
            let n = name.clone();
            Box::new(move || {
                let node = SourceNode::new(n.clone(), ks.clone(), vs.clone());
                Box::new(move |k: Option<&[u8]>, v: &[u8], ts: i64| node.deserialize(k, v, ts))
                    as Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<_, ProcessorError> + Send>
            })
        };

        self.factories.insert(
            name,
            NodeFactory {
                kind: FactoryKind::Source,
                input_kv: None,
                output_kv: Some((TypeId::of::<K>(), TypeId::of::<V>())),
                input_names: None,
                output_names: Some((type_name::<K>(), type_name::<V>())),
                make_node: None,
                make_deser: Some(make_deser),
            },
        );
        self
    }

    /// Add a processor node with the given predecessor node names.
    ///
    /// `supplier` produces a fresh `Processor` instance per task. The closure
    /// form `|| Box::new(MyProc)` satisfies [`ProcessorSupplier`] via a blanket
    /// impl and is the most common form.
    pub fn add_processor<KIn, VIn, KOut, VOut, S>(
        &mut self,
        name: impl Into<String>,
        supplier: S,
        predecessors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        KIn: Any + Send,
        VIn: Any + Send,
        KOut: Any + Send + Clone,
        VOut: Any + Send + Clone,
        S: ProcessorSupplier<KIn, VIn, KOut, VOut> + Clone,
    {
        let name: String = name.into();
        let preds: Vec<String> = predecessors.into_iter().map(Into::into).collect();
        let r = self.reg.add_processor(&name, preds);
        self.record(r);

        let make_node: crate::processor::factory::MakeNode = {
            let n = name.clone();
            let s = supplier;
            Box::new(move || {
                Box::new(ProcessorNode::<KIn, VIn, KOut, VOut>::new(n.clone(), &s))
                    as Box<dyn ErasedNode>
            })
        };

        self.factories.insert(
            name,
            NodeFactory {
                kind: FactoryKind::Processor,
                input_kv: Some((TypeId::of::<KIn>(), TypeId::of::<VIn>())),
                output_kv: Some((TypeId::of::<KOut>(), TypeId::of::<VOut>())),
                input_names: Some((type_name::<KIn>(), type_name::<VIn>())),
                output_names: Some((type_name::<KOut>(), type_name::<VOut>())),
                make_node: Some(make_node),
                make_deser: None,
            },
        );
        self
    }

    /// Add a sink node writing to `topic`, fed by the given predecessors.
    pub fn add_sink<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        topic: impl Into<String>,
        predecessors: impl IntoIterator<Item = impl Into<String>>,
        key_serde: KS,
        value_serde: VS,
    ) -> &mut Self
    where
        K: Any + Send,
        V: Any + Send,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let topic: String = topic.into();
        let preds: Vec<String> = predecessors.into_iter().map(Into::into).collect();
        let r = self.reg.add_sink(&name, topic.clone(), preds);
        self.record(r);

        let make_node: crate::processor::factory::MakeNode = {
            let n = name.clone();
            let t = topic.clone();
            let ks = key_serde;
            let vs = value_serde;
            Box::new(move || {
                Box::new(SinkNode::<K, V, KS, VS>::new(
                    n.clone(),
                    t.clone(),
                    ks.clone(),
                    vs.clone(),
                )) as Box<dyn ErasedNode>
            })
        };

        self.factories.insert(
            name,
            NodeFactory {
                kind: FactoryKind::Sink,
                input_kv: Some((TypeId::of::<K>(), TypeId::of::<V>())),
                output_kv: None,
                input_names: Some((type_name::<K>(), type_name::<V>())),
                output_names: None,
                make_node: Some(make_node),
                make_deser: None,
            },
        );
        self
    }

    /// Register a state store connected to the given processors (→ changelog).
    pub fn add_state_store<S, I, T>(&mut self, name: S, processors: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let procs = processors.into_iter().map(Into::into).collect();
        self.reg.add_store(&name.into(), procs);
        self
    }

    /// Register a topic name as an internal repartition topic.
    pub fn add_repartition_topic<S: Into<String>>(&mut self, name: S) -> &mut Self {
        self.reg.repartition_topics.insert(name.into());
        self
    }

    /// Derive subtopologies and the wire topology. `application_id` drives
    /// internal-topic names (`<app>-<store>-changelog`).
    ///
    /// This also validates that every parent→child edge has matching KV types.
    /// The wire `Topology` is byte-identical to the untyped implementation.
    pub fn build<S: Into<String>>(
        mut self,
        application_id: S,
    ) -> Result<BuiltTopology, TopologyError> {
        if let Some(e) = self.error.take() {
            return Err(e.into());
        }
        self.reg.validate_predecessors()?;
        let groups = group_nodes(&self.reg);
        if groups.is_empty() {
            return Err(TopologyError::Empty);
        }
        let app = application_id.into();
        let wire = to_wire(&groups, &app);

        // Build source_topics map (unchanged from original).
        let mut source_topics: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for g in &groups {
            let mut all = g.source_topics.clone();
            all.extend(g.repartition_source_topics.iter().cloned());
            source_topics.insert(g.id.clone(), all);
        }

        // ── Build-time type validation ────────────────────────────────────────
        for node in &self.reg.nodes {
            let predecessors: &[String] = match &node.kind {
                NodeKind::Processor { predecessors } | NodeKind::Sink { predecessors, .. } => {
                    predecessors
                }
                NodeKind::Source { .. } => continue,
            };

            let child_factory = self.factories.get(&node.name);
            let child_input = child_factory.and_then(|f| f.input_kv);
            let child_input_names = child_factory.and_then(|f| f.input_names);
            let child_kind = child_factory.map_or("unknown", |f| f.kind.as_str());

            for parent_name in predecessors {
                let Some(parent_factory) = self.factories.get(parent_name) else {
                    continue;
                };
                let Some(parent_output) = parent_factory.output_kv else {
                    continue;
                };
                let Some(child_input_kv) = child_input else {
                    continue;
                };

                if parent_output != child_input_kv {
                    let parent_output_names = parent_factory.output_names.unwrap_or(("?", "?"));
                    let child_input_names_pair = child_input_names.unwrap_or(("?", "?"));
                    return Err(TopologyError::TypeMismatch(Box::new(TypeMismatchDetail {
                        parent: parent_name.clone(),
                        parent_kind: parent_factory.kind.as_str(),
                        produces: format!(
                            "Record<{}, {}>",
                            parent_output_names.0, parent_output_names.1
                        ),
                        child: node.name.clone(),
                        child_kind,
                        expects: format!(
                            "Record<{}, {}>",
                            child_input_names_pair.0, child_input_names_pair.1
                        ),
                    })));
                }
            }
        }

        // ── Build node specs for instantiation ────────────────────────────────
        let node_specs: Vec<NodeSpec> = self
            .reg
            .nodes
            .iter()
            .map(|n| {
                let (predecessors, kind_str, st, sink_t) = match &n.kind {
                    NodeKind::Source { topics } => (Vec::new(), "source", topics.clone(), None),
                    NodeKind::Processor { predecessors } => {
                        (predecessors.clone(), "processor", Vec::new(), None)
                    }
                    NodeKind::Sink {
                        predecessors,
                        topic,
                    } => (
                        predecessors.clone(),
                        "sink",
                        Vec::new(),
                        Some(topic.clone()),
                    ),
                };
                NodeSpec {
                    name: n.name.clone(),
                    kind: kind_str,
                    predecessors,
                    source_topics: st,
                    sink_topic: sink_t,
                }
            })
            .collect();

        Ok(BuiltTopology {
            wire,
            source_topics,
            application_id: app,
            factories: self.factories,
            node_specs,
        })
    }

    fn record(&mut self, r: Result<(), TopologyError>) {
        if self.error.is_none()
            && let Err(e) = r
        {
            self.error = Some(e.into());
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// NodeSpec
// ──────────────────────────────────────────────────────────────────────────────

/// Lightweight description of one node's wiring (no type parameters).
/// Used during `BuiltTopology::instantiate()`.
pub(crate) struct NodeSpec {
    pub name: String,
    /// `"source"` | `"processor"` | `"sink"`
    pub kind: &'static str,
    /// Predecessor names (empty for sources).
    pub predecessors: Vec<String>,
    /// Topics read (sources only; empty otherwise).
    pub source_topics: Vec<String>,
    /// Topic written (sinks only; `None` otherwise).
    pub sink_topic: Option<String>,
}

// ──────────────────────────────────────────────────────────────────────────────
// BuiltTopology
// ──────────────────────────────────────────────────────────────────────────────

/// A built topology: the wire `Topology` plus the per-subtopology source-topic
/// map used to resolve task assignments to concrete topic-partitions.
///
/// NOTE: `BuiltTopology` is **not** `Clone` because the node factories hold
/// `Box<dyn Fn…>` closures that are not cloneable. The membership client wraps
/// it in an `Arc<BuiltTopology>` — use that for sharing across tasks.
pub struct BuiltTopology {
    wire: WireTopology,
    source_topics: BTreeMap<String, Vec<String>>,
    application_id: String,
    factories: HashMap<String, NodeFactory>,
    node_specs: Vec<NodeSpec>,
}

impl std::fmt::Debug for BuiltTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltTopology")
            .field("application_id", &self.application_id)
            .field("source_topics", &self.source_topics)
            .finish_non_exhaustive()
    }
}

impl BuiltTopology {
    /// The wire `Topology` to send in the join heartbeat.
    #[must_use]
    pub fn to_wire(&self) -> WireTopology {
        self.wire.clone()
    }

    /// The external + repartition source topics a subtopology's tasks read.
    #[must_use]
    pub fn source_topics_for(&self, subtopology_id: &str) -> &[String] {
        self.source_topics
            .get(subtopology_id)
            .map_or(&[], Vec::as_slice)
    }

    /// The application id (drives internal-topic names).
    #[must_use]
    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    /// Topics that are sources in this topology (for test driver / repartition
    /// loopback).
    #[must_use]
    pub fn list_source_topics(&self) -> Vec<String> {
        self.node_specs
            .iter()
            .filter(|s| s.kind == "source")
            .flat_map(|s| s.source_topics.iter().cloned())
            .collect()
    }

    /// Topics that sinks in this topology write to.
    #[must_use]
    pub fn list_sink_topics(&self) -> Vec<String> {
        self.node_specs
            .iter()
            .filter_map(|s| s.sink_topic.clone())
            .collect()
    }

    /// Instantiate a runnable [`Graph`] for this topology.
    ///
    /// Each call produces an independent graph (its own processor instances).
    pub(crate) fn instantiate(&self) -> Result<Graph, ProcessorError> {
        // 1. Collect the processor/sink nodes in spec order and build a name→idx map.
        //    Sources are NOT in the nodes vec — they become GraphSources.
        let non_source: Vec<&NodeSpec> = self
            .node_specs
            .iter()
            .filter(|s| s.kind != "source")
            .collect();

        let name_to_idx: HashMap<&str, usize> = non_source
            .iter()
            .enumerate()
            .map(|(i, s)| (s.name.as_str(), i))
            .collect();

        let nodes: Vec<Box<dyn ErasedNode>> = non_source
            .iter()
            .map(|s| {
                let factory = self
                    .factories
                    .get(&s.name)
                    .ok_or_else(|| ProcessorError::Serde {
                        node: s.name.clone(),
                        message: "factory missing".into(),
                    })?;
                let make = factory
                    .make_node
                    .as_ref()
                    .ok_or_else(|| ProcessorError::Serde {
                        node: s.name.clone(),
                        message: "make_node missing".into(),
                    })?;
                Ok((make)())
            })
            .collect::<Result<Vec<_>, ProcessorError>>()?;

        // 2. Build children[idx] = indices of nodes that have this node as a predecessor.
        let mut children: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
        for (child_idx, s) in non_source.iter().enumerate() {
            for parent_name in &s.predecessors {
                // parent might be a source (not in name_to_idx) or a processor/sink
                if let Some(&parent_idx) = name_to_idx.get(parent_name.as_str()) {
                    children[parent_idx].push(child_idx);
                }
            }
        }

        // 3. Build GraphSources.
        let sources: Vec<GraphSource> = self
            .node_specs
            .iter()
            .filter(|s| s.kind == "source")
            .flat_map(|src_spec| {
                // Find which processor/sink nodes list this source as a predecessor.
                let src_name = src_spec.name.as_str();
                let src_children: Vec<usize> = non_source
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.predecessors.iter().any(|p| p == src_name))
                    .map(|(i, _)| i)
                    .collect();

                let Some(factory) = self.factories.get(src_spec.name.as_str()) else {
                    return vec![];
                };
                let Some(make_deser) = &factory.make_deser else {
                    return vec![];
                };

                src_spec
                    .source_topics
                    .iter()
                    .map(|topic| {
                        let deser = (make_deser)();
                        GraphSource {
                            topic: topic.clone(),
                            deserialize: deser,
                            children: src_children.clone(),
                        }
                    })
                    .collect()
            })
            .collect();

        Ok(Graph {
            nodes,
            children,
            sources,
            output: Vec::new(),
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processor::api::{Processor, ProcessorContext};
    use crate::processor::record::Record;
    use crate::processor::serde::{I64Serde, StringSerde};
    use assert2::check;

    struct Upper;
    impl Processor<String, String, String, String> for Upper {
        fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    #[test]
    fn build_single_source_sink_wire_unchanged() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "up",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["src"],
        );
        t.add_sink("out", "out-topic", ["up"], StringSerde, StringSerde);
        let built = t.build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.epoch == 0);
        check!(wire.subtopologies[0].subtopology_id == "0");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
    }

    #[test]
    fn type_mismatch_is_reported_at_build() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "up",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["src"],
        ); // forwards Record<String,String>
        t.add_sink("out", "out-topic", ["up"], StringSerde, I64Serde); // expects Record<String,i64>
        let msg = t.build("app").unwrap_err().to_string();
        check!(msg.contains("wiring type error"));
        check!(msg.contains("`out`"));
        check!(msg.contains("`up`"));
    }

    #[test]
    fn unknown_predecessor_still_rejected() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_sink("out", "o", ["nope"], StringSerde, StringSerde);
        check!(t.build("app").is_err());
    }

    #[test]
    fn instantiate_runs_records() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "up",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["src"],
        );
        t.add_sink("out", "out-topic", ["up"], StringSerde, StringSerde);
        let built = t.build("app").unwrap();
        let mut g = built.instantiate().unwrap();
        g.pipe("in", Some(b"k"), b"hi", 0).unwrap();
        let out = g.take_output();
        check!(out.len() == 1);
        check!(out[0].value.as_ref().unwrap().as_ref() == b"HI");
    }

    #[test]
    fn empty_topology_is_rejected() {
        let topo = Topology::new();
        check!(topo.build("app").is_err());
    }

    #[test]
    fn build_with_processor_store_and_repartition() {
        let mut t = Topology::new();
        t.add_repartition_topic("rp");
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "proc",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["src"],
        );
        t.add_state_store("store", ["proc"]);
        t.add_sink("rsink", "rp", ["proc"], StringSerde, StringSerde);
        t.add_source("rsrc", ["rp"], StringSerde, StringSerde);
        t.add_sink("out", "out-topic", ["rsrc"], StringSerde, StringSerde);
        let built = t.build("my-app").unwrap();
        check!(built.application_id() == "my-app");
        let wire = built.to_wire();
        // repartition chain produces at least 2 subtopologies
        check!(wire.subtopologies.len() >= 2);
        // subtopology containing the state store has a changelog topic named my-app-store-changelog
        let has_changelog = wire.subtopologies.iter().any(|s| {
            s.state_changelog_topics
                .iter()
                .any(|c| c.name == "my-app-store-changelog")
        });
        check!(has_changelog);
    }

    #[test]
    fn application_id_accessor() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_sink("snk", "out", ["src"], StringSerde, StringSerde);
        let built = t.build("my-streams-app").unwrap();
        check!(built.application_id() == "my-streams-app");
    }

    #[test]
    fn source_topics_for_unknown_id_returns_empty() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_sink("snk", "out", ["src"], StringSerde, StringSerde);
        let built = t.build("app").unwrap();
        check!(built.source_topics_for("99").is_empty());
    }

    #[test]
    fn duplicate_node_name_propagates_error() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_source("src", ["other"], StringSerde, StringSerde); // duplicate
        check!(t.build("app").is_err());
    }

    #[test]
    fn forward_reference_predecessor_is_rejected_preventing_cycles() {
        // Referencing a not-yet-added node (which is how you'd build a cycle) is rejected.
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "p1",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["src", "p2"], // p2 not added yet → forward reference → rejected
        );
        check!(t.build("app").is_err());
    }

    #[test]
    fn source_to_sink_type_mismatch_reported() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], StringSerde, StringSerde); // produces Record<String,String>
        t.add_sink("out", "o", ["src"], StringSerde, I64Serde); // expects Record<String,i64>
        let msg = t.build("app").unwrap_err().to_string();
        check!(msg.contains("wiring type error"));
    }

    #[test]
    fn instantiate_repartition_topology_lists_topics() {
        let mut t = Topology::new();
        t.add_repartition_topic("rp");
        t.add_source("s1", ["in"], StringSerde, StringSerde);
        t.add_processor(
            "p",
            || Box::new(Upper) as Box<dyn Processor<String, String, String, String>>,
            ["s1"],
        );
        t.add_sink("to_rp", "rp", ["p"], StringSerde, StringSerde);
        t.add_source("s2", ["rp"], StringSerde, StringSerde);
        t.add_sink("out", "out", ["s2"], StringSerde, StringSerde);
        let built = t.build("app").unwrap();
        let mut srcs = built.list_source_topics();
        srcs.sort();
        check!(srcs == vec!["in".to_string(), "rp".to_string()]);
        let mut sinks = built.list_sink_topics();
        sinks.sort();
        check!(sinks == vec!["out".to_string(), "rp".to_string()]);
        // instantiate must succeed and pipe through the first subtopology
        let mut g = built.instantiate().unwrap();
        g.pipe("in", None, b"hi", 0).unwrap();
        let out1 = g.take_output();
        check!(out1.iter().any(|o| o.topic == "rp"));
    }
}
