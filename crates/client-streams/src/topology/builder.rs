//! Topology builder: public Processor-API surface.

use std::any::Any;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

/// Factory that builds a fresh erased [`StateStore`] given `(application_id, store_name)`.
///
/// The two `&str` arguments let the factory embed the correct changelog topic name
/// (`<app_id>-<store_name>-changelog`) at construction time, matching the wire topology.
type StoreFactory = Box<dyn Fn(&str, &str) -> Box<dyn crate::store::api::StateStore> + Send + Sync>;

use crabka_protocol::owned::streams_group_heartbeat_request::Topology as WireTopology;

use super::grouping::group_nodes;
use super::node::{NodeKind, NodeRegistry};
use super::wire::to_wire;
use crate::processor::api::ProcessorSupplier;
use crate::processor::erased::ProcessorError;
use crate::processor::factory::{MakeDeser, NodeFactory};
use crate::processor::graph::{Graph, GraphSource};
use crate::processor::node::{ErasedNode, ProcessorNode, SinkNode, SourceNode};
use crate::processor::serde::{Consumed, Produced, Serde};

// ──────────────────────────────────────────────────────────────────────────────
// TopologyError
// ──────────────────────────────────────────────────────────────────────────────

/// Error building a topology (bad node graph, invalid configuration, etc.).
///
/// Parent→child *type* mismatches are not represented here: typed
/// [`NodeHandle`] wiring makes them a compile error, so they never reach
/// `build()`.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("node {node} references unknown predecessor {predecessor}")]
    UnknownPredecessor { node: String, predecessor: String },
    #[error("topology has no source nodes")]
    Empty,
}

// Cloneable error subset.
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
            TopologyError::Empty => StoredError::Empty,
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
    store_factories: HashMap<String, StoreFactory>,
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
            .field(
                "store_factories",
                &format!("<{} store_factories>", self.store_factories.len()),
            )
            .finish()
    }
}

/// A typed handle to a node in a [`Topology`], returned by
/// [`Topology::add_source`] and [`Topology::add_processor`]. Pass it (by
/// reference) as a parent when adding a child node.
///
/// Wiring by value rather than by string name means the compiler does two jobs
/// that `build()` used to: a parent that doesn't exist yet simply has no handle
/// to pass (so forward references and cycles can't be written), and the
/// parent's output type `(K, V)` is checked against the child's input type —
/// a mismatch is a **compile error**, not a runtime `build()` failure.
///
/// `K` / `V` are the key/value types the node *produces*. The handle is cheap
/// to [`Clone`] (it carries only the node name) so one parent can feed many
/// children.
pub struct NodeHandle<K, V> {
    name: String,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> NodeHandle<K, V> {
    fn new(name: String) -> Self {
        Self {
            name,
            _pd: PhantomData,
        }
    }

    /// Reconstruct a typed handle from a node name recorded during DSL lowering.
    ///
    /// The DSL (sub-project #4) lowers a type-erased logical graph: each lowering
    /// thunk knows its own concrete `K`/`V` statically and looks up its parent's
    /// Processor-API node name from `LowerState`, rebuilding a typed handle to
    /// pass to [`Topology::add_processor`] / [`Topology::add_sink`].
    pub(crate) fn from_name(name: String) -> Self {
        Self::new(name)
    }

    /// The node's name, as it appears in the wire topology. Useful for
    /// [`Topology::add_state_store`], which connects stores to processors by
    /// name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<K, V> Clone for NodeHandle<K, V> {
    fn clone(&self) -> Self {
        Self::new(self.name.clone())
    }
}

impl<K, V> std::fmt::Debug for NodeHandle<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle")
            .field("name", &self.name)
            .finish()
    }
}

impl Topology {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a source node reading the given external topics, returning a typed
    /// [`NodeHandle`] used to wire children to it.
    ///
    /// `consumed` carries the key + value serdes used to deserialize incoming
    /// bytes into typed `Record<K, V>` values at runtime — written
    /// `Consumed::with(key_serde, value_serde)` so the two roles are visible.
    pub fn add_source<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        consumed: Consumed<KS, VS>,
    ) -> NodeHandle<K, V>
    where
        K: Any + Send + Clone,
        V: Any + Send + Clone,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let Consumed {
            key_serde,
            value_serde,
        } = consumed;
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
            name.clone(),
            NodeFactory {
                make_node: None,
                make_deser: Some(make_deser),
            },
        );
        NodeHandle::new(name)
    }

    /// Add a processor node fed by the given parent [`NodeHandle`]s, returning a
    /// handle for its output.
    ///
    /// Wiring is by value: every parent's output type must equal this
    /// processor's input type `(KIn, VIn)`, enforced by the compiler. `supplier`
    /// produces a fresh `Processor` per task; the closure form `|| MyProc`
    /// satisfies [`ProcessorSupplier`] via a blanket impl and infers all four KV
    /// type parameters from the processor's `Processor` impl, so callers never
    /// annotate them.
    pub fn add_processor<KIn, VIn, KOut, VOut, S, P, I>(
        &mut self,
        name: impl Into<String>,
        supplier: S,
        parents: I,
    ) -> NodeHandle<KOut, VOut>
    where
        KIn: Any + Send,
        VIn: Any + Send,
        KOut: Any + Send + Clone,
        VOut: Any + Send + Clone,
        S: ProcessorSupplier<KIn, VIn, KOut, VOut> + Clone,
        I: IntoIterator<Item = P>,
        P: Borrow<NodeHandle<KIn, VIn>>,
    {
        let name: String = name.into();
        let preds: Vec<String> = parents
            .into_iter()
            .map(|p| p.borrow().name.clone())
            .collect();
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
            name.clone(),
            NodeFactory {
                make_node: Some(make_node),
                make_deser: None,
            },
        );
        NodeHandle::new(name)
    }

    /// Add a sink node writing to `topic`, fed by the given parent
    /// [`NodeHandle`]s. Every parent's output type must equal the sink's input
    /// type `(K, V)` — enforced by the compiler.
    ///
    /// `produced` carries the key + value serdes used to serialize outgoing
    /// records — written `Produced::with(key_serde, value_serde)`. A sink is
    /// terminal, so nothing is returned.
    pub fn add_sink<K, V, KS, VS, P, I>(
        &mut self,
        name: impl Into<String>,
        topic: impl Into<String>,
        parents: I,
        produced: Produced<KS, VS>,
    ) where
        K: Any + Send,
        V: Any + Send,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
        I: IntoIterator<Item = P>,
        P: Borrow<NodeHandle<K, V>>,
    {
        let Produced {
            key_serde,
            value_serde,
        } = produced;
        let name: String = name.into();
        let topic: String = topic.into();
        let preds: Vec<String> = parents
            .into_iter()
            .map(|p| p.borrow().name.clone())
            .collect();
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
                make_node: Some(make_node),
                make_deser: None,
            },
        );
    }

    /// Register a state store connected to the given processors (→ changelog).
    ///
    /// `key_serde` / `value_serde` define how records are serialized into the
    /// changelog topic (`<app_id>-<name>-changelog`) and the store's byte map.
    /// Stores connect processors of possibly-differing types, so the processor
    /// list is by name — pass [`NodeHandle::name`] for a handle you hold.
    pub fn add_state_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: 'static,
        V: 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        self.add_state_store_inner::<K, V, KS, VS>(name, key_serde, value_serde, processors, None)
    }

    /// Register a state store whose changelog is an existing **source topic**
    /// rather than the derived `<app_id>-<name>-changelog`.
    ///
    /// This backs the `REUSE_KTABLE_SOURCE_TOPICS` DSL optimizer: a
    /// `builder.table(topic, ...)` store can reuse `topic` as its changelog, so
    /// no separate `app-<store>-changelog` topic is created and the wire
    /// topology lists `topic` as the store's changelog. `changelog_topic` is the
    /// topic name used both in the wire `state_changelog_topics` entry and as the
    /// runtime store's changelog target.
    pub fn add_state_store_with_changelog<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        processors: impl IntoIterator<Item = impl Into<String>>,
        changelog_topic: impl Into<String>,
    ) -> &mut Self
    where
        K: 'static,
        V: 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        self.add_state_store_inner::<K, V, KS, VS>(
            name,
            key_serde,
            value_serde,
            processors,
            Some(changelog_topic.into()),
        )
    }

    fn add_state_store_inner<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        processors: impl IntoIterator<Item = impl Into<String>>,
        changelog_override: Option<String>,
    ) -> &mut Self
    where
        K: 'static,
        V: 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg.add_store(&name, procs, changelog_override.clone());
        self.store_factories.insert(
            name,
            Box::new(move |app_id: &str, store_name: &str| {
                let changelog = changelog_override
                    .clone()
                    .unwrap_or_else(|| format!("{app_id}-{store_name}-changelog"));
                Box::new(crate::store::memory::InMemoryKeyValueStore::<K, V>::new(
                    store_name.to_string(),
                    Box::new(key_serde.clone()),
                    Box::new(value_serde.clone()),
                    changelog,
                )) as Box<dyn crate::store::api::StateStore>
            }),
        );
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
    /// Parent→child KV types are already guaranteed to match by the typed
    /// [`NodeHandle`] wiring, so `build()` only checks structural invariants
    /// (no duplicate names, every predecessor exists, at least one source). The
    /// wire `Topology` is byte-identical to the untyped implementation.
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
            store_factories: self.store_factories,
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
    store_factories: HashMap<String, StoreFactory>,
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
    /// The wire `Topology` as a serde-serializable view, for golden-frame
    /// interop assertions against captured JVM fixtures.
    #[must_use]
    pub fn to_wire(&self) -> super::wire::WireTopology {
        super::wire::WireTopology::from(&self.wire)
    }

    /// The raw protocol `Topology` to send in the `StreamsGroupHeartbeat` join.
    #[must_use]
    pub(crate) fn to_wire_request(&self) -> WireTopology {
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

        // Build the per-task store registry from the typed factories.
        let mut store_registry = crate::store::registry::StoreRegistry::default();
        for (store_name, factory) in &self.store_factories {
            store_registry.insert(factory(&self.application_id, store_name));
        }

        Ok(Graph {
            nodes,
            children,
            sources,
            output: Vec::new(),
            stores: store_registry,
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
    use crate::processor::serde::StringSerde;
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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink(
            "out",
            "out-topic",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
        let built = t.build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.epoch == 0);
        check!(wire.subtopologies[0].subtopology_id == "0");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
    }

    #[test]
    fn topology_error_converts_to_stored_error() {
        // `record()` stashes registry errors as the cloneable `StoredError`;
        // exercise every arm of the conversion so it stays total (the registry
        // only emits `DuplicateNode` at runtime, but the type covers all three).
        check!(matches!(
            StoredError::from(TopologyError::Empty),
            StoredError::Empty
        ));
        check!(matches!(
            StoredError::from(TopologyError::DuplicateNode("n".into())),
            StoredError::DuplicateNode(_)
        ));
        check!(matches!(
            StoredError::from(TopologyError::UnknownPredecessor {
                node: "a".into(),
                predecessor: "b".into(),
            }),
            StoredError::UnknownPredecessor { .. }
        ));
    }

    #[test]
    fn node_handle_clone_debug_and_owned_wiring() {
        // NodeHandle is Clone + Debug, and parents may be passed by value (an
        // owned handle), not only by reference — exercise all three.
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let src2 = src.clone();
        check!(src2.name() == "src");
        check!(format!("{src2:?}").contains("src"));
        // `[src2]` wires by value (the owned-handle `Borrow` path).
        t.add_sink(
            "out",
            "out",
            [src2],
            Produced::with(StringSerde, StringSerde),
        );
        check!(t.build("app").is_ok());
    }

    #[test]
    fn handle_from_another_topology_is_rejected_at_build() {
        // A handle's node name is only registered in the topology that created
        // it; wiring it into a different topology leaves a dangling predecessor
        // that `build()` rejects. (Within one topology, forward references and
        // cycles can't even be written — you need a parent's handle first.)
        let mut a = Topology::new();
        let foreign = a.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));

        let mut b = Topology::new();
        b.add_sink(
            "out",
            "o",
            [&foreign],
            Produced::with(StringSerde, StringSerde),
        );
        check!(b.build("app").is_err());
    }

    #[test]
    fn instantiate_runs_records() {
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink(
            "out",
            "out-topic",
            [&up],
            Produced::with(StringSerde, StringSerde),
        );
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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let proc = t.add_processor("proc", || Upper, [&src]);
        t.add_state_store("store", StringSerde, StringSerde, [proc.name()]);
        t.add_sink(
            "rsink",
            "rp",
            [&proc],
            Produced::with(StringSerde, StringSerde),
        );
        let rsrc = t.add_source("rsrc", ["rp"], Consumed::with(StringSerde, StringSerde));
        t.add_sink(
            "out",
            "out-topic",
            [&rsrc],
            Produced::with(StringSerde, StringSerde),
        );
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
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_sink(
            "snk",
            "out",
            [&src],
            Produced::with(StringSerde, StringSerde),
        );
        let built = t.build("my-streams-app").unwrap();
        check!(built.application_id() == "my-streams-app");
    }

    #[test]
    fn source_topics_for_unknown_id_returns_empty() {
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_sink(
            "snk",
            "out",
            [&src],
            Produced::with(StringSerde, StringSerde),
        );
        let built = t.build("app").unwrap();
        check!(built.source_topics_for("99").is_empty());
    }

    #[test]
    fn duplicate_node_name_propagates_error() {
        let mut t = Topology::new();
        t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_source("src", ["other"], Consumed::with(StringSerde, StringSerde)); // duplicate
        check!(t.build("app").is_err());
    }

    #[test]
    fn instantiate_repartition_topology_lists_topics() {
        let mut t = Topology::new();
        t.add_repartition_topic("rp");
        let s1 = t.add_source("s1", ["in"], Consumed::with(StringSerde, StringSerde));
        let p = t.add_processor("p", || Upper, [&s1]);
        t.add_sink(
            "to_rp",
            "rp",
            [&p],
            Produced::with(StringSerde, StringSerde),
        );
        let s2 = t.add_source("s2", ["rp"], Consumed::with(StringSerde, StringSerde));
        t.add_sink(
            "out",
            "out",
            [&s2],
            Produced::with(StringSerde, StringSerde),
        );
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

    #[test]
    fn instantiate_builds_stores_and_processes_statefully() {
        use crate::processor::serde::I64Serde;
        struct Counter;
        impl Processor<String, String, String, i64> for Counter {
            fn process(
                &mut self,
                ctx: &mut ProcessorContext<'_, '_, String, i64>,
                r: Record<String, String>,
            ) {
                let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                let n = s.get(&r.value).unwrap_or(0) + 1;
                s.put(r.value.clone(), n);
                ctx.forward(Record::new(Some(r.value), n, r.timestamp));
            }
        }
        let mut t = Topology::new();
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c], Produced::with(StringSerde, I64Serde));
        let built = t.build("app").unwrap();
        // wire topology still has the changelog topic (golden frame contract)
        check!(built.to_wire().subtopologies.iter().any(|s| {
            s.state_changelog_topics
                .iter()
                .any(|c| c.name == "app-counts-changelog")
        }));
        let mut g = built.instantiate().unwrap();
        g.pipe("in", None, b"x", 0).unwrap();
        g.pipe("in", None, b"x", 1).unwrap();
        // After two "x" records the count should be 2 (i64 big-endian = [0,0,0,0,0,0,0,2])
        check!(
            g.take_output()
                .last()
                .unwrap()
                .value
                .as_ref()
                .unwrap()
                .as_ref()
                == [0, 0, 0, 0, 0, 0, 0, 2]
        );
    }
}
