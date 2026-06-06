//! Topology builder: public Processor-API surface.

use std::any::Any;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

/// Factory that builds a fresh erased [`StateStore`] given the store name, pre-derived
/// changelog topic, and an already-opened byte backend. The factory only owns the serdes.
pub(crate) type StoreFactory = Box<
    dyn Fn(
            &str,
            String,
            Box<dyn crate::store::byte::ByteKeyValueStore>,
        ) -> Box<dyn crate::store::api::StateStore>
        + Send
        + Sync,
>;

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
    /// `(changelog_override, factory)` — override is `None` for the default
    /// `<app_id>-<store_name>-changelog` derivation.
    store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `GlobalKTable` store factories, keyed by store name. Kept SEPARATE from
    /// `store_factories` so per-task `instantiate` does NOT build them (a global
    /// store is fully replicated, not task-partitioned) and NO changelog topic is
    /// emitted. The override is always `None` here (global stores have no
    /// changelog) but the tuple mirrors the regular-store shape for the
    /// global-store builder/restorer (a later task) to consume. Populated by
    /// [`Topology::add_global_store`].
    global_store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `global store name -> source topic` for each `GlobalKTable`. The shared
    /// global consumer reads each source topic (all partitions) to fully
    /// replicate the matching store. Populated by [`Topology::add_global_store`]
    /// alongside `global_store_factories`. Invisible in the wire output.
    global_store_topics: HashMap<String, String>,
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
            .field(
                "global_store_factories",
                &format!(
                    "<{} global_store_factories>",
                    self.global_store_factories.len()
                ),
            )
            .field("global_store_topics", &self.global_store_topics)
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
        K: Send + 'static,
        V: Send + 'static,
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
        K: Send + 'static,
        V: Send + 'static,
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
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg.add_store(&name, procs, changelog_override.clone());
        self.store_factories.insert(
            name,
            (
                changelog_override,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::kv::KeyValueBytesStore::<K, V>::new(
                            store_name.to_string(),
                            backend,
                            Box::new(key_serde.clone()),
                            Box::new(value_serde.clone()),
                            changelog,
                        )) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a windowed state store connected to the given processors.
    ///
    /// Like [`add_state_store`] but for windowed stores. The changelog topic
    /// carries `compact,delete` configs and a `retention.ms` derived from
    /// `size_ms + grace_ms + 86_400_000` (the JVM's
    /// `windowstore.changelog.additional.retention.ms` default of 1 day).
    ///
    /// [`add_state_store`]: Topology::add_state_store
    pub fn add_window_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        size_ms: i64,
        grace_ms: i64,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        // windowstore.changelog.additional.retention.ms default = 1 day (86_400_000 ms)
        let retention_ms = size_ms + grace_ms + 86_400_000;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg.add_window_store(&name, procs, None, retention_ms);
        self.store_factories.insert(
            name.clone(),
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::window::WindowBytesStore::<K, V>::new(
                            store_name.to_string(),
                            backend,
                            Box::new(key_serde.clone()),
                            Box::new(value_serde.clone()),
                            changelog,
                        )) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a join window state store connected to the given processors.
    ///
    /// Like [`add_window_store`] but for join window stores (retainDuplicates).
    /// The changelog topic carries `delete`-only configs and a `retention.ms`
    /// derived from `before_ms + after_ms + grace_ms + 86_400_000`. Compaction is
    /// not applicable because the store retains duplicates.
    ///
    /// [`add_window_store`]: Topology::add_window_store
    #[allow(clippy::too_many_arguments)] // mirrors add_window_store + extra before_ms/after_ms split
    pub fn add_join_window_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        before_ms: i64,
        after_ms: i64,
        grace_ms: i64,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        // windowstore.changelog.additional.retention.ms default = 1 day (86_400_000 ms)
        let retention_ms = before_ms + after_ms + grace_ms + 86_400_000;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg
            .add_join_window_store(&name, procs, None, retention_ms);
        self.store_factories.insert(
            name.clone(),
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::join_window::JoinWindowBytesStore::<K, V>::new(
                            store_name.to_string(),
                            backend,
                            Box::new(key_serde.clone()),
                            Box::new(value_serde.clone()),
                            changelog,
                        )) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a session state store connected to the given processors.
    ///
    /// Like [`add_window_store`] but for session stores. Reuses the windowed
    /// (`compact,delete`) changelog config; the `retention.ms` is derived from
    /// `gap_ms + grace_ms + 86_400_000` (JVM `windowstore.changelog.additional.
    /// retention.ms` default of 1 day). The store holds the raw aggregate
    /// (`SessionBytesStore`).
    ///
    /// [`add_window_store`]: Topology::add_window_store
    pub fn add_session_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        gap_ms: i64,
        grace_ms: i64,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let retention_ms = gap_ms + grace_ms + 86_400_000;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        // Session changelog == windowed changelog (compact,delete + retention);
        // reuse the AggWindow ChangelogKind via add_window_store.
        self.reg.add_window_store(&name, procs, None, retention_ms);
        self.store_factories.insert(
            name.clone(),
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::session::SessionBytesStore::<K, V>::new(
                            store_name.to_string(),
                            backend,
                            Box::new(key_serde.clone()),
                            Box::new(value_serde.clone()),
                            changelog,
                        )) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a suppress buffer store connected to the given processor.
    ///
    /// The suppress buffer ([`SuppressBytesStore`]) is a time-ordered in-memory
    /// buffer with its own storage (it does NOT use the pluggable byte backend),
    /// so the factory ignores the opened backend. `logging` toggles ONLY the
    /// changelog: when `true` the changelog topic is emitted in the wire topology
    /// (a plain `cleanup.policy=compact` changelog — the JVM suppress buffer is a
    /// compacted KV store) and the store logs/restores; when `false` the store
    /// stays in memory and NO changelog topic appears (so a logging-off suppress
    /// is byte-identical to the slice-A wire output).
    ///
    /// [`SuppressBytesStore`]: crate::store::suppress_store::SuppressBytesStore
    pub fn add_suppress_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        logging: bool,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        // Logging toggles ONLY the changelog topic; the runtime store is always
        // registered so the processor can buffer through it either way. The suppress
        // changelog is a plain compacted KV changelog (ChangelogKind::Kv).
        if logging {
            self.reg.add_store(&name, procs, None);
        }
        self.store_factories.insert(
            name.clone(),
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          _backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        // logging off → empty changelog (never flushed) + flag off.
                        let cl = if logging { changelog } else { String::new() };
                        let mut store =
                            crate::store::suppress_store::SuppressBytesStore::<K, V>::new(
                                store_name.to_string(),
                                Box::new(key_serde.clone()),
                                Box::new(value_serde.clone()),
                                cl,
                            );
                        if !logging {
                            crate::store::api::StateStore::set_logging(&mut store, false);
                        }
                        Box::new(store) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a state store **without** a changelog topic.
    ///
    /// The store is available at runtime (for in-memory state), but NO entry is
    /// emitted in the wire topology's `state_changelog_topics` array. This backs
    /// `Materialized::with_logging(false)` in the DSL.
    pub(crate) fn add_state_store_no_changelog<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        // Insert only into store_factories (runtime use) — NOT into reg.stores,
        // so no changelog topic appears in the wire topology.
        self.store_factories.insert(
            name,
            (
                None, // no changelog
                Box::new(
                    move |store_name: &str,
                          _changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        // No changelog: pass empty string so the store never flushes.
                        Box::new(crate::store::kv::KeyValueBytesStore::<K, V>::new(
                            store_name.to_string(),
                            backend,
                            Box::new(key_serde.clone()),
                            Box::new(value_serde.clone()),
                            String::new(),
                        )) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a `GlobalKTable` source, update-processor, and KV store.
    ///
    /// A `GlobalKTable` is **invisible in the wire**: no subtopology of its own,
    /// no changelog topic. But its source node still occupies a node-group index
    /// during grouping (so other subtopology ids shift). This method:
    ///
    /// 1. registers a source node reading `topic` and a processor node fed by it
    ///    (the source→processor edge unites them into one node group);
    /// 2. marks `topic` global ([`NodeRegistry::add_global_source`]) so the
    ///    grouping pass skips it in the source-bucketing pass — the resulting
    ///    source-less group is dropped by the final filter but already consumed
    ///    its index;
    /// 3. stores the global KV factory in a SEPARATE map (`global_store_factories`,
    ///    NOT `store_factories`) so per-task `instantiate` does not build it and NO
    ///    changelog topic is emitted. The factory builds a
    ///    [`KeyValueBytesStore`] with an empty changelog (like
    ///    [`Topology::add_state_store_no_changelog`]).
    ///
    /// The global store is *not* built by `instantiate` in this slice — the
    /// fully-replicated global-store runtime (a later task) reads the factory.
    ///
    /// [`KeyValueBytesStore`]: crate::store::kv::KeyValueBytesStore
    pub fn add_global_store<K, V, KS, VS>(
        &mut self,
        store_name: impl Into<String>,
        source_name: impl Into<String>,
        topic: impl Into<String>,
        processor_name: impl Into<String>,
        consumed: Consumed<KS, VS>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let store_name: String = store_name.into();
        let source_name: String = source_name.into();
        let topic: String = topic.into();
        let processor_name: String = processor_name.into();
        let Consumed {
            key_serde,
            value_serde,
        } = consumed;

        // (a) source node reading the global topic (consumes a node-group index).
        let r = self.reg.add_source(&source_name, vec![topic.clone()]);
        self.record(r);
        // (b) update-processor fed by the source — the edge unites them.
        let r = self
            .reg
            .add_processor(&processor_name, vec![source_name.clone()]);
        self.record(r);
        // (c) mark the topic global so grouping skips it (group ends source-less).
        self.reg.add_global_source(&topic);

        // (d) global KV factory, in the SEPARATE global map. The override is
        //     `None`; the factory uses an empty changelog so the store never
        //     flushes. `instantiate` only iterates `store_factories`, so it ignores
        //     this and no changelog topic is emitted.
        let factory: StoreFactory = Box::new(
            move |sn: &str,
                  _changelog: String,
                  backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                Box::new(crate::store::kv::KeyValueBytesStore::<K, V>::new(
                    sn.to_string(),
                    backend,
                    Box::new(key_serde.clone()),
                    Box::new(value_serde.clone()),
                    String::new(),
                )) as Box<dyn crate::store::api::StateStore>
            },
        );
        self.global_store_topics.insert(store_name.clone(), topic);
        self.global_store_factories
            .insert(store_name, (None, factory));
        self
    }

    /// Connect an additional processor to an already-registered state store.
    ///
    /// Mirrors `InternalTopologyBuilder.connectProcessorAndStateStores`: a join
    /// processor needs to read the joined table's store even though it wasn't the
    /// store's original owner. The grouping pass unions all connected processors
    /// into one subtopology, so adding a second processor here is sufficient to
    /// pull the join processor into the same subtopology as the store.
    pub fn connect_processor_store(&mut self, processor: &str, store: &str) -> &mut Self {
        self.reg.connect_processor_store(processor, store);
        self
    }

    /// Return the connected-processor list for a store (test helper).
    #[cfg(test)]
    pub(crate) fn store_entry_for_test(&self, store: &str) -> Option<Vec<String>> {
        self.reg
            .stores
            .iter()
            .find(|e| e.name == store)
            .map(|e| e.processors.clone())
    }

    /// Whether a global store factory is registered under `store` (test helper).
    #[cfg(test)]
    pub(crate) fn has_global_store_for_test(&self, store: &str) -> bool {
        self.global_store_factories.contains_key(store)
    }

    /// Register a topic name as an internal repartition topic.
    pub fn add_repartition_topic<S: Into<String>>(&mut self, name: S) -> &mut Self {
        self.reg.repartition_topics.insert(name.into());
        self
    }

    /// Declare a copartition group: the given member topics must share a
    /// partitioning. The grouping pass assigns the group to the subtopology that
    /// reads its members, and the wire layer encodes member names as `int16`
    /// indices into that subtopology's sorted source/repartition arrays. Required
    /// for joins (KIP-1071).
    pub fn add_copartition_group(
        &mut self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        self.reg
            .add_copartition_group(topics.into_iter().map(Into::into).collect());
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

        // ── Identify the GlobalKTable source + update-processor nodes ─────────
        // A `GlobalKTable` source/processor is invisible in the wire AND has no
        // per-task runtime factory (the fully-replicated global store is built by
        // the shared global manager — a later task — or populated directly by the
        // TestDriver, NOT by per-task `instantiate`). Excluding them from
        // `node_specs` keeps `instantiate` from trying to build a node it has no
        // factory for. A node is global if it reads a global source topic, or if
        // any predecessor is global (nodes are in topological insertion order, so
        // one forward pass suffices).
        let mut global_nodes: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for n in &self.reg.nodes {
            let is_global = match &n.kind {
                NodeKind::Source { topics } => topics
                    .iter()
                    .any(|t| self.reg.global_source_topics.contains(t)),
                NodeKind::Processor { predecessors } | NodeKind::Sink { predecessors, .. } => {
                    predecessors
                        .iter()
                        .any(|p| global_nodes.contains(p.as_str()))
                }
            };
            if is_global {
                global_nodes.insert(n.name.as_str());
            }
        }

        // ── Build node specs for instantiation (excluding global nodes) ───────
        let node_specs: Vec<NodeSpec> = self
            .reg
            .nodes
            .iter()
            .filter(|n| !global_nodes.contains(n.name.as_str()))
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
            global_store_factories: self.global_store_factories,
            global_store_topics: self.global_store_topics,
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
    /// `(changelog_override, factory)` — override is `None` for the default
    /// `<app_id>-<store_name>-changelog` derivation.
    store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `GlobalKTable` store factories (separate from `store_factories`): NOT built
    /// by per-task `instantiate`. The fully-replicated global-store runtime
    /// (`StreamThread`) reads these to build + restore each global store once;
    /// the [`TopologyTestDriver`] reads them via
    /// [`global_store_factories`](Self::global_store_factories)
    /// to materialize global stores directly for join tests.
    global_store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `global store name -> source topic` for each `GlobalKTable`. Read by the
    /// shared global manager so the consumer knows which topic feeds each store.
    // Consumed by the global consumer / dispatch wiring in T7/T8 (via the accessor).
    #[allow(dead_code)]
    global_store_topics: HashMap<String, String>,
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

    /// The `GlobalKTable` store factories, keyed by store name. The
    /// [`TopologyTestDriver`] materializes these directly into its per-task store
    /// registry so a stream-globaltable join can find them; the real runtime
    /// (`StreamThread`) builds them once in a shared global manager, NOT per task.
    ///
    /// [`TopologyTestDriver`]: crate::TopologyTestDriver
    pub(crate) fn global_store_factories(
        &self,
    ) -> &HashMap<String, (Option<String>, StoreFactory)> {
        &self.global_store_factories
    }

    /// The `global store name -> source topic` map for each `GlobalKTable`.
    /// The shared [`GlobalStateManager`] reads this so the global consumer knows
    /// which topic feeds each store. Invisible in the wire output.
    ///
    /// [`GlobalStateManager`]: crate::runtime::global::GlobalStateManager
    pub(crate) fn global_store_topics(&self) -> HashMap<String, String> {
        self.global_store_topics.clone()
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
    /// Each call produces an independent graph (its own processor instances and
    /// a fresh byte-store backend opened via `backend`).
    pub(crate) async fn instantiate(
        &self,
        backend: &crate::store::backend::StoreBackend,
        app_id: &str,
    ) -> Result<Graph, ProcessorError> {
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
        for (store_name, (changelog_override, factory)) in &self.store_factories {
            let changelog = changelog_override
                .clone()
                .unwrap_or_else(|| format!("{app_id}-{store_name}-changelog"));
            let bytes = backend.open(app_id, store_name).await;
            store_registry.insert(factory(store_name, changelog, bytes));
        }

        Ok(Graph {
            nodes,
            children,
            sources,
            output: Vec::new(),
            stores: store_registry,
            // Default-empty here; the app wiring (T8b) / TopologyTestDriver builds
            // and assigns the shared GlobalStateManager. `instantiate` produces a
            // per-task graph and never owns the fully-replicated global stores.
            globals: crate::runtime::global::GlobalStateManager::default(),
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

    #[test]
    fn copartition_group_emitted_in_wire() {
        use crate::processor::serde::{BytesSerde, Consumed, Produced};
        let mut t = Topology::new();
        let a = t.add_source("sa", ["left"], Consumed::with(BytesSerde, BytesSerde));
        let b = t.add_source("sb", ["right"], Consumed::with(BytesSerde, BytesSerde));
        t.add_sink(
            "snk",
            "out",
            [&a, &b],
            Produced::with(BytesSerde, BytesSerde),
        ); // both → one subtopology
        t.add_copartition_group(["left", "right"]);
        let wire = t.build("app").unwrap().to_wire();
        let sub = &wire.subtopologies[0];
        check!(sub.copartition_groups.len() == 1);
        check!(sub.copartition_groups[0].source_topics == vec![0i16, 1i16]); // sorted ["left","right"]
    }

    #[test]
    fn global_store_is_invisible_and_bumps_stream_index() {
        // A GlobalKTable declared FIRST takes node-group index 0 but is invisible
        // in the wire (no subtopology, no changelog). The normal stream emits as
        // "1". The global topic appears in NO source_topics. The global factory is
        // recorded in the SEPARATE map (not built by `instantiate`).
        let mut t = Topology::new();
        // Global store declared first → index 0.
        t.add_global_store::<String, String, _, _>(
            "global-store",
            "gsrc",
            "global",
            "gproc",
            Consumed::with(StringSerde, StringSerde),
        );
        // Normal stream second → index 1.
        let src = t.add_source("src", ["in"], Consumed::with(StringSerde, StringSerde));
        t.add_sink(
            "snk",
            "out",
            [&src],
            Produced::with(StringSerde, StringSerde),
        );
        check!(t.has_global_store_for_test("global-store"));
        let built = t.build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].subtopology_id == "1");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        // No changelog topic anywhere.
        check!(
            wire.subtopologies
                .iter()
                .all(|s| s.state_changelog_topics.is_empty())
        );
        // The global topic is not a wire source topic.
        check!(
            !wire.subtopologies[0]
                .source_topics
                .contains(&"global".to_string())
        );
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
        let mut g = pollster::block_on(
            built.instantiate(&crate::store::backend::StoreBackend::InMemory, "app"),
        )
        .unwrap();
        pollster::block_on(g.pipe("in", Some(b"k"), b"hi", 0)).unwrap();
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
    fn connect_processor_store_adds_processor_to_store() {
        let mut t = Topology::new();
        // register a store connected to processor "p1"
        t.add_state_store::<String, String, _, _>("s", StringSerde, StringSerde, ["p1"]);
        t.connect_processor_store("p2", "s");
        // the store's connected-processor list now has both p1 and p2
        let entry = t.store_entry_for_test("s").unwrap();
        check!(entry == vec!["p1".to_string(), "p2".to_string()]);
    }

    #[test]
    fn connect_processor_store_is_idempotent() {
        let mut t = Topology::new();
        t.add_state_store::<String, String, _, _>("s", StringSerde, StringSerde, ["p1"]);
        t.connect_processor_store("p1", "s"); // p1 already connected — must not duplicate
        let entry = t.store_entry_for_test("s").unwrap();
        check!(entry == vec!["p1".to_string()]);
    }

    #[test]
    fn connect_processor_store_unknown_store_is_noop() {
        let mut t = Topology::new();
        t.add_state_store::<String, String, _, _>("s", StringSerde, StringSerde, ["p1"]);
        t.connect_processor_store("p2", "no_such_store"); // should not panic
        let entry = t.store_entry_for_test("s").unwrap();
        check!(entry == vec!["p1".to_string()]); // unchanged
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
        let mut g = pollster::block_on(
            built.instantiate(&crate::store::backend::StoreBackend::InMemory, "app"),
        )
        .unwrap();
        pollster::block_on(g.pipe("in", None, b"hi", 0)).unwrap();
        let out1 = g.take_output();
        check!(out1.iter().any(|o| o.topic == "rp"));
    }

    #[test]
    fn instantiate_builds_stores_and_processes_statefully() {
        use crate::processor::serde::I64Serde;
        struct Counter;
        #[async_trait]
        impl Processor<String, String, String, i64> for Counter {
            async fn process(
                &mut self,
                ctx: &mut ProcessorContext<'_, '_, String, i64>,
                r: Record<String, String>,
            ) {
                let n = {
                    let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                    let n = s.get(&r.value).await.unwrap_or(0) + 1;
                    s.put(r.value.clone(), n).await;
                    n
                };
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
        let mut g = pollster::block_on(
            built.instantiate(&crate::store::backend::StoreBackend::InMemory, "app"),
        )
        .unwrap();
        pollster::block_on(g.pipe("in", None, b"x", 0)).unwrap();
        pollster::block_on(g.pipe("in", None, b"x", 1)).unwrap();
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
