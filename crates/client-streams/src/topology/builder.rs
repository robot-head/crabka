//! Topology builder: public Processor-API surface.

use std::{
    any::Any,
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
};

/// Factory that builds a fresh erased [`StateStore`].
///
/// The caller supplies the store name, the pre-derived changelog topic, and an
/// already-opened byte backend. The factory owns only the serdes.
pub(crate) type StoreFactory = Box<
    dyn Fn(
            &str,
            String,
            Box<dyn crate::store::byte::ByteKeyValueStore>,
        ) -> Box<dyn crate::store::api::StateStore>
        + Send
        + Sync,
>;

/// The JVM default for `windowstore.changelog.additional.retention.ms`.
///
/// This is the extra time added to a window store's own retention. An instance
/// that restores its state can then still read the records that back its open
/// windows.
const CHANGELOG_ADDITIONAL_RETENTION: Time = days(1);

use crabka_protocol::owned::streams_group_heartbeat_request::Topology as WireTopology;
use crabka_units::prelude::*;

use super::{
    grouping::group_nodes,
    node::{NodeKind, NodeRegistry},
    wire::to_wire,
};
use crate::processor::{
    api::ProcessorSupplier,
    erased::ProcessorError,
    factory::{MakeDeser, NodeFactory},
    graph::{Graph, GraphSource},
    node::{ErasedNode, ProcessorNode, SinkNode, SourceNode},
    serde::{Consumed, DefaultSerde, Produced, Serde},
};

// ──────────────────────────────────────────────────────────────────────────────
// TopologyError
// ──────────────────────────────────────────────────────────────────────────────

/// An error that stops a topology build, such as a bad node graph or an
/// invalid configuration.
///
/// This enum does not cover parent→child *type* mismatches. Typed
/// [`NodeHandle`] wiring makes such a mismatch a compile error, so it never
/// reaches `build()`.
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

/// A Processor-API topology under construction.
///
/// Node insertion order is significant. It sets the subtopology indices, which
/// match the JVM.
#[derive(Default)]
pub struct Topology {
    reg: NodeRegistry,
    error: Option<StoredError>,
    factories: HashMap<String, NodeFactory>,
    /// `(changelog_override, factory)`. The override is `None` for the default
    /// `<app_id>-<store_name>-changelog` derivation.
    store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `GlobalKTable` store factories, keyed by store name. These stay SEPARATE
    /// from `store_factories` for two reasons: per-task `instantiate` must NOT
    /// build them, because a global store is fully replicated and not
    /// task-partitioned, and NO changelog topic is emitted. The override is
    /// always `None` here, because a global store has no changelog, but the
    /// tuple still mirrors the regular-store shape that the global-store manager
    /// reads. [`Topology::add_global_store`] fills this map.
    global_store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `global store name -> source topic` for each `GlobalKTable`. The shared
    /// global consumer reads all partitions of each source topic to fully
    /// replicate the matching store. [`Topology::add_global_store`] fills this
    /// map together with `global_store_factories`. It is invisible in the wire
    /// output.
    global_store_topics: HashMap<String, String>,
    /// Materialized KV stores that can use a record cache. This is JVM
    /// `Materialized` caching, which is on by default.
    /// [`Topology::mark_store_caching`] fills this set at the materialized
    /// `KTable` and aggregate lowering sites. `instantiate` reads it and wraps the
    /// store in a [`NamedCache`] when the cache budget is positive.
    caching_stores: std::collections::HashSet<String>,
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
            .field("caching_stores", &self.caching_stores)
            .finish()
    }
}

/// A typed handle to a node in a [`Topology`].
///
/// [`Topology::add_source`] and [`Topology::add_processor`] return a handle.
/// Pass it by reference as a parent when you add a child node.
///
/// Wiring uses values and not string names, so the compiler does two checks.
/// First, a parent that does not exist yet has no handle to pass, so you cannot
/// write a forward reference or a cycle. Second, the compiler checks the
/// parent's output type `(K, V)` against the child's input type. A mismatch is
/// a **compile error**, not a runtime `build()` failure.
///
/// `K` and `V` are the key and value types the node *produces*. The handle is
/// cheap to [`Clone`], because it carries only the node name, so one parent can
/// feed many children.
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
    /// The DSL lowers a type-erased logical graph. Each lowering thunk knows its
    /// own concrete `K` and `V` statically and looks up its parent's
    /// Processor-API node name in `LowerState`. The thunk then rebuilds a typed
    /// handle to pass to [`Topology::add_processor`] or [`Topology::add_sink`].
    pub(crate) fn from_name(name: String) -> Self {
        Self::new(name)
    }

    /// The node's name, as it appears in the wire topology.
    ///
    /// [`Topology::add_state_store`] needs this name, because it connects
    /// stores to processors by name.
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

    /// Add a source node reading the given external topics with the default
    /// serdes for `K` and `V`, returning a typed [`NodeHandle`] used to wire
    /// children to it.
    ///
    /// Use [`Topology::add_source_explicit`] when either type has no default
    /// serde or when a topology needs custom serdes.
    pub fn add_source<K, V>(
        &mut self,
        name: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> NodeHandle<K, V>
    where
        K: Any + Send + Sync + Clone + DefaultSerde,
        V: Any + Send + Sync + Clone + DefaultSerde,
    {
        self.add_source_explicit::<K, V, K::Serde, V::Serde>(
            name,
            topics,
            Consumed::with(K::Serde::default(), V::Serde::default()),
        )
    }

    /// Add a source node reading the given external topics with explicit serdes,
    /// returning a typed [`NodeHandle`] used to wire children to it.
    ///
    /// `consumed` carries the key serde and the value serde. At runtime they
    /// deserialize incoming bytes into typed `Record<K, V>` values. Write it as
    /// `Consumed::with(key_serde, value_serde)` so the two roles are visible.
    ///
    /// Prefer [`Topology::add_source`], the default-serde form, for types that
    /// implement [`DefaultSerde`]. Use this escape hatch when a type has no
    /// default serde, or to override the default for a topic. Examples are a
    /// hand-rolled `Serde<T>`, a **key**-role schema serde
    /// (`AvroSerde::<T>::key(&cache)`), and a validation-on JSON serde
    /// (`JsonSerde::value(&cache, true)`).
    pub fn add_source_explicit<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        consumed: impl Into<Consumed<KS, VS>>,
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
        } = consumed.into();
        let name: String = name.into();
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        // Let each serde pre-register any per-topic state (e.g. a schema-registry
        // subject) so membership pre-warm can resolve ids before processing.
        for t in &topics {
            key_serde.prepare(t, crate::processor::serde::SerdeRole::Key);
            value_serde.prepare(t, crate::processor::serde::SerdeRole::Value);
        }
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
    /// Wiring is by value. Every parent's output type must equal this
    /// processor's input type `(KIn, VIn)`, and the compiler enforces this.
    /// `supplier` produces a fresh `Processor` per task. The closure form
    /// `|| MyProc` satisfies [`ProcessorSupplier`] through a blanket impl, and
    /// it infers all four KV type parameters from the processor's `Processor`
    /// impl, so callers never annotate them.
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

    /// Add a sink node writing to `topic` with the default serdes for `K` and
    /// `V`, fed by the given parent [`NodeHandle`]s. Every parent's output type
    /// must equal the sink's input type `(K, V)`, and the compiler enforces this.
    ///
    /// Use [`Topology::add_sink_explicit`] when either type has no default serde
    /// or when a topology needs custom serdes.
    pub fn add_sink<K, V>(
        &mut self,
        name: impl Into<String>,
        topic: impl Into<String>,
        parents: impl IntoIterator<Item = impl Borrow<NodeHandle<K, V>>>,
    ) where
        K: Any + Send + Sync + DefaultSerde,
        V: Any + Send + Sync + DefaultSerde,
    {
        self.add_sink_explicit::<K, V, K::Serde, V::Serde, _, _>(
            name,
            topic,
            parents,
            Produced::with(K::Serde::default(), V::Serde::default()),
        );
    }

    /// Add a sink node writing to `topic` with explicit serdes, fed by the given
    /// parent [`NodeHandle`]s. Every parent's output type must equal the sink's
    /// input type `(K, V)`, and the compiler enforces this.
    ///
    /// `produced` carries the key serde and the value serde that serialize
    /// outgoing records. Write it as `Produced::with(key_serde, value_serde)`.
    /// A sink is terminal, so this method returns nothing.
    ///
    /// Prefer [`Topology::add_sink`], the default-serde form, for types that
    /// implement [`DefaultSerde`]. Use this escape hatch when a type has no
    /// default serde, or to override the default for a topic. Examples are a
    /// hand-rolled `Serde<T>`, a **key**-role schema serde
    /// (`AvroSerde::<T>::key(&cache)`), and a validation-on JSON serde
    /// (`JsonSerde::value(&cache, true)`).
    pub fn add_sink_explicit<K, V, KS, VS, P, I>(
        &mut self,
        name: impl Into<String>,
        topic: impl Into<String>,
        parents: I,
        produced: impl Into<Produced<KS, VS>>,
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
        } = produced.into();
        let name: String = name.into();
        let topic: String = topic.into();
        // Pre-register per-topic serde state (e.g. schema-registry subject).
        key_serde.prepare(&topic, crate::processor::serde::SerdeRole::Key);
        value_serde.prepare(&topic, crate::processor::serde::SerdeRole::Value);
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

    /// Register a state store connected to the given processors, with a
    /// changelog.
    ///
    /// `key_serde` and `value_serde` define how the store serializes records
    /// into the changelog topic (`<app_id>-<name>-changelog`) and into the
    /// store's byte map. Stores connect processors that can have different
    /// types, so the processor list uses names. Pass [`NodeHandle::name`] for a
    /// handle you hold.
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
    /// This backs the `REUSE_KTABLE_SOURCE_TOPICS` DSL optimizer. A
    /// `builder.table_explicit(topic, ...)` store can reuse `topic` as its
    /// changelog. No separate `app-<store>-changelog` topic is created, and the
    /// wire topology lists `topic` as the store's changelog. `changelog_topic`
    /// is the topic name used both in the wire `state_changelog_topics` entry
    /// and as the runtime store's changelog target.
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
    /// `size + grace +` [`CHANGELOG_ADDITIONAL_RETENTION`].
    ///
    /// [`add_state_store`]: Topology::add_state_store
    ///
    /// The tuple is `(retention_basis, window_size, grace)`. The **retention
    /// basis** sets the changelog `retention.ms`. For tumbling and hopping
    /// windows it equals the window size. For sliding windows it is the
    /// retention span (`2 * time_difference`), which is wider than the window.
    ///
    /// The **window size** is the true window length. Only the store's cache
    /// flush uses it, to rebuild the downstream `Windowed<K>` key's
    /// `end = start + window_size`, because the store-key bytes hold only the
    /// start. For sliding windows this is `time_difference` (1x), NOT the
    /// retention span.
    // retention basis + window size (key-end) are distinct
    pub fn add_window_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        window: (Time, Time, Time),
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let (retention_basis, window_size, grace) = window;
        let name: String = name.into();
        let retention = retention_basis + grace + CHANGELOG_ADDITIONAL_RETENTION;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg.add_window_store(&name, procs, None, retention);
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
                            // Aggregate window stores need the real window size so the
                            // cache flush can reconstruct `end = start + window_size`
                            // for the downstream `Windowed` key (the store-key bytes hold
                            // only the start). This is distinct from the retention basis
                            // — they diverge for sliding windows.
                            window_size,
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
    /// derived from `before + after + grace + `[`CHANGELOG_ADDITIONAL_RETENTION`].
    /// Compaction is not applicable because the store retains duplicates.
    ///
    /// [`add_window_store`]: Topology::add_window_store
    // mirrors add_window_store + extra before/after split
    pub fn add_join_window_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        window: (Time, Time, Time),
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let (before, after, grace) = window;
        let name: String = name.into();
        let retention = before + after + grace + CHANGELOG_ADDITIONAL_RETENTION;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg
            .add_join_window_store(&name, procs, None, retention);
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
    /// Like [`add_window_store`] but for session stores. It reuses the windowed
    /// (`compact,delete`) changelog config. The `retention.ms` comes from
    /// `gap + grace + `[`CHANGELOG_ADDITIONAL_RETENTION`]. The store holds the
    /// raw aggregate (`SessionBytesStore`).
    ///
    /// [`add_window_store`]: Topology::add_window_store
    pub fn add_session_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        gap: Time,
        grace: Time,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let retention = gap + grace + CHANGELOG_ADDITIONAL_RETENTION;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        // Session changelog == windowed changelog (compact,delete + retention);
        // reuse the AggWindow ChangelogKind via add_window_store.
        self.reg.add_window_store(&name, procs, None, retention);
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

    /// Register a versioned state store (KIP-889) connected to the given
    /// processors.
    ///
    /// The changelog topic carries `compact` + `min.compaction.lag.ms
    /// = history_retention + `[`CHANGELOG_ADDITIONAL_RETENTION`]. The
    /// version-chain store is self-contained in memory, so it does not use the
    /// supplied byte backend.
    pub fn add_versioned_store<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        history_retention: Time,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self
    where
        K: Send + Sync + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        self.add_versioned_store_inner::<K, V, KS, VS>(
            name,
            key_serde,
            value_serde,
            history_retention,
            processors,
            None,
        )
    }

    /// Like [`add_versioned_store`] but the changelog is an existing **source
    /// topic**. The `REUSE_KTABLE_SOURCE_TOPICS` optimizer points a versioned
    /// `builder.table_explicit(topic, …)` store's changelog at its own source
    /// `topic`. This mirrors [`add_state_store_with_changelog`].
    ///
    /// [`add_versioned_store`]: Topology::add_versioned_store
    /// [`add_state_store_with_changelog`]: Topology::add_state_store_with_changelog
    pub fn add_versioned_store_with_changelog<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        history_retention: Time,
        processors: impl IntoIterator<Item = impl Into<String>>,
        changelog_topic: impl Into<String>,
    ) -> &mut Self
    where
        K: Send + Sync + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        self.add_versioned_store_inner::<K, V, KS, VS>(
            name,
            key_serde,
            value_serde,
            history_retention,
            processors,
            Some(changelog_topic.into()),
        )
    }

    fn add_versioned_store_inner<K, V, KS, VS>(
        &mut self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
        history_retention: Time,
        processors: impl IntoIterator<Item = impl Into<String>>,
        changelog_override: Option<String>,
    ) -> &mut Self
    where
        K: Send + Sync + 'static,
        V: Send + 'static,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let name: String = name.into();
        let min_compaction_lag = history_retention + CHANGELOG_ADDITIONAL_RETENTION;
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg
            .add_versioned_store(&name, procs, changelog_override.clone(), min_compaction_lag);
        self.store_factories.insert(
            name.clone(),
            (
                changelog_override,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          _backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(
                            crate::store::versioned::VersionedBytesStore::<K, V>::new(
                                store_name.to_string(),
                                history_retention,
                                Box::new(key_serde.clone()),
                                Box::new(value_serde.clone()),
                                changelog,
                            ),
                        ) as Box<dyn crate::store::api::StateStore>
                    },
                ),
            ),
        );
        self
    }

    /// Register a KIP-213 FK-join subscription store connected to the given
    /// processors.
    ///
    /// The subscription store is keyed by `combined_key(fk, pk)` bytes →
    /// `ValueAndTimestamp<SubscriptionWrapper>` bytes. Its changelog is a plain
    /// compacted KV changelog (`<app_id>-<name>-changelog`, like
    /// [`add_state_store`]), NOT windowed retention. The store types are fixed
    /// to raw bytes in and `SubscriptionWrapper` out, so this method takes no
    /// key serde and no value serde.
    ///
    /// [`add_state_store`]: Topology::add_state_store
    pub(crate) fn add_fk_subscription_store(
        &mut self,
        name: impl Into<String>,
        processors: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        let name: String = name.into();
        let procs: Vec<String> = processors.into_iter().map(Into::into).collect();
        self.reg.add_store(&name, procs, None); // plain compact changelog
        self.store_factories.insert(
            name,
            (
                None,
                Box::new(
                    move |store_name: &str,
                          changelog: String,
                          backend: Box<dyn crate::store::byte::ByteKeyValueStore>| {
                        Box::new(crate::store::fk_subscription::SubscriptionBytesStore::new(
                            store_name.to_string(),
                            backend,
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
    /// buffer with its own storage. It does NOT use the pluggable byte backend,
    /// so the factory ignores the opened backend.
    ///
    /// `logging` toggles ONLY the changelog. When `true`, the wire topology
    /// gets the changelog topic, a plain `cleanup.policy=compact` changelog,
    /// because the JVM suppress buffer is a compacted KV store, and the store
    /// logs and restores. When `false`, the store stays in memory and NO
    /// changelog topic appears, so a logging-off suppress has the same wire
    /// shape as an unsuppressed topology.
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

    /// Register a join-grace buffer store (KIP-923) connected to the given
    /// processor.
    ///
    /// The grace buffer ([`JoinGraceBufferStore`]) is a stream-side,
    /// time-ordered in-memory buffer with its own storage. It does NOT use the
    /// pluggable byte backend, so the factory ignores the opened backend. Its
    /// changelog is a COMPACTED KV changelog (`ChangelogKind::Kv` →
    /// `cleanup.policy=compact`, `message.timestamp.type=CreateTime`, NO
    /// explicit `retention.ms`), pinned from the JVM 4.1.0 capture.
    ///
    /// `logging` toggles ONLY the changelog. When `true`, the changelog topic
    /// is emitted and the store logs and restores. When `false`, the store
    /// stays in memory and NO changelog topic appears.
    ///
    /// [`JoinGraceBufferStore`]: crate::store::join_grace_buffer::JoinGraceBufferStore
    pub(crate) fn add_join_grace_store<K, V, KS, VS>(
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
        // registered so the processor can buffer through it either way. The grace
        // buffer changelog is a plain compacted KV changelog (ChangelogKind::Kv).
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
                            crate::store::join_grace_buffer::JoinGraceBufferStore::<K, V>::new(
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
    /// The store is available at runtime for in-memory state, but NO entry is
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
    /// A `GlobalKTable` is **invisible in the wire**. It has no subtopology of
    /// its own and no changelog topic. Its source node still takes a node-group
    /// index during grouping, so other subtopology ids shift. This method does
    /// three things:
    ///
    /// 1. it registers a source node reading `topic` and a processor node fed
    ///    by it, and the source→processor edge unites them into one node group;
    /// 2. it marks `topic` global so the grouping pass skips it in the
    ///    source-bucketing pass. The final filter drops the resulting
    ///    source-less group, but that group already took its index;
    /// 3. it stores the global KV factory in a SEPARATE map
    ///    (`global_store_factories`, NOT `store_factories`) so per-task
    ///    `instantiate` does not build it and NO changelog topic is emitted. The
    ///    factory builds a [`KeyValueBytesStore`] with an empty changelog.
    ///
    /// Per-task `instantiate` does not build the global store. The
    /// fully-replicated global-store manager reads the factory.
    ///
    /// [`KeyValueBytesStore`]: crate::store::kv::KeyValueBytesStore
    pub fn add_global_store<K, V, KS, VS>(
        &mut self,
        store_name: impl Into<String>,
        source_name: impl Into<String>,
        topic: impl Into<String>,
        processor_name: impl Into<String>,
        consumed: impl Into<Consumed<KS, VS>>,
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
        } = consumed.into();

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
    /// This mirrors `InternalTopologyBuilder.connectProcessorAndStateStores`. A
    /// join processor must read the joined table's store even though it was not
    /// the store's original owner. The grouping pass unions all connected
    /// processors into one subtopology, so a second processor here is enough to
    /// pull the join processor into the same subtopology as the store.
    pub fn connect_processor_store(&mut self, processor: &str, store: &str) -> &mut Self {
        self.reg.connect_processor_store(processor, store);
        self
    }

    /// Mark a materialized store as record-cache eligible.
    ///
    /// This is JVM `Materialized` caching, which is on by default. The
    /// materialized `KTable` and aggregate lowering sites call this method with
    /// the `Materialized`'s `caching_enabled()`. `on == false`, which
    /// `with_caching(false)` gives, leaves the store uncached.
    pub(crate) fn mark_store_caching(&mut self, name: &str, on: bool) {
        if on {
            self.caching_stores.insert(name.to_string());
        }
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

    /// Declare a copartition group. The given member topics must share a
    /// partitioning.
    ///
    /// The grouping pass assigns the group to the subtopology that reads its
    /// members. The wire layer encodes member names as `int16` indices into that
    /// subtopology's sorted source and repartition arrays. Joins need this
    /// (KIP-1071).
    pub fn add_copartition_group(
        &mut self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        self.reg
            .add_copartition_group(topics.into_iter().map(Into::into).collect());
        self
    }

    /// Derive subtopologies and the wire topology.
    ///
    /// `application_id` sets the internal-topic names
    /// (`<app>-<store>-changelog`).
    ///
    /// The typed [`NodeHandle`] wiring already guarantees that parent→child KV
    /// types match, so `build()` checks only the structural invariants: no
    /// duplicate names, every predecessor exists, and at least one source. The
    /// wire `Topology` is byte-identical to the untyped implementation.
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
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
        // the shared global manager or populated directly by the TestDriver, NOT
        // by per-task `instantiate`). Excluding them from
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

        // Store name → connected processor names (used by `instantiate` to root a
        // cached store's forwarded changes at its materializing processor's node).
        let store_processors: HashMap<String, Vec<String>> = self
            .reg
            .stores
            .iter()
            .map(|e| (e.name.clone(), e.processors.clone()))
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
            caching_stores: self.caching_stores,
            store_processors,
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

/// Lightweight description of one node's wiring, with no type parameters.
///
/// `BuiltTopology::instantiate()` uses this description.
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
/// map that resolves task assignments to concrete topic-partitions.
///
/// NOTE: `BuiltTopology` is **not** `Clone`, because the node factories hold
/// `Box<dyn Fn…>` closures that are not cloneable. The membership client wraps
/// it in an `Arc<BuiltTopology>`. Use that to share it across tasks.
pub struct BuiltTopology {
    wire: WireTopology,
    source_topics: BTreeMap<String, Vec<String>>,
    application_id: String,
    factories: HashMap<String, NodeFactory>,
    node_specs: Vec<NodeSpec>,
    /// `(changelog_override, factory)`. The override is `None` for the default
    /// `<app_id>-<store_name>-changelog` derivation.
    store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `GlobalKTable` store factories, separate from `store_factories`. Per-task
    /// `instantiate` does NOT build them. The fully-replicated global-store
    /// runtime (`StreamThread`) reads these to build and restore each global
    /// store once. The [`TopologyTestDriver`] reads them through
    /// [`global_store_factories`](Self::global_store_factories)
    /// to materialize global stores directly for join tests.
    global_store_factories: HashMap<String, (Option<String>, StoreFactory)>,
    /// `global store name -> source topic` for each `GlobalKTable`. The shared
    /// global manager reads this so the consumer knows which topic feeds each
    /// store.
    // Consumed by global-store wiring via the accessor.
    #[allow(dead_code)]
    global_store_topics: HashMap<String, String>,
    /// Materialized KV stores that can use a record cache. This is JVM
    /// `Materialized` caching, which is on by default; `with_caching(false)`
    /// opts out. `instantiate` wraps each store in a [`NamedCache`] when the
    /// cache budget is positive.
    caching_stores: std::collections::HashSet<String>,
    /// Store name → its connected processor names. A cached store has one
    /// materializing processor. `instantiate` maps that processor name to the
    /// node's graph index to root the store's `cache_owner` entry.
    store_processors: HashMap<String, Vec<String>>,
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

    /// The `GlobalKTable` store factories, keyed by store name.
    ///
    /// The [`TopologyTestDriver`] materializes these directly into its per-task
    /// store registry so a stream-globaltable join can find them. The real
    /// runtime (`StreamThread`) builds them once in a shared global manager,
    /// NOT per task.
    ///
    /// [`TopologyTestDriver`]: crate::TopologyTestDriver
    pub(crate) fn global_store_factories(
        &self,
    ) -> &HashMap<String, (Option<String>, StoreFactory)> {
        &self.global_store_factories
    }

    /// The `global store name -> source topic` map for each `GlobalKTable`.
    ///
    /// The shared global-store manager reads this so the global consumer knows
    /// which topic feeds each store. The map is invisible in the wire output.
    pub(crate) fn global_store_topics(&self) -> HashMap<String, String> {
        self.global_store_topics.clone()
    }

    /// The external and repartition source topics that a subtopology's tasks
    /// read.
    #[must_use]
    pub fn source_topics_for(&self, subtopology_id: &str) -> &[String] {
        self.source_topics
            .get(subtopology_id)
            .map_or(&[], Vec::as_slice)
    }

    /// The application id, which sets the internal-topic names.
    #[must_use]
    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    /// Topics that are sources in this topology, for the test driver and the
    /// repartition loopback.
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
    /// Each call produces an independent graph with its own processor instances
    /// and a fresh byte-store backend opened through `backend`.
    #[tracing::instrument(
        name = "streams.topology.instantiate",
        level = "info",
        skip_all,
        fields(app_id = %app_id, nodes = self.node_specs.len(), cache_max_bytes = cache_max_bytes.bytes_i64()),
        err,
    )]
    pub(crate) async fn instantiate(
        &self,
        backend: &crate::store::backend::StoreBackend,
        app_id: &str,
        cache_max_bytes: ByteSize,
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

        // Build-time record-cache wiring (see `wire_record_caches`): a no-op when
        // the budget is zero (TopologyTestDriver) or no store is marked.
        let (cache, cache_owner) =
            self.wire_record_caches(&mut store_registry, &name_to_idx, cache_max_bytes);

        Ok(Graph {
            nodes,
            children,
            sources,
            output: Vec::new(),
            stores: store_registry,
            // Default-empty here; app wiring / TopologyTestDriver builds
            // and assigns the shared GlobalStateManager. `instantiate` produces a
            // per-task graph and never owns the fully-replicated global stores.
            globals: crate::runtime::global::GlobalStateManager::default(),
            schedules: Vec::new(),
            stream_time: i64::MIN,
            wall_clock: 0,
            cache_max_bytes,
            cache_owner,
            cache,
        })
    }

    /// Wire the record caches at build time.
    ///
    /// For each materialized KV store marked cache-eligible by the default-on
    /// `Materialized` caching, this method does three things. It registers a
    /// [`NamedCache`](crate::store::cache::named::NamedCache) in the per-task
    /// [`ThreadCache`](crate::store::cache::thread::ThreadCache). It wraps the
    /// typed store's backend through the erased
    /// [`enable_cache_erased`](crate::store::api::StateStore::enable_cache_erased)
    /// hook. It then roots the store's forwarded changes at the materializing
    /// node.
    ///
    /// The method returns the populated `ThreadCache` and the `store name →
    /// owning node index` map for `Graph::flush_caches`. With a zero budget, as
    /// in the `TopologyTestDriver`, or with no marked stores, the cache and the
    /// owner map both stay empty. Every store then stays uncached and the
    /// goldens do not change.
    fn wire_record_caches(
        &self,
        store_registry: &mut crate::store::registry::StoreRegistry,
        name_to_idx: &HashMap<&str, usize>,
        cache_max_bytes: ByteSize,
    ) -> (
        crate::store::cache::thread::ThreadCache,
        HashMap<String, usize>,
    ) {
        let mut cache = crate::store::cache::thread::ThreadCache::new(cache_max_bytes);
        let mut cache_owner: HashMap<String, usize> = HashMap::new();
        if cache_max_bytes <= ByteSize::ZERO {
            return (cache, cache_owner);
        }
        for store_name in &self.caching_stores {
            // The store's single materializing processor → its graph node index.
            let owner_idx = self
                .store_processors
                .get(store_name)
                .and_then(|procs| procs.first())
                .and_then(|proc_name| name_to_idx.get(proc_name.as_str()).copied());
            let Some(owner_idx) = owner_idx else {
                // No connected processor in this subtopology (e.g. a store owned
                // by another partition's graph) — nothing to root here.
                continue;
            };
            let nc = cache.register(store_name);
            // KV, window, and session stores are all cache-aware: each overrides
            // `enable_cache_erased` to return `true` and drives its own
            // `flush_cache_into`. A store that declines caching (returns `false`)
            // is skipped so we don't root a `cache_owner` entry the flush
            // mechanism couldn't drive.
            if store_registry.enable_cache(store_name, nc) {
                cache_owner.insert(store_name.clone(), owner_idx);
            }
        }
        (cache, cache_owner)
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use assert2::check;
    use async_trait::async_trait;

    use super::*;
    use crate::processor::{
        api::{Processor, ProcessorContext},
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

    #[test]
    fn copartition_group_emitted_in_wire() {
        let mut t = Topology::new();
        let a: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("sa", ["left"]);
        let b: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("sb", ["right"]);
        t.add_sink("snk", "out", [&a, &b]); // both → one subtopology
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
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
    fn add_source_and_sink_can_use_default_serdes() {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("out", "out-topic", [&src]);
        let built = t.build("app").unwrap();
        let wire = built.to_wire();

        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
    }

    #[test]
    fn build_single_source_sink_wire_unchanged() {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink("out", "out-topic", [&up]);
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let src2 = src.clone();
        check!(src2.name() == "src");
        check!(format!("{src2:?}").contains("src"));
        // `[src2]` wires by value (the owned-handle `Borrow` path).
        t.add_sink("out", "out", [src2]);
        check!(t.build("app").is_ok());
    }

    #[test]
    fn handle_from_another_topology_is_rejected_at_build() {
        // A handle's node name is only registered in the topology that created
        // it; wiring it into a different topology leaves a dangling predecessor
        // that `build()` rejects. (Within one topology, forward references and
        // cycles can't even be written — you need a parent's handle first.)
        let mut a = Topology::new();
        let foreign: NodeHandle<String, String> = a.add_source("src", ["in"]);

        let mut b = Topology::new();
        b.add_sink("out", "o", [&foreign]);
        check!(b.build("app").is_err());
    }

    #[test]
    fn instantiate_runs_records() {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let up = t.add_processor("up", || Upper, [&src]);
        t.add_sink("out", "out-topic", [&up]);
        let built = t.build("app").unwrap();
        let mut g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            ByteSize::ZERO,
        ))
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let proc = t.add_processor("proc", || Upper, [&src]);
        t.add_state_store("store", StringSerde, StringSerde, [proc.name()]);
        t.add_sink("rsink", "rp", [&proc]);
        let rsrc: NodeHandle<String, String> = t.add_source("rsrc", ["rp"]);
        t.add_sink("out", "out-topic", [&rsrc]);
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        let built = t.build("my-streams-app").unwrap();
        check!(built.application_id() == "my-streams-app");
    }

    #[test]
    fn source_topics_for_unknown_id_returns_empty() {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
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
        let _: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let _: NodeHandle<String, String> = t.add_source("src", ["other"]); // duplicate
        check!(t.build("app").is_err());
    }

    #[test]
    fn instantiate_repartition_topology_lists_topics() {
        let mut t = Topology::new();
        t.add_repartition_topic("rp");
        let s1: NodeHandle<String, String> = t.add_source("s1", ["in"]);
        let p = t.add_processor("p", || Upper, [&s1]);
        t.add_sink("to_rp", "rp", [&p]);
        let s2: NodeHandle<String, String> = t.add_source("s2", ["rp"]);
        t.add_sink("out", "out", [&s2]);
        let built = t.build("app").unwrap();
        let mut srcs = built.list_source_topics();
        srcs.sort();
        check!(srcs == vec!["in".to_string(), "rp".to_string()]);
        let mut sinks = built.list_sink_topics();
        sinks.sort();
        check!(sinks == vec!["out".to_string(), "rp".to_string()]);
        // instantiate must succeed and pipe through the first subtopology
        let mut g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            ByteSize::ZERO,
        ))
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
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor("c", || Counter, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        t.add_sink("out", "out", [&c]);
        let built = t.build("app").unwrap();
        // wire topology still has the changelog topic (golden frame contract)
        check!(built.to_wire().subtopologies.iter().any(|s| {
            s.state_changelog_topics
                .iter()
                .any(|c| c.name == "app-counts-changelog")
        }));
        let mut g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            ByteSize::ZERO,
        ))
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

    // ── Build-time record-cache wiring (sub-task 3b-ii) ───────────────────────
    //
    // A materializing processor that, per record, accumulates a count in its
    // store and forwards `Change<i64>` (so the store's owning node can feed a
    // `Change`-consuming sink). Mirrors the DSL aggregate's store-write + emit.
    struct CountingMaterializer;
    #[async_trait]
    impl Processor<String, String, String, crate::dsl::processors::change::Change<i64>>
        for CountingMaterializer
    {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, crate::dsl::processors::change::Change<i64>>,
            r: Record<String, String>,
        ) {
            use crate::dsl::processors::change::Change;
            let (old, new) = {
                let s = ctx.get_state_store::<String, i64>("counts").unwrap();
                let old = s.get(&r.value).await;
                let new = old.unwrap_or(0) + 1;
                s.put(r.value.clone(), new).await;
                (old, Some(new))
            };
            ctx.forward(Record::new(Some(r.value), Change { old, new }, r.timestamp));
        }
    }

    /// Build a `source → CountingMaterializer(materializes "counts") → sink`
    /// topology, optionally marking the "counts" store cache-eligible.
    fn counting_topology(caching: bool) -> BuiltTopology {
        use crate::{dsl::processors::change::Change, processor::serde::I64Serde};
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let c = t.add_processor("c", || CountingMaterializer, [&src]);
        t.add_state_store("counts", StringSerde, I64Serde, [c.name()]);
        // The materializing node's `Change<i64>` output feeds the sink (a real
        // cached flush forwards `Change<i64>` rooted at "c").
        t.add_sink_explicit::<String, Change<i64>, _, _, _, _>(
            "out",
            "out",
            [&c],
            crate::processor::serde::Produced::with(StringSerde, ChangeI64Serde),
        );
        if caching {
            t.mark_store_caching("counts", true);
        }
        t.build("app").unwrap()
    }

    // A trivial serde so the `Change<i64>` sink can be wired; it never has to
    // round-trip in these tests (we assert on the changelog, not the sink bytes).
    #[derive(Clone)]
    struct ChangeI64Serde;
    impl crate::processor::serde::Serde<crate::dsl::processors::change::Change<i64>>
        for ChangeI64Serde
    {
        fn serialize(
            &self,
            _topic: &str,
            v: &crate::dsl::processors::change::Change<i64>,
        ) -> bytes::Bytes {
            // Encode just the `new` side (8 bytes BE) so the sink has bytes to emit.
            bytes::Bytes::copy_from_slice(&v.new.unwrap_or(0).to_be_bytes())
        }
        fn deserialize(
            &self,
            _topic: &str,
            _bytes: &[u8],
        ) -> Result<crate::dsl::processors::change::Change<i64>, crate::processor::serde::SerdeError>
        {
            unreachable!("Change<i64> sink is never deserialized in these tests")
        }
    }

    #[test]
    fn instantiate_caches_materialized_store_when_budget_positive() {
        // cache_max_bytes > 0 + marked store → the store is cached and
        // cache_owner roots it at the materializing node ("c"), which is graph
        // node index 0 (sources are not in the nodes vec; "c" is first non-source).
        let built = counting_topology(true);
        let mut g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            kibibytes(1),
        ))
        .unwrap();
        check!(g.cache_owner.get("counts") == Some(&0));
        check!(g.stores.kv_is_cached("counts"));

        // Pipe two records for the SAME key, then flush: a cached store dedups the
        // two staged writes into ONE changelog entry. (Without caching the store
        // logs each put immediately → two entries; see the no-cache test below.)
        pollster::block_on(g.pipe("in", None, b"x", 0)).unwrap();
        pollster::block_on(g.pipe("in", None, b"x", 1)).unwrap();
        // No changelog buffered yet — cached writes defer logging to flush.
        check!(
            g.drain_changelogs(&std::collections::HashSet::new())
                .is_empty()
        );
        pollster::block_on(g.flush_caches()).unwrap();
        let cl = g.drain_changelogs(&std::collections::HashSet::new());
        check!(cl.len() == 1); // deduped to the latest count (2)
        // tuple is (changelog_topic, key, value, ts); value is the BE i64 count.
        check!(cl[0].2.as_ref().unwrap().as_ref() == [0, 0, 0, 0, 0, 0, 0, 2]);
    }

    #[test]
    fn instantiate_does_not_cache_when_budget_zero() {
        // cache_max_bytes == 0 (TopologyTestDriver default): nothing is wrapped,
        // cache_owner is empty, and the store logs each put immediately (2 entries
        // for two same-key writes — no dedup).
        let built = counting_topology(true);
        let mut g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            ByteSize::ZERO,
        ))
        .unwrap();
        check!(g.cache_owner.is_empty());
        check!(!g.stores.kv_is_cached("counts"));

        pollster::block_on(g.pipe("in", None, b"x", 0)).unwrap();
        pollster::block_on(g.pipe("in", None, b"x", 1)).unwrap();
        // Uncached: each put logs immediately → two changelog entries (no dedup).
        let cl = g.drain_changelogs(&std::collections::HashSet::new());
        check!(cl.len() == 2);
    }

    #[test]
    fn instantiate_does_not_cache_when_marking_opted_out() {
        // with_caching(false) equivalent (store NOT marked) + budget > 0: the store
        // is NOT in cache_owner and stays uncached.
        let built = counting_topology(false);
        let g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            kibibytes(1),
        ))
        .unwrap();
        check!(g.cache_owner.is_empty());
        check!(!g.stores.kv_is_cached("counts"));
    }

    #[test]
    fn add_versioned_store_registers_wire_spec() {
        use crate::processor::serde::{I64Serde, StringSerde};
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let proc = t.add_processor("proc", || Upper, [&src]);
        t.add_versioned_store::<String, i64, _, _>(
            "vstore",
            StringSerde,
            I64Serde,
            minutes(10),
            [proc.name()],
        );
        t.add_sink("out", "out-topic", [&proc]);
        let wire = t.build("app").unwrap().to_wire();
        let blob = serde_json::to_value(&wire).unwrap().to_string();
        check!(blob.contains("vstore"), "changelog topic name not in wire");
        check!(
            blob.contains("min.compaction.lag.ms"),
            "min.compaction.lag.ms key not in wire"
        );
        // history_retention = 10m -> min.compaction.lag.ms = 600_000 + 86_400_000 = 87_000_000
        check!(blob.contains("87000000"), "lag value not in wire");
    }

    #[tokio::test]
    async fn add_window_store_uses_window_size_not_retention_for_cached_key_end() {
        // Regression: a CACHED sliding aggregate registers its window store with a
        // retention basis of `2 * timeDifferenceMs` but a TRUE window size of
        // `1 * timeDifferenceMs`. The flushed `Windowed` key end must be
        // `start + windowSize`, NOT `start + retentionBasis`. Before the fix the
        // factory fed the retention basis to `WindowBytesStore::new`,
        // so the end was doubled (this asserts `end == 10`, which would be `20`).
        use std::sync::{Arc, Mutex};

        use crate::{
            dsl::{
                processors::change::Change,
                windows::{Window, Windowed},
            },
            processor::{record::RecordContext, serde::I64Serde},
            store::{
                api::StateStore,
                byte::InMemoryBytes,
                cache::named::NamedCache,
                window::{WindowBytesStore, WindowStore},
            },
        };

        const D: Time = millis(10);
        let mut t = Topology::new();
        // The retention basis is 2*D; the true window size (the key end) is D.
        t.add_window_store::<String, i64, _, _>(
            "sw",
            StringSerde,
            I64Serde,
            (D * 2.0, D, Time::ZERO),
            ["p"],
        );

        // Pull the registered factory and instantiate the store over a fresh backend.
        let (_changelog, factory) = t.store_factories.get("sw").expect("factory registered");
        let backend: Box<dyn crate::store::byte::ByteKeyValueStore> =
            Box::new(InMemoryBytes::default());
        let mut store: Box<dyn crate::store::api::StateStore> =
            factory("sw", "sw-changelog".to_string(), backend);

        // Enable the record cache and stage a put for a window starting at 0.
        let typed = store
            .as_any_mut()
            .downcast_mut::<WindowBytesStore<String, i64>>()
            .expect("window store downcast");
        typed.enable_cache(Arc::new(Mutex::new(NamedCache::new("sw".into()))));
        typed.set_record_context(RecordContext {
            topic: "t".into(),
            partition: 0,
            offset: 0,
            timestamp: 7,
        });
        typed.put("a".into(), 0, 1, 7).await;

        let mut buffer = std::collections::VecDeque::new();
        store.flush_cache_into(&mut buffer, &[0]).await;
        assert_eq!(buffer.len(), 1);
        let (_child, rec) = &buffer[0];
        let key = rec
            .key
            .as_ref()
            .unwrap()
            .downcast_ref::<Windowed<String>>()
            .unwrap();
        assert_eq!(key.key, "a");
        // end == start + window_size (D), NOT start + retention basis (2*D).
        check!(
            key.window
                == Window {
                    start: 0,
                    end: D.millis_i64()
                }
        );
        let change = rec.value.downcast_ref::<Change<i64>>().unwrap();
        assert_eq!(change.new, Some(1));
    }
}
