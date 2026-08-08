//! The public `StreamsBuilder` and the `InternalStreamsBuilder` that holds the
//! graph and the name counter.
use std::{cell::RefCell, rc::Rc};

use crate::{
    dsl::graph::{GraphNodeKind, LogicalGraph},
    processor::serde::{Consumed, DefaultSerde, Serde, SerdeAssociate},
};

/// A serde-carrying thunk that registers a DSL-added state store and connects it
/// to a processor by name during lowering.
///
/// `process` and `process_values` look up this thunk and call it.
pub(crate) type StoreConnectThunk =
    std::sync::Arc<dyn Fn(&mut crate::dsl::graph::LowerState, &str) + Send + Sync>;

pub(crate) struct InternalStreamsBuilder {
    pub graph: LogicalGraph,
    index: usize,
    /// Serde-carrying connect thunks for DSL-added stores, keyed by store name.
    /// [`StreamsBuilder::add_state_store`] fills this map. A `process` or
    /// `process_values` node looks up a thunk and calls it during lowering.
    pub store_thunks: std::collections::HashMap<String, StoreConnectThunk>,
}

impl InternalStreamsBuilder {
    pub fn new() -> Self {
        Self {
            graph: LogicalGraph::default(),
            index: 0,
            store_thunks: std::collections::HashMap::new(),
        }
    }

    /// JVM `InternalStreamsBuilder.newProcessorName`. It returns
    /// `prefix + %010d` and then increments the counter.
    pub fn new_processor_name(&mut self, prefix: &str) -> String {
        let n = format!("{prefix}{:010}", self.index);
        self.index += 1;
        n
    }

    /// The connect thunk for an added store, if one exists, as a cloned `Arc`.
    /// `process` uses it.
    pub fn store_thunk(&self, name: &str) -> Option<StoreConnectThunk> {
        self.store_thunks.get(name).cloned()
    }
}

/// The DSL entry point. Build a topology, then call `build` or
/// `build_optimized`.
pub struct StreamsBuilder {
    pub(crate) internal: Rc<RefCell<InternalStreamsBuilder>>,
}

impl StreamsBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            internal: Rc::new(RefCell::new(InternalStreamsBuilder::new())),
        }
    }

    /// Source a `KStream` from one or more topics with explicit serdes.
    ///
    /// Prefer [`StreamsBuilder::stream`], the default-serde form, for types that
    /// implement [`DefaultSerde`]. Use this escape hatch when a type has no
    /// default serde, or to override it. Examples are a hand-rolled `Serde<T>`,
    /// a **key**-role schema serde (`AvroSerde::<T>::key(&cache)`), and a
    /// validation-on JSON serde (`JsonSerde::value(&cache, true)`).
    pub fn stream_explicit<KS, VS>(
        &self,
        topics: impl IntoIterator<Item = impl Into<String>>,
        consumed: impl Into<Consumed<KS, VS>>,
    ) -> crate::dsl::kstream::KStream<KS::Target, VS::Target, KS, VS>
    where
        KS: SerdeAssociate + Serde<KS::Target> + Clone,
        VS: SerdeAssociate + Serde<VS::Target> + Clone,
        KS::Target: std::any::Any + Send + Clone,
        VS::Target: std::any::Any + Send + Clone,
    {
        let consumed = consumed.into();
        let topics: Vec<String> = topics.into_iter().map(Into::into).collect();
        // A stream sourced from exactly one topic carries that topic as its
        // copartition-group member lineage (used by `KStream::join`). A multi-topic
        // source has no single member, so the lineage is `None`.
        let single_source_topic = match topics.as_slice() {
            [only] => Some(only.clone()),
            _ => None,
        };
        let mut g = self.internal.borrow_mut();
        let name = g.new_processor_name(crate::dsl::names::SOURCE);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StreamSource {
                topics: topics.clone(),
            },
            vec![],
        );
        // Attach the lowering thunk: it performs the typed `Topology::add_source`
        // call (capturing the source `Consumed` serdes + topics) and records the
        // resulting node name so children can rebuild a typed parent handle.
        let consumed_for_lower = consumed.clone();
        g.graph.nodes[id].lower = Some(Box::new(
            move |state: &mut crate::dsl::graph::LowerState| {
                let h = state
                    .topology
                    .add_source_explicit::<KS::Target, VS::Target, KS, VS>(
                        name,
                        topics,
                        consumed_for_lower,
                    );
                state.handle_name.insert(id, h.name().to_string());
            },
        ));
        drop(g);
        crate::dsl::kstream::KStream::new(
            Rc::clone(&self.internal),
            id,
            consumed.key_serde.clone(),
            consumed.value_serde.clone(),
        )
        .with_source_topic(single_source_topic)
    }

    /// Source a materialized `KTable` from a changelog-style topic with explicit
    /// serdes.
    ///
    /// Prefer [`StreamsBuilder::table`], the default-serde form, for
    /// [`DefaultSerde`] types. Use this method when a type has no default serde
    /// or to override it, for example with a custom `Serde<T>`, a key-role
    /// schema serde, or validated JSON.
    ///
    /// The method records a single `TableSource` logical node. Its thunk lowers a
    /// source node, the table-source processor, and the materialized state store.
    /// The store name comes from `Materialized`, or from a fresh
    /// `KTABLE-SOURCE-STATE-STORE` counter. The changelog topic is
    /// `<app>-<store>-changelog`, unless the `REUSE_KTABLE_SOURCE_TOPICS`
    /// optimizer pass, which `build_optimized` runs, makes it reuse the source
    /// topic.
    pub fn table_explicit<KS, VS>(
        &self,
        topic: impl Into<String>,
        consumed: impl Into<Consumed<KS, VS>>,
        materialized: impl Into<crate::dsl::config::Materialized<KS, VS>>,
    ) -> crate::dsl::ktable::KTable<KS::Target, VS::Target, KS, VS>
    where
        KS: SerdeAssociate + Serde<KS::Target> + Clone + 'static,
        VS: SerdeAssociate + Serde<VS::Target> + Clone + 'static,
        KS::Target: std::any::Any + Send + Sync + Clone,
        VS::Target: std::any::Any + Send + Clone,
    {
        let consumed = consumed.into();
        let materialized = materialized.into();
        let topic: String = topic.into();
        // Preserve a copy of the source topic to surface via `KTable::source_topic()`.
        let topic_for_ktable = topic.clone();
        // Factory letting a downstream `suppress` register a SuppressBytesStore<K,V>
        // with this table's serdes (non-windowed). Built before the thunk moves them.
        let suppress_factory =
            crate::dsl::ktable::kv_suppress_factory::<KS::Target, VS::Target, KS, VS>(
                materialized.key_serde.clone(),
                materialized.value_serde.clone(),
            );
        let mut g = self.internal.borrow_mut();
        // Store name at the JVM position (minted before the source/processor name).
        let store_name = match &materialized.store_name {
            Some(name) => name.clone(),
            None => g.new_processor_name(crate::dsl::names::TABLE_SOURCE),
        };
        let source_name = g.new_processor_name(crate::dsl::names::SOURCE);
        let proc_name = g.new_processor_name(crate::dsl::names::TABLE_SOURCE);
        let id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::TableSource {
                topic: topic.clone(),
                store_name: store_name.clone(),
                reuse_source_for_changelog: false,
            },
            vec![],
        );
        // Capture versioned config before destructuring moves the serdes.
        let versioned_cfg = materialized.versioned;
        // Surface the versioned history-retention on the returned KTable handle
        // (read by KIP-914 join routing), independent of the thunk's move.
        let versioned_retention = versioned_cfg.map(|v| v.history_retention);
        let crate::dsl::config::Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let store_for_thunk = store_name.clone();
        let key_serde_for_lower = key_serde.clone();
        let value_serde_for_lower = value_serde.clone();
        g.graph.nodes[id].lower = Some(Box::new(
            move |state: &mut crate::dsl::graph::LowerState| {
                let src = state
                    .topology
                    .add_source_explicit::<KS::Target, VS::Target, KS, VS>(
                        source_name,
                        [topic],
                        consumed,
                    );
                let store_for_proc = store_for_thunk.clone();
                if let Some(vc) = versioned_cfg {
                    // ── Versioned KTable branch (KIP-889/914) ──────────────────
                    let h = state
                        .topology
                        .add_processor::<KS::Target, VS::Target, KS::Target, crate::dsl::processors::change::Change<VS::Target>, _, _, _>(
                            proc_name,
                            move || crate::dsl::processors::table::VersionedKTableSourceProcessor {
                                store_name: store_for_proc.clone(),
                                _pd: std::marker::PhantomData,
                            },
                            [&src],
                        );
                    // REUSE_KTABLE_SOURCE_TOPICS applies to versioned tables too:
                    // under optimization the changelog is the source topic, else
                    // the default `<app>-<store>-changelog`.
                    match state.reuse_changelog.get(&id).cloned() {
                        Some(changelog_topic) => {
                            state
                                .topology
                                .add_versioned_store_with_changelog::<KS::Target, VS::Target, KS, VS>(
                                    store_for_thunk.clone(),
                                    key_serde_for_lower,
                                    value_serde_for_lower,
                                    vc.history_retention,
                                    [h.name().to_string()],
                                    changelog_topic,
                                );
                        }
                        None => {
                            state
                                .topology
                                .add_versioned_store::<KS::Target, VS::Target, KS, VS>(
                                    store_for_thunk.clone(),
                                    key_serde_for_lower,
                                    value_serde_for_lower,
                                    vc.history_retention,
                                    [h.name().to_string()],
                                );
                        }
                    }
                    state.handle_name.insert(id, h.name().to_string());
                } else {
                    // ── Standard KV KTable branch ──────────────────────────────
                    // The KTable source forwards Change<V> (prior store value as old).
                    let h = state
                        .topology
                        .add_processor::<KS::Target, VS::Target, KS::Target, crate::dsl::processors::change::Change<VS::Target>, _, _, _>(
                            proc_name,
                            move || crate::dsl::processors::table::KTableSourceProcessor {
                                store_name: store_for_proc.clone(),
                                forwarder:
                                    crate::dsl::processors::tuple_forwarder::TupleForwarder::default(
                                    ),
                                _pd: std::marker::PhantomData,
                            },
                            [&src],
                        );
                    // REUSE_KTABLE_SOURCE_TOPICS: if the optimizer flagged this
                    // TableSource, register the store with the source topic as its
                    // changelog; otherwise the default `<app>-<store>-changelog`.
                    match state.reuse_changelog.get(&id).cloned() {
                        Some(changelog_topic) => {
                            state
                                .topology
                                .add_state_store_with_changelog::<KS::Target, VS::Target, KS, VS>(
                                    store_for_thunk.clone(),
                                    key_serde_for_lower,
                                    value_serde_for_lower,
                                    [h.name().to_string()],
                                    changelog_topic,
                                );
                        }
                        None => {
                            state
                                .topology
                                .add_state_store::<KS::Target, VS::Target, KS, VS>(
                                    store_for_thunk.clone(),
                                    key_serde_for_lower,
                                    value_serde_for_lower,
                                    [h.name().to_string()],
                                );
                        }
                    }
                    // Children of the TableSource wire to the processor output.
                    state.handle_name.insert(id, h.name().to_string());
                }
            },
        ));
        drop(g);
        crate::dsl::ktable::KTable::new(
            Rc::clone(&self.internal),
            id,
            Some(store_name),
            Some(topic_for_ktable),
            key_serde.clone(),
            value_serde.clone(),
        )
        .with_suppress_factory(Some(suppress_factory))
        .with_versioned_retention(versioned_retention)
    }

    /// Source a [`GlobalKTable`] from a topic with explicit serdes. A
    /// `GlobalKTable` is a fully-replicated lookup table and works only as a
    /// join target.
    ///
    /// Prefer [`StreamsBuilder::global_table`], the default-serde form, for
    /// [`DefaultSerde`] types. Use this method when a type has no default serde
    /// or to override it, for example with a custom `Serde<T>`, a key-role
    /// schema serde, or validated JSON.
    ///
    /// The method records a single `GlobalSource` logical node. Its thunk calls
    /// [`Topology::add_global_store`] to lower a source node, an
    /// update-processor, and a global KV store. The store, the source, and the
    /// processor are **invisible in the wire**, with no subtopology and no
    /// changelog. The global source node still takes a node-group index during
    /// grouping, so a `global_table` declared before `stream` shifts the stream
    /// subtopology id, for example to `"1"`. The store name comes from
    /// `materialized`, or from a fresh `KTABLE-SOURCE-STATE-STORE` counter, and
    /// it is minted at the JVM position, before the source name and the
    /// processor name.
    ///
    /// [`GlobalKTable`]: crate::dsl::global_table::GlobalKTable
    /// [`Topology::add_global_store`]: crate::topology::Topology::add_global_store
    pub fn global_table_explicit<KS, VS>(
        &self,
        topic: impl Into<String>,
        consumed: impl Into<Consumed<KS, VS>>,
        materialized: impl Into<crate::dsl::config::Materialized<KS, VS>>,
    ) -> crate::dsl::global_table::GlobalKTable<KS::Target, VS::Target, KS, VS>
    where
        KS: SerdeAssociate + Serde<KS::Target> + Clone + 'static,
        VS: SerdeAssociate + Serde<VS::Target> + Clone + 'static,
        KS::Target: std::any::Any + Send + Sync + Clone,
        VS::Target: std::any::Any + Send + Clone,
    {
        let consumed = consumed.into();
        let materialized = materialized.into();
        let topic: String = topic.into();
        let topic_for_handle = topic.clone();
        let mut g = self.internal.borrow_mut();
        // Store name at the JVM position (minted before the source/processor name).
        let store_name = match &materialized.store_name {
            Some(name) => name.clone(),
            None => g.new_processor_name(crate::dsl::names::TABLE_SOURCE),
        };
        let source_name = g.new_processor_name(crate::dsl::names::GLOBAL_SOURCE);
        let proc_name = g.new_processor_name(crate::dsl::names::GLOBAL_PROCESSOR);
        let id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::GlobalSource {
                topic: topic.clone(),
                store_name: store_name.clone(),
                source_name: source_name.clone(),
                processor_name: proc_name.clone(),
            },
            vec![],
        );
        // `materialized` carries the store serdes; for a global table the store and
        // source serdes coincide, so `add_global_store` (which uses one `Consumed`
        // for both the source deser and the store factory) takes the source serdes.
        // Drop `materialized` to keep its API surface symmetric with `table()` even
        // though its serdes aren't separately threaded here.
        let key_serde = materialized.key_serde.clone();
        let value_serde = materialized.value_serde.clone();
        drop(materialized);
        let store_for_handle = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(
            move |state: &mut crate::dsl::graph::LowerState| {
                state
                    .topology
                    .add_global_store::<KS::Target, VS::Target, KS, VS>(
                        store_name,
                        source_name,
                        topic,
                        proc_name,
                        consumed,
                    );
            },
        ));
        drop(g);
        crate::dsl::global_table::GlobalKTable::new(
            Rc::clone(&self.internal),
            id,
            store_for_handle,
            topic_for_handle,
            key_serde,
            value_serde,
        )
    }

    /// Source a `KStream` using each type's [`DefaultSerde`]. Use
    /// [`StreamsBuilder::stream_explicit`] to supply custom serdes.
    pub fn stream<K, V>(
        &self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> crate::dsl::kstream::KStream<K, V>
    where
        K: DefaultSerde,
        V: DefaultSerde,
        K::Serde: SerdeAssociate<Target = K> + Clone,
        V::Serde: SerdeAssociate<Target = V> + Clone,
        K: std::any::Any + Send + Clone,
        V: std::any::Any + Send + Clone,
    {
        self.stream_explicit(
            topics,
            Consumed::with(K::Serde::default(), V::Serde::default()),
        )
    }

    /// Source a materialized `KTable` using each type's [`DefaultSerde`]. Use
    /// [`StreamsBuilder::table_explicit`] to supply custom serdes or a custom
    /// `Materialized`.
    pub fn table<K, V>(
        &self,
        topic: impl Into<String>,
        store_name: impl Into<String>,
    ) -> crate::dsl::ktable::KTable<K, V>
    where
        K: DefaultSerde,
        V: DefaultSerde,
        K::Serde: SerdeAssociate<Target = K> + Clone,
        V::Serde: SerdeAssociate<Target = V> + Clone,
        K: std::any::Any + Send + Sync + Clone,
        V: std::any::Any + Send + Clone,
    {
        self.table_explicit(
            topic,
            Consumed::with(K::Serde::default(), V::Serde::default()),
            crate::dsl::config::Materialized::with(K::Serde::default(), V::Serde::default())
                .as_store(store_name),
        )
    }

    /// Source a [`GlobalKTable`] using each type's [`DefaultSerde`]. Use
    /// [`StreamsBuilder::global_table_explicit`] to supply custom serdes.
    ///
    /// [`GlobalKTable`]: crate::dsl::global_table::GlobalKTable
    pub fn global_table<K, V>(
        &self,
        topic: impl Into<String>,
        store_name: impl Into<String>,
    ) -> crate::dsl::global_table::GlobalKTable<K, V>
    where
        K: DefaultSerde,
        V: DefaultSerde,
        K::Serde: SerdeAssociate<Target = K> + Clone,
        V::Serde: SerdeAssociate<Target = V> + Clone,
        K: std::any::Any + Send + Sync + Clone,
        V: std::any::Any + Send + Clone,
    {
        self.global_table_explicit(
            topic,
            Consumed::with(K::Serde::default(), V::Serde::default()),
            crate::dsl::config::Materialized::with(K::Serde::default(), V::Serde::default())
                .as_store(store_name),
        )
    }

    /// Register a state store that the DSL can connect by name to a `process` or
    /// `process_values` node.
    ///
    /// The store is registered and its compact changelog is emitted when a
    /// `process` call connects it. Call this method BEFORE the `process` that
    /// names the store.
    ///
    /// The method captures the serdes into a connect thunk. During lowering the
    /// thunk calls [`Topology::add_state_store`] with the named processor as the
    /// store's connected processor, which gives the standard
    /// `<app>-<name>-changelog` compact changelog. The thunk is recorded under
    /// `name`, and `process` looks it up.
    ///
    /// [`Topology::add_state_store`]: crate::topology::Topology::add_state_store
    pub fn add_state_store<KS, VS>(
        &self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
    ) -> &Self
    where
        KS: SerdeAssociate + Serde<KS::Target> + Clone + 'static,
        VS: SerdeAssociate + Serde<VS::Target> + Clone + 'static,
        KS::Target: std::any::Any + Send + Sync + Clone,
        VS::Target: std::any::Any + Send + Clone,
    {
        let name: String = name.into();
        // `name` is moved into the thunk; keep a copy for the map key.
        let key = name.clone();
        let thunk: StoreConnectThunk = std::sync::Arc::new(
            move |state: &mut crate::dsl::graph::LowerState, processor: &str| {
                state
                    .topology
                    .add_state_store::<KS::Target, VS::Target, KS, VS>(
                        name.clone(),
                        key_serde.clone(),
                        value_serde.clone(),
                        [processor.to_string()],
                    );
            },
        );
        self.internal.borrow_mut().store_thunks.insert(key, thunk);
        self
    }

    /// Build the topology with no optimizer, which is the JVM
    /// `NO_OPTIMIZATION` default.
    ///
    /// The method lowers the logical graph straight to the Processor-API
    /// [`crate::topology::Topology`] and then finalizes it into a
    /// [`BuiltTopology`].
    ///
    /// It consumes the builder, so no [`KStream`] or [`KTable`] handle may still
    /// be alive. Each handle holds an `Rc` clone of the internal builder, so an
    /// outstanding handle makes `Rc::try_unwrap` fail and the method panics. The
    /// fluent `stream(..).map_values(..)..to_explicit(..)` form drops every
    /// intermediate handle before `build`, which meets this requirement.
    ///
    /// [`KStream`]: crate::dsl::kstream::KStream
    /// [`KTable`]: crate::dsl::ktable::KTable
    /// [`BuiltTopology`]: crate::topology::BuiltTopology
    #[tracing::instrument(
        name = "streams.dsl.build",
        level = "info",
        skip_all,
        fields(app_id = %app_id),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn build(
        self,
        app_id: &str,
    ) -> Result<crate::topology::BuiltTopology, crate::topology::TopologyError> {
        let graph = self.into_graph("build");
        let topology = crate::dsl::lower::lower(graph, app_id);
        topology.build(app_id)
    }

    /// Build the topology with the DSL optimizations on, which matches the JVM
    /// `optimization=all`.
    ///
    /// The method runs the optimizer passes over the logical graph, then lowers
    /// the graph to the Processor-API [`crate::topology::Topology`] and
    /// finalizes it.
    ///
    /// There are two passes. `MERGE_REPARTITION_TOPICS` makes two aggregations
    /// off one key-changing op share a single repartition topic.
    /// `REUSE_KTABLE_SOURCE_TOPICS` makes a `builder.table_explicit()` store
    /// reuse its source topic as its changelog. The passes are independent, so
    /// their order does not matter. The outstanding-handle requirement is the
    /// same as for [`build`](Self::build).
    #[tracing::instrument(
        name = "streams.dsl.build_optimized",
        level = "info",
        skip_all,
        fields(app_id = %app_id),
        err,
    )]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub fn build_optimized(
        self,
        app_id: &str,
    ) -> Result<crate::topology::BuiltTopology, crate::topology::TopologyError> {
        let mut graph = self.into_graph("build_optimized");
        crate::dsl::optimizer::merge_repartition_topics(&mut graph);
        crate::dsl::optimizer::reuse_ktable_source_topics(&mut graph);
        let topology = crate::dsl::lower::lower(graph, app_id);
        topology.build(app_id)
    }

    /// Unwrap the shared internal builder into its [`LogicalGraph`].
    ///
    /// No [`KStream`] or [`KTable`] handle may still be alive, because each one
    /// holds an `Rc` clone of the internal builder. An outstanding handle makes
    /// `Rc::try_unwrap` fail, and the method panics.
    ///
    /// [`KStream`]: crate::dsl::kstream::KStream
    /// [`KTable`]: crate::dsl::ktable::KTable
    /// [`LogicalGraph`]: crate::dsl::graph::LogicalGraph
    fn into_graph(self, who: &str) -> LogicalGraph {
        Rc::try_unwrap(self.internal)
            .unwrap_or_else(|_| panic!("StreamsBuilder::{who}: outstanding KStream/KTable handles"))
            .into_inner()
            .graph
    }
}

impl Default for StreamsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn counter_assigns_jvm_names_in_call_order() {
        let mut b = InternalStreamsBuilder::new();
        check!(b.new_processor_name(crate::dsl::names::SOURCE) == "KSTREAM-SOURCE-0000000000");
        check!(
            b.new_processor_name(crate::dsl::names::MAPVALUES) == "KSTREAM-MAPVALUES-0000000001"
        );
        check!(b.new_processor_name(crate::dsl::names::FILTER) == "KSTREAM-FILTER-0000000002");
    }

    #[test]
    fn stream_records_a_source_node() {
        let builder = StreamsBuilder::new();
        let _s = builder.stream::<String, String>(["in"]);
        let g = builder.internal.borrow();
        check!(g.graph.nodes.len() == 1);
        check!(matches!(
            g.graph.nodes[0].kind,
            GraphNodeKind::StreamSource { .. }
        ));
        check!(g.graph.nodes[0].name == "KSTREAM-SOURCE-0000000000");
    }

    #[test]
    fn build_lowers_source_sink_to_wire_topology() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"]).to("out");
        let built = b.build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.epoch == 0);
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        // The sink topic surfaces as a list-able output topic.
        check!(built.list_sink_topics() == vec!["out".to_string()]);
    }

    #[test]
    fn build_optimized_matches_build_for_stateless_chain() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .map_values(|v: &String| v.clone())
            .to("out");
        let wire = b.build_optimized("app").unwrap().to_wire();
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
    }

    #[test]
    fn table_build_keeps_derived_changelog_name() {
        // Without the optimizer (plain `build`), a `table()` store's changelog is
        // the JVM-default `<app>-<store>-changelog` — REUSE_KTABLE_SOURCE_TOPICS
        // must NOT fire.
        let b = StreamsBuilder::new();
        b.table::<String, String>("in", "store")
            .to_stream()
            .to("out");
        let wire = b.build("app").unwrap().to_wire();
        let cl: Vec<&str> = wire.subtopologies[0]
            .state_changelog_topics
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        check!(cl == vec!["app-store-changelog"]);
    }

    #[test]
    fn table_optimized_build_reuses_source_topic_as_changelog() {
        // With the optimizer (`build_optimized`), the `table()` store's changelog
        // is the SOURCE topic ("in"), not "app-store-changelog".
        let b = StreamsBuilder::new();
        b.table::<String, String>("in", "store")
            .to_stream()
            .to("out");
        let wire = b.build_optimized("app").unwrap().to_wire();
        let cl: Vec<&str> = wire.subtopologies[0]
            .state_changelog_topics
            .iter()
            .map(|t| t.name.as_str())
            .collect();
        check!(cl == vec!["in"]);
    }

    #[test]
    fn global_table_returns_handle_with_store_name() {
        let builder = StreamsBuilder::new();
        let gt = builder.global_table::<String, String>("global", "g-store");
        check!(gt.store_name() == "g-store");
        check!(gt.source_topic == "global");
        // The global source is the FIRST logical node (so it takes index 0 at
        // grouping time, bumping a later stream's subtopology id).
        let g = builder.internal.borrow();
        check!(matches!(
            g.graph.nodes[0].kind,
            GraphNodeKind::GlobalSource { .. }
        ));
    }

    #[test]
    fn global_table_before_stream_bumps_stream_subtopology_to_one() {
        let b = StreamsBuilder::new();
        // Declared FIRST: the global source is registered before the stream source,
        // so it consumes node-group index 0 and the stream emits as "1".
        let gt = b.global_table::<String, String>("global", "g-store");
        // The GlobalKTable handle holds an `Rc` clone of the internal builder; drop
        // it before `build()` (which requires `Rc::try_unwrap` to succeed).
        drop(gt);
        b.stream::<String, String>(["in"]).to("out");
        let wire = b.build("app").unwrap().to_wire();
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].subtopology_id == "1");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        // No changelog topic for the global store.
        check!(
            wire.subtopologies
                .iter()
                .all(|s| s.state_changelog_topics.is_empty())
        );
    }

    #[test]
    fn add_state_store_records_a_connect_thunk() {
        use crate::processor::serde::{I64Serde, StringSerde};
        let b = StreamsBuilder::new();
        // Chains (returns &Self) and records a thunk under the given name.
        b.add_state_store("counts", StringSerde, I64Serde);
        check!(b.internal.borrow().store_thunk("counts").is_some());
        check!(b.internal.borrow().store_thunk("missing").is_none());
    }

    #[test]
    #[should_panic(expected = "outstanding KStream/KTable handles")]
    fn build_panics_with_outstanding_handle() {
        let b = StreamsBuilder::new();
        // Hold a live KStream handle across the build call: it keeps an `Rc`
        // clone of the internal builder alive, so `Rc::try_unwrap` fails.
        let _held = b.stream::<String, String>(["in"]);
        let _ = b.build("app");
    }
}
