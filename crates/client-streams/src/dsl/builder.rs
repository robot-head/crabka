//! `StreamsBuilder` (public) + `InternalStreamsBuilder` (graph + name counter).
use std::cell::RefCell;
use std::rc::Rc;

use crate::dsl::graph::{GraphNodeKind, LogicalGraph};
use crate::processor::serde::{Consumed, Serde};

/// A serde-carrying thunk that registers + connects a DSL-added state store to a
/// processor by name during lowering. Looked up and invoked by `process` and
/// `process_values`.
pub(crate) type StoreConnectThunk =
    std::sync::Arc<dyn Fn(&mut crate::dsl::graph::LowerState, &str) + Send + Sync>;

pub(crate) struct InternalStreamsBuilder {
    pub graph: LogicalGraph,
    index: usize,
    /// Serde-carrying connect thunks for DSL-added stores, keyed by store name.
    /// Populated by [`StreamsBuilder::add_state_store`]; looked up + invoked by a
    /// `process`/`process_values` node during lowering.
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

    /// JVM `InternalStreamsBuilder.newProcessorName`: `prefix + %010d` then ++.
    pub fn new_processor_name(&mut self, prefix: &str) -> String {
        let n = format!("{prefix}{:010}", self.index);
        self.index += 1;
        n
    }

    /// The connect thunk for an added store, if any (cloned `Arc`). Used by `process`.
    pub fn store_thunk(&self, name: &str) -> Option<StoreConnectThunk> {
        self.store_thunks.get(name).cloned()
    }
}

/// The DSL entry point. Build a topology, then `build`/`build_optimized`.
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

    /// Source a `KStream` from one or more topics.
    pub fn stream<K, V, KS, VS>(
        &self,
        topics: impl IntoIterator<Item = impl Into<String>>,
        consumed: Consumed<KS, VS>,
    ) -> crate::dsl::kstream::KStream<K, V>
    where
        K: std::any::Any + Send + Clone,
        V: std::any::Any + Send + Clone,
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
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
        g.graph.nodes[id].lower = Some(Box::new(
            move |state: &mut crate::dsl::graph::LowerState| {
                let h = state
                    .topology
                    .add_source::<K, V, KS, VS>(name, topics, consumed);
                state.handle_name.insert(id, h.name().to_string());
            },
        ));
        drop(g);
        crate::dsl::kstream::KStream::new(Rc::clone(&self.internal), id)
            .with_source_topic(single_source_topic)
    }

    /// Source a materialized `KTable` from a changelog-style topic.
    ///
    /// Records a single `TableSource` logical node whose thunk lowers a source
    /// node, the table-source processor, and the materialized state store. The
    /// store name is taken from `Materialized` (else a fresh
    /// `KTABLE-SOURCE-STATE-STORE` counter); the changelog topic is
    /// `<app>-<store>-changelog`, unless the `REUSE_KTABLE_SOURCE_TOPICS`
    /// optimizer pass (run by `build_optimized`) makes it reuse the source topic.
    pub fn table<K, V, KS, VS>(
        &self,
        topic: impl Into<String>,
        consumed: Consumed<KS, VS>,
        materialized: crate::dsl::config::Materialized<KS, VS>,
    ) -> crate::dsl::ktable::KTable<K, V>
    where
        K: std::any::Any + Send + Sync + Clone,
        V: std::any::Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        let topic: String = topic.into();
        // Preserve a copy of the source topic to surface via `KTable::source_topic()`.
        let topic_for_ktable = topic.clone();
        // Factory letting a downstream `suppress` register a SuppressBytesStore<K,V>
        // with this table's serdes (non-windowed). Built before the thunk moves them.
        let suppress_factory = crate::dsl::ktable::kv_suppress_factory::<K, V, KS, VS>(
            materialized.key_serde.clone(),
            materialized.value_serde.clone(),
        );
        // Capture the table's key/value serdes for the FK-join DSL (which needs the
        // left key/value + right value serdes to (de)serialize the FK wrappers).
        let key_serde_arc: std::sync::Arc<dyn Serde<K>> =
            std::sync::Arc::new(materialized.key_serde.clone());
        let value_serde_arc: std::sync::Arc<dyn Serde<V>> =
            std::sync::Arc::new(materialized.value_serde.clone());
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
        let crate::dsl::config::Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(
            move |state: &mut crate::dsl::graph::LowerState| {
                let src = state
                    .topology
                    .add_source::<K, V, KS, VS>(source_name, [topic], consumed);
                let store_for_proc = store_for_thunk.clone();
                // The KTable source forwards Change<V> (prior store value as old).
                let h = state
                    .topology
                    .add_processor::<K, V, K, crate::dsl::processors::change::Change<V>, _, _, _>(
                        proc_name,
                        move || crate::dsl::processors::table::KTableSourceProcessor {
                            store_name: store_for_proc.clone(),
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
                            .add_state_store_with_changelog::<K, V, KS, VS>(
                                store_for_thunk.clone(),
                                key_serde,
                                value_serde,
                                [h.name().to_string()],
                                changelog_topic,
                            );
                    }
                    None => {
                        state.topology.add_state_store::<K, V, KS, VS>(
                            store_for_thunk.clone(),
                            key_serde,
                            value_serde,
                            [h.name().to_string()],
                        );
                    }
                }
                // Children of the TableSource wire to the processor output.
                state.handle_name.insert(id, h.name().to_string());
            },
        ));
        drop(g);
        crate::dsl::ktable::KTable::new(
            Rc::clone(&self.internal),
            id,
            Some(store_name),
            Some(topic_for_ktable),
        )
        .with_suppress_factory(Some(suppress_factory))
        .with_serdes(key_serde_arc, value_serde_arc)
    }

    /// Source a [`GlobalKTable`] from a topic: a fully-replicated lookup table,
    /// usable only as a join target.
    ///
    /// Records a single `GlobalSource` logical node whose thunk lowers (via
    /// [`Topology::add_global_store`]) a source + update-processor + a global KV
    /// store. The store/source/processor are **invisible in the wire** (no
    /// subtopology, no changelog), but the global source node still consumes a
    /// node-group index during grouping — so declaring `global_table` before
    /// `stream` shifts the stream subtopology id (e.g. to `"1"`). The store name
    /// is taken from `materialized` (else a fresh `KTABLE-SOURCE-STATE-STORE`
    /// counter), minted at the JVM position (before the source/processor names).
    ///
    /// [`GlobalKTable`]: crate::dsl::global_table::GlobalKTable
    /// [`Topology::add_global_store`]: crate::topology::Topology::add_global_store
    pub fn global_table<K, V, KS, VS>(
        &self,
        topic: impl Into<String>,
        consumed: Consumed<KS, VS>,
        materialized: crate::dsl::config::Materialized<KS, VS>,
    ) -> crate::dsl::global_table::GlobalKTable<K, V>
    where
        K: std::any::Any + Send + Sync + Clone,
        V: std::any::Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
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
        drop(materialized);
        let store_for_handle = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(
            move |state: &mut crate::dsl::graph::LowerState| {
                state.topology.add_global_store::<K, V, KS, VS>(
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
        )
    }

    /// Register a state store the DSL can connect to a `process`/`process_values`
    /// node by name. The store is registered + its (compact) changelog emitted when
    /// a `process` call connects it. Call this BEFORE the `process` that names the
    /// store.
    ///
    /// The serdes are captured into a connect thunk that, during lowering, invokes
    /// [`Topology::add_state_store`] with the named processor as the store's
    /// connected processor — yielding the standard `<app>-<name>-changelog` compact
    /// changelog. The thunk is recorded under `name` and looked up by `process`.
    ///
    /// [`Topology::add_state_store`]: crate::topology::Topology::add_state_store
    pub fn add_state_store<K, V, KS, VS>(
        &self,
        name: impl Into<String>,
        key_serde: KS,
        value_serde: VS,
    ) -> &Self
    where
        K: std::any::Any + Send + Sync + Clone,
        V: std::any::Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        let name: String = name.into();
        // `name` is moved into the thunk; keep a copy for the map key.
        let key = name.clone();
        let thunk: StoreConnectThunk = std::sync::Arc::new(
            move |state: &mut crate::dsl::graph::LowerState, processor: &str| {
                state.topology.add_state_store::<K, V, KS, VS>(
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

    /// Build the topology with no optimizer (the JVM `NO_OPTIMIZATION` default):
    /// lower the logical graph straight to the Processor-API [`crate::topology::Topology`], then
    /// finalize it into a [`BuiltTopology`].
    ///
    /// Consumes the builder. This requires that no [`KStream`]/[`KTable`] handles
    /// are still alive — each holds an `Rc` clone of the internal builder, so an
    /// outstanding handle makes `Rc::try_unwrap` fail (→ panic). The fluent
    /// `stream(..).map_values(..)..to(..)` form drops every intermediate handle
    /// before `build`, satisfying this.
    ///
    /// [`KStream`]: crate::dsl::kstream::KStream
    /// [`KTable`]: crate::dsl::ktable::KTable
    /// [`BuiltTopology`]: crate::topology::BuiltTopology
    pub fn build(
        self,
        app_id: &str,
    ) -> Result<crate::topology::BuiltTopology, crate::topology::TopologyError> {
        let graph = self.into_graph("build");
        let topology = crate::dsl::lower::lower(graph, app_id);
        topology.build(app_id)
    }

    /// Build the topology with DSL optimizations enabled (JVM `optimization=all`):
    /// run the optimizer passes over the logical graph, then lower to the
    /// Processor-API [`crate::topology::Topology`] and finalize.
    ///
    /// The passes are `MERGE_REPARTITION_TOPICS` (two aggregations off one
    /// key-changing op share a single repartition topic) and
    /// `REUSE_KTABLE_SOURCE_TOPICS` (a `builder.table()` store reuses its source
    /// topic as its changelog). They're independent, so order doesn't matter.
    /// Same outstanding-handle requirement as [`build`](Self::build).
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

    /// Unwrap the shared internal builder into its [`LogicalGraph`]. Requires that
    /// no [`KStream`]/[`KTable`] handles are still alive (each holds an `Rc` clone
    /// of the internal builder); an outstanding handle makes `Rc::try_unwrap`
    /// fail → panic.
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
    use super::*;
    use crate::processor::serde::StringSerde;
    use assert2::check;

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
        let _s = builder.stream(
            ["in"],
            crate::processor::serde::Consumed::with(StringSerde, StringSerde),
        );
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
        use crate::processor::serde::{Consumed, Produced};
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .to("out", Produced::with(StringSerde, StringSerde));
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
        use crate::processor::serde::{Consumed, Produced};
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .map_values(|v: &String| v.clone())
            .to("out", Produced::with(StringSerde, StringSerde));
        let wire = b.build_optimized("app").unwrap().to_wire();
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
    }

    #[test]
    fn table_default_build_keeps_derived_changelog_name() {
        // Without the optimizer (plain `build`), a `table()` store's changelog is
        // the JVM-default `<app>-<store>-changelog` — REUSE_KTABLE_SOURCE_TOPICS
        // must NOT fire.
        let b = StreamsBuilder::new();
        b.table::<String, String, _, _>(
            "in",
            Consumed::with(StringSerde, StringSerde),
            crate::dsl::config::Materialized::with(StringSerde, StringSerde).as_store("store"),
        )
        .to_stream()
        .to(
            "out",
            crate::processor::serde::Produced::with(StringSerde, StringSerde),
        );
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
        b.table::<String, String, _, _>(
            "in",
            Consumed::with(StringSerde, StringSerde),
            crate::dsl::config::Materialized::with(StringSerde, StringSerde).as_store("store"),
        )
        .to_stream()
        .to(
            "out",
            crate::processor::serde::Produced::with(StringSerde, StringSerde),
        );
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
        let gt = builder.global_table::<String, String, _, _>(
            "global",
            crate::processor::serde::Consumed::with(StringSerde, StringSerde),
            crate::dsl::config::Materialized::with(StringSerde, StringSerde).as_store("g-store"),
        );
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
        use crate::processor::serde::{Consumed, Produced};
        let b = StreamsBuilder::new();
        // Declared FIRST: the global source is registered before the stream source,
        // so it consumes node-group index 0 and the stream emits as "1".
        let gt = b.global_table::<String, String, _, _>(
            "global",
            Consumed::with(StringSerde, StringSerde),
            crate::dsl::config::Materialized::with(StringSerde, StringSerde).as_store("g-store"),
        );
        // The GlobalKTable handle holds an `Rc` clone of the internal builder; drop
        // it before `build()` (which requires `Rc::try_unwrap` to succeed).
        drop(gt);
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .to("out", Produced::with(StringSerde, StringSerde));
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
        use crate::processor::serde::I64Serde;
        let b = StreamsBuilder::new();
        // Chains (returns &Self) and records a thunk under the given name.
        b.add_state_store::<String, i64, _, _>("counts", StringSerde, I64Serde);
        check!(b.internal.borrow().store_thunk("counts").is_some());
        check!(b.internal.borrow().store_thunk("missing").is_none());
    }

    #[test]
    #[should_panic(expected = "outstanding KStream/KTable handles")]
    fn build_panics_with_outstanding_handle() {
        let b = StreamsBuilder::new();
        // Hold a live KStream handle across the build call: it keeps an `Rc`
        // clone of the internal builder alive, so `Rc::try_unwrap` fails.
        let _held = b.stream(
            ["in"],
            crate::processor::serde::Consumed::with(StringSerde, StringSerde),
        );
        let _ = b.build("app");
    }
}
