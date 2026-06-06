//! `StreamsBuilder` (public) + `InternalStreamsBuilder` (graph + name counter).
use std::cell::RefCell;
use std::rc::Rc;

use crate::dsl::graph::{GraphNodeKind, LogicalGraph};
use crate::processor::serde::{Consumed, Serde};

pub(crate) struct InternalStreamsBuilder {
    pub graph: LogicalGraph,
    index: usize,
}

impl InternalStreamsBuilder {
    pub fn new() -> Self {
        Self {
            graph: LogicalGraph::default(),
            index: 0,
        }
    }

    /// JVM `InternalStreamsBuilder.newProcessorName`: `prefix + %010d` then ++.
    pub fn new_processor_name(&mut self, prefix: &str) -> String {
        let n = format!("{prefix}{:010}", self.index);
        self.index += 1;
        n
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
        // Attach the lowering thunk: when the lowering driver (Task 5) runs it,
        // it performs the typed `Topology::add_source` call (capturing the source
        // `Consumed` serdes + topics) and records the resulting node name so
        // children can rebuild a typed parent handle.
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
    /// node + a [`KTableSourceProcessor`] + the materialized state store. The
    /// store name is taken from `Materialized` (else a fresh
    /// `KTABLE-SOURCE-STATE-STORE` counter); the changelog topic is
    /// `<app>-<store>-changelog`, unless the `REUSE_KTABLE_SOURCE_TOPICS`
    /// optimizer pass (run by `build_optimized`) makes it reuse the source topic.
    ///
    /// [`KTableSourceProcessor`]: crate::dsl::processors::table::KTableSourceProcessor
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
    }

    /// Build the topology with no optimizer (the JVM `NO_OPTIMIZATION` default):
    /// lower the logical graph straight to the Processor-API [`Topology`], then
    /// finalize it into a [`BuiltTopology`].
    ///
    /// Consumes the builder. This requires that no [`KStream`]/[`KTable`] handles
    /// are still alive — each holds an `Rc` clone of the internal builder, so an
    /// outstanding handle makes `Rc::try_unwrap` fail (→ panic). The fluent
    /// `stream(..).map_values(..)..to(..)` form drops every intermediate handle
    /// before `build`, satisfying this.
    ///
    /// [`KStream`]: crate::dsl::kstream::KStream
    /// [`KTable`]: crate::dsl::kstream::KStream
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
    /// Processor-API [`Topology`] and finalize.
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
