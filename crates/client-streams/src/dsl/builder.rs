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
        let graph = Rc::try_unwrap(self.internal)
            .unwrap_or_else(|_| panic!("StreamsBuilder::build: outstanding KStream/KTable handles"))
            .into_inner()
            .graph;
        let topology = crate::dsl::lower::lower(graph, app_id);
        topology.build(app_id)
    }

    /// Build the topology with DSL optimizations enabled (JVM `optimization=all`).
    ///
    /// Placeholder: today it lowers identically to [`build`](Self::build). The
    /// optimizer passes (merge repartition topics, reuse `KTable` source topics)
    /// are added in later tasks.
    pub fn build_optimized(
        self,
        app_id: &str,
    ) -> Result<crate::topology::BuiltTopology, crate::topology::TopologyError> {
        self.build(app_id)
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
