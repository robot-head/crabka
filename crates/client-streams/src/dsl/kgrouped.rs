//! `KGroupedStream<K,V>`: the intermediate handle between `groupByKey`/`groupBy`
//! and a terminal aggregation (`count`/`reduce`/`aggregate`).
//!
//! `groupByKey`/`groupBy` record **no** graph node — they capture lineage state
//! (was the upstream key-changing?), the optional `Grouped` name, the value
//! serdes (for the repartition topic), and a typed repartition-lowering thunk.
//! The terminal aggregation then:
//!
//! 1. Mints the **store name** (from `Materialized` or a fresh
//!    `KSTREAM-AGGREGATE-STATE-STORE-`/`KSTREAM-REDUCE-STATE-STORE-` counter) at
//!    the JVM counter position.
//! 2. If the upstream is key-changing, records a `Repartition` node whose thunk
//!    lowers `sink → add_repartition_topic → source` so records are re-grouped by
//!    key before aggregation (splitting the topology into 2 subtopologies).
//! 3. Records an `Aggregate` node whose thunk adds the
//!    [`KStreamAggregateProcessor`] + its state store.
//!
//! Byte-exact repartition/changelog topic names + the store-name counter index
//! are validated against the JVM `count` fixture in Task 8.

use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::Materialized;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::aggregate::{KStreamAggregateProcessor, KStreamReduceProcessor};
use crate::processor::serde::Serde;
use crate::topology::NodeHandle;

/// A typed thunk that wires a repartition `sink → topic → source` triple into
/// the Processor-API topology, returning the source node's name. Built from the
/// `Grouped` serdes so the (de)serialization round-trip through the repartition
/// topic is type-correct; erased here so `KGroupedStream<K,V>` stays free of the
/// serde type parameters.
///
/// Args: `(state, parent_name, sink_name, source_name, topic)`.
pub(crate) type RepartitionLowerFn =
    Box<dyn FnOnce(&mut LowerState, String, String, String, String) + Send>;

/// Build a [`RepartitionLowerFn`] capturing the `Grouped` key/value serdes.
pub(crate) fn repartition_lower<K, V, KS, VS>(key_serde: KS, value_serde: VS) -> RepartitionLowerFn
where
    K: Any + Send + Clone,
    V: Any + Send + Clone,
    KS: Serde<K> + Clone + 'static,
    VS: Serde<V> + Clone + 'static,
{
    Box::new(
        move |state: &mut LowerState,
              parent_name: String,
              sink_name: String,
              source_name: String,
              topic: String| {
            let parent = NodeHandle::<K, V>::from_name(parent_name);
            // sink: parent → repartition topic
            state.topology.add_sink::<K, V, KS, VS, _, _>(
                sink_name,
                topic.clone(),
                [parent],
                crate::processor::serde::Produced::with(key_serde.clone(), value_serde.clone()),
            );
            // mark the topic internal so the test driver / wire treat it as a
            // repartition topic (loop-back source)
            state.topology.add_repartition_topic(topic.clone());
            // source: repartition topic → new source node
            state.topology.add_source::<K, V, KS, VS>(
                source_name,
                [topic],
                crate::processor::serde::Consumed::with(key_serde, value_serde),
            );
        },
    )
}

/// Handle produced by `groupByKey`/`groupBy`; terminal aggregations consume it.
pub struct KGroupedStream<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    /// Logical id of the node feeding the aggregation (the source/select-key).
    parent: NodeId,
    /// True when the upstream key was rewritten without a re-group → the
    /// aggregation must insert a repartition before the aggregate node.
    key_changing_upstream: bool,
    /// Explicit `Grouped` name (drives repartition topic naming in Task 8).
    #[allow(dead_code)]
    grouped_name: Option<String>,
    /// Typed repartition-lowering thunk (taken once by the terminal op).
    repartition_lower: Option<RepartitionLowerFn>,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> KGroupedStream<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        parent: NodeId,
        key_changing_upstream: bool,
        grouped_name: Option<String>,
        repartition_lower: RepartitionLowerFn,
    ) -> Self {
        Self {
            builder,
            parent,
            key_changing_upstream,
            grouped_name,
            repartition_lower: Some(repartition_lower),
            _pd: PhantomData,
        }
    }

    /// `count`: count records per key into a materialized `KTable<K, i64>`.
    ///
    /// `init = || 0`, `agg = |_k, _v, acc| acc + 1`.
    pub fn count<KS, VS>(self, materialized: Materialized<KS, VS>) -> KTable<K, i64>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        self.aggregate_inner(
            materialized,
            names::AGGREGATE_STORE,
            || 0i64,
            |_k: &K, _v: &V, acc: i64| acc + 1,
        )
    }

    /// `reduce`: combine values per key with `reducer`, materialized as
    /// `KTable<K, V>`. The first value for a key seeds the accumulator (the JVM
    /// `Reducer` has no separate `init`); later values fold via
    /// `reducer(&acc, &value)`. Backed by [`KStreamReduceProcessor`], which keeps
    /// the public value type `V` (no `Option`/sentinel leaks into the `KTable`).
    pub fn reduce<KS, VS, R>(self, reducer: R, materialized: Materialized<KS, VS>) -> KTable<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, names::REDUCE_STORE);
        self.lower_reduce::<KS, VS, R>(materialized, store_name, reducer)
    }

    /// `aggregate`: general aggregation with caller-supplied `init` + `agg`,
    /// materialized as `KTable<K, VA>`.
    pub fn aggregate<KS, VS, VA, I, A>(
        self,
        init: I,
        agg: A,
        materialized: Materialized<KS, VS>,
    ) -> KTable<K, VA>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        self.aggregate_inner(materialized, names::AGGREGATE_STORE, init, agg)
    }

    /// `windowedBy(TimeWindows)`: switch to a windowed aggregation. Moves the
    /// grouped lineage (parent, key-changing flag, repartition thunk) into a
    /// [`TimeWindowedKGroupedStream`], which exposes windowed
    /// `count`/`reduce`/`aggregate` producing `KTable<Windowed<K>, _>`.
    #[must_use]
    pub fn windowed_by(
        mut self,
        windows: crate::dsl::windows::TimeWindows,
    ) -> crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream<K, V> {
        crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream::new(
            Rc::clone(&self.builder),
            self.parent,
            self.key_changing_upstream,
            self.grouped_name.take(),
            self.repartition_lower.take(),
            windows,
        )
    }

    /// `windowedBy(SessionWindows)`: switch to a session aggregation. Moves the
    /// grouped lineage into a [`SessionWindowedKGroupedStream`], which exposes
    /// session `count`/`reduce`/`aggregate` producing `KTable<Windowed<K>, _>`.
    /// (Distinct method name because Rust cannot overload `windowed_by` by the
    /// window-spec argument type as the JVM does.)
    #[must_use]
    pub fn windowed_by_session(
        mut self,
        windows: crate::dsl::windows::SessionWindows,
    ) -> crate::dsl::session_windowed_kgrouped::SessionWindowedKGroupedStream<K, V>
    where
        V: Sync,
    {
        crate::dsl::session_windowed_kgrouped::SessionWindowedKGroupedStream::new(
            Rc::clone(&self.builder),
            self.parent,
            self.key_changing_upstream,
            self.grouped_name.take(),
            self.repartition_lower.take(),
            windows,
        )
    }

    /// Shared body for `count`/`aggregate`: mint the store name at the JVM
    /// counter position, then lower the (optional) repartition + aggregate node.
    fn aggregate_inner<KS, VS, VA, I, A>(
        self,
        materialized: Materialized<KS, VS>,
        store_prefix: &'static str,
        init: I,
        agg: A,
    ) -> KTable<K, VA>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, store_prefix);
        self.lower_aggregate::<KS, VS, VA, I, A>(materialized, store_name, init, agg)
    }

    /// Record the (optional) repartition node + a `KStreamAggregateProcessor`
    /// aggregate node, returning the resulting `KTable`. The store name is already
    /// minted (at the JVM counter position) by the caller.
    fn lower_aggregate<KS, VS, VA, I, A>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        agg: A,
    ) -> KTable<K, VA>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            logging,
            ..
        } = materialized;
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let mut g = self.builder.borrow_mut();
        let agg_parent =
            Self::record_repartition(&mut g, &store_name, parent, key_changing, rp_lower);

        let agg_name = g.new_processor_name(names::AGGREGATE);
        let agg_id = g.graph.add(
            agg_name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: logging,
            },
            vec![agg_parent],
        );
        let store_for_thunk = store_name.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&agg_parent].clone());
            let store_for_proc = store_for_thunk.clone();
            // The aggregate forwards Change<VA> (prior store value as old).
            let h = state
                .topology
                .add_processor::<K, V, K, crate::dsl::processors::change::Change<VA>, _, _, _>(
                    agg_name.clone(),
                    move || KStreamAggregateProcessor {
                        store_name: store_for_proc.clone(),
                        init: init.clone(),
                        agg: agg.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            // Honor `Materialized::with_logging(bool)`:
            // - logging=true  → standard add_state_store (changelog topic emitted)
            // - logging=false → add_state_store_no_changelog (store usable at
            //   runtime, but NO state_changelog_topics entry in the wire topology)
            if logging {
                state.topology.add_state_store::<K, VA, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde.clone(),
                    value_serde.clone(),
                    [h.name().to_string()],
                );
            } else {
                state
                    .topology
                    .add_state_store_no_changelog::<K, VA, KS, VS>(
                        store_for_thunk.clone(),
                        key_serde.clone(),
                        value_serde.clone(),
                    );
            }
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), agg_id, Some(store_name), None)
    }

    /// Record the (optional) repartition node + a `KStreamReduceProcessor`
    /// node (first value seeds, later values fold), returning the `KTable<K, V>`.
    fn lower_reduce<KS, VS, R>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        reducer: R,
    ) -> KTable<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            logging,
            ..
        } = materialized;
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let mut g = self.builder.borrow_mut();
        let agg_parent =
            Self::record_repartition(&mut g, &store_name, parent, key_changing, rp_lower);

        let red_name = g.new_processor_name(names::REDUCE);
        let red_id = g.graph.add(
            red_name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: logging,
            },
            vec![agg_parent],
        );
        let store_for_thunk = store_name.clone();
        g.graph.nodes[red_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&agg_parent].clone());
            let store_for_proc = store_for_thunk.clone();
            let reducer = reducer.clone();
            // The reduce forwards Change<V> (prior store value as old).
            let h = state
                .topology
                .add_processor::<K, V, K, crate::dsl::processors::change::Change<V>, _, _, _>(
                    red_name.clone(),
                    move || KStreamReduceProcessor {
                        store_name: store_for_proc.clone(),
                        reducer: reducer.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            // Honor `Materialized::with_logging(bool)` for reduce as well.
            if logging {
                state.topology.add_state_store::<K, V, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde.clone(),
                    value_serde.clone(),
                    [h.name().to_string()],
                );
            } else {
                state.topology.add_state_store_no_changelog::<K, V, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde.clone(),
                    value_serde.clone(),
                );
            }
            state.handle_name.insert(red_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), red_id, Some(store_name), None)
    }

    /// If the upstream is key-changing, record a `Repartition` node
    /// (`sink → repartition topic → source`) re-grouping by key and return its
    /// id (the aggregation's parent). Otherwise return the grouped parent id
    /// unchanged. The repartition topic name (`<app>-<store>-repartition`) is
    /// finalized in the thunk; byte-exactness is validated in Task 8.
    ///
    /// Takes the needed fields by value (not `&mut self`) so the caller can hold
    /// the `builder.borrow_mut()` guard `g` across the call.
    pub(crate) fn record_repartition(
        g: &mut InternalStreamsBuilder,
        store_name: &str,
        parent: NodeId,
        key_changing_upstream: bool,
        repartition_lower: Option<RepartitionLowerFn>,
    ) -> NodeId {
        if !key_changing_upstream {
            return parent;
        }
        // The JVM's `KStreamImpl.createRepartitionedSource` first mints a
        // null-key **filter** node (records with a null key can't be repartitioned
        // by key), then the sink + source. We don't lower a filter node (the
        // aggregate processors already reject null keys), but we must consume the
        // counter index so downstream auto-names match the JVM byte-for-byte — e.g.
        // a second aggregation's store lands at the same index as the JVM fixture.
        let _filter_name = g.new_processor_name(names::FILTER);
        let sink_name = g.new_processor_name(names::SINK);
        let source_name = g.new_processor_name(names::SOURCE);
        let upstream = parent;
        let topic_store = store_name.to_string();
        let lower = repartition_lower.expect("repartition_lower already consumed");
        let rp_id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::Repartition {
                topic: format!("{topic_store}{}", names::REPARTITION_SUFFIX),
                partitions: None,
            },
            vec![upstream],
        );
        g.graph.nodes[rp_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&upstream].clone();
            let topic = format!(
                "{}-{topic_store}{}",
                state.app_id,
                names::REPARTITION_SUFFIX
            );
            lower(
                state,
                parent_name,
                sink_name.clone(),
                source_name.clone(),
                topic,
            );
            // The aggregate wires to the repartition source node.
            state.handle_name.insert(rp_id, source_name.clone());
        }));
        rp_id
    }
}

/// Mint the store name: the `Materialized` name when present, else a fresh
/// counter at the JVM position (called *before* the aggregate processor name).
pub(crate) fn mint_store_name<KS, VS>(
    builder: &Rc<RefCell<InternalStreamsBuilder>>,
    materialized: &Materialized<KS, VS>,
    prefix: &str,
) -> String {
    match &materialized.store_name {
        Some(name) => name.clone(),
        None => builder.borrow_mut().new_processor_name(prefix),
    }
}
