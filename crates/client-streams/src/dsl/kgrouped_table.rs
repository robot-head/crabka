//! `KGroupedTable<KR, VR>`: the handle between `KTable::group_by` and a terminal
//! table aggregation (`count`/`reduce`/`aggregate`). Unlike `KGroupedStream`,
//! the input is a `Change<V>` change-stream and the repartition topic carries a
//! `Change<VR>` (via the `Changed` serde). `KTable.groupBy` always repartitions.

use std::{any::Any, cell::RefCell, marker::PhantomData, rc::Rc};

use crate::{
    dsl::{
        builder::InternalStreamsBuilder,
        config::Materialized,
        graph::{GraphNodeKind, LowerState, NodeId},
        kgrouped::mint_store_name,
        ktable::KTable,
        names,
        processors::{
            change::Change, table_aggregate::KTableAggregateProcessor,
            tuple_forwarder::TupleForwarder,
        },
    },
    processor::serde::{Changed, Consumed, DefaultSerde, I64Serde, Produced, Serde},
    topology::NodeHandle,
};

/// Erased thunk wiring the `Change`-carrying repartition `sink → topic → source`.
/// Args: `(state, parent_name, sink_name, source_name, topic)`. The parent node
/// here forwards `Record<KR, Change<VR>>` (the SELECT output).
pub(crate) type ChangedRepartitionLowerFn =
    Box<dyn FnOnce(&mut LowerState, String, String, String, String) + Send>;

/// Build a [`ChangedRepartitionLowerFn`] capturing the grouped key serde and a
/// `Changed`-wrapped value serde so the repartition round-trip carries `Change<VR>`.
pub(crate) fn repartition_lower_changed<KR, VR, KS, VS>(
    key_serde: KS,
    value_serde: VS,
) -> ChangedRepartitionLowerFn
where
    KR: Any + Send + Sync + Clone,
    VR: Any + Send + Sync + Clone,
    KS: Serde<KR> + Clone + 'static,
    VS: Serde<VR> + Clone + 'static,
{
    Box::new(
        move |state: &mut LowerState,
              parent_name: String,
              sink_name: String,
              source_name: String,
              topic: String| {
            let parent = NodeHandle::<KR, Change<VR>>::from_name(parent_name);
            state
                .topology
                .add_sink_explicit::<KR, Change<VR>, KS, Changed<VS>, _, _>(
                    sink_name,
                    topic.clone(),
                    [parent],
                    Produced::with(key_serde.clone(), Changed::new(value_serde.clone())),
                );
            state.topology.add_repartition_topic(topic.clone());
            state
                .topology
                .add_source_explicit::<KR, Change<VR>, KS, Changed<VS>>(
                    source_name,
                    [topic],
                    Consumed::with(key_serde, Changed::new(value_serde)),
                );
        },
    )
}

/// Handle produced by `KTable::group_by[_explicit]`.
pub struct KGroupedTable<KR, VR> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    /// The `KTABLE-SELECT` repartition-map node, already recorded by `group_by`
    /// at call time (mirroring the JVM, which mints it when `groupBy()` is called
    /// — *before* the terminal op mints the result store). The terminal op wires
    /// the repartition off this node.
    select_node: NodeId,
    repartition_lower: Option<ChangedRepartitionLowerFn>,
    _pd: PhantomData<fn() -> (KR, VR)>,
}

impl<KR, VR> KGroupedTable<KR, VR>
where
    KR: Any + Send + Sync + Clone + PartialEq,
    VR: Any + Send + Sync + Clone,
{
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        select_node: NodeId,
        repartition_lower: ChangedRepartitionLowerFn,
    ) -> Self {
        Self {
            builder,
            select_node,
            repartition_lower: Some(repartition_lower),
            _pd: PhantomData,
        }
    }

    /// `count` into a materialized `KTable<KR, i64>`.
    pub fn count_explicit<KS, VS>(
        self,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<KR, i64, KS, VS>
    where
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        self.aggregate_inner(
            materialized.into(),
            names::KTABLE_AGGREGATE_STORE,
            || 0i64,
            |_k: &KR, _v: &VR, a: i64| a + 1,
            |_k: &KR, _v: &VR, a: i64| a - 1,
        )
    }

    /// `reduce`: fold per group with `adder`, undo with `subtractor`. Result type
    /// stays `VR`; the first value for a group seeds (an empty group starts at
    /// `VR::default()`, and `adder(default, first) == first` for the additive
    /// reduce — exact JVM parity; `aggregate()` is the escape hatch otherwise).
    pub fn reduce_explicit<KS, VS, Add, Sub>(
        self,
        adder: Add,
        subtractor: Sub,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<KR, VR, KS, VS>
    where
        VR: Default,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<VR> + Clone + 'static,
        Add: Fn(&VR, &VR) -> VR + Clone + Send + Sync + 'static,
        Sub: Fn(&VR, &VR) -> VR + Clone + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::KTABLE_REDUCE_STORE);
        self.lower::<KS, VS, VR, _, _, _>(
            materialized,
            store_name,
            VR::default,
            move |_k: &KR, v: &VR, acc: VR| adder(&acc, v),
            move |_k: &KR, v: &VR, acc: VR| subtractor(&acc, v),
        )
    }

    /// `aggregate`: general subtract/add aggregation into `KTable<KR, T>`.
    pub fn aggregate_explicit<KS, VS, T, I, Add, Sub>(
        self,
        init: I,
        adder: Add,
        subtractor: Sub,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<KR, T, KS, VS>
    where
        T: Any + Send + Clone,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<T> + Clone + 'static,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        self.aggregate_inner(
            materialized.into(),
            names::KTABLE_AGGREGATE_STORE,
            init,
            adder,
            subtractor,
        )
    }

    /// `count` with default serdes.
    pub fn count(
        self,
        store_name: impl Into<String>,
    ) -> KTable<KR, i64, <KR as DefaultSerde>::Serde, I64Serde>
    where
        KR: DefaultSerde,
        <KR as DefaultSerde>::Serde: Serde<KR> + Clone,
    {
        self.count_explicit(
            Materialized::with(<KR as DefaultSerde>::Serde::default(), I64Serde)
                .as_store(store_name),
        )
    }

    /// `aggregate` with default serdes.
    pub fn aggregate<T, I, Add, Sub>(
        self,
        init: I,
        adder: Add,
        subtractor: Sub,
        store_name: impl Into<String>,
    ) -> KTable<KR, T, <KR as DefaultSerde>::Serde, <T as DefaultSerde>::Serde>
    where
        T: DefaultSerde + Any + Send + Clone,
        KR: DefaultSerde,
        <KR as DefaultSerde>::Serde: Serde<KR> + Clone,
        <T as DefaultSerde>::Serde: Serde<T> + Clone,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        self.aggregate_explicit(
            init,
            adder,
            subtractor,
            Materialized::with(
                <KR as DefaultSerde>::Serde::default(),
                <T as DefaultSerde>::Serde::default(),
            )
            .as_store(store_name),
        )
    }

    fn aggregate_inner<KS, VS, T, I, Add, Sub>(
        self,
        materialized: Materialized<KS, VS>,
        store_prefix: &'static str,
        init: I,
        adder: Add,
        subtractor: Sub,
    ) -> KTable<KR, T, KS, VS>
    where
        T: Any + Send + Clone,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<T> + Clone + 'static,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, store_prefix);
        self.lower::<KS, VS, T, I, Add, Sub>(materialized, store_name, init, adder, subtractor)
    }

    /// Record SELECT → repartition(Changed) → AGGREGATE + store; return
    /// `KTable<KR, T>`.
    fn lower<KS, VS, T, I, Add, Sub>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        adder: Add,
        subtractor: Sub,
    ) -> KTable<KR, T, KS, VS>
    where
        T: Any + Send + Clone,
        KS: Serde<KR> + Clone + 'static,
        VS: Serde<T> + Clone + 'static,
        I: Fn() -> T + Clone + Send + Sync + 'static,
        Add: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
        Sub: Fn(&KR, &VR, T) -> T + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            logging,
            caching,
            ..
        } = materialized;
        let suppress_factory = crate::dsl::ktable::kv_suppress_factory::<KR, T, KS, VS>(
            key_serde.clone(),
            value_serde.clone(),
        );
        let rp_lower = self
            .repartition_lower
            .take()
            .expect("repartition_lower consumed");
        // The SELECT (repartition-map) node was recorded at `group_by` time so its
        // name-counter index precedes this terminal op's store (JVM order).
        let select_id = self.select_node;
        let mut g = self.builder.borrow_mut();

        // KTable.groupBy ALWAYS repartitions. Mint sink+source for the repartition
        // (the JVM table-repartition path mints NO null-key filter — the
        // repartition-map already emits keyed records), then a Repartition node
        // fed by SELECT.
        let sink_name = g.new_processor_name(names::SINK);
        let source_name = g.new_processor_name(names::SOURCE);
        let topic_store = store_name.clone();
        let rp_id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::Repartition {
                topic: format!("{topic_store}{}", names::REPARTITION_SUFFIX),
                partitions: None,
            },
            vec![select_id],
        );
        g.graph.nodes[rp_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&select_id].clone();
            let topic = format!(
                "{}-{topic_store}{}",
                state.app_id,
                names::REPARTITION_SUFFIX
            );
            rp_lower(
                state,
                parent_name,
                sink_name.clone(),
                source_name.clone(),
                topic,
            );
            state.handle_name.insert(rp_id, source_name.clone());
        }));

        // 3) AGGREGATE node fed by the repartition source.
        let agg_name = g.new_processor_name(names::KTABLE_AGGREGATE);
        let agg_id = g.graph.add(
            agg_name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: logging,
            },
            vec![rp_id],
        );
        let store_for_thunk = store_name.clone();
        let key_serde_lower = key_serde.clone();
        let value_serde_lower = value_serde.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<KR, Change<VR>>::from_name(state.handle_name[&rp_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state
                .topology
                .add_processor::<KR, Change<VR>, KR, Change<T>, _, _, _>(
                    agg_name.clone(),
                    move || KTableAggregateProcessor {
                        store_name: store_for_proc.clone(),
                        init: init.clone(),
                        adder: adder.clone(),
                        subtractor: subtractor.clone(),
                        forwarder: TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            if logging {
                state.topology.add_state_store::<KR, T, KS, VS>(
                    store_for_thunk.clone(),
                    key_serde_lower.clone(),
                    value_serde_lower.clone(),
                    [h.name().to_string()],
                );
            } else {
                state
                    .topology
                    .add_state_store_no_changelog::<KR, T, KS, VS>(
                        store_for_thunk.clone(),
                        key_serde_lower.clone(),
                        value_serde_lower.clone(),
                    );
            }
            state.topology.mark_store_caching(&store_for_thunk, caching);
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            agg_id,
            Some(store_name),
            None,
            key_serde,
            value_serde,
        )
        .with_suppress_factory(Some(suppress_factory))
    }
}
