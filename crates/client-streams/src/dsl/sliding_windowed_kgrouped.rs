//! `SlidingWindowedKGroupedStream<K,V>`: the handle between
//! `KGroupedStream::windowed_by_sliding(SlidingWindows)` and a terminal sliding
//! (KIP-450) aggregation. Sibling of
//! [`crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream`]: same grouped
//! lineage + window store, but the aggregate processor implements the KIP-450
//! left/right-window algorithm and the windows are data-defined inclusive
//! windows of size `time_difference`.
use std::{any::Any, cell::RefCell, marker::PhantomData, rc::Rc};

use crate::{
    dsl::{
        builder::InternalStreamsBuilder,
        config::Materialized,
        emit::EmitStrategy,
        graph::{GraphNodeKind, LowerState, NodeId},
        kgrouped::{KGroupedStream, RepartitionLowerFn, mint_store_name},
        ktable::{KTable, SuppressStoreFactory},
        names,
        processors::sliding_window_aggregate::{
            KStreamSlidingWindowAggregateProcessor, KStreamSlidingWindowReduceProcessor,
        },
        windows::{SlidingWindows, TimeWindowedSerde, Windowed},
    },
    processor::serde::{DefaultSerde, Serde},
    topology::NodeHandle,
};

/// Handle produced by [`KGroupedStream::windowed_by_sliding`]; terminal sliding
/// aggregations consume it.
///
/// [`KGroupedStream::windowed_by_sliding`]: crate::dsl::kgrouped::KGroupedStream::windowed_by_sliding
pub struct SlidingWindowedKGroupedStream<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    parent: NodeId,
    key_changing_upstream: bool,
    #[allow(dead_code)]
    grouped_name: Option<String>,
    repartition_lower: Option<RepartitionLowerFn>,
    windows: SlidingWindows,
    emit: EmitStrategy,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> SlidingWindowedKGroupedStream<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Sync + Clone,
{
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        parent: NodeId,
        key_changing_upstream: bool,
        grouped_name: Option<String>,
        repartition_lower: Option<RepartitionLowerFn>,
        windows: SlidingWindows,
    ) -> Self {
        Self {
            builder,
            parent,
            key_changing_upstream,
            grouped_name,
            repartition_lower,
            windows,
            emit: EmitStrategy::default(),
            _pd: PhantomData,
        }
    }

    /// Emit on every update (default) or only on window close (KIP-825).
    ///
    /// In `on_window_close`, records whose window has already closed are dropped
    /// (so no duplicate final is emitted — unlike the session variant, which may
    /// re-emit under `grace < gap`).
    #[must_use]
    pub fn emit_strategy(mut self, emit: EmitStrategy) -> Self {
        self.emit = emit;
        self
    }

    pub fn count_explicit<KS, VS>(
        self,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, i64, TimeWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        self.aggregate_inner(
            materialized.into(),
            names::AGGREGATE_STORE,
            || 0i64,
            |_k: &K, _v: &V, acc: i64| acc + 1,
        )
    }

    pub fn reduce_explicit<KS, VS, R>(
        self,
        reducer: R,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, V, TimeWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::REDUCE_STORE);
        self.lower_reduce::<KS, VS, R>(materialized, store_name, reducer)
    }

    pub fn aggregate_explicit<KS, VS, VA, I, A>(
        self,
        init: I,
        agg: A,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Sync + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        self.aggregate_inner(materialized.into(), names::AGGREGATE_STORE, init, agg)
    }

    pub fn count(
        self,
        store_name: impl Into<String>,
    ) -> KTable<
        Windowed<K>,
        i64,
        TimeWindowedSerde<<K as DefaultSerde>::Serde>,
        crate::processor::serde::I64Serde,
    >
    where
        K: DefaultSerde,
        <K as DefaultSerde>::Serde: Serde<K> + Clone,
    {
        self.count_explicit(
            Materialized::with(
                <K as DefaultSerde>::Serde::default(),
                crate::processor::serde::I64Serde,
            )
            .as_store(store_name),
        )
    }

    pub fn reduce<R>(
        self,
        reducer: R,
        store_name: impl Into<String>,
    ) -> KTable<
        Windowed<K>,
        V,
        TimeWindowedSerde<<K as DefaultSerde>::Serde>,
        <V as DefaultSerde>::Serde,
    >
    where
        K: DefaultSerde,
        V: DefaultSerde,
        <K as DefaultSerde>::Serde: Serde<K> + Clone,
        <V as DefaultSerde>::Serde: Serde<V> + Clone,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        self.reduce_explicit(
            reducer,
            Materialized::with(
                <K as DefaultSerde>::Serde::default(),
                <V as DefaultSerde>::Serde::default(),
            )
            .as_store(store_name),
        )
    }

    pub fn aggregate<VA, I, A>(
        self,
        init: I,
        agg: A,
        store_name: impl Into<String>,
    ) -> KTable<
        Windowed<K>,
        VA,
        TimeWindowedSerde<<K as DefaultSerde>::Serde>,
        <VA as DefaultSerde>::Serde,
    >
    where
        VA: DefaultSerde + Any + Send + Clone,
        K: DefaultSerde,
        <K as DefaultSerde>::Serde: Serde<K> + Clone,
        <VA as DefaultSerde>::Serde: Serde<VA> + Clone,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        self.aggregate_explicit(
            init,
            agg,
            Materialized::with(
                <K as DefaultSerde>::Serde::default(),
                <VA as DefaultSerde>::Serde::default(),
            )
            .as_store(store_name),
        )
    }

    fn aggregate_inner<KS, VS, VA, I, A>(
        self,
        materialized: Materialized<KS, VS>,
        store_prefix: &'static str,
        init: I,
        agg: A,
    ) -> KTable<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Sync + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, store_prefix);
        self.lower_aggregate::<KS, VS, VA, I, A>(materialized, store_name, init, agg)
    }

    fn lower_aggregate<KS, VS, VA, I, A>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        agg: A,
    ) -> KTable<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Sync + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            caching,
            ..
        } = materialized;
        let suppress_factory = sliding_suppress_factory::<K, VA, KS, VS>(
            key_serde.clone(),
            value_serde.clone(),
            self.windows,
        );
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let windows = self.windows;
        let emit = self.emit;
        let mut g = self.builder.borrow_mut();
        let agg_parent = KGroupedStream::<K, V>::record_repartition(
            &mut g,
            &store_name,
            parent,
            key_changing,
            rp_lower,
        );

        let agg_name = g.new_processor_name(names::AGGREGATE);
        let agg_id = g.graph.add(
            agg_name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: true,
            },
            vec![agg_parent],
        );
        let store_for_thunk = store_name.clone();
        let key_serde_for_lower = key_serde.clone();
        let value_serde_for_lower = value_serde.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&agg_parent].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<VA>, _, _, _>(
                    agg_name.clone(),
                    move || KStreamSlidingWindowAggregateProcessor {
                        store_name: store_for_proc.clone(),
                        windows,
                        init: init.clone(),
                        agg: agg.clone(),
                        stream_time: i64::MIN,
                        emit,
                        last_emitted_close: i64::MIN,
                        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            // Sliding window retention formula (JVM-exact):
            // timeDifferenceMs + timeDifferenceMs + gracePeriodMs + 86_400_000
            // (a sliding window spans [t - timeDiff, t + timeDiff] so the effective
            // window size for changelog retention is 2 * timeDifferenceMs).
            state.topology.add_window_store::<K, VA, KS, VS>(
                store_for_thunk.clone(),
                key_serde_for_lower.clone(),
                value_serde_for_lower.clone(),
                // Retention basis = 2 * timeDiff (the [t-timeDiff, t+timeDiff] span);
                // the true window size for the key end is 1 * timeDiff.
                (
                    windows.time_difference * 2.0,
                    windows.time_difference,
                    windows.grace,
                ),
                [h.name().to_string()],
            );
            // Cache only emit-on-update sliding aggregates: emit-final stays
            // uncached or the flush would emit the per-update changes emit-final
            // suppresses.
            state
                .topology
                .mark_store_caching(&store_for_thunk, caching && emit.is_on_update());
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            agg_id,
            Some(store_name),
            None,
            TimeWindowedSerde::new(key_serde.clone(), windows.time_difference),
            value_serde.clone(),
        )
        .with_window_grace(Some(windows.grace))
        .with_suppress_factory(Some(suppress_factory))
    }

    fn lower_reduce<KS, VS, R>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        reducer: R,
    ) -> KTable<Windowed<K>, V, TimeWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            caching,
            ..
        } = materialized;
        let suppress_factory = sliding_suppress_factory::<K, V, KS, VS>(
            key_serde.clone(),
            value_serde.clone(),
            self.windows,
        );
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let windows = self.windows;
        let emit = self.emit;
        let mut g = self.builder.borrow_mut();
        let agg_parent = KGroupedStream::<K, V>::record_repartition(
            &mut g,
            &store_name,
            parent,
            key_changing,
            rp_lower,
        );

        let red_name = g.new_processor_name(names::REDUCE);
        let red_id = g.graph.add(
            red_name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: true,
            },
            vec![agg_parent],
        );
        let store_for_thunk = store_name.clone();
        let key_serde_for_lower = key_serde.clone();
        let value_serde_for_lower = value_serde.clone();
        g.graph.nodes[red_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&agg_parent].clone());
            let store_for_proc = store_for_thunk.clone();
            let reducer = reducer.clone();
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<V>, _, _, _>(
                    red_name.clone(),
                    move || KStreamSlidingWindowReduceProcessor {
                        store_name: store_for_proc.clone(),
                        windows,
                        reducer: reducer.clone(),
                        stream_time: i64::MIN,
                        emit,
                        last_emitted_close: i64::MIN,
                        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            // Sliding window retention formula (JVM-exact): 2 * timeDifferenceMs + grace + 1day.
            state.topology.add_window_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde_for_lower.clone(),
                value_serde_for_lower.clone(),
                // Retention basis = 2 * timeDiff (the [t-timeDiff, t+timeDiff] span);
                // the true window size for the key end is 1 * timeDiff.
                (
                    windows.time_difference * 2.0,
                    windows.time_difference,
                    windows.grace,
                ),
                [h.name().to_string()],
            );
            // Cache only emit-on-update sliding reduces (see aggregate lower).
            state
                .topology
                .mark_store_caching(&store_for_thunk, caching && emit.is_on_update());
            state.handle_name.insert(red_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            red_id,
            Some(store_name),
            None,
            TimeWindowedSerde::new(key_serde.clone(), windows.time_difference),
            value_serde.clone(),
        )
        .with_window_grace(Some(windows.grace))
        .with_suppress_factory(Some(suppress_factory))
    }
}

fn sliding_suppress_factory<K, VA, KS, VS>(
    key_serde: KS,
    value_serde: VS,
    windows: SlidingWindows,
) -> SuppressStoreFactory
where
    K: Any + Send + Sync + Clone,
    VA: Any + Send + Clone,
    KS: Serde<K> + Clone + 'static,
    VS: Serde<VA> + Clone + 'static,
{
    std::sync::Arc::new(
        move |state: &mut LowerState, store_name: &str, proc_name: &str, logging: bool| {
            state
                .topology
                .add_suppress_store::<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>(
                    store_name.to_string(),
                    TimeWindowedSerde::new(key_serde.clone(), windows.time_difference),
                    value_serde.clone(),
                    logging,
                    [proc_name.to_string()],
                );
        },
    )
}
