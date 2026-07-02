//! `TimeWindowedKGroupedStream<K,V>`: the intermediate handle between
//! `KGroupedStream::windowed_by(TimeWindows)` and a terminal **windowed**
//! aggregation (`count`/`reduce`/`aggregate`).
//!
//! This is the windowed analogue of [`crate::dsl::kgrouped::KGroupedStream`]: it
//! holds the same grouped lineage (parent node, key-changing flag, optional
//! `Grouped` name, repartition-lowering thunk) plus the [`TimeWindows`] spec, and
//! its terminal ops mirror [`KGroupedStream::aggregate`]/`reduce` exactly — except
//!
//! 1. the aggregate processor emits `Windowed<K>` keys, and
//! 2. the materialized store is a **window store**
//!    ([`crate::topology::Topology::add_window_store`]) carrying the window size +
//!    grace for changelog retention.
//!
//! The result is a `KTable<Windowed<K>, _>` whose changelog is logged with
//! `compact,delete` retention derived from the window size and grace.

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
        processors::window_aggregate::{
            KStreamWindowAggregateProcessor, KStreamWindowReduceProcessor,
        },
        windows::{TimeWindowedSerde, TimeWindows, Windowed},
    },
    processor::serde::{DefaultSerde, Serde},
    topology::NodeHandle,
};

/// Handle produced by [`KGroupedStream::windowed_by`]; terminal windowed
/// aggregations consume it.
///
/// [`KGroupedStream::windowed_by`]: crate::dsl::kgrouped::KGroupedStream::windowed_by
pub struct TimeWindowedKGroupedStream<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    /// Logical id of the node feeding the aggregation (the source/select-key).
    parent: NodeId,
    /// True when the upstream key was rewritten without a re-group → the
    /// aggregation must insert a repartition before the aggregate node.
    key_changing_upstream: bool,
    /// Explicit `Grouped` name (drives repartition topic naming).
    #[allow(dead_code)]
    grouped_name: Option<String>,
    /// Typed repartition-lowering thunk (taken once by the terminal op).
    repartition_lower: Option<RepartitionLowerFn>,
    /// The window spec driving `windows_for(ts)` + the window-store retention.
    windows: TimeWindows,
    /// Emit on every update (default) or only on window close (KIP-825).
    emit: EmitStrategy,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> TimeWindowedKGroupedStream<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        parent: NodeId,
        key_changing_upstream: bool,
        grouped_name: Option<String>,
        repartition_lower: Option<RepartitionLowerFn>,
        windows: TimeWindows,
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

    /// `count`: count records per (key, window) into a windowed
    /// `KTable<Windowed<K>, i64>`. `init = || 0`, `agg = |_k, _v, acc| acc + 1`.
    pub fn count_explicit<KS, VS>(
        self,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, i64, TimeWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        self.aggregate_inner_windowed(
            materialized.into(),
            names::AGGREGATE_STORE,
            || 0i64,
            |_k: &K, _v: &V, acc: i64| acc + 1,
        )
    }

    /// `reduce`: combine values per (key, window) with `reducer`, materialized as
    /// `KTable<Windowed<K>, V>`. The first value in a window seeds the accumulator
    /// (the JVM `Reducer` has no separate `init`); later values fold via
    /// `reducer(&acc, &value)`. The backing processor keeps the public value
    /// type `V`.
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
        self.lower_reduce_windowed::<KS, VS, R>(materialized, store_name, reducer)
    }

    /// `aggregate`: general windowed aggregation with caller-supplied `init` +
    /// `agg`, materialized as `KTable<Windowed<K>, VA>`.
    pub fn aggregate_explicit<KS, VS, VA, I, A>(
        self,
        init: I,
        agg: A,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        self.aggregate_inner_windowed(materialized.into(), names::AGGREGATE_STORE, init, agg)
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

    /// Shared body for windowed `count`/`aggregate`: mint the store name at the
    /// JVM counter position, then lower the (optional) repartition + windowed
    /// aggregate node. Unlike the non-windowed `count`, the JVM windowed `count`
    /// does NOT burn an extra store-name index (validated byte-exact against the
    /// `suppress_until_window_closes_logged` fixture #14, whose suppress store index
    /// is consecutive with the aggregate store + processor).
    fn aggregate_inner_windowed<KS, VS, VA, I, A>(
        self,
        materialized: Materialized<KS, VS>,
        store_prefix: &'static str,
        init: I,
        agg: A,
    ) -> KTable<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, store_prefix);
        self.lower_aggregate_windowed::<KS, VS, VA, I, A>(materialized, store_name, init, agg)
    }

    /// Record the (optional) repartition node + a windowed aggregate node,
    /// returning the resulting
    /// `KTable<Windowed<K>, VA>`. Mirrors `KGroupedStream::lower_aggregate`, but
    /// emits `Windowed<K>` keys and a window store.
    #[allow(clippy::too_many_lines)]
    fn lower_aggregate_windowed<KS, VS, VA, I, A>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        agg: A,
    ) -> KTable<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>
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
            caching,
            ..
        } = materialized;
        // Factory that lets a downstream `suppress` register a SuppressBytesStore
        // with the windowed key serde (`TimeWindowedSerde`) + the aggregate value
        // serde. Built before the agg thunk moves the serdes.
        let suppress_factory = windowed_suppress_factory::<K, VA, KS, VS>(
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
            // The windowed aggregate emits `Windowed<K>` keys + `Change<VA>`.
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<VA>, _, _, _>(
                    agg_name.clone(),
                    move || KStreamWindowAggregateProcessor {
                        store_name: store_for_proc.clone(),
                        windows,
                        init: init.clone(),
                        agg: agg.clone(),
                        emit,
                        stream_time: i64::MIN,
                        last_emitted_close: i64::MIN,
                        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            // Windowed stores carry a changelog so they can be restored by the runtime.
            state.topology.add_window_store::<K, VA, KS, VS>(
                store_for_thunk.clone(),
                key_serde_for_lower.clone(),
                value_serde_for_lower.clone(),
                // Tumbling/hopping: retention basis == window size.
                windows.size_ms,
                windows.size_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            // Cache only emit-on-update windowed aggregates: emit-final must stay
            // uncached or the flush would emit the per-update changes emit-final
            // deliberately suppresses.
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
            TimeWindowedSerde::new(key_serde.clone(), windows.size_ms),
            value_serde.clone(),
        )
        .with_window_grace(Some(windows.grace_ms))
        .with_suppress_factory(Some(suppress_factory))
    }

    /// Record the (optional) repartition node + a windowed reduce node (first
    /// value in a window seeds, later
    /// values fold), returning the `KTable<Windowed<K>, V>`.
    #[allow(clippy::too_many_lines)]
    fn lower_reduce_windowed<KS, VS, R>(
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
        let suppress_factory = windowed_suppress_factory::<K, V, KS, VS>(
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
            // The windowed reduce emits `Windowed<K>` keys + `Change<V>`.
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<V>, _, _, _>(
                    red_name.clone(),
                    move || KStreamWindowReduceProcessor {
                        store_name: store_for_proc.clone(),
                        windows,
                        reducer: reducer.clone(),
                        emit,
                        stream_time: i64::MIN,
                        last_emitted_close: i64::MIN,
                        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_window_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde_for_lower.clone(),
                value_serde_for_lower.clone(),
                // Tumbling/hopping: retention basis == window size.
                windows.size_ms,
                windows.size_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            // Cache only emit-on-update windowed reduces (see aggregate lower).
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
            TimeWindowedSerde::new(key_serde.clone(), windows.size_ms),
            value_serde.clone(),
        )
        .with_window_grace(Some(windows.grace_ms))
        .with_suppress_factory(Some(suppress_factory))
    }
}

/// Build the suppress-store factory for a windowed-aggregation result table.
/// Captures the windowed key serde ([`TimeWindowedSerde`]) + the aggregate value
/// serde so a downstream `suppress` can register a
/// `SuppressBytesStore<Windowed<K>, VA>` with the right serdes + changelog config.
fn windowed_suppress_factory<K, VA, KS, VS>(
    key_serde: KS,
    value_serde: VS,
    windows: TimeWindows,
) -> SuppressStoreFactory
where
    K: Any + Send + Sync + Clone,
    VA: Any + Send + Clone,
    KS: Serde<K> + Clone + 'static,
    VS: Serde<VA> + Clone + 'static,
{
    std::sync::Arc::new(
        move |state: &mut LowerState, store_name: &str, proc_name: &str, logging: bool| {
            // The suppress buffer's changelog is a plain compacted KV changelog
            // (validated byte-exact against the JVM golden #14) — no retention arg.
            state
                .topology
                .add_suppress_store::<Windowed<K>, VA, TimeWindowedSerde<KS>, VS>(
                    store_name.to_string(),
                    TimeWindowedSerde::new(key_serde.clone(), windows.size_ms),
                    value_serde.clone(),
                    logging,
                    [proc_name.to_string()],
                );
        },
    )
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use crate::{
        dsl::{
            StreamsBuilder,
            emit::EmitStrategy,
            windows::{TimeWindowedSerde, TimeWindows, Window, Windowed},
        },
        processor::serde::{Consumed, I64Serde, Produced, StringSerde},
        test_driver::TopologyTestDriver,
    };

    /// Sub-task 3d-ii: an emit-on-update (default) windowed aggregate store is
    /// marked caching, so with a positive cache budget it lands in `cache_owner`.
    #[test]
    fn emit_on_update_window_store_is_cached() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .windowed_by(TimeWindows::of_size(10))
            .count("w");
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            10_485_760,
        ))
        .unwrap();
        check!(
            g.cache_owner.contains_key("w"),
            "emit-on-update window store must be cached, cache_owner = {:?}",
            g.cache_owner
        );
    }

    /// Sub-task 3d-ii: an emit-FINAL (KIP-825) windowed aggregate store must stay
    /// UNCACHED even with a positive budget — a cache flush would emit the
    /// per-update changes emit-final deliberately suppresses. The
    /// `emit.is_on_update()` guard at the mark site enforces this.
    #[test]
    fn emit_final_window_store_is_not_cached() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .windowed_by(TimeWindows::of_size(10))
            .emit_strategy(EmitStrategy::on_window_close())
            .count("w");
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            10_485_760,
        ))
        .unwrap();
        check!(
            !g.cache_owner.contains_key("w"),
            "emit-final window store must NOT be cached, cache_owner = {:?}",
            g.cache_owner
        );
    }

    /// `windowedBy(TimeWindows).emit_strategy(on_window_close).count`: emit-final
    /// (KIP-825) suppresses per-update emits and forwards a single final count for
    /// each window only once stream-time advances past its close. Records a@1 and
    /// a@4 accumulate (count 2) in window `[0,10)` silently; the a@12 record opens
    /// `[10,20)` and closes `[0,10)` (end 10 <= `window_close_time` 12), emitting
    /// exactly one record: window `[0,10)` with value 2.
    #[test]
    fn dsl_windowed_count_emit_final_emits_once_on_close() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .windowed_by(TimeWindows::of_size(10))
            .emit_strategy(EmitStrategy::on_window_close())
            .count("w")
            .to_stream()
            .to_explicit(
                "out",
                Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde),
            );
        let built = b.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        for ts in [1, 4, 12] {
            d.pipe_input(
                "in",
                Consumed::with(StringSerde, StringSerde),
                Some("a".to_string()),
                "x".to_string(),
                ts,
            );
        }
        let p = || Produced::with(TimeWindowedSerde::new(StringSerde, 10), I64Serde);
        assert_eq!(
            d.read_output("out", p()),
            Some((
                Some(Windowed {
                    key: "a".into(),
                    window: Window { start: 0, end: 10 }
                }),
                2
            )),
            "emit-final forwards window [0,10) with final count 2 on close"
        );
        // No per-update emits and the still-open window [10,20) is not emitted.
        assert_eq!(
            d.read_output("out", p()),
            None,
            "exactly one emit-final record"
        );
    }
}
