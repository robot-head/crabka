//! `SessionWindowedKGroupedStream<K,V>`: the handle between
//! `KGroupedStream::windowed_by_session(SessionWindows)` and a terminal session
//! aggregation (`count`/`reduce`/`aggregate`). The session analogue of
//! [`crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream`]: same grouped
//! lineage + the [`SessionWindows`] spec; terminal ops emit `Windowed<K>` keys and
//! materialize a **session store** (`add_session_store`). The result is a
//! `KTable<Windowed<K>, _>` with a changelog-backed session store.
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
        processors::session_aggregate::{
            KStreamSessionAggregateProcessor, KStreamSessionReduceProcessor,
        },
        windows::{SessionWindowedSerde, SessionWindows, Windowed},
    },
    processor::serde::{DefaultSerde, Serde},
    topology::NodeHandle,
};

/// Handle produced by [`KGroupedStream::windowed_by_session`].
///
/// [`KGroupedStream::windowed_by_session`]: crate::dsl::kgrouped::KGroupedStream::windowed_by_session
pub struct SessionWindowedKGroupedStream<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    parent: NodeId,
    key_changing_upstream: bool,
    #[allow(dead_code)]
    grouped_name: Option<String>,
    repartition_lower: Option<RepartitionLowerFn>,
    windows: SessionWindows,
    emit: EmitStrategy,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> SessionWindowedKGroupedStream<K, V>
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
        windows: SessionWindows,
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
    /// For `on_window_close`, configure the session grace `>=` the inactivity
    /// gap. With `grace < gap` a late, in-gap record can re-open and re-merge a
    /// session that already emitted its final result, producing a duplicate
    /// final — the processor does not yet drop such late records (exact
    /// late-record semantics are pinned by the JVM golden).
    #[must_use]
    pub fn emit_strategy(mut self, emit: EmitStrategy) -> Self {
        self.emit = emit;
        self
    }

    /// `count`: count records per session → `KTable<Windowed<K>, i64>`.
    pub fn count_explicit<KS, VS>(
        self,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, i64, SessionWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        // JVM `count` burns an extra store-name counter index when unnamed.
        if materialized.store_name.is_none() {
            self.builder
                .borrow_mut()
                .new_processor_name(names::AGGREGATE_STORE);
        }
        self.lower_aggregate::<KS, VS, i64, _, _, _>(
            materialized,
            store_name,
            || 0i64,
            |_k: &K, _v: &V, acc: i64| acc + 1,
            |_k: &K, a: i64, b: i64| a + b,
        )
    }

    /// `aggregate`: general session aggregation with `init` + `agg` + the session
    /// `merger` (combines two session aggregates on merge).
    pub fn aggregate_explicit<KS, VS, VA, I, A, M>(
        self,
        init: I,
        agg: A,
        merger: M,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, VA, SessionWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Sync + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
        M: Fn(&K, VA, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        self.lower_aggregate::<KS, VS, VA, I, A, M>(materialized, store_name, init, agg, merger)
    }

    /// `reduce`: combine values per session with `reducer` → `KTable<Windowed<K>, V>`.
    pub fn reduce_explicit<KS, VS, R>(
        self,
        reducer: R,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<Windowed<K>, V, SessionWindowedSerde<KS>, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::REDUCE_STORE);
        self.lower_reduce::<KS, VS, R>(materialized, store_name, reducer)
    }

    pub fn count(
        self,
        store_name: impl Into<String>,
    ) -> KTable<
        Windowed<K>,
        i64,
        SessionWindowedSerde<<K as DefaultSerde>::Serde>,
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
        SessionWindowedSerde<<K as DefaultSerde>::Serde>,
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

    pub fn aggregate<VA, I, A, M>(
        self,
        init: I,
        agg: A,
        merger: M,
        store_name: impl Into<String>,
    ) -> KTable<
        Windowed<K>,
        VA,
        SessionWindowedSerde<<K as DefaultSerde>::Serde>,
        <VA as DefaultSerde>::Serde,
    >
    where
        VA: DefaultSerde + Any + Send + Sync + Clone,
        K: DefaultSerde,
        <K as DefaultSerde>::Serde: Serde<K> + Clone,
        <VA as DefaultSerde>::Serde: Serde<VA> + Clone,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
        M: Fn(&K, VA, VA) -> VA + Clone + Send + Sync + 'static,
    {
        self.aggregate_explicit(
            init,
            agg,
            merger,
            Materialized::with(
                <K as DefaultSerde>::Serde::default(),
                <VA as DefaultSerde>::Serde::default(),
            )
            .as_store(store_name),
        )
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    fn lower_aggregate<KS, VS, VA, I, A, M>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        init: I,
        agg: A,
        merger: M,
    ) -> KTable<Windowed<K>, VA, SessionWindowedSerde<KS>, VS>
    where
        VA: Any + Send + Sync + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
        M: Fn(&K, VA, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            caching,
            ..
        } = materialized;
        let suppress_factory =
            session_suppress_factory::<K, VA, KS, VS>(key_serde.clone(), value_serde.clone());
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
            let init = init.clone();
            let agg = agg.clone();
            let merger = merger.clone();
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<VA>, _, _, _>(
                    agg_name.clone(),
                    move || KStreamSessionAggregateProcessor {
                        store_name: store_for_proc.clone(),
                        gap_ms: windows.gap_ms,
                        init: init.clone(),
                        agg: agg.clone(),
                        merger: merger.clone(),
                        emit,
                        grace_ms: windows.grace_ms,
                        stream_time: i64::MIN,
                        last_emitted_close: i64::MIN,
                        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_session_store::<K, VA, KS, VS>(
                store_for_thunk.clone(),
                key_serde_for_lower.clone(),
                value_serde_for_lower.clone(),
                windows.gap_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            // Cache only emit-on-update session aggregates: emit-final stays
            // uncached or the flush would emit the per-update tombstones+updates
            // emit-final suppresses.
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
            SessionWindowedSerde::new(key_serde.clone()),
            value_serde.clone(),
        )
        .with_window_grace(Some(windows.grace_ms))
        .with_suppress_factory(Some(suppress_factory))
    }

    #[allow(clippy::too_many_lines)]
    fn lower_reduce<KS, VS, R>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        reducer: R,
    ) -> KTable<Windowed<K>, V, SessionWindowedSerde<KS>, VS>
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
        let suppress_factory =
            session_suppress_factory::<K, V, KS, VS>(key_serde.clone(), value_serde.clone());
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
                    move || KStreamSessionReduceProcessor {
                        store_name: store_for_proc.clone(),
                        gap_ms: windows.gap_ms,
                        reducer: reducer.clone(),
                        emit,
                        grace_ms: windows.grace_ms,
                        stream_time: i64::MIN,
                        last_emitted_close: i64::MIN,
                        forwarder: crate::dsl::processors::tuple_forwarder::TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_session_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde_for_lower.clone(),
                value_serde_for_lower.clone(),
                windows.gap_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            // Cache only emit-on-update session reduces (see aggregate lower).
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
            SessionWindowedSerde::new(key_serde.clone()),
            value_serde.clone(),
        )
        .with_window_grace(Some(windows.grace_ms))
        .with_suppress_factory(Some(suppress_factory))
    }
}

/// Build the suppress-store factory for a session-aggregation result table.
/// Captures the session key serde ([`SessionWindowedSerde`]) + the aggregate value
/// serde so a downstream `suppress` registers a `SuppressBytesStore<Windowed<K>, VA>`
/// with the session-windowed key serde + the matching changelog config.
fn session_suppress_factory<K, VA, KS, VS>(key_serde: KS, value_serde: VS) -> SuppressStoreFactory
where
    K: Any + Send + Sync + Clone,
    VA: Any + Send + Clone,
    KS: Serde<K> + Clone + 'static,
    VS: Serde<VA> + Clone + 'static,
{
    std::sync::Arc::new(
        move |state: &mut LowerState, store_name: &str, proc_name: &str, logging: bool| {
            // The suppress buffer's changelog is a plain compacted KV changelog
            // (the JVM suppress buffer is a compacted KV store) — no retention arg.
            state
                .topology
                .add_suppress_store::<Windowed<K>, VA, SessionWindowedSerde<KS>, VS>(
                    store_name.to_string(),
                    SessionWindowedSerde::new(key_serde.clone()),
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
            windows::{SessionWindowedSerde, SessionWindows, Window, Windowed},
        },
        processor::serde::{Consumed, I64Serde, Produced, StringSerde},
        test_driver::TopologyTestDriver,
    };

    /// Sub-task 3d-ii: an emit-on-update (default) session aggregate store is
    /// marked caching, so with a positive cache budget it lands in `cache_owner`.
    #[test]
    fn emit_on_update_session_store_is_cached() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .windowed_by_session(SessionWindows::of_inactivity_gap(60))
            .count("s");
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            10_485_760,
        ))
        .unwrap();
        check!(
            g.cache_owner.contains_key("s"),
            "emit-on-update session store must be cached, cache_owner = {:?}",
            g.cache_owner
        );
    }

    /// Sub-task 3d-ii: an emit-FINAL session aggregate store must stay UNCACHED —
    /// a cache flush would emit the per-update tombstones+updates emit-final
    /// suppresses. The `emit.is_on_update()` mark guard enforces this.
    #[test]
    fn emit_final_session_store_is_not_cached() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .windowed_by_session(SessionWindows::of_inactivity_gap(60).grace(10))
            .emit_strategy(EmitStrategy::on_window_close())
            .count("s");
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            10_485_760,
        ))
        .unwrap();
        check!(
            !g.cache_owner.contains_key("s"),
            "emit-final session store must NOT be cached, cache_owner = {:?}",
            g.cache_owner
        );
    }

    /// `windowedBy(SessionWindows).emit_strategy(on_window_close).count`: emit-final
    /// (KIP-825) suppresses per-update + merge-tombstone emits and forwards a single
    /// final count for each session only once stream-time advances past its close.
    /// Records a@1 and a@4 (within gap 60) form session `[1,4]` (count 2) silently;
    /// the a@1000 record opens session `[1000,1000]` and closes `[1,4]` (end 4 <=
    /// `window_close_time` = 1000 - grace 10 = 990), emitting exactly one record:
    /// session `[1,4]` with value 2. A grace of 10 keeps the data-defined session
    /// open at its own (inclusive) end until stream-time jumps ahead.
    #[test]
    fn dsl_session_count_emit_final_emits_once_on_close() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .group_by_key()
            .windowed_by_session(SessionWindows::of_inactivity_gap(60).grace(10))
            .emit_strategy(EmitStrategy::on_window_close())
            .count("s")
            .to_stream()
            .to_explicit(
                "out",
                Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde),
            );
        let built = b.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        for ts in [1, 4, 1000] {
            d.pipe_input(
                "in",
                Consumed::with(StringSerde, StringSerde),
                Some("a".to_string()),
                "x".to_string(),
                ts,
            );
        }
        let p = || Produced::with(SessionWindowedSerde::new(StringSerde), I64Serde);
        assert2::assert!(
            d.read_output("out", p())
                == Some((
                    Some(Windowed {
                        key: "a".into(),
                        window: Window { start: 1, end: 4 }
                    }),
                    2
                ))
        );
        // No per-update / merge-tombstone emits and the still-open session
        // [1000,1000] is not emitted.
        assert2::assert!(d.read_output("out", p()) == None);
    }
}
