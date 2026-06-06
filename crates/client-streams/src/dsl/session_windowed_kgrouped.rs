//! `SessionWindowedKGroupedStream<K,V>`: the handle between
//! `KGroupedStream::windowed_by_session(SessionWindows)` and a terminal session
//! aggregation (`count`/`reduce`/`aggregate`). The session analogue of
//! [`crate::dsl::windowed_kgrouped::TimeWindowedKGroupedStream`]: same grouped
//! lineage + the [`SessionWindows`] spec; terminal ops emit `Windowed<K>` keys and
//! materialize a **session store** (`add_session_store`). Result is a
//! `KTable<Windowed<K>, _>` (always logged in this slice).
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::Materialized;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::{KGroupedStream, RepartitionLowerFn, mint_store_name};
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::session_aggregate::{
    KStreamSessionAggregateProcessor, KStreamSessionReduceProcessor,
};
use crate::dsl::windows::{SessionWindows, Windowed};
use crate::processor::serde::Serde;
use crate::topology::NodeHandle;

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
            _pd: PhantomData,
        }
    }

    /// `count`: count records per session → `KTable<Windowed<K>, i64>`.
    pub fn count<KS, VS>(self, materialized: Materialized<KS, VS>) -> KTable<Windowed<K>, i64>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
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
    pub fn aggregate<KS, VS, VA, I, A, M>(
        self,
        init: I,
        agg: A,
        merger: M,
        materialized: Materialized<KS, VS>,
    ) -> KTable<Windowed<K>, VA>
    where
        VA: Any + Send + Sync + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
        M: Fn(&K, VA, VA) -> VA + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        self.lower_aggregate::<KS, VS, VA, I, A, M>(materialized, store_name, init, agg, merger)
    }

    /// `reduce`: combine values per session with `reducer` → `KTable<Windowed<K>, V>`.
    pub fn reduce<KS, VS, R>(
        self,
        reducer: R,
        materialized: Materialized<KS, VS>,
    ) -> KTable<Windowed<K>, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let store_name = mint_store_name(&self.builder, &materialized, names::REDUCE_STORE);
        self.lower_reduce::<KS, VS, R>(materialized, store_name, reducer)
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
    ) -> KTable<Windowed<K>, VA>
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
            ..
        } = materialized;
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let windows = self.windows;
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
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_session_store::<K, VA, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                windows.gap_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), agg_id, Some(store_name), None)
            .with_window_grace(Some(windows.grace_ms))
    }

    #[allow(clippy::too_many_lines)]
    fn lower_reduce<KS, VS, R>(
        mut self,
        materialized: Materialized<KS, VS>,
        store_name: String,
        reducer: R,
    ) -> KTable<Windowed<K>, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        R: Fn(&V, &V) -> V + Clone + Send + Sync + 'static,
    {
        let Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let parent = self.parent;
        let key_changing = self.key_changing_upstream;
        let rp_lower = self.repartition_lower.take();
        let windows = self.windows;
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
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_session_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                windows.gap_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            state.handle_name.insert(red_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), red_id, Some(store_name), None)
            .with_window_grace(Some(windows.grace_ms))
    }
}
