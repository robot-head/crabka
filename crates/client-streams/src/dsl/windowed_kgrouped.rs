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

use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::Materialized;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::{KGroupedStream, RepartitionLowerFn, mint_store_name};
use crate::dsl::ktable::KTable;
use crate::dsl::ktable::SuppressStoreFactory;
use crate::dsl::names;
use crate::dsl::processors::window_aggregate::{
    KStreamWindowAggregateProcessor, KStreamWindowReduceProcessor,
};
use crate::dsl::windows::{TimeWindowedSerde, TimeWindows, Windowed};
use crate::processor::serde::Serde;
use crate::topology::NodeHandle;

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
            _pd: PhantomData,
        }
    }

    /// `count`: count records per (key, window) into a windowed
    /// `KTable<Windowed<K>, i64>`. `init = || 0`, `agg = |_k, _v, acc| acc + 1`.
    pub fn count<KS, VS>(self, materialized: Materialized<KS, VS>) -> KTable<Windowed<K>, i64>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<i64> + Clone + 'static,
    {
        self.aggregate_inner_windowed(
            materialized,
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
        self.lower_reduce_windowed::<KS, VS, R>(materialized, store_name, reducer)
    }

    /// `aggregate`: general windowed aggregation with caller-supplied `init` +
    /// `agg`, materialized as `KTable<Windowed<K>, VA>`.
    pub fn aggregate<KS, VS, VA, I, A>(
        self,
        init: I,
        agg: A,
        materialized: Materialized<KS, VS>,
    ) -> KTable<Windowed<K>, VA>
    where
        VA: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VA> + Clone + 'static,
        I: Fn() -> VA + Clone + Send + Sync + 'static,
        A: Fn(&K, &V, VA) -> VA + Clone + Send + Sync + 'static,
    {
        self.aggregate_inner_windowed(materialized, names::AGGREGATE_STORE, init, agg)
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
    ) -> KTable<Windowed<K>, VA>
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
    ) -> KTable<Windowed<K>, VA>
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
                        _pd: PhantomData,
                    },
                    [parent],
                );
            // Windowed stores carry a changelog so they can be restored by the runtime.
            state.topology.add_window_store::<K, VA, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                windows.size_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            state.handle_name.insert(agg_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), agg_id, Some(store_name), None)
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
        let suppress_factory = windowed_suppress_factory::<K, V, KS, VS>(
            key_serde.clone(),
            value_serde.clone(),
            self.windows,
        );
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
            // The windowed reduce emits `Windowed<K>` keys + `Change<V>`.
            let h = state
                .topology
                .add_processor::<K, V, Windowed<K>, crate::dsl::processors::change::Change<V>, _, _, _>(
                    red_name.clone(),
                    move || KStreamWindowReduceProcessor {
                        store_name: store_for_proc.clone(),
                        windows,
                        reducer: reducer.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_window_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                windows.size_ms,
                windows.grace_ms,
                [h.name().to_string()],
            );
            state.handle_name.insert(red_id, h.name().to_string());
        }));

        drop(g);
        KTable::new(Rc::clone(&self.builder), red_id, Some(store_name), None)
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
