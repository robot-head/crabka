//! KIP-150 cogroup. It aggregates several co-partitioned input streams into one
//! `KTable`. Each stream has its own value type `Vn`, but they share the key `K`
//! and the output type `VOut`, and each input contributes an
//! `Aggregator<K, Vn, VOut>`.
//!
//! The topology holds one aggregate processor per input. All of them write to a
//! single shared store, and they fan into one passthrough merge node that the
//! result `KTable` reads.
//!
//! `KGroupedStream::cogroup` and `CogroupedKStream::cogroup` capture each input's
//! lineage and a **type-erased** `make_agg` thunk that closes over the concrete
//! `Vn` and the aggregator. The terminal `aggregate` and `windowed_by*` supply
//! the shared `Initializer`, and for sessions the `Merger`, as an internal
//! `CogroupSpec`. `lower_cogroup` then records the per-input repartition and
//! aggregate nodes and the merge node, and it registers the shared store exactly
//! once in the merge thunk.

use std::{any::Any, cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

use crate::{
    dsl::{
        builder::InternalStreamsBuilder,
        config::Materialized,
        graph::{GraphNodeKind, LowerState, NodeId},
        kgrouped::{KGroupedStream, RepartitionLowerFn, mint_store_name},
        ktable::KTable,
        names,
        processors::{
            aggregate::KStreamAggregateProcessor, change::Change,
            cogroup_merge::KStreamPassThrough, tuple_forwarder::TupleForwarder,
        },
    },
    processor::serde::{DefaultSerde, Serde},
    topology::NodeHandle,
};

/// Which window flavor the terminal aggregation uses.
///
/// This enum carries the window spec. The shared init, and the session merger,
/// sit beside it in [`CogroupSpec`]. Each cogroup terminal, `aggregate` or
/// `windowed_by*`, builds its own variant, and [`make_agg_for_input`] matches on
/// it.
#[derive(Clone)]
pub(crate) enum CogroupKind {
    NonWindowed,
    Time(crate::dsl::windows::TimeWindows),
    Sliding(crate::dsl::windows::SlidingWindows),
    Session(crate::dsl::windows::SessionWindows),
}

/// The terminal aggregation spec. The code builds it once at `aggregate()` time
/// and clones it per input.
///
/// `init` and `merger` are `Arc`-erased, so a per-input `make_agg` thunk can hold
/// them. Such a thunk does not know the concrete closure type of `VOut`.
type CogroupInitializer<VOut> = Arc<dyn Fn() -> VOut + Send + Sync>;
type CogroupMerger<K, VOut> = Arc<dyn Fn(&K, VOut, VOut) -> VOut + Send + Sync>;

#[allow(dead_code)]
pub(crate) struct CogroupSpec<K, VOut> {
    pub kind: CogroupKind,
    pub init: CogroupInitializer<VOut>,
    pub merger: Option<CogroupMerger<K, VOut>>,
}

impl<K, VOut> Clone for CogroupSpec<K, VOut> {
    fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            init: self.init.clone(),
            merger: self.merger.clone(),
        }
    }
}

/// Given a [`CogroupSpec`], an input returns a node-lowering thunk that adds its
/// per-window aggregate processor wired to `parent_name`, named `proc_name`,
/// pointing at `store_name`, and returns the lowered processor's handle name.
type AggNodeThunk = Box<dyn FnOnce(&mut LowerState, String, String, String) -> String + Send>;
pub(crate) type MakeAggFn<K, VOut> = Box<dyn FnOnce(CogroupSpec<K, VOut>) -> AggNodeThunk + Send>;

/// One cogrouped input: its grouped lineage plus the erased per-window aggregate
/// builder.
#[allow(dead_code)]
pub(crate) struct CogroupInput<K, VOut> {
    pub parent: NodeId,
    pub key_changing_upstream: bool,
    pub repartition_lower: Option<RepartitionLowerFn>,
    pub make_agg: MakeAggFn<K, VOut>,
    /// The single source topic that this input traces to, when the input is not
    /// key-changing. The code uses it to register a copartition group over all
    /// cogroup inputs.
    pub source_topic: Option<String>,
}

/// A handle that collects the cogrouped inputs. The terminal `aggregate` and
/// `windowed_by*` consume it.
///
/// `builder` and `inputs` are `pub(crate)`, so the windowed-handle modules can
/// move the inputs into their own handles. Build this type through the DSL entry
/// points.
pub struct CogroupedKStream<K, VOut> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) inputs: Vec<CogroupInput<K, VOut>>,
    _pd: PhantomData<fn() -> (K, VOut)>,
}

impl<K, VOut> CogroupedKStream<K, VOut> {
    /// Build the handle from the builder and the collected inputs. It keeps the
    /// `_pd` marker private, so no call site depends on the field set.
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        inputs: Vec<CogroupInput<K, VOut>>,
    ) -> Self {
        Self {
            builder,
            inputs,
            _pd: PhantomData,
        }
    }
}

/// Build the erased `make_agg` for one input. It closes over the concrete `Vn`
/// and the aggregator.
///
/// The returned thunk matches on the window kind and attaches the right
/// per-window processor. `KGroupedStream::cogroup` and the chained
/// `CogroupedKStream::cogroup` share this function.
pub(crate) fn make_agg_for_input<K, Vn, VOut, A>(agg: A) -> MakeAggFn<K, VOut>
where
    K: Any + Send + Sync + Clone,
    Vn: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
    A: Fn(&K, &Vn, VOut) -> VOut + Send + Sync + 'static,
{
    let agg = Arc::new(agg);
    Box::new(move |spec: CogroupSpec<K, VOut>| -> AggNodeThunk {
        Box::new(
            move |state: &mut LowerState,
                  parent_name: String,
                  proc_name: String,
                  store_name: String|
                  -> String {
                let parent = NodeHandle::<K, Vn>::from_name(parent_name);
                let init = spec.init.clone();
                match spec.kind {
                    CogroupKind::NonWindowed => {
                        let agg = agg.clone();
                        let store = store_name.clone();
                        let h = state
                            .topology
                            .add_processor::<K, Vn, K, Change<VOut>, _, _, _>(
                                proc_name,
                                move || KStreamAggregateProcessor {
                                    store_name: store.clone(),
                                    init: {
                                        let i = init.clone();
                                        move || i()
                                    },
                                    agg: {
                                        let a = agg.clone();
                                        move |k: &K, v: &Vn, acc: VOut| a(k, v, acc)
                                    },
                                    forwarder: TupleForwarder::default(),
                                    _pd: PhantomData,
                                },
                                [parent],
                            );
                        h.name().to_string()
                    }
                    CogroupKind::Time(w) => {
                        use crate::dsl::{
                            processors::window_aggregate::KStreamWindowAggregateProcessor,
                            windows::Windowed,
                        };
                        let agg = agg.clone();
                        let store = store_name.clone();
                        let h = state
                            .topology
                            .add_processor::<K, Vn, Windowed<K>, Change<VOut>, _, _, _>(
                                proc_name,
                                move || KStreamWindowAggregateProcessor {
                                    store_name: store.clone(),
                                    windows: w,
                                    init: {
                                        let i = init.clone();
                                        move || i()
                                    },
                                    agg: {
                                        let a = agg.clone();
                                        move |k: &K, v: &Vn, acc: VOut| a(k, v, acc)
                                    },
                                    // Cogroup does not expose emit-final; default to emit-on-update.
                                    emit: crate::dsl::emit::EmitStrategy::default(),
                                    stream_time: i64::MIN,
                                    last_emitted_close: i64::MIN,
                                    forwarder: TupleForwarder::default(),
                                    _pd: PhantomData,
                                },
                                [parent],
                            );
                        h.name().to_string()
                    }
                    CogroupKind::Sliding(w) => {
                        use crate::dsl::{
                            processors::sliding_window_aggregate::KStreamSlidingWindowAggregateProcessor,
                            windows::Windowed,
                        };
                        let agg = agg.clone();
                        let store = store_name.clone();
                        let h = state
                            .topology
                            .add_processor::<K, Vn, Windowed<K>, Change<VOut>, _, _, _>(
                                proc_name,
                                move || KStreamSlidingWindowAggregateProcessor {
                                    store_name: store.clone(),
                                    windows: w,
                                    init: {
                                        let i = init.clone();
                                        move || i()
                                    },
                                    agg: {
                                        let a = agg.clone();
                                        move |k: &K, v: &Vn, acc: VOut| a(k, v, acc)
                                    },
                                    stream_time: i64::MIN,
                                    // Cogroup does not expose emit-final; default to emit-on-update.
                                    emit: crate::dsl::emit::EmitStrategy::default(),
                                    last_emitted_close: i64::MIN,
                                    forwarder: TupleForwarder::default(),
                                    _pd: PhantomData,
                                },
                                [parent],
                            );
                        h.name().to_string()
                    }
                    CogroupKind::Session(w) => {
                        use crate::dsl::{
                            processors::session_aggregate::KStreamSessionAggregateProcessor,
                            windows::Windowed,
                        };
                        let agg = agg.clone();
                        let store = store_name.clone();
                        let merger = spec
                            .merger
                            .clone()
                            .expect("session cogroup requires a merger");
                        let h = state
                            .topology
                            .add_processor::<K, Vn, Windowed<K>, Change<VOut>, _, _, _>(
                                proc_name,
                                move || KStreamSessionAggregateProcessor {
                                    store_name: store.clone(),
                                    gap: w.gap,
                                    init: {
                                        let i = init.clone();
                                        move || i()
                                    },
                                    agg: {
                                        let a = agg.clone();
                                        move |k: &K, v: &Vn, acc: VOut| a(k, v, acc)
                                    },
                                    merger: {
                                        let m = merger.clone();
                                        move |k: &K, a: VOut, b: VOut| m(k, a, b)
                                    },
                                    // Cogroup does not expose emit-final; default to emit-on-update.
                                    emit: crate::dsl::emit::EmitStrategy::default(),
                                    grace: w.grace,
                                    stream_time: i64::MIN,
                                    last_emitted_close: i64::MIN,
                                    forwarder: TupleForwarder::default(),
                                    _pd: PhantomData,
                                },
                                [parent],
                            );
                        h.name().to_string()
                    }
                }
            },
        )
    })
}

impl<K, VOut> CogroupedKStream<K, VOut>
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
{
    /// Chain another co-partitioned input with its own aggregator.
    #[must_use]
    pub fn cogroup<Vn, A>(mut self, grouped: KGroupedStream<K, Vn>, agg: A) -> Self
    where
        Vn: Any + Send + Sync + Clone,
        A: Fn(&K, &Vn, VOut) -> VOut + Send + Sync + 'static,
    {
        let (parent, key_changing, rp_lower, source_topic) = grouped.into_cogroup_parts();
        self.inputs.push(CogroupInput {
            parent,
            key_changing_upstream: key_changing,
            repartition_lower: rp_lower,
            make_agg: make_agg_for_input::<K, Vn, VOut, A>(agg),
            source_topic,
        });
        self
    }

    /// Non-windowed terminal aggregation, which gives a `KTable<K, VOut>`.
    pub fn aggregate_explicit<KS, VS, I>(
        self,
        init: I,
        materialized: impl Into<Materialized<KS, VS>>,
    ) -> KTable<K, VOut, KS, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<VOut> + Clone + 'static,
        I: Fn() -> VOut + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let store_name = mint_store_name(&self.builder, &materialized, names::AGGREGATE_STORE);
        let Materialized {
            key_serde,
            value_serde,
            logging,
            caching,
            ..
        } = materialized;
        let spec = CogroupSpec::<K, VOut> {
            kind: CogroupKind::NonWindowed,
            init: Arc::new(init),
            merger: None,
        };
        let ks = key_serde.clone();
        let vs = value_serde.clone();
        let store_for_reg = store_name.clone();
        // Store registrar: a non-windowed KV store, honoring Materialized logging.
        let registrar: StoreRegistrarFn = Box::new(move |state, procs| {
            if logging {
                state.topology.add_state_store::<K, VOut, KS, VS>(
                    store_for_reg.clone(),
                    ks.clone(),
                    vs.clone(),
                    procs,
                );
            } else {
                state
                    .topology
                    .add_state_store_no_changelog::<K, VOut, KS, VS>(
                        store_for_reg.clone(),
                        ks.clone(),
                        vs.clone(),
                    );
            }
            // Per-input aggregators suppress their immediate forward when this
            // store is cached; the merge passthrough then only relays the cache
            // flush's deduped change. Honor Materialized::with_caching.
            state.topology.mark_store_caching(&store_for_reg, caching);
        });
        let suppress = crate::dsl::ktable::kv_suppress_factory::<K, VOut, KS, VS>(
            key_serde.clone(),
            value_serde.clone(),
        );
        let merge_id = lower_cogroup::<K, VOut, K>(
            &self.builder,
            self.inputs,
            &store_name,
            &spec,
            logging,
            registrar,
        );
        KTable::new(
            Rc::clone(&self.builder),
            merge_id,
            Some(store_name),
            None,
            key_serde,
            value_serde,
        )
        .with_suppress_factory(Some(suppress))
    }

    /// Non-windowed terminal aggregation with default serdes.
    pub fn aggregate<I>(
        self,
        init: I,
        store_name: impl Into<String>,
    ) -> KTable<K, VOut, <K as DefaultSerde>::Serde, <VOut as DefaultSerde>::Serde>
    where
        K: DefaultSerde,
        VOut: DefaultSerde,
        <K as DefaultSerde>::Serde: Serde<K> + Clone,
        <VOut as DefaultSerde>::Serde: Serde<VOut> + Clone,
        I: Fn() -> VOut + Send + Sync + 'static,
    {
        self.aggregate_explicit(
            init,
            Materialized::with(
                <K as DefaultSerde>::Serde::default(),
                <VOut as DefaultSerde>::Serde::default(),
            )
            .as_store(store_name),
        )
    }
}

/// Registers the shared cogroup store with the given per-input processor names.
/// It is boxed, so each terminal supplies its own window-specific store type and
/// serdes.
pub(crate) type StoreRegistrarFn = Box<dyn FnOnce(&mut LowerState, Vec<String>) + Send>;

/// Record the nodes in id order: each input's optional repartition, then its
/// aggregate node against the shared store, and last the merge node.
///
/// The merge thunk attaches the passthrough processor, whose parents are all the
/// aggregate handles, and runs `registrar` once to register the shared store. The
/// function returns the merge node id, which is the result `KTable`'s source. It
/// is generic over the merge output key `KOut`, which is `K` when non-windowed
/// and `Windowed<K>` when windowed.
pub(crate) fn lower_cogroup<K, VOut, KOut>(
    builder: &Rc<RefCell<InternalStreamsBuilder>>,
    inputs: Vec<CogroupInput<K, VOut>>,
    store_name: &str,
    spec: &CogroupSpec<K, VOut>,
    logging: bool,
    registrar: StoreRegistrarFn,
) -> NodeId
where
    K: Any + Send + Sync + Clone,
    VOut: Any + Send + Sync + Clone,
    KOut: Any + Send + Clone,
{
    let mut g = builder.borrow_mut();
    let mut agg_ids: Vec<NodeId> = Vec::with_capacity(inputs.len());
    // Collect source topics for the copartition group declaration (only include
    // inputs whose lineage traces to a single non-repartitioned source topic).
    let copartition_sources: Vec<String> = inputs
        .iter()
        .filter_map(|i| i.source_topic.clone())
        .collect();
    for input in inputs {
        let CogroupInput {
            parent,
            key_changing_upstream,
            repartition_lower,
            make_agg,
            source_topic: _,
        } = input;
        let agg_parent = KGroupedStream::<K, ()>::record_repartition(
            &mut g,
            store_name,
            parent,
            key_changing_upstream,
            repartition_lower,
        );
        let proc_name = g.new_processor_name(names::COGROUP_AGGREGATE);
        let agg_id = g.graph.add(
            proc_name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.to_string(),
                // Per-input nodes share the store; only the merge node owns the
                // changelog (set `changelog: logging` below). The actual store
                // registration happens once, in the merge thunk via `registrar`.
                changelog: false,
            },
            vec![agg_parent],
        );
        let thunk = make_agg(spec.clone());
        let store_for = store_name.to_string();
        let pn = proc_name.clone();
        g.graph.nodes[agg_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&agg_parent].clone();
            let handle = thunk(state, parent_name, pn, store_for);
            state.handle_name.insert(agg_id, handle);
        }));
        agg_ids.push(agg_id);
    }

    let merge_name = g.new_processor_name(names::COGROUP_MERGE);
    let merge_id = g.graph.add(
        merge_name.clone(),
        GraphNodeKind::Aggregate {
            store_name: store_name.to_string(),
            changelog: logging,
        },
        agg_ids.clone(),
    );
    g.graph.nodes[merge_id].lower = Some(Box::new(move |state: &mut LowerState| {
        let parents: Vec<NodeHandle<KOut, Change<VOut>>> = agg_ids
            .iter()
            .map(|id| NodeHandle::<KOut, Change<VOut>>::from_name(state.handle_name[id].clone()))
            .collect();
        let h = state
            .topology
            .add_processor::<KOut, Change<VOut>, KOut, Change<VOut>, _, _, _>(
                merge_name.clone(),
                || KStreamPassThrough::<KOut, Change<VOut>> { _pd: PhantomData },
                parents,
            );
        let proc_names: Vec<String> = agg_ids
            .iter()
            .map(|id| state.handle_name[id].clone())
            .collect();
        registrar(state, proc_names);
        // Declare the copartition group when all inputs trace to a single source
        // topic (i.e. none needed a repartition). The JVM cogroup does the same.
        if copartition_sources.len() >= 2 {
            state
                .topology
                .add_copartition_group(copartition_sources.clone());
        }
        state.handle_name.insert(merge_id, h.name().to_string());
    }));
    // Release the RefCell borrow so `KTable::new` can borrow the builder again.
    drop(g);
    merge_id
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use crate::dsl::StreamsBuilder;

    /// The cogroup store is record-cached when the budget is positive and
    /// caching is on, which is the default. Each per-input aggregator suppresses
    /// its immediate forward when the store is cached. The merge passthrough
    /// relays only the deduped flush change, so there is no double emit.
    #[test]
    fn cogroup_store_is_cached_with_positive_budget() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate(|| 0i64, "co");
        let built = b.build("app").unwrap();

        let g = pollster::block_on(built.instantiate(
            &crate::store::backend::StoreBackend::InMemory,
            "app",
            // A generous (default-sized) budget: a marked cogroup store must land
            // in cache_owner.
            mebibytes(10),
        ))
        .unwrap();
        check!(
            g.cache_owner.contains_key("co"),
            "cogroup store must be cached when budget > 0 and caching enabled, \
             cache_owner = {:?}",
            g.cache_owner
        );
    }
}

#[cfg(test)]
mod cogroup_caching_tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use crate::{
        I64Serde, Materialized, Produced, StringSerde, dsl::StreamsBuilder,
        store::backend::StoreBackend,
    };

    /// Two co-grouped inputs aggregate into one cached KV store.
    ///
    /// Within a single batch, in1 adds `len(value)` and in2 adds 1. The cached
    /// store is marked, so `cache_owner` is rooted, and both per-input forwards
    /// are suppressed. The flush then emits ONE deduped record. Its value,
    /// 3 = 2 + 1, proves that in2's aggregator read in1's buffered accumulator,
    /// which is cross-input read-your-writes.
    #[test]
    fn cogroup_caches_marks_and_dedups_cross_input() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde).as_store("cg"),
        )
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let mut g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
                .unwrap();
        check!(g.cache_owner.contains_key("cg"));
        pollster::block_on(g.init_processors()).unwrap();

        // in1: key "a" value "xx" (len 2) → acc 2 ; in2: key "a" value "z" → acc 3.
        pollster::block_on(g.pipe("in1", Some(b"a"), b"xx", 0)).unwrap();
        pollster::block_on(g.pipe("in2", Some(b"a"), b"z", 1)).unwrap();
        // Both per-input forwards suppressed until flush.
        check!(g.take_output().is_empty());

        pollster::block_on(g.flush_caches()).unwrap();
        let out = g.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out");
        // 3 = in1(+2) then in2(+1) — in2 read in1's buffered accumulator.
        check!(out[0].value.as_ref().unwrap().as_ref() == 3i64.to_be_bytes());
    }

    /// `with_caching(false)` keeps the cogroup store uncached even with a
    /// budget.
    #[test]
    fn cogroup_uncached_when_caching_off() {
        let b = StreamsBuilder::new();
        let g1 = b.stream::<String, String>(["in1"]).group_by_key();
        let g2 = b.stream::<String, String>(["in2"]).group_by_key();
        g1.cogroup::<i64, _>(|_k, v: &String, acc| {
            acc + i64::try_from(v.len()).unwrap_or(i64::MAX)
        })
        .cogroup(g2, |_k, _v: &String, acc| acc + 1)
        .aggregate_explicit(
            || 0i64,
            Materialized::with(StringSerde, I64Serde)
                .as_store("cg")
                .with_caching(false),
        )
        .to_stream()
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
            .unwrap();
        check!(!g.cache_owner.contains_key("cg"));
    }
}
