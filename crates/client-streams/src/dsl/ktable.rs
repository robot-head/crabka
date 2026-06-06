//! `KTable<K,V>`: a materialized, changelog-backed table view. Produced by a
//! terminal aggregation (`count`/`reduce`/`aggregate`) or by
//! [`StreamsBuilder::table`], and convertible back to a `KStream` via
//! [`KTable::to_stream`].
//!
//! Each op records a logical node + a lowering thunk in the same style as
//! [`crate::dsl::kstream::KStream`]: reconstruct the parent handle from
//! `LowerState`, perform the typed Processor-API call, record the resulting node
//! name. Materialized ops (`map_values`/`filter`) also register a state store.
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kstream::KStream;
use crate::dsl::names;
use crate::dsl::processors::change::Change;
use crate::dsl::processors::ktable_join::{
    JoinKind, KTableKTableJoinOtherProcessor, KTableKTableJoinThisProcessor,
};
use crate::dsl::processors::stateless::MergeProcessor;
use crate::dsl::processors::table::{
    KTableFilterProcessor, KTableMapValuesProcessor, KTableMapValuesViewProcessor,
    KTableToStreamProcessor,
};
use crate::processor::serde::Serde;
use crate::topology::NodeHandle;

/// A serde-carrying closure that registers a `SuppressBytesStore` for a `suppress`
/// node during lowering. Attached to a `KTable` by the producing op (windowed/
/// session aggregation or `builder.table`), which alone knows the concrete serdes.
///
/// Called as `factory(state, store_name, processor_name, logging)`: it registers
/// the suppress store (with the captured serdes) under `store_name`, connected to
/// `processor_name`, with the changelog gated by `logging`. Type-erased (the
/// concrete `K`/`V`/serdes are baked into the closure) so the `KTable` field is
/// non-generic. `Arc` + `Send + Sync` because the lowering thunk that clones it in
/// is itself `Send` (the captured serdes are `Send + Sync`: the `Serde` supertrait).
pub(crate) type SuppressStoreFactory = Arc<dyn Fn(&mut LowerState, &str, &str, bool) + Send + Sync>;

/// Build a non-windowed [`SuppressStoreFactory`] from a table's key/value serdes
/// (plain aggregations + `builder.table`). Registers a `SuppressBytesStore<K, V>`
/// with the JVM 1-day default changelog retention. (Windowed/session aggregations
/// use their own factories wrapping `TimeWindowedSerde`/`SessionWindowedSerde`.)
pub(crate) fn kv_suppress_factory<K, V, KS, VS>(
    key_serde: KS,
    value_serde: VS,
) -> SuppressStoreFactory
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
    KS: Serde<K> + Clone + 'static,
    VS: Serde<V> + Clone + 'static,
{
    Arc::new(
        move |state: &mut LowerState, store_name: &str, proc_name: &str, logging: bool| {
            state.topology.add_suppress_store::<K, V, KS, VS>(
                store_name.to_string(),
                key_serde.clone(),
                value_serde.clone(),
                logging,
                [proc_name.to_string()],
            );
        },
    )
}

/// A changelog-backed table handle. `store_name` is the materialized store this
/// table reads/writes (used to derive changelog topics + reuse the store in
/// downstream materialized ops). `source_topic` is the Kafka topic this table
/// was sourced from (set for `builder.table()` `KTables`; `None` for derived
/// `KTables`). Used by the join DSL to declare copartition groups.
pub struct KTable<K, V> {
    builder: Rc<RefCell<InternalStreamsBuilder>>,
    node: NodeId,
    #[allow(dead_code)]
    store_name: Option<String>,
    #[allow(dead_code)]
    source_topic: Option<String>,
    /// For windowed tables: the upstream window's grace (suppress closes a window
    /// at `window.end + window_grace_ms`). `None` for non-windowed tables.
    window_grace_ms: Option<i64>,
    /// Set by serde-carrying producers (aggregations, `builder.table`); read by
    /// `suppress` to register its store with the right serdes. `None` on derived
    /// tables whose value type changed (`map_values`) — `suppress` then panics.
    suppress_store_factory: Option<SuppressStoreFactory>,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> KTable<K, V> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        store_name: Option<String>,
        source_topic: Option<String>,
    ) -> Self {
        Self {
            builder,
            node,
            store_name,
            source_topic,
            window_grace_ms: None,
            suppress_store_factory: None,
            _pd: PhantomData,
        }
    }

    /// The name of the materialized state store backing this table, if any.
    #[allow(dead_code)]
    pub(crate) fn store_name(&self) -> Option<&str> {
        self.store_name.as_deref()
    }

    /// The Kafka source topic this table was sourced from (`builder.table()`),
    /// or `None` for derived `KTables` (aggregations, `map_values`, `filter`).
    #[allow(dead_code)]
    pub(crate) fn source_topic(&self) -> Option<&str> {
        self.source_topic.as_deref()
    }

    /// Tag this table with its upstream window's grace (set by windowed/session
    /// aggregations; propagated through `Change`-preserving ops). Read by `suppress`
    /// (which accesses the `window_grace_ms` field directly).
    #[must_use]
    pub(crate) fn with_window_grace(mut self, grace_ms: Option<i64>) -> Self {
        self.window_grace_ms = grace_ms;
        self
    }

    /// Attach (or propagate) the serde-carrying suppress-store factory. Set by
    /// aggregations / `builder.table`; propagated through value-preserving ops
    /// (`filter`, `suppress` itself). Read by `suppress`.
    #[must_use]
    pub(crate) fn with_suppress_factory(mut self, factory: Option<SuppressStoreFactory>) -> Self {
        self.suppress_store_factory = factory;
        self
    }
}

impl<K, V> KTable<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    /// `toStream`: view the table's change-stream as a `KStream`, forwarding
    /// every record unchanged. Not key-changing.
    #[must_use]
    pub fn to_stream(&self) -> KStream<K, V> {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_TOSTREAM);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            // Parent (a KTable node) forwards Change<V>; to_stream extracts new.
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, Change<V>, K, V, _, _, _>(
                name.clone(),
                || KTableToStreamProcessor { _pd: PhantomData },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KStream::new(Rc::clone(&self.builder), id)
    }

    /// `mapValues`: transform each value and forward the rewritten table view
    /// **without materializing** a store (the JVM's non-materialized
    /// `mapValues`). Key unchanged; emits no changelog topic. Use
    /// [`map_values_materialized`](Self::map_values_materialized) for the
    /// store-backed form.
    pub fn map_values<V2, F>(&self, f: F) -> KTable<K, V2>
    where
        V2: Any + Send + Clone,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
    {
        let grace = self.window_grace_ms;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_MAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            // Parent forwards Change<V>; the view maps both sides to Change<V2>.
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<V2>, _, _, _>(
                    name.clone(),
                    move || KTableMapValuesViewProcessor {
                        f: f2.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, None, None).with_window_grace(grace)
    }

    /// `mapValues`: transform each value, materializing the rewritten table into
    /// a new store. Key unchanged.
    pub fn map_values_materialized<V2, KS, VS, F>(
        &self,
        f: F,
        materialized: crate::dsl::config::Materialized<KS, VS>,
    ) -> KTable<K, V2>
    where
        V2: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V2> + Clone + 'static,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
    {
        let grace = self.window_grace_ms;
        let store_name = mint_table_store(&self.builder, &materialized, names::TABLE_MAPVALUES);
        let crate::dsl::config::Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_MAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor {
                store_name: Some(store_name.clone()),
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            // Parent forwards Change<V>; materialized map maps both sides to V2.
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<V2>, _, _, _>(
                    name.clone(),
                    move || KTableMapValuesProcessor {
                        f: f2.clone(),
                        store_name: store_for_proc.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_state_store::<K, V2, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                [h.name().to_string()],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, Some(store_name), None).with_window_grace(grace)
    }

    /// `filter`: keep rows matching `predicate`, materializing the filtered view.
    /// A row that previously matched but stops matching is removed from the store
    /// and forwarded as a `Change<V>` tombstone so downstream views drop it (see
    /// the processor module doc).
    #[must_use]
    pub fn filter<KS, VS, P>(
        &self,
        predicate: P,
        materialized: crate::dsl::config::Materialized<KS, VS>,
    ) -> KTable<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        P: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let grace = self.window_grace_ms;
        // filter preserves V → suppress can still register a store with the same
        // serdes; propagate the factory.
        let suppress_factory = self.suppress_store_factory.clone();
        let store_name = mint_table_store(&self.builder, &materialized, names::TABLE_FILTER);
        let crate::dsl::config::Materialized {
            key_serde,
            value_serde,
            ..
        } = materialized;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_FILTER);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor {
                store_name: Some(store_name.clone()),
            },
            vec![parent_id],
        );
        let p2 = predicate.clone();
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            // Parent forwards Change<V>; filter re-applies the predicate to both
            // sides and forwards Change<V> (emitting tombstones).
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<V>, _, _, _>(
                    name.clone(),
                    move || KTableFilterProcessor {
                        predicate: p2.clone(),
                        store_name: store_for_proc.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_state_store::<K, V, KS, VS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                [h.name().to_string()],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, Some(store_name), None)
            .with_window_grace(grace)
            .with_suppress_factory(suppress_factory)
    }

    /// `join` (inner KTable-KTable join): for each key, the join row exists only
    /// when **both** tables hold a value. On any change to either side, the join
    /// re-reads the other side's current value from its store and forwards a
    /// `Change<VR>` (a tombstone when the row stops existing).
    ///
    /// Both tables must be materialized — the join reads each side's store. The
    /// two source topics are declared as a copartition group (KIP-1071).
    pub fn join<VB, VR, F>(&self, other: &KTable<K, VB>, joiner: F) -> KTable<K, VR>
    where
        VB: Any + Send + Clone,
        VR: Any + Send + Clone,
        F: Fn(&V, &VB) -> VR + Clone + Send + Sync + 'static,
    {
        // Inner: both sides required → the outer-form joiner only ever sees `Some`.
        let jf = move |a: Option<&V>, b: Option<&VB>| {
            joiner(
                a.expect("inner join: a present"),
                b.expect("inner join: b present"),
            )
        };
        self.join_impl(other, jf, JoinKind::inner())
    }

    /// `leftJoin` (left KTable-KTable join): emits a row whenever the **left**
    /// (this) side is present; the right side is optional (the joiner receives
    /// `None` for it on a miss).
    pub fn left_join<VB, VR, F>(&self, other: &KTable<K, VB>, joiner: F) -> KTable<K, VR>
    where
        VB: Any + Send + Clone,
        VR: Any + Send + Clone,
        F: Fn(&V, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
    {
        let jf = move |a: Option<&V>, b: Option<&VB>| joiner(a.expect("left join: a present"), b);
        self.join_impl(other, jf, JoinKind::left())
    }

    /// `outerJoin` (outer KTable-KTable join): emits a row whenever **either**
    /// side is present; the joiner receives `Option` for each side.
    pub fn outer_join<VB, VR, F>(&self, other: &KTable<K, VB>, joiner: F) -> KTable<K, VR>
    where
        VB: Any + Send + Clone,
        VR: Any + Send + Clone,
        F: Fn(Option<&V>, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
    {
        self.join_impl(other, joiner, JoinKind::outer())
    }

    /// Shared lowering for KTable-KTable inner/left/outer joins.
    ///
    /// Records three logical nodes and their thunks:
    /// - `KTABLE-JOINTHIS-` (fed by this table's node): reads the OTHER (`b`)
    ///   store, applies the join, forwards `Change<VR>`.
    /// - `KTABLE-JOINOTHER-` (fed by the other table's node): reads the OTHER
    ///   (`a`) store, applies the join, forwards `Change<VR>`.
    /// - `KTABLE-MERGE-` (fed by both join nodes): forwards each `Change<VR>`
    ///   unchanged, unioning the two join outputs.
    ///
    /// Each join node is connected to the store it reads (so the lowering pulls it
    /// into the same subtopology as that store's owning table source). When both
    /// tables are single-source-topic tables, their source topics are declared as
    /// a copartition group.
    #[allow(clippy::too_many_lines)]
    fn join_impl<VB, VR, JF>(&self, other: &KTable<K, VB>, jf: JF, kind: JoinKind) -> KTable<K, VR>
    where
        VB: Any + Send + Clone,
        VR: Any + Send + Clone,
        JF: Fn(Option<&V>, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
    {
        let a_store = self
            .store_name()
            .expect("KTable-KTable join: left table must be materialized")
            .to_string();
        let b_store = other
            .store_name()
            .expect("KTable-KTable join: right table must be materialized")
            .to_string();
        let a_src = self.source_topic().map(str::to_string);
        let b_src = other.source_topic().map(str::to_string);
        let self_node = self.node;
        let other_node = other.node;

        let mut g = self.builder.borrow_mut();
        let join_this = g.new_processor_name(names::KTABLE_JOIN_THIS);
        let join_other = g.new_processor_name(names::KTABLE_JOIN_OTHER);
        let merge = g.new_processor_name(names::KTABLE_MERGE);

        // ── "this" side: fed by this table, reads the OTHER (b) store ──────────
        let this_id = g.graph.add(
            join_this.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![self_node],
        );
        let b_store_this = b_store.clone();
        let jf_this = jf.clone();
        let join_this_name = join_this.clone();
        g.graph.nodes[this_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&self_node].clone());
            let store_for_proc = b_store_this.clone();
            let jf_for_proc = jf_this.clone();
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<VR>, _, _, _>(
                    join_this_name.clone(),
                    move || KTableKTableJoinThisProcessor {
                        other_store: store_for_proc.clone(),
                        joiner: jf_for_proc.clone(),
                        kind,
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state
                .topology
                .connect_processor_store(h.name(), &b_store_this);
            state.handle_name.insert(this_id, h.name().to_string());
        }));

        // ── "other" side: fed by the other table, reads the OTHER (a) store ────
        let other_id = g.graph.add(
            join_other.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![other_node],
        );
        let a_store_other = a_store.clone();
        let jf_other = jf.clone();
        let join_other_name = join_other.clone();
        g.graph.nodes[other_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<VB>>::from_name(state.handle_name[&other_node].clone());
            let store_for_proc = a_store_other.clone();
            let jf_for_proc = jf_other.clone();
            let h = state
                .topology
                .add_processor::<K, Change<VB>, K, Change<VR>, _, _, _>(
                    join_other_name.clone(),
                    move || KTableKTableJoinOtherProcessor {
                        other_store: store_for_proc.clone(),
                        joiner: jf_for_proc.clone(),
                        kind,
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state
                .topology
                .connect_processor_store(h.name(), &a_store_other);
            state.handle_name.insert(other_id, h.name().to_string());
        }));

        // ── merge: union the two join outputs (forwards Change<VR> unchanged) ──
        let merge_id = g.graph.add(
            merge.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![this_id, other_id],
        );
        g.graph.nodes[merge_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let this_parent =
                NodeHandle::<K, Change<VR>>::from_name(state.handle_name[&this_id].clone());
            let other_parent =
                NodeHandle::<K, Change<VR>>::from_name(state.handle_name[&other_id].clone());
            let h = state
                .topology
                .add_processor::<K, Change<VR>, K, Change<VR>, _, _, _>(
                    merge.clone(),
                    || MergeProcessor::<K, Change<VR>> { _pd: PhantomData },
                    [this_parent, other_parent],
                );
            // Declare the copartition group (when both tables are single-source).
            if let (Some(a), Some(bb)) = (&a_src, &b_src) {
                state
                    .topology
                    .add_copartition_group([a.clone(), bb.clone()]);
            }
            state.handle_name.insert(merge_id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), merge_id, None, None)
    }
}

impl<K, V> KTable<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    /// `suppress(Suppressed)`: buffer updates and emit on a delay. `until_window_closes`
    /// (windowed tables) emits each window's final value once it closes;
    /// `until_time_limit` rate-limits any table to one update per key per wait.
    ///
    /// The buffer is a registered [`SuppressBytesStore`](crate::store::suppress_store)
    /// — durable (changelog + restore) when `logging` is on. The serdes come from
    /// the producing op's [`SuppressStoreFactory`]; calling `suppress` on a table
    /// that changed its value type (`map_values`) panics (no serde factory).
    #[must_use]
    pub fn suppress(&self, suppressed: crate::dsl::suppress::Suppressed<K>) -> KTable<K, V> {
        let wait_ms = match suppressed.wait {
            crate::dsl::suppress::WaitKind::UpstreamGrace => self.window_grace_ms.unwrap_or(0),
            crate::dsl::suppress::WaitKind::Fixed(ms) => ms,
        };
        let buffer_time = suppressed.buffer_time;
        let max_records = suppressed.buffer.record_cap();
        let max_bytes = suppressed.buffer.byte_cap();
        let emit_early = suppressed.buffer.is_emit_early();
        let logging = suppressed.logging;
        // The serde-carrying factory that registers the suppress store. Required:
        // suppress needs the table's serdes to (de)serialize the buffered changes.
        let factory = self.suppress_store_factory.clone().expect(
            "suppress requires a serde-carrying KTable (a windowed/session aggregation \
             or builder.table); a mapValues-derived view has no value serde for the buffer",
        );
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KTABLE_SUPPRESS);
        // The JVM mints the buffer store via `newStoreName(SUPPRESS_NAME)` right after
        // the processor name → `KTABLE-SUPPRESS-STATE-STORE-<index+1>` (consecutive).
        let store_name = g.new_processor_name(names::KTABLE_SUPPRESS_STORE);
        let store_for_thunk = store_name.clone();
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<V>, _, _, _>(
                    name.clone(),
                    move || {
                        crate::dsl::processors::suppress::KTableSuppressProcessor::<K, V>::new(
                            store_for_proc.clone(),
                            wait_ms,
                            buffer_time,
                            max_records,
                            max_bytes,
                            emit_early,
                        )
                    },
                    [parent],
                );
            let proc_name = h.name().to_string();
            // Register the suppress store (with the producer's serdes), connected to
            // this processor; `logging` gates whether a changelog topic is emitted.
            factory(state, &store_for_thunk, &proc_name, logging);
            state.handle_name.insert(id, proc_name);
        }));
        drop(g);
        // suppress preserves K/V → propagate the grace + factory so a downstream
        // suppress/filter can register against the same serdes.
        KTable::new(Rc::clone(&self.builder), id, Some(store_name), None)
            .with_window_grace(self.window_grace_ms)
            .with_suppress_factory(self.suppress_store_factory.clone())
    }
}

/// Mint a materialized table store name: the `Materialized` name when present,
/// else a fresh counter at the JVM position.
fn mint_table_store<KS, VS>(
    builder: &Rc<RefCell<InternalStreamsBuilder>>,
    materialized: &crate::dsl::config::Materialized<KS, VS>,
    prefix: &str,
) -> String {
    match &materialized.store_name {
        Some(name) => name.clone(),
        None => builder.borrow_mut().new_processor_name(prefix),
    }
}
