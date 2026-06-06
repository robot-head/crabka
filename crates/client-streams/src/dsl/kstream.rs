//! `KStream<K,V>` handle + its stateless DSL ops.
//!
//! Each op (1) mints a JVM-matching node name, (2) adds a type-erased
//! `StatelessProcessor` node to the logical graph with the right
//! `key_changing_operation` flag, and (3) attaches a **lowering thunk**
//! ([`LowerFn`]) that — when the lowering driver (Task 5) runs it — performs the
//! typed [`Topology::add_processor`] call and records the resulting node name.
//! The thunk captures the op's concrete K/V types and the user closure, so types
//! are statically known *inside* the thunk even though the graph is type-erased.
use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::config::{Grouped, Materialized, StreamJoined};
use crate::dsl::graph::{GraphNodeKind, LowerState, NodeId};
use crate::dsl::kgrouped::KGroupedStream;
use crate::dsl::ktable::KTable;
use crate::dsl::names;
use crate::dsl::processors::change::Change;
use crate::dsl::processors::global_join::KStreamGlobalTableJoinProcessor;
use crate::dsl::processors::join::KStreamKTableJoinProcessor;
use crate::dsl::processors::ktable_join::JoinKind;
use crate::dsl::processors::outer_join_store::TimeTracker;
use crate::dsl::processors::stateless;
use crate::dsl::processors::stream_join::KStreamKStreamJoinProcessor;
use crate::dsl::processors::table::KStreamToTableProcessor;
use crate::dsl::windows::JoinWindows;
use crate::processor::serde::{BytesSerde, Produced, Serde};
use crate::topology::NodeHandle;

/// The shared outer-form joiner threaded into a windowed stream-stream join
/// (`join`/`left_join`/`outer_join` all lift their user joiner to this shape).
/// Each per-side processor wraps it so a match passes the present sides.
type SharedOuterJoiner<V, V2, VO> = Arc<dyn Fn(Option<&V>, Option<&V2>) -> VO + Send + Sync>;

pub struct KStream<K, V> {
    #[allow(dead_code)]
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    #[allow(dead_code)]
    pub(crate) node: NodeId,
    /// True when the current key was produced by a key-changing op upstream
    /// (`select_key`/`map`/`flat_map`/`group_by`) that has *not* since been
    /// re-grouped through a repartition. A downstream aggregation reads this to
    /// decide whether it must insert a repartition before the aggregate node.
    /// A source stream starts `false`; value-only ops propagate the parent bit.
    pub(crate) key_changing: bool,
    /// The single Kafka source topic this stream still reads, when known. Set by
    /// [`StreamsBuilder::stream`] when a stream is sourced from exactly one topic;
    /// propagated unchanged through value-only ops (`map_values`/`filter`/`peek`/…)
    /// since they don't change the key or the partitioning. Cleared (`None`) by
    /// key-changing ops, `merge`, `repartition`, `to_stream`, and a join output —
    /// in those cases the stream no longer corresponds to a single original source
    /// topic. [`join`](Self::join) reads this as the stream-side copartition group
    /// member when the key is unchanged (otherwise it repartitions and uses the
    /// repartition topic as the member).
    pub(crate) source_topic: Option<String>,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> KStream<K, V> {
    pub(crate) fn new(builder: Rc<RefCell<InternalStreamsBuilder>>, node: NodeId) -> Self {
        Self::new_with_key_changing(builder, node, false)
    }

    pub(crate) fn new_with_key_changing(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        key_changing: bool,
    ) -> Self {
        Self {
            builder,
            node,
            key_changing,
            source_topic: None,
            _pd: std::marker::PhantomData,
        }
    }

    /// Set the single source-topic lineage (see [`source_topic`](Self::source_topic)).
    #[must_use]
    pub(crate) fn with_source_topic(mut self, topic: Option<String>) -> Self {
        self.source_topic = topic;
        self
    }
}

impl<K, V> KStream<K, V>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    /// `mapValues`: transform each value, key unchanged. Not key-changing.
    pub fn map_values<V2, F>(&self, f: F) -> KStream<K, V2>
    where
        V2: Any + Send + Clone,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::MAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V2, _, _, _>(
                name.clone(),
                move || stateless::MapValuesProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // map_values is value-only → key + source-topic lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
            .with_source_topic(self.source_topic.clone())
    }

    /// `filter`: keep records where `predicate(key, value)` is true.
    #[must_use]
    pub fn filter<F>(&self, predicate: F) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        self.filter_inner(predicate, false)
    }

    /// `filterNot`: keep records where `predicate(key, value)` is false.
    #[must_use]
    pub fn filter_not<F>(&self, predicate: F) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        self.filter_inner(predicate, true)
    }

    fn filter_inner<F>(&self, predicate: F, negate: bool) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FILTER);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let p2 = predicate.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::FilterProcessor {
                    predicate: p2.clone(),
                    negate,
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // filter is value-only → key + source-topic lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
            .with_source_topic(self.source_topic.clone())
    }

    /// `map`: transform key and value. Key-changing.
    pub fn map<K2, V2, F>(&self, f: F) -> KStream<K2, V2>
    where
        K: Default,
        K2: Any + Send + Clone,
        V2: Any + Send + Clone,
        F: Fn(&K, &V) -> (K2, V2) + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::MAP);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K2, V2, _, _, _>(
                name.clone(),
                move || stateless::MapProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // map rewrites the key → key-changing lineage.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
    }

    /// `selectKey`: rewrite the key, value unchanged. Key-changing.
    pub fn select_key<K2, F>(&self, f: F) -> KStream<K2, V>
    where
        K: Default,
        K2: Any + Send + Clone,
        F: Fn(&K, &V) -> K2 + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KEY_SELECT);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K2, V, _, _, _>(
                name.clone(),
                move || stateless::SelectKeyProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // select_key rewrites the key → key-changing lineage.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
    }

    /// `flatMap`: one record → zero or more `(K2, V2)`. Key-changing.
    pub fn flat_map<K2, V2, IT, F>(&self, f: F) -> KStream<K2, V2>
    where
        K: Default,
        K2: Any + Send + Clone,
        V2: Any + Send + Clone,
        IT: IntoIterator<Item = (K2, V2)> + 'static,
        F: Fn(&K, &V) -> IT + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FLATMAP);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K2, V2, _, _, _>(
                name.clone(),
                move || stateless::FlatMapProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // flat_map can rewrite the key → key-changing lineage.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, true)
    }

    /// `flatMapValues`: one record → zero or more `V2`, key unchanged.
    pub fn flat_map_values<V2, IT, F>(&self, f: F) -> KStream<K, V2>
    where
        V2: Any + Send + Clone,
        IT: IntoIterator<Item = V2> + 'static,
        F: Fn(&V) -> IT + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FLATMAPVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V2, _, _, _>(
                name.clone(),
                move || stateless::FlatMapValuesProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // flat_map_values is value-only → key + source-topic lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
            .with_source_topic(self.source_topic.clone())
    }

    /// `peek`: observe each record, then forward it unchanged.
    #[must_use]
    pub fn peek<F>(&self, f: F) -> KStream<K, V>
    where
        K: Default,
        F: Fn(&K, &V) + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::PEEK);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::PeekProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // peek is observe-only → key + source-topic lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
            .with_source_topic(self.source_topic.clone())
    }

    /// `foreach`: terminal side-effect on each record (consumes the stream).
    pub fn foreach<F>(self, f: F)
    where
        K: Default,
        F: Fn(&K, &V) + Clone + Send + Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::FOREACH);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let f2 = f.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::ForeachProcessor {
                    f: f2.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
    }

    /// `to`: write the stream to a topic via a sink (consumes the stream).
    pub fn to<KS, VS>(self, topic: impl Into<String>, produced: Produced<KS, VS>)
    where
        KS: Serde<K> + Clone,
        VS: Serde<V> + Clone,
    {
        let topic: String = topic.into();
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::SINK);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StreamSink {
                topic: topic.clone(),
            },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            state
                .topology
                .add_sink::<K, V, KS, VS, _, _>(name.clone(), topic, [parent], produced);
            // A sink is terminal; record its name so children (none) could find it.
            state.handle_name.insert(id, name.clone());
        }));
    }

    /// `merge`: union this stream with `other` (same K/V). Both feed one node.
    #[must_use]
    pub fn merge(&self, other: &KStream<K, V>) -> KStream<K, V> {
        let left_id = self.node;
        let right_id = other.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::MERGE);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![left_id, right_id],
        );
        g.graph.nodes[id].merge_node = true;
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let left = NodeHandle::<K, V>::from_name(state.handle_name[&left_id].clone());
            let right = NodeHandle::<K, V>::from_name(state.handle_name[&right_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                || stateless::MergeProcessor { _pd: PhantomData },
                [left, right],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // merge keeps keys; conservatively key-changing if either side is.
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing || other.key_changing,
        )
    }

    /// `join` (inner stream-table join): for each record on this stream, look up
    /// the record's key in `table`'s materialized store and, **only when a value is
    /// present**, forward `joiner(&stream_value, &table_value)` keyed by the same
    /// key. Records whose key is absent from the table are dropped (inner join).
    ///
    /// `table` must be materialized (sourced via [`StreamsBuilder::table`] or any
    /// materialized op) — the join reads its store by name. The stream side and the
    /// table's source topic are declared as a **copartition group** (KIP-1071), so
    /// the streams-group coordinator co-locates their partitions.
    ///
    /// The stream key must be unchanged relative to its source partitioning (a
    /// stream-table join is partition-local). If a key-changing op (`map`/
    /// `select_key`/`flat_map`/`group_by`) precedes the join, call
    /// [`repartition`](Self::repartition) first to re-partition by the new key;
    /// otherwise `join_table` panics.
    ///
    /// # Naming vs the JVM
    /// The JVM overloads `join` for both stream-table and (windowed) stream-stream
    /// joins. Rust cannot have two inherent methods of the same name and differing
    /// arity, so the stream-table forms are named `join_table`/`left_join_table`
    /// here, leaving the plain `join`/`left_join` for the windowed stream-stream
    /// join (see [`KStream::join`](Self::join)).
    ///
    /// [`StreamsBuilder::table`]: crate::dsl::builder::StreamsBuilder::table
    #[must_use]
    pub fn join_table<VT, VO, F>(&self, table: &KTable<K, VT>, joiner: F) -> KStream<K, VO>
    where
        VT: Any + Send + Clone,
        VO: Any + Send + Clone,
        F: Fn(&V, &VT) -> VO + Clone + Send + Sync + 'static,
    {
        // Wrap the inner joiner to the left form `Fn(&V, Option<&VT>) -> VO`; with
        // `emit_on_miss = false` the closure is only ever called with `Some`.
        let lf = move |v: &V, opt: Option<&VT>| joiner(v, opt.expect("inner join hit"));
        self.join_table_impl::<VT, VO, _>(table, lf, false)
    }

    /// `leftJoin` (left stream-table join): like [`join_table`](Self::join_table)
    /// but always forwards a record for every stream record. On a table miss the
    /// `joiner` receives `None` for the table-side value. See
    /// [`join_table`](Self::join_table) for the naming-vs-JVM note.
    #[must_use]
    pub fn left_join_table<VT, VO, F>(&self, table: &KTable<K, VT>, joiner: F) -> KStream<K, VO>
    where
        VT: Any + Send + Clone,
        VO: Any + Send + Clone,
        F: Fn(&V, Option<&VT>) -> VO + Clone + Send + Sync + 'static,
    {
        self.join_table_impl::<VT, VO, _>(table, joiner, true)
    }

    /// Shared lowering for inner/left stream-table join. `left_form` is the
    /// `Fn(&V, Option<&VT>) -> VO` joiner; `emit_on_miss` is `false` for inner,
    /// `true` for left.
    ///
    /// Records a `KSTREAM-JOIN-` processor node wired to this stream's node, and in
    /// its thunk: (1) builds a [`KStreamKTableJoinProcessor`] reading the table's
    /// store, (2) `connect_processor_store`s the join to that store (the union pulls
    /// the join into the SAME subtopology as the table source, so both sources land
    /// together), and (3) declares the `(stream_member, table_source)` copartition
    /// group when both members are single source topics.
    fn join_table_impl<VT, VO, LF>(
        &self,
        table: &KTable<K, VT>,
        left_form: LF,
        emit_on_miss: bool,
    ) -> KStream<K, VO>
    where
        VT: Any + Send + Clone,
        VO: Any + Send + Clone,
        LF: Fn(&V, Option<&VT>) -> VO + Clone + Send + Sync + 'static,
    {
        assert!(
            !self.key_changing,
            "join: the stream key was changed upstream (map/select_key/flat_map/group_by); \
             call `.repartition(..)` before joining to re-partition by the new key"
        );
        let table_store = table
            .store_name()
            .expect("join requires a materialized table (a store-backed KTable)")
            .to_string();
        let table_src = table.source_topic().map(str::to_string);
        // The stream-side copartition member is this stream's single source topic
        // (key unchanged → still copartitioned with that topic). `None` if the
        // stream has no single source topic (multi-topic source, prior merge, …).
        let stream_member = self.source_topic.clone();

        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let join_name = g.new_processor_name(names::JOIN);
        let join_id = g.graph.add(
            join_name.clone(),
            // The join lowers to a plain processor node (its store connection +
            // copartition declaration happen in the thunk); reusing the stateless
            // kind keeps the optimizer passes (which only inspect Repartition /
            // TableSource) correctly skipping it.
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let store_for_thunk = table_store.clone();
        g.graph.nodes[join_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            let lf = left_form.clone();
            let h = state.topology.add_processor::<K, V, K, VO, _, _, _>(
                join_name.clone(),
                move || KStreamKTableJoinProcessor {
                    table_store: store_for_proc.clone(),
                    joiner: lf.clone(),
                    emit_on_miss,
                    _pd: PhantomData,
                },
                [parent],
            );
            // Union the join into the table store's processor set: this pulls the
            // join (and therefore the stream source feeding it) into the SAME
            // subtopology as the table source that owns the store.
            state
                .topology
                .connect_processor_store(h.name(), &store_for_thunk);
            // Declare the copartition group when both sides are single source
            // topics. The grouping pass assigns it to the subtopology and the wire
            // layer encodes the members as int16 indices into the sorted sources.
            if let (Some(sm), Some(ts)) = (&stream_member, &table_src) {
                state
                    .topology
                    .add_copartition_group([sm.clone(), ts.clone()]);
            }
            state.handle_name.insert(join_id, h.name().to_string());
        }));
        drop(g);
        // The joined stream keeps the key but no longer maps to a single source
        // topic (it is the join output), so its source-topic lineage is `None`.
        KStream::new_with_key_changing(Rc::clone(&self.builder), join_id, false)
    }

    /// `join` (inner stream-globaltable join): for each record on this stream,
    /// compute the lookup key `gk = key_mapper(&streamKey, &streamValue)`, look it
    /// up in `global`'s fully-replicated store, and — **only on a hit** — forward
    /// `joiner(&stream_value, &global_value)` keyed by the **stream key** with the
    /// stream timestamp. Records whose derived key is absent are dropped.
    ///
    /// Unlike [`join_table`](Self::join_table), the lookup key is *derived* from
    /// the record (so it may differ from the stream key), and because a
    /// `GlobalKTable` is fully replicated there is **no copartitioning, no
    /// repartition, and no key-changing assertion** — any record can look up any
    /// key on every instance.
    ///
    /// [`GlobalKTable`]: crate::dsl::global_table::GlobalKTable
    #[must_use]
    pub fn join_global<GK, VG, VR, KM, J>(
        &self,
        global: &crate::dsl::global_table::GlobalKTable<GK, VG>,
        key_mapper: KM,
        joiner: J,
    ) -> KStream<K, VR>
    where
        GK: Any + Send + Sync + 'static,
        VG: Any + Send + Clone,
        VR: Any + Send + Clone,
        KM: Fn(&K, &V) -> GK + Clone + Send + Sync + 'static,
        J: Fn(&V, &VG) -> VR + Clone + Send + Sync + 'static,
    {
        // Wrap the inner joiner to the left form `Fn(&V, Option<&VG>) -> VR`; with
        // `emit_on_miss = false` the closure is only ever called with `Some`.
        let jf = move |v: &V, opt: Option<&VG>| joiner(v, opt.expect("inner global join hit"));
        self.join_global_impl::<GK, VG, VR, KM, _>(global, key_mapper, jf, false)
    }

    /// `leftJoin` (left stream-globaltable join): like
    /// [`join_global`](Self::join_global) but always forwards a record for every
    /// stream record. On a global-store miss the `joiner` receives `None` for the
    /// global-side value.
    #[must_use]
    pub fn left_join_global<GK, VG, VR, KM, J>(
        &self,
        global: &crate::dsl::global_table::GlobalKTable<GK, VG>,
        key_mapper: KM,
        joiner: J,
    ) -> KStream<K, VR>
    where
        GK: Any + Send + Sync + 'static,
        VG: Any + Send + Clone,
        VR: Any + Send + Clone,
        KM: Fn(&K, &V) -> GK + Clone + Send + Sync + 'static,
        J: Fn(&V, Option<&VG>) -> VR + Clone + Send + Sync + 'static,
    {
        self.join_global_impl::<GK, VG, VR, KM, _>(global, key_mapper, joiner, true)
    }

    /// Shared lowering for inner/left stream-globaltable join. `left_form` is the
    /// `Fn(&V, Option<&VG>) -> VR` joiner; `emit_on_miss` is `false` for inner,
    /// `true` for left.
    ///
    /// Records a `KSTREAM-GLOBALTABLE-JOIN-` processor node wired to this stream's
    /// node; its thunk builds a [`KStreamGlobalTableJoinProcessor`] that reads the
    /// global table's store by name via the global registry accessor. Unlike the
    /// stream-table join there is **no** copartition group and **no**
    /// `connect_processor_store` (the global store is fully replicated and reached
    /// through the global registry, not a copartitioned subtopology), and **no**
    /// `key_changing` assertion (the lookup key is derived, not the stream key).
    fn join_global_impl<GK, VG, VR, KM, LF>(
        &self,
        global: &crate::dsl::global_table::GlobalKTable<GK, VG>,
        key_mapper: KM,
        left_form: LF,
        emit_on_miss: bool,
    ) -> KStream<K, VR>
    where
        GK: Any + Send + Sync + 'static,
        VG: Any + Send + Clone,
        VR: Any + Send + Clone,
        KM: Fn(&K, &V) -> GK + Clone + Send + Sync + 'static,
        LF: Fn(&V, Option<&VG>) -> VR + Clone + Send + Sync + 'static,
    {
        let store_name = global.store_name().to_string();

        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let join_name = g.new_processor_name(names::GLOBALTABLE_JOIN);
        let join_id = g.graph.add(
            join_name.clone(),
            // Reuse the stateless kind: the global join lowers to a plain processor
            // node with no repartition and no store-connection/copartition wiring,
            // so the optimizer passes (Repartition / TableSource) correctly skip it.
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[join_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_name.clone();
            let km = key_mapper.clone();
            let jf = left_form.clone();
            let h = state.topology.add_processor::<K, V, K, VR, _, _, _>(
                join_name.clone(),
                move || KStreamGlobalTableJoinProcessor {
                    store_name: store_for_proc.clone(),
                    key_mapper: km.clone(),
                    joiner: jf.clone(),
                    emit_on_miss,
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(join_id, h.name().to_string());
        }));
        drop(g);
        // The joined stream keeps the stream key but is a join output, so it no
        // longer maps to a single source topic.
        KStream::new_with_key_changing(Rc::clone(&self.builder), join_id, false)
    }

    /// `join` (windowed inner stream-stream join): for each record on either
    /// stream, the record is put into its own [`JoinWindowStore`] and the OTHER
    /// stream's store is fetched over the join window; one output is forwarded per
    /// match, keyed by the same key. A left record at `t` matches right records
    /// with timestamp in `[t - before, t + after]` (and symmetrically — the
    /// per-side OTHER processor swaps `before`/`after` so the rule holds whichever
    /// side drives the record).
    ///
    /// Lowers (mirroring [`KTable::join`]'s dual+merge) to a **THIS** processor fed
    /// by this stream, an **OTHER** processor fed by `other`, and a **MERGE** node
    /// unioning the two outputs. Each side puts into its own `retainDuplicates`
    /// join-window store and reads the other's, and is connected to BOTH stores so
    /// the grouping pass folds A, B, both stores, and the two join nodes into one
    /// copartitioned subtopology. When both streams are single-source-topic streams
    /// their source topics are declared as a copartition group (KIP-1071).
    ///
    /// Both stream keys must be unchanged relative to their source partitioning;
    /// a key-changing op upstream must be `repartition()`-ed first (else panics).
    ///
    /// # Naming vs the JVM
    /// The JVM overloads `join` for stream-table and stream-stream. Rust cannot, so
    /// the windowed stream-stream join keeps `join`/`left_join` while the
    /// stream-table forms are [`join_table`](Self::join_table)/
    /// [`left_join_table`](Self::left_join_table).
    ///
    /// [`JoinWindowStore`]: crate::store::join_window::JoinWindowStore
    /// [`KTable::join`]: crate::dsl::ktable::KTable::join
    #[must_use]
    pub fn join<V2, VO, F, KS, V1S, V2S>(
        &self,
        other: &KStream<K, V2>,
        joiner: F,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, V1S, V2S>,
    ) -> KStream<K, VO>
    where
        V2: Any + Send + Sync + Clone,
        VO: Any + Send + Clone,
        F: Fn(&V, &V2) -> VO + Clone + Send + Sync + 'static,
        KS: Serde<K> + Clone + 'static,
        V1S: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        // Lift the inner joiner to the shared outer form: a match passes `Some`/`Some`
        // (a null result never occurs for an inner join, so `expect` is unreachable).
        let j =
            move |a: Option<&V>, b: Option<&V2>| joiner(a.expect("inner a"), b.expect("inner b"));
        self.join_impl::<V2, VO, KS, V1S, V2S>(
            other,
            Arc::new(j),
            windows,
            stream_joined,
            JoinKind::inner(),
        )
    }

    /// `leftJoin` (windowed left stream-stream join): every record on THIS (left)
    /// stream that finds no match in the OTHER (right) window emits
    /// `joiner(&this, None)` once that record's window closes (KIP-633,
    /// stream-time-driven). Matched records emit `joiner(&this, Some(&other))` as in
    /// the inner join. The right side never emits a non-join.
    #[must_use]
    pub fn left_join<V2, VO, F, KS, V1S, V2S>(
        &self,
        other: &KStream<K, V2>,
        joiner: F,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, V1S, V2S>,
    ) -> KStream<K, VO>
    where
        V2: Any + Send + Sync + Clone,
        VO: Any + Send + Clone,
        F: Fn(&V, Option<&V2>) -> VO + Clone + Send + Sync + 'static,
        KS: Serde<K> + Clone + 'static,
        V1S: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        // Left form: the left (A) side may receive `None` for B; the A side is always
        // present (the join never fires from a non-existent A).
        let j = move |a: Option<&V>, b: Option<&V2>| joiner(a.expect("left a"), b);
        self.join_impl::<V2, VO, KS, V1S, V2S>(
            other,
            Arc::new(j),
            windows,
            stream_joined,
            JoinKind::left(),
        )
    }

    /// `outerJoin` (windowed outer stream-stream join): every record on EITHER side
    /// that finds no match emits `joiner(Some, None)` / `joiner(None, Some)` once its
    /// window closes (KIP-633). Matched records emit `joiner(Some, Some)`.
    #[must_use]
    pub fn outer_join<V2, VO, F, KS, V1S, V2S>(
        &self,
        other: &KStream<K, V2>,
        joiner: F,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, V1S, V2S>,
    ) -> KStream<K, VO>
    where
        V2: Any + Send + Sync + Clone,
        VO: Any + Send + Clone,
        F: Fn(Option<&V>, Option<&V2>) -> VO + Clone + Send + Sync + 'static,
        KS: Serde<K> + Clone + 'static,
        V1S: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        // The user joiner is already in outer form.
        self.join_impl::<V2, VO, KS, V1S, V2S>(
            other,
            Arc::new(joiner),
            windows,
            stream_joined,
            JoinKind::outer(),
        )
    }

    /// Shared dual+merge lowering for inner/left/outer windowed stream-stream joins.
    ///
    /// `outer_joiner` is the shared outer-form joiner `Fn(Option<&V>, Option<&V2>) ->
    /// VO`; each per-side processor wraps it so a match passes the present sides. The
    /// `kind` drives which side emits non-joins: **THIS (A)** emits when B is not
    /// required (`!kind.b_required` → left & outer), **OTHER (B)** emits when A is not
    /// required (`!kind.a_required` → outer only).
    ///
    /// Lowers (mirroring [`KTable::join`]'s dual+merge) to a **THIS** processor fed by
    /// this stream, an **OTHER** processor fed by `other`, and a **MERGE** node. Each
    /// side puts into its own `retainDuplicates` join-window store and reads the
    /// other's. For left/outer, one shared `KSTREAM-OUTERSHARED-` KV store buffers
    /// unmatched records and a single shared [`TimeTracker`] (cloned into both
    /// supplier closures) drives the window-close emission; both join processors
    /// connect to that store. For inner the outer store/tracker are not created, so
    /// the wire topology is byte-identical to the inner-only golden.
    ///
    /// [`KTable::join`]: crate::dsl::ktable::KTable::join
    /// [`TimeTracker`]: crate::dsl::processors::outer_join_store::TimeTracker
    #[allow(clippy::too_many_lines)] // dual+merge lowering: 3 nodes + shared outer store, each a typed thunk
    #[allow(clippy::needless_pass_by_value)] // `outer_joiner` is consumed (cloned into both thunks)
    fn join_impl<V2, VO, KS, V1S, V2S>(
        &self,
        other: &KStream<K, V2>,
        outer_joiner: SharedOuterJoiner<V, V2, VO>,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, V1S, V2S>,
        kind: JoinKind,
    ) -> KStream<K, VO>
    where
        V2: Any + Send + Sync + Clone,
        VO: Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        V1S: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        assert!(
            !self.key_changing && !other.key_changing,
            "stream-stream join: a key-changing stream must `.repartition()` first"
        );
        let a_src = self.source_topic.clone();
        let b_src = other.source_topic.clone();
        let self_node = self.node;
        let other_node = other.node;
        let StreamJoined {
            key_serde,
            value1_serde,
            value2_serde,
        } = stream_joined;
        let before = windows.before_ms;
        let after = windows.after_ms;
        let grace = windows.grace_ms;

        // Which side emits non-joins: A emits when B is not required (left/outer);
        // B emits when A is not required (outer). Inner: neither.
        let this_emit = !kind.b_required;
        let other_emit = !kind.a_required;
        let has_outer = this_emit || other_emit;
        // One shared stream-time tracker, cloned into both supplier closures (the
        // supplier runs once per task, so both processors share the same instance).
        let tracker: Arc<Mutex<TimeTracker>> = Arc::new(Mutex::new(TimeTracker::default()));

        let mut g = self.builder.borrow_mut();
        // Mint names to match the JVM 4.1 `KStreamImplJoin` counter sequence
        // (validated by the `stream_stream_join` golden, Task B4): the JVM first
        // mints two `KSTREAM-WINDOWED-` windowed-stream processors (which put each
        // side into its window store), then the two join processors, then merge.
        // The two window-store names are `<joinProcessorName>-store`. We burn the
        // two windowed indices (those nodes aren't wire-visible) so the join
        // processors — and hence the store names — land at the JVM indices.
        let _windowed_this = g.new_processor_name(names::KSTREAM_WINDOWED);
        let _windowed_other = g.new_processor_name(names::KSTREAM_WINDOWED);
        // Left/outer rename the join processors (and hence their `<name>-store`
        // window stores): THIS → OUTERTHIS when the OTHER side is outer; OTHER →
        // OUTEROTHER when THIS is outer. Inner keeps JOINTHIS/JOINOTHER, so inner
        // topologies are byte-unchanged. (JVM `KStreamImplJoin`; pinned by the
        // `stream_stream_outer_join` golden, Task C4.)
        let join_this_prefix = if other_emit {
            names::KSTREAM_OUTERTHIS
        } else {
            names::KSTREAM_JOINTHIS
        };
        let join_other_prefix = if this_emit {
            names::KSTREAM_OUTEROTHER
        } else {
            names::KSTREAM_JOINOTHER
        };
        let join_this = g.new_processor_name(join_this_prefix);
        let join_other = g.new_processor_name(join_other_prefix);
        let this_store = format!("{join_this}-store");
        let other_store = format!("{join_other}-store");
        let merge = g.new_processor_name(names::MERGE);
        // The shared outer-join KV store (left/outer only). The JVM does NOT mint a
        // fresh counter index for it — it reuses the THIS join processor's 10-digit
        // index: `KSTREAM-OUTERSHARED-<thisIndex>-store`. (`new_processor_name`
        // always formats the index as the trailing 10 chars.)
        let outer_store = has_outer.then(|| {
            let idx = &join_this[join_this.len() - 10..];
            format!("{}{idx}-store", names::KSTREAM_OUTERSHARED)
        });

        // ── THIS side: fed by this stream; puts into `this_store`, reads `other_store`.
        let this_id = g.graph.add(
            join_this.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![self_node],
        );
        {
            let join_this_name = join_this.clone();
            let own = this_store.clone();
            let other_s = other_store.clone();
            let ks = key_serde.clone();
            let vs = value1_serde.clone();
            let outer_joiner_this = Arc::clone(&outer_joiner);
            let outer_store_this = outer_store.clone();
            let tracker_this = Arc::clone(&tracker);
            let ks_proc = key_serde.clone();
            let vs_proc = value1_serde.clone();
            g.graph.nodes[this_id].lower = Some(Box::new(move |state: &mut LowerState| {
                let parent = NodeHandle::<K, V>::from_name(state.handle_name[&self_node].clone());
                let own_for_proc = own.clone();
                let other_for_proc = other_s.clone();
                // THIS joiner: drains V (left); a match passes `Some(a)`, a null passes
                // `None` for the OTHER (right) side.
                let oj = Arc::clone(&outer_joiner_this);
                let outer_store_proc = outer_store_this.clone();
                let tracker_proc = Arc::clone(&tracker_this);
                let ks_for_proc = ks_proc.clone();
                let vs_for_proc = vs_proc.clone();
                let h = state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_this_name.clone(),
                    move || KStreamKStreamJoinProcessor {
                        own_store: own_for_proc.clone(),
                        other_store: other_for_proc.clone(),
                        fetch_before: before,
                        fetch_after: after,
                        joiner: {
                            let oj = Arc::clone(&oj);
                            move |a: &V, b: Option<&V2>| oj(Some(a), b)
                        },
                        side_left: true,
                        emit_unmatched: this_emit,
                        outer_store: outer_store_proc.clone(),
                        tracker: outer_store_proc.as_ref().map(|_| Arc::clone(&tracker_proc)),
                        key_serde: outer_store_proc
                            .as_ref()
                            .map(|_| Box::new(ks_for_proc.clone()) as Box<dyn Serde<K>>),
                        value_serde: outer_store_proc
                            .as_ref()
                            .map(|_| Box::new(vs_for_proc.clone()) as Box<dyn Serde<V>>),
                        before_ms: before,
                        after_ms: after,
                        grace_ms: grace,
                        _pd: PhantomData,
                    },
                    [parent],
                );
                // Register the THIS store (holds V) + connect to BOTH stores.
                state.topology.add_join_window_store::<K, V, KS, V1S>(
                    own.clone(),
                    ks.clone(),
                    vs.clone(),
                    before,
                    after,
                    grace,
                    [h.name().to_string()],
                );
                state.topology.connect_processor_store(h.name(), &own);
                state.topology.connect_processor_store(h.name(), &other_s);
                // For left/outer, register the shared outer KV store ONCE here and
                // connect this processor to it. (The OTHER thunk only connects.)
                if let Some(os) = &outer_store_this {
                    state
                        .topology
                        .add_state_store::<Bytes, Bytes, BytesSerde, BytesSerde>(
                            os.clone(),
                            BytesSerde,
                            BytesSerde,
                            [h.name().to_string()],
                        );
                }
                state.handle_name.insert(this_id, h.name().to_string());
            }));
        }

        // ── OTHER side: fed by `other`; puts into `other_store`, reads `this_store`.
        let other_id = g.graph.add(
            join_other.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![other_node],
        );
        {
            let join_other_name = join_other.clone();
            let own = other_store.clone();
            let other_s = this_store.clone();
            let ks = key_serde.clone();
            let vs = value2_serde.clone();
            let outer_joiner_other = Arc::clone(&outer_joiner);
            let outer_store_other = outer_store.clone();
            let tracker_other = Arc::clone(&tracker);
            let ks_proc = key_serde.clone();
            let vs_proc = value2_serde.clone();
            g.graph.nodes[other_id].lower = Some(Box::new(move |state: &mut LowerState| {
                let parent = NodeHandle::<K, V2>::from_name(state.handle_name[&other_node].clone());
                let own_for_proc = own.clone();
                let other_for_proc = other_s.clone();
                // OTHER joiner: drains V2 (right); a match passes `Some(b)`, a null
                // passes `None` for the THIS (left) side — preserving the user
                // `joiner(a, b)` arg order.
                let oj = Arc::clone(&outer_joiner_other);
                let outer_store_proc = outer_store_other.clone();
                let tracker_proc = Arc::clone(&tracker_other);
                let ks_for_proc = ks_proc.clone();
                let vs_for_proc = vs_proc.clone();
                let h = state.topology.add_processor::<K, V2, K, VO, _, _, _>(
                    join_other_name.clone(),
                    move || KStreamKStreamJoinProcessor {
                        own_store: own_for_proc.clone(),
                        other_store: other_for_proc.clone(),
                        // SWAPPED relative to the named window: from the OTHER
                        // (right) record's perspective, the left side lives in
                        // `[t - after, t + before]`.
                        fetch_before: after,
                        fetch_after: before,
                        joiner: {
                            let oj = Arc::clone(&oj);
                            move |b: &V2, a: Option<&V>| oj(a, Some(b))
                        },
                        side_left: false,
                        emit_unmatched: other_emit,
                        outer_store: outer_store_proc.clone(),
                        tracker: outer_store_proc.as_ref().map(|_| Arc::clone(&tracker_proc)),
                        key_serde: outer_store_proc
                            .as_ref()
                            .map(|_| Box::new(ks_for_proc.clone()) as Box<dyn Serde<K>>),
                        value_serde: outer_store_proc
                            .as_ref()
                            .map(|_| Box::new(vs_for_proc.clone()) as Box<dyn Serde<V2>>),
                        before_ms: before,
                        after_ms: after,
                        grace_ms: grace,
                        _pd: PhantomData,
                    },
                    [parent],
                );
                // Register the OTHER store (holds V2) + connect to BOTH stores.
                state.topology.add_join_window_store::<K, V2, KS, V2S>(
                    own.clone(),
                    ks.clone(),
                    vs.clone(),
                    before,
                    after,
                    grace,
                    [h.name().to_string()],
                );
                state.topology.connect_processor_store(h.name(), &own);
                state.topology.connect_processor_store(h.name(), &other_s);
                // For left/outer, connect this processor to the shared outer store
                // (registered by the THIS thunk).
                if let Some(os) = &outer_store_other {
                    state.topology.connect_processor_store(h.name(), os);
                }
                state.handle_name.insert(other_id, h.name().to_string());
            }));
        }

        // ── MERGE: union the two join outputs (forwards each record unchanged).
        let merge_id = g.graph.add(
            merge.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![this_id, other_id],
        );
        g.graph.nodes[merge_id].merge_node = true;
        g.graph.nodes[merge_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let this_parent = NodeHandle::<K, VO>::from_name(state.handle_name[&this_id].clone());
            let other_parent = NodeHandle::<K, VO>::from_name(state.handle_name[&other_id].clone());
            let h = state.topology.add_processor::<K, VO, K, VO, _, _, _>(
                merge.clone(),
                || stateless::MergeProcessor::<K, VO> { _pd: PhantomData },
                [this_parent, other_parent],
            );
            // Declare the copartition group when both streams are single-source.
            if let (Some(a), Some(bb)) = (&a_src, &b_src) {
                state
                    .topology
                    .add_copartition_group([a.clone(), bb.clone()]);
            }
            state.handle_name.insert(merge_id, h.name().to_string());
        }));
        drop(g);
        // The join output keeps the key but no longer maps to a single source topic.
        KStream::new_with_key_changing(Rc::clone(&self.builder), merge_id, false)
    }

    /// `repartition`: force a repartition through an internal topic.
    ///
    /// Lowers as `sink → add_repartition_topic → source`, the same pattern used
    /// by the implicit repartition inserted before a stateful aggregation. The
    /// repartition topic name is `<app_id>-<name>-repartition`, where `<name>` is
    /// the explicit [`Repartitioned`](crate::dsl::config::Repartitioned) name when
    /// set, otherwise an auto-name minted from the counter.
    ///
    /// **Byte-exactness vs JVM:** the JVM assigns a distinct `KSTREAM-REPARTITION-`
    /// counter for standalone `repartition()` calls. That counter is NOT validated
    /// against a golden fixture in this slice — functional correctness (no panic,
    /// records flow through) is the bar here.
    #[must_use]
    pub fn repartition<KS, VS>(
        &self,
        repartitioned: crate::dsl::config::Repartitioned<KS, VS>,
    ) -> KStream<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        let crate::dsl::config::Repartitioned {
            name: explicit_name,
            partitions,
            key_serde,
            value_serde,
        } = repartitioned;
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        // Mint the base name: explicit name or a fresh counter.
        let base = explicit_name.unwrap_or_else(|| g.new_processor_name(names::SOURCE));
        // Sink + source names used inside the thunk (minted now to advance
        // the counter at the right position relative to later ops).
        let sink_name = g.new_processor_name(names::SINK);
        let source_name = g.new_processor_name(names::SOURCE);
        let topic_base = base.clone();
        let id = g.graph.add(
            source_name.clone(),
            GraphNodeKind::Repartition {
                topic: format!("{topic_base}{}", names::REPARTITION_SUFFIX),
                partitions,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&parent_id].clone();
            let parent = NodeHandle::<K, V>::from_name(parent_name);
            let topic = format!("{}-{topic_base}{}", state.app_id, names::REPARTITION_SUFFIX);
            // sink: write to repartition topic
            state.topology.add_sink::<K, V, KS, VS, _, _>(
                sink_name.clone(),
                topic.clone(),
                [parent],
                crate::processor::serde::Produced::with(key_serde.clone(), value_serde.clone()),
            );
            // mark the topic as internal repartition (loop-back)
            state.topology.add_repartition_topic(topic.clone());
            // source: read from repartition topic
            state.topology.add_source::<K, V, KS, VS>(
                source_name.clone(),
                [topic],
                crate::processor::serde::Consumed::with(key_serde, value_serde),
            );
            state.handle_name.insert(id, source_name.clone());
        }));
        drop(g);
        // An explicit repartition re-groups by key → downstream is no longer
        // key-changing relative to its (now repartitioned) partitioning.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, false)
    }

    /// `split`: begin a branching fan-out. Returns a [`BranchedStream`] builder
    /// from which individual [`branch`](BranchedStream::branch) calls create
    /// filtered child streams. The split itself adds no node to the topology —
    /// each `branch` call creates a [`FilterProcessor`]-backed child wired
    /// directly to this stream's node.
    ///
    /// **Simplification vs JVM:** each branch receives a record when its predicate
    /// matches, not just the first matching branch. For mutually-exclusive
    /// predicates the behaviour is identical to the JVM first-match-wins semantics.
    ///
    /// [`branch`]: BranchedStream::branch
    /// [`FilterProcessor`]: crate::dsl::processors::stateless::FilterProcessor
    #[must_use]
    pub fn split(&self) -> BranchedStream<K, V> {
        // The JVM mints a `KSTREAM-BRANCH-` node at split() time (counter-only:
        // the branch node itself is not wire-visible). Advance the counter here so
        // that downstream auto-names (e.g. from a subsequent aggregation) land at
        // the same indices as the JVM byte-for-byte. Mirrors the FILTER counter
        // mint in `KGroupedStream::record_repartition`.
        let _branch_name = self.builder.borrow_mut().new_processor_name(names::BRANCH);
        BranchedStream {
            builder: Rc::clone(&self.builder),
            parent: self.node,
            key_changing: self.key_changing,
            source_topic: self.source_topic.clone(),
            _pd: std::marker::PhantomData,
        }
    }

    /// `groupByKey`: group by the existing key, preparing for an aggregation.
    ///
    /// Records no graph node — the (optional) repartition + aggregate node are
    /// recorded when a terminal `count`/`reduce`/`aggregate` is called. The
    /// returned [`KGroupedStream`] carries whether the upstream key lineage is
    /// key-changing (→ the aggregation must insert a repartition) and a typed
    /// repartition-lowering thunk built from the `Grouped` serdes.
    pub fn group_by_key<KS, VS>(&self, grouped: Grouped<KS, VS>) -> KGroupedStream<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        KGroupedStream::new(
            Rc::clone(&self.builder),
            self.node,
            self.key_changing,
            grouped.name,
            crate::dsl::kgrouped::repartition_lower::<K, V, KS, VS>(
                grouped.key_serde,
                grouped.value_serde,
            ),
        )
    }

    /// `groupBy`: re-key via `f`, then group by the new key.
    ///
    /// Equivalent to `select_key(f).group_by_key(grouped)`; the key change forces
    /// a repartition before any subsequent aggregation.
    pub fn group_by<K2, KS, VS, F>(&self, f: F, grouped: Grouped<KS, VS>) -> KGroupedStream<K2, V>
    where
        K: Default,
        K2: Any + Send + Sync + Clone,
        KS: Serde<K2> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        F: Fn(&K, &V) -> K2 + Clone + Send + Sync + 'static,
    {
        self.select_key(f).group_by_key(grouped)
    }

    /// `toTable`: materialize this stream into a [`KTable`] by writing each record
    /// into a state store and forwarding a `Change<V>` change-stream (prior store
    /// value as `old`). Backed by [`KStreamToTableProcessor`].
    ///
    /// The key is carried through unchanged, so `to_table` never inserts a
    /// repartition (the JVM only repartitions when the upstream key is rewritten
    /// without a re-group). The store name is `Materialized`'s explicit name when
    /// set, else a fresh `KSTREAM-TOTABLE-STATE-STORE-` counter; the store gets the
    /// standard `<app>-<store>-changelog` changelog (or none when
    /// [`Materialized::with_logging(false)`]).
    ///
    /// [`KStreamToTableProcessor`]: crate::dsl::processors::table::KStreamToTableProcessor
    /// [`Materialized::with_logging(false)`]: crate::dsl::config::Materialized::with_logging
    pub fn to_table<KS, VS>(&self, materialized: Materialized<KS, VS>) -> KTable<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        let parent_id = self.node;
        // Mint the store name at the JVM counter position (before the processor
        // name), matching the aggregate store-naming convention.
        let store_name = match &materialized.store_name {
            Some(name) => name.clone(),
            None => self
                .builder
                .borrow_mut()
                .new_processor_name(names::TOTABLE_STORE),
        };
        let Materialized {
            key_serde,
            value_serde,
            logging,
            ..
        } = materialized;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TOTABLE);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::Aggregate {
                store_name: store_name.clone(),
                changelog: logging,
            },
            vec![parent_id],
        );
        let store_for_thunk = store_name.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            // The stream → table boundary forwards Change<V> (prior store value as old).
            let h = state.topology.add_processor::<K, V, K, Change<V>, _, _, _>(
                name.clone(),
                move || KStreamToTableProcessor {
                    store_name: store_for_proc.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            // Honor `Materialized::with_logging(bool)`, mirroring the aggregate ops:
            // logging=true → changelog topic emitted; logging=false → store usable
            // at runtime but no state_changelog_topics entry in the wire topology.
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
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(Rc::clone(&self.builder), id, Some(store_name), None)
    }
}

// ---------------------------------------------------------------------------
// BranchedStream
// ---------------------------------------------------------------------------

/// Builder returned by [`KStream::split`]. Each [`branch`](Self::branch) call
/// adds a [`FilterProcessor`]-backed child node wired to the parent node and
/// returns a new [`KStream`] carrying only the records for which the predicate
/// returns `true`.
///
/// **Simplification vs JVM first-match-wins:** records are forwarded to ALL
/// branches whose predicate matches. For mutually-exclusive predicates the
/// behaviour is identical.
///
/// Drop `BranchedStream` before calling [`StreamsBuilder::build`] — it holds an
/// `Rc` clone of the shared internal builder and will otherwise cause the
/// `Rc::try_unwrap` inside `build` to fail.
///
/// [`StreamsBuilder::build`]: crate::dsl::builder::StreamsBuilder::build
/// [`FilterProcessor`]: crate::dsl::processors::stateless::FilterProcessor
pub struct BranchedStream<K, V> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) parent: NodeId,
    pub(crate) key_changing: bool,
    pub(crate) source_topic: Option<String>,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V> BranchedStream<K, V>
where
    K: Any + Send + Clone + Default,
    V: Any + Send + Clone,
{
    /// Add a branch: records for which `predicate(key, value)` returns `true`
    /// are forwarded to the returned [`KStream`]. Uses a `KSTREAM-BRANCHCHILD-`
    /// node backed by a [`FilterProcessor`] (negate = false).
    ///
    /// [`FilterProcessor`]: crate::dsl::processors::stateless::FilterProcessor
    pub fn branch<P>(&self, predicate: P) -> KStream<K, V>
    where
        P: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let parent_id = self.parent;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::BRANCHCHILD);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        let p2 = predicate.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, K, V, _, _, _>(
                name.clone(),
                move || stateless::FilterProcessor {
                    predicate: p2.clone(),
                    negate: false,
                    _pd: std::marker::PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        // branch is filter-only → key + source-topic lineage unchanged.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, self.key_changing)
            .with_source_topic(self.source_topic.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::dsl::builder::StreamsBuilder;
    use crate::processor::serde::{Consumed, Produced, StringSerde};
    use assert2::check;

    #[test]
    fn stateless_chain_records_named_nodes() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .map_values(|v: &String| v.to_uppercase())
            .filter(|_k: &String, _v: &String| true)
            .to("out", Produced::with(StringSerde, StringSerde));
        let g = b.internal.borrow();
        let names: Vec<&str> = g.graph.nodes.iter().map(|n| n.name.as_str()).collect();
        check!(
            names
                == vec![
                    "KSTREAM-SOURCE-0000000000",
                    "KSTREAM-MAPVALUES-0000000001",
                    "KSTREAM-FILTER-0000000002",
                    "KSTREAM-SINK-0000000003",
                ]
        );
    }

    #[test]
    fn select_key_marks_key_changing() {
        let b = StreamsBuilder::new();
        b.stream(["in"], Consumed::with(StringSerde, StringSerde))
            .select_key(|_k: &String, v: &String| v.clone());
        let g = b.internal.borrow();
        check!(g.graph.nodes[1].key_changing_operation);
    }
}
