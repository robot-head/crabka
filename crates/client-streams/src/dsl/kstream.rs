//! `KStream<K,V>` handle and its stateless DSL ops.
//!
//! Each op does three things. It mints a JVM-matching node name. It adds a
//! type-erased `StatelessProcessor` node to the logical graph with the right
//! `key_changing_operation` flag. It attaches a lowering thunk that makes the
//! typed [`Topology::add_processor`] call and records the resulting node name.
//! The thunk captures the op's concrete K/V types and the user closure, so the
//! types are known statically *inside* the thunk even though the graph is
//! type-erased.
//!
//! [`Topology::add_processor`]: crate::topology::Topology::add_processor
use std::{
    any::Any,
    cell::RefCell,
    marker::PhantomData,
    rc::Rc,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use crabka_units::prelude::*;

use crate::{
    dsl::{
        builder::InternalStreamsBuilder,
        config::{Grouped, Joined, Materialized, StreamJoined},
        graph::{GraphNodeKind, LowerState, NodeId},
        kgrouped::KGroupedStream,
        ktable::KTable,
        names,
        processors::{
            change::Change, global_join::KStreamGlobalTableJoinProcessor,
            join::KStreamKTableJoinProcessor, ktable_join::JoinKind, outer_join_store::TimeTracker,
            stateless, stream_join::KStreamKStreamJoinProcessor, table::KStreamToTableProcessor,
            tuple_forwarder::TupleForwarder,
        },
        windows::JoinWindows,
    },
    processor::serde::{BytesSerde, DefaultSerde, Produced, Serde, SerdeAssociate},
    topology::NodeHandle,
};

/// The shared outer-form joiner for a windowed stream-stream join.
///
/// `join`, `left_join`, and `outer_join` all lift their user joiner to this
/// shape. Each per-side processor wraps it, so a match passes the present sides.
type SharedOuterJoiner<V, V2, VO> = Arc<dyn Fn(Option<&V>, Option<&V2>) -> VO + Send + Sync>;

struct StreamJoinGraph {
    join_this: String,
    join_other: String,
    this_store: String,
    other_store: String,
    outer_store: Option<String>,
    this_id: NodeId,
    other_id: NodeId,
    merge: String,
}

fn allocate_stream_join_graph(
    builder: &mut InternalStreamsBuilder,
    parents: (NodeId, NodeId),
    required: (bool, bool),
) -> StreamJoinGraph {
    builder.new_processor_name(names::KSTREAM_WINDOWED);
    builder.new_processor_name(names::KSTREAM_WINDOWED);
    let this_prefix = if required.0 {
        names::KSTREAM_JOINTHIS
    } else {
        names::KSTREAM_OUTERTHIS
    };
    let other_prefix = if required.1 {
        names::KSTREAM_JOINOTHER
    } else {
        names::KSTREAM_OUTEROTHER
    };
    let join_this = builder.new_processor_name(this_prefix);
    let join_other = builder.new_processor_name(other_prefix);
    let merge = builder.new_processor_name(names::MERGE);
    let this_store = format!("{join_this}-store");
    let other_store = format!("{join_other}-store");
    let outer_store = (!required.0 || !required.1).then(|| {
        let this_index = &join_this[this_prefix.len()..];
        format!("{}{this_index}-store", names::KSTREAM_OUTERSHARED)
    });
    let this_id = builder.graph.add(
        join_this.clone(),
        GraphNodeKind::StatelessProcessor {
            repartition_required: false,
        },
        vec![parents.0],
    );
    let other_id = builder.graph.add(
        join_other.clone(),
        GraphNodeKind::StatelessProcessor {
            repartition_required: false,
        },
        vec![parents.1],
    );
    StreamJoinGraph {
        join_this,
        join_other,
        this_store,
        other_store,
        outer_store,
        this_id,
        other_id,
        merge,
    }
}

/// Type-erased KIP-923 grace lowering.
///
/// The `*_with` methods build this closure, because they hold the `Serde` and
/// `Sync` bounds that the grace buffer store and the processor need.
/// [`KStream::join_table_impl`] runs it once inside its lowering thunk.
///
/// The closure takes the live `LowerState`, the stream-side parent `NodeId`, the
/// auto-minted join node name, and the table store name. It rebuilds the typed
/// `NodeHandle<K, V>` itself, because `V` is in scope where the closure is built.
/// It registers the grace processor and its `<join_name>-Buffer` store, connects
/// the join to the buffer, and returns the join node handle. The impl then does
/// the shared table-store connect and the copartition declaration, the same as
/// the non-grace path.
type GraceLowering<K, VO> =
    Box<dyn FnOnce(&mut LowerState, NodeId, String, String) -> NodeHandle<K, VO> + Send>;

pub struct KStream<K, V, KS = <K as DefaultSerde>::Serde, VS = <V as DefaultSerde>::Serde> {
    #[allow(dead_code)]
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    #[allow(dead_code)]
    pub(crate) node: NodeId,
    /// True when a key-changing op upstream produced the current key and no
    /// repartition has re-grouped it since. The key-changing ops are
    /// `select_key`, `map`, `flat_map`, and `group_by`. A downstream aggregation
    /// reads this flag to decide whether it must insert a repartition before the
    /// aggregate node. A source stream starts `false`. Value-only ops propagate
    /// the parent bit.
    pub(crate) key_changing: bool,
    /// The single Kafka source topic this stream still reads, when known.
    ///
    /// [`StreamsBuilder::stream`] sets this when a stream reads exactly one
    /// topic. Value-only ops such as `map_values`, `filter`, and `peek` propagate
    /// it unchanged, because they do not change the key or the partitioning.
    /// Key-changing ops, `merge`, `repartition`, `to_stream`, and a join output
    /// all clear it to `None`. In those cases the stream no longer corresponds to
    /// a single original source topic.
    ///
    /// [`join`](Self::join) reads this as the stream-side copartition group
    /// member when the key is unchanged. If the key changed, `join` repartitions
    /// and uses the repartition topic as the member. `group_by_key` also
    /// propagates it, so a downstream cogroup can register its inputs'
    /// copartition group.
    pub(crate) source_topic: Option<String>,
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V, KS, VS> KStream<K, V, KS, VS> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        key_serde: KS,
        value_serde: VS,
    ) -> Self {
        Self::new_with_key_changing(builder, node, false, key_serde, value_serde)
    }

    pub(crate) fn new_with_key_changing(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        key_changing: bool,
        key_serde: KS,
        value_serde: VS,
    ) -> Self {
        Self {
            builder,
            node,
            key_changing,
            source_topic: None,
            key_serde,
            value_serde,
            _pd: std::marker::PhantomData,
        }
    }

    /// Set the single source-topic lineage (see [`source_topic`](Self::source_topic)).
    #[must_use]
    pub(crate) fn with_source_topic(mut self, topic: Option<String>) -> Self {
        self.source_topic = topic;
        self
    }

    #[must_use]
    pub fn with_key_serde<NewKS>(self, serde: NewKS) -> KStream<K, V, NewKS, VS> {
        KStream {
            builder: self.builder,
            node: self.node,
            key_changing: self.key_changing,
            source_topic: self.source_topic,
            key_serde: serde,
            value_serde: self.value_serde,
            _pd: std::marker::PhantomData,
        }
    }

    #[must_use]
    pub fn with_value_serde<NewVS>(self, serde: NewVS) -> KStream<K, V, KS, NewVS> {
        KStream {
            builder: self.builder,
            node: self.node,
            key_changing: self.key_changing,
            source_topic: self.source_topic,
            key_serde: self.key_serde,
            value_serde: serde,
            _pd: std::marker::PhantomData,
        }
    }
}

impl<K, V, KS, VS> KStream<K, V, KS, VS>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
    KS: Clone,
    VS: Clone,
{
    /// `mapValues`: transform each value, key unchanged. Not key-changing.
    pub fn map_values<V2, F>(&self, f: F) -> KStream<K, V2, KS, <V2 as DefaultSerde>::Serde>
    where
        V2: DefaultSerde + Any + Send + Clone,
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing,
            self.key_serde.clone(),
            <V2 as DefaultSerde>::Serde::default(),
        )
        .with_source_topic(self.source_topic.clone())
    }

    /// `filter`: keep records where `predicate(key, value)` is true.
    #[must_use]
    pub fn filter<F>(&self, predicate: F) -> KStream<K, V, KS, VS>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        self.filter_inner(predicate, false)
    }

    /// `filterNot`: keep records where `predicate(key, value)` is false.
    #[must_use]
    pub fn filter_not<F>(&self, predicate: F) -> KStream<K, V, KS, VS>
    where
        K: Default,
        F: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        self.filter_inner(predicate, true)
    }

    fn filter_inner<F>(&self, predicate: F, negate: bool) -> KStream<K, V, KS, VS>
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing,
            self.key_serde.clone(),
            self.value_serde.clone(),
        )
        .with_source_topic(self.source_topic.clone())
    }

    /// `map`: transform key and value. Key-changing.
    pub fn map<K2, V2, F>(
        &self,
        f: F,
    ) -> KStream<K2, V2, <K2 as DefaultSerde>::Serde, <V2 as DefaultSerde>::Serde>
    where
        K: Default,
        K2: DefaultSerde + Any + Send + Clone,
        V2: DefaultSerde + Any + Send + Clone,
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            true,
            <K2 as DefaultSerde>::Serde::default(),
            <V2 as DefaultSerde>::Serde::default(),
        )
    }

    /// `selectKey`: rewrite the key, value unchanged. Key-changing.
    pub fn select_key<K2, F>(&self, f: F) -> KStream<K2, V, <K2 as DefaultSerde>::Serde, VS>
    where
        K: Default,
        K2: DefaultSerde + Any + Send + Clone,
        F: Fn(&K, &V) -> K2 + Clone + Send + Sync + 'static,
    {
        self.select_key_with_serde(f, <K2 as DefaultSerde>::Serde::default())
    }

    /// `selectKey` with an explicit key serde. Key-changing.
    pub fn select_key_with_serde<K2, GKS, F>(&self, f: F, key_serde: GKS) -> KStream<K2, V, GKS, VS>
    where
        K: Default,
        K2: Any + Send + Clone,
        GKS: Serde<K2> + Clone + 'static,
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            true,
            key_serde,
            self.value_serde.clone(),
        )
    }

    /// `flatMap`: one record → zero or more `(K2, V2)`. Key-changing.
    pub fn flat_map<K2, V2, IT, F>(
        &self,
        f: F,
    ) -> KStream<K2, V2, <K2 as DefaultSerde>::Serde, <V2 as DefaultSerde>::Serde>
    where
        K: Default,
        K2: DefaultSerde + Any + Send + Clone,
        V2: DefaultSerde + Any + Send + Clone,
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            true,
            <K2 as DefaultSerde>::Serde::default(),
            <V2 as DefaultSerde>::Serde::default(),
        )
    }

    /// `flatMapValues`: one record → zero or more `V2`, key unchanged.
    pub fn flat_map_values<V2, IT, F>(
        &self,
        f: F,
    ) -> KStream<K, V2, KS, <V2 as DefaultSerde>::Serde>
    where
        V2: DefaultSerde + Any + Send + Clone,
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing,
            self.key_serde.clone(),
            <V2 as DefaultSerde>::Serde::default(),
        )
        .with_source_topic(self.source_topic.clone())
    }

    /// `peek`: observe each record, then forward it unchanged.
    #[must_use]
    pub fn peek<F>(&self, f: F) -> KStream<K, V, KS, VS>
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing,
            self.key_serde.clone(),
            self.value_serde.clone(),
        )
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

    /// `to`: write the stream to a topic through a sink (consumes the stream).
    pub fn to_explicit<KS2, VS2>(
        self,
        topic: impl Into<String>,
        produced: impl Into<Produced<KS2, VS2>>,
    ) where
        KS2: SerdeAssociate + Serde<K> + Clone,
        VS2: SerdeAssociate + Serde<V> + Clone,
    {
        let produced = produced.into();
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
            state.topology.add_sink_explicit::<K, V, KS2, VS2, _, _>(
                name.clone(),
                topic,
                [parent],
                produced,
            );
            // A sink is terminal; record its name so children (none) could find it.
            state.handle_name.insert(id, name.clone());
        }));
    }

    /// Write the stream to a topic using the default/carried serdes of the stream.
    pub fn to(self, topic: impl Into<String>)
    where
        KS: SerdeAssociate + Serde<K> + Clone,
        VS: SerdeAssociate + Serde<V> + Clone,
    {
        let key_serde = self.key_serde.clone();
        let value_serde = self.value_serde.clone();
        self.to_explicit(topic, Produced::with(key_serde, value_serde));
    }

    /// `merge`: union this stream with `other` (same K/V). Both feed one node.
    #[must_use]
    pub fn merge(&self, other: &KStream<K, V, KS, VS>) -> KStream<K, V, KS, VS> {
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
            self.key_serde.clone(),
            self.value_serde.clone(),
        )
    }

    /// `join` (inner stream-table join): look each stream record up in `table`.
    ///
    /// For each record on this stream, this method looks the record's key up in
    /// `table`'s materialized store. **Only when a value is present**, it
    /// forwards `joiner(&stream_value, &table_value)` keyed by the same key. It
    /// drops records whose key is absent from the table. This is an inner join.
    ///
    /// `table` must be materialized, that is, sourced through
    /// [`StreamsBuilder::table`] or any materialized op. The join reads its store
    /// by name. The join declares the stream side and the table's source topic as
    /// a **copartition group** (KIP-1071), so the streams-group coordinator
    /// co-locates their partitions.
    ///
    /// The stream key must be unchanged relative to its source partitioning,
    /// because a stream-table join is partition-local. If a key-changing op such
    /// as `map`, `select_key`, `flat_map`, or `group_by` comes before the join,
    /// call [`repartition`](Self::repartition) first to re-partition by the new
    /// key. If you do not, `join_table` panics.
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
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn join_table<VT, VO, F, VTS>(
        &self,
        table: &KTable<K, VT, KS, VTS>,
        joiner: F,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        VT: Any + Send + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        VTS: Clone,
        F: Fn(&V, &VT) -> VO + Clone + Send + Sync + 'static,
    {
        // Wrap the inner joiner to the left form `Fn(&V, Option<&VT>) -> VO`; with
        // `emit_on_miss = false` the closure is only ever called with `Some`.
        let lf = move |v: &V, opt: Option<&VT>| joiner(v, opt.expect("inner join hit"));
        self.join_table_impl::<VT, VO, _, VTS>(table, lf, false, None)
    }

    /// `leftJoin` (left stream-table join): like [`join_table`](Self::join_table)
    /// but always forwards a record for every stream record. On a table miss the
    /// `joiner` receives `None` for the table-side value. See
    /// [`join_table`](Self::join_table) for the naming-vs-JVM note.
    #[must_use]
    pub fn left_join_table<VT, VO, F, VTS>(
        &self,
        table: &KTable<K, VT, KS, VTS>,
        joiner: F,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        VT: Any + Send + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        VTS: Clone,
        F: Fn(&V, Option<&VT>) -> VO + Clone + Send + Sync + 'static,
    {
        self.join_table_impl::<VT, VO, _, VTS>(table, joiner, true, None)
    }

    /// `join` with [`Joined`] config (KIP-923 grace path).
    ///
    /// This method is identical to [`join_table`](Self::join_table) when
    /// `joined.grace` is `None`. When a grace period is set, the join buffers
    /// each stream record into a `JoinGraceBufferStore`. It drains the buffer
    /// as-of the record's own timestamp once the grace horizon passes.
    /// Out-of-order stream records therefore join against the table version that
    /// was current at the record's own timestamp.
    ///
    /// Grace **requires** the table to be versioned. See
    /// [`Materialized::as_versioned`]. `grace` must be strictly less than the
    /// table's `history_retention`.
    ///
    /// [`Materialized::as_versioned`]: crate::dsl::config::Materialized::as_versioned
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn join_table_with<VT, VO, F, VTS>(
        &self,
        table: &KTable<K, VT, KS, VTS>,
        joiner: F,
        join_config: Joined,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        VT: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        VTS: Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V: Sync,
        F: Fn(&V, &VT) -> VO + Clone + Send + Sync + 'static,
    {
        let lf = move |v: &V, opt: Option<&VT>| joiner(v, opt.expect("inner join hit"));
        let configured_grace = join_config.grace;
        drop(join_config);
        let grace =
            self.build_grace_lowering::<VT, VO, _, VTS>(table, lf.clone(), false, configured_grace);
        self.join_table_impl::<VT, VO, _, VTS>(table, lf, false, grace)
    }

    /// `leftJoin` with [`Joined`] config (KIP-923 grace path). Like
    /// [`left_join_table`](Self::left_join_table) but, when `joined.grace` is
    /// set, wires the grace buffer + as-of drain (see
    /// [`join_table_with`](Self::join_table_with)). On a table miss at drain time
    /// the joiner receives `None`.
    #[must_use]
    pub fn left_join_table_with<VT, VO, F, VTS>(
        &self,
        table: &KTable<K, VT, KS, VTS>,
        joiner: F,
        join_config: Joined,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        VT: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        VTS: Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V: Sync,
        F: Fn(&V, Option<&VT>) -> VO + Clone + Send + Sync + 'static,
    {
        let configured_grace = join_config.grace;
        drop(join_config);
        let grace = self.build_grace_lowering::<VT, VO, _, VTS>(
            table,
            joiner.clone(),
            true,
            configured_grace,
        );
        self.join_table_impl::<VT, VO, _, VTS>(table, joiner, true, grace)
    }

    /// Build the [`GraceLowering`] closure for a KIP-923 grace join.
    ///
    /// Returns `None` when `joined.grace` is unset. This method holds the `Serde`
    /// and `Sync` bounds that the grace buffer store and the processor need,
    /// which the type-erased [`join_table_impl`](Self::join_table_impl) does not
    /// hold. It also checks the versioned-table precondition and the
    /// `grace < history_retention` precondition up front.
    ///
    /// The impl's lowering thunk runs the returned closure once. The closure
    /// rebuilds the typed stream parent handle, registers the grace processor and
    /// its `<join_name>-Buffer` store, connects the join node to the buffer, and
    /// returns the join node handle. The topology layer derives the changelog
    /// name `app-<join_name>-Buffer-changelog`.
    fn build_grace_lowering<VT, VO, LF, VTS>(
        &self,
        table: &KTable<K, VT, KS, VTS>,
        left_form: LF,
        emit_on_miss: bool,
        grace: Option<Time>,
    ) -> Option<GraceLowering<K, VO>>
    where
        VT: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        VTS: Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V: Sync,
        LF: Fn(&V, Option<&VT>) -> VO + Clone + Send + Sync + 'static,
    {
        let grace = grace?;
        // KIP-923 grace requires a versioned table (the drain does as-of lookups)
        // and `grace` strictly below the table's history retention.
        let retention = table
            .versioned_retention
            .expect("grace requires a versioned table");
        assert!(grace < retention, "grace must be < history_retention");

        let key_serde = self.key_serde.clone();
        let value_serde = self.value_serde.clone();
        Some(Box::new(
            move |state: &mut LowerState,
                  parent_id: NodeId,
                  join_name: String,
                  table_store: String| {
                let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
                // KIP-923: buffer store named `<join_name>-Buffer`; the join node
                // connects to BOTH the buffer (here) and the table store (in the
                // impl). Same KSTREAM-JOIN node as a plain stream-table join.
                let buffer_name = format!("{join_name}-Buffer");
                let store_for_proc = table_store.clone();
                let buffer_for_proc = buffer_name.clone();
                let lf = left_form.clone();
                let h = state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_name.clone(),
                    move || crate::dsl::processors::join_grace::KStreamKTableJoinGraceProcessor {
                        table_store: store_for_proc.clone(),
                        buffer_store: buffer_for_proc.clone(),
                        grace,
                        joiner: lf.clone(),
                        emit_on_miss,
                        observed_stream_time: i64::MIN,
                        _pd: PhantomData,
                    },
                    [parent],
                );
                // Register the grace buffer store (holds stream (K,V)) and connect
                // the join node to it.
                state.topology.add_join_grace_store::<K, V, KS, VS>(
                    buffer_name.clone(),
                    key_serde.clone(),
                    value_serde.clone(),
                    true,
                    [h.name().to_string()],
                );
                state
                    .topology
                    .connect_processor_store(h.name(), &buffer_name);
                h
            },
        ))
    }

    /// Shared lowering for the inner and left stream-table join.
    ///
    /// `left_form` is the `Fn(&V, Option<&VT>) -> VO` joiner. `emit_on_miss` is
    /// `false` for an inner join and `true` for a left join.
    ///
    /// This method records a `KSTREAM-JOIN-` processor node wired to this
    /// stream's node. Its thunk then does three things. It builds a
    /// [`KStreamKTableJoinProcessor`] that reads the table's store. It calls
    /// `connect_processor_store` to connect the join to that store, and the union
    /// pulls the join into the SAME subtopology as the table source, so both
    /// sources land together. It declares the `(stream_member, table_source)`
    /// copartition group when both members are single source topics.
    fn join_table_impl<VT, VO, LF, VTS>(
        &self,
        table: &KTable<K, VT, KS, VTS>,
        left_form: LF,
        emit_on_miss: bool,
        grace: Option<GraceLowering<K, VO>>,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        VT: Any + Send + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        VTS: Clone,
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
        // Versioned tables (KIP-889 history retention set) route to the as-of
        // join processor (KIP-914): the lookup is `get_as_of(key, streamRec.ts)`
        // instead of latest `get`. The `table` handle is not available inside the
        // lowering thunk, so capture this flag here.
        let table_versioned = table.versioned_retention.is_some();
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
            // Three processor branches, all registering the SAME node name with the
            // SAME parent + (below) the SAME table-store wiring + copartition group:
            //   grace (Some)  → grace processor + its own buffer store (built by the
            //                   boxed `GraceLowering` closure, which holds the
            //                   stream-serde + `V: Sync` bounds the buffer needs),
            //   versioned     → as-of `get_as_of` processor,
            //   plain         → latest `get` processor.
            let h = if let Some(grace_lower) = grace {
                // The closure rebuilds the typed parent handle, registers the grace
                // processor + `<join_name>-Buffer` store, and connects the join to
                // the buffer. The table-store connect + copartition happen below.
                grace_lower(state, parent_id, join_name.clone(), store_for_thunk.clone())
            } else if table_versioned {
                let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
                let store_for_proc = store_for_thunk.clone();
                let lf = left_form.clone();
                state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_name.clone(),
                    move || crate::dsl::processors::join::KStreamKTableJoinAsOfProcessor {
                        table_store: store_for_proc.clone(),
                        joiner: lf.clone(),
                        emit_on_miss,
                        _pd: PhantomData,
                    },
                    [parent],
                )
            } else {
                let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
                let store_for_proc = store_for_thunk.clone();
                let lf = left_form.clone();
                state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_name.clone(),
                    move || KStreamKTableJoinProcessor {
                        table_store: store_for_proc.clone(),
                        joiner: lf.clone(),
                        emit_on_miss,
                        _pd: PhantomData,
                    },
                    [parent],
                )
            };
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            join_id,
            false,
            self.key_serde.clone(),
            <VO as DefaultSerde>::Serde::default(),
        )
    }

    /// `join` (inner stream-globaltable join): look each record up in `global`.
    ///
    /// For each record on this stream, this method computes the lookup key
    /// `gk = key_mapper(&streamKey, &streamValue)` and looks it up in `global`'s
    /// fully-replicated store. **Only on a hit**, it forwards
    /// `joiner(&stream_value, &global_value)` keyed by the **stream key** with
    /// the stream timestamp. It drops records whose derived key is absent.
    ///
    /// The lookup key is *derived* from the record, unlike
    /// [`join_table`](Self::join_table), so it can differ from the stream key. A
    /// `GlobalKTable` is fully replicated, so there is **no copartitioning, no
    /// repartition, and no key-changing assertion**. Any record can look up any
    /// key on every instance.
    ///
    /// [`GlobalKTable`]: crate::dsl::global_table::GlobalKTable
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn join_global<GK, VG, VR, KM, J, GKS, VGS>(
        &self,
        global: &crate::dsl::global_table::GlobalKTable<GK, VG, GKS, VGS>,
        key_mapper: KM,
        joiner: J,
    ) -> KStream<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        GK: Any + Send + Sync + 'static,
        VG: Any + Send + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        GKS: Clone,
        VGS: Clone,
        KM: Fn(&K, &V) -> GK + Clone + Send + Sync + 'static,
        J: Fn(&V, &VG) -> VR + Clone + Send + Sync + 'static,
    {
        // Wrap the inner joiner to the left form `Fn(&V, Option<&VG>) -> VR`; with
        // `emit_on_miss = false` the closure is only ever called with `Some`.
        let jf = move |v: &V, opt: Option<&VG>| joiner(v, opt.expect("inner global join hit"));
        self.join_global_impl::<GK, VG, VR, KM, _, GKS, VGS>(global, key_mapper, jf, false)
    }

    /// `leftJoin` (left stream-globaltable join): like
    /// [`join_global`](Self::join_global) but always forwards a record for every
    /// stream record. On a global-store miss the `joiner` receives `None` for the
    /// global-side value.
    #[must_use]
    pub fn left_join_global<GK, VG, VR, KM, J, GKS, VGS>(
        &self,
        global: &crate::dsl::global_table::GlobalKTable<GK, VG, GKS, VGS>,
        key_mapper: KM,
        joiner: J,
    ) -> KStream<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        GK: Any + Send + Sync + 'static,
        VG: Any + Send + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        GKS: Clone,
        VGS: Clone,
        KM: Fn(&K, &V) -> GK + Clone + Send + Sync + 'static,
        J: Fn(&V, Option<&VG>) -> VR + Clone + Send + Sync + 'static,
    {
        self.join_global_impl::<GK, VG, VR, KM, _, GKS, VGS>(global, key_mapper, joiner, true)
    }

    /// Shared lowering for the inner and left stream-globaltable join.
    ///
    /// `left_form` is the `Fn(&V, Option<&VG>) -> VR` joiner. `emit_on_miss` is
    /// `false` for an inner join and `true` for a left join.
    ///
    /// This method records a `KSTREAM-GLOBALTABLE-JOIN-` processor node wired to
    /// this stream's node. Its thunk builds a
    /// [`KStreamGlobalTableJoinProcessor`] that reads the global table's store by
    /// name through the global registry accessor. Unlike the stream-table join,
    /// there is **no** copartition group and **no** `connect_processor_store`,
    /// because the global store is fully replicated and the processor reaches it
    /// through the global registry, not through a copartitioned subtopology.
    /// There is also **no** `key_changing` assertion, because the lookup key is
    /// derived and is not the stream key.
    fn join_global_impl<GK, VG, VR, KM, LF, GKS, VGS>(
        &self,
        global: &crate::dsl::global_table::GlobalKTable<GK, VG, GKS, VGS>,
        key_mapper: KM,
        left_form: LF,
        emit_on_miss: bool,
    ) -> KStream<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        GK: Any + Send + Sync + 'static,
        VG: Any + Send + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        GKS: Clone,
        VGS: Clone,
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            join_id,
            false,
            self.key_serde.clone(),
            <VR as DefaultSerde>::Serde::default(),
        )
    }

    /// `join` (windowed inner stream-stream join): match records over a window.
    ///
    /// For each record on either stream, the join puts the record into its own
    /// [`JoinWindowStore`] and fetches the OTHER stream's store over the join
    /// window. It forwards one output per match, keyed by the same key. A left
    /// record at `t` matches right records with a timestamp in
    /// `[t - before, t + after]`. The rule is symmetric: the per-side OTHER
    /// processor swaps `before` and `after`, so the rule holds whichever side
    /// drives the record.
    ///
    /// The join lowers to a **THIS** processor fed by this stream, an **OTHER**
    /// processor fed by `other`, and a **MERGE** node that unions the two
    /// outputs. This mirrors [`KTable::join`]'s dual+merge shape. Each side puts
    /// into its own `retainDuplicates` join-window store and reads the other
    /// side's store. Each side connects to BOTH stores, so the grouping pass
    /// folds A, B, both stores, and the two join nodes into one copartitioned
    /// subtopology. When both streams are single-source-topic streams, the join
    /// declares their source topics as a copartition group (KIP-1071).
    ///
    /// Both stream keys must be unchanged relative to their source partitioning.
    /// You must call `repartition()` after a key-changing op upstream. If you do
    /// not, the join panics.
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
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn join<V2, VO, F, V2S>(
        &self,
        other: &KStream<K, V2, KS, V2S>,
        joiner: F,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, VS, V2S>,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        V2: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        F: Fn(&V, &V2) -> VO + Clone + Send + Sync + 'static,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        // Lift the inner joiner to the shared outer form: a match passes `Some`/`Some`
        // (a null result never occurs for an inner join, so `expect` is unreachable).
        let j =
            move |a: Option<&V>, b: Option<&V2>| joiner(a.expect("inner a"), b.expect("inner b"));
        let j: SharedOuterJoiner<V, V2, VO> = Arc::new(j);
        self.join_impl::<V2, VO, V2S>(other, &j, windows, stream_joined, JoinKind::inner())
    }

    /// `leftJoin` (windowed left stream-stream join): emit unmatched left records.
    ///
    /// Every record on THIS (left) stream that finds no match in the OTHER
    /// (right) window emits `joiner(&this, None)` once that record's window
    /// closes. Stream time drives the window close (KIP-633). Matched records
    /// emit `joiner(&this, Some(&other))`, as in the inner join. The right side
    /// never emits a non-join.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn left_join<V2, VO, F, V2S>(
        &self,
        other: &KStream<K, V2, KS, V2S>,
        joiner: F,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, VS, V2S>,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        V2: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        F: Fn(&V, Option<&V2>) -> VO + Clone + Send + Sync + 'static,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        // Left form: the left (A) side may receive `None` for B; the A side is always
        // present (the join never fires from a non-existent A).
        let j = move |a: Option<&V>, b: Option<&V2>| joiner(a.expect("left a"), b);
        let j: SharedOuterJoiner<V, V2, VO> = Arc::new(j);
        self.join_impl::<V2, VO, V2S>(other, &j, windows, stream_joined, JoinKind::left())
    }

    /// `outerJoin` (windowed outer stream-stream join): emit from either side.
    ///
    /// Every record on EITHER side that finds no match emits
    /// `joiner(Some, None)` or `joiner(None, Some)` once its window closes
    /// (KIP-633). Matched records emit `joiner(Some, Some)`.
    #[must_use]
    pub fn outer_join<V2, VO, F, V2S>(
        &self,
        other: &KStream<K, V2, KS, V2S>,
        joiner: F,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, VS, V2S>,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        V2: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        F: Fn(Option<&V>, Option<&V2>) -> VO + Clone + Send + Sync + 'static,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        // The user joiner is already in outer form.
        let joiner: SharedOuterJoiner<V, V2, VO> = Arc::new(joiner);
        self.join_impl::<V2, VO, V2S>(other, &joiner, windows, stream_joined, JoinKind::outer())
    }

    /// Shared dual+merge lowering for windowed stream-stream joins.
    ///
    /// `outer_joiner` is the shared outer-form joiner
    /// `Fn(Option<&V>, Option<&V2>) -> VO`. Each per-side processor wraps it, so
    /// a match passes the present sides. The `kind` decides which side emits
    /// non-joins. **THIS (A)** emits when B is not required
    /// (`!kind.b_required`), that is, for the left and outer joins. **OTHER (B)**
    /// emits when A is not required (`!kind.a_required`), that is, for the outer
    /// join only.
    ///
    /// The lowering mirrors [`KTable::join`]'s dual+merge shape. It creates a
    /// **THIS** processor fed by this stream, an **OTHER** processor fed by
    /// `other`, and a **MERGE** node. Each side puts into its own
    /// `retainDuplicates` join-window store and reads the other side's store. For
    /// the left and outer joins, one shared `KSTREAM-OUTERSHARED-` KV store
    /// buffers unmatched records, and one shared [`TimeTracker`] drives the
    /// window-close emission. Both supplier closures get a clone of that tracker,
    /// and both join processors connect to that store. For the inner join, this
    /// method creates no outer store and no tracker, so the wire topology is
    /// byte-identical to the inner-only golden.
    ///
    /// [`KTable::join`]: crate::dsl::ktable::KTable::join
    /// [`TimeTracker`]: crate::dsl::processors::outer_join_store::TimeTracker
    // dual+merge lowering: 3 nodes + shared outer store, each a typed thunk
    fn join_impl<V2, VO, V2S>(
        &self,
        other: &KStream<K, V2, KS, V2S>,
        outer_joiner: &SharedOuterJoiner<V, V2, VO>,
        windows: JoinWindows,
        stream_joined: StreamJoined<KS, VS, V2S>,
        kind: JoinKind,
    ) -> KStream<K, VO, KS, <VO as DefaultSerde>::Serde>
    where
        V2: Any + Send + Sync + Clone,
        VO: DefaultSerde + Any + Send + Clone,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        V2S: Serde<V2> + Clone + 'static,
        V: Send + Sync,
    {
        let a_src = self.source_topic.clone();
        let b_src = other.source_topic.clone();

        let parent_id = self.node;
        let other_parent_id = other.node;

        let (before, after, grace) = (windows.before, windows.after, windows.grace);

        let mut g = self.builder.borrow_mut();
        let StreamJoinGraph {
            join_this,
            join_other,
            this_store,
            other_store,
            outer_store,
            this_id,
            other_id,
            merge,
        } = allocate_stream_join_graph(
            &mut g,
            (parent_id, other_parent_id),
            (kind.a_required, kind.b_required),
        );

        let this_store_clone = this_store.clone();
        let other_store_clone = other_store.clone();
        let outer_store_clone = outer_store.clone();

        // One shared TimeTracker tracks the maximum observed stream time across both
        // partition driving threads (synchronizing the window close).
        let tracker = Arc::new(Mutex::new(TimeTracker::default()));

        // We capture both serdes by cloning the stream_joined fields; since they are
        // cloneable wrappers, this moves them into the lowering closures safely.
        let StreamJoined {
            key: ks,
            left_value: vs1,
            right_value: vs2,
        } = stream_joined;

        // ── THIS side: fed by `self`; puts into `this_store`, reads `other_store`.
        {
            let own = this_store_clone.clone();
            let other_s = other_store_clone.clone();
            let outer_store_this = outer_store_clone.clone();
            let tracker_this = Arc::clone(&tracker);
            let ks_for_proc = ks.clone();
            let vs_for_proc = vs1.clone();
            let oj = Arc::clone(outer_joiner);
            let this_emit = !kind.b_required; // this side emits non-joins (left & outer)

            g.graph.nodes[this_id].lower = Some(Box::new(move |state: &mut LowerState| {
                let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
                let own_for_proc = own.clone();
                let other_for_proc = other_s.clone();
                let outer_store_proc = outer_store_this.clone();
                let tracker_proc = Arc::clone(&tracker_this);
                let ks = ks_for_proc.clone();
                let vs = vs_for_proc.clone();
                let oj = Arc::clone(&oj);

                let ks_for_proc_inner = ks.clone();
                let vs_for_proc_inner = vs.clone();
                let outer_store_for_proc = outer_store_proc.clone();
                let h = state.topology.add_processor::<K, V, K, VO, _, _, _>(
                    join_this.clone(),
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
                        outer_store: outer_store_for_proc.clone(),
                        tracker: outer_store_for_proc
                            .as_ref()
                            .map(|_| Arc::clone(&tracker_proc)),
                        key_serde: outer_store_for_proc
                            .as_ref()
                            .map(|_| Box::new(ks_for_proc_inner.clone()) as Box<dyn Serde<K>>),
                        value_serde: outer_store_for_proc
                            .as_ref()
                            .map(|_| Box::new(vs_for_proc_inner.clone()) as Box<dyn Serde<V>>),
                        before,
                        after,
                        grace,
                        _pd: PhantomData,
                    },
                    [parent],
                );
                // Register the THIS store (holds V) + connect to BOTH stores.
                state.topology.add_join_window_store::<K, V, KS, VS>(
                    own.clone(),
                    ks.clone(),
                    vs.clone(),
                    (before, after, grace),
                    [h.name().to_string()],
                );
                state.topology.connect_processor_store(h.name(), &own);
                state.topology.connect_processor_store(h.name(), &other_s);
                // For left/outer, register the shared outer KV store ONCE here and
                // connect this processor to it. (The OTHER thunk only connects.)
                if let Some(os) = &outer_store_proc {
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
        {
            let own = other_store_clone.clone();
            let other_s = this_store_clone.clone();
            let outer_store_other = outer_store_clone.clone();
            let tracker_other = Arc::clone(&tracker);
            let ks_for_proc = ks.clone();
            let vs_for_proc = vs2.clone();
            let oj = Arc::clone(outer_joiner);
            let other_emit = !kind.a_required; // other side emits non-joins (outer only)

            g.graph.nodes[other_id].lower = Some(Box::new(move |state: &mut LowerState| {
                let parent =
                    NodeHandle::<K, V2>::from_name(state.handle_name[&other_parent_id].clone());
                let own_for_proc = own.clone();
                let other_for_proc = other_s.clone();
                let outer_store_proc = outer_store_other.clone();
                let tracker_proc = Arc::clone(&tracker_other);
                let ks = ks_for_proc.clone();
                let vs = vs_for_proc.clone();
                let oj = Arc::clone(&oj);

                let ks_for_proc_inner = ks.clone();
                let vs_for_proc_inner = vs.clone();
                let outer_store_for_proc = outer_store_proc.clone();
                let h = state.topology.add_processor::<K, V2, K, VO, _, _, _>(
                    join_other.clone(),
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
                        outer_store: outer_store_for_proc.clone(),
                        tracker: outer_store_for_proc
                            .as_ref()
                            .map(|_| Arc::clone(&tracker_proc)),
                        key_serde: outer_store_for_proc
                            .as_ref()
                            .map(|_| Box::new(ks_for_proc_inner.clone()) as Box<dyn Serde<K>>),
                        value_serde: outer_store_for_proc
                            .as_ref()
                            .map(|_| Box::new(vs_for_proc_inner.clone()) as Box<dyn Serde<V2>>),
                        before,
                        after,
                        grace,
                        _pd: PhantomData,
                    },
                    [parent],
                );
                // Register the OTHER store (holds V2) + connect to BOTH stores.
                state.topology.add_join_window_store::<K, V2, KS, V2S>(
                    own.clone(),
                    ks.clone(),
                    vs.clone(),
                    (before, after, grace),
                    [h.name().to_string()],
                );
                state.topology.connect_processor_store(h.name(), &own);
                state.topology.connect_processor_store(h.name(), &other_s);
                // For left/outer, connect this processor to the shared outer store
                // (registered by the THIS thunk).
                if let Some(os) = &outer_store_proc {
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            merge_id,
            false,
            self.key_serde.clone(),
            <VO as DefaultSerde>::Serde::default(),
        )
    }

    /// `repartition`: force a repartition through an internal topic.
    ///
    /// This op lowers as `sink → add_repartition_topic → source`. It is the same
    /// pattern that the implicit repartition before a stateful aggregation uses.
    /// The repartition topic name is `<app_id>-<name>-repartition`. `<name>` is
    /// the explicit [`Repartitioned`](crate::dsl::config::Repartitioned) name when
    /// set, otherwise an auto-name minted from the counter.
    ///
    /// **Byte-exactness vs JVM:** the JVM assigns a distinct
    /// `KSTREAM-REPARTITION-` counter for standalone `repartition()` calls. No
    /// golden fixture validates that counter. The bar here is functional
    /// correctness: no panic, and records flow through.
    #[must_use]
    pub fn repartition<KS2, VS2>(
        &self,
        repartitioned: crate::dsl::config::Repartitioned<KS2, VS2>,
    ) -> KStream<K, V, KS2, VS2>
    where
        KS2: Serde<K> + Clone + 'static,
        VS2: Serde<V> + Clone + 'static,
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
        let key_serde_clone = key_serde.clone();
        let value_serde_clone = value_serde.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent_name = state.handle_name[&parent_id].clone();
            let parent = NodeHandle::<K, V>::from_name(parent_name);
            let topic = format!("{}-{topic_base}{}", state.app_id, names::REPARTITION_SUFFIX);
            // sink: write to repartition topic
            state.topology.add_sink_explicit::<K, V, KS2, VS2, _, _>(
                sink_name.clone(),
                topic.clone(),
                [parent],
                crate::processor::serde::Produced::with(
                    key_serde_clone.clone(),
                    value_serde_clone.clone(),
                ),
            );
            // mark the topic as internal repartition (loop-back)
            state.topology.add_repartition_topic(topic.clone());
            // source: read from repartition topic
            state.topology.add_source_explicit::<K, V, KS2, VS2>(
                source_name.clone(),
                [topic],
                crate::processor::serde::Consumed::with(
                    key_serde_clone.clone(),
                    value_serde_clone.clone(),
                ),
            );
            state.handle_name.insert(id, source_name.clone());
        }));
        drop(g);
        // An explicit repartition re-groups by key → downstream is no longer
        // key-changing relative to its (now repartitioned) partitioning.
        KStream::new_with_key_changing(Rc::clone(&self.builder), id, false, key_serde, value_serde)
    }

    /// `split`: begin a branching fan-out.
    ///
    /// Returns a [`BranchedStream`] builder. Individual
    /// [`branch`](BranchedStream::branch) calls on that builder create filtered
    /// child streams. The split itself adds no node to the topology. Each
    /// `branch` call creates a filter-backed child wired directly to this
    /// stream's node.
    ///
    /// **Simplification vs JVM:** each branch receives a record when its predicate
    /// matches, not just the first matching branch. For mutually-exclusive
    /// predicates the behaviour is identical to the JVM first-match-wins semantics.
    ///
    /// [`branch`]: BranchedStream::branch
    #[must_use]
    pub fn split(&self) -> BranchedStream<K, V, KS, VS> {
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
            key_serde: self.key_serde.clone(),
            value_serde: self.value_serde.clone(),
            _pd: std::marker::PhantomData,
        }
    }

    /// `groupByKey`: group by the existing key, ready for an aggregation.
    ///
    /// This op records no graph node. A terminal `count`, `reduce`, or
    /// `aggregate` call records the optional repartition and the aggregate node.
    /// The returned [`KGroupedStream`] carries two things: whether the upstream
    /// key lineage is key-changing, which forces the aggregation to insert a
    /// repartition, and a typed repartition-lowering thunk built from the
    /// `Grouped` serdes.
    pub fn group_by_key_explicit<GKS, GVS>(
        &self,
        grouped: impl Into<Grouped<GKS, GVS>>,
    ) -> KGroupedStream<K, V>
    where
        GKS: Serde<K> + Clone + 'static,
        GVS: Serde<V> + Clone + 'static,
    {
        let grouped = grouped.into();
        KGroupedStream::new(
            Rc::clone(&self.builder),
            self.node,
            self.key_changing,
            grouped.name,
            crate::dsl::kgrouped::repartition_lower::<K, V, GKS, GVS>(
                grouped.key_serde,
                grouped.value_serde,
            ),
        )
        .with_source_topic(if self.key_changing {
            None
        } else {
            self.source_topic.clone()
        })
    }

    /// `groupByKey` using the stream's existing serdes.
    pub fn group_by_key(&self) -> KGroupedStream<K, V>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        KGroupedStream::new(
            Rc::clone(&self.builder),
            self.node,
            self.key_changing,
            None,
            crate::dsl::kgrouped::repartition_lower::<K, V, KS, VS>(
                self.key_serde.clone(),
                self.value_serde.clone(),
            ),
        )
        .with_source_topic(if self.key_changing {
            None
        } else {
            self.source_topic.clone()
        })
    }

    /// `groupBy`: re-key with `f`, then group by the new key.
    ///
    /// This op is equivalent to `select_key(f).group_by_key_explicit(grouped)`.
    /// The key change forces a repartition before any later aggregation.
    pub fn group_by<GKS, GVS, F>(
        &self,
        f: F,
        grouped: impl Into<Grouped<GKS, GVS>>,
    ) -> KGroupedStream<GKS::Target, V>
    where
        K: Default,
        GKS: SerdeAssociate + Serde<GKS::Target> + Clone + 'static,
        GVS: SerdeAssociate + Serde<V> + Clone + 'static,
        GKS::Target: Any + Send + Sync + Clone,
        F: Fn(&K, &V) -> GKS::Target + Clone + Send + Sync + 'static,
    {
        let grouped = grouped.into();
        self.select_key_with_serde(f, grouped.key_serde.clone())
            .group_by_key_explicit(grouped)
    }

    /// `process`: attach a custom Processor-API node that may rewrite the key.
    ///
    /// `supplier` is any user [`ProcessorSupplier`], for example a `|| MyProc`
    /// closure. `store_names` names the stores that
    /// [`StreamsBuilder::add_state_store`] registered and that the processor
    /// reads and writes through [`ProcessorContext::get_state_store`]. This
    /// method looks up each named store's connect thunk **now**, so you must add
    /// the store before this call. Lowering then invokes each thunk to register
    /// the store, emit its compact `<app>-<store>-changelog` changelog, and
    /// connect the store to this processor node.
    ///
    /// This op mirrors the JVM `KStream.process`. It treats the result as
    /// **key-changing**, because the processor may call `forward` with any key.
    /// The single-source-topic lineage therefore breaks, and a downstream
    /// aggregation or join must `repartition` first.
    ///
    /// # Panics
    /// Panics if any name in `store_names` was not registered through
    /// [`add_state_store`](crate::dsl::builder::StreamsBuilder::add_state_store).
    ///
    /// [`ProcessorSupplier`]: crate::processor::api::ProcessorSupplier
    /// [`ProcessorContext::get_state_store`]: crate::processor::api::ProcessorContext::get_state_store
    /// [`StreamsBuilder::add_state_store`]: crate::dsl::builder::StreamsBuilder::add_state_store
    pub fn process<KOut, VOut, PS>(
        &self,
        supplier: PS,
        store_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> KStream<KOut, VOut, <KOut as DefaultSerde>::Serde, <VOut as DefaultSerde>::Serde>
    where
        KOut: DefaultSerde + Any + Send + Sync + Clone,
        VOut: DefaultSerde + Any + Send + Clone,
        PS: crate::processor::api::ProcessorSupplier<K, V, KOut, VOut> + Clone + 'static,
    {
        let stores: Vec<String> = store_names.into_iter().map(Into::into).collect();
        // `add_state_store` must precede `process`; look up each connect thunk now so
        // a missing store panics here (at call time) rather than during lowering.
        let thunks: Vec<crate::dsl::builder::StoreConnectThunk> = {
            let g = self.builder.borrow();
            stores
                .iter()
                .map(|s| {
                    g.store_thunk(s).unwrap_or_else(|| {
                        panic!(
                            "process references store '{s}' that was not added via add_state_store"
                        )
                    })
                })
                .collect()
        };
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KSTREAM_PROCESSOR);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        g.graph.nodes[id].key_changing_operation = true;
        // The supplier is moved into the lower thunk; the thunk clones it per task
        // when `add_processor` runs (`PS: Clone`), so the supplier closure type need
        // not itself be re-instantiable.
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let h = state.topology.add_processor::<K, V, KOut, VOut, _, _, _>(
                name.clone(),
                supplier.clone(),
                [parent],
            );
            let proc_name = h.name().to_string();
            // Register + connect each named store to this processor (the connect
            // thunks carry the store serdes and emit the compact changelog).
            for t in &thunks {
                t(state, &proc_name);
            }
            state.handle_name.insert(id, proc_name);
        }));
        drop(g);
        // process MAY change the key → key-changing; source-topic lineage broken.
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            true,
            <KOut as DefaultSerde>::Serde::default(),
            <VOut as DefaultSerde>::Serde::default(),
        )
    }

    /// `processValues`: attach a custom fixed-key Processor-API node (KIP-820).
    ///
    /// The node may rewrite the value but NOT the key.
    ///
    /// `supplier` is any [`FixedKeyProcessorSupplier`], for example a
    /// `|| MyFixedProc` closure. `store_names` names the stores that
    /// [`add_state_store`] registered and that the processor reads and writes
    /// through [`FixedKeyProcessorContext::get_state_store`]. As with
    /// [`process`](Self::process), this method looks up each named store's
    /// connect thunk **now**, so you must add the store before this call.
    /// Lowering then invokes each thunk to register the store, emit its compact
    /// `<app>-<store>-changelog` changelog, and connect the store to this
    /// processor node.
    ///
    /// Unlike [`process`](Self::process), this op keeps the key. The result is
    /// therefore **non-key-changing** and carries the single-source-topic lineage
    /// unchanged, so a downstream aggregation or join needs no repartition. An
    /// internal adapter wraps the supplier, so the runtime sees a
    /// `Processor<K, V, K, VOut>`.
    ///
    /// # Panics
    /// Panics if any name in `store_names` was not registered through
    /// [`add_state_store`](crate::dsl::builder::StreamsBuilder::add_state_store).
    ///
    /// [`FixedKeyProcessorSupplier`]: crate::processor::fixed_key::FixedKeyProcessorSupplier
    /// [`FixedKeyProcessorContext::get_state_store`]: crate::processor::fixed_key::FixedKeyProcessorContext::get_state_store
    /// [`add_state_store`]: crate::dsl::builder::StreamsBuilder::add_state_store
    pub fn process_values<VOut, PS>(
        &self,
        supplier: PS,
        store_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> KStream<K, VOut, KS, <VOut as DefaultSerde>::Serde>
    where
        VOut: DefaultSerde + Any + Send + Clone,
        PS: crate::processor::fixed_key::FixedKeyProcessorSupplier<K, V, VOut> + Clone + 'static,
    {
        let stores: Vec<String> = store_names.into_iter().map(Into::into).collect();
        // `add_state_store` must precede `process_values`; look up each connect thunk
        // now so a missing store panics here (at call time) not during lowering.
        let thunks: Vec<crate::dsl::builder::StoreConnectThunk> = {
            let g = self.builder.borrow();
            stores
                .iter()
                .map(|s| {
                    g.store_thunk(s).unwrap_or_else(|| {
                        panic!(
                            "process_values references store '{s}' that was not added \
                             via add_state_store"
                        )
                    })
                })
                .collect()
        };
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::KSTREAM_PROCESSVALUES);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::StatelessProcessor {
                repartition_required: false,
            },
            vec![parent_id],
        );
        // The supplier is moved into the lower thunk; the thunk clones it per task
        // when `add_processor` runs (`PS: Clone`). A FixedKey supplier becomes a
        // regular `ProcessorSupplier` producing a `FixedKeyAdapter` (KOut = K), which
        // impls `Processor<K, V, K, VOut>` because `Box<dyn FixedKeyProcessor>` is a
        // `FixedKeyProcessor` (the fixed_key blanket impl).
        let sup = supplier;
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let sup2 = sup.clone();
            let h = state.topology.add_processor::<K, V, K, VOut, _, _, _>(
                name.clone(),
                move || crate::processor::fixed_key::FixedKeyAdapter { inner: sup2.get() },
                [parent],
            );
            let proc_name = h.name().to_string();
            // Register + connect each named store to this processor (the connect
            // thunks carry the store serdes and emit the compact changelog).
            for t in &thunks {
                t(state, &proc_name);
            }
            state.handle_name.insert(id, proc_name);
        }));
        drop(g);
        // process_values keeps the key → NON-key-changing; carry source-topic lineage.
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing,
            self.key_serde.clone(),
            <VOut as DefaultSerde>::Serde::default(),
        )
        .with_source_topic(self.source_topic.clone())
    }

    /// `toTable`: materialize this stream into a [`KTable`].
    ///
    /// The op writes each record into a state store and forwards a `Change<V>`
    /// change-stream that carries the prior store value as `old`.
    ///
    /// `to_table` carries the key through unchanged, so it never inserts a
    /// repartition. The JVM repartitions only when the upstream key is rewritten
    /// without a re-group. The store name is `Materialized`'s explicit name when
    /// set, otherwise a fresh `KSTREAM-TOTABLE-STATE-STORE-` counter. The store
    /// gets the standard `<app>-<store>-changelog` changelog, or no changelog
    /// when [`Materialized::with_logging(false)`].
    ///
    /// [`Materialized::with_logging(false)`]: crate::dsl::config::Materialized::with_logging
    pub fn to_table_explicit<NKS, NVS>(
        &self,
        materialized: impl Into<Materialized<NKS, NVS>>,
    ) -> KTable<K, V, NKS, NVS>
    where
        NKS: Serde<K> + Clone + 'static,
        NVS: Serde<V> + Clone + 'static,
    {
        let materialized = materialized.into();
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
            caching,
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
        let key_serde_for_lower = key_serde.clone();
        let value_serde_for_lower = value_serde.clone();
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent = NodeHandle::<K, V>::from_name(state.handle_name[&parent_id].clone());
            let store_for_proc = store_for_thunk.clone();
            // The stream → table boundary forwards Change<V> (prior store value as old).
            let h = state.topology.add_processor::<K, V, K, Change<V>, _, _, _>(
                name.clone(),
                move || KStreamToTableProcessor {
                    store_name: store_for_proc.clone(),
                    forwarder: TupleForwarder::default(),
                    _pd: PhantomData,
                },
                [parent],
            );
            // Honor `Materialized::with_logging(bool)`, mirroring the aggregate ops:
            // logging=true → changelog topic emitted; logging=false → store usable
            // at runtime but no state_changelog_topics entry in the wire topology.
            if logging {
                state.topology.add_state_store::<K, V, NKS, NVS>(
                    store_for_thunk.clone(),
                    key_serde_for_lower.clone(),
                    value_serde_for_lower.clone(),
                    [h.name().to_string()],
                );
            } else {
                state
                    .topology
                    .add_state_store_no_changelog::<K, V, NKS, NVS>(
                        store_for_thunk.clone(),
                        key_serde_for_lower.clone(),
                        value_serde_for_lower.clone(),
                    );
            }
            // Mark the store cached per `Materialized::with_caching` (default true);
            // the to_table processor's TupleForwarder suppresses immediate forwards
            // when cached and the cache flush forwards the deduped change.
            state.topology.mark_store_caching(&store_for_thunk, caching);
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            id,
            Some(store_name),
            None,
            key_serde,
            value_serde,
        )
    }

    /// Sourced `KTable` from this stream using the stream's carried serdes.
    pub fn to_table(&self, store_name: impl Into<String>) -> KTable<K, V, KS, VS>
    where
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
    {
        self.to_table_explicit(
            Materialized::with(self.key_serde.clone(), self.value_serde.clone())
                .as_store(store_name),
        )
    }
}

// ---------------------------------------------------------------------------
// BranchedStream
// ---------------------------------------------------------------------------

/// Builder returned by [`KStream::split`].
///
/// Each [`branch`](Self::branch) call adds a filter-backed child node wired to
/// the parent node. It returns a new [`KStream`] that carries only the records
/// for which the predicate returns `true`.
///
/// **Simplification vs JVM first-match-wins:** this builder forwards records to
/// ALL branches whose predicate matches. For mutually-exclusive predicates the
/// behaviour is identical.
///
/// Drop `BranchedStream` before you call [`StreamsBuilder::build`]. It holds an
/// `Rc` clone of the shared internal builder, and it otherwise makes the
/// `Rc::try_unwrap` inside `build` fail.
///
/// [`StreamsBuilder::build`]: crate::dsl::builder::StreamsBuilder::build
pub struct BranchedStream<K, V, KS = <K as DefaultSerde>::Serde, VS = <V as DefaultSerde>::Serde> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) parent: NodeId,
    pub(crate) key_changing: bool,
    pub(crate) source_topic: Option<String>,
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) _pd: std::marker::PhantomData<fn() -> (K, V)>,
}

impl<K, V, KS, VS> BranchedStream<K, V, KS, VS>
where
    K: Any + Send + Clone + Default,
    V: Any + Send + Clone,
    KS: Clone,
    VS: Clone,
{
    /// Add a branch.
    ///
    /// This method forwards records for which `predicate(key, value)` returns
    /// `true` to the returned [`KStream`]. It uses a `KSTREAM-BRANCHCHILD-` node
    /// backed by the same filter processor that [`KStream::filter`] uses.
    pub fn branch<P>(&self, predicate: P) -> KStream<K, V, KS, VS>
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
        KStream::new_with_key_changing(
            Rc::clone(&self.builder),
            id,
            self.key_changing,
            self.key_serde.clone(),
            self.value_serde.clone(),
        )
        .with_source_topic(self.source_topic.clone())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use crate::dsl::builder::StreamsBuilder;

    #[test]
    fn stateless_chain_records_named_nodes() {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .map_values(|v: &String| v.to_uppercase())
            .filter(|_k: &String, _v: &String| true)
            .to("out");
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
        b.stream::<String, String>(["in"])
            .select_key(|_k: &String, v: &String| v.clone());
        let g = b.internal.borrow();
        check!(g.graph.nodes[1].key_changing_operation);
    }

    #[test]
    fn grace_join_builds_and_names_buffer_store() {
        // A grace join over a versioned table lowers cleanly and emits a buffer
        // changelog `app-<join_name>-Buffer-changelog`. The join node is the SAME
        // KSTREAM-JOIN node as a plain stream-table join (no extra processor). The
        // versioned table here supplies its own store name ("vt"), so only the
        // table SOURCE(1) + TABLE-SOURCE proc(2) and the stream SOURCE(0) are minted
        // before the join lands at KSTREAM-JOIN-0000000003 (see `dsl_golden_frame`
        // grace golden for the byte-exact node/store layout — this pins the name).
        use crate::{
            dsl::config::{Joined, Materialized},
            processor::serde::{Consumed, I64Serde, Produced, StringSerde},
        };
        let b = StreamsBuilder::new();
        let s = b
            .stream_explicit::<StringSerde, I64Serde>(["s"], Consumed::with(StringSerde, I64Serde));
        let t = b.table_explicit::<StringSerde, I64Serde>(
            "t",
            Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(600_000)),
        );
        s.join_table_with(
            &t,
            |a: &i64, c: &i64| a + c,
            Joined::with_grace_period(millis(60_000)),
        )
        .to_explicit("out", Produced::with(StringSerde, I64Serde));
        drop(s);
        drop(t);
        let wire = b.build("app").unwrap().to_wire();
        let changelogs: Vec<&str> = wire
            .subtopologies
            .iter()
            .flat_map(|st| st.state_changelog_topics.iter())
            .map(|t| t.name.as_str())
            .collect();
        check!(
            changelogs.contains(&"app-KSTREAM-JOIN-0000000003-Buffer-changelog"),
            "buffer changelog missing; got {changelogs:?}"
        );
    }

    #[test]
    #[should_panic(expected = "grace requires a versioned table")]
    fn grace_on_unversioned_table_panics() {
        use crate::{
            dsl::config::{Joined, Materialized},
            processor::serde::{Consumed, I64Serde, StringSerde},
        };
        let b = StreamsBuilder::new();
        let s = b
            .stream_explicit::<StringSerde, I64Serde>(["s"], Consumed::with(StringSerde, I64Serde));
        let t = b.table_explicit::<StringSerde, I64Serde>(
            "t",
            Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_store("plain"),
        );
        let _ = s.join_table_with(
            &t,
            |a: &i64, c: &i64| a + c,
            Joined::with_grace_period(millis(1000)),
        );
    }

    #[test]
    #[should_panic(expected = "grace must be < history_retention")]
    fn grace_geq_retention_panics() {
        use crate::{
            dsl::config::{Joined, Materialized},
            processor::serde::{Consumed, I64Serde, StringSerde},
        };
        let b = StreamsBuilder::new();
        let s = b
            .stream_explicit::<StringSerde, I64Serde>(["s"], Consumed::with(StringSerde, I64Serde));
        let t = b.table_explicit::<StringSerde, I64Serde>(
            "t",
            Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_versioned("vt", millis(1000)),
        );
        let _ = s.join_table_with(
            &t,
            |a: &i64, c: &i64| a + c,
            Joined::with_grace_period(millis(1000)),
        );
    }
}

#[cfg(test)]
mod to_table_caching_tests {
    use assert2::check;
    use crabka_units::prelude::*;

    use crate::{
        I64Serde, Materialized, Produced, StringSerde, dsl::StreamsBuilder,
        store::backend::StoreBackend,
    };

    /// Caching ON: the `to_table` store is marked cached (`cache_owner` rooted).
    /// Two same-key updates stay suppressed until flush, and the flush emits a
    /// single deduped record that carries the latest value.
    #[test]
    fn to_table_caches_marks_and_dedups_emit() {
        let b = StreamsBuilder::new();
        b.stream::<String, i64>(["in"])
            .to_table_explicit(Materialized::with(StringSerde, I64Serde).as_store("t"))
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let mut g =
            pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
                .unwrap();
        check!(g.cache_owner.contains_key("t"));
        pollster::block_on(g.init_processors()).unwrap();

        // Two same-key updates: 7 @0 then 9 @1.
        pollster::block_on(g.pipe("in", Some(b"k"), &7i64.to_be_bytes(), 0)).unwrap();
        pollster::block_on(g.pipe("in", Some(b"k"), &9i64.to_be_bytes(), 1)).unwrap();
        // Suppressed: nothing forwarded downstream until the cache flushes.
        check!(g.take_output().is_empty());

        pollster::block_on(g.flush_caches()).unwrap();
        let out = g.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "out");
        // to_stream forwards the deduped `new` value = 9 (BE i64).
        check!(out[0].value.as_ref().unwrap().as_ref() == 9i64.to_be_bytes());
    }

    /// `with_caching(false)`: the store is NOT cached even with a positive
    /// budget (mark opted out → absent from `cache_owner`).
    #[test]
    fn to_table_uncached_when_caching_off() {
        let b = StreamsBuilder::new();
        b.stream::<String, i64>(["in"])
            .to_table_explicit(
                Materialized::with(StringSerde, I64Serde)
                    .as_store("t")
                    .with_caching(false),
            )
            .to_stream()
            .to_explicit("out", Produced::with(StringSerde, I64Serde));
        let built = b.build("app").unwrap();
        let g = pollster::block_on(built.instantiate(&StoreBackend::InMemory, "app", kibibytes(1)))
            .unwrap();
        check!(!g.cache_owner.contains_key("t"));
    }
}
