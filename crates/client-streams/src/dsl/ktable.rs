//! `KTable<K,V>`: a materialized, changelog-backed table view.
//!
//! A terminal aggregation (`count`/`reduce`/`aggregate`) or
//! [`StreamsBuilder::table`](crate::dsl::StreamsBuilder::table) produces a
//! `KTable`. [`KTable::to_stream`] converts it back to a `KStream`.
//!
//! Each op records a logical node and a lowering thunk in the same style as
//! [`crate::dsl::kstream::KStream`]. The thunk rebuilds the parent handle from
//! `LowerState`, makes the typed Processor-API call, and records the resulting
//! node name. The materialized ops `map_values` and `filter` also register a
//! state store.
use std::{any::Any, cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

use crabka_units::prelude::*;

use crate::{
    dsl::{
        builder::InternalStreamsBuilder,
        graph::{GraphNodeKind, LowerState, NodeId},
        kstream::KStream,
        names,
        processors::{
            change::Change,
            fk::{
                processors::{
                    FkJoinOutputProcessor, ForeignTableJoinProcessor, SubscriptionJoinProcessor,
                    SubscriptionReceiveProcessor, SubscriptionResolverProcessor,
                    SubscriptionSendProcessor,
                },
                subscription::{SubscriptionResponseWrapper, SubscriptionWrapper},
                wrapper_serde::{SubscriptionResponseWrapperSerde, SubscriptionWrapperSerde},
            },
            ktable_join::{
                JoinKind, KTableKTableJoinOtherProcessor, KTableKTableJoinThisProcessor,
            },
            stateless::MergeProcessor,
            table::{
                KTableFilterProcessor, KTableMapValuesProcessor, KTableMapValuesViewProcessor,
                KTableToStreamProcessor,
            },
            tuple_forwarder::TupleForwarder,
        },
    },
    processor::serde::{DefaultSerde, Serde, SerdeArc},
    topology::NodeHandle,
};

/// A serde-carrying closure that registers a `SuppressBytesStore` for a
/// `suppress` node during lowering.
///
/// The producing op attaches this closure to a `KTable`. That op is a windowed
/// aggregation, a session aggregation, or `builder.table`, and it alone knows the
/// concrete serdes.
///
/// The caller calls it as `factory(state, store_name, processor_name, logging)`.
/// It registers the suppress store with the captured serdes under `store_name`,
/// connected to `processor_name`, and `logging` gates the changelog. The closure
/// is type-erased, because the concrete `K`, `V`, and serdes are baked into it,
/// so the `KTable` field is non-generic. It is an `Arc` and it is `Send + Sync`,
/// because the lowering thunk that clones it in is itself `Send`. The captured
/// serdes are `Send + Sync` through the `Serde` supertrait.
pub(crate) type SuppressStoreFactory = Arc<dyn Fn(&mut LowerState, &str, &str, bool) + Send + Sync>;

struct ForeignKeyJoinGraph {
    registration_base: String,
    send_name: String,
    registration_sink: String,
    registration_source: String,
    subscription_store: String,
    receive_name: String,
    subscription_join_name: String,
    foreign_join_name: String,
    response_base: String,
    response_sink: String,
    response_source: String,
    resolver_name: String,
    output_name: String,
    output_id: NodeId,
}

fn allocate_foreign_key_join_graph(
    builder: &mut InternalStreamsBuilder,
    parents: (NodeId, NodeId),
) -> ForeignKeyJoinGraph {
    let registration_base = builder.new_processor_name(names::FK_SUBSCRIPTION_REGISTRATION);
    let send_name = builder.new_processor_name(names::FK_SUBSCRIPTION_REGISTRATION);
    let registration_sink = builder.new_processor_name(names::KTABLE_SINK);
    let registration_source = builder.new_processor_name(names::KTABLE_SOURCE);
    let subscription_store = builder.new_processor_name(names::FK_SUBSCRIPTION_STATE_STORE);
    let receive_name = builder.new_processor_name(names::FK_SUBSCRIPTION_PROCESSOR);
    let subscription_join_name = builder.new_processor_name(names::FK_SUBSCRIPTION_PROCESSOR);
    let foreign_join_name = builder.new_processor_name(names::FK_SUBSCRIPTION_PROCESSOR);
    let response_base = builder.new_processor_name(names::FK_SUBSCRIPTION_RESPONSE);
    let response_sink = builder.new_processor_name(names::KTABLE_SINK);
    let response_source = builder.new_processor_name(names::KTABLE_SOURCE);
    let resolver_name = builder.new_processor_name(names::FK_RESPONSE_RESOLVER);
    let output_name = builder.new_processor_name(names::FK_OUTPUT);
    let result_store = builder.new_processor_name(names::FK_OUTPUT_STATE_STORE);
    let output_id = builder.graph.add(
        output_name.clone(),
        GraphNodeKind::TableProcessor {
            store_name: Some(result_store.clone()),
        },
        vec![parents.0, parents.1],
    );
    ForeignKeyJoinGraph {
        registration_base,
        send_name,
        registration_sink,
        registration_source,
        subscription_store,
        receive_name,
        subscription_join_name,
        foreign_join_name,
        response_base,
        response_sink,
        response_source,
        resolver_name,
        output_name,
        output_id,
    }
}

/// Build a non-windowed [`SuppressStoreFactory`] from a table's key and value
/// serdes.
///
/// Plain aggregations and `builder.table` use this factory. It registers a
/// `SuppressBytesStore<K, V>` with the JVM 1-day default changelog retention.
/// Windowed and session aggregations use their own factories, which wrap
/// `TimeWindowedSerde` and `SessionWindowedSerde`.
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

/// A changelog-backed table handle.
///
/// `store_name` is the materialized store this table reads and writes. The DSL
/// uses it to derive changelog topics and to reuse the store in downstream
/// materialized ops. `source_topic` is the Kafka topic this table was sourced
/// from. It is set for `builder.table_explicit()` `KTables` and is `None` for
/// derived `KTables`. The join DSL reads it to declare copartition groups.
pub struct KTable<K, V, KS = <K as DefaultSerde>::Serde, VS = <V as DefaultSerde>::Serde> {
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    pub(crate) node: NodeId,
    #[allow(dead_code)]
    pub(crate) store_name: Option<String>,
    #[allow(dead_code)]
    pub(crate) source_topic: Option<String>,
    /// For windowed tables: the upstream window's grace (suppress closes a window
    /// at `window.end + window_grace`). `None` for non-windowed tables.
    pub(crate) window_grace: Option<Time>,
    /// The history retention when this table is materialized into a versioned
    /// store (KIP-889). It drives as-of stream-table join lookups (KIP-914), the
    /// table-table out-of-order gate, and grace validation. It mirrors
    /// `window_grace`. `None` for non-versioned and derived tables.
    pub(crate) versioned_retention: Option<Time>,
    /// The serde-carrying producers set this field. Those producers are the
    /// aggregations and `builder.table`. `suppress` reads it to register its store
    /// with the right serdes. It is `None` on a derived table whose value type
    /// changed through `map_values`, and `suppress` then panics.
    pub(crate) suppress_store_factory: Option<SuppressStoreFactory>,
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    pub(crate) _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V, KS, VS> KTable<K, V, KS, VS> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        store_name: Option<String>,
        source_topic: Option<String>,
        key_serde: KS,
        value_serde: VS,
    ) -> Self {
        Self {
            builder,
            node,
            store_name,
            source_topic,
            window_grace: None,
            versioned_retention: None,
            suppress_store_factory: None,
            key_serde,
            value_serde,
            _pd: PhantomData,
        }
    }

    pub fn with_key_serde<NewKS>(self, serde: NewKS) -> KTable<K, V, NewKS, VS> {
        KTable {
            builder: self.builder,
            node: self.node,
            store_name: self.store_name,
            source_topic: self.source_topic,
            window_grace: self.window_grace,
            versioned_retention: self.versioned_retention,
            suppress_store_factory: self.suppress_store_factory,
            key_serde: serde,
            value_serde: self.value_serde,
            _pd: PhantomData,
        }
    }

    pub fn with_value_serde<NewVS>(self, serde: NewVS) -> KTable<K, V, KS, NewVS> {
        KTable {
            builder: self.builder,
            node: self.node,
            store_name: self.store_name,
            source_topic: self.source_topic,
            window_grace: self.window_grace,
            versioned_retention: self.versioned_retention,
            suppress_store_factory: self.suppress_store_factory,
            key_serde: self.key_serde,
            value_serde: serde,
            _pd: PhantomData,
        }
    }

    /// The table's key serde, if captured (only `builder.table` tables have one).
    pub(crate) fn key_serde(&self) -> Arc<dyn Serde<K>>
    where
        KS: Serde<K> + Clone + 'static,
    {
        Arc::new(self.key_serde.clone())
    }

    /// The table's value serde, if captured.
    pub(crate) fn value_serde(&self) -> Arc<dyn Serde<V>>
    where
        VS: Serde<V> + Clone + 'static,
    {
        Arc::new(self.value_serde.clone())
    }

    /// This table's logical graph node id. The FK-join DSL feeds its
    /// `SubscriptionSend` from the left node and its `ForeignTableJoin` from the
    /// right node.
    pub(crate) fn node_id(&self) -> NodeId {
        self.node
    }

    /// The name of the materialized state store backing this table, if any.
    #[allow(dead_code)]
    pub(crate) fn store_name(&self) -> Option<&str> {
        self.store_name.as_deref()
    }

    /// The Kafka source topic this table was sourced from through
    /// `builder.table_explicit()`. `None` for a derived `KTable` such as an
    /// aggregation, a `map_values`, or a `filter`.
    #[allow(dead_code)]
    pub(crate) fn source_topic(&self) -> Option<&str> {
        self.source_topic.as_deref()
    }

    /// Tag this table with its upstream window's grace. The windowed and session
    /// aggregations set it, and `Change`-preserving ops propagate it. `suppress`
    /// reads it, and it accesses the `window_grace` field directly.
    #[must_use]
    pub(crate) fn with_window_grace(mut self, grace: Option<Time>) -> Self {
        self.window_grace = grace;
        self
    }

    /// Tag this table with its versioned-store history retention. `builder.table`
    /// sets it when the caller used `Materialized::as_versioned`. The stream-table
    /// join reads it for as-of routing, and the table-table join reads it for the
    /// out-of-order gate.
    #[must_use]
    pub(crate) fn with_versioned_retention(mut self, retention: Option<Time>) -> Self {
        self.versioned_retention = retention;
        self
    }

    /// Attach or propagate the serde-carrying suppress-store factory. The
    /// aggregations and `builder.table` set it. The value-preserving ops `filter`
    /// and `suppress` itself propagate it. `suppress` reads it.
    #[must_use]
    pub(crate) fn with_suppress_factory(mut self, factory: Option<SuppressStoreFactory>) -> Self {
        self.suppress_store_factory = factory;
        self
    }
}

impl<K, V, KS, VS> KTable<K, V, KS, VS>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    /// Test-only terminal: collect the change-stream into a shared buffer.
    ///
    /// This terminal collects each forwarded `Change<V>`'s key and **new** value
    /// into a shared buffer in arrival order. It includes tombstones, where
    /// `new == None`. Unlike [`to_stream`](Self::to_stream) it keeps tombstones,
    /// so an exec test can assert a table's full change-stream of value updates
    /// *and* `None` deletions. That matches the JVM
    /// `toStream().to_explicit(topic)` capture, which writes null-valued records.
    #[cfg(test)]
    pub(crate) fn collect_changes(
        &self,
        buf: crate::dsl::processors::fk::processors::ChangeBuffer<K, V>,
    ) where
        K: 'static,
        V: Sync + 'static,
    {
        let parent_id = self.node;
        let mut g = self.builder.borrow_mut();
        let name = g.new_processor_name(names::TABLE_TOSTREAM);
        let id = g.graph.add(
            name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        g.graph.nodes[id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let buf = buf.clone();
            let h = state.topology.add_processor::<K, Change<V>, K, V, _, _, _>(
                name.clone(),
                move || crate::dsl::processors::fk::processors::ChangeCollectorProcessor::<K, V> {
                    buf: buf.clone(),
                    _pd: PhantomData,
                },
                [parent],
            );
            state.handle_name.insert(id, h.name().to_string());
        }));
    }

    /// `toStream`: view the table's change-stream as a `KStream`, forwarding
    /// every record unchanged. Not key-changing.
    #[must_use]
    pub fn to_stream(&self) -> KStream<K, V, KS, VS>
    where
        KS: Clone,
        VS: Clone,
    {
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
        KStream::new(
            Rc::clone(&self.builder),
            id,
            self.key_serde.clone(),
            self.value_serde.clone(),
        )
    }

    /// `mapValues`: transform each value and forward the rewritten table view.
    ///
    /// This op **does not materialize** a store. It matches the JVM's
    /// non-materialized `mapValues`. The key stays unchanged, and the op emits no
    /// changelog topic. Use
    /// [`map_values_materialized`](Self::map_values_materialized) for the
    /// store-backed form.
    pub fn map_values<V2, F>(&self, f: F) -> KTable<K, V2, KS, <V2 as DefaultSerde>::Serde>
    where
        V2: DefaultSerde + Any + Send + Clone,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
        KS: Clone,
    {
        let grace = self.window_grace;
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
        KTable::new(
            Rc::clone(&self.builder),
            id,
            None,
            None,
            self.key_serde.clone(),
            <V2 as DefaultSerde>::Serde::default(),
        )
        .with_window_grace(grace)
    }

    /// `mapValues`: transform each value, materializing the rewritten table into
    /// a new store. Key unchanged.
    pub fn map_values_materialized<V2, NKS, NVS, F>(
        &self,
        f: F,
        materialized: impl Into<crate::dsl::config::Materialized<NKS, NVS>>,
    ) -> KTable<K, V2, NKS, NVS>
    where
        V2: Any + Send + Clone,
        NKS: Serde<K> + Clone + 'static,
        NVS: Serde<V2> + Clone + 'static,
        F: Fn(&V) -> V2 + Clone + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let grace = self.window_grace;
        let store_name = mint_table_store(&self.builder, &materialized, names::TABLE_MAPVALUES);
        let key_serde = materialized.key_serde.clone();
        let value_serde = materialized.value_serde.clone();
        let key_serde_for_ktable = key_serde.clone();
        let value_serde_for_ktable = value_serde.clone();
        let caching = materialized.caching_enabled();
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
                        forwarder: TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_state_store::<K, V2, NKS, NVS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                [h.name().to_string()],
            );
            state.topology.mark_store_caching(&store_for_thunk, caching);
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            id,
            Some(store_name),
            None,
            key_serde_for_ktable,
            value_serde_for_ktable,
        )
        .with_window_grace(grace)
    }

    /// `filter`: keep the rows that match `predicate` and materialize the view.
    ///
    /// When a row that matched before stops matching, this op removes it from the
    /// store and forwards it as a `Change<V>` tombstone, so downstream views drop
    /// it. See the processor module doc.
    #[must_use]
    pub fn filter<NKS, NVS, P>(
        &self,
        predicate: P,
        materialized: impl Into<crate::dsl::config::Materialized<NKS, NVS>>,
    ) -> KTable<K, V, NKS, NVS>
    where
        NKS: Serde<K> + Clone + 'static,
        NVS: Serde<V> + Clone + 'static,
        P: Fn(&K, &V) -> bool + Clone + Send + Sync + 'static,
    {
        let materialized = materialized.into();
        let grace = self.window_grace;
        // filter preserves V → suppress can still register a store with the same
        // serdes; propagate the factory.
        let suppress_factory = self.suppress_store_factory.clone();
        let store_name = mint_table_store(&self.builder, &materialized, names::TABLE_FILTER);
        let key_serde = materialized.key_serde.clone();
        let value_serde = materialized.value_serde.clone();
        let key_serde_for_ktable = key_serde.clone();
        let value_serde_for_ktable = value_serde.clone();
        let caching = materialized.caching_enabled();
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
                        forwarder: TupleForwarder::default(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.topology.add_state_store::<K, V, NKS, NVS>(
                store_for_thunk.clone(),
                key_serde.clone(),
                value_serde.clone(),
                [h.name().to_string()],
            );
            state.topology.mark_store_caching(&store_for_thunk, caching);
            state.handle_name.insert(id, h.name().to_string());
        }));
        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            id,
            Some(store_name),
            None,
            key_serde_for_ktable,
            value_serde_for_ktable,
        )
        .with_window_grace(grace)
        .with_suppress_factory(suppress_factory)
    }

    /// `groupBy`: re-group the table by a new `(KR, VR)` derived from each entry.
    ///
    /// After the re-group, aggregate with `count`, `reduce`, or `aggregate`. This
    /// op always repartitions: the JVM `KTable.groupBy` inserts a repartition-map,
    /// a sink, and a source.
    pub fn group_by<KR, VR, M>(
        &self,
        mapper: M,
    ) -> crate::dsl::kgrouped_table::KGroupedTable<KR, VR>
    where
        KR: DefaultSerde + Any + Send + Sync + Clone + PartialEq,
        VR: DefaultSerde + Any + Send + Sync + Clone,
        <KR as DefaultSerde>::Serde: Serde<KR> + Clone,
        <VR as DefaultSerde>::Serde: Serde<VR> + Clone,
        M: Fn(&K, &V) -> (KR, VR) + Clone + Send + Sync + 'static,
        K: Any + Send + Sync + Clone,
        V: Any + Send + Clone,
    {
        self.group_by_explicit(
            mapper,
            crate::dsl::config::Grouped::with(
                <KR as DefaultSerde>::Serde::default(),
                <VR as DefaultSerde>::Serde::default(),
            ),
        )
    }

    /// `groupBy` with explicit repartition serdes.
    pub fn group_by_explicit<KR, VR, GKS, GVS, M>(
        &self,
        mapper: M,
        grouped: impl Into<crate::dsl::config::Grouped<GKS, GVS>>,
    ) -> crate::dsl::kgrouped_table::KGroupedTable<KR, VR>
    where
        KR: Any + Send + Sync + Clone + PartialEq,
        VR: Any + Send + Sync + Clone,
        GKS: Serde<KR> + Clone + 'static,
        GVS: Serde<VR> + Clone + 'static,
        M: Fn(&K, &V) -> (KR, VR) + Clone + Send + Sync + 'static,
        K: Any + Send + Sync + Clone,
        V: Any + Send + Clone,
    {
        use crate::dsl::processors::table_aggregate::KTableRepartitionMapProcessor;

        let grouped = grouped.into();
        let parent_id = self.node;

        // Record the `KTABLE-SELECT` repartition-map node NOW (at `groupBy()` time),
        // matching the JVM `KGroupedTableImpl`, which mints SELECT before the
        // terminal aggregation mints its result store. Recording it here rather
        // than deferring to the terminal op is what keeps an auto-named result
        // store at the JVM counter index (pinned by the `kgrouped_table_autonamed`
        // golden).
        let mut g = self.builder.borrow_mut();
        let select_name = g.new_processor_name(names::KTABLE_SELECT);
        let select_id = g.graph.add(
            select_name.clone(),
            GraphNodeKind::TableProcessor { store_name: None },
            vec![parent_id],
        );
        let mapper2 = mapper.clone();
        g.graph.nodes[select_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&parent_id].clone());
            let h = state
                .topology
                .add_processor::<K, Change<V>, KR, Change<VR>, _, _, _>(
                    select_name.clone(),
                    move || KTableRepartitionMapProcessor {
                        mapper: mapper2.clone(),
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state.handle_name.insert(select_id, h.name().to_string());
        }));
        drop(g);

        crate::dsl::kgrouped_table::KGroupedTable::new(
            Rc::clone(&self.builder),
            select_id,
            crate::dsl::kgrouped_table::repartition_lower_changed::<KR, VR, GKS, GVS>(
                grouped.key_serde,
                grouped.value_serde,
            ),
        )
    }

    /// `join` (inner KTable-KTable join): join rows that both tables hold.
    ///
    /// For each key, the join row exists only when **both** tables hold a value.
    /// On any change to either side, the join re-reads the other side's current
    /// value from its store and forwards a `Change<VR>`. It forwards a tombstone
    /// when the row stops existing.
    ///
    /// Both tables must be materialized, because the join reads each side's
    /// store. The join declares the two source topics as a copartition group
    /// (KIP-1071).
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn join<VB, VR, F, VBS>(
        &self,
        other: &KTable<K, VB, KS, VBS>,
        joiner: F,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        V: Sync,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        F: Fn(&V, &VB) -> VR + Clone + Send + Sync + 'static,
        KS: Clone,
        VBS: Clone,
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

    /// `leftJoin` (left KTable-KTable join): emit a row for every left row.
    ///
    /// This join emits a row whenever the **left** (this) side is present. The
    /// right side is optional, and the joiner receives `None` for it on a miss.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn left_join<VB, VR, F, VBS>(
        &self,
        other: &KTable<K, VB, KS, VBS>,
        joiner: F,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        V: Sync,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        F: Fn(&V, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
        KS: Clone,
        VBS: Clone,
    {
        let jf = move |a: Option<&V>, b: Option<&VB>| joiner(a.expect("left join: a present"), b);
        self.join_impl(other, jf, JoinKind::left())
    }

    /// `outerJoin` (outer KTable-KTable join): emit a row for either side.
    ///
    /// This join emits a row whenever **either** side is present. The joiner
    /// receives an `Option` for each side.
    pub fn outer_join<VB, VR, F, VBS>(
        &self,
        other: &KTable<K, VB, KS, VBS>,
        joiner: F,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        V: Sync,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        F: Fn(Option<&V>, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
        KS: Clone,
        VBS: Clone,
    {
        self.join_impl(other, joiner, JoinKind::outer())
    }

    /// Shared lowering for the inner, left, and outer KTable-KTable joins.
    ///
    /// This method records three logical nodes and their thunks:
    /// - `KTABLE-JOINTHIS-`, fed by this table's node. It reads the OTHER (`b`)
    ///   store, applies the join, and forwards `Change<VR>`.
    /// - `KTABLE-JOINOTHER-`, fed by the other table's node. It reads the OTHER
    ///   (`a`) store, applies the join, and forwards `Change<VR>`.
    /// - `KTABLE-MERGE-`, fed by both join nodes. It forwards each `Change<VR>`
    ///   unchanged and unions the two join outputs.
    ///
    /// Each join node connects to the store it reads, so the lowering pulls it
    /// into the same subtopology as that store's owning table source. When both
    /// tables are single-source-topic tables, this method declares their source
    /// topics as a copartition group.
    fn join_impl<VB, VR, JF, VBS>(
        &self,
        other: &KTable<K, VB, KS, VBS>,
        jf: JF,
        kind: JoinKind,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        V: Sync,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Clone,
        JF: Fn(Option<&V>, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
        KS: Clone,
        VBS: Clone,
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

        // KIP-914: each side's OWN store name, set only when that side is
        // versioned. The matching processor reads its own latest `valid_from`
        // and suppresses out-of-order updates (record ts strictly older).
        let this_versioned_store = self.versioned_retention.is_some().then(|| a_store.clone());
        let other_versioned_store = other.versioned_retention.is_some().then(|| b_store.clone());

        // KIP-914: table-table joins read the OTHER side's LATEST value. Each
        // processor must know whether ITS other store is versioned so the read
        // goes through `get_versioned_store` (a plain `get_state_store` downcast
        // returns `None` for a `VersionedBytesStore`).
        // - This-processor reads the OTHER (b) table → versioned iff `other` is.
        // - Other-processor reads the OTHER (a/self) table → versioned iff `self` is.
        let other_is_versioned_this = other.versioned_retention.is_some();
        let other_is_versioned_other = self.versioned_retention.is_some();

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
        let a_store_this = a_store.clone();
        let jf_this = jf.clone();
        let join_this_name = join_this.clone();
        let this_versioned = this_versioned_store.clone();
        g.graph.nodes[this_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&self_node].clone());
            let store_for_proc = b_store_this.clone();
            let jf_for_proc = jf_this.clone();
            let self_versioned = this_versioned.clone();
            let h = state
                .topology
                .add_processor::<K, Change<V>, K, Change<VR>, _, _, _>(
                    join_this_name.clone(),
                    move || KTableKTableJoinThisProcessor {
                        other_store: store_for_proc.clone(),
                        joiner: jf_for_proc.clone(),
                        kind,
                        self_versioned_store: self_versioned.clone(),
                        // KIP-914: the This-processor reads the OTHER (b) store; it
                        // is versioned iff the `other` table is versioned.
                        other_is_versioned: other_is_versioned_this,
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state
                .topology
                .connect_processor_store(h.name(), &b_store_this);
            // KIP-914: connect this processor to its OWN store so the gate's
            // `get_versioned_store` lookup resolves (only matters when versioned).
            if this_versioned.is_some() {
                state
                    .topology
                    .connect_processor_store(h.name(), &a_store_this);
            }
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
        let b_store_other = b_store.clone();
        let jf_other = jf.clone();
        let join_other_name = join_other.clone();
        let other_versioned = other_versioned_store.clone();
        g.graph.nodes[other_id].lower = Some(Box::new(move |state: &mut LowerState| {
            let parent =
                NodeHandle::<K, Change<VB>>::from_name(state.handle_name[&other_node].clone());
            let store_for_proc = a_store_other.clone();
            let jf_for_proc = jf_other.clone();
            let self_versioned = other_versioned.clone();
            let h = state
                .topology
                .add_processor::<K, Change<VB>, K, Change<VR>, _, _, _>(
                    join_other_name.clone(),
                    move || KTableKTableJoinOtherProcessor {
                        other_store: store_for_proc.clone(),
                        joiner: jf_for_proc.clone(),
                        kind,
                        self_versioned_store: self_versioned.clone(),
                        // KIP-914: the Other-processor reads the OTHER (a/self) store;
                        // it is versioned iff `self` (the receiver KTable) is versioned.
                        other_is_versioned: other_is_versioned_other,
                        _pd: PhantomData,
                    },
                    [parent],
                );
            state
                .topology
                .connect_processor_store(h.name(), &a_store_other);
            // KIP-914: connect this processor to its OWN store so the gate's
            // `get_versioned_store` lookup resolves (only matters when versioned).
            if other_versioned.is_some() {
                state
                    .topology
                    .connect_processor_store(h.name(), &b_store_other);
            }
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
        KTable::new(
            Rc::clone(&self.builder),
            merge_id,
            None,
            None,
            self.key_serde.clone(),
            <VR as DefaultSerde>::Serde::default(),
        )
    }

    /// `join` on a foreign key (KIP-213 inner FK join).
    ///
    /// For each left record, the foreign key `fk_extractor(&VA)` selects a row in
    /// `other`, which is a `KTable<KO, VB>`. `joiner(&VA, &VB)` produces the
    /// result whenever both are present.
    ///
    /// Both tables must be materialized **source** tables built with
    /// `builder.table`, because the join reads `sa` and `sb` and needs their
    /// serdes. The join lowers to the two-subtopology KIP-213 graph: the
    /// subscription registration and response repartition topics, a subscription
    /// state store, and the five FK-join processors. See the module
    /// `dsl::processors::fk` for the per-processor semantics.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn join_on_foreign_key<KO, VB, VR, FKE, J, KOS, KosOther, VbsOther>(
        &self,
        other: &KTable<KO, VB, KosOther, VbsOther>,
        fk_extractor: FKE,
        joiner: J,
        fk_serde: KOS,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        KO: Any + Send + Sync + Clone,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Sync + Clone,
        K: Send + Sync,
        V: Send + Sync,
        FKE: Fn(&V) -> KO + Clone + Send + Sync + 'static,
        J: Fn(&V, &VB) -> VR + Clone + Send + Sync + 'static,
        KOS: Serde<KO> + Clone + 'static,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        KosOther: Clone,
        VbsOther: Serde<VB> + Clone + 'static,
    {
        // Inner: both sides required → the outer-form joiner only sees `Some`.
        let jf = move |a: &V, b: Option<&VB>| joiner(a, b.expect("inner FK join: foreign present"));
        self.fk_join_impl(other, fk_extractor, jf, fk_serde, false)
    }

    /// `leftJoin` on a foreign key (KIP-213 left FK join).
    ///
    /// This join emits a row for every left record. The joiner receives `None`
    /// for the foreign value when the foreign key has no matching row.
    pub fn left_join_on_foreign_key<KO, VB, VR, FKE, J, KOS, KosOther, VbsOther>(
        &self,
        other: &KTable<KO, VB, KosOther, VbsOther>,
        fk_extractor: FKE,
        joiner: J,
        fk_serde: KOS,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        KO: Any + Send + Sync + Clone,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Sync + Clone,
        K: Send + Sync,
        V: Send + Sync,
        FKE: Fn(&V) -> KO + Clone + Send + Sync + 'static,
        J: Fn(&V, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
        KOS: Serde<KO> + Clone + 'static,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        KosOther: Clone,
        VbsOther: Serde<VB> + Clone + 'static,
    {
        self.fk_join_impl(other, fk_extractor, joiner, fk_serde, true)
    }

    /// Shared lowering for the inner and left foreign-key joins.
    ///
    /// `jf` is the outer-form joiner `Fn(&V, Option<&VB>) -> VR`. `is_left`
    /// selects either the JVM `leftJoinInstructions` or the inner staleness
    /// rules.
    ///
    /// This method records the whole KIP-213 graph under a **single** logical
    /// OUTPUT node fed by both tables' nodes, so the lowering driver visits it
    /// after both sources and before `toStream`. It mints all 14 JVM counter
    /// indices **eagerly**, from registration topic 4 to result store 17, so that
    /// a downstream op lands at the JVM index (`toStream`=18, sink=19). The thunk
    /// then registers the Topology sources, processors, sinks, stores,
    /// repartition topics, and copartition group.
    // the 14-node KIP-213 graph is one cohesive lowering
    fn foreign_key_join_sources<KO, VB, KosOther, VbsOther>(
        &self,
        other: &KTable<KO, VB, KosOther, VbsOther>,
    ) -> (String, String, String, String) {
        let left_store = self
            .store_name()
            .expect("FK join: left table must be a materialized source table")
            .to_string();
        let right_store = other
            .store_name()
            .expect("FK join: right table must be a materialized source table")
            .to_string();
        let left_topic = self
            .source_topic()
            .expect("FK join: left table must be sourced from a single topic")
            .to_string();
        let right_topic = other
            .source_topic()
            .expect("FK join: right table must be sourced from a single topic")
            .to_string();
        (left_store, right_store, left_topic, right_topic)
    }

    fn fk_join_impl<KO, VB, VR, FKE, JF, KOS, KosOther, VbsOther>(
        &self,
        other: &KTable<KO, VB, KosOther, VbsOther>,
        fk_extractor: FKE,
        jf: JF,
        fk_serde: KOS,
        is_left: bool,
    ) -> KTable<K, VR, KS, <VR as DefaultSerde>::Serde>
    where
        KO: Any + Send + Sync + Clone,
        VB: Any + Send + Sync + Clone,
        VR: DefaultSerde + Any + Send + Sync + Clone,
        K: Send + Sync,
        V: Send + Sync,
        FKE: Fn(&V) -> KO + Clone + Send + Sync + 'static,
        JF: Fn(&V, Option<&VB>) -> VR + Clone + Send + Sync + 'static,
        KOS: Serde<KO> + Clone + 'static,
        KS: Serde<K> + Clone + 'static,
        VS: Serde<V> + Clone + 'static,
        KosOther: Clone,
        VbsOther: Serde<VB> + Clone + 'static,
    {
        let (sa, sb, a_src, b_src) = self.foreign_key_join_sources(other);
        // Capture the left key/value + right value serdes (boxed clones for the
        // per-processor closures).
        let k_serde = self.key_serde();
        let left_value_serde = self.value_serde();
        let right_value_serde = other.value_serde();
        let self_node = self.node_id();
        let other_node = other.node_id();

        let mut g = self.builder.borrow_mut();
        let graph = allocate_foreign_key_join_graph(&mut g, (self_node, other_node));
        let output_id = graph.output_id;

        let thunk = move |state: &mut LowerState| {
            let app = state.app_id.clone();
            let registration_topic = format!(
                "{app}-{}{}",
                graph.registration_base,
                names::FK_TOPIC_SUFFIX
            );
            let response_topic = format!("{app}-{}{}", graph.response_base, names::FK_TOPIC_SUFFIX);

            let a_parent =
                NodeHandle::<K, Change<V>>::from_name(state.handle_name[&self_node].clone());
            let b_parent =
                NodeHandle::<KO, Change<VB>>::from_name(state.handle_name[&other_node].clone());

            // ── Left chain: SubscriptionSend → reg sink → reg source ──────────
            let send_h = state
                .topology
                .add_processor::<K, Change<V>, KO, SubscriptionWrapper, _, _, _>(
                    graph.send_name.clone(),
                    {
                        let fke = fk_extractor.clone();
                        let left_serde = left_value_serde.clone();
                        let ko = fk_serde.clone();
                        let ks = k_serde.clone();
                        move || SubscriptionSendProcessor {
                            fk_extractor: fke.clone(),
                            va_serde: Box::new(SerdeArc(left_serde.clone())),
                            ko_serde: Box::new(ko.clone()),
                            k_serde: Box::new(SerdeArc(ks.clone())),
                            is_left,
                            _pd: PhantomData,
                        }
                    },
                    [a_parent],
                );
            state
                .topology
                .add_sink_explicit::<KO, SubscriptionWrapper, KOS, SubscriptionWrapperSerde, _, _>(
                    graph.registration_sink.clone(),
                    registration_topic.clone(),
                    [&send_h],
                    crate::processor::serde::Produced::with(
                        fk_serde.clone(),
                        SubscriptionWrapperSerde,
                    ),
                );
            state
                .topology
                .add_repartition_topic(registration_topic.clone());
            let reg_src_h = state
                .topology
                .add_source_explicit::<KO, SubscriptionWrapper, KOS, SubscriptionWrapperSerde>(
                    graph.registration_source.clone(),
                    [registration_topic.clone()],
                    crate::processor::serde::Consumed::with(
                        fk_serde.clone(),
                        SubscriptionWrapperSerde,
                    ),
                );

            // ── Right chain (sub1): receive → subscription-join ───────────────
            let receive_h = state
                .topology
                .add_processor::<KO, SubscriptionWrapper, KO, SubscriptionWrapper, _, _, _>(
                    graph.receive_name.clone(),
                    {
                        let store = graph.subscription_store.clone();
                        let ko = fk_serde.clone();
                        move || SubscriptionReceiveProcessor {
                            store_name: store.clone(),
                            ko_serde: Box::new(ko.clone()),
                            _pd: PhantomData,
                        }
                    },
                    [&reg_src_h],
                );
            let sub_join_h = state
                .topology
                .add_processor::<KO, SubscriptionWrapper, K, SubscriptionResponseWrapper, _, _, _>(
                    graph.subscription_join_name.clone(),
                    {
                        let b = sb.clone();
                        let ks = k_serde.clone();
                        let right_serde = right_value_serde.clone();
                        move || SubscriptionJoinProcessor::<KO, K, VB> {
                            b_store: b.clone(),
                            k_serde: Box::new(SerdeArc(ks.clone())),
                            vb_serde: Box::new(SerdeArc(right_serde.clone())),
                            _pd: PhantomData,
                        }
                    },
                    [&receive_h],
                );

            // ── Right chain (sub1): foreign-table-join (fed by sb source) ─────
            let foreign_join_h = state
                .topology
                .add_processor::<KO, Change<VB>, K, SubscriptionResponseWrapper, _, _, _>(
                    graph.foreign_join_name.clone(),
                    {
                        let store = graph.subscription_store.clone();
                        let ko = fk_serde.clone();
                        let ks = k_serde.clone();
                        let right_serde = right_value_serde.clone();
                        move || ForeignTableJoinProcessor::<KO, K, VB> {
                            store_name: store.clone(),
                            ko_serde: Box::new(ko.clone()),
                            k_serde: Box::new(SerdeArc(ks.clone())),
                            vb_serde: Box::new(SerdeArc(right_serde.clone())),
                            _pd: PhantomData,
                        }
                    },
                    [&b_parent],
                );

            // Subscription store: connected to receive (writer) + foreign-join
            // (prefix-scanner). This unites the registration-source chain with sb's
            // subtopology (subtopology 1).
            state.topology.add_fk_subscription_store(
                graph.subscription_store.clone(),
                [
                    receive_h.name().to_string(),
                    foreign_join_h.name().to_string(),
                ],
            );
            // sub-join reads sb → connect so it joins sb's subtopology.
            state
                .topology
                .connect_processor_store(sub_join_h.name(), &sb);

            // ── Response sink (sub1) ← {sub-join, foreign-join} ───────────────
            state
                .topology
                .add_sink_explicit::<K, SubscriptionResponseWrapper, _, SubscriptionResponseWrapperSerde, _, _>(
                    graph.response_sink.clone(),
                    response_topic.clone(),
                    [&sub_join_h, &foreign_join_h],
                    crate::processor::serde::Produced::with(
                        SerdeArc(k_serde.clone()),
                        SubscriptionResponseWrapperSerde,
                    ),
                );
            state.topology.add_repartition_topic(response_topic.clone());

            // ── Response source (sub0) → resolver → output ────────────────────
            let resp_src_h = state
                .topology
                .add_source_explicit::<K, SubscriptionResponseWrapper, _, SubscriptionResponseWrapperSerde>(
                    graph.response_source.clone(),
                    [response_topic.clone()],
                    crate::processor::serde::Consumed::with(
                        SerdeArc(k_serde.clone()),
                        SubscriptionResponseWrapperSerde,
                    ),
                );
            let resolver_h = state
                .topology
                .add_processor::<K, SubscriptionResponseWrapper, K, Change<VR>, _, _, _>(
                    graph.resolver_name.clone(),
                    {
                        let a = sa.clone();
                        let left_serde = left_value_serde.clone();
                        let right_serde = right_value_serde.clone();
                        let joiner = jf.clone();
                        move || SubscriptionResolverProcessor::<K, V, VB, VR, JF> {
                            a_store: a.clone(),
                            va_serde: Box::new(SerdeArc(left_serde.clone())),
                            vb_serde: Box::new(SerdeArc(right_serde.clone())),
                            joiner: joiner.clone(),
                            is_left,
                            _pd: PhantomData,
                        }
                    },
                    [&resp_src_h],
                );
            // Resolver reads sa → connect so it joins sa's subtopology (subtopology 0).
            state
                .topology
                .connect_processor_store(resolver_h.name(), &sa);

            let output_h = state
                .topology
                .add_processor::<K, Change<VR>, K, Change<VR>, _, _, _>(
                    graph.output_name.clone(),
                    || FkJoinOutputProcessor::<K, VR> { _pd: PhantomData },
                    [&resolver_h],
                );

            // Copartition: the left source topic + the registration repartition
            // source (subtopology 1) and the response repartition source +
            // left source (subtopology 0) are each copartitioned. The JVM declares
            // the external source with the repartition source it co-reads:
            //   sub0: [a, response-topic]  ;  sub1: [b, registration-topic]
            // We declare both; the grouping pass routes each to the subtopology
            // that reads all its members.
            state
                .topology
                .add_copartition_group([a_src.clone(), response_topic.clone()]);
            state
                .topology
                .add_copartition_group([b_src.clone(), registration_topic.clone()]);

            state
                .handle_name
                .insert(output_id, output_h.name().to_string());
        };
        g.graph.nodes[output_id].lower = Some(Box::new(thunk));
        drop(g);
        KTable::new(
            Rc::clone(&self.builder),
            output_id,
            None,
            None,
            self.key_serde.clone(),
            <VR as DefaultSerde>::Serde::default(),
        )
    }
}

impl<K, V, KS, VS> KTable<K, V, KS, VS>
where
    K: Any + Send + Sync + Clone,
    V: Any + Send + Clone,
{
    /// `suppress(Suppressed)`: buffer updates and emit on a delay.
    ///
    /// On a windowed table, `until_window_closes` emits each window's final value
    /// once the window closes. `until_time_limit` rate-limits any table to one
    /// update per key per wait.
    ///
    /// The buffer is a registered
    /// [`SuppressBytesStore`](crate::store::suppress_store). It is durable, with
    /// a changelog and restore, when `logging` is on. The serdes come from the
    /// table-producing operation. A call to `suppress` on a table that changed its
    /// value type through `map_values` panics, because no serde factory is
    /// available.
    #[must_use]
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub fn suppress(&self, suppressed: crate::dsl::suppress::Suppressed<K>) -> KTable<K, V, KS, VS>
    where
        KS: Clone,
        VS: Clone,
    {
        let wait = match suppressed.wait {
            crate::dsl::suppress::WaitKind::UpstreamGrace => {
                self.window_grace.unwrap_or(Time::ZERO)
            }
            crate::dsl::suppress::WaitKind::Fixed(wait) => wait,
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
                            wait,
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
        KTable::new(
            Rc::clone(&self.builder),
            id,
            Some(store_name),
            None,
            self.key_serde.clone(),
            self.value_serde.clone(),
        )
        .with_window_grace(self.window_grace)
        .with_suppress_factory(self.suppress_store_factory.clone())
    }
}

/// Mint a materialized table store name.
///
/// Returns the `Materialized` name when it is present, otherwise a fresh counter
/// at the JVM position.
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

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::prelude::*;

    #[test]
    fn versioned_table_handle_carries_retention() {
        use crate::{
            dsl::{builder::StreamsBuilder, config::Materialized},
            processor::serde::{I64Serde, StringSerde},
        };

        let b = StreamsBuilder::new();
        let t = b.table_explicit::<StringSerde, I64Serde>(
            "in",
            crate::processor::serde::Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_versioned("vt", minutes(10)),
        );
        check!(t.versioned_retention == Some(minutes(10)));

        let plain = b.table_explicit::<StringSerde, I64Serde>(
            "in2",
            crate::processor::serde::Consumed::with(StringSerde, I64Serde),
            Materialized::with(StringSerde, I64Serde).as_store("pt"),
        );
        check!(plain.versioned_retention == None);
    }
}

#[cfg(test)]
mod fk_exec_tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        dsl::builder::StreamsBuilder,
        processor::serde::{Consumed, StringSerde},
        test_driver::TopologyTestDriver,
    };

    type Out = Arc<Mutex<Vec<(Option<String>, Option<String>)>>>;
    /// One sequence step: `(input_topic, key, value, ts, expected_emissions)`,
    /// where each expected emission is `(key, Some(value)|None-tombstone)`.
    type Step<'a> = (
        &'a str,
        &'a str,
        &'a str,
        i64,
        &'a [(&'a str, Option<&'a str>)],
    );

    /// One input record: `topic:key=val@ts`.
    ///
    /// The runtime has no null-value source-record path, because a `Record`'s
    /// value is always present. The behavior.json `a:k1=null@6`
    /// tombstone-into-the-table step therefore cannot be piped here. That step is
    /// a redundant `k1=null` after step 5 already emits `k1=null`, so dropping it
    /// leaves every distinct FK retraction case covered.
    fn pipe(d: &mut TopologyTestDriver, topic: &str, key: &str, val: &str, ts: i64) {
        d.pipe_input(
            topic,
            Consumed::with(StringSerde, StringSerde),
            Some(key.to_string()),
            val.to_string(),
            ts,
        );
    }

    /// Drive the (k, v, ts) input steps and assert each step's *incremental*
    /// collected output equals `want`. The collector keeps tombstones as
    /// `(Some(k), None)`.
    fn run_sequence(buf: &Out, d: &mut TopologyTestDriver, steps: &[Step]) {
        let mut seen = 0usize;
        for (topic, key, val, ts, want) in steps {
            pipe(d, topic, key, val, *ts);
            let all = buf.lock().unwrap().clone();
            let step_out: Vec<(Option<String>, Option<String>)> = all[seen..].to_vec();
            seen = all.len();
            let want_owned: Vec<(Option<String>, Option<String>)> = want
                .iter()
                .map(|(k, v)| (Some((*k).to_string()), v.map(str::to_string)))
                .collect();
            assert_eq!(step_out, want_owned, "step {topic}:{key}={val}@{ts}");
        }
    }

    fn tables(
        b: &StreamsBuilder,
    ) -> (super::KTable<String, String>, super::KTable<String, String>) {
        let ta = b.table::<String, String>("a", "sa");
        let tb = b.table::<String, String>("b", "sb");
        (ta, tb)
    }

    /// Inner FK join over the behavior.json `inner_sequence` (steps 0–5).
    ///
    /// The fk extractor is the identity on the left String value, and the joiner
    /// is va+vb. The test checks the first-arrival skip (`a:k1=A` → []), the
    /// match emit (`b:A=X` → `k1=AX`), and the FK-change retraction tombstone
    /// (`a:k1=A2` → `k1=null`, because fk "A"→"A2" and "A2" has no foreign
    /// value). It then checks a second primary key, the right-table re-emit
    /// (`b:A=Y` → `k2=AY`), and another FK-change tombstone.
    #[test]
    fn fk_inner_sequence_matches_behavior_json() {
        let b = StreamsBuilder::new();
        let (ta, tb) = tables(&b);
        let buf: Out = Arc::new(Mutex::new(Vec::new()));
        ta.join_on_foreign_key(
            &tb,
            |va: &String| va.clone(),
            |va: &String, vb: &String| format!("{va}{vb}"),
            StringSerde,
        )
        .collect_changes(buf.clone());
        drop(ta);
        drop(tb);
        let built = b.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        run_sequence(
            &buf,
            &mut d,
            &[
                ("a", "k1", "A", 0, &[]),
                ("b", "A", "X", 1, &[("k1", Some("AX"))]),
                ("a", "k1", "A2", 2, &[("k1", None)]),
                ("a", "k2", "A", 3, &[("k2", Some("AX"))]),
                ("b", "A", "Y", 4, &[("k2", Some("AY"))]),
                ("a", "k1", "B", 5, &[("k1", None)]),
            ],
        );
    }

    /// Left FK join over the behavior.json `left_sequence` (steps 0–5).
    ///
    /// The joiner is va + (vb? vb : "_"). The test checks the left-join non-match
    /// emit (`a:k1=A` → `k1=A_`), the match (`b:A=X` → `k1=AX`), the FK-change
    /// re-evaluation (`a:k1=A2` → `k1=A2_`), and the right-table re-emit. In the
    /// re-evaluation, fk "A2" has no foreign value, so left emits the left value
    /// with the empty marker and not a tombstone.
    #[test]
    fn fk_left_sequence_matches_behavior_json() {
        let b = StreamsBuilder::new();
        let (ta, tb) = tables(&b);
        let buf: Out = Arc::new(Mutex::new(Vec::new()));
        ta.left_join_on_foreign_key(
            &tb,
            |va: &String| va.clone(),
            |va: &String, vb: Option<&String>| format!("{va}{}", vb.map_or("_", String::as_str)),
            StringSerde,
        )
        .collect_changes(buf.clone());
        drop(ta);
        drop(tb);
        let built = b.build("app").unwrap();
        let mut d = TopologyTestDriver::new(&built).unwrap();
        run_sequence(
            &buf,
            &mut d,
            &[
                ("a", "k1", "A", 0, &[("k1", Some("A_"))]),
                ("b", "A", "X", 1, &[("k1", Some("AX"))]),
                ("a", "k1", "A2", 2, &[("k1", Some("A2_"))]),
                ("a", "k2", "A", 3, &[("k2", Some("AX"))]),
                ("b", "A", "Y", 4, &[("k2", Some("AY"))]),
                ("a", "k1", "B", 5, &[("k1", Some("B_"))]),
            ],
        );
    }
}
