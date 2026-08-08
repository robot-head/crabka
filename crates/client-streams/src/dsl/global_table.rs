//! `GlobalKTable<K,V>`: a fully-replicated lookup table.
//!
//! It is a join target only. It supports no aggregations and no `to_stream`.
//! [`StreamsBuilder::global_table`] builds it, and
//! [`KStream::join_global`] and [`KStream::left_join_global`] consume it.
//!
//! A `GlobalKTable` is invisible in the wire topology. It has no source topic, no
//! changelog, and no subtopology of its own. Its source node only occupies a
//! node-group index during grouping, so the other subtopology ids shift. The DSL
//! registers the store with [`Topology::add_global_store`].
//!
//! [`StreamsBuilder::global_table`]: crate::dsl::builder::StreamsBuilder::global_table
//! [`KStream::join_global`]: crate::dsl::kstream::KStream::join_global
//! [`KStream::left_join_global`]: crate::dsl::kstream::KStream::left_join_global
//! [`Topology::add_global_store`]: crate::topology::Topology::add_global_store
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

use crate::{
    dsl::{builder::InternalStreamsBuilder, graph::NodeId},
    processor::serde::DefaultSerde,
};

/// A handle to a fully-replicated `GlobalKTable`. It works only as a join
/// target.
///
/// `store_name` is the materialized store that `KStream::join_global` and
/// `left_join_global` wire the join processor's lookup against. `source_topic`
/// is the broker topic that fully replicates the global store.
pub struct GlobalKTable<K, V, KS = <K as DefaultSerde>::Serde, VS = <V as DefaultSerde>::Serde> {
    // Keeps the shared builder alive while this handle is live.
    #[allow(dead_code)]
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    // Logical graph node for the global source.
    #[allow(dead_code)]
    pub(crate) node: NodeId,
    pub(crate) store_name: String,
    // Source topic feeding the fully replicated store.
    #[allow(dead_code)]
    pub(crate) source_topic: String,
    pub(crate) key_serde: KS,
    pub(crate) value_serde: VS,
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V, KS, VS> GlobalKTable<K, V, KS, VS> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        store_name: String,
        source_topic: String,
        key_serde: KS,
        value_serde: VS,
    ) -> Self {
        Self {
            builder,
            node,
            store_name,
            source_topic,
            key_serde,
            value_serde,
            _pd: PhantomData,
        }
    }

    /// The name of the materialized state store that backs this global table.
    ///
    /// [`KStream::join_global`](crate::dsl::kstream::KStream::join_global) and
    /// `left_join_global` read it to wire the join processor's global-store
    /// lookup.
    pub(crate) fn store_name(&self) -> &str {
        &self.store_name
    }

    pub fn with_key_serde<NewKS>(self, serde: NewKS) -> GlobalKTable<K, V, NewKS, VS> {
        GlobalKTable {
            builder: self.builder,
            node: self.node,
            store_name: self.store_name,
            source_topic: self.source_topic,
            key_serde: serde,
            value_serde: self.value_serde,
            _pd: PhantomData,
        }
    }

    pub fn with_value_serde<NewVS>(self, serde: NewVS) -> GlobalKTable<K, V, KS, NewVS> {
        GlobalKTable {
            builder: self.builder,
            node: self.node,
            store_name: self.store_name,
            source_topic: self.source_topic,
            key_serde: self.key_serde,
            value_serde: serde,
            _pd: PhantomData,
        }
    }
}
