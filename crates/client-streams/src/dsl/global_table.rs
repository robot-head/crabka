//! `GlobalKTable<K,V>`: a fully-replicated lookup table — a join target only (no
//! aggregations, no `to_stream`). Built by [`StreamsBuilder::global_table`] and
//! consumed by [`KStream::join_global`] / [`KStream::left_join_global`].
//!
//! A `GlobalKTable` is invisible in the wire topology: it has no source topic, no
//! changelog, and no subtopology of its own. Its source node only occupies a
//! node-group index during grouping (so other subtopology ids shift). The store is
//! registered with [`Topology::add_global_store`].
//!
//! [`StreamsBuilder::global_table`]: crate::dsl::builder::StreamsBuilder::global_table
//! [`KStream::join_global`]: crate::dsl::kstream::KStream::join_global
//! [`KStream::left_join_global`]: crate::dsl::kstream::KStream::left_join_global
//! [`Topology::add_global_store`]: crate::topology::Topology::add_global_store
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::NodeId;

/// A handle to a fully-replicated `GlobalKTable`. Only usable as a join target.
///
/// `store_name` is the materialized store `KStream::join_global` /
/// `left_join_global` wire the join processor's lookup against. `source_topic`
/// is the broker topic used to fully replicate the global store.
pub struct GlobalKTable<K, V> {
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
    _pd: PhantomData<fn() -> (K, V)>,
}

impl<K, V> GlobalKTable<K, V> {
    pub(crate) fn new(
        builder: Rc<RefCell<InternalStreamsBuilder>>,
        node: NodeId,
        store_name: String,
        source_topic: String,
    ) -> Self {
        Self {
            builder,
            node,
            store_name,
            source_topic,
            _pd: PhantomData,
        }
    }

    /// The name of the materialized state store backing this global table. Read by
    /// [`KStream::join_global`](crate::dsl::kstream::KStream::join_global) /
    /// `left_join_global` to wire the join processor's global-store lookup.
    pub(crate) fn store_name(&self) -> &str {
        &self.store_name
    }
}
