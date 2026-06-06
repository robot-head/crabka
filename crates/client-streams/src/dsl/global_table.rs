//! `GlobalKTable<K,V>`: a fully-replicated lookup table — a join target only (no
//! aggregations, no `to_stream`). Built by [`StreamsBuilder::global_table`];
//! consumed by `KStream::join_global`/`left_join_global` (a later task).
//!
//! A `GlobalKTable` is invisible in the wire topology: it has no source topic, no
//! changelog, and no subtopology of its own. Its source node only occupies a
//! node-group index during grouping (so other subtopology ids shift). See
//! [`crate::topology::grouping`] and [`crate::topology::Topology::add_global_store`].
//!
//! [`StreamsBuilder::global_table`]: crate::dsl::builder::StreamsBuilder::global_table
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

use crate::dsl::builder::InternalStreamsBuilder;
use crate::dsl::graph::NodeId;

/// A handle to a fully-replicated `GlobalKTable`. Only usable as a join target.
///
/// `builder`, `node`, and `source_topic` are read by `KStream::join_global` /
/// `left_join_global` in a later task to wire the join; `store_name` is the
/// materialized store the join processor reads. They're marked `dead_code` until
/// that task lands.
pub struct GlobalKTable<K, V> {
    // Used by join_global in a later task.
    #[allow(dead_code)]
    pub(crate) builder: Rc<RefCell<InternalStreamsBuilder>>,
    // Used by join_global in a later task.
    #[allow(dead_code)]
    pub(crate) node: NodeId,
    pub(crate) store_name: String,
    // Used by join_global in a later task.
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

    /// The name of the materialized state store backing this global table.
    // Used by join_global in a later task.
    #[allow(dead_code)]
    pub(crate) fn store_name(&self) -> &str {
        &self.store_name
    }
}
