//! `NodeFactory`, the construction closures for one topology node.
//!
//! `Topology` holds the factory during construction, and `build()` moves it into
//! `BuiltTopology` so that `BuiltTopology::instantiate()` can use it.
//!
//! The factory does not track node *types*. The typed
//! [`NodeHandle`](crate::topology::NodeHandle) wiring enforces parent→child type
//! matching at compile time, so the factory only needs to know how to build the
//! node.

use super::{
    erased::{ErasedRecord, ProcessorError},
    node::ErasedNode,
};

/// A closure that builds a fresh [`ErasedNode`], which is a processor or a
/// sink.
pub(crate) type MakeNode = Box<dyn Fn() -> Box<dyn ErasedNode> + Send + Sync>;

/// A closure that builds a fresh deserialization function for a source.
pub(crate) type MakeDeser = Box<
    dyn Fn()
            -> Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<ErasedRecord, ProcessorError> + Send>
        + Send
        + Sync,
>;

/// The construction closures that instantiate a node. A source carries a
/// `make_deser`. A processor or a sink carries a `make_node`.
pub(crate) struct NodeFactory {
    /// Builds a fresh [`ErasedNode`]. It is `None` for a source.
    pub make_node: Option<MakeNode>,

    /// Builds a fresh deserialization closure. It is `None` for any node that is
    /// not a source.
    pub make_deser: Option<MakeDeser>,
}
