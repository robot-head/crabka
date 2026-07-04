//! `NodeFactory`: the construction closures for one topology node. Stored on
//! `Topology` during construction; moved into `BuiltTopology` at `build()` to
//! enable `BuiltTopology::instantiate()`.
//!
//! Node *types* are no longer tracked here — parent→child type matching is
//! enforced at compile time by the typed [`NodeHandle`](crate::topology::NodeHandle)
//! wiring, so the factory only needs to know how to build the node.

use super::{
    erased::{ErasedRecord, ProcessorError},
    node::ErasedNode,
};

/// Closure that constructs a fresh [`ErasedNode`] (processor or sink).
pub(crate) type MakeNode = Box<dyn Fn() -> Box<dyn ErasedNode> + Send + Sync>;

/// Closure that constructs a fresh deserialization function (source).
pub(crate) type MakeDeser = Box<
    dyn Fn()
            -> Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<ErasedRecord, ProcessorError> + Send>
        + Send
        + Sync,
>;

/// The construction closures needed to instantiate a node: a source carries a
/// `make_deser`, a processor or sink a `make_node`.
pub(crate) struct NodeFactory {
    /// Constructs a fresh [`ErasedNode`]; `None` for sources.
    pub make_node: Option<MakeNode>,

    /// Constructs a fresh deserialization closure; `None` for non-sources.
    pub make_deser: Option<MakeDeser>,
}
