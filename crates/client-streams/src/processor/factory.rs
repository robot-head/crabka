//! `NodeFactory`: carries the type metadata and construction closures for one
//! topology node. Stored on `Topology` during construction; moved into
//! `BuiltTopology` at `build()` to enable `BuiltTopology::instantiate()`.

use std::any::TypeId;

use super::erased::{ErasedRecord, ProcessorError};
use super::node::ErasedNode;

// ──────────────────────────────────────────────────────────────────────────────
// Type aliases for the two closure flavours we store.
// ──────────────────────────────────────────────────────────────────────────────

/// Closure that constructs a fresh [`ErasedNode`] (processor or sink).
pub(crate) type MakeNode = Box<dyn Fn() -> Box<dyn ErasedNode> + Send + Sync>;

/// Closure that constructs a fresh deserialization function (source).
pub(crate) type MakeDeser = Box<
    dyn Fn()
            -> Box<dyn Fn(Option<&[u8]>, &[u8], i64) -> Result<ErasedRecord, ProcessorError> + Send>
        + Send
        + Sync,
>;

// ──────────────────────────────────────────────────────────────────────────────
// FactoryKind
// ──────────────────────────────────────────────────────────────────────────────

pub(crate) enum FactoryKind {
    Source,
    Processor,
    Sink,
}

impl FactoryKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FactoryKind::Source => "source",
            FactoryKind::Processor => "processor",
            FactoryKind::Sink => "sink",
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// NodeFactory
// ──────────────────────────────────────────────────────────────────────────────

/// All the information needed to validate wiring and instantiate a node.
pub(crate) struct NodeFactory {
    pub kind: FactoryKind,

    /// `(TypeId<K>, TypeId<V>)` this node **consumes**; `None` for sources.
    pub input_kv: Option<(TypeId, TypeId)>,

    /// `(TypeId<K>, TypeId<V>)` this node **produces**; `None` for sinks.
    pub output_kv: Option<(TypeId, TypeId)>,

    /// Human-readable names for the input pair; `None` for sources.
    pub input_names: Option<(&'static str, &'static str)>,

    /// Human-readable names for the output pair; `None` for sinks.
    pub output_names: Option<(&'static str, &'static str)>,

    /// Constructs a fresh [`ErasedNode`]; `None` for sources.
    #[allow(dead_code)] // used by BuiltTopology::instantiate (Task 7+)
    pub make_node: Option<MakeNode>,

    /// Constructs a fresh deserialization closure; `None` for non-sources.
    #[allow(dead_code)] // used by BuiltTopology::instantiate (Task 7+)
    pub make_deser: Option<MakeDeser>,
}
