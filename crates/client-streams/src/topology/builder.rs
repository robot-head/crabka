//! Topology builder: stub placeholder for later implementation.

/// Error building a topology (bad node graph, invalid configuration, etc.).
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("node {node} references unknown predecessor {predecessor}")]
    UnknownPredecessor { node: String, predecessor: String },
    #[error("topology has no source nodes")]
    Empty,
}

/// An unbuilt topology graph (processor nodes, sources, sinks).
pub struct Topology;

/// A validated, wire-ready topology produced by [`Topology`].
pub struct BuiltTopology;
