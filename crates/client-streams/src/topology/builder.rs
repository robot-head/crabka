//! Topology builder: stub placeholder for later implementation.

/// Error building a topology (bad node graph, invalid configuration, etc.).
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    // Variants added in Task 5.
}

/// An unbuilt topology graph (processor nodes, sources, sinks).
pub struct Topology;

/// A validated, wire-ready topology produced by [`Topology`].
pub struct BuiltTopology;
