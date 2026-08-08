//! The topology builder. It turns a Processor-API node graph into a byte-exact
//! wire `Topology`.

pub(crate) mod builder;
mod grouping;
mod node;
mod wire;

pub use builder::{BuiltTopology, NodeHandle, Topology, TopologyError};
pub use wire::{WireCopartitionGroup, WireKeyValue, WireSubtopology, WireTopicInfo, WireTopology};
