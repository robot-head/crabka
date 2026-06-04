//! Topology builder: Processor-API node graph → byte-exact wire `Topology`.

mod builder;
mod grouping;
mod node;
mod wire;

pub use builder::{BuiltTopology, NodeHandle, Topology, TopologyError};
pub use wire::{WireCopartitionGroup, WireKeyValue, WireSubtopology, WireTopicInfo, WireTopology};
