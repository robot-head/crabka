//! Topology builder: Processor-API node graph → byte-exact wire `Topology`.

mod builder;
mod grouping;
mod node;
mod wire;

pub use builder::{BuiltTopology, Topology, TopologyError};
