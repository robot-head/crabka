//! High-level KStream/KTable DSL (sub-project #4). Compiles to the Processor-API
//! `Topology` via a logical graph + optimizer + lowering.
pub(crate) mod builder;
pub mod config;
pub(crate) mod graph;
pub mod kstream;
pub(crate) mod names;
pub use builder::StreamsBuilder;
pub use config::{Grouped, Materialized, Repartitioned};
