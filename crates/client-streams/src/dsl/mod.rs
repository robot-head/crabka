//! High-level KStream/KTable DSL (sub-project #4). Compiles to the Processor-API
//! `Topology` via a logical graph + optimizer + lowering.
pub mod config;
pub(crate) mod names;
pub use config::{Grouped, Materialized, Repartitioned};
