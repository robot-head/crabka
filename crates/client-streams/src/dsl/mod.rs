//! High-level KStream/KTable DSL (sub-project #4). Compiles to the Processor-API
//! `Topology` via a logical graph + optimizer + lowering.
pub(crate) mod builder;
pub mod config;
pub(crate) mod graph;
pub mod kgrouped;
pub mod kstream;
pub mod ktable;
pub(crate) mod lower;
pub(crate) mod names;
pub(crate) mod optimizer;
pub(crate) mod processors;
pub mod windowed_kgrouped;
pub use builder::StreamsBuilder;
pub use config::{Grouped, Materialized, Repartitioned};
pub use kgrouped::KGroupedStream;
pub use kstream::BranchedStream;
pub use ktable::KTable;
pub mod windows;
pub use windows::{JoinWindows, TimeWindowedSerde, TimeWindows, Window, Windowed};
