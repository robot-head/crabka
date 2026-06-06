//! High-level KStream/KTable DSL (sub-project #4). Compiles to the Processor-API
//! `Topology` via a logical graph + optimizer + lowering.
pub(crate) mod builder;
pub mod config;
pub mod global_table;
pub(crate) mod graph;
pub mod kgrouped;
pub mod kstream;
pub mod ktable;
pub(crate) mod lower;
pub(crate) mod names;
pub(crate) mod optimizer;
pub(crate) mod processors;
pub mod session_windowed_kgrouped;
pub mod suppress;
pub mod windowed_kgrouped;
pub use builder::StreamsBuilder;
pub use config::{Grouped, Materialized, Repartitioned, StreamJoined};
pub use global_table::GlobalKTable;
pub use kgrouped::KGroupedStream;
pub use kstream::BranchedStream;
pub use ktable::KTable;
pub use suppress::{BufferConfig, Suppressed};
pub mod windows;
pub use windows::{
    JoinWindows, SessionWindowedSerde, SessionWindows, TimeWindowedSerde, TimeWindows, Window,
    Windowed,
};
