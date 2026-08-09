//! The high-level `KStream` and `KTable` DSL.
//!
//! The DSL records a typed logical graph. It can then apply the JVM-compatible
//! topology optimizations, and it lowers the graph to the Processor-API
//! [`Topology`].
//!
//! [`Topology`]: crate::topology::Topology
pub(crate) mod builder;
pub mod cogrouped;
pub mod config;
pub mod emit;
pub mod global_table;
pub(crate) mod graph;
pub mod kgrouped;
pub(crate) mod kgrouped_table;
pub mod kstream;
pub mod ktable;
pub(crate) mod lower;
pub(crate) mod names;
pub(crate) mod optimizer;
pub(crate) mod processors;
pub mod session_windowed_cogrouped;
pub mod session_windowed_kgrouped;
pub mod sliding_windowed_cogrouped;
pub mod sliding_windowed_kgrouped;
pub mod suppress;
pub mod time_windowed_cogrouped;
pub mod windowed_kgrouped;
pub use builder::StreamsBuilder;
pub use cogrouped::CogroupedKStream;
pub use config::{Grouped, Joined, Materialized, Repartitioned, StreamJoined, VersionedConfig};
pub use emit::EmitStrategy;
pub use global_table::GlobalKTable;
pub use kgrouped::KGroupedStream;
pub use kgrouped_table::KGroupedTable;
pub use kstream::{BranchedStream, KStream};
pub use ktable::KTable;
pub use session_windowed_cogrouped::SessionWindowedCogroupedStream;
pub use session_windowed_kgrouped::SessionWindowedKGroupedStream;
pub use sliding_windowed_cogrouped::SlidingWindowedCogroupedStream;
pub use sliding_windowed_kgrouped::SlidingWindowedKGroupedStream;
pub use suppress::{BufferConfig, Suppressed};
pub use time_windowed_cogrouped::TimeWindowedCogroupedStream;
pub use windowed_kgrouped::TimeWindowedKGroupedStream;
pub mod windows;
pub use windows::{
    JoinWindows, SessionWindowedSerde, SessionWindows, SlidingWindows, TimeWindowedSerde,
    TimeWindows, Window, Windowed,
};
