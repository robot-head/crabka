//! Language-less continuous-profiling engine for Crabka.
//!
//! This slice lands the pprof codec, shared symbol model, profile type parser,
//! query seam, and flamegraph merge engine.

mod diff;
mod engine;
mod error;
mod frame;
mod heatmap;
mod in_memory;
mod matcher;
mod pprof;
mod profile_type;
mod raw_profile;
mod samples;
mod series;
mod store;
mod symbol_db;
mod symbolizer;
mod tree;
mod union_store;

/// Prost-generated perftools.profiles wire types.
pub mod proto {

    include!(concat!(env!("OUT_DIR"), "/perftools.profiles.rs"));
}

pub use diff::diff_trees;
pub use engine::{EngineOpts, FlameEngine};
pub use error::ProfileError;
pub use frame::{Frame, SymbolSource};
pub use heatmap::{Heatmap, LabeledHeatmap, bin_heatmap};
pub use in_memory::InMemoryProfileStore;
pub use matcher::parse_label_selector;
pub use pprof::PprofProfile;
pub use profile_type::ProfileType;
pub use raw_profile::{tree_to_pprof, tree_to_pprof_with_max_nodes};
pub use samples::{
    COL_FINGERPRINT, COL_TIMESTAMP, PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_schema,
};
pub use series::{Series, SeriesAgg, fold_bucket, step_bucket_ms, step_ms_from_secs};
pub use store::{ProfileScan, ProfileStats, ProfileStore};
pub use symbol_db::{
    FunctionRec, LineRec, LocationRec, MappingRec, MappingSymbolization, RawLocation, SymbolDb,
};
pub use symbolizer::{
    ChainedResolver, DebuginfodResolver, FileSystemResolver, LazySymbolizer, NativeResolver,
    NativeSymbol, ObjectSymbolResolver, SymbolizeRequest,
};
pub use tree::{FlameGraph, FlameGraphDiff, Level, Tree};
pub use union_store::UnionProfileStore;
