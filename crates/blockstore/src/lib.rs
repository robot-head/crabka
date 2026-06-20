//! Signal-agnostic columnar block store for Crabka observability.
//!
//! A block is a tenant-scoped, time-bounded Parquet file on object storage with
//! mandatory signal-declared columns plus arbitrary payload columns. An index
//! prunes a query to candidate blocks before any scan; `DataFusion` handles
//! intra-block row-group pruning and pushdown.

mod block;
mod block_index;
mod error;
mod index;
mod labels;
mod matcher;
mod profile_block;
mod profile_index;
mod profile_schema;

pub use block::{
    BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against, validate_block_schema,
};
pub use block_index::{BlockIndex, BlockSchema, RequiredColumn, series_block_schema};
pub use error::{BlockStoreError, Result};
pub use index::SeriesIndex;
pub use labels::{Labels, SeriesFingerprint};
pub use matcher::{LabelMatcher, MatchOp};
pub use profile_block::{ProfileSampleRow, encode_profile_samples};
pub use profile_index::{LABEL_PROFILE_TYPE, ProfileIndex};
pub use profile_schema::{
    PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION,
    PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl, profile_samples_schema,
};
