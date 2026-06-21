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
mod log_blockstore;
mod matcher;
mod profile_block;
mod profile_index;
mod profile_schema;

pub use block::{
    BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against, validate_block_schema,
};
pub use block_index::{BlockSchema, RequiredColumn, SignalBlockIndex, series_block_schema};
pub use error::{BlockStoreError as SeriesBlockStoreError, Result as SeriesResult};
pub use index::SeriesIndex;
pub use labels::Labels as SeriesLabels;
pub use log_blockstore::{
    BlockDescriptor, BlockIndex, BlockKey, BlockStoreError, LabelIndex, LabelPredicate, Labels,
    LogBlockTableProvider, LogRow, MatchOp, SeriesFingerprint, StructuredMetadata, TimeRange,
    block_path, labels, log_block_object_path, log_index_manifest_object_path,
    log_index_manifest_path, log_tenant_index_manifest_object_path,
    log_tenant_index_shard_catalog_object_path, log_tenant_index_shard_manifest_object_path,
    read_log_block, read_log_block_from_object_store, read_log_index_manifest,
    read_log_index_manifest_from_object_store, read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shards_from_object_store, register_log_blocks,
    register_log_blocks_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store, write_log_index_manifest,
    write_log_index_manifest_to_object_store, write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_to_object_store, write_tenant_log_index_shards_to_object_store,
};
pub use matcher::{LabelMatcher, MatchOp as SeriesMatchOp};
pub use profile_block::{ProfileSampleRow, encode_profile_samples};
pub use profile_index::{LABEL_PROFILE_TYPE, ProfileIndex};
pub use profile_schema::{
    PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION,
    PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl, profile_samples_schema,
};

pub type Result<T> = std::result::Result<T, BlockStoreError>;
