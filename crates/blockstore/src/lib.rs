//! Signal-agnostic columnar block store for Crabka observability.
//!
//! A *block* is a tenant-scoped, time-bounded Parquet file on object storage
//! with mandatory identity/time columns (`series_fingerprint`, `timestamp`)
//! plus arbitrary signal payload columns. An `Index` prunes a query to
//! candidate blocks before any scan. `DataFusion` handles the intra-block
//! row-group prune and the pushdown.

#![forbid(unsafe_code)]

mod block;
mod block_index;
mod bloom;
mod error;
mod index;
mod index_snapshot;
mod labels;
mod log_blockstore;
mod matcher;
mod nested_set;
mod profile_block;
mod profile_index;
mod profile_schema;
mod reader;
mod span_block;
mod span_schema;
mod store;
mod trace_index;
mod writer;

pub use block::{
    BlockMeta, COL_FINGERPRINT, COL_TIMESTAMP, validate_against, validate_block_schema,
};
pub use block_index::{BlockIndex, BlockSchema, RequiredColumn, series_block_schema};
pub use bloom::{ShardedTraceBloom, fnv1_32};
pub use error::{BlockStoreError, Result};
pub use index::{Index, MAX_INDEX_SNAPSHOT_BYTES};
pub use index_snapshot::{
    DEFAULT_INDEX_SNAPSHOT_MAX, DEFAULT_INDEX_SNAPSHOT_RETAIN, IndexSnapshotRetain,
    index_snapshot_prefix_for_key,
};
pub use labels::{Labels, SeriesFingerprint};
// Logs-path block store. Types that share a name with the canonical
// (traces/shared) abstractions above are re-exported under `Log*` names.
pub use log_blockstore::{
    BlockDescriptor, BlockIndex as LogBlockIndex, BlockKey, BlockStoreError as LogBlockStoreError,
    LabelIndex, LabelPredicate, Labels as LogLabels, LogBlockTableProvider, LogRow,
    MatchOp as LogMatchOp, SeriesFingerprint as LogSeriesFingerprint, StructuredMetadata,
    TimeRange, block_path, labels, list_tenant_log_index_shard_ranges_from_object_store,
    list_tenant_log_index_shard_ranges_overlapping_query_from_object_store, log_block_object_path,
    log_index_manifest_object_path, log_index_manifest_path, log_tenant_index_manifest_object_path,
    log_tenant_index_shard_catalog_object_path, log_tenant_index_shard_list_offset_object_path,
    log_tenant_index_shard_list_offset_start_ns, log_tenant_index_shard_manifest_object_path,
    log_tenant_index_shards_object_prefix, read_log_block, read_log_block_from_object_store,
    read_log_index_manifest, read_log_index_manifest_from_object_store,
    read_tenant_log_index_manifest_from_object_store,
    read_tenant_log_index_shard_from_object_store,
    read_tenant_log_index_shard_ranges_from_object_store,
    read_tenant_log_index_shards_from_object_store, register_log_blocks,
    register_log_blocks_from_object_store, series_fingerprint, write_log_block,
    write_log_block_to_object_store, write_log_index_manifest,
    write_log_index_manifest_to_object_store, write_tenant_log_index_manifest_to_object_store,
    write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shard_to_object_store, write_tenant_log_index_shards_to_object_store,
};
pub use matcher::{
    LabelMatcher, MatchOp, QUERY_SHARD_LABEL, QueryShardSelector, parse_query_shard_selector,
};
pub use nested_set::{NestedSet, SpanNode, assign_nested_set};
pub use profile_block::{ProfileSampleRow, encode_profile_samples};
pub use profile_index::{LABEL_PROFILE_TYPE, MAX_PROFILE_INDEX_SNAPSHOT_BYTES, ProfileIndex};
pub use profile_schema::{
    PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION,
    PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl, profile_samples_schema,
};
pub use reader::{
    DEFAULT_BLOCK_READ_MAX, RowGroupMeta, read_block, read_block_row_groups,
    read_block_row_groups_with_max_bytes, read_block_with_max_bytes, read_row_group_metadata,
    read_row_group_metadata_with_max_bytes,
};
pub use span_block::{
    AttrValue, SpanAttr, SpanEvent, SpanLink, SpanRow, encode_span_rows,
    encode_span_rows_with_promoted_attrs,
};
pub use span_schema::{
    PromotedSpanAttr, PromotedSpanAttrType, SCOL_ATTR_IS_ARRAY, SCOL_ATTR_KEYS, SCOL_ATTR_VALUE,
    SCOL_ATTR_VALUE_BOOL, SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, SCOL_CHILD_COUNT,
    SCOL_DURATION_NANOS, SCOL_EVENTS, SCOL_INSTRUMENTATION_NAME, SCOL_INSTRUMENTATION_VERSION,
    SCOL_KIND, SCOL_LINKS, SCOL_NAME, SCOL_NESTED_SET_LEFT, SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID,
    SCOL_PARENT_SPAN_ID, SCOL_PROMOTED_ATTR_PREFIX, SCOL_ROOT_SERVICE_NAME, SCOL_ROOT_SPAN_NAME,
    SCOL_SPAN_ID, SCOL_START_NANO, SCOL_STATUS_CODE, SCOL_STATUS_MESSAGE,
    SCOL_TRACE_DURATION_NANOS, SCOL_TRACE_ID, SCOL_TRACE_START_NANO, SpanKind, StatusCode,
    span_block_decl, span_block_schema, span_block_schema_with_promoted_attrs,
};
pub use store::{BlockStore, ScanTableRequest};
pub use trace_index::{TraceBlockStats, TraceIndex};
pub use writer::{BlockWriter, SummaryColumns};
