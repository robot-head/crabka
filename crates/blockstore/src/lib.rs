//! Signal-agnostic columnar block store for Crabka observability.
//!
//! A block is a tenant-scoped, time-bounded Parquet file on object storage with
//! mandatory identity/time columns plus arbitrary signal payload columns. An
//! index prunes a query to candidate blocks before any scan; `DataFusion` handles
//! intra-block row-group pruning and pushdown.

#![forbid(unsafe_code)]

mod block;
mod block_index;
mod bloom;
mod error;
mod index;
mod labels;
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
pub use index::SeriesIndex;
pub use labels::{Labels, SeriesFingerprint};
pub use matcher::{LabelMatcher, MatchOp};
pub use nested_set::{NestedSet, SpanNode, assign_nested_set};
pub use profile_block::{ProfileSampleRow, encode_profile_samples};
pub use profile_index::{LABEL_PROFILE_TYPE, ProfileIndex};
pub use profile_schema::{
    PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION,
    PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl, profile_samples_schema,
};
pub use reader::{RowGroupMeta, read_block, read_block_row_groups, read_row_group_metadata};
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
pub use store::BlockStore;
pub use trace_index::{TraceBlockStats, TraceIndex};
pub use writer::{BlockWriter, SummaryColumns};
