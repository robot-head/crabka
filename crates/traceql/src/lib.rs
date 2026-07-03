//! `TraceQL` engine for Crabka's Grafana-Tempo-equivalent traces backend.
//!
//! Hand-written lexer and recursive-descent parser, an AST-to-`DataFusion`
//! planner, and nested-set structural self-join lowering for `TraceQL` structural
//! operators. Storage is injected through the `SpanStore` trait.

#![forbid(unsafe_code)]

mod ast;
mod engine;
mod error;
mod ids;
mod in_memory;
mod lexer;
mod parser;
mod planner;
mod result;
mod span_columns;
mod store;
pub mod testkit;

pub use ast::{
    Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, Scope, SpansetExpr,
    StructuralOp, Value,
};
pub use engine::{EngineOpts, SearchOptions, TraceqlEngine};
pub use error::TraceqlError;
pub use in_memory::InMemorySpanStore;
pub use lexer::{Token, lex};
pub use parser::parse;
pub use result::{
    AttrValue, EventRef, LinkRef, ScopedTag, SearchResponse, SpanRef, SpanSet, TagScope,
    TraceMetricSeries, TraceMetricsResponse, TraceResult, TraceSpans, TypedValue,
};
pub use span_columns::{
    ATTR_PREFIX, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME, COL_EVENT_TIME_SINCE_START,
    COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_LINK_SPAN_ID,
    COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID,
    COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START, COL_STATUS_CODE,
    COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START, EVENT_ATTR_PREFIX,
    InputSpan, LINK_ATTR_PREFIX, NestedSet, assign_nested_set, span_schema, span_schema_with_attrs,
};
pub use store::{
    MatchCmp, MatchScope, MatchValue, ScanJob, ScanOptions, ScanResult, SpanMatcher, SpanStore,
    filter_trace_spans_by_time,
};
