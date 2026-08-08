//! `LogQL` parser front-end for Crabka's Loki-compatible logs path.
//!
//! This slice covers stream selectors, line filters, the `json`, `logfmt`,
//! `pattern`, and `regexp` parser stages, `line_format`, field filters, range
//! aggregations, vector aggregations, and unwrapped range aggregation samples.
//! Binary operations and the wider `PromQL` expression surface stay out until
//! the querier has the basic Loki path wired.

use std::collections::BTreeMap;

mod error;
mod extract;
mod filters;
mod labels;
mod planner;
mod stream;
mod syntax;
mod template;
mod types;
mod util;

pub use error::ParseError;
pub use extract::{JsonExtraction, JsonParserConfig, LogfmtExtraction, LogfmtParserConfig};
pub use filters::{
    ComparisonOp, FieldFilter, FieldFilterChain, FieldFilterExpression, FieldFilterLogicOp,
    FieldValue, IpMatcher, LineFilter, LineFilterOp,
};
pub use labels::{
    LabelFormat, LabelFormatAssignment, LabelFormatValue, LabelSelection, LabelSelectionMatcher,
    LabelSelectionSet, UnwrapConversion, UnwrapExpression,
};
pub use planner::{PlanError, StreamPlan, plan_stream_query};
pub use stream::{
    LabelMatcher, MatchOp, ParserStage, PatternParser, PipelineEvaluation, PipelineStage,
    RegexpParser, StreamQuery,
};
pub use syntax::{
    MetricBinaryArithmetic, MetricBinaryComparison, MetricBinarySet, MetricBinarySetOp,
    MetricLabelJoin, MetricLabelReplace, MetricQuery, MetricScalarArithmetic,
    MetricScalarArithmeticOp, MetricScalarComparison, MetricVectorGroupModifier,
    MetricVectorMatching, Quantile, RangeAggregation, VectorAggregation, VectorAggregationOp,
    VectorGrouping, parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query,
};
pub use template::LineFormat;
pub use types::{
    DestinationLabel, DurationNanos, JsonExpressionPath, OffsetNanos, QuantileDenominator,
    QuantileNumerator, SourceLabel,
};

pub type Labels = BTreeMap<String, String>;
pub const UNWRAP_SAMPLE_VALUE_LABEL: &str = "__crabka_unwrap_sample_value__";
