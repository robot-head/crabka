//! `PromQL` engine for Crabka's Prometheus/Grafana-Mimir-equivalent metrics backend.
//!
//! Parses `PromQL` with `promql-parser`, lowers the AST onto `DataFusion` plans, and
//! evaluates instant/range queries over a step grid.
// `#[tracing::instrument]` adds a future layer to already deeply-nested
// DataFusion plan/optimizer types; the test target's Send auto-trait evaluation
// then overflows the default depth, so raise it.
#![recursion_limit = "512"]

mod block_store;
mod conformance;
mod engine;
mod error;
mod extension;
mod functions;
mod http_api;
mod ids;
mod in_memory;
mod merged_store;
pub mod metrics;
mod planner;
mod query_frontend;
mod range_array;
mod result;
mod ruler;
mod store;
#[cfg(test)]
mod test_support;

pub use block_store::MetricBlockStore;
pub use conformance::{
    AnnotationExpect, ExpectLine, LoadSeries, SampleSpec, Statement, TestFile, parse_test_file,
    testkit,
    testkit::{run_test_file, run_test_path},
};
pub use engine::{EngineOpts, PromqlEngine};
pub use error::PromqlError;
pub use extension::{
    instant_manipulate::{InstantManipulate, InstantManipulateExec},
    normalize::{SeriesNormalize, SeriesNormalizeExec},
    range_manipulate::{RangeManipulate, RangeManipulateExec, build_extended_range_schema},
    series_divide::{SeriesDivide, SeriesDivideExec},
};
pub use functions::{
    delta_udf, idelta_udf, increase_udf, irate_udf, rate_family_udfs, rate_udf, register_rate_udfs,
};
pub use http_api::{PrometheusApiState, prometheus_router};
pub use ids::{Offset, PartitionIndex};
pub use in_memory::{
    DEFAULT_RETENTION, InMemoryMetricStore, PartitionWatermark, PruneStats, WalHead,
};
pub use merged_store::MergedMetricStore;
pub use planner::{DurationExprContext, parse_promql, parse_promql_with_duration_context};
pub use query_frontend::{
    FrontendRangeQuery, FrontendRangeRequest, ObjectStoreQueryFrontendCache, QueryFrontendCache,
    QueryFrontendOptions, QueryShard, RangeQueryCache, RangeQueryExecutor,
    execute_range_query_frontend, merge_range_query_results, plan_range_query,
};
pub use range_array::RangeArray;
pub use result::{Annotations, InstantSample, QueryResult, RangeSeries, SampleValue};
pub use ruler::{
    AlertmanagerAlert, AlertmanagerSink, RecordingRuleWalSink, RulerAlertState,
    RulerAlertStateRecord, RulerGroupEvaluation, RulerGroupState, RulerGroupStateRecord,
    RulerShard, RulerStateSink, RulerWalError, evaluate_and_append_recording_rule,
    evaluate_and_append_recording_rule_group, evaluate_and_dispatch_alerting_rule,
    evaluate_and_dispatch_alerting_rule_group, evaluate_and_dispatch_alerting_rule_with_state,
    evaluate_and_persist_alerting_rule_group, evaluate_and_persist_alerting_rule_with_state,
    evaluate_and_persist_ruler_rule_group, evaluate_and_persist_ruler_rule_set,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval, evaluate_recording_rule,
    evaluate_ruler_rule_group, evaluate_ruler_rule_set, filter_ruler_rule_set_due_for_eval,
    filter_ruler_rule_set_for_shard, filter_ruler_rule_set_for_shard_due_for_eval,
};
pub use store::{
    ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
    NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
};
