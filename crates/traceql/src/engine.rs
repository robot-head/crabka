//! Public `TraceQL` engine.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, DictionaryArray, Float64Array, Int64Array, LargeStringArray, ListArray,
    StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::array::AsArray;

use crate::ast::{
    Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, QueryHints, Scope,
    SpansetExpr, Value,
};
use crate::error::{Result, TraceqlError};
use crate::parser::parse;
use crate::planner::{PlannerContext, plan_query};
use crate::result::{
    AttrValue, ScopedTag, SearchResponse, SpanRef, SpanSet, TagScope, TraceMetricExemplar,
    TraceMetricSeries, TraceMetricsResponse, TraceResult, TraceSpans, TypedValue,
};
use crate::span_columns::{
    ATTR_PREFIX, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME, COL_EVENT_TIME_SINCE_START,
    COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_LINK_SPAN_ID,
    COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID,
    COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START, COL_STATUS_CODE,
    COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START, EVENT_ATTR_PREFIX,
    LINK_ATTR_PREFIX,
};
use crate::store::{MatchCmp, MatchScope, MatchValue, ScanOptions, SpanMatcher, SpanStore};

const DEFAULT_HISTOGRAM_BUCKETS_NS: &[f64] = &[
    2_000_000.0,
    4_000_000.0,
    8_000_000.0,
    16_000_000.0,
    32_000_000.0,
    64_000_000.0,
    128_000_000.0,
    256_000_000.0,
    512_000_000.0,
    1_024_000_000.0,
    2_048_000_000.0,
    4_096_000_000.0,
    8_192_000_000.0,
    16_384_000_000.0,
];
const BLOCK_ATTR_KEYS: &str = "attr_keys";
const BLOCK_ATTR_VALUE: &str = "attr_value";
const BLOCK_ATTR_VALUE_INT: &str = "attr_value_int";
const BLOCK_ATTR_VALUE_DOUBLE: &str = "attr_value_double";
const BLOCK_ATTR_VALUE_BOOL: &str = "attr_value_bool";
const RESOURCE_ATTR_PREFIX: &str = "__resource.";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    pub default_limit: usize,
    pub default_spss: usize,
    pub max_traces: usize,
    pub max_exemplars: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            default_limit: 20,
            default_spss: 3,
            max_traces: 1000,
            max_exemplars: 0,
        }
    }
}

pub struct TraceqlEngine<S: SpanStore> {
    store: Arc<S>,
    opts: EngineOpts,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchOptions {
    pub limit: usize,
    pub spss: usize,
    pub search_limit: Option<usize>,
    pub scan_options: ScanOptions,
}

impl<S: SpanStore> TraceqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }

    #[must_use]
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }

    #[must_use]
    pub fn effective_search_limit(&self, limit: usize) -> usize {
        if limit == 0 {
            self.opts.default_limit
        } else {
            limit
        }
        .min(self.opts.max_traces)
    }

    #[must_use]
    pub fn max_traces(&self) -> usize {
        self.opts.max_traces
    }

    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
    ) -> Result<SearchResponse> {
        self.search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                ..SearchOptions::default()
            },
        )
        .await
    }

    pub async fn search_with_spss(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<SearchResponse> {
        self.search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                spss,
                search_limit: None,
                scan_options: ScanOptions::default(),
            },
        )
        .await
    }

    pub async fn search_with_options(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        options: SearchOptions,
    ) -> Result<SearchResponse> {
        let q = parse(query)?;
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns,
                end_ns,
                scan_options: options.scan_options.clone(),
            },
            &q,
        )
        .await?;
        let batches = planned
            .ctx
            .execute_logical_plan(planned.plan)
            .await?
            .collect()
            .await?;
        let effective_limit = self.effective_search_limit(options.limit);
        let search_limit = options
            .search_limit
            .unwrap_or(effective_limit)
            .min(self.opts.max_traces);
        let effective_spss = if options.spss == 0 {
            self.opts.default_spss
        } else {
            options.spss
        };
        assemble_search_response(
            &batches,
            search_limit,
            effective_spss,
            q.hints.most_recent,
            planned.inspected_bytes,
        )
    }

    pub async fn query_range(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    ) -> Result<TraceMetricsResponse> {
        self.query_range_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            step_ns,
            ScanOptions::default(),
        )
        .await
    }

    pub async fn query_range_with_options(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        scan_options: ScanOptions,
    ) -> Result<TraceMetricsResponse> {
        let q = parse(query)?;
        let metric = metric_plan(&q)?;
        let max_exemplars = hinted_max_exemplars(self.opts.max_exemplars, q.hints.exemplars);
        if let Some(compare) = metric.compare.clone() {
            return self
                .query_range_compare(
                    tenant,
                    q.root,
                    compare,
                    MetricsRange {
                        scan_start: start_ns,
                        scan_end: end_ns,
                        output_start: start_ns,
                        step: step_ns,
                    },
                    scan_options,
                )
                .await;
        }

        let mut scan_options = scan_options;
        extend_metric_projection_matchers(&mut scan_options, &metric);
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns,
                end_ns,
                scan_options,
            },
            &Query {
                root: q.root,
                pipeline: Vec::new(),
                hints: QueryHints::default(),
            },
        )
        .await?;
        let batches = collect_planned_batches(planned).await?;
        assemble_metrics_response(
            &batches,
            start_ns,
            end_ns,
            step_ns,
            &metric,
            max_exemplars,
            start_ns,
        )
    }

    /// Executes Tempo's attribute-comparison `compare()` metric.
    ///
    /// Scans every span matching the outer spanset over the query range, then
    /// partitions the matched spans into the `selection` group (spans that also
    /// match the compare's `selection` spanset, optionally restricted to the
    /// `[start, end]` sub-window) and the `baseline` group (the rest). For every
    /// attribute present on the spans, it counts how many spans in each group
    /// carry each distinct value per step bucket, keeps the `top_n` most-frequent
    /// values per (group, attribute), and emits per-value series plus per-group
    /// total series.
    async fn query_range_compare(
        &self,
        tenant: &str,
        root: crate::ast::SpansetExpr,
        compare: CompareSpec,
        range: MetricsRange,
        scan_options: ScanOptions,
    ) -> Result<TraceMetricsResponse> {
        // Validate the selection shape up front so unsupported shapes fail fast
        // with a clear error. Grafana sends selector / And / Or of comparisons
        // (e.g. `{ status = error }`); structural ops, parent scope, and nested
        // event/link scopes are rejected (a per-row, single-span evaluator can't
        // express cross-span structure).
        validate_compare_selection(&compare.selection)?;

        // No attribute projection is added here: the store already supplies
        // every attribute unconditionally. The block attr-list columns
        // (`attr_keys`/`attr_value*`) carry all unpromoted attrs for the real
        // store, and promoted `attr.<key>` columns are present in the scan
        // schema regardless of the filter/by matchers. `compare_row` merges the
        // promoted and block sources and dedups them.
        let planned = plan_query(
            self.store.as_ref(),
            &PlannerContext {
                tenant: tenant.to_string(),
                start_ns: range.scan_start,
                end_ns: range.scan_end,
                scan_options,
            },
            &Query {
                root,
                pipeline: Vec::new(),
                hints: QueryHints::default(),
            },
        )
        .await?;
        let batches = collect_planned_batches(planned).await?;
        assemble_compare_response(&batches, &compare, range)
    }

    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.store.trace_by_id(tenant, trace_id).await
    }

    pub async fn trace_by_id_within(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Option<TraceSpans>> {
        self.store
            .trace_by_id_within(tenant, trace_id, start_ns, end_ns)
            .await
    }

    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.store.tag_names(tenant, scope, start_ns, end_ns).await
    }

    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.store.tag_values(tenant, tag, start_ns, end_ns).await
    }
}

fn hinted_max_exemplars(default: usize, hint: Option<bool>) -> usize {
    match hint {
        Some(false) => 0,
        Some(true) | None => default,
    }
}

#[derive(Clone, Copy)]
enum MetricFunction {
    Rate,
    CountOverTime,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    HistogramOverTime,
    QuantileOverTime,
}

struct MetricPlan {
    function: MetricFunction,
    value: Option<Field>,
    quantiles: Vec<f64>,
    by: Vec<Field>,
    filter: Option<MetricFilter>,
    rank: Option<RankLimit>,
    compare: Option<CompareSpec>,
}

/// The parsed `compare({selection}, topN [, start, end])` spec carried on a
/// `MetricPlan`. Execution scans the outer spanset and partitions its spans into
/// the `selection` group (also matching `selection`) and the `baseline` group
/// (the rest), then emits per-attribute value-distribution series.
#[derive(Clone)]
struct CompareSpec {
    selection: crate::ast::SpansetExpr,
    top_n: usize,
    start: Option<i64>,
    end: Option<i64>,
}

#[derive(Clone, Copy)]
struct MetricFilter {
    op: crate::ast::ComparisonOp,
    value: f64,
}

#[derive(Clone, Copy)]
struct RankLimit {
    direction: RankDirection,
    k: usize,
}

#[derive(Clone, Copy)]
enum RankDirection {
    Top,
    Bottom,
}

#[derive(Clone, Copy)]
struct MetricsRange {
    scan_start: i64,
    scan_end: i64,
    output_start: i64,
    step: i64,
}

fn metric_plan(q: &Query) -> Result<MetricPlan> {
    let normalized_pipeline;
    let pipeline = if q.pipeline.iter().any(is_inert_metric_stage) {
        normalized_pipeline = q
            .pipeline
            .iter()
            .filter(|stage| !is_inert_metric_stage(stage))
            .cloned()
            .collect::<Vec<_>>();
        normalized_pipeline.as_slice()
    } else {
        q.pipeline.as_slice()
    };

    let parts = metric_pipeline_parts(pipeline)?;
    // A `compare()` stage is a standalone metric (no `*_over_time()` aggregate
    // needed): `{outer} | compare({selection}, topN)`. When present it takes
    // precedence and the aggregate, if any, is ignored.
    if let Some(compare) = parts.as_ref().and_then(|parts| parts.compare.clone()) {
        return Ok(metric_plan_with_compare(compare));
    }
    let Some(parts) = parts else {
        return Err(unsupported_metric_pipeline());
    };
    let Some(aggregate) = parts.aggregate else {
        return Err(unsupported_metric_pipeline());
    };
    metric_plan_for(aggregate, parts.by, parts.filter, parts.rank)
}

fn extend_metric_projection_matchers(options: &mut ScanOptions, metric: &MetricPlan) {
    for matcher in metric_nested_projection_matchers(metric) {
        if !options.projection_matchers.contains(&matcher) {
            options.projection_matchers.push(matcher);
        }
    }
}

fn metric_nested_projection_matchers(metric: &MetricPlan) -> Vec<SpanMatcher> {
    let mut out = Vec::new();
    for field in metric.by.iter().chain(metric.value.iter()) {
        if let Some(matcher) = nested_metric_projection_matcher(field)
            && !out.contains(&matcher)
        {
            out.push(matcher);
        }
    }
    out
}

fn nested_metric_projection_matcher(field: &Field) -> Option<SpanMatcher> {
    let (scope, key) = match &field.scope {
        Scope::Event => (MatchScope::Event, field.key.clone()),
        Scope::Link => (MatchScope::Link, field.key.clone()),
        Scope::Intrinsic(Intrinsic::EventName) => (MatchScope::Intrinsic, "event:name".into()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            (MatchScope::Intrinsic, "event:timeSinceStart".into())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => (MatchScope::Intrinsic, "link:traceID".into()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => (MatchScope::Intrinsic, "link:spanID".into()),
        // A metric `by()`/value field on a regular span or resource attribute
        // must be projected so the store materializes its `attr.<key>` column for
        // GROUP BY — otherwise `rate() by(span.http.method)` fails with "missing
        // column attr.http.method". Projection-only (does not filter), so spans
        // lacking the attribute stay in the nil group.
        Scope::Both => (MatchScope::Both, field.key.clone()),
        Scope::Span => (MatchScope::Span, field.key.clone()),
        Scope::Resource => (MatchScope::Resource, field.key.clone()),
        Scope::Parent | Scope::Instrumentation | Scope::Intrinsic(_) => return None,
    };
    Some(SpanMatcher {
        scope,
        key,
        op: MatchCmp::Neq,
        value: MatchValue::Nil,
        negated: false,
    })
}

struct MetricPipelineParts<'a> {
    aggregate: Option<&'a Aggregate>,
    by: Vec<Field>,
    filter: Option<MetricFilter>,
    rank: Option<RankLimit>,
    compare: Option<CompareSpec>,
}

fn metric_pipeline_parts(pipeline: &[Pipeline]) -> Result<Option<MetricPipelineParts<'_>>> {
    let mut aggregate = None;
    let mut by = None;
    let mut filter = None;
    let mut rank = None;
    let mut compare = None;
    for stage in pipeline {
        match stage {
            Pipeline::Aggregate(value) if aggregate.is_none() => aggregate = Some(value),
            Pipeline::By(value) if by.is_none() => by = Some(value.clone()),
            Pipeline::Filter { op, value } if filter.is_none() => {
                filter = Some(metric_filter(*op, *value)?);
            }
            stage @ (Pipeline::TopK(_) | Pipeline::BottomK(_)) if rank.is_none() => {
                rank = Some(rank_limit(stage)?);
            }
            Pipeline::Compare {
                selection,
                top_n,
                start,
                end,
            } if compare.is_none() => {
                compare = Some(CompareSpec {
                    selection: (**selection).clone(),
                    top_n: *top_n,
                    start: *start,
                    end: *end,
                });
            }
            _ => return Ok(None),
        }
    }
    // A compare-only pipeline has no aggregate but is still a valid metric.
    if aggregate.is_none() && compare.is_none() {
        return Ok(None);
    }
    Ok(Some(MetricPipelineParts {
        aggregate,
        by: by.unwrap_or_default(),
        filter,
        rank,
        compare,
    }))
}

fn unsupported_metric_pipeline() -> TraceqlError {
    TraceqlError::Unsupported("traceql metrics: expected supported *_over_time() metric".into())
}

fn is_inert_metric_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::Select(_) | Pipeline::Coalesce | Pipeline::With(_)
    )
}

fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    match pipeline {
        Pipeline::TopK(k) => Ok(RankLimit {
            direction: RankDirection::Top,
            k: *k,
        }),
        Pipeline::BottomK(k) => Ok(RankLimit {
            direction: RankDirection::Bottom,
            k: *k,
        }),
        other => Err(TraceqlError::Unsupported(format!(
            "traceql metrics: expected topk/bottomk, got {other:?}"
        ))),
    }
}

fn metric_filter(op: crate::ast::ComparisonOp, value: f64) -> Result<MetricFilter> {
    if !value.is_finite() {
        return Err(TraceqlError::Plan(
            "metric comparison filter value is not finite".into(),
        ));
    }
    match op {
        crate::ast::ComparisonOp::Eq
        | crate::ast::ComparisonOp::Neq
        | crate::ast::ComparisonOp::Lt
        | crate::ast::ComparisonOp::Lte
        | crate::ast::ComparisonOp::Gt
        | crate::ast::ComparisonOp::Gte => Ok(MetricFilter { op, value }),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => Err(
            TraceqlError::Unsupported("regex filter on metric scalar is not supported".into()),
        ),
    }
}

fn metric_plan_for(
    aggregate: &Aggregate,
    by: Vec<Field>,
    filter: Option<MetricFilter>,
    rank: Option<RankLimit>,
) -> Result<MetricPlan> {
    let (function, value, quantiles) = match aggregate {
        Aggregate::Rate => (MetricFunction::Rate, None, Vec::new()),
        Aggregate::CountOverTime => (MetricFunction::CountOverTime, None, Vec::new()),
        Aggregate::SumOverTime(field) => {
            (MetricFunction::SumOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::AvgOverTime(field) => {
            (MetricFunction::AvgOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::MinOverTime(field) => {
            (MetricFunction::MinOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::MaxOverTime(field) => {
            (MetricFunction::MaxOverTime, Some(field.clone()), Vec::new())
        }
        Aggregate::HistogramOverTime(field) => (
            MetricFunction::HistogramOverTime,
            Some(field.clone()),
            Vec::new(),
        ),
        Aggregate::QuantileOverTime { field, quantiles } => (
            MetricFunction::QuantileOverTime,
            Some(field.clone()),
            quantiles.clone(),
        ),
        Aggregate::Count
        | Aggregate::Avg(_)
        | Aggregate::Sum(_)
        | Aggregate::Min(_)
        | Aggregate::Max(_) => {
            return Err(TraceqlError::Unsupported(
                "traceql metrics: expected supported *_over_time() metric".into(),
            ));
        }
    };
    Ok(MetricPlan {
        function,
        value,
        quantiles,
        by,
        filter,
        rank,
        compare: None,
    })
}

/// Builds the `MetricPlan` for a `compare()` stage. The function/value/by fields
/// are inert placeholders — `query_range_compare` reads `compare` directly and
/// never runs the `*_over_time()` machinery.
fn metric_plan_with_compare(compare: CompareSpec) -> MetricPlan {
    MetricPlan {
        function: MetricFunction::CountOverTime,
        value: None,
        quantiles: Vec::new(),
        by: Vec::new(),
        filter: None,
        rank: None,
        compare: Some(compare),
    }
}

async fn collect_planned_batches(
    planned: crate::planner::PlannedSpanset,
) -> Result<Vec<RecordBatch>> {
    Ok(planned
        .ctx
        .execute_logical_plan(planned.plan)
        .await?
        .collect()
        .await?)
}

/// The reserved label key Tempo uses to tag a compare series with its group
/// (`baseline` / `selection`) or per-group total (`baseline_total` /
/// `selection_total`). Grafana's exploretraces Comparison view reads it.
const META_TYPE_KEY: &str = "__meta_type";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CompareGroup {
    Baseline,
    Selection,
}

impl CompareGroup {
    fn meta_type(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Selection => "selection",
        }
    }

    fn total_meta_type(self) -> &'static str {
        match self {
            Self::Baseline => "baseline_total",
            Self::Selection => "selection_total",
        }
    }
}

/// Validates that a compare selection spanset is a shape the per-row evaluator
/// supports: a selector whose `FieldExpr` is a boolean combination of
/// span/resource/intrinsic comparisons, or a spanset-level `And`/`Or` of such
/// selectors. Structural operators and parent/event/link scopes are rejected.
fn validate_compare_selection(selection: &SpansetExpr) -> Result<()> {
    match selection {
        SpansetExpr::Selector(fe) => validate_compare_field_expr(fe),
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            validate_compare_selection(lhs)?;
            validate_compare_selection(rhs)
        }
        SpansetExpr::Structural { .. } => Err(TraceqlError::Unsupported(
            "compare() selection does not support structural operators".into(),
        )),
    }
}

fn validate_compare_field_expr(fe: &FieldExpr) -> Result<()> {
    match fe {
        FieldExpr::Comparison { lhs, .. } | FieldExpr::Field(lhs) => {
            compare_field_class(lhs).map(|_| ())
        }
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            validate_compare_field_expr(a)?;
            validate_compare_field_expr(b)
        }
        FieldExpr::Not(inner) => validate_compare_field_expr(inner),
        FieldExpr::Const(_) => Ok(()),
    }
}

/// A span/resource attribute or selection-evaluable intrinsic, classified for
/// per-row lookup. Returns an `Unsupported` error for scopes the single-span
/// compare evaluator cannot resolve (parent/event/link and non-selection
/// intrinsics like trace-level or nested-set fields).
enum CompareFieldClass {
    /// A span- or resource-scoped attribute keyed by its raw key. `Both` matches
    /// either scope; `Span`/`Resource` are scope-pinned.
    Attr { scope: Scope, key: String },
    /// A selection-evaluable intrinsic with its row value.
    Intrinsic(Intrinsic),
}

fn compare_field_class(field: &Field) -> Result<CompareFieldClass> {
    match &field.scope {
        Scope::Both | Scope::Span | Scope::Resource => Ok(CompareFieldClass::Attr {
            scope: field.scope.clone(),
            key: field.key.clone(),
        }),
        Scope::Intrinsic(
            intrinsic @ (Intrinsic::Name
            | Intrinsic::Status
            | Intrinsic::StatusMessage
            | Intrinsic::Kind
            | Intrinsic::Duration),
        ) => Ok(CompareFieldClass::Intrinsic(intrinsic.clone())),
        other => Err(TraceqlError::Unsupported(format!(
            "compare() selection does not support {other:?}"
        ))),
    }
}

/// One scanned span row, projected into the values the compare needs: the span
/// start time, every scoped attribute (for the distribution), and the
/// selection-evaluable intrinsics (for membership).
struct CompareRow {
    ts: i64,
    /// Fully-scoped attribute key (e.g. `span.http.method`,
    /// `resource.service.name`) → display value, deduplicated.
    attrs: Vec<(String, String)>,
    /// Raw span/resource attribute key → typed values (repeated attrs allowed),
    /// used to evaluate the selection's attribute comparisons.
    raw_span_attrs: Vec<(String, AttrValue)>,
    raw_resource_attrs: Vec<(String, AttrValue)>,
    name: Option<String>,
    status_code: Option<i32>,
    status_message: Option<String>,
    kind: Option<i32>,
    duration: Option<i64>,
}

/// Upper bound on distinct values tracked per `(group, attribute)` during
/// accumulation, before `top_n` truncation. A high-cardinality attribute (a
/// URL, request id, or UUID present on every span) would otherwise insert one
/// bucket-length `Vec` per distinct value and exhaust memory. Once a
/// `(group, attribute)` reaches this cap, new distinct values are dropped
/// (already-tracked values keep counting); this keeps memory
/// `O(attrs * cap * buckets)`. The cap is clamped to be at least `top_n` so the
/// final `top_n` cut in `build_compare_series` is never starved.
const COMPARE_MAX_VALUES_PER_ATTR: usize = 256;

type CompareCounts = BTreeMap<(CompareGroup, String, String), Vec<u64>>;
type CompareTotals = BTreeMap<CompareGroup, Vec<u64>>;
/// `attribute → value → group → per-bucket counts`. `build_compare_series`
/// regroups the flat counts into this shape so a single shared value set per
/// attribute can be chosen across both groups (Grafana renders the baseline and
/// selection distributions side by side, so they must cover the same values).
type CompareByAttr = BTreeMap<String, BTreeMap<String, BTreeMap<CompareGroup, Vec<u64>>>>;

fn assemble_compare_response(
    batches: &[RecordBatch],
    compare: &CompareSpec,
    range: MetricsRange,
) -> Result<TraceMetricsResponse> {
    if range.step <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if range.scan_end < range.scan_start {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }
    let bucket_count = usize::try_from((range.scan_end - range.scan_start) / range.step + 1)
        .map_err(|e| TraceqlError::Plan(e.to_string()))?;

    let (counts, totals) = accumulate_compare_counts(batches, compare, range, bucket_count)?;
    let series = build_compare_series(counts, &totals, compare.top_n, range, bucket_count);
    Ok(TraceMetricsResponse { series })
}

/// Scans the batches and accumulates per-bucket span counts: `counts[(group,
/// attr_key, value)][bucket]` and `totals[group][bucket]`. The number of
/// DISTINCT values tracked per `(group, attr_key)` is bounded by
/// `COMPARE_MAX_VALUES_PER_ATTR` (clamped up to `top_n`); once the bound is hit,
/// new distinct values are dropped while already-tracked ones keep counting, so
/// memory is `O(attrs * cap * buckets)` regardless of attribute cardinality.
fn accumulate_compare_counts(
    batches: &[RecordBatch],
    compare: &CompareSpec,
    range: MetricsRange,
    bucket_count: usize,
) -> Result<(CompareCounts, CompareTotals)> {
    let mut counts: CompareCounts = BTreeMap::new();
    let mut totals: CompareTotals = BTreeMap::new();
    // Distinct values already tracked per (group, attr_key), to enforce
    // COMPARE_MAX_VALUES_PER_ATTR during accumulation (see the constant docs).
    let mut distinct_per_attr: BTreeMap<(CompareGroup, String), usize> = BTreeMap::new();
    // The cap must never fall below top_n or the final cut would be starved.
    let value_cap = COMPARE_MAX_VALUES_PER_ATTR.max(compare.top_n);
    // Compile every selection regex once, up front, and reuse across all rows.
    let mut regexes: CompareRegexCache = HashMap::new();
    collect_selection_regexes(&compare.selection, &mut regexes);

    for batch in batches {
        let starts = batch
            .column_by_name(COL_START)
            .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            let ts = starts.value(row);
            if ts < range.scan_start || ts > range.scan_end {
                continue;
            }
            let bucket = usize::try_from((ts - range.scan_start) / range.step)
                .map_err(|e| TraceqlError::Exec(e.to_string()))?;
            let compare_row = compare_row(batch, row, ts)?;
            let group = compare_group_for_row(&compare_row, compare, &regexes);
            let group_totals = totals.entry(group).or_insert_with(|| vec![0; bucket_count]);
            if let Some(slot) = group_totals.get_mut(bucket) {
                *slot += 1;
            }
            for (attr_key, value) in &compare_row.attrs {
                let key = (group, attr_key.clone(), value.clone());
                // An already-tracked value keeps counting regardless of cap; a
                // new distinct value is only tracked while under the per-attr cap.
                if !counts.contains_key(&key) {
                    let distinct = distinct_per_attr
                        .entry((group, attr_key.clone()))
                        .or_insert(0);
                    if *distinct >= value_cap {
                        continue;
                    }
                    *distinct += 1;
                }
                let series = counts.entry(key).or_insert_with(|| vec![0; bucket_count]);
                if let Some(slot) = series.get_mut(bucket) {
                    *slot += 1;
                }
            }
        }
    }
    Ok((counts, totals))
}

/// Determines a span row's compare group: `Selection` if it matches the compare
/// selection spanset (and falls inside the optional `[start, end]` sub-window),
/// otherwise `Baseline`.
fn compare_group_for_row(
    row: &CompareRow,
    compare: &CompareSpec,
    regexes: &CompareRegexCache,
) -> CompareGroup {
    if compare_row_in_selection_window(row, compare)
        && spanset_matches_row(&compare.selection, row, regexes)
    {
        CompareGroup::Selection
    } else {
        CompareGroup::Baseline
    }
}

/// Whether the row's start time lies within the compare's optional selection
/// sub-window. With no window, every matched-outer span is eligible for the
/// selection group.
fn compare_row_in_selection_window(row: &CompareRow, compare: &CompareSpec) -> bool {
    compare.start.is_none_or(|start| row.ts >= start) && compare.end.is_none_or(|end| row.ts <= end)
}

/// Per-query cache of compiled selection regexes, keyed by the raw `=~`/`!~`
/// pattern string. Each value is the pattern compiled as `^(?:pat)$` (matching
/// `string_cmp`'s anchoring). Built once in `assemble_compare_response` and
/// reused for every scanned row so a regex selection does not recompile per
/// span. An uncompilable pattern is simply absent from the map, which the
/// lookup treats as a non-match (preserving the prior best-effort semantics
/// rather than failing the query).
type CompareRegexCache = HashMap<String, regex::Regex>;

/// Collects every `=~`/`!~` literal pattern referenced by the selection and
/// compiles each once into `cache`. Uncompilable patterns are skipped.
fn collect_selection_regexes(selection: &SpansetExpr, cache: &mut CompareRegexCache) {
    match selection {
        SpansetExpr::Selector(fe) => collect_field_expr_regexes(fe, cache),
        SpansetExpr::And(lhs, rhs) | SpansetExpr::Or(lhs, rhs) => {
            collect_selection_regexes(lhs, cache);
            collect_selection_regexes(rhs, cache);
        }
        SpansetExpr::Structural { .. } => {}
    }
}

fn collect_field_expr_regexes(fe: &FieldExpr, cache: &mut CompareRegexCache) {
    match fe {
        FieldExpr::And(a, b) | FieldExpr::Or(a, b) => {
            collect_field_expr_regexes(a, cache);
            collect_field_expr_regexes(b, cache);
        }
        FieldExpr::Not(inner) => collect_field_expr_regexes(inner, cache),
        FieldExpr::Comparison {
            op: ComparisonOp::Re | ComparisonOp::Nre,
            rhs: Value::Str(pattern),
            ..
        } => {
            if !cache.contains_key(pattern)
                && let Ok(re) = regex::Regex::new(&format!("^(?:{pattern})$"))
            {
                cache.insert(pattern.clone(), re);
            }
        }
        FieldExpr::Comparison { .. } | FieldExpr::Field(_) | FieldExpr::Const(_) => {}
    }
}

fn spanset_matches_row(
    selection: &SpansetExpr,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match selection {
        SpansetExpr::Selector(fe) => field_expr_matches_row(fe, row, regexes),
        SpansetExpr::And(lhs, rhs) => {
            spanset_matches_row(lhs, row, regexes) && spanset_matches_row(rhs, row, regexes)
        }
        SpansetExpr::Or(lhs, rhs) => {
            spanset_matches_row(lhs, row, regexes) || spanset_matches_row(rhs, row, regexes)
        }
        // Rejected by validate_compare_selection; treat as non-match defensively.
        SpansetExpr::Structural { .. } => false,
    }
}

fn field_expr_matches_row(fe: &FieldExpr, row: &CompareRow, regexes: &CompareRegexCache) -> bool {
    match fe {
        FieldExpr::Const(value) => *value,
        FieldExpr::And(a, b) => {
            field_expr_matches_row(a, row, regexes) && field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Or(a, b) => {
            field_expr_matches_row(a, row, regexes) || field_expr_matches_row(b, row, regexes)
        }
        FieldExpr::Not(inner) => !field_expr_matches_row(inner, row, regexes),
        FieldExpr::Field(field) => compare_field_present(field, row),
        FieldExpr::Comparison { lhs, op, rhs } => {
            compare_comparison_matches(lhs, *op, rhs, row, regexes)
        }
    }
}

fn compare_field_present(field: &Field, row: &CompareRow) -> bool {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => {
            !compare_row_attr_values(row, &scope, &key).is_empty()
        }
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => compare_intrinsic_present(row, &intrinsic),
        Err(_) => false,
    }
}

fn compare_comparison_matches(
    field: &Field,
    op: ComparisonOp,
    rhs: &Value,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => {
            let values = compare_row_attr_values(row, &scope, &key);
            compare_attr_values_match(&values, op, rhs, regexes)
        }
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => {
            compare_intrinsic_matches(row, &intrinsic, op, rhs, regexes)
        }
        Err(_) => false,
    }
}

/// Projects one scanned span row into a `CompareRow`: every scoped attribute
/// (for the distribution) plus the selection-evaluable intrinsics.
///
/// Attributes come from two sources, mirroring `row_attrs`/`block_row_attrs`:
/// the promoted `attr.<key>` schema columns (span-scoped) and the block
/// attribute-list columns (`attr_keys`/`attr_value*`), where a `__resource.`
/// prefix on the key marks a resource attribute. The root-service column is
/// surfaced as `resource.service.name`.
fn compare_row(batch: &RecordBatch, row: usize, ts: i64) -> Result<CompareRow> {
    let mut attrs: Vec<(String, String)> = Vec::new();
    let mut raw_span_attrs: Vec<(String, AttrValue)> = Vec::new();
    let mut raw_resource_attrs: Vec<(String, AttrValue)> = Vec::new();

    // Promoted `attr.<key>` columns are span-scoped attributes. Event/link
    // attrs are stored as `attr.__event.<k>` / `attr.__link.<k>` columns; they
    // belong to a child scope the per-span span distribution must not surface
    // as `span.__event.<k>` / `span.__link.<k>`, so drop them here.
    for (key, value) in row_attrs(batch, row)? {
        if key.starts_with(EVENT_ATTR_PREFIX) || key.starts_with(LINK_ATTR_PREFIX) {
            continue;
        }
        push_scoped_attr(&mut attrs, "span", &key, &value);
        raw_span_attrs.push((key, value));
    }
    // Block attribute-list columns carry the remaining span + resource attrs.
    for (key, value) in block_row_scoped_attrs(batch, row)? {
        if let Some(stripped) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
            // The per-span `__resource.service.name` block attr would
            // double-count `resource.service.name`, which the rest of the
            // engine defines as the TRACE-ROOT service (COL_ROOT_SERVICE_NAME,
            // emitted below). Skip it so the root column is the sole emitter.
            if stripped == "service.name" {
                continue;
            }
            push_scoped_attr(&mut attrs, "resource", stripped, &value);
            raw_resource_attrs.push((stripped.to_string(), value));
        } else {
            push_scoped_attr(&mut attrs, "span", &key, &value);
            raw_span_attrs.push((key, value));
        }
    }
    // The root-service-name column is the canonical `resource.service.name`.
    if let Some(service) = string_value(batch, COL_ROOT_SERVICE_NAME, row)
        && !service.is_empty()
    {
        push_scoped_attr(
            &mut attrs,
            "resource",
            "service.name",
            &AttrValue::Str(service.clone()),
        );
        raw_resource_attrs.push(("service.name".to_string(), AttrValue::Str(service)));
    }

    let name = string_value(batch, COL_NAME, row);
    let status_code = i32_value(batch, COL_STATUS_CODE, row).ok();
    let status_message = string_value(batch, COL_STATUS_MESSAGE, row);
    let kind = i32_value(batch, COL_KIND, row).ok();
    let duration = i64_value(batch, COL_DURATION, row).ok();

    // Intrinsics participate in the value distribution too (Tempo emits e.g.
    // `name` and `status` distributions): name as-is, status/kind as their
    // TraceQL enum names.
    if let Some(name) = &name {
        attrs.push(("name".to_string(), name.clone()));
    }
    if let Some(code) = status_code {
        attrs.push(("status".to_string(), status_enum_name(code).to_string()));
    }
    if let Some(code) = kind {
        attrs.push(("kind".to_string(), kind_enum_name(code).to_string()));
    }

    attrs.sort();
    attrs.dedup();
    Ok(CompareRow {
        ts,
        attrs,
        raw_span_attrs,
        raw_resource_attrs,
        name,
        status_code,
        status_message,
        kind,
        duration,
    })
}

fn push_scoped_attr(attrs: &mut Vec<(String, String)>, scope: &str, key: &str, value: &AttrValue) {
    attrs.push((format!("{scope}.{key}"), attr_value_display(value)));
}

fn attr_value_display(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Float(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
    }
}

/// Like `block_row_attrs`, but keeps `__resource.`-prefixed keys so the caller
/// can split span vs resource scope (the search path drops resource attrs).
fn block_row_scoped_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let Some(keys) = optional_list_column(batch, BLOCK_ATTR_KEYS)? else {
        return Ok(Vec::new());
    };
    if keys.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Exec("attr_keys row is not Utf8".into()))?;
    let str_values = optional_list_column(batch, BLOCK_ATTR_VALUE)?;
    let int_values = optional_list_column(batch, BLOCK_ATTR_VALUE_INT)?;
    let double_values = optional_list_column(batch, BLOCK_ATTR_VALUE_DOUBLE)?;
    let bool_values = optional_list_column(batch, BLOCK_ATTR_VALUE_BOOL)?;

    let mut out = Vec::new();
    for attr_idx in 0..key_values.len() {
        if key_values.is_null(attr_idx) {
            continue;
        }
        let key = key_values.value(attr_idx);
        out.extend(
            block_attr_values_for_key(
                str_values,
                int_values,
                double_values,
                bool_values,
                row,
                attr_idx,
            )?
            .into_iter()
            .map(|value| (key.to_string(), value)),
        );
    }
    Ok(out)
}

fn compare_row_attr_values<'a>(
    row: &'a CompareRow,
    scope: &Scope,
    key: &str,
) -> Vec<&'a AttrValue> {
    let mut out = Vec::new();
    let want_span = matches!(scope, Scope::Both | Scope::Span);
    let want_resource = matches!(scope, Scope::Both | Scope::Resource);
    if want_span {
        out.extend(
            row.raw_span_attrs
                .iter()
                .filter(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value),
        );
    }
    if want_resource {
        out.extend(
            row.raw_resource_attrs
                .iter()
                .filter(|(attr_key, _)| attr_key == key)
                .map(|(_, value)| value),
        );
    }
    out
}

/// Tempo array-attribute semantics: equality/`<`/`>`/regex match if ANY value
/// satisfies the predicate; `!=`/`!~` match only if ALL values do; an absent
/// attribute matches ONLY `= nil`. This mirrors the planner SQL (`NULL != v`
/// excludes) and the in-memory store's nil rules: a span lacking the attribute
/// is NOT pulled into the selection by a `!= <concrete>` / `!~` predicate.
fn compare_attr_values_match(
    values: &[&AttrValue],
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    if values.is_empty() {
        return matches!((op, rhs), (ComparisonOp::Eq, Value::Nil));
    }
    if matches!(rhs, Value::Nil) {
        // present value: `= nil` is false, `!= nil` is true.
        return matches!(op, ComparisonOp::Neq);
    }
    match op {
        ComparisonOp::Neq | ComparisonOp::Nre => values
            .iter()
            .all(|value| compare_value_match(value, op, rhs, regexes)),
        _ => values
            .iter()
            .any(|value| compare_value_match(value, op, rhs, regexes)),
    }
}

fn compare_value_match(
    value: &AttrValue,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    match (value, rhs) {
        (AttrValue::Str(value), Value::Str(rhs)) => string_cmp(value, op, rhs, regexes),
        (AttrValue::Int(value), Value::Int(rhs) | Value::Duration(rhs)) => {
            num_cmp(*value, op, *rhs)
        }
        (AttrValue::Float(value), Value::Float(rhs)) => float_cmp(*value, op, *rhs),
        (AttrValue::Bool(value), Value::Bool(rhs)) => bool_cmp(*value, op, *rhs),
        _ => false,
    }
}

fn compare_intrinsic_present(row: &CompareRow, intrinsic: &Intrinsic) -> bool {
    match intrinsic {
        Intrinsic::Name => row.name.as_ref().is_some_and(|name| !name.is_empty()),
        Intrinsic::Status => row.status_code.is_some(),
        Intrinsic::StatusMessage => row
            .status_message
            .as_ref()
            .is_some_and(|msg| !msg.is_empty()),
        Intrinsic::Kind => row.kind.is_some(),
        Intrinsic::Duration => row.duration.is_some(),
        _ => false,
    }
}

fn compare_intrinsic_matches(
    row: &CompareRow,
    intrinsic: &Intrinsic,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    match intrinsic {
        Intrinsic::Name => row.name.as_ref().is_some_and(
            |name| matches!(rhs, Value::Str(rhs) if string_cmp(name, op, rhs, regexes)),
        ),
        Intrinsic::StatusMessage => row
            .status_message
            .as_ref()
            .is_some_and(|msg| matches!(rhs, Value::Str(rhs) if string_cmp(msg, op, rhs, regexes))),
        Intrinsic::Status => row
            .status_code
            .is_some_and(|code| enum_cmp(code, op, rhs, status_enum_value)),
        Intrinsic::Kind => row
            .kind
            .is_some_and(|code| enum_cmp(code, op, rhs, kind_enum_value)),
        Intrinsic::Duration => row.duration.is_some_and(|duration| match rhs {
            Value::Int(rhs) | Value::Duration(rhs) => num_cmp(duration, op, *rhs),
            _ => false,
        }),
        _ => false,
    }
}

fn enum_cmp(
    value: i32,
    op: ComparisonOp,
    rhs: &Value,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match rhs {
        Value::Str(name) => enum_value(&name.to_ascii_lowercase()),
        Value::Int(value) => i32::try_from(*value).ok(),
        _ => None,
    };
    expected.is_some_and(|expected| num_cmp(i64::from(value), op, i64::from(expected)))
}

fn string_cmp(value: &str, op: ComparisonOp, rhs: &str, regexes: &CompareRegexCache) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        // The pattern was precompiled once per query (see CompareRegexCache); an
        // uncompilable pattern is absent from the cache and yields non-match.
        ComparisonOp::Re => regexes.get(rhs).is_some_and(|re| re.is_match(value)),
        ComparisonOp::Nre => regexes.get(rhs).is_some_and(|re| !re.is_match(value)),
        ComparisonOp::Lt | ComparisonOp::Lte | ComparisonOp::Gt | ComparisonOp::Gte => false,
    }
}

fn num_cmp(value: i64, op: ComparisonOp, rhs: i64) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        ComparisonOp::Lt => value < rhs,
        ComparisonOp::Lte => value <= rhs,
        ComparisonOp::Gt => value > rhs,
        ComparisonOp::Gte => value >= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

#[allow(
    clippy::float_cmp,
    reason = "TraceQL float equality is exact comparison, matching the planner's SQL"
)]
fn float_cmp(value: f64, op: ComparisonOp, rhs: f64) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        ComparisonOp::Lt => value < rhs,
        ComparisonOp::Lte => value <= rhs,
        ComparisonOp::Gt => value > rhs,
        ComparisonOp::Gte => value >= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}

fn bool_cmp(value: bool, op: ComparisonOp, rhs: bool) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        _ => false,
    }
}

fn status_enum_value(name: &str) -> Option<i32> {
    match name {
        "unset" => Some(0),
        "ok" => Some(1),
        "error" => Some(2),
        _ => None,
    }
}

fn kind_enum_value(name: &str) -> Option<i32> {
    match name {
        "unspecified" => Some(0),
        "internal" => Some(1),
        "server" => Some(2),
        "client" => Some(3),
        "producer" => Some(4),
        "consumer" => Some(5),
        _ => None,
    }
}

fn status_enum_name(code: i32) -> &'static str {
    match code {
        1 => "ok",
        2 => "error",
        _ => "unset",
    }
}

fn kind_enum_name(code: i32) -> &'static str {
    match code {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

/// Builds the compare time series from the accumulated per-bucket counts.
///
/// For each attribute, picks ONE shared value set — the `top_n` values with the
/// highest SELECTION-group count (ties broken by value ascending for
/// determinism, since the selection is what the user is investigating) — and
/// emits BOTH a baseline and a selection series for each of those values,
/// labelled `{__meta_type=<group>, <attribute>=<value>}`. A value absent from a
/// group is zero-filled so the baseline and selection distributions cover the
/// same values (Grafana's Comparison view renders them side by side).
/// Per-group totals are emitted as `{__meta_type=<group>_total}`.
fn build_compare_series(
    counts: CompareCounts,
    totals: &CompareTotals,
    top_n: usize,
    range: MetricsRange,
    bucket_count: usize,
) -> Vec<TraceMetricSeries> {
    // Regroup the flat (group, attr_key, value) entries into attr → value →
    // group → buckets so a single shared value set per attribute can be picked.
    let mut by_attr: CompareByAttr = BTreeMap::new();
    for ((group, attr_key, value), buckets) in counts {
        by_attr
            .entry(attr_key)
            .or_default()
            .entry(value)
            .or_default()
            .insert(group, buckets);
    }

    let mut series = Vec::new();
    for (attr_key, values) in by_attr {
        // Rank values by the SELECTION-group count (descending), tie-broken by
        // value ascending. This yields a single, deterministic top_n value set
        // shared by both groups.
        let mut ranked: Vec<(&String, u64)> = values
            .iter()
            .map(|(value, per_group)| {
                let selection_total: u64 = per_group
                    .get(&CompareGroup::Selection)
                    .map_or(0, |buckets| buckets.iter().sum());
                (value, selection_total)
            })
            .collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        ranked.truncate(top_n);

        // Emit a baseline and a selection series for each chosen value; a value
        // missing from a group is zero-filled so both groups share the value set.
        for (value, _) in ranked {
            let per_group = &values[value];
            for group in [CompareGroup::Baseline, CompareGroup::Selection] {
                let buckets = per_group
                    .get(&group)
                    .cloned()
                    .unwrap_or_else(|| vec![0; bucket_count]);
                series.push(compare_value_series(
                    group, &attr_key, value, &buckets, range,
                ));
            }
        }
    }

    // Per-group totals, including a zero-valued total for any group with no
    // spans so Grafana always has a denominator for both groups.
    for group in [CompareGroup::Baseline, CompareGroup::Selection] {
        let buckets = totals
            .get(&group)
            .cloned()
            .unwrap_or_else(|| vec![0; bucket_count]);
        series.push(compare_total_series(group, &buckets, range));
    }

    series
}

fn compare_value_series(
    group: CompareGroup,
    attr_key: &str,
    value: &str,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![
            (META_TYPE_KEY.to_string(), group.meta_type().to_string()),
            (attr_key.to_string(), value.to_string()),
        ],
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}

fn compare_total_series(
    group: CompareGroup,
    buckets: &[u64],
    range: MetricsRange,
) -> TraceMetricSeries {
    TraceMetricSeries {
        labels: vec![(
            META_TYPE_KEY.to_string(),
            group.total_meta_type().to_string(),
        )],
        points: compare_points(buckets, range),
        exemplars: Vec::new(),
    }
}

fn compare_points(buckets: &[u64], range: MetricsRange) -> Vec<(i64, f64)> {
    buckets
        .iter()
        .enumerate()
        .map(|(idx, count)| {
            let ts = range.output_start + i64::try_from(idx).unwrap_or(i64::MAX) * range.step;
            (ts, f64_from_u64(*count).unwrap_or(0.0))
        })
        .collect()
}

fn assemble_metrics_response(
    batches: &[RecordBatch],
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    metric: &MetricPlan,
    max_exemplars: usize,
    output_start_ns: i64,
) -> Result<TraceMetricsResponse> {
    if step_ns <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if end_ns < start_ns {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }

    let bucket_count = usize::try_from((end_ns - start_ns) / step_ns + 1)
        .map_err(|e| TraceqlError::Plan(e.to_string()))?;
    let mut buckets: BTreeMap<Vec<(String, String)>, Vec<MetricBucket>> = BTreeMap::new();
    for batch in batches {
        let starts = batch
            .column_by_name(COL_START)
            .ok_or_else(|| TraceqlError::Exec(format!("missing column {COL_START}")))?
            .as_primitive::<arrow::datatypes::Int64Type>();
        for row in 0..batch.num_rows() {
            let ts = starts.value(row);
            if ts < start_ns || ts > end_ns {
                continue;
            }
            let idx = usize::try_from((ts - start_ns) / step_ns)
                .map_err(|e| TraceqlError::Exec(e.to_string()))?;
            let value = match metric.value.as_ref() {
                // A metric with a value field (avg/min/max/sum/histogram/...)
                // only observes spans where that attribute is present. A row
                // whose value field is NULL means the attribute is absent, so
                // the span is skipped entirely rather than folded as 0 — it
                // must not drag min toward 0, bias avg, or add a 0 observation
                // to a histogram bucket.
                Some(field) => match metric_numeric_value(batch, row, field)? {
                    Some(value) => Some(value),
                    None => continue,
                },
                // Value-less metrics (count_over_time / rate) observe every
                // matching span regardless of any value field.
                None => None,
            };
            let labels = metric_labels(batch, row, &metric.by)?;
            let exemplar = metric_exemplar(batch, row, ts, value.unwrap_or(1.0))?;
            let series_buckets = buckets
                .entry(labels)
                .or_insert_with(|| vec![MetricBucket::default(); bucket_count]);
            if let Some(bucket) = series_buckets.get_mut(idx) {
                bucket.record(value, Some(exemplar));
            }
        }
    }
    if buckets.is_empty() {
        buckets.insert(Vec::new(), vec![MetricBucket::default(); bucket_count]);
    }

    let step_seconds = f64_from_i64(step_ns) / 1_000_000_000.0;
    let series = buckets
        .into_iter()
        .map(|(labels, buckets)| {
            metric_series_for_group(
                labels,
                buckets,
                metric,
                output_start_ns,
                step_ns,
                step_seconds,
                max_exemplars,
            )
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(TraceMetricsResponse {
        series: apply_rank(apply_metric_filter(series, metric.filter), metric.rank),
    })
}

fn apply_metric_filter(
    series: Vec<TraceMetricSeries>,
    filter: Option<MetricFilter>,
) -> Vec<TraceMetricSeries> {
    let Some(filter) = filter else {
        return series;
    };
    series
        .into_iter()
        .filter_map(|mut series| {
            series
                .points
                .retain(|(_, value)| metric_filter_passes(*value, filter));
            series.exemplars.retain(|exemplar| {
                series
                    .points
                    .iter()
                    .any(|(ts, _)| *ts == exemplar.timestamp_ns)
            });
            if series.points.is_empty() {
                None
            } else {
                Some(series)
            }
        })
        .collect()
}

fn metric_filter_passes(value: f64, filter: MetricFilter) -> bool {
    let ordering = value.total_cmp(&filter.value);
    match filter.op {
        crate::ast::ComparisonOp::Eq => ordering.is_eq(),
        crate::ast::ComparisonOp::Neq => !ordering.is_eq(),
        crate::ast::ComparisonOp::Lt => ordering.is_lt(),
        crate::ast::ComparisonOp::Lte => !ordering.is_gt(),
        crate::ast::ComparisonOp::Gt => ordering.is_gt(),
        crate::ast::ComparisonOp::Gte => !ordering.is_lt(),
        crate::ast::ComparisonOp::Re | crate::ast::ComparisonOp::Nre => false,
    }
}

fn apply_rank(
    mut series: Vec<TraceMetricSeries>,
    rank: Option<RankLimit>,
) -> Vec<TraceMetricSeries> {
    let Some(rank) = rank else {
        return series;
    };
    series.sort_by(|a, b| {
        let a_score = series_rank_score(a);
        let b_score = series_rank_score(b);
        match rank.direction {
            RankDirection::Top => b_score
                .total_cmp(&a_score)
                .then_with(|| a.labels.cmp(&b.labels)),
            RankDirection::Bottom => a_score
                .total_cmp(&b_score)
                .then_with(|| a.labels.cmp(&b.labels)),
        }
    });
    series.truncate(rank.k);
    series
}

fn series_rank_score(series: &TraceMetricSeries) -> f64 {
    series.points.iter().map(|(_, value)| *value).sum()
}

fn metric_series_for_group(
    labels: Vec<(String, String)>,
    buckets: Vec<MetricBucket>,
    metric: &MetricPlan,
    start_ns: i64,
    step_ns: i64,
    step_seconds: f64,
    max_exemplars: usize,
) -> Result<Vec<TraceMetricSeries>> {
    let exemplars = metric_exemplars(&buckets, max_exemplars);
    if matches!(metric.function, MetricFunction::QuantileOverTime) {
        return metric
            .quantiles
            .iter()
            .map(|quantile| {
                let mut labels = labels.clone();
                labels.insert(0, ("p".into(), quantile_label(*quantile)));
                let points = buckets
                    .iter()
                    .enumerate()
                    .map(|(idx, bucket)| {
                        let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
                        Ok((ts, bucket.quantile(*quantile)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(TraceMetricSeries {
                    labels,
                    points,
                    exemplars: exemplars.clone(),
                })
            })
            .collect();
    }
    if matches!(metric.function, MetricFunction::HistogramOverTime) {
        return histogram_series_for_group(labels, &buckets, start_ns, step_ns, &exemplars);
    }

    let points = buckets
        .into_iter()
        .enumerate()
        .map(|(idx, bucket)| {
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            let value = match metric.function {
                MetricFunction::Rate => f64_from_u64(bucket.count)? / step_seconds,
                MetricFunction::CountOverTime => f64_from_u64(bucket.count)?,
                MetricFunction::SumOverTime => bucket.sum,
                MetricFunction::AvgOverTime => bucket.average()?,
                MetricFunction::MinOverTime => bucket.min.unwrap_or(0.0),
                MetricFunction::MaxOverTime => bucket.max.unwrap_or(0.0),
                MetricFunction::HistogramOverTime | MetricFunction::QuantileOverTime => {
                    unreachable!("handled above")
                }
            };
            Ok((ts, value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![TraceMetricSeries {
        labels,
        points,
        exemplars,
    }])
}

fn histogram_series_for_group(
    labels: Vec<(String, String)>,
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    exemplars: &[TraceMetricExemplar],
) -> Result<Vec<TraceMetricSeries>> {
    let mut out = Vec::with_capacity(DEFAULT_HISTOGRAM_BUCKETS_NS.len() + 3);
    for le in DEFAULT_HISTOGRAM_BUCKETS_NS {
        let mut labels = labels.clone();
        labels.insert(0, ("le".into(), quantile_label(*le)));
        out.push(TraceMetricSeries {
            labels,
            points: histogram_points(buckets, start_ns, step_ns, |bucket| {
                f64_from_usize(bucket.values.iter().filter(|value| **value <= *le).count())
            })?,
            exemplars: exemplars.to_owned(),
        });
    }

    let mut inf_labels = labels.clone();
    inf_labels.insert(0, ("le".into(), "+Inf".into()));
    out.push(TraceMetricSeries {
        labels: inf_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| {
            f64_from_u64(bucket.count)
        })?,
        exemplars: exemplars.to_owned(),
    });

    let mut sum_labels = labels.clone();
    sum_labels.insert(0, ("__metric__".into(), "sum".into()));
    out.push(TraceMetricSeries {
        labels: sum_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| Ok(bucket.sum))?,
        exemplars: Vec::new(),
    });

    let mut count_labels = labels;
    count_labels.insert(0, ("__metric__".into(), "count".into()));
    out.push(TraceMetricSeries {
        labels: count_labels,
        points: histogram_points(buckets, start_ns, step_ns, |bucket| {
            f64_from_u64(bucket.count)
        })?,
        exemplars: Vec::new(),
    });

    Ok(out)
}

fn histogram_points(
    buckets: &[MetricBucket],
    start_ns: i64,
    step_ns: i64,
    mut value: impl FnMut(&MetricBucket) -> Result<f64>,
) -> Result<Vec<(i64, f64)>> {
    buckets
        .iter()
        .enumerate()
        .map(|(idx, bucket)| {
            let ts = start_ns + i64::try_from(idx).unwrap_or(i64::MAX) * step_ns;
            Ok((ts, value(bucket)?))
        })
        .collect()
}

#[derive(Clone, Default)]
struct MetricBucket {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    values: Vec<f64>,
    exemplars: Vec<TraceMetricExemplar>,
}

impl MetricBucket {
    fn record(&mut self, value: Option<f64>, exemplar: Option<TraceMetricExemplar>) {
        self.count += 1;
        if let Some(exemplar) = exemplar
            && self.exemplars.is_empty()
        {
            self.exemplars.push(exemplar);
        }
        let Some(value) = value else {
            return;
        };
        self.sum += value;
        self.min = Some(self.min.map_or(value, |min| min.min(value)));
        self.max = Some(self.max.map_or(value, |max| max.max(value)));
        self.values.push(value);
    }

    fn average(&self) -> Result<f64> {
        if self.count == 0 {
            Ok(0.0)
        } else {
            Ok(self.sum / f64_from_u64(self.count)?)
        }
    }

    fn quantile(&self, quantile: f64) -> Result<f64> {
        if self.values.is_empty() {
            return Ok(0.0);
        }
        let mut values = self.values.clone();
        values.sort_by(f64::total_cmp);
        if values.len() == 1 {
            return Ok(values[0]);
        }
        let rank = quantile * f64_from_usize(values.len() - 1)?;
        let lower = usize_from_integer_f64(rank.floor())?;
        let upper = usize_from_integer_f64(rank.ceil())?;
        if lower == upper {
            Ok(values[lower])
        } else {
            Ok(values[lower] + (values[upper] - values[lower]) * (rank - f64_from_usize(lower)?))
        }
    }
}

fn metric_exemplar(
    batch: &RecordBatch,
    row: usize,
    timestamp_ns: i64,
    value: f64,
) -> Result<TraceMetricExemplar> {
    Ok(TraceMetricExemplar {
        labels: vec![
            (
                "trace_id".into(),
                bytes_to_hex(&fixed_16(batch, COL_TRACE_ID, row)?),
            ),
            (
                "span_id".into(),
                bytes_to_hex(&fixed_8(batch, COL_SPAN_ID, row)?),
            ),
        ],
        value,
        timestamp_ns,
    })
}

fn metric_exemplars(buckets: &[MetricBucket], max_exemplars: usize) -> Vec<TraceMetricExemplar> {
    buckets
        .iter()
        .flat_map(|bucket| bucket.exemplars.iter().cloned())
        .take(max_exemplars)
        .collect()
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn quantile_label(quantile: f64) -> String {
    let mut label = quantile.to_string();
    if label.contains('.') {
        while label.ends_with('0') {
            label.pop();
        }
        if label.ends_with('.') {
            label.push('0');
        }
    }
    label
}

fn metric_labels(
    batch: &RecordBatch,
    row: usize,
    fields: &[Field],
) -> Result<Vec<(String, String)>> {
    fields
        .iter()
        .map(|field| {
            let column = metric_field_column(field)?;
            let value = metric_label_value(batch, &column, row)?;
            Ok((metric_label_key(field), value))
        })
        .collect()
}

/// The series label key for a `by(<field>)` grouping, matching real Tempo: the
/// FULLY-SCOPED attribute name (e.g. `resource.service.name`, `span.http.method`),
/// not the scope-stripped key. Grafana's Traces Drilldown keys its per-attribute
/// breakdown panels on this exact name, so a stripped key (`service.name`) leaves
/// the breakdown blank even though the underlying data is correct (verified
/// against real Tempo by `tempo_differential`).
fn metric_label_key(field: &Field) -> String {
    let prefix = match &field.scope {
        Scope::Resource => "resource.",
        Scope::Span => "span.",
        Scope::Event => "event.",
        Scope::Link => "link.",
        Scope::Both => ".",
        Scope::Parent => "parent.",
        Scope::Instrumentation => "instrumentation.",
        // Intrinsics (name/status/kind/duration/trace:* …) are referenced by
        // their own names, not a scoped attribute key — keep the parser key.
        Scope::Intrinsic(_) => return field.key.clone(),
    };
    format!("{prefix}{}", field.key)
}

fn metric_field_column(field: &Field) -> Result<String> {
    match field.scope {
        Scope::Both | Scope::Resource if field.key == "service.name" => {
            Ok(COL_ROOT_SERVICE_NAME.to_string())
        }
        Scope::Both | Scope::Span | Scope::Resource => Ok(format!("{ATTR_PREFIX}{}", field.key)),
        Scope::Event => Ok(format!("{ATTR_PREFIX}{EVENT_ATTR_PREFIX}{}", field.key)),
        Scope::Link => Ok(format!("{ATTR_PREFIX}{LINK_ATTR_PREFIX}{}", field.key)),
        Scope::Intrinsic(Intrinsic::Name) => Ok(COL_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::Duration) => Ok(COL_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::Id) => Ok(COL_SPAN_ID.to_string()),
        Scope::Intrinsic(Intrinsic::ParentId) => Ok(COL_PARENT_SPAN_ID.to_string()),
        Scope::Intrinsic(Intrinsic::ChildCount) => Ok(COL_CHILD_COUNT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetLeft) => Ok(COL_NS_LEFT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetRight) => Ok(COL_NS_RIGHT.to_string()),
        Scope::Intrinsic(Intrinsic::NestedSetParent) => Ok(COL_PARENT_ID.to_string()),
        Scope::Intrinsic(Intrinsic::Kind) => Ok(COL_KIND.to_string()),
        Scope::Intrinsic(Intrinsic::Status) => Ok(COL_STATUS_CODE.to_string()),
        Scope::Intrinsic(Intrinsic::StatusMessage) => Ok(COL_STATUS_MESSAGE.to_string()),
        Scope::Intrinsic(Intrinsic::TraceId) => Ok(COL_TRACE_ID.to_string()),
        Scope::Intrinsic(Intrinsic::TraceDuration) => Ok(COL_TRACE_DURATION.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootService) => Ok(COL_ROOT_SERVICE_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::TraceRootName) => Ok(COL_ROOT_SPAN_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::InstrumentationName) => {
            Ok(COL_INSTRUMENTATION_NAME.to_string())
        }
        Scope::Intrinsic(Intrinsic::InstrumentationVersion) => {
            Ok(COL_INSTRUMENTATION_VERSION.to_string())
        }
        Scope::Intrinsic(Intrinsic::EventName) => Ok(COL_EVENT_NAME.to_string()),
        Scope::Intrinsic(Intrinsic::EventTimeSinceStart) => {
            Ok(COL_EVENT_TIME_SINCE_START.to_string())
        }
        Scope::Intrinsic(Intrinsic::LinkTraceId) => Ok(COL_LINK_TRACE_ID.to_string()),
        Scope::Intrinsic(Intrinsic::LinkSpanId) => Ok(COL_LINK_SPAN_ID.to_string()),
        _ => Err(TraceqlError::Unsupported(format!(
            "metrics by() field {field:?} is not supported yet"
        ))),
    }
}

fn metric_label_value(batch: &RecordBatch, column: &str, row: usize) -> Result<String> {
    let array = batch
        .column_by_name(column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(String::new());
    }
    match array.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            string_array_value(array.as_ref(), row)
                .ok_or_else(|| TraceqlError::Exec("unsupported string column type".into()))
        }
        DataType::Dictionary(_, value_type)
            if matches!(
                value_type.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) =>
        {
            string_array_value(array.as_ref(), row)
                .ok_or_else(|| TraceqlError::Exec("unsupported string column type".into()))
        }
        DataType::Int64 => Ok(array
            .as_primitive::<arrow::datatypes::Int64Type>()
            .value(row)
            .to_string()),
        DataType::Float64 => Ok(array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row)
            .to_string()),
        DataType::Boolean => Ok(array.as_boolean().value(row).to_string()),
        DataType::Int32 => Ok(array
            .as_primitive::<arrow::datatypes::Int32Type>()
            .value(row)
            .to_string()),
        DataType::FixedSizeBinary(_) => Ok(bytes_to_hex(array.as_fixed_size_binary().value(row))),
        other => Err(TraceqlError::Exec(format!(
            "unsupported metrics label column type {other:?}"
        ))),
    }
}

/// Extracts the numeric value of a metric fold field for one row.
///
/// Returns `Ok(None)` when the row's value field is NULL (the target attribute
/// is absent for that span), so callers can skip the span instead of folding a
/// spurious `0.0` into sum/min/max/avg/histogram.
fn metric_numeric_value(batch: &RecordBatch, row: usize, field: &Field) -> Result<Option<f64>> {
    let column = metric_field_column(field)?;
    let array = batch
        .column_by_name(&column)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {column}")))?;
    if array.is_null(row) {
        return Ok(None);
    }
    let value = match array.data_type() {
        DataType::Int64 => f64_from_i64(
            array
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(row),
        ),
        DataType::Int32 => f64::from(
            array
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row),
        ),
        DataType::Float64 => array
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(row),
        other => {
            return Err(TraceqlError::Unsupported(format!(
                "metrics fold field {field:?} has non-numeric type {other:?}"
            )));
        }
    };
    Ok(Some(value))
}

/// Converts an `i64` to the nearest `f64` without a per-row heap allocation.
///
/// The cast is round-to-nearest, identical to the previous
/// `to_string().parse()` path for every input; precision loss beyond 2^53 is
/// inherent to `f64` and unavoidable for either approach.
#[allow(clippy::cast_precision_loss)]
fn f64_from_i64(value: i64) -> f64 {
    value as f64
}

fn f64_from_u64(value: u64) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Exec(e.to_string()))
}

fn f64_from_usize(value: usize) -> Result<f64> {
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseFloatError| TraceqlError::Exec(e.to_string()))
}

fn usize_from_integer_f64(value: f64) -> Result<usize> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 {
        return Err(TraceqlError::Exec(format!(
            "expected non-negative integer float, got {value}"
        )));
    }
    value
        .to_string()
        .parse()
        .map_err(|e: std::num::ParseIntError| TraceqlError::Exec(e.to_string()))
}

struct TraceAcc {
    root_service_name: String,
    root_trace_name: String,
    start_time_unix_nano: u64,
    duration_nanos: u64,
    duration_ms: u64,
    spans: Vec<SpanRef>,
}

pub(crate) fn assemble_search_response(
    batches: &[RecordBatch],
    limit: usize,
    spss: usize,
    most_recent: bool,
    inspected_bytes: u64,
) -> Result<SearchResponse> {
    let mut traces: BTreeMap<[u8; 16], TraceAcc> = BTreeMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let trace_id = fixed_16(batch, COL_TRACE_ID, row)?;
            let span = SpanRef {
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
                parent_span_id: optional_fixed_8(batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(batch, COL_NAME, row).unwrap_or_default(),
                kind: i32_value(batch, COL_KIND, row)?,
                nested_set_left: i32_value(batch, COL_NS_LEFT, row)?,
                nested_set_right: i32_value(batch, COL_NS_RIGHT, row)?,
                nested_set_parent: i32_value(batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64_from_i64(i64_value(batch, COL_START, row)?)?,
                duration_nanos: u64_from_i64(i64_value(batch, COL_DURATION, row)?)?,
                status_code: i32_value(batch, COL_STATUS_CODE, row)?,
                status_message: string_value(batch, COL_STATUS_MESSAGE, row).unwrap_or_default(),
                instrumentation_name: string_value(batch, COL_INSTRUMENTATION_NAME, row)
                    .unwrap_or_default(),
                instrumentation_version: string_value(batch, COL_INSTRUMENTATION_VERSION, row)
                    .unwrap_or_default(),
                resource_attributes: Vec::new(),
                attributes: row_attrs(batch, row)?,
                events: Vec::new(),
                links: Vec::new(),
            };
            traces
                .entry(trace_id)
                .or_insert_with(|| {
                    let duration_nanos =
                        u64_from_i64(i64_value(batch, COL_TRACE_DURATION, row).unwrap_or_default())
                            .unwrap_or_default();

                    TraceAcc {
                        root_service_name: string_value(batch, COL_ROOT_SERVICE_NAME, row)
                            .unwrap_or_default(),
                        root_trace_name: string_value(batch, COL_ROOT_SPAN_NAME, row)
                            .unwrap_or_default(),
                        start_time_unix_nano: u64_from_i64(
                            i64_value(batch, COL_TRACE_START, row).unwrap_or_default(),
                        )
                        .unwrap_or_default(),
                        duration_nanos,
                        duration_ms: duration_nanos / 1_000_000,
                        spans: Vec::new(),
                    }
                })
                .spans
                .push(span);
        }
    }

    let mut out: Vec<TraceResult> = traces
        .into_iter()
        .map(|(trace_id, mut acc)| {
            deduplicate_search_spans(&mut acc.spans);
            let matched = u32::try_from(acc.spans.len()).unwrap_or(u32::MAX);
            let spans = acc.spans.into_iter().take(spss).collect();
            TraceResult {
                trace_id,
                root_service_name: acc.root_service_name,
                root_trace_name: acc.root_trace_name,
                start_time_unix_nano: acc.start_time_unix_nano,
                duration_nanos: acc.duration_nanos,
                duration_ms: acc.duration_ms,
                span_sets: vec![SpanSet { spans, matched }],
            }
        })
        .collect();
    let inspected_traces = out.len();
    if most_recent {
        out.sort_by(|a, b| {
            b.start_time_unix_nano
                .cmp(&a.start_time_unix_nano)
                .then_with(|| a.trace_id.cmp(&b.trace_id))
        });
    } else {
        out.sort_by_key(|t| (t.start_time_unix_nano, t.trace_id));
    }
    out.truncate(limit);
    Ok(SearchResponse {
        traces: out,
        inspected_traces,
        inspected_bytes,
    })
}

fn deduplicate_search_spans(spans: &mut Vec<SpanRef>) {
    spans.sort_by_key(|span| span.span_id);
    spans.dedup_by_key(|span| span.span_id);
    spans.sort_by_key(|span| (span.start_time_unix_nano, span.span_id));
}

fn fixed_16(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 16]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 16 bytes")))
}

fn fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<[u8; 8]> {
    batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_fixed_size_binary()
        .value(row)
        .try_into()
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

fn optional_fixed_8(batch: &RecordBatch, col: &str, row: usize) -> Result<Option<[u8; 8]>> {
    let arr = batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?;
    if arr.is_null(row) {
        return Ok(None);
    }
    arr.as_fixed_size_binary()
        .value(row)
        .try_into()
        .map(Some)
        .map_err(|_| TraceqlError::Exec(format!("column {col} is not 8 bytes")))
}

fn i64_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i64> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(row))
}

fn i32_value(batch: &RecordBatch, col: &str, row: usize) -> Result<i32> {
    Ok(batch
        .column_by_name(col)
        .ok_or_else(|| TraceqlError::Exec(format!("missing column {col}")))?
        .as_primitive::<arrow::datatypes::Int32Type>()
        .value(row))
}

fn string_value(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
    let arr = batch.column_by_name(col)?;
    if arr.is_null(row) {
        return None;
    }
    string_array_value(arr.as_ref(), row)
}

fn string_array_value(array: &dyn Array, row: usize) -> Option<String> {
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|arr| arr.value(row).to_string())
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .map(|arr| arr.value(row).to_string())
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .map(|arr| arr.value(row).to_string())
        })
        .or_else(|| {
            array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .and_then(|arr| {
                    let key = usize::try_from(arr.keys().value(row)).ok()?;
                    string_array_value(arr.values().as_ref(), key)
                })
        })
}

fn row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let schema = batch.schema();
    let mut attrs = Vec::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let Some(name) = field.name().strip_prefix(ATTR_PREFIX) else {
            continue;
        };
        let array = batch.column(idx);
        if array.is_null(row) {
            continue;
        }
        let value = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                AttrValue::Str(string_array_value(array.as_ref(), row).ok_or_else(|| {
                    TraceqlError::Exec(format!("unsupported string attribute column {name}"))
                })?)
            }
            DataType::Int64 => AttrValue::Int(
                array
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .value(row),
            ),
            DataType::Float64 => AttrValue::Float(
                array
                    .as_primitive::<arrow::datatypes::Float64Type>()
                    .value(row),
            ),
            DataType::Boolean => AttrValue::Bool(array.as_boolean().value(row)),
            other => {
                return Err(TraceqlError::Exec(format!(
                    "unsupported attribute column type {other:?}"
                )));
            }
        };
        attrs.push((name.to_string(), value));
    }
    attrs.extend(block_row_attrs(batch, row)?);
    attrs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(attrs)
}

fn block_row_attrs(batch: &RecordBatch, row: usize) -> Result<Vec<(String, AttrValue)>> {
    let Some(keys) = optional_list_column(batch, BLOCK_ATTR_KEYS)? else {
        return Ok(Vec::new());
    };
    if keys.is_null(row) {
        return Ok(Vec::new());
    }
    let key_values = keys.value(row);
    let key_values = key_values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Exec("attr_keys row is not Utf8".into()))?;
    let str_values = optional_list_column(batch, BLOCK_ATTR_VALUE)?;
    let int_values = optional_list_column(batch, BLOCK_ATTR_VALUE_INT)?;
    let double_values = optional_list_column(batch, BLOCK_ATTR_VALUE_DOUBLE)?;
    let bool_values = optional_list_column(batch, BLOCK_ATTR_VALUE_BOOL)?;

    let mut out = Vec::new();
    for attr_idx in 0..key_values.len() {
        if key_values.is_null(attr_idx) {
            continue;
        }
        let key = key_values.value(attr_idx);
        if key.starts_with(RESOURCE_ATTR_PREFIX) {
            continue;
        }
        out.extend(
            block_attr_values_for_key(
                str_values,
                int_values,
                double_values,
                bool_values,
                row,
                attr_idx,
            )?
            .into_iter()
            .map(|value| (key.to_string(), value)),
        );
    }
    Ok(out)
}

fn block_attr_values_for_key(
    str_values: Option<&ListArray>,
    int_values: Option<&ListArray>,
    double_values: Option<&ListArray>,
    bool_values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
) -> Result<Vec<AttrValue>> {
    let values = string_attr_values(str_values, row, attr_idx, BLOCK_ATTR_VALUE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Str).collect());
    }
    let values = i64_attr_values(int_values, row, attr_idx, BLOCK_ATTR_VALUE_INT)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Int).collect());
    }
    let values = f64_attr_values(double_values, row, attr_idx, BLOCK_ATTR_VALUE_DOUBLE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Float).collect());
    }
    Ok(
        bool_attr_values(bool_values, row, attr_idx, BLOCK_ATTR_VALUE_BOOL)?
            .into_iter()
            .map(AttrValue::Bool)
            .collect(),
    )
}

fn optional_list_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<Option<&'a ListArray>> {
    batch
        .column_by_name(name)
        .map(|col| {
            col.as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| TraceqlError::Exec(format!("nested column `{name}` is not a list")))
        })
        .transpose()
}

fn row_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Option<arrow::array::ArrayRef>> {
    let Some(values) = values else {
        return Ok(None);
    };
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            TraceqlError::Exec(format!("attribute column `{name}` row is not a list"))
        })?;
    if attr_idx >= row_values.len() || row_values.is_null(attr_idx) {
        return Ok(None);
    }
    Ok(Some(row_values.value(attr_idx)))
}

fn string_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<String>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Utf8")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx).to_string())
        .collect())
}

fn i64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<i64>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Int64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

fn f64_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<f64>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Float64")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

fn bool_attr_values(
    values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
    name: &str,
) -> Result<Vec<bool>> {
    let Some(values) = row_attr_values(values, row, attr_idx, name)? else {
        return Ok(Vec::new());
    };
    let values = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| TraceqlError::Exec(format!("attribute column `{name}` is not Boolean")))?;
    Ok((0..values.len())
        .filter(|idx| !values.is_null(*idx))
        .map(|idx| values.value(idx))
        .collect())
}

fn u64_from_i64(v: i64) -> Result<u64> {
    u64::try_from(v).map_err(|e| TraceqlError::Exec(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, FixedSizeBinaryBuilder, Int32Array, Int64Array, StringArray,
        StringDictionaryBuilder,
    };
    use arrow::datatypes::{Field as ArrowField, Int32Type, Schema};
    use assert2::assert;
    use datafusion::catalog::MemTable;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::in_memory::InMemorySpanStore;
    use crate::result::{AttrValue, EventRef, LinkRef, TypedValue};
    use crate::span_columns::InputSpan;
    use crate::store::ScanResult;

    fn sp(tid: u8, id: u8, parent: Option<u8>, svc: &str) -> InputSpan {
        sp_at(tid, id, parent, svc, 1000 + i64::from(id))
    }

    fn sp_at(tid: u8, id: u8, parent: Option<u8>, svc: &str, start_unix_nano: i64) -> InputSpan {
        InputSpan {
            trace_id: [tid; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("op-{id}"),
            kind: 0,
            start_unix_nano,
            duration_nanos: 200,
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: String::new(),
            attrs: vec![("svc".into(), AttrValue::Str(svc.into()))],
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn engine() -> TraceqlEngine<InMemorySpanStore> {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![sp(9, 1, None, "a"), sp(9, 2, Some(1), "b")],
        );
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x")]);
        TraceqlEngine::new(Arc::new(s), EngineOpts::default())
    }

    struct BatchSpanStore {
        batch: RecordBatch,
    }

    #[async_trait::async_trait]
    impl SpanStore for BatchSpanStore {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[SpanMatcher],
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<ScanResult> {
            let schema = self.batch.schema();
            let ctx = SessionContext::new();
            let inspected_bytes =
                u64::try_from(self.batch.get_array_memory_size()).unwrap_or(u64::MAX);
            let table = MemTable::try_new(schema, vec![vec![self.batch.clone()]])?;
            ctx.register_table("spans", Arc::new(table))?;
            Ok(ScanResult {
                ctx,
                span_table: "spans".into(),
                inspected_bytes,
            })
        }

        async fn trace_by_id(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>> {
            Ok(None)
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>> {
            Ok(Vec::new())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>> {
            Ok(Vec::new())
        }
    }

    fn dictionary_metric_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            ArrowField::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            ArrowField::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
            ArrowField::new(COL_NS_LEFT, DataType::Int32, false),
            ArrowField::new(COL_NS_RIGHT, DataType::Int32, false),
            ArrowField::new(COL_PARENT_ID, DataType::Int32, false),
            ArrowField::new(COL_CHILD_COUNT, DataType::Int32, false),
            ArrowField::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            ArrowField::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
            ArrowField::new(COL_TRACE_START, DataType::Int64, false),
            ArrowField::new(COL_TRACE_DURATION, DataType::Int64, false),
            ArrowField::new(COL_NAME, DataType::Utf8, true),
            ArrowField::new(COL_KIND, DataType::Int32, false),
            ArrowField::new(COL_START, DataType::Int64, false),
            ArrowField::new(COL_DURATION, DataType::Int64, false),
            ArrowField::new(COL_STATUS_CODE, DataType::Int32, false),
            ArrowField::new(COL_STATUS_MESSAGE, DataType::Utf8, true),
            ArrowField::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
            ArrowField::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
            ArrowField::new(
                format!("{ATTR_PREFIX}http.method"),
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(2, 16);
        trace_id.append_value([1; 16]).unwrap();
        trace_id.append_value([2; 16]).unwrap();
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        span_id.append_value([1; 8]).unwrap();
        span_id.append_value([2; 8]).unwrap();
        let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(2, 8);
        parent_span_id.append_null();
        parent_span_id.append_null();
        let mut methods = StringDictionaryBuilder::<Int32Type>::new();
        methods.append_value("GET");
        methods.append_value("POST");

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(parent_span_id.finish()),
                Arc::new(Int32Array::from(vec![1, 1])),
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["api", "api"])),
                Arc::new(StringArray::from(vec!["GET /", "POST /"])),
                Arc::new(Int64Array::from(vec![0, 0])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(StringArray::from(vec!["GET /", "POST /"])),
                Arc::new(Int32Array::from(vec![2, 2])),
                Arc::new(Int64Array::from(vec![0, 10_000])),
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![0, 0])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(StringArray::from(vec!["tracer", "tracer"])),
                Arc::new(StringArray::from(vec!["", ""])),
                Arc::new(methods.finish()),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn search_selector_returns_matching_trace() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].root_service_name == "a");
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_match_all_selectors_return_every_trace() {
        // Grafana's Tempo Explore "Search" tab and TraceQL-metrics default to the
        // empty spanset `{}`; `{ true }` is the equivalent constant-true filter.
        // Both must match every span (not error, not match-none). `{ false }`
        // matches nothing. The fixture holds two traces (3 spans total).
        let e = engine();
        for q in ["{}", "{ true }", "{true}"] {
            let r = e.search("t", q, 0, 100_000, 20).await.unwrap();
            assert!(r.traces.len() == 2, "query {q:?} should match both traces");
        }
        let none = e.search("t", "{ false }", 0, 100_000, 20).await.unwrap();
        assert!(none.traces.is_empty(), "{{ false }} should match no traces");
    }

    #[tokio::test]
    async fn search_reports_inspected_bytes() {
        // The scan's decoded data size is threaded up to inspected_bytes (non-zero
        // for a non-empty store) for the Tempo search `metrics.inspectedBytes`.
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.inspected_bytes > 0);
    }

    #[tokio::test]
    async fn search_deduplicates_same_span_returned_by_multiple_tiers() {
        let mut s = InMemorySpanStore::new();
        let span = sp(9, 1, None, "a");
        s.push_trace("t", "a", "root", vec![span.clone(), span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ .svc = \"a\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans.len() == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_most_recent_hint_returns_newest_traces_first() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "old", "root", vec![sp_at(1, 1, None, "match", 1_000)]);
        s.push_trace("t", "new", "root", vec![sp_at(2, 1, None, "match", 10_000)]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ .svc = \"match\" } with (most_recent=true)",
                0,
                100_000,
                1,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [2; 16]);
    }

    #[tokio::test]
    async fn search_pipeline_with_preserves_matched_spans() {
        let e = engine();
        let r = e
            .search(
                "t",
                "{ .svc = \"b\" } | with(is_error = span:status = error)",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_inter_brace_and_matches_different_spans() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"a\" } && { .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 2);
    }

    #[tokio::test]
    async fn search_inter_brace_and_keeps_nested_selector_predicate() {
        let mut event_span = sp(9, 1, None, "a");
        event_span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let peer = sp(9, 2, Some(1), "b");
        let unrelated = sp(8, 1, None, "b");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![event_span, peer]);
        s.push_trace("t", "b", "root", vec![unrelated]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ event:name = \"cache.miss\" } && { .svc = \"b\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 2);
        let spans = &r.traces[0].span_sets[0].spans;
        assert!(spans.iter().any(|span| span.span_id == [1; 8]));
        assert!(spans.iter().any(|span| span.span_id == [2; 8]));
    }

    #[tokio::test]
    async fn search_descendant_structural() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"a\" } >> { .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn structural_operators_return_right_hand_spans() {
        let e = engine();
        let r = e
            .search("t", "{ .svc = \"a\" } >> { .svc = \"b\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_child_count_intrinsic() {
        let e = engine();
        let r = e
            .search("t", "{ span:childCount = 1 }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_scopeless_nested_set_parent_matches_scoped_and_finds_roots() {
        // Grafana's Traces Drilldown selects root spans with the scopeless
        // primary signal `nestedSetParent < 0`. This must (a) parse as the
        // intrinsic rather than `attr.nestedSetParent`, and (b) actually match
        // roots, whose sentinel is -1. It is equivalent to the scoped form and
        // returns at least one trace (every trace has a root).
        let e = engine();
        let scopeless = e
            .search("t", "{ nestedSetParent < 0 }", 0, 100_000, 20)
            .await
            .unwrap();
        let scoped = e
            .search("t", "{ span:nestedSetParent < 0 }", 0, 100_000, 20)
            .await
            .unwrap();
        let mut scopeless_ids: Vec<_> = scopeless.traces.iter().map(|t| t.trace_id).collect();
        let mut scoped_ids: Vec<_> = scoped.traces.iter().map(|t| t.trace_id).collect();
        scopeless_ids.sort_unstable();
        scoped_ids.sort_unstable();
        assert!(
            !scopeless_ids.is_empty(),
            "roots (nestedSetParent < 0) must exist"
        );
        assert!(scopeless_ids == scoped_ids);
    }

    #[tokio::test]
    async fn search_selector_matches_instrumentation_name_intrinsic() {
        let e = engine();
        let r = e
            .search("t", "{ instrumentation:name = \"tracer\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 2);
        let first = r
            .traces
            .iter()
            .find(|trace| trace.trace_id == [9; 16])
            .unwrap();
        let second = r
            .traces
            .iter()
            .find(|trace| trace.trace_id == [8; 16])
            .unwrap();
        assert!(first.span_sets[0].matched == 2);
        assert!(second.span_sets[0].matched == 1);
    }

    #[tokio::test]
    async fn search_selector_matches_resource_service_name() {
        let e = engine();
        let r = e
            .search("t", "{ resource.service.name = \"a\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].root_service_name == "a");
    }

    #[tokio::test]
    async fn search_selector_matches_trace_id_hex_string() {
        let e = engine();
        let r = e
            .search(
                "t",
                "{ trace:id = \"09090909090909090909090909090909\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 2);
    }

    #[tokio::test]
    async fn search_selector_matches_span_id_hex_string() {
        let e = engine();
        let r = e
            .search("t", "{ span:id = \"0202020202020202\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_parent_id_hex_string() {
        let e = engine();
        let r = e
            .search(
                "t",
                "{ span:parentID = \"0101010101010101\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_event_intrinsic() {
        let mut span = sp(9, 1, None, "a");
        span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![span, sp(8, 1, None, "x")]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ event:name = \"cache.miss\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_event_intrinsic_presence() {
        let mut event_span = sp(9, 1, None, "a");
        event_span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let peer = sp(9, 2, Some(1), "b");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![event_span, peer]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ event:name != nil }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_selector_not_event_intrinsic_excludes_matching_spans() {
        let mut event_span = sp(9, 1, None, "a");
        event_span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let peer = sp(9, 2, Some(1), "b");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![event_span, peer]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ !event:name = \"cache.miss\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_grouped_not_event_intrinsic() {
        let mut event_span = sp(9, 1, None, "a");
        event_span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let peer = sp(9, 2, Some(1), "b");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![event_span, peer]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ !(event:name = \"cache.miss\") }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_not_nested_or_excludes_each_branch() {
        let mut miss_span = sp(9, 1, None, "a");
        miss_span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit_span = sp(9, 2, Some(1), "b");
        hit_span.events = vec![EventRef {
            time_since_start_nano: 60,
            name: "cache.hit".into(),
            attributes: Vec::new(),
        }];
        let peer = sp(9, 3, Some(1), "c");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![miss_span, hit_span, peer]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ !(event:name = \"cache.miss\" || event:name = \"cache.hit\") }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [3; 8]);
    }

    #[tokio::test]
    async fn search_selector_not_nested_and_uses_disjuncts() {
        let mut miss_users = sp(9, 1, None, "a");
        miss_users.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
        }];
        let mut miss_orders = sp(9, 2, Some(1), "b");
        miss_orders.events = vec![EventRef {
            time_since_start_nano: 60,
            name: "cache.miss".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("orders".into()))],
        }];
        let mut hit_users = sp(9, 3, Some(1), "c");
        hit_users.events = vec![EventRef {
            time_since_start_nano: 70,
            name: "cache.hit".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![miss_users, miss_orders, hit_users]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ !(event:name = \"cache.miss\" && event.cache.key = \"users\") }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 2);
        let spans = &r.traces[0].span_sets[0].spans;
        assert!(!spans.iter().any(|span| span.span_id == [1; 8]));
        assert!(spans.iter().any(|span| span.span_id == [2; 8]));
        assert!(spans.iter().any(|span| span.span_id == [3; 8]));
    }

    #[tokio::test]
    async fn search_selector_requires_event_matchers_on_same_event() {
        let mut split_events = sp(9, 1, None, "a");
        split_events.events = vec![
            EventRef {
                time_since_start_nano: 50,
                name: "cache.miss".into(),
                attributes: vec![("cache.key".into(), AttrValue::Str("orders".into()))],
            },
            EventRef {
                time_since_start_nano: 60,
                name: "cache.hit".into(),
                attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
            },
        ];
        let mut same_event = sp(9, 2, Some(1), "b");
        same_event.events = vec![EventRef {
            time_since_start_nano: 70,
            name: "cache.miss".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![split_events, same_event]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ event:name = \"cache.miss\" && event.cache.key = \"users\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_or_with_nested_event_filters_each_branch() {
        let mut event_span = sp(9, 1, None, "a");
        event_span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let attr_span = sp(9, 2, Some(1), "b");
        let unrelated = sp(9, 3, Some(1), "c");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![event_span, attr_span, unrelated]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ event:name = \"cache.miss\" || .svc = \"b\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 2);
        let spans = &r.traces[0].span_sets[0].spans;
        assert!(spans.iter().any(|span| span.span_id == [1; 8]));
        assert!(spans.iter().any(|span| span.span_id == [2; 8]));
        assert!(!spans.iter().any(|span| span.span_id == [3; 8]));
    }

    #[tokio::test]
    async fn search_selector_applies_array_any_none_semantics_to_repeated_attrs() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    attrs: vec![
                        ("http.method".into(), AttrValue::Str("GET".into())),
                        ("http.method".into(), AttrValue::Str("POST".into())),
                    ],
                    ..sp(9, 1, None, "a")
                },
                InputSpan {
                    attrs: vec![("http.method".into(), AttrValue::Str("DELETE".into()))],
                    ..sp(9, 2, Some(1), "b")
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ span.http.method = \"POST\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);

        let r = e
            .search("t", "{ span.http.method != \"POST\" }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_link_attribute_scope() {
        let mut span = sp(9, 1, None, "a");
        span.links = vec![LinkRef {
            trace_id: [7; 16],
            span_id: [6; 8],
            attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![span, sp(8, 1, None, "x")]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ link.link.kind = \"retry\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_nested_set_parent_intrinsic() {
        let e = engine();
        let r = e
            .search("t", "{ span:nestedSetParent = 1 }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_status_enum_value() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    status_code: 2,
                    ..sp(9, 1, None, "a")
                },
                sp(9, 2, Some(1), "b"),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let r = e
            .search("t", "{ span:status = error }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_selector_matches_kind_enum_value() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    kind: 2,
                    ..sp(9, 1, None, "a")
                },
                sp(9, 2, Some(1), "b"),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let r = e
            .search("t", "{ span:kind = server }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn bare_service_name_selector_matches_resource_service_name() {
        let e = engine();
        let r = e
            .search("t", "{ .service.name = \"a\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].root_service_name == "a");
    }

    #[tokio::test]
    async fn parent_scope_selector_matches_direct_parent_attributes() {
        let e = engine();
        let r = e
            .search("t", "{ parent.svc = \"a\" }", 0, 100_000, 20)
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn parent_scope_selector_works_inside_trace_level_and() {
        let e = engine();
        let r = e
            .search(
                "t",
                "{ parent.svc = \"a\" } && { .svc = \"b\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn mixed_parent_and_event_selector_keeps_parent_predicate() {
        let mut wanted = sp(9, 2, Some(1), "b");
        wanted.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut wrong_parent = sp(8, 2, Some(1), "b");
        wrong_parent.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp(9, 1, None, "a"), wanted]);
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x"), wrong_parent]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ parent.svc = \"a\" && event:name = \"cache.miss\" }",
                0,
                100_000,
                20,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [9; 16]);
        assert!(r.traces[0].span_sets[0].matched == 1);
        assert!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_limit_uses_default_for_zero_and_caps_result_count() {
        let e = engine();
        let r = e
            .search("t", "{ .svc != nil }", 0, 100_000, 1)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
    }

    #[tokio::test]
    async fn trace_by_id_path() {
        let e = engine();
        let got = e.trace_by_id("t", &[9; 16]).await.unwrap().unwrap();
        assert!(got.spans.len() == 2);
        assert!(e.trace_by_id("t", &[1; 16]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn trace_by_id_within_returns_spans_in_window() {
        // Delegates to the store; must surface the real trace, not Ok(None).
        let e = engine();
        let got = e
            .trace_by_id_within("t", &[9; 16], 0, 100_000)
            .await
            .unwrap()
            .unwrap();
        assert!(got.spans.len() == 2);
        // A window after the trace retains no spans (but still returns the trace).
        let out = e
            .trace_by_id_within("t", &[9; 16], 100_000, 200_000)
            .await
            .unwrap()
            .unwrap();
        assert!(out.spans.is_empty());
    }

    #[tokio::test]
    async fn tag_names_and_values_delegate_to_store() {
        // Both must surface the store's non-empty results, not Ok(vec![]).
        let e = engine();
        let names = e.tag_names("t", None, 0, 100_000).await.unwrap();
        assert!(!names.is_empty());
        assert!(
            names
                .iter()
                .any(|scoped| scoped.tags.iter().any(|t| t == "svc"))
        );

        let values = e.tag_values("t", ".svc", 0, 100_000).await.unwrap();
        assert!(
            values
                == vec![
                    TypedValue {
                        type_: "string".into(),
                        value: "a".into(),
                    },
                    TypedValue {
                        type_: "string".into(),
                        value: "b".into(),
                    },
                    TypedValue {
                        type_: "string".into(),
                        value: "x".into(),
                    },
                ]
        );
    }

    #[tokio::test]
    async fn count_over_time_counts_matched_spans_per_bucket() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "a", 0),
                sp_at(1, 2, None, "a", 10_000),
                sp_at(1, 3, None, "a", 60_000),
                sp_at(1, 4, None, "b", 70_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc = \"a\" } | count_over_time()",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(got.series[0].points == vec![(0, 2.0), (60_000, 1.0), (120_000, 0.0)]);
    }

    #[tokio::test]
    async fn rate_divides_bucket_count_by_step_seconds() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "a", 0),
                sp_at(1, 2, None, "a", 10_000),
                sp_at(1, 3, None, "a", 20_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc = \"a\" } | rate()",
                0,
                10_000_000_000,
                10_000_000_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(got.series[0].points == vec![(0, 0.3), (10_000_000_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_attribute_emits_one_series_per_group() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "db", 70_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(got[0].points == vec![(0, 2.0), (60_000, 0.0), (120_000, 0.0)]);
        assert!(got[1].labels == vec![("span.svc".into(), "db".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 1.0), (120_000, 0.0)]);
    }

    #[tokio::test]
    async fn metric_comparison_filter_keeps_only_passing_samples() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "db", 70_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) > 1",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(got.len() == 1);
        assert!(got[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(got[0].points == vec![(0, 2.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_resource_service_name_uses_root_service_column() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![sp_at(1, 1, None, "api", 0)]);
        s.push_trace(
            "t",
            "billing",
            "root",
            vec![sp_at(2, 1, None, "api", 10_000)],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(resource.service.name)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("resource.service.name".into(), "billing".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("resource.service.name".into(), "checkout".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_dictionary_promoted_attr_decodes_labels() {
        let store = BatchSpanStore {
            batch: dictionary_metric_batch(),
        };
        let e = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ span:name != nil } | count_over_time() | by(span.http.method)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("span.http.method".into(), "GET".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("span.http.method".into(), "POST".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_event_name_intrinsic() {
        let mut miss = sp_at(1, 1, None, "api", 0);
        miss.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit = sp_at(1, 2, None, "api", 10_000);
        hit.events = vec![EventRef {
            time_since_start_nano: 60,
            name: "cache.hit".into(),
            attributes: Vec::new(),
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![miss, hit]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ event:name != nil } | count_over_time() | by(event:name)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("name".into(), "cache.hit".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("name".into(), "cache.miss".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_event_name_counts_each_event_on_a_span() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.events = vec![
            EventRef {
                time_since_start_nano: 50,
                name: "cache.miss".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start_nano: 60,
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ event:name != nil } | count_over_time() | by(event:name)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("name".into(), "cache.hit".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("name".into(), "cache.miss".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_event_attribute_counts_each_event_attribute() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.events = vec![
            EventRef {
                time_since_start_nano: 50,
                name: "cache.lookup".into(),
                attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
            },
            EventRef {
                time_since_start_nano: 60,
                name: "cache.lookup".into(),
                attributes: vec![("cache.key".into(), AttrValue::Str("orders".into()))],
            },
        ];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(event.cache.key)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("event.cache.key".into(), "orders".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("event.cache.key".into(), "users".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_link_trace_id_intrinsic_uses_hex_label() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: Vec::new(),
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ link:traceID != nil } | count_over_time() | by(link:traceID)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(got.len() == 1);
        assert!(
            got[0].labels == vec![("traceID".into(), "09090909090909090909090909090909".into())]
        );
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_link_span_id_counts_each_link_without_link_selector() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.links = vec![
            LinkRef {
                trace_id: [9; 16],
                span_id: [8; 8],
                attributes: Vec::new(),
            },
            LinkRef {
                trace_id: [7; 16],
                span_id: [6; 8],
                attributes: Vec::new(),
            },
        ];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(link:spanID)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("spanID".into(), "0606060606060606".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("spanID".into(), "0808080808080808".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn inert_stage_before_metric_aggregate_is_ignored() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc != nil } | select(span.svc) | count_over_time() | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(got[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("span.svc".into(), "db".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_kind_and_status_intrinsics() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    kind: 2,
                    status_code: 0,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    kind: 2,
                    status_code: 2,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    kind: 3,
                    status_code: 2,
                    ..sp_at(1, 3, None, "api", 20_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(span:kind, span:status)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 3);
        assert!(got[0].labels == vec![("kind".into(), "2".into()), ("status".into(), "0".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("kind".into(), "2".into()), ("status".into(), "2".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[2].labels == vec![("kind".into(), "3".into()), ("status".into(), "2".into())]);
        assert!(got[2].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_status_message_intrinsic() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    status_message: "timeout".into(),
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    status_message: "timeout".into(),
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    status_message: "cancelled".into(),
                    ..sp_at(1, 3, None, "api", 20_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(span:statusMessage)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("statusMessage".into(), "cancelled".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("statusMessage".into(), "timeout".into())]);
        assert!(got[1].points == vec![(0, 2.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_trace_id_intrinsic_uses_hex_label() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(0x11, 1, None, "api", 0)]);
        s.push_trace("t", "b", "root", vec![sp_at(0x22, 1, None, "api", 10_000)]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(trace:id)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("id".into(), "11111111111111111111111111111111".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("id".into(), "22222222222222222222222222222222".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_child_count_intrinsic() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, Some(1), "api", 10_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(span:childCount)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        assert!(got[0].labels == vec![("childCount".into(), "0".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("childCount".into(), "1".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_instrumentation_name_intrinsic() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(1, 1, None, "api", 0)]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(instrumentation:name)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(got.len() == 1);
        assert!(got[0].labels == vec![("name".into(), "tracer".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_by_nested_set_parent_intrinsic() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, Some(1), "api", 10_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(span:nestedSetParent)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        got.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(got.len() == 2);
        // Root span groups under nestedSetParent = -1 (Tempo root sentinel);
        // "-1" sorts before "1".
        assert!(got[0].labels == vec![("nestedSetParent".into(), "-1".into())]);
        assert!(got[0].points == vec![(0, 1.0), (60_000, 0.0)]);
        assert!(got[1].labels == vec![("nestedSetParent".into(), "1".into())]);
        assert!(got[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn avg_and_sum_over_time_fold_duration_per_bucket() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 100,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 300,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    duration_nanos: 50,
                    ..sp_at(1, 3, None, "api", 70_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let avg = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | avg_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(avg.series[0].points == vec![(0, 200.0), (60_000, 50.0), (120_000, 0.0)]);

        let sum = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | sum_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(sum.series[0].points == vec![(0, 400.0), (60_000, 50.0), (120_000, 0.0)]);

        let min = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | min_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(min.series[0].points == vec![(0, 100.0), (60_000, 50.0), (120_000, 0.0)]);

        let max = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | max_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(max.series[0].points == vec![(0, 300.0), (60_000, 50.0), (120_000, 0.0)]);
    }

    #[tokio::test]
    async fn sum_over_time_can_fold_trace_duration_intrinsic() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 100,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 300,
                    ..sp_at(1, 2, None, "api", 50)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | sum_over_time(trace:duration)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series[0].points == vec![(0, 700.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn quantile_over_time_emits_per_quantile_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 100,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 200,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    duration_nanos: 300,
                    ..sp_at(1, 3, None, "api", 20_000)
                },
                InputSpan {
                    duration_nanos: 400,
                    ..sp_at(1, 4, None, "api", 30_000)
                },
                InputSpan {
                    duration_nanos: 500,
                    ..sp_at(1, 5, None, "api", 40_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | quantile_over_time(span:duration, .5, .9) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(series.len() == 2);
        assert!(
            series[0].labels
                == vec![
                    ("p".into(), "0.5".into()),
                    ("span.svc".into(), "api".into())
                ]
        );
        assert!(series[0].points == vec![(0, 300.0), (60_000, 0.0)]);
        assert!(
            series[1].labels
                == vec![
                    ("p".into(), "0.9".into()),
                    ("span.svc".into(), "api".into())
                ]
        );
        assert!(series[1].points == vec![(0, 460.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn histogram_over_time_emits_cumulative_buckets_sum_and_count() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                InputSpan {
                    duration_nanos: 1_000_000,
                    ..sp_at(1, 1, None, "api", 0)
                },
                InputSpan {
                    duration_nanos: 2_000_000_000,
                    ..sp_at(1, 2, None, "api", 10_000)
                },
                InputSpan {
                    duration_nanos: 12_000_000_000,
                    ..sp_at(1, 3, None, "api", 20_000)
                },
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | histogram_over_time(span:duration) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("le".into(), "2000000".into()),
                    ("span.svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 1.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("le".into(), "2048000000".into()),
                    ("span.svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 2.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("le".into(), "+Inf".into()),
                    ("span.svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 3.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("__metric__".into(), "sum".into()),
                    ("span.svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 14_001_000_000.0), (60_000, 0.0)]
        }));
        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("__metric__".into(), "count".into()),
                    ("span.svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 3.0), (60_000, 0.0)]
        }));
    }

    #[tokio::test]
    async fn histogram_over_time_without_field_defaults_to_span_duration() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![InputSpan {
                duration_nanos: 1_000_000,
                ..sp_at(1, 1, None, "api", 0)
            }],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | histogram_over_time() | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    ("le".into(), "2000000".into()),
                    ("span.svc".into(), "api".into()),
                ]
                && s.points == vec![(0, 1.0), (60_000, 0.0)]
        }));
    }

    #[tokio::test]
    async fn topk_and_bottomk_rank_grouped_metric_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "worker", 30_000),
                sp_at(1, 5, None, "worker", 40_000),
                sp_at(1, 6, None, "worker", 50_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let mut top = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) | topk(2)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        top.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(top.len() == 2);
        assert!(top[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(top[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(top[1].labels == vec![("span.svc".into(), "worker".into())]);
        assert!(top[1].points == vec![(0, 3.0), (60_000, 0.0)]);

        let bottom = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) | bottomk(1)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(bottom.series.len() == 1);
        assert!(bottom.series[0].labels == vec![("span.svc".into(), "db".into())]);
        assert!(bottom.series[0].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn topk_by_ranks_grouped_metric_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "worker", 30_000),
                sp_at(1, 5, None, "worker", 40_000),
                sp_at(1, 6, None, "worker", 50_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let mut top = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | topk(2) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        top.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(top.len() == 2);
        assert!(top[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(top[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(top[1].labels == vec![("span.svc".into(), "worker".into())]);
        assert!(top[1].points == vec![(0, 3.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn by_before_count_over_time_groups_metric_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let mut series = e
            .query_range(
                "t",
                "{ .svc != nil } | by(span.svc) | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        series.sort_by(|a, b| a.labels.cmp(&b.labels));

        assert!(series.len() == 2);
        assert!(series[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(series[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(series[1].labels == vec![("span.svc".into(), "db".into())]);
        assert!(series[1].points == vec![(0, 1.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn by_before_count_over_time_supports_ranked_metric_series() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 0),
                sp_at(1, 2, None, "api", 10_000),
                sp_at(1, 3, None, "db", 20_000),
                sp_at(1, 4, None, "worker", 30_000),
                sp_at(1, 5, None, "worker", 40_000),
                sp_at(1, 6, None, "worker", 50_000),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let mut top = e
            .query_range(
                "t",
                "{ .svc != nil } | by(span.svc) | count_over_time() | topk(2)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        top.sort_by(|a, b| a.labels.cmp(&b.labels));

        assert!(top.len() == 2);
        assert!(top[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(top[0].points == vec![(0, 2.0), (60_000, 0.0)]);
        assert!(top[1].labels == vec![("span.svc".into(), "worker".into())]);
        assert!(top[1].points == vec![(0, 3.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn count_over_time_carries_trace_id_exemplars() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(0x11, 0x22, None, "api", 0),
                sp_at(0x11, 0x33, None, "api", 10_000),
            ],
        );
        let e = TraceqlEngine::new(
            Arc::new(s),
            EngineOpts {
                max_exemplars: 1,
                ..EngineOpts::default()
            },
        );
        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(got.series[0].exemplars.len() == 1);
        assert!(
            got.series[0].exemplars[0].labels
                == vec![
                    ("trace_id".into(), "11111111111111111111111111111111".into()),
                    ("span_id".into(), "2222222222222222".into())
                ]
        );
        assert!(got.series[0].exemplars[0].timestamp_ns == 0);
        assert!((got.series[0].exemplars[0].value - 1.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn metric_comparison_filter_removes_exemplars_for_filtered_samples() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(0x11, 0x22, None, "api", 0),
                sp_at(0x11, 0x33, None, "api", 10_000),
                sp_at(0x44, 0x55, None, "api", 60_000),
            ],
        );
        let e = TraceqlEngine::new(
            Arc::new(s),
            EngineOpts {
                max_exemplars: 10,
                ..EngineOpts::default()
            },
        );
        let got = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) > 1",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series.len() == 1);
        assert!(got.series[0].labels == vec![("span.svc".into(), "api".into())]);
        assert!(got.series[0].points == vec![(0, 2.0)]);
        assert!(got.series[0].exemplars.len() == 1);
        assert!(got.series[0].exemplars[0].timestamp_ns == 0);
        assert!(
            got.series[0].exemplars[0].labels
                == vec![
                    ("trace_id".into(), "11111111111111111111111111111111".into()),
                    ("span_id".into(), "2222222222222222".into())
                ]
        );
    }

    #[tokio::test]
    async fn default_options_disable_traceql_metric_exemplars() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(0x11, 0x22, None, "api", 0)]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series.len() == 1);
        assert!(got.series[0].exemplars.is_empty());
    }

    #[tokio::test]
    async fn query_hint_can_disable_traceql_metric_exemplars() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(0x11, 0x22, None, "api", 0)]);
        let e = TraceqlEngine::new(
            Arc::new(s),
            EngineOpts {
                max_exemplars: 1,
                ..EngineOpts::default()
            },
        );

        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() with (exemplars=false)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series.len() == 1);
        assert!(got.series[0].exemplars.is_empty());
    }

    #[test]
    fn f64_from_i64_matches_decimal_string_conversion() {
        // The direct conversion must be numerically identical to the previous
        // `to_string().parse()` path for representative i64 values, including a
        // large magnitude where float rounding matters.
        for value in [
            0_i64,
            1,
            -1,
            42,
            i64::MAX,
            i64::MIN,
            9_007_199_254_740_993, // 2^53 + 1, not exactly representable in f64
        ] {
            let direct = f64_from_i64(value);
            let via_string: f64 = value.to_string().parse().unwrap();
            assert!(direct.to_bits() == via_string.to_bits());
        }
    }

    fn sp_with_code(id: u8, start: i64, code: Option<i64>) -> InputSpan {
        let mut attrs = vec![("svc".into(), AttrValue::Str("api".into()))];
        if let Some(code) = code {
            attrs.push(("code".into(), AttrValue::Int(code)));
        }
        InputSpan {
            attrs,
            ..sp_at(1, id, None, "api", start)
        }
    }

    #[tokio::test]
    async fn absent_metric_attribute_does_not_pollute_min_and_avg() {
        // Spans whose target attribute is ABSENT must not contribute a 0 to
        // min/avg/max over the value field.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_with_code(1, 0, Some(10)),
                sp_with_code(2, 10_000, Some(30)),
                sp_with_code(3, 20_000, None),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let min = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | min_over_time(.code)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        // min over the present values {10, 30} = 10, not dragged to 0.
        assert!(min.series[0].points == vec![(0, 10.0), (60_000, 0.0)]);

        let avg = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | avg_over_time(.code)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        // avg over {10, 30} = 20, not (10+30+0)/3 = 13.33.
        assert!(avg.series[0].points == vec![(0, 20.0), (60_000, 0.0)]);

        let max = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | max_over_time(.code)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(max.series[0].points == vec![(0, 30.0), (60_000, 0.0)]);
    }

    #[tokio::test]
    async fn search_response_exposes_exact_span_scalars_and_attrs() {
        // A single trace with two spans carrying distinct, non-uniform scalar
        // values so that trivial replacements (None / Some([0;8]) / Some([1;8]) /
        // i32 0/1/-1) and the `/ 1_000_000` duration_ms division are all
        // observable.
        let root = InputSpan {
            trace_id: [9; 16],
            span_id: [10; 8],
            parent_span_id: None,
            name: "root-op".into(),
            kind: 2,
            start_unix_nano: 0,
            duration_nanos: 5_000_000,
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: String::new(),
            attrs: vec![("n".into(), AttrValue::Int(42))],
            events: Vec::new(),
            links: Vec::new(),
        };
        let child = InputSpan {
            span_id: [20; 8],
            parent_span_id: Some([10; 8]),
            kind: 3,
            attrs: vec![("svc".into(), AttrValue::Str("api".into()))],
            ..root.clone()
        };
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root-op", vec![root, child]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search("t", "{ span:kind != nil }", 0, 100_000, 20)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
        let trace = &r.traces[0];
        // trace_duration = 5_000_000 ns -> 5 ms (kills `/ -> *` and `/ -> %`).
        assert!(trace.duration_ms == 5);

        let spans = &trace.span_sets[0].spans;
        assert!(spans.len() == 2);
        let root_span = spans.iter().find(|s| s.span_id == [10; 8]).unwrap();
        let child_span = spans.iter().find(|s| s.span_id == [20; 8]).unwrap();

        // optional_fixed_8: root has no parent (None), child's parent is [10;8],
        // which is neither [0;8] nor [1;8].
        assert!(root_span.parent_span_id.is_none());
        assert!(child_span.parent_span_id == Some([10; 8]));

        // i32_value: kind is 2 / 3, not 0, 1, or -1.
        assert!(root_span.kind == 2);
        assert!(child_span.kind == 3);

        // row_attrs: the int attribute is carried through with its exact value
        // (kills `row_attrs -> Ok(vec![])`).
        let n_attr = root_span
            .attributes
            .iter()
            .find(|(k, _)| k == "n")
            .map(|(_, v)| v.clone());
        assert!(n_attr == Some(AttrValue::Int(42)));
        let svc_attr = child_span
            .attributes
            .iter()
            .find(|(k, _)| k == "svc")
            .map(|(_, v)| v.clone());
        assert!(svc_attr == Some(AttrValue::Str("api".into())));
    }

    // ---- block-format nested attribute columns (List<List<T>>) ----

    /// Builds `attr_keys` (List<Utf8>) plus the four typed value columns
    /// (List<List<T>>) for a single row carrying four attributes: a string `s`,
    /// an int `i`, a float `f`, and a bool `b`. Each attribute is populated only
    /// in its own typed column; the others are empty inner lists.
    fn block_attr_batch() -> RecordBatch {
        use arrow::array::{
            BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
        };

        // attr_keys: [["s", "i", "f", "b"]].
        let mut keys = ListBuilder::new(StringBuilder::new());
        for k in ["s", "i", "f", "b"] {
            keys.values().append_value(k);
        }
        keys.append(true);

        // attr_value (string): [[["hello"], [], [], []]].
        let mut str_values = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        str_values.values().values().append_value("hello");
        str_values.values().append(true); // s -> ["hello"]
        str_values.values().append(true); // i -> []
        str_values.values().append(true); // f -> []
        str_values.values().append(true); // b -> []
        str_values.append(true);

        // attr_value_int: [[[], [42], [], []]].
        let mut int_values = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
        int_values.values().append(true); // s -> []
        int_values.values().values().append_value(42);
        int_values.values().append(true); // i -> [42]
        int_values.values().append(true); // f -> []
        int_values.values().append(true); // b -> []
        int_values.append(true);

        // attr_value_double: [[[], [], [3.5], []]].
        let mut double_values = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
        double_values.values().append(true); // s -> []
        double_values.values().append(true); // i -> []
        double_values.values().values().append_value(3.5);
        double_values.values().append(true); // f -> [3.5]
        double_values.values().append(true); // b -> []
        double_values.append(true);

        // attr_value_bool: [[[], [], [], [true]]].
        let mut bool_values = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
        bool_values.values().append(true); // s -> []
        bool_values.values().append(true); // i -> []
        bool_values.values().append(true); // f -> []
        bool_values.values().values().append_value(true);
        bool_values.values().append(true); // b -> [true]
        bool_values.append(true);

        let keys = keys.finish();
        let str_values = str_values.finish();
        let int_values = int_values.finish();
        let double_values = double_values.finish();
        let bool_values = bool_values.finish();

        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(BLOCK_ATTR_KEYS, keys.data_type().clone(), true),
            ArrowField::new(BLOCK_ATTR_VALUE, str_values.data_type().clone(), true),
            ArrowField::new(BLOCK_ATTR_VALUE_INT, int_values.data_type().clone(), true),
            ArrowField::new(
                BLOCK_ATTR_VALUE_DOUBLE,
                double_values.data_type().clone(),
                true,
            ),
            ArrowField::new(BLOCK_ATTR_VALUE_BOOL, bool_values.data_type().clone(), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(keys) as ArrayRef,
                Arc::new(str_values),
                Arc::new(int_values),
                Arc::new(double_values),
                Arc::new(bool_values),
            ],
        )
        .unwrap()
    }

    #[test]
    fn block_row_attrs_decodes_every_typed_value_column() {
        // Drives block_row_attrs / block_attr_values_for_key / the typed
        // *_attr_values readers / row_attr_values / optional_list_column end to
        // end. Trivial `vec![]`/`None` replacements anywhere on this path drop a
        // value and fail the equality below.
        let batch = block_attr_batch();
        let mut attrs = block_row_attrs(&batch, 0).unwrap();
        attrs.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(
            attrs
                == vec![
                    ("b".to_string(), AttrValue::Bool(true)),
                    ("f".to_string(), AttrValue::Float(3.5)),
                    ("i".to_string(), AttrValue::Int(42)),
                    ("s".to_string(), AttrValue::Str("hello".into())),
                ]
        );
    }

    #[test]
    fn block_attr_values_for_key_picks_the_populated_type_per_index() {
        // Each attr_idx has exactly one populated typed column. The `if
        // !values.is_empty()` guards select that column; removing a `!` would
        // skip the populated column and return the wrong (empty/fallthrough)
        // type.
        let batch = block_attr_batch();
        let str_values = optional_list_column(&batch, BLOCK_ATTR_VALUE).unwrap();
        let int_values = optional_list_column(&batch, BLOCK_ATTR_VALUE_INT).unwrap();
        let double_values = optional_list_column(&batch, BLOCK_ATTR_VALUE_DOUBLE).unwrap();
        let bool_values = optional_list_column(&batch, BLOCK_ATTR_VALUE_BOOL).unwrap();

        // optional_list_column must actually find the columns (kills `-> Ok(None)`).
        assert!(str_values.is_some());
        assert!(int_values.is_some());
        assert!(double_values.is_some());
        assert!(bool_values.is_some());

        let for_idx = |idx| {
            block_attr_values_for_key(str_values, int_values, double_values, bool_values, 0, idx)
                .unwrap()
        };
        assert!(for_idx(0) == vec![AttrValue::Str("hello".into())]);
        assert!(for_idx(1) == vec![AttrValue::Int(42)]);
        assert!(for_idx(2) == vec![AttrValue::Float(3.5)]);
        assert!(for_idx(3) == vec![AttrValue::Bool(true)]);
    }

    #[test]
    fn typed_block_attr_readers_return_exact_values() {
        // Directly exercise each typed reader so trivial returns
        // (vec![] / vec![0] / vec![1] / vec![-1] / vec!["xyzzy"] / etc.) and the
        // `!values.is_null` filter are all observable.
        let batch = block_attr_batch();
        let str_values = optional_list_column(&batch, BLOCK_ATTR_VALUE).unwrap();
        let int_values = optional_list_column(&batch, BLOCK_ATTR_VALUE_INT).unwrap();
        let double_values = optional_list_column(&batch, BLOCK_ATTR_VALUE_DOUBLE).unwrap();
        let bool_values = optional_list_column(&batch, BLOCK_ATTR_VALUE_BOOL).unwrap();

        assert!(
            string_attr_values(str_values, 0, 0, BLOCK_ATTR_VALUE).unwrap()
                == vec!["hello".to_string()]
        );
        assert!(i64_attr_values(int_values, 0, 1, BLOCK_ATTR_VALUE_INT).unwrap() == vec![42]);
        assert!(
            f64_attr_values(double_values, 0, 2, BLOCK_ATTR_VALUE_DOUBLE).unwrap() == vec![3.5]
        );
        assert!(bool_attr_values(bool_values, 0, 3, BLOCK_ATTR_VALUE_BOOL).unwrap() == vec![true]);

        // An index whose inner list is empty yields an empty vec (not a trivial
        // non-empty replacement).
        assert!(
            string_attr_values(str_values, 0, 1, BLOCK_ATTR_VALUE)
                .unwrap()
                .is_empty()
        );
        assert!(
            i64_attr_values(int_values, 0, 0, BLOCK_ATTR_VALUE_INT)
                .unwrap()
                .is_empty()
        );
        assert!(
            f64_attr_values(double_values, 0, 0, BLOCK_ATTR_VALUE_DOUBLE)
                .unwrap()
                .is_empty()
        );
        assert!(
            bool_attr_values(bool_values, 0, 0, BLOCK_ATTR_VALUE_BOOL)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn row_attr_values_bounds_check_returns_none_out_of_range() {
        // `attr_idx >= row_values.len()` must short-circuit to Ok(None):
        //  * `>= -> <` would reject in-range indices instead.
        //  * `|| -> &&` would stop short-circuiting and index out of bounds.
        let batch = block_attr_batch();
        let str_values = optional_list_column(&batch, BLOCK_ATTR_VALUE).unwrap();
        // In range (idx 0) returns Some.
        assert!(
            row_attr_values(str_values, 0, 0, BLOCK_ATTR_VALUE)
                .unwrap()
                .is_some()
        );
        // Out of range (only 4 inner lists exist) returns None without panicking.
        assert!(
            row_attr_values(str_values, 0, 99, BLOCK_ATTR_VALUE)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn metric_pipeline_parts_rejects_duplicate_stages() {
        use crate::ast::{Aggregate, ComparisonOp, Pipeline};

        let by = vec![field(Scope::Span, "svc")];

        // A single aggregate is accepted.
        assert!(
            metric_pipeline_parts(&[Pipeline::Aggregate(Aggregate::CountOverTime)])
                .unwrap()
                .is_some()
        );

        // Duplicate aggregate / by / filter / rank / compare stages must abort the
        // parse (Ok(None)); each match guard `<slot>.is_none()` (and `!compare`)
        // is what enforces that. Replacing a guard with `true` would accept the
        // duplicate.
        assert!(
            metric_pipeline_parts(&[
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::Aggregate(Aggregate::Rate),
            ])
            .unwrap()
            .is_none()
        );
        assert!(
            metric_pipeline_parts(&[
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::By(by.clone()),
                Pipeline::By(by.clone()),
            ])
            .unwrap()
            .is_none()
        );
        assert!(
            metric_pipeline_parts(&[
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 1.0,
                },
                Pipeline::Filter {
                    op: ComparisonOp::Gt,
                    value: 2.0,
                },
            ])
            .unwrap()
            .is_none()
        );
        assert!(
            metric_pipeline_parts(&[
                Pipeline::Aggregate(Aggregate::CountOverTime),
                Pipeline::TopK(1),
                Pipeline::TopK(2),
            ])
            .unwrap()
            .is_none()
        );
        let compare_stage = || Pipeline::Compare {
            selection: Box::new(SpansetExpr::Selector(Box::new(FieldExpr::Const(true)))),
            top_n: 10,
            start: None,
            end: None,
        };
        assert!(
            metric_pipeline_parts(&[
                Pipeline::Aggregate(Aggregate::CountOverTime),
                compare_stage(),
                compare_stage(),
            ])
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn max_traces_returns_configured_cap() {
        // The accessor must return the configured cap (default 1000), not a
        // trivial 0 or 1.
        let e = engine();
        assert!(e.max_traces() == 1000);
        let custom = TraceqlEngine::new(
            Arc::new(InMemorySpanStore::new()),
            EngineOpts {
                max_traces: 7,
                ..EngineOpts::default()
            },
        );
        assert!(custom.max_traces() == 7);
    }

    fn field(scope: Scope, key: &str) -> Field {
        Field {
            scope,
            key: key.to_string(),
        }
    }

    #[test]
    fn metric_by_attribute_field_emits_projection_matcher() {
        // A metric by()/value field on a regular span or resource attribute must
        // produce a projection matcher so the store materializes its attr.<key>
        // column for GROUP BY (otherwise `rate() by(span.http.method)` 400s with
        // "missing column attr.http.method"). Projection-only, so it must not
        // filter. Parent/instrumentation/intrinsic stay None.
        let span = nested_metric_projection_matcher(&field(Scope::Span, "http.method")).unwrap();
        assert!(span.scope == MatchScope::Span && span.key == "http.method");
        let res =
            nested_metric_projection_matcher(&field(Scope::Resource, "service.version")).unwrap();
        assert!(res.scope == MatchScope::Resource && res.key == "service.version");
        let both = nested_metric_projection_matcher(&field(Scope::Both, "team")).unwrap();
        assert!(both.scope == MatchScope::Both && both.key == "team");
        assert!(nested_metric_projection_matcher(&field(Scope::Parent, "x")).is_none());
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "exhaustive per-scope metric column mapping"
    )]
    fn metric_field_column_maps_every_scope_to_its_column() {
        // service.name short-circuits to the root-service column for Both/Resource.
        assert!(
            metric_field_column(&field(Scope::Both, "service.name")).unwrap()
                == COL_ROOT_SERVICE_NAME
        );
        assert!(
            metric_field_column(&field(Scope::Resource, "service.name")).unwrap()
                == COL_ROOT_SERVICE_NAME
        );
        // Generic attribute scopes prefix the key.
        assert!(
            metric_field_column(&field(Scope::Span, "http.method")).unwrap() == "attr.http.method"
        );
        assert!(
            metric_field_column(&field(Scope::Event, "k")).unwrap()
                == format!("{ATTR_PREFIX}{EVENT_ATTR_PREFIX}k")
        );
        // Scope::Link arm.
        assert!(
            metric_field_column(&field(Scope::Link, "k")).unwrap()
                == format!("{ATTR_PREFIX}{LINK_ATTR_PREFIX}k")
        );
        // Intrinsic arms each map to a distinct column (not the `_ => Err`).
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::Name), "x")).unwrap()
                == COL_NAME
        );
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::Id), "x")).unwrap()
                == COL_SPAN_ID
        );
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::ParentId), "x")).unwrap()
                == COL_PARENT_SPAN_ID
        );
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::NestedSetLeft), "x")).unwrap()
                == COL_NS_LEFT
        );
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::NestedSetRight), "x")).unwrap()
                == COL_NS_RIGHT
        );
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::TraceRootService), "x"))
                .unwrap()
                == COL_ROOT_SERVICE_NAME
        );
        assert!(
            metric_field_column(&field(Scope::Intrinsic(Intrinsic::TraceRootName), "x")).unwrap()
                == COL_ROOT_SPAN_NAME
        );
        assert!(
            metric_field_column(&field(
                Scope::Intrinsic(Intrinsic::InstrumentationVersion),
                "x"
            ))
            .unwrap()
                == COL_INSTRUMENTATION_VERSION
        );
        assert!(
            metric_field_column(&field(
                Scope::Intrinsic(Intrinsic::EventTimeSinceStart),
                "x"
            ))
            .unwrap()
                == COL_EVENT_TIME_SINCE_START
        );
    }

    #[test]
    fn usize_from_integer_f64_validates_and_converts() {
        // A valid non-negative integer float converts to the exact usize.
        assert!(usize_from_integer_f64(3.0).unwrap() == 3);
        assert!(usize_from_integer_f64(0.0).unwrap() == 0);
        // Boundary on the `< 0.0` check: exactly 0.0 is accepted, but any negative
        // is rejected (distinguishes `<` from `<=`/`==`).
        assert!(usize_from_integer_f64(-0.5).is_err());
        assert!(usize_from_integer_f64(-1.0).is_err());
        // The `|| value < 0.0 ||` chain: a NaN (non-finite) and a fractional
        // value are both rejected, so neither `&&` form would pass them.
        assert!(usize_from_integer_f64(f64::NAN).is_err());
        assert!(usize_from_integer_f64(f64::INFINITY).is_err());
        assert!(usize_from_integer_f64(2.5).is_err());
    }

    #[test]
    fn metric_filter_passes_covers_every_operator() {
        use crate::ast::ComparisonOp;
        let f = |op| MetricFilter { op, value: 5.0 };
        // Eq / Neq.
        assert!(metric_filter_passes(5.0, f(ComparisonOp::Eq)));
        assert!(!metric_filter_passes(6.0, f(ComparisonOp::Eq)));
        // Neq is the negation of Eq — the `!` is load-bearing.
        assert!(metric_filter_passes(6.0, f(ComparisonOp::Neq)));
        assert!(!metric_filter_passes(5.0, f(ComparisonOp::Neq)));
        // Lt / Lte.
        assert!(metric_filter_passes(4.0, f(ComparisonOp::Lt)));
        assert!(!metric_filter_passes(5.0, f(ComparisonOp::Lt)));
        // Lte is `!is_gt` — both sides matter.
        assert!(metric_filter_passes(5.0, f(ComparisonOp::Lte)));
        assert!(!metric_filter_passes(6.0, f(ComparisonOp::Lte)));
        // Gt / Gte.
        assert!(metric_filter_passes(6.0, f(ComparisonOp::Gt)));
        assert!(!metric_filter_passes(5.0, f(ComparisonOp::Gt)));
        // Gte is `!is_lt` — both sides matter.
        assert!(metric_filter_passes(5.0, f(ComparisonOp::Gte)));
        assert!(!metric_filter_passes(4.0, f(ComparisonOp::Gte)));
    }

    fn metric_start_batch(starts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            ArrowField::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            ArrowField::new(COL_START, DataType::Int64, false),
        ]));
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(starts.len(), 16);
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(starts.len(), 8);
        for _ in starts {
            trace_id.append_value([1; 16]).unwrap();
            span_id.append_value([2; 8]).unwrap();
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(Int64Array::from(starts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn count_plan() -> MetricPlan {
        MetricPlan {
            function: MetricFunction::CountOverTime,
            value: None,
            quantiles: Vec::new(),
            by: Vec::new(),
            filter: None,
            rank: None,
            compare: None,
        }
    }

    #[test]
    fn assemble_metrics_response_allows_equal_start_and_end() {
        // end_ns == start_ns is a valid single-bucket range. The `end_ns < start_ns`
        // guard must NOT fire on equality (kills `< -> <=` and `< -> ==`).
        let batch = metric_start_batch(&[0]);
        let plan = count_plan();
        let resp = assemble_metrics_response(&[batch], 0, 0, 60_000, &plan, 0, 0).unwrap();
        assert!(resp.series.len() == 1);
        assert!(resp.series[0].points == vec![(0, 1.0)]);

        // end_ns < start_ns is rejected.
        let batch = metric_start_batch(&[0]);
        assert!(assemble_metrics_response(&[batch], 10, 0, 60_000, &plan, 0, 0).is_err());
    }

    #[test]
    fn assemble_metrics_response_range_filter_is_inclusive_at_end_and_exclusive_below_start() {
        // Rows: one below start (-10), one at start (0), one at end (60_000), one
        // above end (120_001). With step 60_000 and range [0, 60_000] there are
        // two buckets. The in-range check is `ts < start || ts > end`:
        //  * `|| -> &&` would stop skipping the below-start row.
        //  * `> -> ==` / `> -> >=` would drop the row exactly at end_ns.
        let batch = metric_start_batch(&[-10, 0, 60_000, 120_001]);
        let plan = count_plan();
        let resp = assemble_metrics_response(&[batch], 0, 60_000, 60_000, &plan, 0, 0).unwrap();
        assert!(resp.series.len() == 1);
        // bucket 0 (ts 0) -> 1, bucket 1 (ts 60_000) -> 1. The out-of-range rows
        // (-10 below start, 120_001 above end) are excluded.
        assert!(resp.series[0].points == vec![(0, 1.0), (60_000, 1.0)]);
    }

    #[tokio::test]
    async fn count_over_time_counts_spans_regardless_of_value_field() {
        // count_over_time has no value field, so absent attributes still count.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_with_code(1, 0, Some(10)),
                sp_with_code(2, 10_000, None),
                sp_with_code(3, 20_000, None),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let count = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(count.series[0].points == vec![(0, 3.0), (60_000, 0.0)]);
    }

    // ---------------------------------------------------------------------
    // compare() — Tempo attribute-comparison metric
    // ---------------------------------------------------------------------

    /// A span with explicit status, attrs and start time for compare tests.
    fn compare_span(id: u8, start: i64, status: i32, attrs: Vec<(&str, AttrValue)>) -> InputSpan {
        InputSpan {
            trace_id: [1; 16],
            span_id: [id; 8],
            parent_span_id: None,
            name: format!("op-{id}"),
            kind: 0,
            start_unix_nano: start,
            duration_nanos: 100,
            status_code: status,
            status_message: String::new(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: String::new(),
            attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn series_label<'a>(series: &'a TraceMetricSeries, key: &str) -> Option<&'a str> {
        series
            .labels
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    // Compare counts are exact integers; sum them as i64 so assertions avoid
    // float equality (clippy::float_cmp).
    #[allow(
        clippy::cast_possible_truncation,
        reason = "compare counts are exact small integers"
    )]
    fn sum_points(points: &[(i64, f64)]) -> i64 {
        points.iter().map(|(_, v)| v.round() as i64).sum()
    }

    /// Total count across all buckets for the series whose `__meta_type` is
    /// `meta` and whose `attr_key` label equals `value`.
    fn compare_total(resp: &TraceMetricsResponse, meta: &str, attr_key: &str, value: &str) -> i64 {
        resp.series
            .iter()
            .filter(|series| {
                series_label(series, "__meta_type") == Some(meta)
                    && series_label(series, attr_key) == Some(value)
            })
            .map(|series| sum_points(&series.points))
            .sum()
    }

    fn meta_total(resp: &TraceMetricsResponse, meta: &str) -> i64 {
        resp.series
            .iter()
            .filter(|series| series_label(series, "__meta_type") == Some(meta))
            .map(|series| sum_points(&series.points))
            .sum()
    }

    #[tokio::test]
    async fn compare_partitions_baseline_and_selection_by_status() {
        // Three error spans (selection) and two ok spans (baseline). The selection
        // is `{ status = error }`; baseline is the matched-outer spans not in it.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                compare_span(1, 0, 2, vec![("http.method", AttrValue::Str("GET".into()))]),
                compare_span(2, 0, 2, vec![("http.method", AttrValue::Str("GET".into()))]),
                compare_span(
                    3,
                    0,
                    2,
                    vec![("http.method", AttrValue::Str("POST".into()))],
                ),
                compare_span(4, 0, 1, vec![("http.method", AttrValue::Str("GET".into()))]),
                compare_span(5, 0, 1, vec![("http.method", AttrValue::Str("PUT".into()))]),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{} | compare({ status = error }, 10)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        // Per-group totals: 3 selection spans, 2 baseline spans.
        assert!(meta_total(&resp, "selection_total") == 3);
        assert!(meta_total(&resp, "baseline_total") == 2);

        // Selection http.method distribution: GET=2, POST=1.
        assert!(compare_total(&resp, "selection", "span.http.method", "GET") == 2);
        assert!(compare_total(&resp, "selection", "span.http.method", "POST") == 1);
        // Baseline http.method distribution: GET=1, PUT=1.
        assert!(compare_total(&resp, "baseline", "span.http.method", "GET") == 1);
        assert!(compare_total(&resp, "baseline", "span.http.method", "PUT") == 1);

        // The status intrinsic distribution is emitted too.
        assert!(compare_total(&resp, "selection", "status", "error") == 3);
        assert!(compare_total(&resp, "baseline", "status", "ok") == 2);

        // Every value series carries exactly the __meta_type + one attr label.
        for series in &resp.series {
            let meta = series_label(series, "__meta_type").unwrap();
            if meta.ends_with("_total") {
                assert!(series.labels.len() == 1);
            } else {
                assert!(
                    series.labels.len() == 2,
                    "value series labels: {:?}",
                    series.labels
                );
            }
        }
    }

    #[tokio::test]
    async fn compare_top_n_truncates_least_frequent_values() {
        // Four distinct values for span.path with frequencies 4,3,2,1; topN=2 keeps
        // only the two most frequent (a=4, b=3) in the selection group.
        let mut spans = Vec::new();
        let mut id = 1u8;
        for (path, freq) in [("a", 4), ("b", 3), ("c", 2), ("d", 1)] {
            for _ in 0..freq {
                spans.push(compare_span(
                    id,
                    0,
                    2,
                    vec![("path", AttrValue::Str(path.into()))],
                ));
                id += 1;
            }
        }
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "api", "root", spans);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{} | compare({ status = error }, 2)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        // Exactly the two most-frequent path values survive topN=2.
        let kept: std::collections::BTreeSet<&str> = resp
            .series
            .iter()
            .filter(|series| {
                series_label(series, "__meta_type") == Some("selection")
                    && series.labels.iter().any(|(k, _)| k == "span.path")
            })
            .filter_map(|series| series_label(series, "span.path"))
            .collect();
        assert!(kept == ["a", "b"].into_iter().collect());
        // All five error spans are still counted in the selection total.
        assert!(meta_total(&resp, "selection_total") == 10);
    }

    #[tokio::test]
    async fn compare_emits_zero_totals_for_empty_group() {
        // No span matches the selection, so the selection group is empty but a
        // zero-valued selection_total series is still emitted (Grafana needs a
        // denominator for both groups).
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![compare_span(
                1,
                0,
                1,
                vec![("k", AttrValue::Str("v".into()))],
            )],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{} | compare({ status = error }, 10)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(meta_total(&resp, "baseline_total") == 1);
        assert!(meta_total(&resp, "selection_total") == 0);
        // The lone span lands in baseline.
        assert!(compare_total(&resp, "baseline", "span.k", "v") == 1);
    }

    #[tokio::test]
    async fn compare_selection_window_restricts_selection_group() {
        // Two error spans, one inside the [start,end]=[5000,15000] sub-window and
        // one outside it. With the window, only the in-window error span joins the
        // selection group; the out-of-window error span falls to baseline.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                compare_span(1, 10_000, 2, vec![("z", AttrValue::Str("in".into()))]),
                compare_span(2, 30_000, 2, vec![("z", AttrValue::Str("out".into()))]),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{} | compare({ status = error }, 10, 5000, 15000)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(meta_total(&resp, "selection_total") == 1);
        assert!(meta_total(&resp, "baseline_total") == 1);
        assert!(compare_total(&resp, "selection", "span.z", "in") == 1);
        assert!(compare_total(&resp, "baseline", "span.z", "out") == 1);
    }

    #[tokio::test]
    async fn compare_rejects_structural_selection() {
        let e = engine();
        let err = e
            .query_range(
                "t",
                "{} | compare({ .a = 1 } >> { .b = 2 }, 10)",
                0,
                60_000,
                60_000,
            )
            .await;
        assert!(matches!(err, Err(TraceqlError::Unsupported(_))));
    }

    // ---- MAJOR 1: resource.service.name is the trace-root service only ----

    /// Builds a single-row batch in the REAL store's block shape: the
    /// `attr_keys`/typed-value list columns carry a per-span
    /// `__resource.service.name` (which MUST be ignored), a plain resource attr
    /// (`__resource.cluster`), and a span attr (`http.method`); plus the
    /// standard scalar columns `compare_row` reads (name/status/kind/duration/
    /// root-service/start). The root-service column is the canonical
    /// `resource.service.name` source.
    fn compare_block_batch() -> RecordBatch {
        use arrow::array::{
            BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
        };

        // attr_keys: [["__resource.service.name", "__resource.cluster",
        //              "http.method"]]. All three are string-typed.
        let keys_list = [
            "__resource.service.name",
            "__resource.cluster",
            "http.method",
        ];
        let mut keys = ListBuilder::new(StringBuilder::new());
        for k in keys_list {
            keys.values().append_value(k);
        }
        keys.append(true);

        // attr_value (string): the per-span resource service is "span-svc" (this
        // value must NOT surface); cluster=eu; http.method=GET.
        let mut str_values = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        str_values.values().values().append_value("span-svc");
        str_values.values().append(true); // __resource.service.name -> ["span-svc"]
        str_values.values().values().append_value("eu");
        str_values.values().append(true); // __resource.cluster -> ["eu"]
        str_values.values().values().append_value("GET");
        str_values.values().append(true); // http.method -> ["GET"]
        str_values.append(true);

        // The other typed columns are empty inner lists for every key.
        let empty_list = |count: usize| {
            let mut int_values = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
            let mut double_values = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
            let mut bool_values = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
            for _ in 0..count {
                int_values.values().append(true);
                double_values.values().append(true);
                bool_values.values().append(true);
            }
            int_values.append(true);
            double_values.append(true);
            bool_values.append(true);
            (
                int_values.finish(),
                double_values.finish(),
                bool_values.finish(),
            )
        };
        let (int_values, double_values, bool_values) = empty_list(keys_list.len());

        let keys = keys.finish();
        let str_values = str_values.finish();

        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(BLOCK_ATTR_KEYS, keys.data_type().clone(), true),
            ArrowField::new(BLOCK_ATTR_VALUE, str_values.data_type().clone(), true),
            ArrowField::new(BLOCK_ATTR_VALUE_INT, int_values.data_type().clone(), true),
            ArrowField::new(
                BLOCK_ATTR_VALUE_DOUBLE,
                double_values.data_type().clone(),
                true,
            ),
            ArrowField::new(BLOCK_ATTR_VALUE_BOOL, bool_values.data_type().clone(), true),
            ArrowField::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            ArrowField::new(COL_NAME, DataType::Utf8, true),
            ArrowField::new(COL_STATUS_CODE, DataType::Int32, false),
            ArrowField::new(COL_KIND, DataType::Int32, false),
            ArrowField::new(COL_DURATION, DataType::Int64, false),
            ArrowField::new(COL_START, DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(keys) as ArrayRef,
                Arc::new(str_values),
                Arc::new(int_values),
                Arc::new(double_values),
                Arc::new(bool_values),
                // Trace-root service name (the canonical resource.service.name).
                Arc::new(StringArray::from(vec!["root-svc"])),
                Arc::new(StringArray::from(vec!["op"])),
                Arc::new(Int32Array::from(vec![2])), // status = error
                Arc::new(Int32Array::from(vec![2])), // kind = server
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )
        .unwrap()
    }

    fn compare_range() -> MetricsRange {
        MetricsRange {
            scan_start: 0,
            scan_end: 60_000,
            output_start: 0,
            step: 60_000,
        }
    }

    fn selector(fe: FieldExpr) -> SpansetExpr {
        SpansetExpr::Selector(Box::new(fe))
    }

    #[test]
    fn compare_row_emits_single_root_resource_service_name() {
        // MAJOR 1: a single span row must surface `resource.service.name` exactly
        // once, equal to the trace-root service (COL_ROOT_SERVICE_NAME), never the
        // per-span `__resource.service.name` block attr.
        let batch = compare_block_batch();
        let row = compare_row(&batch, 0, 0).unwrap();
        let service: Vec<&String> = row
            .attrs
            .iter()
            .filter(|(k, _)| k == "resource.service.name")
            .map(|(_, v)| v)
            .collect();
        assert!(service == vec![&"root-svc".to_string()]);
        // The per-span resource service value never leaks in.
        assert!(!row.attrs.iter().any(|(_, v)| v == "span-svc"));
    }

    #[test]
    fn compare_block_batch_carries_span_and_resource_labels() {
        // MINOR 7: drive the real block-attr production path through
        // assemble_compare_response/compare_row. The series must carry BOTH a
        // span.<k> label and resource.<k> labels (including resource.service.name
        // = the trace-root service, MAJOR 1).
        let compare = CompareSpec {
            selection: selector(FieldExpr::Const(false)),
            top_n: 10,
            start: None,
            end: None,
        };
        let resp =
            assemble_compare_response(&[compare_block_batch()], &compare, compare_range()).unwrap();

        // The lone span falls to baseline (selection is Const(false)).
        assert!(compare_total(&resp, "baseline", "span.http.method", "GET") == 1);
        assert!(compare_total(&resp, "baseline", "resource.cluster", "eu") == 1);
        assert!(compare_total(&resp, "baseline", "resource.service.name", "root-svc") == 1);
        // Exactly one resource.service.name value series exists, and it is the
        // trace-root service (no duplicate from the per-span block attr).
        let svc_values: std::collections::BTreeSet<&str> = resp
            .series
            .iter()
            .filter_map(|series| series_label(series, "resource.service.name"))
            .collect();
        assert!(svc_values == ["root-svc"].into_iter().collect());
    }

    // ---- MAJOR 3: != / !~ on an absent attribute stays Baseline ----

    #[test]
    fn compare_neq_concrete_on_absent_attr_is_baseline() {
        // MAJOR 3: a span lacking `.foo` with selection `{ .foo != "bar" }` must
        // NOT be pulled into the selection group — an absent attr matches only
        // `= nil`, mirroring the planner SQL (`NULL != v` excludes).
        let row = CompareRow {
            ts: 0,
            attrs: vec![("span.other".into(), "x".into())],
            raw_span_attrs: vec![("other".into(), AttrValue::Str("x".into()))],
            raw_resource_attrs: Vec::new(),
            name: None,
            status_code: None,
            status_message: None,
            kind: None,
            duration: None,
        };
        let neq = CompareSpec {
            selection: selector(FieldExpr::Comparison {
                lhs: field(Scope::Span, "foo"),
                op: ComparisonOp::Neq,
                rhs: Value::Str("bar".into()),
            }),
            top_n: 10,
            start: None,
            end: None,
        };
        let regexes = CompareRegexCache::new();
        assert!(compare_group_for_row(&row, &neq, &regexes) == CompareGroup::Baseline);

        // `= nil` on the same absent attr DOES match (the only matching op).
        let eq_nil = CompareSpec {
            selection: selector(FieldExpr::Comparison {
                lhs: field(Scope::Span, "foo"),
                op: ComparisonOp::Eq,
                rhs: Value::Nil,
            }),
            ..neq.clone()
        };
        assert!(compare_group_for_row(&row, &eq_nil, &regexes) == CompareGroup::Selection);
    }

    // ---- MAJOR 4: baseline and selection share one value set per attribute ----

    #[tokio::test]
    async fn compare_emits_shared_value_set_across_groups() {
        // MAJOR 4: `span.region` has value "eu" in BOTH groups and "us" only in
        // the selection. The chosen value set is the selection top-N; for each
        // chosen value BOTH a baseline and a selection series are emitted, so a
        // selection-only value ("us") gets a zero-filled baseline series too.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                // selection (status=error): region eu, eu, us.
                compare_span(1, 0, 2, vec![("region", AttrValue::Str("eu".into()))]),
                compare_span(2, 0, 2, vec![("region", AttrValue::Str("eu".into()))]),
                compare_span(3, 0, 2, vec![("region", AttrValue::Str("us".into()))]),
                // baseline (status=ok): region eu.
                compare_span(4, 0, 1, vec![("region", AttrValue::Str("eu".into()))]),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let resp = e
            .query_range(
                "t",
                "{} | compare({ status = error }, 10)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        // Both groups expose the SAME span.region value set (the selection top-N).
        let value_set = |meta: &str| -> std::collections::BTreeSet<String> {
            resp.series
                .iter()
                .filter(|series| series_label(series, "__meta_type") == Some(meta))
                .filter_map(|series| series_label(series, "span.region").map(str::to_string))
                .collect()
        };
        let selection_set = value_set("selection");
        let baseline_set = value_set("baseline");
        assert!(selection_set == ["eu".to_string(), "us".to_string()].into_iter().collect());
        assert!(baseline_set == selection_set);

        // The selection-only value "us" has a zero-filled baseline series.
        assert!(compare_total(&resp, "selection", "span.region", "us") == 1);
        assert!(compare_total(&resp, "baseline", "span.region", "us") == 0);
        // The shared value "eu": selection 2, baseline 1.
        assert!(compare_total(&resp, "selection", "span.region", "eu") == 2);
        assert!(compare_total(&resp, "baseline", "span.region", "eu") == 1);
    }

    // ---- MAJOR 2: per-attribute value cardinality is bounded ----

    /// Builds an `n`-row batch with a single promoted string column `attr.path`
    /// whose value is unique per row (`/p/<i>`), all at start time 0. Drives the
    /// high-cardinality accumulation path.
    fn unique_path_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(COL_START, DataType::Int64, false),
            ArrowField::new(format!("{ATTR_PREFIX}path"), DataType::Utf8, true),
        ]));
        let starts = Int64Array::from(vec![0_i64; n]);
        let paths = StringArray::from((0..n).map(|i| format!("/p/{i}")).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(starts), Arc::new(paths)]).unwrap()
    }

    #[test]
    fn compare_bounds_per_attribute_value_cardinality() {
        // MAJOR 2: feed 1000 per-span-unique span.path values. Unbounded, the
        // accumulator would hold one bucket-Vec per value. With top_n=10 the cap
        // clamps to COMPARE_MAX_VALUES_PER_ATTR (256), so at most 256 distinct
        // span.path values are tracked per (group, attr) BEFORE truncation, and
        // the emitted series obey top_n=10 AFTER.
        let batch = unique_path_batch(1000);
        let compare = CompareSpec {
            // Const(true): every span joins the selection group.
            selection: selector(FieldExpr::Const(true)),
            top_n: 10,
            start: None,
            end: None,
        };
        let range = compare_range();
        let bucket_count = 2;

        // Pre-truncation: distinct span.path values per group are capped.
        let (counts, totals) =
            accumulate_compare_counts(&[batch], &compare, range, bucket_count).unwrap();
        let distinct_paths = counts
            .keys()
            .filter(|(group, attr, _)| *group == CompareGroup::Selection && attr == "span.path")
            .count();
        assert!(
            distinct_paths == COMPARE_MAX_VALUES_PER_ATTR,
            "tracked {distinct_paths} distinct values, expected cap {COMPARE_MAX_VALUES_PER_ATTR}"
        );
        // All 1000 spans are still counted in the per-group total (the cap only
        // limits tracked distinct VALUES, never the per-group span totals).
        assert!(totals[&CompareGroup::Selection].iter().sum::<u64>() == 1000);

        // Post-truncation: the emitted span.path series obey top_n.
        let resp = assemble_compare_response(&[unique_path_batch(1000)], &compare, range).unwrap();
        let path_series = resp
            .series
            .iter()
            .filter(|series| {
                series_label(series, "__meta_type") == Some("selection")
                    && series.labels.iter().any(|(k, _)| k == "span.path")
            })
            .count();
        assert!(path_series <= compare.top_n);
    }

    // ---- MINOR 5: event/link attrs do not leak as span.__event.* ----

    #[tokio::test]
    async fn compare_does_not_leak_event_attrs_as_span_labels() {
        // MINOR 5: the outer query references an event attr, so the scan
        // materializes an `attr.__event.exception.type` column. It must NOT appear
        // in the span distribution as `span.__event.exception.type`.
        let mut span = compare_span(1, 0, 2, vec![("http.method", AttrValue::Str("GET".into()))]);
        span.events = vec![EventRef {
            time_since_start_nano: 10,
            name: "exception".into(),
            attributes: vec![("exception.type".into(), AttrValue::Str("IOError".into()))],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "api", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{ event.exception.type != nil } | compare({ status = error }, 10)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        // No series carries a span.__event.* / span.__link.* label.
        let leaked = resp.series.iter().any(|series| {
            series
                .labels
                .iter()
                .any(|(k, _)| k.starts_with("span.__event.") || k.starts_with("span.__link."))
        });
        assert!(
            !leaked,
            "event/link attrs leaked into the span distribution"
        );
    }

    // ---- MINOR 6: selection regex is compiled once and reused ----

    #[tokio::test]
    async fn compare_regex_selection_matches_with_shared_cache() {
        // MINOR 6: a `=~` selection still partitions correctly when the regex is
        // precompiled once per query. Spans whose name matches `op-[12]` join the
        // selection; the rest stay baseline. (Correctness check for the cached
        // regex path; the cache merely avoids per-row recompilation.)
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                compare_span(1, 0, 2, vec![("k", AttrValue::Str("a".into()))]),
                compare_span(2, 0, 2, vec![("k", AttrValue::Str("b".into()))]),
                compare_span(3, 0, 2, vec![("k", AttrValue::Str("c".into()))]),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{} | compare({ name =~ \"op-[12]\" }, 10)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        // op-1 and op-2 match the regex (selection=2); op-3 does not (baseline=1).
        assert!(meta_total(&resp, "selection_total") == 2);
        assert!(meta_total(&resp, "baseline_total") == 1);
    }
}
