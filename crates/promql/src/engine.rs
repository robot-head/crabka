//! Minimal `PromQL` engine entry point.
//!
//! This currently implements selector evaluation over the `MetricStore` contract.
//! The rest of Slice 2's planner (functions, aggregations, binary ops) will build
//! on this public API.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::array::{Array, AsArray};
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use crabka_blockstore::{LabelMatcher, Labels, MatchOp, SeriesFingerprint};
use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint, decode_native_histograms};
use futures::FutureExt;
use futures::future::BoxFuture;
use promql_parser::label as prom_label;
use promql_parser::parser::token::{
    T_ADD, T_ATAN2, T_AVG, T_BOTTOMK, T_COUNT, T_COUNT_VALUES, T_DIV, T_EQLC, T_GROUP, T_GTE,
    T_GTR, T_LAND, T_LIMIT_RATIO, T_LIMITK, T_LOR, T_LSS, T_LTE, T_LUNLESS, T_MAX, T_MIN, T_MOD,
    T_MUL, T_NEQ, T_POW, T_QUANTILE, T_STDDEV, T_STDVAR, T_SUB, T_SUM, T_TOPK, TokenType,
};
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinModifier, BinaryExpr, Call, Expr, LabelModifier, MatrixSelector,
    Offset, SubqueryExpr, UnaryExpr, VectorMatchCardinality, VectorSelector,
};
use time::OffsetDateTime;

use std::cell::RefCell;

use crate::error::Result;
// Shared stale-NaN predicate: the interpreter and the `InstantManipulate`
// operator must make identical stale-vs-genuine-NaN selection decisions.
use crate::extension::is_stale_nan;
use crate::functions::{OverTimeFamily, ScalarMathOp};
use crate::planner::aggregate::{
    AGGREGATE_VALUE_COLUMN, Grouping, SimpleAggregateOp, plan_simple_aggregate,
};
use crate::planner::label_ops::{self, SortOrder};
use crate::planner::leaf::{
    self, InstantSelectorPlan, LabeledSample, plan_instant_vector_selector,
};
use crate::planner::over_time_range::{
    self, LabeledSample as OverTimeLabeledSample, OverTimeRangePlan,
    over_time_family_from_function_name, plan_over_time_range_selector,
};
use crate::planner::rate_range::{
    self, LabeledSample as RateLabeledSample, RateRangePlan, RateUdfKind, plan_rate_range_selector,
};
use crate::planner::scalar_math::{
    self, LabeledValue as ScalarMathLabeledValue, ScalarMathPlan, plan_scalar_math,
};
use crate::planner::{ExtendedSelectorExpr, ExtendedSelectorModifier};
use crate::result::{Annotations, InstantSample, QueryResult, RangeSeries, SampleValue};
use crate::store::MetricStore;
use crate::{DurationExprContext, parse_promql, parse_promql_with_duration_context};
use crate::{PromqlError, ScanResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use regex::Regex;

/// Static options for `PromQL` evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    /// Maximum age of a sample considered by an instant-vector selector.
    pub lookback_delta_ms: i64,
    /// Global evaluation interval used when a subquery omits its resolution.
    pub eval_interval_ms: i64,
    /// Maximum float samples returned by one query.
    pub max_samples: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            lookback_delta_ms: 5 * 60 * 1000,
            eval_interval_ms: 60_000,
            max_samples: 50_000_000,
        }
    }
}

/// Maximum number of resolution points (steps) a single range/subquery series
/// may span. Prometheus rejects a query whose `(end - start) / step + 1` exceeds
/// this, capping abusive resolutions (e.g. `last_over_time(up[1000d:1ms])`)
/// before the per-step loop runs.
pub const MAX_RESOLUTION_POINTS: u64 = 11_000;

/// Compute the resolution-point count `(end_ms - start_ms) / step_ms + 1` for a
/// range/subquery grid, rejecting an abusive resolution before any per-step
/// evaluation runs.
///
/// The cap is applied to the *interval* count `(end - start) / step`, matching
/// Prometheus' `(end-start)/step > 11000` rule and the HTTP front-gate
/// byte-for-byte (error type, status, and message), so a query that the gate
/// admits is never re-rejected by this backstop.
///
/// # Errors
///
/// Returns [`PromqlError::Plan`] (HTTP 400 `bad_data`) when `step_ms <= 0` or when
/// the interval count exceeds [`MAX_RESOLUTION_POINTS`].
pub fn check_resolution_points(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<u64> {
    if step_ms <= 0 {
        return Err(PromqlError::Plan(format!(
            "zero or negative query resolution step widths are not accepted. Try a positive integer (step={step_ms}ms)"
        )));
    }
    // Reject on the interval count `(end - start) / step` (Prometheus' rule),
    // computed in u64 space so an abusive span can never overflow or wrap into a
    // small count.
    let span = u64::try_from(end_ms.saturating_sub(start_ms).max(0)).unwrap_or(u64::MAX);
    let step = u64::try_from(step_ms).unwrap_or(u64::MAX);
    let intervals = span / step;
    if intervals > MAX_RESOLUTION_POINTS {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .to_string(),
        ));
    }
    Ok(intervals.saturating_add(1))
}

/// `PromQL` evaluator over a concrete metric store.
pub struct PromqlEngine<S: MetricStore> {
    store: Arc<S>,
    opts: EngineOpts,
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
struct QueryRangeContext {
    start: i64,
    end: i64,
    step: i64,
}

#[cfg(feature = "experimental-functions")]
tokio::task_local! {
    static QUERY_RANGE_CONTEXT: QueryRangeContext;
}

tokio::task_local! {
    /// The active range query's `[start, end]` bounds, scoped by the per-step
    /// planner range driver ([`PromqlEngine::eval_range_via_planner_scoped`]) so a
    /// bare top-level selector carrying an `@ start()` / `@ end()` modifier
    /// resolves those bounds to the QUERY's range bounds — per Prometheus — while
    /// the planner still evaluates the selector at each grid step. Absent (no
    /// task-local) for an instant query, where `@ start()`/`@ end()` is invalid and
    /// the selector planner raises the same hard error the interpreter does.
    static AT_MODIFIER_BOUNDS: AtModifierBounds;
}

/// The range bounds in scope for `@ start()`/`@ end()` resolution, or `None` when
/// not inside a range query (an instant query).
fn current_at_modifier_bounds() -> Option<AtModifierBounds> {
    AT_MODIFIER_BOUNDS.try_with(|bounds| *bounds).ok()
}

struct RangeEval {
    series: Vec<RangeSeries>,
    end_ms: i64,
    range_ms: i64,
    modifier: Option<ExtendedSelectorModifier>,
}

/// How a planner-path output batch carries its result value and labels, so the
/// shared assembler ([`PromqlEngine::assemble_planned_instant`]) knows how to
/// read each shape's columns into an [`InstantVector`](QueryResult::InstantVector).
enum InstantShape {
    /// `SeriesDivide -> SeriesNormalize -> InstantManipulate`. Output carries
    /// label columns plus `timestamp`/`value`/`sample_timestamp`; the selected
    /// sample's true timestamp survives in `sample_timestamp`. Result labels are
    /// recovered from `labels_by_fp` keyed by the row's reconstructed fingerprint.
    Selector,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>(...) AS value)`.
    /// Output carries label columns plus a single `value` column; the eval
    /// timestamp is reattached at assembly and the metric name is dropped. NaN
    /// rows are suppressed (the UDF's "no value" sentinel).
    RateProjection,
    /// `... -> RangeManipulate -> Projection(labels..., prom_<fn>_over_time(...)
    /// AS value)`. Output carries label columns plus a single `value` column; the
    /// eval timestamp is reattached at assembly and NaN rows (the UDF's "no
    /// value" sentinel) are suppressed. `preserve_metric_name` keeps `__name__`
    /// only for `last_over_time`; every other family drops it, matching the
    /// interpreter's `eval_over_time_call`.
    OverTimeProjection { preserve_metric_name: bool },
    /// `<inner> -> Aggregate -> Projection(group_labels..., agg AS value)`.
    /// Output carries exactly the grouping label columns plus `value`. The
    /// result labelset is the grouping labels read directly from the batch (no
    /// fingerprint lookup), and the eval timestamp is reattached at assembly.
    Aggregate,
    /// `<leaf over already-evaluated inner vector> -> Projection(labels...,
    /// prom_<fn>([bounds...,] value) AS value)`. Output carries the
    /// metadata-free label columns plus a single `value` column; the metric name
    /// is already dropped at the leaf. The labelset is read directly from the
    /// batch and the eval timestamp is reattached at assembly. Unlike the
    /// rate/`*_over_time` shapes, **every** row is kept (no NaN suppression):
    /// `f(NaN)` / `sqrt(-1)` render as `NaN`, matching the interpreter, which
    /// keeps every float sample.
    ScalarMath,
}

/// A planned instant-query result. Produced by the recursive
/// [`PromqlEngine::plan_instant_expr`] and consumed by
/// [`PromqlEngine::assemble_planned_instant`].
///
/// Most shapes lower to a `DataFusion` [`LogicalPlan`] over the custom operators
/// ([`PlannedInstant::Operator`]). The label-rewrite / ordering functions
/// (`label_replace`/`label_join`/`sort`/`sort_desc`) instead transform their
/// already-assembled inner instant vector in pure Rust, so they carry the
/// finished samples directly ([`PlannedInstant::Precomputed`]); no operator plan
/// is executed for them.
enum PlannedInstant {
    /// An executable operator plan plus the metadata its shape's assembler needs.
    /// Boxed to keep the enum small (the operator payload carries a
    /// [`SessionContext`] and a [`LogicalPlan`]).
    Operator(Box<OperatorInstant>),
    /// A fully-assembled instant vector produced by a label-rewrite / ordering
    /// transform over a recursively-planned inner vector. Returned to the caller
    /// verbatim — there is no operator plan to execute.
    Precomputed(Vec<InstantSample>),
    /// A fully-computed **scalar** result. Carried by the scalar-returning utility
    /// functions (`time`/`pi`/`scalar`, the argless calendar forms) and any
    /// scalar∘scalar binary fold that the planner resolves in pure Rust. Assembled
    /// into a [`QueryResult::Scalar`] verbatim — there is no operator plan to
    /// execute. The `ts_ms`/`value` mirror exactly what the interpreter would
    /// return for the same expression, so the two paths are parity-exact.
    PrecomputedScalar { ts_ms: i64, value: f64 },
    /// A fully-computed **string** result. Carried by a top-level string literal.
    /// Assembled into a [`QueryResult::Str`] verbatim — there is no operator plan
    /// to execute. Mirrors exactly what the interpreter returns for the same
    /// literal.
    PrecomputedString { ts_ms: i64, value: String },
    /// A fully-materialized **range vector** (range matrix). Carried by a
    /// top-level raw matrix selector / subquery, whose `query_instant` result is a
    /// [`QueryResult::RangeMatrix`]. Built via the interpreter's own
    /// `eval_matrix_selector` / `eval_subquery`, so the two paths are parity-exact
    /// by construction.
    PrecomputedMatrix(Vec<RangeSeries>),
}

/// The executable payload of [`PlannedInstant::Operator`].
struct OperatorInstant {
    /// Session context whose physical planner understands the custom operators
    /// (and holds the rate UDFs), with the inner leaf table registered.
    ctx: SessionContext,
    /// The fully-lowered logical plan to execute.
    plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for the selector/rate shapes' result
    /// assembly. The aggregate/scalar-math shapes read labels straight from the
    /// batch and leave this empty.
    labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
    /// How to read the output batches into an instant vector.
    shape: InstantShape,
}

impl PlannedInstant {
    /// Wrap an executable operator plan, boxing the payload.
    fn operator(
        ctx: SessionContext,
        plan: LogicalPlan,
        labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
        shape: InstantShape,
    ) -> Self {
        Self::Operator(Box::new(OperatorInstant {
            ctx,
            plan,
            labels_by_fp,
            shape,
        }))
    }
}

tokio::task_local! {
    /// Per-query annotation sink. Scoped once at each public query entry point
    /// so the deeply recursive evaluation path can record warnings/infos without
    /// threading a collector argument through every call site.
    static ANNOTATIONS: RefCell<Annotations>;
}

/// Record a `PromQL warning:`-class annotation for the current query, if a sink
/// is in scope. No-op outside a scoped query (e.g. unit tests calling internals
/// directly), so emission is always safe.
fn emit_warning(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().warn(message));
}

/// Record a `PromQL info:`-class annotation for the current query, if a sink is
/// in scope. See [`emit_warning`].
fn emit_info(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().info(message));
}

/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text for `metric`.
fn mixed_classic_native_warning(metric: &str) -> String {
    format!(
        "PromQL warning: vector contains a mix of classic and native histograms for metric name {metric:?}"
    )
}

/// Exact Prometheus `InvalidQuantileWarning` text for a `quantile` /
/// `quantile_over_time` phi outside `[0, 1]` (or NaN). Like the
/// `histogram_quantile` family, Prometheus does NOT abort on a bad phi: it
/// returns signed `±Inf` / `NaN` and raises this warning. `got` renders through
/// the canonical Prometheus float formatter, matching Go's `%v`.
fn invalid_quantile_warning(got: f64) -> String {
    format!(
        "PromQL warning: quantile value should be between 0 and 1, got {}",
        crate::http_api::format_sample_value(got)
    )
}

/// Whether `phi` is a valid quantile in `[0, 1]`. An out-of-range or NaN phi is
/// still evaluated (Prometheus returns `±Inf`/`NaN` + an `InvalidQuantileWarning`
/// rather than erroring); this only gates the warning.
fn is_valid_quantile(phi: f64) -> bool {
    (0.0..=1.0).contains(&phi)
}

/// Remember the `__name__` of the first sample seen for a histogram group key,
/// so a later mixed-histogram warning can name the metric like Prometheus does.
fn record_metric_name(names: &mut BTreeMap<String, String>, key: &str, labels: &Labels) {
    if let Some(name) = labels.get("__name__") {
        names
            .entry(key.to_string())
            .or_insert_with(|| name.to_string());
    }
}

/// Emit one `MixedClassicNativeHistogramsWarning` per group key that carried
/// both a classic and a native histogram for the same labelset.
fn warn_mixed_histograms(mixed_keys: &BTreeSet<String>, names: &BTreeMap<String, String>) {
    for key in mixed_keys {
        let metric = names.get(key).map_or("", String::as_str);
        emit_warning(mixed_classic_native_warning(metric));
    }
}

/// Exact Prometheus `InvalidRatioWarning` text.
///
/// Rust's `f64` `Display` matches Go's `%g` for the integral and one-decimal
/// ratios this annotation reports (`1` for `1.0`, `1.1` for `1.1`, `-1` for
/// `-1.0`), so it renders the corpus-asserted text byte-for-byte.
#[cfg(feature = "experimental-functions")]
fn invalid_ratio_warning(got: f64, capped_to: f64) -> String {
    format!(
        "PromQL warning: ratio value should be between -1 and 1, got {got}, capping to {capped_to}"
    )
}

/// Exact Prometheus `IncompatibleTypesInBinOpInfo` text for an operator applied
/// to incompatible operand sample types (e.g. a histogram and a float).
fn incompatible_types_in_binop_info(lhs_type: &str, operator: &str, rhs_type: &str) -> String {
    format!(
        "PromQL info: incompatible sample types encountered for binary operator {operator:?}: {lhs_type} {operator} {rhs_type}"
    )
}

impl<S: MetricStore> PromqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }

    /// Evaluate an instant query at `time_ms`.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    pub async fn query_instant(
        &self,
        tenant: &str,
        query: &str,
        time_ms: i64,
    ) -> Result<QueryResult> {
        self.query_instant_with_annotations(tenant, query, time_ms)
            .await
            .map(|(result, _)| result)
    }

    /// Evaluate an instant query at `time_ms`, returning any warnings/infos
    /// raised during evaluation alongside the result.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    pub async fn query_instant_with_annotations(
        &self,
        tenant: &str,
        query: &str,
        time_ms: i64,
    ) -> Result<(QueryResult, Annotations)> {
        ANNOTATIONS
            .scope(RefCell::new(Annotations::new()), async move {
                let expr = parse_promql_with_duration_context(
                    query,
                    DurationExprContext::instant(time_ms),
                )?;
                let result = self
                    .eval_top_level_instant_expr(tenant, &expr, time_ms)
                    .await?;
                validate_unique_instant_labelsets(&result)?;
                let annotations = ANNOTATIONS.with(|sink| sink.borrow().clone());
                Ok((result, annotations))
            })
            .await
    }

    /// Dispatch a top-level instant query.
    ///
    /// The query is handed to the recursive operator planner
    /// ([`Self::plan_instant_expr`]), which dispatches on the `PromQL` `Expr`
    /// node kind and assembles a `DataFusion` [`LogicalPlan`] over the custom
    /// operators (plus the shared leaf kernels it reuses for histogram-bearing
    /// and other directly-materialized shapes). The planner is **total**: it
    /// returns `Ok(Some(..))` for every valid query and `Err(..)` for every
    /// invalid one, and never `Ok(None)`. The plan is executed and its output
    /// batches assembled into the result.
    ///
    /// `Ok(None)` is therefore unreachable; should it ever arise it is a planner
    /// bug, surfaced as an internal [`PromqlError::Plan`] rather than silently
    /// diverging.
    async fn eval_top_level_instant_expr(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let Some(planned) = self.plan_instant_expr(tenant, expr, time_ms).await? else {
            return Err(PromqlError::Plan(
                "planner returned no result for a valid instant query".to_string(),
            ));
        };
        self.assemble_planned_instant(planned, time_ms).await
    }

    /// Recursively plan an instant expression onto the `DataFusion` operator
    /// chain (plus the shared leaf kernels it reuses), dispatching on the
    /// `PromQL` `Expr` node kind. This is the sole evaluation engine.
    ///
    /// Returns `Ok(Some(plan))` for every valid shape and `Err(..)` for every
    /// invalid one (`Err` also covers genuine store/plan failures). It is
    /// **total**: it never returns `Ok(None)` for a query the public entry
    /// points accept (proven by `plan_instant_expr_is_total_over_construct_sweep`
    /// and the green conformance corpus). Supported node kinds:
    ///
    /// - [`Expr::Paren`] — recurse into the inner expression.
    /// - [`Expr::VectorSelector`] — a bare instant-vector selector over
    ///   float-only series (`SeriesDivide -> SeriesNormalize ->
    ///   InstantManipulate`). Histogram-bearing selectors return `None`.
    /// - [`Expr::Call`] — a rate-family call or a non-experimental `*_over_time`
    ///   call over a bare matrix selector. A FLOAT-only selector lowers onto the
    ///   operator chain (`... -> RangeManipulate -> rate/over_time-UDF`); a
    ///   HISTOGRAM-bearing selector instead assembles the windowed range vector via
    ///   the interpreter's `eval_matrix_selector` and applies the shared
    ///   `apply_outer_range_fn` kernel as a `Precomputed` result (parity-exact). The
    ///   experimental `*_over_time` members (`mad`/`first`/`ts_of_*`), subquery
    ///   arguments, anchored/smoothed selectors, and present-but-empty-valued labels
    ///   return `None`.
    /// - [`Expr::Aggregate`] — a simple float aggregation
    ///   (`sum|avg|min|max|count|group` with `by`/`without`) over a
    ///   planner-supported, float-only inner expression. Param aggregations
    ///   (`topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`),
    ///   histogram-typed inputs, and unsupported inner expressions return `None`.
    ///
    /// Every other node kind (binary ops, unary, literals, raw matrix/subquery,
    /// extensions) returns `None`.
    fn plan_instant_expr<'a>(
        &'a self,
        tenant: &'a str,
        expr: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<Option<PlannedInstant>>> {
        async move {
            match expr {
                Expr::Paren(paren) => self.plan_instant_expr(tenant, &paren.expr, time_ms).await,
                Expr::VectorSelector(selector) => {
                    // A histogram-bearing selector cannot ride the float-only
                    // operator leaf, so select it directly via the interpreter's
                    // own `eval_instant_selector` (the direct shared-kernel scan,
                    // which carries native histograms as `SampleValue::Histogram`
                    // and float series unchanged) and return the finished vector as
                    // `Precomputed` — parity-exact with the interpreter by
                    // construction. This also faithfully carries any empty-valued
                    // labels on those series.
                    if self
                        .selector_has_histogram_series(tenant, selector, time_ms)
                        .await?
                    {
                        let QueryResult::InstantVector(samples) = self
                            .eval_instant_selector(tenant, selector, time_ms)
                            .await?
                        else {
                            return Ok(None);
                        };
                        return Ok(Some(PlannedInstant::Precomputed(samples)));
                    }
                    // A float-only selector — including one matching a series with
                    // a present-but-empty-valued label — rides the operator leaf:
                    // the leaf now encodes an ABSENT label as NULL and a
                    // PRESENT-empty label as `""`, so the reconstructed
                    // fingerprint matches the original series identity.
                    Ok(Some(
                        self.plan_instant_selector(tenant, selector, time_ms)
                            .await?,
                    ))
                }
                Expr::Call(call_expr) => {
                    self.plan_call_expr(tenant, expr, call_expr, time_ms).await
                }
                Expr::Aggregate(aggregate) => {
                    // A simple (no-param) float aggregation lowers onto a
                    // DataFusion aggregate; the parameterized ops
                    // (topk/bottomk/quantile/count_values/stddev/stdvar) recurse
                    // the inner vector and apply the shared interpreter routine in
                    // Rust (a `Precomputed` result). `plan_simple_aggregate_expr`
                    // returns `None` for the param ops, so we then try the param
                    // path; anything neither handles falls back.
                    if let Some(planned) = self
                        .plan_simple_aggregate_expr(tenant, aggregate, time_ms)
                        .await?
                    {
                        return Ok(Some(planned));
                    }
                    self.plan_param_aggregate_expr(tenant, aggregate, time_ms)
                        .await
                }
                Expr::Binary(binary) => self.plan_binary_expr(tenant, binary, time_ms).await,
                // A unary `-`/`+` over a planner-supported operand: recurse the
                // operand, assemble it, and negate via the SHARED
                // `negate_query_result` (parity-exact with `eval_instant_unary`).
                Expr::Unary(unary) => self.plan_unary_expr(tenant, unary, time_ms).await,
                // A top-level numeric literal is a scalar; a top-level string
                // literal is a string. Both mirror the interpreter's
                // `eval_instant_expr` literal arms verbatim.
                Expr::NumberLiteral(number) => Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: number.val,
                })),
                Expr::StringLiteral(s) => Ok(Some(PlannedInstant::PrecomputedString {
                    ts_ms: time_ms,
                    value: s.val.clone(),
                })),
                // A top-level raw matrix selector / subquery yields a range vector
                // from `query_instant`, built via the interpreter's own
                // materialization (parity-exact).
                Expr::MatrixSelector(ms) => Ok(Some(PlannedInstant::PrecomputedMatrix(
                    self.eval_matrix_selector(tenant, ms, time_ms, time_ms, None)
                        .await?,
                ))),
                Expr::Subquery(subquery) => Ok(Some(PlannedInstant::PrecomputedMatrix(
                    self.eval_subquery(tenant, subquery, time_ms).await?,
                ))),
                // An `anchored`/`smoothed` extended selector: reuse the
                // interpreter's `eval_smoothed_instant_selector` kernel (and its
                // anchored-on-instant error) through `Precomputed`.
                Expr::Extension(extension) => {
                    self.plan_extension_expr(tenant, expr, extension, time_ms)
                        .await
                }
            }
        }
        .boxed()
    }

    /// Plan a unary `Expr::Unary` (`-v` / `+v`) onto the operator path: recurse
    /// the operand through the planner, assemble it, and apply the SHARED
    /// [`negate_query_result`] — identical to [`Self::eval_instant_unary`] by
    /// construction. The `PromQL` parser only ever produces a `-` unary (a leading
    /// `+` is dropped), so this always negates. A non-plannable operand falls back
    /// to the interpreter.
    async fn plan_unary_expr(
        &self,
        tenant: &str,
        unary: &UnaryExpr,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let Some(planned) = self.plan_instant_expr(tenant, &unary.expr, time_ms).await? else {
            return Ok(None);
        };
        match self.assemble_planned_instant(planned, time_ms).await? {
            QueryResult::Scalar { ts_ms, value } => Ok(Some(PlannedInstant::PrecomputedScalar {
                ts_ms,
                value: -value,
            })),
            other => match negate_query_result(other)? {
                QueryResult::InstantVector(samples) => {
                    Ok(Some(PlannedInstant::Precomputed(samples)))
                }
                // `negate_query_result` only ever returns a scalar (handled above)
                // or an instant vector for a non-error input; a range-matrix /
                // string operand already surfaced as `Err` above.
                _ => Ok(None),
            },
        }
    }

    /// Plan a top-level `Expr::Extension` (an `anchored`/`smoothed` extended
    /// selector) onto the operator path. The `smoothed` form reuses the
    /// interpreter's [`Self::eval_smoothed_instant_selector`] kernel verbatim
    /// (returned as `Precomputed`); the `anchored` form on an instant selector is
    /// the same hard error the interpreter raises. Any other extension shape
    /// (non-selector child, unknown extension) falls back to the interpreter,
    /// which raises the canonical "not implemented yet" error.
    async fn plan_extension_expr(
        &self,
        tenant: &str,
        expr: &Expr,
        extension: &promql_parser::parser::Extension,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let Some(extended) = extension
            .expr
            .as_any()
            .downcast_ref::<ExtendedSelectorExpr>()
        else {
            return Ok(None);
        };
        let Some(Expr::VectorSelector(selector)) = extended.child() else {
            return Ok(None);
        };
        match extended.modifier() {
            ExtendedSelectorModifier::Smoothed => {
                let QueryResult::InstantVector(samples) = self
                    .eval_smoothed_instant_selector(tenant, selector, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(samples)))
            }
            // `anchored` is invalid on an instant-vector selector — raise the same
            // error the interpreter does, on the operator path.
            ExtendedSelectorModifier::Anchored => {
                let _ = expr;
                Err(PromqlError::Unsupported(
                    "anchored modifier is not valid on instant-vector selectors".to_string(),
                ))
            }
        }
    }

    /// Plan an `Expr::Call` onto the operator path, dispatching on the function
    /// kind. `expr` is the same node as `call_expr` (needed by the rate and
    /// over-time matchers, which inspect the call's range argument). Each arm returns
    /// `Ok(Some(..))` for a supported shape and `Ok(None)` (interpreter fallback)
    /// otherwise; any function not recognized here falls back.
    #[allow(clippy::too_many_lines)]
    async fn plan_call_expr(
        &self,
        tenant: &str,
        expr: &Expr,
        call_expr: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // A rate-family call over a bare matrix selector. A FLOAT-only selector
        // rides the RangeManipulate + rate-UDF operator chain. A HISTOGRAM-bearing
        // selector cannot (the operator leaf is float-only), so it assembles the
        // windowed range vector via the interpreter's own `eval_matrix_selector`
        // and applies the SHARED `apply_outer_range_fn` kernel — byte-for-byte the
        // interpreter's counter-reset/extrapolation (rate/increase/delta) and
        // float-only filter (irate/idelta). `match_rate_range_call` rejects
        // anchored/smoothed selectors and every non-rate function, so the
        // interpreter still owns those. A present-but-empty-valued label now
        // rides the operator leaf too (NULL encodes absent, `""` present-empty).
        if let Some((selector, kind)) = match_rate_range_call(expr) {
            if self
                .matrix_selector_has_histogram_series(tenant, selector, time_ms)
                .await?
            {
                return Ok(Some(
                    self.plan_histogram_range_via_kernel(
                        tenant,
                        selector,
                        time_ms,
                        rate_udf_kind_to_outer_range_fn(kind),
                    )
                    .await?,
                ));
            }
            return Ok(Some(
                self.plan_rate_range(tenant, selector, time_ms, kind)
                    .await?,
            ));
        }
        // A non-experimental `*_over_time` call (or `quantile_over_time`) over a
        // bare matrix selector. A FLOAT-only selector rides the RangeManipulate +
        // over_time-UDF operator chain. A HISTOGRAM-bearing selector assembles the
        // windowed range vector via the interpreter's `eval_matrix_selector` and
        // applies the SHARED `apply_outer_range_fn` kernel, so each member's
        // histogram behaviour matches the interpreter exactly: `sum`/`avg` merge
        // histograms; `count`/`last`/`present` are histogram-safe;
        // `min`/`max`/`stddev`/`stdvar`/`quantile` ignore histograms (an
        // all-histogram window then yields no row). The experimental members,
        // subquery ranges, and anchored/smoothed selectors are rejected by the
        // matcher and stay on the interpreter; a present-but-empty-valued label
        // now rides the operator leaf too.
        if let Some((selector, family, phi_arg)) = match_over_time_range_call(expr) {
            // Resolve `quantile_over_time`'s `phi` (needed by both the float and
            // histogram paths). A non-scalar `phi` falls back to the interpreter;
            // an out-of-range/NaN `phi` is NOT an error — Prometheus returns
            // signed `±Inf` / `NaN` plus an `InvalidQuantileWarning` (matching
            // the `histogram_quantile` family), so we evaluate it directly.
            let phi = match phi_arg {
                Some(arg) => {
                    let QueryResult::Scalar { value, .. } =
                        self.plan_and_resolve(tenant, arg, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    if !is_valid_quantile(value) {
                        emit_warning(invalid_quantile_warning(value));
                    }
                    value
                }
                None => f64::NAN,
            };
            if self
                .matrix_selector_has_histogram_series(tenant, selector, time_ms)
                .await?
            {
                return Ok(Some(
                    self.plan_histogram_range_via_kernel(
                        tenant,
                        selector,
                        time_ms,
                        over_time_family_to_outer_range_fn(family, phi),
                    )
                    .await?,
                ));
            }
            return Ok(Some(
                self.plan_over_time_range(tenant, selector, time_ms, family, phi)
                    .await?,
            ));
        }
        // An EXPERIMENTAL `*_over_time` member (`mad`/`first`/`ts_of_*_over_time`)
        // over a bare matrix selector. These members have no operator-leaf UDF, so
        // both the float and the histogram selector route through the SAME shared
        // `apply_outer_range_fn` kernel as the histogram path above: the windowed
        // range vector is assembled via the interpreter's own `eval_matrix_selector`
        // and folded by `over_time_sample_from_series`, byte-for-byte the
        // interpreter's `eval_over_time_call`. `first_over_time` preserves
        // `__name__` (like `last_over_time`); the others drop it
        // (`OverTimeFn::preserves_metric_name`). The kernel selection is faithful
        // to the interpreter, including any present-but-empty-valued label.
        if let Some((selector, kind)) = match_experimental_over_time_range_call(expr) {
            return Ok(Some(
                self.plan_histogram_range_via_kernel(
                    tenant,
                    selector,
                    time_ms,
                    OuterRangeFn::OverTime(kind),
                )
                .await?,
            ));
        }
        // A RESIDUAL range-vector fold the fast matchers above don't claim: a
        // `changes`/`resets`/`deriv` over a plain matrix selector (no operator-leaf
        // UDF), a `predict_linear`/`double_exponential_smoothing` over a plain
        // matrix, OR ANY rate-family / `*_over_time` fold over an `anchored`/
        // `smoothed` extended selector (which `match_rate_range_call` /
        // `match_over_time_range_call` reject because they require a plain
        // `MatrixSelector`). These all delegate to the SAME interpreter
        // `eval_instant_call` dispatch (which builds the windowed range vector via
        // `eval_range_arg` — honoring the anchored/smoothed window — and folds it
        // with the shared `apply_outer_range_fn`), wrapped in `Precomputed`, so the
        // result is byte-for-byte identical to the interpreter by construction.
        // Subquery range arguments are already claimed by `match_subquery_range_call`
        // above. This arm is what makes the call dispatch TOTAL over the range-fold
        // surface.
        if is_extended_range_fold_call(call_expr) {
            return Ok(Some(
                self.plan_extended_range_fold_call(tenant, call_expr, time_ms)
                    .await?,
            ));
        }
        // A per-row scalar-math call (`abs`/`ceil`/…/`sgn`, the trig/hyperbolic
        // family, `round`, and the `clamp` family) over a planner-supported,
        // float-only instant-vector argument routes through a `Projection(f(value))`
        // over the inner vector. Non-scalar bound args, histogram inputs, and any
        // inner expression the planner cannot evaluate fall back.
        if let Some(op) = scalar_math_op_from_function_name(call_expr.func.name) {
            return self
                .plan_scalar_math_call(tenant, call_expr, op, time_ms)
                .await;
        }
        // A label-rewrite (`label_replace`/`label_join`) or ordering
        // (`sort`/`sort_desc`/`sort_by_label`/`sort_by_label_desc`) call over a
        // planner-supported, float-only inner instant vector recurses into that
        // vector, assembles it, and applies the transform in pure Rust (shared
        // with the interpreter). Wrong arity, non-string label/regex args, and any
        // inner expression the planner cannot evaluate fall back.
        if let Some(kind) = label_ops_kind_from_function_name(call_expr.func.name) {
            return self
                .plan_label_ops_call(tenant, call_expr, kind, time_ms)
                .await;
        }
        // A `histogram_quantile(phi, v)` over a planner-supported classic OR
        // native-histogram bucket vector recurses into `v`, selects it
        // (histogram-aware), and applies the shared classic+native fold in pure
        // Rust.
        if call_expr.func.name == "histogram_quantile" {
            return self
                .plan_histogram_quantile_call(tenant, call_expr, time_ms)
                .await;
        }
        // `histogram_quantiles(label, v, phi...)` (experimental): resolve each
        // scalar `phi`, select the inner bucket vector `v` (histogram-aware), and
        // apply the SAME shared `apply_histogram_quantiles` fold the interpreter
        // uses — emitting one series per `(input series, phi)` pair.
        #[cfg(feature = "experimental-functions")]
        if call_expr.func.name == "histogram_quantiles" {
            return self
                .plan_histogram_quantiles_call(tenant, call_expr, time_ms)
                .await;
        }
        // The native accessors (`histogram_count`/`sum`/`avg`/`stddev`/`stdvar`)
        // over a planner-supported native-histogram vector recurse into the
        // operand, select it (histogram-aware), and apply the SAME shared accessor
        // free function the interpreter uses, so the result is parity-exact.
        if let Some(accessor) = histogram_accessor_from_function_name(call_expr.func.name) {
            return self
                .plan_histogram_accessor_call(tenant, call_expr, accessor, time_ms)
                .await;
        }
        // `histogram_fraction(lower, upper, v)` over a planner-supported classic
        // OR native-histogram vector: resolve the two scalar bounds, select `v`
        // (histogram-aware), and apply the SAME shared fraction free function
        // (incl. the classic+native mixed-schema warning) the interpreter uses.
        if call_expr.func.name == "histogram_fraction" {
            return self
                .plan_histogram_fraction_call(tenant, call_expr, time_ms)
                .await;
        }
        // `info(v [, data_label_selector])`: recurse the input vector `v`
        // (histogram-aware), select the `target_info` (or custom-selector) series
        // through the SAME interpreter helper, and apply the SHARED `apply_info`
        // join. The store-touching info-series selection and the join are identical
        // to the interpreter's, so the result is parity-exact. A non-plannable input
        // falls back; wrong arity / a non-vector-selector data-label arg surfaces as
        // `Err` here, matching the interpreter.
        if call_expr.func.name == "info" {
            return self.plan_info_call(tenant, call_expr, time_ms).await;
        }
        // A range/`*_over_time` call whose argument is a SUBQUERY
        // (`f(inner[range:res] ...)`). The subquery's range vector is built per
        // aligned sub-step through the recursive planner and the outer fold is the
        // shared `apply_outer_range_fn`. A non-plannable / histogram-bearing inner,
        // a non-positive step, or an invalid scalar parameter falls back to the
        // interpreter.
        if let Some((subquery, spec)) = match_subquery_range_call(call_expr) {
            return self
                .plan_subquery_range_call(tenant, subquery, spec, time_ms)
                .await;
        }
        // The EXPERIMENTAL scalar/range functions that have no operator-leaf UDF:
        // `max_of`/`min_of` (scalar∘scalar extrema), `double_exponential_smoothing`
        // over a bare matrix selector (a range fold via the shared
        // `apply_outer_range_fn`), and the duration helpers `range`/`step`/`start`/
        // `end` (scalar, NOT parser-folded). Each delegates to the SAME interpreter
        // method, so the result is parity-exact by construction; a non-experimental
        // build leaves them on the interpreter (which raises the
        // requires-experimental-functions error).
        #[cfg(feature = "experimental-functions")]
        if let Some(planned) = self
            .plan_experimental_call(tenant, call_expr, time_ms)
            .await?
        {
            return Ok(Some(planned));
        }
        // In a NON-experimental build, the experimental-only functions
        // (`max_of`/`min_of`, the duration helpers `range`/`step`/`start`/`end`,
        // `double_exponential_smoothing`, `histogram_quantiles`) must raise the
        // SAME `requires the experimental-functions feature` error the
        // tree-walking oracle raises. Raise it directly planner-side (the
        // canonical message, byte-for-byte identical to the oracle's arms). (In an
        // experimental build these names are handled above / by
        // `plan_histogram_quantiles_call`.)
        #[cfg(not(feature = "experimental-functions"))]
        if matches!(
            call_expr.func.name,
            "max_of"
                | "min_of"
                | "range"
                | "step"
                | "start"
                | "end"
                | "double_exponential_smoothing"
                | "histogram_quantiles"
        ) {
            return Err(PromqlError::Unsupported(format!(
                "function `{}` requires the experimental-functions feature",
                call_expr.func.name
            )));
        }
        // The remaining float UTILITY functions: `timestamp`, the calendar family
        // (`year`/`month`/…/`minute`, both the vector and the argless `time()`
        // forms), `absent`/`absent_over_time`, and the scalar-returning
        // `time`/`pi`/`scalar`/`vector`. Each recurses its plannable inner (where
        // it has one), assembles it, and applies the SAME shared interpreter logic
        // in pure Rust, so the result is parity-exact by construction. A histogram
        // operand, a non-plannable inner, or any form the matcher cannot make
        // parity-exact returns `None` (interpreter fallback).
        self.plan_util_call(tenant, call_expr, time_ms).await
    }

    /// Plan a RESIDUAL range-vector fold call (see [`is_extended_range_fold_call`])
    /// self-recursively: resolve the call's [`OuterRangeFn`] (and any scalar
    /// parameter) through the planner's own helpers, build the windowed range
    /// vector through the shared [`Self::eval_range_arg`] leaf kernel — which honors
    /// an `anchored`/`smoothed` extended selector's window and validates the
    /// modifier against the function name — and fold it with the shared
    /// [`apply_outer_range_fn`]. This is byte-for-byte identical to the
    /// `#[cfg(test)]` tree-walking oracle's `eval_*_call` family by construction
    /// (they run the same `eval_range_arg` + `apply_outer_range_fn`), including the
    /// per-function error for an invalid modifier/arity/parameter, which surfaces
    /// here as the SAME `Err`.
    ///
    /// This is the planner arm that closes the range-fold fallback: a plain-matrix
    /// `changes`/`resets`/`deriv`/`predict_linear`/`double_exponential_smoothing`
    /// and ANY rate-family / `*_over_time` fold over an anchored/smoothed selector
    /// now route through the planner.
    async fn plan_extended_range_fold_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<PlannedInstant> {
        Ok(PlannedInstant::Precomputed(
            self.resolve_range_fold_call(tenant, call, time_ms).await?,
        ))
    }

    /// Resolve a residual range-vector fold [`Call`] (see
    /// [`range_fold_range_arg_index`]) into its folded instant vector without
    /// re-entering the tree-walking interpreter: map the function name to its
    /// [`OuterRangeFn`] (resolving any scalar parameter via the planner's own
    /// scalar resolvers), materialize the windowed range vector through the shared
    /// [`Self::eval_range_arg`] leaf kernel, and apply the shared
    /// [`apply_outer_range_fn`] fold. The per-function arity / scalar-type /
    /// modifier errors are raised exactly as the oracle's `eval_*_call` family
    /// raises them.
    #[allow(
        clippy::too_many_lines,
        reason = "the range-fold name -> OuterRangeFn dispatch table is intentionally centralized"
    )]
    async fn resolve_range_fold_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Vec<InstantSample>> {
        let Some(range_index) = range_fold_range_arg_index(call) else {
            // `range_fold_range_arg_index` returns `None` for a wrong-arity call of
            // an otherwise-known fold (e.g. `predict_linear`/`quantile_over_time`
            // with !=2 args, `double_exponential_smoothing` with !=3). Raise the
            // same arity error the oracle's matching `eval_*_call` raises so a
            // malformed call surfaces an identical message.
            let expected = match call.func.name {
                "quantile_over_time" | "predict_linear" => Some("two"),
                #[cfg(feature = "experimental-functions")]
                "double_exponential_smoothing" => Some("three"),
                _ => None,
            };
            if let Some(expected) = expected {
                return Err(PromqlError::Plan(format!(
                    "{} expects exactly {expected} arguments, got {}",
                    call.func.name,
                    call.args.args.len()
                )));
            }
            return Err(PromqlError::Plan(format!(
                "`{}` is not a range-vector fold call",
                call.func.name
            )));
        };
        // Resolve the function's outer fold, plus any scalar parameter, exactly as
        // the oracle's matching `eval_*_call` does.
        let outer = match call.func.name {
            "rate" => OuterRangeFn::Range(RangeFn::Rate),
            "increase" => OuterRangeFn::Range(RangeFn::Increase),
            "delta" => OuterRangeFn::Range(RangeFn::Delta),
            "changes" => OuterRangeFn::Range(RangeFn::Changes),
            "resets" => OuterRangeFn::Range(RangeFn::Resets),
            "irate" => OuterRangeFn::InstantDelta(IrateFn::Irate),
            "idelta" => OuterRangeFn::InstantDelta(IrateFn::Idelta),
            "deriv" => OuterRangeFn::Deriv,
            "sum_over_time" => OuterRangeFn::OverTime(OverTimeFn::Sum),
            "avg_over_time" => OuterRangeFn::OverTime(OverTimeFn::Avg),
            "count_over_time" => OuterRangeFn::OverTime(OverTimeFn::Count),
            "min_over_time" => OuterRangeFn::OverTime(OverTimeFn::Min),
            "max_over_time" => OuterRangeFn::OverTime(OverTimeFn::Max),
            "stddev_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stddev),
            "stdvar_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
            "last_over_time" => OuterRangeFn::OverTime(OverTimeFn::Last),
            "present_over_time" => OuterRangeFn::OverTime(OverTimeFn::Present),
            #[cfg(feature = "experimental-functions")]
            "mad_over_time" => OuterRangeFn::OverTime(OverTimeFn::Mad),
            #[cfg(feature = "experimental-functions")]
            "first_over_time" => OuterRangeFn::OverTime(OverTimeFn::First),
            #[cfg(feature = "experimental-functions")]
            "ts_of_first_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfFirst),
            #[cfg(feature = "experimental-functions")]
            "ts_of_last_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfLast),
            #[cfg(feature = "experimental-functions")]
            "ts_of_min_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMin),
            #[cfg(feature = "experimental-functions")]
            "ts_of_max_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMax),
            "quantile_over_time" => {
                let quantile = match self
                    .plan_and_resolve(tenant, &call.args.args[0], time_ms)
                    .await?
                {
                    QueryResult::Scalar { value, .. } => value,
                    QueryResult::InstantVector(_)
                    | QueryResult::RangeMatrix(_)
                    | QueryResult::Str { .. } => {
                        return Err(PromqlError::Plan(
                            "quantile_over_time quantile argument must be a scalar".to_string(),
                        ));
                    }
                };
                // An out-of-range / NaN `phi` is NOT an error: Prometheus returns
                // signed `±Inf` / `NaN` plus an `InvalidQuantileWarning` (matching
                // the `histogram_quantile` family).
                if !is_valid_quantile(quantile) {
                    emit_warning(invalid_quantile_warning(quantile));
                }
                OuterRangeFn::QuantileOverTime(quantile)
            }
            "predict_linear" => {
                let duration_seconds = self
                    .eval_scalar_arg(tenant, call, 1, time_ms, "duration")
                    .await?;
                OuterRangeFn::PredictLinear(duration_seconds)
            }
            #[cfg(feature = "experimental-functions")]
            "double_exponential_smoothing" => {
                let smoothing_factor = self
                    .eval_scalar_expr(
                        tenant,
                        &call.args.args[1],
                        time_ms,
                        "double_exponential_smoothing smoothing factor",
                    )
                    .await?;
                let trend_factor = self
                    .eval_scalar_expr(
                        tenant,
                        &call.args.args[2],
                        time_ms,
                        "double_exponential_smoothing trend factor",
                    )
                    .await?;
                validate_smoothing_factor("smoothing factor", smoothing_factor)?;
                validate_smoothing_factor("trend factor", trend_factor)?;
                OuterRangeFn::DoubleExponentialSmoothing {
                    smoothing: smoothing_factor,
                    trend: trend_factor,
                }
            }
            other => {
                return Err(PromqlError::Plan(format!(
                    "`{other}` is not a range-vector fold call"
                )));
            }
        };
        let range = self
            .eval_range_arg(
                tenant,
                &call.args.args[range_index],
                time_ms,
                call.func.name,
            )
            .await?;
        Ok(apply_outer_range_fn(range, outer, time_ms))
    }

    /// Plan the EXPERIMENTAL non-leaf functions onto the operator path by
    /// delegating to the SAME interpreter method (so the result — value,
    /// labelset, and any annotation side effect — is parity-exact by
    /// construction) and wrapping it in the matching `Precomputed*` variant:
    ///
    /// - `max_of`/`min_of` → scalar extrema, a `PrecomputedScalar`.
    /// - `double_exponential_smoothing(m[range], sf, tf)` over a bare matrix
    ///   selector → an instant vector via the shared `apply_outer_range_fn` fold,
    ///   a `Precomputed`. (The subquery-range form is handled earlier by
    ///   `match_subquery_range_call`.)
    /// - the duration helpers `range`/`step`/`start`/`end` → a scalar; these are
    ///   plain `Expr::Call`s (NOT parser-folded), reading the scoped range
    ///   context.
    ///
    /// Returns `Ok(None)` for any other function name (the caller then tries
    /// `plan_util_call`). A wrong-arity / invalid-argument call surfaces the same
    /// `Err` the interpreter would, since this delegates to it.
    #[cfg(feature = "experimental-functions")]
    async fn plan_experimental_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        match call.func.name {
            "max_of" => scalar_call_to_planned(
                &self
                    .eval_scalar_extrema_call(tenant, call, time_ms, ScalarExtremaFn::Max)
                    .await?,
            )
            .map(Some),
            "min_of" => scalar_call_to_planned(
                &self
                    .eval_scalar_extrema_call(tenant, call, time_ms, ScalarExtremaFn::Min)
                    .await?,
            )
            .map(Some),
            "double_exponential_smoothing" => Ok(Some(PlannedInstant::Precomputed(
                self.resolve_range_fold_call(tenant, call, time_ms).await?,
            ))),
            "range" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::Range,
            )?)
            .map(Some),
            "step" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::Step,
            )?)
            .map(Some),
            "start" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::Start,
            )?)
            .map(Some),
            "end" => scalar_call_to_planned(&Self::eval_duration_helper_call(
                call,
                time_ms,
                DurationHelper::End,
            )?)
            .map(Some),
            _ => Ok(None),
        }
    }

    /// Plan the float UTILITY functions onto the operator path:
    /// `timestamp`/`scalar`/`vector`, the calendar family, `time`/`pi`, and
    /// `absent`/`absent_over_time`. See [`Self::plan_call_expr`].
    ///
    /// Returns `Ok(Some(..))` for a supported, parity-exact shape and `Ok(None)`
    /// (interpreter fallback) for everything else (unknown function, wrong arity,
    /// non-plannable / histogram-bearing inner, non-scalar `vector` arg, …). The
    /// interpreter then raises any canonical arity/type error.
    async fn plan_util_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        match call.func.name {
            // `time()` — the eval timestamp in seconds (no arguments).
            "time" => {
                if !call.args.args.is_empty() {
                    return Ok(None);
                }
                Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: timestamp_seconds(time_ms),
                }))
            }
            // `pi()` — the constant π (no arguments).
            "pi" => {
                if !call.args.args.is_empty() {
                    return Ok(None);
                }
                Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: std::f64::consts::PI,
                }))
            }
            // `scalar(v)` — the lone series' value, else NaN (incl. for a
            // histogram-valued single series, which yields NaN).
            "scalar" => self.plan_scalar_function_call(tenant, call, time_ms).await,
            // `vector(s)` — a single no-label series carrying the scalar `s`.
            "vector" => self.plan_vector_function_call(tenant, call, time_ms).await,
            // `timestamp(v)` — per-row: the sample's timestamp in seconds.
            "timestamp" => self.plan_timestamp_call(tenant, call, time_ms).await,
            // `absent(v)` / `absent_over_time(v[range])`.
            "absent" => self.plan_absent_call(tenant, call, time_ms).await,
            "absent_over_time" => self.plan_absent_over_time_call(tenant, call, time_ms).await,
            // The calendar family over a vector argument, or argless over `time()`.
            other => {
                let Some(kind) = calendar_fn_from_function_name(other) else {
                    return Ok(None);
                };
                self.plan_calendar_call(tenant, call, kind, time_ms).await
            }
        }
    }

    /// Plan `timestamp(v)`: recurse `v` through the planner, assemble it, and map
    /// each row to its own sample timestamp in seconds (dropping `__name__`,
    /// reattaching the eval timestamp), exactly mirroring
    /// [`Self::eval_timestamp_call`]. Wrong arity, a non-plannable inner, or a
    /// histogram-bearing inner fall back to the interpreter.
    async fn plan_timestamp_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(samples) = self.label_ops_inner_vector(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        // The interpreter's `timestamp` keeps every sample (it does not filter
        // floats vs histograms — it uses only the timestamp). `label_ops_inner_vector`
        // already rejects histogram-bearing bare selectors, but a nested inner
        // could still surface a histogram sample; fall back wholesale so the
        // interpreter (which would keep it) stays the source of truth.
        if samples
            .iter()
            .any(|sample| matches!(sample.value, SampleValue::Histogram(_)))
        {
            return Ok(None);
        }
        let out = samples
            .into_iter()
            .map(|sample| InstantSample {
                labels: labels_without_metric_name(&sample.labels),
                ts_ms: time_ms,
                value: SampleValue::Float(timestamp_seconds(sample.ts_ms)),
            })
            .collect();
        Ok(Some(PlannedInstant::Precomputed(out)))
    }

    /// Plan a calendar function. With one argument it recurses the inner vector
    /// and applies [`CalendarFn::apply`] per float row (dropping non-floats and
    /// `__name__`, reattaching the eval timestamp), mirroring
    /// [`Self::eval_calendar_call`]. With zero arguments it operates on `time()`
    /// (the eval timestamp in seconds) and yields a [`PrecomputedScalar`]. Wrong
    /// arity or a non-plannable inner fall back.
    async fn plan_calendar_call(
        &self,
        tenant: &str,
        call: &Call,
        kind: CalendarFn,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            // The argless calendar form operates on `time()`.
            if call.args.args.is_empty() {
                return Ok(Some(PlannedInstant::PrecomputedScalar {
                    ts_ms: time_ms,
                    value: kind.apply(timestamp_seconds(time_ms)),
                }));
            }
            return Ok(None);
        };
        let Some(samples) = self.label_ops_inner_vector(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        let out = samples
            .into_iter()
            .filter_map(|sample| {
                let SampleValue::Float(value) = sample.value else {
                    return None;
                };
                Some(InstantSample {
                    labels: labels_without_metric_name(&sample.labels),
                    ts_ms: time_ms,
                    value: SampleValue::Float(kind.apply(value)),
                })
            })
            .collect();
        Ok(Some(PlannedInstant::Precomputed(out)))
    }

    /// Plan `scalar(v)`: recurse `v` through the planner, assemble it, and return
    /// the lone series' float value, or NaN when `v` is not exactly one series (or
    /// the single series is histogram-valued), mirroring
    /// [`Self::eval_scalar_function_call`]. Wrong arity or a non-plannable inner
    /// fall back.
    async fn plan_scalar_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(planned) = self.plan_instant_expr(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        let QueryResult::InstantVector(samples) =
            self.assemble_planned_instant(planned, time_ms).await?
        else {
            return Ok(None);
        };
        let value = if samples.len() == 1 {
            match samples.into_iter().next().expect("single sample").value {
                SampleValue::Float(value) => value,
                SampleValue::Histogram(_) => f64::NAN,
            }
        } else {
            f64::NAN
        };
        Ok(Some(PlannedInstant::PrecomputedScalar {
            ts_ms: time_ms,
            value,
        }))
    }

    /// Plan `vector(s)`: fold the scalar argument `s` via the interpreter's pure
    /// scalar path and emit a single no-label series carrying that value,
    /// mirroring [`Self::eval_vector_function_call`]. Wrong arity or a non-scalar
    /// argument fall back.
    async fn plan_vector_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let QueryResult::Scalar { value, .. } = self.plan_and_resolve(tenant, arg, time_ms).await?
        else {
            return Ok(None);
        };
        Ok(Some(PlannedInstant::Precomputed(vec![InstantSample {
            labels: Labels::new(),
            ts_ms: time_ms,
            value: SampleValue::Float(value),
        }])))
    }

    /// Plan `absent(v)`: recurse `v` through the planner and assemble it; an
    /// empty result yields a single 1-valued series whose labels are derived from
    /// `v`'s matchers ([`absent_labels`]), and a non-empty result yields the empty
    /// vector, mirroring [`Self::eval_absent_call`]. Wrong arity or a
    /// non-plannable / histogram-bearing inner fall back.
    async fn plan_absent_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(planned) = self.plan_instant_expr(tenant, arg, time_ms).await? else {
            return Ok(None);
        };
        let QueryResult::InstantVector(samples) =
            self.assemble_planned_instant(planned, time_ms).await?
        else {
            return Ok(None);
        };
        if !samples.is_empty() {
            return Ok(Some(PlannedInstant::Precomputed(Vec::new())));
        }
        Ok(Some(PlannedInstant::Precomputed(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }])))
    }

    /// Plan `absent_over_time(v[range])`: evaluate the range selector via the
    /// shared [`Self::eval_range_arg`] (parity-exact — the same code the
    /// interpreter runs) and, when no series carries an in-window sample, emit a
    /// single 1-valued series whose labels derive from `v`'s matchers, mirroring
    /// [`Self::eval_absent_over_time_call`]. A histogram-bearing matrix selector
    /// or any non-matrix-selector inner falls back to the interpreter.
    async fn plan_absent_over_time_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // Wrong arity is the interpreter's canonical error — raise it here too.
        if call.args.args.len() != 1 {
            return Ok(Some(PlannedInstant::Precomputed(
                self.absent_over_time_via_interpreter(tenant, call, time_ms)
                    .await?,
            )));
        }
        let arg = call.args.args[0].as_ref();
        // A bare float-only matrix selector rides the fast `eval_range_arg` path
        // (no operator lowering — the shared `eval_range_arg` is reused verbatim).
        // A HISTOGRAM-bearing matrix, a SUBQUERY range, an anchored/smoothed
        // selector, or any other inner shape delegates to the interpreter's
        // `eval_absent_over_time_call` (parity-exact, and the canonical source of
        // the per-shape error), wrapped as `Precomputed` — so the planner is TOTAL.
        let mut inner = arg;
        while let Expr::Paren(paren) = inner {
            inner = paren.expr.as_ref();
        }
        let needs_interpreter = match inner {
            Expr::MatrixSelector(selector) => {
                self.matrix_selector_has_histogram_series(tenant, selector, time_ms)
                    .await?
            }
            _ => true,
        };
        if needs_interpreter {
            return Ok(Some(PlannedInstant::Precomputed(
                self.absent_over_time_via_interpreter(tenant, call, time_ms)
                    .await?,
            )));
        }

        let range = self
            .eval_range_arg(tenant, &call.args.args[0], time_ms, call.func.name)
            .await?;
        if range
            .series
            .iter()
            .any(|series| range_has_samples(series, range.end_ms, range.range_ms))
        {
            return Ok(Some(PlannedInstant::Precomputed(Vec::new())));
        }
        Ok(Some(PlannedInstant::Precomputed(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }])))
    }

    /// Evaluate `absent_over_time(v[range])` for the shapes the fast planner path
    /// declines (a histogram-bearing matrix, a subquery range, an anchored/smoothed
    /// selector) by reusing the shared [`Self::eval_range_arg`] leaf kernel — the
    /// SAME code the tree-walking oracle's `eval_absent_over_time_call` runs — so
    /// the result, and the per-shape / wrong-arity error, are byte-for-byte
    /// identical. This keeps the planner self-recursive (no re-entry into the
    /// interpreter dispatch) while still routing these shapes through `Precomputed`.
    async fn absent_over_time_via_interpreter(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Vec<InstantSample>> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let range = self
            .eval_range_arg(tenant, arg, time_ms, call.func.name)
            .await?;
        if range
            .series
            .iter()
            .any(|series| range_has_samples(series, range.end_ms, range.range_ms))
        {
            return Ok(Vec::new());
        }
        Ok(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }])
    }

    /// Plan an `Expr::Binary` (arithmetic / comparison / set operator) onto the
    /// operator path: recurse both operands through the planner, assemble each to
    /// an [`InstantValue`], then apply the **shared** combine routine
    /// ([`combine_instant_binary`]) in pure Rust. The result is returned as a
    /// [`PlannedInstant::Precomputed`] vector (or, for a scalar∘scalar fold, a
    /// precomputed single-element scalar carried inside the vector shape — see
    /// below). Because the same combine routine backs the interpreter
    /// ([`Self::eval_instant_binary`]), the operator path matches Prometheus by
    /// construction for every supported form: vector∘scalar, scalar∘vector,
    /// one-to-one vector∘vector (with `on`/`ignoring` and `bool`), `group_left` /
    /// `group_right` (with copied labels), and the `and`/`or`/`unless` set ops.
    ///
    /// Both operands must be planner-supported. A scalar operand
    /// (`value_type() == Scalar`) folds via the interpreter's pure scalar
    /// evaluation — scalars carry no NaN-staleness subtlety, so this is
    /// parity-exact. A vector operand recurses [`Self::plan_instant_expr`] and is
    /// assembled (applying that shape's own drop semantics). If either operand is
    /// not planner-supported (the recurse returns `None`), histogram-bearing, or a
    /// non-instant type (matrix / string), the whole binary returns `None`
    /// (interpreter fallback). A scalar∘scalar fold yields a `Scalar` result,
    /// carried through [`PlannedInstant::PrecomputedScalar`] — both operands are
    /// folded via the interpreter's pure scalar path, so it is parity-exact.
    async fn plan_binary_expr(
        &self,
        tenant: &str,
        binary: &BinaryExpr,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let Some(lhs) = self
            .plan_binary_operand(tenant, &binary.lhs, time_ms)
            .await?
        else {
            return Ok(None);
        };
        let Some(rhs) = self
            .plan_binary_operand(tenant, &binary.rhs, time_ms)
            .await?
        else {
            return Ok(None);
        };

        match combine_instant_binary(binary, lhs, rhs, time_ms)? {
            QueryResult::InstantVector(samples) => Ok(Some(PlannedInstant::Precomputed(samples))),
            // A scalar∘scalar fold: carry the constant through the scalar planned
            // result. Both operands were folded via the interpreter's pure scalar
            // path, so this matches the interpreter exactly.
            QueryResult::Scalar { ts_ms, value } => {
                Ok(Some(PlannedInstant::PrecomputedScalar { ts_ms, value }))
            }
            // A string / matrix combine result cannot be produced by a binary op;
            // fall back defensively.
            _ => Ok(None),
        }
    }

    /// Evaluate one binary operand into an [`InstantValue`] via the planner.
    ///
    /// A scalar-typed operand is folded via the interpreter's pure scalar path
    /// (parity-exact — scalars have no staleness/NaN-window subtlety). A
    /// vector-typed operand recurses [`Self::plan_instant_expr`] and is assembled
    /// to an instant vector. Returns `None` (caller falls back) for a
    /// non-planner-supported vector operand or a non-instant operand type
    /// (matrix / string).
    fn plan_binary_operand<'a>(
        &'a self,
        tenant: &'a str,
        operand: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<Option<InstantValue>>> {
        async move {
            match operand.value_type() {
                ValueType::Scalar => {
                    let QueryResult::Scalar { value, .. } =
                        self.plan_and_resolve(tenant, operand, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    Ok(Some(InstantValue::Scalar(value)))
                }
                ValueType::Vector => {
                    let Some(planned) = self.plan_instant_expr(tenant, operand, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    let QueryResult::InstantVector(samples) =
                        self.assemble_planned_instant(planned, time_ms).await?
                    else {
                        return Ok(None);
                    };
                    Ok(Some(InstantValue::Vector(samples)))
                }
                ValueType::Matrix | ValueType::String => Ok(None),
            }
        }
        .boxed()
    }

    /// Plan an `Expr::Aggregate` onto an inner planner plan wrapped in a
    /// `DataFusion` aggregate, when the op is a simple float aggregation and the
    /// inner expression is itself planner-supported and float-only.
    ///
    /// Returns `None` (interpreter fallback) for param aggregations, histogram
    /// inputs, or any inner expression the recursive planner does not support.
    async fn plan_simple_aggregate_expr(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // Param ops (topk/bottomk/quantile/count_values/limitk/limit_ratio) and
        // stddev/stdvar are out of scope; only the no-param simple aggregations
        // lower onto the operator path. `count_values` carries a string param;
        // stddev/stdvar have no param but are excluded by op below.
        let Some(op) = simple_aggregate_op(aggregate.op) else {
            return Ok(None);
        };
        if aggregate.param.is_some() {
            return Ok(None);
        }
        let Some(grouping) = aggregate_grouping(aggregate.modifier.as_ref()) else {
            // Aggregation with no by/without modifier collapses every series into
            // a single group, exactly like `by ()`.
            return self
                .plan_aggregate_with_grouping(
                    tenant,
                    aggregate,
                    op,
                    Grouping::By(Vec::new()),
                    time_ms,
                )
                .await;
        };
        self.plan_aggregate_with_grouping(tenant, aggregate, op, grouping, time_ms)
            .await
    }

    async fn plan_aggregate_with_grouping(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        op: SimpleAggregateOp,
        grouping: Grouping,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // `sum`/`avg` accumulate per-series FLOATS, so their result depends on the
        // accumulation order. DataFusion's hash-aggregate folds the group members
        // in a non-deterministic, parallel order, which flickers run-to-run by a
        // ULP and can flip a fold-produced NaN's sign bit — diverging from the
        // interpreter's stable fold. Route both ops through the SHARED
        // `apply_simple_aggregate` kernel (the exact free function the interpreter
        // uses, folding a fixed `Vec` in a deterministic order), even for a float
        // `Operator` inner. The audit proved this path is bit-exact with the
        // interpreter (incl. `sum by(g)(rate(...))`). `min`/`max` already use the
        // order-independent `prom_min`/`prom_max` UDAFs and `count`/`group` are
        // exact, so they stay on the DataFusion fast path (no regression).
        if matches!(op, SimpleAggregateOp::Sum | SimpleAggregateOp::Avg) {
            return self
                .plan_simple_aggregate_via_kernel(tenant, aggregate, op, time_ms)
                .await;
        }

        // Recurse into the inner expression. If it is not planner-supported, fall
        // back to the interpreter for the whole aggregation — it cannot run on
        // the operator path without its input.
        let Some(inner) = self
            .plan_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?
        else {
            return Ok(None);
        };
        // A float-only inner lowers to an *operator* plan: keep the DataFusion-
        // aggregate fast path (no regression). Anything else — a histogram-
        // bearing inner (`Precomputed`), a label-rewrite / ordering inner
        // (`Precomputed`), or a scalar inner (`PrecomputedScalar`, which an
        // aggregation cannot consume) — is assembled to a `Vec<InstantSample>`
        // and reduced through the shared interpreter kernel, returned verbatim as
        // `Precomputed`. This is the histogram-aware route: `count`/`group` count
        // every sample and `min`/`max`/`stddev`/`stdvar` ignore histogram samples
        // — exactly as the interpreter's `apply_simple_aggregate` does.
        let PlannedInstant::Operator(inner) = inner else {
            return self
                .plan_simple_aggregate_via_kernel(tenant, aggregate, op, time_ms)
                .await;
        };
        let OperatorInstant {
            ctx,
            plan: inner_plan,
            ..
        } = *inner;
        let plan = plan_simple_aggregate(inner_plan, op, &grouping)?;
        Ok(Some(PlannedInstant::operator(
            ctx,
            plan,
            BTreeMap::new(),
            InstantShape::Aggregate,
        )))
    }

    /// Reduce a simple aggregation over a **histogram-bearing** (or otherwise
    /// `Precomputed`) inner through the shared interpreter kernel, returning the
    /// finished vector as [`PlannedInstant::Precomputed`].
    ///
    /// Assembles the inner expression to a `Vec<InstantSample>` that carries
    /// native histograms as `SampleValue::Histogram` (via
    /// [`Self::histogram_fold_inner_vector`], the same direct-selection helper the
    /// native histogram folds use), then applies [`apply_simple_aggregate`] — the
    /// exact same free function that backs the interpreter
    /// ([`Self::eval_instant_aggregate`]) — so the operator path matches
    /// Prometheus by construction (sum/avg merge, count/group count, min/max/
    /// stddev/stdvar ignore histograms, mixed float+histogram groups dropped).
    ///
    /// Returns `None` (interpreter fallback) only when the inner cannot be
    /// assembled (an inner expression the recursive planner cannot evaluate).
    async fn plan_simple_aggregate_via_kernel(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        op: SimpleAggregateOp,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, &aggregate.expr, time_ms)
            .await?
        else {
            return Ok(None);
        };
        // `apply_simple_aggregate` reads the `by`/`without` selection straight off
        // `aggregate.modifier` (the same modifier the resolved `Grouping` was
        // derived from, covering the empty `by ()` and no-modifier global cases),
        // so the result is byte-identical to the interpreter regardless of how the
        // DataFusion fast path would have shaped its grouping.
        let aggregated = apply_simple_aggregate(
            samples,
            simple_aggregate_op_to_aggregate_op(op),
            aggregate.modifier.as_ref(),
            time_ms,
        )?;
        Ok(Some(PlannedInstant::Precomputed(aggregated)))
    }

    /// Plan a **parameterized** aggregation
    /// (`topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`) onto the
    /// operator path: recurse the inner instant-vector expression through the
    /// planner, assemble it to a `Vec<InstantSample>` (preserving genuine NaN and
    /// the full labelset, including `__name__`), then apply the **shared**
    /// interpreter routine in pure Rust and return the result as a
    /// [`PlannedInstant::Precomputed`]. Because the same `apply_*` free function
    /// backs the interpreter ([`Self::eval_instant_aggregate`] and its callees),
    /// the operator path matches Prometheus by construction.
    ///
    /// The experimental `limitk`/`limit_ratio` ops are also handled here: their
    /// scalar parameter is resolved through the SAME interpreter helpers
    /// ([`Self::eval_limitk_parameter`] / [`Self::eval_limit_ratio_parameter`],
    /// including `limit_ratio`'s deduplicated `InvalidRatioWarning`), a `0`
    /// parameter short-circuits to the empty vector *before* the inner is
    /// evaluated (matching the interpreter), and the SHARED selection kernel
    /// ([`apply_limitk_aggregate`] / [`apply_limit_ratio_aggregate`]) is applied.
    ///
    /// An invalid / non-literal / out-of-range parameter is a hard error: each arm
    /// raises the SAME canonical [`PromqlError`] the interpreter does (propagated,
    /// never swallowed into `Ok(None)` — the planner is total, so an `Ok(None)` on
    /// a genuine validation error would surface as the generic "planner returned no
    /// result" message instead of the real error).
    ///
    /// Returns `None` (interpreter fallback) only for:
    /// - a non-param op (handled by [`Self::plan_simple_aggregate_expr`]),
    /// - an inner expression the recursive planner cannot evaluate, or a
    ///   histogram-bearing inner.
    #[allow(clippy::too_many_lines)]
    async fn plan_param_aggregate_expr(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        match aggregate.op.id() {
            T_TOPK | T_BOTTOMK => {
                // A non-literal / negative / non-integer k is a hard error: raise
                // the SAME canonical message the interpreter's `aggregate_k(..)?`
                // does (propagate, do not swallow into `Ok(None)` — the planner is
                // total, so `Ok(None)` would surface the generic "planner returned
                // no result" error instead of the real validation error).
                let k = aggregate_k(aggregate)?;
                let Some(samples) = self
                    .param_aggregate_inner_vector(tenant, &aggregate.expr, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(apply_k_aggregate(
                    samples,
                    aggregate.op,
                    k,
                    aggregate.modifier.as_ref(),
                ))))
            }
            T_QUANTILE => {
                // A non-numeric / missing phi is a hard error: raise the SAME
                // canonical message the interpreter's `aggregate_quantile(..)?`
                // does (propagate, do not swallow into `Ok(None)`, which the total
                // planner would surface as the generic "planner returned no
                // result" error). An out-of-range / NaN phi is NOT an error —
                // `apply_quantile_aggregate` returns signed `±Inf` / `NaN` plus an
                // `InvalidQuantileWarning`, matching Prometheus.
                let quantile = aggregate_quantile(aggregate)?;
                let Some(samples) = self
                    .param_aggregate_inner_vector(tenant, &aggregate.expr, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(apply_quantile_aggregate(
                    samples,
                    quantile,
                    aggregate.modifier.as_ref(),
                    time_ms,
                ))))
            }
            T_COUNT_VALUES => {
                // The label-name parameter must be a string literal; a missing or
                // non-string param is a hard error. Raise the SAME canonical
                // messages the interpreter's `eval_count_values_aggregate` does
                // (the total planner would otherwise surface the generic "planner
                // returned no result" error).
                let Some(param) = &aggregate.param else {
                    return Err(PromqlError::Plan(
                        "count_values requires a label-name parameter".to_string(),
                    ));
                };
                let Expr::StringLiteral(label_name) = param.as_ref() else {
                    return Err(PromqlError::Plan(
                        "count_values label-name parameter must be a string".to_string(),
                    ));
                };
                let Some(samples) = self
                    .param_aggregate_inner_vector(tenant, &aggregate.expr, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(
                    apply_count_values_aggregate(
                        samples,
                        &label_name.val,
                        aggregate.modifier.as_ref(),
                        time_ms,
                    )?,
                )))
            }
            T_STDDEV | T_STDVAR => {
                // stddev/stdvar carry no parameter; a stray one is an interpreter
                // error, so fall back.
                if aggregate.param.is_some() {
                    return Ok(None);
                }
                let op = if aggregate.op.id() == T_STDDEV {
                    AggregateOp::Stddev
                } else {
                    AggregateOp::Stdvar
                };
                let Some(samples) = self
                    .param_aggregate_inner_vector(tenant, &aggregate.expr, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(
                    apply_stddev_stdvar_aggregate(
                        samples,
                        op,
                        aggregate.modifier.as_ref(),
                        time_ms,
                    ),
                )))
            }
            // `limitk(k, v)` (experimental): resolve `k` exactly as the
            // interpreter does (short-circuiting `k==0` to the empty vector BEFORE
            // evaluating the inner), then apply the SHARED `apply_limitk_aggregate`
            // kernel over the planner-assembled inner. A non-scalar / non-integer
            // `k` is a hard interpreter error; fall back so it raises the identical
            // message.
            #[cfg(feature = "experimental-functions")]
            T_LIMITK => {
                // A non-integer / non-resolvable `k` is a hard error: propagate the
                // SAME canonical message the interpreter's `eval_limitk_parameter`
                // raises (do not swallow into `Ok(None)`).
                let k = self
                    .eval_limitk_parameter(tenant, aggregate, time_ms)
                    .await?;
                if k == 0 {
                    return Ok(Some(PlannedInstant::Precomputed(Vec::new())));
                }
                let Some(samples) = self
                    .param_aggregate_inner_vector(tenant, &aggregate.expr, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(apply_limitk_aggregate(
                    samples,
                    k,
                    aggregate.modifier.as_ref(),
                ))))
            }
            // `limit_ratio(ratio, v)` (experimental): resolve and cap `ratio`
            // exactly as the interpreter does — this is where the
            // `InvalidRatioWarning` is emitted, reproduced here verbatim — and
            // short-circuit `ratio==0` BEFORE evaluating the inner, then apply the
            // SHARED `apply_limit_ratio_aggregate` kernel. A NaN ratio is a hard
            // interpreter error; fall back so it raises the identical message.
            #[cfg(feature = "experimental-functions")]
            T_LIMIT_RATIO => {
                // A NaN / non-resolvable ratio is a hard error: propagate the SAME
                // canonical message the interpreter's `eval_limit_ratio_parameter`
                // raises (do not swallow into `Ok(None)`). The capped-ratio
                // `InvalidRatioWarning` is still emitted by that shared helper.
                let ratio = self
                    .eval_limit_ratio_parameter(tenant, aggregate, time_ms)
                    .await?;
                if ratio == 0.0 {
                    return Ok(Some(PlannedInstant::Precomputed(Vec::new())));
                }
                let Some(samples) = self
                    .param_aggregate_inner_vector(tenant, &aggregate.expr, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(
                    apply_limit_ratio_aggregate(samples, ratio),
                )))
            }
            // In a NON-experimental build, `limitk`/`limit_ratio` must raise the
            // SAME `requires the experimental-functions feature` error the
            // tree-walking oracle raises. Raise it directly planner-side (the
            // canonical message, byte-for-byte identical to the oracle's arm).
            #[cfg(not(feature = "experimental-functions"))]
            T_LIMITK => Err(PromqlError::Unsupported(
                "aggregation `limitk` requires the experimental-functions feature".to_string(),
            )),
            #[cfg(not(feature = "experimental-functions"))]
            T_LIMIT_RATIO => Err(PromqlError::Unsupported(
                "aggregation `limit_ratio` requires the experimental-functions feature".to_string(),
            )),
            // Every other op stays on the interpreter.
            _ => Ok(None),
        }
    }

    /// Evaluate the inner instant-vector argument of a parameterized aggregation
    /// into a `Vec<InstantSample>`, preserving genuine (non-stale) NaN, the full
    /// labelset (including `__name__`), and — crucially — native-histogram series
    /// as `SampleValue::Histogram` rows. These are exactly the samples the
    /// interpreter would aggregate. Returns `None` (caller falls back) only for an
    /// inner expression the recursive planner cannot evaluate.
    ///
    /// Shares [`Self::histogram_fold_inner_vector`], which selects a bare instant-
    /// vector selector directly through the interpreter's own
    /// [`Self::eval_instant_selector`] (carrying histogram samples) and recurses
    /// every other plannable inner expression. Feeding histogram-bearing samples
    /// is correct for every param op: `topk`/`bottomk`/`quantile`/`stddev`/
    /// `stdvar` IGNORE histogram samples (their shared `apply_*` routines skip
    /// them), and `count_values` formats a histogram value as its JSON label
    /// value — all matching the interpreter byte-for-byte.
    async fn param_aggregate_inner_vector(
        &self,
        tenant: &str,
        value_arg: &Expr,
        time_ms: i64,
    ) -> Result<Option<Vec<InstantSample>>> {
        self.histogram_fold_inner_vector(tenant, value_arg, time_ms)
            .await
    }

    /// Execute a planned instant query and assemble its output batches into an
    /// [`InstantVector`](QueryResult::InstantVector), reading each shape's
    /// columns per [`InstantShape`].
    async fn assemble_planned_instant(
        &self,
        planned: PlannedInstant,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let operator = match planned {
            // A label-rewrite / ordering transform already produced its instant
            // vector; return it verbatim (no operator plan to execute).
            PlannedInstant::Precomputed(samples) => {
                return Ok(QueryResult::InstantVector(samples));
            }
            // A scalar-returning utility (`time`/`pi`/`scalar`/argless calendar) or
            // a scalar∘scalar fold already computed its value; return it verbatim.
            PlannedInstant::PrecomputedScalar { ts_ms, value } => {
                return Ok(QueryResult::Scalar { ts_ms, value });
            }
            // A top-level string literal already computed its value; return it
            // verbatim.
            PlannedInstant::PrecomputedString { ts_ms, value } => {
                return Ok(QueryResult::Str { ts_ms, value });
            }
            // A top-level raw matrix selector / subquery already materialized its
            // range vector; return it verbatim.
            PlannedInstant::PrecomputedMatrix(series) => {
                return Ok(QueryResult::RangeMatrix(series));
            }
            PlannedInstant::Operator(operator) => operator,
        };
        let OperatorInstant {
            ctx,
            plan,
            labels_by_fp,
            shape,
        } = *operator;
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        match shape {
            InstantShape::Selector => Ok(assemble_selector_batches(&batches, &labels_by_fp)?),
            InstantShape::RateProjection => {
                Ok(assemble_rate_batches(&batches, &labels_by_fp, time_ms)?)
            }
            InstantShape::OverTimeProjection {
                preserve_metric_name,
            } => Ok(assemble_over_time_batches(
                &batches,
                &labels_by_fp,
                time_ms,
                preserve_metric_name,
            )?),
            InstantShape::Aggregate => Ok(assemble_aggregate_batches(&batches, time_ms)?),
            InstantShape::ScalarMath => Ok(assemble_scalar_math_batches(&batches, time_ms)?),
        }
    }

    /// Plan a sub-expression through the recursive operator planner and assemble
    /// it into a [`QueryResult`] — the production resolver the `plan_*` helpers
    /// use to evaluate their sub-trees (scalar args, binary/unary operands,
    /// aggregate inners, function inputs). This makes the planner fully
    /// self-recursive: it never re-enters the tree-walking interpreter, which is
    /// retained solely as the `#[cfg(test)]` differential parity oracle.
    ///
    /// [`Self::plan_instant_expr`] is proven total — it returns `Ok(Some(_))` for
    /// every expression it accepts and `Err` for the invalid ones — so the
    /// `Ok(None)` arm is unreachable and maps to an internal [`PromqlError::Plan`].
    async fn plan_and_resolve(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let Some(planned) = self.plan_instant_expr(tenant, expr, time_ms).await? else {
            return Err(PromqlError::Plan(format!(
                "planner returned no result for a total sub-expression: {expr}"
            )));
        };
        self.assemble_planned_instant(planned, time_ms).await
    }

    /// Tree-walking differential test oracle — NOT compiled into shipped builds.
    ///
    /// `eval_instant_expr` and the `eval_instant_call`/`eval_instant_aggregate`/
    /// `eval_instant_binary`/`eval_instant_unary` dispatch plus the whole
    /// `eval_*_call` / `eval_*_aggregate` family below it form the recursive
    /// tree-walking interpreter. The production engine no longer uses any of it:
    /// `query_instant` / `query_range` route solely through the recursive operator
    /// planner ([`Self::plan_instant_expr`]), and every planner sub-expression is
    /// resolved by [`Self::plan_and_resolve`] — the planner is fully self-recursive.
    ///
    /// This dispatch is retained ONLY behind `#[cfg(test)]` as the differential
    /// parity oracle: the `*_planner_path_matches_interpreter` tests assert the
    /// self-recursive planner produces byte-for-byte the same result the tree-walker
    /// would. Because it is gated `#[cfg(test)]`, it is excluded from
    /// `cargo build` (default and `--features experimental-functions`), proving the
    /// production engine is interpreter-free.
    ///
    /// The genuine leaf KERNELS the planner shares with this oracle
    /// (`eval_instant_selector`, `eval_matrix_selector`, `eval_subquery`,
    /// `eval_range_arg`, `eval_smoothed_instant_selector`) stay in production and
    /// are deliberately NOT gated.
    #[cfg(test)]
    fn eval_instant_expr<'a>(
        &'a self,
        tenant: &'a str,
        expr: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<QueryResult>> {
        async move {
            match expr {
                Expr::NumberLiteral(number) => Ok(QueryResult::Scalar {
                    ts_ms: time_ms,
                    value: number.val,
                }),
                Expr::StringLiteral(s) => Ok(QueryResult::Str {
                    ts_ms: time_ms,
                    value: s.val.clone(),
                }),
                Expr::VectorSelector(vs) => self.eval_instant_selector(tenant, vs, time_ms).await,
                Expr::Aggregate(aggregate) => {
                    self.eval_instant_aggregate(tenant, aggregate, time_ms)
                        .await
                }
                Expr::Binary(binary) => self.eval_instant_binary(tenant, binary, time_ms).await,
                Expr::Call(call) => self.eval_instant_call(tenant, call, time_ms).await,
                Expr::Unary(unary) => self.eval_instant_unary(tenant, unary, time_ms).await,
                Expr::Paren(paren) => self.eval_instant_expr(tenant, &paren.expr, time_ms).await,
                Expr::Extension(extension) => {
                    let Some(extended) = extension
                        .expr
                        .as_any()
                        .downcast_ref::<ExtendedSelectorExpr>()
                    else {
                        return Err(PromqlError::Unsupported(format!(
                            "expression not implemented yet: {expr}"
                        )));
                    };
                    let Some(Expr::VectorSelector(selector)) = extended.child() else {
                        return Err(PromqlError::Unsupported(format!(
                            "expression not implemented yet: {expr}"
                        )));
                    };
                    match extended.modifier() {
                        ExtendedSelectorModifier::Smoothed => {
                            self.eval_smoothed_instant_selector(tenant, selector, time_ms)
                                .await
                        }
                        ExtendedSelectorModifier::Anchored => Err(PromqlError::Unsupported(
                            "anchored modifier is not valid on instant-vector selectors"
                                .to_string(),
                        )),
                    }
                }
                Expr::MatrixSelector(ms) => self
                    .eval_matrix_selector(tenant, ms, time_ms, time_ms, None)
                    .await
                    .map(QueryResult::RangeMatrix),
                Expr::Subquery(subquery) => self
                    .eval_subquery(tenant, subquery, time_ms)
                    .await
                    .map(QueryResult::RangeMatrix),
            }
        }
        .boxed()
    }

    /// Evaluate a range query over `[start_ms, end_ms]`.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    pub async fn query_range(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult> {
        self.query_range_with_annotations(tenant, query, start_ms, end_ms, step_ms)
            .await
            .map(|(result, _)| result)
    }

    /// Evaluate a range query over `[start_ms, end_ms]`, returning any
    /// warnings/infos raised during evaluation alongside the result.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    pub async fn query_range_with_annotations(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<(QueryResult, Annotations)> {
        ANNOTATIONS
            .scope(RefCell::new(Annotations::new()), async move {
                let result = self
                    .eval_range_query(tenant, query, start_ms, end_ms, step_ms)
                    .await?;
                let annotations = ANNOTATIONS.with(|sink| sink.borrow().clone());
                Ok((result, annotations))
            })
            .await
    }

    async fn eval_range_query(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult> {
        if step_ms <= 0 {
            return Err(PromqlError::Plan("step must be positive".to_string()));
        }
        if end_ms < start_ms {
            return Err(PromqlError::Plan("end must be >= start".to_string()));
        }

        let expr = parse_promql_with_duration_context(
            query,
            DurationExprContext::range(start_ms, end_ms, step_ms),
        )?;
        let mut expr = &expr;
        while let Expr::Paren(paren) = expr {
            expr = &paren.expr;
        }

        // A plannable instant expression (a bare selector, a scalar expression,
        // an aggregation/binary/call/unary over those, …) routes through the
        // per-step operator planner. The planner is **total** over the step
        // grid, so `eval_range_via_planner_scoped` always returns `Ok(Some(..))`
        // for a plannable shape; an `Ok(None)` would be a planner bug, surfaced
        // as an internal error rather than silently diverging.
        if range_expr_routes_through_planner(expr) {
            let Some(series) = self
                .eval_range_via_planner_scoped(tenant, expr, start_ms, end_ms, step_ms)
                .await?
            else {
                return Err(PromqlError::Plan(
                    "planner returned no result for a plannable range query".to_string(),
                ));
            };
            return Ok(QueryResult::RangeMatrix(series));
        }

        // The only non-plannable top-level range shapes are a raw matrix
        // selector and a subquery, both of which yield a range vector directly
        // from the shared kernels (with the range query's `[start, end]`
        // bounds). Every other shape is plannable and handled above.
        match expr {
            Expr::MatrixSelector(ms) => self
                .eval_matrix_selector(tenant, ms, start_ms, end_ms, None)
                .await
                .map(QueryResult::RangeMatrix),
            Expr::Subquery(subquery) => self
                .eval_subquery(tenant, subquery, end_ms)
                .await
                .map(QueryResult::RangeMatrix),
            other => Err(PromqlError::Unsupported(format!(
                "range expression not implemented yet: {other}"
            ))),
        }
    }

    /// Per-step planner range driver, scoped in `QUERY_RANGE_CONTEXT` so any
    /// nested duration-helper scalar folds (`step()`/`start()`/…, experimental)
    /// resolve to the query's range grid, AND in `AT_MODIFIER_BOUNDS` so a bare
    /// top-level selector's `@ start()` / `@ end()` resolves to the query's range
    /// bounds.
    #[cfg(feature = "experimental-functions")]
    async fn eval_range_via_planner_scoped(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Option<Vec<RangeSeries>>> {
        AT_MODIFIER_BOUNDS
            .scope(
                AtModifierBounds { start_ms, end_ms },
                QUERY_RANGE_CONTEXT.scope(
                    QueryRangeContext {
                        start: start_ms,
                        end: end_ms,
                        step: step_ms,
                    },
                    self.eval_range_via_planner(tenant, expr, start_ms, end_ms, step_ms),
                ),
            )
            .await
    }

    /// Per-step planner range driver, scoped in `AT_MODIFIER_BOUNDS` so a bare
    /// top-level selector's `@ start()` / `@ end()` resolves to the query's range
    /// bounds.
    #[cfg(not(feature = "experimental-functions"))]
    async fn eval_range_via_planner_scoped(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Option<Vec<RangeSeries>>> {
        AT_MODIFIER_BOUNDS
            .scope(
                AtModifierBounds { start_ms, end_ms },
                self.eval_range_via_planner(tenant, expr, start_ms, end_ms, step_ms),
            )
            .await
    }

    /// Evaluate a plannable instant `expr` over the step grid through the
    /// operator planner, stitching the per-step instant vectors / scalars into a
    /// [`RangeMatrix`](QueryResult::RangeMatrix).
    ///
    /// Walks the step grid — `step` from `start_ms` to `end_ms` inclusive,
    /// advancing by `step_ms` with saturating add — and stitches: samples are
    /// grouped by labelset fingerprint into one series each, points appended in
    /// step order, gaps left implicit (a step where a series has no value
    /// produces no point), and a scalar expr folded into a single empty-labelset
    /// series. The output is iterated in fingerprint order (`BTreeMap` keys),
    /// matching Prometheus byte-for-byte.
    ///
    /// Returns `Ok(None)` only if some step's [`Self::plan_instant_expr`] returns
    /// `None`. The planner is total, so for a plannable `expr` this never
    /// happens; the caller treats an `Ok(None)` as an internal planner bug.
    async fn eval_range_via_planner(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Option<Vec<RangeSeries>>> {
        // Backstop the resolution cap before the per-step loop, so an abusive
        // subquery resolution (e.g. `last_over_time(up[1000d:1ms])`) errors
        // rather than looping ~1e11 times. The HTTP front-gate enforces the same
        // cap on the top-level query window; this guards the engine itself
        // (including subqueries, whose grid the front-gate never sees).
        check_resolution_points(start_ms, end_ms, step_ms)?;
        // Scan the union of all per-step lookback windows once per matcher set,
        // shared across the step loop via the RANGE_SCAN_CACHE task-local. The
        // union starts at the first step's lookback floor (`start - lookback`)
        // and ends at the last step (`end`). Scans outside this window
        // (offset/@-modifier or a `[range]` longer than the lookback) fall back to
        // a direct scan inside `scan_float_rows`, so results are unchanged.
        let cache: RangeScanCache =
            std::sync::Arc::new(std::sync::Mutex::new(RangeScanCacheInner {
                full_start_ms: start_ms.saturating_sub(self.opts.lookback_delta_ms),
                full_end_ms: end_ms,
                floats: std::collections::HashMap::new(),
                histograms: std::collections::HashMap::new(),
                labels: std::collections::HashMap::new(),
            }));
        RANGE_SCAN_CACHE
            .scope(cache, async move {
                let mut by_fp: BTreeMap<SeriesFingerprint, RangeSeries> = BTreeMap::new();
                let mut step = start_ms;
                while step <= end_ms {
                    let Some(planned) = self.plan_instant_expr(tenant, expr, step).await? else {
                        // This step's shape is not planner-supported (e.g. a histogram
                        // series appeared in-window). Abandon the operator path for the
                        // whole query so the interpreter produces a consistent result.
                        return Ok(None);
                    };
                    match self.assemble_planned_instant(planned, step).await? {
                        QueryResult::InstantVector(samples) => {
                            for sample in samples {
                                let fp = sample.labels.fingerprint();
                                by_fp
                                    .entry(fp)
                                    .or_insert_with(|| RangeSeries {
                                        labels: sample.labels.clone(),
                                        samples: Vec::new(),
                                    })
                                    .samples
                                    .push((step, sample.value));
                            }
                        }
                        QueryResult::Scalar { value, .. } => {
                            let labels = Labels::new();
                            by_fp
                                .entry(labels.fingerprint())
                                .or_insert_with(|| RangeSeries {
                                    labels,
                                    samples: Vec::new(),
                                })
                                .samples
                                .push((step, SampleValue::Float(value)));
                        }
                        QueryResult::Str { .. } | QueryResult::RangeMatrix(_) => {
                            // The planner only ever assembles an instant vector or a
                            // scalar; neither of these can arise. Fall back defensively.
                            return Ok(None);
                        }
                    }
                    step = step.saturating_add(step_ms);
                }
                Ok(Some(by_fp.into_values().collect()))
            })
            .await
    }

    /// Force the per-step planner range driver, bypassing the
    /// [`range_expr_routes_through_planner`] production gate. The parity-test
    /// seam: it lets the differential test drive *every* range case (including a
    /// bare top-level selector, which the production gate keeps on the
    /// interpreter) through the operator path and compare it to the interpreter's
    /// `query_range`, proving parity before the gate is trusted.
    #[cfg(test)]
    async fn eval_range_via_planner_forced(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<QueryResult> {
        let expr = parse_promql_with_duration_context(
            query,
            DurationExprContext::range(start_ms, end_ms, step_ms),
        )?;
        let mut expr = &expr;
        while let Expr::Paren(paren) = expr {
            expr = &paren.expr;
        }
        let series = self
            .eval_range_via_planner_scoped(tenant, expr, start_ms, end_ms, step_ms)
            .await?
            .expect("forced planner range driver returned None");
        Ok(QueryResult::RangeMatrix(series))
    }

    /// Evaluate a bare instant-vector selector through the `DataFusion`
    /// `LogicalPlan` operator chain (`SeriesDivide -> SeriesNormalize ->
    /// InstantManipulate`) instead of the interpreter.
    ///
    /// This is the float-only spine of the interpreter -> operator migration.
    /// The caller only routes here when the selector has no matching histogram
    /// series; histogram selection stays on the interpreter
    /// ([`Self::eval_instant_selector`]). Thin wrapper over the plan-builder
    /// [`Self::plan_instant_selector`] plus the shared assembler; kept as the
    /// parity-test seam.
    #[cfg(test)]
    async fn eval_instant_selector_via_planner(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let planned = self
            .plan_instant_selector(tenant, selector, time_ms)
            .await?;
        self.assemble_planned_instant(planned, time_ms).await
    }

    /// Build (without executing) the instant-vector-selector operator plan: scan
    /// the matched float series over `(eval_time - lookback, eval_time]`,
    /// materialize their labels, and assemble the `SeriesDivide ->
    /// SeriesNormalize -> InstantManipulate` chain.
    async fn plan_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<PlannedInstant> {
        // `@ start()`/`@ end()` resolve to the active range query's bounds (when
        // present in a range query); for an instant query the bounds are absent and
        // a bare `@ start()`/`@ end()` raises the same hard error the interpreter
        // does.
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            current_at_modifier_bounds(),
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;

        // Carry the matched series' labels onto each sample. Stale-NaN markers
        // are intentionally kept here: InstantManipulate drops the selected
        // sample only when it is a stale-NaN marker, which suppresses a series
        // whose latest in-window sample is a stale marker while preserving a
        // genuine NaN value (matching interpreter staleness handling).
        // Pre-filtering markers here would instead reveal an older sample and
        // diverge from Prometheus.
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&row.fp).cloned() else {
                continue;
            };
            samples.push(LabeledSample {
                fp: row.fp,
                labels,
                ts_ms: row.ts_ms,
                value: row.value,
            });
        }

        let InstantSelectorPlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_instant_vector_selector(samples, eval_time_ms, self.opts.lookback_delta_ms)
            .await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::Selector,
        ))
    }

    /// True when the selector matches at least one histogram series in the
    /// instant-selector scan window. Such selectors stay on the interpreter
    /// because the float-only operator chain cannot carry histogram samples.
    async fn selector_has_histogram_series(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<bool> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            current_at_modifier_bounds(),
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        Ok(hist_rows
            .into_iter()
            .any(|row| row.ts_ms > start_ms && row.ts_ms <= eval_time_ms))
    }

    /// True when a matrix selector matches at least one histogram series in its
    /// exact range window `(eval_time - range, eval_time]`. Such selectors stay
    /// on the interpreter; the float-only rate operator chain cannot carry
    /// histogram samples (the interpreter's `range_histogram_sample` handles
    /// them). The window matches `eval_matrix_selector`'s `modifier == None`
    /// scan window exactly (no lookback).
    async fn matrix_selector_has_histogram_series(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
    ) -> Result<bool> {
        let range_ms = duration_ms(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range_ms);
        let matcher_sets = label_matcher_sets(&selector.vs);
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        Ok(hist_rows
            .into_iter()
            .any(|row| row.ts_ms > range_start_ms && row.ts_ms <= eval_end_ms))
    }

    /// Evaluate a top-level `f(selector[range])` rate-family call through the
    /// `DataFusion` operator chain (`SeriesDivide -> SeriesNormalize ->
    /// RangeManipulate -> rate-UDF projection`) instead of the interpreter. Thin
    /// wrapper over [`Self::plan_rate_range`] plus the shared assembler; kept as
    /// the parity-test seam.
    #[cfg(test)]
    async fn eval_rate_range_via_planner(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        kind: RateUdfKind,
    ) -> Result<QueryResult> {
        let planned = self
            .plan_rate_range(tenant, selector, time_ms, kind)
            .await?;
        self.assemble_planned_instant(planned, time_ms).await
    }

    /// Build (without executing) the rate-family range-selector operator plan.
    ///
    /// The range-selector window is exactly `(eval_time - range, eval_time]`,
    /// left-open and right-closed, with **no** 5m lookback — unlike the instant
    /// path, which scans `(eval_time - lookback, eval_time]` and selects a single
    /// sample. This matches Prometheus matrix-selector semantics and the
    /// interpreter's `range_function_sample_from_series`. The window's range
    /// width feeds the UDF as `range_ms`; the eval instant feeds it as the scalar
    /// `timestamp` column, from which the UDF re-derives `range_start = t - range`.
    async fn plan_rate_range(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        kind: RateUdfKind,
    ) -> Result<PlannedInstant> {
        let range_ms = duration_ms(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range_ms);
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;

        // Build per-sample labeled rows over the exact range window. Stale-NaN
        // markers are dropped here, matching `eval_matrix_selector`; genuine NaN
        // is carried through (the operator chain does not filter NaN), as the
        // interpreter does.
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            if row.ts_ms <= range_start_ms || row.ts_ms > eval_end_ms {
                continue;
            }
            if is_stale_nan(row.value) {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&row.fp).cloned() else {
                continue;
            };
            samples.push(RateLabeledSample {
                fp: row.fp,
                labels,
                ts_ms: row.ts_ms,
                value: row.value,
            });
        }

        let RateRangePlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_rate_range_selector(samples, eval_end_ms, range_ms, kind).await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::RateProjection,
        ))
    }

    /// Build (without executing) the `*_over_time` range-selector operator plan.
    ///
    /// Shares the rate path's window semantics: the window is exactly
    /// `(eval_time - range, eval_time]`, left-open right-closed, with **no** 5m
    /// lookback, matching the interpreter's `over_time_sample_from_series`. The
    /// `phi` quantile literal is threaded for `quantile_over_time` and ignored
    /// otherwise.
    async fn plan_over_time_range(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        family: OverTimeFamily,
        phi: f64,
    ) -> Result<PlannedInstant> {
        let range_ms = duration_ms(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range_ms);
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;

        // Build per-sample labeled rows over the exact range window. Stale-NaN
        // markers are dropped here, matching `eval_matrix_selector`; genuine NaN
        // is carried through, as the interpreter does.
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            if row.ts_ms <= range_start_ms || row.ts_ms > eval_end_ms {
                continue;
            }
            if is_stale_nan(row.value) {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&row.fp).cloned() else {
                continue;
            };
            samples.push(OverTimeLabeledSample {
                fp: row.fp,
                labels,
                ts_ms: row.ts_ms,
                value: row.value,
            });
        }

        let OverTimeRangePlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_over_time_range_selector(samples, eval_end_ms, range_ms, family, phi).await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::OverTimeProjection {
                // Only `last_over_time` preserves the metric name; every other
                // family drops it (`OverTimeFn::preserves_metric_name`).
                preserve_metric_name: matches!(family, OverTimeFamily::Last),
            },
        ))
    }

    /// Plan a histogram-bearing rate-family / `*_over_time` matrix-selector call
    /// (`outer(selector[range])`) as a fully-computed [`PlannedInstant::Precomputed`].
    ///
    /// This is the range analog of [`Self::histogram_fold_inner_vector`]: the
    /// float-only operator leaf cannot carry native histograms, so instead of
    /// lowering onto the `RangeManipulate + UDF` chain we assemble the per-series
    /// windowed range vector via the interpreter's own
    /// [`Self::eval_matrix_selector`] — identical by construction — and apply the
    /// **same** shared [`apply_outer_range_fn`] kernel the interpreter's
    /// `eval_*_call` uses. The histogram counter-reset/extrapolation rules
    /// (rate/increase/delta), the float-only `irate`/`idelta` filter, and each
    /// `_over_time` member's histogram behaviour (sum/avg merge; count/last/present
    /// histogram-safe; min/max/stddev/stdvar/quantile ignore histograms) all live
    /// in that kernel, so the result is byte-for-byte the interpreter's.
    ///
    /// The window/`@`/offset resolution mirrors [`Self::eval_range_arg`]'s
    /// matrix-selector arm exactly (`modifier: None` — the `anchored`/`smoothed`
    /// modifiers parse to [`Expr::Extension`], which `match_rate_range_call` /
    /// `match_over_time_range_call` reject, so a matrix selector here never carries
    /// one).
    async fn plan_histogram_range_via_kernel(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        outer: OuterRangeFn,
    ) -> Result<PlannedInstant> {
        let range_ms = duration_ms(selector.range)?;
        let end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let series = self
            .eval_matrix_selector(tenant, selector, time_ms, time_ms, None)
            .await?;
        let range = RangeEval {
            series,
            end_ms,
            range_ms,
            modifier: None,
        };
        Ok(PlannedInstant::Precomputed(apply_outer_range_fn(
            range, outer, time_ms,
        )))
    }

    /// Plan a per-row scalar-math `Call` (`abs`/`ceil`/…/`sgn`, the
    /// trig/hyperbolic family, `round`, the `clamp` family) onto a
    /// `Projection(f(value))` over its evaluated inner instant vector.
    ///
    /// Returns `None` (interpreter fallback) when the arity is wrong, a bound
    /// argument (`round`'s `to_nearest`, `clamp`'s bounds) is not a scalar, the
    /// inner argument is a histogram-bearing selector, or the inner expression
    /// is not planner-supported. The inner vector is sourced either from a
    /// NaN-preserving bare-selector selection (so a genuine, non-stale NaN
    /// sample survives, matching the interpreter) or by assembling a nested
    /// plannable inner expression.
    async fn plan_scalar_math_call(
        &self,
        tenant: &str,
        call: &Call,
        op: ScalarMathOp,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // Resolve `(bounds, value_arg_index)` for this op's call shape. Wrong
        // arity falls back so the interpreter raises the canonical error.
        //
        // The bound arg(s) trail the value arg in PromQL source order
        // (`round(v, to_nearest)`, `clamp(v, min, max)`), but the UDF call
        // convention threads them *ahead* of the value column, so `bounds` is
        // built in UDF order: `[to_nearest]`, `[min]`, `[max]`, `[min, max]`.
        let arg_count = call.args.args.len();
        let bounds_args: &[usize] = match op {
            // `round(v, to_nearest?)`: `to_nearest` defaults to 1.
            ScalarMathOp::Round => match arg_count {
                1 => &[],
                2 => &[1],
                _ => return Ok(None),
            },
            ScalarMathOp::ClampMin | ScalarMathOp::ClampMax => {
                if arg_count == 2 {
                    &[1]
                } else {
                    return Ok(None);
                }
            }
            ScalarMathOp::Clamp => {
                if arg_count == 3 {
                    &[1, 2]
                } else {
                    return Ok(None);
                }
            }
            // Unary fns: exactly one argument.
            _ => {
                if arg_count == 1 {
                    &[]
                } else {
                    return Ok(None);
                }
            }
        };

        // Resolve the scalar bound argument(s). A non-scalar bound falls back to
        // the interpreter. `round` with one argument uses the default `1.0`.
        let mut bounds = Vec::with_capacity(bounds_args.len());
        for &index in bounds_args {
            let QueryResult::Scalar { value, .. } = self
                .plan_and_resolve(tenant, &call.args.args[index], time_ms)
                .await?
            else {
                return Ok(None);
            };
            bounds.push(value);
        }
        if matches!(op, ScalarMathOp::Round) && bounds.is_empty() {
            // `round(v)` -> `to_nearest = 1`.
            bounds.push(1.0);
        }

        // `clamp(v, min, max)` with `min > max` yields the empty vector
        // (`eval_clamp_call`); produce an empty result via an empty leaf.
        if matches!(op, ScalarMathOp::Clamp) && bounds[0] > bounds[1] {
            let ScalarMathPlan { ctx, plan, .. } =
                plan_scalar_math(Vec::new(), op, &bounds).await?;
            return Ok(Some(PlannedInstant::operator(
                ctx,
                plan,
                BTreeMap::new(),
                InstantShape::ScalarMath,
            )));
        }

        // The instant-vector argument is always the first positional arg
        // (`round(v, …)`, `clamp(v, …)`, `abs(v)`). Source the already-evaluated
        // inner samples (genuine NaN preserved).
        let value_arg = &call.args.args[0];
        let Some(samples) = self
            .scalar_math_inner_samples(tenant, value_arg, time_ms)
            .await?
        else {
            return Ok(None);
        };

        let ScalarMathPlan { ctx, plan, .. } = plan_scalar_math(samples, op, &bounds).await?;
        Ok(Some(PlannedInstant::operator(
            ctx,
            plan,
            BTreeMap::new(),
            InstantShape::ScalarMath,
        )))
    }

    /// Plan a `label_replace`/`label_join`/`sort`/`sort_desc`/`sort_by_label`/
    /// `sort_by_label_desc` call onto the operator path: recurse into the inner
    /// instant-vector argument, assemble it (preserving genuine NaN), apply the
    /// pure label-rewrite / ordering transform (shared with the interpreter), and
    /// return the finished vector as a [`PlannedInstant::Precomputed`].
    ///
    /// Returns `None` (interpreter fallback, which then raises the canonical
    /// error) for wrong arity, a non-string label/separator/regex argument, or an
    /// inner expression the recursive planner cannot evaluate. An invalid
    /// `label_replace` regex surfaces here as `Err`, matching the interpreter.
    /// Output-labelset collisions are not checked here: the top-level
    /// `validate_unique_instant_labelsets` enforces them identically for both the
    /// operator and interpreter paths.
    async fn plan_label_ops_call(
        &self,
        tenant: &str,
        call: &Call,
        kind: LabelOpsKind,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // Validate the call shape and extract the string-literal arguments. Any
        // mismatch falls back so the interpreter raises the identical error.
        match kind {
            LabelOpsKind::LabelReplace => {
                if call.args.args.len() != 5 {
                    return Ok(None);
                }
                let (Some(dst), Some(replacement), Some(src), Some(regex)) = (
                    string_literal_value(call, 1),
                    string_literal_value(call, 2),
                    string_literal_value(call, 3),
                    string_literal_value(call, 4),
                ) else {
                    return Ok(None);
                };
                let Some(samples) = self
                    .label_ops_inner_vector(tenant, &call.args.args[0], time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                let out =
                    label_ops::apply_label_replace(samples, &dst, &replacement, &src, &regex)?;
                Ok(Some(PlannedInstant::Precomputed(out)))
            }
            LabelOpsKind::LabelJoin => {
                if call.args.args.len() < 4 {
                    return Ok(None);
                }
                let (Some(dst), Some(separator)) =
                    (string_literal_value(call, 1), string_literal_value(call, 2))
                else {
                    return Ok(None);
                };
                let mut src_labels = Vec::with_capacity(call.args.args.len() - 3);
                for index in 3..call.args.args.len() {
                    let Some(label) = string_literal_value(call, index) else {
                        return Ok(None);
                    };
                    src_labels.push(label);
                }
                let Some(samples) = self
                    .label_ops_inner_vector(tenant, &call.args.args[0], time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                let out = label_ops::apply_label_join(samples, &dst, &separator, &src_labels);
                Ok(Some(PlannedInstant::Precomputed(out)))
            }
            LabelOpsKind::Sort(order) => {
                if call.args.args.len() != 1 {
                    return Ok(None);
                }
                let Some(samples) = self
                    .label_ops_inner_vector(tenant, &call.args.args[0], time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(label_ops::apply_sort(
                    samples, order,
                ))))
            }
            LabelOpsKind::SortByLabel(order) => {
                // `sort_by_label(v, label, ...)` needs the inner vector plus at
                // least one string-literal label name. Wrong arity / a non-string
                // label argument falls back so the interpreter raises the
                // canonical error.
                if call.args.args.len() < 2 {
                    return Ok(None);
                }
                let mut label_names = Vec::with_capacity(call.args.args.len() - 1);
                for index in 1..call.args.args.len() {
                    let Some(label) = string_literal_value(call, index) else {
                        return Ok(None);
                    };
                    label_names.push(label);
                }
                let Some(samples) = self
                    .label_ops_inner_vector(tenant, &call.args.args[0], time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(PlannedInstant::Precomputed(
                    label_ops::apply_sort_by_label(samples, &label_names, order),
                )))
            }
        }
    }

    /// Plan an `info(v [, data_label_selector])` call onto the operator path:
    /// parse the (store-independent) [`InfoContext`], recurse the input vector `v`
    /// through the histogram-aware [`Self::histogram_fold_inner_vector`] (so a
    /// histogram-valued input passes through unchanged, exactly as the interpreter
    /// does), select the `target_info` / custom-selector series through the SAME
    /// interpreter helper ([`Self::info_by_key`]), and apply the **shared**
    /// [`apply_info`] join — returning the finished vector as a
    /// [`PlannedInstant::Precomputed`]. Because the context parse, the info-series
    /// selection, and the join all come from the interpreter's own code, the
    /// operator path matches Prometheus by construction (incl. the latest-sample
    /// conflict resolution, the required-matcher drop, and the `target_info` /
    /// info-metric passthrough rules exercised by the conformance corpus).
    ///
    /// Returns `None` (interpreter fallback) only for an input vector the recursive
    /// planner cannot evaluate. Wrong arity or a non-vector-selector data-label
    /// argument surfaces here as `Err` (via [`parse_info_call`]), and a histogram
    /// info-series match surfaces as `Err` (via [`info_samples_by_identifying_key`])
    /// — both identical to the interpreter.
    async fn plan_info_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let context = parse_info_call(call)?;
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, &call.args.args[0], time_ms)
            .await?
        else {
            return Ok(None);
        };
        let info_by_key = self.info_by_key(tenant, &context, time_ms).await?;
        Ok(Some(PlannedInstant::Precomputed(apply_info(
            samples,
            &info_by_key,
            &context,
        ))))
    }

    /// Plan a `histogram_quantile(phi, v)` call onto the operator path: resolve
    /// `phi` to a scalar, select the inner instant-vector `v` through the
    /// histogram-aware [`Self::histogram_fold_inner_vector`] (carrying native
    /// histograms as `SampleValue::Histogram`), then apply the **shared** fold
    /// ([`apply_histogram_quantile`]) in pure Rust and return a
    /// [`PlannedInstant::Precomputed`]. Because the same fold backs the
    /// interpreter ([`Self::eval_histogram_quantile_call`]), the operator path
    /// matches Prometheus by construction for **both** histogram flavors:
    /// - classic `<metric>_bucket{le}` float-bucket vectors — `le`-bound parsing
    ///   (incl. `+Inf`), bucket-monotonicity forcing, the `<2`-bucket /
    ///   `phi`-out-of-range / negative-first-bucket edge cases, linear
    ///   interpolation, and the `__name__` + `le` label drop;
    /// - native-histogram vectors — the `native_histogram_quantile` path and the
    ///   classic+native mixed-schema warning (emitted via the in-scope annotation
    ///   sink, exactly as the interpreter does).
    ///
    /// Returns `None` (interpreter fallback) for:
    /// - wrong arity (the interpreter then raises the canonical error),
    /// - a non-scalar / non-evaluable `phi` argument (the interpreter raises the
    ///   identical "quantile argument must be a scalar" error), or
    /// - an inner expression the recursive planner cannot evaluate.
    async fn plan_histogram_quantile_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [quantile_arg, vector_arg] = call.args.args.as_slice() else {
            return Ok(None);
        };

        // Resolve `phi` exactly as the interpreter does (a scalar expression). A
        // non-scalar result falls back so the interpreter raises the identical
        // error; `phi` is otherwise passed through verbatim (NaN / out-of-range
        // are handled inside the shared classic fold).
        let QueryResult::Scalar {
            value: quantile, ..
        } = self.plan_and_resolve(tenant, quantile_arg, time_ms).await?
        else {
            return Ok(None);
        };

        // Select the inner bucket vector with native histograms carried as
        // `SampleValue::Histogram` (the direct shared-kernel scan, identical to
        // the interpreter's selection). The shared `apply_histogram_quantile` fold
        // handles classic buckets, native histograms, and the mixed-schema warning
        // uniformly, so a native / mixed inner is parity-exact here — no fallback.
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, vector_arg, time_ms)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(PlannedInstant::Precomputed(apply_histogram_quantile(
            quantile, samples, time_ms,
        )?)))
    }

    /// Plan an experimental `histogram_quantiles(label, v, phi...)` call onto the
    /// operator path: validate arity, resolve the label name and each scalar `phi`
    /// exactly as the interpreter does, select the inner bucket vector `v` through
    /// the histogram-aware [`Self::histogram_fold_inner_vector`], then apply the
    /// **shared** [`apply_histogram_quantiles`] fold and return a
    /// [`PlannedInstant::Precomputed`]. Because the same fold backs the interpreter
    /// ([`Self::eval_histogram_quantiles_call`]), the operator path is parity-exact
    /// for classic and native bucket vectors.
    ///
    /// Returns `None` (interpreter fallback) for wrong arity, a non-string label
    /// argument, a non-scalar `phi`, or an inner expression the recursive planner
    /// cannot evaluate (the interpreter then raises the canonical error).
    #[cfg(feature = "experimental-functions")]
    async fn plan_histogram_quantiles_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        if call.args.args.len() < 3 {
            return Ok(None);
        }
        let Some(label_name) = string_literal_value(call, 1) else {
            return Ok(None);
        };
        let mut quantiles = Vec::with_capacity(call.args.args.len() - 2);
        for index in 2..call.args.args.len() {
            let QueryResult::Scalar { value, .. } = self
                .plan_and_resolve(tenant, &call.args.args[index], time_ms)
                .await?
            else {
                return Ok(None);
            };
            quantiles.push(value);
        }
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, &call.args.args[0], time_ms)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(PlannedInstant::Precomputed(
            apply_histogram_quantiles(samples, &label_name, &quantiles, time_ms)?,
        )))
    }

    /// Plan a native-histogram accessor call
    /// (`histogram_count`/`sum`/`avg`/`stddev`/`stdvar`) onto the operator path:
    /// select the single instant-vector operand through the histogram-aware
    /// [`Self::histogram_fold_inner_vector`] (carrying native histograms as
    /// `SampleValue::Histogram`), then apply the **shared**
    /// [`apply_histogram_accessor`] fold in pure Rust and return a
    /// [`PlannedInstant::Precomputed`]. Because the same fold backs the
    /// interpreter ([`Self::eval_histogram_accessor_call`]) — float rows dropped,
    /// `__name__` dropped, source timestamp kept — the two paths match Prometheus
    /// by construction.
    ///
    /// Returns `None` (interpreter fallback) for wrong arity (the interpreter
    /// raises the canonical error) or an operand the recursive planner cannot
    /// evaluate.
    async fn plan_histogram_accessor_call(
        &self,
        tenant: &str,
        call: &Call,
        accessor: HistogramAccessor,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, arg, time_ms)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(PlannedInstant::Precomputed(apply_histogram_accessor(
            samples, accessor,
        ))))
    }

    /// Plan a `histogram_fraction(lower, upper, v)` call onto the operator path:
    /// resolve the two scalar bounds exactly as the interpreter does, select the
    /// instant-vector operand `v` through the histogram-aware
    /// [`Self::histogram_fold_inner_vector`], then apply the **shared**
    /// [`apply_histogram_fraction`] fold in pure Rust and return a
    /// [`PlannedInstant::Precomputed`]. The same fold backs the interpreter
    /// ([`Self::eval_histogram_fraction_call`]) — handling classic buckets, native
    /// histograms, and the classic+native mixed-schema warning — so the two paths
    /// match Prometheus by construction.
    ///
    /// Returns `None` (interpreter fallback) for wrong arity or a non-scalar /
    /// non-evaluable bound (the interpreter raises the canonical error), or an
    /// operand the recursive planner cannot evaluate.
    async fn plan_histogram_fraction_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        let [lower_arg, upper_arg, vector_arg] = call.args.args.as_slice() else {
            return Ok(None);
        };
        let QueryResult::Scalar { value: lower, .. } =
            self.plan_and_resolve(tenant, lower_arg, time_ms).await?
        else {
            return Ok(None);
        };
        let QueryResult::Scalar { value: upper, .. } =
            self.plan_and_resolve(tenant, upper_arg, time_ms).await?
        else {
            return Ok(None);
        };
        let Some(samples) = self
            .histogram_fold_inner_vector(tenant, vector_arg, time_ms)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(PlannedInstant::Precomputed(apply_histogram_fraction(
            lower, upper, samples, time_ms,
        )?)))
    }

    /// Plan a range/`*_over_time` call whose argument is a **subquery**
    /// (`f(inner[range:resolution] ...)`) onto the operator path.
    ///
    /// The subquery's range vector is built by evaluating its inner instant
    /// expression at each aligned sub-step on the grid covering
    /// `(end - range, end]` with stride `resolution` (default = the engine's
    /// global eval interval) — through the **recursive planner**
    /// ([`Self::eval_range_via_planner`]), so every sub-step matches the
    /// interpreter's per-step `eval_instant_expr` byte-for-byte. The sub-grid
    /// alignment ([`align_subquery_start`]), the resolution default, and the
    /// subquery's `@`/offset are resolved identically to the interpreter's
    /// [`Self::eval_subquery`]. The outer fold is then the **shared**
    /// [`apply_outer_range_fn`] — the same one the interpreter's `eval_*_call`
    /// uses — so the whole evaluation is parity-exact by construction, and the
    /// result is returned as a [`PlannedInstant::Precomputed`].
    ///
    /// Returns `None` (interpreter fallback) when:
    /// - the inner expression is not structurally planner-supported, or any
    ///   sub-step's shape is data-dependently non-plannable (e.g. a histogram
    ///   series appears in-window) — [`Self::eval_range_via_planner`] returns
    ///   `None`, and the whole subquery falls back so the interpreter produces a
    ///   consistent result;
    /// - a `quantile_over_time` `phi` is non-scalar, or a `predict_linear`
    ///   duration / smoothing factor is non-scalar (the interpreter then raises
    ///   the identical canonical error). A NaN / out-of-`[0, 1]` `phi` is NOT a
    ///   fallback: it evaluates to signed `±Inf` / `NaN` plus an
    ///   `InvalidQuantileWarning`, matching Prometheus;
    /// - the subquery step is non-positive (the interpreter raises the canonical
    ///   "subquery step must be positive" error).
    async fn plan_subquery_range_call(
        &self,
        tenant: &str,
        subquery: &SubqueryExpr,
        spec: SubqueryOuterFn<'_>,
        time_ms: i64,
    ) -> Result<Option<PlannedInstant>> {
        // Structural gate: the inner must be a planner-supported, non-subquery
        // instant expression. A non-plannable inner falls back wholesale (the
        // per-sub-step planner would return `None` at the first step anyway).
        if !instant_expr_is_plannable(&subquery.expr) {
            return Ok(None);
        }

        // Resolve any scalar parameters exactly as the matching `eval_*_call`
        // does (at the outer eval time), falling back on invalid input so the
        // interpreter raises the canonical error.
        let outer = match spec {
            SubqueryOuterFn::NoParam(outer) => outer,
            SubqueryOuterFn::QuantileOverTime { phi } => {
                let QueryResult::Scalar { value, .. } =
                    self.plan_and_resolve(tenant, phi, time_ms).await?
                else {
                    return Ok(None);
                };
                // An out-of-range / NaN `phi` is NOT an error: Prometheus returns
                // signed `±Inf` / `NaN` plus an `InvalidQuantileWarning`.
                if !is_valid_quantile(value) {
                    emit_warning(invalid_quantile_warning(value));
                }
                OuterRangeFn::QuantileOverTime(value)
            }
            SubqueryOuterFn::PredictLinear { duration } => {
                let QueryResult::Scalar { value, .. } =
                    self.plan_and_resolve(tenant, duration, time_ms).await?
                else {
                    return Ok(None);
                };
                OuterRangeFn::PredictLinear(value)
            }
            #[cfg(feature = "experimental-functions")]
            SubqueryOuterFn::DoubleExponentialSmoothing { smoothing, trend } => {
                let QueryResult::Scalar {
                    value: smoothing, ..
                } = self.plan_and_resolve(tenant, smoothing, time_ms).await?
                else {
                    return Ok(None);
                };
                let QueryResult::Scalar { value: trend, .. } =
                    self.plan_and_resolve(tenant, trend, time_ms).await?
                else {
                    return Ok(None);
                };
                if validate_smoothing_factor("smoothing factor", smoothing).is_err()
                    || validate_smoothing_factor("trend factor", trend).is_err()
                {
                    return Ok(None);
                }
                OuterRangeFn::DoubleExponentialSmoothing { smoothing, trend }
            }
        };

        // Resolve the subquery grid exactly as `eval_subquery` does: range,
        // resolution (default = global eval interval), the `@`/offset-shifted end,
        // and the step-aligned start.
        let range_ms = duration_ms(subquery.range)?;
        let step_ms = match subquery.step {
            Some(step) => duration_ms(step)?,
            None => self.opts.eval_interval_ms,
        };
        if step_ms <= 0 {
            // The interpreter raises a hard error here; fall back so it does.
            return Ok(None);
        }
        let end_ms = apply_selector_time_modifier(
            time_ms,
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            None,
        )?;
        let start_ms = align_subquery_start(end_ms.saturating_sub(range_ms), step_ms);

        // Build the subquery's range vector through the recursive planner. A
        // `None` here means some sub-step's shape is not planner-supported, so the
        // whole subquery falls back to the interpreter.
        let Some(series) = self
            .eval_range_via_planner(tenant, &subquery.expr, start_ms, end_ms, step_ms)
            .await?
        else {
            return Ok(None);
        };

        // Apply the outer fold via the *same* shared routine the interpreter
        // uses. A subquery range argument always carries `modifier: None` (the
        // `anchored`/`smoothed` modifiers attach to a matrix selector, never a
        // subquery), matching `eval_range_arg`'s subquery arm.
        let range = RangeEval {
            series,
            end_ms,
            range_ms,
            modifier: None,
        };
        Ok(Some(PlannedInstant::Precomputed(apply_outer_range_fn(
            range, outer, time_ms,
        ))))
    }

    /// Evaluate the inner instant-vector argument of a label-rewrite / ordering
    /// call into a `Vec<InstantSample>`, preserving genuine (non-stale) NaN —
    /// exactly the samples the interpreter would transform.
    ///
    /// A bare instant-vector selector is selected directly here (preserving a
    /// genuine NaN latest-in-window sample, and dropping only stale-NaN markers,
    /// exactly as the shared `InstantManipulate` operator does) with its full
    /// labelset (including `__name__`). Every other planner-supported inner
    /// expression is recursed into and assembled. Returns `None` (caller falls
    /// back) for a histogram-bearing selector or an inner expression the planner
    /// cannot evaluate.
    fn label_ops_inner_vector<'a>(
        &'a self,
        tenant: &'a str,
        value_arg: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<Option<Vec<InstantSample>>>> {
        async move {
            let mut inner = value_arg;
            while let Expr::Paren(paren) = inner {
                inner = &paren.expr;
            }

            if let Expr::VectorSelector(selector) = inner {
                if self
                    .selector_has_histogram_series(tenant, selector, time_ms)
                    .await?
                {
                    return Ok(None);
                }
                let samples = self
                    .scalar_math_selector_samples(tenant, selector, time_ms)
                    .await?
                    .into_iter()
                    .map(|sample| InstantSample {
                        labels: sample.labels,
                        ts_ms: sample.ts_ms,
                        value: SampleValue::Float(sample.value),
                    })
                    .collect();
                return Ok(Some(samples));
            }

            // A nested plannable inner expression: recurse and assemble it,
            // applying that shape's own drop semantics before transforming.
            let Some(planned) = self.plan_instant_expr(tenant, inner, time_ms).await? else {
                return Ok(None);
            };
            let QueryResult::InstantVector(samples) =
                self.assemble_planned_instant(planned, time_ms).await?
            else {
                return Ok(None);
            };
            Ok(Some(samples))
        }
        .boxed()
    }

    /// Evaluate the inner instant-vector argument of a **histogram-fold** call
    /// (`histogram_quantile` / the native accessors) into a `Vec<InstantSample>`
    /// that carries native-histogram series as `SampleValue::Histogram`.
    ///
    /// A bare instant-vector selector is selected directly via the interpreter's
    /// own [`Self::eval_instant_selector`], so the result is identical to the
    /// interpreter by construction: genuine NaN floats are preserved, stale-NaN
    /// markers are dropped, the full labelset (including `__name__`) is carried,
    /// and — crucially — histogram series yield `SampleValue::Histogram` rows. The
    /// selection is a direct shared-kernel scan (not the float-only operator
    /// leaf), so histogram samples and empty-valued labels round-trip faithfully.
    /// Every other planner-supported inner expression is recursed into and
    /// assembled (the float operator path never surfaces a histogram, so a nested
    /// inner stays float-only). Returns `None` (caller falls back) only for an
    /// inner expression the recursive planner cannot evaluate.
    fn histogram_fold_inner_vector<'a>(
        &'a self,
        tenant: &'a str,
        value_arg: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<Option<Vec<InstantSample>>>> {
        async move {
            let mut inner = value_arg;
            while let Expr::Paren(paren) = inner {
                inner = &paren.expr;
            }

            if let Expr::VectorSelector(selector) = inner {
                let QueryResult::InstantVector(samples) = self
                    .eval_instant_selector(tenant, selector, time_ms)
                    .await?
                else {
                    return Ok(None);
                };
                return Ok(Some(samples));
            }

            let Some(planned) = self.plan_instant_expr(tenant, inner, time_ms).await? else {
                return Ok(None);
            };
            let QueryResult::InstantVector(samples) =
                self.assemble_planned_instant(planned, time_ms).await?
            else {
                return Ok(None);
            };
            Ok(Some(samples))
        }
        .boxed()
    }

    /// Evaluate the inner instant-vector argument of a scalar-math call into the
    /// one-float-per-series rows the projection consumes, preserving genuine
    /// (non-stale) NaN — exactly the samples the interpreter would feed to `f()`.
    ///
    /// A bare instant-vector selector is selected directly here (preserving a
    /// genuine NaN latest-in-window sample, and dropping only stale-NaN markers,
    /// exactly as the shared `InstantManipulate` operator does). Every other
    /// planner-supported inner expression is recursed into and assembled. Returns
    /// `None` (caller falls back) for a histogram-bearing selector or an inner
    /// expression the planner cannot evaluate.
    fn scalar_math_inner_samples<'a>(
        &'a self,
        tenant: &'a str,
        value_arg: &'a Expr,
        time_ms: i64,
    ) -> BoxFuture<'a, Result<Option<Vec<ScalarMathLabeledValue>>>> {
        async move {
            // Unwrap parentheses to reach the underlying expression.
            let mut inner = value_arg;
            while let Expr::Paren(paren) = inner {
                inner = &paren.expr;
            }

            if let Expr::VectorSelector(selector) = inner {
                // A bare selector: select the latest in-window float sample per
                // series, dropping stale-NaN markers but **keeping** genuine NaN
                // (matching `eval_instant_selector`). Histogram-bearing selectors
                // fall back to the interpreter.
                if self
                    .selector_has_histogram_series(tenant, selector, time_ms)
                    .await?
                {
                    return Ok(None);
                }
                return Ok(Some(
                    self.scalar_math_selector_samples(tenant, selector, time_ms)
                        .await?,
                ));
            }

            // A nested plannable inner expression: recurse, then assemble it to
            // an instant vector (applying that shape's own drop semantics — e.g.
            // rate's no-value suppression) before feeding the values to `f`.
            let Some(planned) = self.plan_instant_expr(tenant, inner, time_ms).await? else {
                return Ok(None);
            };
            let QueryResult::InstantVector(inner_samples) =
                self.assemble_planned_instant(planned, time_ms).await?
            else {
                return Ok(None);
            };
            let mut samples = Vec::with_capacity(inner_samples.len());
            for sample in inner_samples {
                let SampleValue::Float(value) = sample.value else {
                    // The planner paths are float-only, so a histogram here would
                    // be a contract violation; fall back defensively.
                    return Ok(None);
                };
                samples.push(ScalarMathLabeledValue {
                    labels: sample.labels,
                    ts_ms: sample.ts_ms,
                    value,
                });
            }
            Ok(Some(samples))
        }
        .boxed()
    }

    /// Select the latest in-window float sample per series for a bare
    /// instant-vector selector, keeping genuine NaN and dropping stale-NaN
    /// markers — a float-only mirror of [`Self::eval_instant_selector`] used as
    /// the scalar-math inner source.
    async fn scalar_math_selector_samples(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<Vec<ScalarMathLabeledValue>> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;

        let mut latest_by_fp: BTreeMap<SeriesFingerprint, (i64, f64)> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, row.value);
                    }
                })
                .or_insert((row.ts_ms, row.value));
        }

        let mut samples = Vec::with_capacity(latest_by_fp.len());
        for (fp, (ts_ms, value)) in latest_by_fp {
            // Drop a stale-NaN marker (the series has no value), matching
            // `eval_instant_selector`; a genuine NaN is kept.
            if is_stale_nan(value) {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&fp).cloned() else {
                continue;
            };
            samples.push(ScalarMathLabeledValue {
                labels,
                ts_ms,
                value,
            });
        }
        Ok(samples)
    }

    async fn eval_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;

        let mut latest_by_fp: BTreeMap<SeriesFingerprint, (i64, SampleValue)> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, SampleValue::Float(row.value));
                    }
                })
                .or_insert((row.ts_ms, SampleValue::Float(row.value)));
        }
        for row in hist_rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, SampleValue::Histogram(row.hist.clone()));
                    }
                })
                .or_insert((row.ts_ms, SampleValue::Histogram(row.hist)));
        }

        let samples = latest_by_fp
            .into_iter()
            .filter_map(|(fp, (ts_ms, value))| {
                if matches!(&value, SampleValue::Float(value) if is_stale_nan(*value)) {
                    return None;
                }
                labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                    labels,
                    ts_ms,
                    value,
                })
            })
            .collect();
        Ok(QueryResult::InstantVector(samples))
    }

    async fn eval_smoothed_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            None,
        )?;
        let scan_start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let scan_end_ms = eval_time_ms.saturating_add(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;

        let mut rows_by_fp: BTreeMap<SeriesFingerprint, Vec<(i64, f64)>> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms || is_stale_nan(row.value) {
                continue;
            }
            rows_by_fp
                .entry(row.fp)
                .or_default()
                .push((row.ts_ms, row.value));
        }

        let samples = rows_by_fp
            .into_iter()
            .filter_map(|(fp, mut rows)| {
                rows.sort_by_key(|(ts_ms, _)| *ts_ms);
                let timestamps = rows.iter().map(|(ts_ms, _)| *ts_ms).collect::<Vec<_>>();
                let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
                let value = instant_smoothed_boundary_value(&timestamps, &values, eval_time_ms)?;
                labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(value),
                })
            })
            .collect();
        Ok(QueryResult::InstantVector(samples))
    }

    async fn eval_matrix_selector(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        start_ms: i64,
        end_ms: i64,
        modifier: Option<ExtendedSelectorModifier>,
    ) -> Result<Vec<RangeSeries>> {
        let range_ms = duration_ms(selector.range)?;
        let bounds = AtModifierBounds { start_ms, end_ms };
        let eval_start_ms = apply_selector_time_modifier(
            start_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            Some(bounds),
        )?;
        let eval_end_ms = apply_selector_time_modifier(
            end_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            Some(bounds),
        )?;
        let range_start_ms = eval_start_ms.saturating_sub(range_ms);
        let scan_start_ms = match modifier {
            Some(ExtendedSelectorModifier::Anchored | ExtendedSelectorModifier::Smoothed) => {
                range_start_ms.saturating_sub(self.opts.lookback_delta_ms)
            }
            None => range_start_ms,
        };
        let scan_end_ms = match modifier {
            Some(ExtendedSelectorModifier::Smoothed) => {
                eval_end_ms.saturating_add(self.opts.lookback_delta_ms)
            }
            Some(ExtendedSelectorModifier::Anchored) | None => eval_end_ms,
        };
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;

        let mut samples_by_fp: BTreeMap<SeriesFingerprint, BTreeMap<i64, SampleValue>> =
            BTreeMap::new();
        for row in rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms {
                continue;
            }
            if is_stale_nan(row.value) {
                continue;
            }
            samples_by_fp
                .entry(row.fp)
                .or_default()
                .insert(row.ts_ms, SampleValue::Float(row.value));
        }
        for row in hist_rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms {
                continue;
            }
            samples_by_fp
                .entry(row.fp)
                .or_default()
                .insert(row.ts_ms, SampleValue::Histogram(row.hist));
        }

        let mut out = Vec::new();
        for (fp, samples) in samples_by_fp {
            let Some(labels) = labels_by_fp.get(&fp).cloned() else {
                continue;
            };
            out.push(RangeSeries {
                labels,
                samples: samples.into_iter().collect(),
            });
        }
        Ok(out)
    }

    async fn eval_subquery(
        &self,
        tenant: &str,
        subquery: &SubqueryExpr,
        time_ms: i64,
    ) -> Result<Vec<RangeSeries>> {
        let range_ms = duration_ms(subquery.range)?;
        let step_ms = match subquery.step {
            Some(step) => duration_ms(step)?,
            None => self.opts.eval_interval_ms,
        };
        if step_ms <= 0 {
            return Err(PromqlError::Plan(
                "subquery step must be positive".to_string(),
            ));
        }
        let end_ms = apply_selector_time_modifier(
            time_ms,
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            None,
        )?;
        let start_ms = align_subquery_start(end_ms.saturating_sub(range_ms), step_ms);
        // Evaluate the subquery's inner instant expression over its sub-grid
        // through the operator planner (the sole evaluation engine). The planner
        // is total, so it produces a result for every plannable inner; an
        // `Ok(None)` would be a planner bug, surfaced as an internal error.
        self.eval_range_via_planner(tenant, &subquery.expr, start_ms, end_ms, step_ms)
            .await?
            .ok_or_else(|| {
                PromqlError::Plan("planner returned no result for a subquery inner".to_string())
            })
    }

    async fn eval_range_arg(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
        function_name: &str,
    ) -> Result<RangeEval> {
        let mut expr = expr;
        let mut modifier = None;
        loop {
            match expr {
                Expr::Paren(paren) => expr = &paren.expr,
                Expr::Extension(extension) => {
                    let Some(extended) = extension
                        .expr
                        .as_any()
                        .downcast_ref::<ExtendedSelectorExpr>()
                    else {
                        return Err(PromqlError::Plan(format!(
                            "{function_name} expects a range-vector selector"
                        )));
                    };
                    validate_extended_selector_modifier(function_name, extended.modifier())?;
                    modifier = Some(extended.modifier());
                    let Some(child) = extended.child() else {
                        return Err(PromqlError::Plan(format!(
                            "{function_name} expects a range-vector selector"
                        )));
                    };
                    expr = child;
                }
                _ => break,
            }
        }

        match expr {
            Expr::MatrixSelector(selector) => {
                let range_ms = duration_ms(selector.range)?;
                let end_ms = apply_selector_time_modifier(
                    time_ms,
                    selector.vs.at.as_ref(),
                    selector.vs.offset.as_ref(),
                    None,
                )?;
                let series = self
                    .eval_matrix_selector(tenant, selector, time_ms, time_ms, modifier)
                    .await?;
                Ok(RangeEval {
                    series,
                    end_ms,
                    range_ms,
                    modifier,
                })
            }
            Expr::Subquery(subquery) => {
                let range_ms = duration_ms(subquery.range)?;
                let end_ms = apply_selector_time_modifier(
                    time_ms,
                    subquery.at.as_ref(),
                    subquery.offset.as_ref(),
                    None,
                )?;
                let series = self.eval_subquery(tenant, subquery, time_ms).await?;
                Ok(RangeEval {
                    series,
                    end_ms,
                    range_ms,
                    modifier,
                })
            }
            _ => Err(PromqlError::Plan(format!(
                "{function_name} expects a range-vector selector"
            ))),
        }
    }

    #[cfg(test)]
    async fn eval_instant_aggregate(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        if aggregate.op.id() == T_COUNT_VALUES {
            return self
                .eval_count_values_aggregate(tenant, aggregate, time_ms)
                .await;
        }
        if matches!(aggregate.op.id(), T_TOPK | T_BOTTOMK) {
            return self.eval_k_aggregate(tenant, aggregate, time_ms).await;
        }
        if aggregate.op.id() == T_LIMITK {
            #[cfg(feature = "experimental-functions")]
            {
                return self.eval_limitk_aggregate(tenant, aggregate, time_ms).await;
            }
            #[cfg(not(feature = "experimental-functions"))]
            {
                return Err(PromqlError::Unsupported(
                    "aggregation `limitk` requires the experimental-functions feature".to_string(),
                ));
            }
        }
        if aggregate.op.id() == T_LIMIT_RATIO {
            #[cfg(feature = "experimental-functions")]
            {
                return self
                    .eval_limit_ratio_aggregate(tenant, aggregate, time_ms)
                    .await;
            }
            #[cfg(not(feature = "experimental-functions"))]
            {
                return Err(PromqlError::Unsupported(
                    "aggregation `limit_ratio` requires the experimental-functions feature"
                        .to_string(),
                ));
            }
        }
        if aggregate.op.id() == T_QUANTILE {
            return self
                .eval_quantile_aggregate(tenant, aggregate, time_ms)
                .await;
        }
        if aggregate.param.is_some() {
            return Err(PromqlError::Unsupported(format!(
                "parameterized aggregation `{}` is not implemented yet",
                aggregate.op
            )));
        }

        let input = self
            .eval_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(format!(
                "aggregation `{}` requires an instant vector",
                aggregate.op
            )));
        };

        let op = AggregateOp::try_from_token(aggregate.op)?;
        // The same routine backs the operator path
        // (`plan_aggregate_with_grouping`), so the two paths are identical by
        // construction once their inputs match.
        Ok(QueryResult::InstantVector(apply_simple_aggregate(
            samples,
            op,
            aggregate.modifier.as_ref(),
            time_ms,
        )?))
    }

    #[cfg(test)]
    async fn eval_k_aggregate(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let k = aggregate_k(aggregate)?;
        let input = self
            .eval_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(format!(
                "{} requires an instant vector",
                aggregate.op
            )));
        };
        // The same routine backs the operator path (`plan_param_aggregate_expr`),
        // so the two paths are identical by construction once their inputs match.
        Ok(QueryResult::InstantVector(apply_k_aggregate(
            samples,
            aggregate.op,
            k,
            aggregate.modifier.as_ref(),
        )))
    }

    #[cfg(all(test, feature = "experimental-functions"))]
    async fn eval_limitk_aggregate(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let k = self
            .eval_limitk_parameter(tenant, aggregate, time_ms)
            .await?;
        if k == 0 {
            return Ok(QueryResult::InstantVector(Vec::new()));
        }

        let input = self
            .eval_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "limitk requires an instant vector".to_string(),
            ));
        };

        // The same routine backs the operator path (`plan_param_aggregate_expr`),
        // so the two paths are identical by construction once their inputs match.
        Ok(QueryResult::InstantVector(apply_limitk_aggregate(
            samples,
            k,
            aggregate.modifier.as_ref(),
        )))
    }

    #[cfg(all(test, feature = "experimental-functions"))]
    async fn eval_limit_ratio_aggregate(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let ratio = self
            .eval_limit_ratio_parameter(tenant, aggregate, time_ms)
            .await?;
        if ratio == 0.0 {
            return Ok(QueryResult::InstantVector(Vec::new()));
        }

        let input = self
            .eval_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "limit_ratio requires an instant vector".to_string(),
            ));
        };

        // The same routine backs the operator path (`plan_param_aggregate_expr`),
        // so the two paths are identical by construction once their inputs match.
        Ok(QueryResult::InstantVector(apply_limit_ratio_aggregate(
            samples, ratio,
        )))
    }

    #[cfg(feature = "experimental-functions")]
    async fn eval_limitk_parameter(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<usize> {
        let value = self
            .eval_aggregate_scalar_parameter(tenant, aggregate, time_ms)
            .await?;
        if value <= 0.0 {
            return Ok(0);
        }
        if !value.is_finite() || value.fract() != 0.0 {
            return Err(PromqlError::Plan(format!(
                "{} parameter must be an integer",
                aggregate.op
            )));
        }
        value
            .to_string()
            .parse::<usize>()
            .map_err(|_| PromqlError::Plan(format!("{} parameter is too large", aggregate.op)))
    }

    #[cfg(feature = "experimental-functions")]
    async fn eval_limit_ratio_parameter(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<f64> {
        let value = self
            .eval_aggregate_scalar_parameter(tenant, aggregate, time_ms)
            .await?;
        if value.is_nan() {
            return Err(PromqlError::Plan(
                "limit_ratio parameter must not be NaN".to_string(),
            ));
        }
        let capped = value.clamp(-1.0, 1.0);
        // Matches Prometheus: warn whenever the ratio fell outside [-1, 1] and
        // had to be capped to the nearer bound.
        if !(-1.0..=1.0).contains(&value) {
            emit_warning(invalid_ratio_warning(value, capped));
        }
        Ok(capped)
    }

    #[cfg(feature = "experimental-functions")]
    async fn eval_aggregate_scalar_parameter(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<f64> {
        let Some(param) = &aggregate.param else {
            return Err(PromqlError::Plan(format!(
                "{} requires a numeric parameter",
                aggregate.op
            )));
        };
        self.eval_scalar_expr(
            tenant,
            param,
            time_ms,
            &format!("{} parameter", aggregate.op),
        )
        .await
    }

    #[cfg(test)]
    async fn eval_quantile_aggregate(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let quantile = aggregate_quantile(aggregate)?;
        let input = self
            .eval_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "quantile requires an instant vector".to_string(),
            ));
        };
        // Shared with the operator path (`plan_param_aggregate_expr`).
        Ok(QueryResult::InstantVector(apply_quantile_aggregate(
            samples,
            quantile,
            aggregate.modifier.as_ref(),
            time_ms,
        )))
    }

    #[cfg(test)]
    async fn eval_count_values_aggregate(
        &self,
        tenant: &str,
        aggregate: &AggregateExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let Some(param) = &aggregate.param else {
            return Err(PromqlError::Plan(
                "count_values requires a label-name parameter".to_string(),
            ));
        };
        let Expr::StringLiteral(label_name) = param.as_ref() else {
            return Err(PromqlError::Plan(
                "count_values label-name parameter must be a string".to_string(),
            ));
        };

        let input = self
            .eval_instant_expr(tenant, &aggregate.expr, time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "count_values requires an instant vector".to_string(),
            ));
        };
        // Shared with the operator path (`plan_param_aggregate_expr`).
        Ok(QueryResult::InstantVector(apply_count_values_aggregate(
            samples,
            &label_name.val,
            aggregate.modifier.as_ref(),
            time_ms,
        )?))
    }

    #[cfg(test)]
    async fn eval_instant_binary(
        &self,
        tenant: &str,
        binary: &BinaryExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        // Evaluate both operands through the interpreter, then hand the already-
        // evaluated operand values to the shared combine routine. The exact same
        // routine backs the operator path (`plan_binary_expr`), so the two paths
        // are identical by construction once their operands match.
        let lhs = self.eval_instant_expr(tenant, &binary.lhs, time_ms).await?;
        let rhs = self.eval_instant_expr(tenant, &binary.rhs, time_ms).await?;
        combine_instant_binary(
            binary,
            InstantValue::try_from_query(lhs)?,
            InstantValue::try_from_query(rhs)?,
            time_ms,
        )
    }

    #[cfg(test)]
    async fn eval_instant_unary(
        &self,
        tenant: &str,
        unary: &UnaryExpr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        // Evaluate the operand through the interpreter, then negate via the shared
        // routine. The same routine backs the operator path
        // (`plan_unary_expr`), so the two paths are identical by construction once
        // their operands match.
        let operand = self.eval_instant_expr(tenant, &unary.expr, time_ms).await?;
        negate_query_result(operand)
    }

    #[cfg(test)]
    #[allow(
        clippy::too_many_lines,
        reason = "PromQL function dispatch is intentionally centralized for now"
    )]
    async fn eval_instant_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        match call.func.name {
            "rate" => {
                self.eval_range_function_call(tenant, call, time_ms, RangeFn::Rate)
                    .await
            }
            "increase" => {
                self.eval_range_function_call(tenant, call, time_ms, RangeFn::Increase)
                    .await
            }
            "delta" => {
                self.eval_range_function_call(tenant, call, time_ms, RangeFn::Delta)
                    .await
            }
            "changes" => {
                self.eval_range_function_call(tenant, call, time_ms, RangeFn::Changes)
                    .await
            }
            "resets" => {
                self.eval_range_function_call(tenant, call, time_ms, RangeFn::Resets)
                    .await
            }
            "irate" => {
                self.eval_instant_delta_call(tenant, call, time_ms, IrateFn::Irate)
                    .await
            }
            "idelta" => {
                self.eval_instant_delta_call(tenant, call, time_ms, IrateFn::Idelta)
                    .await
            }
            "deriv" => self.eval_deriv_call(tenant, call, time_ms).await,
            "predict_linear" => self.eval_predict_linear_call(tenant, call, time_ms).await,
            #[cfg(feature = "experimental-functions")]
            "max_of" => {
                self.eval_scalar_extrema_call(tenant, call, time_ms, ScalarExtremaFn::Max)
                    .await
            }
            #[cfg(feature = "experimental-functions")]
            "min_of" => {
                self.eval_scalar_extrema_call(tenant, call, time_ms, ScalarExtremaFn::Min)
                    .await
            }
            "info" => self.eval_info_call(tenant, call, time_ms).await,
            #[cfg(not(feature = "experimental-functions"))]
            "max_of" | "min_of" => Err(PromqlError::Unsupported(format!(
                "function `{}` requires the experimental-functions feature",
                call.func.name
            ))),
            #[cfg(not(feature = "experimental-functions"))]
            "range" | "step" | "start" | "end" => Err(PromqlError::Unsupported(format!(
                "function `{}` requires the experimental-functions feature",
                call.func.name
            ))),
            #[cfg(feature = "experimental-functions")]
            "range" => Self::eval_duration_helper_call(call, time_ms, DurationHelper::Range),
            #[cfg(feature = "experimental-functions")]
            "step" => Self::eval_duration_helper_call(call, time_ms, DurationHelper::Step),
            #[cfg(feature = "experimental-functions")]
            "start" => Self::eval_duration_helper_call(call, time_ms, DurationHelper::Start),
            #[cfg(feature = "experimental-functions")]
            "end" => Self::eval_duration_helper_call(call, time_ms, DurationHelper::End),
            #[cfg(feature = "experimental-functions")]
            "double_exponential_smoothing" => {
                self.eval_double_exponential_smoothing_call(tenant, call, time_ms)
                    .await
            }
            #[cfg(not(feature = "experimental-functions"))]
            "double_exponential_smoothing" => Err(PromqlError::Unsupported(
                "function `double_exponential_smoothing` requires the experimental-functions feature"
                    .to_string(),
            )),
            "sum_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Sum)
                    .await
            }
            "avg_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Avg)
                    .await
            }
            "count_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Count)
                    .await
            }
            "min_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Min)
                    .await
            }
            "max_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Max)
                    .await
            }
            "stddev_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Stddev)
                    .await
            }
            "stdvar_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Stdvar)
                    .await
            }
            "mad_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Mad)
                    .await
            }
            "first_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::First)
                    .await
            }
            "last_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Last)
                    .await
            }
            "ts_of_first_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::TsOfFirst)
                    .await
            }
            "ts_of_last_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::TsOfLast)
                    .await
            }
            "ts_of_min_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::TsOfMin)
                    .await
            }
            "ts_of_max_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::TsOfMax)
                    .await
            }
            "present_over_time" => {
                self.eval_over_time_call(tenant, call, time_ms, OverTimeFn::Present)
                    .await
            }
            "absent" => self.eval_absent_call(tenant, call, time_ms).await,
            "absent_over_time" => self.eval_absent_over_time_call(tenant, call, time_ms).await,
            "time" => Self::eval_time_call(call, time_ms),
            "timestamp" => self.eval_timestamp_call(tenant, call, time_ms).await,
            "quantile_over_time" => {
                self.eval_quantile_over_time_call(tenant, call, time_ms)
                    .await
            }
            "histogram_quantile" => {
                self.eval_histogram_quantile_call(tenant, call, time_ms)
                    .await
            }
            #[cfg(feature = "experimental-functions")]
            "histogram_quantiles" => {
                self.eval_histogram_quantiles_call(tenant, call, time_ms)
                    .await
            }
            #[cfg(not(feature = "experimental-functions"))]
            "histogram_quantiles" => Err(PromqlError::Unsupported(
                "function `histogram_quantiles` requires the experimental-functions feature"
                    .to_string(),
            )),
            "histogram_count" => {
                self.eval_histogram_accessor_call(tenant, call, time_ms, HistogramAccessor::Count)
                    .await
            }
            "histogram_sum" => {
                self.eval_histogram_accessor_call(tenant, call, time_ms, HistogramAccessor::Sum)
                    .await
            }
            "histogram_avg" => {
                self.eval_histogram_accessor_call(tenant, call, time_ms, HistogramAccessor::Avg)
                    .await
            }
            "histogram_stddev" => {
                self.eval_histogram_accessor_call(tenant, call, time_ms, HistogramAccessor::Stddev)
                    .await
            }
            "histogram_stdvar" => {
                self.eval_histogram_accessor_call(tenant, call, time_ms, HistogramAccessor::Stdvar)
                    .await
            }
            "histogram_fraction" => {
                self.eval_histogram_fraction_call(tenant, call, time_ms)
                    .await
            }
            "clamp" => {
                self.eval_clamp_call(tenant, call, time_ms, ClampKind::Both)
                    .await
            }
            "clamp_min" => {
                self.eval_clamp_call(tenant, call, time_ms, ClampKind::Min)
                    .await
            }
            "clamp_max" => {
                self.eval_clamp_call(tenant, call, time_ms, ClampKind::Max)
                    .await
            }
            "ceil" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Ceil)
                    .await
            }
            "floor" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Floor)
                    .await
            }
            "sgn" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Sgn)
                    .await
            }
            "abs" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Abs)
                    .await
            }
            "sqrt" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Sqrt)
                    .await
            }
            "exp" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Exp)
                    .await
            }
            "ln" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Ln)
                    .await
            }
            "log2" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Log2)
                    .await
            }
            "log10" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Log10)
                    .await
            }
            "sin" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Sin)
                    .await
            }
            "sinh" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Sinh)
                    .await
            }
            "cos" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Cos)
                    .await
            }
            "cosh" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Cosh)
                    .await
            }
            "tan" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Tan)
                    .await
            }
            "tanh" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Tanh)
                    .await
            }
            "asin" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Asin)
                    .await
            }
            "asinh" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Asinh)
                    .await
            }
            "acos" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Acos)
                    .await
            }
            "acosh" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Acosh)
                    .await
            }
            "atan" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Atan)
                    .await
            }
            "atanh" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Atanh)
                    .await
            }
            "deg" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Deg)
                    .await
            }
            "rad" => {
                self.eval_unary_float_call(tenant, call, time_ms, UnaryFloatFn::Rad)
                    .await
            }
            "pi" => Self::eval_pi_call(call, time_ms),
            "round" => self.eval_round_call(tenant, call, time_ms).await,
            "sort" => {
                self.eval_sort_call(tenant, call, time_ms, SortDirection::Ascending)
                    .await
            }
            "sort_desc" => {
                self.eval_sort_call(tenant, call, time_ms, SortDirection::Descending)
                    .await
            }
            "sort_by_label" => {
                self.eval_sort_by_label_call(tenant, call, time_ms, SortDirection::Ascending)
                    .await
            }
            "sort_by_label_desc" => {
                self.eval_sort_by_label_call(tenant, call, time_ms, SortDirection::Descending)
                    .await
            }
            "year" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::Year)
                    .await
            }
            "month" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::Month)
                    .await
            }
            "day_of_month" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::DayOfMonth)
                    .await
            }
            "day_of_week" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::DayOfWeek)
                    .await
            }
            "day_of_year" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::DayOfYear)
                    .await
            }
            "days_in_month" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::DaysInMonth)
                    .await
            }
            "hour" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::Hour)
                    .await
            }
            "minute" => {
                self.eval_calendar_call(tenant, call, time_ms, CalendarFn::Minute)
                    .await
            }
            "scalar" => self.eval_scalar_function_call(tenant, call, time_ms).await,
            "vector" => self.eval_vector_function_call(tenant, call, time_ms).await,
            "label_join" => self.eval_label_join_call(tenant, call, time_ms).await,
            "label_replace" => self.eval_label_replace_call(tenant, call, time_ms).await,
            other => Err(PromqlError::Unsupported(format!(
                "function `{other}` is not implemented yet"
            ))),
        }
    }

    #[cfg(test)]
    async fn eval_clamp_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: ClampKind,
    ) -> Result<QueryResult> {
        let expected_args = kind.argument_count();
        if call.args.args.len() != expected_args {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly {expected_args} arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }

        let input = self
            .eval_instant_expr(tenant, &call.args.args[0], time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(format!(
                "{} requires an instant vector as its first argument",
                call.func.name
            )));
        };

        let (min, max) = match kind {
            ClampKind::Both => {
                let min = self
                    .eval_scalar_arg(tenant, call, 1, time_ms, "minimum")
                    .await?;
                let max = self
                    .eval_scalar_arg(tenant, call, 2, time_ms, "maximum")
                    .await?;
                if min > max {
                    return Ok(QueryResult::InstantVector(Vec::new()));
                }
                (Some(min), Some(max))
            }
            ClampKind::Min => {
                let min = self
                    .eval_scalar_arg(tenant, call, 1, time_ms, "minimum")
                    .await?;
                (Some(min), None)
            }
            ClampKind::Max => {
                let max = self
                    .eval_scalar_arg(tenant, call, 1, time_ms, "maximum")
                    .await?;
                (None, Some(max))
            }
        };

        let out = samples
            .into_iter()
            .filter_map(|mut sample| {
                let SampleValue::Float(value) = sample.value else {
                    return None;
                };
                sample.labels = labels_without_metric_name(&sample.labels);
                sample.value = SampleValue::Float(clamp_float(value, min, max));
                Some(sample)
            })
            .collect();
        Ok(QueryResult::InstantVector(out))
    }

    #[cfg(test)]
    async fn eval_unary_float_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: UnaryFloatFn,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        match self.eval_instant_expr(tenant, arg, time_ms).await? {
            QueryResult::Scalar { value, .. } => Ok(QueryResult::Scalar {
                ts_ms: time_ms,
                value: kind.apply(value),
            }),
            QueryResult::InstantVector(samples) => Ok(QueryResult::InstantVector(
                samples
                    .into_iter()
                    .filter_map(|sample| {
                        let SampleValue::Float(value) = sample.value else {
                            return None;
                        };
                        Some(InstantSample {
                            labels: labels_without_metric_name(&sample.labels),
                            ts_ms: sample.ts_ms,
                            value: SampleValue::Float(kind.apply(value)),
                        })
                    })
                    .collect(),
            )),
            QueryResult::RangeMatrix(_) | QueryResult::Str { .. } => Err(PromqlError::Plan(
                format!("{} expects a scalar or instant vector", call.func.name),
            )),
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[allow(
        clippy::cast_precision_loss,
        reason = "PromQL duration helpers return seconds as f64 scalars"
    )]
    fn eval_duration_helper_call(
        call: &Call,
        time_ms: i64,
        helper: DurationHelper,
    ) -> Result<QueryResult> {
        if !call.args.args.is_empty() {
            return Err(PromqlError::Plan(format!(
                "{} expects no arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value: helper.value_ms() as f64 / 1000.0,
        })
    }

    #[cfg(test)]
    async fn eval_round_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        if !(1..=2).contains(&call.args.args.len()) {
            return Err(PromqlError::Plan(format!(
                "round expects one or two arguments, got {}",
                call.args.args.len()
            )));
        }

        let to_nearest = if call.args.args.len() == 2 {
            self.eval_scalar_arg(tenant, call, 1, time_ms, "to_nearest")
                .await?
        } else {
            1.0
        };

        match self
            .eval_instant_expr(tenant, &call.args.args[0], time_ms)
            .await?
        {
            QueryResult::Scalar { value, .. } => Ok(QueryResult::Scalar {
                ts_ms: time_ms,
                value: round_to_nearest(value, to_nearest),
            }),
            QueryResult::InstantVector(samples) => Ok(QueryResult::InstantVector(
                samples
                    .into_iter()
                    .filter_map(|sample| {
                        let SampleValue::Float(value) = sample.value else {
                            return None;
                        };
                        Some(InstantSample {
                            labels: labels_without_metric_name(&sample.labels),
                            ts_ms: sample.ts_ms,
                            value: SampleValue::Float(round_to_nearest(value, to_nearest)),
                        })
                    })
                    .collect(),
            )),
            QueryResult::RangeMatrix(_) | QueryResult::Str { .. } => Err(PromqlError::Plan(
                "round expects a scalar or instant vector".to_string(),
            )),
        }
    }

    #[cfg(feature = "experimental-functions")]
    async fn eval_scalar_extrema_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: ScalarExtremaFn,
    ) -> Result<QueryResult> {
        let [left_arg, right_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly two arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let left = self
            .eval_scalar_expr(tenant, left_arg, time_ms, call.func.name)
            .await?;
        let right = self
            .eval_scalar_expr(tenant, right_arg, time_ms, call.func.name)
            .await?;
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value: kind.apply(left, right),
        })
    }

    #[cfg(test)]
    async fn eval_sort_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        direction: SortDirection,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let QueryResult::InstantVector(samples) =
            self.eval_instant_expr(tenant, arg, time_ms).await?
        else {
            return Err(PromqlError::Plan(format!(
                "{} expects an instant vector",
                call.func.name
            )));
        };

        Ok(QueryResult::InstantVector(label_ops::apply_sort(
            samples,
            direction.into(),
        )))
    }

    #[cfg(test)]
    async fn eval_sort_by_label_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        direction: SortDirection,
    ) -> Result<QueryResult> {
        if call.args.args.len() < 2 {
            return Err(PromqlError::Plan(format!(
                "{} expects an instant vector and at least one label name",
                call.func.name
            )));
        }

        let labels = (1..call.args.args.len())
            .map(|index| string_literal_arg(call, index, "label name"))
            .collect::<Result<Vec<_>>>()?;
        let QueryResult::InstantVector(samples) = self
            .eval_instant_expr(tenant, &call.args.args[0], time_ms)
            .await?
        else {
            return Err(PromqlError::Plan(format!(
                "{} expects an instant vector",
                call.func.name
            )));
        };

        Ok(QueryResult::InstantVector(label_ops::apply_sort_by_label(
            samples,
            &labels,
            direction.into(),
        )))
    }

    #[cfg(test)]
    async fn eval_calendar_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: CalendarFn,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            if call.args.args.is_empty() {
                return Ok(QueryResult::Scalar {
                    ts_ms: time_ms,
                    value: kind.apply(timestamp_seconds(time_ms)),
                });
            }
            return Err(PromqlError::Plan(format!(
                "{} expects zero or one arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let QueryResult::InstantVector(samples) =
            self.eval_instant_expr(tenant, arg, time_ms).await?
        else {
            return Err(PromqlError::Plan(format!(
                "{} expects an instant vector",
                call.func.name
            )));
        };

        Ok(QueryResult::InstantVector(
            samples
                .into_iter()
                .filter_map(|sample| {
                    let SampleValue::Float(value) = sample.value else {
                        return None;
                    };
                    Some(InstantSample {
                        labels: labels_without_metric_name(&sample.labels),
                        ts_ms: time_ms,
                        value: SampleValue::Float(kind.apply(value)),
                    })
                })
                .collect(),
        ))
    }

    #[cfg(test)]
    async fn eval_scalar_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "scalar expects exactly one argument, got {}",
                call.args.args.len()
            )));
        };

        let QueryResult::InstantVector(samples) =
            self.eval_instant_expr(tenant, arg, time_ms).await?
        else {
            return Err(PromqlError::Plan(
                "scalar expects an instant vector".to_string(),
            ));
        };

        let value = if samples.len() == 1 {
            match samples.into_iter().next().expect("single sample").value {
                SampleValue::Float(value) => value,
                SampleValue::Histogram(_) => f64::NAN,
            }
        } else {
            f64::NAN
        };
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value,
        })
    }

    #[cfg(test)]
    async fn eval_vector_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "vector expects exactly one argument, got {}",
                call.args.args.len()
            )));
        };

        let QueryResult::Scalar { value, .. } =
            self.eval_instant_expr(tenant, arg, time_ms).await?
        else {
            return Err(PromqlError::Plan("vector expects a scalar".to_string()));
        };

        Ok(QueryResult::InstantVector(vec![InstantSample {
            labels: Labels::new(),
            ts_ms: time_ms,
            value: SampleValue::Float(value),
        }]))
    }

    async fn eval_scalar_arg(
        &self,
        tenant: &str,
        call: &Call,
        index: usize,
        time_ms: i64,
        name: &str,
    ) -> Result<f64> {
        match self
            .plan_and_resolve(tenant, &call.args.args[index], time_ms)
            .await?
        {
            QueryResult::Scalar { value, .. } => Ok(value),
            QueryResult::InstantVector(_)
            | QueryResult::RangeMatrix(_)
            | QueryResult::Str { .. } => Err(PromqlError::Plan(format!(
                "{} {name} argument must be a scalar",
                call.func.name
            ))),
        }
    }

    #[cfg(test)]
    async fn eval_histogram_quantile_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [quantile_arg, vector_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly two arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let quantile = match self
            .eval_instant_expr(tenant, quantile_arg, time_ms)
            .await?
        {
            QueryResult::Scalar { value, .. } => value,
            QueryResult::InstantVector(_)
            | QueryResult::RangeMatrix(_)
            | QueryResult::Str { .. } => {
                return Err(PromqlError::Plan(
                    "histogram_quantile quantile argument must be a scalar".to_string(),
                ));
            }
        };

        let input = self.eval_instant_expr(tenant, vector_arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "histogram_quantile requires an instant vector as its second argument".to_string(),
            ));
        };

        Ok(QueryResult::InstantVector(apply_histogram_quantile(
            quantile, samples, time_ms,
        )?))
    }

    #[cfg(all(test, feature = "experimental-functions"))]
    async fn eval_histogram_quantiles_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        if call.args.args.len() < 3 {
            return Err(PromqlError::Plan(format!(
                "{} expects at least three arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }

        let vector_arg = &call.args.args[0];
        let label_name = string_literal_arg(call, 1, "label name")?;
        let mut quantiles = Vec::with_capacity(call.args.args.len().saturating_sub(2));
        for index in 2..call.args.args.len() {
            quantiles.push(
                self.eval_scalar_arg(tenant, call, index, time_ms, "quantile")
                    .await?,
            );
        }

        let input = self.eval_instant_expr(tenant, vector_arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "histogram_quantiles requires an instant vector as its first argument".to_string(),
            ));
        };

        // Shared with the operator path (`plan_histogram_quantiles_call`).
        Ok(QueryResult::InstantVector(apply_histogram_quantiles(
            samples,
            &label_name,
            &quantiles,
            time_ms,
        )?))
    }

    #[cfg(test)]
    async fn eval_histogram_accessor_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        accessor: HistogramAccessor,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let input = self.eval_instant_expr(tenant, arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(format!(
                "{} requires an instant vector",
                call.func.name
            )));
        };

        // Shared with the operator path (`plan_histogram_accessor_call`).
        Ok(QueryResult::InstantVector(apply_histogram_accessor(
            samples, accessor,
        )))
    }

    #[cfg(test)]
    async fn eval_histogram_fraction_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [lower_arg, upper_arg, vector_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly three arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let lower = self
            .eval_scalar_expr(tenant, lower_arg, time_ms, "histogram_fraction lower")
            .await?;
        let upper = self
            .eval_scalar_expr(tenant, upper_arg, time_ms, "histogram_fraction upper")
            .await?;
        let input = self.eval_instant_expr(tenant, vector_arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "histogram_fraction requires an instant vector as its third argument".to_string(),
            ));
        };

        // Shared with the operator path (`plan_histogram_fraction_call`).
        Ok(QueryResult::InstantVector(apply_histogram_fraction(
            lower, upper, samples, time_ms,
        )?))
    }

    #[cfg(any(test, feature = "experimental-functions"))]
    async fn eval_scalar_expr(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
        name: &str,
    ) -> Result<f64> {
        match self.plan_and_resolve(tenant, expr, time_ms).await? {
            QueryResult::Scalar { value, .. } => Ok(value),
            QueryResult::InstantVector(_)
            | QueryResult::RangeMatrix(_)
            | QueryResult::Str { .. } => Err(PromqlError::Plan(format!(
                "{name} argument must be a scalar"
            ))),
        }
    }

    #[cfg(test)]
    async fn eval_label_join_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        if call.args.args.len() < 4 {
            return Err(PromqlError::Plan(format!(
                "{} expects at least four arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }

        let dst_label = string_literal_arg(call, 1, "destination label")?;
        let separator = string_literal_arg(call, 2, "separator")?;
        let src_labels = call.args.args[3..]
            .iter()
            .enumerate()
            .map(|(offset, _)| string_literal_arg(call, offset + 3, "source label"))
            .collect::<Result<Vec<_>>>()?;

        let input = self
            .eval_instant_expr(tenant, &call.args.args[0], time_ms)
            .await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(format!(
                "{} requires an instant vector as its first argument",
                call.func.name
            )));
        };

        Ok(QueryResult::InstantVector(label_ops::apply_label_join(
            samples,
            &dst_label,
            &separator,
            &src_labels,
        )))
    }

    #[cfg(test)]
    async fn eval_label_replace_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [vector_arg, ..] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects five arguments, got 0",
                call.func.name
            )));
        };
        if call.args.args.len() != 5 {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly five arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }

        let dst_label = string_literal_arg(call, 1, "destination label")?;
        let replacement = string_literal_arg(call, 2, "replacement")?;
        let src_label = string_literal_arg(call, 3, "source label")?;
        let regex = string_literal_arg(call, 4, "regex")?;

        let input = self.eval_instant_expr(tenant, vector_arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(format!(
                "{} requires an instant vector as its first argument",
                call.func.name
            )));
        };

        Ok(QueryResult::InstantVector(label_ops::apply_label_replace(
            samples,
            &dst_label,
            &replacement,
            &src_label,
            &regex,
        )?))
    }

    #[cfg(test)]
    async fn eval_info_call(&self, tenant: &str, call: &Call, time_ms: i64) -> Result<QueryResult> {
        let context = parse_info_call(call)?;

        let QueryResult::InstantVector(samples) = self
            .eval_instant_expr(tenant, &call.args.args[0], time_ms)
            .await?
        else {
            return Err(PromqlError::Plan(
                "info expects an instant vector".to_string(),
            ));
        };

        let info_by_key = self.info_by_key(tenant, &context, time_ms).await?;
        Ok(QueryResult::InstantVector(apply_info(
            samples,
            &info_by_key,
            &context,
        )))
    }

    /// Select the `target_info` (or custom-selector) series and fold them into the
    /// `identifying-key -> info sample` map the [`apply_info`] join consumes. This
    /// is the store-touching half of [`Self::eval_info_call`], shared with the
    /// operator path's `info` dispatch.
    async fn info_by_key(
        &self,
        tenant: &str,
        context: &InfoContext<'_>,
        time_ms: i64,
    ) -> Result<BTreeMap<String, InstantSample>> {
        let info_samples = self
            .eval_info_selector_samples(
                tenant,
                context.data_label_selector,
                &context.data_label_matchers,
                time_ms,
            )
            .await?;
        info_samples_by_identifying_key(info_samples, &context.data_label_matchers)
    }

    async fn eval_info_selector_samples(
        &self,
        tenant: &str,
        data_label_selector: Option<&VectorSelector>,
        data_label_matchers: &[LabelMatcher],
        time_ms: i64,
    ) -> Result<Vec<InstantSample>> {
        let selector = if data_label_matchers
            .iter()
            .any(|matcher| matcher.name == "__name__")
        {
            data_label_selector.cloned()
        } else {
            match parse_promql("target_info")? {
                Expr::VectorSelector(selector) => Some(selector),
                _ => {
                    return Err(PromqlError::Plan(
                        "target_info selector did not produce a vector selector".to_string(),
                    ));
                }
            }
        };
        let Some(selector) = selector else {
            return Ok(Vec::new());
        };
        let QueryResult::InstantVector(info_samples) = self
            .eval_instant_selector(tenant, &selector, time_ms)
            .await?
        else {
            return Err(PromqlError::Plan(
                "info selector did not produce an instant vector".to_string(),
            ));
        };
        Ok(info_samples)
    }

    #[cfg(test)]
    async fn eval_range_function_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: RangeFn,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let range = self
            .eval_range_arg(tenant, arg, time_ms, call.func.name)
            .await?;
        let samples = apply_outer_range_fn(range, OuterRangeFn::Range(kind), time_ms);
        Ok(QueryResult::InstantVector(samples))
    }

    #[cfg(test)]
    async fn eval_instant_delta_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: IrateFn,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let range = self
            .eval_range_arg(tenant, arg, time_ms, call.func.name)
            .await?;
        let samples = apply_outer_range_fn(range, OuterRangeFn::InstantDelta(kind), time_ms);
        Ok(QueryResult::InstantVector(samples))
    }

    #[cfg(test)]
    async fn eval_deriv_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let range = self.eval_range_arg(tenant, arg, time_ms, "deriv").await?;
        let samples = apply_outer_range_fn(range, OuterRangeFn::Deriv, time_ms);
        Ok(QueryResult::InstantVector(samples))
    }

    #[cfg(test)]
    async fn eval_over_time_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
        kind: OverTimeFn,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let range = self
            .eval_range_arg(tenant, arg, time_ms, call.func.name)
            .await?;
        let samples = apply_outer_range_fn(range, OuterRangeFn::OverTime(kind), time_ms);
        Ok(QueryResult::InstantVector(samples))
    }

    #[cfg(test)]
    async fn eval_absent_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let input = self.eval_instant_expr(tenant, arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "absent expects an instant vector".to_string(),
            ));
        };
        if !samples.is_empty() {
            return Ok(QueryResult::InstantVector(Vec::new()));
        }

        Ok(QueryResult::InstantVector(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }]))
    }

    #[cfg(test)]
    async fn eval_absent_over_time_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let range = self
            .eval_range_arg(tenant, arg, time_ms, call.func.name)
            .await?;
        if range
            .series
            .iter()
            .any(|series| range_has_samples(series, range.end_ms, range.range_ms))
        {
            return Ok(QueryResult::InstantVector(Vec::new()));
        }

        Ok(QueryResult::InstantVector(vec![InstantSample {
            labels: absent_labels(arg)?,
            ts_ms: time_ms,
            value: SampleValue::Float(1.0),
        }]))
    }

    #[cfg(test)]
    fn eval_time_call(call: &Call, time_ms: i64) -> Result<QueryResult> {
        if !call.args.args.is_empty() {
            return Err(PromqlError::Plan(format!(
                "{} expects no arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value: timestamp_seconds(time_ms),
        })
    }

    #[cfg(test)]
    fn eval_pi_call(call: &Call, time_ms: i64) -> Result<QueryResult> {
        if !call.args.args.is_empty() {
            return Err(PromqlError::Plan(format!(
                "{} expects no arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        }
        Ok(QueryResult::Scalar {
            ts_ms: time_ms,
            value: std::f64::consts::PI,
        })
    }

    #[cfg(test)]
    async fn eval_timestamp_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly one argument, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };

        let input = self.eval_instant_expr(tenant, arg, time_ms).await?;
        let QueryResult::InstantVector(samples) = input else {
            return Err(PromqlError::Plan(
                "timestamp expects an instant vector".to_string(),
            ));
        };
        Ok(QueryResult::InstantVector(
            samples
                .into_iter()
                .map(|sample| InstantSample {
                    labels: labels_without_metric_name(&sample.labels),
                    ts_ms: time_ms,
                    value: SampleValue::Float(timestamp_seconds(sample.ts_ms)),
                })
                .collect(),
        ))
    }

    #[cfg(test)]
    async fn eval_quantile_over_time_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [quantile_arg, range_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly two arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let quantile = match self
            .eval_instant_expr(tenant, quantile_arg, time_ms)
            .await?
        {
            QueryResult::Scalar { value, .. } => value,
            QueryResult::InstantVector(_)
            | QueryResult::RangeMatrix(_)
            | QueryResult::Str { .. } => {
                return Err(PromqlError::Plan(
                    "quantile_over_time quantile argument must be a scalar".to_string(),
                ));
            }
        };
        // An out-of-range / NaN `phi` is NOT an error (Prometheus returns signed
        // `±Inf` / `NaN` plus an `InvalidQuantileWarning`); keep this oracle in
        // parity with the planner path.
        if !is_valid_quantile(quantile) {
            emit_warning(invalid_quantile_warning(quantile));
        }

        let range = self
            .eval_range_arg(tenant, range_arg, time_ms, "quantile_over_time")
            .await?;
        let samples =
            apply_outer_range_fn(range, OuterRangeFn::QuantileOverTime(quantile), time_ms);
        Ok(QueryResult::InstantVector(samples))
    }

    #[cfg(test)]
    async fn eval_predict_linear_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [range_arg, _duration_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly two arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let duration_seconds = self
            .eval_scalar_arg(tenant, call, 1, time_ms, "duration")
            .await?;
        let range = self
            .eval_range_arg(tenant, range_arg, time_ms, "predict_linear")
            .await?;
        let samples = apply_outer_range_fn(
            range,
            OuterRangeFn::PredictLinear(duration_seconds),
            time_ms,
        );
        Ok(QueryResult::InstantVector(samples))
    }

    #[cfg(all(test, feature = "experimental-functions"))]
    async fn eval_double_exponential_smoothing_call(
        &self,
        tenant: &str,
        call: &Call,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let [range_arg, smoothing_arg, trend_arg] = call.args.args.as_slice() else {
            return Err(PromqlError::Plan(format!(
                "{} expects exactly three arguments, got {}",
                call.func.name,
                call.args.args.len()
            )));
        };
        let smoothing_factor = self
            .eval_scalar_expr(
                tenant,
                smoothing_arg,
                time_ms,
                "double_exponential_smoothing smoothing factor",
            )
            .await?;
        let trend_factor = self
            .eval_scalar_expr(
                tenant,
                trend_arg,
                time_ms,
                "double_exponential_smoothing trend factor",
            )
            .await?;
        validate_smoothing_factor("smoothing factor", smoothing_factor)?;
        validate_smoothing_factor("trend factor", trend_factor)?;

        let range = self
            .eval_range_arg(tenant, range_arg, time_ms, "double_exponential_smoothing")
            .await?;
        let samples = apply_outer_range_fn(
            range,
            OuterRangeFn::DoubleExponentialSmoothing {
                smoothing: smoothing_factor,
                trend: trend_factor,
            },
            time_ms,
        );
        Ok(QueryResult::InstantVector(samples))
    }

    async fn labels_by_fingerprint(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Labels>> {
        // Range queries resolve the same selector's series at every step. Labels
        // are window-independent, so cache the union-window resolution once per
        // matcher set and reuse it across steps (see RANGE_SCAN_CACHE). Requests
        // outside the pre-scanned union fall back to a direct resolution.
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(std::sync::Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.labels.get(&key).cloned()
                };
                let resolved = if let Some(map) = cached {
                    map
                } else {
                    let map = std::sync::Arc::new(
                        self.labels_by_fingerprint_uncached(
                            tenant,
                            matchers,
                            full_start_ms,
                            full_end_ms,
                        )
                        .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .labels
                        .insert(key, std::sync::Arc::clone(&map));
                    map
                };
                return Ok((*resolved).clone());
            }
        }
        self.labels_by_fingerprint_uncached(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn labels_by_fingerprint_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Labels>> {
        Ok(self
            .store
            .series(tenant, matchers, start_ms, end_ms)
            .await?
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect())
    }

    async fn labels_by_fingerprint_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<BTreeMap<SeriesFingerprint, Labels>> {
        let mut out = BTreeMap::new();
        for matchers in matcher_sets {
            out.extend(
                self.labels_by_fingerprint(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
        }
        Ok(out)
    }

    async fn scan_float_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        // Inside a range query (see RANGE_SCAN_CACHE), serve overlapping per-step
        // scans from a single union-window scan per matcher set. A request that
        // falls outside the pre-scanned union (offset/@-modifier, or a `[range]`
        // longer than the lookback) bypasses the cache and scans directly, so
        // results are identical — only redundant re-scans are eliminated.
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(std::sync::Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.floats.get(&key).cloned()
                };
                let full_rows = if let Some(rows) = cached {
                    rows
                } else {
                    let rows = std::sync::Arc::new(
                        self.scan_float_rows_uncached(tenant, matchers, full_start_ms, full_end_ms)
                            .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .floats
                        .insert(key, std::sync::Arc::clone(&rows));
                    rows
                };
                return Ok(full_rows
                    .iter()
                    .filter(|row| row.ts_ms >= start_ms && row.ts_ms <= end_ms)
                    .cloned()
                    .collect());
            }
        }
        self.scan_float_rows_uncached(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn scan_float_rows_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
        let Some(table) = scan.float_table.clone() else {
            return Ok(Vec::new());
        };
        collect_float_rows(scan, &table, self.opts.max_samples).await
    }

    async fn scan_float_row_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<FloatRow>> {
        let mut out = Vec::new();
        for matchers in matcher_sets {
            out.extend(
                self.scan_float_rows(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
            if out.len() > self.opts.max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={}",
                    self.opts.max_samples
                )));
            }
        }
        Ok(out)
    }

    async fn scan_histogram_rows(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        // Mirror scan_float_rows: serve per-step histogram probes from one
        // union-window scan during a range query (see RANGE_SCAN_CACHE).
        if let Ok(cache) = RANGE_SCAN_CACHE.try_with(std::sync::Arc::clone) {
            let (full_start_ms, full_end_ms) = {
                let guard = cache.lock().expect("range scan cache poisoned");
                (guard.full_start_ms, guard.full_end_ms)
            };
            if start_ms >= full_start_ms && end_ms <= full_end_ms {
                let key = matchers_cache_key(matchers);
                let cached = {
                    let guard = cache.lock().expect("range scan cache poisoned");
                    guard.histograms.get(&key).cloned()
                };
                let full_rows = if let Some(rows) = cached {
                    rows
                } else {
                    let rows = std::sync::Arc::new(
                        self.scan_histogram_rows_uncached(
                            tenant,
                            matchers,
                            full_start_ms,
                            full_end_ms,
                        )
                        .await?,
                    );
                    cache
                        .lock()
                        .expect("range scan cache poisoned")
                        .histograms
                        .insert(key, std::sync::Arc::clone(&rows));
                    rows
                };
                return Ok(full_rows
                    .iter()
                    .filter(|row| row.ts_ms >= start_ms && row.ts_ms <= end_ms)
                    .cloned()
                    .collect());
            }
        }
        self.scan_histogram_rows_uncached(tenant, matchers, start_ms, end_ms)
            .await
    }

    async fn scan_histogram_rows_uncached(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        let scan = self.store.scan(tenant, matchers, start_ms, end_ms).await?;
        let Some(table) = scan.histogram_table.clone() else {
            return Ok(Vec::new());
        };
        collect_histogram_rows(scan, &table, self.opts.max_samples).await
    }

    async fn scan_histogram_row_sets(
        &self,
        tenant: &str,
        matcher_sets: &[Vec<LabelMatcher>],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<HistogramRow>> {
        let mut out = Vec::new();
        for matchers in matcher_sets {
            out.extend(
                self.scan_histogram_rows(tenant, matchers, start_ms, end_ms)
                    .await?,
            );
            if out.len() > self.opts.max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={}",
                    self.opts.max_samples
                )));
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
fn string_literal_arg(call: &Call, index: usize, name: &str) -> Result<String> {
    let Some(arg) = call.args.args.get(index) else {
        return Err(PromqlError::Plan(format!(
            "{} missing {name} argument",
            call.func.name
        )));
    };
    let Expr::StringLiteral(value) = arg.as_ref() else {
        return Err(PromqlError::Plan(format!(
            "{} {name} argument must be a string",
            call.func.name
        )));
    };
    Ok(value.val.clone())
}

fn aggregate_k(aggregate: &AggregateExpr) -> Result<usize> {
    let Some(param) = &aggregate.param else {
        return Err(PromqlError::Plan(format!(
            "{} requires a numeric parameter",
            aggregate.op
        )));
    };
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return Err(PromqlError::Plan(format!(
            "{} parameter must be numeric",
            aggregate.op
        )));
    };
    if !number.val.is_finite() || number.val < 0.0 || number.val.fract() != 0.0 {
        return Err(PromqlError::Plan(format!(
            "{} parameter must be a non-negative integer",
            aggregate.op
        )));
    }
    number
        .val
        .to_string()
        .parse::<usize>()
        .map_err(|_| PromqlError::Plan(format!("{} parameter is too large", aggregate.op)))
}

fn aggregate_quantile(aggregate: &AggregateExpr) -> Result<f64> {
    let Some(param) = &aggregate.param else {
        return Err(PromqlError::Plan(
            "quantile requires a numeric parameter".to_string(),
        ));
    };
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return Err(PromqlError::Plan(
            "quantile parameter must be numeric".to_string(),
        ));
    };
    // An out-of-range / NaN phi is NOT an error here: Prometheus returns signed
    // `±Inf` / `NaN` plus an `InvalidQuantileWarning` (emitted by
    // `apply_quantile_aggregate`), exactly like the `histogram_quantile` family.
    Ok(number.val)
}

fn count_values_label_value(value: &SampleValue) -> Result<String> {
    match value {
        // Render the float with the crate's canonical Prometheus formatter so
        // non-finite values match the wire form (`+Inf`/`-Inf`/`NaN`) rather than
        // `f64::to_string`'s `inf`/`-inf`/`NaN`.
        SampleValue::Float(value) => Ok(crate::http_api::format_sample_value(*value)),
        SampleValue::Histogram(histogram) => serde_json::to_string(histogram).map_err(|error| {
            PromqlError::Exec(format!(
                "failed to encode histogram sample for count_values: {error}"
            ))
        }),
    }
}

/// Shared **simple** aggregation core (`sum`/`avg`/`count`/`group`/`min`/`max`/
/// `stddev`/`stdvar`) over an already-evaluated instant vector.
///
/// Backs both the interpreter ([`PromqlEngine::eval_instant_aggregate`]) and the
/// operator path ([`PromqlEngine::plan_aggregate_with_grouping`]), so the two are
/// identical by construction once their inputs match. Groups the samples by the
/// `by`/`without` labelset, accumulates each group's [`AggregateState`], and
/// emits one reduced sample per surviving group.
///
/// The native-histogram rules are encoded entirely in [`AggregateState`] and
/// [`AggregateOp`], matching Prometheus exactly:
/// - `sum`/`avg` (`aggregates_histograms`): histogram samples are MERGED (sum
///   adds, avg scales the merged histogram by `1/count`). A group that mixes a
///   float and a histogram is marked invalid and DROPPED from the output (the
///   `invalid_mixed_sample_type` flag), via either a float arriving after a
///   histogram ([`AggregateState::mark_invalid_mixed_sample_type`]) or a
///   histogram arriving after a float ([`AggregateState::push_histogram`]).
/// - `count`/`group` (`counts_histograms`): every sample is counted regardless
///   of type (histograms via [`AggregateState::push_observation`]).
/// - `min`/`max`/`stddev`/`stdvar` (`ignores_histograms`): histogram samples are
///   silently dropped (no-op), exactly as the interpreter ignores them — no
///   annotation is emitted, matching Prometheus.
///
/// Returns `Err` only for the (unreachable) case of a histogram sample under an
/// op that neither aggregates, counts, nor ignores histograms — every
/// [`AggregateOp`] falls into one of those three categories, so this mirrors the
/// interpreter's identical defensive branch.
fn apply_simple_aggregate(
    samples: Vec<InstantSample>,
    op: AggregateOp,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, AggregateState> = BTreeMap::new();
    for sample in samples {
        let labels = aggregate_labels(&sample.labels, modifier);
        let state = groups
            .entry(labels_key(&labels))
            .or_insert_with(|| AggregateState::new(labels));
        match sample.value {
            SampleValue::Float(value) => {
                if op.aggregates_histograms() && state.has_histogram() {
                    state.mark_invalid_mixed_sample_type();
                    continue;
                }
                state.push_float(value);
            }
            SampleValue::Histogram(histogram) if op.aggregates_histograms() => {
                state.push_histogram(histogram)?;
            }
            SampleValue::Histogram(_) if op.counts_histograms() => state.push_observation(),
            SampleValue::Histogram(_) if op.ignores_histograms() => {}
            SampleValue::Histogram(_) => {
                return Err(PromqlError::Unsupported(
                    "histogram aggregation is not implemented yet".to_string(),
                ));
            }
        }
    }

    Ok(groups
        .into_values()
        .filter_map(|state| {
            op.finish(&state).map(|value| InstantSample {
                labels: state.labels,
                ts_ms: time_ms,
                value,
            })
        })
        .collect())
}

/// Shared `topk`/`bottomk` core over an already-evaluated instant vector.
///
/// Backs both the interpreter ([`PromqlEngine::eval_k_aggregate`]) and the
/// operator path ([`PromqlEngine::plan_param_aggregate_expr`]), so the two are
/// identical by construction once their inputs match. Groups the samples by the
/// `by`/`without` labelset, sorts each group by value (highest-first for
/// `topk`, lowest-first for `bottomk`) with a `labels_key` tie-break, clamps to
/// `k`, and returns the surviving **original** samples (labels — including
/// `__name__` — ts, and value all preserved; this is a selection, not a
/// reduction). Histogram-typed samples are skipped (they carry no float to
/// rank); `k == 0` yields the empty vector.
fn apply_k_aggregate(
    samples: Vec<InstantSample>,
    op: TokenType,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    if k == 0 {
        return Vec::new();
    }

    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            continue;
        }
        let labels = aggregate_labels(&sample.labels, modifier);
        groups.entry(labels_key(&labels)).or_default().push(sample);
    }

    let mut out = Vec::new();
    for mut group in groups.into_values() {
        group.sort_by(|left, right| compare_k_aggregate_samples(op, left, right));
        group.truncate(k.min(group.len()));
        out.extend(group);
    }
    out
}

/// Shared `limitk(k, v)` (experimental) core over an already-evaluated instant
/// vector. Backs both the interpreter
/// ([`PromqlEngine::eval_limitk_aggregate`]) and the operator path. Groups by the
/// `by`/`without` labelset and keeps the first `k` members of each group in a
/// deterministic order (fingerprint, then `labels_key`), exactly as Prometheus'
/// reproducible `limitk` does. The caller resolves `k` (and short-circuits `k==0`
/// to the empty vector) before reaching here.
#[cfg(feature = "experimental-functions")]
fn apply_limitk_aggregate(
    samples: Vec<InstantSample>,
    k: usize,
    modifier: Option<&LabelModifier>,
) -> Vec<InstantSample> {
    let mut groups = BTreeMap::<String, Vec<InstantSample>>::new();
    for sample in samples {
        let labels = aggregate_labels(&sample.labels, modifier);
        groups.entry(labels_key(&labels)).or_default().push(sample);
    }

    let mut out = Vec::new();
    for mut samples in groups.into_values() {
        samples.sort_by(|left, right| {
            left.labels
                .fingerprint()
                .cmp(&right.labels.fingerprint())
                .then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
        });
        samples.truncate(k.min(samples.len()));
        out.extend(samples);
    }
    out
}

/// Shared `limit_ratio(ratio, v)` (experimental) core over an already-evaluated
/// instant vector. Backs both the interpreter
/// ([`PromqlEngine::eval_limit_ratio_aggregate`]) and the operator path. Keeps
/// each sample whose labelset hash falls in the ratio's deterministic selection
/// band ([`limit_ratio_includes_sample`]). The caller resolves and caps the
/// ratio (emitting the `InvalidRatioWarning` when it was out of range) and
/// short-circuits `ratio==0` to the empty vector before reaching here.
#[cfg(feature = "experimental-functions")]
fn apply_limit_ratio_aggregate(samples: Vec<InstantSample>, ratio: f64) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter(|sample| limit_ratio_includes_sample(ratio, &sample.labels))
        .collect()
}

/// Order two samples for `topk`/`bottomk` selection: by float value
/// (`right.total_cmp(left)` for `topk` so the highest sorts first, the reverse
/// for `bottomk`), tie-broken by `labels_key`. A non-float sample (which the
/// caller already filters) or a NaN sorts via `total_cmp`, matching Prometheus.
fn compare_k_aggregate_samples(
    op: TokenType,
    left: &InstantSample,
    right: &InstantSample,
) -> std::cmp::Ordering {
    let left_value = float_sample_value(left).unwrap_or(f64::NAN);
    let right_value = float_sample_value(right).unwrap_or(f64::NAN);
    let by_value = if op.id() == T_TOPK {
        right_value.total_cmp(&left_value)
    } else {
        left_value.total_cmp(&right_value)
    };
    by_value.then_with(|| labels_key(&left.labels).cmp(&labels_key(&right.labels)))
}

/// Shared `quantile(phi, v)` core over an already-evaluated instant vector.
///
/// Backs both the interpreter ([`PromqlEngine::eval_quantile_aggregate`]) and
/// the operator path. Groups the float samples by the `by`/`without` labelset
/// and emits the φ-quantile of each group's values (linear interpolation in
/// rank space via [`quantile_value`]; an empty group yields no row).
/// Histogram-typed samples are skipped.
///
/// A `phi` outside `[0, 1]` (or NaN) is NOT an error: each group yields the
/// signed `±Inf` / `NaN` [`quantile_value`] returns, and one
/// `InvalidQuantileWarning` is raised — matching Prometheus and the
/// `histogram_quantile` family.
fn apply_quantile_aggregate(
    samples: Vec<InstantSample>,
    quantile: f64,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Vec<InstantSample> {
    if !is_valid_quantile(quantile) {
        emit_warning(invalid_quantile_warning(quantile));
    }
    let mut groups: BTreeMap<String, (Labels, Vec<f64>)> = BTreeMap::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let labels = aggregate_labels(&sample.labels, modifier);
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(value);
    }

    groups
        .into_values()
        .filter_map(|(labels, mut values)| {
            quantile_value(quantile, &mut values).map(|value| InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect()
}

/// Shared `count_values("label", v)` core over an already-evaluated instant
/// vector.
///
/// Backs both the interpreter ([`PromqlEngine::eval_count_values_aggregate`])
/// and the operator path. Groups by the `by`/`without` labelset extended with
/// the named label set to each sample's formatted value (floats via `Display`,
/// histograms via JSON), and emits one series per distinct value carrying the
/// group's count. Returns `Err` only when a histogram value cannot be encoded.
fn apply_count_values_aggregate(
    samples: Vec<InstantSample>,
    label_name: &str,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups = BTreeMap::<String, AggregateState>::new();
    for sample in samples {
        let mut labels = aggregate_labels(&sample.labels, modifier);
        labels.insert(label_name, count_values_label_value(&sample.value)?);
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| AggregateState::new(labels))
            .push_float(1.0);
    }

    Ok(groups
        .into_values()
        .map(|state| InstantSample {
            labels: state.labels,
            ts_ms: time_ms,
            value: SampleValue::Float(state.count_f64),
        })
        .collect())
}

/// Shared `stddev(v)` / `stdvar(v)` core over an already-evaluated **float-only**
/// instant vector.
///
/// Backs both the interpreter (its general [`PromqlEngine::eval_instant_aggregate`]
/// loop, which builds the same [`AggregateState`] and calls the same
/// [`AggregateOp::finish`]) and the operator path. Groups the float samples by
/// the `by`/`without` labelset, accumulates each group's running
/// sum/sum-of-squares/count, and emits the population standard deviation
/// (`Stddev`) or variance (`Stdvar`) per group. `op` must be
/// [`AggregateOp::Stddev`] or [`AggregateOp::Stdvar`]. Histogram samples are
/// ignored exactly as the interpreter ignores them for these ops; the operator
/// path only feeds float-only inputs, so none appear in practice.
fn apply_stddev_stdvar_aggregate(
    samples: Vec<InstantSample>,
    op: AggregateOp,
    modifier: Option<&LabelModifier>,
    time_ms: i64,
) -> Vec<InstantSample> {
    debug_assert!(
        matches!(op, AggregateOp::Stddev | AggregateOp::Stdvar),
        "apply_stddev_stdvar_aggregate requires a stddev/stdvar op"
    );
    // `stddev`/`stdvar` are `ignores_histograms` ops, so the shared simple-
    // aggregate kernel skips histogram samples (its `op.ignores_histograms()`
    // no-op branch) exactly as this routine used to, and never hits the
    // unreachable error branch. Delegating keeps the interpreter and operator
    // param paths sharing one core.
    apply_simple_aggregate(samples, op, modifier, time_ms)
        .expect("stddev/stdvar ignore histograms, so the kernel is infallible here")
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "PromQL quantile interpolation works in f64 rank space, then indexes a sorted in-memory vector after bounding the rank."
)]
fn quantile_value(quantile: f64, values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // Prometheus' `quantile()` does NOT error on an out-of-range/NaN phi: a NaN
    // phi yields NaN, phi < 0 yields -Inf, and phi > 1 yields +Inf (the caller
    // raises an `InvalidQuantileWarning` alongside). This mirrors the
    // `histogram_quantile` family's leading guards.
    if quantile.is_nan() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }
    values.sort_by(f64::total_cmp);
    if values.len() == 1 {
        return Some(values[0]);
    }

    let rank = quantile * (values.len() - 1) as f64;
    let lower = rank.floor() as usize;
    let upper = rank.ceil() as usize;
    if lower == upper {
        return Some(values[lower]);
    }
    let weight = rank - lower as f64;
    Some(values[lower] * (1.0 - weight) + values[upper] * weight)
}

#[derive(Clone, Copy, Debug)]
struct ClassicBucket {
    upper_bound: f64,
    count: f64,
}

fn parse_classic_bucket_bound(value: &str) -> Result<f64> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => value.parse::<f64>().map_err(|error| {
            PromqlError::Plan(format!(
                "invalid classic histogram bucket `{value}`: {error}"
            ))
        }),
    }
}

/// Shared `histogram_quantile(phi, v)` core over an already-evaluated instant
/// vector.
///
/// Backs both the interpreter ([`PromqlEngine::eval_histogram_quantile_call`])
/// and the recursive operator path (a [`PlannedInstant::Precomputed`] result),
/// so the two are identical by construction. Native-histogram samples are
/// reduced via [`native_histogram_quantile`]; classic `<metric>_bucket{le}`
/// float series are grouped by their labels (excluding `le`), folded by
/// [`classic_histogram_quantile`] (which forces bucket monotonicity, parses each
/// `le` bound incl. `+Inf`, handles `<2`-bucket / `phi` out of `[0, 1]` / the
/// negative-first-bucket lower bound, and linearly interpolates). A series whose
/// labelset (sans `le`) appears as both a native histogram and a classic bucket
/// group is dropped from the output with a mixed-schema warning, matching
/// Apply a native-histogram accessor (`histogram_count` / `sum` / `avg` /
/// `stddev` / `stdvar`) to an instant vector, mirroring
/// [`PromqlEngine::eval_histogram_accessor_call`] exactly.
///
/// Only `SampleValue::Histogram` rows are kept (a float row carries no histogram
/// to read, so it is dropped); each surviving row keeps its source timestamp,
/// drops `__name__`, and carries the scalar accessor value. Shared by the
/// interpreter and the operator path so the two are parity-exact.
fn apply_histogram_accessor(
    samples: Vec<InstantSample>,
    accessor: HistogramAccessor,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter_map(|sample| {
            let SampleValue::Histogram(hist) = sample.value else {
                return None;
            };
            Some(InstantSample {
                labels: labels_without_metric_name(&sample.labels),
                ts_ms: sample.ts_ms,
                value: SampleValue::Float(accessor.value(&hist)),
            })
        })
        .collect()
}

/// Apply `histogram_fraction(lower, upper, v)` to an instant vector `v`,
/// mirroring [`PromqlEngine::eval_histogram_fraction_call`] exactly.
///
/// Native-histogram rows fold through [`native_histogram_fraction`] (keeping the
/// source timestamp); classic `<metric>_bucket{le}` float rows are grouped by
/// labelset (dropping `__name__` + `le`) and folded through
/// [`classic_histogram_fraction`] (carrying `time_ms`). A labelset that carries
/// both a classic and a native histogram is dropped from the output and raises
/// the `MixedClassicNativeHistogramsWarning` (via the in-scope annotation sink),
/// exactly as the interpreter does. Shared by the interpreter and the operator
/// path so the two are parity-exact.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound or a non-float classic
/// bucket count — exactly the errors the interpreter raised inline.
fn apply_histogram_fraction(
    lower: f64,
    upper: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut native_samples = BTreeMap::new();
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(hist) = sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_fraction(lower, upper, &hist)),
                },
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        let key = labels_key(&labels);
        record_metric_name(&mut metric_names, &key, &sample.labels);
        groups
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    warn_mixed_histograms(&mixed_histogram_keys, &metric_names);
    let mut out = native_samples
        .into_iter()
        .filter_map(|(key, sample)| (!mixed_histogram_keys.contains(&key)).then_some(sample))
        .collect::<Vec<_>>();
    out.extend(
        groups
            .into_iter()
            .filter_map(|(key, (labels, mut buckets))| {
                (!mixed_histogram_keys.contains(&key)).then_some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(classic_histogram_fraction(
                        lower,
                        upper,
                        &mut buckets,
                    )),
                })
            }),
    );
    Ok(out)
}

/// Prometheus. Both the `__name__` and `le` labels are dropped from every output
/// series. Classic output samples carry `time_ms`; native ones keep the source
/// sample timestamp.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound or a non-float classic
/// bucket count — exactly the errors the interpreter raised inline.
fn apply_histogram_quantile(
    quantile: f64,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut native_samples = BTreeMap::new();
    let mut metric_names: BTreeMap<String, String> = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            let key = labels_key(&labels);
            record_metric_name(&mut metric_names, &key, &sample.labels);
            native_samples.insert(
                key,
                InstantSample {
                    labels,
                    ts_ms: sample.ts_ms,
                    value: SampleValue::Float(native_histogram_quantile(quantile, histogram)),
                },
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        let key = labels_key(&labels);
        record_metric_name(&mut metric_names, &key, &sample.labels);
        groups
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    warn_mixed_histograms(&mixed_histogram_keys, &metric_names);
    let mut out = native_samples
        .into_iter()
        .filter_map(|(key, sample)| (!mixed_histogram_keys.contains(&key)).then_some(sample))
        .collect::<Vec<_>>();
    out.extend(
        groups
            .into_iter()
            .filter_map(|(key, (labels, mut buckets))| {
                (!mixed_histogram_keys.contains(&key)).then_some(InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(classic_histogram_quantile(quantile, &mut buckets)),
                })
            }),
    );
    Ok(out)
}

/// Apply the experimental `histogram_quantiles(label, v, phi...)` fold to an
/// already-evaluated instant vector, emitting one output series per `(input
/// series, quantile)` pair with the quantile written into the `label`-named label.
///
/// Shared between the interpreter ([`PromqlEngine::eval_histogram_quantiles_call`])
/// and the operator-path `histogram_quantiles` dispatch, so the two match
/// Prometheus by construction for both classic `<metric>_bucket{le}` float-bucket
/// vectors and native-histogram vectors. Unlike `histogram_quantile`, mixed
/// classic+native keys are silently skipped (no annotation), matching the
/// interpreter's `histogram_quantiles` behaviour: classic output samples carry
/// `time_ms`, native ones keep the source sample timestamp, and both drop
/// `__name__` (and `le` for classic buckets).
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound or a non-float classic
/// bucket count — exactly the errors the interpreter raised inline.
#[cfg(feature = "experimental-functions")]
fn apply_histogram_quantiles(
    samples: Vec<InstantSample>,
    label_name: &str,
    quantiles: &[f64],
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let mut groups: BTreeMap<String, (Labels, Vec<ClassicBucket>)> = BTreeMap::new();
    let mut native_samples = BTreeMap::new();
    for sample in samples {
        if let SampleValue::Histogram(histogram) = &sample.value {
            let labels = labels_without_metric_name(&sample.labels);
            native_samples.insert(
                labels_key(&labels),
                (labels, sample.ts_ms, histogram.clone()),
            );
            continue;
        }
        let Some(le) = sample.labels.get("le") else {
            continue;
        };
        let upper_bound = parse_classic_bucket_bound(le)?;
        let count = float_sample_value(&sample)?;
        let labels = labels_without_metric_and_label(&sample.labels, "le");
        groups
            .entry(labels_key(&labels))
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(ClassicBucket { upper_bound, count });
    }

    let mixed_histogram_keys = native_samples
        .keys()
        .filter(|key| groups.contains_key(*key))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut out = Vec::new();
    for (key, (labels, ts_ms, histogram)) in native_samples {
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = labels.clone();
            labels.insert(label_name, quantile.to_string());
            InstantSample {
                labels,
                ts_ms,
                value: SampleValue::Float(native_histogram_quantile(*quantile, &histogram)),
            }
        }));
    }
    for (key, (labels, buckets)) in groups {
        if mixed_histogram_keys.contains(&key) {
            continue;
        }
        out.extend(quantiles.iter().map(|quantile| {
            let mut labels = labels.clone();
            let mut buckets = buckets.clone();
            labels.insert(label_name, quantile.to_string());
            InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(classic_histogram_quantile(*quantile, &mut buckets)),
            }
        }));
    }
    Ok(out)
}

fn classic_histogram_quantile(quantile: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }

    let buckets = normalized_classic_histogram_buckets(buckets);
    if buckets.len() < 2
        || !buckets.last().is_some_and(|bucket| {
            bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
        })
    {
        return f64::NAN;
    }

    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return f64::NAN;
    }
    let rank = quantile * total;
    let bucket_index = buckets
        .iter()
        .position(|bucket| bucket.count >= rank)
        .unwrap_or(buckets.len() - 1);

    if bucket_index == buckets.len() - 1 {
        return buckets[bucket_index - 1].upper_bound;
    }

    let bucket = buckets[bucket_index];
    let (lower_bound, previous_count) = if bucket_index == 0 {
        if bucket.upper_bound <= 0.0 {
            return bucket.upper_bound;
        }
        (0.0, 0.0)
    } else {
        let previous = buckets[bucket_index - 1];
        (previous.upper_bound, previous.count)
    };

    let bucket_count = bucket.count - previous_count;
    if bucket_count <= 0.0 {
        return bucket.upper_bound;
    }
    lower_bound + (bucket.upper_bound - lower_bound) * ((rank - previous_count) / bucket_count)
}

fn native_histogram_quantile(quantile: f64, hist: &NativeHistogram) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mut buckets = native_histogram_buckets(hist);
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    let rank = quantile * hist.count;
    let mut cumulative = 0.0;
    for bucket in buckets {
        let previous = cumulative;
        cumulative += bucket.count;
        if cumulative < rank {
            continue;
        }
        if bucket.count <= 0.0 {
            return bucket.upper;
        }
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            return bucket.upper;
        }
        if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return bucket.lower;
        }
        return native_histogram_bucket_quantile(hist, bucket, (rank - previous) / bucket.count);
    }
    f64::NAN
}

fn native_histogram_fraction(lower: f64, upper: f64, hist: &NativeHistogram) -> f64 {
    if lower.is_nan() || upper.is_nan() || hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let in_range = native_histogram_buckets(hist)
        .into_iter()
        .map(|bucket| bucket.count * bucket_overlap_fraction(bucket, lower, upper))
        .sum::<f64>();
    in_range / hist.count
}

fn classic_histogram_fraction(lower: f64, upper: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let buckets = normalized_classic_histogram_buckets(buckets);
    if !buckets.last().is_some_and(|bucket| {
        bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
    }) {
        return f64::NAN;
    }

    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return f64::NAN;
    }

    classic_histogram_buckets(&buckets)
        .into_iter()
        .map(|bucket| bucket.count * bucket_overlap_fraction(bucket, lower, upper))
        .sum::<f64>()
        / total
}

fn normalized_classic_histogram_buckets(buckets: &mut [ClassicBucket]) -> Vec<ClassicBucket> {
    buckets.sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));

    let mut out: Vec<ClassicBucket> = Vec::with_capacity(buckets.len());
    for bucket in buckets.iter().copied() {
        if let Some(previous) = out.last_mut()
            && previous.upper_bound.total_cmp(&bucket.upper_bound).is_eq()
        {
            previous.count += bucket.count;
            continue;
        }
        out.push(bucket);
    }

    let mut max_count = 0.0_f64;
    for bucket in &mut out {
        max_count = max_count.max(bucket.count);
        bucket.count = max_count;
    }
    out
}

fn classic_histogram_buckets(buckets: &[ClassicBucket]) -> Vec<NativeQuantileBucket> {
    let mut out = Vec::with_capacity(buckets.len());
    let mut lower = if buckets
        .first()
        .is_some_and(|bucket| bucket.upper_bound <= 0.0)
    {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    let mut previous_count = 0.0;
    for bucket in buckets {
        let count = bucket.count - previous_count;
        previous_count = bucket.count;
        out.push(NativeQuantileBucket {
            lower,
            upper: bucket.upper_bound,
            count,
        });
        lower = bucket.upper_bound;
    }
    out
}

fn native_histogram_stdvar(hist: &NativeHistogram) -> f64 {
    if hist.count <= 0.0 || hist.count.is_nan() {
        return f64::NAN;
    }

    let mean = hist.sum / hist.count;
    native_histogram_buckets(hist)
        .into_iter()
        .map(|bucket| {
            let bucket_mean = native_histogram_bucket_mean(hist, bucket);
            bucket.count * (bucket_mean - mean).powi(2)
        })
        .sum::<f64>()
        / hist.count
}

fn add_compatible_native_histogram(
    left: &mut NativeHistogram,
    right: &NativeHistogram,
) -> Result<()> {
    if !native_histograms_have_compatible_metadata(left, right) {
        return Err(PromqlError::Unsupported(
            "incompatible native histogram aggregation is not implemented yet".to_string(),
        ));
    }

    left.zero_count += right.zero_count;
    left.count += right.count;
    left.sum += right.sum;
    (left.positive_spans, left.positive_counts) = add_spanned_histogram_counts(
        &left.positive_spans,
        &left.positive_counts,
        &right.positive_spans,
        &right.positive_counts,
    );
    (left.negative_spans, left.negative_counts) = add_spanned_histogram_counts(
        &left.negative_spans,
        &left.negative_counts,
        &right.negative_spans,
        &right.negative_counts,
    );
    left.start_timestamp_ms = match (left.start_timestamp_ms, right.start_timestamp_ms) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    };
    Ok(())
}

fn native_histograms_have_compatible_metadata(
    left: &NativeHistogram,
    right: &NativeHistogram,
) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.reset_hint == right.reset_hint
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
}

fn native_histograms_are_range_compatible(left: &NativeHistogram, right: &NativeHistogram) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && left.positive_counts.len() == right.positive_counts.len()
        && left.negative_counts.len() == right.negative_counts.len()
}

fn add_spanned_histogram_counts(
    left_spans: &[BucketSpan],
    left_counts: &[f64],
    right_spans: &[BucketSpan],
    right_counts: &[f64],
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut buckets = spanned_histogram_counts(left_spans, left_counts);
    for (index, count) in spanned_histogram_counts(right_spans, right_counts) {
        *buckets.entry(index).or_insert(0.0) += count;
    }
    compact_spanned_histogram_counts(buckets)
}

fn spanned_histogram_counts(spans: &[BucketSpan], counts: &[f64]) -> BTreeMap<i32, f64> {
    let mut buckets = BTreeMap::new();
    let mut index = 0_i32;
    let mut count_index = 0_usize;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return buckets;
            };
            buckets.insert(index, count);
            index += 1;
            count_index += 1;
        }
    }
    buckets
}

fn compact_spanned_histogram_counts(buckets: BTreeMap<i32, f64>) -> (Vec<BucketSpan>, Vec<f64>) {
    let buckets = buckets
        .into_iter()
        .filter(|(_, count)| *count != 0.0)
        .collect::<Vec<_>>();
    let mut spans = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0_i32;
    let mut previous_span_end = 0_i32;
    for (index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (spans, counts)
}

fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    scale_native_histogram_values(&mut out, factor);
    if factor.is_sign_negative() {
        out.reset_hint = ResetHint::Gauge;
    }
    out
}

fn scale_native_histogram_values(histogram: &mut NativeHistogram, factor: f64) {
    histogram.zero_count *= factor;
    histogram.count *= factor;
    histogram.sum *= factor;
    for count in &mut histogram.positive_counts {
        *count *= factor;
    }
    for count in &mut histogram.negative_counts {
        *count *= factor;
    }
}

fn native_histogram_bucket_mean(hist: &NativeHistogram, bucket: NativeQuantileBucket) -> f64 {
    if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
        return bucket.upper;
    }
    if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
        return bucket.lower;
    }
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return f64::midpoint(bucket.lower, bucket.upper);
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower * bucket.upper).sqrt();
    }
    (bucket.lower * bucket.upper).sqrt()
}

fn native_histogram_bucket_quantile(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
    fraction: f64,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower.abs() * (bucket.upper.abs() / bucket.lower.abs()).powf(fraction));
    }
    bucket.lower * (bucket.upper / bucket.lower).powf(fraction)
}

fn bucket_overlap_fraction(bucket: NativeQuantileBucket, lower: f64, upper: f64) -> f64 {
    if bucket.count == 0.0 || bucket.upper <= lower || bucket.lower >= upper {
        return 0.0;
    }
    let overlap_lower = bucket.lower.max(lower);
    let overlap_upper = bucket.upper.min(upper);
    if overlap_lower >= overlap_upper {
        return 0.0;
    }
    if bucket.lower.is_infinite() || bucket.upper.is_infinite() {
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            return f64::from(lower.is_infinite() && lower.is_sign_negative());
        }
        if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return f64::from(upper.is_infinite() && upper.is_sign_positive());
        }
        let covers_left = if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            lower.is_infinite() && lower.is_sign_negative()
        } else {
            lower <= bucket.lower
        };
        let covers_right = if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            upper.is_infinite() && upper.is_sign_positive()
        } else {
            upper >= bucket.upper
        };
        return f64::from(covers_left && covers_right);
    }
    (overlap_upper - overlap_lower) / (bucket.upper - bucket.lower)
}

fn native_histogram_buckets(hist: &NativeHistogram) -> Vec<NativeQuantileBucket> {
    let mut buckets = Vec::new();
    if hist.is_nhcb() {
        let custom_values = hist.custom_values.as_deref().unwrap_or_default();
        append_native_spanned_buckets(
            &mut buckets,
            &hist.positive_spans,
            &hist.positive_counts,
            |index| NativeQuantileBucket {
                lower: custom_histogram_bound(index - 1, custom_values),
                upper: custom_histogram_bound(index, custom_values),
                count: 0.0,
            },
        );
        return buckets;
    }

    append_native_spanned_buckets(
        &mut buckets,
        &hist.negative_spans,
        &hist.negative_counts,
        |index| NativeQuantileBucket {
            lower: -standard_histogram_bound(index, hist.schema),
            upper: -standard_histogram_bound(index - 1, hist.schema),
            count: 0.0,
        },
    );
    if hist.zero_count != 0.0 {
        buckets.push(NativeQuantileBucket {
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
        });
    }
    append_native_spanned_buckets(
        &mut buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| NativeQuantileBucket {
            lower: standard_histogram_bound(index - 1, hist.schema),
            upper: standard_histogram_bound(index, hist.schema),
            count: 0.0,
        },
    );
    buckets
}

fn append_native_spanned_buckets(
    buckets: &mut Vec<NativeQuantileBucket>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> NativeQuantileBucket,
) {
    let mut index: i32 = 0;
    let mut count_index = 0;
    for (span_index, span) in spans.iter().enumerate() {
        // A malformed span whose offset overflows the running bucket index is
        // dropped (the rest of the spans with it) rather than overflow-panicking
        // on the `i32` accumulation.
        index = if span_index == 0 {
            span.offset
        } else {
            let Some(next) = index.checked_add(span.offset) else {
                return;
            };
            next
        };
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return;
            };
            let mut bucket = bucket_for_index(index);
            bucket.count = count;
            buckets.push(bucket);
            // A span that would walk the index past `i32::MAX` is similarly
            // dropped rather than wrapping.
            let Some(next) = index.checked_add(1) else {
                return;
            };
            index = next;
            count_index += 1;
        }
    }
}

fn standard_histogram_bound(index: i32, schema: i8) -> f64 {
    2_f64.powf(f64::from(index) * 2_f64.powi(-i32::from(schema)))
}

fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 if custom_values.first().is_some_and(|value| *value > 0.0) => 0.0,
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}

#[derive(Clone, Copy)]
struct NativeQuantileBucket {
    lower: f64,
    upper: f64,
    count: f64,
}

#[derive(Clone, Copy)]
enum HistogramAccessor {
    Count,
    Sum,
    Avg,
    Stddev,
    Stdvar,
}

impl HistogramAccessor {
    fn value(self, hist: &NativeHistogram) -> f64 {
        match self {
            Self::Count => hist.count,
            Self::Sum => hist.sum,
            Self::Avg => hist.sum / hist.count,
            Self::Stddev => native_histogram_stdvar(hist).sqrt(),
            Self::Stdvar => native_histogram_stdvar(hist),
        }
    }
}

/// Map a native-histogram accessor function name to its [`HistogramAccessor`]
/// variant, mirroring the accessor arms of [`PromqlEngine::eval_instant_call`].
/// Returns `None` for any other function so the planner dispatch falls through.
fn histogram_accessor_from_function_name(name: &str) -> Option<HistogramAccessor> {
    Some(match name {
        "histogram_count" => HistogramAccessor::Count,
        "histogram_sum" => HistogramAccessor::Sum,
        "histogram_avg" => HistogramAccessor::Avg,
        "histogram_stddev" => HistogramAccessor::Stddev,
        "histogram_stdvar" => HistogramAccessor::Stdvar,
        _ => return None,
    })
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum ClampKind {
    Both,
    Min,
    Max,
}

#[cfg(test)]
impl ClampKind {
    fn argument_count(self) -> usize {
        match self {
            Self::Both => 3,
            Self::Min | Self::Max => 2,
        }
    }
}

#[derive(Clone, Copy)]
enum CalendarFn {
    Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
}

impl CalendarFn {
    #[allow(
        clippy::cast_possible_truncation,
        reason = "PromQL calendar functions interpret float sample values as Unix seconds"
    )]
    fn apply(self, unix_seconds: f64) -> f64 {
        if !unix_seconds.is_finite() {
            return f64::NAN;
        }
        let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(unix_seconds as i64) else {
            return f64::NAN;
        };
        match self {
            Self::Year => f64::from(timestamp.year()),
            Self::Month => f64::from(timestamp.month() as u8),
            Self::DayOfMonth => f64::from(timestamp.day()),
            Self::DayOfWeek => f64::from(timestamp.weekday().number_days_from_sunday()),
            Self::DayOfYear => f64::from(timestamp.ordinal()),
            Self::DaysInMonth => {
                f64::from(days_in_month(timestamp.year(), timestamp.month() as u8))
            }
            Self::Hour => f64::from(timestamp.hour()),
            Self::Minute => f64::from(timestamp.minute()),
        }
    }
}

/// Map a `PromQL` calendar-function name to its [`CalendarFn`] variant, mirroring
/// the calendar arms of [`PromqlEngine::eval_instant_call`]. Returns `None` for
/// any other function so the planner dispatch falls through.
fn calendar_fn_from_function_name(name: &str) -> Option<CalendarFn> {
    Some(match name {
        "year" => CalendarFn::Year,
        "month" => CalendarFn::Month,
        "day_of_month" => CalendarFn::DayOfMonth,
        "day_of_week" => CalendarFn::DayOfWeek,
        "day_of_year" => CalendarFn::DayOfYear,
        "days_in_month" => CalendarFn::DaysInMonth,
        "hour" => CalendarFn::Hour,
        "minute" => CalendarFn::Minute,
        _ => return None,
    })
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum SortDirection {
    Ascending,
    Descending,
}

#[cfg(test)]
impl From<SortDirection> for SortOrder {
    fn from(direction: SortDirection) -> Self {
        match direction {
            SortDirection::Ascending => Self::Ascending,
            SortDirection::Descending => Self::Descending,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum UnaryFloatFn {
    Ceil,
    Floor,
    Sgn,
    Abs,
    Sqrt,
    Exp,
    Ln,
    Log2,
    Log10,
    Sin,
    Sinh,
    Cos,
    Cosh,
    Tan,
    Tanh,
    Asin,
    Asinh,
    Acos,
    Acosh,
    Atan,
    Atanh,
    Deg,
    Rad,
}

#[cfg(test)]
impl UnaryFloatFn {
    fn apply(self, value: f64) -> f64 {
        match self {
            Self::Ceil => value.ceil(),
            Self::Floor => value.floor(),
            Self::Abs => value.abs(),
            Self::Sqrt => value.sqrt(),
            Self::Exp => value.exp(),
            Self::Ln => value.ln(),
            Self::Log2 => value.log2(),
            Self::Log10 => value.log10(),
            Self::Sin => value.sin(),
            Self::Sinh => value.sinh(),
            Self::Cos => value.cos(),
            Self::Cosh => value.cosh(),
            Self::Tan => value.tan(),
            Self::Tanh => value.tanh(),
            Self::Asin => value.asin(),
            Self::Asinh => value.asinh(),
            Self::Acos => value.acos(),
            Self::Acosh => value.acosh(),
            Self::Atan => value.atan(),
            Self::Atanh => value.atanh(),
            Self::Deg => value.to_degrees(),
            Self::Rad => value.to_radians(),
            Self::Sgn => {
                if value.is_nan() {
                    f64::NAN
                } else if value > 0.0 {
                    1.0
                } else if value < 0.0 {
                    -1.0
                } else {
                    0.0
                }
            }
        }
    }
}

#[cfg(test)]
fn clamp_float(value: f64, min: Option<f64>, max: Option<f64>) -> f64 {
    if min.is_some_and(f64::is_nan) || max.is_some_and(f64::is_nan) {
        return f64::NAN;
    }
    if let Some(min) = min
        && value < min
    {
        return min;
    }
    if let Some(max) = max
        && value > max
    {
        return max;
    }
    value
}

#[cfg(test)]
fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    (value / to_nearest + 0.5).floor() * to_nearest
}

fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[derive(Clone)]
struct FloatRow {
    fp: SeriesFingerprint,
    ts_ms: i64,
    value: f64,
}

/// Per-range-query float-scan cache (see [`Engine::scan_float_rows`]).
///
/// A range query evaluates the same selector at every step, and each step's
/// instant scan covers `[step - lookback, step]`. Those windows overlap almost
/// entirely, so a naive driver re-scans the store once per step (240× for a
/// 1h/15s query). This cache scans the union window `[start - lookback, end]`
/// **once per matcher set** and serves each step from the in-memory result —
/// the store is a pure time-range filter, so a filtered superset is byte-for-byte
/// what a direct sub-window scan returns (both stores keep `[start, end]`
/// inclusive). Only requests inside the pre-scanned union use the cache; an
/// `offset`/`@`-modified or long-`[range]` scan that falls outside it transparently
/// falls back to a direct scan, so results never change — only the redundant
/// re-scans are removed.
struct RangeScanCacheInner {
    full_start_ms: i64,
    full_end_ms: i64,
    floats: std::collections::HashMap<String, std::sync::Arc<Vec<FloatRow>>>,
    /// Per-matcher-set histogram rows over the union window. The instant-selector
    /// path probes for histogram series at every step (`selector_has_histogram_series`),
    /// a second per-step store scan alongside the float scan; cache it the same way.
    histograms: std::collections::HashMap<String, std::sync::Arc<Vec<HistogramRow>>>,
    /// Per-matcher-set fingerprint→labels resolution. A series' label set is
    /// immutable, so the union-window result is a superset of any sub-window's
    /// active series, and callers only ever use it as a `get(&fp)` lookup keyed
    /// by rows already filtered to the sub-window — extra entries are never read.
    labels: std::collections::HashMap<String, std::sync::Arc<BTreeMap<SeriesFingerprint, Labels>>>,
}

type RangeScanCache = std::sync::Arc<std::sync::Mutex<RangeScanCacheInner>>;

tokio::task_local! {
    /// Active only for the dynamic extent of [`Engine::eval_range_via_planner`]'s
    /// step loop. Nested range evals (subqueries) shadow it with their own cache
    /// and restore the outer one on exit, so each range scans its own union.
    static RANGE_SCAN_CACHE: RangeScanCache;
}

/// Deterministic cache key for a matcher set. `LabelMatcher` is not `Hash`, but
/// its `Debug` is stable and uniquely identifies the (name, op, value) triples
/// in order — sufficient because the same selector yields the same matcher list
/// at every step of a range query.
fn matchers_cache_key(matchers: &[LabelMatcher]) -> String {
    format!("{matchers:?}")
}

#[derive(Clone)]
struct HistogramRow {
    fp: SeriesFingerprint,
    ts_ms: i64,
    hist: NativeHistogram,
}

#[derive(Clone, Copy)]
enum AggregateOp {
    Sum,
    Avg,
    Count,
    Group,
    Min,
    Max,
    Stddev,
    Stdvar,
}

impl AggregateOp {
    #[cfg(test)]
    fn try_from_token(token: TokenType) -> Result<Self> {
        match token.id() {
            T_SUM => Ok(Self::Sum),
            T_AVG => Ok(Self::Avg),
            T_COUNT => Ok(Self::Count),
            T_GROUP => Ok(Self::Group),
            T_MIN => Ok(Self::Min),
            T_MAX => Ok(Self::Max),
            T_STDDEV => Ok(Self::Stddev),
            T_STDVAR => Ok(Self::Stdvar),
            _ => Err(PromqlError::Unsupported(format!(
                "aggregation `{token}` is not implemented yet"
            ))),
        }
    }

    fn finish(self, state: &AggregateState) -> Option<SampleValue> {
        if state.count == 0 || state.invalid_mixed_sample_type {
            return None;
        }
        Some(match self {
            Self::Sum => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(histogram.clone()),
                None => SampleValue::Float(state.sum),
            },
            Self::Avg => match &state.histogram {
                Some(histogram) => SampleValue::Histogram(scaled_native_histogram(
                    histogram,
                    1.0 / state.count_f64,
                )),
                None => SampleValue::Float(state.avg_mean + state.avg_comp),
            },
            Self::Count => SampleValue::Float(state.count_f64),
            Self::Group => SampleValue::Float(1.0),
            Self::Min => SampleValue::Float(state.min),
            Self::Max => SampleValue::Float(state.max),
            Self::Stddev => SampleValue::Float(state.population_variance().sqrt()),
            Self::Stdvar => SampleValue::Float(state.population_variance()),
        })
    }

    fn ignores_histograms(self) -> bool {
        matches!(self, Self::Min | Self::Max | Self::Stddev | Self::Stdvar)
    }

    fn counts_histograms(self) -> bool {
        matches!(self, Self::Count | Self::Group)
    }

    fn aggregates_histograms(self) -> bool {
        matches!(self, Self::Sum | Self::Avg)
    }
}

struct AggregateState {
    labels: Labels,
    count: usize,
    count_f64: f64,
    sum: f64,
    /// Incremental Kahan-compensated mean for `avg` (`avg_mean + avg_comp`),
    /// matching Prometheus. The naive `sum / count` overflows to ±Inf for
    /// very-large-magnitude groups; the incremental form stays finite (and, once
    /// it does saturate, preserves the same-sign-infinity handling).
    avg_mean: f64,
    avg_comp: f64,
    /// Welford running mean / `M2` accumulators for `stddev`/`stdvar`, each
    /// Kahan-compensated. The naive `E[x^2] - E[x]^2` form suffers catastrophic
    /// cancellation for large-magnitude close-valued groups (a negative variance
    /// whose `sqrt` is NaN); Welford stays stable and matches Prometheus.
    var_mean: f64,
    var_mean_comp: f64,
    var_aux: f64,
    var_aux_comp: f64,
    /// Running `min`/`max` over the group's float samples. Prometheus' `min`/
    /// `max` *ignore* NaN: a group's extremum is taken over its non-NaN values,
    /// and the result is NaN only when **every** sample is NaN. We mirror
    /// Prometheus' aggregation loop exactly (`promql/engine.go`): the running
    /// value is seeded with the first sample (NaN included), and each subsequent
    /// sample `f` replaces it when `running {>,<} f` *or* `running` is NaN. So a
    /// later non-NaN always displaces an earlier NaN, and an all-NaN group keeps
    /// NaN. `seen_float` tracks whether the seed has been taken.
    seen_float: bool,
    min: f64,
    max: f64,
    histogram: Option<NativeHistogram>,
    invalid_mixed_sample_type: bool,
}

impl AggregateState {
    fn new(labels: Labels) -> Self {
        Self {
            labels,
            count: 0,
            count_f64: 0.0,
            sum: 0.0,
            avg_mean: 0.0,
            avg_comp: 0.0,
            var_mean: 0.0,
            var_mean_comp: 0.0,
            var_aux: 0.0,
            var_aux_comp: 0.0,
            seen_float: false,
            min: f64::NAN,
            max: f64::NAN,
            histogram: None,
            invalid_mixed_sample_type: false,
        }
    }

    fn push_float(&mut self, value: f64) {
        self.push_observation();
        self.sum += value;

        // Incremental Kahan-compensated mean for `avg` (Prometheus' `avg_over`-
        // style fold), keeping the running mean finite past naive-sum overflow.
        // Once the mean is infinite, a same-sign infinity or any finite sample
        // leaves it unchanged (only a flip to the opposite infinity / a NaN moves
        // it), exactly as Prometheus' `avg` aggregation does.
        let keep_infinite_mean = self.avg_mean.is_infinite()
            && ((value.is_infinite() && (value > 0.0) == (self.avg_mean > 0.0))
                || (!value.is_infinite() && !value.is_nan()));
        if !keep_infinite_mean {
            let (mean, comp) = kahan_sum_inc(
                value / self.count_f64 - self.avg_mean / self.count_f64,
                self.avg_mean,
                self.avg_comp,
            );
            self.avg_mean = mean;
            self.avg_comp = comp;
        }

        // Welford + Kahan variance accumulation for `stddev`/`stdvar`.
        let delta = value - (self.var_mean + self.var_mean_comp);
        let (var_mean, var_mean_comp) =
            kahan_sum_inc(delta / self.count_f64, self.var_mean, self.var_mean_comp);
        self.var_mean = var_mean;
        self.var_mean_comp = var_mean_comp;
        let (var_aux, var_aux_comp) = kahan_sum_inc(
            delta * (value - (self.var_mean + self.var_mean_comp)),
            self.var_aux,
            self.var_aux_comp,
        );
        self.var_aux = var_aux;
        self.var_aux_comp = var_aux_comp;

        if self.seen_float {
            // Replace the running extremum when the new sample wins under the
            // float ordering, or when the running value is NaN (so a non-NaN
            // sample displaces a NaN seed). `NaN > _` / `NaN < _` are false, so
            // a NaN sample never displaces an existing non-NaN extremum.
            if self.min > value || self.min.is_nan() {
                self.min = value;
            }
            if self.max < value || self.max.is_nan() {
                self.max = value;
            }
        } else {
            // First sample seeds both extrema (NaN included).
            self.seen_float = true;
            self.min = value;
            self.max = value;
        }
    }

    fn push_observation(&mut self) {
        self.count += 1;
        self.count_f64 += 1.0;
    }

    fn push_histogram(&mut self, histogram: NativeHistogram) -> Result<()> {
        if self.invalid_mixed_sample_type {
            return Ok(());
        }
        if self.count != 0 && self.histogram.is_none() {
            self.mark_invalid_mixed_sample_type();
            return Ok(());
        }
        self.push_observation();
        match &mut self.histogram {
            Some(existing) => add_compatible_native_histogram(existing, &histogram)?,
            None => self.histogram = Some(histogram),
        }
        Ok(())
    }

    fn mark_invalid_mixed_sample_type(&mut self) {
        self.invalid_mixed_sample_type = true;
        self.histogram = None;
    }

    fn has_histogram(&self) -> bool {
        self.histogram.is_some()
    }

    fn population_variance(&self) -> f64 {
        // Welford `M2 / n` (the running `var_aux` already accumulates the sum of
        // squared deviations from the running mean), Kahan-corrected.
        (self.var_aux + self.var_aux_comp) / self.count_f64
    }
}

enum InstantValue {
    Scalar(f64),
    Vector(Vec<InstantSample>),
}

#[cfg(test)]
impl InstantValue {
    fn try_from_query(result: QueryResult) -> Result<Self> {
        match result {
            QueryResult::Scalar { value, .. } => Ok(Self::Scalar(value)),
            QueryResult::InstantVector(samples) => Ok(Self::Vector(samples)),
            QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
                "binary expression requires instant operands".to_string(),
            )),
            QueryResult::Str { .. } => Err(PromqlError::Plan(
                "binary expression does not support string operands".to_string(),
            )),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RangeFn {
    Rate,
    Increase,
    Delta,
    Changes,
    Resets,
}

#[derive(Clone, Copy)]
enum IrateFn {
    Irate,
    Idelta,
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
enum ScalarExtremaFn {
    Max,
    Min,
}

#[cfg(feature = "experimental-functions")]
impl ScalarExtremaFn {
    fn apply(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Max => left.max(right),
            Self::Min => left.min(right),
        }
    }
}

#[derive(Clone, Copy)]
enum OverTimeFn {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Stddev,
    Stdvar,
    Mad,
    First,
    Last,
    TsOfFirst,
    TsOfLast,
    TsOfMin,
    TsOfMax,
    Present,
}

impl OverTimeFn {
    fn preserves_metric_name(self) -> bool {
        matches!(self, Self::First | Self::Last)
    }
}

/// Wrap a scalar [`QueryResult`] from a delegated interpreter call (the
/// experimental `max_of`/`min_of` extrema and the `range`/`step`/`start`/`end`
/// duration helpers) into a [`PlannedInstant::PrecomputedScalar`]. A non-scalar
/// result is impossible for these callers (they always return a scalar) but is
/// mapped to a canonical error defensively rather than panicking.
#[cfg(feature = "experimental-functions")]
fn scalar_call_to_planned(result: &QueryResult) -> Result<PlannedInstant> {
    match *result {
        QueryResult::Scalar { ts_ms, value } => {
            Ok(PlannedInstant::PrecomputedScalar { ts_ms, value })
        }
        _ => Err(PromqlError::Plan(
            "expected a scalar result from an experimental scalar call".to_string(),
        )),
    }
}

/// Negate an already-evaluated instant query result, mirroring the `PromQL` unary
/// `-` operator: a scalar flips sign; an instant vector flips each sample
/// (floats by negation, native histograms by `scaled_native_histogram(_, -1.0)`)
/// and drops `__name__`; a range-matrix / string input is a hard error (matching
/// the interpreter's `eval_instant_unary`). Both the interpreter and the operator
/// path (`plan_unary_expr`) route through this, so they cannot diverge.
fn negate_query_result(operand: QueryResult) -> Result<QueryResult> {
    match operand {
        QueryResult::Scalar { ts_ms, value } => Ok(QueryResult::Scalar {
            ts_ms,
            value: -value,
        }),
        QueryResult::InstantVector(samples) => Ok(QueryResult::InstantVector(
            samples
                .into_iter()
                .map(|mut sample| {
                    sample.value = match sample.value {
                        SampleValue::Float(value) => SampleValue::Float(-value),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, -1.0))
                        }
                    };
                    sample.labels = labels_without_metric_name(&sample.labels);
                    sample
                })
                .collect(),
        )),
        QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
            "unary expression requires scalar or instant-vector input".to_string(),
        )),
        QueryResult::Str { .. } => Err(PromqlError::Plan(
            "unary expression does not support string input".to_string(),
        )),
    }
}

/// A range/`*_over_time` function applied to an already-evaluated range vector
/// (a [`RangeEval`]), carrying any scalar parameters resolved by the caller.
///
/// This is the **outer** half of a range-function evaluation: the per-series
/// fold that turns each window of `(end - range, end]` samples into one instant
/// sample. Both the interpreter (`eval_*_call`) and the recursive planner's
/// subquery dispatch construct one of these and apply it via
/// [`apply_outer_range_fn`], so the operator path matches the interpreter
/// byte-for-byte for whichever underlying range vector it was handed.
///
/// `absent` / `absent_over_time` (which synthesize an absent-labels series) and
/// the scalar-typed helpers (`time`/`pi`/…) are *not* range-vector folds and are
/// not represented here. The experimental `double_exponential_smoothing` carries
/// its two factors; under the non-experimental build it is unreachable.
#[derive(Clone, Copy)]
enum OuterRangeFn {
    Range(RangeFn),
    InstantDelta(IrateFn),
    Deriv,
    OverTime(OverTimeFn),
    QuantileOverTime(f64),
    PredictLinear(f64),
    #[cfg(feature = "experimental-functions")]
    DoubleExponentialSmoothing {
        smoothing: f64,
        trend: f64,
    },
}

/// Apply an [`OuterRangeFn`] over an already-evaluated range vector, producing the
/// instant vector at `time_ms`. This is the single shared implementation of every
/// range/`*_over_time` function's per-series fold; the interpreter and the
/// planner's subquery path both route through it so they cannot diverge.
fn apply_outer_range_fn(range: RangeEval, outer: OuterRangeFn, time_ms: i64) -> Vec<InstantSample> {
    range
        .series
        .into_iter()
        .filter_map(|series| {
            outer_range_sample_from_series(
                &series,
                range.end_ms,
                range.range_ms,
                outer,
                range.modifier,
            )
            .map(|(labels, value)| InstantSample {
                labels,
                ts_ms: time_ms,
                value,
            })
        })
        .collect()
}

/// Fold one series' window into its `(result labels, value)`, mirroring exactly
/// what each interpreter `eval_*_call` does per series. Returns `None` for a
/// no-value window (the series is dropped from the result).
fn outer_range_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    outer: OuterRangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<(Labels, SampleValue)> {
    match outer {
        OuterRangeFn::Range(kind) => {
            range_function_sample_from_series(series, range_end_ms, range_ms, kind, modifier)
                .map(|value| (labels_without_metric_name(&series.labels), value))
        }
        OuterRangeFn::InstantDelta(kind) => {
            instant_delta_sample_from_series(series, range_end_ms, range_ms, kind).map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
        OuterRangeFn::Deriv => {
            deriv_sample_from_series(series, range_end_ms, range_ms).map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
        OuterRangeFn::OverTime(kind) => {
            over_time_sample_from_series(series, range_end_ms, range_ms, kind).map(|value| {
                let labels = if kind.preserves_metric_name() {
                    series.labels.clone()
                } else {
                    labels_without_metric_name(&series.labels)
                };
                (labels, value)
            })
        }
        OuterRangeFn::QuantileOverTime(quantile) => {
            quantile_over_time_sample_from_series(series, range_end_ms, range_ms, quantile).map(
                |value| {
                    (
                        labels_without_metric_name(&series.labels),
                        SampleValue::Float(value),
                    )
                },
            )
        }
        OuterRangeFn::PredictLinear(duration_seconds) => {
            predict_linear_sample_from_series(series, range_end_ms, range_ms, duration_seconds).map(
                |value| {
                    (
                        labels_without_metric_name(&series.labels),
                        SampleValue::Float(value),
                    )
                },
            )
        }
        #[cfg(feature = "experimental-functions")]
        OuterRangeFn::DoubleExponentialSmoothing { smoothing, trend } => {
            double_exponential_smoothing_sample_from_series(
                series,
                range_end_ms,
                range_ms,
                smoothing,
                trend,
            )
            .map(|value| {
                (
                    labels_without_metric_name(&series.labels),
                    SampleValue::Float(value),
                )
            })
        }
    }
}

fn range_function_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<SampleValue> {
    let range_start_ms = range_end_ms.saturating_sub(range_ms);
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let mut histograms = Vec::new();
    for (timestamp, value) in &series.samples {
        let in_range = match modifier {
            Some(ExtendedSelectorModifier::Anchored) => *timestamp <= range_end_ms,
            Some(ExtendedSelectorModifier::Smoothed) => true,
            None => *timestamp > range_start_ms && *timestamp <= range_end_ms,
        };
        if !in_range {
            continue;
        }
        match value {
            SampleValue::Float(value) => {
                if !histograms.is_empty() {
                    return None;
                }
                timestamps.push(*timestamp);
                values.push(*value);
            }
            SampleValue::Histogram(histogram) => {
                if !values.is_empty() {
                    return None;
                }
                timestamps.push(*timestamp);
                histograms.push(histogram.clone());
            }
        }
    }

    if matches!(modifier, Some(ExtendedSelectorModifier::Anchored)) && !values.is_empty() {
        let value =
            anchored_float_range_value(&timestamps, &values, range_start_ms, range_ms, kind)?;
        return Some(SampleValue::Float(value));
    }
    if matches!(modifier, Some(ExtendedSelectorModifier::Smoothed)) && !values.is_empty() {
        let value = smoothed_float_range_value(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range_ms,
            kind,
        )?;
        return Some(SampleValue::Float(value));
    }

    if !histograms.is_empty() {
        if matches!(kind, RangeFn::Resets) {
            return count_histogram_resets(&histograms).map(SampleValue::Float);
        }
        return range_histogram_sample(
            &timestamps,
            &histograms,
            range_start_ms,
            range_end_ms,
            range_ms,
            kind,
        )
        .map(SampleValue::Histogram);
    }
    let value = match kind {
        RangeFn::Changes => count_changes(&values),
        RangeFn::Resets => count_resets(&values),
        RangeFn::Rate | RangeFn::Increase | RangeFn::Delta => extrapolated_rate(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range_ms,
            kind,
        ),
    }?;
    Some(SampleValue::Float(value))
}

#[allow(clippy::cast_precision_loss)]
fn anchored_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_ms: i64,
    kind: RangeFn,
) -> Option<f64> {
    let mut selected = Vec::new();
    if matches!(kind, RangeFn::Changes | RangeFn::Resets) {
        let has_after_start = timestamps
            .iter()
            .any(|timestamp| *timestamp > range_start_ms);
        if has_after_start {
            if let Some(index) = timestamps
                .iter()
                .rposition(|timestamp| *timestamp <= range_start_ms)
            {
                selected.push((*timestamps.get(index)?, values.get(index).copied()?));
            }
            selected.extend(timestamps.iter().zip(values.iter()).filter_map(
                |(timestamp, value)| (*timestamp > range_start_ms).then_some((*timestamp, *value)),
            ));
        } else if let Some(start_index) = timestamps
            .iter()
            .position(|timestamp| *timestamp == range_start_ms)
        {
            if let Some(previous_index) = timestamps[..start_index]
                .iter()
                .rposition(|timestamp| *timestamp < range_start_ms)
            {
                selected.push((
                    *timestamps.get(previous_index)?,
                    values.get(previous_index).copied()?,
                ));
            }
            selected.push((
                *timestamps.get(start_index)?,
                values.get(start_index).copied()?,
            ));
        }
    } else {
        if let Some(index) = timestamps
            .iter()
            .rposition(|timestamp| *timestamp <= range_start_ms)
        {
            selected.push((*timestamps.get(index)?, values.get(index).copied()?));
        }
        selected.extend(
            timestamps
                .iter()
                .zip(values.iter())
                .filter_map(|(timestamp, value)| {
                    (*timestamp > range_start_ms).then_some((*timestamp, *value))
                }),
        );
    }
    if selected.is_empty() {
        return None;
    }
    if selected.len() == 1 && selected[0].0 <= range_start_ms {
        return None;
    }
    let selected_values = selected.iter().map(|(_, value)| *value).collect::<Vec<_>>();

    match kind {
        RangeFn::Changes => count_changes(&selected_values),
        RangeFn::Resets => count_resets(&selected_values),
        RangeFn::Delta => Some(selected_values.last()? - selected_values.first()?),
        RangeFn::Increase | RangeFn::Rate => {
            let result = counter_delta(&selected_values)?;
            if kind == RangeFn::Rate {
                let range_seconds = range_ms as f64 / 1000.0;
                if range_seconds <= 0.0 {
                    return None;
                }
                Some(result / range_seconds)
            } else {
                let _ = timestamps;
                Some(result)
            }
        }
    }
}

fn counter_delta(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return Some(0.0);
    }
    let mut result = values.last()? - values.first()?;
    for window in values.windows(2) {
        if window[1] < window[0] {
            result += window[0];
        }
    }
    Some(result)
}

#[allow(clippy::cast_precision_loss)]
fn smoothed_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
) -> Option<f64> {
    if !matches!(kind, RangeFn::Delta | RangeFn::Increase | RangeFn::Rate) {
        return None;
    }
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }

    let smoothed_values = if matches!(kind, RangeFn::Increase | RangeFn::Rate) {
        counter_corrected_values(values)?
    } else {
        values.to_vec()
    };
    let start = boundary_value(timestamps, &smoothed_values, range_start_ms)?;
    let end = boundary_value(timestamps, &smoothed_values, range_end_ms)?;
    let mut result = end - start;
    if matches!(kind, RangeFn::Increase | RangeFn::Rate) && result < 0.0 {
        result = 0.0;
    }
    if kind == RangeFn::Rate {
        let range_seconds = range_ms as f64 / 1000.0;
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}

fn counter_corrected_values(values: &[f64]) -> Option<Vec<f64>> {
    let mut out = Vec::with_capacity(values.len());
    let mut correction = 0.0;
    let mut previous = *values.first()?;
    out.push(previous);
    for &value in &values[1..] {
        if value < previous {
            correction += previous;
        }
        out.push(value + correction);
        previous = value;
    }
    Some(out)
}

#[allow(clippy::cast_precision_loss)]
fn boundary_value(timestamps: &[i64], values: &[f64], target_ms: i64) -> Option<f64> {
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if timestamps.len() == 1 {
        return values.first().copied();
    }
    if let Some(index) = timestamps
        .iter()
        .position(|timestamp| *timestamp == target_ms)
    {
        return values.get(index).copied();
    }
    if let Some(after_index) = timestamps
        .iter()
        .position(|timestamp| *timestamp > target_ms)
    {
        if after_index == 0 {
            return values.first().copied();
        }
        return interpolate_boundary(
            timestamps[after_index - 1],
            values[after_index - 1],
            timestamps[after_index],
            values[after_index],
            target_ms,
        );
    }
    let last_index = timestamps.len() - 1;
    let interval = timestamps[last_index].saturating_sub(timestamps[last_index - 1]);
    if target_ms.saturating_sub(timestamps[last_index]) as f64 > interval as f64 * 1.1 {
        return values.last().copied();
    }
    interpolate_boundary(
        timestamps[last_index - 1],
        values[last_index - 1],
        timestamps[last_index],
        values[last_index],
        target_ms,
    )
}

fn instant_smoothed_boundary_value(
    timestamps: &[i64],
    values: &[f64],
    target_ms: i64,
) -> Option<f64> {
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if target_ms <= *timestamps.first()? {
        return values.first().copied();
    }
    if target_ms >= *timestamps.last()? {
        return values.last().copied();
    }
    boundary_value(timestamps, values, target_ms)
}

#[allow(clippy::cast_precision_loss)]
fn interpolate_boundary(
    left_ts: i64,
    left_value: f64,
    right_ts: i64,
    right_value: f64,
    target_ms: i64,
) -> Option<f64> {
    let interval = (right_ts - left_ts) as f64;
    if interval <= 0.0 {
        return None;
    }
    let ratio = (target_ms - left_ts) as f64 / interval;
    Some(left_value + (right_value - left_value) * ratio)
}

fn range_histogram_sample(
    timestamps: &[i64],
    histograms: &[NativeHistogram],
    range_start_ms: i64,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
) -> Option<NativeHistogram> {
    if !matches!(kind, RangeFn::Rate | RangeFn::Increase | RangeFn::Delta) || histograms.len() < 2 {
        return None;
    }
    let first = histograms.first()?;
    let last = histograms.last()?;
    if !histograms
        .windows(2)
        .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    let resets = histogram_reset_indices(histograms);
    let extrapolation = HistogramExtrapolation {
        timestamps,
        reset_indices: &resets,
        range_start_ms,
        range_end_ms,
        range_ms,
        kind,
    };

    let mut out = last.clone();
    out.count = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.count)
            .collect::<Vec<_>>(),
    )?;
    out.sum = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.sum)
            .collect::<Vec<_>>(),
    )?;
    out.zero_count = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.zero_count)
            .collect::<Vec<_>>(),
    )?;
    out.positive_counts = extrapolated_histogram_counts(&extrapolation, histograms, |histogram| {
        &histogram.positive_counts
    })?;
    (out.positive_spans, out.positive_counts) =
        compact_histogram_spans(&out.positive_spans, &out.positive_counts);
    out.negative_counts = extrapolated_histogram_counts(&extrapolation, histograms, |histogram| {
        &histogram.negative_counts
    })?;
    (out.negative_spans, out.negative_counts) =
        compact_histogram_spans(&out.negative_spans, &out.negative_counts);
    if matches!(kind, RangeFn::Delta) || out.is_nhcb() && !resets.is_empty() {
        out.reset_hint = ResetHint::Gauge;
    }
    out.start_timestamp_ms = first.start_timestamp_ms.or(last.start_timestamp_ms);
    Some(out)
}

fn compact_histogram_spans(spans: &[BucketSpan], counts: &[f64]) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut index = 0;
    let mut count_index = 0;
    let mut buckets = Vec::new();
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                break;
            };
            buckets.push((index, count));
            index += 1;
            count_index += 1;
        }
    }
    let Some(first_non_zero) = buckets.iter().position(|(_, count)| *count != 0.0) else {
        return (Vec::new(), Vec::new());
    };
    let last_non_zero = buckets
        .iter()
        .rposition(|(_, count)| *count != 0.0)
        .expect("first non-zero bucket exists");
    let buckets = &buckets[first_non_zero..=last_non_zero];

    let mut compacted_spans = Vec::new();
    let mut compacted_counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0;
    let mut previous_span_end = 0;
    for &(index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            compacted_spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        compacted_counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        compacted_spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (compacted_spans, compacted_counts)
}

fn extrapolated_histogram_counts(
    extrapolation: &HistogramExtrapolation<'_>,
    histograms: &[NativeHistogram],
    counts: impl Fn(&NativeHistogram) -> &[f64],
) -> Option<Vec<f64>> {
    let bucket_count = counts(histograms.first()?).len();
    let mut out = Vec::with_capacity(bucket_count);
    for index in 0..bucket_count {
        let values = histograms
            .iter()
            .map(|histogram| counts(histogram).get(index).copied())
            .collect::<Option<Vec<_>>>()?;
        out.push(extrapolated_histogram_component(extrapolation, &values)?);
    }
    Some(out)
}

struct HistogramExtrapolation<'a> {
    timestamps: &'a [i64],
    reset_indices: &'a [usize],
    range_start_ms: i64,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
}

fn count_histogram_resets(histograms: &[NativeHistogram]) -> Option<f64> {
    if histograms.len() < 2
        || !histograms
            .windows(2)
            .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    Some(
        histogram_reset_indices(histograms)
            .iter()
            .map(|_| 1.0)
            .sum(),
    )
}

fn histogram_reset_indices(histograms: &[NativeHistogram]) -> Vec<usize> {
    histograms
        .windows(2)
        .enumerate()
        .filter_map(|(index, window)| {
            histogram_reset_between(&window[0], &window[1]).then_some(index + 1)
        })
        .collect()
}

fn histogram_reset_between(previous: &NativeHistogram, current: &NativeHistogram) -> bool {
    current.count < previous.count
        || current.sum < previous.sum
        || current.zero_count < previous.zero_count
        || histogram_counts_reset(&previous.positive_counts, &current.positive_counts)
        || histogram_counts_reset(&previous.negative_counts, &current.negative_counts)
}

fn histogram_counts_reset(previous: &[f64], current: &[f64]) -> bool {
    previous
        .iter()
        .zip(current.iter())
        .any(|(previous, current)| current < previous)
}

fn extrapolated_histogram_component(
    extrapolation: &HistogramExtrapolation<'_>,
    values: &[f64],
) -> Option<f64> {
    if matches!(extrapolation.kind, RangeFn::Delta) {
        return extrapolated_rate(
            extrapolation.timestamps,
            values,
            extrapolation.range_start_ms,
            extrapolation.range_end_ms,
            extrapolation.range_ms,
            extrapolation.kind,
        );
    }

    let n = extrapolation.timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let mut result = values[n - 1] - values[0];
    for &reset_index in extrapolation.reset_indices {
        result += values.get(reset_index.checked_sub(1)?)?;
    }

    extrapolate_histogram_delta(
        extrapolation.timestamps,
        result,
        extrapolation.range_start_ms,
        extrapolation.range_end_ms,
        extrapolation.range_ms,
        extrapolation.kind,
    )
}

#[allow(clippy::cast_precision_loss)]
fn extrapolate_histogram_delta(
    timestamps: &[i64],
    mut result: f64,
    range_start_ms: i64,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
) -> Option<f64> {
    let n = timestamps.len();
    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts) as f64 / 1000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_duration_between_samples = sampled_interval / (n - 1) as f64;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms) as f64 / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts) as f64 / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    let extrapolated_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolated_interval / sampled_interval;
    if kind == RangeFn::Rate {
        result /= range_ms as f64 / 1000.0;
    }
    Some(result)
}

fn instant_delta_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    kind: IrateFn,
) -> Option<f64> {
    let range_start_ms = range_end_ms.saturating_sub(range_ms);
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    for (timestamp, value) in &series.samples {
        if *timestamp <= range_start_ms || *timestamp > range_end_ms {
            continue;
        }
        let SampleValue::Float(value) = value else {
            return None;
        };
        timestamps.push(*timestamp);
        values.push(*value);
    }
    instant_delta(&timestamps, &values, kind)
}

fn over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    kind: OverTimeFn,
) -> Option<SampleValue> {
    if matches!(
        kind,
        OverTimeFn::Count | OverTimeFn::First | OverTimeFn::Last | OverTimeFn::Present
    ) {
        let sample_count = range_sample_count(series, range_end_ms, range_ms);
        if sample_count == 0 {
            return None;
        }
        return match kind {
            OverTimeFn::Count => Some(SampleValue::Float((0..sample_count).map(|_| 1.0).sum())),
            OverTimeFn::First => range_samples(series, range_end_ms, range_ms)
                .min_by_key(|(timestamp, _)| *timestamp)
                .map(|(_, value)| value.clone()),
            OverTimeFn::Last => range_samples(series, range_end_ms, range_ms)
                .max_by_key(|(timestamp, _)| *timestamp)
                .map(|(_, value)| value.clone()),
            OverTimeFn::Present => Some(SampleValue::Float(1.0)),
            _ => unreachable!("over_time histogram-safe kind checked above"),
        };
    }

    if matches!(kind, OverTimeFn::Sum | OverTimeFn::Avg) {
        let histograms = histogram_range_samples(series, range_end_ms, range_ms);
        if !histograms.is_empty() {
            return over_time_histogram_sample(&histograms, kind).map(SampleValue::Histogram);
        }
    }

    let samples = float_range_samples(series, range_end_ms, range_ms);
    if samples.is_empty() {
        return None;
    }

    let value = match kind {
        OverTimeFn::Sum => samples.iter().map(|(_, value)| value).sum(),
        OverTimeFn::Avg => over_time_mean(samples.iter().map(|(_, value)| *value)),
        OverTimeFn::Count => unreachable!("count_over_time handled before float extraction"),
        OverTimeFn::Min => fold_over_time_extremum(&samples, ExtremumKind::Min),
        OverTimeFn::Max => fold_over_time_extremum(&samples, ExtremumKind::Max),
        OverTimeFn::Stddev => over_time_variance(&samples).sqrt(),
        OverTimeFn::Stdvar => over_time_variance(&samples),
        OverTimeFn::Mad => over_time_mad(&samples).expect("non-empty samples"),
        OverTimeFn::First => samples
            .into_iter()
            .min_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, value)| value)
            .expect("non-empty samples"),
        OverTimeFn::Last => samples
            .into_iter()
            .max_by_key(|(timestamp, _)| *timestamp)
            .map(|(_, value)| value)
            .expect("non-empty samples"),
        OverTimeFn::TsOfFirst => timestamp_seconds(
            samples
                .into_iter()
                .min_by_key(|(timestamp, _)| *timestamp)
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfLast => timestamp_seconds(
            samples
                .into_iter()
                .max_by_key(|(timestamp, _)| *timestamp)
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfMin => timestamp_seconds(
            samples
                .into_iter()
                .min_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| right.0.cmp(&left.0))
                })
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::TsOfMax => timestamp_seconds(
            samples
                .into_iter()
                .max_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.0.cmp(&right.0))
                })
                .map(|(timestamp, _)| timestamp)
                .expect("non-empty samples"),
        ),
        OverTimeFn::Present => unreachable!("present_over_time handled before float extraction"),
    };
    Some(SampleValue::Float(value))
}

fn over_time_histogram_sample(
    histograms: &[NativeHistogram],
    kind: OverTimeFn,
) -> Option<NativeHistogram> {
    let mut out = histograms.first()?.clone();
    for histogram in &histograms[1..] {
        add_compatible_native_histogram(&mut out, histogram).ok()?;
    }
    if matches!(kind, OverTimeFn::Avg) {
        let count: f64 = histograms.iter().map(|_| 1.0).sum();
        scale_native_histogram_values(&mut out, 1.0 / count);
    }
    Some(out)
}

fn quantile_over_time_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    quantile: f64,
) -> Option<f64> {
    let mut values = float_range_samples(series, range_end_ms, range_ms)
        .into_iter()
        .map(|(_, value)| value)
        .collect::<Vec<_>>();
    quantile_value(quantile, &mut values)
}

fn deriv_sample_from_series(series: &RangeSeries, range_end_ms: i64, range_ms: i64) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range_ms);
    regression_slope(&samples, range_end_ms)
}

fn float_range_samples(series: &RangeSeries, range_end_ms: i64, range_ms: i64) -> Vec<(i64, f64)> {
    let range_start_ms = range_end_ms.saturating_sub(range_ms);
    series
        .samples
        .iter()
        .filter_map(|(timestamp, value)| {
            if *timestamp <= range_start_ms || *timestamp > range_end_ms {
                return None;
            }
            let SampleValue::Float(value) = value else {
                return None;
            };
            Some((*timestamp, *value))
        })
        .collect()
}

fn histogram_range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
) -> Vec<NativeHistogram> {
    range_samples(series, range_end_ms, range_ms)
        .filter_map(|(_, value)| match value {
            SampleValue::Histogram(histogram) => Some(histogram.clone()),
            SampleValue::Float(_) => None,
        })
        .collect()
}

fn range_has_samples(series: &RangeSeries, range_end_ms: i64, range_ms: i64) -> bool {
    range_sample_count(series, range_end_ms, range_ms) != 0
}

fn range_sample_count(series: &RangeSeries, range_end_ms: i64, range_ms: i64) -> usize {
    range_samples(series, range_end_ms, range_ms).count()
}

fn range_samples(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
) -> impl Iterator<Item = (i64, &SampleValue)> {
    let range_start_ms = range_end_ms.saturating_sub(range_ms);
    series
        .samples
        .iter()
        .filter(move |(timestamp, _)| *timestamp > range_start_ms && *timestamp <= range_end_ms)
        .map(|(timestamp, value)| (*timestamp, value))
}

/// Which extremum [`fold_over_time_extremum`] tracks.
#[derive(Clone, Copy)]
enum ExtremumKind {
    Min,
    Max,
}

impl ExtremumKind {
    /// Whether the running value `running` should be replaced by `candidate`
    /// under Prometheus' NaN-ignoring float ordering — the same rule
    /// `AggregateState::push_float` and the `prom_min`/`prom_max` aggregate UDAF
    /// apply. A NaN running value is always replaced; a NaN candidate (with a
    /// non-NaN running value) never is (`NaN > _` / `NaN < _` are both false).
    fn should_replace(self, running: f64, candidate: f64) -> bool {
        if running.is_nan() {
            return true;
        }
        match self {
            Self::Min => running > candidate,
            Self::Max => running < candidate,
        }
    }
}

/// NaN-ignoring `min_over_time`/`max_over_time` fold over a non-empty sample
/// window: seed with the first sample (NaN included), then replace under
/// [`ExtremumKind::should_replace`]. The result is NaN only when *every* sample
/// is NaN — matching Prometheus, the `*_over_time` UDF, and the `min`/`max`
/// aggregate. (The previous `total_cmp` reduction wrongly propagated a single
/// NaN sample into the extremum.)
fn fold_over_time_extremum(samples: &[(i64, f64)], extremum: ExtremumKind) -> f64 {
    let mut running = samples[0].1;
    for (_, candidate) in &samples[1..] {
        if extremum.should_replace(running, *candidate) {
            running = *candidate;
        }
    }
    running
}

/// Population variance of a sample window via Welford's online algorithm with
/// Kahan-compensated accumulation, matching Prometheus' `stdvar_over_time` /
/// `stddev_over_time`. The naive `E[x^2] - E[x]^2` form suffers catastrophic
/// cancellation for large-magnitude close-valued windows (yielding a negative
/// variance whose `sqrt` is NaN); Welford stays numerically stable.
fn over_time_variance(samples: &[(i64, f64)]) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut mean_comp) = (0.0_f64, 0.0_f64);
    let (mut aux, mut aux_comp) = (0.0_f64, 0.0_f64);
    for (_, value) in samples {
        count += 1.0;
        let delta = value - (mean + mean_comp);
        let (new_mean, new_mean_comp) = kahan_sum_inc(delta / count, mean, mean_comp);
        mean = new_mean;
        mean_comp = new_mean_comp;
        let (new_aux, new_aux_comp) =
            kahan_sum_inc(delta * (value - (mean + mean_comp)), aux, aux_comp);
        aux = new_aux;
        aux_comp = new_aux_comp;
    }
    (aux + aux_comp) / count
}

/// Arithmetic mean of a non-empty float window via Prometheus' incremental
/// Kahan-compensated mean (`avg_over_time` in `promql/engine.go`). The naive
/// `sum / count` overflows to ±Inf for very-large-magnitude windows; the
/// incremental form keeps the running mean finite and, once it does saturate to
/// ±Inf, preserves Prometheus' same-sign-infinity handling.
fn over_time_mean(values: impl Iterator<Item = f64>) -> f64 {
    let mut count = 0.0_f64;
    let (mut mean, mut comp) = (0.0_f64, 0.0_f64);
    for value in values {
        count += 1.0;
        if mean.is_infinite() {
            if value.is_infinite() && (value > 0.0) == (mean > 0.0) {
                // Same-sign infinity: the mean stays that infinity.
                continue;
            }
            if !value.is_infinite() && !value.is_nan() {
                // A finite sample cannot pull an already-infinite mean back.
                continue;
            }
        }
        let (new_mean, new_comp) = kahan_sum_inc(value / count - mean / count, mean, comp);
        mean = new_mean;
        comp = new_comp;
    }
    mean + comp
}

/// One Kahan-compensated incremental sum step: add `increment` to the running
/// sum `(sum, comp)`, returning the updated `(sum, comp)`. A direct port of
/// Prometheus' `kahanSumInc` (`promql/engine.go`), used by the numerically
/// stable mean/variance folds so the operator and interpreter agree bit-for-bit.
fn kahan_sum_inc(increment: f64, sum: f64, comp: f64) -> (f64, f64) {
    let new_sum = sum + increment;
    // Recover the rounding error lost when `increment` is small relative to
    // `sum` (or vice versa), matching Prometheus' branch on magnitude.
    let new_comp = if sum.abs() >= increment.abs() {
        comp + ((sum - new_sum) + increment)
    } else {
        comp + ((increment - new_sum) + sum)
    };
    (new_sum, new_comp)
}

fn over_time_mad(samples: &[(i64, f64)]) -> Option<f64> {
    let mut values = samples.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    let median = quantile_value(0.5, &mut values)?;
    let mut deviations = samples
        .iter()
        .map(|(_, value)| (value - median).abs())
        .collect::<Vec<_>>();
    quantile_value(0.5, &mut deviations)
}

fn predict_linear_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    duration_seconds: f64,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range_ms);
    predict_linear(&samples, range_end_ms, duration_seconds)
}

#[cfg(feature = "experimental-functions")]
fn double_exponential_smoothing_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range_ms: i64,
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    let samples = float_range_samples(series, range_end_ms, range_ms);
    double_exponential_smoothing(&samples, smoothing_factor, trend_factor)
}

// Prometheus computes extrapolation in f64 seconds; timestamp/range deltas
// intentionally enter that float domain here.
#[allow(clippy::cast_precision_loss)]
fn extrapolated_rate(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range_ms: i64,
    kind: RangeFn,
) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }

    let is_counter = matches!(kind, RangeFn::Rate | RangeFn::Increase);

    let mut result = values[n - 1] - values[0];
    if is_counter {
        for window in values.windows(2) {
            if window[1] < window[0] {
                result += window[0];
            }
        }
    }

    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts) as f64 / 1000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_duration_between_samples = sampled_interval / (n - 1) as f64;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms) as f64 / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts) as f64 / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    if is_counter && result > 0.0 && values[0] >= 0.0 {
        let duration_to_zero = sampled_interval * (values[0] / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolate_to_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolate_to_interval / sampled_interval;
    if kind == RangeFn::Rate {
        let range_seconds = range_ms as f64 / 1000.0;
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}

fn count_changes(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let changes = values
        .windows(2)
        .filter(|window| window[0].to_bits() != window[1].to_bits())
        .fold(0.0, |count, _| count + 1.0);
    Some(changes)
}

fn count_resets(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    if values.len() < 2 {
        return Some(0.0);
    }

    let resets = values
        .windows(2)
        .filter(|window| window[1] < window[0])
        .fold(0.0, |count, _| count + 1.0);
    Some(resets)
}

fn align_subquery_start(start_ms: i64, step_ms: i64) -> i64 {
    let remainder = start_ms.rem_euclid(step_ms);
    if remainder == 0 {
        start_ms
    } else {
        start_ms.saturating_add(step_ms - remainder)
    }
}

#[cfg(feature = "experimental-functions")]
#[allow(
    clippy::cast_precision_loss,
    reason = "Prometheus limit_ratio normalizes a 64-bit label hash into f64 threshold space"
)]
fn limit_ratio_includes_sample(ratio: f64, labels: &Labels) -> bool {
    let sample_offset = labels.fingerprint() as f64 / u64::MAX as f64;
    (ratio >= 0.0 && sample_offset < ratio) || (ratio < 0.0 && sample_offset >= 1.0 + ratio)
}

// Prometheus predicts gauges from a simple linear regression in f64 seconds.
#[allow(clippy::cast_precision_loss)]
fn predict_linear(samples: &[(i64, f64)], range_end_ms: i64, duration_seconds: f64) -> Option<f64> {
    let (slope, intercept) = regression_slope_and_intercept(samples, range_end_ms)?;
    Some(intercept + (slope * duration_seconds))
}

#[cfg(feature = "experimental-functions")]
fn validate_smoothing_factor(name: &str, value: f64) -> Result<()> {
    if value <= 0.0 || value >= 1.0 {
        return Err(PromqlError::Plan(format!(
            "invalid {name}. Expected: 0 < factor < 1, got: {value}"
        )));
    }
    Ok(())
}

#[cfg(feature = "experimental-functions")]
fn double_exponential_smoothing(
    samples: &[(i64, f64)],
    smoothing_factor: f64,
    trend_factor: f64,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }

    let mut previous_smoothed = 0.0;
    let mut smoothed = samples[0].1;
    let mut trend = samples[1].1 - samples[0].1;

    for (index, (_, value)) in samples.iter().enumerate().skip(1) {
        if index != 1 {
            trend =
                trend_factor.mul_add(smoothed - previous_smoothed, (1.0 - trend_factor) * trend);
        }
        let scaled_value = smoothing_factor * value;
        let smoothed_with_trend = (1.0 - smoothing_factor) * (smoothed + trend);
        previous_smoothed = smoothed;
        smoothed = scaled_value + smoothed_with_trend;
    }

    Some(smoothed)
}

#[allow(clippy::cast_precision_loss)]
fn regression_slope(samples: &[(i64, f64)], range_end_ms: i64) -> Option<f64> {
    regression_slope_and_intercept(samples, range_end_ms).map(|(slope, _)| slope)
}

#[allow(clippy::cast_precision_loss)]
fn regression_slope_and_intercept(samples: &[(i64, f64)], range_end_ms: i64) -> Option<(f64, f64)> {
    if samples.len() < 2 {
        return None;
    }

    let mut sum_x = 0.0;
    let mut sum_y = 0.0;
    let mut count = 0.0;
    for (timestamp, value) in samples {
        sum_x += (*timestamp - range_end_ms) as f64 / 1000.0;
        sum_y += value;
        count += 1.0;
    }
    let mean_x = sum_x / count;
    let mean_y = sum_y / count;

    let mut covariance = 0.0;
    let mut variance = 0.0;
    for (timestamp, value) in samples {
        let x = (*timestamp - range_end_ms) as f64 / 1000.0;
        let x_delta = x - mean_x;
        covariance += x_delta * (value - mean_y);
        variance += x_delta * x_delta;
    }
    if variance == 0.0 {
        return None;
    }

    let slope = covariance / variance;
    let intercept = mean_y - (slope * mean_x);
    Some((slope, intercept))
}

// Prometheus computes instant rate deltas in f64 seconds; timestamp deltas
// intentionally enter that float domain here.
#[allow(clippy::cast_precision_loss)]
fn instant_delta(timestamps: &[i64], values: &[f64], kind: IrateFn) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let previous = values[n - 2];
    let last = values[n - 1];
    let mut result = last - previous;
    if matches!(kind, IrateFn::Irate) && result < 0.0 {
        result = last;
    }

    if matches!(kind, IrateFn::Irate) {
        let interval = (timestamps[n - 1] - timestamps[n - 2]) as f64 / 1000.0;
        if interval <= 0.0 {
            return None;
        }
        result /= interval;
    }
    Some(result)
}

#[derive(Clone, Copy)]
enum ScalarSide {
    Left,
    Right,
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
enum DurationHelper {
    Range,
    Step,
    Start,
    End,
}

#[cfg(feature = "experimental-functions")]
impl DurationHelper {
    fn value_ms(self) -> i64 {
        QUERY_RANGE_CONTEXT
            .try_with(|context| match self {
                Self::Range => context.end.saturating_sub(context.start),
                Self::Step => context.step,
                Self::Start => context.start,
                Self::End => context.end,
            })
            .unwrap_or(0)
    }
}

#[derive(Clone, Copy)]
enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
}

impl BinaryOp {
    fn try_from_token(token: TokenType) -> Result<Self> {
        match token.id() {
            T_ADD => Ok(Self::Add),
            T_SUB => Ok(Self::Sub),
            T_MUL => Ok(Self::Mul),
            T_DIV => Ok(Self::Div),
            T_MOD => Ok(Self::Mod),
            T_POW => Ok(Self::Pow),
            T_ATAN2 => Ok(Self::Atan2),
            T_EQLC => Ok(Self::Eq),
            T_NEQ => Ok(Self::Neq),
            T_GTR => Ok(Self::Gt),
            T_LSS => Ok(Self::Lt),
            T_GTE => Ok(Self::Gte),
            T_LTE => Ok(Self::Lte),
            T_LAND | T_LOR | T_LUNLESS => Err(PromqlError::Unsupported(format!(
                "set operator `{token}` is not implemented yet"
            ))),
            _ => Err(PromqlError::Unsupported(format!(
                "binary operator `{token}` is not implemented yet"
            ))),
        }
    }

    fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Gte | Self::Lte
        )
    }

    /// `PromQL` surface symbol for this operator, matching Prometheus
    /// annotation text (e.g. `==`, `!=`, `>`, `>=`).
    fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
            Self::Pow => "^",
            Self::Atan2 => "atan2",
            Self::Eq => "==",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::Lt => "<",
            Self::Gte => ">=",
            Self::Lte => "<=",
        }
    }

    fn apply_scalar(self, left: f64, right: f64, modifier: Option<&BinModifier>) -> Option<f64> {
        if self.is_comparison() {
            let pass = self.compare(left, right);
            if binary_returns_bool(modifier) {
                Some(if pass { 1.0 } else { 0.0 })
            } else if pass {
                Some(left)
            } else {
                None
            }
        } else {
            Some(self.arithmetic(left, right))
        }
    }

    fn apply_vector_scalar(
        self,
        sample: InstantSample,
        scalar: f64,
        modifier: Option<&BinModifier>,
        scalar_side: ScalarSide,
    ) -> Option<InstantSample> {
        if let SampleValue::Histogram(histogram) = sample.value {
            return self.apply_histogram_scalar(
                &sample.labels,
                sample.ts_ms,
                &histogram,
                scalar,
                scalar_side,
            );
        }

        let sample_value = float_sample_value(&sample).ok()?;
        let (left, right) = match scalar_side {
            ScalarSide::Left => (scalar, sample_value),
            ScalarSide::Right => (sample_value, scalar),
        };
        let value = if self.is_comparison() && !binary_returns_bool(modifier) {
            self.compare(left, right).then_some(sample_value)?
        } else {
            self.apply_scalar(left, right, modifier)?
        };
        let labels = if self.is_comparison() && !binary_returns_bool(modifier) {
            sample.labels
        } else {
            labels_without_metric_name(&sample.labels)
        };
        Some(InstantSample {
            labels,
            ts_ms: sample.ts_ms,
            value: SampleValue::Float(value),
        })
    }

    fn apply_histogram_scalar(
        self,
        labels: &Labels,
        ts_ms: i64,
        histogram: &NativeHistogram,
        scalar: f64,
        scalar_side: ScalarSide,
    ) -> Option<InstantSample> {
        let factor = match (self, scalar_side) {
            (Self::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
            (Self::Div, ScalarSide::Right) => 1.0 / scalar,
            _ => {
                if self.is_comparison() {
                    // Prometheus ignores the histogram operand in a comparison
                    // against a float, dropping the sample and raising an info.
                    let (lhs, rhs) = match scalar_side {
                        ScalarSide::Left => ("float", "histogram"),
                        ScalarSide::Right => ("histogram", "float"),
                    };
                    emit_info(incompatible_types_in_binop_info(lhs, self.symbol(), rhs));
                }
                return None;
            }
        };
        Some(InstantSample {
            labels: labels_without_metric_name(labels),
            ts_ms,
            value: SampleValue::Histogram(scaled_native_histogram(histogram, factor)),
        })
    }

    fn arithmetic(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Add => left + right,
            Self::Sub => left - right,
            Self::Mul => left * right,
            Self::Div => left / right,
            Self::Mod => left % right,
            Self::Pow => left.powf(right),
            Self::Atan2 => left.atan2(right),
            Self::Eq | Self::Neq | Self::Gt | Self::Lt | Self::Gte | Self::Lte => {
                unreachable!("comparison op used as arithmetic")
            }
        }
    }

    fn compare(self, left: f64, right: f64) -> bool {
        match self {
            Self::Eq => left
                .partial_cmp(&right)
                .is_some_and(std::cmp::Ordering::is_eq),
            Self::Neq => !left
                .partial_cmp(&right)
                .is_some_and(std::cmp::Ordering::is_eq),
            Self::Gt => left > right,
            Self::Lt => left < right,
            Self::Gte => left >= right,
            Self::Lte => left <= right,
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow | Self::Atan2 => {
                unreachable!("arithmetic op used as comparison")
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SetOp {
    And,
    Or,
    Unless,
}

impl SetOp {
    fn from_token(token: TokenType) -> Option<Self> {
        match token.id() {
            T_LAND => Some(Self::And),
            T_LOR => Some(Self::Or),
            T_LUNLESS => Some(Self::Unless),
            _ => None,
        }
    }
}

fn validate_binary_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if matches!(modifier.card, VectorMatchCardinality::ManyToMany) {
        return Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        ));
    }
    Ok(())
}

fn validate_set_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
        return Err(PromqlError::Unsupported(
            "binary fill modifiers are not implemented yet".to_string(),
        ));
    }
    Ok(())
}

fn validate_extended_selector_modifier(
    function_name: &str,
    modifier: ExtendedSelectorModifier,
) -> Result<()> {
    let allowed = match modifier {
        ExtendedSelectorModifier::Anchored => matches!(
            function_name,
            "changes" | "delta" | "increase" | "rate" | "resets"
        ),
        ExtendedSelectorModifier::Smoothed => {
            matches!(function_name, "delta" | "increase" | "rate")
        }
    };
    if allowed {
        return Ok(());
    }

    let allowed_functions = match modifier {
        ExtendedSelectorModifier::Anchored => "changes, delta, increase, rate, resets",
        ExtendedSelectorModifier::Smoothed => "delta, increase, rate",
    };
    Err(PromqlError::Plan(format!(
        "{} modifier can only be used with: {allowed_functions} - not with {function_name}",
        modifier.keyword()
    )))
}

/// Combine two **already-evaluated** instant operands under `binary`'s operator
/// and modifier, producing the binary result.
///
/// This is the shared core of `PromQL` binary evaluation: the interpreter
/// ([`PromqlEngine::eval_instant_binary`]) evaluates both operands through the
/// interpreter and calls this; the operator path ([`PromqlEngine::plan_binary_expr`])
/// recurses both operands through the planner, assembles each to an
/// [`InstantValue`], and calls this same function. Because both callers funnel
/// their operands through one combine routine, the two paths are byte-for-byte
/// identical once their operand vectors match — set ops, vector matching,
/// `__name__` dropping, the `bool` modifier, and `group_left`/`group_right`
/// copying are all decided here, not at the call site.
fn combine_instant_binary(
    binary: &BinaryExpr,
    lhs: InstantValue,
    rhs: InstantValue,
    time_ms: i64,
) -> Result<QueryResult> {
    let modifier = binary.modifier.as_ref();

    if let Some(op) = SetOp::from_token(binary.op) {
        validate_set_modifier(modifier)?;
        let (InstantValue::Vector(left), InstantValue::Vector(right)) = (lhs, rhs) else {
            return Err(PromqlError::Plan(format!(
                "set operator `{}` requires instant-vector operands",
                binary.op
            )));
        };
        return Ok(QueryResult::InstantVector(eval_vector_set_binary(
            left, right, op, modifier,
        )));
    }

    validate_binary_modifier(modifier)?;
    let op = BinaryOp::try_from_token(binary.op)?;
    match (lhs, rhs) {
        (InstantValue::Scalar(left), InstantValue::Scalar(right)) => {
            let Some(value) = op.apply_scalar(left, right, modifier) else {
                return Err(PromqlError::Plan(
                    "scalar comparison without bool cannot filter a scalar".to_string(),
                ));
            };
            Ok(QueryResult::Scalar {
                ts_ms: time_ms,
                value,
            })
        }
        (InstantValue::Vector(vector), InstantValue::Scalar(scalar)) => {
            let samples = vector
                .into_iter()
                .filter_map(|sample| {
                    op.apply_vector_scalar(sample, scalar, modifier, ScalarSide::Right)
                })
                .collect();
            Ok(QueryResult::InstantVector(samples))
        }
        (InstantValue::Scalar(scalar), InstantValue::Vector(vector)) => {
            let samples = vector
                .into_iter()
                .filter_map(|sample| {
                    op.apply_vector_scalar(sample, scalar, modifier, ScalarSide::Left)
                })
                .collect();
            Ok(QueryResult::InstantVector(samples))
        }
        (InstantValue::Vector(left), InstantValue::Vector(right)) => {
            eval_vector_vector_binary(left, right, op, modifier).map(QueryResult::InstantVector)
        }
    }
}

fn eval_vector_set_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: SetOp,
    modifier: Option<&BinModifier>,
) -> Vec<InstantSample> {
    let mut left_keys = BTreeSet::new();
    let mut right_keys = BTreeSet::new();
    for sample in &left {
        left_keys.insert(binary_match_key(&sample.labels, modifier));
    }
    for sample in &right {
        right_keys.insert(binary_match_key(&sample.labels, modifier));
    }

    let mut out = Vec::new();
    match op {
        SetOp::And => {
            for sample in left {
                if right_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
        SetOp::Unless => {
            for sample in left {
                if !right_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
        SetOp::Or => {
            out.extend(left);
            for sample in right {
                if !left_keys.contains(&binary_match_key(&sample.labels, modifier)) {
                    out.push(sample);
                }
            }
        }
    }
    out
}

fn eval_vector_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Vec<InstantSample>> {
    let card = modifier.map_or(VectorMatchCardinality::OneToOne, |modifier| {
        modifier.card.clone()
    });
    match card {
        VectorMatchCardinality::OneToOne => {
            eval_one_to_one_vector_binary(left, right, op, modifier)
        }
        VectorMatchCardinality::ManyToOne(group_labels) => {
            eval_many_to_one_vector_binary(left, right, op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::OneToMany(group_labels) => {
            eval_one_to_many_vector_binary(left, right, op, modifier, &group_labels.labels)
        }
        VectorMatchCardinality::ManyToMany => Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        )),
    }
}

fn eval_one_to_one_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Vec<InstantSample>> {
    let mut right_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in right {
        let key = binary_match_key(&sample.labels, modifier);
        if right_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching for key `{key}` is not supported"
            )));
        }
    }

    let mut out = Vec::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.remove(&key) else {
            let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&left_sample, rhs_fill, op, modifier, MissingSide::Right)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                left_sample.labels
            } else {
                one_to_one_binary_result_labels(&left_sample.labels, modifier)
            };
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(&left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            left_sample.labels
        } else {
            one_to_one_binary_result_labels(&left_sample.labels, modifier)
        };
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
        });
    }
    if let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) {
        for right_sample in right_by_key.into_values() {
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                right_sample.labels
            } else {
                one_to_one_binary_result_labels(&right_sample.labels, modifier)
            };
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
            });
        }
    }
    Ok(out)
}

fn eval_many_to_one_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let mut right_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in right {
        let key = binary_match_key(&sample.labels, modifier);
        if right_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "many-to-one matching requires the right side to be unique for key `{key}`"
            )));
        }
    }

    let mut out = Vec::new();
    for left_sample in left {
        let key = binary_match_key(&left_sample.labels, modifier);
        let Some(right_sample) = right_by_key.get(&key) else {
            let Some(rhs_fill) = modifier.and_then(|modifier| modifier.fill_values.rhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&left_sample, rhs_fill, op, modifier, MissingSide::Right)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                left_sample.labels
            } else {
                labels_without_metric_name(&left_sample.labels)
            };
            out.push(InstantSample {
                labels,
                ts_ms: left_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(&left_sample, right_sample, op, modifier)?
        else {
            continue;
        };
        let mut labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            left_sample.labels.clone()
        } else {
            labels_without_metric_name(&left_sample.labels)
        };
        copy_group_labels(&mut labels, &right_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: left_sample.ts_ms,
            value,
        });
    }
    Ok(out)
}

fn eval_one_to_many_vector_binary(
    left: Vec<InstantSample>,
    right: Vec<InstantSample>,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    group_labels: &[String],
) -> Result<Vec<InstantSample>> {
    let mut left_by_key: BTreeMap<String, InstantSample> = BTreeMap::new();
    for sample in left {
        let key = binary_match_key(&sample.labels, modifier);
        if left_by_key.insert(key.clone(), sample).is_some() {
            return Err(PromqlError::Exec(format!(
                "one-to-many matching requires the left side to be unique for key `{key}`"
            )));
        }
    }

    let mut out = Vec::new();
    for right_sample in right {
        let key = binary_match_key(&right_sample.labels, modifier);
        let Some(left_sample) = left_by_key.get(&key) else {
            let Some(lhs_fill) = modifier.and_then(|modifier| modifier.fill_values.lhs) else {
                continue;
            };
            let Some(value) =
                apply_binary_fill_value(&right_sample, lhs_fill, op, modifier, MissingSide::Left)?
            else {
                continue;
            };
            let labels = if op.is_comparison() && !binary_returns_bool(modifier) {
                right_sample.labels
            } else {
                labels_without_metric_name(&right_sample.labels)
            };
            out.push(InstantSample {
                labels,
                ts_ms: right_sample.ts_ms,
                value,
            });
            continue;
        };
        let Some(value) = apply_binary_sample_value(left_sample, &right_sample, op, modifier)?
        else {
            continue;
        };
        let mut labels = if op.is_comparison() && !binary_returns_bool(modifier) {
            right_sample.labels.clone()
        } else {
            labels_without_metric_name(&right_sample.labels)
        };
        copy_group_labels(&mut labels, &left_sample.labels, group_labels);
        out.push(InstantSample {
            labels,
            ts_ms: right_sample.ts_ms,
            value,
        });
    }
    Ok(out)
}

fn copy_group_labels(labels: &mut Labels, one_side: &Labels, group_labels: &[String]) {
    for name in group_labels {
        if is_result_metadata_label(name) {
            continue;
        }
        if let Some(value) = one_side.get(name) {
            labels.insert(name, value);
        }
    }
}

fn one_to_one_binary_result_labels(input: &Labels, modifier: Option<&BinModifier>) -> Labels {
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        Some(LabelModifier::Include(include)) => {
            let mut labels = Labels::new();
            for name in &include.labels {
                if is_result_metadata_label(name) {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
            labels
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            let mut labels = Labels::new();
            for (name, value) in input.iter() {
                if is_result_metadata_label(name) || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
            labels
        }
        None => labels_without_metric_name(input),
    }
}

fn binary_returns_bool(modifier: Option<&BinModifier>) -> bool {
    modifier.is_some_and(|modifier| modifier.return_bool)
}

#[derive(Clone, Copy)]
enum MissingSide {
    Left,
    Right,
}

fn apply_binary_fill_value(
    present: &InstantSample,
    fill_value: f64,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
    missing_side: MissingSide,
) -> Result<Option<SampleValue>> {
    let filled = InstantSample {
        labels: Labels::new(),
        ts_ms: present.ts_ms,
        value: SampleValue::Float(fill_value),
    };
    match missing_side {
        MissingSide::Left => apply_binary_sample_value(&filled, present, op, modifier),
        MissingSide::Right => apply_binary_sample_value(present, &filled, op, modifier),
    }
}

fn apply_binary_sample_value(
    left: &InstantSample,
    right: &InstantSample,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    match (&left.value, &right.value) {
        (SampleValue::Float(left), SampleValue::Float(right)) => Ok(op
            .apply_scalar(*left, *right, modifier)
            .map(SampleValue::Float)),
        (SampleValue::Histogram(left), SampleValue::Histogram(right)) => {
            apply_histogram_histogram_binary(left, right, op, modifier)
        }
        (SampleValue::Float(left), SampleValue::Histogram(right)) => {
            if op.is_comparison() {
                emit_info(incompatible_types_in_binop_info(
                    "float",
                    op.symbol(),
                    "histogram",
                ));
                return Ok(None);
            }
            Ok(apply_histogram_float_binary(
                right,
                *left,
                op,
                ScalarSide::Left,
            ))
        }
        (SampleValue::Histogram(left), SampleValue::Float(right)) => {
            if op.is_comparison() {
                emit_info(incompatible_types_in_binop_info(
                    "histogram",
                    op.symbol(),
                    "float",
                ));
                return Ok(None);
            }
            Ok(apply_histogram_float_binary(
                left,
                *right,
                op,
                ScalarSide::Right,
            ))
        }
    }
}

fn apply_histogram_float_binary(
    histogram: &NativeHistogram,
    scalar: f64,
    op: BinaryOp,
    scalar_side: ScalarSide,
) -> Option<SampleValue> {
    let factor = match (op, scalar_side) {
        (BinaryOp::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
        (BinaryOp::Div, ScalarSide::Right) => 1.0 / scalar,
        _ => return None,
    };
    Some(SampleValue::Histogram(scaled_native_histogram(
        histogram, factor,
    )))
}

fn apply_histogram_histogram_binary(
    left: &NativeHistogram,
    right: &NativeHistogram,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    let mut out = left.clone();
    match op {
        BinaryOp::Add => add_compatible_native_histogram(&mut out, right)?,
        BinaryOp::Sub => {
            let mut right = right.clone();
            scale_native_histogram_values(&mut right, -1.0);
            add_compatible_native_histogram(&mut out, &right)?;
            out.reset_hint = ResetHint::Gauge;
        }
        BinaryOp::Eq | BinaryOp::Neq => {
            let pass = match op {
                BinaryOp::Eq => left == right,
                BinaryOp::Neq => left != right,
                _ => unreachable!("non-comparison histogram op"),
            };
            return Ok(if binary_returns_bool(modifier) {
                Some(SampleValue::Float(if pass { 1.0 } else { 0.0 }))
            } else if pass {
                Some(SampleValue::Histogram(left.clone()))
            } else {
                None
            });
        }
        BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Gte | BinaryOp::Lte => {
            // Ordered comparisons are undefined between two histograms:
            // Prometheus drops the pair and raises an info annotation.
            emit_info(incompatible_types_in_binop_info(
                "histogram",
                op.symbol(),
                "histogram",
            ));
            return Ok(None);
        }
        _ => return Ok(None),
    }
    Ok(Some(SampleValue::Histogram(out)))
}

fn float_sample_value(sample: &InstantSample) -> Result<f64> {
    match sample.value {
        SampleValue::Float(value) => Ok(value),
        SampleValue::Histogram(_) => Err(PromqlError::Unsupported(
            "binary operations over histograms are not implemented yet".to_string(),
        )),
    }
}

fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let mut labels = Labels::new();
    match modifier {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if name == "__name__" {
                    continue;
                }
                if let Some(value) = input.get(name) {
                    labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in input.iter() {
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                labels.insert(name, value);
            }
        }
        None => {}
    }
    labels
}

fn info_identifying_key(labels: &Labels) -> Option<String> {
    Some(format!(
        "job={}\ninstance={}\n",
        labels.get("job")?,
        labels.get("instance")?
    ))
}

/// The parsed, store-independent context of an `info(v [, data_label_selector])`
/// call: the optional data-label selector (borrowed from the call) plus the
/// derived data-label matcher set and the flags that drive the [`apply_info`]
/// join. Built once by [`parse_info_call`] and shared between the interpreter's
/// [`PromqlEngine::eval_info_call`] and the operator-path `info` dispatch so the
/// two cannot diverge.
struct InfoContext<'a> {
    /// The optional second-argument data-label selector (e.g. `{data=~".+"}`).
    data_label_selector: Option<&'a VectorSelector>,
    /// All matchers from the data-label selector (or empty for the argless form).
    data_label_matchers: Vec<LabelMatcher>,
    /// Whether the required (non-identifying) data-label matchers match the empty
    /// label set; controls whether an unmatched input series is dropped.
    required_data_label_matchers_match_empty: bool,
    /// The set of explicitly selected data-label names (non-identifying matchers).
    selected_data_labels: BTreeSet<String>,
    /// Whether to restrict the joined labels to `selected_data_labels`.
    restrict_data_labels: bool,
}

/// Parse and validate an `info(v [, data_label_selector])` call into its
/// store-independent [`InfoContext`], raising the canonical interpreter errors for
/// wrong arity or a non-vector-selector second argument. Pure (no store access).
fn parse_info_call(call: &Call) -> Result<InfoContext<'_>> {
    let [_arg, data_label_selector @ ..] = call.args.args.as_slice() else {
        return Err(PromqlError::Plan(format!(
            "info expects one or two arguments for default target_info enrichment, got {}",
            call.args.args.len()
        )));
    };
    if data_label_selector.len() > 1 {
        return Err(PromqlError::Plan(format!(
            "info expects one or two arguments for default target_info enrichment, got {}",
            call.args.args.len()
        )));
    }
    let data_label_selector = match data_label_selector {
        [] => None,
        [selector] => match selector.as_ref() {
            Expr::VectorSelector(selector) => Some(selector),
            _ => {
                return Err(PromqlError::Plan(
                    "info data label selector must be a vector selector".to_string(),
                ));
            }
        },
        [_, _, ..] => unreachable!("data label selector arity checked above"),
    };
    let data_label_matchers = data_label_selector
        .map(info_data_label_matchers)
        .transpose()?
        .unwrap_or_default();
    let required_data_label_matchers = data_label_matchers
        .iter()
        .filter(|matcher| !matches!(matcher.name.as_str(), "__name__" | "job" | "instance"))
        .cloned()
        .collect::<Vec<_>>();
    let required_data_label_matchers_match_empty =
        labels_match(&Labels::new(), &required_data_label_matchers)?;
    let selected_data_labels = data_label_matchers
        .iter()
        .filter(|matcher| !matches!(matcher.name.as_str(), "__name__" | "job" | "instance"))
        .map(|matcher| matcher.name.clone())
        .collect::<BTreeSet<_>>();
    let restrict_data_labels = data_label_selector.is_some() && !selected_data_labels.is_empty();
    Ok(InfoContext {
        data_label_selector,
        data_label_matchers,
        required_data_label_matchers_match_empty,
        selected_data_labels,
        restrict_data_labels,
    })
}

/// Join each input series with its overlapping `target_info` series, attaching the
/// info series' identifying (non-`__name__`/`job`/`instance`) data labels.
///
/// This is the pure join half of `info(v [, data_label_selector])`, shared between
/// the interpreter ([`PromqlEngine::eval_info_call`]) and the operator path so the
/// two match by construction: a `target_info`-named input passes through; an input
/// with no `(job, instance)` identifying key passes through; an input with no
/// overlapping info series is dropped iff a required data-label matcher does not
/// match the empty set; otherwise the info series' data labels are attached (never
/// overwriting an existing input label, optionally restricted to the explicitly
/// selected labels).
fn apply_info(
    samples: Vec<InstantSample>,
    info_by_key: &BTreeMap<String, InstantSample>,
    context: &InfoContext<'_>,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter_map(|mut sample| {
            if sample.labels.get("__name__") == Some("target_info") {
                return Some(sample);
            }
            let key = info_identifying_key(&sample.labels)?;
            let Some(info) = info_by_key.get(&key) else {
                return if context.data_label_selector.is_some()
                    && !context.required_data_label_matchers_match_empty
                {
                    None
                } else {
                    Some(sample)
                };
            };
            for (name, value) in info.labels.iter() {
                if matches!(name.as_str(), "__name__" | "job" | "instance") {
                    continue;
                }
                if context.restrict_data_labels && !context.selected_data_labels.contains(name) {
                    continue;
                }
                if sample.labels.get(name).is_none() {
                    sample.labels.insert(name, value);
                }
            }
            Some(sample)
        })
        .collect()
}

fn info_samples_by_identifying_key(
    info_samples: Vec<InstantSample>,
    data_label_matchers: &[LabelMatcher],
) -> Result<BTreeMap<String, InstantSample>> {
    // Precompile the regex matchers ONCE before the per-sample loop. The shared
    // `labels_match` recompiles every `=~`/`!~` matcher on each call, so calling
    // it per info sample re-paid the regex compile for every sample.
    let compiled = compile_label_matchers(data_label_matchers)?;
    let mut info_by_key = BTreeMap::<String, InstantSample>::new();
    for sample in info_samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            return Err(PromqlError::Plan(
                "info series selector must match float samples".to_string(),
            ));
        }
        if !compiled.matches(&sample.labels) {
            continue;
        }
        let Some(key) = info_identifying_key(&sample.labels) else {
            continue;
        };
        info_by_key
            .entry(key)
            .and_modify(|existing| {
                if sample.ts_ms > existing.ts_ms {
                    *existing = sample.clone();
                } else if sample.ts_ms == existing.ts_ms {
                    for (name, value) in sample.labels.iter() {
                        if existing.labels.get(name).is_none() {
                            existing.labels.insert(name, value);
                        }
                    }
                }
            })
            .or_insert(sample);
    }
    Ok(info_by_key)
}

fn binary_match_key(labels: &Labels, modifier: Option<&BinModifier>) -> String {
    let mut key_labels = Labels::new();
    match modifier.and_then(|modifier| modifier.matching.as_ref()) {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if let Some(value) = labels.get(name) {
                    key_labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for (name, value) in labels.iter() {
                if name == "__name__" || excluded.contains(name) {
                    continue;
                }
                key_labels.insert(name, value);
            }
        }
        None => {
            for (name, value) in labels.iter() {
                if is_result_metadata_label(name) {
                    continue;
                }
                key_labels.insert(name, value);
            }
        }
    }
    labels_key(&key_labels)
}

fn labels_without_metric_name(input: &Labels) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        if !is_result_metadata_label(name) {
            labels.insert(name, value);
        }
    }
    labels
}

fn is_result_metadata_label(name: &str) -> bool {
    matches!(name, "__name__" | "__type__" | "__unit__")
}

fn validate_unique_instant_labelsets(result: &QueryResult) -> Result<()> {
    let QueryResult::InstantVector(samples) = result else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for sample in samples {
        let key = labels_key(&sample.labels);
        if !seen.insert(key.clone()) {
            return Err(PromqlError::Exec(format!(
                "vector cannot contain metrics with the same labelset: {key}"
            )));
        }
    }
    Ok(())
}

fn labels_without_metric_and_label(input: &Labels, drop_label: &str) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in input.iter() {
        if !is_result_metadata_label(name) && name != drop_label {
            labels.insert(name, value);
        }
    }
    labels
}

fn absent_labels(expr: &Expr) -> Result<Labels> {
    match expr {
        Expr::VectorSelector(selector) => Ok(absent_labels_from_selector(selector)),
        Expr::MatrixSelector(selector) => Ok(absent_labels_from_selector(&selector.vs)),
        Expr::Paren(paren) => absent_labels(&paren.expr),
        _ => Ok(Labels::new()),
    }
}

fn absent_labels_from_selector(selector: &VectorSelector) -> Labels {
    let matcher_sets = label_matcher_sets(selector);
    if matcher_sets.len() == 1 {
        return absent_labels_from_matchers(&matcher_sets[0]);
    }
    Labels::new()
}

fn absent_labels_from_matchers(matchers: &[LabelMatcher]) -> Labels {
    let mut labels = Labels::new();
    for matcher in matchers {
        if matcher.name != "__name__" && matcher.op == MatchOp::Eq {
            labels.insert(&matcher.name, &matcher.value);
        }
    }
    labels
}

fn labels_key(labels: &Labels) -> String {
    let mut key = String::new();
    for (name, value) in labels.iter() {
        key.push_str(name);
        key.push('=');
        key.push_str(value);
        key.push('\n');
    }
    key
}

async fn collect_float_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<FloatRow>> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            if rows.len() >= max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={max_samples}"
                )));
            }
            rows.push(FloatRow {
                fp: fps.value(row),
                ts_ms: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(rows)
}

async fn collect_histogram_rows(
    scan: ScanResult,
    table: &str,
    max_samples: usize,
) -> Result<Vec<HistogramRow>> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await?;
    let batches = dataframe.collect().await?;

    let mut rows = Vec::new();
    for batch in batches {
        let decoded = decode_native_histograms(&batch)
            .map_err(|error| PromqlError::Store(error.to_string()))?;
        for (fp, ts_ms, hist) in decoded {
            if rows.len() >= max_samples {
                return Err(PromqlError::Exec(format!(
                    "query exceeds max_samples={max_samples}"
                )));
            }
            rows.push(HistogramRow { fp, ts_ms, hist });
        }
    }
    Ok(rows)
}

/// Structural predicate: true when every node of `expr` dispatches to a shape
/// the operator planner ([`PromqlEngine::plan_instant_expr`]) handles, so a
/// range query over `expr` can be routed through the per-step planner.
///
/// This mirrors `plan_instant_expr`'s **dispatch** (which node kinds and
/// function names route to the operator path), recursing into vector-typed
/// inner expressions the same way. It is purely structural — it never touches
/// the store — because planner support is structural: which constructs the
/// operator path understands does not change with the evaluation timestamp.
///
/// It deliberately does **not** model the data-dependent fallbacks
/// (histogram-bearing or empty-valued-label series, wrong call arity, a
/// non-scalar bound argument, an invalid `label_replace` regex). Those still
/// surface at evaluation time as a
/// `plan_instant_expr` returning `None` (or an `Err`), and the per-step range
/// driver treats *any* such per-step `None` as a whole-query fallback to the
/// interpreter — so this predicate only needs to gate out the node kinds that
/// cannot be nested as an operand or stitched across a step grid (string
/// literals, raw matrix selectors, and subqueries — whose results are not a
/// numeric scalar / instant vector).
///
/// Scalar-typed sub-expressions (a bound arg, a scalar binary operand) are
/// always treated as plannable: the planner evaluates them through the
/// interpreter's pure scalar path, which carries no staleness/NaN subtlety.
///
/// The arms returning a bare `true` (`VectorSelector`, `NumberLiteral`,
/// `Extension`) are kept separate for their per-variant documentation rather
/// than merged.
#[allow(clippy::match_same_arms)]
fn instant_expr_is_plannable(expr: &Expr) -> bool {
    match expr {
        Expr::Paren(paren) => instant_expr_is_plannable(&paren.expr),
        // A bare instant-vector selector. A histogram-bearing series falls back
        // per-step; an empty-valued-label series now rides the operator leaf
        // (NULL = absent, `""` = present-empty).
        Expr::VectorSelector(_) => true,
        Expr::Call(call) => {
            // Rate-family or `*_over_time` range call (incl. the experimental
            // `mad`/`first`/`ts_of_*_over_time` members) over a bare matrix
            // selector. The matchers already require a plain `MatrixSelector`
            // argument; histogram inputs fall back per-step. A bad
            // `quantile_over_time` phi no longer falls back — it evaluates to
            // signed `±Inf` / `NaN` plus an `InvalidQuantileWarning`.
            if match_rate_range_call(expr).is_some()
                || match_over_time_range_call(expr).is_some()
                || match_experimental_over_time_range_call(expr).is_some()
            {
                return true;
            }
            // A RESIDUAL range-vector fold the fast matchers don't claim:
            // `changes`/`resets`/`deriv`/`predict_linear`/
            // `double_exponential_smoothing` over a plain matrix, or ANY rate /
            // `*_over_time` fold over an `anchored`/`smoothed` extended selector.
            // These route through `plan_extended_range_fold_call` (delegating to
            // the shared interpreter dispatch), so they are plannable — including
            // nested under an aggregate / binary and range-stitched per step.
            if is_extended_range_fold_call(call) {
                return true;
            }
            // The EXPERIMENTAL scalar-returning helpers handled by
            // `plan_experimental_call`: the duration helpers `range`/`step`/
            // `start`/`end` (which read the scoped range context, also scoped by the
            // per-step planner range driver) and `max_of`/`min_of` (scalar∘scalar
            // extrema). These fold to a `PrecomputedScalar`, so they nest and
            // range-stitch like any scalar expression.
            #[cfg(feature = "experimental-functions")]
            if matches!(
                call.func.name,
                "range" | "step" | "start" | "end" | "max_of" | "min_of"
            ) {
                return true;
            }
            // A per-row scalar-math call: the inner vector argument (the first
            // positional arg) must itself be plannable. The trailing bound
            // args are scalars resolved through the interpreter.
            if scalar_math_op_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A label-rewrite / ordering call: the inner vector argument (the
            // first positional arg) must be plannable; the rest are string
            // literals validated per-step.
            if label_ops_kind_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A `histogram_quantile(phi, v)` (classic OR native): the inner bucket
            // vector (the second positional arg) must be plannable. `phi` (the
            // first arg) is a scalar resolved through the interpreter.
            if call.func.name == "histogram_quantile" {
                return call
                    .args
                    .args
                    .get(1)
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // The experimental `histogram_quantiles(label, v, phi...)`: the inner
            // bucket vector (the FIRST positional arg) must be plannable. The label
            // name and the trailing scalar `phi`s are resolved per-step.
            #[cfg(feature = "experimental-functions")]
            if call.func.name == "histogram_quantiles" {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A native accessor (`histogram_count`/`sum`/`avg`/`stddev`/`stdvar`):
            // the single instant-vector operand must be plannable.
            if histogram_accessor_from_function_name(call.func.name).is_some() {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // `histogram_fraction(lower, upper, v)`: the inner vector (the third
            // positional arg) must be plannable. The two scalar bounds are
            // resolved through the interpreter.
            if call.func.name == "histogram_fraction" {
                return call
                    .args
                    .args
                    .get(2)
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // `info(v [, data_label_selector])`: the input vector `v` (the first
            // positional arg) must be plannable. The data-label selector is a
            // vector-selector literal validated at eval time (a non-vector-selector
            // arg / wrong arity surfaces as an `Err` from `plan_info_call`, which
            // the per-step driver treats as a whole-query fallback).
            if call.func.name == "info" {
                return call
                    .args
                    .args
                    .first()
                    .is_some_and(|arg| instant_expr_is_plannable(arg));
            }
            // A range/`*_over_time` call whose argument is a subquery: the
            // subquery's inner instant expression must itself be plannable. The
            // outer scalar params (quantile/predict_linear/double_exp) are
            // resolved through the interpreter; a non-positive step / invalid
            // param falls back inside `plan_subquery_range_call`. This is what lets
            // nested subqueries and subquery calls inside an aggregate/binary route
            // through the planner.
            if let Some((subquery, _)) = match_subquery_range_call(call) {
                return instant_expr_is_plannable(&subquery.expr);
            }
            // The float UTILITY functions handled by `plan_util_call`.
            util_call_is_plannable(call)
        }
        // A simple (no-param) float aggregation, or a parameterized aggregation
        // (topk/bottomk/quantile/count_values/stddev/stdvar), over a plannable
        // inner vector. `limitk`/`limit_ratio` are not plannable.
        Expr::Aggregate(aggregate) => {
            let simple = simple_aggregate_op(aggregate.op).is_some() && aggregate.param.is_none();
            (simple || param_aggregate_op_is_plannable(aggregate))
                && instant_expr_is_plannable(&aggregate.expr)
        }
        // A binary op: each operand must itself be plannable; scalar operands are
        // always fine (folded via the interpreter's pure scalar path). A
        // scalar∘scalar fold is carried through `PrecomputedScalar`.
        Expr::Binary(binary) => {
            binary_operand_is_plannable(&binary.lhs) && binary_operand_is_plannable(&binary.rhs)
        }
        // A unary `-`/`+` over a plannable operand. A scalar operand folds to a
        // scalar; a vector operand to a vector. Both nest and range-stitch.
        Expr::Unary(unary) => instant_expr_is_plannable(&unary.expr),
        // A bare numeric literal is a scalar carried through `PrecomputedScalar`.
        Expr::NumberLiteral(_) => true,
        // An `anchored`/`smoothed` extended selector is handled by
        // `plan_extension_expr` (the `smoothed` kernel, or the `anchored`-on-
        // instant hard error). Structurally plannable so nested forms
        // (`sum(smoothed(m))`) route too; a non-selector / unknown extension falls
        // back inside the planner.
        Expr::Extension(_) => true,
        // A string literal (no numeric/vector result to nest or range-stitch) and
        // a raw matrix selector / subquery (range-vector result, only meaningful
        // at the top level of an instant query) are handled directly in the
        // top-level `plan_instant_expr` dispatch, not through this nesting /
        // range-stitching predicate.
        Expr::StringLiteral(_) | Expr::MatrixSelector(_) | Expr::Subquery(_) => false,
    }
}

/// Gate for routing a **range** query through the per-step operator planner.
///
/// A range query routes through the per-step operator driver iff its top-level
/// shape is per-step planner-supported ([`instant_expr_is_plannable`]). This
/// includes a **bare** instant-vector selector and a top-level **scalar-typed**
/// expression:
///
/// - **Bare instant-vector selector.** The selector chain uses Prometheus'
///   left-**open**, right-closed lookback window `(eval - lookbackDelta, eval]`
///   (`promql/engine.go::vectorSelectorSingle` rejects `t <= eval - lookback`),
///   so a sample landing exactly on the lookback boundary is excluded.
///
/// - **Scalar-typed expression** (`time()`, `1 + 2`, the argless calendar
///   forms). The driver folds a no-label scalar series per step
///   (empty-labelset / `SampleValue::Float`).
///
/// Aggregations over a rate-family or `*_over_time` range call
/// (`sum(rate(m[5m]))`, `avg by(l)(increase(...))`, …) route through the planner
/// too: the rate/`*_over_time` UDF emits **NULL** (not a NaN sentinel) for a
/// no-value window, the aggregate planner drops those NULL rows before grouping,
/// and the built-in / NaN-ignoring aggregates skip NULL — so a no-value series
/// is excluded from the group (and an all-no-value group yields no result row).
/// A genuine NaN value is non-null and propagates through the aggregate.
///
/// A top-level raw **matrix selector** / **subquery** is *not* plannable here
/// (it yields a range vector, owned by the dedicated matrix/subquery range
/// paths), so [`instant_expr_is_plannable`] already excludes it.
///
/// A **top-level bare selector** carrying an `@ start()`/`@ end()` modifier also
/// routes through the planner: the per-step planner range driver scopes the
/// query's `[start, end]` bounds in `AT_MODIFIER_BOUNDS`, and the selector planner
/// ([`PromqlEngine::plan_instant_selector`]) resolves `@ start()`/`@ end()` to
/// those bounds per Prometheus (a fixed eval instant repeated across every grid
/// step).
fn range_expr_routes_through_planner(expr: &Expr) -> bool {
    instant_expr_is_plannable(expr)
}

/// True when one binary operand can be carried through the operator path: a
/// scalar operand is folded via the interpreter's pure scalar path (always
/// fine), and a vector operand must itself be structurally plannable. A
/// matrix/string operand is never plannable.
fn binary_operand_is_plannable(operand: &Expr) -> bool {
    match operand.value_type() {
        ValueType::Scalar => true,
        ValueType::Vector => instant_expr_is_plannable(operand),
        ValueType::Matrix | ValueType::String => false,
    }
}

/// Structural gate for the float UTILITY functions handled by
/// [`PromqlEngine::plan_util_call`]: `time`/`pi` (argless), `scalar`/`vector`,
/// `timestamp`, the calendar family (argless or one vector arg), and
/// `absent`/`absent_over_time`. The inner instant-vector argument (where one
/// exists) must itself be structurally plannable; data-dependent shapes
/// (histogram series, etc.) fall back per-step inside the planner. A `vector`
/// argument must be scalar-typed. Any other function (or a non-matching arity)
/// returns `false` so the dispatch falls through to the interpreter.
fn util_call_is_plannable(call: &Call) -> bool {
    match call.func.name {
        // Argless scalar utilities.
        "time" | "pi" => call.args.args.is_empty(),
        // The lone inner instant-vector argument must be plannable.
        "scalar" | "timestamp" | "absent" => call
            .args
            .args
            .first()
            .is_some_and(|arg| call.args.args.len() == 1 && instant_expr_is_plannable(arg)),
        // `vector(s)` takes a scalar argument resolved through the interpreter.
        "vector" => {
            call.args.args.len() == 1 && call.args.args[0].value_type() == ValueType::Scalar
        }
        // `absent_over_time(v[range])`: a plain float-only matrix selector rides
        // the fast `eval_range_arg` path; a histogram-bearing matrix, a subquery
        // range, or an anchored/smoothed selector delegates to the interpreter's
        // `eval_absent_over_time_call` (parity-exact). All range-vector shapes are
        // plannable; the per-shape / wrong-arity error is raised inside
        // `plan_absent_over_time_call`.
        "absent_over_time" => {
            let [arg] = call.args.args.as_slice() else {
                return false;
            };
            let mut inner = arg.as_ref();
            while let Expr::Paren(paren) = inner {
                inner = paren.expr.as_ref();
            }
            matches!(
                inner,
                Expr::MatrixSelector(_) | Expr::Subquery(_) | Expr::Extension(_)
            )
        }
        // The calendar family: argless (operates on `time()`) or one plannable
        // inner vector argument.
        other if calendar_fn_from_function_name(other).is_some() => match call.args.args.as_slice()
        {
            [] => true,
            [arg] => instant_expr_is_plannable(arg),
            _ => false,
        },
        _ => false,
    }
}

/// Recognize a top-level `f(selector[range])` rate-family call eligible for the
/// operator path, returning the inner [`MatrixSelector`] and the UDF kind.
///
/// Eligible iff `expr` is a [`Call`] whose function is one of
/// `rate|increase|delta|irate|idelta`, called with exactly one argument that —
/// after unwrapping parentheses — is a plain [`Expr::MatrixSelector`]. An
/// `anchored`/`smoothed` selector parses to [`Expr::Extension`], not a plain
/// `MatrixSelector`, so it is rejected here and stays on the interpreter, as do
/// nested forms (`sum(rate(...))`), `_over_time`, subqueries, and every other
/// function.
fn match_rate_range_call(expr: &Expr) -> Option<(&MatrixSelector, RateUdfKind)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let kind = RateUdfKind::from_function_name(call.func.name)?;
    let [arg] = call.args.args.as_slice() else {
        return None;
    };
    let mut arg = arg.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, kind))
}

/// Match a top-level `*_over_time` range call eligible for the operator path.
///
/// Eligible iff `expr` is a [`Call`] whose function is one of the
/// non-experimental members (`sum|avg|count|min|max|stddev|stdvar|
/// last|present_over_time`, or `quantile_over_time`), whose range argument —
/// after unwrapping parentheses — is a plain [`Expr::MatrixSelector`]. For
/// `quantile_over_time` the leading `phi` argument is returned for separate
/// scalar resolution; for every other family it is `None`.
///
/// The experimental members (`mad_over_time`, `first_over_time`, the
/// `ts_of_*_over_time` family) are matched separately by
/// [`match_experimental_over_time_range_call`] (they route through the shared
/// kernel, not this float UDF-chain path). `absent_over_time`, subquery range
/// arguments, `anchored`/`smoothed` selectors (which parse to [`Expr::Extension`]),
/// and nested forms stay on the interpreter (return `None`).
fn match_over_time_range_call(
    expr: &Expr,
) -> Option<(&MatrixSelector, OverTimeFamily, Option<&Expr>)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let family = over_time_family_from_function_name(call.func.name)?;
    let (range_arg, phi_arg) = if matches!(family, OverTimeFamily::Quantile) {
        let [phi, range] = call.args.args.as_slice() else {
            return None;
        };
        (range.as_ref(), Some(phi.as_ref()))
    } else {
        let [range] = call.args.args.as_slice() else {
            return None;
        };
        (range.as_ref(), None)
    };
    let mut arg = range_arg;
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, family, phi_arg))
}

/// The range-vector argument position of a single-range-vector-argument
/// range/`*_over_time` fold, by function name. This is the residual set of
/// range-fold functions whose operator routing is NOT a fast UDF chain: either
/// the function has no operator-leaf lowering (`changes`/`resets`/`deriv`/
/// `predict_linear`/`double_exponential_smoothing`), or its argument is an
/// `anchored`/`smoothed` extended selector (which `match_rate_range_call` /
/// `match_over_time_range_call` reject because they require a plain
/// `MatrixSelector`). Returns the index of the range-vector argument; the
/// parameter args (if any) are resolved by the delegated interpreter method.
fn range_fold_range_arg_index(call: &Call) -> Option<usize> {
    match call.func.name {
        // One argument: the range vector.
        "rate" | "increase" | "delta" | "irate" | "idelta" | "changes" | "resets" | "deriv"
        | "sum_over_time" | "avg_over_time" | "count_over_time" | "min_over_time"
        | "max_over_time" | "stddev_over_time" | "stdvar_over_time" | "last_over_time"
        | "present_over_time" => (call.args.args.len() == 1).then_some(0),
        #[cfg(feature = "experimental-functions")]
        "mad_over_time"
        | "first_over_time"
        | "ts_of_first_over_time"
        | "ts_of_last_over_time"
        | "ts_of_min_over_time"
        | "ts_of_max_over_time" => (call.args.args.len() == 1).then_some(0),
        // `quantile_over_time(phi, range)`: the range vector is the SECOND arg.
        "quantile_over_time" => (call.args.args.len() == 2).then_some(1),
        // `predict_linear(range, t)`: the range vector is the FIRST arg.
        "predict_linear" => (call.args.args.len() == 2).then_some(0),
        // `double_exponential_smoothing(range, sf, tf)`: range is the FIRST arg.
        #[cfg(feature = "experimental-functions")]
        "double_exponential_smoothing" => (call.args.args.len() == 3).then_some(0),
        _ => None,
    }
}

/// Recognize a residual range-vector fold call (see [`range_fold_range_arg_index`])
/// whose range-vector argument — after unwrapping parentheses — is a plain
/// [`Expr::MatrixSelector`] or an `anchored`/`smoothed` [`Expr::Extension`] over a
/// selector. Subquery range arguments are already claimed by
/// [`match_subquery_range_call`], and the fast plain-matrix `rate`/`*_over_time`
/// paths are already claimed by [`match_rate_range_call`] /
/// [`match_over_time_range_call`] earlier in the dispatch; this matcher is what
/// makes the planner TOTAL over the remaining shapes (`changes`/`resets`/`deriv`
/// over a plain matrix, and ANY of these folds over an anchored/smoothed
/// selector) by routing them into the SHARED interpreter `eval_*_call` (parity-
/// exact). Returns `true` when the call should route through
/// [`PromqlEngine::plan_extended_range_fold_call`].
fn is_extended_range_fold_call(call: &Call) -> bool {
    let Some(index) = range_fold_range_arg_index(call) else {
        return false;
    };
    let Some(range_arg) = call.args.args.get(index) else {
        return false;
    };
    let mut arg = range_arg.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    match arg {
        Expr::MatrixSelector(_) => true,
        // An `anchored`/`smoothed` extended selector wraps a `MatrixSelector`
        // child (`anchored(m[5m])`), so the interpreter's `eval_range_arg` can
        // build its windowed range vector.
        Expr::Extension(extension) => extension
            .expr
            .as_any()
            .downcast_ref::<ExtendedSelectorExpr>()
            .is_some_and(|extended| matches!(extended.child(), Some(Expr::MatrixSelector(_)))),
        _ => false,
    }
}

/// Match a top-level EXPERIMENTAL `*_over_time` member range call eligible for the
/// shared-kernel operator path.
///
/// Eligible iff `expr` is a [`Call`] whose function is one of `mad_over_time`,
/// `first_over_time`, or the `ts_of_{first,last,min,max}_over_time` family, called
/// with exactly one argument that — after unwrapping parentheses — is a plain
/// [`Expr::MatrixSelector`], returning the selector and the matching
/// [`OverTimeFn`]. These members have no operator-leaf UDF, so they route through
/// the shared [`apply_outer_range_fn`] kernel rather than the float UDF chain.
///
/// `absent_over_time`, subquery range arguments, `anchored`/`smoothed` selectors
/// (which parse to [`Expr::Extension`]), and nested forms stay on the interpreter
/// (return `None`). The non-experimental members are matched by
/// [`match_over_time_range_call`] instead.
fn match_experimental_over_time_range_call(expr: &Expr) -> Option<(&MatrixSelector, OverTimeFn)> {
    let Expr::Call(call) = expr else {
        return None;
    };
    let kind = match call.func.name {
        "mad_over_time" => OverTimeFn::Mad,
        "first_over_time" => OverTimeFn::First,
        "ts_of_first_over_time" => OverTimeFn::TsOfFirst,
        "ts_of_last_over_time" => OverTimeFn::TsOfLast,
        "ts_of_min_over_time" => OverTimeFn::TsOfMin,
        "ts_of_max_over_time" => OverTimeFn::TsOfMax,
        _ => return None,
    };
    let [range] = call.args.args.as_slice() else {
        return None;
    };
    let mut arg = range.as_ref();
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::MatrixSelector(selector) = arg else {
        return None;
    };
    Some((selector, kind))
}

/// Map a [`RateUdfKind`] (the rate-family matcher's output) to the shared
/// [`OuterRangeFn`] the interpreter's `eval_*_call` applies for the same name.
/// `rate`/`increase`/`delta` are extrapolated range folds; `irate`/`idelta` are
/// instant-delta folds. This is the seam that lets a histogram-bearing rate-family
/// call route through the shared [`apply_outer_range_fn`] kernel instead of the
/// float-only UDF chain.
fn rate_udf_kind_to_outer_range_fn(kind: RateUdfKind) -> OuterRangeFn {
    match kind {
        RateUdfKind::Rate => OuterRangeFn::Range(RangeFn::Rate),
        RateUdfKind::Increase => OuterRangeFn::Range(RangeFn::Increase),
        RateUdfKind::Delta => OuterRangeFn::Range(RangeFn::Delta),
        RateUdfKind::Irate => OuterRangeFn::InstantDelta(IrateFn::Irate),
        RateUdfKind::Idelta => OuterRangeFn::InstantDelta(IrateFn::Idelta),
    }
}

/// Map an [`OverTimeFamily`] (the `*_over_time` matcher's output) to the shared
/// [`OuterRangeFn`] the interpreter's `eval_over_time_call` applies for the same
/// name. `quantile_over_time` carries its resolved `phi`; every other member maps
/// to the matching [`OverTimeFn`]. The matcher only yields the non-experimental
/// members, so the experimental [`OverTimeFn`] variants (`Mad`/`First`/`TsOf*`)
/// are unreachable here.
fn over_time_family_to_outer_range_fn(family: OverTimeFamily, phi: f64) -> OuterRangeFn {
    match family {
        OverTimeFamily::Sum => OuterRangeFn::OverTime(OverTimeFn::Sum),
        OverTimeFamily::Avg => OuterRangeFn::OverTime(OverTimeFn::Avg),
        OverTimeFamily::Count => OuterRangeFn::OverTime(OverTimeFn::Count),
        OverTimeFamily::Min => OuterRangeFn::OverTime(OverTimeFn::Min),
        OverTimeFamily::Max => OuterRangeFn::OverTime(OverTimeFn::Max),
        OverTimeFamily::Stddev => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        OverTimeFamily::Stdvar => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        OverTimeFamily::Last => OuterRangeFn::OverTime(OverTimeFn::Last),
        OverTimeFamily::Present => OuterRangeFn::OverTime(OverTimeFn::Present),
        OverTimeFamily::Quantile => OuterRangeFn::QuantileOverTime(phi),
    }
}

/// The outer range/`*_over_time` function of a `f(inner[range:res] ...)` subquery
/// call, with any scalar parameters still **unresolved** (the parameter argument
/// `Expr`s are resolved through the interpreter inside the async planner method,
/// matching what the corresponding `eval_*_call` does).
enum SubqueryOuterFn<'a> {
    /// A function whose only argument is the range vector; the [`OuterRangeFn`] is
    /// fully determined by the name.
    NoParam(OuterRangeFn),
    /// `quantile_over_time(phi, inner[...])`: resolve `phi` (the leading arg).
    QuantileOverTime { phi: &'a Expr },
    /// `predict_linear(inner[...], t)`: resolve the trailing duration arg.
    PredictLinear { duration: &'a Expr },
    /// `double_exponential_smoothing(inner[...], sf, tf)`: resolve both factors.
    #[cfg(feature = "experimental-functions")]
    DoubleExponentialSmoothing {
        smoothing: &'a Expr,
        trend: &'a Expr,
    },
}

/// Map a range/`*_over_time` function name to its [`OuterRangeFn`] when the
/// function takes exactly one argument (the range vector). Parameterized
/// functions (`quantile_over_time`/`predict_linear`/`double_exponential_smoothing`)
/// and the non-fold helpers (`absent_over_time`/`time`/…) return `None` here and
/// are matched separately.
fn no_param_outer_range_fn(name: &str) -> Option<OuterRangeFn> {
    Some(match name {
        "rate" => OuterRangeFn::Range(RangeFn::Rate),
        "increase" => OuterRangeFn::Range(RangeFn::Increase),
        "delta" => OuterRangeFn::Range(RangeFn::Delta),
        "changes" => OuterRangeFn::Range(RangeFn::Changes),
        "resets" => OuterRangeFn::Range(RangeFn::Resets),
        "irate" => OuterRangeFn::InstantDelta(IrateFn::Irate),
        "idelta" => OuterRangeFn::InstantDelta(IrateFn::Idelta),
        "deriv" => OuterRangeFn::Deriv,
        "sum_over_time" => OuterRangeFn::OverTime(OverTimeFn::Sum),
        "avg_over_time" => OuterRangeFn::OverTime(OverTimeFn::Avg),
        "count_over_time" => OuterRangeFn::OverTime(OverTimeFn::Count),
        "min_over_time" => OuterRangeFn::OverTime(OverTimeFn::Min),
        "max_over_time" => OuterRangeFn::OverTime(OverTimeFn::Max),
        "stddev_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stddev),
        "stdvar_over_time" => OuterRangeFn::OverTime(OverTimeFn::Stdvar),
        "mad_over_time" => OuterRangeFn::OverTime(OverTimeFn::Mad),
        "first_over_time" => OuterRangeFn::OverTime(OverTimeFn::First),
        "last_over_time" => OuterRangeFn::OverTime(OverTimeFn::Last),
        "ts_of_first_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfFirst),
        "ts_of_last_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfLast),
        "ts_of_min_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMin),
        "ts_of_max_over_time" => OuterRangeFn::OverTime(OverTimeFn::TsOfMax),
        "present_over_time" => OuterRangeFn::OverTime(OverTimeFn::Present),
        _ => return None,
    })
}

/// Recognize a `f(inner[range:resolution] ...)` call whose range argument is a
/// **subquery** and whose outer `f` is a planner-supported range/`*_over_time`
/// fold, returning the [`SubqueryExpr`] and the (param-unresolved) outer-fn spec.
///
/// Eligible iff `expr` is a [`Call`] whose function `f` is one of the supported
/// folds and whose range argument — after unwrapping parentheses — is an
/// [`Expr::Subquery`]. `absent_over_time` (synthesizes absent labels) and every
/// non-fold function return `None` and stay on the interpreter, as does a
/// matrix-selector range argument (matched by [`match_rate_range_call`] /
/// [`match_over_time_range_call`] instead).
fn match_subquery_range_call(call: &Call) -> Option<(&SubqueryExpr, SubqueryOuterFn<'_>)> {
    // Resolve the range-vector argument's position and the parameter args by the
    // function's arity, exactly as the corresponding `eval_*_call` does.
    let (range_arg, spec) = match call.func.name {
        "quantile_over_time" => {
            let [phi, range] = call.args.args.as_slice() else {
                return None;
            };
            (range.as_ref(), SubqueryOuterFn::QuantileOverTime { phi })
        }
        "predict_linear" => {
            let [range, duration] = call.args.args.as_slice() else {
                return None;
            };
            (range.as_ref(), SubqueryOuterFn::PredictLinear { duration })
        }
        #[cfg(feature = "experimental-functions")]
        "double_exponential_smoothing" => {
            let [range, smoothing, trend] = call.args.args.as_slice() else {
                return None;
            };
            (
                range.as_ref(),
                SubqueryOuterFn::DoubleExponentialSmoothing { smoothing, trend },
            )
        }
        name => {
            let outer = no_param_outer_range_fn(name)?;
            let [range] = call.args.args.as_slice() else {
                return None;
            };
            (range.as_ref(), SubqueryOuterFn::NoParam(outer))
        }
    };
    let mut arg = range_arg;
    while let Expr::Paren(paren) = arg {
        arg = paren.expr.as_ref();
    }
    let Expr::Subquery(subquery) = arg else {
        return None;
    };
    Some((subquery, spec))
}

pub(crate) fn label_matcher_sets(selector: &VectorSelector) -> Vec<Vec<LabelMatcher>> {
    if selector.matchers.or_matchers.is_empty() {
        return vec![build_label_matchers(
            selector.name.as_deref(),
            &selector.matchers.matchers,
        )];
    }

    let mut out = Vec::new();
    for matchers in &selector.matchers.or_matchers {
        out.push(build_label_matchers(selector.name.as_deref(), matchers));
    }
    out
}

/// Reconstruct a [`Labels`] set from the string label columns of one row of a
/// planner-path output batch. Only `Utf8` columns are treated as labels; the
/// `timestamp`/`value` columns are skipped.
fn labels_from_batch(batch: &arrow::record_batch::RecordBatch, row: usize) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == leaf::TIME_COLUMN
            || field.name() == leaf::VALUE_COLUMN
            || field.name() == leaf::SAMPLE_TIME_COLUMN
        {
            continue;
        }
        if let Some(column) = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            // NULL -> the label is ABSENT (skip); a non-null value (including
            // `""`) -> the label is PRESENT with that value. This preserves the
            // present-empty-vs-absent distinction the leaf encodes, so the
            // reconstructed fingerprint matches the original series identity.
            if !column.is_null(row) {
                labels.insert(field.name().clone(), column.value(row).to_string());
            }
        }
    }
    labels
}

/// Reconstruct a [`Labels`] set from the string label columns of one row of a
/// rate-range projection output batch. The rate projection carries only label
/// (`Utf8`) columns plus the float `value` result column, so every non-`value`
/// `Utf8` column is a label.
fn labels_from_rate_batch(batch: &arrow::record_batch::RecordBatch, row: usize) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == rate_range::RATE_VALUE_COLUMN {
            continue;
        }
        if let Some(column) = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
        {
            // NULL -> absent (skip); any non-null value (including `""`) ->
            // present with that value. See `labels_from_batch`.
            if !column.is_null(row) {
                labels.insert(field.name().clone(), column.value(row).to_string());
            }
        }
    }
    labels
}

/// Assemble instant-vector-selector output batches into a result. Output rows
/// carry label columns plus `timestamp`/`value`/`sample_timestamp`; the selected
/// sample's true timestamp is in `sample_timestamp`. Result labels are recovered
/// from `labels_by_fp` keyed by the row's reconstructed fingerprint.
fn assemble_selector_batches(
    batches: &[arrow::record_batch::RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
) -> Result<QueryResult> {
    // InstantManipulate emits at most one row per series (the single grid step).
    let mut by_fp: BTreeMap<SeriesFingerprint, (i64, f64)> = BTreeMap::new();
    for batch in batches {
        let sample_timestamps = batch
            .column_by_name(leaf::SAMPLE_TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Int64 sample-timestamp column".to_string())
            })?;
        let values = batch
            .column_by_name(leaf::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            let fp = labels_from_batch(batch, row).fingerprint();
            let ts_ms = sample_timestamps.value(row);
            let value = values.value(row);
            by_fp
                .entry(fp)
                .and_modify(|latest| {
                    if ts_ms > latest.0 {
                        *latest = (ts_ms, value);
                    }
                })
                .or_insert((ts_ms, value));
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, (ts_ms, value))| {
            labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                labels,
                ts_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}

/// Assemble rate-family projection output batches into a result. Output rows
/// carry label columns plus a single `value` column; the eval timestamp is
/// reattached and the metric name dropped, and NULL rows (the UDF's "no value"
/// marker for a window with too few samples) are dropped — exactly as the
/// interpreter omits no-value series. A non-null NaN row is a genuine NaN value
/// and is KEPT and propagated.
fn assemble_rate_batches(
    batches: &[arrow::record_batch::RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
) -> Result<QueryResult> {
    let mut by_fp: BTreeMap<SeriesFingerprint, f64> = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(rate_range::RATE_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("rate projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL is the no-value marker (the series has no value at this
            // step): drop it. A non-null NaN is a genuine NaN value: keep it.
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            let fp = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fp, value);
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, value)| {
            labels_by_fp.get(&fp).map(|labels| InstantSample {
                // Rate-family results drop the metric name, matching
                // `eval_range_function_call`'s `labels_without_metric_name`.
                labels: labels_without_metric_name(labels),
                ts_ms: time_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}

/// Assemble `*_over_time` projection output batches into a result. Output rows
/// carry label columns plus a single `value` column; the eval timestamp is
/// reattached and NULL rows (the UDF's "no value" marker for an empty window)
/// are dropped — exactly as the interpreter omits no-value series. A non-null NaN
/// row is a genuine NaN value and is KEPT and propagated. `preserve_metric_name`
/// keeps `__name__` only for `last_over_time`; every other family drops it,
/// matching the interpreter's `eval_over_time_call`
/// (`OverTimeFn::preserves_metric_name`).
fn assemble_over_time_batches(
    batches: &[arrow::record_batch::RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
    preserve_metric_name: bool,
) -> Result<QueryResult> {
    let mut by_fp: BTreeMap<SeriesFingerprint, f64> = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(over_time_range::OVER_TIME_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("over_time projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL is the no-value marker (an empty window): drop it. A non-null
            // NaN is a genuine NaN value: keep it.
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            // The over_time projection carries only label (`Utf8`) columns plus
            // the float `value` result, so `labels_from_rate_batch` (which reads
            // exactly that shape) reconstructs the fingerprint.
            let fp = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fp, value);
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, value)| {
            labels_by_fp.get(&fp).map(|labels| {
                let labels = if preserve_metric_name {
                    labels.clone()
                } else {
                    labels_without_metric_name(labels)
                };
                InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(value),
                }
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}

/// Assemble simple-aggregation output batches into a result. Output rows carry
/// exactly the grouping label columns plus `value`; the labelset is read
/// directly from the batch (no fingerprint lookup) and the eval timestamp is
/// reattached. An empty grouping (`by ()` / no modifier) yields a single row
/// with an empty labelset.
///
/// A NULL aggregate result means the group had no value-bearing input (every
/// member was a no-value series, all dropped by the pre-aggregate NULL filter, or
/// the NaN-ignoring `min`/`max` UDAF saw only nulls): drop it, matching the
/// interpreter, which forms no group when no sample reaches it. A non-null NaN
/// result is a genuine aggregated NaN (e.g. `sum` over a group holding a genuine
/// NaN, or an all-NaN `min`/`max` group) and is KEPT.
fn assemble_aggregate_batches(
    batches: &[arrow::record_batch::RecordBatch],
    time_ms: i64,
) -> Result<QueryResult> {
    let mut samples = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(AGGREGATE_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("aggregate projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL aggregate = no value-bearing input for the group: drop it
            // (the interpreter never forms such a group). A non-null NaN is a
            // genuine aggregated NaN: keep it.
            if values.is_null(row) {
                continue;
            }
            // The grouping labels are exactly the batch's non-`value` Utf8
            // columns; `labels_from_rate_batch` reads precisely those.
            let labels = labels_from_rate_batch(batch, row);
            samples.push(InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(values.value(row)),
            });
        }
    }
    Ok(QueryResult::InstantVector(samples))
}

/// Assemble per-row scalar-math projection output batches into a result. Output
/// rows carry the metadata-free label columns plus a single `value` column; the
/// metric name is already dropped at the leaf, the labelset is read directly
/// from the batch, and the eval timestamp is reattached. **Every** row is kept:
/// the scalar-math functions never drop a float sample, so `f(NaN)` / `sqrt(-1)`
/// surface as `NaN` (matching the interpreter's `eval_unary_float_call` /
/// `eval_clamp_call` / `eval_round_call`).
fn assemble_scalar_math_batches(
    batches: &[arrow::record_batch::RecordBatch],
    _time_ms: i64,
) -> Result<QueryResult> {
    let mut samples = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(scalar_math::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("scalar-math projection missing Float64 value column".to_string())
            })?;
        let sample_timestamps = batch
            .column_by_name(scalar_math::SAMPLE_TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<arrow::array::Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec(
                    "scalar-math projection missing Int64 sample-timestamp column".to_string(),
                )
            })?;
        for row in 0..batch.num_rows() {
            // The scalar-math projection carries label (`Utf8`) columns plus the
            // float `value` result and the Int64 `sample_timestamp`;
            // `labels_from_rate_batch` reads only the string label columns (it
            // skips the Int64 timestamp), reconstructing the labelset.
            let labels = labels_from_rate_batch(batch, row);
            samples.push(InstantSample {
                labels,
                // Scalar-math functions report the inner sample's timestamp
                // unchanged (the interpreter keeps `sample.ts_ms`).
                ts_ms: sample_timestamps.value(row),
                value: SampleValue::Float(values.value(row)),
            });
        }
    }
    Ok(QueryResult::InstantVector(samples))
}

/// Map a `PromQL` function name to its per-row scalar-math op, or `None` for any
/// function outside the scalar-math set (which stays on the interpreter). `pi`
/// is a 0-arg literal, not a per-row op, so it is intentionally excluded.
fn scalar_math_op_from_function_name(name: &str) -> Option<ScalarMathOp> {
    Some(match name {
        "abs" => ScalarMathOp::Abs,
        "ceil" => ScalarMathOp::Ceil,
        "floor" => ScalarMathOp::Floor,
        "sqrt" => ScalarMathOp::Sqrt,
        "exp" => ScalarMathOp::Exp,
        "ln" => ScalarMathOp::Ln,
        "log2" => ScalarMathOp::Log2,
        "log10" => ScalarMathOp::Log10,
        "sgn" => ScalarMathOp::Sgn,
        "sin" => ScalarMathOp::Sin,
        "cos" => ScalarMathOp::Cos,
        "tan" => ScalarMathOp::Tan,
        "asin" => ScalarMathOp::Asin,
        "acos" => ScalarMathOp::Acos,
        "atan" => ScalarMathOp::Atan,
        "sinh" => ScalarMathOp::Sinh,
        "cosh" => ScalarMathOp::Cosh,
        "tanh" => ScalarMathOp::Tanh,
        "asinh" => ScalarMathOp::Asinh,
        "acosh" => ScalarMathOp::Acosh,
        "atanh" => ScalarMathOp::Atanh,
        "deg" => ScalarMathOp::Deg,
        "rad" => ScalarMathOp::Rad,
        "round" => ScalarMathOp::Round,
        "clamp_min" => ScalarMathOp::ClampMin,
        "clamp_max" => ScalarMathOp::ClampMax,
        "clamp" => ScalarMathOp::Clamp,
        _ => return None,
    })
}

/// The label-rewrite / ordering functions handled by the operator-path
/// [`PromqlEngine::plan_label_ops_call`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LabelOpsKind {
    LabelReplace,
    LabelJoin,
    Sort(SortOrder),
    SortByLabel(SortOrder),
}

/// Map a `PromQL` function name to its label-rewrite / ordering kind, or `None`
/// for any function outside this set.
fn label_ops_kind_from_function_name(name: &str) -> Option<LabelOpsKind> {
    Some(match name {
        "label_replace" => LabelOpsKind::LabelReplace,
        "label_join" => LabelOpsKind::LabelJoin,
        "sort" => LabelOpsKind::Sort(SortOrder::Ascending),
        "sort_desc" => LabelOpsKind::Sort(SortOrder::Descending),
        "sort_by_label" => LabelOpsKind::SortByLabel(SortOrder::Ascending),
        "sort_by_label_desc" => LabelOpsKind::SortByLabel(SortOrder::Descending),
        _ => return None,
    })
}

/// The value of a string-literal call argument at `index`, or `None` when the
/// argument is absent or not a string literal. Unlike [`string_literal_arg`],
/// this never errors: the label-ops planner uses it to *probe* the call shape
/// and falls back to the interpreter (which raises the canonical error) on any
/// mismatch.
fn string_literal_value(call: &Call, index: usize) -> Option<String> {
    match call.args.args.get(index).map(Box::as_ref) {
        Some(Expr::StringLiteral(value)) => Some(value.val.clone()),
        _ => None,
    }
}

/// Map an aggregation token to its simple-aggregation lowering, or `None` for
/// ops that are not in the simple set (param ops, `stddev`/`stdvar`, etc.).
fn simple_aggregate_op(token: TokenType) -> Option<SimpleAggregateOp> {
    match token.id() {
        T_SUM => Some(SimpleAggregateOp::Sum),
        T_AVG => Some(SimpleAggregateOp::Avg),
        T_MIN => Some(SimpleAggregateOp::Min),
        T_MAX => Some(SimpleAggregateOp::Max),
        T_COUNT => Some(SimpleAggregateOp::Count),
        T_GROUP => Some(SimpleAggregateOp::Group),
        _ => None,
    }
}

/// Map the planner's [`SimpleAggregateOp`] (which shapes the `DataFusion` plan)
/// to the interpreter's [`AggregateOp`] (which drives the shared
/// [`apply_simple_aggregate`] kernel). Both enumerate the same six simple ops, so
/// the mapping is total; this is the seam that lets the histogram-bearing
/// operator path reuse the interpreter's reduction core.
fn simple_aggregate_op_to_aggregate_op(op: SimpleAggregateOp) -> AggregateOp {
    match op {
        SimpleAggregateOp::Sum => AggregateOp::Sum,
        SimpleAggregateOp::Avg => AggregateOp::Avg,
        SimpleAggregateOp::Min => AggregateOp::Min,
        SimpleAggregateOp::Max => AggregateOp::Max,
        SimpleAggregateOp::Count => AggregateOp::Count,
        SimpleAggregateOp::Group => AggregateOp::Group,
    }
}

/// True when a parameterized / non-simple aggregation routes through the
/// operator path (`plan_param_aggregate_expr`): `topk`/`bottomk`/`quantile`
/// (numeric-literal param), `count_values` (string-literal param),
/// `stddev`/`stdvar` (no param), and the experimental `limitk`/`limit_ratio`
/// (scalar param, resolved through the SAME interpreter helpers — including
/// `limit_ratio`'s deduplicated `InvalidRatioWarning`). The structural param
/// shape is checked here so the range gate matches the per-step planner's own
/// param requirement; a malformed-but-right-kind param still falls back at eval
/// time and the interpreter raises the canonical error.
fn param_aggregate_op_is_plannable(aggregate: &AggregateExpr) -> bool {
    match aggregate.op.id() {
        T_TOPK | T_BOTTOMK | T_QUANTILE => {
            matches!(aggregate.param.as_deref(), Some(Expr::NumberLiteral(_)))
        }
        T_COUNT_VALUES => matches!(aggregate.param.as_deref(), Some(Expr::StringLiteral(_))),
        T_STDDEV | T_STDVAR => aggregate.param.is_none(),
        // `limitk`/`limit_ratio` carry a scalar parameter resolved through the
        // interpreter helpers; the planner short-circuits a 0 param and applies
        // the shared selection kernel.
        #[cfg(feature = "experimental-functions")]
        T_LIMITK | T_LIMIT_RATIO => aggregate
            .param
            .as_deref()
            .is_some_and(|param| param.value_type() == ValueType::Scalar),
        _ => false,
    }
}

/// Map an aggregation `by`/`without` modifier to the planner [`Grouping`].
/// `None` means the aggregation has no modifier (which the caller treats as
/// `by ()`, collapsing all series into one group).
fn aggregate_grouping(modifier: Option<&LabelModifier>) -> Option<Grouping> {
    match modifier? {
        LabelModifier::Include(include) => Some(Grouping::By(include.labels.clone())),
        LabelModifier::Exclude(exclude) => Some(Grouping::Without(exclude.labels.clone())),
    }
}

fn info_data_label_matchers(selector: &VectorSelector) -> Result<Vec<LabelMatcher>> {
    let matcher_sets = label_matcher_sets(selector);
    let [matchers] = matcher_sets.as_slice() else {
        return Err(PromqlError::Plan(
            "info data label selector does not support or matchers".to_string(),
        ));
    };
    Ok(matchers.clone())
}

fn build_label_matchers(
    metric_name: Option<&str>,
    matchers: &[prom_label::Matcher],
) -> Vec<LabelMatcher> {
    let mut out = Vec::new();
    if let Some(name) = metric_name {
        out.push(LabelMatcher::new("__name__", MatchOp::Eq, name));
    }
    let mut seen = out
        .iter()
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<BTreeSet<_>>();
    for matcher in matchers {
        let op = match matcher.op {
            prom_label::MatchOp::Equal => MatchOp::Eq,
            prom_label::MatchOp::NotEqual => MatchOp::Neq,
            prom_label::MatchOp::Re(_) => MatchOp::Re,
            prom_label::MatchOp::NotRe(_) => MatchOp::Nre,
        };
        let next = LabelMatcher::new(&matcher.name, op, &matcher.value);
        if seen.insert((next.name.clone(), next.value.clone())) {
            out.push(next);
        }
    }
    out
}

/// One label matcher with its `=~`/`!~` regex compiled ahead of time, anchored
/// `^(?:...)$` exactly as [`labels_match`] anchors it.
struct CompiledLabelMatcher {
    name: String,
    op: MatchOp,
    /// The literal comparand for `Eq`/`Neq`; the precompiled, anchored regex's
    /// source is `value` too, but the compiled form lives in `regex`.
    value: String,
    regex: Option<Regex>,
}

/// A set of [`CompiledLabelMatcher`]s, so a hot loop can match many labelsets
/// without recompiling each `=~`/`!~` regex per call (the bug [`labels_match`]
/// has when invoked per sample).
struct CompiledLabelMatchers {
    matchers: Vec<CompiledLabelMatcher>,
}

impl CompiledLabelMatchers {
    /// Whether `labels` satisfies every compiled matcher, the precompiled
    /// equivalent of [`labels_match`].
    fn matches(&self, labels: &Labels) -> bool {
        for matcher in &self.matchers {
            let value = labels.get(&matcher.name).unwrap_or("");
            let is_match = match matcher.op {
                MatchOp::Eq => value == matcher.value,
                MatchOp::Neq => value != matcher.value,
                MatchOp::Re | MatchOp::Nre => {
                    let regex_matches = matcher
                        .regex
                        .as_ref()
                        .is_some_and(|regex| regex.is_match(value));
                    if matcher.op == MatchOp::Re {
                        regex_matches
                    } else {
                        !regex_matches
                    }
                }
            };
            if !is_match {
                return false;
            }
        }
        true
    }
}

/// Compile a matcher set once, precompiling each `=~`/`!~` regex (anchored
/// `^(?:...)$`). Returns the same regex-compile error [`labels_match`] would.
fn compile_label_matchers(matchers: &[LabelMatcher]) -> Result<CompiledLabelMatchers> {
    let mut compiled = Vec::with_capacity(matchers.len());
    for matcher in matchers {
        let regex = match matcher.op {
            MatchOp::Re | MatchOp::Nre => Some(
                Regex::new(&format!("^(?:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?,
            ),
            MatchOp::Eq | MatchOp::Neq => None,
        };
        compiled.push(CompiledLabelMatcher {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        });
    }
    Ok(CompiledLabelMatchers { matchers: compiled })
}

fn labels_match(labels: &Labels, matchers: &[LabelMatcher]) -> Result<bool> {
    for matcher in matchers {
        let value = labels.get(&matcher.name).unwrap_or("");
        let is_match = match matcher.op {
            MatchOp::Eq => value == matcher.value,
            MatchOp::Neq => value != matcher.value,
            MatchOp::Re | MatchOp::Nre => {
                let regex = Regex::new(&format!("^(?:{})$", matcher.value)).map_err(|error| {
                    PromqlError::Plan(format!(
                        "invalid label matcher regex for {}: {error}",
                        matcher.name
                    ))
                })?;
                let regex_matches = regex.is_match(value);
                if matcher.op == MatchOp::Re {
                    regex_matches
                } else {
                    !regex_matches
                }
            }
        };
        if !is_match {
            return Ok(false);
        }
    }
    Ok(true)
}

fn apply_selector_time_modifier(
    time_ms: i64,
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let base_time_ms = selector_at_ms(time_ms, at, bounds)?;
    apply_offset_delta(base_time_ms, selector_offset_ms(offset)?)
}

#[derive(Clone, Copy)]
struct AtModifierBounds {
    start_ms: i64,
    end_ms: i64,
}

fn selector_at_ms(
    time_ms: i64,
    at: Option<&AtModifier>,
    bounds: Option<AtModifierBounds>,
) -> Result<i64> {
    let Some(at) = at else {
        return Ok(time_ms);
    };
    match at {
        AtModifier::At(time) => system_time_ms(*time),
        AtModifier::Start => bounds.map(|bounds| bounds.start_ms).ok_or_else(|| {
            PromqlError::Unsupported(
                "@ start()/end() modifiers require range-query bounds".to_string(),
            )
        }),
        AtModifier::End => bounds.map(|bounds| bounds.end_ms).ok_or_else(|| {
            PromqlError::Unsupported(
                "@ start()/end() modifiers require range-query bounds".to_string(),
            )
        }),
    }
}

fn system_time_ms(time: SystemTime) -> Result<i64> {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration_to_i64_ms(duration),
        Err(error) => duration_to_i64_ms(error.duration()).and_then(|duration_ms| {
            duration_ms
                .checked_neg()
                .ok_or_else(|| PromqlError::Plan("@ modifier timestamp is too small".to_string()))
        }),
    }
}

fn duration_to_i64_ms(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PromqlError::Plan("@ modifier timestamp is too large".to_string()))
}

fn selector_offset_ms(offset: Option<&Offset>) -> Result<i64> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    let (duration, sign) = match offset {
        Offset::Pos(duration) => (*duration, -1_i64),
        Offset::Neg(duration) => (*duration, 1_i64),
    };
    let duration_ms = duration_ms(duration)?;
    duration_ms
        .checked_mul(sign)
        .ok_or_else(|| PromqlError::Plan("offset duration is too large".to_string()))
}

fn apply_offset_delta(time_ms: i64, offset_ms: i64) -> Result<i64> {
    time_ms
        .checked_add(offset_ms)
        .ok_or_else(|| PromqlError::Plan("offset evaluation time overflow".to_string()))
}

#[allow(
    clippy::cast_precision_loss,
    reason = "PromQL timestamps are represented as f64 seconds"
)]
fn timestamp_seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms as f64 / 1000.0
}

fn duration_ms(duration: std::time::Duration) -> Result<i64> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PromqlError::Plan("range selector duration is too large".to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use assert2::assert;
    use crabka_blockstore::Labels;
    use crabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

    use crate::{
        EngineOpts, InMemoryMetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue,
    };

    use super::{MAX_RESOLUTION_POINTS, check_resolution_points, match_rate_range_call};

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    fn float_value(value: &SampleValue) -> f64 {
        match value {
            SampleValue::Float(value) => *value,
            SampleValue::Histogram(_) => panic!("expected float sample"),
        }
    }

    fn assert_single_float_sample(result: &QueryResult, job: &str, expected: f64, context: &str) {
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector for {context}");
        };
        assert_eq!(samples.len(), 1, "{context}");
        assert_eq!(samples[0].labels.get("__name__"), None, "{context}");
        assert_eq!(samples[0].labels.get("job"), Some(job), "{context}");
        assert!(
            approx_eq(float_value(&samples[0].value), expected),
            "{context}"
        );
    }

    fn assert_single_on_x_float_sample(result: &QueryResult, expected: f64, context: &str) {
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector for {context}");
        };
        assert_eq!(samples.len(), 1, "{context}");
        assert_eq!(samples[0].labels.get("__name__"), None, "{context}");
        assert_eq!(samples[0].labels.get("job"), None, "{context}");
        assert_eq!(samples[0].labels.get("x"), Some("1"), "{context}");
        assert!(
            approx_eq(float_value(&samples[0].value), expected),
            "{context}"
        );
    }

    #[cfg(feature = "experimental-functions")]
    fn sample_instances(samples: &[crate::InstantSample]) -> Vec<&str> {
        let mut instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").expect("instance label"))
            .collect::<Vec<_>>();
        instances.sort_unstable();
        instances
    }

    fn approx_eq(left: f64, right: f64) -> bool {
        if !left.is_finite() || !right.is_finite() {
            return left == right;
        }
        // Relative tolerance: an absolute `f64::EPSILON` bound is too tight for
        // magnitudes above ~1 — a Kahan/Welford-compensated fold (matching
        // Prometheus) rounds in the last ULP, e.g. a population variance of 4.0
        // lands at 3.999999999999_9996. Scale the bound by operand magnitude.
        let scale = left.abs().max(right.abs()).max(1.0);
        (left - right).abs() <= f64::EPSILON * 4.0 * scale
    }

    fn stale_nan() -> f64 {
        f64::from_bits(0x7ff0_0000_0000_0002)
    }

    /// Compare two sorted instant-sample vectors for the parity tests, treating
    /// floats bit-exactly (so a genuine NaN equals a genuine NaN). `PartialEq`
    /// on `SampleValue::Float` uses IEEE `==`, under which `NaN != NaN`; a plain
    /// `assert_eq!` would therefore spuriously fail whenever a path correctly
    /// preserves a genuine NaN value. Stale-NaN markers are not expected to
    /// survive selection on either path, so they never reach this comparison.
    fn instant_samples_match(
        left: &[crate::InstantSample],
        right: &[crate::InstantSample],
    ) -> bool {
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right.iter()).all(|(l, r)| {
            l.labels == r.labels
                && l.ts_ms == r.ts_ms
                && match (&l.value, &r.value) {
                    (SampleValue::Float(a), SampleValue::Float(b)) => a.to_bits() == b.to_bits(),
                    (other_l, other_r) => other_l == other_r,
                }
        })
    }

    /// Assert the post-fix staleness semantics for the aggregate parity test's
    /// `nan_metric` queries: `sum` keeps the genuine NaN (NaN value, not a stale
    /// marker), and `count` drops the stale-NaN marker before counting (2, not 3).
    fn assert_aggregate_nan_staleness(query: &str, via_operators: &[crate::InstantSample]) {
        if query == "sum(nan_metric)" {
            assert_eq!(via_operators.len(), 1, "sum(nan_metric) row missing");
            let value = float_value(&via_operators[0].value);
            assert!(value.is_nan(), "genuine NaN not kept through sum: {value}");
            assert!(
                !super::is_stale_nan(value),
                "aggregate value is a stale marker"
            );
        }
        if query == "count(nan_metric)" {
            assert_eq!(via_operators.len(), 1, "count(nan_metric) row missing");
            let value = float_value(&via_operators[0].value);
            assert!(
                approx_eq(value, 2.0),
                "stale marker not dropped before count: got {value}, want 2"
            );
        }
    }

    /// Assert the NaN-ignoring `min`/`max` aggregation rule for the parity
    /// test's `minmax_nan` queries on absolute values: the mixed group's
    /// extremum is taken over its non-NaN samples (min=1, max=4), and the
    /// all-NaN group is kept with a NaN result (the series is not dropped).
    fn assert_minmax_nan_ignoring(query: &str, via_operators: &[crate::InstantSample]) {
        // Look up a group's value by its `g` label.
        let by_group = |g: &str| -> f64 {
            let sample = via_operators
                .iter()
                .find(|sample| sample.labels.get("g") == Some(g))
                .unwrap_or_else(|| panic!("`{query}`: group g={g} missing"));
            float_value(&sample.value)
        };
        match query {
            "min by (g) (minmax_nan)" => {
                assert_eq!(via_operators.len(), 2, "`{query}`: expected mixed+allnan");
                let mixed = by_group("mixed");
                assert!(
                    approx_eq(mixed, 1.0),
                    "`{query}`: mixed min over non-NaN: {mixed}"
                );
                let allnan = by_group("allnan");
                assert!(allnan.is_nan(), "`{query}`: all-NaN min not NaN: {allnan}");
            }
            "max by (g) (minmax_nan)" => {
                assert_eq!(via_operators.len(), 2, "`{query}`: expected mixed+allnan");
                let mixed = by_group("mixed");
                assert!(
                    approx_eq(mixed, 4.0),
                    "`{query}`: mixed max over non-NaN: {mixed}"
                );
                let allnan = by_group("allnan");
                assert!(allnan.is_nan(), "`{query}`: all-NaN max not NaN: {allnan}");
            }
            // `min`/`max` with no grouping fold both groups together: the global
            // extremum is over the only non-NaN samples (the mixed group's
            // {4, 1}), so min=1 and max=4 (the all-NaN group is ignored, but its
            // presence does not force a NaN because the mixed group has finite
            // values).
            "min(minmax_nan)" => {
                assert_eq!(via_operators.len(), 1, "`{query}`: expected one group");
                let value = float_value(&via_operators[0].value);
                assert!(
                    approx_eq(value, 1.0),
                    "`{query}`: global min over non-NaN: {value}"
                );
            }
            "max(minmax_nan)" => {
                assert_eq!(via_operators.len(), 1, "`{query}`: expected one group");
                let value = float_value(&via_operators[0].value);
                assert!(
                    approx_eq(value, 4.0),
                    "`{query}`: global max over non-NaN: {value}"
                );
            }
            _ => {}
        }
    }

    /// Assert the SPARSE aggregate-over-rate rule for the parity test's
    /// `sparse_total` queries on absolute values: the no-value (sparse) series is
    /// excluded from its group, the `g="mix"` group survives with only its dense
    /// member's contribution, and the all-no-value `g="allsparse"` group produces
    /// no result row at all (the series is absent, not present-with-NaN).
    fn assert_sparse_aggregate_excludes_no_value(
        query: &str,
        via_operators: &[crate::InstantSample],
    ) {
        let group_value = |g: &str| -> Option<f64> {
            via_operators
                .iter()
                .find(|sample| sample.labels.get("g") == Some(g))
                .map(|sample| float_value(&sample.value))
        };
        match query {
            // g="mix" survives (its dense member has a rate); g="allsparse" has
            // no value-bearing member, so it is absent. Only one row total.
            "sum by (g) (rate(sparse_total[2m]))"
            | "avg by (g) (rate(sparse_total[2m]))"
            | "min by (g) (rate(sparse_total[2m]))"
            | "max by (g) (rate(sparse_total[2m]))"
            | "count by (g) (rate(sparse_total[2m]))"
            | "group by (g) (rate(sparse_total[2m]))" => {
                assert_eq!(
                    via_operators.len(),
                    1,
                    "`{query}`: only g=mix survives (g=allsparse is absent)"
                );
                assert!(group_value("mix").is_some(), "`{query}`: g=mix row missing");
                assert!(
                    group_value("allsparse").is_none(),
                    "`{query}`: g=allsparse must be absent (all members no-value)"
                );
                // `count`/`group` over g=mix see exactly the one dense member.
                if query == "count by (g) (rate(sparse_total[2m]))" {
                    assert!(
                        approx_eq(group_value("mix").unwrap(), 1.0),
                        "`{query}`: count over g=mix must be 1 (sparse member excluded)"
                    );
                }
                if query == "group by (g) (rate(sparse_total[2m]))" {
                    assert!(
                        approx_eq(group_value("mix").unwrap(), 1.0),
                        "`{query}`: group=1"
                    );
                }
            }
            // No grouping: the global aggregate is over the single dense rate.
            "count (rate(sparse_total[2m]))" => {
                assert_eq!(via_operators.len(), 1, "`{query}`: one global row");
                assert!(
                    approx_eq(float_value(&via_operators[0].value), 1.0),
                    "`{query}`: global count must be 1 (only the dense rate)"
                );
            }
            "sum (rate(sparse_total[2m]))" => {
                assert_eq!(via_operators.len(), 1, "`{query}`: one global row");
            }
            // The `*_over_time` window strands every sparse member, leaving only
            // the dense member in g=mix; g=allsparse is absent.
            "count by (g) (avg_over_time(sparse_total[30s]))" => {
                assert_eq!(via_operators.len(), 1, "`{query}`: only g=mix survives");
                assert!(
                    approx_eq(group_value("mix").unwrap(), 1.0),
                    "`{query}`: count over g=mix must be 1"
                );
                assert!(
                    group_value("allsparse").is_none(),
                    "`{query}`: g=allsparse must be absent"
                );
            }
            _ => {}
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "test defines an inline CountingStore mock with a full MetricStore impl"
    )]
    async fn range_query_scans_store_once_per_matcher_set_not_per_step() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crabka_blockstore::LabelMatcher;

        use crate::error::Result;
        use crate::store::{
            ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord,
            MetricStore, ScanResult, TsdbBlock, TsdbStats,
        };

        // Wraps the in-memory store and counts store-level scans / series
        // resolutions, to prove the range driver no longer re-scans per step.
        struct CountingStore {
            inner: InMemoryMetricStore,
            scans: Arc<AtomicUsize>,
            series_calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl MetricStore for CountingStore {
            async fn scan(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<ScanResult> {
                self.scans.fetch_add(1, Ordering::SeqCst);
                self.inner.scan(t, m, s, e).await
            }
            async fn series(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<Labels>> {
                self.series_calls.fetch_add(1, Ordering::SeqCst);
                self.inner.series(t, m, s, e).await
            }
            async fn label_names(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<String>> {
                self.inner.label_names(t, m, s, e).await
            }
            async fn label_values(
                &self,
                t: &str,
                name: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<String>> {
                self.inner.label_values(t, name, m, s, e).await
            }
            async fn exemplars(
                &self,
                t: &str,
                m: &[LabelMatcher],
                s: i64,
                e: i64,
            ) -> Result<Vec<ExemplarRecord>> {
                self.inner.exemplars(t, m, s, e).await
            }
            async fn metadata(&self, t: &str, metric: Option<&str>) -> Result<Vec<MetadataRecord>> {
                self.inner.metadata(t, metric).await
            }
            async fn cardinality_label_names(&self, t: &str) -> Result<Vec<LabelNameCardinality>> {
                self.inner.cardinality_label_names(t).await
            }
            async fn cardinality_label_values(
                &self,
                t: &str,
            ) -> Result<Vec<LabelValueCardinality>> {
                self.inner.cardinality_label_values(t).await
            }
            async fn cardinality_active_series(&self, t: &str) -> Result<Vec<Labels>> {
                self.inner.cardinality_active_series(t).await
            }
            async fn tsdb_stats(&self, t: &str) -> Result<TsdbStats> {
                self.inner.tsdb_stats(t).await
            }
            async fn tsdb_blocks(&self, t: &str) -> Result<Vec<TsdbBlock>> {
                self.inner.tsdb_blocks(t).await
            }
        }

        let mut inner = InMemoryMetricStore::new();
        for i in 0..20 {
            inner.push_float(
                "t",
                labels(&[("__name__", "up"), ("job", "broker")]),
                i * 15_000,
                1.0,
            );
        }
        let scans = Arc::new(AtomicUsize::new(0));
        let series_calls = Arc::new(AtomicUsize::new(0));
        let engine = PromqlEngine::new(
            Arc::new(CountingStore {
                inner,
                scans: Arc::clone(&scans),
                series_calls: Arc::clone(&series_calls),
            }),
            EngineOpts::default(),
        );

        // 20 steps at 15s. Pre-fix this scanned the store ~2× per step (float +
        // histogram probe) plus a per-step series resolution. With the union-window
        // cache it is one float scan + one histogram scan + one series resolution
        // total, reused across every step.
        let result = engine
            .eval_range_via_planner_forced("t", "count({job=\"broker\"})", 0, 19 * 15_000, 15_000)
            .await
            .unwrap();
        assert!(matches!(result, QueryResult::RangeMatrix(_)));
        assert!(
            scans.load(Ordering::SeqCst) == 2,
            "store scans should collapse to one float + one histogram union scan, got {}",
            scans.load(Ordering::SeqCst)
        );
        assert!(
            series_calls.load(Ordering::SeqCst) == 1,
            "series resolution should be cached across steps, got {}",
            series_calls.load(Ordering::SeqCst)
        );
    }

    fn set_op_store() -> InMemoryMetricStore {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 2.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "up"), ("instance", instance), ("job", "api")]),
                10_000,
                value,
            );
        }
        for (instance, value) in [("b", 20.0), ("c", 30.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "target_info"),
                    ("instance", instance),
                    ("region", "east"),
                ]),
                10_000,
                value,
            );
        }
        store
    }

    fn native_histogram(count: f64, sum: f64) -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![],
            positive_counts: vec![],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }

    fn native_histogram_store() -> InMemoryMetricStore {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            10_000,
            native_histogram(4.0, 10.0),
        );
        store
    }

    fn mixed_histogram_store() -> InMemoryMetricStore {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "series"), ("host", "a")]),
            0,
            native_histogram(4.0, 5.0),
        );
        for (le, value) in [("0.1", 2.0), ("1", 3.0), ("+Inf", 9.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "series"), ("host", "a"), ("le", le)]),
                0,
                value,
            );
        }
        store
    }

    #[tokio::test]
    async fn histogram_quantile_mixed_emits_exact_warning_and_no_info() {
        let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
        let (_, annotations) = engine
            .query_instant_with_annotations("tenant-a", "histogram_quantile(0.8, series)", 0)
            .await
            .expect("query");
        assert_eq!(
            annotations.warnings,
            vec![
                "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\""
                    .to_string()
            ]
        );
        assert!(annotations.infos.is_empty());
    }

    #[tokio::test]
    async fn histogram_fraction_mixed_emits_exact_warning() {
        let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
        let (_, annotations) = engine
            .query_instant_with_annotations("tenant-a", "histogram_fraction(-Inf, 1, series)", 0)
            .await
            .expect("query");
        assert!(annotations.warnings.iter().any(|w| w
            == "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\""));
    }

    #[tokio::test]
    async fn histogram_float_comparison_emits_incompatible_types_info() {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "h"), ("job", "app")]),
            0,
            native_histogram(4.0, 5.0),
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let (result, annotations) = engine
            .query_instant_with_annotations("tenant-a", "h > 80", 0)
            .await
            .expect("query");
        assert!(matches!(result, QueryResult::InstantVector(ref v) if v.is_empty()));
        assert_eq!(
            annotations.infos,
            vec![
                "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float"
                    .to_string()
            ]
        );
        assert!(annotations.warnings.is_empty());
    }

    #[tokio::test]
    async fn clean_query_raises_no_annotations() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let (_, annotations) = engine
            .query_instant_with_annotations("tenant-a", "up", 10_000)
            .await
            .expect("query");
        assert!(annotations.is_empty());
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn limit_ratio_over_bound_emits_capping_warning() {
        let mut store = InMemoryMetricStore::new();
        for instance in ["0", "1"] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests"), ("instance", instance)]),
                0,
                1.0,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let (_, annotations) = engine
            .query_instant_with_annotations("tenant-a", "count(limit_ratio(1.1, http_requests))", 0)
            .await
            .expect("query");
        assert_eq!(
            annotations.warnings,
            vec![
                "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 1"
                    .to_string()
            ]
        );
    }

    #[tokio::test]
    async fn instant_label_join_combines_source_labels() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "up"),
                ("job", "api"),
                ("instance", "a"),
                ("zone", "us-east-1a"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"label_join(up, "target", "/", "job", "instance")"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("target") == Some("api/a"));
        assert!(samples[0].labels.get("zone") == Some("us-east-1a"));
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_label_replace_uses_regex_capture_groups() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("instance", "api-1:9100")]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"label_replace(up, "host", "$1", "instance", "([^:]+):.*")"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("host") == Some("api-1"));
        assert!(samples[0].labels.get("instance") == Some("api-1:9100"));
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_clamp_bounds_vector_values() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("low", -5.0), ("mid", 7.0), ("high", 20.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("instance", instance)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "clamp(temperature_celsius, 0, 10)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 3);
        let values = samples
            .iter()
            .map(|sample| {
                (
                    sample.labels.get("instance").unwrap().to_string(),
                    float_value(&sample.value),
                )
            })
            .collect::<Vec<_>>();
        assert!(values.contains(&("low".to_string(), 0.0)));
        assert!(values.contains(&("mid".to_string(), 7.0)));
        assert!(values.contains(&("high".to_string(), 10.0)));
    }

    #[tokio::test]
    async fn instant_clamp_min_and_max_apply_single_bound() {
        let mut store = InMemoryMetricStore::new();
        for (metric, value) in [("below", -5.0), ("inside", 7.0), ("above", 20.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("case", metric)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let min_result = engine
            .query_instant("tenant-a", "clamp_min(temperature_celsius, 0)", 10_000)
            .await
            .unwrap();
        let max_result = engine
            .query_instant("tenant-a", "clamp_max(temperature_celsius, 10)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(min_samples) = min_result else {
            panic!("expected vector");
        };
        let QueryResult::InstantVector(max_samples) = max_result else {
            panic!("expected vector");
        };
        assert!(min_samples.len() == 3);
        assert!(max_samples.len() == 3);
        assert!(min_samples.iter().any(|sample| {
            sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), 0.0)
        }));
        assert!(min_samples.iter().any(|sample| {
            sample.labels.get("case") == Some("above")
                && approx_eq(float_value(&sample.value), 20.0)
        }));
        assert!(max_samples.iter().any(|sample| {
            sample.labels.get("case") == Some("below")
                && approx_eq(float_value(&sample.value), -5.0)
        }));
        assert!(max_samples.iter().any(|sample| {
            sample.labels.get("case") == Some("above")
                && approx_eq(float_value(&sample.value), 10.0)
        }));
    }

    #[tokio::test]
    async fn instant_unary_numeric_functions_transform_vector_values() {
        let mut store = InMemoryMetricStore::new();
        for (case, value) in [("neg", -1.2), ("zero", 0.0), ("pos", 1.2)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("case", case)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            (
                "ceil(temperature_celsius)",
                [("neg", -1.0), ("zero", 0.0), ("pos", 2.0)],
            ),
            (
                "floor(temperature_celsius)",
                [("neg", -2.0), ("zero", 0.0), ("pos", 1.0)],
            ),
            (
                "sgn(temperature_celsius)",
                [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
            ),
            (
                "abs(temperature_celsius)",
                [("neg", 1.2), ("zero", 0.0), ("pos", 1.2)],
            ),
            (
                "sqrt(abs(temperature_celsius))",
                [
                    ("neg", 1.2_f64.sqrt()),
                    ("zero", 0.0),
                    ("pos", 1.2_f64.sqrt()),
                ],
            ),
            (
                "exp(abs(temperature_celsius))",
                [
                    ("neg", 1.2_f64.exp()),
                    ("zero", 1.0),
                    ("pos", 1.2_f64.exp()),
                ],
            ),
            (
                "ln(abs(temperature_celsius))",
                [
                    ("neg", 1.2_f64.ln()),
                    ("zero", f64::NEG_INFINITY),
                    ("pos", 1.2_f64.ln()),
                ],
            ),
            (
                "log2(abs(temperature_celsius))",
                [
                    ("neg", 1.2_f64.log2()),
                    ("zero", f64::NEG_INFINITY),
                    ("pos", 1.2_f64.log2()),
                ],
            ),
            (
                "log10(abs(temperature_celsius))",
                [
                    ("neg", 1.2_f64.log10()),
                    ("zero", f64::NEG_INFINITY),
                    ("pos", 1.2_f64.log10()),
                ],
            ),
            (
                "round(temperature_celsius)",
                [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
            ),
            (
                "round(temperature_celsius, 0.5)",
                [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
            ),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 3);
            for (case, value) in expected {
                let sample = samples
                    .iter()
                    .find(|sample| sample.labels.get("case") == Some(case))
                    .expect("sample for case");
                assert!(sample.labels.get("__name__").is_none());
                assert!(approx_eq(float_value(&sample.value), value));
            }
        }
    }

    #[tokio::test]
    async fn instant_hyperbolic_functions_transform_vector_values() {
        let mut store = InMemoryMetricStore::new();
        for (case, value) in [("neg", -1.2), ("zero", 0.0), ("pos", 1.2)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("case", case)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            (
                "sinh(temperature_celsius)",
                [
                    ("neg", (-1.2_f64).sinh()),
                    ("zero", 0.0_f64.sinh()),
                    ("pos", 1.2_f64.sinh()),
                ],
            ),
            (
                "cosh(temperature_celsius)",
                [
                    ("neg", (-1.2_f64).cosh()),
                    ("zero", 0.0_f64.cosh()),
                    ("pos", 1.2_f64.cosh()),
                ],
            ),
            (
                "tanh(temperature_celsius)",
                [
                    ("neg", (-1.2_f64).tanh()),
                    ("zero", 0.0_f64.tanh()),
                    ("pos", 1.2_f64.tanh()),
                ],
            ),
            (
                "asinh(temperature_celsius)",
                [
                    ("neg", (-1.2_f64).asinh()),
                    ("zero", 0.0_f64.asinh()),
                    ("pos", 1.2_f64.asinh()),
                ],
            ),
            (
                "acosh(abs(temperature_celsius) + 1)",
                [
                    ("neg", 2.2_f64.acosh()),
                    ("zero", 1.0_f64.acosh()),
                    ("pos", 2.2_f64.acosh()),
                ],
            ),
            (
                "atanh(temperature_celsius / 2)",
                [
                    ("neg", (-0.6_f64).atanh()),
                    ("zero", 0.0_f64.atanh()),
                    ("pos", 0.6_f64.atanh()),
                ],
            ),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 3);
            for (case, value) in expected {
                let sample = samples
                    .iter()
                    .find(|sample| sample.labels.get("case") == Some(case))
                    .expect("sample for case");
                assert!(sample.labels.get("__name__").is_none());
                assert!(approx_eq(float_value(&sample.value), value));
            }
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "table-driven coverage keeps related PromQL trig functions readable"
    )]
    #[tokio::test]
    async fn instant_trigonometric_functions_transform_vector_values() {
        let mut store = InMemoryMetricStore::new();
        for (case, value) in [
            ("zero", 0.0),
            ("half_pi", std::f64::consts::FRAC_PI_2),
            ("pi", std::f64::consts::PI),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "angle_radians"), ("case", case)]),
                10_000,
                value,
            );
        }
        for (case, value) in [("neg", -0.5), ("zero", 0.0), ("pos", 0.5)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "unit_value"), ("case", case)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            (
                "sin(angle_radians)",
                [
                    ("zero", 0.0_f64.sin()),
                    ("half_pi", std::f64::consts::FRAC_PI_2.sin()),
                    ("pi", std::f64::consts::PI.sin()),
                ],
            ),
            (
                "cos(angle_radians)",
                [
                    ("zero", 0.0_f64.cos()),
                    ("half_pi", std::f64::consts::FRAC_PI_2.cos()),
                    ("pi", std::f64::consts::PI.cos()),
                ],
            ),
            (
                "tan(angle_radians)",
                [
                    ("zero", 0.0_f64.tan()),
                    ("half_pi", std::f64::consts::FRAC_PI_2.tan()),
                    ("pi", std::f64::consts::PI.tan()),
                ],
            ),
            (
                "deg(angle_radians)",
                [("zero", 0.0), ("half_pi", 90.0), ("pi", 180.0)],
            ),
            (
                "rad(deg(angle_radians))",
                [
                    ("zero", 0.0),
                    ("half_pi", std::f64::consts::FRAC_PI_2),
                    ("pi", std::f64::consts::PI),
                ],
            ),
            (
                "asin(unit_value)",
                [
                    ("neg", (-0.5_f64).asin()),
                    ("zero", 0.0_f64.asin()),
                    ("pos", 0.5_f64.asin()),
                ],
            ),
            (
                "acos(unit_value)",
                [
                    ("neg", (-0.5_f64).acos()),
                    ("zero", 0.0_f64.acos()),
                    ("pos", 0.5_f64.acos()),
                ],
            ),
            (
                "atan(unit_value)",
                [
                    ("neg", (-0.5_f64).atan()),
                    ("zero", 0.0_f64.atan()),
                    ("pos", 0.5_f64.atan()),
                ],
            ),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 3);
            for (case, value) in expected {
                let sample = samples
                    .iter()
                    .find(|sample| sample.labels.get("case") == Some(case))
                    .expect("sample for case");
                assert!(sample.labels.get("__name__").is_none());
                assert!(approx_eq(float_value(&sample.value), value));
            }
        }
    }

    #[tokio::test]
    async fn scalar_pi_function_returns_pi_constant() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "pi()", 10_000)
            .await
            .unwrap();
        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: std::f64::consts::PI,
                }
        );
    }

    #[tokio::test]
    async fn instant_sort_functions_order_vector_by_sample_value() {
        let mut store = InMemoryMetricStore::new();
        for (instance, zone, value) in [
            ("api-b", "us-west-2b", 3.0),
            ("api-a", "us-east-1a", 1.0),
            ("api-c", "us-east-1a", 2.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "queue_depth"),
                    ("instance", instance),
                    ("zone", zone),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected_instances) in [
            ("sort(queue_depth)", ["api-a", "api-c", "api-b"]),
            ("sort_desc(queue_depth)", ["api-b", "api-c", "api-a"]),
            (
                r#"sort_by_label(queue_depth, "zone", "instance")"#,
                ["api-a", "api-c", "api-b"],
            ),
            (
                r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
                ["api-b", "api-c", "api-a"],
            ),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 3);
            let instances = samples
                .iter()
                .map(|sample| sample.labels.get("instance").unwrap().to_string())
                .collect::<Vec<_>>();
            assert!(instances == expected_instances);
            assert!(
                samples
                    .iter()
                    .all(|sample| sample.labels.get("__name__") == Some("queue_depth"))
            );
        }
    }

    #[tokio::test]
    async fn instant_calendar_functions_extract_utc_fields_from_sample_values() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "event_timestamp_seconds"), ("case", "leap")]),
            10_000,
            1_709_178_060.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            ("year(event_timestamp_seconds)", 2024.0),
            ("month(event_timestamp_seconds)", 2.0),
            ("day_of_month(event_timestamp_seconds)", 29.0),
            ("day_of_week(event_timestamp_seconds)", 4.0),
            ("day_of_year(event_timestamp_seconds)", 60.0),
            ("days_in_month(event_timestamp_seconds)", 29.0),
            ("hour(event_timestamp_seconds)", 3.0),
            ("minute(event_timestamp_seconds)", 41.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1);
            assert!(samples[0].labels.get("__name__").is_none());
            assert!(samples[0].labels.get("case") == Some("leap"));
            assert!(approx_eq(float_value(&samples[0].value), expected));
        }
    }

    #[tokio::test]
    async fn instant_calendar_functions_without_args_use_eval_time() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "minute()", 3_660_000)
            .await
            .unwrap();

        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 3_660_000,
                    value: 1.0,
                }
        );
    }

    #[tokio::test]
    async fn instant_clamp_with_reversed_bounds_returns_empty_vector() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("instance", "api")]),
            10_000,
            7.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "clamp(temperature_celsius, 10, 0)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn instant_selector_returns_latest_sample_within_lookback() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            20_000,
            2.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            40_000,
            4.0,
        );

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                lookback_delta_ms: 15_000,
                max_samples: 100,
                ..EngineOpts::default()
            },
        );

        let result = engine
            .query_instant("tenant-a", "up", 30_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(samples[0].ts_ms == 20_000);
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_selector_offset_shifts_evaluation_time_backwards() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            60_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            120_000,
            2.0,
        );

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                lookback_delta_ms: 30_000,
                max_samples: 100,
                ..EngineOpts::default()
            },
        );
        let result = engine
            .query_instant("tenant-a", "up offset 1m", 120_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].ts_ms == 60_000);
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_selector_at_uses_absolute_evaluation_time() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            60_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            120_000,
            2.0,
        );

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                lookback_delta_ms: 30_000,
                max_samples: 100,
                ..EngineOpts::default()
            },
        );
        let result = engine
            .query_instant("tenant-a", "up @ 60", 120_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].ts_ms == 60_000);
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_selector_at_and_offset_combine_order_independently() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            60_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            120_000,
            2.0,
        );

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                lookback_delta_ms: 30_000,
                max_samples: 100,
                ..EngineOpts::default()
            },
        );
        for query in ["up @ 120 offset 1m", "up offset 1m @ 120"] {
            let result = engine
                .query_instant("tenant-a", query, 999_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1);
            assert!(samples[0].ts_ms == 60_000);
            assert!(approx_eq(float_value(&samples[0].value), 1.0));
        }
    }

    #[tokio::test]
    async fn instant_selector_honors_label_matchers_and_tenant() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "web")]),
            10_000,
            0.0,
        );
        store.push_float(
            "tenant-b",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            9.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", r#"up{job=~"a.*"}"#, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_selector_or_matchers_union_matching_series() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            10_000,
            2.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "db"), ("instance", "c")]),
            10_000,
            3.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", r#"up{job="api" or job="web"}"#, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        let values_by_job = samples
            .iter()
            .map(|sample| {
                (
                    sample.labels.get("job").expect("job label").to_string(),
                    float_value(&sample.value),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(approx_eq(values_by_job["api"], 1.0));
        assert!(approx_eq(values_by_job["web"], 2.0));
        assert!(!values_by_job.contains_key("db"));
    }

    #[tokio::test]
    async fn instant_selector_stale_marker_terminates_series_before_lookback_expiry() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            20_000,
            stale_nan(),
        );

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                lookback_delta_ms: 60_000,
                max_samples: 100,
                ..EngineOpts::default()
            },
        );
        let result = engine
            .query_instant("tenant-a", "up", 30_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn instant_sum_aggregates_all_series() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "web")]),
            10_000,
            2.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "sum(up)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.is_empty());
        assert!(approx_eq(float_value(&samples[0].value), 3.0));
    }

    #[tokio::test]
    async fn instant_sum_by_groups_by_exact_labels_and_drops_metric_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
            10_000,
            2.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "c")]),
            10_000,
            4.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "sum by (job) (up)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        let api = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("api"))
            .expect("api group");
        assert!(api.labels.get("__name__").is_none());
        assert!(api.labels.get("instance").is_none());
        assert!(approx_eq(float_value(&api.value), 3.0));
        let web = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("web"))
            .expect("web group");
        assert!(approx_eq(float_value(&web.value), 4.0));
    }

    #[tokio::test]
    async fn instant_count_counts_series() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "web")]),
            10_000,
            0.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "count(up)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_group_returns_one_for_each_group() {
        let mut store = InMemoryMetricStore::new();
        for (job, value) in [("api", 10.0), ("api", 30.0), ("web", 99.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "up"), ("job", job)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "group by (job) (up)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        for sample in samples {
            assert!(sample.labels.get("__name__").is_none());
            assert!(approx_eq(float_value(&sample.value), 1.0));
        }
    }

    #[tokio::test]
    async fn instant_stddev_and_stdvar_aggregate_population_variance() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
            .into_iter()
            .enumerate()
        {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "latency_seconds"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let stdvar = engine
            .query_instant("tenant-a", "stdvar(latency_seconds)", 10_000)
            .await
            .unwrap();
        let stddev = engine
            .query_instant("tenant-a", "stddev(latency_seconds)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(stdvar_samples) = stdvar else {
            panic!("expected vector");
        };
        let QueryResult::InstantVector(stddev_samples) = stddev else {
            panic!("expected vector");
        };
        assert!(stdvar_samples.len() == 1);
        assert!(stddev_samples.len() == 1);
        assert!(approx_eq(float_value(&stdvar_samples[0].value), 4.0));
        assert!(approx_eq(float_value(&stddev_samples[0].value), 2.0));
    }

    /// M16: `stdvar`/`stddev` over a large-offset close-valued group must not
    /// catastrophically cancel into a negative variance (whose `sqrt` is NaN).
    /// Welford yields the small positive population variance `{0,1,2}` -> 2/3.
    #[tokio::test]
    async fn instant_stdvar_aggregate_is_stable_for_large_offset_group() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [1e8, 1e8 + 1.0, 1e8 + 2.0].into_iter().enumerate() {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "big"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                10_000,
                value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(stdvar) = engine
            .query_instant("tenant-a", "stdvar(big)", 10_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert!(stdvar.len() == 1);
        let value = float_value(&stdvar[0].value);
        assert!(!value.is_nan(), "stdvar must be finite, got NaN");
        assert!(value > 0.0, "stdvar must be positive, got {value}");
        assert!(approx_eq(value, 2.0 / 3.0), "stdvar == 2/3, got {value}");
    }

    /// M17: `avg` of very-large-magnitude samples must not overflow the running
    /// sum to +/-Inf; the incremental Kahan mean stays finite and equals the
    /// common value for two equal maxima.
    #[tokio::test]
    async fn instant_avg_aggregate_does_not_overflow() {
        let mut store = InMemoryMetricStore::new();
        for instance in 0..2 {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "huge"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                10_000,
                f64::MAX,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(avg) = engine
            .query_instant("tenant-a", "avg(huge)", 10_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert!(avg.len() == 1);
        let value = float_value(&avg[0].value);
        assert!(value.is_finite(), "avg must stay finite, got {value}");
        assert!(approx_eq(value, f64::MAX));
    }

    /// M19: `count_values` renders a non-finite sample value through the canonical
    /// Prometheus float formatter, so `+Inf` (not `f64::to_string`'s `inf`)
    /// becomes the label value.
    #[tokio::test]
    async fn instant_count_values_formats_infinity_as_prometheus() {
        let mut store = InMemoryMetricStore::new();
        for instance in 0..2 {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "ratio"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                10_000,
                f64::INFINITY,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let QueryResult::InstantVector(samples) = engine
            .query_instant("tenant-a", r#"count_values("v", ratio)"#, 10_000)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(
            samples[0].labels.get("v") == Some("+Inf"),
            "count_values must render +Inf, got {:?}",
            samples[0].labels.get("v")
        );
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_count_values_counts_by_sample_value() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [200.0, 200.0, 500.0].into_iter().enumerate() {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_responses_total"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"count_values("code", http_responses_total)"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        let ok = samples
            .iter()
            .find(|sample| sample.labels.get("code") == Some("200"))
            .expect("200 bucket");
        assert!(ok.labels.get("__name__").is_none());
        assert!(approx_eq(float_value(&ok.value), 2.0));
        let err = samples
            .iter()
            .find(|sample| sample.labels.get("code") == Some("500"))
            .expect("500 bucket");
        assert!(approx_eq(float_value(&err.value), 1.0));
    }

    #[tokio::test]
    async fn instant_count_values_counts_native_histogram_sample_values() {
        let mut repeated = native_histogram(4.0, 10.0);
        repeated.zero_count = 1.0;
        repeated.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        repeated.positive_counts = vec![3.0];
        let mut distinct = repeated.clone();
        distinct.sum = 12.0;

        let mut store = InMemoryMetricStore::new();
        for (instance, histogram) in [
            ("a", repeated.clone()),
            ("b", repeated.clone()),
            ("c", distinct),
        ] {
            store.push_histogram(
                "tenant-a",
                labels(&[
                    ("__name__", "request_duration_seconds"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                histogram,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"count_values by (job) ("histogram", request_duration_seconds)"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().all(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("histogram").is_some()
        }));
        assert!(
            samples
                .iter()
                .any(|sample| approx_eq(float_value(&sample.value), 2.0))
        );
        assert!(
            samples
                .iter()
                .any(|sample| approx_eq(float_value(&sample.value), 1.0))
        );
    }

    #[tokio::test]
    async fn instant_topk_keeps_largest_samples_with_original_labels() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "topk(2, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("instance") == Some("b")
                && approx_eq(float_value(&sample.value), 3.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("instance") == Some("c")
                && approx_eq(float_value(&sample.value), 2.0)
        }));
    }

    #[tokio::test]
    async fn instant_bottomk_keeps_smallest_samples_with_original_labels() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "bottomk(2, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("instance") == Some("a")
                && approx_eq(float_value(&sample.value), 1.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("instance") == Some("c")
                && approx_eq(float_value(&sample.value), 2.0)
        }));
    }

    #[tokio::test]
    async fn instant_topk_by_selects_largest_sample_per_group_with_original_labels() {
        let mut store = InMemoryMetricStore::new();
        for (job, instance, value) in [
            ("api", "a", 1.0),
            ("api", "b", 3.0),
            ("worker", "c", 5.0),
            ("worker", "d", 2.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "memory_bytes"),
                    ("job", job),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "topk by (job) (1, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("instance") == Some("b")
                && approx_eq(float_value(&sample.value), 3.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("worker")
                && sample.labels.get("instance") == Some("c")
                && approx_eq(float_value(&sample.value), 5.0)
        }));
    }

    #[tokio::test]
    async fn instant_bottomk_without_selects_smallest_sample_per_group_with_original_labels() {
        let mut store = InMemoryMetricStore::new();
        for (job, instance, value) in [
            ("api", "a", 4.0),
            ("api", "b", 1.0),
            ("worker", "c", 5.0),
            ("worker", "d", 2.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "memory_bytes"),
                    ("job", job),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "bottomk without (instance) (1, memory_bytes)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("instance") == Some("b")
                && approx_eq(float_value(&sample.value), 1.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("worker")
                && sample.labels.get("instance") == Some("d")
                && approx_eq(float_value(&sample.value), 2.0)
        }));
    }

    #[tokio::test]
    async fn instant_topk_and_bottomk_ignore_histograms() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
                10_000,
                value,
            );
        }
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", "hist")]),
            10_000,
            native_histogram(4.0, 10.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected_instance, expected_value) in [
            ("topk(1, memory_bytes)", "b", 3.0),
            ("bottomk(1, memory_bytes)", "a", 1.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();

            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert_eq!(samples.len(), 1, "{query}");
            assert_eq!(
                samples[0].labels.get("__name__"),
                Some("memory_bytes"),
                "{query}"
            );
            assert_eq!(
                samples[0].labels.get("instance"),
                Some(expected_instance),
                "{query}"
            );
            assert!(
                approx_eq(float_value(&samples[0].value), expected_value),
                "{query}"
            );
        }
    }

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    async fn instant_limit_ratio_requires_experimental_feature() {
        let store = InMemoryMetricStore::new();
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let error = engine
            .query_instant("tenant-a", "limit_ratio(0.5, memory_bytes)", 10_000)
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Unsupported(_)));
        assert!(format!("{error}").contains("experimental-functions"));
    }

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    async fn instant_limitk_requires_experimental_feature() {
        let store = InMemoryMetricStore::new();
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let error = engine
            .query_instant("tenant-a", "limitk(2, memory_bytes)", 10_000)
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Unsupported(_)));
        assert!(format!("{error}").contains("experimental-functions"));
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_limitk_selects_deterministic_hash_subset() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "memory_bytes"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "limitk(2, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let selected = sample_instances(&samples);
        assert!(selected == vec!["c", "e"]);
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_limitk_by_selects_deterministic_hash_subset_per_group() {
        let mut store = InMemoryMetricStore::new();
        for (job, instance, value) in [
            ("api", "a", 1.0),
            ("api", "b", 2.0),
            ("api", "c", 3.0),
            ("worker", "d", 4.0),
            ("worker", "e", 5.0),
            ("worker", "f", 6.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "memory_bytes"),
                    ("job", job),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "limitk by (job) (1, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("instance") == Some("c")
                && approx_eq(float_value(&sample.value), 3.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("memory_bytes")
                && sample.labels.get("job") == Some("worker")
                && sample.labels.get("instance") == Some("d")
                && approx_eq(float_value(&sample.value), 4.0)
        }));
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_limit_ratio_selects_deterministic_hash_subset() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "memory_bytes"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "limit_ratio(0.75, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let selected = sample_instances(&samples);
        assert!(selected == vec!["b", "c", "e"]);
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_limit_ratio_negative_selects_complement_subset() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "memory_bytes"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "limit_ratio(-0.25, memory_bytes)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let selected = sample_instances(&samples);
        assert!(selected == vec!["a", "d"]);
    }

    #[tokio::test]
    async fn instant_quantile_interpolates_per_group() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [1.0, 2.0, 4.0, 8.0].into_iter().enumerate() {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "latency_seconds"),
                    ("job", "api"),
                    ("instance", &instance.to_string()),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "quantile by (job) (0.5, latency_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 3.0));
    }

    #[tokio::test]
    async fn instant_quantile_aggregation_ignores_histograms() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 2.0), ("b", 6.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "latency_seconds"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", "hist"),
            ]),
            10_000,
            native_histogram(4.0, 10.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "quantile by (job) (0.5, latency_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("__name__"), None);
        assert_eq!(samples[0].labels.get("job"), Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 4.0));
    }

    #[tokio::test]
    async fn instant_min_max_and_std_aggregations_ignore_histograms() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            4.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "b"),
            ]),
            10_000,
            8.0,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "hist"),
            ]),
            10_000,
            native_histogram(4.0, 10.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            ("min by (job) (mixed_metric)", 4.0),
            ("max by (job) (mixed_metric)", 8.0),
            ("stddev by (job) (mixed_metric)", 2.0),
            ("stdvar by (job) (mixed_metric)", 4.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();

            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert_eq!(samples.len(), 1, "{query}");
            assert_eq!(samples[0].labels.get("__name__"), None, "{query}");
            assert_eq!(samples[0].labels.get("job"), Some("api"), "{query}");
            assert!(
                approx_eq(float_value(&samples[0].value), expected),
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn instant_count_and_group_aggregations_include_histograms() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "float"),
            ]),
            10_000,
            4.0,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "hist"),
            ]),
            10_000,
            native_histogram(4.0, 10.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            ("count by (job) (mixed_metric)", 2.0),
            ("group by (job) (mixed_metric)", 1.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();

            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert_eq!(samples.len(), 1, "{query}");
            assert_eq!(samples[0].labels.get("__name__"), None, "{query}");
            assert_eq!(samples[0].labels.get("job"), Some("api"), "{query}");
            assert!(
                approx_eq(float_value(&samples[0].value), expected),
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn instant_sum_and_avg_aggregations_combine_compatible_native_histograms() {
        let mut left = native_histogram(4.0, 10.0);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        left.positive_counts = vec![1.0, 2.0];
        let mut right = native_histogram(6.0, 20.0);
        right.zero_count = 2.0;
        right.positive_spans = left.positive_spans.clone();
        right.positive_counts = vec![2.0, 2.0];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            left,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", "b"),
            ]),
            10_000,
            right,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected_count, expected_sum, expected_avg) in [
            ("sum by (job) (request_duration_seconds)", 10.0, 30.0, 3.0),
            ("avg by (job) (request_duration_seconds)", 5.0, 15.0, 3.0),
        ] {
            let count = engine
                .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
                .await
                .unwrap();
            let sum = engine
                .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
                .await
                .unwrap();
            let avg = engine
                .query_instant("tenant-a", &format!("histogram_avg({query})"), 10_000)
                .await
                .unwrap();

            assert_single_float_sample(&count, "api", expected_count, query);
            assert_single_float_sample(&sum, "api", expected_sum, query);
            assert_single_float_sample(&avg, "api", expected_avg, query);
        }
    }

    #[tokio::test]
    async fn instant_sum_aggregation_combines_native_histograms_with_different_span_layouts() {
        let mut left = native_histogram(4.0, 10.0);
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![1.0];
        let mut right = native_histogram(6.0, 20.0);
        right.positive_spans = vec![BucketSpan {
            offset: 1,
            length: 1,
        }];
        right.positive_counts = vec![2.0];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            left,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", "b"),
            ]),
            10_000,
            right,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "sum by (job) (request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1);
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected histogram");
        };
        assert!(approx_eq(histogram.count, 10.0));
        assert!(approx_eq(histogram.sum, 30.0));
        assert_eq!(
            histogram.positive_spans,
            vec![BucketSpan {
                offset: 0,
                length: 2
            }]
        );
        assert_eq!(histogram.positive_counts, vec![1.0, 2.0]);
    }

    #[tokio::test]
    async fn instant_sum_and_avg_aggregations_omit_mixed_float_and_histogram_groups() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "float"),
            ]),
            10_000,
            4.0,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "api"),
                ("instance", "hist"),
            ]),
            10_000,
            native_histogram(4.0, 10.0),
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "mixed_metric"),
                ("job", "web"),
                ("instance", "float"),
            ]),
            10_000,
            6.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for query in ["sum by (job) (mixed_metric)", "avg by (job) (mixed_metric)"] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();

            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert_eq!(samples.len(), 1, "{query}");
            assert_eq!(samples[0].labels.get("job"), Some("web"), "{query}");
            assert!(approx_eq(float_value(&samples[0].value), 6.0), "{query}");
        }
    }

    #[tokio::test]
    async fn instant_sum_aggregation_rejects_incompatible_native_histograms() {
        let mut left = native_histogram(4.0, 10.0);
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![1.0];
        let mut right = native_histogram(6.0, 20.0);
        right.schema = 1;
        right.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        right.positive_counts = vec![2.0];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            left,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[
                ("__name__", "request_duration_seconds"),
                ("job", "api"),
                ("instance", "b"),
            ]),
            10_000,
            right,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let error = engine
            .query_instant(
                "tenant-a",
                "sum by (job) (request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Unsupported(_)));
        assert!(format!("{error}").contains("incompatible native histogram"));
    }

    #[tokio::test]
    async fn instant_histogram_quantile_interpolates_classic_buckets() {
        let mut store = InMemoryMetricStore::new();
        for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_request_duration_seconds_bucket"),
                    ("job", "api"),
                    ("le", le),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("le").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 0.25));
    }

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    async fn histogram_quantiles_requires_experimental_feature() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let error = engine
            .query_instant(
                "tenant-a",
                r#"histogram_quantiles(vector(1), "quantile", 0.5)"#,
                10_000,
            )
            .await
            .unwrap_err();

        assert!(format!("{error}").contains("experimental-functions"));
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn histogram_quantiles_emits_one_sample_per_requested_quantile() {
        let mut store = InMemoryMetricStore::new();
        for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_request_duration_seconds_bucket"),
                    ("job", "api"),
                    ("le", le),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"histogram_quantiles(http_request_duration_seconds_bucket, "quantile", 0.5, 0.9)"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        let values = samples
            .iter()
            .map(|sample| {
                (
                    sample.labels.get("quantile").expect("quantile label"),
                    float_value(&sample.value),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert!(approx_eq(*values.get("0.5").expect("p50 sample"), 0.25));
        assert!(approx_eq(*values.get("0.9").expect("p90 sample"), 0.37));
        assert!(samples.iter().all(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("le").is_none()
        }));
    }

    #[tokio::test]
    async fn instant_histogram_quantile_interpolates_native_histogram_buckets() {
        let mut histogram = native_histogram(4.0, 6.5);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            10_000,
            histogram,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_quantile(0.5, request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(
            float_value(&samples[0].value),
            2_f64.powf(1.0 / 3.0)
        ));
    }

    #[tokio::test]
    async fn instant_histogram_fraction_estimates_native_histogram_bucket_overlap() {
        let mut histogram = native_histogram(4.0, 6.5);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            10_000,
            histogram,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_fraction(1, 2, request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 0.75));
    }

    #[tokio::test]
    async fn instant_histogram_count_returns_native_histogram_count() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_count(request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 4.0));
    }

    #[tokio::test]
    async fn instant_histogram_sum_returns_native_histogram_sum() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_sum(request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 10.0));
    }

    #[tokio::test]
    async fn instant_histogram_avg_returns_native_histogram_average() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_avg(request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 2.5));
    }

    #[tokio::test]
    async fn native_histogram_scalar_arithmetic_scales_histograms() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        for (query, expected) in [
            ("histogram_count(request_duration_seconds * 2)", 8.0),
            ("histogram_sum(2 * request_duration_seconds)", 20.0),
            ("histogram_count(request_duration_seconds / 2)", 2.0),
            ("histogram_sum(request_duration_seconds / 2)", 5.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();

            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert_eq!(samples.len(), 1, "{query}");
            assert_eq!(samples[0].labels.get("__name__"), None, "{query}");
            assert_eq!(samples[0].labels.get("job"), Some("api"), "{query}");
            assert!(
                approx_eq(float_value(&samples[0].value), expected),
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn native_histogram_scalar_arithmetic_drops_invalid_operator_orders() {
        let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
        for query in [
            "histogram_count(2 / request_duration_seconds)",
            "histogram_count(request_duration_seconds + 2)",
        ] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();

            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.is_empty(), "{query}");
        }
    }

    #[tokio::test]
    async fn instant_histogram_stdvar_estimates_native_histogram_population_variance() {
        let mut histogram = native_histogram(4.0, 5.25);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            10_000,
            histogram,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_stdvar(request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(
            float_value(&samples[0].value),
            0.099_384_473_924_297_3
        ));
    }

    #[tokio::test]
    async fn instant_histogram_stddev_returns_native_histogram_population_stddev() {
        let mut histogram = native_histogram(4.0, 5.25);
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        histogram.positive_counts = vec![1.0, 3.0];
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            10_000,
            histogram,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "histogram_stddev(request_duration_seconds)",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(
            float_value(&samples[0].value),
            0.099_384_473_924_297_3_f64.sqrt()
        ));
    }

    #[tokio::test]
    async fn scalar_binary_arithmetic_returns_scalar() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "2 * 3", 10_000)
            .await
            .unwrap();
        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: 6.0
                }
        );
    }

    #[tokio::test]
    async fn scalar_binary_atan2_returns_angle_radians() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "1 atan2 1", 10_000)
            .await
            .unwrap();
        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: std::f64::consts::FRAC_PI_4,
                }
        );
    }

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    async fn scalar_max_of_min_of_require_experimental_feature() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        for query in ["max_of(1, 2)", "min_of(1, 2)"] {
            let error = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap_err();

            assert!(matches!(error, PromqlError::Unsupported(_)));
            assert!(format!("{error}").contains("experimental-functions"));
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn scalar_max_of_min_of_return_larger_and_smaller_scalar() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        for (query, expected) in [("max_of(1, 2)", 2.0), ("min_of(1, 2)", 1.0)] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            assert!(
                result
                    == QueryResult::Scalar {
                        ts_ms: 10_000,
                        value: expected,
                    }
            );
        }
    }

    #[tokio::test]
    async fn scalar_function_converts_single_sample_vector_and_nan_otherwise() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "single_value"), ("instance", "a")]),
            10_000,
            7.0,
        );
        for (instance, value) in [("a", 1.0), ("b", 2.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "multi_value"), ("instance", instance)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let single = engine
            .query_instant("tenant-a", "scalar(single_value)", 10_000)
            .await
            .unwrap();
        assert!(
            single
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: 7.0,
                }
        );

        for query in ["scalar(missing_metric)", "scalar(multi_value)"] {
            let result = engine
                .query_instant("tenant-a", query, 10_000)
                .await
                .unwrap();
            let QueryResult::Scalar { ts_ms, value } = result else {
                panic!("expected scalar");
            };
            assert!(ts_ms == 10_000);
            assert!(value.is_nan());
        }
    }

    #[tokio::test]
    async fn vector_function_converts_scalar_to_unlabeled_instant_vector() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "vector(2 * 3)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.is_empty());
        assert!(samples[0].ts_ms == 10_000);
        assert!(approx_eq(float_value(&samples[0].value), 6.0));
    }

    #[tokio::test]
    async fn vector_scalar_arithmetic_preserves_labels_and_drops_metric_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up * 2", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn vector_scalar_atan2_preserves_labels_and_drops_metric_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "y"), ("job", "api")]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "y atan2 0", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(
            float_value(&samples[0].value),
            std::f64::consts::FRAC_PI_2
        ));
    }

    #[tokio::test]
    async fn vector_vector_arithmetic_matches_on_labels() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "a"), ("x", "1")]),
            10_000,
            10.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "b"), ("x", "1")]),
            10_000,
            5.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "b"), ("x", "2")]),
            10_000,
            99.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "a + on (x) b", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("x") == Some("1"));
        assert!(approx_eq(float_value(&samples[0].value), 15.0));
    }

    #[tokio::test]
    async fn vector_vector_arithmetic_drops_metadata_labels() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "requests_total"),
                ("__type__", "counter"),
                ("__unit__", "requests"),
                ("instance", "a"),
            ]),
            10_000,
            10.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "requests_total"),
                ("__type__", "counter"),
                ("__unit__", "requests"),
                ("instance", "b"),
            ]),
            10_000,
            5.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "requests_total + 1", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__name__").is_none())
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__type__").is_none())
        );
        assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__unit__").is_none())
        );
    }

    #[tokio::test]
    async fn vector_vector_arithmetic_fill_uses_missing_side_values() {
        let mut store = InMemoryMetricStore::new();
        for (metric, instance, value) in [
            ("a", "matched", 10.0),
            ("a", "left-only", 7.0),
            ("b", "matched", 3.0),
            ("b", "right-only", 5.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", metric), ("instance", instance)]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "a + on (instance) fill(0) b", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let values = samples
            .iter()
            .map(|sample| {
                (
                    sample.labels.get("instance").expect("instance label"),
                    float_value(&sample.value),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(values.len(), 3);
        assert!(approx_eq(values["matched"], 13.0));
        assert!(approx_eq(values["left-only"], 7.0));
        assert!(approx_eq(values["right-only"], 5.0));
        assert!(samples.iter().all(|sample| {
            let label_names = sample
                .labels
                .iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>();
            sample.labels.get("__name__").is_none() && label_names == vec!["instance"]
        }));
    }

    #[tokio::test]
    async fn vector_vector_arithmetic_combines_compatible_native_histograms() {
        let mut left = native_histogram(4.0, 10.0);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![3.0];
        let mut right = native_histogram(2.0, 4.0);
        right.zero_count = 0.5;
        right.positive_spans = left.positive_spans.clone();
        right.positive_counts = vec![1.5];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "a"), ("job", "api"), ("x", "1")]),
            10_000,
            left,
        );
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "b"), ("job", "api"), ("x", "1")]),
            10_000,
            right,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected_count, expected_sum) in
            [("a + on (x) b", 6.0, 14.0), ("a - on (x) b", 2.0, 6.0)]
        {
            let count = engine
                .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
                .await
                .unwrap();
            let sum = engine
                .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
                .await
                .unwrap();

            assert_single_on_x_float_sample(&count, expected_count, query);
            assert_single_on_x_float_sample(&sum, expected_sum, query);
        }
    }

    #[tokio::test]
    async fn vector_vector_comparison_matches_native_histogram_equality() {
        let mut left = native_histogram(4.0, 10.0);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![3.0];
        let equal = left.clone();
        let mut different = left.clone();
        different.sum = 11.0;

        let mut store = InMemoryMetricStore::new();
        for (name, histogram) in [("a", left), ("b", equal), ("c", different)] {
            store.push_histogram(
                "tenant-a",
                labels(&[("__name__", name), ("job", "api"), ("x", "1")]),
                10_000,
                histogram,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let equal = engine
            .query_instant("tenant-a", "histogram_count(a == on (x) b)", 10_000)
            .await
            .unwrap();
        assert_single_float_sample(&equal, "api", 4.0, "a == b");

        let not_equal = engine
            .query_instant("tenant-a", "histogram_count(a != on (x) c)", 10_000)
            .await
            .unwrap();
        assert_single_float_sample(&not_equal, "api", 4.0, "a != c");

        let false_filter = engine
            .query_instant("tenant-a", "a == on (x) c", 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = false_filter else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());

        let bool_result = engine
            .query_instant("tenant-a", "a == bool on (x) c", 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = bool_result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job").is_none());
        assert!(samples[0].labels.get("x") == Some("1"));
        assert!(approx_eq(float_value(&samples[0].value), 0.0));

        let invalid = engine
            .query_instant("tenant-a", "a > bool on (x) b", 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = invalid else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn vector_vector_arithmetic_scales_native_histograms_with_matched_floats() {
        let mut histogram = native_histogram(4.0, 10.0);
        histogram.zero_count = 1.0;
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        histogram.positive_counts = vec![3.0];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "duration"), ("job", "api"), ("x", "1")]),
            10_000,
            histogram,
        );
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "factor"), ("job", "api"), ("x", "1")]),
            10_000,
            2.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected_count, expected_sum) in [
            ("duration * on (x) factor", 8.0, 20.0),
            ("factor * on (x) duration", 8.0, 20.0),
            ("duration / on (x) factor", 2.0, 5.0),
        ] {
            let count = engine
                .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
                .await
                .unwrap();
            let sum = engine
                .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
                .await
                .unwrap();

            assert_single_on_x_float_sample(&count, expected_count, query);
            assert_single_on_x_float_sample(&sum, expected_sum, query);
        }

        let invalid = engine
            .query_instant(
                "tenant-a",
                "histogram_count(factor / on (x) duration)",
                10_000,
            )
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = invalid else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn vector_vector_group_left_carries_labels_from_one_side() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 100.0), ("b", 50.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("region", "east"),
            ]),
            10_000,
            10.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "http_requests_total / on (job) group_left(region) target_info",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        for sample in samples {
            assert!(sample.labels.get("__name__").is_none());
            assert!(sample.labels.get("job") == Some("api"));
            assert!(sample.labels.get("region") == Some("east"));
        }
    }

    #[tokio::test]
    async fn vector_vector_group_left_fill_right_preserves_unmatched_many_side() {
        let mut store = InMemoryMetricStore::new();
        for (job, instance, value) in [
            ("api", "a", 100.0),
            ("api", "b", 50.0),
            ("worker", "c", 7.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", job),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("region", "east"),
            ]),
            10_000,
            10.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "http_requests_total + on (job) group_left(region) fill_right(0) target_info",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let values = samples
            .iter()
            .map(|sample| {
                (
                    sample.labels.get("instance").expect("instance label"),
                    (sample.labels.get("region"), float_value(&sample.value)),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(values.len(), 3);
        assert_eq!(values["a"].0, Some("east"));
        assert!(approx_eq(values["a"].1, 110.0));
        assert_eq!(values["b"].0, Some("east"));
        assert!(approx_eq(values["b"].1, 60.0));
        assert_eq!(values["c"].0, None);
        assert!(approx_eq(values["c"].1, 7.0));
    }

    #[tokio::test]
    async fn vector_vector_group_right_fill_left_preserves_unmatched_many_side() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "job_quota"),
                ("job", "api"),
                ("region", "east"),
            ]),
            10_000,
            10.0,
        );
        for (job, instance, value) in [
            ("api", "a", 100.0),
            ("api", "b", 50.0),
            ("worker", "c", 7.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", job),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "job_quota + on (job) group_right(region) fill_left(0) http_requests_total",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let values = samples
            .iter()
            .map(|sample| {
                (
                    sample.labels.get("instance").expect("instance label"),
                    (sample.labels.get("region"), float_value(&sample.value)),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(values.len(), 3);
        assert_eq!(values["a"].0, Some("east"));
        assert!(approx_eq(values["a"].1, 110.0));
        assert_eq!(values["b"].0, Some("east"));
        assert!(approx_eq(values["b"].1, 60.0));
        assert_eq!(values["c"].0, None);
        assert!(approx_eq(values["c"].1, 7.0));
    }

    #[tokio::test]
    async fn info_function_adds_target_info_data_labels_by_job_and_instance() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("instance", "a"),
                ("region", "east"),
                ("cluster", "prod"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "info(http_requests_total)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__") == Some("http_requests_total"));
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(samples[0].labels.get("instance") == Some("a"));
        assert!(samples[0].labels.get("region") == Some("east"));
        assert!(samples[0].labels.get("cluster") == Some("prod"));
        assert!(approx_eq(float_value(&samples[0].value), 7.0));
    }

    #[tokio::test]
    async fn info_function_uses_data_label_selector_to_filter_and_copy_labels() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("instance", "a"),
                ("region", "east"),
                ("cluster", "prod"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"info(http_requests_total, {region="east"})"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(
            samples[0].labels.get("__name__"),
            Some("http_requests_total")
        );
        assert_eq!(samples[0].labels.get("job"), Some("api"));
        assert_eq!(samples[0].labels.get("instance"), Some("a"));
        assert_eq!(samples[0].labels.get("region"), Some("east"));
        assert_eq!(samples[0].labels.get("cluster"), None);
        assert!(approx_eq(float_value(&samples[0].value), 7.0));
    }

    #[tokio::test]
    async fn info_function_drops_series_when_required_data_label_selector_does_not_match() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("instance", "a"),
                ("region", "east"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"info(http_requests_total, {region="west"})"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn info_function_keeps_base_label_when_info_label_overlaps() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
                ("region", "base"),
            ]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("instance", "a"),
                ("region", "info"),
                ("cluster", "prod"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "info(http_requests_total)", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("region"), Some("base"));
        assert_eq!(samples[0].labels.get("cluster"), Some("prod"));
    }

    #[tokio::test]
    async fn info_function_uses_named_info_metric_selector() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "build_info"),
                ("job", "api"),
                ("instance", "a"),
                ("version", "1.2.3"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"info(http_requests_total, {__name__="build_info"})"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("version"), Some("1.2.3"));
    }

    #[tokio::test]
    async fn info_function_merges_data_labels_from_multiple_info_metrics() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
            ]),
            10_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("job", "api"),
                ("instance", "a"),
                ("cluster", "prod"),
            ]),
            10_000,
            1.0,
        );
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "build_info"),
                ("job", "api"),
                ("instance", "a"),
                ("version", "1.2.3"),
            ]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"info(http_requests_total, {__name__=~".+_info"})"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("cluster"), Some("prod"));
        assert_eq!(samples[0].labels.get("version"), Some("1.2.3"));
    }

    #[tokio::test]
    async fn vector_vector_group_right_carries_labels_from_one_side() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_limit"),
                ("job", "api"),
                ("region", "east"),
            ]),
            10_000,
            100.0,
        );
        for (instance, value) in [("a", 10.0), ("b", 25.0)] {
            store.push_float(
                "tenant-a",
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", "api"),
                    ("instance", instance),
                ]),
                10_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "target_limit / on (job) group_right(region) http_requests_total",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 2);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("region") == Some("east")
                && sample.labels.get("instance") == Some("a")
                && approx_eq(float_value(&sample.value), 10.0)
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("region") == Some("east")
                && sample.labels.get("instance") == Some("b")
                && approx_eq(float_value(&sample.value), 4.0)
        }));
    }

    #[tokio::test]
    async fn comparison_bool_returns_one_or_zero() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "a"), ("x", "1")]),
            10_000,
            10.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "a > bool 0", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn comparison_without_bool_filters_false_samples() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "a"), ("x", "1")]),
            10_000,
            10.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "a > 100", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn vector_and_keeps_left_samples_with_matching_right_key() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up and on (instance) target_info", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__") == Some("up"));
        assert!(samples[0].labels.get("instance") == Some("b"));
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn vector_and_default_matching_ignores_metadata_labels() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "requests_total"),
                ("__type__", "counter"),
                ("__unit__", "requests"),
                ("instance", "a"),
            ]),
            10_000,
            10.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "(requests_total + 1) and requests_total",
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("__type__").is_none());
        assert!(samples[0].labels.get("__unit__").is_none());
        assert!(samples[0].labels.get("instance") == Some("a"));
        assert!(approx_eq(float_value(&samples[0].value), 11.0));
    }

    #[tokio::test]
    async fn vector_unless_keeps_left_samples_without_matching_right_key() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up unless on (instance) target_info", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__") == Some("up"));
        assert!(samples[0].labels.get("instance") == Some("a"));
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn vector_or_returns_left_union_unmatched_right_samples() {
        let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up or on (instance) target_info", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 3);
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("up")
                && sample.labels.get("instance") == Some("a")
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("up")
                && sample.labels.get("instance") == Some("b")
        }));
        assert!(samples.iter().any(|sample| {
            sample.labels.get("__name__") == Some("target_info")
                && sample.labels.get("instance") == Some("c")
                && sample.labels.get("region") == Some("east")
                && approx_eq(float_value(&sample.value), 30.0)
        }));
    }

    #[tokio::test]
    async fn instant_rate_extrapolates_counter_window() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 0.0),
            (60_000, 1.0),
            (120_000, 2.0),
            (180_000, 3.0),
            (240_000, 4.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "rate(http_requests_total[5m])", 300_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 5.0 / 300.0));
    }

    #[tokio::test]
    async fn range_rate_uses_each_step_as_window_end() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 0.0),
            (60_000, 1.0),
            (120_000, 2.0),
            (180_000, 3.0),
            (240_000, 4.0),
            (300_000, 5.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_range(
                "tenant-a",
                "rate(http_requests_total[5m])",
                240_000,
                300_000,
                60_000,
            )
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert!(series.len() == 1);
        assert!(series[0].samples.len() == 2);
        assert!(series[0].samples[0].0 == 240_000);
        assert!(approx_eq(float_value(&series[0].samples[0].1), 4.0 / 300.0));
        assert!(series[0].samples[1].0 == 300_000);
        assert!(approx_eq(float_value(&series[0].samples[1].1), 5.0 / 300.0));
    }

    #[tokio::test]
    async fn range_selector_at_start_and_end_use_query_bounds() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(60_000_i64, 1.0), (120_000, 2.0), (180_000, 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "up"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [("up @ start()", 1.0), ("up @ end()", 3.0)] {
            let result = engine
                .query_range("tenant-a", query, 60_000, 180_000, 60_000)
                .await
                .unwrap();

            let QueryResult::RangeMatrix(series) = result else {
                panic!("expected matrix");
            };
            assert!(series.len() == 1);
            assert!(series[0].samples.len() == 3);
            for (ts_ms, value) in &series[0].samples {
                assert!([60_000, 120_000, 180_000].contains(ts_ms));
                assert!(approx_eq(float_value(value), expected));
            }
        }
    }

    #[tokio::test]
    async fn instant_increase_corrects_counter_resets() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 1.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "increase(http_requests_total[2m])", 120_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_delta_is_gauge_delta_without_reset_correction() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(30_000_i64, 4.0), (60_000, 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "delta(temperature_celsius[1m])", 60_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(approx_eq(float_value(&samples[0].value), -2.0));
    }

    #[tokio::test]
    async fn instant_changes_counts_value_transitions_in_range() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 1.0),
            (60_000, 1.0),
            (120_000, 2.0),
            (180_000, 2.0),
            (240_000, 5.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "changes(queue_depth[4m])", 240_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_resets_counts_counter_decreases_in_range() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 0.0),
            (60_000, 5.0),
            (120_000, 1.0),
            (180_000, 4.0),
            (240_000, 2.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "resets(http_requests_total[4m])", 240_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_irate_uses_last_two_samples_per_second() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 0.0), (60_000, 1.0), (90_000, 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "irate(http_requests_total[2m])", 90_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(approx_eq(float_value(&samples[0].value), 2.0 / 30.0));
    }

    #[tokio::test]
    async fn instant_idelta_uses_last_two_samples_without_per_second_division() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 0.0), (60_000, 1.0), (90_000, 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "idelta(temperature_celsius[2m])", 90_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(approx_eq(float_value(&samples[0].value), 2.0));
    }

    #[tokio::test]
    async fn instant_deriv_returns_gauge_slope_per_second() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "deriv(temperature_celsius[2m])", 120_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 2.0 / 60.0));
    }

    #[tokio::test]
    async fn instant_predict_linear_extrapolates_gauge_series() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "disk_free_bytes"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "predict_linear(disk_free_bytes[2m], 60)",
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 7.0));
    }

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    async fn instant_double_exponential_smoothing_requires_experimental_feature() {
        let store = InMemoryMetricStore::new();
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let error = engine
            .query_instant(
                "tenant-a",
                "double_exponential_smoothing(gauge[5m], 0.5, 0.5)",
                120_000,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Unsupported(_)));
        assert!(format!("{error}").contains("experimental-functions"));
    }

    #[cfg(not(feature = "experimental-functions"))]
    #[tokio::test]
    async fn instant_duration_expression_helpers_require_experimental_feature() {
        let store = InMemoryMetricStore::new();
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        for query in ["range()", "step()", "start()", "end()"] {
            let error = engine
                .query_instant("tenant-a", query, 120_000)
                .await
                .unwrap_err();

            assert!(matches!(error, PromqlError::Unsupported(_)), "{query}");
            assert!(
                format!("{error}").contains("experimental-functions"),
                "{query}: {error}"
            );
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_duration_expression_helpers_return_zero() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

        for query in ["range()", "step()", "start()", "end()"] {
            let result = engine
                .query_instant("tenant-a", query, 120_000)
                .await
                .unwrap();

            assert!(
                result
                    == QueryResult::Scalar {
                        ts_ms: 120_000,
                        value: 0.0,
                    },
                "{query}"
            );
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn range_duration_expression_helpers_return_query_range_and_step_seconds() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

        for (query, expected) in [
            ("range()", 120.0),
            ("step()", 30.0),
            ("start()", 60.0),
            ("end()", 180.0),
        ] {
            let result = engine
                .query_range("tenant-a", query, 60_000, 180_000, 30_000)
                .await
                .unwrap();

            let QueryResult::RangeMatrix(series) = result else {
                panic!("expected matrix");
            };
            assert_eq!(series.len(), 1, "{query}");
            assert_eq!(series[0].labels.len(), 0, "{query}");
            assert_eq!(
                series[0]
                    .samples
                    .iter()
                    .map(|(_, value)| float_value(value))
                    .collect::<Vec<_>>(),
                vec![expected; 5],
                "{query}"
            );
        }
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_double_exponential_smoothing_smooths_gauge_series() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 3.0),
            (60_000, 6.0),
            (120_000, 12.0),
            (180_000, 21.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "double_exponential_smoothing(queue_depth[4m], 0.5, 0.5)",
                180_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 17.625));
    }

    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    async fn instant_double_exponential_smoothing_validates_factors() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 3.0), (60_000, 6.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let error = engine
            .query_instant(
                "tenant-a",
                "double_exponential_smoothing(queue_depth[2m], 1, 0.5)",
                60_000,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, PromqlError::Plan(_)));
        assert!(format!("{error}").contains("smoothing factor"));
    }

    #[tokio::test]
    async fn instant_basic_over_time_functions_reduce_range_samples() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (60_000, 3.0), (120_000, 5.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected, preserves_name) in [
            ("sum_over_time(queue_depth[2m])", 8.0, false),
            ("avg_over_time(queue_depth[2m])", 4.0, false),
            ("count_over_time(queue_depth[2m])", 2.0, false),
            ("min_over_time(queue_depth[2m])", 3.0, false),
            ("max_over_time(queue_depth[2m])", 5.0, false),
            ("first_over_time(queue_depth[2m])", 3.0, true),
            ("last_over_time(queue_depth[2m])", 5.0, true),
            ("present_over_time(queue_depth[2m])", 1.0, false),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 120_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1);
            if preserves_name {
                assert!(samples[0].labels.get("__name__") == Some("queue_depth"));
            } else {
                assert!(samples[0].labels.get("__name__").is_none());
            }
            assert!(samples[0].labels.get("job") == Some("api"));
            assert!(approx_eq(float_value(&samples[0].value), expected));
        }
    }

    #[tokio::test]
    async fn instant_count_and_present_over_time_include_native_histograms() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, sum) in [(60_000, 10.0), (120_000, 20.0)] {
            store.push_histogram(
                "tenant-a",
                labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
                ts_ms,
                native_histogram(4.0, sum),
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            ("count_over_time(request_duration_seconds[2m])", 2.0),
            ("present_over_time(request_duration_seconds[2m])", 1.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 120_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1, "{query}");
            assert!(samples[0].labels.get("__name__").is_none(), "{query}");
            assert!(samples[0].labels.get("job") == Some("api"), "{query}");
            assert!(
                approx_eq(float_value(&samples[0].value), expected),
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn instant_first_and_last_over_time_return_native_histograms() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, sum) in [(60_000, 10.0), (120_000, 20.0)] {
            store.push_histogram(
                "tenant-a",
                labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
                ts_ms,
                native_histogram(4.0, sum),
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            (
                "histogram_sum(first_over_time(request_duration_seconds[2m]))",
                10.0,
            ),
            (
                "histogram_sum(last_over_time(request_duration_seconds[2m]))",
                20.0,
            ),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 120_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1, "{query}");
            assert!(samples[0].labels.get("__name__").is_none(), "{query}");
            assert!(samples[0].labels.get("job") == Some("api"), "{query}");
            assert!(
                approx_eq(float_value(&samples[0].value), expected),
                "{query}"
            );
        }
    }

    #[tokio::test]
    async fn instant_ts_of_over_time_functions_return_sample_timestamps_seconds() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 10.0),
            (60_000, 3.0),
            (120_000, 7.0),
            (180_000, 3.0),
            (240_000, 11.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            ("ts_of_first_over_time(queue_depth[4m])", 60.0),
            ("ts_of_last_over_time(queue_depth[4m])", 240.0),
            ("ts_of_min_over_time(queue_depth[4m])", 180.0),
            ("ts_of_max_over_time(queue_depth[4m])", 240.0),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 240_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1);
            assert!(samples[0].labels.get("__name__").is_none());
            assert!(samples[0].labels.get("job") == Some("api"));
            assert!(approx_eq(float_value(&samples[0].value), expected));
        }
    }

    #[tokio::test]
    async fn instant_absent_returns_one_with_equality_matcher_labels_when_vector_is_empty() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"absent(up{job="worker",instance=~".*"})"#,
                10_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("worker"));
        assert!(samples[0].labels.get("instance").is_none());
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_absent_with_or_matchers_returns_unlabeled_absence_sample() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", r#"absent(up{job="api" or job="web"})"#, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.is_empty());
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_absent_over_time_returns_one_when_range_is_empty() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"absent_over_time(up{job="api"}[1m])"#,
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_absent_over_time_with_or_matchers_returns_unlabeled_absence_sample() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"absent_over_time(up{job="api" or job="web"}[1m])"#,
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.is_empty());
        assert!(approx_eq(float_value(&samples[0].value), 1.0));
    }

    #[tokio::test]
    async fn instant_absent_over_time_treats_native_histograms_as_present() {
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            90_000,
            native_histogram(4.0, 10.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                r#"absent_over_time(request_duration_seconds{job="api"}[1m])"#,
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.is_empty());
    }

    #[tokio::test]
    async fn instant_time_returns_evaluation_timestamp_seconds() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "time()", 123_456)
            .await
            .unwrap();

        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 123_456,
                    value: 123.456
                }
        );
    }

    #[tokio::test]
    async fn instant_timestamp_returns_sample_timestamp_seconds() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            60_000,
            1.0,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "timestamp(up)", 120_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(samples[0].ts_ms == 120_000);
        assert!(approx_eq(float_value(&samples[0].value), 60.0));
    }

    #[tokio::test]
    async fn instant_statistical_over_time_functions_reduce_range_samples() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [
            (0_i64, 2.0),
            (60_000, 4.0),
            (120_000, 4.0),
            (180_000, 4.0),
            (240_000, 5.0),
            (300_000, 5.0),
            (360_000, 7.0),
            (420_000, 9.0),
        ] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "latency_seconds"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        for (query, expected) in [
            ("stdvar_over_time(latency_seconds[8m])", 4.0),
            ("stddev_over_time(latency_seconds[8m])", 2.0),
            ("quantile_over_time(0.5, latency_seconds[8m])", 4.5),
            ("mad_over_time(latency_seconds[8m])", 0.5),
        ] {
            let result = engine
                .query_instant("tenant-a", query, 420_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector");
            };
            assert!(samples.len() == 1);
            assert!(samples[0].labels.get("__name__").is_none());
            assert!(samples[0].labels.get("job") == Some("api"));
            assert!(approx_eq(float_value(&samples[0].value), expected));
        }
    }

    #[tokio::test]
    async fn unary_minus_negates_scalar_expression() {
        let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "-(2 * 3)", 10_000)
            .await
            .unwrap();
        assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: -6.0
                }
        );
    }

    #[tokio::test]
    async fn unary_minus_negates_vector_values_and_drops_metric_name() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
            10_000,
            3.5,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "-temperature_celsius", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), -3.5));
    }

    #[tokio::test]
    async fn unary_minus_negates_native_histogram_values_and_marks_gauge() {
        let mut histogram = native_histogram(4.0, 10.0);
        histogram.reset_hint = ResetHint::No;
        histogram.zero_count = 1.0;
        histogram.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        histogram.positive_counts = vec![3.0];

        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
            10_000,
            histogram,
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "-request_duration_seconds", 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        let SampleValue::Histogram(histogram) = &samples[0].value else {
            panic!("expected histogram");
        };
        assert!(histogram.reset_hint == ResetHint::Gauge);
        assert!(approx_eq(histogram.count, -4.0));
        assert!(approx_eq(histogram.sum, -10.0));
        assert!(approx_eq(histogram.zero_count, -1.0));
        assert!(
            histogram
                .positive_counts
                .iter()
                .any(|count| approx_eq(*count, -3.0))
        );
    }

    #[tokio::test]
    async fn range_selector_returns_samples_in_each_step_window() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up")]),
            90_000,
            stale_nan(),
        );
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 180_000, 3.0);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_range("tenant-a", "up[2m]", 120_000, 180_000, 60_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert!(series.len() == 1);
        assert!(series[0].samples.len() == 3);
        assert!(series[0].samples[0].0 == 60_000);
        assert!(series[0].samples[2].0 == 180_000);
        assert!(series[0].samples.iter().all(|(_, value)| {
            let SampleValue::Float(value) = value else {
                return false;
            };
            value.to_bits() != stale_nan().to_bits()
        }));
    }

    #[tokio::test]
    async fn range_query_accepts_parenthesized_expression() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_range("tenant-a", "(up)", 0, 120_000, 60_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert!(series.len() == 1);
        assert!(series[0].samples.len() == 3);
        assert!(series[0].samples[0].0 == 0);
        assert!(approx_eq(float_value(&series[0].samples[0].1), 0.0));
        assert!(series[0].samples[1].0 == 60_000);
        assert!(approx_eq(float_value(&series[0].samples[1].1), 1.0));
        assert!(series[0].samples[2].0 == 120_000);
        assert!(approx_eq(float_value(&series[0].samples[2].1), 2.0));
    }

    #[tokio::test]
    async fn range_selector_offset_shifts_matrix_window_backwards() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 0, 0.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 60_000, 1.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 120_000, 2.0);
        store.push_float("tenant-a", labels(&[("__name__", "up")]), 180_000, 3.0);

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "up[2m] offset 1m", 180_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert!(series.len() == 1);
        assert!(series[0].samples.len() == 2);
        assert!(series[0].samples[0].0 == 60_000);
        assert!(approx_eq(float_value(&series[0].samples[0].1), 1.0));
        assert!(series[0].samples[1].0 == 120_000);
        assert!(approx_eq(float_value(&series[0].samples[1].1), 2.0));
    }

    #[tokio::test]
    async fn instant_subquery_evaluates_expression_at_explicit_steps() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "(queue_depth * 2)[2m:1m]", 120_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert!(series.len() == 1);
        assert!(series[0].labels.get("__name__").is_none());
        assert!(series[0].labels.get("job") == Some("api"));
        assert!(series[0].samples.len() == 3);
        assert!(series[0].samples[0].0 == 0);
        assert!(approx_eq(float_value(&series[0].samples[0].1), 2.0));
        assert!(series[0].samples[1].0 == 60_000);
        assert!(approx_eq(float_value(&series[0].samples[1].1), 4.0));
        assert!(series[0].samples[2].0 == 120_000);
        assert!(approx_eq(float_value(&series[0].samples[2].1), 6.0));
    }

    #[tokio::test]
    async fn instant_subquery_uses_global_eval_interval_when_step_is_omitted() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (30_000, 2.0), (60_000, 3.0), (90_000, 4.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(
            Arc::new(store),
            EngineOpts {
                eval_interval_ms: 30_000,
                ..EngineOpts::default()
            },
        );
        let result = engine
            .query_instant("tenant-a", "queue_depth[90s:]", 90_000)
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert!(series.len() == 1);
        assert!(series[0].samples.len() == 4);
        assert!(series[0].samples[0].0 == 0);
        assert!(series[0].samples[1].0 == 30_000);
        assert!(series[0].samples[2].0 == 60_000);
        assert!(series[0].samples[3].0 == 90_000);
    }

    #[tokio::test]
    async fn instant_subquery_aligns_start_to_step_grid() {
        let mut store = InMemoryMetricStore::new();
        for (index, value) in [
            1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0,
        ]
        .into_iter()
        .enumerate()
        {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "metric_total")]),
                i64::try_from(index).unwrap() * 7_000,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "rate(metric_total[1m500ms:10s])", 80_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(approx_eq(
            float_value(&samples[0].value),
            2.366_666_666_666_666_7
        ));
    }

    #[tokio::test]
    async fn instant_over_time_accepts_subquery_range_argument() {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in [(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)] {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                ts_ms,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant(
                "tenant-a",
                "avg_over_time((queue_depth * 2)[2m:1m])",
                120_000,
            )
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert!(samples.len() == 1);
        assert!(samples[0].labels.get("__name__").is_none());
        assert!(samples[0].labels.get("job") == Some("api"));
        assert!(approx_eq(float_value(&samples[0].value), 5.0));
    }

    /// Compare two `RangeMatrix` results for the range parity test, bit-exact on
    /// float sample values so a genuine NaN equals a genuine NaN (plain
    /// `PartialEq` would spuriously fail `NaN == NaN`). Series order, labelsets,
    /// per-step timestamps, and gaps must all match.
    fn range_matrices_match(left: &QueryResult, right: &QueryResult) -> bool {
        let (QueryResult::RangeMatrix(left), QueryResult::RangeMatrix(right)) = (left, right)
        else {
            return false;
        };
        if left.len() != right.len() {
            return false;
        }
        left.iter().zip(right.iter()).all(|(l, r)| {
            l.labels == r.labels
                && l.samples.len() == r.samples.len()
                && l.samples.iter().zip(r.samples.iter()).all(|(lp, rp)| {
                    lp.0 == rp.0
                        && match (&lp.1, &rp.1) {
                            (SampleValue::Float(a), SampleValue::Float(b)) => {
                                a.to_bits() == b.to_bits()
                            }
                            (a, b) => a == b,
                        }
                })
        })
    }

    /// Differential parity: a range query routed through the per-step operator
    /// planner must produce the byte-exact `RangeMatrix` the interpreter range
    /// path produces — same series (which appear, in what order), labelsets,
    /// per-step `(t, value)` points, gaps, and a scalar-over-range shape.
    /// Lock in which corpus-shaped range expressions route through the operator
    /// planner vs fall back to the interpreter, so the gate's coverage is
    /// explicit and regressions are caught.
    #[test]
    fn range_planner_gate_routes_expected_shapes() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        let routes = |query: &str| -> bool {
            let expr = parse_promql_with_duration_context(
                query,
                DurationExprContext::range(0, 120_000, 60_000),
            )
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            super::range_expr_routes_through_planner(probe)
        };

        // Plannable range shapes that now flow through the per-step operators.
        for query in [
            "rate(bar[30s])",
            "sum_over_time(bar[30s])",
            "requests * 2",
            "foo > 2 or bar",
            "abs(metric)",
            "sum by(job)(metric)",
            "label_replace(metric, \"l\", \"v\", \"\", \"\")",
            // Aggregations over a rate / `*_over_time` range call now route
            // through the planner: the UDF emits NULL (not a NaN sentinel) for a
            // no-value window, the aggregate planner drops those NULL rows before
            // grouping, and the aggregates skip NULL — matching the interpreter,
            // which omits no-value series before aggregating.
            "sum(rate(bar[30s]))",
            "avg by(job)(rate(bar[30s]))",
            "max without(path)(increase(bar[2m]))",
            "count(avg_over_time(bar[1m]))",
            // Parameterized aggregations over a plannable float inner now recurse
            // the inner vector and apply the shared interpreter routine per step
            // (a `Precomputed` result), so they route through the planner too.
            "topk(1, metric)",
            "bottomk(2, metric) by(job)",
            "quantile(0.9, metric)",
            "count_values(\"v\", metric)",
            "stddev by(job)(metric)",
            "stdvar(metric)",
            // A range/`*_over_time` call whose argument is a SUBQUERY now routes
            // through the planner: the subquery's sub-grid is evaluated per-step
            // through the recursive planner and the shared outer fold is applied.
            "avg_over_time(bar[5m:30s])",
            "rate(sum_over_time(bar[30s:10s])[2m:30s])",
            // A subquery whose inner is a unary negation now routes too:
            // `Expr::Unary` is planner-supported, so the subquery's structural
            // gate accepts it.
            "avg_over_time((-bar)[5m:30s])",
            // A param aggregation over a plannable subquery-range inner routes
            // through the planner too (the inner subquery is plannable).
            "topk(1, max_over_time(metric[5m:1m]))",
            // `sort_by_label` / `sort_by_label_desc` now route through the planner.
            "sort_by_label(metric, \"job\")",
            "sort_by_label_desc(metric, \"job\")",
            // The experimental `*_over_time` members route through the shared kernel.
            "mad_over_time(metric[5m])",
            "first_over_time(metric[5m])",
            "ts_of_max_over_time(metric[5m])",
            // `info(v [, selector])` routes through the planner (the input vector is
            // plannable; the join is the shared kernel).
            "info(metric)",
            "info(metric, {__name__=\"target_info\"})",
            // A bare top-level instant-vector selector now routes through the
            // planner: the interpreter range path is fixed to the left-OPEN
            // lookback, agreeing with the operator selector chain (and Prometheus).
            "metric",
            // A top-level scalar-typed expression now routes too: both range paths
            // fold an identical no-label scalar series per step, so the operator
            // driver matches the interpreter byte-for-byte.
            "42",
            "1 + 2",
            "time()",
            // A bare selector with `@ start()`/`@ end()` now routes too: the
            // per-step planner driver scopes the query's `[start, end]` bounds in
            // `AT_MODIFIER_BOUNDS`, and `plan_instant_selector` resolves those
            // modifiers to the range bounds, matching the interpreter's dedicated
            // `eval_vector_selector_over_steps`.
            "metric @ start()",
            "metric @ end()",
        ] {
            assert!(
                routes(query),
                "expected `{query}` to route through the planner"
            );
        }

        // A raw matrix selector is a range-vector shape owned by the interpreter's
        // dedicated matrix range path (not per-step plannable), so the gate keeps
        // it on the interpreter.
        assert!(
            !routes("bar[30s]"),
            "expected `bar[30s]` to stay on the interpreter"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn range_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        let mut store = InMemoryMetricStore::new();
        let stale_bits = stale_nan();
        // Two counters (for rate and sum-by-rate), grouped by `group`.
        for (lbls, samples) in [
            (
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", "api"),
                    ("group", "a"),
                ]),
                vec![
                    (0_i64, 0.0),
                    (60_000, 1.0),
                    (120_000, 3.0),
                    (180_000, 6.0),
                    (240_000, 10.0),
                    (300_000, 15.0),
                ],
            ),
            (
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", "db"),
                    ("group", "a"),
                ]),
                vec![
                    (0, 0.0),
                    (60_000, 2.0),
                    (120_000, 4.0),
                    (180_000, 8.0),
                    (240_000, 16.0),
                    (300_000, 32.0),
                ],
            ),
            (
                // group b: a full history so `rate` has a value at every step
                // (no no-value NaN sentinel), keeping the operator aggregate
                // over this rate parity-exact in the forced comparison below.
                labels(&[
                    ("__name__", "http_requests_total"),
                    ("job", "cache"),
                    ("group", "b"),
                ]),
                vec![
                    (0, 1.0),
                    (60_000, 5.0),
                    (120_000, 7.0),
                    (180_000, 9.0),
                    (240_000, 11.0),
                    (300_000, 20.0),
                ],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", lbls.clone(), ts, value);
            }
        }
        // A second counter family with a single-sample (no-value rate) series, to
        // exercise the SPARSE aggregate-over-rate parity (group b's only member is
        // no-value, so it is excluded from the group on both paths).
        for (lbls, samples) in [
            (
                labels(&[("__name__", "spotty_total"), ("job", "api"), ("group", "a")]),
                vec![
                    (0_i64, 0.0),
                    (60_000, 1.0),
                    (120_000, 2.0),
                    (180_000, 3.0),
                    (240_000, 4.0),
                    (300_000, 5.0),
                ],
            ),
            (
                // Only one in-window sample at each step's 2m window: rate has no
                // value -> the operator rate emits NULL (not a NaN sentinel), the
                // aggregate planner drops it before grouping, and group b collapses
                // to no row at those steps — matching the interpreter, which omits
                // the no-value series. This drives the SPARSE aggregate-over-rate
                // parity proof below.
                labels(&[("__name__", "spotty_total"), ("job", "db"), ("group", "b")]),
                vec![(180_000, 100.0)],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", lbls.clone(), ts, value);
            }
        }
        // A plain gauge for a bare-selector range and a binary op.
        for (ts, value) in [
            (0_i64, 2.0),
            (60_000, 4.0),
            (120_000, 8.0),
            (180_000, 16.0),
            (240_000, 32.0),
            (300_000, 64.0),
        ] {
            store.push_float(
                "t",
                labels(&[("__name__", "gauge"), ("job", "api")]),
                ts,
                value,
            );
        }
        // A series whose mid-range latest in-window sample is a stale-NaN marker
        // (the series must vanish for the steps that select it) and whose later
        // sample is a genuine NaN (kept as a NaN value).
        for (ts, value) in [
            (0_i64, 1.0),
            (60_000, stale_bits),
            (120_000, 3.0),
            (180_000, f64::NAN),
            (240_000, 5.0),
        ] {
            store.push_float(
                "t",
                labels(&[("__name__", "spotty"), ("job", "api")]),
                ts,
                value,
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let (start, end, step) = (0_i64, 300_000_i64, 60_000_i64);

        // Queries the production gate routes through the per-step operator
        // planner. For these, the gate must accept them and the planner-routed
        // public `query_range` must evaluate successfully; the byte-exact value
        // checks are pinned below (and across the conformance corpus).
        let planner_routed = [
            // A rate over a counter (per-step rate projection).
            "rate(http_requests_total[2m])",
            // A vector-scalar binary op.
            "gauge * 2",
            // A vector-vector binary op (one-to-one on job).
            "gauge + on(job) http_requests_total{job=\"api\"}",
            // A scalar-math call over a selector (preserves genuine NaN, gaps
            // the stale-marker steps).
            "abs(spotty - 10)",
            // A simple aggregate over a bare selector (no rate sentinel).
            "sum by(job)(gauge)",
            // Aggregation over a rate: the marquee fix. Every series is dense, so
            // no no-value NULL arises — pure parity with the interpreter.
            "sum by(group)(rate(http_requests_total[2m]))",
            // Aggregation over a rate where one group member is SPARSE (the
            // single-sample `spotty_total` series at job=db,group=b yields no rate
            // value across the early steps). The UDF emits NULL for those steps,
            // the aggregate planner drops them before grouping, and group b
            // collapses to no result row at the steps where its only member is
            // no-value — exactly as the interpreter omits the no-value series.
            // group a (dense) is unaffected. This is the headline divergence the
            // fix closes, proven byte-exact through the public range path.
            "sum by(group)(rate(spotty_total[2m]))",
            // Parameterized aggregations over a plannable inner now route through
            // the planner per step. `topk` selects original series each step (a
            // series can appear/disappear between steps, stitched by fingerprint);
            // `quantile`/`stddev` reduce per group per step. All must equal the
            // interpreter byte-for-byte across the step grid.
            "topk(2, rate(http_requests_total[2m]))",
            "quantile(0.5, gauge)",
            "stddev by(group)(rate(http_requests_total[2m]))",
            // A BARE top-level instant-vector selector now routes through the
            // planner: the interpreter range path is fixed to the left-OPEN
            // lookback, so the operator selector chain matches it (and Prometheus)
            // byte-for-byte, including the stale-marker gaps and genuine-NaN keep.
            "gauge",
            "spotty",
            // A top-level SCALAR-typed expression now routes too: both range paths
            // fold an identical no-label scalar series per step.
            "42",
            "time()",
            "1 + 2",
        ];
        for query in planner_routed {
            let expr = parse_promql_with_duration_context(
                query,
                DurationExprContext::range(start, end, step),
            )
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            assert!(
                super::range_expr_routes_through_planner(probe),
                "gate unexpectedly excludes `{query}` from the planner path"
            );
            // The public range path now routes these through the planner (the
            // only evaluation engine); it must evaluate without falling back.
            let planner = engine
                .query_range("t", query, start, end, step)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
            assert!(
                matches!(planner, QueryResult::RangeMatrix(_)),
                "range query `{query}` did not yield a matrix: {planner:?}"
            );
        }

        // The only top-level range shape the gate keeps on the interpreter is a
        // raw matrix selector / subquery (a range-vector shape owned by the
        // dedicated matrix/subquery range path, not the per-step instant
        // planner). Assert the gate excludes it.
        for query in ["http_requests_total[2m]"] {
            let expr = parse_promql_with_duration_context(
                query,
                DurationExprContext::range(start, end, step),
            )
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            assert!(
                !super::range_expr_routes_through_planner(probe),
                "gate unexpectedly routes `{query}` through the planner"
            );
        }

        // The headline fix, proven directly on the SPARSE aggregate-over-rate.
        // `sum by(group)(rate(spotty_total[2m]))` over the full `[0, 300000]`
        // grid: group b's only member is a single-sample series, so its rate is
        // no-value (NULL) at every step. The rate UDF emits NULL for those steps,
        // the aggregate planner drops them before grouping, and group b collapses
        // to NO result row at all — only group a (dense) survives. (Before the
        // fix the operator path leaked a spurious NaN group-b row here.)
        let QueryResult::RangeMatrix(sparse) = engine
            .eval_range_via_planner_forced(
                "t",
                "sum by(group)(rate(spotty_total[2m]))",
                start,
                end,
                step,
            )
            .await
            .unwrap()
        else {
            panic!("expected matrix for the sparse aggregate-over-rate");
        };
        let sparse_groups: Vec<Option<&str>> = sparse
            .iter()
            .map(|series| series.labels.get("group"))
            .collect();
        assert_eq!(
            sparse_groups,
            vec![Some("a")],
            "no-value group b must be excluded, leaving only group a: {sparse:?}"
        );

        // Pin the stale-vs-genuine-NaN semantics the scalar-math `spotty` parity
        // relies on, via the forced planner path on `abs(spotty - 10)`.
        let QueryResult::RangeMatrix(series) = engine
            .eval_range_via_planner_forced("t", "spotty", start, end, step)
            .await
            .unwrap()
        else {
            panic!("expected matrix for `spotty`");
        };
        assert_eq!(series.len(), 1, "spotty series missing");
        // Steps (ms) -> selected latest-in-window value, lookback 5m:
        //   0 -> 1.0; 60k -> stale (DROPPED, no point); 120k -> 3.0;
        //   180k -> NaN (kept); 240k -> 5.0; 300k -> 5.0 (240k still in window).
        let points = &series[0].samples;
        let times: Vec<i64> = points.iter().map(|(t, _)| *t).collect();
        assert_eq!(
            times,
            vec![0, 120_000, 180_000, 240_000, 300_000],
            "stale-marker step not gapped: {times:?}"
        );
        let nan_point = points
            .iter()
            .find(|(t, _)| *t == 180_000)
            .expect("180k point");
        let SampleValue::Float(nan_value) = nan_point.1 else {
            panic!("expected float at 180k");
        };
        assert!(nan_value.is_nan(), "genuine NaN not kept at 180k");
        assert!(
            !super::is_stale_nan(nan_value),
            "genuine NaN reported as stale at 180k"
        );
    }

    #[tokio::test]
    async fn instant_selector_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        // A small float-only store with multiple series, an empty-string-ish
        // label set, an offset-relevant history, a stale marker (job=db: its
        // latest in-window sample is a stale-NaN marker, so it must be DROPPED
        // on both paths), and a genuine-NaN series (job=nan: its latest
        // in-window sample is a genuine NaN, so it must be KEPT as a NaN value
        // on both paths).
        let mut store = InMemoryMetricStore::new();
        let stale_bits = stale_nan();
        for (lbls, ts, value) in [
            (labels(&[("__name__", "up"), ("job", "api")]), 0_i64, 1.0),
            (labels(&[("__name__", "up"), ("job", "api")]), 60_000, 2.0),
            (labels(&[("__name__", "up"), ("job", "api")]), 120_000, 3.0),
            (labels(&[("__name__", "up"), ("job", "db")]), 60_000, 9.0),
            (
                labels(&[("__name__", "up"), ("job", "db")]),
                120_000,
                stale_bits,
            ),
            // Genuine NaN as the latest in-window sample: kept as a NaN value.
            (labels(&[("__name__", "up"), ("job", "nan")]), 60_000, 5.0),
            (
                labels(&[("__name__", "up"), ("job", "nan")]),
                120_000,
                f64::NAN,
            ),
            (
                labels(&[("__name__", "down"), ("job", "api")]),
                120_000,
                7.0,
            ),
            (labels(&[("__name__", "lonely")]), 120_000, 42.0),
        ] {
            store.push_float("t", lbls, ts, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let selectors = [
            ("up", 120_000_i64),
            ("up{job=\"api\"}", 120_000),
            ("up{job=~\"a.*\"}", 120_000),
            ("up{job!=\"api\"}", 120_000),
            ("{__name__=~\".+\"}", 120_000),
            ("up offset 1m", 120_000),
            ("up @ 60", 120_000),
            ("up{job=\"missing\"}", 120_000),
            ("lonely", 120_000),
            // Genuine NaN must be kept (NaN value) on both paths.
            ("up{job=\"nan\"}", 120_000),
            // Stale-NaN marker must be dropped (empty result) on both paths.
            ("up{job=\"db\"}", 120_000),
        ];

        for (query, time_ms) in selectors {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let Expr::VectorSelector(selector) = expr else {
                panic!("`{query}` did not parse to a bare vector selector");
            };

            let interpreter = engine
                .eval_instant_selector("t", &selector, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
            let planner = engine
                .eval_instant_selector_via_planner("t", &selector, time_ms)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let interpreter = normalize(interpreter);
            let planner = normalize(planner);
            assert!(
                instant_samples_match(&interpreter, &planner),
                "planner/interpreter divergence for `{query}`: {interpreter:?} vs {planner:?}"
            );

            // Pin the staleness semantics the parity above relies on.
            if query == "up{job=\"nan\"}" {
                // Genuine NaN is kept as a NaN value (and is not a stale marker).
                assert_eq!(planner.len(), 1, "genuine NaN dropped for `{query}`");
                let value = float_value(&planner[0].value);
                assert!(value.is_nan(), "genuine NaN not kept for `{query}`");
                assert!(
                    !super::is_stale_nan(value),
                    "genuine NaN reported as stale for `{query}`"
                );
            }
            if query == "up{job=\"db\"}" {
                // Stale-NaN marker terminates the series: empty result.
                assert!(
                    planner.is_empty(),
                    "stale-NaN marker not dropped for `{query}`: {planner:?}"
                );
            }
        }
    }

    /// Differential parity for the present-but-empty-valued-label fix. A series
    /// carrying `__unit__=""` (label PRESENT, value empty) must stay DISTINCT
    /// from a series of the same name with `__unit__` ABSENT, all the way through
    /// the operator leaf — which now encodes absent as NULL and present-empty as
    /// `""`. The planner instant-selector and rate-range paths must therefore
    /// produce the byte-exact result the interpreter does (same series set, same
    /// labelsets, same per-series values), where they previously fell back.
    #[tokio::test]
    async fn empty_valued_label_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        let mut store = InMemoryMetricStore::new();
        // Three series sharing `__name__=m`, distinguished only by the presence
        // and value of `__unit__`:
        //   - job=a: `__unit__=""`  (PRESENT, empty value)
        //   - job=b: `__unit__="s"` (PRESENT, non-empty)
        //   - job=c: `__unit__` ABSENT
        // The fingerprints of a (present-empty) and c (absent) differ, so both
        // must survive selection as distinct series.
        for (lbls, samples) in [
            (
                labels(&[("__name__", "m"), ("job", "a"), ("__unit__", "")]),
                vec![(0_i64, 1.0), (60_000, 2.0), (120_000, 3.0)],
            ),
            (
                labels(&[("__name__", "m"), ("job", "b"), ("__unit__", "s")]),
                vec![(0, 10.0), (60_000, 20.0), (120_000, 30.0)],
            ),
            (
                labels(&[("__name__", "m"), ("job", "c")]),
                vec![(0, 100.0), (60_000, 200.0), (120_000, 300.0)],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", lbls.clone(), ts, value);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected instant vector");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        // (a) INSTANT selector path: the bare selector `m` matches all three
        // series. Planner (operator leaf) must equal the interpreter, preserving
        // the present-empty-vs-absent distinction.
        let time_ms = 120_000_i64;
        for query in ["m", "m{__unit__=\"\"}", "m{__unit__!=\"\"}"] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let Expr::VectorSelector(selector) = expr else {
                panic!("`{query}` did not parse to a bare vector selector");
            };
            let interpreter = normalize(
                engine
                    .eval_instant_selector("t", &selector, time_ms)
                    .await
                    .unwrap(),
            );
            let planner = normalize(
                engine
                    .eval_instant_selector_via_planner("t", &selector, time_ms)
                    .await
                    .unwrap(),
            );
            assert!(
                instant_samples_match(&interpreter, &planner),
                "instant planner/interpreter divergence for `{query}`: {interpreter:?} vs {planner:?}"
            );
            // The plannable gate must route the empty-valued selector through the
            // operator path now (no `Ok(None)` fallback).
            let routed = engine
                .plan_instant_expr("t", &Expr::VectorSelector(selector.clone()), time_ms)
                .await
                .unwrap();
            assert!(
                routed.is_some(),
                "selector `{query}` unexpectedly fell back to the interpreter"
            );
        }

        // The bare selector `m` must yield exactly three rows (a, b, c) — proving
        // the present-empty (a) and absent (c) series were not collapsed.
        let bare = normalize(engine.query_instant("t", "m", time_ms).await.unwrap());
        assert_eq!(bare.len(), 3, "present-empty and absent series collapsed");

        // (b) RANGE/matrix path: a rate over the empty-valued-label series must
        // also route through the operator leaf and keep the present-empty (a),
        // non-empty (b), and absent (c) series DISTINCT — three separate result
        // series, the present-empty/absent pair not collapsed.
        let (start, end, step) = (0_i64, 120_000_i64, 60_000_i64);
        let query = "rate(m[2m])";
        let QueryResult::RangeMatrix(mut series) = engine
            .query_range("t", query, start, end, step)
            .await
            .unwrap()
        else {
            panic!("expected matrix for `{query}`");
        };
        series.sort_by_key(|s| s.labels.fingerprint());
        assert_eq!(
            series.len(),
            3,
            "rate over present-empty/absent-label series collapsed: {series:?}"
        );
        // All three result series carry DISTINCT labelsets (the present-empty and
        // absent `__unit__` were not merged): distinct fingerprints.
        let fps: std::collections::BTreeSet<_> =
            series.iter().map(|s| s.labels.fingerprint()).collect();
        assert_eq!(
            fps.len(),
            3,
            "present-empty and absent series collapsed to the same labelset: {series:?}"
        );
    }

    /// Pin the corrected RANGE-path lookback boundary against Prometheus
    /// semantics: the instant-vector lookback window is `(eval - lookbackDelta,
    /// eval]` — left-OPEN, right-closed. A sample landing EXACTLY on the lower
    /// boundary (`ts == eval - lookbackDelta`) is EXCLUDED. Before the fix the
    /// interpreter range path used a left-CLOSED `>=`, spuriously including it;
    /// the operator path (and the interpreter's instant path) were already
    /// left-open and correct. This test proves:
    ///   1. the bare-selector RANGE query now routes through the planner,
    ///   2. planner == interpreter byte-for-byte across the grid, and
    ///   3. the boundary sample is excluded (the Prometheus-correct behaviour),
    ///      so a step whose only in-window candidate is the boundary sample has
    ///      NO point.
    #[tokio::test]
    async fn range_bare_selector_lookback_boundary_matches_prometheus() {
        let lookback = EngineOpts::default().lookback_delta_ms; // 300_000 (5m)

        let mut store = InMemoryMetricStore::new();
        // A single sample at t=0. With a 5m lookback:
        //   - step t=0:        window (−300000, 0], sample at 0 is in-window (right-closed) -> value.
        //   - step t=300000:   window (0, 300000], sample at 0 is EXACTLY on the
        //                      left boundary -> EXCLUDED (left-open) -> NO point.
        //   - step t=240000:   window (−60000, 240000], sample at 0 in-window -> value.
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("job", "boundary")]),
            0,
            7.0,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let (start, end, step) = (0_i64, lookback, 60_000_i64);

        // (1) the gate routes the bare selector through the planner.
        {
            use crate::DurationExprContext;
            use crate::parse_promql_with_duration_context;
            use promql_parser::parser::Expr;
            let expr = parse_promql_with_duration_context(
                "m",
                DurationExprContext::range(start, end, step),
            )
            .unwrap();
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            assert!(
                super::range_expr_routes_through_planner(probe),
                "bare selector range query should route through the planner"
            );
        }

        // (2) planner (public range path) yields the boundary-correct grid.
        let planner = engine
            .query_range("t", "m", start, end, step)
            .await
            .unwrap();

        // (3) the boundary step (t == eval - lookback) is excluded; the last
        // point is at t=240000, NOT at t=300000.
        let QueryResult::RangeMatrix(series) = &planner else {
            panic!("expected range matrix");
        };
        assert_eq!(series.len(), 1, "boundary series missing");
        let times: Vec<i64> = series[0].samples.iter().map(|(t, _)| *t).collect();
        assert_eq!(
            times,
            vec![0, 60_000, 120_000, 180_000, 240_000],
            "lookback boundary sample (t=300000 step) not excluded: {times:?}"
        );

        // (4) cross-check the interpreter's INSTANT path and the operator both
        // exclude the boundary sample directly, proving all three paths agree.
        let instant_at_boundary = engine.query_instant("t", "m", lookback).await.unwrap();
        let QueryResult::InstantVector(samples) = instant_at_boundary else {
            panic!("expected instant vector");
        };
        assert!(
            samples.is_empty(),
            "instant query at the lookback boundary must exclude the boundary sample: {samples:?}"
        );
    }

    /// Differential parity for a bare top-level selector carrying `@ start()` /
    /// `@ end()` in a RANGE query. The per-step planner range driver now scopes the
    /// query's `[start, end]` bounds, and `plan_instant_selector` resolves
    /// `@ start()`/`@ end()` to those bounds (a fixed eval instant repeated across
    /// every step) — exactly as the interpreter's dedicated
    /// `eval_vector_selector_over_steps`. This proves:
    ///   1. the gate routes `m @ start()` / `m @ end()` through the planner, and
    ///   2. planner (public range path) == interpreter byte-for-byte.
    #[tokio::test]
    async fn range_at_start_end_selector_planner_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        let mut store = InMemoryMetricStore::new();
        for (job, samples) in [
            ("a", vec![(0_i64, 1.0_f64), (120_000, 2.0), (300_000, 3.0)]),
            ("b", vec![(0, 10.0), (180_000, 20.0), (300_000, 30.0)]),
        ] {
            for (ts, value) in samples {
                store.push_float("t", labels(&[("__name__", "m"), ("job", job)]), ts, value);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let (start, end, step) = (0_i64, 300_000_i64, 60_000_i64);

        // `m @ start()` pins eval to t=0 and `m @ end()` to t=300000: both have
        // an in-window sample, so they yield series. `m @ start() offset 1m`
        // shifts the pinned eval back 60s to t=-60000, whose 5m window
        // (-360000, -60000] holds NO sample (the earliest is at t=0), so it
        // yields an EMPTY matrix — the Prometheus-correct result.
        for (query, expect_series) in [
            ("m @ start()", true),
            ("m @ end()", true),
            ("m @ start() offset 1m", false),
        ] {
            // (1) the gate routes the `@ start()/end()` selector through the planner.
            let expr = parse_promql_with_duration_context(
                query,
                DurationExprContext::range(start, end, step),
            )
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            assert!(
                super::range_expr_routes_through_planner(probe),
                "`{query}` should route through the planner"
            );

            // (2) the planner resolves `@ start()`/`@ end()` to a FIXED eval
            // instant repeated across every grid step, so each surviving series
            // carries the SAME value at every one of the 6 steps (the value it
            // had at the pinned eval instant), matching Prometheus.
            let QueryResult::RangeMatrix(series) = engine
                .query_range("t", query, start, end, step)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"))
            else {
                panic!("expected matrix for `{query}`");
            };
            assert_eq!(
                !series.is_empty(),
                expect_series,
                "`{query}` series presence mismatch: {series:?}"
            );
            for s in &series {
                let times: Vec<i64> = s.samples.iter().map(|(t, _)| *t).collect();
                assert_eq!(
                    times,
                    vec![0, 60_000, 120_000, 180_000, 240_000, 300_000],
                    "`{query}` series {:?} must have a point at every step (fixed @ eval): {times:?}",
                    s.labels
                );
                let values: Vec<u64> = s
                    .samples
                    .iter()
                    .map(|(_, v)| float_value(v).to_bits())
                    .collect();
                assert!(
                    values.windows(2).all(|w| w[0] == w[1]),
                    "`{query}` series {:?} value must be constant across steps (fixed @ eval): {:?}",
                    s.labels,
                    s.samples
                );
            }
        }

        // A bare `@ start()` selector in an INSTANT query has no range bounds, so it
        // must raise the SAME hard error on the planner path as the interpreter —
        // never silently produce a result or fall back.
        let instant_err = engine.query_instant("t", "m @ start()", 120_000).await;
        assert!(
            matches!(instant_err, Err(PromqlError::Unsupported(_))),
            "instant `m @ start()` must be a hard Unsupported error, got {instant_err:?}"
        );
    }

    /// Differential parity for the RESIDUAL range-vector folds the planner now
    /// routes through the shared interpreter dispatch (`plan_extended_range_fold_call`):
    /// `changes`/`resets`/`deriv` over a plain matrix selector (no operator-leaf
    /// UDF), and the `anchored`/`smoothed` extended-selector forms of
    /// `rate`/`increase`/`delta`/`changes`/`resets`. Each must plan to `Some` and
    /// match the interpreter's `eval_instant_expr` byte-for-byte.
    #[tokio::test]
    async fn extended_range_fold_planner_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();
        // A monotonic-ish counter with a reset, sampled every 30s through t=300000.
        for (job, samples) in [
            (
                "a",
                vec![
                    (0_i64, 0.0_f64),
                    (30_000, 5.0),
                    (60_000, 10.0),
                    (90_000, 4.0), // reset
                    (120_000, 9.0),
                    (150_000, 15.0),
                    (180_000, 21.0),
                    (210_000, 25.0),
                    (240_000, 30.0),
                    (270_000, 33.0),
                    (300_000, 40.0),
                ],
            ),
            (
                "b",
                vec![
                    (0, 100.0),
                    (60_000, 90.0),
                    (120_000, 80.0),
                    (180_000, 70.0),
                    (240_000, 60.0),
                    (300_000, 50.0),
                ],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", labels(&[("__name__", "ctr"), ("job", job)]), ts, value);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 300_000_i64;

        let queries = [
            // changes/resets/deriv over a plain matrix (no operator-leaf UDF).
            "changes(ctr[5m])",
            "resets(ctr[5m])",
            "deriv(ctr[5m])",
            "changes(ctr[2m])",
            "resets(ctr[2m])",
            // anchored/smoothed extended-selector folds.
            "rate(anchored(ctr[5m]))",
            "increase(anchored(ctr[5m]))",
            "delta(anchored(ctr[5m]))",
            "changes(anchored(ctr[5m]))",
            "resets(anchored(ctr[5m]))",
            "rate(smoothed(ctr[5m]))",
            "increase(smoothed(ctr[5m]))",
            "delta(smoothed(ctr[5m]))",
            // predict_linear over a plain matrix.
            "predict_linear(ctr[5m], 60)",
        ];

        for query in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // The planner must claim this query (Some, never None).
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };
            let via_operators = normalize(via_operators);
            let via_interpreter = normalize(via_interpreter);
            assert!(
                instant_samples_match(&via_operators, &via_interpreter),
                "extended-range-fold planner/interpreter divergence for `{query}`:\n  operator={via_operators:?}\n  interpreter={via_interpreter:?}"
            );
        }
    }

    /// Differential parity for a top-level SCALAR-typed RANGE query. A scalar
    /// expression (`time()`, `1 + 2`, an argless calendar form) now routes through
    /// the per-step planner driver, which folds an identical no-label scalar
    /// series per step. The result must be byte-exact with the interpreter's
    /// `eval_instant_expr_over_steps` scalar stitching.
    #[tokio::test]
    async fn range_scalar_expr_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        // A store with one series so calendar functions over `time()` have a
        // defined eval timeline; scalars ignore the series entirely.
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", labels(&[("__name__", "m"), ("job", "a")]), 0, 1.0);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let (start, end, step) = (0_i64, 300_000_i64, 60_000_i64);

        for query in ["42", "1 + 2", "time()", "2 * (3 + 4)"] {
            let expr = parse_promql_with_duration_context(
                query,
                DurationExprContext::range(start, end, step),
            )
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let mut probe = &expr;
            while let Expr::Paren(paren) = probe {
                probe = &paren.expr;
            }
            assert!(
                super::range_expr_routes_through_planner(probe),
                "gate unexpectedly excludes scalar `{query}` from the planner path"
            );
            let planner = engine
                .query_range("t", query, start, end, step)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
            // A scalar range query stitches a single no-label series, one float
            // point per step across the whole grid.
            let QueryResult::RangeMatrix(series) = &planner else {
                panic!("expected range matrix for `{query}`");
            };
            assert_eq!(series.len(), 1, "scalar `{query}` must yield one series");
            assert!(
                series[0].labels.is_empty(),
                "scalar `{query}` series must be unlabeled"
            );
            assert_eq!(
                series[0].samples.len(),
                6,
                "scalar `{query}` must have one point per grid step"
            );
            // The constant scalars fold to their exact value at every step.
            if let Some(expected) = match query {
                "42" => Some(42.0_f64),
                "1 + 2" => Some(3.0),
                "2 * (3 + 4)" => Some(14.0),
                _ => None,
            } {
                for (_, value) in &series[0].samples {
                    assert_eq!(
                        float_value(value).to_bits(),
                        expected.to_bits(),
                        "scalar `{query}` step value diverged"
                    );
                }
            }
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn rate_range_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;
        use promql_parser::parser::Expr;

        // Float-only counters with a reset, a gauge for delta, an offset
        // history, and a single-sample series (no rate value).
        let mut store = InMemoryMetricStore::new();
        for (lbls, samples) in [
            (
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                vec![
                    (0_i64, 0.0),
                    (60_000, 1.0),
                    (120_000, 2.0),
                    (180_000, 3.0),
                    (240_000, 4.0),
                    (300_000, 5.0),
                ],
            ),
            (
                // A counter reset mid-window (5 -> 1) exercises reset correction.
                labels(&[("__name__", "http_requests_total"), ("job", "db")]),
                vec![
                    (0, 0.0),
                    (60_000, 3.0),
                    (120_000, 5.0),
                    (180_000, 1.0),
                    (240_000, 4.0),
                    (300_000, 8.0),
                ],
            ),
            (
                // A gauge with ups and downs for delta/idelta.
                labels(&[("__name__", "temperature"), ("job", "api")]),
                vec![(180_000, 10.0), (240_000, 7.0), (300_000, 9.0)],
            ),
            (
                // Single sample in-window: rate-family yields no value. Both paths
                // must DROP this series identically (NULL-drop on the operator
                // path, no-value omission on the interpreter).
                labels(&[("__name__", "http_requests_total"), ("job", "lonely")]),
                vec![(295_000, 100.0)],
            ),
            (
                // A gauge whose window holds a GENUINE NaN sample: `delta` computes
                // a value (the window is non-empty with >=2 samples), and the
                // arithmetic yields NaN. That NaN is a real value (non-null), so it
                // must be KEPT and propagated on both paths — not dropped.
                labels(&[("__name__", "nan_gauge"), ("job", "api")]),
                vec![(240_000, f64::NAN), (300_000, 5.0)],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", lbls.clone(), ts, value);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            ("rate(http_requests_total[5m])", 300_000_i64),
            ("increase(http_requests_total[5m])", 300_000),
            ("delta(temperature[2m])", 300_000),
            ("irate(http_requests_total[5m])", 300_000),
            ("idelta(http_requests_total[5m])", 300_000),
            // @ and offset on the matrix selector exercise the time modifier.
            ("rate(http_requests_total[3m] @ 300)", 999_999),
            ("increase(http_requests_total[4m] offset 1m)", 360_000),
            // Tighter window that strands the single-sample series.
            ("rate(http_requests_total{job=\"api\"}[90s])", 300_000),
            // A genuine-NaN delta: the computed value is NaN but non-null, so the
            // series is KEPT (not dropped). Both paths must agree, NaN-aware.
            ("delta(nan_gauge[2m])", 300_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let Expr::Call(_) = &expr else {
                panic!("`{query}` did not parse to a call");
            };
            let (selector, kind) = match_rate_range_call(&expr)
                .unwrap_or_else(|| panic!("`{query}` is not an operator-path rate call"));

            let interpreter = engine
                .eval_instant_call(
                    "t",
                    match &expr {
                        Expr::Call(call) => call,
                        _ => unreachable!(),
                    },
                    time_ms,
                )
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
            let planner = engine
                .eval_rate_range_via_planner("t", selector, time_ms, kind)
                .await
                .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let interpreter = normalize(interpreter);
            let planner = normalize(planner);
            // NaN-aware comparison so a genuine NaN value (e.g. `delta(nan_gauge)`)
            // is treated as equal to itself across both paths rather than spuriously
            // failing under IEEE `NaN != NaN`.
            assert!(
                instant_samples_match(&interpreter, &planner),
                "planner/interpreter divergence for `{query}`: {interpreter:?} vs {planner:?}"
            );

            // Pin that the genuine-NaN delta is KEPT (non-null NaN value), not
            // dropped as if it were a no-value series.
            if query == "delta(nan_gauge[2m])" {
                assert_eq!(planner.len(), 1, "genuine-NaN delta series dropped");
                let value = float_value(&planner[0].value);
                assert!(
                    value.is_nan(),
                    "genuine NaN not kept through delta: {value}"
                );
            }
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn over_time_range_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A float-only store: a multi-sample gauge for the reductions, a second
        // labelset, a single-sample window edge case, and a stale marker that the
        // matrix path drops.
        let mut store = InMemoryMetricStore::new();
        let stale_bits = f64::from_bits(0x7ff0_0000_0000_0002);
        for (lbls, samples) in [
            (
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                vec![
                    (60_000_i64, 2.0),
                    (120_000, 4.0),
                    (180_000, 4.0),
                    (240_000, 5.0),
                    (300_000, 9.0),
                ],
            ),
            (
                labels(&[("__name__", "queue_depth"), ("job", "db")]),
                vec![(120_000, 3.0), (240_000, 7.0), (300_000, 1.0)],
            ),
            (
                // A stale marker mid-window: dropped by both paths.
                labels(&[("__name__", "queue_depth"), ("job", "stale")]),
                vec![(120_000, 5.0), (180_000, stale_bits), (300_000, 6.0)],
            ),
            (
                // Single in-window sample: rate yields no value, but over_time
                // reductions (avg/min/max/last/...) do.
                labels(&[("__name__", "queue_depth"), ("job", "lonely")]),
                vec![(295_000, 100.0)],
            ),
            // A `g`-grouped family for the SPARSE aggregate-over-over_time case at a
            // TIGHT `[30s]` window closing on t=300000 (window (270k, 300k]):
            //   g="mix": a member WITH an in-window sample (300k) -> has a value,
            //     plus a member whose only sample (120k) is outside the window ->
            //     no value (NULL). The no-value member is excluded, so the group
            //     survives with only the in-window member.
            //   g="allsparse": every member's only sample is outside the window,
            //     so the whole group is no-value and produces NO result row.
            (
                labels(&[("__name__", "depth_g"), ("g", "mix"), ("instance", "0")]),
                vec![(300_000, 5.0)],
            ),
            (
                labels(&[("__name__", "depth_g"), ("g", "mix"), ("instance", "1")]),
                vec![(120_000, 9.0)],
            ),
            (
                labels(&[
                    ("__name__", "depth_g"),
                    ("g", "allsparse"),
                    ("instance", "0"),
                ]),
                vec![(120_000, 1.0)],
            ),
            (
                labels(&[
                    ("__name__", "depth_g"),
                    ("g", "allsparse"),
                    ("instance", "1"),
                ]),
                vec![(120_000, 2.0)],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", lbls.clone(), ts, value);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            ("avg_over_time(queue_depth[5m])", 300_000_i64),
            ("sum_over_time(queue_depth[5m])", 300_000),
            ("count_over_time(queue_depth[5m])", 300_000),
            ("min_over_time(queue_depth[5m])", 300_000),
            ("max_over_time(queue_depth[5m])", 300_000),
            ("stddev_over_time(queue_depth[5m])", 300_000),
            ("stdvar_over_time(queue_depth[5m])", 300_000),
            // `last_over_time` preserves the metric name; every other family drops it.
            ("last_over_time(queue_depth[5m])", 300_000),
            ("present_over_time(queue_depth[5m])", 300_000),
            ("quantile_over_time(0.5, queue_depth[5m])", 300_000),
            ("quantile_over_time(0.9, queue_depth[5m])", 300_000),
            // @ and offset on the matrix selector exercise the time modifier.
            ("avg_over_time(queue_depth[3m] @ 300)", 999_999),
            ("sum_over_time(queue_depth[4m] offset 1m)", 360_000),
            // Tighter window that strands the single-sample series for some fns.
            ("min_over_time(queue_depth[90s])", 300_000),
            // EXPERIMENTAL over_time members now route through the shared-kernel
            // operator path. `first_over_time` preserves `__name__`; the `ts_of_*`
            // family returns the matching sample's timestamp in seconds.
            ("mad_over_time(queue_depth[5m])", 300_000),
            ("first_over_time(queue_depth[5m])", 300_000),
            ("ts_of_min_over_time(queue_depth[5m])", 300_000),
            ("ts_of_max_over_time(queue_depth[5m])", 300_000),
            ("ts_of_first_over_time(queue_depth[5m])", 300_000),
            ("ts_of_last_over_time(queue_depth[5m])", 300_000),
            // Experimental members composed under an aggregation also route.
            ("sum by (job) (mad_over_time(queue_depth[5m]))", 300_000),
            (
                "count by (job) (ts_of_max_over_time(queue_depth[5m]))",
                300_000,
            ),
            // @ / offset on an experimental member.
            ("first_over_time(queue_depth[3m] @ 300)", 999_999),
            // Aggregation over over_time: the compositional operator-path case.
            ("sum by (job) (avg_over_time(queue_depth[5m]))", 300_000),
            (
                "max without (job) (last_over_time(queue_depth[5m]))",
                300_000,
            ),
            // SPARSE aggregate-over-over_time: a group mixing an in-window member
            // with a no-value (stranded) member excludes the no-value member, and
            // an all-no-value group produces no result row. Every op must agree
            // with the interpreter.
            ("sum by (g) (avg_over_time(depth_g[30s]))", 300_000),
            ("count by (g) (avg_over_time(depth_g[30s]))", 300_000),
            ("min by (g) (max_over_time(depth_g[30s]))", 300_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            // NaN-aware comparison (a genuine NaN reduction equals itself).
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );

            // Pin the SPARSE aggregate-over-over_time rule: the no-value (stranded)
            // member is excluded from its group, and the all-no-value group is
            // absent.
            if matches!(
                query,
                "sum by (g) (avg_over_time(depth_g[30s]))"
                    | "count by (g) (avg_over_time(depth_g[30s]))"
                    | "min by (g) (max_over_time(depth_g[30s]))"
            ) {
                assert_eq!(
                    via_operators.len(),
                    1,
                    "`{query}`: only g=mix survives (g=allsparse absent)"
                );
                let mix = via_operators
                    .iter()
                    .find(|sample| sample.labels.get("g") == Some("mix"));
                assert!(mix.is_some(), "`{query}`: g=mix row missing");
                assert!(
                    via_operators
                        .iter()
                        .all(|sample| sample.labels.get("g") != Some("allsparse")),
                    "`{query}`: g=allsparse must be absent"
                );
                if query == "count by (g) (avg_over_time(depth_g[30s]))" {
                    assert!(
                        approx_eq(float_value(&mix.unwrap().value), 1.0),
                        "`{query}`: count over g=mix must be 1 (stranded member excluded)"
                    );
                }
            }
        }

        // The experimental over_time members (`mad`/`first`/`ts_of_*`) now route
        // through the shared-kernel operator path and are differentially checked in
        // the `queries` list above; pin that they are in fact claimed by the planner.
        for query in [
            "mad_over_time(queue_depth[5m])",
            "first_over_time(queue_depth[5m])",
            "ts_of_min_over_time(queue_depth[5m])",
            "ts_of_max_over_time(queue_depth[5m])",
            "ts_of_first_over_time(queue_depth[5m])",
            "ts_of_last_over_time(queue_depth[5m])",
        ] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let planned = engine
                .plan_instant_expr("t", &expr, 300_000)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
            assert!(
                planned.is_some(),
                "`{query}` must now route through the operator path"
            );
        }
    }

    /// Differential parity for **subqueries** routed through the recursive
    /// planner: a range/`*_over_time` call whose argument is `inner[range:res]`.
    /// The subquery's range vector is built per aligned sub-step through the
    /// planner and the shared outer fold is applied; the result must equal the
    /// interpreter's `eval_subquery` + outer fold byte-for-byte (NaN-aware).
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn subquery_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A float-only store exercising the subquery sub-grid:
        //  - `reqs_total{l}`: two counters (l=a, l=b) for rate/over_time-of-rate.
        //  - `gauge{l}`: a plain gauge in two label groups for the aggregating
        //    inner (`sum by(l)`).
        //  - `sparse{l}`: a series with a single early sample so a tight subquery
        //    window strands it (no-value sub-grid -> dropped series), plus a dense
        //    member so the surviving series is observable.
        let mut store = InMemoryMetricStore::new();
        // Counters: slope = factor over 60s, sampled every 30s out to 20m.
        for (l, factor) in [("a", 1.0), ("b", 2.0)] {
            let lbls = labels(&[("__name__", "reqs_total"), ("l", l)]);
            for step in 0..=40_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 30_000,
                    f64::from(step) * factor,
                );
            }
        }
        // Gauges in two groups (`sum by(l)` collapses the `g` member dimension).
        for (l, g, base) in [
            ("a", "0", 3.0),
            ("a", "1", 5.0),
            ("b", "0", 7.0),
            ("b", "1", 11.0),
        ] {
            let lbls = labels(&[("__name__", "gauge"), ("l", l), ("g", g)]);
            for step in 0..=40_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 30_000,
                    base + f64::from(step),
                );
            }
        }
        // Sparse: l=dense has a full history; l=stranded has only one early
        // sample, so a tight late subquery window yields it no sub-grid points.
        {
            let dense = labels(&[("__name__", "sparse"), ("l", "dense")]);
            for step in 0..=40_i32 {
                store.push_float(
                    "t",
                    dense.clone(),
                    i64::from(step) * 30_000,
                    f64::from(step),
                );
            }
            let stranded = labels(&[("__name__", "sparse"), ("l", "stranded")]);
            store.push_float("t", stranded, 0, 1.0);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Each query must route through the operator path and match the
        // interpreter byte-for-byte. `EngineOpts::default().eval_interval_ms` is
        // 60s, so a subquery written `[range:]` (no resolution) uses a 60s stride
        // on BOTH paths.
        let queries = [
            // Selector inner, explicit resolution.
            ("rate(reqs_total[5m:1m])", 1_200_000_i64),
            // Nested: `*_over_time` over a `rate(...)` subquery — the inner rate is
            // itself planned per sub-step.
            ("max_over_time(rate(reqs_total[1m])[10m:2m])", 1_200_000),
            // Aggregating inner with DEFAULT resolution (`[5m:]` -> 60s stride).
            ("sum_over_time((sum by(l)(gauge))[5m:])", 1_200_000),
            ("avg_over_time((sum by(l)(gauge))[5m:])", 1_200_000),
            // `@` and offset on the subquery shift the evaluated end (and the
            // step-aligned start) identically on both paths.
            ("sum_over_time(gauge[5m:1m] @ 600)", 1_200_000),
            ("sum_over_time(gauge[5m:1m] offset 5m)", 1_200_000),
            // Sparse: the stranded member yields an empty sub-grid window and is
            // dropped from the result; the dense member survives. A tight window
            // at a late time.
            ("sum_over_time(sparse[1m:30s])", 1_200_000),
            ("last_over_time(sparse[1m:30s])", 1_200_000),
            // Binary inner.
            ("rate((reqs_total + reqs_total)[5m:1m])", 1_200_000),
            // Unary-negation inner: `Expr::Unary` now routes through the planner,
            // so the subquery's structural gate accepts it and the inner negation
            // is planned per sub-step.
            ("sum_over_time((-gauge)[5m:1m])", 1_200_000),
            ("max_over_time((-gauge)[5m:1m])", 1_200_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );

            // Pin the sparse-window rule: the stranded member is dropped (no
            // sub-grid points), so only the dense series survives.
            if query == "sum_over_time(sparse[1m:30s])" {
                assert_eq!(
                    via_operators.len(),
                    1,
                    "`{query}`: only l=dense survives (l=stranded dropped)"
                );
                assert_eq!(
                    via_operators[0].labels.get("l"),
                    Some("dense"),
                    "`{query}`: surviving series must be l=dense"
                );
            }
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn scalar_math_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // NaN-aware sample comparison: labels and ts must match exactly; values
        // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
        fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
            if left.len() != right.len() {
                return false;
            }
            left.iter().zip(right).all(|(a, b)| {
                a.labels == b.labels
                    && a.ts_ms == b.ts_ms
                    && match (&a.value, &b.value) {
                        (SampleValue::Float(x), SampleValue::Float(y)) => {
                            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                        }
                        _ => false,
                    }
            })
        }

        // A float-only store: a multi-label gauge with negatives (for
        // `sqrt`/`ln` NaN/-inf edges), a genuine-NaN series (must survive the
        // operator path), an up-like series for the nested aggregate case, and a
        // counter for the nested rate case.
        let mut store = InMemoryMetricStore::new();
        for (lbls, ts, value) in [
            (labels(&[("__name__", "g"), ("l", "x")]), 60_000_i64, -3.0),
            (labels(&[("__name__", "g"), ("l", "y")]), 60_000, 20.0),
            (labels(&[("__name__", "g"), ("l", "z")]), 60_000, f64::NAN),
            (labels(&[("__name__", "up"), ("job", "api")]), 60_000, 1.0),
            (labels(&[("__name__", "up"), ("job", "db")]), 60_000, 1.0),
        ] {
            store.push_float("t", lbls, ts, value);
        }
        // A counter with a few samples for `abs(rate(...))`.
        let ctr = labels(&[("__name__", "c"), ("job", "api")]);
        for (ts, value) in [(0_i64, 0.0), (60_000, 30.0), (120_000, 90.0)] {
            store.push_float("t", ctr.clone(), ts, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Representative scalar-math queries over a plannable inner vector. Each
        // must route through the operator path and match the interpreter exactly,
        // including the genuine-NaN row and `sqrt(neg)`/`ln(neg)` -> NaN.
        let queries = [
            ("abs(g)", 60_000_i64),
            ("sqrt(g)", 60_000),
            ("ln(g)", 60_000),
            ("log2(g)", 60_000),
            ("sgn(g)", 60_000),
            ("ceil(g)", 60_000),
            ("floor(g)", 60_000),
            ("exp(g)", 60_000),
            ("sin(g)", 60_000),
            ("cos(g)", 60_000),
            ("atan(g)", 60_000),
            ("deg(g)", 60_000),
            ("rad(g)", 60_000),
            ("round(g)", 60_000),
            ("round(g, 5)", 60_000),
            ("clamp_min(g, 0)", 60_000),
            ("clamp_max(g, 10)", 60_000),
            ("clamp(g, 0, 10)", 60_000),
            // `min > max` yields the empty vector.
            ("clamp(g, 10, 0)", 60_000),
            // Nested compositional cases: scalar math over rate and over an
            // aggregate, both already on the operator path.
            ("abs(rate(c[5m]))", 120_000),
            ("ceil(sum by (job) (up))", 60_000),
            // Binary operands are now planner-supported, so scalar math over a
            // binary inner expression also routes through operators and must
            // match the interpreter (incl. the genuine-NaN row in `g`).
            ("abs(g + 1)", 60_000),
            // `atan2` is a binary operator returning a vector; it routes through
            // the binary planner path and must match the interpreter.
            ("g atan2 g", 60_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let interpreter = normalize(via_interpreter);
            let operators = normalize(via_operators);
            assert!(
                samples_match(&interpreter, &operators),
                "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
            );
        }

        // A bare matrix selector now routes through the planner as a
        // `RangeMatrix` result (covered by `matrix_subquery_planner_path_matches_
        // interpreter`), so it is no longer asserted as a scalar-math fall-back
        // here.
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn label_ops_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // NaN-aware sample comparison: labels and ts must match exactly; values
        // match when bit-equal or both NaN.
        fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
            if left.len() != right.len() {
                return false;
            }
            left.iter().zip(right).all(|(a, b)| {
                a.labels == b.labels
                    && a.ts_ms == b.ts_ms
                    && match (&a.value, &b.value) {
                        (SampleValue::Float(x), SampleValue::Float(y)) => {
                            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                        }
                        _ => false,
                    }
            })
        }

        // A float-only store: a multi-label gauge (with a `src` label for
        // capture-group expansion), a genuine-NaN series (must survive the
        // operator path and sort last), and an up-like metric for the nested
        // aggregate case.
        let mut store = InMemoryMetricStore::new();
        for (lbls, value) in [
            (
                labels(&[("__name__", "g"), ("l", "x"), ("src", "a-1")]),
                3.0,
            ),
            (
                labels(&[("__name__", "g"), ("l", "y"), ("src", "b-2")]),
                1.0,
            ),
            (
                labels(&[("__name__", "g"), ("l", "z"), ("src", "c-3")]),
                f64::NAN,
            ),
        ] {
            store.push_float("t", lbls, 60_000, value);
        }
        for (job, value) in [("api", 1.0), ("db", 1.0)] {
            store.push_float(
                "t",
                labels(&[("__name__", "up"), ("job", job)]),
                60_000,
                value,
            );
        }
        // Two `h` series differing only in label `a`; overwriting `a` to a
        // constant collapses them onto the same labelset (the collision case).
        for (a, value) in [("1", 10.0), ("2", 20.0)] {
            store.push_float(
                "t",
                labels(&[("__name__", "h"), ("a", a), ("b", "p")]),
                60_000,
                value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Representative label-ops queries over a plannable inner vector. Each
        // must route through the operator path and match the interpreter exactly,
        // covering: capture-group `$1` expansion, no-match passthrough,
        // delete-via-empty-replacement, multi-source label_join + separator,
        // sort/sort_desc (incl. the genuine-NaN row), and a nested aggregate.
        let queries = [
            // Capture group: `src="a-1"` -> `dst="a"`.
            (
                r#"label_replace(g, "dst", "$1", "src", "(.*)-.*")"#,
                60_000_i64,
            ),
            // No match (`src` has no digit-only-prefix form here): unchanged.
            (r#"label_replace(g, "dst", "$1", "src", "(\\d+)")"#, 60_000),
            // Empty replacement writes `dst=""` (the interpreter keeps it).
            (r#"label_replace(g, "dst", "", "src", ".*")"#, 60_000),
            // Replace the metric name itself (label_replace does not drop it).
            (
                r#"label_replace(g, "__name__", "renamed", "l", "(.+)")"#,
                60_000,
            ),
            // label_join: multi-source with a separator.
            (r#"label_join(g, "dst", "/", "l", "src")"#, 60_000),
            // label_join with a single source and empty separator.
            (r#"label_join(g, "dst", "", "l")"#, 60_000),
            // sort / sort_desc over a bare selector, including the NaN row
            // (which the NaN-preserving inner sourcing must keep and place last).
            ("sort(g)", 60_000),
            ("sort_desc(g)", 60_000),
            // Nested compositional case: sort over an aggregate (NaN-free `up`,
            // so the aggregate operator path matches the interpreter exactly).
            ("sort(sum by (job) (up))", 60_000),
            // label_replace over a nested aggregate (operator inner).
            (
                r#"label_replace(sum by (job) (up), "tag", "$1", "job", "(.+)")"#,
                60_000,
            ),
            // Binary operands are now planner-supported, so label-ops over a
            // binary inner expression route through operators and must match the
            // interpreter (note: `g + 1` drops `__name__`, so `l`/`src` survive).
            (r#"label_join(g + 1, "dst", "/", "l")"#, 60_000),
            ("sort(g + 1)", 60_000),
            // sort_by_label / sort_by_label_desc over a bare selector: order by the
            // `l` label values, then by remaining labels (the canonical key
            // tiebreak). Order-sensitive (the comparator below treats `sort*`
            // queries as ordered).
            (r#"sort_by_label(g, "l")"#, 60_000),
            (r#"sort_by_label_desc(g, "l")"#, 60_000),
            // Multi-label sort_by_label: tie on `l` would fall through to `src`.
            (r#"sort_by_label(g, "l", "src")"#, 60_000),
            // sort_by_label over a nested aggregate (operator inner).
            (r#"sort_by_label(sum by (job) (up), "job")"#, 60_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            // `sort`/`sort_desc` assert ordering, so compare order-sensitively for
            // them and fingerprint-normalize the unordered label-rewrites.
            let ordered = query.starts_with("sort");
            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                if !ordered {
                    samples.sort_by(|left, right| {
                        left.labels.fingerprint().cmp(&right.labels.fingerprint())
                    });
                }
                samples
            };

            let interpreter = normalize(via_interpreter);
            let operators = normalize(via_operators);
            assert!(
                samples_match(&interpreter, &operators),
                "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
            );
        }

        // A `label_replace` that collapses two series onto the same labelset must
        // error identically through the operator path and the interpreter. The
        // top-level uniqueness check enforces this for both (`query_instant`).
        let collision = r#"label_replace(h, "a", "same", "a", ".*")"#;
        let operator_err = engine
            .query_instant("t", collision, 60_000)
            .await
            .expect_err("collision must error through the operator path");
        assert!(matches!(operator_err, PromqlError::Exec(_)));
        // Confirm the operator path actually claimed the collision query (so the
        // error came from the operator path, not an interpreter fallback).
        let collision_expr =
            parse_promql_with_duration_context(collision, DurationExprContext::instant(60_000))
                .unwrap();
        assert!(
            engine
                .plan_instant_expr("t", &collision_expr, 60_000)
                .await
                .unwrap()
                .is_some(),
            "collision query must route through the planner"
        );

        // `sort_by_label` / `sort_by_label_desc` now route through the operator
        // path (differentially checked in the `queries` list above); pin that the
        // planner claims them and falls back on a missing label-name argument.
        for query in [r#"sort_by_label(g, "l")"#, r#"sort_by_label_desc(g, "l")"#] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(60_000))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let planned = engine
                .plan_instant_expr("t", &expr, 60_000)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"));
            assert!(
                planned.is_some(),
                "`{query}` must now route through the operator path"
            );
        }
        // `sort_by_label(g)` with no label-name argument falls back so the
        // interpreter raises the canonical arity error.
        let no_label = parse_promql_with_duration_context(
            "sort_by_label(g)",
            DurationExprContext::instant(60_000),
        )
        .unwrap();
        assert!(
            engine
                .plan_instant_expr("t", &no_label, 60_000)
                .await
                .unwrap()
                .is_none(),
            "`sort_by_label(g)` (no label arg) must fall back to the interpreter"
        );
    }

    /// Differential parity for `info(v [, data_label_selector])` routed through the
    /// recursive planner: the input vector is recursed, the `target_info` /
    /// custom-selector series are selected through the shared interpreter helper,
    /// and the shared `apply_info` join is applied. The result must equal the
    /// interpreter's `eval_info_call` byte-for-byte.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn info_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A store mirroring the conformance corpus: a base metric, a metric whose
        // identifying labels don't match any target_info, a metric with an
        // overlapping data label, plus `target_info` and `build_info` series.
        let mut store = InMemoryMetricStore::new();
        for (lbls, value) in [
            (
                labels(&[
                    ("__name__", "metric"),
                    ("instance", "a"),
                    ("job", "1"),
                    ("label", "value"),
                ]),
                2.0,
            ),
            (
                labels(&[
                    ("__name__", "metric_not_matching_target_info"),
                    ("instance", "a"),
                    ("job", "2"),
                    ("label", "value"),
                ]),
                2.0,
            ),
            (
                labels(&[
                    ("__name__", "metric_with_overlapping_label"),
                    ("instance", "a"),
                    ("job", "1"),
                    ("label", "value"),
                    ("data", "base"),
                ]),
                2.0,
            ),
            (
                labels(&[
                    ("__name__", "target_info"),
                    ("instance", "a"),
                    ("job", "1"),
                    ("data", "info"),
                    ("another_data", "another info"),
                ]),
                1.0,
            ),
            (
                labels(&[
                    ("__name__", "build_info"),
                    ("instance", "a"),
                    ("job", "1"),
                    ("build_data", "build"),
                ]),
                1.0,
            ),
        ] {
            store.push_float("t", lbls, 600_000, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Each query must route through the operator path and match the
        // interpreter exactly: default target_info enrichment, single/all-label
        // restriction, non-matching identifying labels (passthrough), a required
        // matcher not matching empty (drop), overlapping-label passthrough, and
        // explicit `__name__` selectors (target_info / build_info / both /
        // non-existent), plus the input-as-bare-selector form.
        let queries = [
            "info(metric)",
            r#"info(metric, {data=~".+"})"#,
            "info(metric_not_matching_target_info)",
            r#"info(metric, {non_existent=~".+"})"#,
            r#"info(metric, {data=~".+", non_existent=~".*"})"#,
            "info(metric_with_overlapping_label)",
            r#"info(metric, {__name__="target_info"})"#,
            r#"info(metric, {__name__="non_existent"})"#,
            r#"info(metric, {__name__="build_info"})"#,
            r#"info(metric, {__name__=~".+_info"})"#,
            r#"info(build_info, {__name__=~".+_info", build_data=~".+"})"#,
            // Input as a bare brace-only selector.
            r#"info({job="1"}, {__name__="target_info"})"#,
        ];

        for query in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(600_000))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, 600_000)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, 600_000)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, 600_000)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let interpreter = normalize(via_interpreter);
            let operators = normalize(via_operators);
            assert!(
                instant_samples_match(&interpreter, &operators),
                "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
            );
        }

        // A histogram info-series match errors identically (the info series must be
        // float-typed). Pin that the planner surfaces the same error class.
        let mut hist_store = InMemoryMetricStore::new();
        hist_store.push_float(
            "t",
            labels(&[
                ("__name__", "metric"),
                ("instance", "a"),
                ("job", "1"),
                ("label", "value"),
            ]),
            600_000,
            2.0,
        );
        hist_store.push_histogram(
            "t",
            labels(&[("__name__", "hist"), ("instance", "a"), ("job", "1")]),
            600_000,
            native_histogram(4.0, 10.0),
        );
        let hist_engine = PromqlEngine::new(Arc::new(hist_store), EngineOpts::default());
        let hist_query = r#"info(metric, {__name__="hist"})"#;
        let hist_expr =
            parse_promql_with_duration_context(hist_query, DurationExprContext::instant(600_000))
                .unwrap();
        let operator_result = hist_engine
            .plan_instant_expr("t", &hist_expr, 600_000)
            .await;
        assert!(
            matches!(operator_result, Err(PromqlError::Plan(_))),
            "histogram info series must error through the operator path"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn simple_aggregate_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A float-only multi-label store: two jobs across two groups, an
        // instance dimension for `without`, plus counters for the rate case.
        let mut store = InMemoryMetricStore::new();
        for (job, group, instance, value) in [
            ("api", "prod", "0", 1.0),
            ("api", "prod", "1", 2.0),
            ("api", "canary", "0", 4.0),
            ("db", "prod", "0", 8.0),
        ] {
            let lbls = labels(&[
                ("__name__", "http_requests"),
                ("job", job),
                ("group", group),
                ("instance", instance),
            ]);
            store.push_float("t", lbls, 120_000, value);
        }
        // A dedicated NaN metric exercising the post-fix selection semantics
        // through `sum`/`count`. instance=0 is a finite value, instance=1's
        // latest in-window sample is a GENUINE NaN (must be KEPT and flow into
        // the aggregate, so `sum(nan_metric)` is NaN and `count(nan_metric)` is
        // 2), and instance=2's latest in-window sample is a STALE-NaN marker
        // (must be DROPPED before aggregation, so it does not contribute to
        // `count`). Both paths must agree.
        for (instance, ts, value) in [
            ("0", 120_000_i64, 3.0),
            ("1", 60_000, 5.0),
            ("1", 120_000, f64::NAN),
            ("2", 60_000, 9.0),
            ("2", 120_000, stale_nan()),
        ] {
            let lbls = labels(&[
                ("__name__", "nan_metric"),
                ("job", "api"),
                ("instance", instance),
            ]);
            store.push_float("t", lbls, ts, value);
        }
        // A dedicated metric pinning the `min`/`max` NaN-ignoring rule. Group
        // g="mixed" holds genuine NaN alongside finite samples: Prometheus (and
        // the interpreter) take the extremum over the non-NaN values (NaN
        // ignored), so min=1, max=4. Group g="allnan" is entirely NaN:
        // Prometheus keeps the series with a NaN result (it is not dropped).
        // Arrow's built-in min/max instead order floats with total_cmp and
        // PROPAGATE NaN, so the operator path must use the NaN-ignoring UDAFs to
        // match the interpreter here.
        for (group, instance, value) in [
            ("mixed", "0", f64::NAN),
            ("mixed", "1", 4.0),
            ("mixed", "2", 1.0),
            ("mixed", "3", f64::NAN),
            ("allnan", "0", f64::NAN),
            ("allnan", "1", f64::NAN),
        ] {
            let lbls = labels(&[
                ("__name__", "minmax_nan"),
                ("g", group),
                ("instance", instance),
            ]);
            store.push_float("t", lbls, 120_000, value);
        }
        // Counters for `sum by (...) (rate(...))` (slope = step factor / 60s).
        for (job, path, factor) in [("api", "a", 1.0), ("api", "b", 2.0), ("db", "a", 5.0)] {
            let lbls = labels(&[("__name__", "reqs_total"), ("job", job), ("path", path)]);
            for step in 0..=3_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 60_000,
                    f64::from(step) * factor,
                );
            }
        }
        // Counters for the SPARSE aggregate-over-rate parity. The `g` label groups
        // members; at the 2m rate window closing on t=180_000:
        //   g="mix": one DENSE member (full history -> rate has a value) plus one
        //     SPARSE member (a single in-window sample -> rate is no-value). The
        //     no-value series must be excluded, so `sum by(g)(rate)` over g="mix"
        //     equals just the dense member's rate and `count by(g)(rate)` is 1.
        //   g="allsparse": every member is a single-sample (no-value) series, so
        //     the whole group collapses to NO result row (series absent), matching
        //     the interpreter, which forms no group when no sample reaches it.
        for (g, instance) in [
            ("mix", "dense"),
            ("mix", "sparse"),
            ("allsparse", "0"),
            ("allsparse", "1"),
        ] {
            let lbls = labels(&[
                ("__name__", "sparse_total"),
                ("g", g),
                ("instance", instance),
            ]);
            if instance == "dense" {
                // A full counter history: rate has a value at t=180_000.
                for step in 0..=3_i32 {
                    store.push_float(
                        "t",
                        lbls.clone(),
                        i64::from(step) * 60_000,
                        f64::from(step) * 7.0,
                    );
                }
            } else {
                // A single in-window sample: rate yields no value (NULL).
                store.push_float("t", lbls.clone(), 120_000, 100.0);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            ("sum by (group) (http_requests)", 120_000_i64),
            ("avg by (group) (http_requests)", 120_000),
            ("min by (group) (http_requests)", 120_000),
            ("max by (group) (http_requests)", 120_000),
            ("count by (group) (http_requests)", 120_000),
            ("group by (group) (http_requests)", 120_000),
            ("sum without (instance) (http_requests)", 120_000),
            ("sum without () (http_requests)", 120_000),
            ("sum by () (http_requests)", 120_000),
            ("sum(http_requests)", 120_000),
            ("sum by (job, nonexistent) (http_requests)", 120_000),
            ("sum(((http_requests)))", 120_000),
            // Empty-input aggregations must yield an empty vector (no global
            // group row), matching Prometheus and the interpreter.
            ("sum by () (does_not_exist)", 120_000),
            ("sum(does_not_exist)", 120_000),
            ("count by (group) (does_not_exist)", 120_000),
            // Aggregation over a rate call: the marquee operator-path case.
            // `sum by (l) (rate(x[range]))` mirrors the diff-corpus query
            // `sum by (method) (rate(http_requests_total[30s]))`.
            ("sum by (job) (rate(reqs_total[3m]))", 180_000),
            ("sum by (path) (rate(reqs_total[90s]))", 180_000),
            ("max without (path) (rate(reqs_total[3m]))", 180_000),
            // SPARSE aggregate-over-rate (the headline divergence the fix closes):
            // a group mixing a dense rate with a no-value (sparse) rate must
            // exclude the no-value series, and an all-no-value group must produce
            // no result row. Every simple op must agree with the interpreter.
            ("sum by (g) (rate(sparse_total[2m]))", 180_000),
            ("avg by (g) (rate(sparse_total[2m]))", 180_000),
            ("min by (g) (rate(sparse_total[2m]))", 180_000),
            ("max by (g) (rate(sparse_total[2m]))", 180_000),
            ("count by (g) (rate(sparse_total[2m]))", 180_000),
            ("group by (g) (rate(sparse_total[2m]))", 180_000),
            // No grouping: the global aggregate is over the single dense rate
            // (every sparse series is no-value and excluded). One result row.
            ("sum (rate(sparse_total[2m]))", 180_000),
            ("count (rate(sparse_total[2m]))", 180_000),
            // The same fix on `*_over_time`: avg_over_time has a value for the
            // single-sample sparse members too, but a TIGHT window can strand
            // them. Use a window narrow enough that the sparse members fall
            // outside it at t=180000 while the dense member still reduces.
            ("count by (g) (avg_over_time(sparse_total[30s]))", 180_000),
            // Genuine NaN flows into the aggregate (sum -> NaN), and the
            // stale-NaN marker is dropped before counting (count -> 2).
            ("sum(nan_metric)", 120_000),
            ("count(nan_metric)", 120_000),
            // NaN-ignoring min/max: the "mixed" group's extremum is over its
            // non-NaN samples (min=1, max=4); the "allnan" group keeps the
            // series with a NaN result. The operator path (NaN-ignoring UDAFs)
            // must match the interpreter bit-for-bit on every group, including
            // the all-NaN -> NaN case (a plain `value != value` filter would
            // instead drop the all-NaN series).
            ("min by (g) (minmax_nan)", 120_000),
            ("max by (g) (minmax_nan)", 120_000),
            ("min(minmax_nan)", 120_000),
            ("max(minmax_nan)", 120_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );

            // Pin the staleness semantics through the aggregate: the genuine NaN
            // in `nan_metric` is kept (so `sum(nan_metric)` is NaN), and the
            // stale-NaN marker is dropped before counting (so `count(nan_metric)`
            // is 2, not 3).
            assert_aggregate_nan_staleness(query, &via_operators);
            // Pin the NaN-ignoring min/max rule on absolute values (not just
            // operator==interpreter): the mixed group's extremum is over its
            // non-NaN samples, and the all-NaN group is kept with a NaN result.
            assert_minmax_nan_ignoring(query, &via_operators);
            // Pin the SPARSE aggregate-over-rate rule on absolute values: the
            // no-value (sparse) series is excluded from its group, and an
            // all-no-value group produces no result row at all.
            assert_sparse_aggregate_excludes_no_value(query, &via_operators);
        }
    }

    /// Differential parity for a simple aggregation whose inner bare selector has
    /// a genuine (non-stale) NaN series alone in its own `by` group — the exact
    /// shape the operator path must NOT drop. `sum(nan_metric)` (collapsed) already
    /// pins genuine-NaN propagation, but a genuine NaN ALONE in a distinct group is
    /// the case where a NaN-dropping selector would silently omit a whole group row
    /// rather than emit it with value NaN; this test pins that across all six simple
    /// ops, `by`/`without`, and a stale group (dropped) and a mixed NaN+finite group
    /// (NaN ignored by min/max), comparing the operator path against the interpreter
    /// and asserting the absolute Prometheus outcomes.
    #[tokio::test]
    #[allow(clippy::too_many_lines, clippy::type_complexity)]
    async fn aggregate_genuine_nan_group_parity() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();
        // `g` exercises every NaN/stale shape across DISTINCT `by (l)` groups:
        //   l=a: normal finite            -> group value 1.0
        //   l=b: a LONE genuine NaN       -> group KEPT with value NaN
        //   l=c: normal finite            -> group value 3.0
        //   l=d: latest is a STALE marker -> series dropped -> group ABSENT
        //   l=e: a group MIXING genuine NaN with finite {NaN, 2.0, 5.0}
        //        -> sum/avg NaN; min=2/max=5 (NaN ignored); count=3; group=1
        for (l, instance, ts, value) in [
            ("a", "0", 120_000_i64, 1.0_f64),
            ("b", "0", 120_000, f64::NAN),
            ("c", "0", 120_000, 3.0),
            ("d", "0", 60_000, 7.0),
            ("d", "0", 120_000, stale_nan()),
            ("e", "0", 120_000, f64::NAN),
            ("e", "1", 120_000, 2.0),
            ("e", "2", 120_000, 5.0),
        ] {
            let lbls = labels(&[("__name__", "g"), ("l", l), ("instance", instance)]);
            store.push_float("t", lbls, ts, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Each op must (a) route through the planner, (b) match the interpreter
        // byte-for-byte (NaN-aware), and (c) hit the documented absolute outcome.
        // `expect` maps l -> Some(value) for present groups; l=d is always absent.
        let cases: &[(&str, &[(&str, Option<f64>)])] = &[
            (
                "sum by (l) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(f64::NAN)),
                    ("c", Some(3.0)),
                    ("e", Some(f64::NAN)),
                ],
            ),
            (
                "avg by (l) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(f64::NAN)),
                    ("c", Some(3.0)),
                    ("e", Some(f64::NAN)),
                ],
            ),
            (
                "min by (l) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(f64::NAN)),
                    ("c", Some(3.0)),
                    ("e", Some(2.0)),
                ],
            ),
            (
                "max by (l) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(f64::NAN)),
                    ("c", Some(3.0)),
                    ("e", Some(5.0)),
                ],
            ),
            (
                "count by (l) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(1.0)),
                    ("c", Some(1.0)),
                    ("e", Some(3.0)),
                ],
            ),
            (
                "group by (l) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(1.0)),
                    ("c", Some(1.0)),
                    ("e", Some(1.0)),
                ],
            ),
            // `without (instance)` groups by `l` (and drops `__name__`): same shape.
            (
                "sum without (instance) (g)",
                &[
                    ("a", Some(1.0)),
                    ("b", Some(f64::NAN)),
                    ("c", Some(3.0)),
                    ("e", Some(f64::NAN)),
                ],
            ),
        ];

        for (query, expect) in cases {
            let time_ms = 120_000_i64;
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
            let norm = |r: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut s) = r else {
                    panic!("expected vector for `{query}`");
                };
                s.sort_by_key(|item| item.labels.fingerprint());
                s
            };
            let oper = norm(via_operators);
            let interp = norm(via_interpreter);
            assert!(
                instant_samples_match(&interp, &oper),
                "planner/interpreter divergence for `{query}`: {interp:?} vs {oper:?}"
            );
            // The stale group `l=d` is always absent on both paths.
            assert!(
                !oper.iter().any(|s| s.labels.get("l") == Some("d")),
                "`{query}`: stale group l=d must be absent, got {oper:?}"
            );
            // Absolute Prometheus outcome per group.
            for (l, want) in *expect {
                let got = oper.iter().find(|s| s.labels.get("l") == Some(*l));
                match want {
                    Some(value) => {
                        let sample = got.unwrap_or_else(|| {
                            panic!("`{query}`: group l={l} missing in {oper:?}")
                        });
                        let got_value = float_value(&sample.value);
                        if value.is_nan() {
                            assert!(
                                got_value.is_nan() && !super::is_stale_nan(got_value),
                                "`{query}`: l={l} want genuine NaN, got {got_value}"
                            );
                        } else {
                            assert!(
                                approx_eq(got_value, *value),
                                "`{query}`: l={l} want {value}, got {got_value}"
                            );
                        }
                    }
                    None => assert!(got.is_none(), "`{query}`: l={l} must be absent"),
                }
            }
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn param_aggregate_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A float-only store exercising the parameterized aggregations:
        //  - `m{job,instance}`: a multi-instance gauge per job, with a TIE between
        //    two instances (api/0 and api/1 both 5.0) so topk/bottomk tie-breaks
        //    (by `labels_key`) are observable, plus a genuine NaN member to pin
        //    NaN ordering (`total_cmp`) and quantile/stddev NaN handling.
        //  - `single{instance}`: a one-member group (single-element quantile and
        //    stddev/stdvar -> 0).
        //  - `cv{instance}`: a metric with repeated values for `count_values`
        //    (two members share value 1, one is 2).
        //  - `reqs_total`: counters for the nested `topk(.., rate(...))` case.
        let mut store = InMemoryMetricStore::new();
        for (job, instance, value) in [
            ("api", "0", 5.0),
            ("api", "1", 5.0), // ties with api/0 under topk/bottomk
            ("api", "2", 2.0),
            ("api", "3", 8.0),
            ("api", "4", f64::NAN), // genuine NaN member
            ("db", "0", 1.0),
            ("db", "1", 9.0),
        ] {
            let lbls = labels(&[("__name__", "m"), ("job", job), ("instance", instance)]);
            store.push_float("t", lbls, 120_000, value);
        }
        // A single-member group per job (single-element quantile/stddev/stdvar).
        for (job, value) in [("api", 4.0), ("db", 7.0)] {
            let lbls = labels(&[("__name__", "single"), ("job", job)]);
            store.push_float("t", lbls, 120_000, value);
        }
        // Repeated values for count_values: 1, 1, 2 within job=api.
        for (instance, value) in [("0", 1.0), ("1", 1.0), ("2", 2.0)] {
            let lbls = labels(&[("__name__", "cv"), ("job", "api"), ("instance", instance)]);
            store.push_float("t", lbls, 120_000, value);
        }
        // Counters for `topk(.., rate(...))` (slope = factor / 60s).
        for (path, factor) in [("a", 1.0), ("b", 2.0), ("c", 5.0)] {
            let lbls = labels(&[("__name__", "reqs_total"), ("job", "api"), ("path", path)]);
            for step in 0..=3_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 60_000,
                    f64::from(step) * factor,
                );
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Each query must route through the operator path and match the
        // interpreter byte-for-byte (NaN-aware, bit-exact).
        let queries = [
            // topk/bottomk: original series kept (labels incl. __name__), tie-break
            // by labels_key, k clamping (k > group size, k = 0). by and without.
            ("topk(2, m)", 120_000_i64),
            ("bottomk(2, m)", 120_000),
            ("topk(2, m) by (job)", 120_000),
            ("bottomk(2, m) by (job)", 120_000),
            ("topk(2, m) without (instance)", 120_000),
            // k larger than a group's size clamps to the whole group.
            ("topk(10, m) by (job)", 120_000),
            // k = 0 yields the empty vector.
            ("topk(0, m)", 120_000),
            ("bottomk(0, m) by (job)", 120_000),
            // Ties across the whole vector: api/0 and api/1 both 5.0.
            ("topk(3, m)", 120_000),
            // quantile: phi = 0 / 0.5 / 0.9 / 1, by and without.
            ("quantile(0, m) by (job)", 120_000),
            ("quantile(0.5, m) by (job)", 120_000),
            ("quantile(0.9, m) by (job)", 120_000),
            ("quantile(1, m) by (job)", 120_000),
            ("quantile(0.5, m) without (instance)", 120_000),
            // Single-element group: quantile equals the lone value.
            ("quantile(0.5, single) by (job)", 120_000),
            // count_values: one series per distinct value, value -> label, count.
            (r#"count_values("v", cv)"#, 120_000),
            (r#"count_values("v", cv) by (job)"#, 120_000),
            (r#"count_values("v", cv) without (instance)"#, 120_000),
            // stddev/stdvar: population std-dev / variance, by and without.
            ("stddev(m) by (job)", 120_000),
            ("stdvar(m) by (job)", 120_000),
            ("stddev(m) without (instance)", 120_000),
            ("stdvar without (instance) (m)", 120_000),
            // Single-element group -> stddev/stdvar = 0.
            ("stddev(single) by (job)", 120_000),
            ("stdvar(single) by (job)", 120_000),
            // No modifier (collapse all).
            ("stddev(m)", 120_000),
            ("quantile(0.5, m)", 120_000),
            // Nested: a parameterized aggregation over a rate inner already on the
            // operator path.
            ("topk(1, rate(reqs_total[3m]))", 180_000),
            ("quantile(0.5, rate(reqs_total[3m]))", 180_000),
            ("stddev by (job) (rate(reqs_total[3m]))", 180_000),
            (r#"count_values("v", rate(reqs_total[3m]))"#, 180_000),
            // Nested: a parameterized aggregation over a SUBQUERY-range inner,
            // which now routes through the planner (subquery sub-grid evaluated
            // per-step through the recursive planner, shared outer fold).
            ("quantile(0.5, max_over_time((m)[5m:1m]))", 120_000),
            // Unary-negation subquery inner: `Expr::Unary` now routes through the
            // planner, so the subquery's structural gate accepts it.
            ("quantile(0.5, max_over_time((-m)[5m:1m]))", 120_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
        }

        // The experimental `limitk`/`limit_ratio` param aggregations now route
        // through the planner via the shared interpreter kernels (incl.
        // `limit_ratio`'s `InvalidRatioWarning`); their parity is checked in
        // `experimental_param_aggregate_planner_path_matches_interpreter`.
    }

    /// M18: an out-of-range / NaN `quantile` phi does NOT error. Matching
    /// Prometheus (and the `histogram_quantile` family already in this file), the
    /// aggregate returns signed `+Inf` (phi > 1), `-Inf` (phi < 0), `NaN` (phi
    /// NaN) and raises an `InvalidQuantileWarning` — never aborting. (This
    /// reverses the earlier deliberate "canonical quantile-phi error" commit to
    /// realign with Prometheus.)
    #[tokio::test]
    async fn quantile_out_of_range_phi_returns_signed_inf_with_warning() {
        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("0", 1.0), ("1", 2.0), ("2", 3.0)] {
            let lbls = labels(&[("__name__", "m"), ("instance", instance)]);
            store.push_float("t", lbls, 120_000, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 120_000_i64;

        for (query, phi_text, predicate) in [
            (
                "quantile(1.1, m)",
                "1.1",
                f64::is_infinite as fn(f64) -> bool,
            ),
            ("quantile(-0.1, m)", "-0.1", f64::is_infinite),
            ("quantile(NaN, m)", "NaN", f64::is_nan),
        ] {
            let (result, annotations) = engine
                .query_instant_with_annotations("t", query, time_ms)
                .await
                .unwrap_or_else(|error| panic!("`{query}` must NOT error: {error}"));

            let QueryResult::InstantVector(samples) = result else {
                panic!("`{query}` must yield an instant vector");
            };
            assert_eq!(samples.len(), 1, "`{query}`: collapsed to one group");
            let value = float_value(&samples[0].value);
            assert!(
                predicate(value),
                "`{query}`: expected signed Inf / NaN, got {value}"
            );
            // For the +/-Inf cases, also pin the sign.
            if query.contains("1.1") {
                assert!(value > 0.0, "phi > 1 -> +Inf");
            } else if query.contains("-0.1") {
                assert!(value < 0.0, "phi < 0 -> -Inf");
            }

            assert_eq!(
                annotations.warnings,
                vec![format!(
                    "PromQL warning: quantile value should be between 0 and 1, got {phi_text}"
                )],
                "`{query}` must raise exactly one InvalidQuantileWarning"
            );
        }
    }

    /// C2: `check_resolution_points` rejects a non-positive step, an abusive
    /// point count above `MAX_RESOLUTION_POINTS`, and accepts a count at the cap.
    #[test]
    fn check_resolution_points_enforces_cap() {
        // A non-positive step is rejected outright.
        assert!(check_resolution_points(0, 1_000, 0).is_err());
        assert!(check_resolution_points(0, 1_000, -1).is_err());

        // `(end-start)/step == MAX_RESOLUTION_POINTS` intervals is accepted — the
        // same boundary the HTTP gate and Prometheus' `(end-start)/step > 11000`
        // rule admit (no off-by-one re-rejection of a gate-admitted query).
        let at_cap = i64::try_from(MAX_RESOLUTION_POINTS).unwrap(); // step = 1ms => intervals == MAX.
        assert!(check_resolution_points(0, at_cap, 1).is_ok());

        // One interval over the cap errors.
        assert!(check_resolution_points(0, at_cap + 1, 1).is_err());

        // The abusive `[1000d:1ms]`-style resolution is rejected before looping.
        let thousand_days_ms = 1_000_i64 * 24 * 60 * 60 * 1_000;
        let err = check_resolution_points(0, thousand_days_ms, 1).expect_err("must reject");
        assert!(err.to_string().contains("exceeded maximum resolution"));
    }

    /// C2 (engine backstop): an abusive subquery resolution errors via the range
    /// driver's `check_resolution_points` guard rather than looping ~1e11 times.
    #[tokio::test]
    async fn abusive_subquery_resolution_errors_before_looping() {
        let mut store = InMemoryMetricStore::new();
        store.push_float("t", labels(&[("__name__", "up")]), 0, 1.0);
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // `last_over_time(up[1000d:1ms])` would walk ~8.6e10 sub-steps; the
        // backstop rejects it with the resolution error instead.
        let err = engine
            .query_instant("t", "last_over_time(up[1000d:1ms])", 0)
            .await
            .expect_err("abusive subquery resolution must error");
        assert!(
            err.to_string().contains("exceeded maximum resolution"),
            "unexpected error: {err}"
        );
    }

    /// Divergences A + B: a collapsed/global `sum`/`avg` over a multi-series
    /// group must be (a) deterministic run-to-run (bit-exact via `to_bits`) and
    /// (b) bit-for-bit identical to the interpreter oracle — including the
    /// NaN-SIGN-bit case where a `{+Inf,-Inf}` group's sign-flipped NaN
    /// (`0xfff8…`) folds alongside genuine NaNs (`0x7ff8…`). A non-deterministic
    /// `DataFusion` hash-aggregate fold would flicker by 1 ULP or flip the NaN
    /// sign bit; routing through the shared `apply_simple_aggregate` kernel must
    /// not.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn sum_avg_collapsed_is_deterministic_and_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();

        // A multi-series group whose float values sum with float-rounding
        // sensitivity: many members of widely different magnitudes, so the
        // accumulation order changes the low bits of the running sum. The
        // operator fold must pick a single canonical order so the result never
        // flickers and equals the interpreter's stable fold.
        let bz_values = [
            1.0,
            1e16,
            -1e16,
            3.0,
            1e-16,
            7.0,
            -2.5,
            1e8,
            -1e8,
            0.1,
            0.2,
            0.3,
            123_456.789,
            -987_654.321,
            2.617_281_828,
            3.041_592_653,
            -1.314_213_562,
            1e10,
            -1e10,
            42.0,
        ];
        for (idx, value) in bz_values.iter().enumerate() {
            let lbls = labels(&[
                ("__name__", "bz_total"),
                ("g", "all"),
                ("instance", &idx.to_string()),
            ]);
            store.push_float("t", lbls, 120_000, *value);
        }

        // Counters for the rate-then-sum/avg case, mirroring the audit's 2m-window
        // rates: a multi-series group whose per-series rates sum with
        // float-rounding sensitivity.
        for (instance, factor) in [
            ("0", 1.0),
            ("1", 1e8),
            ("2", 1e-8),
            ("3", 7.0),
            ("4", 1234.567),
            ("5", -3.5),
        ] {
            let lbls = labels(&[
                ("__name__", "bz_reqs_total"),
                ("g", "all"),
                ("instance", instance),
            ]);
            for step in 0..=2_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 60_000,
                    f64::from(step) * factor,
                );
            }
        }

        // A global-fold case mixing a sign-flipped NaN (from `+Inf + -Inf`) with
        // genuine NaNs. `+Inf` and `-Inf` in the same group sum to a NaN whose
        // sign bit is SET (`0xfff8…`) on most platforms, distinct from a genuine
        // payload NaN (`0x7ff8…`). The fold order determines which NaN's bits
        // survive, so the operator path must agree with the interpreter bit-for-
        // bit on the sign bit.
        for (instance, value) in [
            ("a", f64::INFINITY),
            ("b", f64::NEG_INFINITY),
            ("c", f64::NAN),
            ("d", 5.0),
        ] {
            let lbls = labels(&[("__name__", "naninf"), ("instance", instance)]);
            store.push_float("t", lbls, 120_000, value);
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // (query, time_ms). Each is a collapsed/global or `by (g)` sum/avg over a
        // multi-series group, plus the rate-wrapped and naninf cases.
        let cases = [
            ("sum(bz_total)", 120_000_i64),
            ("avg(bz_total)", 120_000),
            ("sum by (g) (bz_total)", 120_000),
            ("avg by (g) (bz_total)", 120_000),
            ("sum(rate(bz_reqs_total[2m]))", 120_000),
            ("avg(rate(bz_reqs_total[2m]))", 120_000),
            ("sum by (g) (rate(bz_reqs_total[2m]))", 120_000),
            ("avg by (g) (rate(bz_reqs_total[2m]))", 120_000),
            ("sum(naninf)", 120_000),
            ("avg(naninf)", 120_000),
        ];

        for (query, time_ms) in cases {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // The interpreter oracle: the reference result.
            let interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
            let QueryResult::InstantVector(mut interpreter) = interpreter else {
                panic!("expected vector for interpreter `{query}`");
            };
            interpreter.sort_by_key(|sample| sample.labels.fingerprint());

            // Run the operator path MANY times: every run must be bit-identical
            // (deterministic) and equal the interpreter bit-for-bit.
            let mut first_bits: Option<Vec<u64>> = None;
            for run in 0..50 {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("plan `{query}` (run {run}): {error}"))
                    .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                let operator = engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}` (run {run}): {error}"));
                let QueryResult::InstantVector(mut operator) = operator else {
                    panic!("expected vector for operator `{query}`");
                };
                operator.sort_by_key(|sample| sample.labels.fingerprint());

                // Bit-for-bit parity with the interpreter (NaN sign included).
                assert!(
                    instant_samples_match(&interpreter, &operator),
                    "operator/interpreter divergence for `{query}` (run {run}): \
                     {interpreter:?} vs {operator:?}"
                );

                // Determinism: capture the exact float bits and require every run
                // to reproduce them.
                let bits: Vec<u64> = operator
                    .iter()
                    .map(|sample| float_value(&sample.value).to_bits())
                    .collect();
                match &first_bits {
                    None => first_bits = Some(bits),
                    Some(expected) => assert_eq!(
                        &bits, expected,
                        "operator path flickered for `{query}` on run {run}"
                    ),
                }
            }
        }
    }

    /// Compare two whole [`QueryResult`]s for the parity tests below, NaN-aware
    /// across scalar / vector / matrix / string shapes (so a genuine NaN equals a
    /// genuine NaN). Vectors are pre-sorted by fingerprint by the caller.
    fn query_results_match(left: &QueryResult, right: &QueryResult) -> bool {
        match (left, right) {
            (
                QueryResult::Scalar {
                    ts_ms: lt,
                    value: lv,
                },
                QueryResult::Scalar {
                    ts_ms: rt,
                    value: rv,
                },
            ) => lt == rt && lv.to_bits() == rv.to_bits(),
            (QueryResult::InstantVector(left), QueryResult::InstantVector(right)) => {
                instant_samples_match(left, right)
            }
            (QueryResult::RangeMatrix(_), QueryResult::RangeMatrix(_)) => {
                range_matrices_match(left, right)
            }
            (
                QueryResult::Str {
                    ts_ms: lt,
                    value: lv,
                },
                QueryResult::Str {
                    ts_ms: rt,
                    value: rv,
                },
            ) => lt == rt && lv == rv,
            _ => false,
        }
    }

    /// Sort an instant-vector result by fingerprint in place (a no-op for the
    /// other result shapes), so `query_results_match` can compare vectors order-
    /// independently.
    fn sort_instant_result(result: QueryResult) -> QueryResult {
        match result {
            QueryResult::InstantVector(mut samples) => {
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                QueryResult::InstantVector(samples)
            }
            other => other,
        }
    }

    /// Differential parity for the newly-planned top-level structural node kinds:
    /// unary negation, bare numeric / string literals, a raw matrix selector and a
    /// subquery (both `RangeMatrix` results from `query_instant`), and the
    /// `smoothed` extended selector. Each must produce — through the operator
    /// planner — the byte-exact result the interpreter's `eval_instant_expr`
    /// produces.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn structural_node_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();
        let stale_bits = stale_nan();
        for (lbls, ts, value) in [
            (
                labels(&[("__name__", "m"), ("job", "api")]),
                60_000_i64,
                2.0,
            ),
            (labels(&[("__name__", "m"), ("job", "api")]), 120_000, 3.0),
            (labels(&[("__name__", "m"), ("job", "db")]), 120_000, 7.0),
            // A genuine NaN latest-in-window sample (kept, negated to NaN).
            (
                labels(&[("__name__", "m"), ("job", "nan")]),
                120_000,
                f64::NAN,
            ),
            // A stale marker (dropped on both paths).
            (
                labels(&[("__name__", "m"), ("job", "stale")]),
                120_000,
                stale_bits,
            ),
            // A series with a short history for the matrix / subquery / smoothed
            // shapes.
            (labels(&[("__name__", "g")]), 0, 1.0),
            (labels(&[("__name__", "g")]), 60_000, 2.0),
            (labels(&[("__name__", "g")]), 120_000, 4.0),
            (labels(&[("__name__", "g")]), 180_000, 8.0),
            (labels(&[("__name__", "g")]), 240_000, 16.0),
        ] {
            store.push_float("t", lbls, ts, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries: &[(&str, i64)] = &[
            // Unary over a vector (drops `__name__`, negates each value, keeps a
            // genuine NaN, drops the stale marker).
            ("-m", 120_000),
            // Unary over an aggregate result (vector).
            ("-sum(m)", 120_000),
            // Unary over a scalar.
            ("-(1 + 2)", 120_000),
            // Double negation.
            ("- -m", 120_000),
            // Bare numeric / string literals.
            ("42", 120_000),
            ("-7.5", 120_000),
            (r#""hello""#, 120_000),
            // Raw matrix selector / subquery (RangeMatrix from query_instant).
            ("g[3m]", 240_000),
            ("m[2m]", 120_000),
            ("g[4m:1m]", 240_000),
            // `smoothed` extended selector (vector). The extension parser is not
            // feature-gated, so this routes in both build configs.
            ("smoothed(g)", 90_000),
        ];

        for &(query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // The recursive planner must claim every one of these.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = sort_instant_result(
                engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}`: {error}")),
            );
            let via_interpreter = sort_instant_result(
                engine
                    .eval_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}")),
            );

            assert!(
                query_results_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
        }

        // Pin the result types the parity above relies on.
        for (query, time_ms, want) in [
            ("42", 120_000_i64, "scalar"),
            (r#""hello""#, 120_000, "string"),
            ("g[3m]", 240_000, "matrix"),
            ("g[4m:1m]", 240_000, "matrix"),
            ("-m", 120_000, "vector"),
            ("-(1 + 2)", 120_000, "scalar"),
        ] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap();
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let result = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap();
            assert_eq!(result.result_type(), want, "`{query}` result type");
        }

        // The `anchored` modifier on an instant-vector selector is the same hard
        // error on both paths.
        {
            let expr = parse_promql_with_duration_context(
                "anchored(m)",
                DurationExprContext::instant(120_000),
            )
            .unwrap();
            let planner_err = engine.plan_instant_expr("t", &expr, 120_000).await;
            let interp_err = engine.eval_instant_expr("t", &expr, 120_000).await;
            assert!(
                planner_err.is_err(),
                "anchored(m) must error on the planner"
            );
            assert!(
                interp_err.is_err(),
                "anchored(m) must error on the interpreter"
            );
        }
    }

    /// Differential parity for the experimental scalar / range functions:
    /// `max_of`/`min_of`, `double_exponential_smoothing` over a bare matrix
    /// selector, and the duration helpers. Each delegates to the same interpreter
    /// method, so the result is parity-exact by construction.
    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn experimental_call_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();
        for (ts, value) in [
            (0_i64, 1.0),
            (60_000, 2.0),
            (120_000, 4.0),
            (180_000, 8.0),
            (240_000, 16.0),
        ] {
            store.push_float("t", labels(&[("__name__", "g")]), ts, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries: &[(&str, i64)] = &[
            ("max_of(1, 2)", 120_000),
            ("min_of(1, 2)", 120_000),
            ("max_of(scalar(g), 3)", 240_000),
            ("double_exponential_smoothing(g[4m], 0.5, 0.5)", 240_000),
            // Duration helpers (instant query: no range context -> 0 on both
            // paths).
            ("step()", 120_000),
            ("start()", 120_000),
            ("end()", 120_000),
            ("range()", 120_000),
        ];

        for &(query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = sort_instant_result(
                engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("operator `{query}`: {error}")),
            );
            let via_interpreter = sort_instant_result(
                engine
                    .eval_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}")),
            );
            assert!(
                query_results_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
        }
    }

    /// Differential parity for the experimental `limitk`/`limit_ratio` param
    /// aggregations, including `limit_ratio`'s `InvalidRatioWarning` annotation.
    /// The planner reuses the same parameter-resolution helpers and selection
    /// kernels as the interpreter, so both the result AND the emitted annotations
    /// match.
    #[cfg(feature = "experimental-functions")]
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn experimental_param_aggregate_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();
        for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)] {
            store.push_float(
                "t",
                labels(&[("__name__", "m"), ("job", "api"), ("instance", instance)]),
                120_000,
                value,
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries: &[&str] = &[
            "limitk(2, m)",
            "limitk(10, m)",
            "limitk(0, m)",
            "limitk(2, m) by (job)",
            "limit_ratio(0.5, m)",
            "limit_ratio(-0.5, m)",
            "limit_ratio(1, m)",
            "limit_ratio(0, m)",
            // Out-of-range ratios: must emit the InvalidRatioWarning on BOTH paths.
            "limit_ratio(1.5, m)",
            "limit_ratio(-2, m)",
        ];
        let time_ms = 120_000_i64;

        for &query in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path, scoped so the InvalidRatioWarning is captured.
            let (via_operators, operator_annotations) = super::ANNOTATIONS
                .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                    let plan = engine
                        .plan_instant_expr("t", &expr, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                        .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                    let result = engine
                        .assemble_planned_instant(plan, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                    let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                    (sort_instant_result(result), annotations)
                })
                .await;

            // Interpreter path, scoped identically.
            let (via_interpreter, interpreter_annotations) = super::ANNOTATIONS
                .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                    let result = engine
                        .eval_instant_expr("t", &expr, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                    let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                    (sort_instant_result(result), annotations)
                })
                .await;

            assert!(
                query_results_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
            assert_eq!(
                operator_annotations, interpreter_annotations,
                "annotation divergence for `{query}`"
            );
        }

        // Pin that an out-of-range ratio actually emits the warning (so the
        // equality above is not vacuously comparing two empty sets).
        let expr = parse_promql_with_duration_context(
            "limit_ratio(1.5, m)",
            DurationExprContext::instant(time_ms),
        )
        .unwrap();
        let annotations = super::ANNOTATIONS
            .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                let plan = engine
                    .plan_instant_expr("t", &expr, time_ms)
                    .await
                    .unwrap()
                    .unwrap();
                engine
                    .assemble_planned_instant(plan, time_ms)
                    .await
                    .unwrap();
                super::ANNOTATIONS.with(|sink| sink.borrow().clone())
            })
            .await;
        assert_eq!(annotations.warnings.len(), 1, "InvalidRatioWarning missing");
        assert!(
            annotations.warnings[0].contains("ratio value should be between -1 and 1"),
            "unexpected warning text: {:?}",
            annotations.warnings
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn classic_histogram_quantile_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A float-only store of classic `<metric>_bucket{le}` series exercising the
        // classic histogram_quantile fold:
        //  - `lat_bucket{job}`: a well-formed monotonic histogram with a real `+Inf`
        //    overflow bucket, in two groups (job=api / job=db) so the multi-group
        //    case and the `__name__` + `le` drop are both observable.
        //  - `nonmono_bucket`: a NON-monotonic cumulative bucket set (the le=2 count
        //    dips below le=1) so the monotonicity-forcing path is taken.
        //  - `inf_only_bucket`: a single `+Inf` bucket (<2 buckets -> NaN).
        //  - `reqs_bucket{le}`: counters for the NESTED
        //    `histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))` case,
        //    whose fully-float inner plans through the rate + aggregate operators.
        let mut store = InMemoryMetricStore::new();
        for (job, le, value) in [
            ("api", "0.1", 1.0),
            ("api", "0.2", 2.0),
            ("api", "0.4", 4.0),
            ("api", "+Inf", 5.0),
            ("db", "0.1", 0.0),
            ("db", "0.2", 1.0),
            ("db", "0.4", 3.0),
            ("db", "+Inf", 3.0),
        ] {
            let lbls = labels(&[("__name__", "lat_bucket"), ("job", job), ("le", le)]);
            store.push_float("t", lbls, 300_000, value);
        }
        // A non-monotonic cumulative bucket set: le=2 (count 3) dips below le=1
        // (count 5); the fold must force monotonicity before interpolating.
        for (le, value) in [("1", 5.0), ("2", 3.0), ("+Inf", 8.0)] {
            let lbls = labels(&[("__name__", "nonmono_bucket"), ("le", le)]);
            store.push_float("t", lbls, 300_000, value);
        }
        // A single `+Inf` bucket: fewer than two buckets -> NaN.
        store.push_float(
            "t",
            labels(&[("__name__", "inf_only_bucket"), ("le", "+Inf")]),
            300_000,
            7.0,
        );
        // Counters for the nested `histogram_quantile(.., sum by (le) (rate(...)))`
        // case (slope = factor / 60s within the 5m window).
        for (le, factor) in [("0.1", 1.0), ("0.2", 2.0), ("0.4", 4.0), ("+Inf", 5.0)] {
            let lbls = labels(&[("__name__", "reqs_bucket"), ("le", le)]);
            for step in 0..=5_i32 {
                store.push_float(
                    "t",
                    lbls.clone(),
                    i64::from(step) * 60_000,
                    f64::from(step) * factor,
                );
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // Each query must route through the operator path and match the
        // interpreter byte-for-byte (NaN-aware, bit-exact).
        let queries = [
            // Normal linear interpolation, multi-group (job=api / job=db), with the
            // `__name__` and `le` labels dropped from the output.
            ("histogram_quantile(0.5, lat_bucket)", 300_000_i64),
            ("histogram_quantile(0.9, lat_bucket)", 300_000),
            // phi at the boundaries 0 and 1.
            ("histogram_quantile(0, lat_bucket)", 300_000),
            ("histogram_quantile(1, lat_bucket)", 300_000),
            // phi out of [0, 1]: -Inf below, +Inf above.
            ("histogram_quantile(-0.5, lat_bucket)", 300_000),
            ("histogram_quantile(1.5, lat_bucket)", 300_000),
            // A non-monotonic cumulative bucket set is forced monotonic first.
            ("histogram_quantile(0.5, nonmono_bucket)", 300_000),
            // A single `+Inf` bucket (<2 buckets) yields NaN.
            ("histogram_quantile(0.5, inf_only_bucket)", 300_000),
            // NESTED: a fully-float inner that plans through the rate + aggregate
            // operators, then the classic fold over the assembled bucket vector.
            (
                "histogram_quantile(0.9, sum by (le) (rate(reqs_bucket[5m])))",
                300_000,
            ),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );

            // Pin the `__name__` + `le` drop on the operator-path output.
            assert!(
                via_operators.iter().all(|sample| {
                    sample.labels.get("__name__").is_none() && sample.labels.get("le").is_none()
                }),
                "`{query}`: operator path leaked __name__ / le: {via_operators:?}"
            );
        }
        // The native-histogram flavor of these folds (bare selector, native
        // `histogram_quantile`, the native accessors) now routes through the
        // planner too — see `native_histogram_planner_path_matches_interpreter`.
    }

    /// Differential parity for the **native-histogram** constructs that now route
    /// through the recursive planner: a bare native-histogram selector, native
    /// `histogram_quantile`, and every native accessor (`histogram_count`/`sum`/
    /// `avg`/`stddev`/`stdvar`/`fraction`). Each query MUST claim the operator
    /// (`Precomputed`) path and match the interpreter byte-for-byte, with the
    /// histogram payloads compared by value (not float `==`).
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn native_histogram_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // Build a non-trivial native histogram (schema 0, two positive buckets
        // [1,2] and [2,4] carrying counts 1 and 3) with a real count/sum so the
        // quantile, fraction, and stddev/stdvar folds all produce finite values.
        fn seed_histogram(count: f64, sum: f64) -> NativeHistogram {
            let mut histogram = native_histogram(count, sum);
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 2,
            }];
            histogram.positive_counts = vec![1.0, 3.0];
            histogram
        }

        // Two native-histogram groups (job=api / job=db) so multi-series output and
        // the `__name__` drop are both observable, plus a classic `cls_bucket{le}`
        // float histogram to exercise the classic+native co-routing inside the
        // shared `histogram_quantile` / `histogram_fraction` folds.
        let mut store = InMemoryMetricStore::new();
        store.push_histogram(
            "t",
            labels(&[("__name__", "nh"), ("job", "api")]),
            300_000,
            seed_histogram(4.0, 6.5),
        );
        store.push_histogram(
            "t",
            labels(&[("__name__", "nh"), ("job", "db")]),
            300_000,
            seed_histogram(8.0, 20.0),
        );
        for (le, value) in [("1", 1.0), ("2", 3.0), ("+Inf", 4.0)] {
            let lbls = labels(&[("__name__", "cls_bucket"), ("le", le)]);
            store.push_float("t", lbls, 300_000, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            // The bare native-histogram selector itself (carries the histogram
            // payload + full labelset, including `__name__`).
            ("nh", 300_000_i64),
            // Native histogram_quantile at two phis.
            ("histogram_quantile(0.5, nh)", 300_000),
            ("histogram_quantile(0.9, nh)", 300_000),
            // Every native accessor.
            ("histogram_count(nh)", 300_000),
            ("histogram_sum(nh)", 300_000),
            ("histogram_avg(nh)", 300_000),
            ("histogram_stddev(nh)", 300_000),
            ("histogram_stdvar(nh)", 300_000),
            // histogram_fraction carries two scalar bounds.
            ("histogram_fraction(1, 2, nh)", 300_000),
            ("histogram_fraction(-Inf, +Inf, nh)", 300_000),
            // The shared folds also work over the classic float buckets.
            ("histogram_quantile(0.5, cls_bucket)", 300_000),
            ("histogram_fraction(1, 2, cls_bucket)", 300_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // The recursive planner must claim this query (the `Precomputed` path).
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            // `instant_samples_match` compares histogram payloads structurally
            // (via `SampleValue` `PartialEq`) and floats bit-exactly.
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
        }

        // The bare-selector case must surface the native histogram payload (proving
        // the histogram-aware selection actually carried it, not a dropped/empty
        // vector).
        let bare = parse_promql_with_duration_context("nh", DurationExprContext::instant(300_000))
            .expect("parse nh");
        let plan = engine
            .plan_instant_expr("t", &bare, 300_000)
            .await
            .expect("plan nh")
            .expect("nh routes through planner");
        let QueryResult::InstantVector(samples) = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .expect("assemble nh")
        else {
            panic!("expected vector for nh");
        };
        assert_eq!(
            samples.len(),
            2,
            "bare native selector must keep both groups"
        );
        assert!(
            samples
                .iter()
                .all(|sample| matches!(sample.value, SampleValue::Histogram(_))),
            "bare native selector must carry histogram payloads, got: {samples:?}"
        );

        // `histogram_quantiles` (experimental) now routes through the shared
        // `apply_histogram_quantiles` fold and must match the interpreter for both
        // native-histogram and classic bucket inputs, across multiple phis.
        #[cfg(feature = "experimental-functions")]
        for query in [
            "histogram_quantiles(nh, \"q\", 0.5, 0.9)",
            "histogram_quantiles(cls_bucket, \"q\", 0.5)",
        ] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(300_000))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
            let plan = engine
                .plan_instant_expr("t", &expr, 300_000)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, 300_000)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, 300_000)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };
            assert!(
                instant_samples_match(&normalize(via_interpreter), &normalize(via_operators)),
                "histogram_quantiles planner/interpreter divergence for `{query}`"
            );
        }
    }

    /// Differential parity for **histogram-bearing aggregations** that now route
    /// through the recursive planner via the shared `apply_simple_aggregate` /
    /// `apply_*` kernels (the `Precomputed` path). Each query MUST claim the
    /// operator path and match the interpreter byte-for-byte — including the
    /// native-histogram payloads (compared structurally, not by float `==`) and
    /// any warning/info annotations.
    ///
    /// The store exercises every native-histogram aggregation rule:
    /// - `sum`/`avg` MERGE compatible histograms (and `avg` scales by `1/count`);
    /// - a group that MIXES a float and a histogram is DROPPED (the mixed-sample
    ///   rule) under `sum`/`avg`;
    /// - `count`/`group` count every sample regardless of type;
    /// - `min`/`max`/`stddev`/`stdvar`/`topk`/`bottomk`/`quantile` IGNORE
    ///   histogram samples (drop them), reducing only the floats;
    /// - `count_values` formats a histogram value as its JSON label value.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn histogram_aggregation_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A native histogram with two positive buckets so the merge / quantile
        // folds produce non-trivial structure.
        fn seed_histogram(count: f64, sum: f64) -> NativeHistogram {
            let mut histogram = native_histogram(count, sum);
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 2,
            }];
            histogram.positive_counts = vec![1.0, 3.0];
            histogram
        }

        let mut store = InMemoryMetricStore::new();
        // Group g="hist": TWO compatible native histograms (so sum/avg actually
        // merge), across an `instance` dimension so `without (instance)` collapses
        // them into one group.
        store.push_histogram(
            "t",
            labels(&[("__name__", "m"), ("g", "hist"), ("instance", "0")]),
            300_000,
            seed_histogram(4.0, 6.0),
        );
        store.push_histogram(
            "t",
            labels(&[("__name__", "m"), ("g", "hist"), ("instance", "1")]),
            300_000,
            seed_histogram(8.0, 20.0),
        );
        // Group g="float": TWO float members (so the float aggregations reduce a
        // real group and `count`/`group` see floats).
        for (instance, value) in [("0", 2.0), ("1", 6.0)] {
            store.push_float(
                "t",
                labels(&[("__name__", "m"), ("g", "float"), ("instance", instance)]),
                300_000,
                value,
            );
        }
        // Group g="mixed": ONE float + ONE histogram in the same group. Under
        // `sum`/`avg` this group is dropped (mixed-sample rule); under
        // `count`/`group` it counts 2; under the histogram-ignoring ops only the
        // float survives.
        store.push_float(
            "t",
            labels(&[("__name__", "m"), ("g", "mixed"), ("instance", "0")]),
            300_000,
            10.0,
        );
        store.push_histogram(
            "t",
            labels(&[("__name__", "m"), ("g", "mixed"), ("instance", "1")]),
            300_000,
            seed_histogram(2.0, 3.0),
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let queries = [
            // sum/avg MERGE histograms per group; the mixed group is dropped.
            ("sum by (g) (m)", 300_000_i64),
            ("avg by (g) (m)", 300_000),
            ("sum without (instance) (m)", 300_000),
            ("avg without (instance) (m)", 300_000),
            // Global sum over everything: the lone global group mixes floats and
            // histograms, so it is dropped entirely (empty result).
            ("sum(m)", 300_000),
            // count/group count every sample regardless of type.
            ("count by (g) (m)", 300_000),
            ("group by (g) (m)", 300_000),
            ("count without (instance) (m)", 300_000),
            ("count(m)", 300_000),
            // min/max/stddev/stdvar IGNORE histograms (reduce only floats); the
            // all-histogram g="hist" group produces no row, g="float" reduces its
            // two floats, g="mixed" reduces just its one float.
            ("min by (g) (m)", 300_000),
            ("max by (g) (m)", 300_000),
            ("stddev by (g) (m)", 300_000),
            ("stdvar by (g) (m)", 300_000),
            // topk/bottomk/quantile also IGNORE histograms.
            ("topk by (g) (1, m)", 300_000),
            ("bottomk by (g) (1, m)", 300_000),
            ("quantile by (g) (0.5, m)", 300_000),
            // count_values formats histogram values as JSON label values.
            ("count_values by (g) (\"v\", m)", 300_000),
        ];

        for (query, time_ms) in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // The recursive planner must claim this query (the `Precomputed` path),
            // and its annotations must match the interpreter's. Scope an annotation
            // sink around each path so emitted warnings/infos are captured.
            let (via_operators, operator_annotations) = super::ANNOTATIONS
                .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                    let plan = engine
                        .plan_instant_expr("t", &expr, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                        .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                    let result = engine
                        .assemble_planned_instant(plan, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                    let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                    (result, annotations)
                })
                .await;

            let (via_interpreter, interpreter_annotations) = super::ANNOTATIONS
                .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                    let result = engine
                        .eval_instant_expr("t", &expr, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                    let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                    (result, annotations)
                })
                .await;

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            // `instant_samples_match` compares histogram payloads structurally
            // (via `SampleValue` `PartialEq`) and floats bit-exactly.
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
            // Annotation parity: the shared kernel emits identical (here: no)
            // annotations on both paths.
            assert_eq!(
                operator_annotations.warnings, interpreter_annotations.warnings,
                "`{query}`: warning annotations diverge"
            );
            assert_eq!(
                operator_annotations.infos, interpreter_annotations.infos,
                "`{query}`: info annotations diverge"
            );
        }

        // Pin the absolute histogram-aware rules (not just operator==interpreter).
        let sample_by_group =
            |samples: &[crate::InstantSample], g: &str| -> Option<crate::InstantSample> {
                samples
                    .iter()
                    .find(|sample| sample.labels.get("g") == Some(g))
                    .cloned()
            };

        // `sum by (g) (m)`: g="hist" is the MERGED histogram (count 4+8=12,
        // sum 6+20=26), g="float" sums its two floats (2+6=8), g="mixed" is
        // DROPPED (float+histogram).
        let sum_expr = parse_promql_with_duration_context(
            "sum by (g) (m)",
            DurationExprContext::instant(300_000),
        )
        .expect("parse sum");
        let plan = engine
            .plan_instant_expr("t", &sum_expr, 300_000)
            .await
            .expect("plan sum")
            .expect("sum routes through planner");
        let QueryResult::InstantVector(sum_samples) = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .expect("assemble sum")
        else {
            panic!("expected vector for sum");
        };
        assert!(
            sample_by_group(&sum_samples, "mixed").is_none(),
            "sum: mixed float+histogram group must be dropped, got: {sum_samples:?}"
        );
        let hist_row = sample_by_group(&sum_samples, "hist").expect("sum: g=hist row");
        let SampleValue::Histogram(merged) = hist_row.value else {
            panic!("sum: g=hist must be a merged histogram, got: {hist_row:?}");
        };
        assert!(
            approx_eq(merged.count, 12.0) && approx_eq(merged.sum, 26.0),
            "sum: merged histogram count/sum wrong: {merged:?}"
        );
        let float_row = sample_by_group(&sum_samples, "float").expect("sum: g=float row");
        assert!(
            approx_eq(float_value(&float_row.value), 8.0),
            "sum: g=float must sum its floats to 8, got: {float_row:?}"
        );

        // `min by (g) (m)`: g="hist" (all histograms) yields NO row; g="float"
        // reduces to 2; g="mixed" reduces to just its one float (10).
        let min_expr = parse_promql_with_duration_context(
            "min by (g) (m)",
            DurationExprContext::instant(300_000),
        )
        .expect("parse min");
        let plan = engine
            .plan_instant_expr("t", &min_expr, 300_000)
            .await
            .expect("plan min")
            .expect("min routes through planner");
        let QueryResult::InstantVector(min_samples) = engine
            .assemble_planned_instant(plan, 300_000)
            .await
            .expect("assemble min")
        else {
            panic!("expected vector for min");
        };
        assert!(
            sample_by_group(&min_samples, "hist").is_none(),
            "min: all-histogram group must be absent (histograms ignored), got: {min_samples:?}"
        );
        assert!(
            approx_eq(
                float_value(
                    &sample_by_group(&min_samples, "float")
                        .expect("min g=float")
                        .value
                ),
                2.0
            ),
            "min: g=float must be 2"
        );
        assert!(
            approx_eq(
                float_value(
                    &sample_by_group(&min_samples, "mixed")
                        .expect("min g=mixed")
                        .value
                ),
                10.0
            ),
            "min: g=mixed must reduce just its float (10)"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn histogram_range_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // A native-histogram counter sample with two positive buckets, so the
        // rate/increase/delta extrapolation produces non-trivial per-bucket
        // structure and the over_time merge folds real buckets.
        fn counter_histogram(count: f64, sum: f64, b0: f64, b1: f64) -> NativeHistogram {
            let mut histogram = native_histogram(count, sum);
            histogram.positive_spans = vec![BucketSpan {
                offset: 0,
                length: 2,
            }];
            histogram.positive_counts = vec![b0, b1];
            histogram
        }

        // A single histogram-counter series sampled monotonically over a 10m
        // window, then a COUNTER RESET (all components drop) so the planner must
        // exercise the shared counter-reset + extrapolation rules. Timestamps are
        // 1m apart so the window `(eval-10m, eval]` captures the full series.
        let mut store = InMemoryMetricStore::new();
        let series = labels(&[("__name__", "h"), ("job", "api")]);
        for (ts, count, sum, b0, b1) in [
            (60_000_i64, 4.0, 6.0, 1.0, 3.0),
            (120_000, 6.0, 10.0, 2.0, 4.0),
            (180_000, 9.0, 15.0, 3.0, 6.0),
            // COUNTER RESET: every component decreases below the prior sample.
            (240_000, 2.0, 3.0, 1.0, 1.0),
            (300_000, 5.0, 8.0, 2.0, 3.0),
        ] {
            store.push_histogram(
                "t",
                series.clone(),
                ts,
                counter_histogram(count, sum, b0, b1),
            );
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 300_000_i64;

        // Each query is a histogram-bearing rate-family / `_over_time` call (or a
        // composition over one). It must route through the recursive planner (the
        // `Precomputed` path), produce a result byte-for-byte identical to the
        // interpreter (histogram payloads compared structurally, floats
        // bit-exactly), and emit identical annotations.
        let queries = [
            // rate-family over histogram counters (counter-reset + extrapolation).
            "rate(h[10m])",
            "increase(h[10m])",
            "delta(h[10m])",
            "irate(h[10m])",
            "idelta(h[10m])",
            // `_over_time` members that MERGE histograms.
            "sum_over_time(h[10m])",
            "avg_over_time(h[10m])",
            // `_over_time` members that are histogram-SAFE (count the samples /
            // pick the latest, regardless of type).
            "count_over_time(h[10m])",
            "last_over_time(h[10m])",
            "present_over_time(h[10m])",
            // `_over_time` members that IGNORE histograms: an all-histogram window
            // yields no float sample, so the series is dropped (empty result).
            "min_over_time(h[10m])",
            "max_over_time(h[10m])",
            "stddev_over_time(h[10m])",
            "stdvar_over_time(h[10m])",
            "quantile_over_time(0.5, h[10m])",
            // Nested: `histogram_quantile` over `rate(h[range])` composes through
            // the operator path (rate produces a histogram, the quantile folds it).
            "histogram_quantile(0.5, rate(h[10m]))",
            // Aggregation over a histogram rate composes through operators.
            "sum(rate(h[10m]))",
            "sum by (job) (increase(h[10m]))",
        ];

        for query in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            let (via_operators, operator_annotations) = super::ANNOTATIONS
                .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                    let plan = engine
                        .plan_instant_expr("t", &expr, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                        .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
                    let result = engine
                        .assemble_planned_instant(plan, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
                    let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                    (result, annotations)
                })
                .await;

            let (via_interpreter, interpreter_annotations) = super::ANNOTATIONS
                .scope(std::cell::RefCell::new(crate::Annotations::new()), async {
                    let result = engine
                        .eval_instant_expr("t", &expr, time_ms)
                        .await
                        .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
                    let annotations = super::ANNOTATIONS.with(|sink| sink.borrow().clone());
                    (result, annotations)
                })
                .await;

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let via_interpreter = normalize(via_interpreter);
            let via_operators = normalize(via_operators);
            assert!(
                instant_samples_match(&via_interpreter, &via_operators),
                "planner/interpreter divergence for `{query}`: {via_interpreter:?} vs {via_operators:?}"
            );
            assert_eq!(
                operator_annotations.warnings, interpreter_annotations.warnings,
                "`{query}`: warning annotations diverge"
            );
            assert_eq!(
                operator_annotations.infos, interpreter_annotations.infos,
                "`{query}`: info annotations diverge"
            );
        }

        // Pin the absolute rules the parity above relies on (not just
        // operator==interpreter).

        // `rate(h[10m])` yields ONE histogram sample (name dropped), built by the
        // shared counter-reset + extrapolation rules.
        let rate_expr = parse_promql_with_duration_context(
            "rate(h[10m])",
            DurationExprContext::instant(time_ms),
        )
        .expect("parse rate");
        let plan = engine
            .plan_instant_expr("t", &rate_expr, time_ms)
            .await
            .expect("plan rate")
            .expect("rate routes through planner");
        let QueryResult::InstantVector(rate_samples) = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .expect("assemble rate")
        else {
            panic!("expected vector for rate");
        };
        assert_eq!(rate_samples.len(), 1, "rate yields one sample");
        assert_eq!(
            rate_samples[0].labels.get("__name__"),
            None,
            "rate drops the metric name"
        );
        assert!(
            matches!(rate_samples[0].value, SampleValue::Histogram(_)),
            "rate over histogram counters yields a histogram"
        );

        // `min_over_time(h[10m])` over an all-histogram window yields NO row
        // (histograms ignored).
        let min_expr = parse_promql_with_duration_context(
            "min_over_time(h[10m])",
            DurationExprContext::instant(time_ms),
        )
        .expect("parse min_over_time");
        let plan = engine
            .plan_instant_expr("t", &min_expr, time_ms)
            .await
            .expect("plan min_over_time")
            .expect("min_over_time routes through planner");
        let QueryResult::InstantVector(min_samples) = engine
            .assemble_planned_instant(plan, time_ms)
            .await
            .expect("assemble min_over_time")
        else {
            panic!("expected vector for min_over_time");
        };
        assert!(
            min_samples.is_empty(),
            "min_over_time ignores histograms: all-histogram window yields no row, got: {min_samples:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn binary_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // NaN-aware sample comparison: labels and ts must match exactly; values
        // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
        fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
            if left.len() != right.len() {
                return false;
            }
            left.iter().zip(right).all(|(a, b)| {
                a.labels == b.labels
                    && a.ts_ms == b.ts_ms
                    && match (&a.value, &b.value) {
                        (SampleValue::Float(x), SampleValue::Float(y)) => {
                            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                        }
                        _ => false,
                    }
            })
        }

        // A float-only store with overlapping label dimensions for vector
        // matching. `left`/`right` share `{job}` for one-to-one and group_x
        // matching; `code` differentiates the many side. A NaN row and a series
        // present only on one side exercise NaN preservation and no-match drops.
        let mut store = InMemoryMetricStore::new();
        for (name, job, code, instance, value) in [
            // `left`: one per job (the "one" side for group_left).
            ("left", "api", "", "", 10.0),
            ("left", "db", "", "", 20.0),
            // `right`: many per job (`code` dimension), the "many" side.
            ("right", "api", "200", "", 1.0),
            ("right", "api", "500", "", 2.0),
            ("right", "db", "200", "", 4.0),
            // A `right` series whose job has no `left` match (no-match drop).
            ("right", "web", "200", "", 8.0),
            // `m1`/`m2` for one-to-one on/ignoring matching.
            ("m1", "api", "", "0", 3.0),
            ("m1", "api", "", "1", 5.0),
            ("m2", "api", "", "0", 7.0),
            ("m2", "api", "", "1", 11.0),
            // A genuine-NaN row that must survive vector∘scalar arithmetic.
            ("nanm", "api", "", "0", f64::NAN),
            ("nanm", "api", "", "1", 13.0),
        ] {
            let mut pairs = vec![("__name__", name), ("job", job)];
            if !code.is_empty() {
                pairs.push(("code", code));
            }
            if !instance.is_empty() {
                pairs.push(("instance", instance));
            }
            store.push_float("t", labels(&pairs), 60_000, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 60_000_i64;

        // Each query must route through the operator path and match the
        // interpreter byte-for-byte (NaN-aware).
        let queries = [
            // vector ∘ scalar (arithmetic, drops __name__).
            "m1 + 100",
            "m1 * 2",
            // scalar ∘ vector.
            "100 - m1",
            "2 ^ m1",
            // vector ∘ scalar comparison without bool (filters, keeps labelset).
            "m1 > 4",
            // vector ∘ scalar comparison with bool (keeps all, drops __name__).
            "m1 > bool 4",
            // genuine NaN must survive vector∘scalar arithmetic.
            "nanm + 1",
            // vector ∘ vector one-to-one, default matching (drops __name__).
            "m1 + m2",
            "m2 - m1",
            "m1 / m2",
            "m1 % m2",
            "m1 ^ m2",
            "m1 atan2 m2",
            // one-to-one with on / ignoring.
            "m1 + on(job, instance) m2",
            "m1 + ignoring(__name__) m2",
            // one-to-one comparison without bool (keeps LHS labelset incl. name).
            "m1 > m2",
            "m2 >= m1",
            // one-to-one comparison with bool (drops __name__).
            "m1 == bool m2",
            "m1 != bool m2",
            // group_left (many-to-one): the `right` many side copies a label
            // from the `left` one side.
            "right * on(job) group_left left",
            "right + on(job) group_left() left",
            // group_right (one-to-many): the `left` one side, many `right`.
            "left * on(job) group_right right",
            // set ops: and / or / unless, with and without on/ignoring.
            "m1 and m2",
            "m1 or m2",
            "m1 unless m2",
            "right and on(job) left",
            "right unless on(job) left",
            "left or on(job) right",
            // a no-match set op (web has no left): or keeps it, and/unless drop.
            "right and on(job) left",
        ];

        for query in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            // Operator path: the recursive planner must claim this query.
            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));

            // Interpreter path: evaluate the same expression directly.
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
                let QueryResult::InstantVector(mut samples) = result else {
                    panic!("expected vector for `{query}`");
                };
                samples.sort_by(|left, right| {
                    left.labels.fingerprint().cmp(&right.labels.fingerprint())
                });
                samples
            };

            let interpreter = normalize(via_interpreter);
            let operators = normalize(via_operators);
            assert!(
                samples_match(&interpreter, &operators),
                "planner/interpreter divergence for `{query}`: interpreter={interpreter:?}, operators={operators:?}"
            );
        }

        // Pin specific behaviors the parity above relies on.
        // 1. `__name__` is dropped for arithmetic.
        let arith = engine.query_instant("t", "m1 + m2", time_ms).await.unwrap();
        let QueryResult::InstantVector(arith) = arith else {
            panic!("expected vector");
        };
        assert!(
            arith.iter().all(|s| s.labels.get("__name__").is_none()),
            "arithmetic must drop __name__"
        );
        // 2. A comparison without `bool` keeps the LHS labelset (incl. __name__).
        let cmp = engine.query_instant("t", "m1 > m2", time_ms).await.unwrap();
        let QueryResult::InstantVector(cmp) = cmp else {
            panic!("expected vector");
        };
        assert!(
            cmp.iter().all(|s| s.labels.get("__name__") == Some("m1")),
            "comparison without bool keeps the LHS metric name"
        );
        // 3. A no-match set op: `right and on(job) left` drops `web` (no left).
        let setop = engine
            .query_instant("t", "right and on(job) left", time_ms)
            .await
            .unwrap();
        let QueryResult::InstantVector(setop) = setop else {
            panic!("expected vector");
        };
        assert!(
            setop.iter().all(|s| s.labels.get("job") != Some("web")),
            "`and` must drop the unmatched `web` series"
        );

        // Scalar ∘ scalar now folds through the planner into a scalar planned
        // result; it must route AND match the interpreter's scalar value+ts.
        for (query, expected) in [
            ("1 + 2", 3.0_f64),
            ("3 * 4 - 1", 11.0_f64),
            ("2 > bool 1", 1.0_f64),
        ] {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap();
            let planned = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("scalar∘scalar `{query}` must route through the planner")
                });
            let via_operators = engine
                .assemble_planned_instant(planned, time_ms)
                .await
                .unwrap();
            let QueryResult::Scalar { ts_ms, value } = via_operators else {
                panic!("expected scalar for `{query}`");
            };
            assert_eq!(ts_ms, time_ms, "scalar∘scalar `{query}` ts");
            assert!(
                value.to_bits() == expected.to_bits(),
                "scalar∘scalar `{query}` value: got {value}, want {expected}"
            );
        }
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn util_planner_path_matches_interpreter() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        // NaN-aware vector comparison: labels + ts must match exactly; values
        // match when bit-equal or both NaN (Prometheus treats all NaNs alike).
        fn samples_match(left: &[crate::InstantSample], right: &[crate::InstantSample]) -> bool {
            if left.len() != right.len() {
                return false;
            }
            left.iter().zip(right).all(|(a, b)| {
                a.labels == b.labels
                    && a.ts_ms == b.ts_ms
                    && match (&a.value, &b.value) {
                        (SampleValue::Float(x), SampleValue::Float(y)) => {
                            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
                        }
                        _ => false,
                    }
            })
        }

        // NaN-aware whole-result comparison covering both scalar and vector
        // results, sorting vector samples by fingerprint first.
        fn results_match(left: QueryResult, right: QueryResult) -> bool {
            match (left, right) {
                (
                    QueryResult::Scalar {
                        ts_ms: lt,
                        value: lv,
                    },
                    QueryResult::Scalar {
                        ts_ms: rt,
                        value: rv,
                    },
                ) => lt == rt && (lv.to_bits() == rv.to_bits() || (lv.is_nan() && rv.is_nan())),
                (QueryResult::InstantVector(mut l), QueryResult::InstantVector(mut r)) => {
                    l.sort_by_key(|sample| sample.labels.fingerprint());
                    r.sort_by_key(|sample| sample.labels.fingerprint());
                    samples_match(&l, &r)
                }
                _ => false,
            }
        }

        // A float-only store. `m{job}` carries distinct timestamps per series so
        // `timestamp(m)` differs per row; a single-series metric `solo` exercises
        // `scalar(single)`; `dup` (two series) exercises `scalar(multi)->NaN`. A
        // genuine-NaN row survives `timestamp`/calendar drops. `present` exists,
        // `gone` does not (for absent / absent_over_time).
        let mut store = InMemoryMetricStore::new();
        for (name, job, ts, value) in [
            // Two `m` series at different timestamps within the lookback window.
            ("m", "api", 30_000_i64, 100.0),
            ("m", "db", 60_000, 1_700_000_000.0),
            // A genuine-NaN `m` row (must survive timestamp/calendar, value-> ts).
            ("m", "nan", 45_000, f64::NAN),
            // A single-series metric for scalar(single).
            ("solo", "x", 60_000, 42.5),
            // Two series sharing a name for scalar(multi)->NaN.
            ("dup", "a", 60_000, 1.0),
            ("dup", "b", 60_000, 2.0),
            // A present series for absent(present)->empty / absent_over_time.
            ("present", "p", 55_000, 7.0),
        ] {
            store.push_float("t", labels(&[("__name__", name), ("job", job)]), ts, value);
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let time_ms = 60_000_i64;

        // Each query must route through the operator path AND match the
        // interpreter (NaN-aware), covering both vector and scalar results.
        let queries = [
            // Vector-returning utilities over a plannable inner.
            "timestamp(m)",
            "timestamp(solo)",
            "day_of_week(m)",
            "day_of_month(m)",
            "day_of_year(m)",
            "days_in_month(m)",
            "hour(m)",
            "minute(m)",
            "month(m)",
            "year(m)",
            // vector(scalar) yields a single no-label series.
            "vector(42)",
            "vector(time())",
            // absent / absent_over_time, present and missing.
            "absent(present)",
            "absent(gone)",
            "absent(gone{job=\"z\"})",
            "absent_over_time(present[5m])",
            "absent_over_time(gone[5m])",
            "absent_over_time(gone{job=\"z\"}[5m])",
            // Scalar-returning utilities.
            "time()",
            "pi()",
            "scalar(solo)",
            "scalar(dup)",
            // Argless calendar forms operate on time().
            "hour()",
            "year()",
            // scalar∘scalar arithmetic folds to a scalar.
            "2 + 3 * 4",
            // calendar over a scalar-arg utility (vector arg).
            "timestamp(vector(1700000000))",
        ];

        for query in queries {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));

            let plan = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan `{query}`: {error}"))
                .unwrap_or_else(|| panic!("`{query}` did not route through the planner"));
            let via_operators = engine
                .assemble_planned_instant(plan, time_ms)
                .await
                .unwrap_or_else(|error| panic!("operator `{query}`: {error}"));
            let via_interpreter = engine
                .eval_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));

            assert!(
                results_match(via_interpreter.clone(), via_operators.clone()),
                "planner/interpreter divergence for `{query}`: interpreter={via_interpreter:?}, operators={via_operators:?}"
            );
        }

        // Pin specific behaviors the parity above relies on.
        // 1. scalar(single) returns the lone value; scalar(multi) returns NaN.
        let QueryResult::Scalar { value: single, .. } = engine
            .query_instant("t", "scalar(solo)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected scalar");
        };
        assert!(single.to_bits() == 42.5_f64.to_bits(), "scalar(single)");
        let QueryResult::Scalar { value: multi, .. } = engine
            .query_instant("t", "scalar(dup)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected scalar");
        };
        assert!(multi.is_nan(), "scalar(multi) must be NaN");

        // 2. time() and pi() are the eval-time seconds and π.
        let QueryResult::Scalar {
            ts_ms: returned_ts,
            value: eval_seconds,
        } = engine.query_instant("t", "time()", time_ms).await.unwrap()
        else {
            panic!("expected scalar");
        };
        assert_eq!(returned_ts, time_ms);
        assert!(
            eval_seconds.to_bits() == 60.0_f64.to_bits(),
            "time() seconds"
        );
        let QueryResult::Scalar { value: pi_v, .. } =
            engine.query_instant("t", "pi()", time_ms).await.unwrap()
        else {
            panic!("expected scalar");
        };
        assert!(
            pi_v.to_bits() == std::f64::consts::PI.to_bits(),
            "pi() value"
        );

        // 3. absent(present) is empty; absent(gone{job="z"}) carries the matcher
        //    label and value 1.
        let QueryResult::InstantVector(present) = engine
            .query_instant("t", "absent(present)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert!(present.is_empty(), "absent(present) must be empty");
        let QueryResult::InstantVector(gone) = engine
            .query_instant("t", "absent(gone{job=\"z\"})", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert_eq!(gone.len(), 1, "absent(missing) yields one series");
        assert_eq!(
            gone[0].labels.get("job"),
            Some("z"),
            "absent label from matcher"
        );
        assert_eq!(
            gone[0].labels.get("__name__"),
            None,
            "absent drops __name__"
        );
        assert!(
            float_value(&gone[0].value).to_bits() == 1.0_f64.to_bits(),
            "absent value is 1"
        );

        // 4. timestamp(m) reports each sample's own timestamp in seconds, not the
        //    eval time, and drops __name__.
        let QueryResult::InstantVector(ts_samples) = engine
            .query_instant("t", "timestamp(m)", time_ms)
            .await
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert!(
            ts_samples
                .iter()
                .all(|s| s.labels.get("__name__").is_none()),
            "timestamp drops __name__"
        );
        let by_job: std::collections::BTreeMap<&str, f64> = ts_samples
            .iter()
            .map(|s| (s.labels.get("job").unwrap(), float_value(&s.value)))
            .collect();
        assert!(
            by_job[&"api"].to_bits() == 30.0_f64.to_bits(),
            "timestamp api row"
        );
        assert!(
            by_job[&"db"].to_bits() == 60.0_f64.to_bits(),
            "timestamp db row"
        );
        assert!(
            by_job[&"nan"].to_bits() == 45.0_f64.to_bits(),
            "timestamp keeps NaN row"
        );
    }

    /// Corpus green-through-the-public-entry-points guard.
    ///
    /// Runs the FULL conformance corpus through the public `query_instant` /
    /// `query_range` entry points (exactly as the conformance harness does) and
    /// asserts every file passes. With the tree-walking interpreter deleted, the
    /// operator planner is the SOLE evaluation engine reached from these entry
    /// points, so a green corpus here is a green corpus through the planner. The
    /// direct totality proof (every valid query plans to `Ok(Some)`, every invalid
    /// one to `Err`, never `Ok(None)`) lives in
    /// [`plan_instant_expr_is_total_over_construct_sweep`].
    #[tokio::test]
    async fn conformance_corpus_runs_green_through_planner() {
        use crate::conformance::testkit::run_corpus_dir;

        let report = run_corpus_dir("tests/testdata").await;
        // Sanity: the corpus actually ran (no path/setup error swallowed the run).
        assert!(!report.files.is_empty(), "corpus produced no files");
        assert!(
            report.files.iter().all(|file| file.passed),
            "corpus regressed: {:?}",
            report
                .files
                .iter()
                .filter(|file| !file.passed)
                .collect::<Vec<_>>()
        );
    }

    /// Direct totality assertion over a representative construct sweep: for every
    /// VALID query family the corpus can produce, `plan_instant_expr` must return
    /// `Ok(Some(..))` (it routes through the planner) — never `Ok(None)`. For
    /// every INVALID query, it must return `Err(..)` — never `Ok(None)`. This is
    /// the per-construct complement to the corpus-wide counter proof.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn plan_instant_expr_is_total_over_construct_sweep() {
        use crate::DurationExprContext;
        use crate::parse_promql_with_duration_context;

        let mut store = InMemoryMetricStore::new();
        let time_ms = 300_000_i64;
        for (lbls, samples) in [
            (
                labels(&[("__name__", "m"), ("job", "a")]),
                vec![(120_000_i64, 1.0_f64), (240_000, 2.0), (300_000, 3.0)],
            ),
            (
                labels(&[("__name__", "m"), ("job", "b")]),
                vec![(120_000, 4.0), (240_000, 5.0), (300_000, 6.0)],
            ),
            (
                labels(&[("__name__", "n"), ("job", "a")]),
                vec![(120_000, 7.0), (240_000, 8.0), (300_000, 9.0)],
            ),
        ] {
            for (ts, value) in samples {
                store.push_float("t", lbls.clone(), ts, value);
            }
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        // VALID families: each MUST plan to Some (never None, never Err).
        let valid: &[&str] = &[
            // Leaves / literals.
            "m",
            "42",
            "\"hello\"",
            "m offset 1m",
            "m @ 100",
            // Parenthesized.
            "(m)",
            "((m + 1))",
            // Unary.
            "-m",
            "- (m + 1)",
            // Binary: vector∘vector, vector∘scalar, scalar∘scalar, set ops.
            "m + n",
            "m * 2",
            "2 + 3",
            "m and n",
            "m or n",
            "m unless n",
            "m > 5",
            "m == bool 5",
            "sum(m) / sum(n)",
            // Simple aggregations.
            "sum(m)",
            "sum by (job) (m)",
            "avg without (job) (m)",
            "count(m)",
            "min(m)",
            "max(m)",
            "group(m)",
            // Param aggregations.
            "topk(1, m)",
            "bottomk(1, m)",
            "quantile(0.5, m)",
            "count_values(\"v\", m)",
            "stddev(m)",
            "stdvar(m)",
            "stddev by (job) (m)",
            // Rate-family + over_time range calls.
            "rate(m[5m])",
            "increase(m[5m])",
            "delta(m[5m])",
            "irate(m[5m])",
            "idelta(m[5m])",
            "avg_over_time(m[5m])",
            "sum_over_time(m[5m])",
            "count_over_time(m[5m])",
            "min_over_time(m[5m])",
            "max_over_time(m[5m])",
            "stddev_over_time(m[5m])",
            "stdvar_over_time(m[5m])",
            "last_over_time(m[5m])",
            "present_over_time(m[5m])",
            "quantile_over_time(0.5, m[5m])",
            // Aggregation over a range call (compositional).
            "sum by (job) (rate(m[5m]))",
            "max without (job) (avg_over_time(m[5m]))",
            // Scalar-math per-row calls.
            "abs(m)",
            "ceil(m)",
            "floor(m)",
            "round(m)",
            "round(m, 2)",
            "clamp(m, 1, 5)",
            "clamp_min(m, 2)",
            "clamp_max(m, 4)",
            "sqrt(m)",
            "exp(m)",
            "ln(m)",
            "log2(m)",
            "log10(m)",
            "sgn(m)",
            // Trig.
            "sin(m)",
            "cos(m)",
            "tan(m)",
            // Label ops.
            "label_replace(m, \"x\", \"y\", \"job\", \"(.*)\")",
            "label_join(m, \"x\", \"-\", \"job\")",
            "sort(m)",
            "sort_desc(m)",
            // Utilities.
            "time()",
            "pi()",
            "scalar(sum(m))",
            "vector(1)",
            "timestamp(m)",
            "absent(m)",
            "absent(nonexistent_metric)",
            "absent_over_time(m[5m])",
            "day_of_week()",
            "day_of_month(m)",
            "minute()",
            "hour()",
            // Histogram-quantile over a classic bucket vector (float series).
            "histogram_quantile(0.5, m)",
            // Top-level raw matrix selector / subquery (instant query).
            "m[5m]",
            "m[5m:1m]",
            "rate(m[5m:1m])",
            "sum_over_time(m[5m:1m])",
        ];
        for query in valid {
            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
                    .unwrap_or_else(|error| panic!("parse valid `{query}`: {error}"));
            let planned = engine
                .plan_instant_expr("t", &expr, time_ms)
                .await
                .unwrap_or_else(|error| panic!("plan valid `{query}` errored: {error}"));
            assert!(
                planned.is_some(),
                "VALID `{query}` returned Ok(None) — plan_instant_expr is not total"
            );
        }

        // INVALID families: each MUST surface as Err (never Ok(None), never
        // Ok(Some)). These mirror the corpus `expect fail` cases that previously
        // deferred to the interpreter purely to raise the canonical error.
        let invalid: &[&str] = &[
            // Non-scalar / out-of-range / NaN scalar params.
            "quantile_over_time(m, m[5m])",
            "topk(m, m)",
            "quantile(m, m)",
            "clamp(m, m, 5)",
            "round(m, m)",
            "histogram_quantile(m, m)",
            // Non-string-literal label args.
            "label_replace(m, m, \"y\", \"job\", \"(.*)\")",
            "label_join(m, m, \"-\", \"job\")",
            "count_values(m, m)",
            "sort_by_label(m, m)",
            // Wrong arity.
            "time(m)",
            "pi(m)",
            "scalar(m, m)",
            "vector(m, m)",
            "timestamp(m, m)",
            "label_replace(m, \"x\")",
            "histogram_quantile(0.5)",
            // Type mismatch in a binary op (vector op range).
            "m + m[5m]",
        ];
        for query in invalid {
            let Ok(expr) =
                parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            else {
                // A parse-time rejection is also a total outcome (never reaches
                // the planner), so a query the parser rejects is acceptable here.
                continue;
            };
            let outcome = engine.plan_instant_expr("t", &expr, time_ms).await;
            let kind = match &outcome {
                Ok(Some(_)) => "Ok(Some)",
                Ok(None) => "Ok(None)",
                Err(_) => "Err",
            };
            assert!(
                outcome.is_err(),
                "INVALID `{query}` did not raise a planner-side Err (got {kind}) — \
                 the planner still defers this error to the interpreter via Ok(None)"
            );
        }
    }
}
