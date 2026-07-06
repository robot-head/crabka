use crabka_blockstore::Labels;
use futures::{FutureExt, future::BoxFuture};
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, Expr, MatrixSelector, UnaryExpr, VectorSelector,
    token::{T_BOTTOMK, T_COUNT_VALUES, T_LIMIT_RATIO, T_LIMITK, T_QUANTILE, T_TOPK},
};

use super::{
    AggregateOp, ExtendedSelectorExpr, ExtendedSelectorModifier, HistogramAccessor, InstantValue,
    IrateFn, OuterRangeFn, OverTimeFn, PromqlEngine, RangeFn, aggregate_k, aggregate_quantile,
    apply_count_values_aggregate, apply_histogram_accessor, apply_histogram_fraction,
    apply_histogram_quantile, apply_info, apply_k_aggregate, apply_outer_range_fn,
    apply_quantile_aggregate, apply_simple_aggregate, combine_instant_binary, emit_warning,
    info::parse_info_call,
    invalid_quantile_warning, is_valid_quantile, label_ops,
    labels::{absent_labels, labels_without_metric_name},
    range_functions::range_has_samples,
    scalar::{
        CalendarFn, ClampKind, SortDirection, UnaryFloatFn, clamp_float, negate_query_result,
        round_to_nearest,
    },
    selector::timestamp_seconds,
};
#[cfg(feature = "experimental-functions")]
use super::{
    apply_histogram_quantiles, apply_limit_ratio_aggregate, apply_limitk_aggregate,
    scalar::{DurationHelper, ScalarExtremaFn},
    validate_smoothing_factor,
};
use crate::{
    PromqlError,
    error::Result,
    planner::rate_range::RateUdfKind,
    result::{InstantSample, QueryResult, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
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
    pub(super) fn eval_instant_expr<'a>(
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
    pub(super) async fn eval_instant_selector_via_planner(
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
    /// Evaluate a top-level `f(selector[range])` rate-family call through the
    /// `DataFusion` operator chain (`SeriesDivide -> SeriesNormalize ->
    /// RangeManipulate -> rate-UDF projection`) instead of the interpreter. Thin
    /// wrapper over [`Self::plan_rate_range`] plus the shared assembler; kept as
    /// the parity-test seam.
    #[cfg(test)]
    pub(super) async fn eval_rate_range_via_planner(
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
    pub(super) async fn eval_instant_call(
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
