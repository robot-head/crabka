use crabka_units::prelude::*;
use promql_parser::parser::{Call, SubqueryExpr};

#[cfg(feature = "experimental-functions")]
use super::range_functions::validate_smoothing_factor;
use super::{
    PromqlEngine, RangeEval,
    annotations::{emit_warning, invalid_quantile_warning, is_valid_quantile},
    apply_selector_time_modifier,
    planned::PlannedInstant,
    planner_support::{SubqueryOuterFn, instant_expr_is_plannable, range_fold_range_arg_index},
    range_functions::{
        IrateFn, OuterRangeFn, OverTimeFn, RangeFn, align_subquery_start, apply_outer_range_fn,
    },
    selector_duration,
};
use crate::{
    PromqlError,
    error::Result,
    result::{InstantSample, QueryResult},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
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
    pub(super) async fn plan_extended_range_fold_call(
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
    pub(super) async fn resolve_range_fold_call(
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
                let duration = self
                    .eval_scalar_arg(tenant, call, 1, time_ms, "duration")
                    .await?;
                OuterRangeFn::PredictLinear(Time::from_secs_f64(duration))
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

    /// Plan a range/`*_over_time` call whose argument is a **subquery**
    /// (`f(inner[range:resolution] ...)`) onto the operator path.
    ///
    /// The subquery's range vector is built by evaluating its inner instant
    /// expression at each aligned sub-step on the grid covering
    /// `(end - range, end]` with stride `resolution` (default = the engine's
    /// global eval interval) — through the **recursive planner**
    /// ([`Self::eval_range_via_planner`]), so every sub-step matches the
    /// interpreter's per-step `eval_instant_expr` byte-for-byte. The sub-grid
    /// alignment (`align_subquery_start`), the resolution default, and the
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
    pub(super) async fn plan_subquery_range_call(
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
                OuterRangeFn::PredictLinear(Time::from_secs_f64(value))
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
        let range = selector_duration(subquery.range)?;
        let step = match subquery.step {
            Some(step) => selector_duration(step)?,
            None => self.opts.eval_interval,
        };
        if step <= Time::ZERO {
            // The interpreter raises a hard error here; fall back so it does.
            return Ok(None);
        }
        let end_ms = apply_selector_time_modifier(
            time_ms,
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            None,
        )?;
        let start_ms = align_subquery_start(end_ms.saturating_sub(range.millis_i64()), step);

        // Build the subquery's range vector through the recursive planner. A
        // `None` here means some sub-step's shape is not planner-supported, so the
        // whole subquery falls back to the interpreter.
        let Some(series) = self
            .eval_range_via_planner(tenant, &subquery.expr, start_ms, end_ms, step)
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
            range,
            modifier: None,
        };
        Ok(Some(PlannedInstant::Precomputed(apply_outer_range_fn(
            range, outer, time_ms,
        ))))
    }
}
