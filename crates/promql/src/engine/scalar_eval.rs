#[cfg(feature = "experimental-functions")]
use promql_parser::parser::AggregateExpr;
use promql_parser::parser::Call;
#[cfg(any(test, feature = "experimental-functions"))]
use promql_parser::parser::Expr;

use super::PromqlEngine;
#[cfg(feature = "experimental-functions")]
use super::annotations::{emit_warning, invalid_ratio_warning};
#[cfg(feature = "experimental-functions")]
use super::planned::PlannedInstant;
#[cfg(feature = "experimental-functions")]
use super::scalar::{DurationHelper, ScalarExtremaFn, scalar_call_to_planned};
use crate::{PromqlError, error::Result, result::QueryResult, store::MetricStore};

impl<S: MetricStore> PromqlEngine<S> {
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
    pub(super) async fn plan_experimental_call(
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

    #[cfg(feature = "experimental-functions")]
    pub(super) async fn eval_limitk_parameter(
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
    pub(super) async fn eval_limit_ratio_parameter(
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
    pub(super) async fn eval_aggregate_scalar_parameter(
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

    #[cfg(feature = "experimental-functions")]
    #[allow(
        clippy::cast_precision_loss,
        reason = "PromQL duration helpers return seconds as f64 scalars"
    )]
    pub(super) fn eval_duration_helper_call(
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

    #[cfg(feature = "experimental-functions")]
    pub(super) async fn eval_scalar_extrema_call(
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

    pub(super) async fn eval_scalar_arg(
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

    #[cfg(any(test, feature = "experimental-functions"))]
    pub(super) async fn eval_scalar_expr(
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
}
