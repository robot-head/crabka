use promql_parser::parser::Expr;

use super::{
    PromqlEngine,
    assembly::{
        assemble_aggregate_batches, assemble_over_time_batches, assemble_rate_batches,
        assemble_scalar_math_batches, assemble_selector_batches,
    },
    planned::{InstantShape, OperatorInstant, PlannedInstant},
};
use crate::{PromqlError, error::Result, result::QueryResult, store::MetricStore};

impl<S: MetricStore> PromqlEngine<S> {
    /// Executes a planned instant query and assembles its output batches into an
    /// [`InstantVector`](QueryResult::InstantVector).
    ///
    /// This method reads each shape's columns as `InstantShape` defines them.
    #[tracing::instrument(
        name = "promql.execute_operator",
        level = "debug",
        skip_all,
        fields(time_ms = time_ms, shape = tracing::field::Empty),
        err
    )]
    pub(super) async fn assemble_planned_instant(
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
        let shape_name = match &shape {
            InstantShape::Selector => "selector",
            InstantShape::RateProjection => "rate_projection",
            InstantShape::OverTimeProjection { .. } => "over_time_projection",
            InstantShape::Aggregate => "aggregate",
            InstantShape::ScalarMath => "scalar_math",
        };
        tracing::Span::current().record("shape", shape_name);
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

    /// Plans a sub-expression through the recursive operator planner and
    /// assembles it into a [`QueryResult`].
    ///
    /// This method is the production resolver that the `plan_*` helpers use to
    /// evaluate their sub-trees: scalar args, binary and unary operands,
    /// aggregate inners, and function inputs. It makes the planner fully
    /// self-recursive. The planner never re-enters the tree-walking interpreter,
    /// which stays only as the `#[cfg(test)]` differential parity oracle.
    ///
    /// `Self::plan_instant_expr` is proven total: it returns `Ok(Some(_))` for
    /// every expression it accepts and `Err` for the invalid ones. So the
    /// `Ok(None)` arm is unreachable and maps to an internal
    /// [`PromqlError::Plan`].
    pub(super) async fn plan_and_resolve(
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
}
