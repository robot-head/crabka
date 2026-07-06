use std::cell::RefCell;

use promql_parser::parser::Expr;

use super::{
    PromqlEngine, annotations::ANNOTATIONS, result_utils::validate_unique_instant_labelsets,
};
use crate::{
    DurationExprContext, PromqlError,
    error::Result,
    parse_promql_with_duration_context,
    result::{Annotations, QueryResult},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
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
    #[tracing::instrument(
        name = "promql.query_instant",
        level = "info",
        skip_all,
        fields(tenant = %tenant, query = %query, time_ms = time_ms),
        err
    )]
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
    /// (`Self::plan_instant_expr`), which dispatches on the `PromQL` `Expr` node
    /// kind and assembles a `DataFusion` `LogicalPlan` over the custom operators
    /// (plus the shared leaf kernels it reuses for histogram-bearing and other
    /// directly-materialized shapes). The planner is **total**: it returns
    /// `Ok(Some(..))` for every valid query and `Err(..)` for every invalid one,
    /// and never `Ok(None)`. The plan is executed and its output batches assembled
    /// into the result.
    ///
    /// `Ok(None)` is therefore unreachable; should it ever arise it is a planner
    /// bug, surfaced as an internal `PromqlError::Plan` rather than silently
    /// diverging.
    #[tracing::instrument(
        name = "promql.eval_instant",
        level = "debug",
        skip_all,
        fields(time_ms = time_ms),
        err
    )]
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
}
