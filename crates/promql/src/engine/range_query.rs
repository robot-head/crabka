use std::{cell::RefCell, collections::BTreeMap};

use crabka_blockstore::{Labels, SeriesFingerprint};
use crabka_units::prelude::*;
use promql_parser::parser::Expr;

use super::{
    AT_MODIFIER_BOUNDS, AtModifierBounds, PromqlEngine,
    annotations::ANNOTATIONS,
    check_resolution_points,
    planner_support::range_expr_routes_through_planner,
    row_cache::{RANGE_SCAN_CACHE, RangeScanCache, RangeScanCacheInner},
};
#[cfg(feature = "experimental-functions")]
use super::{QUERY_RANGE_CONTEXT, QueryRangeContext};
use crate::{
    DurationExprContext, PromqlError,
    error::Result,
    parse_promql_with_duration_context,
    result::{Annotations, QueryResult, RangeSeries, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Evaluates a range query over `[start_ms, end_ms]`.
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
        step: Time,
    ) -> Result<QueryResult> {
        self.query_range_with_annotations(tenant, query, start_ms, end_ms, step)
            .await
            .map(|(result, _)| result)
    }

    /// Evaluates a range query over `[start_ms, end_ms]` with its annotations.
    ///
    /// The annotations are the warnings and infos raised during the evaluation.
    /// This method returns them with the result.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    #[tracing::instrument(
        name = "promql.query_range",
        level = "info",
        skip_all,
        fields(
            tenant = %tenant,
            query = %query,
            start_ms = start_ms,
            end_ms = end_ms,
            step_ms = step.millis_i64()
        ),
        err
    )]
    pub async fn query_range_with_annotations(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<(QueryResult, Annotations)> {
        ANNOTATIONS
            .scope(RefCell::new(Annotations::new()), async move {
                let result = self
                    .eval_range_query(tenant, query, start_ms, end_ms, step)
                    .await?;
                let annotations = ANNOTATIONS.with(|sink| sink.borrow().clone());
                Ok((result, annotations))
            })
            .await
    }

    #[tracing::instrument(
        name = "promql.eval_range",
        level = "debug",
        skip_all,
        fields(start_ms = start_ms, end_ms = end_ms, step_ms = step.millis_i64(), route = tracing::field::Empty),
        err
    )]
    async fn eval_range_query(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<QueryResult> {
        if step <= Time::ZERO {
            return Err(PromqlError::Plan("step must be positive".to_string()));
        }
        if end_ms < start_ms {
            return Err(PromqlError::Plan("end must be >= start".to_string()));
        }

        let expr = parse_promql_with_duration_context(
            query,
            DurationExprContext::range(start_ms, end_ms, step),
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
            tracing::Span::current().record("route", "planner");
            let Some(series) = self
                .eval_range_via_planner_scoped(tenant, expr, start_ms, end_ms, step)
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
            Expr::MatrixSelector(ms) => {
                tracing::Span::current().record("route", "matrix");
                self.eval_matrix_selector(tenant, ms, start_ms, end_ms, None)
                    .await
                    .map(QueryResult::RangeMatrix)
            }
            Expr::Subquery(subquery) => {
                tracing::Span::current().record("route", "subquery");
                self.eval_subquery(tenant, subquery, end_ms)
                    .await
                    .map(QueryResult::RangeMatrix)
            }
            other => Err(PromqlError::Plan(format!(
                "planner did not claim range expression: {other}"
            ))),
        }
    }

    /// Per-step planner range driver with both evaluation scopes applied.
    ///
    /// The driver runs in `QUERY_RANGE_CONTEXT`, so any nested duration-helper
    /// scalar fold (`step()`, `start()`, and the other experimental forms)
    /// resolves to the query's range grid. It also runs in `AT_MODIFIER_BOUNDS`,
    /// so a bare top-level selector's `@ start()` / `@ end()` resolves to the
    /// query's range bounds.
    #[cfg(feature = "experimental-functions")]
    async fn eval_range_via_planner_scoped(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<Option<Vec<RangeSeries>>> {
        AT_MODIFIER_BOUNDS
            .scope(
                AtModifierBounds { start_ms, end_ms },
                QUERY_RANGE_CONTEXT.scope(
                    QueryRangeContext {
                        start_ms,
                        end_ms,
                        step,
                    },
                    self.eval_range_via_planner(tenant, expr, start_ms, end_ms, step),
                ),
            )
            .await
    }

    /// Per-step planner range driver with the `AT_MODIFIER_BOUNDS` scope applied.
    ///
    /// The driver runs in `AT_MODIFIER_BOUNDS`, so a bare top-level selector's
    /// `@ start()` / `@ end()` resolves to the query's range bounds.
    #[cfg(not(feature = "experimental-functions"))]
    async fn eval_range_via_planner_scoped(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<Option<Vec<RangeSeries>>> {
        AT_MODIFIER_BOUNDS
            .scope(
                AtModifierBounds { start_ms, end_ms },
                self.eval_range_via_planner(tenant, expr, start_ms, end_ms, step),
            )
            .await
    }

    /// Evaluates a plannable instant `expr` over the step grid through the planner.
    ///
    /// This method stitches the per-step instant vectors and scalars into a
    /// [`RangeMatrix`](QueryResult::RangeMatrix). It walks the step grid from
    /// `start_ms` to `end_ms` inclusive and advances by `step` with a saturating
    /// add.
    ///
    /// The method groups samples by label-set fingerprint into one series each
    /// and appends the points in step order. Gaps stay implicit: a step where a
    /// series has no value produces no point. A scalar expr folds into a single
    /// empty-label-set series. The output iterates in fingerprint order, the
    /// `BTreeMap` keys, which matches Prometheus byte-for-byte.
    ///
    /// Returns `Ok(None)` only if some step's [`Self::plan_instant_expr`] returns
    /// `None`. The planner is total, so this never happens for a plannable
    /// `expr`. The caller treats an `Ok(None)` as an internal planner bug.
    #[tracing::instrument(
        name = "promql.range_planner",
        level = "debug",
        skip_all,
        fields(start_ms = start_ms, end_ms = end_ms, step_ms = step.millis_i64(), steps = tracing::field::Empty),
        err
    )]
    pub(super) async fn eval_range_via_planner(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<Option<Vec<RangeSeries>>> {
        // Backstop the resolution cap before the per-step loop, so an abusive
        // subquery resolution (e.g. `last_over_time(up[1000d:1ms])`) errors
        // rather than looping ~1e11 times. The HTTP front-gate enforces the same
        // cap on the top-level query window; this guards the engine itself
        // (including subqueries, whose grid the front-gate never sees).
        let step_count = check_resolution_points(start_ms, end_ms, step)?;
        tracing::Span::current().record("steps", step_count);
        // Scan the union of all per-step lookback windows once per matcher set,
        // shared across the step loop via the RANGE_SCAN_CACHE task-local. The
        // union starts at the first step's lookback floor (`start - lookback`)
        // and ends at the last step (`end`). Scans outside this window
        // (offset/@-modifier or a `[range]` longer than the lookback) fall back to
        // a direct scan inside `scan_float_rows`, so results are unchanged.
        let cache: RangeScanCache =
            std::sync::Arc::new(std::sync::Mutex::new(RangeScanCacheInner {
                full_start_ms: start_ms.saturating_sub(self.opts.lookback_delta.millis_i64()),
                full_end_ms: end_ms,
                floats: std::collections::HashMap::new(),
                histograms: std::collections::HashMap::new(),
                labels: std::collections::HashMap::new(),
            }));
        RANGE_SCAN_CACHE
            .scope(cache, async move {
                let mut by_fp: BTreeMap<SeriesFingerprint, RangeSeries> = BTreeMap::new();
                let mut step_time_ms = start_ms;
                while step_time_ms <= end_ms {
                    let Some(planned) = self.plan_instant_expr(tenant, expr, step_time_ms).await?
                    else {
                        // This step's shape is not planner-supported (e.g. a histogram
                        // series appeared in-window). Abandon the operator path for the
                        // whole query so the interpreter produces a consistent result.
                        return Ok(None);
                    };
                    match self.assemble_planned_instant(planned, step_time_ms).await? {
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
                                    .push((step_time_ms, sample.value));
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
                                .push((step_time_ms, SampleValue::Float(value)));
                        }
                        QueryResult::Str { .. } | QueryResult::RangeMatrix(_) => {
                            // The planner only ever assembles an instant vector or a
                            // scalar; neither of these can arise. Fall back defensively.
                            return Ok(None);
                        }
                    }
                    step_time_ms = step_time_ms.saturating_add(step.millis_i64());
                }
                Ok(Some(by_fp.into_values().collect()))
            })
            .await
    }

    /// Forces the per-step planner range driver past the production gate.
    ///
    /// This method bypasses [`range_expr_routes_through_planner`] and is the
    /// parity-test seam. It lets the differential test drive every range case
    /// through the operator path and compare the output to the interpreter's
    /// `query_range`, which proves parity before the gate is trusted. The cases
    /// include a bare top-level selector, which the production gate keeps on the
    /// interpreter.
    #[cfg(test)]
    pub(super) async fn eval_range_via_planner_forced(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<QueryResult> {
        let expr = parse_promql_with_duration_context(
            query,
            DurationExprContext::range(start_ms, end_ms, step),
        )?;
        let mut expr = &expr;
        while let Expr::Paren(paren) = expr {
            expr = &paren.expr;
        }
        let series = self
            .eval_range_via_planner_scoped(tenant, expr, start_ms, end_ms, step)
            .await?
            .expect("forced planner range driver returned None");
        Ok(QueryResult::RangeMatrix(series))
    }
}
