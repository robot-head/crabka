use std::collections::BTreeMap;

use promql_parser::parser::{
    AggregateExpr, Expr,
    token::{
        T_BOTTOMK, T_COUNT_VALUES, T_LIMIT_RATIO, T_LIMITK, T_QUANTILE, T_STDDEV, T_STDVAR, T_TOPK,
    },
};

#[cfg(feature = "experimental-functions")]
use super::aggregation::{apply_limit_ratio_aggregate, apply_limitk_aggregate};
use super::{
    PromqlEngine,
    aggregation::{
        AggregateOp, aggregate_k, aggregate_quantile, apply_count_values_aggregate,
        apply_k_aggregate, apply_quantile_aggregate, apply_simple_aggregate,
        apply_stddev_stdvar_aggregate,
    },
    planned::{InstantShape, OperatorInstant, PlannedInstant},
    planner_support::{
        aggregate_grouping, simple_aggregate_op, simple_aggregate_op_to_aggregate_op,
    },
};
use crate::{
    PromqlError,
    error::Result,
    planner::aggregate::{Grouping, SimpleAggregateOp, plan_simple_aggregate},
    result::InstantSample,
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plan an `Expr::Aggregate` onto an inner planner plan wrapped in a
    /// `DataFusion` aggregate, when the op is a simple float aggregation and the
    /// inner expression is itself planner-supported and float-only.
    ///
    /// Returns `None` (interpreter fallback) for param aggregations, histogram
    /// inputs, or any inner expression the recursive planner does not support.
    pub(super) async fn plan_simple_aggregate_expr(
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
    /// (`Self::eval_instant_aggregate`) — so the operator path matches Prometheus
    /// by construction (sum/avg merge, count/group count, min/max/stddev/stdvar
    /// ignore histograms, mixed float+histogram groups dropped).
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
    /// backs the interpreter (`Self::eval_instant_aggregate` and its callees),
    /// the operator path matches Prometheus by construction.
    ///
    /// The experimental `limitk`/`limit_ratio` ops are also handled here: their
    /// scalar parameter is resolved through the SAME interpreter helpers
    /// (`Self::eval_limitk_parameter` / `Self::eval_limit_ratio_parameter`,
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
    pub(super) async fn plan_param_aggregate_expr(
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
}
