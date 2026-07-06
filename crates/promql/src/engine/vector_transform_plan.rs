use std::collections::BTreeMap;

use crabka_blockstore::SeriesFingerprint;
use futures::{FutureExt, future::BoxFuture};
use promql_parser::parser::{Call, Expr, VectorSelector};

use super::{
    InstantShape, LabelOpsKind, PlannedInstant, PromqlEngine, apply_selector_time_modifier,
    label_matcher_sets,
};
use crate::{
    error::Result,
    extension::is_stale_nan,
    functions::ScalarMathOp,
    planner::{
        label_ops,
        scalar_math::{LabeledValue as ScalarMathLabeledValue, ScalarMathPlan, plan_scalar_math},
    },
    result::{InstantSample, QueryResult, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plan a per-row scalar-math `Call` (`abs`/`ceil`/.../`sgn`, the
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
    pub(super) async fn plan_scalar_math_call(
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
        // (`round(v, ...)`, `clamp(v, ...)`, `abs(v)`). Source the already-evaluated
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
    pub(super) async fn plan_label_ops_call(
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
                    super::string_literal_value(call, 1),
                    super::string_literal_value(call, 2),
                    super::string_literal_value(call, 3),
                    super::string_literal_value(call, 4),
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
                let (Some(dst), Some(separator)) = (
                    super::string_literal_value(call, 1),
                    super::string_literal_value(call, 2),
                ) else {
                    return Ok(None);
                };
                let mut src_labels = Vec::with_capacity(call.args.args.len() - 3);
                for index in 3..call.args.args.len() {
                    let Some(label) = super::string_literal_value(call, index) else {
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
                    let Some(label) = super::string_literal_value(call, index) else {
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
    pub(super) fn label_ops_inner_vector<'a>(
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
    /// markers — a float-only mirror of `Self::eval_instant_selector` used as
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
}
