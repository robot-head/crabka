use futures::{FutureExt, future::BoxFuture};
use promql_parser::parser::{Call, Expr, UnaryExpr};

use super::{
    PromqlEngine,
    annotations::{emit_warning, invalid_quantile_warning, is_valid_quantile},
    histogram::histogram_accessor_from_function_name,
    planned::PlannedInstant,
    planner_support::{
        is_extended_range_fold_call, label_ops_kind_from_function_name,
        match_experimental_over_time_range_call, match_over_time_range_call, match_rate_range_call,
        match_subquery_range_call, over_time_family_to_outer_range_fn,
        rate_udf_kind_to_outer_range_fn, scalar_math_op_from_function_name,
    },
    range_functions::OuterRangeFn,
    scalar::negate_query_result,
};
use crate::{
    PromqlError,
    error::Result,
    planner::{ExtendedSelectorExpr, ExtendedSelectorModifier},
    result::QueryResult,
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plans an instant expression onto the `DataFusion` operator chain.
    ///
    /// This method recurses through the expression and dispatches on the
    /// `PromQL` `Expr` node kind. It also reuses the shared leaf kernels. This is
    /// the sole evaluation engine.
    ///
    /// This method returns `Ok(Some(plan))` for every valid shape and `Err(..)`
    /// for every invalid one. `Err` also covers genuine store and plan failures.
    /// The dispatch is total: it never returns `Ok(None)` for a query that the
    /// public entry points accept. The
    /// `plan_instant_expr_is_total_over_construct_sweep` test and the green
    /// conformance corpus prove this. Supported node kinds:
    ///
    /// - [`Expr::Paren`][]: recurse into the inner expression.
    /// - [`Expr::VectorSelector`][]: a bare instant-vector selector over
    ///   float-only series (`SeriesDivide -> SeriesNormalize ->
    ///   InstantManipulate`). Histogram-bearing selectors return `None`.
    /// - [`Expr::Call`][]: a rate-family call or a non-experimental `*_over_time`
    ///   call over a bare matrix selector. A FLOAT-only selector lowers onto the
    ///   operator chain (`... -> RangeManipulate -> rate/over_time-UDF`). A
    ///   HISTOGRAM-bearing selector instead assembles the windowed range vector
    ///   through the interpreter's `eval_matrix_selector` and applies the shared
    ///   `apply_outer_range_fn` kernel as a `Precomputed` result, which is
    ///   parity-exact. The experimental `*_over_time` members
    ///   (`mad`/`first`/`ts_of_*`), subquery arguments, anchored and smoothed
    ///   selectors, and present-but-empty-valued labels return `None`.
    /// - [`Expr::Aggregate`][]: a simple float aggregation
    ///   (`sum|avg|min|max|count|group` with `by`/`without`) over a
    ///   planner-supported, float-only inner expression. Param aggregations
    ///   (`topk`/`bottomk`/`quantile`/`count_values`/`stddev`/`stdvar`),
    ///   histogram-typed inputs, and unsupported inner expressions return `None`.
    ///
    /// Every other node kind returns `None`: binary ops, unary, literals, raw
    /// matrix and subquery, and extensions.
    pub(super) fn plan_instant_expr<'a>(
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

    /// Plans a unary `Expr::Unary` (`-v` / `+v`) onto the operator path.
    ///
    /// This method recurses the operand through the planner, assembles it, and
    /// applies the SHARED [`negate_query_result`]. It is identical to
    /// [`Self::eval_instant_unary`] by construction. The `PromQL` parser only
    /// ever produces a `-` unary and drops a leading `+`, so this method always
    /// negates. A non-plannable operand falls back to the interpreter.
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
    /// which returns the canonical unsupported-expression error.
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

    /// Plans an `Expr::Call` onto the operator path, with a dispatch on the
    /// function kind.
    ///
    /// `expr` is the same node as `call_expr`. The rate and over-time matchers
    /// need it, because they inspect the call's range argument. Each arm returns
    /// `Ok(Some(..))` for a supported shape and `Ok(None)` for every other shape,
    /// which falls back to the interpreter. Any function that this method does
    /// not recognize also falls back.
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
}
