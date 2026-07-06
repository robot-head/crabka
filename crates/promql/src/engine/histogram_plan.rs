use futures::{FutureExt, future::BoxFuture};
use promql_parser::parser::{Call, Expr};

#[cfg(feature = "experimental-functions")]
use super::histogram::apply_histogram_quantiles;
#[cfg(feature = "experimental-functions")]
use super::planner_support::string_literal_value;
use super::{
    PromqlEngine,
    histogram::{
        HistogramAccessor, apply_histogram_accessor, apply_histogram_fraction,
        apply_histogram_quantile,
    },
    planned::PlannedInstant,
};
use crate::{
    error::Result,
    result::{InstantSample, QueryResult},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Plan a `histogram_quantile(phi, v)` call onto the operator path: resolve
    /// `phi` to a scalar, select the inner instant-vector `v` through the
    /// histogram-aware [`Self::histogram_fold_inner_vector`] (carrying native
    /// histograms as `SampleValue::Histogram`), then apply the **shared** fold
    /// ([`apply_histogram_quantile`]) in pure Rust and return a
    /// [`PlannedInstant::Precomputed`]. Because the same fold backs the
    /// interpreter (`Self::eval_histogram_quantile_call`), the operator path
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
    pub(super) async fn plan_histogram_quantile_call(
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
    /// (`Self::eval_histogram_quantiles_call`), the operator path is parity-exact
    /// for classic and native bucket vectors.
    ///
    /// Returns `None` (interpreter fallback) for wrong arity, a non-string label
    /// argument, a non-scalar `phi`, or an inner expression the recursive planner
    /// cannot evaluate (the interpreter then raises the canonical error).
    #[cfg(feature = "experimental-functions")]
    pub(super) async fn plan_histogram_quantiles_call(
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
    /// interpreter (`Self::eval_histogram_accessor_call`) — float rows dropped,
    /// `__name__` dropped, source timestamp kept — the two paths match Prometheus
    /// by construction.
    ///
    /// Returns `None` (interpreter fallback) for wrong arity (the interpreter
    /// raises the canonical error) or an operand the recursive planner cannot
    /// evaluate.
    pub(super) async fn plan_histogram_accessor_call(
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
    /// (`Self::eval_histogram_fraction_call`) — handling classic buckets, native
    /// histograms, and the classic+native mixed-schema warning — so the two paths
    /// match Prometheus by construction.
    ///
    /// Returns `None` (interpreter fallback) for wrong arity or a non-scalar /
    /// non-evaluable bound (the interpreter raises the canonical error), or an
    /// operand the recursive planner cannot evaluate.
    pub(super) async fn plan_histogram_fraction_call(
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

    /// Evaluate the inner instant-vector argument of a **histogram-fold** call
    /// (`histogram_quantile` / the native accessors) into a `Vec<InstantSample>`
    /// that carries native-histogram series as `SampleValue::Histogram`.
    ///
    /// A bare instant-vector selector is selected directly via the interpreter's
    /// own `Self::eval_instant_selector`, so the result is identical to the
    /// interpreter by construction: genuine NaN floats are preserved, stale-NaN
    /// markers are dropped, the full labelset (including `__name__`) is carried,
    /// and — crucially — histogram series yield `SampleValue::Histogram` rows. The
    /// selection is a direct shared-kernel scan (not the float-only operator
    /// leaf), so histogram samples and empty-valued labels round-trip faithfully.
    /// Every other planner-supported inner expression is recursed into and
    /// assembled (the float operator path never surfaces a histogram, so a nested
    /// inner stays float-only). Returns `None` (caller falls back) only for an
    /// inner expression the recursive planner cannot evaluate.
    pub(super) fn histogram_fold_inner_vector<'a>(
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
}
