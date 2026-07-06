use promql_parser::parser::{MatrixSelector, VectorSelector};

use super::{
    InstantShape, OuterRangeFn, PlannedInstant, PromqlEngine, RangeEval, apply_outer_range_fn,
    apply_selector_time_modifier, current_at_modifier_bounds, duration_ms, label_matcher_sets,
};
use crate::{
    error::Result,
    extension::is_stale_nan,
    functions::OverTimeFamily,
    planner::{
        leaf::{InstantSelectorPlan, LabeledSample, plan_instant_vector_selector},
        over_time_range::{
            LabeledSample as OverTimeLabeledSample, OverTimeRangePlan,
            plan_over_time_range_selector,
        },
        rate_range::{
            LabeledSample as RateLabeledSample, RateRangePlan, RateUdfKind,
            plan_rate_range_selector,
        },
    },
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Build (without executing) the instant-vector-selector operator plan: scan
    /// the matched float series over `(eval_time - lookback, eval_time]`,
    /// materialize their labels, and assemble the `SeriesDivide ->
    /// SeriesNormalize -> InstantManipulate` chain.
    pub(super) async fn plan_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<PlannedInstant> {
        // `@ start()`/`@ end()` resolve to the active range query's bounds (when
        // present in a range query); for an instant query the bounds are absent and
        // a bare `@ start()`/`@ end()` raises the same hard error the interpreter
        // does.
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            current_at_modifier_bounds(),
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;

        // Carry the matched series' labels onto each sample. Stale-NaN markers
        // are intentionally kept here: InstantManipulate drops the selected
        // sample only when it is a stale-NaN marker, which suppresses a series
        // whose latest in-window sample is a stale marker while preserving a
        // genuine NaN value (matching interpreter staleness handling).
        // Pre-filtering markers here would instead reveal an older sample and
        // diverge from Prometheus.
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&row.fp).cloned() else {
                continue;
            };
            samples.push(LabeledSample {
                fp: row.fp,
                labels,
                ts_ms: row.ts_ms,
                value: row.value,
            });
        }

        let InstantSelectorPlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_instant_vector_selector(samples, eval_time_ms, self.opts.lookback_delta_ms)
            .await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::Selector,
        ))
    }

    /// True when the selector matches at least one histogram series in the
    /// instant-selector scan window. Such selectors stay on the interpreter
    /// because the float-only operator chain cannot carry histogram samples.
    pub(super) async fn selector_has_histogram_series(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<bool> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            current_at_modifier_bounds(),
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta_ms);
        let matcher_sets = label_matcher_sets(selector);
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        Ok(hist_rows
            .into_iter()
            .any(|row| row.ts_ms > start_ms && row.ts_ms <= eval_time_ms))
    }

    /// True when a matrix selector matches at least one histogram series in its
    /// exact range window `(eval_time - range, eval_time]`. Such selectors stay
    /// on the interpreter; the float-only rate operator chain cannot carry
    /// histogram samples (the interpreter's `range_histogram_sample` handles
    /// them). The window matches `eval_matrix_selector`'s `modifier == None`
    /// scan window exactly (no lookback).
    pub(super) async fn matrix_selector_has_histogram_series(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
    ) -> Result<bool> {
        let range_ms = duration_ms(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range_ms);
        let matcher_sets = label_matcher_sets(&selector.vs);
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        Ok(hist_rows
            .into_iter()
            .any(|row| row.ts_ms > range_start_ms && row.ts_ms <= eval_end_ms))
    }

    /// Build (without executing) the rate-family range-selector operator plan.
    ///
    /// The range-selector window is exactly `(eval_time - range, eval_time]`,
    /// left-open and right-closed, with **no** 5m lookback — unlike the instant
    /// path, which scans `(eval_time - lookback, eval_time]` and selects a single
    /// sample. This matches Prometheus matrix-selector semantics and the
    /// interpreter's `range_function_sample_from_series`. The window's range
    /// width feeds the UDF as `range_ms`; the eval instant feeds it as the scalar
    /// `timestamp` column, from which the UDF re-derives `range_start = t - range`.
    pub(super) async fn plan_rate_range(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        kind: RateUdfKind,
    ) -> Result<PlannedInstant> {
        let range_ms = duration_ms(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range_ms);
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;

        // Build per-sample labeled rows over the exact range window. Stale-NaN
        // markers are dropped here, matching `eval_matrix_selector`; genuine NaN
        // is carried through (the operator chain does not filter NaN), as the
        // interpreter does.
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            if row.ts_ms <= range_start_ms || row.ts_ms > eval_end_ms {
                continue;
            }
            if is_stale_nan(row.value) {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&row.fp).cloned() else {
                continue;
            };
            samples.push(RateLabeledSample {
                fp: row.fp,
                labels,
                ts_ms: row.ts_ms,
                value: row.value,
            });
        }

        let RateRangePlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_rate_range_selector(samples, eval_end_ms, range_ms, kind).await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::RateProjection,
        ))
    }

    /// Build (without executing) the `*_over_time` range-selector operator plan.
    ///
    /// Shares the rate path's window semantics: the window is exactly
    /// `(eval_time - range, eval_time]`, left-open right-closed, with **no** 5m
    /// lookback, matching the interpreter's `over_time_sample_from_series`. The
    /// `phi` quantile literal is threaded for `quantile_over_time` and ignored
    /// otherwise.
    pub(super) async fn plan_over_time_range(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        family: OverTimeFamily,
        phi: f64,
    ) -> Result<PlannedInstant> {
        let range_ms = duration_ms(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range_ms);
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;

        // Build per-sample labeled rows over the exact range window. Stale-NaN
        // markers are dropped here, matching `eval_matrix_selector`; genuine NaN
        // is carried through, as the interpreter does.
        let mut samples = Vec::with_capacity(rows.len());
        for row in rows {
            if row.ts_ms <= range_start_ms || row.ts_ms > eval_end_ms {
                continue;
            }
            if is_stale_nan(row.value) {
                continue;
            }
            let Some(labels) = labels_by_fp.get(&row.fp).cloned() else {
                continue;
            };
            samples.push(OverTimeLabeledSample {
                fp: row.fp,
                labels,
                ts_ms: row.ts_ms,
                value: row.value,
            });
        }

        let OverTimeRangePlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_over_time_range_selector(samples, eval_end_ms, range_ms, family, phi).await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::OverTimeProjection {
                // Only `last_over_time` preserves the metric name; every other
                // family drops it (`OverTimeFn::preserves_metric_name`).
                preserve_metric_name: matches!(family, OverTimeFamily::Last),
            },
        ))
    }

    /// Plan a histogram-bearing rate-family / `*_over_time` matrix-selector call
    /// (`outer(selector[range])`) as a fully-computed [`PlannedInstant::Precomputed`].
    ///
    /// This is the range analog of [`Self::histogram_fold_inner_vector`]: the
    /// float-only operator leaf cannot carry native histograms, so instead of
    /// lowering onto the `RangeManipulate + UDF` chain we assemble the per-series
    /// windowed range vector via the interpreter's own
    /// [`Self::eval_matrix_selector`] — identical by construction — and apply the
    /// **same** shared [`apply_outer_range_fn`] kernel the interpreter's
    /// `eval_*_call` uses. The histogram counter-reset/extrapolation rules
    /// (rate/increase/delta), the float-only `irate`/`idelta` filter, and each
    /// `_over_time` member's histogram behaviour (sum/avg merge; count/last/present
    /// histogram-safe; min/max/stddev/stdvar/quantile ignore histograms) all live
    /// in that kernel, so the result is byte-for-byte the interpreter's.
    ///
    /// The window/`@`/offset resolution mirrors [`Self::eval_range_arg`]'s
    /// matrix-selector arm exactly (`modifier: None` — the `anchored`/`smoothed`
    /// modifiers parse to [`Expr::Extension`], which `match_rate_range_call` /
    /// `match_over_time_range_call` reject, so a matrix selector here never carries
    /// one).
    pub(super) async fn plan_histogram_range_via_kernel(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        outer: OuterRangeFn,
    ) -> Result<PlannedInstant> {
        let range_ms = duration_ms(selector.range)?;
        let end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let series = self
            .eval_matrix_selector(tenant, selector, time_ms, time_ms, None)
            .await?;
        let range = RangeEval {
            series,
            end_ms,
            range_ms,
            modifier: None,
        };
        Ok(PlannedInstant::Precomputed(apply_outer_range_fn(
            range, outer, time_ms,
        )))
    }
}
