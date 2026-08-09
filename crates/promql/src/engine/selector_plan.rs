use crabka_units::prelude::*;
use promql_parser::parser::{MatrixSelector, VectorSelector};

use super::{
    InstantShape, OuterRangeFn, PlannedInstant, PromqlEngine, RangeEval, apply_outer_range_fn,
    apply_selector_time_modifier, current_at_modifier_bounds, label_matcher_sets,
    selector_duration,
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
    /// Builds the instant-vector-selector operator plan without executing it.
    ///
    /// This method scans the matched float series over
    /// `(eval_time - lookback, eval_time]`, materializes their labels, and
    /// assembles the `SeriesDivide -> SeriesNormalize -> InstantManipulate`
    /// chain.
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
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta.millis_i64());
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
        } = plan_instant_vector_selector(samples, eval_time_ms, self.opts.lookback_delta).await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::Selector,
        ))
    }

    /// Returns `true` when the selector matches at least one histogram series.
    ///
    /// The window is the instant-selector scan window. Such selectors stay on the
    /// interpreter, because the float-only operator chain cannot carry histogram
    /// samples.
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
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta.millis_i64());
        let matcher_sets = label_matcher_sets(selector);
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        Ok(hist_rows
            .into_iter()
            .any(|row| row.ts_ms > start_ms && row.ts_ms <= eval_time_ms))
    }

    /// Returns `true` when a matrix selector matches at least one histogram
    /// series.
    ///
    /// The window is the exact range window `(eval_time - range, eval_time]`.
    /// Such selectors stay on the interpreter, because the float-only rate
    /// operator chain cannot carry histogram samples. The interpreter's
    /// `range_histogram_sample` handles them. The window matches
    /// `eval_matrix_selector`'s `modifier == None` scan window exactly, with no
    /// lookback.
    pub(super) async fn matrix_selector_has_histogram_series(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
    ) -> Result<bool> {
        let range = selector_duration(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range.millis_i64());
        let matcher_sets = label_matcher_sets(&selector.vs);
        let hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, range_start_ms, eval_end_ms)
            .await?;
        Ok(hist_rows
            .into_iter()
            .any(|row| row.ts_ms > range_start_ms && row.ts_ms <= eval_end_ms))
    }

    /// Builds the rate-family range-selector operator plan without executing it.
    ///
    /// The range-selector window is exactly `(eval_time - range, eval_time]`,
    /// left-open and right-closed, with no 5m lookback. The instant path differs:
    /// it scans `(eval_time - lookback, eval_time]` and selects a single sample.
    /// This window matches Prometheus matrix-selector semantics and the
    /// interpreter's `range_function_sample_from_series`. The window's range
    /// width feeds the UDF as its range extent. The eval instant feeds the UDF as
    /// the scalar `timestamp` column, and the UDF re-derives
    /// `range_start = t - range` from it.
    pub(super) async fn plan_rate_range(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        kind: RateUdfKind,
    ) -> Result<PlannedInstant> {
        let range = selector_duration(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range.millis_i64());
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
        } = plan_rate_range_selector(samples, eval_end_ms, range, kind).await?;
        Ok(PlannedInstant::operator(
            ctx,
            plan,
            labels_by_fp,
            InstantShape::RateProjection,
        ))
    }

    /// Builds the `*_over_time` range-selector operator plan without executing it.
    ///
    /// This plan shares the rate path's window semantics. The window is exactly
    /// `(eval_time - range, eval_time]`, left-open and right-closed, with no 5m
    /// lookback, and it matches the interpreter's `over_time_sample_from_series`.
    /// This method passes the `phi` quantile literal through for
    /// `quantile_over_time` and ignores it otherwise.
    pub(super) async fn plan_over_time_range(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        family: OverTimeFamily,
        phi: f64,
    ) -> Result<PlannedInstant> {
        let range = selector_duration(selector.range)?;
        let eval_end_ms = apply_selector_time_modifier(
            time_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            None,
        )?;
        let range_start_ms = eval_end_ms.saturating_sub(range.millis_i64());
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
        } = plan_over_time_range_selector(samples, eval_end_ms, range, family, phi).await?;
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

    /// Plans a histogram-bearing matrix-selector call as a fully-computed
    /// [`PlannedInstant::Precomputed`].
    ///
    /// The call is a rate-family or `*_over_time` form, `outer(selector[range])`.
    /// This method is the range analog of
    /// [`Self::histogram_fold_inner_vector`]. The float-only operator leaf cannot
    /// carry native histograms, so this method does not lower onto the
    /// `RangeManipulate + UDF` chain. It assembles the per-series windowed range
    /// vector through the interpreter's own [`Self::eval_matrix_selector`], which
    /// is identical by construction, and applies the same shared
    /// [`apply_outer_range_fn`] kernel that the interpreter's `eval_*_call` uses.
    ///
    /// That kernel holds the histogram counter-reset and extrapolation rules for
    /// `rate`/`increase`/`delta`, the float-only `irate`/`idelta` filter, and
    /// each `_over_time` member's histogram behaviour: `sum`/`avg` merge,
    /// `count`/`last`/`present` are histogram-safe, and
    /// `min`/`max`/`stddev`/`stdvar`/`quantile` ignore histograms. The result is
    /// therefore byte-for-byte the interpreter's.
    ///
    /// The window, `@`, and offset resolution mirrors [`Self::eval_range_arg`]'s
    /// matrix-selector arm exactly, with `modifier: None`. The `anchored` and
    /// `smoothed` modifiers parse to [`Expr::Extension`], which
    /// `match_rate_range_call` and `match_over_time_range_call` reject, so a
    /// matrix selector here never carries one.
    pub(super) async fn plan_histogram_range_via_kernel(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        outer: OuterRangeFn,
    ) -> Result<PlannedInstant> {
        let range = selector_duration(selector.range)?;
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
            range,
            modifier: None,
        };
        Ok(PlannedInstant::Precomputed(apply_outer_range_fn(
            range, outer, time_ms,
        )))
    }
}
