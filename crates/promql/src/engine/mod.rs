//! Minimal `PromQL` engine entry point.
//!
//! This currently implements selector evaluation over the `MetricStore` contract.
//! The rest of Slice 2's planner (functions, aggregations, binary ops) will build
//! on this public API.

mod aggregate_plan;
mod aggregation;
mod annotations;
mod assembly;
mod binary;
mod binary_plan;
mod execution;
mod histogram;
mod histogram_plan;
mod info;
mod info_plan;
mod instant_query;
mod labels;
mod planned;
mod planner_dispatch;
mod planner_support;
mod range_fold_plan;
mod range_functions;
mod range_query;
mod result_utils;
mod row_cache;
mod scalar;
mod scalar_eval;
mod selector;
mod selector_eval;
mod selector_plan;
mod store_scans;
#[cfg(test)]
mod test_oracle;
mod util_plan;
mod vector_transform_plan;

use std::sync::Arc;

#[cfg(test)]
use aggregation::{
    AggregateOp, aggregate_k, aggregate_quantile, apply_count_values_aggregate, apply_k_aggregate,
    apply_quantile_aggregate, apply_simple_aggregate,
};
#[cfg(feature = "experimental-functions")]
#[cfg(test)]
use aggregation::{apply_limit_ratio_aggregate, apply_limitk_aggregate};
#[cfg(test)]
pub(crate) use annotations::ANNOTATIONS;
#[cfg(test)]
use annotations::{emit_warning, invalid_quantile_warning, is_valid_quantile};
#[cfg(test)]
use binary::{InstantValue, combine_instant_binary};
#[cfg(all(test, feature = "experimental-functions"))]
use histogram::apply_histogram_quantiles;
#[cfg(test)]
use histogram::{
    HistogramAccessor, apply_histogram_accessor, apply_histogram_fraction, apply_histogram_quantile,
};
use histogram::{
    add_compatible_native_histogram, native_histograms_are_range_compatible,
    scale_native_histogram_values,
};
#[cfg(test)]
use info::apply_info;
use planned::{InstantShape, PlannedInstant};
use planner_support::{LabelOpsKind, string_literal_value};
#[cfg(test)]
use planner_support::{match_rate_range_call, range_expr_routes_through_planner};
#[cfg(all(test, feature = "experimental-functions"))]
use range_functions::validate_smoothing_factor;
#[cfg(test)]
use range_functions::{IrateFn, OverTimeFn, RangeFn};
use range_functions::{OuterRangeFn, apply_outer_range_fn};
pub(crate) use selector::label_matcher_sets;
use selector::{AtModifierBounds, apply_selector_time_modifier, duration_ms};

#[cfg(test)]
use crate::extension::is_stale_nan;
#[cfg(test)]
use crate::planner::ExtendedSelectorExpr;
#[cfg(test)]
use crate::planner::label_ops;
use crate::{
    PromqlError, error::Result, planner::ExtendedSelectorModifier, result::RangeSeries,
    store::MetricStore,
};

/// Static options for `PromQL` evaluation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineOpts {
    /// Maximum age of a sample considered by an instant-vector selector.
    pub lookback_delta_ms: i64,
    /// Global evaluation interval used when a subquery omits its resolution.
    pub eval_interval_ms: i64,
    /// Maximum float samples returned by one query.
    pub max_samples: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            lookback_delta_ms: 5 * 60 * 1000,
            eval_interval_ms: 60_000,
            max_samples: 50_000_000,
        }
    }
}

/// Maximum number of resolution points (steps) a single range/subquery series
/// may span. Prometheus rejects a query whose `(end - start) / step + 1` exceeds
/// this, capping abusive resolutions (e.g. `last_over_time(up[1000d:1ms])`)
/// before the per-step loop runs.
pub const MAX_RESOLUTION_POINTS: u64 = 11_000;

/// Compute the resolution-point count `(end_ms - start_ms) / step_ms + 1` for a
/// range/subquery grid, rejecting an abusive resolution before any per-step
/// evaluation runs.
///
/// The cap is applied to the *interval* count `(end - start) / step`, matching
/// Prometheus' `(end-start)/step > 11000` rule and the HTTP front-gate
/// byte-for-byte (error type, status, and message), so a query that the gate
/// admits is never re-rejected by this backstop.
///
/// # Errors
///
/// Returns [`PromqlError::Plan`] (HTTP 400 `bad_data`) when `step_ms <= 0` or when
/// the interval count exceeds [`MAX_RESOLUTION_POINTS`].
pub fn check_resolution_points(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<u64> {
    if step_ms <= 0 {
        return Err(PromqlError::Plan(format!(
            "zero or negative query resolution step widths are not accepted. Try a positive integer (step={step_ms}ms)"
        )));
    }
    // Reject on the interval count `(end - start) / step` (Prometheus' rule),
    // computed in u64 space so an abusive span can never overflow or wrap into a
    // small count.
    let span = u64::try_from(end_ms.saturating_sub(start_ms).max(0)).unwrap_or(u64::MAX);
    let step = u64::try_from(step_ms).unwrap_or(u64::MAX);
    let intervals = span / step;
    if intervals > MAX_RESOLUTION_POINTS {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .to_string(),
        ));
    }
    Ok(intervals.saturating_add(1))
}

/// `PromQL` evaluator over a concrete metric store.
pub struct PromqlEngine<S: MetricStore> {
    store: Arc<S>,
    opts: EngineOpts,
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(super) struct QueryRangeContext {
    pub(super) start: i64,
    pub(super) end: i64,
    pub(super) step: i64,
}

#[cfg(feature = "experimental-functions")]
tokio::task_local! {
    pub(super) static QUERY_RANGE_CONTEXT: QueryRangeContext;
}

tokio::task_local! {
    /// The active range query's `[start, end]` bounds, scoped by the per-step
    /// planner range driver ([`PromqlEngine::eval_range_via_planner_scoped`]) so a
    /// bare top-level selector carrying an `@ start()` / `@ end()` modifier
    /// resolves those bounds to the QUERY's range bounds — per Prometheus — while
    /// the planner still evaluates the selector at each grid step. Absent (no
    /// task-local) for an instant query, where `@ start()`/`@ end()` is invalid and
    /// the selector planner raises the same hard error the interpreter does.
    static AT_MODIFIER_BOUNDS: AtModifierBounds;
}

/// The range bounds in scope for `@ start()`/`@ end()` resolution, or `None` when
/// not inside a range query (an instant query).
fn current_at_modifier_bounds() -> Option<AtModifierBounds> {
    AT_MODIFIER_BOUNDS.try_with(|bounds| *bounds).ok()
}

struct RangeEval {
    series: Vec<RangeSeries>,
    end_ms: i64,
    range_ms: i64,
    modifier: Option<ExtendedSelectorModifier>,
}

impl<S: MetricStore> PromqlEngine<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self { store, opts }
    }
}

#[cfg(test)]
mod tests;
