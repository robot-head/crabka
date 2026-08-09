//! Minimal `PromQL` engine entry point.
//!
//! This module implements selector evaluation over the `MetricStore` contract.
//! The rest of the Slice 2 planner (functions, aggregations, binary ops) will
//! build on this public API.

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
use crabka_units::prelude::*;
pub(crate) use histogram::add_compatible_native_histogram;
#[cfg(all(test, feature = "experimental-functions"))]
use histogram::apply_histogram_quantiles;
#[cfg(test)]
use histogram::{
    HistogramAccessor, apply_histogram_accessor, apply_histogram_fraction, apply_histogram_quantile,
};
use histogram::{native_histograms_are_range_compatible, scale_native_histogram_values};
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
use selector::{AtModifierBounds, apply_selector_time_modifier, selector_duration};

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
///
/// This type is not `Eq`, because the two windows are [`Time`] quantities that
/// store `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EngineOpts {
    /// Maximum age of a sample considered by an instant-vector selector.
    pub lookback_delta: Time,
    /// Global evaluation interval used when a subquery omits its resolution.
    pub eval_interval: Time,
    /// Maximum float samples returned by one query.
    pub max_samples: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            lookback_delta: minutes(5),
            eval_interval: minutes(1),
            max_samples: 50_000_000,
        }
    }
}

/// Maximum number of resolution points (steps) in one range or subquery series.
/// Prometheus rejects a query whose `(end - start) / step + 1` is more than this
/// limit. The limit stops an abusive resolution, for example
/// `last_over_time(up[1000d:1ms])`, before the per-step loop runs.
pub const MAX_RESOLUTION_POINTS: u64 = 11_000;

/// Returns the resolution-point count `(end_ms - start_ms) / step + 1`.
///
/// The count is for a range or subquery grid. This function rejects an abusive
/// resolution before any per-step evaluation runs. It applies the cap to the
/// interval count `(end - start) / step`. That matches the Prometheus
/// `(end-start)/step > 11000` rule, and it matches the HTTP front gate
/// byte-for-byte in error type, status, and message. A query that the gate
/// admits is therefore never rejected again by this backstop.
///
/// # Errors
///
/// Returns [`PromqlError::Plan`] (HTTP 400 `bad_data`) when `step` is not
/// positive. Returns [`PromqlError::Plan`] (HTTP 400 `bad_data`) when the
/// interval count is more than [`MAX_RESOLUTION_POINTS`].
pub fn check_resolution_points(start_ms: i64, end_ms: i64, step: Time) -> Result<u64> {
    let step_ms = step.millis_i64();
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
    /// Range start, an epoch-millisecond instant.
    pub(super) start_ms: i64,
    /// Range end, an epoch-millisecond instant.
    pub(super) end_ms: i64,
    /// Grid resolution. This value is an extent, not an instant like the two
    /// bounds.
    pub(super) step: Time,
}

#[cfg(feature = "experimental-functions")]
tokio::task_local! {
    pub(super) static QUERY_RANGE_CONTEXT: QueryRangeContext;
}

tokio::task_local! {
    /// The `[start, end]` bounds of the active range query. The per-step planner
    /// range driver ([`PromqlEngine::eval_range_via_planner_scoped`]) scopes
    /// them. A bare top-level selector with an `@ start()` or `@ end()` modifier
    /// then resolves those bounds to the range bounds of the query, as
    /// Prometheus does, and the planner still evaluates the selector at each grid
    /// step. This task-local is absent for an instant query. There, `@ start()`
    /// and `@ end()` are invalid, and the selector planner raises the same hard
    /// error as the interpreter.
    static AT_MODIFIER_BOUNDS: AtModifierBounds;
}

/// Returns the range bounds in scope for `@ start()` and `@ end()` resolution.
///
/// Returns `None` outside a range query, that is, in an instant query.
fn current_at_modifier_bounds() -> Option<AtModifierBounds> {
    AT_MODIFIER_BOUNDS.try_with(|bounds| *bounds).ok()
}

struct RangeEval {
    series: Vec<RangeSeries>,
    end_ms: i64,
    range: Time,
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
