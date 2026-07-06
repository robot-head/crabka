use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

use crate::result::Annotations;

tokio::task_local! {
    /// Per-query annotation sink. Scoped once at each public query entry point
    /// so the deeply recursive evaluation path can record warnings/infos without
    /// threading a collector argument through every call site.
    pub(crate) static ANNOTATIONS: RefCell<Annotations>;
}

/// Record a `PromQL warning:`-class annotation for the current query, if a sink
/// is in scope. No-op outside a scoped query (e.g. unit tests calling internals
/// directly), so emission is always safe.
pub(super) fn emit_warning(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().warn(message));
}

/// Record a `PromQL info:`-class annotation for the current query, if a sink is
/// in scope. See `emit_warning`.
pub(super) fn emit_info(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().info(message));
}

/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text for `metric`.
fn mixed_classic_native_warning(metric: &str) -> String {
    format!(
        "PromQL warning: vector contains a mix of classic and native histograms for metric name {metric:?}"
    )
}

/// Exact Prometheus `InvalidQuantileWarning` text for a `quantile` /
/// `quantile_over_time` phi outside `[0, 1]` (or NaN). Like the
/// `histogram_quantile` family, Prometheus does NOT abort on a bad phi: it
/// returns signed `+/-Inf` / `NaN` and raises this warning. `got` renders through
/// the canonical Prometheus float formatter, matching Go's `%v`.
pub(super) fn invalid_quantile_warning(got: f64) -> String {
    format!(
        "PromQL warning: quantile value should be between 0 and 1, got {}",
        crate::http_api::format_sample_value(got)
    )
}

/// Whether `phi` is a valid quantile in `[0, 1]`. An out-of-range or NaN phi is
/// still evaluated (Prometheus returns `+/-Inf`/`NaN` + an
/// `InvalidQuantileWarning` rather than erroring); this only gates the warning.
pub(super) fn is_valid_quantile(phi: f64) -> bool {
    (0.0..=1.0).contains(&phi)
}

/// Emit one `MixedClassicNativeHistogramsWarning` per group key that carried
/// both a classic and a native histogram for the same labelset.
pub(super) fn warn_mixed_histograms(
    mixed_keys: &BTreeSet<String>,
    names: &BTreeMap<String, String>,
) {
    for key in mixed_keys {
        let metric = names.get(key).map_or("", String::as_str);
        emit_warning(mixed_classic_native_warning(metric));
    }
}

/// Exact Prometheus `InvalidRatioWarning` text.
///
/// Rust's `f64` `Display` matches Go's `%g` for the integral and one-decimal
/// ratios this annotation reports (`1` for `1.0`, `1.1` for `1.1`, `-1` for
/// `-1.0`), so it renders the corpus-asserted text byte-for-byte.
#[cfg(feature = "experimental-functions")]
pub(super) fn invalid_ratio_warning(got: f64, capped_to: f64) -> String {
    format!(
        "PromQL warning: ratio value should be between -1 and 1, got {got}, capping to {capped_to}"
    )
}

/// Exact Prometheus `IncompatibleTypesInBinOpInfo` text for an operator applied
/// to incompatible operand sample types (e.g. a histogram and a float).
pub(super) fn incompatible_types_in_binop_info(
    lhs_type: &str,
    operator: &str,
    rhs_type: &str,
) -> String {
    format!(
        "PromQL info: incompatible sample types encountered for binary operator {operator:?}: {lhs_type} {operator} {rhs_type}"
    )
}
