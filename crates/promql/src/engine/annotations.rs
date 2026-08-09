use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

use crate::result::Annotations;

tokio::task_local! {
    /// Per-query annotation sink.
    ///
    /// Each public query entry point scopes this once. The deeply recursive
    /// evaluation path can then record warnings and infos without a collector
    /// argument at every call site.
    pub(crate) static ANNOTATIONS: RefCell<Annotations>;
}

/// Records a `PromQL warning:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. A unit test that calls
/// internals directly is outside a scoped query, so a call is always safe.
pub(super) fn emit_warning(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().warn(message));
}

/// Records a `PromQL info:`-class annotation for the current query.
///
/// This function does nothing if no sink is in scope. See `emit_warning`.
pub(super) fn emit_info(message: impl Into<String>) {
    let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().info(message));
}

/// Exact Prometheus `MixedClassicNativeHistogramsWarning` text for `metric`.
fn mixed_classic_native_warning(metric: &str) -> String {
    format!(
        "PromQL warning: vector contains a mix of classic and native histograms for metric name {metric:?}"
    )
}

/// Exact Prometheus `InvalidQuantileWarning` text for a bad phi.
///
/// A bad phi is a `quantile` or `quantile_over_time` phi outside `[0, 1]`, or
/// NaN. Prometheus does not abort on a bad phi. It returns signed `+/-Inf` or
/// `NaN` and raises this warning, the same as the `histogram_quantile` family.
/// `got` renders through the canonical Prometheus float formatter, which
/// matches Go's `%v`.
pub(super) fn invalid_quantile_warning(got: f64) -> String {
    format!(
        "PromQL warning: quantile value should be between 0 and 1, got {}",
        crate::http_api::format_sample_value(got)
    )
}

/// Returns true if `phi` is a valid quantile in `[0, 1]`.
///
/// The engine still evaluates an out-of-range or NaN phi. Prometheus returns
/// `+/-Inf` or `NaN` with an `InvalidQuantileWarning` and does not error. This
/// function only gates the warning.
pub(super) fn is_valid_quantile(phi: f64) -> bool {
    (0.0..=1.0).contains(&phi)
}

/// Emits one `MixedClassicNativeHistogramsWarning` per mixed group key.
///
/// A mixed group key held both a classic and a native histogram for the same
/// label set.
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
/// ratios this annotation reports: `1` for `1.0`, `1.1` for `1.1`, and `-1` for
/// `-1.0`. The rendered text is then byte-for-byte the corpus-asserted text.
#[cfg(feature = "experimental-functions")]
pub(super) fn invalid_ratio_warning(got: f64, capped_to: f64) -> String {
    format!(
        "PromQL warning: ratio value should be between -1 and 1, got {got}, capping to {capped_to}"
    )
}

/// Exact Prometheus `IncompatibleTypesInBinOpInfo` text for incompatible operands.
///
/// An operator gets incompatible operand sample types, for example a histogram
/// and a float.
pub(super) fn incompatible_types_in_binop_info(
    lhs_type: &str,
    operator: &str,
    rhs_type: &str,
) -> String {
    format!(
        "PromQL info: incompatible sample types encountered for binary operator {operator:?}: {lhs_type} {operator} {rhs_type}"
    )
}
