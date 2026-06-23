//! Custom `DataFusion` operators used to model `PromQL` vectors.

pub mod instant_manipulate;
pub mod normalize;
pub mod planner;
pub mod range_manipulate;
pub mod series_divide;

/// Prometheus' stale-NaN marker: the IEEE-754 quiet-NaN bit pattern Prometheus
/// writes to terminate a series. Instant-vector selection drops a series whose
/// selected sample carries this exact pattern, but keeps a *genuine* NaN value
/// (any other NaN bit pattern) as a NaN sample. Both the interpreter
/// (`engine::eval_instant_selector`) and the `InstantManipulate` operator route
/// staleness decisions through [`is_stale_nan`] so the two paths agree.
pub(crate) const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// True when `value` is exactly Prometheus' stale-NaN marker (and not merely
/// some genuine NaN). Genuine NaN values must be preserved as NaN samples.
#[must_use]
pub(crate) fn is_stale_nan(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}
