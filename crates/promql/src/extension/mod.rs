//! Custom `DataFusion` operators used to model `PromQL` vectors.
//!
//! These nodes carry window widths: a step, a lookback delta, a range, and a
//! grid interval. The widths are extents, but they stay raw `i64` milliseconds
//! here instead of [`Time`](crabka_units::Time) quantities.
//! `UserDefinedLogicalNodeCore` needs `Eq` and `Hash` so that the `DataFusion`
//! planner can key on nodes and deduplicate them, and a quantity stores `f64`,
//! so a quantity can be neither. The paired `*Exec` nodes hold the same raw
//! integers, which also keeps the per-row timestamp arithmetic in integer
//! space. The seam is the planner in [`crate::planner`], which converts a `Time`
//! into milliseconds exactly once as it builds the node.

pub mod instant_manipulate;
pub mod normalize;
pub mod planner;
pub mod range_manipulate;
pub mod series_divide;

/// Prometheus' stale-NaN marker: the IEEE-754 quiet-NaN bit pattern that
/// Prometheus writes to end a series.
///
/// Instant-vector selection drops a series whose selected sample carries this
/// exact pattern. It keeps a genuine NaN value, which is any other NaN bit
/// pattern, as a NaN sample. Both the interpreter
/// (`engine::eval_instant_selector`) and the `InstantManipulate` operator route
/// staleness decisions through [`is_stale_nan`], so the two paths agree.
pub(crate) const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// True when `value` is exactly Prometheus' stale-NaN marker and not some
/// genuine NaN. Genuine NaN values must stay as NaN samples.
#[must_use]
pub(crate) fn is_stale_nan(value: f64) -> bool {
    value.to_bits() == STALE_NAN_BITS
}
