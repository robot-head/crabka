//! Custom `DataFusion` operators used to model `PromQL` vectors.
//!
//! The window widths these nodes carry — a step, a lookback delta, a range, a
//! grid interval — are extents, but they stay raw `i64` milliseconds here rather
//! than becoming [`Time`](crabka_units::Time) quantities.
//! `UserDefinedLogicalNodeCore` requires `Eq` and `Hash` so the `DataFusion`
//! planner can key on and deduplicate nodes, and a quantity stores `f64`, so it
//! can be neither. The paired `*Exec` nodes hold the same raw integers, which
//! also keeps the per-row timestamp arithmetic in integer space. The seam is the
//! planner in [`crate::planner`], which converts a `Time` into milliseconds
//! exactly once as it builds the node.

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
