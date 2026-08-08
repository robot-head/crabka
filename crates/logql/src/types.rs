//! Domain newtypes for the `LogQL` front-end.
//!
//! These types wrap the bare `i64`, `u64`, and `String` values that recur in
//! the query AST. Two values of the same type but with different meanings can
//! thus not be transposed at a call site and still compile. Such pairs are a
//! range duration and a query offset, a quantile numerator and its denominator,
//! and an extraction destination and its source.

use derive_more::{Display, From, Into};

/// A range-selector window, in nanoseconds. This is the `[5m]` in a metric
/// query.
///
/// The type holds raw nanoseconds and not a `crabka_units::Time`. A
/// `crabka_units::Time` stores `f64` seconds, which is exact for integers below
/// 2^53 only, that is about 104 days of nanoseconds. `LogQL` admits `w` and `y`
/// duration literals well past that limit. A window written as
/// `1y2w3d4h5m6s7ms8us9ns` must round-trip to the nanosecond, and the field
/// filters compare durations exactly, so the value stays an integer.
/// `docs/uom-adoption.md` excludes nanosecond magnitudes for the same
/// reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DurationNanos(pub i64);

/// A query time offset, in nanoseconds. This is the `offset 1h` in a metric
/// query.
///
/// May be negative, so it is a distinct type from [`DurationNanos`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct OffsetNanos(pub i64);

/// The numerator of a reduced quantile fraction, for example `3` in `3/4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct QuantileNumerator(pub u64);

/// The denominator of a reduced quantile fraction, for example `4` in `3/4`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct QuantileDenominator(pub u64);

/// The extracted-field name an extraction writes into.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DestinationLabel(pub String);

/// The JSON path expression an extraction reads from, for example
/// `request.headers[0]`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct JsonExpressionPath(pub String);

/// The source field a `logfmt` extraction reads from before renaming.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct SourceLabel(pub String);
