//! Domain newtypes for the `LogQL` front-end.
//!
//! These wrap the bare `i64` / `u64` / `String` values that recur in the query
//! AST so that two same-typed values with different meanings — a range duration
//! and a query offset, a quantile numerator and its denominator, an extraction
//! destination and its source — cannot be transposed at a call site and still
//! compile.

use derive_more::{Display, From, Into};

/// A range-selector window, in nanoseconds (the `[5m]` in a metric query).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DurationNanos(pub i64);

/// A query time offset, in nanoseconds (the `offset 1h` in a metric query).
///
/// May be negative, so it is a distinct type from [`DurationNanos`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct OffsetNanos(pub i64);

/// The numerator of a reduced quantile fraction (e.g. `3` in `3/4`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct QuantileNumerator(pub u64);

/// The denominator of a reduced quantile fraction (e.g. `4` in `3/4`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct QuantileDenominator(pub u64);

/// The extracted-field name an extraction writes into.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DestinationLabel(pub String);

/// The JSON path expression an extraction reads from (`request.headers[0]`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct JsonExpressionPath(pub String);

/// The source field a `logfmt` extraction reads from before renaming.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct SourceLabel(pub String);
