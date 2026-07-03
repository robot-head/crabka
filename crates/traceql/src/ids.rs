//! Newtypes for `TraceQL`'s query-time domain scalars.
//!
//! A `TraceQL` search / metrics query carries several `i64` nanosecond values
//! that are trivially transposable in their raw form:
//!
//! - a query time *window*, a `[start, end]` pair of absolute epoch-nanosecond
//!   timestamps ([`UnixNano`]); and
//! - a metrics *step*, a nanosecond *duration* between range-query buckets
//!   ([`DurationNanos`]).
//!
//! These recur as adjacent same-typed fields in the crate-internal planner and
//! metrics machinery (`PlannerContext { start_ns, end_ns }`, `MetricsRange {
//! scan_start, scan_end, output_start, step }`, and the multi-`i64` arg lists of
//! `assemble_metrics_response` / `assemble_compare_response`). In raw `i64` form
//! a swapped start/end, or a step passed where a timestamp is expected, still
//! compiles and silently corrupts the result. Distinguishing the *instant* type
//! from the *duration* type makes the compiler reject those mix-ups.
//!
//! These are purely crate-internal: the engine's public API (`TraceqlEngine`,
//! the `SpanStore` trait) still speaks raw `i64`, and these wrappers are put on
//! at the top of a query and taken off at the storage boundary via
//! `From`/`Into`. None of them are serialized, so no `#[serde(transparent)]` is
//! required.

use derive_more::{Display, From, Into};

/// An absolute timestamp in Unix epoch nanoseconds (an *instant*).
///
/// Used for query window bounds (`start_ns`/`end_ns`), a span row's start time,
/// and the per-bucket output timestamps of a metrics range query.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into,
)]
pub(crate) struct UnixNano(pub i64);

/// A duration in nanoseconds (a *span of time*, not an instant).
///
/// Used for the metrics range-query `step`. Kept distinct from [`UnixNano`] so a
/// step can never be transposed into a timestamp position (or vice versa) in the
/// metrics-range structs and the `assemble_*` arg lists.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into,
)]
pub(crate) struct DurationNanos(pub i64);
