//! Newtypes for `TraceQL`'s query-time domain scalars.
//!
//! A `TraceQL` search or metrics query carries several `i64` nanosecond values.
//! In raw form it is easy to transpose them:
//!
//! - a query time *window*, a `[start, end]` pair of absolute epoch-nanosecond
//!   timestamps ([`UnixNano`]); and
//! - a metrics *step*, a nanosecond *duration* between range-query buckets
//!   ([`DurationNanos`]).
//!
//! These values recur as adjacent same-typed fields in the crate-internal
//! planner and metrics machinery: `PlannerContext { start_ns, end_ns }`,
//! `MetricsRange { scan_start, scan_end, output_start, step }`, and the
//! multi-`i64` arg lists of `assemble_metrics_response` and
//! `assemble_compare_response`. In raw `i64` form, a swapped start and end, or
//! a step passed where a timestamp belongs, still compiles and silently
//! corrupts the result. A distinct *instant* type and *duration* type make the
//! compiler reject those mix-ups.
//!
//! These types are purely crate-internal. The public API of the engine still
//! speaks raw `i64`, both `TraceqlEngine` and the `SpanStore` trait. The crate
//! puts these wrappers on at the top of a query and takes them off at the
//! storage boundary with `From`/`Into`. None of them are serialized, so they
//! need no `#[serde(transparent)]`.

use derive_more::{Display, From, Into};

/// An *instant*: an absolute timestamp in Unix epoch nanoseconds.
///
/// The crate uses this type for the query window bounds `start_ns` and
/// `end_ns`, for a span row's start time, and for the per-bucket output
/// timestamps of a metrics range query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub(crate) struct UnixNano(pub i64);

/// A duration in nanoseconds: a *span of time*, not an instant.
///
/// The crate uses this type for the metrics range-query `step`. It stays
/// distinct from [`UnixNano`], so a step can never take a timestamp position,
/// and a timestamp can never take a step position. This holds for the
/// metrics-range structs and for the `assemble_*` arg lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub(crate) struct DurationNanos(pub i64);
