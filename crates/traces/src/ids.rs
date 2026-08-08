//! Newtypes for the traces domain values that are otherwise bare primitives.
//!
//! Several helpers in this crate thread two or more same-typed `i64`s that mean
//! different things. Examples are a start/end nanosecond pair, a Jaeger
//! trace-id `(high, low)` word pair, and the
//! `(min_offset, max_offset, window_start_ns)` triple that composes a
//! deterministic object key. Bare `i64`s let a caller transpose them and still
//! compile, which is the textbook swap bug. These wrappers make the compiler
//! reject a mixed-up call site.
//!
//! All values here are pure in-memory scalars threaded through function
//! signatures. None of them are serialised, so none need
//! `#[serde(transparent)]`. The serialised WAL and Arrow span fields keep their
//! raw `i64` representation, because the swap surface is the *call site*, not
//! the stored record.
//!
//! Arithmetic runs on the inner `i64` through `.0` at the point of use, because
//! the operations cross newtype boundaries. `end - start` and the `min(...)`
//! and `max(...)` of an offset pair are two such operations. `Add` and `Sub`
//! are therefore not derived, on purpose.

use derive_more::{Display, From, Into};

/// A wall-clock timestamp in nanoseconds since the Unix epoch.
///
/// This carries the `start` and `end` bounds of a time range. They are adjacent
/// `i64`s at every call site that filters or steps over a window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct UnixNano(pub i64);

/// The high 64 bits of a 128-bit Jaeger trace id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct TraceIdHigh(pub i64);

/// The low 64 bits of a 128-bit Jaeger trace id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct TraceIdLow(pub i64);

/// The smallest Kafka log offset covered by a flushed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MinOffset(pub i64);

/// The largest Kafka log offset covered by a flushed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct MaxOffset(pub i64);

/// The earliest span `start_ns` in a flushed block-builder window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct WindowStartNs(pub i64);
