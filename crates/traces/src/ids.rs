//! Newtypes for the traces domain values that are otherwise bare primitives.
//!
//! Several helpers in this crate thread two or more same-typed `i64`s that mean
//! different things — a start/end nanosecond pair, a Jaeger trace-id `(high,
//! low)` word pair, and the `(min_offset, max_offset, window_start_ns)` triple
//! that composes a deterministic object key. Bare `i64`s let a caller transpose
//! them and still compile (the textbook swap bug). These wrappers make the
//! compiler reject a mixed-up call site.
//!
//! All values here are pure in-memory scalars threaded through function
//! signatures — none are serialised, so no `#[serde(transparent)]` is required.
//! (The serialised WAL/Arrow span fields keep their raw `i64` representation;
//! the swap surface is the *call site*, not the stored record.) Arithmetic is
//! done on the inner `i64` (`.0`) at the point of use because operations cross
//! newtype boundaries (`end - start`, `min(...)`/`max(...)` of an offset pair),
//! so `Add`/`Sub` are deliberately not derived.

use derive_more::{Display, From, Into};

/// A wall-clock timestamp in nanoseconds since the Unix epoch.
///
/// Used for the `start`/`end` bounds of a time range, which are adjacent `i64`s
/// at every call site that filters or steps over a window.
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
