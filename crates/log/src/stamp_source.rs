//! The injected stamp source — the seam by which a partition's log obtains
//! the additional internal `TimestampSource` coordinate it records in the
//! `.stampindex` sidecar.
//!
//! The trait is deliberately tiny and lives in `crabka-log` (not the broker)
//! because the log owns both the append seam where stamps are folded in and
//! the `.stampindex` they land in. Keeping the seam here lets the log's own
//! stamp behavior be tested with a deterministic in-crate source and avoids
//! any dependency edge from the log onto a real clock. Wiring a real HLC /
//! solo-mode oracle grant client to this trait is broker-side future work; a
//! log with no injected source stamps nothing and behaves exactly as before.

use std::fmt::Debug;

/// A source of monotonic internal stamps folded into a partition's
/// `.stampindex`. [`StampSource::next_stamp`] is called once per stamped
/// offset range, at append time, strictly after the batch is durably
/// appended.
///
/// Implementations use interior mutability — the log holds the source behind
/// a shared `Arc` and stamps through `&mut self` appends — must be cheap, and
/// must never observe or alter the wire-exact `.log` bytes, offset
/// assignment, or LSO/high-watermark. The stamp is an additional server-side
/// coordinate only.
pub trait StampSource: Debug + Send + Sync {
    /// Return the next stamp. The log calls this in append (offset) order, so
    /// within a partition stamp order never contradicts offset order.
    fn next_stamp(&self) -> u64;
}

#[cfg(any(test, feature = "test-helpers"))]
mod test_sources {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::StampSource;

    /// A deterministic monotonic stamp source for tests: yields `start`,
    /// `start + step`, `start + 2*step`, … on successive calls. Because the
    /// sequence is fully predictable, tests can assert the exact stamps a
    /// sequence of appends produces in the `.stampindex`.
    #[derive(Debug)]
    pub struct MonotonicStampSource {
        next: AtomicU64,
        step: u64,
    }

    impl MonotonicStampSource {
        /// Create a source that yields `start` first and advances by `step`
        /// on each subsequent call.
        #[must_use]
        pub fn new(start: u64, step: u64) -> Self {
            Self {
                next: AtomicU64::new(start),
                step,
            }
        }
    }

    impl StampSource for MonotonicStampSource {
        fn next_stamp(&self) -> u64 {
            self.next.fetch_add(self.step, Ordering::Relaxed)
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub use test_sources::MonotonicStampSource;
