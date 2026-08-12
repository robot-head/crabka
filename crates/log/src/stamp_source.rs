//! The injected stamp source. It is the seam through which a partition's log
//! gets the additional internal `TimestampSource` coordinate that it records
//! in the `.stampindex` sidecar.
//!
//! The trait is deliberately tiny, and it lives in `crabka-log` rather than in
//! the broker. The log owns both the append seam that folds stamps in and the
//! `.stampindex` that they land in. The seam therefore stays here, so tests
//! can check the log's own stamp behavior with a deterministic in-crate
//! source, and the log needs no dependency edge onto a real clock. The broker
//! injects the tenant source when internal stamping is enabled. A log with no
//! injected source stamps nothing and behaves exactly as before.

use std::fmt::Debug;

/// A source of monotonic internal stamps folded into a partition's
/// `.stampindex`. The log calls [`StampSource::next_stamp`] for each
/// non-transactional batch at append time and for each pure-Kafka transaction
/// when its commit marker is durable. A cross-domain coordinator supplies its
/// own commit stamp, which the log folds into this source with
/// [`StampSource::observe`].
///
/// Implementations use interior mutability, because the log holds the source
/// behind a shared `Arc` and stamps through `&mut self` appends. They must be
/// cheap. They must never read or alter the wire-exact `.log` bytes, the
/// offset assignment, the LSO, or the high-watermark. The stamp is an
/// additional server-side coordinate only.
pub trait StampSource: Debug + Send + Sync {
    /// Return the next stamp.
    fn next_stamp(&self) -> u64;

    /// Fold an externally allocated commit stamp into this source so every
    /// later allocation is greater than it.
    ///
    /// Centralized sources can keep the default no-op because they are the
    /// sole timestamp authority. Distributed clocks override this with their
    /// Lamport/HLC receive rule.
    fn observe(&self, _stamp: u64) {}
}

#[cfg(any(test, feature = "test-helpers"))]
mod test_sources {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::StampSource;

    /// A deterministic monotonic stamp source for tests. It yields `start`,
    /// `start + step`, `start + 2*step`, and so on, on successive calls. The
    /// sequence is fully predictable, so tests can assert the exact stamps
    /// that a sequence of appends writes into the `.stampindex`.
    #[derive(Debug)]
    pub struct MonotonicStampSource {
        next: AtomicU64,
        step: u64,
    }

    impl MonotonicStampSource {
        /// Create a source that yields `start` first and then advances by
        /// `step` on each later call.
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

        fn observe(&self, stamp: u64) {
            let next = stamp.saturating_add(self.step);
            self.next.fetch_max(next, Ordering::Relaxed);
        }
    }
}

#[cfg(any(test, feature = "test-helpers"))]
pub use test_sources::MonotonicStampSource;
