//! Per-range local sequence with a published closed timestamp.
//!
//! The single-shard-commit bypass gives every range a monotone allocator in the
//! same packed-`u64` timestamp domain as the global source. A transaction that
//! reads and writes exactly one range draws `start_ts` and `commit_ts` from that
//! range's [`LocalSequence`] instead of the tenant-wide timestamp source, so the
//! global clock's load tracks cross-range traffic and not total writes. The
//! versions written are indistinguishable in storage from globally stamped ones.
//!
//! At recovery the range seeds the sequence from its durable horizon, and the
//! Lamport receive rule advances it. The range
//! [`observe`](LocalSequence::observe)s every globally stamped write it applies,
//! so local allocations always exceed every global timestamp the range has seen.
//! Under HLC mode this fold is exactly the HLC receive rule, so the local
//! sequence is a range-scoped HLC.
//!
//! The sequence also publishes a **closed timestamp**, which is a watermark it
//! promises never to commit at or below again. This range can serve a
//! cross-range read at `read_ts` after its closed timestamp reaches `read_ts`.
//! The closed-timestamp discipline is the safety property the whole bypass rests
//! on. See [`LocalSequence::publish_closed_timestamp`].

use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use crate::timestamp_txn::{
    CommitTimestamp, MonotonicTimestampAllocator, ReadTimestamp, TimestampTransactionId,
    TimestampTxnError,
};

/// A range-local monotone timestamp allocator plus its published closed
/// timestamp.
///
/// A [`Mutex`] that guards the underlying [`MonotonicTimestampAllocator`]
/// serializes allocation. This matches `LocalTimestampSource`'s idiom for a
/// per-range allocator. The critical section is a couple of `checked_add`s that
/// cannot panic, so the mutex never poisons. The closed timestamp lives in a
/// separate [`AtomicU64`], so the read path can load the watermark without
/// contention on the allocation lock. The code *writes* the watermark only while
/// it holds that lock, which keeps its advance atomic with respect to
/// reservation.
#[derive(Debug)]
pub struct LocalSequence {
    allocator: Mutex<MonotonicTimestampAllocator>,
    closed_ts: AtomicU64,
}

impl LocalSequence {
    /// Seed a sequence from a range's durable `horizon`.
    ///
    /// The next allocation then strictly exceeds `horizon`, and the closed
    /// timestamp starts at `horizon`. Everything at or below the horizon is
    /// already settled and can never be committed at again.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampTxnError::TimestampExhausted`] when `horizon` is
    /// `u64::MAX`, because no timestamp above it is left to allocate.
    pub fn seeded_at(horizon: u64) -> Result<Self, TimestampTxnError> {
        let first = horizon
            .checked_add(1)
            .ok_or(TimestampTxnError::TimestampExhausted)?;
        Ok(Self {
            allocator: Mutex::new(MonotonicTimestampAllocator::starting_at(first)?),
            closed_ts: AtomicU64::new(horizon),
        })
    }

    /// The next timestamp the sequence would allocate, without allocating it.
    #[must_use]
    pub fn next_timestamp(&self) -> u64 {
        self.locked().next_timestamp()
    }

    /// Allocate a start timestamp naming a single-shard transaction on this
    /// range.
    ///
    /// # Errors
    ///
    /// Returns an error when the allocator has exhausted the `u64` domain.
    pub fn allocate_transaction_id(&self) -> Result<TimestampTransactionId, TimestampTxnError> {
        self.locked().allocate_transaction_id()
    }

    /// Allocate a read timestamp on this range.
    ///
    /// # Errors
    ///
    /// Returns an error when the allocator has exhausted the `u64` domain.
    pub fn allocate_read_timestamp(&self) -> Result<ReadTimestamp, TimestampTxnError> {
        self.locked().allocate_read_timestamp()
    }

    /// Allocate a commit timestamp strictly greater than `start_ts`.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampTxnError::CommitNotAfterStart`] or
    /// [`TimestampTxnError::TimestampExhausted`] from the underlying allocator.
    pub fn allocate_commit_after(
        &self,
        start_ts: TimestampTransactionId,
    ) -> Result<CommitTimestamp, TimestampTxnError> {
        self.locked().allocate_commit_after(start_ts)
    }

    /// Fold an applied global stamp into the sequence with the Lamport receive
    /// rule, so every future allocation strictly exceeds `ts`.
    ///
    /// The sequence never regresses. A sequence that has already issued a newer
    /// stamp stays unchanged. The fold is best-effort at `u64` exhaustion. The
    /// only case where the fold cannot advance is also the case where the
    /// allocator can serve no further grant, so there is nothing to protect
    /// against.
    pub fn observe(&self, ts: u64) {
        let _ = self.locked().advance_past(ts);
    }

    /// The current closed timestamp: the watermark this range promises never to
    /// commit at or below again.
    #[must_use]
    pub fn closed_timestamp(&self) -> u64 {
        self.closed_ts.load(Ordering::Acquire)
    }

    /// Raise the closed timestamp toward `target`, and return the resulting
    /// watermark.
    ///
    /// # Safety invariant
    ///
    /// The closed timestamp must never reach or exceed a value the sequence
    /// might still allocate a commit at. This method reserves *strictly above*
    /// `target` before it publishes `target`. It advances the allocator so that
    /// `next_timestamp() > target`, and only then raises the watermark to
    /// `target`. Every commit the sequence allocates after that comes from
    /// `next_timestamp()` or higher, so `commit_ts > closed_timestamp()` holds
    /// for all time.
    ///
    /// With `fetch_max`, the watermark is monotone, so it only increases, and it
    /// always stays strictly below the next allocatable timestamp. A cross-range
    /// reader relies on that property when it serves a `read_ts` this range has
    /// closed. A watermark published earlier was reserved above under the same
    /// lock, and the allocator only moves forward, so it too stays strictly
    /// below the current boundary.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampTxnError::TimestampExhausted`] when `target` is
    /// `u64::MAX`, because no timestamp above it is left to reserve. The
    /// watermark stays unchanged.
    pub fn publish_closed_timestamp(&self, target: u64) -> Result<u64, TimestampTxnError> {
        let mut allocator = self.locked();
        // Reserve strictly above `target` before the watermark can cover it.
        allocator.advance_past(target)?;
        let previous = self.closed_ts.fetch_max(target, Ordering::AcqRel);
        Ok(previous.max(target))
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, MonotonicTimestampAllocator> {
        self.allocator
            .lock()
            .expect("local sequence mutex is never held across a panicking section")
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn txn(raw: u64) -> TimestampTransactionId {
        TimestampTransactionId::new(raw).expect("non-zero transaction id")
    }

    #[test]
    fn seeded_allocation_strictly_exceeds_horizon() {
        let sequence = LocalSequence::seeded_at(100).expect("seed");
        assert!(sequence.allocate_transaction_id().expect("start").get() == 101);
        assert!(sequence.closed_timestamp() == 100);
    }

    #[test]
    fn seed_at_u64_max_is_exhausted() {
        assert!(matches!(
            LocalSequence::seeded_at(u64::MAX),
            Err(TimestampTxnError::TimestampExhausted)
        ));
    }

    #[test]
    fn allocation_is_monotone() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        let mut previous = 0;
        for _ in 0..8 {
            let next = sequence.allocate_transaction_id().expect("start").get();
            assert!(next > previous);
            previous = next;
        }
    }

    #[test]
    fn commit_is_strictly_after_start() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        let start = sequence.allocate_transaction_id().expect("start");
        let commit = sequence.allocate_commit_after(start).expect("commit");
        assert!(commit.get() > start.get());
    }

    #[test]
    fn commit_after_a_higher_external_start_jumps_past_it() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        let start = txn(500);
        let commit = sequence.allocate_commit_after(start).expect("commit");
        assert!(commit.get() > 500);
    }

    #[test]
    fn observe_advances_future_allocations() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        sequence.observe(1_000);
        assert!(sequence.allocate_transaction_id().expect("start").get() > 1_000);
    }

    #[test]
    fn observe_never_regresses() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        sequence.observe(1_000);
        // A smaller observation cannot pull the sequence backwards.
        sequence.observe(5);
        assert!(sequence.allocate_transaction_id().expect("start").get() > 1_000);
    }

    #[test]
    fn closed_timestamp_is_monotone() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        assert!(sequence.publish_closed_timestamp(10).expect("publish") == 10);
        assert!(sequence.publish_closed_timestamp(50).expect("publish") == 50);
        // Publishing a lower target cannot lower the watermark.
        assert!(sequence.publish_closed_timestamp(20).expect("publish") == 50);
        assert!(sequence.closed_timestamp() == 50);
    }

    #[test]
    fn publish_at_u64_max_is_exhausted_and_leaves_watermark() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");
        sequence.publish_closed_timestamp(10).expect("publish");
        assert!(
            sequence.publish_closed_timestamp(u64::MAX)
                == Err(TimestampTxnError::TimestampExhausted)
        );
        assert!(sequence.closed_timestamp() == 10);
    }

    #[test]
    fn closed_timestamp_touches_but_never_reaches_the_next_commit_boundary() {
        let sequence = LocalSequence::seeded_at(0).expect("seed");

        // Publishing right up to the current boundary witnesses the tightest
        // safe watermark: exactly one below the next allocatable timestamp.
        let boundary = sequence.next_timestamp() - 1;
        let closed = sequence
            .publish_closed_timestamp(boundary)
            .expect("publish");
        assert!(closed == boundary);
        assert!(closed == sequence.next_timestamp() - 1);
        assert!(closed < sequence.next_timestamp());

        // A commit allocated from that boundary is strictly above the watermark.
        let start = sequence.allocate_transaction_id().expect("start");
        let commit = sequence.allocate_commit_after(start).expect("commit");
        assert!(commit.get() > sequence.closed_timestamp());
    }

    #[test]
    fn closed_timestamp_stays_below_next_allocatable_across_interleaving() {
        let sequence = LocalSequence::seeded_at(5).expect("seed");

        // Each step mixes a global observe, a closed-timestamp publish, and a
        // fresh allocation. The load-bearing safety property is that the
        // watermark is always strictly below anything the sequence could still
        // allocate a commit at, so every commit taken *after* a publish lands
        // strictly above the watermark that publish set.
        let publish_targets = [3_u64, 7, 7, 40, 200, 199, 500];
        let observe_stamps = [0_u64, 20, 0, 100, 0, 1_000, 0];
        let mut highest_closed = sequence.closed_timestamp();

        for step in 0..publish_targets.len() {
            if observe_stamps[step] != 0 {
                sequence.observe(observe_stamps[step]);
            }

            let closed = sequence
                .publish_closed_timestamp(publish_targets[step])
                .expect("publish");

            // Strictly below the next allocatable timestamp — the boundary the
            // whole bypass rests on.
            assert!(closed < sequence.next_timestamp());
            // Monotone: the watermark only ever climbs.
            assert!(closed >= highest_closed);
            highest_closed = closed;

            // Any commit allocated now is strictly above the published watermark.
            let start = sequence.allocate_transaction_id().expect("start");
            let commit = sequence.allocate_commit_after(start).expect("commit");
            assert!(u64::from(commit) > closed);
        }
    }
}
