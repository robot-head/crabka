//! `Consumer::seek` — set the fetch position for an assigned (or
//! yet-to-be-assigned) partition, KIP-320-aware.
//!
//! ## Why a *pending* seek rather than a direct `next_offsets` write
//!
//! A subscribe-based consumer does not own its partitions until the coordinator
//! task completes the first `JoinGroup`/`SyncGroup` round, and the offset prime
//! that follows assignment (`coordinator::prime_offsets`, and the initial prime
//! in `consumer::start` step 5) writes `next_offsets` from the committed offset
//! or the `auto.offset.reset` baseline. A `seek` that wrote `next_offsets`
//! directly would therefore be *clobbered* by a prime that runs after it — for
//! example a restart that seeks before the very first assignment lands.
//!
//! So [`Consumer::seek`] records the target in `pending_seeks` instead, and the
//! position is materialised into `next_offsets` at the top of every
//! [`poll`](Consumer::poll) (via [`apply_pending_seeks`](Consumer::apply_pending_seeks)),
//! *after* the prime, but *before* the `FetchRequest` for that poll is built.
//! Consequences, all load-bearing:
//!
//! - **No skipped records / no data gap.** The sought value is the next offset
//!   to fetch; the very next `Fetch` starts there, so nothing between the sought
//!   offset and the prior position is ever passed over.
//! - **No pre-seek records delivered.** Because the seek lands before the fetch
//!   is built, `poll` never fetches — let alone returns — a record below the
//!   sought offset. There is no window in which a stale `next_offsets` drives a
//!   fetch the caller asked to skip past.
//! - **The prime cannot win.** A prime for an assigned partition runs while we
//!   hold no poll, and the pending entry outlives it: the entry is consumed only
//!   once it has been written into `next_offsets` for a *currently assigned*
//!   partition. A seek issued before assignment simply waits in `pending_seeks`
//!   until its partition appears in `assigned`.
//!
//! This mirrors the JVM client's `seek`: a one-shot position set that takes
//! effect on the next fetch and is not re-applied after a later rebalance.

use crate::consumer::Consumer;
use crate::error::ConsumerError;

impl Consumer {
    /// Set the next offset `poll` will fetch for `(topic, partition)`.
    ///
    /// `offset` is the **next** offset to read — i.e. `last_consumed + 1`, the
    /// same convention as a committed group offset. Pass `0` to re-read a
    /// partition from the beginning.
    ///
    /// The seek is *pending*: it is materialised into the live fetch position at
    /// the start of the next [`poll`](Consumer::poll) that observes the partition
    /// as assigned. This is deliberate — a subscribe-based consumer assigns
    /// partitions asynchronously on `JoinGroup`/`SyncGroup`, and the offset prime
    /// that follows assignment would clobber an eager direct write. Seeking a
    /// partition the consumer is not (yet) assigned is therefore valid: the
    /// target is held until the partition is assigned, then applied before any
    /// record is fetched for it — so no record below `offset` is ever delivered
    /// and none above it is skipped. Re-seeking the same partition before the
    /// next poll replaces the prior target.
    ///
    /// Like the JVM client's `seek`, this is a one-shot position set: once
    /// applied it is not re-applied after a subsequent rebalance.
    ///
    /// # Errors
    ///
    /// Returns [`ConsumerError::InvalidOffset`] if `offset` is negative.
    #[tracing::instrument(
        name = "consumer.seek",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, topic = tracing::field::Empty, partition, offset),
        err
    )]
    pub async fn seek(
        &self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<(), ConsumerError> {
        if offset < 0 {
            return Err(ConsumerError::InvalidOffset(offset));
        }
        let topic = topic.into();
        tracing::Span::current().record("topic", tracing::field::display(&topic));
        self.pending_seeks
            .lock()
            .await
            .insert((topic, partition), offset);
        Ok(())
    }

    /// Materialise any pending [`seek`](Self::seek) whose partition is currently
    /// assigned into `next_offsets`, then drop it from the pending set.
    ///
    /// Called at the very top of [`poll`](Consumer::poll), after the coordinator
    /// has had its chance to prime offsets on assignment and before the fetch is
    /// built — so a seek always wins over the prime, and the sought offset is the
    /// one fetched.
    ///
    /// Lock order matches `poll`'s (`next_offsets` then `positions`) so this can
    /// never deadlock against a concurrent rebalance. A seek also clears the
    /// partition's KIP-320 `offset_epoch`/`awaiting_validation` state: the caller
    /// is asserting a fresh position with no consumed-epoch history to validate
    /// against, so a stale epoch must not wedge the partition in
    /// `validate_positions`.
    #[tracing::instrument(
        name = "consumer.apply_pending_seeks",
        level = "debug",
        skip_all,
        fields(group_id = %self.group_id, pending = tracing::field::Empty)
    )]
    pub(crate) async fn apply_pending_seeks(&self) {
        // Cheap fast path: steady-state polls have no pending seeks, so avoid
        // taking the assigned/offsets/positions locks on every poll.
        if self.pending_seeks.lock().await.is_empty() {
            return;
        }
        let assigned: std::collections::HashSet<(String, i32)> =
            self.assigned.lock().await.iter().cloned().collect();
        let mut pending = self.pending_seeks.lock().await;
        if pending.is_empty() {
            return;
        }
        tracing::Span::current().record("pending", pending.len());
        let mut offsets = self.next_offsets.lock().await;
        let mut positions = self.positions.lock().await;
        pending.retain(|key, &mut offset| {
            if !assigned.contains(key) {
                // Not assigned yet — hold the seek until the partition lands.
                return true;
            }
            offsets.insert(key.clone(), offset);
            // Reset epoch state: a sought position has no prior consumed-epoch
            // to validate, and any stale `awaiting_validation` from before the
            // seek would otherwise gate the partition (poll.rs skips
            // awaiting_validation partitions, validate_positions skips
            // offset_epoch < 0).
            let p = positions.entry(key.clone()).or_default();
            p.offset_epoch = -1;
            p.awaiting_validation = false;
            false
        });
    }
}
