//! `Consumer::seek` sets the KIP-320-aware fetch position for a partition,
//! whether it is assigned yet or not.
//!
//! ## Why a *pending* seek rather than a direct `next_offsets` write
//!
//! A subscribe-based consumer does not own its partitions until the coordinator
//! task completes the first `JoinGroup`/`SyncGroup` round. The offset prime
//! that follows assignment writes `next_offsets` from the committed offset or
//! from the `auto.offset.reset` baseline. That prime is
//! `coordinator::prime_offsets`, plus the initial prime in `consumer::start`
//! step 5. A `seek` that wrote `next_offsets` directly would therefore be
//! *clobbered* by a prime that runs after it, for example on a restart that
//! seeks before the very first assignment lands.
//!
//! So [`Consumer::seek`] records the target in `pending_seeks` instead.
//! [`apply_pending_seeks`](Consumer::apply_pending_seeks) then materialises the
//! position into `next_offsets` at the top of every [`poll`](Consumer::poll),
//! *after* the prime but *before* the code builds the `FetchRequest` for that
//! poll. The consequences are all load-bearing:
//!
//! - **No skipped records / no data gap.** The sought value is the next offset
//!   to fetch. The very next `Fetch` starts there, so nothing between the sought
//!   offset and the prior position is ever passed over.
//! - **No pre-seek records delivered.** The seek lands before the code builds
//!   the fetch, so `poll` never fetches a record below the sought offset, and
//!   never returns one. There is no window in which a stale `next_offsets`
//!   drives a fetch that the caller asked to skip past.
//! - **The prime cannot win.** A prime for an assigned partition runs while no
//!   poll is in progress, and the pending entry outlives it. The entry is
//!   consumed only once it has been written into `next_offsets` for a
//!   *currently assigned* partition. A seek issued before assignment waits in
//!   `pending_seeks` until its partition appears in `assigned`.
//!
//! This mirrors the JVM client's `seek`: a one-shot position set that takes
//! effect on the next fetch and is not re-applied after a later rebalance.

use crate::{consumer::Consumer, error::ConsumerError};

impl Consumer {
    /// Set the next offset `poll` will fetch for `(topic, partition)`.
    ///
    /// `offset` is the **next** offset to read, that is `last_consumed + 1`, the
    /// same convention as a committed group offset. Pass `0` to re-read a
    /// partition from the beginning.
    ///
    /// The seek is *pending*. The next [`poll`](Consumer::poll) that sees the
    /// partition as assigned materialises it into the live fetch position at its
    /// start. This is deliberate. A subscribe-based consumer assigns partitions
    /// asynchronously on `JoinGroup`/`SyncGroup`, and the offset prime that
    /// follows assignment would clobber an eager direct write. A seek on a
    /// partition that the consumer does not hold yet is therefore valid. The
    /// method holds the target until the partition is assigned, then applies it
    /// before any record is fetched for that partition. No record below `offset`
    /// is ever delivered, and none above it is skipped. A second seek on the
    /// same partition before the next poll replaces the prior target.
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
    /// [`poll`](Consumer::poll) calls this at its very top, after the
    /// coordinator has had its chance to prime offsets on assignment and before
    /// the code builds the fetch. A seek therefore always wins over the prime,
    /// and the sought offset is the one fetched.
    ///
    /// The lock order matches `poll`'s, `next_offsets` then `positions`, so this
    /// can never deadlock against a concurrent rebalance. A seek also clears the
    /// partition's KIP-320 `offset_epoch` and `awaiting_validation` state. The
    /// caller asserts a fresh position with no consumed-epoch history to
    /// validate against, so a stale epoch must not wedge the partition in
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
            p.offset_epoch = crabka_ids::LeaderEpoch(-1);
            p.awaiting_validation = false;
            false
        });
    }
}
