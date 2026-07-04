//! Per-(topic, partition) producer-sequence tracking. Drives the
//! idempotent-producer dedup / out-of-order / epoch-fence checks in
//! `handlers::produce`.

use std::{collections::HashMap, sync::Arc};

use crabka_ids::PartitionIndex;
use crabka_log::ProducerId;
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::partition::LogOffset;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    /// Last absolute offset of the last accepted batch for this producer
    /// (`base_offset + last_offset_delta`). Read by
    /// [`ProducerState::truncate`] to drop entries whose batch was truncated
    /// off the log.
    pub last_offset: LogOffset,
    pub base_offset: LogOffset,
    /// Timestamp of the last accepted batch for this producer.
    #[allow(dead_code)]
    pub last_timestamp: i64,
    /// Wall-clock millis of the last `commit` that touched this entry.
    /// Used by [`ProducerState::expire_older_than`] to evict idle
    /// idempotent-producer state, matching Kafka's
    /// `producer.id.expiration.ms` (KAFKA: expire by inactivity).
    pub last_activity_ms: i64,
}

#[derive(Debug, Default)]
pub struct PartitionProducerState {
    pub entries: HashMap<ProducerId, ProducerEntry>,
}

/// Outcome of a dedup check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Producer is fresh or the sequence is one past the last commit. Caller
    /// should append, then call `commit` with the assigned base offset.
    Append,
    /// Previously-committed sequence range. Caller should respond with
    /// `error_code = NONE` and `base_offset = base_offset`.
    Duplicate { base_offset: LogOffset },
    /// `base_sequence != last_sequence + 1`. Caller responds with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER (45)`.
    OutOfOrder,
    /// `epoch < entry.epoch`. Caller responds with
    /// `INVALID_PRODUCER_EPOCH (53)`.
    Fenced,
}

/// Pure idempotent-producer dedup/ordering decision. The async `check` is a thin
/// lock-acquiring wrapper over this; extracted so it is exhaustively and
/// property-tested in isolation (see `producer_state_model.rs`).
pub(crate) fn check_pure(
    entry: Option<&ProducerEntry>,
    producer_epoch: i16,
    base_sequence: i32,
) -> Decision {
    match entry {
        None => Decision::Append,
        Some(entry) => {
            if producer_epoch < entry.epoch {
                return Decision::Fenced;
            }
            if producer_epoch > entry.epoch {
                // A bumped epoch establishes a fresh sequence baseline (restart
                // or KIP-890 per-EndTxn bump). Accept the first higher-epoch batch.
                return Decision::Append;
            }
            if base_sequence <= entry.last_sequence {
                return Decision::Duplicate {
                    base_offset: entry.base_offset,
                };
            }
            if base_sequence == entry.last_sequence + 1 {
                Decision::Append
            } else {
                Decision::OutOfOrder
            }
        }
    }
}

/// Per-partition idempotent-producer state, nested under the owning
/// topic. Keyed by partition index (`i32`, `Copy`) so per-call lookups
/// allocate nothing; the outer topic map is keyed by `String` but its
/// `get`/`entry` accept a borrowed `&str` and only allocate the owned
/// topic key on the first produce to a previously-unseen topic.
type PartitionMap = DashMap<PartitionIndex, Arc<Mutex<PartitionProducerState>>>;

#[derive(Debug, Default)]
pub struct ProducerState {
    by_topic: Arc<DashMap<String, Arc<PartitionMap>>>,
}

impl ProducerState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_topic: Arc::new(DashMap::new()),
        }
    }

    /// Decide whether to append the incoming batch.
    ///
    /// `base_sequence` is the wire `base_sequence`; `last_offset_delta` is
    /// the batch's `last_offset_delta` field. Together they imply the
    /// batch's `last_sequence = base_sequence + last_offset_delta`.
    pub async fn check(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
    ) -> Decision {
        let handle = self.handle(topic, partition);
        let s = handle.lock().await;
        let _ = last_offset_delta; // used only by the caller to compute last_sequence on commit
        check_pure(
            s.entries.get(&ProducerId(producer_id)),
            producer_epoch,
            base_sequence,
        )
    }

    /// Commit a successful append into the tracker.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: LogOffset,
        last_timestamp: i64,
    ) {
        let handle = self.handle(topic, partition);
        let mut s = handle.lock().await;
        let last_sequence = base_sequence + last_offset_delta;
        let last_offset = base_offset + i64::from(last_offset_delta);
        s.entries.insert(
            ProducerId(producer_id),
            ProducerEntry {
                epoch: producer_epoch,
                last_sequence,
                last_offset,
                base_offset,
                last_timestamp,
                last_activity_ms: crate::txn::util::now_millis(),
            },
        );
    }

    /// Drop idempotent-producer entries whose last accepted batch has been
    /// truncated off the log — i.e. `last_offset >= offset`. Called after the
    /// partition log is truncated below the recorded batch (KIP-320 divergence
    /// truncation on rejoin, or `OFFSET_OUT_OF_RANGE` reset).
    ///
    /// Without this, a producer retrying a batch from the truncated tail is
    /// deduplicated against a `base_offset` no longer in the log, and the
    /// `acks=all` HW gate (`await_hw_at_least(base_offset + delta + 1)`) waits
    /// forever for a high watermark that can never reach the truncated offset —
    /// a permanent produce stall after failover. Dropping the entry makes the
    /// retry re-append fresh instead. Mirrors Kafka's
    /// `ProducerStateManager.truncateAndReload`. Does not create state for a
    /// partition that has never been tracked.
    pub async fn truncate(&self, topic: &str, partition: PartitionIndex, offset: LogOffset) {
        let Some(parts) = self.by_topic.get(topic).map(|e| e.value().clone()) else {
            return;
        };
        let Some(handle) = parts.get(&partition).map(|e| e.value().clone()) else {
            return;
        };
        let mut s = handle.lock().await;
        s.entries.retain(|_pid, e| e.last_offset < offset);
    }

    /// Resolve (creating on miss) the per-partition state handle. The
    /// outer topic lookup borrows `&str` and only allocates an owned
    /// `String` key when the topic is seen for the first time; the inner
    /// partition lookup is keyed by `i32` and never allocates.
    fn handle(&self, topic: &str, partition: PartitionIndex) -> Arc<Mutex<PartitionProducerState>> {
        // `get` first to avoid allocating the topic `String` on the hot
        // path (the topic almost always already exists).
        let parts = if let Some(existing) = self.by_topic.get(topic) {
            existing.value().clone()
        } else {
            self.by_topic
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .value()
                .clone()
        };
        parts
            .entry(partition)
            .or_insert_with(|| Arc::new(Mutex::new(PartitionProducerState::default())))
            .value()
            .clone()
    }

    /// Read-only snapshot of every active producer entry on
    /// `(topic, partition)`. Returns an empty list when the partition
    /// has no entries — i.e. no idempotent or transactional producer
    /// has produced to it yet. Used by the
    /// `DescribeProducers` admin handler (`api_key=61`, KIP-664) to
    /// surface per-partition producer-state to admin clients
    /// (`kafka-admin --describe-producers`, etc.).
    ///
    /// The snapshot drops the mutex before returning, so callers don't
    /// hold the per-partition lock across response encoding.
    pub async fn snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
    ) -> Vec<(i64, ProducerEntry)> {
        // Cheaper to bypass `handle` (which inserts on miss): a snapshot
        // for an unknown partition should report "no producers", not
        // wire up an empty entry. The borrowed `&str` / `i32` lookups
        // allocate nothing and map a miss to an empty result.
        let Some(topic_ref) = self.by_topic.get(topic) else {
            return Vec::new();
        };
        let parts = topic_ref.value().clone();
        drop(topic_ref);
        let Some(part_ref) = parts.get(&partition) else {
            return Vec::new();
        };
        let handle = part_ref.value().clone();
        drop(part_ref);
        let state = handle.lock().await;
        // Keep the public return `i64`: unwrap the map's `ProducerId` key at the
        // snapshot boundary (the `DescribeProducers` handler writes it straight
        // into the raw-`i64` wire field).
        state
            .entries
            .iter()
            .map(|(pid, e)| (pid.get(), *e))
            .collect()
    }

    /// Snapshot of currently-active producers on `(topic, partition)`:
    /// `producer_id` → that producer's last-accepted-batch `base_offset`.
    /// A producer is "active" when `now_ms - last_activity_ms <=
    /// expiration_ms` (Kafka's `producer.id.expiration.ms` inactivity
    /// window). Expired producers are excluded.
    ///
    /// Used by the cleaner to build a `CompactionContext`: an active
    /// producer's last batch must be preserved via `RETAIN_EMPTY` even when
    /// fully compacted away, so the producer's sequence/epoch state survives.
    ///
    /// Returns an empty map for an unknown `(topic, partition)`.
    ///
    /// Called by the partition writer task's `WriterMessage::Compact`
    /// handler (the broker-wide `ProducerState` is threaded through
    /// `spawn_partition` into `partition_writer::run`) to populate the
    /// `CompactionContext::active_producers` set.
    pub async fn active_snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
        now_ms: i64,
        expiration_ms: i64,
    ) -> HashMap<i64, LogOffset> {
        // Mirror `snapshot`: avoid inserting an empty entry for an unknown
        // partition (the borrowed lookups allocate nothing on a miss).
        let Some(topic_ref) = self.by_topic.get(topic) else {
            return HashMap::new();
        };
        let parts = topic_ref.value().clone();
        drop(topic_ref);
        let Some(part_ref) = parts.get(&partition) else {
            return HashMap::new();
        };
        let handle = part_ref.value().clone();
        drop(part_ref);
        let state = handle.lock().await;
        // Public return stays `HashMap<i64, i64>`; unwrap the `ProducerId` key at
        // the boundary (the caller re-wraps into the log seam's `ProducerId`).
        state
            .entries
            .iter()
            .filter(|(_pid, e)| now_ms.saturating_sub(e.last_activity_ms) <= expiration_ms)
            .map(|(pid, e)| (pid.get(), e.base_offset))
            .collect()
    }

    /// Evict idempotent-producer entries whose last activity is older
    /// than `ttl_ms` relative to `now_ms`, mirroring Kafka's
    /// `producer.id.expiration.ms` (default `86_400_000` ms = 24h). Kafka
    /// expires by *inactivity*: an entry that keeps receiving produces
    /// is retained; one that has gone quiet past the window is dropped so
    /// the map doesn't grow unbounded.
    ///
    /// Empty partition maps (and empty topic maps) are removed once their
    /// last entry expires so stale `(topic, partition)` keys don't leak.
    /// Returns the number of producer-id entries evicted.
    ///
    /// This provides the mechanism only; the periodic caller (a broker
    /// maintenance loop) is wired separately.
    pub async fn expire_older_than(&self, now_ms: i64, ttl_ms: i64) -> usize {
        let mut evicted = 0usize;
        // Snapshot the (topic -> partition-map) refs first so we don't
        // hold a DashMap shard guard across the per-partition `.await`.
        let topics: Vec<(String, Arc<PartitionMap>)> = self
            .by_topic
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        for (topic, parts) in topics {
            let partition_refs: Vec<(PartitionIndex, Arc<Mutex<PartitionProducerState>>)> = parts
                .iter()
                .map(|e| (*e.key(), e.value().clone()))
                .collect();
            for (partition, handle) in partition_refs {
                let mut state = handle.lock().await;
                let before = state.entries.len();
                state
                    .entries
                    .retain(|_pid, entry| now_ms.saturating_sub(entry.last_activity_ms) < ttl_ms);
                evicted += before - state.entries.len();
                let now_empty = state.entries.is_empty();
                drop(state);
                if now_empty {
                    // Only drop the partition slot if it's *still* empty
                    // under the removal guard, so a concurrent commit that
                    // re-populated it isn't lost.
                    parts.remove_if(&partition, |_, h| {
                        h.try_lock().is_ok_and(|s| s.entries.is_empty())
                    });
                }
            }
            // Drop the topic slot if all its partitions are gone.
            self.by_topic.remove_if(&topic, |_, p| p.is_empty());
        }
        evicted
    }
}

#[cfg(test)]
#[path = "producer_state_model.rs"]
mod producer_state_model;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[tokio::test]
    async fn first_batch_appends() {
        let s = ProducerState::new();
        let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
        assert!(d == Decision::Append);
    }

    #[tokio::test]
    async fn next_sequence_appends() {
        let s = ProducerState::new();
        s.commit(
            "t",
            PartitionIndex(0),
            1000,
            0,
            0,
            4,
            /* base_offset */ 0,
            /* ts */ 1,
        )
        .await;
        let d = s.check("t", PartitionIndex(0), 1000, 0, 5, 2).await;
        assert!(d == Decision::Append);
    }

    #[tokio::test]
    async fn duplicate_returns_cached_offset() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(0), 1000, 0, 0, 4, 0, 1).await;
        let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
        assert!(d == Decision::Duplicate { base_offset: 0 });
    }

    #[tokio::test]
    async fn truncate_drops_dedup_entry_above_offset_so_retry_reappends() {
        // The failover-stall regression: a batch was appended at base_offset
        // 1471686 (last_offset 1471699), then the divergent tail was truncated
        // back to 1471686 on rejoin. A retry must NOT be deduplicated against
        // the now-truncated offset — otherwise the acks=all HW gate
        // (await_hw_at_least(1471700)) waits forever for a high watermark the
        // log can never reach, stalling the producer.
        let s = ProducerState::new();
        s.commit(
            "t",
            PartitionIndex(0),
            1000,
            0,
            /*base_seq*/ 0,
            /*delta*/ 13,
            1_471_686,
            1,
        )
        .await;
        assert!(
            s.check("t", PartitionIndex(0), 1000, 0, 0, 13).await
                == Decision::Duplicate {
                    base_offset: 1_471_686
                }
        );
        s.truncate("t", PartitionIndex(0), 1_471_686).await;
        assert!(
            s.check("t", PartitionIndex(0), 1000, 0, 0, 13).await == Decision::Append,
            "after truncation the retried batch must re-append, not dedup against the truncated offset"
        );
    }

    #[tokio::test]
    async fn truncate_keeps_dedup_entry_below_offset() {
        // A batch whose records survive the truncation (last_offset < offset)
        // must stay deduplicated.
        let s = ProducerState::new();
        s.commit(
            "t",
            PartitionIndex(0),
            1000,
            0,
            0,
            4,
            /*base_offset*/ 100,
            1,
        )
        .await; // last_offset 104
        s.truncate("t", PartitionIndex(0), 200).await;
        assert!(
            s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await
                == Decision::Duplicate { base_offset: 100 }
        );
    }

    #[tokio::test]
    async fn truncate_drops_dedup_entry_at_exact_offset_boundary() {
        // Truncating at an entry's last_offset removes that entry: the last
        // accepted record is no longer below the log end being retained.
        let s = ProducerState::new();
        s.commit(
            "t",
            PartitionIndex(0),
            1000,
            0,
            0,
            4,
            /*base_offset*/ 100,
            1,
        )
        .await; // last_offset 104
        s.truncate("t", PartitionIndex(0), 104).await;
        assert!(s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await == Decision::Append);
    }

    #[tokio::test]
    async fn truncate_unknown_partition_is_noop() {
        let s = ProducerState::new();
        s.truncate("never-seen", PartitionIndex(7), 0).await; // must not panic or create state
        assert!(s.snapshot("never-seen", PartitionIndex(7)).await.is_empty());
    }

    #[tokio::test]
    async fn out_of_order_when_gap() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(0), 1000, 0, 0, 4, 0, 1).await;
        // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
        let d = s.check("t", PartitionIndex(0), 1000, 0, 10, 2).await;
        assert!(d == Decision::OutOfOrder);
    }

    #[tokio::test]
    async fn lower_epoch_is_fenced() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(0), 1000, 5, 0, 4, 0, 1).await;
        let d = s.check("t", PartitionIndex(0), 1000, 4, 5, 2).await;
        assert!(d == Decision::Fenced);
    }

    /// A bumped producer epoch (same `producer_id`, higher epoch) establishes a
    /// FRESH sequence baseline: `base_sequence == 0` at the new epoch must be a
    /// fresh `Append`, NOT a `Duplicate` against the prior epoch's high-water.
    /// This is the EOS-restart path (the client resets its sequence to 0).
    ///
    /// Regression test for the cross-restart EOS data-loss bug: pre-fix, a
    /// restarted EOS producer's first record on each partition was silently
    /// deduped (echoing the old `base_offset`) while the txn's offset commit
    /// still landed, so the source offset advanced but the output record vanished.
    #[tokio::test]
    async fn higher_epoch_at_seq_zero_appends() {
        let s = ProducerState::new();
        // Epoch 5 committed sequences 0..=2 (last_sequence = 2).
        s.commit(
            "t",
            PartitionIndex(0),
            1000,
            5,
            0,
            2,
            /* base_offset */ 0,
            1,
        )
        .await;
        // Same pid, epoch 6, base_sequence 0 — a fresh write, NOT a duplicate.
        let d = s.check("t", PartitionIndex(0), 1000, 6, 0, 0).await;
        assert!(d == Decision::Append);
    }

    /// A bumped epoch that CONTINUES the sequence (`base_sequence > 0`) also
    /// appends: this is the KIP-890 (`TV_2`) per-`EndTxn` epoch-bump path, where
    /// broker bumps the epoch on every commit/abort within the SAME producer
    /// session and the client keeps its sequence counter going. The first batch
    /// at the new epoch is the baseline regardless of its `base_sequence`;
    /// same-epoch ordering resumes once it commits.
    #[tokio::test]
    async fn higher_epoch_continuing_sequence_appends() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(0), 1000, 5, 0, 2, 0, 1).await;
        // Epoch 6 (KIP-890 bump), sequence continues at 3 — still a fresh append.
        let d = s.check("t", PartitionIndex(0), 1000, 6, 3, 0).await;
        assert!(d == Decision::Append);
        // After committing the new epoch's batch, same-epoch dedup resumes.
        s.commit(
            "t",
            PartitionIndex(0),
            1000,
            6,
            3,
            0,
            /* base_offset */ 10,
            2,
        )
        .await;
        let dup = s.check("t", PartitionIndex(0), 1000, 6, 3, 0).await;
        assert!(dup == Decision::Duplicate { base_offset: 10 });
    }

    #[tokio::test]
    async fn snapshot_reports_committed_entries() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(3), 1000, 0, 0, 4, 7, 1).await;
        let snap = s.snapshot("t", PartitionIndex(3)).await;
        // `last_activity_ms` is wall-clock; copy it from the actual entry so
        // the comparison stays deterministic.
        let expected = vec![(
            1000,
            ProducerEntry {
                epoch: 0,
                last_sequence: 4,
                last_offset: 11,
                base_offset: 7,
                last_timestamp: 1,
                last_activity_ms: snap[0].1.last_activity_ms,
            },
        )];
        assert!(snap == expected);
        // Untouched partition / topic report empty without panicking.
        for (topic, partition) in [("t", PartitionIndex(0)), ("other", PartitionIndex(3))] {
            assert!(
                s.snapshot(topic, partition).await == vec![],
                "case: {topic}/{partition}"
            );
        }
    }

    #[tokio::test]
    async fn expire_evicts_only_idle_entries() {
        let s = ProducerState::new();
        // Two producers on the same partition with controlled activity
        // timestamps: we commit, then overwrite last_activity_ms directly
        // to simulate age without sleeping.
        s.commit("t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        s.commit("t", PartitionIndex(0), 2, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            let mut st = h.lock().await;
            st.entries.get_mut(&ProducerId(1)).unwrap().last_activity_ms = 1_000; // old
            st.entries.get_mut(&ProducerId(2)).unwrap().last_activity_ms = 9_000; // recent
        }
        // now = 10_000, ttl = 5_000 → pid 1 (age 9_000) expires, pid 2
        // (age 1_000) survives.
        let evicted = s.expire_older_than(10_000, 5_000).await;
        assert!(evicted == 1);
        let snap = s.snapshot("t", PartitionIndex(0)).await;
        assert!(snap.len() == 1);
        assert!(snap[0].0 == 2, "only the recently-active producer survives");
    }

    #[tokio::test]
    async fn expire_evicts_entry_at_exact_ttl_boundary() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            h.lock()
                .await
                .entries
                .get_mut(&ProducerId(1))
                .unwrap()
                .last_activity_ms = 5_000;
        }

        let evicted = s.expire_older_than(10_000, 5_000).await;
        assert!(evicted == 1);
        assert!(s.snapshot("t", PartitionIndex(0)).await.is_empty());
    }

    #[tokio::test]
    async fn active_snapshot_excludes_expired_includes_active() {
        let s = ProducerState::new();
        // pid 1: last batch base_offset 10; pid 2: base_offset 20.
        s.commit(
            "t",
            PartitionIndex(0),
            1,
            0,
            0,
            0,
            /* base_offset */ 10,
            0,
        )
        .await;
        s.commit(
            "t",
            PartitionIndex(0),
            2,
            0,
            0,
            0,
            /* base_offset */ 20,
            0,
        )
        .await;
        {
            let h = s.handle("t", PartitionIndex(0));
            let mut st = h.lock().await;
            st.entries.get_mut(&ProducerId(1)).unwrap().last_activity_ms = 1_000; // old
            st.entries.get_mut(&ProducerId(2)).unwrap().last_activity_ms = 9_500; // recent
        }
        // now = 10_000, expiration = 5_000 → pid 1 (age 9_000 > 5_000)
        // excluded; pid 2 (age 500 <= 5_000) included with its base_offset.
        let snap = s
            .active_snapshot("t", PartitionIndex(0), 10_000, 5_000)
            .await;
        let expected: HashMap<i64, i64> = [(2, 20)].into_iter().collect();
        assert!(snap == expected);
        // Unknown partition / topic → empty without panicking.
        for (topic, partition) in [("t", PartitionIndex(99)), ("nope", PartitionIndex(0))] {
            assert!(
                s.active_snapshot(topic, partition, 10_000, 5_000).await == HashMap::new(),
                "case: {topic}/{partition}"
            );
        }
    }

    #[tokio::test]
    async fn expire_drops_empty_partition_and_topic_slots() {
        let s = ProducerState::new();
        s.commit("t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            h.lock()
                .await
                .entries
                .get_mut(&ProducerId(1))
                .unwrap()
                .last_activity_ms = 0;
        }
        let evicted = s.expire_older_than(1_000_000, 1).await;
        // The empty partition and topic maps are pruned (the empty topic slot
        // must be removed), and a subsequent produce still works after pruning.
        check!(evicted == 1);
        check!(s.by_topic.get("t").is_none());
        check!(s.check("t", PartitionIndex(0), 1, 0, 0, 0).await == Decision::Append);
    }
}

#[cfg(test)]
mod fuzz {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::{Decision, ProducerEntry, check_pure};

    proptest! {
        /// Large-N randomized submit sequences over `check_pure`: the
        /// accepted-append log per epoch is a contiguous, duplicate-free,
        /// monotonic prefix; a lower epoch is fenced; a higher epoch resets the
        /// baseline. Complements the exhaustive `producer_state_model` at a scale
        /// the BFS can't reach (epoch 0..6, base_seq 0..200, up to 400 ops).
        #[test]
        fn idempotent_log_invariants(
            ops in proptest::collection::vec(
                (0i16..6, 0i32..200), // (producer_epoch, base_sequence)
                0..400usize,
            )
        ) {
            let mut entry: Option<ProducerEntry> = None;
            let mut next_offset: i64 = 0;
            // Reference: per-epoch highest accepted sequence (must stay contiguous).
            let mut hi: HashMap<i16, i32> = HashMap::new();
            for (epoch, base_seq) in ops {
                let d = check_pure(entry.as_ref(), epoch, base_seq);
                match d {
                    Decision::Append => {
                        if let Some(e) = &entry {
                            if epoch == e.epoch {
                                prop_assert_eq!(
                                    base_seq,
                                    e.last_sequence + 1,
                                    "same-epoch Append must be contiguous"
                                );
                            } else {
                                prop_assert!(epoch > e.epoch, "Append epoch must be fresh");
                            }
                        }
                        // Per-epoch contiguity: an accepted seq for a fresh epoch
                        // starts the prefix; a same-epoch accept extends it by 1.
                        if let Some(p) = hi.get(&epoch).copied() {
                            prop_assert_eq!(
                                base_seq,
                                p + 1,
                                "accepted sequence must extend the per-epoch prefix"
                            );
                        }
                        hi.insert(epoch, base_seq);
                        entry = Some(ProducerEntry {
                            epoch,
                            last_sequence: base_seq,
                            last_offset: next_offset,
                            base_offset: next_offset,
                            last_timestamp: 0,
                            last_activity_ms: 0,
                        });
                        next_offset += 1;
                    }
                    Decision::Duplicate { .. } => {
                        let e = entry.as_ref().expect("Duplicate implies an entry");
                        prop_assert_eq!(epoch, e.epoch);
                        prop_assert!(
                            base_seq <= e.last_sequence,
                            "Duplicate must be within committed range"
                        );
                    }
                    Decision::OutOfOrder => {
                        let e = entry.as_ref().expect("OutOfOrder implies an entry");
                        prop_assert_eq!(epoch, e.epoch);
                        prop_assert!(
                            base_seq > e.last_sequence + 1,
                            "OutOfOrder must be a real gap"
                        );
                    }
                    Decision::Fenced => {
                        let e = entry.as_ref().expect("Fenced implies an entry");
                        prop_assert!(epoch < e.epoch, "Fenced must be a stale epoch");
                    }
                }
            }
        }
    }
}
