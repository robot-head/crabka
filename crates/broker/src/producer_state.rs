//! Per-(topic, partition) producer-sequence tracking. Drives the
//! idempotent-producer dedup / out-of-order / epoch-fence checks in
//! `handlers::produce`.

use std::{collections::HashMap, sync::Arc};

use crabka_ids::PartitionIndex;
use crabka_log::ProducerId;
use crabka_protocol::records::{decrement_sequence, increment_sequence};
use crabka_units::{Time, convert::TimeExt as _};
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::partition::LogOffset;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    /// Last absolute offset of the last accepted batch for this producer
    /// (`base_offset + last_offset_delta`). [`ProducerState::truncate`] reads
    /// it to drop entries whose batch was truncated off the log.
    pub last_offset: LogOffset,
    pub base_offset: LogOffset,
    /// Timestamp of the last accepted batch for this producer.
    pub last_timestamp: i64,
    /// Wall-clock millis of the last `commit` that touched this entry.
    /// [`ProducerState::expire_older_than`] uses it to evict idle
    /// idempotent-producer state. This matches Kafka's
    /// `producer.id.expiration.ms`, which expires by inactivity.
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
    /// Exact retry of the last committed sequence range. Caller should respond with
    /// `error_code = NONE` and `base_offset = base_offset`.
    Duplicate { base_offset: LogOffset },
    /// `base_sequence` is not the wrapped successor of `last_sequence` and
    /// does not exactly match the last committed batch. Caller responds with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER (45)`.
    OutOfOrder,
    /// `epoch < entry.epoch`. Caller responds with
    /// `INVALID_PRODUCER_EPOCH (47)`.
    Fenced,
}

/// Pure idempotent-producer dedup/ordering decision.
///
/// The async `check` is a thin lock-acquiring wrapper over this function. The
/// decision is a separate function so that the tests can exhaustively test and
/// property-test it in isolation. The caller has already validated that the
/// two sequence fields are non-negative. See `producer_state_model.rs`.
pub(crate) fn check_pure(
    entry: Option<&ProducerEntry>,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
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
            if base_sequence == increment_sequence(entry.last_sequence, 1) {
                return Decision::Append;
            }
            if matches_last_batch(entry, base_sequence, last_offset_delta) {
                return Decision::Duplicate {
                    base_offset: entry.base_offset,
                };
            }
            Decision::OutOfOrder
        }
    }
}

fn matches_last_batch(entry: &ProducerEntry, base_sequence: i32, last_offset_delta: i32) -> bool {
    let Some(committed_delta) = entry
        .last_offset
        .checked_sub(entry.base_offset)
        .and_then(|delta| i32::try_from(delta).ok())
    else {
        return false;
    };
    base_sequence == decrement_sequence(entry.last_sequence, committed_delta)
        && increment_sequence(base_sequence, last_offset_delta) == entry.last_sequence
}

/// Per-partition idempotent-producer state, nested under the owning
/// topic. The partition index (`i32`, `Copy`) is the key, so per-call
/// lookups allocate nothing. The outer topic map is keyed by `String`, but
/// its `get`/`entry` accept a borrowed `&str`. That map allocates the owned
/// topic key only on the first produce to a topic it has not seen before.
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

    /// Replace one partition's producer sequence state with the state rebuilt
    /// from its durable log.
    ///
    /// Startup calls this for disk-backed and diskless partitions before the
    /// partition writer starts. `Log::open` has already loaded the latest
    /// valid Kafka-compatible producer snapshot and replayed its uncovered
    /// tail, including state whose source segment was removed locally after
    /// remote-tier copy.
    ///
    /// # Errors
    /// This currently cannot fail; the result remains fallible so startup can
    /// preserve its existing error boundary if snapshot projection gains a
    /// checked conversion later.
    pub async fn rebuild_from_log(
        &self,
        topic: &str,
        partition: PartitionIndex,
        log: &crabka_log::Log,
    ) -> Result<(), crabka_log::LogError> {
        self.rebuild_from_snapshot(topic, partition, log.producer_state_snapshot())
            .await;
        Ok(())
    }

    pub(crate) async fn rebuild_from_snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
        snapshot: Vec<crabka_log::ProducerSnapshotEntry>,
    ) {
        self.handle(topic, partition).lock().await.entries = entries_from_snapshot(snapshot);
    }

    /// Install recovered producer state before a partition becomes
    /// request-visible as leader.
    ///
    /// Unlike [`Self::rebuild_from_snapshot`], this replaces the map handle
    /// synchronously. The partition does not exist in `PartitionRegistry` yet,
    /// so no request can have acquired the new handle. Vacant materialization
    /// can therefore make follower-prefix hydration and idempotent-producer
    /// recovery one atomic publication boundary from the request path's point
    /// of view.
    pub(crate) fn install_snapshot_before_materialization(
        &self,
        topic: &str,
        partition: PartitionIndex,
        snapshot: Vec<crabka_log::ProducerSnapshotEntry>,
    ) {
        let parts = if let Some(existing) = self.by_topic.get(topic) {
            existing.value().clone()
        } else {
            self.by_topic
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(DashMap::new()))
                .value()
                .clone()
        };
        parts.insert(
            partition,
            Arc::new(Mutex::new(PartitionProducerState {
                entries: entries_from_snapshot(snapshot),
            })),
        );
    }

    /// Decide whether to append the incoming batch.
    ///
    /// `base_sequence` is the wire `base_sequence`. `last_offset_delta` is
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
        check_pure(
            s.entries.get(&ProducerId(producer_id)),
            producer_epoch,
            base_sequence,
            last_offset_delta,
        )
    }

    /// Commit a successful append into the tracker.
    pub async fn commit(
        &self,
        topic: &str,
        partition: PartitionIndex,
        producer: (i64, i16),
        sequence: (i32, i32),
        append: (LogOffset, i64),
    ) {
        let (producer_id, producer_epoch) = producer;
        let (base_sequence, last_offset_delta) = sequence;
        let (base_offset, last_timestamp) = append;
        let handle = self.handle(topic, partition);
        let mut s = handle.lock().await;
        let last_sequence = increment_sequence(base_sequence, last_offset_delta);
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

    /// Drop idempotent-producer entries whose last accepted batch was
    /// truncated off the log, that is `last_offset >= offset`.
    ///
    /// The broker calls this after it truncates the partition log below the
    /// recorded batch. Two paths do that: KIP-320 divergence truncation on
    /// rejoin, and an `OFFSET_OUT_OF_RANGE` reset.
    ///
    /// Without this call, the broker deduplicates a producer that retries a
    /// batch from the truncated tail against a `base_offset` that is no longer
    /// in the log. The `acks=all` HW gate
    /// (`await_hw_at_least(base_offset + delta + 1)`) then waits forever for a
    /// high watermark that can never reach the truncated offset. That is a
    /// permanent produce stall after failover. When this function drops the
    /// entry, the retry re-appends fresh instead. This mirrors Kafka's
    /// `ProducerStateManager.truncateAndReload`. It does not create state for a
    /// partition that the broker has never tracked.
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

    /// Resolve the per-partition state handle, and create it on a miss.
    ///
    /// The outer topic lookup borrows `&str`. It allocates an owned `String`
    /// key only on the first lookup of that topic. The inner partition lookup
    /// is keyed by `i32` and never allocates.
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
    /// `(topic, partition)`.
    ///
    /// This function returns an empty list when the partition has no entries.
    /// That means no idempotent or transactional producer has produced to it
    /// yet. The `DescribeProducers` admin handler (`api_key=61`, KIP-664)
    /// calls it to show per-partition producer state to admin clients such as
    /// `kafka-admin --describe-producers`.
    ///
    /// The snapshot drops the mutex before it returns, so callers do not
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

    /// Snapshot of currently-active producers on `(topic, partition)`.
    ///
    /// The map holds `producer_id` → that producer's last-accepted-batch
    /// `base_offset`. A producer is "active" when
    /// `now_ms - last_activity_ms <= expiration_ms`. That is Kafka's
    /// `producer.id.expiration.ms` inactivity window. This function excludes
    /// expired producers.
    ///
    /// The cleaner calls it to build a `CompactionContext`. The cleaner must
    /// keep an active producer's last batch with `RETAIN_EMPTY` even when
    /// compaction removes all of its records, so the producer's
    /// sequence/epoch state survives.
    ///
    /// This function returns an empty map for an unknown `(topic, partition)`.
    ///
    /// The caller is the partition writer task's `WriterMessage::Compact`
    /// handler, which fills the `CompactionContext::active_producers` set.
    /// `spawn_partition` threads the broker-wide `ProducerState` into
    /// `partition_writer::run` for that handler.
    pub async fn active_snapshot(
        &self,
        topic: &str,
        partition: PartitionIndex,
        now_ms: i64,
        expiration: Time,
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
            .filter(|(_pid, e)| {
                now_ms.saturating_sub(e.last_activity_ms) <= expiration.millis_i64()
            })
            .map(|(pid, e)| (pid.get(), e.base_offset))
            .collect()
    }

    /// Evict idempotent-producer entries whose last activity is older
    /// than `ttl` relative to `now_ms`.
    ///
    /// This mirrors Kafka's `producer.id.expiration.ms`, whose default is
    /// `86_400_000` ms = 24h. Kafka expires by *inactivity*. An entry that
    /// keeps receiving produces stays. An entry that has gone quiet past the
    /// window goes, so the map does not grow unbounded.
    ///
    /// This function removes empty partition maps and empty topic maps once
    /// their last entry expires, so stale `(topic, partition)` keys do not
    /// leak. It returns the number of producer-id entries it evicted.
    ///
    /// This function gives the mechanism only. The periodic caller is a
    /// broker maintenance loop, wired separately.
    pub async fn expire_older_than(&self, now_ms: i64, ttl: Time) -> usize {
        let ttl_ms = ttl.millis_i64();
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

fn entries_from_snapshot(
    snapshot: Vec<crabka_log::ProducerSnapshotEntry>,
) -> HashMap<ProducerId, ProducerEntry> {
    let recovered_at = crate::txn::util::now_millis();
    snapshot
        .into_iter()
        .map(|entry| {
            let base_offset = if entry.last_offset >= 0 {
                entry.last_offset.0 - i64::from(entry.offset_delta)
            } else {
                // A marker-only producer has no retained data batch.
                -1
            };
            (
                entry.producer_id,
                ProducerEntry {
                    epoch: entry.producer_epoch,
                    last_sequence: entry.last_sequence,
                    last_offset: entry.last_offset.0,
                    base_offset,
                    last_timestamp: entry.timestamp,
                    last_activity_ms: recovered_at,
                },
            )
        })
        .collect()
}

#[cfg(test)]
#[path = "producer_state_model.rs"]
mod producer_state_model;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::{millis, secs};

    use super::*;

    macro_rules! commit {
        ($state:expr, $topic:expr, $partition:expr, $pid:expr, $epoch:expr,
         $base:expr, $delta:expr, $offset:expr, $timestamp:expr $(,)?) => {
            $state.commit(
                $topic,
                $partition,
                ($pid, $epoch),
                ($base, $delta),
                ($offset, $timestamp),
            )
        };
    }

    #[tokio::test]
    async fn first_batch_appends() {
        let s = ProducerState::new();
        let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
        assert!(d == Decision::Append);
    }

    #[tokio::test]
    async fn next_sequence_appends() {
        let s = ProducerState::new();
        commit!(
            s,
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
        commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 4, 0, 1).await;
        let d = s.check("t", PartitionIndex(0), 1000, 0, 0, 4).await;
        assert!(d == Decision::Duplicate { base_offset: 0 });
    }

    #[tokio::test]
    async fn only_an_exact_retry_of_the_last_batch_is_duplicate() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1000, 0, 3, 1, 10, 1).await;

        check!(
            s.check("t", PartitionIndex(0), 1000, 0, 3, 1).await
                == Decision::Duplicate { base_offset: 10 }
        );
        check!(s.check("t", PartitionIndex(0), 1000, 0, 3, 0).await == Decision::OutOfOrder);
        check!(s.check("t", PartitionIndex(0), 1000, 0, 2, 1).await == Decision::OutOfOrder);
    }

    #[tokio::test]
    async fn sequence_rollover_appends_and_commits_without_overflow() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 1, 10, 1,).await;

        check!(s.check("t", PartitionIndex(0), 1000, 0, 0, 0).await == Decision::Append);
        check!(s.check("t", PartitionIndex(0), 1000, 0, 1, 0).await == Decision::OutOfOrder);

        commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 2, 12, 2).await;
        let entry = s.snapshot("t", PartitionIndex(0)).await[0].1;
        check!(entry.last_sequence == 2);
        check!(entry.last_offset == 14);
    }

    #[tokio::test]
    async fn batch_can_cross_sequence_rollover() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 2, 20, 1,).await;

        check!(
            s.check("t", PartitionIndex(0), 1000, 0, i32::MAX - 1, 2)
                .await
                == Decision::Duplicate { base_offset: 20 }
        );
        check!(s.check("t", PartitionIndex(0), 1000, 0, 1, 0).await == Decision::Append);
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
        commit!(
            s,
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
        commit!(
            s,
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
        commit!(
            s,
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
        commit!(s, "t", PartitionIndex(0), 1000, 0, 0, 4, 0, 1).await;
        // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
        let d = s.check("t", PartitionIndex(0), 1000, 0, 10, 2).await;
        assert!(d == Decision::OutOfOrder);
    }

    #[tokio::test]
    async fn lower_epoch_is_fenced() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1000, 5, 0, 4, 0, 1).await;
        let d = s.check("t", PartitionIndex(0), 1000, 4, 5, 2).await;
        assert!(d == Decision::Fenced);
    }

    /// A bumped producer epoch (same `producer_id`, higher epoch) establishes a
    /// FRESH sequence baseline: `base_sequence == 0` at the new epoch must be a
    /// fresh `Append`, NOT a `Duplicate` against the prior epoch's high-water.
    /// This is the EOS-restart path. The client resets its sequence to 0.
    ///
    /// This is the regression test for the cross-restart EOS data-loss bug.
    /// Before the fix, the broker silently deduped a restarted EOS producer's
    /// first record on each partition and echoed the old `base_offset`. The
    /// txn's offset commit still landed. The source offset advanced, but the
    /// output record vanished.
    #[tokio::test]
    async fn higher_epoch_at_seq_zero_appends() {
        let s = ProducerState::new();
        // Epoch 5 committed sequences 0..=2 (last_sequence = 2).
        commit!(
            s,
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
    /// appends. This is the KIP-890 (`TV_2`) per-`EndTxn` epoch-bump path. The
    /// broker bumps the epoch on every commit or abort within the SAME
    /// producer session, and the client keeps its sequence counter going. The
    /// first batch at the new epoch is the baseline whatever its
    /// `base_sequence` is. Same-epoch ordering resumes once that batch
    /// commits.
    #[tokio::test]
    async fn higher_epoch_continuing_sequence_appends() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1000, 5, 0, 2, 0, 1).await;
        // Epoch 6 (KIP-890 bump), sequence continues at 3 — still a fresh append.
        let d = s.check("t", PartitionIndex(0), 1000, 6, 3, 0).await;
        assert!(d == Decision::Append);
        // After committing the new epoch's batch, same-epoch dedup resumes.
        commit!(
            s,
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
        commit!(s, "t", PartitionIndex(3), 1000, 0, 0, 4, 7, 1).await;
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
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        commit!(s, "t", PartitionIndex(0), 2, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            let mut st = h.lock().await;
            st.entries.get_mut(&ProducerId(1)).unwrap().last_activity_ms = 1_000; // old
            st.entries.get_mut(&ProducerId(2)).unwrap().last_activity_ms = 9_000; // recent
        }
        // now = 10_000, ttl = 5_000 → pid 1 (age 9_000) expires, pid 2
        // (age 1_000) survives.
        let evicted = s.expire_older_than(10_000, secs(5)).await;
        assert!(evicted == 1);
        let snap = s.snapshot("t", PartitionIndex(0)).await;
        assert!(snap.len() == 1);
        assert!(snap[0].0 == 2, "only the recently-active producer survives");
    }

    #[tokio::test]
    async fn expire_evicts_entry_at_exact_ttl_boundary() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            h.lock()
                .await
                .entries
                .get_mut(&ProducerId(1))
                .unwrap()
                .last_activity_ms = 5_000;
        }

        let evicted = s.expire_older_than(10_000, secs(5)).await;
        assert!(evicted == 1);
        assert!(s.snapshot("t", PartitionIndex(0)).await.is_empty());
    }

    #[tokio::test]
    async fn active_snapshot_excludes_expired_includes_active() {
        let s = ProducerState::new();
        // pid 1: last batch base_offset 10; pid 2: base_offset 20.
        commit!(
            s,
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
        commit!(
            s,
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
            .active_snapshot("t", PartitionIndex(0), 10_000, secs(5))
            .await;
        let expected: HashMap<i64, i64> = [(2, 20)].into_iter().collect();
        assert!(snap == expected);
        // Unknown partition / topic → empty without panicking.
        for (topic, partition) in [("t", PartitionIndex(99)), ("nope", PartitionIndex(0))] {
            assert!(
                s.active_snapshot(topic, partition, 10_000, secs(5)).await == HashMap::new(),
                "case: {topic}/{partition}"
            );
        }
    }

    #[tokio::test]
    async fn expire_drops_empty_partition_and_topic_slots() {
        let s = ProducerState::new();
        commit!(s, "t", PartitionIndex(0), 1, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", PartitionIndex(0));
            h.lock()
                .await
                .entries
                .get_mut(&ProducerId(1))
                .unwrap()
                .last_activity_ms = 0;
        }
        let evicted = s.expire_older_than(1_000_000, millis(1)).await;
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
        /// Large-N randomized submit sequences over `check_pure`.
        ///
        /// The accepted-append log per epoch is a contiguous, duplicate-free,
        /// monotonic prefix. A lower epoch is fenced. A higher epoch resets
        /// the baseline. This test complements the exhaustive
        /// `producer_state_model` at a scale the BFS cannot reach: epoch 0..6,
        /// base_seq 0..200, and up to 400 ops.
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
                let d = check_pure(entry.as_ref(), epoch, base_seq, 0);
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
                            base_seq == e.last_sequence,
                            "single-record duplicate must match the committed sequence"
                        );
                    }
                    Decision::OutOfOrder => {
                        let e = entry.as_ref().expect("OutOfOrder implies an entry");
                        prop_assert_eq!(epoch, e.epoch);
                        prop_assert!(
                            base_seq != e.last_sequence && base_seq != e.last_sequence + 1,
                            "OutOfOrder must be neither a retry nor the next sequence"
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
