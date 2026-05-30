//! Per-(topic, partition) producer-sequence tracking. Drives the
//! idempotent-producer dedup / out-of-order / epoch-fence checks in
//! `handlers::produce`.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub struct ProducerEntry {
    pub epoch: i16,
    pub last_sequence: i32,
    /// Retained for future slice work (log metrics / WAL replay).
    #[allow(dead_code)]
    pub last_offset: i64,
    pub base_offset: i64,
    /// Retained for future slice work (metrics / compaction).
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
    pub entries: HashMap<i64, ProducerEntry>,
}

/// Outcome of a dedup check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Producer is fresh or the sequence is one past the last commit. Caller
    /// should append, then call `commit` with the assigned base offset.
    Append,
    /// Previously-committed sequence range. Caller should respond with
    /// `error_code = NONE` and `base_offset = base_offset`.
    Duplicate { base_offset: i64 },
    /// `base_sequence != last_sequence + 1`. Caller responds with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER (45)`.
    OutOfOrder,
    /// `epoch < entry.epoch`. Caller responds with
    /// `INVALID_PRODUCER_EPOCH (53)`.
    Fenced,
}

/// Per-partition idempotent-producer state, nested under the owning
/// topic. Keyed by partition index (`i32`, `Copy`) so per-call lookups
/// allocate nothing; the outer topic map is keyed by `String` but its
/// `get`/`entry` accept a borrowed `&str` and only allocate the owned
/// topic key on the first produce to a previously-unseen topic.
type PartitionMap = DashMap<i32, Arc<Mutex<PartitionProducerState>>>;

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
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
    ) -> Decision {
        let handle = self.handle(topic, partition);
        let s = handle.lock().await;
        match s.entries.get(&producer_id) {
            None => Decision::Append,
            Some(entry) => {
                if producer_epoch < entry.epoch {
                    return Decision::Fenced;
                }
                if base_sequence <= entry.last_sequence {
                    // Anywhere within (or before) the committed range counts
                    // as duplicate. We echo the previously-committed base offset.
                    return Decision::Duplicate {
                        base_offset: entry.base_offset,
                    };
                }
                if base_sequence == entry.last_sequence + 1 {
                    let _ = last_offset_delta; // used by caller to compute last_sequence
                    Decision::Append
                } else {
                    Decision::OutOfOrder
                }
            }
        }
    }

    /// Commit a successful append into the tracker.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: i64,
        last_timestamp: i64,
    ) {
        let handle = self.handle(topic, partition);
        let mut s = handle.lock().await;
        let last_sequence = base_sequence + last_offset_delta;
        let last_offset = base_offset + i64::from(last_offset_delta);
        s.entries.insert(
            producer_id,
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

    /// Resolve (creating on miss) the per-partition state handle. The
    /// outer topic lookup borrows `&str` and only allocates an owned
    /// `String` key when the topic is seen for the first time; the inner
    /// partition lookup is keyed by `i32` and never allocates.
    fn handle(&self, topic: &str, partition: i32) -> Arc<Mutex<PartitionProducerState>> {
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
    pub async fn snapshot(&self, topic: &str, partition: i32) -> Vec<(i64, ProducerEntry)> {
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
        state.entries.iter().map(|(pid, e)| (*pid, *e)).collect()
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
            let partition_refs: Vec<(i32, Arc<Mutex<PartitionProducerState>>)> = parts
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
mod tests {
    use super::*;
    use assert2::assert;

    #[tokio::test]
    async fn first_batch_appends() {
        let s = ProducerState::new();
        let d = s.check("t", 0, 1000, 0, 0, 4).await;
        assert!(d == Decision::Append);
    }

    #[tokio::test]
    async fn next_sequence_appends() {
        let s = ProducerState::new();
        s.commit(
            "t", 0, 1000, 0, 0, 4, /* base_offset */ 0, /* ts */ 1,
        )
        .await;
        let d = s.check("t", 0, 1000, 0, 5, 2).await;
        assert!(d == Decision::Append);
    }

    #[tokio::test]
    async fn duplicate_returns_cached_offset() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, 0, 1).await;
        let d = s.check("t", 0, 1000, 0, 0, 4).await;
        assert!(d == Decision::Duplicate { base_offset: 0 });
    }

    #[tokio::test]
    async fn out_of_order_when_gap() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, 0, 1).await;
        // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
        let d = s.check("t", 0, 1000, 0, 10, 2).await;
        assert!(d == Decision::OutOfOrder);
    }

    #[tokio::test]
    async fn lower_epoch_is_fenced() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 5, 0, 4, 0, 1).await;
        let d = s.check("t", 0, 1000, 4, 5, 2).await;
        assert!(d == Decision::Fenced);
    }

    #[tokio::test]
    async fn snapshot_reports_committed_entries() {
        let s = ProducerState::new();
        s.commit("t", 3, 1000, 0, 0, 4, 7, 1).await;
        let snap = s.snapshot("t", 3).await;
        assert!(snap.len() == 1);
        assert!(snap[0].0 == 1000);
        assert!(snap[0].1.base_offset == 7);
        // Untouched partition / topic report empty without panicking.
        assert!(s.snapshot("t", 0).await.is_empty());
        assert!(s.snapshot("other", 3).await.is_empty());
    }

    #[tokio::test]
    async fn expire_evicts_only_idle_entries() {
        let s = ProducerState::new();
        // Two producers on the same partition with controlled activity
        // timestamps: we commit, then overwrite last_activity_ms directly
        // to simulate age without sleeping.
        s.commit("t", 0, 1, 0, 0, 0, 0, 0).await;
        s.commit("t", 0, 2, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", 0);
            let mut st = h.lock().await;
            st.entries.get_mut(&1).unwrap().last_activity_ms = 1_000; // old
            st.entries.get_mut(&2).unwrap().last_activity_ms = 9_000; // recent
        }
        // now = 10_000, ttl = 5_000 → pid 1 (age 9_000) expires, pid 2
        // (age 1_000) survives.
        let evicted = s.expire_older_than(10_000, 5_000).await;
        assert!(evicted == 1);
        let snap = s.snapshot("t", 0).await;
        assert!(snap.len() == 1);
        assert!(snap[0].0 == 2, "only the recently-active producer survives");
    }

    #[tokio::test]
    async fn expire_drops_empty_partition_and_topic_slots() {
        let s = ProducerState::new();
        s.commit("t", 0, 1, 0, 0, 0, 0, 0).await;
        {
            let h = s.handle("t", 0);
            h.lock().await.entries.get_mut(&1).unwrap().last_activity_ms = 0;
        }
        let evicted = s.expire_older_than(1_000_000, 1).await;
        assert!(evicted == 1);
        // The empty partition and topic maps are pruned.
        assert!(
            s.by_topic.get("t").is_none(),
            "empty topic slot must be removed"
        );
        // A subsequent produce still works after pruning.
        assert!(s.check("t", 0, 1, 0, 0, 0).await == Decision::Append);
    }
}
