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

#[derive(Debug, Default)]
pub struct ProducerState {
    #[allow(clippy::type_complexity)]
    by_partition: Arc<DashMap<(String, i32), Arc<Mutex<PartitionProducerState>>>>,
}

impl ProducerState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_partition: Arc::new(DashMap::new()),
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
            },
        );
    }

    fn handle(&self, topic: &str, partition: i32) -> Arc<Mutex<PartitionProducerState>> {
        self.by_partition
            .entry((topic.to_string(), partition))
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
        // wire up an empty entry. `get` returns `None` for un-tracked
        // partitions, which we map to an empty result.
        let Some(entry_ref) = self.by_partition.get(&(topic.to_string(), partition)) else {
            return Vec::new();
        };
        let handle = entry_ref.value().clone();
        drop(entry_ref);
        let state = handle.lock().await;
        state.entries.iter().map(|(pid, e)| (*pid, *e)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_batch_appends() {
        let s = ProducerState::new();
        let d = s.check("t", 0, 1000, 0, 0, 4).await;
        assert_eq!(d, Decision::Append);
    }

    #[tokio::test]
    async fn next_sequence_appends() {
        let s = ProducerState::new();
        s.commit(
            "t", 0, 1000, 0, 0, 4, /* base_offset */ 0, /* ts */ 1,
        )
        .await;
        let d = s.check("t", 0, 1000, 0, 5, 2).await;
        assert_eq!(d, Decision::Append);
    }

    #[tokio::test]
    async fn duplicate_returns_cached_offset() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, 0, 1).await;
        let d = s.check("t", 0, 1000, 0, 0, 4).await;
        assert_eq!(d, Decision::Duplicate { base_offset: 0 });
    }

    #[tokio::test]
    async fn out_of_order_when_gap() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 0, 0, 4, 0, 1).await;
        // Last seq is 4; next valid base_seq is 5. Sending 10 → OutOfOrder.
        let d = s.check("t", 0, 1000, 0, 10, 2).await;
        assert_eq!(d, Decision::OutOfOrder);
    }

    #[tokio::test]
    async fn lower_epoch_is_fenced() {
        let s = ProducerState::new();
        s.commit("t", 0, 1000, 5, 0, 4, 0, 1).await;
        let d = s.check("t", 0, 1000, 4, 5, 2).await;
        assert_eq!(d, Decision::Fenced);
    }
}
