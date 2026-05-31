//! Per-broker `TxnCoordinator`. Owns the in-memory state map of every
//! `transactional_id` whose `__transaction_state` partition this broker
//! hosts as leader. Persists every state change as a record in the
//! corresponding `__transaction_state` partition. Recovers state on
//! `Broker::start` by replaying those partitions.

// `is_coordinator_for`, `get`, and a couple of admin helpers are consumed by
// the transaction wire handlers. Remove this attribute once those land.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crabka_metadata::MetadataImage;
use crabka_protocol::records::{Record, RecordBatch};

use crate::error::BrokerError;
use crate::partition_registry::PartitionRegistry;
use crate::txn::bootstrap;
use crate::txn::partitioner::partition_for_tid;
use crate::txn::state::TxnEntry;

/// Per-broker transaction coordinator. Constructed in `Broker::start`
/// and shared via `Arc` with the transaction wire handlers.
pub(crate) struct TxnCoordinator {
    pub(crate) node_id: crabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Set of `__transaction_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<i32>>,
    /// Reverse lookup: `producer_id` → `transactional_id`. Used by the
    /// Produce handler to verify transactional batches (KIP-1319 v2).
    pid_to_tid: DashMap<i64, String>,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: crabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    ) -> Self {
        Self {
            node_id,
            partitions,
            producer_ids,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
            pid_to_tid: DashMap::new(),
        }
    }

    /// Recompute which `__transaction_state` partitions this broker leads
    /// from the current `MetadataImage`. Called from `recover` and also
    /// on every metadata change.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        let mut set = HashSet::new();
        for p in image.partitions_of(bootstrap::TOPIC) {
            if p.leader == self.node_id {
                set.insert(p.partition);
            }
        }
        *self.leader_partitions.write().await = set;
    }

    /// Returns the `__transaction_state` partition index responsible for `tid`.
    // `self` is unused here because the mapping is purely a function of
    // `tid` and `NUM_PARTITIONS`, but keeping it as a method lets callers
    // use a consistent `coord.partition_for(tid)` style.
    #[allow(clippy::unused_self)]
    pub(crate) fn partition_for(&self, tid: &str) -> i32 {
        partition_for_tid(tid, bootstrap::NUM_PARTITIONS)
    }

    /// Returns `true` if this broker is the transaction coordinator for `tid`.
    pub(crate) async fn is_coordinator_for(&self, tid: &str) -> bool {
        let p = self.partition_for(tid);
        self.leader_partitions.read().await.contains(&p)
    }

    /// Retrieve the locked `TxnEntry` for `tid`, or `None` if unknown.
    pub(crate) fn get(&self, tid: &str) -> Option<Arc<Mutex<TxnEntry>>> {
        self.state.get(tid).map(|e| e.value().clone())
    }

    /// Reverse lookup: given a `producer_id`, return the `transactional_id`
    /// it was registered under, or `None` if the pid is unknown.
    pub(crate) fn tid_for_pid(&self, pid: i64) -> Option<String> {
        self.pid_to_tid.get(&pid).map(|e| e.value().clone())
    }

    /// Evict the stale `prev_producer_id -> tid` mapping after a KIP-890
    /// epoch-overflow roll. When the producer epoch is exhausted the `EndTxn`
    /// completion path allocates a new `producer_id` and records the prior id
    /// as `entry.prev_producer_id` (see `next_producer_identity`); without this
    /// the old id's mapping would leak one entry per roll. Idempotent: a no-op
    /// once the old id is gone, and skipped for entries that never rolled
    /// (`prev == -1`). pids are globally unique, so the prior id only ever
    /// mapped to this tid — removing it can't affect another transaction.
    fn evict_rolled_pid(pid_to_tid: &DashMap<i64, String>, entry: &TxnEntry) {
        if entry.prev_producer_id >= 0 && entry.prev_producer_id != entry.producer_id {
            pid_to_tid.remove(&entry.prev_producer_id);
        }
    }

    /// Snapshot every locally-coordinated `TxnEntry`. Used by the KIP-664
    /// admin handlers (`ListTransactions`, `DescribeTransactions`) to
    /// expose the in-memory txn-state map. Each entry is locked + cloned
    /// in turn so the snapshot is internally consistent per-tid but not
    /// across the entire batch — acceptable for an admin introspection
    /// API (Apache Kafka's JVM coordinator has the same property).
    pub(crate) async fn snapshot(&self) -> Vec<TxnEntry> {
        // Collect the `Arc<Mutex<_>>` handles first so we don't hold the
        // DashMap shard locks while taking the inner async mutex.
        let handles: Vec<Arc<Mutex<TxnEntry>>> =
            self.state.iter().map(|e| e.value().clone()).collect();
        let mut out = Vec::with_capacity(handles.len());
        for h in handles {
            let entry = h.lock().await;
            out.push(entry.clone());
        }
        out
    }

    /// Persist `entry` to the corresponding `__transaction_state` partition
    /// log, then update the in-memory map. The batch is appended via the
    /// partition's writer task (ordered with all other produce appends).
    ///
    /// `txnv` is the finalized `transaction.version` resolved from the live
    /// metadata image at the caller; it selects the byte-exact Kafka
    /// `TransactionLogValue` format (v0 for `TV_0`, v1 for `TV >= 1`).
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the partition is not locally held
    /// or the append fails.
    pub(crate) async fn put(
        &self,
        entry: TxnEntry,
        txnv: crate::txn::version::TxnVersion,
    ) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.clone();
        let p = self.partition_for(&tid);
        let part = self
            .partitions
            .get(bootstrap::TOPIC, p)
            .ok_or_else(|| BrokerError::Txn(format!("__transaction_state-{p} not local")))?;

        // Byte-exact Kafka TransactionLogKey(v0) + TransactionLogValue(v0/v1).
        let key = crate::txn::log_record::encode_key(&tid);
        let value = crate::txn::log_record::encode_value(&entry, txnv.flexible_records());

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await?;

        Self::evict_rolled_pid(&self.pid_to_tid, &entry);
        self.pid_to_tid
            .insert(entry.producer_id, entry.transactional_id.clone());
        self.state.insert(tid, Arc::new(Mutex::new(entry)));
        Ok(())
    }

    /// Replay every locally-led `__transaction_state` partition into the
    /// in-memory state map. Called from `Broker::start`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] if reading a partition's log fails with an
    /// error other than reading past the end (which is treated as a normal
    /// "partition is empty" condition).
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await;

        let local_partitions: Vec<i32> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        for p in local_partitions {
            let Some(part) = self.partitions.get(bootstrap::TOPIC, p) else {
                // Partition is not yet open locally (no log dir / not yet created).
                continue;
            };

            let mut offset = part.log_start_offset();
            loop {
                let out = match part.read_log(offset, 1 << 20) {
                    Ok(o) => o,
                    // OffsetTooLow can happen when the partition just opened
                    // with no data written yet (log_start == log_end == 0
                    // but the log returns empty in that case). Treat any
                    // read error as "nothing to replay here" to be safe.
                    Err(e) => {
                        warn!(
                            partition = p,
                            error = %e,
                            "read error during __transaction_state recovery; skipping partition"
                        );
                        break;
                    }
                };

                if out.batches.is_empty() {
                    break;
                }

                for batch in &out.batches {
                    for rec in &batch.records {
                        let Some(key_bytes) = rec.key.as_ref() else {
                            warn!(
                                partition = p,
                                "__transaction_state record missing key; skipping"
                            );
                            continue;
                        };
                        let tid = match crate::txn::log_record::decode_key(key_bytes) {
                            Ok(t) => t,
                            Err(e) => {
                                warn!(
                                    partition = p,
                                    error = %e,
                                    "invalid TransactionLogKey in __transaction_state; skipping"
                                );
                                continue;
                            }
                        };
                        let Some(value_bytes) = rec.value.as_ref() else {
                            // Tombstone (null value) deletes txn state for this tid.
                            self.state.remove(&tid);
                            continue;
                        };
                        let entry = match crate::txn::log_record::decode_value(value_bytes, tid) {
                            Ok(e) => e,
                            Err(e) => {
                                warn!(
                                    partition = p,
                                    error = %e,
                                    "invalid TransactionLogValue in __transaction_state; skipping"
                                );
                                continue;
                            }
                        };
                        self.pid_to_tid
                            .insert(entry.producer_id, entry.transactional_id.clone());
                        self.state
                            .insert(entry.transactional_id.clone(), Arc::new(Mutex::new(entry)));
                    }
                    offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
                }
            }
        }

        info!(
            tids_loaded = self.state.len(),
            "TxnCoordinator recovery complete"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(pid: i64, prev: i64) -> TxnEntry {
        let mut e = TxnEntry::new_empty("tid-a".into(), pid, 0, 60_000, 0);
        e.prev_producer_id = prev;
        e
    }

    #[test]
    fn evict_rolled_pid_drops_only_the_prior_id_on_a_roll() {
        let map: DashMap<i64, String> = DashMap::new();
        map.insert(1000, "tid-a".into()); // the pre-roll mapping

        // A roll: new pid 2000, prev = 1000. The stale 1000 mapping is evicted;
        // put then inserts 2000 (mirrored here).
        TxnCoordinator::evict_rolled_pid(&map, &entry(2000, 1000));
        map.insert(2000, "tid-a".into());

        assert!(
            map.get(&1000).is_none(),
            "stale pre-roll pid must be evicted"
        );
        assert!(map.get(&2000).map(|e| e.value().clone()) == Some("tid-a".into()));
    }

    #[test]
    fn evict_rolled_pid_is_noop_without_a_roll() {
        let map: DashMap<i64, String> = DashMap::new();
        map.insert(1000, "tid-a".into());
        // Never rolled: prev == -1 → nothing evicted.
        TxnCoordinator::evict_rolled_pid(&map, &entry(1000, -1));
        assert!(map.get(&1000).is_some());
        // prev == current (defensive): nothing evicted.
        TxnCoordinator::evict_rolled_pid(&map, &entry(1000, 1000));
        assert!(map.get(&1000).is_some());
    }

    #[test]
    fn evict_rolled_pid_is_idempotent_after_the_id_is_gone() {
        let map: DashMap<i64, String> = DashMap::new();
        map.insert(2000, "tid-a".into());
        // prev=1000 already absent → repeated evictions are harmless no-ops.
        TxnCoordinator::evict_rolled_pid(&map, &entry(2000, 1000));
        TxnCoordinator::evict_rolled_pid(&map, &entry(2000, 1000));
        assert!(map.get(&1000).is_none());
        assert!(map.get(&2000).is_some());
    }
}
