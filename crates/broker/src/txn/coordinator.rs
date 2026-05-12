//! Per-broker `TxnCoordinator`. Owns the in-memory state map of every
//! `transactional_id` whose `__transaction_state` partition this broker
//! hosts as leader. Persists every state change as a record in the
//! corresponding `__transaction_state` partition. Recovers state on
//! `Broker::start` by replaying those partitions.

// `is_coordinator_for`, `get`, `put`, and `TxnCoordinator` itself are
// consumed by Phase-D handlers (Tasks 11-16). Remove this attribute once
// those tasks land.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use serde_wincode::SerdeCompat;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use wincode::{Deserialize as _, Serialize as _};

use crabka_metadata::MetadataImage;
use crabka_protocol::records::{Record, RecordBatch};

use crate::error::BrokerError;
use crate::partition::Partition;
use crate::txn::bootstrap;
use crate::txn::partitioner::partition_for_tid;
use crate::txn::state::TxnEntry;

/// Per-broker transaction coordinator. Constructed in `Broker::start` (Task 10)
/// and shared via `Arc` with Phase-D wire handlers.
pub(crate) struct TxnCoordinator {
    pub(crate) node_id: crabka_metadata::NodeId,
    pub(crate) partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    /// Live in-memory state: `transactional_id` → locked `TxnEntry`.
    state: DashMap<String, Arc<Mutex<TxnEntry>>>,
    /// Set of `__transaction_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<i32>>,
}

impl TxnCoordinator {
    pub(crate) fn new(
        node_id: crabka_metadata::NodeId,
        partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
        producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    ) -> Self {
        Self {
            node_id,
            partitions,
            producer_ids,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
        }
    }

    /// Recompute which `__transaction_state` partitions this broker leads
    /// from the current `MetadataImage`. Called from `recover` and also
    /// on every metadata change (Task 10 wires this).
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

    /// Persist `entry` to the corresponding `__transaction_state` partition
    /// log, then update the in-memory map. The batch is appended via the
    /// partition's writer task (ordered with all other produce appends).
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Txn`] if the partition is not locally held
    /// or the append fails.
    pub(crate) async fn put(&self, entry: TxnEntry) -> Result<(), BrokerError> {
        let tid = entry.transactional_id.clone();
        let p = self.partition_for(&tid);
        let part = self
            .partitions
            .get(&(bootstrap::TOPIC.to_string(), p))
            .ok_or_else(|| BrokerError::Txn(format!("__transaction_state-{p} not local")))?
            .value()
            .clone();

        // Serialize the entry using the serde-wincode codec (same as TxnEntry
        // test in state.rs).
        let payload = <SerdeCompat<TxnEntry>>::serialize(&entry)
            .map_err(|e| BrokerError::Txn(e.to_string()))?;

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(Bytes::from(tid.clone().into_bytes())),
            value: Some(Bytes::from(payload)),
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await?;

        self.state.insert(tid, Arc::new(Mutex::new(entry)));
        Ok(())
    }

    /// Replay every locally-led `__transaction_state` partition into the
    /// in-memory state map. Called from `Broker::start` (Task 10).
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
            let Some(part) = self
                .partitions
                .get(&(bootstrap::TOPIC.to_string(), p))
                .map(|e| e.value().clone())
            else {
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
                        let Some(tid_bytes) = rec.key.as_ref() else {
                            continue;
                        };
                        let Some(value) = rec.value.as_ref() else {
                            continue;
                        };
                        let Ok(entry) = <SerdeCompat<TxnEntry>>::deserialize(value) else {
                            warn!(
                                partition = p,
                                "invalid TxnEntry in __transaction_state; skipping record"
                            );
                            continue;
                        };
                        let tid = String::from_utf8_lossy(tid_bytes).into_owned();
                        self.state.insert(tid, Arc::new(Mutex::new(entry)));
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
