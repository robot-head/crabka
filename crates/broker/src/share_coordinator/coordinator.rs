//! Per-broker `ShareCoordinator` (KIP-932 persister). Owns the in-memory
//! per-`(group, topicId, partition)` delivery state for every
//! `__share_group_state` partition this broker hosts as leader. Persists
//! every state change as a `ShareSnapshot` / `ShareUpdate` record in the
//! corresponding `__share_group_state` partition, and recovers state on
//! `Broker::start` by replaying those partitions.
//!
//! Mirrors [`crate::txn::coordinator::TxnCoordinator`].
//!
//! Leadership: the wire handlers check [`ShareCoordinator::is_leader`]
//! before dispatching, so the state-machine methods here (`initialize`,
//! `write`, `read`, `delete`) assume this broker leads the relevant
//! `__share_group_state` partition. `persist_record` still defends against a
//! missing local partition log and returns [`BrokerError::Share`] in that case.

// The state-machine methods are consumed by the persister RPC handlers and
// the group-lifecycle hook.

use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use crabka_ids::PartitionIndex;
use crabka_log::Offset;
use crabka_metadata::MetadataImage;
use crabka_protocol::records::{Record, RecordBatch};
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};

use crate::{
    error::BrokerError,
    partition_registry::PartitionRegistry,
    share_coordinator::{
        bootstrap,
        config::ShareCoordinatorConfig,
        partitioner::partition_for_share_key,
        persistence::{
            KEY_SHARE_SNAPSHOT, KEY_SHARE_UPDATE, ShareSnapshotValue, ShareStateKey,
            ShareUpdateValue, StateBatch, encode_state_key, parse_state_key,
        },
        pruning::redundant_offset,
        state::SharePartitionState,
    },
};

/// In-memory map key: `(group_id, topic_id, partition)`.
type ShareStateKey3 = (String, uuid::Uuid, i32);

/// KIP-932 share-group state epoch: bumped on (re-)initialization (e.g.
/// `AlterShareGroupOffsets`); a write or initialize carrying an older epoch is
/// fenced with `FENCED_STATE_EPOCH`.
pub(crate) type StateEpoch = i32;

/// Leader epoch of the share-partition leader that issued a state write; a
/// stale value is fenced with `FENCED_LEADER_EPOCH`.
pub(crate) type LeaderEpoch = i32;

/// Kafka wire error code (see [`crate::codes`]) returned per partition when a
/// state-machine operation fails.
pub(crate) type ShareErrorCode = i16;

/// `(state_epoch, leader_epoch, start_offset, delivery_complete_count)` as
/// returned by [`ShareCoordinator::read_summary`].
pub(crate) type ShareStateSummary = (StateEpoch, LeaderEpoch, Offset, i32);

/// `start_offset` sentinel meaning "no persisted share state": tells the
/// share-partition leader to initialize delivery from scratch (KIP-932).
pub(crate) const UNINITIALIZED_START_OFFSET: i64 = -1;

/// Read-chunk budget in bytes per `read_log` call while replaying
/// `__share_group_state` partitions during recovery.
const RECOVERY_READ_MAX_BYTES: usize = 1 << 20;

/// Per-broker share-state coordinator. Constructed in `Broker::start` and
/// shared via `Arc` with the share-state wire handlers.
pub(crate) struct ShareCoordinator {
    pub(crate) node_id: crabka_metadata::NodeId,
    pub(crate) partitions: Arc<PartitionRegistry>,
    /// Live in-memory state: `(group, topicId, partition)` → locked state.
    state: DashMap<ShareStateKey3, Arc<Mutex<SharePartitionState>>>,
    /// Set of `__share_group_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<PartitionIndex>>,
    config: ShareCoordinatorConfig,
}

impl ShareCoordinator {
    pub(crate) fn new(
        node_id: crabka_metadata::NodeId,
        partitions: Arc<PartitionRegistry>,
        config: ShareCoordinatorConfig,
    ) -> Self {
        Self {
            node_id,
            partitions,
            state: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
            config,
        }
    }

    /// Recompute which `__share_group_state` partitions this broker leads
    /// from the current `MetadataImage`. Called from `recover` and on every
    /// metadata change.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        let mut set = HashSet::new();
        for p in image.partitions_of(bootstrap::TOPIC) {
            if p.leader == self.node_id {
                set.insert(PartitionIndex(p.partition));
            }
        }
        *self.leader_partitions.write().await = set;
    }

    /// Returns `true` if this broker leads `__share_group_state`-`state_partition`.
    pub(crate) async fn is_leader(&self, state_partition: PartitionIndex) -> bool {
        self.leader_partitions
            .read()
            .await
            .contains(&state_partition)
    }

    #[cfg(test)]
    pub(crate) async fn lead_all_partitions_for_test(&self) {
        let mut set = HashSet::new();
        for p in 0..bootstrap::NUM_PARTITIONS {
            set.insert(PartitionIndex(p));
        }
        *self.leader_partitions.write().await = set;
    }

    /// Returns the `__share_group_state` partition index responsible for the
    /// share key `(group, topic_id, partition)`.
    #[must_use]
    pub(crate) fn state_partition_for(
        &self,
        group: &str,
        topic_id: &uuid::Uuid,
        partition: i32,
    ) -> PartitionIndex {
        PartitionIndex(partition_for_share_key(
            group,
            topic_id,
            partition,
            self.config.state_topic_num_partitions,
        ))
    }

    /// Initialize the share state for `(group, topic_id, partition)` at
    /// `state_epoch` / `start_offset`. Fences with `FENCED_STATE_EPOCH` if a
    /// state with `state_epoch >= new state_epoch` already exists. Writes a
    /// `ShareSnapshot` record and seeds the in-memory state.
    ///
    /// # Errors
    ///
    /// Returns the per-partition error code on a fenced epoch or a persist
    /// failure (`COORDINATOR_NOT_AVAILABLE`).
    pub(crate) async fn initialize(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: StateEpoch,
        start_offset: Offset,
    ) -> Result<(), ShareErrorCode> {
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);

        if let Some(existing) = self.state.get(&map_key) {
            let cur = existing.value().clone();
            let guard = cur.lock().await;
            if guard.state_epoch >= state_epoch {
                return Err(crate::codes::FENCED_STATE_EPOCH);
            }
        }

        let snapshot = ShareSnapshotValue {
            snapshot_epoch: 0,
            state_epoch,
            leader_epoch: 0,
            start_offset,
            delivery_complete_count: 0,
            state_batches: Vec::new(),
        };
        let key = ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        let offset = self
            .persist_record(state_partition, key, Some(snapshot.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share initialize persist failed");
                crate::codes::COORDINATOR_NOT_AVAILABLE
            })?;

        let mut st = SharePartitionState::default();
        st.apply_snapshot(&snapshot);
        st.last_snapshot_offset = offset;
        self.state.insert(map_key, Arc::new(Mutex::new(st)));
        Ok(())
    }

    /// Apply a `WriteShareGroupState` delta. Fences a stale `state_epoch`
    /// (`FENCED_STATE_EPOCH`) and a stale `leader_epoch` (`FENCED_LEADER_EPOCH`),
    /// then applies the update, persists a `ShareUpdate`, and — every
    /// `snapshot_update_records_per_snapshot` updates — folds a full
    /// `ShareSnapshot` and prunes redundant log prefix.
    ///
    /// # Errors
    ///
    /// Returns the per-partition error code on a fenced epoch or a persist
    /// failure (`COORDINATOR_NOT_AVAILABLE`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn write(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
        state_epoch: StateEpoch,
        leader_epoch: LeaderEpoch,
        start_offset: Offset,
        delivery_complete_count: i32,
        batches: Vec<StateBatch>,
    ) -> Result<(), ShareErrorCode> {
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);

        let entry = self
            .state
            .entry(map_key)
            .or_insert_with(|| Arc::new(Mutex::new(SharePartitionState::default())))
            .value()
            .clone();
        let mut st = entry.lock().await;

        // Epoch fencing. A write carrying a state_epoch below the durable one
        // is rejected; a stale leader_epoch (lower than the recorded one) is
        // also rejected.
        if state_epoch < st.state_epoch {
            return Err(crate::codes::FENCED_STATE_EPOCH);
        }
        if leader_epoch < st.leader_epoch {
            return Err(crate::codes::FENCED_LEADER_EPOCH);
        }
        st.state_epoch = state_epoch;

        let update = ShareUpdateValue {
            snapshot_epoch: st.snapshot_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count,
            state_batches: batches,
        };
        st.apply_update(&update);

        let key = ShareStateKey {
            record_type: KEY_SHARE_UPDATE,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        self.persist_record(state_partition, key, Some(update.encode()))
            .await
            .map_err(|e| {
                warn!(error = %e, "share write persist failed");
                crate::codes::COORDINATOR_NOT_AVAILABLE
            })?;

        // Snapshot fold + prune once the update count crosses the threshold.
        if st.updates_since_snapshot >= self.config.snapshot_update_records_per_snapshot {
            let snapshot = st.to_snapshot();
            let snap_key = ShareStateKey {
                record_type: KEY_SHARE_SNAPSHOT,
                group_id: group.to_string(),
                topic_id,
                partition,
            };
            match self
                .persist_record(state_partition, snap_key, Some(snapshot.encode()))
                .await
            {
                Ok(offset) => {
                    st.apply_snapshot(&snapshot);
                    st.last_snapshot_offset = offset;
                    // Release the per-key lock before pruning so the
                    // per-partition scan below can lock sibling keys.
                    drop(st);
                    self.maybe_prune(state_partition).await;
                }
                Err(e) => {
                    warn!(error = %e, "share snapshot persist failed");
                    // The update itself was durable; a missed snapshot fold is
                    // recoverable on the next threshold crossing.
                }
            }
        }

        Ok(())
    }

    /// Clone the current state for `(group, topic_id, partition)`.
    pub(crate) async fn read(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<SharePartitionState> {
        let map_key = (group.to_string(), topic_id, partition);
        let handle = self.state.get(&map_key)?.value().clone();
        let st = handle.lock().await;
        Some(st.clone())
    }

    /// Summary `(state_epoch, leader_epoch, start_offset, delivery_complete_count)`.
    pub(crate) async fn read_summary(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Option<ShareStateSummary> {
        let map_key = (group.to_string(), topic_id, partition);
        let handle = self.state.get(&map_key)?.value().clone();
        let st = handle.lock().await;
        Some((
            st.state_epoch,
            st.leader_epoch,
            st.start_offset,
            st.delivery_complete_count,
        ))
    }

    /// Delete the share state for `(group, topic_id, partition)`: write a
    /// tombstone (snapshot key, null value) and drop the in-memory entry.
    ///
    /// # Errors
    ///
    /// Returns `COORDINATOR_NOT_AVAILABLE` if the tombstone persist fails.
    pub(crate) async fn delete(
        &self,
        group: &str,
        topic_id: uuid::Uuid,
        partition: i32,
    ) -> Result<(), ShareErrorCode> {
        let map_key = (group.to_string(), topic_id, partition);
        let state_partition = self.state_partition_for(group, &topic_id, partition);
        let key = ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: group.to_string(),
            topic_id,
            partition,
        };
        self.persist_record(state_partition, key, None)
            .await
            .map_err(|e| {
                warn!(error = %e, "share delete persist failed");
                crate::codes::COORDINATOR_NOT_AVAILABLE
            })?;
        self.state.remove(&map_key);
        Ok(())
    }

    /// Append a single `(key, value)` record to `__share_group_state`-`p` and
    /// return its base offset. A `None` value writes a tombstone.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Share`] if the partition log is not open
    /// locally, or the underlying append error if `produce_batch` fails.
    async fn persist_record(
        &self,
        state_partition: PartitionIndex,
        key: ShareStateKey,
        value: Option<Bytes>,
    ) -> Result<Offset, BrokerError> {
        let part = self
            .partitions
            .get(bootstrap::TOPIC, state_partition)
            .ok_or_else(|| {
                BrokerError::Share(format!("__share_group_state-{state_partition} not local"))
            })?;

        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: Some(encode_state_key(&key)),
            value,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        part.produce_batch(batch).await
    }

    /// Best-effort log-prefix pruning for `state_partition`. Computes the
    /// smallest `last_snapshot_offset` across every live key mapped to that
    /// state partition (`redundant_offset`); if it exceeds the partition's
    /// current `log_start_offset`, trims the log up to it. Every retained key
    /// keeps its latest snapshot, so the trim is safe. Errors are logged and
    /// swallowed — pruning never fails a write.
    async fn maybe_prune(&self, state_partition: PartitionIndex) {
        let Some(part) = self.partitions.get(bootstrap::TOPIC, state_partition) else {
            return;
        };

        // Collect this partition's keys' last-snapshot offsets.
        let handles: Vec<Arc<Mutex<SharePartitionState>>> = self
            .state
            .iter()
            .filter(|e| {
                let (g, t, p) = e.key();
                self.state_partition_for(g, t, *p) == state_partition
            })
            .map(|e| e.value().clone())
            .collect();

        let mut offsets = Vec::with_capacity(handles.len());
        for h in handles {
            offsets.push(h.lock().await.last_snapshot_offset);
        }

        let Some(redundant) = redundant_offset(&offsets) else {
            return;
        };
        if redundant > part.log_start_offset()
            && let Err(e) = part.trim_to_offset(redundant).await
        {
            warn!(
                partition = state_partition.get(),
                error = %e,
                "share-state log prune failed; continuing"
            );
        }
    }

    /// Replay every locally-led `__share_group_state` partition into the
    /// in-memory state map. Called from `Broker::start`.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError`] if refreshing leadership fails. Per-partition
    /// read errors are logged and skipped (treated as "nothing to replay").
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BrokerError> {
        self.refresh_leader_partitions(image).await;
        self.replay_led_partitions().await;
        info!(
            keys_loaded = self.state.len(),
            "ShareCoordinator recovery complete"
        );
        Ok(())
    }

    /// Replay every currently-led `__share_group_state` partition's log into
    /// the in-memory state map. Assumes `leader_partitions` is already
    /// populated (via `refresh_leader_partitions`).
    async fn replay_led_partitions(&self) {
        let local_partitions: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        for p in local_partitions {
            let Some(part) = self.partitions.get(bootstrap::TOPIC, p) else {
                continue;
            };

            let mut offset = part.log_start_offset();
            loop {
                let out = match part.read_log(offset, RECOVERY_READ_MAX_BYTES) {
                    Ok(o) => o,
                    Err(e) => {
                        warn!(
                            partition = p.get(),
                            error = %e,
                            "read error during __share_group_state recovery; skipping partition"
                        );
                        break;
                    }
                };

                if out.batches.is_empty() {
                    break;
                }

                for batch in &out.batches {
                    for rec in &batch.records {
                        let rec_offset = Offset(batch.base_offset + i64::from(rec.offset_delta));
                        let Some(key_bytes) = rec.key.as_ref() else {
                            continue;
                        };
                        let key = match parse_state_key(key_bytes) {
                            Ok(k) => k,
                            Err(e) => {
                                warn!(
                                    partition = p.get(),
                                    error = %e,
                                    "invalid share-state key; skipping record"
                                );
                                continue;
                            }
                        };
                        let map_key = (key.group_id.clone(), key.topic_id, key.partition);

                        // Tombstone: drop the in-memory entry.
                        let Some(value) = rec.value.as_ref() else {
                            self.state.remove(&map_key);
                            continue;
                        };

                        self.replay_value(&key, &map_key, value, rec_offset, p);
                    }
                    offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                }
            }
        }
    }

    /// Fold one replayed record value into the in-memory state map. Snapshot
    /// records reset the state and record `last_snapshot_offset`; update
    /// records apply a delta.
    fn replay_value(
        &self,
        key: &ShareStateKey,
        map_key: &ShareStateKey3,
        value: &Bytes,
        rec_offset: Offset,
        partition: PartitionIndex,
    ) {
        let entry = self
            .state
            .entry(map_key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(SharePartitionState::default())))
            .value()
            .clone();
        // Recovery runs single-threaded before the coordinator is shared, so
        // the lock is uncontended; `try_lock` keeps `recover` non-async here.
        let mut st = entry
            .try_lock()
            .expect("share-state recovery lock uncontended");

        match key.record_type {
            KEY_SHARE_SNAPSHOT => match ShareSnapshotValue::decode(value) {
                Ok(snap) => {
                    st.apply_snapshot(&snap);
                    st.last_snapshot_offset = rec_offset;
                }
                Err(e) => warn!(
                    partition = partition.get(),
                    error = %e,
                    "invalid ShareSnapshot value; skipping record"
                ),
            },
            KEY_SHARE_UPDATE => match ShareUpdateValue::decode(value) {
                Ok(upd) => st.apply_update(&upd),
                Err(e) => warn!(
                    partition = partition.get(),
                    error = %e,
                    "invalid ShareUpdate value; skipping record"
                ),
            },
            other => warn!(
                partition = partition.get(),
                record_type = other,
                "unknown share-state record type"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use assert2::{assert, check};
    use crabka_log::{Log, LogConfig};
    use tempfile::tempdir;

    use super::*;

    fn batch(first: i64, last: i64) -> StateBatch {
        StateBatch {
            first_offset: Offset(first),
            last_offset: Offset(last),
            delivery_state: 0,
            delivery_count: 1,
        }
    }

    /// Build a real `__share_group_state`-`p` partition (with a live writer)
    /// and register it. Mirrors `partition_registry`'s `fixture_partition`.
    fn open_state_partition(reg: &PartitionRegistry, log_dir: &Path, p: i32) {
        let part_dir = crate::log_dir::partition_dir(log_dir, bootstrap::TOPIC, p);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = Log::open(&part_dir, LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            bootstrap::TOPIC.to_string(),
            PartitionIndex(p),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );
        reg.insert(bootstrap::TOPIC.to_string(), PartitionIndex(p), part);
    }

    /// A coordinator that leads every state partition it touches, with all
    /// 50 `__share_group_state` partitions opened locally.
    fn coordinator(dir: &Path) -> (ShareCoordinator, Arc<PartitionRegistry>) {
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..bootstrap::NUM_PARTITIONS {
            open_state_partition(&reg, dir, p);
        }
        let coord = ShareCoordinator::new(
            crabka_audit::NodeId(1),
            reg.clone(),
            ShareCoordinatorConfig::default(),
        );
        (coord, reg)
    }

    async fn lead_all(coord: &ShareCoordinator) {
        let mut set = HashSet::new();
        for p in 0..bootstrap::NUM_PARTITIONS {
            set.insert(PartitionIndex(p));
        }
        *coord.leader_partitions.write().await = set;
    }

    #[tokio::test]
    async fn initialize_then_read() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([3; 16]);

        coord.initialize("g", tid, 0, 5, Offset(100)).await.unwrap();

        let st = coord.read("g", tid, 0).await.expect("present");
        assert!(st.state_epoch == 5);
        assert!(st.start_offset == 100);
        let summary = coord.read_summary("g", tid, 0).await.expect("present");
        assert!(summary == (5, 0, Offset(100), 0));
    }

    #[tokio::test]
    async fn initialize_fences_stale_state_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([4; 16]);

        coord.initialize("g", tid, 0, 5, Offset(0)).await.unwrap();
        let err = coord
            .initialize("g", tid, 0, 5, Offset(0))
            .await
            .unwrap_err();
        assert!(err == crate::codes::FENCED_STATE_EPOCH);
    }

    #[tokio::test]
    async fn write_advances_spso_and_summary_matches() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([5; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        coord
            .write("g", tid, 0, 1, 2, Offset(50), 7, vec![batch(50, 59)])
            .await
            .unwrap();

        let st = coord.read("g", tid, 0).await.expect("present");
        check!(st.state_epoch == 1);
        check!(st.leader_epoch == 2);
        check!(st.start_offset == 50);
        check!(st.delivery_complete_count == 7);
        check!(st.state_batches == vec![batch(50, 59)]);

        let summary = coord.read_summary("g", tid, 0).await.expect("present");
        assert!(summary == (1, 2, Offset(50), 7));
    }

    #[tokio::test]
    async fn write_fences_stale_state_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([6; 16]);

        coord.initialize("g", tid, 0, 5, Offset(0)).await.unwrap();
        let err = coord
            .write("g", tid, 0, 4, 0, Offset(0), 0, vec![])
            .await
            .unwrap_err();
        assert!(err == crate::codes::FENCED_STATE_EPOCH);
    }

    #[tokio::test]
    async fn write_fences_stale_leader_epoch() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([7; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        coord
            .write("g", tid, 0, 1, 5, Offset(0), 0, vec![])
            .await
            .unwrap();
        let err = coord
            .write("g", tid, 0, 1, 4, Offset(0), 0, vec![])
            .await
            .unwrap_err();
        assert!(err == crate::codes::FENCED_LEADER_EPOCH);
    }

    #[tokio::test]
    async fn delete_removes_state() {
        let dir = tempdir().unwrap();
        let (coord, _reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([8; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        assert!(coord.read("g", tid, 0).await.is_some());
        coord.delete("g", tid, 0).await.unwrap();
        assert!(coord.read("g", tid, 0).await.is_none());
    }

    #[tokio::test]
    async fn snapshot_fold_after_threshold_resets_counter() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..bootstrap::NUM_PARTITIONS {
            open_state_partition(&reg, dir.path(), p);
        }
        // Small threshold so a few writes trigger a fold.
        let cfg = ShareCoordinatorConfig {
            snapshot_update_records_per_snapshot: 3,
            ..ShareCoordinatorConfig::default()
        };
        let coord = ShareCoordinator::new(crabka_audit::NodeId(1), reg.clone(), cfg);
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([9; 16]);

        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        for i in 0..3 {
            let base = i64::from(i) * 10;
            coord
                .write("g", tid, 0, 1, 1, Offset(0), 0, vec![batch(base, base + 9)])
                .await
                .unwrap();
        }

        let st = coord.read("g", tid, 0).await.expect("present");
        // After the 3rd update crossed the threshold, a snapshot was folded
        // and the counter reset.
        assert!(st.updates_since_snapshot == 0);
        assert!(st.snapshot_epoch == 1);
    }

    #[tokio::test]
    async fn write_persists_and_recovers() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..bootstrap::NUM_PARTITIONS {
            open_state_partition(&reg, dir.path(), p);
        }
        let tid = uuid::Uuid::from_bytes([10; 16]);
        {
            let coord = ShareCoordinator::new(
                crabka_audit::NodeId(1),
                reg.clone(),
                ShareCoordinatorConfig::default(),
            );
            lead_all(&coord).await;
            coord.initialize("g", tid, 0, 2, Offset(0)).await.unwrap();
            coord
                .write("g", tid, 0, 2, 3, Offset(20), 4, vec![batch(20, 29)])
                .await
                .unwrap();
        }

        // New coordinator over the SAME registry (same open logs); recover
        // replays the records written above.
        let recovered = ShareCoordinator::new(
            crabka_audit::NodeId(1),
            reg.clone(),
            ShareCoordinatorConfig::default(),
        );
        lead_all(&recovered).await;
        // `recover` re-derives leadership from a MetadataImage; here we seed
        // the leadership set directly (lead_all) and replay the open logs.
        recovered.replay_led_partitions().await;

        let st = recovered.read("g", tid, 0).await.expect("recovered");
        check!(st.state_epoch == 2);
        check!(st.leader_epoch == 3);
        check!(st.start_offset == 20);
        check!(st.delivery_complete_count == 4);
        check!(st.state_batches == vec![batch(20, 29)]);
    }

    // `replay_led_partitions` must derive each record's offset as
    // `base_offset + offset_delta` and advance the inter-batch cursor as
    // `base_offset + last_offset_delta + 1`. A hand-crafted TWO-record batch
    // (an update at offset_delta 0, then a snapshot at offset_delta 1) pins
    // both: after replay, the snapshot's recorded `last_snapshot_offset` must
    // be `base_offset + 1`, and a second batch appended after it must also be
    // replayed (only reachable when the cursor advances by
    // `last_offset_delta + 1`).
    #[tokio::test]
    async fn replay_uses_per_record_and_inter_batch_offsets() {
        let dir = tempdir().unwrap();
        let (coord, reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([11; 16]);
        let state_partition = coord.state_partition_for("g", &tid, 0);
        let part = reg
            .get(bootstrap::TOPIC, state_partition)
            .expect("state partition open");

        let snap_key = encode_state_key(&ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: "g".to_string(),
            topic_id: tid,
            partition: 0,
        });
        let upd_key = encode_state_key(&ShareStateKey {
            record_type: KEY_SHARE_UPDATE,
            group_id: "g".to_string(),
            topic_id: tid,
            partition: 0,
        });

        // Batch A (base_offset 0): an UPDATE at delta 0, then a SNAPSHOT at
        // delta 1 (last_offset_delta = 1). The snapshot's rec_offset is
        // `base_offset + 1 == 1`.
        let mut batch_a = RecordBatch {
            last_offset_delta: 1,
            ..RecordBatch::default()
        };
        batch_a.records.push(Record {
            offset_delta: 0,
            key: Some(upd_key.clone()),
            value: Some(
                ShareUpdateValue {
                    snapshot_epoch: 0,
                    leader_epoch: 1,
                    start_offset: Offset(0),
                    delivery_complete_count: 0,
                    state_batches: vec![],
                }
                .encode(),
            ),
            ..Default::default()
        });
        batch_a.records.push(Record {
            offset_delta: 1,
            key: Some(snap_key.clone()),
            value: Some(
                ShareSnapshotValue {
                    snapshot_epoch: 5,
                    state_epoch: 2,
                    leader_epoch: 3,
                    start_offset: Offset(20),
                    delivery_complete_count: 4,
                    state_batches: vec![batch(20, 29)],
                }
                .encode(),
            ),
            ..Default::default()
        });
        part.produce_batch(batch_a).await.unwrap();

        // Batch B (base_offset 2): a later SNAPSHOT. Only reached if the cursor
        // advanced past batch A by `last_offset_delta + 1`.
        let mut batch_b = RecordBatch::default();
        batch_b.records.push(Record {
            offset_delta: 0,
            key: Some(snap_key.clone()),
            value: Some(
                ShareSnapshotValue {
                    snapshot_epoch: 6,
                    state_epoch: 2,
                    leader_epoch: 9,
                    start_offset: Offset(50),
                    delivery_complete_count: 8,
                    state_batches: vec![batch(50, 59)],
                }
                .encode(),
            ),
            ..Default::default()
        });
        part.produce_batch(batch_b).await.unwrap();

        coord.replay_led_partitions().await;

        let st = coord.read("g", tid, 0).await.expect("recovered");
        // Batch B is the final snapshot — proves the inter-batch cursor advanced
        // past batch A (base_offset + last_offset_delta + 1 == 2).
        check!(st.leader_epoch == 9);
        check!(st.start_offset == 50);
        check!(st.delivery_complete_count == 8);
        check!(st.state_batches == vec![batch(50, 59)]);
        // Batch B's snapshot sits at base_offset 2 (single record, delta 0).
        check!(st.last_snapshot_offset == 2);
    }

    /// Replaying ONLY batch A pins the per-record offset arithmetic in
    /// isolation: the snapshot at `offset_delta 1` over `base_offset 0` records
    /// `last_snapshot_offset == 1`, not `Offset(-1)`.
    #[tokio::test]
    async fn replay_snapshot_offset_is_base_plus_delta() {
        let dir = tempdir().unwrap();
        let (coord, reg) = coordinator(dir.path());
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([12; 16]);
        let state_partition = coord.state_partition_for("g", &tid, 0);
        let part = reg
            .get(bootstrap::TOPIC, state_partition)
            .expect("state partition open");

        let snap_key = encode_state_key(&ShareStateKey {
            record_type: KEY_SHARE_SNAPSHOT,
            group_id: "g".to_string(),
            topic_id: tid,
            partition: 0,
        });
        let upd_key = encode_state_key(&ShareStateKey {
            record_type: KEY_SHARE_UPDATE,
            group_id: "g".to_string(),
            topic_id: tid,
            partition: 0,
        });

        // Single batch, base_offset 0: an UPDATE at delta 0 then a SNAPSHOT at
        // delta 1. The snapshot's rec_offset is `0 + 1 == 1`.
        let mut batch_a = RecordBatch {
            last_offset_delta: 1,
            ..RecordBatch::default()
        };
        batch_a.records.push(Record {
            offset_delta: 0,
            key: Some(upd_key),
            value: Some(
                ShareUpdateValue {
                    snapshot_epoch: 0,
                    leader_epoch: 1,
                    start_offset: Offset(0),
                    delivery_complete_count: 0,
                    state_batches: vec![],
                }
                .encode(),
            ),
            ..Default::default()
        });
        batch_a.records.push(Record {
            offset_delta: 1,
            key: Some(snap_key),
            value: Some(
                ShareSnapshotValue {
                    snapshot_epoch: 5,
                    state_epoch: 2,
                    leader_epoch: 3,
                    start_offset: Offset(20),
                    delivery_complete_count: 4,
                    state_batches: vec![batch(20, 29)],
                }
                .encode(),
            ),
            ..Default::default()
        });
        part.produce_batch(batch_a).await.unwrap();

        coord.replay_led_partitions().await;

        let st = coord.read("g", tid, 0).await.expect("recovered");
        check!(st.leader_epoch == 3);
        check!(st.start_offset == 20);
        // The snapshot record sits at base_offset(0) + offset_delta(1) == 1.
        check!(st.last_snapshot_offset == 1);
    }

    /// After a snapshot fold, `maybe_prune` must trim the state-partition log
    /// prefix up to the redundant offset (the folded snapshot's offset). The
    /// partition's `log_start_offset` advances past 0; if pruning is skipped it
    /// stays at 0.
    #[tokio::test]
    async fn snapshot_fold_prunes_log_prefix() {
        let dir = tempdir().unwrap();
        let reg = Arc::new(PartitionRegistry::new());
        for p in 0..bootstrap::NUM_PARTITIONS {
            open_state_partition(&reg, dir.path(), p);
        }
        // Fold after 2 updates so a snapshot lands a few records in.
        let cfg = ShareCoordinatorConfig {
            snapshot_update_records_per_snapshot: 2,
            ..ShareCoordinatorConfig::default()
        };
        let coord = ShareCoordinator::new(crabka_audit::NodeId(1), reg.clone(), cfg);
        lead_all(&coord).await;
        let tid = uuid::Uuid::from_bytes([13; 16]);
        let state_partition = coord.state_partition_for("g", &tid, 0);
        let part = reg
            .get(bootstrap::TOPIC, state_partition)
            .expect("state partition open");

        // record 0: initialize snapshot.
        coord.initialize("g", tid, 0, 1, Offset(0)).await.unwrap();
        // records 1,2: updates; the 2nd crosses the threshold and folds a
        // snapshot at record 3, then prunes up to it.
        coord
            .write("g", tid, 0, 1, 1, Offset(0), 0, vec![batch(0, 9)])
            .await
            .unwrap();
        coord
            .write("g", tid, 0, 1, 1, Offset(0), 0, vec![batch(10, 19)])
            .await
            .unwrap();

        // The folded snapshot's offset is the sole key's last-snapshot offset,
        // which is > 0 and exceeds the log's initial start (0), so the prune
        // advanced the prefix. Without pruning the start stays at 0.
        let start = part.log_start_offset();
        check!(start > 0);
    }
}
