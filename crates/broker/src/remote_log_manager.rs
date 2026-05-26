//! `RemoteLogManager` — KIP-405 tiered-storage copy path (slice 48b).
//!
//! Every `interval`, walks the partition registry and, for each partition
//! where this broker is the leader and the topic has
//! `remote.storage.enable=true`, copies the partition's sealed log
//! segments that are not yet in the remote tier to a
//! [`RemoteStorageManager`], recording each copy in a
//! [`RemoteLogMetadataManager`] (`CopySegmentStarted` →
//! `CopySegmentFinished`).
//!
//! This is the copy path only. Local-retention deletion of copied
//! segments and the remote read path on `Fetch` land in later slices
//! (48c / 48d). The remote-storage SPIs are blocking, so each copy / delete
//! runs on the `tokio` blocking pool.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crabka_log::SegmentExport;
use crabka_metadata::NodeId;
use crabka_raft::ControllerHandle;
use crabka_remote_storage::{
    LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemoteStorageManager, TopicIdPartition,
};

use crate::partition::Partition;

/// Tunables for [`run`].
#[derive(Debug, Clone)]
pub(crate) struct RemoteLogManagerConfig {
    pub interval: Duration,
}

impl Default for RemoteLogManagerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
        }
    }
}

/// Spawned task entry point. Ticks every `cfg.interval` until `shutdown`.
#[allow(clippy::too_many_arguments)] // task dependencies; bundling would obscure them
pub(crate) async fn run(
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    controller: Arc<ControllerHandle>,
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
    node_id: NodeId,
    broker_id: i32,
    cfg: RemoteLogManagerConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                debug!("remote-log-manager task shutting down");
                return;
            }
        }
        tick_all(
            &partitions,
            &controller,
            &rsm,
            rlmm.as_ref(),
            node_id,
            broker_id,
        )
        .await;
    }
}

async fn tick_all(
    partitions: &DashMap<(String, i32), Arc<Partition>>,
    controller: &ControllerHandle,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &dyn RemoteLogMetadataManager,
    node_id: NodeId,
    broker_id: i32,
) {
    // Snapshot first to avoid holding the DashMap iterator across an await.
    let snapshot: Vec<Arc<Partition>> = partitions.iter().map(|kv| kv.value().clone()).collect();
    let image = controller.current_image();
    for partition in snapshot {
        if partition.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        // Read config + sealed-segment list under the log lock, then drop it.
        let exports = {
            let log = partition.log.lock().expect("log mutex poisoned");
            if !log.config_snapshot().remote_storage_enable {
                continue;
            }
            log.tierable_segments()
        };
        if exports.is_empty() {
            continue;
        }
        let Some(topic_id) = image.topic(&partition.topic).map(|t| t.topic_id) else {
            // Topic vanished from the metadata image between snapshots; skip.
            continue;
        };
        let leader_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
        let tp = TopicIdPartition::new(topic_id, partition.topic.clone(), partition.partition_id);
        copy_eligible(&tp, broker_id, leader_epoch, exports, rsm, rlmm).await;
    }
}

/// Copy every sealed segment in `exports` that the metadata store does not
/// already know about. Returns the number of segments newly copied to
/// `CopySegmentFinished`. Factored out of [`tick_all`] so it can be driven
/// directly in tests against a real `Log` + reference RSM/RLMM.
pub(crate) async fn copy_eligible(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: i32,
    exports: Vec<SegmentExport>,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &dyn RemoteLogMetadataManager,
) -> usize {
    let known: HashSet<i64> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .iter()
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments");
            return 0;
        }
    };

    let mut copied = 0;
    for ex in exports {
        if known.contains(&ex.base_offset) {
            continue;
        }
        if copy_one(tp, broker_id, leader_epoch, &ex, rsm, rlmm).await {
            copied += 1;
        }
    }
    copied
}

/// Copy one sealed segment through the full `Started` → `Finished`
/// lifecycle. On any failure, the partial remote data is deleted and the
/// metadata is dropped (`DeleteSegmentStarted` → `DeleteSegmentFinished`)
/// so the segment is retried on the next tick. Returns `true` on success.
async fn copy_one(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: i32,
    ex: &SegmentExport,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &dyn RemoteLogMetadataManager,
) -> bool {
    let id = RemoteLogSegmentId::new(tp.clone(), Uuid::new_v4());
    let epochs: BTreeMap<i32, i64> = if ex.leader_epochs.is_empty() {
        BTreeMap::from([(leader_epoch.max(0), ex.base_offset)])
    } else {
        ex.leader_epochs.iter().copied().collect()
    };
    let size = i32::try_from(ex.size_bytes).unwrap_or(i32::MAX);

    let metadata = match RemoteLogSegmentMetadata::new(
        id.clone(),
        ex.base_offset,
        ex.last_offset,
        ex.max_timestamp,
        broker_id,
        now_ms(),
        size,
        RemoteLogSegmentState::CopySegmentStarted,
        epochs.clone(),
    ) {
        Ok(m) => m,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset,
                  error = %e, "remote-log-manager: skipping segment with invalid metadata");
            return false;
        }
    };

    if let Err(e) = rlmm.add_remote_log_segment_metadata(metadata.clone()) {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset,
              error = %e, "remote-log-manager: failed to record CopySegmentStarted");
        return false;
    }

    let data = LogSegmentData {
        log_segment: ex.log_path.clone(),
        offset_index: ex.offset_index_path.clone(),
        time_index: ex.time_index_path.clone(),
        transaction_index: ex.transaction_index_path.clone(),
        // Crabka does not (yet) write producer-id snapshot files.
        producer_snapshot_index: None,
        leader_epoch_index: leader_epoch_index_bytes(&epochs),
    };

    // The RSM is a blocking SPI — run the copy on the blocking pool.
    let rsm_copy = rsm.clone();
    let md_copy = metadata.clone();
    let copy_result =
        tokio::task::spawn_blocking(move || rsm_copy.copy_log_segment_data(&md_copy, &data)).await;

    let copy_ok = matches!(copy_result, Ok(Ok(_)));
    if copy_ok {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id,
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id,
        };
        if let Err(e) = rlmm.update_remote_log_segment_metadata(upd) {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset,
                  error = %e, "remote-log-manager: failed to record CopySegmentFinished");
            return false;
        }
        debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset,
               end = ex.last_offset, "remote-log-manager: copied segment to remote tier");
        return true;
    }

    // Copy failed (or the blocking task panicked): clean up so the segment
    // is retried next tick.
    match copy_result {
        Ok(Err(e)) => warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset,
                            error = %e, "remote-log-manager: segment copy failed"),
        Err(e) => warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset,
                        error = %e, "remote-log-manager: segment copy task panicked"),
        Ok(Ok(_)) => unreachable!("copy_ok handled above"),
    }
    rollback(&metadata, broker_id, rsm, rlmm).await;
    false
}

/// Delete partial remote data and drop the metadata after a failed copy.
async fn rollback(
    metadata: &RemoteLogSegmentMetadata,
    broker_id: i32,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &dyn RemoteLogMetadataManager,
) {
    let id = metadata.remote_log_segment_id().clone();
    let rsm_del = rsm.clone();
    let md_del = metadata.clone();
    let _ = tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        let _ = rlmm.update_remote_log_segment_metadata(RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state,
            broker_id,
        });
    }
}

/// Serialize a segment's leader-epoch map into Kafka's
/// `leader-epoch-checkpoint` text format (the bytes carried as
/// `LogSegmentData.leader_epoch_index`).
fn leader_epoch_index_bytes(epochs: &BTreeMap<i32, i64>) -> Bytes {
    use std::fmt::Write as _;
    let mut s = String::from("0\n");
    let _ = writeln!(s, "{}", epochs.len());
    for (epoch, start) in epochs {
        let _ = writeln!(s, "{epoch} {start}");
    }
    Bytes::from(s.into_bytes())
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::{Record, RecordBatch};
    use crabka_remote_storage::{
        CustomMetadata, IndexType, InmemoryRemoteLogMetadataManager, LocalTieredStorage,
        RemoteStorageError,
    };

    /// An RSM whose copy always fails (delete succeeds). Used to exercise
    /// the failure rollback path.
    struct AlwaysFailRsm;

    impl RemoteStorageManager for AlwaysFailRsm {
        fn copy_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
            _data: &LogSegmentData,
        ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::InvalidArgument("boom".into()))
        }
        fn fetch_log_segment(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _start: u32,
            _end: Option<u32>,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn fetch_index(
            &self,
            metadata: &RemoteLogSegmentMetadata,
            _index_type: IndexType,
        ) -> Result<Vec<u8>, RemoteStorageError> {
            Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ))
        }
        fn delete_log_segment_data(
            &self,
            _metadata: &RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch {
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(vec![b'x'; 64])),
                ..Default::default()
            });
        }
        b
    }

    /// Build a log rolled into several sealed segments under `dir`.
    fn rolled_log(dir: &std::path::Path) -> Log {
        let mut log = Log::open(
            dir,
            LogConfig {
                segment_bytes: 256, // tiny so we roll fast
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        log
    }

    #[tokio::test]
    async fn copies_all_sealed_segments_and_records_finished() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm = InmemoryRemoteLogMetadataManager::new();

        let copied = copy_eligible(&tp(), 1, 0, exports.clone(), &rsm, &rlmm).await;
        assert_eq!(copied, exports.len());

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert_eq!(listed.len(), exports.len());
        for md in &listed {
            assert_eq!(md.state(), RemoteLogSegmentState::CopySegmentFinished);
            // The data + offset index are fetchable from the remote store.
            assert!(!rsm.fetch_log_segment(md, 0, None).unwrap().is_empty());
            assert!(!rsm.fetch_index(md, IndexType::Offset).unwrap().is_empty());
            assert!(
                !rsm.fetch_index(md, IndexType::LeaderEpoch)
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn re_running_is_idempotent() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm = InmemoryRemoteLogMetadataManager::new();

        let first = copy_eligible(&tp(), 1, 0, exports.clone(), &rsm, &rlmm).await;
        assert_eq!(first, exports.len());
        // Second pass: everything is already known → nothing re-copied.
        let second = copy_eligible(&tp(), 1, 0, exports.clone(), &rsm, &rlmm).await;
        assert_eq!(second, 0);
        assert_eq!(
            rlmm.list_remote_log_segments(&tp()).unwrap().len(),
            exports.len()
        );
    }

    #[tokio::test]
    async fn empty_exports_copies_nothing() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm = InmemoryRemoteLogMetadataManager::new();
        let copied = copy_eligible(&tp(), 1, 0, Vec::new(), &rsm, &rlmm).await;
        assert_eq!(copied, 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_failure_rolls_back_and_leaves_no_metadata() {
        let log_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(!exports.is_empty());

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AlwaysFailRsm);
        let rlmm = InmemoryRemoteLogMetadataManager::new();

        let copied = copy_eligible(&tp(), 1, 0, exports.clone(), &rsm, &rlmm).await;
        assert_eq!(copied, 0, "every copy failed");
        // Rollback (delete + DeleteSegmentStarted -> DeleteSegmentFinished)
        // drops the started metadata, so nothing is left behind and a later
        // run with a healthy store can retry the same segments.
        assert!(
            rlmm.list_remote_log_segments(&tp()).unwrap().is_empty(),
            "failed copies must not leave dangling metadata"
        );
    }

    #[tokio::test]
    async fn fallback_leader_epoch_when_export_has_none() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm = InmemoryRemoteLogMetadataManager::new();

        // Hand-build an export with no leader epochs but real files on disk.
        let src = tempfile::tempdir().unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let p = src.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            p
        };
        let export = SegmentExport {
            base_offset: 0,
            last_offset: 9,
            max_timestamp: 42,
            size_bytes: 10,
            log_path: write("00.log", b"0123456789"),
            offset_index_path: write("00.index", b"i"),
            time_index_path: write("00.timeindex", b"t"),
            transaction_index_path: None,
            leader_epochs: Vec::new(),
        };

        let copied = copy_eligible(&tp(), 7, 3, vec![export], &rsm, &rlmm).await;
        assert_eq!(copied, 1);
        let md = &rlmm.list_remote_log_segments(&tp()).unwrap()[0];
        // The fallback recorded the partition's current leader epoch (3).
        assert_eq!(md.segment_leader_epochs().get(&3), Some(&0));
    }
}
