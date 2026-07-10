//! `RemoteLogManager` — KIP-405 tiered-storage copy path.
//!
//! Every `interval`, walks the partition registry and, for each partition
//! where this broker is the leader and the topic has
//! `remote.storage.enable=true`, copies the partition's sealed log
//! segments that are not yet in the remote tier to a
//! [`RemoteStorageManager`], recording each copy in a
//! [`RemoteLogMetadataManager`] (`CopySegmentStarted` →
//! `CopySegmentFinished`).
//!
//! This is the copy path. Local-retention deletion of copied segments and the
//! remote read path on `Fetch` are implemented in their own modules. The
//! remote-storage SPIs are blocking, so each copy / delete
//! runs on the `tokio` blocking pool.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, atomic::Ordering},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use crabka_log::{LogConfig, Offset, SegmentExport};
use crabka_metadata::NodeId;
use crabka_remote_storage::{
    LogSegmentData, RemoteLogMetadataManager, RemoteLogSegmentId, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
    RemotePartitionDeleteState, RemoteStorageManager, TopicIdPartition,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{partition::Partition, partition_registry::PartitionRegistry};

/// Default cadence of the tiered-storage sweep (copy + retention passes).
const DEFAULT_TIERING_INTERVAL: Duration = Duration::from_secs(30);

/// Tunables for [`run`].
#[derive(Debug, Clone)]
pub(crate) struct RemoteLogManagerConfig {
    pub interval: Duration,
}

impl Default for RemoteLogManagerConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_TIERING_INTERVAL,
        }
    }
}

/// Spawned task entry point. Ticks every `cfg.interval` until `shutdown`.
#[allow(clippy::too_many_arguments)] // task dependencies; bundling would obscure them
pub(crate) async fn run(
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
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
        tick_all(&partitions, &*controller, &rsm, &rlmm, node_id, broker_id).await;
    }
}

async fn tick_all(
    partitions: &PartitionRegistry,
    controller: &dyn crate::metadata_source::MetadataSource,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    node_id: NodeId,
    broker_id: i32,
) {
    // Snapshot first to avoid holding any registry guard across an await.
    let snapshot: Vec<Arc<Partition>> = partitions.arcs();
    let image = controller.current_image();
    for partition in snapshot {
        if partition.current_leader.load(Ordering::Relaxed) != node_id {
            continue;
        }
        // Read config + sealed-segment list under the log lock, then drop it.
        let (log_config, exports) = {
            let log = partition.log.lock().expect("log mutex poisoned");
            let cfg = log.config_snapshot();
            if !cfg.remote_storage_enable {
                continue;
            }
            (cfg, log.tierable_segments())
        };
        if exports.is_empty() {
            continue;
        }
        let Some(topic_id) = image.topic(&partition.topic).map(|t| t.topic_id) else {
            // Topic vanished from the metadata image between snapshots; skip.
            continue;
        };
        // Atomic stores the raw epoch; wrap for the remote-storage metadata seam.
        let leader_epoch =
            crabka_ids::LeaderEpoch(partition.current_leader_epoch.load(Ordering::Acquire));
        let tp = TopicIdPartition::new(
            topic_id,
            partition.topic.clone(),
            partition.partition_id.get(),
        );
        copy_eligible(&tp, broker_id, leader_epoch, exports.clone(), rsm, rlmm).await;
        local_retention_pass(&tp, &partition, &exports, &log_config, rlmm, now_ms()).await;
        remote_retention_pass(&tp, broker_id, &log_config, rsm, rlmm, now_ms()).await;
    }
}

/// Copy every sealed segment in `exports` that the metadata store does not
/// already know about. Returns the number of segments newly copied to
/// `CopySegmentFinished`. Factored out of [`tick_all`] so it can be driven
/// directly in tests against a real `Log` + reference RSM/RLMM.
pub(crate) async fn copy_eligible(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: crabka_ids::LeaderEpoch,
    exports: Vec<SegmentExport>,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
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
        if known.contains(&ex.base_offset.0) {
            continue;
        }
        if copy_one(tp, broker_id, leader_epoch, &ex, rsm, rlmm).await {
            copied += 1;
        }
    }
    copied
}

/// Compute the highest `target` to pass to
/// [`crabka_log::Log::delete_local_segments_through`] given the
/// partition's local sealed-segment exports and the per-topic
/// local-retention settings. Returns `None` when nothing is deletable.
///
/// A segment is eligible iff its `base_offset` is in `finished_bases`
/// (i.e. `CopySegmentFinished` in the RLMM) AND it satisfies either
/// time-based eviction (`now_ms - seg.max_timestamp > effective_local_ms`)
/// or size-based eviction (oldest-first until sealed-total fits
/// `effective_local_bytes`). The walk stops at the first non-finished
/// segment so the local prefix stays contiguous (matches Kafka).
///
/// Size-based eviction ignores the active segment — operators set
/// local.retention.bytes in MB/GB ranges where the active segment
/// (bounded by `segment.bytes`) is negligible.
pub(crate) fn local_retention_target(
    exports: &[SegmentExport],
    finished_bases: &HashSet<i64>,
    effective_local_ms: Option<i64>,
    effective_local_bytes: Option<u64>,
    now_ms: i64,
) -> Option<i64> {
    let sealed_total: u64 = exports.iter().map(|e| e.size_bytes).sum();
    let mut deletable_size_remaining =
        effective_local_bytes.map_or(0, |budget| sealed_total.saturating_sub(budget));

    let mut delete_through_last: Option<i64> = None;
    for ex in exports {
        if !finished_bases.contains(&ex.base_offset.0) {
            break;
        }
        let by_time = matches!(
            effective_local_ms,
            Some(retention) if now_ms.saturating_sub(ex.max_timestamp) > retention
        );
        let by_size = deletable_size_remaining > 0;
        if !(by_time || by_size) {
            break;
        }
        delete_through_last = Some(ex.last_offset.0);
        if by_size {
            deletable_size_remaining = deletable_size_remaining.saturating_sub(ex.size_bytes);
        }
    }

    delete_through_last.map(|last| last + 1)
}

/// After the copy pass, drop local sealed segments whose
/// remote copy is `CopySegmentFinished` and that fall outside the
/// per-topic local-retention window. Returns the count of segments
/// physically removed from disk.
// Async mirrors `copy_eligible` and gives the call site a stable signature
// for the day the RLMM SPI grows async fetch methods.
#[allow(clippy::unused_async)]
pub(crate) async fn local_retention_pass(
    tp: &TopicIdPartition,
    partition: &Partition,
    exports: &[SegmentExport],
    log_config: &LogConfig,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> usize {
    let effective_local_ms = log_config
        .local_retention_ms
        .or(log_config.retention_ms)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let effective_local_bytes = log_config
        .local_retention_bytes
        .or(log_config.retention_bytes);

    let finished_bases: HashSet<i64> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for local retention");
            return 0;
        }
    };

    let Some(target) = local_retention_target(
        exports,
        &finished_bases,
        effective_local_ms,
        effective_local_bytes,
        now_ms,
    ) else {
        return 0;
    };

    let result = {
        let mut log = partition.log.lock().expect("log mutex poisoned");
        log.delete_local_segments_through(Offset(target))
    };
    match result {
        Ok(n) => {
            debug!(topic = %tp.topic, partition = tp.partition, target, removed = n,
                   "remote-log-manager: local-retention deletion pass completed");
            n
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, target, error = %e,
                  "remote-log-manager: failed to delete local segments");
            0
        }
    }
}

/// KIP-405: compute the set of finished remote segments whose
/// total-retention window has expired (by time or by size budget), in
/// oldest-first order. Mirrors [`local_retention_target`]'s walk; **stops at
/// the first non-deletable segment** so the remaining remote prefix stays
/// contiguous (matches Kafka).
///
/// A segment is deletable when either:
/// - `now_ms - md.max_timestamp_ms > retention_ms`, or
/// - the running sum of sizes from the oldest forward must exceed
///   `total_bytes - retention_bytes` (greedy size eviction).
///
/// `None` settings disable that axis. Caller must already have filtered to
/// `CopySegmentFinished` and sorted by `start_offset`.
pub(crate) fn remote_retention_eviction_set(
    finished: &[RemoteLogSegmentMetadata],
    retention_ms: Option<i64>,
    retention_bytes: Option<u64>,
    now_ms: i64,
) -> Vec<RemoteLogSegmentMetadata> {
    let total: u64 = finished
        .iter()
        .map(|m| u64::try_from(m.segment_size_in_bytes().max(0)).unwrap_or(0))
        .sum();
    let mut size_to_reclaim = retention_bytes.map_or(0, |budget| total.saturating_sub(budget));
    let mut out = Vec::new();
    for md in finished {
        let by_time = matches!(
            retention_ms,
            Some(window) if now_ms.saturating_sub(md.max_timestamp_ms()) > window
        );
        let by_size = size_to_reclaim > 0;
        if !(by_time || by_size) {
            break;
        }
        let bytes = u64::try_from(md.segment_size_in_bytes().max(0)).unwrap_or(0);
        if by_size {
            size_to_reclaim = size_to_reclaim.saturating_sub(bytes);
        }
        out.push(md.clone());
    }
    out
}

/// KIP-405: evict remote segments past the topic's total
/// retention window (`retention.ms` / `retention.bytes`). For each
/// deletable segment, runs the lifecycle:
/// `CopySegmentFinished` → `DeleteSegmentStarted` → RSM delete →
/// `DeleteSegmentFinished`. Failures log at WARN and short-circuit the
/// partition's pass — leftover `DeleteSegmentStarted` metadata is invisible
/// to the read path's finished-only filter and gets retried on the next
/// tick. Returns the count of segments transitioned to
/// `DeleteSegmentFinished` (i.e. successfully evicted).
pub(crate) async fn remote_retention_pass(
    tp: &TopicIdPartition,
    broker_id: i32,
    log_config: &LogConfig,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    now_ms: i64,
) -> usize {
    let retention_ms = log_config
        .retention_ms
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
    let retention_bytes = log_config.retention_bytes;
    if retention_ms.is_none() && retention_bytes.is_none() {
        return 0;
    }

    let mut finished: Vec<RemoteLogSegmentMetadata> = match rlmm.list_remote_log_segments(tp) {
        Ok(list) => list
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .collect(),
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list remote segments for retention");
            return 0;
        }
    };
    finished.sort_by_key(RemoteLogSegmentMetadata::start_offset);

    let evict = remote_retention_eviction_set(&finished, retention_ms, retention_bytes, now_ms);
    let mut deleted = 0;
    for md in evict {
        if delete_one_segment(tp, broker_id, &md, rsm, rlmm).await {
            deleted += 1;
        } else {
            // Stop at the first failure to preserve the contiguous-prefix
            // invariant — the next tick re-tries from the same base.
            break;
        }
    }
    deleted
}

/// KIP-405: cascade the
/// [`DeletePartitionMarked` → `DeletePartitionStarted` →
/// `DeletePartitionFinished`] lifecycle for `tp`, deleting every remote
/// segment along the way. Run as a detached task from the `DeleteTopics`
/// handler so the response doesn't wait on remote-tier I/O. Failures log
/// at WARN; leftover `DeleteSegmentStarted` segments are harmless in the
/// in-memory RLMM (a `DeleteTopics`-recreate combination regenerates the
/// topic id and the new partition is a fresh `TopicIdPartition`).
pub(crate) async fn cascade_remote_partition_delete(
    tp: TopicIdPartition,
    broker_id: i32,
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
) {
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionMarked,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to mark partition deleted");
        return;
    }
    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionStarted,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to start partition delete");
        return;
    }

    let segments = match rlmm.list_remote_log_segments(&tp) {
        Ok(list) => list,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, error = %e,
                  "remote-log-manager: failed to list segments for partition delete");
            return;
        }
    };
    for md in segments {
        // Skip segments already past `DeleteSegmentStarted` (no-op delete).
        if md.state() == RemoteLogSegmentState::DeleteSegmentFinished {
            continue;
        }
        let _ = delete_one_segment(&tp, broker_id, &md, &rsm, &rlmm).await;
    }

    if let Err(e) = put_partition_state(
        &rlmm,
        &tp,
        RemotePartitionDeleteState::DeletePartitionFinished,
        broker_id,
    )
    .await
    {
        warn!(topic = %tp.topic, partition = tp.partition, error = %e,
              "remote-log-manager: failed to finish partition delete");
    }
}

/// Run one blocking [`RemoteLogMetadataManager`] mutation on the blocking
/// pool. The topic-backed manager's synchronous SPI methods bridge to a
/// Tokio runtime via `block_on`, which panics if invoked on a runtime
/// worker thread; `spawn_blocking` hands them a thread that is allowed to
/// block (for the in-memory manager the closure is a cheap no-op there).
/// Mirrors the `spawn_blocking` wrapping already used for the blocking
/// [`RemoteStorageManager`] SPI in this module.
async fn rlmm_mutate<F>(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    op: F,
) -> Result<(), crabka_remote_storage::RemoteStorageError>
where
    F: FnOnce(
            &dyn RemoteLogMetadataManager,
        ) -> Result<(), crabka_remote_storage::RemoteStorageError>
        + Send
        + 'static,
{
    let rlmm = Arc::clone(rlmm);
    match tokio::task::spawn_blocking(move || op(rlmm.as_ref())).await {
        Ok(res) => res,
        Err(e) => Err(crabka_remote_storage::RemoteStorageError::Backend(format!(
            "RLMM mutation task panicked: {e}"
        ))),
    }
}

async fn put_partition_state(
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
    tp: &TopicIdPartition,
    state: RemotePartitionDeleteState,
    broker_id: i32,
) -> Result<(), crabka_remote_storage::RemoteStorageError> {
    let md = RemotePartitionDeleteMetadata {
        topic_id_partition: tp.clone(),
        state,
        event_timestamp_ms: now_ms(),
        broker_id,
    };
    rlmm_mutate(rlmm, move |m| m.put_remote_partition_delete_metadata(md)).await
}

/// Drive one `CopySegmentFinished` (or in-flight) segment through the
/// `DeleteSegmentStarted` → RSM delete → `DeleteSegmentFinished` chain.
/// Returns `true` when the lifecycle completes cleanly. Shared by
/// [`remote_retention_pass`] and [`cascade_remote_partition_delete`].
async fn delete_one_segment(
    tp: &TopicIdPartition,
    broker_id: i32,
    md: &RemoteLogSegmentMetadata,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> bool {
    let id = md.remote_log_segment_id().clone();
    // Transition to DeleteSegmentStarted unless the segment is already
    // there (cascade may retry against a partially-cleaned partition).
    if md.state() == RemoteLogSegmentState::CopySegmentFinished {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state: RemoteLogSegmentState::DeleteSegmentStarted,
            broker_id,
        };
        if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await
        {
            warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                  error = %e,
                  "remote-log-manager: failed to record DeleteSegmentStarted");
            return false;
        }
    }

    // RSM delete (blocking).
    let rsm_del = rsm.clone();
    let md_del = md.clone();
    let delete_result =
        tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
    match delete_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                  error = %e, "remote-log-manager: RSM delete failed");
            return false;
        }
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
                  error = %e, "remote-log-manager: RSM delete task panicked");
            return false;
        }
    }

    let upd = RemoteLogSegmentMetadataUpdate {
        remote_log_segment_id: id,
        event_timestamp_ms: now_ms(),
        custom_metadata: None,
        state: RemoteLogSegmentState::DeleteSegmentFinished,
        broker_id,
    };
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await {
        warn!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
              error = %e, "remote-log-manager: failed to record DeleteSegmentFinished");
        return false;
    }
    debug!(topic = %tp.topic, partition = tp.partition, base = md.start_offset(),
           "remote-log-manager: deleted remote segment");
    true
}

/// Copy one sealed segment through the full `Started` → `Finished`
/// lifecycle. On any failure, the partial remote data is deleted and the
/// metadata is dropped (`DeleteSegmentStarted` → `DeleteSegmentFinished`)
/// so the segment is retried on the next tick. Returns `true` on success.
async fn copy_one(
    tp: &TopicIdPartition,
    broker_id: i32,
    leader_epoch: crabka_ids::LeaderEpoch,
    ex: &SegmentExport,
    rsm: &Arc<dyn RemoteStorageManager>,
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) -> bool {
    let id = RemoteLogSegmentId::new(tp.clone(), Uuid::new_v4());
    // Unwrap the log-layer `Offset`s into the remote-storage metadata's `i64`
    // world at the seam; the epoch map keeps its `LeaderEpoch` keys, which
    // `RemoteLogSegmentMetadata` carries verbatim.
    let epochs: BTreeMap<crabka_ids::LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
        BTreeMap::from([(
            crabka_ids::LeaderEpoch(leader_epoch.0.max(0)),
            ex.base_offset.0,
        )])
    } else {
        ex.leader_epochs
            .iter()
            .map(|&(epoch, off)| (epoch, off.0))
            .collect()
    };
    let size = i32::try_from(ex.size_bytes).unwrap_or(i32::MAX);

    let metadata = match RemoteLogSegmentMetadata::new(
        id.clone(),
        ex.base_offset.0,
        ex.last_offset.0,
        ex.max_timestamp,
        broker_id,
        now_ms(),
        size,
        RemoteLogSegmentState::CopySegmentStarted,
        epochs.clone(),
    ) {
        Ok(m) => m,
        Err(e) => {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: skipping segment with invalid metadata");
            return false;
        }
    };
    // KIP-405 txnIndexEmpty: set true when the log segment has no transaction
    // index file (non-transactional topics or segments written before txn support).
    let metadata = if ex.transaction_index_path.is_none() {
        metadata.with_txn_index_empty(true)
    } else {
        metadata
    };

    let md_started = metadata.clone();
    if let Err(e) = rlmm_mutate(rlmm, move |m| m.add_remote_log_segment_metadata(md_started)).await
    {
        warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
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
        if let Err(e) = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await
        {
            warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                  error = %e, "remote-log-manager: failed to record CopySegmentFinished");
            return false;
        }
        debug!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
               end = ex.last_offset.0, "remote-log-manager: copied segment to remote tier");
        return true;
    }

    // Copy failed (or the blocking task panicked): clean up so the segment
    // is retried next tick.
    match copy_result {
        Ok(Err(e)) => warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
                            error = %e, "remote-log-manager: segment copy failed"),
        Err(e) => warn!(topic = %tp.topic, partition = tp.partition, base = ex.base_offset.0,
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
    rlmm: &Arc<dyn RemoteLogMetadataManager>,
) {
    let id = metadata.remote_log_segment_id().clone();
    let rsm_del = rsm.clone();
    let md_del = metadata.clone();
    let _ = tokio::task::spawn_blocking(move || rsm_del.delete_log_segment_data(&md_del)).await;
    for state in [
        RemoteLogSegmentState::DeleteSegmentStarted,
        RemoteLogSegmentState::DeleteSegmentFinished,
    ] {
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: id.clone(),
            event_timestamp_ms: now_ms(),
            custom_metadata: None,
            state,
            broker_id,
        };
        let _ = rlmm_mutate(rlmm, move |m| m.update_remote_log_segment_metadata(upd)).await;
    }
}

/// Serialize a segment's leader-epoch map into Kafka's
/// `leader-epoch-checkpoint` text format (the bytes carried as
/// `LogSegmentData.leader_epoch_index`).
fn leader_epoch_index_bytes(epochs: &BTreeMap<crabka_ids::LeaderEpoch, i64>) -> Bytes {
    use std::fmt::Write as _;
    let mut s = String::from("0\n");
    let _ = writeln!(s, "{}", epochs.len());
    for (epoch, start) in epochs {
        // On-disk `leader-epoch-checkpoint` text format: unwrap to the raw
        // `i32` so the serialized bytes stay byte-identical.
        let _ = writeln!(s, "{} {start}", epoch.0);
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
    use assert2::{assert, check};
    use crabka_ids::{LeaderEpoch, PartitionIndex};
    use crabka_log::{Log, LogConfig};
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use crabka_protocol::records::{Record, RecordBatch};
    use crabka_remote_storage::{
        CustomMetadata, IndexType, InmemoryRemoteLogMetadataManager, LocalTieredStorage,
        RemoteStorageError,
    };

    use super::*;

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

    struct FixedMetadataSource {
        image: Arc<MetadataImage>,
        leader_tx: tokio::sync::watch::Sender<Option<NodeId>>,
    }

    impl FixedMetadataSource {
        fn new(image: MetadataImage) -> Self {
            let (leader_tx, _) = tokio::sync::watch::channel(Some(NodeId(1)));
            Self {
                image: Arc::new(image),
                leader_tx,
            }
        }
    }

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataSource for FixedMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }

        fn watch_image(&self) -> tokio::sync::watch::Receiver<Arc<MetadataImage>> {
            let (_, rx) = tokio::sync::watch::channel(self.image.clone());
            rx
        }

        fn watch_leader(&self) -> tokio::sync::watch::Receiver<Option<NodeId>> {
            self.leader_tx.subscribe()
        }

        fn quorum_state(&self) -> crabka_raft::QuorumState {
            crabka_raft::QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: *self.leader_tx.borrow(),
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(
            &self,
            _records: Vec<MetadataRecord>,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn change_membership(
            &self,
            _new_voters: std::collections::BTreeSet<NodeId>,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn add_learner(
            &self,
            _node_id: NodeId,
            _node: crabka_raft::Node,
        ) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        fn controller_bound_addr(&self) -> std::net::SocketAddr {
            std::net::SocketAddr::from(([0, 0, 0, 0], 0))
        }

        fn read_snapshot_range(
            &self,
            _position: i64,
            _max_bytes: i32,
        ) -> crabka_raft::SnapshotRange {
            crabka_raft::SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn add_voter(
            &self,
            _req: crabka_raft::AddVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn remove_voter(
            &self,
            _req: crabka_raft::RemoveVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn update_voter(
            &self,
            _req: crabka_raft::UpdateVoter,
        ) -> Result<crabka_raft::ReconfigOutcome, crabka_raft::RaftError> {
            Err(crabka_raft::RaftError::Unsupported("fixed metadata source"))
        }

        async fn cancel(&self) {}
    }

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn image_with_orders_topic() -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(9));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: tp().topic_id,
            partitions: 1,
            replication_factor: 1,
        }));
        image
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

    fn rolled_tiered_partition_with_config(
        log_dir: &std::path::Path,
        config: LogConfig,
    ) -> Arc<Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let mut log = Log::open(&part_dir, config).unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        );
        partition.current_leader.store(1, Ordering::Relaxed);
        partition.current_leader_epoch.store(0, Ordering::Release);
        partition
    }

    fn rolled_tiered_partition(log_dir: &std::path::Path) -> Arc<Partition> {
        rolled_tiered_partition_with_config(
            log_dir,
            LogConfig {
                segment_bytes: 256,
                remote_storage_enable: true,
                retention_ms: None,
                retention_bytes: None,
                ..LogConfig::default()
            },
        )
    }

    async fn wait_for_remote_segments(rlmm: &Arc<dyn RemoteLogMetadataManager>, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
                if listed.len() >= expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("remote-log-manager run loop did not copy expected segments");
    }

    #[tokio::test]
    async fn run_ticks_and_copies_eligible_segments() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(FixedMetadataSource::new(image_with_orders_topic()));
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(
            partitions,
            controller,
            rsm,
            rlmm.clone(),
            NodeId(1),
            1,
            RemoteLogManagerConfig {
                interval: Duration::from_millis(10),
            },
            shutdown.clone(),
        ));

        wait_for_remote_segments(&rlmm, export_count).await;
        shutdown.cancel();
        task.await.expect("remote-log-manager task panicked");

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == export_count);
        assert!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn tick_all_copies_local_leader_remote_enabled_partition() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        let export_count = partition
            .log
            .lock()
            .expect("partition log mutex poisoned")
            .tierable_segments()
            .len();
        assert!(export_count >= 2, "test needs multiple sealed segments");
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller = FixedMetadataSource::new(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(&partitions, &controller, &rsm, &rlmm, NodeId(1), 1).await;

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == export_count);
        assert!(
            listed
                .iter()
                .all(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
        );
    }

    #[tokio::test]
    async fn tick_all_skips_partition_led_by_other_node() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition(log_dir.path());
        partition.current_leader.store(2, Ordering::Relaxed);
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller = FixedMetadataSource::new(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(&partitions, &controller, &rsm, &rlmm, NodeId(1), 1).await;

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_all_skips_remote_storage_disabled_partition() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partitions = PartitionRegistry::new();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_bytes: 256,
                remote_storage_enable: false,
                retention_ms: None,
                retention_bytes: None,
                ..LogConfig::default()
            },
        );
        partitions.insert("orders".to_string(), PartitionIndex(0), partition);

        let controller = FixedMetadataSource::new(image_with_orders_topic());
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        tick_all(&partitions, &controller, &rsm, &rlmm, NodeId(1), 1).await;

        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
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
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(copied == exports.len());

        let listed = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(listed.len() == exports.len());
        for md in &listed {
            // The data + offset/leader-epoch indexes are fetchable (non-empty)
            // from the remote store.
            check!(md.state() == RemoteLogSegmentState::CopySegmentFinished);
            check!(!rsm.fetch_log_segment(md, 0, None).unwrap().is_empty());
            check!(!rsm.fetch_index(md, IndexType::Offset).unwrap().is_empty());
            check!(
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
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let first = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(first == exports.len());
        // Second pass: everything is already known → nothing re-copied.
        let second = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(second == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
    }

    #[tokio::test]
    async fn empty_exports_copies_nothing() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), Vec::new(), &rsm, &rlmm).await;
        assert!(copied == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_failure_rolls_back_and_leaves_no_metadata() {
        let log_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        assert!(!exports.is_empty());

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(AlwaysFailRsm);
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(copied == 0, "every copy failed");
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
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());

        // Hand-build an export with no leader epochs but real files on disk.
        let src = tempfile::tempdir().unwrap();
        let write = |name: &str, bytes: &[u8]| {
            let p = src.path().join(name);
            std::fs::write(&p, bytes).unwrap();
            p
        };
        let export = SegmentExport {
            base_offset: Offset(0),
            last_offset: Offset(9),
            max_timestamp: 42,
            size_bytes: 10,
            log_path: write("00.log", b"0123456789"),
            offset_index_path: write("00.index", b"i"),
            time_index_path: write("00.timeindex", b"t"),
            transaction_index_path: None,
            leader_epochs: Vec::new(),
        };

        let copied = copy_eligible(&tp(), 7, LeaderEpoch(3), vec![export], &rsm, &rlmm).await;
        assert!(copied == 1);
        let md = &rlmm.list_remote_log_segments(&tp()).unwrap()[0];
        // The fallback recorded the partition's current leader epoch (3).
        assert!(md.segment_leader_epochs().get(&LeaderEpoch(3)) == Some(&0));
    }

    fn synth_export(base: i64, last: i64, max_ts: i64, size: u64) -> SegmentExport {
        SegmentExport {
            base_offset: Offset(base),
            last_offset: Offset(last),
            max_timestamp: max_ts,
            size_bytes: size,
            log_path: std::path::PathBuf::new(),
            offset_index_path: std::path::PathBuf::new(),
            time_index_path: std::path::PathBuf::new(),
            transaction_index_path: None,
            leader_epochs: Vec::new(),
        }
    }

    #[test]
    fn local_retention_target_returns_none_when_no_finished_segments() {
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        let finished: HashSet<i64> = HashSet::new();
        // Big enough time-pressure to delete everything, but nothing is finished.
        assert!(local_retention_target(&exports, &finished, Some(1), None, 10_000) == None);
    }

    #[test]
    fn local_retention_target_time_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 5_000, 64),
        ];
        let finished: HashSet<i64> = [0, 10, 20].into_iter().collect();
        // now=1000, retention=500ms → segs with max_ts<500 are deletable.
        // Only seg0 (max_ts=100) and seg1 (max_ts=200) qualify; seg2 stops it.
        let target = local_retention_target(&exports, &finished, Some(500), None, 1_000);
        assert!(target == Some(20));
    }

    #[test]
    fn local_retention_target_size_based_eviction() {
        let exports = vec![
            synth_export(0, 9, 100, 100),
            synth_export(10, 19, 200, 100),
            synth_export(20, 29, 300, 100),
        ];
        let finished: HashSet<i64> = [0, 10, 20].into_iter().collect();
        let cases = [
            // Total = 300; budget = 150 → must evict 150 bytes → oldest two go.
            (Some(150), Some(20)),
            // Budget tighter than one segment: still only the oldest, because
            // after evicting 100B the remaining is 100 (>budget? no, 200>150,
            // wait: total=300, budget=150 → need to evict 150; after dropping
            // first 100B we still need 50 more → second segment also drops.
            // Test with budget = 50: need to evict 250 → all three? but the
            // walk stops since segments 0..=2 all become deletable.
            (Some(50), Some(30)),
            // Budget larger than total → nothing deletable.
            (Some(10_000), None),
        ];
        for (budget, expected) in cases {
            let target = local_retention_target(&exports, &finished, None, budget, 1_000);
            assert!(target == expected, "budget: {budget:?}");
        }
    }

    #[test]
    fn local_retention_target_equal_size_budget_keeps_all_segments() {
        let exports = vec![synth_export(0, 9, 100, 100), synth_export(10, 19, 200, 100)];
        let finished: HashSet<i64> = [0, 10].into_iter().collect();
        let target = local_retention_target(&exports, &finished, None, Some(200), 1_000);
        assert!(target == None);
    }

    #[test]
    fn local_retention_target_skips_unfinished_segments_and_stops() {
        let exports = vec![
            synth_export(0, 9, 100, 64),
            synth_export(10, 19, 200, 64),
            synth_export(20, 29, 300, 64),
        ];
        // Segment at base=10 has NOT been copy-finished. Walk stops there.
        let finished: HashSet<i64> = [0, 20].into_iter().collect();
        let target = local_retention_target(&exports, &finished, Some(1), None, 10_000);
        assert!(
            target == Some(10),
            "only seg0 deletable; walk stops at seg1"
        );
    }

    #[test]
    fn local_retention_target_uses_already_resolved_effective_ms() {
        // The pure helper takes already-resolved effective_* args. This test
        // pins that contract: when caller passes effective_local_ms equal to
        // the topic's retention_ms (the fallback), the helper deletes the
        // same set as if local_retention_ms had been set directly.
        let exports = vec![synth_export(0, 9, 100, 64), synth_export(10, 19, 200, 64)];
        let finished: HashSet<i64> = [0, 10].into_iter().collect();
        // Caller resolved effective_local_ms = retention_ms = 250ms; now=1000.
        let target = local_retention_target(&exports, &finished, Some(250), None, 1_000);
        assert!(target == Some(20));
    }

    /// Test-only drive helper: mirrors the body of `local_retention_pass`
    /// without the `Partition` wrapper, so we can exercise the integration
    /// against a real `Log` without standing up the broker fixtures.
    fn local_retention_drive(
        log: &mut Log,
        finished_bases: &HashSet<i64>,
        log_config: &LogConfig,
        now_ms: i64,
    ) -> usize {
        let effective_local_ms = log_config
            .local_retention_ms
            .or(log_config.retention_ms)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
        let effective_local_bytes = log_config
            .local_retention_bytes
            .or(log_config.retention_bytes);
        let exports = log.tierable_segments();
        let Some(target) = local_retention_target(
            &exports,
            finished_bases,
            effective_local_ms,
            effective_local_bytes,
            now_ms,
        ) else {
            return 0;
        };
        log.delete_local_segments_through(Offset(target)).unwrap()
    }

    #[tokio::test]
    async fn local_retention_drive_deletes_copied_segments() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            log_dir.path(),
            LogConfig {
                segment_bytes: 256,
                remote_storage_enable: true,
                local_retention_ms: Some(Duration::from_millis(1)),
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch(2);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");
        let log_config = log.config_snapshot();

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(copied == exports.len());

        // Gather finished bases the same way `local_retention_pass` would.
        let finished_bases: HashSet<i64> = rlmm
            .list_remote_log_segments(&tp())
            .unwrap()
            .iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect();
        assert!(finished_bases.len() == exports.len());

        // Drive retention with `now_ms` far in the future so every sealed
        // segment satisfies the 1ms time-based eviction.
        let future = now_ms() + 1_000_000;
        let removed = local_retention_drive(&mut log, &finished_bases, &log_config, future);
        assert!(removed == exports.len());

        // local_log_start_offset advanced; sealed log files are gone.
        let last = exports.last().unwrap().last_offset;
        assert!(log.local_log_start_offset() == last + 1);
        for ex in &exports {
            assert!(
                !ex.log_path.exists(),
                "sealed segment {:?} should be deleted",
                ex.log_path
            );
        }
        // Re-running is a no-op.
        let removed_again = local_retention_drive(&mut log, &finished_bases, &log_config, future);
        assert!(removed_again == 0);
    }

    #[tokio::test]
    async fn local_retention_pass_deletes_finished_segments_and_returns_count() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let partition = rolled_tiered_partition_with_config(
            log_dir.path(),
            LogConfig {
                segment_bytes: 256,
                remote_storage_enable: true,
                local_retention_ms: Some(Duration::from_millis(1)),
                ..LogConfig::default()
            },
        );
        let (exports, log_config) = {
            let log = partition.log.lock().expect("partition log mutex poisoned");
            (log.tierable_segments(), log.config_snapshot())
        };
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(copied == exports.len());

        let removed = local_retention_pass(
            &tp(),
            &partition,
            &exports,
            &log_config,
            &rlmm,
            now_ms() + 1_000_000,
        )
        .await;

        assert!(removed == exports.len());
        let log = partition.log.lock().expect("partition log mutex poisoned");
        assert!(log.local_log_start_offset() == exports.last().unwrap().last_offset + 1);
        assert!(log.tierable_segments().is_empty());
    }

    // ── remote-retention helper + cascade tests ────────────

    fn synth_remote_md(
        id: u128,
        start: i64,
        end: i64,
        max_ts: i64,
        size: i32,
    ) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            max_ts,
            1,
            max_ts,
            size,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(LeaderEpoch(0), start)]),
        )
        .unwrap()
        .with_update(&RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: max_ts,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        })
        .unwrap()
    }

    #[test]
    fn remote_retention_eviction_set_returns_empty_when_no_segments() {
        let out = remote_retention_eviction_set(&[], Some(1), Some(1), 10_000);
        assert!(out.is_empty());
    }

    #[test]
    fn remote_retention_eviction_set_time_based_picks_oldest_until_first_in_window() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 9_500, 100),
        ];
        // now=10_000, retention=500ms → seg with max_ts < 9_500 is deletable.
        // seg0 (100) + seg1 (200) qualify; seg2 (9_500) stops the walk.
        let out = remote_retention_eviction_set(&segs, Some(500), None, 10_000);
        assert!(
            out.iter()
                .map(|segment| segment.start_offset())
                .collect::<Vec<_>>()
                == vec![0, 10]
        );
    }

    #[test]
    fn remote_retention_eviction_set_size_based_evicts_oldest_first() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 300, 100),
        ];
        let cases = [
            // Total=300, budget=150 → reclaim 150 → oldest two go.
            (Some(150), 2),
            // Budget tighter than one segment → all three.
            (Some(50), 3),
            // Budget larger than total → none.
            (Some(10_000), 0),
        ];
        for (budget, expected_len) in cases {
            let out = remote_retention_eviction_set(&segs, None, budget, 1_000);
            assert!(out.len() == expected_len, "budget: {budget:?}");
        }
    }

    #[test]
    fn remote_retention_eviction_set_equal_size_budget_keeps_all_segments() {
        let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
        let out = remote_retention_eviction_set(&segs, None, Some(100), 1_000);
        assert!(out.is_empty());
    }

    #[test]
    fn remote_retention_eviction_set_time_and_size_take_union_of_either() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),
            synth_remote_md(11, 10, 19, 200, 100),
            synth_remote_md(12, 20, 29, 5_000, 100),
        ];
        // Time-window: seg0+seg1 qualify (max_ts<500). Budget very generous
        // so size-based evicts nothing. Result is the time-window prefix.
        let out = remote_retention_eviction_set(&segs, Some(500), Some(10_000), 1_000);
        assert!(out.len() == 2);
    }

    #[test]
    fn remote_retention_eviction_set_none_settings_disable_axis() {
        let segs = vec![synth_remote_md(10, 0, 9, 100, 100)];
        // No time or size → no eviction.
        assert!(remote_retention_eviction_set(&segs, None, None, 10_000).is_empty());
    }

    #[test]
    fn remote_retention_eviction_set_walk_stops_at_first_non_deletable() {
        let segs = vec![
            synth_remote_md(10, 0, 9, 100, 100),     // deletable by time
            synth_remote_md(11, 10, 19, 9_500, 100), // in window → stops walk
            synth_remote_md(12, 20, 29, 200, 100),   // also deletable by time, but
                                                     // walk stopped at seg1 already.
        ];
        let out = remote_retention_eviction_set(&segs, Some(500), None, 10_000);
        assert!(
            out.iter()
                .map(|segment| segment.start_offset())
                .collect::<Vec<_>>()
                == vec![0]
        );
    }

    #[tokio::test]
    async fn remote_retention_pass_evicts_old_segments_through_lifecycle() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(copied == exports.len());
        let pre = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(!pre.is_empty());

        let cfg = LogConfig {
            retention_ms: Some(Duration::from_millis(1)),
            ..LogConfig::default()
        };
        // far-future `now_ms` → every finished segment is past the window.
        let deleted =
            remote_retention_pass(&tp(), 1, &cfg, &rsm, &rlmm, now_ms() + 1_000_000).await;
        assert!(deleted == exports.len());

        // DeleteSegmentFinished drops the entries entirely from the cache.
        let post = rlmm.list_remote_log_segments(&tp()).unwrap();
        assert!(
            post.is_empty(),
            "every segment should be gone, got {} left",
            post.len()
        );
        // RSM data is gone too.
        for md in &pre {
            assert!(rsm.fetch_log_segment(md, 0, None).is_err());
        }
    }

    #[tokio::test]
    async fn remote_retention_pass_noop_when_nothing_qualifies() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;

        let cfg = LogConfig {
            // Long retention; nothing is past the window.
            retention_ms: Some(Duration::from_hours(8760)),
            retention_bytes: None,
            ..LogConfig::default()
        };
        // Use a `now_ms` close to the segments' max_timestamp so the test
        // is independent of wall-clock. `rolled_log` builds batches with
        // default base_timestamp=0, so picking now=1 keeps every segment
        // inside the year-long retention window.
        let deleted = remote_retention_pass(&tp(), 1, &cfg, &rsm, &rlmm, 1).await;
        assert!(deleted == 0);
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().len() == exports.len());
    }

    #[tokio::test]
    async fn remote_retention_pass_no_settings_no_op() {
        // Neither retention.ms nor retention.bytes — early return, no list.
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let cfg = LogConfig {
            retention_ms: None,
            retention_bytes: None,
            ..LogConfig::default()
        };
        let deleted = remote_retention_pass(&tp(), 1, &cfg, &rsm, &rlmm, now_ms()).await;
        assert!(deleted == 0);
    }

    #[tokio::test]
    async fn cascade_remote_partition_delete_drops_every_segment() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let log = rolled_log(log_dir.path());
        let exports = log.tierable_segments();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm_impl = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let rlmm: Arc<dyn RemoteLogMetadataManager> = rlmm_impl.clone();
        let copied = copy_eligible(&tp(), 1, LeaderEpoch(0), exports.clone(), &rsm, &rlmm).await;
        assert!(copied == exports.len());

        cascade_remote_partition_delete(tp(), 1, rsm.clone(), rlmm.clone()).await;

        // All segments are gone from the cache.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
        // The remote directory for this partition is empty (or absent).
        // LocalTieredStorage layout: <remote_dir>/<topic_id>/<partition>/.
        let part_dir = remote_dir.path().join(tp().topic_id.to_string()).join("0");
        if part_dir.exists() {
            let entries: Vec<_> = std::fs::read_dir(&part_dir).unwrap().collect();
            assert!(entries.is_empty(), "stray remote files: {entries:?}");
        }
        let dump = rlmm_impl.export();
        let partition = dump
            .partitions
            .iter()
            .find(|partition| partition.topic_id_partition == tp())
            .expect("partition delete state should be dumped");
        assert!(
            partition.delete_state == Some(RemotePartitionDeleteState::DeletePartitionFinished)
        );
    }

    #[tokio::test]
    async fn cascade_remote_partition_delete_is_noop_on_empty_partition() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        // No add — partition has no segments. Cascade still walks the
        // three partition-delete states without error.
        cascade_remote_partition_delete(tp(), 1, rsm, rlmm.clone()).await;
        // No segments after, no panics; that's the test.
        assert!(rlmm.list_remote_log_segments(&tp()).unwrap().is_empty());
    }

    #[test]
    fn now_ms_tracks_current_unix_epoch_millis() {
        let before = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let observed = now_ms();
        let after = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();

        assert!(observed >= before);
        assert!(observed <= after);
    }
}
