//! [`TopicBasedRemoteLogMetadataManager`] — production
//! [`RemoteLogMetadataManager`] implementation backed by a publish /
//! subscribe [`MetadataEventLog`].
//!
//! The manager keeps the canonical in-memory view in an
//! [`InmemoryRemoteLogMetadataManager`] (so the 48a lifecycle state
//! machine is the single source of truth for cache mutation) and uses
//! the [`MetadataEventLog`] as the durable event log.
//!
//! Lifecycle:
//!
//! - [`TopicBasedRemoteLogMetadataManager::start`]: read the metadata log's high-water marks,
//!   spawn the consumer pump, then block the caller until the pump
//!   has applied every event that was already in the log at start
//!   time (initial catch-up). After `start` returns, reads from this
//!   manager reflect the full pre-existing history.
//! - Mutation calls (`add`/`update`/`put_partition_delete`):
//!   serialize, publish, and wait until the consumer pump has applied
//!   the published offset to the inner cache. The sync return implies
//!   "the event has been recorded and is visible to local reads".
//! - Read calls: pure local lookups against the inner cache.
//! - Drop / [`TopicBasedRemoteLogMetadataManager::shutdown`]: cancel the consumer pump.

use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crabka_remote_storage::{
    InmemoryRemoteLogMetadataManager, RemoteLogMetadataManager, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
    RemoteStorageError, TopicIdPartition,
};

use crate::error::MetadataLogError;
use crate::log::{AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart};
use crate::partitioning::metadata_partition_for;
use crate::serde::MetadataEvent;

/// Production [`RemoteLogMetadataManager`] backed by the
/// `__remote_log_metadata` topic (via a [`MetadataEventLog`]
/// adapter).
///
/// Construct with [`Self::start`]; once it returns, the manager has
/// finished bootstrapping its in-memory cache from the log and is
/// ready to serve queries and accept mutations.
pub struct TopicBasedRemoteLogMetadataManager {
    log: Arc<dyn MetadataEventLog>,
    inner: Arc<InmemoryRemoteLogMetadataManager>,
    applied: Arc<std::sync::Mutex<Vec<i64>>>,
    applied_tx: watch::Sender<u64>,
    runtime: Handle,
    shutdown: CancellationToken,
    pump: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// 48p: directory the on-disk RLMM cache snapshot is written to (one
    /// [`SNAPSHOT_FILE_NAME`](crate::snapshot::SNAPSHOT_FILE_NAME) file).
    snapshot_dir: std::path::PathBuf,
    /// 48p: handle of the background snapshotter task; aborted on `Drop`,
    /// joined on [`Self::shutdown_and_flush`].
    snapshotter: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Live assignment handle for the metadata-log subscription. Held so
    /// 48p (resume from snapshot offsets) and 48q (per-broker partition
    /// assignment) can mutate the consumed set at runtime. Unused in
    /// 48o beyond construction (assign-all-from-0).
    #[allow(dead_code)]
    assignment: Arc<dyn AssignmentHandle>,
}

impl TopicBasedRemoteLogMetadataManager {
    /// Spawn the consumer pump and block until the manager has
    /// applied every event already in `log` at the moment of the
    /// call.
    ///
    /// `runtime` must be a Tokio runtime handle that lives at least
    /// as long as the returned manager. The synchronous
    /// [`RemoteLogMetadataManager`] methods bridge to this handle via
    /// `block_on`, so they must NOT be called from a task running on
    /// this same runtime — the broker only invokes them through
    /// `spawn_blocking`, which is the only supported call pattern.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::Io`] if the log fails to surface
    /// its current high-water marks.
    pub async fn start(
        log: Arc<dyn MetadataEventLog>,
        runtime: Handle,
        snapshot_dir: std::path::PathBuf,
        snapshot_interval: std::time::Duration,
    ) -> Result<Arc<Self>, RemoteStorageError> {
        let target_hwms = log
            .high_water_marks()
            .await
            .map_err(MetadataLogError::into_storage)?;
        let n = usize::try_from(log.partition_count()).expect("partition_count fits in usize");
        let (applied_tx, _) = watch::channel(0u64);
        let inner = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();

        // Load the snapshot (if any) ONCE: seed the cache from its dump and
        // resume from its committed offsets. On absence/corruption,
        // committed[] is all -1 (full replay) and the cache stays empty —
        // never fatal.
        let mut committed = vec![-1i64; n];
        match crate::snapshot::Snapshot::load(
            &snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME),
        ) {
            Ok(Some(snap)) => {
                for (i, &off) in snap.committed_offsets.iter().take(n).enumerate() {
                    committed[i] = off;
                }
                inner.import(snap.dump);
            }
            Ok(None) => {}
            Err(e) => {
                warn!(error = ?e, "topic-based RLMM: snapshot corrupt; starting from empty cache");
            }
        }

        // Pre-seed `applied` to the committed offsets so wait_for_targets
        // only blocks on the delta from committed+1 to HWM.
        let applied = Arc::new(std::sync::Mutex::new(committed.clone()));

        // 48o assignment: resume each partition at committed + 1.
        let assignment: Vec<PartitionStart> = (0..n)
            .map(|i| PartitionStart {
                partition: i32::try_from(i).expect("partition fits in i32"),
                start_offset: committed[i] + 1,
            })
            .collect();
        let (stream, assignment_handle) = log.subscribe(assignment);
        let pump = runtime.spawn(pump_loop(
            stream,
            inner.clone(),
            applied.clone(),
            applied_tx.clone(),
            shutdown.clone(),
        ));

        let manager = Arc::new(Self {
            log,
            inner,
            applied,
            applied_tx,
            runtime,
            shutdown,
            pump: std::sync::Mutex::new(Some(pump)),
            snapshot_dir,
            snapshotter: std::sync::Mutex::new(None),
            assignment: assignment_handle,
        });

        // Spawn the periodic snapshotter: flush whenever the cache
        // advanced since the last write, plus a final flush on shutdown.
        let snapshotter = {
            let weak = Arc::downgrade(&manager);
            let shutdown = manager.shutdown.clone();
            manager.runtime.spawn(async move {
                let mut last_written: i64 = -1;
                loop {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(snapshot_interval) => {}
                    }
                    let Some(m) = weak.upgrade() else { return };
                    // Only write when the cache advanced since the last snapshot.
                    let highest = {
                        let applied = m.applied.lock().expect("applied mutex poisoned");
                        applied.iter().copied().max().unwrap_or(-1)
                    };
                    if highest > last_written {
                        match m.write_snapshot() {
                            Ok(written) => last_written = written,
                            Err(e) => {
                                warn!(error = ?e, "topic-based RLMM: periodic snapshot failed");
                            }
                        }
                    }
                }
            })
        };
        *manager
            .snapshotter
            .lock()
            .expect("snapshotter mutex poisoned") = Some(snapshotter);

        manager.wait_for_targets(&target_hwms).await;
        Ok(manager)
    }

    /// Cancel the consumer pump. Read methods continue to work
    /// against whatever was applied before shutdown; mutation methods
    /// will time out / fail to make progress.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Cancel the pump + snapshotter, then write a final snapshot
    /// capturing everything applied so far. Safe to call once on
    /// graceful shutdown.
    pub async fn shutdown_and_flush(&self) {
        self.shutdown.cancel();
        // Take the handle out of the lock BEFORE awaiting it, so the
        // (sync) mutex is not held across the await point.
        let handle = self
            .snapshotter
            .lock()
            .expect("snapshotter mutex poisoned")
            .take();
        // Let the snapshotter observe cancellation and stop touching
        // `applied` before we take the final consistent capture.
        if let Some(h) = handle {
            let _ = h.await;
        }
        if let Err(e) = self.write_snapshot() {
            warn!(error = ?e, "topic-based RLMM: final snapshot flush failed");
        }
    }

    /// Capture the pump's committed offsets together with a cache
    /// export under a consistent lock, and write a snapshot. The
    /// `applied` lock is held only long enough to clone the offsets and
    /// run `export()` (which takes the inner partitions lock); no Kafka
    /// round-trips happen inside, so the hold is bounded. Returns the
    /// highest committed offset written (for the "advanced since last"
    /// check).
    fn write_snapshot(&self) -> Result<i64, crate::error::SnapshotError> {
        let (committed_offsets, dump) = {
            let applied = self.applied.lock().expect("applied mutex poisoned");
            let dump = self.inner.export();
            (applied.clone(), dump)
        };
        let max = committed_offsets.iter().copied().max().unwrap_or(-1);
        let snap = crate::snapshot::Snapshot {
            committed_offsets,
            dump,
        };
        let path = self.snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
        snap.write_atomic(&path)?;
        Ok(max)
    }

    /// Build the metadata-consumer assignment from a snapshot on disk:
    /// each metadata partition resumes at `committed + 1`. Absent or
    /// corrupt snapshot → every partition starts at 0 (full replay).
    #[must_use]
    pub fn resume_assignment(
        snapshot_dir: &std::path::Path,
        partition_count: i32,
    ) -> Vec<PartitionStart> {
        let n = usize::try_from(partition_count).expect("partition_count fits in usize");
        let committed = Self::load_committed(snapshot_dir, n);
        (0..n)
            .map(|i| PartitionStart {
                partition: i32::try_from(i).expect("partition fits in i32"),
                start_offset: committed[i] + 1,
            })
            .collect()
    }

    /// Load the per-partition committed offsets from a snapshot, padded /
    /// truncated to `n` partitions. Absent or corrupt → all `-1`.
    fn load_committed(snapshot_dir: &std::path::Path, n: usize) -> Vec<i64> {
        let path = snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
        match crate::snapshot::Snapshot::load(&path) {
            Ok(Some(snap)) => {
                let mut out = vec![-1i64; n];
                for (i, &off) in snap.committed_offsets.iter().take(n).enumerate() {
                    out[i] = off;
                }
                out
            }
            Ok(None) => vec![-1i64; n],
            Err(e) => {
                warn!(error = ?e, "topic-based RLMM: snapshot corrupt; full replay");
                vec![-1i64; n]
            }
        }
    }

    async fn wait_for_targets(&self, targets: &[i64]) {
        // Each `targets[p]` is one past the highest offset that existed
        // at start time; need `applied[p] >= targets[p] - 1` to declare
        // catch-up complete. Empty partitions (`targets[p] == 0`) need
        // no wait.
        let mut rx = self.applied_tx.subscribe();
        loop {
            {
                let applied = self.applied.lock().expect("applied mutex poisoned");
                if (0..targets.len()).all(|i| targets[i] == 0 || applied[i] >= targets[i] - 1) {
                    return;
                }
            }
            if rx.changed().await.is_err() {
                return; // Sender dropped; pump is gone, give up.
            }
        }
    }

    async fn wait_for_offset(&self, partition: i32, offset: i64) {
        let idx = usize::try_from(partition).expect("partition non-negative");
        let mut rx = self.applied_tx.subscribe();
        loop {
            {
                let applied = self.applied.lock().expect("applied mutex poisoned");
                if applied[idx] >= offset {
                    return;
                }
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }

    fn publish_and_wait(
        &self,
        tp: &TopicIdPartition,
        event: Bytes,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata_partition_for(tp, self.log.partition_count());
        let log = self.log.clone();
        // Caller is on a non-runtime (spawn_blocking) thread; block_on
        // is safe and gives us the assigned offset to wait on.
        self.runtime.block_on(async {
            let offset = log
                .publish(partition, event)
                .await
                .map_err(MetadataLogError::into_storage)?;
            self.wait_for_offset(partition, offset).await;
            Ok::<_, RemoteStorageError>(())
        })
    }
}

impl Drop for TopicBasedRemoteLogMetadataManager {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.pump.lock().expect("pump mutex poisoned").take() {
            handle.abort();
        }
        if let Some(handle) = self
            .snapshotter
            .lock()
            .expect("snapshotter mutex poisoned")
            .take()
        {
            handle.abort();
        }
    }
}

impl RemoteLogMetadataManager for TopicBasedRemoteLogMetadataManager {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        // Mirror the in-memory manager's eager precondition: fail
        // fast before paying a round trip through Kafka.
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd {
                id: metadata.remote_log_segment_id().clone(),
                reason: format!(
                    "starting state must be CopySegmentStarted, got {:?}",
                    metadata.state()
                ),
            });
        }
        let tp = metadata.remote_log_segment_id().topic_id_partition.clone();
        let event = MetadataEvent::AddSegment(metadata).encode();
        self.publish_and_wait(&tp, event)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let tp = update.remote_log_segment_id.topic_id_partition.clone();
        let event = MetadataEvent::UpdateSegment(update).encode();
        self.publish_and_wait(&tp, event)
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.inner
            .remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Option<i64>, RemoteStorageError> {
        self.inner
            .highest_offset_for_epoch(topic_id_partition, leader_epoch)
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.inner.list_remote_log_segments(topic_id_partition)
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        self.inner
            .list_remote_log_segments_by_epoch(topic_id_partition, leader_epoch)
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let tp = metadata.topic_id_partition.clone();
        let event = MetadataEvent::PartitionDelete(metadata).encode();
        self.publish_and_wait(&tp, event)
    }
}

async fn pump_loop(
    mut stream: MetadataEventStream,
    inner: Arc<InmemoryRemoteLogMetadataManager>,
    applied: Arc<std::sync::Mutex<Vec<i64>>>,
    applied_tx: watch::Sender<u64>,
    shutdown: CancellationToken,
) {
    let mut version: u64 = 0;
    loop {
        let next = tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            n = stream.next() => n,
        };
        let Some(record) = next else { return };
        match MetadataEvent::decode(&record.payload) {
            Ok(MetadataEvent::AddSegment(md)) => {
                if let Err(e) = inner.add_remote_log_segment_metadata(md) {
                    warn!(error = ?e, partition = record.partition, offset = record.offset,
                          "topic-based RLMM: add replay rejected");
                }
            }
            Ok(MetadataEvent::UpdateSegment(u)) => {
                if let Err(e) = inner.update_remote_log_segment_metadata(u) {
                    warn!(error = ?e, partition = record.partition, offset = record.offset,
                          "topic-based RLMM: update replay rejected");
                }
            }
            Ok(MetadataEvent::PartitionDelete(d)) => {
                if let Err(e) = inner.put_remote_partition_delete_metadata(d) {
                    warn!(error = ?e, partition = record.partition, offset = record.offset,
                          "topic-based RLMM: partition-delete replay rejected");
                }
            }
            Err(e) => {
                warn!(error = ?e, partition = record.partition, offset = record.offset,
                      "topic-based RLMM: failed to decode event");
            }
        }
        if let Ok(idx) = usize::try_from(record.partition) {
            let mut a = applied.lock().expect("applied mutex poisoned");
            if idx < a.len() && record.offset > a[idx] {
                a[idx] = record.offset;
            }
        }
        version = version.wrapping_add(1);
        let _ = applied_tx.send(version);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    use crabka_remote_storage::{CustomMetadata, RemoteLogSegmentId, RemotePartitionDeleteState};

    use crate::log::InProcessMetadataEventLog;

    static SNAP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn snapshot_test_dir(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "crabka-rlmm-{label}-{}-{}",
            std::process::id(),
            SNAP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
    }

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn started(id: u128, start: i64, end: i64) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            end + 1,
            1,
            100,
            2048,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0, start)]),
        )
        .unwrap()
    }

    fn finish(id: u128) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: 200,
            custom_metadata: Some(CustomMetadata(vec![7])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        }
    }

    /// Run the sync RLMM trait method on the blocking pool, exactly
    /// like the broker does.
    async fn on_blocking<T, F>(f: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(f).await.unwrap()
    }

    async fn start_manager(
        log: Arc<dyn MetadataEventLog>,
    ) -> Arc<TopicBasedRemoteLogMetadataManager> {
        TopicBasedRemoteLogMetadataManager::start(
            log,
            Handle::current(),
            snapshot_test_dir("test"),
            std::time::Duration::from_hours(1),
        )
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_finish_query_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = start_manager(log).await;
        let m2 = m.clone();
        on_blocking(move || {
            m2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let m2 = m.clone();
        on_blocking(move || m2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;

        let got = m
            .remote_log_segment_metadata(&tp(), 0, 42)
            .unwrap()
            .expect("segment found");
        assert_eq!(got.remote_log_segment_id().id, Uuid::from_u128(10));
        assert_eq!(got.custom_metadata(), Some(&CustomMetadata(vec![7])));
        assert_eq!(m.highest_offset_for_epoch(&tp(), 0).unwrap(), Some(99));
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_with_wrong_state_is_rejected_eagerly() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager(log.clone()).await;
        // Force a non-Started state via the lifecycle helper.
        let bad = started(10, 0, 9).with_update(&finish(10)).unwrap();
        let m2 = m.clone();
        let err = on_blocking(move || m2.add_remote_log_segment_metadata(bad).unwrap_err()).await;
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
        // Eager rejection means nothing was published.
        assert_eq!(log.high_water_marks().await.unwrap(), vec![0; 2]);
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_managers_sharing_a_log_converge() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let a = start_manager(log.clone()).await;
        let b = start_manager(log.clone()).await;

        let a2 = a.clone();
        on_blocking(move || {
            a2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let a2 = a.clone();
        on_blocking(move || a2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;

        // `b` must observe `a`'s writes once its pump has applied
        // them. Poll up to 2s for the in-process broadcast to fan out.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while b.highest_offset_for_epoch(&tp(), 0).unwrap() != Some(99) {
            assert!(
                std::time::Instant::now() < deadline,
                "manager B did not converge within 2s"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert_eq!(b.highest_offset_for_epoch(&tp(), 0).unwrap(), Some(99));
        let got = b
            .remote_log_segment_metadata(&tp(), 0, 50)
            .unwrap()
            .unwrap();
        assert_eq!(got.remote_log_segment_id().id, Uuid::from_u128(10));

        a.shutdown();
        b.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_rehydrates_from_log() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        {
            let m = start_manager(log.clone()).await;
            for (id, start, end) in [(10u128, 0, 99), (11, 100, 199), (12, 200, 299)] {
                let m2 = m.clone();
                on_blocking(move || {
                    m2.add_remote_log_segment_metadata(started(id, start, end))
                        .unwrap();
                })
                .await;
                let m2 = m.clone();
                on_blocking(move || m2.update_remote_log_segment_metadata(finish(id)).unwrap())
                    .await;
            }
            m.shutdown();
        }

        // Fresh manager against the same log: start() blocks until it
        // has applied the full history.
        let fresh = start_manager(log).await;
        let listed = fresh.list_remote_log_segments(&tp()).unwrap();
        assert_eq!(listed.len(), 3);
        assert_eq!(listed[0].start_offset(), 0);
        assert_eq!(listed[2].end_offset(), 299);
        assert_eq!(fresh.highest_offset_for_epoch(&tp(), 0).unwrap(), Some(299));
        fresh.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partition_delete_lifecycle_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager(log).await;
        for state in [
            RemotePartitionDeleteState::DeletePartitionMarked,
            RemotePartitionDeleteState::DeletePartitionStarted,
            RemotePartitionDeleteState::DeletePartitionFinished,
        ] {
            let m2 = m.clone();
            on_blocking(move || {
                m2.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                    topic_id_partition: tp(),
                    state,
                    event_timestamp_ms: 500,
                    broker_id: 1,
                })
                .unwrap();
            })
            .await;
        }
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_flushes_a_snapshot_covering_applied_events() {
        let dir = snapshot_test_dir("mgr-snap");
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            Handle::current(),
            dir.clone(),
            std::time::Duration::from_hours(1), // long interval: only shutdown flushes
        )
        .await
        .unwrap();
        let m2 = m.clone();
        on_blocking(move || {
            m2.add_remote_log_segment_metadata(started(10, 0, 99))
                .unwrap();
        })
        .await;
        let m2 = m.clone();
        on_blocking(move || m2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;

        m.shutdown_and_flush().await;

        let path = dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
        let snap = crate::snapshot::Snapshot::load(&path)
            .unwrap()
            .expect("snapshot written");
        // The orders partition's committed offset covers both events.
        let p = crate::partitioning::metadata_partition_for(&tp(), 4);
        let idx = usize::try_from(p).unwrap();
        assert!(
            snap.committed_offsets[idx] >= 1,
            "committed >= last applied offset"
        );
        // The dump contains the finished segment.
        assert_eq!(snap.dump.partitions.len(), 1);
        assert_eq!(snap.dump.partitions[0].segments.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_resumes_from_snapshot_without_replaying_from_zero() {
        let dir = snapshot_test_dir("resume");
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let interval = std::time::Duration::from_hours(1);

        // First lifetime: seed three finished segments, then shutdown-flush.
        let pre_cache;
        {
            let m = TopicBasedRemoteLogMetadataManager::start(
                log.clone(),
                Handle::current(),
                dir.clone(),
                interval,
            )
            .await
            .unwrap();
            for (id, start, end) in [(10u128, 0, 99), (11, 100, 199), (12, 200, 299)] {
                let m2 = m.clone();
                on_blocking(move || {
                    m2.add_remote_log_segment_metadata(started(id, start, end))
                        .unwrap();
                })
                .await;
                let m2 = m.clone();
                on_blocking(move || m2.update_remote_log_segment_metadata(finish(id)).unwrap())
                    .await;
            }
            pre_cache = m.list_remote_log_segments(&tp()).unwrap();
            m.shutdown_and_flush().await;
        }

        // Snapshot now records committed offset N for the orders partition.
        let p = crate::partitioning::metadata_partition_for(&tp(), 4);
        let idx = usize::try_from(p).unwrap();
        let snap = crate::snapshot::Snapshot::load(&dir.join(crate::snapshot::SNAPSHOT_FILE_NAME))
            .unwrap()
            .expect("snapshot present");
        let committed = snap.committed_offsets[idx];
        assert!(
            committed >= 5,
            "6 events (3 add + 3 finish) → committed >= 5"
        );

        // The loader's assignment resumes the orders partition at committed + 1.
        let assignment = TopicBasedRemoteLogMetadataManager::resume_assignment(&dir, 4);
        let orders_start = assignment
            .iter()
            .find(|s| s.partition == p)
            .map(|s| s.start_offset)
            .unwrap();
        assert_eq!(orders_start, committed + 1, "resume from N+1, not 0");

        // Second lifetime against the SAME log + dir: must resume, not replay.
        let fresh = TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            Handle::current(),
            dir.clone(),
            interval,
        )
        .await
        .unwrap();
        let post_cache = fresh.list_remote_log_segments(&tp()).unwrap();
        assert_eq!(
            post_cache, pre_cache,
            "post-load cache equals pre-restart cache"
        );
        assert_eq!(fresh.highest_offset_for_epoch(&tp(), 0).unwrap(), Some(299));
        fresh.shutdown();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_partition_query_is_none() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager(log).await;
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        assert_eq!(m.remote_log_segment_metadata(&other, 0, 0).unwrap(), None);
        assert_eq!(m.highest_offset_for_epoch(&other, 0).unwrap(), None);
        assert!(m.list_remote_log_segments(&other).unwrap().is_empty());
        m.shutdown();
    }
}
