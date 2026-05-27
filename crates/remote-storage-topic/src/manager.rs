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
//! - [`Self::start`]: read the metadata log's high-water marks,
//!   spawn the consumer pump, then block the caller until the pump
//!   has applied every event that was already in the log at start
//!   time (initial catch-up). After `start` returns, reads from this
//!   manager reflect the full pre-existing history.
//! - Mutation calls (`add`/`update`/`put_partition_delete`):
//!   serialize, publish, and wait until the consumer pump has applied
//!   the published offset to the inner cache. The sync return implies
//!   "the event has been recorded and is visible to local reads".
//! - Read calls: pure local lookups against the inner cache.
//! - Drop / [`Self::shutdown`]: cancel the consumer pump.

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
use crate::log::{MetadataEventLog, MetadataEventStream};
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
    ) -> Result<Arc<Self>, RemoteStorageError> {
        let target_hwms = log
            .high_water_marks()
            .await
            .map_err(MetadataLogError::into_storage)?;
        let n = usize::try_from(log.partition_count()).expect("partition_count fits in usize");
        let (applied_tx, _) = watch::channel(0u64);
        let applied = Arc::new(std::sync::Mutex::new(vec![-1i64; n]));
        let inner = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();

        let stream = log.subscribe();
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
        });

        manager.wait_for_targets(&target_hwms).await;
        Ok(manager)
    }

    /// Cancel the consumer pump. Read methods continue to work
    /// against whatever was applied before shutdown; mutation methods
    /// will time out / fail to make progress.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
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
        TopicBasedRemoteLogMetadataManager::start(log, Handle::current())
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
