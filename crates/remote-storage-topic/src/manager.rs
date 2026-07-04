//! [`TopicBasedRemoteLogMetadataManager`] — production
//! [`RemoteLogMetadataManager`] implementation backed by a publish /
//! subscribe [`MetadataEventLog`].
//!
//! The manager keeps the canonical in-memory view in an
//! [`InmemoryRemoteLogMetadataManager`] (so the lifecycle state
//! machine is the single source of truth for cache mutation) and uses
//! the [`MetadataEventLog`] as the durable event log.
//!
//! Lifecycle:
//!
//! - [`TopicBasedRemoteLogMetadataManager::start`]: load any on-disk
//!   snapshot into the cache and spawn the consumer pump subscribed to
//!   NOTHING. The broker then drives the consumed set via
//!   [`TopicBasedRemoteLogMetadataManager::reconcile_assignment`], adding
//!   only the `__remote_log_metadata` partitions covering the
//!   user-partitions this broker leads or follows. A newly-added partition
//!   is gated by `NotReady` until the pump reaches the HWM observed at
//!   assignment time; a partition this broker does not consume is a genuine
//!   `Ok(None)` (never served from any stale cache).
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
use tracing::{instrument, warn};

use crabka_remote_storage::{
    InmemoryRemoteLogMetadataManager, RemoteLogMetadataManager, RemoteLogSegmentMetadata,
    RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState, RemotePartitionDeleteMetadata,
    RemoteStorageError, TopicIdPartition,
};

use crate::error::MetadataLogError;
use crate::log::{AssignmentHandle, MetadataEventLog, MetadataEventStream, PartitionStart};
use crate::partitioning::metadata_partition_for;
use crate::serde::MetadataEvent;

/// Sentinel target HWM meaning "this partition is assigned but its real
/// high-water mark is not yet known" (the `high_water_marks` RPC failed,
/// or the partition had no entry in the returned index). The gate treats
/// it as `NotReady` (a real applied offset can never reach `i64::MAX`),
/// and the next `reconcile_assignment` re-attempts the HWM fetch to
/// replace it with the real target.
const HWM_UNKNOWN: i64 = i64::MAX;

/// Outcome of the per-metadata-partition readiness check that gates the
/// gated [`RemoteLogMetadataManager`] read methods
/// ([`RemoteLogMetadataManager::remote_log_segment_metadata`],
/// [`RemoteLogMetadataManager::list_remote_log_segments`],
/// [`RemoteLogMetadataManager::highest_offset_for_epoch`]).
enum ReadGate {
    /// This broker does not consume the metadata partition (neither leads
    /// nor follows any covered user-partition) → answer `Ok(None)`.
    Unassigned,
    /// Assigned but the consumer pump has not reached the assignment-time
    /// HWM → answer `Err(NotReady)` (retryable).
    NotReady,
    /// Assigned and caught up → delegate to the inner cache.
    Ready,
}

/// Production [`RemoteLogMetadataManager`] backed by the
/// `__remote_log_metadata` topic (via a [`MetadataEventLog`]
/// adapter).
///
/// Construct with [`Self::start`]; it loads any on-disk snapshot but
/// consumes no metadata partitions until [`Self::reconcile_assignment`]
/// adds the broker's leader/follower-derived set.
pub struct TopicBasedRemoteLogMetadataManager {
    log: Arc<dyn MetadataEventLog>,
    inner: Arc<InmemoryRemoteLogMetadataManager>,
    applied: Arc<std::sync::Mutex<Vec<i64>>>,
    applied_tx: watch::Sender<u64>,
    runtime: Handle,
    shutdown: CancellationToken,
    pump: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Directory the on-disk RLMM cache snapshot is written to (one
    /// [`SNAPSHOT_FILE_NAME`](crate::snapshot::SNAPSHOT_FILE_NAME) file).
    snapshot_dir: std::path::PathBuf,
    /// Handle of the background snapshotter task; aborted on `Drop`,
    /// joined on [`Self::shutdown_and_flush`].
    snapshotter: std::sync::Mutex<Option<JoinHandle<()>>>,
    /// Live assignment handle for the metadata-log subscription. Held so
    /// resume-from-snapshot and per-broker partition-assignment logic
    /// assignment) can mutate the consumed set at runtime. Driven by
    /// [`Self::reconcile_assignment`].
    assignment: Arc<dyn AssignmentHandle>,
    /// Per-metadata-partition committed offsets loaded from the snapshot
    /// at `start()`, indexed by metadata partition (`-1` == no committed
    /// event for that partition / full replay). Retained as the single
    /// canonical source for resume-offset lookups; assignment
    /// reconciler reads it via [`Self::committed_offset`] when it
    /// dynamically adds a partition (to start at `committed + 1`).
    committed_offsets: Vec<i64>,
    /// Metadata partition → target HWM observed at assignment time.
    /// Presence == this manager is currently assigned that partition;
    /// reads for a user-partition hashing into it return
    /// [`RemoteStorageError::NotReady`] until `applied[mp] >= target - 1`.
    /// Empty for managers that never call [`Self::reconcile_assignment`]
    /// (every read then delegates straight to `inner`).
    ready_targets: Arc<std::sync::Mutex<std::collections::HashMap<i32, i64>>>,
}

impl TopicBasedRemoteLogMetadataManager {
    /// Load any on-disk snapshot into the cache and spawn the consumer
    /// pump with an empty assignment. The manager consumes nothing until
    /// [`Self::reconcile_assignment`] is driven (by the broker).
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
    /// Currently infallible (the consumed set starts empty), but returns a
    /// `Result` so the bootstrap contract stays stable if `start` regains a
    /// fallible step.
    // Kept `async` for the established 4-arg bootstrap contract the broker
    // awaits; bootstrap no longer blocks on catch-up (it consumes nothing
    // until `reconcile_assignment`), so there is no internal `.await`.
    #[allow(clippy::unused_async)]
    pub async fn start(
        log: Arc<dyn MetadataEventLog>,
        runtime: Handle,
        snapshot_dir: std::path::PathBuf,
        snapshot_interval: std::time::Duration,
    ) -> Result<Arc<Self>, RemoteStorageError> {
        let n = usize::try_from(log.partition_count()).expect("partition_count fits in usize");
        let (applied_tx, _) = watch::channel(0u64);
        let inner = Arc::new(InmemoryRemoteLogMetadataManager::new());
        let shutdown = CancellationToken::new();

        // Load the snapshot (if any) ONCE and seed the cache from its
        // dump. `resume_from_snapshot` is the single canonical place that
        // turns a loaded snapshot into the per-partition committed offsets.
        // On absence/corruption, committed[] is all -1 (full replay) and the
        // cache stays empty — never fatal.
        let snapshot = match crate::snapshot::Snapshot::load(
            &snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME),
        ) {
            Ok(snap) => snap,
            Err(e) => {
                warn!(error = ?e, "topic-based RLMM: snapshot corrupt; starting from empty cache");
                None
            }
        };
        if let Some(snap) = &snapshot {
            inner.import(snap.dump.clone());
        }
        // A freshly-started manager consumes NOTHING. The broker drives
        // the consumed set via [`Self::reconcile_assignment`], adding only the
        // metadata partitions covering user-partitions this broker leads or
        // follows (each resumed at its snapshot `committed + 1`). This is what
        // makes an unassigned partition a genuine `Ok(None)` rather than a
        // false hit from globally-replayed state.
        let (committed, _assignment) = Self::resume_from_snapshot(snapshot.as_ref(), n);

        // Pre-seed `applied` to the committed offsets so readiness checks for
        // a later-added partition only block on the delta from committed+1 to
        // the assignment-time HWM.
        let applied = Arc::new(std::sync::Mutex::new(committed.clone()));

        let (stream, assignment_handle) = log.subscribe(Vec::new());
        let pump = runtime.spawn(pump_loop(
            stream,
            Arc::clone(&inner),
            Arc::clone(&applied),
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
            committed_offsets: committed,
            ready_targets: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
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

        // Nothing is consumed at bootstrap (empty assignment), so the manager
        // is immediately ready. Per-partition catch-up after a later
        // `reconcile_assignment` is governed by `metadata_partition_ready`,
        // which gates reads with `NotReady` until the pump reaches the
        // assignment-time HWM.
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
    #[instrument(skip_all, err)]
    fn write_snapshot(&self) -> Result<i64, crate::error::SnapshotError> {
        // Benign-replay invariant: the pump updates `inner` BEFORE bumping
        // `applied`, so the captured cache may lead the captured committed
        // offset by at most one event (the in-flight one). On resume that
        // single event is replayed from committed+1 and harmlessly
        // re-rejected: a re-applied AddSegment hits already-exists, and a
        // re-applied finished→finished update is a no-op. The dangerous
        // direction — cache BEHIND committed, which would skip an event on
        // resume — cannot occur because inner is always updated first.
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

    /// Canonical resume-from-snapshot computation, shared by `start()` and
    /// the resume tests. Given an already-loaded snapshot (or `None` for a
    /// missing/corrupt one) and the metadata-partition count `n`, produce:
    ///
    /// - the per-partition committed offsets, indexed by metadata
    ///   partition and padded/truncated to `n` (`-1` == no committed
    ///   event → full replay for that partition), and
    /// - the metadata-consumer assignment that resumes each partition at
    ///   `committed + 1`.
    ///
    /// This is the ONLY place the `committed + 1` resume policy lives; do
    /// not recompute it elsewhere.
    fn resume_from_snapshot(
        snapshot: Option<&crate::snapshot::Snapshot>,
        n: usize,
    ) -> (Vec<i64>, Vec<PartitionStart>) {
        let mut committed = vec![-1i64; n];
        if let Some(snap) = snapshot {
            for (i, &off) in snap.committed_offsets.iter().take(n).enumerate() {
                committed[i] = off;
            }
        }
        let assignment = (0..n)
            .map(|i| PartitionStart {
                partition: i32::try_from(i).expect("partition fits in i32"),
                start_offset: committed[i] + 1,
            })
            .collect();
        (committed, assignment)
    }

    /// Committed offset loaded from the snapshot for a single metadata
    /// partition, or `-1` when the partition is out of range or had no
    /// committed event (full replay). The assignment reconciler uses
    /// this to start a dynamically-added partition at `committed + 1`.
    #[must_use]
    pub fn committed_offset(&self, partition: i32) -> i64 {
        usize::try_from(partition)
            .ok()
            .and_then(|i| self.committed_offsets.get(i).copied())
            .unwrap_or(-1)
    }

    /// The read decision for metadata partition `mp`, used to gate
    /// [`RemoteLogMetadataManager::remote_log_segment_metadata`].
    fn metadata_partition_gate(&self, mp: i32) -> ReadGate {
        let target = {
            let guard = self.ready_targets.lock().expect("ready_targets poisoned");
            match guard.get(&mp) {
                Some(&t) => t,
                // Not assigned: this broker neither leads nor follows any
                // user-partition in `mp`, so it must not answer from any
                // stale cache it happened to consume earlier. A genuine
                // miss — `Ok(None)`, NOT `NotReady`.
                None => return ReadGate::Unassigned,
            }
        };
        if target == 0 {
            return ReadGate::Ready; // empty partition: nothing to catch up to
        }
        // A sentinel target means the real HWM is not yet known (the
        // assignment-time fetch failed); the partition is assigned but the
        // answer is unknown → retryable, never a false `Ok(None)`.
        if target == HWM_UNKNOWN {
            return ReadGate::NotReady;
        }
        let Ok(idx) = usize::try_from(mp) else {
            // Defensive: a metadata partition index that doesn't fit in
            // usize is nonsensical, but if it ever happens we must NOT
            // fail open into `Ready` (which would serve a possibly-stale
            // or false-miss answer). Treat it as still catching up.
            return ReadGate::NotReady;
        };
        let applied = self.applied.lock().expect("applied mutex poisoned");
        if idx < applied.len() && applied[idx] >= target - 1 {
            ReadGate::Ready
        } else {
            ReadGate::NotReady
        }
    }

    /// `true` when metadata partition `mp` is assigned and caught up to its
    /// assignment-time HWM. Used by tests to poll for catch-up.
    #[cfg(test)]
    fn metadata_partition_ready(&self, mp: i32) -> bool {
        matches!(self.metadata_partition_gate(mp), ReadGate::Ready)
    }

    /// The metadata partitions this manager is currently assigned (tracked
    /// for readiness). Sorted ascending.
    #[must_use]
    pub fn assigned_metadata_partitions(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }

    /// Diff `desired` against the current assignment and drive the
    /// [`AssignmentHandle`]: add newly-needed partitions (seeded from the
    /// snapshot committed offset + 1, falling back to 0 when there is
    /// no committed event) and remove ones no longer needed. Records each
    /// added partition's assignment-time HWM so reads gate on `NotReady`
    /// until the pump catches up.
    ///
    /// HWM-fetch failure fails CLOSED: a partition whose real high-water
    /// mark could not be obtained is recorded with the `HWM_UNKNOWN`
    /// sentinel target so the gate returns `NotReady` (retryable), never a
    /// false `Ok(None)`. Such partitions are re-attempted on every
    /// subsequent reconcile (which the broker drives on each image change /
    /// reconciler tick), so a transient `high_water_marks` failure
    /// self-heals: the sentinel is replaced with the real target as soon as
    /// the fetch succeeds.
    ///
    /// MUST be driven by a SINGLE task. This method is not internally
    /// serialized — it interleaves `.await` points with reads/writes of the
    /// `ready_targets` map under short, non-overlapping locks — so two
    /// concurrent callers could race the add/remove/refresh logic.
    /// Correctness relies on the broker invoking it from exactly one
    /// reconciler task.
    ///
    /// Async because it reads the log's high-water marks; the broker calls
    /// it from its reconciler task (on the runtime), never from a
    /// `spawn_blocking` thread.
    pub async fn reconcile_assignment(&self, desired: &[i32]) {
        use std::collections::HashSet;
        let want: HashSet<i32> = desired.iter().copied().collect();
        // Snapshot the current per-partition targets so we can both diff the
        // assigned set and find partitions still carrying the HWM-unknown
        // sentinel (which need a refresh). Lock released before the `.await`.
        let current: std::collections::HashMap<i32, i64> = self
            .ready_targets
            .lock()
            .expect("ready_targets poisoned")
            .clone();
        let have: HashSet<i32> = current.keys().copied().collect();

        let needs_add = want.difference(&have).copied().collect::<Vec<_>>();
        // Partitions still assigned (in want) whose recorded target is the
        // sentinel: their HWM is still unknown, so re-attempt the fetch.
        let needs_refresh = want
            .iter()
            .copied()
            .filter(|mp| current.get(mp) == Some(&HWM_UNKNOWN))
            .collect::<Vec<_>>();

        // One HWM snapshot covers both additions and sentinel refreshes.
        let needs_hwm = !needs_add.is_empty() || !needs_refresh.is_empty();
        let hwms = if needs_hwm {
            match self.log.high_water_marks().await {
                Ok(h) => Some(h),
                Err(e) => {
                    warn!(error = ?e, "topic-based RLMM: high_water_marks fetch failed; \
                          assigned partitions gate NotReady until a later reconcile refreshes");
                    None
                }
            }
        } else {
            None
        };

        // Resolve a partition's target HWM from the (maybe-missing) snapshot.
        // A failed fetch (`None`) or a missing per-partition entry both yield
        // the sentinel so the gate stays NotReady — never fail open to 0.
        let target_for = |mp: i32| -> i64 {
            match &hwms {
                Some(h) => usize::try_from(mp)
                    .ok()
                    .and_then(|i| h.get(i).copied())
                    .unwrap_or(HWM_UNKNOWN),
                None => HWM_UNKNOWN,
            }
        };

        for mp in needs_add {
            // `committed_offset` is `-1` when there is no committed event
            // (full replay), so `+ 1` lands on the resume start offset (0).
            let start_offset = self.committed_offset(mp) + 1;
            self.assignment.add(PartitionStart {
                partition: mp,
                start_offset,
            });
            // Assign-but-NotReady when the HWM is unknown: the broker DOES
            // own this partition, so leaving it Unassigned would wrongly
            // return Ok(None). The sentinel makes the gate return NotReady.
            self.ready_targets
                .lock()
                .expect("ready_targets poisoned")
                .insert(mp, target_for(mp));
        }
        // Replace the sentinel for already-assigned partitions whose HWM is
        // now known (the partition stays assigned; only its target changes).
        for mp in needs_refresh {
            let target = target_for(mp);
            if target != HWM_UNKNOWN {
                let mut guard = self.ready_targets.lock().expect("ready_targets poisoned");
                // Only refresh if still assigned with the sentinel (a
                // concurrent remove would have dropped it — see the
                // single-task contract above).
                if guard.get(&mp) == Some(&HWM_UNKNOWN) {
                    guard.insert(mp, target);
                }
            }
        }
        for mp in have.difference(&want).copied() {
            self.assignment.remove(mp);
            self.ready_targets
                .lock()
                .expect("ready_targets poisoned")
                .remove(&mp);
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
        let log = Arc::clone(&self.log);
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
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // Not this broker's partition → genuine miss, do NOT serve any
            // stale cache.
            ReadGate::Unassigned => Ok(None),
            // Assigned but not caught up → retryable, distinct from a miss.
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => {
                self.inner
                    .remote_log_segment_metadata(topic_id_partition, leader_epoch, offset)
            }
        }
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Option<i64>, RemoteStorageError> {
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            ReadGate::Unassigned => Ok(None),
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => self
                .inner
                .highest_offset_for_epoch(topic_id_partition, leader_epoch),
        }
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let mp = metadata_partition_for(topic_id_partition, self.log.partition_count());
        match self.metadata_partition_gate(mp) {
            // Not this broker's partition → it does not own it, so it must
            // not serve any stale segments it happened to consume earlier.
            ReadGate::Unassigned => Ok(Vec::new()),
            ReadGate::NotReady => Err(RemoteStorageError::NotReady { partition: mp }),
            ReadGate::Ready => self.inner.list_remote_log_segments(topic_id_partition),
        }
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
    use assert2::assert;
    use assert2::check;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    use crabka_remote_storage::{CustomMetadata, RemoteLogSegmentId, RemotePartitionDeleteState};

    use crate::error::MetadataLogError;
    use crate::log::{AssignmentHandle, InProcessMetadataEventLog, MetadataEventStream};

    /// Test double that delegates to an inner [`InProcessMetadataEventLog`]
    /// but can be told to fail `high_water_marks()` on demand. The
    /// in-process fixture's HWM RPC always succeeds, which is why the rest
    /// of the suite cannot exercise the C1 fail-closed path.
    struct HwmFlakyLog {
        inner: Arc<InProcessMetadataEventLog>,
        fail_hwm: std::sync::atomic::AtomicBool,
    }

    impl HwmFlakyLog {
        fn new(partition_count: i32) -> Arc<Self> {
            Arc::new(Self {
                inner: InProcessMetadataEventLog::new(partition_count),
                fail_hwm: std::sync::atomic::AtomicBool::new(false),
            })
        }
        fn set_fail_hwm(&self, fail: bool) {
            self.fail_hwm
                .store(fail, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl MetadataEventLog for HwmFlakyLog {
        fn partition_count(&self) -> i32 {
            self.inner.partition_count()
        }
        async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError> {
            self.inner.publish(partition, event).await
        }
        fn subscribe(
            &self,
            assignment: Vec<PartitionStart>,
        ) -> (MetadataEventStream, Arc<dyn AssignmentHandle>) {
            self.inner.subscribe(assignment)
        }
        async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError> {
            if self.fail_hwm.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(MetadataLogError::Other("injected HWM failure".into()));
            }
            self.inner.high_water_marks().await
        }
    }

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

    /// Poll until `tp` reads `Ok(Some)` (assigned + caught up), or panic.
    async fn wait_ready(m: &Arc<TopicBasedRemoteLogMetadataManager>, tp: &TopicIdPartition) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            if matches!(m.remote_log_segment_metadata(tp, 0, 42), Ok(Some(_))) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "partition never became ready"
            );
            // Yield-poll the pump's progress rather than sleeping a fixed
            // cadence; the deadline above stays as the hang-guard.
            tokio::task::yield_now().await;
        }
    }

    /// Start a manager that consumes NOTHING until the caller drives
    /// `reconcile_assignment`. Used by the assignment/readiness tests, which
    /// assert pre-assignment reads are a genuine miss.
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

    /// Start a manager and assign EVERY metadata partition (the eager
    /// "consume all" behavior). Used by tests that publish through the
    /// manager and read the result back, and by the multi-broker pre-seed
    /// writers. Blocks until each non-empty partition has caught up to its
    /// assignment-time HWM so a subsequent read does not race the pump.
    async fn start_manager_all(
        log: Arc<dyn MetadataEventLog>,
    ) -> Arc<TopicBasedRemoteLogMetadataManager> {
        let n = log.partition_count();
        let m = start_manager(log).await;
        let all: Vec<i32> = (0..n).collect();
        m.reconcile_assignment(&all).await;
        // Wait for the pump to catch up to every assigned partition's HWM so
        // the manager is "ready" for all partitions, mirroring the old
        // bootstrap contract.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !all.iter().all(|&mp| m.metadata_partition_ready(mp)) {
            assert!(
                std::time::Instant::now() < deadline,
                "manager did not catch up on all partitions within 5s"
            );
            tokio::task::yield_now().await;
        }
        m
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_finish_query_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let m = start_manager_all(log).await;
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
        check!(got.remote_log_segment_id().id == Uuid::from_u128(10));
        check!(got.custom_metadata() == Some(&CustomMetadata(vec![7])));
        check!(m.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(99));
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
        assert!(log.high_water_marks().await.unwrap() == vec![0; 2]);
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_managers_sharing_a_log_converge() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        let a = start_manager_all(log.clone()).await;
        let b = start_manager_all(log.clone()).await;

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
            tokio::task::yield_now().await;
        }
        assert!(b.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(99));
        let got = b
            .remote_log_segment_metadata(&tp(), 0, 50)
            .unwrap()
            .unwrap();
        assert!(got.remote_log_segment_id().id == Uuid::from_u128(10));

        a.shutdown();
        b.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn restart_rehydrates_from_log() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        {
            let m = start_manager_all(log.clone()).await;
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

        // Fresh manager against the same log: assigning all partitions
        // replays the full history before the read below.
        let fresh = start_manager_all(log).await;
        let listed = fresh.list_remote_log_segments(&tp()).unwrap();
        let ranges: Vec<(i64, i64)> = listed
            .iter()
            .map(|s| (s.start_offset(), s.end_offset()))
            .collect();
        assert!(ranges == [(0, 99), (100, 199), (200, 299)]);
        assert!(fresh.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(299));
        fresh.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn partition_delete_lifecycle_round_trip() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager_all(log).await;
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
        m.reconcile_assignment(&(0..log.partition_count()).collect::<Vec<_>>())
            .await;
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
        check!(
            snap.committed_offsets[idx] >= 1,
            "committed >= last applied offset"
        );
        // The dump contains the finished segment.
        assert!(snap.dump.partitions.len() == 1);
        check!(snap.dump.partitions[0].segments.len() == 1);
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
            m.reconcile_assignment(&(0..log.partition_count()).collect::<Vec<_>>())
                .await;
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

        // The canonical resume computation resumes the orders partition at
        // committed + 1 (same path start() uses).
        let (resumed_committed, assignment) =
            TopicBasedRemoteLogMetadataManager::resume_from_snapshot(Some(&snap), 4);
        let orders_start = assignment
            .iter()
            .find(|s| s.partition == p)
            .map(|s| s.start_offset)
            .unwrap();
        assert!(orders_start == committed + 1, "resume from N+1, not 0");
        assert!(resumed_committed[idx] == committed);

        // Second lifetime against the SAME log + dir: must resume, not replay.
        let fresh = TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            Handle::current(),
            dir.clone(),
            interval,
        )
        .await
        .unwrap();
        // The manager exposes the same committed offset via its canonical
        // accessor used by the assignment reconciler.
        assert!(fresh.committed_offset(p) == committed);
        // Assign every partition and wait for catch-up so the gated read
        // methods delegate to the (snapshot-seeded) inner cache. The orders
        // partition has no backlog past `committed`, so it is ready as soon
        // as the assignment-time HWM is recorded.
        fresh
            .reconcile_assignment(&(0..log.partition_count()).collect::<Vec<_>>())
            .await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !fresh.metadata_partition_ready(p) {
            assert!(
                std::time::Instant::now() < deadline,
                "fresh manager did not catch up on the orders partition"
            );
            tokio::task::yield_now().await;
        }
        let post_cache = fresh.list_remote_log_segments(&tp()).unwrap();
        assert!(
            post_cache == pre_cache,
            "post-load cache equals pre-restart cache"
        );
        assert!(fresh.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(299));
        fresh.shutdown();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn add_then_remove_drives_assignment_and_readiness() {
        use crate::partitioning::metadata_partition_for;

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        // Pre-seed a finished segment for `tp()` so a ready read returns Some.
        {
            let writer = start_manager_all(log.clone()).await;
            let w2 = writer.clone();
            on_blocking(move || {
                w2.add_remote_log_segment_metadata(started(10, 0, 99))
                    .unwrap();
            })
            .await;
            let w2 = writer.clone();
            on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
            writer.shutdown();
        }

        let mp = metadata_partition_for(&tp(), log.partition_count());
        let m = start_manager(log).await;

        // Before assignment: the partition is not consumed → genuine miss.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 42),
            Ok(None)
        ));

        // Assign it. add() must enqueue a PartitionStart for `mp`, and the
        // pump catches up; once applied >= HWM-1 the read returns Some.
        m.reconcile_assignment(&[mp]).await;
        assert!(m.assigned_metadata_partitions() == vec![mp]);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match m.remote_log_segment_metadata(&tp(), 0, 42) {
                Ok(Some(md)) => {
                    assert!(md.remote_log_segment_id().id == Uuid::from_u128(10));
                    break;
                }
                Err(RemoteStorageError::NotReady { partition }) => {
                    assert!(partition == mp, "NotReady names the catching-up partition");
                    assert!(
                        std::time::Instant::now() < deadline,
                        "metadata partition never became ready"
                    );
                    tokio::task::yield_now().await;
                }
                other => panic!("unexpected read outcome: {other:?}"),
            }
        }

        // Remove it: assignment drops, and subsequent reads are a genuine
        // miss (Ok(None)) — the partition is no longer consumed.
        m.reconcile_assignment(&[]).await;
        assert!(m.assigned_metadata_partitions().is_empty());
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 42),
            Ok(None)
        ));
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_partition_query_is_none() {
        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(2);
        let m = start_manager(log).await;
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        check!(m.remote_log_segment_metadata(&other, 0, 0).unwrap() == None);
        check!(m.highest_offset_for_epoch(&other, 0).unwrap() == None);
        check!(m.list_remote_log_segments(&other).unwrap().is_empty());
        m.shutdown();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_brokers_split_metadata_partitions() {
        use crate::partitioning::metadata_partition_for;

        // Use a wide metadata topic so two user-partitions land in distinct
        // buckets.
        let n = 16;
        let topic_id = Uuid::from_u128(0xFEED);
        let tp_a = TopicIdPartition::new(topic_id, "orders", 0);
        let tp_b = TopicIdPartition::new(topic_id, "orders", 1);
        let mp_a = metadata_partition_for(&tp_a, n);
        let mp_b = metadata_partition_for(&tp_b, n);
        assert!(
            mp_a != mp_b,
            "test needs the two partitions in distinct buckets"
        );

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(n);

        // Seed one finished segment for each user-partition via a transient
        // writer (consumes all partitions, no assignment gating).
        for (tp, id) in [(tp_a.clone(), 100u128), (tp_b.clone(), 200)] {
            let w = start_manager_all(log.clone()).await;
            let started = RemoteLogSegmentMetadata::new(
                RemoteLogSegmentId::new(tp.clone(), Uuid::from_u128(id)),
                0,
                99,
                100,
                1,
                100,
                2048,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(0, 0)]),
            )
            .unwrap();
            let w2 = w.clone();
            on_blocking(move || w2.add_remote_log_segment_metadata(started).unwrap()).await;
            let upd = RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: RemoteLogSegmentId::new(tp, Uuid::from_u128(id)),
                event_timestamp_ms: 200,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            };
            let w2 = w.clone();
            on_blocking(move || w2.update_remote_log_segment_metadata(upd).unwrap()).await;
            w.shutdown();
        }

        // Broker A consumes mp_a only; Broker B consumes mp_b only.
        let a = start_manager(log.clone()).await;
        let b = start_manager(log).await;
        a.reconcile_assignment(&[mp_a]).await;
        b.reconcile_assignment(&[mp_b]).await;

        check!(a.assigned_metadata_partitions() == vec![mp_a]);
        check!(b.assigned_metadata_partitions() == vec![mp_b]);
        // Disjoint shares.
        check!(
            a.assigned_metadata_partitions()
                .iter()
                .all(|p| !b.assigned_metadata_partitions().contains(p)),
            "shares must be disjoint"
        );

        // Poll until each is caught up and serves its own partition.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let a_own = a.remote_log_segment_metadata(&tp_a, 0, 42);
            let b_own = b.remote_log_segment_metadata(&tp_b, 0, 42);
            if matches!(a_own, Ok(Some(_))) && matches!(b_own, Ok(Some(_))) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "managers did not catch up: a={a_own:?} b={b_own:?}"
            );
            tokio::task::yield_now().await;
        }

        // Cross reads (partition the broker does NOT consume) are a genuine
        // miss, not NotReady.
        assert!(
            matches!(a.remote_log_segment_metadata(&tp_b, 0, 42), Ok(None)),
            "A does not consume mp_b → genuine miss"
        );
        assert!(
            matches!(b.remote_log_segment_metadata(&tp_a, 0, 42), Ok(None)),
            "B does not consume mp_a → genuine miss"
        );

        a.shutdown();
        b.shutdown();
    }

    /// Runtime `remove` then `add` reassignment must not
    /// double-deliver a metadata partition's events into the cache. A
    /// re-applied `AddSegment` is harmlessly rejected by the lifecycle state
    /// machine, so the segment list stays at exactly one entry — proving no
    /// duplicate corruption after remove + re-add.
    #[tokio::test(flavor = "multi_thread")]
    async fn reassignment_remove_then_readd_applies_no_duplicates() {
        use crate::partitioning::metadata_partition_for;

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
        // Pre-seed a single finished segment for `tp()`.
        {
            let writer = start_manager_all(log.clone()).await;
            let w2 = writer.clone();
            on_blocking(move || {
                w2.add_remote_log_segment_metadata(started(10, 0, 99))
                    .unwrap();
            })
            .await;
            let w2 = writer.clone();
            on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
            writer.shutdown();
        }

        let mp = metadata_partition_for(&tp(), log.partition_count());
        let m = start_manager(log).await;

        // Add → catch up → exactly one segment.
        m.reconcile_assignment(&[mp]).await;
        wait_ready(&m, &tp()).await;
        assert!(
            m.list_remote_log_segments(&tp()).unwrap().len() == 1,
            "one segment after first assignment"
        );

        // Remove (drops the live fetch task mid-flight if one is running) …
        m.reconcile_assignment(&[]).await;
        assert!(m.assigned_metadata_partitions().is_empty());

        // … then re-add. The pump re-injects the backlog from the resume
        // offset; the re-applied AddSegment is rejected by the lifecycle
        // machine, so NO duplicate lands in the cache.
        m.reconcile_assignment(&[mp]).await;
        wait_ready(&m, &tp()).await;

        let listed = m.list_remote_log_segments(&tp()).unwrap();
        assert!(
            listed.len() == 1,
            "remove + re-add must not duplicate the segment, got {listed:?}"
        );
        check!(listed[0].remote_log_segment_id().id == Uuid::from_u128(10));
        // The finished state survived (no half-applied duplicate update).
        check!(m.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(99));

        m.shutdown();
    }

    /// C1: a HWM-fetch failure must fail CLOSED. When `high_water_marks`
    /// errors at assignment time, the newly-added partition must gate
    /// `NotReady` (retryable) — NEVER `Ok(None)` (a false end-of-tier) —
    /// and the sentinel must self-heal on a later reconcile once the HWM
    /// fetch succeeds.
    #[tokio::test(flavor = "multi_thread")]
    async fn hwm_fetch_failure_gates_not_ready_then_self_heals() {
        use crate::partitioning::metadata_partition_for;

        let flaky = HwmFlakyLog::new(4);
        let log: Arc<dyn MetadataEventLog> = flaky.clone();

        // Pre-seed a finished segment for `tp()` via a healthy writer (HWM
        // not failing yet), so a ready read would return Some.
        {
            let writer = start_manager_all(log.clone()).await;
            let w2 = writer.clone();
            on_blocking(move || {
                w2.add_remote_log_segment_metadata(started(10, 0, 99))
                    .unwrap();
            })
            .await;
            let w2 = writer.clone();
            on_blocking(move || w2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;
            writer.shutdown();
        }

        let mp = metadata_partition_for(&tp(), log.partition_count());
        let m = start_manager(log).await;

        // Assign the partition WHILE the HWM RPC is failing. The partition
        // must be added (the broker owns it) but recorded with the sentinel
        // target so the gate returns NotReady, not Ok(None).
        flaky.set_fail_hwm(true);
        m.reconcile_assignment(&[mp]).await;
        assert!(
            m.assigned_metadata_partitions() == vec![mp],
            "partition is assigned even though HWM is unknown (broker owns it)"
        );

        // Give the pump ample time to drain the backlog. Even fully caught
        // up, the read must stay NotReady because the real HWM is unknown —
        // it must NEVER collapse to Ok(None).
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(300);
        while std::time::Instant::now() < deadline {
            match m.remote_log_segment_metadata(&tp(), 0, 42) {
                Err(RemoteStorageError::NotReady { partition }) => assert!(partition == mp),
                other => panic!("HWM-unknown partition must read NotReady, got {other:?}"),
            }
            // The list path is gated the same way.
            match m.list_remote_log_segments(&tp()) {
                Err(RemoteStorageError::NotReady { partition }) => assert!(partition == mp),
                other => panic!("HWM-unknown partition list must be NotReady, got {other:?}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Recover: HWM fetch now succeeds. A subsequent reconcile (which the
        // broker drives on each image change / tick) must replace the
        // sentinel with the real target. Once the pump has caught up the read
        // returns Some.
        flaky.set_fail_hwm(false);
        m.reconcile_assignment(&[mp]).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match m.remote_log_segment_metadata(&tp(), 0, 42) {
                Ok(Some(md)) => {
                    assert!(md.remote_log_segment_id().id == Uuid::from_u128(10));
                    break;
                }
                Err(RemoteStorageError::NotReady { partition }) => {
                    assert!(partition == mp);
                    assert!(
                        std::time::Instant::now() < deadline,
                        "partition never became ready after HWM recovered"
                    );
                    tokio::task::yield_now().await;
                }
                other => panic!("unexpected read outcome after recovery: {other:?}"),
            }
        }
        // The list path is now Ready too.
        assert!(m.list_remote_log_segments(&tp()).unwrap().len() == 1);
        assert!(m.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(99));

        m.shutdown();
    }
}
