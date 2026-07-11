//! Checkpointer configuration, trigger accounting, and control surface.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use crabka_client_admin::DeleteRecordsOp;
use crabka_object_store::{ObjectOps, ObjectStoreClient, ObjectStoreConfig, build_object_store};
use crabka_pgkv::{KvSnapshot, SnapshotKv};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    CheckpointMetadata, CheckpointSnapshot, CheckpointStore, Manifest, ObjectOpsCheckpointStore,
    WalPrunePlan, ckpt_dir, latest_checkpoint_metadata, manifest_key, plan_prune, write_checkpoint,
};
use crate::{error::SubstrateError, writer::CheckpointSnapshotSource};

/// Durable checkpoint-service boundaries available only to the crash-test feature.
#[cfg(feature = "checkpoint-test-hooks")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointServiceStep {
    /// Snapshot has been materialized, before the first part upload.
    BeforeParts,
    /// A checkpoint part is durable, before the manifest.
    PartsUploaded,
    /// The manifest is durable, before `DeleteRecords`.
    ManifestWritten,
    /// Writer ownership was validated immediately before generation-scoped pruning.
    LeaseValidated,
    /// `DeleteRecords` completed, before object pruning.
    Truncated,
    /// Object pruning completed.
    Pruned,
}

/// Compile-time test-only crash callback; returning true aborts at `step`.
#[cfg(feature = "checkpoint-test-hooks")]
pub type CheckpointFailpoint = Arc<dyn Fn(CheckpointServiceStep) -> bool + Send + Sync>;

/// Default number of checkpoint directories to retain.
pub const DEFAULT_CHECKPOINT_RETAIN: usize = 2;

/// Checkpointer thresholds and object layout knobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointConfig {
    /// Tenant whose store is checkpointed.
    pub tenant: String,
    /// WAL topic to pass to `DeleteRecords` after a durable manifest.
    pub topic: String,
    /// Checkpoint after at least this many committed frames. Zero disables it.
    pub frames_threshold: u64,
    /// Checkpoint after at least this many committed payload bytes. Zero disables it.
    pub bytes_threshold: u64,
    /// Target checkpoint part size.
    pub part_max_bytes: usize,
    /// Number of newest checkpoint directories to retain.
    pub retain_checkpoints: usize,
    /// Background wake-up interval used by [`CheckpointService::spawn`].
    pub poll_interval: Duration,
}

impl CheckpointConfig {
    /// Build a validated configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] when required names or numeric knobs are invalid.
    pub fn new(
        tenant: String,
        topic: String,
        frames_threshold: u64,
        bytes_threshold: u64,
        part_max_bytes: usize,
        retain_checkpoints: usize,
    ) -> Result<Self, SubstrateError> {
        let config = Self {
            tenant,
            topic,
            frames_threshold,
            bytes_threshold,
            part_max_bytes,
            retain_checkpoints,
            poll_interval: Duration::from_secs(1),
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate invariants that make checkpointing safe and deterministic.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] when a field cannot produce a valid checkpoint.
    pub fn validate(&self) -> Result<(), SubstrateError> {
        if self.tenant.is_empty() {
            return Err(SubstrateError::Checkpoint(
                "checkpoint tenant must not be empty".into(),
            ));
        }
        if self.topic.is_empty() {
            return Err(SubstrateError::Checkpoint(
                "checkpoint WAL topic must not be empty".into(),
            ));
        }
        if self.frames_threshold == 0 && self.bytes_threshold == 0 {
            return Err(SubstrateError::Checkpoint(
                "at least one checkpoint threshold must be non-zero".into(),
            ));
        }
        if self.part_max_bytes < 8 {
            return Err(SubstrateError::Checkpoint(
                "checkpoint part_max_bytes must fit one empty key/value pair".into(),
            ));
        }
        if self.retain_checkpoints == 0 {
            return Err(SubstrateError::Checkpoint(
                "checkpoint retain_checkpoints must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// Reason a checkpoint attempt was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointTrigger {
    /// Explicit caller request.
    Manual,
    /// Frame threshold crossed.
    Frames,
    /// Byte threshold crossed.
    Bytes,
}

/// Counters accumulated since the last successful checkpoint and truncate.
///
/// These interval counters reset after checkpoint success. They are checkpoint
/// scheduling inputs only and must never be exported as live range statistics.
#[derive(Debug, Default)]
pub struct CheckpointStats {
    counters: Mutex<CheckpointCounters>,
}

#[derive(Debug, Default)]
struct CheckpointCounters {
    frames: u64,
    bytes: u64,
}

impl CheckpointStats {
    /// Record one or more committed frames.
    pub fn record_committed(&self, frames: u64, bytes: u64) {
        let mut counters = self.counters.lock().expect("checkpoint stats lock");
        counters.frames = counters.frames.wrapping_add(frames);
        counters.bytes = counters.bytes.wrapping_add(bytes);
    }

    /// Return the current `(frames, bytes)` counters.
    #[must_use]
    pub fn snapshot(&self) -> (u64, u64) {
        let counters = self.counters.lock().expect("checkpoint stats lock");
        (counters.frames, counters.bytes)
    }

    fn discard_checkpointed(&self, checkpointed: (u64, u64)) {
        let mut counters = self.counters.lock().expect("checkpoint stats lock");
        counters.frames = counters.frames.wrapping_sub(checkpointed.0);
        counters.bytes = counters.bytes.wrapping_sub(checkpointed.1);
    }
}

/// Seam for WAL truncation after a durable checkpoint manifest.
#[async_trait::async_trait]
pub trait CheckpointWalPruner: Send + Sync {
    /// Apply `DeleteRecords` operations.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] if the log start cannot be advanced.
    async fn delete_records(&self, ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError>;
}

/// Result of one successful checkpoint attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointRun {
    /// Trigger that caused the attempt.
    pub trigger: CheckpointTrigger,
    /// Durable manifest written last.
    pub manifest: Manifest,
    /// Prune plan applied after the manifest became durable.
    pub prune: WalPrunePlan,
    /// Verified manifest metadata for operator/control-plane consumers.
    pub metadata: CheckpointMetadata,
}

/// Checkpoint service that snapshots KV state, writes parts and manifest, truncates WAL, then prunes objects.
pub struct CheckpointService<P> {
    config: CheckpointConfig,
    kv: Arc<dyn SnapshotKv>,
    store: Arc<dyn CheckpointStore>,
    pruner: Arc<P>,
    stats: Arc<CheckpointStats>,
    attempt_lock: AsyncMutex<()>,
    #[cfg(feature = "checkpoint-test-hooks")]
    failpoint: Option<CheckpointFailpoint>,
}

impl<P> CheckpointService<P>
where
    P: CheckpointWalPruner + 'static,
{
    /// Build a service from already constructed seams.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] when `config` is invalid.
    pub fn new(
        config: CheckpointConfig,
        kv: Arc<dyn SnapshotKv>,
        store: Arc<dyn CheckpointStore>,
        pruner: Arc<P>,
        stats: Arc<CheckpointStats>,
    ) -> Result<Self, SubstrateError> {
        config.validate()?;
        Ok(Self {
            config,
            kv,
            store,
            pruner,
            stats,
            attempt_lock: AsyncMutex::new(()),
            #[cfg(feature = "checkpoint-test-hooks")]
            failpoint: None,
        })
    }

    /// Install a deterministic crash hook in builds explicitly compiled for checkpoint tests.
    #[cfg(feature = "checkpoint-test-hooks")]
    #[must_use]
    pub fn with_test_failpoint(mut self, failpoint: CheckpointFailpoint) -> Self {
        self.failpoint = Some(failpoint);
        self
    }

    /// Build a service backed by a workspace [`ObjectStoreConfig`].
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] when the object-store config or checkpoint config is invalid.
    pub fn from_object_store_config(
        config: CheckpointConfig,
        object_store: &ObjectStoreConfig,
        kv: Arc<dyn SnapshotKv>,
        pruner: Arc<P>,
        stats: Arc<CheckpointStats>,
    ) -> Result<Self, SubstrateError> {
        let object_store = build_object_store(object_store)
            .map_err(|error| SubstrateError::Checkpoint(format!("object store: {error}")))?;
        let ops: Arc<dyn ObjectOps> = Arc::new(ObjectStoreClient::new(object_store));
        Self::new(
            config,
            kv,
            Arc::new(ObjectOpsCheckpointStore::new(ops)),
            pruner,
            stats,
        )
    }

    /// Return shared counters for writer integration.
    #[must_use]
    pub fn stats(&self) -> Arc<CheckpointStats> {
        self.stats.clone()
    }

    /// Return the trigger whose threshold has crossed, if any.
    #[must_use]
    pub fn threshold_trigger(&self) -> Option<CheckpointTrigger> {
        let (frames, bytes) = self.stats.snapshot();
        if self.config.frames_threshold != 0 && frames >= self.config.frames_threshold {
            return Some(CheckpointTrigger::Frames);
        }
        if self.config.bytes_threshold != 0 && bytes >= self.config.bytes_threshold {
            return Some(CheckpointTrigger::Bytes);
        }
        None
    }

    /// Checkpoint if a threshold has crossed.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] if snapshotting, manifest writing, truncating, or pruning fails.
    pub async fn checkpoint_if_threshold_crossed(
        &self,
        snapshot: CheckpointSnapshot,
    ) -> Result<Option<CheckpointRun>, SubstrateError> {
        let Some(trigger) = self.threshold_trigger() else {
            return Ok(None);
        };
        self.checkpoint(snapshot, trigger).await.map(Some)
    }

    /// Force one checkpoint attempt.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] if snapshotting, manifest writing, truncating, or pruning fails.
    pub async fn checkpoint(
        &self,
        snapshot: CheckpointSnapshot,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointRun, SubstrateError> {
        let _guard = self.attempt_lock.lock().await;
        let checkpointed_stats = self.stats.snapshot();
        #[cfg(not(feature = "checkpoint-test-hooks"))]
        let manifest = write_checkpoint(
            self.store.as_ref(),
            &self.config.tenant,
            self.kv.as_ref(),
            snapshot,
            self.config.part_max_bytes,
        )
        .await?;
        #[cfg(feature = "checkpoint-test-hooks")]
        let manifest = match &self.failpoint {
            Some(failpoint) => {
                super::runtime::write_checkpoint_with_failpoint(
                    self.store.as_ref(),
                    &self.config.tenant,
                    self.kv.as_ref(),
                    snapshot,
                    self.config.part_max_bytes,
                    failpoint,
                )
                .await?
            }
            None => {
                write_checkpoint(
                    self.store.as_ref(),
                    &self.config.tenant,
                    self.kv.as_ref(),
                    snapshot,
                    self.config.part_max_bytes,
                )
                .await?
            }
        };
        let prune = plan_prune(
            self.store.as_ref(),
            &self.config.tenant,
            &self.config.topic,
            &manifest,
            self.config.retain_checkpoints,
        )
        .await?;
        self.pruner.delete_records(&prune.delete_records).await?;
        #[cfg(feature = "checkpoint-test-hooks")]
        self.fail_at(CheckpointServiceStep::Truncated)?;
        for key in &prune.delete_object_keys {
            self.store.delete(key).await?;
        }
        #[cfg(feature = "checkpoint-test-hooks")]
        self.fail_at(CheckpointServiceStep::Pruned)?;
        let metadata = latest_checkpoint_metadata(
            self.store.as_ref(),
            &self.config.tenant,
            manifest.wal_generation,
            None,
        )
        .await?
        .unwrap_or_else(|| checkpoint_metadata_from_manifest(&manifest));
        self.stats.discard_checkpointed(checkpointed_stats);
        Ok(CheckpointRun {
            trigger,
            manifest,
            prune,
            metadata,
        })
    }

    async fn checkpoint_from_source(
        &self,
        source: &CheckpointSnapshotSource,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointRun, SubstrateError> {
        let _guard = self.attempt_lock.lock().await;
        let checkpointed_stats = self.stats.snapshot();
        let (snapshot, kv_snapshot) = source.capture(self.kv.as_ref()).await?;
        self.finish_captured_checkpoint(source, snapshot, kv_snapshot, trigger, checkpointed_stats)
            .await
    }

    async fn finish_captured_checkpoint(
        &self,
        source: &CheckpointSnapshotSource,
        snapshot: CheckpointSnapshot,
        kv_snapshot: Box<dyn KvSnapshot>,
        trigger: CheckpointTrigger,
        checkpointed_stats: (u64, u64),
    ) -> Result<CheckpointRun, SubstrateError> {
        #[cfg(not(feature = "checkpoint-test-hooks"))]
        let manifest = super::runtime::write_captured_checkpoint(
            self.store.as_ref(),
            &self.config.tenant,
            kv_snapshot,
            snapshot,
            self.config.part_max_bytes,
        )
        .await?;
        #[cfg(feature = "checkpoint-test-hooks")]
        let manifest = match &self.failpoint {
            Some(failpoint) => {
                super::runtime::write_captured_checkpoint_with_failpoint(
                    self.store.as_ref(),
                    &self.config.tenant,
                    kv_snapshot,
                    snapshot,
                    self.config.part_max_bytes,
                    failpoint,
                )
                .await?
            }
            None => {
                super::runtime::write_captured_checkpoint(
                    self.store.as_ref(),
                    &self.config.tenant,
                    kv_snapshot,
                    snapshot,
                    self.config.part_max_bytes,
                )
                .await?
            }
        };
        source.assert_current().await?;
        #[cfg(feature = "checkpoint-test-hooks")]
        self.fail_at(CheckpointServiceStep::LeaseValidated)?;
        self.finish_manifest(manifest, trigger, checkpointed_stats)
            .await
    }

    async fn finish_manifest(
        &self,
        manifest: Manifest,
        trigger: CheckpointTrigger,
        checkpointed_stats: (u64, u64),
    ) -> Result<CheckpointRun, SubstrateError> {
        let prune = plan_prune(
            self.store.as_ref(),
            &self.config.tenant,
            &self.config.topic,
            &manifest,
            self.config.retain_checkpoints,
        )
        .await?;
        self.pruner.delete_records(&prune.delete_records).await?;
        #[cfg(feature = "checkpoint-test-hooks")]
        self.fail_at(CheckpointServiceStep::Truncated)?;
        for key in &prune.delete_object_keys {
            self.store.delete(key).await?;
        }
        #[cfg(feature = "checkpoint-test-hooks")]
        self.fail_at(CheckpointServiceStep::Pruned)?;
        let metadata = latest_checkpoint_metadata(
            self.store.as_ref(),
            &self.config.tenant,
            manifest.wal_generation,
            None,
        )
        .await?
        .unwrap_or_else(|| checkpoint_metadata_from_manifest(&manifest));
        self.stats.discard_checkpointed(checkpointed_stats);
        Ok(CheckpointRun {
            trigger,
            manifest,
            prune,
            metadata,
        })
    }

    #[cfg(feature = "checkpoint-test-hooks")]
    fn fail_at(&self, step: CheckpointServiceStep) -> Result<(), SubstrateError> {
        if self.failpoint.as_ref().is_some_and(|hook| hook(step)) {
            return Err(SubstrateError::Checkpoint(format!(
                "test failpoint stopped checkpoint after {step:?}"
            )));
        }
        Ok(())
    }

    /// Spawn a narrow background control loop.
    #[must_use]
    pub fn spawn(self: Arc<Self>) -> CheckpointHandle {
        let (commands, mut receiver) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                match command {
                    CheckpointCommand::Run {
                        snapshot,
                        trigger,
                        reply,
                    } => {
                        let result = self.checkpoint(snapshot, trigger).await;
                        let _ignored = reply.send(result);
                    }
                    CheckpointCommand::RunIfThreshold { snapshot, reply } => {
                        let result = self.checkpoint_if_threshold_crossed(snapshot).await;
                        let _ignored = reply.send(result);
                    }
                    CheckpointCommand::RunFromSource {
                        source,
                        trigger,
                        reply,
                    } => {
                        let result = self.checkpoint_from_source(&source, trigger).await;
                        let _ignored = reply.send(result);
                    }
                    CheckpointCommand::Shutdown => break,
                }
            }
        });
        CheckpointHandle { commands, task }
    }
}

fn checkpoint_metadata_from_manifest(manifest: &Manifest) -> CheckpointMetadata {
    let dir = ckpt_dir(
        &manifest.tenant,
        manifest.wal_generation,
        manifest.covered_offset,
        manifest.producer_epoch,
    );
    CheckpointMetadata {
        tenant: manifest.tenant.clone(),
        wal_generation: manifest.wal_generation,
        covered_offset: manifest.covered_offset,
        manifest_key: manifest_key(&dir),
        total_bytes: manifest.total_bytes.max(1),
    }
}

enum CheckpointCommand {
    Run {
        snapshot: CheckpointSnapshot,
        trigger: CheckpointTrigger,
        reply: oneshot::Sender<Result<CheckpointRun, SubstrateError>>,
    },
    RunIfThreshold {
        snapshot: CheckpointSnapshot,
        reply: oneshot::Sender<Result<Option<CheckpointRun>, SubstrateError>>,
    },
    RunFromSource {
        source: Arc<CheckpointSnapshotSource>,
        trigger: CheckpointTrigger,
        reply: oneshot::Sender<Result<CheckpointRun, SubstrateError>>,
    },
    Shutdown,
}

/// Control handle for a spawned checkpoint service.
pub struct CheckpointHandle {
    commands: mpsc::Sender<CheckpointCommand>,
    task: JoinHandle<()>,
}

impl CheckpointHandle {
    /// Atomically capture the matching WAL metadata and KV snapshot between commit groups.
    pub async fn checkpoint_from_source(
        &self,
        source: Arc<CheckpointSnapshotSource>,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointRun, SubstrateError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(CheckpointCommand::RunFromSource {
                source,
                trigger,
                reply,
            })
            .await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?;
        wait.await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?
    }
    /// Request a checkpoint from the background service.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] if the service has stopped or the checkpoint attempt fails.
    pub async fn checkpoint(
        &self,
        snapshot: CheckpointSnapshot,
        trigger: CheckpointTrigger,
    ) -> Result<CheckpointRun, SubstrateError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(CheckpointCommand::Run {
                snapshot,
                trigger,
                reply,
            })
            .await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?;
        wait.await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?
    }

    /// Request a checkpoint only if the spawned service's thresholds have crossed.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] if the service has stopped or the checkpoint attempt fails.
    pub async fn checkpoint_if_threshold_crossed(
        &self,
        snapshot: CheckpointSnapshot,
    ) -> Result<Option<CheckpointRun>, SubstrateError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(CheckpointCommand::RunIfThreshold { snapshot, reply })
            .await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?;
        wait.await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?
    }

    /// Stop the background task.
    ///
    /// # Errors
    ///
    /// Returns [`SubstrateError`] if the task join fails.
    pub async fn shutdown(self) -> Result<(), SubstrateError> {
        let _ignored = self.commands.send(CheckpointCommand::Shutdown).await;
        self.task
            .await
            .map_err(|error| SubstrateError::Checkpoint(format!("checkpoint join: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use assert2::assert;
    use crabka_pgkv::MemKv;
    use tokio::sync::Notify;

    use super::*;
    use crate::checkpoint::{DEFAULT_PART_MAX_BYTES, InMemoryCheckpointStore};

    #[derive(Default)]
    struct FakePruner {
        fail: AtomicBool,
        calls: std::sync::Mutex<Vec<Vec<DeleteRecordsOp>>>,
    }

    #[async_trait::async_trait]
    impl CheckpointWalPruner for FakePruner {
        async fn delete_records(&self, ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(SubstrateError::Unavailable("delete records failed".into()));
            }
            self.calls.lock().expect("lock").push(ops.to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct BlockingPruner {
        started: Notify,
        release: Notify,
    }

    #[async_trait::async_trait]
    impl CheckpointWalPruner for BlockingPruner {
        async fn delete_records(&self, _ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError> {
            self.started.notify_one();
            self.release.notified().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn thresholds_trigger_checkpoint_and_reset_after_truncate() {
        let kv: Arc<dyn SnapshotKv> = Arc::new(MemKv::default());
        kv.put(b"k".to_vec(), b"v".to_vec()).expect("put");
        let store = InMemoryCheckpointStore::shared();
        let pruner = Arc::new(FakePruner::default());
        let stats = Arc::new(CheckpointStats::default());
        let service =
            CheckpointService::new(config(2, 0), kv, store, pruner.clone(), stats.clone())
                .expect("service");

        stats.record_committed(1, 10);
        assert!(
            service
                .checkpoint_if_threshold_crossed(snapshot(3))
                .await
                .expect("check")
                .is_none()
        );
        stats.record_committed(1, 10);
        let run = service
            .checkpoint_if_threshold_crossed(snapshot(3))
            .await
            .expect("checkpoint")
            .expect("run");

        assert!(run.trigger == CheckpointTrigger::Frames);
        assert!(stats.snapshot() == (0, 0));
        assert!(pruner.calls.lock().expect("lock").len() == 1);
    }

    #[tokio::test]
    async fn failed_truncate_keeps_stats_for_retry() {
        let kv: Arc<dyn SnapshotKv> = Arc::new(MemKv::default());
        let store = InMemoryCheckpointStore::shared();
        let pruner = Arc::new(FakePruner::default());
        pruner.fail.store(true, Ordering::SeqCst);
        let stats = Arc::new(CheckpointStats::default());
        let service = CheckpointService::new(config(1, 0), kv, store, pruner, stats.clone())
            .expect("service");
        stats.record_committed(1, 7);

        let result = service.checkpoint_if_threshold_crossed(snapshot(3)).await;

        assert!(result.is_err());
        assert!(stats.snapshot() == (1, 7));
    }

    #[tokio::test]
    async fn successful_checkpoint_retains_post_snapshot_bytes_for_the_next_threshold() {
        let kv: Arc<dyn SnapshotKv> = Arc::new(MemKv::default());
        let store = InMemoryCheckpointStore::shared();
        let pruner = Arc::new(BlockingPruner::default());
        let stats = Arc::new(CheckpointStats::default());
        let service = Arc::new(
            CheckpointService::new(config(0, 10), kv, store, pruner.clone(), stats.clone())
                .expect("service"),
        );
        stats.record_committed(1, 10);

        let checkpoint_service = service.clone();
        let checkpoint = tokio::spawn(async move {
            checkpoint_service
                .checkpoint_if_threshold_crossed(snapshot(3))
                .await
        });
        pruner.started.notified().await;
        stats.record_committed(1, 10);
        pruner.release.notify_one();
        checkpoint
            .await
            .expect("checkpoint task")
            .expect("checkpoint")
            .expect("checkpoint run");

        assert!(stats.snapshot() == (1, 10));
        assert!(service.threshold_trigger() == Some(CheckpointTrigger::Bytes));
    }

    fn config(frames_threshold: u64, bytes_threshold: u64) -> CheckpointConfig {
        CheckpointConfig::new(
            "tenant".into(),
            "topic".into(),
            frames_threshold,
            bytes_threshold,
            DEFAULT_PART_MAX_BYTES,
            DEFAULT_CHECKPOINT_RETAIN,
        )
        .expect("config")
    }

    fn snapshot(covered_offset: i64) -> CheckpointSnapshot {
        CheckpointSnapshot {
            covered_offset,
            journal_seq: 1,
            producer_epoch: 0,
            wal_generation: 0,
            garbage_horizon_xid: 0,
        }
    }
}
