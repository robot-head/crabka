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
        poll_interval: Duration,
    ) -> Result<Self, SubstrateError> {
        let config = Self {
            tenant,
            topic,
            frames_threshold,
            bytes_threshold,
            part_max_bytes,
            retain_checkpoints,
            poll_interval,
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
        if self.poll_interval.is_zero() {
            return Err(SubstrateError::Checkpoint(
                "checkpoint poll_interval must be greater than zero".into(),
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
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    pub fn record_committed(&self, frames: u64, bytes: u64) {
        let mut counters = self.counters.lock().expect("checkpoint stats lock");
        counters.frames = counters.frames.wrapping_add(frames);
        counters.bytes = counters.bytes.wrapping_add(bytes);
    }

    /// Return the current `(frames, bytes)` counters.
    #[must_use]
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
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

/// Shared latest verified checkpoint metadata consumed by SQL planning.
/// Checkpoint manifests are range/tenant scoped and expose no per-table byte
/// breakdown, so `total_bytes` is intentionally a conservative global upper
/// bound returned for every table in that range.
/// Publication clones the authoritative metadata only after verification; reads
/// are synchronous and never retain the lock across an await point.
#[derive(Debug, Default)]
pub struct CheckpointPlannerStats {
    latest: std::sync::RwLock<Option<CheckpointMetadata>>,
}

impl CheckpointPlannerStats {
    /// Publish metadata returned by the verified checkpoint loader/completer.
    /// # Panics
    ///
    /// Panics if an internal invariant is violated.
    pub fn publish_verified(&self, metadata: CheckpointMetadata) {
        *self.latest.write().expect("checkpoint planner stats lock") = Some(metadata);
    }
}

impl crabka_pgexec::plan_dist::Stats for CheckpointPlannerStats {
    fn estimated_bytes(&self, _table_id: u64) -> Option<u64> {
        self.latest
            .read()
            .expect("checkpoint planner stats lock")
            .as_ref()
            .map(|metadata| metadata.total_bytes)
    }
}

/// Checkpoint service that snapshots KV state, writes parts and manifest, truncates WAL, then prunes objects.
pub struct CheckpointService<P> {
    config: CheckpointConfig,
    kv: Arc<dyn SnapshotKv>,
    store: Arc<dyn CheckpointStore>,
    pruner: Arc<P>,
    stats: Arc<CheckpointStats>,
    planner_stats: Arc<CheckpointPlannerStats>,
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
            planner_stats: Arc::new(CheckpointPlannerStats::default()),
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

    /// Return the shared latest-verified checkpoint source for SQL engines.
    #[must_use]
    pub fn planner_stats(&self) -> Arc<CheckpointPlannerStats> {
        self.planner_stats.clone()
    }

    /// Seed planning from metadata loaded and verified during recovery.
    pub fn publish_planner_metadata(&self, metadata: CheckpointMetadata) {
        self.planner_stats.publish_verified(metadata);
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
        self.planner_stats.publish_verified(metadata.clone());
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
        self.planner_stats.publish_verified(metadata.clone());
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
        self.spawn_loop(None)
    }

    /// Spawn a background control loop that also polls thresholds using `source`.
    #[must_use]
    pub fn spawn_periodic(
        self: Arc<Self>,
        source: Arc<CheckpointSnapshotSource>,
    ) -> CheckpointHandle {
        self.spawn_loop(Some(source))
    }

    fn spawn_loop(
        self: Arc<Self>,
        source: Option<Arc<CheckpointSnapshotSource>>,
    ) -> CheckpointHandle {
        let (commands, mut receiver) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            let Some(source) = source else {
                while let Some(command) = receiver.recv().await {
                    if !self.run_command(command).await {
                        break;
                    }
                }
                return;
            };
            let first_tick = tokio::time::Instant::now() + self.config.poll_interval;
            let mut interval = tokio::time::interval_at(first_tick, self.config.poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    command = receiver.recv() => {
                        let Some(command) = command else {
                            break;
                        };
                        if !self.run_command(command).await {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if let Some(trigger) = self.threshold_trigger()
                            && let Err(error) = self.checkpoint_from_source(&source, trigger).await
                        {
                            tracing::warn!(%error, "periodic checkpoint failed");
                        }
                    }
                }
            }
        });
        CheckpointHandle { commands, task }
    }

    async fn run_command(&self, command: CheckpointCommand) -> bool {
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
                pin_operation,
                reply,
            } => {
                let result = self.checkpoint_from_source(&source, trigger).await;
                let result = match (result, pin_operation) {
                    (Ok(run), Some(operation_id)) => super::runtime::pin_checkpoint(
                        self.store.as_ref(),
                        &self.config.tenant,
                        &operation_id,
                        &run.metadata.manifest_key,
                        run.manifest.wal_generation,
                        run.manifest.covered_offset,
                    )
                    .await
                    .map(|()| run),
                    (result, _) => result,
                };
                let _ignored = reply.send(result);
            }
            CheckpointCommand::ReleasePin {
                operation_id,
                reply,
            } => {
                let result = super::runtime::unpin_checkpoint(
                    self.store.as_ref(),
                    &self.config.tenant,
                    &operation_id,
                )
                .await;
                let _ignored = reply.send(result);
            }
            CheckpointCommand::Shutdown => return false,
        }
        true
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
        pin_operation: Option<String>,
        reply: oneshot::Sender<Result<CheckpointRun, SubstrateError>>,
    },
    ReleasePin {
        operation_id: String,
        reply: oneshot::Sender<Result<(), SubstrateError>>,
    },
    Shutdown,
}

/// Control handle for a spawned checkpoint service.
pub struct CheckpointHandle {
    commands: mpsc::Sender<CheckpointCommand>,
    task: JoinHandle<()>,
}

impl CheckpointHandle {
    /// Abort a staged checkpoint worker that will never become serving.
    pub fn abort(&self) {
        self.task.abort();
    }

    /// Atomically capture the matching WAL metadata and KV snapshot between commit groups.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
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
                pin_operation: None,
                reply,
            })
            .await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?;
        wait.await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?
    }

    /// Atomically checkpoint and durably pin its WAL/object boundary for an operation.
    ///
    /// # Errors
    ///
    /// Returns an error when checkpointing or writing the pin fails.
    pub async fn checkpoint_from_source_pinned(
        &self,
        source: Arc<CheckpointSnapshotSource>,
        trigger: CheckpointTrigger,
        operation_id: String,
    ) -> Result<CheckpointRun, SubstrateError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(CheckpointCommand::RunFromSource {
                source,
                trigger,
                pin_operation: Some(operation_id),
                reply,
            })
            .await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?;
        wait.await
            .map_err(|_| SubstrateError::Checkpoint("checkpoint service stopped".into()))?
    }

    /// Release an operation's durable checkpoint pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the service stopped or the marker cannot be deleted.
    pub async fn release_pin(&self, operation_id: String) -> Result<(), SubstrateError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(CheckpointCommand::ReleasePin {
                operation_id,
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
    use crabka_pgkv::{Kv, MemKv};
    use tokio::sync::Notify;

    use super::*;
    use crate::checkpoint::{DEFAULT_PART_MAX_BYTES, InMemoryCheckpointStore};

    #[derive(Default)]
    struct FakePruner {
        fail: AtomicBool,
        attempted: Notify,
        calls: std::sync::Mutex<Vec<Vec<DeleteRecordsOp>>>,
    }

    #[async_trait::async_trait]
    impl CheckpointWalPruner for FakePruner {
        async fn delete_records(&self, ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError> {
            self.attempted.notify_one();
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
        let store: Arc<dyn CheckpointStore> = InMemoryCheckpointStore::shared();
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

    #[test]
    fn checkpoint_config_rejects_zero_poll_interval() {
        let result = CheckpointConfig::new(
            "tenant".into(),
            "topic".into(),
            1,
            0,
            DEFAULT_PART_MAX_BYTES,
            DEFAULT_CHECKPOINT_RETAIN,
            Duration::ZERO,
        );

        assert!(result.is_err());
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

    #[tokio::test(start_paused = true)]
    async fn periodic_checkpoint_waits_for_the_first_interval() {
        let (service, pruner, stats, source) = periodic_service(1);
        stats.record_committed(1, 7);

        let handle = service.spawn_periodic(source);
        tokio::task::yield_now().await;

        assert!(pruner.calls.lock().expect("lock").is_empty());
        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_checkpoint_skips_below_threshold() {
        let (service, pruner, stats, source) = periodic_service(2);
        stats.record_committed(1, 7);
        let handle = service.spawn_periodic(source);

        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;

        assert!(pruner.calls.lock().expect("lock").is_empty());
        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_checkpoint_runs_after_threshold_crosses() {
        let (service, pruner, stats, source) = periodic_service(1);
        stats.record_committed(1, 7);
        let handle = service.spawn_periodic(source);

        tokio::time::advance(Duration::from_millis(10)).await;
        pruner.attempted.notified().await;

        assert!(pruner.calls.lock().expect("lock").len() == 1);
        assert!(stats.snapshot() == (0, 0));
        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_checkpoint_retries_after_tick_failure() {
        let (service, pruner, stats, source) = periodic_service(1);
        pruner.fail.store(true, Ordering::SeqCst);
        stats.record_committed(1, 7);
        let handle = service.spawn_periodic(source);

        tokio::time::advance(Duration::from_millis(10)).await;
        pruner.attempted.notified().await;
        assert!(stats.snapshot() == (1, 7));

        pruner.fail.store(false, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(10)).await;
        pruner.attempted.notified().await;

        assert!(pruner.calls.lock().expect("lock").len() == 1);
        assert!(stats.snapshot() == (0, 0));
        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn pinned_force_checkpoint_survives_more_than_retention_until_release() {
        let kv: Arc<dyn SnapshotKv> = Arc::new(MemKv::default());
        let store = InMemoryCheckpointStore::shared();
        let pruner = Arc::new(FakePruner::default());
        let stats = Arc::new(CheckpointStats::default());
        let mut config = config(1, 0);
        config.retain_checkpoints = 1;
        let service = Arc::new(
            CheckpointService::new(config, kv, store.clone(), pruner.clone(), stats)
                .expect("service"),
        );
        let source = Arc::new(CheckpointSnapshotSource::new(
            1,
            2,
            crate::writer::WriterGeneration(0),
        ));
        let handle = service.spawn();
        let pinned = handle
            .checkpoint_from_source_pinned(source, CheckpointTrigger::Manual, "split-a".to_owned())
            .await
            .expect("pinned checkpoint");

        for offset in 2..=4 {
            handle
                .checkpoint(snapshot(offset), CheckpointTrigger::Manual)
                .await
                .expect("later checkpoint");
        }

        assert!(store.get(&pinned.metadata.manifest_key).await.is_ok());
        assert!(pruner.calls.lock().expect("lock").last().expect("prune")[0].offset == 2);

        handle
            .release_pin("split-a".to_owned())
            .await
            .expect("release pin");
        handle
            .checkpoint(snapshot(5), CheckpointTrigger::Manual)
            .await
            .expect("checkpoint after release");
        assert!(store.get(&pinned.metadata.manifest_key).await.is_err());
        assert!(pruner.calls.lock().expect("lock").last().expect("prune")[0].offset == 6);
        handle.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn verified_checkpoint_publication_changes_next_join_plan() {
        use crabka_pgexec::plan_dist::{
            CombinedStats, JoinInputs, JoinStrategy, PlannerConfig, plan_join, plan_join_for_tables,
        };

        let concrete_kv = Arc::new(MemKv::default());
        concrete_kv
            .put(crabka_pgkv::key::seq_key(1), 1_u64.to_be_bytes().to_vec())
            .expect("left sequence");
        concrete_kv
            .put(crabka_pgkv::key::seq_key(2), 100_u64.to_be_bytes().to_vec())
            .expect("right sequence");
        concrete_kv
            .put(b"large-checkpoint-key".to_vec(), vec![7; 512])
            .expect("put");
        let kv: Arc<dyn SnapshotKv> = concrete_kv.clone();
        let store: Arc<dyn CheckpointStore> = InMemoryCheckpointStore::shared();
        let service = CheckpointService::new(
            config(1, 0),
            kv,
            Arc::clone(&store),
            Arc::new(FakePruner::default()),
            Arc::new(CheckpointStats::default()),
        )
        .expect("service");
        let engine_kv: Arc<dyn crabka_pgkv::Kv> = concrete_kv;
        let mut engine = crabka_pgexec::SqlEngine::with_kv(engine_kv).expect("engine");
        engine.set_join_stats(Arc::new(CombinedStats::new(
            engine.join_stats(),
            service.planner_stats(),
        )));
        let planner_config = PlannerConfig {
            broadcast_threshold_bytes: 64,
        };
        let inputs = JoinInputs {
            left_table_id: 1,
            right_table_id: 2,
        };
        assert_eq!(
            plan_join(engine.join_stats().as_ref(), planner_config, inputs),
            JoinStrategy::Broadcast { small_table_id: 1 }
        );

        service
            .checkpoint(snapshot(3), CheckpointTrigger::Manual)
            .await
            .expect("checkpoint");

        assert_eq!(
            plan_join(engine.join_stats().as_ref(), planner_config, inputs),
            JoinStrategy::Gather
        );

        let restarted = CheckpointService::new(
            config(1, 0),
            Arc::new(MemKv::default()),
            Arc::clone(&store),
            Arc::new(FakePruner::default()),
            Arc::new(CheckpointStats::default()),
        )
        .expect("restarted service");
        let restored = latest_checkpoint_metadata(store.as_ref(), "tenant", 0, None)
            .await
            .expect("load metadata")
            .expect("verified checkpoint");
        restarted.publish_planner_metadata(restored);
        assert!(
            crabka_pgexec::plan_dist::Stats::estimated_bytes(restarted.planner_stats().as_ref(), 1)
                .is_some_and(|bytes| bytes > 64)
        );
        assert_eq!(
            crabka_pgexec::plan_dist::Stats::estimated_bytes(restarted.planner_stats().as_ref(), 1,),
            crabka_pgexec::plan_dist::Stats::estimated_bytes(
                restarted.planner_stats().as_ref(),
                999,
            ),
            "range checkpoint bytes are a global upper bound, not per-table metadata",
        );

        let table = |id, name: &str, group: &str| crabka_pgcatalog::Table {
            id,
            name: name.into(),
            columns: vec![crabka_pgcatalog::Column::new(
                "id",
                crabka_pgtypes::ColumnType::Int4,
            )],
            sharded: true,
            sharding: Some(crabka_pgcatalog::ShardingStrategy::Hash(
                crabka_pgcatalog::HashSharding {
                    columns: vec!["id".into()],
                    buckets: 4,
                    co_location_group: Some(group.into()),
                },
            )),
            foreign: None,
        };
        let left = table(1, "left", "pair");
        let right = table(2, "right", "pair");
        let no_broadcast = PlannerConfig {
            broadcast_threshold_bytes: 0,
        };
        assert_eq!(
            plan_join_for_tables(
                engine.join_stats().as_ref(),
                no_broadcast,
                &left,
                &right,
                &[0],
                &[0],
            ),
            JoinStrategy::CoPartitioned,
        );
        let wrong_group = table(2, "right", "other");
        assert_eq!(
            plan_join_for_tables(
                engine.join_stats().as_ref(),
                no_broadcast,
                &left,
                &wrong_group,
                &[0],
                &[0],
            ),
            JoinStrategy::Gather,
        );
        assert_eq!(
            plan_join_for_tables(
                engine.join_stats().as_ref(),
                no_broadcast,
                &left,
                &right,
                &[1],
                &[1],
            ),
            JoinStrategy::Gather,
        );
    }

    fn config(frames_threshold: u64, bytes_threshold: u64) -> CheckpointConfig {
        CheckpointConfig::new(
            "tenant".into(),
            "topic".into(),
            frames_threshold,
            bytes_threshold,
            DEFAULT_PART_MAX_BYTES,
            DEFAULT_CHECKPOINT_RETAIN,
            Duration::from_secs(1),
        )
        .expect("config")
    }

    fn periodic_service(
        frames_threshold: u64,
    ) -> (
        Arc<CheckpointService<FakePruner>>,
        Arc<FakePruner>,
        Arc<CheckpointStats>,
        Arc<CheckpointSnapshotSource>,
    ) {
        let kv: Arc<dyn SnapshotKv> = Arc::new(MemKv::default());
        let store = InMemoryCheckpointStore::shared();
        let pruner = Arc::new(FakePruner::default());
        let stats = Arc::new(CheckpointStats::default());
        let mut config = config(frames_threshold, 0);
        config.poll_interval = Duration::from_millis(10);
        let service = Arc::new(
            CheckpointService::new(config, kv, store, pruner.clone(), stats.clone())
                .expect("service"),
        );
        let source = Arc::new(CheckpointSnapshotSource::new(
            3,
            1,
            crate::writer::WriterGeneration(0),
        ));
        (service, pruner, stats, source)
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
