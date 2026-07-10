//! Production integration boundary for split orchestration side effects.
//!
//! This module adapts explicitly supplied infrastructure clients to [`SplitHooks`]. It does not
//! provide a broker, storage, registry, or operator client, and therefore does not claim to move
//! live data on its own.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{CheckpointManifest, InDoubtMarker, SplitError, SplitHooks, SplitState};

/// Identifies an infrastructure operation required by split orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitHookOperation {
    /// Checkpoint predecessor ranges.
    Checkpoint,
    /// Pause and unpause writes.
    WriteGate,
    /// Commit the target range map.
    RangeMapCommit,
    /// Restore successor data from checkpoints.
    SuccessorRestore,
    /// Fence and prologue the successor.
    SuccessorPrologue,
    /// Inherit in-doubt markers.
    InDoubtMarkerInheritance,
    /// Park predecessor ranges.
    PredecessorParking,
}

/// Forces durable predecessor checkpoints through the storage/checkpoint integration.
#[async_trait]
pub trait CheckpointOperation: Send + Sync {
    /// Force a checkpoint for the ordinary predecessor path.
    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError>;

    /// Force a checkpoint for the right predecessor of a merge.
    async fn force_right_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError>;
}

/// Pauses and unpauses writes through the serving/write-gate integration.
#[async_trait]
pub trait WriteGateOperation: Send + Sync {
    /// Pause writes before conversion begins.
    async fn pause_conversion_writes(&self, state: &SplitState) -> Result<(), SplitError>;

    /// Pause writes at the supplied durable checkpoint boundary.
    async fn pause_writes_at_covered_offset(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError>;

    /// Unpause serving after a completed operation.
    async fn unpause_serving(&self, state: &SplitState) -> Result<(), SplitError>;
}

/// Restores only the successor interval through the storage integration.
#[async_trait]
pub trait FilteredSuccessorRestoreOperation: Send + Sync {
    /// Start ordinary successor restore filtered to the interval in `state.successor_after`.
    async fn start_successor_restore(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError>;

    /// Start merge successor restore from both checkpoints filtered to `state.successor_after`.
    async fn start_merge_successor_restore(
        &self,
        state: &SplitState,
        left_checkpoint: &CheckpointManifest,
        right_checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError>;
}

/// Commits the target map through the range-map registry integration.
#[async_trait]
pub trait RangeMapCommitOperation: Send + Sync {
    /// Commit the target range map.
    async fn commit_map_version(&self, state: &SplitState) -> Result<(), SplitError>;
}

/// Fences and prologues a successor through its lifecycle integration.
#[async_trait]
pub trait SuccessorPrologueOperation: Send + Sync {
    /// Fence and prologue the successor before it serves.
    async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError>;
}

/// Transfers in-doubt markers through the transaction-coordination integration.
#[async_trait]
pub trait InDoubtMarkerInheritanceOperation: Send + Sync {
    /// Return the markers inherited by the successor interval.
    async fn inherit_in_doubt_markers(
        &self,
        state: &SplitState,
    ) -> Result<Vec<InDoubtMarker>, SplitError>;
}

/// Parks predecessor ranges through their lifecycle integration.
#[async_trait]
pub trait PredecessorParkingOperation: Send + Sync {
    /// Park the ordinary predecessor.
    async fn park_predecessor(&self, state: &SplitState) -> Result<(), SplitError>;

    /// Park the right predecessor of a merge.
    async fn park_right_predecessor(&self, state: &SplitState) -> Result<(), SplitError>;
}

/// Concrete adapter that connects injected infrastructure operations to [`SplitHooks`].
///
/// It is production-ready as an integration boundary: every hook delegates to an explicit client,
/// and absent clients fail clearly. It does not supply those clients or implement live data
/// movement itself.
pub struct SplitHookAdapter {
    checkpoint: Option<Arc<dyn CheckpointOperation>>,
    write_gate: Option<Arc<dyn WriteGateOperation>>,
    filtered_restore: Option<Arc<dyn FilteredSuccessorRestoreOperation>>,
    map_commit: Option<Arc<dyn RangeMapCommitOperation>>,
    prologue: Option<Arc<dyn SuccessorPrologueOperation>>,
    marker_inheritance: Option<Arc<dyn InDoubtMarkerInheritanceOperation>>,
    parking: Option<Arc<dyn PredecessorParkingOperation>>,
}

/// Builder for [`SplitHookAdapter`].
///
/// [`Self::build`] rejects incomplete wiring. [`Self::build_fail_clear`] is intentionally useful
/// only while assembling an integration: it preserves missing operations so they fail at their
/// corresponding durable step rather than silently succeeding.
#[derive(Default)]
pub struct SplitHookAdapterBuilder {
    checkpoint: Option<Arc<dyn CheckpointOperation>>,
    write_gate: Option<Arc<dyn WriteGateOperation>>,
    filtered_restore: Option<Arc<dyn FilteredSuccessorRestoreOperation>>,
    map_commit: Option<Arc<dyn RangeMapCommitOperation>>,
    prologue: Option<Arc<dyn SuccessorPrologueOperation>>,
    marker_inheritance: Option<Arc<dyn InDoubtMarkerInheritanceOperation>>,
    parking: Option<Arc<dyn PredecessorParkingOperation>>,
}

impl SplitHookAdapterBuilder {
    /// Begin explicit adapter wiring.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl SplitHookAdapterBuilder {
    /// Supply the checkpoint integration.
    #[must_use]
    pub fn checkpoint(mut self, operation: Arc<dyn CheckpointOperation>) -> Self {
        self.checkpoint = Some(operation);
        self
    }

    /// Supply the write-gate integration.
    #[must_use]
    pub fn write_gate(mut self, operation: Arc<dyn WriteGateOperation>) -> Self {
        self.write_gate = Some(operation);
        self
    }

    /// Supply the successor-filtered restore integration.
    #[must_use]
    pub fn filtered_restore(
        mut self,
        operation: Arc<dyn FilteredSuccessorRestoreOperation>,
    ) -> Self {
        self.filtered_restore = Some(operation);
        self
    }

    /// Supply the range-map registry integration.
    #[must_use]
    pub fn map_commit(mut self, operation: Arc<dyn RangeMapCommitOperation>) -> Self {
        self.map_commit = Some(operation);
        self
    }

    /// Supply the successor lifecycle integration.
    #[must_use]
    pub fn prologue(mut self, operation: Arc<dyn SuccessorPrologueOperation>) -> Self {
        self.prologue = Some(operation);
        self
    }

    /// Supply the in-doubt marker integration.
    #[must_use]
    pub fn marker_inheritance(
        mut self,
        operation: Arc<dyn InDoubtMarkerInheritanceOperation>,
    ) -> Self {
        self.marker_inheritance = Some(operation);
        self
    }

    /// Supply the predecessor lifecycle integration.
    #[must_use]
    pub fn parking(mut self, operation: Arc<dyn PredecessorParkingOperation>) -> Self {
        self.parking = Some(operation);
        self
    }

    /// Build an adapter only when every required integration is wired.
    ///
    /// # Errors
    ///
    /// Returns the first missing integration in orchestration order.
    pub fn build(self) -> Result<SplitHookAdapter, SplitError> {
        let adapter = self.build_fail_clear();
        adapter.ensure_complete()?;
        Ok(adapter)
    }

    /// Build an adapter that fails clearly for operations not yet wired.
    #[must_use]
    pub fn build_fail_clear(self) -> SplitHookAdapter {
        SplitHookAdapter {
            checkpoint: self.checkpoint,
            write_gate: self.write_gate,
            filtered_restore: self.filtered_restore,
            map_commit: self.map_commit,
            prologue: self.prologue,
            marker_inheritance: self.marker_inheritance,
            parking: self.parking,
        }
    }
}

impl SplitHookAdapter {
    fn ensure_complete(&self) -> Result<(), SplitError> {
        for (operation, configured) in [
            (SplitHookOperation::Checkpoint, self.checkpoint.is_some()),
            (SplitHookOperation::WriteGate, self.write_gate.is_some()),
            (
                SplitHookOperation::RangeMapCommit,
                self.map_commit.is_some(),
            ),
            (
                SplitHookOperation::SuccessorRestore,
                self.filtered_restore.is_some(),
            ),
            (
                SplitHookOperation::SuccessorPrologue,
                self.prologue.is_some(),
            ),
            (
                SplitHookOperation::InDoubtMarkerInheritance,
                self.marker_inheritance.is_some(),
            ),
            (
                SplitHookOperation::PredecessorParking,
                self.parking.is_some(),
            ),
        ] {
            if !configured {
                return Err(SplitError::UnavailableHookOperation { operation });
            }
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<&dyn CheckpointOperation, SplitError> {
        required_operation(self.checkpoint.as_ref(), SplitHookOperation::Checkpoint)
    }

    fn write_gate(&self) -> Result<&dyn WriteGateOperation, SplitError> {
        required_operation(self.write_gate.as_ref(), SplitHookOperation::WriteGate)
    }

    fn filtered_restore(&self) -> Result<&dyn FilteredSuccessorRestoreOperation, SplitError> {
        required_operation(
            self.filtered_restore.as_ref(),
            SplitHookOperation::SuccessorRestore,
        )
    }

    fn map_commit(&self) -> Result<&dyn RangeMapCommitOperation, SplitError> {
        required_operation(self.map_commit.as_ref(), SplitHookOperation::RangeMapCommit)
    }

    fn prologue(&self) -> Result<&dyn SuccessorPrologueOperation, SplitError> {
        required_operation(
            self.prologue.as_ref(),
            SplitHookOperation::SuccessorPrologue,
        )
    }

    fn marker_inheritance(&self) -> Result<&dyn InDoubtMarkerInheritanceOperation, SplitError> {
        required_operation(
            self.marker_inheritance.as_ref(),
            SplitHookOperation::InDoubtMarkerInheritance,
        )
    }

    fn parking(&self) -> Result<&dyn PredecessorParkingOperation, SplitError> {
        required_operation(
            self.parking.as_ref(),
            SplitHookOperation::PredecessorParking,
        )
    }
}

fn required_operation<T: ?Sized>(
    operation: Option<&Arc<T>>,
    kind: SplitHookOperation,
) -> Result<&T, SplitError> {
    operation
        .map(AsRef::as_ref)
        .ok_or(SplitError::UnavailableHookOperation { operation: kind })
}

#[async_trait]
impl SplitHooks for SplitHookAdapter {
    async fn pause_conversion_writes(&self, state: &SplitState) -> Result<(), SplitError> {
        self.write_gate()?.pause_conversion_writes(state).await
    }

    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        self.checkpoint()?.force_predecessor_checkpoint(state).await
    }

    async fn force_right_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError> {
        self.checkpoint()?
            .force_right_predecessor_checkpoint(state)
            .await
    }

    async fn pause_writes_at_covered_offset(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.write_gate()?
            .pause_writes_at_covered_offset(state, checkpoint)
            .await
    }

    async fn commit_map_version(&self, state: &SplitState) -> Result<(), SplitError> {
        self.map_commit()?.commit_map_version(state).await
    }

    async fn start_successor_restore(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.filtered_restore()?
            .start_successor_restore(state, checkpoint)
            .await
    }

    async fn start_merge_successor_restore(
        &self,
        state: &SplitState,
        left_checkpoint: &CheckpointManifest,
        right_checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError> {
        self.filtered_restore()?
            .start_merge_successor_restore(state, left_checkpoint, right_checkpoint)
            .await
    }

    async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError> {
        self.prologue()?.successor_fence_prologue(state).await
    }

    async fn inherit_in_doubt_markers(
        &self,
        state: &SplitState,
    ) -> Result<Vec<InDoubtMarker>, SplitError> {
        self.marker_inheritance()?
            .inherit_in_doubt_markers(state)
            .await
    }

    async fn park_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
        self.parking()?.park_predecessor(state).await
    }

    async fn park_right_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
        self.parking()?.park_right_predecessor(state).await
    }

    async fn unpause_serving(&self, state: &SplitState) -> Result<(), SplitError> {
        self.write_gate()?.unpause_serving(state).await
    }
}
