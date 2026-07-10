//! Dependency-neutral operations required to transfer one hosted range.

use async_trait::async_trait;

use crate::{CheckpointManifest, RangeId};

/// A committed WAL record retained for a bounded range transfer tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTailRecord {
    /// Offset of this committed record in the range WAL.
    pub offset: i64,
    /// Encoded WAL frame bytes.
    pub bytes: Vec<u8>,
}

/// The fence boundary that makes a transfer tail finite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeTransferBarrier {
    /// Range whose writer was fenced.
    pub range_id: RangeId,
    /// Inclusive committed WAL offset through which the tail is valid.
    pub offset: i64,
}

/// Errors reported by a hosted-range transfer capability.
#[derive(Debug, thiserror::Error)]
pub enum RangeTransferError {
    /// The selected runtime deliberately has no durable transfer substrate.
    #[error("range transfer is unavailable for r{range_id}: {reason}")]
    Unavailable {
        /// Range requested by the caller.
        range_id: RangeId,
        /// Why this runtime cannot provide the operation.
        reason: String,
    },
    /// The caller requested an invalid transfer boundary.
    #[error("invalid transfer boundary for r{range_id}: {reason}")]
    Boundary {
        /// Range requested by the caller.
        range_id: RangeId,
        /// Why the boundary cannot be used.
        reason: String,
    },
    /// A pause or transfer barrier is already active for this range.
    #[error("range transfer is already paused for r{range_id}")]
    AlreadyPaused {
        /// Range whose writer is already reserved or paused.
        range_id: RangeId,
    },
    /// The underlying runtime failed while executing the operation.
    #[error("range transfer failed for r{range_id}: {reason}")]
    Runtime {
        /// Range requested by the caller.
        range_id: RangeId,
        /// Runtime failure detail.
        reason: String,
    },
}

/// Runtime operations needed by split orchestration without depending on a substrate crate.
///
/// Implementations must fence writes before returning a barrier and must return only committed
/// records in the inclusive interval requested from [`Self::read_committed_tail`].  Restoring a
/// target is intentionally an empty-target operation; publishing a range map remains outside this
/// foundation seam.
#[async_trait]
pub trait RangeTransferCapability: Send + Sync {
    /// Force a durable checkpoint for one hosted source range.
    async fn force_checkpoint(
        &self,
        range_id: RangeId,
    ) -> Result<CheckpointManifest, RangeTransferError>;

    /// Stop source writes and return the committed boundary for the transfer tail.
    async fn pause_at_checkpoint(
        &self,
        checkpoint: &CheckpointManifest,
    ) -> Result<RangeTransferBarrier, RangeTransferError>;

    /// Read committed WAL records bounded inclusively by `barrier`.
    async fn read_committed_tail(
        &self,
        range_id: RangeId,
        after_offset: i64,
        barrier: RangeTransferBarrier,
    ) -> Result<Vec<CommittedTailRecord>, RangeTransferError>;

    /// Release the stored pause guard and resume source writes after a transfer barrier.
    ///
    /// Call this on every transfer error or cancellation path after
    /// [`Self::pause_at_checkpoint`] succeeds.
    async fn resume(&self, barrier: RangeTransferBarrier) -> Result<(), RangeTransferError>;

    /// Restore a checkpoint and bounded tail into an empty, already-hosted target range.
    async fn restore_empty_target(
        &self,
        target_range: RangeId,
        checkpoint: &CheckpointManifest,
        tail: &[CommittedTailRecord],
        barrier: RangeTransferBarrier,
    ) -> Result<(), RangeTransferError>;
}
