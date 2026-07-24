//! Dependency-neutral operations required to transfer one hosted range.

use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use crabka_pgexec::SqlEngine;

use crate::{CheckpointManifest, RangeId, RangeMap, RangeSpec, RowInterval, SplitState, TableId};

/// Select authoritative catalog identities whose logical row space intersects a predecessor.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub fn predecessor_table_mapping(
    range_map: &RangeMap,
    predecessor: RangeId,
    tables: impl IntoIterator<Item = (TableId, TableId)>,
) -> Result<BTreeMap<TableId, TableId>, crate::SplitError> {
    let mut mapping = BTreeMap::new();
    for (physical, logical) in tables {
        let owns_rows = range_map
            .scan_segments(
                logical,
                RowInterval {
                    start: None,
                    end: None,
                },
            )?
            .iter()
            .any(|segment| segment.range_id == predecessor);
        if !owns_rows {
            continue;
        }
        if let Some(existing) = mapping.insert(physical, logical)
            && existing != logical
        {
            return Err(crate::SplitError::Hook(format!(
                "physical table {physical} maps to conflicting logical tables {existing} and {logical}"
            )));
        }
    }
    Ok(mapping)
}

#[cfg(test)]
mod mapping_tests {
    use super::predecessor_table_mapping;
    use crate::{MapEpoch, RangeId, RangeKey, RangeMap, RangeSpec, TableId, TenantName};

    fn map() -> RangeMap {
        RangeMap::new(
            TenantName::parse("transfer_mapping").unwrap(),
            MapEpoch::new(1),
            vec![
                RangeSpec::for_interval(
                    RangeId::COORDINATOR,
                    RangeKey::MIN,
                    Some(RangeKey::new(TableId::new(50), 10)),
                ),
                RangeSpec::for_interval(
                    RangeId::new(1),
                    RangeKey::new(TableId::new(50), 10),
                    Some(RangeKey::table_start(TableId::new(52))),
                ),
                RangeSpec::for_interval(
                    RangeId::new(2),
                    RangeKey::table_start(TableId::new(52)),
                    None,
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn selects_row_interval_overlap_not_row_zero_owner() {
        let mapping = predecessor_table_mapping(
            &map(),
            RangeId::new(1),
            [(TableId::new(1), TableId::new(50))],
        )
        .unwrap();
        assert_eq!(mapping.get(&TableId::new(1)), Some(&TableId::new(50)));
    }

    #[test]
    fn excludes_non_overlapping_tables() {
        let mapping = predecessor_table_mapping(
            &map(),
            RangeId::new(1),
            [
                (TableId::new(1), TableId::new(49)),
                (TableId::new(2), TableId::new(52)),
            ],
        )
        .unwrap();
        assert!(mapping.is_empty());
    }

    #[test]
    fn rejects_conflicting_physical_identity() {
        let error = predecessor_table_mapping(
            &map(),
            RangeId::new(1),
            [
                (TableId::new(1), TableId::new(50)),
                (TableId::new(1), TableId::new(51)),
            ],
        )
        .unwrap_err();
        assert!(error.to_string().contains("conflicting logical tables"));
    }
}

/// Catalog-validated physical-to-logical translation used by successor restore.
#[derive(Debug, Clone)]
pub struct ValidatedSplitTransferPlan {
    state: SplitState,
    physical_to_logical: BTreeMap<TableId, TableId>,
}

impl ValidatedSplitTransferPlan {
    pub(crate) fn new(state: SplitState, physical_to_logical: BTreeMap<TableId, TableId>) -> Self {
        Self {
            state,
            physical_to_logical,
        }
    }

    /// Build an authenticated control-plane plan after validating complete successor intent.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    pub fn from_control(
        state: SplitState,
        physical_to_logical: BTreeMap<TableId, TableId>,
    ) -> Result<Self, crate::SplitError> {
        state.transfer_requests()?;
        if physical_to_logical.is_empty() {
            return Err(crate::SplitError::Hook(
                "control transfer requires a physical-to-logical table mapping".into(),
            ));
        }
        Ok(Self::new(state, physical_to_logical))
    }

    /// Validated split state whose descriptors own the logical intervals.
    #[must_use]
    pub const fn state(&self) -> &SplitState {
        &self.state
    }

    /// Complete catalog mapping for physical tables hosted by the predecessor.
    #[must_use]
    pub const fn physical_to_logical(&self) -> &BTreeMap<TableId, TableId> {
        &self.physical_to_logical
    }
}

/// Exact key interval staged into one successor.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableTransferRequest {
    /// The unhosted range that will eventually own the table.
    pub target_range: RangeId,
    /// Exact successor interval; its range id must equal `target_range`.
    pub interval: RangeSpec,
    /// Endpoint expected by the authoritative placement descriptor.
    pub endpoint: String,
    /// Successor WAL generation expected at publication.
    pub wal_generation: u64,
    /// Source generation fenced by this transfer.
    pub predecessor_generation: u64,
}

impl TableTransferRequest {
    pub(crate) fn from_successor(
        successor: &crate::SuccessorDescriptor,
        predecessor_generation: u64,
    ) -> Self {
        Self {
            target_range: successor.range_id,
            interval: successor.interval.clone(),
            endpoint: successor.endpoint.clone(),
            wal_generation: successor.wal_generation,
            predecessor_generation,
        }
    }

    /// Target range identity.
    #[must_use]
    pub const fn target_range(&self) -> RangeId {
        self.target_range
    }

    /// Exact validated storage interval.
    #[must_use]
    pub const fn interval(&self) -> &RangeSpec {
        &self.interval
    }

    /// Authenticated advertised endpoint.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Fenced successor generation.
    #[must_use]
    pub const fn wal_generation(&self) -> u64 {
        self.wal_generation
    }

    /// Retiring predecessor generation.
    #[must_use]
    pub const fn predecessor_generation(&self) -> u64 {
        self.predecessor_generation
    }
}

/// A restored successor that has not been published into a serving range map.
///
/// This value deliberately contains no map-publish operation.  The caller that
/// owns the source pause must atomically publish the successor before allowing
/// it to serve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedRangeSuccessor {
    /// The still-unhosted successor range.
    pub range_id: RangeId,
    /// Endpoint lineage copied from the authoritative descriptor.
    pub endpoint: String,
    /// Fenced WAL generation of the staged runtime.
    pub wal_generation: u64,
}

/// Both staged halves produced from one checkpoint and one bounded tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedRangeSuccessors {
    /// Staged left half. Range zero is replaced, never reused in place.
    pub left: StagedRangeSuccessor,
    /// Staged right half.
    pub right: Option<StagedRangeSuccessor>,
}

/// Runtime resources claimed from a staged successor at the publication boundary.
///
/// The opaque keepalive retains runtime-owned tasks and stores without coupling the
/// range orchestration crate to a particular substrate implementation.
pub struct ClaimedStagedSuccessor {
    /// Claimed range identity.
    pub range_id: RangeId,
    /// Claimed endpoint lineage.
    pub endpoint: String,
    /// Claimed WAL generation.
    pub wal_generation: u64,
    /// The fully restored SQL engine, still absent from every serving map.
    pub engine: SqlEngine,
    /// Runtime resources that must outlive the published engine.
    pub keepalive: Arc<dyn Any + Send + Sync>,
}

/// Runtime resources for both successors, claimed as one publication unit.
pub struct ClaimedStagedSuccessors {
    /// Claimed left half. Range zero is replaced, never reused in place.
    pub left: ClaimedStagedSuccessor,
    /// Claimed right half.
    pub right: Option<ClaimedStagedSuccessor>,
}

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
    /// Durably record the complete activation intent before checkpointing or pausing.
    async fn record_topology_activation_intent(
        &self,
        _state: &crate::SplitState,
    ) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Attach the durable source checkpoint while the predecessor can still commit receipts.
    async fn record_topology_activation_checkpoint(
        &self,
        _operation_id: &str,
        _checkpoint: &CheckpointManifest,
    ) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Validate placement descriptors before the source writer is paused.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn validate_successors(
        &self,
        _plan: &ValidatedSplitTransferPlan,
    ) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Atomically refresh runtime control paths from the newly configured serving engines.
    /// # Errors
    ///
    /// Returns an error when the requested operation cannot be completed.
    fn publish_serving_topology(
        &self,
        _engines: &BTreeMap<RangeId, SqlEngine>,
    ) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Enter the fail-closed publication guard after preparation succeeds.
    fn begin_serving_topology_publication(&self) {}

    /// Durably commit the roll-forward decision through the predecessor range zero.
    ///
    /// After this returns successfully, every failure path must leave the predecessor
    /// paused and startup must finish activation before advertising readiness.
    async fn mark_topology_must_activate(&self) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Cross the irreversible writer-activation point after preparation is complete.
    async fn activate_serving_topology(&self) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Commit a previously prepared topology using infallible in-memory swaps only.
    fn commit_serving_topology(&self) {}

    /// Durably mark the infallible topology swap complete through the serving range zero.
    async fn finish_topology_activation(&self) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Make control paths visible after the gateway's serving snapshot is installed.
    fn finish_serving_topology_publication(&self) {}

    /// Force a durable checkpoint for one hosted source range.
    async fn force_checkpoint(
        &self,
        operation_id: &str,
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
    async fn resume(
        &self,
        operation_id: &str,
        barrier: RangeTransferBarrier,
    ) -> Result<(), RangeTransferError>;

    /// Release the durable checkpoint/WAL pin after resume or retirement succeeds.
    async fn release_checkpoint_pin(
        &self,
        _operation_id: &str,
        _range_id: RangeId,
    ) -> Result<(), RangeTransferError> {
        Ok(())
    }

    /// Synchronously release a held source pause when its transfer future is dropped.
    ///
    /// This is the cancellation-safe counterpart to [`Self::resume`]. Implementations
    /// must make the source writable before returning; errors cannot be reported from
    /// [`Drop`](std::ops::Drop) and should be recorded by the implementation.
    fn resume_after_drop(&self, operation_id: &str, barrier: RangeTransferBarrier);

    /// Stage a checkpoint and bounded tail into an empty, unhosted target range.
    ///
    /// The returned successor is intentionally not serving and no range map is
    /// mutated. The caller retains ownership of the source pause represented by
    /// `barrier` and must resume it on every failure path.
    /// Stage both successor intervals from the same immutable checkpoint and bounded tail.
    async fn stage_successors(
        &self,
        plan: &ValidatedSplitTransferPlan,
        checkpoint: &CheckpointManifest,
        tail: &[CommittedTailRecord],
        barrier: RangeTransferBarrier,
    ) -> Result<StagedRangeSuccessors, RangeTransferError>;

    /// Transfer ownership of a staged successor to the serving-map publisher.
    ///
    /// This must only succeed while the source barrier remains held. Callers must
    /// discard the claimed resources and resume the source if publication cannot
    /// proceed.
    /// Claim both staged successors before an atomic map publication.
    async fn claim_successors(
        &self,
        staged: &StagedRangeSuccessors,
        barrier: RangeTransferBarrier,
    ) -> Result<ClaimedStagedSuccessors, RangeTransferError>;
}
