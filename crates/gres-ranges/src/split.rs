//! Durable range split orchestration seams.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    MapEpoch, RangeId, RangeKey, RangeMap, RangeSpec, TableId, split_hooks::SplitHookOperation,
};

/// Durable checkpoint manifest that bounds predecessor writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointManifest {
    /// Range that produced the checkpoint.
    pub range_id: RangeId,
    /// WAL offset covered by the checkpoint.
    pub covered_offset: i64,
    /// Durable object key for the checkpoint manifest.
    pub manifest_key: String,
}

impl CheckpointManifest {
    fn ensure_for_predecessor(&self, predecessor: RangeId) -> Result<(), SplitError> {
        if self.range_id != predecessor {
            return Err(SplitError::InvalidCheckpointRange {
                expected: predecessor,
                actual: self.range_id,
            });
        }
        if self.covered_offset < 0 {
            return Err(SplitError::InvalidCoveredOffset {
                covered_offset: self.covered_offset,
            });
        }
        if self.manifest_key.is_empty() {
            return Err(SplitError::EmptyCheckpointManifestKey);
        }
        Ok(())
    }
}

/// In-doubt marker copied to a successor when its key interval moves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InDoubtMarker {
    /// Transaction identifier for the prepared/in-doubt decision.
    pub transaction_id: u64,
    /// Key that determines interval ownership.
    pub key: RangeKey,
}

/// User request to replace one range with two fresh successor halves.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitCommand {
    /// Tenant range map before the split.
    pub current_map: RangeMap,
    /// Existing range that owns the whole interval before commit.
    pub predecessor: RangeId,
    /// New range that owns the right half after commit.
    pub successor: RangeId,
    /// Split point; belongs to the successor after commit.
    pub split_at: RangeKey,
}

/// User request to move one whole range to a new generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRangeCommand {
    /// Tenant range map before the move.
    pub current_map: RangeMap,
    /// Range being moved.
    pub range_id: RangeId,
    /// New generation used by the successor compute.
    pub successor_generation: u64,
}

/// User request to merge two adjacent ranges into the left range id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeRangeCommand {
    /// Tenant range map before the merge.
    pub current_map: RangeMap,
    /// Left adjacent range. This range id survives the merge.
    pub left: RangeId,
    /// Right adjacent range. This range is parked after the merge.
    pub right: RangeId,
    /// WAL generation used by the merged successor compute.
    pub successor_generation: u64,
}

/// User request to convert one ordinary table range to timestamp-sharded metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvertTableCommand {
    /// Tenant range map before the conversion.
    pub current_map: RangeMap,
    /// Range that owns the unsharded table before conversion.
    pub range_id: RangeId,
    /// Catalog table being converted.
    pub table_id: TableId,
    /// WAL generation used by the post-conversion serving epoch.
    pub successor_generation: u64,
}

/// Split orchestration operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SplitOperation {
    /// A proper interval split.
    Split,
    /// A whole-range move through the same checkpoint/restore/prologue/park path.
    Move,
    /// Adjacent ranges folded into the left range id.
    Merge,
    /// One table converted from xid visibility to timestamp visibility.
    ConvertTable,
}

/// Durable step. The stored value is the next step to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitStep {
    /// Pause writes before a conversion checkpoint rewrites tuple visibility.
    PauseConversionWrites,
    /// Force predecessor checkpoint.
    ForcePredecessorCheckpoint,
    /// Force the right-side predecessor checkpoint for a merge.
    ForceRightPredecessorCheckpoint,
    /// Pause writes at the covered checkpoint offset.
    PauseWritesAtCoveredOffset,
    /// Commit map version n+1 on range 0.
    CommitMapVersion,
    /// Start successor restore from the predecessor checkpoint.
    StartSuccessorRestore,
    /// Run successor fence/prologue before serving.
    SuccessorFencePrologue,
    /// Copy in-doubt markers whose keys belong to the successor interval.
    InheritInDoubtMarkers,
    /// Park the predecessor through the lifecycle seam.
    ParkPredecessor,
    /// Park the right-side predecessor through the lifecycle seam for a merge.
    ParkRightPredecessor,
    /// Unpause serving after ownership is safe.
    UnpauseServing,
    /// The orchestration completed.
    Complete,
}

/// Durable split state persisted after every completed step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SplitState {
    /// Stable orchestration identifier chosen by the caller.
    pub operation_id: String,
    /// Operation flavor.
    pub operation: SplitOperation,
    /// Range that owned the interval before the map commit.
    pub predecessor: RangeId,
    /// Range that owns the moved interval after the map commit.
    pub successor: RangeId,
    /// Successor WAL generation for whole-range moves.
    pub successor_generation: Option<u64>,
    /// Original predecessor interval.
    pub predecessor_before: RangeSpec,
    /// Predecessor interval after map commit.
    pub predecessor_after: RangeSpec,
    /// Successor interval after map commit.
    pub successor_after: RangeSpec,
    /// Right-side predecessor interval before a merge.
    pub merge_right_before: Option<RangeSpec>,
    /// Table converted by a conversion operation.
    pub conversion_table: Option<TableId>,
    /// Map before the operation.
    pub current_map: RangeMap,
    /// Map committed by range 0.
    pub target_map: RangeMap,
    /// Next step to run.
    pub next_step: SplitStep,
    /// Durable checkpoint, once forced.
    pub checkpoint: Option<CheckpointManifest>,
    /// Durable right-side checkpoint for merges, once forced.
    pub right_checkpoint: Option<CheckpointManifest>,
    /// Markers inherited by the successor interval, once copied.
    pub inherited_markers: Vec<InDoubtMarker>,
}

impl SplitState {
    /// Build the initial durable split state.
    pub fn for_split(
        operation_id: impl Into<String>,
        command: SplitCommand,
    ) -> Result<Self, SplitError> {
        let plan = command.current_map.plan_split_at_key(
            command.predecessor,
            command.split_at,
            command.successor,
        )?;
        let left_successor = if command.predecessor.is_coordinator() {
            RangeId::COORDINATOR
        } else {
            next_unused_range_id(&command.current_map, &[command.successor])?
        };
        let left = RangeSpec::for_interval(
            left_successor,
            plan.left.start,
            plan.left.end,
        );
        let target_map = map_with_replaced_ranges(
            &command.current_map,
            command.current_map.epoch().next()?,
            command.predecessor,
            &[left.clone(), plan.right.clone()],
        )?;

        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(SplitError::EmptyOperationId);
        }

        Ok(Self {
            operation_id,
            operation: SplitOperation::Split,
            predecessor: command.predecessor,
            successor: command.successor,
            successor_generation: None,
            predecessor_before: predecessor_before(&command.current_map, command.predecessor)?,
            predecessor_after: left,
            successor_after: plan.right,
            merge_right_before: None,
            conversion_table: None,
            current_map: command.current_map,
            target_map,
            next_step: SplitStep::ForcePredecessorCheckpoint,
            checkpoint: None,
            right_checkpoint: None,
            inherited_markers: Vec::new(),
        })
    }

    /// Build the initial durable move state as a degenerate split.
    pub fn for_move(
        operation_id: impl Into<String>,
        command: MoveRangeCommand,
    ) -> Result<Self, SplitError> {
        let predecessor_before = predecessor_before(&command.current_map, command.range_id)?;
        let successor = next_unused_range_id(&command.current_map, &[])?;
        let target_map = map_with_replaced_ranges(
            &command.current_map,
            command.current_map.epoch().next()?,
            command.range_id,
            &[RangeSpec::for_interval(
                successor,
                predecessor_before.start,
                predecessor_before.end,
            )],
        )?;

        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(SplitError::EmptyOperationId);
        }

        Ok(Self {
            operation_id,
            operation: SplitOperation::Move,
            predecessor: command.range_id,
            successor,
            successor_generation: Some(command.successor_generation),
            predecessor_before: predecessor_before.clone(),
            predecessor_after: RangeSpec::for_interval(
                successor,
                predecessor_before.start,
                predecessor_before.end,
            ),
            successor_after: RangeSpec::for_interval(
                successor,
                predecessor_before.start,
                predecessor_before.end,
            ),
            merge_right_before: None,
            conversion_table: None,
            current_map: command.current_map,
            target_map,
            next_step: SplitStep::ForcePredecessorCheckpoint,
            checkpoint: None,
            right_checkpoint: None,
            inherited_markers: Vec::new(),
        })
    }

    /// Build the initial durable merge state.
    pub fn for_merge(
        operation_id: impl Into<String>,
        command: MergeRangeCommand,
    ) -> Result<Self, SplitError> {
        let plan = command
            .current_map
            .plan_merge(command.left, command.right)?;
        let target_map = command.current_map.merge_adjacent_ranges(
            command.current_map.epoch().next()?,
            command.left,
            command.right,
        )?;

        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(SplitError::EmptyOperationId);
        }

        Ok(Self {
            operation_id,
            operation: SplitOperation::Merge,
            predecessor: command.left,
            successor: command.left,
            successor_generation: Some(command.successor_generation),
            predecessor_before: predecessor_before(&command.current_map, command.left)?,
            predecessor_after: plan.merged.clone(),
            successor_after: plan.merged,
            merge_right_before: Some(predecessor_before(&command.current_map, command.right)?),
            conversion_table: None,
            current_map: command.current_map,
            target_map,
            next_step: SplitStep::ForcePredecessorCheckpoint,
            checkpoint: None,
            right_checkpoint: None,
            inherited_markers: Vec::new(),
        })
    }

    /// Build the initial durable online conversion state.
    pub fn for_conversion(
        operation_id: impl Into<String>,
        command: ConvertTableCommand,
    ) -> Result<Self, SplitError> {
        let predecessor_before = predecessor_before(&command.current_map, command.range_id)?;
        let table_start = RangeKey::table_start(command.table_id);
        if !predecessor_before.contains_key(table_start) {
            return Err(SplitError::ConversionTableOutsideRange {
                table_id: command.table_id,
                range_id: command.range_id,
            });
        }
        let target_map = RangeMap::new(
            command.current_map.tenant().clone(),
            command.current_map.epoch().next()?,
            command.current_map.ranges().to_vec(),
        )?;

        let operation_id = operation_id.into();
        if operation_id.is_empty() {
            return Err(SplitError::EmptyOperationId);
        }

        Ok(Self {
            operation_id,
            operation: SplitOperation::ConvertTable,
            predecessor: command.range_id,
            successor: command.range_id,
            successor_generation: Some(command.successor_generation),
            predecessor_before: predecessor_before.clone(),
            predecessor_after: predecessor_before.clone(),
            successor_after: predecessor_before,
            merge_right_before: None,
            conversion_table: Some(command.table_id),
            current_map: command.current_map,
            target_map,
            next_step: SplitStep::PauseConversionWrites,
            checkpoint: None,
            right_checkpoint: None,
            inherited_markers: Vec::new(),
        })
    }

    fn advance_to(&mut self, next_step: SplitStep) {
        self.next_step = next_step;
    }

    fn checkpoint(&self) -> Result<&CheckpointManifest, SplitError> {
        self.checkpoint
            .as_ref()
            .ok_or(SplitError::MissingCheckpoint)
    }

    fn right_checkpoint(&self) -> Result<&CheckpointManifest, SplitError> {
        self.right_checkpoint
            .as_ref()
            .ok_or(SplitError::MissingCheckpoint)
    }

    fn merge_right_before(&self) -> Result<&RangeSpec, SplitError> {
        self.merge_right_before
            .as_ref()
            .ok_or(SplitError::MissingMergeRightRange)
    }

    fn is_merge(&self) -> bool {
        self.operation == SplitOperation::Merge
    }

    fn is_conversion(&self) -> bool {
        self.operation == SplitOperation::ConvertTable
    }
}

/// Durable split-state store seam.
#[async_trait::async_trait]
pub trait SplitStateStore: Send + Sync {
    /// Load persisted orchestration state by id.
    async fn load_split_state(&self, operation_id: &str) -> Result<Option<SplitState>, SplitError>;
    /// Save the whole durable orchestration state.
    async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError>;
}

/// Idempotent side-effect seams used by the orchestrator.
#[async_trait::async_trait]
pub trait SplitHooks: Send + Sync {
    /// Pause writes before the conversion checkpoint/rewrite pass starts.
    async fn pause_conversion_writes(&self, state: &SplitState) -> Result<(), SplitError>;
    /// Force a durable predecessor checkpoint.
    async fn force_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError>;
    /// Force a durable right-side predecessor checkpoint during merge.
    async fn force_right_predecessor_checkpoint(
        &self,
        state: &SplitState,
    ) -> Result<CheckpointManifest, SplitError>;
    /// Pause writes exactly at the checkpoint-covered offset.
    async fn pause_writes_at_covered_offset(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError>;
    /// Commit the range map version n+1 on range 0.
    async fn commit_map_version(&self, state: &SplitState) -> Result<(), SplitError>;
    /// Start successor restore from the predecessor checkpoint.
    async fn start_successor_restore(
        &self,
        state: &SplitState,
        checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError>;
    /// Start merged successor restore from both predecessor checkpoints.
    async fn start_merge_successor_restore(
        &self,
        state: &SplitState,
        left_checkpoint: &CheckpointManifest,
        right_checkpoint: &CheckpointManifest,
    ) -> Result<(), SplitError>;
    /// Fence and prologue the successor before it may serve.
    async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError>;
    /// Inherit only markers whose keys are inside the successor interval.
    async fn inherit_in_doubt_markers(
        &self,
        state: &SplitState,
    ) -> Result<Vec<InDoubtMarker>, SplitError>;
    /// Park the predecessor through its lifecycle seam.
    async fn park_predecessor(&self, state: &SplitState) -> Result<(), SplitError>;
    /// Park the right-side predecessor after a merge.
    async fn park_right_predecessor(&self, state: &SplitState) -> Result<(), SplitError>;
    /// Unpause serving once the operation is complete.
    async fn unpause_serving(&self, state: &SplitState) -> Result<(), SplitError>;
}

/// Stateless orchestrator over durable state and idempotent hooks.
pub struct SplitOrchestrator<'a> {
    store: &'a dyn SplitStateStore,
    hooks: &'a dyn SplitHooks,
}

impl<'a> SplitOrchestrator<'a> {
    /// Build an orchestrator.
    #[must_use]
    pub const fn new(store: &'a dyn SplitStateStore, hooks: &'a dyn SplitHooks) -> Self {
        Self { store, hooks }
    }

    /// Run until the split state reaches [`SplitStep::Complete`].
    pub async fn run(&self, initial_state: SplitState) -> Result<SplitState, SplitError> {
        let mut state = if let Some(stored) = self
            .store
            .load_split_state(&initial_state.operation_id)
            .await?
        {
            ensure_same_operation(stored, &initial_state)?
        } else {
            self.store.save_split_state(&initial_state).await?;
            initial_state
        };

        while state.next_step != SplitStep::Complete {
            self.run_next_step(&mut state).await?;
            self.store.save_split_state(&state).await?;
        }

        Ok(state)
    }

    async fn run_next_step(&self, state: &mut SplitState) -> Result<(), SplitError> {
        match state.next_step {
            SplitStep::PauseConversionWrites => {
                self.hooks.pause_conversion_writes(state).await?;
                state.advance_to(SplitStep::ForcePredecessorCheckpoint);
            }
            SplitStep::ForcePredecessorCheckpoint => {
                self.force_predecessor_checkpoint(state).await?;
            }
            SplitStep::ForceRightPredecessorCheckpoint => {
                let checkpoint = self.hooks.force_right_predecessor_checkpoint(state).await?;
                checkpoint.ensure_for_predecessor(state.merge_right_before()?.range_id)?;
                state.right_checkpoint = Some(checkpoint);
                state.advance_to(SplitStep::PauseWritesAtCoveredOffset);
            }
            SplitStep::PauseWritesAtCoveredOffset => {
                self.hooks
                    .pause_writes_at_covered_offset(state, state.checkpoint()?)
                    .await?;
                state.advance_to(SplitStep::CommitMapVersion);
            }
            SplitStep::CommitMapVersion => {
                self.hooks.commit_map_version(state).await?;
                if state.is_conversion() {
                    state.advance_to(SplitStep::SuccessorFencePrologue);
                    return Ok(());
                }
                state.advance_to(SplitStep::StartSuccessorRestore);
            }
            SplitStep::StartSuccessorRestore => {
                self.start_successor_restore(state).await?;
                state.advance_to(SplitStep::SuccessorFencePrologue);
            }
            SplitStep::SuccessorFencePrologue => {
                self.hooks.successor_fence_prologue(state).await?;
                state.advance_to(SplitStep::InheritInDoubtMarkers);
            }
            SplitStep::InheritInDoubtMarkers => {
                self.inherit_in_doubt_markers(state).await?;
            }
            SplitStep::ParkPredecessor => {
                self.hooks.park_predecessor(state).await?;
                if state.is_merge() {
                    state.advance_to(SplitStep::ParkRightPredecessor);
                    return Ok(());
                }
                state.advance_to(SplitStep::UnpauseServing);
            }
            SplitStep::ParkRightPredecessor => {
                self.hooks.park_right_predecessor(state).await?;
                state.advance_to(SplitStep::UnpauseServing);
            }
            SplitStep::UnpauseServing => {
                self.hooks.unpause_serving(state).await?;
                state.advance_to(SplitStep::Complete);
            }
            SplitStep::Complete => {}
        }
        Ok(())
    }

    async fn force_predecessor_checkpoint(&self, state: &mut SplitState) -> Result<(), SplitError> {
        let checkpoint = self.hooks.force_predecessor_checkpoint(state).await?;
        checkpoint.ensure_for_predecessor(state.predecessor)?;
        state.checkpoint = Some(checkpoint);
        if state.is_merge() {
            state.advance_to(SplitStep::ForceRightPredecessorCheckpoint);
            return Ok(());
        }
        if state.is_conversion() {
            state.advance_to(SplitStep::CommitMapVersion);
            return Ok(());
        }
        state.advance_to(SplitStep::PauseWritesAtCoveredOffset);
        Ok(())
    }

    async fn start_successor_restore(&self, state: &SplitState) -> Result<(), SplitError> {
        if state.is_merge() {
            return self
                .hooks
                .start_merge_successor_restore(
                    state,
                    state.checkpoint()?,
                    state.right_checkpoint()?,
                )
                .await;
        }

        self.hooks
            .start_successor_restore(state, state.checkpoint()?)
            .await
    }

    async fn inherit_in_doubt_markers(&self, state: &mut SplitState) -> Result<(), SplitError> {
        let markers = self.hooks.inherit_in_doubt_markers(state).await?;
        if state.is_conversion() && !markers.is_empty() {
            return Err(SplitError::ConversionInDoubtPrepared {
                table_id: state.conversion_table.expect("conversion table present"),
                transaction_id: markers[0].transaction_id,
            });
        }
        if let Some(marker) = markers
            .iter()
            .find(|marker| !state.successor_after.contains_key(marker.key))
        {
            return Err(SplitError::MarkerOutsideSuccessorInterval { key: marker.key });
        }
        state.inherited_markers = markers;
        if state.is_conversion() {
            state.advance_to(SplitStep::UnpauseServing);
            return Ok(());
        }
        state.advance_to(if state.predecessor.is_coordinator() {
            SplitStep::UnpauseServing
        } else {
            SplitStep::ParkPredecessor
        });
        Ok(())
    }
}

/// Convenience runner for a split command.
pub async fn run_split(
    operation_id: impl Into<String>,
    command: SplitCommand,
    store: &dyn SplitStateStore,
    hooks: &dyn SplitHooks,
) -> Result<SplitState, SplitError> {
    let state = SplitState::for_split(operation_id, command)?;
    SplitOrchestrator::new(store, hooks).run(state).await
}

/// Convenience runner for a move command.
pub async fn run_move(
    operation_id: impl Into<String>,
    command: MoveRangeCommand,
    store: &dyn SplitStateStore,
    hooks: &dyn SplitHooks,
) -> Result<SplitState, SplitError> {
    let state = SplitState::for_move(operation_id, command)?;
    SplitOrchestrator::new(store, hooks).run(state).await
}

/// Convenience runner for a merge command.
pub async fn run_merge(
    operation_id: impl Into<String>,
    command: MergeRangeCommand,
    store: &dyn SplitStateStore,
    hooks: &dyn SplitHooks,
) -> Result<SplitState, SplitError> {
    let state = SplitState::for_merge(operation_id, command)?;
    SplitOrchestrator::new(store, hooks).run(state).await
}

/// Convenience runner for a table conversion command.
pub async fn run_conversion(
    operation_id: impl Into<String>,
    command: ConvertTableCommand,
    store: &dyn SplitStateStore,
    hooks: &dyn SplitHooks,
) -> Result<SplitState, SplitError> {
    let state = SplitState::for_conversion(operation_id, command)?;
    SplitOrchestrator::new(store, hooks).run(state).await
}

/// Split orchestration errors.
#[derive(Debug, Error)]
pub enum SplitError {
    /// Operation id must be stable and non-empty.
    #[error("split operation id must not be empty")]
    EmptyOperationId,
    /// Map validation failed.
    #[error(transparent)]
    Map(#[from] crate::MapValidationError),
    /// Map epoch overflowed.
    #[error("range map epoch must not overflow")]
    MapEpochOverflow,
    /// No fresh range identity can be allocated.
    #[error("range id must not overflow")]
    RangeIdOverflow,
    /// A retry reused an operation id for different state.
    #[error("split operation id was reused with different state")]
    OperationMismatch,
    /// The step needs a checkpoint persisted by an earlier step.
    #[error("split state is missing predecessor checkpoint")]
    MissingCheckpoint,
    /// Merge state is missing the right predecessor range.
    #[error("merge state is missing right predecessor range")]
    MissingMergeRightRange,
    /// Checkpoint range did not match the predecessor.
    #[error("checkpoint range r{actual} did not match predecessor r{expected}")]
    InvalidCheckpointRange {
        /// Expected predecessor range.
        expected: RangeId,
        /// Actual checkpoint range.
        actual: RangeId,
    },
    /// Checkpoint covered offset was invalid.
    #[error("checkpoint covered offset {covered_offset} is negative")]
    InvalidCoveredOffset {
        /// Invalid offset.
        covered_offset: i64,
    },
    /// Checkpoint manifest key was empty.
    #[error("checkpoint manifest key must not be empty")]
    EmptyCheckpointManifestKey,
    /// A hook returned an in-doubt marker outside the successor interval.
    #[error("in-doubt marker at {key:?} is outside the successor interval")]
    MarkerOutsideSuccessorInterval {
        /// Offending key.
        key: RangeKey,
    },
    /// Conversion was attempted while an unresolved prepared marker still existed.
    #[error("table {table_id} conversion found in-doubt prepared transaction {transaction_id}")]
    ConversionInDoubtPrepared {
        /// Table being converted.
        table_id: TableId,
        /// First unresolved transaction observed.
        transaction_id: u64,
    },
    /// Conversion target table is not owned by the requested range.
    #[error("table {table_id} is outside conversion range r{range_id}")]
    ConversionTableOutsideRange {
        /// Table being converted.
        table_id: TableId,
        /// Range requested for conversion.
        range_id: RangeId,
    },
    /// Durable state storage failed.
    #[error("split state store failed: {0}")]
    Store(String),
    /// A split side-effect seam failed.
    #[error("split hook failed: {0}")]
    Hook(String),
    /// A required integration operation was not supplied to the production hook adapter.
    #[error("split hook operation is unavailable: {operation:?}")]
    UnavailableHookOperation {
        /// Missing operation.
        operation: SplitHookOperation,
    },
}

trait NextEpoch {
    fn next(self) -> Result<Self, SplitError>
    where
        Self: Sized;
}

impl NextEpoch for MapEpoch {
    fn next(self) -> Result<Self, SplitError> {
        let value: u64 = self.into();
        value
            .checked_add(1)
            .map(MapEpoch::new)
            .ok_or(SplitError::MapEpochOverflow)
    }
}

fn predecessor_before(range_map: &RangeMap, predecessor: RangeId) -> Result<RangeSpec, SplitError> {
    range_map
        .ranges()
        .iter()
        .find(|range| range.range_id == predecessor)
        .cloned()
        .ok_or(crate::MapValidationError::InvalidSplitPoint {
            range_id: predecessor,
            split_at: RangeKey::MIN,
        })
        .map_err(Into::into)
}

fn next_unused_range_id(
    range_map: &RangeMap,
    reserved: &[RangeId],
) -> Result<RangeId, SplitError> {
    range_map
        .ranges()
        .iter()
        .map(|range| range.range_id)
        .chain(reserved.iter().copied())
        .max()
        .unwrap_or(RangeId::COORDINATOR)
        .as_u32()
        .checked_add(1)
        .map(RangeId::new)
        .ok_or(SplitError::RangeIdOverflow)
}

fn map_with_replaced_ranges(
    current_map: &RangeMap,
    target_epoch: MapEpoch,
    replaced: RangeId,
    replacements: &[RangeSpec],
) -> Result<RangeMap, SplitError> {
    let mut ranges = Vec::with_capacity(current_map.ranges().len() + replacements.len());
    for range in current_map.ranges() {
        if range.range_id == replaced {
            ranges.extend_from_slice(replacements);
            continue;
        }
        ranges.push(range.clone());
    }
    Ok(RangeMap::new(
        current_map.tenant().clone(),
        target_epoch,
        ranges,
    )?)
}

fn ensure_same_operation(
    stored: SplitState,
    initial_state: &SplitState,
) -> Result<SplitState, SplitError> {
    if stored.operation_id != initial_state.operation_id
        || stored.operation != initial_state.operation
        || stored.predecessor != initial_state.predecessor
        || stored.successor != initial_state.successor
        || stored.successor_generation != initial_state.successor_generation
        || stored.merge_right_before != initial_state.merge_right_before
        || stored.conversion_table != initial_state.conversion_table
        || stored.current_map != initial_state.current_map
        || stored.target_map != initial_state.target_map
    {
        return Err(SplitError::OperationMismatch);
    }

    Ok(stored)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;

    use super::*;
    use crate::{TableId, TenantName};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Event {
        PauseConversion,
        Checkpoint(MapEpoch),
        Pause(i64),
        Commit(MapEpoch),
        Restore(i64),
        Prologue(RangeId),
        Inherit(RangeId),
        Park(RangeId),
        Unpause,
    }

    #[derive(Default)]
    struct MemoryStore(Mutex<Option<SplitState>>);

    #[async_trait::async_trait]
    impl SplitStateStore for MemoryStore {
        async fn load_split_state(
            &self,
            _operation_id: &str,
        ) -> Result<Option<SplitState>, SplitError> {
            Ok(self.0.lock().expect("state lock").clone())
        }

        async fn save_split_state(&self, state: &SplitState) -> Result<(), SplitError> {
            *self.0.lock().expect("state lock") = Some(state.clone());
            Ok(())
        }
    }

    struct TestHooks {
        events: Mutex<Vec<Event>>,
        fail_before: Mutex<Option<SplitStep>>,
        markers: Vec<InDoubtMarker>,
    }

    impl TestHooks {
        fn new(markers: Vec<InDoubtMarker>) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                fail_before: Mutex::new(None),
                markers,
            }
        }

        fn fail_before(&self, step: SplitStep) {
            *self.fail_before.lock().expect("fail lock") = Some(step);
        }

        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("events lock").clone()
        }

        fn push(&self, step: SplitStep, event: Event) -> Result<(), SplitError> {
            if *self.fail_before.lock().expect("fail lock") == Some(step) {
                return Err(SplitError::Hook("kill".to_owned()));
            }
            self.events.lock().expect("events lock").push(event);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl SplitHooks for TestHooks {
        async fn pause_conversion_writes(&self, _state: &SplitState) -> Result<(), SplitError> {
            self.push(SplitStep::PauseConversionWrites, Event::PauseConversion)
        }

        async fn force_predecessor_checkpoint(
            &self,
            state: &SplitState,
        ) -> Result<CheckpointManifest, SplitError> {
            self.push(
                SplitStep::ForcePredecessorCheckpoint,
                Event::Checkpoint(state.current_map.epoch()),
            )?;
            Ok(CheckpointManifest {
                range_id: state.predecessor,
                covered_offset: 42,
                manifest_key: "chk".to_owned(),
            })
        }

        async fn force_right_predecessor_checkpoint(
            &self,
            state: &SplitState,
        ) -> Result<CheckpointManifest, SplitError> {
            let right = state.merge_right_before()?;
            self.push(
                SplitStep::ForceRightPredecessorCheckpoint,
                Event::Checkpoint(state.current_map.epoch()),
            )?;
            Ok(CheckpointManifest {
                range_id: right.range_id,
                covered_offset: 84,
                manifest_key: "right-chk".to_owned(),
            })
        }

        async fn pause_writes_at_covered_offset(
            &self,
            _state: &SplitState,
            checkpoint: &CheckpointManifest,
        ) -> Result<(), SplitError> {
            self.push(
                SplitStep::PauseWritesAtCoveredOffset,
                Event::Pause(checkpoint.covered_offset),
            )
        }

        async fn commit_map_version(&self, state: &SplitState) -> Result<(), SplitError> {
            self.push(
                SplitStep::CommitMapVersion,
                Event::Commit(state.target_map.epoch()),
            )
        }

        async fn start_successor_restore(
            &self,
            _state: &SplitState,
            checkpoint: &CheckpointManifest,
        ) -> Result<(), SplitError> {
            self.push(
                SplitStep::StartSuccessorRestore,
                Event::Restore(checkpoint.covered_offset),
            )
        }

        async fn start_merge_successor_restore(
            &self,
            _state: &SplitState,
            left_checkpoint: &CheckpointManifest,
            right_checkpoint: &CheckpointManifest,
        ) -> Result<(), SplitError> {
            self.push(
                SplitStep::StartSuccessorRestore,
                Event::Restore(left_checkpoint.covered_offset + right_checkpoint.covered_offset),
            )
        }

        async fn successor_fence_prologue(&self, state: &SplitState) -> Result<(), SplitError> {
            self.push(
                SplitStep::SuccessorFencePrologue,
                Event::Prologue(state.successor),
            )
        }

        async fn inherit_in_doubt_markers(
            &self,
            state: &SplitState,
        ) -> Result<Vec<InDoubtMarker>, SplitError> {
            self.push(
                SplitStep::InheritInDoubtMarkers,
                Event::Inherit(state.successor),
            )?;
            Ok(self
                .markers
                .iter()
                .filter(|marker| state.successor_after.contains_key(marker.key))
                .cloned()
                .collect())
        }

        async fn park_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
            self.push(SplitStep::ParkPredecessor, Event::Park(state.predecessor))
        }

        async fn park_right_predecessor(&self, state: &SplitState) -> Result<(), SplitError> {
            self.push(
                SplitStep::ParkRightPredecessor,
                Event::Park(state.merge_right_before()?.range_id),
            )
        }

        async fn unpause_serving(&self, _state: &SplitState) -> Result<(), SplitError> {
            self.push(SplitStep::UnpauseServing, Event::Unpause)
        }
    }

    #[tokio::test]
    async fn split_happy_path_runs_explicit_steps() {
        let store = MemoryStore::default();
        let hooks = TestHooks::new(markers());

        let state = run_split("split-1", split_command(), &store, &hooks)
            .await
            .expect("split");

        assert!(state.next_step == SplitStep::Complete);
        assert!(state.target_map.epoch() == MapEpoch::new(8));
        assert!(state.inherited_markers == vec![markers()[1].clone()]);
        assert!(hooks.events() == expected_split_events(RangeId::new(2)));
    }

    #[tokio::test]
    async fn kill_at_every_step_is_idempotent_with_map_version() {
        let steps = [
            SplitStep::ForcePredecessorCheckpoint,
            SplitStep::PauseWritesAtCoveredOffset,
            SplitStep::CommitMapVersion,
            SplitStep::StartSuccessorRestore,
            SplitStep::SuccessorFencePrologue,
            SplitStep::InheritInDoubtMarkers,
            SplitStep::ParkPredecessor,
            SplitStep::UnpauseServing,
        ];

        for step in steps {
            let store = Arc::new(MemoryStore::default());
            let first_hooks = TestHooks::new(markers());
            first_hooks.fail_before(step);
            let first = run_split("split-1", split_command(), store.as_ref(), &first_hooks).await;
            assert!(first.is_err());

            let retry_hooks = TestHooks::new(markers());
            let state = run_split("split-1", split_command(), store.as_ref(), &retry_hooks)
                .await
                .expect("retry");

            assert!(state.next_step == SplitStep::Complete);
            assert!(state.target_map.epoch() == MapEpoch::new(8));
        }
    }

    #[tokio::test]
    async fn move_uses_same_orchestration_as_degenerate_split() {
        let store = MemoryStore::default();
        let hooks = TestHooks::new(markers());

        let state = run_move(
            "move-1",
            MoveRangeCommand {
                current_map: range_map(),
                range_id: RangeId::new(1),
                successor_generation: 9,
            },
            &store,
            &hooks,
        )
        .await
        .expect("move");

        assert!(state.operation == SplitOperation::Move);
        assert!(state.predecessor_after == state.successor_after);
        assert!(state.successor_generation == Some(9));
        assert!(hooks.events() == expected_split_events(RangeId::new(4)));
    }

    #[tokio::test]
    async fn merge_checkpoints_both_adjacent_ranges_and_parks_both() {
        let store = MemoryStore::default();
        let hooks = TestHooks::new(markers());

        let state = run_merge(
            "merge-1",
            MergeRangeCommand {
                current_map: merge_map(),
                left: RangeId::new(1),
                right: RangeId::new(2),
                successor_generation: 11,
            },
            &store,
            &hooks,
        )
        .await
        .expect("merge");

        assert!(state.operation == SplitOperation::Merge);
        assert!(state.predecessor == RangeId::new(1));
        assert!(state.successor == RangeId::new(1));
        assert!(state.successor_generation == Some(11));
        assert!(state.successor_after.start == RangeKey::table_start(TableId::new(10)));
        assert!(state.successor_after.end == Some(RangeKey::table_start(TableId::new(30))));
        assert!(state.target_map.ranges().len() == 3);
        assert!(state.inherited_markers == markers());
        assert!(hooks.events() == expected_merge_events());
    }

    #[tokio::test]
    async fn merge_retry_is_idempotent_after_each_step() {
        let steps = [
            SplitStep::ForcePredecessorCheckpoint,
            SplitStep::ForceRightPredecessorCheckpoint,
            SplitStep::PauseWritesAtCoveredOffset,
            SplitStep::CommitMapVersion,
            SplitStep::StartSuccessorRestore,
            SplitStep::SuccessorFencePrologue,
            SplitStep::InheritInDoubtMarkers,
            SplitStep::ParkPredecessor,
            SplitStep::ParkRightPredecessor,
            SplitStep::UnpauseServing,
        ];

        for step in steps {
            let store = Arc::new(MemoryStore::default());
            let first_hooks = TestHooks::new(markers());
            first_hooks.fail_before(step);
            let first = run_merge("merge-1", merge_command(), store.as_ref(), &first_hooks).await;
            assert!(first.is_err());

            let retry_hooks = TestHooks::new(markers());
            let state = run_merge("merge-1", merge_command(), store.as_ref(), &retry_hooks)
                .await
                .expect("retry");

            assert!(state.next_step == SplitStep::Complete);
            assert!(state.target_map.epoch() == MapEpoch::new(8));
        }
    }

    #[tokio::test]
    async fn conversion_runs_pause_checkpoint_catalog_flip_and_resume() {
        let store = MemoryStore::default();
        let hooks = TestHooks::new(Vec::new());

        let state = run_conversion("convert-1", conversion_command(), &store, &hooks)
            .await
            .expect("conversion");

        assert!(state.operation == SplitOperation::ConvertTable);
        assert!(state.conversion_table == Some(TableId::new(15)));
        assert!(state.next_step == SplitStep::Complete);
        assert!(state.target_map.epoch() == MapEpoch::new(8));
        assert!(hooks.events() == expected_conversion_events());
    }

    #[tokio::test]
    async fn conversion_rejects_in_doubt_prepared_marker() {
        let store = MemoryStore::default();
        let hooks = TestHooks::new(vec![InDoubtMarker {
            transaction_id: 99,
            key: RangeKey::new(TableId::new(15), 1),
        }]);

        let err = run_conversion("convert-1", conversion_command(), &store, &hooks)
            .await
            .expect_err("prepared marker rejects conversion");

        assert!(matches!(
            err,
            SplitError::ConversionInDoubtPrepared {
                table_id,
                transaction_id: 99,
            } if table_id == TableId::new(15)
        ));
    }

    #[tokio::test]
    async fn conversion_retry_is_idempotent_after_each_crash_point() {
        let steps = [
            SplitStep::PauseConversionWrites,
            SplitStep::ForcePredecessorCheckpoint,
            SplitStep::CommitMapVersion,
            SplitStep::SuccessorFencePrologue,
            SplitStep::InheritInDoubtMarkers,
            SplitStep::UnpauseServing,
        ];

        for step in steps {
            let store = Arc::new(MemoryStore::default());
            let first_hooks = TestHooks::new(Vec::new());
            first_hooks.fail_before(step);
            let first = run_conversion(
                "convert-1",
                conversion_command(),
                store.as_ref(),
                &first_hooks,
            )
            .await;
            assert!(first.is_err());

            let retry_hooks = TestHooks::new(Vec::new());
            let state = run_conversion(
                "convert-1",
                conversion_command(),
                store.as_ref(),
                &retry_hooks,
            )
            .await
            .expect("retry");

            assert!(state.next_step == SplitStep::Complete);
            assert!(state.target_map.epoch() == MapEpoch::new(8));
        }
    }

    #[test]
    fn merge_rejects_non_adjacent_ranges() {
        let result = SplitState::for_merge(
            "merge-1",
            MergeRangeCommand {
                current_map: merge_map(),
                left: RangeId::new(1),
                right: RangeId::new(3),
                successor_generation: 1,
            },
        );

        assert!(matches!(result, Err(SplitError::Map(_))));
    }

    #[test]
    fn ownership_invariant_is_never_neither_or_both_for_one_key() {
        let initial = range_map();
        let state = SplitState::for_split("split-1", split_command()).expect("state");
        let left_key = RangeKey::new(TableId::new(15), 0);
        let right_key = RangeKey::new(TableId::new(25), 0);

        assert!(owners(&initial, left_key) == vec![RangeId::new(1)]);
        assert!(owners(&initial, right_key) == vec![RangeId::new(1)]);
        assert!(owners(&state.target_map, left_key) == vec![RangeId::new(4)]);
        assert!(owners(&state.target_map, right_key) == vec![RangeId::new(2)]);
    }

    #[tokio::test]
    async fn decisions_and_in_doubt_markers_are_inherited_by_interval() {
        let store = MemoryStore::default();
        let hooks = TestHooks::new(markers());

        let state = run_split("split-1", split_command(), &store, &hooks)
            .await
            .expect("split");

        assert!(state.inherited_markers == vec![markers()[1].clone()]);
    }

    fn tenant() -> TenantName {
        TenantName::parse("tenant-a").expect("tenant")
    }

    fn range_map() -> RangeMap {
        RangeMap::new(
            tenant(),
            MapEpoch::new(7),
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(10), Some(TableId::new(30))),
                RangeSpec::new(RangeId::new(3), TableId::new(30), None),
            ],
        )
        .expect("map")
    }

    fn split_command() -> SplitCommand {
        SplitCommand {
            current_map: range_map(),
            predecessor: RangeId::new(1),
            successor: RangeId::new(2),
            split_at: RangeKey::table_start(TableId::new(20)),
        }
    }

    #[test]
    fn split_keeps_exactly_one_owner_after_predecessor_is_parked() {
        let state = SplitState::for_split("split-owners", split_command()).expect("split state");

        for key in [
            RangeKey::table_start(TableId::new(10)),
            RangeKey::table_start(TableId::new(19)),
            RangeKey::table_start(TableId::new(20)),
            RangeKey::table_start(TableId::new(29)),
        ] {
            let owners = state
                .target_map
                .ranges()
                .iter()
                .filter(|range| range.range_id != state.predecessor && range.contains_key(key))
                .count();
            assert_eq!(owners, 1, "key {key:?} must have one serving owner after park");
        }
    }

    fn merge_command() -> MergeRangeCommand {
        MergeRangeCommand {
            current_map: merge_map(),
            left: RangeId::new(1),
            right: RangeId::new(2),
            successor_generation: 11,
        }
    }

    fn conversion_command() -> ConvertTableCommand {
        ConvertTableCommand {
            current_map: range_map(),
            range_id: RangeId::new(1),
            table_id: TableId::new(15),
            successor_generation: 12,
        }
    }

    fn merge_map() -> RangeMap {
        RangeMap::new(
            tenant(),
            MapEpoch::new(7),
            vec![
                RangeSpec::new(RangeId::COORDINATOR, TableId::ZERO, Some(TableId::new(10))),
                RangeSpec::new(RangeId::new(1), TableId::new(10), Some(TableId::new(20))),
                RangeSpec::new(RangeId::new(2), TableId::new(20), Some(TableId::new(30))),
                RangeSpec::new(RangeId::new(3), TableId::new(30), None),
            ],
        )
        .expect("merge map")
    }

    fn markers() -> Vec<InDoubtMarker> {
        vec![
            InDoubtMarker {
                transaction_id: 1,
                key: RangeKey::new(TableId::new(15), 0),
            },
            InDoubtMarker {
                transaction_id: 2,
                key: RangeKey::new(TableId::new(25), 0),
            },
        ]
    }

    fn expected_split_events(successor: RangeId) -> Vec<Event> {
        vec![
            Event::Checkpoint(MapEpoch::new(7)),
            Event::Pause(42),
            Event::Commit(MapEpoch::new(8)),
            Event::Restore(42),
            Event::Prologue(successor),
            Event::Inherit(successor),
            Event::Park(RangeId::new(1)),
            Event::Unpause,
        ]
    }

    fn expected_merge_events() -> Vec<Event> {
        vec![
            Event::Checkpoint(MapEpoch::new(7)),
            Event::Checkpoint(MapEpoch::new(7)),
            Event::Pause(42),
            Event::Commit(MapEpoch::new(8)),
            Event::Restore(126),
            Event::Prologue(RangeId::new(1)),
            Event::Inherit(RangeId::new(1)),
            Event::Park(RangeId::new(1)),
            Event::Park(RangeId::new(2)),
            Event::Unpause,
        ]
    }

    fn expected_conversion_events() -> Vec<Event> {
        vec![
            Event::PauseConversion,
            Event::Checkpoint(MapEpoch::new(7)),
            Event::Commit(MapEpoch::new(8)),
            Event::Prologue(RangeId::new(1)),
            Event::Inherit(RangeId::new(1)),
            Event::Unpause,
        ]
    }

    fn owners(range_map: &RangeMap, key: RangeKey) -> Vec<RangeId> {
        range_map
            .ranges()
            .iter()
            .filter(|range| range.contains_key(key))
            .map(|range| range.range_id)
            .collect()
    }
}
