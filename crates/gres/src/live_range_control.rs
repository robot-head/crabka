//! Production execution of authenticated, generation-fenced range-control steps.

use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use crabka_gres_ranges::{
    CheckpointManifest, RangeId, RangeTransferBarrier, RangeTransferCapability, SplitCommand,
    SplitOperation, TableId, ValidatedSplitTransferPlan,
    control::RangeControlExecutor,
    transport::{RangeControlOperation, RangeControlReq, RangeControlResp},
};

use super::{LiveMultiRangeTransfer, committed_tail_sha256};

pub(super) struct LiveRangeControlReceiptStore {
    tenant: String,
    transfer: Weak<LiveMultiRangeTransfer>,
}

impl LiveRangeControlReceiptStore {
    pub(super) fn new(tenant: impl Into<String>, transfer: &Arc<LiveMultiRangeTransfer>) -> Self {
        Self {
            tenant: tenant.into(),
            transfer: Arc::downgrade(transfer),
        }
    }
}

#[async_trait]
impl crabka_gres_ranges::control::RangeControlReceiptStore for LiveRangeControlReceiptStore {
    async fn load(
        &self,
        key: &str,
    ) -> Result<Option<crabka_gres_ranges::control::RangeControlReceipt>, String> {
        self.transfer
            .upgrade()
            .ok_or_else(|| "range-control runtime stopped".to_owned())?
            .current_range_zero_engine()
            .map_err(|error| error.to_string())?
            .range_control_receipt(&self.tenant, key)
            .map_err(|error| format!("{error:?}"))?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .transpose()
    }

    async fn list(&self) -> Result<Vec<crabka_gres_ranges::control::RangeControlReceipt>, String> {
        self.transfer
            .upgrade()
            .ok_or_else(|| "range-control runtime stopped".to_owned())?
            .current_range_zero_engine()
            .map_err(|error| error.to_string())?
            .range_control_receipts(&self.tenant)
            .map_err(|error| format!("{error:?}"))?
            .into_iter()
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .collect()
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_revision: Option<u64>,
        receipt: crabka_gres_ranges::control::RangeControlReceipt,
    ) -> Result<bool, String> {
        let transfer = self
            .transfer
            .upgrade()
            .ok_or_else(|| "range-control runtime stopped".to_owned())?;
        let engine = transfer
            .current_range_zero_engine()
            .map_err(|error| error.to_string())?;
        let current = engine
            .range_control_receipt(&self.tenant, key)
            .map_err(|error| format!("{error:?}"))?;
        let current_revision = current
            .as_deref()
            .map(serde_json::from_slice::<crabka_gres_ranges::control::RangeControlReceipt>)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(|receipt| receipt.revision);
        let crossed_topology = current_revision.is_none() && expected_revision.is_some();
        if current_revision != expected_revision && !crossed_topology {
            return Ok(false);
        }
        let value = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
        match engine
            .compare_and_swap_range_control_receipt(
                &self.tenant,
                key,
                current.clone(),
                value.clone(),
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(_) => transfer
                .compare_and_swap_paused_control_receipt(
                    &self.tenant,
                    key,
                    current.clone(),
                    value.clone(),
                )
                .await
                .map_err(|error| error.to_string()),
        }
    }
}

pub(super) const fn recovery_step_rank(operation: &RangeControlOperation) -> u8 {
    match operation {
        RangeControlOperation::ForceCheckpoint => 0,
        RangeControlOperation::PauseAtCoveredOffset { .. } => 1,
        RangeControlOperation::StageFilteredRestore { .. } => 2,
        RangeControlOperation::InheritMarkers { .. } => 3,
        RangeControlOperation::SuccessorFencePrologue { .. } => 4,
        RangeControlOperation::RetirePredecessor => 5,
        RangeControlOperation::Resume => 6,
        RangeControlOperation::Status => 7,
    }
}

pub(super) const fn requires_startup_reconcile(operation: &RangeControlOperation) -> bool {
    matches!(
        operation,
        RangeControlOperation::PauseAtCoveredOffset { .. }
            | RangeControlOperation::StageFilteredRestore { .. }
            | RangeControlOperation::InheritMarkers { .. }
            | RangeControlOperation::SuccessorFencePrologue { .. }
            | RangeControlOperation::RetirePredecessor
    )
}

#[derive(Default)]
struct OperationRuntime {
    checkpoint: Option<CheckpointManifest>,
    barrier: Option<RangeTransferBarrier>,
    staged: Option<crabka_gres_ranges::StagedRangeSuccessors>,
    claimed: Option<crabka_gres_ranges::ClaimedStagedSuccessors>,
    split: Option<crabka_gres_ranges::SplitState>,
    tail_sha256: Option<String>,
    published: bool,
    resumed: bool,
}

/// Compute-local executor behind the mTLS range-control service.
pub(super) struct LiveRangeControlExecutor {
    transfer: Weak<LiveMultiRangeTransfer>,
    gateway: crabka_gres_ranges::MultiRangeTenant,
    operations: tokio::sync::Mutex<BTreeMap<String, OperationRuntime>>,
}

impl LiveRangeControlExecutor {
    pub(super) fn new(
        transfer: &Arc<LiveMultiRangeTransfer>,
        gateway: crabka_gres_ranges::MultiRangeTenant,
    ) -> Self {
        Self {
            transfer: Arc::downgrade(transfer),
            gateway,
            operations: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn transfer(&self) -> Result<Arc<LiveMultiRangeTransfer>, RangeControlResp> {
        self.transfer.upgrade().ok_or_else(|| {
            rejected(
                "runtime_stopped",
                "range-control runtime is no longer available",
            )
        })
    }

    async fn status(&self, operation_id: &str, range_id: RangeId) -> RangeControlResp {
        let operations = self.operations.lock().await;
        let runtime = operations.get(operation_id);
        RangeControlResp::Status {
            paused: runtime
                .and_then(|state| state.barrier)
                .is_some_and(|_| !runtime.is_some_and(|state| state.resumed)),
            serving: self.gateway.control_range_is_hosted(range_id),
            barrier_offset: runtime.and_then(|state| state.barrier.map(|barrier| barrier.offset)),
        }
    }

    async fn apply(&self, request: &RangeControlReq) -> Result<RangeControlResp, RangeControlResp> {
        let transfer = self.transfer()?;
        match &request.operation {
            RangeControlOperation::ForceCheckpoint => {
                let checkpoint = transfer
                    .force_checkpoint(request.range_id)
                    .await
                    .map_err(transfer_error)?;
                self.operations
                    .lock()
                    .await
                    .entry(request.operation_id.clone())
                    .or_default()
                    .checkpoint = Some(checkpoint.clone());
                Ok(RangeControlResp::Checkpoint {
                    generation: request.generation,
                    covered_offset: checkpoint.covered_offset,
                    manifest_key: checkpoint.manifest_key,
                })
            }
            RangeControlOperation::PauseAtCoveredOffset {
                manifest_key,
                covered_offset,
            } => {
                if let Some(barrier) = self
                    .operations
                    .lock()
                    .await
                    .get(&request.operation_id)
                    .and_then(|runtime| runtime.barrier)
                {
                    return Ok(RangeControlResp::Paused {
                        barrier_offset: barrier.offset,
                    });
                }
                let checkpoint = CheckpointManifest {
                    range_id: request.range_id,
                    covered_offset: *covered_offset,
                    manifest_key: manifest_key.clone(),
                };
                let barrier = transfer
                    .pause_at_checkpoint(&checkpoint)
                    .await
                    .map_err(transfer_error)?;
                let mut operations = self.operations.lock().await;
                let runtime = operations.entry(request.operation_id.clone()).or_default();
                runtime.checkpoint = Some(checkpoint);
                runtime.barrier = Some(barrier);
                Ok(RangeControlResp::Paused {
                    barrier_offset: barrier.offset,
                })
            }
            RangeControlOperation::Status => {
                Ok(self.status(&request.operation_id, request.range_id).await)
            }
            RangeControlOperation::StageFilteredRestore {
                split,
                source_range,
                source_generation,
                manifest_key,
                covered_offset,
                barrier_offset,
                routing_table_id,
                physical_table_id,
                interval_start,
                interval_end,
                target_map,
                ..
            } => {
                validate_stage_request(
                    request,
                    split,
                    *source_range,
                    *source_generation,
                    *interval_start,
                    *interval_end,
                    target_map,
                )?;
                if let Some(tail_sha256) = self
                    .operations
                    .lock()
                    .await
                    .get(&request.operation_id)
                    .and_then(|runtime| runtime.tail_sha256.clone())
                {
                    return Ok(RangeControlResp::Staged { tail_sha256 });
                }
                let checkpoint = CheckpointManifest {
                    range_id: *source_range,
                    covered_offset: *covered_offset,
                    manifest_key: manifest_key.clone(),
                };
                let requested_barrier = RangeTransferBarrier {
                    range_id: *source_range,
                    offset: *barrier_offset,
                };
                let barrier = self
                    .operations
                    .lock()
                    .await
                    .get(&request.operation_id)
                    .and_then(|runtime| runtime.barrier)
                    .unwrap_or(requested_barrier);
                if barrier.range_id != requested_barrier.range_id
                    || barrier.offset < requested_barrier.offset
                {
                    return Err(rejected(
                        "stale_pause",
                        "staged restore cannot use a stale or foreign pause barrier",
                    ));
                }
                let plan = ValidatedSplitTransferPlan::from_control(
                    (**split).clone(),
                    BTreeMap::from([(
                        TableId::new(u64::from(*physical_table_id)),
                        TableId::new(*routing_table_id),
                    )]),
                )
                .map_err(|error| rejected("invalid_split", error.to_string()))?;
                let tail = transfer
                    .read_committed_tail(checkpoint.range_id, checkpoint.covered_offset, barrier)
                    .await
                    .map_err(transfer_error)?;
                let tail_sha256 = committed_tail_sha256(&tail);
                let staged = transfer
                    .stage_successors(&plan, &checkpoint, &tail, barrier)
                    .await
                    .map_err(transfer_error)?;
                let mut operations = self.operations.lock().await;
                let runtime = operations.entry(request.operation_id.clone()).or_default();
                runtime.checkpoint = Some(checkpoint);
                runtime.barrier = Some(barrier);
                runtime.staged = Some(staged);
                runtime.split = Some((**split).clone());
                runtime.tail_sha256 = Some(tail_sha256.clone());
                Ok(RangeControlResp::Staged { tail_sha256 })
            }
            RangeControlOperation::SuccessorFencePrologue { split: requested } => {
                if request.operation_id != requested.operation_id
                    || request.range_id != requested.predecessor
                    || request.generation != requested.predecessor_generation
                {
                    return Err(rejected(
                        "invalid_split",
                        "prologue request identity differs from split intent",
                    ));
                }
                if self.gateway.control_range_map() == requested.target_map {
                    transfer.note_activation_irreversible(&request.operation_id);
                    return Ok(RangeControlResp::AlreadyApplied);
                }
                let (split, claimed) = {
                    let mut operations = self.operations.lock().await;
                    let runtime = operations
                        .get_mut(&request.operation_id)
                        .ok_or_else(|| rejected("missing_stage", "successors are not staged"))?;
                    if runtime.published {
                        return Ok(RangeControlResp::AlreadyApplied);
                    }
                    (
                        runtime
                            .split
                            .clone()
                            .filter(|split| split == requested.as_ref())
                            .ok_or_else(|| {
                                rejected("missing_stage", "split intent is not staged")
                            })?,
                        runtime.claimed.take().ok_or_else(|| {
                            rejected("missing_stage", "claimed successors are unavailable")
                        })?,
                    )
                };
                self.gateway
                    .publish_control_successors_with_transfer(
                        request.operation_id.clone(),
                        split_command(&split)?,
                        claimed,
                        transfer.as_ref(),
                    )
                    .await
                    .map_err(|error| rejected("publication_failed", error.to_string()))?;
                self.operations
                    .lock()
                    .await
                    .get_mut(&request.operation_id)
                    .expect("operation remains present")
                    .published = true;
                Ok(RangeControlResp::Applied)
            }
            RangeControlOperation::InheritMarkers {
                interval_start,
                interval_end,
            } => {
                let start = wire_key(*interval_start);
                let end = interval_end.map(wire_key);
                let source = self
                    .gateway
                    .control_in_doubt_markers(start, end)
                    .map_err(|error| rejected("marker_source_failed", error.to_string()))?;
                let mut operations = self.operations.lock().await;
                let runtime = operations
                    .get_mut(&request.operation_id)
                    .ok_or_else(|| rejected("missing_stage", "successors are not staged"))?;
                let split = runtime
                    .split
                    .as_ref()
                    .ok_or_else(|| rejected("missing_stage", "split intent is not staged"))?;
                if start != split.predecessor_before.start || end != split.predecessor_before.end {
                    return Err(rejected(
                        "invalid_interval",
                        "marker request must cover the exact predecessor interval",
                    ));
                }
                let (left, right) = if let Some(claimed) = runtime.claimed.as_ref() {
                    (
                        crabka_gres_ranges::tenant::in_doubt_markers_for_engine(
                            &claimed.left.engine,
                            split.predecessor_after.start,
                            split.predecessor_after.end,
                        )
                        .map_err(|error| rejected("marker_verify_failed", error.to_string()))?,
                        crabka_gres_ranges::tenant::in_doubt_markers_for_engine(
                            &claimed.right.engine,
                            split.successor_after.start,
                            split.successor_after.end,
                        )
                        .map_err(|error| rejected("marker_verify_failed", error.to_string()))?,
                    )
                } else {
                    let staged = runtime.staged.as_ref().ok_or_else(|| {
                        rejected("missing_stage", "successor resources are not staged")
                    })?;
                    (
                        transfer
                            .staged_successor_markers(
                                staged.left.range_id,
                                split.predecessor_after.start,
                                split.predecessor_after.end,
                            )
                            .map_err(transfer_error)?,
                        transfer
                            .staged_successor_markers(
                                staged.right.range_id,
                                split.successor_after.start,
                                split.successor_after.end,
                            )
                            .map_err(transfer_error)?,
                    )
                };
                verify_marker_partition(
                    &source,
                    &left,
                    &right,
                    &split.predecessor_after,
                    &split.successor_after,
                )?;
                if runtime.claimed.is_none() {
                    let claimed = transfer
                        .claim_successors(
                            runtime.staged.as_ref().expect("staged checked above"),
                            runtime.barrier.ok_or_else(|| {
                                rejected("missing_pause", "operation has no pause barrier")
                            })?,
                        )
                        .await
                        .map_err(transfer_error)?;
                    runtime.claimed = Some(claimed);
                }
                let markers = source.iter().map(wire_marker).collect::<Vec<_>>();
                Ok(RangeControlResp::Markers {
                    digest: marker_digest(&markers),
                    markers,
                })
            }
            RangeControlOperation::RetirePredecessor => {
                transfer
                    .retire_predecessor(
                        &request.operation_id,
                        request.range_id,
                        request.generation,
                        &self.gateway.control_range_map(),
                    )
                    .await
                    .map_err(transfer_error)?;
                Ok(RangeControlResp::Applied)
            }
            RangeControlOperation::Resume => {
                if transfer.activation_is_irreversible(&request.operation_id) {
                    return Err(rejected(
                        "activation_irreversible",
                        "predecessor cannot resume after successor publication",
                    ));
                }
                let barrier = self
                    .operations
                    .lock()
                    .await
                    .get(&request.operation_id)
                    .and_then(|runtime| runtime.barrier)
                    .ok_or_else(|| rejected("missing_pause", "operation has no pause barrier"))?;
                transfer.resume(barrier).await.map_err(transfer_error)?;
                self.operations
                    .lock()
                    .await
                    .get_mut(&request.operation_id)
                    .expect("operation remains present")
                    .resumed = true;
                Ok(RangeControlResp::Applied)
            }
        }
    }
}

#[async_trait]
impl RangeControlExecutor for LiveRangeControlExecutor {
    async fn execute(&self, request: &RangeControlReq) -> RangeControlResp {
        self.apply(request)
            .await
            .unwrap_or_else(|response| response)
    }

    async fn reconcile(&self, request: &RangeControlReq) -> RangeControlResp {
        match request.operation {
            RangeControlOperation::Status => {
                self.status(&request.operation_id, request.range_id).await
            }
            _ => self
                .apply(request)
                .await
                .unwrap_or_else(|response| response),
        }
    }

    async fn reconcile_completed(
        &self,
        request: &RangeControlReq,
        previous: &RangeControlResp,
    ) -> RangeControlResp {
        if let (
            RangeControlOperation::PauseAtCoveredOffset { .. },
            RangeControlResp::Paused {
                barrier_offset: old_barrier,
            },
        ) = (&request.operation, previous)
        {
            let current = self
                .apply(request)
                .await
                .unwrap_or_else(|response| response);
            let RangeControlResp::Paused {
                barrier_offset: new_barrier,
            } = current
            else {
                return current;
            };
            if new_barrier < *old_barrier {
                return rejected(
                    "unsafe_recovery_tail",
                    "replacement pause barrier moved backwards",
                );
            }
            if new_barrier == *old_barrier {
                return RangeControlResp::Paused {
                    barrier_offset: new_barrier,
                };
            }
            let transfer = match self.transfer() {
                Ok(transfer) => transfer,
                Err(response) => return response,
            };
            let tail = match transfer
                .read_committed_tail(
                    request.range_id,
                    *old_barrier,
                    RangeTransferBarrier {
                        range_id: request.range_id,
                        offset: new_barrier,
                    },
                )
                .await
            {
                Ok(tail) => tail,
                Err(error) => return transfer_error(error),
            };
            if let Err(response) =
                recovery_extension_is_structural(&request.tenant, *old_barrier, new_barrier, &tail)
            {
                return response;
            }
            return RangeControlResp::Paused {
                barrier_offset: new_barrier,
            };
        }
        self.reconcile(request).await
    }
}

fn split_command(split: &crabka_gres_ranges::SplitState) -> Result<SplitCommand, RangeControlResp> {
    let right = split.right.clone().ok_or_else(|| {
        rejected(
            "invalid_split",
            "two-successor split is missing its right side",
        )
    })?;
    Ok(SplitCommand {
        current_map: split.current_map.clone(),
        predecessor: split.predecessor,
        predecessor_generation: split.predecessor_generation,
        left: split.left.clone(),
        right,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_stage_request(
    request: &RangeControlReq,
    split: &crabka_gres_ranges::SplitState,
    source_range: RangeId,
    source_generation: u64,
    interval_start: crabka_gres_ranges::transport::WireRangeKey,
    interval_end: Option<crabka_gres_ranges::transport::WireRangeKey>,
    target_map: &crabka_gres_ranges::RangeMap,
) -> Result<(), RangeControlResp> {
    let identity_matches = split.operation == SplitOperation::Split
        && request.operation_id == split.operation_id
        && request.range_id == source_range
        && split.predecessor == source_range
        && request.generation == source_generation
        && split.predecessor_generation == source_generation;
    let evidence_matches = *target_map == split.target_map
        && wire_key(interval_start) == split.predecessor_before.start
        && interval_end.map(wire_key) == split.predecessor_before.end
        && split.predecessor_before.range_id == source_range;
    if !identity_matches || !evidence_matches {
        return Err(rejected(
            "invalid_split",
            "stage owner, generation, operation, map, or interval differs from split intent",
        ));
    }
    Ok(())
}

fn transfer_error(error: crabka_gres_ranges::RangeTransferError) -> RangeControlResp {
    rejected("transfer_failed", error.to_string())
}

fn wire_key(key: crabka_gres_ranges::transport::WireRangeKey) -> crabka_gres_ranges::RangeKey {
    crabka_gres_ranges::RangeKey::new(TableId::new(key.table_id), key.rowid)
}

fn wire_marker(
    marker: &crabka_gres_ranges::InDoubtMarker,
) -> crabka_gres_ranges::transport::WireInDoubtMarker {
    crabka_gres_ranges::transport::WireInDoubtMarker {
        transaction_id: marker.transaction_id,
        key: crabka_gres_ranges::transport::WireRangeKey {
            table_id: marker.key.table_id.as_u64(),
            rowid: marker.key.rowid,
        },
    }
}

fn verify_marker_partition(
    source: &[crabka_gres_ranges::InDoubtMarker],
    left: &[crabka_gres_ranges::InDoubtMarker],
    right: &[crabka_gres_ranges::InDoubtMarker],
    left_interval: &crabka_gres_ranges::RangeSpec,
    right_interval: &crabka_gres_ranges::RangeSpec,
) -> Result<(), RangeControlResp> {
    if left
        .iter()
        .any(|marker| !left_interval.contains_key(marker.key))
        || right
            .iter()
            .any(|marker| !right_interval.contains_key(marker.key))
    {
        return Err(rejected(
            "marker_wrong_owner",
            "successor contains a marker outside its owned interval",
        ));
    }
    if left.iter().any(|marker| right.contains(marker)) {
        return Err(rejected(
            "marker_wrong_owner",
            "successor marker sets overlap",
        ));
    }
    let mut union = left.iter().chain(right).cloned().collect::<Vec<_>>();
    union.sort_unstable_by_key(|marker| (marker.transaction_id, marker.key));
    union.dedup();
    if union != source {
        return Err(rejected(
            "marker_mismatch",
            "successor marker union differs from source",
        ));
    }
    Ok(())
}

fn marker_digest(markers: &[crabka_gres_ranges::transport::WireInDoubtMarker]) -> String {
    use sha2::{Digest as _, Sha256};
    let mut digest = Sha256::new();
    for marker in markers {
        digest.update(marker.transaction_id.to_be_bytes());
        digest.update(marker.key.table_id.to_be_bytes());
        digest.update(marker.key.rowid.to_be_bytes());
    }
    use std::fmt::Write as _;
    digest
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            write!(&mut encoded, "{byte:02x}").expect("write to string");
            encoded
        })
}

fn recovery_extension_is_structural(
    tenant: &str,
    old_barrier: i64,
    new_barrier: i64,
    records: &[crabka_gres_ranges::CommittedTailRecord],
) -> Result<(), RangeControlResp> {
    if new_barrier < old_barrier {
        return Err(rejected(
            "unsafe_recovery_tail",
            "replacement pause barrier moved backwards",
        ));
    }
    if new_barrier == old_barrier {
        return if records.is_empty() {
            Ok(())
        } else {
            Err(rejected(
                "unsafe_recovery_tail",
                "unchanged pause barrier has an unexpected tail",
            ))
        };
    }
    let control_prefix = crabka_pgkv::key::range_control_receipt_prefix(tenant);
    let activation_prefix = crabka_pgkv::key::topology_activation_receipt_prefix(tenant);
    let mut previous_offset = old_barrier;
    for record in records {
        if record.offset <= previous_offset || record.offset > new_barrier {
            return Err(rejected(
                "unsafe_recovery_tail",
                "replacement pause tail is out of order or outside the widened interval",
            ));
        }
        let frame = crabka_gres_substrate::WalFrame::decode(&record.bytes).map_err(|error| {
            rejected(
                "unsafe_recovery_tail",
                format!("decode recovery tail: {error}"),
            )
        })?;
        let structural = frame.ops.iter().all(|op| {
            let key = match op {
                crabka_pgkv::WriteOp::Put { key, .. }
                | crabka_pgkv::WriteOp::ConditionalPut { key, .. } => key,
                crabka_pgkv::WriteOp::Delete { .. } => return false,
            };
            key.starts_with(&control_prefix) || key.starts_with(&activation_prefix)
        });
        if !structural {
            return Err(rejected(
                "unsafe_recovery_tail",
                "replacement pause extension contains a non-structural write",
            ));
        }
        previous_offset = record.offset;
    }
    if previous_offset != new_barrier {
        return Err(rejected(
            "unsafe_recovery_tail",
            "replacement pause tail does not reach the new barrier",
        ));
    }
    Ok(())
}

fn rejected(code: &str, message: impl Into<String>) -> RangeControlResp {
    RangeControlResp::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use crabka_gres_ranges::{
        InDoubtMarker, MapEpoch, RangeId, RangeKey, RangeMap, RangeSpec, SplitCommand,
        SuccessorDescriptor, TableId, TenantName,
        transport::{RangeControlOperation, RangeControlReq, RangeControlResp, WireRangeKey},
    };
    use crabka_pgkv::WriteOp;

    use super::{
        marker_digest, recovery_extension_is_structural, validate_stage_request,
        verify_marker_partition, wire_marker,
    };

    fn tail_record(offset: i64, ops: Vec<WriteOp>) -> crabka_gres_ranges::CommittedTailRecord {
        crabka_gres_ranges::CommittedTailRecord {
            offset,
            bytes: crabka_gres_substrate::WalFrame {
                journal_seq: u64::try_from(offset).unwrap(),
                ops,
            }
            .encode(),
        }
    }

    #[test]
    fn restart_extension_accepts_only_barriers_and_control_activation_receipts() {
        let tenant = "tenant-a";
        let records = vec![
            tail_record(11, vec![]),
            tail_record(
                12,
                vec![WriteOp::ConditionalPut {
                    key: crabka_pgkv::key::range_control_receipt_key(tenant, "split-42/pause"),
                    expected: None,
                    value: b"receipt".to_vec(),
                }],
            ),
            tail_record(
                13,
                vec![WriteOp::Put {
                    key: crabka_pgkv::key::topology_activation_receipt_key(tenant, "split-42"),
                    value: b"activation".to_vec(),
                }],
            ),
        ];
        assert!(recovery_extension_is_structural(tenant, 10, 13, &records).is_ok());
    }

    #[test]
    fn restart_extension_rejects_an_injected_user_row() {
        let records = vec![tail_record(
            11,
            vec![WriteOp::Put {
                key: crabka_pgkv::key::row_key(7, 99),
                value: b"user-data".to_vec(),
            }],
        )];
        let error = recovery_extension_is_structural("tenant-a", 10, 11, &records)
            .expect_err("user writes must fail readiness");
        assert!(
            matches!(error, RangeControlResp::Rejected { ref code, .. } if code == "unsafe_recovery_tail")
        );
    }

    #[test]
    fn restart_extension_rejects_a_mixed_structural_and_user_batch() {
        let tenant = "tenant-a";
        let records = vec![tail_record(
            11,
            vec![
                WriteOp::ConditionalPut {
                    key: crabka_pgkv::key::range_control_receipt_key(tenant, "split-42/pause"),
                    expected: None,
                    value: b"receipt".to_vec(),
                },
                WriteOp::Put {
                    key: crabka_pgkv::key::row_key(7, 99),
                    value: b"user-data".to_vec(),
                },
            ],
        )];
        assert!(matches!(
            recovery_extension_is_structural(tenant, 10, 11, &records),
            Err(RangeControlResp::Rejected { ref code, .. }) if code == "unsafe_recovery_tail"
        ));
    }

    #[test]
    fn restart_extension_requires_the_terminal_barrier_record() {
        let records = vec![tail_record(11, vec![])];
        let error = recovery_extension_is_structural("tenant-a", 10, 12, &records)
            .expect_err("a missing terminal barrier must fail readiness");
        assert!(
            matches!(error, RangeControlResp::Rejected { ref code, .. } if code == "unsafe_recovery_tail")
        );
    }

    fn marker(transaction_id: u64, rowid: u64) -> InDoubtMarker {
        InDoubtMarker {
            transaction_id,
            key: RangeKey::new(TableId::new(7), rowid),
        }
    }

    fn split_fixture() -> crabka_gres_ranges::SplitState {
        let current_map = RangeMap::new(
            TenantName::parse("tenant-a").unwrap(),
            MapEpoch::ZERO,
            vec![RangeSpec::for_interval(
                RangeId::COORDINATOR,
                RangeKey::MIN,
                None,
            )],
        )
        .unwrap();
        let split_at = RangeKey::new(TableId::new(7), 10);
        crabka_gres_ranges::SplitState::for_split(
            "split-42",
            SplitCommand {
                current_map,
                predecessor: RangeId::COORDINATOR,
                predecessor_generation: 8,
                left: SuccessorDescriptor {
                    range_id: RangeId::COORDINATOR,
                    endpoint: "left:7443".into(),
                    wal_generation: 9,
                    interval: RangeSpec::for_interval(
                        RangeId::COORDINATOR,
                        RangeKey::MIN,
                        Some(split_at),
                    ),
                },
                right: SuccessorDescriptor {
                    range_id: RangeId::new(1),
                    endpoint: "right:7443".into(),
                    wal_generation: 9,
                    interval: RangeSpec::for_interval(RangeId::new(1), split_at, None),
                },
            },
        )
        .unwrap()
    }

    #[test]
    fn stage_rejects_any_identity_generation_map_or_interval_mismatch() {
        let split = split_fixture();
        let request = RangeControlReq {
            tenant: "tenant-a".into(),
            range_id: split.predecessor,
            generation: split.predecessor_generation,
            operation_id: split.operation_id.clone(),
            operation: RangeControlOperation::Status,
        };
        let start = WireRangeKey {
            table_id: 0,
            rowid: 0,
        };
        assert!(
            validate_stage_request(
                &request,
                &split,
                split.predecessor,
                split.predecessor_generation,
                start,
                None,
                &split.target_map,
            )
            .is_ok()
        );
        assert!(
            validate_stage_request(
                &request,
                &split,
                split.predecessor,
                split.predecessor_generation + 1,
                start,
                None,
                &split.target_map,
            )
            .is_err()
        );
        let mut wrong_request = request.clone();
        wrong_request.operation_id = "other".into();
        assert!(
            validate_stage_request(
                &wrong_request,
                &split,
                split.predecessor,
                split.predecessor_generation,
                start,
                None,
                &split.target_map,
            )
            .is_err()
        );
        assert!(
            validate_stage_request(
                &request,
                &split,
                split.predecessor,
                split.predecessor_generation,
                WireRangeKey {
                    table_id: 0,
                    rowid: 1
                },
                None,
                &split.target_map,
            )
            .is_err()
        );
        assert!(
            validate_stage_request(
                &request,
                &split,
                split.predecessor,
                split.predecessor_generation,
                start,
                None,
                &split.current_map,
            )
            .is_err()
        );
    }

    #[test]
    fn marker_partition_requires_exact_disjoint_union() {
        let source = vec![marker(1, 10), marker(2, 20)];
        let left = RangeSpec::for_interval(
            RangeId::new(1),
            RangeKey::new(TableId::new(7), 0),
            Some(RangeKey::new(TableId::new(7), 15)),
        );
        let right =
            RangeSpec::for_interval(RangeId::new(2), RangeKey::new(TableId::new(7), 15), None);
        assert!(
            verify_marker_partition(&source, &source[..1], &source[1..], &left, &right).is_ok()
        );
        assert!(verify_marker_partition(&source, &source, &source[..1], &left, &right).is_err());
        assert!(verify_marker_partition(&source, &source[..1], &[], &left, &right).is_err());
        assert!(
            verify_marker_partition(&source, &source, &[marker(3, 30)], &left, &right).is_err()
        );
        assert!(
            verify_marker_partition(&source, &source[1..], &source[..1], &left, &right).is_err()
        );
    }

    #[test]
    fn marker_digest_is_canonical_and_sensitive_to_identity_and_key() {
        let markers = [marker(1, 10), marker(2, 20)]
            .iter()
            .map(wire_marker)
            .collect::<Vec<_>>();
        let digest = marker_digest(&markers);
        assert_eq!(digest.len(), 64);
        assert_ne!(digest, marker_digest(&markers[..1]));
        let changed = [marker(1, 10), marker(2, 21)]
            .iter()
            .map(wire_marker)
            .collect::<Vec<_>>();
        assert_ne!(digest, marker_digest(&changed));
    }
}
