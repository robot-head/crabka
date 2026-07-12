use crabka_gres_control::{
    RangeRetirementCheckpoint, RangeRetirementPhase, SplitOperationPhase, SplitOperationRecord,
};
use crabka_gres_ranges::{
    AuthorizedSplitIntent, FramedTcpClient, RangeControlOperation, RangeControlReq,
    RangeControlResp, RangeId, RangeRequest, RangeResponse,
};

#[async_trait::async_trait]
pub trait RangeMutationClient: Send + Sync {
    async fn mutate(
        &self,
        endpoint: &str,
        request: RangeControlReq,
    ) -> Result<RangeControlResp, SplitReconcileError>;
}

/// Production mTLS adapter. Construction of `FramedTcpClient` requires a client identity,
/// trust roots and peer DNS verification; plaintext is unavailable outside range crate tests.
pub struct MtlsRangeMutationClient {
    client: FramedTcpClient,
}

impl MtlsRangeMutationClient {
    #[must_use]
    pub const fn new(client: FramedTcpClient) -> Self {
        Self { client }
    }
}

#[async_trait::async_trait]
impl RangeMutationClient for MtlsRangeMutationClient {
    async fn mutate(
        &self,
        endpoint: &str,
        request: RangeControlReq,
    ) -> Result<RangeControlResp, SplitReconcileError> {
        match self
            .client
            .call(endpoint, &RangeRequest::Control(request))
            .await
            .map_err(|error| SplitReconcileError::Transport(error.to_string()))?
        {
            RangeResponse::Control(response) => Ok(response),
            _ => Err(SplitReconcileError::UnexpectedResponse),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SplitReconcileError {
    #[error("range control transport: {0}")]
    Transport(String),
    #[error("split registry: {0}")]
    Registry(String),
    #[error("invalid split journal: {0}")]
    InvalidJournal(String),
    #[error("range control rejected split step {code}: {message}")]
    Rejected { code: String, message: String },
    #[error("range control returned ambiguous result: {0}")]
    Ambiguous(String),
    #[error("range control returned an unexpected response")]
    UnexpectedResponse,
}

/// Advance at most one durable range-RPC phase. Ambiguous responses leave the journal
/// unchanged, so the receipt-keyed request is replayed verbatim after restart.
pub async fn reconcile_one_rpc_phase(
    control: &crate::context::GresControlHandle,
    client: &dyn RangeMutationClient,
    record: &SplitOperationRecord,
) -> Result<SplitOperationRecord, SplitReconcileError> {
    if record.phase == SplitOperationPhase::Initiated {
        let running = record
            .advance(SplitOperationPhase::Running, 1, None)
            .map_err(|error| SplitReconcileError::InvalidJournal(error.to_string()))?;
        return control
            .compare_and_swap_split_operation(record.revision, &running)
            .await
            .map_err(|error| SplitReconcileError::Registry(error.to_string()));
    }
    if record.phase == SplitOperationPhase::Resuming {
        let completed = record
            .advance(SplitOperationPhase::Completed, record.attempts, None)
            .map_err(|error| SplitReconcileError::InvalidJournal(error.to_string()))?;
        return control
            .compare_and_swap_split_operation(record.revision, &completed)
            .await
            .map_err(|error| SplitReconcileError::Registry(error.to_string()));
    }
    if record.phase == SplitOperationPhase::Retiring {
        let tenant = control
            .get_tenant(&record.tenant)
            .await
            .map_err(|error| SplitReconcileError::Registry(error.to_string()))?
            .ok_or_else(|| SplitReconcileError::InvalidJournal("tenant disappeared".into()))?;
        let parked = tenant.range_retirements.iter().any(|retirement| {
            retirement.operation_id == record.operation_id
                && retirement.source_range_id == record.split.source_range_id
                && retirement.source_generation == record.split.predecessor_generation
                && retirement.phase == RangeRetirementPhase::Parked
        });
        if !parked {
            return Ok(record.clone());
        }
    }
    let request = request_for_phase(record)?;
    let response = client.mutate(source_endpoint(record)?, request).await?;
    let next = apply_response(record, response)?;
    control
        .compare_and_swap_split_operation(record.revision, &next)
        .await
        .map_err(|error| SplitReconcileError::Registry(error.to_string()))
}

/// Perform or acknowledge the exact atomic topology cutover. The tenant CAS and journal CAS
/// happen on separate reconciles, making an ambiguous registry acknowledgement restart-safe.
pub async fn reconcile_activated_cutover(
    control: &crate::context::GresControlHandle,
    record: &SplitOperationRecord,
) -> Result<SplitOperationRecord, SplitReconcileError> {
    if record.phase != SplitOperationPhase::Activated {
        return Err(SplitReconcileError::InvalidJournal(
            "cutover requires activated successors".into(),
        ));
    }
    let plan = record
        .plan
        .as_ref()
        .ok_or_else(|| SplitReconcileError::InvalidJournal("sealed plan is missing".into()))?;
    let tenant = control
        .get_tenant(&record.tenant)
        .await
        .map_err(|error| SplitReconcileError::Registry(error.to_string()))?
        .ok_or_else(|| SplitReconcileError::InvalidJournal("tenant disappeared".into()))?;
    let matching_retirement = tenant.range_retirements.iter().find(|retirement| {
        retirement.operation_id == record.operation_id
            && retirement.source_range_id == record.split.source_range_id
            && retirement.source_generation == record.split.predecessor_generation
    });
    if let Some(retirement) = matching_retirement {
        if tenant.ranges != plan.target_layout
            || retirement.successor_ranges
                != plan
                    .target_layout
                    .iter()
                    .map(|range| (range.range_id, range.wal_generation))
                    .collect::<Vec<_>>()
            || retirement.phase != RangeRetirementPhase::Parking
        {
            return Err(SplitReconcileError::InvalidJournal(
                "durable cutover differs from sealed target topology".into(),
            ));
        }
        let retiring = record
            .advance(SplitOperationPhase::Retiring, record.attempts, None)
            .map_err(|error| SplitReconcileError::InvalidJournal(error.to_string()))?;
        return control
            .compare_and_swap_split_operation(record.revision, &retiring)
            .await
            .map_err(|error| SplitReconcileError::Registry(error.to_string()));
    }
    if tenant.record_version != plan.source_record_version || tenant.ranges != plan.current_layout {
        return Err(SplitReconcileError::InvalidJournal(
            "tenant layout or version no longer matches sealed predecessor".into(),
        ));
    }
    let evidence = complete_retirement_evidence(record)?;
    let expected_version = tenant.record_version;
    let cutover = tenant
        .publish_split_target_with_retirement(
            record.operation_id.clone(),
            record.split.source_range_id,
            record.split.predecessor_generation,
            evidence,
            plan.target_layout.clone(),
        )
        .map_err(|error| SplitReconcileError::InvalidJournal(error.to_string()))?;
    control
        .replace_tenant_if_version(&cutover, Some(expected_version))
        .await
        .map_err(|error| SplitReconcileError::Registry(error.to_string()))?;
    Ok(record.clone())
}

fn complete_retirement_evidence(
    record: &SplitOperationRecord,
) -> Result<RangeRetirementCheckpoint, SplitReconcileError> {
    let evidence = &record.evidence;
    Ok(RangeRetirementCheckpoint {
        manifest_key: evidence.manifest_key.clone().ok_or_else(|| {
            SplitReconcileError::InvalidJournal("manifest evidence is missing".into())
        })?,
        covered_offset: evidence.covered_offset.ok_or_else(|| {
            SplitReconcileError::InvalidJournal("covered offset is missing".into())
        })?,
        barrier_offset: evidence.barrier_offset.ok_or_else(|| {
            SplitReconcileError::InvalidJournal("barrier offset is missing".into())
        })?,
        tail_sha256: evidence
            .tail_sha256
            .clone()
            .ok_or_else(|| SplitReconcileError::InvalidJournal("tail digest is missing".into()))?,
        marker_digest: evidence.marker_digest.clone().ok_or_else(|| {
            SplitReconcileError::InvalidJournal("marker digest is missing".into())
        })?,
    })
}

fn source_endpoint(record: &SplitOperationRecord) -> Result<&str, SplitReconcileError> {
    record
        .plan
        .as_ref()
        .and_then(|plan| {
            plan.current_layout
                .iter()
                .find(|range| range.range_id == record.split.source_range_id)
        })
        .map(|range| range.endpoint.as_str())
        .ok_or_else(|| SplitReconcileError::InvalidJournal("source endpoint is missing".into()))
}

fn request_for_phase(
    record: &SplitOperationRecord,
) -> Result<RangeControlReq, SplitReconcileError> {
    if record.plan.is_none() {
        return Err(SplitReconcileError::InvalidJournal(
            "sealed plan is missing".into(),
        ));
    }
    let operation = match record.phase {
        SplitOperationPhase::Running => RangeControlOperation::ForceCheckpoint,
        SplitOperationPhase::Checkpointed => RangeControlOperation::PauseAtCoveredOffset {
            manifest_key: record.evidence.manifest_key.clone().ok_or_else(|| {
                SplitReconcileError::InvalidJournal("manifest receipt is missing".into())
            })?,
            covered_offset: record.evidence.covered_offset.ok_or_else(|| {
                SplitReconcileError::InvalidJournal("checkpoint offset is missing".into())
            })?,
        },
        SplitOperationPhase::Paused if record.evidence.tail_sha256.is_none() => {
            journal_operation(record, |revision, digest| {
                RangeControlOperation::StageFilteredRestore {
                    journal_revision: revision,
                    journal_digest: digest,
                }
            })?
        }
        SplitOperationPhase::Paused => journal_operation(record, |revision, digest| {
            RangeControlOperation::InheritMarkers {
                journal_revision: revision,
                journal_digest: digest,
            }
        })?,
        SplitOperationPhase::Restored => journal_operation(record, |revision, digest| {
            RangeControlOperation::SuccessorFencePrologue {
                journal_revision: revision,
                journal_digest: digest,
            }
        })?,
        SplitOperationPhase::Activated | SplitOperationPhase::Retiring => {
            RangeControlOperation::RetirePredecessor
        }
        phase => {
            return Err(SplitReconcileError::InvalidJournal(format!(
                "phase {phase:?} has no range RPC step"
            )));
        }
    };
    source_endpoint(record)?;
    Ok(RangeControlReq {
        tenant: record.tenant.as_str().into(),
        range_id: RangeId::new(record.split.source_range_id),
        generation: record.split.predecessor_generation,
        operation_id: record.operation_id.clone(),
        operation,
    })
}

fn journal_operation(
    record: &SplitOperationRecord,
    build: impl FnOnce(u64, String) -> RangeControlOperation,
) -> Result<RangeControlOperation, SplitReconcileError> {
    let authorized = AuthorizedSplitIntent::from_record(record.clone())
        .map_err(SplitReconcileError::InvalidJournal)?;
    Ok(build(record.revision, authorized.digest().to_owned()))
}

fn apply_response(
    record: &SplitOperationRecord,
    response: RangeControlResp,
) -> Result<SplitOperationRecord, SplitReconcileError> {
    let mut evidence = record.evidence.clone();
    let phase = match (record.phase, response) {
        (
            SplitOperationPhase::Running,
            RangeControlResp::Checkpoint {
                generation,
                covered_offset,
                manifest_key,
            },
        ) => {
            if generation != record.split.predecessor_generation
                || covered_offset < 0
                || manifest_key.is_empty()
            {
                return Err(SplitReconcileError::UnexpectedResponse);
            }
            evidence.manifest_key = Some(manifest_key);
            evidence.covered_offset = Some(covered_offset);
            SplitOperationPhase::Checkpointed
        }
        (SplitOperationPhase::Checkpointed, RangeControlResp::Paused { barrier_offset }) => {
            if barrier_offset < evidence.covered_offset.unwrap_or(i64::MAX) {
                return Err(SplitReconcileError::UnexpectedResponse);
            }
            evidence.barrier_offset = Some(barrier_offset);
            SplitOperationPhase::Paused
        }
        (SplitOperationPhase::Paused, RangeControlResp::Staged { tail_sha256 })
            if evidence.tail_sha256.is_none() && !tail_sha256.is_empty() =>
        {
            evidence.tail_sha256 = Some(tail_sha256);
            SplitOperationPhase::Paused
        }
        (SplitOperationPhase::Paused, RangeControlResp::Markers { digest, .. })
            if evidence.tail_sha256.is_some() && !digest.is_empty() =>
        {
            evidence.marker_digest = Some(digest);
            SplitOperationPhase::Restored
        }
        (
            SplitOperationPhase::Restored,
            RangeControlResp::Applied | RangeControlResp::AlreadyApplied,
        ) => SplitOperationPhase::Activated,
        (
            SplitOperationPhase::Retiring,
            RangeControlResp::Applied | RangeControlResp::AlreadyApplied,
        ) => SplitOperationPhase::Resuming,
        (_, RangeControlResp::Rejected { code, message }) => {
            return Err(SplitReconcileError::Rejected { code, message });
        }
        (_, RangeControlResp::Ambiguous { message }) => {
            return Err(SplitReconcileError::Ambiguous(message));
        }
        _ => return Err(SplitReconcileError::UnexpectedResponse),
    };
    record
        .advance_with_evidence(phase, record.attempts, None, evidence)
        .map_err(|error| SplitReconcileError::InvalidJournal(error.to_string()))
}

pub(crate) fn active_operations(
    mut records: Vec<SplitOperationRecord>,
) -> Vec<SplitOperationRecord> {
    records.retain(|record| {
        !matches!(
            record.phase,
            SplitOperationPhase::Completed | SplitOperationPhase::Failed
        )
    });
    records.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    records
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;
    use crabka_gres_control::{
        RangeBoundary, RangeLayoutEntry, RangeLayoutSplit, RangeLifecycle, SplitOperationPlan,
        TenantName,
    };
    use crabka_gres_ranges::{RangeControlResp, RangeId};
    use tokio::sync::Mutex;

    #[test]
    fn active_operation_filter_is_ordered_and_excludes_terminal_records() {
        let mut second = operation();
        second.operation_id = "split-b".into();
        let second = second
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let mut first = operation();
        first.operation_id = "split-a".into();
        let first = first
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let terminal = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap()
            .advance(SplitOperationPhase::Completed, 1, None)
            .unwrap();
        let active = active_operations(vec![second, terminal, first]);
        assert_eq!(
            active
                .iter()
                .map(|record| record.operation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["split-a", "split-b"]
        );
    }

    fn operation() -> SplitOperationRecord {
        let source = RangeLayoutEntry {
            range_id: 0,
            end_key: None,
            endpoint: "old:7443".into(),
            wal_generation: 4,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let left = RangeLayoutEntry {
            range_id: 0,
            end_key: Some(RangeBoundary::new(7, 50)),
            endpoint: "left:7443".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        let right = RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint: "right:7443".into(),
            wal_generation: 5,
            lifecycle: RangeLifecycle::Serving,
            retirement: None,
        };
        SplitOperationRecord::new(
            TenantName::try_from("tenant-a").unwrap(),
            "split-1",
            RangeLayoutSplit {
                source_range_id: 0,
                predecessor_generation: 4,
                left: left.clone(),
                right: right.clone(),
            },
        )
        .unwrap()
        .with_plan(SplitOperationPlan {
            source_record_version: 9,
            source_map_epoch: 9,
            routing_table_id: 7,
            current_layout: vec![source],
            target_layout: vec![left, right],
        })
        .unwrap()
    }

    #[test]
    fn checkpoint_receipt_must_match_predecessor_generation() {
        let running = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let request = request_for_phase(&running).unwrap();
        assert_eq!(request.range_id, RangeId::new(0));
        assert!(
            apply_response(
                &running,
                RangeControlResp::Checkpoint {
                    generation: 99,
                    covered_offset: 8,
                    manifest_key: "m".into()
                }
            )
            .is_err()
        );
        let checkpointed = apply_response(
            &running,
            RangeControlResp::Checkpoint {
                generation: 4,
                covered_offset: 8,
                manifest_key: "m".into(),
            },
        )
        .unwrap();
        assert_eq!(checkpointed.phase, SplitOperationPhase::Checkpointed);
        assert_eq!(checkpointed.evidence.covered_offset, Some(8));
    }

    #[test]
    fn response_receipts_advance_exactly_one_revision_per_durable_step() {
        let mut record = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let responses = [
            RangeControlResp::Checkpoint {
                generation: 4,
                covered_offset: 8,
                manifest_key: "manifest".into(),
            },
            RangeControlResp::Paused { barrier_offset: 10 },
            RangeControlResp::Staged {
                tail_sha256: "tail".into(),
            },
            RangeControlResp::Markers {
                markers: vec![],
                digest: "markers".into(),
            },
            RangeControlResp::Applied,
        ];
        let phases = [
            SplitOperationPhase::Checkpointed,
            SplitOperationPhase::Paused,
            SplitOperationPhase::Paused,
            SplitOperationPhase::Restored,
            SplitOperationPhase::Activated,
        ];
        for (response, phase) in responses.into_iter().zip(phases) {
            let revision = record.revision;
            record = apply_response(&record, response).unwrap();
            assert_eq!(record.revision, revision + 1);
            assert_eq!(record.phase, phase);
        }
        assert_eq!(record.evidence.manifest_key.as_deref(), Some("manifest"));
        assert_eq!(record.evidence.barrier_offset, Some(10));
        assert_eq!(record.evidence.tail_sha256.as_deref(), Some("tail"));
        assert_eq!(record.evidence.marker_digest.as_deref(), Some("markers"));
    }

    #[test]
    fn ambiguous_and_rejected_responses_never_advance_the_journal() {
        let running = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        assert!(matches!(
            apply_response(
                &running,
                RangeControlResp::Ambiguous {
                    message: "lost ack".into()
                }
            ),
            Err(SplitReconcileError::Ambiguous(_))
        ));
        assert!(matches!(
            apply_response(
                &running,
                RangeControlResp::Rejected {
                    code: "stale".into(),
                    message: "forged".into()
                }
            ),
            Err(SplitReconcileError::Rejected { .. })
        ));
        assert_eq!(running.revision, 1);
        assert_eq!(running.phase, SplitOperationPhase::Running);
    }

    struct CrashOnceControl {
        operation: Mutex<SplitOperationRecord>,
        tenant: Mutex<crabka_gres_control::TenantRecord>,
        fail_next_cas: AtomicBool,
        apply_replace_then_fail: AtomicBool,
    }

    #[async_trait::async_trait]
    impl crate::context::GresControlLike for CrashOnceControl {
        async fn get_tenant(
            &self,
            _tenant: &crabka_gres_control::TenantName,
        ) -> Result<Option<crabka_gres_control::TenantRecord>, crate::context::GresControlWriteError>
        {
            Ok(Some(self.tenant.lock().await.clone()))
        }

        async fn replace_tenant_if_version(
            &self,
            record: &crabka_gres_control::TenantRecord,
            expected: Option<u64>,
        ) -> Result<crabka_gres_control::TenantRecord, crate::context::GresControlWriteError>
        {
            let mut current = self.tenant.lock().await;
            if expected != Some(current.record_version) {
                return Err(test_registry_error());
            }
            *current = record.clone();
            if self.apply_replace_then_fail.swap(false, Ordering::SeqCst) {
                return Err(test_registry_error());
            }
            Ok(record.clone())
        }

        async fn delete_tenant(
            &self,
            _tenant: &crabka_gres_control::TenantName,
        ) -> Result<(), crate::context::GresControlWriteError> {
            unreachable!()
        }

        async fn validate_final_checkpoint_manifest(
            &self,
            _record: &crabka_gres_control::TenantRecord,
        ) -> Result<(), crate::context::GresControlWriteError> {
            unreachable!()
        }

        async fn compare_and_swap_split_operation(
            &self,
            expected: u64,
            operation: &SplitOperationRecord,
        ) -> Result<SplitOperationRecord, crate::context::GresControlWriteError> {
            if self.fail_next_cas.swap(false, Ordering::SeqCst) {
                return Err(test_registry_error());
            }
            let mut current = self.operation.lock().await;
            if current.revision != expected {
                return Err(test_registry_error());
            }
            *current = operation.clone();
            Ok(operation.clone())
        }
    }

    fn test_registry_error() -> crate::context::GresControlWriteError {
        crabka_gres_control::ControlError::UnsupportedRegistryMutation {
            mutation: "test_crash",
            reason: "injected acknowledgement loss",
        }
        .into()
    }

    struct ReceiptClient {
        response: RangeControlResp,
        requests: Mutex<Vec<RangeControlReq>>,
    }

    #[async_trait::async_trait]
    impl RangeMutationClient for ReceiptClient {
        async fn mutate(
            &self,
            _endpoint: &str,
            request: RangeControlReq,
        ) -> Result<RangeControlResp, SplitReconcileError> {
            self.requests.lock().await.push(request);
            Ok(self.response.clone())
        }
    }

    fn tenant_for(operation: &SplitOperationRecord) -> crabka_gres_control::TenantRecord {
        let plan = operation.plan.as_ref().unwrap();
        let mut tenant = crabka_gres_control::TenantRecord::new(
            plan.source_record_version,
            crabka_gres_control::TenantId::try_from("tenant-a").unwrap(),
            crabka_gres_control::TenantName::try_from("tenant-a").unwrap(),
            crabka_gres_control::TenantState::Active,
            crabka_gres_control::SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".into(),
            1,
        )
        .unwrap()
        .with_range_layout(plan.current_layout.clone())
        .unwrap();
        tenant.record_version = plan.source_record_version;
        tenant
    }

    #[tokio::test]
    async fn crash_after_each_rpc_side_effect_replays_the_exact_receipt_request() {
        let mut running = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        let cases = vec![
            (
                running.clone(),
                RangeControlResp::Checkpoint {
                    generation: 4,
                    covered_offset: 8,
                    manifest_key: "manifest".into(),
                },
            ),
            {
                running = apply_response(
                    &running,
                    RangeControlResp::Checkpoint {
                        generation: 4,
                        covered_offset: 8,
                        manifest_key: "manifest".into(),
                    },
                )
                .unwrap();
                (
                    running.clone(),
                    RangeControlResp::Paused { barrier_offset: 10 },
                )
            },
            {
                running = apply_response(&running, RangeControlResp::Paused { barrier_offset: 10 })
                    .unwrap();
                (
                    running.clone(),
                    RangeControlResp::Staged {
                        tail_sha256: "tail".into(),
                    },
                )
            },
            {
                running = apply_response(
                    &running,
                    RangeControlResp::Staged {
                        tail_sha256: "tail".into(),
                    },
                )
                .unwrap();
                (
                    running.clone(),
                    RangeControlResp::Markers {
                        markers: vec![],
                        digest: "markers".into(),
                    },
                )
            },
            {
                running = apply_response(
                    &running,
                    RangeControlResp::Markers {
                        markers: vec![],
                        digest: "markers".into(),
                    },
                )
                .unwrap();
                (running.clone(), RangeControlResp::Applied)
            },
        ];
        for (record, response) in cases {
            let control = Arc::new(CrashOnceControl {
                operation: Mutex::new(record.clone()),
                tenant: Mutex::new(tenant_for(&record)),
                fail_next_cas: AtomicBool::new(true),
                apply_replace_then_fail: AtomicBool::new(false),
            });
            let handle: crate::context::GresControlHandle = control.clone();
            let client = ReceiptClient {
                response,
                requests: Mutex::new(Vec::new()),
            };
            assert!(
                reconcile_one_rpc_phase(&handle, &client, &record)
                    .await
                    .is_err()
            );
            assert_eq!(control.operation.lock().await.revision, record.revision);
            reconcile_one_rpc_phase(&handle, &client, &record)
                .await
                .unwrap();
            let requests = client.requests.lock().await;
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0], requests[1]);
            assert_eq!(control.operation.lock().await.revision, record.revision + 1);
        }
    }

    #[tokio::test]
    async fn ambiguous_layout_cutover_reloads_exact_target_without_duplicate_retirement() {
        let mut activated = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        for response in [
            RangeControlResp::Checkpoint {
                generation: 4,
                covered_offset: 8,
                manifest_key: "manifest".into(),
            },
            RangeControlResp::Paused { barrier_offset: 10 },
            RangeControlResp::Staged {
                tail_sha256: "tail".into(),
            },
            RangeControlResp::Markers {
                markers: vec![],
                digest: "markers".into(),
            },
            RangeControlResp::Applied,
        ] {
            activated = apply_response(&activated, response).unwrap();
        }
        let source = tenant_for(&activated);
        let control = Arc::new(CrashOnceControl {
            operation: Mutex::new(activated.clone()),
            tenant: Mutex::new(source),
            fail_next_cas: AtomicBool::new(false),
            apply_replace_then_fail: AtomicBool::new(true),
        });
        let handle: crate::context::GresControlHandle = control.clone();

        assert!(
            reconcile_activated_cutover(&handle, &activated)
                .await
                .is_err()
        );
        let durable = control.tenant.lock().await.clone();
        assert_eq!(
            durable.ranges,
            activated.plan.as_ref().unwrap().target_layout
        );
        assert_eq!(durable.range_retirements.len(), 1);
        let retiring = reconcile_activated_cutover(&handle, &activated)
            .await
            .unwrap();
        assert_eq!(retiring.phase, SplitOperationPhase::Retiring);
        assert_eq!(control.tenant.lock().await.range_retirements.len(), 1);
    }

    #[tokio::test]
    async fn crash_after_retire_rpc_replays_receipt_only_after_sidecar_is_parked() {
        let mut record = operation()
            .advance(SplitOperationPhase::Running, 1, None)
            .unwrap();
        for response in [
            RangeControlResp::Checkpoint {
                generation: 4,
                covered_offset: 8,
                manifest_key: "manifest".into(),
            },
            RangeControlResp::Paused { barrier_offset: 10 },
            RangeControlResp::Staged {
                tail_sha256: "tail".into(),
            },
            RangeControlResp::Markers {
                markers: vec![],
                digest: "markers".into(),
            },
            RangeControlResp::Applied,
        ] {
            record = apply_response(&record, response).unwrap();
        }
        record = record
            .advance(SplitOperationPhase::Retiring, 1, None)
            .unwrap();
        let plan = record.plan.as_ref().unwrap();
        let tenant = tenant_for(&record)
            .publish_split_target_with_retirement(
                record.operation_id.clone(),
                0,
                4,
                complete_retirement_evidence(&record).unwrap(),
                plan.target_layout.clone(),
            )
            .unwrap()
            .confirm_split_predecessor_parked(&record.operation_id, 0, 4)
            .unwrap();
        let control = Arc::new(CrashOnceControl {
            operation: Mutex::new(record.clone()),
            tenant: Mutex::new(tenant),
            fail_next_cas: AtomicBool::new(true),
            apply_replace_then_fail: AtomicBool::new(false),
        });
        let handle: crate::context::GresControlHandle = control.clone();
        let client = ReceiptClient {
            response: RangeControlResp::Applied,
            requests: Mutex::new(Vec::new()),
        };
        assert!(
            reconcile_one_rpc_phase(&handle, &client, &record)
                .await
                .is_err()
        );
        let resumed = reconcile_one_rpc_phase(&handle, &client, &record)
            .await
            .unwrap();
        assert_eq!(resumed.phase, SplitOperationPhase::Resuming);
        let requests = client.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }
}
