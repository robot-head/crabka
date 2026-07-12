//! Generation-fenced, idempotent range-control dispatch for split orchestration.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;

use crate::transport::{RangeControlOperation, RangeControlReq, RangeControlResp};

/// Read-only tenant-scoped view of the registry's durable split-operation journal.
#[async_trait]
pub trait SplitIntentAuthority: Send + Sync {
    async fn authorize_request(
        &self,
        request: &RangeControlReq,
        context: IntentAuthorizationContext,
    ) -> Result<bool, String>;
}

/// Dispatcher-derived receipt state; callers cannot claim replay status on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentAuthorizationContext {
    New,
    InProgress,
    CompletedReplay,
}

/// Immutable registry/config snapshot suitable for compute-side authorization.
#[derive(Debug, Default)]
pub struct RegistrySplitIntentView {
    operations: BTreeMap<(String, String), crabka_gres_control::SplitOperationRecord>,
}

impl RegistrySplitIntentView {
    #[must_use]
    pub fn new(
        operations: impl IntoIterator<Item = crabka_gres_control::SplitOperationRecord>,
    ) -> Self {
        Self {
            operations: operations
                .into_iter()
                .map(|operation| {
                    (
                        (
                            operation.tenant.as_str().to_string(),
                            operation.operation_id.clone(),
                        ),
                        operation,
                    )
                })
                .collect(),
        }
    }
}

#[async_trait]
impl SplitIntentAuthority for RegistrySplitIntentView {
    async fn authorize_request(
        &self,
        request: &RangeControlReq,
        context: IntentAuthorizationContext,
    ) -> Result<bool, String> {
        Ok(self
            .operations
            .get(&(request.tenant.clone(), request.operation_id.clone()))
            .is_some_and(|operation| request_matches_split_operation(request, operation, context)))
    }
}

/// Irreversible topology-activation progress persisted on range zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyActivationPhase {
    Prepared,
    SourceCheckpoint,
    MustActivate,
    Aborted,
    WriterActivated,
    CheckpointDurable,
    TopologyCommitted,
}

/// Per-target progress needed to resume activation after a crash.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActivationTargetProgress {
    pub range_id: crate::RangeId,
    pub wal_generation: u64,
    pub endpoint: String,
    pub interval: crate::RangeSpec,
    #[serde(default)]
    pub replay_journal_seq: Option<u64>,
    pub writer_activated: bool,
    pub bootstrap_checkpoint: Option<crate::CheckpointManifest>,
}

/// Durable split activation intent and its monotone completion state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TopologyActivationReceipt {
    pub tenant: String,
    pub operation_id: String,
    pub revision: u64,
    pub phase: TopologyActivationPhase,
    pub split: crate::SplitState,
    pub source_checkpoint: Option<crate::CheckpointManifest>,
    pub barrier_offset: Option<i64>,
    pub tail_sha256: Option<String>,
    pub targets: BTreeMap<crate::RangeId, ActivationTargetProgress>,
}

#[async_trait]
pub trait TopologyActivationReceiptStore: Send + Sync {
    async fn load(&self, operation_id: &str) -> Result<Option<TopologyActivationReceipt>, String>;
    async fn list(&self) -> Result<Vec<TopologyActivationReceipt>, String>;
    async fn compare_and_swap(
        &self,
        operation_id: &str,
        expected_revision: Option<u64>,
        receipt: TopologyActivationReceipt,
    ) -> Result<bool, String>;
}

/// Production activation store committed through range zero's writer.
pub struct RangeZeroTopologyActivationStore {
    tenant: String,
    engine: crabka_pgexec::SqlEngine,
}

impl RangeZeroTopologyActivationStore {
    #[must_use]
    pub fn new(tenant: impl Into<String>, engine: crabka_pgexec::SqlEngine) -> Self {
        Self {
            tenant: tenant.into(),
            engine,
        }
    }
}

#[async_trait]
impl TopologyActivationReceiptStore for RangeZeroTopologyActivationStore {
    async fn load(&self, operation_id: &str) -> Result<Option<TopologyActivationReceipt>, String> {
        self.engine
            .topology_activation_receipt(&self.tenant, operation_id)
            .map_err(|error| format!("{error:?}"))?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .transpose()
    }

    async fn list(&self) -> Result<Vec<TopologyActivationReceipt>, String> {
        self.engine
            .topology_activation_receipts(&self.tenant)
            .map_err(|error| format!("{error:?}"))?
            .into_iter()
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .collect()
    }

    async fn compare_and_swap(
        &self,
        operation_id: &str,
        expected_revision: Option<u64>,
        receipt: TopologyActivationReceipt,
    ) -> Result<bool, String> {
        let current = self
            .engine
            .topology_activation_receipt(&self.tenant, operation_id)
            .map_err(|error| format!("{error:?}"))?;
        let current_revision = current
            .as_deref()
            .map(serde_json::from_slice::<TopologyActivationReceipt>)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(|receipt| receipt.revision);
        if current_revision != expected_revision {
            return Ok(false);
        }
        let value = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
        self.engine
            .compare_and_swap_topology_activation_receipt(
                &self.tenant,
                operation_id,
                current,
                value,
            )
            .await
            .map_err(|error| format!("{error:?}"))
    }
}

/// Runtime capability behind authenticated range-control requests.
#[async_trait]
pub trait RangeControlExecutor: Send + Sync {
    async fn execute(&self, request: &RangeControlReq) -> RangeControlResp;

    async fn reconcile(&self, _request: &RangeControlReq) -> RangeControlResp {
        RangeControlResp::Ambiguous {
            message: "operation intent is durable but runtime status is unknown".into(),
        }
    }

    /// Reconcile a step whose durable completion evidence may be widened after restart.
    ///
    /// Implementations may advance only evidence intrinsic to the same immutable request. The
    /// dispatcher validates the permitted response shape and persists it with a revision CAS.
    async fn reconcile_completed(
        &self,
        request: &RangeControlReq,
        _previous: &RangeControlResp,
    ) -> RangeControlResp {
        self.reconcile(request).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RangeControlReceipt {
    pub request: RangeControlReq,
    pub request_digest: String,
    pub generation: u64,
    pub revision: u64,
    pub result: Option<RangeControlResp>,
}

#[async_trait]
pub trait RangeControlReceiptStore: Send + Sync {
    async fn load(&self, key: &str) -> Result<Option<RangeControlReceipt>, String>;
    async fn list(&self) -> Result<Vec<RangeControlReceipt>, String>;
    async fn compare_and_swap(
        &self,
        key: &str,
        expected_revision: Option<u64>,
        receipt: RangeControlReceipt,
    ) -> Result<bool, String>;
}

#[derive(Default)]
pub struct MemoryRangeControlReceiptStore {
    receipts: tokio::sync::Mutex<BTreeMap<String, RangeControlReceipt>>,
}

#[async_trait]
impl RangeControlReceiptStore for MemoryRangeControlReceiptStore {
    async fn load(&self, key: &str) -> Result<Option<RangeControlReceipt>, String> {
        Ok(self.receipts.lock().await.get(key).cloned())
    }

    async fn list(&self) -> Result<Vec<RangeControlReceipt>, String> {
        Ok(self.receipts.lock().await.values().cloned().collect())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        expected_revision: Option<u64>,
        receipt: RangeControlReceipt,
    ) -> Result<bool, String> {
        let mut receipts = self.receipts.lock().await;
        if receipts.get(key).map(|receipt| receipt.revision) != expected_revision {
            return Ok(false);
        }
        receipts.insert(key.into(), receipt);
        Ok(true)
    }
}

/// Production receipt store committed through range 0's durable SQL-engine committer.
pub struct RangeZeroReceiptStore {
    tenant: String,
    engine: crabka_pgexec::SqlEngine,
}

impl RangeZeroReceiptStore {
    #[must_use]
    pub fn new(tenant: impl Into<String>, engine: crabka_pgexec::SqlEngine) -> Self {
        Self {
            tenant: tenant.into(),
            engine,
        }
    }
}

#[async_trait]
impl RangeControlReceiptStore for RangeZeroReceiptStore {
    async fn load(&self, key: &str) -> Result<Option<RangeControlReceipt>, String> {
        self.engine
            .range_control_receipt(&self.tenant, key)
            .map_err(|error| format!("{error:?}"))?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(|error| error.to_string()))
            .transpose()
    }

    async fn list(&self) -> Result<Vec<RangeControlReceipt>, String> {
        self.engine
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
        receipt: RangeControlReceipt,
    ) -> Result<bool, String> {
        let current = self
            .engine
            .range_control_receipt(&self.tenant, key)
            .map_err(|error| format!("{error:?}"))?;
        let current_revision = current
            .as_deref()
            .map(serde_json::from_slice::<RangeControlReceipt>)
            .transpose()
            .map_err(|error| error.to_string())?
            .map(|receipt| receipt.revision);
        if current_revision != expected_revision {
            return Ok(false);
        }
        let value = serde_json::to_vec(&receipt).map_err(|error| error.to_string())?;
        self.engine
            .compare_and_swap_range_control_receipt(&self.tenant, key, current, value)
            .await
            .map_err(|error| format!("{error:?}"))
    }
}

/// Service-side dispatcher that fences tenant/generation and replays completed operation IDs.
pub struct GenerationFencedRangeControl {
    tenant: String,
    generations: BTreeMap<crate::RangeId, std::collections::BTreeSet<u64>>,
    executor: Box<dyn RangeControlExecutor>,
    receipts: Arc<dyn RangeControlReceiptStore>,
    intent_authority: Arc<dyn SplitIntentAuthority>,
}

impl GenerationFencedRangeControl {
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        range_id: crate::RangeId,
        generation: u64,
        executor: Box<dyn RangeControlExecutor>,
        intent_authority: Arc<dyn SplitIntentAuthority>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            generations: BTreeMap::from([(
                range_id,
                std::collections::BTreeSet::from([generation]),
            )]),
            executor,
            receipts: Arc::new(MemoryRangeControlReceiptStore::default()),
            intent_authority,
        }
    }

    #[must_use]
    pub fn with_receipt_store(mut self, store: Arc<dyn RangeControlReceiptStore>) -> Self {
        self.receipts = store;
        self
    }

    #[must_use]
    pub fn with_range(mut self, range_id: crate::RangeId, generation: u64) -> Self {
        self.generations
            .entry(range_id)
            .or_default()
            .insert(generation);
        self
    }

    pub async fn handle(&self, request: RangeControlReq) -> RangeControlResp {
        if request.tenant != self.tenant {
            return rejected("wrong_tenant", "control request belongs to another tenant");
        }
        let Some(expected_generations) = self.generations.get(&request.range_id) else {
            return rejected("wrong_range", "range is not hosted by this compute");
        };
        if request.operation_id.is_empty() {
            return rejected("invalid_operation_id", "operation_id must not be empty");
        }
        let receipt_key = receipt_key(&request);
        let digest = request_digest(&request);
        let existing = match self.receipts.load(&receipt_key).await {
            Ok(existing) => existing,
            Err(message) => return rejected("receipt_store", message),
        };
        let authorization_context = match existing
            .as_ref()
            .and_then(|receipt| receipt.result.as_ref())
        {
            Some(_) => IntentAuthorizationContext::CompletedReplay,
            None if existing.is_some() => IntentAuthorizationContext::InProgress,
            None => IntentAuthorizationContext::New,
        };
        match self
            .intent_authority
            .authorize_request(&request, authorization_context)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                return rejected(
                    "unauthorized_intent",
                    "control request differs from the durable split intent",
                );
            }
            Err(message) => return rejected("intent_authority", message),
        }
        if !expected_generations.contains(&request.generation)
            && !existing.as_ref().is_some_and(|receipt| {
                receipt.request == request && receipt.request_digest == digest
            })
        {
            return rejected(
                "stale_generation",
                format!("expected one of generations {expected_generations:?}"),
            );
        }
        if let Some(receipt) = existing {
            if receipt.request_digest != digest || receipt.request != request {
                return rejected("operation_mismatch", "operation step payload changed");
            }
            if let Some(response) = receipt.result.as_ref() {
                if matches!(
                    request.operation,
                    RangeControlOperation::PauseAtCoveredOffset { .. }
                        | RangeControlOperation::StageFilteredRestore { .. }
                        | RangeControlOperation::InheritMarkers { .. }
                        | RangeControlOperation::SuccessorFencePrologue { .. }
                ) {
                    let reconciled = self.executor.reconcile_completed(&request, response).await;
                    let accepted = match (response, &reconciled) {
                        (
                            RangeControlResp::Staged { tail_sha256: _ },
                            RangeControlResp::Staged { tail_sha256: _ },
                        ) => true,
                        (
                            RangeControlResp::Paused {
                                barrier_offset: previous,
                            },
                            RangeControlResp::Paused {
                                barrier_offset: current,
                            },
                        ) if current >= previous => true,
                        (
                            RangeControlResp::Markers {
                                markers: expected_markers,
                                digest: expected_digest,
                            },
                            RangeControlResp::Markers {
                                markers: actual_markers,
                                digest: actual_digest,
                            },
                        ) if expected_markers == actual_markers
                            && expected_digest == actual_digest =>
                        {
                            true
                        }
                        (
                            RangeControlResp::Applied | RangeControlResp::AlreadyApplied,
                            RangeControlResp::Applied | RangeControlResp::AlreadyApplied,
                        ) => true,
                        _ => false,
                    };
                    if matches!(reconciled, RangeControlResp::Rejected { .. }) {
                        return reconciled;
                    }
                    if !accepted {
                        return rejected(
                            "stage_reconcile_mismatch",
                            "reconstructed evidence is stale or has an invalid response shape",
                        );
                    }
                    if &reconciled == response {
                        return replayed(response);
                    }
                    return self
                        .replace_completed_receipt(&receipt_key, receipt, reconciled)
                        .await;
                }
                return replayed(response);
            }
            let response = self.executor.reconcile(&request).await;
            crash_after_effect_if_requested(&request, &response);
            return self.complete_receipt(&receipt_key, receipt, response).await;
        }
        let intent = RangeControlReceipt {
            request: request.clone(),
            request_digest: digest,
            generation: request.generation,
            revision: 1,
            result: None,
        };
        match self
            .receipts
            .compare_and_swap(&receipt_key, None, intent.clone())
            .await
        {
            Ok(true) => {}
            Ok(false) => return rejected("receipt_race", "control receipt changed concurrently"),
            Err(message) => return rejected("receipt_store", message),
        }
        let response = self.executor.execute(&request).await;
        crash_after_effect_if_requested(&request, &response);
        self.complete_receipt(&receipt_key, intent, response).await
    }

    async fn complete_receipt(
        &self,
        key: &str,
        mut receipt: RangeControlReceipt,
        response: RangeControlResp,
    ) -> RangeControlResp {
        if matches!(response, RangeControlResp::Ambiguous { .. }) {
            return response;
        }
        let expected = receipt.revision;
        receipt.revision += 1;
        receipt.result = Some(response.clone());
        match self
            .receipts
            .compare_and_swap(key, Some(expected), receipt)
            .await
        {
            Ok(true) => response,
            Ok(false) => rejected("receipt_race", "control completion raced another writer"),
            Err(message) => rejected("receipt_store", message),
        }
    }

    async fn replace_completed_receipt(
        &self,
        key: &str,
        mut receipt: RangeControlReceipt,
        response: RangeControlResp,
    ) -> RangeControlResp {
        let expected = receipt.revision;
        receipt.revision = match receipt.revision.checked_add(1) {
            Some(revision) => revision,
            None => return rejected("receipt_revision", "control receipt revision overflow"),
        };
        receipt.result = Some(response.clone());
        match self
            .receipts
            .compare_and_swap(key, Some(expected), receipt)
            .await
        {
            Ok(true) => response,
            Ok(false) => rejected(
                "receipt_race",
                "control reconciliation raced another writer",
            ),
            Err(message) => rejected("receipt_store", message),
        }
    }
}

fn crash_after_effect_if_requested(request: &RangeControlReq, response: &RangeControlResp) {
    if matches!(
        response,
        RangeControlResp::Rejected { .. } | RangeControlResp::Ambiguous { .. }
    ) {
        return;
    }
    let requested = match std::env::var("CRABKA_GRES_CONTROL_CRASH_AFTER_EFFECT") {
        Ok(requested) => requested,
        Err(_) => return,
    };
    let step = match request.operation {
        RangeControlOperation::StageFilteredRestore { .. } => "stage",
        RangeControlOperation::InheritMarkers { .. } => "markers",
        RangeControlOperation::SuccessorFencePrologue { .. } => "prologue",
        RangeControlOperation::RetirePredecessor => "retire",
        _ => return,
    };
    if requested == step {
        std::process::exit(86);
    }
}

fn request_digest(request: &RangeControlReq) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;
    let bytes = serde_json::to_vec(request).expect("range control request serializes");
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut encoded, byte| {
            write!(&mut encoded, "{byte:02x}").expect("write digest to string");
            encoded
        })
}

fn receipt_key(request: &RangeControlReq) -> String {
    use crate::transport::RangeControlOperation as Operation;
    let step = match request.operation {
        Operation::ForceCheckpoint => "checkpoint",
        Operation::PauseAtCoveredOffset { .. } => "pause",
        Operation::Status => "status",
        Operation::StageFilteredRestore { .. } => "stage",
        Operation::SuccessorFencePrologue { .. } => "prologue",
        Operation::InheritMarkers { .. } => "markers",
        Operation::RetirePredecessor => "retire-predecessor",
        Operation::Resume => "resume",
    };
    format!(
        "r{}.g{}:{}:{step}",
        request.range_id.as_u32(),
        request.generation,
        request.operation_id
    )
}

fn replayed(response: &RangeControlResp) -> RangeControlResp {
    match response {
        RangeControlResp::Applied => RangeControlResp::AlreadyApplied,
        response => response.clone(),
    }
}

fn request_matches_split_operation(
    request: &RangeControlReq,
    operation: &crabka_gres_control::SplitOperationRecord,
    context: IntentAuthorizationContext,
) -> bool {
    let intent = &operation.split;
    if operation.tenant.as_str() != request.tenant
        || operation.operation_id != request.operation_id
        || !phase_authorizes_operation(operation.phase, &request.operation, context)
    {
        return false;
    }
    let source = request.range_id.as_u32() == intent.source_range_id
        && request.generation == intent.predecessor_generation;
    match &request.operation {
        RangeControlOperation::ForceCheckpoint
        | RangeControlOperation::PauseAtCoveredOffset { .. }
        | RangeControlOperation::Status
        | RangeControlOperation::RetirePredecessor
        | RangeControlOperation::Resume => source,
        RangeControlOperation::StageFilteredRestore {
            split: requested,
            source_range,
            source_generation,
            target_map: _,
            ..
        } => {
            source
                && runtime_split_matches_intent(requested, intent, &request.operation_id)
                && source_range.as_u32() == intent.source_range_id
                && *source_generation == intent.predecessor_generation
        }
        RangeControlOperation::SuccessorFencePrologue { split: requested } => {
            source && runtime_split_matches_intent(requested, intent, &request.operation_id)
        }
        RangeControlOperation::InheritMarkers { .. } => source,
    }
}

fn phase_authorizes_operation(
    phase: crabka_gres_control::SplitOperationPhase,
    operation: &RangeControlOperation,
    context: IntentAuthorizationContext,
) -> bool {
    use crabka_gres_control::SplitOperationPhase as Phase;
    if phase == Phase::Completed {
        return context == IntentAuthorizationContext::CompletedReplay;
    }
    match operation {
        RangeControlOperation::Status => phase != Phase::Initiated && phase != Phase::Failed,
        RangeControlOperation::ForceCheckpoint => {
            phase >= Phase::Running && phase <= Phase::Resuming
        }
        RangeControlOperation::PauseAtCoveredOffset { .. } => {
            phase >= Phase::Checkpointed && phase <= Phase::Resuming
        }
        RangeControlOperation::StageFilteredRestore { .. } => {
            phase >= Phase::Paused && phase <= Phase::Resuming
        }
        RangeControlOperation::InheritMarkers { .. } => {
            phase >= Phase::Paused && phase <= Phase::Resuming
        }
        RangeControlOperation::SuccessorFencePrologue { .. } => {
            phase >= Phase::Restored && phase <= Phase::Resuming
        }
        RangeControlOperation::RetirePredecessor => {
            phase >= Phase::Activated && phase <= Phase::Resuming
        }
        RangeControlOperation::Resume => {
            matches!(phase, Phase::Running | Phase::Checkpointed | Phase::Paused)
        }
    }
}

fn runtime_split_matches_intent(
    split: &crate::SplitState,
    intent: &crabka_gres_control::SplitState,
    operation_id: &str,
) -> bool {
    let matches_successor =
        |runtime: &crate::SuccessorDescriptor, durable: &crabka_gres_control::RangeLayoutEntry| {
            runtime.range_id.as_u32() == durable.range_id
                && runtime.endpoint == durable.endpoint
                && runtime.wal_generation == durable.wal_generation
                && runtime
                    .interval
                    .end
                    .map(|end| crabka_gres_control::RangeBoundary {
                        table_id: end.table_id.as_u64(),
                        rowid: end.rowid,
                    })
                    == durable.end_key
        };
    split.operation_id == operation_id
        && split.predecessor.as_u32() == intent.source_range_id
        && split.predecessor_generation == intent.predecessor_generation
        && matches_successor(&split.left, &intent.left)
        && split
            .right
            .as_ref()
            .is_some_and(|right| matches_successor(right, &intent.right))
}

fn rejected(code: impl Into<String>, message: impl Into<String>) -> RangeControlResp {
    RangeControlResp::Rejected {
        code: code.into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::{RangeId, transport::RangeControlOperation};

    struct MissingIntent;

    #[async_trait::async_trait]
    impl SplitIntentAuthority for MissingIntent {
        async fn authorize_request(
            &self,
            _request: &RangeControlReq,
            _context: IntentAuthorizationContext,
        ) -> Result<bool, String> {
            Ok(false)
        }
    }

    struct AllowIntent;

    #[async_trait::async_trait]
    impl SplitIntentAuthority for AllowIntent {
        async fn authorize_request(
            &self,
            _request: &RangeControlReq,
            _context: IntentAuthorizationContext,
        ) -> Result<bool, String> {
            Ok(true)
        }
    }

    fn allow_authority() -> Arc<dyn SplitIntentAuthority> {
        Arc::new(AllowIntent)
    }

    #[test]
    fn journal_phase_only_authorizes_current_or_replay_control_step() {
        use crabka_gres_control::SplitOperationPhase as Phase;

        assert!(phase_authorizes_operation(
            Phase::Running,
            &RangeControlOperation::ForceCheckpoint,
            IntentAuthorizationContext::New,
        ));
        assert!(!phase_authorizes_operation(
            Phase::Running,
            &RangeControlOperation::RetirePredecessor,
            IntentAuthorizationContext::New,
        ));
        assert!(phase_authorizes_operation(
            Phase::Checkpointed,
            &RangeControlOperation::ForceCheckpoint,
            IntentAuthorizationContext::New,
        ));
        assert!(phase_authorizes_operation(
            Phase::Checkpointed,
            &RangeControlOperation::PauseAtCoveredOffset {
                manifest_key: "m".into(),
                covered_offset: 1,
            },
            IntentAuthorizationContext::New,
        ));
        assert!(!phase_authorizes_operation(
            Phase::Activated,
            &RangeControlOperation::Resume,
            IntentAuthorizationContext::New,
        ));
        assert!(!phase_authorizes_operation(
            Phase::Completed,
            &RangeControlOperation::RetirePredecessor,
            IntentAuthorizationContext::New,
        ));
        assert!(phase_authorizes_operation(
            Phase::Completed,
            &RangeControlOperation::RetirePredecessor,
            IntentAuthorizationContext::CompletedReplay,
        ));
    }

    struct CountingExecutor {
        calls: Arc<AtomicUsize>,
        response: RangeControlResp,
    }

    struct WideningExecutor {
        execute: RangeControlResp,
        reconcile: RangeControlResp,
    }

    #[async_trait]
    impl RangeControlExecutor for WideningExecutor {
        async fn execute(&self, _request: &RangeControlReq) -> RangeControlResp {
            self.execute.clone()
        }

        async fn reconcile_completed(
            &self,
            _request: &RangeControlReq,
            _previous: &RangeControlResp,
        ) -> RangeControlResp {
            self.reconcile.clone()
        }
    }

    #[async_trait]
    impl RangeControlExecutor for CountingExecutor {
        async fn execute(&self, _request: &RangeControlReq) -> RangeControlResp {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.response.clone()
        }

        async fn reconcile(&self, _request: &RangeControlReq) -> RangeControlResp {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.response.clone()
        }
    }

    fn request(tenant: &str, generation: u64, operation_id: &str) -> RangeControlReq {
        RangeControlReq {
            tenant: tenant.into(),
            range_id: RangeId::new(1),
            generation,
            operation_id: operation_id.into(),
            operation: RangeControlOperation::RetirePredecessor,
        }
    }

    #[tokio::test]
    async fn destructive_control_rejects_missing_registry_intent() {
        let control = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::new(AtomicUsize::new(0)),
                response: RangeControlResp::Applied,
            }),
            Arc::new(MissingIntent),
        );

        let response = control.handle(request("tenant-a", 7, "forged")).await;

        assert!(
            matches!(response, RangeControlResp::Rejected { code, .. } if code == "unauthorized_intent")
        );
    }

    #[tokio::test]
    async fn wrong_tenant_and_generation_are_fenced_before_side_effects() {
        let calls = Arc::new(AtomicUsize::new(0));
        let control = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Applied,
            }),
            allow_authority(),
        );

        for rejected_request in [request("tenant-b", 7, "a"), request("tenant-a", 6, "b")] {
            assert!(matches!(
                control.handle(rejected_request).await,
                RangeControlResp::Rejected { .. }
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn operation_id_replays_success_and_rejects_mismatched_reuse() {
        let calls = Arc::new(AtomicUsize::new(0));
        let control = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Applied,
            }),
            allow_authority(),
        );
        let mut original = request("tenant-a", 7, "same");
        original.operation = RangeControlOperation::PauseAtCoveredOffset {
            manifest_key: "manifest".into(),
            covered_offset: 10,
        };
        assert_eq!(
            control.handle(original.clone()).await,
            RangeControlResp::Applied
        );
        assert_eq!(
            control.handle(original).await,
            RangeControlResp::AlreadyApplied
        );
        let mut mismatch = request("tenant-a", 7, "same");
        mismatch.operation = RangeControlOperation::PauseAtCoveredOffset {
            manifest_key: "manifest".into(),
            covered_offset: 11,
        };
        assert!(
            matches!(control.handle(mismatch).await, RangeControlResp::Rejected { code, .. } if code == "operation_mismatch")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ambiguous_outcome_is_not_cached_as_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let control = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Ambiguous {
                    message: "unknown".into(),
                },
            }),
            allow_authority(),
        );
        let original = request("tenant-a", 7, "retry");
        assert!(matches!(
            control.handle(original.clone()).await,
            RangeControlResp::Ambiguous { .. }
        ));
        assert!(matches!(
            control.handle(original).await,
            RangeControlResp::Ambiguous { .. }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn reopened_dispatcher_reconciles_durable_in_progress_receipt() {
        let store = Arc::new(MemoryRangeControlReceiptStore::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let first = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Ambiguous {
                    message: "killed after effect".into(),
                },
            }),
            allow_authority(),
        )
        .with_receipt_store(store.clone());
        let original = request("tenant-a", 7, "restart");
        assert!(matches!(
            first.handle(original.clone()).await,
            RangeControlResp::Ambiguous { .. }
        ));

        let reopened = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Applied,
            }),
            allow_authority(),
        )
        .with_receipt_store(store);
        assert_eq!(
            reopened.handle(original.clone()).await,
            RangeControlResp::Applied
        );
        assert_eq!(
            reopened.handle(original).await,
            RangeControlResp::AlreadyApplied
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one execute plus one status reconciliation"
        );
    }

    #[tokio::test]
    async fn range_zero_receipt_survives_durable_engine_reopen() {
        let directory = tempfile::tempdir().expect("receipt directory");
        let calls = Arc::new(AtomicUsize::new(0));
        let original = request("tenant-a", 7, "durable-restart");
        {
            let engine = crabka_pgexec::SqlEngine::open(directory.path()).expect("open range zero");
            let control = GenerationFencedRangeControl::new(
                "tenant-a",
                RangeId::new(1),
                7,
                Box::new(CountingExecutor {
                    calls: Arc::clone(&calls),
                    response: RangeControlResp::Ambiguous {
                        message: "killed".into(),
                    },
                }),
                allow_authority(),
            )
            .with_receipt_store(Arc::new(RangeZeroReceiptStore::new("tenant-a", engine)));
            assert!(matches!(
                control.handle(original.clone()).await,
                RangeControlResp::Ambiguous { .. }
            ));
        }
        let engine = crabka_pgexec::SqlEngine::open(directory.path()).expect("reopen range zero");
        let reopened = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Applied,
            }),
            allow_authority(),
        )
        .with_receipt_store(Arc::new(RangeZeroReceiptStore::new("tenant-a", engine)));
        assert_eq!(
            reopened.handle(original.clone()).await,
            RangeControlResp::Applied
        );
        assert_eq!(
            reopened.handle(original).await,
            RangeControlResp::AlreadyApplied
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn same_operation_step_is_independent_across_ranges_and_generations() {
        let calls = Arc::new(AtomicUsize::new(0));
        let control = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(CountingExecutor {
                calls: Arc::clone(&calls),
                response: RangeControlResp::Applied,
            }),
            allow_authority(),
        )
        .with_range(RangeId::new(2), 8);
        let first = request("tenant-a", 7, "split-same");
        let mut second = request("tenant-a", 8, "split-same");
        second.range_id = RangeId::new(2);

        assert_eq!(control.handle(first).await, RangeControlResp::Applied);
        assert_eq!(control.handle(second).await, RangeControlResp::Applied);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn completed_pause_receipt_can_advance_monotonically_under_same_request() {
        let store = Arc::new(MemoryRangeControlReceiptStore::default());
        let mut pause = request("tenant-a", 7, "widen");
        pause.operation = RangeControlOperation::PauseAtCoveredOffset {
            manifest_key: "manifest".into(),
            covered_offset: 5,
        };
        let first = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(WideningExecutor {
                execute: RangeControlResp::Paused { barrier_offset: 10 },
                reconcile: RangeControlResp::Paused { barrier_offset: 10 },
            }),
            allow_authority(),
        )
        .with_receipt_store(store.clone());
        assert_eq!(
            first.handle(pause.clone()).await,
            RangeControlResp::Paused { barrier_offset: 10 }
        );

        let reopened = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(WideningExecutor {
                execute: RangeControlResp::Paused { barrier_offset: 14 },
                reconcile: RangeControlResp::Paused { barrier_offset: 14 },
            }),
            allow_authority(),
        )
        .with_receipt_store(store.clone());
        assert_eq!(
            reopened.handle(pause.clone()).await,
            RangeControlResp::Paused { barrier_offset: 14 }
        );
        let receipt = store
            .load(&receipt_key(&pause))
            .await
            .unwrap()
            .expect("updated receipt");
        assert_eq!(receipt.revision, 3);
        assert_eq!(
            receipt.result,
            Some(RangeControlResp::Paused { barrier_offset: 14 })
        );
    }

    #[tokio::test]
    async fn completed_pause_receipt_rejects_a_barrier_downgrade() {
        let store = Arc::new(MemoryRangeControlReceiptStore::default());
        let mut pause = request("tenant-a", 7, "downgrade");
        pause.operation = RangeControlOperation::PauseAtCoveredOffset {
            manifest_key: "manifest".into(),
            covered_offset: 5,
        };
        let first = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(WideningExecutor {
                execute: RangeControlResp::Paused { barrier_offset: 10 },
                reconcile: RangeControlResp::Paused { barrier_offset: 10 },
            }),
            allow_authority(),
        )
        .with_receipt_store(store.clone());
        assert!(matches!(
            first.handle(pause.clone()).await,
            RangeControlResp::Paused { .. }
        ));
        let reopened = GenerationFencedRangeControl::new(
            "tenant-a",
            RangeId::new(1),
            7,
            Box::new(WideningExecutor {
                execute: RangeControlResp::Paused { barrier_offset: 9 },
                reconcile: RangeControlResp::Paused { barrier_offset: 9 },
            }),
            allow_authority(),
        )
        .with_receipt_store(store.clone());
        assert!(matches!(
            reopened.handle(pause.clone()).await,
            RangeControlResp::Rejected { ref code, .. } if code == "stage_reconcile_mismatch"
        ));
        assert_eq!(
            store
                .load(&receipt_key(&pause))
                .await
                .unwrap()
                .unwrap()
                .revision,
            2
        );
    }
}
