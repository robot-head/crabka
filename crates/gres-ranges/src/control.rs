//! Generation-fenced, idempotent range-control dispatch for split orchestration.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;

use crate::transport::{RangeControlReq, RangeControlResp};

/// Irreversible topology-activation progress persisted on range zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyActivationPhase {
    Prepared,
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
    generations: BTreeMap<crate::RangeId, u64>,
    executor: Box<dyn RangeControlExecutor>,
    receipts: Arc<dyn RangeControlReceiptStore>,
}

impl GenerationFencedRangeControl {
    #[must_use]
    pub fn new(
        tenant: impl Into<String>,
        range_id: crate::RangeId,
        generation: u64,
        executor: Box<dyn RangeControlExecutor>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            generations: BTreeMap::from([(range_id, generation)]),
            executor,
            receipts: Arc::new(MemoryRangeControlReceiptStore::default()),
        }
    }

    #[must_use]
    pub fn with_receipt_store(mut self, store: Arc<dyn RangeControlReceiptStore>) -> Self {
        self.receipts = store;
        self
    }

    #[must_use]
    pub fn with_range(mut self, range_id: crate::RangeId, generation: u64) -> Self {
        self.generations.insert(range_id, generation);
        self
    }

    pub async fn handle(&self, request: RangeControlReq) -> RangeControlResp {
        if request.tenant != self.tenant {
            return rejected("wrong_tenant", "control request belongs to another tenant");
        }
        let Some(expected_generation) = self.generations.get(&request.range_id) else {
            return rejected("wrong_range", "range is not hosted by this compute");
        };
        if request.generation != *expected_generation {
            return rejected(
                "stale_generation",
                format!("expected generation {expected_generation}"),
            );
        }
        if request.operation_id.is_empty() {
            return rejected("invalid_operation_id", "operation_id must not be empty");
        }
        let receipt_key = receipt_key(&request);
        let digest = request_digest(&request);
        let existing = match self.receipts.load(&receipt_key).await {
            Ok(existing) => existing,
            Err(message) => return rejected("receipt_store", message),
        };
        if let Some(receipt) = existing {
            if receipt.request_digest != digest || receipt.request != request {
                return rejected("operation_mismatch", "operation step payload changed");
            }
            if let Some(response) = receipt.result {
                return replayed(&response);
            }
            let response = self.executor.reconcile(&request).await;
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
        Operation::SuccessorFencePrologue => "prologue",
        Operation::InheritMarkers { .. } => "markers",
        Operation::Park => "park",
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

    struct CountingExecutor {
        calls: Arc<AtomicUsize>,
        response: RangeControlResp,
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
            operation: RangeControlOperation::Park,
        }
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
        assert_eq!(calls.load(Ordering::SeqCst), 1);
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
        )
        .with_range(RangeId::new(2), 8);
        let first = request("tenant-a", 7, "split-same");
        let mut second = request("tenant-a", 8, "split-same");
        second.range_id = RangeId::new(2);

        assert_eq!(control.handle(first).await, RangeControlResp::Applied);
        assert_eq!(control.handle(second).await, RangeControlResp::Applied);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
