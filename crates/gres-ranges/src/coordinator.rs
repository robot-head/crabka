//! Transaction coordinators over local state and range-compute RPC.

use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    RangeId,
    registry::{RangeRegistry, RegistryError},
    transport::{FramedTcpClient, RangeRequest, RangeResponse, TransportError, TxnReq, TxnResp},
};

/// In-memory local 2PC coordinator state machine for cross-range transactions.
#[derive(Clone, Default)]
pub struct LocalCoordinator {
    state: Arc<Mutex<CoordinatorState>>,
}

#[derive(Debug, Default)]
struct CoordinatorState {
    next_id: u64,
    transactions: BTreeMap<u64, LocalTransactionRecord>,
}

/// Durable-enough-for-process state for a local transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalTransactionRecord {
    /// Process-local global transaction id.
    pub xid: u64,
    /// Participants known at begin time.
    pub participants: Vec<RangeId>,
    /// Current coarse phase of the transaction.
    pub phase: TransactionPhase,
    /// Participants whose prepare record has been accepted.
    pub prepared: Vec<PreparedParticipant>,
    /// Terminal decision, when present.
    pub decision: Option<TransactionDecision>,
}

/// One participant prepare record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedParticipant {
    /// Prepared range id.
    pub range_id: RangeId,
}

/// Durable-enough-for-process state for a local transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionPhase {
    /// Single-range local transaction; no 2PC needed.
    Local,
    /// A global xid exists and participants may be prepared.
    Begun,
    /// Every participant has a prepare record and is waiting for decision.
    Prepared,
    /// Terminal state.
    Decided,
}

/// Terminal global decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionDecision {
    /// Commit every participant.
    Commit,
    /// Abort every participant.
    Abort,
}

/// Invalid local coordinator transition.
#[derive(Debug, thiserror::Error)]
pub enum LocalCoordinatorError {
    /// The xid is unknown to this process-local coordinator.
    #[error("unknown local coordinator transaction {xid}")]
    UnknownTransaction { xid: u64 },
    /// The range was not part of the participant set captured at begin.
    #[error("range r{range_id} is not a participant in local coordinator transaction {xid}")]
    UnknownParticipant { xid: u64, range_id: RangeId },
    /// A different final decision has already been recorded.
    #[error("local coordinator transaction {xid} already decided {existing:?}")]
    DecisionAlreadyFinal {
        xid: u64,
        existing: TransactionDecision,
    },
    /// Commit requires all participants to have prepare records.
    #[error("local coordinator transaction {xid} cannot commit before every participant prepared")]
    CommitBeforePrepared { xid: u64 },
    /// An existing xid is already associated with different participants.
    #[error("local coordinator transaction {xid} already begun with different participants")]
    ExistingTransactionConflict { xid: u64 },
    /// An existing xid has advanced past the begin phase.
    #[error("local coordinator transaction {xid} is already {phase:?}")]
    ExistingTransactionAdvanced { xid: u64, phase: TransactionPhase },
}

impl LocalCoordinator {
    /// Begin a process-local global transaction and return its xid.
    pub async fn begin(&self, participants: Vec<RangeId>) -> u64 {
        let mut state = self.state.lock().await;
        let xid = state.next_xid();
        state.insert_begun(xid, participants);
        xid
    }

    /// Begin a process-local coordinator record for an xid allocated by range 0's GTM.
    pub async fn begin_existing_xid(
        &self,
        xid: u64,
        participants: Vec<RangeId>,
    ) -> Result<(), LocalCoordinatorError> {
        let mut state = self.state.lock().await;
        state.next_id = state.next_id.max(xid);
        state.insert_existing_begun(xid, participants)
    }

    /// Record that one participant reached the prepared phase.
    pub async fn prepare(&self, xid: u64, range_id: RangeId) -> Result<(), LocalCoordinatorError> {
        let mut state = self.state.lock().await;
        let record = state.record_mut(xid)?;

        if let Some(existing) = record.decision {
            return Err(LocalCoordinatorError::DecisionAlreadyFinal { xid, existing });
        }
        if !record.participants.contains(&range_id) {
            return Err(LocalCoordinatorError::UnknownParticipant { xid, range_id });
        }
        if record.is_participant_prepared(range_id) {
            return Ok(());
        }

        record.prepared.push(PreparedParticipant { range_id });
        if record.all_participants_prepared() {
            record.phase = TransactionPhase::Prepared;
        }
        Ok(())
    }

    /// Record a terminal decision for an already-begun local transaction.
    pub async fn decide_prepared(
        &self,
        xid: u64,
        decision: TransactionDecision,
    ) -> Result<TransactionDecision, LocalCoordinatorError> {
        let mut state = self.state.lock().await;
        let record = state.record_mut(xid)?;

        if let Some(existing) = record.decision {
            if existing == decision {
                return Ok(existing);
            }
            return Err(LocalCoordinatorError::DecisionAlreadyFinal { xid, existing });
        }
        if decision == TransactionDecision::Commit && !record.all_participants_prepared() {
            return Err(LocalCoordinatorError::CommitBeforePrepared { xid });
        }

        record.phase = TransactionPhase::Decided;
        record.decision = Some(decision);
        Ok(decision)
    }

    #[cfg(test)]
    pub(crate) async fn commit(
        &self,
        ranges: Vec<RangeId>,
        escalated: bool,
    ) -> TransactionDecision {
        if !escalated {
            return self
                .record_local_decision(ranges, TransactionDecision::Commit)
                .await;
        }

        let xid = self.begin(ranges.clone()).await;
        for range_id in ranges {
            self.prepare(xid, range_id)
                .await
                .expect("participant came from local coordinator begin record");
        }
        self.decide_prepared(xid, TransactionDecision::Commit)
            .await
            .expect("all local coordinator participants were prepared")
    }

    pub(crate) async fn abort(&self, ranges: Vec<RangeId>, escalated: bool) -> TransactionDecision {
        if !escalated {
            return self
                .record_local_decision(ranges, TransactionDecision::Abort)
                .await;
        }

        let xid = self.begin(ranges).await;
        self.decide_prepared(xid, TransactionDecision::Abort)
            .await
            .expect("abort is valid before every participant prepares")
    }

    /// Snapshot coordinator records for tests and gateway invariants.
    pub async fn records(&self) -> Vec<LocalTransactionRecord> {
        self.state
            .lock()
            .await
            .transactions
            .values()
            .cloned()
            .collect()
    }

    /// Snapshot coordinator phases for tests.
    pub async fn phases(&self) -> Vec<TransactionPhase> {
        self.records()
            .await
            .into_iter()
            .map(|record| record.phase)
            .collect()
    }

    async fn record_local_decision(
        &self,
        ranges: Vec<RangeId>,
        decision: TransactionDecision,
    ) -> TransactionDecision {
        let mut state = self.state.lock().await;
        let xid = state.next_xid();
        state.transactions.insert(
            xid,
            LocalTransactionRecord {
                xid,
                participants: ranges,
                phase: TransactionPhase::Local,
                prepared: Vec::new(),
                decision: Some(decision),
            },
        );
        decision
    }
}

impl CoordinatorState {
    fn next_xid(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }

    fn record_mut(
        &mut self,
        xid: u64,
    ) -> Result<&mut LocalTransactionRecord, LocalCoordinatorError> {
        self.transactions
            .get_mut(&xid)
            .ok_or(LocalCoordinatorError::UnknownTransaction { xid })
    }

    fn insert_begun(&mut self, xid: u64, participants: Vec<RangeId>) {
        self.transactions.insert(
            xid,
            LocalTransactionRecord {
                xid,
                participants,
                phase: TransactionPhase::Begun,
                prepared: Vec::new(),
                decision: None,
            },
        );
    }

    fn insert_existing_begun(
        &mut self,
        xid: u64,
        participants: Vec<RangeId>,
    ) -> Result<(), LocalCoordinatorError> {
        let Some(existing) = self.transactions.get(&xid) else {
            self.insert_begun(xid, participants);
            return Ok(());
        };

        if existing.phase != TransactionPhase::Begun
            || !existing.prepared.is_empty()
            || existing.decision.is_some()
        {
            return Err(LocalCoordinatorError::ExistingTransactionAdvanced {
                xid,
                phase: existing.phase,
            });
        }
        if existing.participants != participants {
            return Err(LocalCoordinatorError::ExistingTransactionConflict { xid });
        }
        Ok(())
    }
}

impl LocalTransactionRecord {
    fn is_participant_prepared(&self, range_id: RangeId) -> bool {
        self.prepared
            .iter()
            .any(|participant| participant.range_id == range_id)
    }

    fn all_participants_prepared(&self) -> bool {
        self.participants
            .iter()
            .copied()
            .all(|range_id| self.is_participant_prepared(range_id))
    }
}

/// Transaction RPC client for one registry-discovered range.
#[derive(Debug, Clone)]
pub struct TxnRpc {
    registry: RangeRegistry,
    client: FramedTcpClient,
}

impl TxnRpc {
    /// Build an RPC client with an injected authenticated range client.
    #[must_use]
    pub const fn new(registry: RangeRegistry, client: FramedTcpClient) -> Self {
        Self { registry, client }
    }

    /// Send one transaction RPC to the range endpoint from registry discovery.
    pub async fn call(&self, range_id: RangeId, request: TxnReq) -> Result<TxnResp, TxnRpcError> {
        let endpoint = self.registry.resolve(range_id).await?;
        match self
            .client
            .call(&endpoint.endpoint, &RangeRequest::Txn(request))
            .await?
        {
            RangeResponse::Txn(response) => Ok(response),
            RangeResponse::Error { error, message } => Err(TxnRpcError::Remote {
                kind: error,
                message,
            }),
            RangeResponse::Sql { .. }
            | RangeResponse::SqlResults { .. }
            | RangeResponse::SqlResultsChunk { .. }
            | RangeResponse::SqlResultsDone
            | RangeResponse::SqlError { .. }
            | RangeResponse::ScanRange(_)
            | RangeResponse::ScanRangeError { .. }
            | RangeResponse::Tso(_)
            | RangeResponse::ResolveTxn(_) => Err(TxnRpcError::UnexpectedResponse),
        }
    }
}

/// Two-phase commit coordinator using [`TxnRpc`].
#[derive(Debug, Clone)]
pub struct NetCoordinator {
    rpc: TxnRpc,
}

impl NetCoordinator {
    /// Build a network coordinator from a transaction RPC client.
    #[must_use]
    pub const fn new(rpc: TxnRpc) -> Self {
        Self { rpc }
    }

    /// Prepare every participant, then commit if all prepared or abort otherwise.
    pub async fn commit(&self, gtid: u64, ranges: &[RangeId]) -> Result<NetDecision, TxnRpcError> {
        if ranges.is_empty() {
            return Ok(NetDecision::Commit);
        }

        let mut prepared = Vec::with_capacity(ranges.len());
        for range_id in ranges.iter().copied() {
            match self
                .rpc
                .call(range_id, TxnReq::Prepare { gtid, range_id })
                .await?
            {
                TxnResp::Prepared => prepared.push(range_id),
                TxnResp::Aborted => {
                    self.abort_prepared(gtid, &prepared).await?;
                    return Ok(NetDecision::Abort);
                }
                TxnResp::Committed | TxnResp::Barrier { .. } => {
                    self.abort_prepared(gtid, &prepared).await?;
                    return Err(TxnRpcError::UnexpectedResponse);
                }
            }
        }

        for range_id in prepared {
            match self
                .rpc
                .call(range_id, TxnReq::Commit { gtid, range_id })
                .await?
            {
                TxnResp::Committed => {}
                TxnResp::Prepared | TxnResp::Aborted | TxnResp::Barrier { .. } => {
                    return Err(TxnRpcError::UnexpectedResponse);
                }
            }
        }
        Ok(NetDecision::Commit)
    }

    async fn abort_prepared(&self, gtid: u64, prepared: &[RangeId]) -> Result<(), TxnRpcError> {
        for range_id in prepared.iter().copied() {
            match self
                .rpc
                .call(range_id, TxnReq::Abort { gtid, range_id })
                .await?
            {
                TxnResp::Aborted => {}
                TxnResp::Prepared | TxnResp::Committed | TxnResp::Barrier { .. } => {
                    return Err(TxnRpcError::UnexpectedResponse);
                }
            }
        }
        Ok(())
    }
}

/// Terminal network 2PC decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetDecision {
    /// All participants committed.
    Commit,
    /// At least one participant refused prepare; prepared participants were aborted.
    Abort,
}

/// Transaction RPC failure.
#[derive(Debug, thiserror::Error)]
pub enum TxnRpcError {
    /// Discovery failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// Transport failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Remote endpoint returned an error frame.
    #[error("remote transaction rpc returned {kind:?}: {message}")]
    Remote {
        kind: crate::transport::WireErrorKind,
        message: String,
    },
    /// Response variant did not match the request.
    #[error("remote transaction rpc returned an unexpected response")]
    UnexpectedResponse,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use crabka_gres_control::{
        RangeLayoutEntry, SqlUser, TenantId, TenantName, TenantRecord, TenantState,
    };

    use super::*;
    use crate::transport::{RangeService, spawn_loopback};

    struct Participant {
        abort_prepare: bool,
        prepares: AtomicUsize,
        commits: AtomicUsize,
        aborts: AtomicUsize,
        barrier_offset: i64,
        saw_substrate_barrier: AtomicBool,
    }

    #[async_trait]
    impl RangeService for Participant {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            let RangeRequest::Txn(request) = request else {
                return RangeResponse::Error {
                    error: crate::transport::WireErrorKind::Failed,
                    message: "expected txn".to_string(),
                };
            };
            match request {
                TxnReq::Prepare { .. } if self.abort_prepare => {
                    RangeResponse::Txn(TxnResp::Aborted)
                }
                TxnReq::Prepare { .. } => {
                    self.prepares.fetch_add(1, Ordering::SeqCst);
                    RangeResponse::Txn(TxnResp::Prepared)
                }
                TxnReq::Commit { .. } => {
                    self.commits.fetch_add(1, Ordering::SeqCst);
                    RangeResponse::Txn(TxnResp::Committed)
                }
                TxnReq::Abort { .. } => {
                    self.aborts.fetch_add(1, Ordering::SeqCst);
                    RangeResponse::Txn(TxnResp::Aborted)
                }
                TxnReq::Barrier { .. } => {
                    self.saw_substrate_barrier.store(true, Ordering::SeqCst);
                    RangeResponse::Txn(TxnResp::Barrier {
                        substrate_offset: self.barrier_offset,
                    })
                }
            }
        }
    }

    impl Participant {
        fn happy(offset: i64) -> Arc<Self> {
            Arc::new(Self {
                abort_prepare: false,
                prepares: AtomicUsize::new(0),
                commits: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
                barrier_offset: offset,
                saw_substrate_barrier: AtomicBool::new(false),
            })
        }

        fn aborting() -> Arc<Self> {
            Arc::new(Self {
                abort_prepare: true,
                prepares: AtomicUsize::new(0),
                commits: AtomicUsize::new(0),
                aborts: AtomicUsize::new(0),
                barrier_offset: 0,
                saw_substrate_barrier: AtomicBool::new(false),
            })
        }
    }

    async fn registry_for(participants: &[(RangeId, Arc<Participant>)]) -> RangeRegistry {
        let mut ranges = Vec::with_capacity(participants.len());
        for (index, (range_id, service)) in participants.iter().enumerate() {
            let service: Arc<dyn RangeService> = Arc::clone(service) as Arc<dyn RangeService>;
            let addr = spawn_loopback(service).await.unwrap();
            ranges.push(RangeLayoutEntry {
                range_id: range_id.as_u32(),
                end_key: (index + 1 != participants.len()).then(|| {
                    crabka_gres_control::RangeBoundary::table_start((index as u64 + 1) * 10)
                }),
                endpoint: addr.to_string(),
                wal_generation: 1,
            });
        }
        let record = TenantRecord::new(
            1,
            TenantId::try_from("tenant-a").unwrap(),
            TenantName::try_from("tenant-a").unwrap(),
            TenantState::Active,
            SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .unwrap()
        .with_range_layout(ranges)
        .unwrap();
        RangeRegistry::from_tenant_record(&record).unwrap()
    }

    #[tokio::test]
    async fn cross_range_2pc_net_happy_path_commits_all_participants() {
        let r1 = Participant::happy(10);
        let r2 = Participant::happy(20);
        let registry =
            registry_for(&[(RangeId::new(1), r1.clone()), (RangeId::new(2), r2.clone())]).await;
        let coordinator = NetCoordinator::new(TxnRpc::new(registry, FramedTcpClient::default()));

        let decision = coordinator
            .commit(7, &[RangeId::new(1), RangeId::new(2)])
            .await
            .unwrap();

        assert_eq!(decision, NetDecision::Commit);
        assert_eq!(r1.prepares.load(Ordering::SeqCst), 1);
        assert_eq!(r2.commits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cross_range_2pc_net_abort_path_aborts_prepared_participants() {
        let r1 = Participant::happy(10);
        let r2 = Participant::aborting();
        let registry = registry_for(&[(RangeId::new(1), r1.clone()), (RangeId::new(2), r2)]).await;
        let coordinator = NetCoordinator::new(TxnRpc::new(registry, FramedTcpClient::default()));

        let decision = coordinator
            .commit(8, &[RangeId::new(1), RangeId::new(2)])
            .await
            .unwrap();

        assert_eq!(decision, NetDecision::Abort);
        assert_eq!(r1.aborts.load(Ordering::SeqCst), 1);
        assert_eq!(r1.commits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn txn_rpc_barrier_carries_substrate_offset() {
        let r0 = Participant::happy(123);
        let registry = registry_for(&[(RangeId::COORDINATOR, r0.clone())]).await;
        let rpc = TxnRpc::new(registry, FramedTcpClient::default());

        let response = rpc
            .call(
                RangeId::COORDINATOR,
                TxnReq::Barrier {
                    range_id: RangeId::COORDINATOR,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            TxnResp::Barrier {
                substrate_offset: 123
            }
        );
        assert!(r0.saw_substrate_barrier.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn local_2pc_commit_keeps_prepare_records_before_final_decision() {
        let coordinator = LocalCoordinator::default();

        let decision = coordinator
            .commit(vec![RangeId::new(1), RangeId::new(2)], true)
            .await;

        assert_eq!(decision, TransactionDecision::Commit);
        let records = coordinator.records().await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].phase, TransactionPhase::Decided);
        assert_eq!(records[0].decision, Some(TransactionDecision::Commit));
        assert_eq!(
            records[0].participants,
            vec![RangeId::new(1), RangeId::new(2)]
        );
        assert_eq!(
            records[0].prepared,
            vec![
                PreparedParticipant {
                    range_id: RangeId::new(1),
                },
                PreparedParticipant {
                    range_id: RangeId::new(2),
                },
            ]
        );
    }

    #[tokio::test]
    async fn local_2pc_abort_records_abort_after_partial_prepare() {
        let coordinator = LocalCoordinator::default();
        let xid = coordinator
            .begin(vec![RangeId::new(1), RangeId::new(2)])
            .await;
        coordinator.prepare(xid, RangeId::new(1)).await.unwrap();

        let decision = coordinator
            .decide_prepared(xid, TransactionDecision::Abort)
            .await
            .unwrap();

        assert_eq!(decision, TransactionDecision::Abort);
        let records = coordinator.records().await;
        assert_eq!(records[0].phase, TransactionPhase::Decided);
        assert_eq!(records[0].decision, Some(TransactionDecision::Abort));
        assert_eq!(
            records[0].prepared,
            vec![PreparedParticipant {
                range_id: RangeId::new(1),
            }]
        );
    }

    #[tokio::test]
    async fn local_2pc_commit_before_all_participants_prepare_is_rejected() {
        let coordinator = LocalCoordinator::default();
        let xid = coordinator
            .begin(vec![RangeId::new(1), RangeId::new(2)])
            .await;
        coordinator.prepare(xid, RangeId::new(1)).await.unwrap();

        let error = coordinator
            .decide_prepared(xid, TransactionDecision::Commit)
            .await
            .expect_err("commit before prepare rejected");

        assert!(matches!(
            error,
            LocalCoordinatorError::CommitBeforePrepared { .. }
        ));
        let records = coordinator.records().await;
        assert_eq!(records[0].phase, TransactionPhase::Begun);
        assert_eq!(records[0].decision, None);
    }

    #[tokio::test]
    async fn local_2pc_duplicate_prepare_is_idempotent() {
        let coordinator = LocalCoordinator::default();
        let xid = coordinator.begin(vec![RangeId::new(1)]).await;

        coordinator.prepare(xid, RangeId::new(1)).await.unwrap();
        coordinator.prepare(xid, RangeId::new(1)).await.unwrap();

        let records = coordinator.records().await;
        assert_eq!(records[0].phase, TransactionPhase::Prepared);
        assert_eq!(
            records[0].prepared,
            vec![PreparedParticipant {
                range_id: RangeId::new(1),
            }]
        );
    }

    #[tokio::test]
    async fn local_2pc_begin_existing_xid_is_idempotent_for_matching_begun_record() {
        let coordinator = LocalCoordinator::default();
        let participants = vec![RangeId::new(1), RangeId::new(2)];

        coordinator
            .begin_existing_xid(42, participants.clone())
            .await
            .unwrap();
        coordinator
            .begin_existing_xid(42, participants.clone())
            .await
            .unwrap();

        let records = coordinator.records().await;
        assert_eq!(
            records,
            vec![LocalTransactionRecord {
                xid: 42,
                participants,
                phase: TransactionPhase::Begun,
                prepared: Vec::new(),
                decision: None,
            }]
        );
    }

    #[tokio::test]
    async fn local_2pc_begin_existing_xid_rejects_conflicting_participants() {
        let coordinator = LocalCoordinator::default();
        coordinator
            .begin_existing_xid(42, vec![RangeId::new(1)])
            .await
            .unwrap();

        let error = coordinator
            .begin_existing_xid(42, vec![RangeId::new(2)])
            .await
            .expect_err("conflicting begin rejected");

        assert!(matches!(
            error,
            LocalCoordinatorError::ExistingTransactionConflict { xid: 42 }
        ));
        let records = coordinator.records().await;
        assert_eq!(records[0].participants, vec![RangeId::new(1)]);
    }

    #[tokio::test]
    async fn local_2pc_begin_existing_xid_does_not_erase_prepared_or_decided_records() {
        let coordinator = LocalCoordinator::default();
        coordinator
            .begin_existing_xid(42, vec![RangeId::new(1)])
            .await
            .unwrap();
        coordinator.prepare(42, RangeId::new(1)).await.unwrap();

        let prepared_error = coordinator
            .begin_existing_xid(42, vec![RangeId::new(1)])
            .await
            .expect_err("prepared record is immutable");
        coordinator
            .decide_prepared(42, TransactionDecision::Commit)
            .await
            .unwrap();
        let decided_error = coordinator
            .begin_existing_xid(42, vec![RangeId::new(1)])
            .await
            .expect_err("decided record is immutable");

        assert!(matches!(
            prepared_error,
            LocalCoordinatorError::ExistingTransactionAdvanced {
                phase: TransactionPhase::Prepared,
                ..
            }
        ));
        assert!(matches!(
            decided_error,
            LocalCoordinatorError::ExistingTransactionAdvanced {
                phase: TransactionPhase::Decided,
                ..
            }
        ));
        let records = coordinator.records().await;
        assert_eq!(records[0].decision, Some(TransactionDecision::Commit));
    }

    #[tokio::test]
    async fn local_2pc_final_decision_is_immutable() {
        let coordinator = LocalCoordinator::default();
        let xid = coordinator.begin(vec![RangeId::new(1)]).await;
        coordinator.prepare(xid, RangeId::new(1)).await.unwrap();
        coordinator
            .decide_prepared(xid, TransactionDecision::Commit)
            .await
            .unwrap();

        let repeated = coordinator
            .decide_prepared(xid, TransactionDecision::Commit)
            .await
            .unwrap();
        let changed = coordinator
            .decide_prepared(xid, TransactionDecision::Abort)
            .await;

        assert_eq!(repeated, TransactionDecision::Commit);
        assert!(matches!(
            changed,
            Err(LocalCoordinatorError::DecisionAlreadyFinal {
                existing: TransactionDecision::Commit,
                ..
            })
        ));
        let records = coordinator.records().await;
        assert_eq!(records[0].decision, Some(TransactionDecision::Commit));
    }
}
