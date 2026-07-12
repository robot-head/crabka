//! Registry-backed remote SQL forwarding with bounded stale-endpoint retry.

use std::{
    collections::BTreeMap,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use crabka_pgwire::{
    engine::{
        BoundParam, Cell, CloseTarget, Engine as _, ExecuteOutcome, FieldDescription, QueryResult,
        ResultPage, ResultSink, Session as _, TxStatus,
    },
    error::PgError,
};

use crate::{
    RangeId,
    registry::{RangeRegistry, RegistryError},
    transport::{
        ExplicitGateReq, ExplicitGateResp, FramedTcpClient, RangeRequest, RangeResponse,
        RangeService, ResolveTxnResp, ScanCursorReq, ScanCursorResp, ScanRangeReq, ScanRangeResp,
        ScanRangeRow, TransportError, WireColumnPredicate, WireDatum, WireErrorKind,
        WireExecuteOutcome, WireGlobalStatus, WirePartialAggregateFunction,
        WirePartialAggregateSpec, WirePredicateOp, WirePredicatePushdown, WireProjectionPushdown,
        WireQueryResult, WireRowInterval, WireSessionOperation, WireSessionResult, WireSnapshot,
        WireSqlResultChunk, WireTopKColumn, WireTopKSpec, write_frame,
    },
    tso::{GrantLease, TsoError, TsoRpc},
};

/// Production handler for ranges hosted by one compute process.
///
/// Every request is checked against the hosted-engine map before execution;
/// therefore a stale registry entry is visible to callers instead of being
/// accidentally served from another range's state.
pub struct HostedRangeService {
    engines: BTreeMap<RangeId, crabka_pgexec::SqlEngine>,
    tso: Option<Arc<dyn TsoRpc>>,
    timestamp_primary_remote: Option<(RangeRegistry, FramedTcpClient)>,
    next_session_id: AtomicU64,
    sessions: tokio::sync::Mutex<BTreeMap<u64, HostedSession>>,
    explicit_gate: Arc<ExplicitGate>,
}

struct ExplicitGate {
    state: tokio::sync::Mutex<Option<ExplicitGateOwner>>,
    changed: tokio::sync::Notify,
    next_token: AtomicU64,
}

struct ExplicitGateOwner {
    token: u64,
    deadline: Instant,
}

struct HostedSession {
    range_id: RangeId,
    session: crabka_pgexec::SqlSession,
    last_used: Instant,
}

const REMOTE_SESSION_IDLE: Duration = Duration::from_secs(60);
const MAX_REMOTE_SESSIONS: usize = 1024;
#[cfg(not(test))]
const EXPLICIT_GATE_LEASE: Duration = Duration::from_secs(2);
#[cfg(test)]
const EXPLICIT_GATE_LEASE: Duration = Duration::from_millis(100);

impl HostedRangeService {
    /// Build a hosted range service. Only range 0 may be given a TSO RPC.
    #[must_use]
    pub fn new(engines: BTreeMap<RangeId, crabka_pgexec::SqlEngine>) -> Self {
        Self {
            engines,
            tso: None,
            timestamp_primary_remote: None,
            next_session_id: AtomicU64::new(1),
            sessions: tokio::sync::Mutex::new(BTreeMap::new()),
            explicit_gate: Arc::new(ExplicitGate {
                state: tokio::sync::Mutex::new(None),
                changed: tokio::sync::Notify::new(),
                next_token: AtomicU64::new(1),
            }),
        }
    }

    #[must_use]
    pub fn with_timestamp_primary_remote(
        mut self,
        registry: RangeRegistry,
        client: FramedTcpClient,
    ) -> Self {
        self.timestamp_primary_remote = Some((registry, client));
        self
    }

    /// Attach range 0's durable timestamp oracle RPC.
    #[must_use]
    pub fn with_tso(mut self, tso: Arc<dyn TsoRpc>) -> Self {
        self.tso = Some(tso);
        self
    }

    fn hosted_engine(&self, range_id: RangeId) -> Result<&crabka_pgexec::SqlEngine, RangeResponse> {
        self.engines
            .get(&range_id)
            .ok_or_else(|| RangeResponse::Error {
                error: WireErrorKind::StaleEndpoint,
                message: format!("range r{range_id} is not hosted here"),
            })
    }

    async fn handle_explicit_gate(&self, request: ExplicitGateReq) -> ExplicitGateResp {
        match request {
            ExplicitGateReq::Acquire => loop {
                let notified = self.explicit_gate.changed.notified();
                let wait = {
                    let mut state = self.explicit_gate.state.lock().await;
                    let now = Instant::now();
                    if state.as_ref().is_some_and(|owner| owner.deadline <= now) {
                        *state = None;
                    }
                    if state.is_none() {
                        let token = self
                            .explicit_gate
                            .next_token
                            .fetch_add(1, Ordering::Relaxed);
                        *state = Some(ExplicitGateOwner {
                            token,
                            deadline: now + EXPLICIT_GATE_LEASE,
                        });
                        return ExplicitGateResp::Acquired {
                            token,
                            lease_millis: EXPLICIT_GATE_LEASE.as_millis() as u64,
                        };
                    }
                    state
                        .as_ref()
                        .expect("checked occupied")
                        .deadline
                        .saturating_duration_since(now)
                };
                tokio::select! {
                    () = notified => {}
                    () = tokio::time::sleep(wait) => {}
                }
            },
            ExplicitGateReq::Renew { token } => {
                let mut state = self.explicit_gate.state.lock().await;
                let now = Instant::now();
                let Some(owner) = state.as_mut() else {
                    return ExplicitGateResp::Stale;
                };
                if owner.token != token || owner.deadline <= now {
                    if owner.deadline <= now {
                        *state = None;
                        self.explicit_gate.changed.notify_waiters();
                    }
                    return ExplicitGateResp::Stale;
                }
                owner.deadline = now + EXPLICIT_GATE_LEASE;
                ExplicitGateResp::Renewed {
                    lease_millis: EXPLICIT_GATE_LEASE.as_millis() as u64,
                }
            }
            ExplicitGateReq::Release { token } => {
                let mut state = self.explicit_gate.state.lock().await;
                if state.as_ref().is_some_and(|owner| owner.token == token) {
                    *state = None;
                    self.explicit_gate.changed.notify_waiters();
                    ExplicitGateResp::Released
                } else {
                    ExplicitGateResp::Stale
                }
            }
        }
    }

    async fn authenticated_primary_outcome(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
    ) -> Result<
        (
            crabka_pgexec::PrimaryTxnDecision,
            Vec<crabka_pgexec::TimestampTxnOperation>,
        ),
        crabka_pgexec::ExecError,
    > {
        let primary_range = RangeId::new(identity.primary_range);
        if let Some(primary) = self.engines.get(&primary_range) {
            let descriptor = primary.validate_timestamp_primary_identity(identity)?;
            return Ok((descriptor.decision, descriptor.operations));
        }
        let (registry, client) = self.timestamp_primary_remote.as_ref().ok_or_else(|| {
            crabka_pgexec::ExecError::Unsupported(
                "timestamp primary cannot be authenticated from this range service".into(),
            )
        })?;
        let endpoint = registry
            .resolve(primary_range)
            .await
            .map_err(|error| crabka_pgexec::ExecError::Unsupported(error.to_string()))?;
        let request =
            RangeRequest::TimestampPrimaryInspect(crate::transport::TimestampPrimaryRecoverReq {
                primary_range,
                identity: encode_timestamp_identity(identity),
            });
        match client
            .call(&endpoint.endpoint, &request)
            .await
            .map_err(|error| crabka_pgexec::ExecError::Unsupported(error.to_string()))?
        {
            RangeResponse::TimestampPrimaryOutcome {
                decision,
                operations,
            } => {
                let decision = match decision {
                    crate::transport::WirePrimaryTxnDecision::Pending => {
                        crabka_pgexec::PrimaryTxnDecision::Pending
                    }
                    crate::transport::WirePrimaryTxnDecision::Aborted => {
                        crabka_pgexec::PrimaryTxnDecision::Aborted
                    }
                    crate::transport::WirePrimaryTxnDecision::Committed { commit_ts } => {
                        crabka_pgexec::PrimaryTxnDecision::Committed(
                            crabka_pgexec::CommitTimestamp::new(commit_ts).map_err(|error| {
                                crabka_pgexec::ExecError::Unsupported(error.to_string())
                            })?,
                        )
                    }
                };
                Ok((
                    decision,
                    operations
                        .into_iter()
                        .map(|operation| crabka_pgexec::TimestampTxnOperation {
                            range_id: operation.range_id,
                            table_id: operation.table_id,
                            rowid: operation.rowid,
                            delete: operation.delete,
                        })
                        .collect(),
                ))
            }
            RangeResponse::SqlError { message, .. } | RangeResponse::Error { message, .. } => {
                Err(crabka_pgexec::ExecError::Unsupported(message))
            }
            _ => Err(crabka_pgexec::ExecError::Unsupported(
                "unexpected timestamp primary authentication response".into(),
            )),
        }
    }
}

#[async_trait]
impl RangeService for HostedRangeService {
    async fn handle(&self, request: RangeRequest) -> RangeResponse {
        match request {
            RangeRequest::SessionOpen { range_id } => {
                let engine = match self.hosted_engine(range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let mut sessions = self.sessions.lock().await;
                let now = Instant::now();
                sessions
                    .retain(|_, lease| now.duration_since(lease.last_used) < REMOTE_SESSION_IDLE);
                if sessions.len() >= MAX_REMOTE_SESSIONS {
                    return RangeResponse::SqlError {
                        code: "53300".into(),
                        message: "too many remote range sessions".into(),
                    };
                }
                let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
                sessions.insert(
                    session_id,
                    HostedSession {
                        range_id,
                        session: engine.connect(),
                        last_used: now,
                    },
                );
                RangeResponse::SessionOpened { session_id }
            }
            RangeRequest::Session {
                range_id,
                session_id,
                operation,
            } => {
                let mut lease = {
                    let mut sessions = self.sessions.lock().await;
                    sessions.remove(&session_id)
                };
                let Some(mut lease) = lease.take() else {
                    return RangeResponse::SqlError {
                        code: "08003".into(),
                        message: "remote range session does not exist".into(),
                    };
                };
                if lease.range_id != range_id {
                    return RangeResponse::SqlError {
                        code: "08003".into(),
                        message: "remote range session belongs to another range".into(),
                    };
                }
                let result = handle_session_operation(&mut lease.session, operation).await;
                lease.last_used = Instant::now();
                self.sessions.lock().await.insert(session_id, lease);
                match result {
                    Ok(result) => RangeResponse::SessionResult { result },
                    Err(error) => RangeResponse::SqlError {
                        code: error.code,
                        message: error.message,
                    },
                }
            }
            RangeRequest::SessionClose {
                range_id,
                session_id,
            } => {
                let removed = self.sessions.lock().await.remove(&session_id);
                if removed
                    .as_ref()
                    .is_some_and(|lease| lease.range_id != range_id)
                {
                    return RangeResponse::SqlError {
                        code: "08003".into(),
                        message: "remote range session belongs to another range".into(),
                    };
                }
                RangeResponse::SessionResult {
                    result: WireSessionResult::Closed,
                }
            }
            RangeRequest::GlobalDecision {
                range_id,
                global_xid,
                status,
            } => {
                let engine = match self.hosted_engine(range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let status = match decode_global_status(status) {
                    Ok(status) => status,
                    Err(error) => {
                        return RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        };
                    }
                };
                match engine.commit_global_decision(global_xid, status).await {
                    Ok(status) => RangeResponse::GlobalStatus {
                        status: encode_global_status(status),
                    },
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::GlobalBegin { range_id } => {
                let engine = match self.hosted_engine(range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                match engine.begin_global_durable().await {
                    Ok(global_xid) => RangeResponse::GlobalXid { global_xid },
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::ExplicitGate(request) => {
                if let Err(response) = self.hosted_engine(RangeId::COORDINATOR) {
                    return response;
                }
                RangeResponse::ExplicitGate(self.handle_explicit_gate(request).await)
            }
            RangeRequest::RecoverGlobal {
                range_id,
                global_xid,
                commit,
            } => {
                if let Err(response) = self.hosted_engine(range_id) {
                    return response;
                }
                let session_ids: Vec<_> = self.sessions.lock().await.keys().copied().collect();
                for session_id in session_ids {
                    let Some(mut lease) = self.sessions.lock().await.remove(&session_id) else {
                        continue;
                    };
                    if lease.range_id == range_id
                        && lease.session.prepared_global_xid() == Some(global_xid)
                    {
                        let result = if commit {
                            lease
                                .session
                                .release_global_participant_commit(global_xid)
                                .await
                        } else {
                            lease
                                .session
                                .release_global_participant_abort(global_xid)
                                .await
                        };
                        if let Err(error) = result {
                            self.sessions.lock().await.insert(session_id, lease);
                            let error = error.into_pg();
                            return RangeResponse::SqlError {
                                code: error.code,
                                message: error.message,
                            };
                        }
                    }
                    self.sessions.lock().await.insert(session_id, lease);
                }
                RangeResponse::GlobalRecovered
            }
            RangeRequest::Sql { range_id, sql } => {
                let engine = match self.hosted_engine(range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                match engine.connect().simple_query(&sql).await {
                    Ok(results) => RangeResponse::SqlResults {
                        results: results.into_iter().map(WireQueryResult::from).collect(),
                    },
                    Err(error) => RangeResponse::SqlError {
                        code: error.code,
                        message: error.message,
                    },
                }
            }
            RangeRequest::ScanRange(request) => {
                let engine = match self.hosted_engine(request.range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                match handle_scan_range(engine, request) {
                    Ok(response) => RangeResponse::ScanRange(response),
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::ScanRangeError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::ScanCursor(request) => {
                let engine = match self.hosted_engine(request.scan.range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                match handle_scan_cursor(engine, request) {
                    Ok(response) => RangeResponse::ScanCursor(response),
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::ScanRangeError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::ResolveTxn(request) => {
                let engine = match self.hosted_engine(request.primary_range) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                match resolve_primary(engine, request.start_ts) {
                    Ok(response) => RangeResponse::ResolveTxn(response),
                    Err(error) => RangeResponse::Error {
                        error: WireErrorKind::Failed,
                        message: error.into_pg().message,
                    },
                }
            }
            RangeRequest::TimestampPrewrite(request) => {
                let engine = match self.hosted_engine(request.range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let identity = match decode_timestamp_identity(request.identity) {
                    Ok(value) => value,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let writes = request
                    .writes
                    .into_iter()
                    .map(decode_timestamp_write)
                    .collect::<Result<Vec<_>, _>>();
                let writes = match writes {
                    Ok(value) => value,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let participant = engine.timestamp_txn_participant(request.range_id.as_u32());
                let result = if request.existing_primary {
                    participant.prewrite_on_primary(identity, &writes).await
                } else if request.secondary {
                    participant.prewrite_as_secondary(identity, &writes).await
                } else if request.primary_participants.is_empty() {
                    participant.prewrite_with_primary(identity, &writes).await
                } else {
                    participant
                        .prewrite_as_primary(identity, &request.primary_participants, &writes)
                        .await
                };
                match result {
                    Ok(()) => RangeResponse::TimestampParticipantDone,
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::TimestampPrimaryAck(request) => {
                let engine = match self.hosted_engine(request.primary_range) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let identity = match decode_timestamp_identity(request.identity) {
                    Ok(identity) => identity,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                if identity.primary_range != request.primary_range.as_u32() {
                    return RangeResponse::SqlError {
                        code: "40001".into(),
                        message: "timestamp primary identity mismatch".into(),
                    };
                }
                if engine
                    .validate_timestamp_primary_identity(identity)
                    .is_err()
                {
                    return RangeResponse::SqlError {
                        code: "40001".into(),
                        message: "timestamp primary identity is fenced".into(),
                    };
                }
                let operations = request
                    .operations
                    .into_iter()
                    .map(|op| crabka_pgexec::TimestampTxnOperation {
                        range_id: op.range_id,
                        table_id: op.table_id,
                        rowid: op.rowid,
                        delete: op.delete,
                    })
                    .collect::<Vec<_>>();
                let result = if request.add_participant {
                    engine
                        .add_timestamp_transaction_participant(
                            identity.start_ts,
                            request.participant_range,
                        )
                        .await
                } else {
                    engine
                        .acknowledge_timestamp_participant_operations(
                            identity.start_ts,
                            request.participant_range,
                            &operations,
                        )
                        .await
                };
                match result {
                    Ok(_) => RangeResponse::TimestampParticipantDone,
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::TimestampResolve(request) => {
                let engine = match self.hosted_engine(request.range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let identity = match decode_timestamp_identity(request.identity) {
                    Ok(value) => value,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let decision = match request.decision {
                    crate::transport::WireTimestampDecision::Aborted => {
                        crabka_pgexec::TimestampTxnDecision::Aborted
                    }
                    crate::transport::WireTimestampDecision::Committed { commit_ts } => {
                        match crabka_pgexec::CommitTimestamp::new(commit_ts) {
                            Ok(ts) => crabka_pgexec::TimestampTxnDecision::Committed(ts),
                            Err(error) => {
                                return RangeResponse::SqlError {
                                    code: "22023".into(),
                                    message: error.to_string(),
                                };
                            }
                        }
                    }
                };
                let writes = request
                    .writes
                    .into_iter()
                    .map(decode_timestamp_write)
                    .collect::<Result<Vec<_>, _>>();
                let writes = match writes {
                    Ok(value) => value,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let participant = engine.timestamp_txn_participant(request.range_id.as_u32());
                let result = if identity.primary_range == request.range_id.as_u32() {
                    participant
                        .resolve_as_primary(identity, decision, &writes)
                        .await
                } else {
                    let (expected, primary_operations) =
                        match self.authenticated_primary_outcome(identity).await {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                let error = error.into_pg();
                                return RangeResponse::SqlError {
                                    code: error.code,
                                    message: error.message,
                                };
                            }
                        };
                    let requested = match decision {
                        crabka_pgexec::TimestampTxnDecision::Aborted => {
                            crabka_pgexec::PrimaryTxnDecision::Aborted
                        }
                        crabka_pgexec::TimestampTxnDecision::Committed(ts)
                        | crabka_pgexec::TimestampTxnDecision::Deleted(ts) => {
                            crabka_pgexec::PrimaryTxnDecision::Committed(ts)
                        }
                        crabka_pgexec::TimestampTxnDecision::Pending => unreachable!(),
                    };
                    if expected != requested {
                        return RangeResponse::SqlError {
                            code: "40001".into(),
                            message: "timestamp terminal decision is not authenticated by primary"
                                .into(),
                        };
                    }
                    let asserted_operations = writes
                        .iter()
                        .map(|write| crabka_pgexec::TimestampTxnOperation {
                            range_id: request.range_id.as_u32(),
                            table_id: write.table_id,
                            rowid: write.rowid,
                            delete: write.delete,
                        })
                        .collect::<Vec<_>>();
                    let actual_operations = primary_operations
                        .into_iter()
                        .filter(|operation| operation.range_id == request.range_id.as_u32())
                        .collect::<Vec<_>>();
                    if actual_operations != asserted_operations {
                        return RangeResponse::SqlError {
                            code: "40001".into(),
                            message: "timestamp operations are not authenticated by primary".into(),
                        };
                    }
                    participant
                        .resolve_as_secondary(identity, decision, &writes)
                        .await
                };
                match result {
                    Ok(()) => RangeResponse::TimestampParticipantDone,
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::TimestampRecover(request) => {
                let engine = match self.hosted_engine(request.range_id) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let identity = match decode_timestamp_identity(request.identity) {
                    Ok(value) => value,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let asserted_decision = match request.decision {
                    crate::transport::WireTimestampDecision::Aborted => {
                        crabka_pgexec::PrimaryTxnDecision::Aborted
                    }
                    crate::transport::WireTimestampDecision::Committed { commit_ts } => {
                        match crabka_pgexec::CommitTimestamp::new(commit_ts) {
                            Ok(ts) => crabka_pgexec::PrimaryTxnDecision::Committed(ts),
                            Err(error) => {
                                return RangeResponse::SqlError {
                                    code: "22023".into(),
                                    message: error.to_string(),
                                };
                            }
                        }
                    }
                };
                let asserted_operations = request
                    .operations
                    .into_iter()
                    .map(|op| crabka_pgexec::TimestampTxnOperation {
                        range_id: op.range_id,
                        table_id: op.table_id,
                        rowid: op.rowid,
                        delete: op.delete,
                    })
                    .collect::<Vec<_>>();
                let (decision, primary_operations) =
                    match self.authenticated_primary_outcome(identity).await {
                        Ok(outcome) => outcome,
                        Err(_) => {
                            return RangeResponse::SqlError {
                                code: "40001".into(),
                                message: "timestamp recovery primary identity is fenced".into(),
                            };
                        }
                    };
                if decision == crabka_pgexec::PrimaryTxnDecision::Pending {
                    return RangeResponse::SqlError {
                        code: "40001".into(),
                        message: "timestamp recovery primary has no terminal decision".into(),
                    };
                }
                let operations = primary_operations
                    .into_iter()
                    .filter(|operation| operation.range_id == request.range_id.as_u32())
                    .collect::<Vec<_>>();
                if asserted_decision != decision || asserted_operations != operations {
                    return RangeResponse::SqlError {
                        code: "40001".into(),
                        message: "timestamp recovery assertion differs from primary outcome".into(),
                    };
                }
                let result = if decision == crabka_pgexec::PrimaryTxnDecision::Aborted {
                    engine
                        .abort_timestamp_transaction_intents(identity.start_ts)
                        .await
                } else {
                    match engine
                        .resolve_timestamp_transaction_operations(
                            request.range_id.as_u32(),
                            identity,
                            decision,
                            &operations,
                        )
                        .await
                    {
                        Ok(()) => engine.recover_timestamp_scan_terminals(&operations).await,
                        Err(error) => Err(error),
                    }
                };
                match result {
                    Ok(()) => RangeResponse::TimestampParticipantDone,
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::TimestampPrimaryRecover(request) => {
                let engine = match self.hosted_engine(request.primary_range) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let identity = match decode_timestamp_identity(request.identity) {
                    Ok(identity) => identity,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let descriptor = match engine.timestamp_transaction_descriptors() {
                    Ok(descriptors) => descriptors.into_iter().find(|descriptor| {
                        descriptor.start_ts == identity.start_ts
                            && descriptor.global_xid == identity.global_xid
                            && identity.primary_range == request.primary_range.as_u32()
                    }),
                    Err(error) => {
                        let error = error.into_pg();
                        return RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        };
                    }
                };
                if descriptor.is_none() {
                    return RangeResponse::SqlError {
                        code: "40001".into(),
                        message: "timestamp primary identity is fenced".into(),
                    };
                }
                let operations = descriptor
                    .as_ref()
                    .expect("validated descriptor")
                    .operations
                    .iter()
                    .map(|operation| crate::transport::WireTimestampOperation {
                        range_id: operation.range_id,
                        table_id: operation.table_id,
                        rowid: operation.rowid,
                        delete: operation.delete,
                    })
                    .collect();
                match engine
                    .recover_timestamp_transaction(identity.start_ts)
                    .await
                {
                    Ok(decision) => RangeResponse::TimestampPrimaryDecision {
                        decision: match decision {
                            crabka_pgexec::PrimaryTxnDecision::Aborted => {
                                crate::transport::WireTimestampDecision::Aborted
                            }
                            crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts) => {
                                crate::transport::WireTimestampDecision::Committed {
                                    commit_ts: commit_ts.get(),
                                }
                            }
                            crabka_pgexec::PrimaryTxnDecision::Pending => {
                                unreachable!("recovery returns terminal decision")
                            }
                        },
                        operations,
                    },
                    Err(error) => {
                        let error = error.into_pg();
                        RangeResponse::SqlError {
                            code: error.code,
                            message: error.message,
                        }
                    }
                }
            }
            RangeRequest::TimestampPrimaryInspect(request) => {
                let engine = match self.hosted_engine(request.primary_range) {
                    Ok(engine) => engine,
                    Err(response) => return response,
                };
                let identity = match decode_timestamp_identity(request.identity) {
                    Ok(identity) => identity,
                    Err(message) => {
                        return RangeResponse::SqlError {
                            code: "22023".into(),
                            message,
                        };
                    }
                };
                let descriptor = match engine.validate_timestamp_primary_identity(identity) {
                    Ok(descriptor) if identity.primary_range == request.primary_range.as_u32() => {
                        descriptor
                    }
                    _ => {
                        return RangeResponse::SqlError {
                            code: "40001".into(),
                            message: "timestamp primary identity is fenced".into(),
                        };
                    }
                };
                RangeResponse::TimestampPrimaryOutcome {
                    decision: match descriptor.decision {
                        crabka_pgexec::PrimaryTxnDecision::Pending => {
                            crate::transport::WirePrimaryTxnDecision::Pending
                        }
                        crabka_pgexec::PrimaryTxnDecision::Aborted => {
                            crate::transport::WirePrimaryTxnDecision::Aborted
                        }
                        crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts) => {
                            crate::transport::WirePrimaryTxnDecision::Committed {
                                commit_ts: commit_ts.get(),
                            }
                        }
                    },
                    operations: descriptor
                        .operations
                        .into_iter()
                        .map(|operation| crate::transport::WireTimestampOperation {
                            range_id: operation.range_id,
                            table_id: operation.table_id,
                            rowid: operation.rowid,
                            delete: operation.delete,
                        })
                        .collect(),
                }
            }
            RangeRequest::Tso(crate::transport::TsoReq::Grant { count }) => {
                let Some(tso) = &self.tso else {
                    return RangeResponse::Error {
                        error: WireErrorKind::StaleEndpoint,
                        message: "range r0 timestamp oracle is not hosted here".to_string(),
                    };
                };
                let Some(count) = NonZeroU64::new(count) else {
                    return RangeResponse::Error {
                        error: WireErrorKind::Failed,
                        message: "timestamp grant count must be greater than zero".to_string(),
                    };
                };
                match tso.grant(count).await {
                    Ok(GrantLease { first_ts, count }) => {
                        RangeResponse::Tso(crate::transport::TsoResp::Granted {
                            first_ts: first_ts.get(),
                            count: count.get(),
                        })
                    }
                    Err(error) => tso_error_response(&error),
                }
            }
            // TxnReq has no payload for the participant's previously executed
            // transaction/session. Refusing is correct until that narrow stateful
            // participant RPC is introduced; accepting would fabricate 2PC.
            RangeRequest::Txn(_) => RangeResponse::Error {
                error: WireErrorKind::Failed,
                message: "remote transaction participants require a stateful participant RPC"
                    .to_string(),
            },
        }
    }

    async fn handle_connection(
        &self,
        request: RangeRequest,
        writer: &mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    ) -> Result<Option<RangeResponse>, TransportError> {
        let RangeRequest::Sql { range_id, sql } = request else {
            return Ok(Some(self.handle(request).await));
        };
        let engine = match self.hosted_engine(range_id) {
            Ok(engine) => engine,
            Err(response) => return Ok(Some(response)),
        };
        let mut sink = RangeFrameSink {
            writer,
            transport_error: None,
            terminal_error_sent: false,
        };
        let result = engine
            .connect()
            .simple_query_into(&sql, 256, &mut sink)
            .await;
        if let Some(error) = sink.transport_error {
            return Err(error);
        }
        if let Err(error) = result {
            if !sink.terminal_error_sent {
                let message = if error.code == "54000" {
                    "one remote SQL row exceeds the transport frame limit".to_string()
                } else {
                    error.message
                };
                write_frame(
                    sink.writer,
                    &RangeResponse::SqlError {
                        code: error.code,
                        message,
                    },
                )
                .await?;
            }
            return Ok(None);
        }
        write_frame(sink.writer, &RangeResponse::SqlResultsDone).await?;
        Ok(None)
    }
}

fn encode_global_status(status: crabka_pgmvcc::clog::XidStatus) -> WireGlobalStatus {
    match status {
        crabka_pgmvcc::clog::XidStatus::InProgress => WireGlobalStatus::InProgress,
        crabka_pgmvcc::clog::XidStatus::Prepared(global_xid) => {
            WireGlobalStatus::Prepared { global_xid }
        }
        crabka_pgmvcc::clog::XidStatus::Committed => WireGlobalStatus::Committed,
        crabka_pgmvcc::clog::XidStatus::Aborted => WireGlobalStatus::Aborted,
    }
}

fn decode_global_status(
    status: WireGlobalStatus,
) -> Result<crabka_pgmvcc::clog::XidStatus, PgError> {
    Ok(match status {
        WireGlobalStatus::InProgress => crabka_pgmvcc::clog::XidStatus::InProgress,
        WireGlobalStatus::Prepared { global_xid } => {
            crabka_pgmvcc::clog::XidStatus::Prepared(global_xid)
        }
        WireGlobalStatus::Committed => crabka_pgmvcc::clog::XidStatus::Committed,
        WireGlobalStatus::Aborted => crabka_pgmvcc::clog::XidStatus::Aborted,
    })
}

struct RangeFrameSink<'a> {
    writer: &'a mut (dyn tokio::io::AsyncWrite + Unpin + Send),
    transport_error: Option<TransportError>,
    terminal_error_sent: bool,
}

async fn handle_session_operation(
    session: &mut crabka_pgexec::SqlSession,
    operation: WireSessionOperation,
) -> Result<WireSessionResult, PgError> {
    match operation {
        WireSessionOperation::SimpleQuery { sql } => {
            session
                .simple_query(&sql)
                .await
                .map(|results| WireSessionResult::Query {
                    results: results.into_iter().map(Into::into).collect(),
                })
        }
        WireSessionOperation::Parse {
            name,
            sql,
            parameter_types,
        } => session
            .parse(&name, &sql, &parameter_types)
            .await
            .map(|description| WireSessionResult::Prepared {
                parameter_types: description.parameter_types,
                fields: description.fields.into_iter().map(Into::into).collect(),
            }),
        WireSessionOperation::Bind {
            portal,
            statement,
            params,
            result_formats,
        } => {
            let params = params
                .into_iter()
                .map(|param| BoundParam {
                    type_oid: param.type_oid,
                    format: param.format,
                    value: param.value.map(Into::into),
                })
                .collect::<Vec<_>>();
            session
                .bind(&portal, &statement, &params, &result_formats)
                .await
                .map(|description| WireSessionResult::Portal {
                    fields: description.fields.into_iter().map(Into::into).collect(),
                })
        }
        WireSessionOperation::DescribeStatement { name } => session
            .describe_statement(&name)
            .await
            .map(|description| WireSessionResult::Prepared {
                parameter_types: description.parameter_types,
                fields: description.fields.into_iter().map(Into::into).collect(),
            }),
        WireSessionOperation::DescribePortal { name } => {
            session
                .describe_portal(&name)
                .await
                .map(|description| WireSessionResult::Portal {
                    fields: description.fields.into_iter().map(Into::into).collect(),
                })
        }
        WireSessionOperation::Execute { portal, max_rows } => session
            .execute(&portal, max_rows)
            .await
            .and_then(|outcome| match outcome {
                ExecuteOutcome::Rows { rows, completion } => {
                    Ok(WireSessionResult::Execute(WireExecuteOutcome::Rows {
                        rows: rows
                            .into_iter()
                            .map(|row| {
                                row.into_iter()
                                    .map(|cell| cell.map(|value| value.to_vec()))
                                    .collect()
                            })
                            .collect(),
                        completion,
                    }))
                }
                ExecuteOutcome::CommandComplete { tag } => {
                    Ok(WireSessionResult::Execute(WireExecuteOutcome::Command {
                        tag,
                    }))
                }
                ExecuteOutcome::EmptyQuery => {
                    Ok(WireSessionResult::Execute(WireExecuteOutcome::Empty))
                }
                _ => Err(PgError::error(
                    "0A000",
                    "remote range session does not support this execute outcome",
                )),
            }),
        WireSessionOperation::PrepareGlobal { global_xid } => session
            .prepare_global_participant(global_xid)
            .await
            .map(|global_xid| WireSessionResult::GlobalPrepared { global_xid })
            .map_err(crabka_pgexec::ExecError::into_pg),
        WireSessionOperation::CommitGlobal { global_xid } => session
            .release_global_participant_commit(global_xid)
            .await
            .map(|()| WireSessionResult::Closed)
            .map_err(crabka_pgexec::ExecError::into_pg),
        WireSessionOperation::AbortGlobal { global_xid } => session
            .release_global_participant_abort(global_xid)
            .await
            .map(|()| WireSessionResult::Closed)
            .map_err(crabka_pgexec::ExecError::into_pg),
        WireSessionOperation::SetTimestampOwner { start_ts } => {
            let start_ts = start_ts
                .map(crabka_pgexec::TimestampTransactionId::new)
                .transpose()
                .map_err(|error| PgError::protocol(error.to_string()))?;
            session.set_timestamp_own_start_ts(start_ts);
            Ok(WireSessionResult::Closed)
        }
        WireSessionOperation::CloseStatement { name } => session
            .close(CloseTarget::Statement(&name))
            .await
            .map(|()| WireSessionResult::Closed),
        WireSessionOperation::ClosePortal { name } => session
            .close(CloseTarget::Portal(&name))
            .await
            .map(|()| WireSessionResult::Closed),
        WireSessionOperation::Sync => session.sync().await.map(|()| WireSessionResult::Synced {
            tx_status: match session.tx_status() {
                TxStatus::Idle => b'I',
                TxStatus::InTransaction => b'T',
                TxStatus::Failed => b'E',
            },
        }),
    }
}

#[async_trait]
impl ResultSink for RangeFrameSink<'_> {
    async fn send(&mut self, page: ResultPage) -> Result<(), PgError> {
        let response = match page {
            ResultPage::Rows {
                result_index,
                fields,
                rows,
                tag,
            } => RangeResponse::SqlResultsChunk {
                chunk: WireSqlResultChunk::Rows {
                    result_index: u32::try_from(result_index).map_err(|_| {
                        PgError::error("54000", "remote SQL result index exceeds wire capacity")
                    })?,
                    fields: fields.map(|fields| fields.into_iter().map(Into::into).collect()),
                    rows: rows
                        .into_iter()
                        .map(|row| row.into_iter().map(|cell| cell.map(Into::into)).collect())
                        .collect(),
                    tag,
                },
            },
            ResultPage::Command { result_index, tag } => RangeResponse::SqlResultsChunk {
                chunk: WireSqlResultChunk::Complete {
                    result_index: u32::try_from(result_index).map_err(|_| {
                        PgError::error("54000", "remote SQL result index exceeds wire capacity")
                    })?,
                    result: WireQueryResult::Command { tag },
                },
            },
            ResultPage::Empty { result_index } => RangeResponse::SqlResultsChunk {
                chunk: WireSqlResultChunk::Complete {
                    result_index: u32::try_from(result_index).map_err(|_| {
                        PgError::error("54000", "remote SQL result index exceeds wire capacity")
                    })?,
                    result: WireQueryResult::Empty,
                },
            },
        };
        match write_frame(self.writer, &response).await {
            Ok(()) => Ok(()),
            Err(TransportError::FrameTooLarge { .. }) => {
                write_frame(
                    self.writer,
                    &RangeResponse::SqlError {
                        code: "54000".into(),
                        message: "one remote SQL row exceeds the transport frame limit".into(),
                    },
                )
                .await
                .map_err(|error| {
                    self.transport_error = Some(error);
                    PgError::error("08006", "remote result transport failed")
                })?;
                self.terminal_error_sent = true;
                Err(PgError::error(
                    "54000",
                    "one remote SQL row exceeds the transport frame limit",
                ))
            }
            Err(error) => {
                self.transport_error = Some(error);
                Err(PgError::error("08006", "remote result transport failed"))
            }
        }
    }
}

fn sql_result_summary(results: &[QueryResult]) -> String {
    results.last().map_or_else(
        || "EMPTY".to_string(),
        |result| match result {
            QueryResult::Command { tag } | QueryResult::Rows { tag, .. } => tag.clone(),
            QueryResult::Empty => "EMPTY".to_string(),
        },
    )
}

fn tso_error_response(error: &TsoError) -> RangeResponse {
    RangeResponse::Error {
        error: WireErrorKind::Failed,
        message: error.to_string(),
    }
}

/// Forwarder contract used by routers that do not own the target range locally.
#[async_trait]
pub trait RemoteForward: Send + Sync {
    /// Forward SQL to the owning range and return the remote command summary.
    async fn forward_sql(&self, range_id: RangeId, sql: String) -> Result<String, ForwardError>;

    /// Forward SQL and preserve row descriptions, values, and command results.
    async fn forward_query(
        &self,
        range_id: RangeId,
        sql: String,
    ) -> Result<Vec<QueryResult>, ForwardError>;

    /// Forward SQL while preserving transport backpressure end to end.
    async fn forward_query_into(
        &self,
        range_id: RangeId,
        sql: String,
        sink: &mut dyn crabka_pgwire::engine::ResultSink,
    ) -> Result<(), ForwardError>;

    /// Open a stateful owner-side session for extended protocol and transactions.
    async fn open_session(&self, range_id: RangeId) -> Result<RemoteRangeSession, ForwardError>;

    /// Acquire the range-0 ordinary explicit-transaction lease when this
    /// forwarder represents a distributed topology.
    async fn acquire_explicit_gate(&self) -> Result<Option<RemoteExplicitGateLease>, ForwardError> {
        Ok(None)
    }

    async fn recover_global(
        &self,
        _range_id: RangeId,
        _global_xid: u64,
        _commit: bool,
    ) -> Result<(), ForwardError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct RemoteExplicitGateLease {
    token: Option<u64>,
    registry: RangeRegistry,
    client: FramedTcpClient,
}

impl RemoteExplicitGateLease {
    pub async fn renew(&self) -> Result<(), ForwardError> {
        let token = self.token.ok_or(ForwardError::UnexpectedResponse)?;
        match self.call(ExplicitGateReq::Renew { token }).await? {
            ExplicitGateResp::Renewed { .. } => Ok(()),
            ExplicitGateResp::Stale => Err(ForwardError::RemoteSql {
                code: "40001".into(),
                message: "explicit transaction lease expired or was fenced".into(),
            }),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }

    pub async fn release(&mut self) -> Result<(), ForwardError> {
        let Some(token) = self.token.take() else {
            return Ok(());
        };
        match self.call(ExplicitGateReq::Release { token }).await? {
            ExplicitGateResp::Released | ExplicitGateResp::Stale => Ok(()),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }

    async fn call(&self, request: ExplicitGateReq) -> Result<ExplicitGateResp, ForwardError> {
        let endpoint = self.registry.resolve(RangeId::COORDINATOR).await?;
        match self
            .client
            .call(&endpoint.endpoint, &RangeRequest::ExplicitGate(request))
            .await?
        {
            RangeResponse::ExplicitGate(response) => Ok(response),
            RangeResponse::SqlError { code, message } => {
                Err(ForwardError::RemoteSql { code, message })
            }
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }
}

impl Drop for RemoteExplicitGateLease {
    fn drop(&mut self) {
        let Some(token) = self.token.take() else {
            return;
        };
        let registry = self.registry.clone();
        let client = self.client.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Ok(endpoint) = registry.resolve(RangeId::COORDINATOR).await {
                    let _ = client
                        .call(
                            &endpoint.endpoint,
                            &RangeRequest::ExplicitGate(ExplicitGateReq::Release { token }),
                        )
                        .await;
                }
            });
        }
    }
}

/// TCP implementation of [`RemoteForward`] backed by [`RangeRegistry`].
#[derive(Debug, Clone)]
pub struct RegistryRemoteForward {
    registry: RangeRegistry,
    client: FramedTcpClient,
}

#[derive(Debug, Clone)]
pub struct RegistryTsoRpc {
    registry: RangeRegistry,
    client: FramedTcpClient,
}

impl RegistryTsoRpc {
    #[must_use]
    pub const fn new(registry: RangeRegistry, client: FramedTcpClient) -> Self {
        Self { registry, client }
    }
}

#[async_trait]
impl TsoRpc for RegistryTsoRpc {
    async fn grant(&self, count: NonZeroU64) -> Result<GrantLease, TsoError> {
        let endpoint = self
            .registry
            .resolve(RangeId::COORDINATOR)
            .await
            .map_err(|error| TsoError::Rpc(error.to_string()))?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::Tso(crate::transport::TsoReq::Grant { count: count.get() }),
            )
            .await
            .map_err(|error| TsoError::Rpc(error.to_string()))?
        {
            RangeResponse::Tso(crate::transport::TsoResp::Granted { first_ts, count }) => {
                let first_ts = crate::tso::TsoTimestamp::new(
                    NonZeroU64::new(first_ts).ok_or(TsoError::TimestampOverflow)?,
                );
                let count = NonZeroU64::new(count).ok_or(TsoError::TimestampOverflow)?;
                Ok(GrantLease::new(first_ts, count))
            }
            RangeResponse::Error { message, .. } | RangeResponse::SqlError { message, .. } => {
                Err(TsoError::Rpc(message))
            }
            _ => Err(TsoError::Rpc("unexpected range-zero TSO response".into())),
        }
    }
}

impl RegistryRemoteForward {
    /// Build a forwarding client with an injected authenticated range client.
    #[must_use]
    pub const fn new(registry: RangeRegistry, client: FramedTcpClient) -> Self {
        Self { registry, client }
    }
}

#[async_trait]
impl RemoteForward for RegistryRemoteForward {
    async fn forward_sql(&self, range_id: RangeId, sql: String) -> Result<String, ForwardError> {
        let mut retry_used = false;
        loop {
            let endpoint = self.registry.resolve(range_id).await?;
            let response = self
                .client
                .call(
                    &endpoint.endpoint,
                    &RangeRequest::Sql {
                        range_id,
                        sql: sql.clone(),
                    },
                )
                .await;
            match response {
                Ok(RangeResponse::Sql { result }) => return Ok(result),
                Ok(RangeResponse::SqlResults { results }) => {
                    let results = results
                        .into_iter()
                        .map(QueryResult::from)
                        .collect::<Vec<_>>();
                    return Ok(sql_result_summary(&results));
                }
                Ok(RangeResponse::SqlError { code, message }) => {
                    return Err(ForwardError::RemoteSql { code, message });
                }
                Ok(RangeResponse::Error { error, message }) if error.permits_reresolve() => {
                    if retry_used {
                        return Err(ForwardError::Remote {
                            kind: error,
                            message,
                        });
                    }
                    retry_used = true;
                    self.registry.refresh_authoritatively().await?;
                }
                Ok(RangeResponse::Error { error, message }) => {
                    return Err(ForwardError::Remote {
                        kind: error,
                        message,
                    });
                }
                Ok(_) => return Err(ForwardError::UnexpectedResponse),
                Err(TransportError::Remote { kind, message }) if kind.permits_reresolve() => {
                    if retry_used {
                        return Err(ForwardError::Remote { kind, message });
                    }
                    retry_used = true;
                    self.registry.refresh_authoritatively().await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn acquire_explicit_gate(&self) -> Result<Option<RemoteExplicitGateLease>, ForwardError> {
        let endpoint = self.registry.resolve(RangeId::COORDINATOR).await?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::ExplicitGate(ExplicitGateReq::Acquire),
            )
            .await?
        {
            RangeResponse::ExplicitGate(ExplicitGateResp::Acquired { token, .. }) => {
                Ok(Some(RemoteExplicitGateLease {
                    token: Some(token),
                    registry: self.registry.clone(),
                    client: self.client.clone(),
                }))
            }
            RangeResponse::SqlError { code, message } => {
                Err(ForwardError::RemoteSql { code, message })
            }
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }

    async fn recover_global(
        &self,
        range_id: RangeId,
        global_xid: u64,
        commit: bool,
    ) -> Result<(), ForwardError> {
        let endpoint = self.registry.resolve(range_id).await?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::RecoverGlobal {
                    range_id,
                    global_xid,
                    commit,
                },
            )
            .await?
        {
            RangeResponse::GlobalRecovered => Ok(()),
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }

    async fn forward_query(
        &self,
        range_id: RangeId,
        sql: String,
    ) -> Result<Vec<QueryResult>, ForwardError> {
        let endpoint = self.registry.resolve(range_id).await?;
        match self
            .client
            .call(&endpoint.endpoint, &RangeRequest::Sql { range_id, sql })
            .await?
        {
            RangeResponse::SqlResults { results } => {
                Ok(results.into_iter().map(QueryResult::from).collect())
            }
            RangeResponse::Sql { result } => Ok(vec![QueryResult::Command { tag: result }]),
            RangeResponse::SqlError { code, message } => {
                Err(ForwardError::RemoteSql { code, message })
            }
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }

    async fn forward_query_into(
        &self,
        range_id: RangeId,
        sql: String,
        sink: &mut dyn crabka_pgwire::engine::ResultSink,
    ) -> Result<(), ForwardError> {
        let endpoint = self.registry.resolve(range_id).await?;
        self.client
            .call_sql_into(
                &endpoint.endpoint,
                &RangeRequest::Sql { range_id, sql },
                sink,
            )
            .await
            .map_err(|error| match error {
                TransportError::Sql { code, message } => ForwardError::RemoteSql { code, message },
                error => ForwardError::Transport(error),
            })
    }

    async fn open_session(&self, range_id: RangeId) -> Result<RemoteRangeSession, ForwardError> {
        let endpoint = self.registry.resolve(range_id).await?;
        match self
            .client
            .call(&endpoint.endpoint, &RangeRequest::SessionOpen { range_id })
            .await?
        {
            RangeResponse::SessionOpened { session_id } => Ok(RemoteRangeSession {
                range_id,
                session_id,
                registry: self.registry.clone(),
                client: self.client.clone(),
            }),
            RangeResponse::SqlError { code, message } => {
                Err(ForwardError::RemoteSql { code, message })
            }
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }),
            _ => Err(ForwardError::UnexpectedResponse),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteRangeSession {
    range_id: RangeId,
    session_id: u64,
    registry: RangeRegistry,
    client: FramedTcpClient,
}

impl RemoteRangeSession {
    pub async fn timestamp_primary_inspect(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
    ) -> Result<
        (
            crabka_pgexec::PrimaryTxnDecision,
            Vec<crabka_pgexec::TimestampTxnOperation>,
        ),
        PgError,
    > {
        let endpoint = self
            .registry
            .resolve(self.range_id)
            .await
            .map_err(|error| ForwardError::Registry(error).into_pg())?;
        let request =
            RangeRequest::TimestampPrimaryInspect(crate::transport::TimestampPrimaryRecoverReq {
                primary_range: self.range_id,
                identity: encode_timestamp_identity(identity),
            });
        match self
            .client
            .call(&endpoint.endpoint, &request)
            .await
            .map_err(|error| ForwardError::Transport(error).into_pg())?
        {
            RangeResponse::TimestampPrimaryOutcome {
                decision,
                operations,
            } => {
                let decision = match decision {
                    crate::transport::WirePrimaryTxnDecision::Pending => {
                        crabka_pgexec::PrimaryTxnDecision::Pending
                    }
                    crate::transport::WirePrimaryTxnDecision::Aborted => {
                        crabka_pgexec::PrimaryTxnDecision::Aborted
                    }
                    crate::transport::WirePrimaryTxnDecision::Committed { commit_ts } => {
                        crabka_pgexec::PrimaryTxnDecision::Committed(
                            crabka_pgexec::CommitTimestamp::new(commit_ts)
                                .map_err(|error| PgError::protocol(error.to_string()))?,
                        )
                    }
                };
                Ok((
                    decision,
                    operations
                        .into_iter()
                        .map(|operation| crabka_pgexec::TimestampTxnOperation {
                            range_id: operation.range_id,
                            table_id: operation.table_id,
                            rowid: operation.rowid,
                            delete: operation.delete,
                        })
                        .collect(),
                ))
            }
            RangeResponse::SqlError { code, message } => Err(PgError::error(&code, message)),
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }
            .into_pg()),
            _ => Err(ForwardError::UnexpectedResponse.into_pg()),
        }
    }

    pub async fn set_timestamp_own_start_ts(
        &mut self,
        start_ts: Option<crabka_pgexec::TimestampTransactionId>,
    ) -> Result<(), PgError> {
        match self
            .call(WireSessionOperation::SetTimestampOwner {
                start_ts: start_ts.map(crabka_pgexec::TimestampTransactionId::get),
            })
            .await?
        {
            WireSessionResult::Closed => Ok(()),
            _ => Err(PgError::protocol("unexpected timestamp-owner response")),
        }
    }

    pub async fn timestamp_prewrite(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let request = RangeRequest::TimestampPrewrite(crate::transport::TimestampPrewriteReq {
            range_id: self.range_id,
            identity: encode_timestamp_identity(identity),
            primary_participants: Vec::new(),
            secondary: false,
            existing_primary: false,
            writes: writes
                .iter()
                .map(encode_timestamp_write)
                .collect::<Result<_, _>>()?,
        });
        self.call_timestamp_participant(request).await
    }

    pub async fn timestamp_prewrite_as_primary(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participants: &[u32],
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let request = RangeRequest::TimestampPrewrite(crate::transport::TimestampPrewriteReq {
            range_id: self.range_id,
            identity: encode_timestamp_identity(identity),
            primary_participants: participants.to_vec(),
            secondary: false,
            existing_primary: false,
            writes: writes
                .iter()
                .map(encode_timestamp_write)
                .collect::<Result<_, _>>()?,
        });
        self.call_timestamp_participant(request).await
    }

    pub async fn timestamp_prewrite_as_secondary(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let request = RangeRequest::TimestampPrewrite(crate::transport::TimestampPrewriteReq {
            range_id: self.range_id,
            identity: encode_timestamp_identity(identity),
            primary_participants: Vec::new(),
            secondary: true,
            existing_primary: false,
            writes: writes
                .iter()
                .map(encode_timestamp_write)
                .collect::<Result<_, _>>()?,
        });
        self.call_timestamp_participant(request).await
    }

    pub async fn timestamp_prewrite_on_primary(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let request = RangeRequest::TimestampPrewrite(crate::transport::TimestampPrewriteReq {
            range_id: self.range_id,
            identity: encode_timestamp_identity(identity),
            primary_participants: Vec::new(),
            secondary: false,
            existing_primary: true,
            writes: writes
                .iter()
                .map(encode_timestamp_write)
                .collect::<Result<_, _>>()?,
        });
        self.call_timestamp_participant(request).await
    }

    pub async fn timestamp_primary_add_participant(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participant_range: RangeId,
    ) -> Result<(), PgError> {
        let request = RangeRequest::TimestampPrimaryAck(crate::transport::TimestampPrimaryAckReq {
            primary_range: self.range_id,
            identity: encode_timestamp_identity(identity),
            participant_range: participant_range.as_u32(),
            operations: Vec::new(),
            add_participant: true,
        });
        self.call_timestamp_participant(request).await
    }

    pub async fn timestamp_primary_ack(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        participant_range: RangeId,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let request = RangeRequest::TimestampPrimaryAck(crate::transport::TimestampPrimaryAckReq {
            primary_range: self.range_id,
            identity: encode_timestamp_identity(identity),
            participant_range: participant_range.as_u32(),
            operations: writes
                .iter()
                .map(|write| crate::transport::WireTimestampOperation {
                    range_id: participant_range.as_u32(),
                    table_id: write.table_id,
                    rowid: write.rowid,
                    delete: write.delete,
                })
                .collect(),
            add_participant: false,
        });
        self.call_timestamp_participant(request).await
    }

    pub async fn timestamp_resolve(
        &self,
        identity: crabka_pgexec::TimestampTxnIdentity,
        decision: crabka_pgexec::TimestampTxnDecision,
        writes: &[crabka_pgexec::TimestampWrite],
    ) -> Result<(), PgError> {
        let decision = match decision {
            crabka_pgexec::TimestampTxnDecision::Aborted => {
                crate::transport::WireTimestampDecision::Aborted
            }
            crabka_pgexec::TimestampTxnDecision::Committed(ts) => {
                crate::transport::WireTimestampDecision::Committed {
                    commit_ts: ts.get(),
                }
            }
            _ => {
                return Err(PgError::protocol(
                    "remote timestamp resolution requires a terminal put decision",
                ));
            }
        };
        let request = RangeRequest::TimestampResolve(crate::transport::TimestampResolveReq {
            range_id: self.range_id,
            identity: encode_timestamp_identity(identity),
            decision,
            writes: writes
                .iter()
                .map(encode_timestamp_write)
                .collect::<Result<_, _>>()?,
        });
        self.call_timestamp_participant(request).await
    }

    async fn call_timestamp_participant(&self, request: RangeRequest) -> Result<(), PgError> {
        let endpoint = self
            .registry
            .resolve(self.range_id)
            .await
            .map_err(|error| ForwardError::Registry(error).into_pg())?;
        match self
            .client
            .call(&endpoint.endpoint, &request)
            .await
            .map_err(|error| ForwardError::Transport(error).into_pg())?
        {
            RangeResponse::TimestampParticipantDone => Ok(()),
            RangeResponse::SqlError { code, message } => Err(PgError::error(&code, message)),
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }
            .into_pg()),
            _ => Err(ForwardError::UnexpectedResponse.into_pg()),
        }
    }

    pub async fn timestamp_primary_decision(
        &self,
        start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> Result<crabka_pgexec::PrimaryTxnDecision, PgError> {
        let endpoint = self
            .registry
            .resolve(self.range_id)
            .await
            .map_err(|error| ForwardError::Registry(error).into_pg())?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::ResolveTxn(crate::transport::ResolveTxnReq {
                    primary_range: self.range_id,
                    start_ts: start_ts.get(),
                }),
            )
            .await
            .map_err(|error| ForwardError::Transport(error).into_pg())?
        {
            RangeResponse::ResolveTxn(ResolveTxnResp::Pending) => {
                Ok(crabka_pgexec::PrimaryTxnDecision::Pending)
            }
            RangeResponse::ResolveTxn(ResolveTxnResp::Aborted) => {
                Ok(crabka_pgexec::PrimaryTxnDecision::Aborted)
            }
            RangeResponse::ResolveTxn(ResolveTxnResp::Committed { commit_ts }) => {
                Ok(crabka_pgexec::PrimaryTxnDecision::Committed(
                    crabka_pgexec::CommitTimestamp::new(commit_ts)
                        .map_err(|error| PgError::error("22023", error.to_string()))?,
                ))
            }
            RangeResponse::SqlError { code, message } => Err(PgError::error(&code, message)),
            _ => Err(ForwardError::UnexpectedResponse.into_pg()),
        }
    }

    pub async fn begin_global(&mut self) -> Result<u64, PgError> {
        let endpoint = self
            .registry
            .resolve(self.range_id)
            .await
            .map_err(|error| ForwardError::Registry(error).into_pg())?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::GlobalBegin {
                    range_id: self.range_id,
                },
            )
            .await
            .map_err(|error| ForwardError::Transport(error).into_pg())?
        {
            RangeResponse::GlobalXid { global_xid } => Ok(global_xid),
            RangeResponse::SqlError { code, message } => Err(PgError::error(&code, message)),
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }
            .into_pg()),
            _ => Err(ForwardError::UnexpectedResponse.into_pg()),
        }
    }

    async fn call(
        &mut self,
        operation: WireSessionOperation,
    ) -> Result<WireSessionResult, PgError> {
        let endpoint = self
            .registry
            .resolve(self.range_id)
            .await
            .map_err(|error| ForwardError::Registry(error).into_pg())?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::Session {
                    range_id: self.range_id,
                    session_id: self.session_id,
                    operation,
                },
            )
            .await
            .map_err(|error| ForwardError::Transport(error).into_pg())?
        {
            RangeResponse::SessionResult { result } => Ok(result),
            RangeResponse::SqlError { code, message } => Err(PgError::error(&code, message)),
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }
            .into_pg()),
            _ => Err(ForwardError::UnexpectedResponse.into_pg()),
        }
    }

    pub async fn simple_query(&mut self, sql: String) -> Result<Vec<QueryResult>, PgError> {
        match self.call(WireSessionOperation::SimpleQuery { sql }).await? {
            WireSessionResult::Query { results } => {
                Ok(results.into_iter().map(Into::into).collect())
            }
            _ => Err(PgError::protocol("unexpected remote simple-query response")),
        }
    }

    pub async fn parse(
        &mut self,
        name: String,
        sql: String,
        parameter_types: Vec<u32>,
    ) -> Result<crabka_pgwire::engine::PreparedDescription, PgError> {
        match self
            .call(WireSessionOperation::Parse {
                name,
                sql,
                parameter_types,
            })
            .await?
        {
            WireSessionResult::Prepared {
                parameter_types,
                fields,
            } => Ok(crabka_pgwire::engine::PreparedDescription {
                parameter_types,
                fields: fields.into_iter().map(Into::into).collect(),
            }),
            _ => Err(PgError::protocol("unexpected remote parse response")),
        }
    }

    pub async fn bind(
        &mut self,
        portal: String,
        statement: String,
        params: &[BoundParam],
        result_formats: Vec<i16>,
    ) -> Result<crabka_pgwire::engine::PortalDescription, PgError> {
        let params = params
            .iter()
            .map(|param| crate::transport::WireBoundParam {
                type_oid: param.type_oid,
                format: param.format,
                value: param.value.as_ref().map(|value| value.to_vec()),
            })
            .collect();
        match self
            .call(WireSessionOperation::Bind {
                portal,
                statement,
                params,
                result_formats,
            })
            .await?
        {
            WireSessionResult::Portal { fields } => Ok(crabka_pgwire::engine::PortalDescription {
                fields: fields.into_iter().map(Into::into).collect(),
            }),
            _ => Err(PgError::protocol("unexpected remote bind response")),
        }
    }

    pub async fn describe_statement(
        &mut self,
        name: String,
    ) -> Result<crabka_pgwire::engine::PreparedDescription, PgError> {
        match self
            .call(WireSessionOperation::DescribeStatement { name })
            .await?
        {
            WireSessionResult::Prepared {
                parameter_types,
                fields,
            } => Ok(crabka_pgwire::engine::PreparedDescription {
                parameter_types,
                fields: fields.into_iter().map(Into::into).collect(),
            }),
            _ => Err(PgError::protocol("unexpected remote describe response")),
        }
    }

    pub async fn describe_portal(
        &mut self,
        name: String,
    ) -> Result<crabka_pgwire::engine::PortalDescription, PgError> {
        match self
            .call(WireSessionOperation::DescribePortal { name })
            .await?
        {
            WireSessionResult::Portal { fields } => Ok(crabka_pgwire::engine::PortalDescription {
                fields: fields.into_iter().map(Into::into).collect(),
            }),
            _ => Err(PgError::protocol("unexpected remote describe response")),
        }
    }

    pub async fn execute(
        &mut self,
        portal: String,
        max_rows: u32,
    ) -> Result<ExecuteOutcome, PgError> {
        match self
            .call(WireSessionOperation::Execute { portal, max_rows })
            .await?
        {
            WireSessionResult::Execute(WireExecuteOutcome::Rows { rows, completion }) => {
                Ok(ExecuteOutcome::Rows {
                    rows: rows
                        .into_iter()
                        .map(|row| row.into_iter().map(|cell| cell.map(Into::into)).collect())
                        .collect(),
                    completion,
                })
            }
            WireSessionResult::Execute(WireExecuteOutcome::Command { tag }) => {
                Ok(ExecuteOutcome::CommandComplete { tag })
            }
            WireSessionResult::Execute(WireExecuteOutcome::Empty) => Ok(ExecuteOutcome::EmptyQuery),
            _ => Err(PgError::protocol("unexpected remote execute response")),
        }
    }

    pub async fn prepare_global(&mut self, global_xid: u64) -> Result<u64, PgError> {
        match self
            .call(WireSessionOperation::PrepareGlobal { global_xid })
            .await?
        {
            WireSessionResult::GlobalPrepared { global_xid } => Ok(global_xid),
            _ => Err(PgError::protocol("unexpected remote prepare response")),
        }
    }

    pub async fn release_global(&mut self, global_xid: u64, commit: bool) -> Result<(), PgError> {
        let operation = if commit {
            WireSessionOperation::CommitGlobal { global_xid }
        } else {
            WireSessionOperation::AbortGlobal { global_xid }
        };
        match self.call(operation).await? {
            WireSessionResult::Closed => Ok(()),
            _ => Err(PgError::protocol("unexpected remote release response")),
        }
    }

    pub async fn record_global_decision(
        &mut self,
        global_xid: u64,
        status: crabka_pgmvcc::clog::XidStatus,
    ) -> Result<crabka_pgmvcc::clog::XidStatus, PgError> {
        let endpoint = self
            .registry
            .resolve(self.range_id)
            .await
            .map_err(|error| ForwardError::Registry(error).into_pg())?;
        match self
            .client
            .call(
                &endpoint.endpoint,
                &RangeRequest::GlobalDecision {
                    range_id: self.range_id,
                    global_xid,
                    status: encode_global_status(status),
                },
            )
            .await
            .map_err(|error| ForwardError::Transport(error).into_pg())?
        {
            RangeResponse::GlobalStatus { status } => decode_global_status(status),
            RangeResponse::SqlError { code, message } => Err(PgError::error(&code, message)),
            RangeResponse::Error { error, message } => Err(ForwardError::Remote {
                kind: error,
                message,
            }
            .into_pg()),
            _ => Err(ForwardError::UnexpectedResponse.into_pg()),
        }
    }

    pub async fn close(&mut self, target: CloseTarget<'_>) -> Result<(), PgError> {
        let operation = match target {
            CloseTarget::Statement(name) => WireSessionOperation::CloseStatement {
                name: name.to_owned(),
            },
            CloseTarget::Portal(name) => WireSessionOperation::ClosePortal {
                name: name.to_owned(),
            },
        };
        match self.call(operation).await? {
            WireSessionResult::Closed => Ok(()),
            _ => Err(PgError::protocol("unexpected remote close response")),
        }
    }

    pub async fn sync(&mut self) -> Result<TxStatus, PgError> {
        match self.call(WireSessionOperation::Sync).await? {
            WireSessionResult::Synced { tx_status } => match tx_status {
                b'I' => Ok(TxStatus::Idle),
                b'T' => Ok(TxStatus::InTransaction),
                b'E' => Ok(TxStatus::Failed),
                _ => Err(PgError::protocol("invalid remote transaction status")),
            },
            _ => Err(PgError::protocol("unexpected remote sync response")),
        }
    }
}

impl From<QueryResult> for WireQueryResult {
    fn from(value: QueryResult) -> Self {
        match value {
            QueryResult::Rows { fields, rows, tag } => Self::Rows {
                fields: fields.into_iter().map(Into::into).collect(),
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|cell| cell.map(Into::into)).collect())
                    .collect(),
                tag,
            },
            QueryResult::Command { tag } => Self::Command { tag },
            QueryResult::Empty => Self::Empty,
        }
    }
}

impl From<WireQueryResult> for QueryResult {
    fn from(value: WireQueryResult) -> Self {
        match value {
            WireQueryResult::Rows { fields, rows, tag } => Self::Rows {
                fields: fields.into_iter().map(Into::into).collect(),
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(|cell| cell.map(Into::into)).collect())
                    .collect(),
                tag,
            },
            WireQueryResult::Command { tag } => Self::Command { tag },
            WireQueryResult::Empty => Self::Empty,
        }
    }
}

impl From<Cell> for crate::transport::WireCell {
    fn from(value: Cell) -> Self {
        Self {
            text: value.text.to_vec(),
            binary: value.binary.to_vec(),
        }
    }
}

impl From<crate::transport::WireCell> for Cell {
    fn from(value: crate::transport::WireCell) -> Self {
        Self {
            text: value.text.into(),
            binary: value.binary.into(),
        }
    }
}

impl From<FieldDescription> for crate::transport::WireFieldDescription {
    fn from(value: FieldDescription) -> Self {
        Self {
            name: value.name,
            table_oid: value.table_oid,
            column_id: value.column_id,
            type_oid: value.type_oid,
            type_size: value.type_size,
            type_modifier: value.type_modifier,
            format: value.format,
        }
    }
}

impl From<crate::transport::WireFieldDescription> for FieldDescription {
    fn from(value: crate::transport::WireFieldDescription) -> Self {
        Self {
            name: value.name,
            table_oid: value.table_oid,
            column_id: value.column_id,
            type_oid: value.type_oid,
            type_size: value.type_size,
            type_modifier: value.type_modifier,
            format: value.format,
        }
    }
}

/// Range scanner that reads locally hosted ranges in-process and remote ranges over RPC.
pub struct RegistryRangeScanner {
    registry: RangeRegistry,
    client: FramedTcpClient,
    local_engines: std::collections::BTreeMap<RangeId, crabka_pgexec::SqlEngine>,
}

impl Clone for RegistryRangeScanner {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
            client: self.client.clone(),
            local_engines: self
                .local_engines
                .iter()
                .map(|(range_id, engine)| (*range_id, engine.clone_handle()))
                .collect(),
        }
    }
}

impl RegistryRangeScanner {
    /// Build a scanner from registry discovery, an authenticated client, and local engines.
    #[must_use]
    pub fn new(
        registry: RangeRegistry,
        client: FramedTcpClient,
        local_engines: std::collections::BTreeMap<RangeId, crabka_pgexec::SqlEngine>,
    ) -> Self {
        Self {
            registry,
            client,
            local_engines,
        }
    }

    async fn scan_async(
        &self,
        request: crabka_pgexec::ScanRequest<'_>,
    ) -> Result<Vec<crabka_pgexec::ScannedRow>, crabka_pgexec::ExecError> {
        if !request.table.sharded {
            return crabka_pgexec::RangeScanner::scan(&crabka_pgexec::LocalRangeScanner, request);
        }
        if request.read_ts.is_none() {
            return Err(crabka_pgexec::ExecError::Unsupported(
                "sharded scatter scans require a finite statement read timestamp".into(),
            ));
        }
        let mut rows = Vec::new();
        for range_id in self.registry.range_ids().await {
            if let Some(engine) = self.local_engines.get(&range_id) {
                let local_rows = engine.scan_local_visible_with_timestamp_owner(
                    request.table,
                    request.global_snapshot,
                    request.snapshot,
                    request.own_xid,
                    request.read_ts,
                    request.own_start_ts,
                    request.interval,
                )?;
                rows.extend(crabka_pgexec::scanner::apply_executable_scan_pushdown(
                    local_rows,
                    &request.predicate,
                    &request.projection,
                    request.partial_aggregate.as_ref(),
                    request.top_k.as_ref(),
                )?);
                continue;
            }
            rows.extend(self.scan_remote_range(range_id, &request).await?);
        }
        if let Some(spec) = request.partial_aggregate.as_ref() {
            rows = merge_partial_aggregate_rows(rows, spec)?;
        } else if let Some(spec) = request.top_k.as_ref() {
            crabka_pgexec::scanner::apply_top_k_pushdown(&mut rows, spec)?;
        } else {
            rows.sort_by_key(|row| (row.rowid, row.xmin));
        }
        Ok(rows)
    }

    async fn scan_remote_range(
        &self,
        range_id: RangeId,
        request: &crabka_pgexec::ScanRequest<'_>,
    ) -> Result<Vec<crabka_pgexec::ScannedRow>, crabka_pgexec::ExecError> {
        let req = ScanRangeReq {
            range_id,
            table_name: request.table.name.clone(),
            interval: WireRowInterval {
                start: request.interval.start,
                end: request.interval.end,
            },
            local_snapshot: WireSnapshot::from(request.snapshot),
            global_snapshot: WireSnapshot::from(request.global_snapshot),
            own_xid: request.own_xid,
            read_ts: request
                .read_ts
                .map(crabka_pgexec::timestamp_txn::ReadTimestamp::get),
            own_start_ts: request
                .own_start_ts
                .map(crabka_pgexec::TimestampTransactionId::get),
            predicate: encode_predicate(&request.predicate)?,
            projection: encode_projection(&request.projection),
            partial_aggregate: request
                .partial_aggregate
                .as_ref()
                .map(encode_partial_aggregate),
            top_k: request.top_k.as_ref().map(encode_top_k),
        };
        let mut retry_used = false;
        loop {
            let endpoint = self
                .registry
                .resolve(range_id)
                .await
                .map_err(|error| scanner_error(error.into()))?;
            let response = self
                .client
                .call(&endpoint.endpoint, &RangeRequest::ScanRange(req.clone()))
                .await;
            match response {
                Ok(RangeResponse::ScanRange(response)) => return decode_scan_rows(response),
                Ok(RangeResponse::ScanRangeError { code, message }) => {
                    return Err(crabka_pgexec::ExecError::Remote(PgError::error(
                        &code, message,
                    )));
                }
                Ok(RangeResponse::Error { error, message }) if error.permits_reresolve() => {
                    if retry_used {
                        return Err(scanner_error(ForwardError::Remote {
                            kind: error,
                            message,
                        }));
                    }
                    retry_used = true;
                    self.registry
                        .refresh_authoritatively()
                        .await
                        .map_err(|error| scanner_error(error.into()))?;
                }
                Ok(RangeResponse::Error { error, message }) => {
                    return Err(scanner_error(ForwardError::Remote {
                        kind: error,
                        message,
                    }));
                }
                Ok(_) => return Err(scanner_error(ForwardError::UnexpectedResponse)),
                Err(TransportError::Remote { kind, message }) if kind.permits_reresolve() => {
                    if retry_used {
                        return Err(scanner_error(ForwardError::Remote { kind, message }));
                    }
                    retry_used = true;
                    self.registry
                        .refresh_authoritatively()
                        .await
                        .map_err(|error| scanner_error(error.into()))?;
                }
                Err(error) => return Err(scanner_error(ForwardError::Transport(error))),
            }
        }
    }

    async fn scan_remote_cursor(
        &self,
        range_id: RangeId,
        request: ScanCursorReq,
    ) -> Result<ScanCursorResp, crabka_pgexec::ExecError> {
        let mut retry_used = false;
        loop {
            let endpoint = self
                .registry
                .resolve(range_id)
                .await
                .map_err(|error| scanner_error(error.into()))?;
            match self
                .client
                .call(
                    &endpoint.endpoint,
                    &RangeRequest::ScanCursor(request.clone()),
                )
                .await
            {
                Ok(RangeResponse::ScanCursor(response)) => return Ok(response),
                Ok(RangeResponse::ScanRangeError { code, message }) => {
                    return Err(crabka_pgexec::ExecError::Remote(PgError::error(
                        &code, message,
                    )));
                }
                Ok(RangeResponse::Error { error, message }) if error.permits_reresolve() => {
                    if retry_used {
                        return Err(scanner_error(ForwardError::Remote {
                            kind: error,
                            message,
                        }));
                    }
                    retry_used = true;
                    self.registry
                        .refresh_authoritatively()
                        .await
                        .map_err(|error| scanner_error(error.into()))?;
                }
                Ok(RangeResponse::Error { error, message }) => {
                    return Err(scanner_error(ForwardError::Remote {
                        kind: error,
                        message,
                    }));
                }
                Ok(_) => return Err(scanner_error(ForwardError::UnexpectedResponse)),
                Err(TransportError::Remote { kind, message }) if kind.permits_reresolve() => {
                    if retry_used {
                        return Err(scanner_error(ForwardError::Remote { kind, message }));
                    }
                    retry_used = true;
                    self.registry
                        .refresh_authoritatively()
                        .await
                        .map_err(|error| scanner_error(error.into()))?;
                }
                Err(error) => return Err(scanner_error(ForwardError::Transport(error))),
            }
        }
    }
}

impl crabka_pgexec::RangeScanner for RegistryRangeScanner {
    fn scan(
        &self,
        request: crabka_pgexec::ScanRequest<'_>,
    ) -> Result<Vec<crabka_pgexec::ScannedRow>, crabka_pgexec::ExecError> {
        let scanner = self.clone();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("range scanner runtime builds")
                        .block_on(scanner.scan_async(request))
                })
                .join()
                .expect("range scanner thread does not panic")
        })
    }

    fn scan_cursor<'a>(
        &'a self,
        request: crabka_pgexec::ScanRequest<'a>,
    ) -> Result<Box<dyn crabka_pgexec::RangeCursor + 'a>, crabka_pgexec::ExecError> {
        if !request.table.sharded {
            return crabka_pgexec::RangeScanner::scan_cursor(
                &crabka_pgexec::LocalRangeScanner,
                request,
            );
        }
        if request.partial_aggregate.is_some() || request.top_k.is_some() {
            return Ok(Box::new(crabka_pgexec::MaterializedRangeCursor::new(
                self.scan(request)?,
            )));
        }
        Ok(Box::new(RegistryRangeCursor {
            scanner: self,
            request,
            done: false,
            owners: None,
            tokens: BTreeMap::new(),
            finished: std::collections::BTreeSet::new(),
            pending: std::collections::VecDeque::new(),
        }))
    }
}

struct RegistryRangeCursor<'a> {
    scanner: &'a RegistryRangeScanner,
    request: crabka_pgexec::ScanRequest<'a>,
    done: bool,
    /// Range membership is statement-stable. Endpoint refresh may move an
    /// owner, but must never add/remove owners halfway through a snapshot.
    owners: Option<Vec<RangeId>>,
    tokens: BTreeMap<RangeId, Option<Vec<u8>>>,
    finished: std::collections::BTreeSet<RangeId>,
    pending: std::collections::VecDeque<crabka_pgexec::ScannedRow>,
}

#[async_trait]
impl crabka_pgexec::RangeCursor for RegistryRangeCursor<'_> {
    async fn next_page(
        &mut self,
        max_rows: usize,
    ) -> Result<crabka_pgexec::ScanPage, crabka_pgexec::ExecError> {
        if max_rows == 0 {
            return Err(crabka_pgexec::ExecError::Unsupported(
                "range cursor page size must be greater than zero".into(),
            ));
        }
        if self.done {
            return Ok(crabka_pgexec::ScanPage {
                rows: Box::new([]),
                is_last: true,
            });
        }
        if self.owners.is_none() {
            self.owners = Some(self.scanner.registry.range_ids().await);
        }
        if self.pending.is_empty() {
            let active = self
                .owners
                .as_ref()
                .expect("owners initialized above")
                .iter()
                .copied()
                .filter(|range_id| !self.finished.contains(range_id))
                .collect::<Vec<_>>();
            if active.is_empty() {
                self.done = true;
            } else {
                let per_owner = max_rows.div_ceil(active.len()).max(1);
                let mut rows = Vec::new();
                let mut updates = Vec::with_capacity(active.len());
                for range_id in active {
                    let scan = ScanRangeReq {
                        range_id,
                        table_name: self.request.table.name.clone(),
                        interval: WireRowInterval {
                            start: self.request.interval.start,
                            end: self.request.interval.end,
                        },
                        local_snapshot: WireSnapshot::from(self.request.snapshot),
                        global_snapshot: WireSnapshot::from(self.request.global_snapshot),
                        own_xid: self.request.own_xid,
                        read_ts: self
                            .request
                            .read_ts
                            .map(crabka_pgexec::timestamp_txn::ReadTimestamp::get),
                        own_start_ts: self
                            .request
                            .own_start_ts
                            .map(crabka_pgexec::TimestampTransactionId::get),
                        predicate: encode_predicate(&self.request.predicate)?,
                        projection: encode_projection(&self.request.projection),
                        partial_aggregate: None,
                        top_k: None,
                    };
                    let request = ScanCursorReq {
                        scan: Box::new(scan),
                        token: self.tokens.get(&range_id).cloned().flatten(),
                        max_rows: per_owner,
                    };
                    let response = if let Some(engine) = self.scanner.local_engines.get(&range_id) {
                        handle_scan_cursor(engine, request)?
                    } else {
                        self.scanner.scan_remote_cursor(range_id, request).await?
                    };
                    rows.extend(decode_scan_rows(ScanRangeResp {
                        rows: response.rows,
                    })?);
                    updates.push((range_id, response.token, response.is_last));
                }
                for (range_id, token, is_last) in updates {
                    self.tokens.insert(range_id, token);
                    if is_last {
                        self.finished.insert(range_id);
                    }
                }
                rows.sort_by_key(|row| (row.rowid, row.xmin));
                self.pending.extend(rows);
            }
        }
        let take = max_rows.min(self.pending.len());
        let rows = self.pending.drain(..take).collect::<Vec<_>>();
        self.done = self.pending.is_empty()
            && self.finished.len()
                == self
                    .owners
                    .as_ref()
                    .expect("owners initialized above")
                    .len();
        Ok(crabka_pgexec::ScanPage {
            rows: rows.into_boxed_slice(),
            is_last: self.done,
        })
    }
}

/// Range-compute service that evaluates scan visibility on the owning local engine.
pub struct RangeScanService {
    engines: std::collections::BTreeMap<RangeId, crabka_pgexec::SqlEngine>,
}

/// Range-compute service that answers timestamp transaction primary-resolution RPCs.
pub struct TimestampResolveService {
    engines: std::collections::BTreeMap<RangeId, crabka_pgexec::SqlEngine>,
}

impl TimestampResolveService {
    /// Build a resolver service for locally hosted primary ranges.
    #[must_use]
    pub fn new(engines: std::collections::BTreeMap<RangeId, crabka_pgexec::SqlEngine>) -> Self {
        Self { engines }
    }
}

#[async_trait]
impl RangeService for TimestampResolveService {
    async fn handle(&self, request: RangeRequest) -> RangeResponse {
        let RangeRequest::ResolveTxn(request) = request else {
            return RangeResponse::Error {
                error: WireErrorKind::Failed,
                message: "expected resolve_txn rpc".to_string(),
            };
        };
        let Some(engine) = self.engines.get(&request.primary_range) else {
            return RangeResponse::Error {
                error: WireErrorKind::StaleEndpoint,
                message: format!("range r{} is not hosted here", request.primary_range),
            };
        };
        match resolve_primary(engine, request.start_ts) {
            Ok(response) => RangeResponse::ResolveTxn(response),
            Err(error) => RangeResponse::Error {
                error: WireErrorKind::Failed,
                message: error.into_pg().message,
            },
        }
    }
}

fn resolve_primary(
    engine: &crabka_pgexec::SqlEngine,
    start_ts: u64,
) -> Result<ResolveTxnResp, crabka_pgexec::ExecError> {
    let start_ts = crabka_pgexec::TimestampTransactionId::new(start_ts).map_err(|error| {
        crabka_pgexec::ExecError::Unsupported(format!("invalid resolve timestamp: {error}"))
    })?;
    Ok(match engine.primary_timestamp_decision(start_ts)? {
        crabka_pgexec::PrimaryTxnDecision::Pending => ResolveTxnResp::Pending,
        crabka_pgexec::PrimaryTxnDecision::Aborted => ResolveTxnResp::Aborted,
        crabka_pgexec::PrimaryTxnDecision::Committed(commit_ts) => ResolveTxnResp::Committed {
            commit_ts: commit_ts.get(),
        },
    })
}

impl RangeScanService {
    /// Build a scan service for locally hosted range engines.
    #[must_use]
    pub fn new(engines: std::collections::BTreeMap<RangeId, crabka_pgexec::SqlEngine>) -> Self {
        Self { engines }
    }
}

#[async_trait]
impl RangeService for RangeScanService {
    async fn handle(&self, request: RangeRequest) -> RangeResponse {
        let (range_id, scan_request, cursor_request) = match request {
            RangeRequest::ScanRange(request) => (request.range_id, Some(request), None),
            RangeRequest::ScanCursor(request) => (request.scan.range_id, None, Some(request)),
            _ => {
                return RangeResponse::Error {
                    error: WireErrorKind::Failed,
                    message: "expected scan_range rpc".to_string(),
                };
            }
        };
        let Some(engine) = self.engines.get(&range_id) else {
            return RangeResponse::Error {
                error: WireErrorKind::StaleEndpoint,
                message: format!("range r{range_id} is not hosted here"),
            };
        };
        let response = match cursor_request {
            Some(request) => handle_scan_cursor(engine, request).map(RangeResponse::ScanCursor),
            None => handle_scan_range(engine, scan_request.expect("scan request present"))
                .map(RangeResponse::ScanRange),
        };
        match response {
            Ok(response) => response,
            Err(error) => {
                let error = error.into_pg();
                RangeResponse::ScanRangeError {
                    code: error.code,
                    message: error.message,
                }
            }
        }
    }
}

fn handle_scan_range(
    engine: &crabka_pgexec::SqlEngine,
    request: ScanRangeReq,
) -> Result<ScanRangeResp, crabka_pgexec::ExecError> {
    let table = crabka_pgcatalog::get_table(engine.catalog_kv(), &request.table_name)?;
    let rows = engine.scan_local_visible_with_timestamp_owner(
        &table,
        &request.global_snapshot.into(),
        &request.local_snapshot.into(),
        request.own_xid,
        request
            .read_ts
            .map(crabka_pgexec::timestamp_txn::ReadTimestamp::new)
            .transpose()
            .map_err(|error| crabka_pgexec::ExecError::Unsupported(error.to_string()))?,
        request
            .own_start_ts
            .map(crabka_pgexec::TimestampTransactionId::new)
            .transpose()
            .map_err(|error| crabka_pgexec::ExecError::Unsupported(error.to_string()))?,
        crabka_pgexec::RowInterval {
            start: request.interval.start,
            end: request.interval.end,
        },
    )?;
    let predicate = decode_predicate(request.predicate)?;
    let projection = decode_projection(request.projection);
    let partial_aggregate = request
        .partial_aggregate
        .as_ref()
        .map(decode_partial_aggregate);
    let top_k = request.top_k.map(decode_top_k);
    let rows = crabka_pgexec::scanner::apply_executable_scan_pushdown(
        rows,
        &predicate,
        &projection,
        partial_aggregate.as_ref(),
        top_k.as_ref(),
    )?;
    Ok(ScanRangeResp {
        rows: rows
            .into_iter()
            .map(|row| ScanRangeRow {
                rowid: row.rowid,
                xmin: row.xmin,
                tuple: crabka_pgmvcc::version::encode_tuple(row.xmin, 0, &row.row),
            })
            .collect(),
    })
}

fn handle_scan_cursor(
    engine: &crabka_pgexec::SqlEngine,
    mut request: ScanCursorReq,
) -> Result<ScanCursorResp, crabka_pgexec::ExecError> {
    if request.max_rows == 0 {
        return Err(crabka_pgexec::ExecError::Unsupported(
            "range cursor page size must be greater than zero".into(),
        ));
    }
    if request.scan.partial_aggregate.is_some() || request.scan.top_k.is_some() {
        return Err(crabka_pgexec::ExecError::Unsupported(
            "blocking scan pushdowns cannot use the row cursor protocol".into(),
        ));
    }
    let table = crabka_pgcatalog::get_table(engine.catalog_kv(), &request.scan.table_name)?;
    let (next, terminal) = match request.token.as_deref() {
        Some(token) => decode_owner_cursor_token(token)?,
        None => {
            let start = request.scan.interval.start.unwrap_or(0);
            let terminal = request
                .scan
                .interval
                .end
                .unwrap_or(engine.scan_local_terminal(&table)?);
            (start, terminal)
        }
    };
    if next >= terminal {
        return Ok(ScanCursorResp {
            rows: Vec::new(),
            token: None,
            is_last: true,
        });
    }
    let width = u64::try_from(request.max_rows).unwrap_or(u64::MAX);
    let page_end = next.saturating_add(width).min(terminal);
    request.scan.interval = WireRowInterval {
        start: Some(next),
        end: Some(page_end),
    };
    let response = handle_scan_range(engine, *request.scan)?;
    let is_last = page_end >= terminal;
    Ok(ScanCursorResp {
        rows: response.rows,
        token: (!is_last).then(|| encode_owner_cursor_token(page_end, terminal)),
        is_last,
    })
}

fn encode_owner_cursor_token(next: u64, terminal: u64) -> Vec<u8> {
    let mut token = Vec::with_capacity(16);
    token.extend_from_slice(&next.to_be_bytes());
    token.extend_from_slice(&terminal.to_be_bytes());
    token
}

fn decode_owner_cursor_token(token: &[u8]) -> Result<(u64, u64), crabka_pgexec::ExecError> {
    let token: [u8; 16] = token.try_into().map_err(|_| {
        crabka_pgexec::ExecError::Unsupported("invalid owner range cursor token".into())
    })?;
    let next = u64::from_be_bytes(token[..8].try_into().expect("token half is eight bytes"));
    let terminal = u64::from_be_bytes(token[8..].try_into().expect("token half is eight bytes"));
    Ok((next, terminal))
}

fn merge_partial_aggregate_rows(
    rows: Vec<crabka_pgexec::ScannedRow>,
    spec: &crabka_pgexec::PartialAggregateSpec,
) -> Result<Vec<crabka_pgexec::ScannedRow>, crabka_pgexec::ExecError> {
    crabka_pgexec::scanner::merge_partial_aggregate_rows(rows, spec)
}

fn encode_predicate(
    predicate: &crabka_pgexec::PredicatePushdown,
) -> Result<WirePredicatePushdown, crabka_pgexec::ExecError> {
    Ok(match predicate {
        crabka_pgexec::PredicatePushdown::FullScan => WirePredicatePushdown::FullScan,
        crabka_pgexec::PredicatePushdown::Conjunctive(predicates) => {
            WirePredicatePushdown::Conjunctive {
                predicates: predicates
                    .iter()
                    .map(|predicate| {
                        Ok(WireColumnPredicate {
                            column: predicate.column,
                            op: encode_predicate_op(predicate.op),
                            value: encode_datum(&predicate.value)?,
                        })
                    })
                    .collect::<Result<Vec<_>, crabka_pgexec::ExecError>>()?,
            }
        }
    })
}

fn decode_predicate(
    predicate: WirePredicatePushdown,
) -> Result<crabka_pgexec::PredicatePushdown, crabka_pgexec::ExecError> {
    Ok(match predicate {
        WirePredicatePushdown::FullScan => crabka_pgexec::PredicatePushdown::FullScan,
        WirePredicatePushdown::Conjunctive { predicates } => {
            crabka_pgexec::PredicatePushdown::Conjunctive(
                predicates
                    .into_iter()
                    .map(|predicate| {
                        Ok(crabka_pgexec::ColumnPredicate {
                            column: predicate.column,
                            op: decode_predicate_op(predicate.op),
                            value: decode_datum(predicate.value),
                        })
                    })
                    .collect::<Result<Vec<_>, crabka_pgexec::ExecError>>()?,
            )
        }
    })
}

fn encode_projection(projection: &crabka_pgexec::ProjectionPushdown) -> WireProjectionPushdown {
    match projection {
        crabka_pgexec::ProjectionPushdown::All => WireProjectionPushdown::All,
        crabka_pgexec::ProjectionPushdown::Columns(columns) => WireProjectionPushdown::Columns {
            columns: columns.clone(),
        },
    }
}

fn decode_projection(projection: WireProjectionPushdown) -> crabka_pgexec::ProjectionPushdown {
    match projection {
        WireProjectionPushdown::All => crabka_pgexec::ProjectionPushdown::All,
        WireProjectionPushdown::Columns { columns } => {
            crabka_pgexec::ProjectionPushdown::Columns(columns)
        }
    }
}

fn encode_partial_aggregate(
    spec: &crabka_pgexec::PartialAggregateSpec,
) -> WirePartialAggregateSpec {
    WirePartialAggregateSpec {
        function: match spec.function {
            crabka_pgexec::PartialAggregateFunction::Count => WirePartialAggregateFunction::Count,
            crabka_pgexec::PartialAggregateFunction::Sum => WirePartialAggregateFunction::Sum,
            crabka_pgexec::PartialAggregateFunction::Min => WirePartialAggregateFunction::Min,
            crabka_pgexec::PartialAggregateFunction::Max => WirePartialAggregateFunction::Max,
            crabka_pgexec::PartialAggregateFunction::AvgParts => {
                WirePartialAggregateFunction::AvgParts
            }
        },
        column: spec.column,
    }
}

fn decode_partial_aggregate(
    spec: &WirePartialAggregateSpec,
) -> crabka_pgexec::PartialAggregateSpec {
    crabka_pgexec::PartialAggregateSpec {
        function: match spec.function {
            WirePartialAggregateFunction::Count => crabka_pgexec::PartialAggregateFunction::Count,
            WirePartialAggregateFunction::Sum => crabka_pgexec::PartialAggregateFunction::Sum,
            WirePartialAggregateFunction::Min => crabka_pgexec::PartialAggregateFunction::Min,
            WirePartialAggregateFunction::Max => crabka_pgexec::PartialAggregateFunction::Max,
            WirePartialAggregateFunction::AvgParts => {
                crabka_pgexec::PartialAggregateFunction::AvgParts
            }
        },
        column: spec.column,
    }
}

fn encode_top_k(spec: &crabka_pgexec::TopKSpec) -> WireTopKSpec {
    WireTopKSpec {
        order_by: spec
            .order_by
            .iter()
            .map(|column| WireTopKColumn {
                column: column.column,
                asc: column.asc,
            })
            .collect(),
        limit: spec.limit,
    }
}

fn decode_top_k(spec: WireTopKSpec) -> crabka_pgexec::TopKSpec {
    crabka_pgexec::TopKSpec {
        order_by: spec
            .order_by
            .into_iter()
            .map(|column| crabka_pgexec::TopKColumn {
                column: column.column,
                asc: column.asc,
            })
            .collect(),
        limit: spec.limit,
    }
}

fn encode_predicate_op(op: crabka_pgexec::PredicateOp) -> WirePredicateOp {
    match op {
        crabka_pgexec::PredicateOp::Eq => WirePredicateOp::Eq,
        crabka_pgexec::PredicateOp::Lt => WirePredicateOp::Lt,
        crabka_pgexec::PredicateOp::Le => WirePredicateOp::Le,
        crabka_pgexec::PredicateOp::Gt => WirePredicateOp::Gt,
        crabka_pgexec::PredicateOp::Ge => WirePredicateOp::Ge,
    }
}

fn decode_predicate_op(op: WirePredicateOp) -> crabka_pgexec::PredicateOp {
    match op {
        WirePredicateOp::Eq => crabka_pgexec::PredicateOp::Eq,
        WirePredicateOp::Lt => crabka_pgexec::PredicateOp::Lt,
        WirePredicateOp::Le => crabka_pgexec::PredicateOp::Le,
        WirePredicateOp::Gt => crabka_pgexec::PredicateOp::Gt,
        WirePredicateOp::Ge => crabka_pgexec::PredicateOp::Ge,
    }
}

fn encode_datum(datum: &crabka_pgtypes::Datum) -> Result<WireDatum, crabka_pgexec::ExecError> {
    match datum {
        crabka_pgtypes::Datum::Null => Ok(WireDatum::Null),
        crabka_pgtypes::Datum::Bool(value) => Ok(WireDatum::Bool(*value)),
        crabka_pgtypes::Datum::Int4(value) => Ok(WireDatum::Int4(*value)),
        crabka_pgtypes::Datum::Int8(value) => Ok(WireDatum::Int8(*value)),
        crabka_pgtypes::Datum::Text(value) => Ok(WireDatum::Text(value.clone())),
        _ => Err(crabka_pgexec::ExecError::Unsupported(
            "remote predicate pushdown supports only bool/int4/int8/text literals".into(),
        )),
    }
}

fn decode_datum(datum: WireDatum) -> crabka_pgtypes::Datum {
    match datum {
        WireDatum::Null => crabka_pgtypes::Datum::Null,
        WireDatum::Bool(value) => crabka_pgtypes::Datum::Bool(value),
        WireDatum::Int4(value) => crabka_pgtypes::Datum::Int4(value),
        WireDatum::Int8(value) => crabka_pgtypes::Datum::Int8(value),
        WireDatum::Text(value) => crabka_pgtypes::Datum::Text(value),
    }
}

fn decode_timestamp_identity(
    identity: crate::transport::WireTimestampIdentity,
) -> Result<crabka_pgexec::TimestampTxnIdentity, String> {
    Ok(crabka_pgexec::TimestampTxnIdentity {
        start_ts: crabka_pgexec::TimestampTransactionId::new(identity.start_ts)
            .map_err(|error| error.to_string())?,
        global_xid: identity.global_xid,
        primary_range: identity.primary_range,
    })
}

fn decode_timestamp_write(
    write: crate::transport::WireTimestampWrite,
) -> Result<crabka_pgexec::TimestampWrite, String> {
    Ok(crabka_pgexec::TimestampWrite {
        table_id: write.table_id,
        rowid: write.rowid,
        row: write.row.into_iter().map(decode_datum).collect(),
        delete: write.delete,
        global_index_intents: vec![],
    })
}

fn encode_timestamp_identity(
    identity: crabka_pgexec::TimestampTxnIdentity,
) -> crate::transport::WireTimestampIdentity {
    crate::transport::WireTimestampIdentity {
        start_ts: identity.start_ts.get(),
        global_xid: identity.global_xid,
        primary_range: identity.primary_range,
    }
}

fn encode_timestamp_write(
    write: &crabka_pgexec::TimestampWrite,
) -> Result<crate::transport::WireTimestampWrite, PgError> {
    Ok(crate::transport::WireTimestampWrite {
        table_id: write.table_id,
        rowid: write.rowid,
        row: write
            .row
            .iter()
            .map(encode_datum)
            .collect::<Result<_, _>>()
            .map_err(crabka_pgexec::ExecError::into_pg)?,
        delete: write.delete,
    })
}

fn decode_scan_rows(
    response: ScanRangeResp,
) -> Result<Vec<crabka_pgexec::ScannedRow>, crabka_pgexec::ExecError> {
    response
        .rows
        .into_iter()
        .map(|row| {
            let (xmin, _xmax, payload) = crabka_pgmvcc::version::decode_tuple(&row.tuple)?;
            if xmin != row.xmin {
                return Err(crabka_pgexec::ExecError::Unsupported(
                    "remote scan row xmin did not match tuple payload".into(),
                ));
            }
            Ok(crabka_pgexec::ScannedRow {
                rowid: row.rowid,
                xmin: row.xmin,
                row: payload,
            })
        })
        .collect()
}

fn scanner_error(error: ForwardError) -> crabka_pgexec::ExecError {
    match error {
        ForwardError::Transport(_) => crabka_pgexec::ExecError::Unavailable,
        error => crabka_pgexec::ExecError::Remote(error.into_pg()),
    }
}

/// Forwarding failure.
#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    /// Range could not be discovered.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// Transport failed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// Remote compute returned a retry-visible or terminal error.
    #[error("remote range returned {kind:?}: {message}")]
    Remote {
        kind: WireErrorKind,
        message: String,
    },
    /// Remote SQL execution failed with the owner's `PostgreSQL` error code.
    #[error("remote SQL error {code}: {message}")]
    RemoteSql { code: String, message: String },
    /// Response variant did not match the request.
    #[error("remote range returned an unexpected response")]
    UnexpectedResponse,
}

impl ForwardError {
    /// Convert a remote-forwarding failure into its `PostgreSQL` error class.
    #[must_use]
    pub fn into_pg(self) -> PgError {
        match self {
            Self::RemoteSql { code, message } => PgError::error(&code, message),
            Self::Remote {
                kind:
                    WireErrorKind::Aborted | WireErrorKind::StaleEndpoint | WireErrorKind::NotLeader,
                message,
            } => PgError::error("40001", message),
            Self::Remote {
                kind: WireErrorKind::Failed,
                message,
            } => PgError::error("XX000", message),
            Self::Registry(error) => PgError::error("XX000", error.to_string()),
            Self::Transport(error) => PgError::error("08006", error.to_string()),
            Self::UnexpectedResponse => {
                PgError::error("08P01", "remote range returned an unexpected response")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use async_trait::async_trait;
    use crabka_gres_control::{
        RangeLayoutEntry, SqlUser, TenantId, TenantName, TenantRecord, TenantState,
    };
    use crabka_pgcatalog::{Column, Table};
    use crabka_pgexec::RangeScanner;
    use crabka_pgkv::{Kv, MemKv};
    use crabka_pgtypes::{ColumnType, Datum};
    use crabka_pgwire::engine::{Engine, Session};
    use tokio::net::TcpListener;

    use super::*;

    #[tokio::test]
    async fn range_zero_explicit_gate_serializes_expires_and_fences_stale_owners() {
        let service = Arc::new(HostedRangeService::new(BTreeMap::new()));
        let ExplicitGateResp::Acquired { token: first, .. } =
            service.handle_explicit_gate(ExplicitGateReq::Acquire).await
        else {
            panic!("first lease");
        };

        let contender = {
            let service = Arc::clone(&service);
            tokio::spawn(
                async move { service.handle_explicit_gate(ExplicitGateReq::Acquire).await },
            )
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!contender.is_finished(), "conflicting owner must wait");

        let ExplicitGateResp::Acquired { token: second, .. } = contender.await.unwrap() else {
            panic!("second lease after expiry");
        };
        assert_ne!(first, second);
        assert_eq!(
            service
                .handle_explicit_gate(ExplicitGateReq::Release { token: first })
                .await,
            ExplicitGateResp::Stale
        );
        assert_eq!(
            service
                .handle_explicit_gate(ExplicitGateReq::Renew { token: second })
                .await,
            ExplicitGateResp::Renewed {
                lease_millis: EXPLICIT_GATE_LEASE.as_millis() as u64,
            }
        );
        assert_eq!(
            service
                .handle_explicit_gate(ExplicitGateReq::Release { token: second })
                .await,
            ExplicitGateResp::Released
        );
    }

    struct EndlessCursorScanner {
        dropped: Arc<AtomicUsize>,
    }

    struct EndlessCursor {
        page: usize,
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for EndlessCursor {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl crabka_pgexec::RangeCursor for EndlessCursor {
        async fn next_page(
            &mut self,
            _max_rows: usize,
        ) -> Result<crabka_pgexec::ScanPage, crabka_pgexec::ExecError> {
            self.page += 1;
            let value = if self.page == 1 {
                "first".to_string()
            } else {
                "x".repeat(100_000)
            };
            Ok(crabka_pgexec::ScanPage {
                rows: vec![crabka_pgexec::ScannedRow {
                    rowid: u64::try_from(self.page).expect("test page fits"),
                    xmin: 1,
                    row: vec![Datum::Text(value)],
                }]
                .into_boxed_slice(),
                is_last: false,
            })
        }
    }

    impl RangeScanner for EndlessCursorScanner {
        fn scan(
            &self,
            _request: crabka_pgexec::ScanRequest<'_>,
        ) -> Result<Vec<crabka_pgexec::ScannedRow>, crabka_pgexec::ExecError> {
            panic!("hosted streaming test must not materialize")
        }

        fn scan_cursor<'a>(
            &'a self,
            _request: crabka_pgexec::ScanRequest<'a>,
        ) -> Result<Box<dyn crabka_pgexec::RangeCursor + 'a>, crabka_pgexec::ExecError> {
            Ok(Box::new(EndlessCursor {
                page: 0,
                dropped: Arc::clone(&self.dropped),
            }))
        }
    }
    use crate::transport::{RangeService, spawn_loopback};

    struct StaleThenOk {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RangeService for StaleThenOk {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return RangeResponse::Error {
                    error: WireErrorKind::StaleEndpoint,
                    message: "range moved".to_string(),
                };
            }
            match request {
                RangeRequest::Sql { sql, .. } => RangeResponse::Sql { result: sql },
                RangeRequest::ScanRange(_)
                | RangeRequest::ScanCursor(_)
                | RangeRequest::SessionOpen { .. }
                | RangeRequest::Session { .. }
                | RangeRequest::SessionClose { .. }
                | RangeRequest::GlobalDecision { .. }
                | RangeRequest::GlobalBegin { .. }
                | RangeRequest::ExplicitGate(_)
                | RangeRequest::RecoverGlobal { .. }
                | RangeRequest::Txn(_)
                | RangeRequest::Tso(_)
                | RangeRequest::ResolveTxn(_)
                | RangeRequest::TimestampPrewrite(_)
                | RangeRequest::TimestampPrimaryAck(_)
                | RangeRequest::TimestampResolve(_)
                | RangeRequest::TimestampRecover(_)
                | RangeRequest::TimestampPrimaryRecover(_)
                | RangeRequest::TimestampPrimaryInspect(_) => RangeResponse::Error {
                    error: WireErrorKind::Failed,
                    message: "wrong rpc".to_string(),
                },
            }
        }
    }

    struct AlwaysNotLeader {
        calls: Arc<AtomicUsize>,
    }

    struct CurrentRecord {
        record: Mutex<TenantRecord>,
    }

    #[async_trait]
    impl crate::registry::RangeRegistrySource for CurrentRecord {
        async fn load_current(&self) -> Result<TenantRecord, RegistryError> {
            Ok(self.record.lock().expect("current record lock").clone())
        }
    }

    #[async_trait]
    impl RangeService for AlwaysNotLeader {
        async fn handle(&self, _request: RangeRequest) -> RangeResponse {
            self.calls.fetch_add(1, Ordering::SeqCst);
            RangeResponse::Error {
                error: WireErrorKind::NotLeader,
                message: "not writer".to_string(),
            }
        }
    }

    fn record(endpoint: String) -> TenantRecord {
        record_with_layout(vec![RangeLayoutEntry {
            range_id: 1,
            end_key: None,
            endpoint,
            wal_generation: 1,
        }])
    }

    fn record_with_layout(layout: Vec<RangeLayoutEntry>) -> TenantRecord {
        TenantRecord::new(
            1,
            TenantId::try_from("tenant-a").unwrap(),
            TenantName::try_from("tenant-a").unwrap(),
            TenantState::Active,
            SqlUser::try_from("alice").unwrap(),
            "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
            3,
        )
        .unwrap()
        .with_range_layout(layout)
        .unwrap()
    }

    fn sharded_table() -> Table {
        Table {
            id: 11,
            name: "t11".to_string(),
            columns: vec![Column {
                name: "id".to_string(),
                ty: ColumnType::Int4,
                not_null: false,
                default: None,
            }],
            sharded: true,
            sharding: None,
            foreign: None,
        }
    }

    struct FakeScanRange {
        requests: Mutex<Vec<ScanRangeReq>>,
        rowid: u64,
        value: i32,
    }

    struct FakePartialAggregateRange {
        requests: Mutex<Vec<ScanRangeReq>>,
        row: Vec<Datum>,
    }

    struct ScanErrorRange {
        code: &'static str,
        message: &'static str,
    }

    #[async_trait]
    impl RangeService for ScanErrorRange {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            assert!(matches!(request, RangeRequest::ScanRange(_)));
            RangeResponse::ScanRangeError {
                code: self.code.to_string(),
                message: self.message.to_string(),
            }
        }
    }

    #[async_trait]
    impl RangeService for FakeScanRange {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            let RangeRequest::ScanRange(request) = request else {
                return RangeResponse::Error {
                    error: WireErrorKind::Failed,
                    message: "expected scan".to_string(),
                };
            };
            self.requests.lock().expect("requests lock").push(request);
            RangeResponse::ScanRange(ScanRangeResp {
                rows: vec![ScanRangeRow {
                    rowid: self.rowid,
                    xmin: 7,
                    tuple: crabka_pgmvcc::version::encode_tuple(7, 0, &[Datum::Int4(self.value)]),
                }],
            })
        }
    }

    #[async_trait]
    impl RangeService for FakePartialAggregateRange {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            let RangeRequest::ScanRange(request) = request else {
                return RangeResponse::Error {
                    error: WireErrorKind::Failed,
                    message: "expected scan".to_string(),
                };
            };
            self.requests.lock().expect("requests lock").push(request);
            RangeResponse::ScanRange(ScanRangeResp {
                rows: vec![ScanRangeRow {
                    rowid: 0,
                    xmin: 7,
                    tuple: crabka_pgmvcc::version::encode_tuple(7, 0, &self.row),
                }],
            })
        }
    }

    #[tokio::test]
    async fn stale_endpoint_gets_one_reresolve_retry_then_succeeds() {
        let service = Arc::new(StaleThenOk {
            calls: AtomicUsize::new(0),
        });
        let stale_addr = spawn_loopback(service).await.unwrap();
        let live_addr = spawn_loopback(Arc::new(StaleThenOk {
            calls: AtomicUsize::new(1),
        }))
        .await
        .unwrap();
        let source = Arc::new(CurrentRecord {
            record: Mutex::new(record(live_addr.to_string())),
        });
        let registry = RangeRegistry::from_tenant_record(&record(stale_addr.to_string()))
            .unwrap()
            .with_authoritative_source(source);
        let forward = RegistryRemoteForward::new(registry, FramedTcpClient::default());

        let result = forward
            .forward_sql(RangeId::new(1), "insert into t1 values (1)".to_string())
            .await
            .unwrap();

        assert_eq!(result, "insert into t1 values (1)");
    }

    #[tokio::test]
    async fn stale_endpoint_retry_is_bounded_to_two_attempts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let service = Arc::new(AlwaysNotLeader {
            calls: Arc::clone(&calls),
        });
        let addr = spawn_loopback(service).await.unwrap();
        let source = Arc::new(CurrentRecord {
            record: Mutex::new(record(addr.to_string())),
        });
        let registry = RangeRegistry::from_tenant_record(&record(addr.to_string()))
            .unwrap()
            .with_authoritative_source(source);
        let forward = RegistryRemoteForward::new(registry, FramedTcpClient::default());

        let error = forward
            .forward_sql(RangeId::new(1), "insert into t1 values (1)".to_string())
            .await
            .expect_err("second not-leader must stop retrying");

        assert!(matches!(
            error,
            ForwardError::Remote {
                kind: WireErrorKind::NotLeader,
                ..
            }
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn remote_sql_error_preserves_owner_sqlstate() {
        let engine = crabka_pgexec::SqlEngine::new();
        let address = spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            engine,
        )]))))
        .await
        .expect("start range service");
        let registry =
            RangeRegistry::from_tenant_record(&record(address.to_string())).expect("registry");
        let error = RegistryRemoteForward::new(registry, FramedTcpClient::default())
            .forward_sql(RangeId::new(1), "SELECT * FROM missing_table".to_string())
            .await
            .expect_err("remote query fails");

        let pg = error.into_pg();
        assert_eq!(pg.code, "42P01");
    }

    #[tokio::test]
    async fn remote_query_returns_fields_and_cells() {
        let engine = crabka_pgexec::SqlEngine::new();
        let mut setup = engine.connect();
        setup
            .simple_query("CREATE TABLE t (id int, value text)")
            .await
            .expect("create table");
        setup
            .simple_query("INSERT INTO t VALUES (7, 'remote')")
            .await
            .expect("insert row");
        let address = spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            engine,
        )]))))
        .await
        .expect("start range service");
        let registry =
            RangeRegistry::from_tenant_record(&record(address.to_string())).expect("registry");

        let results = RegistryRemoteForward::new(registry, FramedTcpClient::default())
            .forward_query(RangeId::new(1), "SELECT id, value FROM t".to_string())
            .await
            .expect("forward query rows");

        let [QueryResult::Rows { fields, rows, tag }] = results.as_slice() else {
            panic!("expected row result: {results:?}");
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "value"]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_ref().expect("id").text, "7");
        assert_eq!(rows[0][1].as_ref().expect("value").text, "remote");
        assert_eq!(tag, "SELECT 1");
    }

    #[tokio::test]
    async fn remote_range_zero_allocates_and_records_global_decision() {
        let mut engine = crabka_pgexec::SqlEngine::new();
        engine
            .init_gtm_coordinator()
            .expect("initialize range zero GTM");
        let address = spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::COORDINATOR,
            engine,
        )]))))
        .await
        .expect("start range zero service");
        let registry =
            RangeRegistry::from_tenant_record(&record_with_layout(vec![RangeLayoutEntry {
                range_id: 0,
                end_key: None,
                endpoint: address.to_string(),
                wal_generation: 1,
            }]))
            .expect("registry");
        let forward = RegistryRemoteForward::new(registry, FramedTcpClient::default());
        let mut session = forward
            .open_session(RangeId::COORDINATOR)
            .await
            .expect("open remote range zero session");

        let global_xid = session.begin_global().await.expect("allocate global xid");
        let status = session
            .record_global_decision(global_xid, crabka_pgmvcc::clog::XidStatus::Committed)
            .await
            .expect("record remote decision");

        assert_eq!(status, crabka_pgmvcc::clog::XidStatus::Committed);
    }

    #[tokio::test]
    async fn hosted_query_writes_first_frame_before_completion_and_disconnect_cancels_cursor() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let dropped = Arc::new(AtomicUsize::new(0));
        let mut engine = crabka_pgexec::SqlEngine::new();
        engine.set_range_scanner(Arc::new(EndlessCursorScanner {
            dropped: Arc::clone(&dropped),
        }));
        engine
            .connect()
            .simple_query("CREATE TABLE live_frames (value text) SHARDED")
            .await
            .expect("create streamed table");
        let address = spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            engine,
        )]))))
        .await
        .expect("start range service");
        let mut stream = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect range service");
        let request = serde_json::to_vec(&RangeRequest::Sql {
            range_id: RangeId::new(1),
            sql: "SELECT value FROM live_frames".into(),
        })
        .expect("serialize request");
        stream
            .write_u32(u32::try_from(request.len()).expect("request length fits"))
            .await
            .expect("write request length");
        stream.write_all(&request).await.expect("write request");

        let frame_len = tokio::time::timeout(Duration::from_secs(2), stream.read_u32())
            .await
            .expect("first frame arrives before query completion")
            .expect("read first frame length");
        let mut frame = vec![0; usize::try_from(frame_len).expect("frame length fits")];
        stream
            .read_exact(&mut frame)
            .await
            .expect("read first frame");
        let first: RangeResponse = serde_json::from_slice(&frame).expect("decode first frame");
        assert!(matches!(first, RangeResponse::SqlResultsChunk { .. }));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);

        drop(stream);
        tokio::time::timeout(Duration::from_secs(2), async {
            while dropped.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disconnect cancels and drops the active cursor");
    }

    #[tokio::test]
    async fn remote_query_pages_results_larger_than_one_transport_frame() {
        let engine = crabka_pgexec::SqlEngine::new();
        let mut setup = engine.connect();
        setup
            .simple_query("CREATE TABLE big (id int, value text, nullable text)")
            .await
            .expect("create table");
        let payload = "x".repeat(320);
        let values = (0..2_000)
            .map(|id| format!("({id}, '{payload}', NULL)"))
            .collect::<Vec<_>>()
            .join(",");
        setup
            .simple_query(&format!("INSERT INTO big VALUES {values}"))
            .await
            .expect("insert rows");
        let address = spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            engine,
        )]))))
        .await
        .expect("start range service");
        let registry =
            RangeRegistry::from_tenant_record(&record(address.to_string())).expect("registry");

        let results = RegistryRemoteForward::new(registry, FramedTcpClient::default())
            .forward_query(
                RangeId::new(1),
                "SELECT id, value, nullable FROM big".to_string(),
            )
            .await
            .expect("paged query succeeds");

        let [QueryResult::Rows { fields, rows, tag }] = results.as_slice() else {
            panic!("expected rows: {results:?}");
        };
        assert_eq!(fields.len(), 3);
        assert_eq!(rows.len(), 2_000);
        assert_eq!(rows[1_999][0].as_ref().expect("id").text, "1999");
        let value = rows[0][1].as_ref().expect("value");
        assert_eq!(value.text.len(), 320);
        assert_eq!(value.binary.len(), 320);
        assert!(rows[0][2].is_none());
        assert_eq!(tag, "SELECT 2000");
    }

    #[tokio::test]
    async fn oversized_single_row_returns_bounded_error_and_does_not_poison_server() {
        let engine = crabka_pgexec::SqlEngine::new();
        let mut setup = engine.connect();
        setup
            .simple_query("CREATE TABLE huge (value text)")
            .await
            .expect("create table");
        let payload = "z".repeat(600_000);
        setup
            .simple_query(&format!("INSERT INTO huge VALUES ('{payload}')"))
            .await
            .expect("insert huge row");
        let address = spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
            RangeId::new(1),
            engine,
        )]))))
        .await
        .expect("start range service");
        let registry =
            RangeRegistry::from_tenant_record(&record(address.to_string())).expect("registry");
        let forward = RegistryRemoteForward::new(registry, FramedTcpClient::default());

        let error = forward
            .forward_query(RangeId::new(1), "SELECT value FROM huge".to_string())
            .await
            .expect_err("one row cannot be split");
        let pg = error.into_pg();
        assert_eq!(pg.code, "54000");
        assert!(pg.message.contains("one remote SQL row"));

        let results = forward
            .forward_query(RangeId::new(1), "SELECT 7".to_string())
            .await
            .expect("next connection remains healthy");
        assert!(matches!(results.as_slice(), [QueryResult::Rows { .. }]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_scan_error_preserves_owner_sqlstate_and_message() {
        let snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: 1,
            xmax: 2,
            xip: vec![],
        };
        let local = MemKv::new();
        let global = MemKv::new();

        for (code, message) in [
            ("42P01", "relation \"missing\" does not exist"),
            ("22023", "invalid parameter value"),
            ("0A000", "scan is unsupported"),
        ] {
            let address = spawn_loopback(Arc::new(ScanErrorRange { code, message }))
                .await
                .expect("start range service");
            let registry =
                RangeRegistry::from_tenant_record(&record(address.to_string())).expect("registry");
            let scanner =
                RegistryRangeScanner::new(registry, FramedTcpClient::default(), BTreeMap::new());

            let error = scanner
                .scan(crabka_pgexec::ScanRequest {
                    local: &local,
                    global: &global,
                    global_snapshot: &snapshot,
                    snapshot: &snapshot,
                    own_xid: None,
                    read_ts: Some(
                        crabka_pgexec::ReadTimestamp::new(100).expect("finite test timestamp"),
                    ),
                    own_start_ts: None,
                    table: &sharded_table(),
                    interval: crabka_pgexec::RowInterval::ALL,
                    predicate: crabka_pgexec::PredicatePushdown::FullScan,
                    projection: crabka_pgexec::ProjectionPushdown::All,
                    partial_aggregate: None,
                    top_k: None,
                })
                .expect_err("owner scan fails");

            let pg = error.into_pg();
            assert_eq!(pg.code, code);
            assert_eq!(pg.message, message);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_range_scanner_sends_payload_and_merges_rows_deterministically() {
        let left = Arc::new(FakeScanRange {
            requests: Mutex::new(Vec::new()),
            rowid: 2,
            value: 20,
        });
        let right = Arc::new(FakeScanRange {
            requests: Mutex::new(Vec::new()),
            rowid: 1,
            value: 10,
        });
        let left_addr = spawn_loopback(left.clone()).await.unwrap();
        let right_addr = spawn_loopback(right.clone()).await.unwrap();
        let registry = RangeRegistry::from_tenant_record(&record_with_layout(vec![
            RangeLayoutEntry {
                range_id: 1,
                end_key: Some(crabka_gres_control::RangeBoundary::table_start(100)),
                endpoint: left_addr.to_string(),
                wal_generation: 1,
            },
            RangeLayoutEntry {
                range_id: 2,
                end_key: None,
                endpoint: right_addr.to_string(),
                wal_generation: 1,
            },
        ]))
        .unwrap();
        let scanner = RegistryRangeScanner::new(
            registry,
            FramedTcpClient::default(),
            std::collections::BTreeMap::new(),
        );
        let local = MemKv::new();
        let global = MemKv::new();
        let snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: 3,
            xmax: 9,
            xip: vec![5],
        };
        let global_snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: crabka_pgmvcc::xid::GLOBAL_XID_BASE,
            xmax: crabka_pgmvcc::xid::GLOBAL_XID_BASE + 10,
            xip: vec![],
        };

        let rows = scanner
            .scan(crabka_pgexec::ScanRequest {
                local: &local,
                global: &global,
                global_snapshot: &global_snapshot,
                snapshot: &snapshot,
                own_xid: Some(8),
                read_ts: Some(
                    crabka_pgexec::ReadTimestamp::new(100).expect("finite test timestamp"),
                ),
                own_start_ts: None,
                table: &sharded_table(),
                interval: crabka_pgexec::RowInterval {
                    start: Some(1),
                    end: Some(9),
                },
                predicate: crabka_pgexec::PredicatePushdown::FullScan,
                projection: crabka_pgexec::ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            })
            .unwrap();

        assert_eq!(
            rows.iter().map(|row| row.rowid).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(rows[0].row, vec![Datum::Int4(10)]);
        let left_requests = left.requests.lock().expect("left requests");
        assert_eq!(left_requests[0].table_name, "t11");
        assert_eq!(left_requests[0].interval.start, Some(1));
        assert_eq!(left_requests[0].local_snapshot.xip, vec![5]);
        assert_eq!(left_requests[0].own_xid, Some(8));
        assert_eq!(
            left_requests[0].read_ts,
            Some(100),
            "every scatter participant receives the statement read timestamp"
        );
        let right_requests = right.requests.lock().expect("right requests");
        assert_eq!(
            right_requests[0].read_ts, left_requests[0].read_ts,
            "remote participants share one read timestamp"
        );
        assert_eq!(left_requests[0].predicate, WirePredicatePushdown::FullScan);
        assert_eq!(left_requests[0].projection, WireProjectionPushdown::All);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_range_scanner_merges_remote_top_k_deterministically() {
        let left = Arc::new(FakeScanRange {
            requests: Mutex::new(Vec::new()),
            rowid: 2,
            value: 20,
        });
        let right = Arc::new(FakeScanRange {
            requests: Mutex::new(Vec::new()),
            rowid: 1,
            value: 30,
        });
        let left_addr = spawn_loopback(left).await.unwrap();
        let right_addr = spawn_loopback(right).await.unwrap();
        let registry = RangeRegistry::from_tenant_record(&record_with_layout(vec![
            RangeLayoutEntry {
                range_id: 1,
                end_key: Some(crabka_gres_control::RangeBoundary::table_start(100)),
                endpoint: left_addr.to_string(),
                wal_generation: 1,
            },
            RangeLayoutEntry {
                range_id: 2,
                end_key: None,
                endpoint: right_addr.to_string(),
                wal_generation: 1,
            },
        ]))
        .unwrap();
        let scanner = RegistryRangeScanner::new(
            registry,
            FramedTcpClient::default(),
            std::collections::BTreeMap::new(),
        );
        let local = MemKv::new();
        let global = MemKv::new();
        let snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: 1,
            xmax: 100,
            xip: vec![],
        };

        let rows = scanner
            .scan(crabka_pgexec::ScanRequest {
                local: &local,
                global: &global,
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: Some(
                    crabka_pgexec::ReadTimestamp::new(100).expect("finite test timestamp"),
                ),
                own_start_ts: None,
                table: &sharded_table(),
                interval: crabka_pgexec::RowInterval::ALL,
                predicate: crabka_pgexec::PredicatePushdown::FullScan,
                projection: crabka_pgexec::ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: Some(crabka_pgexec::TopKSpec {
                    order_by: vec![crabka_pgexec::TopKColumn {
                        column: 0,
                        asc: false,
                    }],
                    limit: 1,
                }),
            })
            .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row, vec![Datum::Int4(30)]);
    }

    async fn scan_remote_partial_aggregate(
        left_row: Vec<Datum>,
        right_row: Vec<Datum>,
        spec: crabka_pgexec::PartialAggregateSpec,
    ) -> (
        Vec<crabka_pgexec::ScannedRow>,
        Option<WirePartialAggregateSpec>,
    ) {
        let left = Arc::new(FakePartialAggregateRange {
            requests: Mutex::new(Vec::new()),
            row: left_row,
        });
        let right = Arc::new(FakePartialAggregateRange {
            requests: Mutex::new(Vec::new()),
            row: right_row,
        });
        let left_addr = spawn_loopback(left.clone()).await.unwrap();
        let right_addr = spawn_loopback(right).await.unwrap();
        let registry = RangeRegistry::from_tenant_record(&record_with_layout(vec![
            RangeLayoutEntry {
                range_id: 1,
                end_key: Some(crabka_gres_control::RangeBoundary::table_start(100)),
                endpoint: left_addr.to_string(),
                wal_generation: 1,
            },
            RangeLayoutEntry {
                range_id: 2,
                end_key: None,
                endpoint: right_addr.to_string(),
                wal_generation: 1,
            },
        ]))
        .unwrap();
        let scanner = RegistryRangeScanner::new(
            registry,
            FramedTcpClient::default(),
            std::collections::BTreeMap::new(),
        );
        let local = MemKv::new();
        let global = MemKv::new();
        let snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: 1,
            xmax: 100,
            xip: vec![],
        };

        let rows = scanner
            .scan(crabka_pgexec::ScanRequest {
                local: &local,
                global: &global,
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: Some(
                    crabka_pgexec::ReadTimestamp::new(100).expect("finite test timestamp"),
                ),
                own_start_ts: None,
                table: &sharded_table(),
                interval: crabka_pgexec::RowInterval::ALL,
                predicate: crabka_pgexec::PredicatePushdown::FullScan,
                projection: crabka_pgexec::ProjectionPushdown::All,
                partial_aggregate: Some(spec),
                top_k: None,
            })
            .unwrap();
        let request = left.requests.lock().expect("left requests")[0]
            .partial_aggregate
            .clone();
        (rows, request)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_range_scanner_merges_remote_partial_aggregate_rows() {
        let cases = [
            (
                crabka_pgexec::PartialAggregateFunction::Count,
                WirePartialAggregateFunction::Count,
                None,
                vec![Datum::Int8(0)],
                vec![Datum::Int8(3)],
                Datum::Int8(3),
            ),
            (
                crabka_pgexec::PartialAggregateFunction::Sum,
                WirePartialAggregateFunction::Sum,
                Some(0),
                vec![Datum::Int8(20)],
                vec![Datum::Int8(30)],
                Datum::Int8(50),
            ),
            (
                crabka_pgexec::PartialAggregateFunction::Min,
                WirePartialAggregateFunction::Min,
                Some(0),
                vec![Datum::Null],
                vec![Datum::Int4(5)],
                Datum::Int4(5),
            ),
            (
                crabka_pgexec::PartialAggregateFunction::Max,
                WirePartialAggregateFunction::Max,
                Some(0),
                vec![Datum::Null],
                vec![Datum::Int4(5)],
                Datum::Int4(5),
            ),
            (
                crabka_pgexec::PartialAggregateFunction::AvgParts,
                WirePartialAggregateFunction::AvgParts,
                Some(0),
                vec![Datum::Numeric(10.into()), Datum::Int8(1)],
                vec![Datum::Numeric(14.into()), Datum::Int8(2)],
                Datum::Numeric(24.into()),
            ),
        ];

        for (function, wire_function, column, left_row, right_row, expected) in cases {
            let (rows, requested_partial_aggregate) = scan_remote_partial_aggregate(
                left_row,
                right_row,
                crabka_pgexec::PartialAggregateSpec { function, column },
            )
            .await;

            let expected_row = if function == crabka_pgexec::PartialAggregateFunction::AvgParts {
                vec![expected, Datum::Int8(3)]
            } else {
                vec![expected]
            };
            assert_eq!(
                rows,
                vec![crabka_pgexec::ScannedRow {
                    rowid: 0,
                    xmin: 0,
                    row: expected_row,
                }]
            );
            assert_eq!(
                requested_partial_aggregate,
                Some(WirePartialAggregateSpec {
                    function: wire_function,
                    column,
                })
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_range_service_applies_predicate_and_projection_pushdown() {
        let owner = crabka_pgexec::SqlEngine::new();
        let mut owner_session = owner.connect();
        owner_session
            .simple_query("CREATE TABLE t11 (id int4, name text) SHARDED")
            .await
            .expect("create owner table");
        owner_session
            .simple_query("INSERT INTO t11 VALUES (1, 'drop'), (2, 'keep')")
            .await
            .expect("insert owner rows");
        let service = RangeScanService::new(std::collections::BTreeMap::from([(
            RangeId::new(1),
            owner.clone_handle(),
        )]));

        let response = service
            .handle(RangeRequest::ScanRange(ScanRangeReq {
                range_id: RangeId::new(1),
                table_name: "t11".to_string(),
                interval: WireRowInterval {
                    start: None,
                    end: None,
                },
                local_snapshot: WireSnapshot {
                    xmin: 1,
                    xmax: 100,
                    xip: vec![],
                },
                global_snapshot: WireSnapshot {
                    xmin: 1,
                    xmax: 100,
                    xip: vec![],
                },
                own_xid: None,
                read_ts: Some(100),
                own_start_ts: None,
                predicate: WirePredicatePushdown::Conjunctive {
                    predicates: vec![WireColumnPredicate {
                        column: 0,
                        op: WirePredicateOp::Eq,
                        value: WireDatum::Int4(2),
                    }],
                },
                projection: WireProjectionPushdown::Columns { columns: vec![1] },
                partial_aggregate: None,
                top_k: None,
            }))
            .await;

        let RangeResponse::ScanRange(response) = response else {
            panic!("expected scan_range response");
        };
        assert_eq!(response.rows.len(), 1);
        let (_xmin, _xmax, row) = crabka_pgmvcc::version::decode_tuple(&response.rows[0].tuple)
            .expect("decode projected tuple");
        assert_eq!(row, vec![Datum::Text("keep".to_string())]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_cursor_uses_owner_token_without_skips_or_duplicates() {
        let owner = crabka_pgexec::SqlEngine::new();
        let mut owner_session = owner.connect();
        owner_session
            .simple_query("CREATE TABLE cursor_items (id int4) SHARDED")
            .await
            .expect("create owner table");
        owner_session
            .simple_query("INSERT INTO cursor_items VALUES (10), (20)")
            .await
            .expect("insert owner rows");
        let service = RangeScanService::new(std::collections::BTreeMap::from([(
            RangeId::new(1),
            owner.clone_handle(),
        )]));
        let scan = ScanRangeReq {
            range_id: RangeId::new(1),
            table_name: "cursor_items".to_string(),
            interval: WireRowInterval {
                start: None,
                end: None,
            },
            local_snapshot: WireSnapshot {
                xmin: 1,
                xmax: 100,
                xip: vec![],
            },
            global_snapshot: WireSnapshot {
                xmin: 1,
                xmax: 100,
                xip: vec![],
            },
            own_xid: None,
            read_ts: Some(100),
            own_start_ts: None,
            predicate: WirePredicatePushdown::FullScan,
            projection: WireProjectionPushdown::All,
            partial_aggregate: None,
            top_k: None,
        };

        let mut token = None;
        let mut rowids = Vec::new();
        loop {
            let response = service
                .handle(RangeRequest::ScanCursor(ScanCursorReq {
                    scan: Box::new(scan.clone()),
                    token,
                    max_rows: 1,
                }))
                .await;
            let RangeResponse::ScanCursor(response) = response else {
                panic!("expected cursor page");
            };
            rowids.extend(response.rows.into_iter().map(|row| row.rowid));
            if response.is_last {
                assert!(response.token.is_none());
                break;
            }
            token = response.token;
        }
        assert_eq!(rowids.len(), 2);
        assert!(rowids.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_range_service_executes_partial_count_pushdown() {
        let owner = crabka_pgexec::SqlEngine::new();
        let mut owner_session = owner.connect();
        owner_session
            .simple_query("CREATE TABLE t11 (id int4, name text) SHARDED")
            .await
            .expect("create owner table");
        owner_session
            .simple_query("INSERT INTO t11 VALUES (1, 'drop'), (2, 'keep'), (3, 'keep')")
            .await
            .expect("insert owner rows");
        let service = RangeScanService::new(std::collections::BTreeMap::from([(
            RangeId::new(1),
            owner.clone_handle(),
        )]));

        let response = service
            .handle(RangeRequest::ScanRange(ScanRangeReq {
                range_id: RangeId::new(1),
                table_name: "t11".to_string(),
                interval: WireRowInterval {
                    start: None,
                    end: None,
                },
                local_snapshot: WireSnapshot {
                    xmin: 1,
                    xmax: 100,
                    xip: vec![],
                },
                global_snapshot: WireSnapshot {
                    xmin: 1,
                    xmax: 100,
                    xip: vec![],
                },
                own_xid: None,
                read_ts: Some(100),
                own_start_ts: None,
                predicate: WirePredicatePushdown::Conjunctive {
                    predicates: vec![WireColumnPredicate {
                        column: 1,
                        op: WirePredicateOp::Eq,
                        value: WireDatum::Text("keep".to_string()),
                    }],
                },
                projection: WireProjectionPushdown::All,
                partial_aggregate: Some(WirePartialAggregateSpec {
                    function: WirePartialAggregateFunction::Count,
                    column: None,
                }),
                top_k: None,
            }))
            .await;

        let RangeResponse::ScanRange(response) = response else {
            panic!("expected scan_range response");
        };
        assert_eq!(response.rows.len(), 1);
        let (_xmin, _xmax, row) = crabka_pgmvcc::version::decode_tuple(&response.rows[0].tuple)
            .expect("decode count tuple");
        assert_eq!(row, vec![Datum::Int8(2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_range_service_executes_top_k_pushdown() {
        let owner = crabka_pgexec::SqlEngine::new();
        let mut owner_session = owner.connect();
        owner_session
            .simple_query("CREATE TABLE t11 (id int4, name text) SHARDED")
            .await
            .expect("create owner table");
        owner_session
            .simple_query("INSERT INTO t11 VALUES (10, 'd'), (30, 'z'), (30, 'a'), (20, 'b')")
            .await
            .expect("insert owner rows");
        let service = RangeScanService::new(std::collections::BTreeMap::from([(
            RangeId::new(1),
            owner.clone_handle(),
        )]));

        let response = service
            .handle(RangeRequest::ScanRange(ScanRangeReq {
                range_id: RangeId::new(1),
                table_name: "t11".to_string(),
                interval: WireRowInterval {
                    start: None,
                    end: None,
                },
                local_snapshot: WireSnapshot {
                    xmin: 1,
                    xmax: 100,
                    xip: vec![],
                },
                global_snapshot: WireSnapshot {
                    xmin: 1,
                    xmax: 100,
                    xip: vec![],
                },
                own_xid: None,
                read_ts: Some(100),
                own_start_ts: None,
                predicate: WirePredicatePushdown::FullScan,
                projection: WireProjectionPushdown::All,
                partial_aggregate: None,
                top_k: Some(WireTopKSpec {
                    order_by: vec![
                        WireTopKColumn {
                            column: 0,
                            asc: false,
                        },
                        WireTopKColumn {
                            column: 1,
                            asc: true,
                        },
                    ],
                    limit: 2,
                }),
            }))
            .await;

        let RangeResponse::ScanRange(response) = response else {
            panic!("expected scan_range response");
        };
        let rows = response
            .rows
            .iter()
            .map(|wire_row| {
                let (_xmin, _xmax, row) = crabka_pgmvcc::version::decode_tuple(&wire_row.tuple)
                    .expect("decode top-k tuple");
                row
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            vec![
                vec![Datum::Int4(30), Datum::Text("a".to_string())],
                vec![Datum::Int4(30), Datum::Text("z".to_string())],
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_range_scanner_surfaces_remote_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_mins(1)).await;
        });
        let registry = RangeRegistry::from_tenant_record(&record(addr.to_string())).unwrap();
        let scanner = RegistryRangeScanner::new(
            registry,
            FramedTcpClient::with_timeout(Duration::from_millis(20)),
            std::collections::BTreeMap::new(),
        );
        let local = MemKv::new();
        let global = MemKv::new();
        let snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: 1,
            xmax: 2,
            xip: vec![],
        };

        let error = scanner
            .scan(crabka_pgexec::ScanRequest {
                local: &local,
                global: &global,
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: Some(
                    crabka_pgexec::ReadTimestamp::new(100).expect("finite test timestamp"),
                ),
                own_start_ts: None,
                table: &sharded_table(),
                interval: crabka_pgexec::RowInterval::ALL,
                predicate: crabka_pgexec::PredicatePushdown::FullScan,
                projection: crabka_pgexec::ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            })
            .expect_err("timeout must surface");

        assert_eq!(error.into_pg().code, "08006");
    }

    #[tokio::test]
    async fn timestamp_resolve_service_reports_primary_commit() {
        let kv = Arc::new(MemKv::new());
        let engine = crabka_pgexec::SqlEngine::with_kv(kv.clone()).expect("engine");
        let start = crabka_pgexec::TimestampTransactionId::new(5).expect("start");
        let commit = crabka_pgexec::CommitTimestamp::after_start(start, 8).expect("commit");
        let mut descriptor = crabka_pgexec::TimestampTxnDescriptor::begun(start, 5, vec![]);
        descriptor
            .decide(crabka_pgexec::PrimaryTxnDecision::Committed(commit))
            .expect("descriptor decision");
        kv.write_batch(&[crabka_pgexec::timestamp_txn::timestamp_txn_descriptor_op(
            &descriptor,
        )])
        .expect("descriptor decision");
        let service = TimestampResolveService::new(std::collections::BTreeMap::from([(
            RangeId::new(7),
            engine,
        )]));

        let response = service
            .handle(RangeRequest::ResolveTxn(crate::transport::ResolveTxnReq {
                primary_range: RangeId::new(7),
                start_ts: 5,
            }))
            .await;

        assert_eq!(
            response,
            RangeResponse::ResolveTxn(ResolveTxnResp::Committed { commit_ts: 8 })
        );
    }

    #[tokio::test]
    async fn timestamp_primary_ack_fences_wrong_global_identity() {
        let engine = crabka_pgexec::SqlEngine::new();
        let start_ts = crabka_pgexec::TimestampTransactionId::new(90).expect("start timestamp");
        engine
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                91,
                vec![1],
            ))
            .await
            .expect("descriptor");
        let service = HostedRangeService::new(BTreeMap::from([(RangeId::new(1), engine)]));
        let response = service
            .handle(RangeRequest::TimestampPrimaryAck(
                crate::transport::TimestampPrimaryAckReq {
                    primary_range: RangeId::new(1),
                    identity: crate::transport::WireTimestampIdentity {
                        start_ts: start_ts.get(),
                        global_xid: 999,
                        primary_range: 1,
                    },
                    participant_range: 2,
                    operations: Vec::new(),
                    add_participant: true,
                },
            ))
            .await;
        assert!(matches!(response, RangeResponse::SqlError { code, .. } if code == "40001"));
    }

    #[tokio::test]
    async fn timestamp_secondary_resolve_rejects_decision_not_held_by_primary() {
        let primary = crabka_pgexec::SqlEngine::new();
        let secondary = crabka_pgexec::SqlEngine::new();
        let start_ts = crabka_pgexec::TimestampTransactionId::new(100).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 101,
            primary_range: 1,
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            rowid: 2,
            row: vec![crabka_pgtypes::Datum::Int4(2)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![1, 2],
            ))
            .await
            .expect("descriptor");
        secondary
            .timestamp_txn_participant(2)
            .prewrite_as_secondary(identity, std::slice::from_ref(&write))
            .await
            .expect("secondary prewrite");
        primary
            .decide_timestamp_transaction(start_ts, crabka_pgexec::PrimaryTxnDecision::Aborted)
            .await
            .expect("abort primary");
        let service = HostedRangeService::new(BTreeMap::from([
            (RangeId::new(1), primary),
            (RangeId::new(2), secondary),
        ]));
        let response = service
            .handle(RangeRequest::TimestampResolve(
                crate::transport::TimestampResolveReq {
                    range_id: RangeId::new(2),
                    identity: encode_timestamp_identity(identity),
                    decision: crate::transport::WireTimestampDecision::Committed { commit_ts: 102 },
                    writes: vec![encode_timestamp_write(&write).expect("wire write")],
                },
            ))
            .await;
        assert!(matches!(response, RangeResponse::SqlError { code, .. } if code == "40001"));
    }

    #[tokio::test]
    async fn timestamp_recover_fences_forged_identity_without_mutating_intent() {
        let primary = crabka_pgexec::SqlEngine::new();
        let secondary = crabka_pgexec::SqlEngine::new();
        let start_ts = crabka_pgexec::TimestampTransactionId::new(300).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 301,
            primary_range: 1,
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            rowid: 2,
            row: vec![crabka_pgtypes::Datum::Int4(2)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![1, 2],
            ))
            .await
            .expect("descriptor");
        secondary
            .timestamp_txn_participant(2)
            .prewrite_as_secondary(identity, std::slice::from_ref(&write))
            .await
            .expect("secondary prewrite");
        let service = HostedRangeService::new(BTreeMap::from([
            (RangeId::new(1), primary),
            (RangeId::new(2), secondary.clone_handle()),
        ]));
        let response = service
            .handle(RangeRequest::TimestampRecover(
                crate::transport::TimestampRecoverReq {
                    range_id: RangeId::new(2),
                    identity: crate::transport::WireTimestampIdentity {
                        start_ts: start_ts.get(),
                        global_xid: 999,
                        primary_range: 9,
                    },
                    decision: crate::transport::WireTimestampDecision::Aborted,
                    operations: Vec::new(),
                },
            ))
            .await;
        assert!(matches!(response, RangeResponse::SqlError { code, .. } if code == "40001"));
        assert_eq!(
            timestamp_tuple_state(&secondary, &write, start_ts),
            crabka_pgmvcc::version::TsVersionState::Intent
        );
    }

    #[tokio::test]
    async fn timestamp_recover_rejects_forged_commit_and_ops_without_mutating_intent() {
        let primary = crabka_pgexec::SqlEngine::new();
        let secondary = crabka_pgexec::SqlEngine::new();
        let start_ts = crabka_pgexec::TimestampTransactionId::new(310).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 311,
            primary_range: 1,
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            rowid: 2,
            row: vec![crabka_pgtypes::Datum::Int4(2)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        let operation = crabka_pgexec::TimestampTxnOperation {
            range_id: 2,
            table_id: write.table_id,
            rowid: write.rowid,
            delete: false,
        };
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![2],
            ))
            .await
            .expect("descriptor");
        secondary
            .timestamp_txn_participant(2)
            .prewrite_as_secondary(identity, std::slice::from_ref(&write))
            .await
            .expect("secondary prewrite");
        primary
            .acknowledge_timestamp_participant_operations(start_ts, 2, &[operation])
            .await
            .expect("ack operations");
        let actual_commit =
            crabka_pgexec::CommitTimestamp::after_start(start_ts, 312).expect("commit timestamp");
        primary
            .decide_timestamp_transaction(
                start_ts,
                crabka_pgexec::PrimaryTxnDecision::Committed(actual_commit),
            )
            .await
            .expect("commit primary");
        let service = HostedRangeService::new(BTreeMap::from([
            (RangeId::new(1), primary),
            (RangeId::new(2), secondary.clone_handle()),
        ]));
        let response = service
            .handle(RangeRequest::TimestampRecover(
                crate::transport::TimestampRecoverReq {
                    range_id: RangeId::new(2),
                    identity: encode_timestamp_identity(identity),
                    decision: crate::transport::WireTimestampDecision::Committed { commit_ts: 999 },
                    operations: vec![crate::transport::WireTimestampOperation {
                        range_id: 2,
                        table_id: 10,
                        rowid: 999,
                        delete: true,
                    }],
                },
            ))
            .await;
        assert!(matches!(response, RangeResponse::SqlError { code, .. } if code == "40001"));
        assert_eq!(
            timestamp_tuple_state(&secondary, &write, start_ts),
            crabka_pgmvcc::version::TsVersionState::Intent
        );
    }

    #[tokio::test]
    async fn remote_timestamp_inspection_does_not_abort_pending_primary() {
        let primary = crabka_pgexec::SqlEngine::new();
        let secondary = crabka_pgexec::SqlEngine::new();
        let start_ts = crabka_pgexec::TimestampTransactionId::new(400).expect("start timestamp");
        let identity = crabka_pgexec::TimestampTxnIdentity {
            start_ts,
            global_xid: 401,
            primary_range: 1,
        };
        let write = crabka_pgexec::TimestampWrite {
            table_id: 10,
            rowid: 2,
            row: vec![crabka_pgtypes::Datum::Int4(2)],
            delete: false,
            global_index_intents: Vec::new(),
        };
        primary
            .begin_timestamp_transaction(&crabka_pgexec::TimestampTxnDescriptor::begun(
                start_ts,
                identity.global_xid,
                vec![2],
            ))
            .await
            .expect("pending descriptor");
        secondary
            .timestamp_txn_participant(2)
            .prewrite_as_secondary(identity, std::slice::from_ref(&write))
            .await
            .expect("secondary prewrite");
        let primary_address =
            spawn_loopback(Arc::new(HostedRangeService::new(BTreeMap::from([(
                RangeId::new(1),
                primary.clone_handle(),
            )]))))
            .await
            .expect("primary service");
        let registry = RangeRegistry::from_tenant_record(&record(primary_address.to_string()))
            .expect("registry");
        let secondary_service = HostedRangeService::new(BTreeMap::from([(
            RangeId::new(2),
            secondary.clone_handle(),
        )]))
        .with_timestamp_primary_remote(registry, FramedTcpClient::default());
        let response = secondary_service
            .handle(RangeRequest::TimestampRecover(
                crate::transport::TimestampRecoverReq {
                    range_id: RangeId::new(2),
                    identity: encode_timestamp_identity(identity),
                    decision: crate::transport::WireTimestampDecision::Committed { commit_ts: 402 },
                    operations: vec![crate::transport::WireTimestampOperation {
                        range_id: 2,
                        table_id: write.table_id,
                        rowid: 999,
                        delete: true,
                    }],
                },
            ))
            .await;
        assert!(matches!(response, RangeResponse::SqlError { code, .. } if code == "40001"));
        assert_eq!(
            primary
                .primary_timestamp_decision(start_ts)
                .expect("primary decision"),
            crabka_pgexec::PrimaryTxnDecision::Pending
        );
        assert_eq!(
            timestamp_tuple_state(&secondary, &write, start_ts),
            crabka_pgmvcc::version::TsVersionState::Intent
        );
    }

    fn timestamp_tuple_state(
        engine: &crabka_pgexec::SqlEngine,
        write: &crabka_pgexec::TimestampWrite,
        start_ts: crabka_pgexec::TimestampTransactionId,
    ) -> crabka_pgmvcc::version::TsVersionState {
        let key =
            crabka_pgmvcc::version::version_key_ts(write.table_id, write.rowid, start_ts.get());
        let bytes = engine
            .kv_handle()
            .get(&key)
            .expect("read tuple")
            .expect("tuple");
        crabka_pgmvcc::version::decode_ts_tuple(&bytes)
            .expect("decode tuple")
            .state
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registry_range_scanner_uses_owner_visibility_not_gateway_local_store() {
        let owner = crabka_pgexec::SqlEngine::new();
        let mut owner_session = owner.connect();
        owner_session
            .simple_query("CREATE TABLE t11 (id int4) SHARDED")
            .await
            .expect("create owner table");
        owner_session
            .simple_query("INSERT INTO t11 VALUES (42)")
            .await
            .expect("insert owner row");
        let owner_table = crabka_pgcatalog::get_table(owner.catalog_kv(), "t11").expect("t11");
        let service = RangeScanService::new(std::collections::BTreeMap::from([(
            RangeId::new(1),
            owner.clone_handle(),
        )]));
        let addr = spawn_loopback(Arc::new(service)).await.unwrap();
        let registry = RangeRegistry::from_tenant_record(&record(addr.to_string())).unwrap();
        let scanner = RegistryRangeScanner::new(
            registry,
            FramedTcpClient::default(),
            std::collections::BTreeMap::new(),
        );
        let gateway_local = MemKv::new();
        let global = MemKv::new();
        let snapshot = crabka_pgmvcc::visibility::Snapshot {
            xmin: 1,
            xmax: 100,
            xip: vec![],
        };

        let rows = scanner
            .scan(crabka_pgexec::ScanRequest {
                local: &gateway_local,
                global: &global,
                global_snapshot: &snapshot,
                snapshot: &snapshot,
                own_xid: None,
                read_ts: Some(
                    crabka_pgexec::ReadTimestamp::new(100).expect("finite test timestamp"),
                ),
                own_start_ts: None,
                table: &owner_table,
                interval: crabka_pgexec::RowInterval::ALL,
                predicate: crabka_pgexec::PredicatePushdown::FullScan,
                projection: crabka_pgexec::ProjectionPushdown::All,
                partial_aggregate: None,
                top_k: None,
            })
            .expect("remote owner scan");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row, vec![Datum::Int4(42)]);
    }
}
