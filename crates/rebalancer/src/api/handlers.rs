//! One Rust fn per Connect-RPC method. Each takes axum's `Extension`
//! (carrying `Arc<AppState>`) and a typed `ConnectRequest`, returning
//! a typed `ConnectResponse` or a `ConnectError`.
//!
//! The connectrpc-axum-build 0.1 codegen produces a *builder* (not a
//! trait): `pb::rebalancer_connect::RebalancerServiceBuilder` accepts
//! these freestanding async fns via `post_connect`-style wiring. The
//! builder is parameterized over the axum router's `S = ()` state, so
//! we propagate the per-server `AppState` via an `Extension` layer
//! rather than typed state — that keeps the codegen S generic at `()`
//! and avoids `FromRef`/`with_state` plumbing.

use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::error::Code;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use tokio_util::sync::CancellationToken;

use crate::executor::{Execution, ExecutionHandle};
use crate::ingest::SharedSnapshot;
use crate::metrics::RebalancerMetrics;
use crate::model::proposal::ProposalStatus;
use crate::model::{ClusterState, ProposalStore};
use crate::optimizer;
use crate::pb;

/// State shared across all RPC handlers. Wired into axum via an
/// `Extension(Arc<AppState>)` layer applied to the generated router.
pub struct AppState {
    pub snapshot: SharedSnapshot,
    pub store: Arc<ProposalStore>,
    pub goal_registry: super::GoalRegistry,
    pub goal_ctx: crate::goals::GoalContext,
    pub metrics: RebalancerMetrics,
    // new in 43b:
    pub executor: crate::executor::ExecutorState,
    pub client_facade: Arc<dyn crate::executor::phases::ClientFacade>,
}

/// Convert a `ClusterState` into the proto `GetStateResponse`.
#[must_use]
pub fn cluster_state_to_proto(state: &ClusterState) -> pb::GetStateResponse {
    let mut topics_by_name: std::collections::BTreeMap<String, Vec<pb::Partition>> =
        std::collections::BTreeMap::new();
    for p in &state.partitions {
        topics_by_name
            .entry(p.topic.clone())
            .or_default()
            .push(pb::Partition {
                partition: p.partition,
                replicas: p.replicas.clone(),
                leader: p.leader,
                isr: p.isr.clone(),
            });
    }
    pb::GetStateResponse {
        snapshot_at_ms: state.snapshot_at_ms,
        brokers: state
            .brokers
            .iter()
            .map(|b| pb::Broker {
                id: b.id,
                host: b.host.clone(),
                port: b.port,
                rack: b.rack.clone(),
            })
            .collect(),
        topics: topics_by_name
            .into_iter()
            .map(|(name, partitions)| pb::Topic { name, partitions })
            .collect(),
        in_flight_reassignments: state
            .in_flight_reassignments
            .iter()
            .map(|r| pb::InFlightReassignment {
                topic: r.topic.clone(),
                partition: r.partition,
                adding_replicas: r.adding.clone(),
                removing_replicas: r.removing.clone(),
            })
            .collect(),
    }
}

#[must_use]
fn status_to_proto(s: ProposalStatus) -> pb::ProposalStatus {
    match s {
        ProposalStatus::Computed => pb::ProposalStatus::Computed,
        ProposalStatus::Executing => pb::ProposalStatus::Executing,
        ProposalStatus::Completed => pb::ProposalStatus::Completed,
        ProposalStatus::Failed => pb::ProposalStatus::Failed,
        ProposalStatus::Cancelled => pb::ProposalStatus::Cancelled,
    }
}

#[must_use]
pub fn proposal_to_proto(p: &crate::model::Proposal) -> pb::Proposal {
    pb::Proposal {
        id: p.id.clone(),
        status: i32::from(status_to_proto(p.status)),
        created_at_ms: p.created_at_ms,
        goals_applied: p.goals_applied.clone(),
        summary: Some(pb::ProposalSummary {
            replica_movements: p.summary.replica_movements,
            leader_movements: p.summary.leader_movements,
            max_replicas_before: p.summary.max_replicas_before,
            max_replicas_after: p.summary.max_replicas_after,
            max_leaders_before: p.summary.max_leaders_before,
            max_leaders_after: p.summary.max_leaders_after,
        }),
        movements: p
            .movements
            .iter()
            .map(|m| pb::Movement {
                topic: m.topic.clone(),
                partition: m.partition,
                old_replicas: m.old_replicas.clone(),
                new_replicas: m.new_replicas.clone(),
                old_leader: m.old_leader,
                new_leader: m.new_leader,
            })
            .collect(),
        started_at_ms: p.started_at_ms,
        terminated_at_ms: p.terminated_at_ms,
        failure_reason: p.failure_reason.clone(),
        throttle_bytes_per_sec: p.throttle_bytes_per_sec,
    }
}

// ────────────────────────────────────────────────────────────────────────────
// RPC handlers
// ────────────────────────────────────────────────────────────────────────────

/// Read the latest cluster snapshot; 503 (`Unavailable`) if none yet.
pub async fn get_state(
    Extension(state): Extension<Arc<AppState>>,
    _req: ConnectRequest<pb::GetStateRequest>,
) -> Result<ConnectResponse<pb::GetStateResponse>, ConnectError> {
    let g = state.snapshot.load();
    let Some(cs) = (*g).as_ref() else {
        return Err(ConnectError::new(Code::Unavailable, "no snapshot yet"));
    };
    Ok(ConnectResponse::new(cluster_state_to_proto(cs)))
}

/// Run the optimizer with the selected goals, persist the proposal, return it.
pub async fn create_proposal(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::CreateProposalRequest>,
) -> Result<ConnectResponse<pb::Proposal>, ConnectError> {
    let g = state.snapshot.load();
    let Some(snap) = (*g).as_ref() else {
        return Err(ConnectError::new(Code::Unavailable, "no snapshot yet"));
    };
    let names = req.0.goals;
    let goals = state
        .goal_registry
        .select(&names)
        .map_err(|e| ConnectError::new(Code::InvalidArgument, e.to_string()))?;
    let out = optimizer::optimize(snap, &goals, &state.goal_ctx)
        .map_err(|e| ConnectError::new(Code::Internal, e.to_string()))?;
    state.store.insert(out.proposal.clone());
    state.metrics.proposals_created_total.inc();
    Ok(ConnectResponse::new(proposal_to_proto(&out.proposal)))
}

/// Look up a stored proposal and return summary + estimated cost
/// (estimated cost is 0 in 43a; slice 43e fills it in).
pub async fn dry_run_proposal(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::DryRunProposalRequest>,
) -> Result<ConnectResponse<pb::DryRunResponse>, ConnectError> {
    let id = req.0.id;
    let p = state
        .store
        .get(&id)
        .ok_or_else(|| ConnectError::new(Code::NotFound, format!("proposal `{id}` not found")))?;
    let proto = proposal_to_proto(&p);
    Ok(ConnectResponse::new(pb::DryRunResponse {
        id: p.id,
        summary: proto.summary,
        estimated_bytes_moved: 0,
    }))
}

/// Fetch a proposal by id; 404 (`NotFound`) if missing.
pub async fn get_proposal(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::GetProposalRequest>,
) -> Result<ConnectResponse<pb::Proposal>, ConnectError> {
    let id = req.0.id;
    let p = state
        .store
        .get(&id)
        .ok_or_else(|| ConnectError::new(Code::NotFound, format!("proposal `{id}` not found")))?;
    Ok(ConnectResponse::new(proposal_to_proto(&p)))
}

/// Return the most recent `limit` proposals (or capacity if `limit == 0`).
pub async fn list_proposals(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::ListProposalsRequest>,
) -> Result<ConnectResponse<pb::ListProposalsResponse>, ConnectError> {
    let limit = req.0.limit;
    let n = usize::try_from(limit).unwrap_or(0);
    let proposals = state.store.list(n).iter().map(proposal_to_proto).collect();
    Ok(ConnectResponse::new(pb::ListProposalsResponse {
        proposals,
    }))
}

/// Kick off an execution. Returns Executing-state proposal; operator
/// polls `GetProposal` for progress. Async — the executor runs on a
/// detached task.
pub async fn execute_proposal(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::ExecuteProposalRequest>,
) -> Result<ConnectResponse<pb::ExecuteProposalResponse>, ConnectError> {
    let inner = req.0;
    let id = inner.id;
    let throttle_bytes_per_sec = inner
        .throttle_bytes_per_sec
        .unwrap_or(state.executor.config.default_throttle_bytes_per_sec);

    let proposal = state.store.get(&id).ok_or_else(|| {
        ConnectError::new(Code::NotFound, format!("proposal `{id}` not found"))
    })?;
    if proposal.status.is_terminal() || matches!(proposal.status, ProposalStatus::Executing) {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            format!("proposal `{id}` is {:?} (must be Computed)", proposal.status),
        ));
    }
    if proposal.movements.is_empty() {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            format!("proposal `{id}` has no movements"),
        ));
    }

    let mut slot = state.executor.in_flight.lock().await;
    if slot.is_some() {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            "another execution is already in flight",
        ));
    }

    let now = now_ms();
    let updated = state
        .store
        .mutate(&id, |p| {
            p.status = ProposalStatus::Executing;
            p.started_at_ms = now;
            p.throttle_bytes_per_sec = throttle_bytes_per_sec;
        })
        .ok_or_else(|| ConnectError::new(Code::Internal, "store.mutate vanished"))?;

    let cancel = CancellationToken::new();
    let executor_state = state.executor.clone();
    let client = state.client_facade.clone();
    let prop_for_task = updated.clone();
    let cancel_for_task = cancel.clone();

    let task = tokio::spawn(async move {
        Execution::new(
            client,
            executor_state,
            prop_for_task,
            throttle_bytes_per_sec,
            cancel_for_task,
        )
        .run()
        .await;
    });

    *slot = Some(ExecutionHandle {
        proposal_id: id.clone(),
        task,
        cancel,
        started_at: std::time::Instant::now(),
    });
    drop(slot);

    state.executor.metrics.executions_started_total.inc();

    Ok(ConnectResponse(pb::ExecuteProposalResponse {
        proposal: Some(proposal_to_proto(&updated)),
    }))
}

/// Signal cancellation on the in-flight execution. Returns the proposal
/// already transitioned to `Cancelled`.
pub async fn cancel_execution(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::CancelExecutionRequest>,
) -> Result<ConnectResponse<pb::CancelExecutionResponse>, ConnectError> {
    let id = req.0.id;

    let cancel_token = {
        let slot = state.executor.in_flight.lock().await;
        let Some(handle) = slot.as_ref() else {
            return Err(ConnectError::new(
                Code::NotFound,
                "no execution in flight",
            ));
        };
        if handle.proposal_id != id {
            return Err(ConnectError::new(
                Code::FailedPrecondition,
                format!(
                    "in-flight execution is `{}`, not `{id}`",
                    handle.proposal_id
                ),
            ));
        }
        handle.cancel.clone()
    };

    cancel_token.cancel();

    // Spin briefly waiting for the executor task to release the slot
    // and update the store. Bound to 5s; if the executor doesn't drain
    // in that time, return the current (Executing) proposal — the
    // operator can re-poll.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let proposal = state
            .store
            .get(&id)
            .ok_or_else(|| ConnectError::new(Code::NotFound, format!("proposal `{id}` vanished")))?;
        if matches!(
            proposal.status,
            ProposalStatus::Cancelled | ProposalStatus::Failed | ProposalStatus::Completed
        ) {
            return Ok(ConnectResponse(pb::CancelExecutionResponse {
                proposal: Some(proposal_to_proto(&proposal)),
            }));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(ConnectResponse(pb::CancelExecutionResponse {
                proposal: Some(proposal_to_proto(&proposal)),
            }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
