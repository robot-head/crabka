//! One Rust fn per Connect-RPC method. Each one takes axum's `Extension`,
//! which carries `Arc<AppState>`, and a typed `ConnectRequest`. Each one
//! returns a typed `ConnectResponse` or a `ConnectError`.
//!
//! The connectrpc-axum-build 0.1 codegen produces a *builder*, not a trait.
//! `pb::rebalancer_connect::RebalancerServiceBuilder` accepts these
//! freestanding async fns through `post_connect`-style wiring. The builder is
//! parameterized over the axum router's `S = ()` state, so the crate
//! propagates the per-server `AppState` through an `Extension` layer rather
//! than through typed state. That keeps the codegen S generic at `()` and
//! avoids `FromRef` and `with_state` plumbing.

use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse, error::Code};
use crabka_units::{
    ByteRate, Time,
    convert::{ByteRateExt as _, StdDurationExt as _, TimeExt as _},
};
#[cfg(test)]
use crabka_units::{millis, secs};
use tokio_util::sync::CancellationToken;

use crate::{
    executor::{
        Execution, ExecutionHandle,
        state::{InFlightFile, Phase},
    },
    ingest::SharedSnapshot,
    metrics::RebalancerMetrics,
    model::{ClusterState, ProposalStore, proposal::ProposalStatus},
    optimizer, pb,
    time::now_ms,
};

/// State shared across all RPC handlers. The generated router receives it
/// through an `Extension(Arc<AppState>)` layer.
pub struct AppState {
    pub snapshot: SharedSnapshot,
    pub store: Arc<ProposalStore>,
    // Arc so the binary can share one registry instance between
    // `AppState` and the `Detector` (which holds it for auto_trigger).
    pub goal_registry: Arc<super::GoalRegistry>,
    pub goal_ctx: crate::goals::GoalContext,
    pub metrics: RebalancerMetrics,
    pub executor: crate::executor::ExecutorState,
    pub client_facade: Arc<dyn crate::executor::phases::ClientFacade>,
    pub anomaly_store: Arc<crate::detector::AnomalyStore>,
    // Gates /readyz and execute_proposal.
    pub state_topic: Arc<dyn crate::state_topic::StateBackend>,
    pub cancel_drain_timeout: Time,
    pub cancel_drain_poll_interval: Time,
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
        throttle_bytes_per_sec: p.throttle.bytes_per_sec_i64(),
    }
}

#[must_use]
pub fn anomaly_to_proto(a: &crate::detector::Anomaly) -> pb::Anomaly {
    pb::Anomaly {
        id: a.id.clone(),
        kind: i32::from(anomaly_kind_to_proto(a.kind)),
        key: Some(anomaly_key_to_proto(&a.key)),
        severity: i32::from(anomaly_severity_to_proto(a.severity)),
        detected_at_ms: a.detected_at_ms,
        last_seen_at_ms: a.last_seen_at_ms,
        resolved_at_ms: a.resolved_at_ms.unwrap_or(0),
        triggered_proposal_id: a.triggered_proposal_id.clone(),
        mute_until_ms: a.mute_until_ms.unwrap_or(0),
        details: a.details.clone(),
    }
}

#[must_use]
fn anomaly_kind_to_proto(k: crate::detector::AnomalyKind) -> pb::AnomalyKind {
    match k {
        crate::detector::AnomalyKind::BrokerDeath => pb::AnomalyKind::BrokerDeath,
        crate::detector::AnomalyKind::UnderReplicatedPartitions => {
            pb::AnomalyKind::UnderReplicatedPartitions
        }
        crate::detector::AnomalyKind::DiskPressure => pb::AnomalyKind::DiskPressure,
        crate::detector::AnomalyKind::SlowBroker => pb::AnomalyKind::SlowBroker,
    }
}

#[must_use]
fn anomaly_severity_to_proto(s: crate::detector::AnomalySeverity) -> pb::AnomalySeverity {
    match s {
        crate::detector::AnomalySeverity::Warning => pb::AnomalySeverity::Warning,
        crate::detector::AnomalySeverity::Critical => pb::AnomalySeverity::Critical,
    }
}

#[must_use]
fn anomaly_key_to_proto(k: &crate::detector::AnomalyKey) -> pb::AnomalyKey {
    use pb::anomaly_key::Inner;
    let inner = match k {
        crate::detector::AnomalyKey::Broker(id) => Inner::Broker(*id),
        crate::detector::AnomalyKey::Partition { topic, partition } => {
            Inner::Partition(pb::PartitionKey {
                topic: topic.clone(),
                partition: *partition,
            })
        }
        crate::detector::AnomalyKey::BrokerPartition {
            broker,
            topic,
            partition,
        } => Inner::BrokerPartition(pb::BrokerPartitionKey {
            broker: *broker,
            topic: topic.clone(),
            partition: *partition,
        }),
    };
    pb::AnomalyKey { inner: Some(inner) }
}

// ────────────────────────────────────────────────────────────────────────────
// RPC handlers
// ────────────────────────────────────────────────────────────────────────────

/// Read the latest cluster snapshot. Returns 503 (`Unavailable`) if there is
/// no snapshot yet.
#[tracing::instrument(level = "info", skip_all, err(Debug))]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(goals = req.0.goals.len()),
    err(Debug),
)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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
    let started = std::time::Instant::now();
    let out = optimizer::optimize(snap, &goals, &state.goal_ctx).map_err(|e| {
        state.metrics.record_rebalance("error");
        ConnectError::new(Code::Internal, e.to_string())
    })?;
    state
        .metrics
        .observe_rebalance_duration(started.elapsed().as_time());
    state
        .metrics
        .record_rebalance(if out.proposal.movements.is_empty() {
            "no_movements"
        } else {
            "ok"
        });
    state.store.insert(out.proposal.clone());
    state.metrics.proposals_created_total.inc();
    Ok(ConnectResponse::new(proposal_to_proto(&out.proposal)))
}

/// Look up a stored proposal and return its summary and estimated cost.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

/// Fetch a proposal by id. Returns 404 (`NotFound`) if it is missing.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

/// Return the most recent `limit` proposals, or the full capacity if
/// `limit == 0`.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

/// Start an execution and return the proposal in the Executing state.
///
/// The call is asynchronous: the executor runs on a detached task. The
/// operator polls `GetProposal` for progress.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(proposal_id = %req.0.id),
    err(Debug),
)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn execute_proposal(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::ExecuteProposalRequest>,
) -> Result<ConnectResponse<pb::ExecuteProposalResponse>, ConnectError> {
    if !state.state_topic.is_loaded() {
        return Err(ConnectError::new(
            Code::Unavailable,
            "state topic not yet loaded; retry shortly",
        ));
    }
    let inner = req.0;
    let id = inner.id;
    let throttle = inner.throttle_bytes_per_sec.map_or(
        state.executor.config.default_throttle,
        ByteRate::from_bytes_per_sec,
    );

    let proposal = state
        .store
        .get(&id)
        .ok_or_else(|| ConnectError::new(Code::NotFound, format!("proposal `{id}` not found")))?;
    if proposal.status.is_terminal() || matches!(proposal.status, ProposalStatus::Executing) {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            format!(
                "proposal `{id}` is {:?} (must be Computed)",
                proposal.status
            ),
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

    // Persist the initial in-flight record to the state topic BEFORE mutating
    // proposals.json. Recovery keys off the topic record; writing proposals.json
    // first opens a crash window where the proposal is `Executing` but no
    // recovery marker exists, leaving the proposal orphaned.
    let in_flight_file = InFlightFile::new(id.clone(), Phase::ApplyThrottle, now, throttle);
    if let Err(e) = state.state_topic.write(&in_flight_file).await {
        return Err(ConnectError::new(
            Code::Internal,
            format!("failed to persist in-flight state to topic: {e}"),
        ));
    }

    let Some(updated) = state.store.mutate(&id, |p| {
        p.status = ProposalStatus::Executing;
        p.started_at_ms = now;
        p.throttle = throttle;
    }) else {
        // Best-effort cleanup: tombstone the state topic record we just wrote.
        let topic = state.state_topic.clone();
        tokio::spawn(async move {
            let _ = topic.delete().await;
        });
        return Err(ConnectError::new(Code::Internal, "store.mutate vanished"));
    };

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
            throttle,
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
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(proposal_id = %req.0.id),
    err(Debug),
)]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn cancel_execution(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::CancelExecutionRequest>,
) -> Result<ConnectResponse<pb::CancelExecutionResponse>, ConnectError> {
    let id = req.0.id;

    let cancel_token = {
        let slot = state.executor.in_flight.lock().await;
        let Some(handle) = slot.as_ref() else {
            return Err(ConnectError::new(Code::NotFound, "no execution in flight"));
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
    let deadline = cancel_poll_deadline(std::time::Instant::now(), state.cancel_drain_timeout);
    loop {
        let proposal = state.store.get(&id).ok_or_else(|| {
            ConnectError::new(Code::NotFound, format!("proposal `{id}` vanished"))
        })?;
        if matches!(
            proposal.status,
            ProposalStatus::Cancelled | ProposalStatus::Failed | ProposalStatus::Completed
        ) {
            return Ok(ConnectResponse(pb::CancelExecutionResponse {
                proposal: Some(proposal_to_proto(&proposal)),
            }));
        }
        if cancel_poll_expired(std::time::Instant::now(), deadline) {
            return Ok(ConnectResponse(pb::CancelExecutionResponse {
                proposal: Some(proposal_to_proto(&proposal)),
            }));
        }
        tokio::time::sleep(state.cancel_drain_poll_interval.to_std()).await;
    }
}

fn cancel_poll_deadline(now: std::time::Instant, wait: Time) -> std::time::Instant {
    now + wait.to_std()
}

fn cancel_poll_expired(now: std::time::Instant, deadline: std::time::Instant) -> bool {
    now >= deadline
}

/// Read the anomaly history. `limit = 0` returns up to the `AnomalyStore`'s
/// full capacity.
///
/// `include_resolved` defaults to `true` when unset. The wire's default boolean
/// false would surprise the caller, because most callers want the full history
/// surface.
#[tracing::instrument(level = "info", skip_all, err(Debug))]
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn get_anomalies(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::GetAnomaliesRequest>,
) -> Result<ConnectResponse<pb::GetAnomaliesResponse>, ConnectError> {
    let inner = req.0;
    let limit = usize::try_from(inner.limit).unwrap_or(0);
    let include_resolved = inner.include_resolved.unwrap_or(true);
    let items = state.anomaly_store.list(limit, include_resolved);
    let proto: Vec<pb::Anomaly> = items.iter().map(anomaly_to_proto).collect();
    Ok(ConnectResponse::new(pb::GetAnomaliesResponse {
        anomalies: proto,
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use async_trait::async_trait;
    use crabka_units::{bytes_per_sec, percent};

    use super::*;
    use crate::{
        capacity::BrokerCapacities,
        executor::{
            ExecutorConfig, ExecutorState,
            phases::{ClientFacade, ConfigOp, PhaseError},
            throttle::ThrottleTargets,
        },
        goals::GoalContext,
        ingest::new_shared_snapshot,
        metrics::RebalancerMetrics,
        model::proposal::{Movement, Proposal, ProposalSummary},
        scraper::UsageStore,
    };

    /// Local no-op `ClientFacade` for handler-level unit tests. These tests do
    /// not run the executor. They only need a type-correct `client_facade`
    /// field on `AppState`.
    struct NoopClient;

    #[async_trait]
    impl ClientFacade for NoopClient {
        async fn alter_throttle_configs(
            &self,
            _op: ConfigOp,
            _targets: &ThrottleTargets,
            _throttle: ByteRate,
        ) -> Result<(), PhaseError> {
            Ok(())
        }
        async fn submit_reassignments(&self, _movements: &[Movement]) -> Result<(), PhaseError> {
            Ok(())
        }
        async fn cancel_reassignments(
            &self,
            _partitions: &[(String, i32)],
        ) -> Result<(), PhaseError> {
            Ok(())
        }
        async fn list_in_flight(
            &self,
            _of_interest: &[(String, i32)],
        ) -> Result<Vec<(String, i32)>, PhaseError> {
            // Return an empty list so the executor would complete immediately
            // if it ever ran — but these handler tests don't actually let it run.
            Ok(vec![])
        }
    }

    fn build_app_state(dir: &std::path::Path) -> Arc<AppState> {
        let store = Arc::new(ProposalStore::new(20));
        let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
        let metrics = RebalancerMetrics::register(&mut registry);
        let executor = ExecutorState {
            store: store.clone(),
            config: ExecutorConfig {
                data_dir: dir.to_path_buf(),
                default_throttle: bytes_per_sec(50_000_000),
                poll_interval: millis(50),
                execute_deadline: secs(30),
                batch_size: 200,
            },
            metrics: metrics.clone(),
            in_flight: Arc::new(tokio::sync::Mutex::new(None)),
            state_topic: std::sync::Arc::new(
                crate::state_topic::fake::InMemoryBackend::new_loaded(),
            ),
        };
        let client_facade: Arc<dyn ClientFacade> = Arc::new(NoopClient);
        Arc::new(AppState {
            snapshot: new_shared_snapshot(),
            store,
            goal_registry: Arc::new(crate::api::GoalRegistry::default_registry()),
            goal_ctx: GoalContext {
                imbalance_threshold: percent(10),
                max_movements_per_proposal: 256,
                min_topic_leaders_per_broker: 0,
                broker_capacities: Arc::new(BrokerCapacities::default()),
                broker_usages: Arc::new(UsageStore::default()),
            },
            metrics,
            executor,
            client_facade,
            anomaly_store: Arc::new(crate::detector::AnomalyStore::new(20)),
            state_topic: Arc::new(crate::state_topic::fake::InMemoryBackend::new_loaded()),
            cancel_drain_timeout: secs(5),
            cancel_drain_poll_interval: millis(25),
        })
    }

    fn insert_computed_proposal(state: &AppState, id: &str, movements: Vec<Movement>) {
        state.store.insert(Proposal {
            id: id.into(),
            status: ProposalStatus::Computed,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements,
            started_at_ms: 0,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle: ByteRate::ZERO,
        });
    }

    fn mv(topic: &str, p: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition: p,
            old_replicas: old,
            new_replicas: new,
            old_leader: 0,
            new_leader: 0,
        }
    }

    fn req<T>(msg: T) -> ConnectRequest<T> {
        ConnectRequest(msg)
    }

    #[tokio::test]
    async fn execute_proposal_zero_movements_returns_failed_precondition() {
        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());
        insert_computed_proposal(&state, "p", vec![]);

        let err = execute_proposal(
            Extension(state),
            req(pb::ExecuteProposalRequest {
                id: "p".into(),
                throttle_bytes_per_sec: None,
            }),
        )
        .await
        .expect_err("expected FailedPrecondition");
        assert2::assert!(err.code() == Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn execute_proposal_unknown_id_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());

        let err = execute_proposal(
            Extension(state),
            req(pb::ExecuteProposalRequest {
                id: "ghost".into(),
                throttle_bytes_per_sec: None,
            }),
        )
        .await
        .expect_err("expected NotFound");
        assert2::assert!(err.code() == Code::NotFound);
    }

    #[tokio::test]
    async fn execute_proposal_terminal_status_returns_failed_precondition() {
        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());
        state.store.insert(Proposal {
            id: "done".into(),
            status: ProposalStatus::Completed,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements: vec![mv("t", 0, vec![1], vec![2])],
            started_at_ms: 1,
            terminated_at_ms: 2,
            failure_reason: None,
            throttle: ByteRate::ZERO,
        });

        let err = execute_proposal(
            Extension(state),
            req(pb::ExecuteProposalRequest {
                id: "done".into(),
                throttle_bytes_per_sec: None,
            }),
        )
        .await
        .expect_err("expected FailedPrecondition");
        assert2::assert!(err.code() == Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn execute_proposal_sets_started_at_and_throttle_from_request() {
        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());
        insert_computed_proposal(&state, "p", vec![mv("t", 0, vec![1], vec![2])]);

        let before = now_ms();
        let resp = execute_proposal(
            Extension(state.clone()),
            req(pb::ExecuteProposalRequest {
                id: "p".into(),
                throttle_bytes_per_sec: Some(12345),
            }),
        )
        .await
        .expect("execute proposal");

        let proposal = resp.0.proposal.expect("proposal in response");
        check!(
            (
                proposal.status,
                proposal.started_at_ms >= before,
                proposal.throttle_bytes_per_sec,
            ) == (i32::from(pb::ProposalStatus::Executing), true, 12345)
        );

        if let Some(handle) = state.executor.in_flight.lock().await.take() {
            handle.cancel.cancel();
            let _ = tokio::time::timeout(secs(1).to_std(), handle.task).await;
        }
    }

    #[tokio::test]
    async fn cancel_execution_when_idle_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());

        let err = cancel_execution(
            Extension(state),
            req(pb::CancelExecutionRequest {
                id: "anything".into(),
            }),
        )
        .await
        .expect_err("expected NotFound");
        assert2::assert!(err.code() == Code::NotFound);
    }

    #[test]
    fn cancel_poll_deadline_and_expiry_are_strict_until_deadline() {
        let now = std::time::Instant::now();
        let deadline = cancel_poll_deadline(now, secs(5));
        assert2::assert!(!cancel_poll_expired(now + secs(4).to_std(), deadline));
        assert2::assert!(cancel_poll_expired(now + secs(5).to_std(), deadline));
    }

    #[test]
    fn anomaly_kind_to_proto_covers_all_variants() {
        use crate::detector::AnomalyKind;
        for (_case, kind, want) in [
            (
                "broker death",
                AnomalyKind::BrokerDeath,
                pb::AnomalyKind::BrokerDeath,
            ),
            (
                "under-replicated partitions",
                AnomalyKind::UnderReplicatedPartitions,
                pb::AnomalyKind::UnderReplicatedPartitions,
            ),
            (
                "disk pressure",
                AnomalyKind::DiskPressure,
                pb::AnomalyKind::DiskPressure,
            ),
            (
                "slow broker",
                AnomalyKind::SlowBroker,
                pb::AnomalyKind::SlowBroker,
            ),
        ] {
            assert2::assert!(anomaly_kind_to_proto(kind) == want);
        }
    }

    #[test]
    fn anomaly_severity_to_proto_covers_all_variants() {
        use crate::detector::AnomalySeverity;
        for (_name, severity, expected) in [
            (
                "warning",
                AnomalySeverity::Warning,
                pb::AnomalySeverity::Warning,
            ),
            (
                "critical",
                AnomalySeverity::Critical,
                pb::AnomalySeverity::Critical,
            ),
        ] {
            assert2::assert!(anomaly_severity_to_proto(severity) == expected);
        }
    }

    #[test]
    fn anomaly_to_proto_maps_all_fields() {
        use crate::detector::{Anomaly, AnomalyKey, AnomalyKind, AnomalySeverity};
        let anomaly = Anomaly {
            id: "a1".into(),
            kind: AnomalyKind::SlowBroker,
            key: AnomalyKey::BrokerPartition {
                broker: 7,
                topic: "orders".into(),
                partition: 3,
            },
            severity: AnomalySeverity::Critical,
            detected_at_ms: 10,
            last_seen_at_ms: 20,
            resolved_at_ms: Some(30),
            triggered_proposal_id: Some("p1".into()),
            mute_until_ms: Some(40),
            details: "slow".into(),
        };

        let proto = anomaly_to_proto(&anomaly);

        assert2::assert!(
            proto
                == pb::Anomaly {
                    id: "a1".into(),
                    kind: i32::from(pb::AnomalyKind::SlowBroker),
                    key: Some(pb::AnomalyKey {
                        inner: Some(pb::anomaly_key::Inner::BrokerPartition(
                            pb::BrokerPartitionKey {
                                broker: 7,
                                topic: "orders".into(),
                                partition: 3,
                            }
                        )),
                    }),
                    severity: i32::from(pb::AnomalySeverity::Critical),
                    detected_at_ms: 10,
                    last_seen_at_ms: 20,
                    resolved_at_ms: 30,
                    triggered_proposal_id: Some("p1".into()),
                    mute_until_ms: 40,
                    details: "slow".into(),
                }
        );
    }

    #[test]
    fn anomaly_key_to_proto_roundtrips_each_variant() {
        use crate::detector::AnomalyKey;
        let b = anomaly_key_to_proto(&AnomalyKey::Broker(7));
        assert2::assert!(matches!(b.inner, Some(pb::anomaly_key::Inner::Broker(7))));

        let p = anomaly_key_to_proto(&AnomalyKey::Partition {
            topic: "t".into(),
            partition: 3,
        });
        assert2::assert!(matches!(
            p.inner,
            Some(pb::anomaly_key::Inner::Partition(_))
        ));

        let bp = anomaly_key_to_proto(&AnomalyKey::BrokerPartition {
            broker: 1,
            topic: "t".into(),
            partition: 2,
        });
        assert2::assert!(matches!(
            bp.inner,
            Some(pb::anomaly_key::Inner::BrokerPartition(_))
        ));
    }

    #[tokio::test]
    async fn get_anomalies_returns_empty_when_store_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());
        let resp = get_anomalies(
            Extension(state),
            ConnectRequest(pb::GetAnomaliesRequest {
                limit: 0,
                include_resolved: None,
            }),
        )
        .await
        .expect("handler should succeed");
        assert2::assert!(resp.0.anomalies.is_empty());
    }

    #[tokio::test]
    async fn cancel_execution_id_mismatch_returns_failed_precondition() {
        use tokio_util::sync::CancellationToken;

        use crate::executor::ExecutionHandle;

        let dir = tempfile::tempdir().unwrap();
        let state = build_app_state(dir.path());

        // Pre-stage an in-flight handle for a different proposal.
        let cancel = CancellationToken::new();
        let dummy_task = tokio::spawn(async {});
        *state.executor.in_flight.lock().await = Some(ExecutionHandle {
            proposal_id: "actually-running".into(),
            task: dummy_task,
            cancel,
            started_at: std::time::Instant::now(),
        });

        let err = cancel_execution(
            Extension(state),
            req(pb::CancelExecutionRequest {
                id: "different-id".into(),
            }),
        )
        .await
        .expect_err("expected FailedPrecondition");
        assert2::assert!(err.code() == Code::FailedPrecondition);
    }
}
