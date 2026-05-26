//! Slice 44: `KafkaRebalance` reconciler.
//!
//! Drives the standalone `crabka-rebalancer` service through its
//! Connect-RPC API and reflects the proposal lifecycle into the CRD
//! status. The state machine is Strimzi-shaped and annotation-driven:
//!
//! ```text
//!  (new) ──CreateProposal──▶ ProposalReady ──approve/Execute──▶ Rebalancing
//!                                  │                                  │
//!                                  │ refresh                          │ poll
//!                                  ▼                          ┌───────┼────────┐
//!                            (recompute)                   Ready  NotReady  (stop→Stopped)
//! ```
//!
//! The optimizer call (`CreateProposal`) never mutates the cluster — only
//! `approve` (→ `ExecuteProposal`) does. This keeps a human (or `GitOps`
//! approval) in the loop before any partition data moves.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Resource, ResourceExt as _};
use serde_json::json;

use crate::context::Context;
use crate::controller::common::{FIELD_MANAGER, ReconcileError, condition};
use crate::crd::{KafkaRebalance, OptimizationResult};
use crate::rebalancer_client::{ProposalStatus, RebalancerError, RebalancerProposal};

/// Annotation that drives the rebalance state machine. Mirrors Strimzi's
/// `strimzi.io/rebalance`.
const ANNOTATION: &str = "crabka.io/rebalance";
/// Default Connect-RPC port the rebalancer binds (`--listen-addr`).
const REBALANCER_PORT: u16 = 9300;

const POLL_INTERVAL: Duration = Duration::from_secs(10);
const IDLE_INTERVAL: Duration = Duration::from_mins(5);
const TRANSPORT_RETRY: Duration = Duration::from_secs(15);

/// The rebalance lifecycle state. Surfaced as the active condition's
/// `type` in the CRD status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumString)]
pub enum RebalanceState {
    New,
    ProposalReady,
    Rebalancing,
    Ready,
    NotReady,
    Stopped,
}

impl RebalanceState {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }

    #[must_use]
    fn from_condition_type(t: &str) -> Option<Self> {
        t.parse().ok()
    }
}

/// The annotation-encoded operator command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceCommand {
    Approve,
    Refresh,
    Stop,
}

impl RebalanceCommand {
    #[must_use]
    fn parse(v: &str) -> Option<Self> {
        match v.trim() {
            "approve" => Some(Self::Approve),
            "refresh" => Some(Self::Refresh),
            "stop" => Some(Self::Stop),
            _ => None,
        }
    }
}

/// The RPC the reconcile should issue this pass. Pure output of
/// [`decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceAction {
    /// Compute a fresh proposal (`CreateProposal`).
    CreateProposal,
    /// Execute the stored proposal (`ExecuteProposal`).
    Execute,
    /// Poll the in-flight execution (`GetProposal`).
    PollExecution,
    /// Cancel the in-flight execution (`CancelExecution`).
    Cancel,
    /// Nothing to do this pass (await approval / terminal state /
    /// no-op command).
    Idle,
}

/// Pure state-machine core: given the current state, an optional pending
/// command, and whether a proposal id is on file, decide which RPC to
/// issue. Fully unit-tested below — the reconcile fn only does I/O.
#[must_use]
pub fn decide(
    state: RebalanceState,
    command: Option<RebalanceCommand>,
    has_session: bool,
) -> RebalanceAction {
    match command {
        // Refresh always recomputes, from any state.
        Some(RebalanceCommand::Refresh) => RebalanceAction::CreateProposal,
        // Stop only means something while executing.
        Some(RebalanceCommand::Stop) => {
            if state == RebalanceState::Rebalancing && has_session {
                RebalanceAction::Cancel
            } else {
                RebalanceAction::Idle
            }
        }
        // Approve only means something with a ready proposal on file.
        Some(RebalanceCommand::Approve) => {
            if state == RebalanceState::ProposalReady && has_session {
                RebalanceAction::Execute
            } else {
                RebalanceAction::Idle
            }
        }
        None => match state {
            RebalanceState::New => RebalanceAction::CreateProposal,
            // Poll the in-flight execution; a Rebalancing status without a
            // session id is corrupt, so recompute from scratch.
            RebalanceState::Rebalancing => {
                if has_session {
                    RebalanceAction::PollExecution
                } else {
                    RebalanceAction::CreateProposal
                }
            }
            // ProposalReady awaits approval; terminal states await refresh.
            RebalanceState::ProposalReady
            | RebalanceState::Ready
            | RebalanceState::NotReady
            | RebalanceState::Stopped => RebalanceAction::Idle,
        },
    }
}

/// The resolved status outcome of a reconcile pass.
struct Outcome {
    state: RebalanceState,
    reason: String,
    message: String,
    requeue: Duration,
    /// Set when a fresh proposal id should be recorded (`CreateProposal`).
    new_session: Option<String>,
    /// Set when a fresh optimization result should be recorded.
    new_optimization: Option<OptimizationResult>,
    /// Advance `observedGeneration` to the current generation (only when
    /// a new proposal is computed against the current spec).
    advance_generation: bool,
}

impl Outcome {
    fn from_create(p: &RebalancerProposal) -> Self {
        if p.status == ProposalStatus::Computed {
            Self {
                state: RebalanceState::ProposalReady,
                reason: "ProposalReady".into(),
                message: format!(
                    "proposal {} computed: {} replica / {} leader movements",
                    p.id, p.summary.replica_movements, p.summary.leader_movements
                ),
                requeue: IDLE_INTERVAL,
                new_session: Some(p.id.clone()),
                new_optimization: Some(optimization_result_from(p)),
                advance_generation: true,
            }
        } else {
            Self {
                state: RebalanceState::NotReady,
                reason: "UnexpectedProposalStatus".into(),
                message: format!("CreateProposal returned non-Computed status for {}", p.id),
                requeue: IDLE_INTERVAL,
                new_session: Some(p.id.clone()),
                new_optimization: None,
                advance_generation: false,
            }
        }
    }

    /// Map an `ExecuteProposal` / `GetProposal` (poll) result onto a state.
    fn from_execute_or_poll(p: &RebalancerProposal) -> Self {
        match p.status {
            ProposalStatus::Executing | ProposalStatus::Computed => Self::transient(
                RebalanceState::Rebalancing,
                "Rebalancing",
                format!("executing proposal {}", p.id),
                POLL_INTERVAL,
            ),
            ProposalStatus::Completed => Self::transient(
                RebalanceState::Ready,
                "Ready",
                format!("proposal {} completed", p.id),
                IDLE_INTERVAL,
            ),
            ProposalStatus::Failed => Self::transient(
                RebalanceState::NotReady,
                "RebalanceFailed",
                p.failure_reason
                    .clone()
                    .unwrap_or_else(|| format!("proposal {} failed", p.id)),
                IDLE_INTERVAL,
            ),
            ProposalStatus::Cancelled => Self::transient(
                RebalanceState::Stopped,
                "Stopped",
                format!("proposal {} cancelled", p.id),
                IDLE_INTERVAL,
            ),
            ProposalStatus::Unspecified => Self::transient(
                RebalanceState::NotReady,
                "UnexpectedProposalStatus",
                format!("proposal {} reported an unknown status", p.id),
                IDLE_INTERVAL,
            ),
        }
    }

    fn from_cancel(p: &RebalancerProposal) -> Self {
        Self::transient(
            RebalanceState::Stopped,
            "Stopped",
            format!("execution of proposal {} cancelled", p.id),
            IDLE_INTERVAL,
        )
    }

    /// An RPC-level error from the rebalancer (`failed_precondition`,
    /// `not_found`, …). Surfaces as `NotReady`.
    fn from_rpc_error(e: &RebalancerError) -> Self {
        Self::transient(
            RebalanceState::NotReady,
            "RebalancerError",
            e.to_string(),
            IDLE_INTERVAL,
        )
    }

    /// A status with no proposal-id / optimization changes.
    fn transient(state: RebalanceState, reason: &str, message: String, requeue: Duration) -> Self {
        Self {
            state,
            reason: reason.into(),
            message,
            requeue,
            new_session: None,
            new_optimization: None,
            advance_generation: false,
        }
    }
}

/// Project a rebalancer `Proposal` summary onto the CRD status type.
#[must_use]
pub fn optimization_result_from(p: &RebalancerProposal) -> OptimizationResult {
    OptimizationResult {
        replica_movements: p.summary.replica_movements,
        leader_movements: p.summary.leader_movements,
        max_replicas_before: p.summary.max_replicas_before,
        max_replicas_after: p.summary.max_replicas_after,
        max_leaders_before: p.summary.max_leaders_before,
        max_leaders_after: p.summary.max_leaders_after,
        goals: p.goals_applied.clone(),
    }
}

/// Current state derived from the active (`status: "True"`) condition.
/// Defaults to `New` when there's no recognized condition yet.
#[must_use]
pub fn current_state(obj: &KafkaRebalance) -> RebalanceState {
    obj.status
        .as_ref()
        .map(|s| s.conditions.as_slice())
        .unwrap_or_default()
        .iter()
        .rev()
        .find(|c| c.status == "True")
        .and_then(|c| RebalanceState::from_condition_type(&c.type_))
        .unwrap_or(RebalanceState::New)
}

/// The pending command from the `crabka.io/rebalance` annotation, if any.
#[must_use]
pub fn read_command(obj: &KafkaRebalance) -> Option<RebalanceCommand> {
    obj.meta()
        .annotations
        .as_ref()
        .and_then(|a| a.get(ANNOTATION))
        .and_then(|v| RebalanceCommand::parse(v))
}

/// Resolve the rebalancer Connect base URL: `spec.endpoint` wins; else
/// derive `http://<cluster>-rebalancer.<ns>.svc.cluster.local:9300` from
/// the `crabka.io/cluster` label. Returns `None` when neither is present.
#[must_use]
pub fn resolve_endpoint(obj: &KafkaRebalance, namespace: &str) -> Option<String> {
    if let Some(ep) = obj.spec.endpoint.as_ref().filter(|s| !s.is_empty()) {
        return Some(ep.clone());
    }
    let cluster = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster"))
        .filter(|s| !s.is_empty())?;
    Some(format!(
        "http://{cluster}-rebalancer.{namespace}.svc.cluster.local:{REBALANCER_PORT}"
    ))
}

/// Run the controller forever.
pub async fn run(ctx: Context) -> anyhow::Result<()> {
    let api: Api<KafkaRebalance> = Api::all(ctx.client.clone());
    Controller::new(api, watcher::Config::default())
        .run(reconcile, error_policy, Arc::new(ctx))
        .for_each(|res| async move {
            match res {
                Ok((obj, _)) => tracing::debug!(?obj, "rebalance reconciled"),
                Err(e) => tracing::warn!(error = %e, "rebalance reconcile error"),
            }
        })
        .await;
    Ok(())
}

pub fn error_policy(_obj: Arc<KafkaRebalance>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "rebalance reconcile error, requeueing");
    Action::requeue(TRANSPORT_RETRY)
}

pub async fn reconcile(
    obj: Arc<KafkaRebalance>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let api: Api<KafkaRebalance> = Api::namespaced(ctx.client.clone(), &ns);

    // 1. Resolve the rebalancer endpoint.
    let Some(endpoint) = resolve_endpoint(&obj, &ns) else {
        write_status(
            &api,
            &name,
            &obj,
            &Outcome::transient(
                RebalanceState::NotReady,
                "MissingEndpoint",
                "spec.endpoint is unset and no crabka.io/cluster label is present to derive it"
                    .into(),
                IDLE_INTERVAL,
            ),
        )
        .await?;
        return Ok(Action::requeue(IDLE_INTERVAL));
    };

    // 2. Decide what to do.
    let state = current_state(&obj);
    let command = read_command(&obj);
    let session = obj.status.as_ref().and_then(|s| s.session_id.clone());
    let action = decide(state, command, session.is_some());

    // 3. Idle: nothing to call. Consume any stray command, otherwise just
    //    requeue without touching status.
    if action == RebalanceAction::Idle {
        if command.is_some() {
            remove_command_annotation(&api, &name).await?;
        }
        return Ok(Action::requeue(IDLE_INTERVAL));
    }

    // 4. Issue the RPC.
    let client = ctx.rebalancer_client_for(&endpoint).await;
    let rpc_result = match action {
        RebalanceAction::CreateProposal => {
            let goals = obj.spec.goals.clone().unwrap_or_default();
            client
                .create_proposal(&goals)
                .await
                .map(|p| Outcome::from_create(&p))
        }
        RebalanceAction::Execute => {
            let id = session.clone().unwrap_or_default();
            client
                .execute_proposal(&id, obj.spec.throttle_bytes_per_sec)
                .await
                .map(|p| Outcome::from_execute_or_poll(&p))
        }
        RebalanceAction::PollExecution => {
            let id = session.clone().unwrap_or_default();
            client
                .get_proposal(&id)
                .await
                .map(|p| Outcome::from_execute_or_poll(&p))
        }
        RebalanceAction::Cancel => {
            let id = session.clone().unwrap_or_default();
            client
                .cancel_execution(&id)
                .await
                .map(|p| Outcome::from_cancel(&p))
        }
        RebalanceAction::Idle => unreachable!("Idle handled above"),
    };

    // 5. Map the result. Transient transport failures leave status
    //    untouched (and the command in place) so the next pass retries.
    let outcome = match rpc_result {
        Ok(o) => o,
        Err(RebalancerError::Transport(msg)) => {
            tracing::warn!(error = %msg, %endpoint, "rebalancer unreachable; retrying");
            ctx.drop_rebalancer_client(&endpoint).await;
            return Ok(Action::requeue(TRANSPORT_RETRY));
        }
        Err(e) => Outcome::from_rpc_error(&e),
    };

    // 6. A command drove this pass (or was a no-op alongside it): consume
    //    it now that we reached a terminal decision.
    if command.is_some() {
        remove_command_annotation(&api, &name).await?;
    }

    // 7. Persist status, carrying forward the existing session / result
    //    when the outcome didn't produce new ones.
    let requeue = outcome.requeue;
    write_status(&api, &name, &obj, &outcome).await?;
    Ok(Action::requeue(requeue))
}

/// Merge-patch the status. Carries forward `sessionId` /
/// `optimizationResult` / `observedGeneration` when the outcome doesn't
/// set new values, so a poll pass never wipes the computed result.
async fn write_status(
    api: &Api<KafkaRebalance>,
    name: &str,
    obj: &KafkaRebalance,
    outcome: &Outcome,
) -> Result<(), ReconcileError> {
    let existing = obj.status.as_ref();
    let session_id = outcome
        .new_session
        .clone()
        .or_else(|| existing.and_then(|s| s.session_id.clone()));
    let optimization_result = outcome
        .new_optimization
        .clone()
        .or_else(|| existing.and_then(|s| s.optimization_result.clone()));
    let observed_generation = if outcome.advance_generation {
        obj.meta().generation
    } else {
        existing.and_then(|s| s.observed_generation)
    };

    let conditions = vec![condition(
        outcome.state.as_str(),
        "True",
        &outcome.reason,
        &outcome.message,
    )];
    let body = json!({
        "status": {
            "conditions": conditions,
            "observedGeneration": observed_generation,
            "sessionId": session_id,
            "optimizationResult": optimization_result,
        }
    });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch_status(name, &params, &Patch::Merge(&body))
        .await?;
    Ok(())
}

/// Remove the `crabka.io/rebalance` annotation (JSON-merge null deletes
/// the key) so a one-shot command isn't re-applied on the next reconcile.
async fn remove_command_annotation(
    api: &Api<KafkaRebalance>,
    name: &str,
) -> Result<(), ReconcileError> {
    let patch = json!({ "metadata": { "annotations": { ANNOTATION: serde_json::Value::Null } } });
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.into()),
        ..Default::default()
    };
    api.patch(name, &params, &Patch::Merge(&patch)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{KafkaCondition, KafkaRebalanceSpec, KafkaRebalanceStatus};
    use crate::rebalancer_client::ProposalSummary;

    fn cr(name: &str) -> KafkaRebalance {
        let mut k = KafkaRebalance::new(name, KafkaRebalanceSpec::default());
        k.metadata.namespace = Some("kafka".into());
        k
    }

    fn proposal(id: &str, status: ProposalStatus) -> RebalancerProposal {
        RebalancerProposal {
            id: id.into(),
            status,
            summary: ProposalSummary {
                replica_movements: 3,
                leader_movements: 1,
                max_replicas_before: 9,
                max_replicas_after: 6,
                max_leaders_before: 5,
                max_leaders_after: 3,
            },
            goals_applied: vec!["RackAware".into()],
            movement_count: 3,
            failure_reason: None,
        }
    }

    // ----- decide() matrix --------------------------------------------

    #[test]
    fn decide_new_creates_proposal() {
        assert_eq!(
            decide(RebalanceState::New, None, false),
            RebalanceAction::CreateProposal
        );
    }

    #[test]
    fn decide_proposal_ready_idles_without_command() {
        assert_eq!(
            decide(RebalanceState::ProposalReady, None, true),
            RebalanceAction::Idle
        );
    }

    #[test]
    fn decide_approve_executes_when_ready_with_session() {
        assert_eq!(
            decide(
                RebalanceState::ProposalReady,
                Some(RebalanceCommand::Approve),
                true
            ),
            RebalanceAction::Execute
        );
    }

    #[test]
    fn decide_approve_ignored_without_session() {
        assert_eq!(
            decide(
                RebalanceState::ProposalReady,
                Some(RebalanceCommand::Approve),
                false
            ),
            RebalanceAction::Idle
        );
    }

    #[test]
    fn decide_approve_ignored_when_not_ready() {
        assert_eq!(
            decide(RebalanceState::Ready, Some(RebalanceCommand::Approve), true),
            RebalanceAction::Idle
        );
    }

    #[test]
    fn decide_refresh_recomputes_from_any_state() {
        for st in [
            RebalanceState::ProposalReady,
            RebalanceState::Ready,
            RebalanceState::NotReady,
            RebalanceState::Stopped,
            RebalanceState::Rebalancing,
        ] {
            assert_eq!(
                decide(st, Some(RebalanceCommand::Refresh), true),
                RebalanceAction::CreateProposal,
                "refresh from {st:?}"
            );
        }
    }

    #[test]
    fn decide_stop_cancels_only_while_rebalancing() {
        assert_eq!(
            decide(
                RebalanceState::Rebalancing,
                Some(RebalanceCommand::Stop),
                true
            ),
            RebalanceAction::Cancel
        );
        assert_eq!(
            decide(
                RebalanceState::ProposalReady,
                Some(RebalanceCommand::Stop),
                true
            ),
            RebalanceAction::Idle
        );
    }

    #[test]
    fn decide_rebalancing_polls_when_session_present() {
        assert_eq!(
            decide(RebalanceState::Rebalancing, None, true),
            RebalanceAction::PollExecution
        );
    }

    #[test]
    fn decide_rebalancing_without_session_recomputes() {
        assert_eq!(
            decide(RebalanceState::Rebalancing, None, false),
            RebalanceAction::CreateProposal
        );
    }

    // ----- outcome mapping --------------------------------------------

    #[test]
    fn create_computed_becomes_proposal_ready() {
        let o = Outcome::from_create(&proposal("p1", ProposalStatus::Computed));
        assert_eq!(o.state, RebalanceState::ProposalReady);
        assert_eq!(o.new_session.as_deref(), Some("p1"));
        assert!(o.advance_generation);
        let opt = o.new_optimization.unwrap();
        assert_eq!(opt.replica_movements, 3);
        assert_eq!(opt.goals, vec!["RackAware"]);
    }

    #[test]
    fn poll_executing_stays_rebalancing_with_short_requeue() {
        let o = Outcome::from_execute_or_poll(&proposal("p", ProposalStatus::Executing));
        assert_eq!(o.state, RebalanceState::Rebalancing);
        assert_eq!(o.requeue, POLL_INTERVAL);
        assert!(o.new_session.is_none(), "poll must not rewrite session");
    }

    #[test]
    fn poll_completed_becomes_ready() {
        let o = Outcome::from_execute_or_poll(&proposal("p", ProposalStatus::Completed));
        assert_eq!(o.state, RebalanceState::Ready);
    }

    #[test]
    fn poll_failed_becomes_not_ready_with_reason() {
        let mut p = proposal("p", ProposalStatus::Failed);
        p.failure_reason = Some("broker 2 down".into());
        let o = Outcome::from_execute_or_poll(&p);
        assert_eq!(o.state, RebalanceState::NotReady);
        assert_eq!(o.message, "broker 2 down");
    }

    #[test]
    fn cancel_becomes_stopped() {
        let o = Outcome::from_cancel(&proposal("p", ProposalStatus::Cancelled));
        assert_eq!(o.state, RebalanceState::Stopped);
    }

    // ----- current_state ----------------------------------------------

    #[test]
    fn current_state_defaults_to_new() {
        assert_eq!(current_state(&cr("x")), RebalanceState::New);
    }

    #[test]
    fn current_state_reads_active_condition() {
        let mut k = cr("x");
        k.status = Some(KafkaRebalanceStatus {
            conditions: vec![KafkaCondition {
                type_: "Rebalancing".into(),
                status: "True".into(),
                reason: "Rebalancing".into(),
                message: String::new(),
                last_transition_time: "2026-05-22T00:00:00Z".into(),
            }],
            ..Default::default()
        });
        assert_eq!(current_state(&k), RebalanceState::Rebalancing);
    }

    #[test]
    fn current_state_ignores_false_conditions() {
        let mut k = cr("x");
        k.status = Some(KafkaRebalanceStatus {
            conditions: vec![KafkaCondition {
                type_: "Ready".into(),
                status: "False".into(),
                reason: "x".into(),
                message: String::new(),
                last_transition_time: "2026-05-22T00:00:00Z".into(),
            }],
            ..Default::default()
        });
        assert_eq!(current_state(&k), RebalanceState::New);
    }

    // ----- read_command -----------------------------------------------

    #[test]
    fn read_command_parses_annotation() {
        let mut k = cr("x");
        k.metadata.annotations = Some(
            [("crabka.io/rebalance".to_string(), "approve".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(read_command(&k), Some(RebalanceCommand::Approve));
    }

    #[test]
    fn read_command_none_for_unknown_value() {
        let mut k = cr("x");
        k.metadata.annotations = Some(
            [("crabka.io/rebalance".to_string(), "yolo".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(read_command(&k), None);
    }

    // ----- resolve_endpoint -------------------------------------------

    #[test]
    fn resolve_endpoint_prefers_spec() {
        let mut k = cr("x");
        k.spec.endpoint = Some("http://explicit:9300".into());
        assert_eq!(
            resolve_endpoint(&k, "kafka").as_deref(),
            Some("http://explicit:9300")
        );
    }

    #[test]
    fn resolve_endpoint_derives_from_cluster_label() {
        let mut k = cr("x");
        k.metadata.labels = Some(
            [("crabka.io/cluster".to_string(), "demo".to_string())]
                .into_iter()
                .collect(),
        );
        assert_eq!(
            resolve_endpoint(&k, "kafka").as_deref(),
            Some("http://demo-rebalancer.kafka.svc.cluster.local:9300")
        );
    }

    #[test]
    fn resolve_endpoint_none_without_spec_or_label() {
        assert_eq!(resolve_endpoint(&cr("x"), "kafka"), None);
    }
}
