//! `KafkaRebalance` reconciler.
//!
//! This reconciler drives the standalone `crabka-rebalancer` service through
//! its Connect-RPC API. It reflects the proposal lifecycle into the CRD
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
//! The optimizer call `CreateProposal` never mutates the cluster. Only
//! `approve`, which issues `ExecuteProposal`, mutates the cluster. This keeps
//! a human or a `GitOps` approval in the loop before any partition data
//! moves.

use std::sync::Arc;

use crabka_units::Time;
use futures::StreamExt as _;
use kube::{
    Resource, ResourceExt as _,
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Controller},
        watcher,
    },
};
use serde_json::json;

use crate::{
    context::Context,
    controller::common::{self, FIELD_MANAGER, ReconcileError, condition},
    crd::{KafkaRebalance, OptimizationResult},
    rebalancer_client::{ProposalStatus, RebalancerError, RebalancerProposal},
};

/// Annotation that drives the rebalance state machine. Mirrors Strimzi's
/// `strimzi.io/rebalance`.
const ANNOTATION: &str = "crabka.io/rebalance";
/// Default Connect-RPC port that the rebalancer binds with `--listen-addr`.
const REBALANCER_PORT: u16 = 9300;

/// The rebalance lifecycle state. It appears as the `type` of the active
/// condition in the CRD status.
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

/// The RPC that the reconcile should issue this pass. It is the pure output
/// of [`decide`].
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
    /// Nothing to do this pass: await approval, terminal state, or a no-op
    /// command.
    Idle,
}

/// Pure state-machine core. It takes the current state, an optional pending
/// command, and a flag for a proposal id on file, and decides which RPC to
/// issue. The unit tests below cover it fully, and the reconcile function only
/// does I/O.
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
#[derive(Debug, PartialEq)]
struct Outcome {
    state: RebalanceState,
    reason: String,
    message: String,
    requeue: Time,
    /// Set when the operator should record a fresh proposal id for
    /// `CreateProposal`.
    new_session: Option<String>,
    /// Set when a fresh optimization result should be recorded.
    new_optimization: Option<OptimizationResult>,
    /// Advance `observedGeneration` to the current generation. This happens
    /// only when the operator computes a new proposal against the current
    /// spec.
    advance_generation: bool,
}

impl Outcome {
    fn from_create(p: &RebalancerProposal, idle_interval: Time) -> Self {
        if p.status == ProposalStatus::Computed {
            Self {
                state: RebalanceState::ProposalReady,
                reason: "ProposalReady".into(),
                message: format!(
                    "proposal {} computed: {} replica / {} leader movements",
                    p.id, p.summary.replica_movements, p.summary.leader_movements
                ),
                requeue: idle_interval,
                new_session: Some(p.id.clone()),
                new_optimization: Some(optimization_result_from(p)),
                advance_generation: true,
            }
        } else {
            Self {
                state: RebalanceState::NotReady,
                reason: "UnexpectedProposalStatus".into(),
                message: format!("CreateProposal returned non-Computed status for {}", p.id),
                requeue: idle_interval,
                new_session: Some(p.id.clone()),
                new_optimization: None,
                advance_generation: false,
            }
        }
    }

    /// Map an `ExecuteProposal` or `GetProposal` poll result onto a state.
    fn from_execute_or_poll(
        p: &RebalancerProposal,
        poll_interval: Time,
        idle_interval: Time,
    ) -> Self {
        match p.status {
            ProposalStatus::Executing | ProposalStatus::Computed => Self::transient(
                RebalanceState::Rebalancing,
                "Rebalancing",
                format!("executing proposal {}", p.id),
                poll_interval,
            ),
            ProposalStatus::Completed => Self::transient(
                RebalanceState::Ready,
                "Ready",
                format!("proposal {} completed", p.id),
                idle_interval,
            ),
            ProposalStatus::Failed => Self::transient(
                RebalanceState::NotReady,
                "RebalanceFailed",
                p.failure_reason
                    .clone()
                    .unwrap_or_else(|| format!("proposal {} failed", p.id)),
                idle_interval,
            ),
            ProposalStatus::Cancelled => Self::transient(
                RebalanceState::Stopped,
                "Stopped",
                format!("proposal {} cancelled", p.id),
                idle_interval,
            ),
            ProposalStatus::Unspecified => Self::transient(
                RebalanceState::NotReady,
                "UnexpectedProposalStatus",
                format!("proposal {} reported an unknown status", p.id),
                idle_interval,
            ),
        }
    }

    fn from_cancel(p: &RebalancerProposal, idle_interval: Time) -> Self {
        Self::transient(
            RebalanceState::Stopped,
            "Stopped",
            format!("execution of proposal {} cancelled", p.id),
            idle_interval,
        )
    }

    /// An RPC-level error from the rebalancer, such as `failed_precondition`
    /// or `not_found`. It appears as `NotReady`.
    fn from_rpc_error(e: &RebalancerError, idle_interval: Time) -> Self {
        Self::transient(
            RebalanceState::NotReady,
            "RebalancerError",
            e.to_string(),
            idle_interval,
        )
    }

    /// A status with no proposal-id changes and no optimization changes.
    fn transient(state: RebalanceState, reason: &str, message: String, requeue: Time) -> Self {
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

/// Current state derived from the active condition, which has
/// `status: "True"`. Defaults to `New` when there is no recognized condition
/// yet.
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

/// Cluster-internal DNS suffixes that a rebalancer endpoint host can end
/// with. The operator rejects any other suffix. This prevents a user-supplied
/// `spec.endpoint` from pointing the operator at arbitrary in-cluster
/// addresses such as the K8s API, cloud metadata, and internal admin
/// endpoints. Such a value is a blind SSRF that uses the operator's network
/// position. See finding L-5.
const CLUSTER_INTERNAL_SUFFIXES: [&str; 3] = [".svc", ".svc.cluster.local", ".cluster.local"];

/// Reason why the operator rejected a user-supplied `spec.endpoint`. It
/// appears in the CR status as a terminal `NotReady` condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidEndpoint {
    pub message: String,
}

/// Validate a user-supplied `spec.endpoint` before the operator uses it to
/// build a request URL. The operator issues authenticated-by-network POSTs to
/// this base URL, so an unrestricted value is a server-side request forgery
/// (SSRF) vector. This function requires:
///
/// - a scheme of `http` or `https`;
/// - a hostname, not an IP literal, that ends in a cluster-internal DNS
///   suffix: `.svc`, `.svc.cluster.local`, or `.cluster.local`.
///
/// `reqwest` and `url` are not dependencies here, so this function parses by
/// hand and stays conservative. It splits off the scheme, removes any
/// userinfo, and then isolates the host from the `host[:port]` authority. It
/// also handles bracketed IPv6 literals.
fn validate_endpoint(endpoint: &str) -> Result<(), InvalidEndpoint> {
    let reject = |msg: String| Err(InvalidEndpoint { message: msg });

    // Scheme.
    let Some((scheme, rest)) = endpoint.split_once("://") else {
        return reject(format!(
            "spec.endpoint {endpoint:?} is not an absolute http(s) URL"
        ));
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return reject(format!(
            "spec.endpoint scheme {scheme:?} is not allowed; only http/https are permitted"
        ));
    }

    // Authority = everything up to the first '/', '?' or '#'.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    // Drop any userinfo ("user:pass@host").
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);

    // Isolate the host from "host[:port]". Bracketed IPv6 ("[::1]:9300") is
    // an IP literal and therefore rejected outright below.
    let host = if let Some(stripped) = host_port.strip_prefix('[') {
        // [ipv6]:port — anything bracketed is an IPv6 literal.
        let inner = stripped.split(']').next().unwrap_or(stripped);
        return reject(format!(
            "spec.endpoint host {inner:?} is an IP literal; only cluster-internal DNS names are allowed"
        ));
    } else {
        // host:port — split on the last ':' only if what follows is a port.
        match host_port.rsplit_once(':') {
            Some((h, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => h,
            _ => host_port,
        }
    };

    if host.is_empty() {
        return reject(format!("spec.endpoint {endpoint:?} has no host"));
    }

    // Reject bare IPv4 literals (e.g. 169.254.169.254, 127.0.0.1, 10.x).
    if host
        .split('.')
        .all(|seg| !seg.is_empty() && seg.bytes().all(|b| b.is_ascii_digit()))
    {
        return reject(format!(
            "spec.endpoint host {host:?} is an IP literal; only cluster-internal DNS names are allowed"
        ));
    }

    let host_lc = host.to_ascii_lowercase();
    if CLUSTER_INTERNAL_SUFFIXES
        .iter()
        .any(|suffix| host_lc.ends_with(suffix))
    {
        Ok(())
    } else {
        reject(format!(
            "spec.endpoint host {host:?} is not cluster-internal; it must end in one of {CLUSTER_INTERNAL_SUFFIXES:?}"
        ))
    }
}

/// Resolve the rebalancer Connect base URL. `spec.endpoint` wins after SSRF
/// validation. If it is absent, this function derives
/// `http://<cluster>-rebalancer.<ns>.svc.cluster.local:9300` from the
/// `crabka.io/cluster` label.
///
/// Returns `Ok(None)` when there is no valid `spec.endpoint` and no cluster
/// label. Returns `Err(InvalidEndpoint)` when a user-supplied `spec.endpoint`
/// fails validation. The operator always trusts the derived default endpoint
/// and never validates it.
///
/// # Errors
///
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub fn resolve_endpoint(
    obj: &KafkaRebalance,
    namespace: &str,
) -> Result<Option<String>, InvalidEndpoint> {
    if let Some(ep) = obj.spec.endpoint.as_ref().filter(|s| !s.is_empty()) {
        validate_endpoint(ep)?;
        return Ok(Some(ep.clone()));
    }
    let Some(cluster) = obj
        .meta()
        .labels
        .as_ref()
        .and_then(|l| l.get("crabka.io/cluster"))
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    Ok(Some(format!(
        "http://{cluster}-rebalancer.{namespace}.svc.cluster.local:{REBALANCER_PORT}"
    )))
}

/// Run the controller forever.
///
/// # Errors
///
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
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

pub fn error_policy(_obj: Arc<KafkaRebalance>, err: &ReconcileError, ctx: Arc<Context>) -> Action {
    tracing::warn!(error = %err, "rebalance reconcile error, requeueing");
    common::error_requeue(ctx)
}

/// Reconcile entry point. This function times the pass, records the reconcile
/// counter and histogram, and then delegates to the internal `reconcile_inner`
/// operation.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        kind = "KafkaRebalance",
        namespace = %obj.namespace().unwrap_or_else(|| "default".into()),
        name = %obj.name_any(),
        generation = ?obj.meta().generation,
    )
)]
/// # Errors
///
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn reconcile(
    obj: Arc<KafkaRebalance>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    common::record_reconcile(
        &ctx,
        "KafkaRebalance",
        Box::pin(reconcile_inner(obj, ctx.clone())),
    )
    .await
}

async fn reconcile_inner(
    obj: Arc<KafkaRebalance>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let ns = obj.namespace().unwrap_or_else(|| "default".into());
    let name = obj.name_any();
    let api: Api<KafkaRebalance> = Api::namespaced(ctx.client.clone(), &ns);

    // 1. Resolve (and validate) the rebalancer endpoint.
    let endpoint = match resolve_endpoint(&obj, &ns) {
        Ok(Some(endpoint)) => endpoint,
        Ok(None) => {
            write_status(
                &api,
                &name,
                &obj,
                &Outcome::transient(
                    RebalanceState::NotReady,
                    "MissingEndpoint",
                    "spec.endpoint is unset and no crabka.io/cluster label is present to derive it"
                        .into(),
                    ctx.config.rebalancer_idle_interval,
                ),
            )
            .await?;
            return Ok(common::requeue(ctx.config.rebalancer_idle_interval));
        }
        Err(invalid) => {
            tracing::warn!(error = %invalid.message, "rejecting spec.endpoint (SSRF guard)");
            write_status(
                &api,
                &name,
                &obj,
                &Outcome::transient(
                    RebalanceState::NotReady,
                    "InvalidEndpoint",
                    invalid.message,
                    ctx.config.rebalancer_idle_interval,
                ),
            )
            .await?;
            return Ok(common::requeue(ctx.config.rebalancer_idle_interval));
        }
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
        return Ok(common::requeue(ctx.config.rebalancer_idle_interval));
    }

    // 4. Issue the RPC.
    let client = ctx.rebalancer_client_for(&endpoint).await;
    let rpc_result = match action {
        RebalanceAction::CreateProposal => {
            let goals = obj.spec.goals.clone().unwrap_or_default();
            client
                .create_proposal(&goals)
                .await
                .map(|p| Outcome::from_create(&p, ctx.config.rebalancer_idle_interval))
        }
        RebalanceAction::Execute => {
            let id = session.clone().unwrap_or_default();
            client
                .execute_proposal(&id, obj.spec.throttle_bytes_per_sec)
                .await
                .map(|p| {
                    Outcome::from_execute_or_poll(
                        &p,
                        ctx.config.rebalancer_poll_interval,
                        ctx.config.rebalancer_idle_interval,
                    )
                })
        }
        RebalanceAction::PollExecution => {
            let id = session.clone().unwrap_or_default();
            client.get_proposal(&id).await.map(|p| {
                Outcome::from_execute_or_poll(
                    &p,
                    ctx.config.rebalancer_poll_interval,
                    ctx.config.rebalancer_idle_interval,
                )
            })
        }
        RebalanceAction::Cancel => {
            let id = session.clone().unwrap_or_default();
            client
                .cancel_execution(&id)
                .await
                .map(|p| Outcome::from_cancel(&p, ctx.config.rebalancer_idle_interval))
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
            return Ok(common::requeue(ctx.config.controller_error_requeue));
        }
        Err(e) => Outcome::from_rpc_error(&e, ctx.config.rebalancer_idle_interval),
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
    Ok(common::requeue(requeue))
}

/// Merge-patch the status. This function carries forward `sessionId`,
/// `optimizationResult`, and `observedGeneration` when the outcome sets no new
/// values, so a poll pass never wipes the computed result.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(rebalance = %name, state = %outcome.state.as_str(), reason = %outcome.reason),
    err,
)]
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

/// Remove the `crabka.io/rebalance` annotation. A JSON-merge null deletes the
/// key, so the operator does not apply a one-shot command again on the next
/// reconcile.
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
    use assert2::assert;
    use crabka_units::{minutes, secs};

    use super::*;
    use crate::{
        crd::{KafkaCondition, KafkaRebalanceSpec, KafkaRebalanceStatus},
        ids::{LeaderMovementCount, MaxLeadersCount, MaxReplicasCount, ReplicaMovementCount},
        rebalancer_client::ProposalSummary,
    };

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
                replica_movements: ReplicaMovementCount(3),
                leader_movements: LeaderMovementCount(1),
                max_replicas_before: MaxReplicasCount(9),
                max_replicas_after: MaxReplicasCount(6),
                max_leaders_before: MaxLeadersCount(5),
                max_leaders_after: MaxLeadersCount(3),
            },
            goals_applied: vec!["RackAware".into()],
            movement_count: 3,
            failure_reason: None,
        }
    }

    // ----- decide() matrix --------------------------------------------

    #[test]
    fn decide_new_creates_proposal() {
        assert!(decide(RebalanceState::New, None, false) == RebalanceAction::CreateProposal);
    }

    #[test]
    fn decide_proposal_ready_idles_without_command() {
        assert!(decide(RebalanceState::ProposalReady, None, true) == RebalanceAction::Idle);
    }

    #[test]
    fn decide_approve_executes_when_ready_with_session() {
        assert!(
            decide(
                RebalanceState::ProposalReady,
                Some(RebalanceCommand::Approve),
                true
            ) == RebalanceAction::Execute
        );
    }

    #[test]
    fn decide_approve_ignored_without_session() {
        assert!(
            decide(
                RebalanceState::ProposalReady,
                Some(RebalanceCommand::Approve),
                false
            ) == RebalanceAction::Idle
        );
    }

    #[test]
    fn decide_approve_ignored_when_not_ready() {
        assert!(
            decide(RebalanceState::Ready, Some(RebalanceCommand::Approve), true)
                == RebalanceAction::Idle
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
            assert!(
                decide(st, Some(RebalanceCommand::Refresh), true)
                    == RebalanceAction::CreateProposal,
                "refresh from {st:?}"
            );
        }
    }

    #[test]
    fn decide_stop_cancels_only_while_rebalancing() {
        assert!(
            decide(
                RebalanceState::Rebalancing,
                Some(RebalanceCommand::Stop),
                true
            ) == RebalanceAction::Cancel
        );
        assert!(
            decide(
                RebalanceState::ProposalReady,
                Some(RebalanceCommand::Stop),
                true
            ) == RebalanceAction::Idle
        );
    }

    #[test]
    fn decide_rebalancing_polls_when_session_present() {
        assert!(decide(RebalanceState::Rebalancing, None, true) == RebalanceAction::PollExecution);
    }

    #[test]
    fn decide_rebalancing_without_session_recomputes() {
        assert!(
            decide(RebalanceState::Rebalancing, None, false) == RebalanceAction::CreateProposal
        );
    }

    // ----- outcome mapping --------------------------------------------

    #[test]
    fn create_computed_becomes_proposal_ready() {
        let o = Outcome::from_create(&proposal("p1", ProposalStatus::Computed), minutes(5));
        assert!(
            o == Outcome {
                state: RebalanceState::ProposalReady,
                reason: "ProposalReady".into(),
                message: "proposal p1 computed: 3 replica / 1 leader movements".into(),
                requeue: minutes(5),
                new_session: Some("p1".into()),
                new_optimization: Some(OptimizationResult {
                    replica_movements: ReplicaMovementCount(3),
                    leader_movements: LeaderMovementCount(1),
                    max_replicas_before: MaxReplicasCount(9),
                    max_replicas_after: MaxReplicasCount(6),
                    max_leaders_before: MaxLeadersCount(5),
                    max_leaders_after: MaxLeadersCount(3),
                    goals: vec!["RackAware".into()],
                }),
                advance_generation: true,
            }
        );
    }

    #[test]
    fn poll_executing_stays_rebalancing_with_short_requeue() {
        let o = Outcome::from_execute_or_poll(
            &proposal("p", ProposalStatus::Executing),
            secs(10),
            minutes(5),
        );
        // `new_session: None` — poll must not rewrite the session.
        assert!(
            o == Outcome {
                state: RebalanceState::Rebalancing,
                reason: "Rebalancing".into(),
                message: "executing proposal p".into(),
                requeue: secs(10),
                new_session: None,
                new_optimization: None,
                advance_generation: false,
            }
        );
    }

    #[test]
    fn poll_completed_becomes_ready() {
        let o = Outcome::from_execute_or_poll(
            &proposal("p", ProposalStatus::Completed),
            secs(10),
            minutes(5),
        );
        assert!(o.state == RebalanceState::Ready);
    }

    #[test]
    fn poll_failed_becomes_not_ready_with_reason() {
        let mut p = proposal("p", ProposalStatus::Failed);
        p.failure_reason = Some("broker 2 down".into());
        let o = Outcome::from_execute_or_poll(&p, secs(10), minutes(5));
        assert!(o.state == RebalanceState::NotReady);
        assert!(o.message == "broker 2 down");
    }

    #[test]
    fn cancel_becomes_stopped() {
        let o = Outcome::from_cancel(&proposal("p", ProposalStatus::Cancelled), minutes(5));
        assert!(o.state == RebalanceState::Stopped);
    }

    // ----- current_state ----------------------------------------------

    #[test]
    fn current_state_defaults_to_new() {
        assert!(current_state(&cr("x")) == RebalanceState::New);
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
        assert!(current_state(&k) == RebalanceState::Rebalancing);
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
        assert!(current_state(&k) == RebalanceState::New);
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
        assert!(read_command(&k) == Some(RebalanceCommand::Approve));
    }

    #[test]
    fn read_command_none_for_unknown_value() {
        let mut k = cr("x");
        k.metadata.annotations = Some(
            [("crabka.io/rebalance".to_string(), "yolo".to_string())]
                .into_iter()
                .collect(),
        );
        assert!(read_command(&k) == None);
    }

    // ----- resolve_endpoint -------------------------------------------

    #[test]
    fn resolve_endpoint_prefers_valid_spec() {
        let mut k = cr("x");
        k.spec.endpoint = Some("http://other-rebalancer.kafka.svc.cluster.local:9300".into());
        assert!(
            resolve_endpoint(&k, "kafka").unwrap().as_deref()
                == Some("http://other-rebalancer.kafka.svc.cluster.local:9300")
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
        assert!(
            resolve_endpoint(&k, "kafka").unwrap().as_deref()
                == Some("http://demo-rebalancer.kafka.svc.cluster.local:9300")
        );
    }

    #[test]
    fn resolve_endpoint_none_without_spec_or_label() {
        assert!(resolve_endpoint(&cr("x"), "kafka").unwrap() == None);
    }

    // ----- spec.endpoint SSRF validation (finding L-5) -----------------

    fn resolve_with_endpoint(ep: &str) -> Result<Option<String>, InvalidEndpoint> {
        let mut k = cr("x");
        k.spec.endpoint = Some(ep.into());
        resolve_endpoint(&k, "kafka")
    }

    #[test]
    fn endpoint_accepts_cluster_internal_hosts() {
        for ep in [
            "http://demo-rebalancer.kafka.svc.cluster.local:9300",
            "https://demo-rebalancer.kafka.svc.cluster.local:9300",
            "http://demo-rebalancer.kafka.svc:9300",
            "http://demo-rebalancer.kafka.svc.cluster.local",
            "HTTP://Demo-Rebalancer.Kafka.SVC.Cluster.Local:9300",
        ] {
            assert!(validate_endpoint(ep).is_ok(), "should accept {ep:?}");
        }
    }

    #[test]
    fn endpoint_rejects_cloud_metadata_ip() {
        let err = resolve_with_endpoint("http://169.254.169.254/latest/meta-data/").unwrap_err();
        assert!(err.message.contains("IP literal"), "{}", err.message);
    }

    #[test]
    fn endpoint_rejects_loopback_ip() {
        let err = resolve_with_endpoint("http://127.0.0.1:9300").unwrap_err();
        assert!(err.message.contains("IP literal"), "{}", err.message);
    }

    #[test]
    fn endpoint_rejects_ipv6_literal() {
        let err = resolve_with_endpoint("http://[::1]:9300").unwrap_err();
        assert!(err.message.contains("IP literal"), "{}", err.message);
    }

    #[test]
    fn endpoint_rejects_external_host() {
        let err = resolve_with_endpoint("http://attacker.example.com/").unwrap_err();
        assert!(err.message.contains("cluster-internal"), "{}", err.message);
    }

    #[test]
    fn endpoint_rejects_bare_internal_name() {
        // A short name with no cluster suffix (e.g. the K8s API "kubernetes")
        // must not slip through.
        let err = resolve_with_endpoint("http://kubernetes:443").unwrap_err();
        assert!(err.message.contains("cluster-internal"), "{}", err.message);
    }

    #[test]
    fn endpoint_rejects_disallowed_scheme() {
        let err = resolve_with_endpoint("file:///etc/passwd").unwrap_err();
        assert!(err.message.contains("scheme") || err.message.contains("http(s)"));
    }

    #[test]
    fn endpoint_rejects_userinfo_smuggling() {
        // "host" in userinfo must not be mistaken for the real host.
        let err =
            resolve_with_endpoint("http://demo.svc.cluster.local@169.254.169.254/").unwrap_err();
        assert!(err.message.contains("IP literal"), "{}", err.message);
    }
}
