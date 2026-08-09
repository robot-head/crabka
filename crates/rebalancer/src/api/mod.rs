//! Connect-RPC service wiring.
//!
//! `connectrpc-axum-build` 0.1.1 emits a *builder*, not a trait, at
//! `pb::rebalancer_connect::RebalancerServiceBuilder`. Each method on the
//! builder registers an axum handler under the canonical Connect route
//! `/crabka.rebalancer.v1.Rebalancer/<Method>`. This module feeds it the
//! freestanding async fns from `handlers`.
//!
//! An `Extension(Arc<AppState>)` layer on the built router carries the
//! per-server state. The generated builder's typed `S` parameter stays at the
//! default `()`, which avoids type-juggling around `with_state` and `FromRef`.
//! Handlers extract the state with `axum::Extension<Arc<AppState>>`.

pub mod handlers;

use std::sync::Arc;

use crate::{goals::Goal, pb::rebalancer_connect::RebalancerServiceBuilder};

/// Registry of `Goal` trait objects, keyed by name. It maps the
/// `CreateProposalRequest::goals` strings to concrete implementations.
pub struct GoalRegistry {
    /// The registry keeps insertion order as the canonical priority order,
    /// after the optimizer stable-sorts by `GoalPriority`.
    goals: Vec<Box<dyn Goal>>,
}

impl GoalRegistry {
    #[must_use]
    pub fn default_registry() -> Self {
        Self {
            goals: vec![
                // Hard goals (priority order matters for the optimizer's Hard-first ordering).
                Box::new(crate::goals::preferred_leader_idempotency::PreferredLeaderIdempotency),
                Box::new(crate::goals::rack_aware::RackAware),
                Box::new(crate::goals::replica_capacity::ReplicaCapacity),
                Box::new(crate::goals::disk_capacity::DiskCapacity),
                Box::new(crate::goals::network_in_capacity::NetworkInCapacity),
                Box::new(crate::goals::network_out_capacity::NetworkOutCapacity),
                Box::new(crate::goals::cpu_capacity::CpuCapacity),
                // Soft goals.
                Box::new(crate::goals::replica_distribution::ReplicaDistribution),
                Box::new(crate::goals::leader_distribution::LeaderDistribution),
                Box::new(crate::goals::topic_replica_distribution::TopicReplicaDistribution),
                Box::new(crate::goals::min_topic_leaders_per_broker::MinTopicLeadersPerBroker),
                Box::new(crate::goals::disk_usage::DiskUsage),
                Box::new(crate::goals::leader_bytes_in::LeaderBytesIn),
                Box::new(crate::goals::network_in_usage::NetworkInUsage),
                Box::new(crate::goals::network_out_usage::NetworkOutUsage),
                Box::new(crate::goals::cpu_usage::CpuUsage),
            ],
        }
    }

    /// Translate user-supplied goal name strings into `&dyn Goal` references.
    /// An empty `names` slice returns all registered goals in registration
    /// order.
    /// # Errors
    /// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
    pub fn select<'a>(&'a self, names: &[String]) -> Result<Vec<&'a dyn Goal>, GoalSelectError> {
        if names.is_empty() {
            return Ok(self.goals.iter().map(std::convert::AsRef::as_ref).collect());
        }
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            let g = self
                .goals
                .iter()
                .find(|g| g.name() == n)
                .ok_or_else(|| GoalSelectError::Unknown(n.clone()))?;
            out.push(g.as_ref());
        }
        Ok(out)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GoalSelectError {
    #[error("unknown goal `{0}`")]
    Unknown(String),
}

/// Build the axum `Router` exposing the Connect-RPC service.
///
/// The returned router carries the shared `AppState` as an `Extension` layer,
/// so each handler can extract it with `axum::Extension<Arc<AppState>>`.
pub fn router(state: Arc<handlers::AppState>) -> axum::Router {
    RebalancerServiceBuilder::<()>::new()
        .get_state(handlers::get_state)
        .create_proposal(handlers::create_proposal)
        .dry_run_proposal(handlers::dry_run_proposal)
        .get_proposal(handlers::get_proposal)
        .list_proposals(handlers::list_proposals)
        .execute_proposal(handlers::execute_proposal)
        .cancel_execution(handlers::cancel_execution)
        .get_anomalies(handlers::get_anomalies)
        // `build_connect()` applies the `ConnectLayer` (protocol detection + per-request
        // `ConnectContext`); plain `.build()` omits it, so every Connect response falls back
        // to `application/json` regardless of the request's content-type, which breaks proto
        // connect-go clients (`invalid content-type: "application/json"; expecting
        // "application/proto"`).
        .build_connect()
        .layer(axum::Extension(state))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn default_registry_has_sixteen_goals() {
        let r = GoalRegistry::default_registry();
        let all = r.select(&[]).unwrap();
        assert2::assert!(all.len() == 16);
    }

    #[test]
    fn default_registry_order_matches_spec() {
        let r = GoalRegistry::default_registry();
        let names: Vec<&str> = r.select(&[]).unwrap().iter().map(|g| g.name()).collect();
        assert2::assert!(
            names
                == vec![
                    // Hard goals (priority order matters for the optimizer's
                    // Hard-first ordering).
                    "PreferredLeaderIdempotency",
                    "RackAware",
                    "ReplicaCapacity",
                    "DiskCapacity",
                    "NetworkInCapacity",
                    "NetworkOutCapacity",
                    "CpuCapacity",
                    // Soft goals.
                    "ReplicaDistribution",
                    "LeaderDistribution",
                    "TopicReplicaDistribution",
                    "MinTopicLeadersPerBroker",
                    "DiskUsage",
                    "LeaderBytesIn",
                    "NetworkInUsage",
                    "NetworkOutUsage",
                    "CpuUsage",
                ]
        );
    }

    #[test]
    fn select_by_name() {
        let r = GoalRegistry::default_registry();
        let one = r.select(&["ReplicaDistribution".into()]).unwrap();
        assert2::assert!(
            one.iter().map(|goal| goal.name()).collect::<Vec<_>>() == vec!["ReplicaDistribution"]
        );
    }

    #[test]
    fn select_unknown_goal_errors() {
        let r = GoalRegistry::default_registry();
        // `Result::unwrap_err` requires `Debug` on the `Ok` variant which
        // `Vec<&dyn Goal>` does not have — match instead.
        match r.select(&["GhostGoal".into()]) {
            Err(GoalSelectError::Unknown(ref n)) if n == "GhostGoal" => {}
            Err(other) => panic!("unexpected error variant: {other:?}"),
            Ok(_) => panic!("expected GoalSelectError::Unknown, got Ok"),
        }
    }
}
