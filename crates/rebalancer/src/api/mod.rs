//! Connect-RPC service wiring.
//!
//! `connectrpc-axum-build` 0.1.1 emits a *builder* (not a trait) at
//! `pb::rebalancer_connect::RebalancerServiceBuilder`. Each method on
//! the builder registers an axum handler under the canonical Connect
//! route (`/crabka.rebalancer.v1.Rebalancer/<Method>`). We feed it the
//! six freestanding async fns from `handlers`.
//!
//! Per-server state is propagated through an `Extension(Arc<AppState>)`
//! layer attached to the built router; the generated builder's typed
//! `S` parameter is left at the default `()` to avoid type-juggling
//! around `with_state` and `FromRef`. Handlers extract the state with
//! `axum::Extension<Arc<AppState>>`.

pub mod handlers;

use std::sync::Arc;

use crate::goals::Goal;
use crate::pb::rebalancer_connect::RebalancerServiceBuilder;

/// Registry of `Goal` trait objects, name-keyed. Maps the
/// `CreateProposalRequest::goals` strings to concrete implementations.
pub struct GoalRegistry {
    /// Insertion order is preserved as the canonical priority order
    /// after stable-sorting by `GoalPriority` inside the optimizer.
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
                // Soft goals.
                Box::new(crate::goals::replica_distribution::ReplicaDistribution),
                Box::new(crate::goals::leader_distribution::LeaderDistribution),
                Box::new(crate::goals::topic_replica_distribution::TopicReplicaDistribution),
                Box::new(crate::goals::min_topic_leaders_per_broker::MinTopicLeadersPerBroker),
            ],
        }
    }

    /// Translate user-supplied goal name strings into `&dyn Goal`
    /// references. An empty `names` slice returns all registered goals
    /// in registration order.
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
/// The returned router has the shared `AppState` attached as an
/// `Extension` layer so each handler can extract it with
/// `axum::Extension<Arc<AppState>>`.
pub fn router(state: Arc<handlers::AppState>) -> axum::Router {
    RebalancerServiceBuilder::<()>::new()
        .get_state(handlers::get_state)
        .create_proposal(handlers::create_proposal)
        .dry_run_proposal(handlers::dry_run_proposal)
        .get_proposal(handlers::get_proposal)
        .list_proposals(handlers::list_proposals)
        .execute_proposal(handlers::execute_proposal)
        .cancel_execution(handlers::cancel_execution)
        .build()
        .layer(axum::Extension(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_has_six_goals() {
        let r = GoalRegistry::default_registry();
        let all = r.select(&[]).unwrap();
        assert_eq!(all.len(), 6);
    }

    #[test]
    fn select_by_name() {
        let r = GoalRegistry::default_registry();
        let one = r.select(&["ReplicaDistribution".into()]).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].name(), "ReplicaDistribution");
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
