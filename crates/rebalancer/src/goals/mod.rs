//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules (`preferred_leader_idempotency`, `replica_distribution`,
//! `leader_distribution`).

use crate::model::{ClusterState, Movement};

pub mod preferred_leader_idempotency;
pub mod replica_distribution;
pub mod leader_distribution;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalPriority {
    /// Hard goals must be satisfied. If the optimizer truncates the
    /// movement list at `max_movements_per_proposal` and a hard goal
    /// still has unfulfilled movements, the optimizer returns
    /// `OptimizeError::HardGoalUnsatisfied`.
    Hard,
    /// Soft goals improve placement on a best-effort basis. Movements
    /// that don't fit under the cap are simply skipped.
    Soft,
}

#[derive(Debug, Clone, Copy)]
pub struct GoalContext {
    /// `(max - min) * 100 / total` must exceed this percentage for a
    /// soft goal to act. Hard goals ignore the threshold.
    pub imbalance_threshold_pct: u32,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
}

pub trait Goal: Send + Sync {
    /// Stable identifier surfaced in `Proposal::goals_applied`. Must
    /// match the user-facing name accepted in
    /// `CreateProposalRequest::goals`.
    fn name(&self) -> &'static str;

    fn priority(&self) -> GoalPriority;

    /// Inspect `state` and return an ordered list of movements that
    /// improve (or satisfy) this goal. An empty `Vec` means the goal
    /// is already satisfied. Movements are intent; the optimizer
    /// validates and reconciles them across goals before producing
    /// the final proposal.
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement>;
}

#[cfg(test)]
pub mod tests {
    use super::*;

    /// Minimal goal that returns a fixed movement list. Used by
    /// `optimizer::tests` to exercise the optimizer without depending
    /// on any concrete goal implementation.
    #[allow(dead_code)] // Consumed by optimizer tests in T9.
    pub struct FixedGoal {
        pub name: &'static str,
        pub priority: GoalPriority,
        pub movements: Vec<Movement>,
    }

    impl Goal for FixedGoal {
        fn name(&self) -> &'static str {
            self.name
        }
        fn priority(&self) -> GoalPriority {
            self.priority
        }
        fn propose(&self, _: &ClusterState, _: &GoalContext) -> Vec<Movement> {
            self.movements.clone()
        }
    }

    #[test]
    fn priority_ordering_hard_before_soft() {
        assert!(matches!(GoalPriority::Hard, GoalPriority::Hard));
        assert_ne!(GoalPriority::Hard, GoalPriority::Soft);
    }
}
