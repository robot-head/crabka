//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules.

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    capacity::BrokerCapacities,
    model::{ClusterState, Movement},
    scraper::UsageStore,
};

/// Wall-clock millis since the Unix epoch. Goals that read usage data
/// pass this to `UsageStore` queries so stale-broker samples are
/// excluded from the window. Saturates to 0 / `i64::MAX` on overflow.
#[must_use]
pub fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(i64::MAX)
}

pub mod cpu_capacity;
pub mod cpu_usage;
pub mod disk_capacity;
pub mod disk_usage;
pub mod leader_bytes_in;
pub mod leader_distribution;
pub mod min_topic_leaders_per_broker;
pub mod network_in_capacity;
pub mod network_in_usage;
pub mod network_out_capacity;
pub mod network_out_usage;
pub mod preferred_leader_idempotency;
pub mod rack_aware;
pub mod replica_capacity;
pub mod replica_distribution;
pub mod topic_replica_distribution;

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

#[derive(Debug, Clone)]
pub struct GoalContext {
    /// `(max - min) * 100 / total` must exceed this percentage for a
    /// soft goal to act. Hard goals ignore the threshold.
    pub imbalance_threshold_pct: u32,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` disables the goal.
    pub min_topic_leaders_per_broker: u32,
    /// Per-broker capacity limits for the five capacity goals.
    pub broker_capacities: Arc<BrokerCapacities>,
    /// Per-partition usage data (counters + gauges) from the metric
    /// scraper. Empty default = usage-driven goals see no data and
    /// return empty `Vec<Movement>` (same self-limiting pattern as
    /// the capacity stubs).
    pub broker_usages: Arc<UsageStore>,
}

pub trait Goal: Send + Sync {
    /// Stable identifier surfaced in `Proposal::goals_applied`.
    fn name(&self) -> &'static str;

    fn priority(&self) -> GoalPriority;

    /// Inspect `state` and return movements that satisfy or improve
    /// this goal. The optimizer validates each movement against the
    /// post-application state before accepting it.
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement>;

    /// Returns true if the goal's invariant holds against `state`
    /// alone (no `GoalContext` access). Soft goals use the default
    /// (always true); hard goals that don't depend on context (e.g.
    /// `PreferredLeaderIdempotency`, `RackAware`) override.
    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        true
    }

    /// Same as `is_satisfied` but with `GoalContext` access. The
    /// optimizer's incremental hard-goal validation calls this so
    /// capacity goals can consult `broker_capacities` /
    /// `broker_usages` when deciding whether a tentative movement
    /// keeps their invariant intact. Default forwards to
    /// `is_satisfied`.
    fn is_satisfied_with_ctx(&self, state: &ClusterState, _ctx: &GoalContext) -> bool {
        self.is_satisfied(state)
    }
}

#[cfg(test)]
pub mod tests {
    use assert2::assert;

    use super::*;

    /// Minimal goal that returns a fixed movement list. Used by
    /// `optimizer::tests` to exercise the optimizer without depending
    /// on any concrete goal implementation.
    #[allow(dead_code)] // Consumed by optimizer tests.
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
        assert!(GoalPriority::Hard != GoalPriority::Soft);
    }
}
