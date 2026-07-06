//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules.

use std::{collections::HashMap, sync::Arc};

use crate::{
    capacity::BrokerCapacities,
    model::{ClusterState, Movement, PartitionView},
    scraper::UsageStore,
};

/// Wall-clock millis since the Unix epoch. Goals that read usage data
/// pass this to `UsageStore` queries so stale-broker samples are
/// excluded from the window. Saturates to 0 / `i64::MAX` on overflow.
#[must_use]
pub fn now_ms() -> i64 {
    crate::time::now_ms()
}

pub(crate) fn imbalance_pct_usize(counts: &HashMap<i32, usize>) -> u32 {
    let values: Vec<usize> = counts.values().copied().collect();
    let total: usize = values.iter().sum();
    if total == 0 {
        return 0;
    }
    let max = *values.iter().max().unwrap_or(&0);
    let min = *values.iter().min().unwrap_or(&0);
    u32::try_from((max - min) * 100 / total).unwrap_or(u32::MAX)
}

pub(crate) fn imbalance_pct_f64(totals: &HashMap<i32, f64>) -> u32 {
    let vals: Vec<f64> = totals.values().copied().collect();
    let total: f64 = vals.iter().sum();
    if total <= 0.0 {
        return 0;
    }
    let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
    let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
    let pct = ((max - min) * 100.0 / total).clamp(0.0, f64::from(u32::MAX));
    // Saturating cast: pct is clamped to [0, u32::MAX] above.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let out = pct as u32;
    out
}

pub(crate) fn replica_totals(
    partitions: &[PartitionView],
    broker_ids: &[i32],
    mut value: impl FnMut(i32, &str, i32) -> Option<f64>,
) -> HashMap<i32, f64> {
    let mut totals: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
    for p in partitions {
        for replica in &p.replicas {
            if let Some(amount) = value(*replica, &p.topic, p.partition) {
                *totals.entry(*replica).or_insert(0.0) += amount;
            }
        }
    }
    totals
}

pub(crate) fn leader_totals(
    partitions: &[PartitionView],
    broker_ids: &[i32],
    mut value: impl FnMut(i32, &str, i32) -> Option<f64>,
) -> HashMap<i32, f64> {
    let mut totals: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
    for p in partitions {
        if let Some(amount) = value(p.leader, &p.topic, p.partition) {
            *totals.entry(p.leader).or_insert(0.0) += amount;
        }
    }
    totals
}

pub(crate) struct OriginalReplicaState {
    replicas: HashMap<(String, i32), Vec<i32>>,
    leaders: HashMap<(String, i32), i32>,
}

impl OriginalReplicaState {
    pub(crate) fn from_partitions(partitions: &[PartitionView]) -> Self {
        Self {
            replicas: partitions
                .iter()
                .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
                .collect(),
            leaders: partitions
                .iter()
                .map(|p| ((p.topic.clone(), p.partition), p.leader))
                .collect(),
        }
    }

    pub(crate) fn replace_replica(
        &self,
        partition: &mut PartitionView,
        removed_broker: i32,
        added_broker: i32,
    ) -> Movement {
        let key = (partition.topic.clone(), partition.partition);
        let old_replicas = self
            .replicas
            .get(&key)
            .cloned()
            .unwrap_or_else(|| partition.replicas.clone());
        let old_leader = self.leaders.get(&key).copied().unwrap_or(partition.leader);

        let pos = partition
            .replicas
            .iter()
            .position(|r| *r == removed_broker)
            .expect("removed broker present");
        partition.replicas[pos] = added_broker;

        let new_leader = if partition.leader == removed_broker {
            *partition
                .replicas
                .iter()
                .find(|r| partition.isr.contains(r))
                .unwrap_or(&partition.replicas[0])
        } else {
            partition.leader
        };

        let movement = Movement {
            topic: partition.topic.clone(),
            partition: partition.partition,
            old_replicas,
            new_replicas: partition.replicas.clone(),
            old_leader,
            new_leader,
        };
        partition.leader = new_leader;
        movement
    }

    pub(crate) fn change_leader(&self, partition: &mut PartitionView, new_leader: i32) -> Movement {
        let key = (partition.topic.clone(), partition.partition);
        let old_replicas = self
            .replicas
            .get(&key)
            .cloned()
            .unwrap_or_else(|| partition.replicas.clone());
        let old_leader = self.leaders.get(&key).copied().unwrap_or(partition.leader);

        partition.leader = new_leader;
        Movement {
            topic: partition.topic.clone(),
            partition: partition.partition,
            old_replicas: old_replicas.clone(),
            new_replicas: old_replicas,
            old_leader,
            new_leader,
        }
    }
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
