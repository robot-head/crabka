//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules.

use std::{collections::HashMap, sync::Arc};

use crabka_units::{Ratio, fraction};
use num_traits::ToPrimitive;

use crate::{
    capacity::BrokerCapacities,
    model::{ClusterState, Movement, PartitionView},
    scraper::UsageStore,
};

/// Wall-clock millis since the Unix epoch. Goals that read usage data pass
/// this to `UsageStore` queries, so the queries exclude stale-broker samples
/// from the window. The value saturates to 0 or `i64::MAX` on overflow.
#[must_use]
pub fn now_ms() -> i64 {
    crate::time::now_ms()
}

/// Spread of a per-broker count, as a fraction of the cluster total:
/// `(max - min) / total`. An empty or all-zero distribution is perfectly
/// balanced.
pub(crate) fn imbalance_ratio_usize(counts: &HashMap<i32, usize>) -> Ratio {
    let values: Vec<usize> = counts.values().copied().collect();
    let total: usize = values.iter().sum();
    let max = *values.iter().max().unwrap_or(&0);
    let min = *values.iter().min().unwrap_or(&0);
    let spread = (max - min).to_f64().unwrap_or_default();
    match total.to_f64() {
        Some(total) if total > 0.0 => fraction(spread / total),
        _ => fraction(0.0),
    }
}

/// Same spread, over per-broker totals that are already floats: summed byte
/// counts, rates, or core counts.
pub(crate) fn imbalance_ratio_f64(totals: &HashMap<i32, f64>) -> Ratio {
    let vals: Vec<f64> = totals.values().copied().collect();
    let total: f64 = vals.iter().sum();
    if total <= 0.0 {
        return fraction(0.0);
    }
    let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
    let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
    fraction((max - min) / total)
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
    /// Soft goals improve placement on a best-effort basis. The optimizer
    /// skips movements that do not fit under the cap.
    Soft,
}

#[derive(Debug, Clone)]
pub struct GoalContext {
    /// `(max - min) / total` must exceed this fraction for a soft goal
    /// to act. Hard goals ignore the threshold.
    pub imbalance_threshold: Ratio,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` disables the goal.
    pub min_topic_leaders_per_broker: u32,
    /// Per-broker capacity limits for the five capacity goals.
    pub broker_capacities: Arc<BrokerCapacities>,
    /// Per-partition usage data, counters and gauges, from the metric
    /// scraper. With the empty default, usage-driven goals see no data and
    /// return an empty `Vec<Movement>`. This is the same self-limiting pattern
    /// as the capacity stubs.
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

    /// Returns true if the goal's invariant holds against `state` alone,
    /// without `GoalContext` access. Soft goals use the default, which is
    /// always true. Hard goals that do not depend on context, such as
    /// `PreferredLeaderIdempotency` and `RackAware`, override it.
    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        true
    }

    /// Same as `is_satisfied`, but with `GoalContext` access. The optimizer's
    /// incremental hard-goal validation calls this, so capacity goals can read
    /// `broker_capacities` and `broker_usages` when they decide whether a
    /// tentative movement keeps their invariant intact. The default forwards
    /// to `is_satisfied`.
    fn is_satisfied_with_ctx(&self, state: &ClusterState, _ctx: &GoalContext) -> bool {
        self.is_satisfied(state)
    }
}

#[cfg(test)]
pub mod tests {
    use crabka_units::prelude::*;

    use super::*;

    /// Minimal goal that returns a fixed movement list. `optimizer::tests`
    /// uses it to exercise the optimizer without a dependency on any concrete
    /// goal implementation.
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
        assert2::assert!(matches!(GoalPriority::Hard, GoalPriority::Hard));
        assert2::assert!(GoalPriority::Hard != GoalPriority::Soft);
    }

    /// Both spread helpers hand back a dimensionless fraction of the
    /// cluster total, not a percentage point count.
    #[test]
    fn imbalance_ratio_usize_is_spread_over_total() {
        let cases = [
            ("empty", vec![], fraction(0.0)),
            ("all zero", vec![(1, 0), (2, 0)], fraction(0.0)),
            ("balanced", vec![(1, 5), (2, 5)], fraction(0.0)),
            ("three to one", vec![(1, 3), (2, 1)], percent(50)),
            ("one broker", vec![(1, 4)], fraction(0.0)),
        ];
        for (name, counts, expected) in cases {
            let counts: HashMap<i32, usize> = counts.into_iter().collect();
            assert2::check!(imbalance_ratio_usize(&counts) == expected, "{}", name);
        }
    }

    /// A spread between two whole percentage points still counts against the
    /// threshold.
    ///
    /// A comparison as a truncated integer percentage reads a 10.9% imbalance
    /// as 10. That sits exactly on a 10% threshold without tripping it, so the
    /// goal declares the cluster balanced and moves nothing. A comparison of
    /// `Ratio` to `Ratio` keeps the fraction and therefore trips. Goals treat
    /// the threshold as inclusive, so `imbalance <= threshold` is balanced.
    /// The boundary case below pins that rule.
    #[test]
    fn fractional_spread_is_not_rounded_away_at_the_threshold() {
        let threshold = percent(10);

        // Spread is (max - min) over the cluster total, so these three brokers
        // sum to 1000 and differ by 109: a 10.9% imbalance.
        let just_over: HashMap<i32, f64> =
            [(1, 400.0), (2, 291.0), (3, 309.0)].into_iter().collect();
        assert2::check!(imbalance_ratio_f64(&just_over) > threshold);

        let exactly_at: HashMap<i32, f64> =
            [(1, 400.0), (2, 300.0), (3, 300.0)].into_iter().collect();
        assert2::check!(imbalance_ratio_f64(&exactly_at) == threshold);
        // The predicate the goals actually evaluate: at the threshold exactly,
        // the cluster still reads as balanced.
        assert2::check!(imbalance_ratio_f64(&exactly_at) <= threshold);

        let just_under: HashMap<i32, f64> =
            [(1, 399.0), (2, 300.0), (3, 301.0)].into_iter().collect();
        assert2::check!(imbalance_ratio_f64(&just_under) < threshold);
    }

    #[test]
    fn imbalance_ratio_f64_is_spread_over_total() {
        let cases = [
            ("empty", vec![], fraction(0.0)),
            ("all zero", vec![(1, 0.0), (2, 0.0)], fraction(0.0)),
            ("three to one", vec![(1, 300.0), (2, 100.0)], percent(50)),
            ("eighth", vec![(1, 500.0), (2, 300.0)], percent(25)),
        ];
        for (name, totals, expected) in cases {
            let totals: HashMap<i32, f64> = totals.into_iter().collect();
            assert2::check!(imbalance_ratio_f64(&totals) == expected, "{}", name);
        }
    }
}
