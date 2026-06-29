//! Soft goal: balance the count of partitions led per broker.
//! Movements are leader-only — replicas stay put — and only target
//! brokers already in the partition's replica set.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct LeaderDistribution;

impl LeaderDistribution {
    pub const NAME: &'static str = "LeaderDistribution";

    #[allow(dead_code)]
    fn leader_counts(state: &ClusterState) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = state.brokers.iter().map(|b| (b.id, 0)).collect();
        for p in &state.partitions {
            *m.entry(p.leader).or_insert(0) += 1;
        }
        m
    }

    fn imbalance_pct(counts: &HashMap<i32, usize>) -> u32 {
        let values: Vec<usize> = counts.values().copied().collect();
        let total: usize = values.iter().sum();
        if total == 0 {
            return 0;
        }
        let max = *values.iter().max().unwrap_or(&0);
        let min = *values.iter().min().unwrap_or(&0);
        u32::try_from((max - min) * 100 / total).unwrap_or(u32::MAX)
    }
}

impl Goal for LeaderDistribution {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        loop {
            let mut counts: HashMap<i32, usize> = state.brokers.iter().map(|b| (b.id, 0)).collect();
            for p in &working {
                *counts.entry(p.leader).or_insert(0) += 1;
            }
            if Self::imbalance_pct(&counts) <= ctx.imbalance_threshold_pct {
                break;
            }
            let mut by_load: Vec<(i32, usize)> = counts.into_iter().collect();
            by_load.sort_by_key(|b| std::cmp::Reverse(b.1));
            let (hot, _) = *by_load.first().expect("at least one broker");
            let (cold, _) = *by_load.last().expect("at least one broker");
            if hot == cold {
                break;
            }
            // Find a partition where:
            // - leader is `hot`
            // - `cold` is in the replica set (leader-only moves can
            //   only target an existing replica)
            // - `cold` is in ISR (leader must be in ISR per Kafka
            //   invariants)
            let idx = working.iter().position(|p| {
                p.leader == hot && p.replicas.contains(&cold) && p.isr.contains(&cold)
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let old_leader = p.leader;
            let old_replicas = p.replicas.clone();
            p.leader = cold;
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas: old_replicas.clone(),
                new_replicas: old_replicas, // leader-only move
                old_leader,
                new_leader: cold,
            });

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;
    use assert2::assert;

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: std::sync::Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: std::sync::Arc::new(crate::scraper::UsageStore::default()),
        }
    }

    fn state_with(partitions: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions,
            in_flight_reassignments: vec![],
        }
    }

    fn part(partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: "t".into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    #[test]
    fn balanced_no_movements() {
        let parts = vec![
            PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            },
            PartitionView {
                topic: "t".into(),
                partition: 1,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            },
        ];
        let s = state_with(parts, vec![1, 2]);
        assert!(LeaderDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn leader_only_movements_preserve_replicas() {
        // Every partition led by broker 1; broker 2 in every replica set.
        let parts = (0..4)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            })
            .collect();
        let s = state_with(parts, vec![1, 2]);
        let mvs = LeaderDistribution.propose(&s, &ctx());
        assert!(!mvs.is_empty());
        for m in &mvs {
            assert!(m.old_replicas == m.new_replicas, "leader-only move");
            assert!(m.old_leader == 1);
            assert!(m.new_leader == 2);
        }
    }

    #[test]
    fn skips_when_cold_broker_not_in_replicas() {
        // Broker 3 is "cold" but isn't in any partition's replica set.
        let parts = (0..4)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            })
            .collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = LeaderDistribution.propose(&s, &ctx());
        // No movement may target broker 3 as new_leader.
        for m in &mvs {
            assert!(m.new_leader != 3, "broker 3 isn't in any replica set");
        }
    }

    #[test]
    fn leader_counts_includes_idle_brokers_and_unknown_leaders() {
        let parts = vec![
            part(0, vec![1, 2], 1),
            part(1, vec![1, 2], 1),
            part(2, vec![2, 3], 3),
            part(3, vec![1, 2], 99),
        ];
        let s = state_with(parts, vec![1, 2, 3]);

        let counts = LeaderDistribution::leader_counts(&s);

        assert!(counts.get(&1) == Some(&2));
        assert!(counts.get(&2) == Some(&0));
        assert!(counts.get(&3) == Some(&1));
        assert!(counts.get(&99) == Some(&1));
    }

    #[test]
    fn imbalance_pct_uses_difference_times_100_over_total() {
        let counts = std::collections::HashMap::from([(1, 3), (2, 1)]);
        assert!(LeaderDistribution::imbalance_pct(&counts) == 50);
    }

    #[test]
    fn movement_cap_limits_leader_distribution_swaps() {
        let parts: Vec<_> = (0..6).map(|i| part(i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let mut ctx = ctx();
        ctx.max_movements_per_proposal = 1;

        let mvs = LeaderDistribution.propose(&s, &ctx);

        assert!(mvs.len() == 1);
    }
}
