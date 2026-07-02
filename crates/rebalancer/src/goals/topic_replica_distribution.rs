//! Soft goal: per-topic, balance replica counts across brokers.
//!
//! Distinct from `ReplicaDistribution`, which balances cluster-wide
//! replica counts (sum across all topics). A cluster can be evenly
//! balanced overall while one topic is concentrated on a single
//! broker — that case is invisible to `ReplicaDistribution` but
//! fixed by this goal.

use std::collections::{HashMap, HashSet};

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct TopicReplicaDistribution;

impl TopicReplicaDistribution {
    pub const NAME: &'static str = "TopicReplicaDistribution";

    /// Replicas of `topic` per broker id.
    fn counts_for_topic(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        topic: &str,
    ) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = broker_ids.iter().map(|b| (*b, 0)).collect();
        for p in partitions.iter().filter(|p| p.topic == topic) {
            for r in &p.replicas {
                *m.entry(*r).or_insert(0) += 1;
            }
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

impl Goal for TopicReplicaDistribution {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let topics: HashSet<String> = state.partitions.iter().map(|p| p.topic.clone()).collect();

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        for topic in &topics {
            loop {
                let counts = Self::counts_for_topic(&working, &broker_ids, topic);
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

                let idx = working.iter().position(|p| {
                    p.topic == *topic
                        && p.replicas.contains(&hot)
                        && !p.replicas.contains(&cold)
                        && p.replicas.len() < state.brokers.len()
                });
                let Some(idx) = idx else {
                    break;
                };

                let p = &mut working[idx];
                let key = (p.topic.clone(), p.partition);
                let pos = p
                    .replicas
                    .iter()
                    .position(|r| *r == hot)
                    .expect("hot present");
                p.replicas[pos] = cold;
                let new_leader = if p.leader == hot {
                    *p.replicas
                        .iter()
                        .find(|r| p.isr.contains(r))
                        .unwrap_or(&p.replicas[0])
                } else {
                    p.leader
                };

                let old_replicas = original_replicas
                    .get(&key)
                    .cloned()
                    .unwrap_or_else(|| p.replicas.clone());
                let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);

                out.push(Movement {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    old_replicas,
                    new_replicas: p.replicas.clone(),
                    old_leader,
                    new_leader,
                });
                p.leader = new_leader;

                if out.len() >= ctx.max_movements_per_proposal {
                    return out;
                }
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

    fn ctx_with(threshold: u32, cap: usize) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: threshold,
            max_movements_per_proposal: cap,
            min_topic_leaders_per_broker: 0,
            broker_capacities: std::sync::Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: std::sync::Arc::new(crate::scraper::UsageStore::default()),
        }
    }

    fn ctx() -> GoalContext {
        ctx_with(10, 256)
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
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
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    #[test]
    fn balanced_topic_no_op() {
        let mut parts = Vec::new();
        for i in 0..4 {
            parts.push(part("t", i, vec![1], 1));
            parts.push(part("t", i + 100, vec![2], 2));
            parts.push(part("t", i + 200, vec![3], 3));
        }
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(TopicReplicaDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn imbalance_pct_uses_difference_times_100_over_total() {
        let counts = std::collections::HashMap::from([(1, 3), (2, 1)]);
        assert!(TopicReplicaDistribution::imbalance_pct(&counts) == 50);
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        let parts: Vec<_> = (0..9).map(|i| part("t", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = TopicReplicaDistribution.propose(&s, &ctx());
        assert!(
            !mvs.is_empty(),
            "expected swaps for hot-broker concentration"
        );
        for m in &mvs {
            assert!(m.old_replicas.len() == m.new_replicas.len());
        }
    }

    #[test]
    fn full_length_topic_replica_vector_with_unknown_broker_is_not_expandable() {
        let parts = vec![part("t", 0, vec![1, 99], 1), part("t", 1, vec![1], 1)];
        let s = state_with(parts, vec![1, 2]);

        let mvs = TopicReplicaDistribution.propose(&s, &ctx_with(10, 1));

        assert!(
            mvs == vec![Movement {
                topic: "t".into(),
                partition: 1,
                old_replicas: vec![1],
                new_replicas: vec![2],
                old_leader: 1,
                new_leader: 2,
            }]
        );
    }

    #[test]
    fn replaces_hot_replica_at_its_actual_position() {
        let parts = vec![
            part("t", 0, vec![99, 1], 99),
            part("t", 1, vec![1], 1),
            part("t", 2, vec![2], 2),
            part("t", 3, vec![99], 99),
            part("t", 4, vec![1], 1),
        ];
        let s = state_with(parts, vec![1, 2, 99]);

        let mvs = TopicReplicaDistribution.propose(&s, &ctx_with(10, 1));

        assert!(
            mvs == vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![99, 1],
                new_replicas: vec![99, 2],
                old_leader: 99,
                new_leader: 99,
            }]
        );
    }

    #[test]
    fn rehomes_leader_when_hot_topic_replica_was_leader() {
        let parts = vec![
            part("t", 0, vec![1], 1),
            part("t", 1, vec![1], 1),
            part("t", 2, vec![2], 2),
        ];
        let s = state_with(parts, vec![1, 2, 3]);

        let mvs = TopicReplicaDistribution.propose(&s, &ctx_with(10, 1));

        assert!(
            mvs == vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1],
                new_replicas: vec![3],
                old_leader: 1,
                new_leader: 3,
            }]
        );
    }

    #[test]
    fn multi_topic_independence() {
        let mut parts = vec![
            part("a", 0, vec![1], 1),
            part("a", 1, vec![2], 2),
            part("a", 2, vec![3], 3),
        ];
        for i in 0..6 {
            parts.push(part("b", i, vec![1], 1));
        }
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = TopicReplicaDistribution.propose(&s, &ctx());
        for m in &mvs {
            assert!(m.topic == "b", "movement on wrong topic: {m:?}");
        }
        assert!(!mvs.is_empty(), "expected swaps on topic b");
    }

    #[test]
    fn respects_max_movements_cap() {
        let parts: Vec<_> = (0..20).map(|i| part("t", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = TopicReplicaDistribution.propose(&s, &ctx_with(10, 2));
        assert!(
            mvs.len() <= 2,
            "expected at most 2 movements per cap, got {}",
            mvs.len()
        );
    }

    #[test]
    fn movement_cap_stops_at_exact_limit_when_more_work_remains() {
        let parts: Vec<_> = (0..20).map(|i| part("t", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);

        let mvs = TopicReplicaDistribution.propose(&s, &ctx_with(10, 1));

        assert!(mvs.len() == 1);
    }
}
