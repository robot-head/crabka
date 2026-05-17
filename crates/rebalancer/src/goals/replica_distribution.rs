//! Soft goal: balance the count of replicas (any role) hosted on each
//! broker. Greedy heuristic — swap one replica at a time from the
//! most-loaded broker to the least-loaded.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct ReplicaDistribution;

impl ReplicaDistribution {
    pub const NAME: &'static str = "ReplicaDistribution";

    /// Count of replicas hosted per broker id.
    #[allow(dead_code)]
    fn counts(state: &ClusterState) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = state.brokers.iter().map(|b| (b.id, 0)).collect();
        for p in &state.partitions {
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

impl Goal for ReplicaDistribution {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        // Working clone of partitions — we mutate replicas as we
        // accumulate movements so subsequent iterations see post-move
        // counts.
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        loop {
            // Recompute counts from `working`.
            let mut counts: HashMap<i32, usize> =
                state.brokers.iter().map(|b| (b.id, 0)).collect();
            for p in &working {
                for r in &p.replicas {
                    *counts.entry(*r).or_insert(0) += 1;
                }
            }
            if Self::imbalance_pct(&counts) <= ctx.imbalance_threshold_pct {
                break;
            }
            // Sort brokers by load: descending for "most loaded", ascending for "least loaded".
            let mut by_load: Vec<(i32, usize)> = counts.into_iter().collect();
            by_load.sort_by_key(|b| std::cmp::Reverse(b.1));
            let (hot, _hot_count) = *by_load.first().expect("at least one broker");
            let (cold, _cold_count) = *by_load.last().expect("at least one broker");
            if hot == cold {
                break;
            }
            // Find a partition on `hot` whose `replicas` set excludes `cold`.
            let candidate_idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    // Skip if the replica set already covers every alive broker — no spare home.
                    && p.replicas.len() < state.brokers.len()
            });
            let Some(idx) = candidate_idx else {
                // No valid swap remains.
                break;
            };
            let p = &mut working[idx];
            let old_replicas = p.replicas.clone();
            let pos = p.replicas.iter().position(|r| *r == hot).unwrap();
            p.replicas[pos] = cold;
            // If the leader was `hot`, choose a new leader from the new replica set.
            // Prefer staying with whatever's left of the prior ISR; fall back to the
            // first member of the new replica set.
            let new_leader = if p.leader == hot {
                *p.replicas
                    .iter()
                    .find(|r| p.isr.contains(r))
                    .unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader: p.leader,
                new_leader,
            });
            p.leader = new_leader;

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

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
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

    #[test]
    fn balanced_cluster_no_movements() {
        let parts = vec![
            PartitionView { topic: "t".into(), partition: 0, replicas: vec![1, 2], leader: 1, isr: vec![1, 2] },
            PartitionView { topic: "t".into(), partition: 1, replicas: vec![2, 3], leader: 2, isr: vec![2, 3] },
            PartitionView { topic: "t".into(), partition: 2, replicas: vec![1, 3], leader: 3, isr: vec![1, 3] },
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(ReplicaDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn one_hot_broker_produces_swaps() {
        // Every replica on broker 1 — broker 2 + 3 empty.
        let parts = (0..6)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            })
            .collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaDistribution.propose(&s, &ctx());
        assert!(!mvs.is_empty(), "expected at least one swap");
        // RF preserved on every movement.
        for m in &mvs {
            assert_eq!(m.old_replicas.len(), m.new_replicas.len());
        }
    }

    #[test]
    fn partition_already_on_every_broker_skipped() {
        // RF == broker_count: no spare home for swaps.
        let parts = vec![PartitionView {
            topic: "t".into(),
            partition: 0,
            replicas: vec![1, 2, 3],
            leader: 1,
            isr: vec![1, 2, 3],
        }];
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(ReplicaDistribution.propose(&s, &ctx()).is_empty());
    }
}
