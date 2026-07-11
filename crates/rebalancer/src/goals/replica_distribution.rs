//! Soft goal: balance the count of replicas (any role) hosted on each
//! broker. Greedy heuristic — swap one replica at a time from the
//! most-loaded broker to the least-loaded.

use std::collections::HashMap;

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
};

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
        crate::goals::imbalance_pct_usize(counts)
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

        // Snapshot the original replica set + leader per partition. When
        // a partition gets swapped twice in the loop, the second
        // Movement's `old_replicas` / `old_leader` must still reflect
        // the original cluster state — not the post-first-swap working
        // state. The optimizer's last-writer-wins coalesce keeps the
        // later Movement, so without this snapshot operators would see
        // an intermediate "before" state in the proposal.
        let originals = OriginalReplicaState::from_partitions(&state.partitions);

        loop {
            // Recompute counts from `working`.
            let mut counts: HashMap<i32, usize> = state.brokers.iter().map(|b| (b.id, 0)).collect();
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
            out.push(originals.replace_replica(p, hot, cold));

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::model::BrokerView;

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
    fn balanced_cluster_no_movements() {
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
                replicas: vec![2, 3],
                leader: 2,
                isr: vec![2, 3],
            },
            PartitionView {
                topic: "t".into(),
                partition: 2,
                replicas: vec![1, 3],
                leader: 3,
                isr: vec![1, 3],
            },
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        assert2::assert!(ReplicaDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn counts_includes_idle_brokers_and_unknown_replicas() {
        let parts = vec![
            part(0, vec![1, 2], 1),
            part(1, vec![1, 3], 1),
            part(2, vec![99], 99),
        ];
        let s = state_with(parts, vec![1, 2, 3, 4]);

        let counts = ReplicaDistribution::counts(&s);

        assert2::assert!(counts == HashMap::from([(1, 2), (2, 1), (3, 1), (4, 0), (99, 1)]));
    }

    #[test]
    fn imbalance_pct_uses_difference_times_100_over_total() {
        let counts = std::collections::HashMap::from([(1, 3), (2, 1)]);
        assert2::assert!(ReplicaDistribution::imbalance_pct(&counts) == 50);
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
        assert2::assert!(!mvs.is_empty());
        // RF preserved on every movement.
        for m in &mvs {
            assert2::assert!(m.old_replicas.len() == m.new_replicas.len());
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
        assert2::assert!(ReplicaDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn full_length_replica_vector_with_unknown_brokers_is_not_expandable() {
        let parts = vec![part(0, vec![1, 99], 1), part(1, vec![1], 1)];
        let s = state_with(parts, vec![1, 2]);

        let mvs = ReplicaDistribution.propose(&s, &ctx_with_cap(1));

        assert2::assert!(
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
    fn rehomes_leader_when_hot_replica_was_leader() {
        let parts = vec![
            part(0, vec![1], 1),
            part(1, vec![1], 1),
            part(2, vec![2], 2),
        ];
        let s = state_with(parts, vec![1, 2, 3]);

        let mvs = ReplicaDistribution.propose(&s, &ctx_with_cap(1));

        assert2::assert!(
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
    fn old_replicas_reflects_original_state() {
        // Force a double-swap on the same partition. With a single
        // partition replicated on brokers 1 + 2 on a 4-broker cluster,
        // brokers 3 + 4 are empty (imbalance = (2-0)*100/2 = 100%).
        // The greedy loop picks (hot=1-or-2, cold=3-or-4) and swaps the
        // sole candidate partition repeatedly. After two swaps the only
        // partition has been mutated twice, and pre-fix its second
        // Movement records the post-first-swap replicas as
        // `old_replicas`. We assert the contract: every Movement's
        // `old_replicas` / `old_leader` matches the *input*
        // ClusterState, regardless of how many times the algorithm
        // visited that partition.
        let parts: Vec<_> = vec![PartitionView {
            topic: "t".into(),
            partition: 0,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        }];
        let s = state_with(parts.clone(), vec![1, 2, 3, 4]);
        let mvs = ReplicaDistribution.propose(&s, &ctx());
        assert2::assert!(!mvs.is_empty());
        // Sanity check: we want this scenario to actually trigger the
        // double-swap path. Otherwise the test is silently passing for
        // the wrong reason and the bug could regress unnoticed.
        assert2::assert!(mvs.len() >= 2);
        for m in &mvs {
            let original = parts
                .iter()
                .find(|p| p.topic == m.topic && p.partition == m.partition)
                .expect("movement references a partition in the input");
            check!(
                m.old_replicas == original.replicas,
                "movement old_replicas must reflect the original cluster state, \
                 not an intermediate working-state snapshot"
            );
            check!(
                m.old_leader == original.leader,
                "movement old_leader must reflect the original cluster state, \
                 not an intermediate working-state snapshot"
            );
        }
    }

    fn ctx_with_cap(cap: usize) -> GoalContext {
        GoalContext {
            max_movements_per_proposal: cap,
            ..ctx()
        }
    }
}
