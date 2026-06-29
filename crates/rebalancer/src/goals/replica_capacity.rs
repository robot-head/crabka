//! Hard goal: enforce a per-broker `max_replicas` limit from the
//! capacity config. Brokers without a config entry — or with
//! `max_replicas: None` — are ignored.
//!
//! `propose` emits a movement per iteration that swaps one replica
//! from an over-capacity broker to a broker with headroom. Greedy;
//! stops when no broker exceeds its limit or no valid swap remains.
//!
//! `is_satisfied` returns `true` unconditionally because the `Goal`
//! trait signature doesn't expose `GoalContext` to `is_satisfied`
//! (the per-broker limits live in the context). Capacity enforcement
//! happens at `propose` time only.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct ReplicaCapacity;

impl ReplicaCapacity {
    pub const NAME: &'static str = "ReplicaCapacity";

    /// Replica counts per broker (cluster-wide).
    fn counts(parts: &[PartitionView], broker_ids: &[i32]) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = broker_ids.iter().map(|b| (*b, 0)).collect();
        for p in parts {
            for r in &p.replicas {
                *m.entry(*r).or_insert(0) += 1;
            }
        }
        m
    }

    /// Find the broker with the largest excess over its configured
    /// `max_replicas`. Ignore brokers without an entry or without a
    /// `max_replicas` limit. Returns `None` when no broker is over.
    /// Ties on excess resolve to the lowest `broker_id`, so the emitted
    /// movement list is stable across runs for identical input
    /// (`HashMap` iteration order is randomized).
    fn find_over_capacity(counts: &HashMap<i32, usize>, ctx: &GoalContext) -> Option<i32> {
        // Sort by broker_id ascending so ties on excess resolve
        // deterministically (lower broker_id wins).
        let mut ordered: Vec<(i32, usize)> = counts.iter().map(|(b, c)| (*b, *c)).collect();
        ordered.sort_by_key(|(b, _)| *b);

        let mut over: Option<(i32, usize, u32)> = None;
        for (broker, current) in &ordered {
            let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                continue;
            };
            let Some(limit) = cap.max_replicas else {
                continue;
            };
            if *current > limit as usize {
                let excess = current.saturating_sub(limit as usize);
                let prior_excess = over.map_or(0, |(_, c, l)| c.saturating_sub(l as usize));
                if excess > prior_excess {
                    over = Some((*broker, *current, limit));
                }
            }
        }
        over.map(|(b, _, _)| b)
    }

    /// Pick a destination broker for the next replica eviction. Scores
    /// brokers by current replica count (lower = better) so the
    /// emptiest broker wins. Brokers with `max_replicas` already at or
    /// above their limit are pushed to the back (score = `usize::MAX`).
    /// Brokers with **no** capacity entry — or no `max_replicas` field —
    /// compete on equal footing with under-capacity entry brokers
    /// (operator hasn't expressed a constraint → fair game). Tie-breaks
    /// on `broker_id` ascending.
    fn pick_cold(
        broker_ids: &[i32],
        hot: i32,
        counts: &HashMap<i32, usize>,
        ctx: &GoalContext,
    ) -> Option<i32> {
        broker_ids
            .iter()
            .filter(|b| **b != hot)
            .min_by_key(|b| {
                let current = counts.get(b).copied().unwrap_or(0);
                let headroom = ctx
                    .broker_capacities
                    .for_broker(**b)
                    .and_then(|c| c.max_replicas);
                let score = match headroom {
                    Some(limit) if current < limit as usize => current,
                    Some(_) => usize::MAX,
                    None => current,
                };
                (score, **b)
            })
            .copied()
    }
}

impl Goal for ReplicaCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

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

        loop {
            let counts = Self::counts(&working, &broker_ids);

            let Some(hot) = Self::find_over_capacity(&counts, ctx) else {
                break;
            };
            let Some(cold) = Self::pick_cold(&broker_ids, hot, &counts, ctx) else {
                break;
            };
            // Refuse if the chosen `cold` is itself at or above its limit.
            if let Some(c) = ctx.broker_capacities.for_broker(cold)
                && let Some(limit) = c.max_replicas
                && counts.get(&cold).copied().unwrap_or(0) >= limit as usize
            {
                break;
            }

            // Find a partition on hot whose replica set doesn't already
            // include cold and where moving doesn't break RF.
            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() <= state.brokers.len()
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
                break;
            }
        }

        out
    }

    fn is_satisfied(&self, _state: &ClusterState) -> bool {
        // ReplicaCapacity's invariant depends on GoalContext.broker_capacities
        // which is_satisfied doesn't see. Returns true so soft goals can
        // proceed; propose-time enforcement is the real safety. The
        // `is_satisfied_with_ctx` override below closes that gap.
        true
    }

    fn is_satisfied_with_ctx(&self, state: &ClusterState, ctx: &GoalContext) -> bool {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let counts = Self::counts(&state.partitions, &broker_ids);
        for (broker, current) in &counts {
            let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                continue;
            };
            let Some(limit) = cap.max_replicas else {
                continue;
            };
            if *current > limit as usize {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::BrokerView;
    use crate::scraper::UsageStore;
    use assert2::assert;
    use std::sync::Arc;

    fn ctx_with(caps: BrokerCapacities) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(caps),
            broker_usages: Arc::new(UsageStore::default()),
        }
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

    fn caps_with(broker: i32, max_replicas: u32) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        b.insert(
            broker,
            BrokerCapacity {
                max_replicas: Some(max_replicas),
                ..Default::default()
            },
        );
        BrokerCapacities { by_broker: b }
    }

    fn caps_many(entries: &[(i32, u32)]) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        for (broker, max_replicas) in entries {
            b.insert(
                *broker,
                BrokerCapacity {
                    max_replicas: Some(*max_replicas),
                    ..Default::default()
                },
            );
        }
        BrokerCapacities { by_broker: b }
    }

    #[test]
    fn under_capacity_no_op() {
        let parts = vec![
            part("t", 0, vec![1, 2], 1),
            part("t", 1, vec![1, 2], 1),
            part("t", 2, vec![1, 2], 1),
        ];
        let s = state_with(parts, vec![1, 2]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps_with(1, 10)));
        assert!(mvs.is_empty(), "under-capacity must no-op, got {mvs:?}");
    }

    #[test]
    fn exact_capacity_no_op_and_satisfied() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps_with(1, 3));

        assert!(ReplicaCapacity.propose(&s, &ctx).is_empty());
        assert!(ReplicaCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn over_capacity_triggers_movement() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps_with(1, 3)));
        assert!(!mvs.is_empty(), "over-capacity must emit movements");
        for m in &mvs {
            let before = m.old_replicas.iter().filter(|x| **x == 1).count();
            let after = m.new_replicas.iter().filter(|x| **x == 1).count();
            assert!(
                after < before,
                "movement must reduce broker-1 replicas: {m:?}"
            );
        }
    }

    #[test]
    fn over_capacity_moves_to_alive_under_capacity_broker() {
        let parts = vec![
            part("hot", 0, vec![1], 1),
            part("hot", 1, vec![1], 1),
            part("warm", 0, vec![2], 2),
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(caps_many(&[(1, 1), (2, 1), (3, 2)]));

        let mvs = ReplicaCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].new_replicas == vec![3]);
    }

    #[test]
    fn exact_limit_destination_is_not_headroom() {
        let parts = vec![
            part("hot", 0, vec![1], 1),
            part("hot", 1, vec![1], 1),
            part("warm", 0, vec![3], 3),
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(caps_many(&[(1, 1), (2, 0), (3, 2)]));

        let mvs = ReplicaCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].new_replicas == vec![3]);
    }

    #[test]
    fn skips_partition_that_already_contains_cold_destination() {
        let parts = vec![part("t", 0, vec![1, 3], 1), part("t", 1, vec![1, 2], 1)];
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(caps_many(&[(1, 1), (2, 1), (3, 3)]));

        let mvs = ReplicaCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].partition == 1);
        assert!(mvs[0].old_replicas == vec![1, 2]);
        assert!(mvs[0].new_replicas == vec![3, 2]);
    }

    #[test]
    fn rehomes_leader_when_hot_leader_is_moved() {
        let parts = vec![part("hot", 0, vec![1], 1), part("hot", 1, vec![1], 1)];
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps_many(&[(1, 1), (2, 2)]));

        let mvs = ReplicaCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].old_leader == 1);
        assert!(mvs[0].new_leader == 2);
    }

    #[test]
    fn broker_without_entry_ignored() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(BrokerCapacities::default()));
        assert!(mvs.is_empty(), "no entries must no-op, got {mvs:?}");
    }

    #[test]
    fn broker_with_no_max_replicas_field_ignored() {
        let mut b = std::collections::HashMap::new();
        b.insert(
            1,
            BrokerCapacity {
                max_replicas: None,
                disk_bytes: Some(1_000),
                ..Default::default()
            },
        );
        let caps = BrokerCapacities { by_broker: b };
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps));
        assert!(mvs.is_empty(), "no max_replicas must no-op, got {mvs:?}");
    }

    #[test]
    fn over_capacity_evict_choice_is_deterministic() {
        // Two brokers (1, 2) each holding 5 replicas; max_replicas: 3.
        // Both have equal excess (2). Without sort, HashMap order would
        // pick either; with sort, broker 1 wins each run.
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                max_replicas: Some(3),
                ..Default::default()
            },
        );
        by.insert(
            2,
            BrokerCapacity {
                max_replicas: Some(3),
                ..Default::default()
            },
        );
        let caps = BrokerCapacities { by_broker: by };

        // 5 partitions, all on brokers 1 and 2 (broker 3 is the destination).
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);

        // Run propose 5 times against fresh state; first movement's
        // donor must be broker 1 each time (tie-broken by id).
        for _ in 0..5 {
            let mvs = ReplicaCapacity.propose(&s, &ctx_with(caps.clone()));
            assert!(!mvs.is_empty(), "expected at least one movement");
            let first = &mvs[0];
            // The first movement evicts whichever broker find_over_capacity
            // picked first. Determinism = same broker every run.
            let evicted = first
                .old_replicas
                .iter()
                .find(|r| !first.new_replicas.contains(r))
                .copied()
                .expect("a broker was evicted");
            assert!(
                evicted == 1,
                "tie should resolve to broker 1 (lowest id), got broker {evicted}"
            );
        }
    }

    #[test]
    fn is_satisfied_with_ctx_returns_false_when_over_capacity() {
        // Broker 1 has 5 replicas but max_replicas: 3 → not satisfied.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(caps_with(1, 3));
        assert!(!ReplicaCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn is_satisfied_without_context_is_true_even_when_over_capacity() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);

        assert!(ReplicaCapacity.is_satisfied(&s));
    }

    #[test]
    fn is_satisfied_with_ctx_returns_true_when_within() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps_with(1, 10));
        assert!(ReplicaCapacity.is_satisfied_with_ctx(&s, &ctx));
    }
}
