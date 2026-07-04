//! Hard goal: every partition's leader equals `replicas[0]` whenever
//! that broker is alive and in ISR.

use crate::{
    goals::{Goal, GoalContext, GoalPriority},
    model::{ClusterState, Movement},
};

pub struct PreferredLeaderIdempotency;

impl PreferredLeaderIdempotency {
    pub const NAME: &'static str = "PreferredLeaderIdempotency";
}

impl Goal for PreferredLeaderIdempotency {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }
    fn propose(&self, state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        let alive: std::collections::HashSet<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut out = Vec::new();
        for p in &state.partitions {
            let Some(&preferred) = p.replicas.first() else {
                continue;
            };
            if p.leader == preferred {
                continue;
            }
            if !alive.contains(&preferred) {
                continue;
            }
            if !p.isr.contains(&preferred) {
                continue;
            }
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas: p.replicas.clone(),
                new_replicas: p.replicas.clone(),
                old_leader: p.leader,
                new_leader: preferred,
            });
        }
        out
    }

    fn is_satisfied(&self, state: &ClusterState) -> bool {
        for p in &state.partitions {
            // Preferred = first replica. PLI is satisfied if the leader
            // matches the preferred whenever the preferred is alive + in ISR.
            let Some(preferred) = p.replicas.first().copied() else {
                continue;
            };
            let preferred_alive = state.brokers.iter().any(|b| b.id == preferred);
            let preferred_in_isr = p.isr.contains(&preferred);
            if preferred_alive && preferred_in_isr && p.leader != preferred {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::model::{BrokerView, PartitionView};

    fn state(parts: Vec<PartitionView>, alive_brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: alive_brokers
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

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: std::sync::Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: std::sync::Arc::new(crate::scraper::UsageStore::default()),
        }
    }

    fn part(replicas: Vec<i32>, leader: i32, isr: Vec<i32>) -> PartitionView {
        PartitionView {
            topic: "foo".into(),
            partition: 0,
            replicas,
            leader,
            isr,
        }
    }

    #[test]
    fn preferred_already_leader_no_op() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 1,
                isr: vec![1, 2, 3],
            }],
            vec![1, 2, 3],
        );
        assert!(PreferredLeaderIdempotency.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn is_satisfied_when_preferred_already_leads() {
        let s = state(vec![part(vec![1, 2, 3], 1, vec![1, 2, 3])], vec![1, 2, 3]);

        assert!(PreferredLeaderIdempotency.is_satisfied(&s));
    }

    #[test]
    fn is_not_satisfied_when_alive_isr_preferred_is_not_leader() {
        let s = state(vec![part(vec![1, 2, 3], 2, vec![1, 2, 3])], vec![1, 2, 3]);

        assert!(!PreferredLeaderIdempotency.is_satisfied(&s));
    }

    #[test]
    fn is_satisfied_when_preferred_is_dead_or_out_of_isr() {
        let preferred_dead = state(vec![part(vec![1, 2, 3], 2, vec![2, 3])], vec![2, 3]);
        let preferred_out_of_isr = state(vec![part(vec![1, 2, 3], 2, vec![2, 3])], vec![1, 2, 3]);

        assert!(PreferredLeaderIdempotency.is_satisfied(&preferred_dead));
        assert!(PreferredLeaderIdempotency.is_satisfied(&preferred_out_of_isr));
    }

    #[test]
    fn preferred_alive_in_isr_but_not_leader_triggers_swap() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 2,
                isr: vec![1, 2, 3],
            }],
            vec![1, 2, 3],
        );
        let mvs = PreferredLeaderIdempotency.propose(&s, &ctx());
        assert!(
            mvs == vec![Movement {
                topic: "foo".into(),
                partition: 0,
                old_replicas: vec![1, 2, 3],
                new_replicas: vec![1, 2, 3],
                old_leader: 2,
                new_leader: 1,
            }]
        );
    }

    #[test]
    fn preferred_dead_skipped() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 2,
                isr: vec![2, 3],
            }],
            vec![2, 3], // broker 1 is missing — dead
        );
        assert!(PreferredLeaderIdempotency.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn preferred_out_of_isr_skipped() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 2,
                isr: vec![2, 3], // broker 1 alive but not in ISR
            }],
            vec![1, 2, 3],
        );
        assert!(PreferredLeaderIdempotency.propose(&s, &ctx()).is_empty());
    }
}
