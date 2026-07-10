//! Soft goal: ensure every (broker, topic) pair where the broker
//! holds at least one replica also leads at least
//! `ctx.min_topic_leaders_per_broker` of that topic's partitions.
//!
//! Default `ctx.min_topic_leaders_per_broker == 0` makes the goal a
//! no-op. Operators opt in via the `--min-topic-leaders-per-broker`
//! CLI flag.
//!
//! Emits leader-only movements (replicas unchanged); the broker
//! receiving the leadership must already be in the partition's
//! replica set AND in ISR.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
};

pub struct MinTopicLeadersPerBroker;

impl MinTopicLeadersPerBroker {
    pub const NAME: &'static str = "MinTopicLeadersPerBroker";
}

impl Goal for MinTopicLeadersPerBroker {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let min = usize::try_from(ctx.min_topic_leaders_per_broker).unwrap_or(0);
        if min == 0 {
            return Vec::new();
        }

        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let topics: BTreeSet<String> = state.partitions.iter().map(|p| p.topic.clone()).collect();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();

        let originals = OriginalReplicaState::from_partitions(&state.partitions);

        loop {
            let mut under: Option<(i32, String, usize)> = None;
            'find_under: for topic in &topics {
                let topic_parts: Vec<&PartitionView> =
                    working.iter().filter(|p| p.topic == *topic).collect();
                let covered: HashSet<i32> = topic_parts
                    .iter()
                    .flat_map(|p| p.replicas.iter().copied())
                    .collect();
                for broker in &broker_ids {
                    if !covered.contains(broker) {
                        continue;
                    }
                    let leader_count = topic_parts.iter().filter(|p| p.leader == *broker).count();
                    if leader_count < min {
                        under = Some((*broker, topic.clone(), leader_count));
                        break 'find_under;
                    }
                }
            }
            let Some((under_broker, under_topic, _)) = under else {
                break;
            };

            let surplus_broker_count: HashMap<i32, usize> = {
                let topic_parts: Vec<&PartitionView> =
                    working.iter().filter(|p| p.topic == under_topic).collect();
                let mut m: HashMap<i32, usize> = HashMap::new();
                for p in &topic_parts {
                    *m.entry(p.leader).or_insert(0) += 1;
                }
                m
            };

            let idx = working.iter().position(|p| {
                p.topic == under_topic
                    && p.replicas.contains(&under_broker)
                    && p.isr.contains(&under_broker)
                    && p.leader != under_broker
                    && surplus_broker_count.get(&p.leader).copied().unwrap_or(0) > min
            });
            let Some(idx) = idx else {
                break;
            };

            let p = &mut working[idx];
            out.push(originals.change_leader(p, under_broker));

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

    fn ctx_with(min: u32) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: min,
            broker_capacities: std::sync::Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: std::sync::Arc::new(crate::scraper::UsageStore::default()),
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

    fn part_with_isr(
        topic: &str,
        partition: i32,
        replicas: Vec<i32>,
        leader: i32,
        isr: Vec<i32>,
    ) -> PartitionView {
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        part_with_isr(topic, partition, replicas, leader, isr)
    }

    #[test]
    fn min_zero_is_no_op() {
        let parts: Vec<_> = (0..4).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        assert2::assert!(
            MinTopicLeadersPerBroker
                .propose(&s, &ctx_with(0))
                .is_empty()
        );
    }

    #[test]
    fn min_one_ensures_coverage() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = MinTopicLeadersPerBroker.propose(&s, &ctx_with(1));
        assert2::assert!(mvs.len() >= 2);
        for m in &mvs {
            assert2::assert!(m.old_replicas == m.new_replicas);
        }
    }

    #[test]
    fn broker_not_in_replica_set_skipped() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = MinTopicLeadersPerBroker.propose(&s, &ctx_with(1));
        for m in &mvs {
            assert2::assert!(m.new_leader != 3);
        }
    }

    #[test]
    fn under_served_not_in_isr_skipped() {
        // Broker 2 is in every partition's replica set but never in ISR.
        // The goal must not emit any movement because flipping leadership
        // to a non-ISR broker would violate Kafka ISR invariants.
        let parts: Vec<_> = (0..3)
            .map(|i| part_with_isr("t", i, vec![1, 2], 1, vec![1]))
            .collect();
        let s = state_with(parts, vec![1, 2]);
        let mvs = MinTopicLeadersPerBroker.propose(&s, &ctx_with(1));
        assert2::assert!(mvs.is_empty());
    }

    #[test]
    fn broker_at_minimum_is_not_under_served() {
        let parts = vec![
            part("t", 0, vec![1, 2], 1),
            part("t", 1, vec![1, 2], 2),
            part("t", 2, vec![1, 2], 2),
            part("t", 3, vec![1, 2], 2),
        ];
        let s = state_with(parts, vec![1, 2]);

        assert2::assert!(
            MinTopicLeadersPerBroker
                .propose(&s, &ctx_with(1))
                .is_empty()
        );
    }

    #[test]
    fn donor_at_minimum_is_not_drained() {
        let parts = vec![part("t", 0, vec![1, 2], 1)];
        let s = state_with(parts, vec![1, 2]);

        assert2::assert!(
            MinTopicLeadersPerBroker
                .propose(&s, &ctx_with(1))
                .is_empty()
        );
    }
}
