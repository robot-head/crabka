//! Hard goal: ensure no two replicas of the same partition share a
//! rack. Brokers with `rack: None` each count as their own
//! pseudo-rack (matches Kafka KIP-36 broker-side rack-aware
//! assignment behavior).
//!
//! Strict mode: if RF exceeds the distinct rack count for the cluster,
//! the affected partition is logged at warn level and skipped — the
//! goal never produces `HardGoalUnsatisfied`. Operators with
//! RF > rack-count get a no-op rather than a failed proposal.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use tracing::warn;

use crate::{
    goals::{Goal, GoalContext, GoalPriority},
    model::{ClusterState, Movement, PartitionView},
};

pub struct RackAware;

impl RackAware {
    pub const NAME: &'static str = "RackAware";
}

impl Goal for RackAware {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let rack_of = build_rack_map(state);
        let distinct_rack_count: usize = rack_of.values().cloned().collect::<BTreeSet<_>>().len();

        // Build a working copy so multi-pass within one propose call sees
        // post-swap state.
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        // Snapshot original old_replicas / old_leader to avoid drift
        // when the same partition is touched twice.
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

        let mut warned_partitions: HashSet<(String, i32)> = HashSet::new();

        loop {
            let chosen = pick_swap(
                state,
                &working,
                &rack_of,
                distinct_rack_count,
                &mut warned_partitions,
            );
            let Some((idx, donor, target)) = chosen else {
                break;
            };

            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p
                .replicas
                .iter()
                .position(|r| *r == donor)
                .expect("donor present");
            p.replicas[pos] = target;
            let new_leader = if p.leader == donor {
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

    fn is_satisfied(&self, state: &ClusterState) -> bool {
        // Build the same per-broker rack map propose() uses, so the
        // satisfied-check matches the goal's own model.
        let rack_of: HashMap<i32, String> = state
            .brokers
            .iter()
            .map(|b| {
                let tag = b
                    .rack
                    .clone()
                    .unwrap_or_else(|| format!("__no_rack_{}", b.id));
                (b.id, tag)
            })
            .collect();

        let distinct_rack_count: usize = state
            .brokers
            .iter()
            .map(|b| rack_of.get(&b.id).cloned().unwrap_or_default())
            .collect::<BTreeSet<_>>()
            .len();

        for p in &state.partitions {
            if p.replicas.len() > distinct_rack_count {
                // Infeasible partition — RackAware self-limits, so we
                // accept its current state as "satisfied" for composition
                // purposes (the goal's propose would emit nothing).
                continue;
            }
            let mut racks_seen = BTreeSet::new();
            for r in &p.replicas {
                if let Some(rack) = rack_of.get(r)
                    && !racks_seen.insert(rack.clone())
                {
                    return false;
                }
            }
        }
        true
    }
}

/// Per-broker rack tag, treating None as a per-broker unique pseudo-rack.
/// Encode unique racks as a synthetic `__no_rack_<broker_id>` string so
/// collision detection is straightforward.
fn build_rack_map(state: &ClusterState) -> HashMap<i32, String> {
    state
        .brokers
        .iter()
        .map(|b| {
            let tag = b
                .rack
                .clone()
                .unwrap_or_else(|| format!("__no_rack_{}", b.id));
            (b.id, tag)
        })
        .collect()
}

/// Scan `working` for the next viable (idx, donor, target) swap.
/// Returns `None` once every partition is either rack-diverse or
/// infeasible.
fn pick_swap(
    state: &ClusterState,
    working: &[PartitionView],
    rack_of: &HashMap<i32, String>,
    distinct_rack_count: usize,
    warned_partitions: &mut HashSet<(String, i32)>,
) -> Option<(usize, i32, i32)> {
    for (idx, p) in working.iter().enumerate() {
        if p.replicas.len() > distinct_rack_count {
            // Dedupe by (topic, partition) so each infeasible partition
            // warns at most once per propose() call.
            let key = (p.topic.clone(), p.partition);
            if warned_partitions.insert(key) {
                warn!(
                    topic = %p.topic,
                    partition = p.partition,
                    rf = p.replicas.len(),
                    rack_count = distinct_rack_count,
                    "`RackAware`: cluster has fewer racks than RF; goal self-limits"
                );
            }
            continue;
        }

        let mut by_rack: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for r in &p.replicas {
            if let Some(rack) = rack_of.get(r) {
                by_rack.entry(rack.clone()).or_default().push(*r);
            }
        }
        let collision = by_rack.iter().find(|(_, brokers)| brokers.len() >= 2);
        let Some((_collision_rack, brokers_in_collision)) = collision else {
            continue;
        };
        let donor = *brokers_in_collision.iter().max().expect("non-empty");

        let used_racks: BTreeSet<String> = by_rack.keys().cloned().collect();
        let mut candidate_brokers: Vec<i32> = state
            .brokers
            .iter()
            .filter(|b| {
                let r = rack_of.get(&b.id).expect("rack_of populated");
                !used_racks.contains(r) && !p.replicas.contains(&b.id)
            })
            .map(|b| b.id)
            .collect();
        candidate_brokers.sort_unstable();
        let target = candidate_brokers.first().copied()?;

        return Some((idx, donor, target));
    }
    None
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

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

    fn broker(id: i32, rack: Option<&str>) -> BrokerView {
        BrokerView {
            id,
            host: format!("h{id}"),
            port: 9092,
            rack: rack.map(str::to_string),
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<BrokerView>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers,
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
    fn balanced_three_racks_no_op() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("b")),
            broker(3, Some("c")),
        ];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);
        assert!(RackAware.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn single_collision_resolved() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
        ];
        let parts = vec![part("t", 0, vec![1, 2], 1)];
        let s = state_with(parts, brokers);
        let mvs = RackAware.propose(&s, &ctx());
        assert!(
            mvs == vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 3],
                old_leader: 1,
                new_leader: 1,
            }],
            "exactly one movement for one collision"
        );
    }

    #[test]
    fn collision_uses_unused_rack_not_lower_id_used_rack_broker() {
        let brokers = vec![
            broker(0, Some("a")),
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
            broker(4, Some("c")),
        ];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);

        let mvs = RackAware.propose(&s, &ctx());

        assert!(mvs.len() == 1);
        assert!(mvs[0].new_replicas == vec![1, 4, 3]);
    }

    #[test]
    fn collision_rehomes_leader_when_donor_was_leader() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
        ];
        let parts = vec![part("t", 0, vec![1, 2], 2)];
        let s = state_with(parts, brokers);

        let mvs = RackAware.propose(&s, &ctx());

        assert!(
            mvs == vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 3],
                old_leader: 2,
                new_leader: 1,
            }]
        );
    }

    #[test]
    fn multi_collision_iterates_within_propose() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
            broker(4, Some("c")),
        ];
        let parts = vec![part("t", 0, vec![1, 2], 1), part("t", 1, vec![1, 2], 1)];
        let s = state_with(parts, brokers);
        let mvs = RackAware.propose(&s, &ctx());
        assert!(mvs.len() == 2, "one movement per partition");
        for m in &mvs {
            check!(m.old_replicas == vec![1, 2]);
            check!(!m.new_replicas.contains(&2), "broker 2 must move out");
        }
    }

    #[test]
    fn rf_equals_rack_count_satisfiable() {
        let brokers = vec![broker(1, Some("a")), broker(2, Some("b"))];
        let parts = vec![part("t", 0, vec![1, 2], 1)];
        let s = state_with(parts, brokers);
        assert!(RackAware.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn rf_greater_than_rack_count_logs_warn_and_skips() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
        ];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);
        let mvs = RackAware.propose(&s, &ctx());
        assert!(
            mvs.is_empty(),
            "RF > rack count must self-limit, got {mvs:?}"
        );
    }

    #[test]
    fn is_satisfied_accepts_rack_diverse_assignment() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("b")),
            broker(3, Some("c")),
        ];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);

        assert!(RackAware.is_satisfied(&s));
    }

    #[test]
    fn is_satisfied_self_limits_infeasible_rf() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
        ];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);

        assert!(RackAware.is_satisfied(&s));
    }

    #[test]
    fn is_satisfied_detects_rack_collision() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
        ];
        let parts = vec![part("t", 0, vec![1, 2], 1)];
        let s = state_with(parts, brokers);

        assert!(!RackAware.is_satisfied(&s));
    }
}
