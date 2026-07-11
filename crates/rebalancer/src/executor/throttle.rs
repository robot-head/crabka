//! Pure-logic KIP-73 throttle target computation. Given a slice of
//! `Movement`s, returns the per-broker rate targets and per-topic
//! replica-list targets that `ApplyThrottle` will write via
//! `IncrementalAlterConfigs`.
//!
//! The computation is deterministic and side-effect-free so the
//! executor's state machine can test it in isolation.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::Movement;

/// All four KIP-73 target families for a single proposal.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ThrottleTargets {
    /// Brokers that will act as leaders for moving replicas.
    /// `leader.replication.throttled.rate` is set on each.
    pub leader_brokers: BTreeSet<i32>,
    /// Brokers that will act as new followers (catching up).
    /// `follower.replication.throttled.rate` is set on each.
    pub follower_brokers: BTreeSet<i32>,
    /// Per-topic value for `leader.replication.throttled.replicas`.
    /// Map value is the canonical `partition:broker,partition:broker,...`
    /// string ready for `IncrementalAlterConfigs`.
    pub leader_replicas_per_topic: BTreeMap<String, String>,
    /// Per-topic value for `follower.replication.throttled.replicas`.
    pub follower_replicas_per_topic: BTreeMap<String, String>,
}

#[must_use]
pub fn compute_throttle_targets(movements: &[Movement]) -> ThrottleTargets {
    let mut leader_brokers: BTreeSet<i32> = BTreeSet::new();
    let mut follower_brokers: BTreeSet<i32> = BTreeSet::new();
    // Topic → (partition, broker) entries, kept sorted for deterministic output.
    let mut leader_replicas_per_topic: BTreeMap<String, BTreeSet<(i32, i32)>> = BTreeMap::new();
    let mut follower_replicas_per_topic: BTreeMap<String, BTreeSet<(i32, i32)>> = BTreeMap::new();

    for m in movements {
        let old: BTreeSet<i32> = m.old_replicas.iter().copied().collect();
        let new: BTreeSet<i32> = m.new_replicas.iter().copied().collect();

        // Leaders = the set of source brokers (movements *from*).
        for src in &old {
            leader_brokers.insert(*src);
            leader_replicas_per_topic
                .entry(m.topic.clone())
                .or_default()
                .insert((m.partition, *src));
        }
        // Followers = new replicas that weren't already in `old`.
        for dst in new.difference(&old) {
            follower_brokers.insert(*dst);
            follower_replicas_per_topic
                .entry(m.topic.clone())
                .or_default()
                .insert((m.partition, *dst));
        }
    }

    ThrottleTargets {
        leader_brokers,
        follower_brokers,
        leader_replicas_per_topic: stringify(&leader_replicas_per_topic),
        follower_replicas_per_topic: stringify(&follower_replicas_per_topic),
    }
}

fn stringify(per_topic: &BTreeMap<String, BTreeSet<(i32, i32)>>) -> BTreeMap<String, String> {
    per_topic
        .iter()
        .map(|(topic, entries)| {
            let joined = entries
                .iter()
                .map(|(p, b)| format!("{p}:{b}"))
                .collect::<Vec<_>>()
                .join(",");
            (topic.clone(), joined)
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    fn mv(topic: &str, p: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition: p,
            old_replicas: old,
            new_replicas: new,
            old_leader: 0,
            new_leader: 0,
        }
    }

    #[test]
    fn empty_movements_returns_empty_targets() {
        let t = compute_throttle_targets(&[]);
        assert2::assert!(
            t == ThrottleTargets {
                leader_brokers: BTreeSet::new(),
                follower_brokers: BTreeSet::new(),
                leader_replicas_per_topic: BTreeMap::new(),
                follower_replicas_per_topic: BTreeMap::new(),
            }
        );
    }

    #[test]
    fn single_movement_one_topic() {
        // Move partition 0 from broker 1 to broker 2.
        let m = mv("t", 0, vec![1], vec![2]);
        let t = compute_throttle_targets(std::slice::from_ref(&m));
        assert2::assert!(
            t == ThrottleTargets {
                leader_brokers: BTreeSet::from([1]),
                follower_brokers: BTreeSet::from([2]),
                leader_replicas_per_topic: BTreeMap::from([("t".to_string(), "0:1".to_string())]),
                follower_replicas_per_topic: BTreeMap::from([("t".to_string(), "0:2".to_string())]),
            }
        );
    }

    #[test]
    fn replica_set_growth_distinguishes_new_vs_existing() {
        // Replicas [1] → [1, 2]: broker 1 stays, broker 2 is the new follower.
        let m = mv("t", 5, vec![1], vec![1, 2]);
        let t = compute_throttle_targets(std::slice::from_ref(&m));
        // leader.replication.throttled.replicas covers the partition × source brokers.
        assert2::assert!(
            t == ThrottleTargets {
                leader_brokers: BTreeSet::from([1]),
                follower_brokers: BTreeSet::from([2]),
                leader_replicas_per_topic: BTreeMap::from([("t".to_string(), "5:1".to_string())]),
                follower_replicas_per_topic: BTreeMap::from([("t".to_string(), "5:2".to_string())]),
            }
        );
    }

    #[test]
    fn multiple_movements_aggregate_per_topic() {
        let ms = vec![
            mv("t1", 0, vec![1], vec![2]),
            mv("t1", 1, vec![1, 3], vec![2, 3]),
            mv("t2", 0, vec![2], vec![1]),
        ];
        let t = compute_throttle_targets(&ms);
        // Per-topic strings are sorted by (partition, broker).
        assert2::assert!(
            t == ThrottleTargets {
                leader_brokers: BTreeSet::from([1, 2, 3]),
                follower_brokers: BTreeSet::from([1, 2]),
                leader_replicas_per_topic: BTreeMap::from([
                    ("t1".to_string(), "0:1,1:1,1:3".to_string()),
                    ("t2".to_string(), "0:2".to_string()),
                ]),
                follower_replicas_per_topic: BTreeMap::from([
                    ("t1".to_string(), "0:2,1:2".to_string()),
                    ("t2".to_string(), "0:1".to_string()),
                ]),
            }
        );
    }

    #[test]
    fn output_is_deterministic_across_input_orders() {
        let a = vec![mv("z", 1, vec![3], vec![4]), mv("a", 0, vec![1], vec![2])];
        let b = vec![mv("a", 0, vec![1], vec![2]), mv("z", 1, vec![3], vec![4])];
        assert2::assert!(compute_throttle_targets(&a) == compute_throttle_targets(&b));
    }
}
