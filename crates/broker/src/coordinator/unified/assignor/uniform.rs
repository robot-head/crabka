//! `UniformAssignor` — KIP-848's default. Distributes partitions as evenly
//! as possible across members subscribed to each topic.
//!
//! Rack-aware: when the coordinator's `TopicMetadata` carries
//! a `partition_racks` entry for `(topic_id, partition_index)`, the
//! assignor prefers subscribers whose `rack_id` matches one of the
//! partition's replica racks. If no subscriber matches (or the partition
//! has no rack data), all subscribers are eligible — the original
//! non-rack-aware behavior.
//!
//! Selection within the eligible pool minimizes the running per-member
//! partition count for this topic, with ties broken by member-id lex
//! order. Without rack data the running-min reduces exactly to the
//! original `p % subscribers.len()` round-robin, so behavior is
//! unchanged for clusters that don't expose broker racks.

use std::collections::HashMap;

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

#[derive(Debug)]
pub struct UniformAssignor;

impl Assignor for UniformAssignor {
    fn name(&self) -> &'static str {
        "uniform"
    }

    fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment {
        let mut out: Assignment = HashMap::new();
        for m in members {
            out.insert(m.member_id.clone(), HashMap::new());
        }
        // Per-member rack lookup table; one entry per subscribed member.
        let rack_by_member: HashMap<&str, Option<&str>> = members
            .iter()
            .map(|m| (m.member_id.as_str(), m.rack_id.as_deref()))
            .collect();

        for (topic_id, partition_count) in &topics.partitions_per_topic {
            let mut subscribers: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str())
                .collect();
            subscribers.sort_unstable();
            if subscribers.is_empty() {
                continue;
            }
            // Per-member partition count for THIS topic, used to choose
            // the least-loaded member from the eligible pool. Reset per
            // topic — KIP-848 balances within-topic, not across topics.
            let mut count_by_member: HashMap<&str, usize> =
                subscribers.iter().map(|&s| (s, 0)).collect();

            for p in 0..*partition_count {
                let eligible = eligible_subscribers_for_partition(
                    &subscribers,
                    &rack_by_member,
                    topics.partition_racks.get(&(*topic_id, p)),
                );
                let chosen = eligible
                    .iter()
                    .min_by_key(|&&sid| (count_by_member[sid], sid))
                    .copied()
                    .expect("eligible is non-empty: falls back to all subscribers");
                *count_by_member.get_mut(chosen).expect("subscriber tracked") += 1;
                let mid = chosen.to_string();
                out.get_mut(&mid)
                    .expect("inserted above")
                    .entry(*topic_id)
                    .or_default()
                    .push(p);
            }
        }
        out
    }
}

/// Compute the pool of subscribers eligible for a partition. When the
/// partition has rack info AND at least one subscriber rack matches, the
/// pool is just those rack-collocated subscribers. Otherwise the pool is
/// all subscribers (the non-rack-aware fallback).
fn eligible_subscribers_for_partition<'a>(
    subscribers: &[&'a str],
    rack_by_member: &HashMap<&str, Option<&str>>,
    partition_racks: Option<&Vec<String>>,
) -> Vec<&'a str> {
    let Some(racks) = partition_racks.filter(|r| !r.is_empty()) else {
        return subscribers.to_vec();
    };
    let preferred: Vec<&str> = subscribers
        .iter()
        .copied()
        .filter(|sid| {
            rack_by_member
                .get(sid)
                .and_then(|r| r.as_deref())
                .is_some_and(|r| racks.iter().any(|s| s == r))
        })
        .collect();
    if preferred.is_empty() {
        subscribers.to_vec()
    } else {
        preferred
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_protocol::primitives::uuid::Uuid;

    use super::*;

    fn tid(b: u8) -> Uuid {
        Uuid([b; 16])
    }

    fn member(id: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: None,
            subscribed_topic_ids: topics.to_vec(),
        }
    }

    fn member_in_rack(id: &str, rack: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: Some(rack.into()),
            subscribed_topic_ids: topics.to_vec(),
        }
    }

    #[test]
    fn assignment_scenarios() {
        let t = tid(1);
        let cases = [
            (
                "single member",
                vec![member("m1", &[t])],
                4,
                HashMap::from([("m1".into(), HashMap::from([(t, vec![0, 1, 2, 3])]))]),
            ),
            (
                "round robin",
                vec![member("m1", &[t]), member("m2", &[t])],
                4,
                HashMap::from([
                    ("m1".into(), HashMap::from([(t, vec![0, 2])])),
                    ("m2".into(), HashMap::from([(t, vec![1, 3])])),
                ]),
            ),
            (
                "unsubscribed member",
                vec![member("m1", &[t]), member("m2", &[])],
                2,
                HashMap::from([
                    ("m1".into(), HashMap::from([(t, vec![0, 1])])),
                    ("m2".into(), HashMap::new()),
                ]),
            ),
            (
                "zero partitions",
                vec![member("m1", &[t])],
                0,
                HashMap::from([("m1".into(), HashMap::new())]),
            ),
            ("empty members", vec![], 4, HashMap::new()),
        ];

        for (case, members, partition_count, expected) in cases {
            let topics = TopicMetadata {
                partitions_per_topic: [(t, partition_count)].into(),
                ..Default::default()
            };
            assert!(
                UniformAssignor.assign(&members, &topics) == expected,
                "case {case}"
            );
        }
    }

    #[test]
    fn deterministic_under_member_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
            ..Default::default()
        };
        let a1 = UniformAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        let a2 = UniformAssignor.assign(
            &[member("m3", &[t]), member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        assert!(a1 == a2);
    }

    // ── rack-aware ────────────────────────────────────────────────

    /// Build a `TopicMetadata` with rack info on every partition of `t`.
    fn topics_with_racks(
        t: Uuid,
        partitions: i32,
        racks_per_partition: Vec<Vec<&str>>,
    ) -> TopicMetadata {
        let mut partition_racks = std::collections::HashMap::new();
        for (i, racks) in racks_per_partition.into_iter().enumerate() {
            partition_racks.insert(
                (t, i32::try_from(i).unwrap()),
                racks.into_iter().map(String::from).collect(),
            );
        }
        TopicMetadata {
            partitions_per_topic: [(t, partitions)].into(),
            partition_racks,
        }
    }

    #[test]
    fn rack_aware_prefers_collocated_member() {
        // Two members, two racks, two partitions each pinned to one rack.
        // Each member must own exactly the partition for its own rack.
        let t = tid(1);
        let topics = topics_with_racks(t, 2, vec![vec!["us-east-1a"], vec!["us-east-1b"]]);
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "us-east-1a", &[t]),
                member_in_rack("m2", "us-east-1b", &[t]),
            ],
            &topics,
        );
        assert!(
            a == HashMap::from([
                ("m1".into(), HashMap::from([(t, vec![0])])),
                ("m2".into(), HashMap::from([(t, vec![1])])),
            ])
        );
    }

    #[test]
    fn rack_aware_falls_back_to_round_robin_when_no_rack_match() {
        // Both partitions are in us-east-1a; members are in us-east-1b
        // and us-east-1c. No subscriber matches → fall back to balanced
        // round-robin over all subscribers.
        let t = tid(1);
        let topics = topics_with_racks(t, 4, vec![vec!["us-east-1a"]; 4]);
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "us-east-1b", &[t]),
                member_in_rack("m2", "us-east-1c", &[t]),
            ],
            &topics,
        );
        // 4 partitions / 2 members → 2 each, distributed evenly.
        assert!(a["m1"][&t].len() == 2);
        assert!(a["m2"][&t].len() == 2);
        // Union covers all 4 partitions exactly once.
        let mut all: Vec<i32> = a["m1"][&t]
            .iter()
            .chain(a["m2"][&t].iter())
            .copied()
            .collect();
        all.sort_unstable();
        assert!(all == vec![0, 1, 2, 3]);
    }

    #[test]
    fn rack_aware_balances_within_rack_pool() {
        // Three partitions all in us-east-1a, two members both in
        // us-east-1a. Same rack → both eligible for every partition,
        // balanced 2/1 (3/2 rounded).
        let t = tid(1);
        let topics = topics_with_racks(t, 3, vec![vec!["us-east-1a"]; 3]);
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "us-east-1a", &[t]),
                member_in_rack("m2", "us-east-1a", &[t]),
            ],
            &topics,
        );
        assert!(
            a["m1"][&t].len() + a["m2"][&t].len() == 3,
            "all partitions assigned"
        );
        assert!(
            a["m1"][&t].len().abs_diff(a["m2"][&t].len()) <= 1,
            "balanced within ±1: {:?} vs {:?}",
            a["m1"][&t],
            a["m2"][&t],
        );
    }

    #[test]
    fn rack_aware_handles_partition_with_no_rack_data() {
        // Partition 0 has rack info, partition 1 does NOT (omitted).
        // Partition 0 goes to rack-matched m1; partition 1 falls back
        // to all-subscribers and load-balances to whoever has fewer.
        let t = tid(1);
        let mut partition_racks = std::collections::HashMap::new();
        partition_racks.insert((t, 0), vec!["rack-a".into()]);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
            partition_racks,
        };
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "rack-a", &[t]),
                member_in_rack("m2", "rack-b", &[t]),
            ],
            &topics,
        );
        assert!(
            a == HashMap::from([
                ("m1".into(), HashMap::from([(t, vec![0])])),
                ("m2".into(), HashMap::from([(t, vec![1])])),
            ])
        );
    }

    #[test]
    fn rack_aware_empty_partition_racks_acts_like_non_rack_aware() {
        // partition_racks is empty → behavior must match the original
        // non-rack-aware path for backwards-compat with old test cases.
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
            partition_racks: std::collections::HashMap::new(),
        };
        let a = UniformAssignor.assign(
            &[
                member_in_rack("m1", "rack-a", &[t]),
                member_in_rack("m2", "rack-b", &[t]),
            ],
            &topics,
        );
        // Same as `two_members_split_round_robin` above.
        assert!(
            a == HashMap::from([
                ("m1".into(), HashMap::from([(t, vec![0, 2])])),
                ("m2".into(), HashMap::from([(t, vec![1, 3])])),
            ])
        );
    }

    #[test]
    fn rack_aware_no_subscriber_has_rack_id_acts_like_non_rack_aware() {
        // Partitions have rack info, but neither subscriber declares
        // rack_id. Eligible pool falls back to all subscribers.
        let t = tid(1);
        let topics = topics_with_racks(t, 4, vec![vec!["rack-a"]; 4]);
        let a = UniformAssignor.assign(&[member("m1", &[t]), member("m2", &[t])], &topics);
        assert!(
            a == HashMap::from([
                ("m1".into(), HashMap::from([(t, vec![0, 2])])),
                ("m2".into(), HashMap::from([(t, vec![1, 3])])),
            ])
        );
    }
}
