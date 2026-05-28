//! `UniformAssignor` — KIP-848's default. Distributes partitions as evenly
//! as possible across members subscribed to each topic. Deterministic.

use std::collections::HashMap;

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

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
            for p in 0..*partition_count {
                let idx =
                    usize::try_from(p).expect("partition index non-negative") % subscribers.len();
                let mid = subscribers[idx].to_string();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_protocol::primitives::uuid::Uuid;

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

    #[test]
    fn single_member_gets_all_partitions() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert_eq!(a["m1"][&t], vec![0, 1, 2, 3]);
    }

    #[test]
    fn two_members_split_round_robin() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = UniformAssignor.assign(&[member("m1", &[t]), member("m2", &[t])], &topics);
        assert_eq!(a["m1"][&t], vec![0, 2]);
        assert_eq!(a["m2"][&t], vec![1, 3]);
    }

    #[test]
    fn unsubscribed_member_gets_empty_for_topic() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
        };
        let a = UniformAssignor.assign(&[member("m1", &[t]), member("m2", &[])], &topics);
        assert_eq!(a["m1"][&t], vec![0, 1]);
        assert!(!a["m2"].contains_key(&t) || a["m2"][&t].is_empty());
    }

    #[test]
    fn zero_partitions_no_assignment() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 0)].into(),
        };
        let a = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert!(!a["m1"].contains_key(&t) || a["m1"][&t].is_empty());
    }

    #[test]
    fn deterministic_under_member_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
        };
        let a1 = UniformAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        let a2 = UniformAssignor.assign(
            &[member("m3", &[t]), member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        assert_eq!(a1, a2);
    }

    #[test]
    fn empty_members_no_panic() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = UniformAssignor.assign(&[], &topics);
        assert!(a.is_empty());
    }
}
