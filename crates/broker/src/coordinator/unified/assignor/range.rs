//! `RangeAssignor`: it assigns a contiguous partition range for each topic.
//!
//! It matches the classic `RangeAssignor` semantics, which co-partition across
//! topics that have equal partition counts.

use std::collections::HashMap;

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

#[derive(Debug)]
pub struct RangeAssignor;

impl Assignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }

    fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment {
        let mut out: Assignment = HashMap::new();
        for m in members {
            out.insert(m.member_id.clone(), HashMap::new());
        }
        let mut sorted_topics: Vec<_> = topics.partitions_per_topic.iter().collect();
        sorted_topics.sort_by_key(|(id, _)| id.0);
        for (topic_id, partition_count) in sorted_topics {
            let mut subscribers: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str())
                .collect();
            subscribers.sort_unstable();
            if subscribers.is_empty() {
                continue;
            }
            let n = i32::try_from(subscribers.len()).expect("member count fits i32");
            let p = *partition_count;
            let per_member = p / n;
            let remainder = p % n;
            let mut cursor = 0;
            for (i, sub) in subscribers.iter().enumerate() {
                let extra = i32::from(i32::try_from(i).expect("index fits i32") < remainder);
                let take = per_member + extra;
                if take == 0 {
                    continue;
                }
                let range: Vec<i32> = (cursor..cursor + take).collect();
                out.get_mut(*sub)
                    .expect("inserted above")
                    .insert(*topic_id, range);
                cursor += take;
            }
        }
        out
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

    #[test]
    fn contiguous_ranges() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
            ..Default::default()
        };
        let a = RangeAssignor.assign(&[member("m1", &[t]), member("m2", &[t])], &topics);
        assert!(a["m1"][&t] == vec![0, 1, 2]);
        assert!(a["m2"][&t] == vec![3, 4, 5]);
    }

    #[test]
    fn non_divisible_extra_goes_to_first_members() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 7)].into(),
            ..Default::default()
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        for (m, want) in [
            ("m1", vec![0, 1, 2]),
            ("m2", vec![3, 4]),
            ("m3", vec![5, 6]),
        ] {
            assert!(a[m][&t] == want);
        }
    }

    #[test]
    fn co_partitioning_two_topics_equal_size() {
        let t1 = tid(1);
        let t2 = tid(2);
        let topics = TopicMetadata {
            partitions_per_topic: [(t1, 4), (t2, 4)].into(),
            ..Default::default()
        };
        let a = RangeAssignor.assign(&[member("m1", &[t1, t2]), member("m2", &[t1, t2])], &topics);
        for (m, topic, want) in [
            ("m1", t1, vec![0, 1]),
            ("m1", t2, vec![0, 1]),
            ("m2", t1, vec![2, 3]),
            ("m2", t2, vec![2, 3]),
        ] {
            assert!(a[m][&topic] == want);
        }
    }

    #[test]
    fn fewer_partitions_than_members() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
            ..Default::default()
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        for (m, want) in [("m1", vec![0]), ("m2", vec![1])] {
            assert!(a[m][&t] == want);
        }
        assert!(!a["m3"].contains_key(&t) || a["m3"][&t].is_empty());
    }

    #[test]
    fn unsubscribed_skipped() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
            ..Default::default()
        };
        let a = RangeAssignor.assign(&[member("m1", &[t]), member("m2", &[])], &topics);
        assert!(a["m1"][&t] == vec![0, 1, 2, 3]);
    }

    #[test]
    fn deterministic_under_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
            ..Default::default()
        };
        let a1 = RangeAssignor.assign(&[member("m1", &[t]), member("m2", &[t])], &topics);
        let a2 = RangeAssignor.assign(&[member("m2", &[t]), member("m1", &[t])], &topics);
        assert!(a1 == a2);
    }
}
