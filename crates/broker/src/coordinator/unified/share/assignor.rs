//! KIP-932 share-group assignor.
//!
//! Reuses the consumer next-gen assignor types
//! ([`MemberSubscription`], [`TopicMetadata`], [`Assignment`]). Unlike the
//! consumer assignors, share-group assignment is **non-exclusive**: when there
//! are fewer partitions than members, members are allowed to share partitions
//! so that every member still receives at least one.

use std::collections::HashMap;

use crate::coordinator::unified::assignor::{Assignment, MemberSubscription, TopicMetadata};

/// KIP-932 `SimpleAssignor`: round-robin distribution that permits overlap.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShareGroupAssignor;

impl ShareGroupAssignor {
    #[must_use]
    pub fn name(&self) -> &'static str {
        "simple"
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn assign(&self, members: &[MemberSubscription], topics: &TopicMetadata) -> Assignment {
        let mut out: Assignment = members
            .iter()
            .map(|m| (m.member_id.clone(), HashMap::new()))
            .collect();
        if members.is_empty() {
            return out;
        }
        for (topic_id, &num_parts) in &topics.partitions_per_topic {
            // Subscribers to this topic, in a stable order so the assignment is
            // deterministic across reconciles.
            let mut subs: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str())
                .collect();
            subs.sort_unstable();
            if subs.is_empty() || num_parts <= 0 {
                continue;
            }
            let sub_count = i32::try_from(subs.len()).expect("member count fits i32");
            if num_parts >= sub_count {
                // Enough partitions for everyone: round-robin, no overlap.
                for p in 0..num_parts {
                    let idx = usize::try_from(p % sub_count).expect("non-negative");
                    let m = subs[idx];
                    out.get_mut(m)
                        .unwrap()
                        .entry(*topic_id)
                        .or_default()
                        .push(p);
                }
            } else {
                // Fewer partitions than members: every member gets at least one
                // partition (overlap), handed out round-robin from the set.
                for (i, m) in subs.iter().enumerate() {
                    let p = i32::try_from(i).expect("index fits i32") % num_parts;
                    out.get_mut(*m)
                        .unwrap()
                        .entry(*topic_id)
                        .or_default()
                        .push(p);
                }
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
    use crate::coordinator::unified::assignor::{MemberSubscription, TopicMetadata};

    fn topic(p: i32) -> (Uuid, TopicMetadata) {
        let id = Uuid([1; 16]);
        let mut t = TopicMetadata::default();
        t.partitions_per_topic.insert(id, p);
        (id, t)
    }

    #[test]
    fn distributes_partitions_across_members() {
        let (id, topics) = topic(4);
        let members = vec![
            MemberSubscription {
                member_id: "m1".into(),
                rack_id: None,
                subscribed_topic_ids: vec![id],
            },
            MemberSubscription {
                member_id: "m2".into(),
                rack_id: None,
                subscribed_topic_ids: vec![id],
            },
        ];
        let a = ShareGroupAssignor.assign(&members, &topics);
        let total: usize = a.values().flat_map(|m| m.values()).map(Vec::len).sum();
        assert!(total == 4); // all 4 partitions assigned
        assert!(a["m1"][&id].len() == 2 && a["m2"][&id].len() == 2);
    }

    #[test]
    fn more_members_than_partitions_overlap() {
        let (id, topics) = topic(1);
        let members: Vec<_> = (0..3)
            .map(|i| MemberSubscription {
                member_id: format!("m{i}"),
                rack_id: None,
                subscribed_topic_ids: vec![id],
            })
            .collect();
        let a = ShareGroupAssignor.assign(&members, &topics);
        // Every member gets the single partition (share semantics permit overlap).
        for i in 0..3 {
            assert!(a[&format!("m{i}")][&id] == vec![0]);
        }
    }
}
