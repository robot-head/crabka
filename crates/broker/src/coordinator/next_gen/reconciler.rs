//! Trigger-driven reconciler. Runs at the next heartbeat after a dirty
//! signal: subscription change, member add/leave, metadata change, or
//! assignor selection change.

use std::collections::{HashMap, HashSet};

use crabka_protocol::primitives::uuid::Uuid;

use super::assignor::{self, MemberSubscription, TopicMetadata};
use super::group_state::GroupState;

#[derive(Debug, Clone, Default)]
pub struct ReconcileInput {
    pub topic_id_by_name: HashMap<String, Uuid>,
    pub partitions_per_topic: HashMap<Uuid, i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileOutcome {
    NoChange,
    Recomputed,
}

pub fn reconcile_if_dirty(
    group: &mut GroupState,
    input: &ReconcileInput,
    assignor_name: &str,
) -> ReconcileOutcome {
    if !group.dirty {
        return ReconcileOutcome::NoChange;
    }
    let Some(impl_) = assignor::select(assignor_name) else {
        return ReconcileOutcome::NoChange;
    };
    let subscriptions: Vec<MemberSubscription> = group
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: m
                .subscribed_topic_names
                .iter()
                .filter_map(|n| input.topic_id_by_name.get(n).copied())
                .collect(),
        })
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic.clone(),
    };
    let assignment = impl_.assign(&subscriptions, &topics);
    group.bump_epoch();
    group.install_target(assignment);
    group.dirty = false;
    ReconcileOutcome::Recomputed
}

#[must_use]
pub fn membership_topic_ids(group: &GroupState, input: &ReconcileInput) -> HashSet<Uuid> {
    let mut out = HashSet::new();
    for m in group.members.values() {
        for name in &m.subscribed_topic_names {
            if let Some(id) = input.topic_id_by_name.get(name) {
                out.insert(*id);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::next_gen::group_state::MemberState;
    use crate::coordinator::next_gen::persistence::MemberAssignmentState;
    use std::time::{Duration, Instant};

    fn fresh_member(id: &str, topic: &str) -> MemberState {
        let mut sub = HashSet::new();
        sub.insert(topic.into());
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: sub,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
        }
    }

    fn input(topic_name: &str, partitions: i32) -> (ReconcileInput, Uuid) {
        let t = Uuid([1; 16]);
        (
            ReconcileInput {
                topic_id_by_name: [(topic_name.into(), t)].into(),
                partitions_per_topic: [(t, partitions)].into(),
            },
            t,
        )
    }

    #[test]
    fn dirty_triggers_recompute() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 4);
        let outcome = reconcile_if_dirty(&mut g, &inp, "uniform");
        assert_eq!(outcome, ReconcileOutcome::Recomputed);
        assert_eq!(g.target.per_member["m1"][&t], vec![0, 1, 2, 3]);
        assert!(!g.dirty);
    }

    #[test]
    fn clean_is_no_op() {
        let mut g = GroupState::new("g");
        g.dirty = false;
        let (inp, _) = input("t", 4);
        assert_eq!(
            reconcile_if_dirty(&mut g, &inp, "uniform"),
            ReconcileOutcome::NoChange
        );
    }

    #[test]
    fn unknown_assignor_is_no_op() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, _) = input("t", 4);
        assert_eq!(
            reconcile_if_dirty(&mut g, &inp, "doesnotexist"),
            ReconcileOutcome::NoChange
        );
        assert!(g.dirty, "unknown assignor must leave dirty bit set");
    }

    #[test]
    fn idempotent_under_repeated_calls() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp, "uniform");
        let epoch1 = g.group_epoch;
        let outcome = reconcile_if_dirty(&mut g, &inp, "uniform");
        assert_eq!(outcome, ReconcileOutcome::NoChange);
        assert_eq!(g.group_epoch, epoch1);
    }

    #[test]
    fn metadata_change_via_dirty_flag_recomputes() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp1, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp1, "uniform");
        let epoch_before = g.group_epoch;
        let (inp2, _) = input("t", 4);
        g.dirty = true;
        let outcome = reconcile_if_dirty(&mut g, &inp2, "uniform");
        assert_eq!(outcome, ReconcileOutcome::Recomputed);
        assert!(g.group_epoch > epoch_before);
    }

    #[test]
    fn subscription_topic_ids_resolved() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 2);
        let ids = membership_topic_ids(&g, &inp);
        assert!(ids.contains(&t));
    }
}
