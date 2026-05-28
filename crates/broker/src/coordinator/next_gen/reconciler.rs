//! Trigger-driven reconciler. Runs at the next heartbeat after a dirty
//! signal: subscription change, member add/leave, metadata change, or
//! assignor selection change.

use std::collections::{HashMap, HashSet};

use crabka_protocol::primitives::uuid::Uuid;

use super::assignor::{Assignor, MemberSubscription, TopicMetadata};
use super::group_state::GroupState;

#[derive(Debug, Clone, Default)]
pub struct ReconcileInput {
    pub topic_id_by_name: HashMap<String, Uuid>,
    pub partitions_per_topic: HashMap<Uuid, i32>,
    /// Slice 64b: per-`(topic_id, partition_index)` set of replica racks.
    /// Empty / missing entries mean "no rack data for this partition" —
    /// `UniformAssignor` then falls back to its non-rack-aware path.
    pub partition_racks: HashMap<(Uuid, i32), Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileOutcome {
    NoChange,
    Recomputed,
}

pub fn reconcile_if_dirty(
    group: &mut GroupState,
    input: &ReconcileInput,
    assignor: &dyn Assignor,
) -> ReconcileOutcome {
    if !group.dirty {
        return ReconcileOutcome::NoChange;
    }
    let subscriptions: Vec<MemberSubscription> = group
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(
                &m.subscribed_topic_names,
                m.subscribed_topic_regex.as_deref(),
                &input.topic_id_by_name,
            ),
        })
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic.clone(),
        partition_racks: input.partition_racks.clone(),
    };
    let assignment = assignor.assign(&subscriptions, &topics);
    group.bump_epoch();
    group.install_target(assignment);
    group.dirty = false;
    ReconcileOutcome::Recomputed
}

/// Resolve a member's effective topic-id subscription. Unions exact-name
/// lookups (`subscribed_topic_names`) with regex matches against every
/// topic in the current metadata image. KIP-848 v1+: the regex is
/// supplied as a Java `Pattern`-syntax string — Crabka compiles it with
/// Rust's RE2-based `regex` crate, which differs only in extended
/// constructs (lookaround) that aren't expected in operator-facing
/// subscription patterns. Invalid patterns are silently dropped (logged
/// at warn) so a bad regex never poisons the rest of the assignment.
fn resolve_subscribed_topic_ids(
    names: &HashSet<String>,
    regex_pattern: Option<&str>,
    topic_id_by_name: &HashMap<String, Uuid>,
) -> Vec<Uuid> {
    let mut out: HashSet<Uuid> = names
        .iter()
        .filter_map(|n| topic_id_by_name.get(n).copied())
        .collect();
    if let Some(pat) = regex_pattern {
        match regex::Regex::new(pat) {
            Ok(re) => {
                for (name, id) in topic_id_by_name {
                    if re.is_match(name) {
                        out.insert(*id);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    pattern = %pat, error = %e,
                    "consumer-group: subscribed_topic_regex failed to compile; ignored"
                );
            }
        }
    }
    out.into_iter().collect()
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
        if let Some(pat) = &m.subscribed_topic_regex
            && let Ok(re) = regex::Regex::new(pat)
        {
            for (name, id) in &input.topic_id_by_name {
                if re.is_match(name) {
                    out.insert(*id);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::assignor::UniformAssignor;
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
            subscribed_topic_regex: None,
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
                ..Default::default()
            },
            t,
        )
    }

    #[test]
    fn dirty_triggers_recompute() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 4);
        let outcome = reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
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
            reconcile_if_dirty(&mut g, &inp, &UniformAssignor),
            ReconcileOutcome::NoChange
        );
    }

    #[test]
    fn idempotent_under_repeated_calls() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let epoch1 = g.group_epoch;
        let outcome = reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        assert_eq!(outcome, ReconcileOutcome::NoChange);
        assert_eq!(g.group_epoch, epoch1);
    }

    #[test]
    fn metadata_change_via_dirty_flag_recomputes() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp1, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp1, &UniformAssignor);
        let epoch_before = g.group_epoch;
        let (inp2, _) = input("t", 4);
        g.dirty = true;
        let outcome = reconcile_if_dirty(&mut g, &inp2, &UniformAssignor);
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

    // ── slice 64a-regex: subscribed_topic_regex resolution ───────────────────

    fn input_with_topics(topics: &[(&str, i32)]) -> ReconcileInput {
        let mut topic_id_by_name = HashMap::new();
        let mut partitions_per_topic = HashMap::new();
        for (i, (name, parts)) in topics.iter().enumerate() {
            let id = Uuid([u8::try_from(i + 1).unwrap_or(255); 16]);
            topic_id_by_name.insert((*name).to_string(), id);
            partitions_per_topic.insert(id, *parts);
        }
        ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            ..Default::default()
        }
    }

    fn member_with_regex(id: &str, names: &[&str], regex: Option<&str>) -> MemberState {
        let mut sub = HashSet::new();
        for n in names {
            sub.insert((*n).to_string());
        }
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: sub,
            subscribed_topic_regex: regex.map(String::from),
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

    #[test]
    fn regex_resolves_to_matching_topic_ids() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("^orders-.*")));
        let inp = input_with_topics(&[("orders-eu", 1), ("orders-us", 1), ("shipments", 1)]);
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        let orders_us = inp.topic_id_by_name["orders-us"];
        let shipments = inp.topic_id_by_name["shipments"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        assert!(assigned.contains(&orders_eu));
        assert!(assigned.contains(&orders_us));
        assert!(
            !assigned.contains(&shipments),
            "shipments shouldn't match `^orders-.*`"
        );
    }

    #[test]
    fn regex_unions_with_names() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &["audit"], Some("^orders-.*")));
        let inp = input_with_topics(&[("orders-eu", 1), ("audit", 1), ("shipments", 1)]);
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        let audit = inp.topic_id_by_name["audit"];
        let shipments = inp.topic_id_by_name["shipments"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        assert!(assigned.contains(&orders_eu), "regex match");
        assert!(assigned.contains(&audit), "explicit name");
        assert!(!assigned.contains(&shipments));
    }

    #[test]
    fn invalid_regex_is_silently_dropped_and_names_still_apply() {
        let mut g = GroupState::new("g");
        // `*` at the start is an invalid Rust regex (and invalid in JVM
        // too — `PatternSyntaxException`). We must not panic; just fall
        // back to the names-only subscription.
        g.add_or_update_member(member_with_regex("m1", &["audit"], Some("*invalid")));
        let inp = input_with_topics(&[("audit", 1), ("orders-eu", 1)]);
        let audit = inp.topic_id_by_name["audit"];
        let orders_eu = inp.topic_id_by_name["orders-eu"];
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        assert!(
            assigned.contains(&audit),
            "names-only subscription still applied"
        );
        assert!(
            !assigned.contains(&orders_eu),
            "invalid regex must not silently match every topic"
        );
    }

    #[test]
    fn empty_regex_matches_everything() {
        // Empty pattern is a valid regex that matches every string; this
        // covers the operator-intended "subscribe to all topics" case
        // without forcing the client to enumerate them.
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("")));
        let inp = input_with_topics(&[("a", 1), ("b", 1), ("c", 1)]);
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        let assigned: HashSet<Uuid> = g.target.per_member["m1"].keys().copied().collect();
        assert_eq!(
            assigned.len(),
            3,
            "empty regex matches every topic; got {assigned:?}"
        );
    }

    #[test]
    fn regex_change_marks_group_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("^a")));
        let inp = input_with_topics(&[("a1", 1), ("b1", 1)]);
        reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        assert!(!g.dirty, "fresh recompute clears dirty");
        let epoch_before = g.group_epoch;

        // Change the regex pattern → must dirty the group so the next
        // reconcile re-runs.
        g.add_or_update_member(member_with_regex("m1", &[], Some("^b")));
        assert!(g.dirty, "regex change must mark group dirty");
        let outcome = reconcile_if_dirty(&mut g, &inp, &UniformAssignor);
        assert_eq!(outcome, ReconcileOutcome::Recomputed);
        assert!(g.group_epoch > epoch_before);
    }

    #[test]
    fn membership_topic_ids_includes_regex_matches() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member_with_regex("m1", &[], Some("^orders-")));
        let inp = input_with_topics(&[("orders-eu", 1), ("shipments", 1)]);
        let orders = inp.topic_id_by_name["orders-eu"];
        let ids = membership_topic_ids(&g, &inp);
        assert!(
            ids.contains(&orders),
            "regex match flows into membership set"
        );
    }
}
