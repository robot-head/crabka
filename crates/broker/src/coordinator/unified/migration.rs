//! Classic ↔ next-gen consumer-group conversion predicates (KIP-848).
//!
//! This module owns the [`super::config::ConsumerGroupMigrationPolicy`], the
//! convertibility predicates, and the state translation helpers used by live
//! migration.

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use bytes::{BufMut, Bytes, BytesMut};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition},
        consumer_protocol_subscription::ConsumerProtocolSubscription,
    },
    primitives::uuid::Uuid,
};

use super::{
    classic_state::{Group as ClassicState, Member as ClassicMember, select_protocol},
    consumer_state::{ClassicMemberFacade, GroupState as ConsumerState, MemberState},
    persistence_next_gen::MemberAssignmentState,
    reconciler::ReconcileInput,
};

/// Decode a classic member's `protocol_metadata` blob as a
/// `ConsumerProtocolSubscription`. The blob carries a leading `i16` version
/// (the "consumer" embedded-protocol version negotiation, separate from the
/// `ConsumerProtocolSubscription` schema's per-field version gates) followed by
/// the schema body. Returns `None` on any decode error or unknown version —
/// such a member's subscription cannot survive translation to the server-side
/// consumer model. Mirrors `offset_delete::decode_subscribed_topics`.
pub(crate) fn decode_consumer_subscription(
    metadata: &[u8],
) -> Option<ConsumerProtocolSubscription> {
    use bytes::Buf;
    if metadata.len() < 2 {
        return None;
    }
    let mut cur = metadata;
    let version = cur.get_i16();
    if !(0..=3).contains(&version) {
        return None;
    }
    ConsumerProtocolSubscription::decode(&mut cur, version).ok()
}

/// Can this classic group be upgraded to a next-gen consumer group?
///
/// Mirrors Apache Kafka's `ConsumerGroup.fromClassicGroup` admission rule: the
/// group must use the `"consumer"` protocol type and **every** current member's
/// selected `protocol_metadata` must decode as a valid
/// `ConsumerProtocolSubscription`, so each subscription survives translation. An
/// empty group is trivially convertible.
pub(crate) fn classic_is_convertible(state: &ClassicState) -> bool {
    if state.protocol_type.as_deref() != Some("consumer") {
        return false;
    }
    state
        .members
        .values()
        .all(|m| decode_consumer_subscription(&m.protocol_metadata).is_some())
}

/// Can this consumer group be downgraded to a classic group? Always `true` in
/// Kafka — a server-managed consumer group can always be re-expressed as a
/// classic group (members become classic members, the server target becomes the
/// seed assignment). Provided for symmetry with the upgrade path.
pub(crate) fn consumer_is_convertible() -> bool {
    true
}

/// Convert a classic group into a consumer group that **hosts its classic
/// members** during KIP-848 upgrade. Each classic member becomes a
/// [`MemberState`] carrying a [`ClassicMemberFacade`]; its subscription is
/// decoded from its `ConsumerProtocolSubscription` metadata (topic names — the
/// reconciler resolves them to topic-IDs against the metadata image). The
/// group is marked dirty so the next reconcile computes the unified target.
///
/// Precondition: the caller has checked [`classic_is_convertible`]. Committed
/// offsets live on the kind-agnostic `Group` container and are untouched here.
pub(crate) fn convert_classic_to_consumer(classic: &ClassicState) -> ConsumerState {
    let mut state = ConsumerState::new(classic.group_id.clone());
    // Seed the group epoch from the classic generation so epochs stay
    // monotonic across the flip; the first reconcile bumps it.
    state.group_epoch = classic.generation_id.max(0);
    for m in classic.members.values() {
        let names: HashSet<String> = decode_consumer_subscription(&m.protocol_metadata)
            .map(|s| s.topics.into_iter().collect())
            .unwrap_or_default();
        let facade = ClassicMemberFacade {
            generation_id: classic.generation_id,
            supported_protocols: m.protocols.clone(),
            session_timeout: m.session_timeout,
            last_synced_assignment: m.assignment.clone().unwrap_or_default(),
            awaiting_sync: true,
        };
        state.add_or_update_member(MemberState {
            member_id: m.member_id.clone(),
            instance_id: m.group_instance_id.clone(),
            rack_id: None,
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            subscribed_topic_names: names,
            subscribed_topic_regex: None,
            compiled_regex: None,
            server_assignor: None,
            rebalance_timeout: m.rebalance_timeout,
            member_epoch: state.group_epoch,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
            classic: Some(facade),
        });
    }
    state.dirty = true;
    state
}

/// The atomic record batch for an upgrade: tombstone the classic k2
/// `GroupMetadata` and write the full next-gen record set for the converted
/// group. The two go into one batch so the flip is all-or-nothing.
pub(crate) fn upgrade_pending_records(state: &ConsumerState) -> super::actor::PendingRecords {
    let mut pending = super::actor::full_pending_records(state);
    pending.classic_group_metadata_tombstone = true;
    pending
}

/// Convert a consumer group back into a classic group during KIP-848 downgrade.
/// Every member is re-expressed as a classic [`ClassicMember`] restored from its
/// [`ClassicMemberFacade`]; its assignment seed is the server-computed target
/// translated to a `ConsumerProtocolAssignment` blob, so the member keeps its
/// partitions across the flip with no spurious revoke. Committed offsets live on
/// the kind-agnostic `Group` container and are untouched here.
///
/// Precondition: every member is a hosted classic member (`classic.is_some()`),
/// which holds once the last native consumer-protocol member has departed.
pub(crate) fn convert_consumer_to_classic(
    state: &ConsumerState,
    image: &ReconcileInput,
) -> ClassicState {
    let mut classic = ClassicState::new(state.group_id.clone());
    classic.protocol_type = Some("consumer".into());
    for (mid, m) in &state.members {
        let facade = m
            .classic
            .as_ref()
            .expect("downgrade precondition: all members are hosted classic members");
        // Seed from the server-computed TARGET, not `assigned_partitions`: a
        // hosted classic member's `assigned_partitions` only fills in as a
        // NATIVE consumer acks epochs over heartbeats, which a hosted classic
        // member never does. Its real partitions live in `target.per_member`,
        // so reading the target keeps them across the downgrade.
        let seed = member_target_assignment(state, mid, image);
        let mut cm = ClassicMember::new(
            mid.clone(),
            m.client_id.clone(),
            m.client_host.clone(),
            facade.session_timeout,
            m.rebalance_timeout,
            facade.supported_protocols.clone(),
        )
        .with_instance_id(m.instance_id.clone());
        cm.assignment = Some(seed);
        classic.add_member(cm);
    }
    if let Some(name) = select_protocol(&classic.members) {
        classic.complete_rebalance(&name);
        // Drive to Stable so a downgraded member's first Heartbeat/SyncGroup
        // reads its seed assignment instead of REBALANCE_IN_PROGRESS.
        let assignments: std::collections::HashMap<String, bytes::Bytes> = classic
            .members
            .iter()
            .filter_map(|(id, m)| m.assignment.clone().map(|a| (id.clone(), a)))
            .collect();
        classic.install_assignments(assignments);
    }
    // Set generation_id LAST so neither complete_rebalance (+1) nor
    // install_assignments overrides the consumer group's epoch.
    classic.generation_id = state.group_epoch.max(0);
    classic
}

/// The atomic record batch for a downgrade: tombstone the consumer group's
/// group-level next-gen k3 `GroupMetadata` and k6 `TargetAssignmentMetadata`,
/// every member's k5/k7/k8, and write the classic k2 `GroupMetadata` for the
/// re-expressed classic group. All in one batch so the flip is all-or-nothing
/// (and bootstrap replay sees a clean next-gen-drop → classic-write sequence;
/// log order wins).
///
/// The k6 tombstone is load-bearing under log compaction: `__consumer_offsets`
/// is compacted, so without it a surviving post-upgrade k6 write (group-level,
/// never per-member-tombstoned) would outlive the GC'd k3 and re-create a
/// next-gen seed on replay, resurrecting the downgraded group as next-gen.
pub(crate) fn downgrade_pending_records(
    consumer: &ConsumerState,
    classic: &ClassicState,
) -> super::actor::PendingRecords {
    let mut pending = super::actor::PendingRecords {
        next_gen_group_metadata_tombstone: true,
        next_gen_target_metadata_tombstone: true,
        classic_group_metadata: Some(super::actor::classic_group_metadata_record(classic)),
        ..Default::default()
    };
    for mid in consumer.members.keys() {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
    pending
}

/// Translate a member's server-side target (topic-ID → partitions) into a
/// classic `ConsumerProtocolAssignment` wire blob (topic-name → partitions),
/// with the leading `i16` version prefix a classic client expects in the
/// `SyncGroup` assignment field. Topic IDs absent from the metadata image are
/// dropped (the topic was deleted). Deterministic order (by topic name).
pub(crate) fn target_to_consumer_assignment(
    target: &HashMap<Uuid, Vec<i32>>,
    image: &ReconcileInput,
) -> Bytes {
    let id_to_name: HashMap<Uuid, &str> = image
        .topic_id_by_name
        .iter()
        .map(|(name, id)| (*id, name.as_str()))
        .collect();
    let mut assigned: Vec<TopicPartition> = target
        .iter()
        .filter_map(|(tid, parts)| {
            id_to_name.get(tid).map(|name| {
                let mut p = parts.clone();
                p.sort_unstable();
                TopicPartition {
                    topic: (*name).to_string(),
                    partitions: p,
                    ..Default::default()
                }
            })
        })
        .collect();
    assigned.sort_by(|a, b| a.topic.cmp(&b.topic));
    let assignment = ConsumerProtocolAssignment {
        assigned_partitions: assigned,
        ..Default::default()
    };
    let mut out = BytesMut::new();
    out.put_i16(0); // "consumer" embedded-protocol version-negotiation prefix
    assignment
        .encode(&mut out, 0)
        .expect("ConsumerProtocolAssignment encode is infallible into BytesMut");
    out.freeze()
}

// ───────────────────────────────────────────────────────────────────────────
// Serving hosted classic members from the consumer-group reconciler.
//
// A classic member hosted in an upgraded consumer group keeps speaking the
// classic Heartbeat/JoinGroup/SyncGroup RPCs. We map those onto the next-gen
// machinery: the member's server-side target (translated to a
// `ConsumerProtocolAssignment` blob) is the assignment it should hold. The
// "does this member owe a re-sync?" signal is whether that translated target
// differs from `facade.last_synced_assignment` — purely derived from the
// reconciler's output, so it needs no separate epoch bookkeeping.
// ───────────────────────────────────────────────────────────────────────────

use super::actor::{JoinResult, JoinResultMember, SyncResult};
use crate::codes;

/// Classic `Heartbeat` for a hosted member: refresh liveness and signal a
/// re-sync while the member's current target differs from what it last synced.
/// `REBALANCE_IN_PROGRESS` tells a classic client to re-`JoinGroup`/`SyncGroup`
/// to pick up the changed assignment; `NONE` once it is in sync.
pub(crate) fn serve_classic_heartbeat(
    state: &mut ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> i16 {
    let Some(m) = state.members.get(member_id) else {
        return codes::UNKNOWN_MEMBER_ID;
    };
    let current = member_target_assignment(state, member_id, image);
    let owes = m
        .classic
        .as_ref()
        .is_none_or(|c| c.last_synced_assignment != current);
    if let Some(m) = state.members.get_mut(member_id) {
        m.last_seen = Instant::now();
    }
    if owes {
        codes::REBALANCE_IN_PROGRESS
    } else {
        codes::NONE
    }
}

/// Translate a member's server-side TARGET (the source of truth for what it
/// should own, mirroring the native heartbeat response) into a
/// `ConsumerProtocolAssignment` blob. In the next-gen model a member's
/// `assigned_partitions` only fills in as the client acks the target; a hosted
/// classic member has no such ack loop, so the target is what it must sync.
fn member_target_assignment(
    state: &ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> Bytes {
    let target = state
        .target
        .per_member
        .get(member_id)
        .cloned()
        .unwrap_or_default();
    target_to_consumer_assignment(&target, image)
}

/// Classic `SyncGroup` for a hosted member: return its current target
/// translated to a `ConsumerProtocolAssignment` blob and record it as
/// `last_synced_assignment` so subsequent heartbeats report `NONE`.
pub(crate) fn serve_classic_sync(
    state: &mut ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> SyncResult {
    if !state.members.contains_key(member_id) {
        return SyncResult {
            error_code: codes::UNKNOWN_MEMBER_ID,
            ..Default::default()
        };
    }
    let blob = member_target_assignment(state, member_id, image);
    if let Some(m) = state.members.get_mut(member_id)
        && let Some(c) = m.classic.as_mut()
    {
        c.last_synced_assignment = blob.clone();
        c.awaiting_sync = false;
    }
    SyncResult {
        error_code: codes::NONE,
        assignment: blob,
        protocol_type: Some("consumer".into()),
        protocol_name: None,
    }
}

/// Upsert a hosted classic member from a classic `JoinGroup` into the consumer
/// group. A rejoin of an existing member refreshes its facade/subscription and
/// preserves its `assigned_partitions` / `last_synced_assignment`; a brand-new
/// member is added with a fresh facade (`awaiting_sync = true`).
///
/// `add_or_update_member` marks the group dirty iff the subscription is new or
/// changed, so the caller can reconcile + persist only when needed.
#[allow(clippy::too_many_arguments)]
pub(crate) fn upsert_classic_member(
    state: &mut ConsumerState,
    member_id: &str,
    subscription_topics: HashSet<String>,
    protocols: Vec<(String, Bytes)>,
    client_id: String,
    client_host: String,
    session_timeout: std::time::Duration,
    rebalance_timeout: std::time::Duration,
    instance_id: Option<String>,
) {
    // Preserve a rejoining member's existing assignment + last-synced blob so a
    // rejoin with an unchanged subscription is a pure no-op (no spurious revoke
    // and no re-sync signal). A new member starts fresh, awaiting its first sync.
    let existing = state.members.get(member_id);
    let assigned_partitions = existing
        .map(|m| m.assigned_partitions.clone())
        .unwrap_or_default();
    let partitions_pending_revocation = existing
        .map(|m| m.partitions_pending_revocation.clone())
        .unwrap_or_default();
    let last_synced_assignment = existing
        .and_then(|m| m.classic.as_ref())
        .map(|c| c.last_synced_assignment.clone())
        .unwrap_or_default();
    let member_epoch = existing.map_or(state.group_epoch, |m| m.member_epoch);
    let previous_member_epoch = existing.map_or(0, |m| m.previous_member_epoch);
    let assignment_state = existing.map_or(MemberAssignmentState::Stable, |m| m.assignment_state);

    let facade = ClassicMemberFacade {
        generation_id: state.group_epoch,
        supported_protocols: protocols,
        session_timeout,
        last_synced_assignment,
        awaiting_sync: existing.is_none(),
    };
    state.add_or_update_member(MemberState {
        member_id: member_id.to_string(),
        instance_id,
        rack_id: None,
        client_id,
        client_host,
        subscribed_topic_names: subscription_topics,
        subscribed_topic_regex: None,
        compiled_regex: None,
        server_assignor: None,
        rebalance_timeout,
        member_epoch,
        previous_member_epoch,
        assignment_state,
        assigned_partitions,
        partitions_pending_revocation,
        last_seen: Instant::now(),
        classic: Some(facade),
    });
}

/// Build the `JoinGroup` result for a hosted classic member. The group is
/// server-assigned, so the member is its own leader of a single-member view at
/// `generation = group_epoch`; the real assignment arrives on the next
/// `SyncGroup`.
pub(crate) fn build_hosted_classic_join_result(
    state: &ConsumerState,
    member_id: &str,
    protocol_name: Option<String>,
) -> JoinResult {
    JoinResult {
        error_code: codes::NONE,
        generation_id: state.group_epoch,
        protocol_type: Some("consumer".into()),
        protocol_name,
        leader: member_id.to_string(),
        member_id: member_id.to_string(),
        members: vec![JoinResultMember {
            member_id: member_id.to_string(),
            group_instance_id: state
                .members
                .get(member_id)
                .and_then(|m| m.instance_id.clone()),
            metadata: Bytes::new(),
        }],
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use assert2::{assert, check};
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use crabka_protocol::{Encode, primitives::uuid::Uuid};

    use super::{
        super::classic_state::{Group, Member},
        *,
    };

    /// Encode a `ConsumerProtocolSubscription` with the leading version prefix,
    /// as a real classic consumer client sends in its `JoinGroup` protocol
    /// metadata.
    fn subscription_blob(topics: &[&str]) -> Bytes {
        let sub = ConsumerProtocolSubscription {
            topics: topics.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        let mut out = BytesMut::new();
        out.put_i16(0); // protocol version-negotiation prefix
        sub.encode(&mut out, 0).unwrap();
        out.freeze()
    }

    fn consumer_member(id: &str, metadata: Bytes) -> Member {
        let mut m = Member::new(
            id,
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), metadata.clone())],
        );
        m.protocol_metadata = metadata;
        m
    }

    #[derive(Debug, PartialEq, Eq)]
    struct FacadeProjection {
        generation_id: i32,
        awaiting_sync: bool,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ConversionProjection<'a> {
        group_id: &'a str,
        group_epoch: i32,
        member_count: usize,
        m1_is_classic: bool,
        m1_subscribed_topic_names: HashSet<String>,
        m1_facade: FacadeProjection,
        m2_subscribed_topic_names: HashSet<String>,
        dirty: bool,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct DowngradeProjection<'a> {
        group_id: &'a str,
        generation_id: i32,
        member_instance_id: Option<&'a str>,
        member_session_timeout: Duration,
        assigned_partitions:
            Vec<crabka_protocol::owned::consumer_protocol_assignment::TopicPartition>,
        state: super::super::classic_state::GroupState,
        seed_assignment_preserved: bool,
        protocol_name: Option<&'a str>,
        leader_id: Option<&'a str>,
    }

    #[test]
    fn empty_consumer_group_is_convertible() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        assert!(classic_is_convertible(&g));
    }

    #[test]
    fn non_consumer_protocol_type_is_not_convertible() {
        let mut g = Group::new("g");
        g.protocol_type = Some("connect".into());
        assert!(!classic_is_convertible(&g));
        // None protocol_type (never joined) is also not convertible.
        let g2 = Group::new("g2");
        assert!(!classic_is_convertible(&g2));
    }

    #[test]
    fn group_of_valid_consumer_members_is_convertible() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));
        assert!(classic_is_convertible(&g));
    }

    #[test]
    fn member_with_undecodable_metadata_blocks_conversion() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        g.add_member(consumer_member("ok", subscription_blob(&["t1"])));
        // Garbage metadata that is not a ConsumerProtocolSubscription.
        g.add_member(consumer_member(
            "bad",
            Bytes::from_static(&[0xff, 0xff, 0x01]),
        ));
        assert!(!classic_is_convertible(&g));
    }

    #[test]
    fn decode_rejects_short_and_bad_version() {
        // Too short to hold a version, or (version 99) out of the supported
        // 0..=3 range.
        for (case, input) in [
            ("empty input", &[][..]),
            ("truncated version", &[0][..]),
            ("unsupported version", &[0, 99][..]),
        ] {
            assert!(decode_consumer_subscription(input).is_none(), "case {case}");
        }
    }

    #[test]
    fn consumer_group_always_downgradable() {
        assert!(consumer_is_convertible());
    }

    #[test]
    fn convert_preserves_members_subscriptions_and_facade() {
        let mut g = Group::new("g");
        g.protocol_type = Some("consumer".into());
        g.generation_id = 3;
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));

        let state = convert_classic_to_consumer(&g);
        let m1 = &state.members["m1"];
        let facade = m1.classic.as_ref().unwrap();
        let m2 = &state.members["m2"];
        assert!(
            ConversionProjection {
                group_id: &state.group_id,
                group_epoch: state.group_epoch,
                member_count: state.members.len(),
                m1_is_classic: m1.is_classic(),
                m1_subscribed_topic_names: m1.subscribed_topic_names.clone(),
                m1_facade: FacadeProjection {
                    generation_id: facade.generation_id,
                    awaiting_sync: facade.awaiting_sync,
                },
                m2_subscribed_topic_names: m2.subscribed_topic_names.clone(),
                dirty: state.dirty,
            } == ConversionProjection {
                group_id: "g",
                group_epoch: 3,
                member_count: 2,
                m1_is_classic: true,
                m1_subscribed_topic_names: HashSet::from(["t1".to_string()]),
                m1_facade: FacadeProjection {
                    generation_id: 3,
                    awaiting_sync: true,
                },
                m2_subscribed_topic_names: HashSet::from(["t1".to_string(), "t2".to_string(),]),
                dirty: true,
            }
        );
    }

    #[test]
    fn target_translates_to_consumer_assignment_blob() {
        let t1 = Uuid([1; 16]);
        let t2 = Uuid([2; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), t1), ("events".to_string(), t2)].into(),
            ..Default::default()
        };
        let target: std::collections::HashMap<Uuid, Vec<i32>> =
            [(t1, vec![2, 0, 1]), (t2, vec![5])].into();

        let blob = target_to_consumer_assignment(&target, &image);
        // Strip the version prefix and decode back.
        let mut cur = &blob[..];
        let version = cur.get_i16();
        assert!(version == 0);
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, version).unwrap();
        assert!(
            decoded.assigned_partitions
                == vec![
                    crabka_protocol::owned::consumer_protocol_assignment::TopicPartition {
                        topic: "events".to_string(),
                        partitions: vec![5],
                        ..Default::default()
                    },
                    crabka_protocol::owned::consumer_protocol_assignment::TopicPartition {
                        topic: "orders".to_string(),
                        partitions: vec![0, 1, 2],
                        ..Default::default()
                    },
                ]
        );
    }

    #[test]
    fn target_drops_unknown_topic_ids() {
        let known = Uuid([1; 16]);
        let ghost = Uuid([9; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), known)].into(),
            ..Default::default()
        };
        let target: std::collections::HashMap<Uuid, Vec<i32>> =
            [(known, vec![0]), (ghost, vec![0])].into();
        let blob = target_to_consumer_assignment(&target, &image);
        let mut cur = &blob[..];
        let _ = cur.get_i16();
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, 0).unwrap();
        assert!(
            decoded.assigned_partitions
                == vec![
                    crabka_protocol::owned::consumer_protocol_assignment::TopicPartition {
                        topic: "orders".to_string(),
                        partitions: vec![0],
                        ..Default::default()
                    }
                ]
        );
    }

    #[test]
    fn downgrade_re_expresses_members_as_classic() {
        use std::time::{Duration, Instant};

        use crate::coordinator::unified::{
            classic_state::GroupState as ClassicGroupState,
            consumer_state::{ClassicMemberFacade, GroupState, MemberState},
            persistence_next_gen::MemberAssignmentState,
        };

        let t1 = Uuid([1; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), t1)].into(),
            ..Default::default()
        };
        let mut state = GroupState::new("g");
        state.group_epoch = 7;
        let m = MemberState {
            member_id: "m1".into(),
            instance_id: Some("inst-a".into()),
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: ["orders".to_string()].into(),
            subscribed_topic_regex: None,
            compiled_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 7,
            previous_member_epoch: 6,
            assignment_state: MemberAssignmentState::Stable,
            // A hosted classic member never acks epochs, so its
            // `assigned_partitions` stays EMPTY — its real partitions live in
            // the group's target (set below). The downgrade must seed from the
            // target, not from this empty map.
            assigned_partitions: std::collections::HashMap::new(),
            partitions_pending_revocation: std::collections::HashMap::new(),
            last_seen: Instant::now(),
            classic: Some(ClassicMemberFacade {
                generation_id: 7,
                supported_protocols: vec![("range".into(), bytes::Bytes::from_static(b"meta"))],
                session_timeout: Duration::from_secs(30),
                last_synced_assignment: bytes::Bytes::new(),
                awaiting_sync: false,
            }),
        };
        state.add_or_update_member(m);
        // The member's real partitions are in the server target, not in
        // `assigned_partitions`.
        state.target.epoch = 7;
        state
            .target
            .per_member
            .insert("m1".into(), [(t1, vec![0, 1])].into());

        let classic = convert_consumer_to_classic(&state, &image);
        let member = classic.members.get("m1").expect("member preserved");
        let asn = member.assignment.clone().expect("seed assignment");
        let mut cur = &asn[..];
        let version = cur.get_i16();
        assert!(version == 0);
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, 0).unwrap();
        let asn2 = member
            .assignment
            .clone()
            .expect("seed assignment still set after stabilize");
        check!(
            DowngradeProjection {
                group_id: &classic.group_id,
                generation_id: classic.generation_id,
                member_instance_id: member.group_instance_id.as_deref(),
                member_session_timeout: member.session_timeout,
                assigned_partitions: decoded.assigned_partitions,
                state: classic.state,
                seed_assignment_preserved: asn2 == asn,
                protocol_name: classic.protocol_name.as_deref(),
                leader_id: classic.leader_id.as_deref(),
            } == DowngradeProjection {
                group_id: "g",
                generation_id: 7,
                member_instance_id: Some("inst-a"),
                member_session_timeout: Duration::from_secs(30),
                assigned_partitions: vec![
                    crabka_protocol::owned::consumer_protocol_assignment::TopicPartition {
                        topic: "orders".to_string(),
                        partitions: vec![0, 1],
                        ..Default::default()
                    },
                ],
                state: ClassicGroupState::Stable,
                seed_assignment_preserved: true,
                protocol_name: Some("range"),
                leader_id: Some("m1"),
            }
        );
    }
}
