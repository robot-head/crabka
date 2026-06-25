//! Classic-protocol per-`group_id` state machine (`Group`). Pure data +
//! transitions, owned by the unified per-group actor. Committed offsets are
//! protocol-agnostic and live on the unified [`super::group::Group`]
//! container, not here.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

/// Five-state machine for a consumer group, matching the Apache Kafka
/// classic protocol (KIP-62 / KIP-394).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GroupState {
    /// No members and no committed offsets.
    Empty,
    /// At least one member has called `JoinGroup`; waiting for the rebalance
    /// deadline or every expected member.
    PreparingRebalance,
    /// `JoinGroup` returned to all members; waiting for the leader's `SyncGroup`.
    CompletingRebalance,
    /// `SyncGroup` completed; members are heart-beating.
    Stable,
    /// Group has been deleted (e.g. after the last member leaves and an
    /// optional retention period). Reserved; the MVP doesn't actively
    /// transition into this state.
    Dead,
}

/// One member of a [`Group`].
#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Member {
    pub member_id: String,
    /// KIP-345 static-membership pin. When `Some`, the broker preserves
    /// this member's slot across session timeouts and matches reconnecting
    /// clients by `group_instance_id` rather than minting a fresh
    /// `member_id`.
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub last_heartbeat: Instant,
    /// Encoded `ConsumerProtocolSubscription` bytes (a `subscription` field
    /// from `JoinGroupRequest`). Opaque to the broker. This is the metadata
    /// for the selected protocol — populated after `select_protocol` picks a
    /// winner via [`Group::resolve_selected_protocol_metadata`].
    pub protocol_metadata: Bytes,
    /// Full list of `(protocol_name, metadata)` pairs the member proposed in
    /// its `JoinGroupRequest`. Used to negotiate the group protocol.
    pub protocols: Vec<(String, Bytes)>,
    /// Encoded `ConsumerProtocolAssignment` bytes — populated by the leader
    /// in `SyncGroup`. `None` until then.
    pub assignment: Option<Bytes>,
}

impl Member {
    #[must_use]
    pub fn new(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        host: impl Into<String>,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        protocols: Vec<(String, Bytes)>,
    ) -> Self {
        let protocol_metadata = protocols
            .first()
            .map(|(_, b)| b.clone())
            .unwrap_or_default();
        Self {
            member_id: member_id.into(),
            group_instance_id: None,
            client_id: client_id.into(),
            host: host.into(),
            session_timeout,
            rebalance_timeout,
            last_heartbeat: Instant::now(),
            protocol_metadata,
            protocols,
            assignment: None,
        }
    }

    /// Builder-style: pin a `group.instance.id` (KIP-345 static membership).
    #[must_use]
    pub fn with_instance_id(mut self, instance_id: Option<String>) -> Self {
        self.group_instance_id = instance_id;
        self
    }

    #[must_use]
    pub fn is_static(&self) -> bool {
        self.group_instance_id.is_some()
    }
}

/// Outcome of [`Group::add_member`] — drives the `JoinGroup` handler's
/// fast-path decisions for KIP-345 static-rejoin.
#[derive(Debug, PartialEq, Eq)]
pub enum AddMemberOutcome {
    /// Brand-new member added; group transitioned to `PreparingRebalance`
    /// if it was previously `Empty` or `Stable`.
    NewMember,
    /// A static member with this `group.instance.id` already existed and
    /// has been replaced in-place. The prior `member_id` (which the new
    /// session may or may not match) is returned. If the group was
    /// `Stable` the state is preserved — the new session reuses the
    /// cached assignment without forcing a rebalance.
    StaticRejoin { prior_member_id: String },
    /// A *different* live `member_id` is currently pinned to this
    /// `group.instance.id`. The caller must reject with
    /// `FENCED_INSTANCE_ID`. No state change happened.
    Fenced { live_member_id: String },
}

/// Pick the protocol name with the most first-place votes among names
/// proposed by every member. Ties broken lexicographically. Returns
/// `None` if the intersection is empty (or there are no members).
#[must_use]
pub fn select_protocol(members: &HashMap<String, Member>) -> Option<String> {
    if members.is_empty() {
        return None;
    }
    let mut iter = members.values();
    let first = iter.next()?;
    let mut intersection: std::collections::HashSet<String> =
        first.protocols.iter().map(|(n, _)| n.clone()).collect();
    for m in iter {
        let names: std::collections::HashSet<String> =
            m.protocols.iter().map(|(n, _)| n.clone()).collect();
        intersection = intersection.intersection(&names).cloned().collect();
    }
    if intersection.is_empty() {
        return None;
    }
    let mut votes: HashMap<&str, usize> = HashMap::new();
    for m in members.values() {
        if let Some((name, _)) = m.protocols.first()
            && intersection.contains(name)
        {
            *votes.entry(name.as_str()).or_insert(0) += 1;
        }
    }
    intersection
        .iter()
        .max_by(|a, b| {
            let va = votes.get(a.as_str()).copied().unwrap_or(0);
            let vb = votes.get(b.as_str()).copied().unwrap_or(0);
            va.cmp(&vb).then_with(|| b.cmp(a))
        })
        .cloned()
}

/// A committed offset entry. Keyed by `(topic, partition)` in
/// [`Group::committed_offsets`].
#[derive(Debug, Clone)]
pub struct OffsetEntry {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_field_names)]
pub struct Group {
    pub group_id: String,
    pub state: GroupState,
    /// `"consumer"` for `KafkaConsumer`. The broker doesn't interpret the
    /// value beyond rejecting inconsistent proposals.
    pub protocol_type: Option<String>,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub protocol_name: Option<String>,
    pub members: HashMap<String, Member>,
    /// KIP-345 secondary index: `group.instance.id` → current `member_id`.
    /// Mirrors the `group_instance_id` field on entries in `members`. Used
    /// to look up a static member's slot when a reconnecting client either
    /// omits its `member_id` (KIP-394 bootstrap) or supplies a stale one
    /// from a prior session.
    pub static_members: HashMap<String, String>,
    pub rebalance_deadline: Option<Instant>,
    /// Members whose `JoinGroup` has arrived since the last transition into
    /// `PreparingRebalance`. The `JoinGroup` handler runs the rebalance early
    /// — without waiting out `INITIAL_REBALANCE_DELAY` — once every member
    /// still in `members` shows up here, which avoids the leader running
    /// the assignor on a stale-metadata snapshot when a slow member misses
    /// the wait window under load (the assignor's cooperative-sticky
    /// Pass-3 omissions then strand partitions on no member). Cleared on
    /// every transition into `PreparingRebalance`.
    pub joined_this_round: std::collections::HashSet<String>,
    /// `true` while the current `PreparingRebalance` round opened from an
    /// `Empty` group — a brand-new group or one whose members had all left
    /// (e.g. after a warm-up consumer joins and leaves). Such a round honors
    /// the full `INITIAL_REBALANCE_DELAY` batching window — mirroring Apache
    /// Kafka's `InitialDelayedJoin` — instead of eager-completing the moment
    /// the first member shows up, so a herd of consumers starting together
    /// lands in a single generation. Eager-completing a from-`Empty` round
    /// instead strands the first joiner in a solo generation and forces an
    /// immediate re-rebalance when the next member arrives, thrashing
    /// produce+fetch under load. `false` for rebalances triggered by a
    /// membership change in a group that still had members (`Stable` →
    /// `PreparingRebalance`), which complete as soon as every still-live
    /// member rejoins.
    pub rebalance_from_empty: bool,
}

impl Group {
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            state: GroupState::Empty,
            protocol_type: None,
            generation_id: 0,
            leader_id: None,
            protocol_name: None,
            members: HashMap::new(),
            static_members: HashMap::new(),
            rebalance_deadline: None,
            joined_this_round: std::collections::HashSet::new(),
            rebalance_from_empty: false,
        }
    }

    /// Look up the current `member_id` pinned to a `group.instance.id`,
    /// if any. KIP-345 entry point used by every group RPC handler.
    #[must_use]
    pub fn current_member_id_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.static_members.get(instance_id).map(String::as_str)
    }

    /// Add or refresh a member.
    ///
    /// **Dynamic** (no `group_instance_id`): inserted; transitions to
    /// `PreparingRebalance` if previously `Empty` or `Stable`. Returns
    /// [`AddMemberOutcome::NewMember`].
    ///
    /// **Static** (KIP-345, `group_instance_id` set): three cases.
    /// 1. The instance id is new → behaves like a dynamic add. Returns
    ///    [`AddMemberOutcome::NewMember`].
    /// 2. The instance id maps to an existing live `member_id` that
    ///    matches the incoming `member.member_id` (or the incoming id is
    ///    different but the leader-side bootstrap-rejoin path supplied a
    ///    fresh id for the same instance) → the slot is replaced
    ///    in-place; the prior `assignment` and the group's `state` are
    ///    preserved (no rebalance triggered on `Stable` rejoin). Returns
    ///    [`AddMemberOutcome::StaticRejoin`] with the prior member id.
    /// 3. The instance id maps to a different live `member_id` and the
    ///    caller did not request a takeover → the caller must reject
    ///    with `FENCED_INSTANCE_ID`. Returns [`AddMemberOutcome::Fenced`].
    ///    The handler decides whether a non-empty mismatching
    ///    `req.member_id` is a real fence or a legitimate replacement;
    ///    `add_member` itself always performs the takeover unless the
    ///    caller pre-checks with [`Self::current_member_id_for_instance`].
    pub fn add_member(&mut self, member: Member) -> AddMemberOutcome {
        if let Some(instance_id) = member.group_instance_id.clone() {
            if let Some(prior_member_id) = self.static_members.get(&instance_id).cloned() {
                // Static rejoin: replace slot in-place, preserve assignment.
                let prior = self.members.remove(&prior_member_id);
                let mut next = member;
                if let Some(p) = prior {
                    // Inherit the previously installed assignment so a
                    // Stable-state rejoin can short-circuit the rebalance.
                    if next.assignment.is_none() {
                        next.assignment = p.assignment;
                    }
                }
                let new_member_id = next.member_id.clone();
                self.static_members
                    .insert(instance_id, new_member_id.clone());
                self.members.insert(new_member_id.clone(), next);
                // If this static rejoin lands during PreparingRebalance,
                // count the (potentially renamed) member as joined so the
                // early-completion check still fires.
                if matches!(self.state, GroupState::PreparingRebalance) {
                    self.joined_this_round.remove(&prior_member_id);
                    self.joined_this_round.insert(new_member_id);
                }
                // Crucially: do NOT touch self.state. Static rejoin from
                // Stable stays Stable; from PreparingRebalance stays
                // PreparingRebalance.
                return AddMemberOutcome::StaticRejoin { prior_member_id };
            }
            // Brand-new instance id: pin it and fall through to a
            // dynamic-style add.
            self.static_members
                .insert(instance_id, member.member_id.clone());
        }
        let was_empty = matches!(self.state, GroupState::Empty);
        // Also restart a rebalance when a new member joins while stuck in
        // CompletingRebalance. Real Kafka (KIP-62 AwaitingSync) transitions
        // back to PreparingRebalance on any new join so a dead leader's
        // CompletingRebalance deadlock doesn't strand the group forever.
        let was_first_or_stable = matches!(
            self.state,
            GroupState::Empty | GroupState::Stable | GroupState::CompletingRebalance
        );
        let member_id = member.member_id.clone();
        self.members.insert(member_id.clone(), member);
        if was_first_or_stable {
            self.state = GroupState::PreparingRebalance;
            self.joined_this_round.clear();
            // A round that opens from `Empty` batches the startup herd over
            // `INITIAL_REBALANCE_DELAY` (see `rebalance_from_empty`); one that
            // opens from `Stable` is a live-membership change and
            // eager-completes once every still-live member rejoins.
            self.rebalance_from_empty = was_empty;
        }
        if matches!(self.state, GroupState::PreparingRebalance) {
            self.joined_this_round.insert(member_id);
        }
        AddMemberOutcome::NewMember
    }

    /// True once every member currently in `members` has issued a
    /// `JoinGroup` since the last transition into `PreparingRebalance`.
    /// The `JoinGroup` handler uses this to short-circuit the rebalance
    /// wait the moment all members are accounted for, so the leader runs
    /// the assignor on a fresh snapshot of every member's owned set.
    #[must_use]
    pub fn all_members_joined_this_round(&self) -> bool {
        if self.members.is_empty() {
            return false;
        }
        self.members
            .keys()
            .all(|id| self.joined_this_round.contains(id))
    }

    /// Remove a member; transitions to `Empty` if no members remain.
    /// KIP-345: clears the static-membership index entry too.
    pub fn remove_member(&mut self, member_id: &str) {
        if let Some(m) = self.members.remove(member_id)
            && let Some(ref iid) = m.group_instance_id
        {
            // Only clear the index entry if it still points at *this*
            // member_id — a takeover may have already repointed it.
            if self.static_members.get(iid).map(String::as_str) == Some(member_id) {
                self.static_members.remove(iid);
            }
        }
        self.joined_this_round.remove(member_id);
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.leader_id = None;
            self.protocol_name = None;
            self.rebalance_deadline = None;
        }
    }

    /// Complete the rebalance: pick the leader (oldest `member_id` wins —
    /// stable for tests), bump the generation, advance state.
    pub fn complete_rebalance(&mut self, protocol_name: impl Into<String>) {
        let leader = self
            .members
            .keys()
            .min()
            .cloned()
            .expect("complete_rebalance requires ≥1 member");
        self.leader_id = Some(leader);
        self.protocol_name = Some(protocol_name.into());
        self.generation_id += 1;
        self.state = GroupState::CompletingRebalance;
        self.rebalance_deadline = None;
        self.joined_this_round.clear();
        self.rebalance_from_empty = false;
    }

    /// Set each member's `protocol_metadata` to its proposal for `name`.
    /// Members that didn't propose `name` retain their existing metadata
    /// (should not happen if called after a successful `select_protocol`).
    pub fn resolve_selected_protocol_metadata(&mut self, name: &str) {
        for m in self.members.values_mut() {
            if let Some((_, bytes)) = m.protocols.iter().find(|(n, _)| n == name) {
                m.protocol_metadata = bytes.clone();
            }
        }
    }

    /// Called when the leader's `SyncGroup` arrives with assignments.
    /// Stores each member's `assignment` and transitions to `Stable`.
    pub fn install_assignments(&mut self, assignments: HashMap<String, Bytes>) {
        for (member_id, bytes) in assignments {
            if let Some(m) = self.members.get_mut(&member_id) {
                m.assignment = Some(bytes);
            }
        }
        self.state = GroupState::Stable;
    }

    /// Drop any **dynamic** member whose `last_heartbeat` is older than
    /// its `session_timeout`. Returns the dropped member IDs.
    /// Transitions to `PreparingRebalance` if any were dropped and the
    /// group still has members; to `Empty` if it became empty.
    ///
    /// KIP-345: static members (`group_instance_id.is_some()`) are
    /// **skipped** — their slot is preserved across the session timeout
    /// so a restarting client reclaims its assignment on rejoin without
    /// kicking the rest of the group into a rebalance.
    pub fn expire_dead_members(&mut self, now: Instant) -> Vec<String> {
        // Expire members in all active states (PreparingRebalance,
        // CompletingRebalance, Stable). Empty and Dead have no members to
        // expire. Importantly, expiring in CompletingRebalance lets the broker
        // evict a dead group leader that will never send SyncGroup — otherwise
        // the group is permanently stuck: expire_dead_members only ran in
        // Stable (post-SyncGroup), so a dead leader stranded the group forever.
        //
        // The original Stable-only guard existed to prevent a race where a
        // member with a very small session_timeout could self-evict before its
        // JoinGroup returned (last_heartbeat = add_member time, session_timeout
        // = 3s = INITIAL_REBALANCE_DELAY). With the default session_timeout of
        // 45s and a 3s rebalance delay this race is impossible in practice, and
        // real Kafka expires members regardless of group state.
        if matches!(self.state, GroupState::Empty | GroupState::Dead) {
            return Vec::new();
        }
        let dropped: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| {
                !m.is_static() && now.duration_since(m.last_heartbeat) > m.session_timeout
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dropped {
            // Dynamic members only — no static_members entry to clear.
            self.members.remove(id);
        }
        if !dropped.is_empty() {
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.leader_id = None;
                self.protocol_name = None;
            } else {
                self.state = GroupState::PreparingRebalance;
                // Live-membership change (a member timed out), not a
                // start-from-empty herd: eager-complete once the survivors
                // rejoin rather than holding the initial-delay window.
                self.rebalance_from_empty = false;
                // If we just evicted from CompletingRebalance, rebalance_deadline
                // is None (complete_rebalance clears it). Set a fresh deadline so
                // parked joiners/followers wake up via the actor's opt_sleep path
                // rather than waiting indefinitely for a rejoin that never comes.
                if self.rebalance_deadline.is_none() {
                    self.rebalance_deadline = Some(Instant::now() + Duration::from_secs(3));
                }
            }
        }
        dropped
    }
}

#[cfg(test)]
#[path = "classic_state_model.rs"]
mod classic_state_model;

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    fn sample_member(id: &str) -> Member {
        Member::new(
            id,
            "test-client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), Bytes::new())],
        )
    }

    fn member_with_protocols(id: &str, protocols: Vec<(&str, &[u8])>) -> Member {
        Member::new(
            id,
            "test-client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            protocols
                .into_iter()
                .map(|(n, b)| (n.to_string(), Bytes::copy_from_slice(b)))
                .collect(),
        )
    }

    #[test]
    fn empty_to_preparing_on_first_join() {
        let mut g = Group::new("g");
        assert!(g.state == GroupState::Empty);
        g.add_member(sample_member("m1"));
        assert!(g.state == GroupState::PreparingRebalance);
    }

    #[test]
    fn complete_rebalance_bumps_generation() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.add_member(sample_member("m2"));
        g.complete_rebalance("range");
        assert!(g.generation_id == 1);
        assert!(g.leader_id.as_deref() == Some("m1"));
        assert!(g.protocol_name.as_deref() == Some("range"));
        assert!(g.state == GroupState::CompletingRebalance);
    }

    #[test]
    fn install_assignments_to_stable() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.complete_rebalance("range");
        let mut a = HashMap::new();
        a.insert("m1".into(), Bytes::from_static(b"assignment-bytes"));
        g.install_assignments(a);
        assert!(g.state == GroupState::Stable);
        assert!(g.members["m1"].assignment.is_some());
    }

    #[test]
    fn remove_last_member_empties_group() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.remove_member("m1");
        assert!(g.state == GroupState::Empty);
        assert!(g.leader_id.is_none());
    }

    fn static_member(member_id: &str, instance_id: &str) -> Member {
        sample_member(member_id).with_instance_id(Some(instance_id.to_string()))
    }

    #[test]
    fn static_rejoin_preserves_stable_state_and_assignment() {
        let mut g = Group::new("g");
        let outcome = g.add_member(static_member("m1", "inst-a"));
        assert!(outcome == AddMemberOutcome::NewMember);
        g.complete_rebalance("range");
        let mut a = HashMap::new();
        a.insert("m1".into(), Bytes::from_static(b"assignment-bytes"));
        g.install_assignments(a);
        assert!(g.state == GroupState::Stable);

        // Rejoin with the same instance id but a fresh `member_id` (the
        // client restarted; KIP-394 bootstrap gave it a new id).
        let outcome = g.add_member(static_member("m2", "inst-a"));
        assert!(
            outcome
                == AddMemberOutcome::StaticRejoin {
                    prior_member_id: "m1".into()
                }
        );
        // State preserved: no rebalance kicked off.
        assert!(g.state == GroupState::Stable);
        assert!(g.members.len() == 1);
        // New member inherited the prior assignment.
        assert!(g.members.contains_key("m2"));
        assert!(g.members["m2"].assignment.as_deref() == Some(b"assignment-bytes" as &[u8]));
        // Index repointed.
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
    }

    #[test]
    fn static_member_timeout_is_suppressed() {
        let mut g = Group::new("g");
        let mut m = static_member("m1", "inst-a");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;

        let dropped = g.expire_dead_members(Instant::now());
        assert!(dropped.is_empty(), "static member must NOT be expired");
        assert!(g.state == GroupState::Stable);
        assert!(g.members.contains_key("m1"));
        // Index entry retained.
        assert!(g.current_member_id_for_instance("inst-a") == Some("m1"));
    }

    #[test]
    fn dynamic_member_timeout_still_drops_in_mixed_group() {
        let mut g = Group::new("g");
        g.add_member(static_member("static-1", "inst-a"));
        let mut dyn_m = sample_member("dyn-1");
        dyn_m.session_timeout = Duration::from_millis(1);
        dyn_m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(dyn_m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;

        let dropped = g.expire_dead_members(Instant::now());
        assert!(dropped == vec!["dyn-1".to_string()]);
        assert!(g.state == GroupState::PreparingRebalance);
        assert!(g.members.contains_key("static-1"));
    }

    #[test]
    fn remove_static_member_clears_index() {
        let mut g = Group::new("g");
        g.add_member(static_member("m1", "inst-a"));
        assert!(g.current_member_id_for_instance("inst-a") == Some("m1"));
        g.remove_member("m1");
        assert!(g.current_member_id_for_instance("inst-a") == None);
        assert!(g.state == GroupState::Empty);
    }

    #[test]
    fn static_takeover_does_not_clear_index_for_prior_id() {
        // After a takeover, the index points at the new id. Removing the
        // *prior* id (e.g. via a stale LeaveGroup from the old session)
        // must NOT wipe the index entry.
        let mut g = Group::new("g");
        g.add_member(static_member("m1", "inst-a"));
        g.add_member(static_member("m2", "inst-a"));
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
        // m1 is no longer in members (replaced), so this is a no-op.
        g.remove_member("m1");
        assert!(g.current_member_id_for_instance("inst-a") == Some("m2"));
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn select_protocol_single_member_picks_first() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("range"));
    }

    #[test]
    fn select_protocol_intersection_empty_returns_none() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("cooperative_sticky", b"")]),
        );
        assert!(select_protocol(&members) == None);
    }

    #[test]
    fn select_protocol_max_votes_wins() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m3".to_string(),
            member_with_protocols("m3", vec![("cooperative_sticky", b""), ("range", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("range"));
    }

    #[test]
    fn select_protocol_tie_breaks_lexicographically() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("cooperative_sticky", b""), ("range", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("cooperative_sticky"));
    }

    #[test]
    fn select_protocol_empty_members_returns_none() {
        let members = HashMap::new();
        assert!(select_protocol(&members) == None);
    }

    #[test]
    fn resolve_metadata_updates_each_member() {
        let mut g = Group::new("g");
        g.add_member(member_with_protocols(
            "m1",
            vec![("range", b"r1"), ("cooperative_sticky", b"c1")],
        ));
        g.add_member(member_with_protocols(
            "m2",
            vec![("range", b"r2"), ("cooperative_sticky", b"c2")],
        ));
        g.resolve_selected_protocol_metadata("cooperative_sticky");
        assert!(g.members["m1"].protocol_metadata.as_ref() == b"c1");
        assert!(g.members["m2"].protocol_metadata.as_ref() == b"c2");
    }

    #[test]
    fn expire_dead_members_drops_stale() {
        let mut g = Group::new("g");
        let mut m = sample_member("m1");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        g.complete_rebalance("range");
        g.state = GroupState::Stable;
        let dropped = g.expire_dead_members(Instant::now());
        assert!(dropped == vec!["m1".to_string()]);
        assert!(g.state == GroupState::Empty);
    }
}
