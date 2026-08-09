//! Per-group state for KIP-848 next-gen consumer groups. Exactly one
//! `actor::GroupActor` task owns this state. It is never shared.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use bytes::Bytes;
use crabka_protocol::primitives::uuid::Uuid;
use regex::Regex;

use super::{expired_member_ids, persistence_next_gen::MemberAssignmentState};

/// Classic-protocol state for a member hosted inside an *upgraded* consumer
/// group during a KIP-848 upgrade.
///
/// The value is `None` on a native consumer-protocol member. It is `Some` once
/// the broker upgraded a classic member's group, and when a classic member
/// joins an already-upgraded group.
///
/// The member keeps speaking the classic `JoinGroup`, `SyncGroup`, and
/// `Heartbeat` protocol. The coordinator serves it by mapping onto the
/// consumer-group machinery, and by translating its target into a
/// `ConsumerProtocolAssignment` blob on `SyncGroup`.
#[derive(Debug, Clone)]
pub struct ClassicMemberFacade {
    /// Classic generation that the coordinator echoes to the member. It
    /// advances with the group epoch.
    pub generation_id: i32,
    /// `(protocol_name, metadata)` pairs the member proposed in `JoinGroup`.
    /// The state keeps them so that a downgrade restores the classic member
    /// with no loss.
    pub supported_protocols: Vec<(String, Bytes)>,
    /// The member's classic `session.timeout.ms`.
    pub session_timeout: Duration,
    /// The last `ConsumerProtocolAssignment` blob that `SyncGroup` returned.
    pub last_synced_assignment: Bytes,
    /// `true` once the member must send `SyncGroup` again to pick up a changed
    /// target.
    pub awaiting_sync: bool,
}

#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    /// KIP-848 v1+ `subscribed_topic_regex`. When set, the reconciler resolves
    /// it against the metadata image and unions the match with
    /// `subscribed_topic_names`. `None` means "no regex", that is an
    /// exact-name subscription only.
    ///
    /// The broker does not persist this field on `__consumer_offsets` yet. The
    /// client supplies it again on every heartbeat, so a coordinator failover
    /// loses at most one heartbeat interval of state.
    pub subscribed_topic_regex: Option<String>,
    /// Compiled form of `subscribed_topic_regex`. The cache stops the
    /// reconciler from compiling the pattern for this member again on every
    /// recompute. It separates three cases: a compiled pattern, a cached
    /// compilation failure, and an absent pattern. Always keep it in step
    /// through [`MemberState::set_regex`]. Never set `subscribed_topic_regex`
    /// directly.
    ///
    /// `Regex` is `Clone` and `Debug`, but NOT `PartialEq` or `Eq`.
    /// `MemberState` derives only `Clone` and `Debug`, with no `PartialEq`, so
    /// this field needs no special handling. If someone adds `PartialEq`,
    /// compare on the pattern string instead and skip this cached field.
    ///
    /// This field is public only so that cross-module struct literals can
    /// initialize it to its default. Treat it as private, and change it only
    /// through [`MemberState::set_regex`] and
    /// [`MemberState::sync_regex_cache`].
    pub compiled_regex: CompiledRegex,
    pub server_assignor: Option<String>,
    pub rebalance_timeout: Duration,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub assignment_state: MemberAssignmentState,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub partitions_pending_revocation: HashMap<Uuid, Vec<i32>>,
    pub last_seen: Instant,
    /// Set if and only if this is a classic member hosted in an upgraded
    /// group.
    pub classic: Option<ClassicMemberFacade>,
}

#[derive(Debug, Clone, Default)]
pub enum CompiledRegex {
    #[default]
    Absent,
    Invalid,
    Valid(Regex),
}

impl MemberState {
    /// `true` if this member speaks the classic protocol inside an upgraded
    /// group. Its RPCs are then `JoinGroup`, `SyncGroup`, and `Heartbeat`, not
    /// `ConsumerGroupHeartbeat`.
    #[must_use]
    pub fn is_classic(&self) -> bool {
        self.classic.is_some()
    }

    /// Sets `subscribed_topic_regex` and compiles the cached `Regex` again.
    ///
    /// The compile runs exactly once per distinct pattern, so call this method
    /// only when the pattern changes. An invalid pattern goes into the cache
    /// as [`CompiledRegex::Invalid`], with one warning. The reconciler then
    /// neither retries the compile nor treats the pattern as "match
    /// everything".
    pub fn set_regex(&mut self, pattern: Option<String>) {
        self.compiled_regex = match pattern.as_deref() {
            None => CompiledRegex::Absent,
            Some(pat) => match Regex::new(pat) {
                Ok(regex) => CompiledRegex::Valid(regex),
                Err(e) => {
                    tracing::warn!(
                        pattern = %pat, error = %e,
                        "consumer-group: subscribed_topic_regex failed to compile; ignored"
                    );
                    CompiledRegex::Invalid
                }
            },
        };
        self.subscribed_topic_regex = pattern;
    }

    /// Compiles the cache again from the current value of
    /// `subscribed_topic_regex`.
    ///
    /// This method exists for construction sites that set the pattern field
    /// through a struct literal. Those sites are in other modules, so they
    /// cannot call the setter inline. Call this method once afterwards to fill
    /// the cache.
    pub fn sync_regex_cache(&mut self) {
        let pattern = self.subscribed_topic_regex.take();
        self.set_regex(pattern);
    }

    /// The subscription regex that compiled successfully, if there is one. It
    /// returns `None` when there is no pattern, and when the pattern failed to
    /// compile.
    #[must_use]
    pub fn compiled_regex(&self) -> Option<&Regex> {
        match &self.compiled_regex {
            CompiledRegex::Valid(regex) => Some(regex),
            CompiledRegex::Absent | CompiledRegex::Invalid => None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}

#[derive(Debug)]
pub struct GroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, MemberState>,
    pub instance_to_member: HashMap<String, String>,
    pub target: TargetAssignment,
    pub dirty: bool,
}

impl GroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: 0,
            members: HashMap::new(),
            instance_to_member: HashMap::new(),
            target: TargetAssignment::default(),
            dirty: false,
        }
    }

    pub fn bump_epoch(&mut self) {
        self.group_epoch += 1;
        self.dirty = true;
    }

    /// The KIP-848 `OffsetCommit` fencing decision: a member may commit only
    /// with its CURRENT member epoch. `Ok(())` accepts the commit. Any other
    /// result is the Kafka error code.
    ///
    /// This method deliberately does NOT check partition ownership, because
    /// Kafka lets a member with the right epoch commit any partition. The
    /// epoch is the only fence. It therefore rejects a zombie from before a
    /// rebalance, whose epoch the group has since raised.
    ///
    /// The method is pure. It is separate from the actor's `ValidateCommit` so
    /// that the consumer-group composition model can drive the real rule.
    pub(crate) fn validate_commit_decision(&self, member_id: &str, epoch: i32) -> Result<(), i16> {
        match self.members.get(member_id) {
            None => Err(crate::codes::UNKNOWN_MEMBER_ID),
            Some(m) if epoch < m.member_epoch => Err(crate::codes::STALE_MEMBER_EPOCH),
            Some(m) if epoch > m.member_epoch => Err(crate::codes::FENCED_MEMBER_EPOCH),
            Some(_) => Ok(()),
        }
    }

    pub fn add_or_update_member(&mut self, mut m: MemberState) {
        // Ensure the cached compiled regex matches the pattern the caller
        // supplied. Construction sites set `subscribed_topic_regex` via a
        // struct literal and leave `compiled_regex` as `None`; recompile once
        // here so the reconciler never has to.
        m.sync_regex_cache();
        if let Some(iid) = m.instance_id.clone() {
            self.instance_to_member.insert(iid, m.member_id.clone());
        }
        let cached: Option<(HashSet<String>, Option<String>)> =
            self.members.get(&m.member_id).map(|prev| {
                (
                    prev.subscribed_topic_names.clone(),
                    prev.subscribed_topic_regex.clone(),
                )
            });
        let subscription_changed = cached.as_ref().is_none_or(|(names, regex)| {
            names != &m.subscribed_topic_names || regex != &m.subscribed_topic_regex
        });
        self.members.insert(m.member_id.clone(), m);
        if subscription_changed {
            self.dirty = true;
        }
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<MemberState> {
        let m = self.members.remove(member_id)?;
        if let Some(ref iid) = m.instance_id
            && self.instance_to_member.get(iid).map(String::as_str) == Some(member_id)
        {
            self.instance_to_member.remove(iid);
        }
        self.dirty = true;
        Some(m)
    }

    pub fn evict_expired(&mut self, now: Instant, session_timeout: Duration) -> Vec<String> {
        let evicted = expired_member_ids(
            self.members
                .iter()
                .map(|(id, member)| (id.as_str(), member.last_seen)),
            now,
            session_timeout,
        );
        for id in &evicted {
            self.remove_member(id);
        }
        evicted
    }

    pub fn install_target(&mut self, per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>) {
        self.target.epoch = self.group_epoch;
        self.target.per_member = per_member;
        for (mid, member) in &mut self.members {
            let target = self.target.per_member.get(mid).cloned().unwrap_or_default();
            // Split everything the member still holds — its current assignment
            // PLUS any partitions already pending revocation — against the new
            // target. Splitting `assigned ∪ pending` (not just `assigned`)
            // preserves in-flight revocations across successive reconciles, so a
            // partition a member is still releasing is never mistaken for "free"
            // by the withholding in `reconcile_member`.
            let mut held = member.assigned_partitions.clone();
            for (tid, parts) in &member.partitions_pending_revocation {
                held.entry(*tid).or_default().extend(parts.iter().copied());
            }
            let (revoke, assigned) = compute_revoke_split(&held, &target);
            member.partitions_pending_revocation = revoke;
            member.assigned_partitions = assigned;
            member.assignment_state = if !member.partitions_pending_revocation.is_empty() {
                MemberAssignmentState::UnrevokedPartitions
            } else if assignment_covers(&member.assigned_partitions, &target) {
                MemberAssignmentState::Stable
            } else {
                MemberAssignmentState::UnreleasedPartitions
            };
        }
    }

    pub fn advance_member_epoch(&mut self, member_id: &str) {
        if let Some(m) = self.members.get_mut(member_id) {
            m.previous_member_epoch = m.member_epoch;
            m.member_epoch = self.group_epoch;
        }
    }

    /// KIP-848 per-heartbeat reconciliation, the `CurrentAssignmentBuilder`.
    ///
    /// It computes the authoritative *current* assignment for `member_id` from
    /// the member's reported owned set and the group target. It **withholds
    /// any partition that another member still holds**, whether that member
    /// owns it or has it pending revocation.
    ///
    /// The returned set is what the coordinator grants the member now and
    /// advertises in the heartbeat response. Storing it as
    /// `assigned_partitions` makes the grant authoritative, so the broker
    /// never advertises a partition to two members at once. That is the main
    /// KIP-848 safety property. See `reconciler_model.rs`.
    ///
    /// A member keeps every target partition it already owns, and gains target
    /// partitions that are *free*. A partition it owns but no longer targets
    /// moves to `partitions_pending_revocation`. Another member claims a freed
    /// partition only once its previous owner reports that it released the
    /// partition. That owner's next heartbeat drops the partition from
    /// `reported_owned`, which drains it from both sets.
    ///
    /// It returns `true` when the member's assignment or pending set
    /// changed.
    pub fn reconcile_member(
        &mut self,
        member_id: &str,
        reported_owned: &HashMap<Uuid, Vec<i32>>,
    ) -> bool {
        let target = self
            .target
            .per_member
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        // `assigned ∪ pending` of every OTHER member is exactly what that member
        // still holds; it is invariant under `install_target`'s keep/revoke split.
        let mut held_by_others: HashSet<(Uuid, i32)> = HashSet::new();
        for (mid, m) in &self.members {
            if mid == member_id {
                continue;
            }
            for (tid, parts) in m
                .assigned_partitions
                .iter()
                .chain(m.partitions_pending_revocation.iter())
            {
                for &p in parts {
                    held_by_others.insert((*tid, p));
                }
            }
        }
        // Grant each target partition the member already owns OR that is free.
        let mut new_assigned: HashMap<Uuid, Vec<i32>> = HashMap::new();
        let mut fully_assigned = true;
        for (tid, tparts) in &target {
            for &p in tparts {
                let owned_here = reported_owned.get(tid).is_some_and(|o| o.contains(&p));
                let free = !held_by_others.contains(&(*tid, p));
                if owned_here || free {
                    new_assigned.entry(*tid).or_default().push(p);
                } else {
                    fully_assigned = false;
                }
            }
        }
        // Pending revocation = reported-owned partitions no longer in the target.
        let mut new_pending: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for (tid, oparts) in reported_owned {
            let tset: HashSet<i32> = target
                .get(tid)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect();
            for &p in oparts {
                if !tset.contains(&p) {
                    new_pending.entry(*tid).or_default().push(p);
                }
            }
        }
        for v in new_assigned.values_mut() {
            v.sort_unstable();
        }
        for v in new_pending.values_mut() {
            v.sort_unstable();
        }
        let m = self
            .members
            .get_mut(member_id)
            .expect("member exists in reconcile_member");
        let changed =
            m.assigned_partitions != new_assigned || m.partitions_pending_revocation != new_pending;
        m.assigned_partitions = new_assigned;
        m.partitions_pending_revocation = new_pending;
        m.assignment_state = if !m.partitions_pending_revocation.is_empty() {
            MemberAssignmentState::UnrevokedPartitions
        } else if fully_assigned {
            MemberAssignmentState::Stable
        } else {
            MemberAssignmentState::UnreleasedPartitions
        };
        changed
    }

    pub fn current_member_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.instance_to_member.get(instance_id).map(String::as_str)
    }
}

/// True if `assigned` contains every partition in `target`.
fn assignment_covers(assigned: &HashMap<Uuid, Vec<i32>>, target: &HashMap<Uuid, Vec<i32>>) -> bool {
    target.iter().all(|(tid, tparts)| {
        tparts
            .iter()
            .all(|p| assigned.get(tid).is_some_and(|a| a.contains(p)))
    })
}

fn compute_revoke_split(
    current: &HashMap<Uuid, Vec<i32>>,
    target: &HashMap<Uuid, Vec<i32>>,
) -> (HashMap<Uuid, Vec<i32>>, HashMap<Uuid, Vec<i32>>) {
    let mut revoke: HashMap<Uuid, Vec<i32>> = HashMap::new();
    let mut keep: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, parts) in current {
        let target_parts = target.get(tid).cloned().unwrap_or_default();
        let target_set: HashSet<i32> = target_parts.into_iter().collect();
        for p in parts {
            if target_set.contains(p) {
                keep.entry(*tid).or_default().push(*p);
            } else {
                revoke.entry(*tid).or_default().push(*p);
            }
        }
    }
    (revoke, keep)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn member(id: &str) -> MemberState {
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: HashSet::new(),
            subscribed_topic_regex: None,
            compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
            classic: None,
        }
    }

    #[test]
    fn add_member_marks_dirty_first_time() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        assert!(g.dirty);
    }

    #[test]
    fn re_add_same_subscription_keeps_clean_after_reset() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        g.add_or_update_member(member("m1"));
        assert!(!g.dirty);
    }

    #[test]
    fn subscription_change_marks_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        let mut m = member("m1");
        m.subscribed_topic_names.insert("t".into());
        g.add_or_update_member(m);
        assert!(g.dirty);
    }

    #[test]
    fn remove_member_marks_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        g.remove_member("m1");
        assert!(g.dirty);
    }

    #[test]
    fn evict_expired_drops_old_members() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.last_seen = Instant::now().checked_sub(Duration::from_mins(2)).unwrap();
        g.add_or_update_member(m);
        g.add_or_update_member(member("m2"));
        let evicted = g.evict_expired(Instant::now(), Duration::from_mins(1));
        assert!(evicted == vec!["m1".to_string()]);
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn install_target_computes_revoke_split() {
        let mut g = GroupState::new("g");
        let t = Uuid([1; 16]);
        let mut m = member("m1");
        m.assigned_partitions.insert(t, vec![0, 1, 2]);
        g.add_or_update_member(m);
        let mut target_for_m1 = HashMap::new();
        target_for_m1.insert(t, vec![0, 1]);
        g.install_target([("m1".to_string(), target_for_m1)].into());
        let m = &g.members["m1"];
        check!(m.partitions_pending_revocation[&t] == vec![2]);
        check!(m.assigned_partitions[&t] == vec![0, 1]);
        check!(m.assignment_state == MemberAssignmentState::UnrevokedPartitions);
    }

    #[test]
    fn instance_binding_tracked() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.instance_id = Some("inst1".into());
        g.add_or_update_member(m);
        assert!(g.current_member_for_instance("inst1") == Some("m1"));
    }

    #[test]
    fn bump_epoch_increments_and_dirties() {
        let mut g = GroupState::new("g");
        g.dirty = false;
        g.bump_epoch();
        assert!(g.group_epoch == 1);
        assert!(g.dirty);
    }

    #[test]
    fn set_regex_compiles_and_caches() {
        let mut m = member("m1");
        m.set_regex(Some("^orders-.*".into()));
        assert!(m.subscribed_topic_regex.as_deref() == Some("^orders-.*"));
        let re = m.compiled_regex().expect("valid regex must compile");
        assert!(re.is_match("orders-eu"));
        assert!(!re.is_match("shipments"));
    }

    #[test]
    fn set_regex_caches_invalid_as_none() {
        let mut m = member("m1");
        m.set_regex(Some("*invalid".into()));
        // Pattern string is retained, but no compiled regex is exposed —
        // the reconciler treats this as names-only, not match-everything.
        assert!(m.subscribed_topic_regex.as_deref() == Some("*invalid"));
        assert!(m.compiled_regex().is_none());
    }

    #[test]
    fn set_regex_none_clears_cache() {
        let mut m = member("m1");
        m.set_regex(Some("^a".into()));
        assert!(m.compiled_regex().is_some());
        m.set_regex(None);
        assert!(m.subscribed_topic_regex.is_none());
        assert!(m.compiled_regex().is_none());
    }

    #[test]
    fn sync_regex_cache_populates_from_literal_field() {
        // Mimics a cross-module struct literal: pattern set, cache left None.
        let mut m = member("m1");
        m.subscribed_topic_regex = Some("^a".into());
        m.compiled_regex = crate::coordinator::unified::consumer_state::CompiledRegex::Absent;
        m.sync_regex_cache();
        assert!(m.subscribed_topic_regex.as_deref() == Some("^a"));
        assert!(m.compiled_regex().expect("synced").is_match("apple"));
    }

    #[test]
    fn advance_member_epoch_records_previous() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.group_epoch = 5;
        g.advance_member_epoch("m1");
        let m = &g.members["m1"];
        assert!(m.member_epoch == 5);
        assert!(m.previous_member_epoch == 0);
    }
}
