//! KIP-1071 streams-group in-memory state machine.
//!
//! This module mirrors the overall shape of the KIP-932 share-group state
//! machine (`super::super::share::state`): the `dirty` flag pattern,
//! `evict_expired`, `bump_epoch`, `install_target`, and
//! `advance_member_epoch`. It also mirrors the reconciliation mechanics of the
//! KIP-848 next-gen consumer state machine
//! (`super::super::consumer_state`): the `member_epoch` and
//! `previous_member_epoch` epoch exchange, and the revoke-before-assign split.
//! The difference is that streams members hold *tasks*
//! `(subtopology, partition)` across three disjoint roles, **active**,
//! **standby**, and **warmup**, instead of topic partitions.
//!
//! Only **active** tasks use the revoke-before-assign exchange. The current
//! owner must revoke an active task before another member can take it as
//! active. Standby and warmup tasks move freely, with no pending-revocation
//! bookkeeping.
//!
//! A role's assignment is a `BTreeMap<String, Vec<i32>>`, from
//! `subtopology_id` to a sorted, deduped partition list. Everything here uses
//! that representation, so the state machine stays independent of any wire,
//! codec, or persistence newtype.
//!
//! This module is fully self-contained. It depends only on `std` and the
//! `uuid` crate, and it needs `uuid` only for the
//! [`StreamsMemberState::joining`] helper that synthesizes a random
//! `process_id`. It deliberately does NOT import the sibling `persistence`
//! module. The `i8` conversions on [`StreamsMemberAssignmentState`] live here,
//! so the actor can persist the state without coupling the two files.

use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use crabka_log::Offset;

use super::super::expired_member_ids;

/// The reconciliation state of one streams-group member's **active** task set.
///
/// It mirrors KIP-848's `MemberAssignmentState`. Standby and warmup tasks take
/// no part in it.
///
/// Persistence stores this as a raw `i8`. [`as_i8`](Self::as_i8) and
/// [`from_i8`](Self::from_i8) convert it without coupling this module to the
/// persistence layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsMemberAssignmentState {
    /// The member's active tasks match its target, and nothing is pending.
    #[default]
    Stable = 0,
    /// The member still owns active tasks the new target took away from it.
    /// It must revoke them and acknowledge that in a heartbeat before it
    /// advances.
    UnrevokedActiveTasks = 1,
    /// The member's target includes active tasks that another member still
    /// owns and has not revoked. The member must wait for their release.
    UnreleasedActiveTasks = 2,
}

impl StreamsMemberAssignmentState {
    /// Raw `i8` discriminant for the persistence layer.
    #[must_use]
    pub fn as_i8(self) -> i8 {
        self as i8
    }

    /// Inverse of [`as_i8`](Self::as_i8). It returns `None` for an unknown
    /// discriminant instead of a panic, so that the caller, the actor, decides
    /// how to report a corrupt persisted record.
    #[must_use]
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Stable),
            1 => Some(Self::UnrevokedActiveTasks),
            2 => Some(Self::UnreleasedActiveTasks),
            _ => None,
        }
    }
}

/// One member of a streams group.
///
/// A member holds three disjoint sets of tasks by role: `active`, `standby`,
/// and `warmup`. Each set keys by subtopology id and holds a sorted, deduped
/// partition list. The `active_pending_revocation` map holds the active tasks
/// the member must give up before it can advance its epoch.
#[derive(Debug, Clone)]
pub struct StreamsMemberState {
    // --- identity ---
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    /// The Streams `process.id`, a stable per-process UUID string. The
    /// assignor reads it to co-locate the tasks of one process.
    pub process_id: String,
    /// Optional `(host, port)` the member advertises for interactive-query
    /// routing.
    pub user_endpoint: Option<(String, u32)>,
    /// Arbitrary `(key, value)` client tags for rack-aware and custom
    /// assignment.
    pub client_tags: Vec<(String, String)>,
    pub rebalance_timeout_ms: i32,

    // --- epochs ---
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    /// The topology epoch this member last acknowledged.
    pub topology_epoch: i32,

    // --- assignment ---
    pub assignment_state: StreamsMemberAssignmentState,
    /// Assigned active tasks: `subtopology_id` -> sorted, deduped partitions.
    pub active: BTreeMap<String, Vec<i32>>,
    /// Assigned standby tasks.
    pub standby: BTreeMap<String, Vec<i32>>,
    /// Assigned warmup tasks.
    pub warmup: BTreeMap<String, Vec<i32>>,
    /// Active tasks the member must revoke before it advances (KIP-848).
    pub active_pending_revocation: BTreeMap<String, Vec<i32>>,

    // --- reported catch-up progress (for warmup -> active promotion) ---
    /// `(subtopology, partition)` -> the changelog position the member last
    /// reported for that task.
    pub task_offsets: BTreeMap<(String, i32), Offset>,
    /// `(subtopology, partition)` -> the changelog end offset the member last
    /// reported for that task.
    pub task_end_offsets: BTreeMap<(String, i32), Offset>,

    pub last_seen: Instant,
}

impl StreamsMemberState {
    /// Constructs a newly joining member at epoch 0 with no assignment.
    ///
    /// When the client supplies no `process_id`, this method synthesizes a
    /// random UUID. A caller that already knows the process id should set the
    /// field afterwards.
    pub fn joining(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            instance_id: None,
            rack_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            process_id: uuid::Uuid::new_v4().to_string(),
            user_endpoint: None,
            client_tags: Vec::new(),
            rebalance_timeout_ms: 0,
            member_epoch: 0,
            previous_member_epoch: 0,
            topology_epoch: 0,
            assignment_state: StreamsMemberAssignmentState::Stable,
            active: BTreeMap::new(),
            standby: BTreeMap::new(),
            warmup: BTreeMap::new(),
            active_pending_revocation: BTreeMap::new(),
            task_offsets: BTreeMap::new(),
            task_end_offsets: BTreeMap::new(),
            last_seen: Instant::now(),
        }
    }
}

/// The target assignment from the most recent reconcile, stamped with the
/// assignment epoch it was computed against. Each role maps a member id to
/// that member's per-subtopology partition lists.
#[derive(Debug, Clone, Default)]
pub struct StreamsTargetAssignment {
    pub epoch: i32,
    pub active: HashMap<String, BTreeMap<String, Vec<i32>>>,
    pub standby: HashMap<String, BTreeMap<String, Vec<i32>>>,
    pub warmup: HashMap<String, BTreeMap<String, Vec<i32>>>,
}

/// The KIP-1071 group lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamsGroupStatePhase {
    /// No members.
    #[default]
    Empty,
    /// The group has members but cannot be assigned yet. Usually no topology
    /// is initialized, or required internal topics are missing.
    NotReady,
    /// A reconcile is in flight computing a new target assignment.
    Assigning,
    /// A target exists, and members converge on it by revoking and
    /// installing tasks.
    Reconciling,
    /// All members are at the assignment epoch with no pending revocations.
    Stable,
}

impl StreamsGroupStatePhase {
    /// The Kafka group-state string this phase serializes to.
    /// `DescribeGroups`, `ListGroups`, and the admin tools read it.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::NotReady => "NotReady",
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::Stable => "Stable",
        }
    }
}

/// A minimal handle for the resolved topology that lives in `topology.rs`.
///
/// The state machine tracks only the topology's *presence* and *epoch*. The
/// topology module derives the full subtopology and task sets.
#[derive(Debug, Clone, Default)]
pub struct StoredTopologyHandle {
    pub epoch: i32,
}

/// Full in-memory state of one streams group. Exactly one
/// `actor::GroupActor` task owns it, and it is never shared.
#[derive(Debug)]
pub struct StreamsGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub members: HashMap<String, StreamsMemberState>,
    /// The topology epoch. 0 means no topology is initialized yet.
    pub topology_epoch: i32,
    /// Presence and epoch of the stored topology. The full topology lives in
    /// `topology.rs`. This field only records that one exists.
    pub topology: Option<StoredTopologyHandle>,
    pub target: StreamsTargetAssignment,
    /// The state machine sets this on every change to membership,
    /// subscription, or topology epoch, so the actor knows a reconcile is
    /// pending. It clears the flag once the reconcile installs a target.
    pub dirty: bool,
    pub phase: StreamsGroupStatePhase,
    /// `(status_code, status_detail)` pairs that `DescribeStreamsGroups`
    /// reports, for example missing-source-topic and missing-internal-topic
    /// warnings.
    pub status: Vec<(i8, String)>,
}

impl StreamsGroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: 0,
            assignment_epoch: 0,
            members: HashMap::new(),
            topology_epoch: 0,
            topology: None,
            target: StreamsTargetAssignment::default(),
            dirty: false,
            phase: StreamsGroupStatePhase::Empty,
            status: Vec::new(),
        }
    }

    /// Increments the group epoch. This mirrors the share and consumer state
    /// machines: a fresh epoch makes the assignment stale, so the method marks
    /// the group dirty.
    pub fn bump_epoch(&mut self) {
        self.group_epoch += 1;
        self.dirty = true;
    }

    /// Inserts or replaces a member.
    ///
    /// The method marks the group dirty when the membership is new or the
    /// member's topology epoch changed. Those are the two signals that can
    /// force a reconcile. Re-adding an identical member leaves `dirty`
    /// unchanged.
    pub fn add_or_update_member(&mut self, m: StreamsMemberState) {
        let changed = match self.members.get(&m.member_id) {
            None => true,
            Some(prev) => prev.topology_epoch != m.topology_epoch,
        };
        self.members.insert(m.member_id.clone(), m);
        if changed {
            self.dirty = true;
        }
    }

    /// Removes a member and returns it if it was present. The method marks
    /// the group dirty only on a real removal.
    pub fn remove_member(&mut self, member_id: &str) -> Option<StreamsMemberState> {
        let m = self.members.remove(member_id);
        if m.is_some() {
            self.dirty = true;
        }
        m
    }

    /// Removes members whose `last_seen` is older than `session_timeout` and
    /// returns the evicted member ids. The method marks the group dirty if it
    /// removed any member.
    pub fn evict_expired(&mut self, now: Instant, session_timeout: Duration) -> Vec<String> {
        let evicted = expired_member_ids(
            self.members
                .iter()
                .map(|(id, member)| (id.as_str(), member.last_seen)),
            now,
            session_timeout,
        );
        for id in &evicted {
            self.members.remove(id);
        }
        if !evicted.is_empty() {
            self.dirty = true;
        }
        evicted
    }

    /// Installs a newly computed target assignment, stamped at the current
    /// group epoch, which becomes the new `assignment_epoch`.
    ///
    /// For every current member, this method computes the **active**
    /// revoke-split. Any active task the member owns that is *not* in its new
    /// active target moves into `active_pending_revocation`. If the method
    /// revoked anything, the member moves to
    /// [`StreamsMemberAssignmentState::UnrevokedActiveTasks`]; otherwise it
    /// stays at or returns to `Stable`. The method trims the member's assigned
    /// `active` set to the tasks it keeps, the intersection of current and
    /// target.
    ///
    /// This method does *not* install standby and warmup.
    /// [`Self::advance_member_epoch`] hands those over as a whole, so the
    /// member keeps serving its old standby and warmup tasks until it
    /// advances.
    pub fn install_target(&mut self, target: StreamsTargetAssignment) {
        self.assignment_epoch = self.group_epoch;
        self.target = target;
        self.target.epoch = self.assignment_epoch;

        for (mid, member) in &mut self.members {
            let target_active = self.target.active.get(mid).cloned().unwrap_or_default();
            // `compute_active_revoke_split` returns (keep, revoke): tasks the
            // member retains (current ∩ target) first, tasks it must give up
            // (current \ target) second.
            let (keep, revoke) = compute_active_revoke_split(&member.active, &target_active);
            member.active = keep;
            member.active_pending_revocation = revoke;
            member.assignment_state = if member.active_pending_revocation.is_empty() {
                StreamsMemberAssignmentState::Stable
            } else {
                StreamsMemberAssignmentState::UnrevokedActiveTasks
            };
        }
    }

    /// Advances a member to the current assignment epoch and gives it the full
    /// target that the latest reconcile allotted to it.
    ///
    /// The method records `previous_member_epoch`, installs the member's
    /// target active, standby, and warmup sets as its assigned sets, clears
    /// any pending revocation, and returns the member to
    /// [`StreamsMemberAssignmentState::Stable`].
    pub fn advance_member_epoch(&mut self, member_id: &str) {
        // Read the target out first to sidestep the borrow conflict between the
        // immutable `self.target` reads and the mutable member borrow.
        let active = self
            .target
            .active
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        let standby = self
            .target
            .standby
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        let warmup = self
            .target
            .warmup
            .get(member_id)
            .cloned()
            .unwrap_or_default();
        let epoch = self.assignment_epoch;
        if let Some(m) = self.members.get_mut(member_id) {
            m.previous_member_epoch = m.member_epoch;
            m.member_epoch = epoch;
            m.active = normalize_task_map(active);
            m.standby = normalize_task_map(standby);
            m.warmup = normalize_task_map(warmup);
            m.active_pending_revocation.clear();
            m.assignment_state = StreamsMemberAssignmentState::Stable;
        }
    }
}

/// Sorts and dedups every subtopology's partition list, then drops the
/// subtopology entries that end up empty. The function is idempotent.
fn normalize_task_map(mut map: BTreeMap<String, Vec<i32>>) -> BTreeMap<String, Vec<i32>> {
    map.retain(|_, parts| {
        parts.sort_unstable();
        parts.dedup();
        !parts.is_empty()
    });
    map
}

/// Splits a member's currently-owned active tasks against its new active
/// target. The function *keeps* tasks that are in both, and *revokes* tasks
/// the member owns that the target no longer holds. It normalizes both halves:
/// sorted, deduped, and with empty entries dropped.
fn compute_active_revoke_split(
    current: &BTreeMap<String, Vec<i32>>,
    target: &BTreeMap<String, Vec<i32>>,
) -> (BTreeMap<String, Vec<i32>>, BTreeMap<String, Vec<i32>>) {
    let mut revoke: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    let mut keep: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for (sub, parts) in current {
        let target_set: std::collections::HashSet<i32> =
            target.get(sub).into_iter().flatten().copied().collect();
        for &p in parts {
            if target_set.contains(&p) {
                keep.entry(sub.clone()).or_default().push(p);
            } else {
                revoke.entry(sub.clone()).or_default().push(p);
            }
        }
    }
    (normalize_task_map(keep), normalize_task_map(revoke))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn task_map(entries: &[(&str, &[i32])]) -> BTreeMap<String, Vec<i32>> {
        entries
            .iter()
            .map(|(sub, parts)| (sub.to_string(), parts.to_vec()))
            .collect()
    }

    #[test]
    fn add_member_marks_dirty_first_time() {
        let mut g = StreamsGroupState::new("g");
        assert!(!g.dirty);
        g.add_or_update_member(StreamsMemberState::joining("m1", "c1", "h1"));
        assert!(g.members.len() == 1);
        assert!(g.dirty);
    }

    #[test]
    fn re_add_identical_member_keeps_clean() {
        let mut g = StreamsGroupState::new("g");
        g.add_or_update_member(StreamsMemberState::joining("m1", "c1", "h1"));
        g.dirty = false;
        // Re-add a member with the same id and same topology epoch.
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.topology_epoch = 0;
        g.add_or_update_member(m);
        assert!(!g.dirty);
    }

    #[test]
    fn topology_epoch_change_marks_dirty() {
        let mut g = StreamsGroupState::new("g");
        g.add_or_update_member(StreamsMemberState::joining("m1", "c1", "h1"));
        g.dirty = false;
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.topology_epoch = 3;
        g.add_or_update_member(m);
        assert!(g.dirty);
    }

    #[test]
    fn remove_member_marks_dirty() {
        let mut g = StreamsGroupState::new("g");
        g.add_or_update_member(StreamsMemberState::joining("m1", "c1", "h1"));
        g.dirty = false;
        let removed = g.remove_member("m1");
        assert!(removed.is_some());
        assert!(g.dirty);
        // Removing a now-absent member does not re-dirty.
        g.dirty = false;
        assert!(g.remove_member("m1").is_none());
        assert!(!g.dirty);
    }

    #[test]
    fn bump_epoch_increments_and_dirties() {
        let mut g = StreamsGroupState::new("g");
        g.dirty = false;
        g.bump_epoch();
        assert!(g.group_epoch == 1);
        assert!(g.dirty);
    }

    #[test]
    fn evict_expired_removes_and_returns_ids() {
        let mut g = StreamsGroupState::new("g");
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        // Anchor `last_seen` at "now"; evaluate eviction slightly in the future
        // so we never subtract from an `Instant` (underflows on low-uptime CI).
        m.last_seen = Instant::now();
        g.add_or_update_member(m);
        g.add_or_update_member(StreamsMemberState::joining("m2", "c1", "h1"));
        g.dirty = false;

        // Within the timeout: nothing evicted, stays clean.
        let recent = Instant::now() + Duration::from_millis(50);
        let kept = g.evict_expired(recent, Duration::from_secs(45));
        check!(kept.is_empty());
        check!(g.members.len() == 2);
        check!(!g.dirty);

        // Timeout shrinks below the silence: both overdue, dirty flips.
        let later = Instant::now() + Duration::from_millis(50);
        let mut evicted = g.evict_expired(later, Duration::from_millis(1));
        evicted.sort();
        check!(evicted == vec!["m1".to_string(), "m2".to_string()]);
        check!(g.members.is_empty());
        check!(g.dirty);
    }

    #[test]
    fn install_target_moves_vanished_active_to_pending_and_keeps_kept() {
        let mut g = StreamsGroupState::new("g");
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.active = task_map(&[("sub0", &[0, 1, 2])]);
        g.add_or_update_member(m);
        g.group_epoch = 7;

        // New active target keeps {0,1} and drops {2}.
        let mut target = StreamsTargetAssignment::default();
        target
            .active
            .insert("m1".to_string(), task_map(&[("sub0", &[0, 1])]));
        g.install_target(target);

        let m = &g.members["m1"];
        check!(g.assignment_epoch == 7);
        check!(g.target.epoch == 7);
        check!(m.active == task_map(&[("sub0", &[0, 1])]));
        check!(m.active_pending_revocation == task_map(&[("sub0", &[2])]));
        check!(m.assignment_state == StreamsMemberAssignmentState::UnrevokedActiveTasks);
    }

    #[test]
    fn install_target_with_no_revocation_stays_stable() {
        let mut g = StreamsGroupState::new("g");
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.active = task_map(&[("sub0", &[0, 1])]);
        g.add_or_update_member(m);
        g.group_epoch = 2;

        let mut target = StreamsTargetAssignment::default();
        // Target is a superset — nothing to revoke.
        target
            .active
            .insert("m1".to_string(), task_map(&[("sub0", &[0, 1, 2])]));
        g.install_target(target);

        let m = &g.members["m1"];
        // Kept = intersection of current and target = {0,1}; the new {2} is not
        // installed until the member advances its epoch.
        check!(m.active == task_map(&[("sub0", &[0, 1])]));
        check!(m.active_pending_revocation.is_empty());
        check!(m.assignment_state == StreamsMemberAssignmentState::Stable);
    }

    #[test]
    fn advance_member_epoch_installs_target_clears_pending_sets_stable() {
        let mut g = StreamsGroupState::new("g");
        let mut m = StreamsMemberState::joining("m1", "c1", "h1");
        m.active = task_map(&[("sub0", &[0, 1, 2])]);
        g.add_or_update_member(m);
        g.group_epoch = 9;

        let mut target = StreamsTargetAssignment::default();
        target
            .active
            .insert("m1".to_string(), task_map(&[("sub0", &[0, 1])]));
        target
            .standby
            .insert("m1".to_string(), task_map(&[("sub1", &[3])]));
        target
            .warmup
            .insert("m1".to_string(), task_map(&[("sub2", &[4, 5])]));
        g.install_target(target);

        // After install the member is mid-revocation.
        assert!(
            g.members["m1"].assignment_state == StreamsMemberAssignmentState::UnrevokedActiveTasks
        );

        g.advance_member_epoch("m1");
        let m = &g.members["m1"];
        check!(m.member_epoch == 9);
        check!(m.previous_member_epoch == 0);
        check!(m.active == task_map(&[("sub0", &[0, 1])]));
        check!(m.standby == task_map(&[("sub1", &[3])]));
        check!(m.warmup == task_map(&[("sub2", &[4, 5])]));
        check!(m.active_pending_revocation.is_empty());
        check!(m.assignment_state == StreamsMemberAssignmentState::Stable);
    }

    #[test]
    fn group_state_phase_as_str_strings() {
        for (phase, want) in [
            (StreamsGroupStatePhase::Empty, "Empty"),
            (StreamsGroupStatePhase::NotReady, "NotReady"),
            (StreamsGroupStatePhase::Assigning, "Assigning"),
            (StreamsGroupStatePhase::Reconciling, "Reconciling"),
            (StreamsGroupStatePhase::Stable, "Stable"),
        ] {
            assert!(phase.as_str() == want);
        }
        assert!(StreamsGroupStatePhase::default() == StreamsGroupStatePhase::Empty);
    }

    #[test]
    fn assignment_state_i8_roundtrips() {
        for s in [
            StreamsMemberAssignmentState::Stable,
            StreamsMemberAssignmentState::UnrevokedActiveTasks,
            StreamsMemberAssignmentState::UnreleasedActiveTasks,
        ] {
            assert!(StreamsMemberAssignmentState::from_i8(s.as_i8()) == Some(s));
        }
        assert!(StreamsMemberAssignmentState::from_i8(99).is_none());
        assert!(StreamsMemberAssignmentState::default() == StreamsMemberAssignmentState::Stable);
    }

    #[test]
    fn normalize_sorts_dedups_and_drops_empty() {
        let m = normalize_task_map(task_map(&[("sub0", &[2, 0, 1, 1]), ("sub1", &[])]));
        assert!(m == task_map(&[("sub0", &[0, 1, 2])]));
    }
}
