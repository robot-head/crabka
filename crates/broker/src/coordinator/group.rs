//! `Group` — per-`group_id` state machine. Pure data + transitions; the
//! coordinator handlers (Tasks 6–12) hold the mutex around it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

/// Five-state machine for a consumer group, matching the Apache Kafka
/// classic protocol (KIP-62 / KIP-394).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub client_id: String,
    pub host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub last_heartbeat: Instant,
    /// Encoded `ConsumerProtocolSubscription` bytes (a `subscription` field
    /// from `JoinGroupRequest`). Opaque to the broker.
    pub protocol_metadata: Bytes,
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
        protocol_metadata: Bytes,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            client_id: client_id.into(),
            host: host.into(),
            session_timeout,
            rebalance_timeout,
            last_heartbeat: Instant::now(),
            protocol_metadata,
            assignment: None,
        }
    }
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

#[derive(Debug)]
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
    pub committed_offsets: HashMap<(String, i32), OffsetEntry>,
    pub rebalance_deadline: Option<Instant>,
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
            committed_offsets: HashMap::new(),
            rebalance_deadline: None,
        }
    }

    /// Add or refresh a member. Transitions to `PreparingRebalance` if
    /// currently `Empty` or `Stable`.
    pub fn add_member(&mut self, member: Member) {
        let was_first_join = matches!(self.state, GroupState::Empty | GroupState::Stable);
        self.members.insert(member.member_id.clone(), member);
        if was_first_join {
            self.state = GroupState::PreparingRebalance;
        }
    }

    /// Remove a member; transitions to `Empty` if no members remain.
    pub fn remove_member(&mut self, member_id: &str) {
        self.members.remove(member_id);
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

    /// Drop any member whose `last_heartbeat` is older than its
    /// `session_timeout`. Returns the dropped member IDs. Transitions to
    /// `PreparingRebalance` if any were dropped and the group still has
    /// members; to `Empty` if it became empty.
    pub fn expire_dead_members(&mut self, now: Instant) -> Vec<String> {
        let dropped: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_heartbeat) > m.session_timeout)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dropped {
            self.members.remove(id);
        }
        if !dropped.is_empty() {
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.leader_id = None;
                self.protocol_name = None;
            } else {
                self.state = GroupState::PreparingRebalance;
            }
        }
        dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_member(id: &str) -> Member {
        Member::new(
            id,
            "test-client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            Bytes::new(),
        )
    }

    #[test]
    fn empty_to_preparing_on_first_join() {
        let mut g = Group::new("g");
        assert_eq!(g.state, GroupState::Empty);
        g.add_member(sample_member("m1"));
        assert_eq!(g.state, GroupState::PreparingRebalance);
    }

    #[test]
    fn complete_rebalance_bumps_generation() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.add_member(sample_member("m2"));
        g.complete_rebalance("range");
        assert_eq!(g.generation_id, 1);
        assert_eq!(g.leader_id.as_deref(), Some("m1"));
        assert_eq!(g.protocol_name.as_deref(), Some("range"));
        assert_eq!(g.state, GroupState::CompletingRebalance);
    }

    #[test]
    fn install_assignments_to_stable() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.complete_rebalance("range");
        let mut a = HashMap::new();
        a.insert("m1".into(), Bytes::from_static(b"assignment-bytes"));
        g.install_assignments(a);
        assert_eq!(g.state, GroupState::Stable);
        assert!(g.members["m1"].assignment.is_some());
    }

    #[test]
    fn remove_last_member_empties_group() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.remove_member("m1");
        assert_eq!(g.state, GroupState::Empty);
        assert!(g.leader_id.is_none());
    }

    #[test]
    fn expire_dead_members_drops_stale() {
        let mut g = Group::new("g");
        let mut m = sample_member("m1");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        g.add_member(m);
        let dropped = g.expire_dead_members(Instant::now());
        assert_eq!(dropped, vec!["m1".to_string()]);
        assert_eq!(g.state, GroupState::Empty);
    }
}
