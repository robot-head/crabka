//! Unified `Group` container (KIP-848 64d-B).
//!
//! One in-memory model for a consumer group regardless of which protocol its
//! members speak. A `Group` is a discriminated container over the two existing,
//! tested state machines — the classic 5-state machine ([`ClassicState`]) and
//! the next-gen epoch machine ([`ConsumerState`]) — so the unified coordinator
//! and persistence path can hold either behind one type.
//!
//! In this slice (64d-B) a group is single-type for its lifetime: the `kind`
//! is chosen when the actor is spawned and never flipped. Slices 64d-C..E
//! replace `GroupKind` with a single member list that holds classic *and*
//! consumer members simultaneously (live migration); this container is the seam
//! that localizes that change.

// The state machines are reused verbatim. They are physically relocated under
// `unified/` in B5 once the classic handlers stop reaching the old
// `GroupManager`; until then these aliases give the unified surface its types
// without churning the still-live classic/next-gen call sites.
pub(crate) use crate::coordinator::group::Group as ClassicState;
pub(crate) use crate::coordinator::next_gen::group_state::GroupState as ConsumerState;

/// Which protocol a [`Group`]'s members speak. The variant carries that
/// protocol's full state machine.
pub(crate) enum GroupKind {
    /// Classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` group.
    Classic(ClassicState),
    /// KIP-848 `ConsumerGroupHeartbeat` group.
    Consumer(ConsumerState),
}

/// A consumer group in the unified coordinator.
pub(crate) struct Group {
    pub group_id: String,
    pub kind: GroupKind,
}

impl Group {
    /// A fresh, empty classic group.
    pub fn new_classic(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Classic(ClassicState::new(group_id.clone())),
            group_id,
        }
    }

    /// A fresh, empty next-gen (consumer-protocol) group.
    pub fn new_consumer(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Consumer(ConsumerState::new(group_id.clone())),
            group_id,
        }
    }

    /// `true` if this group speaks the classic protocol.
    pub fn is_classic(&self) -> bool {
        matches!(self.kind, GroupKind::Classic(_))
    }

    /// `true` if this group speaks the next-gen protocol.
    pub fn is_consumer(&self) -> bool {
        matches!(self.kind, GroupKind::Consumer(_))
    }

    pub fn as_classic(&self) -> Option<&ClassicState> {
        match &self.kind {
            GroupKind::Classic(s) => Some(s),
            GroupKind::Consumer(_) => None,
        }
    }

    pub fn as_classic_mut(&mut self) -> Option<&mut ClassicState> {
        match &mut self.kind {
            GroupKind::Classic(s) => Some(s),
            GroupKind::Consumer(_) => None,
        }
    }

    pub fn as_consumer(&self) -> Option<&ConsumerState> {
        match &self.kind {
            GroupKind::Consumer(s) => Some(s),
            GroupKind::Classic(_) => None,
        }
    }

    pub fn as_consumer_mut(&mut self) -> Option<&mut ConsumerState> {
        match &mut self.kind {
            GroupKind::Consumer(s) => Some(s),
            GroupKind::Classic(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn classic_container_exposes_classic_state_only() {
        let mut g = Group::new_classic("g");
        assert!(g.is_classic());
        assert!(!g.is_consumer());
        assert!(g.as_classic().is_some());
        assert!(g.as_consumer().is_none());
        assert!(g.as_classic_mut().is_some());
        assert!(g.group_id == "g");
    }

    #[test]
    fn consumer_container_exposes_consumer_state_only() {
        let mut g = Group::new_consumer("g");
        assert!(g.is_consumer());
        assert!(!g.is_classic());
        assert!(g.as_consumer().is_some());
        assert!(g.as_classic().is_none());
        assert!(g.as_consumer_mut().is_some());
        assert!(g.group_id == "g");
    }
}
