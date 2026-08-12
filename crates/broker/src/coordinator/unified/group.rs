//! Unified `Group` container for KIP-848 migration.
//!
//! One in-memory model for a consumer group, whichever protocol its members
//! speak.
//!
//! A `Group` is a discriminated container over the two existing, tested state
//! machines: the classic 5-state machine ([`ClassicState`]) and the next-gen
//! epoch machine ([`ConsumerState`]). The unified coordinator and the
//! persistence path can therefore hold either one behind a single type.
//!
//! The actor can change the live kind during KIP-848 upgrade and downgrade.
//! The container keeps both protocol-specific state machines behind one surface
//! so the coordinator and persistence code always use the live state.

// The state machines are reused verbatim, relocated under `unified/`. These
// aliases give the unified surface its types without renaming the moved code
// (the classic file keeps its internal `Group`/`GroupState` names).
use std::collections::HashMap;

pub(crate) use crate::coordinator::unified::{
    classic_state::{ClassicGroup as ClassicState, OffsetEntry},
    consumer_state::GroupState as ConsumerState,
};

/// Which protocol the members of a [`Group`] speak. The variant carries that
/// protocol's full state machine.
#[derive(Debug)]
pub enum GroupKind {
    /// Classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` group.
    Classic(ClassicState),
    /// KIP-848 `ConsumerGroupHeartbeat` group.
    Consumer(ConsumerState),
}

/// A consumer group in the unified coordinator.
#[derive(Debug)]
pub struct CoordinatorGroup {
    pub group_id: String,
    pub kind: GroupKind,
    /// Committed offsets, from `__consumer_offsets` k0 and k1.
    ///
    /// They do not depend on the protocol. A group's offsets key by
    /// `(topic, partition)` whichever protocol its members speak, so they live
    /// on the container instead of inside either state machine. A later type
    /// change, in slices C to E, can therefore carry the committed offsets
    /// through a conversion untouched.
    pub committed_offsets: HashMap<(String, i32), OffsetEntry>,
}

impl CoordinatorGroup {
    /// A fresh, empty classic group.
    pub fn new_classic(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Classic(ClassicState::new(group_id.clone())),
            group_id,
            committed_offsets: HashMap::new(),
        }
    }

    /// A fresh, empty next-gen group, on the consumer protocol.
    pub fn new_consumer(group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        Self {
            kind: GroupKind::Consumer(ConsumerState::new(group_id.clone())),
            group_id,
            committed_offsets: HashMap::new(),
        }
    }

    /// Returns `true` if this group speaks the classic protocol.
    pub fn is_classic(&self) -> bool {
        matches!(self.kind, GroupKind::Classic(_))
    }

    /// Returns `true` if this group speaks the next-gen protocol.
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

    /// Gives mutable access to the discriminated `kind`, so that a
    /// live-migration trigger can replace `Classic(..)` with `Consumer(..)` in
    /// place. This is the KIP-848 upgrade.
    pub fn kind_mut(&mut self) -> &mut GroupKind {
        &mut self.kind
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn classic_container_exposes_classic_state_only() {
        let mut g = CoordinatorGroup::new_classic("g");
        check!(g.is_classic());
        check!(!g.is_consumer());
        check!(g.as_classic().is_some());
        check!(g.as_consumer().is_none());
        check!(g.as_classic_mut().is_some());
        check!(g.group_id == "g");
    }

    #[test]
    fn consumer_container_exposes_consumer_state_only() {
        let mut g = CoordinatorGroup::new_consumer("g");
        check!(g.is_consumer());
        check!(!g.is_classic());
        check!(g.as_consumer().is_some());
        check!(g.as_classic().is_none());
        check!(g.as_consumer_mut().is_some());
        check!(g.group_id == "g");
    }
}
