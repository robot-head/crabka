//! The replica's volatile role within the current epoch and its per-role state.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{NodeId, SimInstant};

/// Per-follower replication progress that a leader tracks for the HWM.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ReplicaProgress {
    /// Highest offset the follower has acknowledged, that is, its fetch offset.
    pub fetch_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Role {
    /// Knows the epoch, no leader yet. May hold a non-binding pre-vote grant.
    Unattached { election_deadline: SimInstant },
    /// Cast a binding vote this epoch; now waits for a leader.
    Voted { election_deadline: SimInstant },
    /// Has a leader for the epoch and fetches from it.
    Follower {
        leader_id: NodeId,
        fetch_deadline: SimInstant,
    },
    /// KIP-996 pre-vote candidate that collects non-binding grants.
    Prospective {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
    },
    /// Real candidacy: the epoch is bumped and the replica voted for itself.
    Candidate {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
    },
    /// Won the election; tracks follower progress for HWM.
    Leader {
        replicas: BTreeMap<NodeId, ReplicaProgress>,
        high_watermark: i64,
        /// Log end offset at the moment of promotion, that is, where this
        /// leader's `LeaderChange` record sits. That record is the first
        /// current-epoch record.
        ///
        /// The HWM may only advance past this offset. This enforces Raft Fig.8
        /// leader completeness: a current-epoch entry must be
        /// majority-replicated before commit.
        epoch_start_offset: i64,
    },
    /// Steps down and emits `EndQuorumEpoch`.
    // `Resigned` (and `Action::SendEndQuorumEpoch`) are produced by
    // transport-facing paths; the core also receives `EndQuorumEpoch`, so there
    // is intentionally no transition into this variant yet.
    Resigned,
    /// Not in the voter set; only ever fetches.
    Observer {
        leader_id: Option<NodeId>,
        fetch_deadline: SimInstant,
    },
}

impl Default for Role {
    fn default() -> Self {
        Role::Unattached {
            election_deadline: SimInstant(0),
        }
    }
}

impl Role {
    #[must_use]
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Role::Unattached { .. } => "Unattached",
            Role::Voted { .. } => "Voted",
            Role::Follower { .. } => "Follower",
            Role::Prospective { .. } => "Prospective",
            Role::Candidate { .. } => "Candidate",
            Role::Leader { .. } => "Leader",
            Role::Resigned => "Resigned",
            Role::Observer { .. } => "Observer",
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    #[test]
    fn role_defaults_to_unattached() {
        let r = Role::default();
        assert2::assert!((matches!(r, Role::Unattached { .. }), r.is_leader()) == (true, false));
    }
}
