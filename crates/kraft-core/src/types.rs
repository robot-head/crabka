//! Core data types for the `KRaft` consensus state machine (KIP-595/996).
//! Pure, sans-IO: no clock, no wire, no log bytes.

pub use crabka_voters::NodeId;
use crabka_voters::VoterSet;
use uuid::Uuid;

/// A simulated or logical instant in milliseconds.
///
/// The caller always injects the time. The state machine never reads the
/// system clock, which keeps it deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimInstant(pub u64);

impl SimInstant {
    #[must_use]
    pub fn saturating_add_ms(self, ms: u64) -> Self {
        Self(self.0.saturating_add(ms))
    }
}

/// Consensus epoch, always non-negative; the wire leader epoch is
/// `crabka_ids::LeaderEpoch`.
pub type Epoch = u32;

/// Identifies a voter by node id and directory id, as Kafka's `ReplicaKey` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplicaKey {
    pub id: NodeId,
    pub directory_id: Uuid,
}

/// A log position: an offset together with the leader epoch that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogOffsetMetadata {
    pub offset: i64,
    pub epoch: Epoch,
}

/// Read-only view of the local replicated log that the state machine uses.
///
/// Production uses the real `crabka-log`-backed implementation; tests supply a fake.
pub trait LogView {
    /// The log end offset: one offset after the last appended record.
    fn end_offset(&self) -> i64;
    /// Leader epoch of the last appended record. An empty log gives 0.
    fn last_epoch(&self) -> Epoch;
    /// The end offset for `epoch`: the offset of the first record with a
    /// strictly greater epoch, or `end_offset()` if there is no such record.
    ///
    /// The state machine uses this value to compute the diverging-epoch hint.
    /// This method returns `None` if `epoch` is unknown, that is, greater than
    /// the last epoch.
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64>;
}

/// The durable quorum state: the logical content of the `quorum-state` file.
///
/// This is the in-memory model. The log layer owns the file persistence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuorumState {
    pub cluster_id: Uuid,
    /// Finalized `KRaft` protocol feature level. Level 0 uses configured static
    /// voters; level 1 persists membership in Raft control records.
    pub kraft_version: u16,
    pub leader_epoch: Epoch,
    pub leader_id: Option<NodeId>,
    pub voted_key: Option<ReplicaKey>,
    pub voters: VoterSet,
}

impl QuorumState {
    #[must_use]
    pub fn bootstrap(cluster_id: Uuid, voters: VoterSet) -> Self {
        Self {
            cluster_id,
            kraft_version: 0,
            leader_epoch: 0,
            leader_id: None,
            voted_key: None,
            voters,
        }
    }

    /// Majority size for the current voter set: `floor(n/2) + 1`.
    #[must_use]
    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn quorum_state_starts_unattached_at_epoch_zero() {
        let voters = test_voter_set(&[NodeId(1), NodeId(2), NodeId(3)]);
        let qs = QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone());
        assert2::assert!(
            qs == QuorumState {
                cluster_id: uuid::Uuid::nil(),
                kraft_version: 0,
                leader_epoch: 0,
                leader_id: None,
                voted_key: None,
                voters,
            }
        );
        assert2::assert!(qs.voters.contains(NodeId(2)));
    }

    pub(crate) fn test_voter_set(ids: &[NodeId]) -> crabka_voters::VoterSet {
        crabka_voters::VoterSet::from_voters(ids.iter().map(|&id| crabka_voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: Vec::new(),
            kraft_version: crabka_voters::KRaftVersionRange::default(),
        }))
    }
}
