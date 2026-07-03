//! Core data types for the `KRaft` consensus state machine (KIP-595/996).
//! Pure, sans-IO: no clock, no wire, no log bytes.

pub use crabka_voters::NodeId;
use crabka_voters::VoterSet;
use uuid::Uuid;

/// A simulated/logical instant in milliseconds. Time is always injected, never
/// read from the system clock (keeps the state machine deterministic).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SimInstant(pub u64);

impl SimInstant {
    #[must_use]
    pub fn saturating_add_ms(self, ms: u64) -> Self {
        Self(self.0.saturating_add(ms))
    }
}

/// Consensus epoch (always non-negative); the wire leader epoch is
/// `crabka_ids::LeaderEpoch`.
pub type Epoch = u32;

/// Identifies a voter by node id + directory id (Kafka's `ReplicaKey`).
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

/// Read-only view of the local replicated log the state machine reasons about.
/// Production uses the real `crabka-log`-backed implementation; tests supply a fake.
pub trait LogView {
    /// Offset one past the last appended record (the log end offset).
    fn end_offset(&self) -> i64;
    /// Leader epoch of the last appended record (0 for an empty log).
    fn last_epoch(&self) -> Epoch;
    /// The end offset for `epoch`: the offset of the first record with a
    /// strictly greater epoch, or `end_offset()` if none. Used to compute the
    /// diverging-epoch hint. Returns `None` if `epoch` is unknown (> last).
    fn end_offset_for_epoch(&self, epoch: Epoch) -> Option<i64>;
}

/// The durable quorum state — the logical content of the `quorum-state` file.
/// This is the in-memory model; file persistence is owned by the log layer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuorumState {
    pub cluster_id: Uuid,
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
            leader_epoch: 0,
            leader_id: None,
            voted_key: None,
            voters,
        }
    }

    /// Majority size for the current voter set (`floor(n/2) + 1`).
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
        assert!(
            qs == QuorumState {
                cluster_id: uuid::Uuid::nil(),
                leader_epoch: 0,
                leader_id: None,
                voted_key: None,
                voters,
            }
        );
        assert!(qs.voters.contains(NodeId(2)));
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
