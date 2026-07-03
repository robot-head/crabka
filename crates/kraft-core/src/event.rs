//! Inputs to the consensus state machine.

use crate::types::{Epoch, NodeId};

/// A peer's view of its log tip, carried in Vote/Fetch requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogEnd {
    pub last_epoch: Epoch,
    pub last_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Event {
    /// The election timer fired.
    ElectionTimeout,
    /// The fetch timer fired (follower/observer lost contact with the leader).
    FetchTimeout,
    /// A peer asks us for our vote.
    ReceiveVoteRequest {
        from: NodeId,
        /// The recipient this Vote is addressed to (the wire top-level
        /// `voterId`). KIP-595 / Kafka's `KafkaRaftClient` validates that an
        /// incoming Vote targets this node before considering the grant; a
        /// request addressed to a different voter is rejected.
        voter_id: NodeId,
        candidate_epoch: Epoch,
        candidate: NodeId,
        candidate_log_end: LogEnd,
        pre_vote: bool,
    },
    /// A peer answered our Vote. The round (pre-vote vs real vote) is NOT on the
    /// wire — the candidate infers it from its own `Prospective`/`Candidate`
    /// role + epoch (KIP-996; mirrors Kafka's field-less `VoteResponse`).
    ReceiveVoteResponse {
        from: NodeId,
        epoch: Epoch,
        vote_granted: bool,
    },
    /// A leader announces its epoch to us.
    ReceiveBeginQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: Epoch,
    },
    /// A resigning leader tells us to start an election.
    ReceiveEndQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: Epoch,
    },
    /// (Leader side) a follower fetched at this position.
    ReceiveFetch {
        from: NodeId,
        fetch_epoch: Epoch,
        fetch_offset: i64,
    },
    /// (Follower side) the leader answered our Fetch.
    ReceiveFetchResponse {
        leader_id: NodeId,
        leader_epoch: Epoch,
        /// Set when the leader signalled log divergence.
        diverging: Option<crate::types::LogOffsetMetadata>,
    },
}
