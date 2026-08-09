//! Inputs to the consensus state machine.

use crate::types::{Epoch, NodeId};

/// A peer's view of its log tip, carried in Vote/Fetch requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogEnd {
    pub last_epoch: Epoch,
    pub last_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Event {
    /// The election timer fired.
    ElectionTimeout,
    /// The fetch timer fired.
    ///
    /// The follower or observer lost contact with the leader.
    FetchTimeout,
    /// A peer asks us for our vote.
    ReceiveVoteRequest {
        from: NodeId,
        /// The recipient of this Vote, that is, the wire top-level `voterId`.
        ///
        /// KIP-595 and Kafka's `KafkaRaftClient` check that an incoming Vote
        /// targets this node before they consider the grant. They reject a
        /// request that is addressed to a different voter.
        voter_id: NodeId,
        candidate_epoch: Epoch,
        candidate: NodeId,
        candidate_log_end: LogEnd,
        pre_vote: bool,
    },
    /// A peer answered our Vote.
    ///
    /// The wire does NOT carry the round, that is, pre-vote against real vote.
    /// The candidate infers the round from its own `Prospective` or `Candidate`
    /// role and epoch (KIP-996). This matches Kafka's field-less
    /// `VoteResponse`.
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
    /// Leader side: a follower fetched at this position.
    ReceiveFetch {
        from: NodeId,
        fetch_epoch: Epoch,
        fetch_offset: i64,
    },
    /// Follower side: the leader answered our Fetch.
    ReceiveFetchResponse {
        leader_id: NodeId,
        leader_epoch: Epoch,
        /// Holds a value when the leader signalled log divergence.
        diverging: Option<crate::types::LogOffsetMetadata>,
    },
}
