//! Inputs to the consensus state machine.

use crate::kraft::types::{LeaderEpoch, NodeId};

/// A peer's view of its log tip, carried in Vote/Fetch requests.
#[derive(Debug, Clone, Copy)]
pub struct LogEnd {
    pub last_epoch: LeaderEpoch,
    pub last_offset: i64,
}

#[derive(Debug, Clone)]
pub enum Event {
    /// The election timer fired.
    ElectionTimeout,
    /// The fetch timer fired (follower/observer lost contact with the leader).
    FetchTimeout,
    /// A peer asks us for our vote.
    ReceiveVoteRequest {
        from: NodeId,
        candidate_epoch: LeaderEpoch,
        candidate: NodeId,
        candidate_log_end: LogEnd,
        pre_vote: bool,
    },
    /// A peer answered our Vote.
    ReceiveVoteResponse {
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
        pre_vote: bool,
    },
    /// A leader announces its epoch to us.
    ReceiveBeginQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: LeaderEpoch,
    },
    /// A resigning leader tells us to start an election.
    ReceiveEndQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: LeaderEpoch,
    },
    /// (Leader side) a follower fetched at this position.
    ReceiveFetch {
        from: NodeId,
        fetch_epoch: LeaderEpoch,
        fetch_offset: i64,
    },
    /// (Follower side) the leader answered our Fetch.
    ReceiveFetchResponse {
        leader_id: NodeId,
        leader_epoch: LeaderEpoch,
        /// Set when the leader signalled log divergence.
        diverging: Option<crate::kraft::types::LogOffsetMetadata>,
    },
}
