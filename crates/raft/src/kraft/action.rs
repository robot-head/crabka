//! Outputs from the consensus state machine, executed by 3b/3c. In slice 3a
//! they are only inspected by tests.

use crate::kraft::types::{LeaderEpoch, LogOffsetMetadata, NodeId, SimInstant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerKind {
    Election,
    Fetch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Broadcast a Vote request to all other voters (pre- or real vote).
    SendVoteRequest { epoch: LeaderEpoch, pre_vote: bool },
    /// Reply to a Vote request.
    ReplyVote {
        to: NodeId,
        epoch: LeaderEpoch,
        granted: bool,
        pre_vote: bool,
    },
    /// New leader announces its epoch to all voters.
    SendBeginQuorumEpoch { epoch: LeaderEpoch },
    /// Resigning leader tells voters to elect.
    SendEndQuorumEpoch { epoch: LeaderEpoch },
    /// Follower/observer should fetch from this leader.
    SendFetch { leader_id: NodeId },
    /// We changed role (carries the new role name for observability/tests).
    TransitionedTo(&'static str),
    /// Persist the durable quorum state (epoch/votedKey/leaderId changed).
    PersistQuorumState,
    /// As new leader, append the `LeaderChange` control record for `epoch`.
    AppendLeaderChange { epoch: LeaderEpoch },
    /// Leader advanced the high watermark.
    AdvanceHighWatermark(i64),
    /// Follower must truncate its log to this diverging point.
    TruncateTo(LogOffsetMetadata),
    /// (Re)arm a timer to fire at `deadline`.
    ResetTimer {
        kind: TimerKind,
        deadline: SimInstant,
    },
}
