//! Outputs from the consensus state machine. They are pure side-effect
//! descriptions executed by the engine.

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
    /// Reply to a Vote request. (Kafka's `VoteResponse` carries no pre-vote
    /// flag — the candidate matches the reply to its round by its own role, so
    /// the responder does not echo `pre_vote`.)
    ReplyVote {
        to: NodeId,
        epoch: LeaderEpoch,
        granted: bool,
    },
    /// New leader announces its epoch to all voters.
    SendBeginQuorumEpoch { epoch: LeaderEpoch },
    /// Resigning leader tells voters to elect.
    // Emitted by transport-facing paths alongside `Role::Resigned`; the core
    // also receives `EndQuorumEpoch`.
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
