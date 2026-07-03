//! Outputs from the consensus state machine. They are pure side-effect
//! descriptions executed by the engine.

use crate::types::{Epoch, LogOffsetMetadata, NodeId, SimInstant};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimerKind {
    Election,
    Fetch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// Broadcast a Vote request to all other voters (pre- or real vote).
    SendVoteRequest { epoch: Epoch, pre_vote: bool },
    /// Reply to a Vote request. (Kafka's `VoteResponse` carries no pre-vote
    /// flag — the candidate matches the reply to its round by its own role, so
    /// the responder does not echo `pre_vote`.)
    ReplyVote {
        to: NodeId,
        epoch: Epoch,
        granted: bool,
    },
    /// New leader announces its epoch to all voters.
    SendBeginQuorumEpoch { epoch: Epoch },
    /// Resigning leader tells voters to elect.
    // Emitted by transport-facing paths alongside `Role::Resigned`; the core
    // also receives `EndQuorumEpoch`.
    SendEndQuorumEpoch { epoch: Epoch },
    /// Follower/observer should fetch from this leader.
    SendFetch { leader_id: NodeId },
    /// We changed role (carries the new role name for observability/tests).
    TransitionedTo(&'static str),
    /// Persist the durable quorum state (epoch/votedKey/leaderId changed).
    PersistQuorumState,
    /// As new leader, append the `LeaderChange` control record for `epoch`.
    AppendLeaderChange { epoch: Epoch },
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
