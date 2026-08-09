//! Outputs from the consensus state machine. They are pure side-effect
//! descriptions that the engine executes.

use crate::types::{Epoch, LogOffsetMetadata, NodeId, SimInstant};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimerKind {
    Election,
    Fetch,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// Broadcast a Vote request to all other voters, as a pre-vote or a real vote.
    SendVoteRequest { epoch: Epoch, pre_vote: bool },
    /// Reply to a Vote request.
    ///
    /// Kafka's `VoteResponse` carries no pre-vote flag. The candidate matches
    /// the reply to its round by its own role, so the responder does not echo
    /// `pre_vote`.
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
    /// The follower or observer should fetch from this leader.
    SendFetch { leader_id: NodeId },
    /// We changed role. The variant carries the new role name for
    /// observability and for tests.
    TransitionedTo(&'static str),
    /// Persist the durable quorum state: epoch, votedKey, or leaderId changed.
    PersistQuorumState,
    /// As new leader, append the `LeaderChange` control record for `epoch`.
    AppendLeaderChange { epoch: Epoch },
    /// Leader advanced the high watermark.
    AdvanceHighWatermark(i64),
    /// Follower must truncate its log to this diverging point.
    TruncateTo(LogOffsetMetadata),
    /// Arm or re-arm a timer to fire at `deadline`.
    ResetTimer {
        kind: TimerKind,
        deadline: SimInstant,
    },
}
