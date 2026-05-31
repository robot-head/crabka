//! Outputs from the consensus state machine, executed by 3b/3c. In slice 3a
//! they are only inspected by tests. Minimal scaffold; full action set lands
//! in Task 2.

/// Outputs from the consensus state machine. Expanded in Task 2.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Persist the durable quorum state (epoch/votedKey/leaderId changed).
    PersistQuorumState,
}
