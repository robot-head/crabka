//! Proposal + Movement types. Mirrors the proto definitions but owned
//! by the model layer so the optimizer + goals don't depend on
//! generated code.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Movement {
    pub topic: String,
    pub partition: i32,
    pub old_replicas: Vec<i32>,
    pub new_replicas: Vec<i32>,
    pub old_leader: i32,
    pub new_leader: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalStatus {
    Computed,
    Executing,
    Completed,
    Failed,
    Cancelled,
}

impl ProposalStatus {
    /// True if the status is a final state (no further transitions).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ProposalStatus::Completed | ProposalStatus::Failed | ProposalStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProposalSummary {
    pub replica_movements: i32,
    pub leader_movements: i32,
    pub max_replicas_before: i32,
    pub max_replicas_after: i32,
    pub max_leaders_before: i32,
    pub max_leaders_after: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub id: String,
    pub status: ProposalStatus,
    pub created_at_ms: i64,
    pub goals_applied: Vec<String>,
    pub summary: ProposalSummary,
    pub movements: Vec<Movement>,
    /// Set when transitioning to `Executing`; 0 otherwise.
    #[serde(default)]
    pub started_at_ms: i64,
    /// Set when transitioning to a terminal status; 0 otherwise.
    #[serde(default)]
    pub terminated_at_ms: i64,
    /// Set on `Failed`. None otherwise.
    #[serde(default)]
    pub failure_reason: Option<String>,
    /// Set when transitioning to `Executing` (echoes the throttle the
    /// executor applied). 0 otherwise.
    #[serde(default)]
    pub throttle_bytes_per_sec: i64,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn status_terminal_flags() {
        for (status, want) in [
            (ProposalStatus::Computed, false),
            (ProposalStatus::Executing, false),
            (ProposalStatus::Completed, true),
            (ProposalStatus::Failed, true),
            (ProposalStatus::Cancelled, true),
        ] {
            assert!(status.is_terminal() == want, "status {status:?}");
        }
    }
}
