//! Proposal + Movement types. Mirrors the proto definitions but owned
//! by the model layer so the optimizer + goals don't depend on
//! generated code.

use crabka_units::{ByteRate, convert::ByteRateExt};
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

/// `serde` default for [`Proposal::throttle`] — no throttle applied.
fn zero_byte_rate() -> ByteRate {
    ByteRate::ZERO
}

/// `PartialEq` but not `Eq`: [`Proposal::throttle`] is an `f64`-backed
/// quantity, so equality is not reflexive over the whole domain.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// executor applied). Zero otherwise.
    #[serde(
        default = "zero_byte_rate",
        with = "crabka_units::serde_units::numeric::bytes_per_sec_i64"
    )]
    pub throttle: ByteRate,
}

#[cfg(test)]
mod tests {
    use crabka_units::mebibytes_per_sec;

    use super::*;

    fn proposal(throttle: ByteRate) -> Proposal {
        Proposal {
            id: "p1".into(),
            status: ProposalStatus::Executing,
            created_at_ms: 7,
            goals_applied: vec!["RackAware".into()],
            summary: ProposalSummary::default(),
            movements: vec![],
            started_at_ms: 8,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle,
        }
    }

    #[test]
    fn status_terminal_flags() {
        for (status, want) in [
            (ProposalStatus::Computed, false),
            (ProposalStatus::Executing, false),
            (ProposalStatus::Completed, true),
            (ProposalStatus::Failed, true),
            (ProposalStatus::Cancelled, true),
        ] {
            assert2::assert!(status.is_terminal() == want);
        }
    }

    #[test]
    fn throttle_serialises_as_bytes_per_sec_integer() {
        let json = serde_json::to_value(proposal(mebibytes_per_sec(8))).unwrap();
        assert2::assert!(json["throttle"] == serde_json::json!(8 * 1024 * 1024));
    }

    #[test]
    fn throttle_round_trips_through_json() {
        for rate in [
            ByteRate::ZERO,
            mebibytes_per_sec(8),
            ByteRate::from_bytes_per_sec(50_000_000),
        ] {
            let want = proposal(rate);
            let json = serde_json::to_string(&want).unwrap();
            let got: Proposal = serde_json::from_str(&json).unwrap();
            assert2::assert!(got == want);
        }
    }

    #[test]
    fn missing_throttle_defaults_to_zero() {
        let json = r#"{
            "id": "p1",
            "status": "Computed",
            "created_at_ms": 0,
            "goals_applied": [],
            "summary": {
                "replica_movements": 0,
                "leader_movements": 0,
                "max_replicas_before": 0,
                "max_replicas_after": 0,
                "max_leaders_before": 0,
                "max_leaders_after": 0
            },
            "movements": []
        }"#;
        let parsed: Proposal = serde_json::from_str(json).unwrap();
        assert2::assert!(parsed.throttle == ByteRate::ZERO);
    }
}
