//! Anomaly rules. Each rule inspects a `RuleCtx` and returns a list of
//! currently-active anomalies. The tick loop diffs each rule's output
//! against the open set in `AnomalyStore` to compute new vs. updated
//! vs. resolved transitions.

pub mod broker_death;
pub mod disk_pressure;
pub mod slow_broker;
pub mod under_replicated;

pub use broker_death::BrokerDeath;
pub use disk_pressure::DiskPressure;
pub use slow_broker::SlowBroker;
pub use under_replicated::UnderReplicatedPartitions;

use super::DetectorConfig;
use crate::{
    capacity::BrokerCapacities,
    detector::{AnomalyKey, AnomalyKind, AnomalySeverity, SnapshotHistory},
    model::ClusterState,
    scraper::UsageStore,
};

/// Rule input — passed to every rule on each tick. Borrowed because
/// rules don't own state (the tick loop owns the history + stores).
pub struct RuleCtx<'a> {
    pub snapshot: &'a ClusterState,
    pub history: &'a SnapshotHistory,
    pub usages: &'a UsageStore,
    pub capacities: &'a BrokerCapacities,
    pub now_ms: i64,
    pub cfg: &'a DetectorConfig,
}

/// A single anomaly detection rule.
///
/// Implementations return ALL currently-active anomalies of their kind
/// — not "newly fired". The tick loop computes new/resolved transitions
/// by diffing against `AnomalyStore`.
pub trait Rule: Send + Sync {
    fn kind(&self) -> AnomalyKind;
    fn evaluate(&self, ctx: &RuleCtx<'_>) -> Vec<RuleHit>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuleHit {
    pub key: AnomalyKey,
    pub severity: AnomalySeverity,
    pub details: String,
}
