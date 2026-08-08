//! Anomaly rules. Each rule inspects a `RuleCtx` and returns a list of the
//! anomalies that are active now. The tick loop diffs each rule's output
//! against the open set in `AnomalyStore`, and from that it computes the new,
//! updated, and resolved transitions.

pub mod broker_death;
pub mod disk_pressure;
pub mod slow_broker;
pub mod under_replicated;

pub use broker_death::BrokerDeath;
use crabka_units::{Time, convert::TimeExt as _};
pub use disk_pressure::DiskPressure;
pub use slow_broker::SlowBroker;
pub use under_replicated::UnderReplicatedPartitions;

use super::{DetectorConfig, SnapshotMemo};
use crate::{
    capacity::BrokerCapacities,
    detector::{AnomalyKey, AnomalyKind, AnomalySeverity, SnapshotHistory},
    model::ClusterState,
    scraper::UsageStore,
};

/// Rule input. The tick loop passes it to every rule on each tick. It is
/// borrowed because rules own no state: the tick loop owns the history and the
/// stores.
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
/// Implementations return ALL anomalies of their kind that are active now,
/// not only the newly fired ones. The tick loop computes the new and resolved
/// transitions by diffing against `AnomalyStore`.
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

pub(super) fn sustained_memo<'a>(
    ctx: &'a RuleCtx<'_>,
    threshold: Time,
) -> Option<&'a SnapshotMemo> {
    let cutoff = ctx.now_ms.saturating_sub(threshold.millis_i64());
    ctx.history
        .oldest_since(cutoff)
        .filter(|memo| memo.snapshot_at_ms <= cutoff)
}
