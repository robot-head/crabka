//! `BrokerDeath` rule — fires when a broker id present in some partition's
//! replicas list is missing from the cluster snapshot for at least
//! `cfg.broker_death_threshold`.

use std::collections::HashSet;

use super::{Rule, RuleCtx, RuleHit};
use crate::detector::{AnomalyKey, AnomalyKind, AnomalySeverity};

pub struct BrokerDeath;

impl Rule for BrokerDeath {
    fn kind(&self) -> AnomalyKind {
        AnomalyKind::BrokerDeath
    }

    fn evaluate(&self, ctx: &RuleCtx<'_>) -> Vec<RuleHit> {
        let live: HashSet<i32> = ctx.snapshot.brokers.iter().map(|b| b.id).collect();
        let expected: HashSet<i32> = ctx
            .snapshot
            .partitions
            .iter()
            .flat_map(|p| p.replicas.iter().copied())
            .collect();
        let missing_now: Vec<i32> = expected.difference(&live).copied().collect();
        if missing_now.is_empty() {
            return Vec::new();
        }

        let threshold_ms =
            i64::try_from(ctx.cfg.broker_death_threshold.as_millis()).unwrap_or(i64::MAX);
        let cutoff = ctx.now_ms.saturating_sub(threshold_ms);
        // Need at least one memo old enough to confirm the absence is
        // sustained — guards against snapshot lag firing on a single tick.
        let Some(memo) = ctx.history.oldest_since(cutoff) else {
            return Vec::new();
        };
        if memo.snapshot_at_ms > cutoff {
            return Vec::new();
        }
        let old_live: HashSet<i32> = memo.broker_ids.iter().copied().collect();

        let mut ids: Vec<i32> = missing_now
            .into_iter()
            .filter(|id| !old_live.contains(id))
            .collect();
        ids.sort_unstable();
        let secs = ctx.cfg.broker_death_threshold.as_secs();
        ids.into_iter()
            .map(|id| RuleHit {
                key: AnomalyKey::Broker(id),
                severity: AnomalySeverity::Critical,
                details: format!("broker {id} absent from snapshots for >={secs}s"),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};

    use super::*;
    use crate::{
        capacity::BrokerCapacities,
        detector::{DetectorConfig, SnapshotHistory},
        model::{BrokerView, ClusterState, PartitionView},
        scraper::UsageStore,
    };

    fn state(
        brokers: &[i32],
        partitions: Vec<(&str, i32, Vec<i32>, i32)>,
        now: i64,
    ) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: now,
            brokers: brokers
                .iter()
                .map(|id| BrokerView {
                    id: *id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: partitions
                .into_iter()
                .map(|(topic, partition, replicas, leader)| {
                    let isr = replicas.clone();
                    PartitionView {
                        topic: topic.into(),
                        partition,
                        replicas,
                        leader,
                        isr,
                    }
                })
                .collect(),
            in_flight_reassignments: vec![],
        }
    }

    fn cfg(threshold: Duration) -> DetectorConfig {
        DetectorConfig {
            broker_death_threshold: threshold,
            ..DetectorConfig::default()
        }
    }

    #[test]
    fn fresh_absence_does_not_fire() {
        let cfg = cfg(Duration::from_mins(1));
        let snap = state(&[1, 2], vec![("t", 0, vec![1, 2, 3], 1)], 1_000);
        let mut hist = SnapshotHistory::new(10);
        hist.push(&snap);
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let ctx = RuleCtx {
            snapshot: &snap,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1_000,
            cfg: &cfg,
        };
        assert!(BrokerDeath.evaluate(&ctx).is_empty());
    }

    #[test]
    fn sustained_absence_fires_critical() {
        let cfg = cfg(Duration::from_mins(1));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let mut hist = SnapshotHistory::new(10);
        // Two memos: one before the cutoff (anchoring history depth)
        // and one inside the recent window — both must lack broker 3
        // for `oldest_since(cutoff)` to confirm the absence is sustained.
        let old = state(&[1, 2], vec![("t", 0, vec![1, 2, 3], 1)], 0);
        hist.push(&old);
        let mid = state(&[1, 2], vec![("t", 0, vec![1, 2, 3], 1)], 60_000);
        hist.push(&mid);
        let now_snap = state(&[1, 2], vec![("t", 0, vec![1, 2, 3], 1)], 120_000);
        let ctx = RuleCtx {
            snapshot: &now_snap,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 120_000,
            cfg: &cfg,
        };
        let hits = BrokerDeath.evaluate(&ctx);
        assert!(hits.len() == 1);
        check!(hits[0].key == AnomalyKey::Broker(3));
        check!(hits[0].severity == AnomalySeverity::Critical);
    }

    #[test]
    fn reappearance_does_not_fire() {
        let cfg = cfg(Duration::from_mins(1));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let mut hist = SnapshotHistory::new(10);
        // Even with a history depth that would confirm sustained absence,
        // the current snapshot includes broker 3 so the rule emits empty.
        let old = state(&[1, 2], vec![("t", 0, vec![1, 2, 3], 1)], 0);
        hist.push(&old);
        let mid = state(&[1, 2], vec![("t", 0, vec![1, 2, 3], 1)], 60_000);
        hist.push(&mid);
        let now_snap = state(&[1, 2, 3], vec![("t", 0, vec![1, 2, 3], 1)], 120_000);
        let ctx = RuleCtx {
            snapshot: &now_snap,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 120_000,
            cfg: &cfg,
        };
        assert!(BrokerDeath.evaluate(&ctx).is_empty());
    }
}
