//! `UnderReplicatedPartitions` rule — fires when a partition's ISR has
//! been smaller than its replica set for >=`under_replicated_threshold`.
//! Skips partitions currently being reassigned (transient ISR
//! shortfalls during rebalance are expected).

use std::collections::{HashMap, HashSet};

use super::{Rule, RuleCtx, RuleHit, sustained_memo};
use crate::detector::{AnomalyKey, AnomalyKind, AnomalySeverity};

pub struct UnderReplicatedPartitions;

impl Rule for UnderReplicatedPartitions {
    fn kind(&self) -> AnomalyKind {
        AnomalyKind::UnderReplicatedPartitions
    }

    fn evaluate(&self, ctx: &RuleCtx<'_>) -> Vec<RuleHit> {
        // In-flight reassignment set — skip these.
        let reassigning: HashSet<(String, i32)> = ctx
            .snapshot
            .in_flight_reassignments
            .iter()
            .map(|r| (r.topic.clone(), r.partition))
            .collect();

        // Partitions under-replicated *right now*.
        let now_under: Vec<&crate::model::PartitionView> = ctx
            .snapshot
            .partitions
            .iter()
            .filter(|p| p.isr.len() < p.replicas.len())
            .filter(|p| !reassigning.contains(&(p.topic.clone(), p.partition)))
            .collect();
        if now_under.is_empty() {
            return Vec::new();
        }

        let Some(memo) = sustained_memo(ctx, ctx.cfg.under_replicated_threshold) else {
            return Vec::new(); // no history old enough
        };

        // Per-topic under-replication ratio for severity gating.
        let mut topic_under: HashMap<String, usize> = HashMap::new();
        let mut topic_total: HashMap<String, usize> = HashMap::new();
        for p in &ctx.snapshot.partitions {
            *topic_total.entry(p.topic.clone()).or_insert(0) += 1;
        }
        for p in &now_under {
            *topic_under.entry(p.topic.clone()).or_insert(0) += 1;
        }

        let secs = ctx.cfg.under_replicated_threshold.as_secs();
        let mut hits: Vec<RuleHit> = Vec::new();
        for p in &now_under {
            // Sustained check: in the oldest memo within the threshold,
            // was the same partition also under-replicated?
            let Some((rep_then, isr_then)) = memo
                .partition_isr
                .get(&(p.topic.clone(), p.partition))
                .copied()
            else {
                continue; // didn't exist back then; not sustained
            };
            if isr_then >= rep_then {
                continue; // wasn't under-replicated back then
            }
            let total = *topic_total.get(&p.topic).unwrap_or(&1);
            let under = *topic_under.get(&p.topic).unwrap_or(&0);
            let severity = if under * 2 > total {
                AnomalySeverity::Critical
            } else {
                AnomalySeverity::Warning
            };
            hits.push(RuleHit {
                key: AnomalyKey::Partition {
                    topic: p.topic.clone(),
                    partition: p.partition,
                },
                severity,
                details: format!("isr={}/{} for >={secs}s", p.isr.len(), p.replicas.len()),
            });
        }
        // Stable order for tests + consumer.
        hits.sort_by(|a, b| match (&a.key, &b.key) {
            (
                AnomalyKey::Partition {
                    topic: at,
                    partition: ap,
                },
                AnomalyKey::Partition {
                    topic: bt,
                    partition: bp,
                },
            ) => at.cmp(bt).then(ap.cmp(bp)),
            _ => std::cmp::Ordering::Equal,
        });
        hits
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{
        capacity::BrokerCapacities,
        detector::{DetectorConfig, SnapshotHistory},
        model::{BrokerView, ClusterState, InFlightReassignment, PartitionView},
        scraper::UsageStore,
    };

    fn part_full(topic: &str, partition: i32, replicas: Vec<i32>) -> PartitionView {
        PartitionView {
            topic: topic.into(),
            partition,
            replicas: replicas.clone(),
            leader: replicas[0],
            isr: replicas,
        }
    }
    fn part_under(topic: &str, partition: i32, replicas: Vec<i32>, isr: Vec<i32>) -> PartitionView {
        PartitionView {
            topic: topic.into(),
            partition,
            leader: replicas[0],
            replicas,
            isr,
        }
    }

    fn state(parts: Vec<PartitionView>, now: i64) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: now,
            brokers: (1..=3)
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn cfg(threshold: Duration) -> DetectorConfig {
        DetectorConfig {
            under_replicated_threshold: threshold,
            ..DetectorConfig::default()
        }
    }

    fn ctx<'a>(
        snap: &'a ClusterState,
        hist: &'a SnapshotHistory,
        cfg: &'a DetectorConfig,
        usages: &'a UsageStore,
        capacities: &'a BrokerCapacities,
        now: i64,
    ) -> RuleCtx<'a> {
        RuleCtx {
            snapshot: snap,
            history: hist,
            usages,
            capacities,
            now_ms: now,
            cfg,
        }
    }

    #[test]
    fn transient_no_fire() {
        let cfg = cfg(Duration::from_mins(2));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let snap = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 1_000);
        let mut hist = SnapshotHistory::new(10);
        hist.push(&snap);
        let ctx = ctx(&snap, &hist, &cfg, &usages, &capacities, 1_000);
        assert2::assert!(UnderReplicatedPartitions.evaluate(&ctx).is_empty());
    }

    #[test]
    fn sustained_fires() {
        // cutoff = 200_000 - 120_000 = 80_000; push a memo at the cutoff
        // so oldest_since(cutoff) returns a memo whose snapshot_at_ms <= cutoff.
        let cfg = cfg(Duration::from_mins(2));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let old = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 0);
        let mid = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 80_000);
        let now_snap = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 200_000);
        let mut hist = SnapshotHistory::new(10);
        hist.push(&old);
        hist.push(&mid);
        let ctx = ctx(&now_snap, &hist, &cfg, &usages, &capacities, 200_000);
        let hits = UnderReplicatedPartitions.evaluate(&ctx);
        assert2::assert!(
            hits.iter().map(|hit| hit.severity).collect::<Vec<_>>()
                == vec![AnomalySeverity::Critical]
        );
    }

    #[test]
    fn severity_warning_when_only_one_of_many_under() {
        // 4-partition topic, 1 under-replicated → 25%, below 50%, warning.
        let cfg = cfg(Duration::from_mins(2));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let parts = vec![
            part_under("t", 0, vec![1, 2, 3], vec![1, 2]),
            part_full("t", 1, vec![1, 2, 3]),
            part_full("t", 2, vec![1, 2, 3]),
            part_full("t", 3, vec![1, 2, 3]),
        ];
        let old = state(parts.clone(), 0);
        let mid = state(parts.clone(), 80_000);
        let now_snap = state(parts, 200_000);
        let mut hist = SnapshotHistory::new(10);
        hist.push(&old);
        hist.push(&mid);
        let ctx = ctx(&now_snap, &hist, &cfg, &usages, &capacities, 200_000);
        let hits = UnderReplicatedPartitions.evaluate(&ctx);
        assert2::assert!(
            hits.iter().map(|hit| hit.severity).collect::<Vec<_>>()
                == vec![AnomalySeverity::Warning]
        );
    }

    #[test]
    fn severity_warning_when_exactly_half_of_topic_is_under_replicated() {
        let cfg = cfg(Duration::from_mins(2));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let parts = vec![
            part_under("t", 0, vec![1, 2, 3], vec![1, 2]),
            part_under("t", 1, vec![1, 2, 3], vec![1, 2]),
            part_full("t", 2, vec![1, 2, 3]),
            part_full("t", 3, vec![1, 2, 3]),
        ];
        let old = state(parts.clone(), 0);
        let mid = state(parts.clone(), 80_000);
        let now_snap = state(parts, 200_000);
        let mut hist = SnapshotHistory::new(10);
        hist.push(&old);
        hist.push(&mid);
        let ctx = ctx(&now_snap, &hist, &cfg, &usages, &capacities, 200_000);

        let hits = UnderReplicatedPartitions.evaluate(&ctx);

        assert2::assert!(
            (
                hits.len(),
                hits.iter()
                    .all(|hit| matches!(hit.severity, AnomalySeverity::Warning))
            ) == (2, true)
        );
    }

    #[test]
    fn hits_are_sorted_by_topic_then_partition() {
        let cfg = cfg(Duration::from_mins(2));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let parts = vec![
            part_under("b", 2, vec![1, 2, 3], vec![1, 2]),
            part_under("a", 3, vec![1, 2, 3], vec![1, 2]),
            part_under("a", 1, vec![1, 2, 3], vec![1, 2]),
        ];
        let old = state(parts.clone(), 0);
        let mid = state(parts.clone(), 80_000);
        let now_snap = state(parts, 200_000);
        let mut hist = SnapshotHistory::new(10);
        hist.push(&old);
        hist.push(&mid);
        let ctx = ctx(&now_snap, &hist, &cfg, &usages, &capacities, 200_000);

        let keys: Vec<_> = UnderReplicatedPartitions
            .evaluate(&ctx)
            .into_iter()
            .map(|hit| hit.key)
            .collect();

        assert2::assert!(
            keys == vec![
                AnomalyKey::Partition {
                    topic: "a".into(),
                    partition: 1,
                },
                AnomalyKey::Partition {
                    topic: "a".into(),
                    partition: 3,
                },
                AnomalyKey::Partition {
                    topic: "b".into(),
                    partition: 2,
                },
            ]
        );
    }

    #[test]
    fn skip_in_flight_reassignment() {
        // Under-replicated *and* sustained, but partition is being
        // reassigned → suppress.
        let cfg = cfg(Duration::from_mins(2));
        let usages = UsageStore::default();
        let capacities = BrokerCapacities::default();
        let old = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 0);
        let mid = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 80_000);
        let mut now_snap = state(vec![part_under("t", 0, vec![1, 2, 3], vec![1, 2])], 200_000);
        now_snap.in_flight_reassignments = vec![InFlightReassignment {
            topic: "t".into(),
            partition: 0,
            adding: vec![],
            removing: vec![],
        }];
        let mut hist = SnapshotHistory::new(10);
        hist.push(&old);
        hist.push(&mid);
        let ctx = ctx(&now_snap, &hist, &cfg, &usages, &capacities, 200_000);
        assert2::assert!(UnderReplicatedPartitions.evaluate(&ctx).is_empty());
    }
}
