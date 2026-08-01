//! `DiskPressure` rule — fires when a broker's summed `disk_bytes_avg`
//! exceeds `disk_pressure_threshold` of its configured `disk_bytes`
//! capacity. Skips brokers with no capacity info.

use std::collections::HashMap;

use crabka_units::{
    ByteSize, Ratio,
    fmt::Human as _,
    prelude::{ByteSizeExt as _, RatioExt as _},
};

use super::{Rule, RuleCtx, RuleHit};
use crate::{
    detector::{AnomalyKey, AnomalyKind, AnomalySeverity},
    scraper::Window,
};

pub struct DiskPressure;

impl Rule for DiskPressure {
    fn kind(&self) -> AnomalyKind {
        AnomalyKind::DiskPressure
    }

    fn evaluate(&self, ctx: &RuleCtx<'_>) -> Vec<RuleHit> {
        // Sum disk_bytes_avg per broker across all hosted replicas.
        let mut per_broker: HashMap<i32, ByteSize> = HashMap::new();
        for p in &ctx.snapshot.partitions {
            for replica in &p.replicas {
                if let Some(bytes) = ctx.usages.disk_bytes_avg(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                    ctx.now_ms,
                ) {
                    *per_broker.entry(*replica).or_insert(ByteSize::ZERO) += bytes;
                }
            }
        }

        let mut hits: Vec<RuleHit> = Vec::new();
        let mut sorted: Vec<(i32, ByteSize)> = per_broker.into_iter().collect();
        sorted.sort_by_key(|(id, _)| *id);
        for (id, total) in sorted {
            let Some(cap) = ctx.capacities.for_broker(id) else {
                continue;
            };
            let Some(cap_bytes) = cap.disk_bytes else {
                continue;
            };
            if cap_bytes == ByteSize::ZERO {
                continue;
            }
            // Two byte counts divide to a dimensionless fill factor.
            let fill: Ratio = total / cap_bytes;
            if fill <= ctx.cfg.disk_pressure_threshold {
                continue;
            }
            let severity = if fill > ctx.cfg.disk_critical_threshold {
                AnomalySeverity::Critical
            } else {
                AnomalySeverity::Warning
            };
            hits.push(RuleHit {
                key: AnomalyKey::Broker(id),
                severity,
                details: format!(
                    "broker {id} disk usage {pct:.1}% (cap {cap})",
                    pct = fill.percent_f64(),
                    cap = cap_bytes.human(),
                ),
            });
        }
        hits
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        capacity::{BrokerCapacities, BrokerCapacity},
        detector::{DetectorConfig, SnapshotHistory},
        model::{BrokerView, ClusterState, PartitionView},
        scraper::{MetricKind, UsageStore, WindowConfig, parse::ParsedSample},
    };

    fn state(parts: Vec<PartitionView>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
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

    fn part(topic: &str, partition: i32, replicas: Vec<i32>) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            leader: replicas[0],
            replicas,
            isr,
        }
    }

    fn store_with_disk(samples: Vec<(i32, &str, i32, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: secs(30),
            retention: hours(1),
        });
        for (broker, topic, partition, value) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::DiskBytes,
                    topic: topic.into(),
                    partition,
                    value,
                }],
                0,
            );
        }
        Arc::new(store)
    }

    fn caps_with(broker: i32, disk_bytes: ByteSize) -> BrokerCapacities {
        let mut by = std::collections::HashMap::new();
        by.insert(
            broker,
            BrokerCapacity {
                disk_bytes: Some(disk_bytes),
                ..Default::default()
            },
        );
        BrokerCapacities { by_broker: by }
    }

    fn cfg(pressure: Ratio, critical: Ratio) -> DetectorConfig {
        DetectorConfig {
            disk_pressure_threshold: pressure,
            disk_critical_threshold: critical,
            ..DetectorConfig::default()
        }
    }

    #[test]
    fn under_threshold_no_fire() {
        // Broker 1: 5 partitions × 100 bytes = 500. Cap 10_000. Ratio 5%.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2])).collect();
        let s = state(parts);
        let usages = store_with_disk((0..5).map(|i| (1, "t", i, 100.0)).collect());
        let capacities = caps_with(1, bytes(10_000));
        let cfg = cfg(percent(85), percent(95));
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 0,
            cfg: &cfg,
        };
        assert2::assert!(DiskPressure.evaluate(&ctx).is_empty());
    }

    #[test]
    fn over_threshold_warning() {
        // 5 partitions × 180 = 900 ÷ 1000 = 90% → Warning (>85%, <95%).
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2])).collect();
        let s = state(parts);
        let usages = store_with_disk((0..5).map(|i| (1, "t", i, 180.0)).collect());
        let capacities = caps_with(1, bytes(1_000));
        let cfg = cfg(percent(85), percent(95));
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 0,
            cfg: &cfg,
        };
        let hits = DiskPressure.evaluate(&ctx);
        assert2::assert!(
            hits.iter()
                .map(|hit| (&hit.key, hit.severity))
                .collect::<Vec<_>>()
                == vec![(&AnomalyKey::Broker(1), AnomalySeverity::Warning)]
        );
    }

    #[test]
    fn over_critical_threshold_critical() {
        // 5 × 200 = 1000 ÷ 1000 = 100% → Critical (>95%).
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2])).collect();
        let s = state(parts);
        let usages = store_with_disk((0..5).map(|i| (1, "t", i, 200.0)).collect());
        let capacities = caps_with(1, bytes(1_000));
        let cfg = cfg(percent(85), percent(95));
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 0,
            cfg: &cfg,
        };
        let hits = DiskPressure.evaluate(&ctx);
        assert2::assert!(
            hits.iter().map(|hit| hit.severity).collect::<Vec<_>>()
                == vec![AnomalySeverity::Critical]
        );
    }

    #[test]
    fn exact_critical_threshold_is_warning() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2])).collect();
        let s = state(parts);
        let usages = store_with_disk((0..5).map(|i| (1, "t", i, 190.0)).collect());
        let capacities = caps_with(1, bytes(1_000));
        let cfg = cfg(percent(85), percent(95));
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 0,
            cfg: &cfg,
        };

        let hits = DiskPressure.evaluate(&ctx);

        assert2::assert!(
            hits.iter().map(|hit| hit.severity).collect::<Vec<_>>()
                == vec![AnomalySeverity::Warning]
        );
    }

    #[test]
    fn no_capacity_info_skips_broker() {
        // Lots of disk usage but no capacity configured → skip.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2])).collect();
        let s = state(parts);
        let usages = store_with_disk((0..5).map(|i| (1, "t", i, 10_000_000.0)).collect());
        let capacities = BrokerCapacities::default(); // empty
        let cfg = cfg(percent(85), percent(95));
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 0,
            cfg: &cfg,
        };
        assert2::assert!(DiskPressure.evaluate(&ctx).is_empty());
    }
}
