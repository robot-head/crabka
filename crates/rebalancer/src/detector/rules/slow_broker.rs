//! `SlowBroker` rule — fires when a broker's per-broker CPU rate (cores)
//! exceeds `max(slow_broker_min_cores, slow_broker_multiplier × cluster
//! median)`. Requires >=3 brokers reporting CPU for a stable median.

use std::collections::HashMap;

use crate::detector::AnomalyKey;
use crate::detector::AnomalyKind;
use crate::detector::AnomalySeverity;
use crate::scraper::Window;

use super::Rule;
use super::RuleCtx;
use super::RuleHit;

pub struct SlowBroker;

impl SlowBroker {
    fn per_broker_cores(ctx: &RuleCtx<'_>) -> HashMap<i32, f64> {
        let mut per_broker: HashMap<i32, f64> = HashMap::new();
        for p in &ctx.snapshot.partitions {
            for replica in &p.replicas {
                if let Some(rate) = ctx.usages.cpu_micros_rate(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                    ctx.now_ms,
                ) {
                    *per_broker.entry(*replica).or_insert(0.0) += rate / 1_000_000.0;
                }
            }
        }
        per_broker
    }
}

impl Rule for SlowBroker {
    fn kind(&self) -> AnomalyKind {
        AnomalyKind::SlowBroker
    }

    fn evaluate(&self, ctx: &RuleCtx<'_>) -> Vec<RuleHit> {
        let per_broker = Self::per_broker_cores(ctx);
        if per_broker.len() < 3 {
            return Vec::new();
        }

        let mut sorted: Vec<f64> = per_broker.values().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = sorted.len() / 2;
        let median = if sorted.len().is_multiple_of(2) {
            f64::midpoint(sorted[mid - 1], sorted[mid])
        } else {
            sorted[mid]
        };
        if !median.is_finite() {
            return Vec::new();
        }

        let multiplier_threshold = ctx.cfg.slow_broker_multiplier * median;
        let threshold = ctx.cfg.slow_broker_min_cores.max(multiplier_threshold);

        let mut hits: Vec<RuleHit> = Vec::new();
        let mut ids: Vec<(i32, f64)> = per_broker.into_iter().collect();
        ids.sort_by_key(|(id, _)| *id);
        for (id, cores) in ids {
            if cores > threshold {
                hits.push(RuleHit {
                    key: AnomalyKey::Broker(id),
                    severity: AnomalySeverity::Warning,
                    details: format!(
                        "broker {id} cpu {cores:.2} cores (median {median:.2}, threshold {threshold:.2})"
                    ),
                });
            }
        }
        hits
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::capacity::BrokerCapacities;
    use crate::detector::{DetectorConfig, SnapshotHistory};
    use crate::model::{BrokerView, ClusterState, PartitionView};
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};

    fn state(brokers: &[i32], parts: Vec<PartitionView>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .iter()
                .map(|id| BrokerView {
                    id: *id,
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

    /// `samples`: (broker, topic, partition, `v_t0`, `v_t1`) — CPU counter is
    /// monotonic; we feed two samples 1s apart so `cpu_micros_rate` has
    /// at least one delta to compute.
    fn store_with_cpu(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_hours(1),
        });
        for (broker, topic, partition, v0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::CpuMicros,
                    topic: (*topic).into(),
                    partition: *partition,
                    value: *v0,
                }],
                0,
            );
        }
        for (broker, topic, partition, _, v1) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::CpuMicros,
                    topic: topic.into(),
                    partition,
                    value: v1,
                }],
                1000,
            );
        }
        Arc::new(store)
    }

    fn cfg(multiplier: f64, min_cores: f64) -> DetectorConfig {
        DetectorConfig {
            slow_broker_multiplier: multiplier,
            slow_broker_min_cores: min_cores,
            ..DetectorConfig::default()
        }
    }

    #[test]
    fn per_broker_cores_converts_cpu_micros_to_cores() {
        let parts = vec![part("t", 0, vec![1])];
        let s = state(&[1], parts);
        let usages = store_with_cpu(vec![(1, "t", 0, 0.0, 2_000_000.0)]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(2.0, 0.0);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };

        let cores = SlowBroker::per_broker_cores(&ctx);

        assert!((cores[&1] - 2.0).abs() < 1e-9);
    }

    #[test]
    fn idle_cluster_no_fire() {
        // All three brokers reporting ~0 cores → max threshold = min_cores (0.5).
        // No broker over that → no fire.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3])).collect();
        let s = state(&[1, 2, 3], parts);
        // 10 micros/sec rate ≈ 0.00001 cores; well below 0.5 min_cores.
        let usages = store_with_cpu(vec![
            (1, "t", 0, 0.0, 10.0),
            (2, "t", 1, 0.0, 10.0),
            (3, "t", 2, 0.0, 10.0),
        ]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(2.0, 0.5);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };
        assert!(SlowBroker.evaluate(&ctx).is_empty());
    }

    #[test]
    fn outlier_fires_warning() {
        // Brokers 2 and 3 at ~0.5 cores; broker 1 at ~3 cores.
        // Median = 0.5; threshold = max(0.5, 2 × 0.5) = 1.0. Broker 1 > 1.0 → fire.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3])).collect();
        let s = state(&[1, 2, 3], parts);
        // CPU is in micros/sec; 3_000_000 micros/sec = 3 cores. Counter
        // values t0=0, t1=3_000_000 with 1s delta → rate 3_000_000 µs/s.
        let usages = store_with_cpu(vec![
            (1, "t", 0, 0.0, 3_000_000.0),
            (2, "t", 1, 0.0, 500_000.0),
            (3, "t", 2, 0.0, 500_000.0),
        ]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(2.0, 0.5);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };
        let hits = SlowBroker.evaluate(&ctx);
        assert!(hits.len() == 1);
        assert!(hits[0].key == AnomalyKey::Broker(1));
        assert!(matches!(hits[0].severity, AnomalySeverity::Warning));
    }

    #[test]
    fn even_broker_count_uses_average_of_middle_two_for_median() {
        let parts: Vec<_> = (0..4).map(|i| part("t", i, vec![1, 2, 3, 4])).collect();
        let s = state(&[1, 2, 3, 4], parts);
        let usages = store_with_cpu(vec![
            (1, "t", 0, 0.0, 1_000_000.0),
            (2, "t", 1, 0.0, 3_000_000.0),
            (3, "t", 2, 0.0, 5_000_000.0),
            (4, "t", 3, 0.0, 7_000_000.0),
        ]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(1.5, 0.0);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };

        let hits = SlowBroker.evaluate(&ctx);

        assert!(hits.len() == 1);
        assert!(hits[0].key == AnomalyKey::Broker(4));
        assert!(hits[0].details.contains("median 4.00"));
        assert!(hits[0].details.contains("threshold 6.00"));
    }

    #[test]
    fn multiplier_threshold_uses_product_not_sum() {
        let parts: Vec<_> = (0..4).map(|i| part("t", i, vec![1, 2, 3, 4])).collect();
        let s = state(&[1, 2, 3, 4], parts);
        let usages = store_with_cpu(vec![
            (1, "t", 0, 0.0, 1_000_000.0),
            (2, "t", 1, 0.0, 3_000_000.0),
            (3, "t", 2, 0.0, 5_000_000.0),
            (4, "t", 3, 0.0, 7_500_000.0),
        ]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(2.0, 0.0);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };

        assert!(SlowBroker.evaluate(&ctx).is_empty());
    }

    #[test]
    fn broker_exactly_at_threshold_does_not_fire() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3])).collect();
        let s = state(&[1, 2, 3], parts);
        let usages = store_with_cpu(vec![
            (1, "t", 0, 0.0, 2_000_000.0),
            (2, "t", 1, 0.0, 1_000_000.0),
            (3, "t", 2, 0.0, 1_000_000.0),
        ]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(2.0, 0.0);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };

        assert!(SlowBroker.evaluate(&ctx).is_empty());
    }

    #[test]
    fn too_few_brokers_no_fire() {
        // Only 2 brokers reporting CPU → skip (median unreliable).
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2])).collect();
        let s = state(&[1, 2], parts);
        let usages = store_with_cpu(vec![
            (1, "t", 0, 0.0, 3_000_000.0),
            (2, "t", 1, 0.0, 500_000.0),
        ]);
        let capacities = BrokerCapacities::default();
        let cfg = cfg(2.0, 0.5);
        let hist = SnapshotHistory::new(10);
        let ctx = RuleCtx {
            snapshot: &s,
            history: &hist,
            usages: &usages,
            capacities: &capacities,
            now_ms: 1000,
            cfg: &cfg,
        };
        assert!(SlowBroker.evaluate(&ctx).is_empty());
    }
}
