//! Soft goal: balance per-broker total CPU usage. Per-broker total =
//! sum over partitions a broker hosts of
//! `UsageStore::cpu_cores_rate(broker, topic, partition, FiveMin)`.
//! Greedy hot->cold swap, threshold-driven via
//! `GoalContext.imbalance_threshold`.

use std::collections::HashMap;

use crabka_units::{Ratio, convert::RatioExt};

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

pub struct CpuUsage;

impl CpuUsage {
    pub const NAME: &'static str = "CpuUsage";

    /// CPU micros/sec total per broker. Skips partitions with no
    /// usage data.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        crate::goals::replica_totals(partitions, broker_ids, |broker, topic, partition| {
            ctx.broker_usages
                .cpu_cores_rate(broker, topic, partition, Window::FiveMin, now_ms)
                .map(RatioExt::as_f64)
        })
    }

    fn imbalance(totals: &HashMap<i32, f64>) -> Ratio {
        crate::goals::imbalance_ratio_f64(totals)
    }
}

impl Goal for CpuUsage {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let now_ms = crate::goals::now_ms();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let originals = OriginalReplicaState::from_partitions(&state.partitions);

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx, now_ms);
            if totals.values().all(|v| *v == 0.0) {
                break;
            }
            if Self::imbalance(&totals) <= ctx.imbalance_threshold {
                break;
            }
            let mut by_load: Vec<(i32, f64)> = totals.into_iter().collect();
            by_load.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let (hot, _) = by_load.first().copied().unwrap_or((0, 0.0));
            let (cold, _) = by_load.last().copied().unwrap_or((0, 0.0));
            if hot == cold {
                break;
            }

            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() < state.brokers.len()
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            out.push(originals.replace_replica(p, hot, cold));

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        model::BrokerView,
        scraper::{MetricKind, UsageStore, WindowConfig, parse::ParsedSample},
    };

    fn ctx_with(store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold: percent(10),
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(crate::capacity::BrokerCapacities::default()),
            broker_usages: store,
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
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

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    /// Build a `UsageStore` pre-populated with `CpuMicros` counter pairs.
    /// Each tuple is `(broker, topic, partition, v_t0, v_t1)` inserted
    /// at `now-1000` and `now` so the rate is `(v_t1 - v_t0)/sec`.
    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: secs(30),
            retention: hours(1),
        });
        let now_ms = crate::goals::now_ms();
        let t0 = now_ms - 1000;
        for (broker, topic, partition, v_t0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::CpuMicros,
                    topic: (*topic).into(),
                    partition: *partition,
                    value: *v_t0,
                }],
                t0,
            );
        }
        for (broker, topic, partition, _, v_t1) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::CpuMicros,
                    topic: topic.into(),
                    partition,
                    value: v_t1,
                }],
                now_ms,
            );
        }
        Arc::new(store)
    }

    #[test]
    fn empty_usage_store_no_op() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(Arc::new(UsageStore::default()));
        assert2::assert!(CpuUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        // Broker 1 burns ~500_000 micros/sec across 5 partitions; broker
        // 2 only ~5_000. Imbalance well over the 10% threshold.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<(i32, &str, i32, f64, f64)> = (0..5)
            .map(|i| (1, "t", i, 0.0, 100_000.0))
            .chain((0..5).map(|i| (2, "t", i, 0.0, 1_000.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = CpuUsage.propose(&s, &ctx);
        assert2::assert!(!mvs.is_empty());
    }

    #[test]
    fn threshold_respected() {
        // Two brokers within 10% on CPU -> no movement.
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = vec![
            (1, "t", 0, 0.0, 100_000.0),
            (1, "t", 1, 0.0, 100_000.0),
            (2, "t", 0, 0.0, 95_000.0),
            (2, "t", 1, 0.0, 95_000.0),
        ];
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        assert2::assert!(CpuUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn imbalance_is_spread_over_total() {
        let totals = std::collections::HashMap::from([(1, 300.0), (2, 100.0)]);
        assert2::assert!(CpuUsage::imbalance(&totals) == percent(50));
    }

    #[test]
    fn full_replica_set_has_no_legal_cpu_usage_move() {
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<(i32, &str, i32, f64, f64)> = (0..2)
            .map(|i| (1, "t", i, 0.0, 100_000.0))
            .chain((0..2).map(|i| (2, "t", i, 0.0, 1_000.0)))
            .chain((0..2).map(|i| (3, "t", i, 0.0, 1_000.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        assert2::assert!(CpuUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn movement_cap_limits_cpu_usage_swaps() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<(i32, &str, i32, f64, f64)> = (0..5)
            .map(|i| (1, "t", i, 0.0, 100_000.0))
            .chain((0..5).map(|i| (2, "t", i, 0.0, 1_000.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;
        let mvs = CpuUsage.propose(&s, &ctx);
        assert2::assert!(mvs.len() == 1);
    }
}
