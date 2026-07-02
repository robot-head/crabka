//! Soft goal: balance per-broker total disk usage. Per-broker total
//! = sum over partitions a broker hosts of
//! `UsageStore::disk_bytes_avg(broker, topic, partition, FiveMin)`.
//! Greedy hot->cold swap, threshold-driven via
//! `GoalContext.imbalance_threshold_pct`.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct DiskUsage;

impl DiskUsage {
    pub const NAME: &'static str = "DiskUsage";

    /// Disk-bytes total per broker. Skips partitions with no usage data.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(bytes) = ctx.broker_usages.disk_bytes_avg(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                    now_ms,
                ) {
                    *m.entry(*replica).or_insert(0.0) += bytes;
                }
            }
        }
        m
    }

    fn imbalance_pct(totals: &HashMap<i32, f64>) -> u32 {
        let vals: Vec<f64> = totals.values().copied().collect();
        let total: f64 = vals.iter().sum();
        if total <= 0.0 {
            return 0;
        }
        let max = vals.iter().fold(0.0f64, |a, b| a.max(*b));
        let min = vals.iter().fold(f64::INFINITY, |a, b| a.min(*b));
        let pct = ((max - min) * 100.0 / total).clamp(0.0, f64::from(u32::MAX));
        // Saturating cast: pct is clamped to [0, u32::MAX] above.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let out = pct as u32;
        out
    }
}

impl Goal for DiskUsage {
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

        let original_replicas: HashMap<(String, i32), Vec<i32>> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.replicas.clone()))
            .collect();
        let original_leader: HashMap<(String, i32), i32> = state
            .partitions
            .iter()
            .map(|p| ((p.topic.clone(), p.partition), p.leader))
            .collect();

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx, now_ms);
            if totals.values().all(|v| *v == 0.0) {
                // No usage data anywhere -> no-op.
                break;
            }
            if Self::imbalance_pct(&totals) <= ctx.imbalance_threshold_pct {
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
            let key = (p.topic.clone(), p.partition);
            let pos = p
                .replicas
                .iter()
                .position(|r| *r == hot)
                .expect("hot present");
            p.replicas[pos] = cold;
            let new_leader = if p.leader == hot {
                *p.replicas
                    .iter()
                    .find(|r| p.isr.contains(r))
                    .unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };

            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);

            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader,
                new_leader,
            });
            p.leader = new_leader;

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use assert2::assert;
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with(store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
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

    fn store_with_disk_samples(samples: Vec<(i32, &str, i32, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_hours(1),
        });
        // Insert at "now" so the stale-data guard in UsageStore (which
        // compares against the goal's wall-clock `now_ms()`) sees the
        // sample as recent.
        let now_ms = crate::goals::now_ms();
        for (broker, topic, partition, value) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::DiskBytes,
                    topic: topic.into(),
                    partition,
                    value,
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
        assert!(DiskUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        // Broker 1 has 5 partitions x 100MB each = 500MB; broker 2 has 0.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts.clone(), vec![1, 2, 3]);
        let samples: Vec<(i32, &str, i32, f64)> = (0..5)
            .map(|i| (1, "t", i, 100.0))
            .chain((0..5).map(|i| (2, "t", i, 1.0)))
            .collect();
        let store = store_with_disk_samples(samples);
        let ctx = ctx_with(store);
        let mvs = DiskUsage.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected disk-hot swaps");
    }

    #[test]
    fn threshold_respected() {
        // Two brokers each holding ~equal disk (within 10%) -> no-op.
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = vec![
            (1, "t", 0, 100.0),
            (1, "t", 1, 100.0),
            (2, "t", 0, 95.0),
            (2, "t", 1, 95.0),
        ];
        let store = store_with_disk_samples(samples);
        let ctx = ctx_with(store);
        assert!(
            DiskUsage.propose(&s, &ctx).is_empty(),
            "within-threshold should no-op"
        );
    }

    #[test]
    fn imbalance_pct_uses_difference_times_100_over_total() {
        let totals = std::collections::HashMap::from([(1, 300.0), (2, 100.0)]);
        assert!(DiskUsage::imbalance_pct(&totals) == 50);
    }

    #[test]
    fn nonzero_imbalanced_disk_usage_moves_and_rehomes_hot_leader() {
        let parts = vec![part("hot", 0, vec![1], 1), part("cold", 0, vec![2], 2)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_disk_samples(vec![(1, "hot", 0, 300.0), (2, "cold", 0, 100.0)]);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = DiskUsage.propose(&s, &ctx);

        assert!(
            mvs == vec![Movement {
                topic: "hot".into(),
                partition: 0,
                old_replicas: vec![1],
                new_replicas: vec![2],
                old_leader: 1,
                new_leader: 2,
            }]
        );
    }

    #[test]
    fn replica_vector_at_broker_count_without_cold_is_not_expandable() {
        let parts = vec![part("t", 0, vec![1, 99], 1)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_disk_samples(vec![(1, "t", 0, 300.0)]);
        let ctx = ctx_with(store);

        assert!(DiskUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn movement_cap_limits_disk_usage_swaps() {
        let mut parts: Vec<_> = (0..5).map(|i| part("hot", i, vec![1], 1)).collect();
        parts.push(part("cold", 0, vec![2], 2));
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..5)
            .map(|i| (1, "hot", i, 100.0))
            .chain(std::iter::once((2, "cold", 0, 1.0)))
            .collect();
        let store = store_with_disk_samples(samples);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = DiskUsage.propose(&s, &ctx);

        assert!(mvs.len() == 1);
    }
}
