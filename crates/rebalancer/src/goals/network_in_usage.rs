//! Soft goal: balance per-broker total bytes-in rate, summed across
//! every replica role (leader + followers). Counts replication
//! ingress in addition to producer traffic -- use `LeaderBytesIn` for
//! a leader-only view.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct NetworkInUsage;

impl NetworkInUsage {
    pub const NAME: &'static str = "NetworkInUsage";

    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(rate) = ctx.broker_usages.bytes_in_rate(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                    now_ms,
                ) {
                    *m.entry(*replica).or_insert(0.0) += rate;
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

impl Goal for NetworkInUsage {
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

    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_hours(1),
        });
        // Insert at "now-1000" and "now" so the 1-second delta still
        // yields the same rate, and both samples are inside the 5-min
        // stale-data guard window of UsageStore.
        let now_ms = crate::goals::now_ms();
        let t0 = now_ms - 1000;
        for (broker, topic, partition, v_t0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesIn,
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
                    metric: MetricKind::BytesIn,
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
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(Arc::new(UsageStore::default()));
        assert!(NetworkInUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        // Broker 1 sees high ingress on every partition (leader + replicating).
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples = (0..5)
            .map(|i| (1, "t", i, 0.0, 100_000.0))
            .chain((0..5).map(|i| (2, "t", i, 0.0, 1.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = NetworkInUsage.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected swaps");
    }

    #[test]
    fn imbalance_pct_uses_difference_times_100_over_total() {
        let totals = std::collections::HashMap::from([(1, 300.0), (2, 100.0)]);
        assert!(NetworkInUsage::imbalance_pct(&totals) == 50);
    }

    #[test]
    fn nonzero_imbalanced_network_in_usage_moves() {
        let parts = vec![part("hot", 0, vec![1], 1), part("cold", 0, vec![2], 2)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_counter_pair(vec![
            (1, "hot", 0, 0.0, 300_000.0),
            (2, "cold", 0, 0.0, 100_000.0),
        ]);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = NetworkInUsage.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].old_replicas == vec![1]);
        assert!(mvs[0].new_replicas == vec![2]);
    }
}
