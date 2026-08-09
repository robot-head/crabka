//! Soft goal: balance the per-broker total bytes-in rate, summed across every
//! replica role, leader and followers. It counts replication ingress as well as
//! producer traffic. Use `LeaderBytesIn` for a leader-only view.

use std::collections::HashMap;

use crabka_units::{Ratio, convert::ByteRateExt};

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

pub struct NetworkInUsage;

impl NetworkInUsage {
    pub const NAME: &'static str = "NetworkInUsage";

    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        crate::goals::replica_totals(partitions, broker_ids, |broker, topic, partition| {
            ctx.broker_usages
                .bytes_in_rate(broker, topic, partition, Window::FiveMin, now_ms)
                .map(ByteRateExt::bytes_per_sec_f64)
        })
    }

    fn imbalance(totals: &HashMap<i32, f64>) -> Ratio {
        crate::goals::imbalance_ratio_f64(totals)
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

    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: secs(30),
            retention: hours(1),
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
        assert2::assert!(NetworkInUsage.propose(&s, &ctx).is_empty());
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
        assert2::assert!(!mvs.is_empty());
    }

    #[test]
    fn imbalance_is_spread_over_total() {
        let totals = std::collections::HashMap::from([(1, 300.0), (2, 100.0)]);
        assert2::assert!(NetworkInUsage::imbalance(&totals) == percent(50));
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

        assert2::assert!(
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
    fn partition_already_on_hot_and_cold_has_no_legal_network_in_usage_move() {
        let parts = vec![part("t", 0, vec![1, 2], 1), part("warm", 0, vec![3], 3)];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_counter_pair(vec![
            (1, "t", 0, 0.0, 300_000.0),
            (2, "t", 0, 0.0, 100_000.0),
            (3, "warm", 0, 0.0, 200_000.0),
        ]);
        let ctx = ctx_with(store);

        assert2::assert!(NetworkInUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn replica_vector_at_broker_count_without_cold_is_not_expandable() {
        let parts = vec![part("t", 0, vec![1, 99], 1)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_counter_pair(vec![(1, "t", 0, 0.0, 300_000.0)]);
        let ctx = ctx_with(store);

        assert2::assert!(NetworkInUsage.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn network_in_usage_replaces_hot_replica_at_its_actual_position() {
        let parts = vec![part("t", 0, vec![99, 1], 99), part("cold", 0, vec![2], 2)];
        let s = state_with(parts, vec![1, 2, 99]);
        let store = store_with_counter_pair(vec![
            (1, "t", 0, 0.0, 300_000.0),
            (99, "t", 0, 0.0, 200_000.0),
            (2, "cold", 0, 0.0, 100_000.0),
        ]);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = NetworkInUsage.propose(&s, &ctx);

        assert2::assert!(
            mvs == vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![99, 1],
                new_replicas: vec![99, 2],
                old_leader: 99,
                new_leader: 99,
            }]
        );
    }

    #[test]
    fn movement_cap_limits_network_in_usage_swaps() {
        let mut parts: Vec<_> = (0..5).map(|i| part("hot", i, vec![1], 1)).collect();
        parts.push(part("cold", 0, vec![2], 2));
        let s = state_with(parts, vec![1, 2, 3]);
        let samples = (0..5)
            .map(|i| (1, "hot", i, 0.0, 100_000.0))
            .chain(std::iter::once((2, "cold", 0, 0.0, 1.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = NetworkInUsage.propose(&s, &ctx);

        assert2::assert!(mvs.len() == 1);
    }
}
