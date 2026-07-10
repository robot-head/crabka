//! Hard goal: enforce a per-broker `network_in_bytes_per_sec` limit
//! using the scraped `UsageStore::bytes_in_rate` summed across the
//! broker's hosted partitions (all replica roles).

use std::collections::HashMap;

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

pub struct NetworkInCapacity;

impl NetworkInCapacity {
    pub const NAME: &'static str = "NetworkInCapacity";

    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        crate::goals::replica_totals(partitions, broker_ids, |broker, topic, partition| {
            ctx.broker_usages
                .bytes_in_rate(broker, topic, partition, Window::FiveMin, now_ms)
        })
    }
}

impl Goal for NetworkInCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }
    #[allow(clippy::too_many_lines, clippy::cast_precision_loss)]
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let now_ms = crate::goals::now_ms();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();
        let originals = OriginalReplicaState::from_partitions(&state.partitions);

        loop {
            let totals = Self::totals(&working, &broker_ids, ctx, now_ms);
            let mut over: Option<(i32, f64, f64)> = None;
            for (broker, current) in &totals {
                let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                    continue;
                };
                let Some(limit) = cap.network_in_bytes_per_sec else {
                    continue;
                };
                let limit_f = limit as f64;
                if *current > limit_f {
                    let excess = current - limit_f;
                    let prior = over.map_or(0.0, |(_, c, l)| c - l);
                    if excess > prior {
                        over = Some((*broker, *current, limit_f));
                    }
                }
            }
            let Some((hot, _, _)) = over else {
                break;
            };

            // Order candidate destination brokers by current load
            // (lower first). Within ties, smaller broker_id wins.
            let mut candidates: Vec<i32> =
                broker_ids.iter().copied().filter(|b| *b != hot).collect();
            candidates.sort_by(|a, b| {
                let cur_a = totals.get(a).copied().unwrap_or(0.0);
                let cur_b = totals.get(b).copied().unwrap_or(0.0);
                cur_a
                    .partial_cmp(&cur_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.cmp(b))
            });

            // Pick the first (cold, partition_idx) pair where cold isn't
            // already a replica of the chosen partition on `hot`.
            let mut chosen: Option<(i32, usize)> = None;
            for cold in &candidates {
                if let Some(idx) = working.iter().position(|p| {
                    p.replicas.contains(&hot)
                        && !p.replicas.contains(cold)
                        && p.replicas.len() <= state.brokers.len()
                }) {
                    chosen = Some((*cold, idx));
                    break;
                }
            }
            let Some((cold, idx)) = chosen else {
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

    #[allow(clippy::cast_precision_loss)]
    fn is_satisfied_with_ctx(&self, state: &ClusterState, ctx: &GoalContext) -> bool {
        let now_ms = crate::goals::now_ms();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let totals = Self::totals(&state.partitions, &broker_ids, ctx, now_ms);
        for (broker, current) in &totals {
            let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                continue;
            };
            let Some(limit) = cap.network_in_bytes_per_sec else {
                continue;
            };
            if *current > limit as f64 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::*;
    use crate::{
        capacity::{BrokerCapacities, BrokerCapacity},
        model::BrokerView,
        scraper::{MetricKind, UsageStore, WindowConfig, parse::ParsedSample},
    };

    fn ctx_with(caps: BrokerCapacities, store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
            broker_capacities: Arc::new(caps),
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
        // yields the same rate and both samples are inside the 5-min
        // stale-data guard window.
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

    fn caps(broker: i32, bps: u64) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        b.insert(
            broker,
            BrokerCapacity {
                network_in_bytes_per_sec: Some(bps),
                ..Default::default()
            },
        );
        BrokerCapacities { by_broker: b }
    }

    fn caps_many(entries: &[(i32, u64)]) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        for (broker, bps) in entries {
            b.insert(
                *broker,
                BrokerCapacity {
                    network_in_bytes_per_sec: Some(*bps),
                    ..Default::default()
                },
            );
        }
        BrokerCapacities { by_broker: b }
    }

    #[test]
    fn empty_usage_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps(1, 1_000_000), Arc::new(UsageStore::default()));
        assert2::assert!(
            (
                NetworkInCapacity.propose(&s, &ctx).is_empty(),
                NetworkInCapacity.is_satisfied_with_ctx(&s, &ctx)
            ) == (true, true)
        );
    }

    #[test]
    fn over_capacity_emits_movement() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        // 3 partitions × 200kB/s rate = 600kB/s on broker 1; limit 500kB/s.
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 200_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 500_000), store);
        let mvs = NetworkInCapacity.propose(&s, &ctx);
        assert2::assert!(!mvs.is_empty());
    }

    #[test]
    fn is_satisfied_with_ctx_returns_false_when_over() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 200_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 500_000), store);
        assert2::assert!(!NetworkInCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn exact_network_in_capacity_limit_is_satisfied() {
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples: Vec<_> = (0..2).map(|i| (1, "t", i, 0.0, 250_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 500_000), store);

        assert2::assert!(
            (
                NetworkInCapacity.propose(&s, &ctx).is_empty(),
                NetworkInCapacity.is_satisfied_with_ctx(&s, &ctx)
            ) == (true, true)
        );
    }

    #[test]
    fn network_in_capacity_picks_largest_absolute_excess() {
        let parts = vec![
            part("small_limit", 0, vec![1], 1),
            part("large_limit", 0, vec![2], 2),
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_counter_pair(vec![
            (1, "small_limit", 0, 0.0, 1_000.0),
            (2, "large_limit", 0, 0.0, 2_000.0),
        ]);
        let mut ctx = ctx_with(caps_many(&[(1, 100), (2, 1_000)]), store);
        ctx.max_movements_per_proposal = 1;

        let mvs = NetworkInCapacity.propose(&s, &ctx);

        assert2::assert!(
            mvs == vec![Movement {
                topic: "large_limit".into(),
                partition: 0,
                old_replicas: vec![2],
                new_replicas: vec![3],
                old_leader: 2,
                new_leader: 3,
            }]
        );
    }

    #[test]
    fn network_in_capacity_does_not_rank_by_current_plus_limit() {
        let parts = vec![
            part("high_sum", 0, vec![1], 1),
            part("high_excess", 0, vec![2], 2),
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_counter_pair(vec![
            (1, "high_sum", 0, 0.0, 1_000.0),
            (2, "high_excess", 0, 0.0, 600.0),
        ]);
        let mut ctx = ctx_with(caps_many(&[(1, 900), (2, 100)]), store);
        ctx.max_movements_per_proposal = 1;

        let mvs = NetworkInCapacity.propose(&s, &ctx);

        assert2::assert!(
            mvs == vec![Movement {
                topic: "high_excess".into(),
                partition: 0,
                old_replicas: vec![2],
                new_replicas: vec![3],
                old_leader: 2,
                new_leader: 3,
            }]
        );
    }

    #[test]
    fn network_in_capacity_skips_destinations_already_in_replica_set() {
        let parts = vec![part("hot", 0, vec![1, 2], 1)];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_counter_pair(vec![(1, "hot", 0, 0.0, 1_000.0)]);
        let ctx = ctx_with(caps(1, 500), store);

        let mvs = NetworkInCapacity.propose(&s, &ctx);

        assert2::assert!(
            mvs.iter().map(|m| &m.new_replicas).collect::<Vec<_>>() == vec![&vec![3, 2]]
        );
    }

    #[test]
    fn network_in_capacity_stops_when_all_destinations_are_replicas() {
        let parts = vec![part("hot", 0, vec![1, 2], 1)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_counter_pair(vec![(1, "hot", 0, 0.0, 1_000.0)]);
        let ctx = ctx_with(caps(1, 500), store);

        assert2::assert!(NetworkInCapacity.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn network_in_capacity_rehomes_hot_leader_to_replacement() {
        let parts = vec![part("hot", 0, vec![1], 1)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_counter_pair(vec![(1, "hot", 0, 0.0, 1_000.0)]);
        let ctx = ctx_with(caps(1, 500), store);

        let mvs = NetworkInCapacity.propose(&s, &ctx);

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
    fn movement_cap_limits_network_in_capacity_swaps() {
        let parts: Vec<_> = (0..5).map(|i| part("hot", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..5).map(|i| (1, "hot", i, 0.0, 200_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let mut ctx = ctx_with(caps(1, 500_000), store);
        ctx.max_movements_per_proposal = 1;

        let mvs = NetworkInCapacity.propose(&s, &ctx);

        assert2::assert!(mvs.len() == 1);
    }
}
