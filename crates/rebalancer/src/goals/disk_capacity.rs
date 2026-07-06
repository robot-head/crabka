//! Hard goal: enforce a per-broker `disk_bytes` limit using the
//! scraped `UsageStore::disk_bytes_avg` for each (broker, topic,
//! partition) the broker hosts.

use std::collections::HashMap;

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

pub struct DiskCapacity;

impl DiskCapacity {
    pub const NAME: &'static str = "DiskCapacity";

    /// Disk-bytes total per broker (sum of partition `disk_bytes_avg`
    /// for the 5-min window). Skips partitions with no usage data.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        crate::goals::replica_totals(partitions, broker_ids, |broker, topic, partition| {
            ctx.broker_usages
                .disk_bytes_avg(broker, topic, partition, Window::FiveMin, now_ms)
        })
    }
}

impl Goal for DiskCapacity {
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
            // Find a broker exceeding its capacity.
            let mut over: Option<(i32, f64, f64)> = None;
            for (broker, current) in &totals {
                let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                    continue;
                };
                let Some(limit) = cap.disk_bytes else {
                    continue;
                };
                let limit_f = limit as f64;
                if *current > limit_f {
                    let excess = current - limit_f;
                    let prior_excess = over.map_or(0.0, |(_, c, l)| c - l);
                    if excess > prior_excess {
                        over = Some((*broker, *current, limit_f));
                    }
                }
            }
            let Some((hot, _, _)) = over else {
                break;
            };

            // Order candidate destination brokers by disk headroom
            // (more headroom = better). Brokers without a capacity entry
            // fall back to lowest current-usage ordering. Within equal
            // headroom, smaller broker_id wins for determinism.
            let mut candidates: Vec<i32> =
                broker_ids.iter().copied().filter(|b| *b != hot).collect();
            candidates.sort_by(|a, b| {
                let cur_a = totals.get(a).copied().unwrap_or(0.0);
                let cur_b = totals.get(b).copied().unwrap_or(0.0);
                let headroom_a = ctx
                    .broker_capacities
                    .for_broker(*a)
                    .and_then(|c| c.disk_bytes)
                    .map(|l| l as f64 - cur_a);
                let headroom_b = ctx
                    .broker_capacities
                    .for_broker(*b)
                    .and_then(|c| c.disk_bytes)
                    .map(|l| l as f64 - cur_b);
                match (headroom_a, headroom_b) {
                    (Some(ha), Some(hb)) if ha > 0.0 && hb > 0.0 => hb
                        .partial_cmp(&ha)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.cmp(b)),
                    (Some(ha), _) if ha > 0.0 => std::cmp::Ordering::Less,
                    (_, Some(hb)) if hb > 0.0 => std::cmp::Ordering::Greater,
                    _ => cur_a
                        .partial_cmp(&cur_b)
                        .unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| a.cmp(b)),
                }
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
            let Some(limit) = cap.disk_bytes else {
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

    use assert2::assert;

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

    fn store_with_disk(samples: Vec<(i32, &str, i32, f64)>) -> Arc<UsageStore> {
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

    fn caps_with_disk(broker: i32, disk_bytes: u64) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        b.insert(
            broker,
            BrokerCapacity {
                disk_bytes: Some(disk_bytes),
                ..Default::default()
            },
        );
        BrokerCapacities { by_broker: b }
    }

    #[test]
    fn empty_usage_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(
            caps_with_disk(1, 1_000_000),
            Arc::new(UsageStore::default()),
        );
        assert!(DiskCapacity.propose(&s, &ctx).is_empty());
        assert!(DiskCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn over_capacity_emits_movement() {
        // Broker 1 has 3 partitions × 500 = 1500 disk; limit 1000.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 500.0)).collect();
        let store = store_with_disk(samples);
        let ctx = ctx_with(caps_with_disk(1, 1000), store);
        let mvs = DiskCapacity.propose(&s, &ctx);
        assert!(!mvs.is_empty(), "expected eviction; got {mvs:?}");
        for m in &mvs {
            let before = m.old_replicas.iter().filter(|x| **x == 1).count();
            let after = m.new_replicas.iter().filter(|x| **x == 1).count();
            assert!(after < before, "movement must reduce broker 1's replicas");
        }
    }

    #[test]
    fn is_satisfied_with_ctx_returns_false_when_over() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 500.0)).collect();
        let store = store_with_disk(samples);
        let ctx = ctx_with(caps_with_disk(1, 1000), store);
        assert!(!DiskCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn exact_disk_capacity_limit_is_satisfied() {
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..2).map(|i| (1, "t", i, 500.0)).collect();
        let store = store_with_disk(samples);
        let ctx = ctx_with(caps_with_disk(1, 1000), store);
        assert!(DiskCapacity.propose(&s, &ctx).is_empty());
        assert!(DiskCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn disk_capacity_picks_largest_positive_headroom_destination() {
        let parts = vec![part("hot", 0, vec![1], 1), part("warm", 0, vec![3], 3)];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_disk(vec![(1, "hot", 0, 1500.0), (3, "warm", 0, 900.0)]);
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                disk_bytes: Some(1000),
                ..Default::default()
            },
        );
        by.insert(
            2,
            BrokerCapacity {
                disk_bytes: Some(100),
                ..Default::default()
            },
        );
        by.insert(
            3,
            BrokerCapacity {
                disk_bytes: Some(1400),
                ..Default::default()
            },
        );
        let ctx = ctx_with(BrokerCapacities { by_broker: by }, store);

        let mvs = DiskCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].new_replicas == vec![3]);
    }

    #[test]
    fn disk_capacity_prefers_positive_headroom_over_uncapped_low_usage() {
        let parts = vec![part("hot", 0, vec![1], 1)];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_disk(vec![(1, "hot", 0, 1500.0)]);
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                disk_bytes: Some(1000),
                ..Default::default()
            },
        );
        by.insert(
            2,
            BrokerCapacity {
                disk_bytes: Some(2000),
                ..Default::default()
            },
        );
        let ctx = ctx_with(BrokerCapacities { by_broker: by }, store);

        let mvs = DiskCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].new_replicas == vec![2]);
    }

    #[test]
    fn disk_capacity_falls_back_to_current_load_without_positive_headroom() {
        let parts = vec![part("hot", 0, vec![1], 1), part("busy", 0, vec![2], 2)];
        let s = state_with(parts, vec![1, 2, 3]);
        let store = store_with_disk(vec![(1, "hot", 0, 1500.0), (2, "busy", 0, 900.0)]);
        let mut by = std::collections::HashMap::new();
        by.insert(
            1,
            BrokerCapacity {
                disk_bytes: Some(1000),
                ..Default::default()
            },
        );
        by.insert(
            2,
            BrokerCapacity {
                disk_bytes: Some(900),
                ..Default::default()
            },
        );
        by.insert(
            3,
            BrokerCapacity {
                disk_bytes: Some(0),
                ..Default::default()
            },
        );
        let ctx = ctx_with(BrokerCapacities { by_broker: by }, store);

        let mvs = DiskCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
        assert!(mvs[0].new_replicas == vec![3]);
    }

    #[test]
    fn disk_capacity_skips_destinations_already_in_replica_set() {
        let parts = vec![part("hot", 0, vec![1, 2], 1)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_disk(vec![(1, "hot", 0, 1500.0)]);
        let ctx = ctx_with(caps_with_disk(1, 1000), store);

        assert!(DiskCapacity.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn disk_capacity_rehomes_hot_leader_to_replacement() {
        let parts = vec![part("hot", 0, vec![1], 1)];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_disk(vec![(1, "hot", 0, 1500.0)]);
        let ctx = ctx_with(caps_with_disk(1, 1000), store);

        let mvs = DiskCapacity.propose(&s, &ctx);

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
    fn movement_cap_limits_disk_capacity_swaps() {
        let parts: Vec<_> = (0..3).map(|i| part("hot", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "hot", i, 600.0)).collect();
        let store = store_with_disk(samples);
        let mut ctx = ctx_with(caps_with_disk(1, 1000), store);
        ctx.max_movements_per_proposal = 1;

        let mvs = DiskCapacity.propose(&s, &ctx);

        assert!(mvs.len() == 1);
    }
}
