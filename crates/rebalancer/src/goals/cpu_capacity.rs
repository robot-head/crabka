//! Hard goal: enforce a per-broker `cpu_cores` limit. It uses the scraped
//! `UsageStore::cpu_cores_rate`, summed across the broker's hosted partitions
//! in all replica roles. Produce work hits the leader, and fetch and
//! replication work hits every broker that serves the partition.
//!
//! Both the measured rate and the `cpu_cores` capacity are core counts, so the
//! comparison is a plain `f64` comparison.

use std::collections::HashMap;

use crabka_units::convert::RatioExt;

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

pub struct CpuCapacity;

impl CpuCapacity {
    pub const NAME: &'static str = "CpuCapacity";

    /// Per-broker cores-in-use total. It skips partitions with no usage data.
    /// This is the same fail-safe pattern as the other capacity goals.
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
}

impl Goal for CpuCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }
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
                let Some(limit) = cap.cpu_cores else {
                    continue;
                };
                // Defensive: a non-finite or non-positive limit is an
                // operator-config bug, but treating it as "no limit"
                // avoids spurious movements.
                if !limit.is_finite() || limit <= 0.0 {
                    continue;
                }
                if *current > limit {
                    let excess = current - limit;
                    let prior = over.map_or(0.0, |(_, c, l)| c - l);
                    if excess > prior {
                        over = Some((*broker, *current, limit));
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

    fn is_satisfied_with_ctx(&self, state: &ClusterState, ctx: &GoalContext) -> bool {
        let now_ms = crate::goals::now_ms();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let totals = Self::totals(&state.partitions, &broker_ids, ctx, now_ms);
        for (broker, current) in &totals {
            let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                continue;
            };
            let Some(limit) = cap.cpu_cores else {
                continue;
            };
            if !limit.is_finite() || limit <= 0.0 {
                continue;
            }
            if *current > limit {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crabka_units::prelude::*;

    use super::*;
    use crate::{
        capacity::{BrokerCapacities, BrokerCapacity},
        model::BrokerView,
        scraper::{MetricKind, UsageStore, WindowConfig, parse::ParsedSample},
    };

    fn ctx_with(caps: BrokerCapacities, store: Arc<UsageStore>) -> GoalContext {
        GoalContext {
            imbalance_threshold: percent(10),
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

    fn caps(broker: i32, cores: f64) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        b.insert(
            broker,
            BrokerCapacity {
                cpu_cores: Some(cores),
                ..Default::default()
            },
        );
        BrokerCapacities { by_broker: b }
    }

    #[test]
    fn empty_usage_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps(1, 8.0), Arc::new(UsageStore::default()));
        assert2::assert!(
            (
                CpuCapacity.propose(&s, &ctx).is_empty(),
                CpuCapacity.is_satisfied_with_ctx(&s, &ctx)
            ) == (true, true)
        );
    }

    #[test]
    fn over_capacity_emits_movement() {
        // Broker 1 hosts 3 partitions, each burning 600_000 micros/sec
        // = 0.6 cores. Sum = 1.8 cores. Limit = 1.0 core. Movement
        // must be emitted.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        let mvs = CpuCapacity.propose(&s, &ctx);
        assert2::assert!(!mvs.is_empty());
        for m in &mvs {
            let before = m.old_replicas.iter().filter(|x| **x == 1).count();
            let after = m.new_replicas.iter().filter(|x| **x == 1).count();
            assert2::assert!(after < before);
        }
    }

    #[test]
    fn is_satisfied_with_ctx_returns_false_when_over() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        // Same 0.6 cores/partition × 3 = 1.8 cores; limit 1.0.
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert2::assert!(!CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn is_satisfied_with_ctx_returns_true_when_within_capacity() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        // 100_000 micros/sec × 3 partitions = 0.3 cores; limit 1.0 — fine.
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 100_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert2::assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn non_finite_capacity_treated_as_unlimited() {
        // NaN limit must not trigger spurious movement. Non-finite
        // values are ignored rather than rejecting the proposal.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, f64::NAN), store);
        assert2::assert!(
            (
                CpuCapacity.propose(&s, &ctx).is_empty(),
                CpuCapacity.is_satisfied_with_ctx(&s, &ctx)
            ) == (true, true)
        );
    }

    #[test]
    fn exact_cpu_capacity_limit_is_satisfied() {
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        // 500_000 micros/sec x 2 partitions = exactly 1.0 core.
        let samples: Vec<_> = (0..2).map(|i| (1, "t", i, 0.0, 500_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert2::assert!(
            (
                CpuCapacity.propose(&s, &ctx).is_empty(),
                CpuCapacity.is_satisfied_with_ctx(&s, &ctx)
            ) == (true, true)
        );
    }

    #[test]
    fn zero_cpu_capacity_is_treated_as_unlimited() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 0.0), store);
        assert2::assert!(CpuCapacity.propose(&s, &ctx).is_empty());
        assert2::assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn full_replica_set_has_no_legal_cpu_capacity_move() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert2::assert!(CpuCapacity.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn cpu_capacity_rehomes_leader_when_hot_leader_moves() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        let mvs = CpuCapacity.propose(&s, &ctx);
        assert2::assert!(
            (
                mvs.is_empty(),
                mvs[0].old_leader,
                mvs[0].new_leader != 1,
                mvs[0].new_replicas.contains(&mvs[0].new_leader),
            ) == (false, 1, true, true)
        );
    }

    #[test]
    fn movement_cap_limits_cpu_capacity_swaps() {
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..5).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let mut ctx = ctx_with(caps(1, 1.0), store);
        ctx.max_movements_per_proposal = 1;
        let mvs = CpuCapacity.propose(&s, &ctx);
        assert2::assert!(mvs.len() == 1);
    }
}
