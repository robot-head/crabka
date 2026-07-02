//! Hard goal: enforce a per-broker `cpu_cores` limit using the
//! scraped `UsageStore::cpu_micros_rate` summed across the broker's
//! hosted partitions (all replica roles — produce work hits the
//! leader, fetch / replication work hits everyone serving).
//!
//! The rate is in microseconds-of-CPU per second; dividing by
//! `1_000_000` yields the equivalent number of CPU cores in use. The
//! `cpu_cores` capacity is a fractional core count (`f64`).

use std::collections::HashMap;

use crate::{
    goals::{Goal, GoalContext, GoalPriority},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

/// Conversion factor from `cpu_micros_rate` (micros/sec) to cores.
const MICROS_PER_CORE_SECOND: f64 = 1_000_000.0;

pub struct CpuCapacity;

impl CpuCapacity {
    pub const NAME: &'static str = "CpuCapacity";

    /// Per-broker cores-in-use total. Skips partitions with no usage
    /// data — same fail-safe pattern as the other capacity goals.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(rate_micros) = ctx.broker_usages.cpu_micros_rate(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                    now_ms,
                ) {
                    *m.entry(*replica).or_insert(0.0) += rate_micros / MICROS_PER_CORE_SECOND;
                }
            }
        }
        m
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
    use std::{sync::Arc, time::Duration};

    use assert2::{assert, check};

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
        assert!(CpuCapacity.propose(&s, &ctx).is_empty());
        assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
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
        // Same 0.6 cores/partition × 3 = 1.8 cores; limit 1.0.
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert!(!CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn is_satisfied_with_ctx_returns_true_when_within_capacity() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        // 100_000 micros/sec × 3 partitions = 0.3 cores; limit 1.0 — fine.
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 100_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
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
        assert!(CpuCapacity.propose(&s, &ctx).is_empty());
        assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn exact_cpu_capacity_limit_is_satisfied() {
        let parts: Vec<_> = (0..2).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        // 500_000 micros/sec x 2 partitions = exactly 1.0 core.
        let samples: Vec<_> = (0..2).map(|i| (1, "t", i, 0.0, 500_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert!(CpuCapacity.propose(&s, &ctx).is_empty());
        assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn zero_cpu_capacity_is_treated_as_unlimited() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 0.0), store);
        assert!(CpuCapacity.propose(&s, &ctx).is_empty());
        assert!(CpuCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn full_replica_set_has_no_legal_cpu_capacity_move() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        assert!(CpuCapacity.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn cpu_capacity_rehomes_leader_when_hot_leader_moves() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 600_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 1.0), store);
        let mvs = CpuCapacity.propose(&s, &ctx);
        assert!(!mvs.is_empty());
        check!(mvs[0].old_leader == 1);
        check!(mvs[0].new_leader != 1);
        check!(mvs[0].new_replicas.contains(&mvs[0].new_leader));
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
        assert!(mvs.len() == 1);
    }
}
