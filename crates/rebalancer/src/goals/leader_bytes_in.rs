//! Soft goal: balance the producer-driven ingress load per broker. The
//! per-broker total is the sum, over the partitions where the broker is the
//! current leader, of
//! `UsageStore::bytes_in_rate(broker, topic, partition, FiveMin)`. This differs
//! from `NetworkInUsage`, which sums over every replica and therefore includes
//! follower replication traffic.

use std::collections::{HashMap, HashSet};

use crabka_units::{Ratio, convert::ByteRateExt};

use crate::{
    goals::{Goal, GoalContext, GoalPriority, OriginalReplicaState},
    model::{ClusterState, Movement, PartitionView},
    scraper::Window,
};

pub struct LeaderBytesIn;

impl LeaderBytesIn {
    pub const NAME: &'static str = "LeaderBytesIn";

    /// Leader-bytes-in rate per broker, in bytes/sec.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
        now_ms: i64,
    ) -> HashMap<i32, f64> {
        crate::goals::leader_totals(partitions, broker_ids, |broker, topic, partition| {
            ctx.broker_usages
                .bytes_in_rate(broker, topic, partition, Window::FiveMin, now_ms)
                .map(ByteRateExt::bytes_per_sec_f64)
        })
    }

    fn imbalance(totals: &HashMap<i32, f64>) -> Ratio {
        crate::goals::imbalance_ratio_f64(totals)
    }
}

impl Goal for LeaderBytesIn {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        // For LeaderBytesIn, the lever is *leader election*, not replica
        // movement: shift leadership from hot brokers to cold ones.
        // Mirrors the LeaderDistribution goal's shape (leader-only swap).
        let now_ms = crate::goals::now_ms();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();
        // Avoid oscillation: do not promote the same partition's leader
        // twice in one proposal. `bytes_in_rate` for the new leader is
        // measured from before the swap took effect, so re-evaluating
        // would otherwise let the algorithm shuffle leadership back and
        // forth.
        let mut swapped: HashSet<(String, i32)> = HashSet::new();

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

            // Find a partition currently led by `hot` whose replica set
            // includes `cold` AND `cold` is in ISR (Kafka invariant).
            // Skip partitions already swapped this proposal to avoid
            // oscillation.
            let idx = working.iter().position(|p| {
                p.leader == hot
                    && p.replicas.contains(&cold)
                    && p.isr.contains(&cold)
                    && !swapped.contains(&(p.topic.clone(), p.partition))
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            out.push(originals.change_leader(p, cold));
            swapped.insert(key);

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

    use assert2::check;
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
        // Each entry: (broker, topic, partition, v_t0, v_t1).
        // Inserts at t=now-1000 and t=now so rate = (v_t1 - v_t0)/sec
        // and both samples sit inside the 5-min stale-data guard window.
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
        assert2::assert!(LeaderBytesIn.propose(&s, &ctx).is_empty());
    }

    #[test]
    fn hot_leader_triggers_leader_only_swaps() {
        // All partitions led by broker 1 with high ingress; broker 2 idle.
        // Each partition's replica set is [1, 2] so cold=2 is in ISR.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = (0..3)
            .map(|i| (1, "t", i, 0.0, 100_000.0)) // broker 1 leader: 100kB/s per partition
            .chain((0..3).map(|i| (2, "t", i, 0.0, 1.0))) // broker 2 follower: ~nothing
            .collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = LeaderBytesIn.propose(&s, &ctx);
        assert2::assert!(!mvs.is_empty());
        for m in &mvs {
            check!(
                (&m.old_replicas, &m.new_replicas, m.new_leader)
                    == (&m.new_replicas, &m.new_replicas, 2)
            );
        }
    }

    #[test]
    fn cold_broker_not_in_isr_skipped() {
        // Hot broker 1 with high traffic; cold broker 2 is in replicas
        // but NOT in ISR -- can't be promoted.
        let parts: Vec<_> = (0..3)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1], // 2 not in ISR
            })
            .collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = (0..3).map(|i| (1, "t", i, 0.0, 100_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(store);
        let mvs = LeaderBytesIn.propose(&s, &ctx);
        for m in &mvs {
            assert2::assert!(m.new_leader != 2);
        }
    }

    #[test]
    fn imbalance_is_spread_over_total() {
        let totals = std::collections::HashMap::from([(1, 300.0), (2, 100.0)]);
        assert2::assert!(LeaderBytesIn::imbalance(&totals) == percent(50));
    }

    #[test]
    fn nonzero_imbalanced_leader_bytes_moves_leadership() {
        let parts = vec![
            part("hot", 0, vec![1, 2], 1),
            part("cold", 0, vec![1, 2], 2),
        ];
        let s = state_with(parts, vec![1, 2]);
        let store = store_with_counter_pair(vec![
            (1, "hot", 0, 0.0, 300_000.0),
            (2, "cold", 0, 0.0, 100_000.0),
        ]);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = LeaderBytesIn.propose(&s, &ctx);

        assert2::assert!(
            mvs == vec![Movement {
                topic: "hot".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 1,
                new_leader: 2,
            }]
        );
    }

    #[test]
    fn movement_cap_limits_leader_bytes_in_swaps() {
        let parts: Vec<_> = (0..5).map(|i| part("hot", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples = (0..5)
            .map(|i| (1, "hot", i, 0.0, 100_000.0))
            .chain((0..5).map(|i| (2, "hot", i, 0.0, 1.0)))
            .collect();
        let store = store_with_counter_pair(samples);
        let mut ctx = ctx_with(store);
        ctx.max_movements_per_proposal = 1;

        let mvs = LeaderBytesIn.propose(&s, &ctx);

        assert2::assert!(mvs.len() == 1);
    }
}
