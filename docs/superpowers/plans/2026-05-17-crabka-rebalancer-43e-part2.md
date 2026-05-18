# Slice 43e — Implementation Plan (Part 2: T11–T17)

> **Continuation of** [`2026-05-17-crabka-rebalancer-43e.md`](2026-05-17-crabka-rebalancer-43e.md). Part 1 (T1–T10) lives there. This file holds T11 through T17 — capacity real bodies, optimizer switch, registry growth, binary wiring, integration test, Helm chart, STATUS.

Branch: `feature/rebalancer-43e`. Working dir: `/home/matt/git/crabka`.

---

## Batch 7 (continued) — Capacity real bodies

### Task 11: Capacity goals — three stubs become real, ReplicaCapacity gets `is_satisfied_with_ctx`

**Files:**
- Modify: `crates/rebalancer/src/goals/disk_capacity.rs`
- Modify: `crates/rebalancer/src/goals/network_in_capacity.rs`
- Modify: `crates/rebalancer/src/goals/network_out_capacity.rs`
- Modify: `crates/rebalancer/src/goals/replica_capacity.rs`

`cpu_capacity.rs` is untouched in 43e — still a stub.

- [ ] **Step 1: Replace `crates/rebalancer/src/goals/disk_capacity.rs`**

```rust
//! Hard goal: enforce a per-broker `disk_bytes` limit using the
//! scraped `UsageStore::disk_bytes_avg` for each (broker, topic,
//! partition) the broker hosts. Slice 43e wires the real body; the
//! 43d stub returning empty Vec is replaced.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct DiskCapacity;

impl DiskCapacity {
    pub const NAME: &'static str = "DiskCapacity";

    /// Disk-bytes total per broker (sum of partition disk_bytes_avg
    /// for the 5-min window). Skips partitions with no usage data.
    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(bytes) = ctx.broker_usages.disk_bytes_avg(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                ) {
                    *m.entry(*replica).or_insert(0.0) += bytes;
                }
            }
        }
        m
    }
}

impl Goal for DiskCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
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
            let totals = Self::totals(&working, &broker_ids, ctx);
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
                    let prior_excess = over.map(|(_, c, l)| c - l).unwrap_or(0.0);
                    if excess > prior_excess {
                        over = Some((*broker, *current, limit_f));
                    }
                }
            }
            let Some((hot, _, _)) = over else {
                break;
            };

            // Pick a destination broker with disk headroom.
            let cold = broker_ids
                .iter()
                .filter(|b| **b != hot)
                .min_by(|a, b| {
                    let cur_a = totals.get(a).copied().unwrap_or(0.0);
                    let cur_b = totals.get(b).copied().unwrap_or(0.0);
                    let headroom_a = ctx
                        .broker_capacities
                        .for_broker(**a)
                        .and_then(|c| c.disk_bytes)
                        .map(|l| l as f64 - cur_a);
                    let headroom_b = ctx
                        .broker_capacities
                        .for_broker(**b)
                        .and_then(|c| c.disk_bytes)
                        .map(|l| l as f64 - cur_b);
                    // Brokers with headroom (positive) sort first; ties by broker id.
                    match (headroom_a, headroom_b) {
                        (Some(ha), Some(hb)) if ha > 0.0 && hb > 0.0 => {
                            hb.partial_cmp(&ha).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.cmp(b))
                        }
                        (Some(ha), _) if ha > 0.0 => std::cmp::Ordering::Less,
                        (_, Some(hb)) if hb > 0.0 => std::cmp::Ordering::Greater,
                        _ => cur_a.partial_cmp(&cur_b).unwrap_or(std::cmp::Ordering::Equal),
                    }
                })
                .copied();
            let Some(cold) = cold else {
                break;
            };

            // Find a partition on hot whose replica set doesn't include cold.
            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() <= state.brokers.len()
            });
            let Some(idx) = idx else {
                break;
            };

            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == hot).expect("hot present");
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
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let totals = Self::totals(&state.partitions, &broker_ids, ctx);
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
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

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
            retention: Duration::from_secs(3600),
        });
        for (broker, topic, partition, value) in samples {
            store.insert(
                broker,
                vec![ParsedSample {
                    metric: MetricKind::DiskBytes,
                    topic: topic.into(),
                    partition,
                    value,
                }],
                0,
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
        let ctx = ctx_with(caps_with_disk(1, 1_000_000), Arc::new(UsageStore::default()));
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
}
```

- [ ] **Step 2: Replace `crates/rebalancer/src/goals/network_in_capacity.rs`**

Mirror `disk_capacity.rs` but using `bytes_in_rate` and the `network_in_bytes_per_sec` capacity field. Full file:

```rust
//! Hard goal: enforce a per-broker `network_in_bytes_per_sec` limit
//! using the scraped `UsageStore::bytes_in_rate` summed across the
//! broker's hosted partitions (all replica roles).

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};
use crate::scraper::Window;

pub struct NetworkInCapacity;

impl NetworkInCapacity {
    pub const NAME: &'static str = "NetworkInCapacity";

    fn totals(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        ctx: &GoalContext,
    ) -> HashMap<i32, f64> {
        let mut m: HashMap<i32, f64> = broker_ids.iter().map(|b| (*b, 0.0)).collect();
        for p in partitions {
            for replica in &p.replicas {
                if let Some(rate) = ctx.broker_usages.bytes_in_rate(
                    *replica,
                    &p.topic,
                    p.partition,
                    Window::FiveMin,
                ) {
                    *m.entry(*replica).or_insert(0.0) += rate;
                }
            }
        }
        m
    }
}

impl Goal for NetworkInCapacity {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
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
            let totals = Self::totals(&working, &broker_ids, ctx);
            let mut over: Option<(i32, f64, f64)> = None;
            for (broker, current) in &totals {
                let Some(cap) = ctx.broker_capacities.for_broker(*broker) else { continue };
                let Some(limit) = cap.network_in_bytes_per_sec else { continue };
                let limit_f = limit as f64;
                if *current > limit_f {
                    let excess = current - limit_f;
                    let prior = over.map(|(_, c, l)| c - l).unwrap_or(0.0);
                    if excess > prior {
                        over = Some((*broker, *current, limit_f));
                    }
                }
            }
            let Some((hot, _, _)) = over else { break };

            let cold = broker_ids.iter().filter(|b| **b != hot).min_by(|a, b| {
                let cur_a = totals.get(a).copied().unwrap_or(0.0);
                let cur_b = totals.get(b).copied().unwrap_or(0.0);
                cur_a.partial_cmp(&cur_b).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.cmp(b))
            }).copied();
            let Some(cold) = cold else { break };

            let idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    && p.replicas.len() <= state.brokers.len()
            });
            let Some(idx) = idx else { break };
            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == hot).expect("hot present");
            p.replicas[pos] = cold;
            let new_leader = if p.leader == hot {
                *p.replicas.iter().find(|r| p.isr.contains(r)).unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };
            let old_replicas = original_replicas.get(&key).cloned().unwrap_or_else(|| p.replicas.clone());
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
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let totals = Self::totals(&state.partitions, &broker_ids, ctx);
        for (broker, current) in &totals {
            let Some(cap) = ctx.broker_capacities.for_broker(*broker) else { continue };
            let Some(limit) = cap.network_in_bytes_per_sec else { continue };
            if *current > limit as f64 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

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
        PartitionView { topic: topic.into(), partition, replicas, leader, isr }
    }

    fn store_with_counter_pair(samples: Vec<(i32, &str, i32, f64, f64)>) -> Arc<UsageStore> {
        let store = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_secs(3600),
        });
        for (broker, topic, partition, v_t0, _) in &samples {
            store.insert(
                *broker,
                vec![ParsedSample {
                    metric: MetricKind::BytesIn,
                    topic: (*topic).into(),
                    partition: *partition,
                    value: *v_t0,
                }],
                0,
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
                1000,
            );
        }
        Arc::new(store)
    }

    fn caps(broker: i32, bps: u64) -> BrokerCapacities {
        let mut b = std::collections::HashMap::new();
        b.insert(broker, BrokerCapacity { network_in_bytes_per_sec: Some(bps), ..Default::default() });
        BrokerCapacities { by_broker: b }
    }

    #[test]
    fn empty_usage_no_op() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps(1, 1_000_000), Arc::new(UsageStore::default()));
        assert!(NetworkInCapacity.propose(&s, &ctx).is_empty());
        assert!(NetworkInCapacity.is_satisfied_with_ctx(&s, &ctx));
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
        assert!(!mvs.is_empty(), "expected eviction; got {mvs:?}");
    }

    #[test]
    fn is_satisfied_with_ctx_returns_false_when_over() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let samples: Vec<_> = (0..3).map(|i| (1, "t", i, 0.0, 200_000.0)).collect();
        let store = store_with_counter_pair(samples);
        let ctx = ctx_with(caps(1, 500_000), store);
        assert!(!NetworkInCapacity.is_satisfied_with_ctx(&s, &ctx));
    }
}
```

- [ ] **Step 3: Replace `crates/rebalancer/src/goals/network_out_capacity.rs`**

Mirror `network_in_capacity.rs` exactly, swapping `bytes_in_rate` → `bytes_out_rate`, `network_in_bytes_per_sec` → `network_out_bytes_per_sec`, `BytesIn` → `BytesOut`, `NetworkInCapacity` → `NetworkOutCapacity`. Same 3-test pattern.

(Full file omitted here for brevity but follows the network_in_capacity.rs structure mechanically — substitute the four name pairs above throughout the file and test module.)

- [ ] **Step 4: Add `is_satisfied_with_ctx` to `crates/rebalancer/src/goals/replica_capacity.rs`**

Read the file. Find the existing `is_satisfied` impl (currently returns `true` unconditionally — the 43d known trade). Add `is_satisfied_with_ctx` to the `impl Goal for ReplicaCapacity` block:

```rust
    fn is_satisfied_with_ctx(&self, state: &ClusterState, ctx: &GoalContext) -> bool {
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let counts = Self::counts(&state.partitions, &broker_ids);
        for (broker, current) in &counts {
            let Some(cap) = ctx.broker_capacities.for_broker(*broker) else {
                continue;
            };
            let Some(limit) = cap.max_replicas else {
                continue;
            };
            if *current > limit as usize {
                return false;
            }
        }
        true
    }
```

The existing `is_satisfied(&self, _state)` keeps returning `true` (unchanged — the public default-impl signature can't see context). The new method closes 43d's known trade.

Add a unit test:

```rust
    #[test]
    fn is_satisfied_with_ctx_returns_false_when_over_capacity() {
        // Broker 1 has 5 replicas but max_replicas: 3 → not satisfied.
        let parts: Vec<_> = (0..5).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let ctx = ctx_with(caps_with(1, 3));
        assert!(!ReplicaCapacity.is_satisfied_with_ctx(&s, &ctx));
    }

    #[test]
    fn is_satisfied_with_ctx_returns_true_when_within() {
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2]);
        let ctx = ctx_with(caps_with(1, 10));
        assert!(ReplicaCapacity.is_satisfied_with_ctx(&s, &ctx));
    }
```

These tests use the existing `ctx_with` / `state_with` / `caps_with` / `part` helpers already in `replica_capacity::tests`.

- [ ] **Step 5: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::disk_capacity goals::network_in_capacity goals::network_out_capacity goals::replica_capacity -- --nocapture
```

Expected: ~13 tests pass (3 per modified file + 2 new ReplicaCapacity tests).

```bash
cargo clippy -p crabka-rebalancer --all-targets -- -D warnings 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/disk_capacity.rs crates/rebalancer/src/goals/network_in_capacity.rs crates/rebalancer/src/goals/network_out_capacity.rs crates/rebalancer/src/goals/replica_capacity.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): three capacity stubs become real + ReplicaCapacity is_satisfied_with_ctx

DiskCapacity / NetworkInCapacity / NetworkOutCapacity now consult
ctx.broker_usages (UsageStore) for per-partition rates and
disk_bytes, sum per broker, and emit movements when a broker exceeds
its respective limit. Each adds an is_satisfied_with_ctx override
so the optimizer's incremental hard-goal validation can reject
soft movements that would re-break the invariant.

ReplicaCapacity also gains is_satisfied_with_ctx — closes the 43d
known trade where its plain is_satisfied was unconditionally true.

CpuCapacity remains a stub (slice 43f).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 8 — Optimizer + Registry + Binary (parallel: T12, T13, T14)

### Task 12: Optimizer switches to `is_satisfied_with_ctx` + regression test

**Files:**
- Modify: `crates/rebalancer/src/optimizer/mod.rs`

- [ ] **Step 1: Locate the existing `is_satisfied` call**

The 43c slice added an incremental hard-goal validation in the optimizer's accumulator loop. Find that call site:

```bash
grep -n "is_satisfied" /home/matt/git/crabka/crates/rebalancer/src/optimizer/mod.rs | head -5
```

You should see one call like `gg.is_satisfied(&tentative)` inside the soft-movement-validation block.

- [ ] **Step 2: Switch to the new method**

Replace `gg.is_satisfied(&tentative)` with `gg.is_satisfied_with_ctx(&tentative, ctx)`. The `ctx: &GoalContext` is already in scope in the surrounding `optimize` function — pass it through.

- [ ] **Step 3: Add a regression test**

In the `#[cfg(test)] mod tests` block of `optimizer/mod.rs`, append:

```rust
#[test]
fn soft_movement_that_violates_capacity_invariant_is_dropped() {
    use crate::capacity::{BrokerCapacities, BrokerCapacity};
    use crate::goals::disk_capacity::DiskCapacity;
    use crate::goals::tests::FixedGoal;
    use crate::model::BrokerView;
    use crate::scraper::parse::ParsedSample;
    use crate::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

    // Three brokers; broker 3 is small (disk_bytes: 1000).
    // Broker 1 holds partition replicas at 0 disk_bytes. A soft goal
    // proposes moving a replica to broker 3 — but doing so would push
    // broker 3 over its limit (the partition would contribute 600 bytes
    // making broker 3's total 1500 > 1000). DiskCapacity::is_satisfied_with_ctx
    // must catch this and the optimizer must drop the movement.
    let state = ClusterState {
        cluster_id: None,
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None },
            BrokerView { id: 2, host: "h2".into(), port: 9092, rack: None },
            BrokerView { id: 3, host: "h3".into(), port: 9092, rack: None },
        ],
        partitions: vec![PartitionView {
            topic: "t".into(),
            partition: 0,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        }],
        in_flight_reassignments: vec![],
    };
    let mut by = std::collections::HashMap::new();
    by.insert(3, BrokerCapacity { disk_bytes: Some(1000), ..Default::default() });
    let caps = BrokerCapacities { by_broker: by };

    let store = UsageStore::new(WindowConfig {
        scrape_interval: Duration::from_secs(30),
        retention: Duration::from_secs(3600),
    });
    // Broker 3 already at 900 disk_bytes from another partition (not
    // in this state); the optimizer's tentative-apply will add the
    // moved partition's 600 bytes, blowing the 1000 cap.
    store.insert(
        3,
        vec![ParsedSample {
            metric: MetricKind::DiskBytes,
            topic: "other".into(),
            partition: 0,
            value: 900.0,
        }],
        0,
    );
    store.insert(
        3,
        vec![ParsedSample {
            metric: MetricKind::DiskBytes,
            topic: "t".into(),
            partition: 0,
            value: 600.0,
        }],
        0,
    );
    let ctx = GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(caps),
        broker_usages: Arc::new(store),
    };

    let bad_soft = FixedGoal {
        name: "bad_soft",
        priority: GoalPriority::Soft,
        movements: vec![Movement {
            topic: "t".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3], // moves replica from 2 to 3
            old_leader: 1,
            new_leader: 1,
        }],
    };

    let goals: Vec<&dyn Goal> = vec![&DiskCapacity, &bad_soft];
    let out = optimize(&state, &goals, &ctx).unwrap();
    assert!(
        out.proposal.movements.is_empty(),
        "soft move that pushes broker 3 over disk cap must be dropped; got {:?}",
        out.proposal.movements
    );
}
```

The test demonstrates the closed loop: 43c's incremental validation now uses `is_satisfied_with_ctx`, and `DiskCapacity::is_satisfied_with_ctx` consults `ctx.broker_usages` to detect the would-be-over state.

- [ ] **Step 4: Tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib optimizer -- --nocapture
```

Expected: existing optimizer tests + the new one pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "optimizer"
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/optimizer/mod.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): optimizer switches to is_satisfied_with_ctx

Incremental hard-goal validation in optimize() now calls
gg.is_satisfied_with_ctx(&tentative, ctx) instead of
gg.is_satisfied(&tentative). Soft goals retain default forwarding;
the four capacity goals (Replica/Disk/NetworkIn/NetworkOut) now
properly defend their invariants against soft-goal interference
— closes 43d's known trade.

New regression test
soft_movement_that_violates_capacity_invariant_is_dropped
demonstrates DiskCapacity rejecting a tentative soft move that
would push a broker over its disk_bytes limit.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 13: `GoalRegistry::default_registry` grows from 11 to 15 goals

**Files:**
- Modify: `crates/rebalancer/src/api/mod.rs`

- [ ] **Step 1: Update `default_registry`**

Add four new goals (soft) to the end of the existing list:

```rust
pub fn default_registry() -> Self {
    Self {
        goals: vec![
            // Hard goals (priority order matters).
            Box::new(crate::goals::preferred_leader_idempotency::PreferredLeaderIdempotency),
            Box::new(crate::goals::rack_aware::RackAware),
            Box::new(crate::goals::replica_capacity::ReplicaCapacity),
            Box::new(crate::goals::disk_capacity::DiskCapacity),
            Box::new(crate::goals::network_in_capacity::NetworkInCapacity),
            Box::new(crate::goals::network_out_capacity::NetworkOutCapacity),
            Box::new(crate::goals::cpu_capacity::CpuCapacity),
            // Soft goals.
            Box::new(crate::goals::replica_distribution::ReplicaDistribution),
            Box::new(crate::goals::leader_distribution::LeaderDistribution),
            Box::new(crate::goals::topic_replica_distribution::TopicReplicaDistribution),
            Box::new(crate::goals::min_topic_leaders_per_broker::MinTopicLeadersPerBroker),
            // New in 43e:
            Box::new(crate::goals::disk_usage::DiskUsage),
            Box::new(crate::goals::leader_bytes_in::LeaderBytesIn),
            Box::new(crate::goals::network_in_usage::NetworkInUsage),
            Box::new(crate::goals::network_out_usage::NetworkOutUsage),
        ],
    }
}
```

- [ ] **Step 2: Update tests**

Rename `default_registry_has_eleven_goals` to `default_registry_has_fifteen_goals` and bump the assertion to 15.

Update `default_registry_order_matches_spec` (added in 43d) to include the four new names at the end of the expected vec:

```rust
assert_eq!(
    names,
    vec![
        "PreferredLeaderIdempotency",
        "RackAware",
        "ReplicaCapacity",
        "DiskCapacity",
        "NetworkInCapacity",
        "NetworkOutCapacity",
        "CpuCapacity",
        "ReplicaDistribution",
        "LeaderDistribution",
        "TopicReplicaDistribution",
        "MinTopicLeadersPerBroker",
        "DiskUsage",
        "LeaderBytesIn",
        "NetworkInUsage",
        "NetworkOutUsage",
    ],
    "registry order must match the spec's documented priority"
);
```

- [ ] **Step 3: Tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib api -- --nocapture
```

Expected: existing api tests + the renamed one pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "api/mod.rs"
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/api/mod.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): GoalRegistry adds 4 new soft goals (15 total)

DiskUsage, LeaderBytesIn, NetworkInUsage, NetworkOutUsage join the
soft tier of default_registry. Test default_registry_has_eleven_goals
renamed to default_registry_has_fifteen_goals;
default_registry_order_matches_spec updated with the new four
appended.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 14: Binary wiring — 3 new CLI flags + Scraper spawn + UsageStore threading

**Files:**
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`

- [ ] **Step 1: Add the CLI flags**

Read the file. Find the `Args` struct. Add three new fields near the existing `--broker-capacity-file`:

```rust
    /// Per-broker metric scrape targets. Format: "id:host:port,id:host:port,…".
    /// Empty = scraper disabled (usage-driven goals are no-ops).
    #[arg(long, env = "CRABKA_METRICS_SCRAPE_TARGETS", default_value = "")]
    metrics_scrape_targets: String,

    /// How often the scraper polls each target's /metrics endpoint.
    #[arg(long, env = "CRABKA_METRICS_SCRAPE_INTERVAL_SECS", default_value_t = 30)]
    metrics_scrape_interval_secs: u64,

    /// How long to retain scraped samples in the rolling window
    /// store. Default 12h matches the longest window (TwelveHour).
    #[arg(long, env = "CRABKA_METRICS_RETENTION_SECS", default_value_t = 43_200)]
    metrics_retention_secs: u64,
```

- [ ] **Step 2: Build the UsageStore and spawn the Scraper**

In `main`, after the broker_capacity_file loader block, before `AppState` construction, add:

```rust
    let usage_store = std::sync::Arc::new(
        crabka_rebalancer::scraper::UsageStore::new(
            crabka_rebalancer::scraper::WindowConfig {
                scrape_interval: std::time::Duration::from_secs(args.metrics_scrape_interval_secs),
                retention: std::time::Duration::from_secs(args.metrics_retention_secs),
            },
        ),
    );

    if !args.metrics_scrape_targets.is_empty() {
        match crabka_rebalancer::scraper::parse_targets(&args.metrics_scrape_targets) {
            Ok(targets) => {
                info!(
                    target_count = targets.len(),
                    scrape_interval_secs = args.metrics_scrape_interval_secs,
                    retention_secs = args.metrics_retention_secs,
                    "starting metrics scraper"
                );
                let scraper = crabka_rebalancer::scraper::Scraper::new(
                    targets,
                    std::time::Duration::from_secs(args.metrics_scrape_interval_secs),
                    usage_store.clone(),
                    shutdown.clone(),
                );
                tokio::spawn(scraper.run());
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to parse --metrics-scrape-targets `{}`: {e}",
                    args.metrics_scrape_targets
                ));
            }
        }
    }
```

- [ ] **Step 3: Update the GoalContext literal**

T9 set the field to `Arc::new(UsageStore::default())` as a placeholder. Swap to:

```rust
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
            min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
            broker_capacities: broker_capacities.clone(),
            broker_usages: usage_store.clone(),
        },
```

- [ ] **Step 4: Build + verify CLI**

```bash
cargo build -p crabka-rebalancer 2>&1 | tail -5
```

Expected: clean.

```bash
target/debug/crabka-rebalancer --help 2>&1 | grep "metrics-"
```

Expected: three new flags listed.

- [ ] **Step 5: Tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib 2>&1 | tail -5
```

Expected: all lib tests pass.

```bash
cargo clippy -p crabka-rebalancer --all-targets -- -D warnings 2>&1 | tail -3
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/bin/rebalancer.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): binary wires 3 new CLI flags + scraper spawn

--metrics-scrape-targets, --metrics-scrape-interval-secs,
--metrics-retention-secs. When targets are non-empty, the binary
constructs an Arc<UsageStore> with the configured WindowConfig,
spawns a Scraper::run() task, and threads the store into
AppState's GoalContext. When targets are empty, the store remains
empty (default) and usage-driven goals stay no-op — same
fail-safe pattern as the 43d capacity config.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 9 — Integration test + Helm (parallel: T15, T16)

### Task 15: Integration test — `disk_usage_evicts_hot_broker`

**Files:**
- Modify: `crates/rebalancer/tests/end_to_end.rs`

- [ ] **Step 1: Append the new test**

Append at the end:

```rust
/// Synthetic three-broker ClusterState with broker 1 holding 5× more
/// disk than broker 2. The UsageStore is pre-populated with disk_bytes
/// gauge samples. DiskUsage.propose must emit movements that reduce
/// broker 1's total.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disk_usage_evicts_hot_broker() {
    use crabka_rebalancer::goals::disk_usage::DiskUsage;
    use crabka_rebalancer::goals::{Goal, GoalContext};
    use crabka_rebalancer::model::{BrokerView, ClusterState, Movement, PartitionView};
    use crabka_rebalancer::scraper::parse::ParsedSample;
    use crabka_rebalancer::scraper::{MetricKind, UsageStore, WindowConfig};
    use std::sync::Arc;
    use std::time::Duration;

    let parts: Vec<_> = (0..5)
        .map(|i| PartitionView {
            topic: "t".into(),
            partition: i,
            replicas: vec![1, 2],
            leader: 1,
            isr: vec![1, 2],
        })
        .collect();

    let state = ClusterState {
        cluster_id: Some("c".into()),
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None },
            BrokerView { id: 2, host: "h2".into(), port: 9092, rack: None },
            BrokerView { id: 3, host: "h3".into(), port: 9092, rack: None },
        ],
        partitions: parts,
        in_flight_reassignments: vec![],
    };

    let store = UsageStore::new(WindowConfig {
        scrape_interval: Duration::from_secs(30),
        retention: Duration::from_secs(3600),
    });
    // Broker 1: 500 disk_bytes per partition × 5 partitions = 2500.
    for i in 0..5 {
        store.insert(
            1,
            vec![ParsedSample {
                metric: MetricKind::DiskBytes,
                topic: "t".into(),
                partition: i,
                value: 500.0,
            }],
            0,
        );
    }
    // Broker 2: 100 disk_bytes per partition × 5 partitions = 500.
    for i in 0..5 {
        store.insert(
            2,
            vec![ParsedSample {
                metric: MetricKind::DiskBytes,
                topic: "t".into(),
                partition: i,
                value: 100.0,
            }],
            0,
        );
    }

    let ctx = GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
        broker_capacities: Arc::new(crabka_rebalancer::capacity::BrokerCapacities::default()),
        broker_usages: Arc::new(store),
    };

    let mvs: Vec<Movement> = DiskUsage.propose(&state, &ctx);
    assert!(!mvs.is_empty(), "expected disk-eviction movements; got {mvs:?}");

    // Apply movements; broker 1's post-state total must shrink.
    let mut working = state.partitions.clone();
    for m in &mvs {
        if let Some(p) = working.iter_mut().find(|p| p.topic == m.topic && p.partition == m.partition) {
            p.replicas = m.new_replicas.clone();
        }
    }
    let broker_1_count = working.iter().map(|p| p.replicas.iter().filter(|x| **x == 1).count()).sum::<usize>();
    assert!(broker_1_count < 5, "broker 1 still hosts all replicas after eviction");
}
```

- [ ] **Step 2: Run tests**

```bash
cargo test -p crabka-rebalancer --test end_to_end disk_usage_evicts_hot_broker -- --nocapture 2>&1 | tail -10
```

Expected: PASS.

```bash
cargo test -p crabka-rebalancer --test end_to_end 2>&1 | tail -5
```

Expected: 8 tests pass (7 existing + 1 new).

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/tests/end_to_end.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43e): integration test for DiskUsage eviction

Synthetic three-broker ClusterState; broker 1 with 500 disk_bytes
per partition × 5 partitions vs broker 2 with 100 × 5. Asserts
DiskUsage emits movements that reduce broker 1's replica count.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 16: Helm chart — three new values + deployment env + helm-unittest assertion

**Files:**
- Modify: `charts/crabka-rebalancer/values.yaml`
- Modify: `charts/crabka-rebalancer/templates/deployment.yaml`
- Modify: `charts/crabka-rebalancer/tests/deployment_test.yaml`

- [ ] **Step 1: Update `values.yaml`**

After the existing `brokerCapacities`/`brokerCapacityFile` block, add:

```yaml
# Per-broker /metrics scrape targets. Format:
# "id:host:port,id:host:port,…". Empty = scraper disabled (usage-driven
# goals are no-ops).
metricsScrapeTargets: ""
metricsScrapeIntervalSecs: 30
# Retention for the rolling-window store. Default 12h matches the
# longest window the goals query.
metricsRetentionSecs: 43200
```

- [ ] **Step 2: Update `templates/deployment.yaml`**

After the existing capacity env block, add three conditional env entries:

```yaml
            {{- if .Values.metricsScrapeTargets }}
            - name: CRABKA_METRICS_SCRAPE_TARGETS
              value: {{ .Values.metricsScrapeTargets | quote }}
            - name: CRABKA_METRICS_SCRAPE_INTERVAL_SECS
              value: {{ .Values.metricsScrapeIntervalSecs | quote }}
            - name: CRABKA_METRICS_RETENTION_SECS
              value: {{ .Values.metricsRetentionSecs | quote }}
            {{- end }}
```

Match the surrounding indentation (12 spaces for `- name:`).

- [ ] **Step 3: Update `tests/deployment_test.yaml`**

Append a new test:

```yaml
  - it: passes metrics scrape env vars when metricsScrapeTargets is set
    set:
      metricsScrapeTargets: "1:broker1:9100,2:broker2:9100"
      metricsScrapeIntervalSecs: 15
      metricsRetentionSecs: 3600
    asserts:
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_METRICS_SCRAPE_TARGETS
            value: "1:broker1:9100,2:broker2:9100"
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_METRICS_SCRAPE_INTERVAL_SECS
            value: "15"
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_METRICS_RETENTION_SECS
            value: "3600"
```

- [ ] **Step 4: Verify (if helm available)**

```bash
helm lint /home/matt/git/crabka/charts/crabka-rebalancer --set bootstrapServers=test:9092 2>&1 | tail -3
helm unittest /home/matt/git/crabka/charts/crabka-rebalancer 2>&1 | tail -10
```

Expected: lint clean; all suites pass. If `helm` unavailable, skip — CI runs them.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add charts/crabka-rebalancer/values.yaml charts/crabka-rebalancer/templates/deployment.yaml charts/crabka-rebalancer/tests/deployment_test.yaml
git -C /home/matt/git/crabka commit -m "rebalancer(43e): Helm chart wires metrics scrape targets

values.yaml gains metricsScrapeTargets (empty default = scraper
disabled), metricsScrapeIntervalSecs (30), metricsRetentionSecs
(43200 = 12h). deployment.yaml conditionally emits the three
CRABKA_METRICS_* env vars when metricsScrapeTargets is non-empty.
New helm-unittest test in deployment_test.yaml asserts all three
env entries render.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 10 — Docs

### Task 17: STATUS docs

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Append slice-43e entry**

Append at the very end:

```markdown
## Slice 43e — Rebalancer usage scraper + soft usage goals (2026-05-17)

- **Broker-side (the `43e-core` half):**
  - New `PartitionLabel { topic, partition }` drives three new
    metric families on `BrokerMetrics`:
    `crabka_broker_partition_bytes_in_total{topic,partition}`,
    `crabka_broker_partition_bytes_out_total{topic,partition}`,
    and `crabka_broker_partition_disk_bytes{topic,partition}`.
    The slice-39 topic-level counters stay.
  - `handlers/produce.rs` + `handlers/fetch.rs` emit one per-partition
    `record_partition_*` call per (topic, partition) in addition to
    the existing topic-level inc.
  - New `disk_scanner` module periodically (default 60s) walks each
    partition's log directory and updates
    `partition_disk_bytes`. CLI flag
    `--partition-disk-scan-interval-secs` (env
    `CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS`; `0` disables).
- **Rebalancer-side:**
  - New top-level `scraper/` module: `parse` (scoped
    OpenMetrics text parser), `targets` (CLI value parser),
    `window` (per-series ring buffer + counter-reset-aware rate
    computation), and `mod.rs` (HTTP tick loop).
  - Four new soft goals shipped: `DiskUsage`, `LeaderBytesIn`,
    `NetworkInUsage`, `NetworkOutUsage`. Each consumes
    `ctx.broker_usages`; empty store → no-op (same fail-safe
    pattern as the 43d capacity stubs).
  - Three 43d capacity stubs become real:
    `DiskCapacity`, `NetworkInCapacity`, `NetworkOutCapacity`.
    Each adds an `is_satisfied_with_ctx` override that consults
    `ctx.broker_usages`.
  - `Goal` trait gains
    `is_satisfied_with_ctx(&ClusterState, &GoalContext) -> bool`
    with a default impl that forwards to `is_satisfied`. The
    optimizer's incremental hard-goal validation (slice 43c)
    switches to call this so capacity goals enforce their
    invariants against soft-goal interference.
    **Closes the 43d known trade** on `ReplicaCapacity` — which
    also adds its own `is_satisfied_with_ctx`.
  - `CpuCapacity` remains a stub (slice 43f).
  - `GoalRegistry::default_registry` grows from 11 to **15
    goals** in priority order. Renamed
    `default_registry_has_eleven_goals` →
    `default_registry_has_fifteen_goals`; updated
    `default_registry_order_matches_spec` accordingly.
- New CLI flags:
  `--metrics-scrape-targets` (env
  `CRABKA_METRICS_SCRAPE_TARGETS`, format
  `id:host:port,id:host:port,…`, empty default = scraper
  disabled),
  `--metrics-scrape-interval-secs` (default 30),
  `--metrics-retention-secs` (default 43200 = 12h).
- Helm chart picks up the three new env vars conditionally on
  `metricsScrapeTargets` being set. New helm-unittest assertion
  in `deployment_test.yaml`.
- ~40 new unit tests (parse, targets, window, four soft usage
  goals, three capacity real bodies + ReplicaCapacity
  is_satisfied_with_ctx, optimizer regression, broker
  PartitionLabel, broker disk scanner) + 1 broker integration
  test + 1 rebalancer integration test
  (`disk_usage_evicts_hot_broker`) + 1 helm-unittest assertion.
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43e-design.md`].
- Out of scope (deferred): `CpuUsage` soft goal + real
  `CpuCapacity` body (slice 43f); discovery of scrape targets
  via `Metadata` (currently operator-supplied);
  per-topic resource hints in capacity config (usage metrics
  provide the real input now); anomaly detection (slice 43g);
  operator `KafkaRebalance` CRD (slice 44).

### Known risks
- Memory footprint of the per-series ring buffer scales as
  brokers × partitions × 3 metrics × (retention / scrape_interval).
  At the default 30s scrape / 12h retention, a 5-broker × 1000-
  partition cluster is roughly 350MB. Tune via
  `--metrics-scrape-interval-secs` or `--metrics-retention-secs`.
- Counter resets (broker restart) are detected by
  `latest.value < earliest.value` returning `None`. The affected
  goal sees no rate signal until two post-reset samples
  accumulate.
```

- [ ] **Step 2: Final verification**

```bash
cargo fmt --check 2>&1 | tail -3
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
cargo test -p crabka-broker 2>&1 | tail -5
cargo test -p crabka-rebalancer 2>&1 | tail -10
```

All four must pass clean. If `cargo fmt --check` reports differences, run `cargo fmt` and commit the formatting changes separately before the docs commit.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add STATUS.md
git -C /home/matt/git/crabka commit -m "rebalancer(43e): STATUS

Slice 43e entry covering the broker-side per-partition metrics,
the rebalancer scraper module, the four new soft usage goals,
the three now-functional capacity goals, the
Goal::is_satisfied_with_ctx trait addition that closes 43d's
known trade, and the deferred items (CpuUsage, target discovery,
per-topic hints, anomaly detection, operator CRD).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-review checklist (across both parts)

**1. Spec coverage:** Every section of the spec maps to a task in part 1 (T1–T10) or part 2 (T11–T17). Goal lineup, broker emit changes, disk scanner, scraper module, window storage, four soft usage goals, three capacity real bodies, `is_satisfied_with_ctx`, optimizer switch, registry growth, binary wiring, integration test, Helm chart, STATUS — all enumerated.

**2. Placeholder scan:** None of the tasks defer to "implement later". `network_out_capacity.rs` in T11 references mechanical substitution from `network_in_capacity.rs` — every substitution pair is enumerated explicitly (`bytes_in_rate` → `bytes_out_rate`, `network_in_bytes_per_sec` → `network_out_bytes_per_sec`, `BytesIn` → `BytesOut`, `NetworkInCapacity` → `NetworkOutCapacity`). Subagent has enough to write the full file.

**3. Type consistency:** `BrokerCapacities`, `BrokerCapacity`, `UsageStore`, `WindowConfig`, `Window`, `MetricKind`, `ParsedSample`, `ScrapeTarget`, `Goal`, `GoalContext`, `Arc<UsageStore>` all referenced identically across both parts.
