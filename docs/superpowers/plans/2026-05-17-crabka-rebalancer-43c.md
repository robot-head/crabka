# Slice 43c — Rebalancer topology goals — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Add three new goals to the rebalancer — `RackAware` (hard), `TopicReplicaDistribution` (soft), `MinTopicLeadersPerBroker` (soft) — under one new `GoalContext` field and one new CLI flag. Goal-only slice: no proto, persistence, or executor changes.

**Architecture:** Three sibling modules under `crates/rebalancer/src/goals/`. `RackAware` enforces no-two-replicas-of-a-partition-share-a-rack (strict mode; self-limits when RF > rack count via warn log). `TopicReplicaDistribution` is a per-topic analog of the existing cluster-wide `ReplicaDistribution`. `MinTopicLeadersPerBroker` ensures every (broker, topic) pair has at least N leaders; default `0` = no-op. All three plug into the existing `Goal` trait and the `GoalRegistry::default_registry()` list. One new CLI flag `--min-topic-leaders-per-broker` (default 0) sets the threshold.

**Tech Stack:** Rust 1.95.0. No new workspace deps. Reuses `tracing` (for the RackAware warn), `clap` (for the new flag), existing `model` + `goals` modules.

**Reference spec:** [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43c-design.md`](../specs/2026-05-17-crabka-rebalancer-43c-design.md).

**Working directory:** `/home/matt/git/crabka`. Branch `feature/rebalancer-43c` exists with the spec committed.

---

## File structure

```
crates/rebalancer/
├── src/
│   ├── goals/
│   │   ├── mod.rs                                  # MODIFIED — GoalContext gains min_topic_leaders_per_broker; three new pub mod lines
│   │   ├── rack_aware.rs                           # NEW
│   │   ├── topic_replica_distribution.rs           # NEW
│   │   └── min_topic_leaders_per_broker.rs         # NEW
│   ├── api/mod.rs                                  # MODIFIED — GoalRegistry::default_registry adds three new goals
│   └── bin/rebalancer.rs                           # MODIFIED — new CLI flag + GoalContext literal extended
├── src/api/handlers.rs                             # MODIFIED — AppState test fixture GoalContext literal
├── src/optimizer/mod.rs                            # MODIFIED — 3 test fixture GoalContext literals
└── tests/end_to_end.rs                             # MODIFIED — 1 new integration test + 1 fixture GoalContext literal
charts/crabka-rebalancer/
├── values.yaml                                      # MODIFIED — minTopicLeadersPerBroker: 0
├── templates/deployment.yaml                        # MODIFIED — new CRABKA_MIN_TOPIC_LEADERS_PER_BROKER env var
└── tests/deployment_test.yaml                       # MODIFIED — assert env var present
STATUS.md                                            # MODIFIED — slice 43c entry
```

**7 tasks across 5 batches.**

- **Batch 1 (alone):** T1 — `GoalContext.min_topic_leaders_per_broker` + three `pub mod` mounts + update all existing `GoalContext { ... }` literal call sites.
- **Batch 2 (parallel):** T2 RackAware, T3 TopicReplicaDistribution, T4 MinTopicLeadersPerBroker (different files; no shared file edits since T1 pre-added the mounts).
- **Batch 3 (alone):** T5 — `GoalRegistry::default_registry` + binary CLI flag + Helm chart updates.
- **Batch 4 (alone):** T6 — integration test in `end_to_end.rs`.
- **Batch 5 (alone):** T7 — STATUS docs.

---

## Batch 1 — GoalContext extension + module mounts

### Task 1: Extend `GoalContext` with `min_topic_leaders_per_broker`; mount the three new module declarations; update all call sites

**Files:**
- Modify: `crates/rebalancer/src/goals/mod.rs`
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`
- Modify: `crates/rebalancer/src/api/handlers.rs`
- Modify: `crates/rebalancer/src/optimizer/mod.rs`
- Modify: `crates/rebalancer/tests/end_to_end.rs`
- Modify: `crates/rebalancer/src/goals/preferred_leader_idempotency.rs`
- Modify: `crates/rebalancer/src/goals/replica_distribution.rs`
- Modify: `crates/rebalancer/src/goals/leader_distribution.rs`

- [ ] **Step 1: Edit `crates/rebalancer/src/goals/mod.rs`**

Replace the existing `mod.rs` head + struct definition. The full top of the file becomes:

```rust
//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules.

use crate::model::{ClusterState, Movement};

pub mod leader_distribution;
pub mod min_topic_leaders_per_broker;
pub mod preferred_leader_idempotency;
pub mod rack_aware;
pub mod replica_distribution;
pub mod topic_replica_distribution;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalPriority {
    /// Hard goals must be satisfied. If the optimizer truncates the
    /// movement list at `max_movements_per_proposal` and a hard goal
    /// still has unfulfilled movements, the optimizer returns
    /// `OptimizeError::HardGoalUnsatisfied`.
    Hard,
    /// Soft goals improve placement on a best-effort basis. Movements
    /// that don't fit under the cap are simply skipped.
    Soft,
}

#[derive(Debug, Clone, Copy)]
pub struct GoalContext {
    /// `(max - min) * 100 / total` must exceed this percentage for a
    /// soft goal to act. Hard goals ignore the threshold.
    pub imbalance_threshold_pct: u32,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` (default) disables the goal.
    pub min_topic_leaders_per_broker: u32,
}
```

The rest of the file (`Goal` trait + `#[cfg(test)] pub mod tests`) stays unchanged.

The crate will not compile after this step because of the missing module files + missing field on every `GoalContext` literal. The next steps fix the literals; T2/T3/T4 add the goal files.

- [ ] **Step 2: Update `crates/rebalancer/src/bin/rebalancer.rs`**

Find the `GoalContext { ... }` literal (around line 211). Add the new field:

```rust
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
            min_topic_leaders_per_broker: 0,
        },
```

(The CLI flag itself lands in T5; for now hard-code 0 so the build works.)

- [ ] **Step 3: Update `crates/rebalancer/src/api/handlers.rs`**

The test fixture's `GoalContext` literal (around line 448) gains the field:

```rust
            goal_ctx: GoalContext {
                imbalance_threshold_pct: 10,
                max_movements_per_proposal: 256,
                min_topic_leaders_per_broker: 0,
            },
```

- [ ] **Step 4: Update `crates/rebalancer/src/optimizer/mod.rs`**

Three test fixture sites (around lines 231-234, 444-447, 500-503). Add the new field to each, defaulting to `0`:

```rust
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
        }
```

(Or whatever the surrounding values are at each call site — preserve existing fields; add the new one.)

- [ ] **Step 5: Update `crates/rebalancer/tests/end_to_end.rs`**

The `build_state` fixture (around line 138) gains the field:

```rust
        goal_ctx: GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
        },
```

- [ ] **Step 6: Update the three existing goal-test fixtures**

`preferred_leader_idempotency.rs`, `replica_distribution.rs`, `leader_distribution.rs` each have a `fn ctx() -> GoalContext { GoalContext { ... } }` helper in their `#[cfg(test)] mod tests` block. Add `min_topic_leaders_per_broker: 0` to each.

For example, in `preferred_leader_idempotency.rs:73`:

```rust
    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
        }
    }
```

Same pattern in `replica_distribution.rs:148` and `leader_distribution.rs:101`.

- [ ] **Step 7: Verify call-site coverage**

Run: `grep -rn "GoalContext {" /home/matt/git/crabka/crates/rebalancer/ --include="*.rs" | grep -v "min_topic_leaders_per_broker" | grep -v "pub struct GoalContext"`

Expected: no output. Every `GoalContext { ... }` literal in the crate now includes the new field.

If the grep shows any remaining sites, add the field there too.

The crate still won't compile because the three new goal modules don't exist yet. T2/T3/T4 create them. **For T1's verification gate**, skip `cargo test` — instead run:

```bash
cargo check -p crabka-rebalancer --lib 2>&1 | grep -E "unresolved.*goals::(rack_aware|topic_replica_distribution|min_topic_leaders_per_broker)" | head -5
```

Expected: errors that reference the three missing modules. Any **other** kind of error (missing field, type mismatch, etc.) is a T1 bug.

- [ ] **Step 8: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/mod.rs crates/rebalancer/src/bin/rebalancer.rs crates/rebalancer/src/api/handlers.rs crates/rebalancer/src/optimizer/mod.rs crates/rebalancer/tests/end_to_end.rs crates/rebalancer/src/goals/preferred_leader_idempotency.rs crates/rebalancer/src/goals/replica_distribution.rs crates/rebalancer/src/goals/leader_distribution.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43c): GoalContext.min_topic_leaders_per_broker + three new module mounts

Adds the new GoalContext field defaulting to 0 (= goal disabled at
default config). Declares pub mod rack_aware, topic_replica_distribution,
min_topic_leaders_per_broker — the three goal files land in
follow-on tasks T2/T3/T4. Every existing GoalContext { ... } literal
site updated to include the new field set to 0.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Three new goal files (parallel: T2, T3, T4)

### Task 2: `RackAware` goal (hard)

**Files:**
- Create: `crates/rebalancer/src/goals/rack_aware.rs`

- [ ] **Step 1: Write the goal + tests**

Create `crates/rebalancer/src/goals/rack_aware.rs`:

```rust
//! Hard goal: ensure no two replicas of the same partition share a
//! rack. Brokers with `rack: None` each count as their own
//! pseudo-rack (matches Kafka KIP-36 broker-side rack-aware
//! assignment behavior).
//!
//! Strict mode: if RF exceeds the distinct rack count for the cluster,
//! the affected partition is logged at warn level and skipped — the
//! goal never produces `HardGoalUnsatisfied`. Operators with
//! RF > rack-count get a no-op rather than a failed proposal.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use tracing::warn;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct RackAware;

impl RackAware {
    pub const NAME: &'static str = "RackAware";
}

impl Goal for RackAware {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        // Per-broker rack tag, treating None as a per-broker unique pseudo-rack.
        // Encode unique racks as a synthetic `__no_rack_<broker_id>` string so
        // collision detection is straightforward.
        let rack_of: HashMap<i32, String> = state
            .brokers
            .iter()
            .map(|b| {
                let tag = b
                    .rack
                    .clone()
                    .unwrap_or_else(|| format!("__no_rack_{}", b.id));
                (b.id, tag)
            })
            .collect();

        let distinct_rack_count: usize = state
            .brokers
            .iter()
            .map(|b| rack_of.get(&b.id).cloned().unwrap_or_default())
            .collect::<BTreeSet<_>>()
            .len();

        // Build a working copy so multi-pass within one propose call sees
        // post-swap state.
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        // Snapshot original old_replicas / old_leader to avoid drift
        // when the same partition is touched twice (same fix as
        // ReplicaDistribution's 43a fixup).
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
            // Find the first colliding partition + the pair to fix.
            let mut chosen: Option<(usize, i32, i32)> = None; // (idx, donor broker, target broker)
            for (idx, p) in working.iter().enumerate() {
                // Skip if RF > rack count (infeasible). Log once per partition.
                if p.replicas.len() > distinct_rack_count {
                    // Only log once: check the original state, not the working
                    // copy, so we don't double-log after a swap.
                    if state
                        .partitions
                        .iter()
                        .find(|orig| orig.topic == p.topic && orig.partition == p.partition)
                        .map_or(true, |orig| orig.replicas.len() > distinct_rack_count)
                    {
                        warn!(
                            topic = %p.topic,
                            partition = p.partition,
                            rf = p.replicas.len(),
                            rack_count = distinct_rack_count,
                            "RackAware: cluster has fewer racks than RF; goal self-limits"
                        );
                    }
                    continue;
                }

                let mut by_rack: BTreeMap<String, Vec<i32>> = BTreeMap::new();
                for r in &p.replicas {
                    if let Some(rack) = rack_of.get(r) {
                        by_rack.entry(rack.clone()).or_default().push(*r);
                    }
                }
                // First rack with > 1 replica is a collision.
                let collision = by_rack.iter().find(|(_, brokers)| brokers.len() >= 2);
                let Some((_collision_rack, brokers_in_collision)) = collision else {
                    continue;
                };
                // Donor: the higher broker id from the colliding pair (deterministic).
                let donor = *brokers_in_collision.iter().max().expect("non-empty");

                // Target: lowest-id broker in a rack not currently represented in
                // this partition's replica set.
                let used_racks: BTreeSet<String> = by_rack.keys().cloned().collect();
                let mut candidate_brokers: Vec<i32> = state
                    .brokers
                    .iter()
                    .filter(|b| {
                        let r = rack_of.get(&b.id).expect("rack_of populated");
                        !used_racks.contains(r) && !p.replicas.contains(&b.id)
                    })
                    .map(|b| b.id)
                    .collect();
                candidate_brokers.sort_unstable();
                let Some(target) = candidate_brokers.first().copied() else {
                    continue;
                };

                chosen = Some((idx, donor, target));
                break;
            }

            let Some((idx, donor, target)) = chosen else {
                break; // No more collisions resolvable.
            };

            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let pos = p.replicas.iter().position(|r| *r == donor).expect("donor present");
            p.replicas[pos] = target;
            // If the donor was the leader, pick a new leader from the new replica
            // set, preferring something in ISR.
            let new_leader = if p.leader == donor {
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

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: 0,
        }
    }

    fn broker(id: i32, rack: Option<&str>) -> BrokerView {
        BrokerView {
            id,
            host: format!("h{id}"),
            port: 9092,
            rack: rack.map(str::to_string),
        }
    }

    fn state_with(parts: Vec<PartitionView>, brokers: Vec<BrokerView>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers,
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

    #[test]
    fn balanced_three_racks_no_op() {
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("b")),
            broker(3, Some("c")),
        ];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);
        assert!(RackAware.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn single_collision_resolved() {
        // partition t/0 has replicas [1, 2] both in rack `a`; broker 3 in rack b.
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
        ];
        let parts = vec![part("t", 0, vec![1, 2], 1)];
        let s = state_with(parts, brokers);
        let mvs = RackAware.propose(&s, &ctx());
        assert_eq!(mvs.len(), 1, "exactly one movement for one collision");
        let m = &mvs[0];
        assert_eq!(m.old_replicas, vec![1, 2]);
        // The donor is broker 2 (higher id in the colliding rack); target is
        // broker 3 (lowest id in the unrepresented rack).
        assert_eq!(m.new_replicas, vec![1, 3]);
    }

    #[test]
    fn multi_collision_iterates_within_propose() {
        // Two partitions, each with a same-rack collision.
        let brokers = vec![
            broker(1, Some("a")),
            broker(2, Some("a")),
            broker(3, Some("b")),
            broker(4, Some("c")),
        ];
        let parts = vec![
            part("t", 0, vec![1, 2], 1),
            part("t", 1, vec![1, 2], 1),
        ];
        let s = state_with(parts, brokers);
        let mvs = RackAware.propose(&s, &ctx());
        assert_eq!(mvs.len(), 2, "one movement per partition");
        // Each movement must move the higher-id colliding broker (2) to a
        // not-yet-represented rack.
        for m in &mvs {
            assert_eq!(m.old_replicas, vec![1, 2]);
            assert!(!m.new_replicas.contains(&2), "broker 2 must move out");
        }
    }

    #[test]
    fn rf_equals_rack_count_satisfiable() {
        let brokers = vec![broker(1, Some("a")), broker(2, Some("b"))];
        // RF=2, two racks, no collision -> no-op.
        let parts = vec![part("t", 0, vec![1, 2], 1)];
        let s = state_with(parts, brokers);
        assert!(RackAware.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn rf_greater_than_rack_count_logs_warn_and_skips() {
        // RF=3, two racks — pigeonhole guarantees a collision; goal must skip.
        let brokers = vec![broker(1, Some("a")), broker(2, Some("a")), broker(3, Some("b"))];
        let parts = vec![part("t", 0, vec![1, 2, 3], 1)];
        let s = state_with(parts, brokers);
        let mvs = RackAware.propose(&s, &ctx());
        // No valid movement — `b` is already represented, no other rack exists.
        // Goal logs warn (we don't assert the log here) and returns empty.
        assert!(
            mvs.is_empty(),
            "RF > rack count must self-limit, got {mvs:?}"
        );
    }
}
```

- [ ] **Step 2: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::rack_aware -- --nocapture
```

Expected: 5 tests pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "rack_aware"
```

Expected: no output (no clippy warnings on this file).

If the workspace `clippy::pedantic` fires:
- `cast_*_truncation` lints → use `try_from` (per 43a pattern).
- `doc_markdown` on CamelCase identifiers in docstrings → backtick them.
- `option_map_or` lints → use `.map_or(...)` instead of `.map(...).unwrap_or(...)`.

Note: other parts of the lib may not compile yet (T3/T4 unfinished). The targeted `--lib goals::rack_aware` test still builds the lib but only runs your module's tests. If the build fails because of `goals::topic_replica_distribution` / `goals::min_topic_leaders_per_broker` not existing, run instead:

```bash
cargo check -p crabka-rebalancer --lib 2>&1 | grep "rack_aware" | head -10
```

…to confirm `rack_aware.rs` itself compiles cleanly. The tests will run when T3 + T4 also land.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/rack_aware.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43c): RackAware goal (hard)

Per-partition rack diversity: no two replicas in the same rack.
Brokers with rack: None each treated as their own pseudo-rack
(matches Kafka KIP-36). Strict mode: if RF > distinct rack count
for any partition, the goal logs warn and emits no movements for
that partition — never produces HardGoalUnsatisfied. Five unit
tests cover balanced three-rack / single-collision / multi-collision
/ RF==rack-count / RF>rack-count infeasibility paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 3: `TopicReplicaDistribution` goal (soft)

**Files:**
- Create: `crates/rebalancer/src/goals/topic_replica_distribution.rs`

- [ ] **Step 1: Write the goal + tests**

Create `crates/rebalancer/src/goals/topic_replica_distribution.rs`:

```rust
//! Soft goal: per-topic, balance replica counts across brokers.
//!
//! Distinct from `ReplicaDistribution`, which balances cluster-wide
//! replica counts (sum across all topics). A cluster can be evenly
//! balanced overall while one topic is concentrated on a single
//! broker — that case is invisible to `ReplicaDistribution` but
//! fixed by this goal.

use std::collections::{HashMap, HashSet};

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct TopicReplicaDistribution;

impl TopicReplicaDistribution {
    pub const NAME: &'static str = "TopicReplicaDistribution";

    /// Replicas of `topic` per broker id.
    fn counts_for_topic(
        partitions: &[PartitionView],
        broker_ids: &[i32],
        topic: &str,
    ) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = broker_ids.iter().map(|b| (*b, 0)).collect();
        for p in partitions.iter().filter(|p| p.topic == topic) {
            for r in &p.replicas {
                *m.entry(*r).or_insert(0) += 1;
            }
        }
        m
    }

    fn imbalance_pct(counts: &HashMap<i32, usize>) -> u32 {
        let values: Vec<usize> = counts.values().copied().collect();
        let total: usize = values.iter().sum();
        if total == 0 {
            return 0;
        }
        let max = *values.iter().max().unwrap_or(&0);
        let min = *values.iter().min().unwrap_or(&0);
        u32::try_from((max - min) * 100 / total).unwrap_or(u32::MAX)
    }
}

impl Goal for TopicReplicaDistribution {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();
        let topics: HashSet<String> = state.partitions.iter().map(|p| p.topic.clone()).collect();

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

        // Iterate per-topic; within each topic, greedy swap hot→cold.
        for topic in &topics {
            loop {
                let counts = Self::counts_for_topic(&working, &broker_ids, topic);
                if Self::imbalance_pct(&counts) <= ctx.imbalance_threshold_pct {
                    break;
                }
                let mut by_load: Vec<(i32, usize)> = counts.into_iter().collect();
                by_load.sort_by_key(|b| std::cmp::Reverse(b.1));
                let (hot, _) = *by_load.first().expect("at least one broker");
                let (cold, _) = *by_load.last().expect("at least one broker");
                if hot == cold {
                    break;
                }

                // Find a partition on `hot` (of this topic) whose replica set
                // doesn't already include `cold` and where moving hot→cold
                // doesn't break RF.
                let idx = working.iter().position(|p| {
                    p.topic == *topic
                        && p.replicas.contains(&hot)
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
                    return out;
                }
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;

    fn ctx_with(threshold: u32, cap: usize) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: threshold,
            max_movements_per_proposal: cap,
            min_topic_leaders_per_broker: 0,
        }
    }

    fn ctx() -> GoalContext {
        ctx_with(10, 256)
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

    #[test]
    fn balanced_topic_no_op() {
        // Each of three brokers holds 4 replicas of topic t.
        let mut parts = Vec::new();
        for i in 0..4 {
            parts.push(part("t", i, vec![1], 1));
            parts.push(part("t", i + 100, vec![2], 2));
            parts.push(part("t", i + 200, vec![3], 3));
        }
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(TopicReplicaDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn hot_broker_triggers_swaps() {
        // All 9 partitions of topic t live on broker 1.
        let parts: Vec<_> = (0..9).map(|i| part("t", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = TopicReplicaDistribution.propose(&s, &ctx());
        assert!(!mvs.is_empty(), "expected swaps for hot-broker concentration");
        // Each movement keeps RF unchanged.
        for m in &mvs {
            assert_eq!(m.old_replicas.len(), m.new_replicas.len());
        }
    }

    #[test]
    fn multi_topic_independence() {
        // Topic a is balanced (each broker holds one replica). Topic b is
        // hot on broker 1.
        let mut parts = vec![
            part("a", 0, vec![1], 1),
            part("a", 1, vec![2], 2),
            part("a", 2, vec![3], 3),
        ];
        for i in 0..6 {
            parts.push(part("b", i, vec![1], 1));
        }
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = TopicReplicaDistribution.propose(&s, &ctx());
        // All movements should be on topic b — topic a is already balanced.
        for m in &mvs {
            assert_eq!(m.topic, "b", "movement on wrong topic: {m:?}");
        }
        assert!(!mvs.is_empty(), "expected swaps on topic b");
    }

    #[test]
    fn respects_max_movements_cap() {
        let parts: Vec<_> = (0..20).map(|i| part("t", i, vec![1], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = TopicReplicaDistribution.propose(&s, &ctx_with(10, 2));
        assert!(mvs.len() <= 2, "expected at most 2 movements per cap, got {}", mvs.len());
    }
}
```

- [ ] **Step 2: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::topic_replica_distribution -- --nocapture
```

Expected: 4 tests pass. (May fail to build until T2 + T4 also land — see T2's Step 2 notes.)

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "topic_replica_distribution"
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/topic_replica_distribution.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43c): TopicReplicaDistribution goal (soft)

Per-topic replica balance: iterates topics, runs hot→cold greedy
swaps within each, stops when within ctx.imbalance_threshold_pct.
Distinct from ReplicaDistribution (cluster-wide). Four unit tests
cover balanced no-op, hot-broker swaps, multi-topic independence,
and max-movements cap.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 4: `MinTopicLeadersPerBroker` goal (soft)

**Files:**
- Create: `crates/rebalancer/src/goals/min_topic_leaders_per_broker.rs`

- [ ] **Step 1: Write the goal + tests**

Create `crates/rebalancer/src/goals/min_topic_leaders_per_broker.rs`:

```rust
//! Soft goal: ensure every (broker, topic) pair where the broker
//! holds at least one replica also leads at least
//! `ctx.min_topic_leaders_per_broker` of that topic's partitions.
//!
//! Default `ctx.min_topic_leaders_per_broker == 0` makes the goal a
//! no-op. Operators opt in via the `--min-topic-leaders-per-broker`
//! CLI flag.
//!
//! Emits leader-only movements (replicas unchanged); the broker
//! receiving the leadership must already be in the partition's
//! replica set AND in ISR.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct MinTopicLeadersPerBroker;

impl MinTopicLeadersPerBroker {
    pub const NAME: &'static str = "MinTopicLeadersPerBroker";
}

impl Goal for MinTopicLeadersPerBroker {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }

    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let min = ctx.min_topic_leaders_per_broker as usize;
        if min == 0 {
            return Vec::new();
        }

        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        let topics: BTreeSet<String> = state.partitions.iter().map(|p| p.topic.clone()).collect();
        let broker_ids: Vec<i32> = state.brokers.iter().map(|b| b.id).collect();

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
            // For each (broker, topic), compute leader count + presence in
            // any replica set. A broker is "covered" by a topic if it
            // appears in at least one of the topic's partitions' replica
            // set. We only require min leaders for covered brokers.
            let mut under: Option<(i32, String, usize)> = None; // (broker, topic, current_leaders)
            'find_under: for topic in &topics {
                let topic_parts: Vec<&PartitionView> =
                    working.iter().filter(|p| p.topic == *topic).collect();
                // Brokers covered by this topic.
                let covered: HashSet<i32> = topic_parts
                    .iter()
                    .flat_map(|p| p.replicas.iter().copied())
                    .collect();
                for broker in &broker_ids {
                    if !covered.contains(broker) {
                        continue;
                    }
                    let leader_count = topic_parts.iter().filter(|p| p.leader == *broker).count();
                    if leader_count < min {
                        under = Some((*broker, topic.clone(), leader_count));
                        break 'find_under;
                    }
                }
            }
            let Some((under_broker, under_topic, _)) = under else {
                break; // all (broker, topic) pairs meet the minimum
            };

            // Find a candidate partition: one of this topic's partitions
            // where the current leader broker has a surplus AND the
            // under-served broker is in the replica set AND in ISR.
            let surplus_broker_count: HashMap<i32, usize> = {
                let topic_parts: Vec<&PartitionView> =
                    working.iter().filter(|p| p.topic == under_topic).collect();
                let mut m: HashMap<i32, usize> = HashMap::new();
                for p in &topic_parts {
                    *m.entry(p.leader).or_insert(0) += 1;
                }
                m
            };

            let idx = working.iter().position(|p| {
                p.topic == under_topic
                    && p.replicas.contains(&under_broker)
                    && p.isr.contains(&under_broker)
                    && p.leader != under_broker
                    && surplus_broker_count.get(&p.leader).copied().unwrap_or(0) > min
            });
            let Some(idx) = idx else {
                // Some kind of structural obstacle (under-broker not in ISR
                // anywhere, or no surplus partition). Skip this pair; try
                // the next one. To avoid an infinite loop on the same
                // (broker, topic), mark it satisfied by breaking — there's
                // no progress to be made.
                break;
            };

            let p = &mut working[idx];
            let key = (p.topic.clone(), p.partition);
            let old_leader = original_leader.get(&key).copied().unwrap_or(p.leader);
            let old_replicas = original_replicas
                .get(&key)
                .cloned()
                .unwrap_or_else(|| p.replicas.clone());

            p.leader = under_broker;
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas: old_replicas.clone(),
                new_replicas: old_replicas, // leader-only — replicas unchanged
                old_leader,
                new_leader: under_broker,
            });

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

    fn ctx_with(min: u32) -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
            min_topic_leaders_per_broker: min,
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

    fn part_with_isr(
        topic: &str,
        partition: i32,
        replicas: Vec<i32>,
        leader: i32,
        isr: Vec<i32>,
    ) -> PartitionView {
        PartitionView {
            topic: topic.into(),
            partition,
            replicas,
            leader,
            isr,
        }
    }

    fn part(topic: &str, partition: i32, replicas: Vec<i32>, leader: i32) -> PartitionView {
        let isr = replicas.clone();
        part_with_isr(topic, partition, replicas, leader, isr)
    }

    #[test]
    fn min_zero_is_no_op() {
        // Even on a hot-leader cluster, min=0 emits nothing.
        let parts: Vec<_> = (0..4).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(MinTopicLeadersPerBroker.propose(&s, &ctx_with(0)).is_empty());
    }

    #[test]
    fn min_one_ensures_coverage() {
        // All 3 partitions of t lead on broker 1; brokers 2 + 3 are in every
        // replica set + ISR. With min=1, the goal must give each of broker 2
        // and broker 3 at least one leader.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2, 3], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = MinTopicLeadersPerBroker.propose(&s, &ctx_with(1));
        // At least 2 leader-flips (one for broker 2, one for broker 3).
        assert!(mvs.len() >= 2, "expected ≥2 leader flips, got {}", mvs.len());
        for m in &mvs {
            assert_eq!(m.old_replicas, m.new_replicas, "leader-only move");
        }
    }

    #[test]
    fn broker_not_in_replica_set_skipped() {
        // Broker 3 isn't in any of t's replica sets. The goal must not try
        // to give it leadership.
        let parts: Vec<_> = (0..3).map(|i| part("t", i, vec![1, 2], 1)).collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = MinTopicLeadersPerBroker.propose(&s, &ctx_with(1));
        for m in &mvs {
            assert_ne!(m.new_leader, 3, "broker 3 must not get leadership of t");
        }
    }

    #[test]
    fn under_served_not_in_isr_skipped() {
        // Broker 2 is in replica set but not in ISR for any partition.
        // The goal cannot make broker 2 leader (would violate Kafka ISR
        // invariant). Goal must not emit any movement targeting broker 2.
        let parts: Vec<_> = (0..3)
            .map(|i| part_with_isr("t", i, vec![1, 2], 1, vec![1]))
            .collect();
        let s = state_with(parts, vec![1, 2]);
        let mvs = MinTopicLeadersPerBroker.propose(&s, &ctx_with(1));
        for m in &mvs {
            assert_ne!(
                m.new_leader, 2,
                "broker 2 not in ISR; must not be promoted to leader"
            );
        }
    }
}
```

- [ ] **Step 2: Run tests + clippy**

```bash
cargo test -p crabka-rebalancer --lib goals::min_topic_leaders_per_broker -- --nocapture
```

Expected: 4 tests pass.

```bash
cargo clippy -p crabka-rebalancer --lib -- -D warnings 2>&1 | grep "min_topic_leaders_per_broker"
```

Expected: no output. If a `cast_possible_truncation` lint fires on `ctx.min_topic_leaders_per_broker as usize`, switch to `usize::try_from(ctx.min_topic_leaders_per_broker).unwrap_or(0)`.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/goals/min_topic_leaders_per_broker.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43c): MinTopicLeadersPerBroker goal (soft)

Per-(broker, topic): every broker that holds at least one replica
of a topic should lead at least N partitions of that topic, where
N = ctx.min_topic_leaders_per_broker. Default 0 = goal is a no-op.
Emits leader-only movements; under-served broker must be in
replica set AND in ISR. Four unit tests cover min=0 no-op,
min=1 coverage, broker-not-in-replicas skip, and ISR-required.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Registry + binary + Helm

### Task 5: `GoalRegistry`, CLI flag, Helm chart

**Files:**
- Modify: `crates/rebalancer/src/api/mod.rs`
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`
- Modify: `charts/crabka-rebalancer/values.yaml`
- Modify: `charts/crabka-rebalancer/templates/deployment.yaml`
- Modify: `charts/crabka-rebalancer/tests/deployment_test.yaml`

- [ ] **Step 1: Extend `GoalRegistry::default_registry()`**

Edit `crates/rebalancer/src/api/mod.rs`. Find `default_registry` and add the three new goals. The existing implementation looks like:

```rust
pub fn default_registry() -> Self {
    Self {
        goals: vec![
            Box::new(crate::goals::preferred_leader_idempotency::PreferredLeaderIdempotency),
            Box::new(crate::goals::replica_distribution::ReplicaDistribution),
            Box::new(crate::goals::leader_distribution::LeaderDistribution),
        ],
    }
}
```

Replace with:

```rust
pub fn default_registry() -> Self {
    Self {
        goals: vec![
            // Hard goals (priority order matters for the optimizer's Hard-first ordering).
            Box::new(crate::goals::preferred_leader_idempotency::PreferredLeaderIdempotency),
            Box::new(crate::goals::rack_aware::RackAware),
            // Soft goals.
            Box::new(crate::goals::replica_distribution::ReplicaDistribution),
            Box::new(crate::goals::leader_distribution::LeaderDistribution),
            Box::new(crate::goals::topic_replica_distribution::TopicReplicaDistribution),
            Box::new(crate::goals::min_topic_leaders_per_broker::MinTopicLeadersPerBroker),
        ],
    }
}
```

Also update the existing `default_registry_has_three_goals` test in the same file's `#[cfg(test)] mod tests` block:

```rust
#[test]
fn default_registry_has_six_goals() {
    let r = GoalRegistry::default_registry();
    let all = r.select(&[]).unwrap();
    assert_eq!(all.len(), 6);
}
```

Rename the function from `_has_three_goals` to `_has_six_goals` and bump the assertion. Keep `select_by_name` and `select_unknown_goal_errors` tests as-is.

- [ ] **Step 2: Add the CLI flag**

Edit `crates/rebalancer/src/bin/rebalancer.rs`. Find the `Args` struct (around line 28) and add a new field, placed near the `imbalance_threshold_pct` field for grouping:

```rust
    /// Minimum leader count per (broker, topic) pair for the
    /// `MinTopicLeadersPerBroker` goal. `0` (default) disables it.
    #[arg(long, env = "CRABKA_MIN_TOPIC_LEADERS_PER_BROKER", default_value_t = 0)]
    min_topic_leaders_per_broker: u32,
```

Then find the `goal_ctx: GoalContext { ... }` literal (around line 211) and replace the hard-coded `0` from T1's compile-keeper with the real arg:

```rust
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
            min_topic_leaders_per_broker: args.min_topic_leaders_per_broker,
        },
```

- [ ] **Step 3: Update Helm `values.yaml`**

Edit `charts/crabka-rebalancer/values.yaml`. Find the rebalancer-config section and add:

```yaml
minTopicLeadersPerBroker: 0
```

…placed near `imbalanceThresholdPct` for grouping.

- [ ] **Step 4: Update Helm `templates/deployment.yaml`**

Edit `charts/crabka-rebalancer/templates/deployment.yaml`. In the container `env:` list (after the `CRABKA_REASSIGNMENT_BATCH_SIZE` entry), add:

```yaml
            - name: CRABKA_MIN_TOPIC_LEADERS_PER_BROKER
              value: {{ .Values.minTopicLeadersPerBroker | quote }}
```

- [ ] **Step 5: Update Helm `tests/deployment_test.yaml`**

Edit `charts/crabka-rebalancer/tests/deployment_test.yaml`. Find the "passes bootstrapServers env var" test (the one with `contains` against the env list) and either extend it or add a new test:

```yaml
  - it: passes minTopicLeadersPerBroker env var
    set:
      minTopicLeadersPerBroker: 2
    asserts:
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_MIN_TOPIC_LEADERS_PER_BROKER
            value: "2"
```

- [ ] **Step 6: Build + test + clippy**

```bash
cargo build -p crabka-rebalancer
```

Expected: clean (with T2/T3/T4 landed, the entire crate now compiles).

```bash
cargo test -p crabka-rebalancer --lib 2>&1 | tail -5
```

Expected: all lib tests pass (existing + 13 new from T2/T3/T4).

```bash
cargo clippy -p crabka-rebalancer --all-targets -- -D warnings 2>&1 | tail -5
```

Expected: clean.

```bash
helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092 2>&1 | tail -3
```

Expected: `1 chart(s) linted, 0 chart(s) failed`. (If `helm` isn't locally available, skip — CI runs it in the helm-lint job.)

```bash
helm unittest charts/crabka-rebalancer 2>&1 | tail -10
```

Expected: all suites pass with the new test in the deployment suite. (Skip if `helm-unittest` plugin isn't installed.)

- [ ] **Step 7: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/api/mod.rs crates/rebalancer/src/bin/rebalancer.rs charts/crabka-rebalancer/values.yaml charts/crabka-rebalancer/templates/deployment.yaml charts/crabka-rebalancer/tests/deployment_test.yaml
git -C /home/matt/git/crabka commit -m "rebalancer(43c): wire three new goals into GoalRegistry + CLI + Helm

GoalRegistry::default_registry adds RackAware (hard),
TopicReplicaDistribution (soft), MinTopicLeadersPerBroker (soft).
New CLI flag --min-topic-leaders-per-broker (env
CRABKA_MIN_TOPIC_LEADERS_PER_BROKER, default 0). Helm values gain
minTopicLeadersPerBroker, deployment template wires the env var,
new helm-unittest assertion verifies the env entry renders.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Integration test

### Task 6: `rack_aware_eliminates_same_rack_collisions` integration test

**Files:**
- Modify: `crates/rebalancer/tests/end_to_end.rs`

- [ ] **Step 1: Append the new test**

Read the current `tests/end_to_end.rs` to see the existing imports + fixtures. After the last existing test, append:

```rust
/// Synthetic ClusterState with three brokers in rack labels [A, A, B]
/// and a partition with replicas on the two rack-A brokers. The
/// RackAware goal must propose moving one off to a non-A rack. We
/// drive the optimizer directly (no real broker needed) since this
/// test is purely about goal interaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rack_aware_eliminates_same_rack_collisions() {
    use crabka_rebalancer::goals::rack_aware::RackAware;
    use crabka_rebalancer::goals::{Goal, GoalContext};
    use crabka_rebalancer::model::{BrokerView, ClusterState, Movement, PartitionView};

    let state = ClusterState {
        cluster_id: Some("c".into()),
        snapshot_at_ms: 0,
        brokers: vec![
            BrokerView { id: 1, host: "h1".into(), port: 9092, rack: Some("A".into()) },
            BrokerView { id: 2, host: "h2".into(), port: 9092, rack: Some("A".into()) },
            BrokerView { id: 3, host: "h3".into(), port: 9092, rack: Some("B".into()) },
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

    let ctx = GoalContext {
        imbalance_threshold_pct: 10,
        max_movements_per_proposal: 256,
        min_topic_leaders_per_broker: 0,
    };

    let mvs: Vec<Movement> = RackAware.propose(&state, &ctx);
    assert_eq!(mvs.len(), 1, "expected exactly one RackAware movement, got {mvs:?}");
    let m = &mvs[0];
    assert_eq!(m.topic, "t");
    assert_eq!(m.partition, 0);
    assert!(
        !m.new_replicas.contains(&1) || !m.new_replicas.contains(&2),
        "movement must remove one of the rack-A brokers; got {m:?}"
    );
    assert!(
        m.new_replicas.contains(&3),
        "movement must add the rack-B broker (3); got {m:?}"
    );
}
```

The test exercises the goal directly (not via `Execution` or the Connect-RPC handlers) because slice 43c is goal-only — there's no new wire-path. The optimizer's `Hard`-first ordering means RackAware would run first in a full `optimize()` invocation; we don't need a separate test for that since slice 43a already covers Hard-first ordering.

- [ ] **Step 2: Run the test**

```bash
cargo test -p crabka-rebalancer --test end_to_end rack_aware_eliminates_same_rack_collisions -- --nocapture 2>&1 | tail -10
```

Expected: 1 test passes. Also re-run the full e2e suite:

```bash
cargo test -p crabka-rebalancer --test end_to_end 2>&1 | tail -5
```

Expected: 6 tests pass (5 existing + 1 new).

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/tests/end_to_end.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43c): integration test for RackAware collision resolution

Builds a synthetic ClusterState with brokers in racks [A, A, B] and
a partition with both replicas on rack-A brokers. Asserts RackAware
emits exactly one movement that adds the rack-B broker and removes
one of the rack-A pair.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — Docs

### Task 7: STATUS docs

**Files:**
- Modify: `STATUS.md`

- [ ] **Step 1: Append slice-43c entry**

`STATUS.md` is chronological-append-at-end. Append:

```markdown
## Slice 43c — Rebalancer topology goals (2026-05-17)

- Three new goals shipped under the existing `Goal` trait:
  - `RackAware` (hard): no two replicas of the same partition share a
    rack tag (`BrokerView.rack`). Brokers with `rack: None` each
    count as their own pseudo-rack (matches Kafka KIP-36). Strict
    mode: if RF > distinct rack count for any partition, the goal
    logs `warn!` and emits no movements for that partition — never
    produces `HardGoalUnsatisfied`.
  - `TopicReplicaDistribution` (soft): per-topic replica balance.
    Distinct from the existing cluster-wide `ReplicaDistribution`;
    catches the case where a single topic is concentrated on one
    broker even though cluster-wide counts look balanced.
  - `MinTopicLeadersPerBroker` (soft, default off): every broker
    that holds at least one replica of a topic should also lead at
    least `N` partitions of that topic. `N` comes from the new
    `--min-topic-leaders-per-broker` CLI flag (env
    `CRABKA_MIN_TOPIC_LEADERS_PER_BROKER`, default 0). At default
    config the goal is a no-op; operators opt in by setting N > 0.
- `GoalRegistry::default_registry` now contains six goals in
  priority order: `PreferredLeaderIdempotency`, `RackAware`
  (Hard); `ReplicaDistribution`, `LeaderDistribution`,
  `TopicReplicaDistribution`, `MinTopicLeadersPerBroker` (Soft).
- 13 new unit tests (5 + 4 + 4) across the three new goal files,
  plus 1 new integration test
  (`rack_aware_eliminates_same_rack_collisions`).
- No proto changes, no persistence changes, no executor changes.
  Slice 43c is goal-only.
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43c-design.md`].
- Out of scope (deferred): `RackAwareDistributionGoal` (soft,
  best-effort variant of RackAware); per-proposal goal config
  (requires proto change); capacity / usage / CPU / anomaly goals
  (slices 43d–43g).
```

- [ ] **Step 2: Final verification**

```bash
cargo fmt --check 2>&1 | tail -3
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -3
cargo test -p crabka-rebalancer 2>&1 | tail -10
```

All three must pass clean. If `cargo fmt --check` reports differences, run `cargo fmt` and commit separately.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add STATUS.md
git -C /home/matt/git/crabka commit -m "rebalancer(43c): STATUS

Slice 43c entry documenting the three new goals (RackAware hard +
TopicReplicaDistribution soft + MinTopicLeadersPerBroker soft),
the new CLI flag, and the goal-only scope (no proto / persistence
/ executor changes).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-review checklist

**1. Spec coverage:**
- Goal semantics (RackAware strict / pseudo-rack / no-HardGoalUnsatisfied) → T2
- TopicReplicaDistribution per-topic semantics → T3
- MinTopicLeadersPerBroker default-off + ISR + replica-set requirement → T4
- `GoalContext.min_topic_leaders_per_broker` field → T1
- CLI flag → T5
- Helm chart wiring → T5
- helm-unittest assertion → T5
- `GoalRegistry::default_registry` six goals → T5
- Integration test → T6
- STATUS entry → T7

**2. Placeholder scan:** No "TBD" / "implement later" / "similar to" patterns. All code blocks are concrete. Adaptations (e.g. clippy fix-ups) are specified explicitly.

**3. Type consistency:** `BrokerView`, `PartitionView`, `Movement`, `ClusterState`, `Goal`, `GoalContext`, `GoalPriority`, `min_topic_leaders_per_broker` all spelled identically across tasks.
