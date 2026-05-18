# Crabka rebalancer — slice 43c — topology goals (design)

**Date:** 2026-05-17
**Status:** Spec, ready for implementation plan
**Scope:** Three new goals — one hard (`RackAware`) and two soft (`TopicReplicaDistribution`, `MinTopicLeadersPerBroker`). Goal-only slice — no proto / persistence / executor changes.

## Goal

Land slice 43c: the rebalancer can produce proposals that respect rack diversity and balance replicas + leaders on a per-topic basis. After 43c the rebalancer ships six goals (three from 43a + three from this slice) covering the placement axes most production Kafka operators care about.

## Out of scope (deferred)

- `RackAwareDistributionGoal` (soft, best-effort variant). Defer until operators ask.
- Per-proposal goal configuration (proto change to `CreateProposalRequest`). 43c uses CLI flags only.
- Capacity / usage / CPU / anomaly goals (slices 43d–43g).
- Multi-broker integration test coverage — same single-broker fixture constraint as 43a/43b.

## Decisions captured during brainstorm

1. **`RackAware` strict mode:** matches Cruise Control's `RackAwareGoal` (not `RackAwareDistributionGoal`). If RF > distinct rack count for any partition, the goal emits no movements for that partition and logs a `warn!`. It never produces `HardGoalUnsatisfied` — the goal self-limits rather than failing the proposal.
2. **Brokers with `rack: None`:** each treated as its own pseudo-rack (matches Kafka KIP-36's broker-side rack-aware partition assignment).
3. **`TopicReplicaDistribution`:** uses the existing `GoalContext.imbalance_threshold_pct` knob (same as `ReplicaDistribution`). Per-topic scope: each topic's replica counts must lie within the threshold band; balance across topics is the existing `ReplicaDistribution` goal's job.
4. **`MinTopicLeadersPerBroker`:** new `GoalContext.min_topic_leaders_per_broker: u32` field, sourced from a new CLI flag. Default `0` (goal is a no-op at default config).
5. **Goal registry order:** Hard goals first by priority. The new order in `GoalRegistry::default_registry()` is `PreferredLeaderIdempotency`, `RackAware`, `ReplicaDistribution`, `LeaderDistribution`, `TopicReplicaDistribution`, `MinTopicLeadersPerBroker`.

## Component layout

Three new files under `crates/rebalancer/src/goals/`:

```
crates/rebalancer/src/goals/
├── leader_distribution.rs                  # 43a (unchanged)
├── min_topic_leaders_per_broker.rs         # NEW
├── mod.rs                                  # MODIFIED — three new module mounts + GoalContext.min_topic_leaders_per_broker
├── preferred_leader_idempotency.rs         # 43a (unchanged)
├── rack_aware.rs                           # NEW
├── replica_distribution.rs                 # 43a (unchanged)
└── topic_replica_distribution.rs           # NEW
```

Other changes:

```
crates/rebalancer/src/api/mod.rs            # MODIFIED — GoalRegistry::default_registry adds three new Goal instances
crates/rebalancer/src/bin/rebalancer.rs     # MODIFIED — new CLI flag --min-topic-leaders-per-broker, threaded into GoalContext
charts/crabka-rebalancer/values.yaml         # MODIFIED — minTopicLeadersPerBroker: 0
charts/crabka-rebalancer/templates/deployment.yaml  # MODIFIED — CRABKA_MIN_TOPIC_LEADERS_PER_BROKER env var
charts/crabka-rebalancer/tests/deployment_test.yaml # MODIFIED — one new `contains` assertion
README.md                                    # unchanged
STATUS.md                                    # MODIFIED — slice 43c entry
```

No proto changes. No new `model` types. No new persistence. No executor changes.

## Goal semantics

### `RackAware` (hard)

**Invariant:** for each partition, every replica lives on a broker in a different rack — or, for brokers with `rack: None`, on a different broker (each None-rack broker is its own pseudo-rack).

**Emit shape:** one movement per resolved collision. The movement swaps one replica from a colliding broker to a candidate broker in a not-yet-represented rack. Candidate selection is deterministic — sorted by broker id ascending — for stable test fixtures.

**Infeasibility detection:** if for any partition the count of distinct racks (treating None as unique) across the cluster's live brokers is less than the partition's RF, the goal cannot fully diversify. It emits no movements for that partition and logs `warn!(topic = %p.topic, partition = p.partition, "RackAware: cluster has fewer racks than RF; goal self-limits")` once per affected partition.

**Greedy:** the goal recomputes collisions after each emitted movement, so a single `propose()` call can resolve multiple collisions in one pass. Bounded by `ctx.max_movements_per_proposal` (the optimizer's cap is the safety net).

### `TopicReplicaDistribution` (soft)

**Invariant:** for each topic, replicas-per-broker across all brokers satisfies `(max - min) * 100 / total <= ctx.imbalance_threshold_pct`, where `total` is the topic's total replica count.

**Emit shape:** greedy hot→cold swaps within a topic. Iteratively find the topic's most-loaded and least-loaded broker; emit a movement that re-homes one replica from hot to cold, provided the cold broker isn't already in the partition's replica set and the swap doesn't break RF. Stop when within threshold or no valid swap remains.

**Distinct from `ReplicaDistribution`:** the existing goal balances cluster-wide replica counts (sum across all topics). This goal balances per-topic; a single topic concentrated on one broker won't move that goal but will move this one.

### `MinTopicLeadersPerBroker` (soft)

**Invariant:** for every (broker, topic) pair where the broker holds at least one replica of the topic, the broker holds at least `ctx.min_topic_leaders_per_broker` leaders of that topic.

**Default (off):** when `ctx.min_topic_leaders_per_broker == 0`, the goal is a no-op (returns empty Vec).

**Emit shape:** for each under-served (broker, topic), find a partition of that topic whose current leader broker has a surplus (more than the minimum) and whose replica set includes the under-served broker + the under-served broker is in ISR. Emit a leader-only swap. Skip pairs where the broker isn't in the topic's replica set at all (this goal doesn't move replicas — only leaders).

## Configuration

`GoalContext` (defined in `crates/rebalancer/src/goals/mod.rs`) gains one field:

```rust
pub struct GoalContext {
    pub imbalance_threshold_pct: u32,
    pub max_movements_per_proposal: usize,
    /// Minimum leader count per (broker, topic) pair. `0` = goal is a no-op.
    pub min_topic_leaders_per_broker: u32,  // NEW
}
```

The binary entry's `GoalContext` literal sets it from the new CLI flag.

CLI flag (in `bin/rebalancer.rs`):

```rust
#[arg(long, env = "CRABKA_MIN_TOPIC_LEADERS_PER_BROKER", default_value_t = 0)]
min_topic_leaders_per_broker: u32,
```

Helm chart additions:
- `values.yaml`: `minTopicLeadersPerBroker: 0`
- `templates/deployment.yaml`: `- name: CRABKA_MIN_TOPIC_LEADERS_PER_BROKER` env entry, value `{{ .Values.minTopicLeadersPerBroker | quote }}`
- `tests/deployment_test.yaml`: one new `contains` assertion in the existing "passes env vars" test for the new env var name.

## Testing

### Unit tests (per goal, `#[cfg(test)]` in source files)

**`rack_aware::tests`** (5 tests):
- `balanced_three_racks_no_op` — RF=3 across 3 racks, no collisions, returns empty Vec.
- `single_collision_resolved` — two replicas in rack A, one in B; goal emits one movement to rack C.
- `multi_collision_iterates_within_propose` — multiple partitions with collisions; goal emits one movement per collision in a single call.
- `rf_equals_rack_count_satisfiable` — RF=2 across 2 racks, no collisions possible after correction.
- `rf_greater_than_rack_count_logs_warn_and_skips` — RF=3 across 2 racks; the affected partition gets zero movements (verified by counting emitted movements for that partition's key).

**`topic_replica_distribution::tests`** (4 tests):
- `balanced_topic_no_op` — three brokers each hold 4 replicas of topic `t`; threshold 10%; returns empty.
- `hot_broker_triggers_swaps` — broker 1 holds 9 replicas, brokers 2+3 hold 0; emits movements until within threshold.
- `multi_topic_independence` — topic A balanced, topic B imbalanced; only topic B sees movements.
- `respects_max_movements_cap` — extreme imbalance + `max_movements_per_proposal = 2`; emits exactly 2 movements.

**`min_topic_leaders_per_broker::tests`** (4 tests):
- `min_zero_is_no_op` — `ctx.min_topic_leaders_per_broker = 0`; returns empty Vec regardless of distribution.
- `min_one_ensures_coverage` — three brokers, one topic, all leaders on broker 1, `min = 1`; emits one leader-swap per under-served (broker, topic).
- `broker_not_in_replica_set_skipped` — broker C isn't in topic T's replica set anywhere; goal doesn't try to flip a leader onto C.
- `under_served_not_in_isr_skipped` — under-served broker not in ISR for the candidate partition; goal skips that swap (a non-ISR broker can't safely be made leader).

### Integration test (1 new test in `tests/end_to_end.rs`)

**`rack_aware_eliminates_same_rack_collisions`:** construct a synthetic `ClusterState` with three brokers (rack labels A, A, B), a partition `t/0` with replicas `[1, 2]` (both in rack A → collision), call `optimize()` with the full goal set, assert the resulting proposal includes a movement that swaps one of `{1, 2}` for `3`.

No new `connect_smoke.rs` test — no new RPC surface.

## Risks

- **`RackAware` candidate selection deterministic but not optimal.** Sorting by broker id ascending picks the lowest-id available broker in the target rack. This is good enough for the strict-mode goal but may concentrate fixes on lower-numbered brokers. Acceptable trade-off — the soft goals balance after RackAware.
- **Goal ordering matters.** `RackAware` must run before the soft balance goals so soft moves don't undo rack-corrections. The existing optimizer ordering (Hard first, then Soft, registration order within priority) handles this — but `RackAware` and `PreferredLeaderIdempotency` are both Hard; `PreferredLeaderIdempotency` (leader-only) runs first by registration order. A `PreferredLeaderIdempotency` movement could create a rack collision? No — it only changes the leader, not the replica set, so racks are preserved. Verified by inspection.
- **`MinTopicLeadersPerBroker` requires ISR membership.** A broker in the replica set but not the ISR can't safely be made leader. The goal correctly checks `isr.contains(&broker)` per Kafka semantics.

## Acceptance criteria

1. `cargo test -p crabka-rebalancer` — 81+ lib tests (68 from 43b + 13 new) + 6 e2e tests (5 from 43b + 1 new) + 2 Connect smoke tests, all green.
2. `cargo fmt --check` and `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092` clean.
4. `helm unittest charts/crabka-rebalancer` clean (5 existing suites pass with the updated `deployment_test.yaml`).
5. `STATUS.md` gains a slice-43c entry listing the three new goals + the new CLI flag.

## File layout (summary)

```
crates/rebalancer/
├── src/
│   ├── goals/
│   │   ├── mod.rs                                # MODIFIED — three pub mod lines + min_topic_leaders_per_broker field on GoalContext
│   │   ├── rack_aware.rs                         # NEW
│   │   ├── topic_replica_distribution.rs         # NEW
│   │   └── min_topic_leaders_per_broker.rs       # NEW
│   ├── api/mod.rs                                # MODIFIED — GoalRegistry::default_registry adds three new goals
│   └── bin/rebalancer.rs                         # MODIFIED — new CLI flag + threading
└── tests/end_to_end.rs                           # MODIFIED — one new integration test
charts/crabka-rebalancer/
├── values.yaml                                    # MODIFIED — minTopicLeadersPerBroker: 0
├── templates/deployment.yaml                      # MODIFIED — CRABKA_MIN_TOPIC_LEADERS_PER_BROKER env
└── tests/deployment_test.yaml                     # MODIFIED — one new contains assertion
STATUS.md                                          # MODIFIED — slice 43c entry
```
