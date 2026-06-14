# Crabka partition-reassignment model — stateright design

**Date:** 2026-06-13
**Status:** Approved (design); plan + implementation to follow
**Workstream:** A (stateright correctness models) — KIP-455 partition reassignment, the last strong model candidate after the consensus / share-group / ISR / failover trilogy-plus.
**Predecessor specs:**
- `2026-06-13-crabka-isr-replica-state-model-design.md` (ISR/HWM; #515)
- `2026-06-13-crabka-failover-recovery-model-design.md` (failover/unclean-recovery; #516)

## Goal

Model-check the pure KIP-455 reassignment-completion core
(`reassignment.rs::compute_reassignment_progress`) with
[stateright](https://github.com/stateright/stateright), exhaustively exploring
every interleaving of replica catch-up, broker liveness, and completion ticks,
and proving the reassignment-safety invariants — above all that a partition's
replica set never switches off its leader (the leader is handed off *before* it
is removed) and the ISR stays a subset of the replica set throughout.

This is the **wrap-real** counterpart for partition reassignment, structurally
identical to the just-built failover model (`failover_one`): a pure
per-partition decision function driven by the checker.

## Background: what `compute_reassignment_progress` is

`reassignment.rs::compute_reassignment_progress(image, liveness)` is the
controller-leader background task's pure core. For each partition with an
in-flight reassignment (`adding_replicas` or `removing_replicas` non-empty):

1. `target = replicas − removing_replicas` (the replica set to converge to).
2. If not all `adding_replicas` are in `isr` → **wait** (no update this tick).
3. Else if `leader ∈ removing_replicas` → **leader-handoff phase**: pick a new
   leader from `target ∩ isr ∩ alive`; if found, emit a `PartitionRecord` with
   that leader and `leader_epoch + 1`, the replica set/ISR/adding/removing
   **unchanged** (completion happens on a later tick). If none, wait.
4. Else → **completion phase**: emit a `PartitionRecord` with `replicas = target`,
   `isr = isr ∩ target`, `adding = removing = []`, leader and `leader_epoch`
   **unchanged**, directories remapped (`remap_directories`).

The decision is deterministic given `(PartitionRecord, alive_set)`; it is only
`async` to snapshot the liveness set once. It has a comprehensive existing unit
suite (`reassignment::tests`, 10 tests).

## Why this is the right slice

- It is the last strong correctness-model candidate (the scout rated it
  STRONG; the core is cleanly separable and deterministic).
- KIP-455 reassignment is safety-critical: a bug that drops a leader before
  handoff, or switches the replica set off the leader, loses availability or
  committed data. This proves it can't.
- It reuses the exact `failover_one` extraction + wrap-real pattern, so it is
  low-risk and fast to build.

## Production change: extract `reassign_one`

Mirror the `failover_one` extraction. Pull the per-partition body out of the
`for pr in image.reassignments_in_flight()` loop into a pure sync function:

```rust
/// The pure per-partition reassignment decision: given a partition's current
/// record and the alive set, return the next `PartitionRecord` (a leader
/// handoff or a completion), or `None` to wait. No I/O. Extracted so the policy
/// is independently unit-testable and model-checkable.
pub(crate) fn reassign_one(
    pr: &PartitionRecord,
    alive: &std::collections::HashSet<NodeId>,
) -> Option<PartitionRecord> {
    let target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|r| !pr.removing_replicas.contains(r))
        .copied()
        .collect();
    if !pr.adding_replicas.iter().all(|n| pr.isr.contains(n)) {
        return None; // wait for replication
    }
    if pr.removing_replicas.contains(&pr.leader) {
        // Leader-handoff phase: target ∩ isr ∩ alive.
        let new_leader = target
            .iter()
            .find(|n| pr.isr.contains(n) && alive.contains(n))
            .copied()?;
        return Some(PartitionRecord {
            leader: new_leader,
            leader_epoch: pr.leader_epoch + 1,
            partition_epoch: pr.partition_epoch + 1,
            replicas: pr.replicas.clone(),
            isr: pr.isr.clone(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            topic: pr.topic.clone(),
            partition: pr.partition,
        });
    }
    // Completion phase.
    let new_isr: Vec<NodeId> = pr.isr.iter().filter(|n| target.contains(n)).copied().collect();
    let new_directories = remap_directories(&pr.replicas, &pr.directories, &target);
    Some(PartitionRecord {
        leader: pr.leader,
        leader_epoch: pr.leader_epoch,
        replicas: target,
        isr: new_isr,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
        topic: pr.topic.clone(),
        partition: pr.partition,
    })
}
```

`compute_reassignment_progress` becomes a thin loop:

```rust
let alive = liveness.alive_snapshot().await;
for pr in image.reassignments_in_flight() {
    if let Some(next) = reassign_one(pr, &alive) {
        updates.push(MetadataRecord::V1Partition(next));
    }
}
updates
```

Behavior-preserving; the existing `reassignment::tests` (10 tests) are the
safety net. No other production change. `stateright` is already a broker
dev-dependency (on main via #514); the model lives in-src as a `#[cfg(test)]`
descendant module of `reassignment`.

## What this is NOT

- **No alter-request validation / start path** (`alter_partition_reassignments.rs`
  — turns a client `AlterPartitionReassignments` into the `adding`/`removing`
  record). This slice models the *convergence* core, not request validation.
- **No async task plumbing** (`run` loop, image watch, `submit_change`).
- **No multi-partition concurrency / cancellation.** Single-partition
  reassignment lifecycle. (`compute_reassignment_progress` already handles each
  partition independently.)
- **No `LinearizabilityTester`** — reassignment safety is the invariant set below.

## Model specification

### State

```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ReassignState {
    replicas: Vec<NodeId>,  // current replica set (order significant)
    isr: Vec<NodeId>,       // order significant (isr ∩ target preserves order)
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    leader: NodeId,
    leader_epoch: i32,
    alive: BTreeSet<NodeId>,
}
```

A `PartitionRecord` is built from this state to call `reassign_one`
(`directories` is set empty — not safety-relevant; the model ignores the
result's directories).

### Config (held in the model, not the state)

```rust
struct ReassignModel {
    replicas: Vec<NodeId>,
    adding: Vec<NodeId>,
    removing: Vec<NodeId>,
    initial_isr: Vec<NodeId>,
    leader: NodeId,
    max_epoch: i32,
}
```

**Initial state:** one in-flight reassignment — `replicas`, `initial_isr`,
`adding`, `removing`, `leader`, `leader_epoch = 0`, all replicas alive.

### Actions

- `AdmitToIsr(node)` — for `node ∈ replicas`, `node ∉ isr`: add it to `isr`
  (in replica order). Models a lagging/adding replica catching up and the
  controller admitting it. (Only ever *adds* a caught-up replica, so it is
  always safe.)
- `Die(node)` / `Revive(node)` — toggle `alive` (keep ≥1 alive). Affects which
  replicas are eligible new leaders in the handoff phase.
- `ReassignStep` — when the reassignment is in flight (`adding` or `removing`
  non-empty) and `leader_epoch < max_epoch`: build the `PartitionRecord`, call
  the real `reassign_one(pr, &alive)`, verify the decision's safety properties
  against the pre-state, then apply the returned record (or `None` ⇒ no
  transition).

`next_state` for `ReassignStep`:
1. Clone state, build `pr`, `let decision = reassign_one(&pr, &alive_set)`.
2. `assert_step(&pre, &decision)` — panic on any safety violation.
3. If `Some(next)`, set `leader/isr/adding/removing/replicas/leader_epoch` from
   `next`; if `None`, return `None`.

### Properties

**State-level — `Property::always`:**
- `isr_subset_replicas` — `isr ⊆ replicas`.
- `leader_in_replicas` — `leader ∈ replicas`.
- `leader_in_isr` — `leader ∈ isr` (Kafka invariant; the model never drops the
  leader from ISR, and handoff/completion both keep it in ISR). Implies the ISR
  is never empty.
- `adding_subset_replicas` — `adding ⊆ replicas`.
- `removing_subset_replicas` — `removing ⊆ replicas`.

**Transition-level — `assert!` in `next_state` (`assert_step`):** the decision
shape is classified from `(pre, next)`: a **handoff** is `next.leader !=
pre.leader`; otherwise a `Some` with `next.adding` and `next.removing` both
empty is a **completion**; any other `Some` shape is unexpected and panics.
- `leader_epoch_monotonic` — `next.leader_epoch >= pre.leader_epoch`.
- On a **handoff**: the replica set, ISR, adding, and removing are
  **unchanged**; `next.leader ∈ pre.isr`, `next.leader ∈ target`, `next.leader ∈
  alive`, `next.leader ∉ pre.removing`; `leader_epoch == pre.leader_epoch + 1`.
- On a **completion**: **`next.leader ∈ next.replicas`** (the replica set never
  switches off the leader — the headline safety), `next.replicas == target`
  (`= pre.replicas − pre.removing`), `next.isr ⊆ next.replicas`, `next.adding`
  and `next.removing` empty, leader and `leader_epoch` unchanged.
- `adding_caught_up_required` — any `Some` decision implies every `pre.adding`
  member was in `pre.isr` (no progress before replication completes).

**Non-vacuity — `Property::sometimes`:**
- `can_complete` — reach a state with `adding == [] && removing == []`
  (reassignment finished).
- `can_handoff` — leadership moved mid-reassignment. Formulated
  config-conditionally so it is not vacuously unsatisfiable in `reassign_basic`
  (where the leader is never removed and no handoff occurs):
  `!m.removing.contains(&m.leader) || s.leader != m.leader` — trivially
  witnessed when the config does no handoff, and requires an actual leader
  change in the handoff config.
- `can_wait` — the model exercises the wait-for-replication path: a reachable
  state where the reassignment is in flight (`adding`/`removing` non-empty) but
  some `adding` member is not yet in `isr` (so a `ReassignStep` would return
  `None`). Witnessed by the initial state.

### Bounds (`within_boundary`) — OOM safety

`within_boundary` bounds only `leader_epoch <= max_epoch` (the replica set, ISR,
adding, removing are bounded by the fixed node set). Plus the standing OOM rule:
a hard `target_state_count` (~200k) + `timeout`, the `run` harness asserts the
search was exhaustive (not cap/depth-truncated), and **every checker run is
executed under the PowerShell memory watchdog** while bounds are tuned. The
reassignment is a short, terminating process (catch-up → handoff ≤1 →
completion ≤1), so the space is tiny (low hundreds, like the failover model).

### Configurations

1. **`reassign_basic`** — `replicas=[1,2,3]`, `adding=[3]`, `removing=[2]`,
   `initial_isr=[1,2]`, `leader=1`, `max_epoch=10`. Leader not removed → catch-up
   (admit 3) then completion to `[1,3]`.
2. **`reassign_leader_handoff`** — same but `leader=2` (`∈ removing`) → catch-up,
   then a leader handoff (2 → an alive target ISR member), then completion.

(Bounds may scale empirically — e.g. `replicas=[1,2,3,4,5]`, `adding=[4,5]`,
`removing=[1,2]` — if the state count stays well under the cap.)

## File structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/reassignment.rs` | (modify) add `reassign_one`; rewrite `compute_reassignment_progress` to call it; declare `#[cfg(test)] #[path="reassignment_model.rs"] mod reassignment_model;`. |
| `crates/broker/src/reassignment_model.rs` | (create) the model: `ReassignModel`/`ReassignState`/`ReassignAction`, the `Model` impl + `assert_step`, the watchdog-friendly `run` harness, and the `#[test]` configs. |

## Testing strategy

- Each config is a `#[test]` building a bounded `ReassignModel`, spawning a BFS
  checker with `target_state_count` + `timeout`, joining, asserting
  exhaustiveness, and `assert_properties()`.
- The existing `reassignment::tests` (10 tests) must still pass — confirming the
  `reassign_one` extraction is behavior-preserving.
- Runs under the memory watchdog while tuning; the committed tests are
  self-bounded and safe for unguarded CI.
- **Nightly fmt** (`cargo +nightly fmt -p crabka-broker -- --check`) — CI's fmt
  gate is nightly; stable `cargo fmt` silently skips the workspace's unstable
  rustfmt options (this failed CI on a prior slice).

## Out of scope (future slices)

- KIP-848 consumer-group rebalance model.
- Alter-request validation / cancellation (`alter_partition_reassignments.rs`).
- Multi-partition / cluster-wide reassignment interactions.
- Dynamic voters (KIP-853 — blocked on the core), KIP-966 ELR (unimplemented).
