# Crabka failover / unclean-recovery model — stateright design

**Date:** 2026-06-13
**Status:** Approved (design); plan + implementation to follow
**Workstream:** A (stateright correctness models) — leader-change safety, the capstone of the consensus + ISR replication-safety trilogy
**Predecessor specs:**
- `2026-06-13-crabka-stateright-consensus-deflake-design.md` (raft consensus; merged #511)
- `2026-06-13-crabka-share-group-model-design.md` (share-partition acquisition; #514)
- `2026-06-13-crabka-isr-replica-state-model-design.md` (ISR/HWM replication; #515)

## Goal

Model-check the controller-side **leader-failover and unclean-recovery** decision logic with stateright, proving the leader-change safety invariants — above all that a **clean election never loses committed data**, an **out-of-ISR (unclean) election happens only when explicitly enabled**, and KIP-966 offset-aware recovery **elects the most-complete log**.

This completes the leader-change-safety trilogy: raft proves the metadata log is consistent; the ISR model proves *when* data is committed (`hw ≤ every ISR member's LEO`); this proves *what happens to that committed data when the leader fails* — the data-loss boundary.

## Background: the two pure cores

### 1. The failover scan — `leader_election.rs`

`compute_failover_changes(image, dead, liveness, metrics)` (lines 49-170) is
called when a broker transitions alive→dead. For each partition where the dead
broker is leader or an ISR member it computes `alive_isr` (the ISR minus the
dead broker and any other non-alive member), then decides:

- **dead was leader, `alive_isr` non-empty** → elect `alive_isr.first()`, ISR =
  `alive_isr`, `leader_epoch += 1`. **Clean** — the new leader was in the ISR, so
  it holds every committed record; no loss.
- **dead was leader, `alive_isr` empty** → branch on the topic's recovery config:
  - `Balanced`/`Aggressive` (KIP-966) → defer to the offset-aware Unclean
    Recovery Manager (don't elect synchronously).
  - `None` + `unclean.leader.election.enable=true` (KIP-841) → elect the first
    alive replica (in or out of ISR), ISR = `[new_leader]`, `leader_epoch += 1`.
    **Unclean** — possible data loss, allowed only because the operator opted in.
  - `None` + disabled → leave unavailable (no change).
- **dead was a non-leader ISR member** → shrink ISR to `alive_isr`, leader and
  `leader_epoch` unchanged.

`compute_offline_dir_failover_changes` (lines 186-295) has **identical**
per-partition logic; only the partition *filter* differs (offline-log-dir slot
vs dead-broker-in-replicas/ISR).

### 2. The KIP-966 winner selection — `unclean_recovery.rs`

Already pure, sync, dependency-free:
- `select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId>` — the
  most-complete log: highest `last_written_leader_epoch`, then highest
  `log_end_offset`, then lowest `broker_id` (deterministic tiebreak).
- `has_newer_leader(responses, known_leader_epoch) -> bool` — aborts a stale
  recovery when any responder reports a `current_leader_epoch` greater than the
  controller's known epoch.

The async `UncleanRecoveryManager` that polls replicas and submits the change is
I/O orchestration, out of scope; the *selection* is the safety-relevant core.

## Why this is the right next slice

- It is the data-loss boundary — the single most safety-critical Kafka behavior —
  and the natural capstone of the replication-safety models (consensus → ISR →
  failover).
- Both cores are deterministic and (after one extraction) pure/sync — clean
  wrap-real targets with a comprehensive existing test suite as a refactor net.
- KIP-853 dynamic voters remains blocked (core doesn't implement runtime
  membership changes); reassignment (KIP-455) is the other strong candidate but
  is about convergence rather than data-loss.

## Production change: extract `failover_one`

To wrap-real the failover scan without dragging `MetadataImage` + `async` +
`block_on` into the model, extract the shared per-partition decision into a pure
sync function, used by **both** `compute_failover_changes` and
`compute_offline_dir_failover_changes` (deduplicating ~80 lines):

```rust
pub(crate) enum FailoverDecision {
    /// Elect `leader` with `isr`; the caller bumps leader_epoch+1 and, when
    /// `unclean`, records the unclean-election metric.
    Elect { leader: NodeId, isr: Vec<NodeId>, unclean: bool },
    /// Defer to the offset-aware Unclean Recovery Manager (KIP-966).
    Recover(RecoveryStrategy),
    /// Dead broker was a non-leader ISR member: shrink ISR (leader/epoch kept).
    ShrinkIsr { isr: Vec<NodeId> },
    /// Leader is dead, ISR empty, and no unclean path is permitted/available.
    Unavailable,
    /// Nothing to do for this partition (dead broker isn't leader or ISR member).
    NoChange,
}

pub(crate) fn failover_one(
    pr: &PartitionRecord,
    dead: NodeId,
    alive: &HashSet<NodeId>,
    strategy: RecoveryStrategy,
    unclean_enabled: bool,
) -> FailoverDecision { /* the decision tree above */ }
```

`compute_failover_changes` / `compute_offline_dir_failover_changes` keep their
per-function partition filters and `alive_snapshot()`, then call `failover_one`
and map the decision to the `PartitionRecord`/recovery/metrics pushes (preserving
the `leader_epoch+1` on `Elect`, `partition_epoch+1` on every change, the
unclean metric, the recovery deferrals, and the warn logs). This is a
behavior-preserving refactor; the comprehensive existing `leader_election.rs`
failover tests are the safety net. It mirrors the ISR slice's `now`-injection: a
small extraction that makes the core deterministic and independently testable —
and here it also removes real duplication.

No other production change. `select_best_replica`/`has_newer_leader` are already
pure. `stateright` is already a broker dev-dependency. The models live in-src as
`#[cfg(test)]` descendant modules reading `pub(crate)` items directly.

## What this is NOT

- **No multi-partition `MetadataImage` model.** Each model is single-partition;
  concurrency is modeled as multiple broker deaths processed by per-broker
  `Failover` actions (faithful to the real per-broker liveness scan).
- **No async URM / replica-polling / `submit_change`.** The model drives the
  *decision* functions; gathering and submission are I/O.
- **No ELR (eligible-leader-replicas).** Not implemented in the codebase
  (`leader_recovery_state` is wire-only); cannot be modeled against real code.
- **No operator-triggered elections** (`select_new_leader_for_partition`,
  `select_replacement_leader_for_shutdown` — KIP-460 preferred / shutdown drain).
  Possible future slice.
- **No `LinearizabilityTester`** — leader-change safety is the invariant set below.

## Model specification

Two small model types in one file (`leader_failover_model.rs`).

### A. `FailoverModel` — the failover scan (`failover_one`)

**State:**
```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct FailoverState {
    leader: NodeId,
    isr: Vec<NodeId>,        // order significant (clean election picks isr.first())
    replicas: Vec<NodeId>,   // fixed assignment; order significant (KIP-841 picks replicas order)
    leader_epoch: i32,
    alive: BTreeSet<NodeId>, // which brokers are currently alive
}
```

**Config:** `{ replicas: Vec<NodeId>, strategy: RecoveryStrategy, unclean_enabled: bool, max_epoch: i32 }`.

**Initial state:** leader = `replicas[0]`, isr = replicas, all replicas alive, `leader_epoch = 0`.

**Actions:**
- `Die(node)` — when `node` is alive and not the only alive broker; removes it
  from `alive`. (Keep ≥1 alive so there's always a candidate to reason about.)
- `Revive(node)` — when `node` is dead; adds it back to `alive`.
- `Failover(dead)` — for each dead broker in `replicas` (the real scan's filter
  is `replicas.contains(dead) || isr.contains(dead)`; all model brokers are
  replicas): construct a `PartitionRecord` from the state, call
  the real `failover_one(pr, dead, &alive, strategy, unclean_enabled)`, verify
  the decision's safety properties (below), then apply it:
  - `Elect{leader, isr, ..}` → set leader/isr, `leader_epoch += 1`.
  - `ShrinkIsr{isr}` → set isr (leader/epoch unchanged).
  - `Recover` / `Unavailable` / `NoChange` → no state change (return `None`).

`max_epoch` bounds the space: `Die`→`Failover` cycles bump `leader_epoch`, so
`within_boundary` caps it and `Failover` is not offered past the cap.

**Transition-level asserts (verify each `failover_one` output against pre-state):**
- `clean_no_data_loss` — `Elect{leader, unclean: false}` ⟹ `leader ∈ pre_isr`
  (a clean election's new leader was in the ISR → held all committed data).
- `unclean_only_when_enabled` — `Elect{unclean: true}` ⟹ `unclean_enabled`.
- `elected_leader_alive` — `Elect{leader, ..}` ⟹ `leader ∈ alive`.
- `elected_leader_in_new_isr` — `Elect{leader, isr, ..}` ⟹ `leader ∈ isr`.
- `epoch_bumped_on_elect` — `Elect` ⟹ new `leader_epoch == pre + 1`.
- `shrink_only_removes` — `ShrinkIsr{isr}` ⟹ `isr ⊆ pre_isr` and leader/epoch
  unchanged.
- `recover_requires_strategy` — `Recover(s)` ⟹ `s != None` and the dead broker
  was the leader with an empty `alive_isr`.

**State-level `always` properties** (hold globally under *any* death/failover
ordering — deliberately limited to genuine invariants):
- `isr_subset_replicas` — `isr ⊆ replicas`.
- `leader_in_replicas` — `leader ∈ replicas`.

> No `isr_nonempty` invariant: an empty ISR (under-min-ISR) is a *legitimate*
> Kafka state — the real per-broker scan can transiently produce one (a
> non-leader `Failover` shrinking the last alive member while a dead leader
> hasn't been failed-over yet). The model therefore allows all `Die`/`Failover`
> orderings without pruning. This is safe to explore: ISR *shrink* never loses
> committed data (it only narrows who is in-sync); the only data-loss-relevant
> decisions are `Elect`s on leader death, which are always offered and are
> covered by the decision-level asserts above under every ordering.

**Non-vacuity `sometimes`:**
- `can_clean_elect` — a clean `Elect{unclean: false}` was applied (leader changed
  to a former ISR member).
- `can_unclean_elect` — an unclean `Elect{unclean: true}` was applied (only
  reachable when `unclean_enabled`).
- `can_shrink` — an ISR shrink occurred.
- `can_defer_recover` — a `Recover` decision occurred (only reachable when
  `strategy != None`).

**Configs:**
1. `failover_safe` — `strategy=None, unclean_enabled=false`. Asserts (via the
   transition asserts holding vacuously for the unclean case) that **no unclean
   election ever happens**; exercises clean election + shrink + unavailability.
2. `failover_unclean` — `strategy=None, unclean_enabled=true`. Exercises the
   KIP-841 out-of-ISR path, gated correctly.
3. `failover_recover` — `strategy=Balanced, unclean_enabled=false`. Exercises the
   KIP-966 deferral decision (`Recover`), asserting it only fires on empty-ISR
   leader death.

### B. `RecoveryModel` — KIP-966 winner selection (`select_best_replica` / `has_newer_leader`)

`select_best_replica`/`has_newer_leader` are pure functions, so the model
exhaustively *generates every bounded set of replica responses* and checks the
real function's output against a reference on each — a clean "the function is
correct on every reachable input" model (no evolving-winner ambiguity).

**State:**
```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct RecoveryState {
    // Responses gathered so far, keyed by replica. A replica reports at most
    // once (one log state). Built up by AddResponse actions.
    responses: BTreeMap<NodeId, ReplicaLog>, // { last_written_leader_epoch, log_end_offset, current_leader_epoch }
    known_leader_epoch: i32,
}
```

**Config:** `{ replicas: Vec<NodeId>, max_epoch: i32, max_leo: i64, known_leader_epoch: i32 }` — `replicas=[1,2,3]`, all caps small (≤2).

**Actions:** `AddResponse{node, last_written_epoch, leo, current_epoch}` — for a
replica not yet in `responses`, with each field ranging over its bounded domain.
(This fans out over every bounded response set.)

**Properties** (verify the real functions against a reference on every state):
- `always select_best_is_max` — let `infos = responses.values()` as
  `Vec<ReplicaLogInfo>`; if `select_best_replica(&infos) = Some(w)`, then the
  response with `broker_id == w` is `>=` every response by the spec order
  `(last_written_leader_epoch, log_end_offset, then lower broker_id)`, i.e. the
  real function returns the true maximum. Empty input ⟹ `None`.
- `always has_newer_leader_matches` — `has_newer_leader(&infos,
  known_leader_epoch)` equals `infos.iter().any(|i| i.current_leader_epoch >
  known_leader_epoch)` (the real function matches its specification).
- `sometimes can_pick_winner` — a non-empty `responses` yields `Some(winner)`.
- `sometimes can_detect_newer` — a state where `has_newer_leader` is `true`
  (some responder's `current_leader_epoch > known_leader_epoch`).

**Config:** `offset_recovery` — `replicas=[1,2,3]`, `max_epoch=2`, `max_leo=2`,
`known_leader_epoch=1` (so both `has_newer_leader` outcomes are reachable).

## Bounds & run protocol (OOM safety)

`within_boundary` caps `leader_epoch ≤ max_epoch` (FailoverModel) and log
epoch/LEO ≤ caps (RecoveryModel). Each config has a hard `target_state_count`
(~200k) + `timeout`, the `run` harness asserts the search was exhaustive (not
cap/depth-truncated), and **every checker run is executed under the PowerShell
memory watchdog** while bounds are tuned. With `replicas=[1,2,3]` the spaces are
tiny (low thousands), comparable to the ISR model. Start small, scale empirically.

## File structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/leader_election.rs` | (modify) add `FailoverDecision` + `failover_one`; rewrite `compute_failover_changes` and `compute_offline_dir_failover_changes` to call it; declare `#[cfg(test)] #[path="leader_failover_model.rs"] mod leader_failover_model;`. |
| `crates/broker/src/leader_failover_model.rs` | (create) both models: `FailoverModel`/`FailoverState`/`FailoverAction` + `RecoveryModel`/`RecoveryState`/`RecoveryAction`, the `Model` impls, the watchdog-friendly `run` harness, and the `#[test]` configs. |

`select_best_replica`/`has_newer_leader`/`ReplicaLogInfo` stay `pub(crate)` in
`unclean_recovery.rs`; the model imports them via `crate::unclean_recovery::…`.

## Testing strategy

- Each config is a `#[test]` building a bounded model, spawning a BFS checker
  with `target_state_count` + `timeout`, joining, asserting exhaustiveness, and
  `assert_properties()`.
- The comprehensive existing `leader_election.rs` and `unclean_recovery.rs` unit
  tests must still pass — confirming the `failover_one` extraction is
  behavior-preserving.
- Runs under the memory watchdog while tuning; committed tests are self-bounded
  and safe for unguarded CI.

## Out of scope (future slices)

- Reassignment (KIP-455), unclean recovery ELR (KIP-966 ELR — unimplemented).
- Operator-triggered elections (KIP-460 preferred / shutdown drain).
- The async URM orchestration (polling, dedup, submission).
- Multi-partition cluster-level failover interactions.
