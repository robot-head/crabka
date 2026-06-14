# Crabka ISR / replica-state model — stateright design

**Date:** 2026-06-13
**Status:** Approved (design); plan + implementation to follow
**Workstream:** A (stateright correctness models) — partition-replication safety, complementing the merged raft consensus model
**Predecessor specs:**
- `2026-06-13-crabka-stateright-consensus-deflake-design.md` (raft model; merged #511)
- `2026-06-13-crabka-share-group-model-design.md` (share-partition acquisition model; #514)

## Goal

Model-check the pure leader-side replication core (`ReplicaState`,
`crates/broker/src/replica_state.rs`) with
[stateright](https://github.com/stateright/stateright), exhaustively exploring
every interleaving of leader appends, follower fetches, and ISR shrink/expand,
and proving the partition-replication safety invariants — above all
**no-committed-data-loss**: the leader never advances the high-watermark past a
record until every in-sync replica holds it.

This is the **wrap-real** partition-replication counterpart of the merged raft
consensus model. Raft proves the *metadata log* is safe; this proves the
*partition ISR/high-watermark* core is safe. Together they cover both pillars of
Kafka durability.

## Background: what `ReplicaState` is

`ReplicaState` lives on the partition leader and is a small, deterministic core
(once time is injected — see Production changes). It tracks:

- `isr: HashSet<NodeId>` — the committed in-sync replica set (includes the leader).
- `per_follower: HashMap<NodeId, FollowerStats>` — each non-leader replica's
  last-known LEO (log-end offset) plus lag timestamps.
- `hw: i64` — the high-watermark (one past the last committed offset).
- `current_leader_epoch: i32` — stamped on AlterPartition proposals (KIP-903).

Methods that become the model's actions:

| Method | Effect |
| --- | --- |
| `install_isr(isr, replicas, leader, now)` | Set ISR membership; seed each non-leader ISR member's `per_follower` to LEO 0 (`or_insert`, so existing progress is preserved); drop `per_follower` entries for replicas no longer in the replica set. Does **not** recompute `hw`. |
| `update_follower_leo(follower, follower_leo, leader_leo, now) -> hw` | Record a follower fetch: clamp the reported LEO to `leader_leo`, update stats, recompute and return `hw`. For an ISR follower this gates `hw`; for a non-ISR (catching-up) follower it updates stats and recomputes `hw` via the leader-append path (the follower does not gate `hw`). |
| `recompute_hw_for_leader_append(leader_leo) -> hw` | Recompute `hw` after the leader appends (no time; no `Instant`). |
| `compute_hw(leader_leo)` (private) | `hw = min(leader_leo, min over non-leader ISR members' LEO)`; `= leader_leo` when ISR is empty. An ISR member with no `per_follower` entry (e.g. the leader itself) does not gate `hw`. |

The HWM safety rule lives entirely in `compute_hw`. The lag timestamps
(`last_fetch`, `last_caught_up`) feed only the *time-based* `isr_maintenance`
shrink/expand task — which this model represents as explicit `install_isr`
actions, not by modeling wall-clock time. So the timestamps are irrelevant to
the safety invariants here.

## Why this is the right next slice

- It is the partition-replication safety complement to the merged raft model —
  the other half of Kafka's durability story.
- `ReplicaState` is a cleanly separable, deterministic core (after time
  injection) with rich, checkable safety invariants and existing unit tests.
- Highest bug-finding potential among the candidates surveyed: HWM/ISR is where
  real committed-data-loss bugs live (KIP-98 / KIP-320 / KIP-903 / KIP-966).
- KIP-853 dynamic voters was the other candidate but is **not viable yet**: the
  `QuorumStateMachine` core does not apply runtime voter changes
  (`change_membership` is a stub returning `Unsupported`), so a model would test
  unbuilt behavior. Deferred until that feature lands.

## What this is NOT

- **No leader failover / election / epoch fencing.** A leader change resets
  `per_follower` on `Partition` atomic fields *outside* `ReplicaState`
  (`partition.rs install_leader_change`) and depends on the controller; modeling
  it needs the `Partition` wrapper + controller and is out of scope. This slice
  models the single-leader HWM/ISR core. The no-data-loss invariant it proves
  (`hw ≤ every ISR member's LEO`) is exactly what *guarantees* a safe failover to
  any ISR member, so the core invariant is the meaningful one to establish first.
- **No `isr_maintenance` time-based decisions.** ISR shrink/expand is driven as
  explicit `install_isr` actions whose preconditions encode the controller's
  rules (notably: only admit caught-up replicas). `current_leader_epoch` is held
  constant (epoch fencing is controller-side, out of scope).
- **No replica-set reassignment churn.** The replica set is fixed; only ISR
  membership within it changes. (Reassignment is a future slice.)
- **No `LinearizabilityTester`.** Replication safety here is the invariant set
  below, not register linearizability.

## Production changes

### 1. Inject `now: Instant` (approach A — approved)

`install_isr` and `update_follower_leo` call `Instant::now()` internally
(`replica_state.rs:56,79`) and store wall-clock `Instant`s in `FollowerStats` —
non-deterministic and unbounded, fatal to a model checker. (Two existing unit
tests, `replica_state.rs:258,274`, even use `std::thread::sleep` to advance time.)

Refactor both methods to accept `now: Instant`, replacing the internal
`Instant::now()` calls. This makes the core deterministic, matches the
established codebase pattern (raft `on_event(.., now)`, share `acquire(.., now)`),
and lets those two unit tests pass explicit instants instead of sleeping
(de-flaking them — a Workstream-B win). `recompute_hw_for_leader_append` has no
`Instant` and is unchanged.

Caller updates (pass `Instant::now()` at the I/O boundary; signatures of the
`Partition` wrappers do not change):
- `partition.rs:453` — `st.install_isr(isr, replicas, leader)` → add `Instant::now()`.
- `handlers/fetch.rs:390` — `st.update_follower_leo(...)` → add `Instant::now()`.
- Test callers: `partition_writer.rs:632,765`, the `replica_state.rs` unit tests
  (pass a fixed instant), and the two sleep-based tests (pass two explicit,
  ordered instants instead of `thread::sleep`).

(The other `install_isr` call sites — `replicator_supervisor.rs:311`,
`handlers/create_partitions.rs:290`, `handlers/create_topics.rs:278` — call the
`Partition::install_isr` *wrapper*, not the core, and are unaffected.)

### 2. Derives

`#[derive(Clone, Debug)]` on `ReplicaState` (so the model state can clone it and
the model's `Debug` derives; `FollowerStats` already derives `Debug, Clone,
Copy`). `Hash`/`Eq` are **not** derived — `HashMap`/`HashSet` aren't `Hash`; the
model state hand-implements them over a normalized projection (see below).

### 3. Model module declaration

At the end of `replica_state.rs`:
```rust
#[cfg(test)]
#[path = "replica_state_model.rs"]
mod replica_state_model;
```
As a `#[cfg(test)]` descendant of `replica_state`, the model reads the
`pub(crate)` fields (`isr`, `per_follower`, `hw`, `current_leader_epoch`,
`FollowerStats::leo`) directly. No new `pub` surface. (`stateright` is already a
broker dev-dependency from the share-group slice.)

## Modeling decisions

### Hashable projection (the state cannot be the raw `ReplicaState`)

`ReplicaState` holds `HashMap`/`HashSet`, which don't implement `Hash`, and the
timestamps are non-deterministic. The model state therefore carries the *real*
`ReplicaState` but hand-implements `PartialEq`/`Eq`/`Hash` over a normalized
projection that ignores map ordering and timestamps:

```
project(state) = (
    sorted Vec<NodeId> of isr,
    sorted Vec<(NodeId, i64)> of (follower, leo),
    hw,
    current_leader_epoch,
    leader_leo,
)
```

Because `now` is injected (approach A) and the model passes a **constant**
`T0`, the stored timestamps are constant anyway; the projection drops them
regardless. `next_state` is fully deterministic.

### Transition invariants as `next_state` asserts

As in the share-group model, the monotonicity invariant (`hw` never regresses)
is checked by an imperative `assert!` in `next_state` comparing parent to child,
keeping predecessor data out of the fingerprinted state (the Phase-1 OOM
lesson). Single-state invariants are `Property::always`.

### Controller rules encoded as action preconditions (the crux)

`ReplicaState` alone does not enforce *which* ISR changes are legal — that is the
controller's / `isr_maintenance`'s job. The model encodes those rules as
`install_isr` action preconditions, so it checks: **given the controller follows
its rules, does `ReplicaState` maintain HWM safety?** The essential rule:

> **ISR expansion only admits caught-up replicas.** A follower may be *added* to
> the ISR only if its current `per_follower.leo >= hw`.

Without this guard the model would generate a physically-impossible action
(admitting a straggler at LEO 0 while `hw` is high) and report a *false*
no-data-loss violation. Shrinking (removing a non-leader follower) is always
allowed. The leader is always kept in the ISR.

## Model specification

### State

```rust
struct IsrState {
    rs: ReplicaState, // the real production core
    leader_leo: i64,  // the leader's own log-end offset
}
// Clone + Debug derived; PartialEq/Eq/Hash hand-implemented over project().
```

**Initial state:** `ReplicaState::new()` then `install_isr(replicas, replicas,
leader, t0)` — a fresh leader with the full replica set in the ISR, each
non-leader follower seeded at LEO 0, `hw = 0`, `leader_leo = 0`.

### Config (held in the model struct, not the state)

```rust
struct IsrModel {
    t0: Instant,        // constant injected `now`
    replicas: Vec<NodeId>, // = [1, 2, 3]; replicas[0] = 1 is the fixed leader
    max_offset: i64,    // leader_leo / LEO cap
    test_overshoot: bool, // also offer follower LEO reports above leader_leo (clamp test)
}
```

### Actions

```rust
enum IsrAction {
    LeaderAppend,                               // leader_leo += 1, recompute hw
    FollowerFetch { follower: NodeId, leo: i64 }, // update_follower_leo(follower, leo, leader_leo)
    InstallIsr { isr: Vec<NodeId> },            // install_isr(isr, replicas, leader)
}
```

`actions(state)` enumeration (leader = `replicas[0]`, followers = the rest):

- `LeaderAppend` — when `leader_leo < max_offset`.
- `FollowerFetch` — for each follower `f`, for each `leo` in
  `{cur_leo(f) + 1, leader_leo}` intersected with `[cur_leo(f), leader_leo]`
  (monotonic: never below the follower's current LEO; this is what keeps `hw`
  monotonic — a real follower's reported LEO never goes backwards). When
  `test_overshoot`, additionally offer `leo = leader_leo + 1` to exercise the
  clamp. Offered for ISR *and* non-ISR (catching-up) followers.
- `InstallIsr { isr: S }` — for each `S ⊆ replicas` with `leader ∈ S` and
  `S != current isr`, allowed iff every follower in `S \ current_isr` (the
  members being *added*) has `per_follower[f].leo >= hw`. (Removals are
  unconditional.)

`next_state(state, action)`:
1. Clone `state`.
2. Apply the corresponding **real** method to `new.rs` (or bump `leader_leo`),
   passing `t0` where `now` is required.
3. Assert `new.rs.hw >= state.rs.hw` (HWM monotonicity); panic on regression.
4. Return `new`. (No-op transitions — e.g. a `FollowerFetch` that changes
   nothing — may return `None` to avoid redundant states.)

### Properties

**State-level — `Property::always`:**

- `hw_within_leader` — `rs.hw <= leader_leo`.
- `no_data_loss` — for every `f` in `rs.isr` with `f != leader`, `per_follower`
  has an entry for `f` **and** `per_follower[f].leo >= rs.hw`. A *missing* entry
  for an ISR member counts as a violation: `compute_hw` skips entryless members,
  so an ISR member with unknown progress must never coexist with an advanced
  `hw`. (Every committed record is held by every ISR member — the
  failover-safety guarantee.)
- `leo_clamped` — every `per_follower[f].leo <= leader_leo`.
- `hw_nonneg` — `rs.hw >= 0`.
- `leader_in_isr` — `leader ∈ rs.isr` (the model maintains it; `install_isr`
  preserves it).

**Transition-level — `assert!` in `next_state`:**

- `hw_monotonic` — `child.rs.hw >= parent.rs.hw`.

**Non-vacuity — `Property::sometimes`:**

- `can_advance_hw` — reach `rs.hw > 0`.
- `can_reach_leader_leo` — reach `rs.hw == leader_leo` with `leader_leo > 0`
  (fully replicated).
- `can_pin_below_leader` — reach `0 < rs.hw < leader_leo` (a slow ISR follower
  pinning `hw` below the leader's log).
- `can_shrink_isr` — reach a state where some non-leader replica is outside the
  ISR but still tracked in `per_follower` (a catching-up replica).

### Bounds (`within_boundary`) — OOM safety

Bounds only the design-unbounded dimensions:

- `leader_leo <= max_offset`
- `rs.hw <= max_offset`
- every `per_follower[f].leo <= max_offset` (when `test_overshoot` is off; the
  clamp keeps LEOs `<= leader_leo <= max_offset` regardless)

Plus, per the standing OOM rule: a hard `target_state_count` (~200k) + `timeout`
backstop, the `run` harness asserts the search was exhaustive (not cap- or
depth-truncated), and **every checker run is executed under the PowerShell
memory watchdog** while bounds are tuned. Estimated space for `replicas=[1,2,3]`,
`max_offset=3` is tiny (~thousands of states: `leo2`×`leo3`×`isr`×`hw`×`leader_leo`
≈ `4×4×4×4×4`), comparable to the share-group model. Verified empirically.

### Configurations

1. **`isr_safety`** — `replicas=[1,2,3]`, `max_offset=3`, `test_overshoot=false`.
   BFS exhaustive; all state-level + transition + non-vacuity properties.
2. **`isr_overshoot`** — same but `test_overshoot=true` (followers may report
   LEOs above `leader_leo`), focusing the `leo_clamped` invariant and the
   defensive clamp path.

(`max_offset` may be scaled empirically in a tuning step if the state count
stays well under the cap, exactly as the share-group model scaled to 3.)

## File structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/replica_state.rs` | (modify) inject `now: Instant` into `install_isr`/`update_follower_leo` + update its unit tests (incl. de-flaking the two `thread::sleep` tests); add `#[derive(Clone, Debug)]` to `ReplicaState`; declare `#[cfg(test)] #[path="replica_state_model.rs"] mod replica_state_model;`. |
| `crates/broker/src/partition.rs` | (modify) `partition.rs:453` pass `Instant::now()` to `install_isr`; update the partition-level test callers as needed. |
| `crates/broker/src/handlers/fetch.rs` | (modify) `fetch.rs:390` pass `Instant::now()` to `update_follower_leo`. |
| `crates/broker/src/partition_writer.rs` | (modify) test call sites `:632,:765` pass `Instant::now()`. |
| `crates/broker/src/replica_state_model.rs` | (create) the model: `IsrState`/`IsrModel`/`IsrAction`, the projection-based `Hash`/`Eq`, the `Model` impl, the watchdog-friendly `run` harness, and the `#[test]` configs. |

If `replica_state_model.rs` grows large, split via `#[path="replica_state_model/mod.rs"]`.

## Testing strategy

- Each config is a `#[test]` that builds the bounded `IsrModel`, spawns a BFS
  checker with `target_state_count` + `timeout`, joins, asserts the search was
  exhaustive (state/depth below caps), and `assert_properties()`.
- Runs performed under the memory watchdog while tuning; the committed test is
  self-bounded (caps + `within_boundary`) and safe to run unguarded in CI.
- The existing `replica_state.rs` unit tests (now passing explicit instants)
  continue to pass — confirming the `now` injection is behavior-preserving.

## Out of scope (future slices)

- Leader failover / election / `current_leader_epoch` fencing (needs the
  `Partition` wrapper + controller).
- Replica-set reassignment churn (KIP-455).
- `isr_maintenance` time-based ISR shrink/expand decisions.
- Unclean recovery (KIP-966), dynamic voters (KIP-853 — blocked on the core
  implementing membership changes).
