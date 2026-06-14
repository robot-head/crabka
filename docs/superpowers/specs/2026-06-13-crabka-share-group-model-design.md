# Crabka share-group acquisition model — stateright design

**Date:** 2026-06-13
**Status:** Approved (design); plan + implementation to follow
**Workstream:** A (stateright correctness models) — next slice after the merged raft consensus model
**Predecessor specs:**
- `2026-06-13-crabka-stateright-consensus-deflake-design.md` (raft model + shared infra; merged #511)
- `2026-06-13-crabka-share-group-deflake-design.md` (share/group sleep-test de-flake; #513)

## Goal

Model-check the pure KIP-932 share-partition acquisition core
(`AcquisitionState`, `crates/broker/src/share_partition/state.rs`) with
[stateright](https://github.com/stateright/stateright), exhaustively exploring
every interleaving of consumer operations (acquire / acknowledge / renew /
expire-locks), production, time advance, and leader-failover reload, and
asserting the share-group delivery-safety invariants can never be violated.

This is the **wrap-real** counterpart for share groups of what the merged raft
model did for consensus: the model drives the *real production methods* — it
verifies the actual code, not a reimplementation.

## Background: what `AcquisitionState` is

`AcquisitionState` is the deterministic, I/O-free state machine the
share-partition leader drives. It tracks per-offset delivery state over the live
window `[start_offset, end_offset)` (SPSO..SPEO) as a `Vec<InFlightBatch>` of
contiguous runs, each carrying `(first_offset, last_offset, state,
delivery_count, acquired_by, lock_deadline)`. Delivery state is one of
`Available | Acquired | Acknowledged | Archived`.

Production methods (these become the model's actions):

| Method | Effect |
| --- | --- |
| `materialize(hwm, max_inflight)` | Extend the window with freshly produced records (appends one `Available` batch up to `min(hwm-1, end+max_inflight-1)`), advancing `end_offset`. Only fires when no `Available` records remain and `end < hwm`. |
| `acquire(member, max_records, _bytes, now, lock_dur, max_attempts)` | Hand out `Available` records to `member`: poison pills (`delivery_count >= max_attempts`) are `Archived` and SPSO advances past them; others go `Acquired` with `delivery_count += 1`, `acquired_by`/`lock_deadline` set; splits a run that exceeds `max_records`. |
| `acknowledge(member, first, last, ack, _now)` | Range must be wholly `Acquired` by `member` (else `Err(INVALID_RECORD_STATE)`). `Accept → Acknowledged`, `Release → Available` (delivery_count retained for redelivery), `Reject`/`Gap → Archived`. Advances SPSO over any new terminal prefix. |
| `renew(member, first, last, now, lock_dur)` | Range must be wholly `Acquired` by `member`. Resets each covered `lock_deadline` to `now + lock_dur`; state/owner/count preserved; SPSO **not** advanced. |
| `expire_locks(now)` | Any `Acquired` batch with `now >= lock_deadline` reverts to `Available` (owner/lock cleared, delivery_count retained). |
| `to_persist_batches()` / `load_from(...)` | Persistence projection / rehydrate. On reload, persisted `Acquired` maps to `Available` (locks do not survive a leader change), SPSO and `delivery_complete_count` are restored. |

Internal invariants the machine maintains (and which the model verifies hold
under *all* interleavings): the batch list stays sorted, contiguous, and
gap-free over `[start_offset, end_offset)`; SPSO advances only over terminal
prefixes; `coalesce` merges same-state neighbors.

## Why this is the right next slice

- We just de-flaked the share/group integration tests (#513) — the share
  subsystem is fresh, and a model complements those tests by proving the *core*
  is correct independent of broker plumbing.
- It advances Workstream A (correctness models) past raft into KIP-932.
- `AcquisitionState` is pure, small, and deterministic — a clean wrap-real
  target with rich, checkable safety invariants.

## What this is NOT

- **No `LinearizabilityTester`.** A share partition is a delivery-state machine,
  not a read/write register; register-linearizability is already covered by the
  merged raft `linearizable` config. The relevant correctness here is the
  delivery-safety invariants below. The serial order of operations is provided
  for free by the single-owner `Mutex` on the live state — the model verifies
  that *every* such serial execution respects the invariants.
- **No broker / actor / network modeling.** The model is the single pure
  `AcquisitionState`; concurrency is the interleaving of atomic operations, not
  a distributed message system.
- **No backwards-compat shims** (greenfield project; see CLAUDE.md). Schema/enum
  changes are made directly.

## Modeling decisions

### 1. Time and `Instant`

`acquire` / `renew` / `expire_locks` take `now: Instant` and store
`lock_deadline: Instant`; lock expiry depends on the *ordering* of `now` vs
`deadline`, so renew (extend lock) and expire (release lock) are only meaningful
with a real clock. `std::time::Instant` is `Eq + Ord + Hash`, so it is safe to
keep inside a hashed model state **provided the set of instants is finite**.

Approach (timeouts-as-actions extended to a tiny ordered clock):

- The model struct holds one base `t0: Instant` captured once at construction
  (`Instant::now()` — legal inside a normal `#[test]`).
- The model state carries `clock: u8` (`0..=MAX_TICK`).
- A `LOCK: Duration` constant is the only lock duration used.
- Time-sensitive ops use `now = t0 + LOCK * clock`. Deadlines are therefore
  drawn from `{t0 + LOCK*(c+1)}` for `c in 0..=MAX_TICK` — a finite set, so the
  state space stays finite and hashing is deterministic within a run. (Across
  runs `t0` differs but every state's *relative* structure — and thus the state
  count and all property results — is identical.)
- A `Tick` action advances `clock` (bounded by `MAX_TICK`). A lock acquired at
  `clock = c` has deadline `t0 + LOCK*(c+1)`, so it expires once `clock >= c+1`;
  `renew` at `clock = c'` pushes the deadline to `t0 + LOCK*(c'+1)`. This
  exercises both "lock expires" and "renew beats expiry."

### 2. Location and observability

The model lives **inside the broker crate** as a `#[cfg(test)]` descendant
module of `state`:

```rust
// at the bottom of crates/broker/src/share_partition/state.rs
#[cfg(test)]
#[path = "state_model.rs"]
mod state_model;
```

Because `state::state_model` is a *descendant* of the module that declares
`AcquisitionState` and the private `InFlightBatch`, it reads `sm.batches`,
`b.acquired_by`, `b.state`, `b.lock_deadline`, `delivery_complete_count`, etc.
**directly via private access** — exactly how the existing `mod tests` at
`state.rs:540` already does (`delivery_complete_count()`, `count_acquired_batches()`).

Consequences:
- **Zero new production observability surface** — no `BatchView` accessor, no
  `test-helpers` feature gating, no widening of `pub`.
- Runs as a fast single `--lib` test binary
  (`cargo test -p crabka-broker --lib state::state_model`).
- The model file (`state_model.rs`) is `#[cfg(test)]`, so it is excluded from
  normal/published builds.

The only production change is adding derives (see §Production changes).

### 3. Transition invariants vs ghost state (OOM discipline)

Single-state invariants (e.g. window integrity) are checked as stateright
`Property::always` predicates. Invariants that compare a state to its
predecessor (monotonicity, durability) are checked **imperatively inside
`next_state`** — the function computes the child from the parent, so it has both
in hand and can `assert!` (panic with a descriptive message) on violation.

This deliberately keeps predecessor/history data **out of the hashed state**.
Storing path-history (e.g. a per-offset "max delivery_count seen" map, or a
`prev_spso` ghost) would make two structurally-identical machine states reached
via different paths hash as *distinct* states, multiplying — potentially
exploding — the state count. That is the direct lesson from the Phase-1 raft
OOM (`feedback_bound_model_checkers.md`): the model state must hold only the
genuine machine state plus the small finite clock/hwm, nothing path-dependent.

Tradeoff accepted: a transition-assert failure surfaces as a panic with the
invariant name (not a minimal stateright counterexample trace). These
invariants are *expected to hold*; the model's job is to gain confidence. If one
ever fires, we add a bounded ghost locally to extract the trace.

## Model specification

### State

```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct ShareState {
    sm: AcquisitionState, // the real production machine (wrapped)
    clock: u8,            // logical time, 0..=MAX_TICK
    hwm: i64,             // produced high-watermark, start_offset..=MAX_OFFSET
}
```

### Config (held in the model struct, not the state)

```rust
struct ShareModel {
    t0: Instant,
    members: u8,        // = 2  (m0, m1)
    max_offset: i64,    // hwm cap (window size)
    max_tick: u8,
    max_attempts: i16,
    max_inflight: i32,
    allow_reload: bool, // failover config only
}
```

### Actions

```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum ShareAction {
    Produce,    // raise the produced high-watermark by one record
    Materialize, // leader pulls produced-but-unmaterialized records into the window
    Acquire { member: u8, max_records: i32 }, // max_records in {1, i32::MAX (= all available)}
    Acknowledge { member: u8, first: i64, last: i64, ack: AckType }, // ack in {Accept, Release, Reject}
    Renew { member: u8, first: i64, last: i64 },
    ExpireLocks,
    Tick,
    Reload, // only generated when allow_reload
}
```

`Produce` and `Materialize` are kept separate (rather than producing-then-
materializing in one step) so the high-watermark can run ahead of the
materialized window — only then does a single `materialize` pull a *multi-record*
batch (up to `max_inflight`), which is what exercises the in-flight cap and
multi-record acquire/split paths. Folding them would only ever materialize one
record at a time.

`actions(state)` enumeration:

- `Produce` — when `hwm < max_offset`.
- `Materialize` — when `end_offset < hwm` and no `Available` batch remains (the
  leader pulls lazily; `materialize` itself no-ops while Available records are
  in flight).
- `Acquire` — for each member, with `max_records in {1, i32::MAX}` (one record
  vs all available), when at least one `Available` batch exists.
- `Acknowledge` / `Renew` — **data-dependent**, like raft's fetch actions:
  enumerate the current `Acquired` runs *per owner*; for each maximal run offer
  the full range and one split (first half). `ack in {Accept, Release, Reject}`.
  `Gap` is omitted — it shares the `Reject | Gap` match arm verbatim
  (`Archived`), so it is behaviorally identical and adds no coverage.
- `ExpireLocks` — when any `Acquired` batch exists.
- `Tick` — when `clock < max_tick`.
- `Reload` — when `allow_reload` and the window is non-empty.

`next_state(state, action)`:
1. Clone `state`.
2. Apply the corresponding **real** method to `new.sm` (or bump `clock`/`hwm`),
   using `now = t0 + LOCK * new.clock` where time is needed.
3. Run the **transition-level asserts** comparing `state` (parent) to `new`
   (child); panic on violation.
4. Return `new`.

`Reload` body:
```rust
let (start, dcc, batches) = new.sm.to_persist_batches();
let mut fresh = AcquisitionState::new(start);
fresh.load_from(start, new.sm.state_epoch, new.sm.leader_epoch, dcc, &batches);
new.sm = fresh; // Acquired -> Available, locks dropped
```

### Properties

**State-level — `Property::always`:**

- `window_integrity` — batches sorted by `first_offset`; adjacent
  (`b[i].last + 1 == b[i+1].first`); first batch starts at `start_offset` and
  last ends at `end_offset - 1` when non-empty (full, gap-free, non-overlapping
  cover of `[start_offset, end_offset)`); `start_offset <= end_offset`.
- `mutual_exclusion` — no offset is `Acquired` by two members (structurally: no
  two `Acquired` batches overlap, which holds by construction, *and* every
  `Acquired` batch has `Some(acquired_by)`).
- `lock_consistency` — `Acquired` ⇒ `acquired_by.is_some() &&
  lock_deadline.is_some()`; every non-`Acquired` batch ⇒ `acquired_by.is_none()
  && lock_deadline.is_none()`.
- `delivery_count_bounded` — every batch `delivery_count <= max_attempts`
  (poison pills are archived *at* the limit, never handed out beyond it).
- `spso_in_range` — `0 <= start_offset <= end_offset <= max_offset`.

**Transition-level — `assert!` in `next_state` (parent → child):**

- `spso_monotonic` — `child.sm.start_offset >= parent.sm.start_offset`.
- `dcc_monotonic` — `child.sm.delivery_complete_count >= parent.sm.delivery_complete_count`.
- `delivery_count_monotonic` — for every offset live in both parent and child,
  `child_dc(offset) >= parent_dc(offset)` (release / expire / reload retain the
  count).
- `acknowledged_is_terminal` — every offset that was `Acknowledged` in the
  parent is, in the child, either still `Acknowledged` or has been dropped below
  the (non-decreasing) SPSO. It is **never** observed as `Available`/`Acquired`
  again. This is the key durability guarantee, and the one `Reload` stresses
  (an accepted record must survive leader failover).

**Non-vacuity — `Property::sometimes`** (proves the model is not vacuously
stuck and every terminal / redelivery transition is reachable). All four are
robustly observable from a *single* state (a non-prefix terminal batch survives
in the window because an earlier non-terminal offset blocks `advance_spso`):

- `can_advance_spso` — reach a state with `start_offset > 0`.
- `can_acknowledge` — reach a state with some batch in `Acknowledged` (proves
  the `Accept` path; distinct from Archive, which `delivery_complete_count`
  alone cannot distinguish).
- `can_archive` — reach a state with some batch in `Archived` (proves the
  `Reject`/poison-pill path). Together with `can_redeliver` below, this also
  witnesses the poison-pill path: a record whose `delivery_count` reached
  `max_attempts` and was then archived.
- `can_redeliver` — reach a state with some batch `delivery_count >= 2` (proves
  release/expire redelivery and delivery-count retention).

### Bounds (`within_boundary`) — OOM safety

`within_boundary` bounds only the dimensions that are **unbounded by design**
(so the model is finite) — it must *not* prune the dimensions whose
boundedness is a property we are trying to verify, or a real violation would be
silently dropped before the property runs. So it rejects any state exceeding:

- `clock <= max_tick`
- `hwm <= max_offset`
- `end_offset <= max_offset`
- batch count `<= 2 * max_offset` (loose structural cap)

It does **not** bound `delivery_count` — that is bounded *by the code*
(`<= max_attempts`), which is exactly what the `delivery_count_bounded`
`always` property verifies; bounding it here would mask the violation. A bug
producing unbounded `delivery_count` is instead caught by that property, and its
state-space blast radius is contained by the structural caps above plus the hard
`target_state_count` backstop and the memory watchdog (below).

Plus, in each test: a hard `target_state_count` backstop (~200_000) and a
`timeout`, and **every checker run is executed under the PowerShell memory
watchdog** (`Start-Process`, poll `WorkingSet64` every ~0.6s, `Stop-Process` if
`>3GB` or `>150s`) per the standing OOM rule. Each test asserts the discovered
state count is below the cap (proving the run was bounded and exhaustive, not
truncated).

Rough state estimate for `max_offset=3, members=2, max_tick=3, max_attempts=2`:
each of ≤3 offsets is in one of ~5 delivery states × ≤3 delivery counts × owner,
times `clock`(4) × `hwm`(4) ⇒ well under 100k. Verified empirically before
declaring exhaustive; bounds tightened if the count is larger than expected.

### Configurations

1. **`share_concurrency`** — full action set **minus** `Reload`;
   `members=2, max_offset=3, max_tick=3, max_attempts=2`. Run twice: once with
   `max_inflight=3` (window fills in one shot) and once with `max_inflight=1`
   (exercises drain-then-rematerialize via `Produce`). BFS exhaustive; all
   state-level + transition-level + non-vacuity properties.

2. **`share_failover`** — adds `Reload`; smaller window
   (`max_offset=2, max_tick=2, max_attempts=2, max_inflight=2`). Focus: the
   `acknowledged_is_terminal` durability invariant and `spso`/`dcc`
   monotonicity across crash-recovery. BFS exhaustive.

## Production changes

Pure, behavior-preserving derive additions (raft precedent: commit `fecba097`
added `Eq`/`Hash` to consensus types for the raft model):

- `AcquisitionState`: `#[derive(Debug)]` → add `Clone, PartialEq, Eq, Hash`.
- `InFlightBatch`: `#[derive(Debug, Clone)]` → add `PartialEq, Eq, Hash`.
- `RecordState`: add `Hash` (already `Debug, Clone, Copy, PartialEq, Eq`).

No new `pub` items, no `test-helpers` surface, no logic changes.

## File structure

| File | Responsibility |
| --- | --- |
| `crates/broker/src/share_partition/state.rs` | (modify) add the three derive additions; add `#[cfg(test)] #[path="state_model.rs"] mod state_model;` |
| `crates/broker/src/share_partition/state_model.rs` | (create) the entire model: `ShareState`, `ShareModel`, `ShareAction`, the `Model` impl (`init_states`/`actions`/`next_state`/`properties`/`within_boundary`), transition-assert helpers, and the `#[test]` config functions running each checker under the bounds. |

If `state_model.rs` grows past a comfortable single-file size, split into
`state_model/{mod.rs, props.rs, actions.rs}` via `#[path="state_model/mod.rs"]`.

## Testing strategy

- The model *is* the test. Each config is a `#[test]` that builds the bounded
  `ShareModel`, spawns a BFS checker with `target_state_count` + `timeout`
  backstops, joins, and `assert_properties()`.
- Each test additionally asserts the unique-state count is below the cap
  (exhaustive, not truncated) and that the `sometimes` properties were
  witnessed (model not vacuous).
- All runs performed under the memory watchdog; if any config's state count is
  uncomfortable, tighten `within_boundary` before landing.
- Spot-confirm wrap-real fidelity: the existing `mod tests` unit tests for
  `AcquisitionState` continue to pass unchanged (the derives must not alter
  behavior).

## Out of scope (future slices)

- ISR / replication `ReplicaState` model.
- Dynamic-voters (KIP-853) consensus-membership model.
- Share-group *coordinator*-level concurrency (multiple partitions / group
  membership churn) — this slice is the single-partition acquisition core only.
- De-flaking the remaining sleep-based tests in non-broker crates.
