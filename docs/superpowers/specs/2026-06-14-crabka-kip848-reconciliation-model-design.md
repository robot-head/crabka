# KIP-848 Consumer-Group Reconciliation Safety Model — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright models of pure cores)
**Predecessors:** raft consensus, share-group `AcquisitionState` (#514), ISR/`ReplicaState` (#515),
failover/unclean-recovery (#516), reassignment/KIP-455 (#520)

## Goal

Build an exhaustive `stateright` model of the next-generation (KIP-848) consumer-group
reconciliation state machine that proves its headline safety property — **no two members ever
simultaneously own the same partition during a rebalance** — under every interleaving of member
joins, leaves, target recomputation, and client-side revoke/add, by driving the **real** broker
reconciliation core.

This completes the leader/ownership-change-safety arc: raft consensus → ISR/HWM → failover →
reassignment → **consumer-group rebalance**.

## Background — what the code actually does

The next-gen coordinator lives in `crates/broker/src/coordinator/unified/`. The reconciliation
core is **pure and synchronous**; the only `.await` in the heartbeat path is the offsets-log append
that happens *after* all state mutation is finalized.

The pure decision flow on a `ConsumerGroupHeartbeat`:

```
handle_heartbeat (actor.rs:1015)        // async: pure phase, then flush_pending().await
  ├─ epoch validation                   // UNKNOWN_MEMBER_ID / STALE_MEMBER_EPOCH / FENCED_MEMBER_EPOCH
  ├─ update_member_state (actor.rs:1096)// member reports owned partitions; →Stable when pending drains
  │    └─ run_reconcile → reconcile_if_dirty (reconciler.rs:28)
  │         ├─ assignor.assign(...)      // UniformAssignor: deterministic target from membership+subscription
  │         ├─ group.bump_epoch()        // group_epoch += 1; dirty = true
  │         └─ group.install_target(...) // per member: compute_revoke_split(current, target)
  ├─ advance_member_epoch (consumer_state.rs:224)  // member_epoch ← group_epoch when target advanced
  └─ build_assignment_resp (actor.rs:1263)         // returns the member's FULL target assignment
```

Key data (`consumer_state.rs`):

- `GroupState { group_id, group_epoch, members, instance_to_member, target: TargetAssignment, dirty }`
- `MemberState { member_epoch, previous_member_epoch, assignment_state, assigned_partitions:
  HashMap<Uuid, Vec<i32>>, partitions_pending_revocation: HashMap<Uuid, Vec<i32>>, … }`
- `MemberAssignmentState ∈ { Stable, UnreleasedPartitions (unused), UnrevokedPartitions }`
  (`persistence_next_gen.rs:319`)

The revoke-before-assign mechanism (`install_target` → `compute_revoke_split`,
`consumer_state.rs:208`/`236`): on a new target, each member's currently-owned set splits into
`keep = current ∩ target` (stays in `assigned_partitions`) and `revoke = current − target` (moves to
`partitions_pending_revocation`); the member becomes `UnrevokedPartitions` until it heartbeats back
the reduced owned set, then returns to `Stable`.

### The point of interest

`build_assignment_resp` (`actor.rs:1263`) returns each member its **full target** assignment
immediately, with **no cross-member withholding gate** — there is no `CurrentAssignmentBuilder`
equivalent that withholds partition `P` from member `B` until member `A` has revoked it. The headline
KIP-848 safety guarantee lives exactly at this seam. Whether Crabka's simpler scheme preserves
disjoint ownership is the open question this model resolves. We treat the no-withholding observation
as a hypothesis to be confirmed empirically during implementation (the file is large), not as a
settled fact.

## Safety property and model philosophy

The no-double-ownership property has teeth only against a **faithful client environment** — a model
of correct Kafka consumer behavior. A real consumer trusts the coordinator: upon receiving an
assignment it revokes partitions no longer assigned and adds newly-assigned partitions, **without
checking whether any other consumer still holds them**. The cross-member safety must come entirely
from the coordinator. So the model supplies faithful clients as the environment and treats the real
coordinator core as the system under test.

### Properties

- **`always no_double_ownership`** (HEADLINE): for all members `m ≠ m'`,
  `client_owned[m] ∩ client_owned[m'] = ∅`.
- **`always ownership_entitled`**: `client_owned[m] ⊆ target[m] ∪ pending_revocation[m]` — a client
  holds only partitions it is targeted to own or has not yet revoked from a prior target.
- **`always member_epoch_monotonic`**: enforced as a `next_state` assertion (per member, the epoch
  never regresses across a step), not stored in fingerprinted state.
- **`sometimes handoff_witness`**: a reachable state where some partition's target owner differs from
  its current owner (proves handoffs actually occur — guards against vacuity).
- **`sometimes converged_witness`**: a reachable state where every `client_owned[m] == target[m]` and
  every member is `Stable` (proves the protocol can converge).

## Production refactor — the wrap-real seam

Following the established `failover_one` / `reassign_one` extraction pattern, extract the pure
synchronous decision core of `handle_heartbeat` into a function the model can drive directly:

```rust
/// Outcome of the pure heartbeat decision phase: the response to return and the
/// records the async caller must append to the offsets log.
pub(crate) struct HeartbeatStep {
    pub response: ConsumerGroupHeartbeatResponse,
    pub pending: PendingRecords,
}

/// The pure, synchronous heartbeat core: epoch validation, member upsert/leave,
/// update_member_state, run_reconcile, advance_member_epoch, and response build.
/// No `.await`, no I/O. The async `handle_heartbeat` calls this, then flushes
/// `pending` to the offsets log.
pub(crate) fn step_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    now: Instant,
) -> HeartbeatStep;
```

`handle_heartbeat` becomes: `let step = step_heartbeat(...); flush_pending(step.pending, …).await?;
Ok(step.response)`. The refactor is **behavior-preserving** and gated by the existing actor /
heartbeat test suite. It is the largest extraction of the series — `handle_heartbeat` has distinct
leave (`member_epoch == -1`), first-join (`member_epoch == 0`), and steady-state paths, each of which
currently interleaves its own `flush_pending().await`. The pure/IO boundary is clean (no `GroupState`
mutation occurs after the flush), so threading the pending records out is mechanical.

The model implements a tiny fake `MetadataProvider` returning bounded partition counts, and uses the
real `UniformAssignor`. No new production observability surface is added.

## The model

Wrap-real, consistent with the ISR and failover models: the stateright `State` is a **hashable
projection**; `next_state` reconstructs a real `GroupState`, drives the real `step_heartbeat`
(and the faithful-client actions), then reads results back into the projection.

### State (all fields hashable — `Vec`/`BTreeMap`/`BTreeSet`, sorted-canonical)

- Per member: `member_epoch: i32`, `assignment_state`, `assigned_partitions` (coordinator view),
  `pending_revocation`, `target`.
- `group_epoch: i32`, `dirty: bool`.
- **`client_owned: BTreeMap<MemberId, BTreeSet<(TopicId, Partition)>>`** — the authoritative ledger
  of what each consumer is actually consuming; the observable the headline invariant checks.

Time is a constant `Instant` (v1 excludes session-timeout eviction), so there is no clock in the
fingerprint — avoiding the state explosion that a numeric clock would cause (the Phase-1 / share-group
lesson).

### Actions

- `Join(m)` / `Leave(m)` — drive `step_heartbeat` with `member_epoch` `0` / `-1`. Membership change is
  the churn source: the assignor rebalances, producing handoffs. (v1 fixes every member's subscription
  to the single modeled topic — no subscription / regex / metadata churn yet.)
- `Heartbeat(m)` — member `m` reports `client_owned[m]` as `topic_partitions`; drives the real
  `step_heartbeat` (updates `assigned_partitions`, the `Stable` transition, reconcile, epoch advance);
  the model reads `m`'s new target out of the response.
- `ClientRevoke(m, tp)` — **faithful**: enabled iff `tp ∈ client_owned[m]` and `tp ∉ target[m]`;
  removes `tp` from `client_owned[m]`.
- `ClientAdd(m, tp)` — **faithful**: enabled iff `tp ∈ target[m]` and `tp ∉ client_owned[m]`; adds
  `tp` to `client_owned[m]`. **No cross-member check** — the consumer trusts the coordinator. This is
  what gives the headline property teeth.

### Bounding

- `recon_basic`: 2 members, 1 topic, 2 partitions.
- `recon_wide`: 3 members or 3 partitions (whichever stays tractable).
- `within_boundary`: `group_epoch ≤ MAX_EPOCH`; members drawn from a fixed small pool (rejoin allowed,
  epoch cap bounds the cycles).
- Mandatory checker fences on every run: `target_state_count` (hard cap, e.g. 200k), `target_max_depth`,
  `timeout` (≤ 120 s as `Duration::from_mins(2)`), executed under the host memory watchdog (3 GB / 150 s
  kill) while bounds are tuned. Configs are kept if exhaustive under ~100k states; the harness asserts
  no state/depth-cap truncation before asserting properties.

## Two outcomes — both valuable

Because the response returns the full target with no withholding gate, a shallow counterexample is
plausible (2 members, 2 partitions: `B` is handed `P1` and `ClientAdd(B, P1)` fires before `A` does
`ClientRevoke(P1)` → both own `P1`).

- **RED (counterexample):** a real KIP-848 conformance bug — Crabka omits the `CurrentAssignmentBuilder`
  withholding canonical Kafka uses to keep `P` out of `B`'s response until `A` revokes. The slice then
  extends to a coordinator fix (compute and return a withheld *current* assignment rather than the raw
  target) and a re-run of the model to GREEN. The fix shape mirrors the `failover_one` refactor: a pure
  function the model also covers.
- **GREEN:** the model *proves* Crabka's simpler scheme preserves disjoint ownership; we document
  precisely why (e.g. an invariant of the assignor + heartbeat round-trip that makes the premature
  target safe).

The no-withholding hypothesis is verified carefully during implementation before any bug is declared.

## File structure

- **Modify** `crates/broker/src/coordinator/unified/actor.rs` — extract `step_heartbeat` +
  `HeartbeatStep`; rewrite `handle_heartbeat` to call it then flush. Behavior-preserving.
- **Create** `crates/broker/src/coordinator/unified/reconciler_model.rs` — the model, wired as a
  `#[cfg(test)] #[path = "reconciler_model.rs"] mod reconciler_model;` descendant of a module in
  `unified/` so it reaches the `pub(crate)` core (`step_heartbeat`, `GroupState`, `reconcile_if_dirty`,
  `install_target`). Hashable-projection `State`; safety as `next_state` asserts; `Property::always` /
  `sometimes` for the rest; `run(model, label)` harness with the mandatory caps.
- Add `Clone` / `Eq` / `Hash` derives where needed (e.g. `MemberAssignmentState`) — no behavior change.

## Out of scope (YAGNI for v1)

- Session-timeout / `last_seen` eviction (would introduce a clock → state explosion). Membership change
  is modeled via explicit `Leave`, which exercises the same reconcile path.
- Subscription / regex / metadata churn (fixed single-topic subscription suffices to drive handoffs).
- Classic-protocol migration (upgrade/downgrade), share-group and streams reconcilers — separate cores.
- Persistence / offsets-log replay — the model drives the pure phase; the flush is out of band.

## Risks

- **Client-model fidelity:** an over-permissive `ClientAdd`/`ClientRevoke` could yield a false-positive
  violation. Mitigation: the actions are conservative and grounded in real consumer semantics
  (add-only-what's-assigned, revoke-before-add, trust-the-coordinator); the spec-compliance review
  scrutinizes the client model specifically.
- **Refactor blast radius:** `step_heartbeat` touches the 3588-line `actor.rs`. Mitigation:
  behavior-preserving extraction gated by the full existing actor/heartbeat test suite; no semantic
  change in the same commit.
- **State explosion:** multi-member + partitions + epochs is the largest space of the series.
  Mitigation: small fixed pools, constant time, epoch cap, `within_boundary`, mandatory watchdog +
  hard `target_state_count` cap.

## Success criteria

1. `step_heartbeat` extracted; all existing coordinator/heartbeat tests pass unchanged.
2. The model drives the real core and either proves `no_double_ownership` exhaustively across all
   configs (GREEN) or produces a minimal, human-legible counterexample trace (RED) — and, if RED, a
   coordinator fix lands and the re-run is GREEN.
3. All configs exhaustive (no cap truncation) under the memory watchdog; non-vacuity witnesses satisfied.
4. `cargo +nightly fmt` clean; clippy `-D warnings` clean; the broader broker suite unaffected.
