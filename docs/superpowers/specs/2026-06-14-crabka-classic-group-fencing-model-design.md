# Classic Group-Coordinator Membership/Fencing Model (KIP-345/62) — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright model of an existing state machine) + proptest.
**Predecessors:** raft, share-group, ISR, failover, reassignment, KIP-848 reconciliation (bug),
KIP-98 txn, data-plane (#524), KIP-534 compaction (bug, #528), fetch-HWM (#529), KIP-73 token-bucket
(bug, #531).

## Goal

Exhaustively verify the **classic consumer-group membership state machine** — KIP-345 static-member
fencing + the static-membership secondary index — with a wrap-real `stateright` model + a `proptest`,
across all interleavings of join (dynamic / static rejoin / fenced), leave, rebalance completion,
sync, and session-timeout expiry. The headline property is **static-index coherence** (the
`group.instance.id → member_id` index is a faithful, injective mirror of the members map), which
guards against a fencing-bypass (two live members claiming one instance id) and assignment loss.

This mirrors the ISR / share-group wins (drive real `&mut self` methods of a synchronous state
machine under exhaustive interleaving). Survey rank-3. Likely confirmation, with the
takeover-repoint / `remove_member`-guard / `joined_this_round`-rename orderings as genuine
coupled-structure invariants exhaustive interleaving catches.

## Background — the state machine

`Group` (`crates/broker/src/coordinator/unified/classic_state.rs:166-435`) is a synchronous,
I/O-free five-state machine (`Empty / PreparingRebalance / CompletingRebalance / Stable / Dead`) with:

- `members: HashMap<member_id, Member>` and the KIP-345 secondary index
  `static_members: HashMap<group_instance_id, member_id>`.
- `joined_this_round: HashSet<member_id>` (members that re-joined since the last
  `PreparingRebalance`; drives `all_members_joined_this_round` early-completion).
- `generation_id`, `leader_id`, `rebalance_from_empty`.
- Transitions: `add_member` (dynamic add / KIP-345 static rejoin-in-place / takeover),
  `remove_member` (clears the index entry **only if it still points at this member** — a takeover may
  have repointed it), `complete_rebalance` (pick leader = min member_id, bump generation,
  → `CompletingRebalance`), `install_assignments` (→ `Stable`), `expire_dead_members(now)` (drop
  **dynamic** members past `session_timeout` when `Stable`; **static members are never expired** —
  the KIP-345 guarantee), and `current_member_id_for_instance` (the index lookup).

The handler's KIP-345 fence (`classic_ops.rs:88-100`) is a pure pre-check: reject
`FENCED_INSTANCE_ID` iff the request's `group_instance_id` is pinned to a **different** live
`member_id` than the request's, else proceed to `add_member`.

Subtle coupled-structure orderings the model exercises:
- `add_member` static rejoin during `PreparingRebalance` **repoints** the index and moves the
  member's `joined_this_round` entry from the prior to the new `member_id` (the rename path).
- `remove_member` clears `static_members[iid]` only if it still equals the removed `member_id`.

## Model (`classic_state_model.rs`, `#[cfg(test)]` descendant of `classic_state`)

**Wrap-real:** the model holds a **real `Group`** and drives its real methods + the real fence
pre-check. Only production change: add `#[derive(Clone)]` to `Group` (its fields — `HashMap`,
`Instant`, `Bytes`, `String`, the `Copy` enums — are all `Clone`; behavior-preserving). The model's
`State = { g: Group, clock: i64 }` with a **manual `Hash`/`PartialEq`/`Eq`** over a canonical
projection: `(state, generation_id, leader_id, rebalance_from_empty, clock, sorted
members[(member_id, group_instance_id, assignment.is_some(), last_heartbeat)], sorted
static_members[(iid, member_id)], sorted joined_this_round)`. `Instant` is `Hash`, and the model
constructs every `last_heartbeat` deterministically as `EPOCH + clock*UNIT` (a fixed `OnceLock`
epoch), so the fingerprint is stable and finite.

**Actions** (bounded: members `{a,b,c}`, instances `{x,y}`):
- `JoinDynamic(mid)` — `add_member(Member::new(mid, …))` (no instance id).
- `JoinStatic(iid, mid)` — fence pre-check (`current_member_id_for_instance(iid)` is `Some(other)`,
  `other != mid` ⟹ FENCED, no-op); else `add_member(... .with_instance_id(Some(iid)))`. New members'
  `last_heartbeat = now`.
- `Heartbeat(mid)` — refresh `members[mid].last_heartbeat = now` (the classic heartbeat keeps a
  member alive past `session_timeout`).
- `Leave(mid)` — `remove_member(mid)`.
- `CompleteRebalance` — when `PreparingRebalance` and ≥1 member: `complete_rebalance("range")`.
- `Sync` — when `CompletingRebalance`: `install_assignments(members → some bytes)` (→ `Stable`).
- `ExpireTick` — advance `clock` by 1 (bounded); `expire_dead_members(EPOCH + clock*UNIT)`.

`session_timeout` is a small fixed number of `UNIT`s so expiry is reachable within the bounded clock.

**Safety asserts (per-transition + `Property::always`):**
- **index_coherence** (HEADLINE): for every `(iid → mid)` in `static_members`, `members[mid]` exists
  and `members[mid].group_instance_id == Some(iid)`; and every static member in `members` has a
  matching index entry. (Bidirectional faithful mirror; injective by map construction.)
- **single_owner_per_instance** (no fencing-bypass): no two distinct live members share a
  `group_instance_id`.
- **joined_this_round ⊆ members**: every id in `joined_this_round` is a current member (no stale id
  can wedge `all_members_joined_this_round`).
- **static_never_expired**: a member with `group_instance_id.is_some()` is never dropped by
  `expire_dead_members` (assert the returned dropped set contains no static member; the KIP-345
  assignment-preservation guarantee).
- **empty_iff_empty_state**: `members.is_empty() ⟺ state == Empty`.
- **generation_monotonic**: `generation_id` never decreases.
- **leader_in_members_when_set**: `leader_id`, when `Some`, is a current member.
- Non-vacuity (`sometimes`): a static rejoin occurs; a fence is rejected; a takeover/repoint happens
  during `PreparingRebalance`; a dynamic member is expired while a static one survives; a generation
  bump; the group reaches `Stable`.

**Bounds (watchdog-guarded):** small — `classic_basic` (2 members / 1 instance / short clock) and
`classic_wide` (3 members / 2 instances / longer clock), scaled while exhaustive under the host
memory watchdog. If the clock × `last_heartbeat` dimension explodes, project `last_heartbeat` to a
small ordinal and/or shrink the clock window (the share-group/compaction tuning techniques).

## proptest fuzz (`proptest` already a `crabka-broker` dev-dep)

Generate large-N random op sequences (the same actions, small alphabets, interleaved heartbeats +
expiry ticks) driving a real `Group`, asserting the same invariants after every step (index
coherence, single-owner, joined-subset, static-never-expired, empty⟺Empty, generation monotonic).

## Out of scope (YAGNI)

- The async RPC handlers' wire encoding / response shaping (the model drives the state-machine
  decision, not the `JoinGroupResponse` bytes); `select_protocol` metadata negotiation (covered by
  existing unit tests).
- The next-gen (KIP-848) protocol (already modeled in #521); offset commit/fetch.
- The `rebalance_deadline` wall-clock timer (the model drives `complete_rebalance` directly rather
  than the deadline-fires path; the deadline is a scheduling detail, not a safety invariant).

## Verification discipline

- Every `stateright` run is fenced (`within_boundary` + `target_state_count`/`timeout`
  `Duration::from_mins(2)`) and run under the host memory watchdog (kill > 3 GB / > 150 s) while
  bounds are tuned — `[[feedback_bound_model_checkers]]`. `proptest` is bounded sampling.
- `cargo +nightly fmt` per-crate (`[[reference_windows_fmt_path_length]]`); `cargo clippy
  --all-targets -- -D warnings` clean.

## Success criteria

1. `#[derive(Clone)]` added to `Group`; existing `classic_state` + group-coordinator tests pass
   unchanged.
2. The model proves index-coherence + single-owner + joined-subset + static-never-expired +
   state/generation/leader invariants exhaustively across both configs (or produces a concrete
   counterexample — handled like prior slices); non-vacuity witnesses satisfied; clean under the
   watchdog.
3. The proptest passes at large N over the same real methods + invariants.
4. fmt + clippy clean; the broader broker suite unaffected.
