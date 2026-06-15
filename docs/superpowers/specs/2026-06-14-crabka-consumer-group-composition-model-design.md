# Consumer-Group Composition Model — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (formal verification) — the **third compositional / end-to-end** model, after the data-path
(#539) and txn/EOS (#541) compositions. Verifies the consumer-side delivery guarantee.

## Goal

Verify **consumer delivery correctness through rebalances** by composing the KIP-848 reconciliation engine
with the offset-commit fencing + fetch path. The headline:

> Through any sequence of joins / leaves / target changes / heartbeats: no two members ever own the same
> partition (exclusivity), and a partition's committed offset never regresses and is only writable by its
> current owner — so a partition resumed after a rebalance continues from exactly its last committed offset
> (no duplicate processing, no gap).

The reconciliation engine's no-double-ownership is already verified *in isolation* (#521, which found + fixed
a real double-delivery bug). This composes it with the **offset/delivery layer** to verify the consumer-facing
consequence — the **ownership ↔ offset seam**: a member that lost a partition via reconciliation must be
fenced from committing a stale offset for it.

Honest discovery odds: **low-moderate** — exclusivity is already proven; the new risk is the offset-fencing
seam (a stale in-flight commit landing after a revocation). Narrower than the first two compositions.

## Scope — DRIVEN vs MODELED (stated up front, per the txn/EOS review lesson)

- **DRIVEN (real code):** the reconciliation engine on a real consumer `GroupState` (`consumer_state.rs`):
  `reconcile_member` (the `acquire only free-or-owned` core — #521's fix), `install_target`,
  `advance_member_epoch`, group-epoch bump. The model holds a real `GroupState` and drives these methods.
- **MODELED (faithful abstraction, NOT driving real code):** the offset-commit **fencing** rule
  (`validate_group_commit` is async / handle-based; the rule is *a member may commit for a partition only
  while it currently owns it at the current epoch*), the committed-offset store, and fetch-resumes-from-
  committed. These are a small ownership/epoch predicate + a per-partition offset map.
- **Out / NOT covered:** the `__consumer_offsets` log persistence + replication (the data-path model's
  territory), the classic (non-KIP-848) rebalance protocol (`classic_state_model`, #534), the async
  `validate_group_commit` plumbing itself.

## Construction: wrap-real reconciliation + modeled offsets

A single new `stateright` model `crates/broker/src/coordinator/unified/consumer_group_composition_model.rs`
(`#[cfg(test)]` submodule). It holds a **real `GroupState`** (members + target + `group_epoch`) and a modeled
`committed: HashMap<(topic,partition), offset>`. `GroupState` carries `HashMap`s (not `Hash`), so the model
hand-implements `Hash`/`Eq` over a sorted projection (per-member `assigned_partitions` / `member_epoch` /
`assignment_state`, `group_epoch`, the committed-offset map) — the `classic_state_model` (#534) pattern.
(Likely needs `#[derive(Clone)]` on `GroupState`/`MemberState`; no logic change.)

## State & actions

**State (projected for hashing):** the real `GroupState` + `committed: map<(tid,part), i64>` + per-member
`position: map<member, map<(tid,part), i64>>` (where a member has consumed to, for the no-gap/no-dup check).

**Actions:**
- `Join(m)` — a new member joins (added to `GroupState.members`); bump group epoch.
- `Leave(m)` — member leaves (removed); bump group epoch.
- `SetTarget(assignment)` — the assignor produces a new target over the tiny topic/partition universe;
  drive `install_target` + group-epoch bump.
- `Heartbeat(m, reported_owned)` — drive the real `reconcile_member(m, reported_owned)`; the member's
  `reported_owned` reflects what it currently holds (its prior `assigned_partitions`).
- `AdvanceEpoch(m)` — drive `advance_member_epoch(m)` (the member acks a new epoch).
- `Commit(m, tid, part, off)` — MODELED fenced commit: succeeds (writes `committed[(tid,part)] = off`) only if
  `m` currently has `(tid,part)` in its real `assigned_partitions`; else it is fenced (no-op).
- `Fetch(m, tid, part)` — MODELED: if `m` owns `(tid,part)`, advance `position[m][(tid,part)]` from
  `committed[(tid,part)]` (resume-from-committed).

## Invariants

Per-transition `next_state` asserts + `Property::always`:
- **`exclusive_ownership`** (HEADLINE): no `(tid,part)` appears in two members' `assigned_partitions` — the
  reconciliation engine never double-grants. (Re-verifies #521's property *in the composed context*, where
  joins/leaves/target-changes/heartbeats interleave with offset traffic.)
- **`no_offset_regression`**: `committed[(tid,part)]` is monotonic non-decreasing across every transition.
- **`only_owner_commits`**: a successful `Commit(m, …)` implies `m` owned the partition at commit time (the
  fencing) — a fenced (non-owning / stale-epoch) member's commit is a no-op and never mutates `committed`.
- **`no_dup_no_gap`**: a member's `position` for an owned partition never moves backwards (no reprocessing)
  and never skips past `committed` (resumes exactly from the committed offset).

**Non-vacuity witnesses (`sometimes`):** a rebalance moves a partition from one member to another; a stale /
non-owning member's commit is fenced (a no-op); a partition is resumed by a new owner from its committed
offset; ≥2 members hold disjoint partitions; a member is in `UnrevokedPartitions` (mid-revocation).

## Configs

- **`cg_basic`** — 2 members, 1 topic / 2 partitions, short sequences. Exhaustive.
- **`cg_wide`** — 3 members / 2 partitions (or a 3rd partition) as the watchdog allows.

## Tractability

Wrap-real holding a real `GroupState` (HashMaps) → keep the projection minimal + monotonic generators
(`group_epoch`, `member_epoch`) bounded/out of the fingerprint where they don't gate decisions (the DPM-A1 /
classic-group lesson). Bound on **unique** states with a high `target_state_count` backstop; **host memory
watchdog (3 GB / 150 s)** on every run. Small universe (≤3 members, ≤2–3 partitions, ≤short offsets).

## RED handling

A counterexample at the ownership↔offset seam is the goal. Triage real-bug (a reconciliation gap, or the
fencing rule being wrong) vs model-faithfulness (the modeled offset/fencing). Fix RED→GREEN (record the
counterexample) or fix the model + document.

## Post-GREEN: adversarial faithfulness review

After GREEN, run the adversarial faithfulness review workflow (now standard for compositional models): is the
reconciliation genuinely driven? are the invariants non-tautological (mutation-test the fencing predicate)? is
the offset model faithful? does the GREEN over-claim? Apply fixes before finalizing — as the txn/EOS review
drove the HWM-clamp strengthening.

## Verification discipline

- `stateright` wrap-real; watchdog-guarded. `cargo +nightly fmt -p crabka-broker`; `cargo clippy -p
  crabka-broker --all-targets -- -D warnings` clean. Production change limited to `#[derive(Clone)]` (and any
  small visibility widening) on the coordinator state structs; no logic change.

## Success criteria

1. The model drives the real `reconcile_member` / `install_target` / `advance_member_epoch` over interleaved
   join/leave/target/heartbeat + offset traffic; both configs.
2. `exclusive_ownership` + `no_offset_regression` + `only_owner_commits` + `no_dup_no_gap` proved exhaustively
   (or a real bug found + fixed RED→GREEN); witnesses satisfied; clean under the watchdog.
3. Adversarial faithfulness review passed (or its fixes applied); module doc states DRIVEN vs MODELED.
4. fmt + clippy clean. Extends the compositional layer (delivery pillar); only control/reconfiguration remains.
