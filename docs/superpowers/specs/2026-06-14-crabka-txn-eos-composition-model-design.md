# Transaction / EOS Composition Model — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (formal verification) — the **second compositional / end-to-end** model, after the data-path
composition (#539). Verifies the exactly-once *atomicity* pillar (the data path verified durability).

## Goal

Verify the broker's end-to-end **exactly-once read** guarantee by composing the transaction coordinator's
commit/abort decision with the Last-Stable-Offset (LSO) mechanics and the `read_committed` fetch-visibility
core. The headline:

> What a `read_committed` consumer can see — every offset below the LSO, minus aborted batches — is
> **exactly the committed records, in order**: no aborted record ever leaks, and no record of a still-open
> transaction is exposed.

Note on "atomicity": because concurrent producers' transactional batches **interleave at the offset level**,
a committed transaction can legitimately be only partially below the LSO at a given moment (a later batch
sits behind another producer's still-open txn). The consumer sees the rest in order as the LSO advances —
so the guarantee is **prefix-correctness + in-order delivery**, not whole-txn atomicity at a single snapshot.
The all-or-nothing property is exact on the **abort** side (an aborted txn contributes *nothing*, ever).

The EndTxn decision core (#523) and the visibility core (#529) are each verified *in isolation*. This targets
the **seam** between them — the LSO, which translates a coordinator's commit/abort outcome into what a
`read_committed` consumer may read. Honest discovery odds: **low-moderate** — the subtle bit is the LSO
"held back by the oldest open transaction" rule (an out-of-order commit must NOT become visible while an
older transaction is still open).

## Scope

**In:** a single partition on a single leader; ≥2 concurrent transactional producers interleaving
begin → append → commit/abort; the LSO advancing as transactions resolve; a `read_committed` consumer.

**Out (deliberate):**
- **Leader changes / failover** — the data-path composition (#539) already verifies that markers + LSO + HWM
  survive leader changes through the replicated log. v1 isolates the txn ↔ LSO ↔ `read_committed` seam; a v2
  could compose with failover (large state space, the data-path layer's territory).
- **`read_uncommitted`** (sees everything up to HWM — no txn semantics); the offset-commit / `__transaction_state`
  persistence; producer-id allocation internals.

## Construction: wrap-real cores at the seam

A single new `stateright` model `crates/broker/src/txn/eos_composition_model.rs` (`#[cfg(test)]` submodule of
`txn`, reaching the `pub(crate)` cores). It **drives the real cores** at the seam and models the LSO + aborted
-list bookkeeping (which are incrementally-maintained stored state, not pure fns):

| Seam | Real core driven | Location |
|------|------------------|----------|
| coordinator commit/abort decision | `decide_phase1_transition`, `decide_end_txn_completion` (over `TxnEntry` / `TxnState::can_transition_to`) | `crates/broker/src/txn/decision.rs` |
| `read_committed` visibility | `compute_visibility_window` (read-committed branch: `effective_lso = lso.min(hw)`) | `crates/broker/src/handlers/fetch.rs` |

**Modeled (faithfully, per the real rule):** the **LSO** = the base offset of the oldest still-open
transactional batch, else the log end (Kafka's `first-unstable-offset`); the **aborted-txn list**
(`AbortedTxn{start_offset,last_offset,producer_id}`) recorded when a txn aborts; the `read_committed` visible
set = batches with `last_offset < effective_lso` minus aborted batches (mirrors `TxnIndex::aborted_in_range`).
Single leader ⇒ `hw = log_end` (fully replicated), so `effective_lso = lso` — the LSO is the visibility gate.

## State & actions

**State (hashable projection):**
- `log: Vec<Batch>` where `Batch { producer: u8, txn_seq: u8, kind: Data | CommitMarker | AbortMarker }` (offset
  = index). A txn = a producer's run of `Data` batches terminated by a marker.
- per-producer `TxnEntry`-projection: `{ state: TxnState, epoch }` (the coordinator side).
- `lso: i64` (derived each step from the open-txn rule).
- `aborted: Vec<(start,last,producer)>` (resolved aborts).
- ghost: per-txn outcome (`Open | Committed | Aborted`) for the atomicity check.

**Actions:**
- `Begin(p)` — producer `p` opens a txn (drive `can_transition_to(Ongoing)`).
- `Append(p)` — `p` appends a `Data` batch to its open txn (extends its offset range).
- `End(p, commit)` — drive `decide_phase1_transition(entry, commit)` (Ongoing→Prepare) then
  `decide_end_txn_completion(...)` (Prepare→Complete); append the Commit/Abort marker; on abort record the
  `AbortedTxn`; recompute the LSO. A concurrent `End` with a stale epoch drives the real fencing
  (`CompletionDecision::Reject`).
- `ConsumerRead` — drive `compute_visibility_window(read_committed=true, …, hw=log_end, lso, …)`; the visible
  set = `Data` batches with `offset < effective_lso` minus aborted batches; assert atomicity.

## Invariants

The `read_committed` visible set `V = { Data batch b : b.offset < effective_lso AND b's txn ∉ aborted-list }`.
Per-transition `next_state` asserts + `Property::always`:
- **`only_committed_visible`** (HEADLINE): every batch in `V` belongs to a transaction that has resolved as
  **Committed** — no open/uncommitted record and no aborted record is ever visible.
- **`committed_prefix_complete`**: every committed `Data` batch below `effective_lso` (and not aborted) **is**
  in `V` — no committed record below the LSO is wrongly hidden or filtered. (Together with the headline,
  `V` = *exactly* the committed records below the LSO.)
- **`no_visible_aborted`**: no batch from an aborted transaction is ever in `V` (the abort-side all-or-nothing).
- **`lso_blocks_open`**: the LSO never exceeds the base offset of any open transaction (an out-of-order commit
  stays invisible while an older txn is open).
- **`lso_monotonic`**: the LSO never regresses.

**Non-vacuity witnesses (`sometimes`):** a committed txn's records become visible; an aborted txn is recorded
+ filtered; two producers' transactional batches interleave at the offset level; a younger txn commits while
an older stays open so its committed batch sits *above* the LSO (held back); a stale-epoch `End` is fenced
(`CompletionDecision::Reject`).

## Configs

- **`txn_basic`** — 2 producers, ≤2 transactions each, short log. Exhaustive.
- **`txn_wide`** — wider (more interleavings / a third txn or producer) as the watchdog allows.

## Tractability

Small by construction (≤2–3 producers, ≤2–3 txns, log-len ≤ 4–5, no failover): far smaller than the data-path
model. Standard controls: keep monotonic generators out of the fingerprint; bound on **unique** states with a
high `target_state_count` backstop; **host memory watchdog (3 GB / 150 s)** on every run.

## RED handling

A counterexample at the LSO seam is the goal. Determine real-bug vs model-faithfulness (the LSO rule / aborted
-list mirroring). If a real bug in `compute_visibility_window`'s `read_committed` branch or the decision cores →
fix RED→GREEN, recording the counterexample; if a faithfulness gap in the modeled LSO/aborted mechanics → fix
the model + document (as the data-path composition's three refinements did).

## Verification discipline

- `stateright` wrap-real; watchdog-guarded. `cargo +nightly fmt -p crabka-broker`; `cargo clippy
  -p crabka-broker --all-targets -- -D warnings` clean. Likely **no production change** (the cores are already
  extracted; the LSO/aborted bookkeeping is modeled, not driven from file-backed `TxnIndex`/`Log`).

## Success criteria

1. The composed model drives the real decision + `read_committed` visibility cores over interleaved
   transactions; both configs.
2. `txn_atomic_visibility` + `no_visible_aborted` + `lso_monotonic` + `lso_blocks_open` proved exhaustively
   (or a real bug found + fixed RED→GREEN); witnesses satisfied; clean under the watchdog.
3. fmt + clippy clean; existing txn + fetch suites unaffected.
4. Extends the compositional layer (atomicity pillar). Remaining end-to-end paths: consumer-group, control.
