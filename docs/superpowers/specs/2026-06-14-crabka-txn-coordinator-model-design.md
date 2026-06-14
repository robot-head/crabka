# Transaction-Coordinator (KIP-98 / EOS) Safety Model — Design

**Date:** 2026-06-14
**Status:** Approved (design); spec under review
**Workstream:** A (wrap-real stateright models of pure cores)
**Predecessors:** raft consensus, share-group `AcquisitionState` (#514), ISR/`ReplicaState` (#515),
failover/unclean-recovery (#516), reassignment/KIP-455 (#520), KIP-848 reconciliation (#521).

## Goal

Build an exhaustive `stateright` model of the transaction coordinator's per-`TransactionalId`
state machine that proves its EOS safety invariants — above all that **a producer fenced
(epoch bumped) during EndTxn's marker window can never complete its transaction**, and **no
transactional id is ever both committed and aborted** — by driving the *real* coordinator
decision logic under every concurrent interleaving.

This is the last major untouched correctness surface in the broker (the model program has
covered consensus, ISR/HWM, failover, reassignment, and consumer-group reconciliation).

## Background — what the code does

The transaction coordinator lives in `crates/broker/src/txn/`:
- `txn/state.rs` (159 lines) — the state machine: `TxnState` enum + `TxnEntry` + `can_transition_to`.
- `txn/coordinator.rs` (349 lines) — the coordinator (per-tid entry map, persistence via `put`/`get`).
- `txn/handlers/` — RPC handlers: `end_txn.rs` (860 lines, the critical one),
  `add_partitions_to_txn.rs`, `txn_offset_commit.rs`, `add_offset_commits_to_txn.rs`,
  `write_txn_markers.rs`; `InitProducerId` is in `crates/broker/src/handlers/init_producer_id.rs`.

**`TxnState`** (`txn/state.rs:8`): `Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit,
CompleteAbort, Dead`. Legal transitions (`can_transition_to`, `state.rs:20`):
`Empty|Complete* → Empty`; `Empty|Ongoing|Complete* → Ongoing`; `Ongoing → PrepareCommit|PrepareAbort`;
`PrepareCommit → CompleteCommit`; `PrepareAbort → CompleteAbort`; `Complete* → Dead`.

**`TxnEntry`** (`txn/state.rs:84`): `transactional_id, producer_id, producer_epoch, state,
txn_timeout_ms, partitions: HashSet<TopicPartition>, prev/next_producer_id, timestamps`.

**Epoch fencing.** `InitProducerId` allocates a fresh `(pid, epoch=0)` or bumps an existing
entry's epoch (`init_producer_id.rs:209`, `checked_add(1)` with wraparound). The RPC handlers
reject a request whose `(producer_id, producer_epoch)` doesn't match the persisted entry with
`INVALID_PRODUCER_EPOCH` (e.g. `end_txn.rs:89`, `add_partitions_to_txn.rs:263`).

**The multi-phase EndTxn** (`end_txn.rs`) — the safety-critical sequence:
1. **Phase 1** (`end_txn.rs:114`): validate state can transition `Ongoing → Prepare{Commit,Abort}`,
   set `state = Prepare`, persist the Prepare record (one `.await`).
2. **Phase 2** (marker fan-out): write transaction markers to the partitions in the txn set
   (local + remote brokers; multiple `.await`s). The decision is fixed from the Phase-1 snapshot.
3. **Phase 3** (`end_txn.rs:176`): re-fetch the entry, **re-validate** via `validate_complete_reacquire`
   (`end_txn.rs:397` — checks the `(pid, epoch)` hasn't changed and the state is still the expected
   Prepare), compute `next_producer_identity` (`end_txn.rs:345` — at KIP-890 TV_2, bump epoch),
   set `state = Complete`, persist (one `.await`). On a fencing/state mismatch it rejects without
   persisting Complete.

**The window** between Phase 1 and Phase 3 is where a concurrent `InitProducerId` (bumping the
epoch) or `AddPartitionsToTxn` can interleave. Phase 3's `validate_complete_reacquire` is the
mechanism that must prevent a fenced producer from finalizing. This window is exactly what a
model checks and unit tests cannot.

## Production refactor (behavior-preserving, ~30–50 LOC)

Two pure decision functions already exist and are unit-tested (`validate_complete_reacquire`,
`next_producer_identity` in `end_txn.rs`). The refactor relocates them into a new
`crates/broker/src/txn/decision.rs` and adds the Phase-1 decision (currently inline):

```rust
// txn/decision.rs
pub(crate) fn decide_phase1_transition(
    entry: &mut TxnEntry,
    committed: bool,
) -> Result<(TxnState, TxnState), i16>;   // (prepare, complete) or INVALID_TXN_STATE

pub(crate) fn decide_end_txn_completion(
    entry: &TxnEntry, expected_pid: i64, expected_epoch: i16,
    prepare: TxnState, complete: TxnState, txnv: TxnVersion,
    ids: &ProducerIdManager,
) -> CompletionDecision;   // Proceed{next_state, resp_pid, resp_epoch} | AlreadyComplete | Reject(code)
```

`decide_end_txn_completion` wraps the existing `validate_complete_reacquire` + `next_producer_identity`
unchanged. The `end_txn` handler calls these between its existing `.await` persist/marker steps —
no I/O change. Gated by the existing `txn` handler tests (`add_partitions`, `end_txn` unit tests).

## The model (wrap-real, hashable projection)

Consistent with the ISR/failover/reassignment/reconciliation models: the stateright `State` is a
hashable projection; `next_state` reconstructs a real `TxnEntry`, drives the real decision functions,
and reads the result back.

### State (per modeled `TransactionalId`, hashable)

`pid: i64, epoch: i16, state: TxnState, partitions: BTreeSet<(topic,partition)>` + a ghost
**`markers_written: BTreeSet<(topic,partition)>`** ledger and per-`(pid,epoch)` **terminal-outcome
map** (to assert no commit-and-abort). `TxnState` gets a `Hash` derive (no behavior change).
Timestamps dropped. One transaction version (`Verified`/TV_2 — the epoch-bumping path, the most
interesting for fencing).

### Actions (drive the real decision fns)

- `InitProducerId(tid)` — allocate (new) or bump epoch (existing); if `Ongoing`, abort-then-bump.
- `AddPartitionsToTxn(tid, pid, epoch, partition)` — epoch/state-gated add (`Ongoing`).
- `AddOffsetsToTxn(tid, pid, epoch)` — epoch/state-gated.
- `EndTxnPhase1(tid, pid, epoch, committed)` — drives `decide_phase1_transition`; on success the
  entry enters `Prepare*` and the model records the partition set for the marker fan-out.
- `MarkerWindow` — the interleave point: while a tid is in `Prepare*`, the other actions above
  remain enabled (a concurrent `InitProducerId` here is the zombie-fencing scenario).
- `EndTxnPhase3(tid, pid, epoch)` — drives `decide_end_txn_completion`; either finalizes to
  `Complete*` (and "writes" markers) or rejects (fenced / state advanced).

Splitting EndTxn into Phase1 / MarkerWindow / Phase3 with the other actions enabled in between is
what exercises the concurrency window the real handler must survive.

### Properties

- **`always` epoch-fencing:** an RPC whose `(pid, epoch)` ≠ the entry's is rejected
  (`INVALID_PRODUCER_EPOCH`); a producer whose epoch was bumped during the window cannot finalize.
- **`always` legal-DAG:** every realized transition satisfies `can_transition_to`.
- **`always` no-commit-and-abort:** for a given `(tid, pid, epoch)`, the terminal outcome is never
  both `CompleteCommit` and `CompleteAbort` (headline atomicity).
- **`always` idempotency:** a replayed `EndTxnPhase3` after completion returns success
  (`AlreadyComplete`) without a second finalize.
- **`always` marker/state coherence:** if Complete-markers were written for a partition, the entry
  reached the matching `Complete*` (or the producer was fenced and isn't authoritative).
- **`sometimes` witnesses:** a commit completes; a producer gets fenced mid-window (non-vacuity).

Safety as `next_state` asserts where per-transition (not stored ghost history) — consistent with
the prior models, to keep the fingerprint small.

### Bounding

1–2 transactional ids, `pid` from a tiny pool, `epoch ∈ 0..=3`, 2–3 partitions, bounded step count
via `within_boundary` (epoch ≤ MAX). Mandatory checker fences (`target_state_count`,
`target_max_depth`, `timeout = Duration::from_mins(2)`), run under the host memory watchdog
(3 GB / 150 s) while bounds are tuned — see `[[feedback_bound_model_checkers]]`.

## Out of scope (YAGNI for v1)

- The marker *I/O* itself (partition produce, inter-broker `WriteTxnMarkers` RPC, marker acks) —
  the model abstracts Phase 2 as the interleave window; it checks the *decision*, not the transport.
- KIP-447 transactional-offset materialization mechanics (modeled only as a pending-offset flag if
  needed for the abort-drops-offsets property; full offset visibility is a separate concern).
- Transaction-timeout-driven expiry (`Dead` via timeout) — time-based; excluded like prior models.
- Producer-id exhaustion / rollover-to-new-pid edge (`next_producer_identity` overflow branch) —
  kept out of the bounded space; covered by the existing unit tests.

## Risks

- **Phase-window fidelity:** the model must faithfully represent "Phase 1 persisted, markers in
  flight, Phase 3 not yet run" so concurrent actions interleave realistically. Mitigation: the
  three-action split mirrors the real handler's `.await` boundaries; the spec-compliance review
  scrutinizes the window model.
- **Refactor blast radius:** `end_txn.rs` is 860 lines. Mitigation: the extracted functions are
  small and two already exist; behavior-preserving, gated by the existing txn tests.
- **State explosion:** multiple tids × pid/epoch × partition sets × phase state. Mitigation: 1–2
  tids, tiny epoch/partition bounds, watchdog + hard `target_state_count`.

## Success criteria

1. `decide_phase1_transition` + `decide_end_txn_completion` extracted; all existing `txn` handler
   tests pass unchanged.
2. The model drives the real decision core and proves the safety properties exhaustively across all
   configs (or produces a concrete counterexample — handled like the reconciliation slice).
3. All configs exhaustive (no cap truncation) under the memory watchdog; non-vacuity witnesses hold.
4. `cargo +nightly fmt` clean; `clippy -D warnings` clean; broader broker suite unaffected.
