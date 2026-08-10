# KIP-939 Two-Phase-Commit (2PC) Participation — Design

**Date:** 2026-06-16
**Status:** Implemented
**Workstream:** A (wrap-real stateright models of pure cores) + coordinator semantics
**Predecessors:** KIP-98/EOS txn coordinator (`txn/decision.rs`, `txn/decision_model.rs`),
EOS composition model (`txn/eos_composition_model.rs`), KIP-890 epoch fencing.

## Goal

Make Crabka a correct **2PC participant** per
[KIP-939](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939%3A+Support+Participation+in+2PC):
a producer that declares `enable2Pc=true` hands the commit/abort decision to an
*external* transaction manager (an XA resource manager, Apache Flink's sink, …).
The coordinator must therefore **never proactively abort such a transaction on
the transaction timeout** — only the external manager (via `EndTxn`) or an
explicit new `InitProducerId` generation may end it.

The safety-critical claim is proved by an exhaustive `stateright` model that
drives the *real* decision core under every interleaving of the transaction
lifecycle with the timeout reaper.

## Background — how Kafka encodes 2PC (matched exactly)

Confirmed against Apache Kafka's `TransactionCoordinator` / `TransactionMetadata`:

- **No new persisted field.** 2PC is encoded in the already-persisted
  `TransactionTimeoutMs` via the sentinel `Integer.MAX_VALUE`. Kafka's
  `TransactionMetadata.isDistributedTwoPhaseCommitTxn()` is literally
  `txnTimeoutMs == Integer.MAX_VALUE`, and `handleInitProducerId` resolves the
  stored timeout to `Int.MaxValue` when `enable2Pc`. Because the timeout
  round-trips through `TransactionLogValue`, the property survives coordinator
  failover and log replay with **no schema change** — important given Crabka's
  hand-written `log_record.rs` codec.
- **Cluster gate.** `transaction.two.phase.commit.enable` (broker config,
  default `false`). When disabled, an `enable2Pc` request is rejected with
  `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (not an `UNSUPPORTED_*`, so a client
  cannot probe the flag).
- **ACL.** A new `TWO_PHASE_COMMIT` operation (wire discriminant **15**) on the
  `TransactionalId` resource, required in addition to `Write`.
- **`keepPreparedTxn`.** The prepared-txn recovery flow is available behind
  opt-in `transaction.version=3`. It returns the original ongoing identity and
  stages a separately fenced recovery identity that persists in the flexible
  transaction-state record.

Crabka had **no transaction-timeout reaper at all** before this change — the
`txn_timeout_ms` was persisted but never enforced. So implementing "2PC is never
auto-aborted" required first implementing the auto-abort it must be exempt from
(the classic KIP-98 timeout).

## What was built

### Pure decision cores — `txn/two_pc.rs`

- `NO_TIMEOUT_MS = i32::MAX` — the 2PC sentinel.
- `resolve_txn_timeout(enable_2pc, requested_ms)` — `NO_TIMEOUT_MS` for 2PC,
  else the request clamped to `[1s, 15min]` (Kafka's `transaction.max.timeout.ms`).
- `is_two_phase_commit(txn_timeout_ms)` — the sentinel check.
- **`should_abort_idle_txn(state, txn_timeout_ms, start_ms, now_ms)`** — THE
  safety-critical predicate. `true` iff the txn is `Ongoing`, **not** 2PC, and
  `now - start >= timeout` (saturating, so a backwards clock can't reap). The
  2PC skip happens *before* the arithmetic, so even a far-future `now` can't reap
  a 2PC txn.

### Coordinator + reaper

- `TxnCoordinator::sweep_expired(now_ms, txnv)` (`txn/coordinator.rs`): for each
  locally-coordinated tid, if `should_abort_idle_txn` says so, run the same
  two-step `Ongoing → PrepareAbort → (markers) → CompleteAbort` as an
  `EndTxn(abort)`, bumping the producer epoch on completion (TV_2) to fence the
  timed-out producer. Re-validates identity+state before the Complete write so a
  concurrent `EndTxn`/`InitProducerId` is never clobbered. Marker fan-out is
  sent to both local and remote partition leaders.
- `txn/expiration.rs`: a `tokio` ticker (`DEFAULT_REAP_INTERVAL = 10s`, matching
  `transaction.abort.timed.out.transaction.cleanup.interval.ms`) spawned from
  `Broker::start`. Every broker runs it; it acts only on tids it coordinates.
- `AddPartitionsToTxn` now stamps `start_ms` on the edge into `Ongoing` (Kafka's
  `txnStartTimestamp`), so the timeout is measured from the real start.

### `InitProducerId` handler

In the transactional branch, before the coordinator check: gate `enable2Pc` and
`keepPreparedTxn` on `transaction.version=3`, the cluster config, and the
`TWO_PHASE_COMMIT` ACL. Recovery returns the original ongoing identity, advances
the staged identity on every call, persists it, and lets the latest recovery
client complete through `EndTxn`. `keepPreparedTxn` is valid without
`enable2Pc`; only `enable2Pc` replaces the stored timeout with the no-timeout
sentinel.

### ACL plumbing

`AclOperation::TwoPhaseCommit` added to the metadata enum and all wire/byte
mappings (`kraft_translate`, broker `acl_wire`, admin client, OPA string,
operator/grpc/schema-registry conversions), and to the `TransactionalId`
authorized-operations set (Kafka 4.0 parity).

## The model — `txn/two_pc_model.rs`

Exhaustive `stateright` BFS over one tid, interleaving `Init(classic|2PC)` /
`BeginTxn` / `EndTxn(commit|abort)` / `TimeoutSweep(elapsed?)`. The sweep drives
the **real** `should_abort_idle_txn` (with `now = i64::MAX` or `start`); `EndTxn`
drives the real `decide_phase1_transition`. Ghost per-epoch commit/abort sets
track outcomes.

Properties:
- **`two_pc_never_reaped`** (headline, KIP-939): the reaper never aborts a 2PC
  txn. Provable only if `should_abort_idle_txn` is correct.
- **`no_commit_and_abort`**: atomicity / single-finalize survives the reaper
  interleaving.
- Non-vacuity: the reaper *does* abort classic txns; a 2PC txn is reachable+open;
  a commit can complete.

Bounds: `max_epoch ∈ {3, 6}`, fenced by `within_boundary` + state/depth caps +
2-minute timeout, like the sibling txn models.

## Deliberately out of scope

- Native client `prepareTransaction` / `completeTransaction` APIs.
- Admin `forceTerminateTransaction` and its command-line surface.
