# G8 two-successor Split crash-anywhere matrix design

## Goal

Prove, with real broker processes, mTLS range control, the actual `crabka gres split` CLI, and source-process SIGKILL/restart, that a two-successor Split recovers correctly at every externally observable durable or idempotent transition. Every case must preserve an exact continuous payload history, exact r2/r3 ownership, authenticated marker inheritance, atomic topology publication, exact predecessor retirement, bounded write pause, and process cleanup.

No production test-only crash hooks are permitted. A crash boundary is eligible only when the harness can recognize it from durable registry state, authenticated control receipts, tenant/retirement state, broker topic metadata, successor status, or the external workload ledger.

## Reconciliation graph and exhaustive case derivation

The case list is derived mechanically by walking the actual Split graph in `reconcile_one_rpc_phase`, `request_for_phase`, `GenerationFencedRangeControl`, `LiveRangeControl`, `reconcile_activated_cutover`, and `reconcile_one_retiring_range_wal`:

```text
Initiated --journal CAS--> Running
Running --checkpoint side effect/receipt--> Running --journal CAS--> Checkpointed
Checkpointed --pause side effect/receipt--> Checkpointed --journal CAS--> Paused(no tail)
Paused --two-successor stage side effect/receipt--> Paused(no tail) --journal CAS--> Paused(tail)
Paused(tail) --marker verify + claim r2/r3 + receipt--> Paused(tail) --journal CAS--> Restored
Restored --successor publication/prologue receipt--> Restored --journal CAS--> Activated
Activated --tenant target-layout + retirement-sidecar CAS--> Activated --journal CAS--> LayoutPublished
LayoutPublished --journal CAS--> Retiring(Parking)
Retiring --topic delete--> Retiring(Parking) --sidecar CAS--> Retiring(Parked)
Retiring(Parked) --retire-source control receipt--> Retiring --journal CAS--> Resuming
Resuming --journal CAS--> Completed
```

Each side-effect/receipt edge and each following journal/tenant/sidecar edge yields a distinct pre/post crash boundary. The Retiring journal-CAS post-state is already the pre-delete state, so it is represented once as `retiring_before_delete`; inventing a second case would require a forbidden non-durable scheduling hook. The exact cases and external predicates are:

| Family | Case | Required pre-kill external predicate |
|---|---|---|
| source/restore | `initiated_before_running_cas` | journal `Initiated`; sealed Split plan; tenant current layout; no control receipt |
| source/restore | `checkpoint_receipt_before_journal_cas` | journal `Running` with empty evidence; authenticated completed `ForceCheckpoint` receipt exists |
| source/restore | `checkpointed_after_journal_cas` | journal `Checkpointed`; manifest and covered offset durable; pause receipt absent |
| source/restore | `pause_receipt_before_journal_cas` | journal `Checkpointed`; completed pause receipt has barrier at or beyond covered offset; journal barrier absent |
| source/restore | `paused_before_stage` | journal `Paused`; barrier durable; tail/marker evidence absent; stage receipt absent |
| source/restore | `stage_receipt_before_journal_cas` | journal `Paused` without tail; completed stage receipt exists; authenticated replay returns identical tail digest; r2/r3 staged but not serving |
| source/restore | `staged_after_journal_cas` | journal `Paused`; tail digest durable; marker receipt absent |
| source/restore | `marker_claim_receipt_before_journal_cas` | journal `Paused` with tail but no marker digest; completed marker receipt exists; captured source/left/right partitions are exact and both distinct successor claims exist |
| source/restore | `restored_after_journal_cas` | journal `Restored`; complete transfer evidence; tenant current layout; prologue receipt absent |
| source/restore | `prologue_receipt_before_journal_cas` | journal `Restored`; completed prologue receipt exists; authenticated status says r2 and r3 generation 1 are serving on distinct endpoints; tenant still current layout |
| source/restore | `activated_after_journal_cas` | journal `Activated`; r2/r3 status serving; tenant current layout; retirement sidecar absent |
| publication | `tenant_cas_before_journal_cas` | journal `Activated`; tenant exact target `[r0,r2,r3]` at sealed version + 1; Parking sidecar exact; journal not LayoutPublished |
| publication | `layout_published_after_journal_cas` | journal `LayoutPublished`; target tenant/sidecar exact; predecessor topic present |
| retirement/resume | `retiring_before_delete` | journal `Retiring`; target layout; sidecar `Parking`; predecessor topic present; no retire receipt |
| retirement/resume | `delete_success_before_sidecar_cas` | journal `Retiring`; sidecar still `Parking`; predecessor topic absent; counting admin records exactly one successful predecessor-only delete |
| retirement/resume | `parked_after_sidecar_cas` | journal `Retiring`; sidecar `Parked`; predecessor topic absent; no retire receipt |
| retirement/resume | `retire_receipt_before_journal_cas` | journal `Retiring`; sidecar `Parked`; durable authenticated retire receipt exists; journal not Resuming |
| retirement/resume | `resuming_after_journal_cas` | journal `Resuming`; durable retire receipt; predecessor topic absent; source parked |
| retirement/resume | `completed_after_journal_cas` | journal `Completed`; all terminal invariants true; SIGKILL/restart remains a no-op and leaves no operation-owned child process |

The family scripts contain these literal expected name sets. Validation fails on a missing, duplicate, extra, malformed, or wrong-family evidence file.

## Harness architecture

Add a Split-specific `SplitKillPoint`; do not overload Move's `SourceKillPoint`. Each variant owns its exact predicate, expected pre-kill durable evidence, restart hosted ranges, and maximum pause/operation bound. A single exact test reads `CRABKA_G8_SPLIT_KILL_POINT`, creates unique tenant/operation/sentinel identities, starts the real cluster and continuous workload, initiates Split through the CLI, and drives production reconciliation.

The mutation-client wrapper records authenticated requests and responses for checkpoint, pause, stage, marker/claim, prologue, status, and retire operations. Receipt probes replay the exact authorized request and may only observe the durable response; they never synthesize state. The counting retirement admin records requested topics, successful deletes, injected post-delete ambiguity, and rejects unrelated deletion.

At the selected predicate the harness waits for at least eight fsynced ACKs, records the workload pause boundary, SIGKILLs the real source child, verifies the old PID/process group is gone, restarts with the phase-appropriate hosted set, reconstructs every client/admin from durable configuration, and resumes the same operation. Each case injects exactly one kill.

## Continuous workload and authoritative oracle

A separate child continuously attempts inserts into `live_ledger51(seq, route_key, checksum)`. It writes an fsynced JSON-lines ledger for `attempt`, `ack`, and `recovered_ack`. `route_key` alternates temporally across the low/high payload halves, while physical `(table_id,rowid)` remains the authoritative range key. After the case finishes, the workload is stopped, waited, and its process group verified absent. The ledger file is closed and reopened; parsed ACK/recovered-ACK records, not an in-memory mirror, are the sole payload oracle.

The test measures elapsed wall-clock time between fsynced ACK timestamps spanning pause/cutover/restart. The bound is case-specific and encoded in evidence. At least one ACK must occur before the kill, after restart, and after target layout publication. Every ledger record includes table identity. Post-cutover acknowledgements exercise both sealed physical intervals with two active streams: qualifying table50 rows route to r2, while table51 rowids at or above 16 route to r3. Each reopened ledger stream is verified against its own sealed `(table_id,rowid)` interval.

## Terminal verification and evidence

After restart and `Completed`, every case performs the same verifier:

- journal is `Completed`; tenant is exactly `[r0,r2,r3]`; r2/r3 endpoints are distinct and both generations are exactly 1;
- authenticated marker response has exact source union, explicit disjoint left/right partitions, interval membership, and digest equality with journal and retirement checkpoint;
- direct decoded scans equal the reopened ACK oracle's physical r2 and r3 partitions, have no cross-side rows, and their union is exact;
- a fresh SQL connection after restart returns the exact acknowledged payload union with no loss, duplication, or resurrection;
- workload shows recovered acknowledgements where an ACK response was ambiguous and includes post-publication writes routed to both successors;
- r1 WAL topic is absent; r0/r2/r3 and sentinel topics remain; exactly one predecessor-only deletion occurred; unrelated deletion was never attempted;
- old source PID differs from the restarted PID, all spawned workload/source process groups are reaped, and no child is left behind;
- measured maximum ACK gap and whole-operation duration are positive and below their per-case bounds.

Source/restore cases use a strict 25-second ACK-gap bound: the live unoptimized harness measured
21.113 seconds for process restart plus two-successor restore and prologue. Publication and
retirement cases use a tighter 15-second bound, above the measured 12.946-second unoptimized
late process-restart path.

The 15-second bound is an intentional evidence-based correction to the earlier agent-authored
12-second estimate, not an unresolved threshold: repeated real-process traces measured 12.946
seconds on the unoptimized harness. The validator still rejects any publication or retirement
gap above 15 seconds.

Each JSON evidence file contains measured predicates and values, never success literals. A validator recomputes counts, sets, interval facts, marker partition arithmetic, bounds, unique identity, and topic expectations. `--validate-only` must fail nonzero for empty, incomplete, wrong-case, duplicate, or missing-family inputs.

## CI sharding

Create three family scripts:

- source/restore: 11 exact invocations;
- publication: 2 exact invocations;
- retirement/resume: 6 exact invocations.

Each invocation has its own timeout, unique evidence path, and exact kill-point environment value. Scripts build once, run cases serially within the family, then validate the complete family directory. Individual names remain filterable for local reproduction and CI diagnosis.

## Verification and review

Unit/model tests first prove predicate truth tables, receipt probes, evidence parsing, exact family membership, and fail-closed validators. Then run all three real-process shards, the existing Split Stateright exhaustive gate, existing Move CI, operator/range-control focused tests, locked checks, rustfmt, and `git diff --check`. Commit task-sized changes and request independent review of the final matrix and generated evidence. No remote operations are allowed, and the unrelated `crates/gres-ranges/src/control.rs` formatting dirt remains uncommitted.
