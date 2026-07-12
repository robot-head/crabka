# G8 Move Retirement-Phase SIGKILL Design

## Scope

Extend the existing real-process Move nemesis fixture across four retirement windows:

- `retiring_before_delete`: journal `Retiring`, authoritative target tenant layout, matching `Parking` sidecar, exact predecessor WAL topic present.
- `retiring_after_delete`: journal `Retiring`, target layout, matching `Parking` sidecar, predecessor topic absent after one real delete that returns a deterministic error before the sidecar CAS.
- `retiring_parked`: journal `Retiring`, target layout, matching `Parked` sidecar, predecessor topic absent, before the retire-predecessor RPC.
- `resuming`: journal `Resuming`, target layout, matching `Parked` sidecar, predecessor topic absent, after the durable retire RPC receipt and before `Completed`.

The fixture kills only the actual Gres child after the exact durable predicate is simultaneously true. It restarts the child and reconstructs registry, range-mutation, and admin clients.

## Retirement Driver and Admin Seam

The production retirement reconciler remains unchanged. The test uses a counting `AdminClientLike` wrapper backed by a real Kafka admin client. The wrapper:

- permits deletion only of the exact predecessor generation topic;
- records exact predecessor delete calls and requested topic names in shared state;
- fails immediately on unrelated delete requests;
- can arm a one-shot AfterDelete fault that performs the real delete and then returns a deterministic admin error, causing the production helper to exit before its absence check and `Parking` to `Parked` CAS;
- is reconstructed after restart around a fresh real admin client while retaining the shared counters.

For `retiring_after_delete`, the pre-kill call must report the injected error, metadata must prove the predecessor topic is absent, and the tenant sidecar must remain `Parking`. Replay after restart must observe absence, issue no second predecessor delete, and advance the sidecar to `Parked`.

An unrelated sentinel topic is created before retirement. Every phase proves that the sentinel, coordinator topic, and successor generation topic survive. The predecessor topic must never be recreated.

## Phase Flow

The driver advances one durable step at a time:

1. `LayoutPublished` advances to `Retiring`.
2. BeforeDelete pauses before the WAL helper.
3. AfterDelete invokes the faulted production WAL helper and pauses after exact topic absence while the sidecar is still `Parking`.
4. Parked invokes the normal WAL helper and pauses after the sidecar CAS while the journal remains `Retiring`.
5. The retire-predecessor RPC advances the journal to `Resuming`; Resuming pauses before the journal CAS to `Completed`.

After restart, the normal production paths finish the remaining steps. The driver records phase, tenant version/layout, sidecar phase, topic presence, delete counts, PIDs, receipt evidence, and timing at the kill boundary.

## Exact Safety Assertions

Each case proves:

- old and new child PIDs differ;
- at least one deterministic ambiguous write is recovered;
- the final database ledger exactly equals the durable ACK ledger;
- ACKs exist before SIGKILL, after restart, and after `Completed`;
- the authoritative target layout contains exactly one replacement owner and no predecessor owner;
- the predecessor never serves after restart and does not reappear in tenant state or Kafka metadata;
- the coordinator, replacement, and unrelated sentinel topics remain unchanged;
- the predecessor topic is deleted exactly once where deletion is required, and no unrelated delete is attempted;
- the operation marker digest equals the retirement checkpoint marker digest;
- retirement reaches `Parked`, the journal reaches `Completed`, and no orphan retirement or predecessor topic remains;
- the observed maximum ACK gap and operation duration remain within evidence-derived bounds.

## TDD and Evidence

First add pure tests for phase predicates and the counting admin fault/call ledger, then observe their RED failures before implementing the seam. Run each real-process phase repeatedly to collect ACK-gap distributions before selecting bounds. Add a dedicated CI script that runs each phase in a separate process and validates every evidence field, including AfterDelete single-delete replay and sentinel preservation. Finish with formatting, focused tests, the full retirement shard, local commits, and independent review. No remote Git operations are permitted.
