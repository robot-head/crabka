# GRES G8 real two-successor Split design

Date: 2026-07-12

## Scope

Build one no-kill process foundation for a real two-successor `Split` before adding any Split kill matrix. The foundation uses the actual CLI, real broker, real GRES child, production operator reconciliation helpers, production mTLS range control, and two distinct stable successor proxy endpoints. Once this foundation is green, the already-proven source, cutover, and retirement durable phase groups may be parameterized for Split in a later implementation slice.

Separate target process placement is outside this nemesis slice. One real child hosts `r0,r1` before cutover and `r0,r2,r3` after recovery; r2 and r3 remain distinct routing identities, WAL owners, topics, and proxy endpoints.

## Harness topology

Extend `ProcessHarness` with harness-owned stable r2 and r3 range proxies. Both proxies forward to the single child range-control listener, just as the current r1 proxy can forward to the all-on-zero child. The harness exposes exact r2/r3 endpoints and retargets them whenever the child restarts. All proxy tasks, the child, temporary storage, TLS material, and broker remain owned by the harness cleanup path.

The initial child starts with `--host-ranges r0,r1`. Post-activation Split restart semantics use `--host-ranges r0,r2,r3`. Pre-activation Split phases continue to use `r0,r1`. The deferred hosted-range validation and activation recovery path must select the durable Split target before accepting r2/r3.

## Operation and workload

Every invocation creates a timestamp-and-PID tenant and operation ID and proves that the operation is absent before CLI initiation. The test creates a SHARDED ledger whose routing key deliberately alternates below and above one sealed `(table,rowid)` split boundary. Each acknowledged row records a unique sequence, routing key, and deterministic checksum in an external append-only ACK ledger.

The test invokes `crabka gres split` with explicit source boundary, left range r2, right range r3, explicit generations, and the two stable proxy endpoints. The driver loads the sealed operation and advances it with the production cutover, range-RPC, WAL-retirement, and tenant-sidecar helpers until `Completed`. There is no SIGKILL in this foundation.

## Required proofs

The foundation passes only when all of the following are independently observed:

1. The final durable tenant layout is exactly r0/r2/r3; predecessor r1 is absent and its retirement sidecar is `Parked` before completion.
2. Authenticated direct production range requests to the r2 and r3 proxy endpoints report both successors serving with the exact range IDs and generations.
3. Ownership is measured, not inferred from the registry: authenticated direct successor scans or equivalent production `RangeScanner` requests show every low-side ledger key only on r2, every high-side key only on r3, and no cross-side rows.
4. A fresh client full SQL scan after completion equals the external ACK ledger exactly: no loss, duplication, resurrection, or unacknowledged row.
5. Successor in-doubt markers are partitioned by ownership; their disjoint union equals the predecessor marker set and the canonical union digest equals the journal marker digest.
6. Broker metadata shows the predecessor r1 WAL topic absent, both exact r2/r3 generation topics present, r0 present, and an unrelated sentinel topic preserved.
7. The operation finishes within its explicit deadline and all background workload/process resources are cleaned up on success or panic.

## Failure handling and evidence

All RPC, registry, topic, ownership, and ledger mismatches fail closed with the child log and exact endpoint context. The evidence JSON records unique tenant/operation IDs, sealed boundary, exact target endpoints and generations, per-successor row counts/key ranges, marker partition/union/digest, final topic set, ACK counts, operation duration, and cleanup-relevant PIDs. A dedicated CI script builds the real binaries, runs the foundation in one process, and validates every field.

## Implementation sequence

Use test-driven development. First add a failing focused harness/test contract for r2/r3 proxies, explicit Split CLI initiation, and post-activation `r0,r2,r3` hosting. Then implement the smallest harness and driver changes required for the no-kill foundation. Run focused ownership and marker proofs, followed by the dedicated real-process CI validator. Do not add Split kill-point parameters until this foundation is green, committed, and independently reviewed.

The subsequent kill slice will reuse the foundation operation-kind abstraction while preserving all exact Move assertions. Split kill phases must retain two target descriptors, endpoints, generations, topics, ownership partitions, and marker union across restart; no phase may collapse Split into Move semantics.
