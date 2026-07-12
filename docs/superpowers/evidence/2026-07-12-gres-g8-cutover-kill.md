# Gres G8 Move cutover-phase process evidence

The real-process Move shard SIGKILLs the source Gres child only after exact durable journal,
evidence, tenant-layout, and retirement-sidecar predicates. Every run replaces the PID, reconnects
fresh clients, recovers one deliberately ambiguous acknowledgement, commits on the successor
before retirement, completes retirement and WAL cleanup, and proves the final database exactly
equals the fsynced acknowledgement ledger.

Three-run observations establish phase-specific acknowledgement-gap ceilings:

| Kill point | Observed gaps (ms) | Operation times (ms) | CI ceiling (ms) |
| --- | --- | --- | --- |
| Restored | 16,235 / 15,720 / 15,553 | 43,592 / 42,726 / 42,975 | 20,000 |
| ActivatedBeforeCutover | 9,457 / 9,482 / 9,564 | 39,103 / 39,462 / 40,803 | 12,000 |
| ActivatedAfterTenantCas | 9,943 / 9,363 / 9,553 | 41,118 / 39,246 / 40,355 | 12,000 |
| LayoutPublished | 9,560 / 9,131 / 9,567 | 39,795 / 40,328 / 39,369 | 12,000 |

ActivatedBeforeCutover exposed two recovery boundaries. Non-range-zero activation now retains r0
as receipt authority while reconstructing the exact sealed successor set. Until the authoritative
tenant registry reaches the target CAS, scan/forward routing uses an exact must-activate overlay;
refresh may converge to the identical target but cannot regress to the predecessor or accept a
third layout. Historical timestamp primary identities are aliased only for a one-target replacement
covering the exact predecessor interval; ambiguous two-successor aliases fail closed.

ActivatedAfterTenantCas additionally snapshots the target tenant record and Parking sidecar before
replay, then proves `reconcile_activated_cutover` advances only the journal to LayoutPublished:
tenant version, layout, and retirement sidecar remain unchanged.

Command:

```text
scripts/tests/gres-topology-process-cutover-nemesis-ci.sh
```
