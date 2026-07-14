# G9 hash crash harness evidence slice

Status: BLOCKED ON DURABLE RAW-EVIDENCE OBSERVABILITY

## Landed harness parameterization

Commit `b510c57b` adds an explicit fail-closed `ordinary`/`hash` workload mode to the
authoritative process split-crash binary. Ordinary remains the default. The hash mode creates
real `SHARDED BY HASH (id) BUCKETS 16` tables and invokes the real CLI with the typed bucket-8
boundary (`table 50`, `rowid 0`, `--bucket 8`). An exhaustive unit test proves all nineteen kill
points retain the exact existing family, pause bound, operation bound, and restart placement while
ordinary schema-v2 and hash schema-v3 physical contracts remain distinct.

RED was the expected compile failure for the missing `SplitWorkload` type. GREEN was:

```text
cargo test --locked -p crabka-gres --test topology_process_split_crash split_workload -- --nocapture
# 1 passed
```

## Focused live RED

The bounded source-family probe used the real CLI, operator, broker, two external GRES children,
SQL protocol, and hash table:

```text
cargo build --locked -p crabka-cli -p crabka-gres
CRABKA_G8_SPLIT_CRASH=1 \
CRABKA_G8_SPLIT_WORKLOAD=hash \
CRABKA_G8_SPLIT_KILL_POINT=initiated_before_running_cas \
timeout 240s cargo test --locked -p crabka-gres \
  --test topology_process_split_crash \
  -- --exact real_process_split_crash_anywhere --nocapture
```

It failed in 8.09 seconds before split initiation at the first ordinary-only physical assertion:

```text
crates/gres/tests/topology_process_split_crash.rs:1656
assertion left == right failed
left: 4
right: 20
```

For hash tables the SQL hash value (`id`) is deliberately not the physical rowid. The existing
logical scan response exposes `rowid` and tuple bytes, but not the raw storage key or its bucket.

## Exact blocker

The external `RangeRequest` protocol has logical `ScanRange` and validated
`TimestampPrimaryInspect`, but no authenticated durable-record inspection operation. Consequently
the process test cannot observe raw `HashPrimaryVersion` keys, distinguish them from legacy
`PrimaryVersion` keys, enumerate timestamp intent/reservation sidecars, or capture the raw TXD2
descriptor bytes. The process harness keeps cache and checkpoint roots private, and cache files are
not an authoritative public evidence format. SQL rows cannot be used to infer these facts because
the requested validator must derive them from durable sources independently.

Adding schema-v3 summary fields populated from decoded SQL results would therefore create false
evidence: a broken implementation writing legacy keys could pass. The next prerequisite is one of:

1. an authenticated, generation-fenced, read-only raw durable-record inspection request returning
   exact key/value bytes for a bounded range and table/system prefix; or
2. a stable checkpoint evidence reader that opens the journal-recorded manifest and parts and
   returns their exact durable entries, including system timestamp metadata.

That prerequisite must be production code with authorization, bounds, restart semantics, and
tests. Once present, the harness can add the bucket-0..15 corpus, 8/8 folds, committed/abandoned
remote transaction proof, schema-v3 validator negatives, and hash-family wrappers without
inferring durable facts from logical results.

`crates/gres-ranges/src/control.rs` was not edited or staged.

## Rollback cleanup RED (2026-07-13)

The next focused real-child setup added a two-participant hash transaction on the pinned bucket-0
and bucket-8 values `7` and `15`, followed by an explicit SQL `ROLLBACK`. Authoritative inspection
proved the TXD2 descriptor is terminal `Aborted`, has participants `[0, 1]`, and carries exactly
the operation buckets `{0, 8}`. It also exposed a blocking mismatch with the requested schema-v3
contract: both primary records remain durably present as `HashPrimaryVersion` values whose tuple
state is `Aborted`. They are absent from SQL visibility, but they are not absent from raw storage.

The strict RED initially failed in the committed-only decoder with:

```text
decode authoritative hash row: "hash primary version is not committed: Aborted"
```

The inspection was then made state-aware solely to characterize the prerequisite. The bounded
live command passes while asserting the exact aborted raw set `{(7, bucket 0), (15, bucket 8)}` and
the exact aborted descriptor:

```text
CRABKA_G9_HASH_INSPECT=1 timeout 120s cargo test --locked -p crabka-gres \
  --test topology_process_split_crash \
  -- --exact real_child_hash_durable_inspection_covers_pinned_bucket_corpus --nocapture
# 1 passed; 3.74s live test time including restart recovery
```

The evidence contract is now clarified to follow timestamp MVCC: rolled-back rows must be absent
from SQL visibility, while raw primary versions may remain only in terminal `Aborted` state until
garbage-horizon pruning. After restarting the real child with both ranges hosted, authoritative
inspection still proves exactly the two aborted bucket records, the terminal aborted TXD2
descriptor, and no pending descriptor. Schema-v3 must encode and independently validate that
logical/raw distinction, plus the absence of unresolved intent/reservation sidecars.

## Resume after durable-inspection prerequisite (`586a5648`)

The authenticated `InspectDurableRecords` prerequisite is now present and the prior observability
blocker is resolved. Commit `01826ef1` adds a focused real-child test using a real hash table and
the pinned SQL values `0..15`.

RED was an exact placement counterexample: inspecting only r1 returned buckets `1..15`; bucket 0
was on r0 under the existing initial boundary. GREEN inspects the authoritative r0/r1 union and
proves buckets `0..15` exactly once, every table record is `HashPrimaryVersion` (no legacy primary
class), every record has a source WAL offset and journal revision, pagination is complete, and the
sample provenance is valid:

```text
CRABKA_G9_HASH_INSPECT=1 timeout 120s cargo test --locked -p crabka-gres \
  --test topology_process_split_crash \
  -- --exact real_child_hash_durable_inspection_covers_pinned_bucket_corpus --nocapture
# 1 passed; 2.06s live test time
```

The next strict RED remains in the ordinary-only payload model, not in child readiness. The live
hash probe previously demonstrated `id = 4` and physical `rowid = 20`, while `PayloadEvent`,
`PhysicalPayloadRow`, pre-split assertions, successor ownership, and the Python schema-v2 validator
all require `id == rowid`. Hash schema-v3 must carry both logical hash value and physical rowid plus
the independently decoded bucket/key class. It must replace those assumptions coherently before a
kill run; selectively skipping them would weaken the authoritative gate.

Remaining commands after that schema-v3 conversion are:

```text
python3 scripts/tests/validate-gres-split-crash-evidence.py --self-test
cargo test --locked -p crabka-gres --test topology_process_split_crash
CRABKA_G8_SPLIT_WORKLOAD=hash scripts/tests/gres-topology-process-split-source-restore-ci.sh
CRABKA_G8_SPLIT_WORKLOAD=hash scripts/tests/gres-topology-process-split-publication-ci.sh
CRABKA_G8_SPLIT_WORKLOAD=hash scripts/tests/gres-topology-process-split-retirement-ci.sh
```
