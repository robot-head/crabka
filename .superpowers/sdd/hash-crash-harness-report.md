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
