# G8 two-successor Split foundation evidence

Date: 2026-07-12

The dedicated process test drives the real `crabka gres split` CLI request through the production operator reconciler and authenticated range-control transport. It proves that predecessor r1 is replaced by distinct serving successors r2 and r3, then retires only r1's WAL topic.

## Workload and ownership

The fsynced external ACK ledger records 32 tuples `(seq, route_key, checksum)`. `route_key` deliberately alternates between the low and high halves over time, so the user payload preserves the requested cross-boundary temporal pattern.

GRES range ownership is authoritative on the hidden physical `(table_id, rowid)` key, not a user column named `route_key`. The test therefore validates the sealed table-51 split at physical rowid 16: r2 contains exactly physical rowids 1 through 15 and r3 contains exactly rowids 16 through 32. Both direct successor scans decode and compare the complete payload to the fsynced ACK ledger; a fresh post-restart SQL scatter scan also equals that ledger exactly.

## Marker proof

The production `InheritMarkers` response now carries the predecessor marker union and explicit left/right successor partitions. A transparent recording wrapper around the production mTLS mutation client captures the live authenticated response without sending an extra RPC.

The test requires exactly one captured r1 generation-0 request, a nonempty journal revision/digest receipt, response digest equality with the durable operation and retirement evidence, exact `left + right == predecessor`, disjoint successor partitions, and interval membership for every marker.

This live workload has no in-doubt transactions, so all three captured sets are empty. The assertion is still production-path evidence rather than an inferred empty value. Nonempty partition behavior remains covered by the marker partition unit/model tests in `crates/gres/src/live_range_control.rs`.

## Reproducible gates

Run the full build, live foundation, and strict JSON validator:

```text
scripts/tests/gres-topology-process-split-foundation-ci.sh
```

Observed result on 2026-07-12: one exact live test passed in 49.55 seconds and the generated evidence JSON passed validation.

The validator's negative gate was also exercised against `/dev/null`; parsing failed and the command returned nonzero, proving malformed or missing evidence cannot pass.

Focused schema and compile gates:

```text
cargo test -q -p crabka-gres-ranges transport::tests --lib --no-run
cargo test -q -p crabka-operator controller::gres_split_operation --lib --no-run
cargo test -q -p crabka-gres --test topology_process_nemesis --no-run
```

All three completed successfully. The live JSON additionally requires target layout `[0,2,3]`, distinct r2/r3 endpoints, generation 1 on both successors, exact row counts 15 and 17, zero cross-side rows, 32-row SQL/ACK equality, the authenticated marker receipt, predecessor topic absence, successor topic presence, one predecessor deletion, sentinel-topic survival, and a bounded operation duration.
