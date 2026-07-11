# G-2 live/fault gate report — 2026-07-11

Scope: Chapter Gres G-2 substrate WAL only. This report does not claim G-3 or
later chapters complete.

## Closed contracts

- Production `ProducerWalWriter` distinguishes proven broker rejection from
  indeterminate `EndTxn`; transport/unknown commit and abort failures invoke a
  fatal compute handler and never resolve the commit caller. A fenced producer
  is a proven rejection and cannot be mislabeled indeterminate.
- A deterministic delivery-result fault runs through the production pending
  send-error branch, completes broker abort, fails that group, and the next
  group commits. Fresh recovery proves the aborted group's row is absent.
- Live recovery now applies Kafka's `aborted_transactions` producer-id/control
  marker algorithm. Before this fix, the new transient test failed with
  `SequenceGap { expected: 1, found: 0 }` because recovery replayed an aborted
  transactional batch.
- A deterministic after-commit ambiguous outcome and a deterministic abort
  failure both leave the caller unanswered. A fenced successor resolves truth
  from the committed log.
- Raw `READ_UNCOMMITTED` batch inspection captures real `(producer_id,
  producer_epoch)` pairs and proves no stale identity's data batch occurs after
  the successor barrier. A third recovery sees only the first and successor
  rows.
- A real >1 MiB logical group (two 700,000-byte values) crosses the 1 MiB frame
  cap, commits atomically as two WAL frames, and recovers both values. The
  process gate writes the same two-row group before `SIGKILL`, then proves both
  rows survive in the cache-empty successor.
- The native three-node broker fixture explicitly compares `FindCoordinator`
  broker id with the WAL partition leader id and selects only a differing pair.
  Client producer registration now sends `AddPartitionsToTxn` to the remote
  coordinator; the partition leader defers remote epoch truth to `EndTxn`.
  Stale commit fails, successor commit succeeds, and a third recovery is exact.
- The process smoke provisions a real tenant, drives pgwire with `psql`, writes
  acked and 1.4 MB SQL state, holds an uncommitted transaction at a protocol
  response marker, sends `SIGKILL` to the compute, starts a cache-empty
  successor, proves no unacked resurrection, and accepts a successor write.

## Verification evidence

- `cargo nextest run -p crabka-client-producer`
  - PASS: 63/63, 0 skipped.
- `RUST_LOG=off cargo nextest run -p crabka-gres-substrate`
  - PASS: 128/128, 0 skipped, including six live writer/fault tests and the
    explicit three-node coordinator-not-leader test.
- `cargo nextest run -p crabka-broker --test transactions --test transaction_version`
  - PASS: 13/13, 0 skipped.
- `cargo clippy -p crabka-client-producer -p crabka-broker -p crabka-gres-substrate --all-targets -- -D warnings`
  - PASS after mechanical `assert_eq!`/`debug_assert_eq!` modernization required
    by the current clippy toolchain.
- `cargo +nightly fmt --all`
  - PASS; subsequent check is clean.
- `cargo build --locked -p crabka-cli -p crabka-broker -p crabka-gres`
  - PASS.
- `CRABKA_GRES_SKIP_BUILD=1 timeout 90s ./scripts/gres-substrate-smoke.sh`
  - PASS: `abrupt-loss replay preserved acked+oversized state, rejected unacked
    state, and accepted successor writes`.
- `bash -n scripts/gres-substrate-smoke.sh`
  - PASS.

The multi-broker test raises its soft file-descriptor limit to 8192 (within the
host hard limit) before starting three in-process brokers. At the default soft
limit of 1024, the broker's replicated internal transaction partitions exhaust
descriptors; the test performs this bounded capability setup itself rather than
silently skipping.

The nextest configuration caps all `crabka-gres-substrate` test binaries at two
threads in both default and CI profiles. Shell readiness and cleanup use
absolute deadlines; external CLI/psql calls are command-timeout bounded and
cleanup escalates to `SIGKILL` before a bounded wait.
