# Task 2 report: explicit timestamp-transaction sessions

## Status

Implemented and committed a persistent gateway timestamp-transaction session for explicit SQL transactions after the first cross-range sharded timestamp write.

## Behavior implemented

- Added a typed `GatewayTransaction::Timestamp` state containing one durable `TimestampTxnIdentity`, structurally accumulated per-range `TimestampWrite` vectors, and accumulated statement bookkeeping `WriteOp`s. SQL text is never buffered or concatenated.
- Cross-range sharded inserts inside `BEGIN` now prewrite and acknowledge participants without deciding. Later sharded writes to an already declared participant, including single-range statements, reuse the same identity.
- `COMMIT` allocates one commit timestamp, writes the range-0 descriptor decision once, resolves every accumulated participant, and folds statement bookkeeping only after the durable decision.
- `ROLLBACK` and statement failure durably choose abort and resolve all accumulated intents. A failed timestamp session then obeys PostgreSQL's `25P02` behavior until rollback.
- Added typed read-your-writes plumbing (`own_start_ts`) through `SqlSession`, `TimestampedRangeScanner`, local scans, the registry scanner, the scan RPC wire request, and the range owner. A pending intent is visible only when its verified descriptor is pending and its `start_ts` matches the reader's typed owner identity.
- Kept unsharded and ordinary G7 paths separate. Existing non-timestamp explicit transaction handling is unchanged except removal of the obsolete unit-test expectation that explicit cross-range sharded insertion must fail.
- Preserved descriptor-driven recovery: the in-memory session contains no independent decision authority, and failed/rollback cleanup uses the existing durable descriptor and participant operations.

## TDD evidence

RED:

- `cargo test -p crabka-gres-ranges --test multirange sharded_multi_row_insert -- --nocapture`
  - Initial expected failure: both new explicit tests returned `0A000` (`multi-range sharded timestamp DML is only supported in autocommit`).
- After session prewrite accumulation, the same command produced the expected intermediate failure: read-your-writes returned `[]` instead of `[10, 30, 20, 40]`.
- `cargo test -p crabka-gres-ranges --test multirange failed_explicit_timestamp -- --nocapture`
  - Expected failure before failed-session cleanup: the post-error SELECT succeeded and exposed the pending rows instead of returning `25P02`.

GREEN:

- Focused explicit session tests pass after adding typed owner visibility and failed-session abort.
- The multi-statement test was strengthened so the second and third statements are individually single-range but remain under the identity established by the first cross-range statement.

## Final verification

Fresh final command:

```text
cargo test -p crabka-gres-ranges --lib &&
cargo test -p crabka-gres-ranges --test multirange &&
cargo test -p crabka-gres-ranges --test sharded_visibility &&
cargo test -p crabka-pgexec --lib &&
cargo check -p crabka-gres-ranges --all-targets &&
git diff --check
```

Results:

- `crabka-gres-ranges --lib`: 105 passed, 0 failed.
- `multirange`: 26 passed, 0 failed.
- `sharded_visibility`: 7 passed, 0 failed.
- `crabka-pgexec --lib`: 336 passed, 0 failed.
- all-target check: success.
- diff whitespace check: success.

Additional focused run:

- `cargo test -p crabka-gres-ranges --test multirange sharded_multi_row_inserts_in_explicit -- --nocapture`: 1 passed.

## Self-review findings addressed

- Fixed a commit-error path that initially moved the participant set out of the gateway state too early, which would have prevented the common failed-statement cleanup from aborting it.
- Added remote scan wire propagation after noticing that the first RYW implementation covered only in-process range scanning.
- Routed later single-range sharded statements through the already-open timestamp identity instead of accidentally falling back to ordinary/autocommit execution.

## Review-fix wave

The requested review fixes were applied in a second TDD wave:

- A single-range first sharded write now starts the timestamp session. `TimestampTxnDescriptor::add_participant` and `SqlEngine::add_timestamp_transaction_participant` durably expand pending participants with generation-fenced CAS, making later new-range statements legal.
- Bound extended-protocol sharded DML is materialized as typed bound SQL at Bind and executed once at the gateway through the same structural timestamp write path. It no longer enters an owner-local transaction or bypasses accumulation.
- Dropping a gateway session transfers its timestamp identity, participants, serving snapshot, and remote participant handles to a runtime cleanup task. Cleanup chooses the descriptor's write-once effective decision and resolves every participant; the integration test proves conflicting writes can immediately proceed after a dropped session yields.
- Explicit statement bookkeeping is durably committed before statement success rather than retained solely in gateway memory until after the primary decision. A crash after the decision can therefore reconstruct/resolve the descriptor without losing sequence bookkeeping.
- Failed-statement cleanup now clones rather than moves timestamp recovery state before fallible cleanup. The identity and participants remain available if cleanup errors.
- Explicit COMMIT uses the existing before/after-decision fault points. Abort cleanup observes the write-once primary result, so an after-decision failure resolves as committed rather than changing the decision to abort.
- Stateful remote SQL sessions gained a typed `SetTimestampOwner` operation, in addition to scan-RPC owner propagation, so directly routed remote queries can read their transaction's pending intents.
- The catalog-wide table scan was removed; sharded routing checks now use the statement's table reference and the existing catalog lookup helper.

### Added RED evidence

- `cargo test -p crabka-pgexec pending_descriptor_adds --lib` initially failed to compile because `add_participant` did not exist.
- `cargo test -p crabka-gres-ranges --test multirange explicit_timestamp_transaction_expands -- --nocapture` initially failed with `0A000` on the first single-range write.
- `cargo test -p crabka-gres-ranges --test multirange extended_sharded_writes_join -- --nocapture` initially failed with `0A000` because extended scatter was rejected at Bind.
- `cargo test -p crabka-gres-ranges --test multirange dropping_explicit_timestamp -- --nocapture` initially failed with `40001` because Drop leaked the abandoned intents.

### Review-wave final verification

```text
cargo test -p crabka-gres-ranges --test multirange &&
cargo test -p crabka-gres-ranges --test sharded_visibility &&
cargo test -p crabka-gres-ranges --lib &&
cargo test -p crabka-pgexec --lib &&
cargo check -p crabka-gres-ranges --all-targets &&
git diff --check
```

Results:

- multirange: 29 passed, 0 failed.
- sharded_visibility: 7 passed, 0 failed.
- gres-ranges lib: 105 passed, 0 failed.
- pgexec lib: 337 passed, 0 failed.
- all-target check and diff check: passed.

## Second review-fix wave

- Replaced the no-runtime Drop gap with an owned cleanup executor. Runtime-owned drops spawn cleanup and trace failures; no-runtime drops start a named current-thread Tokio runtime on an owned thread, synchronously join it, and trace executor/runtime/cleanup failures with `start_ts`. Every failure log states that the durable descriptor remains recovery authority.
- Extended Execute now sets or clears `timestamp_own_start_ts` immediately before local and remote query execution. A bound extended SELECT in the explicit transaction sees pending timestamp intents.
- Generalized bound parameter SQL rendering independently of shard-key routing. It now validates OIDs and formats and supports bool, int2/int4/int8, text/varchar/char, bytea, float4/float8, numeric, date/time/timestamps/interval, and UUID literals. Binary bool/integer/floating/bytea encodings are rendered without treating non-shard parameters as shard keys.
- Added bytea assignment coercion for the materialized typed extended path by reusing pgexec's existing bytea text decoder.

### Second-wave RED evidence

- Extended SELECT initially returned no pending row because Execute did not install the owner identity.
- The bool-text/bytea-binary/float8-binary INSERT initially failed at Bind with `0A000`, then exposed bytea materialization type errors until the existing decoder was reused at assignment.
- The no-runtime Drop test initially represented a silent cleanup leak; the fallback executor now resolves intents before Drop returns.

### Second-wave verification

```text
cargo test -p crabka-gres-ranges --test multirange &&
cargo test -p crabka-gres-ranges --test sharded_visibility &&
cargo test -p crabka-gres-ranges --lib &&
cargo test -p crabka-pgexec --lib &&
cargo check -p crabka-gres-ranges --all-targets &&
git diff --check
```

Results: multirange 31/31, sharded_visibility 7/7, gres-ranges lib 105/105, pgexec lib 337/337; all-target check and diff check passed.
