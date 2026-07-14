# Task 3 report: live scaling CI closure

## Outcome

- Converted `gres-range-scaling` from synthetic, non-gating dry-run CI to a real live-fast gate.
- Installed Rust 1.96, Rust cache, and `postgresql-client`; the script builds required binaries with `cargo build --locked` in CI.
- Removed `continue-on-error`, raised timeout from 5 to 30 minutes, retained artifact upload on failure, and added a post-run JSON assertion for `mode == live` plus every threshold gate.
- Kept the G-8 flattened decision-ceiling contrast and current G-9 measured commit-rate curve in the per-PR JSON.
- Added a static anti-rot test and wired it into the Gres path filter/job.
- Added dated, environment-qualified live evidence.

## TDD

`bash scripts/tests/gres-range-scaling-ci.sh` failed against the old workflow because the job lacked a stable toolchain/cache, PostgreSQL client, fast/live mode, and gating semantics. After the workflow change it prints `PASS: live Gres scaling CI contract`.

## Verification

```text
bash scripts/tests/gres-range-scaling-ci.sh
bash -n scripts/gres-range-scaling.sh
bash -n scripts/tests/gres-range-scaling-ci.sh
```

All passed.

Fresh live-fast command:

```text
CRABKA_GRES_SKIP_BUILD=1 CRABKA_GRES_RANGE_SCALING_MODE=fast CRABKA_GRES_RANGE_SCALING_ARTIFACT_DIR=target/gres-scaling-task3-final CRABKA_GRES_RANGE_SCALING_KEEP_ARTIFACTS=1 ./scripts/gres-range-scaling.sh
```

Result: exit 0; live mode; range-local 4x/1x `3.7538`; sharded 4x/1x `3.2558`; range-4 G-9/G-8 ceiling ratio `1.6279`; every JSON gate true.

## Self-review

- CI does not reuse local binaries; the skip-build variable appears only in documented local evidence.
- Artifact upload remains `if: !cancelled()`, so measurement or gate failures still publish diagnostics.
- The script allocates three distinct ephemeral ports per run and tears down Gres before reusing its chosen port across tenant cases. No duplicate mutable port locals or same-run port reuse was found.
- The fast live workload is deliberately small and noisy; thresholds and raw artifacts, not exact ratios, are the gate.

## Concerns

- GitHub-hosted runner scheduling noise may occasionally require increasing the fast sample count; the current 30-minute timeout leaves room to do so without reverting to synthetic evidence.

## Scaling review remediation

- Replaced one-`psql`-process-per-transaction with persistent worker sessions using one timed SQL stream, explicit warmup markers, and 50 measured commits/session.
- Fast mode now runs two sessions/range and three full trials. Canonical results use median throughput, embed every raw trial, record warmup/sample/trial counts, and state the aggregation derivation.
- Corrected shard-key spacing so each range receives its intended workers; the robust run exposed that the previous tiny apparent sharded scaling was not valid.
- Replaced substring-only anti-rot checks with an exact job-block parser and negative mutations for invocation removal, live assertion removal, expression-valued continue-on-error, comment-only fast mode, and upper-envelope removal.
- Expanded Gres path filtering to broker/client/protocol dependencies plus root Cargo/toolchain inputs.
- Decision-envelope success now requires every point to remain within both 0.70 and 1.25 bounds.

Static checks and dry-run compatibility pass. The required fresh robust live run completed all trials but correctly exited nonzero: range-local scaled `3.2333x`, while sharded scaled only `0.9611x` (raw medians 155.7632 to 149.7006 tx/s). This is a genuine remaining product/topology scaling failure, not a harness error that can be honestly papered over in CI.

## Primary-range architecture closure

- Extended range-layout parsing with lexicographically validated `table:rowid` boundaries and changed the sharded benchmark to use row-ID cuts. The static anti-rot test mutates this contract back to table-only cuts and requires failure.
- Made the first-write range the immutable timestamp primary for autocommit and explicit transactions. Primary prewrite atomically stores the pending descriptor and primary intents; secondary prewrite, participant expansion/acknowledgement, terminal resolution, dropped-session cleanup, and startup recovery all target that primary locally or over authenticated RPC.
- Added startup recovery for descriptors and durable intent sidecars on every hosted range, including abort-won reconstruction for orphaned pending intents and fail-closed behavior when their primary is unreachable.
- Added timestamp write leases, removed hidden-rowid sequence commit operations, wrapped TSO RPC with batching, and added a 10 ms epoch-liveness certificate that rechecks fencing on expiry.
- Replaced the per-table DML mutex with an RW lock: concurrent DML takes shared access while split/conversion retains exclusive fencing.

Final robust live result at `target/gres-scaling-primary-range-final/range-scaling.json`: range-local `3.2930x`, sharded `3.4371x`, decision-envelope efficiency `0.8593`, G-9/G-8 range-4 contrast `1.7185`, and every gate true. The artifact records balanced primary distributions across all expected ranges.

## Independent review hardening

- `3f99f0a9` aborts durable secondary intents from the exact prewrite-before-primary-add/ack crash window by start timestamp, clearing reservations and identity/index sidecars even when descriptor operations are absent.
- `0a9cae36` lets rN-only startup authenticate and settle through a registry-resolved remote primary; unknown, fenced, or unreachable primaries still fail readiness closed.
- `6ecf7201` validates full primary identities for add/ack and makes secondary runtime resolution read the effective terminal outcome directly from the hosted or authenticated remote primary.
- `b8be230c` rereads descriptor state after primary prewrite/resolve CAS batches and fences conditional no-op races.
- Runtime `timestamp_primary_committed` observations now replace the benchmark's derived distribution formula. The final robust artifact reports balanced observed counts of 110, 220, and 440 transactions across 1/2/4 ranges and passes at `3.2110x` range-local and `3.3473x` sharded scaling.

Post-remediation verification: formatting and diff checks passed; pgexec library `341/341` and transaction integration `36/36`; gres-ranges library `114/114`, crossrange `21/21`, and multirange `33/33`; gateway-local `10/11` with only the pre-existing invalid-socket fixture failure at `gateway_local.rs:1110`; targeted CLI row-boundary parsing `1/1`; static scaling contract and negative mutations passed; robust live artifact passed every threshold. Clippy remains blocked only by the verified pre-existing `semicolon_if_nothing_returned` lint in `crates/pgwire/src/engine.rs:297`.

## Second-review recovery and distribution closure

- `TimestampRecover` now treats its identity, decision, and operations as assertions only. It authenticates the complete identity against the actual hosted or registry/mTLS-resolved primary, obtains the primary descriptor's terminal decision and operations, rejects missing/pending/mismatched outcomes with `40001`, and only then mutates the participant using primary-derived state. Forged abort and forged commit/operation tests prove the local intent remains unchanged.
- The gateway restart fixture now exposes the local primary through a real TLS range endpoint, so remote-secondary recovery exercises direct primary authentication rather than caller trust.
- `scripts/check-gres-primary-distribution.py` requires the exact uniform count per range, exact range-id set, and exact total. A concrete `{437,1,1,1}` artifact fails with `expected 110 per range`.
- Fresh robust artifact `target/gres-scaling-auth-recovery-final2/range-scaling.json` passes all gates: range-local `3.2778x`, sharded `3.4144x`, envelope efficiency `0.8536`, and observed distributions exactly 110 per range.

Final second-review verification passed formatting/diff checks, pgexec library `341/341`, pgexec transactions `36/36`, gres-ranges library `116/116`, crossrange `21/21`, multirange `33/33`, the targeted CLI parser, and the static mutation/skew suite. Gateway-local remains `10/11` solely because of the existing invalid-socket fixture now at `gateway_local.rs:1136`.

## Third-review read-only inspection closure

- Added `TimestampPrimaryInspect`, a distinct read-only full-descriptor RPC. Normal secondary resolution and `TimestampRecover` use inspection; only the explicit startup recovery coordinator invokes `TimestampPrimaryRecover` and may abort-win a pending primary.
- A remote pending-primary RED changed the primary to Aborted merely by checking a forged Commit assertion. It now returns `40001` while the primary remains Pending and the secondary remains Intent.
- Local hosted secondaries now independently validate full primary identity, terminal decision, and exact participant operations before `resolve_as_secondary`. Opposite Commit/Abort and Pending-primary RED cases leave the intent unchanged.
- Verification: gres-ranges library `119/119`, crossrange `21/21`, multirange `33/33`, focused remote restart `1/1`, static gate, formatting/diff checks, and gateway-local `10/11` with only the known invalid-socket fixture.
- Fresh robust artifact `target/gres-scaling-readonly-inspect-final/range-scaling.json` passes all gates: range-local `3.2407x`, sharded `3.4701x`, envelope efficiency `0.8675`, and exact 110-per-range observations.

## Canonical operation assertions

Local and remote secondary validation now share one canonicalizer that sorts by `(range_id, table_id, rowid, delete)` and rejects exact duplicates before comparison. Valid multi-row operations resolve regardless of input order, while a duplicated forged operation list returns `40001` without changing either intent. Verification passed gres-ranges library `122/122`, crossrange `21/21`, multirange `33/33`, focused remote restart `1/1`, formatting/diff checks, and the static scaling suite.
