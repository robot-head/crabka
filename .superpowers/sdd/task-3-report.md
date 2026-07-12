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
