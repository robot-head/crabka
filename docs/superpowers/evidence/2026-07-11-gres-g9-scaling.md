# Gres G-9 live scaling evidence — 2026-07-11

Environment-qualified local evidence only; CI now repeats the same fast live workload on every relevant PR and gates on its own runner-local measurements.

Command:

```bash
CRABKA_GRES_SKIP_BUILD=1 \
CRABKA_GRES_RANGE_SCALING_MODE=fast \
CRABKA_GRES_RANGE_SCALING_ARTIFACT_DIR=target/gres-scaling-task3-final \
CRABKA_GRES_RANGE_SCALING_KEEP_ARTIFACTS=1 \
./scripts/gres-range-scaling.sh
```

Fresh result on host `clod`, Linux 7.0.0-27-generic x86_64, Python 3.14.4, one session/range and two transactions/session:

- artifact mode: `live`
- range-local 4-range / 1-range: `3.7538` (passed)
- sharded G-9 4-range / 1-range: `3.2558` (passed)
- measured G-9 range-4 / retained G-8 decision-ceiling contrast: `1.6279`, unflattened (passed)
- all JSON gates: passed

The local command reused already-built debug binaries (`CRABKA_GRES_SKIP_BUILD=1`). The CI job does not set that variable: `scripts/gres-range-scaling.sh` therefore executes its locked Cargo build before measuring.

An immediately preceding live-fast run at `target/gres-scaling-current/range-scaling.json` measured range-local `3.5556`, sharded `3.0588`, and G-9/G-8 ceiling `1.5294`. These small fast workloads are scheduling-sensitive; the proof is the environment-qualified threshold result and retained raw artifact, not cross-host equality of point estimates.

The JSON publishes both curves:

- `sharded_table.timestamp_transactions.commit_rate_curve` is the current measured G-9 curve.
- `sharded_table.decision_ceiling.g8_decision_ceiling_curve` retains the prior flattened G-8 ceiling as an explicitly synthetic contrast generated from the same one-range baseline.

## Robust rerun correction

The earlier two-transaction measurements above are retained as historical evidence but are not a valid commit-scaling proof: fresh `psql` startup dominated them. The corrected persistent-session workload (two sessions/range, five warmups, 50 measured transactions/session, three trials, median throughput) measured:

- range-local: 515.4639, 917.4312, 1666.6667 tx/s for 1/2/4 ranges (`3.2333x`, passes 2.5 floor);
- sharded: 155.7632, 154.6790, 149.7006 tx/s (`0.9611x`, fails 2.5 floor).

Command: `CRABKA_GRES_SKIP_BUILD=1 CRABKA_GRES_RANGE_SCALING_MODE=fast CRABKA_GRES_RANGE_SCALING_ARTIFACT_DIR=target/gres-scaling-task3-review2 ./scripts/gres-range-scaling.sh`.

This environment-qualified result fails the intended G-9 scaling gate and is reported rather than relabeled as passing. The corrected CI job will likewise fail until the sharded commit bottleneck is removed or the intended multi-process topology is represented by the harness.

## Primary-range remediation and final robust rerun

The flat result above exposed two product/harness defects: sharded workers used table-ID boundaries rather than row-ID boundaries, and timestamp decisions were still anchored on range 0. The corrected path uses `table:rowid` boundaries, assigns the first-write range as the immutable timestamp primary, atomically persists its pending descriptor with primary intents, and resolves/recoveries through that primary. Timestamp leases and epoch-liveness certificates remove per-write sequence allocation and per-grant heartbeats from the steady-state path.

Fresh command:

```bash
CRABKA_GRES_SKIP_BUILD=1 \
CRABKA_GRES_RANGE_SCALING_MODE=fast \
CRABKA_GRES_RANGE_SCALING_ARTIFACT_DIR=target/gres-scaling-primary-range-final \
./scripts/gres-range-scaling.sh
```

The robust workload used two persistent sessions/range, five warmups, 50 measured transactions/session, three trials, and median aggregation. The retained artifact reports:

- range-local: 564.9718, 1030.9278, 1860.4651 tx/s (`3.2930x`, passed);
- sharded: 248.1390, 455.5809, 852.8785 tx/s (`3.4371x`, passed);
- range-4 measured/envelope efficiency: `0.8593` (passed);
- range-4 G-9/G-8 decision-ceiling contrast: `1.7185`, unflattened;
- primary distribution: `{0: 100}`, `{0: 100, 1: 100}`, then `{0: 100, 1: 100, 2: 100, 3: 100}`;
- all JSON gates: passed.

## Independent review remediation

The follow-up robust run at `target/gres-scaling-observed-final/range-scaling.json` replaces the earlier formula-derived distribution with runtime observations emitted only after the primary decision becomes durable. It reports 110 observed primaries on r0 for one range, 110 each on r0/r1 for two ranges, and 110 each on r0/r1/r2/r3 for four ranges (measured plus warmup transactions). The artifact rejects missing ranges or an observation total different from the executed workload, and the static gate rejects replacing the observation marker with a workload formula.

This run preserved every threshold: range-local `3.2110x`, sharded `3.3473x`, range-4 envelope efficiency `0.8368`, and all JSON gates true.

Recovery and fencing review tests additionally cover the prewrite-before-primary-ack crash window, rN-only recovery through an authenticated remote primary, forged primary identity/terminal decisions, and conditional descriptor CAS no-op races.

Post-remediation broad verification passed 341 pgexec library tests, 36 pgexec transaction tests, 114 gres-ranges library tests, 21 crossrange tests, 33 multirange tests, the targeted CLI row-boundary parser test, formatting/diff checks, and the static scaling mutation suite. Gateway-local retained its single known invalid-socket fixture failure (10 passed), and strict clippy retained its known pgwire semicolon lint blocker; neither file is part of this remediation.

Second-review hardening removes the remaining recovery trust edge: participant recovery authenticates the full identity and reads the effective decision plus operation set from the actual primary before applying anything. Wrong-primary/global-xid aborts and forged commit timestamps/operations return `40001` without changing the local intent. The distribution validator also rejects the concrete skewed observation `{0:437,1:1,2:1,3:1}` because this uniform workload requires exactly 110 observed primaries per range.

The fresh live artifact at `target/gres-scaling-auth-recovery-final2/range-scaling.json` reports range-local `3.2778x`, sharded `3.4144x`, range-4 envelope efficiency `0.8536`, exact per-range counts of 110, and every gate true.

Final broad verification after these changes passed 341 pgexec library tests, 36 pgexec transaction tests, 116 gres-ranges library tests, 21 crossrange tests, 33 multirange tests, formatting/diff checks, the targeted CLI parser, and the static skew/mutation suite. Gateway-local remains 10/11 only for the known invalid-socket fixture at its shifted line 1136.

Third-review hardening separates read-only primary inspection from startup recovery. Normal local and remote secondary resolution now inspect the full primary descriptor without choosing a decision, require a terminal effective outcome and exact participant operations, and reject opposite or Pending-primary assertions without changing either primary or secondary state. `TimestampPrimaryRecover` remains exclusive to explicit startup orphan recovery.

The fresh artifact at `target/gres-scaling-readonly-inspect-final/range-scaling.json` passes every gate with range-local `3.2407x`, sharded `3.4701x`, envelope efficiency `0.8675`, and exact observed distributions of 110 transactions per range.

The final operation-order hardening canonicalizes both asserted and primary-descriptor operations by `(range_id, table_id, rowid, delete)` through one shared helper. Descending valid multi-row lists now settle locally and remotely; duplicate assertions fail with `40001` and leave every intent untouched. The final relevant suites pass 122 gres-ranges library tests, 21 crossrange tests, 33 multirange tests, and the focused remote restart test.
