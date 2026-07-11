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
