#!/usr/bin/env bash
# Run the full benchmark matrix from WSL.
#
# Usage: bash bench/run-matrix.sh [TOPOLOGY] [SCENARIOS]
#   TOPOLOGY    1broker-rf1 | 3broker-rf3 | 6broker-rf3   (default 3broker-rf3)
#   SCENARIOS   space-separated scenario basenames        (default: cluster set)
#
# Env vars:
#   RUNS                 how many repeats to do THIS invocation (default 10).
#                        Each repeat tags its output files "-runNN" so nothing
#                        is clobbered; the report averages all runs per cell.
#   RUN_START            first repeat index (default 1). Lets a run be resumed /
#                        split across invocations: a 1-repeat smoke with
#                        RUN_START=1 RUNS=1 writes run01, then RUN_START=2 RUNS=9
#                        writes run02..run10 — 10 distinct runs in total.
#   BENCH_DRIVER_IMAGE   driver Job image ref.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Use native Linux kubectl installed in ~/.local/bin (kubectl.exe can't
# resolve WSL /mnt/c/... paths when called from bash scripts).
export PATH="$HOME/.local/bin:$PATH"

# Point at the Windows-side kubeconfig so we can reach the GKE cluster.
export KUBECONFIG="${KUBECONFIG:-/mnt/c/Users/Matt Stone/.kube/config}"

# Verify kubectl works
kubectl version --client 2>/dev/null || { echo "ERROR: kubectl not available"; exit 1; }
kubectl cluster-info 2>&1 | head -1 || { echo "ERROR: cannot reach cluster"; exit 1; }

export BENCH_DRIVER_IMAGE="${BENCH_DRIVER_IMAGE:-us-central1-docker.pkg.dev/robot-head/crabka/bench-driver:custom}"
TOPOLOGY="${1:-3broker-rf3}"
SCENARIOS="${2:-small-msg-saturate fixed-rate-latency large-msg fan-out mixed-acks failover high-partition-saturate high-partition-latency high-partition-fanout}"
RUNS="${RUNS:-10}"
RUN_START="${RUN_START:-1}"
RUN_END=$(( RUN_START + RUNS - 1 ))

RESULTS_DIR="$REPO_ROOT/bench/results"
mkdir -p "$RESULTS_DIR"

echo "═══ MATRIX: topology=$TOPOLOGY runs ${RUN_START}..${RUN_END} ═══"
echo "scenarios: $SCENARIOS"

for run in $(seq "$RUN_START" "$RUN_END"); do
  # Two-digit, zero-padded so lexical sort == numeric sort in the results dir.
  run_tag=$(printf -- "-run%02d" "$run")
  export BENCH_RUN_TAG="$run_tag"
  echo "═══════════ REPEAT $run / $RUN_END (tag ${run_tag}) ═══════════"
  for scenario in $SCENARIOS; do
    for stack in crabka kafka; do
      echo "═══ $stack / $scenario / $TOPOLOGY / run $run ═══"
      BENCH_DRIVER_IMAGE="$BENCH_DRIVER_IMAGE" \
        "$REPO_ROOT/bench/scripts/run-scenario.sh" "$stack" "$scenario" "$TOPOLOGY" \
        || echo "WARN: $stack/$scenario run $run failed (continuing)"
      "$REPO_ROOT/bench/scripts/teardown.sh" "$stack" || true
    done
  done
done

echo "═══ ALL DONE ($RUNS run(s)) ═══"
echo "Results in: $RESULTS_DIR"
ls -la "$RESULTS_DIR/"
echo
echo "Aggregate the averaged report + graph-ready CSVs with:"
echo "  cargo run --release -p crabka-bench-driver --bin crabka-bench-report -- \\"
echo "    --input-dir bench/results --out bench/results/SUMMARY.md \\"
echo "    --csv bench/results/results.csv \\"
echo "    --timeseries-csv bench/results/timeseries.csv"
