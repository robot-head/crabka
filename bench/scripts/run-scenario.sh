#!/usr/bin/env bash
# Run one scenario × one stack: apply Kafka CR, wait Ready, render and
# apply the Job from the template, collect the result JSON, augment it
# with operator-latency, teardown.
#
# Usage: run-scenario.sh STACK SCENARIO TOPOLOGY
#   STACK     crabka|kafka
#   SCENARIO  scenario file basename (without .yaml), under bench/scenarios/
#   TOPOLOGY  1broker-rf1 | 3broker-rf3
#
# Env vars:
#   BENCH_DRIVER_IMAGE   image ref for the driver Job (default crabka-bench-driver:e2e)
#   BENCH_NAMESPACE      target namespace for the Kafka CR (default 'default')
#   BENCH_RESULTS_DIR    where to write the per-run JSON (default bench/results)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

STACK="${1:?stack: crabka|kafka}"
SCENARIO="${2:?scenario name}"
TOPOLOGY="${3:-1broker-rf1}"

: "${BENCH_DRIVER_IMAGE:=crabka-bench-driver:e2e}"
: "${BENCH_RESULTS_DIR:=$REPO_ROOT/bench/results}"

mkdir -p "$BENCH_RESULTS_DIR"

SCEN_PATH="$REPO_ROOT/bench/scenarios/${SCENARIO}.yaml"
[[ -f "$SCEN_PATH" ]] || { log "scenario file not found: $SCEN_PATH"; exit 2; }

BENCH_PARTITIONS=$(scenario_field "$SCEN_PATH" partitions)
BENCH_REPLICATION_FACTOR=$(scenario_field "$SCEN_PATH" replication_factor)
BENCH_DURATION_S=$(scenario_field "$SCEN_PATH" duration_s)
BENCH_WARMUP_S=$(scenario_field "$SCEN_PATH" warmup_s)
: "${BENCH_PARTITIONS:=6}"
: "${BENCH_REPLICATION_FACTOR:=1}"
: "${BENCH_DURATION_S:=60}"
: "${BENCH_WARMUP_S:=10}"
export BENCH_PARTITIONS BENCH_REPLICATION_FACTOR

case "$TOPOLOGY" in
  1broker-rf1) BENCH_BROKER_COUNT=1 ;;
  3broker-rf3) BENCH_BROKER_COUNT=3 ;;
  *) log "unknown topology '$TOPOLOGY'"; exit 2 ;;
esac

KAFKA_CR_PATH="$REPO_ROOT/bench/manifests/$STACK/kafka-cr-${TOPOLOGY}.yaml"
TOPIC_PATH="$REPO_ROOT/bench/manifests/$STACK/kafkatopic-bench.yaml"
JOB_TEMPLATE="$REPO_ROOT/bench/manifests/driver/job-template.yaml"
RBAC_PATH="$REPO_ROOT/bench/manifests/driver/rbac.yaml"

[[ -f "$KAFKA_CR_PATH" ]] || { log "missing Kafka CR manifest $KAFKA_CR_PATH"; exit 2; }

log "[$STACK/$SCENARIO/$TOPOLOGY] applying RBAC + Kafka CR"
kubectl apply -f "$RBAC_PATH"

T0=$(date +%s%3N)
kubectl apply -f "$KAFKA_CR_PATH"

log "[$STACK/$SCENARIO/$TOPOLOGY] waiting for Kafka Ready"
elapsed=$(wait_kafka_ready "$STACK" 600)
T_READY=$(date +%s%3N)
STARTUP_MS=$(( T_READY - T0 ))
log "[$STACK/$SCENARIO/$TOPOLOGY] Kafka Ready in ${elapsed}s (startup_ms=$STARTUP_MS)"

log "[$STACK/$SCENARIO/$TOPOLOGY] applying KafkaTopic (partitions=$BENCH_PARTITIONS rf=$BENCH_REPLICATION_FACTOR)"
envsubst < "$TOPIC_PATH" | kubectl apply -f -
wait_kafka_topic_ready "$STACK" bench-topic 180

BENCH_BOOTSTRAP=$(bootstrap_for "$STACK")
JOB_NAME="bench-driver-${STACK}-${SCENARIO}"

# Pre-emptively delete any stale Job from a prior run (Job names are
# unique per stack+scenario; ttlSecondsAfterFinished may not have fired
# if the run was aborted).
kubectl delete job "$JOB_NAME" -n "$BENCH_NAMESPACE" --ignore-not-found

export BENCH_STACK="$STACK"
export BENCH_SCENARIO_NAME="$SCENARIO"
export BENCH_BOOTSTRAP
export BENCH_BROKER_COUNT
export BENCH_DRIVER_IMAGE

log "[$STACK/$SCENARIO/$TOPOLOGY] launching driver Job $JOB_NAME"
envsubst < "$JOB_TEMPLATE" | kubectl apply -f -

# duration + warmup + 5 min buffer for image-pull + producer build
job_timeout=$(( BENCH_DURATION_S + BENCH_WARMUP_S + 300 ))
wait_job_complete "$JOB_NAME" "$job_timeout"

# Pull the result JSON out of the (now-completed) driver pod.
pod=$(kubectl get pod -n "$BENCH_NAMESPACE" -l "job-name=$JOB_NAME" -o jsonpath='{.items[0].metadata.name}')
out_json="$BENCH_RESULTS_DIR/${STACK}-${SCENARIO}-${TOPOLOGY}.json"
log "[$STACK/$SCENARIO/$TOPOLOGY] copying /results/run.json from $pod → $out_json"

# Job pods may have already exited; kubectl cp works against completed
# pods as long as ttlSecondsAfterFinished hasn't elapsed.
kubectl cp -n "$BENCH_NAMESPACE" --retries=3 \
  "${pod}:/results/run.json" "$out_json" \
  -c driver

# Augment the JSON with operator-latency captured here (the driver itself
# can't observe T0 → Ready).
python3 - "$out_json" "$STARTUP_MS" <<'PY'
import json, sys, pathlib
p = pathlib.Path(sys.argv[1])
data = json.loads(p.read_text())
data["startup_ms"] = int(sys.argv[2])
p.write_text(json.dumps(data, indent=2))
PY

log "[$STACK/$SCENARIO/$TOPOLOGY] wrote $out_json"
