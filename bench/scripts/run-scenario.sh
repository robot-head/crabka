#!/usr/bin/env bash
# Run one scenario × one stack: apply Kafka CR, wait Ready, render and
# apply the Job from the template, collect the result JSON, augment it
# with operator-latency, teardown.
#
# Usage: run-scenario.sh STACK SCENARIO TOPOLOGY [TLS]
#   STACK     crabka|kafka
#   SCENARIO  scenario file basename (without .yaml), under bench/scenarios/
#   TOPOLOGY  1broker-rf1 | 3broker-rf3 | 6broker-rf3
#   TLS       tls  → run the TLS-encrypted data path (4th positional arg, or
#                    set BENCH_TLS=1 in the env). Omitted = plaintext (default).
#
# Env vars:
#   BENCH_DRIVER_IMAGE   image ref for the driver Job (default crabka-bench-driver:e2e)
#   BENCH_NAMESPACE      target namespace for the Kafka CR (default 'default')
#   BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS
#                        Prometheus HTTP request timeout (default 15)
#   BENCH_RESULTS_DIR    where to write the per-run JSON (default bench/results)
#   BENCH_RUN_TAG        optional filename suffix (e.g. "-run07") for repeated
#                        runs; set by run-matrix.sh so a 10× pass keeps all
#                        per-iteration JSONs instead of overwriting one file
#   BENCH_TLS            non-empty → TLS data path (equivalent to TLS=tls arg)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

STACK="${1:?stack: crabka|kafka}"
SCENARIO="${2:?scenario name}"
TOPOLOGY="${3:-1broker-rf1}"
# TLS dimension: 4th positional arg `tls`, or BENCH_TLS already set in env.
if [[ "${4:-}" == "tls" ]]; then
  BENCH_TLS=1
fi
: "${BENCH_TLS:=}"

: "${BENCH_DRIVER_IMAGE:=crabka-bench-driver:e2e}"
: "${BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS:=15}"
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
  6broker-rf3) BENCH_BROKER_COUNT=6 ;;
  *) log "unknown topology '$TOPOLOGY'"; exit 2 ;;
esac

manifest_stack="$STACK"
if [[ "$manifest_stack" == "kafka" ]]; then
  manifest_stack="strimzi"
fi

# TLS runs use the `-tls` CR variant (adds an Ssl listener on :9093 alongside
# the existing plaintext :9092). Plaintext runs use the base CR unchanged.
CR_SUFFIX=""
[[ -n "$BENCH_TLS" ]] && CR_SUFFIX="-tls"
KAFKA_CR_PATH="$REPO_ROOT/bench/manifests/$manifest_stack/kafka-cr-${TOPOLOGY}${CR_SUFFIX}.yaml"
TOPIC_PATH="$REPO_ROOT/bench/manifests/$manifest_stack/kafkatopic-bench.yaml"
JOB_TEMPLATE="$REPO_ROOT/bench/manifests/driver/job-template.yaml"
RBAC_PATH="$REPO_ROOT/bench/manifests/driver/rbac.yaml"

[[ -f "$KAFKA_CR_PATH" ]] || { log "missing Kafka CR manifest $KAFKA_CR_PATH"; exit 2; }

log "[$STACK/$SCENARIO/$TOPOLOGY] applying RBAC + Kafka CR"
kubectl apply -f "$RBAC_PATH"

T0=$(date +%s%N)
kubectl apply -f "$KAFKA_CR_PATH"

log "[$STACK/$SCENARIO/$TOPOLOGY] waiting for Kafka Ready"
elapsed=$(wait_kafka_ready "$STACK" 600)
T_READY=$(date +%s%N)
# This WSL `date` ignores the %3N width spec and emits 19-digit epoch
# *nanoseconds*, so the T_READY-T0 delta is in ns — convert to ms for the
# report's startup_ms field. (`%s%N` is unambiguous: epoch seconds + 9-digit ns.)
STARTUP_MS=$(( (T_READY - T0) / 1000000 ))
log "[$STACK/$SCENARIO/$TOPOLOGY] Kafka Ready in ${elapsed}s (startup_ms=$STARTUP_MS)"

log "[$STACK/$SCENARIO/$TOPOLOGY] applying KafkaTopic (partitions=$BENCH_PARTITIONS rf=$BENCH_REPLICATION_FACTOR)"
envsubst < "$TOPIC_PATH" | kubectl apply -f -
wait_kafka_topic_ready "$STACK" bench-topic 180

BENCH_BOOTSTRAP=$(bootstrap_for "$STACK")
# Suffix the Job + result file on the TLS dimension so a TLS cell does not
# clobber / collide with the plaintext cell for the same stack+scenario.
# BENCH_RUN_SUFFIX is consumed by the job-template Job name via envsubst.
BENCH_RUN_SUFFIX=""
[[ -n "$BENCH_TLS" ]] && BENCH_RUN_SUFFIX="-tls"
export BENCH_RUN_SUFFIX
JOB_NAME="bench-driver-${STACK}-${SCENARIO}${BENCH_RUN_SUFFIX}"

# Pre-emptively delete any stale Job from a prior run (Job names are
# unique per stack+scenario; ttlSecondsAfterFinished may not have fired
# if the run was aborted).
kubectl delete job "$JOB_NAME" -n "$BENCH_NAMESPACE" --ignore-not-found

export BENCH_STACK="$STACK"
export BENCH_SCENARIO_NAME="$SCENARIO"
export BENCH_BOOTSTRAP
export BENCH_BROKER_COUNT
export BENCH_DRIVER_IMAGE
export BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS

# TLS data-path knobs consumed by the job-template envsubst. All three are
# always exported (with inert defaults on the plaintext path) so envsubst never
# blanks the volume's secretName / the driver env. The cluster-CA Secret is
# named demo-cluster-ca-cert (key ca.crt) for BOTH crabka and Strimzi.
if [[ -n "$BENCH_TLS" ]]; then
  BENCH_TLS_ENABLED=true
  BENCH_TLS_SERVER_NAME=$(tls_server_name_for "$STACK")
else
  BENCH_TLS_ENABLED=false
  BENCH_TLS_SERVER_NAME=""
fi
: "${BENCH_CA_SECRET:=demo-cluster-ca-cert}"
export BENCH_TLS_ENABLED BENCH_TLS_SERVER_NAME BENCH_CA_SECRET

log "[$STACK/$SCENARIO/$TOPOLOGY] launching driver Job $JOB_NAME (tls=${BENCH_TLS:-0} server_name=${BENCH_TLS_SERVER_NAME:-none})"
envsubst < "$JOB_TEMPLATE" | kubectl apply -f -

# duration + warmup + 5 min buffer for image-pull + producer build
job_timeout=$(( BENCH_DURATION_S + BENCH_WARMUP_S + 300 ))
wait_job_complete "$JOB_NAME" "$job_timeout"

# Pull the result JSON out of the (now-completed) driver pod logs.
pod=$(kubectl get pod -n "$BENCH_NAMESPACE" -l "job-name=$JOB_NAME" -o jsonpath='{.items[0].metadata.name}')
# BENCH_RUN_TAG (e.g. "-run07") is set by run-matrix.sh on the Nth repeat so
# each iteration writes a distinct file instead of clobbering the previous one;
# the report aggregator averages all runs that share a (scenario, topology).
out_json="${BENCH_RESULTS_DIR}/${STACK}-${SCENARIO}-${TOPOLOGY}${BENCH_RUN_SUFFIX}${BENCH_RUN_TAG:-}.json"
log "[$STACK/$SCENARIO/$TOPOLOGY] extracting results from logs of pod $pod → $out_json"

kubectl logs -n "$BENCH_NAMESPACE" "$pod" -c driver | awk '/===RESULT_START===/{flag=1;next} /===RESULT_END===/{flag=0} flag' > "$out_json"

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
