#!/usr/bin/env bash
# Tear down a Kafka cluster (cascades via owner-ref to the StatefulSet,
# Services, ConfigMaps, etc.). Idempotent.
#
# Usage: teardown.sh STACK
#   STACK   crabka|kafka

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

STACK="${1:?stack: crabka|kafka}"

case "$STACK" in
  crabka)
    log "tearing down Crabka cluster 'demo'"
    kubectl delete kafkatopic.crabka.io bench-topic -n "$BENCH_NAMESPACE" --ignore-not-found
    kubectl delete kafka.crabka.io demo -n "$BENCH_NAMESPACE" --ignore-not-found --wait=false
    # Delete all KafkaNodePools belonging to the demo cluster (handles both
    # single 'brokers' pool and multi-pool topologies like broker-0/1/2).
    kubectl delete kafkanodepool.crabka.io \
      -n "$BENCH_NAMESPACE" \
      -l "crabka.io/cluster=demo" \
      --ignore-not-found 2>/dev/null || true
    # Fallback: also try the old single-pool name in case the label isn't set
    kubectl delete kafkanodepool.crabka.io brokers \
      -n "$BENCH_NAMESPACE" --ignore-not-found 2>/dev/null || true
    kubectl delete pvc -n "$BENCH_NAMESPACE" -l app.kubernetes.io/instance=demo --ignore-not-found
    ;;
  kafka|strimzi)
    log "tearing down Strimzi cluster 'demo'"
    kubectl delete kafkatopic.kafka.strimzi.io bench-topic -n "$BENCH_NAMESPACE" --ignore-not-found
    kubectl delete kafka.kafka.strimzi.io demo -n "$BENCH_NAMESPACE" --ignore-not-found --wait=false
    kubectl delete kafkanodepool.kafka.strimzi.io kafka -n "$BENCH_NAMESPACE" --ignore-not-found
    kubectl delete pvc -n "$BENCH_NAMESPACE" -l strimzi.io/cluster=demo --ignore-not-found
    ;;
  *)
    log "unknown stack '$STACK'"
    exit 2
    ;;
esac

# Wait for the StatefulSet & broker pods to disappear so the next run
# doesn't race the GC.
for i in $(seq 1 60); do
  case "$STACK" in
    crabka)
      pods=$(kubectl get pods -n "$BENCH_NAMESPACE" \
        -l "app.kubernetes.io/instance=demo,app.kubernetes.io/managed-by=crabka-operator" \
        -o name 2>/dev/null | wc -l | tr -d ' ')
      ;;
    *)
      pods=$(kubectl get pods -n "$BENCH_NAMESPACE" -l strimzi.io/cluster=demo -o name 2>/dev/null | wc -l | tr -d ' ')
      ;;
  esac
  if [[ "$pods" == "0" ]]; then
    log "teardown complete"
    exit 0
  fi
  sleep 5
done

log "WARN: pods still present after 5 min teardown wait"
kubectl get pods -n "$BENCH_NAMESPACE" >&2
exit 0
