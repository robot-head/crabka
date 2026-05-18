#!/usr/bin/env bash
# Deploy the minimal Prometheus the driver queries for resource metrics.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

log "installing Prometheus"
kubectl apply -f "$SCRIPT_DIR/../manifests/prom/prometheus.yaml"
kubectl rollout status -n monitoring deploy/prometheus --timeout=180s
log "Prometheus ready"
