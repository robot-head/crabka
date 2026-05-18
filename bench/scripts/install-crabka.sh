#!/usr/bin/env bash
# Install the Crabka operator + CRDs. Mirrors `.github/workflows/operator-e2e.yml`.
# Idempotent — safe to re-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

: "${CRABKA_OPERATOR_IMAGE_REPO:=ghcr.io/robot-head/crabka-operator}"
: "${CRABKA_OPERATOR_IMAGE_TAG:=0.1.1}"
: "${CRABKA_BROKER_IMAGE_REPO:=ghcr.io/robot-head/crabka-broker}"
: "${CRABKA_BROKER_IMAGE_TAG:=0.1.1}"
: "${CRABKA_IMAGE_PULL_POLICY:=IfNotPresent}"

log "installing Crabka operator (image=$CRABKA_OPERATOR_IMAGE_REPO:$CRABKA_OPERATOR_IMAGE_TAG)"

kubectl apply -f "$REPO_ROOT/deploy/crds/crabka.io_kafkas.yaml"
kubectl apply -f "$REPO_ROOT/deploy/crds/crabka.io_kafkanodepools.yaml"
kubectl apply -f "$REPO_ROOT/deploy/crds/crabka.io_kafkatopics.yaml"
kubectl apply -f "$REPO_ROOT/deploy/crds/crabka.io_kafkausers.yaml"

kubectl create namespace crabka-operator --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install operator "$REPO_ROOT/charts/crabka-operator" \
  --namespace crabka-operator \
  --set "image.repository=$CRABKA_OPERATOR_IMAGE_REPO" \
  --set "image.tag=$CRABKA_OPERATOR_IMAGE_TAG" \
  --set "image.pullPolicy=$CRABKA_IMAGE_PULL_POLICY" \
  --set "brokerImage.repository=$CRABKA_BROKER_IMAGE_REPO" \
  --set "brokerImage.tag=$CRABKA_BROKER_IMAGE_TAG" \
  --set "brokerImage.pullPolicy=$CRABKA_IMAGE_PULL_POLICY"

log "waiting for crabka-operator rollout"
kubectl rollout status -n crabka-operator deploy/operator-crabka-operator --timeout=300s
log "Crabka operator ready"
