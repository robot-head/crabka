#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

gate=scripts/gres-kind-lifecycle.sh
test -x "$gate"

required_patterns=(
    'kind create cluster'
    'crabka.io_greses.yaml'
    'crabka.io_grestenants.yaml'
    'kubectl rollout status'
    'sslmode=verify-full'
    'ResumeRequested'
    'wal_generation'
    'CRABKA_GRES_COLDSTART_ITERATIONS:-10'
    'ghcr.io/pgdogdev/pgdog:0.1.47'
    '//packaging:${target}_image_load'
    'load_image operator "crabka-operator:$IMAGE_TAG"'
    'load_image broker "crabka-broker:$IMAGE_TAG"'
    'load_image gres "crabka-gres:$IMAGE_TAG"'
    'load_image gres_activator "crabka-gres-activator:$IMAGE_TAG"'
    'kubectl logs -l app.kubernetes.io/name=crabka-pgdog,app.kubernetes.io/instance=fleet'
    'deployment\.kubernetes\.io/revision'
    'post-grace-pgdog-${iteration}.log'
    'timeout '
)
for pattern in "${required_patterns[@]}"; do
    grep -Fq "$pattern" "$gate"
done

test "$(grep -Fc 'kubectl port-forward deploy/tenant-a-gres 17432:5432' "$gate")" -eq 2
! grep -Fq 'kubectl port-forward svc/tenant-a-gres 17432:5432' "$gate"
! grep -Fq '.items[0].metadata.uid' "$gate"
! grep -Fq 'docker build' "$gate"
! grep -Fq 'Dockerfile' "$gate"
! grep -Fq 'cargo build' "$gate"

grep -Fq 'name = "gres_image"' packaging/BUILD.bazel
grep -Fq 'name = "gres_activator_image"' packaging/BUILD.bazel
test -f packaging/apko/runtime-base.lock.json

echo 'PASS: operator-backed Gres lifecycle gate is structurally wired'
