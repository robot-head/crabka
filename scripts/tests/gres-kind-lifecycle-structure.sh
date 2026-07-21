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

grep -Fq -- '-p crabka-gres -p crabka-gres-activator' packaging/melange/crabka.yaml
test -f packaging/apko/crabka-gres.yaml
test -f packaging/apko/crabka-gres-activator.yaml

echo 'PASS: operator-backed Gres lifecycle gate is structurally wired'
