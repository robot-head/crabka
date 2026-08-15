#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

gate=scripts/gres-kind-lifecycle.sh
controller=crates/operator/src/controller/gres_tenant.rs
test -x "$gate"

required_patterns=(
    'kind create cluster'
    'kubectl apply -f deploy/crds'
    'kubectl rollout status'
    'sslmode=verify-full'
    'ResumeRequested'
    'wal_generation'
    'CRABKA_GRES_COLDSTART_ITERATIONS:-10'
    'deadline_wait 360 "tenant lifecycle $expected"'
    'ghcr.io/pgdogdev/pgdog:0.1.47'
    'kubectl logs -l app.kubernetes.io/name=crabka-pgdog,app.kubernetes.io/instance=fleet'
    'deployment\.kubernetes\.io/revision'
    'initial confirmed PgDog route'
    'initial PgDog Deployment hash'
    'post-grace-pgdog-${iteration}.log'
    'timeout '
)
for pattern in "${required_patterns[@]}"; do
    grep -Fq "$pattern" "$gate"
done

test "$(grep -Fc 'kubectl port-forward deploy/tenant-a-gres 17432:5432' "$gate")" -eq 2
! grep -Fq 'kubectl port-forward svc/tenant-a-gres 17432:5432' "$gate"
test "$(grep -Ec '^[[:space:]]*start_pgdog_port_forward$' "$gate")" -eq 3
test "$(grep -Fc "while true; do printf 'SELECT 1;\\n'; sleep 1; done" "$gate")" -eq 2
seed_line=$(grep -nF 'CREATE TABLE lifecycle_marker' "$gate" | cut -d: -f1)
test "$(grep -nF 'initial confirmed PgDog route' "$gate" | cut -d: -f1)" -lt "$seed_line"
test "$(grep -nF 'initial PgDog Deployment hash' "$gate" | cut -d: -f1)" -lt "$seed_line"
grep -Fq -- '--field-selector=status.phase=Running' "$gate"
grep -Fq -- '--sort-by=.metadata.creationTimestamp' "$gate"
grep -Fq 'kubectl port-forward "$pod" 16432:6432' "$gate"
! grep -Fq 'kubectl port-forward svc/fleet-pgdog' "$gate"
! grep -Fq 'kubectl port-forward deploy/fleet-pgdog' "$gate"
! grep -Fq '.items[0].metadata.uid' "$gate"
test "$(grep -nF 'post-wake busy-session keeper exited' "$gate" | cut -d: -f1)" \
    -lt "$(grep -nF 'wait_lifecycle active' "$gate" | cut -d: -f1)"
grep -Fq '.owns(deployments, watcher::Config::default())' "$controller"

grep -Fq -- '-p crabka-gres -p crabka-gres-activator' packaging/melange/crabka.yaml
test -f packaging/apko/crabka-gres.yaml
test -f packaging/apko/crabka-gres-activator.yaml

echo 'PASS: operator-backed Gres lifecycle gate is structurally wired'
