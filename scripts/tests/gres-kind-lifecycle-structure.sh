#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

gate=scripts/gres-kind-lifecycle.sh
test -x "$gate"

grep -Fq 'kind create cluster' "$gate"
grep -Fq 'crabka.io_greses.yaml' "$gate"
grep -Fq 'crabka.io_grestenants.yaml' "$gate"
grep -Fq 'kubectl rollout status' "$gate"
grep -Fq 'sslmode=verify-full' "$gate"
grep -Fq 'ResumeRequested' "$gate"
grep -Fq 'wal_generation' "$gate"
grep -Fq 'CRABKA_GRES_COLDSTART_ITERATIONS:-10' "$gate"
grep -Fq 'ghcr.io/pgdogdev/pgdog:0.1.47' "$gate"
grep -Fq 'timeout ' "$gate"

grep -Fq -- '-p crabka-gres -p crabka-gres-activator' packaging/melange/crabka.yaml
test -f packaging/apko/crabka-gres.yaml
test -f packaging/apko/crabka-gres-activator.yaml

echo 'PASS: operator-backed Gres lifecycle gate is structurally wired'
