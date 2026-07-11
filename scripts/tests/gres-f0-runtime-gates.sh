#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

grep -Fq -- '--extended-corpus crates/gres-conformance/corpus-extended' scripts/gres-e2e.sh
grep -Fq -- '--extended-baseline crates/gres-conformance/corpus-extended/baseline.json' scripts/gres-e2e.sh
grep -Fq 'extended-parity-pgdog.json' scripts/gres-e2e.sh
grep -Fq 'crabka-gres-driver-smoke' scripts/gres-e2e.sh
grep -Fq 'import psycopg' scripts/gres-e2e.sh
grep -Fq 'psycopg is required' scripts/gres-e2e.sh

grep -Fq 'extended-parity-standalone.json' .github/workflows/ci.yml
grep -Fq 'extended-parity-substrate.json' .github/workflows/ci.yml
grep -Fq 'crates/gres-conformance/corpus-extended/baseline.json' .github/workflows/ci.yml
grep -Fq 'python3 -m pip install --require-hashes --no-deps' .github/workflows/ci.yml
grep -Fq 'extended-parity-pgdog.json' .github/workflows/ci.yml

grep -Fq 'name = "crabka-gres-driver-smoke"' crates/gres-conformance/Cargo.toml
grep -Fq 'sqlx = { workspace = true }' crates/gres-conformance/Cargo.toml
test -f crates/gres-conformance/src/bin/driver_smoke.rs

echo 'PASS: F-0 runtime gate wiring contract'
