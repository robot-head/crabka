#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

workflow=.github/workflows/ci.yml

job="$(sed -n '/^  gres-range-scaling:/,/^  gres-sharded-conformance:/p' "$workflow")"

grep -q 'dtolnay/rust-toolchain@stable' <<<"$job"
grep -q 'Swatinem/rust-cache@v2' <<<"$job"
grep -q 'postgresql-client' <<<"$job"
grep -q 'CRABKA_GRES_RANGE_SCALING_MODE=fast' <<<"$job"
grep -q 'cargo build --locked' scripts/gres-range-scaling.sh
grep -q 'if:.*!cancelled()' <<<"$job"

if grep -q 'continue-on-error: true' <<<"$job"; then
    echo 'scaling job must gate CI' >&2
    exit 1
fi
if grep -q 'CRABKA_GRES_RANGE_SCALING_MODE=dry-run' <<<"$job"; then
    echo 'scaling job must publish live evidence' >&2
    exit 1
fi

python3 - <<'PY'
from pathlib import Path

source = Path("scripts/gres-range-scaling.sh").read_text()
required = [
    '"mode": mode',
    '"g8_decision_ceiling_curve": g8_points',
    '"commit_rate_curve": timestamp_points',
    '"overall": range_passed and sharded_passed and decision_ceiling_passed',
]
for needle in required:
    assert needle in source, needle
PY

echo 'PASS: live Gres scaling CI contract'
