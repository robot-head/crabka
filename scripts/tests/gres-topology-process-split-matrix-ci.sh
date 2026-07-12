#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

scripts/tests/gres-topology-process-split-source-restore-ci.sh
scripts/tests/gres-topology-process-split-publication-ci.sh
scripts/tests/gres-topology-process-split-retirement-ci.sh
python3 scripts/tests/validate-gres-split-crash-evidence.py \
  --validate-matrix "$PWD/target/g8-split-crash"
