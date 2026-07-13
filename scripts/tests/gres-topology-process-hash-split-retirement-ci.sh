#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export CRABKA_G8_SPLIT_WORKLOAD=hash
export CRABKA_G8_SPLIT_EVIDENCE_ROOT="$PWD/target/g9-hash-split-crash"
exec scripts/tests/gres-topology-process-split-retirement-ci.sh
