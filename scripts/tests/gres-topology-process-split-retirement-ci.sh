#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

readonly family=retirement_resume
readonly evidence_dir="${CRABKA_G8_SPLIT_EVIDENCE_ROOT:-$PWD/target/g8-split-crash}/$family"
readonly validator="$PWD/scripts/tests/validate-gres-split-crash-evidence.py"
readonly cases=(
  retiring_before_delete delete_success_before_sidecar_cas parked_after_sidecar_cas
  retire_receipt_before_journal_cas resuming_after_journal_cas completed_after_journal_cas
)

cargo build --locked -p crabka-cli -p crabka-gres
cargo test --locked -p crabka-gres --test topology_process_split_crash --no-run
rm -rf "$evidence_dir"
mkdir -p "$evidence_dir"
for case_name in "${cases[@]}"; do
  evidence="$evidence_dir/$case_name.json"
  CRABKA_G8_SPLIT_CRASH=1 CRABKA_G8_SPLIT_WORKLOAD="${CRABKA_G8_SPLIT_WORKLOAD:-ordinary}" CRABKA_G8_SPLIT_KILL_POINT="$case_name" \
    CRABKA_G8_SPLIT_CRASH_EVIDENCE="$evidence" \
    timeout 240s cargo test --locked -p crabka-gres --test topology_process_split_crash \
      -- --exact real_process_split_crash_anywhere --nocapture
  python3 "$validator" --validate-file "$family" "$case_name" "$evidence"
done
python3 "$validator" --validate-family "$family" "$evidence_dir"
