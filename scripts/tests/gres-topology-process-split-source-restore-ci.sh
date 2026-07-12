#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

readonly family=source_restore
readonly evidence_dir="$PWD/target/g8-split-crash/$family"
readonly validator="$PWD/scripts/tests/validate-gres-split-crash-evidence.py"
readonly cases=(
  initiated_before_running_cas checkpoint_receipt_before_journal_cas
  checkpointed_after_journal_cas pause_receipt_before_journal_cas
  paused_before_stage stage_receipt_before_journal_cas staged_after_journal_cas
  marker_claim_receipt_before_journal_cas restored_after_journal_cas
  prologue_receipt_before_journal_cas activated_after_journal_cas
)

cargo build --locked -p crabka-cli -p crabka-gres
cargo test --locked -p crabka-gres --test topology_process_split_crash --no-run
rm -rf "$evidence_dir"
mkdir -p "$evidence_dir"
for case_name in "${cases[@]}"; do
  evidence="$evidence_dir/$case_name.json"
  CRABKA_G8_SPLIT_CRASH=1 CRABKA_G8_SPLIT_KILL_POINT="$case_name" \
    CRABKA_G8_SPLIT_CRASH_EVIDENCE="$evidence" \
    timeout 240s cargo test --locked -p crabka-gres --test topology_process_split_crash \
      -- --exact real_process_split_crash_anywhere --nocapture
  python3 "$validator" --validate-file "$family" "$case_name" "$evidence"
done
python3 "$validator" --validate-family "$family" "$evidence_dir"
