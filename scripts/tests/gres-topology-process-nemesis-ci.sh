#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

cargo build --locked -p crabka-cli -p crabka-gres
mkdir -p target/g8-topology-process-nemesis
evidence_path="$PWD/target/g8-topology-process-nemesis/move-foundation.json"
CRABKA_G8_PROCESS_NEMESIS=1 \
CRABKA_G8_NEMESIS_EVIDENCE="$evidence_path" \
timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis \
  -- --exact real_process_move_cli_operator_and_wal_retirement --nocapture
CRABKA_G8_PROCESS_NEMESIS=1 \
CRABKA_G8_KILL_EVIDENCE="$PWD/target/g8-topology-process-nemesis/move-paused-stage-kill.json" \
timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis \
  -- --exact real_process_move_recovers_after_paused_stage_sigkill_with_exact_ack_ledger --nocapture

python3 - <<'PY'
import json
from pathlib import Path

evidence = json.loads(Path("target/g8-topology-process-nemesis/move-foundation.json").read_text())
assert evidence["operation"] == "move"
assert evidence["completed"] is True
assert evidence["acknowledged_rows"] == evidence["target_rows"] == 32
assert evidence["predecessor_wal_retired"] is True
kill_evidence = json.loads(Path("target/g8-topology-process-nemesis/move-paused-stage-kill.json").read_text())
assert kill_evidence["kill_point"] == "paused_after_stage"
assert kill_evidence["completed"] is True
assert kill_evidence["old_pid"] != kill_evidence["new_pid"]
assert kill_evidence["max_ack_gap_ms"] <= kill_evidence["max_ack_gap_bound_ms"] == 7000
assert kill_evidence["predecessor_wal_retired"] is True
PY
