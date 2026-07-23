#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

cargo build --locked -p crabka-cli -p crabka-gres
mkdir -p target/g8-topology-process-nemesis
for kill_point in retiring_before_delete retiring_after_delete retiring_parked resuming; do
  CRABKA_G8_PROCESS_NEMESIS=1 \
  CRABKA_G8_RETIREMENT_KILL_POINT="$kill_point" \
  CRABKA_G8_KILL_EVIDENCE="$PWD/target/g8-topology-process-nemesis/retirement-${kill_point}-kill.json" \
  timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis \
    -- --exact real_process_move_source_phase_sigkill_with_exact_ack_ledger --nocapture
done

python3 - <<'PY'
import json
from pathlib import Path

expected = {
    "retiring_before_delete": ("Retiring", "Parking", 3, True, 0, 1, 0),
    "retiring_after_delete": ("Retiring", "Parking", 3, False, 1, 0, 1),
    "retiring_parked": ("Retiring", "Parked", 4, False, 1, 0, 0),
    "resuming": ("Resuming", "Parked", 4, False, 1, 0, 0),
}
identities = set()
for kill_point, values in expected.items():
    phase, retirement, version, topic_at_kill, calls_at_kill, replay_calls, injected = values
    path = Path(f"target/g8-topology-process-nemesis/retirement-{kill_point}-kill.json")
    evidence = json.loads(path.read_text())
    assert evidence["operation"] == "move" and evidence["kill_point"] == kill_point
    assert evidence["tenant_id"] and evidence["operation_id"]
    identities.add((evidence["tenant_id"], evidence["operation_id"]))
    assert evidence["completed"] is True and evidence["old_pid"] != evidence["new_pid"]
    assert evidence["durable_phase"] == phase
    assert evidence["durable_tenant_layout"] == "target"
    assert evidence["durable_retirement_phase"] == retirement
    assert evidence["durable_tenant_record_version"] == version
    assert evidence["predecessor_topic_present_at_kill"] is topic_at_kill
    assert evidence["exact_predecessor_delete_calls"] == 1
    assert evidence["delete_calls_at_kill"] == calls_at_kill
    assert evidence["replay_delete_calls"] == replay_calls
    assert evidence["injected_after_delete_errors"] == injected
    receipt_expected = kill_point == "resuming"
    assert evidence["retire_receipt_probed_before_kill"] is receipt_expected
    assert evidence["retire_receipt_probed_after_restart"] is receipt_expected
    assert evidence["unrelated_delete_attempted"] is False
    assert evidence["delete_requested_topics"] == [
        evidence["sentinel_topic"].replace("__gres_g8_retirement_sentinel.", "__gres_wal.") + ".r1"
    ]
    assert evidence["sentinel_topic_preserved"] is True
    assert evidence["predecessor_wal_retired"] is True
    assert evidence["replacement_owner"] == {"range_id": 2, "generation": 1}
    assert evidence["marker_digest"] == evidence["durable_evidence"]["marker_digest"]
    assert evidence["recovered_acknowledgements"] >= 1
    assert evidence["post_publication_ack_before_retirement"] is True
    assert evidence["post_completed_ack"] is True
    assert evidence["acknowledged_rows"] > evidence["acknowledgements_at_completion"]
    assert evidence["max_ack_gap_ms"] <= evidence["max_ack_gap_bound_ms"] == 16000
assert len(identities) == len(expected)
PY
