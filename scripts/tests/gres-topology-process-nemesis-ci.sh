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
for kill_point in running checkpointed paused_before_stage paused_after_stage; do
  CRABKA_G8_PROCESS_NEMESIS=1 \
  CRABKA_G8_SOURCE_KILL_POINT="$kill_point" \
  CRABKA_G8_KILL_EVIDENCE="$PWD/target/g8-topology-process-nemesis/move-${kill_point}-kill.json" \
  timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis \
    -- --exact real_process_move_source_phase_sigkill_with_exact_ack_ledger --nocapture
done

python3 - <<'PY'
import json
from pathlib import Path

evidence = json.loads(Path("target/g8-topology-process-nemesis/move-foundation.json").read_text())
assert evidence["operation"] == "move"
assert evidence["tenant_id"] and evidence["operation_id"]
assert evidence["completed"] is True
assert evidence["acknowledged_rows"] == evidence["target_rows"] == 32
assert evidence["predecessor_wal_retired"] is True
identities = {(evidence["tenant_id"], evidence["operation_id"])}
for kill_point in ("running", "checkpointed", "paused_before_stage", "paused_after_stage"):
    kill_evidence = json.loads(Path(f"target/g8-topology-process-nemesis/move-{kill_point}-kill.json").read_text())
    assert kill_evidence["kill_point"] == kill_point
    assert kill_evidence["tenant_id"] and kill_evidence["operation_id"]
    identities.add((kill_evidence["tenant_id"], kill_evidence["operation_id"]))
    assert kill_evidence["completed"] is True
    assert kill_evidence["old_pid"] != kill_evidence["new_pid"]
    assert kill_evidence["recovered_acknowledgements"] >= 1
    expected_gap_bound = {
        "running": 12000,
        "checkpointed": 12000,
        "paused_before_stage": 20000,
        "paused_after_stage": 20000,
    }[kill_point]
    assert kill_evidence["max_ack_gap_ms"] <= kill_evidence["max_ack_gap_bound_ms"] == expected_gap_bound
    assert kill_evidence["predecessor_wal_retired"] is True
    assert kill_evidence["post_publication_ack_before_retirement"] is True
    durable = kill_evidence["durable_evidence"]
    if kill_point == "running":
        assert kill_evidence["durable_phase"] == "Running"
        assert all(value is None for value in durable.values())
    elif kill_point == "checkpointed":
        assert kill_evidence["durable_phase"] == "Checkpointed"
        assert durable["manifest_key"] and durable["covered_offset"] is not None
        assert durable["barrier_offset"] is None and durable["tail_sha256"] is None and durable["marker_digest"] is None
    elif kill_point == "paused_before_stage":
        assert kill_evidence["durable_phase"] == "Paused"
        assert durable["manifest_key"] and durable["covered_offset"] is not None
        assert durable["barrier_offset"] is not None and durable["tail_sha256"] is None and durable["marker_digest"] is None
    else:
        assert kill_evidence["durable_phase"] == "Paused"
        assert durable["manifest_key"] and durable["covered_offset"] is not None
        assert durable["barrier_offset"] is not None and durable["tail_sha256"]
        assert durable["marker_digest"] is None
assert len(identities) == 5
PY
