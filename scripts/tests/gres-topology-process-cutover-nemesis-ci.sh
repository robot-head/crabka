#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

cargo build --locked -p crabka-cli -p crabka-gres
mkdir -p target/g8-topology-process-nemesis
for kill_point in restored activated_before_cutover activated_after_tenant_cas layout_published; do
  CRABKA_G8_PROCESS_NEMESIS=1 \
  CRABKA_G8_CUTOVER_KILL_POINT="$kill_point" \
  CRABKA_G8_KILL_EVIDENCE="$PWD/target/g8-topology-process-nemesis/cutover-${kill_point}-kill.json" \
  timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis \
    -- --exact real_process_move_source_phase_sigkill_with_exact_ack_ledger --nocapture
done

python3 - <<'PY'
import json
from pathlib import Path

expected = {
    "restored": ("Restored", "current", None, 20000),
    "activated_before_cutover": ("Activated", "current", None, 12000),
    "activated_after_tenant_cas": ("Activated", "target", "Parking", 12000),
    "layout_published": ("LayoutPublished", "target", "Parking", 12000),
}
for kill_point, (phase, layout, retirement, gap_bound) in expected.items():
    evidence = json.loads(Path(f"target/g8-topology-process-nemesis/cutover-{kill_point}-kill.json").read_text())
    assert evidence["operation"] == "move" and evidence["kill_point"] == kill_point
    assert evidence["completed"] is True and evidence["old_pid"] != evidence["new_pid"]
    assert evidence["durable_phase"] == phase
    durable = evidence["durable_evidence"]
    assert durable["manifest_key"] and durable["covered_offset"] is not None
    assert durable["barrier_offset"] is not None and durable["tail_sha256"] and durable["marker_digest"]
    assert evidence["durable_tenant_layout"] == layout
    assert evidence["durable_retirement_phase"] == retirement
    assert evidence["recovered_acknowledgements"] >= 1
    assert evidence["post_publication_ack_before_retirement"] is True
    assert evidence["max_ack_gap_ms"] <= evidence["max_ack_gap_bound_ms"] == gap_bound
    assert evidence["predecessor_wal_retired"] is True
    if kill_point == "activated_after_tenant_cas":
        assert evidence["ambiguous_cutover_advanced_without_republish"] is True
PY
