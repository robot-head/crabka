#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

validate() {
  python3 - "$1" <<'PY'
import json, sys
from pathlib import Path
e = json.loads(Path(sys.argv[1]).read_text())
assert e["schema_version"] == 1 and e["operation"] == "split"
assert e["tenant_id"] and e["operation_id"] and e["phase"] == "completed"
assert e["target_range_ids"] == [0, 2, 3]
assert e["routing_table_id"] == 51 and e["split_rowid"] == 16
assert e["endpoints_distinct"] and e["r2"]["endpoint"] != e["r3"]["endpoint"]
assert e["r2"]["generation"] == e["r3"]["generation"] == 1
assert e["r2"]["serving"] and e["r3"]["serving"]
assert e["r2"]["row_count"] == 15 and e["r3"]["row_count"] == 17
assert e["r2"]["cross_side_rows"] == e["r3"]["cross_side_rows"] == 0
assert e["acknowledged_rows"] == e["full_scan_rows"] == 32 and e["full_ack_equality"]
m = e["marker_partition"]
assert m["disjoint"] and m["exact_union"] and m["predecessor_count"] == m["r2_count"] + m["r3_count"]
assert m["digest"]
assert m["authenticated_endpoint"]
assert m["request_range_id"] == 1 and m["request_generation"] == 0
assert m["request_journal_revision"] > 0 and m["request_journal_digest"]
assert e["predecessor_topic_absent"] and e["predecessor_delete_count"] == 1
topics = set(e["topics"])
tenant = e["tenant_id"]
assert f"__gres_wal.{tenant}.r1" not in topics
assert {f"__gres_wal.{tenant}.r0", f"__gres_wal.{tenant}.r2.g0000000001", f"__gres_wal.{tenant}.r3.g0000000001", e["sentinel_topic"]} <= topics
assert 0 < e["operation_elapsed_ms"] < 180000
PY
}

if [[ ${1:-} == --validate-only ]]; then
  validate "$2"
  exit 0
fi

cargo build --locked -p crabka-cli -p crabka-gres
mkdir -p target/g8-topology-process-split-foundation
evidence="$PWD/target/g8-topology-process-split-foundation/split-foundation.json"
CRABKA_G8_SPLIT_FOUNDATION=1 CRABKA_G8_SPLIT_EVIDENCE="$evidence" \
  timeout 180s cargo test --locked -p crabka-gres --test topology_process_nemesis \
  -- --exact real_process_split_two_successor_foundation --nocapture
validate "$evidence"
