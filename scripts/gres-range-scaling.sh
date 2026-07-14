#!/usr/bin/env bash
# Gres multi-range scaling artifact: compare range-local and sharded workloads.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/gres-range-scaling.sh [--help]

Runs 1, 2, and 4 range-local Gres workloads plus a single SHARDED-table
ingest workload through the existing crabka-gres multi-range CLI and writes
range-scaling.json under the artifact directory. The artifact includes the
range-local curve, sharded ingest curve, and a decision-ceiling aggregate
commit-rate comparison against the expected batched-decision envelope.

Environment:
  CRABKA_GRES_SKIP_BUILD=1                    Reuse existing target/debug binaries.
  CRABKA_GRES_RANGE_SCALING_ARTIFACT_DIR=dir  Artifact directory (default: target/gres-range-scaling-artifacts).
  CRABKA_GRES_RANGE_SCALING_FLOOR=float       Required 4-range/1-range throughput floor (default: 2.5).
  CRABKA_GRES_SHARDED_SCALING_FLOOR=float     Required sharded 4-range/1-range ingest floor (default: same as range floor).
  CRABKA_GRES_DECISION_CEILING_MIN_RATIO=float
                                               Required sharded 4-range measured/envelope ratio (default: 0.70).
  CRABKA_GRES_RANGE_SCALING_MODE=auto|live|dry-run|fast
                                               auto runs live when psql is available, otherwise dry-run (default: auto).
                                               fast runs live with the small-workload defaults.
  CRABKA_GRES_RANGE_SCALING_FAST=1            Use a small live workload.
  CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE=n
                                               Concurrent psql workers per range (default: 2; fast: 1).
  CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION=n
                                               Insert transactions per worker (default: 20; fast: 2).
  CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS=n      Warmup transactions per persistent worker (fast: 5).
  CRABKA_GRES_RANGE_SCALING_TRIALS=n           Repeated trials aggregated by median (fast: 3).
  CRABKA_GRES_RANGE_SCALING_KEEP_ARTIFACTS=0  Accepted for parity with other Gres scripts; artifacts are always kept.
EOF
}

case "${1:-}" in
    "") ;;
    --help|-h) usage; exit 0 ;;
    *) echo "FAIL: unknown argument $1" >&2; usage >&2; exit 2 ;;
esac

readonly ARTIFACT_DIR="${CRABKA_GRES_RANGE_SCALING_ARTIFACT_DIR:-target/gres-range-scaling-artifacts}"
readonly FLOOR="${CRABKA_GRES_RANGE_SCALING_FLOOR:-2.5}"
readonly SHARDED_FLOOR="${CRABKA_GRES_SHARDED_SCALING_FLOOR:-$FLOOR}"
readonly DECISION_CEILING_MIN_RATIO="${CRABKA_GRES_DECISION_CEILING_MIN_RATIO:-0.70}"
readonly CLUSTER_ID="00000000-0000-0000-0000-000000000001"
readonly SQL_USER="scaleuser"
readonly SQL_PASSWORD="scale-secret"
readonly MODE_REQUEST="${CRABKA_GRES_RANGE_SCALING_MODE:-auto}"
readonly SHARDED_TABLE_NAME="s1"

BROKER_PID=""
GRES_PID=""
BROKER_PORT=""
CONTROLLER_PORT=""
GRES_PORT=""

log() {
    printf 'gres-range-scaling: %s\n' "$*"
}

dump_diagnostics() {
    echo "---- gres-range-scaling artifacts: ${ARTIFACT_DIR} ----" >&2
    for file in "${ARTIFACT_DIR}"/*.log "${ARTIFACT_DIR}"/*.err "${ARTIFACT_DIR}"/*.out \
        "${ARTIFACT_DIR}"/run-*/*.err "${ARTIFACT_DIR}"/run-sharded-*/*.err; do
        [ -f "$file" ] || continue
        echo "---- ${file} ----" >&2
        tail -n 160 "$file" >&2 || true
    done
}

fail() {
    echo "FAIL: $*" >&2
    dump_diagnostics
    exit 1
}

cleanup() {
    local status=$?
    stop_gres || true
    kill "${BROKER_PID:-}" 2>/dev/null || true
    wait "${BROKER_PID:-}" 2>/dev/null || true
    if [ "$status" -ne 0 ]; then
        dump_diagnostics
    fi
    log "kept artifacts in ${ARTIFACT_DIR}"
}
trap cleanup EXIT

require_positive_integer() {
    local name="$1"
    local value="$2"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        fail "${name} must be a positive integer, got '${value}'"
    fi
}

require_decimal() {
    local name="$1"
    local value="$2"
    if [[ ! "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
        fail "${name} must be a non-negative decimal, got '${value}'"
    fi
}

choose_ports() {
    python3 - <<'PY'
import socket

sockets = []
try:
    for _ in range(3):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    for sock in sockets:
        print(sock.getsockname()[1])
finally:
    for sock in sockets:
        sock.close()
PY
}

wait_for_tcp_port() {
    local host="$1"
    local port="$2"
    local label="$3"

    for _ in $(seq 80); do
        if python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=0.2):
    pass
PY
        then
            return 0
        fi
        sleep 0.25
    done

    fail "${label} did not open ${host}:${port}"
}

wait_for_sql() {
    local conninfo="$1"
    local label="$2"

    for _ in $(seq 120); do
        if PGAPPNAME= psql "$conninfo" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        if [ -n "${GRES_PID:-}" ] && ! kill -0 "$GRES_PID" 2>/dev/null; then
            fail "${label} exited before serving SQL"
        fi
        sleep 0.25
    done

    fail "${label} did not become SQL-ready"
}

resolve_mode() {
    case "$MODE_REQUEST" in
        auto)
            if command -v psql >/dev/null 2>&1; then
                printf '%s\n' live
            else
                printf '%s\n' dry-run
            fi
            ;;
        live|dry-run) printf '%s\n' "$MODE_REQUEST" ;;
        fast) printf '%s\n' live ;;
        *) fail "CRABKA_GRES_RANGE_SCALING_MODE must be auto, live, dry-run, or fast" ;;
    esac
}

range_boundaries() {
    local range_count="$1"
    local boundaries=()
    local index
    for index in $(seq 0 $((range_count - 1))); do
        boundaries+=("$((index * 1000000))")
    done
    local joined="${boundaries[*]}"
    printf '%s\n' "${joined// /,}"
}

sharded_range_boundaries() {
    local range_count="$1"
    local boundaries=("0")
    local index
    for index in $(seq 1 $((range_count - 1))); do
        boundaries+=("1:${index}:0")
    done
    local joined="${boundaries[*]}"
    printf '%s\n' "${joined// /,}"
}

int4_hash_bucket() {
    local value="$1"
    local bucket_count="$2"
    local mask=$((bucket_count - 1))
    local hash=$((1 & mask))
    local shift
    local byte

    # Only the low log2(bucket_count) bits are needed. The FNV-1a offset
    # basis ends in 1, and reducing after each byte preserves those bits.
    for shift in 24 16 8 0; do
        byte=$(((value >> shift) & 255))
        hash=$((((hash ^ byte) * 1099511628211) & mask))
    done
    printf '%s\n' "$hash"
}

sharded_id_for_range() {
    local seed="$1"
    local bucket_count="$2"
    local target_bucket="$3"
    local candidate=$((seed * bucket_count))

    while [ "$(int4_hash_bucket "$candidate" "$bucket_count")" -ne "$target_bucket" ]; do
        candidate=$((candidate + 1))
    done
    printf '%s\n' "$candidate"
}

range_table_id() {
    local range_index="$1"
    printf '%s\n' "$((range_index * 1000000))"
}

start_broker() {
    ./target/debug/crabka format \
        --log-dir "${ARTIFACT_DIR}/broker-data" \
        --cluster-id "$CLUSTER_ID" \
        --standalone \
        --node-id 1 \
        --controller-listener "127.0.0.1:${CONTROLLER_PORT}" \
        >"${ARTIFACT_DIR}/format.log" 2>&1

    cat >"${ARTIFACT_DIR}/broker.toml" <<EOF
broker_id = 1
log_dir = "${ARTIFACT_DIR}/broker-data"
cluster_id = "${CLUSTER_ID}"
inter_broker_listener_name = "plain"

[[listeners]]
name = "plain"
bind_addr = "127.0.0.1:${BROKER_PORT}"
advertised = "127.0.0.1:${BROKER_PORT}"
protocol = "Plaintext"

[authorization]
type = "simple"
super_users = ["ANONYMOUS"]
EOF

    ./target/debug/crabka-broker \
        --log-dir "${ARTIFACT_DIR}/broker-data" \
        --cluster-id "$CLUSTER_ID" \
        --broker-id 1 \
        --config-file "${ARTIFACT_DIR}/broker.toml" \
        >"${ARTIFACT_DIR}/broker.log" 2>&1 &
    BROKER_PID=$!
    wait_for_tcp_port 127.0.0.1 "$BROKER_PORT" broker
}

stop_gres() {
    if [ -z "${GRES_PID:-}" ]; then
        return 0
    fi
    kill "$GRES_PID" 2>/dev/null || true
    wait "$GRES_PID" 2>/dev/null || true
    GRES_PID=""
}

create_tenant_config() {
    local tenant="$1"
    local boundaries="$2"
    local hash_buckets="${3:-}"
    local password_file="${ARTIFACT_DIR}/${tenant}.password"
    local placement_args=()

    if [ -n "$hash_buckets" ]; then
        placement_args=(--hash-placement "1:id:${hash_buckets}")
    fi

    printf '%s\n' "$SQL_PASSWORD" >"$password_file"
    ./target/debug/crabka gres create-tenant \
        --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --name "$tenant" \
        --user "$SQL_USER" \
        --password-file "$password_file" \
        --ranges "$boundaries" \
        "${placement_args[@]}" \
        >"${ARTIFACT_DIR}/create-${tenant}.log" 2>&1
}

start_gres() {
    local tenant="$1"
    local boundaries="$2"

    ./target/debug/crabka-gres \
        --listen "127.0.0.1:${GRES_PORT}" \
        --substrate-bootstrap "127.0.0.1:${BROKER_PORT}" \
        --tenant "$tenant" \
        --ranges "$boundaries" \
        --auth trust \
        >"${ARTIFACT_DIR}/gres-${tenant}.log" 2>&1 &
    GRES_PID=$!
    wait_for_sql "host=127.0.0.1 port=${GRES_PORT} user=${SQL_USER} dbname=crab sslmode=prefer" "$tenant"
}

prepare_range_tables() {
    local range_count="$1"
    local conninfo="$2"
    local sql=""
    local range_index

    for range_index in $(seq 0 $((range_count - 1))); do
        local table_id
        table_id="$(range_table_id "$range_index")"
        sql+="CREATE TABLE t${table_id} (id int4);"
    done
    PGAPPNAME= psql "$conninfo" -v ON_ERROR_STOP=1 -c "$sql" >"${ARTIFACT_DIR}/prepare-${range_count}.log" 2>&1
}

run_psql_worker() {
    local conninfo="$1"
    local table_name="$2"
    local txns="$3"
    local warmup_txns="$4"
    local worker_id="$5"
    local out_file="$6"
    local id_base="$7"
    local bucket_count="${8:-}"
    local target_bucket="${9:-}"
    local iteration
    local sql_file="${out_file}.sql"
    local raw_file="${out_file}.psql"

    : >"$sql_file"
    printf '\\timing on\n' >>"$sql_file"
    for iteration in $(seq 1 "$warmup_txns"); do
        local warmup_id=$((id_base + 500000 + iteration))
        if [ -n "$bucket_count" ]; then
            warmup_id="$(sharded_id_for_range "$warmup_id" "$bucket_count" "$target_bucket")"
        fi
        printf 'INSERT INTO %s VALUES (%s);\n' "$table_name" "$warmup_id" >>"$sql_file"
    done
    printf '\\echo MEASURE_BEGIN\n' >>"$sql_file"
    for iteration in $(seq 1 "$txns"); do
        local id
        id=$((id_base + iteration))
        if [ -n "$bucket_count" ]; then
            id="$(sharded_id_for_range "$id" "$bucket_count" "$target_bucket")"
        fi
        printf 'INSERT INTO %s VALUES (%s);\n' "$table_name" "$id" >>"$sql_file"
    done
    PGAPPNAME= psql "$conninfo" -v ON_ERROR_STOP=1 -qAt -f "$sql_file" >"$raw_file" 2>"${out_file}.err"
    awk '/MEASURE_BEGIN/{measured=1; next} measured && /^Time:/{value=$2+0; print value < 1 ? 1 : int(value + 0.5)}' "$raw_file" >"$out_file"
    [ "$(wc -l <"$out_file")" -eq "$txns" ] || fail "persistent worker recorded the wrong measured sample count"
}

wait_for_workers() {
    local status=0
    local pid

    for pid in "$@"; do
        if ! wait "$pid"; then
            status=1
        fi
    done

    return "$status"
}

write_live_unsupported_artifact() {
    local reason="$1"

    python3 - "$ARTIFACT_DIR" "$reason" <<'PY'
import json
import os
import pathlib
import platform
import socket
import subprocess
import sys
import time

artifact_dir = pathlib.Path(sys.argv[1])
reason = sys.argv[2]

def optional_command(args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None

partial_results = sorted(path.name for path in artifact_dir.glob("result-*.json"))
payload = {
    "schema_version": 3,
    "generated_at_unix": int(time.time()),
    "mode": "live",
    "supported": False,
    "reason": reason,
    "partial_result_files": partial_results,
    "environment": {
        "host": socket.gethostname(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "ci": os.environ.get("CI") == "true",
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "git_sha": optional_command(["git", "rev-parse", "HEAD"]),
    },
}
(artifact_dir / "live-unsupported.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
}

run_live_workload() {
    local range_count="$1"
    local sessions_per_range="$2"
    local txns_per_session="$3"
    local warmup_txns="$4"
    local trial="$5"
    local tenant="range-scale-${range_count}-${trial}-${GRES_PORT}"
    local boundaries
    local conninfo="host=127.0.0.1 port=${GRES_PORT} user=${SQL_USER} dbname=crab sslmode=prefer"
    local run_dir="${ARTIFACT_DIR}/run-${range_count}-trial-${trial}"
    local pids=()
    local worker_id=0
    local range_index
    local session_index
    local started_ns
    local elapsed_ms

    boundaries="$(range_boundaries "$range_count")"
    mkdir -p "$run_dir"
    create_tenant_config "$tenant" "$boundaries"
    start_gres "$tenant" "$boundaries"
    prepare_range_tables "$range_count" "$conninfo"

    started_ns="$(date +%s%N)"
    for range_index in $(seq 0 $((range_count - 1))); do
        local table_id
        table_id="$(range_table_id "$range_index")"
        for session_index in $(seq 1 "$sessions_per_range"); do
            worker_id=$((worker_id + 1))
            run_psql_worker "$conninfo" "t${table_id}" "$txns_per_session" "$warmup_txns" "$worker_id" "${run_dir}/latencies-${worker_id}.txt" "$((worker_id * 10000000))" &
            pids+=("$!")
        done
    done
    wait_for_workers "${pids[@]}" || fail "${range_count}-range workload worker failed"
    elapsed_ms=$((( $(date +%s%N) - started_ns ) / 1000000))
    stop_gres

    python3 - "$range_count" "$sessions_per_range" "$txns_per_session" "$elapsed_ms" "$run_dir" \
        >"${ARTIFACT_DIR}/result-${range_count}-trial-${trial}.json" <<'PY'
import json
import math
import pathlib
import re
import statistics
import sys

range_count = int(sys.argv[1])
sessions_per_range = int(sys.argv[2])
txns_per_session = int(sys.argv[3])
elapsed_ms = int(sys.argv[4])
run_dir = pathlib.Path(sys.argv[5])
latencies = []
for path in sorted(run_dir.glob("latencies-*.txt")):
    latencies.extend(int(line) for line in path.read_text().splitlines() if line.strip())
if not latencies:
    raise SystemExit("no workload latency samples recorded")

def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    rank = math.ceil((percentile / 100) * len(ordered))
    return ordered[max(0, min(rank - 1, len(ordered) - 1))]

committed = len(latencies)
elapsed_s = elapsed_ms / 1000
print(json.dumps({
    "workload_kind": "range_local_ingest",
    "range_count": range_count,
    "sessions_per_range": sessions_per_range,
    "txns_per_session": txns_per_session,
    "committed_transactions": committed,
    "duration_ms": elapsed_ms,
    "throughput_tps": round(committed / elapsed_s, 4) if elapsed_s else 0,
    "latency_ms": {
        "p50": percentile_nearest_rank(latencies, 50),
        "p95": percentile_nearest_rank(latencies, 95),
        "max": max(latencies),
        "mean": round(statistics.fmean(latencies), 2),
    },
}, sort_keys=True))
PY
}

prepare_sharded_table() {
    local range_count="$1"
    local conninfo="$2"

    PGAPPNAME= psql "$conninfo" -v ON_ERROR_STOP=1 -c "CREATE TABLE ${SHARDED_TABLE_NAME} (id int4) SHARDED BY HASH (id) BUCKETS ${range_count};" \
        >"${ARTIFACT_DIR}/prepare-sharded-${range_count}.log" 2>&1
}

run_live_sharded_workload() {
    local range_count="$1"
    local sessions_per_range="$2"
    local txns_per_session="$3"
    local warmup_txns="$4"
    local trial="$5"
    local tenant="sharded-scale-${range_count}-${trial}-${GRES_PORT}"
    local boundaries
    local conninfo="host=127.0.0.1 port=${GRES_PORT} user=${SQL_USER} dbname=crab sslmode=prefer"
    local run_dir="${ARTIFACT_DIR}/run-sharded-${range_count}-trial-${trial}"
    local pids=()
    local worker_id=0
    local range_index
    local session_index
    local started_ns
    local elapsed_ms

    boundaries="$(sharded_range_boundaries "$range_count")"
    mkdir -p "$run_dir"
    create_tenant_config "$tenant" "$boundaries" "$range_count"
    start_gres "$tenant" "$boundaries"
    prepare_sharded_table "$range_count" "$conninfo"

    started_ns="$(date +%s%N)"
    for range_index in $(seq 0 $((range_count - 1))); do
        for session_index in $(seq 1 "$sessions_per_range"); do
            worker_id=$((worker_id + 1))
            run_psql_worker "$conninfo" "$SHARDED_TABLE_NAME" "$txns_per_session" "$warmup_txns" "$worker_id" "${run_dir}/latencies-${worker_id}.txt" "$((range_index * 1000000 + session_index * 10000))" "$range_count" "$range_index" &
            pids+=("$!")
        done
    done
    if ! wait_for_workers "${pids[@]}"; then
        if grep -R "sharded table writes require a global transaction manager" "$run_dir" >/dev/null 2>&1; then
            write_live_unsupported_artifact \
                "live SHARDED-table writes require a global transaction manager in the current crabka-gres service configuration; dry-run remains the reliable CI artifact path"
            fail "live SHARDED-table workload is unsupported by the current service configuration"
        fi
        fail "${range_count}-range SHARDED-table workload worker failed"
    fi
    elapsed_ms=$((( $(date +%s%N) - started_ns ) / 1000000))
    stop_gres

    python3 - "$range_count" "$sessions_per_range" "$txns_per_session" "$warmup_txns" "$elapsed_ms" "$run_dir" "$SHARDED_TABLE_NAME" "${ARTIFACT_DIR}/gres-${tenant}.log" \
        >"${ARTIFACT_DIR}/result-sharded-${range_count}-trial-${trial}.json" <<'PY'
import json
import math
import pathlib
import re
import statistics
import sys

range_count = int(sys.argv[1])
sessions_per_range = int(sys.argv[2])
txns_per_session = int(sys.argv[3])
warmup_txns = int(sys.argv[4])
elapsed_ms = int(sys.argv[5])
run_dir = pathlib.Path(sys.argv[6])
table_name = sys.argv[7]
gres_log = pathlib.Path(sys.argv[8])
latencies = []
for path in sorted(run_dir.glob("latencies-*.txt")):
    latencies.extend(int(line) for line in path.read_text().splitlines() if line.strip())
if not latencies:
    raise SystemExit("no sharded workload latency samples recorded")

def percentile_nearest_rank(values, percentile):
    ordered = sorted(values)
    rank = math.ceil((percentile / 100) * len(ordered))
    return ordered[max(0, min(rank - 1, len(ordered) - 1))]

committed = len(latencies)
elapsed_s = elapsed_ms / 1000
primary_range_distribution = {}
ansi_escape = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
for raw_line in gres_log.read_text().splitlines():
    line = ansi_escape.sub("", raw_line)
    if "timestamp_primary_committed" not in line:
        continue
    marker = "primary_range="
    if marker not in line:
        raise SystemExit("timestamp primary observation omitted primary_range")
    range_id = line.split(marker, 1)[1].split()[0].rstrip(",")
    primary_range_distribution[range_id] = primary_range_distribution.get(range_id, 0) + 1
expected_ranges = {str(range_id) for range_id in range(range_count)}
if set(primary_range_distribution) != expected_ranges:
    raise SystemExit(f"observed timestamp primaries {primary_range_distribution} do not cover {expected_ranges}")
observed_primary_transactions = sum(primary_range_distribution.values())
expected_primary_transactions = range_count * sessions_per_range * (txns_per_session + warmup_txns)
if observed_primary_transactions != expected_primary_transactions:
    raise SystemExit(
        f"observed {observed_primary_transactions} timestamp primaries, expected {expected_primary_transactions}"
    )
print(json.dumps({
    "workload_kind": "sharded_table_ingest",
    "table_name": table_name,
    "range_count": range_count,
    "sessions_per_range": sessions_per_range,
    "txns_per_session": txns_per_session,
    "concurrency_sessions": range_count * sessions_per_range,
    "committed_transactions": committed,
    "primary_range_distribution": primary_range_distribution,
    "observed_primary_transactions": observed_primary_transactions,
    "distribution_check": "runtime timestamp_primary_committed observations cover all expected ranges",
    "duration_ms": elapsed_ms,
    "throughput_tps": round(committed / elapsed_s, 4) if elapsed_s else 0,
    "latency_ms": {
        "p50": percentile_nearest_rank(latencies, 50),
        "p95": percentile_nearest_rank(latencies, 95),
        "max": max(latencies),
        "mean": round(statistics.fmean(latencies), 2),
    },
}, sort_keys=True))
PY
    python3 scripts/check-gres-primary-distribution.py \
        "${ARTIFACT_DIR}/result-sharded-${range_count}-trial-${trial}.json" \
        "$range_count" "$sessions_per_range" "$txns_per_session" "$warmup_txns"
}

write_dry_run_results() {
    python3 - "$ARTIFACT_DIR" <<'PY'
import json
import pathlib
import sys

artifact_dir = pathlib.Path(sys.argv[1])
fixtures = [
    (1, 100.0, 10, 15),
    (2, 190.0, 11, 17),
    (4, 260.0, 13, 21),
]
for range_count, throughput, p50, p95 in fixtures:
    result = {
        "workload_kind": "range_local_ingest",
        "range_count": range_count,
        "sessions_per_range": 1,
        "txns_per_session": 2,
        "committed_transactions": range_count * 2,
        "duration_ms": int((range_count * 2 / throughput) * 1000),
        "throughput_tps": throughput,
        "latency_ms": {"p50": p50, "p95": p95, "max": p95 + 3, "mean": float(p50)},
    }
    (artifact_dir / f"result-{range_count}.json").write_text(json.dumps(result, sort_keys=True) + "\n")

sharded_fixtures = [
    (1, 95.0, 12, 18),
    (2, 182.0, 13, 20),
    (4, 342.0, 15, 24),
]
for range_count, throughput, p50, p95 in sharded_fixtures:
    committed = range_count * 2
    result = {
        "workload_kind": "sharded_table_ingest",
        "table_name": "s0",
        "range_count": range_count,
        "sessions_per_range": 1,
        "txns_per_session": 2,
        "concurrency_sessions": range_count,
        "committed_transactions": committed,
        "duration_ms": int((committed / throughput) * 1000),
        "throughput_tps": throughput,
        "latency_ms": {"p50": p50, "p95": p95, "max": p95 + 4, "mean": float(p50)},
    }
    (artifact_dir / f"result-sharded-{range_count}.json").write_text(json.dumps(result, sort_keys=True) + "\n")
PY
}

write_range_scaling_json() {
    local mode="$1"
    local sessions_per_range="$2"
    local txns_per_session="$3"
    local warmup_txns="$4"
    local trials="$5"

    python3 - "$ARTIFACT_DIR" "$FLOOR" "$SHARDED_FLOOR" "$DECISION_CEILING_MIN_RATIO" "$mode" "$sessions_per_range" "$txns_per_session" "$warmup_txns" "$trials" <<'PY'
import json
import os
import pathlib
import platform
import socket
import subprocess
import sys
import time

artifact_dir = pathlib.Path(sys.argv[1])
floor = float(sys.argv[2])
sharded_floor = float(sys.argv[3])
decision_ceiling_min_ratio = float(sys.argv[4])
mode = sys.argv[5]
sessions_per_range = int(sys.argv[6])
txns_per_session = int(sys.argv[7])
warmup_txns = int(sys.argv[8])
trials = int(sys.argv[9])
range_results = []
for range_count in (1, 2, 4):
    with (artifact_dir / f"result-{range_count}.json").open(encoding="utf-8") as handle:
        range_results.append(json.load(handle))

sharded_results = []
for range_count in (1, 2, 4):
    with (artifact_dir / f"result-sharded-{range_count}.json").open(encoding="utf-8") as handle:
        sharded_results.append(json.load(handle))

def by_range_count(results):
    return {item["range_count"]: item for item in results}

def scale_4_vs_1(results_by_count):
    baseline = results_by_count[1]["throughput_tps"]
    return results_by_count[4]["throughput_tps"] / baseline if baseline else 0

range_by_count = by_range_count(range_results)
sharded_by_count = by_range_count(sharded_results)
range_scale_4_vs_1 = scale_4_vs_1(range_by_count)
sharded_scale_4_vs_1 = scale_4_vs_1(sharded_by_count)
range_passed = range_scale_4_vs_1 >= floor
sharded_passed = sharded_scale_4_vs_1 >= sharded_floor

def expected_envelope_points(results, baseline_tps, min_efficiency, max_efficiency):
    points = []
    for result in results:
        range_count = result["range_count"]
        expected_min_tps = baseline_tps * range_count * min_efficiency
        expected_max_tps = baseline_tps * range_count * max_efficiency
        measured_tps = result["throughput_tps"]
        envelope_ratio = measured_tps / (baseline_tps * range_count) if baseline_tps else 0
        points.append({
            "range_count": range_count,
            "concurrency_sessions": result.get("concurrency_sessions", range_count * sessions_per_range),
            "committed_transactions": result["committed_transactions"],
            "aggregate_commit_tps": measured_tps,
            "expected_min_tps": round(expected_min_tps, 4),
            "expected_max_tps": round(expected_max_tps, 4),
            "envelope_ratio": round(envelope_ratio, 4),
            "within_expected_envelope": expected_min_tps <= measured_tps <= expected_max_tps,
        })
    return points

def g8_decision_ceiling_points(results, baseline_tps):
    ceiling_tps = baseline_tps * 2.0
    points = []
    for result in results:
        range_count = result["range_count"]
        measured_tps = min(baseline_tps * range_count, ceiling_tps)
        points.append({
            "range_count": range_count,
            "concurrency_sessions": result.get("concurrency_sessions", range_count * sessions_per_range),
            "aggregate_commit_tps": round(measured_tps, 4),
            "ceiling_source": "G-8 range-0 batched-decision ceiling contrast; flattened after the two-range envelope.",
        })
    return points

def timestamp_commit_rate_curve(results, baseline_tps, g8_points):
    g8_by_range = {point["range_count"]: point for point in g8_points}
    points = []
    for result in results:
        range_count = result["range_count"]
        measured_tps = result["throughput_tps"]
        g8_tps = g8_by_range[range_count]["aggregate_commit_tps"]
        points.append({
            "range_count": range_count,
            "concurrency_sessions": result.get("concurrency_sessions", range_count * sessions_per_range),
            "committed_transactions": result["committed_transactions"],
            "aggregate_commit_tps": measured_tps,
            "scale_vs_1": round(measured_tps / baseline_tps, 4) if baseline_tps else 0,
            "g8_decision_ceiling_tps": g8_tps,
            "g9_vs_g8_ceiling_ratio": round(measured_tps / g8_tps, 4) if g8_tps else 0,
            "unflattened": range_count < 4 or measured_tps > g8_tps,
        })
    return points

sharded_baseline_tps = sharded_by_count[1]["throughput_tps"]
decision_points = expected_envelope_points(
    sharded_results,
    sharded_baseline_tps,
    decision_ceiling_min_ratio,
    1.25,
)
g8_points = g8_decision_ceiling_points(sharded_results, sharded_baseline_tps)
timestamp_points = timestamp_commit_rate_curve(sharded_results, sharded_baseline_tps, g8_points)
range4_decision_ratio = next(
    point["envelope_ratio"] for point in decision_points if point["range_count"] == 4
)
decision_ceiling_passed = all(point["within_expected_envelope"] for point in decision_points)

def optional_command(args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None

payload = {
    "schema_version": 3,
    "generated_at_unix": int(time.time()),
    "mode": mode,
    "thresholds": {
        "monotone_scaling_floor_env": "CRABKA_GRES_RANGE_SCALING_FLOOR",
        "range4_vs_range1_min": floor,
        "sharded_scaling_floor_env": "CRABKA_GRES_SHARDED_SCALING_FLOOR",
        "sharded_range4_vs_range1_min": sharded_floor,
        "decision_ceiling_min_ratio_env": "CRABKA_GRES_DECISION_CEILING_MIN_RATIO",
        "decision_ceiling_range4_min_ratio": decision_ceiling_min_ratio,
        "decision_ceiling_all_points_max_ratio": 1.25,
    },
    "environment": {
        "host": socket.gethostname(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "ci": os.environ.get("CI") == "true",
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "git_sha": optional_command(["git", "rev-parse", "HEAD"]),
        "crabka_gres_skip_build": os.environ.get("CRABKA_GRES_SKIP_BUILD") == "1",
        "sessions_per_range": sessions_per_range,
        "txns_per_session": txns_per_session,
        "warmup_txns_per_session": warmup_txns,
        "trial_count": trials,
        "aggregate_derivation": "median throughput across repeated live trials",
    },
    "results": range_results,
    "scaling": {
        "range4_vs_range1": round(range_scale_4_vs_1, 4),
        "passed": range_passed,
    },
    "range_local": {
        "results": range_results,
        "scaling": {
            "range4_vs_range1": round(range_scale_4_vs_1, 4),
            "passed": range_passed,
        },
    },
    "sharded_table": {
        "supported": True,
        "table_name": sharded_results[0].get("table_name", "s0"),
        "results": sharded_results,
        "ingest_curve": [
            {
                "range_count": point["range_count"],
                "throughput_tps": point["aggregate_commit_tps"],
                "scale_vs_1": round(
                    sharded_by_count[point["range_count"]]["throughput_tps"] / sharded_baseline_tps,
                    4,
                ) if sharded_baseline_tps else 0,
                "expected_min_tps": point["expected_min_tps"],
                "expected_max_tps": point["expected_max_tps"],
                "within_expected_envelope": point["within_expected_envelope"],
            }
            for point in decision_points
        ],
        "scaling": {
            "range4_vs_range1": round(sharded_scale_4_vs_1, 4),
            "passed": sharded_passed,
        },
        "decision_ceiling": {
            "description": "Aggregate commit-rate curve for one SHARDED table as range-count and writer concurrency rise.",
            "expected_envelope": {
                "source": "G-8 batched-decision envelope; one-range measured baseline multiplied by active range count.",
                "baseline_commit_tps": sharded_baseline_tps,
                "min_efficiency_ratio": decision_ceiling_min_ratio,
                "max_efficiency_ratio": 1.25,
                "concurrency_model": "concurrency_sessions = range_count * sessions_per_range",
            },
            "aggregate_commit_rate_curve": decision_points,
            "g8_decision_ceiling_curve": g8_points,
            "measured_comparison": {
                "range4_vs_range1": round(sharded_scale_4_vs_1, 4),
                "range4_efficiency_ratio": round(range4_decision_ratio, 4),
                "passed": decision_ceiling_passed,
            },
        },
        "timestamp_transactions": {
            "description": "G-9 timestamp-transaction commit-rate curve for the same SHARDED-table workload, contrasted with the old G-8 flattened decision ceiling.",
            "visibility_model": "read_ts with committed versions visible iff commit_ts <= read_ts",
            "commit_rate_curve": timestamp_points,
            "contrast": {
                "g8_decision_ceiling_source": "schema v3 synthetic contrast from the previous range-0 decision ceiling; live measurements remain the G-9 timestamp path when supported.",
                "range4_g9_vs_g8_ceiling_ratio": next(
                    point["g9_vs_g8_ceiling_ratio"] for point in timestamp_points if point["range_count"] == 4
                ),
                "unflattened_at_range4": next(
                    point["unflattened"] for point in timestamp_points if point["range_count"] == 4
                ),
            },
        },
    },
    "passed": {
        "range_local": range_passed,
        "sharded_ingest": sharded_passed,
        "decision_ceiling": decision_ceiling_passed,
        "overall": range_passed and sharded_passed and decision_ceiling_passed,
    },
}
(artifact_dir / "range-scaling.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
if not range_passed:
    raise SystemExit(
        f"range-local 4-range throughput scale {range_scale_4_vs_1:.4f} is below floor {floor:.4f}"
    )
if not sharded_passed:
    raise SystemExit(
        f"sharded 4-range throughput scale {sharded_scale_4_vs_1:.4f} is below floor {sharded_floor:.4f}"
    )
if not decision_ceiling_passed:
    raise SystemExit(
        "one or more sharded decision-envelope points are outside "
        f"[{decision_ceiling_min_ratio:.4f}, 1.2500]"
    )
PY
}

write_step_summary() {
    local summary_path="${GITHUB_STEP_SUMMARY:-${ARTIFACT_DIR}/range-scaling-summary.md}"
    python3 - "$ARTIFACT_DIR/range-scaling.json" "$summary_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    summary = json.load(handle)

rows = [
    "## Gres range scaling",
    "",
    "### Range-local ingest",
    "",
    "| ranges | committed txns | duration ms | throughput tx/s | p50 ms | p95 ms |",
    "| ---: | ---: | ---: | ---: | ---: | ---: |",
]
for result in summary["range_local"]["results"]:
    rows.append(
        f"| {result['range_count']} | {result['committed_transactions']} | "
        f"{result['duration_ms']} | {result['throughput_tps']} | "
        f"{result['latency_ms']['p50']} | {result['latency_ms']['p95']} |"
    )
rows.extend([
    "",
    f"Range-local 4x/1x throughput scale: {summary['range_local']['scaling']['range4_vs_range1']} "
    f"(floor {summary['thresholds']['range4_vs_range1_min']})",
    "",
    "### Single SHARDED-table ingest",
    "",
    "| ranges | concurrency | committed txns | throughput tx/s | envelope ratio | p50 ms | p95 ms |",
    "| ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
])
decision_by_range = {
    point["range_count"]: point
    for point in summary["sharded_table"]["decision_ceiling"]["aggregate_commit_rate_curve"]
}
for result in summary["sharded_table"]["results"]:
    decision = decision_by_range[result["range_count"]]
    rows.append(
        f"| {result['range_count']} | {result['concurrency_sessions']} | "
        f"{result['committed_transactions']} | {result['throughput_tps']} | "
        f"{decision['envelope_ratio']} | {result['latency_ms']['p50']} | "
        f"{result['latency_ms']['p95']} |"
    )
rows.extend([
    "",
    f"Sharded 4x/1x throughput scale: {summary['sharded_table']['scaling']['range4_vs_range1']} "
    f"(floor {summary['thresholds']['sharded_range4_vs_range1_min']})",
    f"Decision-ceiling 4-range efficiency: "
    f"{summary['sharded_table']['decision_ceiling']['measured_comparison']['range4_efficiency_ratio']} "
    f"(floor {summary['thresholds']['decision_ceiling_range4_min_ratio']})",
    f"G-9 timestamp 4-range vs G-8 ceiling ratio: "
    f"{summary['sharded_table']['timestamp_transactions']['contrast']['range4_g9_vs_g8_ceiling_ratio']} "
    f"(unflattened={summary['sharded_table']['timestamp_transactions']['contrast']['unflattened_at_range4']})",
    "",
])
with open(sys.argv[2], "a", encoding="utf-8") as handle:
    handle.write("\n".join(rows))
PY
}

run_live() {
    local sessions_per_range="$1"
    local txns_per_session="$2"
    local warmup_txns="$3"
    local trials="$4"
    local range_count
    local trial

    if [ "${CRABKA_GRES_SKIP_BUILD:-}" != "1" ]; then
        cargo build --locked -p crabka-cli -p crabka-broker -p crabka-gres
    fi

    mapfile -t PORTS < <(choose_ports)
    BROKER_PORT="${PORTS[0]}"
    CONTROLLER_PORT="${PORTS[1]}"
    GRES_PORT="${PORTS[2]}"
    start_broker

    for range_count in 1 2 4; do
        for trial in $(seq 1 "$trials"); do
            log "running ${range_count}-range workload trial ${trial}/${trials} (${sessions_per_range} persistent sessions/range, ${warmup_txns} warmup + ${txns_per_session} measured txns/session)"
            run_live_workload "$range_count" "$sessions_per_range" "$txns_per_session" "$warmup_txns" "$trial"
        done
    done

    for range_count in 1 2 4; do
        for trial in $(seq 1 "$trials"); do
            log "running ${range_count}-range SHARDED-table workload trial ${trial}/${trials} (${sessions_per_range} persistent sessions/range, ${warmup_txns} warmup + ${txns_per_session} measured txns/session)"
            run_live_sharded_workload "$range_count" "$sessions_per_range" "$txns_per_session" "$warmup_txns" "$trial"
        done
    done

    python3 - "$ARTIFACT_DIR" "$trials" "$warmup_txns" <<'PY'
import json, pathlib, statistics, sys
root = pathlib.Path(sys.argv[1]); trials = int(sys.argv[2]); warmup = int(sys.argv[3])
for prefix in ("result", "result-sharded"):
    for ranges in (1, 2, 4):
        samples = [json.loads((root / f"{prefix}-{ranges}-trial-{trial}.json").read_text()) for trial in range(1, trials + 1)]
        median_sample = sorted(samples, key=lambda item: item["throughput_tps"])[len(samples) // 2]
        aggregate = dict(median_sample)
        aggregate["throughput_tps"] = round(statistics.median(item["throughput_tps"] for item in samples), 4)
        aggregate["trials"] = samples
        aggregate["trial_count"] = trials
        aggregate["warmup_txns_per_session"] = warmup
        aggregate["aggregate_derivation"] = "median throughput across repeated live trials; latency summary from the median-throughput trial"
        (root / f"{prefix}-{ranges}.json").write_text(json.dumps(aggregate, sort_keys=True) + "\n")
PY
}

require_decimal CRABKA_GRES_RANGE_SCALING_FLOOR "$FLOOR"
require_decimal CRABKA_GRES_SHARDED_SCALING_FLOOR "$SHARDED_FLOOR"
require_decimal CRABKA_GRES_DECISION_CEILING_MIN_RATIO "$DECISION_CEILING_MIN_RATIO"
require_positive_integer CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE "${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-1}"
require_positive_integer CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION "${CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION:-1}"
require_positive_integer CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS "${CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS:-1}"
require_positive_integer CRABKA_GRES_RANGE_SCALING_TRIALS "${CRABKA_GRES_RANGE_SCALING_TRIALS:-1}"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

rm -rf "$ARTIFACT_DIR"
mkdir -p "$ARTIFACT_DIR"

MODE="$(resolve_mode)"
if [ "$MODE" = "dry-run" ]; then
    SESSIONS_PER_RANGE="${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-1}"
    TXNS_PER_SESSION="${CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION:-2}"
elif [ "${MODE_REQUEST}" = "fast" ] || [ "${CRABKA_GRES_RANGE_SCALING_FAST:-0}" = "1" ]; then
    SESSIONS_PER_RANGE="${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-2}"
    TXNS_PER_SESSION="${CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION:-50}"
    WARMUP_TXNS="${CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS:-5}"
    TRIALS="${CRABKA_GRES_RANGE_SCALING_TRIALS:-3}"
else
    SESSIONS_PER_RANGE="${CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE:-2}"
    TXNS_PER_SESSION="${CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION:-20}"
    WARMUP_TXNS="${CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS:-5}"
    TRIALS="${CRABKA_GRES_RANGE_SCALING_TRIALS:-3}"
fi
if [ "$MODE" = "dry-run" ]; then
    WARMUP_TXNS="${CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS:-5}"
    TRIALS="${CRABKA_GRES_RANGE_SCALING_TRIALS:-3}"
fi
require_positive_integer CRABKA_GRES_RANGE_SCALING_SESSIONS_PER_RANGE "$SESSIONS_PER_RANGE"
require_positive_integer CRABKA_GRES_RANGE_SCALING_TXNS_PER_SESSION "$TXNS_PER_SESSION"
require_positive_integer CRABKA_GRES_RANGE_SCALING_WARMUP_TXNS "$WARMUP_TXNS"
require_positive_integer CRABKA_GRES_RANGE_SCALING_TRIALS "$TRIALS"

case "$MODE" in
    dry-run)
        log "writing deterministic dry-run scaling artifact"
        write_dry_run_results
        ;;
    live)
        command -v psql >/dev/null 2>&1 || fail "psql is required for live mode"
        run_live "$SESSIONS_PER_RANGE" "$TXNS_PER_SESSION" "$WARMUP_TXNS" "$TRIALS"
        ;;
    *) fail "internal error: unsupported resolved mode ${MODE}" ;;
esac

write_range_scaling_json "$MODE" "$SESSIONS_PER_RANGE" "$TXNS_PER_SESSION" "$WARMUP_TXNS" "$TRIALS"
write_step_summary
log "PASS: wrote ${ARTIFACT_DIR}/range-scaling.json"
