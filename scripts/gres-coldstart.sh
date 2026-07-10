#!/usr/bin/env bash
# Gres cold-start SLO gate: suspended tenant -> PgDog -> activator -> compute.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/gres-coldstart.sh [--help]

Measures first-connection-to-SELECT-1 latency through PgDog and the Gres
activator for a small suspended tenant. Each iteration forces the tenant back to
Suspended, kills the compute, lets a local controller loop observe the
activator's ResumeRequested record, starts the compute, marks it Active, and
records the psql round-trip latency.

Environment:
  CRABKA_GRES_SKIP_BUILD=1                  Reuse existing target/debug binaries.
  CRABKA_GRES_COLDSTART_ITERATIONS=<n>      Iterations to measure (default: 10).
  CRABKA_GRES_COLDSTART_ARTIFACT_DIR=<dir>  Artifact directory (default: target/gres-coldstart-artifacts).
  CRABKA_GRES_COLDSTART_KEEP_ARTIFACTS=1    Keep artifacts after a successful run.
  CRABKA_GRES_COLDSTART_P95_CEILING_MS=<ms> CI backstop ceiling for p95 (default: 30000).
  CRABKA_GRES_PGDOG_IMAGE=<image>           Override the pinned PgDog image.
EOF
}

case "${1:-}" in
    "") ;;
    --help|-h) usage; exit 0 ;;
    *) echo "FAIL: unknown argument $1" >&2; usage >&2; exit 2 ;;
esac

readonly TENANT="coldstart-a"
readonly SQL_USER="colduser"
readonly SQL_PASSWORD="cold-secret"
readonly COMPUTE_HOST="127.0.0.2"
readonly COMPUTE_PORT="5432"
readonly CLUSTER_ID="00000000-0000-0000-0000-000000000001"
readonly PGDOG_IMAGE="${CRABKA_GRES_PGDOG_IMAGE:-ghcr.io/pgdogdev/pgdog:0.1.6}"
readonly ARTIFACT_DIR="${CRABKA_GRES_COLDSTART_ARTIFACT_DIR:-target/gres-coldstart-artifacts}"
readonly ITERATIONS="${CRABKA_GRES_COLDSTART_ITERATIONS:-10}"
# This is a deliberately generous CI-environment backstop, not the product SLO.
readonly P95_CEILING_MS="${CRABKA_GRES_COLDSTART_P95_CEILING_MS:-30000}"

BROKER_PID=""
ACTIVATOR_PID=""
CONTROLLER_PID=""
PGDOG_CONTAINER=""

log() {
    printf 'gres-coldstart: %s\n' "$*"
}

dump_diagnostics() {
    echo "---- gres-coldstart artifacts: ${ARTIFACT_DIR} ----" >&2
    for file in "${ARTIFACT_DIR}"/*.log "${ARTIFACT_DIR}"/*.err "${ARTIFACT_DIR}"/*.out; do
        [ -f "$file" ] || continue
        echo "---- ${file} ----" >&2
        tail -n 120 "$file" >&2 || true
    done
}

fail() {
    echo "FAIL: $*" >&2
    dump_diagnostics
    exit 1
}

cleanup() {
    local status=$?
    docker rm -f "${PGDOG_CONTAINER:-}" >/dev/null 2>&1 || true
    kill "${CONTROLLER_PID:-}" "${ACTIVATOR_PID:-}" "${BROKER_PID:-}" 2>/dev/null || true
    if [ -f "${ARTIFACT_DIR}/compute.pid" ]; then
        kill "$(cat "${ARTIFACT_DIR}/compute.pid")" 2>/dev/null || true
    fi
    wait "${CONTROLLER_PID:-}" "${ACTIVATOR_PID:-}" "${BROKER_PID:-}" 2>/dev/null || true
    if [ -f "${ARTIFACT_DIR}/compute.pid" ]; then
        wait "$(cat "${ARTIFACT_DIR}/compute.pid")" 2>/dev/null || true
    fi
    if [ "$status" -ne 0 ]; then
        dump_diagnostics
    fi
    if [ "${CRABKA_GRES_COLDSTART_KEEP_ARTIFACTS:-0}" != "1" ] && [ "$status" -eq 0 ]; then
        rm -rf "$ARTIFACT_DIR"
    else
        log "kept artifacts in ${ARTIFACT_DIR}"
    fi
}
trap cleanup EXIT

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

require_positive_integer() {
    local name="$1"
    local value="$2"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        fail "${name} must be a positive integer, got '${value}'"
    fi
}

choose_ports() {
    python3 - <<'PY'
import socket

sockets = []
try:
    for _ in range(4):
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
    local label="$1"
    local conninfo="$2"
    local password="$3"

    for _ in $(seq 120); do
        if PGAPPNAME= PGPASSWORD="$password" psql "$conninfo" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done

    fail "${label} did not become SQL-ready"
}

docker_is_available() {
    command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

tenant_state() {
    ./target/debug/crabka gres describe \
        --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --name "$TENANT" \
        >"${ARTIFACT_DIR}/describe-${TENANT}.json"
    python3 - "${ARTIFACT_DIR}/describe-${TENANT}.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["state"])
PY
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

[[listeners]]
name = "sasl"
bind_addr = "127.0.0.1:${SASL_PORT}"
advertised = "127.0.0.1:${SASL_PORT}"
protocol = "SaslPlaintext"
sasl_config = { enabled_mechanisms = ["SCRAM-SHA-512"] }

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

create_tenant() {
    printf '%s\n' "$SQL_PASSWORD" >"${ARTIFACT_DIR}/${TENANT}.password"
    ./target/debug/crabka gres create-tenant \
        --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --name "$TENANT" \
        --user "$SQL_USER" \
        --password-file "${ARTIFACT_DIR}/${TENANT}.password" \
        >"${ARTIFACT_DIR}/create-${TENANT}.log" 2>&1
}

start_compute() {
    GRES_KAFKA_USERNAME="gres-${TENANT}" GRES_KAFKA_PASSWORD="$SQL_PASSWORD" \
        ./target/debug/crabka-gres \
            --listen "${COMPUTE_HOST}:${COMPUTE_PORT}" \
            --substrate-bootstrap "127.0.0.1:${SASL_PORT}" \
            --tenant "$TENANT" \
            >"${ARTIFACT_DIR}/compute-$(date +%s%N).log" 2>&1 &
    printf '%s\n' "$!" >"${ARTIFACT_DIR}/compute.pid"
}

stop_compute() {
    if [ ! -f "${ARTIFACT_DIR}/compute.pid" ]; then
        return 0
    fi
    kill "$(cat "${ARTIFACT_DIR}/compute.pid")" 2>/dev/null || true
    wait "$(cat "${ARTIFACT_DIR}/compute.pid")" 2>/dev/null || true
    rm -f "${ARTIFACT_DIR}/compute.pid"
}

force_suspend() {
    ./target/debug/crabka gres suspend \
        --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --name "$TENANT" \
        >"${ARTIFACT_DIR}/suspend-${TENANT}.log" 2>&1
    stop_compute
}

patch_pgdog_listen_port() {
    python3 - "$PGDOG_PORT" "$P95_CEILING_MS" "${ARTIFACT_DIR}/pgdog/pgdog.toml" <<'PY'
import pathlib
import sys

port = sys.argv[1]
ceiling_ms = int(sys.argv[2])
path = pathlib.Path(sys.argv[3])
text = path.read_text()
text = text.replace("listen_port = 6432", f"listen_port = {port}", 1)
text = text.replace("connect_timeout = 10", f"connect_timeout = {ceiling_ms + 10000}", 1)
text = text.replace("checkout_timeout = 30", f"checkout_timeout = {ceiling_ms + 10000}", 1)
path.write_text(text)
PY
}

patch_pgdog_local_users() {
    cat >"${ARTIFACT_DIR}/pgdog/users.toml" <<EOF
[[users]]
name = "${SQL_USER}"
database = "${TENANT}"
password = "${SQL_PASSWORD}"
EOF
}

start_activator() {
    ./target/debug/crabka-gres-activator \
        --listen "127.0.0.1:${ACTIVATOR_PORT}" \
        --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --registry-poll-ms 100 \
        --cold-start-timeout-ms "$((P95_CEILING_MS + 10000))" \
        --backend-endpoint-template "${COMPUTE_HOST}:${COMPUTE_PORT}" \
        >"${ARTIFACT_DIR}/activator.log" 2>&1 &
    ACTIVATOR_PID=$!
    wait_for_tcp_port 127.0.0.1 "$ACTIVATOR_PORT" activator
}

start_pgdog() {
    docker pull "$PGDOG_IMAGE" >"${ARTIFACT_DIR}/pull-pgdog.log" 2>&1
    PGDOG_CONTAINER=$(docker run -d --network host \
        --name "crabka-gres-coldstart-pgdog-${PGDOG_PORT}" \
        -v "${PWD}/${ARTIFACT_DIR}/pgdog:/etc/pgdog:ro" \
        "$PGDOG_IMAGE" \
        /usr/local/bin/pgdog --config /etc/pgdog/pgdog.toml --users /etc/pgdog/users.toml run)
    docker logs -f "$PGDOG_CONTAINER" >"${ARTIFACT_DIR}/pgdog-container.log" 2>&1 &
    wait_for_tcp_port 127.0.0.1 "$PGDOG_PORT" pgdog
}

start_controller_loop() {
    (
        while true; do
            state="$(tenant_state 2>"${ARTIFACT_DIR}/controller-describe.err" || true)"
            if [ "$state" = "resume_requested" ]; then
                if [ ! -f "${ARTIFACT_DIR}/compute.pid" ] || ! kill -0 "$(cat "${ARTIFACT_DIR}/compute.pid")" 2>/dev/null; then
                    start_compute
                fi
                if PGAPPNAME= PGPASSWORD="$SQL_PASSWORD" psql "host=${COMPUTE_HOST} port=${COMPUTE_PORT} user=${SQL_USER} dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1; then
                    ./target/debug/crabka gres resume \
                        --bootstrap "127.0.0.1:${BROKER_PORT}" \
                        --name "$TENANT" \
                        >>"${ARTIFACT_DIR}/controller-resume.log" 2>&1 || true
                fi
            fi
            sleep 0.1
        done
    ) >"${ARTIFACT_DIR}/controller.log" 2>&1 &
    CONTROLLER_PID=$!
}

measure_select_one_ms() {
    local conninfo="$1"
    local password="$2"
    local timeout_seconds="$3"

    python3 - "$conninfo" "$password" "$timeout_seconds" <<'PY'
import os
import subprocess
import sys
import time

conninfo = sys.argv[1]
password = sys.argv[2]
timeout_seconds = float(sys.argv[3])
env = os.environ.copy()
env["PGAPPNAME"] = ""
env["PGPASSWORD"] = password
started = time.monotonic_ns()
result = subprocess.run(
    ["psql", conninfo, "-tAc", "SELECT 1"],
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    timeout=timeout_seconds,
    check=False,
)
elapsed_ms = (time.monotonic_ns() - started) // 1_000_000
if result.returncode != 0:
    sys.stderr.write(result.stderr)
    sys.exit(result.returncode)
if result.stdout.strip() != "1":
    sys.stderr.write(f"expected SELECT 1 to return 1, got {result.stdout!r}\n")
    sys.exit(1)
print(elapsed_ms)
PY
}

write_coldstart_json() {
    python3 - "$ARTIFACT_DIR" "$P95_CEILING_MS" <<'PY'
import json
import math
import pathlib
import statistics
import sys

artifact_dir = pathlib.Path(sys.argv[1])
ceiling_ms = int(sys.argv[2])
timings = []
for line in (artifact_dir / "iteration-timings.tsv").read_text().splitlines():
    if not line.strip():
        continue
    iteration, latency_ms = line.split("\t")
    timings.append({"iteration": int(iteration), "latency_ms": int(latency_ms)})

if not timings:
    raise SystemExit("no cold-start timings recorded")

latencies = sorted(item["latency_ms"] for item in timings)
duration_s = sum(latencies) / 1000
wake_rate = len(latencies) / duration_s if duration_s else 0

def percentile_nearest_rank(values, percentile):
    rank = math.ceil((percentile / 100) * len(values))
    return values[max(0, min(rank - 1, len(values) - 1))]

summary = {
    "iterations": len(timings),
    "p50_ms": percentile_nearest_rank(latencies, 50),
    "p95_ms": percentile_nearest_rank(latencies, 95),
    "max_ms": max(latencies),
    "mean_ms": round(statistics.fmean(latencies), 2),
    "sustained_wake_rate_per_second": round(wake_rate, 4),
    "p95_ceiling_ms": ceiling_ms,
    "timings": timings,
}
(artifact_dir / "coldstart.json").write_text(json.dumps(summary, indent=2) + "\n")
PY
}

enforce_coldstart_ceiling() {
    python3 - "$ARTIFACT_DIR/coldstart.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    summary = json.load(handle)
if summary["p95_ms"] > summary["p95_ceiling_ms"]:
    raise SystemExit(
        f"cold-start p95 {summary['p95_ms']} ms exceeds ceiling {summary['p95_ceiling_ms']} ms"
    )
PY
}

write_step_summary() {
    local summary_path="${GITHUB_STEP_SUMMARY:-${ARTIFACT_DIR}/coldstart-summary.md}"
    python3 - "$ARTIFACT_DIR/coldstart.json" "$summary_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    summary = json.load(handle)

table = "\n".join([
    "## Gres cold-start SLO",
    "",
    "| iterations | p50 ms | p95 ms | max ms | wake/s | ceiling ms |",
    "| ---: | ---: | ---: | ---: | ---: | ---: |",
    f"| {summary['iterations']} | {summary['p50_ms']} | {summary['p95_ms']} | {summary['max_ms']} | {summary['sustained_wake_rate_per_second']} | {summary['p95_ceiling_ms']} |",
    "",
])
with open(sys.argv[2], "a", encoding="utf-8") as handle:
    handle.write(table)
PY
}

require_positive_integer CRABKA_GRES_COLDSTART_ITERATIONS "$ITERATIONS"
require_positive_integer CRABKA_GRES_COLDSTART_P95_CEILING_MS "$P95_CEILING_MS"
require_command python3
require_command psql
docker_is_available || fail "Docker/PgDog runtime unavailable"

mapfile -t PORTS < <(choose_ports)
BROKER_PORT="${PORTS[0]}"
CONTROLLER_PORT="${PORTS[1]}"
SASL_PORT="${PORTS[2]}"
ACTIVATOR_PORT="${PORTS[3]}"
PGDOG_PORT="${CRABKA_GRES_PGDOG_PORT:-6432}"

rm -rf "$ARTIFACT_DIR"
mkdir -p "${ARTIFACT_DIR}/pgdog"
: >"${ARTIFACT_DIR}/iteration-timings.tsv"

if [ "${CRABKA_GRES_SKIP_BUILD:-}" != "1" ]; then
    cargo build --locked -p crabka-cli -p crabka-broker -p crabka-gres -p crabka-gres-activator
fi

start_broker
create_tenant
start_compute
wait_for_sql "${TENANT} direct compute" "host=${COMPUTE_HOST} port=${COMPUTE_PORT} user=${SQL_USER} dbname=crab sslmode=prefer" "$SQL_PASSWORD"
PGAPPNAME= PGPASSWORD="$SQL_PASSWORD" psql "host=${COMPUTE_HOST} port=${COMPUTE_PORT} user=${SQL_USER} dbname=crab sslmode=prefer" -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE coldstart_marker (id int4); INSERT INTO coldstart_marker VALUES (1);" \
    >"${ARTIFACT_DIR}/warmup-workload.log" 2>&1

force_suspend
./target/debug/crabka gres render-pgdog \
    --bootstrap "127.0.0.1:${BROKER_PORT}" \
    --activator "127.0.0.1:${ACTIVATOR_PORT}" \
    --out-dir "${ARTIFACT_DIR}/pgdog" \
    >"${ARTIFACT_DIR}/render-pgdog.log" 2>&1
patch_pgdog_listen_port
patch_pgdog_local_users
start_activator
start_pgdog
start_controller_loop

TENANT_CONN="host=127.0.0.1 port=${PGDOG_PORT} dbname=${TENANT} user=${SQL_USER} sslmode=prefer"
PSQL_TIMEOUT_SECONDS=$(((P95_CEILING_MS / 1000) + 20))

for iteration in $(seq 1 "$ITERATIONS"); do
    force_suspend
    log "iteration ${iteration}/${ITERATIONS}: measuring cold wake"
    if ! latency_ms="$(measure_select_one_ms "$TENANT_CONN" "$SQL_PASSWORD" "$PSQL_TIMEOUT_SECONDS" 2>"${ARTIFACT_DIR}/iteration-${iteration}.err")"; then
        fail "iteration ${iteration} failed"
    fi
    printf '%s\t%s\n' "$iteration" "$latency_ms" >>"${ARTIFACT_DIR}/iteration-timings.tsv"
    log "iteration ${iteration}/${ITERATIONS}: ${latency_ms} ms"
done

write_coldstart_json
enforce_coldstart_ceiling
write_step_summary
log "PASS: cold-start p95 within ${P95_CEILING_MS} ms ceiling"
