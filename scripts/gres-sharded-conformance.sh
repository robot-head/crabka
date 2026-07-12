#!/usr/bin/env bash
# G-8 sharded conformance gate: deterministic metadata/global-visibility/scanner tests.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/gres-sharded-conformance.sh [--help]

Runs the deterministic G-8 sharded conformance tests. In live mode it also runs
the primary PostgreSQL oracle corpus against SHARDED tables on a two-range Gres
tenant. The gate fails loudly on any regression and writes machine-readable
reports under the artifact directory.

Environment:
  CRABKA_GRES_SHARDED_CONFORMANCE_ARTIFACT_DIR=dir
      Artifact directory (default: target/gres-sharded-conformance-artifacts).
  CRABKA_GRES_SHARDED_CONFORMANCE_EXTRA_ARGS=args
      Extra arguments appended after `--` for each cargo test invocation.
  CRABKA_GRES_SHARDED_CONFORMANCE_MODE=static|live
      Run deterministic tests only (default), or also run the live corpus gate.
  CRABKA_GRES_SHARDED_ORACLE_URL=url
      PostgreSQL admin connection used to recreate the live oracle database.
EOF
}

case "${1:-}" in
    "") ;;
    --help|-h) usage; exit 0 ;;
    *) echo "FAIL: unknown argument $1" >&2; usage >&2; exit 2 ;;
esac

readonly ARTIFACT_DIR="${CRABKA_GRES_SHARDED_CONFORMANCE_ARTIFACT_DIR:-target/gres-sharded-conformance-artifacts}"
readonly RESULTS_TSV="${ARTIFACT_DIR}/results.tsv"
readonly EXTRA_ARGS="${CRABKA_GRES_SHARDED_CONFORMANCE_EXTRA_ARGS:-}"
readonly MODE="${CRABKA_GRES_SHARDED_CONFORMANCE_MODE:-static}"
readonly ORACLE_ADMIN_URL="${CRABKA_GRES_SHARDED_ORACLE_URL:-host=127.0.0.1 port=5432 user=postgres dbname=postgres password=postgres}"
readonly CLUSTER_ID="00000000-0000-0000-0000-000000000001"
BROKER_PID=""
GRES_PID=""
BROKER_PORT=""
CONTROLLER_PORT=""
GRES_PORT=""

cleanup() {
    local status=$?
    kill "${GRES_PID:-}" 2>/dev/null || true
    wait "${GRES_PID:-}" 2>/dev/null || true
    kill "${BROKER_PID:-}" 2>/dev/null || true
    wait "${BROKER_PID:-}" 2>/dev/null || true
    return "$status"
}
trap cleanup EXIT

choose_ports() {
    python3 - <<'PY'
import socket
sockets = []
try:
    for _ in range(3):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    for sock in sockets:
        print(sock.getsockname()[1])
finally:
    for sock in sockets:
        sock.close()
PY
}

wait_for_tcp() {
    local port="$1"
    local label="$2"
    for _ in $(seq 1 100); do
        if python3 - "$port" <<'PY' >/dev/null 2>&1
import socket, sys
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.2):
    pass
PY
        then return 0; fi
        sleep 0.2
    done
    echo "FAIL: ${label} did not open port ${port}" >&2
    return 1
}

run_live_corpus() {
    command -v psql >/dev/null 2>&1 || { echo "FAIL: psql is required for live mode" >&2; return 1; }
    if [ "${CRABKA_GRES_SKIP_BUILD:-0}" != "1" ]; then
        cargo build --locked \
            -p crabka-cli --bin crabka \
            -p crabka-broker --bin crabka-broker \
            -p crabka-gres --bin crabka-gres \
            -p crabka-gres-conformance --bin crabka-gres-conformance \
            >"${ARTIFACT_DIR}/build.log" 2>&1
    fi
    mapfile -t ports < <(choose_ports)
    BROKER_PORT="${ports[0]}"
    CONTROLLER_PORT="${ports[1]}"
    GRES_PORT="${ports[2]}"

    ./target/debug/crabka format \
        --log-dir "${ARTIFACT_DIR}/broker-data" --cluster-id "$CLUSTER_ID" \
        --standalone --node-id 1 --controller-listener "127.0.0.1:${CONTROLLER_PORT}" \
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
    ./target/debug/crabka-broker --log-dir "${ARTIFACT_DIR}/broker-data" \
        --cluster-id "$CLUSTER_ID" --broker-id 1 --config-file "${ARTIFACT_DIR}/broker.toml" \
        >"${ARTIFACT_DIR}/broker.log" 2>&1 &
    BROKER_PID=$!
    wait_for_tcp "$BROKER_PORT" broker

    printf '%s\n' 'corpus-secret' >"${ARTIFACT_DIR}/tenant.password"
    ./target/debug/crabka gres create-tenant --bootstrap "127.0.0.1:${BROKER_PORT}" \
        --name sharded-corpus --user corpus --password-file "${ARTIFACT_DIR}/tenant.password" \
        --ranges 0,0:2 >"${ARTIFACT_DIR}/create-tenant.log" 2>&1
    ./target/debug/crabka-gres --listen "127.0.0.1:${GRES_PORT}" \
        --substrate-bootstrap "127.0.0.1:${BROKER_PORT}" --tenant sharded-corpus \
        --ranges 0,0:2 --auth trust >"${ARTIFACT_DIR}/gres.log" 2>&1 &
    GRES_PID=$!
    for _ in $(seq 1 120); do
        if psql "host=127.0.0.1 port=${GRES_PORT} user=corpus dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null 2>&1; then break; fi
        kill -0 "$GRES_PID" 2>/dev/null || { echo "FAIL: Gres exited before readiness" >&2; return 1; }
        sleep 0.25
    done
    psql "host=127.0.0.1 port=${GRES_PORT} user=corpus dbname=crab sslmode=prefer" -tAc 'SELECT 1' >/dev/null

    psql "$ORACLE_ADMIN_URL" -v ON_ERROR_STOP=1 -c 'DROP DATABASE IF EXISTS gres_sharded_oracle WITH (FORCE)' \
        >"${ARTIFACT_DIR}/oracle-setup.log" 2>&1
    psql "$ORACLE_ADMIN_URL" -v ON_ERROR_STOP=1 -c 'CREATE DATABASE gres_sharded_oracle' \
        >>"${ARTIFACT_DIR}/oracle-setup.log" 2>&1
    local oracle_url="${ORACLE_ADMIN_URL/dbname=postgres/dbname=gres_sharded_oracle}"
    ./target/debug/crabka-gres-conformance \
        --oracle-url "$oracle_url" \
        --subject-url "host=127.0.0.1 port=${GRES_PORT} user=corpus dbname=crab" \
        --subject-sharded-ddl \
        --corpus crates/gres-conformance/corpus \
        --baseline crates/gres-conformance/baseline.json \
        --out "${ARTIFACT_DIR}/parity-sharded.json" \
        --summary "${ARTIFACT_DIR}/parity-sharded.md" \
        >"${ARTIFACT_DIR}/conformance.log" 2>&1

    python3 scripts/gres-sharded-evidence.py "${ARTIFACT_DIR}/gres.log" \
        "${ARTIFACT_DIR}/corpus-through-sharding.json"
    python3 - "$ARTIFACT_DIR" <<'PY'
import json, pathlib, sys
artifact = pathlib.Path(sys.argv[1])
path = artifact / "corpus-through-sharding.json"
payload = json.loads(path.read_text())
payload.update({
    "mode": "live",
    "range_count": 2,
    "subject_ddl": "sharded",
    "baseline": "crates/gres-conformance/baseline.json",
    "passed": True,
})
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
}

log() {
    printf 'gres-sharded-conformance: %s\n' "$*"
}

dump_diagnostics() {
    echo "---- gres-sharded-conformance artifacts: ${ARTIFACT_DIR} ----" >&2
    for file in "${ARTIFACT_DIR}"/*.log; do
        [ -f "$file" ] || continue
        echo "---- ${file} ----" >&2
        tail -n 160 "$file" >&2 || true
    done
}

write_report() {
    local overall_status="$1"

    python3 - "$ARTIFACT_DIR" "$RESULTS_TSV" "$overall_status" <<'PY'
import json
import os
import pathlib
import platform
import socket
import subprocess
import sys
import time

artifact_dir = pathlib.Path(sys.argv[1])
results_tsv = pathlib.Path(sys.argv[2])
overall_status = sys.argv[3]

def optional_command(args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None

tests = []
if results_tsv.exists():
    for line in results_tsv.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        name, package, target, status, log_path = line.split("\t")
        tests.append({
            "name": name,
            "package": package,
            "target": target,
            "status": status,
            "log": log_path,
        })

payload = {
    "schema_version": 1,
    "generated_at_unix": int(time.time()),
    "gate": "G-8 sharded conformance",
    "description": "Deterministic metadata, global-visibility, scanner, and proof-style tests for sharded Gres tables.",
    "environment": {
        "host": socket.gethostname(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "ci": os.environ.get("CI") == "true",
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "git_sha": optional_command(["git", "rev-parse", "HEAD"]),
        "rustc": optional_command(["rustc", "--version"]),
        "cargo": optional_command(["cargo", "--version"]),
    },
    "tests": tests,
    "passed": overall_status == "passed",
}
runtime_path = artifact_dir / "corpus-through-sharding.json"
if runtime_path.exists():
    payload["corpus_through_sharding"] = json.loads(runtime_path.read_text(encoding="utf-8"))
(artifact_dir / "sharded-conformance.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

run_gate() {
    local name="$1"
    local package="$2"
    local target="$3"
    shift 3

    local log_file="${ARTIFACT_DIR}/${name}.log"
    log "running ${name}"
    if [ -n "$EXTRA_ARGS" ]; then
        "$@" -- $EXTRA_ARGS >"$log_file" 2>&1
    else
        "$@" >"$log_file" 2>&1
    fi
    local status=$?
    local status_text="passed"
    if [ "$status" -ne 0 ]; then
        status_text="failed"
    fi
    printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$package" "$target" "$status_text" "$log_file" >>"$RESULTS_TSV"
    return "$status"
}

command -v python3 >/dev/null 2>&1 || { echo "FAIL: python3 is required" >&2; exit 1; }
command -v cargo >/dev/null 2>&1 || { echo "FAIL: cargo is required" >&2; exit 1; }
case "$MODE" in
    static|live) ;;
    *) echo "FAIL: CRABKA_GRES_SHARDED_CONFORMANCE_MODE must be static or live" >&2; exit 2 ;;
esac

rm -rf "$ARTIFACT_DIR"
mkdir -p "$ARTIFACT_DIR"
: >"$RESULTS_TSV"

status=0
run_gate sharded-visibility crabka-gres-ranges sharded_visibility \
    cargo test -p crabka-gres-ranges --test sharded_visibility || status=1
run_gate multirange-global-visibility crabka-gres-ranges multirange \
    cargo test -p crabka-gres-ranges --test multirange || status=1
run_gate pgexec-global-decisions crabka-pgexec transactions \
    cargo test -p crabka-pgexec --test transactions || status=1
run_gate pgexec-sharded-seams crabka-pgexec lib \
    cargo test -p crabka-pgexec create_table_sharded_persists_catalog_metadata || status=1

if [ "$status" -eq 0 ] && [ "$MODE" = "live" ]; then
    log "running primary PostgreSQL corpus through a live two-range SHARDED tenant"
    run_live_corpus || status=1
fi

if [ "$status" -eq 0 ]; then
    write_report passed
    log "PASS: wrote ${ARTIFACT_DIR}/sharded-conformance.json"
    exit 0
fi

write_report failed
dump_diagnostics
echo "FAIL: one or more G-8 sharded conformance gates failed" >&2
exit 1
