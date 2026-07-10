#!/usr/bin/env bash
# Durability across a real binary restart: insert, kill, restart on the same
# data dir, select the data back.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
    cargo test -p crabka-gres runtime_reopens_durable_local_storage
    exit 0
fi

choose_port() {
    python3 - <<'PY'
import socket

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

PORT="${1:-$(choose_port)}"
DATA_DIR="$(mktemp -d)"
PID=""

cleanup() {
    kill "${PID:-}" 2>/dev/null || true
    wait "${PID:-}" 2>/dev/null || true
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

CONNINFO="host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=prefer"

start_server() {
    ./target/debug/crabka-gres --listen "127.0.0.1:${PORT}" --data-dir "$DATA_DIR" \
        >"${DATA_DIR}/server.log" 2>&1 &
    PID=$!
}

wait_until_ready() {
    for _ in $(seq 40); do
        if psql "$CONNINFO" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.3
    done
    return 1
}

stop_server() {
    kill "$PID"
    wait "$PID" 2>/dev/null || true
    PID=""
}

cargo build -p crabka-gres

start_server
if ! wait_until_ready; then
    echo "FAIL: first boot not ready" >&2
    exit 1
fi
psql "$CONNINFO" -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE t (id int4, name text); INSERT INTO t VALUES (1,'durable');"
stop_server

start_server
if ! wait_until_ready; then
    echo "FAIL: second boot not ready" >&2
    exit 1
fi

out=$(psql "$CONNINFO" -tAc "SELECT name FROM t WHERE id = 1")
if [ "$out" = "durable" ]; then
    echo "PASS: data survived restart -> ${out}"
    exit 0
fi

echo "FAIL: expected 'durable', got '${out}'" >&2
exit 1
