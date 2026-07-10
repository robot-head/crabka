#!/usr/bin/env bash
# End-to-end check with a real psql client. sslmode=prefer exercises the
# SSLRequest -> 'N' -> plaintext fallback path.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
    cargo test -p crabka-gres runtime_serves_sql_over_pgwire
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
DATA_ROOT="$(mktemp -d)"
SERVER_PID=""
TLS_PID=""
SCRAM_TLS_PID=""

cleanup() {
    kill "${SERVER_PID:-}" "${TLS_PID:-}" "${SCRAM_TLS_PID:-}" 2>/dev/null || true
    wait "${SERVER_PID:-}" "${TLS_PID:-}" "${SCRAM_TLS_PID:-}" 2>/dev/null || true
    rm -rf "$DATA_ROOT"
}
trap cleanup EXIT

connection_string() {
    local port="$1"
    local sslmode="$2"
    local extra="${3:-}"
    printf 'host=127.0.0.1 port=%s user=crab dbname=crab sslmode=%s%s' "$port" "$sslmode" "$extra"
}

wait_for_select_one() {
    local label="$1"
    local conninfo="$2"
    local password="${3:-}"

    for _ in $(seq 40); do
        if PGPASSWORD="$password" psql "$conninfo" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.3
    done

    echo "FAIL: ${label} server not ready" >&2
    return 1
}

expect_select_one() {
    local label="$1"
    local conninfo="$2"
    local password="${3:-}"
    local out

    out=$(PGPASSWORD="$password" psql "$conninfo" -tAc 'SELECT 1')
    if [ "$out" = "1" ]; then
        echo "PASS: ${label} SELECT 1 -> ${out}"
        return 0
    fi

    echo "FAIL (${label}): expected '1', got '${out}'" >&2
    return 1
}

cargo build -p crabka-gres

./target/debug/crabka-gres --listen "127.0.0.1:${PORT}" \
    --data-dir "${DATA_ROOT}/plain" \
    >"${DATA_ROOT}/plain.log" 2>&1 &
SERVER_PID=$!

PLAIN_CONN=$(connection_string "$PORT" "prefer")
wait_for_select_one "psql" "$PLAIN_CONN"
expect_select_one "psql" "$PLAIN_CONN"

CERT_DIR="crates/pgwire/tests/fixtures"
if [ ! -f "${CERT_DIR}/test-server.pem" ]; then
    exit 0
fi

TLS_PORT=$((PORT + 1))
./target/debug/crabka-gres --listen "127.0.0.1:${TLS_PORT}" \
    --data-dir "${DATA_ROOT}/tls" \
    --tls-cert "${CERT_DIR}/test-server.pem" \
    --tls-key "${CERT_DIR}/test-server-key.pem" \
    >"${DATA_ROOT}/tls.log" 2>&1 &
TLS_PID=$!

TLS_CONN=$(connection_string "$TLS_PORT" "require" " sslrootcert=${CERT_DIR}/test-ca.pem")
wait_for_select_one "TLS" "$TLS_CONN"
expect_select_one "psql over TLS" "$TLS_CONN"

SCRAM_TLS_PORT=$((PORT + 2))
./target/debug/crabka-gres --listen "127.0.0.1:${SCRAM_TLS_PORT}" \
    --data-dir "${DATA_ROOT}/scram-tls" \
    --tls-cert "${CERT_DIR}/test-server.pem" \
    --tls-key "${CERT_DIR}/test-server-key.pem" \
    --auth scram \
    --user-cred "crab=hunter2" \
    >"${DATA_ROOT}/scram-tls.log" 2>&1 &
SCRAM_TLS_PID=$!

SCRAM_TLS_CONN=$(connection_string "$SCRAM_TLS_PORT" "require" " sslrootcert=${CERT_DIR}/test-ca.pem")
wait_for_select_one "TLS+SCRAM" "$SCRAM_TLS_CONN" "hunter2"
expect_select_one "psql over TLS+SCRAM" "$SCRAM_TLS_CONN" "hunter2"
