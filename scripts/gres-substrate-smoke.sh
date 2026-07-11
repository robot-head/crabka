#!/usr/bin/env bash
# Substrate-mode smoke: boot a local Crabka broker, run crabka-gres against a
# tenant WAL topic, kill the compute, and prove a fresh disposable compute
# replays the acked SQL state without any cache directory.
set -euo pipefail

cd "$(dirname "$0")/.."

if [ "${1:-}" = "--help" ]; then
    cat <<'EOF'
Usage: scripts/gres-substrate-smoke.sh [broker-port [controller-port [gres-port]]]

Boots a local standalone Crabka broker, starts crabka-gres with
--substrate-bootstrap and --tenant, writes SQL state, restarts the disposable
compute without a cache directory, and verifies replay from the tenant WAL.

Set CRABKA_GRES_SKIP_BUILD=1 to reuse existing target/debug binaries.
EOF
    exit 0
fi

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed; substrate smoke requires a real pgwire client"
    cargo run -p crabka-gres -- --help >/dev/null
    exit 0
fi

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

mapfile -t PORTS < <(choose_ports)
BROKER_PORT="${1:-${PORTS[0]}}"
CONTROLLER_PORT="${2:-${PORTS[1]}}"
GRES_PORT="${3:-${PORTS[2]}}"
TENANT="substrate-smoke-$GRES_PORT"
CLUSTER_ID="00000000-0000-0000-0000-000000000001"
DATA_ROOT="$(mktemp -d)"
BROKER_PID=""
GRES_PID=""
CLIENT_PID=""

cleanup() {
    kill "${CLIENT_PID:-}" "${GRES_PID:-}" "${BROKER_PID:-}" 2>/dev/null || true
    wait "${CLIENT_PID:-}" "${GRES_PID:-}" "${BROKER_PID:-}" 2>/dev/null || true
    rm -rf "$DATA_ROOT"
}
trap cleanup EXIT

CONNINFO="host=127.0.0.1 port=${GRES_PORT} user=crab password=crab dbname=crab sslmode=prefer"

wait_for_tcp_port() {
    local label="$1"
    local port="$2"

    for _ in $(seq 80); do
        if python3 - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=0.2):
    pass
PY
        then
            return 0
        fi
        sleep 0.25
    done

    echo "FAIL: ${label} did not open port ${port}" >&2
    return 1
}

wait_for_sql() {
    local label="$1"

    for _ in $(seq 80); do
        if psql "$CONNINFO" -tAc 'SELECT 1' >/dev/null 2>&1; then
            return 0
        fi
        if [ -n "${GRES_PID:-}" ] && ! kill -0 "$GRES_PID" 2>/dev/null; then
            echo "FAIL: ${label} exited before serving SQL" >&2
            cat "${DATA_ROOT}/${label}.log" >&2 || true
            return 1
        fi
        sleep 0.25
    done

    echo "FAIL: ${label} did not become SQL-ready" >&2
    cat "${DATA_ROOT}/${label}.log" >&2 || true
    return 1
}

stop_gres() {
    if [ -z "${GRES_PID:-}" ]; then
        return 0
    fi

    kill "$GRES_PID"
    wait "$GRES_PID" 2>/dev/null || true
    GRES_PID=""
}

kill_gres_abruptly() {
    kill -KILL "$GRES_PID"
    wait "$GRES_PID" 2>/dev/null || true
    GRES_PID=""
}

start_gres() {
    local label="$1"
    local deadline=$((SECONDS + 30))
    while [ "$SECONDS" -lt "$deadline" ]; do
        ./target/debug/crabka-gres \
            --listen "127.0.0.1:${GRES_PORT}" \
            --substrate-bootstrap "127.0.0.1:${BROKER_PORT}" \
            --tenant "$TENANT" \
            >"${DATA_ROOT}/${label}.log" 2>&1 &
        GRES_PID=$!
        if wait_for_sql "$label"; then
            return 0
        fi
        wait "$GRES_PID" 2>/dev/null || true
        GRES_PID=""
    done
    echo "FAIL: ${label} did not become ready before restart deadline" >&2
    return 1
}

if [ "${CRABKA_GRES_SKIP_BUILD:-}" != "1" ]; then
    cargo build --locked -p crabka-cli -p crabka-broker -p crabka-gres
fi

./target/debug/crabka format \
    --log-dir "${DATA_ROOT}/broker" \
    --cluster-id "$CLUSTER_ID" \
    --standalone \
    --node-id 1 \
    --controller-listener "127.0.0.1:${CONTROLLER_PORT}"

./target/debug/crabka-broker \
    --log-dir "${DATA_ROOT}/broker" \
    --cluster-id "$CLUSTER_ID" \
    --broker-id 1 \
    --listen-addr "127.0.0.1:${BROKER_PORT}" \
    >"${DATA_ROOT}/broker.log" 2>&1 &
BROKER_PID=$!
wait_for_tcp_port broker "$BROKER_PORT"

printf '%s\n' crab | ./target/debug/crabka gres create-tenant \
    --bootstrap "127.0.0.1:${BROKER_PORT}" \
    --name "$TENANT" \
    --user crab \
    --password-stdin

start_gres first-compute
psql "$CONNINFO" -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE t (id int4, name text); INSERT INTO t VALUES (1, 'substrate'); CREATE TABLE payloads (id int4, body text); INSERT INTO payloads VALUES (1, repeat('x', 1400000));"

# Keep a real SQL transaction open and kill the compute after the INSERT has
# executed but before COMMIT. The marker is a protocol response, not a settle
# delay, so the kill point is deterministic.
coproc INFLIGHT_CLIENT { stdbuf -oL psql "$CONNINFO" -Atq; }
CLIENT_PID=$INFLIGHT_CLIENT_PID
printf '%s\n' "BEGIN;" "INSERT INTO t VALUES (99, 'unacked');" "SELECT 'inflight-ready';" >&"${INFLIGHT_CLIENT[1]}"
deadline=$((SECONDS + 10))
inflight_ready=""
while [ "$SECONDS" -lt "$deadline" ]; do
    if IFS= read -r -t 1 line <&"${INFLIGHT_CLIENT[0]}"; then
        if [ "$line" = "inflight-ready" ]; then
            inflight_ready=1
            break
        fi
    fi
done
if [ -z "$inflight_ready" ]; then
    echo "FAIL: in-flight SQL transaction did not reach deterministic kill point" >&2
    exit 1
fi
kill_gres_abruptly
kill "$CLIENT_PID" 2>/dev/null || true
wait "$CLIENT_PID" 2>/dev/null || true
CLIENT_PID=""

start_gres successor-compute
out=$(psql "$CONNINFO" -tAc "SELECT name FROM t WHERE id = 1")
unacked=$(psql "$CONNINFO" -tAc "SELECT count(*) FROM t WHERE id = 99")
payload_len=$(psql "$CONNINFO" -tAc "SELECT length(body) FROM payloads WHERE id = 1")
psql "$CONNINFO" -v ON_ERROR_STOP=1 -c "INSERT INTO t VALUES (2, 'successor');" >/dev/null
successor=$(psql "$CONNINFO" -tAc "SELECT count(*) FROM t WHERE id = 2")
if [ "$out" = "substrate" ] && [ "$unacked" = "0" ] && [ "$payload_len" = "1400000" ] && [ "$successor" = "1" ]; then
    echo "PASS: abrupt-loss replay preserved acked+oversized state, rejected unacked state, and accepted successor writes"
    exit 0
fi

echo "FAIL: replay result name=${out} unacked=${unacked} payload_len=${payload_len} successor=${successor}" >&2
exit 1
