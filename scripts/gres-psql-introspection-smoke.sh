#!/usr/bin/env bash
# F-2: pin what a real psql client's `\d` family gets out of gres.
#
# Each backslash command is run against a freshly seeded server and its output
# is compared to a checked-in transcript, so a catalog regression that changes
# the SHAPE of `\dt` (a lost column, a lost row, a 42P01) fails CI even though
# every SQL-level test still passes. `\d <table>` and `\dp` are deliberately
# absent: they need parser support this wave does not own (`OPERATOR(...)`
# qualified operators and `ARRAY(subquery)` respectively). Add them here when
# that lands.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v psql >/dev/null; then
    echo "SKIP: psql not installed"
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

cleanup() {
    kill "${SERVER_PID:-}" 2>/dev/null || true
    wait "${SERVER_PID:-}" 2>/dev/null || true
    rm -rf "$DATA_ROOT"
}
trap cleanup EXIT

CONN="host=127.0.0.1 port=${PORT} user=crab dbname=crab sslmode=disable"

# Set CRABKA_GRES_SKIP_BUILD=1 to reuse an existing target/debug binary.
if [ "${CRABKA_GRES_SKIP_BUILD:-}" != "1" ]; then
    cargo build --locked -p crabka-gres
fi

./target/debug/crabka-gres --listen "127.0.0.1:${PORT}" \
    --data-dir "${DATA_ROOT}/data" \
    >"${DATA_ROOT}/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 40); do
    if psql "$CONN" -tAc 'SELECT 1' >/dev/null 2>&1; then
        break
    fi
    sleep 0.3
done

if ! psql "$CONN" -tAc 'SELECT 1' >/dev/null 2>&1; then
    echo "FAIL: server not ready" >&2
    exit 1
fi

psql "$CONN" -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE TABLE smoke_t (id int4 PRIMARY KEY, code text NOT NULL, price numeric(9,2));
CREATE INDEX smoke_t_code_idx ON smoke_t (code);
CREATE VIEW smoke_v AS SELECT id, code FROM smoke_t;
CREATE SEQUENCE smoke_s;
SQL

GOLDEN="scripts/fixtures/gres-psql-introspection.txt"
ACTUAL="${DATA_ROOT}/actual.txt"

: >"$ACTUAL"
for command in '\dt' '\di' '\dv' '\ds' '\d' '\dn' '\du' '\l' '\df'; do
    printf '===== %s\n' "$command" >>"$ACTUAL"
    if ! psql "$CONN" -c "$command" >>"$ACTUAL" 2>&1; then
        echo "FAIL: psql ${command} exited non-zero" >&2
        exit 1
    fi
done

if ! diff -u "$GOLDEN" "$ACTUAL"; then
    echo "FAIL: psql introspection transcript changed" >&2
    echo "If the change is intended, refresh ${GOLDEN} from the diff above." >&2
    exit 1
fi

echo "PASS: psql introspection transcript matches ${GOLDEN}"
