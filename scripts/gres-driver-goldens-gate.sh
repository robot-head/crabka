#!/usr/bin/env bash
# Replay checked, capture-backed pinned-driver startup/SET behavior against Gres.
set -euo pipefail

cd "$(dirname "$0")/.."

port="${CRABKA_GRES_DRIVER_GOLDEN_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)}"
data_root="$(mktemp -d)"
server_pid=""

cleanup() {
    kill "${server_pid:-}" 2>/dev/null || true
    wait "${server_pid:-}" 2>/dev/null || true
    rm -rf "$data_root"
}
trap cleanup EXIT

if [ "${CRABKA_GRES_SKIP_BUILD:-}" != "1" ]; then
    cargo build --locked -p crabka-gres -p crabka-gres-conformance \
        --bin crabka-gres-driver-golden-replay
fi

./target/debug/crabka-gres --listen "127.0.0.1:${port}" \
    --data-dir "${data_root}/gres" >"${data_root}/gres.log" 2>&1 &
server_pid=$!

for _ in $(seq 40); do
    if (echo >/dev/tcp/127.0.0.1/"$port") >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done

./target/debug/crabka-gres-driver-golden-replay --port "$port"
