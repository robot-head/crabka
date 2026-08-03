#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
source scripts/gres-pg-regress.sh

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/input" "$tmp/build" "$tmp/bin"
touch "$tmp/input/parallel_schedule"

printf '%s\n' \
    '#!/usr/bin/env bash' \
    'for arg in "$@"; do printf "%s\n" "$arg"; done >"$FAKE_ARGS"' \
    'for arg in "$@"; do case "$arg" in --outputdir=*) out=${arg#*=};; esac; done' \
    'mkdir -p "$out/results"' \
    'printf "intentional mismatch\n" >"$out/regression.diffs"' \
    'exit "${FAKE_EXIT:-1}"' >"$tmp/bin/pg_regress"
chmod +x "$tmp/bin/pg_regress"

PG_REGRESS_BIN="$tmp/bin/pg_regress"
PG_BINDIR="$tmp/bin"
PG_CTL_BIN=true
REGRESS_SOURCE_DIR="$tmp/input"
REGRESS_BUILD_DIR="$tmp/build"
GRES_PORT=55432
export FAKE_ARGS="$tmp/args"

if PATH="$tmp/empty" require_commands bzip2 2>"$tmp/prerequisite-error"; then
    echo "missing prerequisite unexpectedly passed" >&2
    exit 1
fi
grep -F 'error: missing prerequisites: bzip2' "$tmp/prerequisite-error" >/dev/null

printf '%s\n' '#!/usr/bin/env bash' 'exec sleep 30' >"$tmp/bin/gres"
chmod +x "$tmp/bin/gres"
GRES_BIN="$tmp/bin/gres"
PSQL_BIN=true
GRES_PG_REGRESS_PORT=55433
mkdir "$tmp/fresh-gres"
start_gres "$tmp/fresh-gres"
if grep -F -- '--data-dir' "$tmp/fresh-gres/server-command.txt" >/dev/null; then
    echo "standalone Gres unexpectedly used durable state" >&2
    exit 1
fi
grep -F 'TOKIO_WORKER_THREADS=1' "$tmp/fresh-gres/server-command.txt" >/dev/null
grep -F -- '--pgexec-blocking-query-memory=20MiB' "$tmp/fresh-gres/server-command.txt" >/dev/null
stop_gres "$tmp/fresh-gres"

mkdir "$tmp/fresh-gres-parallel"
start_gres "$tmp/fresh-gres-parallel" parallel
if grep -F 'TOKIO_WORKER_THREADS=' "$tmp/fresh-gres-parallel/server-command.txt" >/dev/null; then
    echo "parallel Gres unexpectedly forced a Tokio worker count" >&2
    exit 1
fi
grep -F -- '--pgexec-blocking-query-memory=20MiB' "$tmp/fresh-gres-parallel/server-command.txt" >/dev/null
stop_gres "$tmp/fresh-gres-parallel"

if run_pg_regress gres serial "$tmp/gres"; then
    echo "expected an upstream mismatch to fail" >&2
    exit 1
fi
[[ "$(<"$tmp/gres/exit-status")" == 1 ]]
[[ -s "$tmp/gres/regression.diffs" ]]
grep -Fx -- '--use-existing' "$FAKE_ARGS" >/dev/null
grep -Fx -- '--dbname=crab' "$FAKE_ARGS" >/dev/null
grep -Fx -- '--max-connections=1' "$FAKE_ARGS" >/dev/null

FAKE_EXIT=0 run_pg_regress self-check parallel "$tmp/self-check"
grep -Fx -- '--no-locale' "$FAKE_ARGS" >/dev/null
grep -Fx -- '--encoding=UTF8' "$FAKE_ARGS" >/dev/null
grep -Fx -- "--temp-instance=$tmp/self-check/temp-instance" "$FAKE_ARGS" >/dev/null
if grep -Fx -- '--use-existing' "$FAKE_ARGS" >/dev/null; then
    echo "self-check unexpectedly used an existing server" >&2
    exit 1
fi

SOURCE_DIR="$tmp/source"
REGRESS_SOURCE_DIR="$SOURCE_DIR/src/test/regress"
mkdir -p "$REGRESS_SOURCE_DIR"/{data,expected,sql}
printf 'select 1;\n' >"$REGRESS_SOURCE_DIR/sql/smoke.sql"
printf '1\n' >"$REGRESS_SOURCE_DIR/expected/smoke.out"
touch "$REGRESS_SOURCE_DIR/parallel_schedule" "$REGRESS_SOURCE_DIR/resultmap"
(
    cd "$REGRESS_SOURCE_DIR"
    { find data expected sql -type f -print0; printf '%s\0' parallel_schedule resultmap; } |
        sort -z | xargs -0 sha256sum
) >"$SOURCE_DIR/.crabka-regress-inputs.sha256"
verify_regress_inputs
printf 'changed\n' >>"$REGRESS_SOURCE_DIR/expected/smoke.out"
if verify_regress_inputs 2>/dev/null; then
    echo "modified expected output passed integrity verification" >&2
    exit 1
fi

mkdir -p "$tmp/infrastructure/results"
printf "thread 'worker' panicked at engine.rs:1\n" >"$tmp/infrastructure/server.log"
printf 'psql: error: connection to server was lost\n' \
    >"$tmp/infrastructure/results/crash.out"
if detect_infrastructure_failures "$tmp/infrastructure" 1 0 124; then
    echo "infrastructure failures unexpectedly passed" >&2
    exit 1
fi
printf '%s\n' \
    'rust-panic: server.log' \
    'connection-loss: results/crash.out' \
    'pg-regress-exit: 124' \
    'postflight-failed' \
    'server-exited' >"$tmp/expected-infrastructure-failures"
cmp "$tmp/expected-infrastructure-failures" \
    "$tmp/infrastructure/infrastructure-failures.txt"

: >"$tmp/infrastructure/server.log"
: >"$tmp/infrastructure/results/crash.out"
detect_infrastructure_failures "$tmp/infrastructure" 0 1 1
[[ ! -s "$tmp/infrastructure/infrastructure-failures.txt" ]]
