#!/usr/bin/env bash
# Run PostgreSQL 18.4's unmodified core regression suite against PostgreSQL or Gres.
set -euo pipefail

readonly POSTGRES_TAG="REL_18_4"
readonly POSTGRES_VERSION="18.4"
readonly POSTGRES_URL="https://ftp.postgresql.org/pub/source/v${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.tar.bz2"
readonly POSTGRES_SHA256="81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_HELPER="${ROOT_DIR}/scripts/gres-pg-regress-baseline.py"
BASELINE_FILE="${ROOT_DIR}/crates/gres-conformance/pg-regress-baseline.json"
CACHE_DIR="${GRES_PG_REGRESS_CACHE_DIR:-${ROOT_DIR}/target/pg-regress-postgresql-${POSTGRES_VERSION}}"
ARCHIVE="${CACHE_DIR}/postgresql-${POSTGRES_VERSION}.tar.bz2"
SOURCE_DIR="${CACHE_DIR}/source"
BUILD_DIR="${CACHE_DIR}/build"
INSTALL_DIR="${CACHE_DIR}/install"
REGRESS_SOURCE_DIR="${SOURCE_DIR}/src/test/regress"
REGRESS_BUILD_DIR="${BUILD_DIR}/src/test/regress"
PG_REGRESS_BIN=""
PSQL_BIN=""
PG_CTL_BIN=""
PG_BINDIR=""
GRES_BIN="${GRES_PG_REGRESS_BIN:-${ROOT_DIR}/target/debug/crabka-gres}"
GRES_HOST="127.0.0.1"
GRES_PORT=""
GRES_USER="${GRES_PG_REGRESS_USER:-crab}"
GRES_DB="${GRES_PG_REGRESS_DB:-crab}"
GRES_PID=""
declare -a FOCUS_TESTS=()

export LC_ALL=C LANG=C LANGUAGE=C TZ=UTC
unset PGDATABASE PGHOST PGHOSTADDR PGOPTIONS PGPORT PGUSER PGSERVICE PGSERVICEFILE

usage() {
    cat <<'EOF'
Usage: scripts/gres-pg-regress.sh self-check|gres [serial|parallel|both]
                                  [--tests <name>...]

self-check  Run PostgreSQL 18.4 against a clean PostgreSQL 18.4 temp instance.
gres        Run both self-checks, then gate fresh local Gres instances. Serial
            mode enforces crates/gres-conformance/pg-regress-baseline.json.

Options:
  --tests <name>...  Run only the named regression tests instead of
                     parallel_schedule, using the identical server flags,
                     environment and pg_regress arguments as a full run. Must
                     be the last option; every remaining argument is a test
                     name. test_setup is prepended automatically when absent,
                     because most tests depend on the objects it creates. The
                     baseline gate is skipped, since it grades the whole
                     schedule. Example:
                       scripts/gres-pg-regress.sh gres serial --tests boolean

                     Not every test can be run this way. Many depend on objects
                     built by earlier tests in the schedule, not just those from
                     test_setup, and PostgreSQL itself fails those in isolation
                     (alter_table is one). The self-check gate catches that and
                     reports "PostgreSQL self-check failed" -- which means the
                     subset is not self-contained, not that Gres is at fault.
                     Read self-check-serial/regression.out to see which test,
                     and certify against the full schedule instead.

Environment:
  GRES_PG_REGRESS_ARTIFACT_DIR  New directory for retained run artifacts.
  GRES_PG_REGRESS_CACHE_DIR     PostgreSQL archive/build cache under target/ by default.
  GRES_PG_REGRESS_BIN           Existing crabka-gres binary; otherwise cargo builds it.
  GRES_PG_REGRESS_PORT          Gres listen port; otherwise an unused port is selected.
  GRES_PG_REGRESS_TIMEOUT       Per-schedule timeout (default: 3600s).
  GRES_PG_REGRESS_LOCK          Machine-wide run lock (default: /tmp/gres-pg-regress.lock).
  GRES_PG_REGRESS_WAIT          Queue behind a running certification instead of failing.
  GRES_PG_REGRESS_NO_LOCK       Skip the lock; timings become unreliable.
  GRES_PG_REGRESS_PROCESS_TOKEN Deterministic backend-id process token (default: 1).
  GRES_PG_REGRESS_RANDOM_SEED   Deterministic initial random seed (default: 1).
  GRES_PG_REGRESS_TOKIO_WORKERS Override Tokio workers (serial defaults to 1;
                                parallel uses the runtime default).
  GRES_PG_REGRESS_BLOCKING_QUERY_MEMORY
                                Gres blocking-query memory budget (default: 20MiB).
EOF
}

require_commands() {
    local missing=() command
    for command in "$@"; do
        command -v "$command" >/dev/null || missing+=("$command")
    done
    if ((${#missing[@]})); then
        printf 'error: missing prerequisites: %s\n' "${missing[*]}" >&2
        return 1
    fi
}

fetch_source() {
    mkdir -p "$CACHE_DIR"
    if [[ ! -f "$ARCHIVE" ]] ||
        ! printf '%s  %s\n' "$POSTGRES_SHA256" "$ARCHIVE" | sha256sum --check --status; then
        local download="${ARCHIVE}.download.$$"
        curl --fail --location --retry 3 --output "$download" "$POSTGRES_URL"
        printf '%s  %s\n' "$POSTGRES_SHA256" "$download" | sha256sum --check --status
        mv "$download" "$ARCHIVE"
    fi

    if [[ -f "${SOURCE_DIR}/.crabka-archive-sha256" ]] &&
        [[ "$(<"${SOURCE_DIR}/.crabka-archive-sha256")" == "$POSTGRES_SHA256" ]]; then
        verify_regress_inputs
        return
    fi
    if [[ -e "$SOURCE_DIR" ]]; then
        echo "error: unverified source cache exists: ${SOURCE_DIR}" >&2
        return 1
    fi

    local extracted="${CACHE_DIR}/source.extract.$$"
    mkdir "$extracted"
    tar --extract --bzip2 --file "$ARCHIVE" --directory "$extracted" --strip-components=1
    printf '%s\n' "$POSTGRES_SHA256" >"${extracted}/.crabka-archive-sha256"
    (
        cd "${extracted}/src/test/regress"
        { find data expected sql -type f -print0; printf '%s\0' parallel_schedule resultmap; } |
            sort -z | xargs -0 sha256sum
    ) >"${extracted}/.crabka-regress-inputs.sha256"
    chmod -R a-w "$extracted"
    mv "$extracted" "$SOURCE_DIR"
    verify_regress_inputs
}

verify_regress_inputs() {
    local manifest="${SOURCE_DIR}/.crabka-regress-inputs.sha256"
    [[ -f "$manifest" ]] || {
        echo "error: regression input manifest is missing: ${manifest}" >&2
        return 1
    }
    local expected_count actual_count
    expected_count="$(wc -l <"$manifest")"
    actual_count="$({
        cd "$REGRESS_SOURCE_DIR"
        { find data expected sql -type f -print0; printf '%s\0' parallel_schedule resultmap; } |
            tr '\0' '\n' | wc -l
    })"
    [[ "$actual_count" == "$expected_count" ]] &&
        (cd "$REGRESS_SOURCE_DIR" && sha256sum --check --status "$manifest") || {
        echo "error: pinned PostgreSQL regression inputs were modified" >&2
        return 1
    }
}

prepare_postgres() {
    local log="$1"
    require_commands bison bzip2 curl flex gcc getconf make perl sha256sum tar timeout
    fetch_source
    mkdir -p "$BUILD_DIR" "$INSTALL_DIR"
    if [[ ! -f "${BUILD_DIR}/GNUmakefile" ]]; then
        (
            cd "$BUILD_DIR"
            "$SOURCE_DIR/configure" --prefix="$INSTALL_DIR" \
                --without-icu --without-readline --without-zlib
        ) >"$log" 2>&1
    fi
    local jobs="${GRES_PG_REGRESS_JOBS:-$(getconf _NPROCESSORS_ONLN)}"
    make -C "$BUILD_DIR" -j"$jobs" \
        world-bin >>"$log" 2>&1
    make -C "$BUILD_DIR" install-world-bin >>"$log" 2>&1
    make -C "$REGRESS_BUILD_DIR" -j"$jobs" pg_regress regress.so >>"$log" 2>&1
    PG_REGRESS_BIN="${REGRESS_BUILD_DIR}/pg_regress"
    PSQL_BIN="${INSTALL_DIR}/bin/psql"
    PG_CTL_BIN="${INSTALL_DIR}/bin/pg_ctl"
    PG_BINDIR="${INSTALL_DIR}/bin"
    [[ "$($PG_REGRESS_BIN --version)" == *" ${POSTGRES_VERSION}" ]]
    [[ "$($PSQL_BIN --version)" == *" ${POSTGRES_VERSION}" ]]
    [[ "$(${PG_BINDIR}/postgres --version)" == *" ${POSTGRES_VERSION}" ]]
    [[ "$(${PG_BINDIR}/initdb --version)" == *" ${POSTGRES_VERSION}" ]]
    [[ -f "${REGRESS_BUILD_DIR}/regress.so" ]]
}

run_pg_regress() {
    local subject="$1" mode="$2" output="$3" rc=0
    local -a command=(
        "$PG_REGRESS_BIN"
        "--bindir=${PG_BINDIR}"
        "--dlpath=${REGRESS_BUILD_DIR}"
        "--inputdir=${REGRESS_SOURCE_DIR}"
        "--expecteddir=${REGRESS_SOURCE_DIR}"
        "--outputdir=${output}"
    )
    if ((${#FOCUS_TESTS[@]} == 0)); then
        command+=("--schedule=${REGRESS_SOURCE_DIR}/parallel_schedule")
    fi
    command+=("--max-concurrent-tests=20")
    mkdir -p "$output"

    case "$subject" in
        self-check)
            command+=("--temp-instance=${output}/temp-instance" --no-locale \
                --encoding=UTF8 --dbname=regression)
            ;;
        gres)
            command+=(--use-existing "--dbname=${GRES_DB}" "--host=${GRES_HOST}" \
                "--port=${GRES_PORT}" "--user=${GRES_USER}")
            ;;
        *)
            echo "error: unknown subject: ${subject}" >&2
            return 2
            ;;
    esac
    case "$mode" in
        serial) command+=(--max-connections=1) ;;
        parallel) ;;
        *)
            echo "error: unknown schedule mode: ${mode}" >&2
            return 2
            ;;
    esac
    if ((${#FOCUS_TESTS[@]})); then
        command+=("${FOCUS_TESTS[@]}")
    fi

    printf '%q ' "${command[@]}" >"${output}/command.txt"
    printf '\n' >>"${output}/command.txt"
    if timeout --signal=TERM --kill-after=30s \
        "${GRES_PG_REGRESS_TIMEOUT:-3600s}" "${command[@]}" \
        >"${output}/command.log" 2>&1; then
        rc=0
    else
        rc=$?
    fi
    printf '%s\n' "$rc" >"${output}/exit-status"

    if [[ "$subject" == self-check && -f "${output}/temp-instance/postmaster.pid" ]]; then
        "$PG_CTL_BIN" -D "${output}/temp-instance" -m immediate stop \
            >>"${output}/cleanup.log" 2>&1 || true
    fi
    return "$rc"
}

choose_port() {
    python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

probe_gres() {
    local log="$1"
    PGCONNECT_TIMEOUT=5 "$PSQL_BIN" --no-psqlrc --host="$GRES_HOST" \
        --port="$GRES_PORT" --username="$GRES_USER" --dbname="$GRES_DB" \
        --set=ON_ERROR_STOP=1 --tuples-only --no-align --command='SELECT 1' >>"$log" 2>&1
}

start_gres() {
    local output="$1" mode="${2:-serial}"
    local -a command
    GRES_PORT="${GRES_PG_REGRESS_PORT:-$(choose_port)}"
    command=(
        env
        "CRABKA_BACKEND_PROCESS_TOKEN=${GRES_PG_REGRESS_PROCESS_TOKEN:-1}"
        "CRABKA_RANDOM_SEED=${GRES_PG_REGRESS_RANDOM_SEED:-1}"
        "CRABKA_PG_REGRESS_LIBRARY=${REGRESS_BUILD_DIR}/regress.so"
    )
    if [[ -n "${GRES_PG_REGRESS_TOKIO_WORKERS:-}" ]]; then
        command+=("TOKIO_WORKER_THREADS=${GRES_PG_REGRESS_TOKIO_WORKERS}")
    elif [[ "$mode" == serial ]]; then
        command+=("TOKIO_WORKER_THREADS=1")
    fi
    command+=("$GRES_BIN" --listen "${GRES_HOST}:${GRES_PORT}" \
        "--pgexec-blocking-query-memory=${GRES_PG_REGRESS_BLOCKING_QUERY_MEMORY:-20MiB}")
    printf '%q ' "${command[@]}" >"${output}/server-command.txt"
    printf '\n' >>"${output}/server-command.txt"
    "${command[@]}" >"${output}/server.log" 2>&1 &
    GRES_PID=$!

    local attempt
    for ((attempt = 0; attempt < 100; attempt++)); do
        if ! kill -0 "$GRES_PID" 2>/dev/null; then
            local rc=0
            wait "$GRES_PID" || rc=$?
            printf '%s\n' "$rc" >"${output}/server-exit-status"
            GRES_PID=""
            echo "error: Gres exited during startup; see ${output}/server.log" >&2
            return 1
        fi
        if probe_gres "${output}/preflight.log"; then
            return
        fi
        sleep 0.2
    done
    echo "error: Gres readiness timed out; see ${output}/server.log" >&2
    return 1
}

stop_gres() {
    local output="${1:-}"
    [[ -n "$GRES_PID" ]] || return 0
    local rc=0
    kill "$GRES_PID" 2>/dev/null || true
    wait "$GRES_PID" || rc=$?
    [[ -z "$output" ]] || printf '%s\n' "$rc" >"${output}/server-exit-status"
    GRES_PID=""
}

detect_infrastructure_failures() {
    local output="$1" postflight_rc="$2" server_alive="$3" regress_rc="$4"
    local failures="${output}/infrastructure-failures.txt" match
    : >"$failures"

    if grep -Eiq "thread '.*' panicked|panicked at" "${output}/server.log" 2>/dev/null; then
        printf 'rust-panic: server.log\n' >>"$failures"
    fi
    while IFS= read -r match; do
        [[ -n "$match" ]] && printf 'connection-loss: results/%s\n' "${match##*/}" >>"$failures"
    done < <(grep -ERil --include='*.out' \
        'server closed the connection|connection (to server )?(was )?(lost|closed|reset)|connection reset by peer|could not (receive|send) data (from|to) server' \
        "${output}/results" 2>/dev/null || true)
    ((regress_rc <= 1)) || printf 'pg-regress-exit: %s\n' "$regress_rc" >>"$failures"
    ((postflight_rc == 0)) || printf 'postflight-failed\n' >>"$failures"
    ((server_alive == 1)) || printf 'server-exited\n' >>"$failures"
    [[ ! -s "$failures" ]]
}

check_serial_baseline() {
    local output="$1" rc=0
    python3 "$BASELINE_HELPER" check \
        --postgres-tag "$POSTGRES_TAG" \
        --schedule "${REGRESS_SOURCE_DIR}/parallel_schedule" \
        --tap "${output}/command.log" \
        --diff "${output}/regression.diffs" \
        --source-root "$SOURCE_DIR" \
        --build-root "$output" \
        --baseline "$BASELINE_FILE" \
        --actual-output "${output}/actual-baseline.json" \
        --summary-output "${output}/summary.md" || rc=$?
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" && -f "${output}/summary.md" ]]; then
        cat "${output}/summary.md" >>"$GITHUB_STEP_SUMMARY"
    fi
    return "$rc"
}

run_gres_mode() {
    local mode="$1" output="$2" regress_rc=0 postflight_rc=0 server_alive=0 infrastructure_rc=0 baseline_rc=0
    mkdir -p "$output"
    start_gres "$output" "$mode" || {
        stop_gres "$output"
        printf 'server-start-failed\n' >"${output}/infrastructure-failures.txt"
        return 1
    }
    run_pg_regress gres "$mode" "$output" || regress_rc=$?
    probe_gres "${output}/postflight.log" || postflight_rc=$?
    kill -0 "$GRES_PID" 2>/dev/null && server_alive=1
    stop_gres "$output"
    detect_infrastructure_failures "$output" "$postflight_rc" "$server_alive" "$regress_rc" || infrastructure_rc=$?
    if [[ "$mode" == serial ]] && ((${#FOCUS_TESTS[@]} == 0)) &&
        [[ "$regress_rc" -le 1 && "$infrastructure_rc" -eq 0 ]]; then
        check_serial_baseline "$output" || baseline_rc=$?
        ((baseline_rc == 0))
    else
        ((regress_rc == 0 && infrastructure_rc == 0))
    fi
}

run_modes() {
    local subject="$1" modes="$2" artifact_root="$3" failed=0 mode
    local -a selected
    if [[ "$modes" == both ]]; then
        selected=(serial parallel)
    else
        selected=("$modes")
    fi
    for mode in "${selected[@]}"; do
        echo "==> ${subject} ${mode}"
        if [[ "$subject" == gres ]]; then
            run_gres_mode "$mode" "${artifact_root}/${subject}-${mode}" || failed=1
        else
            run_pg_regress "$subject" "$mode" "${artifact_root}/${subject}-${mode}" || failed=1
        fi
    done
    return "$failed"
}

parse_focus_tests() {
    if (($# == 0)); then
        echo "error: --tests requires at least one test name" >&2
        return 2
    fi
    local name
    for name in "$@"; do
        if [[ ! "$name" =~ ^[A-Za-z0-9_-]+$ ]]; then
            echo "error: invalid test name: ${name}" >&2
            return 2
        fi
        if [[ -d "${REGRESS_SOURCE_DIR}/sql" && ! -f "${REGRESS_SOURCE_DIR}/sql/${name}.sql" ]]; then
            echo "error: no such regression test: ${name}" >&2
            return 2
        fi
    done
    FOCUS_TESTS=("$@")
    if [[ " $* " != *" test_setup "* ]]; then
        FOCUS_TESTS=(test_setup "$@")
    fi
}

main() {
    local subject="" modes="both" positional=0
    while (($#)); do
        case "$1" in
            -h | --help)
                usage
                return
                ;;
            --tests)
                shift
                parse_focus_tests "$@" || return $?
                break
                ;;
            -*)
                echo "error: unknown option: $1" >&2
                usage >&2
                return 2
                ;;
            *)
                case "$positional" in
                    0) subject="$1" ;;
                    1) modes="$1" ;;
                    *)
                        echo "error: unexpected argument: $1" >&2
                        usage >&2
                        return 2
                        ;;
                esac
                positional=$((positional + 1))
                shift
                ;;
        esac
    done
    if [[ "$subject" != self-check && "$subject" != gres ]]; then
        usage >&2
        return 2
    fi
    if [[ "$modes" != serial && "$modes" != parallel && "$modes" != both ]]; then
        usage >&2
        return 2
    fi

    local artifact_root="${GRES_PG_REGRESS_ARTIFACT_DIR:-${ROOT_DIR}/target/pg-regress-runs/$(date -u +%Y%m%dT%H%M%SZ)-$$-${subject}}"
    if [[ -e "$artifact_root" ]]; then
        echo "error: artifact directory already exists: ${artifact_root}" >&2
        return 1
    fi
    mkdir -p "$artifact_root"
    printf 'postgres_tag=%s\nsource_url=%s\nsha256=%s\n' \
        "$POSTGRES_TAG" "$POSTGRES_URL" "$POSTGRES_SHA256" >"${artifact_root}/provenance.txt"
    if ((${#FOCUS_TESTS[@]})); then
        printf 'tests=%s\n' "${FOCUS_TESTS[*]}" >>"${artifact_root}/provenance.txt"
        echo "==> focused subset: ${FOCUS_TESTS[*]}"
    fi
    trap 'stop_gres' EXIT INT TERM
    prepare_postgres "${artifact_root}/postgres-build.log"

    if [[ "$subject" == self-check ]]; then
        run_modes self-check "$modes" "$artifact_root"
    else
        if ! run_modes self-check both "$artifact_root"; then
            echo "error: PostgreSQL self-check failed; Gres was not tested" >&2
            return 1
        fi
        require_commands python3
        if [[ -z "${GRES_PG_REGRESS_BIN:-}" ]]; then
            require_commands cargo
            cargo build --locked -p crabka-gres --bin crabka-gres \
                >"${artifact_root}/gres-build.log" 2>&1
        fi
        [[ -x "$GRES_BIN" ]] || {
            echo "error: Gres binary is not executable: ${GRES_BIN}" >&2
            return 1
        }
        run_modes gres "$modes" "$artifact_root"
    fi
    echo "artifacts: ${artifact_root}"
}

# Serialise runs across the whole machine.
#
# Two concurrent runs do not corrupt each other's results -- each has its own
# port, data directory and artifact tree -- but they ruin each other's timings,
# and timings are part of what a run reports. Two overlapping runs drove the
# load average to 21 and took one lateral join from 7s to over 120s, which
# looks exactly like a performance regression and is not one.
#
# The lock is deliberately NOT under the worktree's target/: a run is usually
# certified from an isolated `git archive` copy with its own target/, so a
# per-tree lock would not see the run it needs to wait for. Contention is for
# the machine's CPUs, so the mutex has to be machine-wide too.
#
# Checking `pgrep` instead was tried and does not work: it matches the checking
# shell's own command line, and it matches stale `tail -f` monitors, so it
# reports a busy machine that is idle and an idle one that is busy.
gres_pg_regress_lock() {
    local lock="${GRES_PG_REGRESS_LOCK:-/tmp/gres-pg-regress.lock}"
    if [[ "${GRES_PG_REGRESS_NO_LOCK:-0}" == 1 ]]; then
        return 0
    fi
    if ! command -v flock >/dev/null 2>&1; then
        echo "warning: flock not found; running without the machine-wide lock" >&2
        return 0
    fi
    # Open read-write rather than `>`: `>` truncates on open, so a second run
    # would erase the holder's PID before discovering it could not have the
    # lock, and then report "PID unknown" about a process it just blanked.
    if ! exec 9<>"$lock"; then
        echo "warning: cannot open ${lock}; running without the machine-wide lock" >&2
        return 0
    fi
    if flock -n 9; then
        printf '%s\n' "$$" >"$lock"
        return 0
    fi
    local holder
    holder="$(cat "$lock" 2>/dev/null || true)"
    if [[ "${GRES_PG_REGRESS_WAIT:-0}" == 1 ]]; then
        echo "waiting for the run held by PID ${holder:-unknown} to finish..." >&2
        flock 9
        printf '%s\n' "$$" >"$lock"
        return 0
    fi
    echo "error: another pg_regress run holds ${lock} (PID ${holder:-unknown})." >&2
    echo "       Concurrent runs distort each other's timings." >&2
    echo "       Set GRES_PG_REGRESS_WAIT=1 to queue behind it," >&2
    echo "       or GRES_PG_REGRESS_NO_LOCK=1 if you do not care about timings." >&2
    return 1
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    gres_pg_regress_lock || exit 1
    main "$@"
fi
