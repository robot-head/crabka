#!/usr/bin/env bash
# G-8 sharded conformance gate: deterministic metadata/global-visibility/scanner tests.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/gres-sharded-conformance.sh [--help]

Runs the deterministic G-8 sharded conformance tests that do not require a
running broker, PostgreSQL oracle, or external services. The gate fails loudly on
any regression and writes a machine-readable report under the artifact directory.

Environment:
  CRABKA_GRES_SHARDED_CONFORMANCE_ARTIFACT_DIR=dir
      Artifact directory (default: target/gres-sharded-conformance-artifacts).
  CRABKA_GRES_SHARDED_CONFORMANCE_EXTRA_ARGS=args
      Extra arguments appended after `--` for each cargo test invocation.
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

if [ "$status" -eq 0 ]; then
    write_report passed
    log "PASS: wrote ${ARTIFACT_DIR}/sharded-conformance.json"
    exit 0
fi

write_report failed
dump_diagnostics
echo "FAIL: one or more G-8 sharded conformance gates failed" >&2
exit 1
