# Sourced by every bench/omb script. Sets BENCH_OMB_* paths.
# shellcheck shell=bash disable=SC2034

set -euo pipefail

BENCH_OMB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$BENCH_OMB_DIR/../.." && pwd)"
OMB_CHECKOUT="$REPO_ROOT/.omb"
TF_DIR="$BENCH_OMB_DIR/terraform/gcp"
ANSIBLE_DIR="$BENCH_OMB_DIR/ansible"
RESULTS_DIR="$BENCH_OMB_DIR/results"
PINNED_OMB_COMMIT="$(cat "$BENCH_OMB_DIR/.pinned-omb-commit" | tr -d '[:space:]')"

log()  { printf '[bench-omb] %s\n' "$*" >&2; }
die()  { log "ERROR: $*"; exit 1; }

require() {
  local cmd
  for cmd in "$@"; do
    command -v "$cmd" >/dev/null 2>&1 || die "missing required tool: $cmd"
  done
}
