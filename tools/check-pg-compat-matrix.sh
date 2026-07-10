#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The Python checker invokes crabka-gres-parser-commands, which parses stable
# representative SQL probes through the exported pgparser API.
exec python3 "$repo_root/tools/check-pg-compat-matrix.py" "$@"
