#!/usr/bin/env bash
# Tear down everything.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require terraform

terraform -chdir="$TF_DIR" destroy -auto-approve "$@"
