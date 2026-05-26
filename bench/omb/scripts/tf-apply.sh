#!/usr/bin/env bash
# Provision the GCP infra for the OMB harness.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require terraform

if [[ ! -f "$TF_DIR/terraform.tfvars" ]]; then
  die "create $TF_DIR/terraform.tfvars first (cp terraform.tfvars.example terraform.tfvars and edit)"
fi

terraform -chdir="$TF_DIR" init -input=false -upgrade
terraform -chdir="$TF_DIR" apply -auto-approve "$@"

log "Done. Outputs:"
terraform -chdir="$TF_DIR" output
