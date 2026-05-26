#!/usr/bin/env bash
# Pull all results from /opt/benchmark/results/ on every client VM
# into bench/omb/results/<stack>/. Useful when you've left runs going
# detached or want to recover after a network blip.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require terraform jq scp ssh

tf_outs="$(terraform -chdir="$TF_DIR" output -json)"
ssh_user="$(jq -r '.ssh_user.value' <<<"$tf_outs")"

mkdir -p "$RESULTS_DIR"
ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

while read -r ip; do
  log "Pulling results from $ip"
  rsync_dir="$RESULTS_DIR/_raw/${ip}"
  mkdir -p "$rsync_dir"
  rsync -az -e "ssh ${ssh_opts[*]}" "${ssh_user}@${ip}:/opt/benchmark/results/" "$rsync_dir/" || true
done < <(jq -r '.clients.value[].public_ip' <<<"$tf_outs")

log "Raw results under $RESULTS_DIR/_raw/. The per-stack copies created by run-workload.sh are unaffected."
