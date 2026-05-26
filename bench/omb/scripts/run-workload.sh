#!/usr/bin/env bash
# Run one OMB workload against one stack.
# Usage:
#   run-workload.sh kafka  1-topic-16-partitions-1kb
#   run-workload.sh crabka 1-topic-16-partitions-1kb
#
# The coordinator runs on the first client VM (groups['client'][0]).
# Workload YAMLs are resolved from bench/omb/workloads/ first, then
# .omb/workloads/ (the upstream catalog).

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require terraform ansible jq ssh scp

stack="${1:-}"; workload="${2:-}"
[[ -n "$stack" && -n "$workload" ]] || die "usage: $0 {kafka|crabka} <workload-name-without-yaml>"

case "$stack" in kafka|crabka) ;; *) die "unknown stack: $stack" ;; esac

# Resolve the workload YAML locally.
candidates=(
  "$BENCH_OMB_DIR/workloads/${workload}.yaml"
  "$OMB_CHECKOUT/workloads/${workload}.yaml"
)
workload_file=""
for c in "${candidates[@]}"; do
  if [[ -f "$c" ]]; then workload_file="$c"; break; fi
done
[[ -n "$workload_file" ]] || die "no workload yaml found for '$workload' (looked in: ${candidates[*]})"

# Find the coordinator host (first client) + the per-stack driver yaml on that host.
tf_outs="$(terraform -chdir="$TF_DIR" output -json)"
coordinator_ip="$(jq -r '.clients.value[0].public_ip' <<<"$tf_outs")"
ssh_user="$(jq -r '.ssh_user.value' <<<"$tf_outs")"
[[ "$coordinator_ip" != "null" && -n "$coordinator_ip" ]] || die "no client VMs found in terraform output"

driver_path="/opt/benchmark/driver-kafka/kafka-${stack}.yaml"

# Build a tag like `kafka_2026-05-26T22-15-03Z_1-topic-16-partitions-1kb`.
ts="$(date -u +%Y-%m-%dT%H-%M-%SZ)"
run_id="${stack}_${ts}_${workload}"
remote_workload="/tmp/${workload}.yaml"
remote_results="/opt/benchmark/results/${run_id}"

log "Stack:       $stack"
log "Workload:    $workload  ($workload_file)"
log "Coordinator: $ssh_user@$coordinator_ip"
log "Run id:      $run_id"

ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

scp "${ssh_opts[@]}" "$workload_file" "${ssh_user}@${coordinator_ip}:${remote_workload}"

ssh "${ssh_opts[@]}" "${ssh_user}@${coordinator_ip}" "sudo mkdir -p ${remote_results} && sudo chown ${ssh_user}:${ssh_user} ${remote_results}"

ssh "${ssh_opts[@]}" "${ssh_user}@${coordinator_ip}" \
  "cd /opt/benchmark && sudo bin/benchmark \
     --drivers ${driver_path} \
     --workers \"\$(cat workers.yaml | grep -oP 'http://[^ ]+' | paste -sd,)\" \
     --output ${remote_results}/result.json \
     ${remote_workload} 2>&1 | tee ${remote_results}/run.log"

mkdir -p "$RESULTS_DIR/$stack"
local_dest="$RESULTS_DIR/$stack/$run_id"
log "Fetching results to $local_dest"
mkdir -p "$local_dest"
scp -r "${ssh_opts[@]}" "${ssh_user}@${coordinator_ip}:${remote_results}/." "$local_dest/"

log "Done. Results: $local_dest"
