#!/usr/bin/env bash
# Render a side-by-side Markdown table of crabka vs kafka results.
# Expects bench/omb/results/<stack>/<run_id>/result.json layouts from
# run-workload.sh.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require jq

[[ -d "$RESULTS_DIR" ]] || die "no results dir at $RESULTS_DIR — run scripts/run-workload.sh first"

out="$RESULTS_DIR/SUMMARY.md"
{
  printf '# OMB results — Crabka vs Kafka\n\n'
  printf 'Generated %s\n\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '| workload | stack | run | pub MB/s | pub p99 ms | end-to-end p99 ms |\n'
  printf '|---|---|---|---:|---:|---:|\n'

  shopt -s nullglob
  for stack_dir in "$RESULTS_DIR"/{kafka,crabka}; do
    stack="$(basename "$stack_dir")"
    for run_dir in "$stack_dir"/*; do
      run_id="$(basename "$run_dir")"
      result="$run_dir/result.json"
      [[ -f "$result" ]] || continue
      # OMB result.json shape: publishRate (msg/s), publishLatency99pct (ms),
      # endToEndLatency99pct (ms), messageSize (bytes).
      pub_rate="$(jq -r '.publishRate | add / length' "$result" 2>/dev/null || echo "n/a")"
      msg_sz="$(jq -r '.messageSize // 0' "$result" 2>/dev/null || echo 0)"
      mbs="n/a"
      if [[ "$pub_rate" != "n/a" && "$msg_sz" != "0" ]]; then
        mbs="$(awk -v r="$pub_rate" -v s="$msg_sz" 'BEGIN{printf "%.1f", r*s/1048576}')"
      fi
      pub_p99="$(jq -r '.publishLatency99pct // (.aggregatedPublishLatency99pct // "n/a")' "$result" 2>/dev/null || echo "n/a")"
      e2e_p99="$(jq -r '.endToEndLatency99pct // (.aggregatedEndToEndLatency99pct // "n/a")' "$result" 2>/dev/null || echo "n/a")"
      workload="$(echo "$run_id" | sed -E 's/^[^_]+_[^_]+_(.+)$/\1/')"
      printf '| %s | %s | %s | %s | %s | %s |\n' "$workload" "$stack" "$run_id" "$mbs" "$pub_p99" "$e2e_p99"
    done
  done
} >"$out"

log "Wrote $out"
