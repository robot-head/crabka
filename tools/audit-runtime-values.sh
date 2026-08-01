#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

root=$(git rev-parse --show-toplevel)
cd "$root"

{
  rg -n \
    --glob '*.rs' \
    --glob '!**/tests/**' \
    --glob '!**/benches/**' \
    --glob '!**/*_model.rs' \
    --glob '!**/test_*.rs' \
    '(^[[:space:]]*(pub([[:space:]]*\([^)]*\))?[[:space:]]+)?const[[:space:]]+[A-Z][A-Z0-9_]*|Duration::from_(secs|millis|micros|nanos|mins)\([0-9_]+\)|with_capacity\([0-9_]+\)|(channel|sync_channel)(::<[^>]+>)?\([0-9][0-9_]*\)|(Semaphore::new|buffered|buffer_unordered)\([0-9][0-9_]*\)|[A-Za-z_][A-Za-z0-9_]*(channel|semaphore|buffer|queue|limit|timeout|interval|backoff|capacity|delay|deadline|max_bytes|min_bytes|poll|tick)[A-Za-z0-9_]*\([0-9][0-9_]*\)|[A-Za-z_][A-Za-z0-9_]*(channel|semaphore|buffer|queue|limit|timeout|interval|backoff|capacity|batch|delay|deadline|max_bytes|min_bytes|poll|tick)[A-Za-z0-9_]*[[:space:]]*[:=][[:space:]]*[0-9][0-9_]*|(channel|semaphore|buffer|queue|limit|timeout|interval|backoff|capacity|batch|delay|deadline|max_bytes|min_bytes|poll|tick)[A-Za-z0-9_]*[[:space:]]*[:=][[:space:]]*[0-9][0-9_]*|\.(min|max)\([1-9][0-9_]*\))' \
    crates
  rg -nH \
    '^[[:space:]]+[0-9][0-9_]*,?[[:space:]]*$' \
    crates/broker/src/share_partition/manager.rs \
    || true
} | sort -u
