#!/bin/sh
set -eu

# One bucket per signal (no URL path prefix -> avoids object-store prefix doubling).
endpoint="${AWS_ENDPOINT_URL:-http://rustfs:9000}"

for bucket in crabka-metrics crabka-traces crabka-logs crabka-profiles; do
  if aws --endpoint-url "$endpoint" s3api head-bucket --bucket "$bucket" >/dev/null 2>&1; then
    continue
  fi
  aws --endpoint-url "$endpoint" s3api create-bucket --bucket "$bucket" >/dev/null
done

echo "rustfs bootstrap: per-signal buckets ready"
