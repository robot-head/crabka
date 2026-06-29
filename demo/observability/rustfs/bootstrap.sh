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

# The logs querier reads tenant shard manifests in this fixture. Older fixture
# revisions also rewrote one full tenant manifest on every compaction batch; on
# RustFS that left large stale on-disk overwrite parts even though S3 exposes
# only the latest object. Remove the obsolete key when reusing an old volume.
aws --endpoint-url "$endpoint" s3 rm \
  s3://crabka-logs/logs/tenant=demo/index/logs/manifest.json >/dev/null 2>&1 || true

echo "rustfs bootstrap: per-signal buckets ready"
