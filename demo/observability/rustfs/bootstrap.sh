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
obsolete_logs_manifest_bucket="crabka-logs"
obsolete_logs_manifest_key="logs/tenant=demo/index/logs/manifest.json"

if aws --endpoint-url "$endpoint" s3api head-object \
  --bucket "$obsolete_logs_manifest_bucket" \
  --key "$obsolete_logs_manifest_key" >/dev/null 2>&1; then
  removed_obsolete_logs_manifest=0
  for attempt in 1 2 3 4 5; do
    if aws --endpoint-url "$endpoint" s3api delete-object \
      --bucket "$obsolete_logs_manifest_bucket" \
      --key "$obsolete_logs_manifest_key" >/dev/null 2>&1 &&
      aws --endpoint-url "$endpoint" s3api wait object-not-exists \
        --bucket "$obsolete_logs_manifest_bucket" \
        --key "$obsolete_logs_manifest_key" >/dev/null 2>&1; then
      removed_obsolete_logs_manifest=1
      echo "rustfs bootstrap: removed obsolete logs full manifest"
      break
    fi

    echo "rustfs bootstrap: retrying obsolete logs full manifest cleanup (attempt $attempt)" >&2
    sleep "$attempt"
  done

  if [ "$removed_obsolete_logs_manifest" -ne 1 ]; then
    echo "rustfs bootstrap: failed to remove obsolete logs full manifest" >&2
    exit 1
  fi
fi

echo "rustfs bootstrap: per-signal buckets ready"
