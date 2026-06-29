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

# The logs querier lists tenant shard manifests in this fixture. Older fixture
# revisions also rewrote one full tenant manifest and one shard catalog on every
# compaction batch; on RustFS those static keys left large stale on-disk
# overwrite parts even though S3 exposes only the latest object. Remove the
# obsolete keys when reusing an old volume.
obsolete_logs_manifest_bucket="crabka-logs"
obsolete_logs_manifest_key="logs/tenant=demo/index/logs/manifest.json"
obsolete_logs_shard_catalog_key="logs/tenant=demo/index/logs/shards/manifest.json"

delete_obsolete_object() {
  label="$1"
  bucket="$2"
  key="$3"

  if ! aws --endpoint-url "$endpoint" s3api head-object \
    --bucket "$bucket" \
    --key "$key" >/dev/null 2>&1; then
    return 0
  fi

  removed_obsolete_object=0
  for attempt in 1 2 3 4 5; do
    if aws --endpoint-url "$endpoint" s3api delete-object \
      --bucket "$bucket" \
      --key "$key" >/dev/null 2>&1 &&
      aws --endpoint-url "$endpoint" s3api wait object-not-exists \
        --bucket "$bucket" \
        --key "$key" >/dev/null 2>&1; then
      removed_obsolete_object=1
      echo "rustfs bootstrap: removed obsolete $label"
      break
    fi

    echo "rustfs bootstrap: retrying obsolete $label cleanup (attempt $attempt)" >&2
    sleep "$attempt"
  done

  if [ "$removed_obsolete_object" -ne 1 ]; then
    echo "rustfs bootstrap: failed to remove obsolete $label" >&2
    exit 1
  fi
}

delete_obsolete_object \
  "logs full manifest" \
  "$obsolete_logs_manifest_bucket" \
  "$obsolete_logs_manifest_key"
delete_obsolete_object \
  "logs shard catalog" \
  "$obsolete_logs_manifest_bucket" \
  "$obsolete_logs_shard_catalog_key"

echo "rustfs bootstrap: per-signal buckets ready"
