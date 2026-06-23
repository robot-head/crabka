#!/bin/sh
set -eu
# One bucket per signal (no URL path prefix -> avoids object-store prefix doubling).
mc alias set local "${AWS_ENDPOINT_URL:-http://minio:9000}" "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"
for b in crabka-metrics crabka-traces crabka-logs crabka-profiles; do mc mb --ignore-existing "local/$b"; done
echo "minio bootstrap: per-signal buckets ready"
