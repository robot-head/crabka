#!/bin/sh
set -eu
# Create the shared blocks bucket used by all four observability backends.
# (metrics, traces, logs, profiles each write under their own key prefix:
#  metrics/ via the compactor's hardcoded prefix, traces/ logs/ profiles/ via
#  their --object-store-url path.)
mc alias set local "${AWS_ENDPOINT_URL:-http://minio:9000}" \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"
mc mb --ignore-existing local/crabka-blocks
echo "minio bootstrap: crabka-blocks ready"
