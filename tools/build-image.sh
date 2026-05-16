#!/usr/bin/env bash
set -euo pipefail
TAG="${1:-crabka-operator:dev}"
# Sandbox runtime for the melange build step. Override with `bubblewrap`
# if your local env has bwrap and you'd rather avoid Docker.
RUNNER="${MELANGE_RUNNER:-docker}"
WORK="$(pwd)"
mkdir -p packages

# Generate a melange signing keypair if one doesn't exist locally.
if [ ! -f melange.rsa ]; then
  melange keygen
fi

# Build the apk.
melange build packaging/melange/crabka-operator.yaml \
  --source-dir "$WORK" \
  --signing-key melange.rsa \
  --arch x86_64 \
  --runner "$RUNNER" \
  --out-dir packages/

# Build the OCI image.
apko build packaging/apko/crabka-operator.yaml \
  "$TAG" \
  crabka-operator.tar \
  --arch x86_64

echo "Built image archive: crabka-operator.tar (tag: $TAG)"
