#!/usr/bin/env bash
set -euo pipefail
OPERATOR_TAG="${1:-crabka-operator:e2e}"
BROKER_TAG="${2:-crabka-broker:e2e}"
# Sandbox runtime for the melange build step. Override with `bubblewrap`
# if your local env has bwrap and you'd rather avoid Docker.
RUNNER="${MELANGE_RUNNER:-docker}"
WORK="$(pwd)"
mkdir -p packages

# Generate a melange signing keypair if one doesn't exist locally.
if [ ! -f melange.rsa ]; then
  melange keygen
fi

# Build the crabka-operator apk.
melange build packaging/melange/crabka-operator.yaml \
  --source-dir "$WORK" \
  --signing-key melange.rsa \
  --arch x86_64 \
  --runner "$RUNNER" \
  --out-dir packages/

# Build the crabka-broker apk (contains both crabka-broker and crabka binaries).
melange build packaging/melange/crabka-broker.yaml \
  --source-dir "$WORK" \
  --signing-key melange.rsa \
  --arch x86_64 \
  --runner "$RUNNER" \
  --out-dir packages/

# Build the operator OCI image.
apko build packaging/apko/crabka-operator.yaml \
  "$OPERATOR_TAG" \
  crabka-operator.tar \
  --arch x86_64 \
  --repository-append "$WORK/packages" \
  --keyring-append "$WORK/melange.rsa.pub"

# Build the broker OCI image.
apko build packaging/apko/crabka-broker.yaml \
  "$BROKER_TAG" \
  crabka-broker.tar \
  --arch x86_64 \
  --repository-append "$WORK/packages" \
  --keyring-append "$WORK/melange.rsa.pub"

echo "Built image archive: crabka-operator.tar (tag: $OPERATOR_TAG)"
echo "Built image archive: crabka-broker.tar (tag: $BROKER_TAG)"
