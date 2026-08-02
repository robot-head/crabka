#!/usr/bin/env bash
# Build the Creusot toolchain image (melange APK -> apko OCI tar).
# Load the result with:
#   docker load < creusot-toolchain.tar
set -euo pipefail
PIN="$(cat "$(dirname "$0")/../.creusot-version")"
TAG="${1:-crabka-creusot:${PIN}}"
RUST_TOOLCHAIN="nightly-2026-06-22"
RUNNER="${MELANGE_RUNNER:-docker}"
WORK="$(pwd)"
mkdir -p packages

if [ ! -f melange.rsa ]; then
  melange keygen
fi

melange build packaging/melange/creusot-toolchain.yaml \
  --source-dir "$WORK" \
  --signing-key melange.rsa \
  --arch x86_64 \
  --runner "$RUNNER" \
  --out-dir packages/

apko build packaging/apko/creusot-toolchain.yaml \
  "$TAG" \
  creusot-toolchain.tar \
  --arch x86_64 \
  --repository-append "$WORK/packages" \
  --keyring-append "$WORK/melange.rsa.pub"

if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
  LOADED_TAG="$(docker load -i creusot-toolchain.tar | sed -n 's/^Loaded image: //p')"
  if [ -z "$LOADED_TAG" ]; then
    echo "Unable to determine the image tag loaded from creusot-toolchain.tar." >&2
    exit 1
  fi
  docker tag "$LOADED_TAG" "$TAG"
  docker run --rm --pull=never "$TAG" \
    "rustup show active-toolchain | grep -F '$RUST_TOOLCHAIN' && cargo creusot --help >/dev/null"
else
  echo "Skipping Docker smoke verification: docker is unavailable." >&2
fi

echo "Built image archive: creusot-toolchain.tar (tag: $TAG)"
