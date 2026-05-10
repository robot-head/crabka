#!/usr/bin/env bash
set -euo pipefail

# Usage: tools/sync-schemas.sh <git-ref>
# Vendors Apache Kafka's wire-protocol JSON schemas at the given ref
# into crates/protocol/schemas/.

REF="${1:?usage: sync-schemas.sh <git-ref>}"
REPO="https://github.com/apache/kafka.git"
DEST="crates/protocol/schemas"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Cloning apache/kafka at $REF into $TMP..."
git clone --depth 1 --branch "$REF" "$REPO" "$TMP/kafka" 2>/dev/null || {
  git clone "$REPO" "$TMP/kafka"
  (cd "$TMP/kafka" && git checkout "$REF")
}

SRC="$TMP/kafka/clients/src/main/resources/common/message"
test -d "$SRC" || { echo "schema dir not found under upstream"; exit 1; }

rm -rf "$DEST"
mkdir -p "$DEST"
cp "$SRC"/*.json "$DEST"/

SHA=$(cd "$TMP/kafka" && git rev-parse HEAD)
cat > "$DEST/VERSION" <<EOF
ref: $REF
sha: $SHA
synced_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

echo "Vendored $(ls "$DEST"/*.json | wc -l) schemas at $SHA"
