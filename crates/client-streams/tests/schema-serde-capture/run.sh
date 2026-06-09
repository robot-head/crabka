#!/usr/bin/env bash
# Capture byte-exact Confluent serializer goldens into ./out/<fmt>.hex.
# Requires Docker. Not run in CI. After running, copy the hex into
# crates/client-streams/tests/testdata/schema_serde/<fmt>/order.hex and update
# the seeded schema ids in the *_golden.rs tests to the ids printed below.
set -euo pipefail
cd "$(dirname "$0")"

cleanup() { docker compose down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo ">> starting kafka + schema-registry"
docker compose up -d kafka schema-registry

echo ">> waiting for schema-registry on :8081"
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8081/subjects >/dev/null 2>&1; then break; fi
  sleep 2
  if [ "$i" = 60 ]; then echo "schema-registry did not come up" >&2; exit 1; fi
done

echo ">> running capture"
docker compose run --rm capture

echo ">> captured files:"
for f in avro protobuf json; do
  printf '  %-9s %s\n' "$f" "$(cat "out/$f.hex" 2>/dev/null || echo MISSING)"
done
