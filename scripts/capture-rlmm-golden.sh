#!/usr/bin/env bash
#
# Captures golden byte vectors from the real JVM `RemoteLogMetadataSerde`
# (mirror.gcr.io/apache/kafka:4.0.0) for Crabka's RLMM byte-exactness proof and writes them
# to crates/remote-storage-topic/tests/fixtures/rlmm_golden.json.
#
# The committed fixture is the source of truth consumed by the Rust test
# crates/remote-storage-topic/tests/jvm_serde_golden.rs. This script documents
# the provenance and lets anyone reproduce the vectors.
#
# Provenance / mechanics:
#   - mirror.gcr.io/apache/kafka:4.0.0 ships only a JRE (no javac/javap), so we extract the
#     Kafka jars from that image and compile + run scripts/capture-rlmm/Capture.java
#     against them using a JDK image (mirror.gcr.io/library/eclipse-temurin:21-jdk).
#   - Capture.java constructs each event with the FIXED constants documented in
#     that file and in jvm_serde_golden.rs, calls
#     `new RemoteLogMetadataSerde().serialize(obj)`, and prints `name=<hex>`.
#
# Usage:
#   ./scripts/capture-rlmm-golden.sh            # prints captured hex lines
#   ./scripts/capture-rlmm-golden.sh --write    # also rewrites the JSON fixture
#
# Requirements: docker, with mirror.gcr.io/apache/kafka:4.0.0 and mirror.gcr.io/library/eclipse-temurin:21-jdk pulled.
set -euo pipefail

KAFKA_IMAGE="mirror.gcr.io/apache/kafka:4.0.0"
JDK_IMAGE="mirror.gcr.io/library/eclipse-temurin:21-jdk"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CAPTURE_DIR="$REPO_ROOT/scripts/capture-rlmm"
FIXTURE="$REPO_ROOT/crates/remote-storage-topic/tests/fixtures/rlmm_golden.json"

LIBS_DIR="$(mktemp -d)"
trap 'rm -rf "$LIBS_DIR"' EXIT

echo "Extracting Kafka jars from $KAFKA_IMAGE ..." >&2
cid="$(docker create "$KAFKA_IMAGE")"
docker cp "$cid:/opt/kafka/libs/." "$LIBS_DIR/" >/dev/null
docker rm "$cid" >/dev/null

echo "Compiling + running Capture.java against the Kafka classpath ..." >&2
OUTPUT="$(docker run --rm \
  -v "$CAPTURE_DIR:/work" \
  -v "$LIBS_DIR:/libs" \
  "$JDK_IMAGE" \
  bash -lc 'cd /work && javac -proc:none -classpath "/libs/*" Capture.java && java -classpath "/libs/*:." Capture')"

echo "$OUTPUT"

if [[ "${1:-}" == "--write" ]]; then
  echo "Writing fixture $FIXTURE ..." >&2
  {
    echo "{"
    first=1
    while IFS='=' read -r name hex; do
      [[ -z "$name" ]] && continue
      if [[ $first -eq 0 ]]; then echo ","; fi
      printf '  "%s": "%s"' "$name" "$hex"
      first=0
    done <<< "$OUTPUT"
    echo ""
    echo "}"
  } > "$FIXTURE"
  echo "Wrote $FIXTURE" >&2
fi
