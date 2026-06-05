#!/usr/bin/env bash
#
# Capture the 7 DSL golden fixtures from JVM Kafka Streams 4.1.0, inside Docker.
# (stateless_chain, count, repartition_merge, table_reuse, branch_merge, to_table,
#  stream_table_join)
#
# Mechanism A (default, no broker): builds the 7 DSL topologies with optimization=all
# and runs Kafka's own DSL -> StreamsGroupHeartbeatRequest.Topology conversion via
# reflection, writing Crabka wire-shape JSON to ../testdata/golden/dsl/.
#
# Usage:
#   ./run.sh                 # capture fixtures via Gradle in Docker (writes the fixtures)
#   ./run.sh --javac         # capture fixtures via plain javac in Docker (no Gradle)
#   ./run.sh --verify-broker # mechanism B cross-check: run count vs a real Kafka 4.1 broker
#
# Requires: Docker, network access to Maven Central (and Docker Hub for the broker image).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$HERE/../testdata/golden/dsl"
JDK_IMAGE="eclipse-temurin:21-jdk"
KAFKA_VERSION="4.1.0"
ROCKSDB_VERSION="9.7.3"
MODE="${1:---gradle}"

mkdir -p "$OUT_DIR"

case "$MODE" in
  --gradle)
    # Mount the parent tests/ dir so Gradle can write into testdata/golden/dsl.
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      -v capture-gradle-cache:/home/gradle/.gradle \
      gradle:8.10-jdk21 \
      gradle --no-daemon -PoutDir=/tests/testdata/golden/dsl run
    ;;

  --javac)
    # Self-contained: download the three jars and compile/run with plain javac/java.
    docker run --rm \
      -v "$HERE":/work -w /work \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar"
        RT="$CP:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/Capture.java
        java -cp "/tmp/build:$RT" crabka.capture.Capture /work/../testdata/golden/dsl
      '
    ;;

  --verify-broker)
    # Mechanism B: stand up a real Kafka 4.1 broker (KRaft, streams groups enabled),
    # run the count topology with group.protocol=streams, dump the live rebalance data.
    NET=crabka-capture
    docker network create "$NET" >/dev/null 2>&1 || true
    docker rm -f crabka-broker >/dev/null 2>&1 || true
    docker run -d --name crabka-broker --network "$NET" \
      -e KAFKA_NODE_ID=1 \
      -e KAFKA_PROCESS_ROLES=broker,controller \
      -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
      -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://crabka-broker:9092 \
      -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
      -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
      -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@crabka-broker:9093 \
      -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer,streams \
      -e KAFKA_UNSTABLE_API_VERSIONS_ENABLE=true \
      -e KAFKA_UNSTABLE_FEATURE_VERSIONS_ENABLE=true \
      "apache/kafka:${KAFKA_VERSION}" >/dev/null
    sleep 8
    docker exec crabka-broker bash -c '
      /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic in  --partitions 2 --replication-factor 1
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic out --partitions 2 --replication-factor 1
    '
    # Build the broker classpath from the broker image's own libs (has jackson, rocksdb, etc.).
    rm -rf /tmp/kafka-libs && mkdir -p /tmp/kafka-libs
    docker run --rm -v /tmp/kafka-libs:/out --entrypoint bash "apache/kafka:${KAFKA_VERSION}" \
      -c 'cp /opt/kafka/libs/*.jar /out/'
    docker run --rm --network "$NET" \
      -v "$HERE":/work -w /work -v /tmp/kafka-libs:/klibs \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        CP=$(ls /klibs/*.jar | paste -sd: -)
        mkdir -p /tmp/vbuild
        javac -cp "$CP" -d /tmp/vbuild src/verify/java/crabka/capture/CaptureBroker.java
        BOOTSTRAP=crabka-broker:9092 java -cp "/tmp/vbuild:$CP" crabka.capture.CaptureBroker \
          | grep -vE "SLF4J|^\[20|INFO|WARN|DEBUG"
      '
    docker rm -f crabka-broker >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    ;;

  *)
    echo "usage: $0 [--gradle|--javac|--verify-broker]" >&2
    exit 2
    ;;
esac

echo "fixtures in: $OUT_DIR"
ls -1 "$OUT_DIR" 2>/dev/null || true
