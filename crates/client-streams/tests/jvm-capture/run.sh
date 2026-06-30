#!/usr/bin/env bash
#
# Capture the 27 DSL golden fixtures from JVM Kafka Streams 4.1.0, inside Docker.
# (stateless_chain, count, repartition_merge, table_reuse, branch_merge, to_table,
#  stream_table_join, ktable_ktable_join, windowed_count, stream_stream_join,
#  stream_stream_outer_join, session_count, suppress_until_window_closes,
#  suppress_until_window_closes_logged,
#  global_table_join, process, process_values, fk_join_inner, fk_join_left,
#  sliding_window_count, sliding_window_aggregate, versioned_table,
#  cogroup, cogroup_time, cogroup_sliding, cogroup_session, kgrouped_table)
#
# Mechanism A (default, no broker): builds the 27 DSL topologies with optimization=all
# and runs Kafka's own DSL -> StreamsGroupHeartbeatRequest.Topology conversion via
# reflection, writing Crabka wire-shape JSON to ../testdata/golden/dsl/.
#
# Usage:
#   ./run.sh                   # capture fixtures via Gradle in Docker (writes the fixtures)
#   ./run.sh --javac           # capture fixtures via plain javac in Docker (no Gradle)
#   ./run.sh --verify-broker   # mechanism B cross-check: run count vs a real Kafka 4.1 broker
#   ./run.sh --sliding         # pin sliding-window (KIP-450) behavioral golden (writes testdata/sliding_window)
#   ./run.sh --kgrouped-table  # pin KTable.groupBy / KGroupedTable behavioral + ChangedSerializer goldens
#
# Requires: Docker, network access to Maven Central (and Docker Hub for the broker image).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$HERE/../testdata/golden/dsl"
JDK_IMAGE="mirror.gcr.io/library/eclipse-temurin:21-jdk"
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
    # Self-contained: download the jars and compile/run with plain javac/java.
    # Mount the parent tests/ dir (like --bufval/--iq) so the fixtures persist to the
    # host at /tests/testdata/golden/dsl.
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
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
        mkdir -p /tmp/build /tests/testdata/golden/dsl
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/Capture.java
        java -cp "/tmp/build:$RT" crabka.capture.Capture /tests/testdata/golden/dsl
      '
    ;;

  --bufval)
    # Dump the JVM suppress-buffer changelog VALUE bytes (BufferValue.serialize)
    # as hex into testdata/suppress_bufval/, for the Rust suppress_bufval codec.
    # Mount the parent tests/ dir so the output persists to the host (like --gradle).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/suppress_bufval
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/BufferValueCapture.java
        java -cp "/tmp/build:$RT" crabka.capture.BufferValueCapture /tests/testdata/suppress_bufval
      '
    ;;

  --punctuation)
    # Pin the JVM TopologyTestDriver punctuation firing semantics (stream-time +
    # wall-clock fire sequences) into testdata/punctuation/behavior.json, for the
    # Rust punctuation execution tests. Mirrors --bufval; adds streams-test-utils.
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/punctuation
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/PunctuationBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.PunctuationBehavior /tests/testdata/punctuation
      '
    ;;

  --iq)
    # Pin the JVM TopologyTestDriver Interactive-Query read semantics (KV get/
    # range/all/count, window point+range fetch, session fetch) into
    # testdata/iq/behavior.json, for the Rust IQ golden-parity tests. Mirrors
    # --punctuation; same jars (incl. streams-test-utils + rocksdb).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/iq
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/InteractiveQueryBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.InteractiveQueryBehavior /tests/testdata/iq
      '
    ;;

  --sliding)
    # Pin the JVM TopologyTestDriver sliding-window (KIP-450) behavioral golden
    # into testdata/sliding_window/behavior.json for the Rust KStreamSlidingWindowAggregate
    # out-of-order fidelity tests. Mirrors --punctuation; same jars (incl. streams-test-utils + rocksdb).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/sliding_window
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/SlidingWindowBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.SlidingWindowBehavior /tests/testdata/sliding_window
      '
    ;;

  --versioned)
    # Pin the JVM TopologyTestDriver versioned-table (KIP-889/914) behavioral +
    # changelog goldens into testdata/golden/dsl/behavioral/ for the Rust
    # VersionedKTableSourceProcessor parity + changelog-format tests. Mirrors
    # --sliding; same jars (incl. streams-test-utils + rocksdb). Compiles Capture.java
    # too (VersionedTableBehavior references Capture.versionedTableUnoptimized).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/golden/dsl/behavioral
        javac -cp "$CP" -d /tmp/build \
          src/main/java/crabka/capture/Capture.java \
          src/main/java/crabka/capture/VersionedTableBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.VersionedTableBehavior /tests/testdata/golden/dsl
      '
    ;;

  --cogroup)
    # Pin the JVM TopologyTestDriver cogroup (KIP-150) behavioral golden into
    # testdata/cogroup/behavior*.json for the Rust cogroup golden-parity tests.
    # Mirrors --sliding; same jars (incl. streams-test-utils + rocksdb).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/cogroup
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/CogroupBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.CogroupBehavior /tests/testdata/cogroup
      '
    ;;

  --fkjoin)
    # Pin the JVM FK-join (KIP-213) byte + semantic oracle into
    # testdata/fk_join/behavior.json, for the Rust FK-join codec + processor parity
    # tests. Mirrors --iq; same jars (incl. streams-test-utils + rocksdb). Captures the
    # internal CombinedKeySchema / SubscriptionWrapper / SubscriptionResponseWrapper /
    # Murmur3 serialized bytes plus inner/left TopologyTestDriver output sequences.
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/fk_join
        javac -cp "$CP" -d /tmp/build \
          src/main/java/crabka/capture/Capture.java \
          src/main/java/crabka/capture/ForeignKeyJoinBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.ForeignKeyJoinBehavior /tests/testdata/fk_join
      '
    ;;

  --versioned-joins)
    # Pin the JVM TopologyTestDriver versioned-JOIN (KIP-889/914/923) behavioral
    # goldens into testdata/versioned_joins/{asof,grace,tabletable}.json for the Rust
    # versioned stream-table / table-table join parity tests. Mirrors --versioned; same
    # jars (incl. streams-test-utils + rocksdb). Compiles Capture.java too
    # (StreamTableGraceBehavior references Capture.wireSubtopologies for the buffer
    # store's changelog topic config).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/versioned_joins
        javac -cp "$CP" -d /tmp/build \
          src/main/java/crabka/capture/Capture.java \
          src/main/java/crabka/capture/StreamTableAsOfBehavior.java \
          src/main/java/crabka/capture/StreamTableGraceBehavior.java \
          src/main/java/crabka/capture/TableTableVersionedBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.StreamTableAsOfBehavior /tests/testdata/versioned_joins
        java -cp "/tmp/build:$RT" crabka.capture.StreamTableGraceBehavior /tests/testdata/versioned_joins
        java -cp "/tmp/build:$RT" crabka.capture.TableTableVersionedBehavior /tests/testdata/versioned_joins
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
      "mirror.gcr.io/apache/kafka:${KAFKA_VERSION}" >/dev/null
    sleep 8
    docker exec crabka-broker bash -c '
      /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic in  --partitions 2 --replication-factor 1
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic out --partitions 2 --replication-factor 1
    '
    # Build the broker classpath from the broker image's own libs (has jackson, rocksdb, etc.).
    rm -rf /tmp/kafka-libs && mkdir -p /tmp/kafka-libs
    docker run --rm -v /tmp/kafka-libs:/out --entrypoint bash "mirror.gcr.io/apache/kafka:${KAFKA_VERSION}" \
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

  --emit-final)
    # Pin the JVM TopologyTestDriver emit-final (KIP-825 EmitStrategy.onWindowClose)
    # behavioral golden into testdata/emit_final/{time,sliding,session}.json for the
    # Rust emit-final parity tests. Mirrors --sliding; same jars (incl. streams-test-utils + rocksdb).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/emit_final
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/EmitFinalBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.EmitFinalBehavior /tests/testdata/emit_final
      '
    ;;

  --kgrouped-table)
    # Pin the JVM TopologyTestDriver KTable.groupBy / KGroupedTable behavioral golden
    # (count + reduce + aggregate) and ChangedSerializer wire bytes into
    # testdata/kgrouped_table/{behavior.json,changed_bytes.json} for the Rust
    # KGroupedTable implementation parity tests. Also regenerates the topology golden
    # testdata/golden/dsl/kgrouped_table.topology.json via Capture.java (mechanism A).
    # Mirrors --cogroup; same jars (incl. streams-test-utils + rocksdb).
    TESTS_DIR="$(cd "$HERE/.." && pwd)"
    docker run --rm \
      -v "$TESTS_DIR":/tests -w /tests/jvm-capture \
      "$JDK_IMAGE" bash -c '
        set -euo pipefail
        M=https://repo1.maven.org/maven2
        J=/tmp/j; mkdir -p "$J"
        get() { f=$(basename "$2"); [ -f "$J/$f" ] || curl -sSfL "$M/$1/$2" -o "$J/$f"; }
        get org/apache/kafka/kafka-streams/'"$KAFKA_VERSION"' kafka-streams-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-streams-test-utils/'"$KAFKA_VERSION"' kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar
        get org/apache/kafka/kafka-clients/'"$KAFKA_VERSION"' kafka-clients-'"$KAFKA_VERSION"'.jar
        get org/slf4j/slf4j-api/1.7.36 slf4j-api-1.7.36.jar
        get org/rocksdb/rocksdbjni/'"$ROCKSDB_VERSION"' rocksdbjni-'"$ROCKSDB_VERSION"'.jar
        CP="$J/kafka-streams-'"$KAFKA_VERSION"'.jar:$J/kafka-streams-test-utils-'"$KAFKA_VERSION"'.jar:$J/kafka-clients-'"$KAFKA_VERSION"'.jar:$J/rocksdbjni-'"$ROCKSDB_VERSION"'.jar"
        RT="$CP:$J/slf4j-api-1.7.36.jar"
        mkdir -p /tmp/build /tests/testdata/kgrouped_table /tests/testdata/golden/dsl
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/KGroupedTableBehavior.java
        java -cp "/tmp/build:$RT" crabka.capture.KGroupedTableBehavior /tests/testdata/kgrouped_table
        javac -cp "$CP" -d /tmp/build src/main/java/crabka/capture/Capture.java
        java -cp "/tmp/build:$RT" crabka.capture.Capture /tests/testdata/golden/dsl
      '
    ;;

  *)
    echo "usage: $0 [--gradle|--javac|--bufval|--punctuation|--iq|--fkjoin|--sliding|--cogroup|--emit-final|--versioned-joins|--kgrouped-table|--verify-broker]" >&2
    exit 2
    ;;
esac

echo "fixtures in: $OUT_DIR"
ls -1 "$OUT_DIR" 2>/dev/null || true
