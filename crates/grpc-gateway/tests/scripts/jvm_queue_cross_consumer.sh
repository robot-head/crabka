#!/usr/bin/env sh
set -eu

KAFKA_IMAGE="${CRABKA_JVM_QUEUE_KAFKA_IMAGE:-mirror.gcr.io/apache/kafka:4.1.0}"
VALUE="${CRABKA_JVM_QUEUE_VALUE:-jvm-queue-cross-consumer}"
DRY_RUN=0

usage() {
  cat <<'USAGE'
Usage: jvm_queue_cross_consumer.sh --gateway-endpoint URL --bootstrap-server HOST:PORT --topic NAME --group NAME [--dry-run]

Runs the MSG6 JVM queue cross-consumer flow:
  1. create the topic with the JVM Kafka tools;
  2. produce one record with the JVM kafka-console-producer path;
  3. acquire that record through the Crabka gateway QueueAcquire RPC;
  4. acknowledge it with QueueAcknowledge(ACCEPT);
  5. run the JVM kafka-console-share-consumer on the same topic/group and fail
     if the accepted record is redelivered.

Prerequisites for a real run: docker, curl, jq, an already-running Crabka
broker reachable at --bootstrap-server, and an already-running gateway reachable
at --gateway-endpoint. The broker must advertise an address reachable from the
Kafka container (for example host.docker.internal:9092 with Docker's host-gateway
mapping).
USAGE
}

GATEWAY_ENDPOINT=
BOOTSTRAP_SERVER=
TOPIC=
GROUP=

while [ "$#" -gt 0 ]; do
  case "$1" in
    --gateway-endpoint)
      GATEWAY_ENDPOINT="${2:-}"
      shift 2
      ;;
    --bootstrap-server)
      BOOTSTRAP_SERVER="${2:-}"
      shift 2
      ;;
    --topic)
      TOPIC="${2:-}"
      shift 2
      ;;
    --group)
      GROUP="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

require_value() {
  name="$1"
  value="$2"
  if [ -n "$value" ]; then
    return 0
  fi
  echo "missing required $name" >&2
  usage >&2
  exit 64
}

require_command() {
  command_name="$1"
  if command -v "$command_name" >/dev/null 2>&1; then
    return 0
  fi
  echo "missing required command '$command_name' for JVM queue cross-consumer run" >&2
  exit 69
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

connect_url() {
  method="$1"
  printf '%s/crabka.gateway.v1.Gateway/%s' "${GATEWAY_ENDPOINT%/}" "$method"
}

require_value "--gateway-endpoint" "$GATEWAY_ENDPOINT"
require_value "--bootstrap-server" "$BOOTSTRAP_SERVER"
require_value "--topic" "$TOPIC"
require_value "--group" "$GROUP"

if [ "$DRY_RUN" -eq 1 ]; then
  cat <<PLAN
JVM queue cross-consumer plan
kafka-image=$KAFKA_IMAGE
bootstrap-server=$BOOTSTRAP_SERVER
gateway-endpoint=$GATEWAY_ENDPOINT
topic=$TOPIC
group=$GROUP
produce-command=docker run --rm --add-host=host.docker.internal:host-gateway $KAFKA_IMAGE /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic $TOPIC
acquire-url=$(connect_url QueueAcquire)
ack-url=$(connect_url QueueAcknowledge)
verify-command=docker run --rm --add-host=host.docker.internal:host-gateway $KAFKA_IMAGE /opt/kafka/bin/kafka-console-share-consumer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic $TOPIC --group $GROUP --consumer-property group.share.auto.offset.reset=earliest --timeout-ms 5000 --max-messages 1
PLAN
  exit 0
fi

require_command docker
require_command curl
require_command jq
require_command base64
require_command grep
require_command sed

docker run --rm --add-host=host.docker.internal:host-gateway "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1

printf '%s\n' "$VALUE" | docker run --rm -i --add-host=host.docker.internal:host-gateway "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC"

acquire_body=$(cat <<JSON
{"groupId":"$(json_escape "$GROUP")","topics":["$(json_escape "$TOPIC")"],"maxMessages":1,"waitMs":10000,"lockDurationMs":"30000"}
JSON
)
acquire_response=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  --data "$acquire_body" \
  "$(connect_url QueueAcquire)")

session_id=$(printf '%s' "$acquire_response" | jq -r '.sessionId // empty')
message_count=$(printf '%s' "$acquire_response" | jq '.messages | length')
if [ "$message_count" -ne 1 ]; then
  echo "QueueAcquire must return exactly one JVM-produced message; response: $acquire_response" >&2
  exit 70
fi
if [ -z "$session_id" ]; then
  echo "QueueAcquire response did not include a sessionId: $acquire_response" >&2
  exit 70
fi

encoded_value=$(printf '%s' "$acquire_response" | jq -r '.messages[0].value')
decoded_value=$(printf '%s' "$encoded_value" | base64 -d)
if [ "$decoded_value" != "$VALUE" ]; then
  echo "QueueAcquire returned unexpected value '$decoded_value'; expected '$VALUE'" >&2
  exit 70
fi

partition=$(printf '%s' "$acquire_response" | jq '.messages[0].partition')
offset=$(printf '%s' "$acquire_response" | jq '.messages[0].offset')
ack_body=$(cat <<JSON
{"sessionId":"$(json_escape "$session_id")","entries":[{"topic":"$(json_escape "$TOPIC")","partition":$partition,"offset":$offset,"type":"ACCEPT"}]}
JSON
)
ack_response=$(curl --fail --silent --show-error \
  -H 'Content-Type: application/json' \
  --data "$ack_body" \
  "$(connect_url QueueAcknowledge)")
ack_errors=$(printf '%s' "$ack_response" | jq '[.results[] | select(.error != null)] | length')
if [ "$ack_errors" -ne 0 ]; then
  echo "QueueAcknowledge returned per-entry errors: $ack_response" >&2
  exit 70
fi

set +e
jvm_output=$(docker run --rm --add-host=host.docker.internal:host-gateway "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-console-share-consumer.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --topic "$TOPIC" \
  --group "$GROUP" \
  --consumer-property group.share.auto.offset.reset=earliest \
  --timeout-ms 5000 \
  --max-messages 1 2>&1)
jvm_status=$?
set -e

if printf '%s' "$jvm_output" | grep -F "$VALUE" >/dev/null 2>&1; then
  echo "JVM share consumer redelivered an accepted gateway queue record; status=$jvm_status output:" >&2
  printf '%s\n' "$jvm_output" >&2
  exit 70
fi

if [ "$jvm_status" -ne 0 ] && ! printf '%s' "$jvm_output" | grep -F "ConsumerTimeoutException" >/dev/null 2>&1; then
  echo "JVM share consumer failed before proving no redelivery; status=$jvm_status output:" >&2
  printf '%s\n' "$jvm_output" >&2
  exit 70
fi

printf '%s\n' "JVM queue cross-consumer flow passed: JVM produce -> gateway acquire/accept -> JVM no-redelivery"
