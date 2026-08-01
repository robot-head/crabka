#!/usr/bin/env bash
# Shared helpers for bench/scripts/*.sh.

set -euo pipefail

: "${BENCH_NAMESPACE:=default}"

# Print to stderr so command substitutions (`x=$(... | log)`) stay clean.
log() {
  printf '%s %s\n' "[$(date -u +%H:%M:%S)]" "$*" >&2
}

# bootstrap_for STACK
#   echo the in-cluster DNS:port for the Kafka bootstrap Service.
#   Plaintext: port 9092 on both stacks. TLS (BENCH_TLS set): the TLS data
#   listener port, which DIFFERS per stack. crabka reserves 9093 for the KRaft
#   controller listener (CONTROLLER_PORT), so its TLS data listener is on 9094;
#   Strimzi follows its own convention (9093). The DNS host is identical to the
#   plaintext case (both listeners share the headless / bootstrap Service); only
#   the port selects the listener.
bootstrap_for() {
  local stack="$1"
  case "$stack" in
    crabka)
      local port=9092
      [[ -n "${BENCH_TLS:-}" ]] && port=9094
      printf 'demo-broker-headless.%s.svc.cluster.local:%s' "$BENCH_NAMESPACE" "$port"
      ;;
    kafka|strimzi)
      local port=9092
      [[ -n "${BENCH_TLS:-}" ]] && port=9093
      printf 'demo-kafka-bootstrap.%s.svc.cluster.local:%s' "$BENCH_NAMESPACE" "$port"
      ;;
    *)
      log "unknown stack '$stack' (want crabka|kafka)"
      return 2
      ;;
  esac
}

# tls_server_name_for STACK
#   echo the SNI / cert-SAN name the driver must present for one-way TLS.
#   LOAD-BEARING: the bootstrap DNS resolves to a pod IP and is dialed by IP,
#   so the SNI is NOT the bootstrap host — it is a name that appears as a SAN
#   on the broker serving cert:
#     crabka : the shared headless-Service FQDN (a SAN on every broker cert)
#     strimzi: the short bootstrap-Service name (a SAN on Strimzi broker certs)
tls_server_name_for() {
  local stack="$1"
  case "$stack" in
    crabka)
      printf 'demo-broker-headless.%s.svc.cluster.local' "$BENCH_NAMESPACE"
      ;;
    kafka|strimzi)
      printf 'demo-kafka-bootstrap'
      ;;
    *)
      log "unknown stack '$stack' (want crabka|kafka)"
      return 2
      ;;
  esac
}

# kafka_kind STACK
#   echo the apiVersion/Kind path the Ready condition lives under.
kafka_kind() {
  local stack="$1"
  case "$stack" in
    crabka)        printf 'kafka.crabka.io/demo' ;;
    kafka|strimzi) printf 'kafka.kafka.strimzi.io/demo' ;;
  esac
}

# wait_kafka_ready STACK [TIMEOUT_S]
#   poll the Kafka CR until status.conditions[type=Ready].status == True
#   (and for Crabka: all BENCH_BROKER_COUNT brokers are listed as ready)
#   or TIMEOUT_S elapses. Echo the elapsed wall-clock seconds.
wait_kafka_ready() {
  local stack="$1"
  local timeout="${2:-600}"
  local kind name
  case "$stack" in
    crabka)        kind="kafka.crabka.io"; name="demo" ;;
    kafka|strimzi) kind="kafka.kafka.strimzi.io"; name="demo" ;;
  esac
  local started=$(date +%s)
  local now status msg
  # Number of brokers we expect (set by run-scenario.sh before sourcing common.sh).
  local expected_brokers="${BENCH_BROKER_COUNT:-1}"
  while :; do
    now=$(date +%s)
    if (( now - started > timeout )); then
      log "TIMEOUT: $stack Kafka 'demo' not Ready after ${timeout}s"
      kubectl describe "$kind" "$name" -n "$BENCH_NAMESPACE" >&2 || true
      return 1
    fi
    status=$(kubectl get "$kind" "$name" -n "$BENCH_NAMESPACE" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [[ "$status" == "True" ]]; then
      # For Crabka multi-broker: also verify all brokers have joined.
      # The operator sets message like "3/3 brokers ready across 3 pool(s)".
      if [[ "$stack" == "crabka" && "$expected_brokers" -gt 1 ]]; then
        msg=$(kubectl get "$kind" "$name" -n "$BENCH_NAMESPACE" \
          -o jsonpath='{.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || echo "")
        if echo "$msg" | grep -q "${expected_brokers}/${expected_brokers} brokers ready"; then
          echo "$(( now - started ))"
          return 0
        fi
        sleep 5
        continue
      fi
      echo "$(( now - started ))"
      return 0
    fi
    sleep 5
  done
}


# wait_kafka_topic_ready STACK TOPIC [TIMEOUT_S]
wait_kafka_topic_ready() {
  local stack="$1"
  local topic="$2"
  local timeout="${3:-120}"
  local kind
  case "$stack" in
    crabka)        kind="kafkatopic.crabka.io" ;;
    kafka|strimzi) kind="kafkatopic.kafka.strimzi.io" ;;
  esac
  local started=$(date +%s)
  while :; do
    if (( $(date +%s) - started > timeout )); then
      log "TIMEOUT: KafkaTopic '$topic' not Ready"
      kubectl describe "$kind" "$topic" -n "$BENCH_NAMESPACE" >&2 || true
      return 1
    fi
    local status
    status=$(kubectl get "$kind" "$topic" -n "$BENCH_NAMESPACE" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    if [[ "$status" == "True" ]]; then
      return 0
    fi
    sleep 3
  done
}

# wait_job_complete JOB [TIMEOUT_S]
wait_job_complete() {
  local job="$1"
  local timeout="${2:-900}"
  local started=$(date +%s)
  while :; do
    if (( $(date +%s) - started > timeout )); then
      log "TIMEOUT: Job '$job' did not complete in ${timeout}s"
      kubectl describe job "$job" -n "$BENCH_NAMESPACE" >&2 || true
      kubectl logs -n "$BENCH_NAMESPACE" -l "job-name=$job" --tail=200 >&2 || true
      return 1
    fi
    local succeeded failed
    succeeded=$(kubectl get job "$job" -n "$BENCH_NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null || echo "")
    failed=$(kubectl get job "$job" -n "$BENCH_NAMESPACE" -o jsonpath='{.status.failed}' 2>/dev/null || echo "")
    if [[ "$succeeded" == "1" ]]; then
      return 0
    fi
    if [[ "$failed" =~ ^[1-9] ]]; then
      log "Job '$job' failed (failed=$failed)"
      kubectl logs -n "$BENCH_NAMESPACE" -l "job-name=$job" --tail=200 >&2 || true
      return 1
    fi
    sleep 5
  done
}

# scenario_field SCENARIO_PATH FIELD
#   tiny grep-based extractor for top-level scalar fields. Avoids a
#   yq dependency. Returns the value (rhs of `field:`) or empty.
scenario_field() {
  local path="$1"
  local field="$2"
  awk -v k="$field" '
    $1 == k":" {
      val = $0
      sub(/^[^:]*:[ \t]*/, "", val)
      sub(/[ \t]*#.*$/, "", val)
      print val
      exit
    }' "$path"
}

# scenario_seconds SCENARIO_PATH FIELD
#   A scenario duration as whole seconds. The scenario keys carry their unit
#   (`duration: 180s`), so the suffix has to be read rather than assumed —
#   reading the bare number would silently mistake `30m` for 30 seconds.
#   Empty (missing key or unrecognised unit) so the caller's default applies.
scenario_seconds() {
  local raw
  raw="$(scenario_field "$1" "$2")"
  [[ -n "$raw" ]] || return 0
  awk -v v="$raw" '
    BEGIN {
      if (match(v, /^[0-9]+(\.[0-9]+)?/) == 0) { exit }
      n = substr(v, 1, RLENGTH) + 0
      unit = substr(v, RLENGTH + 1)
      gsub(/[ \t]/, "", unit)
      if (unit == "s" || unit == "") { printf "%d", n }
      else if (unit == "m") { printf "%d", n * 60 }
      else if (unit == "h") { printf "%d", n * 3600 }
      else if (unit == "ms") { printf "%d", (n + 999) / 1000 }
    }'
}
