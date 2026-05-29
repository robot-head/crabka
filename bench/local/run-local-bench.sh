#!/usr/bin/env bash
# Local, single-box Crabka-vs-Kafka benchmark.
#
# Runs each scenario against a freshly-formatted single-node broker of
# each stack, on this machine, with the same Rust load driver
# (`crabka-bench-driver`) hitting localhost:9092. Both stacks speak the
# Kafka wire protocol, so the driver is unmodified between them.
#
# Unlike bench/justfile (which is Kubernetes + Strimzi + Prometheus),
# this harness needs nothing but a JDK (for the Kafka distro) and the
# built crabka binaries. Resource numbers (broker CPU-seconds, peak RSS)
# are scraped straight from /proc and injected into the RunOutput JSON so
# the standard `crabka-bench-report` aggregator renders them.
#
# Usage:
#   bench/local/run-local-bench.sh [SCENARIO ...]
# Env:
#   KAFKA_HOME   path to an unpacked Apache Kafka distribution (required)
#   STACKS       space-separated subset of "crabka kafka" (default both)
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCAL_DIR="$REPO_ROOT/bench/local"
RESULTS_DIR="$LOCAL_DIR/results"
WORK_DIR="${WORK_DIR:-/tmp/crabka-local-bench}"
KAFKA_HOME="${KAFKA_HOME:?set KAFKA_HOME to an unpacked Apache Kafka dir}"

CRABKA="$REPO_ROOT/target/release/crabka"
CRABKA_BROKER="$REPO_ROOT/target/release/crabka-broker"
DRIVER="$REPO_ROOT/target/release/crabka-bench-driver"

BOOTSTRAP="127.0.0.1:9092"
TOPIC="bench-topic"
CLK_TCK="$(getconf CLK_TCK)"

STACKS="${STACKS:-crabka kafka}"
SCENARIOS=("$@")
if [ "${#SCENARIOS[@]}" -eq 0 ]; then
  SCENARIOS=(
    "$REPO_ROOT/bench/scenarios/small-msg-saturate.yaml"
    "$LOCAL_DIR/scenarios/local-1kb-saturate.yaml"
    "$REPO_ROOT/bench/scenarios/fixed-rate-latency.yaml"
  )
fi

mkdir -p "$RESULTS_DIR" "$WORK_DIR"
rm -f "$RESULTS_DIR"/*.json 2>/dev/null

log() { echo "[$(date +%H:%M:%S)] $*"; }

# Read combined user+system CPU seconds for a pid from /proc/<pid>/stat.
cpu_seconds() {
  local pid="$1"
  [ -r "/proc/$pid/stat" ] || { echo 0; return; }
  # fields 14 (utime) and 15 (stime); skip the comm field which may
  # contain spaces by splitting on the closing paren.
  local rest utime stime
  rest="$(sed 's/^.*) //' "/proc/$pid/stat")"
  utime="$(echo "$rest" | awk '{print $12}')"
  stime="$(echo "$rest" | awk '{print $13}')"
  awk -v u="$utime" -v s="$stime" -v t="$CLK_TCK" 'BEGIN{printf "%.3f", (u+s)/t}'
}

# Peak resident set size (VmHWM) in bytes for a pid.
peak_rss_bytes() {
  local pid="$1"
  local kb
  kb="$(awk '/^VmHWM:/{print $2}' "/proc/$pid/status" 2>/dev/null)"
  [ -n "$kb" ] && echo $((kb * 1024)) || echo 0
}

wait_for_broker() {
  local deadline=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
         --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_port_free() {
  local deadline=$((SECONDS + 30))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if ! (exec 3<>"/dev/tcp/127.0.0.1/9092") 2>/dev/null; then
      return 0
    fi
    exec 3>&- 2>/dev/null
    sleep 1
  done
  return 0
}

create_topic() {
  local parts="$1" rf="$2"
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$TOPIC" \
    --partitions "$parts" --replication-factor "$rf" >/dev/null 2>&1
}

scenario_field() {
  # crude YAML scalar reader: scenario_field <file> <key>
  awk -v k="$2" '$1==k":"{print $2; exit}' "$1"
}

start_crabka() {
  local datadir="$WORK_DIR/crabka-data"
  rm -rf "$datadir"; mkdir -p "$datadir"
  "$CRABKA" format --log-dir "$datadir" --standalone --node-id 1 \
    --controller-listener 127.0.0.1:9093 >"$WORK_DIR/crabka-format.log" 2>&1
  # warn-level only: the default INFO filter logs every request/response,
  # which would dwarf real broker CPU under load and skew the comparison.
  RUST_LOG="${RUST_LOG:-warn}" \
  "$CRABKA_BROKER" --log-dir "$datadir" --listen-addr 127.0.0.1:9092 \
    --metrics-listen-addr none >"$WORK_DIR/crabka-broker.log" 2>&1 &
  BROKER_PID=$!
}

start_kafka() {
  local logdir="$WORK_DIR/kafka-logs"
  rm -rf "$logdir"; mkdir -p "$logdir"
  local props="$WORK_DIR/kafka-server.properties"
  # Start from the shipped KRaft combined config, override log.dirs.
  grep -vE '^log\.dirs=' "$KAFKA_HOME/config/server.properties" >"$props"
  echo "log.dirs=$logdir" >>"$props"
  local cid
  cid="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$cid" -c "$props" \
    --standalone --ignore-formatted >"$WORK_DIR/kafka-format.log" 2>&1
  "$KAFKA_HOME/bin/kafka-server-start.sh" "$props" \
    >"$WORK_DIR/kafka-server.log" 2>&1 &
  BROKER_PID=$!
}

stop_broker() {
  [ -n "${BROKER_PID:-}" ] || return 0
  kill "$BROKER_PID" 2>/dev/null
  for _ in $(seq 1 20); do
    kill -0 "$BROKER_PID" 2>/dev/null || break
    sleep 0.5
  done
  kill -9 "$BROKER_PID" 2>/dev/null
  wait "$BROKER_PID" 2>/dev/null
  # nuke any stragglers holding the ports
  pkill -9 -f "kafka.Kafka" 2>/dev/null
  pkill -9 -f "crabka-broker --log-dir" 2>/dev/null
  BROKER_PID=""
}

run_one() {
  local stack="$1" scenario="$2"
  local name parts rf
  name="$(scenario_field "$scenario" name)"
  parts="$(scenario_field "$scenario" partitions)"; parts="${parts:-1}"
  rf="$(scenario_field "$scenario" replication_factor)"; rf="${rf:-1}"
  local out="$RESULTS_DIR/${stack}-${name}.json"

  log "=== $stack / $name (partitions=$parts rf=$rf) ==="
  wait_for_port_free

  local t0=$SECONDS
  if [ "$stack" = "crabka" ]; then start_crabka; else start_kafka; fi

  if ! wait_for_broker; then
    log "!! $stack broker did not become ready; tail of log:"
    tail -20 "$WORK_DIR/${stack}"*.log 2>/dev/null
    stop_broker; return 1
  fi
  local startup_ms=$(( (SECONDS - t0) * 1000 ))
  log "$stack ready in ${startup_ms}ms (pid $BROKER_PID)"

  if ! create_topic "$parts" "$rf"; then
    log "!! failed to create topic on $stack"; stop_broker; return 1
  fi

  local cpu_before peak_rss
  cpu_before="$(cpu_seconds "$BROKER_PID")"

  BENCH_STACK="$stack" "$DRIVER" \
    --scenario "$scenario" \
    --bootstrap "$BOOTSTRAP" \
    --stack "$stack" \
    --topic "$TOPIC" \
    --broker-count 1 \
    --out "$out" 2>>"$WORK_DIR/driver-${stack}-${name}.log"
  local rc=$?

  local cpu_after
  cpu_after="$(cpu_seconds "$BROKER_PID")"
  peak_rss="$(peak_rss_bytes "$BROKER_PID")"
  stop_broker

  if [ "$rc" -ne 0 ] || [ ! -f "$out" ]; then
    log "!! driver failed for $stack/$name (rc=$rc)"
    tail -15 "$WORK_DIR/driver-${stack}-${name}.log" 2>/dev/null
    return 1
  fi

  # Inject locally-measured resource numbers + startup time into the
  # RunOutput so crabka-bench-report renders msgs/s-per-core and
  # working-set MB.
  local cpu_delta
  cpu_delta="$(awk -v a="$cpu_after" -v b="$cpu_before" 'BEGIN{d=a-b; if(d<0)d=0; printf "%.3f", d}')"
  CPU_DELTA="$cpu_delta" PEAK_RSS="$peak_rss" STARTUP_MS="$startup_ms" \
  python3 - "$out" <<'PY'
import json, os, sys
path = sys.argv[1]
with open(path) as f:
    o = json.load(f)
cpu = float(os.environ["CPU_DELTA"])
rss = int(os.environ["PEAK_RSS"])
o.setdefault("resource", {})
o["resource"]["broker_cpu_seconds"] = cpu
o["resource"]["mem_cgroup_working_set_bytes"] = rss
produced = o.get("throughput", {}).get("msgs_produced", 0)
o["resource"]["msgs_per_cpu_core"] = (produced / cpu) if cpu > 0 else 0.0
o["startup_ms"] = int(os.environ["STARTUP_MS"])
with open(path, "w") as f:
    json.dump(o, f, indent=2)
PY

  local mps
  mps="$(python3 -c "import json;print(round(json.load(open('$out'))['throughput']['producer_msgs_per_sec']))")"
  log "$stack/$name done: ${mps} msgs/s, broker cpu=${cpu_delta}s peak_rss=$((peak_rss/1024/1024))MiB"
}

for scenario in "${SCENARIOS[@]}"; do
  for stack in $STACKS; do
    run_one "$stack" "$scenario" || log "(continuing after failure)"
  done
done

log "Aggregating report..."
"$REPO_ROOT/target/release/crabka-bench-report" \
  --input-dir "$RESULTS_DIR" \
  --out "$RESULTS_DIR/SUMMARY.md"
log "Wrote $RESULTS_DIR/SUMMARY.md"
