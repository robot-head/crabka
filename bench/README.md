# Crabka vs Strimzi benchmark harness

End-to-end benchmark comparing **Apache Kafka via the Strimzi operator**
against **Crabka via its own operator**, on the same Kubernetes cluster
with identical topology, identical load, and identical observability.

> Looking for a third-party comparison using the industry-standard
> [openmessaging-benchmark](https://github.com/openmessaging/openmessaging-benchmark)
> suite on bare GCP VMs? See [`bench/omb/`](./omb/README.md).

Both stacks speak the Kafka wire protocol, so a single Rust load driver
— `crates/bench-driver/`, built on top of Crabka's own
`crabka-client-producer` / `crabka-client-consumer` crates — runs
unmodified against either bootstrap address.

## What it measures

For every scenario × stack run:

| metric | source |
|---|---|
| producer throughput (msgs/s, MB/s) | driver-internal counters |
| consumer throughput (msgs/s, MB/s) | driver-internal counters |
| p50 / p95 / p99 / p999 producer-ack latency | HDR histogram per producer task |
| p50 / p95 / p99 / p999 consumer end-to-end latency | embedded `send_unix_nanos` in payload header |
| broker CPU-seconds | Prometheus `rate(container_cpu_usage_seconds_total)` |
| broker cgroup working-set (memory) | Prometheus `container_memory_working_set_bytes` |
| msgs/s per CPU-core | derived |
| operator startup time (CR apply → Ready) | wall-clock in `run-scenario.sh` |
| broker first-ack time (Ready → first ack) | wall-clock in the driver |
| (Kafka only) JVM heap & non-heap used | Strimzi JMX exporter via Prometheus |
| (Kafka only) page-cache approx | cgroup working-set − heap − non-heap |
| failover recovery time | first Ok ack post-kill |
| failover dropped messages | exhausted-retry errors during disturbance |
| failover latency spike | max sample in 30 s after kill |

The Markdown report (`bench/results/SUMMARY.md`) lays these out side-by-side.

## Quick start

### KinD smoke (CI-friendly, ~25 minutes)

```bash
# from the repo root
just -f bench/justfile bench-ci
```

This:
1. Builds the bench-driver OCI image via melange/apko.
2. Creates a `crabka-bench` KinD cluster (`kindest/node:v1.30.0`).
3. Installs Strimzi (`0.46.0`) + Crabka + a minimal Prometheus.
4. Runs `small-msg-saturate` and `fixed-rate-latency` against both
   stacks at 1 broker / RF=1.
5. Aggregates per-run JSON into `bench/results/SUMMARY.md`.

### Real cluster (publication-quality)

```bash
# Point kubeconfig at your beefy cluster first.
just -f bench/justfile bench-cluster
```

This runs the full scenario matrix (including `failover` and `large-msg`)
at 3 brokers / RF=3. Plan for ~2 hours of runtime, plus storage for
PersistentVolumeClaims.

### Iteration loop

```bash
# Run one scenario × one stack:
just -f bench/justfile run crabka small-msg-saturate 1broker-rf1
just -f bench/justfile run kafka  small-msg-saturate 1broker-rf1

# Re-aggregate the report:
just -f bench/justfile bench-report
```

## Scenarios

| name | mode | size | partitions | RF | duration | notes |
|---|---|---|---|---|---|---|
| `small-msg-saturate`   | CI      | 100 B   | 6  | 1 | 60 s   | throughput ceiling |
| `fixed-rate-latency`   | CI      | 1 KiB   | 6  | 1 | 120 s  | coordinated-omission-free p99 |
| `large-msg`            | cluster | 100 KiB | 6  | 1 | 60 s   | MB/s ceiling, lz4 |
| `fan-out`              | cluster | 1 KiB   | 24 | 1 | 120 s  | 4 prod × 4 cons in one group |
| `mixed-acks-all`       | cluster | 1 KiB   | 6  | 1 | 120 s  | acks=all under fixed rate |
| `failover`             | cluster | 1 KiB   | 12 | 3 | 180 s  | kill leader @ t=60 s, RF=3 |
| `endurance`            | cluster | 1 KiB   | 24 | 1 | 1800 s | 30-min soak |

Scenarios with `mode_tag: cluster` are auto-skipped when the cluster
doesn't have 3 broker replicas; the driver writes `"notes":
["skipped:topology-mismatch ..."]` so it shows up in the report.

## Output

```
bench/results/
  crabka-small-msg-saturate-1broker-rf1.json
  kafka-small-msg-saturate-1broker-rf1.json
  ...
  SUMMARY.md
```

Each `*.json` is one `RunOutput` (see `crates/bench-driver/src/scenario.rs`).
The Markdown summary groups by scenario name and renders crabka-vs-kafka
columns with a `ratio` column (higher-is-better for throughput,
lower-is-better for latency/resource).

## Honest gaps

Crabka is greenfield; some Kafka features aren't shipped yet. The
benchmark surfaces this honestly:

- **RF=3 / 3-broker quorum** — only attempted in `cluster` mode. If
  Crabka's broker pods don't all reach Ready, the orchestrator marks the
  run skipped (`"crabka:rf3-unstable"`) rather than reporting zeros.
- **Failover** — depends on RF=3 stability.
- **Compression `zstd` / `snappy`** — wired today are gzip and lz4; if a
  scenario asks for an unsupported codec the driver fails fast.
- **KIP-848 cooperative-sticky rebalance** — out of scope; `fan-out`
  exercises the eager-rebalance path only.
- **Transactional producer / EOS** — out of scope; no scenario uses it.

All of these appear in the report's `notes:` section so readers don't
mistake "skipped" for "0".

## Memory measurement caveat

`container_memory_working_set_bytes` is the cgroup-level metric
Kubernetes evaluates against the pod's memory limit — it's the
production-relevant "does this fit in its envelope?" number, and it's
the primary cross-stack comparison.

For JVM-based Kafka it conflates three things: GC-managed heap,
non-heap (metaspace, direct buffers), and Linux page cache from
`mmap`'d log segments. The driver therefore also scrapes
`jvm_memory_bytes_used{area="heap"}` and `area="nonheap"` from the
Strimzi JMX exporter, and the report shows the split plus a derived
`page_cache_approx = working_set − heap − non-heap`. Crabka, having no
JVM and minimal `mmap` reliance, has nothing equivalent to subtract.

So when you read the report: cgroup working-set is the apples-to-apples
number. Heap/non-heap breakdowns are Kafka-only colour. Don't conclude
"Crabka uses 5× less heap than Kafka" — that's not what the chart is
showing.

## Files

- `crates/bench-driver/` — Rust load driver + report aggregator.
- `bench/manifests/strimzi/` — Strimzi Kafka CR (1- and 3-broker), KafkaTopic, JMX exporter ConfigMap.
- `bench/manifests/crabka/` — Crabka Kafka + KafkaNodePool (1- and 3-broker), KafkaTopic.
- `bench/manifests/prom/prometheus.yaml` — minimal in-cluster Prometheus with cAdvisor + broker `/metrics` scrapes.
- `bench/manifests/driver/` — driver Job template + RBAC for the failover scenario.
- `bench/scenarios/` — YAML scenario definitions.
- `bench/scripts/` — install / run / teardown bash helpers.
- `bench/justfile` — top-level orchestration.
- `.github/workflows/benchmark.yml` — `workflow_dispatch` + weekly cron CI run.
- `packaging/melange/bench-driver.yaml` + `packaging/apko/bench-driver.yaml` — driver image build.
