# Crabka vs Strimzi benchmark harness

This end-to-end benchmark compares **Apache Kafka with the Strimzi operator**
against **Crabka with its own operator**. Both stacks run on the same Kubernetes
cluster with identical topology, identical load, and identical observability.

> For a third-party comparison with the industry-standard
> [openmessaging-benchmark](https://github.com/openmessaging/openmessaging-benchmark)
> suite on bare GCP VMs, see [`bench/omb/`](./omb/README.md).

Both stacks speak the Kafka wire protocol. One Rust load driver in
`crates/bench-driver/` therefore runs unchanged against either bootstrap
address. The driver is built on Crabka's own `crabka-client-producer` and
`crabka-client-consumer` crates.

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

The Markdown report `bench/results/SUMMARY.md` shows these metrics side-by-side.

## Quick start

### KinD smoke (CI-friendly, ~25 minutes)

```bash
# from the repo root
just -f bench/justfile bench-ci
```

This recipe does the following:
1. Builds the bench-driver OCI image with melange and apko.
2. Creates a `crabka-bench` KinD cluster (`mirror.gcr.io/kindest/node:v1.30.0`).
3. Installs Strimzi (`1.0.0`, running Apache Kafka `4.2.0`) + Crabka + a minimal Prometheus.
4. Runs `small-msg-saturate` and `fixed-rate-latency` against both
   stacks at 1 broker / RF=1.
5. Aggregates per-run JSON into `bench/results/SUMMARY.md`.

### Real cluster (publication-quality)

```bash
# Point kubeconfig at your beefy cluster first.
just -f bench/justfile bench-cluster
```

This runs the full scenario matrix at 3 brokers / RF=3. The matrix includes
`failover` and `large-msg`. Plan for about 2 hours of runtime, plus storage for
PersistentVolumeClaims.

### 6-broker topology + averaged runs

The `6broker-rf3` topology spreads 6 brokers per stack across a 6-node
`beefy-pool`. The broker `podAntiAffinity` is one-per-node, so the pool
**must** have ≥6 nodes. Increase `broker_pool_node_count` in
[`terraform/gke/`](./terraform/gke). The high-partition scenarios need this
topology: at 100 partitions and RF=3, replication fans out across the larger
cluster.

To smooth out cloud noise, repeat the whole matrix and average the results.
`RUNS` controls the repeat count and defaults to 10. Each repeat tags its output
files with `-runNN`, so no repeat overwrites another. The report aggregator
averages all runs that share a `(scenario, topology)` cell:

```bash
# 10× the 6-broker matrix from WSL, then aggregate the averaged report.
RUNS=10 bash bench/run-matrix.sh 6broker-rf3
just -f bench/justfile bench-report
```

The report shows each cell as the mean across its runs, with a `(±N%)`
coefficient-of-variation marker. Plan the budget with care: 9 scenarios ×
2 stacks × 10 runs is a long run that uses many PVCs.

If you need a cluster, [`bench/terraform/gke/`](./terraform/gke) provisions the
exact GKE cluster that the published [Crabka vs Strimzi](https://robot-head.github.io/crabka/benchmarks/crabka-vs-strimzi/)
run used (`e2-standard-4` / COS / pd-ssd). Its
[README](./terraform/gke/README.md) gives the full provision → install → run →
aggregate recipe.

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
| `mixed-acks-all`       | cluster | 1 KiB   | 6   | 1 | 120 s  | acks=all under fixed rate |
| `failover`             | cluster | 1 KiB   | 12  | 3 | 180 s  | kill leader @ t=60 s, RF=3 |
| `high-partition-saturate` | cluster | 100 B | 100 | 3 | 60 s  | throughput ceiling at 100 partitions, RF=3, 4 prod |
| `high-partition-latency`  | cluster | 1 KiB | 100 | 3 | 120 s | fixed-rate p99 at 100 partitions, acks=all, RF=3 |
| `high-partition-fanout`   | cluster | 1 KiB | 100 | 3 | 120 s | 4 prod × 4 cons rebalance at 100 partitions, RF=3 |
| `endurance`            | cluster | 1 KiB   | 24  | 1 | 1800 s | 30-min soak |

The harness skips scenarios with `mode_tag: cluster` when the cluster does not
have 3 broker replicas. The driver then writes `"notes":
["skipped:topology-mismatch ..."]`, so the skip shows in the report.

## Output

```
bench/results/
  crabka-small-msg-saturate-1broker-rf1.json          # single run, no tag
  kafka-small-msg-saturate-1broker-rf1.json
  crabka-high-partition-saturate-6broker-rf3-run01.json  # 10× averaged run
  crabka-high-partition-saturate-6broker-rf3-run02.json
  ...
  SUMMARY.md       # human-readable means ± CV
  results.csv      # one row per run (wide) — bars with error bars
  timeseries.csv   # long format (run × time-offset × metric) — values over time
```

Each `*.json` file is one `RunOutput`. See `crates/bench-driver/src/scenario.rs`.
Repeated runs of one cell carry a `-runNN` filename tag. The Markdown summary
groups by `(scenario name, broker_count)` and **averages every metric across all
runs in a cell**. It renders crabka-vs-kafka columns with a `ratio` column. A
higher ratio is better for throughput, and a lower ratio is better for latency
and resource use. Multi-run cells also carry a `(±N%)`
coefficient-of-variation marker.

### Time-series (values over the test)

Each `RunOutput` also carries two per-run time series, so you can graph values
*during* a run and not only the end-of-run aggregates:

- `samples[]` gives client throughput (producer and consumer msgs/s) and
  **per-interval** latency (producer-ack p50/p99, consumer-e2e p99). The driver
  samples these values every 2 s of the measurement window. It tallies them
  locally per task, without hot-path locks, and then merges them.
- `broker_samples[]` gives broker CPU (cores) and working-set memory over the
  whole run window. The driver pulls them as a Prometheus **range** query at the
  end of the cell, with a 15 s step.

`bench-report --timeseries-csv` flattens these series into a long CSV with the
columns
`scenario,stack,broker_count,partitions,replication_factor,run_tag,t_offset_ms,metric,value`.
To plot lines, filter by `metric` and group by `(scenario, stack, run_tag)`.
`--csv` writes the per-run aggregate table.

## Honest gaps

Crabka is greenfield, and some Kafka features are not available yet. The
benchmark reports this clearly:

- **RF=3 / 3-broker quorum.** The harness tries this only in `cluster` mode. If
  Crabka's broker pods do not all reach Ready, the orchestrator marks the run as
  skipped with `"crabka:rf3-unstable"`. It does not report zeros.
- **Failover.** This depends on RF=3 stability.
- **Compression `zstd` / `snappy`.** The driver supports gzip and lz4 today. If
  a scenario asks for an unsupported codec, the driver stops immediately.
- **KIP-848 cooperative-sticky rebalance.** This is out of scope. `fan-out`
  exercises the eager-rebalance path only.
- **Transactional producer / EOS.** This is out of scope. No scenario uses it.

All of these appear in the `notes:` section of the report, so readers do not
mistake "skipped" for "0".

## Memory measurement caveat

`container_memory_working_set_bytes` is the cgroup-level metric that
Kubernetes evaluates against the pod's memory limit. It shows whether the
pod fits in its memory limit, and it is the primary cross-stack
comparison.

For JVM-based Kafka, this metric mixes three things: the GC-managed heap,
the non-heap memory (metaspace and direct buffers), and the Linux page
cache from `mmap`'d log segments. The driver therefore also scrapes
`jvm_memory_bytes_used{area="heap"}` and `area="nonheap"` from the
Strimzi JMX exporter. The report shows the split and a derived
`page_cache_approx = working_set − heap − non-heap`. Crabka has no JVM
and uses little `mmap`, so it has nothing equivalent to subtract.

When you read the report, use the cgroup working-set as the directly
comparable number. The heap and non-heap breakdowns apply to Kafka only.
Do not conclude that "Crabka uses 5× less heap than Kafka". The chart
does not show that.

## Files

- `crates/bench-driver/`: Rust load driver and report aggregator.
- `bench/manifests/strimzi/`: Strimzi Kafka CR (1-, 3-, and 6-broker), KafkaTopic, JMX exporter ConfigMap.
- `bench/manifests/crabka/`: Crabka Kafka and KafkaNodePool (1-, 3-, and 6-broker), KafkaTopic.
- `bench/manifests/prom/prometheus.yaml`: minimal in-cluster Prometheus with cAdvisor and broker `/metrics` scrapes.
- `bench/manifests/driver/`: driver Job template and RBAC for the failover scenario.
- `bench/scenarios/`: YAML scenario definitions.
- `bench/scripts/`: install, run, and teardown bash helpers.
- `bench/justfile`: top-level orchestration.
- `bench/terraform/gke/`: Terraform for the GKE benchmark cluster, plus the end-to-end provision → install → run → aggregate README.
- `.github/workflows/benchmark.yml`: `workflow_dispatch` and weekly cron CI run.
- `packaging/melange/bench-driver.yaml` and `packaging/apko/bench-driver.yaml`: driver image build.
