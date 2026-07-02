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

This runs the full scenario matrix (including `failover` and `large-msg`)
at 3 brokers / RF=3. Plan for ~2 hours of runtime, plus storage for
PersistentVolumeClaims.

### 6-broker topology + averaged runs

The `6broker-rf3` topology spreads 6 brokers per stack across a 6-node
`beefy-pool` (the broker `podAntiAffinity` is one-per-node, so the pool
**must** have ≥6 nodes — bump `broker_pool_node_count` in
[`terraform/gke/`](./terraform/gke)). It is what the high-partition
scenarios are designed for: 100 partitions at RF=3 actually fan replication
out across the larger cluster.

To smooth out cloud noise, repeat the whole matrix and average. `RUNS`
(default 10) controls the repeat count; each repeat tags its output files
`-runNN` so nothing is clobbered, and the report aggregator averages all
runs that share a `(scenario, topology)` cell:

```bash
# 10× the 6-broker matrix from WSL, then aggregate the averaged report.
RUNS=10 bash bench/run-matrix.sh 6broker-rf3
just -f bench/justfile bench-report
```

The report shows each cell as the mean across its runs with a `(±N%)`
coefficient-of-variation marker. Budget accordingly: 9 scenarios × 2 stacks
× 10 runs is a long, PVC-heavy campaign.

Need a cluster? [`bench/terraform/gke/`](./terraform/gke) provisions the exact
GKE cluster the published [Crabka vs Strimzi](https://robot-head.github.io/crabka/benchmarks/crabka-vs-strimzi/)
run used (`e2-standard-4` / COS / pd-ssd), and its
[README](./terraform/gke/README.md) is the full provision → install → run →
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

Scenarios with `mode_tag: cluster` are auto-skipped when the cluster
doesn't have 3 broker replicas; the driver writes `"notes":
["skipped:topology-mismatch ..."]` so it shows up in the report.

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

Each `*.json` is one `RunOutput` (see `crates/bench-driver/src/scenario.rs`).
Repeated runs of one cell carry a `-runNN` filename tag. The Markdown summary
groups by `(scenario name, broker_count)`, **averages every metric across all
runs in a cell**, and renders crabka-vs-kafka columns with a `ratio` column
(higher-is-better for throughput, lower-is-better for latency/resource) plus a
`(±N%)` coefficient-of-variation marker on multi-run cells.

### Time-series (values over the test)

Each `RunOutput` also carries two per-run time series, so you can graph values
*during* a run instead of just end-of-run aggregates:

- `samples[]` — client throughput (producer/consumer msgs/s) and **per-interval**
  latency (producer-ack p50/p99, consumer-e2e p99) sampled every 2 s of the
  measurement window. Tallied locally per task (no hot-path locks) and merged.
- `broker_samples[]` — broker CPU (cores) and working-set memory over the whole
  run window, pulled as a Prometheus **range** query at cell end (15 s step).

`bench-report --timeseries-csv` flattens these into a tidy long CSV
(`scenario,stack,broker_count,partitions,replication_factor,run_tag,t_offset_ms,metric,value`)
— filter by `metric` and group by `(scenario, stack, run_tag)` to plot lines.
`--csv` writes the per-run aggregate table.

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
- `bench/manifests/strimzi/` — Strimzi Kafka CR (1-, 3-, and 6-broker), KafkaTopic, JMX exporter ConfigMap.
- `bench/manifests/crabka/` — Crabka Kafka + KafkaNodePool (1-, 3-, and 6-broker), KafkaTopic.
- `bench/manifests/prom/prometheus.yaml` — minimal in-cluster Prometheus with cAdvisor + broker `/metrics` scrapes.
- `bench/manifests/driver/` — driver Job template + RBAC for the failover scenario.
- `bench/scenarios/` — YAML scenario definitions.
- `bench/scripts/` — install / run / teardown bash helpers.
- `bench/justfile` — top-level orchestration.
- `bench/terraform/gke/` — Terraform for the GKE benchmark cluster, plus the end-to-end provision → install → run → aggregate README.
- `.github/workflows/benchmark.yml` — `workflow_dispatch` + weekly cron CI run.
- `packaging/melange/bench-driver.yaml` + `packaging/apko/bench-driver.yaml` — driver image build.
