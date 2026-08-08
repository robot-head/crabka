+++
title = "Crabka vs Strimzi (Kubernetes)"
description = "Operator-managed, six-broker GKE benchmark: Crabka vs Strimzi (Apache Kafka) under identical pod resources, matching throughput at a fraction of the memory."
weight = 20
template = "docs/page.html"

[extra]
lead = "Operator-managed, six-broker comparison on GKE. Crabka and Strimzi (Apache Kafka) run under identical Kubernetes pod resources, driven by the same Rust load driver over the Kafka wire protocol. Each scenario is run ten times and averaged. Crabka matches or beats Strimzi's throughput while resident in a fraction of the memory and serving fetches zero-copy — sendfile(2) on plaintext and kernel-TLS (kTLS) on encrypted connections."
+++

This is a Kubernetes comparison of two six-broker clusters at RF=3. The Crabka
operator manages one cluster, and [Strimzi](https://strimzi.io) manages the
other. Both clusters run on the same GKE node pool with byte-for-byte identical
pod resources.

The same Rust load driver, `crabka-bench-driver`, runs each scenario over the
Kafka wire protocol. It produces and consumes through a Kubernetes `Job`. Broker
CPU and container working-set memory come from cAdvisor. The JVM heap / non-heap
split comes from the Strimzi JMX exporter. **Every cell is the mean of ten
runs**, so the numbers below carry run-to-run error bars instead of a single
sample. The harness is in
[`bench/`](https://github.com/robot-head/crabka/tree/main/bench).

The [Throughput, CPU & memory over time](@/benchmarks/timeseries.md) page shows
the same data **over the run window**. It gives throughput, broker CPU, and
broker memory as time series, with every individual run plus the average.

## Environment

| | |
|---|---|
| Platform | GKE, `e2-standard-4` nodes (4 vCPU, 16 GiB), one broker per node |
| Storage | `pd-ssd` PersistentClaim, 200 GiB per broker |
| Pod resources | **identical** both stacks: 2–4 vCPU, 6 GiB request / 12 GiB limit |
| Crabka | `crabka-operator` + `crabka-broker`, release build, 6 brokers, RF=3 |
| Strimzi | Strimzi 1.0, **Apache Kafka 4.2** (KRaft), JVM, 6 brokers, RF=3 |
| Driver | `crabka-bench-driver`, in-cluster `Job`, Kafka wire protocol |
| Repeats | 10 runs per (scenario, stack); cells are the mean |

The two stacks get the same pods, the same storage class, the same partition
counts, and the same driver. The brokers run one cluster at a time.

## Produce-and-consume

The same driver consumes back every record that it produces. Higher is better,
except for memory. **Throughput** is the crabka ÷ strimzi ratio. **Memory** and
**msgs/CPU-core** show the Crabka advantage.

| scenario (6 brokers, RF=3) | Crabka | Strimzi | throughput | Crabka mem | Strimzi mem | msgs/CPU-core |
|---|--:|--:|--:|--:|--:|--:|
| **small-msg-saturate** (100 B, acks=leader) | 99.4k msgs/s | 70.0k msgs/s | **1.42×** | **190 MiB** | 9 217 MiB | **4.1×** |
| **fan-out** (1 KiB, 4P/4C) | 68.4k msgs/s | 47.9k msgs/s | **1.43×** | **620 MiB** | 7 483 MiB | **2.2×** |
| **fixed-rate-latency** (1 KiB, paced) | 9.1k msgs/s | 6.8k msgs/s | **1.34×** | **305 MiB** | 5 159 MiB | **3.3×** |
| **large-msg** (100 KiB, acks=leader) | 5.8k msgs/s | 4.2k msgs/s | **1.37×** | **131 MiB** | 7 443 MiB | **4.6×** |
| **mixed-acks** (1 KiB, acks=all) | 20.0k msgs/s | 19.6k msgs/s | 1.02× | **306 MiB** | 6 277 MiB | **2.6×** |
| **high-partition-saturate** (100 part., 100 B) | 274.2k msgs/s | 235.8k msgs/s | **1.16×** | **1 812 MiB** | 8 106 MiB | **1.6×** |
| **high-partition-latency** (100 part., paced) | 5.8k msgs/s | 5.3k msgs/s | **1.08×** | **2 306 MiB** | 7 261 MiB | **1.5×** |
| **high-partition-fanout** (100 part., fan-out) | 52.8k msgs/s | 39.0k msgs/s | **1.36×** | **5 584 MiB** | 12 358 MiB | **1.5×** |

On throughput, Crabka wins every steady-state and fan-out workload outright at
**1.34–1.43×**. It ties the `acks=all` workload, and it leads both the
saturating and the paced 100-partition runs. Crabka's larger advantages are in
memory and CPU:

- **Memory:** a Crabka broker's container working set runs from the low hundreds
  of MiB up to a few GiB on the heaviest 100-partition workloads. A Strimzi
  broker carries **5–12 GiB**, most of it JVM heap. The **2.2–57× gap** depends
  on the workload. The gap is structural and is not a tuning default.
- **CPU efficiency:** Crabka moves **1.5–4.6× more messages per CPU-core** in
  every scenario. It gives equal or better throughput for less CPU.

## Failover

A ninth scenario kills partition 0's leader mid-run. Strimzi fails over
transparently, with no measured message loss. Crabka recovers in tens of
seconds. Its producers find the new leader after a latency spike and a small
number of dropped records, a fraction of a percent of the messages in flight.
The producers then continue. Recovery is repeatable, and every run in the matrix
re-converges within that window.

Crabka's whole-run failover throughput trails Strimzi's at 0.89×. This is the one
scenario where Strimzi leads outright. Even here, a Crabka broker holds a
fraction of the memory at 2.3× leaner, and it turns 1.6× the messages per
CPU-core.

## Zero-copy fetch

Crabka serves the records portion of a `Fetch` response **without copying it
through userspace**. On Linux, it uses `sendfile(2)` to send the log-segment
bytes straight from the page cache to the socket and out to the NIC, exactly as
Apache Kafka does. The produce path also appends the producer's record batch
verbatim, without a decode and re-encode round trip. macOS and the BSDs use
their native `sendfile`. Other platforms use a buffered copy.

On **encrypted** connections, Crabka uses **Linux kTLS**, or kernel TLS. After
the rustls handshake, the kernel does the record encryption, so `sendfile` runs
*through* TLS. The kernel encrypts the page-cache pages into TLS records on the
way to the NIC. Encryption stays zero-copy, so a Crabka broker's TLS fetch
throughput tracks its plaintext throughput. Every wire byte is identical on the
plaintext and TLS paths. kTLS moves only where encryption occurs, not what
crosses the wire.

## Methodology notes

- Both stacks get identical pod resource requests/limits, the same `pd-ssd`
  storage class, the same partition counts, and the same in-cluster driver.
- Container memory is the cgroup working set (`container_memory_working_set_bytes`)
  summed over the broker pods. CPU is the cAdvisor CPU-seconds over the measured
  window. The Strimzi JMX exporter gives the JVM heap / non-heap split.
- The harness runs each (scenario, stack) cell ten times. The table shows the
  mean, and the [time-series page](@/benchmarks/timeseries.md) shows every run
  plus the average with the run-to-run spread. Shared-cloud infrastructure has
  large variance, so the ratio between the two stacks is the most reliable
  measure.

## Reproduce

Terraform defines the GKE cluster. The code is in the repo at
[`bench/terraform/gke/`](https://github.com/robot-head/crabka/tree/main/bench/terraform/gke).
Its [README](https://github.com/robot-head/crabka/tree/main/bench/terraform/gke/README.md)
gives the full recipe: provision the cluster, install both operators and
Prometheus, run the matrix, and aggregate the results.

```bash
# provision the e2-standard-4 / pd-ssd cluster and point kubectl at it:
cd bench/terraform/gke && terraform init && terraform apply
eval "$(terraform output -raw get_credentials_command)"

# install both operators + Prometheus:
just -f bench/justfile install-all

# run the full 6-broker matrix, 10 repeats, on both stacks:
RUNS=10 bash bench/run-matrix.sh 6broker-rf3

# aggregate into SUMMARY.md, CSVs, a standalone report.html, and the
# website charts fragment (per-run + averaged throughput/CPU/memory):
just -f bench/justfile bench-report
```
