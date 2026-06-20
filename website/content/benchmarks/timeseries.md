+++
title = "Throughput, CPU & memory over time"
description = "Interactive per-run and averaged time series from the six-broker matrix: producer throughput, broker CPU, and working-set memory, Crabka against Strimzi."
weight = 30
template = "docs/page.html"

[extra]
lead = "Interactive per-run and averaged time series from the six-broker matrix: producer throughput, broker CPU and broker working-set memory over each run window, Crabka against Strimzi. Every thin line is one run; the bold line is the across-run mean."
+++

These charts come from the same harness as the
[Crabka vs Strimzi comparison](@/benchmarks/crabka-vs-strimzi.md): two
six-broker clusters (RF=3) on one GKE node pool with byte-for-byte identical pod
resources, each scenario driven by the same Rust load driver
(`crabka-bench-driver`) over the Kafka wire protocol. Each cell is run **ten
times**; the driver samples client throughput and latency every two seconds and
scrapes broker CPU / working-set memory from Prometheus across the run window.

## How to read these

For each scenario there are three charts — **throughput**, **broker CPU** and
**broker memory** — each plotting the value over the run window:

- **Thin lines** are individual runs (one per repeat), so you can see run-to-run
  spread directly rather than trusting a single number.
- **Bold lines** are the mean across all runs at each time offset.
- **Orange is Crabka, blue is Strimzi.** Click a legend entry to toggle a stack.

The summary bars at the top are the end-of-run aggregates: the bar is the mean
across runs and the error bar is the run-to-run sample standard deviation, so a
short error bar means a stable, repeatable result.

{{ benchmark_charts() }}

## Methodology

- Both stacks get identical pod resource requests/limits, the same storage
  class, the same partition counts, and the same in-cluster driver; brokers run
  one cluster at a time.
- Throughput is the driver's own produce/consume rate. Broker CPU is summed
  cAdvisor CPU usage (cores) across the broker pods; broker memory is the summed
  cgroup working set (`container_memory_working_set_bytes`).
- Means and standard deviations are computed across the ten repeats per cell;
  time-series means are averaged per two-second offset, so a curve reflects the
  typical shape of a run rather than any single one.
- The full harness, scenarios and aggregator live in
  [`bench/`](https://github.com/robot-head/crabka/tree/main/bench); the charts on
  this page are regenerated with `crabka-bench-report --web-fragment`.
