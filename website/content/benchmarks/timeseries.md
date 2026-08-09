+++
title = "Throughput, CPU & memory over time"
description = "Interactive per-run and averaged time series from the six-broker matrix: producer throughput, broker CPU, and working-set memory, Crabka against Strimzi."
weight = 30
template = "docs/page.html"

[extra]
lead = "Interactive per-run and averaged time series from the six-broker matrix: producer throughput, broker CPU and broker working-set memory over each run window, Crabka against Strimzi. Every thin line is one run; the bold line is the across-run mean."
+++

These charts come from the same harness as the
[Crabka vs Strimzi comparison](@/benchmarks/crabka-vs-strimzi.md). Two
six-broker clusters at RF=3 run on one GKE node pool with byte-for-byte
identical pod resources. The same Rust load driver, `crabka-bench-driver`, runs
each scenario over the Kafka wire protocol. The harness runs each cell **ten
times**. The driver samples client throughput and latency every two seconds, and
it scrapes broker CPU and working-set memory from Prometheus across the run
window.

## How to read these

Each scenario has three charts: **throughput**, **broker CPU**, and **broker
memory**. Each chart plots the value over the run window.

- **Thin lines** are individual runs, one line per repeat. They show the
  run-to-run spread directly instead of one number.
- **Bold lines** are the mean across all runs at each time offset.
- **Orange is Crabka, blue is Strimzi.** Click a legend entry to toggle a stack.

The summary bars at the top are the end-of-run aggregates. The bar is the mean
across runs. The error bar is the run-to-run sample standard deviation, so a
short error bar shows a stable and repeatable result.

{{ benchmark_charts() }}

## Methodology

- Both stacks get identical pod resource requests/limits, the same storage
  class, the same partition counts, and the same in-cluster driver; brokers run
  one cluster at a time.
- Throughput is the driver's own produce/consume rate. Broker CPU is the summed
  cAdvisor CPU usage in cores across the broker pods. Broker memory is the
  summed cgroup working set (`container_memory_working_set_bytes`).
- The means and the standard deviations come from the ten repeats per cell. The
  charts average the time series per two-second offset, so a curve shows the
  typical shape of a run and not one single run.
- The full harness, the scenarios, and the aggregator are in
  [`bench/`](https://github.com/robot-head/crabka/tree/main/bench). Use
  `crabka-bench-report --web-fragment` to regenerate the charts on this page.
