+++
title = "Crabka vs Apache Kafka 4.3"
weight = 10
template = "docs/page.html"

[extra]
lead = "Single-box, like-for-like comparison: same host, same load driver, same wire protocol — Crabka sustains 1.3–1.5× the producer throughput at ~40× less memory."
+++

This is a Kubernetes-free, single-machine comparison run with
[`bench/local/run-local-bench.sh`](https://github.com/robot-head/crabka/tree/main/bench/local).
Each scenario runs once per stack against a freshly-formatted single-node
broker, driven by the same Rust load driver (`crabka-bench-driver`) over
the Kafka wire protocol on `localhost:9092`. Broker CPU-seconds and peak
RSS are scraped from `/proc` and folded into the report.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10 GHz, **4 vCPU** |
| RAM | 15 GiB |
| Crabka | `crabka-broker` v0.1.1, release build |
| Kafka | **Apache Kafka 4.3.0** (KRaft combined mode), latest release |
| JVM | OpenJDK 21, default `-Xmx1G -Xms1G` heap |

Both the broker **and** the load driver share the same 4-vCPU box, so the
absolute throughput figures are laptop-class, not datacenter numbers. The
Crabka-vs-Kafka *comparison* is apples-to-apples: identical load,
identical host, identical driver, brokers run one at a time.

## Producer (write) path

Fully comparable across both brokers. Higher is better except for latency,
memory, and startup.

| scenario (1 broker, RF=1, 6 partitions) | metric | Crabka | Kafka 4.3 | Crabka advantage |
|---|---|--:|--:|--:|
| **small-msg-saturate** (100 B, acks=leader) | producer msgs/s | 9 628 | 6 261 | **1.54×** |
| | p99 ack latency | 0.327 ms | 0.443 ms | 1.35× lower |
| | broker peak RSS | **19 MiB** | 1 047 MiB | **54× lighter** |
| | msgs/s per CPU-core | 10 069 | 6 882 | 1.46× |
| **local-1kb-saturate** (1 KiB, acks=leader, 2P/2C) | producer msgs/s | 16 267 | 12 449 | **1.31×** |
| | p99 ack latency | 0.321 ms | 0.386 ms | 1.20× lower |
| | broker peak RSS | **26 MiB** | 1 025 MiB | **39× lighter** |
| | msgs/s per CPU-core | 11 368 | 7 321 | 1.55× |
| **fixed-rate-latency** (1 KiB, acks=all + idempotence) | producer msgs/s | 5 831 | 4 254 | **1.37×** |
| | p99 ack latency | 0.471 ms | 0.603 ms | 1.28× lower |
| | broker peak RSS | **26 MiB** | 1 023 MiB | **39× lighter** |

Across all three scenarios Crabka sustained **1.3–1.5× the producer
throughput** at **lower p99 ack latency**, resident in **19–26 MiB**
versus Kafka's ~1 GiB, and reached ready in **1–2 s** versus **8–9 s**.

> The "saturate" scenarios are latency-bound rather than bandwidth-bound:
> the driver awaits each send's ack before issuing the next per producer
> task, so a single task tops out around its round-trip rate. Both stacks
> are driven identically, so the ratio is still meaningful — it just
> isn't a raw MB/s ceiling.

## Methodology notes

- Crabka's broker is pinned to `RUST_LOG=warn` so per-request logging
  doesn't inflate its CPU.
- Broker CPU is the user+system delta over the measured window; memory is
  the process peak RSS (`VmHWM`).
- A freshly-started broker returns transient
  `COORDINATOR_LOAD_IN_PROGRESS` / `NOT_COORDINATOR` until its coordinators
  load; the harness warms them symmetrically on both stacks with the JDK
  clients before measuring, so the producer comparison reflects broker
  steady state.

## Honest gaps

Crabka is a young project, and pointing its own clients at a real Kafka
broker surfaced genuine *client-side* interop gaps (none are broker
performance issues — they are tracked separately):

- **The consumer can't yet decode Kafka 4.3's Fetch response**
  (`codec: invalid value: records: body truncated`). Crabka reads from its
  own broker fine at sub-millisecond p99, but reads zero from Kafka — so
  there is no consumer-side comparison here.
- The idempotent producer and the consumer do not retry the transient
  cold-coordinator errors a production Kafka client retries through; the
  harness warms past them.

These are reported rather than hidden: the comparison above is the
producer write path, which is clean on both stacks.

## Reproduce

```bash
cargo build --release -p crabka-cli -p crabka-broker -p crabka-bench-driver
# unpack Apache Kafka 4.3.0, then:
KAFKA_HOME=/path/to/kafka_2.13-4.3.0 bench/local/run-local-bench.sh
# results + SUMMARY.md land in bench/local/results/
```
