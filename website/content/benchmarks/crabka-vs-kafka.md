+++
title = "Crabka vs Apache Kafka 4.3"
weight = 10
template = "docs/page.html"

[extra]
lead = "Single-box, like-for-like comparison: same host, same load driver, same wire protocol. Crabka drives a full produce-and-consume round-trip against Apache Kafka 4.3 — matching its throughput within a few percent (at or ahead on the 1 KiB workloads) while resident in 30–40× less memory, sustaining 1.15–1.2× more work per CPU core, and starting in 1–2 s instead of 8–9 s."
+++

This is a Kubernetes-free, single-machine comparison run with
[`bench/local/run-local-bench.sh`](https://github.com/robot-head/crabka/tree/main/bench/local).
Each scenario runs once per stack against a freshly-formatted single-node
broker, driven by the same Rust load driver (`crabka-bench-driver`) over the
Kafka wire protocol on `localhost:9092`. Broker CPU-seconds and peak RSS are
scraped from `/proc` and folded into the report. Every record produced is also
consumed back through the same driver, so the producer and consumer columns
track each other.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10 GHz, **4 vCPU** |
| RAM | 15 GiB |
| Crabka | `crabka-broker` v0.2.0 @ commit `cdbb379`, release build |
| Kafka | **Apache Kafka 4.3.0** (KRaft combined mode), latest release |
| JVM | OpenJDK 21.0.10, default `-Xmx1G -Xms1G` heap |

Both the broker **and** the load driver share the same 4-vCPU box, so the
absolute throughput figures are laptop-class, not datacenter numbers, and each
cell is a single measurement run. The Crabka-vs-Kafka *comparison* is
apples-to-apples: identical load, identical host, identical driver, brokers run
one at a time. Crabka's broker is pinned to `RUST_LOG=warn` so per-request
logging doesn't inflate its CPU.

## Produce-and-consume round-trip

Every record produced is also consumed back through the same driver, so the
producer and consumer columns move together. Higher is better except for
latency, memory, and startup.

| scenario (1 broker, RF=1, 6 partitions) | metric | Crabka | Kafka 4.3 | comparison |
|---|---|--:|--:|--:|
| **small-msg-saturate** (100 B, acks=leader, 1P/1C) | producer msgs/s | 5 549 | 5 892 | 0.94× |
| | consumer msgs/s | 5 549 | 5 892 | 0.94× |
| | p99 producer ack | 0.552 ms | 0.479 ms | Kafka 1.15× lower |
| | p99 consumer e2e | 0.873 ms | 0.812 ms | Kafka 1.08× lower |
| | msgs/s per CPU-core | 4 114 | 3 528 | **1.17×** |
| | broker peak RSS | **24 MiB** | 1 027 MiB | **43× lighter** |
| **local-1kb-saturate** (1 KiB, acks=leader, 2P/2C) | producer msgs/s | 11 253 | 10 924 | 1.03× |
| | consumer msgs/s | 11 253 | 10 924 | 1.03× |
| | p99 producer ack | 0.367 ms | 0.448 ms | 1.22× lower |
| | p99 consumer e2e | 0.639 ms | 0.785 ms | 1.23× lower |
| | msgs/s per CPU-core | 6 631 | 5 670 | **1.17×** |
| | broker peak RSS | **32 MiB** | 1 040 MiB | **32× lighter** |
| **fixed-rate-latency** (1 KiB, acks=all + idempotence, 1P/1C) | producer msgs/s | 4 289 | 4 232 | 1.01× |
| | consumer msgs/s | 4 289 | 4 232 | 1.01× |
| | p99 producer ack | 0.441 ms | 0.477 ms | 1.08× lower |
| | p99 consumer e2e | 0.565 ms | 0.622 ms | 1.10× lower |
| | msgs/s per CPU-core | 3 943 | 3 387 | **1.16×** |
| | broker peak RSS | **32 MiB** | 1 039 MiB | **32× lighter** |

On raw throughput the two stacks trade blows within a few percent: Crabka is
ahead on both 1 KiB workloads (1.01–1.03×) and a touch behind on the 100 B
saturation run (0.94×). Where Crabka pulls clearly ahead is everything around
the throughput:

- **Memory:** resident in **24–32 MiB** versus Kafka's ~1 GiB — **32–43×
  lighter**. The JVM heap is fixed at `-Xms1G`, but even the live working set
  dwarfs Crabka's.
- **CPU efficiency:** **1.16–1.17× more messages per CPU-core** in every
  scenario, so equal-or-better throughput is delivered for noticeably less CPU.
- **Tail latency:** comparable at p99 on the small-message run and tighter on
  both 1 KiB runs; Crabka's p99.9 and max are consistently lower — e.g. on
  `local-1kb-saturate`, producer p99.9 **0.933 ms vs 1.717 ms** and max
  **11.2 ms vs 37.8 ms**; on `fixed-rate-latency`, max **19.2 ms vs 42.5 ms**.
- **Startup:** ready in **1–2 s** versus Kafka's **8–9 s**, and first ack lands
  sooner.

> The "saturate" scenarios are latency-bound rather than bandwidth-bound: the
> driver awaits each send's ack before issuing the next per producer task, so a
> single task tops out around its round-trip rate. Both stacks are driven
> identically, so the ratio is still meaningful — it just isn't a raw MB/s
> ceiling.

## How low can the JVM go?

The memory gap above is measured against Kafka's default `-Xms1G -Xmx1G` — is
that just an unfairly fat default? To check, we reran the 1 KiB saturation
workload against Kafka with progressively smaller heaps (`-Xmx = -Xms`), same
box, same driver. Crabka holds this workload in **~32 MiB**.

| Kafka heap | boots? | producer msgs/s | p99 ack | p99.9 ack | broker RSS | verdict |
|---|:--:|--:|--:|--:|--:|---|
| 1024 MiB (default) | ✅ | 12 988 | 0.287 ms | 1.17 ms | 1 011 MiB | competitive |
| 512 MiB | ✅ | 13 758 | 0.258 ms | 1.14 ms | 694 MiB | competitive |
| 256 MiB | ✅ | 13 645 | 0.272 ms | 1.59 ms | **463 MiB** | competitive |
| 224 MiB | ✅ | 13 595 | 0.265 ms | 2.14 ms | 422 MiB | competitive, tail fraying |
| 192 MiB | ✅ | 12 790 | 0.322 ms | 3.19 ms | 397 MiB | runs, tail degraded |
| ≤ 160 MiB | ❌ | — | — | — | — | OOM at startup |

- **Throughput survives down to a ~192 MiB heap** (~397 MiB RSS): Kafka's
  footprint isn't all default-heap fat — you can shrink the heap dramatically
  with little throughput loss.
- **Latency degrades before throughput does.** From 224 MiB down, G1 pauses
  push the worst-case ack out (p99.9 climbs from ~1 ms to ~3 ms) while broker
  CPU rises on identical work — that extra CPU is GC.
- **The hard floor is ~176–192 MiB just to boot.** At ≤160 MiB the KRaft broker
  dies during startup with `java.lang.OutOfMemoryError: Java heap space` in
  `LogManager` / `MetadataLoader`; it never serves a request.
- **Even squeezed to its minimum, the JVM is ~12× Crabka.** The minimum viable
  heap (~192 MiB) still resides in ~397 MiB, because RSS also carries the JVM's
  non-heap floor — metaspace, code cache, thread stacks, direct buffers — which
  alone exceeds Crabka's entire 32 MiB process. The gap is structural, not a
  tuning default.

## Methodology notes

- Crabka's broker is pinned to `RUST_LOG=warn` so per-request logging doesn't
  inflate its CPU.
- Broker CPU is the user+system delta over the measured window; memory is the
  process peak RSS (`VmHWM`).
- A freshly-started broker returns transient `COORDINATOR_LOAD_IN_PROGRESS` /
  `NOT_COORDINATOR` until its coordinators load. The harness warms them
  symmetrically on both stacks with the JDK clients before measuring, so the
  measured window reflects broker steady state regardless of client retry
  behavior.

## Interop with the JVM

The load driver is built on Crabka's own `crabka-client-producer` /
`crabka-client-consumer` crates and runs unmodified against either broker. Its
consumer decodes Kafka 4.3's `Fetch` response and drains every produced record
from the JVM broker; the idempotent / `acks=all` producer completes cleanly
against Kafka with zero producer errors; and the consumer locates the
coordinator and joins the group on every stack without manual intervention.
Beyond the driver, Crabka is validated against the JVM via differential
byte-equality tests on every encode/decode and a JVM acceptance suite that
drives the official `kafka-*.sh` admin tools against a live Crabka broker.

## Reproduce

```bash
cargo build --release -p crabka-cli -p crabka-broker -p crabka-bench-driver
# unpack Apache Kafka 4.3.0, then:
KAFKA_HOME=/path/to/kafka_2.13-4.3.0 bench/local/run-local-bench.sh
# results + SUMMARY.md land in bench/local/results/
```
