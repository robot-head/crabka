# Local Crabka vs Apache Kafka benchmark — results

Single-box, Kubernetes-free comparison run via
[`run-local-bench.sh`](./run-local-bench.sh). Each scenario was run once
per stack against a freshly-formatted single-node broker, driven by the
same Rust load driver (`crabka-bench-driver`) over the Kafka wire
protocol on `localhost:9092`. Every record produced is also consumed back
through the same driver, against Crabka's own broker *and* against Kafka 4.3.

The machine-readable per-run JSON and the auto-generated side-by-side
tables are in [`results/`](./results/) (`results/SUMMARY.md`). This file
is the human-written interpretation.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10 GHz, **4 vCPU** |
| RAM | 15 GiB |
| Crabka | `crabka-broker` v0.2.0 @ commit `cdbb379`, release build |
| Kafka | **Apache Kafka 4.3.0** (KRaft combined mode), the latest release |
| JVM | OpenJDK 21.0.10, default `-Xmx1G -Xms1G` heap |
| Driver | `crabka-bench-driver`, 1 measurement run per cell |

Both the broker **and** the load driver share the same 4 vCPU box, so the
absolute throughput ceilings are "laptop-class single host", not
datacenter numbers. The crabka-vs-kafka *comparison* is apples-to-apples:
identical load, identical host, identical driver, brokers run one at a
time. Crabka's broker is pinned to `RUST_LOG=warn` so per-request logging
doesn't skew its CPU.

## Headline: produce-and-consume round-trip

Every record produced is consumed back through the same driver, so the
producer and consumer columns track each other. Higher is better except
latency / memory / startup.

| scenario (1 broker, RF=1, 6 partitions) | metric | crabka | kafka 4.3 | comparison |
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

Raw throughput is within a few percent either way — Crabka is ahead on both
1 KiB workloads (1.01–1.03×) and a touch behind on 100 B saturation (0.94×).
Crabka wins clearly on the surrounding metrics:

- **Memory:** 24–32 MiB resident vs Kafka's ~1 GiB — **32–43× lighter**.
- **CPU efficiency:** **1.16–1.17× more msgs/s per CPU-core** in every
  scenario, so equal-or-better throughput costs Crabka less CPU.
- **Tail latency:** comparable at p99 on 100 B, tighter on both 1 KiB runs,
  and consistently tighter at p99.9 / max — e.g. `local-1kb-saturate`
  producer p99.9 **0.933 ms vs 1.717 ms** and max **11.2 ms vs 37.8 ms**;
  `fixed-rate-latency` max **19.2 ms vs 42.5 ms**.
- **Startup:** ready in **1–2 s** vs Kafka's **8–9 s**.

> The "saturate" scenarios are latency-bound, not bandwidth-bound: the driver
> awaits each send's ack before issuing the next per producer task, so a single
> producer task tops out around its round-trip rate. Both stacks are driven
> identically, so the ratio is still meaningful — it just isn't a raw MB/s
> ceiling.

## How low can the JVM go?

The memory gap is measured against Kafka's default `-Xms1G -Xmx1G`. To check it
isn't just a fat default, we reran the 1 KiB workload against Kafka with
shrinking heaps (`-Xmx = -Xms`), same box, same driver. Crabka holds this
workload in **~32 MiB**.

| Kafka heap | boots? | producer msgs/s | p99 ack | p99.9 ack | broker RSS | verdict |
|---|:--:|--:|--:|--:|--:|---|
| 1024 MiB (default) | ✅ | 12 988 | 0.29 ms | 1.17 ms | 1 011 MiB | competitive |
| 512 MiB | ✅ | 13 758 | 0.26 ms | 1.14 ms | 694 MiB | competitive |
| 256 MiB | ✅ | 13 645 | 0.27 ms | 1.59 ms | 463 MiB | competitive |
| 224 MiB | ✅ | 13 595 | 0.27 ms | 2.14 ms | 422 MiB | competitive, tail fraying |
| 192 MiB | ✅ | 12 790 | 0.32 ms | 3.19 ms | 397 MiB | runs, tail degraded |
| ≤ 160 MiB | ❌ | — | — | — | — | OOM at startup |

- Throughput survives to a ~192 MiB heap (~397 MiB RSS); tail latency degrades
  earlier (p99.9 climbs from ~1 ms to ~3 ms as G1 pauses grow).
- Hard floor ~176–192 MiB just to boot — at ≤160 MiB the KRaft broker OOMs
  during startup (`OutOfMemoryError: Java heap space` in
  `LogManager`/`MetadataLoader`) and never serves a request.
- Minimum viable JVM (~397 MiB RSS) is still ~12× Crabka's 32 MiB; the JVM's
  non-heap floor (metaspace, code cache, stacks, direct buffers) alone exceeds
  Crabka's whole process. The gap is structural.

## Reproduce

```bash
cargo build --release -p crabka-cli -p crabka-broker -p crabka-bench-driver
# unpack Apache Kafka 4.3.0 somewhere, then:
KAFKA_HOME=/path/to/kafka_2.13-4.3.0 bench/local/run-local-bench.sh
# results land in bench/local/results/ (+ SUMMARY.md)
```
