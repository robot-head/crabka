# Local Crabka vs Apache Kafka benchmark — results

Single-box, Kubernetes-free comparison run via
[`run-local-bench.sh`](./run-local-bench.sh). Each scenario was run once
per stack against a freshly-formatted single-node broker, driven by the
same Rust load driver (`crabka-bench-driver`) over the Kafka wire
protocol on `localhost:9092`.

The machine-readable per-run JSON and the auto-generated side-by-side
tables are in [`results/`](./results/) (`results/SUMMARY.md`). This file
is the human-written interpretation.

This run is the first **full produce-and-consume round-trip on both
stacks**: the consumer Fetch-decode and cold-coordinator interop gaps
that previously limited the comparison to the producer path are now
fixed, so Crabka's consumer reads every record it writes — against its
own broker *and* against Kafka 4.3.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10 GHz, **4 vCPU** |
| RAM | 15 GiB |
| Crabka | `crabka-broker` v0.1.2 @ commit `a3d2f1f`, release build |
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
| **small-msg-saturate** (100 B, acks=leader, 1P/1C) | producer msgs/s | 4 774 | 4 686 | 1.02× |
| | consumer msgs/s | 4 774 | 4 686 | 1.02× |
| | p99 producer ack | 0.338 ms | 0.346 ms | 1.02× lower |
| | p99 consumer e2e | 0.539 ms | 0.558 ms | 1.04× lower |
| | msgs/s per CPU-core | 3 464 | 2 796 | **1.24×** |
| | broker peak RSS | **19 MiB** | 1 019 MiB | **53× lighter** |
| **local-1kb-saturate** (1 KiB, acks=leader, 2P/2C) | producer msgs/s | 8 646 | 8 215 | 1.05× |
| | consumer msgs/s | 8 646 | 8 215 | 1.05× |
| | p99 producer ack | 0.419 ms | 0.493 ms | 1.18× lower |
| | p99 consumer e2e | 0.715 ms | 0.836 ms | 1.17× lower |
| | msgs/s per CPU-core | 5 009 | 3 863 | **1.30×** |
| | broker peak RSS | **24 MiB** | 1 031 MiB | **43× lighter** |
| **fixed-rate-latency** (1 KiB, acks=all + idempotence, 1P/1C) | producer msgs/s | 3 438 | 3 590 | 0.96× |
| | consumer msgs/s | 3 438 | 3 590 | 0.96× |
| | p99 producer ack | 0.387 ms | 0.390 ms | ~tied |
| | p99 consumer e2e | 0.496 ms | 0.520 ms | 1.05× lower |
| | msgs/s per CPU-core | 3 162 | 2 818 | **1.12×** |
| | broker peak RSS | **24 MiB** | 1 022 MiB | **43× lighter** |

Raw throughput is now within a few percent either way (0.96–1.05×):
neither stack has a decisive write- or read-path lead on this box. Crabka
wins clearly on the surrounding metrics:

- **Memory:** 19–24 MiB resident vs Kafka's ~1 GiB — **43–53× lighter**.
- **CPU efficiency:** **1.12–1.30× more msgs/s per CPU-core**, so parity
  throughput costs Crabka less CPU.
- **Tail latency:** comparable at p99, tighter at p99.9 / max — e.g.
  `local-1kb-saturate` producer p99.9 **0.554 ms vs 1.345 ms** and max
  **3.2 ms vs 34.8 ms**; consumer p99.9 **0.931 ms vs 1.935 ms**.
- **Startup:** ready in **1–2 s** vs Kafka's **8 s**.

> The "saturate" scenarios are effectively latency-bound, not bandwidth-
> bound: the driver awaits each send's ack before issuing the next per
> producer task, so a single producer task tops out around its
> round-trip rate. Both stacks are driven identically, so the ratio is
> still meaningful — it just isn't a raw MB/s ceiling. Absolute numbers
> also sit lower than earlier write-only runs because both stacks now
> carry a fully active consumer (previously Kafka's consumer read zero)
> on the shared box.

## Consumer (read) path

The driver's consumer is Crabka's own `crabka-client-consumer`. It now
reads back every produced record from **both** brokers:

- Against **crabka's broker**: 286 415 / 389 077 / 412 571 records across
  the three scenarios, sub-millisecond end-to-end p99 (0.50–0.72 ms).
- Against **Kafka 4.3**: 281 143 / 369 670 / 430 836 records, p99
  0.52–0.84 ms — and with no `Fetch`-decode errors.

This is the comparison that was impossible before the interop fixes.

## How low can the JVM go?

The memory gap is measured against Kafka's default `-Xms1G -Xmx1G`. To
check it isn't just a fat default, we reran `local-1kb-saturate` against
Kafka with shrinking heaps (`-Xmx = -Xms`), same box, same driver. Crabka
holds this workload in **24 MiB**.

| Kafka heap | boots? | producer msgs/s | p99 ack | p99.9 ack | broker RSS | verdict |
|---|:--:|--:|--:|--:|--:|---|
| 1024 MiB (default) | ✅ | 8 215 | 0.49 ms | 1.3 ms | 1 031 MiB | competitive |
| 512 MiB | ✅ | 8 396 | 0.44 ms | 0.9 ms | 723 MiB | competitive |
| 256 MiB | ✅ | 8 380 | 0.45 ms | 2.5 ms | 489 MiB | competitive, tail fraying |
| 224 MiB | ✅ | 8 061 | 0.49 ms | 4.6 ms | 457 MiB | borderline |
| 192 MiB | ⚠️ | 7 607 | 0.78 ms | 6.4 ms | 442 MiB | runs, not competitive |
| ≤ 160 MiB | ❌ | — | — | — | — | OOM at startup |

- Throughput survives to ~256 MiB heap (~490 MiB RSS); tail latency
  degrades earlier (224–192 MiB: p99 ~2× Crabka, max ~40 ms, +20 % CPU on
  GC).
- Hard floor ~176–192 MiB just to boot — at ≤160 MiB the KRaft broker
  OOMs during startup (`OutOfMemoryError: Java heap space` in
  `LogManager`/`MetadataLoader`) and never serves a request.
- Minimum viable JVM (~440–490 MiB RSS) is still ~18–20× Crabka's 24 MiB;
  the JVM's non-heap floor (metaspace, code cache, stacks, direct
  buffers) alone (~200 MiB) exceeds Crabka's whole process. The gap is
  structural.

## Can you even push Crabka to 256 MiB?

Trying to escalate load until *Crabka* needs 256 MiB shows it can't be
driven there on this box — it's a small fixed base plus ~3 KiB RAM per
partition, so it hits OS limits first:

- Each partition holds ~3 open files; the box's hard `ulimit -n` of 4096
  caps Crabka at ~1,200 partitions (~25 MiB RSS) before FD exhaustion.
- Reaching 256 MiB would need tens of thousands of partitions, far past
  that FD ceiling.

Measuring the inverse: take the heaviest workload the box allows —
**1,024 partitions, saturating 1 KiB load, which Crabka serves in
32.5 MiB at ~9.4k msgs/s** — and sweep Kafka's heap on the same scenario:

| Kafka heap | producer msgs/s | p99 ack | broker RSS | vs Crabka (33 MiB) |
|---|--:|--:|--:|--:|
| 1024 MiB | 10 391 | 1.42 ms | 1 131 MiB | 35× |
| 512 MiB | 9 322 | 1.71 ms | 769 MiB | 24× |
| 384 MiB | 9 024 | 1.90 ms | 638 MiB | 20× |
| 256 MiB | 7 480 | 4.28 ms | 530 MiB | 16× |

Kafka stays competitive to ~384–512 MiB heap (≈640–770 MiB RSS, ~20–24×
Crabka); at a 256 MiB heap it loses ~28 % throughput, triples p99, and
still resides in 530 MiB (~16× Crabka). The 6→1024-partition jump adds
~8 MiB to Crabka (24→33) but ~100 MiB to Kafka (1031→1131). There is no
box-feasible workload that makes the Rust broker want 256 MiB, while the
JVM needs ~256 MiB just to boot.

## Interop status (crabka *client* vs a real Kafka broker)

The three client-side gaps that earlier runs surfaced are now closed:

1. ✅ **Consumer decodes Kafka's Fetch response.** No more
   `records: body truncated` / `header too short`; the consumer drained
   every Kafka partition cleanly.
2. ✅ **Idempotent producer retries `COORDINATOR_LOAD_IN_PROGRESS`.** The
   `acks=all` + idempotence `fixed-rate-latency` run completed against
   Kafka with zero producer errors.
3. ✅ **Consumer locates the coordinator and retries `JoinGroup`.** The
   group joined and consumed on every stack without manual warm-up
   intervention.

The harness still performs a symmetric coordinator warm-up before the
measured window so the comparison reflects broker steady-state on both
stacks, but it is no longer papering over client bugs.

**Remaining rough edge:** in `local-1kb-saturate` (two producers + two
consumers in one group), one of the two crabka consumer tasks still
occasionally times out during build under concurrent load
(`consumer-0-build: request timed out after 30s`). The surviving group
member consumed every produced record, so throughput was unaffected, but
the build-time concurrency is worth hardening.

## Reproduce

```bash
cargo build --release -p crabka-cli -p crabka-broker -p crabka-bench-driver
# unpack Apache Kafka 4.3.0 somewhere, then:
KAFKA_HOME=/path/to/kafka_2.13-4.3.0 bench/local/run-local-bench.sh
# results land in bench/local/results/ (+ SUMMARY.md)
```
