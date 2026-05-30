+++
title = "Crabka vs Apache Kafka 4.3"
weight = 10
template = "docs/page.html"

[extra]
lead = "Single-box, like-for-like comparison: same host, same load driver, same wire protocol. With the consumer-interop gaps closed, Crabka now drives a full produce-and-consume round-trip against Kafka 4.3 — matching its throughput within a few percent while resident in ~40–50× less memory, starting 4–8× faster, and sustaining 1.1–1.3× more work per CPU core."
+++

This is a Kubernetes-free, single-machine comparison run with
[`bench/local/run-local-bench.sh`](https://github.com/robot-head/crabka/tree/main/bench/local).
Each scenario runs once per stack against a freshly-formatted single-node
broker, driven by the same Rust load driver (`crabka-bench-driver`) over
the Kafka wire protocol on `localhost:9092`. Broker CPU-seconds and peak
RSS are scraped from `/proc` and folded into the report.

Earlier runs could only compare the **producer** path: Crabka's consumer
client couldn't decode Kafka 4.3's `Fetch` response, so it read zero from
the JVM broker. That gap (and the cold-coordinator retry gaps alongside
it) is now fixed, so this run is a **full produce-and-consume round-trip
on both stacks** — Crabka's consumer reads every record it writes, against
its own broker *and* against Kafka.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10 GHz, **4 vCPU** |
| RAM | 15 GiB |
| Crabka | `crabka-broker` v0.1.2 @ commit `a3d2f1f`, release build |
| Kafka | **Apache Kafka 4.3.0** (KRaft combined mode), latest release |
| JVM | OpenJDK 21.0.10, default `-Xmx1G -Xms1G` heap |

Both the broker **and** the load driver share the same 4-vCPU box, so the
absolute throughput figures are laptop-class, not datacenter numbers, and
each cell is a single measurement run. The Crabka-vs-Kafka *comparison* is
apples-to-apples: identical load, identical host, identical driver,
brokers run one at a time.

## Produce-and-consume round-trip

Every record produced is also consumed back through the same driver, so
the producer and consumer columns move together. Higher is better except
for latency, memory, and startup.

| scenario (1 broker, RF=1, 6 partitions) | metric | Crabka | Kafka 4.3 | comparison |
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

On raw throughput the two stacks now trade blows within a few percent
(0.96–1.05×) — neither has a decisive write-path lead on this box. Where
Crabka pulls clearly ahead is everything around the throughput:

- **Memory:** resident in **19–24 MiB** versus Kafka's ~1 GiB — **43–53×
  lighter**. The JVM heap is fixed at `-Xms1G`, but even the live
  working set dwarfs Crabka's.
- **CPU efficiency:** **1.12–1.30× more messages per CPU-core**, so the
  near-parity throughput is delivered for noticeably less CPU.
- **Tail latency:** comparable at p99, but Crabka's p99.9 and max are
  consistently tighter — e.g. on `local-1kb-saturate`, producer p99.9
  **0.554 ms vs 1.345 ms** and max **3.2 ms vs 34.8 ms**; consumer p99.9
  **0.931 ms vs 1.935 ms**.
- **Startup:** ready in **1–2 s** versus Kafka's **8 s**, and first ack
  lands sooner.

> The "saturate" scenarios are latency-bound rather than bandwidth-bound:
> the driver awaits each send's ack before issuing the next per producer
> task, so a single task tops out around its round-trip rate. Both stacks
> are driven identically, so the ratio is still meaningful — it just
> isn't a raw MB/s ceiling. Absolute numbers also run lower than some
> earlier write-only reports because both stacks now carry a fully active
> consumer (previously Kafka's consumer read nothing) on the shared box.

## How low can the JVM go?

The memory gap above is measured against Kafka's default `-Xms1G -Xmx1G`
— is that just an unfairly fat default? To check, we reran
`local-1kb-saturate` against Kafka with progressively smaller heaps
(`-Xmx = -Xms`), same box, same driver. Crabka holds this exact workload
in **24 MiB**.

| Kafka heap | boots? | producer msgs/s | p99 ack | p99.9 ack | broker RSS | verdict |
|---|:--:|--:|--:|--:|--:|---|
| 1024 MiB (default) | ✅ | 8 215 | 0.49 ms | 1.3 ms | 1 031 MiB | competitive |
| 512 MiB | ✅ | 8 396 | 0.44 ms | 0.9 ms | 723 MiB | competitive |
| 256 MiB | ✅ | 8 380 | 0.45 ms | 2.5 ms | **489 MiB** | competitive, tail fraying |
| 224 MiB | ✅ | 8 061 | 0.49 ms | 4.6 ms | 457 MiB | borderline |
| 192 MiB | ⚠️ | 7 607 | **0.78 ms** | 6.4 ms | 442 MiB | runs, no longer competitive |
| ≤ 160 MiB | ❌ | — | — | — | — | OOM at startup |

- **Throughput survives down to ~256 MiB heap** (~490 MiB RSS) — Kafka's
  footprint isn't all default-heap fat; you can quarter the heap with
  little throughput loss.
- **Latency degrades before throughput does.** By 224–192 MiB, G1 pauses
  push p99 ack to ~2× Crabka's and worst-case ack to ~40 ms (Crabka stays
  ~3 ms), while broker CPU climbs ~20 % on identical work — that extra CPU
  is GC.
- **The hard floor is ~176–192 MiB just to boot.** At ≤160 MiB the KRaft
  broker dies during startup with `java.lang.OutOfMemoryError: Java heap
  space` in `LogManager` / `MetadataLoader`; it never serves a request.
- **Even squeezed to its minimum, the JVM is ~18–20× Crabka.** The minimum
  viable heap (~192–256 MiB) still resides in ~440–490 MiB, because RSS
  also carries the JVM's non-heap floor — metaspace, code cache, thread
  stacks, direct buffers — which alone is ~200 MiB, larger than Crabka's
  entire process. The gap is structural, not a tuning default.

## Can you even push Crabka to 256 MiB?

The natural escalation: scale the load up until *Crabka* needs 256 MiB,
then see what the JVM needs for the same load. On this 4-vCPU box that's a
trick question — **Crabka can't be driven to 256 MiB.** Its memory is a
small fixed base plus a few KiB per partition, so the load that would get
it there hits OS limits first:

- Each partition holds ~3 open files; the box's hard `ulimit -n` of 4096
  caps Crabka at **~1,200 partitions (~25 MiB RSS)** before it exhausts
  file descriptors. Kafka pays the same per-partition FD cost.
- Partitions cost Crabka **~3 KiB of RAM each** — reaching 256 MiB would
  take tens of thousands of them, far past the FD ceiling.

So we measured the inverse instead: take the heaviest workload the box
lets Crabka run — **1,024 partitions under saturating 1 KiB load, which
Crabka serves in 32.5 MiB at ~9.4k msgs/s** — and sweep Kafka's heap on
the *same* workload:

| Kafka heap | producer msgs/s | p99 ack | broker RSS | vs Crabka (33 MiB) |
|---|--:|--:|--:|--:|
| 1024 MiB | 10 391 | 1.42 ms | 1 131 MiB | **35×** |
| 512 MiB | 9 322 | 1.71 ms | 769 MiB | 24× |
| 384 MiB | 9 024 | 1.90 ms | 638 MiB | 20× |
| 256 MiB | 7 480 | 4.28 ms | 530 MiB | 16× |

Kafka stays competitive down to ~384–512 MiB heap (≈640–770 MiB RSS,
**~20–24× Crabka**); crushed to a 256 MiB heap it loses ~28 % throughput
and triples its tail latency, and *still* resides in 530 MiB — **~16×
Crabka for the identical workload.** Going from 6 to 1,024 partitions adds
~8 MiB to Crabka (24 → 33 MiB) but ~100 MiB to Kafka (1 031 → 1 131 MiB).

The question inverts: there's no workload on this box heavy enough to make
the Rust broker want 256 MiB, while the JVM needs ~256 MiB *just to boot*
and 2–4× that to host a partition-heavy load Crabka handles in tens of MiB.

## Methodology notes

- Crabka's broker is pinned to `RUST_LOG=warn` so per-request logging
  doesn't inflate its CPU.
- Broker CPU is the user+system delta over the measured window; memory is
  the process peak RSS (`VmHWM`).
- A freshly-started broker returns transient
  `COORDINATOR_LOAD_IN_PROGRESS` / `NOT_COORDINATOR` until its coordinators
  load. The harness still warms them symmetrically on both stacks with the
  JDK clients before measuring, so the measured window reflects broker
  steady state regardless of client retry behavior.

## Interop status

Pointing Crabka's own clients at a real Kafka broker previously surfaced
three client-side interop gaps. All three are now closed, which is what
makes the consumer comparison above possible:

- ✅ **Consumer decodes Kafka 4.3's `Fetch` response.** It read back
  every produced record from the JVM broker (281 k / 369 k / 412 k across
  the three scenarios) at equal-or-lower end-to-end p99 — no
  `records: body truncated` decode errors.
- ✅ **Idempotent producer retries the cold-coordinator errors.** The
  `acks=all` + idempotence `fixed-rate-latency` run completed cleanly
  against Kafka with zero producer errors.
- ✅ **Consumer locates the coordinator and retries `JoinGroup`.** The
  group joined and consumed on every stack without manual intervention.

One rough edge remains and is reported rather than hidden: in
`local-1kb-saturate` (two producers + two consumers in one group), one of
the two Crabka consumer tasks occasionally times out during build under
concurrent load (`request timed out after 30s`). The surviving group
member still consumed every produced record, so throughput was
unaffected, but the build-time concurrency is worth hardening.

## Reproduce

```bash
cargo build --release -p crabka-cli -p crabka-broker -p crabka-bench-driver
# unpack Apache Kafka 4.3.0, then:
KAFKA_HOME=/path/to/kafka_2.13-4.3.0 bench/local/run-local-bench.sh
# results + SUMMARY.md land in bench/local/results/
```
