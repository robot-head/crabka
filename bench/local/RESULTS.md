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
