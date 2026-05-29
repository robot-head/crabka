# Local Crabka vs Apache Kafka benchmark — results

Single-box, Kubernetes-free comparison run via
[`run-local-bench.sh`](./run-local-bench.sh). Each scenario was run once
per stack against a freshly-formatted single-node broker, driven by the
same Rust load driver (`crabka-bench-driver`) over the Kafka wire
protocol on `localhost:9092`.

The machine-readable per-run JSON and the auto-generated side-by-side
tables are in [`results/`](./results/) (`results/SUMMARY.md`). This file
is the human-written interpretation.

## Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10 GHz, **4 vCPU** |
| RAM | 15 GiB |
| Crabka | `crabka-broker` @ commit `e3ddea0` (v0.1.1), release build |
| Kafka | **Apache Kafka 4.3.0** (KRaft combined mode), the latest release |
| JVM | OpenJDK 21.0.10, default `-Xmx1G -Xms1G` heap |
| Driver | `crabka-bench-driver`, 1 measurement run per cell |

Both the broker **and** the load driver share the same 4 vCPU box, so the
absolute throughput ceilings are "laptop-class single host", not
datacenter numbers. The crabka-vs-kafka *comparison* is apples-to-apples:
identical load, identical host, identical driver, brokers run one at a
time. Crabka's broker is pinned to `RUST_LOG=warn` so per-request logging
doesn't skew its CPU.

## Headline: producer (write) path

Fully comparable across both brokers. Higher is better except latency /
memory / startup.

| scenario (1 broker, RF=1, 6 partitions) | metric | crabka | kafka 4.3 | crabka advantage |
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

Across all three scenarios crabka sustained **1.3–1.5× the producer
throughput** at **lower p99 ack latency** while resident in **19–26 MiB**
versus Kafka's ~1 GiB (the JVM heap is fixed at `-Xms1G`, but even the
working set dwarfs crabka). Cold start to first ready: crabka **1–2 s** vs
Kafka **8–9 s**.

> The "saturate" scenarios are effectively latency-bound, not bandwidth-
> bound: the driver awaits each send's ack before issuing the next per
> producer task, so a single producer task tops out around its
> round-trip rate. Both stacks are driven identically, so the ratio is
> still meaningful — it just isn't a raw MB/s ceiling.

## Consumer (read) path — crabka-only

The driver's consumer is crabka's own `crabka-client-consumer`. Against
**crabka's broker** it consumed every produced record with sub-millisecond
end-to-end p99 (0.40–0.53 ms). Against **Kafka 4.3 it consumed zero**, so
no consumer comparison is possible. Root cause is a real wire-decode bug
in crabka's consumer client (see below), not a broker difference.

## Honest interop findings (crabka *client* gaps vs a real Kafka broker)

Running crabka's clients against Apache Kafka surfaced three genuine
client-side gaps. None of them are broker-performance issues; they're
worth filing.

1. **Consumer can't decode Kafka's Fetch response.**
   `consumer-0-poll: client: codec: invalid value: records: body
   truncated` (and `records: header too short`). After the consumer joins
   the group and fetches, crabka's record-batch decoder rejects the bytes
   Kafka 4.3 returns. This is the blocker for any consumer comparison and
   the most important finding.

2. **Idempotent producer doesn't retry `COORDINATOR_LOAD_IN_PROGRESS`.**
   The first `InitProducerId` against a cold Kafka broker returns error
   code **14**; crabka's producer fails the build instead of retrying.
   Only bites acks=all / idempotent producers.

3. **Consumer issues `JoinGroup` with no `FindCoordinator` and no retry.**
   The first `JoinGroup` against a cold broker returns **16
   `NOT_COORDINATOR`**; crabka fails the build rather than locating the
   coordinator and retrying.

Findings 2 and 3 are cold-start-only: crabka's own broker has its
coordinators ready instantly, so they never surface in a crabka-vs-crabka
run. The harness works around them with a symmetric coordinator warm-up
(produce one idempotent record + load the group via the JDK clients)
before the measured window, so the *broker* producer comparison above is
clean. Finding 1 is unconditional and can't be warmed away.

There was also one crabka-vs-crabka hiccup: in `local-1kb-saturate`, one
of the two crabka consumer tasks timed out during build (`request timed
out after 30s`) under concurrent load; the surviving group member still
consumed every record, so throughput was unaffected.

## Reproduce

```bash
cargo build --release -p crabka-cli -p crabka-broker -p crabka-bench-driver
# unpack Apache Kafka 4.3.0 somewhere, then:
KAFKA_HOME=/path/to/kafka_2.13-4.3.0 bench/local/run-local-bench.sh
# results land in bench/local/results/ (+ SUMMARY.md)
```
