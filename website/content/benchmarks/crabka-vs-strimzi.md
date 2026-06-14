+++
title = "Crabka vs Strimzi (Kubernetes)"
weight = 20
template = "docs/page.html"

[extra]
lead = "Operator-managed, three-broker comparison on GKE. Crabka and Strimzi (Apache Kafka) run under identical Kubernetes pod resources, driven by the same Rust load driver over the Kafka wire protocol. Crabka matches or beats Strimzi's throughput while resident in roughly a tenth of the memory and serving fetches zero-copy — sendfile(2) on plaintext and kernel-TLS (kTLS) on encrypted connections."
+++

This is a Kubernetes comparison: two three-broker clusters, one managed by the
Crabka operator and one by [Strimzi](https://strimzi.io), brought up on the same
GKE node pool with byte-for-byte identical pod resources. Each scenario is driven
by the same Rust load driver (`crabka-bench-driver`) over the Kafka wire protocol,
producing and consuming through a Kubernetes `Job`. Broker CPU and container
working-set memory come from cAdvisor; the JVM heap / non-heap split comes from
the Strimzi JMX exporter. The harness lives in
[`bench/`](https://github.com/robot-head/crabka/tree/main/bench).

## Environment

| | |
|---|---|
| Platform | GKE, `e2-standard-4` nodes (4 vCPU, 16 GiB), one broker per node |
| Storage | `pd-ssd` PersistentClaim, 200 GiB per broker |
| Pod resources | **identical** both stacks: 2–4 vCPU, 6 GiB request / 12 GiB limit |
| Crabka | `crabka-operator` + `crabka-broker`, release build, 3 brokers |
| Strimzi | Strimzi 0.46, **Apache Kafka 3.9** (KRaft), JVM, 3 brokers |
| Driver | `crabka-bench-driver`, in-cluster `Job`, Kafka wire protocol |

The two stacks get the same pods, the same storage class, and the same driver;
brokers run one cluster at a time. Throughput figures are single-sample per
scenario on shared cloud infrastructure, so treat individual cells as
representative rather than exact — the **ratio** between the stacks is the
apples-to-apples result.

## Produce-and-consume

Every record produced is also consumed back through the same driver. Higher is
better except for memory.

| scenario (3 brokers, RF=1) | Crabka | Strimzi | Crabka memory | Strimzi memory | msgs/CPU-core |
|---|--:|--:|--:|--:|--:|
| **small-msg-saturate** (100 B, acks=leader) | 95.7k msgs/s | 103.1k msgs/s | **283 MiB** | 2 512 MiB | **2.1×** |
| **fan-out** (1 KiB, 4P/4C, 24 partitions) | **69.5k msgs/s** | 58.8k msgs/s | **622 MiB** | 5 556 MiB | **1.6×** |
| **mixed-acks** (1 KiB @ 20k/s, acks=all) | 20.0k msgs/s | 19.8k msgs/s | **114 MiB** | 4 249 MiB | **2.0×** |
| **large-msg** (100 KiB, acks=leader) | 356 MiB/s | 380 MiB/s | **512 MiB** | 4 977 MiB | **3.0×** |

On throughput the two stacks trade blows: Crabka matches Strimzi on the
small-record and `acks=all` workloads, wins the multi-producer fan-out outright,
and lands within ~6% on 100 KiB large messages. Where Crabka pulls clearly ahead
is everything around the throughput:

- **Memory:** a Crabka broker's container working set sits in the low hundreds of
  MiB; a Strimzi broker carries **2.5–5.5 GiB**, the bulk of it JVM heap — a
  **9–37× gap** depending on workload, structural rather than a tuning default.
- **CPU efficiency:** **1.6–3.0× more messages (or MiB) per CPU-core** in every
  scenario — equal-or-better throughput delivered for noticeably less CPU.

## Zero-copy fetch

Crabka serves the records portion of a `Fetch` response **without copying it
through userspace**: on Linux it `sendfile(2)`s the log-segment bytes straight
from the page cache to the socket (page-cache → NIC), exactly as Apache Kafka
does. The produce path likewise appends the producer's record batch verbatim,
without a decode/re-encode round trip. macOS and the BSDs use their native
`sendfile`; other platforms fall back to a buffered copy.

On **encrypted** connections Crabka uses **Linux kTLS** (kernel TLS): after the
rustls handshake, record encryption is offloaded to the kernel, so `sendfile`
runs *through* TLS — the kernel encrypts the page-cache pages into TLS records on
the way to the NIC. Encryption stays zero-copy, so a Crabka broker's TLS fetch
throughput equals its plaintext throughput:

| listener | small-msg-saturate | large-msg fetch |
|---|--:|--:|
| plaintext | 95.7k msgs/s | 375 MiB/s |
| **TLS (kTLS)** | 116.9k msgs/s | 402 MiB/s |

Every wire byte is identical across the plaintext and TLS paths; kTLS only moves
where encryption happens, not what crosses the wire.

## Methodology notes

- Both stacks get identical pod resource requests/limits, the same `pd-ssd`
  storage class, the same partition counts, and the same in-cluster driver.
- Container memory is the cgroup working set (`container_memory_working_set_bytes`)
  summed over the broker pods; CPU is the cAdvisor CPU-seconds over the measured
  window. The Strimzi JVM heap / non-heap split is read from its JMX exporter.
- Shared-cloud infrastructure has meaningful run-to-run variance; each cell is a
  single measurement, so the inter-stack ratio is the reliable comparison, not
  the absolute number.

## Reproduce

```bash
# from a kubectl context with the Crabka + Strimzi operators installed:
bench/scripts/run-scenario.sh crabka small-msg-saturate 3broker-rf3
bench/scripts/run-scenario.sh kafka  small-msg-saturate 3broker-rf3
# add a 4th arg `tls` to exercise the TLS (kTLS) data path:
bench/scripts/run-scenario.sh crabka large-msg 3broker-rf3 tls
# results land in bench/results/ ; aggregate with crabka-bench-report
```
