+++
title = "Overview"
weight = 5
template = "docs/page.html"

[extra]
mermaid = true
+++

## What is Crabka

Crabka is a single-binary, native-Rust reimplementation of Apache Kafka. It
speaks the Apache Kafka wire protocol byte-for-byte (targeting the 4.3.0
message schemas), stores data in Kafka-compatible log segments, and runs its
metadata quorum on a native KRaft (Raft, KIP-595) implementation — no JVM and
no ZooKeeper anywhere in the stack. Existing Kafka clients and the official
`kafka-*.sh` admin tools talk to it unmodified.

## Why use it

If you run Kafka on Kubernetes today, you typically run the JVM broker plus
Strimzi to operate it and Cruise Control to balance it — three moving parts,
each with its own heap. Crabka collapses that into one Rust binary and ships
the operator and rebalancer in the same workspace.

- **About a tenth of the memory.** A Crabka broker's working set sits in the
  low hundreds of MiB (114–622 MiB measured) against a comparable Strimzi
  broker's 2.5–5.5 GiB. There is no garbage collector and no heap to tune.
- **Ready in 1–2 seconds.** Cold start to first ack is 1–2 s, versus 8–9 s for
  the JVM — no JVM warmup, no GC ramp. That makes restarts, rolls, and
  autoscaling cheap.
- **Kafka-class throughput.** Produce-and-consume matches or beats a
  like-for-like Strimzi cluster, winning multi-producer fan-out, at 1.6–3.0×
  the messages per CPU-core.
- **Byte-exact compatibility.** Every encode/decode is differentially tested
  against `kafka-clients` 4.3.0, and a JVM acceptance suite drives the official
  `cp-kafka` admin tools against a live broker. Your clients and tooling do not
  change.
- **Batteries included.** A Kubernetes operator, a Cruise-Control-equivalent
  rebalancer, native producer/consumer/admin clients, a Confluent-compatible
  schema registry, and tiered storage (KIP-405) all live in one project.

## How it fits together

A broker accepts client and admin-tool traffic on its Kafka listener, dispatches
each request to the right subsystem, and persists metadata through the KRaft
quorum. The operator, schema registry, rebalancer, and tiered storage attach
around that core rather than living inside the request path.

{% mermaid() %}
flowchart TD
  Clients[Clients and kafka-*.sh] --> Dispatch
  subgraph Broker
    Dispatch[Network dispatch] --> Group[Group coordinator]
    Dispatch --> Txn[Txn coordinator]
    Dispatch --> Log[Log segments]
    Log --> Repl[Replication / ISR]
  end
  Dispatch <--> KRaft[KRaft quorum]
  Repl <--> KRaft
  Log --> Tiered[Tiered storage]
  Registry[Schema registry] --> Dispatch
  Rebalancer[Rebalancer] --> Dispatch
  Operator[Operator] -.reconciles.-> Broker
{% end %}

## How do I run it

Locally, two commands stand up a single-node broker. First format a fresh data
directory as a standalone controller voter, then start the broker against it:

```bash
# 1. Format a data dir as the sole initial controller voter.
crabka format \
  --log-dir ./crabka-data \
  --standalone \
  --node-id 1 \
  --controller-listener 127.0.0.1:9093

# 2. Start the broker against that data dir.
crabka-broker \
  --log-dir ./crabka-data \
  --listen-addr 127.0.0.1:9092 \
  --broker-id 1
```

The broker now serves the Kafka protocol on `127.0.0.1:9092` and exposes
Prometheus metrics on `:9404`. Point any Kafka client or `kafka-topics.sh
--bootstrap-server 127.0.0.1:9092` at it.

For a real cluster, you do not run these commands by hand — the Kubernetes
operator formats, configures, and rolls brokers for you from a handful of CRDs.
See [Deploying the Operator](/guide/deploying-operator/).
