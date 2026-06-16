+++
title = "Quickstart"
weight = 20
template = "docs/page.html"
+++

This quickstart runs a single local Crabka broker and verifies it with standard
Kafka tooling. It is the smallest useful loop: format a data directory, start
the broker, create a topic, then produce and consume a message.

## Prerequisites

- Rust toolchain matching the workspace `rust-toolchain.toml`.
- A Kafka CLI distribution on your `PATH` for commands such as
  `kafka-topics.sh`, `kafka-console-producer.sh`, and
  `kafka-console-consumer.sh`.
- A clean local data directory.

## 1. Format storage

```bash
rm -rf ./crabka-data
cargo run -p crabka-cli -- format \
  --log-dir ./crabka-data \
  --standalone \
  --node-id 1 \
  --controller-listener 127.0.0.1:9093
```

`--standalone` creates a one-node KRaft controller quorum. The broker will use
the formatted directory in the next step.

## 2. Start the broker

```bash
cargo run -p crabka-broker -- \
  --log-dir ./crabka-data \
  --listen-addr 127.0.0.1:9092 \
  --broker-id 1
```

The Kafka listener is now `127.0.0.1:9092`. Prometheus metrics are exposed on
`:9404`.

## 3. Use Kafka tools unmodified

In another terminal, create a topic:

```bash
kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --create \
  --topic orders \
  --partitions 1 \
  --replication-factor 1
```

Produce one record:

```bash
printf 'order-1\n' | kafka-console-producer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic orders
```

Consume it back:

```bash
kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 1
```

## Next steps

For Kubernetes, use the [operator guide](../deploying-operator/). For an
architectural map of the generated reference docs, see
[Architecture & where things are documented](../architecture/).
