+++
title = "Quickstart"
description = "Run a local Crabka broker in minutes: format a data directory, start the broker, then create a topic and produce and consume a record with standard Kafka tooling."
weight = 10
template = "docs/page.html"
+++

This quickstart runs one local Crabka broker and verifies it with standard Kafka
tooling. It is the smallest useful loop: format a data directory, start the
broker, create a topic, then produce and consume one record.

To run a Kubernetes cluster, go to [Operator Deployment](/docs/deploy/operator/).

## Prerequisites

- Rust toolchain matching the workspace `rust-toolchain.toml`.
- A Kafka CLI distribution on your `PATH` for commands such as
  `kafka-topics.sh`, `kafka-console-producer.sh`, and
  `kafka-console-consumer.sh`.
- A clean local data directory.

## 1. Format storage

```bash
export CRABKA_CLUSTER_ID=00000000-0000-0000-0000-000000000001
rm -rf target/crabka-data

cargo run -p crabka-cli --bin crabka -- format \
  --log-dir target/crabka-data \
  --cluster-id "$CRABKA_CLUSTER_ID" \
  --standalone \
  --node-id 1 \
  --controller-listener 127.0.0.1:9093
```

`crabka format` is a one-time initialization step for an empty log directory.
`--standalone` creates a one-node KRaft controller quorum for local evaluation.

## 2. Start the broker

```bash
cargo run -p crabka-broker --bin crabka-broker -- \
  --log-dir target/crabka-data \
  --cluster-id "$CRABKA_CLUSTER_ID" \
  --broker-id 1 \
  --listen-addr 127.0.0.1:9092
```

The Kafka listener is now `127.0.0.1:9092`. The broker exposes Prometheus
metrics on `:9404`.

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

When you are done, stop the broker process. To start over with a fresh cluster,
remove `target/crabka-data` and repeat the format step.

- Read [Architecture](/docs/start-here/architecture/) for the system model.
- Deploy with the [Operator](/docs/deploy/operator/) for a real cluster.
- Look up exact broker settings in the generated
  [Server Configuration reference](/docs/reference/broker/server-config/).
