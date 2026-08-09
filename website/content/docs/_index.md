+++
title = "Documentation"
description = "Crabka documentation: a Rust reimplementation of Apache Kafka. Learn the architecture, run the quickstart, deploy on Kubernetes, and look up config reference."
sort_by = "weight"
weight = 10
template = "docs/section.html"

[extra]
mermaid = true
+++

Crabka is a Rust reimplementation of Apache Kafka. It speaks the Kafka wire
protocol, stores records in Kafka-compatible log segments, runs metadata on
KRaft, and is validated against the official JVM clients and command-line
tooling.

Use this page to learn what Crabka is, how the parts fit together, and where to
go next.

## Project Status

Crabka is **beta**, pre-1.0 software. The Kafka-facing surface is broad. Crabka
implements the wire protocol, log storage, replication, KRaft metadata,
authorization, quotas, tiered storage, transactions, consumer groups, share
groups, Schema Registry, Kubernetes operation, and Rust clients. All of these
are tested against JVM Kafka behavior.

The repository README states the same caveat: Crabka is still greenfield
infrastructure. It has no production users, and it does not yet promise on-disk
compatibility across versions. Use it for evaluation, development,
interoperability testing, and non-critical workloads while the project hardens.

## Why Crabka

Kafka on Kubernetes usually needs three separate pieces: the JVM broker,
Strimzi, and Cruise Control. Crabka reimplements the broker in safe Rust and
puts the operational tools in one workspace.

- **Kafka wire compatibility.** Crabka generates the protocol codecs from the
  Apache Kafka message schemas, and the tests check them byte-for-byte against
  `kafka-clients`.
- **Works with existing tooling.** JVM tools such as `kafka-topics.sh`,
  `kafka-configs.sh`, `kafka-acls.sh`, and `kafka-consumer-groups.sh` run
  against a live Crabka broker.
- **Rust runtime.** Crabka uses `tokio`, forbids unsafe code across the
  workspace, and avoids JVM heap tuning and GC behavior.
- **KRaft-native.** Crabka stores metadata in a native KRaft quorum. ZooKeeper
  mode and ZooKeeper-to-KRaft migration are deliberately out of scope.
- **Operations included.** The repository includes a Kubernetes operator,
  Prometheus metrics, OTLP tracing, Helm charts, OCI images, and a partition
  rebalancer.

## How It Fits Together

{% mermaid() %}
flowchart TD
  Clients[Kafka clients and kafka-*.sh tools] --> Broker[crabka-broker]
  Broker --> Log[Kafka-compatible log segments]
  Broker --> KRaft[KRaft metadata quorum]
  Broker --> Remote[Tiered storage]
  Operator[crabka-operator] --> Broker
  Registry[crabka-schema-registry] --> Broker
  Gateway[crabka-grpc-gateway] --> Broker
  Rebalancer[crabka-rebalancer] --> Broker
  Replicator[crabka-replicator] --> Broker
  RustClients[Rust producer / consumer / admin / streams clients] --> Broker
{% end %}

The broker handles Kafka protocol traffic and persists data to Kafka-compatible
logs. Metadata changes flow through the KRaft quorum. The operator, Schema
Registry, gateway, rebalancer, replicator, and Rust clients connect to that core
as normal Kafka clients or cluster controllers.

## Documentation Map

The docs are organized by what you are trying to do:

- **Start Here** explains the local quickstart, architecture, and browser
  consensus playground.
- **Deploy** covers Kubernetes operation and Schema Registry.
- **Build** covers the Rust Streams client, data formats, and rustdoc entry
  points.
- **Reference** is generated from source during the docs build and contains exact
  CRD fields, broker configuration keys, topic configuration keys, protocol API
  tables, and consensus failure diagrams.

## Next Steps

- Run a local broker with the [Quickstart](start-here/quickstart/).
- Understand the system with [Architecture](start-here/architecture/).
- Deploy a Kubernetes cluster with [Operator Deployment](deploy/operator/).
- Look up exact fields and config keys in the generated [Reference](reference/).
