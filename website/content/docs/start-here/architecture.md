+++
title = "Architecture"
description = "How Crabka is built: a Kafka wire-protocol broker core over Kafka-compatible log segments and KRaft metadata, with operator, registry, gateway, and rebalancer."
weight = 20
template = "docs/page.html"

[extra]
mermaid = true
+++

Crabka is a Kafka-compatible system built from one broker core and a set of
operational services around it. The broker speaks the Kafka wire protocol,
stores data in Kafka-compatible log segments, and uses KRaft for metadata. The
operator, Schema Registry, gateway, rebalancer, replicator, and Rust clients
connect to that core as ordinary cluster components.

## Runtime Map

{% mermaid() %}
flowchart TD
  subgraph Kubernetes [Kubernetes control plane]
    Operator[crabka-operator]
    Kafka[Kafka cluster]
    Pool[KafkaNodePool]
    Topic[KafkaTopic]
    User[KafkaUser]
    Rebal[KafkaRebalance]
    SR[SchemaRegistry]
  end
  subgraph Broker [crabka-broker]
    Dispatch[Kafka request dispatch]
    Log[Log segments]
    Groups[Group and transaction coordinators]
    Quorum[KRaft metadata quorum]
  end
  Clients[Kafka clients and kafka-*.sh tools] --> Dispatch
  Dispatch --> Log
  Dispatch --> Groups
  Dispatch <--> Quorum
  Operator --> Kafka
  Operator --> Pool
  Operator --> Topic
  Operator --> User
  Operator --> Rebal
  Operator --> SR
  Operator -.reconciles.-> Broker
  Registry[crabka-schema-registry] --> Dispatch
  Gateway[crabka-grpc-gateway] --> Dispatch
  Rebalancer[crabka-rebalancer] --> Dispatch
  Replicator[crabka-replicator] --> Dispatch
{% end %}

## Core Components

**Broker.** `crabka-broker` controls the Kafka protocol, log storage,
replication, group coordination, transactions, quotas, authorization, metrics,
and the KRaft metadata quorum.

**Operator.** `crabka-operator` turns Kubernetes Custom Resources into
StatefulSets, Services, ConfigMaps, Secrets, certificates, broker configuration,
topic/user operations, rebalances, gateways, and Schema Registry deployments.

**Clients and services.** The native Rust clients, Schema Registry, gRPC /
Connect-RPC gateway, rebalancer, and replicator all use the Kafka-compatible
surface rather than private broker internals.

**Reference generation.** The docs build generates the exact CRD fields, broker
settings, topic settings, protocol tables, and consensus failure diagrams from
source, so the reference pages stay close to the implementation.

## Documentation Map

Use the docs by intent:

- Run locally: [Quickstart](/docs/start-here/quickstart/).
- Run on Kubernetes: [Operator Deployment](/docs/deploy/operator/).
- Add schemas: [Schema Registry Deployment](/docs/deploy/schema-registry/).
- Build stream processors: [Streams and Data Formats](/docs/develop/streams/).
- Inspect exact generated fields and keys:
  [Reference](/docs/reference/).
- Browse crate APIs: [Rust API](/docs/develop/rust-api/).

## Generated Reference

The generated reference is organized by component:

- [Operator CRDs](/docs/reference/operator/) document Kubernetes resource
  fields such as `Kafka`, `KafkaNodePool`, `KafkaUser`, `KafkaTopic`,
  `SchemaRegistry`, and `KafkaRebalance`.
- [Broker reference](/docs/reference/broker/) documents server configuration,
  topic configuration, and supported Kafka protocol APIs.
- [Concepts](/docs/reference/concepts/) document KRaft failure behavior with
  simulator-generated diagrams.

Use those pages when you need an exact field name, config key, API version, or
rustdoc signature.
