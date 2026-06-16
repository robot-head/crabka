+++
title = "Architecture & where things are documented"
weight = 15
template = "docs/page.html"

[extra]
mermaid = true
+++

Crabka is several cooperating components, documented at two levels. The Guide
pages explain the shape and common workflows; the Reference tree is generated
from source during the docs build and is where exact CRD fields, broker
settings, protocol tables, and simulator diagrams live.

## The map

{% mermaid() %}
flowchart TD
  subgraph Operator [Operator — Kubernetes CRDs]
    Kafka[Kafka cluster]
    Pool[KafkaNodePool]
    Topic[KafkaTopic]
    User[KafkaUser]
    Rebal[KafkaRebalance]
    SR[SchemaRegistry]
  end
  subgraph Broker [Broker runtime]
    Cfg[Server config]
    TopicCfg[Topic configs]
    Apis[Protocol APIs]
  end
  Consensus[KRaft consensus core]
  Docs[Guide pages]
  Crates[Rust crate APIs]

  Operator --> OpRef[/reference/operator/]
  Broker --> BrokerRef[/reference/broker/]
  Consensus --> ConRef[/reference/concepts/]
  Crates --> RustRef[/api/rust/]
  Docs --> Guide[/guide/]
{% end %}

## Where each component is documented

**Guide pages → narrative workflows.** Start with [Overview](../overview/)
and [Quickstart](../quickstart/). Use [Deploying the Operator](../deploying-operator/),
[Deploying Schema Registry](../deploying-schema-registry/), and
[Streams & Data Formats](../streams/) when you are trying to run or build
something end to end.

**Operator and CRDs → generated operator reference.** Everything you declare in
YAML — cluster shape, node pools, topics, users, rebalances, schema registry,
and gateway — is a Custom Resource. Each CRD's fields are documented under
[`/reference/operator/`](../../reference/operator/), for example the
[`Kafka` cluster CRD](../../reference/operator/kafka/) and the
[`SchemaRegistry` CRD](../../reference/operator/schemaregistry/).

**Broker behavior → generated broker reference.** How the broker itself behaves
is split into three generated pages:

- [Server config](../../reference/broker/server-config/) — broker-level
  configuration keys (the `server.properties` surface).
- [Topic configs](../../reference/broker/topic-configs/) — per-topic settings such
  as retention, cleanup policy, and segment sizing.
- [Protocol APIs](../../reference/broker/protocol-apis/) — the Kafka request/response
  APIs the broker implements and the versions it negotiates.

**Consensus and failure behavior → generated concepts reference.** The KRaft
metadata quorum and how the cluster behaves under partitions, leader loss, and
reconfiguration are documented under [`/reference/concepts/`](../../reference/concepts/),
including the [failure-scenario diagrams](../../reference/concepts/failure-scenarios/).

**Crate-level APIs → the rustdoc.** If you are building against Crabka's Rust
crates — the protocol codec, client libraries, or runtime internals — the
generated rustdoc starts at [`/api/rust/crabka/`](../../api/rust/crabka/index.html).

## How to read the docs

Start with the [Overview](../overview/) for the big picture, then use the
Guide for narrative how-tos. Reach for the generated reference when you need an
exact field name, config key, protocol API, or rustdoc signature. Those pages
are exhaustive, but they assume you already know which component you are looking
at; this page is the map.
