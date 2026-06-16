+++
title = "Architecture & where things are documented"
weight = 15
template = "docs/page.html"

[extra]
mermaid = true
+++

Crabka is several cooperating components, and each one is documented in a
different reference tree. This page is the map: it names every component and
links straight to the page that documents it, so you never have to guess which
reference covers what.

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
  Crates[Rust crate APIs]

  Operator --> OpRef[/reference/operator/]
  Broker --> BrokerRef[/reference/broker/]
  Consensus --> ConRef[/reference/concepts/]
  Crates --> RustRef[/api/rust/]
{% end %}

## Where each component is documented

**Operator and CRDs → the operator reference.** Everything you declare in
YAML — cluster shape, node pools, topics, users, rebalances, schema registry —
is a Custom Resource. Each CRD's fields are documented under
[`/reference/operator/`](/reference/operator/), for example the
[`Kafka` cluster CRD](/reference/operator/kafka/) and the
[`SchemaRegistry` CRD](/reference/operator/schemaregistry/).

**Broker behavior → the broker reference.** How the broker itself behaves is
split into three generated pages:

- [Server config](/reference/broker/server-config/) — broker-level
  configuration keys (the `server.properties` surface).
- [Topic configs](/reference/broker/topic-configs/) — per-topic settings such
  as retention, cleanup policy, and segment sizing.
- [Protocol APIs](/reference/broker/protocol-apis/) — the Kafka request/response
  APIs the broker implements and the versions it negotiates.

**Consensus and failure behavior → the concepts reference.** The KRaft metadata
quorum and how the cluster behaves under partitions, leader loss, and
reconfiguration are documented under [`/reference/concepts/`](/reference/concepts/),
including the [failure-scenario diagrams](/reference/concepts/failure-scenarios/).

**Crate-level APIs → the rustdoc.** If you are building against Crabka's Rust
crates — the protocol codec, client libraries, or runtime internals — the
generated rustdoc lives at [`/api/rust/`](/api/rust/).

## How to read the docs

Start with the [Overview](/guide/overview/) for the big picture, then this
Guide for narrative how-tos (deploying the operator, the schema registry,
streams). Reach for the reference trees above when you need an exact field
name, config key, or API signature — they are generated from the source, so
they are exhaustive but assume you already know which component you are looking
at. That is what this page is for.
