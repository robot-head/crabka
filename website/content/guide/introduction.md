+++
title = "Introduction"
weight = 10
template = "docs/page.html"
+++

Crabka is a Rust reimplementation of Apache Kafka. It speaks the Apache Kafka
wire protocol byte-for-byte (targeting the 4.3.0 message schemas), stores data
in Kafka-compatible log segments, runs metadata on KRaft, and integrates with
standard JVM tooling such as `kafka-topics.sh`, `kafka-configs.sh`,
`kafka-acls.sh`, `kafka-consumer-groups.sh`, and the official Java client.

Beyond the broker, Crabka ships native Rust producer, consumer, admin, and
streams clients; a Confluent-compatible Schema Registry; a gRPC /
Connect-RPC gateway; a Kubernetes operator; and a Cruise-Control-equivalent
partition rebalancer.

## Project status

Crabka is in **beta** (`v0.3.6`). The Kafka-parity surface — wire protocol,
storage, replication, KRaft metadata, security, authorization, quotas, Schema
Registry, gateway, Kubernetes operator, and rebalancer — is broad and validated
byte-for-byte against the JVM, so the project has matured out of its alpha
phase.

It is still **greenfield and pre-1.0**: undeployed, with no production users and
no on-disk compatibility guarantees yet. Treat it as beta: ready for evaluation
and non-critical workloads, not yet proven by production mileage.

## Where to go next

- [Overview](../overview/) explains the system shape and when Crabka is a
  good fit.
- [Quickstart](../quickstart/) gets a local broker running and shows how to
  point Kafka tools at it.
- [Deploying the Operator](../deploying-operator/) is the Kubernetes path.
- [Benchmarks](../../benchmarks/crabka-vs-strimzi/) documents the throughput and
  memory claims on the landing page.
- [Reference](../../reference/) is generated during the docs build from the broker,
  operator, and consensus source code.
