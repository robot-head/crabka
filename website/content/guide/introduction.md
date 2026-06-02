+++
title = "Introduction"
weight = 10
template = "docs/page.html"
+++

Crabka is a Rust reimplementation of Apache Kafka. It speaks the Apache
Kafka wire protocol byte-for-byte (targeting the 4.3.0 message schemas),
stores data in Kafka-compatible log segments, runs its metadata quorum on
KRaft, and integrates with the standard JVM tooling (`kafka-topics.sh`,
`kafka-configs.sh`, `kafka-acls.sh`, and the official Java client).

Beyond the broker, Crabka ships native Rust clients, a Kubernetes operator
(Strimzi-equivalent), and a Cruise-Control-equivalent partition rebalancer.

## Project status

Crabka is in **beta**. The Kafka-parity surface — wire protocol, storage,
replication, KRaft metadata, security, authorization, quotas, the Kubernetes
operator, and the rebalancer — is broad and validated byte-for-byte against the
JVM, so the project has matured out of its alpha phase.

It is still **greenfield and pre-1.0**: undeployed, with no production users and
no on-disk compatibility guarantees yet. Treat it as beta — ready for evaluation
and non-critical workloads, not yet proven by production mileage. See the
[feature and KIP matrix](https://github.com/robot-head/crabka) in the README for
exactly what is implemented today.
