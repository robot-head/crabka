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
