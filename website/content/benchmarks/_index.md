+++
title = "Benchmarks"
sort_by = "weight"
weight = 15
template = "docs/section.html"
+++

How Crabka stacks up against Apache Kafka, measured with the same Rust load
driver speaking the Kafka wire protocol to both stacks — single-box against
Kafka 4.3, and operator-managed three-broker against Strimzi on Kubernetes.
