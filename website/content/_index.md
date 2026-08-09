+++
title = "Crabka"
sort_by = "weight"

[extra]
lead = "A native-Rust Kafka-compatible broker and toolkit. Crabka speaks the Kafka wire protocol byte-for-byte, runs KRaft without a JVM, and keeps the operator, clients, schema registry, gateway, and rebalancer in one workspace."
url = "/docs/"
url_button = "Get started"
url2 = "/benchmarks/crabka-vs-strimzi/"
url2_button = "See the benchmarks"
repo_url = "https://github.com/robot-head/crabka"
repo_license = "Apache 2.0"
repo_version = "v0.3.7"

# --- Headline numbers (rendered as the stat band under the hero). ---
# Sourced from /benchmarks/crabka-vs-strimzi/ — an operator-managed,
# three-broker Kubernetes comparison against Strimzi (Apache Kafka).
[[extra.list]]
icon = "zap"
title = "Kafka-class throughput"
content = 'Matches or beats a like-for-like <strong>Strimzi</strong> cluster on key produce-and-consume workloads, including multi-producer fan-out, at <strong>1.6–3.0×</strong> the messages per CPU-core.'

[[extra.list]]
icon = "feather"
title = "≈ a tenth of the memory"
content = "A Crabka broker's working set sits in the <strong>low hundreds of MiB</strong> against Strimzi's <strong>2.5–5.5 GiB</strong>. No GC pauses, no heap to tune."

[[extra.list]]
icon = "clock"
title = "Ready in 1–2 seconds"
content = "Cold start to first ack in <strong>1–2 s</strong> — no JVM warmup, no GC ramp."

[[extra.list]]
icon = "check-circle"
title = "Byte-for-byte compatible"
content = 'Speaks the Kafka wire protocol exactly. Existing clients and the JVM <code>kafka-*.sh</code> tools work unmodified.'

# --- Crabka vs Apache Kafka comparison table. ---
[[extra.compare]]
feature = "Runtime"
kafka = "JVM (OpenJDK 21) + garbage collector"
crabka = "Native binary — no JVM, no GC"

[[extra.compare]]
feature = "Broker memory"
kafka = "2.5–5.5 GiB"
crabka = "<strong>114–622 MiB measured</strong>"

[[extra.compare]]
feature = "Cold start to ready"
kafka = "8–9 s"
crabka = "<strong>1–2 s</strong>"

[[extra.compare]]
feature = "Produce/consume throughput"
kafka = "baseline"
crabka = "<strong>matches–beats</strong>"

[[extra.compare]]
feature = "Packaging"
kafka = "JVM + shell scripts"
crabka = "Single static binary"

[[extra.compare]]
feature = "Memory safety"
kafka = "Manual / JVM-managed"
crabka = "Safe Rust — <code>unsafe</code> forbidden"

[[extra.compare]]
feature = "Metadata quorum"
kafka = "KRaft"
crabka = "KRaft — native Rust, real KIP-595 wire"

[[extra.compare]]
feature = "Wire protocol"
kafka = "Apache Kafka 4.3"
crabka = "Apache Kafka 4.3 — byte-exact"

[[extra.compare]]
feature = "Admin tooling"
kafka = "<code>kafka-*.sh</code>"
crabka = "<code>kafka-*.sh</code>, unmodified"

[[extra.compare]]
feature = "Operator & rebalancer"
kafka = "Strimzi + Cruise Control (separate)"
crabka = "Crabka operator + rebalancer"

[[extra.compare]]
feature = "License"
kafka = "Apache 2.0"
crabka = "Apache 2.0"

# --- Feature highlights ("Why Crabka"). ---
[[extra.features]]
icon = "shuffle"
title = "Drop-in protocol compatibility"
content = 'Every encode/decode is checked against <code>kafka-clients</code> 4.3.0 with differential byte-equality tests, and a JVM acceptance suite drives the official <code>cp-kafka</code> admin tools against a live broker.'

[[extra.features]]
icon = "shield"
title = "Memory-safe &amp; concurrent"
content = 'Async Rust on <code>tokio</code>. No JVM, no GC pauses, and <code>unsafe_code = "forbid"</code> across the entire workspace.'

[[extra.features]]
icon = "box"
title = "Single static binary"
content = "No JDK and no ZooKeeper. Run a broker/controller process as a native binary, or let the Kubernetes operator manage it."

[[extra.features]]
icon = "layers"
title = "KRaft-native"
content = 'Metadata lives in a native KRaft quorum from day one — speaking the real KIP-595 wire (interoperable with JVM controllers), with snapshots, dynamic reconfiguration, and split controller/broker roles included.'

[[extra.features]]
icon = "lock"
title = "Modern crypto"
content = "TLS via <code>rustls</code>; SASL/SCRAM-256/512, PLAIN, OAUTHBEARER (JWT/JWKS), and GSSAPI/Kerberos out of the box."

[[extra.features]]
icon = "grid"
title = "Batteries included"
content = "Native producer, consumer, admin, and streams clients, Schema Registry, a gateway, Kubernetes operator, and automated rebalancer — all in one workspace."

# --- Top navigation. ---
[[extra.menu.main]]
name = "Documentation"
url = "/docs/"
section = "docs"
weight = 10

[[extra.menu.main]]
name = "Benchmarks"
url = "/benchmarks/"
section = "benchmarks"
weight = 15
+++

Crabka is a Rust reimplementation of Apache Kafka. It speaks the Kafka wire
protocol byte-for-byte, stores records in Kafka-compatible logs, and runs
metadata through KRaft. The same workspace holds the operational pieces: native
Rust clients, Schema Registry, a gRPC / Connect-RPC gateway, a Kubernetes
operator, and a Cruise-Control-equivalent rebalancer.

Crabka is in **beta**: greenfield and pre-1.0. Kafka parity is broad and
validated against the JVM. Use Crabka for evaluation and non-critical workloads.
Production deployment has not hardened it yet.
