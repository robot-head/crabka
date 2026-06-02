+++
title = "Crabka"
sort_by = "weight"

[extra]
lead = "A Rust reimplementation of Apache Kafka. Byte-for-byte wire compatible and KRaft-native — <strong>matching its throughput</strong> on <strong>~40× less memory</strong>, and no JVM to babysit."
url = "/guide/introduction/"
url_button = "Get started"
url2 = "/benchmarks/crabka-vs-kafka/"
url2_button = "See the benchmarks"
repo_url = "https://github.com/robot-head/crabka"
repo_license = "Apache 2.0"
repo_version = "v0.2.0"

# --- Headline numbers (rendered as the stat band under the hero). ---
# Sourced from /benchmarks/crabka-vs-kafka/ — a single-box, like-for-like
# comparison against Apache Kafka 4.3 over the Kafka wire protocol.
[[extra.list]]
icon = "zap"
title = "Kafka-class throughput"
content = 'Matches <strong>Apache Kafka 4.3</strong>&apos;s produce-and-consume throughput within a few percent on identical hardware — ahead on the 1 KiB workloads — at 1.15–1.2× the messages per CPU-core and tighter tail latency.'

[[extra.list]]
icon = "feather"
title = "~40× less memory"
content = "Broker resident in <strong>24–32 MiB</strong> versus Kafka's ~1 GiB JVM heap. No GC pauses, no heap to tune."

[[extra.list]]
icon = "clock"
title = "Ready in 1–2 seconds"
content = "Cold start to first ack in <strong>1–2 s</strong>, versus 8–9 s for a Kafka broker on the same box."

[[extra.list]]
icon = "check-circle"
title = "Byte-for-byte compatible"
content = 'Speaks the Kafka wire protocol exactly. Your existing clients and the JVM <code>kafka-*.sh</code> tools work unmodified.'

# --- Crabka vs Apache Kafka comparison table. ---
[[extra.compare]]
feature = "Runtime"
kafka = "JVM (OpenJDK 21) + garbage collector"
crabka = "Native binary — no JVM, no GC"

[[extra.compare]]
feature = "Broker memory"
kafka = "~1 GiB heap"
crabka = "<strong>24–32 MiB</strong> RSS"

[[extra.compare]]
feature = "Cold start to ready"
kafka = "8–9 s"
crabka = "<strong>1–2 s</strong>"

[[extra.compare]]
feature = "Produce/consume throughput"
kafka = "baseline"
crabka = "<strong>≈ parity</strong> (0.9–1.0×)"

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
crabka = "Built in"

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
content = "No JDK, no ZooKeeper, no separate controller process. One binary to ship, run, and operate."

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
content = "Native producer, consumer, and admin clients, a Kubernetes operator, and an automated rebalancer — all in one workspace."

# --- Top navigation. ---
[[extra.menu.main]]
name = "Guide"
url = "/guide/"
section = "guide"
weight = 10

[[extra.menu.main]]
name = "Benchmarks"
url = "/benchmarks/"
section = "benchmarks"
weight = 15

[[extra.menu.main]]
name = "Reference"
url = "/reference/"
section = "reference"
weight = 20
+++

Crabka is a Rust reimplementation of Apache Kafka. It speaks the Kafka wire
protocol byte-for-byte, runs its metadata quorum on KRaft, and ships native
Rust clients, a Kubernetes operator, and a Cruise-Control-equivalent
rebalancer — all without a JVM.

With Kafka parity now broad and validated against the JVM, Crabka is in
**beta**: greenfield and pre-1.0, ready for evaluation and non-critical
workloads, not yet hardened by production deployment.
