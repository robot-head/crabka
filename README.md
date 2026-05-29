<p align="center">
  <img src="docs/crabka-text-wide.png" alt="Crabka" width="480">
</p>

<p align="center">
  <a href="https://codspeed.io/robot-head/crabka?utm_source=badge"><img src="https://img.shields.io/endpoint?url=https://codspeed.io/badge.json" alt="CodSpeed"></a>
  <a href="https://codecov.io/gh/robot-head/crabka"><img src="https://codecov.io/gh/robot-head/crabka/graph/badge.svg?token=EU56CZE3DU" alt="codecov"></a>
</p>

# Crabka

**Crabka is a Rust reimplementation of [Apache Kafka](https://kafka.apache.org).**

It speaks the Apache Kafka wire protocol byte-for-byte (targeting the 4.3.0
message schemas), stores data in Kafka-compatible log segments, runs its metadata
quorum on KRaft, and integrates cleanly with the standard JVM tooling —
`kafka-topics.sh`, `kafka-configs.sh`, `kafka-acls.sh`, `kafka-consumer-groups.sh`,
`kafka-leader-election.sh`, `kafka-reassign-partitions.sh`, and the official Java
client. Existing producers, consumers, and operator workflows work against a
Crabka broker without modification.

Beyond the broker, Crabka ships native Rust clients, a Kubernetes operator
(Strimzi-equivalent), and a Cruise-Control-equivalent partition rebalancer.

Distributed under the Apache License 2.0 as a derivative work.

## Why Crabka

- **Drop-in protocol compatibility.** Crabka is validated against the JVM Kafka
  client via differential byte-equality tests: every encode/decode is checked
  against `kafka-clients` 4.3.0, and a JVM acceptance suite drives the official
  `cp-kafka` admin tools against a live Crabka broker.
- **Memory-safe, fearlessly concurrent.** Written in async Rust on `tokio`, with
  no JVM and no GC pauses. `unsafe_code = "forbid"` across the workspace.
- **Single static binary.** No JDK, no ZooKeeper, no separate controller process.
- **KRaft-native.** Metadata lives in an `openraft`-backed quorum from day one.
- **Modern crypto.** TLS via `rustls`; SASL/SCRAM-SHA-256/512, SASL/PLAIN,
  SASL/OAUTHBEARER (signed-JWT / JWKS), and SASL/GSSAPI (Kerberos) out of the box.
- **Batteries included.** Native producer/consumer/admin clients, a Kubernetes
  operator, and an automated rebalancer live in the same workspace.

## Project status

Crabka is **greenfield and pre-1.0** (`v0.1.1`) — undeployed, with no production
users and no on-disk compatibility guarantees yet. The Kafka wire protocol is the
contract that matters, and it is locked to byte-exactness via the differential
oracle and JVM acceptance tests.

What works today:

- Single-broker and multi-broker clusters with KRaft metadata (including Raft
  snapshots, KIP-630, dynamic quorum reconfiguration, KIP-853, and separate
  `process.roles` controller-only / broker-only nodes with observer metadata
  fetch), replication, ISR maintenance, leader election (including offset-aware
  unclean recovery, KIP-966 / KIP-841), fetch-from-follower / rack-aware reads
  (KIP-392), partition reassignment, JBOD (multi-log-dir) with intra-broker
  log-dir reassignment (`AlterReplicaLogDirs`, KIP-113).
- Idempotent and transactional produce/consume (exactly-once), consumer groups
  with both the classic (eager) and cooperative-incremental (KIP-429) rebalance
  protocols, static membership (KIP-345), incremental fetch sessions (KIP-227),
  and log compaction.
- TLS / mTLS, SASL (PLAIN, SCRAM-256/512, OAUTHBEARER with JWKS / signed-JWT
  and opaque-token introspection, GSSAPI/Kerberos), SASL re-authentication
  (KIP-368), delegation tokens (KIP-48 / KIP-373), ACL authorization, the OPA
  cluster-authorizer bridge, and the full client-quota surface.
- Native Rust producer / consumer / admin clients.
- A Kubernetes operator (Strimzi-equivalent CRDs, including a tiered-storage
  surface) and a Cruise-Control-equivalent rebalancer.

Notable gaps (see the [KIP matrix](#kip-implementation-status) for detail): the
next-gen consumer group protocol (KIP-848) is in progress — broker-side
foundations, `__consumer_offsets` persistence, a rack-aware `UniformAssignor`,
the pluggable server-side assignor surface, and `subscribed_topic_regex` are in
tree, and GA `group.protocol=consumer` clients (kafka-clients 4.0) now consume
end to end; classic→next-gen group migration is still pending. Tiered storage
(KIP-405) is partial: the `crabka-remote-storage-topic` (KIP-405 production
RLMM) crate is in tree but not yet wired into the broker. Share groups /
queues (KIP-932) and a Kafka Connect / Streams / MirrorMaker equivalent are not
yet implemented. ZooKeeper mode and ZK→KRaft migration are
deliberately out of scope — Crabka is KRaft-only.

## Architecture

Crabka is organized as a Rust workspace.

### Core protocol & storage

| Crate | Role |
|-------|------|
| [`crabka-protocol`](crates/protocol) | Kafka wire-protocol codec (4.3.0), typed `RecordBatch`, zero-copy borrowed decode |
| [`crabka-protocol-codegen`](crates/protocol-codegen) | Code generator that turns Kafka message schemas into the protocol codec |
| [`crabka-compression`](crates/compression) | Kafka-compatible compression codecs (gzip, snappy, lz4, zstd) |
| [`crabka-log`](crates/log) | Byte-compatible log segments, indexes, transaction index, compaction, retention |
| [`crabka-metadata`](crates/metadata) | Versioned metadata records, immutable metadata image, ACL model |
| [`crabka-raft`](crates/raft) | Metadata Raft quorum (`openraft` adapters + controller) |
| [`crabka-security`](crates/security) | TLS (`rustls`), SASL/PLAIN, SASL/SCRAM-256/512, SASL/OAUTHBEARER, SASL/GSSAPI, mTLS |

### Broker

| Crate | Role |
|-------|------|
| [`crabka-broker`](crates/broker) | Broker runtime: request handlers, replication, group & transaction coordinators, quotas, ACL authorizer, metrics |

### Clients

| Crate | Role |
|-------|------|
| [`crabka-client-core`](crates/client-core) | Connection pool + API-version negotiation |
| [`crabka-client-producer`](crates/client-producer) | Native idempotent / transactional producer |
| [`crabka-client-consumer`](crates/client-consumer) | Native subscribe-style consumer + group membership |
| [`crabka-client-admin`](crates/client-admin) | Admin client (topics, configs, ACLs, quotas, SCRAM) |

### Operations

| Crate | Role |
|-------|------|
| [`crabka-cli`](crates/cli) | `crabka` binary: `format` + bootstrap |
| [`crabka-operator`](crates/operator) | Kubernetes operator (Strimzi-equivalent CRDs) |
| [`crabka-rebalancer`](crates/rebalancer) | Cruise-Control-equivalent partition rebalancer |
| [`crabka-bench-driver`](crates/bench-driver) | Load driver + report aggregator for the Crabka-vs-Strimzi benchmark harness |

## Feature compatibility

The following tables list Apache Kafka functional surface area and whether Crabka
implements it today. Legend: ✅ implemented · ⚠️ partial · ❌ not yet · ⛔ out of scope.

### Wire protocol & clients

| Feature | Status |
|---------|:------:|
| Wire-protocol byte-exact codec (request / response), Kafka 4.3.0 schemas | ✅ |
| API version negotiation (`ApiVersions`) | ✅ |
| Flexible / tagged-field versions (KIP-482) | ✅ |
| Compression: gzip, snappy, lz4, zstd | ✅ |
| JVM Java client interoperability | ✅ |
| Native Rust producer / consumer / admin clients | ✅ |
| Broker-side recompression (`compression.type` per topic) | ✅ |
| Incremental fetch sessions (KIP-227) | ✅ |

### Storage

| Feature | Status |
|---------|:------:|
| Byte-compatible log segments | ✅ |
| Offset + time indexes | ✅ |
| Time-based and size-based retention | ✅ |
| Transaction index (`.txnindex`) per segment | ✅ |
| Leader-epoch checkpoint file (KIP-101 / KIP-279) | ✅ |
| Log compaction (`cleanup.policy=compact`) | ✅ |
| Multiple log directories (JBOD) + `DescribeLogDirs` (KIP-113) | ✅ |
| Intra-broker log-dir reassignment (`AlterReplicaLogDirs`, KIP-113) | ✅ |
| Message format v0/v1 down-conversion | ✅ |
| Tiered storage (KIP-405) | ⚠️ |

### Producer

| Feature | Status |
|---------|:------:|
| `Produce` (acks=0, acks=1, acks=all) | ✅ |
| Idempotent producer (`enable.idempotence=true`) | ✅ |
| `InitProducerId` + per-(pid, epoch, sequence) dedup | ✅ |
| Producer-id recovery / safe epoch bump (KIP-360) | ✅ |
| Transactional producer (KIP-98) | ✅ |
| Transactions v2 / server-side defense (KIP-890 + KIP-1228) | ✅ |

### Consumer

| Feature | Status |
|---------|:------:|
| `Fetch` (single + multi-partition) | ✅ |
| Consumer groups + group coordinator | ✅ |
| `__consumer_offsets` topic | ✅ |
| `OffsetCommit` / `OffsetFetch` | ✅ |
| Classic (eager) group rebalance protocol | ✅ |
| `isolation.level=read_committed` (LSO clamping) | ✅ |
| Cooperative incremental rebalance (KIP-429) | ✅ |
| Static membership (KIP-345) | ✅ |
| `OffsetDelete` admin API (KIP-496) | ✅ |
| Next-gen consumer group protocol (KIP-848) | ⚠️ |

### Replication & durability

| Feature | Status |
|---------|:------:|
| Multi-broker replication, follower Fetch loop | ✅ |
| In-Sync Replica (ISR) tracking | ✅ |
| ISR shrink / expand via `AlterPartition` (KIP-497) | ✅ |
| High-watermark tracking | ✅ |
| `acks=all` blocks until full-ISR replication | ✅ |
| KIP-101 leader-epoch fencing | ✅ |
| Automatic leader election on broker death | ✅ |
| `ElectLeaders` API (KIP-460, PREFERRED + UNCLEAN) | ✅ |
| Auto preferred-replica rebalance | ✅ |
| `AlterPartitionReassignments` / `ListPartitionReassignments` (KIP-455) | ✅ |
| KIP-73 throttled replication | ✅ |
| Fetch-from-follower / rack-aware reads (KIP-392) | ✅ |
| Offset-aware unclean recovery + force-elect toggle (KIP-966 / KIP-841) | ✅ |
| Cruise-Control-equivalent rebalancer (advisor + executor) | ✅ |

### Metadata quorum (KRaft)

| Feature | Status |
|---------|:------:|
| KRaft controller quorum (raft-based, `openraft`) | ✅ |
| Metadata image + delta apply | ✅ |
| Metadata snapshots + `FetchSnapshot` (KIP-630) | ✅ |
| Controller bootstrap via `crabka format` | ✅ |
| Separate `process.roles` (controller-only / broker-only) + observer metadata fetch | ✅ |
| `metadata.version` feature level (KIP-584) | ⚠️ |
| Dynamic quorum voters — `Add`/`Remove`/`UpdateRaftVoter` (KIP-853) | ✅ |
| ZooKeeper mode / ZK→KRaft migration | ⛔ (KRaft only) |

### Admin & operator surface

| Feature | Status |
|---------|:------:|
| `CreateTopics` / `DeleteTopics` | ✅ |
| `CreatePartitions` | ✅ |
| `DeleteRecords` | ✅ |
| `Metadata` / `DescribeCluster` | ✅ |
| `AlterConfigs` / `IncrementalAlterConfigs` (topic + broker scope) | ✅ |
| `DescribeConfigs` | ✅ |
| `ListGroups` / `DescribeGroups` / `DeleteGroups` | ✅ |
| Controlled shutdown (`BrokerHeartbeat.want_shut_down`) | ✅ |
| JVM `kafka-*.sh` operator-tool compatibility | ✅ |

### Security

| Feature | Status |
|---------|:------:|
| TLS (`rustls`-backed, per-listener) | ✅ |
| SASL/PLAIN | ✅ |
| SASL/SCRAM-SHA-256 | ✅ |
| SASL/SCRAM-SHA-512 | ✅ |
| SASL/OAUTHBEARER (unsecured + signed-JWT / JWKS, KIP-255) | ✅ |
| Per-listener protocol multiplexing (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL) | ✅ |
| Inter-broker auth (TLS + SASL on data plane & raft) | ✅ |
| `AlterUserScramCredentials` / `DescribeUserScramCredentials` (KIP-554) | ✅ |
| mTLS client authentication | ✅ |
| TLS cert hot-reload (non-disruptive rotation) | ✅ |
| SASL re-authentication (KIP-368) | ✅ |
| SASL/GSSAPI (Kerberos) | ✅ |
| Delegation tokens (KIP-48 / KIP-373) | ✅ |

### Authorization

| Feature | Status |
|---------|:------:|
| ACL authorizer (Topic / Group / Cluster / TransactionalId) | ✅ |
| KRaft-native `StandardAuthorizer` semantics (KIP-801) | ✅ |
| `Literal` + `Prefixed` pattern matching (KIP-290) | ✅ |
| `Allow` + `Deny` rules, DENY-wins, deny-by-default | ✅ |
| Operation implications (`Read`/`Write`/`Delete`/`Alter` → `Describe`) | ✅ |
| `CreateAcls` / `DeleteAcls` / `DescribeAcls` (KIP-140) | ✅ |
| Multiple super-users (`super.users`-style) | ✅ |
| Host filters | ✅ |
| Authorized-operations in describe responses (KIP-430) | ✅ |

### Quotas

| Feature | Status |
|---------|:------:|
| Client quotas (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`) | ✅ |
| `AlterClientQuotas` / `DescribeClientQuotas` (KIP-13 + KIP-124 + KIP-546) | ✅ |
| User + (user, client-id) tuple + default entity scopes | ✅ |
| IP entity + `connection_creation_rate` (KIP-612) | ✅ |
| Controller mutation rate (KIP-599) | ✅ |

### Observability

| Feature | Status |
|---------|:------:|
| Structured logging via `tracing` | ✅ |
| Prometheus metrics / JMX-equivalent exporter | ✅ |
| OTLP distributed tracing (OpenTelemetry) | ✅ |
| Client metrics push (KIP-714) | ⚠️ |

### Kubernetes operator (Strimzi-equivalent)

| Feature | Status |
|---------|:------:|
| `Kafka` / `KafkaNodePool` / `KafkaTopic` / `KafkaUser` / `KafkaRebalance` CRDs | ✅ |
| StatefulSet per node pool + headless services | ✅ |
| Persistent (PVC) + JBOD storage | ✅ |
| Listeners: internal / NodePort / LoadBalancer | ✅ |
| Listeners: Ingress / Route | ⚠️ |
| Listener auth wiring (TLS / SCRAM / OAuth / Kerberos) | ✅ |
| Cluster CA + clients CA generation & rotation | ✅ |
| `KafkaUser` mTLS + SCRAM + ACLs + quotas | ✅ |
| `KafkaTopic` reconciliation (create / alter / partitions) | ✅ |
| NetworkPolicy generation | ✅ |
| `metricsConfig` (PodMonitor / ServiceMonitor) | ✅ |
| Configurable logging (`Kafka.spec.logging`) | ✅ |
| Rolling restart on config drift | ✅ |
| Version upgrades (`metadata.version` + ordered roll) | ✅ |
| `KafkaConnect` / `KafkaMirrorMaker` / `KafkaBridge` CRDs | ❌ |

### Partition rebalancer (Cruise-Control-equivalent)

| Feature | Status |
|---------|:------:|
| Connect-RPC advisor service | ✅ |
| Prometheus metrics scraper + usage windows | ✅ |
| Goals: rack-aware, replica/leader distribution, topic-replica distribution | ✅ |
| Goals: disk / CPU / network-in / network-out capacity + usage | ✅ |
| Goals: min-topic-leaders-per-broker, preferred-leader idempotency | ✅ |
| Execute path (KIP-455 reassignment + KIP-73 throttling, persisted) | ✅ |
| Anomaly detector + self-healing proposals | ✅ |
| `KafkaRebalance` CRD integration | ✅ |

### Ecosystem (out of broker core)

| Feature | Status |
|---------|:------:|
| Kafka Streams equivalent | ❌ |
| Kafka Connect equivalent | ❌ |
| MirrorMaker equivalent | ❌ |
| Schema Registry | ❌ |

## KIP implementation status

This matrix tracks the significant Kafka Improvement Proposals that define
Apache Kafka's protocol and feature surface, with Crabka's status for each. It
is not exhaustive of every accepted KIP (Kafka has well over a thousand) — it
covers the user-visible protocol, storage, replication, security, and operations
KIPs. Legend: ✅ implemented · ⚠️ partial · ❌ not yet · ⛔ out of scope.

### Wire protocol & message format

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-31](https://cwiki.apache.org/confluence/display/KAFKA/KIP-31) | Move to relative offsets in compressed message sets | ✅ |
| [KIP-32](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32) | Add timestamps to messages | ✅ |
| [KIP-82](https://cwiki.apache.org/confluence/display/KAFKA/KIP-82) | Add record headers | ✅ |
| [KIP-219](https://cwiki.apache.org/confluence/display/KAFKA/KIP-219) | Improve quota communication (throttle-then-respond) | ⚠️ |
| [KIP-227](https://cwiki.apache.org/confluence/display/KAFKA/KIP-227) | Incremental fetch sessions | ✅ |
| [KIP-482](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482) | Optional tagged fields (flexible versions) | ✅ |
| [KIP-511](https://cwiki.apache.org/confluence/display/KAFKA/KIP-511) | Collect & expose client name and version | ✅ |
| [KIP-559](https://cwiki.apache.org/confluence/display/KAFKA/KIP-559) | Make the protocol friendlier with L7 proxies | ✅ |
| [KIP-903](https://cwiki.apache.org/confluence/display/KAFKA/KIP-903) | Fence replicas with stale broker epoch from the ISR | ⚠️ |

### Producer — idempotence, transactions & EOS

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98) | Exactly-once delivery & transactional messaging | ✅ |
| [KIP-360](https://cwiki.apache.org/confluence/display/KAFKA/KIP-360) | Improve reliability of idempotent / transactional producer | ✅ |
| [KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447) | Producer scalability for exactly-once semantics | ⚠️ |
| [KIP-480](https://cwiki.apache.org/confluence/display/KAFKA/KIP-480) | Sticky partitioner | ✅ |
| [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679) | Strongest delivery guarantee (idempotence) by default | ✅ |
| [KIP-794](https://cwiki.apache.org/confluence/display/KAFKA/KIP-794) | Strictly uniform sticky partitioner | ✅ |
| [KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890) | Transactions server-side defense (`transaction.version` 2) | ✅ |
| [KIP-1228](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1228) | Add transaction version to `WriteTxnMarkers` | ✅ |

### Consumer groups

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-62](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62) | Background-thread heartbeats (session vs poll timeout) | ✅ |
| [KIP-394](https://cwiki.apache.org/confluence/display/KAFKA/KIP-394) | Require `member.id` for initial JoinGroup | ✅ |
| [KIP-345](https://cwiki.apache.org/confluence/display/KAFKA/KIP-345) | Static membership | ✅ |
| [KIP-429](https://cwiki.apache.org/confluence/display/KAFKA/KIP-429) | Cooperative incremental rebalance protocol | ✅ |
| [KIP-496](https://cwiki.apache.org/confluence/display/KAFKA/KIP-496) | `OffsetDelete` admin API | ✅ |
| [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848) | Next-generation consumer rebalance protocol | ⚠️ |

### Storage & log

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-204](https://cwiki.apache.org/confluence/display/KAFKA/KIP-204) | `DeleteRecords` via the Admin client | ✅ |
| [KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112) | Handle disk failure for JBOD | ⚠️ |
| [KIP-113](https://cwiki.apache.org/confluence/display/KAFKA/KIP-113) | Replica movement between log directories (JBOD) | ✅ |
| [KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405) | Kafka tiered storage | ⚠️ |

### Replication & availability

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-73](https://cwiki.apache.org/confluence/display/KAFKA/KIP-73) | Replication quotas | ✅ |
| [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101) | Leader-epoch-based truncation | ✅ |
| [KIP-279](https://cwiki.apache.org/confluence/display/KAFKA/KIP-279) | Fix leader/follower log divergence | ✅ |
| [KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320) | Detect & handle log truncation (leader epoch in fetch) | ⚠️ |
| [KIP-392](https://cwiki.apache.org/confluence/display/KAFKA/KIP-392) | Fetch from closest replica (rack-aware) | ✅ |
| [KIP-455](https://cwiki.apache.org/confluence/display/KAFKA/KIP-455) | Replica reassignment admin API | ✅ |
| [KIP-460](https://cwiki.apache.org/confluence/display/KAFKA/KIP-460) | Admin leader election (`ElectLeaders`) | ✅ |
| [KIP-497](https://cwiki.apache.org/confluence/display/KAFKA/KIP-497) | Inter-broker `AlterPartition` (AlterIsr) | ✅ |
| [KIP-841](https://cwiki.apache.org/confluence/display/KAFKA/KIP-841) | Fence stale-epoch replicas / unclean recovery toggle | ✅ |
| [KIP-966](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966) | Eligible leader replicas / offset-aware unclean recovery | ✅ |
| [KIP-996](https://cwiki.apache.org/confluence/display/KAFKA/KIP-996) | Pre-vote | ❌ |

### KRaft metadata quorum

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500) | Replace ZooKeeper with a self-managed metadata quorum | ✅ |
| [KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595) | A Raft protocol for the metadata quorum | ⚠️ |
| [KIP-630](https://cwiki.apache.org/confluence/display/KAFKA/KIP-630) | Kafka Raft snapshot | ✅ |
| [KIP-631](https://cwiki.apache.org/confluence/display/KAFKA/KIP-631) | The quorum-based Kafka controller (metadata records) | ✅ |
| [KIP-584](https://cwiki.apache.org/confluence/display/KAFKA/KIP-584) | Versioning scheme for features (`metadata.version`) | ⚠️ |
| [KIP-778](https://cwiki.apache.org/confluence/display/KAFKA/KIP-778) | KRaft-to-KRaft upgrades | ⚠️ |
| [KIP-853](https://cwiki.apache.org/confluence/display/KAFKA/KIP-853) | KRaft controller membership changes (dynamic voters) | ✅ |
| [KIP-1022](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1022) | Formatting and updating features | ⚠️ |
| [KIP-866](https://cwiki.apache.org/confluence/display/KAFKA/KIP-866) | ZooKeeper-to-KRaft migration | ⛔ |

### Admin, configs & topics

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-4](https://cwiki.apache.org/confluence/display/KAFKA/KIP-4) | Admin protocol foundation (Metadata / Create / Delete topics) | ✅ |
| [KIP-133](https://cwiki.apache.org/confluence/display/KAFKA/KIP-133) | Describe & Alter Configs | ✅ |
| [KIP-195](https://cwiki.apache.org/confluence/display/KAFKA/KIP-195) | `CreatePartitions` | ✅ |
| [KIP-226](https://cwiki.apache.org/confluence/display/KAFKA/KIP-226) | Dynamic broker configuration | ✅ |
| [KIP-339](https://cwiki.apache.org/confluence/display/KAFKA/KIP-339) | `IncrementalAlterConfigs` | ✅ |
| [KIP-525](https://cwiki.apache.org/confluence/display/KAFKA/KIP-525) | Return configs in `CreateTopics` response | ✅ |
| [KIP-554](https://cwiki.apache.org/confluence/display/KAFKA/KIP-554) | Broker-side SCRAM config API | ✅ |
| [KIP-700](https://cwiki.apache.org/confluence/display/KAFKA/KIP-700) | `DescribeCluster` API | ✅ |
| [KIP-516](https://cwiki.apache.org/confluence/display/KAFKA/KIP-516) | Topic identifiers | ⚠️ |
| [KIP-430](https://cwiki.apache.org/confluence/display/KAFKA/KIP-430) | Authorized operations in describe responses | ✅ |
| [KIP-664](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664) | `DescribeProducers` / `ListTransactions` / `DescribeTransactions` admin APIs | ✅ |
| [KIP-185](https://cwiki.apache.org/confluence/display/KAFKA/KIP-185) | `UnregisterBroker` admin API | ✅ |
| [KIP-966](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966) | `DescribeTopicPartitions` admin API | ✅ |

### Security & authentication

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-12](https://cwiki.apache.org/confluence/display/KAFKA/KIP-12) | SSL & SASL/Kerberos | ✅ |
| [KIP-43](https://cwiki.apache.org/confluence/display/KAFKA/KIP-43) | SASL mechanism negotiation | ✅ |
| [KIP-84](https://cwiki.apache.org/confluence/display/KAFKA/KIP-84) | SASL/SCRAM | ✅ |
| [KIP-152](https://cwiki.apache.org/confluence/display/KAFKA/KIP-152) | SASL authentication failure diagnostics | ✅ |
| [KIP-255](https://cwiki.apache.org/confluence/display/KAFKA/KIP-255) | SASL/OAUTHBEARER | ✅ |
| [KIP-368](https://cwiki.apache.org/confluence/display/KAFKA/KIP-368) | Periodic SASL re-authentication | ✅ |
| [KIP-48](https://cwiki.apache.org/confluence/display/KAFKA/KIP-48) | Delegation token support | ✅ |
| [KIP-373](https://cwiki.apache.org/confluence/display/KAFKA/KIP-373) | Delegation tokens for other users | ✅ |

### Authorization (ACLs)

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-11](https://cwiki.apache.org/confluence/display/KAFKA/KIP-11) | Authorization interface | ✅ |
| [KIP-140](https://cwiki.apache.org/confluence/display/KAFKA/KIP-140) | ACL admin APIs (Create / Delete / Describe) | ✅ |
| [KIP-290](https://cwiki.apache.org/confluence/display/KAFKA/KIP-290) | Prefixed ACLs | ✅ |
| [KIP-504](https://cwiki.apache.org/confluence/display/KAFKA/KIP-504) | New Java authorizer API | ✅ |
| [KIP-801](https://cwiki.apache.org/confluence/display/KAFKA/KIP-801) | KRaft-native `StandardAuthorizer` | ✅ |

### Quotas & throttling

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-13](https://cwiki.apache.org/confluence/display/KAFKA/KIP-13) | Quota design (byte-rate) | ✅ |
| [KIP-124](https://cwiki.apache.org/confluence/display/KAFKA/KIP-124) | Request-rate quotas | ✅ |
| [KIP-546](https://cwiki.apache.org/confluence/display/KAFKA/KIP-546) | Client quota admin APIs | ✅ |
| [KIP-599](https://cwiki.apache.org/confluence/display/KAFKA/KIP-599) | Controller mutation quotas | ✅ |
| [KIP-612](https://cwiki.apache.org/confluence/display/KAFKA/KIP-612) | Connection-creation-rate (IP) quotas | ✅ |

### Observability, queues & streams

| KIP | Title | Status |
|-----|-------|:------:|
| [KIP-714](https://cwiki.apache.org/confluence/display/KAFKA/KIP-714) | Client metrics & observability push | ⚠️ |
| [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932) | Queues for Kafka (share groups) | ❌ |
| [KIP-1071](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1071) | Streams rebalance protocol | ❌ |

## Published crates

- [`crabka-compression`](https://crates.io/crates/crabka-compression) — Kafka wire-protocol compression codecs ([docs](https://docs.rs/crabka-compression)).
- [`crabka-protocol`](https://crates.io/crates/crabka-protocol) — Apache Kafka wire-protocol codec ([docs](https://docs.rs/crabka-protocol)).

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
