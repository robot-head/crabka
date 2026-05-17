<p align="center">
  <img src="docs/crabka-text-wide.png" alt="Crabka" width="480">
</p>

<p align="center">
  <a href="https://codspeed.io/robot-head/crabka?utm_source=badge"><img src="https://img.shields.io/endpoint?url=https://codspeed.io/badge.json" alt="CodSpeed"></a>
  <a href="https://codecov.io/gh/robot-head/crabka"><img src="https://codecov.io/gh/robot-head/crabka/graph/badge.svg?token=EU56CZE3DU" alt="codecov"></a>
</p>

# Crabka

**Crabka is a Rust reimplementation of [Apache Kafka](https://kafka.apache.org).**

It speaks the Apache Kafka wire protocol byte-for-byte, stores data in Kafka-compatible
log segments, runs its metadata quorum on KRaft, and integrates cleanly with the
standard JVM tooling — `kafka-topics.sh`, `kafka-configs.sh`, `kafka-acls.sh`,
`kafka-consumer-groups.sh`, `kafka-leader-election.sh`, `kafka-reassign-partitions.sh`,
and the official Java client. Existing producers, consumers, and operator workflows
work against a Crabka broker without modification.

Distributed under the Apache License 2.0 as a derivative work.

## Why Crabka

- **Drop-in protocol compatibility.** Crabka is validated against the JVM Kafka
  client via differential byte-equality tests, not against a hand-rolled spec.
- **Memory-safe, fearlessly concurrent.** Written in async Rust on `tokio`, with
  no JVM and no GC pauses.
- **Single static binary.** No JDK, no ZooKeeper, no separate controller process.
- **KRaft-native.** Metadata lives in an `openraft`-backed quorum from day one.
- **Modern crypto.** TLS via `rustls`; SASL/SCRAM-SHA-512 and SASL/PLAIN out of
  the box.

## Architecture

Crabka is organized as a Rust workspace:

| Crate | Role |
|-------|------|
| [`crabka-protocol`](crates/protocol) | Kafka wire-protocol codec (codegen-driven from message schemas) |
| [`crabka-compression`](crates/compression) | Kafka-compatible compression codecs |
| [`crabka-log`](crates/log) | Byte-compatible log segments, indexes, retention |
| [`crabka-metadata`](crates/metadata) | KRaft metadata image, records, replicas |
| [`crabka-raft`](crates/raft) | Controller quorum on top of `openraft` |
| [`crabka-security`](crates/security) | TLS, SASL/PLAIN, SASL/SCRAM-SHA-512 |
| [`crabka-broker`](crates/broker) | Broker runtime: handlers, replication, coordinators |
| [`crabka-client-core`](crates/client-core) | Client connection pool + API-version negotiation |
| [`crabka-client-producer`](crates/client-producer) | Native Rust producer |
| [`crabka-client-consumer`](crates/client-consumer) | Native Rust consumer |
| [`crabka-cli`](crates/cli) | `crabka` binary: `format`, bootstrap, operator commands |

## Feature compatibility

The following table lists Apache Kafka functional surface area and whether Crabka
implements it today.

### Wire protocol & clients

| Feature | Status |
|---------|:------:|
| Wire-protocol byte-exact codec (request / response) | ✅ |
| API version negotiation (`ApiVersions`) | ✅ |
| Flexible / tagged-field versions | ✅ |
| Compression: gzip, snappy, lz4, zstd | ✅ |
| JVM Java client interoperability | ✅ |
| Native Rust producer | ✅ |
| Native Rust consumer | ✅ |
| Broker-side recompression | ❌ |

### Storage

| Feature | Status |
|---------|:------:|
| Byte-compatible log segments | ✅ |
| Offset + time indexes | ✅ |
| Time-based and size-based retention | ✅ |
| Transaction index (`.txnindex`) per segment | ✅ |
| Log compaction (`cleanup.policy=compact`) | ✅ |
| Tiered storage (KIP-405) | ❌ |
| Multiple log directories / KIP-113 log-dir reassignment | ❌ |

### Producer

| Feature | Status |
|---------|:------:|
| `Produce` (acks=0, acks=1, acks=all) | ✅ |
| Idempotent producer (`enable.idempotence=true`) | ✅ |
| `InitProducerId` + per-(pid, epoch, sequence) dedup | ✅ |
| Transactional producer (KIP-98) | ✅ |
| KIP-1319 transactions v2 | ✅ |

### Consumer

| Feature | Status |
|---------|:------:|
| `Fetch` (single + multi-partition) | ✅ |
| Consumer groups + group coordinator | ✅ |
| `__consumer_offsets` topic | ✅ |
| `OffsetCommit` / `OffsetFetch` | ✅ |
| Group rebalance protocol | ✅ |
| `isolation.level=read_committed` (LSO clamping) | ✅ |
| KIP-848 next-gen consumer group protocol | ❌ |
| Static membership (KIP-345) | ❌ |

### Replication & durability

| Feature | Status |
|---------|:------:|
| Multi-broker replication, follower Fetch loop | ✅ |
| In-Sync Replica (ISR) tracking | ✅ |
| ISR shrink / expand via `AlterPartition` | ✅ |
| High-watermark tracking | ✅ |
| `acks=all` blocks until full-ISR replication | ✅ |
| KIP-101 leader-epoch fencing | ✅ |
| Automatic leader election on broker death | ✅ |
| `ElectLeaders` API (KIP-460, PREFERRED + UNCLEAN) | ✅ |
| Auto preferred-replica rebalance | ✅ |
| `AlterPartitionReassignments` / `ListPartitionReassignments` (KIP-455) | ✅ |
| KIP-73 throttled replication | ✅ |
| KIP-841 force-elect / unclean recovery toggle | ❌ |

### Metadata quorum (KRaft)

| Feature | Status |
|---------|:------:|
| KRaft controller quorum (raft-based) | ✅ |
| Metadata image + delta apply | ✅ |
| Controller bootstrap via `crabka format` | ✅ |
| ZooKeeper mode | ❌ (won't implement — KRaft only) |

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
| SASL/SCRAM-SHA-512 | ✅ |
| Per-listener protocol multiplexing (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL) | ✅ |
| Inter-broker auth (TLS + SASL on data plane & raft) | ✅ |
| `AlterUserScramCredentials` (KIP-554) | ✅ |
| `DescribeUserScramCredentials` (KIP-554) | ✅ |
| SASL/SCRAM-SHA-256 | ❌ |
| mTLS client authentication | ❌ |
| SASL/OAUTHBEARER | ❌ |
| SASL/GSSAPI (Kerberos) | ❌ |
| Delegation tokens | ❌ |

### Authorization

| Feature | Status |
|---------|:------:|
| ACL authorizer (Topic / Group / Cluster / TransactionalId) | ✅ |
| `Literal` + `Prefixed` pattern matching | ✅ |
| `Allow` + `Deny` rules, DENY-wins, deny-by-default | ✅ |
| Operation implications (`Read`/`Write`/`Delete`/`Alter` → `Describe`) | ✅ |
| `CreateAcls` / `DeleteAcls` / `DescribeAcls` | ✅ |
| Multiple super-users (`super.users`-style) | ✅ |
| IPv4 host filter | ✅ |
| IPv6 host filter | ❌ |
| ACL audit log sinks beyond `tracing` | ❌ |

### Observability

| Feature | Status |
|---------|:------:|
| Structured logging via `tracing` | ✅ |
| Metrics / JMX-equivalent exporter | ❌ |
| Distributed tracing integration | ❌ |

### Quotas

| Feature | Status |
|---------|:------:|
| Client quotas (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`) | ✅ |
| `AlterClientQuotas` / `DescribeClientQuotas` (KIP-13 + KIP-124 + KIP-257) | ✅ |
| User + (user, client-id) tuple + default entity scopes | ✅ |
| IP entity + `connection_creation_rate` (KIP-612) | ✅ |
| Controller mutation rate (KIP-599) | ✅ |

### Ecosystem (out of broker core)

| Feature | Status |
|---------|:------:|
| Kafka Streams equivalent | ❌ |
| Kafka Connect equivalent | ❌ |
| MirrorMaker equivalent | ❌ |
| Schema Registry | ❌ |

## Published crates

- [`crabka-compression`](https://crates.io/crates/crabka-compression) — Kafka wire-protocol compression codecs ([docs](https://docs.rs/crabka-compression)).
- [`crabka-protocol`](https://crates.io/crates/crabka-protocol) — Apache Kafka wire-protocol codec ([docs](https://docs.rs/crabka-protocol)).

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
