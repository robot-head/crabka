# Crabka

[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/robot-head/crabka?utm_source=badge)
[![codecov](https://codecov.io/gh/robot-head/crabka/graph/badge.svg?token=EU56CZE3DU)](https://codecov.io/gh/robot-head/crabka)

![Crabka Logo](docs/crabka-text-wide.png)

A Rust reimplementation of [Apache Kafka](https://kafka.apache.org), distributed under the
Apache License 2.0 as a derivative work.

This repository hosts the [`crabka-protocol`](crates/protocol) crate. Other components
(broker, clients, KRaft, etc.) will arrive in their own crates over time. See the design
spec for the full roadmap.

## Status

Pre-1.0, pre-alpha. No production use.

### Slices delivered

- **Slice 1** — `crabka-protocol`: wire-protocol codec, JVM-differential
  tested.
- **Slice 2** — `crabka-client-core`: connection pool, API-version
  negotiation, request dispatch.
- **Slice 3** — `crabka-log`: Apache Kafka byte-compatible segments,
  indexes, retention.
- **Slice 4** — single-node broker MVP: Produce/Fetch/Metadata/CreateTopics
  over TCP. JVM clients connect, produce, and consume.
- **Slice 5** — consumer groups + coordinator: `__consumer_offsets`,
  OffsetCommit, OffsetFetch, group rebalance.
- **Slice 6** — idempotent producer: `InitProducerId`, per-(producer_id,
  epoch, sequence) dedup.
- **Slice 7** — KRaft / metadata quorum: openraft-backed controller,
  metadata image, CreateTopics through quorum.
- **Slice 8** — replication: multi-broker clusters, follower Fetch loop,
  rf-aware leader/follower roles. Deferred: HW, acks=all, leader
  election, KIP-101 (slice 10).
- **Slice 9** — transactions: KIP-98 + full KIP-1319 v2. TxnCoordinator,
  `__transaction_state`, per-segment `.txnindex`, LSO, transactional
  producer + consumer `isolation_level=read_committed`.
- **Slice 10a** — bulletproof EOS (HW + acks=all): partition-leader HW
  tracking; `acks=all` Produces block until full-ISR replication;
  consumer Fetch + `read_committed` LSO clamped at HW. Slice 10b will
  add KIP-101 leader-epoch, leader-election-on-failure, and ISR
  shrink/expand.
- **Slice 10b** — bulletproof EOS complete: KIP-101 leader-epoch
  fencing; leader election on broker death (BrokerHeartbeat-driven);
  ISR shrink/expand via AlterPartition. A 3-broker cluster survives
  partition-leader crashes and slow followers; `acks=all` produces
  complete after election; zombie writes from fenced ex-leaders are
  rejected.
- **Slice 11** — admin handlers: `AlterConfigs` /
  `IncrementalAlterConfigs` (with live propagation to `Log.config`),
  `CreatePartitions`, `DeleteRecords`, `ListGroups`, `DescribeGroups`,
  `DeleteGroups`, `DescribeCluster`. Validated end-to-end against the
  JVM `kafka-*.sh` operator tooling.

## Published crates

- [`crabka-compression`](https://crates.io/crates/crabka-compression) — Kafka wire-protocol compression codecs ([docs](https://docs.rs/crabka-compression)).
- [`crabka-protocol`](https://crates.io/crates/crabka-protocol) — Apache Kafka wire-protocol codec ([docs](https://docs.rs/crabka-protocol)).

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
