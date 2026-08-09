# Crabka ↔ Apache Kafka KIP implementation matrix

This document tracks Crabka's implementation status against the Apache Kafka
Improvement Proposals (KIPs) that define Kafka's compatibility surface: the wire
protocol, message format, storage, replication, KRaft metadata quorum, security,
authorization, quotas, admin APIs, queues (share groups), and the streams
rebalance protocol.

It complements the higher-level feature tables in
[`README.md`](../README.md#feature-compatibility); where the two disagree, the
README's differential-tested matrix is authoritative and this file should be
corrected to match.

**Target surface.** Apache Kafka 4.x message schemas. The wire codec is
generated from, and validated against, the Kafka schema corpus
(`crates/protocol-codegen` validates the 4.2 corpus, 197 schema files; the
README targets the 4.3.0 schemas). Encode/decode is checked byte-for-byte
against `kafka-clients`, and a JVM acceptance suite drives the official
`cp-kafka` / `apache/kafka` admin tools against a live Crabka broker.

**Legend:** ✅ fully implemented · ⚠️ partial (gap noted) · ❌ in scope but not
yet implemented · ⛔ out of scope by design.

**Scope honesty.** Kafka has ~1300 KIP *numbers*, but a large fraction are
unassigned, withdrawn/discarded, folded into another KIP, or purely
JVM-client-library / Kafka Connect / Kafka Streams-library internal with no
broker or wire surface. This matrix is exhaustive over the KIPs that define
Crabka's actual compatibility contract; the rest are handled categorically in
[§4](#4-deliberately-out-of-scope-) and [§7](#7-the-long-tail). Numbers are not
invented to pad a one-row-per-integer table.

---

## 1. Fully implemented (✅)

### Wire protocol & message format

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-31 | Relative offsets in compressed message sets | `records-legacy`, `log` |
| KIP-32 | Add timestamps to messages | README |
| KIP-74 | Fetch response size limit (`max_bytes` / `partition_max_bytes`) | fetch handler honors limits |
| KIP-82 | Add record headers | README |
| KIP-110 | Zstandard compression codec (message format v2 only) | `compression` |
| KIP-219 | Improve quota communication (throttle-then-respond) | dispatch loop patches leading `ThrottleTimeMs` |
| KIP-227 | Incremental fetch sessions | fetch-session cache + forget/merge model |
| KIP-394 | Require `member.id` for initial JoinGroup | README |
| KIP-464 | `CreateTopics` with broker-default partitions / replication factor | schema `CreateTopicsRequest` v4 |
| KIP-467 | Augmented `ProduceResponse` per-record errors | schema `ProduceResponse` `RecordErrors` |
| KIP-482 | Optional tagged fields (flexible versions) | README |
| KIP-511 | Collect & expose client software name / version | README |
| KIP-559 | Protocol-type / name on coordination responses (L7-proxy friendly) | README |
| KIP-734 | `ListOffsets` `MAX_TIMESTAMP` (`-3`) | list_offsets handler |
| KIP-903 | Fence stale-broker-epoch replicas from the ISR | README, kip903 spec |
| KIP-951 | Leader-discovery hint (current leader in Produce/Fetch) | produce handler |

### Producer — idempotence, transactions & EOS

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-98 | Exactly-once delivery & transactional messaging | txn coordinator |
| KIP-360 | Reliable idempotent / transactional producer (safe epoch bump) | README |
| KIP-447 | Producer scalability for EOS | kip447 spec |
| KIP-480 | Sticky partitioner (client) | README |
| KIP-679 | Idempotence on by default (client) | README |
| KIP-794 | Strictly-uniform sticky partitioner (client) | README |
| KIP-890 | Transactions server-side defense (`transaction.version=2`) | feature-pins note (byte-verified) |
| KIP-915 | Txn/group coordinator record flexible-version downgrade foundation | txn log v1, feature-pins note |
| KIP-1228 | Transaction version on `WriteTxnMarkers` | README |

### Consumer groups & queues

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-62 | Background-thread heartbeat (session vs poll timeout) | README |
| KIP-345 | Static membership | static-membership stateright model |
| KIP-429 | Cooperative incremental rebalance protocol | README |
| KIP-496 | `OffsetDelete` admin API | README |
| KIP-518 | List groups by state (`StatesFilter` / `GroupState`) | schema `ListGroupsRequest` v4 |
| KIP-699 | Batched `FindCoordinator` | schema `FindCoordinator` v4 |
| KIP-800 | `Reason` field on Join/Leave group | schema Join/LeaveGroup |
| KIP-848 | Next-generation consumer rebalance protocol (+ live classic↔next-gen migration) | specs 64a–64e |
| KIP-1043 | Admin of all group types (`GROUP_ID_NOT_FOUND`) | schema `DescribeGroups` v6 |
| KIP-1082 | Client-generated member ID (`ConsumerGroupHeartbeat`) | schema; KIP-848 path |
| KIP-1099 | `MemberType` in `ConsumerGroupDescribe` | schema |
| KIP-932 | Queues for Kafka / share groups | share-group specs + model |
| KIP-1206 / KIP-1222 | ShareFetch `ShareAcquireMode` / `Renew` acknowledgement | schema `ShareFetch` v2; share-group slice F |
| KIP-1226 | Share-group lag | share-group slice F |
| KIP-1319 | Member-epoch fencing (next-gen / txn coordinator) | txn coordinator |

### Storage & log

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-63 | Streams record cache / changelog dedup | streams state-store spec |
| KIP-112 | Handle disk failure for JBOD | kip-112 spec |
| KIP-113 | Replica movement between log dirs (`AlterReplicaLogDirs`) | README |
| KIP-204 | `DeleteRecords` via the Admin client | README |
| KIP-405 | Tiered storage (topic-backed RLMM default; copy/read/retention; RLMM snapshots; metadata byte-exact with JVM) | specs 48a–48r — *segment-data interop partial, see [§2](#2-partially-implemented--what-is-left)* |
| KIP-534 | Log retention with delete-horizon (tombstone retention) | kip534 spec |
| KIP-1005 | `ListOffsets` last-tiered offset | schema v9 |
| KIP-1023 | `ListOffsets` earliest-pending-upload offset | schema v11 |
| KIP-1075 | Async remote `ListOffsets` | schema v10 |

### Replication & availability

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-36 | Rack-aware replica assignment | rebalancer specs |
| KIP-73 | Replication quotas (throttled replication) | token-bucket stateright model |
| KIP-101 | Leader-epoch-based truncation | README |
| KIP-207 | Monotonic `ListOffsets` across leader change | data-path model |
| KIP-279 | Fix leader/follower log divergence | README |
| KIP-320 | Detect & handle log truncation (leader epoch in Fetch) | kip-320 spec |
| KIP-392 | Fetch from closest replica (rack-aware) | kip-392 spec |
| KIP-455 | `AlterPartitionReassignments` / `ListPartitionReassignments` | README |
| KIP-460 | Admin `ElectLeaders` (PREFERRED + UNCLEAN) | README |
| KIP-497 | Inter-broker `AlterPartition` (AlterIsr) | ISR state model |
| KIP-704 | Leader-recovery-state hint in `AlterPartition` | unclean-recovery path |
| KIP-841 | Fence stale-epoch replicas / unclean-recovery toggle | README |
| KIP-858 | JBOD in KRaft (`PartitionRecord.Directories`) | partition record v1 |
| KIP-966 | Eligible leader replicas / offset-aware unclean recovery; `DescribeTopicPartitions` | kip966 spec |
| KIP-996 | Pre-vote | kip-996 spec |

### KRaft metadata quorum

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-500 | Replace ZooKeeper with a self-managed metadata quorum | README |
| KIP-584 | Feature versioning (`metadata` / `group` / `transaction.version`) | feature-versioning framework |
| KIP-595 | Raft protocol for the metadata quorum (wire) | kip595-* specs |
| KIP-630 | Kafka Raft snapshot + `FetchSnapshot` | kip630 spec |
| KIP-631 | Quorum-based controller (metadata records, RPCs) | kip631 spec |
| KIP-836 | `DescribeQuorum` voter-lag timestamps | schema v1 |
| KIP-853 | Dynamic KRaft voters (Add/Remove/UpdateRaftVoter) | deterministic Raft model + snapshot recovery + operator lifecycle tests; Kafka 4.3.1 `kafka-features` and `kafka-metadata-quorum` oracle |
| KIP-919 | AdminClient ↔ controller routing (`DescribeCluster` `EndpointType`; controller registration; `UnregisterBroker`) | schema `DescribeCluster` v1; api_key 64 |
| KIP-1022 | Formatting & updating features (`crabka format --feature`) | JVM `kafka-features` validated |
| KIP-1073 | `IncludeFencedBrokers` / `IsFenced` in `DescribeCluster` | schema v2 |

### Admin, configs & topics

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-4 | Admin protocol foundation | README |
| KIP-133 | Describe & Alter Configs | README |
| KIP-195 | `CreatePartitions` | README |
| KIP-226 | Dynamic broker configuration | README |
| KIP-339 | `IncrementalAlterConfigs` | README |
| KIP-430 | Authorized operations in describe responses | README |
| KIP-516 | Topic identifiers | kip-516 spec |
| KIP-525 | Return configs in `CreateTopics` response | README |
| KIP-664 | `DescribeProducers` / `ListTransactions` / `DescribeTransactions` | README |
| KIP-700 | `DescribeCluster` API | README |
| KIP-827 | `DescribeLogDirs` total / usable bytes (v4) | describe_log_dirs handler |
| KIP-919 | `UnregisterBroker` admin API (api_key 64) | dispatch + handler *(repo previously mislabeled this KIP-185; see [§6](#6-attribution-caveat))* |
| KIP-994 | `ListTransactions` v1 minor additions | schema |
| KIP-1142 | `ListConfigResources` admin API | list_config_resources handler |
| KIP-1152 | `ListTransactions` `TransactionalIdPattern` | schema v2 |

### Security & authentication

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-11 | Authorization interface | README |
| KIP-12 | SSL & SASL/Kerberos | README |
| KIP-43 | SASL mechanism negotiation | README |
| KIP-48 / KIP-373 | Delegation tokens (+ for other users) | specs 51 / 51b |
| KIP-84 | SASL/SCRAM | README |
| KIP-140 | ACL admin APIs (Create / Delete / Describe) | README |
| KIP-152 | SASL authentication-failure diagnostics | README |
| KIP-255 | SASL/OAUTHBEARER | README |
| KIP-290 | Prefixed ACLs | README |
| KIP-368 | Periodic SASL re-authentication | spec 49e |
| KIP-504 | New Java authorizer API (semantics) | README |
| KIP-554 | Broker-side SCRAM config API | slices 12 / 17a |
| KIP-768 | OAUTHBEARER OIDC (JWKS / signed-JWT / introspection) | `security/oauthbearer`, operator-e2e interop |
| KIP-801 | KRaft-native `StandardAuthorizer` | `authz` |

### Quotas & throttling

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-13 | Quota design (byte-rate) | quota precedence model |
| KIP-124 | Request-rate quotas | README |
| KIP-257 | Configurable quota management | quota module |
| KIP-546 | Client-quota admin APIs | README |
| KIP-599 | Controller mutation quotas | slice 16c |
| KIP-612 | IP / connection-creation-rate quotas | slice 16b |

### Observability & streams DSL/runtime (in the Rust Streams client)

> The streams sub-KIPs below are implemented in `crabka-client-streams`, which
> is itself ⚠️ partial versus the JVM Kafka Streams library (see
> [§2](#2-partially-implemented--what-is-left)). They are listed here because the
> individual DSL/runtime features exist and are golden-tested against JVM
> capture.

| KIP | Title | Grounding |
|-----|-------|-----------|
| KIP-714 | Client metrics & observability push | kip-714 spec |
| KIP-1000 | List client-metrics configuration resources | kip-714 spec |
| KIP-129 | Streams exactly-once semantics | streams EOS |
| KIP-150 / KIP-213 | Cogroup / KTable foreign-key join | streams DSL |
| KIP-328 / KIP-825 | Suppress / emit-final (`EmitStrategy`) | streams DSL |
| KIP-401 / KIP-444 | Streams `stores()` auto-connect / metrics | streams specs |
| KIP-450 | Sliding-window aggregations | streams DSL |
| KIP-617 / KIP-796 / KIP-960 / KIP-968 | IQv2 (range / versioned / multi-versioned key queries) | streams IQv2 |
| KIP-633 | Drop 24h grace default; stream-time-driven left/outer join emission | streams stream-join |
| KIP-820 | `processValues` fixed-key Processor API | streams |
| KIP-889 / KIP-914 / KIP-962 | Versioned state stores / DSL semantics / relax non-null key | streams |
| KIP-923 | Grace period on stream-table join | streams |
| KIP-1024 | `statestore.cache.max.bytes` | streams record-caching spec |

---

## 2. Partially implemented (⚠️) — what is left

| KIP / area | Done | What's left for full parity |
|------------|------|-----------------------------|
| **KIP-778** — KRaft-to-KRaft upgrades | `metadata.version` level model (7–25), runtime enforcement, bootstrap/format, operator ordered roll + MV bump | Full online **`metadata.version` downgrade** (lossy record-level downgrade) semantics, and JVM-validated mixed-version rolling up/down-grade orchestration. |
| **KIP-939** — 2PC participation | `InitProducerId` v6 `enable2Pc` fully wired: cluster gate (`transaction.two.phase.commit.enable`) + `TWO_PHASE_COMMIT` ACL, 2PC transactions persisted with the no-timeout sentinel and **never** auto-aborted by the idle-transaction reaper (the reaper itself is new; KIP-98 timeout). `keepPreparedTxn` returns `UNSUPPORTED_VERSION` (matches Kafka, where it is still unstable). Safety proven by an exhaustive `stateright` model (`txn::two_pc_model`). | Prepared-txn **retention/recovery** flow (`keepPreparedTxn` → `OngoingTxnProducerId/Epoch`) once Kafka stabilises it; remote-led abort-marker fan-out from the reaper. |
| **KIP-1071** — streams rebalance protocol | **Broker side fully done**: `StreamsGroupHeartbeat` / `StreamsGroupDescribe`, topology ingestion, internal repartition/changelog topic creation, active/standby/warmup assignment with changelog catch-up, `__consumer_offsets` persistence, `streams.version` gate. Rust client DSL/runtime/state-stores/joins/windows/suppress/IQv2/EOS are broad. | (a) `crabka-client-streams` is **not** a full JVM Kafka Streams library replacement; (b) **live classic↔streams group migration is not wired**. |
| **KIP-405** — tiered storage *segment-data* interop | `__remote_log_metadata` records byte-exact with JVM `RemoteLogMetadataSerde`; copy/read/retention; RLMM snapshots | Shared `RemoteStorageManager` object layout + **producer-snapshot upload** not yet validated against JVM `LocalTieredStorageManager`, so segment-level mixing in a mixed JVM+Crabka cluster is not claimed. |
| Operator — Ingress / Route listeners | Internal / NodePort / LoadBalancer listeners done | Ingress / Route external-listener types only partially wired. |

---

## 3. In scope but not yet implemented (❌)

| KIP / area | Note |
|------------|------|
| KIP-899 — AdminClient `--bootstrap-controller` (talk directly to controller quorum) | `DescribeCluster` `endpoint_type=CONTROLLERS` now projects the KRaft voter set (KIP-919 — done). Still pending: the controller listener serving the admin RPC surface, and the client-side `--bootstrap-controller` dial path. |
| KIP-1102 — client re-bootstrap on stale metadata | Native-client robustness item; not implemented. |
| Full JVM **Kafka Streams library** KIPs (e.g. KIP-258, 300, 307, 572, 761, 862, 865, 925, 1106, …) | Covered only insofar as `crabka-client-streams`' DSL needs them; full Streams-library parity is an open frontier. |
| KIP-1150 — diskless / "Inkless" topics | Not in any GA Kafka release; out of near-term parity scope but in scope long-term. |

---

## 4. Deliberately out of scope (⛔)

| KIP(s) / area | Reason |
|---------------|--------|
| KIP-866 + all ZooKeeper-mode / ZK→KRaft migration KIPs (incl. KIP-590 controller forwarding) | **Crabka is KRaft-only.** Explicit non-goal (`CLAUDE.md`, `KNOWN_ISSUES.md`, README ⛔). Greenfield, no production users, no migration burden. |
| Kafka **Connect** framework + connectors + EOS source + REST/offsets APIs (KIP-26, 145, 158, 208, 215, 238, 298, 305, 558, 610, 611, 618, 745, 875, 980, …) | Crabka provides its own Rust connector SPI, a managed Postgres CDC worker with durable Kafka-backed offsets, and a `KafkaConnector` operator CRD. JVM plugin loading, the distributed Connect worker protocol, the Connect REST API, multi-task execution, initial snapshots, and exactly-once source delivery remain out of scope for this first managed vertical slice. |
| **MirrorMaker 2** / geo-replication (KIP-382, 545, 716, 984, …) | MirrorMaker equivalent not implemented (`KafkaMirrorMaker` CRD ❌). |
| **Kafka Bridge** (HTTP) | Superseded in Crabka by the native gRPC / Connect-RPC + HTTP gateway; `KafkaBridge` CRD ❌. |
| JVM-**client-library-internal** KIPs (e.g. KIP-235/302 DNS bootstrap, KIP-266 consumer block fix, KIP-289 default `group.id`, KIP-421 dynamic client config, KIP-580 client exponential backoff, KIP-91 producer `delivery.timeout.ms`) | Not applicable to a broker. Where relevant, equivalent behavior lives in Crabka's native Rust clients rather than as a tracked broker KIP. |

---

## 5. Wire-level note

Several KIPs in §1 are present as **byte-exact codec support** in
`crates/protocol/schemas/*.json` — every request/response version negotiates and
round-trips against the JVM — even where the broker *handler* semantics lag the
wire. Notable cases where schema support is ahead of full behavior: parts of
the tiered `ListOffsets` variants (KIP-1005 / 1023 / 1075).
Those are flagged in §2/§3 rather than claimed as full feature parity on the
strength of schema presence alone.

---

## 6. Attribution caveat

The repo historically labeled the **`UnregisterBroker`** admin API (api_key 64)
as "KIP-185". Canonical **KIP-185** is *"Make exactly-once in-order delivery per
partition the default producer setting"* — unrelated. The `UnregisterBroker`
*feature itself* is implemented and JVM-validated; only the cited KIP number was
wrong.

This has been corrected throughout the source and README to **KIP-919**
(*"Allow AdminClient to Talk Directly with the KRaft Controller Quorum and add
Controller Registration"*), which is the KIP that adds `unregisterBroker` support
to the AdminClient (Apache JIRA KAFKA-17039). The underlying RPC originates with
the KRaft controller surface (KIP-631). Release-managed `CHANGELOG.md` files were
left untouched as historical record.

---

## 7. The long tail

Kafka has ~1300 KIP *numbers*. This matrix does **not** invent a row per integer,
because a large share are unassigned/never-used, discarded/withdrawn/rejected,
folded into another KIP, or JVM-client / Connect / Streams-library-internal with
no broker or wire surface. Those are covered categorically in §3 (Streams-library
frontier) and §4 (out-of-scope ecosystems and client-library internals). Every
KIP that defines Crabka's actual compatibility contract — protocol, storage,
replication, KRaft, security, authorization, quotas, queues, and the streams
*protocol* — is enumerated in §1–§3 and grounded in the repo.
