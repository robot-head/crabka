# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.3.8] — 2026-06-23


### <!-- 0 -->🚀 Features


- Grafana-Mimir-equivalent metrics & PromQL observability signal ([#642](https://github.com/robot-head/crabka/pull/642))


### <!-- 1 -->🐛 Bug Fixes


- Faster broker failover — retry backoff + startupProbe ([#583](https://github.com/robot-head/crabka/pull/583))

- Seamless KRaft failover — voter hostname re-resolution, submit_change forwarding, SIGTERM controlled shutdown ([#591](https://github.com/robot-head/crabka/pull/591))


### <!-- 2 -->🚜 Refactor


- Migrate whole-function cargo-mutants exclusions to #[mutants::skip] ([#615](https://github.com/robot-head/crabka/pull/615))


### <!-- 4 -->⚡ Performance


- Profiling-driven optimizations (codec decode, broker group-commit, streams changelog) ([#630](https://github.com/robot-head/crabka/pull/630))


### <!-- 6 -->🧪 Testing


- Kill engine-IO mutants with mockall seams, shrink mutants.toml ([#607](https://github.com/robot-head/crabka/pull/607))

- Deterministic heartbeat liveness tests via injectable clock ([#647](https://github.com/robot-head/crabka/pull/647))

## [0.3.7] — 2026-06-17


### <!-- 0 -->🚀 Features


- Emit Cloud Logging-friendly structured JSON across services ([#508](https://github.com/robot-head/crabka/pull/508))

- KIP-1071 streams->classic cold downgrade + admin type-awareness (slice 2) ([#506](https://github.com/robot-head/crabka/pull/506))

- KIP-534 log-compaction retention + fix control-batch dedup data-loss ([#528](https://github.com/robot-head/crabka/pull/528))

- DescribeCluster endpoint_type=CONTROLLERS (KIP-919) + KIP matrix fixes ([#551](https://github.com/robot-head/crabka/pull/551))

- Surface request-quota throttle in ThrottleTimeMs (KIP-219) ([#553](https://github.com/robot-head/crabka/pull/553))

- KIP-939 2PC participation — coordinator semantics + stateright model ([#560](https://github.com/robot-head/crabka/pull/560))

- Native GCS RemoteStorageManager backend (keyless Workload Identity) ([#574](https://github.com/robot-head/crabka/pull/574))


### <!-- 1 -->🐛 Bug Fixes


- Withhold KIP-848 partitions until previous owner revokes (+ stateright model that found it) ([#521](https://github.com/robot-head/crabka/pull/521))

- Token-bucket over-grant/underflow race + stateright interleaving model (KIP-73) ([#531](https://github.com/robot-head/crabka/pull/531))

- Classic group static-membership index corruption + stateright model (KIP-345) ([#534](https://github.com/robot-head/crabka/pull/534))


### <!-- 3 -->📚 Documentation


- Add exhaustive KIP matrix; fix UnregisterBroker KIP-185→919 mislabel ([#549](https://github.com/robot-head/crabka/pull/549))

- Make website coherent and fix links ([#579](https://github.com/robot-head/crabka/pull/579))


### <!-- 6 -->🧪 Testing


- Stateright consensus model + deterministic-sync test infra (Phase 1) ([#511](https://github.com/robot-head/crabka/pull/511))

- De-flake share + group-coordination tests (Phase 2) ([#513](https://github.com/robot-head/crabka/pull/513))

- Stateright model of the share-partition acquisition core (KIP-932) ([#514](https://github.com/robot-head/crabka/pull/514))

- Stateright model of the ISR / replica-state core (no-committed-data-loss) ([#515](https://github.com/robot-head/crabka/pull/515))

- Stateright model of leader-failover + unclean-recovery safety (KIP-841/966) ([#516](https://github.com/robot-head/crabka/pull/516))

- Stateright model of KIP-455 partition-reassignment completion ([#520](https://github.com/robot-head/crabka/pull/520))

- Stateright model of the KIP-98/EOS transaction coordinator ([#523](https://github.com/robot-head/crabka/pull/523))

- Data-plane safety models — idempotent-producer + log-truncation (stateright + proptest) ([#524](https://github.com/robot-head/crabka/pull/524))

- Fetch HWM visibility-window model + de-dup response fields ([#529](https://github.com/robot-head/crabka/pull/529))

- Exhaustive + proptest verification of quota lookup precedence (KIP-13/124/612) ([#535](https://github.com/robot-head/crabka/pull/535))

- KIP-227 fetch-session forget+merge composition model ([#536](https://github.com/robot-head/crabka/pull/536))

- Data-path composition model — end-to-end formal verification (HWM↔truncation↔failover↔visibility) ([#539](https://github.com/robot-head/crabka/pull/539))

- Txn/EOS composition model — read_committed visibility algebra (LSO ↔ HWM clamp ↔ abort filter) ([#541](https://github.com/robot-head/crabka/pull/541))

- Consumer-group composition model — reconciliation ↔ real OffsetCommit epoch fence ([#543](https://github.com/robot-head/crabka/pull/543))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- De-hardcode release versions + sign/attest published Helm charts ([#530](https://github.com/robot-head/crabka/pull/530))

## [0.3.6] — 2026-06-13

## [0.3.5] — 2026-06-12


### <!-- 0 -->🚀 Features


- KIP-1071 classic→streams group cold upgrade (slice 1) ([#496](https://github.com/robot-head/crabka/pull/496))


### <!-- 1 -->🐛 Bug Fixes

- `__consumer_offsets` bootstrap is now leader-gated: only the controller leader
  registers the topic, and followers wait for it to replicate. Previously every
  broker raced to create it, and concurrent boots appended two conflicting
  `TopicRecord`s (different random `topic_id`) plus per-node `PartitionRecord`s
  to the metadata log — which fatal-faulted any JVM controller replicating that
  far (`duplicate TopicRecord ... with a different ID`).

## [0.1.1] — 2026-05-29


### <!-- 0 -->🚀 Features


- S3-compatible RemoteStorageManager backend (KIP-405) ([#215](https://github.com/robot-head/crabka/pull/215))

- S3 multipart upload for large segments ([#224](https://github.com/robot-head/crabka/pull/224))

- KafkaMetadataEventLog adapter + broker config knob ([#222](https://github.com/robot-head/crabka/pull/222))

- Wire TopicBasedRemoteLogMetadataManager into Broker::start ([#227](https://github.com/robot-head/crabka/pull/227))

- DescribeLogDirs v4 totalBytes/usableBytes via statvfs (KIP-827) ([#229](https://github.com/robot-head/crabka/pull/229))

- Authorized-operations bitfield on describe responses (KIP-430) ([#231](https://github.com/robot-head/crabka/pull/231))

- JBOD offline-log-dir detection at startup ([#232](https://github.com/robot-head/crabka/pull/232))

- Validate KIP-511 client info and expose Prometheus counter ([#234](https://github.com/robot-head/crabka/pull/234))

- Runtime offline-dir detection on partition write/fsync failure ([#236](https://github.com/robot-head/crabka/pull/236))

- KIP-559 protocol_type/name on group-coordination responses ([#237](https://github.com/robot-head/crabka/pull/237))

- DescribeTopicPartitions admin API (KIP-966) ([#241](https://github.com/robot-head/crabka/pull/241))

- Honor operator-supplied assignments in CreatePartitions ([#245](https://github.com/robot-head/crabka/pull/245))

- KIP-714 client telemetry handshake (no-op subscription) ([#246](https://github.com/robot-head/crabka/pull/246))

- KIP-841 unclean.leader.election.enable topic config ([#247](https://github.com/robot-head/crabka/pull/247))

- DescribeProducers admin API (KIP-664) ([#252](https://github.com/robot-head/crabka/pull/252))

- Enforce min.insync.replicas on Produce acks=-1 ([#251](https://github.com/robot-head/crabka/pull/251))

- ListTransactions + DescribeTransactions admin APIs (KIP-664) ([#255](https://github.com/robot-head/crabka/pull/255))

- ListConfigResources admin API (KIP-1142) ([#256](https://github.com/robot-head/crabka/pull/256))

- DescribeQuorum admin API (KIP-595) ([#258](https://github.com/robot-head/crabka/pull/258))

- UnregisterBroker admin API (KIP-185) ([#259](https://github.com/robot-head/crabka/pull/259))

- Expose real raft metrics in DescribeQuorum (KIP-595 follow-up) ([#264](https://github.com/robot-head/crabka/pull/264))

- Rack-aware UniformAssignor (KIP-848 slice 64b) ([#266](https://github.com/robot-head/crabka/pull/266))

- KIP-584 read-side ApiVersions feature surface ([#263](https://github.com/robot-head/crabka/pull/263))

- KIP-848 subscribed_topic_regex support (slice 64a follow-up) ([#268](https://github.com/robot-head/crabka/pull/268))

- Persist subscribed_topic_regex on MemberMetadataValue ([#270](https://github.com/robot-head/crabka/pull/270))

- KIP-584 UpdateFeatures write path (api_key 57) ([#281](https://github.com/robot-head/crabka/pull/281))

- Topic-backed RLMM loopback integration test + produce v13 fix (slice 48f) ([#282](https://github.com/robot-head/crabka/pull/282))

- KIP-853 dynamic KRaft quorum reconfiguration ([#290](https://github.com/robot-head/crabka/pull/290))

- KIP-392 fetch-from-follower / rack-aware reads ([#291](https://github.com/robot-head/crabka/pull/291))

- Vertical role separation — process.roles + true observer metadata fetch ([#292](https://github.com/robot-head/crabka/pull/292))

- KRaft metadata snapshots (KIP-630) ([#287](https://github.com/robot-head/crabka/pull/287))

- KIP-966 offset-aware unclean leader recovery ([#294](https://github.com/robot-head/crabka/pull/294))

- SASL/GSSAPI (Kerberos) authentication ([#295](https://github.com/robot-head/crabka/pull/295))

- Expose SASL/GSSAPI (Kerberos) through broker config + operator CRDs ([#306](https://github.com/robot-head/crabka/pull/306))

- Broker runtime metadata.version enforcement ([#307](https://github.com/robot-head/crabka/pull/307))


### <!-- 1 -->🐛 Bug Fixes


- Install rustls CryptoProvider in ReqwestIntrospectionClient::new ([#207](https://github.com/robot-head/crabka/pull/207))

- Eliminate cooperative-sticky rebalance metadata races ([#286](https://github.com/robot-head/crabka/pull/286))

- Keep catching-up replicas in per_follower so ISR re-admission survives image churn ([#308](https://github.com/robot-head/crabka/pull/308))


### <!-- 10 -->💼 Other


- Crates/broker — oauthbearer_jwks_tls_trust config field

- Incremental fetch sessions ([#208](https://github.com/robot-head/crabka/pull/208))

- Introduce RecordsPayload as the wire-field type for `records` ([#214](https://github.com/robot-head/crabka/pull/214))

- Up-convert v0/v1 MessageSet payloads to v2 RecordBatch ([#219](https://github.com/robot-head/crabka/pull/219))

- Replication_bytes_in/out per-partition counters ([#240](https://github.com/robot-head/crabka/pull/240))

- Tiered_storage_rlmm_topic_backed gauge (slice 48l) ([#243](https://github.com/robot-head/crabka/pull/243))

- Produce/fetch message-conversion counters (slice 12g) ([#250](https://github.com/robot-head/crabka/pull/250))

- Unclean_leader_elections_total counter (slice 10c) ([#253](https://github.com/robot-head/crabka/pull/253))

- Partitions_total + under_replicated_partitions gauges (slice 8b) ([#254](https://github.com/robot-head/crabka/pull/254))

- Under_min_isr_partition_count + offline_partitions_count gauges (slice 8c) ([#257](https://github.com/robot-head/crabka/pull/257))

- Api_requests counter family per Kafka API (slice 12h) ([#261](https://github.com/robot-head/crabka/pull/261))

- Unsupported_api_requests_total counter (slice 12i) ([#262](https://github.com/robot-head/crabka/pull/262))

- Controller_leader_changes_total counter (slice 7c) ([#265](https://github.com/robot-head/crabka/pull/265))

- Messages_in_total per-topic counter (slice 12j) ([#271](https://github.com/robot-head/crabka/pull/271))

- Failed_produce/fetch_requests counters (slice 12k) ([#272](https://github.com/robot-head/crabka/pull/272))

- Zerocopy OffsetIndex/TimeIndex parsing ([#279](https://github.com/robot-head/crabka/pull/279))


### <!-- 3 -->📚 Documentation


- Zola documentation site on GitHub Pages with generated Operator & Broker API references ([#324](https://github.com/robot-head/crabka/pull/324))


### <!-- 6 -->🧪 Testing


- JVM acceptance test for kafka-consumer-groups --delete-offsets (KIP-496) ([#211](https://github.com/robot-head/crabka/pull/211))

- MinIO-backed JVM acceptance test for S3 tiered storage ([#218](https://github.com/robot-head/crabka/pull/218))

- Fix AddrInUse flake in raft_sasl via pre-bound controller listener ([#284](https://github.com/robot-head/crabka/pull/284))

- Close v0/v1 down-conversion coverage gaps ([#285](https://github.com/robot-head/crabka/pull/285))


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

- Release v0.1.0 ([#52](https://github.com/robot-head/crabka/pull/52))


### <!-- 8 -->🛡️ Security


- Crates/broker — wire jwks_tls_trust into JwksRefresher

- SASL successful/failed authentication counters (slice 12l) ([#278](https://github.com/robot-head/crabka/pull/278))

## [0.1.0] — 2026-05-12


### <!-- 7 -->⚙️ Miscellaneous Tasks


- Release v0.1.0 ([#49](https://github.com/robot-head/crabka/pull/49))

