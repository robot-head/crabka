# Crabka Protocol Foundation — Acceptance Gate Status

Generated: 2026-05-10

## Branch summary

- **Branch:** main
- **Total commits:** 29
- **Head commit:** a3bf25d ci: rust + clippy + fmt matrix and JVM differential job

## Test inventory

| Category       | Tests | Notes                                                                 |
|----------------|-------|-----------------------------------------------------------------------|
| Unit           | 33    | Primitives, error, codec, owned/borrowed message types, tagged fields |
| Proptest       | 3     | ApiVersionsRequest v3, ApiVersionsResponse v0 + v3 round-trips        |
| Differential   | 5     | 4 JVM byte-equality / decode-parity tests + oracle smoke test         |
| Corpus replay  | 1     | `corpus_round_trips` covering 1 hex entry                             |
| Snapshot       | 3     | Owned + borrowed codegen snapshots, snapshot_compiles smoke           |
| **Total**      | **45**|                                                                       |

## Acceptance gate — PASS

| # | Check                                         | Result |
|---|-----------------------------------------------|--------|
| 1 | `cargo fmt --check`                           | PASS   |
| 2 | `cargo clippy --workspace --all-targets`      | PASS   |
| 3 | Default tests (`cargo test --workspace`)      | PASS   |
| 4 | With JVM oracle (`--include-ignored`)         | PASS   |
| 5 | No drift (`regenerate.sh` + `git diff`)       | PASS   |
| 6 | ApiVersionsRequest v0 + v3 byte-equal JVM     | PASS   |
| 7 | ApiVersionsResponse v3 byte-equal + decode    | PASS   |
| 8 | Borrowed flavor exercised                     | PASS   |
| 9 | Corpus replay green (1 entry)                 | PASS   |
|10 | CI matrix ubuntu/macos/windows in ci.yml      | PASS   |
|11 | CONTRIBUTING.md: regenerate, oracle, version  | PASS   |

**Overall: PASS — all 11 acceptance items green.**

## Next step

Extend codegen and tests to the remaining ~99 Kafka message types via the
follow-up `crabka-protocol-coverage` plan.  The protocol-foundation
infrastructure (codegen pipeline, JVM oracle, differential harness, corpus
replay, CI matrix) is fully in place and proven correct against a live JVM
Kafka client for ApiVersions.

## Slice 11 — admin handlers (2026-05-14)

- 8 new handlers: AlterConfigs (33), IncrementalAlterConfigs (44),
  CreatePartitions (37), DeleteRecords (21), DescribeCluster (60),
  ListGroups (16), DescribeGroups (15), DeleteGroups (42).
- 1 new metadata record: V1TopicConfig.
- Topic-config whitelist with live propagation to `Log.config` via
  `Arc<RwLock<LogConfig>>` and a supervisor reconcile push.
- 5 new JVM acceptance tests covering `kafka-configs --alter`,
  `kafka-topics --alter --partitions`, `kafka-delete-records`,
  `kafka-consumer-groups --list/--describe`, `kafka-cluster cluster-id`.
- Side fixes: DescribeConfigs now projects topic_configs (not stub);
  OffsetFetch honors `topics: None` "fetch all" sentinel; flexible-body
  dispatch table + ApiVersions response register all slice-11 api_keys.
- Out of scope: Rust CLI, ACLs, quotas, partition reassignments,
  ElectLeaders, log compaction, broker-side recompression.

## Slice 12 — auth & security (2026-05-15)

- 2 new crates: `crabka-security` (pure-logic SCRAM-SHA-512 server +
  client state machines, PBKDF2 hashing, PLAIN constant-time verifier,
  `rustls` `ServerConfig`/`ClientConfig` builders) and `crabka-cli`
  (`crabka format --add-scram` bootstrap subcommand).
- 3 new wire handlers: `SaslHandshake` (17), `SaslAuthenticate` (36),
  `AlterUserScramCredentials` (51, KIP-554).
- 2 new metadata records: `V1ScramCredential`,
  `V1DeleteScramCredential`.
- Per-listener accept loops; TLS termination per listener via
  `tokio_rustls::TlsAcceptor`; `ConnectionAuth` state machine + pre-auth
  allowlist gate (`ApiVersions`, `SaslHandshake`, `SaslAuthenticate`
  pre-auth on SASL listeners; everything else rejected).
- `InterBrokerClient` runs TLS + outbound SASL handshake for the
  replicator and the controller-heartbeat client. Raft transport gains
  an `OutboundDialer` trait abstraction but stays plaintext in this
  slice (promoting raft RPC onto the unified inter-broker listener is
  deferred — the controller listener itself needs SASL/TLS termination
  first).
- 5 new JVM acceptance tests: SASL/PLAIN produce/consume,
  SASL/SCRAM-SHA-512 produce/consume (provisioning via
  `kafka-configs --alter --entity-type users`), SSL handshake,
  SASL_SSL full stack, two-broker SASL inter-broker.
- Bootstrap CLI: `crabka format --log-dir D --add-scram
  'SCRAM-SHA-512=[name=admin,password=…]'` writes
  `bootstrap.{json,records.bin}` artifacts; the broker doesn't consume
  them yet — wiring `Broker::start` to read them on
  `BootstrapMode::Bootstrap` is deferred to a follow-up slice. The
  super-user-via-static-PLAIN-creds path is the operator bootstrap
  channel today.
- Cert fixtures regenerated as ECDSA P-256 end-entity (was ED25519 with
  `CA:TRUE` — Java 11 in cp-kafka:6.1.1 doesn't negotiate ed25519 in
  TLS).
- Side fix: `UNACCEPTABLE_CREDENTIAL = 78` (plan spec said 74 — that
  value collides with `FENCED_LEADER_EPOCH`).
- Out of scope: ACLs (only a super-user-name stand-in), delegation
  tokens, OAUTHBEARER, GSSAPI, SCRAM-SHA-256, mTLS client-auth,
  quotas, raft-over-SASL.

## Slice 12b — auth cleanup (2026-05-15)

- New `BrokerConfig.controller_listener_protocol` (default
  `Plaintext`). Controller listener terminates TLS + SASL when set
  via a new `RaftListenerHandshake` trait in `crabka-raft`. The
  `crabka-broker::raft_handshake::BrokerRaftHandshake` impl reuses
  slice 12's `network::auth` state machines so the data plane and
  controller share one source of truth for inbound SASL.
- `InterBrokerDialer` (constructed-but-unused in slice 12) is now
  wired into `ControllerConfig::dialer` AND into
  `ControllerHandle::forward_submit_to` (follower→leader
  submit_change forwarding). Both inbound and outbound raft RPC
  honor the controller listener protocol.
- `forward_submit_to` was rewritten to use
  `Connection::raw_request` over a dialer-built `Connection`,
  removing ~50 lines of bespoke framing.
- `Broker::start` consumes `<log_dir>/bootstrap.records.bin`
  (produced by `crabka format --add-scram`) on first start.
  `controller.submit_change(records).await` blocks until applied
  before the broker becomes ready. Corrupt files →
  `BrokerError::BootstrapFile`.
- 3 new no-Docker raft tests (`tests/raft_sasl.rs`); 3 new
  bootstrap-consumption tests; 1 new JVM acceptance test
  (`jvm_inter_broker_sasl_ssl_raft_replication`).
- Out of scope: mTLS, ACLs, per-listener controller-quorum protocol
  mapping, SCRAM rotation under live raft traffic.

## Slice 13 — ACLs (2026-05-15)

- New `crabka-metadata::AclEntry` + `AclEntryFilter` + 4 enums
  (`ResourceType`, `PatternType`, `PermissionType`, `AclOperation`).
  Two new `MetadataRecord` variants: `V1AccessControlEntry`,
  `V1DeleteAccessControlEntry`. `MetadataImage` indexes ACLs by
  `(ResourceType, ResourceName)` for LITERAL and by `ResourceType`
  for PREFIXED.
- New `crabka_broker::authorizer::authorize` — pure-logic Kafka ACL
  decision algorithm with a compatibility shim: zero ACLs AND
  `super_user_name = None` → ALLOW (preserves slice 11/12 test
  behavior unchanged). Once one ACL or a super-user exists,
  deny-by-default kicks in.
- 3 new wire handlers: `DescribeAcls` (29), `CreateAcls` (30),
  `DeleteAcls` (31). Wire `i8` enum discriminants map to/from
  metadata enums via `handlers::acl_wire`. `kafka-acls.sh` works
  end-to-end against cp-kafka:7.5.0.
- 16 existing handlers migrated off the static `HandlerTable` and
  gained an authorize-preamble: `Produce`/`Fetch`/`Metadata`
  (per-topic), `CreateTopics`/`DeleteTopics`/`AlterConfigs`/
  `IncrementalAlterConfigs`/`CreatePartitions`/`DeleteRecords`
  (topic admin), `ListGroups`/`DescribeGroups`/`DeleteGroups`/
  `JoinGroup`/`OffsetCommit`/`OffsetFetch` (group),
  `DescribeCluster`/`AlterUserScramCredentials` (cluster),
  `InitProducerId`/`AddPartitionsToTxn`/`EndTxn`/`TxnOffsetCommit`
  (txn). Slice 12's super-user-name check on
  `AlterUserScramCredentials` was replaced by the proper Cluster
  Alter ACL check; the authorizer's super-user bypass keeps
  slice-12 tests green.
- `crabka format --add-acl` seeds ACL records alongside SCRAM
  credentials in `bootstrap.records.bin`. The slice 12b loader
  reads them on first start.
- 10 new no-Docker integration tests (`tests/acl_handlers.rs`):
  ACL flow (Create/Describe/Delete via super-user + non-super-user
  rejection), Produce/Fetch enforcement, Metadata silent-filter vs
  explicit-deny, JoinGroup gate, InitProducerId txn gate.
- 5 new JVM acceptance tests (cp-kafka:7.5.0):
  `kafka-acls.sh` provision + list + remove; authorized
  produce/consume; unauthorized produce / consumer / prefixed-ACL.
- Side fix: `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` constant was
  declared at value 51 (incorrect); corrected to 53 per Kafka spec.
- Known limitation: Crabka's authorizer does not implement Kafka's
  "Read/Write implies Describe" operation implication. Tests work
  around by seeding explicit Describe ACLs alongside Read/Write —
  production deployments should do the same until an implication
  slice lands.
- Out of scope: delegation tokens, SCRAM-SHA-256 ACL principals,
  IPv6 host filters, audit log destinations beyond `tracing`,
  `allow.everyone.if.no.acl.found=true` toggle (compat shim
  instead), operation implications (Read/Write → Describe).

## Slice 13b — ACL implications + multi-super-user (2026-05-15)

- `crabka_broker::authorizer::matches_operation` calls new `implies`
  helper: `Read`/`Write`/`Delete`/`Alter` on any resource imply
  `Describe`; `AlterConfigs` implies `DescribeConfigs`. One-way table
  (`Describe` does not imply `Read`). Resource-type independent —
  works on `Topic`, `Group`, `Cluster`, and `TransactionalId`.
- `BrokerConfig::super_user_name: Option<String>` renamed to
  `super_users: HashSet<String>`. Authorizer + 22 handler call sites +
  4 test fixture files migrated atomically. Semantically a no-op for
  pre-13b single-/zero-super-user cases. Both `authorize` and
  `authorize_topics` signatures generic over `BuildHasher` per
  clippy's `implicit_hasher` convention (matches the slice-12
  `verify_plain` pattern).
- Authorizer unit tests: 10 new (6 implication matrix, 3 resource-type
  independence on Group/Cluster/TransactionalId, 1 multi-super-user
  bypass).
- Broker integration tests: 3 new in `tests/acl_handlers.rs`
  (`implication_metadata_describes_after_read_acl`,
  `implication_metadata_describes_after_write_acl`,
  `multi_super_user_both_can_provision`).
- Workaround removal: redundant Describe ACL seeds dropped from
  slice-12 SCRAM JVM tests (3 tests, one loop each) and slice-13 ACL
  JVM tests (5 tests, 6 invocations across Topic + Group resources).
  Standard `kafka-acls.sh --add --operation Read|Write|...` now works
  end-to-end without separate Describe grants.
- Out of scope: ACL audit logging, `User:` prefix in super-user
  config strings, `ClusterAction` implication, persisted broker
  config.

## Slice 14 — ElectLeaders + auto-rebalance (2026-05-15)

- Pure-logic `select_new_leader_for_partition` in
  `crates/broker/src/leader_election.rs` computes the new
  `PartitionRecord` for one partition under PREFERRED or UNCLEAN.
  Returns a small `ElectError` enum mapped to wire codes 3/15/80/81/84.
- New `crates/broker/src/handlers/elect_leaders.rs` (api_key 43, KIP-460).
  Cluster Alter authorize gate; per-partition results in the response.
  Inline-intercept dispatch matches the slice-13 ACL handler pattern.
- New `crates/broker/src/leader_rebalance.rs`. Background ticker on
  the controller leader scans for imbalanced partitions every
  `leader_imbalance_check_interval_secs` (default 300s, matches Kafka);
  submits batched preferred-elections when imbalance crosses
  `leader_imbalance_per_broker_percentage` (default 10%).
- `BrokerConfig` gains `auto_leader_rebalance_enable` (default `true`
  in `Default`, `false` in `for_tests` so slice-10b multi-broker tests
  don't see surprise re-elections from the ticker),
  `leader_imbalance_check_interval_secs`, and
  `leader_imbalance_per_broker_percentage`. Two new `BrokerError`
  variants validate non-zero interval and ≤100% threshold at startup.
- 8 new authorizer-pure unit tests (PREFERRED + UNCLEAN matrix), 2
  rebalance-tick unit tests with mock controller, 4 broker integration
  tests (preferred + unclean wire paths, non-super-user denied,
  auto-rebalance restores preferred leader within 15s on a 1-second
  tick; gated `#[cfg(not(target_os = "windows"))]` per multi-broker
  test convention). 1 new JVM acceptance test drives
  `kafka-leader-election --election-type preferred` through a 3-broker
  cp-kafka:7.5 cluster.
- Out of scope: manual partition reassignment, quotas, log compaction,
  KIP-841 force-elect, operator preferred-replica override.

## Slice 15 — Partition reassignment (KIP-455) (2026-05-15)

- `PartitionRecord` gains `adding_replicas: Vec<NodeId>` +
  `removing_replicas: Vec<NodeId>`. `MetadataImage::reassignments_in_flight()`
  iterator returns all partitions with a non-empty `adding_replicas` or
  `removing_replicas`.
- Pure-logic `process_one_partition` in
  `crates/broker/src/handlers/alter_partition_reassignments.rs` converts
  one `AlterablePartitionReassignment` row into an intermediate
  `PartitionRecord` (start: union-replicas; cancel: revert with optional
  leader-revert; replace-in-flight: re-compute union from current target;
  RF-change guard behind `allow_replication_factor_change`). Returns a
  `(i16, &str)` wire error on validation failure.
- New `crates/broker/src/handlers/alter_partition_reassignments.rs`
  (api_key 45, KIP-455). Cluster Alter authorize gate; builds a batch of
  `MetadataRecord` updates and submits via `controller.submit_change`.
- New `crates/broker/src/handlers/list_partition_reassignments.rs`
  (api_key 46). Filters by topic+partition index or returns all in-flight
  rows; grouped by topic name in BTreeMap order. Cluster Describe gate.
- New `crates/broker/src/reassignment.rs`. Background task spawned in
  `Broker::start`; per-tick `is_leader()` check makes it a no-op on
  followers (no `BrokerConfig` gate needed, unlike slice 14's
  `auto_leader_rebalance_enable`). `compute_reassignment_progress`
  reads the current image via `reassignments_in_flight()`, checks ISR
  catch-up, performs leader handoff when the current leader is in
  `removing_replicas`, then atomically commits the target replica set.
  Image-driven (not timer-driven): runs on every controller-image update.
- New `BrokerHandle::partition_record_for_test` helper used by T9
  integration tests to poll partition state.
- Test inventory:
  - 4 unit tests on `reassignments_in_flight` in
    `crates/metadata/src/image.rs` (T1).
  - 7 unit tests on `process_one_partition` in
    `crates/broker/src/handlers/alter_partition_reassignments.rs` (T3):
    noop-already-at-target, start-union-replicas, replace-in-flight,
    RF-change rejected/allowed, cancel-with-leader-in-adding, empty-target.
  - 8 async unit tests on `compute_reassignment_progress` in
    `crates/broker/src/reassignment.rs` (T4): complete when adding in
    ISR, wait when adding not in ISR, leader handoff when leader in
    removing, leader handoff skipped if no alive target, idle partition
    no-op, multiple partitions independent, target replicas computed
    correctly, ISR intersection.
  - 4 broker integration tests in `tests/partition_reassignment.rs`
    (T9+T10): alter-and-complete, list-in-flight, cancel, and
    auth-deny (Cluster Alter). Gated `#[cfg(not(target_os = "windows"))]`
    per multi-broker convention; run in CI on Linux.
  - 1 JVM acceptance test (T11) driving `kafka-reassign-partitions
    --execute` + `--verify` against a 3-broker SASL/PLAINTEXT cluster
    in WSL Docker.
- Known limitation: `kafka-reassign-partitions --verify` exits 1 because
  it unconditionally issues `IncrementalAlterConfigs resource_type=4`
  (broker-scoped throttle config clear) which Crabka does not implement
  yet. The reassignment itself completes correctly; the JVM test asserts
  on stdout (`"is completed"` / `"completed successfully"` / `"is
  complete"`) rather than exit code. Slice 15b will add KIP-73 throttling
  configs and close this gap.
- Out of scope: KIP-73 throttled replication (slice 15b), KIP-113
  log-dir reassignment, KIP-841 force-elect.

## Slice 15b — Replication throttling (KIP-73) (2026-05-15)

- `IncrementalAlterConfigs` broker-scoped (`resource_type=4`) now accepted;
  validates two KIP-73 rate keys (`leader.replication.throttled.rate`,
  `follower.replication.throttled.rate`) and stores them as `BrokerConfigRecord`
  in metadata. All other broker keys are still rejected.
- Topic-level `*.throttled.replicas` configs added to the validator allowlist;
  values parse via `ThrottledReplicas` enum (none / `*` / explicit pair list).
- New `BrokerConfigRecord` + `V1BrokerConfig` metadata record carries per-broker
  key/value pairs. `MetadataImage::broker_configs` map + `broker_throttle_rate`
  accessor. 4 unit tests.
- `TokenBucket` in `crates/broker/src/throttle/bucket.rs` — one-second burst
  capacity at configured rate, rate-0 fast path for unthrottled. 6 unit tests
  covering refill / drain / cap / set.
- Background refresh task in `throttle/refresh.rs` subscribes to
  `controller.watch_image()` and pushes rate updates to the buckets on every
  image apply. 2 unit tests via a mock `ImageWatcher`.
- Fetch handler: leader-side enforcement caps response bytes when partitions in
  `leader.replication.throttled.replicas` are fetched by listed followers.
  Whole-partition-chunk drop (no mid-batch truncation).
- Replicator: follower-side enforcement caps `max_bytes` in outgoing Fetch
  requests when this broker is listed as a throttled follower.
- `DescribeConfigs` broker-resource path now emits per-broker configs from
  `MetadataImage::broker_configs`. Note: `config_source` is set to `2`
  (`DYNAMIC_BROKER_LOGGER_CONFIG`) rather than the canonical Kafka value `3`
  (`DYNAMIC_BROKER_CONFIG`). JVM tools were tolerant of this (T12 JVM test
  passed); cleanup deferred as a follow-up.
- 22 new unit tests total: 6 throttle parser, 6 token bucket, 4 metadata image
  apply, 4 config validator allowlist, 2 refresh task.
- 4 broker integration tests in `tests/throttle.rs`: broker-scoped alter
  persists, topic throttle propagates, rate caps Fetch response size, unthrottled
  topic unaffected. Gated `#[cfg(not(target_os = "windows"))]` per multi-broker
  convention; run in CI on Linux.
- 1 new JVM acceptance test (`jvm_kafka_reassign_partitions_with_throttle_end_to_end`)
  exercising the full KIP-73 round-trip including `kafka-configs --describe`
  visibility check.
- Closes slice 15 T11 known limitation: `kafka-reassign-partitions --verify`
  now exits 0; the slice-15 JVM test updated to assert on exit code.

### Real bugs surfaced during integration

- **T9 leader-side throttle keyed off the wrong id.** Original T9 implementation
  checked the leader's own broker id; KIP-73's
  `leader.replication.throttled.replicas` actually lists `(partition, follower_id)`
  pairs. The fix (keying off `replica_id` from the Fetch request) was applied in
  T11 alongside the integration tests that caught it.
- **`kafka-reassign-partitions --verify` clears three configs, not two.** The
  third key (`replica.alter.log.dirs.io.max.bytes.per.second`, KIP-113 log-dir
  throttle) was rejected by the broker-config allowlist, causing `--verify` to
  exit 1. T12 added this key to the allowlist as accept-but-don't-persist (log-dir
  throttle is not yet implemented; the no-op accept exists purely so JVM tooling
  completes its `--verify` workflow without error).

### Known follow-ups

- `DescribeConfigs` `config_source=2` for broker resources (canonical value is `3`
  `DYNAMIC_BROKER_CONFIG`). JVM tools tolerate either; cleanup deferred.
- Metrics emission (`replication_bytes_in/out`) deferred to a dedicated
  observability slice (Crabka has no metrics framework yet).
- KIP-113 log-dir throttle (`replica.alter.log.dirs.io.max.bytes.per.second`)
  accepted but not enforced.
- Out of scope: per-listener config refresh, dynamic reload of non-throttle
  broker configs.

## Slice 16 — Client quotas (2026-05-15)

- `AlterClientQuotas` (api_key 49) + `DescribeClientQuotas` (api_key 48), v0–1.
- Three quota types: `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`.
- Four entity scopes: `user`, `client-id`, `(user, client-id)` tuple, `<default>` (entity_name=null).
- Kafka's 8-priority entity lookup in `crates/broker/src/quota/lookup.rs` — 9 unit tests.
- Per-broker `QuotaBuckets` (DashMap) caches `(quota_key, entity_key) → Arc<TokenBucket>`, lazy-allocated on first lookup. 4 unit tests.
- Image-driven refresh task in `quota/refresh.rs` pushes new rates on every metadata apply. 2 unit tests.
- New `ClientQuotaRecord` metadata record + `MetadataImage::client_quotas` map keyed by canonicalized entity tuple (sorted by entity_type). 4 image unit tests + 1 round-trip test in records.rs.
- Produce hot path consumes from `producer_byte_rate` bucket; Fetch (consumer-only) from `consumer_byte_rate`; dispatch loop wraps every handler with `request_percentage` accounting. KIP-257 delays applied via `tokio::time::sleep` before response write; capped at 1 second.
- 30 new unit tests total: lookup 9, buckets 4, refresh 2, alter 6, describe 4, image 4, records round-trip 1.
- 5 broker integration tests in `tests/client_quotas.rs`.
- 1 JVM acceptance test exercising `kafka-configs --alter/describe/delete` round-trip.
- **Known limitation:** `client_id` is currently `""` in Produce/Fetch quota lookups because the HandlerTable signature does not thread it through. User-level + default quotas WORK; `(user, client-id)` tuple quotas do not fire on data-plane paths yet. Integration test 4 was swapped from `tuple_quota_wins_over_user_only` to `user_specific_overrides_user_default` to reflect this. The 8-priority lookup itself is fully verified by unit tests in `quota/lookup.rs`. Fix: thread `client_id` through handler signatures — deferred.
- **Known limitation:** `kafka-configs --describe --entity-type users` calls `DescribeUserScramCredentials` (api_key 51) after fetching quotas. Crabka does not implement api_key 51 yet, so the JVM tool exits non-zero even though the quota stdout is correct. JVM acceptance test asserts on stdout substring instead of exit code. Follow-up: implement api_key 51.
- **Known limitation:** `throttle_time_ms` in the response is only set for Produce + Fetch. Other handlers absorb the `request_percentage` delay silently. Closing this requires routing the throttle value through the handler trait — deferred.
- Out of scope: `ip` entity + KIP-612 connection_creation_rate (slice 16b), KIP-599 controller_mutation_rate (slice 16c).

## Slice 16b — IP quotas + KIP-612 (2026-05-15)

- `ip` entity type recognized by `AlterClientQuotas` (api_key 49) and `DescribeClientQuotas` (api_key 48). IPv4 only — entity_name validated via `Ipv4Addr::from_str`; IPv6 rejected with `INVALID_REQUEST` (slice-13 ACL parity).
- New `connection_creation_rate` quota key (KIP-612). Stored in `ClientQuotaRecord` like any other quota; no new metadata record.
- `lookup_ip_quota` + `lookup_ip_quota_with_key` in `crates/broker/src/quota/lookup.rs` — two-priority (specific IP → default). Disjoint from the 8-priority user/client-id lookup. 4 unit tests.
- Validator extension in `process_one_entry`: `SUPPORTED_ENTITY_TYPES` += `"ip"`; `KNOWN_QUOTA_KEYS` += `"connection_creation_rate"`; IPv4 validation on `entity_name`. 2 unit tests.
- **T2 caught a slice 16 test bug:** `unsupported_entity_type_rejected` used `"ip"` as the example unsupported entity type. Since slice 16b now SUPPORTS `ip`, that test would have silently started passing against the wrong code path. T2 changed the example to `"group"` to keep the test honest.
- TCP accept enforcement in `broker.rs` accept loop. After `listener.accept()` returns `(stream, peer)`, look up `(ip=peer.ip())` `connection_creation_rate`; if rate > 0 and bucket exhausted, compute `delay = 1/rate` seconds (capped at 1s) and `tokio::time::sleep` before spawning the per-connection handler. Connection is never rejected — only delayed (KIP-612 semantic).
- **T4 wall-time tolerance is `≥1.5s` not `≥3s` per plan.** `TcpStream::connect()` returns at the OS TCP handshake level — the throttle delays the broker's RESPONSE to the first request, not the TCP handshake itself. T4 sends an ApiVersions request on each connection and times the full round-trip. Additionally, the token bucket refills during the sleep interval, so 5 connections at rate=1 take ~2s not ~4s (correct token-bucket behavior, just different from the naive sequential expectation).
- 3 broker integration tests in `tests/ip_quotas.rs`: SASL/PLAIN alter+describe round-trip, accept-throttle wall-clock proof (≥1.5s for 5 connections at rate=1/sec), unthrottled baseline (<500ms). Gated `#![cfg(not(target_os = "windows"))]`; run in CI on Linux.
- **T5 positive surprise:** `--describe --entity-type ips` does NOT trigger the `DescribeUserScramCredentials` side-call that slice 16's user-scoped describe hit. Exit code is 0 cleanly — no stdout-substring workaround needed. The JVM describe assertion uses `assert_status_success`.
- 1 new JVM acceptance test for `kafka-configs --entity-type ips` alter + describe + delete-config round-trip.
- **Known limitations:**
  - Sub-1-connection-per-second rates floor to 1 — `rate.max(1.0) as u64` to avoid the "0 tokens/sec = always blocked" footgun. Production operators don't configure sub-1 rates.
  - Byte-rate quotas on `(ip)` entity are accepted by the validator but not enforced (matches Kafka's permissive validator).
  - Per-IP bucket cache grows unbounded over the broker's lifetime (inherits slice 16's no-eviction limitation).
- Out of scope: IPv6 entity names, connection rejection (vs delay), `controller_mutation_rate` (KIP-599 — slice 16c).

## Slice 16c — controller_mutation_rate (2026-05-15)

- KIP-599 `controller_mutation_rate` quota type — partition-mutations-per-second; user / client-id entity scopes (no IP per KIP-599).
- Validator extension: `KNOWN_QUOTA_KEYS += "controller_mutation_rate"` in `alter_client_quotas.rs`. 1 unit test.
- New helper `consume_controller_mutation_quota` in `crates/broker/src/quota/controller_mutation.rs`. Reuses slice-16's `lookup_quota_with_key` (8-priority) and `QuotaBuckets`. 3 unit tests.
- Enforcement on three handlers:
  - `CreateTopics` — mutation count = sum of `num_partitions` across all topics (`-1` → 1 for accounting).
  - `CreatePartitions` — count = sum of `(target_count - current_partition_count)` across topics; nonexistent topics count their full target.
  - `DeleteTopics` — count = sum of partition counts (image lookup); nonexistent topics count 0.
- Counted BEFORE handler runs (so invalid requests still count — bad-faith clients can't escape the throttle by spamming malformed RPCs).
- Throttle delay set on `throttle_time_ms` + `tokio::time::sleep` before encoding response. Capped at 1 second per slice-16 convention.
- 3 broker integration tests (`tests/controller_mutation_quota.rs`): throttled CreateTopics with wall-clock proof, unthrottled baseline, throttled DeleteTopics.
  - **T6 ACL correction:** the plan suggested seeding a `Cluster Delete` ACL for the DeleteTopics throttle test, but Crabka's authorizer checks `ResourceType::Topic` + `AclOperation::Delete` on the specific topic name (not a cluster-level operation). T6 was corrected to seed `Topic Delete` on `"to-delete"` to match the actual authorizer behavior.
- 1 new JVM acceptance test (`jvm_kafka_configs_alter_controller_mutation_rate_end_to_end`).
  - **T7 finding:** `kafka-configs --delete-config --entity-type users` also exits non-zero due to the `DescribeUserScramCredentials` side-call — not just `--describe` as slice 16 T13 documented. T7 used raw `std::process::Command` for both the `--describe` and `--delete-config` steps (asserting on stdout substring and image state respectively) to work around this.
- **Inherits slice 16 known limitations:**
  - `client_id` not threaded through `HandlerTable` — `(user, client-id)` tuple quotas don't fire from these handlers; `(user)`-only quotas work. Closing requires the slice-16 cleanup work.
  - Per-entity bucket cache grows unbounded over broker's lifetime.
- Out of scope: IP entity (KIP-599 doesn't apply to IP); other admin operations (ACL CRUD, IncrementalAlterConfigs, AlterPartitionReassignments — KIP-599 limits to topic/partition CRUD).

## Slice 17a — DescribeUserScramCredentials (2026-05-15)

- KIP-554 read half: `DescribeUserScramCredentials` (api_key 50, v0). Reads from existing slice-12 `MetadataImage::scram_credentials`.
- Two new image accessors: `scram_credentials_users() -> Vec<String>` and `scram_credentials_for_user(user) -> Vec<(SaslMechanism, i32)>`. 2 unit tests.
- New handler `crates/broker/src/handlers/describe_user_scram_credentials.rs`. Filter semantics: `users=None` OR empty list → all users; non-empty → filter. Unknown users return per-user `RESOURCE_NOT_FOUND_USER (83)`. 4 unit tests.
  - **Real finding:** `RESOURCE_NOT_FOUND = 66` already existed in `codes.rs` for delete-target-missing. Added a distinct `RESOURCE_NOT_FOUND_USER = 83` for the describe-unknown-user case. Both wire values match Kafka conventions.
  - **Real finding:** `SaslMechanism` only has `Plain` + `ScramSha512` in Crabka (no `ScramSha256` variant). `sasl_mechanism_to_byte` matches exhaustively.
  - **Real finding:** Used `cast_signed()` to convert `u32` iterations to `i32` for the wire `CredentialInfo.iterations`.
- Authorization: Cluster Alter (matches slice-12 `AlterUserScramCredentials` — JVM AdminClient uses Alter for both Alter and Describe SCRAM ops).
- Inline-intercept dispatch (handler needs `&Principal`). Mirrors slice-16 `DescribeClientQuotas` framing.
- **Real finding:** `SaslMechanism` lives in `crabka_security`, not `crabka_metadata` — handler imports adjusted accordingly.
- 2 broker integration tests in `tests/describe_user_scram_credentials.rs`: all-users round-trip with seeded alice credential, unknown-user RESOURCE_NOT_FOUND.
- 3 slice-16-family JVM tests retroactively cleaned up: `jvm_kafka_configs_alter_client_quota_end_to_end`, `jvm_kafka_configs_alter_ip_quota_end_to_end`, `jvm_kafka_configs_alter_controller_mutation_rate_end_to_end` now use `docker_run_kafka_tool_with_image_and_mount` + `assert!(status.success())` for `--describe`/`--delete-config` instead of the stdout-only workaround. Stdout-substring assertions kept as additional coverage.
- 1 new JVM acceptance test: `jvm_kafka_configs_describe_users_scram_credentials_end_to_end` provisions a SCRAM user and confirms `kafka-configs --describe --entity-type users` shows the credential.
- Closes the recurring JVM-tool quirk that slices 16/16b/16c documented as known limitations.
- Out of scope: slice 16 `client_id` HandlerTable gap (slice 17b).

## Slice 67 — Broker-side recompression (2026-05-17)

- Topic-level `compression.type` now enforces broker-side
  re-encoding. The Kafka default `producer` stays as pass-through;
  `gzip` / `snappy` / `lz4` / `zstd` / `uncompressed` re-encode every
  Produce batch on this topic before write.
- `LogConfig` gains
  `compression_type: Option<crabka_compression::CompressionType>`.
  `None` = pass-through. Defaults to `None`.
- `config_keys::validate_topic_config` now accepts all five codec
  names plus the existing `producer`. New `parse_compression_type`
  helper maps the wire value to the `Option<CompressionType>`. The
  applier wires it into `LogConfig.compression_type` via the existing
  slice-11 `Arc<RwLock<LogConfig>>` push.
- Recompression itself happens inside the partition writer task:
  before each `Log::append` of a Produce batch, if the topic's
  target codec is set and differs from the batch's current codec,
  the batch's `Attributes::compression` is overwritten. The encoder
  in `RecordBatch::encode` (called from `Log::append`) re-compresses
  the records body to match.
- New `crabka-log` dep on `crabka-compression` (for the
  `CompressionType` enum reference in the new `LogConfig` field).
  New `crabka-broker` dep on `crabka-compression` for
  `parse_compression_type`.
- Test inventory:
  - 6 new lib unit tests in `config_keys.rs` (all-five-codecs
    accepted, bogus rejected, producer/concrete mapping,
    `apply_to_log_config` zstd propagates + producer resets-to-None).
  - 1 new lib unit test in `crabka-log`'s `config` (default is
    `None` / pass-through).
  - 2 new broker integration tests in `tests/recompression.rs`:
    `compression.type=lz4` happy path (produce gzip → fetch lz4 +
    payload intact) and a `compression.type=producer` negative
    guard that proves the broker preserves the producer flag when
    no override is set.
- Closes the last ❌ in the `Wire protocol & clients` section of the
  README feature matrix.
- Out of scope: per-batch compression negotiation against producer
  capabilities (the broker assumes the producer supports the codec
  it forces); preserving the producer's compression bytes verbatim
  for `compression.type=producer` (we decode-and-re-encode for all
  Produce batches that pass through Crabka's record path —
  byte-equivalent on the codec side but not byte-identical).

## Slice 39 — Prometheus metrics exporter (2026-05-17)

- New `crabka_broker::metrics` module: a `BrokerMetrics` bundle of
  cheap `Arc`-cloneable counter/gauge handles registered against a
  shared `Registry` (`prometheus-client`). Eight metrics covering the
  Kafka JMX surface that operators commonly scrape:
  - per-topic counters: `topic_bytes_in`, `topic_bytes_out`,
    `topic_produce_requests`, `topic_fetch_requests`
  - scalar gauges: `partitions_led`, `active_controller`
  - scalar counters: `isr_shrinks`, `isr_expands`
- New `crabka_broker::metrics_server` axum app exposing `GET /metrics`
  in OpenMetrics text. Spawned by `Broker::start` when
  `BrokerConfig::metrics_listen_addr` is `Some`; cancelled via the
  supervisor shutdown token.
- New `BrokerConfig::metrics_listen_addr: Option<SocketAddr>`
  (default `Some(0.0.0.0:9404)` in production — same port as
  `jmx_prometheus_javaagent`; default `None` in `for_tests` so unit
  tests don't compete for ports).
- New `BrokerHandle::metrics_addr() -> Option<SocketAddr>` returns the
  bound address so integration tests can use `127.0.0.1:0` and
  discover the OS-assigned port.
- Counter wiring:
  - `Produce` handler accounts per-topic `bytes_in` + `requests` from
    the encoded `RecordBatch` length, regardless of per-partition
    outcome (matches `BrokerTopicMetrics:TotalProduceRequestsPerSec`).
  - `Fetch` handler accounts per-topic `bytes_out` + `requests` from
    the encoded response bytes about to be shipped.
  - `isr_maintenance` background loop bumps `isr_shrinks` /
    `isr_expands` whenever `compute_proposal` returns a change,
    classified by set-difference against the pre-proposal ISR.
  - Background gauge updater runs once a second: counts partitions
    where `current_leader == this node`, and sets
    `active_controller` from the raft `watch_leader()` borrow.
- 4 new lib unit tests (registry shape, label semantics, zero-byte
  fetch still increments request count). 1 broker integration test
  in `tests/metrics.rs`: boots a broker with metrics on
  `127.0.0.1:0`, scrapes pre-traffic for scalar metrics, drives
  Create/Produce/Fetch, scrapes again for topic-labelled metrics and
  the post-tick gauges.
- New broker workspace deps: `prometheus-client` (already in the
  workspace), `axum`. New dev-dep: `tower` for the
  `Router::oneshot` test helper.
- **Real finding:** `prometheus-client` auto-appends `_total` when
  encoding `Counter`s, so registering as `isr_shrinks_total` would
  emit `isr_shrinks_total_total`. Counters are named without the
  suffix in the registry; the wire output gets the suffix from the
  encoder.
- Unblocks operator slice 40 (`Kafka.spec.metricsConfig` →
  `ServiceMonitor` / `PodMonitor` generation against this endpoint).
- Out of scope: per-broker label on Family metrics (single-broker
  Prometheus targets already encode `broker_id` via the K8s
  `__address__` discovery); request-latency histograms; per-API-key
  request counters.

## Slice 33 — TLS cert hot-reload (2026-05-16)

- New `crabka_security::DynamicServerConfig` wraps the `rustls::ServerConfig`
  in an `arc_swap::ArcSwap`. The broker snapshots the current `Arc<ServerConfig>`
  on each TLS accept and wraps it in a fresh `tokio_rustls::TlsAcceptor`,
  so a mid-rotation reload affects only *new* handshakes — in-flight
  TLS sessions keep the config they negotiated against.
- `DynamicServerConfig::reload_from(&TlsConfig)` re-reads cert / key /
  optional client-CA paths and atomically swaps the inner `Arc`.
  Reload-on-error leaves the previous config in place (better to keep
  serving with the old cert than to drop connections on a typo).
- Broker plumbing: replaced `Broker::tls_acceptor: Option<TlsAcceptor>`
  with `tls_dynamic: Option<Arc<DynamicServerConfig>>`. Updated the
  data-plane dispatch path and the raft handshake to snapshot per use.
- New `BrokerConfig::tls_reload_interval` (default 30s production /
  200ms `for_tests`). `Duration::ZERO` disables the watcher; callers
  can still drive immediate reloads via the new public
  `BrokerHandle::reload_tls()`.
- New `crabka_broker::tls_reload::run` background task: polls cert /
  key / client-CA mtimes every `tls_reload_interval`, reloads on
  change. Spawned only when `tls_config` is set; cancelled via the
  supervisor shutdown token.
- New `BrokerHandle::reload_tls() -> Result<(), BrokerError>` — for
  operators / sidecars that just rewrote cert files and want the
  change to take effect immediately without waiting for a watcher
  tick.
- New dev fixture `dev_cert_alt.pem` / `dev_key_alt.pem` (P-256
  self-signed, CN=crabka-dev-alt — distinct from the original
  `dev_cert.pem`'s `CN=crabka-dev` and sha256 fingerprint).
- 2 new unit tests in `crabka_security::reload` (snapshot stability
  across reload; reload-on-error preserves prior config).
- 2 new broker integration tests in `tests/cert_hot_reload.rs`:
  explicit `reload_tls()` swaps the served cert; periodic
  mtime-watcher swaps after an on-disk rewrite (100ms tick, 1.1s
  warm-up so the new mtime exceeds the old by ≥1s on coarse FS
  resolutions).
- New workspace dep: `arc-swap` (lock-free atomic Arc swap).
- Unblocks operator slice 34 (non-disruptive CA rotation
  orchestration) and slice 30 (cluster CA + inter-broker mTLS).
- Out of scope: outbound `ClientConfig` hot-reload (only the server
  side reloads today — inter-broker outbound dialers use the original
  trust roots; rotation requires a broker restart). Hot-reload of
  `BrokerConfig.tls_reload_interval` itself.

## Slice 29 — mTLS client authentication (2026-05-16)

- `TlsConfig` gains two fields: `client_auth: ClientAuthMode`
  (`Disabled` / `Optional` / `Required`, defaults to `Disabled` to
  preserve pre-slice behaviour) and `client_ca_path: Option<PathBuf>`
  (operator-supplied clients CA, mirrors Kafka's
  `ssl.client.auth.truststore.location`).
- `build_server_config` branches on the mode: `Disabled` uses
  `with_no_client_auth` as before; `Optional`/`Required` wire a
  `rustls::server::WebPkiClientVerifier` against `client_ca_path`.
  `Required` rejects the handshake when no cert is presented;
  `Optional` accepts both.
- New `crabka-security::extract_principal_from_cert` parses the
  DER-encoded X.509 client cert via `x509-parser` and returns the
  Subject DN (matches Kafka's `DefaultKafkaPrincipalBuilder`).
- New `AuthMethod` enum on `Principal` — strict superset of
  `SaslMechanism` covering `Anonymous`, `SaslPlain`,
  `SaslScramSha{256,512}`, and `MTls`. `Principal::mechanism:
  SaslMechanism` is renamed to `auth_method: AuthMethod`. All ~50
  call sites updated; `SaslMechanism` stays the canonical SASL enum
  (used by `V1ScramCredential` metadata + `ConnectionAuth::Negotiating`).
- `network/dispatch.rs::serve_connection_on_listener` extracts the
  peer cert via `tokio_rustls::TlsStream::get_ref()` after a successful
  handshake. When present, the connection starts as `Authenticated`
  with `auth_method = MTls`; otherwise falls back to `ANONYMOUS`.
  SASL listeners ignore mTLS principals — Kafka's SASL_SSL semantics
  require SASL to be the auth even if a cert was negotiated.
- New dev fixtures in `crates/security/tests/fixtures/`:
  `dev_client_ca.pem` (self-signed P-256 CA), `dev_client_cert.pem`
  (CN=test-client signed by the CA), `dev_client_key.pem`.
- 3 new `TlsConfig` unit tests (missing-CA error, Required +
  Optional builders). 2 new mTLS unit tests in
  `crabka_security::mtls` (Subject DN extraction, malformed-cert
  None). 1 new broker integration test in `tests/mtls.rs`:
  SSL listener with `client_auth=Required`, fixture-cert TLS client,
  `super_users = [CLIENT_PRINCIPAL]`. The test sends `CreateTopics`
  — which requires Cluster Create — and asserts `error_code=0`.
  ANONYMOUS would have been denied with `CLUSTER_AUTHORIZATION_FAILED`,
  so success proves the principal was the cert DN.
- New workspace deps: `x509-parser` (Subject DN extraction),
  `rustls-webpki` (already transitively present, now explicit).
- **Real finding:** `x509-parser` renders the Subject DN
  most-significant first with no spaces: e.g.
  `CN=test-client,OU=integration,O=crabka`. Operators must pin this
  exact string in ACLs / super_users.
- Out of scope: cert hot-reload (slice 33), CA rotation orchestration
  (operator slice 34), `Optional` mode end-to-end test (covered by
  unit `client_auth_optional_with_ca_builds` only).

## Slice 32 — SASL/SCRAM-SHA-256 (2026-05-16)

- New `SaslMechanism::ScramSha256` variant + `wire_name`/`from_wire`
  round-trip ("SCRAM-SHA-256") and a `is_scram()` predicate that the
  handler / handshake code uses to treat both SCRAM variants uniformly.
- `crabka-security` SCRAM primitives now branch on the mechanism:
  `hash_scram_password_with_salt` runs `PBKDF2-HMAC-{SHA256,SHA512}`
  and stretches to the matching output size (32 / 64 bytes);
  `derive_keys_from_salted` takes a mechanism arg and dispatches
  accordingly. New helper `scram_hash_len(mechanism) -> usize`.
- `ScramServerExchange` reads the mechanism off the credential it was
  constructed with. `ScramClientExchange::new` gained a third arg
  (mechanism) so it can compute the right hash on the client side too.
  Asserts the mechanism is a SCRAM variant at construction.
- Wire mapping changes:
  - `alter_user_scram_credentials`: `wire_to_mech(1)` now returns
    `Some(ScramSha256)`; `salted_password` length check is per-mechanism
    via `scram_hash_len(mech)`.
  - `describe_user_scram_credentials`: `sasl_mechanism_to_byte(ScramSha256) = 1`.
  - `network/auth.rs::handle_handshake`: SCRAM-SHA-256 enters
    `SaslExchange::ScramPending` like SHA-512.
  - `network/dispatch.rs` + `raft_handshake.rs` + `network/client.rs`:
    SCRAM-SHA-256 routes through the same `handle_authenticate_scram`
    / `run_scram_client` paths; outbound inter-broker dialer threads
    the mechanism through.
- `crabka format --add-scram` accepts both `SCRAM-SHA-256=[...]` and
  `SCRAM-SHA-512=[...]` prefixes. 1 new CLI unit test.
- 7 new unit tests in `crabka-security`: SHA-256 PBKDF2 + key
  derivation, SHA-256 round-trip, mechanism wire-name round-trip,
  `is_scram` predicate.
- 3 new broker integration tests in `tests/auth_handlers.rs`:
  SHA-256 happy path, SHA-256 wrong-password connection close,
  end-to-end `AlterUserScramCredentials` provisioning with
  `mechanism=1` followed by a successful SHA-256 SCRAM session.
- 1 new JVM acceptance test
  (`jvm_sasl_scram_sha256_produce_consume`) drives `kafka-configs
  --alter --entity-type users --add-config 'SCRAM-SHA-256=[...]'`
  + `kafka-console-producer/consumer` with
  `sasl.mechanism=SCRAM-SHA-256`.
- Out of scope: mTLS client auth, OAUTHBEARER, GSSAPI, delegation
  tokens.

## Slice 22 — Controlled shutdown (2026-05-16)

- KIP-500 `BrokerHeartbeat.want_shut_down` is now honored end-to-end.
  The wire field already existed (api_key 63 v0/v1 in the generated
  codec) but slice 10b's heartbeat handler ignored it and always
  responded `should_shut_down=false`.
- `ControllerLivenessState` gains a `wants_shutdown: HashSet<u64>` plus
  `set_wants_shutdown(broker_id, want)` / `wants_shutdown(broker_id)`
  accessors. 2 new unit tests.
- New pure function `leader_election::select_replacement_leader_for_shutdown`.
  Picks the first alive ISR member that isn't the draining broker, bumps
  `leader_epoch`, leaves ISR untouched (the draining broker stays in ISR
  until the heartbeat ticker observes it dead). 5 new unit tests
  covering the success path, dead-ISR fallback, not-actually-leader
  no-op, no-eligible-replica failure, and unknown-partition error.
- `handlers::broker_heartbeat` runs `drain_leaderships_for_shutdown`
  when `want_shut_down=true`: scans every partition where the
  shutting-down broker is leader, submits one `V1Partition` record per
  drainable partition, returns `should_shut_down=true` iff the pre-submit
  leader count was zero. Image-driven (no per-partition wait inside the
  handler); the client polls via subsequent heartbeats.
- Broker plumbing: two new `watch::Sender` channels on `Broker`
  (`want_shutdown`, `should_shutdown`). The heartbeat client stamps the
  request flag onto outbound `BrokerHeartbeatRequest`s via
  `borrow_and_update()`; it latches `should_shutdown` on the first
  `should_shut_down=true` response.
- New `BrokerHandle::controlled_shutdown(timeout) -> Result<(), BrokerError>`.
  Signals `want_shutdown=true`, awaits `should_shutdown=true` via a
  `watch::Receiver`, then invokes the existing `shutdown()`. Returns
  `BrokerError::ShutdownTimeout` if leadership doesn't drain within the
  timeout.
- 1 new broker integration test in `tests/controlled_shutdown.rs`:
  3-broker PLAINTEXT cluster, rf=3 topic with target broker forced as
  leader of all 4 partitions, `controlled_shutdown` drains leadership
  and returns `Ok(())`; surviving brokers verify the post-shutdown image
  shows zero partitions led by the target. Gated
  `#[cfg(not(target_os = "windows"))]` per multi-broker convention.
- **Real finding:** the bootstrap broker is the sole replica of
  `__consumer_offsets/0` (rf=1, replicas=[node_id]). Picking it as the
  controlled-shutdown target causes the drain loop to block on that
  rf=1 partition forever — `select_replacement_leader_for_shutdown` has
  no live alternative to pick. The integration test sidesteps this by
  targeting a follower broker; production operators must either bump
  `__consumer_offsets`'s replication factor before draining the
  bootstrap broker, or hard-shutdown after a deadline. This mirrors
  Kafka's own behavior (KIP-500 returns `partitions_remaining > 0`
  until the operator increases rf).
- Out of scope: the legacy `ControlledShutdown` RPC (api_key 7) — the
  KIP-500 / KRaft world uses `BrokerHeartbeat.want_shut_down` exclusively;
  no slice consumer needs the legacy RPC.

## Slice 43a — Rebalancer foundation (2026-05-17)

- New workspace member `crates/rebalancer/` producing the
  `crabka-rebalancer` binary. Connects to a Crabka cluster as a
  regular admin client (`crabka_client_core::Client`), snapshots
  state every 10s via `Metadata` + `DescribeCluster` +
  `ListPartitionReassignments`, and exposes a Connect-RPC service
  on `:9300` for "what would balance this?" proposals.
- Connect-RPC service shape via `connectrpc-axum` 0.1 + prost 0.14.
  Six RPCs (`GetState`, `CreateProposal`, `DryRunProposal`,
  `GetProposal`, `ListProposals`, stub `ExecuteProposal`). Slice
  43a's `ExecuteProposal` returns `Code::Unimplemented` — execute
  lands in slice 43b. Clients can use JSON or protobuf per request
  (Connect content negotiation). Codegen produces a builder pattern
  (not a tonic-style trait); freestanding async fn handlers receive
  shared `AppState` via an `axum::Extension(Arc<AppState>)` layer.
- Three goals: `PreferredLeaderIdempotency` (hard),
  `ReplicaDistribution` (soft), `LeaderDistribution` (soft). Pure
  trait-based plumbing — slices 43c–43g add rack-aware, capacity,
  usage, and anomaly goals against the same surface.
- Optimizer: hard-goals-first ordering, last-writer-wins coalesce on
  duplicate `(topic, partition)` keys, `OptimizeError::HardGoalUnsatisfied`
  when the cap drops a hard movement, deterministic post-coalesce
  movement order.
- In-memory `ProposalStore` (UUID-keyed VecDeque ring buffer,
  default capacity 20). No on-disk persistence in 43a — slice 43b
  adds it alongside the executor.
- Operational endpoints (`/healthz`, `/readyz`, `/metrics`) on the
  same axum listener. `/readyz` gates on the first successful
  snapshot. `/metrics` serves OpenMetrics text from a
  `crabka_rebalancer`-prefixed registry exposing three metrics:
  `crabka_rebalancer_snapshot_at_ms` (gauge, epoch-millis of the
  last successful snapshot), `crabka_rebalancer_snapshots_total`
  (counter, successful snapshots), and
  `crabka_rebalancer_proposals_created_total` (counter, proposals
  computed via `CreateProposal`). Later slices add usage / latency
  counters off the same registry.
- New workspace deps: `connectrpc-axum` 0.1, `connectrpc-axum-build` 0.1,
  `prost` 0.14 (bumped from 0.13 to match what `connectrpc-axum` requires),
  `pbjson` 0.9. New dev-dep on `reqwest` for the Connect HTTP smoke test.
- 36 new unit tests across `model`, `goals/*`, `optimizer`,
  `ingest`, `api`, `health`. 2 in-process integration tests in
  `tests/end_to_end.rs` (balanced cluster proposal + pre-snapshot
  `Unavailable`). 1 binary-level Connect-protocol smoke test in
  `tests/connect_smoke.rs` (HTTP+JSON `GetState` round-trip).
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43a-design.md`].
  Roadmap (slices 43a–43g + operator slice 44) in
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-roadmap-design.md`].
- Out of scope (deferred): execute path (slice 43b), persistence
  (slice 43b), metric scraping for usage goals (slice 43e),
  rack-aware / capacity / usage / CPU / anomaly goals (slices
  43c–43g), operator `KafkaRebalance` CRD (slice 44).

## Slice 43b — Rebalancer execute path (2026-05-17)

- Rebalancer transitions from advisor to executor. `ExecuteProposal`
  now drives `AlterPartitionReassignments` (KIP-455) under a
  KIP-73 throttle managed via `IncrementalAlterConfigs`, with
  progress polled via `ListPartitionReassignments`. `ClearThrottle`
  runs in every terminal path — success, failure, and cancel —
  so the broker never gets stuck with throttle configs set.
- New `CancelExecution` RPC reverts pending reassignments (KIP-455
  null-replicas) and clears throttle, transitioning the proposal to
  `Cancelled`.
- `ProposalStatus` extended with `Executing` / `Completed` /
  `Failed` / `Cancelled`. `Proposal` gains `started_at_ms`,
  `terminated_at_ms`, `failure_reason`, `throttle_bytes_per_sec`.
- One execution at a time. Concurrent `ExecuteProposal` returns
  `FailedPrecondition`. `CreateProposal` continues to compute
  against the current (transition-state) snapshot during execution.
- On-disk persistence at `{data_dir}/proposals.json` (full ring
  buffer, atomic write) + `{data_dir}/in_flight.json` (active
  marker, deleted on terminal). On startup, recovery loads both and
  resumes the persisted phase via re-issuing
  `AlterPartitionReassignments` (KIP-455 idempotent). `data_dir`
  defaults to `/var/lib/crabka-rebalancer`.
- Production Helm chart at `charts/crabka-rebalancer/`: Deployment
  (replicas: 1, strategy: Recreate), ClusterIP Service on 9300,
  ServiceAccount (no cluster RBAC), RWO PVC. `bootstrapServers` is
  a required value (chart fails to render without it).
- Five `helm-unittest` test files under
  `charts/crabka-rebalancer/tests/` run in CI alongside `helm lint`
  and the `helm template + grep` sanity check.
- New CLI flags: `--data-dir`, `--default-throttle-bytes-per-sec`
  (default 50 MB/s), `--execute-deadline-secs` (default 1800),
  `--reassignment-poll-interval-secs` (default 5),
  `--reassignment-batch-size` (default 200).
- New metrics:
  `crabka_rebalancer_executions_started_total` /
  `_completed_total` / `_failed_total` / `_cancelled_total`.
- 62 lib unit tests across `model`, `executor`, `api`,
  `health`, `metrics`, `optimizer`, `goals`, plus 7
  integration tests (`end_to_end.rs`'s
  `execute_proposal_settles_against_real_broker`,
  `cancel_clears_throttle_and_reverts`,
  `restart_resumes_in_flight_plan`, and the two carried-over 43a
  tests) and an extended Connect HTTP smoke test
  (`connect_smoke.rs` covers ExecuteProposal's FailedPrecondition
  path).
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43b-design.md`].
- Out of scope (deferred): multi-replica HA (later slice), metric
  scraping for usage goals (43e), rack-aware / capacity / usage /
  CPU / anomaly goals (43c–43g), operator `KafkaRebalance` CRD
  (slice 44), pause/step-through, adaptive throttle.

## Slice 43c — Rebalancer topology goals (2026-05-17)

- Three new goals shipped under the existing `Goal` trait:
  - `RackAware` (hard): no two replicas of the same partition share a
    rack tag (`BrokerView.rack`). Brokers with `rack: None` each
    count as their own pseudo-rack (matches Kafka KIP-36). Strict
    mode: if RF > distinct rack count for any partition, the goal
    logs `warn!` and emits no movements for that partition — never
    produces `HardGoalUnsatisfied`.
  - `TopicReplicaDistribution` (soft): per-topic replica balance.
    Distinct from the existing cluster-wide `ReplicaDistribution`;
    catches the case where a single topic is concentrated on one
    broker even though cluster-wide counts look balanced.
  - `MinTopicLeadersPerBroker` (soft, default off): every broker
    that holds at least one replica of a topic should also lead at
    least `N` partitions of that topic. `N` comes from the new
    `--min-topic-leaders-per-broker` CLI flag (env
    `CRABKA_MIN_TOPIC_LEADERS_PER_BROKER`, default 0). At default
    config the goal is a no-op; operators opt in by setting N > 0.
- `GoalRegistry::default_registry` now contains six goals in
  priority order: `PreferredLeaderIdempotency`, `RackAware`
  (Hard); `ReplicaDistribution`, `LeaderDistribution`,
  `TopicReplicaDistribution`, `MinTopicLeadersPerBroker` (Soft).
- 13 new unit tests (5 + 4 + 4) across the three new goal files,
  plus 1 new integration test
  (`rack_aware_eliminates_same_rack_collisions`).
- No proto changes, no persistence changes, no executor changes.
  Slice 43c is goal-only.
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43c-design.md`].
- Out of scope (deferred): `RackAwareDistributionGoal` (soft,
  best-effort variant of RackAware); per-proposal goal config
  (requires proto change); capacity / usage / CPU / anomaly goals
  (slices 43d–43g).

## Slice 43d — Rebalancer capacity goals (2026-05-17)

- Five new hard goals shipped under the existing `Goal` trait:
  - `ReplicaCapacity` (fully functional): enforces per-broker
    `max_replicas` from the operator-supplied capacity config.
    Greedy hot→cold evict for any broker over its limit.
  - `DiskCapacity` / `NetworkInCapacity` / `NetworkOutCapacity` /
    `CpuCapacity` (stubs): structs + registry entries + config-field
    reads ship now; `propose` returns empty and `is_satisfied`
    returns true unconditionally until slice 43e wires per-partition
    metrics. 43e replaces the bodies mechanically.
- New top-level `capacity` module (parallel to `goals`/`model`/
  `optimizer`) owns the `BrokerCapacities` + `BrokerCapacity` types
  and the YAML loader. Sparse-by-design: missing field = no limit
  for that resource on that broker; missing broker entry = no limits
  at all.
- New CLI flag `--broker-capacity-file` (env
  `CRABKA_BROKER_CAPACITY_FILE`, default empty). When unset, all
  five capacity goals are no-ops. When set, the binary loads + parses
  the YAML at startup and threads an `Arc<BrokerCapacities>` into
  the `AppState`'s `GoalContext`.
- `GoalContext` gains `broker_capacities: Arc<BrokerCapacities>`.
  The `Copy` bound is dropped (`Clone` is cheap via `Arc` bump;
  verified: every existing caller takes `&GoalContext`, zero-friction).
- `GoalRegistry::default_registry` now contains **11 goals** in
  priority order: `PreferredLeaderIdempotency`, `RackAware`,
  `ReplicaCapacity`, `DiskCapacity`, `NetworkInCapacity`,
  `NetworkOutCapacity`, `CpuCapacity` (Hard); `ReplicaDistribution`,
  `LeaderDistribution`, `TopicReplicaDistribution`,
  `MinTopicLeadersPerBroker` (Soft).
- Helm chart picks up an optional ConfigMap-based config: new
  `brokerCapacities` (map) + `brokerCapacityFile` (override path)
  in `values.yaml`; new `templates/configmap.yaml`; deployment
  conditionally mounts the ConfigMap + sets the env var. New
  `helm-unittest` suite `configmap_test.yaml` (3 tests) plus 1 new
  assertion in `deployment_test.yaml`.
- 14 new unit tests (6 capacity + 4 ReplicaCapacity + 4 stub) + 1
  new integration test (`replica_capacity_evicts_over_capacity_broker`).
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43d-design.md`].
- Out of scope (deferred): per-partition usage data + the four
  metric-dependent capacity goals' real bodies (43e); `CpuUsage`
  soft goal (43f); per-topic resource hints in the capacity config;
  dynamic capacity discovery; capacity-aware leader election.

### Known trade
- `ReplicaCapacity::is_satisfied` returns `true` unconditionally
  because the `Goal::is_satisfied(&ClusterState)` signature doesn't
  expose `GoalContext` (added in 43c without ctx access). Capacity
  enforcement happens at `propose` time only. If 43e needs stricter
  composition guarantees, a `Goal::is_satisfied_with_ctx` trait
  method can be introduced then.

## Slice 43e — Rebalancer usage scraper + soft usage goals (2026-05-17)

- **Broker-side (the `43e-core` half):**
  - New `PartitionLabel { topic, partition }` drives three new
    metric families on `BrokerMetrics`:
    `crabka_broker_partition_bytes_in_total{topic,partition}`,
    `crabka_broker_partition_bytes_out_total{topic,partition}`,
    and `crabka_broker_partition_disk_bytes{topic,partition}`.
    The slice-39 topic-level counters stay.
  - `handlers/produce.rs` + `handlers/fetch.rs` emit one per-partition
    `record_partition_*` call per (topic, partition) in addition to
    the existing topic-level inc.
  - New `disk_scanner` module periodically (default 60s) walks each
    partition's log directory and updates
    `partition_disk_bytes`. CLI flag
    `--partition-disk-scan-interval-secs` (env
    `CRABKA_PARTITION_DISK_SCAN_INTERVAL_SECS`; `0` disables).
- **Rebalancer-side:**
  - New top-level `scraper/` module: `parse` (scoped
    OpenMetrics text parser), `targets` (CLI value parser),
    `window` (per-series ring buffer + counter-reset-aware rate
    computation), and `mod.rs` (HTTP tick loop).
  - Four new soft goals shipped: `DiskUsage`, `LeaderBytesIn`,
    `NetworkInUsage`, `NetworkOutUsage`. Each consumes
    `ctx.broker_usages`; empty store → no-op (same fail-safe
    pattern as the 43d capacity stubs).
  - Three 43d capacity stubs become real:
    `DiskCapacity`, `NetworkInCapacity`, `NetworkOutCapacity`.
    Each adds an `is_satisfied_with_ctx` override that consults
    `ctx.broker_usages`.
  - `Goal` trait gains
    `is_satisfied_with_ctx(&ClusterState, &GoalContext) -> bool`
    with a default impl that forwards to `is_satisfied`. The
    optimizer's incremental hard-goal validation (slice 43c)
    switches to call this so capacity goals enforce their
    invariants against soft-goal interference.
    **Closes the 43d known trade** on `ReplicaCapacity` — which
    also adds its own `is_satisfied_with_ctx`.
  - `CpuCapacity` remains a stub (slice 43f).
  - `GoalRegistry::default_registry` grows from 11 to **15
    goals** in priority order. Renamed
    `default_registry_has_eleven_goals` →
    `default_registry_has_fifteen_goals`; updated
    `default_registry_order_matches_spec` accordingly.
- New CLI flags:
  `--metrics-scrape-targets` (env
  `CRABKA_METRICS_SCRAPE_TARGETS`, format
  `id:host:port,id:host:port,…`, empty default = scraper
  disabled),
  `--metrics-scrape-interval-secs` (default 30),
  `--metrics-retention-secs` (default 43200 = 12h).
- Helm chart picks up the three new env vars conditionally on
  `metricsScrapeTargets` being set. New helm-unittest assertion
  in `deployment_test.yaml`.
- ~40 new unit tests (parse, targets, window, four soft usage
  goals, three capacity real bodies + ReplicaCapacity
  is_satisfied_with_ctx, optimizer regression, broker
  PartitionLabel, broker disk scanner) + 1 broker integration
  test + 1 rebalancer integration test
  (`disk_usage_evicts_hot_broker`) + 1 helm-unittest assertion.
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43e-design.md`].
- Out of scope (deferred): `CpuUsage` soft goal + real
  `CpuCapacity` body (slice 43f); discovery of scrape targets
  via `Metadata` (currently operator-supplied);
  per-topic resource hints in capacity config (usage metrics
  provide the real input now); anomaly detection (slice 43g);
  operator `KafkaRebalance` CRD (slice 44).

### Known risks
- Memory footprint of the per-series ring buffer scales as
  brokers × partitions × 3 metrics × (retention / scrape_interval).
  At the default 30s scrape / 12h retention, a 5-broker × 1000-
  partition cluster is roughly 350MB. Tune via
  `--metrics-scrape-interval-secs` or `--metrics-retention-secs`.
- Counter resets (broker restart) are detected by
  `latest.value < earliest.value` returning `None`. The affected
  goal sees no rate signal until two post-reset samples
  accumulate.

## Slice 43f — Rebalancer CPU usage + real CpuCapacity (2026-05-18)

- **Broker-side:**
  - New `partition_cpu_micros` metric family on `BrokerMetrics`,
    exported as
    `crabka_broker_partition_cpu_micros_total{topic,partition}`.
    Counts on-CPU microseconds spent polling each (topic,
    partition)'s work. Microseconds (`u64` counter) instead of
    seconds (`f64`) because `prometheus-client` counters are
    integer-valued; rate(...) / 1_000_000 = cores in use.
  - `handlers/produce.rs` factors the per-partition loop body
    into an `async fn process_partition(...)`. Each iteration
    wraps the call with a fresh `tokio_metrics::TaskMonitor`;
    `monitor.cumulative().total_poll_duration` is the per-
    partition CPU charge. This is **actual on-CPU time**, not
    wall-clock — blocked awaits (writer queue, HW-wait under
    `acks=-1`, txn coordinator round-trip) don't inflate the
    sample.
  - `handlers/fetch.rs` adds a `cpu_micros: u64` accumulator on
    `PendingRead`. Both `do_read` call sites (first pass + the
    `long_poll_then_reread` re-pass) are instrumented with a
    fresh `TaskMonitor` and their `total_poll_duration` is added
    in. The response-emit loop drains a `(topic, partition) ->
    cpu_micros` side map built just before `pending` is consumed
    by `group_into_topic_responses`.
- **Rebalancer-side:**
  - `scraper::parse::MetricKind` gains `CpuMicros`; parser
    recognizes the new family.
  - `UsageStore::cpu_micros_rate(broker, topic, partition,
    window, now_ms)` returns micros/sec with the same
    counter-reset and stale-data guards as `bytes_in_rate`.
  - New soft goal `CpuUsage` — balances per-broker CPU rate
    summed across all replicas a broker hosts (CPU is consumed
    by both leader-driven produce work and any-replica fetch /
    replication work). Mirrors `DiskUsage` shape with greedy
    hot→cold replica swap, threshold-gated by
    `imbalance_threshold_pct`.
  - `CpuCapacity` (hard) stub replaced with the real body and
    `is_satisfied_with_ctx` override. Sums per-broker
    `cpu_micros_rate / 1_000_000` to get cores-in-use; emits
    movements off the most-over-capacity broker when the total
    exceeds the operator-supplied `cpu_cores` limit. Non-finite
    / non-positive `cpu_cores` is treated as "unlimited" — a
    defensive no-op rather than a panic on operator typo.
- `GoalRegistry::default_registry` grows from 15 to **16 goals**.
  Renamed `default_registry_has_fifteen_goals` →
  `default_registry_has_sixteen_goals`;
  `default_registry_order_matches_spec` updated with `CpuUsage`
  appended to the soft tier.
- ~9 new unit tests (parse, window, `CpuUsage` x3, `CpuCapacity`
  x5 covering empty / over-capacity / within / non-finite /
  is_satisfied_with_ctx) plus updates to existing broker metrics
  tests for the new counter family.
- Reference: same scraper / window plumbing as slice 43e; no new
  CLI flag, no new Helm value (operator-supplied scrape targets
  already cover the new metric automatically).
- Out of scope (deferred): per-process CPU attribution (we use
  handler-thread wall-time as a proxy; producer ack-wait and the
  writer task's append work are out of scope); discovery of
  scrape targets via `Metadata` (still operator-supplied);
  anomaly detection (slice 43g); operator `KafkaRebalance` CRD
  (slice 44).

### Known risks
- **TaskMonitor overhead.** Each per-partition iteration creates
  a fresh `tokio_metrics::TaskMonitor` and wraps the work in
  `monitor.instrument(...)`. The wrapper adds per-poll
  bookkeeping; at the per-request × per-partition granularity
  this is small but non-zero. Measure under load before
  declaring it free.
- **CPU time is per-future-poll, not per-thread.** The
  `total_poll_duration` excludes blocked awaits (the goal), but
  also excludes time the runtime spent *scheduling* the task
  between polls. On a saturated runtime that scheduling gap can
  be material. Operators can compare `partition_cpu_micros`
  against process-wide CPU to estimate the gap.
- **Fetch first-pass setup not counted.** Per-partition
  `PendingRead` construction in the first loop (epoch fence,
  follower-fetch HW maintenance) isn't instrumented — only the
  `do_read` futures are. The setup is fast and synchronous;
  expected to be negligible compared to log read CPU.

## Slice 38 — Operator: KafkaUser client quotas (2026-05-18)

- `crates/operator`: `KafkaUser.spec.quotas` (Strimzi-shaped) lands as
  an optional `KafkaUserQuotas` struct with four fields —
  `producerByteRate` / `consumerByteRate` / `requestPercentage` (i32)
  and `controllerMutationRate` (f64). Status gains `quotasInSync` bool.
- `crates/client-admin`: new `quotas` module surfaces
  `AdminClient::describe_user_quotas(&str) -> BTreeMap<String,f64>`
  and `AdminClient::alter_user_quotas(&str, &[QuotaOp], validate_only)`,
  wrapping `DescribeClientQuotas` (`api_key` 48, KIP-13/124/546) and
  `AlterClientQuotas` (`api_key` 49, KIP-13/124/257). Pure
  `diff_user_quotas(current, desired) -> Vec<QuotaOp>` produces the
  minimal `(Set, Remove)` op stream the reconciler applies.
- `controller/user.rs` extends the reconcile pipeline with one
  additional step after ACL convergence: `describe → diff → alter` for
  the `(user)` entity. `spec.quotas: null` skips quota reconciliation
  (operator does not manage); `spec.quotas: {}` (empty object) wipes
  every broker quota key for the user. Finalizer cleanup tombstones
  whatever the broker has for the user.
- `AdminClientLike` trait gains `describe_user_quotas` +
  `alter_user_quotas`. The reconcile-test fake under
  `tests/shared/fake_admin.rs` grows an in-memory
  `BTreeMap<String, UserQuotaConfig>` store and serves the two new
  RPCs.
- Tests: 7 unit tests in `quotas.rs` (diff happy-path / add / change /
  remove / mixed; wire-projection for `Set` and `Remove`), 6 unit
  tests in `crd/user.rs` (status defaults, quota-map projection,
  Strimzi-shape parse, `{}` vs absent semantics), 4 reconcile-level
  integration tests in `tests/reconcile_user.rs` (omitted-quotas =
  no broker call, first-reconcile sets every declared key,
  no-op when in sync, drift removes orphaned keys), and 2 wire
  round-trip tests in `tests/quotas_round_trip.rs` against a live
  in-process broker (set + change + remove flow; `validate_only` does
  not persist).
- CRD YAML regenerated: `deploy/crds/crabka.io_kafkausers.yaml` picks
  up `spec.quotas` (with min/max constraints) and `status.quotasInSync`.
- Out of scope (deferred): per-`client-id` and per-`ip` quota entities;
  tuple `(user, client-id)` quotas; surfacing `connection_creation_rate`
  via the CRD (broker pairs it with the `ip` entity, not `user`);
  Strimzi's user-quotas `Plugin` variant indirection (Crabka surfaces
  the typed shape directly).
## Slice 43g — Rebalancer anomaly detector + self-healing proposals (2026-05-18)

- New top-level `detector/` module:
  - `anomaly` — `Anomaly`, `AnomalyKind` (`BrokerDeath` /
    `UnderReplicatedPartitions` / `DiskPressure` / `SlowBroker`),
    `AnomalyKey` (`Broker(id)` / `Partition { topic, partition }` /
    `BrokerPartition { … }`), `AnomalySeverity` (`Warning` /
    `Critical`).
  - `store` — `AnomalyStore`: ring-buffered persistent store at
    `{data_dir}/anomalies.json`, schema version 1, atomic
    tempfile + rename, default capacity 200. `upsert_open`
    returns `(id, is_new)` so the tick loop can fire
    auto-trigger only on freshly-detected anomalies;
    `mark_resolved` flips `resolved_at_ms`;
    `set_triggered_proposal` tags an anomaly with its
    auto-triggered proposal id + mute window.
  - `rules/` — `Rule` trait + `RuleCtx`, with one file per kind.
    `SnapshotHistory` (in-memory ring of the last ~10
    snapshots' broker ids + partition ISR fingerprints) lets
    sustained-condition rules answer "has X held for ≥N
    seconds?". Not persisted; restart resets timers.
  - `metrics` — `DetectorMetrics`: per-kind
    `anomalies_detected_total`, `anomalies_resolved_total`,
    `auto_trigger_fired_total`, `anomalies_open` gauges, plus
    six `auto_trigger_skipped_total` reasons (`disabled`,
    `executing`, `reassignments`, `muted`, `no_movements`,
    `optimizer_error`). Shares the existing
    `crabka_rebalancer_` registry and `/metrics` endpoint.
  - `auto_trigger` — maps `AnomalyKind` to a minimal goal list
    via `goals_for_kind`, runs `optimizer::optimize`, inserts
    the resulting `Computed` proposal into `ProposalStore`,
    and tags the anomaly with `triggered_proposal_id` +
    `mute_until_ms`. Gated by `auto_trigger_enabled` and by
    executor / cluster in-flight checks.
- Four anomaly rules:
  - **BrokerDeath** — broker id appears in some partition's
    `replicas` but is missing from `snapshot.brokers` for
    ≥ `broker_death_threshold` (default 60s). Confirmed
    against `SnapshotHistory::oldest_since`. Severity:
    `Critical`. Auto-trigger goals:
    `PreferredLeaderIdempotency`, `RackAware`,
    `ReplicaDistribution`, `TopicReplicaDistribution`.
  - **UnderReplicatedPartitions** — `isr.len() <
    replicas.len()` sustained ≥
    `under_replicated_threshold` (default 120s), skipping
    partitions already in
    `snapshot.in_flight_reassignments` (transient ISR
    shortfalls during rebalance are expected). Severity:
    `Critical` when > 50% of a topic's partitions affected,
    else `Warning`. Auto-trigger goals:
    `PreferredLeaderIdempotency`, `ReplicaDistribution`.
  - **DiskPressure** — per-broker `disk_bytes_avg / capacity
    > disk_pressure_pct` (default 0.85). Severity:
    `Critical` when `> disk_critical_pct` (default 0.95),
    else `Warning`. Skips brokers with no
    `capacity.disk_bytes` configured. Auto-trigger goals:
    `DiskCapacity`, `DiskUsage`.
  - **SlowBroker** — per-broker CPU cores > `max(
    slow_broker_min_cores, slow_broker_multiplier ×
    cluster_median)` with ≥ 3 brokers reporting CPU.
    Severity: `Warning`. Auto-trigger goals: `CpuUsage`,
    `LeaderBytesIn`. `min_cores` floor (0.5) prevents false
    positives on idle clusters.
- **Auto-trigger posture: detect on by default, auto-trigger
  off by default.** Operators get observability for free via
  `GetAnomalies` and `/metrics`; they must opt in to writes
  via `--detector-auto-trigger-enabled`. When fired,
  auto-trigger creates a `Computed` proposal — the operator
  still runs `ExecuteProposal`. **No auto-execute in 43g.**
- Auto-trigger gates: (1) `auto_trigger_enabled` master switch,
  (2) skipped when `executor.in_flight` is occupied,
  (3) skipped when `snapshot.in_flight_reassignments` is
  non-empty, (4) per-anomaly `mute_until_ms` (default 15min
  after a fire), (5) skipped when the optimizer produces an
  empty movement list. Muted anomalies stay visible via
  `GetAnomalies`.
- New Connect-RPC method `GetAnomalies` at
  `/crabka.rebalancer.v1.Rebalancer/GetAnomalies` (the
  roadmap's `GET /api/v1/anomalies`). Request supports
  `limit` + optional `include_resolved` (unset = `true` so
  the surface includes resolved history). Proto adds two
  enums (`AnomalyKind`, `AnomalySeverity`) and four messages
  (`Anomaly`, `AnomalyKey`, `PartitionKey`,
  `BrokerPartitionKey`).
- `AppState` gains `anomaly_store: Arc<AnomalyStore>`. The
  `goal_registry` field migrates from owned to `Arc<GoalRegistry>`
  so the binary can share one instance between `AppState` and
  `Detector`.
- 10 new CLI flags + env vars:
  `--detector-tick-interval-secs` (default 30; 0 disables),
  `--detector-broker-death-threshold-secs` (60),
  `--detector-under-replicated-threshold-secs` (120),
  `--detector-disk-pressure-pct` (0.85),
  `--detector-disk-critical-pct` (0.95),
  `--detector-slow-broker-multiplier` (2.0),
  `--detector-slow-broker-min-cores` (0.5),
  `--detector-mute-window-secs` (900),
  `--detector-auto-trigger-enabled` (`false`),
  `--anomaly-ring-buffer-size` (200).
- Helm chart picks up all ten env vars under a new
  `detector:` block (plus top-level `anomalyRingBufferSize`).
  Two new helm-unittest tests in `deployment_test.yaml`
  (default values render correctly; `autoTriggerEnabled: true`
  flips the env).
- ~32 new unit tests across `detector::*` modules and ~4 new
  integration tests in `tests/end_to_end.rs`
  (`get_anomalies_returns_empty_when_detector_quiet`,
  `anomaly_store_persists_and_get_anomalies_returns_it`,
  `auto_trigger_skipped_when_executor_in_flight`,
  `disk_pressure_anomaly_auto_triggers_proposal`), plus 2 new
  helm-unittest cases.
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-roadmap-design.md`]
  (slice 43g closes the 43-series). No separate design doc;
  the roadmap covers it.
- Out of scope (deferred): auto-execute (the detector only
  proposes — `ExecuteProposal` stays operator-driven);
  per-kind mute window overrides (single global default);
  persisted `SnapshotHistory` (restart resets
  sustained-condition timers, briefly delaying re-detection
  of pre-restart conditions — acceptable since anomalies are
  derived signals); discovery of scrape targets via
  `Metadata` (still operator-supplied); alertmanager
  integration; operator `KafkaRebalance` CRD (slice 44).

### Known risks
- **Detector accumulates open anomalies during long-running
  executions.** When a proposal is executing, auto-trigger
  short-circuits on the in-flight gate; rules keep firing
  and anomalies accumulate in the store but no action is
  taken. Operators observing many open anomalies during a
  long execution should not be alarmed — auto-trigger
  resumes on the first post-execution tick. The
  `auto_trigger_skipped_executing_total` counter makes this
  case observable.
- **Sustained-condition timers reset on restart.** A
  rebalancer crash mid-detection means `SnapshotHistory` is
  empty and the rule needs to re-observe the condition for
  its threshold duration before re-firing. Acceptable
  trade-off — the anomaly will fire again, just slightly
  delayed.
- **`AnomalyStore` persistence is best-effort.** Like
  `ProposalStore`, a disk write failure logs a warning and
  leaves the in-memory state ahead of disk. On restart the
  rebalancer may forget the most-recent anomalies and which
  proposal each triggered. Acceptable trade-off — anomalies
  are derived signals, not source of truth.
- **Auto-trigger goal lists may overlap with operator
  manual-proposal flows.** Auto-trigger selects an explicit
  small goal subset (e.g., DiskCapacity + DiskUsage for
  DiskPressure); if the operator's parallel `CreateProposal`
  request includes the same goals, both proposals could
  produce overlapping movements. The optimizer's
  last-writer-wins coalesce makes this safe (no double
  application; one proposal wins per partition), but
  operators should be aware of the interaction.

## Slice 37 — Operator: KafkaUser mTLS authentication (2026-05-19)

- `crates/operator`: adds the `tls` variant to
  `KafkaUser.spec.authentication` (Strimzi-shaped). Operator
  generates a per-user X.509 client cert signed by a per-cluster
  clients CA and exposes it via a `Secret` with keys `user.crt`,
  `user.key`, `ca.crt` (PEM). `KafkaUserStatus` gains three
  fields: `tls`, `tlsCertNotAfter`, `tlsPrincipal`.
- `crates/security`: new `ca` module with pure rcgen helpers
  `generate_clients_ca` and `issue_user_cert` (ECDSA P-256). No
  kube types, no I/O — reusable by slice 30 (CA lifecycle) and
  slice 33 (cert hot-reload tests). Leaf cert carries
  `Subject = CN=<KafkaUser name>` (bare RDN — no `O`, no `OU`),
  `EKU = clientAuth`, `KeyUsage = digitalSignature|keyEncipherment`.
- `controller/user_tls.rs`: new module owning the clients-CA
  Secret bootstrap (`<cluster>-clients-ca` for the private key,
  `<cluster>-clients-ca-cert` for the public cert; both
  owner-ref'd to the parent `Kafka`), the per-user cert
  reuse/renew/issue decision, and the Secret render. Default 365d
  validity, reissue when `not_after - now <= renewal_days`
  (default 30). `controller/user.rs` dispatches on
  `Authentication` at step 6 of the reconcile pipeline; the TLS
  arm makes no broker call (the broker learns the identity from
  the cert at mTLS handshake).
- `principal_for(name, &Authentication)` is the single source of
  truth: `User:<name>` for SCRAM, `User:CN=<name>` for TLS. Used
  by ACL reconcile, ACL finalizer-delete, and quotas (with the
  `User:` prefix stripped — the broker accepts any string as
  `entity_name`, so quota slots cleanly partition between SCRAM
  and TLS users sharing a name).
- Requeue cadence relaxed to 6h for TLS users (vs 1m for SCRAM).
  Cert renewal needs daily-ish heartbeat, not minutely; ACL drift
  is still caught.
- Design decisions: bare `CN=<name>` Subject DN (dodges RFC 2253
  vs 4514 ordering ambiguity); per-user Secret carries `ca.crt`
  so consumers build trust stores without separately mounting the
  cluster-wide Secret; lazy CA bootstrap (slice 30 takes over the
  full lifecycle).
- Hand-rolled `Authentication` JSON schema via `schema_with`:
  kube-rs 3.x's `StructuralSchemaRewriter` panics on multi-variant
  tagged enums where the discriminator's `enum` values differ
  across `oneOf` branches. Same workaround as `Storage` in
  `kafka_node_pool.rs` (slice 19).
- Out of scope (deferred): full CA rotation / renewal (slice 30);
  listener-side mTLS so the broker requests client certs at
  handshake (slice 31); PKCS#12 keystore bundle `user.p12` +
  `user.password` (slice 37 follow-up if a JVM-client consumer
  needs it); user-provided clients-CA (BYO-CA, slice 30).
- Tests: +5 `crabka_security::ca` unit tests (CA round-trip,
  CA-signed leaf, leaf DN matches `extract_principal_from_cert`,
  leaf EKU is `clientAuth`, each generate is unique); +5
  `crd::user` unit tests (`Tls(TlsAuth)` round-trip, with-fields
  round-trip, SCRAM behaviour unchanged, status field omit, status
  field emit); +1 `controller::user` test
  (`principal_for_dispatches_on_auth_type`); +1 `controller::user_tls`
  test (`is_cert_expiring_soon_boundary_cases`); reconcile-level
  goes 8 → 13 tests (TLS first-reconcile, cert reuse, cert reissue,
  finalizer ACL DN filter, quotas-by-DN). Totals land around 187
  lib tests / 13 `reconcile_user` tests / 5 `security::ca` tests.
- CRD YAML regenerated: `deploy/crds/crabka.io_kafkausers.yaml`
  picks up the `tls` discriminator + `validityDays` / `renewalDays`
  properties and `status.{tls,tlsCertNotAfter,tlsPrincipal}`.
- Reference doc:
  [`docs/superpowers/specs/2026-05-19-crabka-operator-kafkauser-37-design.md`].

## Slice 30 — Operator: Cluster CA + clients CA generation (2026-05-21)

- New `Kafka.spec.clusterCa` + `Kafka.spec.clientsCa`: Strimzi-shaped
  `CertificateAuthority { generateCertificateAuthority, validityDays,
  renewalDays }`, default `(true, 365, 30)`. `clientsCa` replaces the
  slice-37 lazy-bootstrap path (deleted outright — greenfield).
- Operator generates and rotates per-broker keystore
  (`<cluster>-kafka-brokers`) signed by the cluster CA. Inter-broker
  mTLS on by default: the broker controller listener terminates TLS
  with `client_auth=Required` and the cluster CA cert as the
  truststore. Renewal of leaf certs is handled by a new CronJob
  (`crabka-operator ca-renewal-check`) shipped in the Helm chart with
  a dedicated ServiceAccount + narrower RBAC.
- BYO CAs (`generateCertificateAuthority: false`) — operator validates
  pre-existing Secret pair and refuses to overwrite; CronJob emits
  `ByoCaExpiringSoon` Events when nearing expiry.
- CA-itself expiry handled disruptively in this slice:
  `CaRotationRequired=True` status condition + Event, no
  auto-rotation. Slice 34 owns the multi-generation trust bundle +
  zero-downtime rotation.
- Slice-21 config-hash gains a fourth segment (cluster CA cert PEM)
  so CA changes force a cluster roll. Leaf cert renewal piggybacks on
  slice 33's cert hot-reload — no restart.
- New `crabka-operator ca-renewal-check` CLI subcommand + Helm-chart
  CronJob with daily schedule (`0 2 * * *`).
- Per-broker keystore is a single per-cluster Secret with `<id>.crt` +
  `<id>.key` entries; broker container picks its own by node id at
  mount time. Pruned on replica scale-down, appended on scale-up,
  reused (never reissued) on steady-state reconciles.
- 11 new operator unit tests (CA crd schema, cluster_ca module,
  combined_config_hash) + 11 new integration tests across three
  files (`reconcile_ca.rs`, `reconcile_inter_broker_mtls.rs`,
  `ca_renewal_cronjob.rs`). 4 of the 7 planned `reconcile_ca` tests
  landed; scale-up/down + chain-verify are deferred to a follow-up
  due to the FIFO-mock harness's lack of stateful Secret simulation.
- Helm chart additions: `cronjob-ca-renewal.yaml`,
  `serviceaccount-renewal.yaml`, `clusterrole-renewal.yaml`,
  `clusterrolebinding-renewal.yaml`, plus `caRenewal.*` values stanza.
  kind-e2e workflow asserts the new Secrets exist + the broker pod
  mounts them at `/etc/crabka/{cluster-ca,broker-tls}`.
- Out of scope: data-plane listener TLS (slice 31), non-disruptive CA
  rotation (slice 34), PKCS#12 keystore output, MaintenanceTimeWindows.

## Slice 31 — Operator: Listener auth wiring (TLS + SCRAM) (2026-05-22)

- New `Kafka.spec.listeners[].authentication`: Strimzi-shape
  `{ type: tls | scram-sha-512 | scram-sha-256 }`. mTLS (`type: tls`)
  requires `tls: true` on the listener; validation rejects otherwise
  with `ListenerMtlsRequiresTransportTls` reason on
  `ListenersValid=False`.
- Operator emits per-listener inline TOML `tls_config = { ... }` /
  `sasl_config = { enabled_mechanisms = [...] }` blocks inside each
  `[[listeners]]` entry. Slice-30 top-level `[tls_config]` (controller
  / inter-broker) is preserved as a fallback.
- Broker `file_config.rs` parses the new per-listener blocks; accept
  loop resolves TLS material and SASL mechanisms per listener with
  fallback to broker-wide defaults. Inter-broker continues to read
  the top-level config; no slice-30 regression.
- Per-broker cert SAN list extended at issuance time with external
  advertised addresses computed from observed NodePort (`Node.status.addresses`
  with type `ExternalIP`/`ExternalDNS`/`Hostname`) and LoadBalancer
  (`Service.status.loadBalancer.ingress[]`) state. `issue_broker_cert`
  gains `extra_sans` parameter. Cert reissue triggered via SHA-256
  digest of the SAN list stored in the `<cluster>-kafka-brokers`
  Secret (`{id}.sans-digest` key alongside `{id}.crt` / `{id}.key`).
- Validation: `tls: false + auth: tls` →
  `ListenerMtlsRequiresTransportTls`, blocks ConfigMap render +
  StatefulSet bump. SCRAM without TLS is accepted but produces a
  `WeakAuth` Warning Event each reconcile.
- LB ingress pending: per-broker cert issuance for affected brokers
  skipped this reconcile (issued with internal SANs only) and
  `WaitingForLoadBalancerIp=True` surfaced; reconcile requeues.
  Steady state (LB ingress assigned) sets the condition to `False`
  with reason `LoadBalancerReady`.
- Listener-auth changes flow through slice-21's config-hash → ordered
  rolling restart. Free — the rendered TOML is already in the hash.
- 9 new operator unit-test files / inline modules updated, 1 new
  integration file (`reconcile_listener_auth.rs` — 7 scenarios). The
  shared test harness (`tests/shared/mod.rs`) absorbs four previously
  duplicated helpers (`fake_ca_secret`, `fake_keystore_secret`,
  `happy_path_rules`, `build_ctx`).
- Broker: 5 new unit tests for TLS/SASL resolver fallback in the
  accept loop.
- kind e2e: 2 new scenarios in a new `kind-listener-auth` job —
  SCRAM-SHA-512 over TLS (internal) and mTLS (internal). NodePort SAN
  validation deferred (operator-side wiring is covered by the
  `nodeport_listener_external_san_added_to_per_broker_cert`
  integration test).
- Out of scope: BYO server cert (`brokerCertChainAndKey`), OAuth/OIDC
  listener auth (slice 49), custom authentication plugin,
  Ingress/Route listener TLS (slice 27), non-disruptive auth
  hot-reload, PKCS#12 user keystore bundle (slice-37 follow-up).

## Slice 27 — Operator: Ingress / Route external listeners (2026-05-22)

- Implements the `type: ingress` and `type: route` external listener
  reconcile that slice 25 deferred (the schema landed forward-stable
  then; slice 27 lights up the reconcile now that Phase 4's listener TLS
  exists). Kafka-over-Ingress requires SNI-passthrough routing, so both
  types **require `tls: true`** — the ingress controller / OpenShift
  router inspects the TLS `ClientHello` SNI and forwards the raw TLS
  stream to the matching broker; the broker terminates TLS.
- Each broker advertises a distinct config-supplied hostname
  (`configuration.brokers[].host`, `advertisedHost` override wins) on
  **port 443** (`configuration.brokers[].advertisedPort` overrides). The
  bootstrap advertises `configuration.bootstrap.host:443`.
- New schema field: `ListenerConfiguration.class` (`ingress_class`,
  Strimzi-shaped) → `spec.ingressClassName` on generated Ingresses. The
  `host` fields on bootstrap/broker config were already present from
  slice 25.
- Validation (replaces slice-25 `IngressDeferred`/`RouteDeferred`
  placeholders): `ListenerIngressRequiresTls` (ingress/route without
  TLS), `ListenerIngressBootstrapHostMissing` (no bootstrap host).
  Per-broker host absence surfaces at advertised time as
  `ListenersReady=False reason=PendingExternalAddresses` with an
  `IngressBrokerHostMissing` message.
- Objects rendered (owner-ref'd to the `Kafka`): per-broker + bootstrap
  **ClusterIP backend Services** (`render_broker_service` /
  `render_bootstrap_service` now emit `ClusterIP` for ingress/route);
  per-broker + bootstrap **Ingress** (`networking.k8s.io/v1`, typed) with
  `nginx.ingress.kubernetes.io/ssl-passthrough: "true"`,
  `spec.ingressClassName`, `spec.tls[].hosts` (no `secretName`), and a
  host→backend rule; per-broker + bootstrap **Route**
  (`route.openshift.io/v1`, applied as a `DynamicObject` via a new shared
  `common::apply_dynamic`) with `spec.tls.termination: passthrough` and
  `spec.to` → backend Service.
- SAN extension: `compute_extra_sans` now adds the config ingress/route
  hostnames (broker + bootstrap) as DNS SANs to each per-broker server
  cert so the SNI hostname validates. No "not ready" SAN gating (hosts
  are config-deterministic, unlike LB ingress).
- Reconcile wiring: `apply_external_services` handles ingress/route
  (ClusterIP backends + Ingress/Route objects); the Node/Pod LIST inside
  `read_external_state` is now gated on a NodePort/LoadBalancer listener
  being present, so an ingress/route-only cluster issues no Node/Pod
  reads. Listener-intent changes flow through slice-21's config-hash →
  rolling restart for free (advertised `host:443` is in the hash).
- RBAC: ClusterRole gains `networking.k8s.io/ingresses` and
  `route.openshift.io/routes`.
- Tests: +13 unit (validation ×4, ClusterIP backend ×2, ingress/route
  render ×4, `compute_advertised` ingress/route ×4, SAN ingress/route
  ×2) and +3 integration (`reconcile_listener_ingress.rs`: ingress
  renders objects + advertises 443 + `ListenersReady=True`; route renders
  passthrough objects; ingress-without-tls validation error). Operator
  lib tests 253; full operator suite green; clippy + fmt clean; CRD
  regenerated (only the `class` field added).
- Out of scope (deferred): OpenShift-assigned Route hosts (slice 27
  requires an explicit `host`, symmetric with ingress; reading
  `Route.status.ingress[].host` back + requeue is a follow-up needing a
  live OpenShift API to validate); BYO per-listener server cert; non-SNI
  ingress controllers (Kafka-over-Ingress fundamentally needs raw TLS
  passthrough); per-listener connection limits. kind-e2e for ingress
  (MetalLB + nginx ssl-passthrough) is a CI follow-up; operator-side
  wiring is covered by the integration tests.
- Reference doc:
  [`docs/superpowers/specs/2026-05-22-crabka-operator-listener-ingress-route-27-design.md`].

## Slice 44 — Operator: `KafkaRebalance` CRD (2026-05-22)

- Closes Phase 7 of the operator roadmap: the standalone
  `crabka-rebalancer` service (slices 43a–43g) was fully built but had no
  operator front-end. Slice 44 adds the `KafkaRebalance` CRD and a
  controller that translates it into Connect-RPC calls against the
  rebalancer and reflects the proposal lifecycle into `status`. Pure
  operator work — no Crabka-core dependency.
- New CRD `crabka.io/v1alpha1` kind `KafkaRebalance` (short `kr`),
  Strimzi-shaped: `spec.goals` (→ `CreateProposal.goals`),
  `spec.throttleBytesPerSec` (→ `ExecuteProposal` KIP-73 throttle),
  `spec.endpoint` (rebalancer Connect base URL; defaults to
  `http://<cluster>-rebalancer.<ns>.svc.cluster.local:9300` derived from
  the `crabka.io/cluster` label). `status` carries `conditions` (active
  state in the condition `type`), `sessionId` (proposal id),
  `observedGeneration`, and a typed `optimizationResult`.
- Annotation-driven state machine (Strimzi-shaped): `crabka.io/rebalance`
  ∈ `{approve, refresh, stop}`, consumed (merge-null deleted) once acted
  on. States `New → ProposalReady → Rebalancing → Ready/NotReady`, plus
  `Stopped`. `approve` executes a ready proposal; `refresh` recomputes;
  `stop` cancels an in-flight execution. The decision core is a pure
  `decide(state, command, has_session) -> RebalanceAction` (unit-tested in
  isolation); reconcile does only I/O. `Rebalancing` polls at 10s, other
  states requeue at 5min (the watch wakes on annotation changes).
- New `crabka_operator::rebalancer_client`: a `reqwest`-backed Connect/JSON
  client (`ConnectRebalancerClient`) + `RebalancerClientLike` trait test
  seam (mirrors `AdminClientLike`). Hand-rolled serde DTOs keep the
  operator decoupled from the rebalancer's prost/pbjson codegen. Decode
  tolerates pbjson's proto3-JSON shape (camelCase, enum-name strings,
  int64-as-string, default-omission); Connect errors → `RebalancerError::Rpc`;
  transport errors leave status untouched and retry. `Context` caches one
  client per endpoint, evicted on transport failure.
- `Cargo.toml`: `async-trait` promoted to a runtime dep; `reqwest` 0.13
  (`default-features = false, features = ["json"]`, plain HTTP) added;
  `crabka-rebalancer` added as a dev-dep for the e2e wire test.
  ClusterRole gains `kafkarebalances` + `/status`.
- ~44 new lib unit tests (CRD round-trip; Connect-JSON decode; full
  `decide` matrix; outcome mapping; `current_state` / `read_command` /
  `resolve_endpoint`), 7 reconcile integration tests
  (`tests/reconcile_rebalance.rs`) over a faked rebalancer + FIFO mock
  kube transport, and 1 end-to-end wire test (`tests/rebalance_e2e.rs`)
  driving the *real* `ConnectRebalancerClient` over HTTP against the *real*
  rebalancer Connect router served in-process against a real broker
  (verifies create/get round-trips, `not_found`, and zero-movement
  `failed_precondition`). Operator lib tests land at 290; full operator
  suite green; clippy + fmt clean; CRD YAML regenerated.
- Out of scope (deferred): `spec.mode`/`spec.brokers` (needs
  rebalancer-side broker-scoped modes); `DryRunProposal` surfacing;
  delete-cancels-rebalance finalizer; auto-approval / scheduling; kind-e2e
  (CI follow-up — operator wiring covered by the in-process wire test).
- Reference doc:
  [`docs/superpowers/specs/2026-05-22-crabka-operator-kafkarebalance-44-design.md`].

## Slice 28 — Operator: Version upgrades (2026-05-22)

- Closes the Phase-3 "Version upgrades" roadmap item. Pure operator work:
  the broker has no runtime `metadata.version` feature (the `UpdateFeatures`
  codec exists but no handler consumes it), so the resolved metadata
  version is rendered into the broker's inert `[server_properties]` table
  (same broker-inert-config pattern as slices 21/25). All upgrade safety
  lives in the operator.
- New `Kafka.spec.metadataVersion: Option<String>` (Strimzi-shaped). Crabka
  is KRaft-only, so this is the *only* feature-level knob — the runtime
  analog of the ZK-era `inter.broker.protocol.version`; there is no
  `inter.broker.protocol.version` / `log.message.format.version` lineage.
  When unset it tracks `kafkaVersion`'s `major.minor`; when set it pins the
  metadata version for the safe two-step upgrade.
- New `crabka_operator::version` module: `KafkaVersion::parse` (tolerates
  `X`, `X.Y`, `X.Y.Z`, and `X.Y-IVn` IBP suffixes), `(major,minor)`
  metadata-key comparison, and `evaluate(kafka_version, spec_metadata,
  finalized_metadata)`. Invariant on success: `binary >= resolved metadata
  >= finalized metadata` — the two inequalities are the downgrade window.
  Reasons: `InvalidVersion`, `MetadataVersionTooHigh` (metadata newer than
  binary), `MetadataVersionDowngrade` (metadata below the finalized value,
  incl. a binary downgrade that drags a default-tracked metadata below
  finalized). `finalized` is read from `status.metadataVersion` on the
  watched object — no extra API request.
- New `KafkaVersionValid` status condition + `KafkaStatus.kafkaVersion`
  (echo) and `KafkaStatus.metadataVersion` (operator-finalized; advances
  only when valid, holds the last value on rejection — drives the
  downgrade-window check next reconcile). On a validation failure the
  operator does not inject the new metadata version, does not advance the
  config hash, and does not finalize — "surface the error and wait".
- `metadata.version` is injected into each `broker-{id}.toml`
  `[server_properties]` (operator-owned key; operator value wins). An
  *explicit* `spec.metadataVersion` pin participates in the slice-21 config
  hash (so a pin change rolls); a *defaulted* metadata version does not (a
  binary bump rolls via the pod-template image change), which preserves the
  slice-24 empty-hash collapse.
- **Ordered, one-node-at-a-time rollout** across pools via a new pure
  `common::plan_rollout(pools_in_order, desired) -> per-pool target hash`.
  `adopt_pools` now sorts pools by `(node_id_start, name)`, reads each
  pool's `crabka.io/config-hash` label + `ready_replicas` (already in the
  listed objects — no new API requests), and advances one pool at a time
  gated on the prior pool reaching Ready. Initial bring-up / non-uniform
  state still applies the hash to every pool in parallel — a KRaft
  controller quorum needs all controllers up together, so gating initial
  creation would deadlock. The owner-ref is still patched to every pool
  every reconcile, so the request shape is identical to slice 21.
- Tests: 17 new `version` unit tests; 8 new `plan_rollout` unit tests; 3
  new CRD round-trip tests; 2 new `combined_config_hash`/`render_configmap`
  unit tests; 2 new `reconcile_kafka` integration tests (metadata.version
  rendered + status/condition echo; too-high pin rejected without injecting
  or finalizing). Operator suite green (lib 319); clippy `-D warnings`
  clean; fmt clean; CRD YAML regenerated.
- kind-e2e: a new probe in the `kind` job asserts the default
  `metadata.version` is rendered into the broker config, then that an
  invalid (too-high) `metadataVersion` pin surfaces
  `KafkaVersionValid=False reason=MetadataVersionTooHigh` without writing
  the rejected value or rolling the pod.
- Out of scope (deferred): broker-side `metadata.version` feature-level
  enforcement (`UpdateFeatures` handler) — a Crabka-core slice; a
  `kafkaVersion` → image-tag mapping (image stays `pool.spec.image >
  operator default > built-in`); draining each node via slice-22
  `ControlledShutdown` before its roll (the gate orders + waits for Ready
  but does not pre-drain); multi-replica pools.
- Reference docs:
  [`docs/superpowers/specs/2026-05-22-crabka-operator-version-upgrades-28-design.md`],
  [`docs/superpowers/plans/2026-05-22-crabka-operator-version-upgrades-28.md`].

## Slice 41 — Operator: Configurable logging (`Kafka.spec.logging`) (2026-05-23)

- Continues Phase 6 (observability) after slices 39 (metrics exporter) +
  40 (`metricsConfig`). Pure operator work — the broker already reads its
  log filter from `RUST_LOG` via `EnvFilter::try_from_default_env()`
  (`crates/broker/src/bin/broker.rs`), so no Crabka-core change.
- New `Kafka.spec.logging` (Strimzi-shaped `Logging`): `type: inline`
  carries a `loggers` map (tracing target → level) the operator composes
  into a single `RUST_LOG` env-filter directive; `type: external`
  references a user-managed `ConfigMap` key whose value is used verbatim.
  `loggers` keys are **tracing targets** (Rust module paths, e.g.
  `crabka_broker`); the key `root` (case-insensitive) sets the bare
  global level. Levels are `trace|debug|info|warn|error|off`
  (case-insensitive; `warning`→`warn`, `fatal`→`error`, `none`→`off`).
- New `crd::logging` (`Logging`, `LoggingType`, `ExternalLoggingSource`,
  `ConfigMapKeyRef`) and `controller::logging`
  (`compose_inline_filter` — pure + deterministic, directives sorted so
  the hash is stable; `resolve_logging` — inline composed in-process,
  external read via one `ConfigMap` GET; `LoggingOutcome` +
  `condition_for`).
- Delivery: the resolved filter is rendered into the cluster broker
  `ConfigMap` under a `rust.log` key (`common::render_configmap`), and
  each broker pod's `RUST_LOG` env points at it via `configMapKeyRef`
  (`optional: true` → pod stays bootable if the key is briefly absent,
  falling back to the broker default). The env entry is gated on
  `spec.logging.is_some()` — a logging-unset cluster keeps a
  byte-identical pod template (no spurious roll), same approach as the
  slice-40 metrics port/flag.
- `combined_config_hash` gains a sixth segment (the resolved filter,
  empty when unset) so a *value* change rolls the cluster via slice 21
  — the broker only re-reads `RUST_LOG` at startup. The slice-24
  empty-hash collapse is preserved (listeners + metricsConfig + CA cert +
  metadata pin + logging all absent → `config_hash(config_part)`).
- New `LoggingReady` condition (mirrors slice-40 `MetricsReady`):
  `False/Disabled` when unset; `True/Available` when resolved;
  `False/<reason>` for user errors (`InvalidLogLevel`,
  `EmptyLoggers`, `ExternalRefMissing`, `LoggingConfigMapNotFound`,
  `LoggingConfigMapKeyNotFound`). A user error surfaces the condition and
  leaves the broker on its default filter; a transient API error during
  the external GET propagates and requeues instead.
- Tests: +20 lib unit (`crd::logging` ×4, `controller::logging` ×14,
  `kafka_node_pool` env on/off ×2) + 4 reconcile integration tests
  (`reconcile_kafka.rs`: inline renders `rust.log`+`LoggingReady=True`,
  unset omits it, external reads the user ConfigMap, missing external CM
  surfaces the condition without rendering a key). Operator lib tests
  land at 339; full operator suite green; clippy `-D warnings` + fmt
  clean; CRD YAML regenerated (only the `logging` field added).
- kind-e2e: a new probe patches `demo` with inline logging, asserts the
  composed `rust.log` is rendered + `LoggingReady=True` + the broker STS
  pod template wires `RUST_LOG` to the `configMapKeyRef`, then resets.
- Out of scope (deferred): per-`KafkaNodePool` logging override (logging
  lives on the cluster spec; the pool reads `parent.spec.logging`); live
  log-level hot-reload without a restart (broker reads `RUST_LOG` only at
  startup — a future core control surface); log4j-name → tracing-target
  translation; OTLP / structured-logging knobs (slice 42 territory).
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-operator-logging-41-design.md`].

## Slice 42 — Crabka core: OTLP distributed tracing (2026-05-23)

- Continues Phase 6 (observability) after 39/40/41. Crabka-core slice: the
  broker gains an OpenTelemetry tracing pipeline that batch-exports spans
  over OTLP (gRPC `:4317` or HTTP/protobuf `:4318`). **Off by default** and
  driven entirely by env — a broker with no OTLP env behaves exactly as
  before. The operator-surfacing follow-up (`Kafka.spec.tracing`) is a
  later slice.
- New `crabka_broker::telemetry` module owns the whole pipeline:
  - `OtlpConfig::from_env(get, instance_id, version) -> Option<Self>` — a
    pure, injectable env resolver (the `get` closure is the only I/O);
    `None` means disabled. CRABKA vars win over the standard OTel vars:
    enable via `CRABKA_OTLP_ENDPOINT` / any `OTEL_EXPORTER_OTLP*ENDPOINT` /
    `CRABKA_OTLP_ENABLED`; `OTEL_SDK_DISABLED=true` force-disables. Protocol
    (`CRABKA_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_PROTOCOL`, default `grpc`),
    sample ratio (`CRABKA_OTLP_SAMPLE_RATIO` / `OTEL_TRACES_SAMPLER_ARG`,
    clamped to `[0,1]`), `OTEL_SERVICE_NAME`, export timeout.
  - `init(otlp, default_filter) -> TelemetryGuard` — installs the global
    subscriber: always a stdout `fmt` layer (the existing `RUST_LOG`
    behaviour), plus a `tracing-opentelemetry` batch-export layer when OTLP
    is on. Resource attrs `service.name` / `service.version` (crate version)
    / `service.instance.id` (broker id); sampler
    `ParentBased(TraceIdRatioBased(ratio))`. The guard's `shutdown()`
    flushes the final batch before exit.
  - `request_span(...)` + `api_name(api_key)`.
- **Per-request span on a dedicated `DEBUG` target** (`crabka_broker::request`):
  the `fmt` layer's default `info` filter never enables it (no stdout spam,
  zero cost on a no-OTLP broker), while the OTLP layer carries its own
  per-layer filter (`info,crabka_broker::request=debug,crabka_log=info`,
  overridable via `CRABKA_OTLP_FILTER`) that does. Span name = API name via
  `otel.name`; `otel.kind=server`; OTel-semconv attributes
  (`messaging.system`, `kafka.api_key`, `kafka.api_version`,
  `kafka.correlation_id`, `messaging.kafka.client_id`,
  `network.peer.address`).
- Dispatch instrumentation is uniform + additive: the span is built once per
  loop iteration (guarded by `tracing::enabled!` so the extra header parse
  only runs when OTLP is on) and attached to every handler future via
  `.instrument(req_span.clone())` — all 30 inline `handle_*_frame` arms and
  the generic `dispatch_one` fallback — plus `req_span.in_scope(..)` for the
  sync SASL path. No control-flow change; no api_key coverage gap.
- **Why broker-side root spans:** the Kafka request header carries no trace
  context (`RequestHeader` is non-flexible), and ecosystem tracing
  propagates `traceparent` via *record headers*, not the RPC. So requests
  start root server spans here; linking to a producing client's trace
  (record-header `traceparent` extraction in Produce/Fetch) is deferred to a
  follow-up that builds on this pipeline.
- **Runtime note:** the batch processor exports on a dedicated thread via
  `futures_executor::block_on`; the gRPC (tonic) exporter therefore needs
  the provider built inside a tokio runtime to capture the handle —
  `telemetry::init` is called from the broker `#[tokio::main]`, satisfying
  it. HTTP/protobuf uses the blocking reqwest client (no such requirement).
- New broker workspace deps: `opentelemetry` 0.32, `opentelemetry_sdk` 0.32,
  `opentelemetry-otlp` 0.32 (`grpc-tonic` + `http-proto` +
  `reqwest-blocking-client`), `tracing-opentelemetry` 0.33. The 0.32 line
  lines up with the tonic 0.14 / prost 0.14 / reqwest 0.13 stack already in
  the graph — the lockfile grows by only 7 crates (all Apache-2.0 / MIT).
- Tests: +11 lib unit (`telemetry`): env resolution paths, endpoint
  precedence, `OTEL_SDK_DISABLED`, protocol parse/defaults, sample-ratio
  parse+clamp, service-name/timeout overrides, the `api_name` table, and a
  `request_span` test driving a capturing `tracing` `Layer` (scoped via
  `with_default`) to assert `otel.name`/`otel.kind`/`kafka.api_key`. Broker
  lib tests at 330; full broker suite green (the instrumented dispatch path
  is exercised unchanged with spans disabled). clippy `-D warnings` + fmt
  clean.
- Out of scope (deferred): record-header `traceparent` extraction (the
  cross-process link); OTLP metrics/logs signals (metrics stay on the
  slice-39 Prometheus endpoint); operator `Kafka.spec.tracing` surfacing;
  per-response error-code span attributes.
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-broker-otlp-tracing-42-design.md`].

## Slice 34 — Operator: CA rotation orchestration (2026-05-23)

- Closes Phase 4 (security & certificate management). Turns slice-30's
  *disruptive* CA-expiry path (which only set a `CaRotationRequired=True`
  condition + Event) into hands-off, zero-downtime rotation, building on
  slice-33 cert/truststore hot-reload + slice-21/28 ordered rolling
  restart.
- The cluster-CA and clients-CA cert Secret (`<cluster>-cluster-ca-cert`
  / `-clients-ca-cert`, key `ca.crt`) is now a multi-generation PEM
  **trust bundle**, signing cert first. Steady state is a single cert
  (byte-identical to slice 30). The broker's `client_ca_path` already
  loads all blocks from the file (slice 33), so a bundle "just works" as
  a truststore; the signing path (`issue_broker_cert`, `cert_not_after`)
  reads the first block.
- Two rotation modes, both zero-downtime:
  - **Same-key cert renewal** (automatic on expiry, or
    `crabka.io/force-renew-ca`): re-sign the CA cert reusing the existing
    key with a fresh `validityDays`, prepend it to the bundle, prune
    expired anchors. One ordered roll distributes the new bundle; broker
    leafs are untouched (same key → same SPKI → existing leafs still
    chain). New `crabka_security::ca::renew_cluster_ca` /
    `renew_clients_ca`.
  - **Cluster-CA key replacement** (`crabka.io/force-replace-ca-key`): a
    staged three-phase machine — `key-replace-trust` (generate new
    key+cert, add the new cert to the bundle as trust-only, stage the new
    key under `ca.key.next`/`ca.crt.next`, roll to distribute trust) →
    `key-replace-promote` (promote the staged key to signer, move the new
    cert to the front, reissue every broker leaf with the new key, roll)
    → prune the old anchor (roll back to a single-cert bundle, `idle`).
    Each phase advances only once the prior roll has converged.
- The whole decision is a pure function `plan_ca_rotation(state, inputs)`
  (mirrors `version::evaluate` / `logging::resolve_logging`), so the
  staged machine is exhaustively unit-testable despite the FIFO
  integration mock. The reconciler executes the returned plan via
  `apply_ca_rotation`.
- "No roll in flight" (the convergence gate) is computed from pool state
  alone: every `KafkaNodePool` carries the same non-empty
  `crabka.io/config-hash` label AND is Ready.
- The config-hash already hashes `ca.crt`; making it a bundle means the
  hash now covers the whole cluster-CA trust set for free — adding /
  promoting / pruning an anchor rolls the cluster, while same-key leaf
  renewal (slice-33 hot-reload) does not. (Clients-CA cert is NOT in the
  hash; its truststore is hot-reloaded.)
- Triggers are one-shot `Kafka` CR annotations (`force-renew-ca`,
  `force-replace-ca-key`), stripped after they're consumed. The slice-30
  `ca-renewal-check` CronJob no longer sets `CaRotationRequired`; for an
  operator-managed CA within `renewalDays` it now stamps a one-shot
  `crabka.io/ca-renew-after` annotation that nudges the reconciler, and
  emits a Normal `CaRenewalScheduled` Event. BYO CAs are still never
  rotated (a forced rotation is refused with a `CaRotation` condition +
  Warning Event).
- New `CaRotation` status condition (driven by the cluster CA):
  `False/Idle`, `True/RenewingCert`, `True/DistributingTrust`,
  `True/PromotingKey`, `False/ByoCaImmutable`,
  `False/ClientsCaKeyReplaceUnsupported`. `CertificateAuthorityStatus`
  gains `certGeneration`, `keyGeneration`, `rotationPhase`,
  `trustAnchors`.
- Clients-CA *key* replacement is deferred (it additionally needs
  re-signing every KafkaUser mTLS cert, owned by the slice-37
  controller); the clients CA gets the bundle + same-key renewal +
  auto-prune only.
- Tests: +4 `crabka_security::ca` unit tests (renew reuses key,
  preserves subject incl. `OU=cluster`, extends validity, leaf-still-
  chains) + 16 `controller::cluster_ca` rotation unit tests (bundle
  helpers, full `plan_ca_rotation` decision table, same-key renewal
  chaining) + new `reconcile_ca_rotation` integration tests; slice-30 BYO
  test + the `ca_renewal_cronjob` flag test updated for the new nudge
  behaviour. Operator lib tests, full operator suite, security suite
  green; clippy `-D warnings` + fmt clean; CRD YAML regenerated (only the
  new status fields).
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-operator-ca-rotation-34-design.md`].

## Slice 45 — Crabka core: JBOD / multi-log-dir + DescribeLogDirs (KIP-113) (2026-05-23)

- Opens Phase 8 (storage gaps). The broker becomes a real JBOD broker:
  partition data spreads across multiple on-disk log directories on one
  broker, and `kafka-log-dirs --describe` works. This is the read +
  placement half of KIP-113; the intra-broker replica *move*
  (`AlterReplicaLogDirs`, api 34) is deferred to slice 45b.
- **Config:** `BrokerConfig.extra_log_dirs: Vec<PathBuf>` (default empty) +
  `all_log_dirs()` → `[log_dir] + extra_log_dirs`, deduped, primary first.
  `log_dir` keeps its meaning (primary + `__cluster_metadata` + default data
  dir), so the ~100 existing config sites that build via `..default()` /
  `for_tests` are untouched — only the two constructors gain the field. CLI
  `--log-dirs a,b` (env `CRABKA_EXTRA_LOG_DIRS`); TOML `extra_log_dirs`.
- **Placement** (`crates/broker/src/log_dir.rs`): stateless
  `place_partition_dir(log_dirs, topic, partition)` — existing on-disk
  location wins (idempotent across restart / re-materialize), else the dir
  with the fewest `topic-partition` subdirs (`count_partitions`), ties by
  order. Matches Kafka's default round-robin-by-count. It runs inside
  `materialize_partition`'s `DashMap::entry` arm, so two concurrent
  materializations of one partition can never split across dirs. `scan_all`
  discovers `(topic, partition, owning_dir)` across all dirs (first dir wins
  on duplicate, logged).
- **Threading:** every materialization site funnels through `all_log_dirs()`
  + `place_partition_dir` — startup recovery (`scan_all`), the replicator
  supervisor (`materialize_partition` now takes `&[PathBuf]`), the follower
  replicator (`Config.log_dirs` + `ensure_local_partition`),
  `__consumer_offsets` bootstrap, and the `CreateTopics` / `CreatePartitions`
  / `InitProducerId` handlers. `DeleteTopics` resolves the real dir before
  `remove_dir_all`. The slice-43e disk scanner walks all dirs.
  `__cluster_metadata` is unaffected — always on the primary `log_dir`;
  bootstrap detection unchanged. Log-dir assignment is broker-local, not in
  cluster metadata (exactly like Kafka — no per-replica field in
  `PartitionRecord`).
- **`DescribeLogDirs` handler** (api 35, v1–5): one `Results` entry per
  configured dir (canonical absolute path), listing the partitions
  physically present with `partition_size` (sum of file bytes) and
  `offset_lag` (`max(0, LEO − HW)` for the loaded current log).
  `is_future_key=false` (no future logs this slice); `total_bytes` /
  `usable_bytes` keep the generated `-1` ("unknown"). Reflects the
  filesystem (scans per request) so it can't drift. Registered in the
  handler table, advertised in `ApiVersions`, and added to
  `handler_body_flexible` (api 35) so the flexible request/response headers
  are framed correctly. No protocol regeneration — the KIP-113 message types
  were already generated from the Kafka 4.3.0 schemas.
- Tests: +7 lib unit (`log_dir`: count/place/scan_all) +3 (`describe_log_dirs`
  filter); broker integration `tests/jbod.rs`
  (`partitions_spread_across_dirs_and_describe_log_dirs_reports_them` — 2-dir
  broker, 6-partition topic, asserts on-disk spread + wire `DescribeLogDirs`
  v4 union); JVM acceptance `jvm_kafka_log_dirs_describe_reports_jbod_spread`
  (`#[ignore]`, `kafka-log-dirs --describe` against a two-dir host broker).
  Full broker suite green; clippy `-D warnings` + fmt clean.
- Out of scope (deferred to 45b): `AlterReplicaLogDirs` move + future-log
  catch-up; `total_bytes` / `usable_bytes` via statvfs;
  `kafka-reassign-partitions` per-replica `log_dirs`; offline-dir /
  `KAFKA_STORAGE_ERROR` handling. Operator JBOD surface is slice 46.
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-jbod-multi-log-dir-45-design.md`].

## Slice 46 — Operator: JBOD in `KafkaNodePool.spec.storage` (2026-05-23)

- Surfaces slice 45's broker-side JBOD through the operator: a
  `KafkaNodePool` can now back its pods with **multiple PVCs**, one per
  JBOD disk, and the broker spreads partition data across all of them.
  Phase 8, the operator half of the slice-45 core work.
- **CRD:** new `Storage::Jbod(JbodSpec)` variant (alongside `Ephemeral`
  / `PersistentClaim`). `JbodSpec { volumes: Vec<JbodVolume>, delete_claim
  }`; `JbodVolume { id, size, class }`. Flat tagged wire shape
  (`type: Jbod`, sibling `volumes` array + `deleteClaim`). The hand-rolled
  `storage_schema` gains the `Jbod` enum value and the `volumes` array
  (kube-rs 3.x can't emit the schemars tagged-union schema). JBOD requires
  **>= 2 disks** — a single-disk JBOD is just a `PersistentClaim` and would
  render an identical one-PVC StatefulSet, making the storage kind
  ambiguous on re-reconcile (the observed kind is derived from the live
  STS's `volumeClaimTemplates` count: 0 = Ephemeral, 1 = PersistentClaim,
  >= 2 = Jbod).
- **Layout (zero broker / init-script / main-script / cluster-TOML
  change):** the **lowest-id disk is primary** — it keeps the slice-24 PVC
  name `data` / mount `/var/lib/crabka/data`, so the `__cluster_metadata`
  raft log, the init container (`crabka format --log-dir
  /var/lib/crabka/data`), and the cluster-level broker TOML (`log_dir =
  "/var/lib/crabka/data"`) are all untouched. Every other disk `id = N`
  gets PVC `data-{N}` mounted at `/var/lib/crabka/data-{N}`, an extra
  broker-container `volumeMount`, and is handed to the broker via the
  existing `CRABKA_EXTRA_LOG_DIRS` env (slice 45) — comma-joined, sorted by
  id, primary excluded. Disks are sorted by id before rendering so the pod
  template is deterministic regardless of YAML order. Non-JBOD pools render
  byte-identically (no env, no extra mounts).
- **Retention:** a `StatefulSet`'s
  `persistentVolumeClaimRetentionPolicy.whenDeleted` is set-wide (K8s has
  no per-template retention), so JBOD exposes a single `deleteClaim`
  covering all disks (diverging from Strimzi's per-volume flag — Crabka is
  Strimzi-shaped, not -compatible, and delegates PVC GC to K8s).
- **Validation:** static — non-empty, >= 2 disks, unique ids, every `size`
  a positive `Quantity`. Monotonic (vs the live STS's templates) — rejects
  storage-type switches, per-disk `class` change + size shrink, and JBOD
  disk-set changes (add/remove/primary-reassign deferred until KIP-113
  intra-broker moves land, slice 45b). Disks match by identity: `data` ↔
  desired primary (lowest id), `data-{N}` ↔ desired id N.
- Tests: +2 CRD round-trip, +19 controller unit (render/validate/monotonic),
  +1 reconcile integration (`pool_jbod_renders_multiple_volume_claim_templates`
  — asserts two PVC templates, set-wide retention, and the env in the SSA
  body). CRD YAML regenerated. operator-e2e gains an isolated JBOD smoke
  step (2×1Gi pool in its own namespace: both PVCs `Bound`, both disks
  mounted, `CRABKA_EXTRA_LOG_DIRS` set). Full operator suite + clippy
  `-D warnings` + fmt clean.
- Out of scope (deferred): adding/removing JBOD disks on a live pool
  (needs `AlterReplicaLogDirs`, slice 45b); per-disk `deleteClaim`;
  ephemeral disks inside a JBOD set; `KafkaNodePool.status.storage` mirror.
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-operator-jbod-storage-46-design.md`].

## Slice 49 — Crabka core: SASL/OAUTHBEARER (KIP-255 / RFC 7628) (2026-05-23)

- Adds `OAUTHBEARER` as a fourth broker-side SASL mechanism alongside
  `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`. A client presents a bearer
  token in an RFC 7628 SASL exchange; the broker validates it and derives the
  connection principal from a token claim. Phase 9 (auth extensions) core
  work — unblocks operator slice 50 (`KafkaUser` OAuth + listener OAuth
  config).
- **Validator:** the concrete validator is Kafka's **unsecured JWS**
  (`alg:none`) path — the built-in, no-external-dependency mode the JVM
  `OAuthBearerLoginModule` / `OAuthBearerUnsecuredValidatorCallbackHandler`
  use for dev/test. Signed-token validation against a JWKS endpoint
  (RS256/ES256 signature, issuer/audience, key rotation) is deferred to
  slice 49b. The mechanism plumbing (handshake, RFC 7628 parse, two-round
  failure handshake, principal extraction) is validator-agnostic and reused
  unchanged by 49b.
- **`crates/security`:** `SaslMechanism::OAuthBearer` (wire `OAUTHBEARER`);
  `AuthMethod::SaslOAuthBearer`; `AuthError::InvalidToken`. New
  `oauthbearer.rs` (pure logic): `parse_client_initial_response` (RFC 7628
  GS2 + kvpair parse → bearer token + optional authzid),
  `UnsecuredJwsValidator` (validates `alg:none`, required `exp`, optional
  `iat`, optional required scope; principal from a configurable claim,
  default `sub`), and `invalid_token_json`. `serde_json` added as a dep.
- **`crates/broker` (`network/auth.rs`):** `SaslExchange::OAuthBearer` +
  `OAuthBearerFailed`; `handle_authenticate_oauthbearer` implements the
  Kafka state machine — **single round on success** (empty `auth_bytes`,
  `error_code 0`, `Authenticated`); **two rounds on failure**
  (`{"status":"invalid_token"}` JSON in `auth_bytes` with `error_code 0`, the
  connection stays open; the client's `\x01` dummy then yields `error_code 58`
  + close). The dispatcher's existing `close = error_code != 0` rule produces
  the correct close timing for both rounds with no special-casing. A
  non-empty client authzid must equal the token principal.
- **Wiring:** `dispatch.rs` routes `OAUTHBEARER` (computing `now_ms` from
  `SystemTime`); `BrokerConfig.oauthbearer_validator` (default unsecured,
  `sub`, 30s skew) is consulted only when `OAUTHBEARER` is in
  `enabled_sasl_mechanisms` (handshake won't advertise it otherwise);
  `[oauthbearer]` TOML section in `FileConfig` (principal/scope claim names,
  required scope, clock-skew ms). Outbound paths (`network/client.rs`
  inter-broker, `raft_handshake.rs` controller listener) return an explicit
  "OAUTHBEARER not supported" error — it is a client mechanism, not an
  inter-broker one. The SCRAM-only credential-byte helpers fold the
  non-SCRAM mechanism into the `UNKNOWN` (0) arm.
- Tests: +13 security unit (parser happy/malformed, validator
  accept/expired/future-iat/signed/missing-exp/missing-principal/
  required-scope string+array/custom-claim, error JSON shape); +6 broker
  `network::auth` unit (handshake advertise, authenticate success, two-round
  failure, malformed, authzid mismatch); +2 broker wire integration in
  `auth_handlers.rs` (no Docker): full `ApiVersions` → `SaslHandshake` →
  `SaslAuthenticate` → Metadata happy path, and the expired-token two-round
  `invalid_token` → 58 failure handshake; +1 `#[ignore]` JVM acceptance
  (`jvm_sasl_oauthbearer_produce_consume`) driving
  `kafka-console-producer`/`-consumer` with the unsecured login module.
  Workspace clippy `-D warnings` + fmt clean.
- Out of scope (deferred): JWKS / signed-JWT validation (49b); token
  re-authentication + `session_lifetime_ms` expiry, KIP-368 (49b);
  OAUTHBEARER for inter-broker / controller listeners; `KafkaUser` OAuth +
  `Kafka.spec` listener OAuth config (operator slice 50).
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-sasl-oauthbearer-49-design.md`].

## Slice 49b — Crabka core: SASL/OAUTHBEARER JWKS / signed-JWT validation (2026-05-23)

- Promotes OAUTHBEARER from the slice-49 development-only *unsecured JWS*
  (`alg:none`) validator to production **signed-JWT validation against a JWKS
  endpoint**. Clients now present real OAuth 2.0 access tokens (a JWS signed by
  the identity provider); the broker verifies the signature against the IdP's
  published JSON Web Key Set, checks the standard JWT claims, and derives the
  principal from a configured claim. Unblocks the production path for operator
  slice 50 (`KafkaUser` OAuth + listener OAuth config). The RFC 7628 wire state
  machine (handshake, single-round success / two-round failure, authzid match)
  is reused unchanged — only the validator behind it changed.
- **`crates/security` (pure logic, no I/O):**
  - New `jwks.rs`: `Jwks` (RFC 7517 key set parsed from JSON; RSA `n`/`e` and
    EC P-256 `x`/`y` keys indexed by `kid`; encryption-use / unsupported `kty` /
    non-P-256 keys skipped, not fatal) + `Jwks::verify(kid, alg, signing_input,
    sig)` for `RS256` (RSASSA-PKCS1-v1_5+SHA-256) and `ES256` (ECDSA
    P-256+SHA-256) via `ring` — no JWT crate, no second crypto backend, and
    `now_ms` stays injectable. `JwksHandle` = `Arc<ArcSwap<Jwks>>` (mirrors
    slice 33's `DynamicServerConfig`) for lock-free key rotation.
  - `oauthbearer.rs`: `SignedJwsValidator` (config: principal/scope claim,
    required scope, clock skew, expected issuer, expected audience + a
    `JwksHandle`) — `validate` checks 3-segment JWS with non-empty signature,
    `alg ∈ {RS256, ES256}`, signature, then `exp` (required) / `iat` / `nbf`
    (skew-tolerant), `iss`, `aud` (string or array), scope, principal.
    `OAuthBearerValidator` enum = `Unsecured | Signed` (default `Unsecured`);
    the broker holds one and the `SaslAuthenticate` handler dispatches on it.
- **`crates/broker`:**
  - New `oauth_jwks.rs`: `JwksRefresher` background task (`reqwest`, rustls)
    fetches the JWKS endpoint and `store`s the parsed key set into the shared
    handle on an interval; the first fetch fires immediately, fetch failures
    log a warning and keep the previous set (a transient IdP outage never
    crashes the broker). `BrokerConfig.oauthbearer_validator` is now the enum;
    new `oauthbearer_jwks_endpoint` + `oauthbearer_jwks_refresh_interval`
    (default 5 min). `[oauthbearer]` TOML gains `jwks_endpoint_uri`,
    `valid_issuer_uri`, `expected_audience`, `jwks_refresh_interval_ms` —
    setting `jwks_endpoint_uri` selects the signed validator and
    `Broker::start` spawns the refresher (sharing the validator's key handle).
- Tests: +13 security unit (`jwks`: parse mixed RSA+EC, skip enc/unsupported,
  reject non-JSON; verify RS256/ES256; reject tampered sig / unknown kid /
  ambiguous-missing-kid / wrong key / alg-key mismatch; handle round-trip) +12
  security unit (`oauthbearer`: signed accept, reject unsecured-alg-none /
  expired / future-nbf, issuer + audience string&array + required-scope honor,
  missing principal, custom claim, key rotation via handle, empty keyset,
  enum dispatch); +2 broker `file_config` (jwks→Signed, no-jwks→Unsecured); +3
  broker `oauth_jwks` (fetch served keyset, error on dead endpoint, refresher
  populates handle then stops); +2 broker wire integration in
  `auth_handlers.rs` (signed-token happy path + wrong-key two-round failure,
  no Docker). Workspace clippy `-D warnings` + fmt clean.
- RSA test tokens are minted at runtime from a static embedded RSA-2048 PKCS#8
  key (`ring` can't generate RSA); ES256 keys are generated fresh per test.
  Production never touches a private key — it reads `n`/`e` (RSA) and `x`/`y`
  (EC) from the IdP's JWKS JSON.
- Out of scope (deferred): KIP-368 token re-authentication +
  `session_lifetime_ms` connection expiry (49c); opaque-token introspection
  (RFC 7662); custom CA trust / mTLS to the JWKS endpoint (webpki/Mozilla roots
  only); RS384/512, ES384/512, PS256; OAUTHBEARER for inter-broker / controller
  listeners; `KafkaUser` OAuth + `Kafka.spec` listener OAuth config (slice 50).
- No JVM acceptance test: the JVM unsecured login module mints only `alg:none`
  tokens, and a signed-token JVM test needs a live OAuth server; the slice-49
  unsecured JVM test still covers the wire handshake, and the signature path is
  covered by the Rust integration tests above.
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-sasl-oauthbearer-jwks-49b-design.md`].

## Slice 50 — Operator: Listener OAuth + `KafkaUser` tls-external (2026-05-23)

- Surfaces the broker-side OAUTHBEARER work from slices 49 + 49b through the
  operator: `Kafka.spec.listeners[].authentication.type: oauth` is now a valid
  discriminator (with the Strimzi `KafkaListenerAuthenticationOAuth` v1 field
  shape — issuer URI, JWKS endpoint, refresh / clock-skew, audience, principal
  + scope claims, single `customClaimCheck`), and `KafkaUser.spec.type:
  tls-external` declares an OAuth-/external-CA-owned principal whose
  credentials the operator deliberately does **not** manage. Together this is
  the minimum surface needed to run an end-to-end OAUTHBEARER deployment
  through the operator; the rest of Strimzi's listener-OAuth knobs follow as
  the umbrella's 50b–50f sub-slices.
- **`crates/operator` (`crd/listener.rs`):**
  `KafkaListenerAuthentication::OAuth` arm holds `valid_issuer_uri`,
  `jwks_endpoint_uri` (both `MinLength = 1`), `jwks_refresh_seconds`,
  `max_clock_skew_seconds`, `user_name_claim` (default `sub`),
  `valid_audience`, `enable_oauth_bearer`, and a single
  `custom_claim_check: Option<CustomClaimCheck { scope, scope_claim }>`. All
  sibling fields are flattened into the `authentication` object — schemars
  emits them as peers of `type` rather than nested under a `oauth:` object,
  matching Strimzi's JSON shape. CRD-only validation rejects bad URIs and
  `enableOauthBearer: false`; cross-field rules (`tls: true` required) live in
  the reconciler.
- **`crates/operator` (`controller/listeners.rs`):**
  reconciler validates the OAuth arm (listener is `tls: true`, `validIssuerUri`
  non-empty, `jwksEndpointUri` is `http://` or `https://`, `jwksRefreshSeconds
  >= 30` when set, `customClaimCheck.scope` non-empty when present) plus the
  cross-listener constraint that all OAuth listeners share one canonical
  config (because the broker's `[oauthbearer]` section is global until 49h),
  renders the broker-global `[oauthbearer]` TOML block consumed by the 49b
  broker (`jwks_endpoint_uri`, `valid_issuer_uri`, `expected_audience`,
  `principal_claim_name`, `scope_claim_name`, `required_scope`,
  `jwks_refresh_interval_ms`, `allowable_clock_skew_ms`), and writes
  `OAUTHBEARER` into the listener's enabled SASL mechanism list so the
  broker's handshake advertises it. Field ordering in the rendered TOML is
  pinned for byte-stable output (so reconcile is a no-op when nothing
  changed); a divergence test fires per-field. An `http://` `jwksEndpointUri`
  is accepted but emits a `WeakAuth` Event (mirroring SCRAM-without-TLS).
- **`crates/operator` (`crd/user.rs` + `controller/user.rs`):**
  `KafkaUserAuthType::TlsExternal` is a no-credential variant — the operator
  never mints a key/cert, never writes a Secret, never owns the principal,
  but still reconciles ACLs and renders status. New
  `KafkaUserStatus.external: bool` (default `false`) flips to `true` when a
  `tls-external` user has been observed, so `kubectl describe ku` shows at a
  glance that credentials live outside the cluster. The TlsExternal arm
  short-circuits the Secret-management path entirely; ACL reconciliation is
  reused unchanged.
- **`crates/operator/sample/oauth-listener.yaml`:** end-to-end sample wiring
  an external OAuth listener on `:9096` (TLS + OAUTHBEARER, JWKS endpoint
  placeholder) plus a `tls-external` `KafkaUser` with topic ACLs — what a
  user copies to bootstrap an OAuth deployment.
- **`deploy/crds/*.yaml`:** regenerated. `crabka.io_kafkas.yaml` gains the
  `oauth` discriminator value + 8 sibling OAuth fields under
  `listeners[].authentication`; `crabka.io_kafkausers.yaml` gains
  `tls-external` to the `type` enum and a `status.external: boolean` with a
  description. No other CRDs touched.
- **`.github/workflows/operator-e2e.yml`:** new `kind-oauth` job — stands
  up a kind cluster + a real Keycloak (Bitnami chart pinned at `25.2.0`,
  TLS off because broker→IdP HTTPS with custom trust is 49c territory),
  bootstraps a `kafka` realm via `kcadm.sh`, applies the OAuth sample,
  runs two `apache/kafka:3.8.0` Jobs — one with a scoped client-credentials
  token (asserts SASL+ACL success) and one with a no-scope client (asserts
  SaslAuthenticationException). Asserts the `WeakAuth` Event for the
  in-cluster `http://` JWKS URL. Gated on `push: main` or PRs labeled
  `e2e-oauth` to keep CI latency under control on unrelated PRs.
- Tests: +35 operator unit (CRD parse / validation / schema regression
  across `crd/listener.rs` +7, `crd/user.rs` +5, reconciler `controller/
  listeners.rs` +20 incl. happy-path render, per-field divergence, TLS-not-
  enabled rejection, bad-URI scheme rejection, empty-scope rejection, byte-stable
  ordering; `controller/user.rs` +3 for TlsExternal arm + status.external
  flip); +18 integration (`tests/reconcile_listener_oauth.rs` +12 driving
  the full OAuth listener reconcile loop, `tests/reconcile_user_tls_external
  .rs` +6 covering no-Secret-created, ACL-only reconcile, status.external
  flip on first observe). Workspace clippy `-D warnings` + fmt clean.
- Out of scope (deferred to the OAUTHBEARER-parity umbrella): listener
  `tlsTrustedCertificates` for custom CA trust to the IdP (49c + 50b);
  opaque-token introspection (49d + 50c); KIP-368 re-authentication
  (`maxSecondsWithoutReauthentication`, 49e + 50d); PLAIN-with-OAuth-token
  + `tokenEndpointUri` (49f + 50e); the remaining Strimzi long-tail —
  `groupsClaim`, fallback-username chain, `validTokenType`, multi-rule
  `customClaimCheck`, JWKS refresh policy knobs, `jwksIgnoreKeyUse`
  (49g + 50f).
- Reference docs:
  [`docs/superpowers/specs/2026-05-23-crabka-operator-listener-user-oauth-50-design.md`],
  [`docs/superpowers/specs/2026-05-23-crabka-oauth-parity-roadmap-design.md`],
  [`docs/superpowers/plans/2026-05-23-crabka-operator-listener-user-oauth-50.md`].

## Slice 49c — Broker: Custom TLS trust to IdP for JWKS (2026-05-23)

- Adds an optional, broker-side custom TLS trust bundle for the JWKS endpoint
  fetcher introduced in slice 49b: until now the `JwksRefresher` only reached
  IdPs whose certs chain to webpki / Mozilla roots; this slice lets a
  deployment point at a private CA (corp PKI, in-cluster Keycloak with a
  self-signed cert) by passing a PEM bundle path. Unblocks slice 50b's
  operator `tlsTrustedCertificates` listener surface (which will mount a
  user-supplied `Secret` and pass the file path through) and the Keycloak
  `kind` e2e upgrade from HTTP to HTTPS.
- **`crates/security`:** new `jwks_trust.rs` with
  `build_client_config_from_pem(path: &Path) -> Result<Arc<rustls::ClientConfig>, JwksTrustError>`
  — parses one-or-many concatenated PEM `CERTIFICATE` blocks into an empty
  `RootCertStore` (no webpki / Mozilla roots), returns a `ClientConfig` with
  no client auth. Strimzi-shaped **replace** semantic: when set the user PEM
  is the *exclusive* trust store, not additive. Reusable as-is for slice
  49d's opaque-token introspection client.
- **`crates/broker`:** new `[oauthbearer].jwks_tls_trust` TOML key in
  `FileOAuthBearerConfig` and matching `BrokerConfig.oauthbearer_jwks_tls_trust:
  Option<PathBuf>` runtime field (default `None`, webpki-roots behaviour
  preserved when unset). `JwksRefresher::run` reads the PEM at startup,
  calls the security helper, and wires the resulting `rustls::ClientConfig`
  into the `reqwest` builder via `use_preconfigured_tls(...)`. PEM-load
  failure is a hard-stop for the refresher (logged at `tracing::error`); the
  broker stays up and OAUTHBEARER signed-token validation degrades
  gracefully (handle empty → unknown-kid).
- Tests: +5 security unit (`jwks_trust`: load single cert, load concatenated
  chain, missing-file, empty-bytes, garbage-bytes-rejected); +2 broker
  `file_config` unit (path threads through TOML; absent key defaults to
  `None`); +2 broker HTTPS integration via a `tokio-rustls` server with an
  `rcgen` self-signed cert (happy path: refresher populates the handle;
  mismatched-trust: handle stays empty). Workspace `cargo clippy --tests -D
  warnings` + `cargo fmt --check` clean.
- Out of scope (deferred): operator CRD field + `Secret`-mounting wiring
  (50b); opaque-token introspection, which will reuse this helper under a
  new `introspection_tls_trust` key (49d); hot reload of the trust bundle
  (refresher / broker restart required to pick up a rotated CA); multiple
  PEM paths in one key (the operator concatenates before mounting, mirroring
  Strimzi); cert pinning; mTLS to the IdP.
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-broker-jwks-tls-trust-49c-design.md`].

## Slice 50b — Operator: Listener OAuth `tlsTrustedCertificates` (2026-05-23)

- Surfaces 49c's broker-side `[oauthbearer].jwks_tls_trust` knob on the
  CRD as a Strimzi-shaped `tlsTrustedCertificates` array on
  `listeners[].authentication.type: oauth`. The operator concatenates
  the named PEM keys from each referenced `Secret` into one managed
  `Secret` and mounts it read-only into broker pods at
  `/etc/crabka/oauth-jwks-trust/ca.crt`, which the broker loads as the
  *exclusive* JWKS trust root. With this the `kind-oauth` e2e is
  upgraded from HTTP to HTTPS using Keycloak's auto-generated cert,
  closing the last OAUTHBEARER-with-private-CA gap.
- **`crates/operator/src/crd/listener.rs`:** new
  `TlsTrustedCertificate { secret_name, certificate }` struct + a
  `tls_trusted_certificates: Vec<TlsTrustedCertificate>` sibling on
  `ListenerAuthenticationOAuth` (Strimzi JSON shape). Schema regression
  pins it as `array` of `object` with `required: [secretName,
  certificate]`, both `minLength: 1`.
- **`crates/operator/src/controller/listeners.rs`:** rendered
  `[oauthbearer]` block gains `jwks_tls_trust =
  "/etc/crabka/oauth-jwks-trust/ca.crt"` when the canonical tuple has
  trust certs (omitted otherwise so webpki-roots behaviour is
  preserved). Cross-listener canonical-tuple comparison includes the
  new field via derived `Eq` (no manual masking); per-field divergence
  walk is extended so a mismatched trust list yields a precise conflict
  message.
- **`crates/operator/src/controller/kafka.rs`:** new
  `reconcile_oauth_jwks_trust` helper reads each source `Secret` from
  the Kafka CR's namespace, concatenates the named PEM keys in declared
  order, and upserts a managed `Secret` named
  `{kafka.name}-oauth-jwks-trust` (single key `ca.crt`, owner-ref to
  the Kafka CR). New `pub(crate) fn oauth_jwks_trust_secret_name`
  feeds the pool reconciler. Three new `ReconcileError` variants
  (`MissingOauthTrustSecret`, `MissingOauthTrustKey`,
  `EmptyOauthTrustValue`) surface as `Ready=False` conditions naming
  the offending `secretName`/`certificate` pair.
- **`crates/operator/src/controller/kafka_node_pool.rs`:** an
  `Option<&str>` trust-secret-name param threads through
  `render_storage` + `render_broker_container`; when `Some`, the
  StatefulSet pod template gains a `secret` volume (defaultMode
  `0o400`) and a read-only mount at `/etc/crabka/oauth-jwks-trust`.
  `render_storage`'s match arms (`Ephemeral` / `PersistentClaim` /
  `Jbod`) were refactored for uniform volume-list assembly.
- **Sample + CRD regen:** `crates/operator/sample/oauth-listener.yaml`
  gains a `tlsTrustedCertificates` example pointing at a `keycloak-ca`
  Secret; `deploy/crds/crabka.io_kafkas.yaml` regenerated.
- **E2E (`.github/workflows/operator-e2e.yml`):** `kind-oauth` upgraded
  HTTP → HTTPS. Bitnami Keycloak chart `25.2.0` with `tls.enabled=true
  tls.autoGenerated=true`; the chart's auto-generated CA `Secret` is
  copied cross-namespace into `default` as `keycloak-ca`; the sample
  Kafka CR declares it via `tlsTrustedCertificates`; producer/consumer
  `apache/kafka:3.8.0` Jobs' JKS truststore is extended with the
  Keycloak CA via `keytool -importcert`; kcadm bootstrap uses `--server
  https://localhost:8443/ --insecure`; the `WeakAuth` Event assertion
  is inverted (no longer fires now that the JWKS URL is `https://`).
- Tests: +3 `crd::listener` unit, +2 `controller::listeners` (TOML
  render + divergence-walk extension), +3 `controller::kafka` unit
  (canonical helpers + managed-Secret name + error variants), +3
  `controller::kafka_node_pool` (StatefulSet volume mount across all
  three storage shapes), +2 `tests/reconcile_listener_oauth.rs`
  integration, +8 `tests/reconcile_oauth_trust.rs` integration (happy
  path concat, missing source Secret, missing key, empty value,
  no-trust-certs no-op, source-rotation re-render, StatefulSet
  mount-when-some, StatefulSet omit-when-none).
  Workspace clippy `-D warnings` + `cargo fmt --check` clean.
- Out of scope (deferred): source-`Secret` reflector for instant
  rotation pickup; cross-namespace `Secret` refs; mTLS *to* the IdP;
  per-listener `[oauthbearer]` config (cross-listener canonical-tuple
  rule still rejects divergent OAuth configs — future 49h); active
  managed-Secret cleanup when `tlsTrustedCertificates` is emptied
  mid-life (cascades on Kafka CR delete via owner-ref).
- Reference doc:
  [`docs/superpowers/specs/2026-05-23-crabka-operator-oauth-tls-trust-50b-design.md`].

## Slice 49d — Broker: OAUTHBEARER opaque-token introspection (2026-05-24)

- Adds RFC 7662 OAuth 2.0 token introspection alongside the JWKS / signed-JWT
  path landed in slice 49b: the broker now accepts opaque bearer tokens
  issued by IdPs that don't return JWTs (or where the deployment prefers
  the IdP to remain authoritative on revocation). Unblocks the operator
  slice 50c, which will surface `introspectionEndpointUri` on the listener
  CRD. Same slice also renames the slice-49c `[oauthbearer].jwks_tls_trust`
  TOML key to `idp_tls_trust` — one trust bundle now covers all outbound
  HTTPS to the IdP (JWKS, introspection, userinfo).
- **`crates/security`:** new `IntrospectionValidator` (the RFC 7662
  validator) backed by a new `IntrospectionClient` trait (`introspect` +
  optional `userinfo`) so the security crate stays I/O-free; new
  `IntrospectionError` enum (`Transport` / `Status` / `Parse`) and an
  `AuthError::IntrospectionTransport` variant on the existing auth error.
  `OAuthBearerValidator::validate` becomes `async fn` so the introspection
  variant can `.await` the HTTP call; the Unsecured and Signed variants
  wrap the synchronous validation in `async {}` (zero runtime cost). The
  enum is built so adding 49f device-grant or hybrid validators is a
  variant-and-arm change rather than a trait redesign.
- **`crates/broker/src/oauth_introspection.rs`:** new
  `ReqwestIntrospectionClient` implementing `IntrospectionClient`. POSTs
  `token=<opaque>` to the introspection endpoint with HTTP Basic Auth
  (`client_id` + `client_secret`); when a userinfo endpoint is configured,
  follows up with a bearer-auth GET and merges the profile claims under
  the introspection response (introspection wins for `active`, `exp`,
  `iat`, `nbf`, `scope`, `client_id`, `sub`). Reuses slice 49c's
  `build_client_config_from_pem` for outbound TLS to the IdP via the
  shared trust bundle.
- **`crates/broker/src/file_config.rs`:** five new `[oauthbearer]`
  TOML keys — `introspection_endpoint_uri`, `userinfo_endpoint_uri`,
  `introspection_client_id`, `introspection_client_secret_path`,
  `introspection_http_timeout_ms`. Three-way validator selection at
  config-load: neither endpoint URI → unsecured-JWS (dev only); only
  `jwks_endpoint_uri` → signed (49b); only `introspection_endpoint_uri`
  → introspection (49d); **both → reject** with a precise error. The
  client secret is read from disk at config-load (path-based, not
  literal, so secret bytes don't sit in the TOML; trailing `\r` / `\n`
  stripped — the operator slice 50c will mount a `Secret` and write the
  mount path here).
- **Rename:** `[oauthbearer].jwks_tls_trust` →
  `[oauthbearer].idp_tls_trust` (and matching
  `BrokerConfig.oauthbearer_jwks_tls_trust` →
  `oauthbearer_idp_tls_trust`). The PEM bundle now covers JWKS,
  introspection, and userinfo — one trust root rather than per-endpoint
  knobs. Greenfield rename per `CLAUDE.md` (no `serde(default)`
  back-compat alias, no V2 fallthroughs). Coordinated flips: broker
  `file_config` + `config` + `broker.rs` + operator
  `render_broker_toml` + ~5 test assertions + 4 stale doc comments.
- **SASL handler async ripple:** `handle_authenticate_oauthbearer` and
  `validate_bearer` become `async fn`; the `try_handle_sasl_frame` /
  `handle_sasl_frame` sync wrappers in `dispatch.rs` convert to async
  and `.instrument(req_span.clone()).await` (matching the file's other
  handlers). The four existing OAUTHBEARER tests flip to
  `#[tokio::test]`.
- Tests: +14 security unit (introspection validator paths —
  active/inactive/transport-fail/non-success-status/parse-fail/userinfo
  merge/scope-required/principal-claim/expired-token — plus
  enum-dispatch async coverage); +9 broker integration (HTTPS-served
  introspection fixture via `tokio-rustls` + `rcgen` self-signed cert,
  end-to-end through the SASL handler); +6 broker `file_config` unit
  (three-way selection, mutually-exclusive rejection, missing-fields,
  with/without userinfo); +2 broker tests renamed for the
  `idp_tls_trust` flip; +2 operator tests renamed likewise. Workspace
  `cargo fmt --check` + `cargo clippy --workspace --all-targets -D
  warnings` + `cargo test --workspace` all clean (T6 also paid down 12
  pre-existing clippy nits in T2 / T3 files surfaced when the workspace
  gate ran).
- Out of scope (deferred): slice 50c (operator CRD field +
  `Secret` mount for the client secret + reconciler wiring for
  `introspectionEndpointUri` / `userinfoEndpointUri`); hybrid validator
  (try JWT first, fall back to introspection); broker-side token
  caching keyed by `(token, exp)` to amortize IdP round-trips;
  `client_secret_post` / `private_key_jwt` introspection-endpoint auth
  styles (HTTP Basic only); outbound mTLS to the IdP (one-way TLS via
  the shared trust bundle only); per-listener `[oauthbearer]` config
  (still rejected at config-load — future slice 49h).
- Reference doc:
  [`docs/superpowers/specs/2026-05-24-crabka-broker-oauth-introspection-49d-design.md`].

## Slice 50c — Operator: Listener OAuth introspection surface (2026-05-24)

- Surfaces slice 49d's broker introspection validator on the operator
  CRD: `listeners[].authentication.type: oauth` grows an explicit
  `accessTokenIsJwt` toggle plus the introspection/userinfo endpoint
  URIs, IdP client credentials, and HTTP timeout. The toggle is *not*
  inferred from sibling presence (Strimzi parity); reconciler
  cross-mode validation rejects any field that doesn't belong to the
  active mode. A second e2e job `kind-oauth-introspection` runs the
  introspection path end-to-end against Keycloak alongside the
  pre-existing JWT-mode `kind-oauth` job.
- **`crates/operator/src/crd/listener.rs`:** `jwksEndpointUri`
  flipped from `String` (required) to `Option<String>` (greenfield
  rename per `CLAUDE.md`, no `serde(default)` shim). Six new siblings
  on `ListenerAuthenticationOAuth`: `accessTokenIsJwt: bool` (default
  `true`), `introspectionEndpointUri: Option<String>`,
  `userInfoEndpointUri: Option<String>`, `clientId: Option<String>`,
  `clientSecret: Option<OauthClientSecretRef>` (new Strimzi-shaped
  struct `{ secretName, key }`), and
  `introspectionHttpTimeoutSeconds: Option<u32>`. Hand-rolled schema
  extended; CRD regenerated.
- **`crates/operator/src/controller/common.rs`:** three new
  `ReconcileError` variants surfaced as `Ready=False` from the
  introspection-Secret validator: `MissingOauthIntrospectionSecret`
  (`clientSecret.secretName` not found in the namespace),
  `MissingOauthIntrospectionKey` (Secret exists but doesn't contain
  `clientSecret.key`), and `EmptyOauthIntrospectionValue` (key present
  but value empty). Plus one `ValidationError` variant in
  `controller/listeners.rs` —
  `ListenerOauthAccessTokenIsJwtInvalid` — surfaced as
  `ListenersValid=False` when the cross-mode rules reject a listener
  config (required field missing or forbidden field set for the active
  mode). The validator runs before the reconciler, so cross-mode
  rejections never reach the Secret-validation path. No central
  `reason()` method — reasons are matched inline at each call-site in
  `reconcile_kafka`.
- **`crates/operator/src/controller/listeners.rs`:** `render_broker_toml`
  forks on `access_token_is_jwt`. `true` → emit
  `jwks_endpoint_uri = ...` (the slice 49b path). `false` → emit
  introspection-mode keys in the slice 49d field order:
  `introspection_endpoint_uri`, `userinfo_endpoint_uri` (when set),
  `introspection_client_id`, `introspection_client_secret_path` (the
  fixed pod-mount path), `introspection_http_timeout_ms`. Per-field
  cross-listener divergence walk extended for each new field so a
  mismatch yields a precise conflict message.
- **`crates/operator/src/controller/kafka.rs`:** new
  `pub(crate) struct OauthIntrospectionMount` + pure
  `oauth_introspection_secret_mount` derivation helper, plus an async
  `reconcile_oauth_introspection_secret` validator that reads the
  referenced source `Secret` from the Kafka CR's namespace and checks
  the named key is present and non-empty. **Validation-only** — no
  managed-Secret upsert, unlike slice 50b's `tlsTrustedCertificates`
  (which concatenates multiple PEM keys and therefore needs a managed
  aggregate).
- **`crates/operator/src/controller/kafka_node_pool.rs`:** the
  source `Secret` is mounted DIRECTLY into the broker pod via a
  projected `items: [{key: <user-key>, path: "client-secret"}]` entry,
  so the broker reads from `/etc/crabka/oauth-introspection/client-secret`
  regardless of what the user named the key in their `Secret`.
  `defaultMode: 0o400`, `readOnly: true`. Cleanly composes with the
  slice 50b trust-bundle mount (different mount path, independent
  volume).
- **Sample + CRD regen:** `crates/operator/sample/oauth-listener.yaml`
  gains a commented introspection example; `deploy/crds/crabka.io_kafkas.yaml`
  regenerated.
- **E2E (`.github/workflows/operator-e2e.yml`):** new
  `kind-oauth-introspection` job mirrors `kind-oauth` but bootstraps a
  *second* Keycloak client `kafka-broker` (confidential,
  service-account enabled) for the broker to authenticate to the
  introspection endpoint; the operator-provisioned Secret carries that
  client's secret. Producer/consumer Jobs are unchanged from
  `kind-oauth` — the token endpoint is identical regardless of which
  validation mode the broker runs. Label-gated `e2e-oauth-introspection`
  + `push` to main.
- Tests: +6 `crd::listener` unit (round-trip + schema-regression
  extension for all six new fields); +7 `controller::listeners`
  validation + +5 TOML render + +1 divergence-walk extension + +1
  introspection-mode divergence; +4 `controller::kafka` unit
  (introspection helper paths); +2 `controller::kafka_node_pool` unit
  (pod-mount present/absent); +1 `tests/reconcile_listener_oauth.rs`
  divergence integration; +9 `tests/reconcile_oauth_introspection.rs`
  integration (happy path, missing source Secret, missing key, empty
  value, validation-only-no-managed-Secret, both-modes-rejected,
  cross-mode-field-on-wrong-mode, pod-mount-when-some,
  pod-mount-absent-when-jwt). ~35 new tests total. Workspace
  `cargo fmt --check` + `cargo clippy --workspace --all-targets -D
  warnings` + `cargo test --workspace` all clean; CRD-drift gate
  (`tools/regen-crds.sh` then `git diff --exit-code -- deploy/crds/`)
  also clean.
- Out of scope (deferred): per-listener introspection config (still
  rejected with `ConflictingOAuthConfig` until slice 49h);
  source-`Secret` reflector for instant rotation (broker reads at
  startup — rotation requires pod restart); cross-namespace `Secret`
  references (must live in the Kafka CR's namespace);
  `client_secret_post` / `private_key_jwt` introspection-endpoint
  auth methods (slice 49d ships HTTP Basic only); outbound mTLS to the
  IdP (slice 49c provides one-way TLS trust only; mTLS would need a
  future broker slice); operator-managed Keycloak client provisioning
  (ops bootstrap the IdP's `kafka-broker` client out-of-band — see the
  kind e2e for the manual `kcadm` flow).
- Reference doc:
  [`docs/superpowers/specs/2026-05-24-crabka-operator-oauth-introspection-50c-design.md`].

## Slice 49e — Broker: SASL re-authentication (KIP-368) (2026-05-24)

- Bounds an OAUTHBEARER SASL session by the token's `exp`. The broker
  now populates `SaslAuthenticateResponse.session_lifetime_ms` from the
  validated token, and a per-connection `tokio::select!` timer closes
  the TCP connection at session expiry. In-band re-auth (a fresh
  `SaslHandshake` + `SaslAuthenticate` on an already-authenticated
  connection) refreshes the session without dropping the stream, so
  long-lived producers/consumers can roll tokens cleanly.
- **Validator surface (`crates/security/src/oauthbearer.rs`):** new
  `AuthOutcome { principal, expires_at_ms }`. `OAuthBearerValidator::
  validate` returns `AuthOutcome` instead of bare `Principal`; all three
  concrete validators (unsecured JWS / signed JWKS / RFC 7662
  introspection) surface the token's `exp` (each already extracted it
  during temporal-claim checks — this just stops discarding the value).
  **Note:** introspection's `exp` is now REQUIRED (was optional
  pre-49e); a response without `exp` returns `AuthError::InvalidToken`.
  Intentional — without `exp` we cannot compute `session_lifetime_ms`.
- **Connection state (`crates/broker/src/network/auth.rs`):**
  `ConnectionAuth::Authenticated` extended with `{ mechanism:
  SaslMechanism, expires_at_ms: Option<i64> }`. New
  `Reauthenticating { previous: AuthenticatedSnapshot, exchange:
  SaslExchange }` variant captures the in-band re-auth handshake mid-
  flight. New `ConnectionAuth::allows_request(api_key)` method gates the
  dispatch loop's pre-auth allowlist per state — during
  `Reauthenticating`, only `SaslAuthenticate=36` is accepted.
- **Handler updates (`auth.rs`):** `handle_handshake` now accepts an
  in-band handshake from `Authenticated`. Same-mechanism enforced —
  mismatch returns `ILLEGAL_SASL_STATE (34)`.
  `handle_authenticate_oauthbearer` handles the `Reauthenticating` arm:
  same-principal-name enforced — mismatch returns
  `SASL_AUTHENTICATION_FAILED (58)` with message
  "re-authentication may not change the principal".
- **Dispatch loop (`crates/broker/src/network/dispatch.rs`):**
  per-connection read becomes
  `tokio::select! { biased; next = framed.next() => ...,
  _ = sleep_until_some(deadline) => break }`. `deadline` is derived
  from `Authenticated.expires_at_ms` (or
  `Reauthenticating.previous.expires_at_ms` so a slow re-auth can't
  extend the session past the original token's `exp` — security
  invariant). `biased` makes the read arm win ties so the last in-
  flight request before expiry completes. Non-OAuth connections return
  `None` from the deadline derivation and the timer arm is disarmed via
  `std::future::pending()`. The SASL-frame routing was also extended to
  recognize `Reauthenticating` (T4 surfaced this gap).
- Tests: +3 `crates/security/src/oauthbearer.rs` unit (each validator
  surfaces `exp`); +8 `crates/broker/src/network/auth.rs` unit
  (`Authenticated` shape; in-band handshake same-mech + diff-mech;
  in-band authenticate same-principal + diff-principal;
  `allows_request` behavior across all states); +6
  `crates/broker/tests/auth_handlers.rs` integration (session lifetime
  populated, timer fires at expiry, in-band re-auth happy path,
  in-band re-auth principal-switch reject, in-band re-auth
  mechanism-switch reject, PLAIN regression). Workspace
  `cargo fmt --check` + `cargo clippy --workspace --all-targets -D
  warnings` + `cargo test --workspace` all clean.
- Out of scope (deferred):
  - Mechanism-agnostic `connections.max.reauth.ms` broker config
    (would gate PLAIN/SCRAM too); not in the OAUTHBEARER parity
    umbrella.
  - Operator-side `maxSecondsWithoutReauthentication` CRD field —
    slice 50d.
  - Server-side cap on `session_lifetime_ms`
    (`oauthbearer.max.session.lifetime.ms` defense-in-depth knob).
  - Server-side minimum check ("token too-short-lived, reject auth").
  - Client-side re-auth scheduler in Crabka's Kafka client crate
    (broker-only this slice).
- Reference doc:
  [`docs/superpowers/specs/2026-05-24-crabka-broker-sasl-reauth-49e-design.md`].

