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
