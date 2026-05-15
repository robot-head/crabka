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
