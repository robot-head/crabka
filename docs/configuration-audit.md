# Runtime Configuration Audit

> **Historical configuration note:** This audit is an append-only record of
> configuration slices as they existed when each section was completed.
> Unit-suffixed names and primitive numeric examples in earlier sections are
> historical, not the live contract; use current binary `--help`, generated
> CRDs, and unit-bearing values.

The audit follows the scope in the runtime-configuration design. Paths and
line numbers are refreshed before completion; names are stable identifiers.

Run `tools/audit-runtime-values.sh` to reproduce the candidate set. The scanner
is intentionally an over-approximation: it also finds constants and literals
inside inline test modules and allocation hints that do not control behavior.
Within the Broker section, each broker match is classified by the first
applicable rule below:

1. A match in a `#[cfg(test)]` item or module is **fixed: test input**.
2. A production match named in **Configurable** is configuration, or must be
   moved to configuration by the owning service slice.
3. Every other broker production match is covered by one of the exhaustive **Fixed**
   groups. The exclusions are semantic, not based on inconvenience.

## Coverage Status

- Complete: `admin-ui`, `audit`, `authz`, `bench-driver`, `blockstore`,
  `broker`, `broker/test-helpers`, `cli`, `client-admin`, `client-producer`,
  `compression`,
  `connect`, `connect-derive`, `connect-postgres`,
  `docgen`, `gres`, `gres-activator`, `gres-balancer`, `gres-control`,
  `grpc-gateway`, `ids`,
  `gres-conformance`, `integration-tests`, `kafka-tap`, `log`,
  `kraft-core`, `log-iobench`, `logfmt`, `logql`, `metadata`, `object-store`, `operator`,
  `pgcatalog`, `pgmvcc`, `pgtypes`, `protocol`, `protocol-codegen`,
  `records-legacy`, `remote-storage`,
  `pgparser`, `playground`,
  `schema-registry`, `throttle`, `units`, `verified`, `voters`.
- Pending: `client-consumer`, `client-core`, `client-streams`,
  `gres-fdw`, `gres-loadtest`,
  `gres-ranges`, `gres-substrate`,
  `metrics`,
  `metrics-service`, `observability`, `observability-demo-app`,
  `pgexec`, `pgkv`, `pgwire`,
  `pprof`, `profiles`, `promql`, `raft`,
  `rebalancer`, `remote-storage-topic`, `replicator`,
  `schema-serde`, `security`, `telemetry`,
  `traceql`, and `traces`.

## Broker

### Configurable

The following production values are deployment policy. Existing configuration
defaults remain configurable even when the scanner reports the default constant.

- Startup and registration: `STARTUP_LEADER_WAIT_TIMEOUT`,
  `SELF_REGISTRATION_MAX_ATTEMPTS`, `SELF_REGISTRATION_BACKOFF_MIN`,
  `SELF_REGISTRATION_BACKOFF_MAX`, `OBSERVER_FETCH_MAX_BYTES`,
  `OBSERVER_POLL_INTERVAL`, and the offsets-topic metadata wait deadline in
  `coordinator/bootstrap.rs`.
- Audit maintenance and recovery: `AUDIT_EVENT_QUEUE_CAPACITY`,
  `AUDIT_SPOOL_REPLAY_INTERVAL`, `AUDIT_STATS_POLL_INTERVAL`,
  `AUDIT_PARTITION_WAIT_TIMEOUT`, `TAIL_WINDOW_OFFSETS`, and
  `TAIL_READ_MAX_BYTES`.
- Broker maintenance: `LIVENESS_TICK_INTERVAL`, `GAUGE_POLL_INTERVAL`,
  `ISR_SCAN_INTERVAL`, `DEFAULT_COMPACTION_INTERVAL`,
  `DEFAULT_TIERING_INTERVAL` (the default for the existing
  `remote_log_manager_interval` setting), `MOVE_READ_CHUNK_BYTES`, and
  `MOVE_RETRY_BACKOFF`.
- Client metrics: `CLIENT_METRICS_EVICTION_TICK`,
  `CLIENT_METRICS_STALE_PUSH_INTERVALS`, `CLIENT_METRICS_STALE_FLOOR`,
  `DEFAULT_INTERVAL_MS`, `DEFAULT_TELEMETRY_MAX_BYTES`, and
  `PROM_SNAPSHOT_TTL`; and the OTLP forwarding queue capacity.
- Remote log metadata: `RLMM_RECONCILE_TICK`,
  `RLMM_BOOTSTRAP_BACKOFF_INITIAL`, and `RLMM_BOOTSTRAP_BACKOFF_MAX`.
- Network and authorization: `CONNECTION_CREATION_THROTTLE_MAX`,
  `OPA_HTTP_TIMEOUT`, the JWKS HTTP timeout in `oauth_jwks.rs`,
  `MAX_FRAME_BYTES`, `SENDFILE_MIN_BYTES`, `SOCKET_BUF_BYTES`,
  `MAX_PRINCIPAL_LEN`, `MAX_RESOURCE_NAME_LEN`, and `INTER_BROKER_SNI`.
- Telemetry resource protection: `MAX_DECOMPRESSION_RATIO`,
  `DECOMPRESSED_OUTPUT_FLOOR`, and `DECOMPRESSED_OUTPUT_CEILING`.
- Auto-join and replication: `RETRY_BACKOFF`, `FETCH_MAX_BYTES`,
  `FETCH_MAX_WAIT_MS`, `FETCH_MIN_BYTES`, `THROTTLE_EXHAUSTED_BACKOFF`,
  `SEND_ERROR_BACKOFF`, `UNKNOWN_TOPIC_RETRY_DELAY`, `EPOCH_FENCE_BACKOFF`,
  `UNEXPECTED_ERROR_BACKOFF`, `RECONNECT_INITIAL_DELAY`, and
  `RECONNECT_DELAY_CAP`; and the AddRaftVoter request timeout.
- Coordinators: `ACTOR_MAILBOX_CAPACITY`, `SESSION_EXPIRY_TICK_INTERVAL`,
  `SHUTDOWN_ACK_TIMEOUT`, `DEFAULT_SESSION_TIMEOUT`,
  `DEFAULT_HEARTBEAT_INTERVAL`, `DEFAULT_MIN_SESSION_TIMEOUT`,
  `DEFAULT_MAX_SESSION_TIMEOUT`, `DEFAULT_MIN_HEARTBEAT_INTERVAL`,
  `DEFAULT_MAX_HEARTBEAT_INTERVAL`, `DEFAULT_MAX_GROUP_SIZE`,
  `DEFAULT_SESSION_TIMEOUT_MS`, `DEFAULT_REBALANCE_TIMEOUT_MS`,
  `INITIAL_REBALANCE_DELAY`, and `FOLLOWER_WAIT`. This includes the naked
  `mpsc::channel(64)` mailbox capacities in the Share and Streams actors.
- Share and Streams group policy: every behaviorally consumed field default in
  `coordinator/unified/share/config.rs` and
  `coordinator/unified/streams/config.rs`; and the
  `share_session_cache_max_when_unlimited` fallback of 10,000 sessions in
  `share_partition/manager.rs`. The Streams `enable`, `max_groups`, and
  `max_size` fields and the 100 ms Share lock-sweeper floor are fixed
  separately below.
- Recovery and quota policy: `AGGRESSIVE_DEADLINE`, `BALANCED_DEADLINE`,
  `OPERATOR_RECOVERY_DEADLINE`, the maximum quota throttle delay, and
  `RECOVERY_READ_MAX_BYTES`; and the
  `mpsc::channel::<RecoveryJob>(256)` unclean-recovery queue capacity.
- Produce policy: `PRODUCER_ID_EXPIRATION_MS`, its expiration scan interval,
  `MAX_PRODUCE_GROUP`, `PARTITION_WRITER_QUEUE_DEPTH`, and
  `DEFAULT_MIN_INSYNC_REPLICAS`.
- Internal-topic sizing: the partition counts and replication factors for
  `__share_group_state` and `__transaction_state`. Replication factors are
  capped by the number of available brokers. Internal topic names and fixed
  partition sentinels remain fixed.
- Transaction policy: `MIN_TXN_TIMEOUT_MS` and `MAX_TXN_TIMEOUT_MS`.
- Existing `BrokerConfig` defaults in `config.rs`: RLMM snapshot interval,
  heartbeat interval and timeout, replica lag, metadata snapshot byte/time/
  record thresholds, observer lag, controller election and heartbeat,
  controlled-shutdown drain, transaction cleanup, TLS reload, remote-log
  maintenance, leader-imbalance interval and percentage, fetch-session cache
  slots, JWKS refresh and on-demand pause, RLMM topic sizing, audit topic/
  checkpoint/spool settings, and delegation-token lifetime/check/renewal.
- Existing file configuration defaults in `file_config.rs`: OPA cache size and
  expiry, Kerberos service name, introspection HTTP timeout, and allowable
  clock skew.
- Existing component configuration: all fields already present in
  `ShareCoordinatorConfig`, `RemoteLogManagerConfig`, and the direct broker
  CLI/file configuration. The scanner's naked
  `partition_disk_scan_interval_secs: 60` is one such configured default.

### Fixed

- All matches inside `#[cfg(test)]` items or modules, including
  `TEST_AWAITER_TIMEOUT`, `TEST_MAX_FETCH_BYTES`, fixture `VERSION`, `TOPIC`,
  `PARTITION`, `NODE`, `LOCK`, `REQ`, `STATE`, `SAMPLE`, body strings, test
  deadlines, test sleeps, and test allocation sizes: verification inputs, not
  runtime policy. This rule covers inline tests under `src/` that the scanner
  deliberately reports.
- Matches in `BrokerConfig::for_tests` and items gated by the `test-helpers`
  feature: test inputs even when their declaration precedes a test module.
- Every constant in `codes.rs`, error-code aliases in handlers and auth, and
  the `NONE`/`UNKNOWN_TOPIC_OR_PARTITION` values in `wal/quorum/wire.rs`:
  Kafka wire compatibility.
- API keys, API versions, request-header sizes, advertised pre-auth API sets,
  resource/operation/config-source/match/election/upgrade enum values, and
  request timestamp or offset sentinels in `raft_handshake.rs`,
  `network/dispatch.rs`, `handlers/*.rs`, `fetch_session.rs`,
  `share_partition/session.rs`, `assign_dirs.rs`, and `isr_maintenance.rs`:
  Kafka wire compatibility.
- `KRAFT_METADATA_TOPIC_ID` and `KIP_595_FETCH_VERSION` in
  `wal/quorum/wire.rs`: KRaft wire compatibility.
- Record keys/tags and state values in `coordinator/**/persistence*.rs`,
  `share_coordinator/persistence.rs`, `share_partition/state.rs`, and
  `txn/log_record.rs`: persisted-format compatibility.
- `MAGIC`, `VERSION`, `HEADER_LEN`, `TRAILER_LEN`, and `ENTRY_LEN` in
  `diskless/wal_object.rs`; the three index-entry lengths in
  `remote_reader.rs`; and fixed `with_capacity(...)` values used to encode a
  known wire or record shape: serialization invariants.
- `with_capacity(...)` used only as a preallocation hint: it does not bound
  accepted input, concurrency, memory, or output and therefore is not a
  runtime policy.
- Internal topic names, config-key names, entity names, telemetry target,
  audit header names, principal prefixes, diagnostic messages, `ALL_METRICS`,
  and Kafka group/state strings in `config_keys.rs`,
  `client_metrics/config.rs`,
  `coordinator/**`, `handlers/**`, `telemetry.rs`, `throttle/mod.rs`,
  `diskless/index_log.rs`, and the share/transaction bootstrap modules:
  protocol, persisted, or public configuration identifiers.
- `MIN_INTERVAL_MS` and `MAX_INTERVAL_MS` in `client_metrics/config.rs` and
  `ACCEPTED_COMPRESSION_TYPES` in `client_metrics/manager.rs`: KIP-714
  validation and wire compatibility. The default interval and byte limit are
  configurable above.
- `MIN_ITERATIONS` and `MAX_ITERATIONS` in
  `handlers/alter_user_scram_credentials.rs`: Kafka SCRAM validation bounds.
- `GSSAPI_MAX_RECV_SIZE` and `SASL_AUTHENTICATION_FAILED`: SASL/GSSAPI wire
  limits and error codes.
- The `mpsc::channel::<()>(1)` JWKS refresh signal capacity in
  `file_config.rs`: capacity one is the coalescing mechanism used with
  `try_send`, not queue-throughput policy.
- `TOKEN_SCRAM_ITERS`: the KIP-48 SCRAM iteration constant, not deployment
  tuning.
- `REQUEST_PERCENTAGE_MAX`, quota-key sets, and supported entity-type sets:
  units and Kafka configuration schema. The time cap applied by the quota
  implementation is configurable above.
- `UNKNOWN_BROKER_EPOCH`, `INVALID_SESSION_ID`, initial/final epochs,
  no-producer/no-timeout values, initial coordinator/leader epochs,
  `UNINITIALIZED_START_OFFSET`, retention inheritance values, and other
  negative/zero sentinels: state-machine and wire invariants.
- `MURMUR2_SEED`, `MURMUR2_M`, and `MURMUR2_R`: Kafka's partitioning hash
  algorithm.
- `PID_BASE`: the producer-id allocator's stable initial sentinel, not a
  resource or scheduling policy.
- `REQUEST_DURATION_BUCKETS`: exported metric schema; changing buckets breaks
  time-series continuity. `UNKNOWN_LABEL` is a stable metric label.
- `QUORUM_STATE_FILE`, `FILE_NAME`, `FUTURE_SUFFIX`, and `PROBE_FILENAME`:
  persisted-state or probe identifiers.
- `CLUSTER_METADATA_TOPIC`, `CLUSTER_RESOURCE_NAME`, and the fixed offsets
  partition number: Kafka metadata/resource identity.
- `OFFSETS_NUM_PARTITIONS`: routing and storage currently hardcode offsets
  partition zero, so changing the count alone would violate that invariant.
- `ACKS_ALL`, accepted transaction-state arrays, share delivery-state flags,
  topology error states, and fixed collection dimensions derived from those
  protocol shapes: protocol invariants.
- `FLUSH_INTERVAL`, `FLUSH_MAX_BYTES`, `DEFAULT_TRIM_SAFETY_LAG`, and the
  diskless projection timeout in `diskless/flusher.rs`: staged code with no
  production caller. Exposing them now would create no-op configuration;
  reclassify them when production starts the flusher.
- The heartbeat polling clamp of 500 ms through 1 s, the share lock-sweeper
  minimum of 100 ms, and fallback rebalance deadlines derived from configured
  group timeouts: algorithmic safety bounds or derived values, not independent
  tuning controls.
- Nonnegative and positive wire-value clamps such as `.max(0)` and `.max(1)`:
  protocol sanitization before signed-to-unsigned conversion, not independent
  tuning controls.
- The 100 ms startup-leader watch wake and 10 ms audit-partition registry poll:
  internal observation quanta bounded by their configured deadlines, not
  independent tuning controls.
- `FALLBACK_SESSION_TIMEOUT_MS`, `FALLBACK_SESSION_TIMEOUT_MS_I32`,
  `FALLBACK_REBALANCE_TIMEOUT_MS`, `FALLBACK_REBALANCE_TIMEOUT_MS_I32`, and
  `FALLBACK_HEARTBEAT_INTERVAL_MS`: classic coordinator conversion fallbacks
  derived from configured policy, not independent settings.
- Streams `enable`, `max_groups`, and `max_size`: staged fields with no
  production behavior. Exposing overrides now would create no-op
  configuration; reclassify them when the Streams coordinator consumes them.
- Model-checking constants, generated model inputs, and values in files matched
  by `*_model.rs`: verification inputs excluded directly by the scanner.

## Audit Snapshot

At the broker closure checkpoint on 2026-07-24 the scanner reported 5,849
matches across all crates, including 1,293 broker matches across 149 files. Of
the broker matches, 428 are named constant declarations and 865 are other
literal, capacity, or duration expressions. The positional first-`#[cfg(test)]`
heuristic yields 514 matches before and 779 after the boundary. That split is
not semantic because mixed files contain test-gated items before later
production items. The semantic rules above are authoritative. Independent
review and the strengthened scan found four production-policy omissions; all
four were remediated and rechecked.

## Broker Slice Completion Evidence

The broker slice passed these gates on 2026-07-24:

- `tools/audit-runtime-values.sh`: 5,849 repository matches; the broker subset
  contains 1,293 matches across 149 files, with no unclassified production
  policy after semantic review.
- `cargo +nightly fmt --all -- --check`: passed.
- `cargo clippy -p crabka-broker -p crabka-operator --all-targets -- -D warnings`:
  passed.
- `cargo nextest run -p crabka-broker -p crabka-operator --test-threads 1`:
  3,091 passed, 71 skipped, and no failures.
- `cargo run -p crabka-broker -- --help`: exposes cleaner-interval,
  OPA HTTP timeout, replication-fetch, auto-join voter timeout, and Share and
  transaction and Streams internal-topic replication-factor settings.
- `cargo run -p crabka-operator -- gen-crds <temporary-directory>` followed by
  `diff -ru deploy/crds <temporary-directory>`: all nine generated CRDs match
  the checked-in manifests.
- `git diff --check`: passed.

This evidence closes only the broker slice. Every crate listed as pending in
Coverage Status still requires its own semantic audit and verification.

## Schema Registry

### Configurable

The scanner reports eight production deployment-policy defaults. All eight are
already wired through validated CLI/environment inputs, and through typed
`SchemaRegistry` CRD fields where the operator owns the process:

- The seven reported `RegistryRuntimeConfig` scalar defaults: election session,
  rebalance, heartbeat, and reconnect policy; store-reader retry and fetch-byte
  policy; and `_schemas` creation timeout. The scanner does not report the
  adjacent configured fetch-wait default because its field name is outside the
  scanner's intentionally narrow name pattern.
- The ACL refresh interval in `AuthzConfig`.

The same runtime path also owns the configured initial compatibility and mode,
the forwarded-write body cap, `_schemas` replication factor, JWKS refresh
interval, client id, and service-specific health checks. The forwarding cap is
not reported because `forward_max_body_bytes` does not match the scanner's
`max_bytes` field-name pattern. The direct-only admin listen address remains a
CLI/environment input because the operator has no admin Service.

### Fixed

- Nine Kafka error codes: the eight group-coordinator response codes in
  `election/client.rs` and `TOPIC_ALREADY_EXISTS` in `kafkastore/topic.rs`.
- Eight Kafka or Schema Registry compatibility identifiers: the Kafka cluster
  ACL resource name; Schema Registry election protocol type, name, and version;
  REST content type and forwarding header; and the accepted mode and
  compatibility enums.
- The `_schemas` single-partition ordering, partition-zero reader, and compacted
  log semantics are persistence invariants. Producer idempotence and `acks=all`
  are durability requirements. These adjacent literals are intentionally fixed
  even though the scanner does not report them.
- All 38 remaining matches are inputs inside `#[cfg(test)]` modules: fixture
  constants, alternate configured values, invalid relationship values,
  durations, and error cases.

### Audit Snapshot

On 2026-07-24 the scanner reports 5,881 matches across all crates. The Schema
Registry subset contains exactly 63 matches across 15 files: 8 configured
production defaults, 17 fixed production compatibility values, and 38 test
inputs. Every candidate is covered by one of those classifications, and the
adjacent `_schemas` storage and writer invariants were reviewed explicitly.
Independent review found one operational literal outside the scanner's name
pattern: the 16 MiB forwarded-write body cap. It is now configurable and
rechecked through the direct and operator paths.

### Schema Registry Slice Completion Evidence

The Schema Registry slice passed these gates on 2026-07-24:

- `tools/audit-runtime-values.sh`: 5,881 repository matches and exactly 63
  Schema Registry matches across 15 files, with no unclassified production
  policy.
- `cargo +nightly fmt --all -- --check`: passed.
- `cargo clippy -p crabka-schema-registry -p crabka-operator --all-targets -- -D warnings`:
  passed.
- `cargo nextest run -p crabka-schema-registry -p crabka-operator`: 1,076
  passed, 9 skipped, and no failures.
- `cargo run -p crabka-schema-registry -- --help`: exposes election-session,
  store-reader, schemas-topic-create, forwarded-body, default-compatibility,
  and admin-listen settings.
- `cargo run -p crabka-operator -- gen-crds <temporary-directory>` followed by
  an exact diff of the generated `SchemaRegistry` CRD: passed.
- `git diff --check`: passed.

This evidence closes only the Schema Registry slice. Every crate listed as
pending in Coverage Status still requires its own semantic audit and
verification.

## gRPC Gateway

### Configurable

The scanner reports four production deployment-policy defaults:
`internal_topic_create_timeout_ms`, `consumer_poll_timeout_ms`,
`ownership_warmup_empty_polls`, and `readiness_poll_interval_ms`. They are
members of `GatewayRuntimeConfig` and flow through checked CLI/environment
inputs and typed `KafkaGrpcGateway` CRD fields.

The same validated paths own the adjacent policy that the scanner does not
report: internal-topic replication, fallback, segment and compaction ratio;
Schema Registry cache and framing; dedup sizing, retention and ownership;
TLS reload; ACL refresh; bearer clock skew; membership topic; webhook replay,
body and schema settings; outbound delivery retry, timeout, group and decoding
settings; the generic HTTP produce and internal forwarding body caps; and
Kubernetes readiness/liveness probe timing. Direct-process and operator
defaults intentionally remain distinct where they were distinct before this
work.

### Fixed

- Two scanner matches are Kafka protocol error codes:
  `INVALID_REPLICATION_FACTOR` and `TOPIC_ALREADY_EXISTS`.
- Three scanner matches initialize or reset the ownership warmup empty-poll
  counter. The threshold is configured; zero is the state-machine reset value.
- The membership topic partition count of one supplies total ordering.
  `cleanup.policy=compact` for membership and `compact,delete` plus configured
  retention for dedup are storage semantics, not independent tuning.
- gRPC and HTTP error codes, Kafka isolation/acks/offset modes, schema and
  protocol identifiers, and negative error coordinates are compatibility or
  state-machine invariants.
- Confluent framing bytes, header sizes, Protobuf varint masks and shift width,
  FNV-1a hash constants, and capacities derived from encoded payload length are
  wire or hashing invariants.
- Empty/unknown identities, clock-anomaly bounds, absent schema id zero, and
  error partition/offset `-1` are sentinels.
- Exponential-backoff doubling, half-window jitter, attempt indexing, and
  saturating arithmetic are retry math driven by configured base, cap and
  attempt values.
- Histogram buckets are the exported metric schema and remain fixed for
  time-series continuity.
- The remaining 37 scanner matches are test inputs: 3 bearer tokens, 10
  CLI/default fixtures, 2 internal-topic policy fixtures, 6 forwarding
  fixtures/sentinels, 9 outbound retry/validation fixtures, 2 Schema Registry
  cache timings, 4 schema fixtures, and 1 TLS reload boundary.

### Audit Snapshot

On 2026-07-24 the scanner reports 5,890 matches across all crates. The gRPC
Gateway subset contains exactly 46 matches across 14 files: 4 configured
production defaults, 5 fixed production values, and 37 test inputs. Six
production gaps found by the adjacent semantic review were remediated:

- Kafka-unrepresentable partition counts are rejected instead of coerced to
  `i32::MAX` or partition zero; derived conversions now rely on an explicit
  checked partition-domain invariant.
- Direct webhook TOML rejects negative timestamp tolerance and zero body caps
  with `refined_type`, matching the CRD trust boundary.
- Named webhooks collect request bodies with their configured per-endpoint cap
  instead of Axum's unrelated fixed 2 MiB extractor default.
- Generic HTTP produce collects request bodies with its validated
  `produce_max_body_bytes` runtime/CRD cap, defaulting to the prior 2 MiB
  behavior instead of inheriting Axum's fixed extractor limit.
- Internal forwarding collects and deserializes request bodies with its
  validated `forward_max_body_bytes` runtime/CRD cap, also defaulting to the
  prior 2 MiB behavior.
- Gateway readiness and liveness initial delays and periods are typed
  `healthChecks` CRD fields, validated before child rendering.

An exhaustive review of production Axum body extraction found no remaining
implicit caps: named webhooks, generic produce, and internal forwarding now use
explicit bounded collection. Authentication middleware passes `Request`
bodies through untouched, Connect RPC owns its protocol extraction, and no
`DefaultBodyLimit` or request-body-limit layer is installed.

### gRPC Gateway Slice Completion Evidence

The gRPC Gateway slice passed these gates on 2026-07-24:

- `tools/audit-runtime-values.sh`: 5,890 repository matches and exactly 46
  gateway matches across 14 files, with every result classified above.
- `cargo +nightly fmt --all -- --check`: passed.
- `cargo clippy -p crabka-grpc-gateway -p crabka-operator --all-targets -- -D warnings`:
  passed.
- `cargo nextest run -p crabka-grpc-gateway -p crabka-operator`: 1,040 passed,
  1 skipped, and no failures.
- `cargo run -p crabka-grpc-gateway -- --help`: exposes internal-topic,
  consumer-poll, ownership-warmup, Schema Registry cache, and bearer skew
  settings plus the generic produce and internal forwarding body caps with
  `CRABKA_GATEWAY_*` environment bindings.
- `cargo run -p crabka-operator -- gen-crds <temporary-directory>` followed by
  an exact diff of the generated `KafkaGrpcGateway` CRD: passed.
- `git diff --check`: passed.

This evidence closes only the gRPC Gateway slice. Every crate listed as pending
in Coverage Status still requires its own semantic audit and verification.

## Gres Activator

### Configurable

The direct activator process exposes every activator-owned deployment input
through both Clap and environment variables:

- listen address and Kafka bootstrap through
  `CRABKA_GRES_ACTIVATOR_LISTEN` and
  `CRABKA_GRES_ACTIVATOR_BOOTSTRAP`;
- readiness poll interval and cold-start timeout through their
  `CRABKA_GRES_ACTIVATOR_*` variables;
- the shared registry policy through the five `CRABKA_GRES_REGISTRY_*`
  variables documented in the Gres Registry section below; and
- backend endpoint template through
  `CRABKA_GRES_ACTIVATOR_BACKEND_ENDPOINT_TEMPLATE`.

The non-empty strings, positive millisecond values, and registry replication
factor are validated boundary types backed by `refined_type`. Registry
replication is restricted to `1..=32767`, Kafka's signed-16-bit wire domain.

Operator-managed deployments expose image, replicas, registry polling,
cold-start timeout, and readiness-probe period under typed
`Gres.spec.activator` fields. Shared registry policy belongs to
`Kafka.spec.gresRegistry`, not the activator. Present values fail before child
API writes. The operator also supports its existing global
`--default-gres-activator-image`/`DEFAULT_GRES_ACTIVATOR_IMAGE` fallback and
renders the effective policy as explicit process arguments. The configured
cold-start timeout drives a checked PgDog connection budget so the front door
does not expire first.

### Fixed

- The one-partition compacted tenant-registry topic supplies total ordering.
- The operator-derived listen address, Kafka bootstrap address, backend
  service template, internal port, Kubernetes object names and labels, TCP
  protocol, and non-root user/group IDs are topology or security invariants.
- PostgreSQL error code `57P03`, SSL/GSS refusal byte `N`, startup-frame
  lengths, maximum imported startup-frame size, parameter framing, and parser
  values are PostgreSQL compatibility invariants.
- Endpoint substitution and PgDog timeout arithmetic are derived from
  configured values. Collection capacities are exact input-size
  preallocation, not limits.
- All 12 scanner matches under `crates/gres-activator` are test inputs: 11
  readiness timing fixtures in `src/lib.rs` and one ten-variable environment
  fixture declaration in `src/main.rs`.

### Adjacent Pending Policy

The semantic audit followed the activator into `Registry::ensure_topic`, its
background reader, `PgdogTimeouts`, and the Gres controller. The shared
registry policy is now closed in the Gres Registry section below. The
following adjacent policy is intentionally not misclassified as
activator-owned:

- PgDog idle timeout, server lifetime, connection-attempt count, and related
  pool policy in `gres-control`; and
- Gres-controller reconcile, reload, PgDog admin, and credential-transition
  retry/timing policy.

Those values remain work for the Gres-control/front-door slice. No other
activator-owned hardcoded deployment policy was found in `main`, `lib`,
`config_value`, `hold`, `peek`, or `pipe`, or in the operator's activator
Service, Deployment, and fail-fast validation path.

### Gres Activator Sub-slice Evidence

On 2026-07-24 the scanner reported 5,896 matches across the repository and
exactly 12 `crates/gres-activator` matches across two files, classified above.
The activator-owned sub-slice passed:

- `cargo nextest run -p crabka-gres-activator -p crabka-gres-control
  -p crabka-operator --no-fail-fast`: 975 passed, 0 skipped, and no failures.
- `cargo clippy -p crabka-gres-activator -p crabka-gres-control
  -p crabka-operator --all-targets -- -D warnings`: passed.
- `cargo run -p crabka-gres-activator -- --help`: displayed all six direct
  settings and their `CRABKA_GRES_ACTIVATOR_*` environment bindings.
- `cargo +nightly fmt --all -- --check`: passed.
- `cargo run -p crabka-operator -- gen-crds <temporary-directory>` followed by
  `diff -ru deploy/crds <temporary-directory>`: all nine generated CRDs
  matched exactly.
- `tools/audit-runtime-values.sh` and `git diff --check`: passed.

This closes only the activator-owned sub-slice. `gres-activator`,
`gres-control`, `gres`, and broader Gres crates remain Pending in the
crate-level coverage list until their other owned policy is exposed and
audited.

## Gres Registry

### Configurable

`RegistryPolicy` owns the shared `__gres_tenants` topic and reader policy:
replication factor, topic-create timeout, reader retry backoff, fetch maximum
wait, and fetch partition maximum bytes. Its constructor validates positive
values and Kafka's `1..=32767` replication domain with `refined_type`-backed
boundary types. `Registry::connect_with_policy` stores the validated policy;
argument-free `ensure_topic()` applies it to topic creation and every
foreground/background reader path.

For operator-managed workloads, `Kafka.spec.gresRegistry` is the single
cluster-wide source. Gres reconciliation requires the referenced Kafka before
writing any child, then renders all five effective values into the activator.
GresTenant reconciliation loads the same Kafka policy for compute
Deployments, cleanup, and the cached control handle. Cache identity includes
namespace and Kafka name, and an entry is reused only when bootstrap and
policy are equal; replacement construction does not hold the cache mutex
across Kafka I/O.

Standalone activator, compute, `crabka gres`, and loadtest `run`/`compare`
surfaces expose the exact common variables:

- `CRABKA_GRES_REGISTRY_REPLICATION_FACTOR`;
- `CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT`;
- `CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF`;
- `CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT`; and
- `CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX`.

CLI values take precedence over environment values. Loadtest carries one
policy through provisioning and every spawned compute. No production caller
uses `Registry::connect`, the compatibility shorthand that chooses defaults.
All production creators use `connect_with_policy`, and `ensure_topic` accepts
no replication argument.

Existing-topic metadata is checked after both successful creation and
topic-already-exists responses. A nonzero observed replication factor that
differs from policy returns an explicit immutable-policy mismatch. Tenant WAL
replication remains a separate input to per-tenant config-topic creation and
cannot affect `__gres_tenants`.

### Fixed

- `__gres_tenants`, its compacted cleanup policy, one partition, partition
  zero, and read-committed isolation are persisted ordering/visibility
  invariants.
- Kafka error code 36 is `TOPIC_ALREADY_EXISTS`; producer client and
  transactional IDs, idempotence, and `Acks::All` are protocol/integrity
  invariants.
- Replication is checked before narrowing to Kafka's signed-16-bit wire
  domain. Metadata's zero replication value remains the protocol's unknown
  sentinel and is not treated as a mismatch.
- `SPLIT_OPERATION_KEY_PREFIX` is a persisted record-key discriminator.
- Of the nine scanner matches in `crates/gres-control/src/registry.rs`, four
  are the fixed production constants above and five are test-only timing or
  policy fixtures.

### Adjacent Pending Policy

This closes only the shared registry sub-slice. PgDog pool policy and
Gres-controller reconcile, reload, admin, and credential-transition timing
remain Pending. The crate-level Coverage Status stays unchanged until those
and the remaining Gres-owned runtime values are audited.

### Gres Registry Sub-slice Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported exactly 5,896
repository matches. Relevant crate totals were 33 for `gres-control`, 12 for
`gres-activator`, 40 for `gres`, 19 for `cli`, 88 for `gres-loadtest`, and
181 for `operator`; the registry implementation itself contributed the nine
fully classified matches above.

- `cargo nextest run -p crabka-gres-control -p crabka-gres-activator
  -p crabka-gres -p crabka-cli -p crabka-gres-loadtest -p crabka-operator
  --no-fail-fast`: 1,309 passed, 1 skipped, and no failures.
- `cargo clippy -p crabka-gres-control -p crabka-gres-activator
  -p crabka-gres -p crabka-cli -p crabka-gres-loadtest -p crabka-operator
  --all-targets --all-features -- -D warnings`: passed.
- The five standalone help surfaces each displayed all five
  `--registry-*` settings and exact `CRABKA_GRES_REGISTRY_*` bindings:
  `crabka-gres-activator --help`, `crabka-gres --help`,
  `crabka gres --help`, `crabka-gres-loadtest run --help`, and
  `crabka-gres-loadtest compare --help`.
- `cargo +nightly fmt --all -- --check`: passed.
- `cargo run -p crabka-operator -- gen-crds <temporary-directory>` followed
  by `diff -ru deploy/crds <temporary-directory>`: all nine CRDs matched
  exactly.
- Focused `rg` audits found zero production `Registry::connect` calls, zero
  public `ensure_topic` replication parameters, and explicit policy flow
  through every required creator.
- `git diff --check`: passed.

## PgDog Front Door

### Configurable

The standalone `crabka gres render-pgdog` command exposes exactly 13
CLI/environment pairs:

- Kafka bootstrap, output directory, and optional activator route;
- listen port;
- TLS certificate, private key, and client CA;
- fleet pooler mode and backend connection-attempt count; and
- cold-start ceiling, normal and suspension idle timeouts, and server
  lifetime.

The corresponding variables are the 13
`CRABKA_GRES_PGDOG_{BOOTSTRAP,OUT_DIR,ACTIVATOR,LISTEN_PORT,TLS_CERTIFICATE,TLS_PRIVATE_KEY,TLS_CLIENT_CA_CERTIFICATE,POOLER_MODE,CONNECT_ATTEMPTS,COLD_START_CEILING_MS,IDLE_TIMEOUT_MS,SUSPENSION_IDLE_TIMEOUT_MS,SERVER_LIFETIME_MS}`
bindings. CLI values take precedence over environment values. Required
strings remain required; the listen port uses `NonZeroU16`; attempts use the
`refined_type`-backed positive `PgdogConnectAttempts`; and each configured
millisecond value uses the `refined_type`-backed `PositiveMillis`. TLS
certificate and key must be supplied together, and the client CA requires
both.

`Gres.spec.pgdog` has seven optional runtime-policy fields:

| Field | Bounds | Effective default |
|---|---:|---:|
| `poolerMode` | `transaction` or `session` | `transaction` |
| `connectAttempts` | `1..=65535` | `3` |
| `idleTimeoutMs` | positive `u64` | `60000` |
| `suspensionIdleTimeoutMs` | positive `u64` | `1000` |
| `serverLifetimeMs` | positive `u64` | `300000` |
| `readinessProbePeriodSeconds` | positive `i32` | `5` |
| `directBootstrapGraceMs` | positive `u64` | `4000` |

The existing required `listenPort` is independently bounded to `1..=65535`
in OpenAPI and validated before child API access. It has no fallback or clamp.
The effective policy rejects every configured zero. Fleet pooler mode reaches
both the general PgDog setting and each database lacking a tenant override;
an explicit tenant mode still wins.

The operator process exposes six positive CLI/environment timing values:

| CLI | Environment | Default |
|---|---|---:|
| `--pgdog-reload-attempts` | `PGDOG_RELOAD_ATTEMPTS` | `3` |
| `--pgdog-reload-backoff-ms` | `PGDOG_RELOAD_BACKOFF_MS` | `100` |
| `--pgdog-reload-requeue-ms` | `PGDOG_RELOAD_REQUEUE_MS` | `15000` |
| `--pgdog-admin-timeout-ms` | `PGDOG_ADMIN_TIMEOUT_MS` | `20000` |
| `--pgdog-transition-poll-ms` | `PGDOG_TRANSITION_POLL_MS` | `60000` |
| `--controller-error-requeue-ms` | `CONTROLLER_ERROR_REQUEUE_MS` | `15000` |

One validated connection-attempt value drives both sides of timeout
arithmetic. In the operator, the activator's configured per-attempt timeout is
checked-multiplied by attempts to form the cold-start ceiling; PgDog then
derives the same per-attempt connect timeout and the ceiling-sized checkout
timeout. Standalone rendering likewise derives both values from its configured
ceiling and attempts. There are no independently configurable derived
connect/checkout values.

Suspension idle selection considers only tenants whose `spec.gres` matches the
reconciled fleet. A tenant override takes precedence over the fleet default,
and only an effective value greater than zero selects the suspension timeout;
`idleSeconds: 0`, absence, and unrelated fleets select the normal timeout.

The one effective `directBootstrapGraceMs` value is passed into tenant status
deadline creation, PgDog credential retention and route expiry, and bounded
transition scheduling. The transition delay is the earliest matching-fleet
boundary capped by `pgdogTransitionPollMs`, so polling cannot become
unbounded.

The common configured error delay is used by exactly eight controllers:
Kafka, KafkaNodePool, KafkaTopic, KafkaUser, SchemaRegistry,
KafkaGrpcGateway, Gres, and GresTenant. KafkaRebalance retains its distinct
15-second `TRANSPORT_RETRY`, which is transport state-machine policy rather
than the common reconcile-error delay.

### Fixed

- `min_pool_size = 0` is required for scale-to-zero: PgDog must not retain an
  eager backend session.
- `3155760000000ms` is PgDog's documented disabled idle-healthcheck sentinel;
  a healthcheck must not wake a suspended tenant.
- Passthrough authentication is fixed enabled for the forwarding model.
  TLS-client-required state is derived from mounted frontend TLS.
- Activator/compute ports, TCP, command and configuration paths, UID/GID,
  Kubernetes names and labels, and the PgDog admin command/column protocol are
  topology, security, or compatibility invariants.
- Checked duration conversion, saturating Unix-deadline arithmetic, rounded-up
  timeout conversion, and the one-millisecond minimum scheduled delay are
  safety or derived arithmetic, not deployment tuning.

### Adjacent Pending Policy

This closes only the PgDog/front-door sub-slice. The scanner still reports
unrelated values in `gres-control`, `cli`, and `operator`, including broader
GresTenant lifecycle and compute policy. `gres-control`, `cli`, `operator`,
`gres`, `gres-activator`, `gres-loadtest`, and the other Gres-family crates
therefore remain Pending in the crate-level Coverage Status until each
remaining owner is independently audited.

### PgDog Front-door Sub-slice Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported 5,902 repository
matches. Relevant package totals were 41 matches across three
`gres-control` files, 27 across two `cli` files, and 171 across 18 `operator`
files. The focused implementation paths contributed 26 matches in
`gres-control/src/pgdog.rs`, 20 in `cli/src/gres.rs`, 17 in
`operator/src/controller/gres.rs`, 13 in
`operator/src/controller/gres_tenant.rs`, and one in
`operator/src/config.rs`. The semantic review classified every focused match
and found no code remediation.

- `cargo nextest run -p crabka-gres-control -p crabka-cli
  -p crabka-operator --no-fail-fast`: 1,058 passed, 0 skipped, and no
  failures.
- `cargo clippy -p crabka-gres-control -p crabka-cli -p crabka-operator
  --all-targets --all-features -- -D warnings`: passed.
- `crabka gres render-pgdog --help` displayed all 13 exact
  `CRABKA_GRES_PGDOG_*` bindings, including the client CA.
- `crabka-operator run --help` displayed all six controller timing
  CLI/environment pairs.
- `cargo +nightly fmt --all -- --check`: passed.
- `cargo run -p crabka-operator -- gen-crds <temporary-directory>` followed
  by `diff -ru deploy/crds <temporary-directory>`: all nine CRDs matched
  exactly.
- Focused `rg` checks found eight common error-policy users, the unchanged
  KafkaRebalance transport retry, one production grace default/source, no
  listen-port fallback/clamp, and no duplicate hardcoded front-door timing.
- `git diff --check`: passed.

## Gres Checkpoint and Lifecycle Policy

Periodic checkpoint execution, standalone checkpoint/suspend inputs, and fleet
compute/lifecycle policy are complete. The shared compiled defaults have one
numeric owner:

- `gres-control` owns checkpoint frames, bytes, delete-records timeout,
  checkpoint polling, and idle-suspend polling.
- `gres-substrate` owns checkpoint part size and retained-manifest count.
- The operator owns its lifecycle requeue cadence.

Explicit Gres CLI/environment values override hydrated tenant thresholds, which
override the shared compiled frame/byte defaults. Tenant-record defaults alone
do not activate checkpointing. The operator emits exactly the five compute
checkpoint policy flags only when a checkpoint store is configured; it never
emits frame/byte threshold or lifecycle-requeue flags.

Every configurable value has a traced live consumer:

- frame/byte thresholds reach the periodic checkpoint trigger;
- part size reaches checkpoint object splitting;
- retention reaches manifest/object/WAL pruning;
- delete-records timeout reaches Kafka Admin deletion;
- checkpoint polling reaches the delayed checkpoint interval;
- idle-suspend polling reaches the suspend monitor;
- lifecycle requeue reaches all three tenant lifecycle progress branches.

### Fixed

Partition 0, offset arithmetic, manifest version/layout and manifest-last
durability, Kafka protocol codes, registry/pin/object key prefixes, generation
fencing, the eight-byte checkpoint format minimum, nonzero manifest-size
normalization, and the serialized checkpoint command queue capacity are
protocol, format, safety, or internal serialization invariants. They are not
deployment tuning.

### Adjacent Pending Policy

This closes only checkpoint and lifecycle policy. The next coherent Gres-owned
runtime slice is local-vacuum pacing and debt policy in
`crates/gres/src/lib.rs`: idle interval, backoff floor, hot-debt threshold,
maximum key budget, fast/slow step cadence, and idle-after duration. The
range-0 follower retry, durable-inspection limits, and operator
dependency-discovery retries are classified adjacent candidates for later
owner-specific review.

### Gres Checkpoint/Lifecycle Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported 5,913 repository
matches. The focused checkpoint/lifecycle source set contained 81 matches:
34 inline test/harness values, 25 fixed protocol/format/topology/derived
values, eight single-owned policy defaults, and 14 adjacent runtime candidates.
No focused candidate remains unclassified.

- Full affected Gres-control, Gres-substrate, Gres, and operator suites passed,
  including the Gres hard-crash/process matrices and all operator integration
  tests.
- Strict all-target/all-feature Clippy with `-D warnings`, formatting, and
  `git diff --check` passed.
- Fresh generation of all nine CRDs matched `deploy/crds` exactly.
- Focused behavior tests proved threshold precedence, default mapping,
  store-gated flag rendering, no-store startup, live consumer wiring, and
  configured lifecycle cadence through the ready, WAL-deletion-pending, and
  resume-fenced branches.
- Independent reviews found and verified remediation of terminal pin cleanup,
  explicit no-op lifecycle inputs, no-store checkpoint activation, behavioral
  lifecycle coverage, and duplicate default ownership.

## Gres Local Vacuum Policy

Local-engine vacuum pacing and debt policy is complete. Eight optional
CLI/environment inputs feed one validated effective policy: idle interval,
backoff floor, hot-debt threshold, base and maximum key budgets, fast and slow
step thresholds, and foreground idle-after duration. Explicit inputs reject
with substrate mode, and default substrate mode constructs no local policy.

All eight values reach live local-only consumers: relaxed and maintenance
cadence, backoff clamping, hot-debt selection, per-step scan budget, maximum
budget growth, fast/slow latency adaptation, and foreground-idle
classification. The loop is additionally gated by
`SqlEngine::supports_local_vacuum`; replicated/substrate engines return no-op
vacuum results because out-of-WAL local deletion would diverge replicas.
Accordingly, there is intentionally no operator or CRD surface.

### Fixed

Zero-delay hot mode, doubling/halving and the derived four-times maximum,
vacuum cursor interval geometry, sparse-scan minimum cost, cooperative yield
quantum, and the engine's nonzero budget normalization are state-machine,
derived arithmetic, scan-geometry, or safety invariants rather than
independent deployment policy.

### Adjacent Pending Policy

This closes local-vacuum pacing and debt policy. The next coherent Gres-owned
runtime slice is range-0 follower retry/poll policy in
`crates/gres/src/lib.rs`. Durable-inspection limits and operator
dependency-discovery retries remain later adjacent owner-specific reviews.

### Gres Local Vacuum Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported 5,921 repository
matches. The direct local-vacuum scanner subset contains 11 candidates: five
effective configuration defaults, one test/harness marker, and five fixed
engine/state-machine values. The exact focused textual search reports 110
references: 46 production parser/default/live-wiring references in Gres, 50
test/harness references in Gres, and 14 fixed engine/state-machine references
in pgexec. No local-vacuum candidate remains unclassified.

- The full Gres suite passed: 121 library, 25 runtime, 17 topology
  process-nemesis, and 30 topology split-crash tests, with no failures.
- Strict all-target/all-feature Clippy, formatting, and `git diff --check`
  passed.
- Gres help exposes all eight local-vacuum flags.
- `git diff -- deploy/crds` is empty, and focused operator/CRD searches found
  no local-vacuum surface.
- Independent audit traced all eight consumers, scalar and relationship
  validation, local-only spawn, substrate rejection, tests, and diff scope.
  Broader review then found validation occurred after listener binding and
  default-policy tests inherited the host environment. Commits `3be7294e` and
  `18404ed1` moved validation before binding and isolated every default-policy
  test; final hostile-environment re-review returned PASS with no findings.

## Gres Range-0 Follower Poll Policy

The periodic range-0 follower refresh cadence has one shared compiled default,
`DEFAULT_RANGE0_FOLLOWER_POLL_INTERVAL_MS = 100`, owned by `gres-control`.
Standalone Gres exposes the optional
`--range0-follower-poll-interval-ms` /
`CRABKA_GRES_RANGE0_FOLLOWER_POLL_INTERVAL_MS` pair, with CLI taking
precedence over the environment. The fleet surface is the optional
`spec.compute.range0FollowerPollIntervalMs` field. Clap's `PositiveMillis`,
the CRD's `minimum: 1`, and the effective-policy conversion all reject zero;
both Clap requirements and programmatic validation reject explicitly setting
the value without `--ranges`.

Final review found that the programmatic guard originally ran only while
constructing `SubstrateRuntimeConfig`, after the normal serve path had bound
its listener and after the injected-listener path could begin tenant/network
work. Remediation commit `e9e4e07c` extracted
`validate_range0_follower_poll_interval` and calls it before I/O in both serve
entry points as well as in `SubstrateRuntimeConfig::from_args`. Its rejection
test now scrubs the host environment in child processes and separately proves
that an environment value without `--ranges` is rejected. Follow-up commit
`f32cc3e8` moved the occupied-listener regression into the same scrubbed-child
pattern; the complete range-0 follower filter passes four of four tests even
when the host provides a hostile poll-interval environment value.

The complete live consumer path is:

```text
GresComputeSpec
  -> EffectiveGresComputePolicy
  -> multi-range render_deployment argument
  -> ServeArgs
  -> SubstrateRuntimeConfig
  -> attach_range0_read_barrier
  -> wait_for_range0_follower_refresh
```

The operator renders the argument only in the existing range-control branch
that also renders `--ranges`; single-range Deployments omit both. Gres resolves
the optional parser value once into a `Duration`. A process whose hosted-range
set excludes the coordinator attaches the range-0 read barrier and passes that
duration to the follower loop.

The loop still selects between `Notify::notified()` and the configured timer.
The notification is the immediate catalog-barrier wake path, not an
independent cadence, so configuring the periodic fallback does not change its
semantics. Coordinator range identity, hosted-range membership, committed-end
and applied-offset comparisons, follower bootstrap, and WAL offset handling
remain fixed topology/protocol behavior rather than deployment policy. The
three remaining `Duration::from_millis(100)` occurrences are process-test
settling/retry waits, not production follower sleeps.

### Adjacent Pending Policy

This closes the range-0 follower poll cadence only. The next coherent owner is
generic WAL recovery fetch/retry policy, beginning with `FETCH_MAX_WAIT_MS` and
`EMPTY_FETCH_RETRIES` in `crates/gres-substrate/src/recovery.rs`.

### Gres Range-0 Follower Poll Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported 5,941 repository
matches. The exact focused search reported 98 references: six shared-default
owner/import/fallback references, 27 live configured parser/validation/schema/
render/runtime-consumer references, five fixed range-0 topology/bootstrap
references, 60 test/harness references, and no next-owner reference in the
focused path set. The four exact 100 ms literals in that result are one shared
production default and three test/harness waits; there is no unexplained fixed
production 100 ms follower sleep.

- `cargo test -p crabka-gres-control --no-fail-fast`: 80 passed.
- After final-review remediation,
  `cargo test -p crabka-gres --no-fail-fast`: 125 library, 25 runtime, 17
  topology process-nemesis, and 30 topology split-crash tests passed, for 197
  top-level tests with no failures.
- `cargo test -p crabka-operator --no-fail-fast`: 931 test and doc-test
  results passed.
- Combined control, Gres, and operator evidence is 1,208 top-level test and
  doc-test results passed.
- Strict all-target/all-feature Clippy with `-D warnings`, `cargo fmt --check`,
  and `git diff --check` passed.
- Gres help displayed the exact CLI/environment pair.
- Fresh operator generation produced nine CRDs, and `diff -ru` against
  `deploy/crds` was empty.
- Focused tests proved CLI-over-environment precedence, default selection,
  zero and explicit non-multirange rejection, pre-I/O validation,
  environment-hermetic rejection including environment-without-ranges,
  scrubbed occupied-listener precedence under a hostile environment,
  configured timer wake, notification wake preservation, multi-range argument
  rendering, and single-range omission. The hostile-environment range-0
  follower filter passed 4/4.

## Gres WAL Recovery Read Policy

Normal committed-WAL recovery now resolves one validated
`RecoveryReadPolicy` with four substrate-owned defaults:

- `DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS = 100`;
- `DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES = 1_048_576`;
- `DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES`, which reuses
  `client-core`'s named 50 MiB default;
- `DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES = 100`.

Standalone Gres exposes one optional CLI/environment pair per value. Explicit
values require substrate mode, and `PositiveI32`/`PositiveUsize` plus
`RecoveryReadPolicy` reject zero. The fleet surface adds four optional
`spec.compute.walRecovery*` fields with schema minimum one; the operator
validates omitted values through the shared defaults and renders all four
effective arguments for both single-range and multi-range computes.

The complete live path is:

```text
GresComputeSpec
  -> EffectiveGresComputePolicy
  -> render_deployment
  -> ServeArgs
  -> SubstrateRuntimeConfig::recovery_read_policy
  -> SubstrateRuntimeConfig::live_recovery_config
  -> LiveRecoveryConfig::with_read_policy
  -> KafkaCommittedWalReader
  -> recovery_fetch / build_fetch_request / empty_fetch_decision
```

There is exactly one production `LiveRecoveryConfig::new` in Gres, inside the
shared helper. Its production call sites cover the range-0 follower,
multi-range recovery map, single-range recovery, activation discovery,
successor recovery, and staged transfer recovery. Every one therefore receives
the configured policy.

The old `FETCH_MAX_WAIT_MS`, `FETCH_MAX_BYTES`, and `EMPTY_FETCH_RETRIES`
identifiers have no repository matches. Normal recovery reads wait and apply
both configured byte limits; retry exhaustion uses the configured consecutive
empty-fetch limit and resets after cursor progress. The sole production
recovery zero-wait path is `END_SAMPLE_MAX_WAIT_MS`, used by the committed-end
sampler so a stable-end probe returns immediately.

All nine repository `IsolatedFetch` literals explicitly choose `max_bytes`.
Recovery uses the configured response limit; positional and unrelated
client-streams, registry, FDW, and Gres registry-fetch callers retain the named
client-core default. Partition zero, read-committed isolation, the one-byte
minimum, offset-out-of-range handling, and offset arithmetic remain fixed
protocol or algorithm behavior.

### Adjacent Pending Policy

This closes only normal WAL recovery read wait, byte, and empty-retry policy.
The next coherent owner is the recovery connection policy in
`crates/gres-substrate/src/recovery.rs`: the fixed 10-second connect timeout
and 30-second request timeout. DNS behavior, topic creation, and writer policy
remain separate owners.

### Gres WAL Recovery Read Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported 5,949 repository
matches. The exact policy-focused search reported 267 references: 22
shared-default production references, 115 configured production
type/parser/validation/schema/render/runtime references, two fixed
committed-end zero-wait references, and 128 test/harness references. The
separate protocol-invariant search reported 28 references: 24 production and
four tests. The deferred timeout search reported exactly two production
references. No focused candidate remains unclassified.

The exact `100`, `1_048_576`, and `50 * 1024 * 1024` search in client fetch and
substrate recovery reported eight references: four named production defaults
and four client-core test values. There is no fixed normal-recovery fetch wait,
byte limit, or empty-retry value outside the shared defaults.

- The affected six-package test command reported 1,522 passing test and
  doc-test results, zero failures, and four ignored Docker-only tests.
- The strict all-target/all-feature Clippy command is blocked by the unchanged
  267-line `kafka_fdw_roundtrip_avro_and_raw_fallback` test in
  `crates/gres-fdw/tests/roundtrip.rs`, which violates the repository's
  200-line `clippy::too_many_lines` limit. The file is unchanged across this
  slice. Strict Clippy passed for every other affected package and target;
  the full FDW target set passed with only that one pre-existing lint allowed.
- Gres help displayed all four exact CLI/environment pairs.
- Fresh operator generation produced nine CRDs, and `diff -ru` against
  `deploy/crds` was empty.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- Focused tests cover defaults, zero rejection, CLI-over-environment
  precedence, hostile-environment isolation, pre-I/O inert-use rejection,
  request wiring, retry boundaries and reset, shared-helper propagation,
  single/multi-range rendering, schema minima, and the fixed sampler zero wait.

## Gres WAL Recovery Connection Timeouts

The raw committed-WAL connection now receives its connect and request
timeouts from the existing validated `RecoveryReadPolicy`. Substrate owns the
10,000 ms and 30,000 ms defaults. `with_timeouts` validates both positive
millisecond values with `refined_type`, and `wal_connection_options` applies
them to `ConnectionOptions`.

Both raw connection paths carry the same policy:

```text
LiveEndDialer::dial
  -> open_wal_connection(read_policy)
  -> wal_connection_options

KafkaCommittedWalReader::open_connection
  -> open_wal_connection(read_policy)
  -> wal_connection_options
```

The former recovery-local `Duration::from_secs(10)` and
`Duration::from_secs(30)` expressions have no matches in
`crates/gres-substrate/src/recovery.rs`. The committed-end sampler retains its
fixed zero fetch wait; this is an immediate-probe algorithm invariant, not a
connection timeout.

Standalone Gres exposes
`--wal-recovery-connect-timeout-ms` /
`CRABKA_GRES_WAL_RECOVERY_CONNECT_TIMEOUT_MS` and
`--wal-recovery-request-timeout-ms` /
`CRABKA_GRES_WAL_RECOVERY_REQUEST_TIMEOUT_MS`. Together with the four recovery
read settings, all six CLI/environment settings resolve in the single
`SubstrateRuntimeConfig::recovery_read_policy` helper. Explicit recovery
settings still require substrate mode and are rejected before listener or
network I/O.

The fleet CRD adds optional positive
`spec.compute.walRecoveryConnectTimeoutMs` and
`spec.compute.walRecoveryRequestTimeoutMs` fields. The existing effective
compute policy validates all six recovery values and renders all six effective
arguments for both single-range and multi-range substrate Deployments.

### Deferred Timeout Owners

This slice changes only raw recovery `ConnectionOptions`. Seven recovery
admin-client connections, the raw path's DNS lookup, the producer builder, and
the WAL topic ensure timeout remain separate owners. Registry policy is
already independently configured; generic client-library defaults serve other
callers and are not recovery-local fallbacks. The next coherent review is the
recovery admin/topic-operation policy, beginning with
`WAL_TOPIC_ENSURE_TIMEOUT_MS` and the admin-client connection settings.

### Gres WAL Recovery Timeout Evidence

On 2026-07-24 `tools/audit-runtime-values.sh` reported 5,955 repository
matches. The exact timeout-focused search plus the committed-end invariant
reported 113 references: 10 shared-default production references, 41
configured production parser/validation/schema/render/runtime references, two
fixed committed-end sampler references, and 60 test/harness references. No
focused candidate remains unclassified.

- The full six-package test command reported 1,858 passing test and doc-test
  results and one ignored timing benchmark. The only failure was
  `real_range_partition_aborts_transfer_and_heal_restores_2pc`, whose
  post-heal credit operation timed out after five seconds. An exact rerun
  failed identically, and a detached pre-slice `65773fbc` worktree rebuilt with
  its own Gres binary failed identically, so this is an unchanged baseline
  process-nemesis failure.
- Strict all-target/all-feature Clippy passed for every affected target except
  the unchanged 267-line
  `kafka_fdw_roundtrip_avro_and_raw_fallback` test, which exceeds the
  repository's 200-line `clippy::too_many_lines` limit. The complete FDW target
  set passed with only that pre-existing lint allowed.
- Gres help displayed all six exact recovery CLI options.
- Fresh operator generation produced nine CRDs, and `diff -ru` against
  `deploy/crds` was empty.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- Focused coverage pins compiled defaults, positive validation, distinctive
  timeout replacement without fetch-policy mutation, exact
  `ConnectionOptions` wiring, environment and CLI precedence, hostile
  environment isolation, pre-I/O inert-use rejection, shared-helper
  propagation, schema minima, and exact single-/multi-range Deployment
  arguments.

## Gres WAL Admin and Topic Policy

WAL recovery now resolves one validated `WalAdminPolicy` with four
substrate-owned defaults:

- `DEFAULT_WAL_TOPIC_REPLICATION_FACTOR = 1`;
- `DEFAULT_WAL_TOPIC_ENSURE_TIMEOUT_MS = 30_000`;
- `DEFAULT_WAL_ADMIN_CONNECT_TIMEOUT_MS = 5_000`;
- `DEFAULT_WAL_ADMIN_REQUEST_TIMEOUT_MS = 30_000`.

`WalAdminPolicy::new` validates replication against the Kafka `i16` wire
range and validates all four values as positive through `refined_type`.
`LiveRecoveryConfig` owns the effective policy. The two existing public names
`WAL_TOPIC_REPLICAS` and `WAL_TOPIC_ENSURE_TIMEOUT_MS` remain only as
compatibility aliases of the new defaults; live recovery does not consume
them as independent policy.

All seven recovery admin connections route through one helper:

```text
read_live_committed_tail
read_live_retained_committed
LiveEndDialer::dial
ensure_live_wal_topic
bootstrap_live_range0_follower
recover_live_for_range_inner
live_committed_reader
  -> connect_wal_admin
  -> AdminClient::connect_with_options
```

The helper supplies the configured client id, security, connect timeout, and
request timeout. `AdminClient` stores that complete `ConnectionOptions`
template and clones it for both controller and bootstrap reconnects. Its
legacy constructors still resolve their named 5-second/30-second defaults for
unrelated callers.

Topic creation passes the configured replication factor and ensure timeout
through `ensure_wal_topic_name_with_policy` and the narrow `TopicAdmin` seam.
The legacy ensure functions retain default behavior. WAL partitions remain
one to preserve ordered range WAL semantics; `cleanup.policy=delete` and
`retention.ms=-1` remain fixed durability invariants. The committed-end
zero-wait sample, partition zero, read-committed isolation, and Kafka
`TOPIC_ALREADY_EXISTS` code are fixed protocol behavior.

Standalone Gres exposes the four WAL-admin CLI/environment pairs and combines
them with the existing six WAL-recovery settings in the same pre-I/O
validation and hostile-environment matrix. The single
`SubstrateRuntimeConfig::live_recovery_config` helper applies both validated
policies to all Gres recovery construction sites. The fleet CRD exposes
four optional positive `spec.compute.walTopic*`/`walAdmin*` fields, validates
omitted values through the shared defaults, and renders all four effective
arguments for both single- and multi-range substrate computes. Together, all
ten WAL recovery/admin CLI, environment, and CRD settings have live
consumers.

### Adjacent Pending Policy

This closes recovery admin connection and topic-creation policy only. DNS
resolution, checkpoint deletion, registry clients, and generic client
defaults remain separate owners. The next coherent Gres owner is the
transactional WAL producer construction/initialization policy, beginning with
the producer request timeout, retry backoff, and transaction timeout currently
inherited from `crabka-client-producer`.

### Gres WAL Admin Evidence

On 2026-07-25 `tools/audit-runtime-values.sh` reported 5,978 repository
matches. The exact policy-focused search reported 250 references: 26
shared-default or compatibility references, 86 configured production
type/parser/validation/schema/render/runtime references, and 138 test/harness
references. A separate fixed protocol/durability search reported 17
references. No focused candidate remains unclassified.

- The six-package affected test command reported 1,539 passing test and
  doc-test results, zero failures, and four ignored Docker-only tests.
- Strict all-target/all-feature Clippy with `-D warnings` passed for
  `crabka-client-core`, `crabka-client-admin`, `crabka-gres-control`,
  `crabka-gres-substrate`, `crabka-gres`, and `crabka-operator`.
- Gres help displayed all ten WAL recovery/admin options, including the four
  new topic/admin pairs.
- Fresh operator generation produced nine CRDs; both directories contained
  nine files and `diff -ru` against `deploy/crds` was empty.
- `cargo fmt --all -- --check` and `git diff --check` passed. Stable rustfmt
  emitted only the repository's existing nightly-option warnings.
- Focused tests cover exact defaults and bounds, zero and overflow rejection,
  environment parsing and CLI precedence, hostile-environment isolation,
  pre-listener validation, shared recovery propagation, topic request values,
  initial connection options, controller/bootstrap reconnect preservation,
  schema bounds, and exact single-/multi-range Deployment arguments.

## Gres WAL Producer Retry Policy

The generic producer now resolves one validated `ProducerRetryPolicy` with
seven shared defaults:

- 30-second request timeout;
- `i32::MAX` retries after the initial batch send;
- 100-millisecond retry and producer-ID initial backoff;
- 30-second per-batch routing retry budget;
- 30-second producer-ID initialization retry timeout;
- 1-second producer-ID backoff cap;
- 60-second transaction timeout.

`ProducerRetryPolicy::new` validates positive durations and nonnegative retries
through `refined_type`, bounds protocol millisecond fields to `i32::MAX`, and
rejects an initial backoff above its cap. The existing producer builder keeps
its source-compatible defaults and validates the complete policy before
opening a broker connection.

The sender stores `retries_used` on each prepared batch across reroutes and
resends. It admits a resend only through `take_retry`, while
`collect_retries` independently expires the batch's configured wall-clock
routing budget. Thus the existing `retries` builder input is live, and the
first exhausted limit terminates the batch. The request timeout reaches both
client connection/request handling and the Produce request wire field.

Both producer-ID initialization paths use the same configured retry helper:
idempotent producer construction in `Producer::start` and transactional
initialization/reinitialization in `Producer::init_transactions`. The helper
receives the configured initialization timeout, initial backoff, and maximum
backoff. Transactional requests use the validated exact transaction-timeout
millisecond conversion rather than a fallback.

Gres has one `Producer::builder()` construction site. Its
`LiveRecoveryConfig` policy supplies all seven builder values before the
transactional producer is initialized. Standalone Gres exposes seven exact
CLI/environment pairs under `--wal-producer-*` and
`CRABKA_GRES_WAL_PRODUCER_*`. The fleet CRD exposes the matching seven optional
`spec.compute.walProducer*` fields, validates them through the shared policy,
and renders all seven effective arguments in both single- and multi-range
compute Deployments.

Protocol coordinator error codes, disabled producer identities, the
single-in-flight-per-partition ordering rule, and the 1-millisecond/50-
millisecond scheduler floors remain fixed invariants. Compression, linger,
batch size, and the per-connection in-flight limit remain the separate
producer-throughput policy owner; none is a retry deadline or backoff.

### Adjacent Pending Policy

This closes the Gres WAL producer retry and transaction policy only. Producer
throughput, DNS resolution, checkpoint deletion, registry clients, and other
generic client defaults remain separate owners.

### Gres WAL Producer Retry Evidence

On 2026-07-25 `tools/audit-runtime-values.sh` reported 6,070 repository
matches. The exact producer-policy search reported 260 references, and the
focused fixed-invariant/throughput search reported 58 references. Production
searches found no retry deadline or backoff literal outside the seven named
shared defaults; the remaining timing literals are the classified scheduler
floors.

- The four-package affected test reruns reported 1,438 passing test and
  doc-test results, zero failures, and zero ignored tests after excluding one
  exact unrelated baseline failure.
- `production_service_manifest_loss_after_truncate_refuses` failed unchanged
  at both current HEAD and pre-slice commit `aeecec39`; the isolated current
  run and the pre-slice run produced the same `must refuse torn truncation`
  assertion at `checkpoint_crashes.rs:362`.
- Strict all-target/all-feature Clippy with `-D warnings` passed for
  `crabka-client-producer`, `crabka-gres-substrate`, `crabka-gres`, and
  `crabka-operator`.
- Gres help displayed all seven exact WAL producer options.
- Two fresh operator generations each produced nine CRDs; they matched each
  other and the nine checked-in CRDs exactly.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- Focused coverage pins exact defaults and bounds, pre-I/O rejection,
  configured producer-ID retry timing, exact transaction-timeout conversion,
  retry-count and routing-budget exhaustion, environment and CLI precedence,
  hostile-environment isolation, shared recovery propagation, exact schema
  bounds/errors, and exact single-/multi-range Deployment arguments.

## Gres WAL Producer Throughput Policy

The generic producer now resolves one validated `ProducerThroughputPolicy`
with four shared defaults:

- no compression;
- zero linger;
- 16,384 batch bytes;
- five cross-partition in-flight requests.

`ProducerThroughputPolicy::new` uses `refined_type` to require whole-millisecond
linger in `0..=i32::MAX`, batch bytes in `1..=i32::MAX`, and a positive
cross-partition in-flight limit. The producer builder preserves its existing
arguments and defaults, but validates the complete policy before opening a
broker connection.

All four settings have live generic consumers. Batch bytes construct every
per-topic/partition `Accumulator`, where they control rollover. Compression is
encoded into each `RecordBatch`'s attributes. The sender limits each drain
cycle's combined new and retry fanout to the configured cross-partition
maximum. `MAX_IN_FLIGHT_PER_PARTITION = 1` remains fixed because idempotent
same-partition ordering requires a single outstanding sequence until the
client can guarantee ordered frame writes.

Linger is batch-relative rather than poll-relative. The first append records an
`Instant`; subsequent appends to the same partial batch neither reset its
deadline nor wake the sender. Rollover wakes ready work, zero linger sends a
force wake, and explicit flush and shutdown force draining. The sender sleeps
until the earliest exact batch or retry deadline, so off-phase appends send at
their own `first_append_at + linger` deadline. The implementation therefore has
no production 1-millisecond scheduler floor: that planned fixed classification
was superseded by the deadline-driven sender. Remaining 1-millisecond matches
in this crate are test inputs.

Gres stores the shared policy in `LiveRecoveryConfig` and applies all four
values at its sole `Producer::builder()` call. Standalone Gres exposes exactly
three CLI/environment pairs:

- `--wal-producer-compression` /
  `CRABKA_GRES_WAL_PRODUCER_COMPRESSION`;
- `--wal-producer-linger` /
  `CRABKA_GRES_WAL_PRODUCER_LINGER`;
- `--wal-producer-batch` /
  `CRABKA_GRES_WAL_PRODUCER_BATCH`.

The fleet CRD adds the matching optional
`spec.compute.walProducerCompression`, `walProducerLinger`, and
`walProducerBatch` fields. Compression has the exact
`none`/`gzip`/`snappy`/`lz4`/`zstd` enum; linger and batch size carry the same
protocol bounds as the shared policy. The effective compute policy validates
the three values and the central Deployment renderer emits each effective
argument exactly once in both single- and multi-range modes.

Gres intentionally does not expose the generic maximum. Its WAL producer
always targets partition zero, so the fixed one-request-per-partition ordering
rule is the effective limit and changing the cross-partition fanout would do
nothing.

### Adjacent Pending Policy

This closes compression, batching, linger, and cross-partition producer fanout
only. `Producer::flush` still uses a fixed 50-millisecond notification wait for
up to 1,000 iterations. That approximately 50-second flush bound is live but
is not a throughput setting; it remains a separate flush-lifecycle owner.
DNS resolution, checkpoint deletion, registry clients, and other generic
client defaults also remain separate owners.

### Gres WAL Producer Throughput Evidence

On 2026-07-25 `tools/audit-runtime-values.sh` reported 6,109 repository
matches. The exact throughput-focused search reported 356 references across
generic policy/default/validation/runtime code, the three Gres
CLI/environment/CRD flows, fixed behavior, and tests. Production searches
found no compression, linger, batch-byte, or cross-partition fanout fallback
outside the four named shared defaults. The separate flush wait above is the
only adjacent live producer value found.

- The generic producer's 93 unit tests passed. Focused substrate, Gres, and
  operator policy tests also passed.
- Focused coverage pins exact defaults and protocol bounds, pre-I/O rejection,
  canonical compression parsing, accumulator rollover, encoded compression,
  batch-relative and off-phase linger deadlines, zero-linger/flush/shutdown
  force drains, rollover wake behavior, and max-in-flight retry/new-batch
  fanout.
- Gres coverage pins defaults, environment and CLI precedence, hostile
  environment isolation, exact runtime propagation, help, schema bounds and
  enum values, and exact single-/multi-range Deployment arguments.

## Gres WAL Producer Flush Policy

The generic producer now has one named
`DEFAULT_PRODUCER_FLUSH_TIMEOUT` of 50 seconds. The public
`ProducerFlushTimeout` scalar accepts only whole milliseconds in
`1..=2,147,483,647`; it rejects zero, fractional milliseconds, and overflow
before broker connection I/O. The source-compatible producer builder retains
its raw `Duration` input, validates it into that scalar, and stores the typed
deadline on the producer.

`Producer::flush` sends the existing force-drain wake, computes one absolute
deadline, and enables its `Notify` future before checking accumulator and
in-flight state. This subscribe-before-check loop cannot miss a notification
delivered during the asynchronous state check. It waits only for state changes
until the configured deadline; the old 50-millisecond polling interval and
1,000-attempt loop were removed rather than exposed as meaningless settings.

Gres carries the typed value through `SubstrateRuntimeConfig` and the existing
`LiveRecoveryConfig` into its sole WAL `Producer::builder()` construction.
Standalone Gres exposes the exact
`--wal-producer-flush-timeout-ms` /
`CRABKA_GRES_WAL_PRODUCER_FLUSH_TIMEOUT_MS` pair. The fleet CRD exposes the
matching optional `spec.compute.walProducerFlushTimeoutMs` field with the same
positive protocol bounds, validates its effective value through
`ProducerFlushTimeout`, and renders one exact argument pair in every
single- and multi-range compute Deployment.

### Adjacent Pending Policy

This closes only the Gres WAL producer flush deadline. DNS resolution,
checkpoint deletion, registry clients, and other generic client defaults
remain separate owners, and the repository-wide hardcoded-value audit remains
active. The next coherent live owner is the raw WAL DNS lookup in
`gres-substrate/src/recovery.rs`, which currently awaits
`tokio::net::lookup_host` without an independently audited deadline.

### Gres WAL Producer Flush Evidence

On 2026-07-25 `tools/audit-runtime-values.sh` reported 6,122 repository
matches across 1,048 files. The exact flush-focused search reported 119 lines
across 13 files: 48 production or checked-in schema references and 71
test/harness references.

Every production match is classified: 13 generic producer references define
or re-export the validated scalar, builder field, stored deadline, and absolute
deadline wait; 2 define and propagate the generic flush error; 9 substrate
references carry the typed value into the sole producer construction; 13
standalone Gres references cover CLI/environment resolution and runtime
propagation; and 11 operator or CRD references cover effective policy, argument
rendering, and the checked-in schema. The only focused generic 50-millisecond
match is paused-time test input, and no 1,000-attempt loop remains.

- Generic producer all-target verification reported 98 passing tests. Focused
  deadline tests prove the exact configured timeout and the missed-wakeup
  interleaving.
- Gres substrate all-target verification reported 173 passing unit tests plus
  every integration target. Gres reported 136 unit, 25 runtime, 17 topology
  nemesis, and 30 split-crash tests passing.
- Operator all-target verification reported 719 passing library tests plus
  every integration target. Focused schema/rendering tests pin the exact
  bounds, default, override, errors, and one argument pair per single- and
  multi-range Deployment.
- Strict all-target Clippy passed for each affected crate, Gres help displays
  `--wal-producer-flush-timeout-ms`, and two fresh nine-CRD generations matched
  each other before the checked-in Gres CRD was updated.
- `cargo fmt --all -- --check` and `git diff --check` passed throughout the
  implementation tasks.

## Gres WAL Recovery DNS Timeout Policy

Raw committed-WAL hostname resolution now has one named
`DEFAULT_WAL_RECOVERY_DNS_TIMEOUT_MS` default of 10,000 milliseconds.
`RecoveryReadPolicy::with_dns_timeout` validates the value through the existing
`refined_type`-backed positive-duration helper. Standalone Gres accepts
`--wal-recovery-dns-timeout-ms` and
`CRABKA_GRES_WAL_RECOVERY_DNS_TIMEOUT_MS`; the fleet API accepts optional
`spec.compute.walRecoveryDnsTimeoutMs`, whose generated CRD schema has
`minimum: 1`.

The effective value follows the existing
`RecoveryReadPolicy` -> `LiveRecoveryConfig` -> `open_wal_connection` path.
`SubstrateRuntimeConfig` resolves CLI-over-environment precedence into the
policy, `live_recovery_config` installs that policy on every raw WAL recovery
configuration, and `open_wal_connection` applies `dns_timeout()` only around
`tokio::net::lookup_host`. TCP establishment remains independently bounded by
the existing connect timeout.

The focused scan found 91 lines across 25 files: 43 production or checked-in
schema references, 45 test or harness references, and 3 prior audit references.
Of the 43 production/schema references, 30 define, propagate, render, or consume
this WAL policy. The remaining 13 belong to unresolved generic client
bootstrap/pool resolution, client-admin, client-streams, Gres FDW, and Raft;
this slice does not cover them. The only raw WAL `lookup_host` call is the one
in `open_wal_connection`, and it is bounded by
`RecoveryReadPolicy::dns_timeout`.

### Adjacent Pending Policy

This closes only raw committed-WAL DNS resolution. The next coherent unresolved
owner is generic Kafka client DNS resolution in
`crates/client-core/src/bootstrap.rs` and `crates/client-core/src/pool.rs`,
where initial bootstrap and advertised-broker lookups currently have no
independently audited deadline. Client-admin, client-streams, Gres FDW, and
Raft lookups remain separate visible owners. The repository-wide hardcoded
operational-value audit remains active.

### Gres WAL Recovery DNS Timeout Evidence

On 2026-07-25 `tools/audit-runtime-values.sh` reported 6,138 lines across 1,048
files. The focused
`rg -n "lookup_host|DNS lookup|dns[_-]timeout|DnsTimeout|WAL_RECOVERY_DNS_TIMEOUT|wal-recovery-dns-timeout|walRecoveryDnsTimeout" crates deploy/crds docs/configuration-audit.md`
search produced the 91-line classification above.

- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-gres-substrate wal_dns_lookup --lib` passed all three focused tests. The paused-time pending-resolver case stopped at the exact configured 37-millisecond deadline; success, resolver-error, and empty-result behavior also passed without external DNS.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-gres-substrate -p crabka-gres -p crabka-operator --all-targets` exited successfully; every emitted suite summary reported zero failures and zero ignored tests. Nested child-process summaries can interleave, so no canonical aggregate test inventory is claimed.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-gres-substrate -p crabka-gres -p crabka-operator --all-targets -- -D warnings` passed.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q -p crabka-gres -- --help | rg -- '--wal-recovery-dns-timeout-ms'` displayed the exact standalone flag.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check` and `git diff --check` passed.
- Two fresh `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q -p crabka-operator -- gen-crds <temporary-directory>` generations each produced nine files and matched each other and `deploy/crds` exactly.

## Client Core DNS Timeout Policy

Generic Kafka client resolution now has one shared
`DEFAULT_CLIENT_DNS_TIMEOUT` of 10 seconds. `ClientDnsTimeout` uses
`refined_type` to accept only positive, whole-millisecond durations
representable as `u64` milliseconds. The client builder validates its raw
`Duration` before resolver or socket I/O and stores the typed value in
`ConnectionOptions`.

Initial bootstrap resolution and `Client::reconnect_bootstrap` pass the stored
policy to `bootstrap::resolve`. Each non-empty bootstrap entry independently
passes `tokio::net::lookup_host` through `bounded_lookup`; a failed or expired
entry is skipped, later entries retain a full attempt, and exhaustion still
returns `Disconnected`. `BrokerPool` copies the same policy from
`ConnectionOptions`; each advertised-broker lookup passes through
`first_resolved_addr` and then `bounded_lookup`, so an unresolved broker remains
absent and metadata refresh remains best-effort.

DNS, TCP establishment, and request handling retain independent deadlines. The
named defaults are 10 seconds for each DNS lookup, 30 seconds for each TCP
connection attempt, and 30 seconds for each request.

`tools/audit-runtime-values.sh` reported 6,149 lines across 1,049 files. The
exact focused search reported 142 lines: 69 production references, 61 test or
harness references, and 12 audit
references.

Every focused match is classified by the following reproducible groups:

- The 69 production matches comprise 39 client-core references and 30 visible
  references owned elsewhere. The client-core subtotal is 6 bootstrap, 8
  client-builder/propagation, 11 policy/default/connection-option, 2 re-export,
  and 12 pool references. The other 30 are 3 client-admin, 2 client-streams, 2
  Gres FDW, 11 Gres substrate, 4 Gres, 1 operator controller, 4 operator CRD,
  and 3 Raft references.
- The 61 test/harness matches comprise 34 client-core inline-test references
  and 27 references in other inline tests or integration-test harnesses.
- The 12 remaining matches are in this and the prior Gres WAL recovery audit
  sections.

The two production `lookup_host` sites owned by client-core are both bounded:
`bootstrap.rs` passes its resolver future directly to `bounded_lookup`, while
`pool.rs` passes its resolver future to `first_resolved_addr`, which calls the
same seam. This statement does not close client-admin, client-streams, Gres FDW,
Gres substrate, Gres, operator, or Raft DNS ownership.

### Adjacent Pending Policy

The next coherent owner is propagation of this typed policy through the
higher-level producer, consumer, streams, and admin builders and their
deployment configuration surfaces. Other raw resolver sites remain separate
visible owners, and the repository-wide hardcoded operational-value audit
remains active.

### Client Core DNS Timeout Evidence

On 2026-07-25 the following fresh gates exited successfully:

- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-client-core --all-targets`;
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-client-core --all-targets -- -D warnings`;
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check`;
- `git diff --check`.

## Gres WAL Producer DNS Timeout

The Gres WAL producer reuses client-core's validated `ClientDnsTimeout` and
10-second `DEFAULT_CLIENT_DNS_TIMEOUT`; it adds no producer-specific DNS type
or default. `Producer::start` validates the builder's raw duration before
constructing `Client`, then forwards the exact validated duration through
`Client::builder().dns_timeout(...)`. DNS remains independent of the
producer's connect/request/retry/transaction policy.

Standalone Gres owns the
`--wal-producer-dns-timeout-ms` /
`CRABKA_GRES_WAL_PRODUCER_DNS_TIMEOUT_MS` pair. CLI input takes precedence
over environment input, absence selects the shared 10,000-millisecond default,
and explicit use without substrate mode is rejected. The fleet owner is the
optional positive `spec.compute.walProducerDnsTimeoutMs` field in the Gres
CRD.

The complete live path is:

```text
GresComputeSpec.wal_producer_dns_timeout_ms
  -> EffectiveGresComputePolicy.wal_producer_dns_timeout
  -> wal_producer_args / --wal-producer-dns-timeout-ms
  -> ServeArgs.wal_producer_dns_timeout_ms
  -> SubstrateRuntimeConfig.producer_dns_timeout
  -> LiveRecoveryConfig::with_producer_dns_timeout
  -> recover_live_for_range_inner
  -> Producer::builder().dns_timeout
  -> Client::builder().dns_timeout
  -> ConnectionOptions.dns_timeout
  -> bootstrap and advertised-broker bounded lookup
```

The operator extends the shared WAL-producer argument vector once before
single- versus multi-range rendering. Gres constructs
`LiveRecoveryConfig` through one shared runtime helper, and
`recover_live_for_range_inner` contains the only production Gres WAL
`Producer::builder()` call. Thus the standalone and CRD inputs converge on one
live Gres consumer; this does not cover other producer deployments.

### Adjacent Pending Policy

Production producer constructions still default their DNS policy in
bench-driver, client-streams, Gres registry, gRPC Gateway, metrics,
metrics-service, observability, observability-demo-app, profiles,
remote-storage-topic, replicator, Schema Registry, and traces. Their
deployment surfaces remain separate owners.

The higher-level `client-consumer` builder, the client-streams raw resolver and
embedded clients/producers, and client-admin's raw resolver and client
connections also remain unresolved. Gres FDW and Raft raw resolver sites are
separate visible owners. The next coherent owner is the Gres registry producer
in `crates/gres-control/src/registry.rs`, whose shared registry deployment
surfaces can carry the typed client policy without conflating it with WAL
recovery or WAL producer configuration. The repository-wide hardcoded
operational-value audit remains active.

### Gres WAL Producer DNS Timeout Evidence

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,155 lines across 1,049 files. The exact required search

```text
rg -n "lookup_host|ClientDnsTimeout|DEFAULT_CLIENT_DNS_TIMEOUT|dns[_-]timeout|DnsTimeout|wal-producer-dns-timeout|walProducerDnsTimeout" crates deploy/crds docs/configuration-audit.md
```

reported 226 lines across 28 files: 94 production-code references, two
checked-in CRD schema references, 117 test/harness references, and 13 prior
audit references. The 96 production/schema references include the completed
generic client and Gres WAL recovery paths as well as the Gres WAL producer
path and the unresolved owners listed above; the count is evidence inventory,
not a claim that all 96 are covered.

Fresh focused verification on 2026-07-25 passed:

- producer builder filter: 8 passed, including invalid-before-I/O and exact
  override coverage;
- Gres substrate producer-DNS filter: 1 passed;
- standalone Gres producer-DNS filter: 2 passed, including default,
  environment/CLI precedence, zero rejection, and substrate-only use;
- operator producer-DNS filter: 2 passed, covering schema/default/error and
  exact-once single-/multi-range rendering;
- `crabka-gres --help` displayed
  `--wal-producer-dns-timeout-ms`;
- a fresh operator generation produced nine CRDs and matched `deploy/crds`
  exactly; and
- `git diff --check` passed.

This evidence closes only the Gres WAL producer DNS slice. It does not close
the pending crate-level coverage entries or the repository-wide goal.

## Gres Registry Producer DNS Timeout

The Gres registry producer reuses client-core's validated `ClientDnsTimeout`
and 10-second `DEFAULT_CLIENT_DNS_TIMEOUT`; it adds no registry-specific DNS
type or default. The four standalone surfaces accept the exact
`--registry-producer-dns-timeout-ms` CLI flag and
`CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS` environment variable. The Kafka
CRD owns optional `spec.gresRegistry.producerDnsTimeoutMs`, whose checked-in
schema has `minimum: 1`.

Absence selects the shared validated 10,000-millisecond default. Each
standalone parser validates explicit input through `PositiveMillis` and
`RegistryPolicy::with_producer_dns_timeout_ms`; CLI input takes precedence over
environment input. The CRD follows the same typed override path.

The effective fleet value follows:

```text
Kafka.spec.gresRegistry.producerDnsTimeoutMs
  -> RegistryPolicy::with_producer_dns_timeout_ms
  -> operator control policy / rendered --registry-producer-dns-timeout-ms
  -> standalone RegistryOptions::policy
  -> Registry::connect_with_policy
  -> Producer::builder().dns_timeout
  -> Client::builder().dns_timeout
  -> ConnectionOptions.dns_timeout
  -> client-core bounded bootstrap and advertised-broker lookup
```

Operator control construction and both activator and compute rendering consume
the same effective `RegistryPolicy`. Standalone Gres, `crabka gres`, the Gres
activator, and the Gres load-test each own the exact CLI/environment input; the
load-test also forwards one exact flag/value pair to each child. Structural
inspection found exactly one registry `Producer::builder()` construction, in
`Registry::connect_with_policy`, and it consumes
`policy.producer_dns_timeout().duration()`.

The required focused scan reported 322 lines across 38 files: 134 production
references across 21 files, 3 checked-in CRD schema references across 2 files,
156 test or harness references across 31 files, and 29 prior-audit references
in this file. These groups classify the complete focused result, including
previously completed and still-open DNS owners; they do not claim all matches
are covered by this slice.

### Adjacent Pending Policy

Raw registry reader and registry admin DNS paths remain open. Registry refresh
and its background reader still use `resolve_bootstrap_addr` in
`crates/gres-control/src/registry.rs`, while registry topic creation and
metadata refresh use `AdminClient`, whose raw `tokio::net::lookup_host` path is
in `crates/client-admin/src/lib.rs`. The next coherent unresolved configuration
owner is the Gres registry reader/admin DNS policy across those paths. Other
raw resolver sites and unrelated producers remain separate visible owners, and
the repository-wide hardcoded operational-value audit remains active.

### Gres Registry Producer DNS Timeout Evidence

On 2026-07-25 `tools/audit-runtime-values.sh` reported 6,170 lines across 1,051
files. The exact required search

```text
rg -n "lookup_host|ClientDnsTimeout|DEFAULT_CLIENT_DNS_TIMEOUT|dns[_-]timeout|DnsTimeout|registry-producer-dns-timeout|producerDnsTimeoutMs" crates deploy/crds docs/configuration-audit.md
```

produced the 322-line classification above.

The implementation-task verification reported:

- Gres control: 81 tests passed; strict all-target Clippy, formatting, and diff
  hygiene passed.
- The four standalone packages: 379 tests passed, zero failed, and one
  pre-existing live-cluster test was ignored; strict all-target Clippy,
  formatting, and diff hygiene passed. Gres help displayed the exact
  `--registry-producer-dns-timeout-ms` flag.
- Operator: the focused registry CRD, cache, activator, and compute tests
  passed; the full all-target test and strict Clippy gates passed; all nine CRDs
  were regenerated deterministically, and only
  `crabka.io_kafkas.yaml` changed.

This evidence closes only the Gres registry producer DNS slice. Raw registry
reader/admin DNS ownership and the repository-wide goal remain open.

## Gres Registry Reader/Admin DNS Timeout

The Gres registry reader and admin paths reuse client-core's validated
`ClientDnsTimeout` and 10-second `DEFAULT_CLIENT_DNS_TIMEOUT`; they add no
registry-specific timeout type or default. The four standalone surfaces accept
the exact `--registry-reader-admin-dns-timeout-ms` CLI flag and
`CRABKA_GRES_REGISTRY_READER_ADMIN_DNS_TIMEOUT_MS` environment variable. The
Kafka CRD owns optional `spec.gresRegistry.readerAdminDnsTimeoutMs`, whose
checked-in schema has `minimum: 1`.

Absence selects the shared validated 10,000-millisecond default. Each
standalone parser validates explicit input through its existing positive
millisecond type and
`RegistryPolicy::with_reader_admin_dns_timeout_ms`; CLI input takes precedence
over environment input. The CRD follows the same typed override path and
reports invalid input under the exact
`spec.gresRegistry.readerAdminDnsTimeoutMs` owner.

The effective fleet value follows:

```text
Kafka.spec.gresRegistry.readerAdminDnsTimeoutMs
  -> GresRegistrySpec::policy
  -> RegistryPolicy::with_reader_admin_dns_timeout_ms
  -> operator-internal Gres control policy
     / rendered --registry-reader-admin-dns-timeout-ms
  -> standalone RegistryOptions::policy
  -> Registry::connect_with_policy
  -> Registry::ensure_topic / refresh / background reader
```

Operator-internal control construction and both activator and compute rendering
consume the same effective `RegistryPolicy`. Standalone Gres, `crabka gres`,
the Gres activator, and the Gres load-test each own the exact CLI/environment
input; the load-test also forwards one exact flag/value pair to each child.

Within `Registry`, topic creation and its following metadata request construct
`AdminClient` with `policy.reader_admin_dns_timeout()`. Synchronous refresh
uses the same value for both its admin metadata lookup and
`resolve_bootstrap_addr`; the background reader receives a clone of the same
policy and uses that value on every resolution/reconnect cycle. Each ordered,
comma-separated reader lookup gets its own deadline, and the resulting raw
`ConnectionOptions` carries the same typed value.

`AdminClient` now applies its carried `ConnectionOptions.dns_timeout` in
`connect_one` around every `tokio::net::lookup_host`. Initial bootstrap,
controller reconnect, and bootstrap retry all route through `connect_one`, so
the root fix covers topic creation and metadata refresh without caller-local
guards. Its 5-second TCP-connect and 30-second request defaults remain separate
and unchanged.

The registry producer DNS slice remains separately completed and unchanged:
`--registry-producer-dns-timeout-ms`,
`CRABKA_GRES_REGISTRY_PRODUCER_DNS_TIMEOUT_MS`, and
`spec.gresRegistry.producerDnsTimeoutMs` continue through
`RegistryPolicy::producer_dns_timeout()` to the sole registry
`Producer::builder()` construction.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,191 lines across 1,051 files. The exact required focused search

```text
rg -n "lookup_host|ToSocketAddrs|ClientDnsTimeout|dns[_-]timeout|DnsTimeout|registry-reader-admin-dns-timeout|readerAdminDnsTimeoutMs" crates deploy/crds docs/configuration-audit.md
```

reported 436 lines across 42 files: 187 production references across 25 files,
4 checked-in CRD schema references across 2 files, 203 test or harness
references across 31 files, and 42 prior-audit references in this file. These
groups classify the complete focused result, including separately completed
DNS paths and unresolved owners; they do not claim all matches are covered by
this slice.

### Adjacent Pending Policy

This closes only Gres registry reader/admin DNS resolution. The next coherent
unresolved owner is the raw Gres FDW lookup in
`crates/gres-fdw/src/source.rs`: `open_connection` awaits
`tokio::net::lookup_host` without a deadline even though its subsequent
`ConnectionOptions` contains the default typed DNS value. Client Streams,
Raft, Schema Registry, and unrelated producer DNS paths remain separate visible
owners. The repository-wide hardcoded operational-value audit remains active.

### Gres Registry Reader/Admin DNS Timeout Evidence

Implementation-task verification reported:

- Client admin: the exact paused-clock lookup test stopped at 37 milliseconds;
  the custom constructor preserved the standard admin identity and independent
  connect/request defaults; all-target tests and strict Clippy passed.
- Gres control: both focused reader/admin policy and resolver tests passed; the
  full all-target run passed 83 tests; strict Clippy, formatting, and diff
  hygiene passed.
- The four standalone packages: default, zero rejection, environment/CLI
  precedence, and load-test child forwarding tests passed; the full all-target
  run had zero failures and retained one pre-existing ignored live-cluster
  case; strict Clippy and formatting passed. Gres help displayed the exact
  `--registry-reader-admin-dns-timeout-ms` flag.
- Operator: focused CRD, one-field cache replacement, activator, and compute
  rendering tests passed; the final library run passed 721 tests and the
  `reconcile_gres` target passed 20 tests; strict all-target Clippy passed. Two
  fresh nine-file CRD generations matched each other and `deploy/crds`.

This evidence does not close the Gres FDW or other pending DNS owners, and it
does not complete the repository-wide configuration goal.

## Gres FDW Broker DNS Timeout

The Gres FDW broker DNS policy reuses client-core's validated
`ClientDnsTimeout` and its 10,000-millisecond default. Standalone Gres exposes
the exact `--fdw-broker-dns-timeout-ms` CLI flag and
`CRABKA_GRES_FDW_BROKER_DNS_TIMEOUT_MS` environment variable. The Gres CRD
owns optional `spec.compute.fdwBrokerDnsTimeoutMs`, whose checked-in schema has
`minimum: 1`. CLI input takes precedence over environment input, which takes
precedence over the typed default; zero is rejected at the CLI or CRD boundary.

The effective value follows:

```text
Gres.spec.compute.fdwBrokerDnsTimeoutMs
  -> EffectiveGresComputePolicy
  -> rendered --fdw-broker-dns-timeout-ms
  -> ServeArgs
  -> ClientDnsTimeout
  -> KafkaFdw
  -> scan metadata admin connection
     / scan raw broker lookup
     / import metadata admin connection
```

Local and substrate modes resolve the same process policy once when registering
the scanner. `KafkaFdw` carries that value separately from catalog-derived
foreign-server options, so SQL cannot override it. Scan metadata and
`IMPORT FOREIGN SCHEMA` use the secured admin constructor, preserving TLS,
SASL, bootstrap ordering, and the independent 5-second connect and 30-second
request defaults. Raw scans preserve first-address selection while bounding
`lookup_host`; resolver and empty-result errors retain the broker address, and
deadline errors name both the address and configured milliseconds.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,199 lines across 1,052 files. The exact required focused search

```text
rg -n "lookup_host|ToSocketAddrs|ClientDnsTimeout|dns[_-]timeout|DnsTimeout|fdw-broker-dns-timeout|fdwBrokerDnsTimeoutMs" crates deploy/crds docs/configuration-audit.md
```

reported 542 lines across 43 files. Every match was classified by its owning
path and surrounding module: this slice's production flow is in
`client-admin`, `gres-fdw`, `gres`, and the Gres operator; the five
`deploy/crds` matches are checked-in schema; matches under test targets and
`cfg(test)` modules are test or harness evidence; the 59 matches already in
this file are prior audit evidence. Client-core, WAL recovery and producer,
and registry producer and reader/admin matches are completed unrelated DNS
policies. The remaining production resolver matches are unresolved owners,
including Client Streams, Raft, Schema Registry, and general address-binding
sites.

### Adjacent Pending Policy

This closes only Kafka broker DNS resolution owned by the Gres FDW. The next
coherent unresolved DNS owner is Client Streams broker I/O:
`crates/client-streams/src/runtime/io_broker.rs` contains two direct
`tokio::net::lookup_host` calls without an independently audited deadline.
Raft, Schema Registry, and general address-binding sites remain visible
separate owners. Other FDW connection, request, and fetch operational values
also remain open, and the repository-wide hardcoded operational-value goal is
not complete.

### Gres FDW Broker DNS Timeout Evidence

Implementation-task verification reported:

- Client admin: the secured constructor preserved supplied security and the
  standard admin defaults while changing only DNS policy; all-target tests and
  strict Clippy passed.
- Gres FDW: the paused-clock raw resolver test expired at exactly 37
  milliseconds; scan metadata, raw scan resolution, and import metadata consume
  the carried policy; 56 tests and strict Clippy passed.
- Standalone Gres: default, environment, CLI precedence, zero rejection, local
  and substrate acceptance, exact help, and scanner propagation tests passed;
  the all-target suite and strict Clippy passed.
- Operator: schema default, override, minimum, field-specific error,
  pre-I/O rejection, and exact-once single- and two-range rendering tests
  passed; 723 library tests and all integration targets passed; strict Clippy
  passed; all nine generated CRDs were compared and only the expected Gres CRD
  changed.

This evidence closes only the Gres FDW broker DNS slice. Other FDW operational
values and the repository-wide configuration goal remain open.

## Client Streams Broker DNS Timeout

Client Streams reuses the library-level
`crabka_client_core::ClientDnsTimeout`, publicly re-exported as
`crabka_client_streams::ClientDnsTimeout`, with its exact 10,000-millisecond
default. `StreamsApp::builder().broker_dns_timeout(...)` is the public library
surface. The observability demo exposes
`--streams-broker-dns-timeout-ms` and
`CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS`; the `demo-stream` Compose service
passes through that environment variable with a `10000` default. Clap provides
CLI-over-environment precedence, and absence selects
`ClientDnsTimeout::default()`.

The complete live flow is:

```text
--streams-broker-dns-timeout-ms
  / CRABKA_DEMO_STREAMS_BROKER_DNS_TIMEOUT_MS
  / ClientDnsTimeout::default()
  -> StreamsApp::broker_dns_timeout
  -> KafkaStreams::broker_dns_timeout
  -> ALO or EOS broker I/O
     -> metadata Client
     -> raw fetch lookup and ConnectionOptions
     -> Producer
     -> offsets Client
  -> StreamsMembership::broker_dns_timeout
     -> join Client
     -> coordinator/heartbeat Client
```

One typed value therefore covers ALO and EOS metadata, raw fetch, producer,
offsets, join, and heartbeat broker DNS resolution. Both direct production
`tokio::net::lookup_host` calls in `runtime/io_broker.rs` pass through the
shared bounded first-address helper. Deadline errors name the broker and the
configured milliseconds; resolver and empty-result errors preserve broker
context. The demo parses explicit values as `NonZeroU64`, rejects zero at the
CLI or environment boundary, and rejects the option for non-Stream roles
before telemetry or external I/O. `ClientDnsTimeout` retains the shared
positive whole-millisecond validation.

This policy changes DNS resolution only. Bootstrap ordering and first-address
selection remain unchanged, as do TLS/SASL forwarding, the independent TCP
connect and request deadlines, fetch sizing, producer and offset behavior,
ALO/EOS semantics, membership timing and protocol behavior, and Schema
Registry resolution. Defaulted builder fields preserve existing callers.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,203 lines across 1,053 files. The exact focused search

```text
rg -n "lookup_host|ToSocketAddrs|ClientDnsTimeout|dns[_-]timeout|DnsTimeout|streams-broker-dns-timeout|STREAMS_BROKER_DNS_TIMEOUT|broker_dns_timeout" crates/client-streams crates/observability-demo-app demo/observability docs/configuration-audit.md
```

reported 159 lines across 19 files. The exclusive primary classification is
46 Client Streams production references, 25 demo-deployment references
(24 application references and one Compose reference), 20 test or harness
references, and 68 prior-audit references in this file. No focused line remains
as a current unresolved owner: the earlier audit text includes completed
downstream DNS policies and the former unresolved Client Streams owner, but
those lines are classified as prior audit evidence rather than counted twice.
Both production raw lookups are bounded as described above.

Task 1 verification passed the focused raw-lookup, fetch-options, and
builder-default tests, then the Client Streams all-target suite (437 library
tests plus every integration and example target) and strict all-target Clippy.
Task 2 verification passed the demo's two unit tests, two subprocess
configuration tests, one focused Compose test, the full demo all-target suite,
strict all-target Clippy, and the exact single-help-flag check. Formatting and
diff-hygiene gates passed for both tasks.

### Adjacent Pending Policy

This closes only Client Streams broker DNS resolution. The next coherent
Client Streams operational owner is the runtime cadence pair in
`crates/client-streams/src/runtime/app.rs`: the default 200-millisecond
`poll_interval` and 5-second `commit_interval`. They already share one
high-level runtime owner and flow, while membership and protocol timing remain
separate policy work. Other Client Streams operational values and the
repository-wide hardcoded operational-value goal remain open.

## Client Streams Runtime Cadence

Client Streams now represents processing cadence with the public
`StreamsPollInterval` and `StreamsCommitInterval` semantic types. Their exact
defaults remain `DEFAULT_STREAMS_POLL_INTERVAL = 200 ms` and
`DEFAULT_STREAMS_COMMIT_INTERVAL = 5,000 ms`. `StreamsApp` stores them in the
`poll_interval` and `commit_interval` builder fields. The demo exposes
`--streams-poll-interval-ms` and `--streams-commit-interval-ms`, backed by
`CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS` and
`CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS`. Only the `demo-stream` Compose
service passes those variables, with `${CRABKA_DEMO_STREAMS_POLL_INTERVAL_MS:-200}`
and `${CRABKA_DEMO_STREAMS_COMMIT_INTERVAL_MS:-5000}`. Clap provides the exact
CLI over environment precedence, and absence of either source selects the
corresponding typed library default.

The demo parses explicit values as `NonZeroU64`, resolves both typed values
before telemetry or external I/O, and rejects either option for a non-Stream
role at that same early boundary. `StreamsPollInterval::new` and
`StreamsCommitInterval::new` accept only positive, whole-millisecond durations
whose millisecond count fits in `u64`. The demo forwards the resolved types to
the matching `StreamsApp` fields. `StreamsApp::run_built` converts them back to
`Duration` for the compatible low-level `KafkaStreams` builder. That builder
retains its existing `Duration` inputs and defaults, immediately validates both
fields before topology wrapping or broker setup, and gives field-specific
configuration errors. The validated durations then feed the existing
`tokio::time::interval` poll and commit timers. Direct low-level callers remain
source-compatible, and Tokio's immediate first ticks and the surrounding
`select!` ordering are unchanged.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,213 lines across 1,053 files. The exact focused search

```text
rg -n "poll_interval|commit_interval|StreamsPollInterval|StreamsCommitInterval|DEFAULT_STREAMS_(POLL|COMMIT)_INTERVAL|streams-(poll|commit)-interval|STREAMS_(POLL|COMMIT)_INTERVAL" crates/client-streams crates/observability-demo-app demo/observability docs/configuration-audit.md
```

reported 118 lines across 10 files. The exclusive primary classification is
38 Client Streams production references, 23 completed downstream demo-policy
references, two demo-deployment references, 52 test or harness references,
three prior-audit references in this file, and zero current unresolved-owner
references. Thus every focused production and deployment path for the 200-ms
poll interval and 5,000-ms commit interval now enters through the validated
policy described above.

Task 1 verification passed three focused library tests, including defaults,
independent overrides, invalid low-level values, and field-specific errors;
the Client Streams all-target suite (441 library tests plus every integration
and example target); strict all-target Clippy; formatting; and diff-hygiene
gates. Existing minute-long direct `KafkaStreams` `Duration` callers compiled
unchanged. Task 2 verification passed two demo unit tests, two subprocess
configuration tests, one focused Compose test, the complete demo all-target
suite (6 library, 4 binary, 24 configuration, 2 cadence subprocess, and 2 DNS
subprocess tests), strict all-target Clippy, the exact two-help-flag check,
formatting, and diff-hygiene gates. Its follow-up hermetic subprocess test also
passed with a hostile inherited DNS-timeout environment.

## Client Streams Rebalance Timeout

Client Streams now represents the client-provided rebalance timeout with the
public `StreamsRebalanceTimeout` semantic type. It accepts exactly positive,
whole-millisecond durations in `1..=i32::MAX` milliseconds and defaults to
`DEFAULT_STREAMS_REBALANCE_TIMEOUT = 30,000 ms`. `StreamsApp` owns the typed
value. The public `KafkaStreams` and `StreamsMembership` builders retain their
compatible raw-`Duration` inputs and 30-second defaults, but validate them at
their boundaries: `KafkaStreams` before broker construction and
`StreamsMembership` before schema prewarm or broker construction. Invalid
values therefore cannot reach prewarm or broker I/O.

The validated signed millisecond value reaches both the initial join heartbeat
and every subsequent coordinator heartbeat. The demo exposes
`--streams-rebalance-timeout-ms`, backed by
`CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS`, with CLI over environment over the
typed 30,000-ms default precedence. Resolution and validation occur before
telemetry or external I/O. Only the `demo-stream` Compose service receives the
variable, with `${CRABKA_DEMO_STREAMS_REBALANCE_TIMEOUT_MS:-30000}`; Produce
and Consume reject the setting.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,223 lines across 1,053 files. The focused search

```text
rg -n "rebalance_timeout|StreamsRebalanceTimeout|DEFAULT_STREAMS_REBALANCE_TIMEOUT|streams-rebalance-timeout|STREAMS_REBALANCE_TIMEOUT" crates/client-streams crates/observability-demo-app demo/observability docs/configuration-audit.md
```

reported 94 lines across 12 files. The exclusive classification is 33 Client
Streams production references, 13 completed downstream demo-policy references,
one demo-deployment reference, 46 test or harness references, one prior-audit
reference in this file, and zero unresolved-owner references. The categories
sum to all 94 focused lines.

Task 1 verification passed four focused tests, the Client Streams all-target
suite (444 library tests plus every integration and example target), strict
all-target Clippy, formatting, and diff-hygiene gates. Task 2 verification
passed the typed app test, two demo subprocess tests, the Compose ownership
test, the complete demo all-target suite (6 library, 6 binary, 24
configuration, 2 cadence, 2 DNS, and 2 rebalance-timeout subprocess tests),
strict all-target Clippy, the exact single-help-flag check, formatting, and
diff-hygiene gates. Its reviewed follow-up added only the narrow test-local
Clippy allowance already used by Task 1 and passed the focused app test,
combined strict Clippy, nightly formatting, and commit-diff checks. The fresh
combined final run passed 445 Client Streams library tests and every
integration, example, and demo target; strict Clippy, live help, nightly
formatting, and diff-hygiene gates also passed. The help flag appeared exactly
once and `Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the client-provided Client Streams rebalance timeout. The next
scanner-visible operational owner is the fixed 200-millisecond
`COORDINATOR_LOAD_IN_PROGRESS` join retry delay in
`crates/client-streams/src/membership/client.rs`. The broker-provided heartbeat
interval and fixed 3-second invalid-response fallback remain defensive protocol
behavior, are not configuration policy, and are unchanged. Other membership
and protocol timing, other Client Streams operational values, and the
repository-wide hardcoded operational-value goal remain open.

## Client Streams Join Retry Backoff

Client Streams now represents the delay between initial join retries with the
public `StreamsJoinRetryBackoff` semantic type. It accepts exactly positive,
whole-millisecond durations in `1..=u64::MAX` milliseconds and defaults to
`DEFAULT_STREAMS_JOIN_RETRY_BACKOFF = 200 ms`. `StreamsApp` owns the typed
value. The public `KafkaStreams` and `StreamsMembership` builders retain their
compatible raw-`Duration` inputs and 200-millisecond defaults, but validate
them at both low-level boundaries: `KafkaStreams` before topology wrapping or
broker construction, and `StreamsMembership` before schema prewarm or broker
construction.

The exact live flow is:

```text
StreamsApp::join_retry_backoff
  -> KafkaStreams::join_retry_backoff
  -> StreamsMembership::join_retry_backoff
  -> initial-join COORDINATOR_LOAD_IN_PROGRESS response
  -> tokio::time::sleep(configured backoff)
  -> next initial-join request
```

Each `COORDINATOR_LOAD_IN_PROGRESS` response receives the same configured
fixed delay; there is no exponential schedule or jitter. A success or any
other response code does not take this sleep path and retains its existing
mapping. The numeric response code, initial join request, retry ordering,
broker-provided heartbeat interval, fixed invalid-heartbeat fallback,
subsequent coordinator heartbeat behavior, rebalance timeout, and all other
protocol behavior are unchanged.

The observability demo exposes `--streams-join-retry-backoff-ms`, backed by
`CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS`, with CLI over environment over
the typed 200-millisecond default precedence. Explicit values parse as
`NonZeroU64` and pass through the public type's validation. Resolution rejects
the setting for Produce and Consume before telemetry or external I/O. Only the
`demo-stream` Compose service receives the variable, with
`${CRABKA_DEMO_STREAMS_JOIN_RETRY_BACKOFF_MS:-200}`. There is no CRD because
this slice configures the standalone demo Stream process, not an
operator-managed resource.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,233 lines across 1,053 files. The focused search

```text
rg -n "join_retry_backoff|StreamsJoinRetryBackoff|DEFAULT_STREAMS_JOIN_RETRY_BACKOFF|streams-join-retry-backoff|STREAMS_JOIN_RETRY_BACKOFF|COORDINATOR_LOAD_IN_PROGRESS" crates/client-streams crates/observability-demo-app demo/observability docs/configuration-audit.md
```

reported 85 lines across 10 files. The exclusive classification is 31 Client
Streams production references, 13 demo-policy references, one demo-deployment
reference, 39 test or harness references, one prior-audit reference in this
file, and zero unresolved-owner references. The categories sum to all 85
focused lines.

Task 1 verification passed five focused join-retry tests, the focused
low-level validation test, the Client Streams all-target suite (450 unit tests
plus every integration and example target), strict all-target Clippy, nightly
formatting, and diff-hygiene gates. Task 2 verification passed two subprocess
configuration tests, one focused Compose ownership test, the complete demo
all-target suite (45 tests), strict all-target Clippy, the live help check,
nightly formatting, and diff-hygiene gates. The fresh combined final run
passed the Client Streams and demo all-target suites, strict combined Clippy,
the exact single-help-flag check, nightly formatting, and diff-hygiene gates.
`Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the Client Streams initial-join retry delay. The next
scanner-visible operational owner with actual production consumers is the
supervisor interactive-query queue capacity in
`crates/client-streams/src/runtime/app.rs`: separate v1 and v2 channels each
have a fixed capacity of 64. `KafkaStreams` store accessors and `query` enqueue
requests through those senders, and the supervisor `select!` drains both
receivers into `StreamThread`. Protocol identifiers, wire constants, topology
names, test values, and the broker-provided heartbeat fallback remain
invariants rather than configuration. Other Client Streams operational values
and the repository-wide hardcoded operational-value goal remain open.

## Client Streams Interactive Query Queue Capacity

Client Streams now represents the capacity shared by its v1 and v2
interactive-query request queues with the public
`StreamsInteractiveQueryQueueCapacity` semantic type. It accepts capacities in
`1..=tokio::sync::Semaphore::MAX_PERMITS` through
`MinMaxUsize`, rejects values outside that Tokio-supported range before
channel construction, and defaults to the public
`DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY` value of `64`.
Both public builders own a defaulted typed value: `StreamsApp` carries it
through `run_built`, and `KafkaStreams` converts the one validated setting into
the two equal bounded-channel capacities.

The exact live flow is:

```text
StreamsApp::interactive_query_queue_capacity
  -> KafkaStreams::interactive_query_queue_capacity
  -> interactive_query_queue_capacities
  -> mpsc::channel::<IqRequest>(capacity)
     <- key-value, window, and session store requests
     -> supervisor iq_rx branch
     -> StreamThread::serve_iq
  -> mpsc::channel::<Iq2Request>(capacity)
     <- KafkaStreams::query
     -> supervisor iq2_rx branch
     -> StreamThread::serve_iq2
```

The queues remain independently bounded and retain Tokio `send` backpressure.
Their sender cloning, request and oneshot-response types, validation,
dispatch, supervisor `select!` branches, shutdown behavior, and v1/v2 response
handling are unchanged. The separate 16-entry channel in
`runtime/iq_view.rs` remains a test-only servicer channel and is not part of
this production policy.

The observability demo exposes
`--streams-interactive-query-queue-capacity`, backed by
`CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY`. Clap provides CLI over
environment precedence; absence selects the typed library default. Explicit
values parse as `NonZeroUsize`, and the demo resolves the type and rejects the
setting for Produce and Consume before telemetry or external I/O. Only the
`demo-stream` Compose service receives the variable, with
`${CRABKA_DEMO_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY:-64}`. There is no CRD
because this setting belongs to the standalone observability demo Stream
process, not an operator-managed resource.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,233 lines across 1,053 files. The exact focused search

```text
rg -n \
  "interactive_query_queue_capacity|StreamsInteractiveQueryQueueCapacity|DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY|streams-interactive-query-queue-capacity|STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY|mpsc::channel::<Iq(Request|2Request)>\(64\)" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

reported 65 lines across eight files. The exclusive classification is
17 Client Streams production references, 15 demo-policy references, one
demo-deployment reference, 32 test or harness references, zero prior-audit
references, and zero unresolved-owner references. The categories sum to all
65 focused lines.

Task 1 verification passed three focused tests plus the plural-named shared
capacity test in the full package run, the Client Streams all-target suite
(454 library tests plus every integration and example target), strict
all-target Clippy, nightly formatting, and diff-hygiene gates. Task 2
verification passed two subprocess configuration tests, one focused Compose
ownership test, the complete demo all-target suite (48 tests), strict
all-target Clippy, the exact single-help-flag check, nightly formatting, and
diff-hygiene gates. `Cargo.lock` remained unchanged in both tasks.

The fresh combined final run passed the 454-test Client Streams library target,
every Client Streams integration and example target, and all 48 demo tests.
Strict combined Clippy, the exact single-help-flag check, nightly formatting,
and diff-hygiene gates also passed. `Cargo.lock` remained unchanged.

Final review aligned the public domain with Tokio's actual bounded-channel
limit. A focused constructor-only boundary test proves
`tokio::sync::Semaphore::MAX_PERMITS` is accepted and one greater is rejected
before either MPSC channel can be constructed.

### Adjacent Pending Policy

This closes only the Client Streams interactive-query queue capacity. The next
scanner-visible operational owner with an actual production consumer is the
state-store record-cache byte budget. Its current `10_485_760`-byte (10 MiB)
default is owned by `StreamsApp`, passed through `KafkaStreams` into
`StreamThread`, and supplied to each task topology instantiation, where it
constructs the cache budget used by eligible materialized state stores.
Protocol identifiers, wire-format sizes, algorithm constants, topology names,
test values, the test-only 16-entry query channel, and the broker-derived
heartbeat fallback remain invariants rather than configuration. Other Client
Streams operational values and the repository-wide hardcoded-operational-value
goal remain open.

## Client Streams State-Store Cache Budget

Client Streams now represents its state-store record-cache byte budget with the
public `StreamsStateStoreCacheMaxBytes` semantic type. It accepts
`0..=MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES`, where the maximum is the largest
`i64` representable by the target's internal `usize` accounting (`i64::MAX` on
64-bit targets), and defaults to
`DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES = 10_485_760` bytes (10 MiB).
Zero retains its existing meaning: caching is disabled and eligible stores
emit on update.

The public `StreamsApp::cache_max_bytes(i64)` and
`KafkaStreams::cache_max_bytes(i64)` builder setters remain raw and
source-compatible. `KafkaStreams::start` is the single validation boundary:
it validates the raw value before broker lookup, broker construction, or
supervisor spawn, then recovers the validated raw bytes for the unchanged
downstream interfaces. The exact live flow is:

```text
StreamsApp::cache_max_bytes
  -> KafkaStreams::cache_max_bytes
  -> KafkaStreams::start validation
  -> StreamThread::new(cache_max_bytes)
  -> BuiltTopology::instantiate(cache_max_bytes)
  -> wire_record_caches
  -> ThreadCache::new(cache_max_bytes)
```

Cache accounting, eviction, flushing, per-task allocation, materialized-store
eligibility, and the downstream raw `i64` flow are unchanged.

The observability demo exposes
`--streams-state-store-cache-max-bytes`, backed by
`CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES`. Clap provides CLI over
environment precedence; absence selects the typed 10 MiB library default.
Resolution validates the value and rejects the setting for Produce and Consume
before telemetry or external I/O. Zero is accepted as the explicit disable
value. Only the `demo-stream` Compose service receives the variable, with
`${CRABKA_DEMO_STREAMS_STATE_STORE_CACHE_MAX_BYTES:-10485760}`. There is no CRD
because this setting belongs to the standalone observability demo Stream
process, not an operator-managed resource.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,239 lines across 1,053 files. The exact focused search

```text
rg -n \
  "cache_max_bytes|StreamsStateStoreCacheMaxBytes|DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES|MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES|streams-state-store-cache-max-bytes|STREAMS_STATE_STORE_CACHE_MAX_BYTES|statestore.cache.max.bytes" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

reported 129 lines across 17 files. The exclusive classification is 58 Client
Streams production references, 14 demo-policy references, one demo-deployment
reference, 56 test or harness references, zero prior-audit references, and zero
unresolved-owner references. The categories sum to all 129 focused lines.

Task 1 verification passed five focused state-store-cache tests, the focused
shared low-level validation test, the Client Streams all-target suite (460
library tests plus every integration and example target), strict all-target
Clippy, nightly formatting, and diff-hygiene gates. Task 2 verification passed
two subprocess configuration tests, one focused Compose ownership test, the
complete demo all-target suite (50 tests), strict all-target Clippy, the exact
single-help-flag check, nightly formatting, and diff-hygiene gates.
`Cargo.lock` remained unchanged in both tasks.

The fresh combined final run passed the Client Streams and demo all-target
suites, strict combined Clippy, nightly formatting, and diff-hygiene gates.
The prescribed `./target/debug/observability-demo-app --help` flag count
returned `0` because that pre-existing binary was stale. The freshly built
task-specific binary at
`/tmp/configuration_expose_task3_target_20260726.lf2J58/debug/observability-demo-app`
returned the required flag count of `1`. `Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the Client Streams state-store record-cache byte budget. The
next scanner-visible operational owner with an actual production consumer is
the fixed five-second shutdown leave-heartbeat deadline in
`crates/client-streams/src/membership/coordinator.rs`: after the coordinator
loop observes shutdown, `run` bounds its final `member_epoch = -1` heartbeat
with that timeout. Error codes, wire constants, topology names, test values,
the broker-provided heartbeat interval, and its invalid-response fallback
remain protocol or test invariants rather than configuration policy. Other
Client Streams operational values and the repository-wide hardcoded
operational-value goal remain open.

## Client Streams Leave-Heartbeat Timeout

Client Streams now represents the deadline for its final shutdown heartbeat
with the public `StreamsLeaveHeartbeatTimeout` semantic type. It accepts
positive, whole-millisecond durations in `1..=u64::MAX` milliseconds and
defaults to exactly `DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT = 5,000 ms`.
Zero, fractional milliseconds, and durations above the representable
millisecond range are rejected; zero does not disable the leave attempt.

The public `StreamsMembership` and `KafkaStreams` builders retain compatible
raw-`Duration` setters and five-second defaults. Direct
`StreamsMembership::start` validates after the group-id check and before schema
prewarming or broker lookup. `KafkaStreams::start` validates with the other
runtime settings before topology wrapping or broker construction.
`StreamsApp` owns the typed setting and forwards its duration through the exact
live path:

```text
StreamsApp
  -> KafkaStreams
  -> StreamsMembership
  -> CoordinatorState
  -> tokio::time::timeout(configured timeout, final heartbeat)
```

The final request still carries `member_epoch = -1`. It remains one
best-effort attempt: timeout, transport error, and broker error are ignored,
and shutdown continues when the configured deadline expires.

The observability demo exposes
`--streams-leave-heartbeat-timeout-ms`, backed by
`CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS`. Clap provides CLI over
environment precedence; absence selects the typed five-second default.
Explicit values parse as `NonZeroU64` and pass through the semantic type.
Resolution rejects the setting for Produce and Consume before telemetry or
external I/O. Only the `demo-stream` Compose service receives the variable,
with `${CRABKA_DEMO_STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS:-5000}`. There is no
CRD because this setting belongs to the standalone observability demo Stream
process, not an operator-managed resource.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,249 lines across 1,053 files. The exact focused search

```text
rg -n \
  "leave_heartbeat_timeout|StreamsLeaveHeartbeatTimeout|DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT|streams-leave-heartbeat-timeout-ms|STREAMS_LEAVE_HEARTBEAT_TIMEOUT_MS|member_epoch: -1" \
  crates/client-streams \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

reported 90 lines across 11 files. The exclusive classification is 33 Client
Streams production references, 14 demo-policy references, one demo-deployment
reference, 42 test or harness references, zero prior-audit references, and
zero unresolved-owner references. The categories sum to all 90 focused lines.

Task 1 verification passed five focused leave-timeout tests, the focused
configured coordinator deadline test, the focused low-level validation test,
the Client Streams all-target suite (466 unit tests plus every integration and
example target), strict all-target Clippy, nightly formatting, and
diff-hygiene gates. Task 2 verification passed two subprocess configuration
tests, one focused Compose ownership test, the complete demo all-target suite
(52 tests), strict all-target Clippy, the exact single-help-flag check, nightly
formatting, and diff-hygiene gates. `Cargo.lock` remained unchanged in both
tasks.

The fresh combined final run passed 466 Client Streams library tests, every
Client Streams integration and example target, and all 52 demo tests. Strict
combined Clippy, the exact single-help-flag check, nightly formatting, and
diff-hygiene gates also passed. The help flag appeared exactly once and
`Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the Client Streams final leave-heartbeat deadline. The next
scanner-visible operational owner with real production consumers is the fixed
five-second Client Consumer leave-group deadline: startup cleanup in
`crates/client-consumer/src/consumer.rs` and coordinator shutdown in
`crates/client-consumer/src/coordinator.rs` both bound best-effort sends with
that value. The separate fixed five-second Share Consumer leave-heartbeat
deadline in `crates/client-consumer/src/share/coordinator.rs` also remains
pending and is not part of that owner. Other Client Streams operational
values, both consumer leave policies, and the repository-wide hardcoded
operational-value goal remain open.

## Client Consumer Leave-Group Timeout

The classic Client Consumer now represents its best-effort leave-group
deadline with the public `ConsumerLeaveGroupTimeout` semantic type. It accepts
positive, whole-millisecond durations in `1..=u64::MAX` milliseconds and
defaults to exactly `DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT = 5,000 ms`. Zero,
fractional milliseconds, and durations above the representable millisecond
range are rejected; zero does not disable either leave attempt.

The public `Consumer::builder().leave_group_timeout(Duration)` setter remains a
raw duration input with the five-second default. `Consumer::start` validates it
after the existing local argument checks and before the startup retry loop or
network I/O, then carries the validated duration through the exact live flow:

```text
Consumer::start
  -> StartConfig
     -> failed-startup cleanup
        -> leave_startup_member
        -> tokio::time::timeout(configured timeout, LeaveGroup)
     -> successful startup
        -> CoordinatorState
        -> coordinator shutdown
        -> leave_group
        -> tokio::time::timeout(configured timeout, LeaveGroup)
```

Both paths retain one best-effort request. Request construction, coordinator
routing, and member identity are unchanged; timeout, transport, and broker
errors remain ignored so shutdown or startup error propagation continues after
the configured deadline.

The observability demo exposes `--consumer-leave-group-timeout-ms`, backed by
`CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS`. Clap provides CLI over
environment precedence; absence selects the typed five-second default.
Explicit values parse as `NonZeroU64` and pass through the semantic type.
Resolution rejects the setting for Produce and Stream before telemetry or
external I/O. Only the `demo-consume` Compose service receives the variable,
with `${CRABKA_DEMO_CONSUMER_LEAVE_GROUP_TIMEOUT_MS:-5000}`. There is no CRD
because the operator does not own or render this standalone demo Consumer.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,262 lines across 1,053 files. The exact focused search

```text
rg -n \
  "leave_group_timeout|ConsumerLeaveGroupTimeout|DEFAULT_CONSUMER_LEAVE_GROUP_TIMEOUT|consumer-leave-group-timeout-ms|CONSUMER_LEAVE_GROUP_TIMEOUT_MS|leave_startup_member|leave_group\(" \
  crates/client-consumer \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

reported 68 lines across seven files. The exclusive classification is 23
classic Client Consumer production references, zero ShareConsumer production
references, 13 demo-policy references, one demo-deployment reference, 31 test
or harness references, zero prior-audit references, and zero unresolved-owner
references. The categories sum to all 68 focused lines.

Task 1 verification passed three focused semantic/builder tests, two startup
cleanup tests, the configured coordinator-shutdown test, the
`crabka-client-consumer` all-target suite (139 unit tests plus all three
integration tests), strict all-target Clippy, nightly formatting, and
diff-hygiene gates. Adding the existing workspace `refined_type` dependency
required and received approval for the one-line `Cargo.lock` package-dependency
update.

Task 2 verification passed two subprocess configuration tests, one focused
Compose ownership test, the complete demo all-target suite (55 tests), strict
all-target Clippy, the exact single-help-flag check, nightly formatting,
lockfile stability, and diff-hygiene gates.

The fresh combined final run passed 139 Client Consumer unit tests, all three
Client Consumer integration tests, and all 55 demo tests. Strict combined
Clippy, the exact single-help-flag check, nightly formatting, lockfile
stability, and diff-hygiene gates also passed.

### Adjacent Pending Policy

This closes only the classic Client Consumer leave-group deadline. The next
scanner-visible operational owner with an actual production consumer is the
separate fixed five-second ShareConsumer final leave-heartbeat deadline in
`crates/client-consumer/src/share/coordinator.rs`: after its coordinator loop
observes shutdown, `run` bounds one best-effort `member_epoch = -1` heartbeat
with that timeout. Protocol error codes, wire fields, test values, and the
broker-driven heartbeat interval remain invariants rather than this
configuration policy. The ShareConsumer deadline and the repository-wide
hardcoded-operational-value goal remain open.

## ShareConsumer Leave-Heartbeat Timeout

ShareConsumer now represents the deadline for its final shutdown heartbeat
with the public `ShareConsumerLeaveHeartbeatTimeout` semantic type. It accepts
positive whole milliseconds through `u64::MAX` and defaults to exactly
`DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT = 5,000 ms`. Zero, fractional
milliseconds, and larger durations are rejected.

`ShareConsumer::builder()` accepts a raw `Duration` and validates it before
`Client` construction or network I/O. The validated duration flows through the
exact live path:

```text
ShareConsumer::start
  -> ShareCoordinatorState
  -> coordinator observes shutdown
  -> leave_group
  -> tokio::time::timeout(configured timeout, final heartbeat)
```

Shutdown preserves the final acknowledgement flush, then coordinator
cancellation and join. The coordinator sends exactly one best-effort
`member_epoch = -1` heartbeat. Timeout, transport, and broker errors remain
ignored, with no retry or disable switch.

There is no CLI, environment variable, demo service, CRD, or operator field
because no production process owns ShareConsumer.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,270 lines across 1,053 files. The exact focused search

```text
rg -n \
  "leave_heartbeat_timeout|ShareConsumerLeaveHeartbeatTimeout|DEFAULT_SHARE_CONSUMER_LEAVE_HEARTBEAT_TIMEOUT|build_leave_heartbeat_request|member_epoch: -1" \
  crates/client-consumer \
  docs/configuration-audit.md
```

reported 36 lines across six files. The exclusive classification is 19
ShareConsumer production references, one classic Consumer production
reference, 15 test or harness references, one prior-audit reference, and zero
unresolved-owner references. The categories sum to all 36 focused lines.

Focused tests, the `crabka-client-consumer` all-target tests, strict Clippy,
nightly formatting, and diff hygiene passed. `Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the ShareConsumer final leave-heartbeat deadline. The next
scanner-visible operational owner is the ShareConsumer poll-fetch limits in
`crates/client-consumer/src/share/poll.rs`: `MAX_BYTES = 52_428_800`,
`PARTITION_MAX_BYTES = 1_048_576`, `MAX_RECORDS = 500`, and request
`min_bytes = 1`. Other ShareConsumer operational values and the
repository-wide hardcoded-operational-value goal remain open.

## ShareConsumer Fetch Limits

ShareConsumer now represents its fetch limits with the public
`ShareConsumerFetchMinBytes`, `ShareConsumerFetchMaxBytes`, and
`ShareConsumerFetchMaxRecords` semantic types. All three accept values from 1
through `i32::MAX`. Defaults remain exactly 1 minimum byte, 52,428,800 maximum
bytes, and 500 maximum records. Zero and negative values are rejected, as is a
minimum greater than the maximum.

`ShareConsumer::builder()` accepts raw `i32` values and validates them before
`Client` construction or network I/O. The validated values are stored on
`ShareConsumer` and used by every poll through this exact flow:

```text
ShareConsumer::start
  -> validated ShareConsumer fetch fields
  -> ShareConsumer::poll
  -> build_share_fetch_request
     -> min_bytes
     -> max_bytes
     -> max_records
     -> batch_size = max_records
```

Both `max_records` and `batch_size` receive the configured maximum-record
value. `poll(timeout)` remains the sole `max_wait_ms` control.
`PARTITION_MAX_BYTES` was deleted because supported ShareFetch versions 1 and
2 do not encode the version-0-only field. `FetchPartition::partition_max_bytes`
remains at its generated zero default.

There is no CLI, environment variable, demo service, CRD, or operator field
because no production process owns ShareConsumer. `Cargo.lock` remained
unchanged.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,271 lines across 1,053 files. The exact focused search

```text
rg -n \
  "fetch_min_bytes|fetch_max_bytes|fetch_max_records|ShareConsumerFetch(MinBytes|MaxBytes|MaxRecords)|DEFAULT_SHARE_CONSUMER_FETCH_(MIN_BYTES|MAX_BYTES|MAX_RECORDS)|PARTITION_MAX_BYTES|batch_size" \
  crates/client-consumer \
  docs/configuration-audit.md
```

reported 79 lines across seven files. The exclusive classification is 40
ShareConsumer production references, 11 classic Consumer production
references, 25 test or harness references, three prior-audit references, and
zero unresolved-owner references. The categories sum to all 79 focused lines.

Focused tests, the `crabka-client-consumer` all-target tests, strict Clippy,
nightly formatting, and diff hygiene passed. `Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the ShareConsumer fetch limits. The next scanner-visible
operational owner is `share_acquire_mode: 0` in
`crates/client-consumer/src/share/poll.rs`. It is a supported ShareFetch policy
field and needs its own design decision; it is not folded into this slice, and
the repository-wide hardcoded-operational-value goal remains open.

## Client Consumer Subscription Metadata Refresh Interval

The classic Client Consumer now represents its subscribed-topic metadata
refresh cadence with the public
`ConsumerSubscriptionMetadataRefreshInterval` semantic type. It accepts
positive, whole-millisecond durations in `1..=u64::MAX` milliseconds and
defaults to exactly
`DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL = 5,000 ms`. Zero,
fractional milliseconds, and durations above the representable millisecond
range are rejected.

The public
`Consumer::builder().subscription_metadata_refresh_interval(Duration)` setter
remains a raw duration input. `Consumer::start` validates it after the existing
local argument checks and before the startup retry loop or network I/O, then
carries the validated duration through the exact live flow:

```text
Consumer::start
  -> StartConfig
  -> start_once
  -> CoordinatorState
  -> run
```

The coordinator refreshes when elapsed time is greater than or equal to the
configured interval. Checks run on heartbeat wakeups, so the effective refresh
can occur up to one heartbeat interval after the threshold. Existing semantics
are unchanged: no refresh occurs while a rejoin is already pending, metadata
errors remain best-effort and retry on a later wakeup, only subscribed-topic
partition growth triggers a rejoin, and the baseline advances monotonically
from metadata actually used by a successful rejoin.

Before this section was appended, `tools/audit-runtime-values.sh` reported
6,280 lines across 1,053 files. The exact focused search

```text
rg -n \
  "SUBSCRIPTION_METADATA_REFRESH|subscription_metadata_refresh|ConsumerSubscriptionMetadataRefreshInterval|DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL|subscribed_partition_counts|ShareAcquireMode|BatchOptimized" \
  crates/client-consumer \
  crates/integration-tests/tests/consumer_integration.rs \
  crates/observability-demo-app \
  docs/configuration-audit.md
```

reported 38 lines across four files. The mutually exclusive classification is
23 classic Consumer production references, zero ShareConsumer production
references, 15 integration-test references (one focused integration-test line
and 14 colocated unit-test lines), zero observability-demo owner references,
zero prior-audit references, zero parked acquisition-mode references, and zero
unresolved-owner references. The categories sum to all 38 focused lines.

Task 1 verification passed all 150 `crabka-client-consumer` unit tests and its
three integration tests. The focused
`cold_start_rejoins_when_subscribed_topic_appears` run passed one test with
eight filtered out. Strict Clippy for both targets, nightly formatting, and
diff hygiene passed. `Cargo.lock` remained unchanged.

This library slice adds no CLI, environment variable, or CRD. The
`observability-demo-app` is the first production configuration owner to
propagate next, using only its Consume role and `demo-consume` service, with no
CRD. The separately queued ShareAcquireMode slice retains the approved
`ShareAcquireMode::BatchOptimized` default.

### Adjacent Pending Policy

This closes only the classic Client Consumer library configuration slice.
Propagation through the observability demo remains open, as does the broader
repository-wide hardcoded-operational-value objective. The separately queued
ShareAcquireMode policy slice also remains open with its approved
`BatchOptimized` default.

## Observability Demo Consumer Metadata Refresh

The observability demo exposes the classic Consumer subscribed-topic metadata
refresh cadence as
`--consumer-subscription-metadata-refresh-interval-ms` and
`CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS`. The setting
has CLI-over-environment-over-typed-default precedence: when both explicit
inputs are absent, the resolver uses the typed
`ConsumerSubscriptionMetadataRefreshInterval::default()`. Inputs must be
positive whole milliseconds, and the default remains exactly `5_000`
milliseconds.

The role check and typed construction happen before telemetry initialization or
external I/O. Explicit values are valid only for the Consume role. The exact
data flow is:

```text
Cli
  -> effective_consumer_subscription_metadata_refresh_interval
  -> main
  -> run_consume
  -> Consumer::builder().subscription_metadata_refresh_interval(...)
```

Compose owns the setting only on `demo-consume`, through:

```text
CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS: "${CRABKA_DEMO_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS:-5000}"
```

No CRD or operator field exists because the operator does not own this
standalone Compose demo process. The client-consumer library API, validation,
default, refresh scheduling, and error behavior are unchanged; this slice only
propagates the existing typed setting through the demo's Consume role.

Before this section was appended, the exact scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,280 lines across 1,053 files. The exact focused search

```text
rg -n \
  "consumer_subscription_metadata_refresh|ConsumerSubscriptionMetadataRefreshInterval|DEFAULT_CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL|consumer-subscription-metadata-refresh-interval-ms|CONSUMER_SUBSCRIPTION_METADATA_REFRESH_INTERVAL_MS|ShareAcquireMode|BatchOptimized" \
  crates/client-consumer \
  crates/integration-tests/tests/consumer_integration.rs \
  crates/observability-demo-app \
  demo/observability \
  docs/configuration-audit.md
```

reported 56 lines. The exclusive classification is nine classic Consumer
production references, 15 demo production-policy references, one demo
deployment reference, 24 test or harness references, three prior-audit
references, four parked acquisition-mode-policy references, and zero
unresolved-owner references. The categories sum to all 56 focused lines:
`9 + 15 + 1 + 24 + 3 + 4 + 0 = 56`.

Task 1 verification passed its typed resolver test, both hermetic subprocess
tests, and its Compose ownership test. All 59 observability-demo package tests
passed, as did strict Clippy, nightly formatting, the demo binary build, the
single-help-entry check, and diff hygiene. `Cargo.lock` remained unchanged.

### Adjacent Pending Policy

This closes only the observability-demo owner of the classic Consumer
subscription metadata refresh interval. Other classic Consumer production
owners and the repository-wide hardcoded-operational-value objective remain
open. The separately queued `ShareAcquireMode` policy slice also remains open
with its approved `ShareAcquireMode::BatchOptimized` default.

## Share Consumer Acquire Mode

`ShareAcquireMode` now exposes the ShareFetch acquisition policy on
`ShareConsumer::builder().acquire_mode`. `BatchOptimized` remains the default
and maps to wire value `0`; `RecordLimit` maps to wire value `1`. The exact
live flow is:

```text
ShareConsumer::builder().acquire_mode
  -> ShareConsumer::acquire_mode
  -> ShareConsumer::poll
  -> build_share_fetch_request
  -> ShareFetchRequest::share_acquire_mode
```

ShareFetch version 1 compatibility is unchanged: the generated encoder already
omits `share_acquire_mode` before version 2. No `refined_type` or string parser
was added because the public enum admits only the two supported policies. No
CLI, environment setting, CRD, or operator field was added because no
production process owns `ShareConsumer`.

Before this section was appended, the exact scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,280 lines across 1,053 files. The exact focused search is:

```text
rg -n \
  "share_acquire_mode|ShareAcquireMode|BatchOptimized|RecordLimit|acquire_mode" \
  crates/client-consumer \
  crates/integration-tests/tests/consumer_share_consumer.rs \
  docs/configuration-audit.md
```

Before this section was appended, it reported 37 lines. After this section and
formatting, it reported 45 lines. The final mutually exclusive classification
is 16 production-policy-flow references, 12 test or harness references, 17
prior-audit references, and zero unresolved-owner references:
`16 + 12 + 17 + 0 = 45`.

The authoritative workspace verification at `90d68c80`

```text
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace --all-targets --locked
```

exited successfully. The producer package passed all 101 tests, including
`producer_builder_uses_configured_init_retry_timeout`, and
`crossrange_2pc_nemesis` passed all three tests.

Two intermediate workspace failures received scoped fixes. The producer test's
one-millisecond retry deadline could expire before its local mock response was
processed under load; only its test timings changed, from 1 ms to 100 ms for
the configured retry timeout and from 200 ms to 500 ms for the outer guard.
Its assertions and producer runtime behavior are unchanged. The generic Gres
`ProcessHarness` checkpoint frame changed from `1` to `u64::MAX`, removing
accidental periodic pruning from unrelated range tests while preserving the
manual and dedicated checkpoint tests.

`cargo clippy --workspace --all-targets --locked` exited successfully with only
the pre-existing `large_futures` warning at
`crates/traces/src/bin/crabka-traces.rs:170`; the new ShareFetch documentation
warnings are gone. Nightly formatting, `git diff --check`, and the `Cargo.lock`
diff check all passed.

### Adjacent Pending Policy

This closes only the ShareConsumer acquisition mode. The next scanner-visible
operational owner is the admin UI mutation JSON body limit,
`MUTATION_JSON_BODY_LIMIT_BYTES = 1,048,576`, in
`crates/admin-ui/src/server.rs`. It is a production security and resource-usage
policy applied to every authenticated mutation request, remains a fixed
constant rather than an `AdminUiConfig` value, and is not a protocol constant,
sentinel, test fixture, ignored argument, or already-configured value.

## Admin UI Mutation JSON Body Limit

The admin UI mutation JSON body limit is now a positive
`MutationJsonBodyLimitBytes` stored in `AdminUiConfig`. It retains the
1,048,576-byte default and is resolved from:

```text
--mutation-json-body-limit-bytes
CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and platform-overflow values before server startup.
`MutationJsonBodyLimitBytes` uses `refined_type::rule::GreaterUsize<0>` for the
positive-value invariant.

The exact runtime flow is:

```text
AdminUiRuntimeArgs
  -> AdminUiConfig::mutation_json_body_limit_bytes
  -> AppState
  -> read_mutation_json
  -> axum::body::to_bytes
```

Every mutation route continues to authenticate before decoding its request
body. Oversized authenticated bodies still return HTTP 413, and malformed JSON
within the configured limit still returns HTTP 400. No CRD or operator field
was added because no checked-in Kubernetes owner deploys the standalone admin
UI process.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,280 lines across 1,054 files. Its admin UI subset contained 14
lines: one configured body-limit default, 11 invariant permission-bit
assignments, one invariant session-cookie name, and one structural sidebar-link
table. The categories sum to all 14 lines, with zero unresolved scanner-visible
admin UI owners.

Before this section was appended, the exact focused search

```text
rg -n \
  "mutation_json_body_limit_bytes|MutationJsonBodyLimitBytes|DEFAULT_MUTATION_JSON_BODY_LIMIT_BYTES|mutation-json-body-limit-bytes|CRABKA_ADMIN_UI_MUTATION_JSON_BODY_LIMIT_BYTES" \
  crates/admin-ui \
  docs/configuration-audit.md
```

reported 26 lines. The mutually exclusive classification is 14 production
configuration-flow references, 12 test references, and zero unresolved-owner
references: `14 + 12 + 0 = 26`.

The package's 116 tests passed, including typed boundary and hermetic
CLI-over-environment precedence tests, the shared-limit mutation-route test,
and the authentication-before-decoding test. Strict package Clippy, nightly
formatting, the single-help-entry check, diff hygiene, and the exact one-line
`Cargo.lock` direct-dependency change all passed.

### Adjacent Pending Policy

This closes only the admin UI mutation JSON body limit. The next admin UI
operational owner is the eight-hour `session_ttl_seconds` default, which is
stored in `AdminUiConfig` but has no CLI or environment input. The three
30,000-millisecond broker-admin request timeouts in `server_fns.rs` also remain
fixed operational policy for a later slice.

## Admin UI Session TTL

The admin UI session lifetime is now a positive, platform-representable
`SessionTtlSeconds` stored in `AdminUiConfig`. It retains the 28,800-second
default and is resolved from:

```text
--session-ttl-seconds
CRABKA_ADMIN_UI_SESSION_TTL_SECONDS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and values that cannot be added to the platform monotonic
clock before server startup. `SessionTtlSeconds` uses
`refined_type::rule::GreaterU64<0>` for positivity and
`Instant::checked_add` for representability.

The exact runtime flow is:

```text
AdminUiRuntimeArgs
  -> AdminUiConfig::session_ttl
  -> AppState::new
  -> SessionStore::new
  -> SessionRecord::expires_at
```

Session creation, lookup, expiration, logout, credentials storage, cookie
handling, and the lower-level `SessionStore` handling of zero and oversized
durations are unchanged. No CRD or operator field was added because no
checked-in Kubernetes owner deploys the standalone admin UI process.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,281 lines across 1,054 files. Its admin UI subset contained 15
lines: two configured defaults, 11 invariant permission-bit assignments, one
invariant session-cookie name, and one structural sidebar-link table. The
categories sum to all 15 lines, with zero unresolved scanner-visible admin UI
owners.

Before this section was appended, the exact focused search

```text
rg -n \
  "session_ttl|SessionTtlSeconds|DEFAULT_SESSION_TTL_SECONDS|session-ttl-seconds|CRABKA_ADMIN_UI_SESSION_TTL_SECONDS" \
  crates/admin-ui \
  docs/configuration-audit.md
```

reported 35 lines. The mutually exclusive classification is 16 production
configuration-flow references, 18 test references, one prior-audit reference,
and zero unresolved-owner references: `16 + 18 + 1 + 0 = 35`.

The package's 121 tests passed, including typed default, accepted minimum,
invalid-value rejection, hermetic CLI-over-environment precedence,
`AppState` propagation, immediate-expiry, and oversized-duration no-panic
tests. Strict package Clippy, nightly formatting, the single-help-entry check,
diff hygiene, and the unchanged `Cargo.lock` check all passed.

### Adjacent Pending Policy

This closes only the admin UI session TTL. The next admin UI operational owner
is the 30,000-millisecond broker-admin request timeout repeated in the topic
creation, deletion, and partition-expansion calls in `server_fns.rs`.

## Admin UI Topic Mutation Timeout

The admin UI topic-mutation timeout is now a positive
`TopicMutationTimeoutMs` stored in `AdminUiConfig`. It retains the
30,000-millisecond default and is resolved from:

```text
--topic-mutation-timeout-ms
CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and `i32` overflow values before server startup.
`TopicMutationTimeoutMs` uses `refined_type::rule::GreaterI32<0>` for the
positive-value invariant.

The exact runtime flow is:

```text
AdminUiRuntimeArgs
  -> AdminUiConfig::topic_mutation_timeout_ms
  -> BrokerAdminMutationSeam
  -> AdminClient::create_topics
     AdminClient::delete_topics
     AdminClient::create_partitions
```

Authentication, request validation, outcome mapping, and the existing
`NOT_CONTROLLER` retry behavior are unchanged. Each retry continues to reuse
the same configured Kafka request timeout. No CRD or operator field was added
because no checked-in Kubernetes owner deploys the standalone admin UI
process.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,282 lines across 1,054 files. Its admin UI subset contained 16
lines: three configured defaults, 11 invariant permission-bit assignments,
one invariant session-cookie name, and one structural sidebar-link table. The
categories sum to all 16 lines, with zero unresolved scanner-visible admin UI
owners.

Before this section was appended, the exact focused search

```text
rg -n \
  "topic_mutation_timeout_ms|TopicMutationTimeoutMs|DEFAULT_TOPIC_MUTATION_TIMEOUT_MS|topic-mutation-timeout-ms|CRABKA_ADMIN_UI_TOPIC_MUTATION_TIMEOUT_MS" \
  crates/admin-ui \
  docs/configuration-audit.md
```

reported 26 lines. The mutually exclusive classification is 17 production
configuration-flow references, nine test references, and zero unresolved-owner
references: `17 + 9 + 0 = 26`.

The package's 126 tests passed, including typed default, accepted minimum,
invalid-value rejection, and hermetic CLI-over-environment precedence tests.
Focused call-site inspection found exactly three configured consumers and no
remaining `30_000` literal in `server_fns.rs`. Strict package Clippy, nightly
formatting, the single-help-entry check, diff hygiene, and the unchanged
`Cargo.lock` check all passed.

### Adjacent Pending Policy

This closes the scanner-visible operational owners in `crabka-admin-ui`. The
next scanner-visible production owner is the fixed 15-second Prometheus HTTP
request timeout in `crates/bench-driver/src/prom.rs`. The bench-driver binary
already owns a CLI/environment boundary, but this timeout does not yet flow
through it.

## Bench Driver Prometheus Request Timeout

The bench driver's Prometheus HTTP request timeout is now a positive
`PrometheusRequestTimeoutSeconds` stored in `DriverConfig`. It retains the
15-second default and is resolved from:

```text
--prometheus-request-timeout-seconds
BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and `u64` overflow values before Prometheus I/O.
`PrometheusRequestTimeoutSeconds` uses
`refined_type::rule::GreaterU64<0>` for the positive-value invariant.

The exact runtime flow is:

```text
Cli
  -> DriverConfig::prometheus_request_timeout_seconds
  -> PromClient::new
  -> reqwest::ClientBuilder::timeout
```

`PromClient::new` requires the validated timeout and has no hidden fallback.
Prometheus queries, resource capture, response parsing, skip notes, and error
handling are unchanged.

The checked-in deployment flow is:

```text
BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS
  -> bench/scripts/run-scenario.sh
  -> envsubst
  -> bench/manifests/driver/job-template.yaml
  -> driver container environment
```

The launcher supplies an overrideable 15-second default, and the rendered Job
always contains a nonempty value. No CRD or operator field was added because
the benchmark launcher and Job template, rather than an operator-managed
resource, own this binary.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,287 lines across 1,055 files. Its bench-driver subset contained 39
lines: one configured timeout default, 16 test or harness values, 12 protocol,
format, state, mathematical, or query invariants, and 10 unresolved operational
values. The categories sum to all 39 lines:
`1 + 16 + 12 + 10 = 39`.

Before this section was appended, the exact focused search

```text
rg -n \
  "prometheus_request_timeout_seconds|PrometheusRequestTimeoutSeconds|DEFAULT_PROMETHEUS_REQUEST_TIMEOUT_SECONDS|prometheus-request-timeout-seconds|BENCH_PROMETHEUS_REQUEST_TIMEOUT_SECONDS" \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
```

reported 32 lines. The mutually exclusive classification is 16 production
configuration-flow references, six deployment-flow references, 10 test
references, and zero unresolved-owner references:
`16 + 6 + 10 + 0 = 32`.

The package's 67 registered tests passed, including the typed default, accepted
minimum, invalid-value rejection, validated client construction, and hermetic
CLI-over-environment precedence tests. Strict package Clippy, nightly
formatting, the single-help-entry check, shell syntax, rendered manifest
inspection with an explicit seven-second override, and diff hygiene all
passed. The only `Cargo.lock` change adds the already workspace-pinned
`refined_type` to `crabka-bench-driver`'s direct dependency list; dependency
versions and transitive packages are unchanged.

### Adjacent Pending Policy

This closes only the Prometheus HTTP request timeout. The next coherent
bench-driver operational owner is the client request-timeout policy in
`crates/bench-driver/src/workload.rs`: two seconds for producers, five seconds
for Crabka consumers, and 30 seconds for Kafka consumers. Those values affect
network failure behavior and remain fixed rather than flowing through the
existing CLI/environment boundary.

## Bench Driver Client Request Timeouts

The bench driver's producer and active-stack consumer request timeouts are now
positive, protocol-safe `ClientRequestTimeoutSeconds` values stored in
`DriverConfig`. They retain the existing defaults:

- producer: 2 seconds;
- Crabka consumer: 5 seconds;
- Kafka consumer: 30 seconds.

The settings are resolved from:

```text
--producer-request-timeout-seconds
BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS

--consumer-request-timeout-seconds
BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS
```

The command-line value wins when both inputs are present. When no consumer
override is supplied, the parsed stack selects the existing 5- or 30-second
default. Parsing rejects zero, malformed, negative, and values above 2,147,483
seconds before client construction or I/O.
`ClientRequestTimeoutSeconds` uses
`refined_type::rule::MinMaxU64<1, 2_147_483>` so every accepted whole-second
value fits the Kafka protocol's signed 32-bit millisecond timeout.

The producer value flow is:

```text
Cli
  -> DriverConfig::producer_request_timeout_seconds
  -> ProducerTask
  -> Producer::builder
  -> request_timeout
```

The consumer value flow is:

```text
Cli / active-stack default
  -> DriverConfig::consumer_request_timeout_seconds
  -> ConsumerTask
  -> every build_consumer_with_retry attempt
  -> Consumer::builder
  -> request_timeout
```

Producer send and failover behavior, consumer build attempts and backoff,
polling and error backoff, TLS selection, error reporting, scenario behavior,
final-drain timing, sampling cadence, and Prometheus timing are unchanged.

The checked-in deployment flow is:

```text
BENCH_PRODUCER_REQUEST_TIMEOUT_SECONDS
BENCH_CONSUMER_REQUEST_TIMEOUT_SECONDS
  -> bench/scripts/run-scenario.sh
  -> envsubst
  -> bench/manifests/driver/job-template.yaml
  -> driver container environment
```

The launcher supplies an overrideable 2-second producer default and selects the
5- or 30-second consumer default from its required stack argument. No CRD or
operator field was added because the benchmark launcher and Job template own
this binary.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,298 lines across 1,055 files. Its bench-driver subset contained 50
lines: four configured defaults, 26 test or harness values, 13 protocol,
format, state, mathematical, or query invariants, and seven unresolved
operational values. The categories sum to all 50 lines:
`4 + 26 + 13 + 7 = 50`.

Before this section was appended, the exact focused search

```text
rg -n \
  "producer_request_timeout_seconds|consumer_request_timeout_seconds|ClientRequestTimeoutSeconds|DEFAULT_(PRODUCER|CRABKA_CONSUMER|KAFKA_CONSUMER)_REQUEST_TIMEOUT_SECONDS|producer-request-timeout-seconds|consumer-request-timeout-seconds|BENCH_(PRODUCER|CONSUMER)_REQUEST_TIMEOUT_SECONDS" \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
```

reported 62 lines. The mutually exclusive classification is 31 production
configuration-flow references, 13 deployment-flow references, 18 test
references, and zero unresolved-owner references:
`31 + 13 + 18 + 0 = 62`.

The package's 72 registered tests passed, including the preserved defaults,
protocol-bound acceptance, invalid-value rejection, active-stack resolution,
and hermetic CLI-over-environment precedence tests. Strict package Clippy,
nightly formatting, one help entry per flag, shell syntax, rendered Crabka
2/5, Kafka 2/30, and explicit 7/11 manifests, diff hygiene, and the unchanged
`Cargo.lock` check all passed.

### Adjacent Pending Policy

This closes only the client request timeouts. The next coherent bench-driver
operational owner is the consumer-build retry policy in
`crates/bench-driver/src/workload.rs`: six attempts with a 100-millisecond
initial backoff and a two-second backoff cap. Those values control startup
failure recovery and remain fixed rather than flowing through the existing
CLI/environment boundary.

## Bench Driver Consumer Build Retry

The bench driver's consumer-build retry policy is now represented by validated
`ConsumerBuildAttempts`, `ConsumerBuildBackoffMs`, and
`ConsumerBuildRetryPolicy` values stored in `DriverConfig`. It retains the
existing defaults:

- six total build attempts;
- 100 milliseconds of initial backoff;
- 2,000 milliseconds of maximum backoff.

The settings are resolved from:

```text
--consumer-build-attempts
BENCH_CONSUMER_BUILD_ATTEMPTS

--consumer-build-initial-backoff-ms
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS

--consumer-build-max-backoff-ms
BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and primitive-overflow values.
`ConsumerBuildAttempts` uses `refined_type::rule::GreaterU32<0>` and
`ConsumerBuildBackoffMs` uses `refined_type::rule::GreaterU64<0>`. The complete
policy also rejects an initial backoff above the maximum immediately after
command-line parsing and before scenario-file I/O.

The value flow is:

```text
Cli
  -> ConsumerBuildRetryPolicy
  -> DriverConfig::consumer_build_retry_policy
  -> ConsumerTask
  -> build_consumer_with_retry
  -> exponential_backoff::Backoff::new
```

The existing loop still retries every consumer-build error, preserves attempt
numbering, warning fields, and the terminal error, and uses the dependency's
fixed growth factor and jitter. Client request timeouts, TLS, polling,
poll-error backoff, sampling, and producer behavior are unchanged.

The checked-in deployment flow is:

```text
BENCH_CONSUMER_BUILD_ATTEMPTS
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF_MS
BENCH_CONSUMER_BUILD_MAX_BACKOFF_MS
  -> bench/scripts/run-scenario.sh
  -> envsubst
  -> bench/manifests/driver/job-template.yaml
  -> driver container environment
```

The launcher supplies overrideable 6/100/2,000 defaults, and the rendered Job
always contains all three values. No CRD or operator field was added because
the benchmark launcher and Job template own this binary.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,309 lines across 1,055 files. Its bench-driver subset contained 61
lines: seven configured defaults, 37 test or harness values, 13 protocol,
format, state, mathematical, or query invariants, and four unresolved
operational values. The categories sum to all 61 lines:
`7 + 37 + 13 + 4 = 61`.

Before this section was appended, the exact focused search

```text
rg -n \
  "consumer_build_retry_policy|ConsumerBuildRetryPolicy|ConsumerBuildAttempts|ConsumerBuildBackoffMs|DEFAULT_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)|consumer-build-(attempts|initial-backoff-ms|max-backoff-ms)|BENCH_CONSUMER_BUILD_(ATTEMPTS|INITIAL_BACKOFF_MS|MAX_BACKOFF_MS)" \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
```

reported 91 lines. The mutually exclusive classification is 44 production
configuration-flow references, 18 deployment-flow references, 29 test
references, and zero prior-audit or unresolved-owner references:
`44 + 18 + 29 + 0 = 91`.

The package's 80 registered tests passed, including positive-boundary,
primitive-rejection, ordered-range, preserved-default, early-resolution, and
hermetic CLI-over-environment tests. Strict package Clippy, nightly formatting,
one help entry per flag, shell syntax, default 6/100/2,000 and explicit
3/21/22 manifest renders, diff hygiene, and the unchanged `Cargo.lock` check
all passed.

### Adjacent Pending Policy

This closes only consumer construction retries. The next coherent bench-driver
operational owner is consumer polling in
`crates/bench-driver/src/workload.rs`: the 50-millisecond poll wait and
100-millisecond sleep after poll errors. Those values control steady-state
latency and failure recovery and remain fixed rather than flowing through the
existing CLI/environment boundary.

## Bench Driver Consumer Poll Timing

The bench driver's consumer poll timeout and poll-error backoff are now
positive `ConsumerPollDurationMs` values stored separately in `DriverConfig`.
They retain the existing defaults:

- 50 milliseconds for each `Consumer::poll` call;
- 100 milliseconds of sleep after a poll error.

The settings are resolved from:

```text
--consumer-poll-timeout-ms
BENCH_CONSUMER_POLL_TIMEOUT_MS

--consumer-poll-error-backoff-ms
BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and primitive-overflow values before scenario-file or
network I/O. `ConsumerPollDurationMs` uses
`refined_type::rule::GreaterU64<0>`. One shared value type is sufficient
because both settings are positive millisecond durations and have no
cross-field invariant.

The value flows are:

```text
Cli
  -> DriverConfig::consumer_poll_timeout
  -> ConsumerTask
  -> Consumer::poll

Cli
  -> DriverConfig::consumer_poll_error_backoff
  -> ConsumerTask
  -> tokio::time::sleep after a poll error
```

The consumer loop, stop checks, message processing, first-error recording,
close behavior, client construction, and all other timing are unchanged.

The checked-in deployment flow is:

```text
BENCH_CONSUMER_POLL_TIMEOUT_MS
BENCH_CONSUMER_POLL_ERROR_BACKOFF_MS
  -> bench/scripts/run-scenario.sh
  -> envsubst
  -> bench/manifests/driver/job-template.yaml
  -> driver container environment
```

The launcher supplies overrideable 50- and 100-millisecond defaults, and the
rendered Job always contains both values. No CRD or operator field was added
because the benchmark launcher and Job template own this binary.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,319 lines across 1,055 files. Its bench-driver subset contained 71
lines: nine configured defaults, 47 test or harness values, 13 protocol,
format, state, mathematical, or query invariants, and two unresolved
operational values. The categories sum to all 71 lines:
`9 + 47 + 13 + 2 = 71`.

Before this section was appended, the exact focused search

```text
rg -n \
  "consumer_poll_(timeout|error_backoff)|ConsumerPollDurationMs|DEFAULT_CONSUMER_POLL_(TIMEOUT|ERROR_BACKOFF)_MS|consumer-poll-(timeout|error-backoff)-ms|BENCH_CONSUMER_POLL_(TIMEOUT|ERROR_BACKOFF)_MS" \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
```

reported 56 lines. The mutually exclusive classification is 25 production
configuration-flow references, 12 deployment-flow references, 18 test
references, one prior-audit reference, and zero unresolved-owner references:
`25 + 12 + 18 + 1 + 0 = 56`.

The package's 86 registered tests passed, including positive-boundary,
invalid-value, preserved-default, and hermetic CLI-over-environment tests.
Strict package Clippy, nightly formatting, one help entry per flag, shell
syntax, default 50/100 and explicit 21/22 manifest renders, diff hygiene, and
the unchanged `Cargo.lock` check all passed.

### Adjacent Pending Policy

This closes only consumer poll timing. The next coherent bench-driver
operational owner is the producer's 10-second final-drain timeout in
`crates/bench-driver/src/workload.rs`. It bounds how long unresolved sends can
delay benchmark completion and remains fixed rather than flowing through the
existing CLI/environment boundary.

## Bench Driver Producer Final-Drain Timeout

The bench driver's producer final-drain timeout is now a positive
`ProducerFinalDrainTimeoutSeconds` value stored in `DriverConfig`. It retains
the existing 10-second default.

The setting is resolved from:

```text
--producer-final-drain-timeout-seconds
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and primitive-overflow values before scenario-file or
network I/O. `ProducerFinalDrainTimeoutSeconds` uses
`refined_type::rule::GreaterU64<0>`. It is separate from
`ClientRequestTimeoutSeconds` because the latter's upper bound exists for a
Kafka protocol field, while final drain is an in-process Tokio deadline.

The value flow is:

```text
Cli
  -> DriverConfig::producer_final_drain_timeout
  -> ProducerTask
  -> final drain deadline
  -> timeout_at for outstanding sends
```

The deadline check, unresolved-send drop accounting, first-error preservation,
timeout error text, producer flush, producer close, and all other behavior are
unchanged.

The checked-in deployment flow is:

```text
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS
  -> bench/scripts/run-scenario.sh
  -> envsubst
  -> bench/manifests/driver/job-template.yaml
  -> driver container environment
```

The launcher supplies an overrideable 10-second default, and the rendered Job
always contains the value. No CRD or operator field was added because the
benchmark launcher and Job template own this binary.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,325 lines across 1,055 files. Its bench-driver subset contained 77
lines: 10 configured defaults, 53 test or harness values, 13 protocol, format,
state, mathematical, or query invariants, and one unresolved operational
value. The categories sum to all 77 lines:
`10 + 53 + 13 + 1 = 77`.

Before this section was appended, the exact focused search

```text
rg -n \
  "producer_final_drain_timeout|ProducerFinalDrainTimeoutSeconds|DEFAULT_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS|producer-final-drain-timeout-seconds|BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT_SECONDS" \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
```

reported 38 lines. The mutually exclusive classification is 15 production
configuration-flow references, six deployment-flow references, 17 test
references, and zero prior-audit or unresolved-owner references:
`15 + 6 + 17 + 0 = 38`.

The package's 92 registered tests passed, including positive-boundary,
invalid-value, preserved-default, and hermetic CLI-over-environment tests.
Strict package Clippy, nightly formatting, one help entry, shell syntax,
default 10 and explicit 21-second manifest renders, diff hygiene, and the
unchanged `Cargo.lock` check all passed.

### Adjacent Pending Policy

This closes only producer final-drain timing. The next coherent bench-driver
operational owner is the fixed 2,000-millisecond sample interval in
`crates/bench-driver/src/workload.rs`. It controls time-series resolution and
remains fixed rather than flowing through the existing CLI/environment
boundary.

## Bench Driver Sample Interval

The bench driver's time-series sample interval is now a positive
`SampleIntervalMs` value stored in `DriverConfig`. It retains the existing
2,000-millisecond default.

The setting is resolved from:

```text
--sample-interval-ms
BENCH_SAMPLE_INTERVAL_MS
```

The command-line value wins when both inputs are present. Parsing rejects zero,
malformed, negative, and primitive-overflow values before scenario-file or
network I/O. `SampleIntervalMs` uses
`refined_type::rule::GreaterU64<0>`.

The value flow is:

```text
Cli
  -> DriverConfig::sample_interval
  -> run
  -> Grid::interval_ms
  -> producer and consumer sample buckets
```

The interval-count ceiling, minimum-one bucket behavior, shared `Copy` grid,
index clamping, task-local histograms, and merged output are unchanged. No task
field was added because every producer and consumer already receives the
complete grid.

The checked-in deployment flow is:

```text
BENCH_SAMPLE_INTERVAL_MS
  -> bench/scripts/run-scenario.sh
  -> envsubst
  -> bench/manifests/driver/job-template.yaml
  -> driver container environment
```

The launcher supplies an overrideable 2,000-millisecond default, and the
rendered Job always contains the value. No CRD or operator field was added
because the benchmark launcher and Job template own this binary.

After the implementation and before this section was appended, the exact
scanner command

```text
tools/audit-runtime-values.sh
```

reported 6,326 lines across 1,055 files. Its bench-driver subset contained 78
lines: 11 configured defaults, 54 test or harness values, and 13 protocol,
format, state, mathematical, or query invariants. The categories sum to all 78
lines, with zero unresolved operational values:
`11 + 54 + 13 = 78`.

Before this section was appended, the exact focused search

```text
rg -n \
  "sample_interval|SampleIntervalMs|DEFAULT_SAMPLE_INTERVAL_MS|sample-interval-ms|BENCH_SAMPLE_INTERVAL_MS" \
  crates/bench-driver \
  bench \
  docs/configuration-audit.md
```

reported 37 lines. The mutually exclusive classification is 14 production
configuration-flow references, six deployment-flow references, 17 test
references, and zero prior-audit or unresolved-owner references:
`14 + 6 + 17 + 0 = 37`.

The package's 98 registered tests passed, including positive-boundary,
invalid-value, preserved-default, and hermetic CLI-over-environment tests.
Strict package Clippy, nightly formatting, one help entry, shell syntax,
default 2,000 and explicit 21-millisecond manifest renders, diff hygiene, and
the unchanged `Cargo.lock` check all passed.

### Bench Driver Closure

Every bench-driver scanner hit is now a configured value, a test or harness
value, or a protocol, format, state, mathematical, or query invariant. The
bench driver has no remaining unresolved operational values and is moved to
the complete list above.

The next coherent repository owner is blockstore safety and retention policy:
the one-gibibyte block read cap, 256-mebibyte index and profile-index snapshot
caps, and eight-snapshot retention default in `crates/blockstore`. These values
bound production I/O and storage but currently remain library defaults rather
than deployment configuration.

## Blockstore Trace/Profile Index Snapshot Policy

Trace and profile index-snapshot reads now use a positive
`IndexSnapshotMaxBytes` value, and snapshot writers use a positive
`IndexSnapshotRetain` value. The settings preserve the existing
268,435,456-byte and eight-snapshot defaults.

Both service binaries accept:

```text
--index-snapshot-max-bytes
--index-snapshot-retain
```

The traces binary reads:

```text
CRABKA_TRACES_INDEX_SNAPSHOT_MAX_BYTES
CRABKA_TRACES_INDEX_SNAPSHOT_RETAIN
```

The profiles binary reads:

```text
CRABKA_PROFILES_INDEX_SNAPSHOT_MAX_BYTES
CRABKA_PROFILES_INDEX_SNAPSHOT_RETAIN
```

Command-line values win over environment values. Parsing rejects zero,
malformed, negative, and primitive-overflow values before object-store or
network I/O. The newtypes use `refined_type::rule::GreaterU64<0>` and
`refined_type::rule::GreaterUsize<0>`.

The read flow is:

```text
CLI / environment / typed default
  -> service or block-builder configuration
  -> TraceIndex / ProfileIndex configurable load
  -> crabka_object_store::read_capped
```

This adds the existing 256-mebibyte safety boundary to the previously unbounded
trace snapshot read. Startup, periodic refresh, query-index, compactor, and
block-builder load paths all receive the configured value.

The write flow is:

```text
CLI / environment / typed default
  -> service or block-builder configuration
  -> TraceIndex / ProfileIndex configurable save
  -> shared snapshot writer
  -> prune to configured retention
```

Block-builder and compactor save paths receive the configured retention.
Existing public methods remain default-preserving compatibility wrappers.
Snapshot format, naming, latest selection, object-store layout, and caller
fallback behavior are unchanged.

The checked-in deployment owner is
`demo/observability/docker-compose.yml`. Trace/profile block-builders receive
both settings; read-only queriers receive only the maximum-byte setting.
Compose supplies overrideable 268,435,456-byte and eight-snapshot defaults. No
CRD or operator field was added because these services are not managed by an
existing repository CRD.

After implementation and before this section was appended,
`tools/audit-runtime-values.sh` reported 6,328 lines across 1,055 files. Its
affected-package subsets contained 64 blockstore lines, 132 traces lines, and
70 profiles lines.

Before this section was appended, the exact focused search

```text
rg -n \
  "MAX_(INDEX|PROFILE_INDEX)_SNAPSHOT_BYTES|DEFAULT_INDEX_SNAPSHOT_(MAX_BYTES|RETAIN)|index_snapshot_(max_bytes|retain)|index-snapshot-(max-bytes|retain)|INDEX_SNAPSHOT_(MAX_BYTES|RETAIN)" \
  crates \
  demo \
  docs/configuration-audit.md
```

reported 97 lines. The mutually exclusive classification is 43 production
configuration-flow or compatibility-API references, six deployment-flow
references, 48 test references, and zero unresolved references for the
trace/profile snapshot owner: `43 + 6 + 48 + 0 = 97`.

The legacy generic `Index::load` cap has no production caller and remains a
default-preserving library compatibility API. The trace/profile production
paths use the new configurable APIs.

The combined all-target test gate passed for `crabka-blockstore`,
`crabka-traces`, `crabka-profiles`, and `observability-demo-app`. Strict
combined Clippy with warnings denied, nightly formatting, Compose validation,
diff hygiene, and lockfile-diff inspection also passed. Default and overridden
Compose rendering and one help entry per setting in each service binary passed
during the package-level gates. The only lockfile change for this slice was the
already committed direct `refined_type` dependency on `crabka-blockstore`.

### Adjacent Pending Policy

This closes trace/profile index-snapshot size and retention configuration. The
separate one-gibibyte Parquet block-read cap in
`crates/blockstore/src/reader.rs` remains pending as the next coherent
blockstore owner.

## Blockstore Parquet Read Cap

The explicit Parquet readers now use a positive `BlockReadMaxBytes` value while
preserving the existing 1,073,741,824-byte default. The traces binary accepts:

```text
--block-read-max-bytes
CRABKA_TRACES_BLOCK_READ_MAX_BYTES
```

Command-line values win over environment values. Parsing rejects zero,
malformed, negative, and primitive-overflow values before object-store or
network I/O. The newtype uses `refined_type::rule::GreaterU64<0>`.

The compactor flow is:

```text
CLI / environment / typed default
  -> traces compactor
  -> configurable whole-block reader
  -> object-store head and size check
  -> Parquet stream
```

The sharded query flow is:

```text
CLI / environment / typed default
  -> query-frontend BlockStore
  -> configurable row-group metadata reader
```

```text
CLI / environment / typed default
  -> querier BlockStore
  -> configurable selected-row-group reader
```

All three reader forms reject an object above the configured cap after
`head()` and before streaming Parquet bytes. An object exactly at the cap is
accepted. `BlockStore::empty_like` preserves the configured value.

The existing read functions, `BlockStore::new`, and traces compactor helpers
remain default-preserving compatibility wrappers. Parquet encoding, block
writing, existing error mapping, and DataFusion's independent whole-block scan
path are unchanged.

Metrics has no corresponding setting because its production code does not call
these capped reader APIs. Its test-only reader calls continue using the default
wrapper.

The checked-in deployment owner is the traces querier in
`demo/observability/docker-compose.yml`. Compose supplies an overrideable
1,073,741,824-byte default. The demo does not run traces query-frontend or
compactor roles, so it does not carry unused entries for them. No CRD or
operator field was added because traces is not managed by an existing
repository CRD.

After implementation and before this section was appended,
`tools/audit-runtime-values.sh` reported 6,330 lines across 1,055 files. Its
affected-package subsets contained 65 blockstore lines and 133 traces lines.

Before this section was appended, the exact focused search

```text
rg -n \
  'MAX_BLOCK_BYTES|DEFAULT_BLOCK_READ_MAX_BYTES|BlockReadMaxBytes|block_read_max_bytes|block-read-max-bytes|BLOCK_READ_MAX_BYTES' \
  crates \
  demo \
  docs/configuration-audit.md
```

reported 73 lines. The mutually exclusive classification is 43 production
configuration-flow or compatibility-API references, one deployment-flow
reference, 29 test references, and zero unresolved references:
`43 + 1 + 29 + 0 = 73`.

The combined all-target test gate passed for `crabka-blockstore`,
`crabka-traces`, and `observability-demo-app`. Strict combined Clippy with
warnings denied, nightly formatting, one help entry, default and overridden
Compose rendering, Compose validation, diff hygiene, scanner stability, and
lockfile-diff inspection also passed. `Cargo.lock` is unchanged.

## Observability WAL Fetch Limits

The traces and profiles WAL consumers now preserve their existing total and
per-partition fetch defaults of 2,097,152 and 262,144 bytes while accepting:

```text
--wal-fetch-max-bytes
--wal-fetch-partition-max-bytes
CRABKA_TRACES_WAL_FETCH_MAX_BYTES
CRABKA_TRACES_WAL_FETCH_PARTITION_MAX_BYTES
CRABKA_PROFILES_WAL_FETCH_MAX_BYTES
CRABKA_PROFILES_WAL_FETCH_PARTITION_MAX_BYTES
```

Command-line values win over environment values. Shared
`ConsumerFetchMaxBytes` and `ConsumerFetchPartitionMaxBytes` newtypes use
`refined_type::rule::GreaterI32<0>`, rejecting zero, malformed, negative, and
primitive-overflow values before network I/O. They convert the validated
protocol integer to `ByteSize` only at the consumer-builder boundary.

The traces binary routes both settings through its common WAL-consumer helper,
covering block builder, live store, embedded-querier live store, and metrics
generator consumers. The profiles binary routes them through
`BlockBuilderConfig`. The consumer library's independent 50-MiB total and
1-MiB per-partition defaults remain unchanged.

The checked-in deployment owners are `traces-block-builder` and
`profiles-block-builder` in `demo/observability/docker-compose.yml`. No other
demo service receives unused entries. No CRD or operator field was added
because these services are not managed by an existing repository CRD.

After implementation and before this section was appended,
`tools/audit-runtime-values.sh` reported 5,533 lines across 1,038 files. Its
affected-package subsets contained 92 client-consumer lines, 114 traces lines,
63 profiles lines, and 10 observability-demo-app lines.

Before this section was appended, the exact focused search

```text
rg -n \
  'WAL_FETCH_(MAX|PARTITION_MAX)|ConsumerFetch(Max|PartitionMax)Bytes|wal_fetch_(max|partition_max)_bytes|wal-fetch-(max|partition-max)-bytes' \
  crates \
  demo \
  docs/configuration-audit.md
```

reported 115 lines. The mutually exclusive classification is 56 production
configuration-flow references, four deployment-flow references, 44 test
references, 11 adjacent existing share-consumer or audit references matched by
the broad expression, and zero unresolved references:
`56 + 4 + 44 + 11 + 0 = 115`.

The combined all-target test gate passed for `crabka-client-consumer`,
`crabka-traces`, `crabka-profiles`, and `observability-demo-app`. Strict
combined Clippy with warnings denied, nightly formatting, one help entry per
setting in each service binary, default and overridden Compose rendering,
Compose validation, diff hygiene, scanner stability, and lockfile-diff
inspection also passed. `Cargo.lock` is unchanged.

## Traces Scan Concatenation Cap

The traces querier and live-store span stores now preserve the existing
1,500,000,000-byte scan-concatenation cap while accepting:

```text
--scan-concat-max-bytes
CRABKA_TRACES_SCAN_CONCAT_MAX_BYTES
```

Command-line values win over environment values. `ScanConcatMaxBytes` uses
`refined_type::rule::MinMaxU64<1, 1_500_000_000>`, rejecting zero, malformed,
negative, primitive-overflow, and above-ceiling values before external I/O.
It converts to `ByteSize` at the existing batch-memory comparison.

The upper bound remains a fixed invariant because Arrow variable-length
columns use signed 32-bit offsets. Operators can lower the cap to constrain
memory, but cannot configure a value beyond the established safe headroom. An
exact-cap scan remains accepted; a larger scan returns the existing actionable
error before `concat_batches`.

`CrabkaSpanStore::new` remains a default-preserving compatibility constructor.
The traces querier and live-store production paths use the configurable
constructor. The checked-in deployment exposes the setting only on
`traces-querier`; the demo does not run the separate live-store role. No CRD or
operator field was added because traces is not managed by an existing
repository CRD.

After implementation and before this section was appended,
`tools/audit-runtime-values.sh` reported 5,534 lines across 1,038 files. Its
affected-package subsets contained 115 traces lines and 10
observability-demo-app lines.

Before this section was appended, the exact focused search

```text
rg -n \
  'MAX_SCAN_CONCAT|DEFAULT_SCAN_CONCAT_MAX_BYTES|ScanConcatMaxBytes|scan_concat_max_bytes|scan-concat-max-bytes|SCAN_CONCAT_MAX_BYTES' \
  crates \
  demo \
  docs/configuration-audit.md
```

reported 44 lines. The mutually exclusive classification is 24 production
configuration-flow or compatibility-API references, one deployment-flow
reference, 19 test references, and zero unresolved references:
`24 + 1 + 19 + 0 = 44`.

The combined all-target test gate passed for `crabka-traces` and
`observability-demo-app`. Strict combined Clippy with warnings denied, nightly
formatting, one help entry, default and overridden Compose rendering, Compose
validation, diff hygiene, scanner stability, and lockfile inspection also
passed. The only lockfile change is the direct workspace-pinned `refined_type`
dependency on `crabka-traces`.

## Branch-Wide UOM Boundary Audit

This checkpoint audits the branch code and configuration at `HEAD`, relative
to the recorded merge-base
`1d171e99ac73cebdb944479d0d249b816e55a454`. It does not close the separate
whole-repository runtime-value audit.

### Counts

The recorded branch baseline contained 192 dimensioned boundary appearances:
20 already used UOM quantities and 172 were raw migration candidates. The
final branch contains zero unresolved live configuration surfaces from that
set. The exhaustive removed-name scan covers 979 changed files and 138 removed
unit-suffixed patterns. Its 377 residual lines in 81 files are 186 historical
documentation lines, three benchmark output-format fields, and 188 Rust
external-contract, invariant, UOM-backed, lowering-bound, or test-only lines.

The broader name-and-type search over 96,455 branch-added lines reports 18,787
matches: 6,299 historical Markdown lines, 1,657 test/model-path lines, and
10,831 production-path references. These are references rather than distinct
configuration definitions; the removed-name scan above is the authoritative
branch configuration-boundary classification.

The final whole-repository scanner reports 5,661 candidates across 1,041
files. The four owners declared complete above account for 1,023 candidates:
30 in `bench-driver`, 919 in `broker`, 38 in `schema-registry`, and 36 in
`grpc-gateway`.

The complete per-line ledger mechanically separates all 5,661 rows into 2,259
generated/protocol/format rows, 579 obvious invariants, 84 test/model rows, 38
allocation-only hints, 53 existing configuration-flow declarations, 1,023
completed-owner rows, and 1,625 rows requiring semantic owner review. The
categories are conservative: only the last 1,625 remain unclassified, and no
claim is made that they are all production tunables.

### Owner Migration Summary

- Shared units: validated parsing, formatting, serde, conversion, and explicit
  zero handling for time, size, rate, frequency, and ratio quantities.
- Admin UI, bench driver, and observability demo: CLI-over-environment typed
  runtime settings with physical defaults preserved.
- Traces and profiles: typed size, cadence, rate, and ratio boundaries plus
  checked-in Compose routing.
- Broker: typed time, size, rate, and ratio configuration through direct
  CLI/file inputs and the `Kafka` CRD.
- Gres: typed time and size settings through direct binaries, the CLI, and
  `Gres`/`GresTenant` CRDs.
- Schema Registry and gRPC Gateway: typed runtime fields through their CRDs.
- Telemetry: the proven residual OTLP timeout and heartbeat owner now uses
  `CRABKA_OTLP_TIMEOUT` and `CRABKA_OTLP_HEARTBEAT_INTERVAL`; the `Kafka` CRD
  uses string-valued `otlp.timeout`. The standard
  `OTEL_EXPORTER_OTLP_TIMEOUT_SECS` remains an external OpenTelemetry contract.

### Post-Closure UOM API Checks

A follow-up caller audit found two internal policy constructors that stored
UOM quantities but still accepted unit-suffixed primitives.
`RecoveryReadPolicy` now accepts `Time` and `ByteSize`, and `WalAdminPolicy`
accepts `Time` for all three timeouts. Both validate exact positive protocol
units at the shared substrate boundary; Gres passes its CLI/CRD-derived
quantities through directly and lowers only when building Kafka requests.

The same branch-diff pass found share-consumer heartbeat settings still exposed
as `Duration`. The real heartbeat and leave timeout settings now accept and
carry `Time`, and share-fetch sizes reject fractional, nonfinite, or overflowing
`ByteSize` values before broker I/O. The ignored share `session_timeout`
builder option was removed instead of preserving a knob with no effect.

The affected Gres all-target tests and strict Clippy passed. Client-consumer
reported 157 unit and three integration tests passing, its strict all-target
Clippy passed, and the share-consumer integration target compiled successfully.

### Renamed Public Surfaces

No compatibility aliases remain for the migrated branch-added settings.
Dimensioned CLI flags, environment variables, file fields, and CRD fields use
quantity names without `_MS`, `_SECONDS`, `_BYTES`, or equivalent fixed-unit
suffixes. Nonzero operator values require explicit units. The bench deployment
family is the representative external rename:

```text
BENCH_PROMETHEUS_REQUEST_TIMEOUT
BENCH_PRODUCER_REQUEST_TIMEOUT
BENCH_PRODUCER_FINAL_DRAIN_TIMEOUT
BENCH_CONSUMER_REQUEST_TIMEOUT
BENCH_CONSUMER_BUILD_INITIAL_BACKOFF
BENCH_CONSUMER_BUILD_MAX_BACKOFF
BENCH_CONSUMER_POLL_TIMEOUT
BENCH_CONSUMER_POLL_ERROR_BACKOFF
BENCH_SAMPLE_INTERVAL
```

The Gres lifecycle manifest uses `suspendMaxCheckpointSize: "0B"`; the prior
byte-suffixed numeric CRD field is absent.

### Branch Exceptions

- Historical Superpowers plans, specifications, and earlier sections of this
  append-only audit retain superseded names as historical evidence.
- `STARTUP_MS` is a benchmark result field, not operator input.
- Kubernetes-native `initialDelaySeconds`, `periodSeconds`, and
  `leaseDurationSeconds` are external API contracts.
- `checkpointBytes`, `durationMs`, `throttleBytesPerSec`, `timeDifferenceMs`,
  `maxBytes`, `valueBytes`, `baseBackoffMs`, and `maxBackoffMs` are wire,
  persisted-format, or generated external-contract fields.
- `KEY_INTERVAL_MS`, `SESSION_TIMEOUT_MS`, `REBALANCE_TIMEOUT_MS`,
  `NO_TIMEOUT_MS`, `PRODUCER_ID_EXPIRATION_MS`, `MAX_TXN_TIMEOUT_MS`,
  `MIN_TXN_TIMEOUT_MS`, and exact fetch byte/wait constants are Kafka protocol
  compatibility fields or sentinels.
- Remaining uppercase time/byte constants are internal invariants, typed UOM
  defaults, protocol-lowering bounds, or test inputs. They are not live CLI,
  environment, file-config, CRD, Compose, shell, or CI surfaces.

### Exact Searches

```bash
base=1d171e99ac73cebdb944479d0d249b816e55a454
git diff --unified=0 "$base"..HEAD -- \
  '*.rs' '*.toml' '*.yaml' '*.yml' '*.json' '*.md'
```

The added-line inventory was searched for:

```text
_MS _MILLIS _SECONDS _SECS _BYTES _BYTES_PER_SEC
timeout interval delay deadline backoff linger window ttl rate ratio
Time ByteSize ByteRate Frequency Ratio
```

The exhaustive stale-surface check extracts every removed unit-suffixed flag,
environment variable, and field pattern from the same diff and fixed-string
searches every changed file for the removed patterns. The whole-repository
candidate inventory is reproduced with:

```bash
tools/audit-runtime-values.sh
```

### Verification Evidence

Task 10 verified the only changed Compose file with default and representative
override renders, rendered the bench Job with all nine renamed environment
families, generated nine operator CRDs with no checked-in diff, built every
affected binary, and checked each renamed help flag exactly once. The final
Task 11 workspace test, strict Clippy, formatting, CRD, Compose, diff, lockfile,
and scanner gates are recorded in the Task 11 execution report.

### Unresolved Repository Coverage

The pending list in **Coverage Status** is authoritative. The 1,625 exact
remaining-review rows are preserved in the Task 11 execution report with
crate, path, line, source text, and the reason
`pending owner; semantic production-policy review required`. Closing those
rows requires owner-by-owner production/test/contract/invariant review and any
resulting configuration changes. Treating the branch-only zero as
whole-repository completion would be incorrect.

## Coverage Reconciliation: Admin UI, Audit, and Operator

The current owner ledger now closes three previously stale entries:

- `admin-ui` has 16 scanner rows. Its three runtime-policy defaults are
  positive UOM values exposed through CLI-over-environment inputs. The
  remaining rows are permission-bit assignments, the session-cookie identity,
  and the static sidebar table.
- `audit` has 11 scanner rows. The genesis hash, checkpoint and schema
  identities, signing domain, record header names, and spool filename are
  persisted-format or security-domain invariants. The remaining four UOM
  timing/capacity constants are inside test modules.
- `operator` has 138 scanner rows. Runtime reconciliation, leader-election,
  topic-mutation, rebalancer, and delegation-token timing policy flows through
  `OperatorConfig` as positive UOM CLI/environment values. Resource-owned
  defaults flow through their CRDs. The residual rows are test inputs,
  Kubernetes or Kafka external contracts, resource names and paths, generated
  protocol bounds, allocation hints, ports, image defaults, or other fixed
  identities.

The operator package's all-target tests and strict Clippy passed, its generated
CRDs reproduced exactly, every operator timing flag appeared exactly once in
`run --help`, nightly formatting and diff hygiene passed, and the affected Gres
process tests compiled. No new audit-crate setting is warranted: none of its
production scanner rows expresses deployment policy.

## Authz

The `authz` owner has two scanner rows, both in the `#[cfg(test)]`
`precedence` module: the independent Kafka ACL implication table and the
fixture principal name. Production authorization is a pure decision function
with no timing, capacity, or deployment-policy literal. Both rows are
verification inputs, so no runtime setting is warranted.

## Blockstore

The `blockstore` owner has 64 scanner rows. Snapshot read caps, snapshot
retention, and persisted Parquet read caps have positive UOM/configured APIs;
the traces and profiles process owners pass their CLI-over-environment values
into those seams. Default-preserving public wrappers retain the prior 256 MiB,
eight-snapshot, and 1 GiB behavior.

The remaining rows are Arrow/Parquet schema identities, object-format names
and versions, FNV and Bloom-filter mathematics, minimum-safe nonzero
invariants, allocation hints, compatibility defaults, or test inputs. In
particular, the 250 ms receive timeout is a test-only hang guard and the Tempo
Bloom defaults are an interoperability algorithm with an already-parameterized
lower-level constructor. No unresolved blockstore deployment policy remains.

The current `crabka-blockstore` all-target gate passed 180 tests across its
library and integration targets.

## CLI

The CLI owner had one unresolved runtime policy: Gres tenant SCRAM credentials
were generated with a fixed PBKDF2 iteration count. The shared security crate
now owns a `refined_type`-validated `ScramIterations` in the exact broker range
4096 through 16384. Existing defaults remain unchanged: `crabka gres
create-tenant` defaults to 4096, while operator-managed tenants default to the
existing 8192 client value.

The direct CLI boundary is:

```text
--scram-iterations
CRABKA_GRES_SCRAM_ITERATIONS
```

The existing `--wal-replication` input now also reads
`CRABKA_GRES_WAL_REPLICATION` and validates through the existing replication
factor type. Command-line values win over environment values.

Operator-managed Gres tenants resolve the same validated SCRAM count from:

```text
Gres.spec.defaults.scramIterations
GresTenant.spec.overrides.scramIterations
```

The tenant override wins over the fleet default. Both generated CRDs carry
minimum 4096 and maximum 16384. A changed iteration count regenerates the
stored PostgreSQL verifier even when the password is unchanged, and the same
value reaches the Kafka SCRAM upsertion.

The current CLI scanner subset has 43 rows. The remaining production rows are
stable exit codes, the Base64 alphabet, and the fixed in-cluster PostgreSQL
service port; all other rows are test inputs. The CLI's 62 all-target tests,
the operator's 743 library tests and all integration targets, the security
crate's 208 tests, client-admin's 78 library/integration tests, the broker's 26
focused SCRAM-handler tests, strict affected-package Clippy, nightly
formatting, exact help-entry checks, generated-CRD reproduction, lockfile
scope, and diff hygiene passed.

## Client Admin

The `client-admin` owner has 21 scanner rows. Its seven production rows are Kafka
wire contracts: the dynamic-topic config source, topic resource type,
`NOT_CONTROLLER` error code, exact quota match type, two SCRAM mechanism IDs,
and the ACL wildcard filter value. These values must match the Kafka protocol
and are not deployment policy.

The remaining 14 rows are request-encoding expectations and bounded test
timings inside `#[cfg(test)]` modules. In particular, the two 5,000 ms topic
request values assert conversion from the UOM `Time` supplied to the request
builders; they are not defaults used by production calls.

Connection policy is already an explicit library input. Callers that need
custom DNS, TCP-connect, or request deadlines pass a complete
`ConnectionOptions` containing validated UOM values to
`AdminClient::connect_with_options`. The convenience constructors preserve the
existing shared defaults. Adding crate-owned CLI flags or environment
variables would put process configuration in a library and duplicate the
actual process owners, so no new setting is warranted.

The current client-admin all-target gate passed 78 library and integration
tests, and strict affected-package Clippy passed.

## Compression

The `compression` owner has 16 scanner rows. Its three production values are
Kafka interoperability contracts: the xerial Snappy header, xerial's 32 KiB
chunk size, and Kafka's default zstd compression level 3. Changing any of them
would change the wire output rather than tune deployment policy.

All decompression entry points already require callers to provide a
dimensioned `ByteSize` output cap. LZ4 framing likewise intentionally matches
Kafka's fixed 64 KiB independent-block format. The remaining scanner rows are
test payloads, safety caps, and expected error limits, so no configuration
surface is warranted.

The current compression all-target gate passed 41 tests; four JVM-oracle
differential tests remained intentionally ignored. Its benchmark target ran
successfully, and strict all-target Clippy passed.

## Connect Derive and Logfmt

The `connect-derive` and `logfmt` owners each have zero scanner rows. A full
production-source review found no hidden runtime timing, capacity, retry, or
resource policy:

- `connect-derive` translates user-declared connector field metadata into
  schema code. Its numeric tokens are Rust primitive type names and generated
  syntax, not runtime values.
- `logfmt` maps tracing events to Cloud Logging JSON. Its timestamp format,
  severity mapping, and JSON field representation are output contracts; all
  numeric values are in tests.

Neither crate owns a process or deployment boundary, so CLI, environment, and
CRD settings would be unused. The current combined gate passed 17 tests across
both crates, including compile-pass and compile-fail cases, and strict
all-target Clippy passed.

## IDs

The `ids` owner has four scanner rows, all fixed domain sentinels: zero offset,
Kafka's no-producer ID, unknown leader epoch, and initial leader epoch. They
encode Kafka wire and state-machine meaning and cannot be deployment policy.
The crate has no timing, capacity, resource, or retry behavior, so no
configuration surface is warranted.

The current IDs all-target gate passed eight tests, including wire-boundary
round trips, and strict all-target Clippy passed.

## Docgen

The `docgen` owner has 14 scanner rows. Eleven are canonical Kafka protocol or
KIP documentation identities, two are static explanatory text blocks, and one
is the recursion safety bound used while rendering cyclic JSON Schema
references. The recursion bound controls generated-document traversal rather
than deployed runtime behavior.

The command already exposes its input directories as CLI arguments with
documented defaults. Page weights, table columns, and scenario node labels are
website structure and generated content. No environment variable or CRD field
is warranted for this build-time tool.

The current docgen all-target gate passed 20 tests, including cyclic-schema
termination and generated-tree output, and strict all-target Clippy passed.

## Protocol Codegen

The `protocol-codegen` owner has 16 scanner rows. Fifteen are emitted Rust
constant declarations or generator lookup tables whose values come from the
vendored Kafka schemas. The remaining 127 version cap bounds generated
differential-test cases to Kafka's signed protocol-version domain; it is not a
deployed runtime limit.

Primitive widths, nullable markers, version ranges, formatting width, and
generated default literals are serialization or source-format contracts.
Changing them through runtime configuration would make generated code
non-deterministic, so no setting is warranted.

The current protocol-codegen all-target gate passed 35 tests, including every
vendored schema parse, validation, parity, snapshot, and compile check. Strict
all-target Clippy passed.

## Kafka Tap and LogQL

The `kafka-tap` owner had one scanner row: an arbitrary one-hour sleep repeated
forever solely to keep its main thread alive. It now parks the thread instead,
removing a fake timing policy without adding a useless setting.

The `logql` owner has one scanner row, the reserved internal label used to
carry unwrapped sample values between parsing and evaluation. Its other
numeric literals implement grammar, decimal arithmetic, unit conversion, and
template formatting contracts or are test inputs. The crate owns no runtime
timing, capacity, retry, or resource policy.

The current combined all-target gate passed three Kafka Tap tests and 258
LogQL tests. Strict all-target Clippy, nightly formatting, a zero-row Kafka Tap
scanner check, and diff hygiene passed.

## Integration Tests

The `integration-tests` owner is a non-published harness package with no
production dependencies or deployed binary. Its two scanner rows are the
request timeout and producer linger used by the `loadgen` example fixture.
Every other source file is an integration test or example, so none of its
values owns production configuration.

No CLI, environment variable, or CRD field is warranted. The package library
target compiled and ran successfully, and strict all-target Clippy compiled
the complete test and example surface.

## Remote Storage

The `remote-storage` owner has one scanner row, an expected byte cap in an
error-format test. Production multipart thresholds and chunk sizes already
come from the shared S3/GCS object-store configuration, flow through broker
file configuration and Kafka CRD fields, and become dimensioned `ByteSize`
values at the upload boundary.

Object-key names, index suffixes, KIP-405 state transitions, and wire
sentinels are compatibility contracts. No unresolved deployment policy remains
in this crate.

The current remote-storage all-target gate passed 60 tests, including
configured multipart paths for S3 and GCS, and strict all-target Clippy passed.

## Object Store

The `object-store` owner has four scanner rows. Its 100 MiB multipart threshold
and 16 MiB chunk size are defaults in the public S3/GCS configuration structs,
not fixed use-site policy. Broker file configuration and Kafka CRDs can
override both values. Every buffered object read separately requires its
caller to supply an explicit cap.

The remaining two scanner rows are expected limits in error and capped-read
tests. Backend protocol limits and secret redaction are fixed invariants, so no
additional setting is warranted.

The current object-store all-target gate passed 29 tests, including configured
multipart boundaries and capped reads, and strict all-target Clippy passed.

## Pgparser

The `pgparser` owner has six scanner rows. Five are parser contracts:
the typed non-goal refusal registry, the `COPY FROM STDIN` sentinel, the
generated command-identity catalog, the identifier-slot grammar table, and a
test-only placeholder table.

The remaining row is `MAX_DEPTH = 50`, the parser's denial-of-service safety
boundary. It caps recursive parsing and the depth of iteratively built ASTs
before either can overflow the server worker stack during parse, evaluation,
or recursive drop. The value is deliberately derived from the smallest
deployed stack and has tests immediately below and above the boundary.
Allowing operators to raise it would make a crash-safety invariant tunable, so
it remains fixed.

The crate owns no deployment timing, capacity, retry, or resource policy, so
no CLI, environment variable, or CRD field is warranted. The current
`pgparser` all-target gate passed 214 tests; its feature-gated oracle target
compiled with zero active tests, and strict all-target Clippy passed.

## Throttle

The `throttle` owner has seven scanner rows. Its only production row is the
documented one-second burst used by the rate-only convenience methods.
Callers that own independent burst policy already use
`set_token_rate_with_burst`, `set_byte_rate_with_burst`, or
`set_event_rate_with_burst`; adding another configuration layer inside the
shared library would duplicate those inputs.

The remaining rows are unit and model-test deadlines, mock-clock advances, and
test token counts. Production literals not reported by the scanner implement
nanosecond conversion and the seqlock generation protocol. They are arithmetic
invariants, not deployment policy.

The current `throttle` all-target gate passed 15 unit and property tests plus
three bounded concurrency-model tests, and strict all-target Clippy passed.

## Playground, Verified, and Voters

The scanner reports zero rows for each of these crates, and their complete
source surfaces own no deployed runtime policy.

`playground` is a browser-only wrapper around the deterministic KRaft
simulator. Its selectable voter count is clamped to the simulator's supported
one-to-seven-node domain, and its settle step budget prevents a pathological
simulation from hanging the browser. Those are UI safety bounds, not
deployment settings.

`verified` contains pure, formally verified kernels. Operational values such
as delete retention, election jitter range, quorum majority, and offsets are
explicit caller inputs. Its remaining literals are integer-domain, proof,
deterministic-hash, algorithm, and test contracts.

`voters` contains pure KIP-853 value types. Its default `kraft.version` range
describes the implementation's supported protocol capability; it is not an
operator-selected tuning policy.

No CLI, environment variable, or CRD field is warranted. The combined
all-target gate passed 10 Playground tests, 15 Verified tests, and three Voters
tests, and strict all-target Clippy passed.

## Log I/O Benchmark

`log-iobench` is a non-published benchmark-only package. Its production library
is a seven-line marker target, and the scanner reports zero rows.

Every numeric value lives in the single Criterion benchmark and defines its
fixed comparison fixture: segment and read sizes, batch shape and count, byte
payload, and midpoint selection. Making those values deployment configuration
would not change any shipped runtime behavior and would make benchmark results
less comparable.

No CLI, environment variable, or CRD field is warranted. The library test
target passed, the complete benchmark target compiled, and strict all-target
Clippy passed.

## Connect Postgres

The `connect-postgres` owner has eleven scanner rows. Its one operational
quantity, `max_messages_per_poll`, is already a typed connector configuration
field and flows directly to PostgreSQL's logical-slot peek query. The two
reported `1000` values are test fixtures confirming that configured default.

The PostgreSQL-to-Unix epoch delta is a protocol time-domain conversion. The
remaining schema ids, protobuf message indexes, package and message names are
the connector's stable wire schema. None is deployment policy.

No additional CLI, environment variable, or CRD field is warranted. The
current all-target gate passed 69 tests, and strict all-target Clippy passed.

## Gres Activator Completion

The current full-crate review closes the activator sub-slice recorded above.
All production deployment policy is already exposed as typed CLI options with
environment-variable backing: connection addresses, readiness timing, shared
registry timing and fetch limits, replication, and the backend endpoint
template. Dimensioned settings use `Time` or `ByteSize`; validated scalar and
string boundaries use `refined_type`.

The current scanner rows are test timing fixtures, invalid-zero parser cases,
and environment/CLI precedence values. Startup packet framing and its imported
maximum length are PostgreSQL protocol safety invariants, while vector
capacities exactly preallocate already-known frame lengths. No additional CLI,
environment variable, or CRD field is warranted.

The current all-target gate passed 13 unit and lifecycle tests, and strict
all-target Clippy passed.

## Gres Control Completion

The `gres-control` owner has 30 scanner rows. Its production defaults for
checkpoint triggers, checkpoint maintenance, idle-suspend polling, range-zero
follower polling and rebuild backoff, registry operations, and PgDog behavior
already feed exposed Gres CLI/environment or CRD policy. Their dimensioned
values use `Time` and `ByteSize`; validated counts and integral boundaries use
`refined_type`.

The remaining constants are stable wire, storage, topology, or compatibility
contracts: Kafka protocol codes and transactional identities, registry envelope
versions and typed keys, PgDog's minimum accepted timeout, and its documented
healthcheck-disable sentinel required by scale-to-zero. Test deadlines, test
polls, golden fixtures, and exact collection preallocation are not deployment
policy.

No additional CLI, environment variable, or CRD field is warranted. The
current all-target gate passed 84 tests, and strict all-target Clippy passed.

## Gres Binary Completion

The `gres` owner has 181 scanner rows, overwhelmingly CLI validation,
environment-precedence, and runtime integration fixtures. Its production
local-vacuum cadence, debt, step thresholds, and key budgets are all exposed as
validated CLI options with environment-variable backing. Registry, checkpoint,
idle-suspend, range-zero follower, durable-inspection, WAL recovery/admin/
producer, and FDW DNS policy also enter through typed CLI/environment settings
and the operator's Gres CRD.

The remaining production values are storage/protocol key prefixes, topology
contracts, configured-value derivations, and exact allocation hints. Policy
identified in `gres-fdw`, `gres-ranges`, and `gres-substrate` remains pending
under those owning crates rather than being duplicated here.

No additional binary-owned CLI, environment variable, or CRD field is
warranted. The current all-target gate passed 220 tests, including process-level
runtime, crash, and topology targets, and strict all-target Clippy passed.

## Client Producer

The `client-producer` owner has 177 scanner rows, predominantly test deadlines,
mock-clock advances, and model bounds. All production deployment policy is
already exposed through validated producer-builder inputs: compression, linger,
batch bytes, cross-partition in-flight concurrency, DNS, request and flush
timeouts, resend count and backoff, routing and initialization retry budgets,
and transaction timeout. Dimensioned values are represented as `Duration` at
the public API and converted to `Time` inside the runtime; validated duration,
count, and byte boundaries use `refined_type`.

The bounded sender wake channel is an internal coalescing mechanism. Producers
use best-effort `try_send` for ordinary wakeups, and the sender derives work
from authoritative accumulator state, so its capacity neither limits buffered
records nor controls throughput. The per-partition in-flight limit is pinned to
one, with a compile-time assertion, because raising it without ordered frame
writes can violate idempotent sequence ordering. Neither value is meaningful or
safe deployment tuning.

The remaining production constants are Kafka error codes, sentinel identities,
state tags, the Kafka-compatible Murmur2 hash constants, protocol-unit
conversion bounds, and exact allocation hints. No additional CLI, environment
variable, or CRD field is warranted. The current all-target gate passed 101
unit, runtime, and bounded failover-model tests, and strict all-target Clippy
passed.

## Connect

The `connect` owner has 16 scanner rows. Its only runtime policy is already
exposed through `ConnectorRuntime`: commit interval, maximum batch records, and
caught-up poll backoff each have public builder settings with behavior-preserving
defaults. Connector-specific fields are described by the public typed
configuration system, including duration and secret kinds.

The remaining scanner rows are configuration type tags and test inputs. Watch
channels carry current control and lifecycle state rather than queued work, and
unbounded test channels do not affect deployed behavior. The only production
caller currently supplying fixed runtime overrides is `replicator`; those
values remain pending under that deployment owner instead of being duplicated
inside this library.

No additional library configuration surface is warranted. The current
all-target gate passed 70 unit, derive-contract, and integration tests, and
strict all-target Clippy passed.

## Metadata

The `metadata` owner has 27 scanner rows. Every production value is a
compatibility contract: KRaft private record keys, feature names and supported
levels, metadata-version gates, serialized sentinels, and the deterministic
hash multiplier used to derive stable ACL identifiers. Changing any of these
through deployment configuration would make stored metadata or advertised
protocol capabilities incompatible.

The remaining rows are exact serialization preallocation and test fixtures.
This crate owns no runtime timing, queue, retry, or resource policy, so no CLI,
environment variable, or CRD field is warranted. The current all-target gate
passed 180 unit and evolution tests plus the complete benchmark target, and
strict all-target Clippy passed.

## PostgreSQL Catalog

The `pgcatalog` owner has 47 scanner rows. Every production constant belongs
to its persisted catalog format: key prefixes, schema and record versions,
table/index/sharding flags, index constraints and placements, and scalar/array
type discriminants. These values must remain stable so existing catalog rows
retain their meaning.

The reported collection capacities are bounded preallocation hints and do not
limit accepted catalog data. No runtime timing, queue, retry, or resource policy
exists in this crate, so no CLI, environment variable, or CRD field is
warranted. The current all-target gate passed 36 serialization and catalog
behavior tests, and strict all-target Clippy passed.

## PostgreSQL MVCC

The `pgmvcc` owner has 16 scanner rows. Every production value is an MVCC or
persisted-format invariant: reserved transaction identifiers, the local/global
transaction-id partition, CLOG state tags, tuple record tags, and timestamp
version state tags. Changing any of them through deployment configuration
would change transaction visibility or the meaning of stored rows.

The reported capacities exactly preallocate known serialized headers. Garbage
collection accepts its work cap from the caller and owns no hidden timing,
queue, retry, or resource policy. No CLI, environment variable, or CRD field
is warranted. The current all-target gate passed 54 visibility, serialization,
and reclamation tests, and strict all-target Clippy passed.

## PostgreSQL Types

The `pgtypes` owner has 50 scanner rows. Its production constants are
PostgreSQL compatibility and arithmetic contracts: type and array OIDs,
calendar and epoch conversions, numeric storage/display bounds, JSONB's binary
version, and fixed formatting vocabularies.

The JSONB recursion limit is a process-safety boundary tested against deeply
nested input; allowing operators to raise it would make stack safety tunable.
Reported capacities are exact or input-sized preallocation. The crate owns no
runtime timing, queue, retry, or resource policy, so no CLI, environment
variable, or CRD field is warranted. The current all-target gate passed 218
tests; two external-PostgreSQL oracle tests were explicitly ignored. Strict
all-target Clippy passed.

## Client Core UOM Closure and Remaining Policy

The shared Kafka DNS deadline now uses UOM at every configuration boundary.
`DEFAULT_CLIENT_DNS_TIMEOUT` is a `Time`,
`ClientDnsTimeout::new` accepts `Time`, and `ClientDnsTimeout::time` returns
`Time`. The validated type stores its positive whole-millisecond `i64` value so
operator effective-policy structs retain `Eq`; validation uses
`refined_type::rule::GreaterI64` and rejects non-finite or fractional values.
Only the `tokio::time::timeout` call sites lower the value to
`std::time::Duration`. The producer builder now also accepts `Time` directly,
and the prior `Time -> Duration -> Time` conversions were removed from all
workspace callers.

The current scanner reports exactly 25 `client-core` rows. Eleven are test
inputs. Four named defaults are existing configuration: DNS, connect, request,
and fetch-response limits. Six production rows are fixed: four Kafka API or
bootstrap sentinels, the auth-only GSSAPI receive offer, and one exact
preallocation hint. The four remaining operational rows are now closed at the
generic client boundary:

- `ConnectionDispatchQueueCapacity` is a positive `refined_type` newtype with
  the existing default of `64`, stored in `ConnectionOptions` and applied to
  each connection's Tokio dispatch channel;
- `FetchMinBytes` is a positive whole-byte UOM newtype fitting Kafka's signed
  `i32`, with the existing `1B` default stored by `IsolatedFetch` and copied
  into `FetchRequest.min_bytes`;
- `ClientFrameMax` is a positive whole-byte UOM newtype with the existing
  `100MiB` default and a fixed, non-configurable `100MiB` security ceiling; it
  bounds normal framed reads/writes and SASL request/response frames, with SASL
  response lengths rejected before payload allocation; and
- outbound SASL headers now reuse `ConnectionOptions.client_id`, including
  broker inter-broker connections, so no independent SASL-client-id setting
  exists.

Existing direct `ConnectionOptions` and `IsolatedFetch` constructors select the
typed defaults, preserving wire behavior while preventing invalid state inside
client-core. The reusable library owners now carry typed policy without reading
the environment:

- producer primary, transaction-coordinator, and group-coordinator clients;
- admin initial connections and both reconnect paths;
- streams membership, metadata, fetch, producer, offset, and EOS clients;
- Gres FDW admin/scan clients and isolated fetches;
- Gres WAL admin, producer, end-sampler, replay, and reconstructed readers; and
- Gres registry writer, admin, refresh, reconnecting reader, and fetch paths.

The workspace construction-site inventory is:

```bash
rg -n 'Client::builder\(|Producer::builder\(|ConnectionOptions \{|IsolatedFetch \{' \
  crates --glob '*.rs'
```

Remaining production hits belong to deployed binaries or their private
components (for example rebalancer, remote-storage-topic, schema-registry,
replicator, and gRPC gateway). They remain open for the deployment
CLI/environment phase because that phase must introduce the process owner and
thread its values through those components together. Kafka/Gres CRD fields
remain a separate final ownership phase.

The generic resource-policy gates passed:

- all targets for `crabka-client-core`, `crabka-client-streams`,
  `crabka-gres-control`, `crabka-gres-fdw`, `crabka-gres-substrate`, and
  `crabka-gres`, including their live broker and crash/recovery suites;
- `cargo check --workspace --all-targets --locked`;
- `cargo clippy --workspace --all-targets --locked -- -D warnings`;
- `cargo +nightly fmt --all`; and
- `git diff --check`.

## KRaft Core Snapshot Fetch Audit

The `kraft-core` owner has exactly five scanner rows. Four are test or
deterministic simulator inputs. The sole production value is the one-GiB
snapshot reassembly limit, which bounds memory a follower can be made to retain
while accepting KIP-630 snapshot chunks from its leader.

The limit has both deployment-policy and security roles. The proposed minimal
surface is a validated positive whole-byte `ByteSize` setting that deployments
may lower but not raise above the existing one-GiB hard ceiling. It would flow
through `raft::ControllerConfig`, broker CLI/environment/file configuration,
and the Kafka CRD as `metadata_snapshot_fetch_max`, with
`CRABKA_METADATA_SNAPSHOT_FETCH_MAX` as the environment variable. The default
remains one GiB, preserving current behavior; the ceiling itself remains a
fixed security invariant rather than becoming tunable.

The approved design is implemented. `MetadataSnapshotFetchMax` validates the
UOM value through `refined_type`, requiring a positive whole-byte count at or
below the immutable one-GiB ceiling. The value now flows through
`raft::ControllerConfig`, broker CLI/environment/file configuration, and
`spec.brokerTuning.metadataSnapshotFetchMax`; the operator renders it into the
broker runtime TOML. The generated Kafka CRD is updated. The Raft controller's
separate eight-MiB decoded-log read and snapshot chunk budget belongs to the
Raft owner audit and is not part of this setting.

Closure gates passed:

- all 37 `crabka-kraft-core` tests;
- 151 `crabka-raft` library tests, with the Docker-only snapshot test ignored;
- focused broker validation, CLI/environment, and file-config tests;
- focused operator CRD validation and broker-TOML handoff tests;
- `cargo clippy --workspace --all-targets --locked -- -D warnings`;
- `cargo +nightly fmt --all`; and
- `git diff --check`.

## Raft Runtime Policy Audit

The Raft scanner rows are predominantly tests, protocol identifiers and
versions, serialized layout sizes, sentinels, exact preallocation, and fixed
progress invariants. In particular, the one-byte observer fetch floor prevents
a zero-byte request from permanently stalling, and should not be configurable.

Four runtime-policy findings remain:

- `controller_heartbeat_interval` is already exposed through broker
  CLI/environment/file configuration and the Kafka CRD, but is dropped before
  the KRaft engine. The engine instead derives its cadence as one third of the
  election timeout. Honoring the documented 500-ms default would change the
  current effective default from roughly 1.667 seconds, so this behavior
  correction needs explicit approval.
- Three consecutive fetch misses are tolerated before election. This is a
  distinct failure-detection policy layered on the election timeout.
- The controller actor mailbox has a fixed capacity of 256.
- One eight-MiB value currently couples replication response size, snapshot
  request chunk size, committed-record application, and restart replay.

The proposed minimal design is to thread the existing heartbeat setting into
the engine; add validated positive `controller_fetch_miss_limit` and
`metadata_raft_command_queue_capacity` values; and replace the coupled
eight-MiB value with a UOM `metadata_raft_fetch_max` policy. Application and
replay would iterate over bounded reads until their target offset so lowering
the byte budget cannot silently skip committed metadata. The byte setting
would flow through broker CLI/environment/file configuration and the Kafka CRD
as `CRABKA_METADATA_RAFT_FETCH_MAX`; the two counts would use
`CRABKA_CONTROLLER_FETCH_MISS_LIMIT` and
`CRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY`. Existing effective values remain
the defaults except for the heartbeat discrepancy described above.

This design is pending explicit approval and has not been implemented.

## Log

The `log` owner has 23 scanner rows. Five are existing UOM-backed `LogConfig`
defaults corresponding to Kafka topic policy: segment size and roll interval,
retention, index interval, and delete retention. They already flow through the
topic configuration path and need no new environment variable or CRD field.

The index entry sizes, filename width, transaction/stamp entry widths, undefined
epoch and offset values, and test-only budgets are persisted-format,
compatibility, or test invariants. The four-MiB read-buffer cap limits only an
initial allocation; reads still accept the caller's full budget. The 64-KiB
timestamp scan window grows and repeats until it finds the result or reaches
the end of the segment. Neither changes accepted data or query results, so both
remain fixed implementation details rather than deployment knobs.

The all-target crate gate passed 185 unit tests and two property tests; the
Docker-only JVM integration test was explicitly ignored. Every benchmark
target also completed successfully.

## Metrics UOM Closure and Remaining Policy

The `crabka-metrics` binary now accepts unit-bearing compactor retention and
sweep intervals instead of raw milliseconds. The demo uses `1h` and `30s`,
and every binary option is backed by a `CRABKA_METRICS_*` environment
variable. Positive time settings reject zero or negative values at parsing;
the retention window remains nonnegative because zero disables retention.

The scanner reports 42 rows. Schema column names, WAL and HA topic names,
native-histogram schema bounds, the Prometheus-compatible exemplar and tenant
limits, timestamp safety bounds, encoding tables, and test inputs are fixed
format, compatibility, safety, or test values. Compactor flush thresholds are
already CLI/environment configuration. The remote-read decompression default
is a library fallback for callers that do not supply their own `ByteSize`; no
production route currently calls that decoder, so adding a deployment knob now
would configure unused code.

Three distributor policies remain pending:

- the 30-second HA replica failover timeout;
- the 100,000-tenant ingestion-rate bucket cap; and
- the 32-MiB distributor decompression cap.

The proposed minimal surface is to reuse the existing `Time`, `ByteSize`,
`HaTracker::elect` timeout argument, `IngestEnforcer::with_max_rate_buckets`,
and `DistributorState::with_max_decompressed` paths. The binary would expose
`CRABKA_METRICS_HA_FAILOVER_TIMEOUT`,
`CRABKA_METRICS_INGEST_RATE_BUCKET_CAP`, and
`CRABKA_METRICS_DISTRIBUTOR_MAX_DECOMPRESSED`, preserving current defaults.
The positive count would use a validated `refined_type` newtype; the
dimensioned values remain UOM quantities. No CRD currently owns this
standalone service.

This design is pending explicit approval and has not been implemented. The
completed UOM/env slice passed all 188 library tests, 15 binary tests, six
integration/property tests, and strict all-target Clippy.

## Metrics Service UOM Closure and Remaining Policy

The metrics-service WAL-head retention flag now accepts `Time` directly and
the demo uses `5m` instead of raw milliseconds. Every service option is backed
by a `CRABKA_METRICS_*` environment variable, and positive intervals reject
zero or negative values at parsing.

The scanner reports 12 rows. Eight are tests or embedded test YAML and the
ruler-state topic is a compatibility name. Two runtime policies remain: the
30-second cold-manifest cache TTL and the one-hour lookback substituted for an
unbounded compatibility query.

The proposed minimal surface is to store both `Time` values in
`RefreshingMetricBlockStore` and expose `CRABKA_METRICS_COLD_CACHE_TTL` and
`CRABKA_METRICS_UNBOUNDED_COMPATIBILITY_LOOKBACK` through the existing
standalone binary CLI. Defaults preserve current behavior. No CRD currently
owns this standalone service.

This design is pending explicit approval and has not been implemented. The
completed UOM/env slice passed 25 library tests, 13 binary tests, the
non-Docker integration test, and strict all-target Clippy; three Docker-only
compatibility tests were explicitly ignored.

## Observability UOM Closure and Remaining Policy

Every existing `ServiceConfig` option is now backed by a
`CRABKA_OBSERVABILITY_*` environment variable. The `max_query_length` byte
limit now remains a `ByteSize` from CLI/environment parsing through
`QuerierState` and is lowered to `usize` only where the query string length is
compared. All duration and byte-limit parsers reject negative values, matching
the unsigned raw fields they replaced. Absolute query start and end
nanosecond timestamps remain raw `i64` instants rather than UOM extents.

The scanner reports 43 observability rows. Fixed rows are the compaction
frontier manifest version/path, Kafka quota key, Loki Parquet media type,
metric decimal scale, hexadecimal alphabet, role-operation tables, service
discovery labels, exact time-unit scales, progress guards, preallocation
hints, and test inputs. Loki request-default and compatibility behavior also
stays fixed: the one-hour query range, six-hour metadata range, default tail
limit, 11,000-point resolution ceiling, five-second `delay_for` ceiling, and
30-day-one-hour volume-query ceiling. Clients already control the applicable
request values, and changing these server constants would make the compatible
API diverge rather than tune deployment resources.

The remaining deployment policies are:

- distributor ingest age, future-timestamp grace, and quota burst window;
- distributor WAL dependency startup deadline, per-attempt timeout, and
  initial/maximum retry backoff;
- compactor WAL poll timeout, accumulation window, accumulation poll timeout,
  maximum records per batch, idle interval, and initial/maximum object-store
  retry backoff;
- querier compaction-frontier refresh interval, dynamic-index cache TTL,
  shard-index cache TTL, shard-fetch concurrency, cold-block fetch
  concurrency, hot-tail bucket width, hot-tail poll/delivery interval, and
  dependency reconnect interval.

The proposed minimal surface adds these values directly to the existing
role-selectable `ServiceConfig`, preserving every current value as its
default. Times remain `Time`; byte extents remain `ByteSize`; positive
concurrency and batch counts use `NonZeroUsize`. The two equal 50-ms hot-tail
poll/backoff/delivery cadences share one setting, and the two equal 500-ms
querier WAL/authorizer reconnect cadences share one setting; unrelated equal
numbers remain separate. Each field gets a `CRABKA_OBSERVABILITY_*`
environment variable and is supplied only to its owning demo role. No CRD
currently owns this standalone service.

This design is pending explicit approval and has not been implemented. The
completed UOM/env slice passed all 50 library tests, 10 CLI tests, the
environment test, 35 compactor tests, 480 HTTP tests, 69 querier tests, and
strict all-target Clippy.

## Kafka Record Decompression Policy

The `records-legacy` scanner has five rows. The compression mask and timestamp
bit remain fixed Kafka v0/v1 wire-format invariants. Its former three
decompression-budget constants were deployment policy and have been removed.

The current `protocol` scanner has 2,223 rows across 455 files. Generated
output accounts for 2,172 rows across 440 files: 2,050 API-key/version
constants and 122 schema defaults, populated-value helpers, and differential
table entries. These values are generated from the checked-in Kafka schemas;
they describe the wire contract or supply test values. Operational callers
own and override request timeouts, byte limits, and intervals rather than
configuring the codec's schema defaults.

The remaining 51 rows span 15 handwritten files: four request-trait associated
constant declarations, 18 wire/layout invariants, and 29 test fixtures. The
invariants are varint widths, UUID and record-header sizes, attribute bits,
CRC ranges, protocol version thresholds, metadata/control/envelope versions,
and exact allocation sizes derived from those formats. None is deployment
policy. The duplicated 100x/16-MiB/1-GiB decompression budgets in the modern
v2 borrowed and owned record decoders were the only operational policy owned
by this crate and have been removed.

Modern and legacy records are two encodings of the same broker traffic class,
so one `RecordDecompressionPolicy` in `crabka-compression` now owns both
decoding paths. It contains UOM `Ratio` and `ByteSize` values, uses
`refined_type` for positive whole-byte validation, and preserves the existing
100x ratio, 16-MiB floor, and 1-GiB ceiling defaults. Operators may lower the
ratio and ceiling but cannot raise the fixed 100x and 1-GiB security bounds.

Broker configuration, CLI/environment, and `BrokerTuning` expose:

```text
record_decompression_max_ratio
record_decompression_output_floor
record_decompression_output_ceiling
CRABKA_RECORD_DECOMPRESSION_MAX_RATIO
CRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR
CRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING
```

The Kafka CRD uses camel-case equivalents and the operator renders the same
broker `[runtime]` keys. Produce constructs the validated policy once per
request and supplies it only to owned modern/legacy fallback decoding; the
header-only v2 verbatim path remains unchanged. Public protocol and legacy
decode entry points retain default-compatible wrappers, with explicit
policy-aware variants for the broker trust boundary.

Telemetry decompression remains independently configurable because it is a
different traffic class. `records-legacy` and `protocol` are complete. No
additional protocol CLI, environment variable, or CRD field is warranted.
Caller-owned request policy remains classified with each service; the next
scanner-visible unresolved owner is the rebalancer's reassignment request
timeout.



## Telemetry UOM Closure and Profiling Policy

`OtlpConfig` now retains `CRABKA_OTLP_TIMEOUT` and
`CRABKA_OTLP_HEARTBEAT_INTERVAL` as UOM `Time` values. Conversion to
`std::time::Duration` happens only at the OpenTelemetry exporter and Tokio
sleep boundaries. The standard `OTEL_EXPORTER_OTLP_TIMEOUT_SECS` spelling and
integer-seconds interpretation remain an external OpenTelemetry compatibility
contract, but are lifted into `Time` immediately.

The telemetry scanner now reports only the two W3C propagation header names,
`traceparent` and `tracestate`; both are fixed protocol identifiers. A manual
profiling-route review found policy outside the scanner:

- CPU profile default and maximum durations, plus sampling frequency;
- heap profile activation default and maximum durations; and
- the native-frame blocklist.

The request already controls profile duration within the current bounds.
Duration defaults/caps, sampling frequency, and the blocklist remain pending
deployment policy; gzip format/compression, pprof media type, route names, and
allocation hints remain fixed implementation/API behavior. A shared profiling
configuration should be threaded through the existing router constructors and
flattened into each owning binary's CLI/environment boundary rather than read
from process-global environment inside request handlers.

The UOM closure passed all 30 all-target tests and strict all-target Clippy.
The profiling design is pending explicit approval, so `telemetry` remains
Pending in Coverage Status.

## Security GSSAPI Clock-Skew Policy

The `security` scanner has 23 rows. Example fixture credentials/timing,
Kerberos enctype and security-layer identifiers, W3C/JWT claim names,
cryptographic key sizes, SCRAM compatibility bounds, PostgreSQL verifier
prefixes, allocation hints, and test data are fixed. The one-slot JWKS refresh
signal is deliberate coalescing semantics: validators only need to record that
at least one refresh is pending.

SCRAM credential iterations and OAuth clock skew/cache expiry already flow
through their owning configuration. The accept-path KDC URL fallback is
required only to construct the SSPI server context; that path performs no KDC
network I/O, so making the fallback separately tunable would not change
runtime behavior.

The remaining deployment policy is the five-minute maximum clock skew used
when validating incoming Kerberos AP-REQs. The proposed minimal design adds
`max_time_skew: Time` to the existing `GssapiConfig` and
`FileGssapiConfig`, preserving five minutes as the default and accepting a
nonnegative UOM duration. `SspiAcceptor` receives that value explicitly and
lowers it to `std::time::Duration` only at the SSPI boundary.

The Kafka CRD's existing `ListenerAuthenticationGssapi` object would expose
the same unit-bearing field and the operator would render it into the existing
broker-global `[gssapi]` TOML block. No new configuration subtree or
environment-only library lookup is needed.

This design is pending explicit approval and has not been implemented. All 208
unit tests and the SCRAM benchmark targets pass; the external-KDC integration
test remains intentionally ignored. Strict all-target Clippy passes.

## Replicator Runtime and Topic Policy

The `replicator` scanner has 23 rows. Kafka error codes, MM2 record versions
and internal topic names, the provenance header, epoch timestamp handling,
test fixtures, and serialization goldens are fixed. The production deployment
policy currently hidden in constants or literals is:

- topic-creation timeout;
- source and internal-drain poll timeouts plus consecutive-empty drain count;
- worker-build retry budget and initial/maximum backoff;
- connect commit interval and maximum batch size;
- supervisor, heartbeat, and checkpoint intervals; and
- target-data and internal-topic replication factors.

The equal 500-ms defaults serve different source-poll, drain-poll, and commit
semantics and therefore remain separate settings. Defaults preserve all
current timing/count values and single-replica behavior.

The proposed minimal design threads one `ReplicatorRuntimePolicy` through the
existing supervisor/worker/task constructors. Times remain positive UOM
`Time`; positive counts and batch sizes use `NonZeroUsize`; replication
factors use the repository's existing validated positive Kafka replication
type. The standalone binary exposes direct flags backed by
`CRABKA_REPLICATOR_*` environment variables rather than adding another YAML
subtree solely for Kubernetes overrides.

The hardcoded one-partition shape for replicated data topics should not become
a knob. The supervisor already reads source metadata, so it should retain each
source topic's partition count, create the target with that count, and preserve
the source partition on each produced record. Replicator internal topics remain
single-partition for ordering/compatibility, while their replication factor is
configurable.

This design is pending explicit approval and has not been implemented. All 42
all-target tests, including recovery and two-cluster behavior, pass. Strict
all-target Clippy passes.

## Gres Conformance Harness

The `gres-conformance` scanner has 17 rows. Report and fixture schema
versions, pinned PostgreSQL/PgDog artifacts, SQL probe manifests and
SQLSTATEs, the case identifier token, protocol encodings, golden driver
versions, test counts, and test data are compatibility fixtures rather than
deployment policy. The 16-MiB backend-message ceiling is a local capture
safety bound paired with its boundary test, not a production server limit.

The harness already exposes its meaningful inputs directly through CLI
arguments: endpoints, corpus and baseline paths, output paths, and subject
execution modes. It has no runtime service or CRD owner, so adding Kubernetes
environment aliases would create dead configuration.

No configuration change is needed. All 43 all-target tests and strict
all-target Clippy pass.

## Gres Ranges Runtime Policy

The `gres-ranges` scanner has 79 rows. Range/coordinator zero identifiers,
range-map and metadata format versions, FNV constants, the range-WAL frame
version, wire token widths, hexadecimal encoding, protocol bounds, allocation
hints, synchronization sentinels, and inline test values are fixed. The SQL
chunk target remains derived from the frame ceiling rather than becoming an
independent knob.

The real deployment policy is:

- range RPC frame size, request timeout, server idle timeout, client-pool idle
  TTL, and maximum pooled connections per endpoint;
- hosted remote-session idle retention and maximum session count;
- range-0 catch-up and whole-reply budgets plus the cross-range lock-wait cap;
- durable-inspection page record and byte ceilings;
- decision-release lag retry count and retry backoff; and
- timestamp-oracle heartbeat cadence, logical horizon base/maximum stride and
  persistence cadence, and HLC horizon headroom.

The proposed minimal design keeps these values in the existing Gres compute
configuration boundary. `ServeArgs` exposes unit-bearing
`CRABKA_GRES_RANGE_*` CLI/environment options, `RangeRpcRuntimeConfig` carries
the validated effective policy into `crabka-gres-ranges`, and
`GresComputeSpec` exposes the same fields in the existing CRD compute object.
Times and sizes remain UOM `Time` and `ByteSize`; positive counts and strides
use repository `refined_type` aliases.

Validation preserves the current coupled invariants: the lock and barrier
reply budgets remain below the RPC timeout, pooled connections expire before
the server idle timeout, the frame ceiling remains larger than its fixed
encoding envelope, and logical base stride does not exceed its maximum.
Defaults preserve 1 MiB frames, five-second RPCs, one-minute server/session
idle periods, five-second pool TTL, 32 pooled connections, 1,024 hosted
sessions, ten/four/two-second barrier and lock budgets, 4,096 records and
128 KiB per durable-inspection page, ten 200-ms release retries, 10-ms TSO
heartbeats, 100-ms logical persistence pacing, 1,024/2^24 logical strides, and
128-ms HLC horizon headroom.

This design is pending explicit approval and has not been implemented. All 426
all-target tests pass, with one timing benchmark intentionally ignored. Strict
all-target Clippy passes.

## Pgkv Fjall Policy

The `pgkv` scanner has 33 rows. Key/index identifiers, FNV constants, row and
notification encoding tags and versions, PostgreSQL notification size limits,
exact key/token allocation widths, the in-memory restore chunk, and test
values are fixed implementation or compatibility behavior.

Two Fjall settings are workload policy: the 8-MiB memtable cap and the
262,144-operation rotation threshold. The rotation threshold already reads
`CRABKA_PGKV_ROTATE_AFTER_OPS` from inside the library, but invalid values are
silently replaced and no owning CLI or CRD validates or carries it.

The proposed minimal design adds one `FjallOptions` value containing UOM
`max_memtable_size: ByteSize` and a `refined_type` positive
`rotate_after_ops`. Default `FjallKv::open` behavior remains available for
library/test callers; Gres passes explicit options to its durable and
disposable-cache stores. The existing environment spelling backs a new
`--pgkv-rotate-after-ops` argument, and
`CRABKA_PGKV_MAX_MEMTABLE_SIZE` backs `--pgkv-max-memtable-size`.
`GresComputeSpec` exposes both fields in the existing compute CRD object.
Defaults and Fjall block partitioning/pinning behavior remain unchanged.

This design is pending explicit approval and has not been implemented. All 80
all-target tests and strict all-target Clippy pass.

## Promql Query Policy

The `promql` scanner has 49 rows. Storage/schema and planner column names,
Prometheus stale-NaN bits, FNV constants, HTTP range-boundary tags, function
names, exact allocation hints, the 11,000-point Prometheus compatibility
ceiling, and tests are fixed. The six-hour in-memory retention default already
has explicit constructor/setter overrides, and the metrics service supplies
its production WAL-head retention, so no library-global retention setting is
needed.

The remaining deployment policy is the engine's five-minute lookback,
one-minute default subquery evaluation interval, 50,000,000-sample query cap,
and the 64-MiB compressed/decompressed remote-read body cap. The proposed
minimal design keeps these in `EngineOpts`/`PrometheusApiState` and adds
metrics-service CLI arguments backed by:

```text
CRABKA_METRICS_QUERY_LOOKBACK_DELTA
CRABKA_METRICS_QUERY_EVAL_INTERVAL
CRABKA_METRICS_QUERY_MAX_SAMPLES
CRABKA_METRICS_REMOTE_READ_MAX_BODY
```

Times and the body cap remain UOM `Time`/`ByteSize`; the positive sample count
uses a `refined_type` newtype. The existing runtime per-tenant overrides still
apply their stricter limits. No CRD currently owns the standalone metrics
service.

This design is pending explicit approval and has not been implemented. All 559
all-target tests and strict all-target Clippy pass.

## Traceql Engine Policy

The `traceql` scanner has 42 rows. Span/block column names, intrinsic/tag
names, ID widths, metadata markers, hexadecimal encoding, allocation hints,
and tests are fixed. Tempo's omitted-argument `compare()` top-N remains
compatibility behavior.

The existing `EngineOpts` already owns search defaults and caps. The missing
deployment surface is its 20-trace default result limit, three spans per span
set, 1,000-trace maximum, disabled-by-default metric exemplars, the
256-distinct-values-per-attribute compare cap, and the default duration
histogram buckets. The Traces binary currently exposes only the trace and
exemplar maxima, without environment backing.

The proposed minimal design reuses and extends `EngineOpts`, with positive
counts validated by repository `refined_type` aliases and histogram
boundaries stored as ordered positive UOM `Time` values. The existing
`crabka-traces` flags remain compatible and every field receives a
`CRABKA_TRACES_TRACEQL_*` environment-backed spelling. Defaults preserve the
current counts and the 2-ms-through-16.384-second doubling bucket layout. No
CRD currently owns the standalone Traces service.

This design is pending explicit approval and has not been implemented. All 400
all-target tests and strict all-target Clippy pass.


## Security GSSAPI Policy

The `security` scanner has 17 rows. GSSAPI encryption and security-layer
codes, OAuth reserved claim names, PostgreSQL SCRAM verifier shape, exact
elliptic-curve point allocation, single-slot coalescing channels, examples,
and test fixtures are fixed. SCRAM's 4,096-to-16,384 Kafka acceptance range is
wire-compatible validation, while each credential's iteration count is
already an explicit KafkaUser/GresTenant input with an 8,192 default.

The one hidden deployment policy is the five-minute clock-skew tolerance used
to validate incoming Kerberos AP-REQs. The proposed minimal design adds UOM
`allowable_clock_skew: Time` to the existing `GssapiConfig`, broker TOML, and
`ListenerAuthenticationGssapi` CRD object, preserving five minutes as the
default and rejecting negative values.

The acceptor's `tcp://localhost:88` URL is a constructor placeholder required
by `sspi`; the accept path does not contact a KDC. It should remain fixed
rather than be a fake tuning surface. The client initiate path already takes
the configured KDC endpoint explicitly.

The KafkaUser CRD currently advertises SCRAM iteration values through
1,000,000 even though the broker correctly rejects values above 16,384. Its
schema and generated CRD should be narrowed to the broker limit so invalid
configuration fails at admission rather than reconciliation.

This design is pending explicit approval and has not been implemented. All
208 all-target tests and every benchmark check pass; one KDC-backed test is
explicitly ignored. Strict all-target Clippy passes.

## Profiles Runtime Policy

The `profiles` scanner has 63 rows. Generated-file lists and source markers,
profile/WAL encoding identifiers, Kafka partition zero, FNV and hexadecimal
constants, exact time-unit conversion, internal labels/messages, upstream
tenant-id compatibility, allocation hints, and test values are fixed. Tenant
ingest/query limits and their Pyroscope-compatible defaults are already
configurable through the existing per-tenant limit files.

The recently added WAL fetch, poll, and block-index snapshot settings are
UOM-backed CLI/environment options, but they do not complete this owner. The
remaining deployment policy is:

- the shared profiles WAL topic, block-builder consumer group, and profile
  index object key;
- profile-index refresh interval;
- distributor request/decompressed-body cap and tracked-tenant cap;
- legacy tree/trie node, materialized-path byte, and depth budgets;
- hot WAL-tail retention age and record cap;
- query heatmap value-bucket default and time-bucket ceiling; and
- compactor job size/downsample resolution, block-builder flush size/age, and
  query-frontend shard width already exposed by primitive CLI values.

The distributor raw HTTP, Connect receive, and decompression paths must share
one body-size setting rather than expose three knobs. The hot-store rebuild
factor remains an internal amortization choice, and the fixed tenant-ID length
continues to match the upstream API. Compactor jobs require at least two
blocks; other positive counts should reject zero instead of silently clamping
it.

Every existing deployment argument also needs environment backing, including
target, listen/bootstrap endpoints, object-store URL, limit-file paths, WAL
consumer groups, compactor and block-builder settings, query shard width, and
debuginfod URLs. Existing `*-ms` and `*-ns` CLI spellings remain compatibility
aliases; new canonical options and all new environment variables use
human-unit UOM `Time`/`ByteSize` values. Positive counts and non-empty
identifiers use repository `refined_type` newtypes.

No CRD owns the standalone Profiles roles. The proposed minimal implementation
adds the validated options directly to `crabka-profiles` and threads only each
role's applicable values through its existing state/config type. Defaults
preserve the current topic/key/group names, 15-second index refresh, 16-MiB
request budget, 4,096 tracked tenants, 500,000 tree nodes, 64-MiB materialized
paths, 4,096 trie depth, six-hour/1,000,000-record hot retention, 32 value
buckets, 4,096 time buckets, eight-block compaction, 1,024-record/ten-second
flushes, and 15-minute query shards.

This design is pending explicit approval and has not been implemented. All
239 all-target tests pass; four external integration tests are explicitly
ignored. Strict all-target Clippy passes.

## Telemetry and Profiling Policy

The telemetry scanner reports only `traceparent` and `tracestate`; both are
fixed W3C protocol identifiers. Default OTLP endpoints/ports and protocol
spellings follow the OpenTelemetry contract, while heartbeat trace/span ID
prefixes, gzip encoding, and allocation hints are internal format or
implementation choices.

OTLP endpoint, protocol, filter, timeout, heartbeat cadence, service name, and
sampling ratio already have environment surfaces. This branch keeps timeout
and heartbeat as UOM `Time` values, with human-unit
`CRABKA_OTLP_TIMEOUT`/`CRABKA_OTLP_HEARTBEAT_INTERVAL` inputs and the standard
OTel seconds input retained for compatibility. The sampling ratio still parses
to raw `f64`, silently ignores malformed values, and clamps out-of-range
values; it should instead use validated UOM `Ratio` and reject invalid
configuration.

The hidden pprof deployment policy is the 99-Hz CPU sample frequency and the
60-second CPU/30-second heap request ceilings. The 30-second CPU and
five-second heap durations used when a request omits `seconds` are
pprof-compatible API defaults; the one-second minimum is request validation.
The proposed minimal design adds a small `ProfilingConfig` carrying UOM
`Frequency` and `Time` values into `pprof_router`/`serve_admin`. Each owning
binary exposes its applicable fields as CLI arguments backed by
`CRABKA_PROFILING_*` environment variables; no library-global environment
lookup is added.

No CRD owns the cross-service diagnostics endpoint. Defaults preserve 99 Hz,
60 seconds, and 30 seconds.

This design is pending explicit approval and has not been implemented. All 30
all-target tests and strict all-target Clippy pass.

## Pgwire Runtime Policy

The `pgwire` scanner has 30 rows. PostgreSQL protocol/version/request codes,
message field widths, SQLSTATEs, type OIDs, special packet lengths, the exact
10,000-byte PostgreSQL startup-packet compatibility cap, SCRAM salt/hash
shapes, server parameter values, allocation hints, and stub/test data are
fixed.

The 64-MiB regular frontend-message ceiling is Gres deployment policy. The
proposed minimal design adds UOM `max_message_size: ByteSize` to a small
pgwire decode policy, supplied by Gres CLI/environment and
`GresComputeSpec` as:

```text
pgwire_max_message_size
CRABKA_GRES_PGWIRE_MAX_MESSAGE_SIZE
```

The ad-hoc standalone Gres `--auth scram --user-cred` path also hardcodes
PostgreSQL's 4,096-iteration verifier default. It should accept a validated
`--pgwire-scram-iterations` backed by
`CRABKA_GRES_PGWIRE_SCRAM_ITERATIONS`, while tenant-backed authentication
continues to use the iteration count embedded in each configured verifier.
Unknown-user mock verifiers must derive the applicable iteration count from
the real verifier policy rather than gain an independent knob.

The existing connection-cap, authentication-timeout, and cumulative-COPY
safeguard TODO is not a hardcoded value and is outside this constant-removal
slice; inventing defaults for it would change current behavior. Defaults here
preserve 64 MiB and 4,096 iterations.

This design is pending explicit approval and has not been implemented. All
106 all-target tests and strict all-target Clippy pass.

## Pgexec Runtime Policy

The `pgexec` scanner has 80 rows. PostgreSQL catalog OIDs, database/SQL
compatibility values, datetime units, field and NOTIFY payload/name limits,
HLC bit layout, persisted key prefixes, recursion guards, allocation hints,
logging rate limits, cooperative-yield cadence, and test values are fixed.
The local-vacuum pacing budget and intervals already have validated Gres
CLI/environment overrides.

The remaining deployment policy is:

- blocking-query memory and distributed-join broadcast threshold;
- distributed-join key/projection/predicate, snapshot-XID, broadcast-row,
  row-byte, and result-row limits;
- per-session notification queue capacity;
- durable XID and sequence reservation block sizes; and
- timestamp-version prune count per written row and reclaim-floor lag.

The one-MiB streamed result page ceiling should derive from the configured
range transport frame budget rather than become an independent knob. Likewise,
clog deletion batches should consume the existing local-vacuum key budget
instead of adding another batch-size setting.

The proposed minimal design adds one defaultable `PgExecRuntimePolicy` to
`SqlEngine` construction. Gres carries its fields through existing
CLI/environment and `GresComputeSpec` boundaries. Byte extents remain UOM
`ByteSize`, the reclaim lag remains UOM `Time`, and positive counts use
repository `refined_type` newtypes. Validation keeps the join broadcast
threshold and encoded request/result bounds within the configured range
transport budget.

Defaults preserve 16-MiB blocking memory, 64-MiB broadcast selection, 16 join
keys, 256 projections/predicates, 65,536 snapshot XIDs/result rows, 8,192
broadcast rows, 256-KiB rows, 16,384 queued notifications, 1,024-entry XID and
sequence reservations, 64 opportunistic prunes per row, and a five-second
timestamp-GC floor lag.

This design is pending explicit approval and has not been implemented. All
988 all-target tests and strict all-target Clippy pass.

## Gres Balancer Policy

The `gres-balancer` scanner has four rows. The two expected JSON strings and
the two-minute timestamp are test inputs. The remaining production expression,
`stride.max(1)`, is a defensive arithmetic invariant that prevents an
unbounded range split from selecting its existing lower bound; zero is not a
meaningful deployment policy.

All actual planner policy is already explicit. `BalancerConfig` carries goal
enablement plus range-size, split-stride, load-skew, range-count,
operation-count, and cooldown settings. `TablePolicy` carries the per-table
auto-sharding thresholds. The standalone Gres CLI reads these values from its
JSON planning input, while the dry-run-only Gres CRD exposes the fleet
thresholds used with externally supplied plan snapshots. Snapshot freshness is
also an explicit UOM `Time` argument at the provider-planning boundary rather
than a library default.

No process in this crate owns an implicit deployment environment, and the
operator does not currently invoke the planner. Adding CLI/environment
variables inside the library or wiring unused CRD values into a nonexistent
operator planning loop would add behavior rather than expose a hardcoded
constant. No configuration change is warranted.

All 20 all-target tests and strict all-target Clippy pass.

## Observability Demo Application Policy

The `observability-demo-app` scanner has ten rows. Its generated protobuf
descriptor, category/region/warehouse/payment/tier vocabularies, and
deterministic fixture values are the definition of the repeatable demo
scenario. The anomaly and fraud cases, topology store name, stage names,
relative synthetic stage work, and periodic progress log are likewise demo
content rather than deployment safety or capacity policy.

The existing deployment surface is incomplete in four places:

- input/output topics are CLI-only, while the Streams application ID and
  consumer group ID are still embedded strings;
- the producer rate is exposed as a raw `orders_per_sec: u32` even though the
  runtime immediately converts it to UOM `Frequency`;
- the consumer loop embeds a 500-ms poll timeout; and
- the four simulated processing-stage delays are hidden workload-shaping
  policy.

The proposed minimal surface adds environment backing for the existing topic
arguments, exposes application/group identity, parses the canonical producer
rate as UOM `Frequency`, exposes the poll timeout as UOM `Time`, and exposes
the four stage delays as UOM `Time`. The current
`CRABKA_DEMO_ORDERS_PER_SEC` bare integer remains a compatibility input; the
canonical option and environment variable use a human frequency value.
Defaults preserve `orders`, `order-counts`, `orders-analytics`,
`orders-processor`, 50 orders/second, a 500-ms poll, and 150/400/200/300
microseconds of validate/enrich/fraud-check/fulfill work. Zero remains valid
for the producer rate and stage work because it intentionally pauses
production or disables simulated latency.

This standalone demo has no CRD owner. The existing admin listener already has
environment override support through the shared telemetry helper.

This design is pending explicit approval and has not been implemented. The
stale compose contract assertion found by the gate was updated to match this
branch's already-approved human-unit metrics compactor flags. All 64
all-target tests and strict all-target Clippy pass.

## Gres Substrate Policy

The `gres-substrate` scanner has 64 rows. Most are test values or durable
contracts: GRW1 and checkpoint manifest versions, encoded key/tag widths,
Kafka protocol codes, the single-partition tenant WAL layout, recovery
sentinels, monotone offset clamps, and exact allocation hints. The one-permit
commit/checkpoint gates enforce serialization. The eight-slot checkpoint
control channel only bounds callers waiting for that serialized service, and
the one-byte planner normalization represents an existing empty checkpoint;
neither is independent deployment policy. The raw-KV split runtime explicitly
exists only to exercise durable split seams, so its tiny checkpoint values are
fixed test-runtime inputs.

Checkpoint thresholds, object part size, retained manifests, polling and
delete-records deadlines, durable-fold record/byte/deadline limits, WAL
recovery fetch/response/retry/DNS/connect/request policy, WAL topic
replication and admin timeouts, and WAL producer retry/throughput/flush/DNS
policy are already explicit. Gres supplies them through validated
CLI/environment options and, where fleet-owned, UOM-backed `GresComputeSpec`
fields.

The remaining deployment value is `DEFAULT_MAX_FRAME_SIZE`: the one-MiB target
used to split one logical commit into GRW1 WAL records. The library already
supports overriding it with `SubstrateCommitter::with_max_frame_size`, but the
Gres construction path always leaves the default in place. This is not the
producer's 16-KiB batching target: a logical WAL record may exceed a producer
batch, and the frame target independently controls journal granularity and
recovery work.

The proposed minimal surface is a positive UOM `ByteSize` value named
`wal_frame_max_size`, exposed as `--wal-frame-max-size`,
`CRABKA_GRES_WAL_FRAME_MAX_SIZE`, and `GresComputeSpec.walFrameMaxSize`, then
passed to every live `SubstrateCommitter`. The default remains one MiB. The
existing behavior that allows one indivisible operation to exceed the target
is preserved; the setting remains a chunking target rather than pretending to
be a hard rejection limit.

This design is pending explicit approval and has not been implemented. All
204 all-target tests and strict all-target Clippy pass.

## Gres Load-Test Harness Policy

The `gres-loadtest` scanner has 72 rows. Test fixtures account for most of
them. The isolated tenant/database/TLS/broker identities, range-to-table ID
stride, worker ID partition, fault action names, Linux `/proc` constants, and
exact allocation hints are fixed harness contracts. The one-second sampler
and timeline cadence define the durable report schema; histogram resolution
and bounds match the workspace benchmark format; proxy chunking/burst
mechanics, the minimum flap period, and Markdown elision thresholds keep runs
comparable rather than represent target-cluster deployment policy.

Scenario YAML already owns topology, CPU allocation, HLC skew, warmup and
measured durations, connection count, UOM transaction rate, operation mix,
contention shape, and fully dimensioned fault timing/rate. Registry policy is
already validated UOM CLI/environment configuration and is passed to spawned
Gres nodes.

The remaining workload policy is the read slice and schema seed batch sizes,
serialization retry count/backoff/jitter, per-operation and connection
timeouts, startup retry deadline/cadence, worker shutdown grace, and reconnect
backoff floor/ceiling. These values change what a run measures, so the
proposed minimal design adds them to a defaultable `workload.runtime` scenario
section. Time values remain UOM `Time`; positive/non-negative counts use
repository `refined_type` newtypes. Defaults preserve 1,024 read rows, 500-row
seed batches, five serialization retries with the existing two-millisecond
step and two-millisecond jitter, 30-second operations, five-second
connections, a 30-second/250-millisecond startup loop, five-second shutdown,
and 100-millisecond/two-second reconnect bounds.

Child launch/reap/log-drain deadlines, broker readiness polling, and diagnostic
log-tail length are host-harness policy rather than workload semantics. A
shared `HarnessPolicy` should expose them to `run` and `compare` as CLI options
backed by `CRABKA_GRES_LOADTEST_*` environment variables, using UOM `Time` and
validated positive counts. Defaults preserve two minutes, ten seconds, five
seconds, 100 milliseconds, and 40 lines. The separate hardcoded 30-second WAL
topic creation timeout should be deleted and reuse the already-configured
registry topic-create timeout.

This test harness has no CRD owner. This design is pending explicit approval
and has not been implemented. All 100 runnable all-target tests pass; one
end-to-end cluster test requiring prebuilt binaries is explicitly ignored.
Strict all-target Clippy passes.

## Units and Broker Test Facade

The `units` scanner has 17 rows. Every value is a mathematical or
serialization contract: additive/multiplicative identities, SI and IEC scale
factors, exact wire sentinels, and the stable unit table and floating-point
tolerance used to round-trip human configuration values. Making any of these
deployment-configurable would make the meaning of existing configuration
itself mutable.

The nested unpublished `broker/test-helpers` workspace package contains only a
re-export of the broker with its test-helper feature activated. It has no
runtime values or production configuration owner.

No CLI, environment variable, or CRD field is warranted. All 25 `units`
all-target tests pass; the facade has no tests. Strict all-target Clippy passes
for both packages.

## Remote Storage Topic Client Policy

The topic-backed production metadata client now exposes all six operational
settings end to end:

| Policy | Rust field | TOML field | Kafka CRD field | Preserved default |
|---|---|---|---|---|
| topic creation timeout | `topic_create_timeout` | `topic_create_timeout` | `topicCreateTimeout` | `30s` |
| fetch maximum wait | `fetch_max_wait` | `fetch_max_wait` | `fetchMaxWait` | `500ms` |
| fetch byte budget | `fetch_max_bytes` | `fetch_max_bytes` | `fetchMaxBytes` | `1MiB` |
| failed-fetch backoff | `fetch_retry_backoff` | `fetch_retry_backoff` | `fetchRetryBackoff` | `200ms` |
| event queue capacity | `event_queue_capacity` | `event_queue_capacity` | `eventQueueCapacity` | `1024` |
| RLMM snapshot cadence | `snapshot_interval` | `snapshot_interval` | `snapshotInterval` | `1m` |

The five dimensioned settings remain UOM `Time` or `ByteSize` values through
the public, broker TOML, and CRD boundaries. Queue capacity is represented by
`MetadataEventQueueCapacity`, constructed through
`refined_type::rule::GreaterUsize<0>`. Broker TOML owns the standalone surface
under `[remote_storage.kafka_metadata]`; the operator owns deployed policy
under `Kafka.spec.tieredStorage.metadataManager.topic`. No CLI or environment
surface is added because neither owns this deployment policy.

One broker mapper applies the five transport settings to both the RLMM
metadata log and the diskless WAL-index log. Only the RLMM receives
`snapshot_interval`. Direct construction is still validated at the
remote-storage topic, broker, file-config, and CRD boundaries.

The metadata topic name, cleanup and retention settings, partition hashing,
request and high-water-mark sentinels, topic-id checks, assignment semantics,
snapshot format version and filename, serialization allocation hint, security
derivation, and the in-memory fixture capacity remain fixed. They are
protocol, durable-format, security, deterministic-routing, or test-fixture
behavior rather than deployment policy.

Verification used:

```text
cargo test -p crabka-remote-storage-topic --all-targets --locked
cargo test -p crabka-broker --all-targets --locked
cargo test -p crabka-operator --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
cargo run -p crabka-operator --locked -- gen-crds <temporary-directory>
tools/audit-runtime-values.sh
```

The deployed Kafka CRD is byte-for-byte identical to fresh generated output.
The first broker all-target run exhausted this host's 1,024-file-descriptor
soft limit; every named failure passed in isolation, and the complete suite
passed unchanged with a per-process limit of 8,192. The focused
`remote-storage-topic` scanner remains at 25 rows across 5 files. Those rows
are the exposed named defaults, the already-configurable partition and
replication defaults, or the fixed semantics and tests described above. The
full scanner reports 5,592 rows across 1,037 files.

## Schema Serde Retry Policy

The schema-cache background fetch retry range is now one opaque,
cross-field-validated `SchemaFetchRetryPolicy`. Its UOM `Time` bounds are
`initial_backoff` and `max_backoff`; the public defaults are
`DEFAULT_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF` (`10ms`) and
`DEFAULT_SCHEMA_FETCH_RETRY_MAX_BACKOFF` (`1s`). `CacheConfig` owns the policy
as `fetch_retry_policy`. Both bounds must be positive and representable as
`std::time::Duration`, the initial bound may not exceed the maximum, and equal
bounds remain valid.

The observability demo exposes:

```text
--schema-fetch-retry-initial-backoff
--schema-fetch-retry-max-backoff
CRABKA_DEMO_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF
CRABKA_DEMO_SCHEMA_FETCH_RETRY_MAX_BACKOFF
```

Gres exposes the same CLI names with:

```text
CRABKA_GRES_SCHEMA_FETCH_RETRY_INITIAL_BACKOFF
CRABKA_GRES_SCHEMA_FETCH_RETRY_MAX_BACKOFF
```

The Gres CRD owns the fleet boundary at
`Gres.spec.compute.schemaFetchRetryInitialBackoff` and
`Gres.spec.compute.schemaFetchRetryMaxBackoff`. Both generated schema
properties are optional strings. Defaults remain `10ms` and `1s` at every
boundary.

Client Streams reuses its existing `cache_config` builder input. The
observability demo constructs one validated policy and supplies it to its
producer, stream, and consumer schema caches. Gres validates CLI/environment
inputs once, stores the policy on `KafkaFdw`, and applies it to every
per-scan `SchemaCache`. The operator overlays the two optional CRD values onto
the library defaults, calls the authoritative constructor, and renders the two
Gres CLI arguments exactly once for every compute deployment. No library-global
environment lookup or duplicate Client Streams retry fields were added.

The Confluent media type and magic byte remain fixed wire/API compatibility.
The 64-reference traversal ceiling remains a cycle/pathology safety bound.
Transient/terminal error classification, exponential doubling, exponent cap
`7`, and deterministic zero-to-25-percent per-ID jitter remain fixed algorithm
semantics. The remaining one-second cache timeout is test-only.

Post-format verification passed 2,018 affected all-target tests:

```text
crabka-schema-serde: 41
crabka-client-streams: 618
observability-demo-app: 71
crabka-gres-fdw: 59
crabka-gres: 252
crabka-operator: 977
```

Strict workspace all-target Clippy and nightly formatting checks pass. The
generated `crabka.io_greses.yaml` is byte-for-byte identical to fresh output.
The full runtime-value scanner reports 5,607 rows across 1,037 files; the
focused `schema-serde` result reports 11 rows across 3 files. Those focused
rows are the two exposed defaults, fixed exponent/media/reference/magic
semantics, or retry/test assertions and deadlines described above.

## Rebalancer Runtime Policy

The `rebalancer` scanner has 78 rows. Persisted schema versions and filenames,
Kafka error/resource/operation codes and configuration keys, goal names,
zero-value protocol sentinels, CPU unit conversion, capacity clamps,
preallocation hints, and test values are fixed. The existing scrape,
optimization, anomaly, execution, persistence, and state-topic identity
settings are already CLI arguments backed by environment variables and flow
into UOM values at their owning components.

The generated Kafka request default formerly supplied the 60-second
`AlterPartitionReassignments` broker-side timeout. Submit and cancel now frame
an explicit validated `ReassignmentRequestTimeout`; `LiveClient::new` retains
the 60-second default for compatibility. The standalone binary exposes human
UOM values through `--reassignment-request-timeout` and
`CRABKA_REBALANCER_REASSIGNMENT_REQUEST_TIMEOUT`, while the Helm chart exposes
`reassignmentRequestTimeout: 60s`. Values must be positive whole milliseconds
within `i32`; this daemon transport policy does not belong in the
per-proposal `KafkaRebalance` CRD.

The remaining deployment policy is:

- the metrics HTTP request timeout;
- state-topic poll interval, load quiet period, fetch byte cap, create timeout,
  produce request timeout, retry count and retry backoff;
- state-topic segment duration and minimum cleanable dirty ratio;
- the CancelExecution response wait, executor shutdown drain timeout, and
  ingester shutdown join timeout.

The recovery wait should reuse the state-topic poll interval rather than gain
another equal-cadence option. The 25-ms CancelExecution polling step remains
an internal responsiveness detail. The detector snapshot history capacity
must be derived from its configured tick interval and longest sustained-rule
threshold; exposing the current value of ten would let valid threshold
settings silently discard required history.

The proposed minimal design adds direct `crabka-rebalancer` CLI arguments
backed by `CRABKA_REBALANCER_*` environment variables and corresponding Helm
values. Times, bytes, and the dirty ratio remain UOM `Time`, `ByteSize`, and
`Ratio`; the positive retry count uses a repository `refined_type` newtype.
Validation requires a positive poll interval, quiet period, request/drain
timeouts, byte cap and retry count, a dirty ratio strictly between zero and
one, and metrics retention at least as long as the fixed twelve-hour query
window. Defaults preserve the current 5-second metrics request timeout,
100-ms state poll, 500-ms quiet period, 1-MiB fetch cap, 10-second create and
produce timeouts, 50 attempts with 200-ms backoff, one-minute segments,
one-percent dirty ratio, five-second cancellation wait, ten-second executor
drain, and five-second ingester join.

No CRD owns the standalone daemon. `KafkaRebalance` already exposes
per-proposal goals and throttle and must not absorb service lifecycle or
state-topic settings.

This broader design is pending explicit approval and has not been implemented.
The completed reassignment-request-timeout slice passes all 379 runnable
all-target tests; one real-broker test is explicitly ignored. Helm lint and
all 20 chart unit tests pass. Strict workspace all-target Clippy, nightly
formatting, scanner-count, and diff-hygiene gates pass.

## Pprof Debuginfod Resource Policy

The `pprof` scanner has 21 rows. Eleven are fixed data-format or algorithm
invariants: root and synthetic-node names, the empty-stacktrace sentinel,
Go-shape prefix, symbolization bit flags, and the minimum-one node clamp. Seven
are test-only profile fixtures and bounded local-server waits. The remaining
three are deployment policy.

The three deployment policies belong to the optional debuginfod client: a
512-MiB downloaded-artifact cap, a five-second connect timeout, and a
ten-second whole-request timeout. They were already UOM `ByteSize` and `Time`
values, but were not configurable.

The implemented minimal design adds a `DebuginfodConfig` accepted by
`DebuginfodResolver`, containing:

```text
max_artifact_size: ByteSize
connect_timeout: Time
request_timeout: Time
```

All values must be positive and the connect timeout must not exceed the whole
request timeout. The artifact size must be a whole-byte UOM value. Existing
constructors remain default-backed compatibility wrappers; config-aware paths
reach the querier, query-frontend, and symbolizer roles. The existing defaults
remain unchanged. The owning `crabka-profiles` binary exposes them as
unit-bearing CLI arguments:

```text
--debuginfod-max-artifact-size
--debuginfod-connect-timeout
--debuginfod-request-timeout
```

They are backed by:

```text
CRABKA_PROFILES_DEBUGINFOD_MAX_ARTIFACT_SIZE
CRABKA_PROFILES_DEBUGINFOD_CONNECT_TIMEOUT
CRABKA_PROFILES_DEBUGINFOD_REQUEST_TIMEOUT
```

The existing comma-delimited `--debuginfod-url` option also gains
`CRABKA_PROFILES_DEBUGINFOD_URLS` backing so the complete debuginfod surface
can be overridden in Kubernetes without rewriting the container command.

No CRD owns the standalone Profiles service. The URL path construction,
redirect prohibition, build-id validation, and capped streaming read remain
fixed security behavior rather than configuration.

The implementation preserves the 512-MiB, five-second, and ten-second
defaults, validates positive whole bytes and positive finite timeouts with
`refined_type` and UOM values, and rejects a connect timeout greater than the
whole-request timeout before role startup. The config-aware path is exercised
by the querier, query-frontend, and symbolizer roles while the existing public
constructors remain default-backed compatibility wrappers. An empty URL list
still prevents outbound debuginfod requests.

The exact scanner remains at 21 rows. The all-target gates passed 120 `pprof`
tests and 244 runnable `profiles` tests; four Docker-only differential tests
remain explicitly ignored. Strict workspace all-target Clippy, nightly
formatting, and diff hygiene pass.
