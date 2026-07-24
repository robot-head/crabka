# Runtime Configuration Audit

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

- Complete: `broker`, `schema-registry`, `grpc-gateway`.
- Pending: `admin-ui`, `audit`, `authz`, `bench-driver`, `blockstore`, `cli`,
  `client-admin`, `client-consumer`, `client-core`, `client-producer`,
  `client-streams`, `compression`, `connect`, `connect-derive`,
  `connect-postgres`, `docgen`, `gres`, `gres-activator`, `gres-balancer`,
  `gres-conformance`, `gres-control`, `gres-fdw`, `gres-loadtest`,
  `gres-ranges`, `gres-substrate`, `ids`,
  `integration-tests`, `kafka-tap`, `kraft-core`, `log`, `log-iobench`,
  `logfmt`, `logql`, `metadata`, `metrics`, `metrics-service`,
  `object-store`, `observability`, `observability-demo-app`, `operator`,
  `pgcatalog`, `pgexec`, `pgkv`, `pgmvcc`, `pgparser`, `pgtypes`, `pgwire`,
  `playground`, `pprof`, `profiles`, `promql`, `protocol`,
  `protocol-codegen`, `raft`, `rebalancer`, `records-legacy`,
  `remote-storage`, `remote-storage-topic`, `replicator`,
  `schema-serde`, `security`, `telemetry`, `throttle`,
  `traceql`, `traces`, `verified`, and `voters`.

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
- `CRABKA_GRES_REGISTRY_TOPIC_CREATE_TIMEOUT_MS`;
- `CRABKA_GRES_REGISTRY_READER_RETRY_BACKOFF_MS`;
- `CRABKA_GRES_REGISTRY_FETCH_MAX_WAIT_MS`; and
- `CRABKA_GRES_REGISTRY_FETCH_PARTITION_MAX_BYTES`.

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
