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
- `--wal-producer-linger-ms` /
  `CRABKA_GRES_WAL_PRODUCER_LINGER_MS`;
- `--wal-producer-batch-bytes` /
  `CRABKA_GRES_WAL_PRODUCER_BATCH_BYTES`.

The fleet CRD adds the matching optional
`spec.compute.walProducerCompression`, `walProducerLingerMs`, and
`walProducerBatchBytes` fields. Compression has the exact
`none`/`gzip`/`snappy`/`lz4`/`zstd` enum; linger and batch bytes carry the same
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
