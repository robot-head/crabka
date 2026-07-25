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
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p crabka-gres-substrate -p crabka-gres -p crabka-operator --all-targets` reported 1,369 passing test results, zero failures, and zero ignored tests.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p crabka-gres-substrate -p crabka-gres -p crabka-operator --all-targets -- -D warnings` passed.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q -p crabka-gres -- --help | rg -- '--wal-recovery-dns-timeout-ms'` displayed the exact standalone flag.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo fmt --all -- --check` and `git diff --check` passed.
- Two fresh `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo run -q -p crabka-operator -- gen-crds <temporary-directory>` generations each produced nine files and matched each other and `deploy/crds` exactly.
