# Remote Storage Topic Policy Design

## Goal

Expose the deployment policy currently embedded in the topic-backed internal
metadata client while preserving its behavior and fixed Kafka/security
contracts.

The broker uses the same `KafkaRlmmConfig` to construct both:

- the `__remote_log_metadata` event log used by the topic-backed remote-log
  metadata manager; and
- the diskless WAL-index event log.

The transport settings therefore form one shared internal-metadata policy.
Creating separate settings for the two logs would duplicate an identical
client boundary without an existing operational need.

## Configuration Surface

Add the following values to `KafkaMetadataLogConfig` and `KafkaRlmmConfig`:

| Rust field | TOML field | CRD field | Default |
| --- | --- | --- | --- |
| `topic_create_timeout: Time` | `topic_create_timeout` | `topicCreateTimeout` | `30s` |
| `fetch_max_wait: Time` | `fetch_max_wait` | `fetchMaxWait` | `500ms` |
| `fetch_max_bytes: ByteSize` | `fetch_max_bytes` | `fetchMaxBytes` | `1MiB` |
| `fetch_retry_backoff: Time` | `fetch_retry_backoff` | `fetchRetryBackoff` | `200ms` |
| `event_queue_capacity: MetadataEventQueueCapacity` | `event_queue_capacity` | `eventQueueCapacity` | `1024` |

The existing `KafkaRlmmConfig::snapshot_interval: Time` also becomes
configurable as:

| Rust field | TOML field | CRD field | Default |
| --- | --- | --- | --- |
| `snapshot_interval: Time` | `snapshot_interval` | `snapshotInterval` | `60s` |

All six external fields are optional. Omission retains the current values.
The broker's existing `[remote_storage.kafka_metadata]` table owns the TOML
surface, and `TopicMetadataManagerSpec` owns the CRD surface. No CLI or
environment variables are added because the Kafka CRD already owns this
deployment policy; standalone brokers can use the same TOML table.

## Types and Validation

Dimensioned settings remain UOM values:

- timeouts, waits, backoff, and snapshot cadence use `Time`;
- the fetch budget uses `ByteSize`.

`MetadataEventQueueCapacity` is a small validated newtype around `usize`.
Construction passes through `refined_type::rule::GreaterUsize<0>`, so a zero
capacity cannot reach `tokio::sync::mpsc::channel`.

Validation occurs at every public configuration boundary:

- `KafkaMetadataLogConfig::start` validates direct library construction;
- broker file application validates TOML values;
- `BrokerConfig::validate` validates embedded/library construction; and
- `TopicMetadataManagerSpec::validate` validates CRD input before rendering.

The authoritative runtime constraints are:

- topic-create timeout: positive, finite, whole milliseconds, and at most
  `i32::MAX` milliseconds;
- fetch maximum wait: positive, finite, whole milliseconds, and at most
  `i32::MAX` milliseconds;
- fetch maximum bytes: positive, finite, whole bytes, and at most
  `i32::MAX` bytes;
- fetch retry backoff: positive, finite, and representable by
  `std::time::Duration`;
- event queue capacity: greater than zero; and
- snapshot interval: positive, finite, and representable by
  `std::time::Duration`.

`KafkaMetadataLogConfig` owns validation of the five transport settings.
`KafkaRlmmConfig` reuses that validation and adds the snapshot-interval check.
The operator builds the effective default-overlaid `KafkaRlmmConfig` during
CRD validation rather than maintaining a second set of numeric limits.

## Runtime Flow

The configuration path is:

```text
Kafka.spec.tieredStorage.metadataManager.topic
    -> rendered [remote_storage.kafka_metadata]
    -> FileKafkaRlmmConfig
    -> KafkaRlmmConfig
    -> KafkaMetadataLogConfig
       -> topic provisioning
       -> per-partition fetch loops
       -> shared metadata event queue
    -> TopicBasedRemoteLogMetadataManager snapshot loop
```

`bootstrap_topic_rlmm` and `bootstrap_diskless_index_log` copy the same five
transport fields into their respective `KafkaMetadataLogConfig` values.
Only `bootstrap_topic_rlmm` consumes `snapshot_interval`, because the diskless
index projection has no RLMM snapshot loop.

`KafkaMetadataEventLog` retains the fetch settings and queue capacity.
`subscribe` creates the bounded channel with the configured capacity and
copies fetch policy into `ConsumerState`; every spawned partition loop uses
those values. Topic provisioning uses the configured creation timeout.

## Preserved Fixed Behavior

The following remain fixed because changing them would alter protocol,
security, or durable-format semantics rather than deployment policy:

- internal topic names and diskless-index topic identity;
- `cleanup.policy=delete` and infinite retention;
- metadata partition hashing and live-assignment semantics;
- request sentinels such as latest timestamp and replica id;
- topic-id and build/wire validation;
- snapshot format version, file name, and encoding allocation hint; and
- the in-process fixture's broadcast capacity.

The existing constructors remain default-backed. Existing direct
`KafkaRlmmConfig` and `KafkaMetadataLogConfig` callers receive the six current
defaults when migrated.

## Testing

Tests proceed through RED/GREEN cycles and cover:

1. `KafkaMetadataLogConfig` default and custom values, validation failures,
   wire conversion, queue construction, and use by a partition fetch state.
2. `KafkaRlmmConfig` default propagation and validation, including snapshot
   cadence.
3. `[remote_storage.kafka_metadata]` default and explicit TOML parsing with
   UOM strings, plus rejection of invalid values.
4. Both broker bootstrap paths copying the shared transport policy.
5. `TopicMetadataManagerSpec` default, explicit, invalid, serialized, schema,
   rendered-TOML, and broker round-trip behavior.
6. Existing remote-storage-topic, broker, and operator all-target suites,
   strict workspace Clippy, nightly formatting, scanner evidence, and diff
   hygiene.

No network-dependent test is required to prove configuration propagation; the
existing live Kafka integration coverage remains unchanged.
