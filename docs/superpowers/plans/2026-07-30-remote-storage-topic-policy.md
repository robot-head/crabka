# Remote Storage Topic Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the topic-backed internal metadata client's operational transport and snapshot policy through validated UOM-backed TOML and Kafka CRD fields while preserving every existing default.

**Architecture:** `crabka-remote-storage-topic` owns the validated transport policy and applies it at the Kafka client boundary. `crabka-broker` overlays standalone TOML values onto those defaults and shares the resulting policy between the RLMM and diskless WAL-index logs. `crabka-operator` validates CRD input by constructing the effective broker config, then renders parseable human-unit TOML.

**Tech Stack:** Rust, Tokio, `crabka-units`, `refined_type`, Serde/TOML, Kube `JsonSchema`, generated CRDs.

## Global Constraints

- Preserve defaults exactly: topic-create timeout `30s`, fetch maximum wait `500ms`, fetch maximum bytes `1MiB`, fetch retry backoff `200ms`, event queue capacity `1024`, and RLMM snapshot interval `60s`.
- Use `Time` for durations and `ByteSize` for byte budgets at every configuration boundary.
- Use a `refined_type::rule::GreaterUsize<0>`-backed `MetadataEventQueueCapacity` newtype for the queue capacity.
- Apply the five transport fields to both the RLMM metadata log and diskless WAL-index log; apply `snapshot_interval` only to the RLMM snapshot loop.
- Put the standalone surface under `[remote_storage.kafka_metadata]` and the Kubernetes surface under `Kafka.spec.tieredStorage.metadataManager.topic`.
- Add no CLI flags or environment variables because the Kafka CRD owns deployed broker policy and TOML owns standalone broker policy.
- Keep topic names, cleanup/retention policy, partition hashing, request sentinels, topic-id validation, snapshot format/file name, serialization allocation hints, security derivation, and in-memory fixture capacity fixed.
- Preserve direct-construction safety by validating in `KafkaMetadataEventLog::start`, `BrokerConfig::validate`, file-config application, and CRD validation.
- Preserve unrelated dirty-worktree changes and stage only the files or hunks named by each task.
- Do not modify or stage the four protected untracked plans dated 2026-07-28.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not run `cargo clean` until the entire repository-wide configuration goal is complete.

---

### Task 1: Add validated transport policy to `crabka-remote-storage-topic`

**Files:**
- Modify: `crates/remote-storage-topic/Cargo.toml`
- Modify: `crates/remote-storage-topic/src/kafka_log.rs`
- Modify: `crates/remote-storage-topic/src/lib.rs`
- Modify selected dependency hunk only: `Cargo.lock`

**Interfaces:**
- Produces: `DEFAULT_METADATA_TOPIC_CREATE_TIMEOUT: Time`
- Produces: `DEFAULT_METADATA_FETCH_MAX_WAIT: Time`
- Produces: `DEFAULT_METADATA_FETCH_MAX_BYTES: ByteSize`
- Produces: `DEFAULT_METADATA_FETCH_RETRY_BACKOFF: Time`
- Produces: `DEFAULT_METADATA_EVENT_QUEUE_CAPACITY: usize`
- Produces: `MetadataEventQueueCapacity::new(usize) -> Result<Self, String>`
- Produces: `MetadataEventQueueCapacity::capacity(self) -> usize`
- Produces: `KafkaMetadataLogConfig::validate(&self) -> Result<(), String>`

- [x] **Step 1: Write failing default, custom-value, and newtype tests**

Extend the `kafka_log.rs` test module:

```rust
#[test]
fn config_defaults_preserve_transport_policy() {
    let cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
    check!(cfg.topic_create_timeout == secs(30));
    check!(cfg.fetch_max_wait == millis(500));
    check!(cfg.fetch_max_bytes == mebibytes(1));
    check!(cfg.fetch_retry_backoff == millis(200));
    check!(cfg.event_queue_capacity.capacity() == 1024);
}

#[test]
fn config_accepts_custom_transport_policy() {
    let mut cfg = KafkaMetadataLogConfig::new("127.0.0.1:9092");
    cfg.topic_create_timeout = secs(45);
    cfg.fetch_max_wait = millis(750);
    cfg.fetch_max_bytes = mebibytes(2);
    cfg.fetch_retry_backoff = millis(300);
    cfg.event_queue_capacity = MetadataEventQueueCapacity::new(2048).unwrap();
    cfg.validate().unwrap();
}

#[test]
fn metadata_event_queue_capacity_rejects_zero() {
    assert!(MetadataEventQueueCapacity::new(0).is_err());
    check!(MetadataEventQueueCapacity::new(1).unwrap().capacity() == 1);
}
```

- [x] **Step 2: Write failing validation-table tests**

Add a table that independently mutates each field and expects `validate()` to
fail for:

- zero, fractional-millisecond, infinite, and `i32::MAX + 1` millisecond topic-create timeouts;
- the same four invalid fetch waits;
- zero, fractional-byte, infinite, and `i32::MAX + 1` byte fetch budgets;
- zero and infinite retry backoffs.

Construct the out-of-range values without scalar magic:

```rust
Time::from_millis(i64::from(i32::MAX) + 1)
ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1)
```

Check that errors name the offending field.

- [x] **Step 3: Write failing runtime-propagation tests**

Update the existing wire-default test to assert conversions from a config:

```rust
check!(cfg.topic_create_timeout.millis_i32() == 30_000);
check!(cfg.fetch_max_wait.millis_i32() == 500);
check!(cfg.fetch_max_bytes.bytes_i32() == 1 << 20);
```

Extract a private `metadata_event_channel(MetadataEventQueueCapacity)` helper
used by `subscribe`; assert its sender's `max_capacity()` equals a custom
capacity. Construct `ConsumerState` directly in its existing unit test and
assert that custom `fetch_max_wait`, `fetch_max_bytes`, and
`fetch_retry_backoff` values are retained. Keep the existing assignment tests
intact.

Add a Tokio test that passes an invalid config to
`KafkaMetadataEventLog::start` and asserts the validation error is returned
before any network operation.

- [x] **Step 4: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-remote-storage-topic config_ --locked
```

Expected: compilation fails because the new constants, fields, type, and
validation method do not exist.

- [x] **Step 5: Implement the validated policy**

Add `refined_type = { workspace = true }` to the crate. Replace the private
transport constants with public default constants. Implement:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataEventQueueCapacity(usize);

impl MetadataEventQueueCapacity {
    pub fn new(value: usize) -> Result<Self, String> {
        refined_type::rule::GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata event queue capacity: {error}"))
    }

    #[must_use]
    pub const fn capacity(self) -> usize {
        self.0
    }
}
```

Implement `Default` with the `1024` public constant. Add the five fields to
`KafkaMetadataLogConfig` and initialize them in `new`.

Validate the two wire millisecond values as positive, finite, whole
milliseconds in `1..=i32::MAX`; validate the fetch budget as positive, finite,
whole bytes in `1..=i32::MAX`; validate retry backoff with
`std::time::Duration::try_from_secs_f64` and reject zero. Keep the validated
newtype opaque so zero cannot be assigned.

Re-export the new type and defaults from `lib.rs`.

- [x] **Step 6: Apply policy at the Kafka runtime boundary**

At the start of `KafkaMetadataEventLog::start`, map validation failure to
`MetadataLogError::Other`. Store the fetch settings and queue capacity on the
log. Then:

- pass `cfg.topic_create_timeout` to `AdminClient::create_topics`;
- pass `self.event_queue_capacity.capacity()` to `mpsc::channel`;
- copy fetch wait, byte budget, and retry backoff into `ConsumerState`;
- pass the state values to `fetch_partition`;
- sleep for `state.fetch_retry_backoff.to_std()` after a failed fetch.

- [x] **Step 7: Run GREEN and the crate suite**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-remote-storage-topic --all-targets --locked
```

Expected: all 60 baseline tests plus the new policy tests pass.

- [x] **Step 8: Commit only this task**

Inspect `git diff` and stage only this task's source files plus the
`crabka-remote-storage-topic` dependency hunk in `Cargo.lock`.

```bash
git commit -m "feat(tiered): configure metadata transport"
```

---

### Task 2: Add broker defaults, validation, and TOML overlays

**Files:**
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/file_config.rs`
- Modify only as required for struct-literal compatibility:
  - `crates/broker/src/broker.rs`
  - `crates/broker/tests/jvm_acceptance.rs`
  - `crates/broker/tests/tiered_storage_topic_rlmm.rs`
  - `crates/broker/tests/tiered_storage_multi_broker.rs`

**Interfaces:**
- Extends: `KafkaRlmmConfig`
- Extends: `FileKafkaRlmmConfig`
- Produces: `KafkaRlmmConfig::validate(&self) -> Result<(), BrokerError>`
- Consumes: all five `crabka_remote_storage_topic` defaults

- [x] **Step 1: Write failing broker-default and validation tests**

Extend the `config.rs` tests:

```rust
#[test]
fn kafka_rlmm_defaults_preserve_metadata_policy() {
    let cfg = KafkaRlmmConfig::default();
    check!(cfg.topic_create_timeout == secs(30));
    check!(cfg.fetch_max_wait == millis(500));
    check!(cfg.fetch_max_bytes == mebibytes(1));
    check!(cfg.fetch_retry_backoff == millis(200));
    check!(cfg.event_queue_capacity.capacity() == 1024);
    check!(cfg.snapshot_interval == minutes(1));
}
```

Add a custom valid policy test and a table-driven invalid test covering all
transport cases from Task 1 plus zero/infinite snapshot intervals. Assert that
`BrokerConfig::validate()` rejects an invalid `RlmmKind::TopicBacked` config
with `BrokerError::InvalidRuntimeConfig`.

- [x] **Step 2: Write failing TOML parsing and application tests**

Extend `file_config.rs` tests with:

```toml
[remote_storage]
storage_dir = "/tmp/tier"

[remote_storage.kafka_metadata]
bootstrap = "broker-0:9094"
num_partitions = 8
replication = 1
topic_create_timeout = "45s"
fetch_max_wait = "750ms"
fetch_max_bytes = "2MiB"
fetch_retry_backoff = "300ms"
event_queue_capacity = 2048
snapshot_interval = "90s"
```

Assert exact `Time`, `ByteSize`, and `MetadataEventQueueCapacity` values after
`apply_to`. Extend the existing default test to assert all six preserved
defaults. Add rejection cases for zero, fractional wire values, wire overflow,
non-finite time, zero queue capacity, and zero snapshot interval.

- [x] **Step 3: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker kafka_rlmm --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker kafka_metadata_section --lib --locked
```

Expected: compilation fails on missing config fields.

- [x] **Step 4: Extend broker configuration and reuse authoritative validation**

Add the five transport fields to `KafkaRlmmConfig`, defaulting directly from
the public `crabka_remote_storage_topic` constants. Implement
`KafkaRlmmConfig::validate` by constructing a `KafkaMetadataLogConfig` with its
transport values, calling its `validate`, mapping the error to
`BrokerError::InvalidRuntimeConfig`, and then validating the snapshot interval
as positive, finite, and `Duration`-representable.

Call this validation from `BrokerConfig::validate` whenever
`remote_log_metadata` is `RlmmKind::TopicBacked`.

Migrate existing `KafkaRlmmConfig` test literals with
`..KafkaRlmmConfig::default()` where their purpose is unrelated. Keep explicit
fields in policy tests.

- [x] **Step 5: Extend and apply file configuration**

Add optional fields to `FileKafkaRlmmConfig`:

```rust
#[serde(default, with = "crabka_units::serde_units::human::option_time")]
#[schemars(with = "Option<String>")]
pub topic_create_timeout: Option<Time>,

#[serde(default, with = "crabka_units::serde_units::human::option_time")]
#[schemars(with = "Option<String>")]
pub fetch_max_wait: Option<Time>,

#[serde(default, with = "crabka_units::serde_units::human::option_byte_size")]
#[schemars(with = "Option<String>")]
pub fetch_max_bytes: Option<ByteSize>,

#[serde(default, with = "crabka_units::serde_units::human::option_time")]
#[schemars(with = "Option<String>")]
pub fetch_retry_backoff: Option<Time>,

#[serde(default)]
#[schemars(range(min = 1))]
pub event_queue_capacity: Option<usize>,

#[serde(default, with = "crabka_units::serde_units::human::option_time")]
#[schemars(with = "Option<String>")]
pub snapshot_interval: Option<Time>,
```

Confirm the byte-size helper's exact module name in `serde_units::human`
before compiling and use that repository-provided helper. During
`apply_remote_storage`, begin with `KafkaRlmmConfig::default()`, overlay the
existing identity fields and all present policy values, construct the queue
newtype through `MetadataEventQueueCapacity::new`, set `snapshot_dir`, then
call `validate()` before assigning `RlmmKind::TopicBacked`.

- [x] **Step 6: Run GREEN and broker configuration suites**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker kafka_rlmm --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker kafka_metadata_section --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker config --lib --locked
```

- [x] **Step 7: Commit only this task**

```bash
git commit -m "feat(broker): expose metadata topic policy"
```

Stage only the broker files changed for this task.

---

### Task 3: Propagate one shared transport policy to both live logs

**Files:**
- Modify: `crates/broker/src/broker.rs`

**Interfaces:**
- Produces private helper:
  `metadata_log_config(&KafkaRlmmConfig, String, String) -> KafkaMetadataLogConfig`
- Consumes the helper in:
  - `bootstrap_topic_rlmm`
  - `bootstrap_diskless_index_log`

- [x] **Step 1: Write a failing propagation test**

Add a unit test next to the existing RLMM bootstrap tests. Create one custom
`KafkaRlmmConfig`, call the missing helper for both
`METADATA_TOPIC` and `DISKLESS_WAL_INDEX_TOPIC`, and assert:

- bootstrap, partitions, replication, security, and client ID are preserved;
- all five transport fields are identical for both outputs;
- only the topic name differs.

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker metadata_log_config --lib --locked
```

Expected: compilation fails because the helper does not exist.

- [x] **Step 3: Implement the shared mapping**

Implement:

```rust
fn metadata_log_config(
    config: &KafkaRlmmConfig,
    topic: String,
    client_id: String,
) -> crabka_remote_storage_topic::KafkaMetadataLogConfig
```

Copy bootstrap, topic identity, partitions, replication, client ID, security,
and all five transport fields. Call the helper from both bootstrap paths.
Continue passing `cfg.cfg.snapshot_interval.to_std()` only to
`TopicBasedRemoteLogMetadataManager::start`.

If ownership of boxed security prevents borrowing, clone only the security
value; do not duplicate the policy mapping.

- [x] **Step 4: Run GREEN and relevant bootstrap tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker metadata_log_config --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker rlmm --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker diskless_index --lib --locked
```

- [x] **Step 5: Commit only the runtime propagation**

```bash
git commit -m "feat(broker): apply metadata transport policy"
```

---

### Task 4: Expose, validate, render, and generate the Kafka CRD surface

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `crates/operator/src/controller/listeners.rs`
- Regenerate only: `deploy/crds/crabka.io_kafkas.yaml`

**Interfaces:**
- Extends: `TopicMetadataManagerSpec`
- Reuses: `KafkaRlmmConfig::validate`
- Produces CRD fields:
  - `topicCreateTimeout`
  - `fetchMaxWait`
  - `fetchMaxBytes`
  - `fetchRetryBackoff`
  - `eventQueueCapacity`
  - `snapshotInterval`

- [x] **Step 1: Write failing CRD serde and validation tests**

Extend `TopicMetadataManagerSpec` tests with an explicit JSON/YAML object using
all six fields. Assert UOM parsing to exact values and camelCase serialization.
Add table-driven invalid specs covering the same transport and snapshot
constraints as the broker tests, including `eventQueueCapacity: 0`.

Keep absent fields as `None`, proving omission retains broker defaults.

- [x] **Step 2: Write failing schema tests**

Generate the Kafka schema in a unit test and assert:

- all six properties exist under the topic metadata-manager schema;
- five dimensioned properties have JSON type `string`;
- `eventQueueCapacity` has JSON type `integer` and minimum `1`;
- none of the six optional fields appears in `required`.

- [x] **Step 3: Write failing rendered-TOML round-trip test**

Extend `render_broker_toml_emits_kafka_metadata_when_topic_rlmm_set` with all
six custom fields. Assert quoted human UOM text:

```toml
topic_create_timeout = "45s"
fetch_max_wait = "750ms"
fetch_max_bytes = "2MiB"
fetch_retry_backoff = "300ms"
event_queue_capacity = 2048
snapshot_interval = "90s"
```

Parse the rendered output through `crabka_broker::FileConfig`, apply it to a
default `BrokerConfig`, and assert all six effective values. This is the
behavioral proof that the CRD surface reaches broker runtime configuration.

- [x] **Step 4: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator topic_metadata --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator render_broker_toml_emits_kafka_metadata --lib --locked
```

Expected: compilation fails on missing CRD fields.

- [x] **Step 5: Extend the CRD type and reuse broker validation**

Add the six optional fields to `TopicMetadataManagerSpec`. Use
`option_time`/`Option<String>` for times, the repository's human byte-size
Serde helper/`Option<String>` for fetch bytes, and
`#[schemars(range(min = 1))]` for capacity.

In `TopicMetadataManagerSpec::validate`:

1. retain bootstrap, partition, and replication checks;
2. start from `KafkaRlmmConfig::default()`;
3. overlay every present policy field;
4. construct `MetadataEventQueueCapacity` through `new`;
5. call `KafkaRlmmConfig::validate` and prefix its error with
   `metadataManager.topic`.

This keeps wire-size and duration limits authoritative in the runtime crates,
not duplicated in the operator.

Migrate unrelated `TopicMetadataManagerSpec` literals with
`..Default::default()`.

- [x] **Step 6: Render parseable human-unit TOML**

Import `crabka_units::fmt::Human as _`. For each present dimensioned value,
render the human adapter inside TOML quotes:

```rust
if let Some(value) = topic.fetch_max_wait {
    let _ = writeln!(out, "fetch_max_wait = \"{}\"", value.human());
}
if let Some(value) = topic.fetch_max_bytes {
    let _ = writeln!(out, "fetch_max_bytes = \"{}\"", value.human());
}
```

Repeat for all time values and render capacity as an integer. The human
adapter selects canonical parseable units (`500ms`, `1MiB`) and preserves
sub-millisecond valid retry/snapshot values through nanosecond units where
needed.

- [x] **Step 7: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator topic_metadata --lib --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator render_broker_toml_emits_kafka_metadata --lib --locked
```

- [x] **Step 8: Regenerate only the Kafka CRD safely**

Because the worktree contains unrelated operator changes, do not run the
repository script directly over every deployed CRD. Generate twice into
separate temporary directories:

```bash
crd_tmp_a="$(mktemp -d /var/tmp/crabka-crd-a.XXXXXX)"
crd_tmp_b="$(mktemp -d /var/tmp/crabka-crd-b.XXXXXX)"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator --locked -- gen-crds "$crd_tmp_a"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator --locked -- gen-crds "$crd_tmp_b"
diff -u "$crd_tmp_a/crabka.io_kafkas.yaml" "$crd_tmp_b/crabka.io_kafkas.yaml"
```

After deterministic output is proven, mechanically replace only
`deploy/crds/crabka.io_kafkas.yaml` with the generated Kafka file. Verify its
diff contains only the six new schema properties and descriptions. Remove
only the two exact temporary directories after checking their resolved
`/var/tmp/crabka-crd-` prefixes.

- [x] **Step 9: Run operator all-targets**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
```

- [x] **Step 10: Commit only the operator and Kafka CRD changes**

```bash
git commit -m "feat(operator): expose metadata topic policy"
```

---

### Task 5: Close the audit slice and verify the repository boundary

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify checkboxes only: `docs/superpowers/plans/2026-07-30-remote-storage-topic-policy.md`

- [x] **Step 1: Update audit evidence**

Replace the 25 `remote-storage-topic` findings with their final disposition:

- six exposed policy values, with exact Rust/TOML/CRD names and defaults;
- shared application to RLMM and diskless WAL-index logs;
- fixed protocol/security/durable-format values and the reason each remains
  non-configurable;
- package test, workspace Clippy, formatting, generated-CRD, and scanner
  evidence.

Do not describe fixed semantic constants as tunables.

- [x] **Step 2: Run all affected all-target suites**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-remote-storage-topic --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
```

- [x] **Step 3: Format and run strict workspace Clippy**

```bash
cargo +nightly fmt --all
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
```

- [x] **Step 4: Verify generated schema and scanner evidence**

Generate the Kafka CRD into a fresh temporary directory and compare it to the
deployed file. Re-run the repository magic-value scanner and confirm the
`remote-storage-topic` owner remains at the expected fixed-semantic count,
with no policy literal left in runtime paths. Record the exact commands and
counts in `docs/configuration-audit.md`.

```bash
crd_verify_tmp="$(mktemp -d /var/tmp/crabka-crd-verify.XXXXXX)"
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo run -p crabka-operator --locked -- gen-crds "$crd_verify_tmp"
diff -u \
  deploy/crds/crabka.io_kafkas.yaml \
  "$crd_verify_tmp/crabka.io_kafkas.yaml"

tools/audit-runtime-values.sh \
  > /var/tmp/remote-storage-topic-runtime-audit.txt
wc -l /var/tmp/remote-storage-topic-runtime-audit.txt
cut -d: -f1 /var/tmp/remote-storage-topic-runtime-audit.txt \
  | sort -u \
  | wc -l
rg '^crates/remote-storage-topic/' \
  /var/tmp/remote-storage-topic-runtime-audit.txt \
  > /var/tmp/remote-storage-topic-focused-audit.txt
wc -l /var/tmp/remote-storage-topic-focused-audit.txt
cut -d: -f1 /var/tmp/remote-storage-topic-focused-audit.txt \
  | sort -u \
  | wc -l
rg -n \
  '30_000|500|1_048_576|1024|200|TOPIC_CREATE_TIMEOUT|FETCH_MAX_WAIT|FETCH_MAX|FETCH_RETRY_BACKOFF|mpsc::channel' \
  crates/remote-storage-topic \
  crates/broker/src \
  crates/operator/src \
  docs/configuration-audit.md
```

Remove only the exact `crd_verify_tmp` directory after confirming it resolves
under `/var/tmp/crabka-crd-verify.`.

- [x] **Step 5: Review scope and plan completeness**

```bash
git status --short
git diff --check
git diff --stat
git diff -- crates/remote-storage-topic crates/broker crates/operator \
  deploy/crds/crabka.io_kafkas.yaml docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-30-remote-storage-topic-policy.md
```

Confirm:

- all six approved settings are present end-to-end;
- all dimensioned fields remain UOM values;
- all validated scalar construction goes through `refined_type`;
- both live metadata logs receive one shared transport policy;
- existing defaults and fixed semantics are unchanged;
- no placeholder (`TODO`, `FIXME`, `unimplemented!`, test-only bypass) was
  added;
- unrelated dirty files and the four protected plans remain untouched.

- [x] **Step 6: Commit the audit closure**

```bash
git commit -m "docs(config): close metadata topic audit"
```

Stage only the audit document and this plan's completed checkboxes. Do not run
`cargo clean`; that remains the final action after every repository owner is
complete.
