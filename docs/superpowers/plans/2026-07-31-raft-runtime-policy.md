# Raft Runtime Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose Raft heartbeat, fetch-miss, command-queue, and bounded-read policies through broker and Kafka CRD configuration without changing omitted-value behavior.

**Architecture:** Raft owns validated policy types and consumes them in the engine. Broker CLI/environment/runtime TOML retains heartbeat explicitness and forwards the three new settings; `Kafka.spec.brokerTuning` renders the same runtime TOML keys. Bounded application and replay loop until their target offset while replication and snapshot requests remain single-chunk operations.

**Tech Stack:** Rust, Tokio, `refined_type`, `crabka-units`, Clap, Serde TOML, kube CRDs, Cargo.

## Global Constraints

- Preserve omitted heartbeat behavior: derive election timeout divided by `3`.
- Honor every explicit heartbeat value, including `500ms`.
- Preserve defaults: fetch-miss limit `3`, command queue capacity `256`, metadata Raft fetch maximum `8MiB`.
- Use `refined_type` for both positive dimensionless count newtypes.
- Keep dimensioned settings as UOM `Time` and `ByteSize`.
- The byte limit must be positive, whole-byte, and fit signed `i32`.
- Keep `metadataSnapshotFetchMax` as the separate total snapshot security cap.
- Add no knobs for the heartbeat divisor, protocol values, or timer internals.
- Omitted CRD fields render no runtime TOML.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0`.
- Do not stage or edit the four protected untracked plans dated 2026-07-28.
- Do not run `cargo clean`; it remains the final repository-goal cleanup.

---

### Task 1: Raft-owned policy types and engine behavior

**Files:**
- Modify: `crates/raft/Cargo.toml`
- Modify: `crates/raft/src/lib.rs`
- Modify: `crates/raft/src/config.rs`
- Modify: `crates/raft/src/controller.rs`
- Modify: `crates/raft/src/kraft/controller.rs`

**Interfaces:**
- Produces: `ControllerFetchMissLimit`, `MetadataRaftCommandQueueCapacity`, and `MetadataRaftFetchMax`.
- `ControllerConfig::heartbeat_interval`: `Option<Time>`.
- `ControllerConfig` and `KraftConfig` carry all three validated policy types.
- `heartbeat_period(election_timeout: Time, configured: Option<Time>) -> Time`.

- [x] **Step 1: Write failing policy-type and heartbeat tests**

In `config.rs`, add tests proving defaults and invalid boundaries:

```rust
#[test]
fn raft_runtime_policy_defaults_and_validation() {
    check!(ControllerFetchMissLimit::default().get() == 3);
    check!(MetadataRaftCommandQueueCapacity::default().get() == 256);
    check!(MetadataRaftFetchMax::default().size() == mebibytes(8));
    check!(ControllerFetchMissLimit::new(0).is_err());
    check!(MetadataRaftCommandQueueCapacity::new(0).is_err());
    check!(MetadataRaftFetchMax::try_from(bytes(0)).is_err());
    check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
    check!(
        MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(
            i64::from(i32::MAX) + 1
        ))
        .is_err()
    );
}
```

In `kraft/controller.rs`, add:

```rust
#[test]
fn configured_heartbeat_overrides_derived_period() {
    check!(heartbeat_period(secs(5), None) == millis(1_666));
    check!(heartbeat_period(secs(5), Some(millis(500))) == millis(500));
}
```

- [x] **Step 2: Run the focused tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-raft raft_runtime_policy_defaults_and_validation --locked
```

Expected: compilation fails because the three types and optional-heartbeat
interface do not exist.

- [x] **Step 3: Implement the three validated policy types**

Add `refined_type = { workspace = true }` to `crates/raft/Cargo.toml`. In
`config.rs`, define:

```rust
pub const DEFAULT_CONTROLLER_FETCH_MISS_LIMIT: u32 = 3;
pub const DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY: usize = 256;
pub const DEFAULT_METADATA_RAFT_FETCH_MAX: ByteSize = mebibytes(8);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControllerFetchMissLimit(u32);

impl ControllerFetchMissLimit {
    pub fn new(value: u32) -> Result<Self, String> {
        refined_type::rule::GreaterU32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("controller fetch miss limit: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for ControllerFetchMissLimit {
    fn default() -> Self {
        Self::new(DEFAULT_CONTROLLER_FETCH_MISS_LIMIT)
            .expect("default controller fetch miss limit is valid")
    }
}
```

Define the command queue type:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRaftCommandQueueCapacity(usize);

impl MetadataRaftCommandQueueCapacity {
    pub fn new(value: usize) -> Result<Self, String> {
        refined_type::rule::GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata raft command queue capacity: {error}"))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for MetadataRaftCommandQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY)
            .expect("default metadata raft command queue capacity is valid")
    }
}
```

Define the byte policy:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetadataRaftFetchMax(i32);

impl MetadataRaftFetchMax {
    #[must_use]
    pub const fn bytes(self) -> i32 {
        self.0
    }

    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes_i64(i64::from(self.0))
    }
}

impl TryFrom<ByteSize> for MetadataRaftFetchMax {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_i64();
        if ByteSize::from_bytes_i64(bytes) != value {
            return Err("metadata raft fetch max must be a whole number of bytes".into());
        }
        let bytes = i32::try_from(bytes)
            .map_err(|_| "metadata raft fetch max must fit i32".to_owned())?;
        refined_type::rule::GreaterI32::<0>::new(bytes)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("metadata raft fetch max: {error}"))
    }
}

impl Default for MetadataRaftFetchMax {
    fn default() -> Self {
        Self::try_from(DEFAULT_METADATA_RAFT_FETCH_MAX)
            .expect("default metadata raft fetch max is valid")
    }
}

impl std::str::FromStr for ControllerFetchMissLimit {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<u32>()
            .map_err(|error| error.to_string())
            .and_then(Self::new)
    }
}
```

- [x] **Step 4: Write failing propagation and bounded-read tests**

Rename the existing `build_engine_only` body to
`build_engine_only_with_policy`; add fetch-miss and fetch-maximum parameters
and assign them to the matching `Engine` fields. Keep its local test-only
`mpsc::channel(1)` unchanged because that helper does not spawn the engine
loop. Restore `build_engine_only` as this default-backed wrapper:

Likewise rename the existing `build_full` body to `build_full_with_policy`
with these four additional tail parameters:

```rust
heartbeat_interval: Option<Time>,
fetch_miss_limit: ControllerFetchMissLimit,
command_queue_capacity: MetadataRaftCommandQueueCapacity,
metadata_raft_fetch_max: MetadataRaftFetchMax,
```

Have `build_full` call it with `None` and the three policy defaults, so
unrelated tests remain behavior-identical.

```rust
fn build_engine_only(me: NodeId, ids: &[NodeId]) -> (Engine, tempfile::TempDir) {
    build_engine_only_with_policy(
        me,
        ids,
        ControllerFetchMissLimit::default(),
        MetadataRaftFetchMax::default(),
    )
}

#[test]
fn engine_uses_configured_miss_limit_and_fetch_max() {
    let (engine, _dir) = build_engine_only_with_policy(
        NodeId(1),
        &[NodeId(1)],
        ControllerFetchMissLimit::new(5).unwrap(),
        MetadataRaftFetchMax::try_from(bytes(512)).unwrap(),
    );
    check!(engine.fetch_miss_limit.get() == 5);
    check!(engine.metadata_raft_fetch_max.bytes() == 512);
}

#[test]
fn spawned_controller_uses_configured_command_queue_capacity() {
    let (ctrl, _dir) = build_full_with_policy(
        NodeId(1),
        &[NodeId(1)],
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::new(7).unwrap(),
        MetadataRaftFetchMax::default(),
    );
    check!(ctrl.cmd_tx.capacity() == 7);
}
```

Use the existing in-memory `KraftLog` test helpers to append committed batches
whose encoded total exceeds a small configured limit. Add behavioral tests
using existing `one_offset_batch`, `build_engine_only_with_policy`,
`record_peer_sends`, `recv_peer_send`, and `wire::decode_fetch_snapshot`:

```rust
let records = engine.serve_fetch_records(Offset(0));
check!(decode_batches(&records).unwrap().len() == 1);

engine.send_fetch_snapshot(NodeId(2), (9, 3), 0);
let request = recv_peer_send(&mut sends).await;
match wire::decode_fetch_snapshot(&request.body) {
    Some(wire::PeerRequest::FetchSnapshot { max_bytes, .. }) => {
        check!(max_bytes == 512);
    }
    other => panic!("unexpected snapshot request: {other:?}"),
}

engine.advance_and_apply(target);
check!(engine.image.topic("configured-topic").is_some());

let mut recovered = MetadataImage::new(uuid::Uuid::nil());
replay_committed(
    &engine.log,
    &mut recovered,
    Offset(0),
    MetadataRaftFetchMax::try_from(bytes(512)).unwrap(),
);
check!(recovered.topic("configured-topic").is_some());
```

Each fixture must contain at least three batches so application and restart
replay require more than one bounded read. The expected image is constructed
from literal metadata records, not from the replay helper.

- [x] **Step 5: Implement engine propagation and progress-safe loops**

Change `ControllerConfig::heartbeat_interval` to `Option<Time>` and add the
three policy fields. Thread them through `Controller::start_with_listener`,
`KraftController::open`, `KraftConfig`, and `Engine`.

Replace the fixed implementations with:

```rust
fn heartbeat_period(election_timeout: Time, configured: Option<Time>) -> Time {
    configured.unwrap_or_else(|| {
        let period_ms = election_timeout_ms(election_timeout)
            .div_euclid(HEARTBEAT_DIVISOR)
            .max(1);
        Time::from_millis(i64::try_from(period_ms).unwrap_or(i64::MAX))
    })
}
```

Use `fetch_miss_limit.get()` in the consecutive-miss comparison,
`command_queue_capacity.get()` in `mpsc::channel`, and
`metadata_raft_fetch_max.size()` for decoded-log reads. Use
`metadata_raft_fetch_max.bytes()` for FetchSnapshot `max_bytes`.

Implement the remaining parsers:

```rust
impl std::str::FromStr for MetadataRaftCommandQueueCapacity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<usize>()
            .map_err(|error| error.to_string())
            .and_then(Self::new)
    }
}

impl std::str::FromStr for MetadataRaftFetchMax {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<ByteSize>()
            .map_err(|error| error.to_string())
            .and_then(Self::try_from)
    }
}
```

Re-export all three policy types and their defaults from `crates/raft/src/lib.rs`
beside `ControllerConfig`.

For application and replay, loop from the current cursor until the target:

```rust
while cursor < target {
    let batches = match log.read_decoded(cursor, metadata_raft_fetch_max.size()) {
        Ok(batches) => batches,
        Err(error) => {
            tracing::error!(?error, "kraft: bounded committed read failed");
            break;
        }
    };
    let Some(last) = batches.last() else {
        break;
    };
    apply_batches(&batches, image, cursor, target);
    let next = Offset(
        last.base_offset
            .saturating_add(i64::from(last.last_offset_delta))
            .saturating_add(1),
    );
    if next <= cursor {
        break;
    }
    cursor = next;
}
```

Keep replication serving and each snapshot request to one bounded chunk.
Remove `FETCH_MISS_LIMIT` and `MAX_APPLY`; keep `HEARTBEAT_DIVISOR`.

- [x] **Step 6: Verify and commit the Raft engine**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-raft --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-raft --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
```

Commit only Task 1 files:

```bash
git add crates/raft/Cargo.toml crates/raft/src/lib.rs crates/raft/src/config.rs \
  crates/raft/src/controller.rs crates/raft/src/kraft/controller.rs Cargo.lock
git commit -m "feat(raft): expose runtime policy"
```

### Task 2: Broker CLI, environment, and runtime TOML ownership

**Files:**
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/bin/broker.rs`
- Modify: `crates/broker/src/broker.rs`
- Modify: broker configuration tests in those files

**Interfaces:**
- Consumes: the three validated Raft types from Task 1.
- Produces: four broker runtime keys and exact explicit-heartbeat tracking.
- `BrokerConfig::controller_heartbeat_interval_explicit: bool` distinguishes omission from an explicit default-valued input.

- [x] **Step 1: Write failing broker default and file-overlay tests**

Add assertions to broker config tests:

```rust
let defaults = BrokerConfig::default();
check!(!defaults.controller_heartbeat_interval_explicit);
check!(defaults.controller_fetch_miss_limit.get() == 3);
check!(defaults.metadata_raft_command_queue_capacity.get() == 256);
check!(defaults.metadata_raft_fetch_max.size() == mebibytes(8));
```

Add runtime TOML tests using:

```toml
[runtime]
controller_heartbeat_interval = "500ms"
controller_fetch_miss_limit = 5
metadata_raft_command_queue_capacity = 7
metadata_raft_fetch_max = "512KiB"
```

Assert the overlay sets explicit heartbeat to `true` and produces typed values
`500ms`, `5`, `7`, and `512KiB`. Add table-driven rejection cases for zero
counts, `0B`, `1.5B`, and `2147483648B`.

- [x] **Step 2: Run focused broker tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker raft_runtime_policy --locked
```

Expected: compilation fails because the new broker fields and runtime keys do
not exist.

- [x] **Step 3: Implement BrokerConfig and runtime TOML merging**

Add these `BrokerConfig` fields:

```rust
pub controller_heartbeat_interval_explicit: bool,
pub controller_fetch_miss_limit: crabka_raft::ControllerFetchMissLimit,
pub metadata_raft_command_queue_capacity: crabka_raft::MetadataRaftCommandQueueCapacity,
pub metadata_raft_fetch_max: crabka_raft::MetadataRaftFetchMax,
```

Default explicitness to `false` and the three policies through their `Default`
implementations. Extend `RuntimeFileConfig` with raw optional serde fields:

```rust
pub controller_fetch_miss_limit: Option<u32>,
pub metadata_raft_command_queue_capacity: Option<usize>,
#[serde(with = "crabka_units::serde_units::human::option_byte_size")]
pub metadata_raft_fetch_max: Option<ByteSize>,
```

When `controller_heartbeat_interval` is present, set
`controller_heartbeat_interval_explicit = true` even when the value equals
`500ms`. Convert each new raw value through its Task 1 constructor before
storing it:

```rust
if let Some(value) = self.controller_heartbeat_interval {
    cfg.controller_heartbeat_interval =
        positive_time("controller_heartbeat_interval", value)?;
    cfg.controller_heartbeat_interval_explicit = true;
}
if let Some(value) = self.controller_fetch_miss_limit {
    cfg.controller_fetch_miss_limit =
        crabka_raft::ControllerFetchMissLimit::new(value)
            .map_err(FileConfigError::InvalidConfig)?;
}
if let Some(value) = self.metadata_raft_command_queue_capacity {
    cfg.metadata_raft_command_queue_capacity =
        crabka_raft::MetadataRaftCommandQueueCapacity::new(value)
            .map_err(FileConfigError::InvalidConfig)?;
}
if let Some(value) = self.metadata_raft_fetch_max {
    cfg.metadata_raft_fetch_max = crabka_raft::MetadataRaftFetchMax::try_from(value)
        .map_err(FileConfigError::InvalidConfig)?;
}
```

Keep the existing heartbeat/election cross-field validation.

- [x] **Step 4: Write failing CLI/environment precedence tests**

Extend broker binary tests to assert:

```rust
let parsed = Args::try_parse_from([
    "crabka-broker",
    "--controller-fetch-miss-limit", "5",
    "--metadata-raft-command-queue-capacity", "7",
    "--metadata-raft-fetch-max", "512KiB",
    "--controller-heartbeat-interval", "500ms",
]).unwrap();
let config = parsed_config(parsed);
check!(config.controller_heartbeat_interval_explicit);
check!(config.controller_fetch_miss_limit.get() == 5);
check!(config.metadata_raft_command_queue_capacity.get() == 7);
check!(config.metadata_raft_fetch_max.size() == kibibytes(512));
```

Add subprocess or Clap environment tests for:

```text
CRABKA_CONTROLLER_FETCH_MISS_LIMIT=5
CRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY=7
CRABKA_METADATA_RAFT_FETCH_MAX=512KiB
```

Assert CLI values override environment values and zero/fractional/overflow
inputs fail before startup.

- [x] **Step 5: Implement CLI/env fields and ControllerConfig propagation**

Add exact CLI fields:

```rust
#[arg(long, env = "CRABKA_CONTROLLER_FETCH_MISS_LIMIT")]
controller_fetch_miss_limit: Option<crabka_raft::ControllerFetchMissLimit>,

#[arg(long, env = "CRABKA_METADATA_RAFT_COMMAND_QUEUE_CAPACITY")]
metadata_raft_command_queue_capacity: Option<crabka_raft::MetadataRaftCommandQueueCapacity>,

#[arg(long, env = "CRABKA_METADATA_RAFT_FETCH_MAX")]
metadata_raft_fetch_max: Option<crabka_raft::MetadataRaftFetchMax>,
```

Implement `FromStr` on the three Task 1 types so Clap parses through their
validated constructors. Manually copy their primitive/UOM values into
`RuntimeFileConfig` in `runtime_overlay`:

```rust
runtime.controller_fetch_miss_limit =
    self.controller_fetch_miss_limit.map(|value| value.get());
runtime.metadata_raft_command_queue_capacity = self
    .metadata_raft_command_queue_capacity
    .map(|value| value.get());
runtime.metadata_raft_fetch_max =
    self.metadata_raft_fetch_max.map(|value| value.size());
```

In `broker.rs`, construct:

```rust
heartbeat_interval: config
    .controller_heartbeat_interval_explicit
    .then_some(config.controller_heartbeat_interval),
fetch_miss_limit: config.controller_fetch_miss_limit,
command_queue_capacity: config.metadata_raft_command_queue_capacity,
metadata_raft_fetch_max: config.metadata_raft_fetch_max,
```

Update explicit `BrokerConfig` and `ControllerConfig` fixtures with default
values; do not change their behavioral inputs.

- [x] **Step 6: Verify and commit broker ownership**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-broker --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
```

Commit only Task 2 files:

```bash
git add crates/broker/src/config.rs crates/broker/src/file_config.rs \
  crates/broker/src/bin/broker.rs crates/broker/src/broker.rs
git commit -m "feat(broker): expose raft runtime policy"
```

### Task 3: Kafka CRD propagation

**Files:**
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: operator CRD/rendering tests
- Modify generated: `deploy/crds/crabka.io_kafkas.yaml`

**Interfaces:**
- Consumes: broker runtime TOML keys from Task 2.
- Produces: optional `Kafka.spec.brokerTuning` fields `controllerFetchMissLimit`, `metadataRaftCommandQueueCapacity`, and `metadataRaftFetchMax`.

- [x] **Step 1: Write failing CRD rendering and validation tests**

Construct `BrokerTuning` with:

```rust
controller_fetch_miss_limit: Some(5),
metadata_raft_command_queue_capacity: Some(7),
metadata_raft_fetch_max: Some(kibibytes(512)),
```

Assert rendered TOML contains:

```toml
controller_fetch_miss_limit = 5
metadata_raft_command_queue_capacity = 7
metadata_raft_fetch_max = "512KiB"
```

Add validation cases rejecting zero counts, `0B`, `1.5B`, and
`2147483648B`. Add an omission test asserting a default `BrokerTuning` renders
none of the three keys.

- [x] **Step 2: Run focused operator tests and verify the red state**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator raft_runtime_policy --locked
```

Expected: compilation fails because the three CRD fields do not exist.

- [x] **Step 3: Add the three fields to the existing broker-tuning macro**

Add:

```rust
refined #[schemars(range(min = 1))]
controller_fetch_miss_limit: u32 => refined_type::rule::GreaterU32<0>;
refined #[schemars(range(min = 1))]
metadata_raft_command_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
size_i32
#[serde(with = "crabka_units::serde_units::human::option_byte_size")]
#[schemars(with = "Option<String>")]
metadata_raft_fetch_max: ByteSize => ();
```

Reuse the macro's existing validation and runtime-TOML rendering. Do not add a
second heartbeat field.

- [x] **Step 4: Regenerate and verify the Kafka CRD**

Run:

```bash
tools/regen-crds.sh
git diff --check
sha256sum deploy/crds/crabka.io_kafkas.yaml > /var/tmp/raft-policy-crd.sha256
tools/regen-crds.sh
sha256sum --check /var/tmp/raft-policy-crd.sha256
```

Assert `deploy/crds/crabka.io_kafkas.yaml` contains camel-case properties with
minimum `1` for both counts and a string schema for
`metadataRaftFetchMax`. The checksum check proves the second generation is
byte-identical to the first.

- [x] **Step 5: Verify and commit CRD propagation**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p crabka-operator --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
```

Commit only Task 3 files:

```bash
git add crates/operator/src/crd/kafka.rs deploy/crds/crabka.io_kafkas.yaml
git commit -m "feat(operator): expose raft runtime policy"
```

### Task 4: Audit and close the slice

**Files:**
- Modify: `docs/configuration-audit.md`
- Modify: `docs/superpowers/plans/2026-07-31-raft-runtime-policy.md`

**Interfaces:**
- Consumes: verified implementation and exact test counts from Tasks 1-3.
- Produces: completed plan and permanent audit record.

- [x] **Step 1: Audit ownership and removed constants**

Use `rg` to prove:

- `FETCH_MISS_LIMIT` and `MAX_APPLY` are removed;
- `HEARTBEAT_DIVISOR` remains fixed;
- each CLI/env/TOML/CRD name has one owner;
- every `ControllerConfig` and `KraftConfig` construction supplies the new
  fields;
- snapshot total-size policy remains separate from request chunk size.

- [x] **Step 2: Run workspace gates**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo check --workspace --all-targets --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all -- --check
git diff --check
```

- [x] **Step 3: Re-run focused all-target tests after formatting**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-raft -p crabka-broker -p crabka-operator \
  --all-targets --locked
```

- [x] **Step 4: Document and commit**

Record exact defaults, names, validation, omission semantics, CRD ownership,
bounded-read progress behavior, removed constants, generated schema, test
counts, and workspace gates in `configuration-audit.md`. Replace the earlier
pending Raft audit paragraph with the completed result. Mark every plan
checkbox complete, run `git diff --check`, and commit:

```bash
git add docs/configuration-audit.md \
  docs/superpowers/plans/2026-07-31-raft-runtime-policy.md
git commit -m "docs(config): close raft runtime policy"
```
