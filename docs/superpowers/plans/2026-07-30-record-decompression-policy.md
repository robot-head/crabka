# Shared Record Decompression Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace duplicated Kafka record decompression budgets with one validated UOM policy configurable through the broker and Kafka CRD.

**Architecture:** `crabka-compression` owns the policy and budget calculation. Protocol and legacy decoders retain default-compatible entry points and add explicit policy-aware variants; only the broker's untrusted Produce fallback supplies deployment configuration. Existing broker runtime TOML and operator `BrokerTuning` carry the three values.

**Tech Stack:** Rust, `crabka-units`, `refined_type`, Clap environment arguments, Serde/TOML, kube/schemars CRDs.

## Global Constraints

- Preserve defaults: ratio `100`, output floor `16MiB`, output ceiling `1GiB`.
- Ratio and ceiling may be lowered but never raised above their fixed security bounds.
- Require finite positive ratio, positive whole-byte sizes, and `output_floor <= output_ceiling`.
- Keep Kafka wire masks, format identifiers, codec framing, and v2 verbatim passthrough fixed.
- Preserve existing public decode entry points by routing them through the default policy.
- Do not modify or stage the four protected untracked plans dated 2026-07-28.
- Treat task commits as logical checkpoints in this already-dirty worktree.
  Do not stage or commit unless every listed path has been reviewed and the
  user explicitly requests publication; never sweep pre-existing changes into
  a task commit.
- Run Cargo with `TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0` and `--locked` after dependency lock updates.

---

### Task 1: Shared validated decompression policy

**Files:**
- Modify: `crates/compression/Cargo.toml`
- Modify: `crates/compression/src/lib.rs`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces: `RecordDecompressionPolicy::new(Ratio, ByteSize, ByteSize) -> Result<Self, String>`
- Produces: `RecordDecompressionPolicy::output_limit(self, ByteSize) -> ByteSize`
- Produces: `RECORD_DECOMPRESSION_HARD_MAX_RATIO`, `RECORD_DECOMPRESSION_HARD_MAX_OUTPUT`

- [x] **Step 1: Add failing policy tests**

Add tests covering defaults, the linear range, both clamps, and rejected values:

```rust
#[test]
fn record_policy_preserves_existing_budget_curve() {
    let policy = RecordDecompressionPolicy::default();
    assert2::check!(policy.output_limit(bytes(1)) == mebibytes(16));
    assert2::check!(policy.output_limit(mebibytes(1)) == mebibytes(100));
    assert2::check!(policy.output_limit(mebibytes(11)) == gibibytes(1));
}

#[test]
fn record_policy_rejects_invalid_or_weakened_security_bounds() {
    for result in [
        RecordDecompressionPolicy::new(fraction(0.0), mebibytes(16), gibibytes(1)),
        RecordDecompressionPolicy::new(fraction(101.0), mebibytes(16), gibibytes(1)),
        RecordDecompressionPolicy::new(fraction(100.0), gibibytes(1), mebibytes(16)),
        RecordDecompressionPolicy::new(fraction(100.0), mebibytes(16), gibibytes(2)),
        RecordDecompressionPolicy::new(
            fraction(100.0),
            ByteSize::from_bytes_f64(0.5),
            gibibytes(1),
        ),
    ] {
        assert2::check!(result.is_err());
    }
}
```

- [x] **Step 2: Run the RED gate**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-compression record_policy --locked
```

Expected: compilation fails because `RecordDecompressionPolicy` does not exist.

- [x] **Step 3: Implement the minimal shared policy**

Add the existing workspace `refined_type` dependency. Implement a copyable policy:

```rust
pub const RECORD_DECOMPRESSION_HARD_MAX_RATIO: Ratio = fraction(100.0);
pub const RECORD_DECOMPRESSION_HARD_MAX_OUTPUT: ByteSize = gibibytes(1);

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordDecompressionPolicy {
    max_ratio: Ratio,
    output_floor: ByteSize,
    output_ceiling: ByteSize,
}

impl RecordDecompressionPolicy {
    pub fn new(
        max_ratio: Ratio,
        output_floor: ByteSize,
        output_ceiling: ByteSize,
    ) -> Result<Self, String> {
        let ratio = max_ratio.as_f64();
        if !ratio.is_finite()
            || ratio <= 0.0
            || max_ratio > RECORD_DECOMPRESSION_HARD_MAX_RATIO
        {
            return Err(
                "record decompression ratio must be finite and within 0 < ratio <= 100".into(),
            );
        }
        let floor = validated_whole_bytes("output floor", output_floor)?;
        let ceiling = validated_whole_bytes("output ceiling", output_ceiling)?;
        if floor > ceiling {
            return Err("record decompression output floor exceeds ceiling".into());
        }
        Ok(Self {
            max_ratio,
            output_floor,
            output_ceiling,
        })
    }

    #[must_use]
    pub fn output_limit(self, compressed: ByteSize) -> ByteSize {
        ByteSize::from_bytes_f64(compressed.bytes_f64() * self.max_ratio.as_f64())
            .max(self.output_floor)
            .min(self.output_ceiling)
    }
}
```

Provide getters for all three values and `Default` with the exact existing values. Use a private `MinMaxU64<1, 1_073_741_824>`-backed helper for whole-byte validation rather than another public wrapper.

```rust
fn validated_whole_bytes(name: &str, value: ByteSize) -> Result<u64, String> {
    let raw = value.bytes_f64();
    if !raw.is_finite() || raw.fract() != 0.0 {
        return Err(format!("{name} must be a positive whole number of bytes"));
    }
    refined_type::rule::MinMaxU64::<1, 1_073_741_824>::new(value.bytes_u64())
        .map(refined_type::Refined::into_value)
        .map_err(|error| format!("{name}: {error}"))
}
```

- [x] **Step 4: Update the lockfile offline and run GREEN**

Run:

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-compression record_policy --offline
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-compression --locked
```

Expected: all compression tests pass.

- [x] **Step 5: Commit**

```bash
git add Cargo.lock crates/compression/Cargo.toml crates/compression/src/lib.rs
git commit -m "feat(compression): add record decode policy"
```

### Task 2: Policy-aware v2 record decoding

**Files:**
- Modify: `crates/protocol/src/records/owned.rs`
- Modify: `crates/protocol/src/records/borrowed.rs`
- Modify: `crates/protocol/src/records/payload.rs`

**Interfaces:**
- Consumes: `crabka_compression::RecordDecompressionPolicy`
- Produces: `RecordBatch::decode_with_policy`
- Produces: borrowed `RecordBatch::decode_borrow_with_policy`
- Produces: `RecordsPayload::from_bytes_with_policy`

- [x] **Step 1: Add failing v2 policy tests**

Encode a compressed batch whose decompressed body exceeds a deliberately small
policy ceiling. Assert default decode succeeds and explicit decode returns
`RecordsError::Compression(CompressionError::TooLarge { .. })`. Exercise owned,
borrowed, and `RecordsPayload` paths with the same encoded bytes.

```rust
let mut batch = fixture_single_record_batch();
batch.records[0].value = Some(Bytes::from(vec![b'x'; 4096]));
batch.attributes = batch.attributes.with_compression(CompressionType::Lz4);
let mut wire = BytesMut::new();
batch.encode(&mut wire).unwrap();
let policy =
    RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();

let mut owned = &wire[..];
assert2::assert!(matches!(
    RecordBatch::decode_with_policy(&mut owned, policy),
    Err(RecordsError::Compression(CompressionError::TooLarge { .. }))
));
let mut borrowed = &wire[..];
assert2::assert!(
    borrowed::RecordBatch::decode_borrow_with_policy(&mut borrowed, policy).is_err()
);
assert2::assert!(
    RecordsPayload::from_bytes_with_policy(wire.freeze(), policy).is_err()
);
```

- [x] **Step 2: Run the RED gate**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-protocol decompression_policy --locked
```

Expected: compilation fails on the missing policy-aware methods.

- [x] **Step 3: Centralize v2 budget use**

Add:

```rust
pub fn decode_with_policy<B: Buf>(
    buf: &mut B,
    policy: RecordDecompressionPolicy,
) -> Result<Self, RecordsError>;

pub fn from_bytes_with_policy(
    bytes: Bytes,
    policy: RecordDecompressionPolicy,
) -> Result<Self, RecordsError>;
```

Replace both local 16 MiB / 100× / 1 GiB calculations with
`policy.output_limit(ByteSize::from_bytes(body_len))`. Existing `decode`,
`Decode`, `decode_borrow`, and `from_bytes` call the policy-aware functions with
`RecordDecompressionPolicy::default()`.

- [x] **Step 4: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-protocol decompression_policy --locked
```

Expected: policy tests pass and existing record tests remain green.

- [x] **Step 5: Commit**

```bash
git add crates/protocol/src/records/owned.rs \
  crates/protocol/src/records/borrowed.rs \
  crates/protocol/src/records/payload.rs
git commit -m "refactor(protocol): share record decode limits"
```

### Task 3: Policy-aware legacy record decoding

**Files:**
- Modify: `crates/records-legacy/src/set.rs`
- Modify: `crates/records-legacy/src/bridge.rs`
- Modify: `crates/records-legacy/src/lib.rs`

**Interfaces:**
- Consumes: `RecordDecompressionPolicy`
- Produces: `decode_message_set_with_policy`
- Produces: `legacy_to_v2_with_policy`

- [x] **Step 1: Add a failing legacy boundary test**

Compress a valid legacy message set, then decode it with a policy whose
effective ceiling is below the decompressed size. Assert the explicit path
returns `CompressionError::TooLarge`, while `decode_message_set` retains the
default behavior.

```rust
let records = vec![ParsedRecord {
    offset: Offset(0),
    timestamp: Some(1),
    key: None,
    value: Some(Bytes::from(vec![b'x'; 4096])),
}];
let mut wire = BytesMut::new();
encode_compressed_message_set(&records, Magic::V1, CompressionType::Lz4, &mut wire)
    .unwrap();
let policy =
    RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
let mut limited = &wire[..];
assert2::assert!(
    decode_message_set_with_policy(&mut limited, wire.len(), policy).is_err()
);
```

- [x] **Step 2: Run RED**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-records-legacy decompression_policy --locked
```

Expected: compilation fails on the missing policy-aware functions.

- [x] **Step 3: Replace the three local constants**

Implement:

```rust
pub fn decode_message_set_with_policy<B: Buf>(
    buf: &mut B,
    set_size_bytes: usize,
    policy: RecordDecompressionPolicy,
) -> Result<Vec<ParsedRecord>, LegacyRecordsError>;

pub fn legacy_to_v2_with_policy(
    set_bytes: &[u8],
    policy: RecordDecompressionPolicy,
) -> Result<RecordBatch, LegacyRecordsError>;
```

Thread `policy` through recursive `decode_into`. Keep nested compression
rejection unchanged. Existing functions call these variants with `Default`.
Delete `MAX_EXPANSION`, `MIN_DECOMPRESSED`, `MAX_DECOMPRESSED`, and their local
budget function.

- [x] **Step 4: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-records-legacy --locked
```

- [x] **Step 5: Commit**

```bash
git add crates/records-legacy/src/set.rs \
  crates/records-legacy/src/bridge.rs \
  crates/records-legacy/src/lib.rs
git commit -m "refactor(records): share legacy decode limits"
```

### Task 4: Broker configuration surface

**Files:**
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/file_config.rs`
- Modify: `crates/broker/src/bin/broker.rs`

**Interfaces:**
- Consumes: `RecordDecompressionPolicy::new`
- Produces: three validated `BrokerConfig` fields
- Produces: CLI/environment and `[runtime]` overlays

- [x] **Step 1: Add failing configuration tests**

Cover defaults, CLI/environment parsing, TOML application, floor-above-ceiling,
ratio above 100, ceiling above 1 GiB, and fractional byte rejection. Assert the
three defaults construct `RecordDecompressionPolicy::default()`.

```rust
#[test]
fn record_decompression_defaults_match_shared_policy() {
    let cfg = BrokerConfig::default();
    assert2::assert!(
        cfg.record_decompression_policy().unwrap()
            == RecordDecompressionPolicy::default()
    );
}

#[test]
fn record_decompression_rejects_weakened_security_bounds() {
    let cfg = BrokerConfig {
        record_decompression_max_ratio: fraction(101.0),
        ..BrokerConfig::default()
    };
    assert2::assert!(cfg.validate().is_err());
}
```

- [x] **Step 2: Run RED**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker record_decompression --locked
```

- [x] **Step 3: Add broker fields and validation**

Add UOM `Ratio` / `ByteSize` fields using the shared defaults. In
`BrokerConfig::validate`, construct `RecordDecompressionPolicy`; map failure to
`BrokerError::InvalidRuntimeConfig` naming `record_decompression`.
Add `BrokerConfig::record_decompression_policy() ->
Result<RecordDecompressionPolicy, BrokerError>` and have validation and Produce
reuse that single constructor path.

Add CLI/env fields:

```rust
#[arg(long, env = "CRABKA_RECORD_DECOMPRESSION_MAX_RATIO",
      value_parser = crabka_units::parse::positive_ratio)]
record_decompression_max_ratio: Option<Ratio>,

#[arg(long, env = "CRABKA_RECORD_DECOMPRESSION_OUTPUT_FLOOR",
      value_parser = crabka_units::parse::positive_byte_size)]
record_decompression_output_floor: Option<ByteSize>,

#[arg(long, env = "CRABKA_RECORD_DECOMPRESSION_OUTPUT_CEILING",
      value_parser = crabka_units::parse::positive_byte_size)]
record_decompression_output_ceiling: Option<ByteSize>,
```

Add matching `RuntimeFileConfig` fields with human UOM Serde, validate whole
bytes before assignment, and include all three in the runtime overlay.

- [x] **Step 4: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --lib record_decompression --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --bin crabka-broker runtime_policy_cli --locked
```

- [x] **Step 5: Commit**

```bash
git add crates/broker/src/config.rs crates/broker/src/file_config.rs \
  crates/broker/src/bin/broker.rs
git commit -m "feat(broker): expose record decode policy"
```

### Task 5: Thread policy through Produce fallback decoding

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`
- Test: `crates/broker/tests/produce_legacy_upconvert.rs`

**Interfaces:**
- Consumes: broker record-decompression fields
- Consumes: `RecordsPayload::from_bytes_with_policy`
- Consumes: `legacy_to_v2_with_policy`

- [x] **Step 1: Add failing v2 and legacy Produce tests**

Configure a ceiling below each compressed payload's decompressed size and
assert Produce returns `INVALID_RECORD`. Repeat with defaults and assert both
formats retain their existing successful behavior.

```rust
let policy =
    RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
let metrics = crate::metrics::BrokerMetrics::new();
let error = prepare_batch(
    PartitionPayload::Slice(compressed_v2_wire),
    Some(CompressionType::Zstd),
    "t",
    &metrics,
    policy,
)
.unwrap_err();
assert2::assert!(error == codes::INVALID_RECORD);

let error = decode_owned_batch(
    RecordsPayload::Legacy(compressed_legacy_wire),
    "t",
    &metrics,
    policy,
)
.unwrap_err();
assert2::assert!(error == codes::INVALID_RECORD);
```

- [x] **Step 2: Run RED**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --test produce_legacy_upconvert record_decompression --locked
```

- [x] **Step 3: Pass one policy value through the fallback**

Construct the already-validated policy once per request from `broker.config`.
Add it to `prepare_batch` and `decode_owned_batch`. Use
`RecordsPayload::from_bytes_with_policy` for v2 and
`legacy_to_v2_with_policy` for legacy. Do not touch the header-only verbatim
path.

- [x] **Step 4: Run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --test produce_legacy_upconvert --locked
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-broker --test produce_verbatim_passthrough --locked
```

- [x] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/produce.rs \
  crates/broker/tests/produce_legacy_upconvert.rs
git commit -m "feat(broker): apply record decode limits"
```

### Task 6: Kafka CRD and operator rendering

**Files:**
- Modify: `crates/operator/Cargo.toml`
- Modify: `crates/operator/src/crd/kafka.rs`
- Modify: `deploy/crds/crabka.io_kafkas.yaml`
- Modify: `Cargo.lock`

**Interfaces:**
- Consumes: shared policy validation
- Produces: three `BrokerTuning` fields and rendered broker runtime TOML

- [x] **Step 1: Add failing CRD tests**

Deserialize `100`, `16MiB`, and `1GiB`; validate and render them. Parse the
rendered TOML with `crabka_broker::file_config::FileConfig` and assert the
effective broker values. Add invalid cases for ratio 101, floor above ceiling,
and ceiling above 1 GiB.

```rust
let tuning: BrokerTuning = serde_json::from_value(serde_json::json!({
    "recordDecompressionMaxRatio": "100",
    "recordDecompressionOutputFloor": "16MiB",
    "recordDecompressionOutputCeiling": "1GiB"
}))
.unwrap();
tuning.validate().unwrap();
let rendered = tuning.render_runtime_toml();
let file: crabka_broker::file_config::FileConfig = toml::from_str(&rendered).unwrap();
let mut broker = crabka_broker::BrokerConfig::default();
file.apply_to(&mut broker).unwrap();
assert2::assert!(
    broker.record_decompression_policy().unwrap()
        == RecordDecompressionPolicy::default()
);
```

- [x] **Step 2: Run RED**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator broker_tuning_record_decompression --locked
```

- [x] **Step 3: Add CRD fields and shared validation**

Add `crabka-compression` as a direct dependency. Define the fields in
`BrokerTuning` with human ratio/byte-size Serde. In relational validation,
construct `RecordDecompressionPolicy` from configured values or shared
defaults and map errors to `spec.brokerTuning.recordDecompression*`.

- [x] **Step 4: Regenerate CRDs and run GREEN**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  tools/regen-crds.sh
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-operator broker_tuning_record_decompression --locked
```

- [x] **Step 5: Commit**

```bash
git add Cargo.lock crates/operator/Cargo.toml \
  crates/operator/src/crd/kafka.rs deploy/crds/crabka.io_kafkas.yaml
git commit -m "feat(operator): expose record decode policy"
```

### Task 7: Audit closure and full verification

**Files:**
- Modify: `docs/configuration-audit.md`

**Interfaces:**
- Consumes: all prior task deliverables
- Produces: completed `records-legacy` coverage entry and verification evidence

- [x] **Step 1: Update the audit**

Add a `Records Legacy Decompression Policy` section classifying the two wire
bit masks as fixed, documenting the shared configured policy, and moving
`records-legacy` from Pending to Complete.

- [x] **Step 2: Run focused owner tests**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p crabka-compression -p crabka-protocol \
    -p crabka-records-legacy --locked
```

- [x] **Step 3: Run workspace gates**

```bash
TMPDIR=/var/tmp RUSTC_WRAPPER= CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy --workspace --all-targets --locked -- -D warnings
cargo +nightly fmt --all
git diff --check
```

Expected: all commands exit zero; no protected plan is staged or modified.

- [x] **Step 4: Commit audit closure**

```bash
git add docs/configuration-audit.md
git commit -m "docs(config): close records legacy audit"
```
