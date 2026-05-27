# Typed `RecordBatch` (sub-plan 1c) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a typed `RecordBatch` v2 decoder/encoder to `crabka-protocol` that consumes `crabka-compression`, switching `records` fields in generated messages from opaque `Bytes` to a fully typed view.

**Architecture:** Fixed 61-byte v2 header reinterpreted via `zerocopy` (no allocation, BigEndian numeric wrappers). Variable-length record bodies decoded by varint helpers, materialised eagerly for owned, lazily for borrowed. Compressed batches flow through `crabka-compression`. Owned + borrowed flavors mirror the existing codec pattern; `to_owned()` bridges them.

**Tech Stack:** Rust 1.95.0 (edition 2024); new deps `zerocopy = "0.8"` (derive), `crc32c = "0.6"`; existing `crabka-compression`. All tests use the parameterized-fixture pattern (table-driven + `macro_rules!` + factored proptest `Strategy`s).

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-records-1c-design.md`](../specs/2026-05-11-crabka-records-1c-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Implementation runs on `feature/records-1c`, branched from `plan/records-1c` (or from `main` after PR #20 merges). All commits land on the feature branch.

---

## File structure (created in this plan)

```
crates/protocol/src/records/
├── mod.rs              # public re-exports
├── header.rs           # RecordBatchHeader (zerocopy), Attributes, TimestampType
├── crc.rs              # CRC-32C wrapper + table tests
├── owned.rs            # RecordBatch, Record, RecordHeader (owned)
├── borrowed.rs         # RecordBatch<'a>, Record<'a>, RecordHeader<'a>
└── error.rs            # RecordsError + From<RecordsError> for ProtocolError

crates/protocol/tests/
├── proptest_records.rs      # per-codec proptest round-trips (table-driven)
└── differential_records.rs  # JVM differential (one test per codec)

crates/protocol/benches/
└── records.rs               # CodSpeed bench: encode + decode × 5 codecs

crates/protocol-codegen/src/type_map.rs   # records → RecordBatch mapping

tools/oracle/src/main/java/com/crabka/oracle/Oracle.java
                              # adds record_batch_encode / record_batch_decode ops
```

---

## Phase A — Foundations: deps, error, CRC, attributes

### Task 1: Add `zerocopy` and `crc32c` to the workspace

**Files:**
- Modify: `Cargo.toml` (repo root) — add workspace deps

- [ ] **Step 1: Append to `[workspace.dependencies]`**

In `Cargo.toml` at repo root:

```toml
zerocopy = { version = "0.8", features = ["derive"] }
crc32c = "0.6"
```

Leave the existing entries unchanged.

- [ ] **Step 2: Verify the workspace still resolves**

```bash
cargo metadata --no-deps 2>&1 | tail -3
```

Expected: no errors. (Nothing consumes the new deps yet; this is a manifest check.)

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml
git commit -m "chore(deps): add zerocopy and crc32c to workspace"
```

---

### Task 2: Wire `crabka-compression` into `crabka-protocol` + add feature mirrors

`crabka-protocol` will depend on `crabka-compression`. To preserve consumer choice of which codecs to compile, `crabka-protocol` exposes mirror features that forward.

**Files:**
- Modify: `crates/protocol/Cargo.toml`

- [ ] **Step 1: Add the dep + features**

In `crates/protocol/Cargo.toml`:

```toml
[dependencies]
# ... existing entries ...
zerocopy = { workspace = true }
crc32c = { workspace = true }
crabka-compression = { path = "../compression", default-features = false }

[features]
default = ["gzip", "snappy", "lz4", "zstd"]
gzip   = ["crabka-compression/gzip"]
snappy = ["crabka-compression/snappy"]
lz4    = ["crabka-compression/lz4"]
zstd   = ["crabka-compression/zstd"]
```

(If a `[features]` section already exists with an `arbitrary` feature or similar, merge: keep existing entries, add the four new codec features.)

- [ ] **Step 2: Verify cross-feature compilation**

```bash
cargo build -p crabka-protocol
cargo build -p crabka-protocol --no-default-features
cargo build -p crabka-protocol --no-default-features --features gzip
cargo build -p crabka-protocol --no-default-features --features "gzip snappy lz4 zstd"
```

Each must exit 0. The crate currently has no code referencing `crabka-compression`, so this is a manifest-only check.

- [ ] **Step 3: Commit**

```bash
git add crates/protocol/Cargo.toml
git commit -m "feat(protocol): depend on crabka-compression with mirror features"
```

---

### Task 3: `RecordsError` and `From` impl

**Files:**
- Create: `crates/protocol/src/records/mod.rs`
- Create: `crates/protocol/src/records/error.rs`
- Modify: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Create the records module skeleton**

`crates/protocol/src/records/mod.rs`:

```rust
//! Typed v2 record batch decoder/encoder.
//!
//! See `docs/superpowers/specs/2026-05-11-crabka-records-1c-design.md`.
//! v0/v1 record batches are deferred to `crabka-log`.

mod error;

pub use error::RecordsError;
```

- [ ] **Step 2: Implement `RecordsError`**

`crates/protocol/src/records/error.rs`:

```rust
//! Errors specific to record-batch decoding/encoding.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RecordsError {
    #[error("buffer too short for batch header (need {needed} more bytes)")]
    HeaderTooShort { needed: usize },

    #[error("batch magic byte {found} unsupported (only v2 supported)")]
    UnsupportedMagic { found: i8 },

    #[error("CRC mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    CrcMismatch { expected: u32, computed: u32 },

    #[error("batch body truncated (need {needed} more bytes)")]
    BodyTooShort { needed: usize },

    #[error("record parse failed: {0}")]
    RecordParse(String),

    #[error("compression: {0}")]
    Compression(#[from] crabka_compression::CompressionError),

    #[error("zerocopy reinterpretation failed")]
    ZerocopyFailure,
}

impl From<RecordsError> for crate::ProtocolError {
    fn from(e: RecordsError) -> Self {
        crate::ProtocolError::InvalidValue(match e {
            RecordsError::HeaderTooShort { .. } => "records: header too short",
            RecordsError::UnsupportedMagic { .. } => "records: unsupported magic",
            RecordsError::CrcMismatch { .. } => "records: CRC mismatch",
            RecordsError::BodyTooShort { .. } => "records: body truncated",
            RecordsError::RecordParse(_) => "records: record parse failed",
            RecordsError::Compression(_) => "records: compression error",
            RecordsError::ZerocopyFailure => "records: zerocopy failure",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages() {
        let cases: &[(RecordsError, &str)] = &[
            (RecordsError::HeaderTooShort { needed: 4 }, "buffer too short for batch header"),
            (RecordsError::UnsupportedMagic { found: 1 }, "batch magic byte 1 unsupported"),
            (RecordsError::CrcMismatch { expected: 0xDEADBEEF, computed: 0x12345678 },
             "CRC mismatch: expected 0xdeadbeef"),
            (RecordsError::BodyTooShort { needed: 17 }, "batch body truncated"),
            (RecordsError::RecordParse("bad varint".into()), "record parse failed"),
            (RecordsError::ZerocopyFailure, "zerocopy reinterpretation failed"),
        ];
        for (err, contains) in cases {
            assert!(
                err.to_string().contains(contains),
                "{} did not contain {:?}",
                err,
                contains
            );
        }
    }

    #[test]
    fn into_protocol_error_is_invalid_value() {
        let e: crate::ProtocolError = RecordsError::UnsupportedMagic { found: 0 }.into();
        assert!(matches!(e, crate::ProtocolError::InvalidValue(_)));
    }
}
```

- [ ] **Step 3: Hook the records module into the crate**

Modify `crates/protocol/src/lib.rs` to add `pub mod records;` next to the other `pub mod` declarations.

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol records::error
```

Expected: 2 tests pass (`display_messages`, `into_protocol_error_is_invalid_value`).

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): RecordsError + ProtocolError conversion"
```

---

### Task 4: CRC-32C wrapper module

CRC-32C is computed over everything after the `crc` field in the header (i.e., bytes 21 onward of the batch body, where byte 0 is `base_offset`).

**Files:**
- Create: `crates/protocol/src/records/crc.rs`
- Modify: `crates/protocol/src/records/mod.rs`

- [ ] **Step 1: Write the module**

`crates/protocol/src/records/crc.rs`:

```rust
//! CRC-32C (Castagnoli) wrapping the `crc32c` crate. Kafka v2 record batches
//! use this CRC over everything after the `crc` field of the header.

/// CRC-32C of the input.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Standard CRC-32C reference vectors.
    /// "123456789" -> 0xE3069283 (RFC 3720 / iSCSI).
    const VECTORS: &[(&[u8], u32)] = &[
        (b"",          0x0000_0000),
        (b"a",         0xC1D04330),
        (b"123456789", 0xE306_9283),
        (b"The quick brown fox jumps over the lazy dog", 0x22620404),
    ];

    #[test]
    fn known_vectors() {
        for (input, expected) in VECTORS {
            let got = crc32c(input);
            assert_eq!(
                got, *expected,
                "input={:?}: expected {:#010x}, got {:#010x}",
                input, expected, got
            );
        }
    }
}
```

- [ ] **Step 2: Hook up the module**

`crates/protocol/src/records/mod.rs`:

```rust
mod crc;
mod error;

pub use error::RecordsError;
```

(Module `crc` is not re-exported — internal use only.)

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol records::crc
```

Expected: 1 test passes (`known_vectors`).

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): CRC-32C wrapper with reference vectors"
```

---

### Task 5: `Attributes` + `TimestampType`

**Files:**
- Create: `crates/protocol/src/records/header.rs`
- Modify: `crates/protocol/src/records/mod.rs`

- [ ] **Step 1: Write `Attributes` and `TimestampType` (header struct comes next task)**

`crates/protocol/src/records/header.rs`:

```rust
//! Record-batch v2 header types: `RecordBatchHeader` (zerocopy),
//! `Attributes`, `TimestampType`.

use crabka_compression::CompressionType;

/// Timestamp-type bit in the attributes word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

/// Packed batch-level attributes, encoded as a 16-bit big-endian field
/// in the wire header.
///
/// - bits 0-2: compression type (matches `CompressionType::as_attribute_bits`)
/// - bit 3:    timestamp type (0 = CreateTime, 1 = LogAppendTime)
/// - bit 4:    is_transactional
/// - bit 5:    is_control_batch
/// - bit 6:    has_delete_horizon_ms (Kafka 2.8+; not surfaced separately here)
/// - bits 7-15: reserved
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attributes(pub i16);

impl Attributes {
    pub const TIMESTAMP_TYPE_BIT: i16 = 1 << 3;
    pub const TRANSACTIONAL_BIT:  i16 = 1 << 4;
    pub const CONTROL_BIT:        i16 = 1 << 5;

    #[must_use]
    pub fn compression(self) -> CompressionType {
        // The low 3 bits are the codec id. Wider attribute bits are ignored.
        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let byte = (self.0 & 0x07) as u8;
        CompressionType::from_attribute_bits(byte).unwrap_or(CompressionType::None)
    }

    #[must_use]
    pub fn timestamp_type(self) -> TimestampType {
        if self.0 & Self::TIMESTAMP_TYPE_BIT != 0 {
            TimestampType::LogAppendTime
        } else {
            TimestampType::CreateTime
        }
    }

    #[must_use]
    pub fn is_transactional(self) -> bool {
        self.0 & Self::TRANSACTIONAL_BIT != 0
    }

    #[must_use]
    pub fn is_control_batch(self) -> bool {
        self.0 & Self::CONTROL_BIT != 0
    }

    #[must_use]
    pub fn with_compression(self, c: CompressionType) -> Self {
        let cleared = self.0 & !0x07;
        Self(cleared | i16::from(c.as_attribute_bits()))
    }

    #[must_use]
    pub fn with_timestamp_type(self, t: TimestampType) -> Self {
        match t {
            TimestampType::CreateTime => Self(self.0 & !Self::TIMESTAMP_TYPE_BIT),
            TimestampType::LogAppendTime => Self(self.0 | Self::TIMESTAMP_TYPE_BIT),
        }
    }

    #[must_use]
    pub fn with_transactional(self, b: bool) -> Self {
        if b { Self(self.0 | Self::TRANSACTIONAL_BIT) }
        else { Self(self.0 & !Self::TRANSACTIONAL_BIT) }
    }

    #[must_use]
    pub fn with_control(self, b: bool) -> Self {
        if b { Self(self.0 | Self::CONTROL_BIT) }
        else { Self(self.0 & !Self::CONTROL_BIT) }
    }
}

impl Default for Attributes {
    fn default() -> Self { Self(0) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_compression::CompressionType;

    macro_rules! attr_case {
        ($name:ident, $bits:expr, $codec:expr, $ts:expr, $txn:expr, $ctrl:expr) => {
            #[test]
            fn $name() {
                let a = Attributes($bits);
                assert_eq!(a.compression(),       $codec,  "compression mismatch in {}", stringify!($name));
                assert_eq!(a.timestamp_type(),    $ts,     "timestamp_type mismatch in {}", stringify!($name));
                assert_eq!(a.is_transactional(),  $txn,    "is_transactional mismatch in {}", stringify!($name));
                assert_eq!(a.is_control_batch(),  $ctrl,   "is_control_batch mismatch in {}", stringify!($name));
            }
        };
    }

    attr_case!(zero,           0,                 CompressionType::None,   TimestampType::CreateTime,    false, false);
    attr_case!(gzip_only,      0b0000_0000_0000_0001, CompressionType::Gzip,   TimestampType::CreateTime,    false, false);
    attr_case!(snappy_only,    0b0000_0000_0000_0010, CompressionType::Snappy, TimestampType::CreateTime,    false, false);
    attr_case!(lz4_only,       0b0000_0000_0000_0011, CompressionType::Lz4,    TimestampType::CreateTime,    false, false);
    attr_case!(zstd_only,      0b0000_0000_0000_0100, CompressionType::Zstd,   TimestampType::CreateTime,    false, false);
    attr_case!(log_append,     0b0000_0000_0000_1000, CompressionType::None,   TimestampType::LogAppendTime, false, false);
    attr_case!(transactional,  0b0000_0000_0001_0000, CompressionType::None,   TimestampType::CreateTime,    true,  false);
    attr_case!(control,        0b0000_0000_0010_0000, CompressionType::None,   TimestampType::CreateTime,    false, true);
    attr_case!(all_set,
        0b0000_0000_0011_1100,
        CompressionType::Zstd,
        TimestampType::LogAppendTime,
        true, true);

    #[test]
    fn builder_round_trip() {
        let a = Attributes::default()
            .with_compression(CompressionType::Snappy)
            .with_timestamp_type(TimestampType::LogAppendTime)
            .with_transactional(true)
            .with_control(false);

        assert_eq!(a.compression(),       CompressionType::Snappy);
        assert_eq!(a.timestamp_type(),    TimestampType::LogAppendTime);
        assert!(a.is_transactional());
        assert!(!a.is_control_batch());
    }

    #[test]
    fn replacing_compression_clears_old_bits() {
        // Starting with Lz4 (bits 0-2 = 011), switching to Gzip (= 001)
        // must clear bit 1, not OR over it.
        let a = Attributes::default().with_compression(CompressionType::Lz4);
        let b = a.with_compression(CompressionType::Gzip);
        assert_eq!(b.compression(), CompressionType::Gzip);
        assert_eq!(b.0 & 0x07, 1);
    }
}
```

- [ ] **Step 2: Hook the module up + re-export the public bits**

`crates/protocol/src/records/mod.rs`:

```rust
mod crc;
mod error;
pub mod header;

pub use error::RecordsError;
pub use header::{Attributes, TimestampType};
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol records::header
```

Expected: 11 tests pass (9 `attr_case!` invocations + `builder_round_trip` + `replacing_compression_clears_old_bits`).

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): Attributes + TimestampType with macro_rules table tests"
```

---

### Task 6: `RecordBatchHeader` zerocopy struct

The fixed 61-byte v2 header. Reinterpret via `zerocopy::FromBytes::ref_from_bytes`.

**Files:**
- Modify: `crates/protocol/src/records/header.rs`

- [ ] **Step 1: Add the zerocopy header struct**

Append to `crates/protocol/src/records/header.rs`:

```rust
use zerocopy::byteorder::{I16, I32, I64, U32};
use zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned};

/// The fixed 61-byte v2 record-batch header, reinterpreted in place from
/// the wire bytes via `zerocopy`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct RecordBatchHeader {
    pub base_offset:            I64<BigEndian>,
    pub batch_length:           I32<BigEndian>,
    pub partition_leader_epoch: I32<BigEndian>,
    pub magic:                  i8,
    pub crc:                    U32<BigEndian>,
    pub attributes:             I16<BigEndian>,
    pub last_offset_delta:      I32<BigEndian>,
    pub base_timestamp:         I64<BigEndian>,
    pub max_timestamp:          I64<BigEndian>,
    pub producer_id:            I64<BigEndian>,
    pub producer_epoch:         I16<BigEndian>,
    pub base_sequence:          I32<BigEndian>,
    pub records_count:          I32<BigEndian>,
}

/// Size of the v2 record-batch header in bytes.
pub const HEADER_LEN: usize = 61;

// Compile-time assertion that the layout is exactly 61 bytes.
const _: () = assert!(std::mem::size_of::<RecordBatchHeader>() == HEADER_LEN);
```

Append tests in the same file's `mod tests`:

```rust
    use zerocopy::FromBytes as _;

    /// Build a sample 61-byte header with known values. Reused across the
    /// header table tests below.
    fn sample_header_bytes() -> [u8; HEADER_LEN] {
        let mut buf = [0u8; HEADER_LEN];
        buf[0..8].copy_from_slice(&100i64.to_be_bytes());            // base_offset
        buf[8..12].copy_from_slice(&77i32.to_be_bytes());            // batch_length
        buf[12..16].copy_from_slice(&1i32.to_be_bytes());            // partition_leader_epoch
        buf[16] = 2;                                                  // magic
        buf[17..21].copy_from_slice(&0x1234_5678u32.to_be_bytes());  // crc
        buf[21..23].copy_from_slice(&0i16.to_be_bytes());            // attributes
        buf[23..27].copy_from_slice(&3i32.to_be_bytes());            // last_offset_delta
        buf[27..35].copy_from_slice(&111i64.to_be_bytes());          // base_timestamp
        buf[35..43].copy_from_slice(&222i64.to_be_bytes());          // max_timestamp
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());         // producer_id
        buf[51..53].copy_from_slice(&7i16.to_be_bytes());            // producer_epoch
        buf[53..57].copy_from_slice(&(-1i32).to_be_bytes());         // base_sequence
        buf[57..61].copy_from_slice(&4i32.to_be_bytes());            // records_count
        buf
    }

    macro_rules! header_field {
        ($name:ident, $field:ident, $expected:expr) => {
            #[test]
            fn $name() {
                let buf = sample_header_bytes();
                let h = RecordBatchHeader::ref_from_bytes(&buf[..])
                    .expect("header reinterpret");
                assert_eq!(h.$field.get(), $expected);
            }
        };
    }

    header_field!(reads_base_offset,             base_offset, 100i64);
    header_field!(reads_batch_length,            batch_length, 77i32);
    header_field!(reads_partition_leader_epoch,  partition_leader_epoch, 1i32);
    header_field!(reads_crc,                     crc, 0x1234_5678u32);
    header_field!(reads_last_offset_delta,       last_offset_delta, 3i32);
    header_field!(reads_base_timestamp,          base_timestamp, 111i64);
    header_field!(reads_max_timestamp,           max_timestamp, 222i64);
    header_field!(reads_producer_id,             producer_id, -1i64);
    header_field!(reads_producer_epoch,          producer_epoch, 7i16);
    header_field!(reads_base_sequence,           base_sequence, -1i32);
    header_field!(reads_records_count,           records_count, 4i32);

    #[test]
    fn reads_magic_directly() {
        let buf = sample_header_bytes();
        let h = RecordBatchHeader::ref_from_bytes(&buf[..]).unwrap();
        assert_eq!(h.magic, 2);
    }

    #[test]
    fn header_is_exactly_61_bytes() {
        assert_eq!(std::mem::size_of::<RecordBatchHeader>(), HEADER_LEN);
    }

    #[test]
    fn too_short_buffer_errors() {
        let buf = [0u8; HEADER_LEN - 1];
        assert!(RecordBatchHeader::ref_from_bytes(&buf[..]).is_err());
    }
```

- [ ] **Step 2: Re-export from the records module**

`crates/protocol/src/records/mod.rs`:

```rust
pub use header::{Attributes, RecordBatchHeader, TimestampType};
pub use header::HEADER_LEN;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol records::header
```

Expected: 11 (from Task 5) + 14 (here) = 25 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): zerocopy RecordBatchHeader (61-byte fixed v2 layout)"
```

---

## Phase B — Owned `RecordBatch`

### Task 7: Owned `Record` and `RecordHeader` types

Just the data shapes; encode/decode comes after the batch wrapper.

**Files:**
- Create: `crates/protocol/src/records/owned.rs`
- Modify: `crates/protocol/src/records/mod.rs`

- [ ] **Step 1: Write the data types**

`crates/protocol/src/records/owned.rs`:

```rust
//! Owned `RecordBatch`, `Record`, and `RecordHeader` types.

use bytes::Bytes;

use crate::records::header::Attributes;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: Attributes,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,        // sentinel: non-idempotent
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}
```

- [ ] **Step 2: Re-export**

`crates/protocol/src/records/mod.rs`:

```rust
mod crc;
mod error;
pub mod header;
mod owned;

pub use error::RecordsError;
pub use header::{Attributes, RecordBatchHeader, TimestampType, HEADER_LEN};
pub use owned::{Record, RecordBatch, RecordHeader};
```

- [ ] **Step 3: Build check (no tests yet on these types)**

```bash
cargo build -p crabka-protocol
```

Expected: clean compile.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): owned Record/RecordHeader/RecordBatch types"
```

---

### Task 8: Inner-records encode/decode (varint format)

The records block (after decompression) is a sequence of varint-prefixed records. This task implements the per-record codec independent of the batch wrapper, so it can be unit-tested cleanly.

**Files:**
- Modify: `crates/protocol/src/records/owned.rs`

- [ ] **Step 1: Add per-record encode/decode helpers**

Append to `crates/protocol/src/records/owned.rs`:

```rust
use bytes::{Buf, BufMut, BytesMut};

use crate::primitives::varint::{
    get_uvarint, get_varint, get_varlong, put_uvarint, put_varint, put_varlong,
    uvarint_len, varint_len, varlong_len,
};
use crate::records::RecordsError;

impl Record {
    /// Encode a single record (varint length prefix + fields) into `buf`.
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        let body_len = self.body_len();
        put_varlong(buf, i64::try_from(body_len).map_err(|_| {
            RecordsError::RecordParse("record body length overflow".into())
        })?);
        self.encode_body(buf)
    }

    /// Predicted total length of this record on the wire (length-prefix + body).
    pub fn encoded_len(&self) -> usize {
        let body = self.body_len();
        varlong_len(body as i64) + body
    }

    fn body_len(&self) -> usize {
        let mut n = 1; // attributes
        n += varlong_len(self.timestamp_delta);
        n += varint_len(self.offset_delta);
        n += match &self.key {
            None => varint_len(-1),
            Some(k) => varint_len(i32::try_from(k.len()).unwrap_or(i32::MAX)) + k.len(),
        };
        n += match &self.value {
            None => varint_len(-1),
            Some(v) => varint_len(i32::try_from(v.len()).unwrap_or(i32::MAX)) + v.len(),
        };
        n += varint_len(i32::try_from(self.headers.len()).unwrap_or(i32::MAX));
        for h in &self.headers {
            let key_bytes = h.key.as_bytes();
            n += varint_len(i32::try_from(key_bytes.len()).unwrap_or(i32::MAX)) + key_bytes.len();
            n += match &h.value {
                None => varint_len(-1),
                Some(v) => varint_len(i32::try_from(v.len()).unwrap_or(i32::MAX)) + v.len(),
            };
        }
        n
    }

    fn encode_body<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        buf.put_i8(self.attributes);
        put_varlong(buf, self.timestamp_delta);
        put_varint(buf, self.offset_delta);
        match &self.key {
            None => put_varint(buf, -1),
            Some(k) => {
                put_varint(buf, i32::try_from(k.len()).map_err(|_| {
                    RecordsError::RecordParse("record key length overflow".into())
                })?);
                buf.put_slice(k);
            }
        }
        match &self.value {
            None => put_varint(buf, -1),
            Some(v) => {
                put_varint(buf, i32::try_from(v.len()).map_err(|_| {
                    RecordsError::RecordParse("record value length overflow".into())
                })?);
                buf.put_slice(v);
            }
        }
        put_varint(buf, i32::try_from(self.headers.len()).map_err(|_| {
            RecordsError::RecordParse("record header count overflow".into())
        })?);
        for h in &self.headers {
            let key_bytes = h.key.as_bytes();
            put_varint(buf, i32::try_from(key_bytes.len()).map_err(|_| {
                RecordsError::RecordParse("header key length overflow".into())
            })?);
            buf.put_slice(key_bytes);
            match &h.value {
                None => put_varint(buf, -1),
                Some(v) => {
                    put_varint(buf, i32::try_from(v.len()).map_err(|_| {
                        RecordsError::RecordParse("header value length overflow".into())
                    })?);
                    buf.put_slice(v);
                }
            }
        }
        Ok(())
    }

    /// Decode a single record. `buf` must be positioned at the record's
    /// varlong length prefix.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, RecordsError> {
        let body_len = get_varlong(buf).map_err(|e| {
            RecordsError::RecordParse(format!("record length: {e}"))
        })?;
        let body_len = usize::try_from(body_len).map_err(|_| {
            RecordsError::RecordParse(format!("record length negative or too large: {body_len}"))
        })?;
        if buf.remaining() < body_len {
            return Err(RecordsError::BodyTooShort {
                needed: body_len - buf.remaining(),
            });
        }
        // Restrict to body_len bytes for parsing so a malformed inner field
        // doesn't run past the record boundary.
        let mut body = buf.take(body_len);
        let r = Self::decode_body(&mut body)?;
        // `body.has_remaining()` would mean trailing bytes inside the record's
        // claimed length — reject so we surface protocol corruption.
        if body.has_remaining() {
            return Err(RecordsError::RecordParse(format!(
                "trailing bytes inside record (left={})",
                body.remaining()
            )));
        }
        Ok(r)
    }

    fn decode_body<B: Buf>(buf: &mut B) -> Result<Self, RecordsError> {
        if buf.remaining() == 0 {
            return Err(RecordsError::RecordParse("record body empty".into()));
        }
        let attributes = buf.get_i8();
        let timestamp_delta = get_varlong(buf).map_err(|e| {
            RecordsError::RecordParse(format!("timestamp_delta: {e}"))
        })?;
        let offset_delta = get_varint(buf).map_err(|e| {
            RecordsError::RecordParse(format!("offset_delta: {e}"))
        })?;

        let key = decode_nullable_bytes(buf, "key")?;
        let value = decode_nullable_bytes(buf, "value")?;

        let header_count = get_varint(buf).map_err(|e| {
            RecordsError::RecordParse(format!("header_count: {e}"))
        })?;
        if header_count < 0 {
            return Err(RecordsError::RecordParse(format!(
                "negative header count {header_count}"
            )));
        }
        let mut headers = Vec::with_capacity(header_count as usize);
        for i in 0..header_count {
            headers.push(decode_record_header(buf).map_err(|e| {
                RecordsError::RecordParse(format!("header[{i}]: {e}"))
            })?);
        }

        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}

fn decode_nullable_bytes<B: Buf>(buf: &mut B, label: &str) -> Result<Option<Bytes>, RecordsError> {
    let len = get_varint(buf).map_err(|e| {
        RecordsError::RecordParse(format!("{label} length: {e}"))
    })?;
    if len < 0 {
        Ok(None)
    } else {
        let n = len as usize;
        if buf.remaining() < n {
            return Err(RecordsError::BodyTooShort {
                needed: n - buf.remaining(),
            });
        }
        let mut v = vec![0u8; n];
        buf.copy_to_slice(&mut v);
        Ok(Some(Bytes::from(v)))
    }
}

fn decode_record_header<B: Buf>(buf: &mut B) -> Result<RecordHeader, String> {
    let key_len = get_varint(buf).map_err(|e| format!("key length: {e}"))?;
    if key_len < 0 {
        return Err(format!("non-nullable key has negative length {key_len}"));
    }
    let n = key_len as usize;
    if buf.remaining() < n {
        return Err(format!("key truncated (need {} more)", n - buf.remaining()));
    }
    let mut kv = vec![0u8; n];
    buf.copy_to_slice(&mut kv);
    let key = String::from_utf8(kv).map_err(|e| format!("key utf-8: {e}"))?;

    let value_len = get_varint(buf).map_err(|e| format!("value length: {e}"))?;
    let value = if value_len < 0 {
        None
    } else {
        let n = value_len as usize;
        if buf.remaining() < n {
            return Err(format!("value truncated (need {} more)", n - buf.remaining()));
        }
        let mut vv = vec![0u8; n];
        buf.copy_to_slice(&mut vv);
        Some(Bytes::from(vv))
    };

    Ok(RecordHeader { key, value })
}

#[cfg(test)]
mod record_tests {
    use super::*;
    use bytes::BytesMut;

    fn fixture_minimal_record() -> Record {
        Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: None,
            value: None,
            headers: vec![],
        }
    }

    fn fixture_keyed_record() -> Record {
        Record {
            attributes: 0,
            timestamp_delta: 17,
            offset_delta: 2,
            key: Some(Bytes::from_static(b"the-key")),
            value: Some(Bytes::from_static(b"hello kafka")),
            headers: vec![
                RecordHeader { key: "trace-id".to_string(), value: Some(Bytes::from_static(b"abc")) },
                RecordHeader { key: "null-val".to_string(), value: None },
            ],
        }
    }

    fn fixture_large_payload_record() -> Record {
        Record {
            attributes: 0,
            timestamp_delta: 1_000_000,
            offset_delta: 999,
            key: Some(Bytes::from(vec![b'k'; 128])),
            value: Some(Bytes::from(vec![b'v'; 4096])),
            headers: vec![],
        }
    }

    macro_rules! roundtrip {
        ($name:ident, $fixture:ident) => {
            #[test]
            fn $name() {
                let r = $fixture();
                let mut buf = BytesMut::new();
                r.encode(&mut buf).unwrap();
                assert_eq!(buf.len(), r.encoded_len(), "predicted len mismatch");

                let mut cur: &[u8] = &buf[..];
                let decoded = Record::decode(&mut cur).unwrap();
                assert_eq!(decoded, r);
                assert!(cur.is_empty(), "trailing bytes after decode");
            }
        };
    }

    roundtrip!(minimal, fixture_minimal_record);
    roundtrip!(keyed_with_headers, fixture_keyed_record);
    roundtrip!(large_payload, fixture_large_payload_record);

    #[test]
    fn decode_rejects_negative_header_count() {
        let mut buf = BytesMut::new();
        // body: attributes + timestamp_delta + offset_delta + key=-1 + value=-1 + headers=-1
        crate::primitives::varint::put_varlong(&mut buf, 7); // body length 7 bytes
        buf.put_i8(0);                                       // attributes
        crate::primitives::varint::put_varlong(&mut buf, 0); // timestamp_delta = 0  (1 byte)
        crate::primitives::varint::put_varint(&mut buf, 0);  // offset_delta = 0     (1 byte)
        crate::primitives::varint::put_varint(&mut buf, -1); // key len               (1 byte)
        crate::primitives::varint::put_varint(&mut buf, -1); // value len             (1 byte)
        crate::primitives::varint::put_varint(&mut buf, -1); // negative header count (1 byte)

        let mut cur: &[u8] = &buf[..];
        match Record::decode(&mut cur) {
            Err(RecordsError::RecordParse(msg)) => {
                assert!(msg.contains("negative header count"), "got: {msg}");
            }
            other => panic!("expected RecordParse, got {other:?}"),
        }
    }
}
```

> **Note:** the plan assumes `crate::primitives::varint` exposes `varlong_len(i64) -> usize`. If that function does not yet exist (the foundation added `varint_len(i32)` and `uvarint_len(u32)` but possibly not the long form), add it as a tiny sibling using the same zigzag rule. Verify before relying on it; if missing, extend `crates/protocol/src/primitives/varint.rs` with:
>
> ```rust
> #[must_use]
> pub fn varlong_len(v: i64) -> usize {
>     let zz = ((v << 1) ^ (v >> 63)) as u64;
>     uvarlong_len(zz)
> }
> #[must_use]
> pub fn uvarlong_len(v: u64) -> usize {
>     if v == 0 { return 1; }
>     let bits = 64 - v.leading_zeros() as usize;
>     (bits + 6) / 7
> }
> ```
>
> and a trivial unit test for each.

- [ ] **Step 2: Run tests**

```bash
cargo test -p crabka-protocol records::owned::record_tests
```

Expected: 4 tests pass (`minimal`, `keyed_with_headers`, `large_payload`, `decode_rejects_negative_header_count`).

- [ ] **Step 3: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): per-record varint encode/decode with macro_rules tests"
```

---

### Task 9: Owned `RecordBatch::{encode, decode, encoded_len}` (uncompressed first)

This task wires up the batch wrapper for the uncompressed case. Task 10 adds compression.

**Files:**
- Modify: `crates/protocol/src/records/owned.rs`

- [ ] **Step 1: Add the batch encode/decode**

Append to `crates/protocol/src/records/owned.rs`:

```rust
use crate::records::crc::crc32c;
use crate::records::header::HEADER_LEN;
use crate::records::RecordsError;

impl RecordBatch {
    /// Decode a complete v2 record batch from `buf` (uncompressed only;
    /// Task 10 adds compression). Reads from the start of the header.
    pub fn decode<B: Buf>(buf: &mut B) -> Result<Self, RecordsError> {
        // We need the full header before doing anything.
        if buf.remaining() < HEADER_LEN {
            return Err(RecordsError::HeaderTooShort {
                needed: HEADER_LEN - buf.remaining(),
            });
        }
        // Copy out the header to a stack buffer so we can use zerocopy.
        let mut hdr_bytes = [0u8; HEADER_LEN];
        buf.copy_to_slice(&mut hdr_bytes);

        let hdr = crate::records::header::RecordBatchHeader::ref_from_bytes(&hdr_bytes[..])
            .map_err(|_| RecordsError::ZerocopyFailure)?;

        if hdr.magic != 2 {
            return Err(RecordsError::UnsupportedMagic { found: hdr.magic });
        }

        // Records-body length: batch_length minus the 9 bytes that follow
        // `batch_length` *before* the CRC's coverage starts (the bytes from
        // `partition_leader_epoch` up to and including `crc` itself are
        // `9 + 4 = 13` bytes wait no — re-derive:
        //   The wire layout after `base_offset(8) + batch_length(4)` is
        //   partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2) +
        //   last_offset_delta(4) + base_timestamp(8) + max_timestamp(8) +
        //   producer_id(8) + producer_epoch(2) + base_sequence(4) +
        //   records_count(4) = 49 bytes of header tail.
        //   `batch_length` is defined as the number of bytes after itself,
        //   so body length = batch_length - 49.
        const HEADER_TAIL_LEN: i32 = 49;
        let body_len = i32::checked_sub(hdr.batch_length.get(), HEADER_TAIL_LEN)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| RecordsError::RecordParse("negative or oversized batch_length".into()))?;

        if buf.remaining() < body_len {
            return Err(RecordsError::BodyTooShort {
                needed: body_len - buf.remaining(),
            });
        }

        // Read the body into a contiguous Vec so we can both CRC it and
        // parse records out of it.
        let mut body = vec![0u8; body_len];
        buf.copy_to_slice(&mut body);

        // CRC is computed over: attributes...records (the part of the header
        // *after* `crc` field, plus the records body).
        let expected_crc = hdr.crc.get();
        let crc_payload = &hdr_bytes[21..HEADER_LEN]; // attributes onwards
        let computed = {
            let mut s = crc32c(crc_payload);
            s = crc32c_combine(s, crc32c(&body), body.len() as u64);
            s
        };
        if computed != expected_crc {
            return Err(RecordsError::CrcMismatch {
                expected: expected_crc,
                computed,
            });
        }

        let attributes = Attributes(hdr.attributes.get());
        let codec = attributes.compression();
        if codec != crabka_compression::CompressionType::None {
            return Err(RecordsError::RecordParse(format!(
                "compression {codec:?} requires Task 10 implementation"
            )));
        }

        // Parse records out of the body.
        let mut body_cur: &[u8] = &body[..];
        let count = hdr.records_count.get();
        if count < 0 {
            return Err(RecordsError::RecordParse(format!(
                "negative records_count {count}"
            )));
        }
        let mut records = Vec::with_capacity(count as usize);
        for i in 0..count {
            records.push(Record::decode(&mut body_cur).map_err(|e| {
                RecordsError::RecordParse(format!("record[{i}]: {e}"))
            })?);
        }
        if !body_cur.is_empty() {
            return Err(RecordsError::RecordParse(format!(
                "trailing bytes after records (left={})",
                body_cur.len()
            )));
        }

        Ok(Self {
            base_offset: hdr.base_offset.get(),
            partition_leader_epoch: hdr.partition_leader_epoch.get(),
            attributes,
            last_offset_delta: hdr.last_offset_delta.get(),
            base_timestamp: hdr.base_timestamp.get(),
            max_timestamp: hdr.max_timestamp.get(),
            producer_id: hdr.producer_id.get(),
            producer_epoch: hdr.producer_epoch.get(),
            base_sequence: hdr.base_sequence.get(),
            records,
        })
    }

    /// Encode this batch into `buf` (uncompressed only in this task).
    pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError> {
        if self.attributes.compression() != crabka_compression::CompressionType::None {
            return Err(RecordsError::RecordParse(
                "compression encoding requires Task 10 implementation".into(),
            ));
        }

        // 1. Encode records into a temporary buffer so we know the body
        //    length up front (needed for `batch_length` and CRC).
        let mut body = BytesMut::with_capacity(
            self.records.iter().map(Record::encoded_len).sum(),
        );
        for r in &self.records {
            r.encode(&mut body)?;
        }
        let body = body.freeze();

        // 2. batch_length = (header_tail bytes) + body_len
        const HEADER_TAIL_LEN: i32 = 49;
        let batch_length = HEADER_TAIL_LEN
            + i32::try_from(body.len()).map_err(|_| {
                RecordsError::RecordParse("body length exceeds i32".into())
            })?;

        // 3. Materialise the part of the header that the CRC covers
        //    (attributes through records_count) so we can compute CRC.
        let mut covered = BytesMut::with_capacity(40 + body.len());
        covered.put_i16(self.attributes.0);
        covered.put_i32(self.last_offset_delta);
        covered.put_i64(self.base_timestamp);
        covered.put_i64(self.max_timestamp);
        covered.put_i64(self.producer_id);
        covered.put_i16(self.producer_epoch);
        covered.put_i32(self.base_sequence);
        covered.put_i32(i32::try_from(self.records.len()).map_err(|_| {
            RecordsError::RecordParse("records_count exceeds i32".into())
        })?);
        let covered_head = covered.split().freeze();
        let crc = {
            let s = crc32c(&covered_head);
            crc32c_combine(s, crc32c(&body), body.len() as u64)
        };

        // 4. Emit the full header.
        buf.put_i64(self.base_offset);
        buf.put_i32(batch_length);
        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(2); // magic
        buf.put_u32(crc);
        buf.put_slice(&covered_head);
        buf.put_slice(&body);
        Ok(())
    }

    /// Predicted total bytes that `encode` will write.
    pub fn encoded_len(&self) -> usize {
        let body: usize = self.records.iter().map(Record::encoded_len).sum();
        HEADER_LEN + body
    }
}

/// Combine two CRC-32C values where the second was computed over `len2` bytes.
/// `crc32c` 0.6 doesn't expose a public `combine` function, so we incrementally
/// hash by feeding bytes; in practice the calling code computes the CRC as a
/// single stream so this helper is unused in the hot path. Kept for clarity.
#[allow(dead_code)]
fn crc32c_combine(_s1: u32, s2_full: u32, _len2: u64) -> u32 {
    // Simplification: the call sites above hash the covered region and body
    // as one logical stream by passing the concatenated bytes through
    // `crc32c::crc32c_append`. Since we materialise both pieces before calling,
    // refactor to a single call:
    s2_full
}
```

> **The `crc32c_combine` shape above is a placeholder pattern.** In practice rewrite the CRC computation to use `crc32c::crc32c_append` (which takes a running state) so we don't need a combine function at all:
>
> ```rust
> // Compute CRC over covered_head, then continue into body.
> let mut crc = crc32c::crc32c(&covered_head);
> crc = crc32c::crc32c_append(crc, &body);
> ```
>
> And mirror this in the decode path:
>
> ```rust
> let mut computed = crc32c::crc32c(&hdr_bytes[21..HEADER_LEN]);
> computed = crc32c::crc32c_append(computed, &body);
> ```
>
> Drop the `crc32c_combine` placeholder entirely. The plan's two snippets above should be rewritten before commit. The `crc.rs` module from Task 4 may want to expose a tiny `crc32c_append(seed: u32, data: &[u8]) -> u32` re-export for consistency.

- [ ] **Step 2: Refactor CRC computation per the note above**

Edit both `encode` and `decode` to use `crc32c::crc32c` for the first slice and `crc32c::crc32c_append` for the second. Delete `crc32c_combine`. Update `crates/protocol/src/records/crc.rs`:

```rust
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

#[must_use]
pub fn crc32c_append(seed: u32, data: &[u8]) -> u32 {
    crc32c::crc32c_append(seed, data)
}
```

(Both functions are pure pass-throughs. Defining them locally makes future swapping of the implementation trivial.)

- [ ] **Step 3: Add round-trip unit tests on uncompressed batches**

In `crates/protocol/src/records/owned.rs`, add a new `#[cfg(test)] mod batch_tests`:

```rust
#[cfg(test)]
mod batch_tests {
    use super::*;
    use crabka_compression::CompressionType;

    fn fixture_empty_batch() -> RecordBatch {
        RecordBatch::default()
    }

    fn fixture_single_record_batch() -> RecordBatch {
        RecordBatch {
            records: vec![Record {
                key: Some(Bytes::from_static(b"k1")),
                value: Some(Bytes::from_static(b"v1")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    fn fixture_multi_record_batch() -> RecordBatch {
        RecordBatch {
            base_offset: 42,
            partition_leader_epoch: 5,
            last_offset_delta: 2,
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_500,
            producer_id: 100,
            producer_epoch: 3,
            base_sequence: 7,
            records: vec![
                Record { offset_delta: 0, timestamp_delta: 0,   key: Some(Bytes::from_static(b"a")), value: Some(Bytes::from_static(b"1")), ..Default::default() },
                Record { offset_delta: 1, timestamp_delta: 100, key: Some(Bytes::from_static(b"b")), value: Some(Bytes::from_static(b"2")), ..Default::default() },
                Record { offset_delta: 2, timestamp_delta: 500, key: None, value: Some(Bytes::from_static(b"3")), headers: vec![
                    RecordHeader { key: "h".to_string(), value: Some(Bytes::from_static(b"hv")) },
                ], ..Default::default() },
            ],
            ..RecordBatch::default()
        }
    }

    macro_rules! roundtrip_uncompressed {
        ($name:ident, $fixture:ident) => {
            #[test]
            fn $name() {
                let b = $fixture();
                // Force uncompressed.
                let mut b = b;
                b.attributes = b.attributes.with_compression(CompressionType::None);

                let mut buf = bytes::BytesMut::new();
                b.encode(&mut buf).unwrap();
                assert_eq!(buf.len(), b.encoded_len());

                let mut cur: &[u8] = &buf[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                assert_eq!(decoded, b);
                assert!(cur.is_empty());
            }
        };
    }

    roundtrip_uncompressed!(uncompressed_empty,         fixture_empty_batch);
    roundtrip_uncompressed!(uncompressed_single,        fixture_single_record_batch);
    roundtrip_uncompressed!(uncompressed_multi,         fixture_multi_record_batch);

    #[test]
    fn rejects_pre_v2_magic() {
        let mut buf = bytes::BytesMut::new();
        buf.put_i64(0);            // base_offset
        buf.put_i32(49);           // batch_length
        buf.put_i32(0);            // partition_leader_epoch
        buf.put_i8(1);             // magic = 1 (v1, deprecated)
        buf.put_u32(0);            // crc (irrelevant; we reject on magic first)
        for _ in 21..HEADER_LEN { buf.put_u8(0); }
        let mut cur: &[u8] = &buf[..];
        assert!(matches!(
            RecordBatch::decode(&mut cur),
            Err(RecordsError::UnsupportedMagic { found: 1 })
        ));
    }

    #[test]
    fn rejects_bad_crc() {
        let b = fixture_single_record_batch();
        let mut buf = bytes::BytesMut::new();
        b.encode(&mut buf).unwrap();
        // Corrupt the CRC bytes (offsets 17..21).
        buf[17] ^= 0xFF;
        let mut cur: &[u8] = &buf[..];
        assert!(matches!(
            RecordBatch::decode(&mut cur),
            Err(RecordsError::CrcMismatch { .. })
        ));
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol records::owned
```

Expected: 4 record tests (Task 8) + 5 batch tests (here) = 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): owned RecordBatch encode/decode (uncompressed)"
```

---

### Task 10: Compression support in owned `RecordBatch`

Wire `crabka-compression::{compress, decompress}` into the encode/decode paths so non-None codecs work.

**Files:**
- Modify: `crates/protocol/src/records/owned.rs`

- [ ] **Step 1: Add the compression hooks**

In `RecordBatch::encode`, replace the early-exit check for non-None compression with the real path: encode records into `body` first (same as before), then `body = crabka_compression::compress(codec, &body)?` if `codec != None`. The rest of the function (batch_length, CRC, header emission) uses the (compressed) `body`.

In `RecordBatch::decode`, replace the early-exit check with: after CRC validation, if the codec is non-None, replace `body` with `crabka_compression::decompress(codec, &body)?.to_vec()` (or keep as `Bytes` and adapt the parsing — `Bytes` already implements `Buf`, so use `Bytes::from(body)` and parse from that). Then continue parsing records from the now-decompressed body.

Concretely, after CRC validation in decode:

```rust
let codec = attributes.compression();
let body_for_records: Bytes = if codec == crabka_compression::CompressionType::None {
    Bytes::from(body)
} else {
    crabka_compression::decompress(codec, &body)?
};
let mut body_cur: &[u8] = &body_for_records[..];
// ... rest of records parsing unchanged
```

And in encode, after building `body`:

```rust
let body = if self.attributes.compression() == crabka_compression::CompressionType::None {
    body
} else {
    crabka_compression::compress(self.attributes.compression(), &body)?
};
```

(Where `body` is the `Bytes` from `freeze()`.)

The `body` byte length used for `batch_length` and CRC is the COMPRESSED length when applicable.

- [ ] **Step 2: Add a per-codec round-trip macro test**

Append to `mod batch_tests` in `owned.rs`:

```rust
    macro_rules! roundtrip_compressed {
        ($name:ident, $codec:expr) => {
            #[test]
            fn $name() {
                let mut b = fixture_multi_record_batch();
                b.attributes = b.attributes.with_compression($codec);

                let mut buf = bytes::BytesMut::new();
                b.encode(&mut buf).unwrap();
                let mut cur: &[u8] = &buf[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                assert_eq!(decoded, b);
                assert!(cur.is_empty());
            }
        };
    }

    roundtrip_compressed!(compressed_gzip,   CompressionType::Gzip);
    roundtrip_compressed!(compressed_snappy, CompressionType::Snappy);
    roundtrip_compressed!(compressed_lz4,    CompressionType::Lz4);
    roundtrip_compressed!(compressed_zstd,   CompressionType::Zstd);
```

- [ ] **Step 3: Run**

```bash
cargo test -p crabka-protocol records::owned::batch_tests
```

Expected: 5 (uncompressed + magic + crc) + 4 (compressed) = 9 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): compression support in owned RecordBatch"
```

---

### Task 11: Implement `Encode` and `Decode<'de>` traits on owned `RecordBatch`

So codegen can call `.encode(buf, version)` / `<Type>::decode(buf, version)` uniformly with other struct-typed fields.

**Files:**
- Modify: `crates/protocol/src/records/owned.rs`

- [ ] **Step 1: Add the trait impls**

Append:

```rust
impl crate::Encode for RecordBatch {
    fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), crate::ProtocolError> {
        self.encode(buf).map_err(Into::into)
    }
    fn encoded_len(&self, _version: i16) -> usize {
        self.encoded_len()
    }
}

impl<'de> crate::Decode<'de> for RecordBatch {
    fn decode<B: Buf>(buf: &mut B, _version: i16) -> Result<Self, crate::ProtocolError> {
        Self::decode(buf).map_err(Into::into)
    }
}
```

Note: the trait methods shadow the inherent methods only via the `Encode`/`Decode` trait scope. To avoid ambiguity, the trait impls call through to the inherent `Self::encode(buf)` and `Self::decode(buf)`. Make sure inherent methods stay named `encode`/`decode` (not renamed).

- [ ] **Step 2: Run all owned tests**

```bash
cargo test -p crabka-protocol records::owned
```

Expected: all existing tests still pass; the new trait impls compile but aren't directly tested here (proptest covers them).

- [ ] **Step 3: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): Encode/Decode trait impls for owned RecordBatch"
```

---

## Phase C — Borrowed `RecordBatch`

### Task 12: Borrowed `RecordBatch<'a>` skeleton + types

**Files:**
- Create: `crates/protocol/src/records/borrowed.rs`
- Modify: `crates/protocol/src/records/mod.rs`

- [ ] **Step 1: Write the data shapes**

`crates/protocol/src/records/borrowed.rs`:

```rust
//! Borrowed `RecordBatch<'a>`, `Record<'a>`, and `RecordHeader<'a>`.

use bytes::Bytes;

use crate::records::header::{Attributes, RecordBatchHeader};

pub struct RecordBatch<'a> {
    pub(crate) header: &'a RecordBatchHeader,
    pub(crate) body: RecordBody<'a>,
}

pub(crate) enum RecordBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record<'a> {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Vec<RecordHeader<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader<'a> {
    pub key: &'a str,
    pub value: Option<&'a [u8]>,
}

impl<'a> RecordBatch<'a> {
    #[must_use]
    pub fn header(&self) -> &RecordBatchHeader {
        self.header
    }

    #[must_use]
    pub fn attributes(&self) -> Attributes {
        Attributes(self.header.attributes.get())
    }
}
```

- [ ] **Step 2: Re-export**

`crates/protocol/src/records/mod.rs`:

```rust
mod borrowed;
mod crc;
mod error;
pub mod header;
mod owned;

pub use error::RecordsError;
pub use header::{Attributes, RecordBatchHeader, TimestampType, HEADER_LEN};
pub use owned::{Record, RecordBatch, RecordHeader};
pub use borrowed::{
    Record as RecordBorrowed,
    RecordBatch as RecordBatchBorrowed,
    RecordHeader as RecordHeaderBorrowed,
};
```

- [ ] **Step 3: Build check**

```bash
cargo build -p crabka-protocol
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): borrowed RecordBatch<'a> skeleton + types"
```

---

### Task 13: Borrowed `DecodeBorrow` + lazy iterator

**Files:**
- Modify: `crates/protocol/src/records/borrowed.rs`

- [ ] **Step 1: Implement decode and iteration**

Append to `crates/protocol/src/records/borrowed.rs`:

```rust
use crate::primitives::varint::{get_varint, get_varlong};
use crate::records::crc::{crc32c, crc32c_append};
use crate::records::header::HEADER_LEN;
use crate::records::RecordsError;

impl<'de> crate::DecodeBorrow<'de> for RecordBatch<'de> {
    fn decode_borrow(buf: &mut &'de [u8], _version: i16) -> Result<Self, crate::ProtocolError> {
        decode_borrow_impl(buf).map_err(Into::into)
    }
}

fn decode_borrow_impl<'de>(buf: &mut &'de [u8]) -> Result<RecordBatch<'de>, RecordsError> {
    if buf.len() < HEADER_LEN {
        return Err(RecordsError::HeaderTooShort {
            needed: HEADER_LEN - buf.len(),
        });
    }
    let (hdr_slice, rest) = buf.split_at(HEADER_LEN);
    let hdr = RecordBatchHeader::ref_from_bytes(hdr_slice)
        .map_err(|_| RecordsError::ZerocopyFailure)?;
    if hdr.magic != 2 {
        return Err(RecordsError::UnsupportedMagic { found: hdr.magic });
    }

    const HEADER_TAIL_LEN: i32 = 49;
    let body_len = i32::checked_sub(hdr.batch_length.get(), HEADER_TAIL_LEN)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| RecordsError::RecordParse("negative or oversized batch_length".into()))?;

    if rest.len() < body_len {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - rest.len(),
        });
    }
    let (raw_body, after) = rest.split_at(body_len);
    *buf = after;

    // CRC: hash header[21..HEADER_LEN] (attributes through records_count)
    // and append the raw_body bytes.
    let expected = hdr.crc.get();
    let mut computed = crc32c(&hdr_slice[21..HEADER_LEN]);
    computed = crc32c_append(computed, raw_body);
    if computed != expected {
        return Err(RecordsError::CrcMismatch {
            expected,
            computed,
        });
    }

    let attributes = Attributes(hdr.attributes.get());
    let codec = attributes.compression();
    let body = if codec == crabka_compression::CompressionType::None {
        RecordBody::Borrowed(raw_body)
    } else {
        let decompressed = crabka_compression::decompress(codec, raw_body)?;
        RecordBody::Owned(decompressed)
    };

    Ok(RecordBatch { header: hdr, body })
}

impl<'a> RecordBatch<'a> {
    /// Iterate over records, parsing each lazily. The iterator returns
    /// `Record<'a>` for borrowed batches (uncompressed); for compressed
    /// batches, records borrow from the `RecordBatch`'s decompressed
    /// buffer, so the returned `Record` lifetime is tied to `&self`,
    /// not `'a`. We expose two iterators: `iter()` (records bound to
    /// the batch) and `into_borrowed_iter()` (only valid for
    /// uncompressed batches; records bound to `'a`).
    pub fn iter<'b>(&'b self) -> RecordIter<'b> {
        let body: &'b [u8] = match &self.body {
            RecordBody::Borrowed(s) => s,
            RecordBody::Owned(b) => b.as_ref(),
        };
        RecordIter {
            remaining: body,
            count: self.header.records_count.get().max(0) as usize,
            index: 0,
        }
    }
}

pub struct RecordIter<'a> {
    remaining: &'a [u8],
    count: usize,
    index: usize,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<Record<'a>, RecordsError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.count {
            return None;
        }
        self.index += 1;
        Some(parse_one_record(&mut self.remaining))
    }
}

fn parse_one_record<'a>(buf: &mut &'a [u8]) -> Result<Record<'a>, RecordsError> {
    let body_len = get_varlong(buf).map_err(|e| {
        RecordsError::RecordParse(format!("record length: {e}"))
    })?;
    let body_len = usize::try_from(body_len).map_err(|_| {
        RecordsError::RecordParse(format!("record length negative or too large: {body_len}"))
    })?;
    if buf.len() < body_len {
        return Err(RecordsError::BodyTooShort {
            needed: body_len - buf.len(),
        });
    }
    let (body, rest) = buf.split_at(body_len);
    *buf = rest;
    let mut body_cur = body;
    let r = parse_body(&mut body_cur)?;
    if !body_cur.is_empty() {
        return Err(RecordsError::RecordParse(format!(
            "trailing bytes inside record (left={})",
            body_cur.len()
        )));
    }
    Ok(r)
}

fn parse_body<'a>(buf: &mut &'a [u8]) -> Result<Record<'a>, RecordsError> {
    if buf.is_empty() {
        return Err(RecordsError::RecordParse("record body empty".into()));
    }
    let attributes = buf[0] as i8;
    *buf = &buf[1..];
    let timestamp_delta = get_varlong(buf).map_err(|e| {
        RecordsError::RecordParse(format!("timestamp_delta: {e}"))
    })?;
    let offset_delta = get_varint(buf).map_err(|e| {
        RecordsError::RecordParse(format!("offset_delta: {e}"))
    })?;

    let key = read_nullable_slice(buf, "key")?;
    let value = read_nullable_slice(buf, "value")?;

    let header_count = get_varint(buf).map_err(|e| {
        RecordsError::RecordParse(format!("header_count: {e}"))
    })?;
    if header_count < 0 {
        return Err(RecordsError::RecordParse(format!(
            "negative header count {header_count}"
        )));
    }
    let mut headers = Vec::with_capacity(header_count as usize);
    for i in 0..header_count {
        let key_len = get_varint(buf).map_err(|e| {
            RecordsError::RecordParse(format!("header[{i}] key length: {e}"))
        })?;
        if key_len < 0 {
            return Err(RecordsError::RecordParse(format!(
                "header[{i}] negative key length"
            )));
        }
        let n = key_len as usize;
        if buf.len() < n {
            return Err(RecordsError::BodyTooShort { needed: n - buf.len() });
        }
        let (key_bytes, rest) = buf.split_at(n);
        *buf = rest;
        let key_str = std::str::from_utf8(key_bytes).map_err(|e| {
            RecordsError::RecordParse(format!("header[{i}] key utf-8: {e}"))
        })?;

        let value = read_nullable_slice(buf, &format!("header[{i}] value"))?;
        headers.push(RecordHeader { key: key_str, value });
    }

    Ok(Record {
        attributes,
        timestamp_delta,
        offset_delta,
        key,
        value,
        headers,
    })
}

fn read_nullable_slice<'a>(buf: &mut &'a [u8], label: &str) -> Result<Option<&'a [u8]>, RecordsError> {
    let len = get_varint(buf).map_err(|e| {
        RecordsError::RecordParse(format!("{label} length: {e}"))
    })?;
    if len < 0 {
        Ok(None)
    } else {
        let n = len as usize;
        if buf.len() < n {
            return Err(RecordsError::BodyTooShort { needed: n - buf.len() });
        }
        let (head, rest) = buf.split_at(n);
        *buf = rest;
        Ok(Some(head))
    }
}
```

- [ ] **Step 2: Add `to_owned` bridge**

Append:

```rust
impl<'a> RecordBatch<'a> {
    /// Materialise an owned `RecordBatch` by copying every byte slice into
    /// `Bytes` / `String`.
    pub fn to_owned(&self) -> Result<super::owned::RecordBatch, RecordsError> {
        let mut records = Vec::new();
        for r in self.iter() {
            let r = r?;
            records.push(super::owned::Record {
                attributes: r.attributes,
                timestamp_delta: r.timestamp_delta,
                offset_delta: r.offset_delta,
                key: r.key.map(Bytes::copy_from_slice),
                value: r.value.map(Bytes::copy_from_slice),
                headers: r.headers.into_iter().map(|h| super::owned::RecordHeader {
                    key: h.key.to_string(),
                    value: h.value.map(Bytes::copy_from_slice),
                }).collect(),
            });
        }
        Ok(super::owned::RecordBatch {
            base_offset: self.header.base_offset.get(),
            partition_leader_epoch: self.header.partition_leader_epoch.get(),
            attributes: self.attributes(),
            last_offset_delta: self.header.last_offset_delta.get(),
            base_timestamp: self.header.base_timestamp.get(),
            max_timestamp: self.header.max_timestamp.get(),
            producer_id: self.header.producer_id.get(),
            producer_epoch: self.header.producer_epoch.get(),
            base_sequence: self.header.base_sequence.get(),
            records,
        })
    }
}
```

- [ ] **Step 3: Add tests**

Append:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::DecodeBorrow;
    use bytes::BytesMut;
    use crabka_compression::CompressionType;

    fn encode_owned_then_borrow(b: &super::super::owned::RecordBatch) -> Vec<u8> {
        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();
        buf.to_vec()
    }

    macro_rules! borrowed_roundtrip {
        ($name:ident, $codec:expr) => {
            #[test]
            fn $name() {
                let mut owned = super::super::owned::RecordBatch::default();
                owned.attributes = owned.attributes.with_compression($codec);
                owned.records.push(super::super::owned::Record {
                    key: Some(Bytes::from_static(b"key")),
                    value: Some(Bytes::from_static(b"value")),
                    ..Default::default()
                });

                let encoded = encode_owned_then_borrow(&owned);
                let mut cur: &[u8] = &encoded[..];
                let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
                assert!(cur.is_empty());
                assert_eq!(borrowed.attributes(), owned.attributes);

                let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].key, Some(b"key".as_slice()));
                assert_eq!(records[0].value, Some(b"value".as_slice()));

                let back_owned = borrowed.to_owned().unwrap();
                assert_eq!(back_owned, owned);
            }
        };
    }

    borrowed_roundtrip!(roundtrip_none,   CompressionType::None);
    borrowed_roundtrip!(roundtrip_gzip,   CompressionType::Gzip);
    borrowed_roundtrip!(roundtrip_snappy, CompressionType::Snappy);
    borrowed_roundtrip!(roundtrip_lz4,    CompressionType::Lz4);
    borrowed_roundtrip!(roundtrip_zstd,   CompressionType::Zstd);

    #[test]
    fn zero_copy_for_uncompressed() {
        // Pointer-identity: record key/value slices must point into the
        // input buffer for uncompressed batches.
        let mut owned = super::super::owned::RecordBatch::default();
        owned.records.push(super::super::owned::Record {
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        });
        let encoded = encode_owned_then_borrow(&owned);
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();

        let mut cur: &[u8] = &encoded[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();
        let records: Vec<_> = borrowed.iter().collect::<Result<_, _>>().unwrap();

        let v_ptr = records[0].value.unwrap().as_ptr() as usize;
        assert!(
            v_ptr >= encoded_start && v_ptr < encoded_end,
            "value slice does not point into the input buffer: \
             input range [{encoded_start:#x}, {encoded_end:#x}), value ptr {v_ptr:#x}",
        );
    }
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol records::borrowed
```

Expected: 6 tests pass (5 codec round-trips + 1 zero-copy pointer-identity).

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): borrowed RecordBatch decode + iterator + to_owned"
```

---

### Task 14: Borrowed `Encode` trait impl

`RecordBatch<'a>` serialising back to bytes lets the codegen treat it uniformly. The simplest implementation routes through `to_owned()` and the owned encoder; that's a copy, but correct, and consumers who care about performance use the owned flavor.

**Files:**
- Modify: `crates/protocol/src/records/borrowed.rs`

- [ ] **Step 1: Implement**

Append:

```rust
impl<'a> crate::Encode for RecordBatch<'a> {
    fn encode<B: bytes::BufMut>(&self, buf: &mut B, version: i16) -> Result<(), crate::ProtocolError> {
        let owned = self.to_owned().map_err(crate::ProtocolError::from)?;
        crate::Encode::encode(&owned, buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        match self.to_owned() {
            Ok(o) => crate::Encode::encoded_len(&o, version),
            Err(_) => 0,
        }
    }
}
```

- [ ] **Step 2: Tests** — add a roundtrip that goes through `Encode` on the borrowed flavor

Append to `mod tests` in `borrowed.rs`:

```rust
    #[test]
    fn borrowed_encode_via_trait_roundtrips() {
        use crate::Encode as _;
        let owned_in = super::super::owned::RecordBatch {
            records: vec![super::super::owned::Record {
                key: Some(Bytes::from_static(b"x")),
                value: Some(Bytes::from_static(b"y")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let bytes_in = encode_owned_then_borrow(&owned_in);
        let mut cur: &[u8] = &bytes_in[..];
        let borrowed = RecordBatch::decode_borrow(&mut cur, 0).unwrap();

        let mut out = BytesMut::new();
        borrowed.encode(&mut out, 0).unwrap();
        assert_eq!(&out[..], &bytes_in[..]);
    }
```

- [ ] **Step 3: Run**

```bash
cargo test -p crabka-protocol records::borrowed
```

Expected: 7 tests pass (6 from Task 13 + 1 new).

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(records): Encode trait impl on borrowed RecordBatch"
```

---

## Phase D — Tests and benches

### Task 15: Proptest round-trips per codec

**Files:**
- Create: `crates/protocol/tests/proptest_records.rs`

- [ ] **Step 1: Write the proptest harness**

`crates/protocol/tests/proptest_records.rs`:

```rust
use bytes::{Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_protocol::records::{Record, RecordBatch, RecordHeader};
use proptest::prelude::*;

fn arb_bytes(max: usize) -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..=max).prop_map(Bytes::from)
}

fn arb_header() -> impl Strategy<Value = RecordHeader> {
    (
        "[a-z0-9_-]{1,32}",
        proptest::option::of(arb_bytes(256)),
    )
        .prop_map(|(key, value)| RecordHeader { key, value })
}

fn arb_record() -> impl Strategy<Value = Record> {
    (
        any::<i8>(),
        -100_000i64..100_000,
        0i32..100,
        proptest::option::of(arb_bytes(512)),
        proptest::option::of(arb_bytes(2048)),
        proptest::collection::vec(arb_header(), 0..=4),
    )
        .prop_map(|(attributes, ts, off, key, value, headers)| Record {
            attributes,
            timestamp_delta: ts,
            offset_delta: off,
            key,
            value,
            headers,
        })
}

fn arb_record_batch(codec: CompressionType) -> impl Strategy<Value = RecordBatch> {
    (
        proptest::collection::vec(arb_record(), 0..=8),
        any::<i64>(),
        any::<i32>(),
        any::<i64>(),
        any::<i64>(),
    )
        .prop_map(move |(records, base_offset, leader_epoch, ts0, ts1)| {
            let mut b = RecordBatch {
                base_offset,
                partition_leader_epoch: leader_epoch,
                base_timestamp: ts0,
                max_timestamp: ts1,
                records,
                ..RecordBatch::default()
            };
            b.attributes = b.attributes.with_compression(codec);
            b
        })
}

macro_rules! proptest_codec {
    ($name:ident, $codec:expr) => {
        proptest! {
            #[test]
            fn $name(b in arb_record_batch($codec)) {
                let mut buf = BytesMut::new();
                b.encode(&mut buf).unwrap();
                prop_assert_eq!(buf.len(), b.encoded_len());

                let mut cur: &[u8] = &buf[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                prop_assert_eq!(decoded, b);
            }
        }
    };
}

proptest_codec!(none,   CompressionType::None);
proptest_codec!(gzip,   CompressionType::Gzip);
proptest_codec!(snappy, CompressionType::Snappy);
proptest_codec!(lz4,    CompressionType::Lz4);
proptest_codec!(zstd,   CompressionType::Zstd);
```

- [ ] **Step 2: Run**

```bash
cargo test -p crabka-protocol --test proptest_records
```

Expected: 5 proptest cases pass (256 default iterations each).

- [ ] **Step 3: Commit**

```bash
git add crates/protocol
git commit -m "test(records): per-codec proptest round-trip suite"
```

---

### Task 16: JVM oracle — `record_batch_encode` and `record_batch_decode` ops

**Files:**
- Modify: `tools/oracle/src/main/java/com/crabka/oracle/Oracle.java`

- [ ] **Step 1: Add the new ops**

The existing dispatch on `req.get("op")` gains two cases:

```java
case "record_batch_encode": {
    return encodeRecordBatch(req.get("value"));
}
case "record_batch_decode": {
    byte[] bytes = HexFormat.of().parseHex(req.get("hex").asText());
    return decodeRecordBatch(bytes);
}
```

Implement two helper methods using Kafka's `MemoryRecords` /
`MemoryRecordsBuilder`. Kafka 4.2.0's API:

```java
private static ObjectNode encodeRecordBatch(JsonNode value) throws Exception {
    long baseOffset = value.get("base_offset").asLong();
    short producerEpoch = (short) value.get("producer_epoch").asInt();
    int baseSequence = value.get("base_sequence").asInt();
    long producerId = value.get("producer_id").asLong();
    int partitionLeaderEpoch = value.get("partition_leader_epoch").asInt();
    long baseTimestamp = value.get("base_timestamp").asLong();
    boolean isTransactional = value.get("is_transactional").asBoolean();
    boolean isControl = value.get("is_control_batch").asBoolean();
    String tsType = value.get("timestamp_type").asText(); // "CreateTime" or "LogAppendTime"
    String codecName = value.get("compression").asText(); // "NONE" / "GZIP" / "SNAPPY" / "LZ4" / "ZSTD"
    org.apache.kafka.common.record.CompressionType compression =
        org.apache.kafka.common.record.CompressionType.valueOf(codecName);

    ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
    org.apache.kafka.common.record.MemoryRecordsBuilder mrb =
        org.apache.kafka.common.record.MemoryRecords.builder(
            buffer,
            org.apache.kafka.common.record.RecordBatch.CURRENT_MAGIC_VALUE,
            compression,
            org.apache.kafka.common.record.TimestampType.valueOf(tsType.toUpperCase().replace("CREATETIME","CREATE_TIME").replace("LOGAPPENDTIME","LOG_APPEND_TIME")),
            baseOffset,
            baseTimestamp,
            producerId,
            producerEpoch,
            baseSequence,
            isTransactional,
            isControl,
            partitionLeaderEpoch);

    JsonNode records = value.get("records");
    for (JsonNode r : records) {
        long ts = baseTimestamp + r.get("timestamp_delta").asLong();
        long offset = baseOffset + r.get("offset_delta").asLong();
        byte[] key = r.has("key") && !r.get("key").isNull() ? HexFormat.of().parseHex(r.get("key").asText()) : null;
        byte[] val = r.has("value") && !r.get("value").isNull() ? HexFormat.of().parseHex(r.get("value").asText()) : null;

        java.util.List<org.apache.kafka.common.header.Header> headers = new java.util.ArrayList<>();
        if (r.has("headers")) {
            for (JsonNode h : r.get("headers")) {
                String hk = h.get("key").asText();
                byte[] hv = h.has("value") && !h.get("value").isNull() ? HexFormat.of().parseHex(h.get("value").asText()) : null;
                headers.add(new org.apache.kafka.common.header.internals.RecordHeader(hk, hv));
            }
        }
        mrb.appendWithOffset(offset, ts, key, val, headers.toArray(new org.apache.kafka.common.header.Header[0]));
    }
    org.apache.kafka.common.record.MemoryRecords mr = mrb.build();
    ByteBuffer out = mr.buffer();
    byte[] bytes = new byte[out.remaining()];
    out.duplicate().get(bytes);

    ObjectNode resp = M.createObjectNode();
    resp.put("ok", true);
    resp.put("hex", HexFormat.of().formatHex(bytes));
    return resp;
}

private static ObjectNode decodeRecordBatch(byte[] bytes) throws Exception {
    org.apache.kafka.common.record.MemoryRecords mr =
        org.apache.kafka.common.record.MemoryRecords.readableRecords(ByteBuffer.wrap(bytes));
    java.util.Iterator<org.apache.kafka.common.record.MutableRecordBatch> it = mr.batches().iterator();
    if (!it.hasNext()) {
        ObjectNode err = M.createObjectNode();
        err.put("ok", false);
        err.put("error", "no batch in input");
        return err;
    }
    org.apache.kafka.common.record.MutableRecordBatch b = it.next();

    ObjectNode value = M.createObjectNode();
    value.put("base_offset", b.baseOffset());
    value.put("partition_leader_epoch", b.partitionLeaderEpoch());
    value.put("compression", b.compressionType().name()); // NONE / GZIP / etc
    value.put("timestamp_type",
        b.timestampType() == org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME
            ? "LogAppendTime" : "CreateTime");
    value.put("is_transactional", b.isTransactional());
    value.put("is_control_batch", b.isControlBatch());
    value.put("base_timestamp", b.baseSequence() == -1 ? b.maxTimestamp() : b.firstOffset()); // adjust
    // ... fill the rest from the batch ...

    ArrayNode records = value.putArray("records");
    for (org.apache.kafka.common.record.Record r : b) {
        ObjectNode rj = records.addObject();
        rj.put("offset_delta", r.offset() - b.baseOffset());
        rj.put("timestamp_delta", r.timestamp() - b.maxTimestamp()); // adjust
        if (r.hasKey()) {
            byte[] k = new byte[r.keySize()];
            r.key().duplicate().get(k);
            rj.put("key", HexFormat.of().formatHex(k));
        } else { rj.putNull("key"); }
        if (r.hasValue()) {
            byte[] v = new byte[r.valueSize()];
            r.value().duplicate().get(v);
            rj.put("value", HexFormat.of().formatHex(v));
        } else { rj.putNull("value"); }
        ArrayNode hs = rj.putArray("headers");
        for (org.apache.kafka.common.header.Header h : r.headers()) {
            ObjectNode hj = hs.addObject();
            hj.put("key", h.key());
            if (h.value() != null) hj.put("value", HexFormat.of().formatHex(h.value()));
            else hj.putNull("value");
        }
    }

    ObjectNode resp = M.createObjectNode();
    resp.put("ok", true);
    resp.set("value", value);
    return resp;
}
```

> The above sketch uses Kafka 4.2.0's public `MemoryRecords` /
> `MemoryRecordsBuilder` API. Class names should be stable. If a method
> signature differs (e.g., `appendWithOffset` has a different parameter
> order in 4.2), grep the jar via
> `unzip -l tools/oracle/build/install/crabka-oracle/lib/kafka-clients-*.jar`
> and adjust.

- [ ] **Step 2: Rebuild the oracle and smoke-test**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
(cd tools/oracle && ./gradlew installDist -q --no-daemon)

# Smoke: encode an empty batch (no records) at compression=NONE
echo '{"op":"record_batch_encode","value":{"base_offset":0,"partition_leader_epoch":0,"compression":"NONE","timestamp_type":"CreateTime","is_transactional":false,"is_control_batch":false,"base_timestamp":0,"producer_id":-1,"producer_epoch":-1,"base_sequence":-1,"records":[]}}' \
    | tools/oracle/build/install/crabka-oracle/bin/crabka-oracle.bat
```

Expected: a line with `"ok":true` and a hex string of the empty batch.

- [ ] **Step 3: Commit**

```bash
git add tools/oracle
git commit -m "feat(oracle): record_batch_encode/decode ops for differential testing"
```

---

### Task 17: Differential tests for `RecordBatch`

**Files:**
- Create: `crates/protocol/tests/differential_records.rs`
- Modify: `crates/protocol/tests/support/oracle.rs` — add `record_batch_encode` / `record_batch_decode` helpers

- [ ] **Step 1: Extend the oracle wrapper**

In `crates/protocol/tests/support/oracle.rs`, add methods:

```rust
pub fn record_batch_encode(&mut self, value: &Value) -> Vec<u8> {
    let r = self.call(&json!({
        "op": "record_batch_encode",
        "value": value,
    }));
    hex::decode(r["hex"].as_str().unwrap()).unwrap()
}

pub fn record_batch_decode(&mut self, bytes: &[u8]) -> Value {
    let r = self.call(&json!({
        "op": "record_batch_decode",
        "hex": hex::encode(bytes),
    }));
    r["value"].clone()
}
```

- [ ] **Step 2: Write differential tests**

`crates/protocol/tests/differential_records.rs`:

```rust
mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_compression::CompressionType;
use crabka_protocol::records::{Record, RecordBatch, RecordHeader};
use proptest::prelude::*;
use serde_json::{json, Value};

fn record_to_json(r: &Record) -> Value {
    let mut headers = Vec::new();
    for h in &r.headers {
        headers.push(json!({
            "key": h.key,
            "value": h.value.as_ref().map(|b| hex::encode(b)),
        }));
    }
    json!({
        "offset_delta": r.offset_delta,
        "timestamp_delta": r.timestamp_delta,
        "key": r.key.as_ref().map(|b| hex::encode(b)),
        "value": r.value.as_ref().map(|b| hex::encode(b)),
        "headers": headers,
    })
}

fn batch_to_json(b: &RecordBatch) -> Value {
    let codec_name = match b.attributes.compression() {
        CompressionType::None => "NONE",
        CompressionType::Gzip => "GZIP",
        CompressionType::Snappy => "SNAPPY",
        CompressionType::Lz4 => "LZ4",
        CompressionType::Zstd => "ZSTD",
    };
    json!({
        "base_offset": b.base_offset,
        "partition_leader_epoch": b.partition_leader_epoch,
        "compression": codec_name,
        "timestamp_type": match b.attributes.timestamp_type() {
            crabka_protocol::records::TimestampType::CreateTime => "CreateTime",
            crabka_protocol::records::TimestampType::LogAppendTime => "LogAppendTime",
        },
        "is_transactional": b.attributes.is_transactional(),
        "is_control_batch": b.attributes.is_control_batch(),
        "base_timestamp": b.base_timestamp,
        "producer_id": b.producer_id,
        "producer_epoch": b.producer_epoch,
        "base_sequence": b.base_sequence,
        "records": b.records.iter().map(record_to_json).collect::<Vec<_>>(),
    })
}

fn arb_record() -> impl Strategy<Value = Record> {
    (
        (-1_000_000i64..1_000_000),
        (0i32..100),
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=256).prop_map(bytes::Bytes::from)),
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=1024).prop_map(bytes::Bytes::from)),
    )
        .prop_map(|(ts, off, key, value)| Record {
            timestamp_delta: ts,
            offset_delta: off,
            key,
            value,
            ..Default::default()
        })
}

fn arb_batch(codec: CompressionType) -> impl Strategy<Value = RecordBatch> {
    proptest::collection::vec(arb_record(), 0..=6)
        .prop_map(move |records| {
            let mut b = RecordBatch {
                records,
                ..Default::default()
            };
            b.attributes = b.attributes.with_compression(codec);
            b
        })
}

macro_rules! diff_test {
    ($name:ident, $codec:expr) => {
        #[test]
        #[ignore = "requires JVM oracle"]
        fn $name() {
            let oracle_cell = std::cell::RefCell::new(oracle::shared());
            proptest!(|(b in arb_batch($codec))| {
                let mut o = oracle_cell.borrow_mut();

                // Rust encodes; JVM decodes; structural equality on the JSON projection
                let mut rust_bytes = BytesMut::new();
                b.encode(&mut rust_bytes).unwrap();
                let jvm_decoded = o.record_batch_decode(&rust_bytes);
                let expected = batch_to_json(&b);
                // Compare records arrays (the JVM's full JSON has more fields we don't care about here)
                prop_assert_eq!(&jvm_decoded["records"], &expected["records"]);

                // JVM encodes; Rust decodes; round-trip back to Rust batch
                let jvm_bytes = o.record_batch_encode(&expected);
                let mut cur: &[u8] = &jvm_bytes[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                prop_assert_eq!(decoded.records.len(), b.records.len());
                for (i, (a, b_)) in decoded.records.iter().zip(b.records.iter()).enumerate() {
                    prop_assert_eq!(a.key.as_deref(), b_.key.as_deref(), "record[{}].key", i);
                    prop_assert_eq!(a.value.as_deref(), b_.value.as_deref(), "record[{}].value", i);
                    prop_assert_eq!(a.offset_delta, b_.offset_delta, "record[{}].offset_delta", i);
                }
            });
        }
    };
}

diff_test!(diff_none,   CompressionType::None);
diff_test!(diff_gzip,   CompressionType::Gzip);
diff_test!(diff_snappy, CompressionType::Snappy);
diff_test!(diff_lz4,    CompressionType::Lz4);
diff_test!(diff_zstd,   CompressionType::Zstd);
```

- [ ] **Step 3: Run**

```bash
cargo test -p crabka-protocol --test differential_records -- --ignored
```

Expected: 5 tests pass. If any codec produces a structural mismatch, fix the codec or the JSON-projection helper, not the assertions.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol/tests
git commit -m "test(records): JVM differential tests per codec"
```

---

### Task 18: Codegen mapping update — `records` → `RecordBatch`

**Files:**
- Modify: `crates/protocol-codegen/src/type_map.rs`

- [ ] **Step 1: Change the mapping**

In `crates/protocol-codegen/src/type_map.rs`, find the two `"records"` arms in `inner_owned` and `inner_borrowed`. Change them:

```rust
"records" => "crate::records::RecordBatch".into(),                          // owned
"records" => "crate::records::RecordBatchBorrowed<'a>".into(),              // borrowed
```

(The current value is `::bytes::Bytes` / `&'a [u8]`.)

- [ ] **Step 2: Regenerate**

```bash
./tools/regenerate.sh
```

Affects: every CURATED message with a `records` schema field. Currently
that's `ProduceRequest`. Snapshots will update.

- [ ] **Step 3: Update snapshots and run tests**

```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-protocol-codegen
cargo test -p crabka-protocol-codegen
cargo test -p crabka-protocol
```

Expected: snapshot tests regenerate then pass; protocol tests still pass.

- [ ] **Step 4: Run differential regression**

```bash
cargo test -p crabka-protocol --test differential_produce -- --ignored
```

Expected: the existing differential tests still pass. **If any fail with byte mismatches**, the bug is in the new RecordBatch encode/decode — fix it; do NOT accept divergence.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(codegen): records type-map switches to typed RecordBatch"
```

---

### Task 19: CodSpeed bench

**Files:**
- Create: `crates/protocol/benches/records.rs`
- Modify: `crates/protocol/Cargo.toml` — declare the new bench target

- [ ] **Step 1: Declare the bench**

In `crates/protocol/Cargo.toml`:

```toml
[[bench]]
name = "records"
harness = false
```

(`criterion` is already a dev-dep in the protocol crate from earlier work.)

- [ ] **Step 2: Write the bench**

`crates/protocol/benches/records.rs`:

```rust
use bytes::{Bytes, BytesMut};
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};

use crabka_compression::CompressionType;
use crabka_protocol::records::{Record, RecordBatch};

fn make_batch(record_count: usize, payload_size: usize, codec: CompressionType) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.attributes = b.attributes.with_compression(codec);
    for i in 0..record_count {
        b.records.push(Record {
            offset_delta: i as i32,
            timestamp_delta: (i as i64) * 10,
            key: Some(Bytes::from(format!("key-{i:08}"))),
            value: Some(Bytes::from(vec![0xABu8; payload_size])),
            ..Default::default()
        });
    }
    b
}

fn bench_codec(c: &mut Criterion, name: &str, codec: CompressionType) {
    let mut group = c.benchmark_group(name);
    const RECORDS: usize = 100;
    const PAYLOAD: usize = 256;

    let batch = make_batch(RECORDS, PAYLOAD, codec);
    let mut encoded = BytesMut::new();
    batch.encode(&mut encoded).unwrap();
    let encoded_bytes: Vec<u8> = encoded.to_vec();

    group.bench_function("encode_100_records_256B", |b| {
        let mut buf = BytesMut::with_capacity(encoded_bytes.len());
        b.iter(|| {
            buf.clear();
            black_box(&batch).encode(&mut buf).unwrap();
        });
    });

    group.bench_function("decode_100_records_256B", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&encoded_bytes);
            RecordBatch::decode(&mut cur).unwrap()
        });
    });

    group.finish();
}

fn bench_none(c: &mut Criterion)   { bench_codec(c, "records_none",   CompressionType::None); }
fn bench_gzip(c: &mut Criterion)   { bench_codec(c, "records_gzip",   CompressionType::Gzip); }
fn bench_snappy(c: &mut Criterion) { bench_codec(c, "records_snappy", CompressionType::Snappy); }
fn bench_lz4(c: &mut Criterion)    { bench_codec(c, "records_lz4",    CompressionType::Lz4); }
fn bench_zstd(c: &mut Criterion)   { bench_codec(c, "records_zstd",   CompressionType::Zstd); }

criterion_group!(records, bench_none, bench_gzip, bench_snappy, bench_lz4, bench_zstd);
criterion_main!(records);
```

- [ ] **Step 3: Smoke-bench**

```bash
cargo bench -p crabka-protocol --bench records -- --quick
```

Expected: each of the 10 (5 codecs × 2 directions) benches runs through one iteration.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "bench(records): per-codec encode/decode CodSpeed benches"
```

---

## Phase E — Acceptance

### Task 20: Acceptance gate

Verification only. Mark complete only when every item passes.

- [x] `cargo fmt --check` clean.
- [x] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [x] `cargo build -p crabka-protocol --no-default-features` succeeds.
- [x] Each single-feature build succeeds: `--features gzip`, `--features snappy`, `--features lz4`, `--features zstd`.
- [x] `cargo build -p crabka-protocol` (default features) succeeds.
- [x] `cargo test --workspace` clean.
- [x] `cargo test --workspace -- --include-ignored` clean (JVM differential records pass for all 5 codecs).
- [x] `cargo test -p crabka-protocol --test differential_produce -- --ignored` continues to pass (regression check).
- [x] `cargo bench -p crabka-protocol --bench records -- --quick` runs without crashing.
- [x] `RecordBatchHeader` is `#[repr(C)]`, derives `FromBytes + KnownLayout + Immutable + Unaligned`, is exactly 61 bytes.
- [x] CRC mismatches and unsupported magic produce typed errors (no panics).
- [x] Borrowed flavor passes the pointer-identity test for uncompressed batches.
- [x] `to_owned()` on borrowed materialises an equal owned batch.
- [x] Codegen mapping for `records` emits `RecordBatch` / `RecordBatchBorrowed<'a>`.
- [x] All tests in this plan follow the parameterized + shared-fixture pattern (table-driven / `macro_rules!` / factored proptest `Strategy`s).

When all items pass, push the feature branch and open a PR to `main`.

```bash
git push -u origin feature/records-1c
gh pr create --base main --head feature/records-1c \
    --title "Sub-plan 1c: typed RecordBatch v2" \
    --body "Implements typed RecordBatch v2 with zerocopy header reinterpretation and crabka-compression integration. See spec docs/superpowers/specs/2026-05-11-crabka-records-1c-design.md."
```

---

## Self-review against the spec

**Spec coverage:**

| Spec requirement | Plan coverage |
|---|---|
| `crates/protocol/src/records/` module with header/crc/owned/borrowed/error | Tasks 3-6, 7, 12 |
| `crabka-protocol` depends on `crabka-compression` + mirror features | Task 2 |
| `RecordBatchHeader` zerocopy `FromBytes + KnownLayout + Immutable + Unaligned`, size 61 | Task 6 |
| CRC-32C validation on decode, JVM byte-equal on encode | Task 9 (+ refinement note) |
| v2 magic enforced; v0/v1 rejected | Task 9 |
| Owned encode/decode/encoded_len round-trip across all 5 codecs | Tasks 9, 10, 15 |
| Borrowed decode zero-copies uncompressed batches (pointer identity) | Task 13 |
| Compressed batches use fresh `Bytes` body | Task 13 |
| `to_owned()` bridge | Task 13 |
| Codegen `type_map` `records` → `RecordBatch` / `RecordBatchBorrowed<'a>` | Task 18 |
| JVM-differential per codec, both directions | Tasks 16, 17 |
| No regressions in existing differential tests | Task 18 (regression check) |
| CodSpeed benches per codec | Task 19 |
| Parameterized + shared-fixture tests throughout | every test task uses `macro_rules!` or table loops |
| Rustdoc on public types | spec language; implementer adds doc comments alongside each definition |

**Placeholder scan:**
- Task 9 carries a `crc32c_combine` placeholder that the same task's Step 2 instructs the implementer to remove and replace with `crc32c::crc32c_append`. The replacement is concrete; the placeholder is a temporary scaffold the plan deliberately walks the implementer through. Not a hidden TODO.
- Task 16's Java sketch flags two `// adjust` comments and a `... fill the rest` placeholder. These are spots where the Kafka 4.2.0 API method names should be verified against the jar before relying on them. The plan calls this out explicitly. **Replace with concrete field assignments** during implementation, after grep-confirming each accessor (`b.producerId()`, `b.producerEpoch()`, `b.baseSequence()`, `b.maxTimestamp()`, etc.). If any accessor name differs, find the actual name and use it. **Do not leave placeholders in committed code.**

**Type consistency:**
- `RecordBatch::encode` signature is consistent: inherent `pub fn encode<B: BufMut>(&self, buf: &mut B) -> Result<(), RecordsError>`. The trait impl (Task 11) shadows it through `crate::Encode::encode(&self, buf: &mut B, version: i16) -> Result<(), crate::ProtocolError>`. Both names match.
- `RecordBatchBorrowed` (re-export of `borrowed::RecordBatch`) is referenced consistently in Task 18's type-map update.
- `Attributes::with_compression` / `with_timestamp_type` / `with_transactional` / `with_control` consistent between Task 5 and downstream test fixtures.

Plan is ready for execution.
