# Typed `RecordBatch` (sub-plan 1c) — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessors:** coverage meta-spec (`2026-05-11-crabka-protocol-coverage-design.md`); compression (`2026-05-11-crabka-compression-1b-design.md`).

## Summary

Add a typed `RecordBatch` v2 decoder/encoder to `crabka-protocol` that
consumes `crabka-compression`. After 1c ships, `records` fields in
generated messages (Produce, Fetch, …) move from opaque `Bytes` to a
fully typed `RecordBatch` value with decompressed `Record`s exposed.

The fixed 61-byte v2 header is reinterpreted via the
[`zerocopy`](https://github.com/google/zerocopy) crate — no allocation,
no byte-by-byte parsing. Variable-length record bodies (varint-prefixed)
continue to use the existing owned/borrowed flavor pattern.

## North star (acceptance gate for sub-plan 1c)

1. `crates/protocol/src/records/` exists with typed owned + borrowed
   `RecordBatch` and `Record` types.
2. The 61-byte v2 header is `zerocopy`-derived (`FromBytes`,
   `KnownLayout`, `Immutable`, `Unaligned`).
3. CRC-32C validation on decode; correct CRC computed on encode (JVM
   byte-equal).
4. Only v2 magic accepted; v0/v1 produce `UnsupportedMagic`.
5. The borrowed flavor delivers true zero-copy on uncompressed batches
   (pointer-identity test). Compressed batches use a fresh `Bytes`
   body; per-record slices borrow from it.
6. Codegen's `records` schema-type mapping switches from `Bytes` to
   `RecordBatch` (owned) / `RecordBatch<'a>` (borrowed); all curated
   messages that have `records` fields regenerated with no manual
   wrapper edits.
7. JVM-differential tests pass per compression codec, both directions.
8. No regressions in existing differential tests (api_versions, metadata,
   produce, offset_commit, describe_groups).
9. CI matrix green on Linux/macOS/Windows × Rust 1.95.0.

## Non-goals

- **v0/v1 record batches.** Modern brokers reject them on the wire. Old
  log segments are a `crabka-log` concern.
- **Schema-registry / serdes / key-value typing.** Records carry
  `Option<Bytes>` / `Option<&[u8]>` for keys and values.
- **Streaming / lazy iteration.** Decode materialises a `Vec<Record>`
  eagerly for owned; borrowed exposes an iterator that lazily parses
  per call to `.next()`, but the underlying body buffer is fully
  materialised (decompressed in one shot).
- **Compaction/tombstone semantics, transaction-marker filtering.** The
  decoder surfaces whatever's on the wire; higher layers interpret.

---

# 1. Wire format reminder (informational)

```
RecordBatch v2:

  ┌────────── 61-byte fixed header ──────────┐
  base_offset:           i64 BE
  batch_length:          i32 BE   (bytes that follow, inclusive of fields below)
  partition_leader_epoch:i32 BE
  magic:                 i8       (= 2 for v2)
  crc:                   u32 BE   (CRC-32C of everything below)
  attributes:            i16 BE   (compression bits 0-2, ts type bit 3,
                                   transactional bit 4, control bit 5)
  last_offset_delta:     i32 BE
  base_timestamp:        i64 BE
  max_timestamp:         i64 BE
  producer_id:           i64 BE   (-1 if non-idempotent)
  producer_epoch:        i16 BE
  base_sequence:         i32 BE   (-1 if non-idempotent)
  records_count:         i32 BE
  └─────────────────────────────────────────┘
  records: [Record; records_count]   (optionally compressed per attributes)

Record (after decompression):
  length:           varlong (zigzag)
  attributes:       i8
  timestamp_delta:  varlong
  offset_delta:     varint
  key_length:       varint    (-1 = null)
  key:              [u8; key_length]
  value_length:     varint    (-1 = null)
  value:            [u8; value_length]
  header_count:     varint
  headers:          [Header; header_count]

Header:
  key_length:   varint (non-null, non-negative)
  key:          utf-8
  value_length: varint (-1 = null)
  value:        [u8; value_length]
```

---

# 2. `zerocopy` adoption policy (project-wide)

A struct is a `zerocopy` candidate when **all** of these hold:

1. Fixed binary layout (no varints, no length-prefixed variable fields,
   no version-conditional fields).
2. Big-endian numeric fields at known offsets.
3. No padding or alignment requirements (or solvable with
   `#[derive(Unaligned)]` + `#[repr(C, packed)]`).
4. Decode is the hot path.

When all four hold, declare the struct via:

```rust
use zerocopy::byteorder::{I16, I32, I64, U32};
use zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned};

#[derive(FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct Foo { /* … */ }
```

Decode is `Foo::ref_from_bytes(&buf[..N])?` — zero allocation, zero
copy. Numeric access via `.get()` on the BE wrappers.

**Where `zerocopy` does NOT apply** (and the existing owned/borrowed
flavor pattern remains correct):

- Varint / varlong / UVARINT fields.
- Length-prefixed strings/bytes.
- Length-prefixed arrays.
- Tagged fields (KIP-482).
- Version-conditional fields.
- Most Kafka request/response message bodies.
- Per-record bodies inside a `RecordBatch`.

**1c's `RecordBatchHeader` is the canonical use case.** Future fixed-
layout headers (e.g., RecordBatch v0/v1 in `crabka-log`) get the same
treatment from day one. Existing primitives, codegen-emitted messages,
and `RequestHeader` are NOT retrofitted — they have variable parts in
later versions, or are not the hot path.

---

# 3. Module layout

```
crates/protocol/src/records/
├── mod.rs        # public API re-exports
├── header.rs     # zerocopy RecordBatchHeader + Attributes + TimestampType
├── crc.rs        # CRC-32C wrapping the `crc32c` crate
├── owned.rs      # RecordBatch (owned), Record, RecordHeader
├── borrowed.rs   # RecordBatch<'a>, Record<'a>, RecordHeader<'a>
└── error.rs      # RecordsError + From<RecordsError> for ProtocolError
```

`crates/protocol/Cargo.toml` adds:

- `zerocopy = { workspace = true, features = ["derive"] }`
- `crc32c = { workspace = true }`
- `crabka-compression = { workspace = true }`
- Mirror-features: `gzip`/`snappy`/`lz4`/`zstd` each forwards to the
  same feature on `crabka-compression`.

---

# 4. Public API

```rust
// crates/protocol/src/records/mod.rs

mod borrowed;
mod crc;
pub mod header;
mod owned;
mod error;

pub use error::RecordsError;
pub use header::{Attributes, TimestampType};
pub use owned::{Record, RecordBatch, RecordHeader};
pub use borrowed::{
    Record as RecordBorrowed,
    RecordBatch as RecordBatchBorrowed,
    RecordHeader as RecordHeaderBorrowed,
};
```

### `header::Attributes`

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attributes(pub i16);

impl Attributes {
    pub const TIMESTAMP_TYPE_BIT: i16 = 1 << 3;
    pub const TRANSACTIONAL_BIT:  i16 = 1 << 4;
    pub const CONTROL_BIT:        i16 = 1 << 5;

    #[must_use] pub fn compression(self) -> crabka_compression::CompressionType { /* low 3 bits */ }
    #[must_use] pub fn timestamp_type(self) -> TimestampType { /* bit 3 */ }
    #[must_use] pub fn is_transactional(self) -> bool        { self.0 & Self::TRANSACTIONAL_BIT != 0 }
    #[must_use] pub fn is_control_batch(self) -> bool        { self.0 & Self::CONTROL_BIT != 0 }

    // Builders return a new Attributes; chainable.
    #[must_use] pub fn with_compression(self, c: crabka_compression::CompressionType) -> Self { /* … */ }
    #[must_use] pub fn with_timestamp_type(self, t: TimestampType) -> Self { /* … */ }
    #[must_use] pub fn with_transactional(self, b: bool) -> Self { /* … */ }
    #[must_use] pub fn with_control(self, b: bool) -> Self { /* … */ }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType { CreateTime, LogAppendTime }
```

### `header::RecordBatchHeader`

```rust
use zerocopy::byteorder::{I16, I32, I64, U32};
use zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned};

#[derive(Debug, FromBytes, KnownLayout, Immutable, Unaligned, Clone, Copy)]
#[repr(C)]
pub struct RecordBatchHeader {
    pub base_offset:             I64<BigEndian>,
    pub batch_length:            I32<BigEndian>,
    pub partition_leader_epoch:  I32<BigEndian>,
    pub magic:                   i8,
    pub crc:                     U32<BigEndian>,
    pub attributes:              I16<BigEndian>,
    pub last_offset_delta:       I32<BigEndian>,
    pub base_timestamp:          I64<BigEndian>,
    pub max_timestamp:           I64<BigEndian>,
    pub producer_id:             I64<BigEndian>,
    pub producer_epoch:          I16<BigEndian>,
    pub base_sequence:           I32<BigEndian>,
    pub records_count:           I32<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<RecordBatchHeader>() == 61);
```

### Owned `RecordBatch`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset:            i64,
    pub partition_leader_epoch: i32,
    pub attributes:             Attributes,
    pub last_offset_delta:      i32,
    pub base_timestamp:         i64,
    pub max_timestamp:          i64,
    pub producer_id:            i64,
    pub producer_epoch:         i16,
    pub base_sequence:          i32,
    pub records:                Vec<Record>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub attributes:      i8,
    pub timestamp_delta: i64,
    pub offset_delta:    i32,
    pub key:             Option<bytes::Bytes>,
    pub value:           Option<bytes::Bytes>,
    pub headers:         Vec<RecordHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key:   String,
    pub value: Option<bytes::Bytes>,
}

impl RecordBatch {
    pub fn decode<B: bytes::Buf>(buf: &mut B) -> Result<Self, RecordsError>;
    pub fn encode<B: bytes::BufMut>(&self, buf: &mut B) -> Result<(), RecordsError>;
    pub fn encoded_len(&self) -> usize;
}

// Also implements crate::Encode and crate::Decode<'de> so codegen can
// call .encode(buf, version) / decode(buf, version) uniformly with
// other struct-typed fields. The `version` parameter is ignored — the
// record-batch format is independent of message version.
```

### Borrowed `RecordBatch<'a>`

```rust
pub struct RecordBatch<'a> {
    header: &'a header::RecordBatchHeader,
    body:   RecordBody<'a>,
}

enum RecordBody<'a> {
    Borrowed(&'a [u8]),    // points into the input buffer (uncompressed batches)
    Owned(bytes::Bytes),   // freshly decompressed (compressed batches)
}

pub struct Record<'a> {
    pub attributes:      i8,
    pub timestamp_delta: i64,
    pub offset_delta:    i32,
    pub key:             Option<&'a [u8]>,
    pub value:           Option<&'a [u8]>,
    pub headers:         Vec<RecordHeader<'a>>,
}

pub struct RecordHeader<'a> {
    pub key:   &'a str,
    pub value: Option<&'a [u8]>,
}

impl<'a> RecordBatch<'a> {
    pub fn header(&self) -> &header::RecordBatchHeader;
    pub fn attributes(&self) -> Attributes;
    pub fn iter(&self) -> impl Iterator<Item = Result<Record<'a>, RecordsError>> + '_;
    pub fn to_owned(&self) -> Result<super::owned::RecordBatch, RecordsError>;
}

impl<'a> crate::DecodeBorrow<'a> for RecordBatch<'a> {
    fn decode_borrow(buf: &mut &'a [u8], _version: i16) -> Result<Self, crate::ProtocolError>;
}

impl<'a> crate::Encode for RecordBatch<'a> {
    fn encode<B: bytes::BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), crate::ProtocolError>;
    fn encoded_len(&self, _version: i16) -> usize;
}
```

**Zero-copy claim, exact:** for an uncompressed batch, decode reads the
length prefix, reinterprets the next 61 bytes as `&'a RecordBatchHeader`
via `ref_from_bytes`, then slices the rest of the body as `&'a [u8]` for
`RecordBody::Borrowed`. No allocation. Per-record key/value slices
returned by `iter()` point into that same `&'a [u8]`. Pointer identity
holds across the whole chain.

For a compressed batch, `crabka_compression::decompress` returns a fresh
`Bytes`. `RecordBody::Owned(bytes)` holds it; record slices are borrowed
from the `Bytes`'s contents (which `Bytes::slice` keeps alive as long as
any reference exists). Pointer identity holds within the decompressed
buffer.

### `RecordsError`

```rust
#[derive(Debug, thiserror::Error)]
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
        crate::ProtocolError::InvalidValue(/* … */)
    }
}
```

---

# 5. Codegen integration

`crabka-protocol-codegen`'s `type_map.rs` already routes the schema
type `records` to a Rust type. Update it:

- Owned mapping: `::bytes::Bytes` → `crate::records::RecordBatch`.
- Borrowed mapping: `&'a [u8]` → `crate::records::RecordBatchBorrowed<'a>`.

The encode/decode call sites already work uniformly for struct-typed
fields (proven by 1a Task 7's nested-struct support). No new emitter
capability is needed. The existing helpers call
`self.field.encode(buf, version)?` and `<Type>::decode(buf, version)?`
which dispatch to `RecordBatch`'s `Encode`/`Decode` impls automatically.

Regenerated message files for messages with `records` fields:
`ProduceRequest` (always has records), plus any other curated message
that exercises the schema's `records` primitive. The snapshot tests
will update accordingly; this is a one-time noisy diff during the 1c
implementation but stable afterwards.

---

# 6. Testing strategy

All test bodies follow the project's **parameterized + shared-fixture
preference**. Concretely:

- Tables (`&[(input, expected)]`) with a `#[track_caller]` helper
  function for similar cases that vary only in inputs.
- `macro_rules!` for tests that vary in types or codecs (the pattern
  already established by `roundtrip!` in `primitives/fixed.rs` and
  `diff_test!` in `compression/tests/differential.rs`).
- Fixture helpers in the same `mod tests` (or a shared
  `tests/support/`) so multiple tests reuse the same builders rather
  than duplicating literal data.
- proptest `Strategy`s factored as named functions
  (`fn arb_record_batch()`) reused across the proptest module and
  differential module.

## 6.1 Layer 1 — unit tests

**`header.rs` tests** — table-driven over hand-built 61-byte buffers:

```rust
macro_rules! header_field {
    ($name:ident, $byte_range:expr, $bytes:expr, $field:ident, $expected:expr) => {
        #[test] fn $name() {
            let mut buf = sample_header_bytes();
            buf[$byte_range].copy_from_slice($bytes);
            let h = RecordBatchHeader::ref_from_bytes(&buf).unwrap();
            assert_eq!(h.$field.get(), $expected);
        }
    };
}
header_field!(base_offset_min, 0..8,   &i64::MIN.to_be_bytes(), base_offset, i64::MIN);
header_field!(base_offset_max, 0..8,   &i64::MAX.to_be_bytes(), base_offset, i64::MAX);
header_field!(magic_two,       17..18, &[2u8],                  magic,       2);
// … one per field
```

`Attributes` accessor tests are similarly table-driven over `(bits,
expected_compression, expected_ts_type, ...)` tuples.

**`crc.rs` tests** — table-driven over known CRC-32C vectors:

```rust
const CRC_VECTORS: &[(&[u8], u32)] = &[
    (b"",            0x00000000),
    (b"123456789",   0xE3069283),
    (KAFKA_CAPTURED_BODY, KAFKA_CAPTURED_CRC),
];
#[test] fn crc_table() {
    for (input, expected) in CRC_VECTORS {
        assert_eq!(crc32c(input), *expected, "input={:?}", input);
    }
}
```

**`owned.rs` / `borrowed.rs` round-trip tests** — shared fixture builders:

```rust
fn fixture_empty_batch() -> RecordBatch { /* default header, records: vec![] */ }
fn fixture_single_record() -> RecordBatch { /* one record, key=Some, value=Some, no headers */ }
fn fixture_full_batch() -> RecordBatch { /* multiple records with headers, mixed null/non-null */ }
fn fixture_compressed_batch(c: CompressionType) -> RecordBatch { /* fixture_full_batch + attributes */ }

macro_rules! roundtrip_owned {
    ($name:ident, $fixture:ident) => {
        #[test] fn $name() {
            let b = $fixture();
            let mut buf = BytesMut::new();
            b.encode(&mut buf).unwrap();
            assert_eq!(b.encoded_len(), buf.len());
            let mut cur: &[u8] = &buf[..];
            let decoded = RecordBatch::decode(&mut cur).unwrap();
            assert_eq!(decoded, b);
            assert!(cur.is_empty());
        }
    };
}
roundtrip_owned!(owned_empty,           fixture_empty_batch);
roundtrip_owned!(owned_single_record,   fixture_single_record);
roundtrip_owned!(owned_full_batch,      fixture_full_batch);
```

The borrowed flavor mirrors this with a `roundtrip_borrowed!` macro
that adds a pointer-identity check.

## 6.2 Layer 2 — proptest, `crates/protocol/tests/proptest_records.rs`

```rust
fn arb_record_batch(compression: CompressionType) -> impl Strategy<Value = RecordBatch> { /* … */ }

macro_rules! proptest_roundtrip {
    ($name:ident, $ct:expr) => {
        proptest! {
            #[test]
            fn $name(b in arb_record_batch($ct)) {
                let mut buf = BytesMut::new();
                b.encode(&mut buf).unwrap();
                let mut cur: &[u8] = &buf[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                prop_assert_eq!(decoded, b);
            }
        }
    };
}
proptest_roundtrip!(roundtrip_none,   CompressionType::None);
proptest_roundtrip!(roundtrip_gzip,   CompressionType::Gzip);
proptest_roundtrip!(roundtrip_snappy, CompressionType::Snappy);
proptest_roundtrip!(roundtrip_lz4,    CompressionType::Lz4);
proptest_roundtrip!(roundtrip_zstd,   CompressionType::Zstd);
```

## 6.3 Layer 3 — JVM differential

Oracle gains two ops:

```
{"op":"record_batch_encode","value": <RecordBatch-as-JSON>}      → {"hex": "..."}
{"op":"record_batch_decode","hex": "..."}                        → {"value": ...}
```

Implemented via Kafka's `MemoryRecords` /
`MemoryRecordsBuilder` / `Record.Builder` classes. JSON shape mirrors
the Rust types one-to-one.

Tests use the same `diff_test!` macro pattern as compression's
differential file:

```rust
macro_rules! diff_test {
    ($name:ident, $ct:expr) => {
        #[test]
        #[ignore = "requires JVM oracle"]
        fn $name() {
            let mut o = oracle::shared();
            proptest!(|(b in arb_record_batch($ct))| {
                // round-trip Rust→JVM and JVM→Rust
            });
        }
    };
}
diff_test!(diff_none,   CompressionType::None);
diff_test!(diff_gzip,   CompressionType::Gzip);
// … etc
```

## 6.4 Layer 4 — CodSpeed bench

Single bench file `crates/protocol/benches/records.rs` parameterised
via criterion's `BenchmarkGroup` looping over compression codecs and a
canned 100-record batch. Pattern matches the existing benches.

## 6.5 Regression check

The existing `differential_produce.rs` continues to pass after
codegen regenerates Produce with `RecordBatch` in place of `Bytes`. No
new test file — the existing tests are the regression gate.

---

# 7. CI integration

- **`rust` matrix** (Linux/macOS/Windows × 1.95.0) picks up new tests
  transparently.
- **`jvm-differential` job** picks up `differential_records.rs`.
- **`drift` job** continues to apply (regenerated Produce + any other
  CURATED message with records).
- **`Run benchmarks` (CodSpeed)** picks up the new bench file.

No new workflows.

---

# 8. Acceptance criteria

The sub-plan ships when **all** of these hold:

1. `crates/protocol/src/records/` module exists with `header`, `crc`,
   `owned`, `borrowed`, `error` modules.
2. `crabka-protocol` depends on `crabka-compression` and `zerocopy` and
   `crc32c`. Four mirror-features (`gzip`/`snappy`/`lz4`/`zstd`)
   forward to `crabka-compression`.
3. `RecordBatchHeader` is a `zerocopy`-derived `#[repr(C)] FromBytes +
   KnownLayout + Immutable + Unaligned` struct of size 61 bytes.
4. CRC-32C validation on decode (rejects mismatched batches), CRC
   computed correctly on encode (JVM byte-equal).
5. v2 magic enforced; v0/v1 rejected with `UnsupportedMagic`.
6. Owned `RecordBatch::{encode, decode, encoded_len}` round-trip
   cleanly per unit + proptest at all five compression types.
7. Borrowed `RecordBatch<'a>::decode_borrow` zero-copies uncompressed
   batches (pointer-identity test passes); compressed batches use a
   fresh `Bytes` body.
8. `to_owned()` bridge on borrowed materialises an equivalent owned
   `RecordBatch`.
9. Codegen `type_map` updated so the schema type `records` emits
   `RecordBatch` / `RecordBatch<'a>`. All curated messages with
   `records` fields regenerated.
10. JVM-differential tests pass per compression codec, both directions,
    at PR-CI budget.
11. Existing differential tests (api_versions, metadata, produce,
    offset_commit, describe_groups) continue to pass — no regressions.
12. CodSpeed bench file added; per-codec decode + encode numbers
    recorded.
13. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
    warnings`, `cargo test --workspace -- --include-ignored` all green.
14. CI matrix green on Linux/macOS/Windows.
15. Rustdoc on every public type in `records::`; crate-level doc updated
    to mention typed RecordBatch.
16. All new tests follow the parameterized + shared-fixture preference
    documented in this design.

---

# 9. Open questions deferred to the implementation plan

- **Iterator vs slice for borrowed records.** The design says
  `RecordBatch<'a>::iter()` returns an iterator. Alternative: a
  `records: &[Record<'a>]` accessor that requires up-front parsing. The
  plan will pick one and justify; iterator is the recommended
  starting point (preserves the lazy-parsing benefit on the read side).
- **Whether `Record` should carry the absolute offset and timestamp** or
  just the deltas. Recommend deltas (matches the wire); a helper method
  on `RecordBatch::iter()` can yield `(absolute_offset, record)` if
  consumers need it later.
- **Decompression buffer sizing.** Whether to call
  `crabka_compression::decompress` directly (allocates a fresh Bytes)
  or expose a pooled-allocator hook. Defer; current allocation behavior
  is good enough for 1c.

None block this design.

---

# 10. Next step

Invoke `writing-plans` to produce a detailed implementation plan for
sub-plan 1c. Sub-plans 1d (mass rollout) and 1e (publish) get their own
brainstorm → plan cycles when their turn comes.
