# `crabka-compression` (sub-plan 1b) — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessor:** [`2026-05-11-crabka-protocol-coverage-design.md`](2026-05-11-crabka-protocol-coverage-design.md) (coverage meta-spec).

## Summary

`crabka-compression` is a standalone Rust crate covering the four
compression codecs Kafka uses on the wire — **gzip, snappy, lz4, zstd**
— with byte-level wire compatibility verified against the JVM
`kafka-clients` implementation. Pure Rust where viable; isolated as a
separate crate so other Crabka crates can choose which codecs they pull
in via Cargo features.

This sub-plan does not change `crabka-protocol`. The typed `RecordBatch`
decoder that consumes this crate is **sub-plan 1c**.

## North star (acceptance gate for sub-plan 1b)

1. `crabka-compression` 0.0.0 exists in the workspace, ready for
   downstream depending.
2. Free-function API over a `CompressionType` enum (Section 3 below).
3. Default features enable all four codecs; each individually toggleable
   via `--no-default-features --features <codec>`; calling a disabled
   codec returns `Err(CompressionError::FeatureDisabled(_))`.
4. For every supported codec, JVM-differential round-trips work both
   directions (Rust decompresses what JVM compressed, JVM decompresses
   what Rust compressed).
5. Proptest round-trips: `decompress(compress(x))? == x` for arbitrary
   `x` per codec.
6. CI matrix (Linux/macOS/Windows × Rust 1.95.0) green; the existing
   `jvm-differential` job picks up the new tests transparently.

## Non-goals

- **Streaming APIs.** Buffer-at-a-time is sufficient for Kafka's
  batch-oriented wire format.
- **Per-codec parameter exposure** (gzip level, lz4 block size, zstd
  level). Default parameters that match Kafka's defaults are enough.
- **Async APIs.** Compression is CPU-bound; callers can run it on a
  blocking pool if they need to.

---

# 1. Crate layout

```
crates/compression/
├── Cargo.toml                  # name = "crabka-compression"
├── src/
│   ├── lib.rs                  # CompressionType, compress/decompress, error, dispatch
│   ├── gzip.rs                 # #[cfg(feature = "gzip")]
│   ├── snappy.rs               # #[cfg(feature = "snappy")]
│   ├── lz4.rs                  # #[cfg(feature = "lz4")]
│   └── zstd.rs                 # #[cfg(feature = "zstd")]
└── tests/
    ├── proptest.rs             # per-codec round-trip property tests
    └── differential.rs         # vs JVM oracle (#[ignore]-gated)
```

Each codec module is a small (~50–150 LOC) facade over a third-party
crate, including any Kafka-specific framing.

# 2. Cargo.toml shape

```toml
[package]
name = "crabka-compression"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Kafka wire-protocol compression codecs for Rust"

[lints]
workspace = true

[features]
default = ["gzip", "snappy", "lz4", "zstd"]
gzip   = ["dep:flate2"]
snappy = ["dep:snap"]
lz4    = ["dep:lz4_flex"]
zstd   = ["dep:zstd"]

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
flate2   = { version = "1", default-features = false, features = ["rust_backend"], optional = true }
snap     = { version = "1", optional = true }
lz4_flex = { version = "0.11", default-features = false, features = ["frame"], optional = true }
zstd     = { version = "0.13", optional = true }

[dev-dependencies]
proptest = { workspace = true }
arbitrary = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
once_cell = { workspace = true }
```

# 3. Public API (`lib.rs`)

```rust
//! Kafka wire-protocol compression codecs.

use bytes::Bytes;

/// Codec identifier matching Kafka's record-batch attribute bits 0-2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionType {
    None   = 0,
    Gzip   = 1,
    Snappy = 2,
    Lz4    = 3,
    Zstd   = 4,
}

impl CompressionType {
    /// Decode the lowest three bits of a Kafka record-batch attribute byte.
    /// Returns `None` for codec ids outside `0..=4`.
    pub fn from_attribute_bits(b: u8) -> Option<Self> {
        match b & 0b0000_0111 {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Encode this codec into the lowest three bits of an attribute byte.
    pub fn as_attribute_bits(self) -> u8 { self as u8 }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CompressionError {
    #[error("compression feature `{0}` not enabled at compile time")]
    FeatureDisabled(&'static str),
    #[error("invalid compressed data: {0}")]
    InvalidData(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub fn compress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None   => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip   => gzip_compress(data),
        CompressionType::Snappy => snappy_compress(data),
        CompressionType::Lz4    => lz4_compress(data),
        CompressionType::Zstd   => zstd_compress(data),
    }
}

pub fn decompress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None   => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip   => gzip_decompress(data),
        CompressionType::Snappy => snappy_decompress(data),
        CompressionType::Lz4    => lz4_decompress(data),
        CompressionType::Zstd   => zstd_decompress(data),
    }
}

// Per-codec functions are conditionally compiled; the disabled variants
// return Err(FeatureDisabled(...)).
#[cfg(feature = "gzip")] fn gzip_compress(d: &[u8]) -> Result<Bytes, CompressionError> { gzip::compress(d) }
#[cfg(not(feature = "gzip"))] fn gzip_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}
// ... same shape for the other three codecs and both directions ...
```

`CompressionType::None` passes data through unchanged in both
directions via `Bytes::copy_from_slice` (cheap, single allocation).

# 4. Per-codec implementation notes

Each module's job is to wrap a third-party crate and emit/consume the
**exact wire framing Kafka writes**. Differential tests are the safety
net.

## 4.1 Gzip

- **Crate:** `flate2 = { version = "1", default-features = false, features = ["rust_backend"] }` —
  pure-Rust `miniz_oxide` backend; no C dep.
- **Framing:** standard RFC-1952 gzip; no Kafka-specific layer.
- **Parameters:** library default level (6). Match.
- **Risk:** Low.

## 4.2 Snappy — xerial framing trap

- **Crate:** `snap = "1"` provides raw block encode/decode only.
- **Framing:** Kafka does **not** use Google's official Snappy framing
  format. It uses **xerial-snappy**, a Java-library convention with
  magic `\x82SNAPPY\x00`, version bytes
  `0x00 0x00 0x00 0x01 0x00 0x00 0x00 0x01`, then a sequence of
  `(UINT32_BE length, raw-snappy-block)` chunks. This was hardcoded
  into Kafka in 2011 and never updated.
- **Implementation:** write the xerial framing ourselves (~80 LOC).
  No published crate ships it as of the cutoff.
- **Sanity check first:** before any encode work, the implementation
  task decodes a known JVM-produced xerial-snappy frame (captured as a
  hex constant) and asserts the decoded plaintext is correct.
- **Risk:** Medium.

## 4.3 LZ4 — frame format, independent blocks

- **Crate:**
  `lz4_flex = { version = "0.11", default-features = false, features = ["frame"] }` —
  pure-Rust LZ4 frame implementation.
- **Framing:** LZ4 frame format (magic `0x04 22 4D 18`).
- **Parameters:** Kafka writes 64 KiB blocks, **independent** (each
  block decompressable in isolation), block-checksum disabled,
  content-size present. Match.
- **Risk:** Medium. Old Kafka (pre-0.10) had a checksum bug
  ("KAFKA-3160"); 4.x is well past it, no compatibility shim needed.

## 4.4 Zstd

- **Crate:** `zstd = "0.13"` — wraps `libzstd` (C). Only unavoidable C
  dep; pure-Rust alternatives (`ruzstd`) are decompression-only.
- **Framing:** plain zstd frame; no Kafka-specific layer.
- **Parameters:** library default level (3). Match.
- **Risk:** Low. Windows MSVC + MinGW linkage handled by `zstd-sys`.

## 4.5 Cross-codec concerns

- **Empty input.** Each codec's `decompress(b"")` returns
  `Err(InvalidData(...))` (most frame formats require at least a header).
  `compress(c, b"")` returns a minimal valid frame. Tested per codec.
- **Max sizes.** No artificial cap. Library-native limits apply
  (effectively unbounded for our use case).
- **Allocation.** Each call returns a fresh `Bytes`. No scratch-buffer
  reuse in 1b; revisit in 1c if profiling shows allocation hot.

# 5. JVM oracle extension

The existing `tools/oracle/` speaks message-level JSON ops. This
sub-plan adds two new ops:

```
{"op":"compress",   "codec":"gzip|snappy|lz4|zstd", "data":"<hex>"}
{"op":"decompress", "codec":"gzip|snappy|lz4|zstd", "data":"<hex>"}
```

Implementation uses Kafka's own
`org.apache.kafka.common.compress.{Gzip,Snappy,Lz4,Zstd}Compression`
classes (their stable API since 4.x — confirm at task time). Output is
hex-encoded bytes.

# 6. Test strategy

Three layers, mirroring `crabka-protocol`:

**Layer 1 — Unit (per codec module).**
- Round-trip a short known input (`b"hello kafka"`).
- Empty-input behaviour per codec.
- Malformed input → `InvalidData`, never panic.
- For snappy and lz4: decode a hand-captured JVM frame (hex constant)
  to prove the framing layer is right before any encode work runs.

**Layer 2 — Proptest (`tests/proptest.rs`).**
- Per codec: `decompress(c, &compress(c, &x)?)? == x` for `Vec<u8>` of
  size 0–32 KiB, default 256-case budget.

**Layer 3 — JVM differential (`tests/differential.rs`, `#[ignore]`-gated).**
- Per codec, both directions:
  - `jvm_decompress(c, rust_compress(c, x)?)? == x`
  - `rust_decompress(c, jvm_compress(c, x))? == x`
- Reuses the oracle wrapper at
  `crates/protocol/tests/support/oracle.rs`. Either factor it into a
  shared `crates/test-support` crate or duplicate the ~40-line wrapper;
  implementer's choice in the plan.

**Layer 4 — CodSpeed (`benches/codec.rs`).**
- Compress + decompress at 1 KiB / 64 KiB / 1 MiB input sizes per
  codec. Baseline numbers for future regression detection.

# 7. CI integration

- **`rust` matrix job** (Linux/macOS/Windows × 1.95.0) picks up the new
  crate transparently — `cargo test --workspace` covers it.
- **`jvm-differential` job** picks up the new differential tests
  transparently — already runs `cargo test --workspace -- --include-ignored`.
- **`drift` job** does not apply (no generated code).
- **`Run benchmarks` job** picks up new criterion benches automatically
  if added under `crates/compression/benches/`.
- **No new workflows needed.**

# 8. Acceptance criteria

The sub-plan ships when **all** of these hold:

1. `crates/compression/` exists; `cargo build -p crabka-compression --no-default-features`
   succeeds (verifies the crate compiles with all codecs off).
2. `cargo build -p crabka-compression` (default features) succeeds.
3. For each codec individually:
   `cargo build -p crabka-compression --no-default-features --features <codec>`
   succeeds.
4. Free-function API matches Section 3: `CompressionType` enum with
   `None`, `Gzip`, `Snappy`, `Lz4`, `Zstd`, `compress`/`decompress`
   returning `Result<Bytes, CompressionError>`, disabled codecs
   returning `Err(FeatureDisabled(_))`.
5. Unit tests pass: at least four per codec (round-trip, empty,
   malformed, JVM-frame sanity for snappy/lz4).
6. Proptest round-trips pass per codec at default budget.
7. JVM-differential tests pass per codec, both directions, on the
   proptest budget the foundation uses.
8. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
   `cargo test --workspace -- --include-ignored` all green.
9. CI matrix green on Linux/macOS/Windows.
10. CodSpeed bench file exists with per-codec compress + decompress
    benchmarks at one representative payload size.
11. Rustdoc lands on `CompressionType`, `CompressionError`, `compress`,
    `decompress`. Crate-level doc explains the Kafka framing quirks
    (snappy xerial, lz4 frame).

# 9. Open questions deferred to the implementation plan

- **Test-support sharing.** Whether to factor the JVM oracle wrapper
  into a `crates/test-support` crate or duplicate the ~40 lines for
  now. Recommend duplicate; revisit if a third crate needs it.
- **Snappy xerial framing edge cases.** First block size, end-of-stream
  marker behaviour, behaviour with malformed magic. The plan adds a
  parameterised test per case as the implementation surfaces details.
- **LZ4 KAFKA-3160 compatibility.** If the differential corpus turns
  out to include pre-0.10 LZ4 frames, we may need a tolerance flag.
  Defer until evidence appears.

None block this design.

# 10. Next step

Invoke `writing-plans` to produce a detailed implementation plan for
sub-plan 1b. Sub-plans 1c, 1d, 1e get their own brainstorm → plan
cycles when their turn comes.
