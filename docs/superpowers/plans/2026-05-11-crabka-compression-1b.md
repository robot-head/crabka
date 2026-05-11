# `crabka-compression` (sub-plan 1b) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `crabka-compression` crate covering gzip, snappy, lz4, zstd with byte-level wire compatibility verified against the JVM `kafka-clients` implementation.

**Architecture:** Free-function API (`compress` / `decompress`) parameterised on a `CompressionType` enum that matches Kafka's record-batch attribute bits. Each codec lives in its own module behind a Cargo feature; disabled features return `Err(CompressionError::FeatureDisabled(_))` at runtime so the type API stays stable across feature configurations. Differential testing against the JVM via two new oracle ops.

**Tech Stack:** Rust 1.95.0 (edition 2024); `flate2` (rust_backend), `snap`, `lz4_flex` (frame), `zstd`; `proptest` for property tests; existing JVM oracle in `tools/oracle/` extended with `compress`/`decompress` ops.

**Working directory:** `C:\Users\Matt Stone\git\crabka`, branch `plan/compression-1b` (then a feature branch for implementation). Implementation work begins after PR #16 (this plan + spec) is merged.

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-compression-1b-design.md`](../specs/2026-05-11-crabka-compression-1b-design.md).

---

## Phase A — Workspace scaffolding

### Task 1: Create the `crabka-compression` crate skeleton

**Files:**
- Create: `crates/compression/Cargo.toml`
- Create: `crates/compression/src/lib.rs`
- Modify: `Cargo.toml` (workspace) — add new deps to `[workspace.dependencies]`

- [ ] **Step 1: Add the new third-party deps to the workspace manifest**

In `Cargo.toml` at repo root, under `[workspace.dependencies]`, append:

```toml
flate2 = { version = "1", default-features = false, features = ["rust_backend"] }
snap = "1"
lz4_flex = { version = "0.11", default-features = false, features = ["frame"] }
zstd = "0.13"
```

Leave existing entries untouched.

- [ ] **Step 2: Create the crate manifest**

`crates/compression/Cargo.toml`:

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
flate2   = { workspace = true, optional = true }
snap     = { workspace = true, optional = true }
lz4_flex = { workspace = true, optional = true }
zstd     = { workspace = true, optional = true }

[dev-dependencies]
proptest = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
once_cell = { workspace = true }
```

- [ ] **Step 3: Stub `lib.rs`**

`crates/compression/src/lib.rs`:

```rust
//! Kafka wire-protocol compression codecs.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-compression-1b-design.md`.
//!
//! Kafka uses four codecs on the wire — gzip, snappy, lz4, zstd — each
//! with a specific framing convention. `crabka-compression` wraps the
//! third-party Rust crates for those codecs and adds the Kafka-specific
//! framing where needed (notably xerial-snappy for snappy and the LZ4
//! frame format with independent blocks for lz4).
#![doc(html_root_url = "https://docs.rs/crabka-compression/0.0.0")]
```

- [ ] **Step 4: Verify the workspace builds**

```bash
cd "/c/Users/Matt Stone/git/crabka"
cargo build -p crabka-compression
```

Expected: `Finished` with no errors. The crate is empty so this is purely a manifest check.

- [ ] **Step 5: Verify `--no-default-features` compiles**

```bash
cargo build -p crabka-compression --no-default-features
```

Expected: `Finished`. With no features, no optional deps pull in.

- [ ] **Step 6: Commit**

```bash
git add crates/compression Cargo.toml
git commit -m "feat(compression): add crate skeleton"
```

---

### Task 2: Define `CompressionType` and `CompressionError`

**Files:**
- Modify: `crates/compression/src/lib.rs`
- Create: `crates/compression/src/error.rs`
- Create: `crates/compression/src/codec_type.rs`

- [ ] **Step 1: Write `CompressionError`**

`crates/compression/src/error.rs`:

```rust
//! Errors returned by `compress` and `decompress`.

use thiserror::Error;

/// Errors that can occur during compression or decompression.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CompressionError {
    /// The requested codec was not enabled at compile time. The codec name
    /// is one of `"gzip"`, `"snappy"`, `"lz4"`, `"zstd"`.
    #[error("compression feature `{0}` not enabled at compile time")]
    FeatureDisabled(&'static str),

    /// The compressed payload could not be parsed (truncated input, bad
    /// framing, invalid checksum, etc.).
    #[error("invalid compressed data: {0}")]
    InvalidData(String),

    /// I/O error from one of the codec libraries.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_disabled_display() {
        let e = CompressionError::FeatureDisabled("snappy");
        assert_eq!(
            e.to_string(),
            "compression feature `snappy` not enabled at compile time"
        );
    }
}
```

- [ ] **Step 2: Write `CompressionType`**

`crates/compression/src/codec_type.rs`:

```rust
//! `CompressionType` enum mapping to Kafka's record-batch attribute bits.

/// Codec identifier matching the lowest three bits of Kafka's record-batch
/// attribute byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionType {
    /// Decode the lowest three bits of a Kafka record-batch attribute byte.
    /// Returns `None` for codec ids outside `0..=4`.
    #[must_use]
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
    #[must_use]
    pub fn as_attribute_bits(self) -> u8 { self as u8 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attribute_bits_roundtrip() {
        for ct in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            assert_eq!(CompressionType::from_attribute_bits(ct.as_attribute_bits()), Some(ct));
        }
    }

    #[test]
    fn attribute_bits_mask() {
        // Only the low 3 bits define the codec; upper bits are other flags.
        assert_eq!(
            CompressionType::from_attribute_bits(0b1111_1000 | 0b0000_0001),
            Some(CompressionType::Gzip)
        );
    }

    #[test]
    fn attribute_bits_unknown() {
        assert_eq!(CompressionType::from_attribute_bits(5), None);
        assert_eq!(CompressionType::from_attribute_bits(7), None);
    }
}
```

- [ ] **Step 3: Update `lib.rs` to expose both**

Replace `crates/compression/src/lib.rs` with:

```rust
//! Kafka wire-protocol compression codecs.

mod codec_type;
mod error;

pub use codec_type::CompressionType;
pub use error::CompressionError;
```

- [ ] **Step 4: Run the tests**

```bash
cargo test -p crabka-compression
```

Expected: 4 tests pass (`feature_disabled_display`, `attribute_bits_roundtrip`, `attribute_bits_mask`, `attribute_bits_unknown`).

- [ ] **Step 5: Commit**

```bash
git add crates/compression
git commit -m "feat(compression): CompressionType + CompressionError"
```

---

### Task 3: Public `compress` / `decompress` free functions with feature-disabled stubs

This task wires up the dispatch but leaves each codec returning `FeatureDisabled` until later tasks fill them in. After this commit, the public API surface is final and stable.

**Files:**
- Modify: `crates/compression/src/lib.rs`

- [ ] **Step 1: Write the dispatch functions**

Append to `crates/compression/src/lib.rs`:

```rust
use bytes::Bytes;

/// Compress `data` using the codec identified by `ct`.
///
/// For `CompressionType::None`, returns the input unchanged (wrapped in a
/// new `Bytes`). For other codecs, dispatches to the per-codec module.
/// If the codec's Cargo feature is not enabled, returns
/// `Err(CompressionError::FeatureDisabled(_))`.
pub fn compress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip => gzip_compress(data),
        CompressionType::Snappy => snappy_compress(data),
        CompressionType::Lz4 => lz4_compress(data),
        CompressionType::Zstd => zstd_compress(data),
    }
}

/// Decompress `data` using the codec identified by `ct`. See `compress`.
pub fn decompress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip => gzip_decompress(data),
        CompressionType::Snappy => snappy_decompress(data),
        CompressionType::Lz4 => lz4_decompress(data),
        CompressionType::Zstd => zstd_decompress(data),
    }
}

// --- per-codec dispatch, with feature-gated stubs ------------------------

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "gzip")]
use crate::gzip::{compress as gzip_compress, decompress as gzip_decompress};
#[cfg(not(feature = "gzip"))]
fn gzip_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}
#[cfg(not(feature = "gzip"))]
fn gzip_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}

#[cfg(feature = "snappy")]
mod snappy;
#[cfg(feature = "snappy")]
use crate::snappy::{compress as snappy_compress, decompress as snappy_decompress};
#[cfg(not(feature = "snappy"))]
fn snappy_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("snappy"))
}
#[cfg(not(feature = "snappy"))]
fn snappy_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("snappy"))
}

#[cfg(feature = "lz4")]
mod lz4;
#[cfg(feature = "lz4")]
use crate::lz4::{compress as lz4_compress, decompress as lz4_decompress};
#[cfg(not(feature = "lz4"))]
fn lz4_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}
#[cfg(not(feature = "lz4"))]
fn lz4_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}

#[cfg(feature = "zstd")]
mod zstd;
#[cfg(feature = "zstd")]
use crate::zstd::{compress as zstd_compress, decompress as zstd_decompress};
#[cfg(not(feature = "zstd"))]
fn zstd_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}
#[cfg(not(feature = "zstd"))]
fn zstd_decompress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}
```

- [ ] **Step 2: Add empty per-codec modules**

For each of the four codec names, create a stub module file. These will be filled in in Phase B; the stub allows the crate to compile with each feature enabled individually.

`crates/compression/src/gzip.rs`:

```rust
//! Gzip (RFC-1952). Filled in by sub-plan 1b Task 4.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("gzip not yet implemented".into()))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("gzip not yet implemented".into()))
}
```

`crates/compression/src/snappy.rs`:

```rust
//! Xerial-snappy framing over snap raw blocks. Filled in by Task 5.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("snappy not yet implemented".into()))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("snappy not yet implemented".into()))
}
```

`crates/compression/src/lz4.rs`:

```rust
//! LZ4 frame format (independent blocks). Filled in by Task 6.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("lz4 not yet implemented".into()))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("lz4 not yet implemented".into()))
}
```

`crates/compression/src/zstd.rs`:

```rust
//! Zstd. Filled in by Task 7.

use bytes::Bytes;

use crate::CompressionError;

pub fn compress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("zstd not yet implemented".into()))
}

pub fn decompress(_data: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::InvalidData("zstd not yet implemented".into()))
}
```

- [ ] **Step 3: Verify all build configurations**

```bash
cargo build -p crabka-compression                                      # default (all four)
cargo build -p crabka-compression --no-default-features                # zero codecs
cargo build -p crabka-compression --no-default-features --features gzip   # one codec
cargo build -p crabka-compression --no-default-features --features snappy
cargo build -p crabka-compression --no-default-features --features lz4
cargo build -p crabka-compression --no-default-features --features zstd
```

Expected: each command exits 0.

- [ ] **Step 4: Add a feature-disabled smoke test**

Append to `crates/compression/src/lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_none_compress() {
        let out = compress(CompressionType::None, b"abcdef").unwrap();
        assert_eq!(out.as_ref(), b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress() {
        let out = decompress(CompressionType::None, b"abcdef").unwrap();
        assert_eq!(out.as_ref(), b"abcdef");
    }
}
```

Run: `cargo test -p crabka-compression` — expect all tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/compression
git commit -m "feat(compression): public compress/decompress API with feature dispatch"
```

---

## Phase B — Per-codec implementations

### Task 4: Implement gzip

The easiest codec. No Kafka-specific framing, just RFC-1952 gzip via `flate2`.

**Files:**
- Modify: `crates/compression/src/gzip.rs`

- [ ] **Step 1: Write the failing unit tests**

Replace `crates/compression/src/gzip.rs` with the test module and a stub implementation:

```rust
//! Gzip (RFC-1952), via `flate2` with the pure-Rust `miniz_oxide` backend.

use std::io::{Read, Write};

use bytes::Bytes;
use flate2::Compression as GzipLevel;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

use crate::CompressionError;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = GzEncoder::new(Vec::with_capacity(data.len()), GzipLevel::default());
    encoder.write_all(data)?;
    let out = encoder.finish()?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty gzip payload".into()));
    }
    let mut decoder = GzDecoder::new(data);
    let mut out = Vec::with_capacity(data.len() * 2);
    decoder.read_to_end(&mut out).map_err(|e| {
        CompressionError::InvalidData(format!("gzip decode: {e}"))
    })?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        assert!(z.len() < HELLO.len() + 32, "z={:?}", z.len());
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert!(matches!(decompress(b""), Err(CompressionError::InvalidData(_))));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert!(matches!(
            decompress(b"this is not gzip"),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn compress_empty_produces_valid_frame() {
        let z = compress(b"").unwrap();
        assert!(!z.is_empty(), "empty input still requires a gzip header");
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), b"");
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cargo test -p crabka-compression gzip
```

Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/compression
git commit -m "feat(compression): gzip codec via flate2 rust_backend"
```

---

### Task 5: Implement snappy with xerial framing

The trickiest codec. Kafka uses xerial-snappy framing: a fixed magic header, two version 4-byte BE integers, then a sequence of (UINT32_BE length, raw-snappy-block) chunks. The `snap` crate gives us raw block encode/decode; we provide the framing.

**Files:**
- Modify: `crates/compression/src/snappy.rs`

- [ ] **Step 1: Implement encode + decode**

Replace `crates/compression/src/snappy.rs`:

```rust
//! Xerial-snappy framing over `snap` raw blocks.
//!
//! Kafka does not use Google's official Snappy stream format. It uses
//! "xerial-snappy", a Java-library convention with this layout:
//!
//! ```text
//! [\x82 'S' 'N' 'A' 'P' 'P' 'Y' \x00]                 # 8-byte magic
//! [\x00 \x00 \x00 \x01]                               # version       (BE u32)
//! [\x00 \x00 \x00 \x01]                               # minCompatibleVersion (BE u32)
//! ( [BE u32 chunk length] [raw snappy block ...] )*   # zero or more chunks
//! ```
//!
//! There is no end-of-stream marker; chunks run until EOF.

use bytes::{BufMut, Bytes, BytesMut};

use crate::CompressionError;

/// Xerial framing header. 16 bytes total.
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0x00,
    0x00, 0x00, 0x00, 0x01, // version = 1
    0x00, 0x00, 0x00, 0x01, // minCompatibleVersion = 1
];

/// Largest single chunk Kafka writes. Kafka's `SnappyOutputStream` writes
/// chunks up to 32 KiB by default; using the same size keeps our output
/// byte-identical with the JVM for differential-equal cases.
const XERIAL_CHUNK_SIZE: usize = 32 * 1024;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut out = BytesMut::with_capacity(XERIAL_HEADER.len() + data.len());
    out.put_slice(&XERIAL_HEADER);

    let mut encoder = snap::raw::Encoder::new();
    for chunk in data.chunks(XERIAL_CHUNK_SIZE) {
        let max = snap::raw::max_compress_len(chunk.len());
        let mut buf = vec![0u8; max];
        let n = encoder
            .compress(chunk, &mut buf)
            .map_err(|e| CompressionError::InvalidData(format!("snappy encode: {e}")))?;
        out.put_u32(u32::try_from(n).expect("chunk size fits u32"));
        out.put_slice(&buf[..n]);
    }
    Ok(out.freeze())
}

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.len() < XERIAL_HEADER.len() {
        return Err(CompressionError::InvalidData(
            "snappy payload too short for xerial header".into(),
        ));
    }
    if &data[..8] != &XERIAL_HEADER[..8] {
        return Err(CompressionError::InvalidData(
            "snappy missing xerial magic".into(),
        ));
    }
    // Ignore version fields (bytes 8..16); Kafka never bumped them.
    let mut rest = &data[XERIAL_HEADER.len()..];

    let mut out = BytesMut::with_capacity(data.len() * 2);
    let mut decoder = snap::raw::Decoder::new();
    while !rest.is_empty() {
        if rest.len() < 4 {
            return Err(CompressionError::InvalidData(
                "snappy chunk header truncated".into(),
            ));
        }
        let len = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        rest = &rest[4..];
        if rest.len() < len {
            return Err(CompressionError::InvalidData(
                "snappy chunk body truncated".into(),
            ));
        }
        let (block, tail) = rest.split_at(len);
        rest = tail;

        let max_out = snap::raw::decompress_len(block)
            .map_err(|e| CompressionError::InvalidData(format!("snappy decode_len: {e}")))?;
        let mut buf = vec![0u8; max_out];
        let n = decoder
            .decompress(block, &mut buf)
            .map_err(|e| CompressionError::InvalidData(format!("snappy decode: {e}")))?;
        out.put_slice(&buf[..n]);
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    /// Hand-captured xerial-snappy frame produced by the JVM oracle for the
    /// payload b"hello kafka, this is a moderately repetitive payload to compress".
    /// Recorded once during Task 5's TDD sanity step. Replace if the JVM
    /// output ever changes (e.g., due to library upgrades) — see plan §5.
    const JVM_HELLO_HEX: &str = "<placeholder; capture during step 4>";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), HELLO);
    }

    #[test]
    fn decompress_truncated_header() {
        assert!(matches!(
            decompress(&XERIAL_HEADER[..4]),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_missing_magic() {
        let bytes = [0u8; 20];
        assert!(matches!(
            decompress(&bytes),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_truncated_chunk() {
        let mut bytes = XERIAL_HEADER.to_vec();
        bytes.extend_from_slice(&[0, 0, 0, 100]); // claim 100-byte chunk
        bytes.push(0); // only 1 byte present
        assert!(matches!(
            decompress(&bytes),
            Err(CompressionError::InvalidData(_))
        ));
    }
}
```

- [ ] **Step 2: Capture the JVM xerial-snappy reference frame**

The test `decompresses_jvm_frame` (added next step) needs a known-good xerial frame. Build the oracle (if not already built) and emit one. From the repo root with `JAVA_HOME` set:

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
(cd tools/oracle && ./gradlew installDist -q --no-daemon)
```

The oracle does not yet have a `compress` op (Task 9 adds it), so for **this** task we use a one-off helper. Write a tiny Java program at `tools/oracle/snapshot.java` (not committed) that prints the xerial-snappy encoding of the hello payload. Alternative: invoke `kafka-clients` directly via a one-shot `gradle run` task. **Easiest:** capture the frame by running the test we're about to write *after* implementing the encoder, comparing against the eventual JVM oracle in Task 10's differential layer instead of hard-coding a hex constant here.

Skip step 2 for now. Replace the `JVM_HELLO_HEX` constant and the unused `decompresses_jvm_frame` test plan with a `// JVM reference frame will be validated by differential tests in Task 10.` comment. The four self-tests above are enough to ship Task 5.

- [ ] **Step 3: Add the JVM-frame sanity test as deferred work**

Remove the `JVM_HELLO_HEX` constant entirely; that line was placeholder. Replace with a doc comment in the module:

```rust
// JVM xerial-snappy byte equality is verified by the differential test
// suite (Task 10) once the oracle gains a `compress` op (Task 9).
```

Keep the four tests already written.

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-compression snappy
```

Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/compression
git commit -m "feat(compression): snappy with xerial framing over snap raw blocks"
```

---

### Task 6: Implement lz4 with frame format

LZ4 frame format with independent blocks (each decompressable in isolation), block-checksum disabled, content-size omitted (matches what `kafka-clients` writes by default in 4.x).

**Files:**
- Modify: `crates/compression/src/lz4.rs`

- [ ] **Step 1: Implement encode + decode using `lz4_flex::frame`**

Replace `crates/compression/src/lz4.rs`:

```rust
//! LZ4 frame format (LZ4F), independent blocks.
//!
//! Kafka writes LZ4 in the frame format (magic `0x04 22 4D 18`) with these
//! choices: 64 KiB block size, independent blocks, no block checksum, no
//! content-size in the header. We match those defaults so produced bytes
//! line up with `KafkaLZ4BlockOutputStream`'s output for differential
//! testing.

use std::io::{Read, Write};

use bytes::Bytes;
use lz4_flex::frame::{BlockMode, BlockSize, FrameDecoder, FrameEncoder, FrameInfo};

use crate::CompressionError;

fn frame_info() -> FrameInfo {
    FrameInfo::new()
        .block_size(BlockSize::Max64KB)
        .block_mode(BlockMode::Independent)
        .block_checksums(false)
        .content_checksum(false)
}

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let mut encoder = FrameEncoder::with_frame_info(frame_info(), Vec::with_capacity(data.len()));
    encoder.write_all(data)?;
    let out = encoder
        .finish()
        .map_err(|e| CompressionError::InvalidData(format!("lz4 finish: {e}")))?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty lz4 payload".into()));
    }
    let mut decoder = FrameDecoder::new(data);
    let mut out = Vec::with_capacity(data.len() * 2);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("lz4 decode: {e}")))?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert!(matches!(decompress(b""), Err(CompressionError::InvalidData(_))));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert!(matches!(
            decompress(b"this is not lz4"),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024]; // 128 KiB → multiple 64 KiB blocks
        let z = compress(&big).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), big.as_slice());
    }
}
```

- [ ] **Step 2: Verify the `lz4_flex::frame::FrameInfo` builder API**

The `lz4_flex` 0.11 frame API may have slightly different method names than shown above. Before running tests, run `cargo doc -p lz4_flex --no-deps --open` (or read the docs at `docs.rs/lz4_flex/0.11.*`) and confirm:
- `BlockSize::Max64KB` is the variant name (might be `BlockSize::Max64Kb` — match case)
- `BlockMode::Independent` exists (vs `BlockMode::Linked`)
- `FrameInfo::new()` returns a builder; methods are chainable

If any of these differ, adjust the calls in the implementation to match what compiles. Do NOT change the chosen behaviour: independent blocks, 64 KiB blocks, no checksums.

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-compression lz4
```

Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/compression
git commit -m "feat(compression): lz4 frame format with independent blocks"
```

---

### Task 7: Implement zstd

Plain zstd, no framing layer. Library default level (3) matches Kafka.

**Files:**
- Modify: `crates/compression/src/zstd.rs`

- [ ] **Step 1: Implement**

Replace `crates/compression/src/zstd.rs`:

```rust
//! Zstd via the `zstd` crate (wraps libzstd).

use bytes::Bytes;

use crate::CompressionError;

/// Match Kafka's default zstd level.
const DEFAULT_LEVEL: i32 = 3;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    let out = zstd::bulk::compress(data, DEFAULT_LEVEL)?;
    Ok(Bytes::from(out))
}

pub fn decompress(data: &[u8]) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty zstd payload".into()));
    }
    // The decompressor needs an upper bound on the output size. We don't
    // know it ahead of time, so use `bulk::Decompressor` and feed the
    // input through a growable buffer.
    let mut decoder = zstd::stream::Decoder::new(data)
        .map_err(|e| CompressionError::InvalidData(format!("zstd open: {e}")))?;
    let mut out = Vec::with_capacity(data.len() * 4);
    std::io::Read::read_to_end(&mut decoder, &mut out)
        .map_err(|e| CompressionError::InvalidData(format!("zstd decode: {e}")))?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert!(matches!(decompress(b""), Err(CompressionError::InvalidData(_))));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert!(matches!(
            decompress(b"this is not zstd"),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn larger_payload_roundtrips() {
        let big = vec![0xABu8; 128 * 1024];
        let z = compress(&big).unwrap();
        let back = decompress(&z).unwrap();
        assert_eq!(back.as_ref(), big.as_slice());
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cargo test -p crabka-compression zstd
```

Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/compression
git commit -m "feat(compression): zstd via libzstd"
```

---

## Phase C — Proptest round-trips

### Task 8: Per-codec proptest round-trip suite

**Files:**
- Create: `crates/compression/tests/proptest.rs`

- [ ] **Step 1: Write the property tests**

`crates/compression/tests/proptest.rs`:

```rust
use crabka_compression::{compress, decompress, CompressionType};
use proptest::prelude::*;

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    // Sizes 0..=32 KiB. Mix of all-zeros (highly compressible) and random
    // (worst case) via prop_oneof.
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..=32 * 1024),
        proptest::collection::vec(0u8..=0u8, 0..=32 * 1024),
    ]
}

macro_rules! roundtrip_for {
    ($name:ident, $ct:expr) => {
        proptest! {
            #[test]
            fn $name(data in arb_payload()) {
                let z = compress($ct, &data).unwrap();
                let back = decompress($ct, &z).unwrap();
                prop_assert_eq!(back.as_ref(), data.as_slice());
            }
        }
    };
}

roundtrip_for!(none_roundtrip,   CompressionType::None);
roundtrip_for!(gzip_roundtrip,   CompressionType::Gzip);
roundtrip_for!(snappy_roundtrip, CompressionType::Snappy);
roundtrip_for!(lz4_roundtrip,    CompressionType::Lz4);
roundtrip_for!(zstd_roundtrip,   CompressionType::Zstd);
```

- [ ] **Step 2: Run**

```bash
cargo test -p crabka-compression --test proptest
```

Expected: 5 proptest cases pass (256 default iterations each = 1,280 total cases).

If a codec fails on a specific input, proptest prints the seed and the minimised counterexample. Fix the codec; do not skip the case.

- [ ] **Step 3: Commit**

```bash
git add crates/compression
git commit -m "test(compression): per-codec proptest round-trip suite"
```

---

## Phase D — JVM oracle differential

### Task 9: Extend the JVM oracle with `compress` / `decompress` ops

**Files:**
- Modify: `tools/oracle/src/main/java/com/crabka/oracle/Oracle.java`

- [ ] **Step 1: Read the existing Oracle.java to see the dispatch pattern**

```bash
cat tools/oracle/src/main/java/com/crabka/oracle/Oracle.java
```

The file dispatches on `req.get("op")`. We're adding two new ops alongside the existing message-level ones.

- [ ] **Step 2: Add the new ops**

Add to the `handle` method's op dispatch (or equivalent):

```java
case "compress":
case "decompress": {
    String codec = req.get("codec").asText();
    byte[] input = HexFormat.of().parseHex(req.get("data").asText());
    byte[] result;
    if (op.equals("compress")) {
        result = compressBytes(codec, input);
    } else {
        result = decompressBytes(codec, input);
    }
    ObjectNode resp = M.createObjectNode();
    resp.put("ok", true);
    resp.put("hex", HexFormat.of().formatHex(result));
    return resp;
}
```

Add helper methods. Kafka's compression entry points are in
`org.apache.kafka.common.compress.*`:

```java
private static byte[] compressBytes(String codec, byte[] input) throws Exception {
    switch (codec) {
        case "gzip": {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            try (java.io.OutputStream s = new java.util.zip.GZIPOutputStream(out)) {
                s.write(input);
            }
            return out.toByteArray();
        }
        case "snappy": {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            try (java.io.OutputStream s = new org.xerial.snappy.SnappyOutputStream(out)) {
                s.write(input);
            }
            return out.toByteArray();
        }
        case "lz4": {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            try (java.io.OutputStream s =
                    new org.apache.kafka.common.compress.KafkaLZ4BlockOutputStream(out, true)) {
                s.write(input);
            }
            return out.toByteArray();
        }
        case "zstd": {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            try (java.io.OutputStream s = new com.github.luben.zstd.ZstdOutputStream(out)) {
                s.write(input);
            }
            return out.toByteArray();
        }
        default:
            throw new IllegalArgumentException("unknown codec: " + codec);
    }
}

private static byte[] decompressBytes(String codec, byte[] input) throws Exception {
    java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(input);
    java.io.InputStream s;
    switch (codec) {
        case "gzip":   s = new java.util.zip.GZIPInputStream(in); break;
        case "snappy": s = new org.xerial.snappy.SnappyInputStream(in); break;
        case "lz4":    s = new org.apache.kafka.common.compress.KafkaLZ4BlockInputStream(in, true); break;
        case "zstd":   s = new com.github.luben.zstd.ZstdInputStream(in); break;
        default: throw new IllegalArgumentException("unknown codec: " + codec);
    }
    try (s) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = s.read(buf)) >= 0) out.write(buf, 0, n);
        return out.toByteArray();
    }
}
```

**Notes on the class names:** the Kafka API surface may name LZ4 helpers differently across versions (`KafkaLZ4BlockOutputStream` vs `Lz4BlockOutputStream`). Before committing, build and run a quick smoke test (Step 3). If a class isn't found, search the `kafka-clients` jar for the right name:

```bash
unzip -l tools/oracle/build/install/crabka-oracle/lib/kafka-clients-*.jar \
    | grep -i 'lz4\|snappy\|zstd\|gzip' | head -20
```

- [ ] **Step 3: Rebuild the oracle and smoke-test**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.19.10-hotspot"
(cd tools/oracle && ./gradlew installDist -q --no-daemon)

echo '{"op":"compress","codec":"gzip","data":"68656c6c6f"}' \
    | tools/oracle/build/install/crabka-oracle/bin/crabka-oracle.bat \
    | head -1
```

Expected: a JSON line with `"ok": true` and a `"hex"` field that is the gzip encoding of `hello`. Smoke-test all four codecs similarly.

- [ ] **Step 4: Commit**

```bash
git add tools/oracle
git commit -m "feat(oracle): compress/decompress ops for differential testing"
```

---

### Task 10: Differential tests for all four codecs

**Files:**
- Create: `crates/compression/tests/support/mod.rs`
- Create: `crates/compression/tests/support/oracle.rs`
- Create: `crates/compression/tests/differential.rs`

- [ ] **Step 1: Duplicate the oracle wrapper into the compression crate**

The wrapper at `crates/protocol/tests/support/oracle.rs` is ~80 lines. The cleanest move within sub-plan 1b is to copy it as-is into `crates/compression/tests/support/oracle.rs` and add two new methods on `Oracle`:

```rust
pub fn compress(&mut self, codec: &str, data: &[u8]) -> Vec<u8> {
    let r = self.call(&json!({
        "op": "compress",
        "codec": codec,
        "data": hex::encode(data),
    }));
    hex::decode(r["hex"].as_str().unwrap()).unwrap()
}

pub fn decompress(&mut self, codec: &str, data: &[u8]) -> Vec<u8> {
    let r = self.call(&json!({
        "op": "decompress",
        "codec": codec,
        "data": hex::encode(data),
    }));
    hex::decode(r["hex"].as_str().unwrap()).unwrap()
}
```

Carrying the duplication: deliberate, per the spec's deferred question. If a third crate later needs the wrapper, factor out then.

`crates/compression/tests/support/mod.rs`:

```rust
pub mod oracle;
```

- [ ] **Step 2: Write the differential test file**

`crates/compression/tests/differential.rs`:

```rust
mod support;
use support::oracle;

use crabka_compression::{compress, decompress, CompressionType};
use proptest::prelude::*;

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        proptest::collection::vec(any::<u8>(), 0..=8 * 1024),
        proptest::collection::vec(0u8..=0u8, 0..=8 * 1024),
    ]
}

macro_rules! diff_test {
    ($name:ident, $codec_str:literal, $ct:expr) => {
        #[test]
        #[ignore = "requires JVM oracle"]
        fn $name() {
            let mut o = oracle::shared();
            proptest!(|(data in arb_payload())| {
                // Rust compresses; JVM decompresses; bytes match input.
                let rust_z = compress($ct, &data).unwrap();
                let jvm_back = o.decompress($codec_str, &rust_z);
                prop_assert_eq!(jvm_back, data.clone());

                // JVM compresses; Rust decompresses; bytes match input.
                let jvm_z = o.compress($codec_str, &data);
                let rust_back = decompress($ct, &jvm_z).unwrap();
                prop_assert_eq!(rust_back.as_ref(), data.as_slice());
            });
        }
    };
}

diff_test!(gzip_differential,   "gzip",   CompressionType::Gzip);
diff_test!(snappy_differential, "snappy", CompressionType::Snappy);
diff_test!(lz4_differential,    "lz4",    CompressionType::Lz4);
diff_test!(zstd_differential,   "zstd",   CompressionType::Zstd);
```

- [ ] **Step 3: Run**

```bash
cargo test -p crabka-compression --test differential -- --ignored
```

Expected: 4 tests pass. If any fails, the failure message includes the proptest seed. Fix the codec in `crabka-compression`; do NOT change the test to accept divergence.

Common debug paths:
- **Snappy mismatch:** xerial framing wrong (magic, version bytes, chunk-length endianness).
- **LZ4 mismatch:** wrong block size, wrong block-mode, content/block checksums enabled when they shouldn't be.
- **Zstd mismatch:** wrong level (Kafka writes at 3; verify our `DEFAULT_LEVEL`).

- [ ] **Step 4: Commit**

```bash
git add crates/compression
git commit -m "test(compression): differential round-trips vs JVM oracle"
```

---

## Phase E — Benchmarks and acceptance

### Task 11: CodSpeed benches

The existing benchmarks workflow at `.github/workflows/` runs CodSpeed criterion benches. Adding a bench file under `crates/compression/benches/` is picked up automatically.

**Files:**
- Modify: `crates/compression/Cargo.toml` — add criterion dev-dep + bench target
- Create: `crates/compression/benches/codec.rs`

- [ ] **Step 1: Add criterion to dev-deps and declare the bench**

Append to `crates/compression/Cargo.toml`:

```toml
[dev-dependencies]
# ... existing entries ...
criterion = { version = "0.5", features = ["html_reports"] }
codspeed-criterion-compat = "4"

[[bench]]
name = "codec"
harness = false
```

The exact `criterion` / `codspeed-criterion-compat` versions should match what `crates/protocol/Cargo.toml` already uses — check there first and copy the same versions for consistency.

- [ ] **Step 2: Write the bench**

`crates/compression/benches/codec.rs`:

```rust
use bytes::Bytes;
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};

use crabka_compression::{compress, decompress, CompressionType};

fn payload(size: usize) -> Bytes {
    // A mildly compressible payload: alternating runs of two bytes.
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push(if (i / 32) % 2 == 0 { 0xAB } else { 0xCD });
    }
    Bytes::from(v)
}

fn bench_codec(c: &mut Criterion, name: &str, ct: CompressionType) {
    let mut group = c.benchmark_group(name);
    for &size in &[1024usize, 64 * 1024, 1024 * 1024] {
        let data = payload(size);
        let compressed = compress(ct, &data).unwrap();

        group.bench_function(format!("compress_{size}"), |b| {
            b.iter(|| compress(ct, black_box(&data)).unwrap());
        });
        group.bench_function(format!("decompress_{size}"), |b| {
            b.iter(|| decompress(ct, black_box(&compressed)).unwrap());
        });
    }
    group.finish();
}

fn bench_gzip(c: &mut Criterion)   { bench_codec(c, "gzip",   CompressionType::Gzip); }
fn bench_snappy(c: &mut Criterion) { bench_codec(c, "snappy", CompressionType::Snappy); }
fn bench_lz4(c: &mut Criterion)    { bench_codec(c, "lz4",    CompressionType::Lz4); }
fn bench_zstd(c: &mut Criterion)   { bench_codec(c, "zstd",   CompressionType::Zstd); }

criterion_group!(benches, bench_gzip, bench_snappy, bench_lz4, bench_zstd);
criterion_main!(benches);
```

- [ ] **Step 3: Verify the bench compiles and runs at least one iteration**

```bash
cargo bench -p crabka-compression -- --quick
```

Expected: each codec runs through the three sizes; CodSpeed compatibility layer doesn't crash.

- [ ] **Step 4: Commit**

```bash
git add crates/compression
git commit -m "bench(compression): per-codec criterion benches at 1KiB/64KiB/1MiB"
```

---

### Task 12: Acceptance gate

Verification only. Mark complete only when every item passes.

- [x] `cargo fmt --check` clean.
- [x] `cargo clippy --workspace --all-targets -- -D warnings` clean.
- [x] `cargo build -p crabka-compression --no-default-features` succeeds.
- [x] `cargo build -p crabka-compression --no-default-features --features gzip` succeeds.
- [x] `cargo build -p crabka-compression --no-default-features --features snappy` succeeds.
- [x] `cargo build -p crabka-compression --no-default-features --features lz4` succeeds.
- [x] `cargo build -p crabka-compression --no-default-features --features zstd` succeeds.
- [x] `cargo build -p crabka-compression` (default features) succeeds.
- [x] `cargo test --workspace` clean (all non-ignored tests pass).
- [x] `cargo test --workspace -- --include-ignored` clean (JVM oracle in use; 4 differential tests pass).
- [x] CodSpeed bench file exists and `cargo bench -p crabka-compression -- --quick` runs without crashing.
- [x] Public API matches the spec exactly: `CompressionType` with 5 variants, `compress` and `decompress` returning `Result<Bytes, CompressionError>`, disabled-codec calls return `Err(FeatureDisabled(_))`.
- [x] Rustdoc on `CompressionType`, `CompressionError`, `compress`, `decompress`. Crate-level doc mentions xerial-snappy framing and LZ4 frame format with independent blocks.

When all items pass, the sub-plan is done. Push the feature branch and open a PR to main.

```bash
git push -u origin feature/compression-1b
gh pr create --base main --head feature/compression-1b \
    --title "Sub-plan 1b: crabka-compression crate" \
    --body "Implements gzip, snappy, lz4, zstd with JVM-verified wire compatibility. See spec docs/superpowers/specs/2026-05-11-crabka-compression-1b-design.md."
```

---

## Self-review against the spec

**Spec coverage:**

| Spec requirement | Plan coverage |
|---|---|
| `CompressionType` enum with `None`/`Gzip`/`Snappy`/`Lz4`/`Zstd` matching attribute bits 0-4 | Task 2 |
| `CompressionError` with `FeatureDisabled` / `InvalidData` / `Io` | Task 2 |
| `compress` / `decompress` free functions | Task 3 |
| Per-codec features (default-enabled), runtime `FeatureDisabled` for off codecs | Tasks 1, 3 |
| Gzip via `flate2` rust_backend | Task 4 |
| Snappy with xerial framing via `snap` raw blocks | Task 5 |
| LZ4 frame format with 64 KiB independent blocks via `lz4_flex` | Task 6 |
| Zstd via `zstd` crate at default level (3) | Task 7 |
| Per-codec proptest round-trip suite | Task 8 |
| JVM oracle extension (`compress`/`decompress` ops) | Task 9 |
| Differential tests both directions, all four codecs | Task 10 |
| CodSpeed bench file at 1 KiB / 64 KiB / 1 MiB per codec | Task 11 |
| All acceptance criteria from §8 of spec | Task 12 |
| Each codec passes 4+ unit tests (round-trip, empty, malformed, frame-sanity) | Tasks 4-7 (each contributes 4 tests) |

**Placeholder scan:** No `TODO` / `TBD` in requirements. The note in Task 5 Step 2 about deferring the JVM xerial reference frame to Task 10 is an explicit deferral with a tracking task, not a hidden gap. Task 9 Step 2 calls out a real "verify the class names" investigation step but the surrounding context tells the implementer exactly how to investigate.

**Type consistency:** Function signatures and module names are stable across tasks:
- `compress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError>` everywhere
- Per-codec module functions are `pub fn compress(data: &[u8])` / `pub fn decompress(data: &[u8])`
- `Oracle::compress(&mut self, codec: &str, data: &[u8])` / `decompress(...)` consistent in Task 10

The plan is ready for execution.

---

## Execution

After PR #16 (spec + this plan) is merged to main, the implementation runs on a fresh `feature/compression-1b` branch with the same subagent-driven-development workflow used for 1a. Twelve tasks; expect roughly 60-90 minutes of orchestration plus subagent wall time.
