# crabka-protocol Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `crabka-protocol` crate with primitives, traits, schema-driven codegen for two API flavors (owned and borrowed), and three test layers (unit / proptest / JVM-differential), proven end-to-end on the `ApiVersions` request and response.

**Architecture:** New Rust workspace `crabka/` with a single crate `crabka-protocol`. The crate provides Kafka primitive encoders/decoders, two `Encode`/`Decode` traits, a code generator (separate `bin`) that consumes upstream Kafka JSON message schemas, and a test infrastructure built around a long-lived JVM subprocess oracle that uses `org.apache.kafka:kafka-clients` as the ground truth.

**Tech Stack:** Rust (stable), `bytes`, `thiserror`, `proptest`, `arbitrary`, `serde_json`, `tempfile`. Java 17 + Gradle wrapper for the JVM oracle. CI: GitHub Actions on Linux, macOS, Windows.

**Scope of this plan:** Foundation + complete vertical slice through `ApiVersionsRequest` (v0 non-flexible + v3 flexible) and `ApiVersionsResponse`. Extension to the remaining ~99 message types is the subject of a follow-up plan, `crabka-protocol-coverage`.

**Working directory:** Implementation happens in a new sibling repository (e.g., `~/git/crabka/`), NOT inside this Apache Kafka clone. File paths in this plan are relative to that new repo's root.

**Reference spec:** [`docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`](../specs/2026-05-10-crabka-rust-rewrite-design.md) in this repo.

---

## Phase 0 — Bootstrap

### Task 1: Create the Crabka repository and workspace skeleton

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.gitignore`
- Create: `LICENSE`
- Create: `NOTICE`
- Create: `README.md`
- Create: `crates/.gitkeep`

- [ ] **Step 1: Create a new empty git repo for Crabka outside the Kafka clone**

```bash
mkdir -p ~/git/crabka
cd ~/git/crabka
git init -b main
```

Expected: empty repository on branch `main`.

- [ ] **Step 2: Write the workspace `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
version = "0.0.0"
edition = "2021"
license = "Apache-2.0"
repository = "https://github.com/<org>/crabka"
authors = ["The Crabka Authors"]

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
pedantic = { level = "warn", priority = -1 }
module_name_repetitions = "allow"
missing_errors_doc = "allow"
missing_panics_doc = "allow"

[workspace.dependencies]
bytes = "1"
thiserror = "1"
proptest = "1"
arbitrary = { version = "1", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tempfile = "3"
hex = "0.4"
toml = "0.8"
```

- [ ] **Step 3: Pin the Rust toolchain**

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.82.0"
components = ["rustfmt", "clippy"]
profile = "minimal"
```

This sets the MSRV. Bump only via PR with rationale.

- [ ] **Step 4: Write `.gitignore`**

```
target/
**/*.rs.bk
.idea/
.vscode/
*.iml
.DS_Store
tools/oracle/.gradle/
tools/oracle/build/
```

- [ ] **Step 5: Write LICENSE (Apache 2.0)**

Copy the canonical Apache License 2.0 text into `LICENSE`. Source: https://www.apache.org/licenses/LICENSE-2.0.txt

- [ ] **Step 6: Write NOTICE**

```
Crabka
Copyright 2026 The Crabka Authors

This product includes software developed at
The Apache Software Foundation (https://www.apache.org/).

Apache Kafka
Copyright 2010-2026 The Apache Software Foundation

This product includes wire-protocol message schemas vendored from
the Apache Kafka project (https://github.com/apache/kafka) under
the Apache License 2.0.
```

- [ ] **Step 7: Write a minimal README**

`README.md`:

```markdown
# Crabka

A Rust reimplementation of [Apache Kafka](https://kafka.apache.org), distributed under the
Apache License 2.0 as a derivative work.

This repository hosts the [`crabka-protocol`](crates/protocol) crate. Other components
(broker, clients, KRaft, etc.) will arrive in their own crates over time. See the design
spec for the full roadmap.

## Status

Pre-1.0, pre-alpha. No production use.

## License

Apache 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
```

- [ ] **Step 8: Verify the workspace compiles (it should, with zero members)**

```bash
touch crates/.gitkeep
cargo check --workspace
```

Expected: `Finished` with no errors. `cargo check` against an empty workspace is a no-op but validates the manifest.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "chore: bootstrap workspace skeleton"
```

---

### Task 2: Create the `crabka-protocol` crate skeleton

**Files:**
- Create: `crates/protocol/Cargo.toml`
- Create: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Write the crate manifest**

`crates/protocol/Cargo.toml`:

```toml
[package]
name = "crabka-protocol"
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
authors.workspace = true
description = "Kafka wire protocol codec for Rust"

[lints]
workspace = true

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
arbitrary = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tempfile = { workspace = true }
toml = { workspace = true }
```

- [ ] **Step 2: Write the initial `lib.rs`**

`crates/protocol/src/lib.rs`:

```rust
//! Kafka wire protocol codec.
//!
//! See the design document at
//! `docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`
//! in this repo for the project rationale.
#![doc(html_root_url = "https://docs.rs/crabka-protocol/0.0.0")]
```

- [ ] **Step 3: Verify the crate builds**

```bash
cargo build -p crabka-protocol
```

Expected: `Compiling crabka-protocol v0.0.0` then `Finished`.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add crate skeleton"
```

---

## Phase 1 — Errors and traits

### Task 3: Define `ProtocolError`

**Files:**
- Create: `crates/protocol/src/error.rs`
- Modify: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Write the failing test**

`crates/protocol/src/error.rs`:

```rust
use thiserror::Error;

/// Errors that can occur during wire-protocol encoding or decoding.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// Decode reached end of buffer before the expected number of bytes.
    #[error("unexpected end of buffer: needed {needed} more bytes")]
    UnexpectedEof { needed: usize },

    /// Decoded a value that the schema says is impossible (e.g. negative array length).
    #[error("invalid value: {0}")]
    InvalidValue(&'static str),

    /// Decoded UTF-8 bytes that are not valid UTF-8.
    #[error("invalid UTF-8 in string field")]
    InvalidUtf8(#[source] std::str::Utf8Error),

    /// Decoded a varint that exceeds the maximum legal length.
    #[error("varint exceeds {max} bytes")]
    VarintTooLong { max: usize },

    /// Encountered an unknown API version for a known API key.
    #[error("unsupported API version {version} for api key {api_key}")]
    UnsupportedVersion { api_key: i16, version: i16 },

    /// Schema version requested is not within the message's supported range.
    #[error("schema mismatch: {0}")]
    SchemaMismatch(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_useful() {
        let e = ProtocolError::UnexpectedEof { needed: 4 };
        assert_eq!(e.to_string(), "unexpected end of buffer: needed 4 more bytes");
    }
}
```

- [ ] **Step 2: Re-export from `lib.rs`**

Replace `crates/protocol/src/lib.rs`:

```rust
//! Kafka wire protocol codec.
//!
//! See the design document at
//! `docs/superpowers/specs/2026-05-10-crabka-rust-rewrite-design.md`
//! in this repo for the project rationale.

mod error;

pub use error::ProtocolError;
```

- [ ] **Step 3: Run the test**

```bash
cargo test -p crabka-protocol error::tests
```

Expected: PASS, `display_is_useful` succeeds.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add ProtocolError"
```

---

### Task 4: Define `Encode` and `Decode` traits

**Files:**
- Create: `crates/protocol/src/codec.rs`
- Modify: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Write the traits**

`crates/protocol/src/codec.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::ProtocolError;

/// Encode a Kafka wire-protocol value into a buffer at the given protocol version.
///
/// `version` is the message-level version negotiated via `ApiVersionsRequest`.
/// Implementations must produce bytes that are byte-equal to the upstream JVM
/// `kafka-clients` implementation for the same `(message_type, version, value)`.
pub trait Encode {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError>;

    /// Size in bytes that `encode` will write. Must equal the actual count.
    fn encoded_len(&self, version: i16) -> usize;
}

/// Decode a Kafka wire-protocol value from a buffer at the given protocol version.
///
/// The `'de` lifetime is the lifetime the decoded value may borrow from the input.
/// Owned-flavor types implement `Decode<'de>` for any `'de` (their output is `'static`).
/// Borrowed-flavor types implement `Decode<'de>` where `Self: 'de`.
pub trait Decode<'de>: Sized {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError>;
}
```

- [ ] **Step 2: Re-export from `lib.rs`**

```rust
//! Kafka wire protocol codec.

mod codec;
mod error;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
```

- [ ] **Step 3: Compile**

```bash
cargo build -p crabka-protocol
```

Expected: `Finished`.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add Encode/Decode traits"
```

---

## Phase 2 — Fixed-width integer primitives

### Task 5: Implement `INT8`, `INT16`, `INT32`, `INT64`, `BOOLEAN`, `DOUBLE`

Kafka primitives are big-endian. We provide free functions rather than trait impls to keep call sites explicit about which primitive is in use.

**Files:**
- Create: `crates/protocol/src/primitives/fixed.rs`
- Create: `crates/protocol/src/primitives/mod.rs`
- Modify: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Write the failing tests**

`crates/protocol/src/primitives/fixed.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::ProtocolError;

#[inline]
fn need(buf: &impl Buf, n: usize) -> Result<(), ProtocolError> {
    if buf.remaining() < n {
        Err(ProtocolError::UnexpectedEof { needed: n - buf.remaining() })
    } else {
        Ok(())
    }
}

pub fn put_i8<B: BufMut>(buf: &mut B, v: i8) { buf.put_i8(v); }
pub fn get_i8<B: Buf>(buf: &mut B) -> Result<i8, ProtocolError> {
    need(buf, 1)?; Ok(buf.get_i8())
}

pub fn put_i16<B: BufMut>(buf: &mut B, v: i16) { buf.put_i16(v); }
pub fn get_i16<B: Buf>(buf: &mut B) -> Result<i16, ProtocolError> {
    need(buf, 2)?; Ok(buf.get_i16())
}

pub fn put_i32<B: BufMut>(buf: &mut B, v: i32) { buf.put_i32(v); }
pub fn get_i32<B: Buf>(buf: &mut B) -> Result<i32, ProtocolError> {
    need(buf, 4)?; Ok(buf.get_i32())
}

pub fn put_i64<B: BufMut>(buf: &mut B, v: i64) { buf.put_i64(v); }
pub fn get_i64<B: Buf>(buf: &mut B) -> Result<i64, ProtocolError> {
    need(buf, 8)?; Ok(buf.get_i64())
}

pub fn put_u32<B: BufMut>(buf: &mut B, v: u32) { buf.put_u32(v); }
pub fn get_u32<B: Buf>(buf: &mut B) -> Result<u32, ProtocolError> {
    need(buf, 4)?; Ok(buf.get_u32())
}

pub fn put_bool<B: BufMut>(buf: &mut B, v: bool) { buf.put_u8(u8::from(v)); }
pub fn get_bool<B: Buf>(buf: &mut B) -> Result<bool, ProtocolError> {
    need(buf, 1)?;
    match buf.get_u8() {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProtocolError::InvalidValue("boolean must be 0 or 1")),
    }
}

pub fn put_f64<B: BufMut>(buf: &mut B, v: f64) { buf.put_f64(v); }
pub fn get_f64<B: Buf>(buf: &mut B) -> Result<f64, ProtocolError> {
    need(buf, 8)?; Ok(buf.get_f64())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    macro_rules! roundtrip {
        ($name:ident, $put:ident, $get:ident, $values:expr) => {
            #[test]
            fn $name() {
                for v in $values {
                    let mut buf = BytesMut::new();
                    $put(&mut buf, v);
                    let mut cur = &buf[..];
                    assert_eq!($get(&mut cur).unwrap(), v);
                    assert!(cur.is_empty(), "decoder did not consume all bytes");
                }
            }
        };
    }

    roundtrip!(i8_roundtrip,  put_i8,  get_i8,  [i8::MIN, -1, 0, 1, i8::MAX]);
    roundtrip!(i16_roundtrip, put_i16, get_i16, [i16::MIN, -1, 0, 1, i16::MAX]);
    roundtrip!(i32_roundtrip, put_i32, get_i32, [i32::MIN, -1, 0, 1, i32::MAX]);
    roundtrip!(i64_roundtrip, put_i64, get_i64, [i64::MIN, -1, 0, 1, i64::MAX]);

    #[test]
    fn bool_roundtrip() {
        for v in [false, true] {
            let mut buf = BytesMut::new();
            put_bool(&mut buf, v);
            let mut cur = &buf[..];
            assert_eq!(get_bool(&mut cur).unwrap(), v);
        }
    }

    #[test]
    fn bool_rejects_invalid() {
        let bytes = [2u8];
        let mut cur = &bytes[..];
        assert!(get_bool(&mut cur).is_err());
    }

    #[test]
    fn eof_is_reported() {
        let empty: &[u8] = &[];
        let mut cur = empty;
        assert!(matches!(
            get_i32(&mut cur),
            Err(ProtocolError::UnexpectedEof { needed: 4 })
        ));
    }

    #[test]
    fn big_endian_layout_i32() {
        let mut buf = BytesMut::new();
        put_i32(&mut buf, 0x01020304);
        assert_eq!(&buf[..], &[0x01, 0x02, 0x03, 0x04]);
    }
}
```

`crates/protocol/src/primitives/mod.rs`:

```rust
pub mod fixed;
```

- [ ] **Step 2: Hook the module up**

Modify `crates/protocol/src/lib.rs`:

```rust
//! Kafka wire protocol codec.

mod codec;
mod error;
pub mod primitives;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
```

- [ ] **Step 3: Run the tests**

```bash
cargo test -p crabka-protocol primitives::fixed
```

Expected: 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add fixed-width integer primitives"
```

---

### Task 6: Implement `VARINT`, `VARLONG`, `UVARINT`

Kafka's `VARINT` and `VARLONG` use zig-zag + LEB128 (same as Protocol Buffers signed varints). `UNSIGNED_VARINT` (also called `UVARINT`) uses plain LEB128. The max byte length is 5 for 32-bit (the Kafka convention) and 10 for 64-bit.

**Files:**
- Create: `crates/protocol/src/primitives/varint.rs`
- Modify: `crates/protocol/src/primitives/mod.rs`

- [ ] **Step 1: Write the varint code with inline tests**

`crates/protocol/src/primitives/varint.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::ProtocolError;

const MAX_VARINT_BYTES: usize = 5;   // 32-bit
const MAX_VARLONG_BYTES: usize = 10; // 64-bit

pub fn put_uvarint<B: BufMut>(buf: &mut B, mut v: u32) {
    while (v & !0x7F) != 0 {
        buf.put_u8(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

pub fn get_uvarint<B: Buf>(buf: &mut B) -> Result<u32, ProtocolError> {
    let mut result: u32 = 0;
    let mut shift = 0;
    for _ in 0..MAX_VARINT_BYTES {
        if buf.remaining() == 0 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let b = buf.get_u8();
        result |= u32::from(b & 0x7F) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(ProtocolError::VarintTooLong { max: MAX_VARINT_BYTES })
}

pub fn uvarint_len(v: u32) -> usize {
    if v == 0 { return 1; }
    let bits = 32 - v.leading_zeros() as usize;
    (bits + 6) / 7
}

pub fn put_varint<B: BufMut>(buf: &mut B, v: i32) {
    let zz = ((v << 1) ^ (v >> 31)) as u32;
    put_uvarint(buf, zz);
}

pub fn get_varint<B: Buf>(buf: &mut B) -> Result<i32, ProtocolError> {
    let zz = get_uvarint(buf)?;
    Ok(((zz >> 1) as i32) ^ -((zz & 1) as i32))
}

pub fn varint_len(v: i32) -> usize {
    let zz = ((v << 1) ^ (v >> 31)) as u32;
    uvarint_len(zz)
}

pub fn put_uvarlong<B: BufMut>(buf: &mut B, mut v: u64) {
    while (v & !0x7F) != 0 {
        buf.put_u8(((v & 0x7F) as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

pub fn get_uvarlong<B: Buf>(buf: &mut B) -> Result<u64, ProtocolError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for _ in 0..MAX_VARLONG_BYTES {
        if buf.remaining() == 0 {
            return Err(ProtocolError::UnexpectedEof { needed: 1 });
        }
        let b = buf.get_u8();
        result |= u64::from(b & 0x7F) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(ProtocolError::VarintTooLong { max: MAX_VARLONG_BYTES })
}

pub fn put_varlong<B: BufMut>(buf: &mut B, v: i64) {
    let zz = ((v << 1) ^ (v >> 63)) as u64;
    put_uvarlong(buf, zz);
}

pub fn get_varlong<B: Buf>(buf: &mut B) -> Result<i64, ProtocolError> {
    let zz = get_uvarlong(buf)?;
    Ok(((zz >> 1) as i64) ^ -((zz & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn uvarint_known_vectors() {
        // (value, expected bytes) — pulled from KIP-482 / protobuf reference.
        let cases: &[(u32, &[u8])] = &[
            (0,          &[0x00]),
            (1,          &[0x01]),
            (127,        &[0x7F]),
            (128,        &[0x80, 0x01]),
            (16_383,     &[0xFF, 0x7F]),
            (16_384,     &[0x80, 0x80, 0x01]),
            (u32::MAX,   &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];
        for (v, expected) in cases {
            let mut buf = BytesMut::new();
            put_uvarint(&mut buf, *v);
            assert_eq!(&buf[..], *expected, "encoding {v}");
            let mut cur = *expected;
            assert_eq!(get_uvarint(&mut cur).unwrap(), *v);
            assert!(cur.is_empty());
            assert_eq!(uvarint_len(*v), expected.len());
        }
    }

    #[test]
    fn varint_zigzag_sample() {
        // (value, expected bytes) — protobuf zig-zag examples.
        let cases: &[(i32, &[u8])] = &[
            (0,          &[0x00]),
            (-1,         &[0x01]),
            (1,          &[0x02]),
            (-2,         &[0x03]),
            (2,          &[0x04]),
            (i32::MAX,   &[0xFE, 0xFF, 0xFF, 0xFF, 0x0F]),
            (i32::MIN,   &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];
        for (v, expected) in cases {
            let mut buf = BytesMut::new();
            put_varint(&mut buf, *v);
            assert_eq!(&buf[..], *expected, "encoding {v}");
            let mut cur = *expected;
            assert_eq!(get_varint(&mut cur).unwrap(), *v);
            assert_eq!(varint_len(*v), expected.len());
        }
    }

    #[test]
    fn uvarint_rejects_overlong() {
        let too_long = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x01];
        let mut cur = &too_long[..];
        assert!(matches!(get_uvarint(&mut cur), Err(ProtocolError::VarintTooLong { .. })));
    }

    #[test]
    fn uvarint_eof() {
        let truncated = [0x80u8];
        let mut cur = &truncated[..];
        assert!(matches!(get_uvarint(&mut cur), Err(ProtocolError::UnexpectedEof { .. })));
    }
}
```

- [ ] **Step 2: Add module to primitives**

`crates/protocol/src/primitives/mod.rs`:

```rust
pub mod fixed;
pub mod varint;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol primitives::varint
```

Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add varint/varlong/uvarint primitives"
```

---

### Task 7: Implement `STRING` / `COMPACT_STRING` / `BYTES` / `COMPACT_BYTES`

**Files:**
- Create: `crates/protocol/src/primitives/string_bytes.rs`
- Modify: `crates/protocol/src/primitives/mod.rs`

- [ ] **Step 1: Write the module**

`crates/protocol/src/primitives/string_bytes.rs`:

```rust
use bytes::{Buf, BufMut, Bytes};

use crate::primitives::fixed::{get_i16, get_i32, put_i16, put_i32};
use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};
use crate::ProtocolError;

// ---- STRING (non-flexible) ----
// Wire: INT16 length (>=0), then `length` bytes UTF-8. -1 = null.

pub fn put_string<B: BufMut>(buf: &mut B, s: &str) {
    let len = i16::try_from(s.len()).expect("string length must fit in i16");
    put_i16(buf, len);
    buf.put_slice(s.as_bytes());
}

pub fn put_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        None => put_i16(buf, -1),
        Some(s) => put_string(buf, s),
    }
}

pub fn get_string_owned<B: Buf>(buf: &mut B) -> Result<String, ProtocolError> {
    match get_nullable_string_owned(buf)? {
        Some(s) => Ok(s),
        None => Err(ProtocolError::InvalidValue("non-nullable STRING was null")),
    }
}

pub fn get_nullable_string_owned<B: Buf>(buf: &mut B) -> Result<Option<String>, ProtocolError> {
    let len = get_i16(buf)?;
    if len < 0 { return Ok(None); }
    let n = len as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.remaining() });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    let s = String::from_utf8(v).map_err(|e| ProtocolError::InvalidUtf8(e.utf8_error()))?;
    Ok(Some(s))
}

pub fn string_len(s: &str) -> usize { 2 + s.len() }
pub fn nullable_string_len(s: Option<&str>) -> usize {
    2 + s.map_or(0, str::len)
}

// ---- COMPACT_STRING (flexible) ----
// Wire: UVARINT length+1 (0 = null), then `length` UTF-8 bytes.

pub fn put_compact_string<B: BufMut>(buf: &mut B, s: &str) {
    let len = u32::try_from(s.len() + 1).expect("string length too large");
    put_uvarint(buf, len);
    buf.put_slice(s.as_bytes());
}

pub fn put_compact_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        None => put_uvarint(buf, 0),
        Some(s) => put_compact_string(buf, s),
    }
}

pub fn get_compact_string_owned<B: Buf>(buf: &mut B) -> Result<String, ProtocolError> {
    match get_compact_nullable_string_owned(buf)? {
        Some(s) => Ok(s),
        None => Err(ProtocolError::InvalidValue("non-nullable COMPACT_STRING was null")),
    }
}

pub fn get_compact_nullable_string_owned<B: Buf>(buf: &mut B) -> Result<Option<String>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 { return Ok(None); }
    let n = (raw - 1) as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.remaining() });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    let s = String::from_utf8(v).map_err(|e| ProtocolError::InvalidUtf8(e.utf8_error()))?;
    Ok(Some(s))
}

pub fn compact_string_len(s: &str) -> usize {
    uvarint_len(u32::try_from(s.len() + 1).unwrap()) + s.len()
}
pub fn compact_nullable_string_len(s: Option<&str>) -> usize {
    match s {
        None => uvarint_len(0),
        Some(s) => compact_string_len(s),
    }
}

// ---- BYTES / COMPACT_BYTES ----
// BYTES: INT32 length, `length` bytes. -1 = null.
// COMPACT_BYTES: UVARINT length+1 (0=null), `length` bytes.

pub fn put_bytes<B: BufMut>(buf: &mut B, b: &[u8]) {
    let len = i32::try_from(b.len()).expect("bytes length must fit in i32");
    put_i32(buf, len);
    buf.put_slice(b);
}

pub fn put_nullable_bytes<B: BufMut>(buf: &mut B, b: Option<&[u8]>) {
    match b {
        None => put_i32(buf, -1),
        Some(b) => put_bytes(buf, b),
    }
}

pub fn get_bytes_owned<B: Buf>(buf: &mut B) -> Result<Bytes, ProtocolError> {
    match get_nullable_bytes_owned(buf)? {
        Some(b) => Ok(b),
        None => Err(ProtocolError::InvalidValue("non-nullable BYTES was null")),
    }
}

pub fn get_nullable_bytes_owned<B: Buf>(buf: &mut B) -> Result<Option<Bytes>, ProtocolError> {
    let len = get_i32(buf)?;
    if len < 0 { return Ok(None); }
    let n = len as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.remaining() });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    Ok(Some(Bytes::from(v)))
}

pub fn put_compact_bytes<B: BufMut>(buf: &mut B, b: &[u8]) {
    let len = u32::try_from(b.len() + 1).expect("bytes length too large");
    put_uvarint(buf, len);
    buf.put_slice(b);
}

pub fn get_compact_nullable_bytes_owned<B: Buf>(buf: &mut B) -> Result<Option<Bytes>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 { return Ok(None); }
    let n = (raw - 1) as usize;
    if buf.remaining() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.remaining() });
    }
    let mut v = vec![0u8; n];
    buf.copy_to_slice(&mut v);
    Ok(Some(Bytes::from(v)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn string_roundtrip() {
        let mut buf = BytesMut::new();
        put_string(&mut buf, "kafka");
        // INT16(5) + bytes
        assert_eq!(&buf[..], &[0x00, 0x05, b'k', b'a', b'f', b'k', b'a']);
        let mut cur = &buf[..];
        assert_eq!(get_string_owned(&mut cur).unwrap(), "kafka");
    }

    #[test]
    fn nullable_string_null() {
        let mut buf = BytesMut::new();
        put_nullable_string(&mut buf, None);
        assert_eq!(&buf[..], &[0xFF, 0xFF]);
        let mut cur = &buf[..];
        assert_eq!(get_nullable_string_owned(&mut cur).unwrap(), None);
    }

    #[test]
    fn compact_string_roundtrip() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "kafka");
        // UVARINT(6) + bytes
        assert_eq!(&buf[..], &[0x06, b'k', b'a', b'f', b'k', b'a']);
        let mut cur = &buf[..];
        assert_eq!(get_compact_string_owned(&mut cur).unwrap(), "kafka");
    }

    #[test]
    fn compact_nullable_string_null() {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, None);
        assert_eq!(&buf[..], &[0x00]);
        let mut cur = &buf[..];
        assert_eq!(get_compact_nullable_string_owned(&mut cur).unwrap(), None);
    }

    #[test]
    fn empty_compact_string() {
        let mut buf = BytesMut::new();
        put_compact_string(&mut buf, "");
        assert_eq!(&buf[..], &[0x01]); // length = 1 means "0 bytes"
        let mut cur = &buf[..];
        assert_eq!(get_compact_string_owned(&mut cur).unwrap(), "");
    }

    #[test]
    fn bytes_roundtrip() {
        let mut buf = BytesMut::new();
        put_bytes(&mut buf, &[1, 2, 3]);
        let mut cur = &buf[..];
        let out = get_bytes_owned(&mut cur).unwrap();
        assert_eq!(out.as_ref(), &[1, 2, 3]);
    }

    #[test]
    fn invalid_utf8_is_rejected() {
        // INT16(2) + invalid UTF-8 byte sequence
        let bytes = [0x00, 0x02, 0xC3, 0x28];
        let mut cur = &bytes[..];
        assert!(matches!(get_string_owned(&mut cur), Err(ProtocolError::InvalidUtf8(_))));
    }
}
```

- [ ] **Step 2: Update primitives module**

`crates/protocol/src/primitives/mod.rs`:

```rust
pub mod fixed;
pub mod string_bytes;
pub mod varint;
```

- [ ] **Step 3: Run the tests**

```bash
cargo test -p crabka-protocol primitives::string_bytes
```

Expected: 7 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add string/bytes/compact primitives"
```

---

### Task 8: Implement `UUID`

Kafka UUIDs are 16 big-endian bytes. We expose a `Uuid([u8; 16])` newtype to avoid pulling in `uuid` crate (no feature value vs. an extra dep).

**Files:**
- Create: `crates/protocol/src/primitives/uuid.rs`
- Modify: `crates/protocol/src/primitives/mod.rs`

- [ ] **Step 1: Write module + tests**

`crates/protocol/src/primitives/uuid.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::ProtocolError;

/// 16-byte Kafka UUID. Big-endian on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Uuid(pub [u8; 16]);

impl Uuid {
    pub const ZERO: Uuid = Uuid([0; 16]);
}

pub fn put_uuid<B: BufMut>(buf: &mut B, u: Uuid) { buf.put_slice(&u.0); }

pub fn get_uuid<B: Buf>(buf: &mut B) -> Result<Uuid, ProtocolError> {
    if buf.remaining() < 16 {
        return Err(ProtocolError::UnexpectedEof { needed: 16 - buf.remaining() });
    }
    let mut out = [0u8; 16];
    buf.copy_to_slice(&mut out);
    Ok(Uuid(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn uuid_roundtrip() {
        let u = Uuid([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]);
        let mut buf = BytesMut::new();
        put_uuid(&mut buf, u);
        assert_eq!(buf.len(), 16);
        let mut cur = &buf[..];
        assert_eq!(get_uuid(&mut cur).unwrap(), u);
    }
}
```

- [ ] **Step 2: Hook up module**

`crates/protocol/src/primitives/mod.rs`:

```rust
pub mod fixed;
pub mod string_bytes;
pub mod uuid;
pub mod varint;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol primitives::uuid
```

Expected: 1 test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add UUID primitive"
```

---

## Phase 3 — Tagged fields (KIP-482)

Tagged fields are how flexible-version messages carry optional fields. On the wire: `UVARINT num_tagged_fields`, followed by `num` entries of `UVARINT tag, UVARINT size, <size bytes>`. Tags appear in ascending order. Unknown tags must be preserved so a sender does not lose information.

### Task 9: Implement tagged-field reader/writer

**Files:**
- Create: `crates/protocol/src/tagged_fields.rs`
- Modify: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/protocol/src/tagged_fields.rs`:

```rust
//! KIP-482 flexible-version tagged fields.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};
use crate::ProtocolError;

/// An unknown tagged field that was preserved verbatim during decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownTaggedField {
    pub tag: u32,
    pub bytes: Bytes,
}

/// A collection of tagged fields that the schema does not declare. Generated
/// message types contain a `Vec<UnknownTaggedField>` (sorted by tag) so that
/// values can be round-tripped without information loss.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UnknownTaggedFields(pub Vec<UnknownTaggedField>);

impl UnknownTaggedFields {
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn len(&self) -> usize { self.0.len() }
}

/// Read the tagged-fields trailer at the current position of `buf`. `known`
/// is called for each entry whose tag is in the schema; it should consume the
/// field's payload (a `size`-byte slice) and return Ok if it recognised the
/// tag. Anything `known` returns `Ok(false)` for is captured into the unknown
/// vec instead.
pub fn read_tagged_fields<B, F>(buf: &mut B, mut known: F) -> Result<UnknownTaggedFields, ProtocolError>
where
    B: Buf,
    F: FnMut(u32, &mut &[u8]) -> Result<bool, ProtocolError>,
{
    let count = get_uvarint(buf)? as usize;
    let mut unknown = Vec::new();
    let mut last_tag: Option<u32> = None;
    for _ in 0..count {
        let tag = get_uvarint(buf)?;
        if let Some(prev) = last_tag {
            if tag <= prev {
                return Err(ProtocolError::InvalidValue("tagged fields not strictly ascending"));
            }
        }
        last_tag = Some(tag);
        let size = get_uvarint(buf)? as usize;
        if buf.remaining() < size {
            return Err(ProtocolError::UnexpectedEof { needed: size - buf.remaining() });
        }
        // Copy the payload so we can hand a slice to the closure or store it.
        let mut payload = vec![0u8; size];
        buf.copy_to_slice(&mut payload);
        let mut slice = &payload[..];
        if !known(tag, &mut slice)? {
            unknown.push(UnknownTaggedField { tag, bytes: Bytes::from(payload) });
        } else if !slice.is_empty() {
            return Err(ProtocolError::InvalidValue("tagged field decoder did not consume all bytes"));
        }
    }
    Ok(UnknownTaggedFields(unknown))
}

/// Helper used by generated code while emitting tagged fields. Call
/// `WriteTaggedFields::new()`, then `add` each known tag-and-payload-encoder,
/// then `write` to flush, merging with `unknown`.
pub struct WriteTaggedFields {
    entries: Vec<(u32, Bytes)>,
}

impl WriteTaggedFields {
    pub fn new() -> Self { Self { entries: Vec::new() } }

    pub fn add(&mut self, tag: u32, payload: Bytes) {
        self.entries.push((tag, payload));
    }

    pub fn write<B: BufMut>(mut self, buf: &mut B, unknown: &UnknownTaggedFields) {
        for u in &unknown.0 {
            self.entries.push((u.tag, u.bytes.clone()));
        }
        self.entries.sort_by_key(|(t, _)| *t);
        put_uvarint(buf, u32::try_from(self.entries.len()).expect("too many tagged fields"));
        for (tag, payload) in self.entries {
            put_uvarint(buf, tag);
            put_uvarint(buf, u32::try_from(payload.len()).expect("tagged field too large"));
            buf.put_slice(&payload);
        }
    }
}

/// Predicted length of the tagged-fields trailer.
pub fn tagged_fields_len(known: &[(u32, usize)], unknown: &UnknownTaggedFields) -> usize {
    let total = known.len() + unknown.0.len();
    let mut n = uvarint_len(u32::try_from(total).unwrap());
    for (tag, len) in known {
        n += uvarint_len(*tag) + uvarint_len(u32::try_from(*len).unwrap()) + *len;
    }
    for u in &unknown.0 {
        n += uvarint_len(u.tag) + uvarint_len(u32::try_from(u.bytes.len()).unwrap()) + u.bytes.len();
    }
    n
}

/// Encode a value into a freshly-allocated `Bytes` (used to materialize a
/// tagged-field payload before sizing the outer trailer).
pub fn encode_to_bytes<F>(predicted_len: usize, write: F) -> Bytes
where
    F: FnOnce(&mut BytesMut),
{
    let mut buf = BytesMut::with_capacity(predicted_len);
    write(&mut buf);
    debug_assert_eq!(buf.len(), predicted_len, "encoded_len lied");
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tagged_fields() {
        let buf = [0x00u8];
        let mut cur = &buf[..];
        let unknown = read_tagged_fields(&mut cur, |_, _| Ok(false)).unwrap();
        assert!(unknown.is_empty());
        assert!(cur.is_empty());
    }

    #[test]
    fn unknown_tagged_fields_preserved() {
        // count=1, tag=5, size=3, payload=[10,20,30]
        let buf = [0x01, 0x05, 0x03, 10, 20, 30];
        let mut cur = &buf[..];
        let unknown = read_tagged_fields(&mut cur, |_, _| Ok(false)).unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown.0[0].tag, 5);
        assert_eq!(unknown.0[0].bytes.as_ref(), &[10, 20, 30]);
    }

    #[test]
    fn ascending_order_enforced() {
        // count=2, tag=5..., tag=3...  — invalid (descending)
        let buf = [0x02, 0x05, 0x01, 0x00, 0x03, 0x01, 0x00];
        let mut cur = &buf[..];
        assert!(read_tagged_fields(&mut cur, |_, _| Ok(false)).is_err());
    }

    #[test]
    fn write_merges_known_and_unknown_sorted() {
        let mut w = WriteTaggedFields::new();
        w.add(10, Bytes::from_static(&[0xAA]));
        let unknown = UnknownTaggedFields(vec![
            UnknownTaggedField { tag: 5, bytes: Bytes::from_static(&[0xBB]) },
        ]);
        let mut out = BytesMut::new();
        w.write(&mut out, &unknown);
        // Expect: count=2, tag=5,len=1,0xBB, tag=10,len=1,0xAA
        assert_eq!(&out[..], &[0x02, 0x05, 0x01, 0xBB, 0x0A, 0x01, 0xAA]);
    }
}
```

- [ ] **Step 2: Re-export from `lib.rs`**

`crates/protocol/src/lib.rs`:

```rust
//! Kafka wire protocol codec.

mod codec;
mod error;
pub mod primitives;
pub mod tagged_fields;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol tagged_fields
```

Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): add KIP-482 tagged-fields support"
```

---

## Phase 4 — Schema vendoring and codegen IR

### Task 10: Vendor upstream Kafka message schemas

We pin to a specific Apache Kafka commit and copy the JSON message schemas. The exact upstream version is recorded in `schemas/VERSION`. Use the latest GA available at the time of execution.

**Files:**
- Create: `tools/sync-schemas.sh`
- Create: `crates/protocol/schemas/VERSION`
- Create: `crates/protocol/schemas/*.json` (copied)

- [ ] **Step 1: Write the sync script**

`tools/sync-schemas.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Usage: tools/sync-schemas.sh <git-ref>
# Vendors Apache Kafka's wire-protocol JSON schemas at the given ref
# into crates/protocol/schemas/.

REF="${1:?usage: sync-schemas.sh <git-ref>}"
REPO="https://github.com/apache/kafka.git"
DEST="crates/protocol/schemas"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

echo "Cloning apache/kafka at $REF into $TMP..."
git clone --depth 1 --branch "$REF" "$REPO" "$TMP/kafka" 2>/dev/null || {
  git clone "$REPO" "$TMP/kafka"
  (cd "$TMP/kafka" && git checkout "$REF")
}

SRC="$TMP/kafka/clients/src/main/resources/common/message"
test -d "$SRC" || { echo "schema dir not found under upstream"; exit 1; }

rm -rf "$DEST"
mkdir -p "$DEST"
cp "$SRC"/*.json "$DEST"/

SHA=$(cd "$TMP/kafka" && git rev-parse HEAD)
cat > "$DEST/VERSION" <<EOF
ref: $REF
sha: $SHA
synced_at: $(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

echo "Vendored $(ls "$DEST"/*.json | wc -l) schemas at $SHA"
```

```bash
chmod +x tools/sync-schemas.sh
```

- [ ] **Step 2: Run the sync against the latest Kafka GA tag**

Pick the latest `4.x.0` GA tag from https://github.com/apache/kafka/tags.

```bash
./tools/sync-schemas.sh 4.0.0
```

Expected: `Vendored 97 schemas at <sha>` (the exact count will vary per version; expect roughly 80-120 schemas).

- [ ] **Step 3: Verify a few schemas look sane**

```bash
ls crates/protocol/schemas | head -20
cat crates/protocol/schemas/ApiVersionsRequest.json | head -30
```

Expected: schemas contain `apiKey`, `type`, `name`, `validVersions`, `flexibleVersions`, `fields` keys.

- [ ] **Step 4: Commit**

```bash
git add tools/sync-schemas.sh crates/protocol/schemas
git commit -m "feat(protocol): vendor Kafka 4.0.0 message schemas"
```

---

### Task 11: Build the codegen crate skeleton

The codegen runs as a workspace `bin` target inside a dedicated crate `crabka-protocol-codegen`. It reads schemas, builds an IR, and writes Rust source files. Build-time integration with `crabka-protocol` comes after the codegen is testable.

**Files:**
- Create: `crates/protocol-codegen/Cargo.toml`
- Create: `crates/protocol-codegen/src/main.rs`
- Create: `crates/protocol-codegen/src/ir.rs`
- Create: `crates/protocol-codegen/src/main.rs`

- [ ] **Step 1: Write the crate manifest**

`crates/protocol-codegen/Cargo.toml`:

```toml
[package]
name = "crabka-protocol-codegen"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
description = "Code generator for crabka-protocol"
publish = false

[lints]
workspace = true

[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[[bin]]
name = "crabka-protocol-codegen"
path = "src/main.rs"
```

- [ ] **Step 2: Stub `main.rs`**

`crates/protocol-codegen/src/main.rs`:

```rust
use std::path::PathBuf;
use std::process::ExitCode;

mod ir;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let schemas_dir = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
            return ExitCode::from(2);
        }
    };
    let out_dir = match args.next() {
        Some(s) => PathBuf::from(s),
        None => {
            eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
            return ExitCode::from(2);
        }
    };

    match run(&schemas_dir, &out_dir) {
        Ok(n) => {
            eprintln!("Generated code for {n} messages into {}", out_dir.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(_schemas: &std::path::Path, _out: &std::path::Path) -> Result<usize, ir::IrError> {
    Ok(0) // filled in later
}
```

- [ ] **Step 3: Verify the workspace still builds**

```bash
cargo build --workspace
```

Expected: both crates compile.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol-codegen
git commit -m "feat(codegen): add codegen crate skeleton"
```

---

### Task 12: Parse JSON schemas into the IR

Kafka schemas allow `//` comments — `serde_json` does not. We strip comments before parsing.

**Files:**
- Modify: `crates/protocol-codegen/src/ir.rs`
- Create: `crates/protocol-codegen/tests/parse_schemas.rs`

- [ ] **Step 1: Define the IR types and parser**

`crates/protocol-codegen/src/ir.rs`:

```rust
use serde::Deserialize;
use std::fs;
use std::path::Path;
use thiserror::Error;

/// One parsed Kafka message schema.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    #[serde(default)]
    pub api_key: Option<i16>,
    pub valid_versions: VersionRange,
    #[serde(default)]
    pub flexible_versions: FlexibleVersions,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub common_structs: Vec<CommonStruct>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MessageType {
    Request,
    Response,
    Header,
    Data,
}

#[derive(Debug, Deserialize)]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub versions: VersionRange,
    #[serde(default)]
    pub nullable_versions: Option<VersionRange>,
    #[serde(default)]
    pub tagged_versions: Option<VersionRange>,
    #[serde(default)]
    pub tag: Option<u32>,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default = "default_entity_type")]
    pub entity_type: String,
    #[serde(default)]
    pub map_key: bool,
    #[serde(default)]
    pub about: String,
}

fn default_entity_type() -> String { String::new() }

#[derive(Debug, Deserialize)]
pub struct CommonStruct {
    pub name: String,
    pub versions: VersionRange,
    pub fields: Vec<FieldSpec>,
}

/// `"0+"`, `"3+"`, `"0-2"`, `"none"`, `"4"` etc.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VersionRange {
    pub min: i16,
    pub max: i16, // inclusive; i16::MAX represents `+` (open-ended)
}

impl<'de> Deserialize<'de> for VersionRange {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        parse_version_range(&s).map_err(serde::de::Error::custom)
    }
}

fn parse_version_range(s: &str) -> Result<VersionRange, String> {
    if s == "none" {
        return Ok(VersionRange { min: i16::MAX, max: i16::MIN });
    }
    if let Some(rest) = s.strip_suffix('+') {
        let min: i16 = rest.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
        return Ok(VersionRange { min, max: i16::MAX });
    }
    if let Some((lo, hi)) = s.split_once('-') {
        let min: i16 = lo.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
        let max: i16 = hi.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
        return Ok(VersionRange { min, max });
    }
    let single: i16 = s.parse().map_err(|e| format!("bad version `{s}`: {e}"))?;
    Ok(VersionRange { min: single, max: single })
}

impl VersionRange {
    pub fn contains(&self, v: i16) -> bool { v >= self.min && v <= self.max }
    pub fn is_empty(&self) -> bool { self.min > self.max }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum FlexibleVersions {
    #[default]
    None,
    Range(VersionRange),
}

impl<'de> Deserialize<'de> for FlexibleVersions {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s == "none" { return Ok(FlexibleVersions::None); }
        Ok(FlexibleVersions::Range(parse_version_range(&s).map_err(serde::de::Error::custom)?))
    }
}

impl FlexibleVersions {
    pub fn is_flexible(&self, v: i16) -> bool {
        match self {
            FlexibleVersions::None => false,
            FlexibleVersions::Range(r) => r.contains(v),
        }
    }
}

#[derive(Debug, Error)]
pub enum IrError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse {file}: {source}")]
    Parse { file: String, #[source] source: serde_json::Error },
}

/// Read every `*.json` file in `dir`, strip `//` comments, parse as `MessageSpec`.
pub fn load_dir(dir: &Path) -> Result<Vec<MessageSpec>, IrError> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") { continue; }
        let raw = fs::read_to_string(&path)?;
        let stripped = strip_line_comments(&raw);
        let spec: MessageSpec = serde_json::from_str(&stripped).map_err(|e| IrError::Parse {
            file: path.display().to_string(),
            source: e,
        })?;
        out.push(spec);
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Strip JavaScript-style `//` line comments. Naive but adequate for these schemas:
/// quoted strings in the schemas do not contain `//`.
fn strip_line_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    for line in src.lines() {
        if let Some(idx) = line.find("//") {
            out.push_str(&line[..idx]);
        } else {
            out.push_str(line);
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_range_parsing() {
        assert_eq!(parse_version_range("0+").unwrap(), VersionRange { min: 0, max: i16::MAX });
        assert_eq!(parse_version_range("3+").unwrap(), VersionRange { min: 3, max: i16::MAX });
        assert_eq!(parse_version_range("0-2").unwrap(), VersionRange { min: 0, max: 2 });
        assert_eq!(parse_version_range("4").unwrap(),   VersionRange { min: 4, max: 4 });
        assert!(parse_version_range("none").is_err()); // handled at call site
    }

    #[test]
    fn comment_strip() {
        let src = "{\n// hi\n  \"x\": 1 // trailing\n}";
        let out = strip_line_comments(src);
        assert!(!out.contains("hi"));
        assert!(!out.contains("trailing"));
        assert!(out.contains("\"x\": 1"));
    }
}
```

- [ ] **Step 2: Write an integration test that parses the vendored schemas**

`crates/protocol-codegen/tests/parse_schemas.rs`:

```rust
use std::path::PathBuf;

#[test]
fn every_vendored_schema_parses() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap()
        .join("protocol")
        .join("schemas");

    let specs = crabka_protocol_codegen::ir::load_dir(&dir)
        .expect("schemas must parse");

    assert!(specs.len() > 50, "expected many schemas, got {}", specs.len());

    // Sanity: ApiVersionsRequest is present.
    let api_versions = specs.iter().find(|s| s.name == "ApiVersionsRequest").unwrap();
    assert!(api_versions.valid_versions.contains(0));
    assert!(matches!(api_versions.message_type, crabka_protocol_codegen::ir::MessageType::Request));
}
```

- [ ] **Step 3: Expose `ir` as a library too**

Modify `crates/protocol-codegen/Cargo.toml` — add a `lib` target:

```toml
[lib]
name = "crabka_protocol_codegen"
path = "src/lib.rs"
```

Create `crates/protocol-codegen/src/lib.rs`:

```rust
pub mod ir;
```

And remove `mod ir;` from `main.rs`, replacing the import with `use crabka_protocol_codegen::ir;`. Update `main.rs`:

```rust
use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::ir;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(schemas) = args.next() else {
        eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
        return ExitCode::from(2);
    };
    let Some(out) = args.next() else {
        eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
        return ExitCode::from(2);
    };
    match run(&PathBuf::from(schemas), &PathBuf::from(out)) {
        Ok(n) => { eprintln!("Generated code for {n} messages"); ExitCode::SUCCESS }
        Err(e) => { eprintln!("error: {e}"); ExitCode::FAILURE }
    }
}

fn run(schemas: &std::path::Path, _out: &std::path::Path) -> Result<usize, ir::IrError> {
    let specs = ir::load_dir(schemas)?;
    Ok(specs.len())
}
```

- [ ] **Step 4: Run all codegen tests**

```bash
cargo test -p crabka-protocol-codegen
```

Expected: `ir::tests::version_range_parsing`, `ir::tests::comment_strip`, and `every_vendored_schema_parses` all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol-codegen
git commit -m "feat(codegen): parse vendored schemas into IR"
```

---

### Task 13: Validate IR before emitting

Validation rejects schemas that contain constructs the generator does not yet understand, instead of emitting wrong code.

**Files:**
- Create: `crates/protocol-codegen/src/validate.rs`
- Modify: `crates/protocol-codegen/src/lib.rs`

- [ ] **Step 1: Add the validator**

`crates/protocol-codegen/src/validate.rs`:

```rust
use thiserror::Error;

use crate::ir::{FieldSpec, FlexibleVersions, MessageSpec, MessageType};

#[derive(Debug, Error)]
pub enum ValidateError {
    #[error("{message}: in {context}")]
    Unsupported { message: &'static str, context: String },
}

/// Field types the generator currently understands. Anything else is a hard error.
const KNOWN_PRIMITIVE_TYPES: &[&str] = &[
    "bool", "int8", "int16", "int32", "int64", "uint16", "uint32", "float64",
    "string", "bytes", "uuid", "records",
];

pub fn validate(specs: &[MessageSpec]) -> Result<(), ValidateError> {
    for spec in specs {
        let ctx = spec.name.clone();
        if matches!(spec.message_type, MessageType::Request | MessageType::Response)
            && spec.api_key.is_none()
        {
            return Err(ValidateError::Unsupported {
                message: "request/response missing apiKey",
                context: ctx,
            });
        }
        validate_fields(&spec.fields, &spec.flexible_versions, &ctx)?;
        for cs in &spec.common_structs {
            validate_fields(&cs.fields, &spec.flexible_versions, &format!("{ctx}.{}", cs.name))?;
        }
    }
    Ok(())
}

fn validate_fields(
    fields: &[FieldSpec],
    flexible: &FlexibleVersions,
    ctx: &str,
) -> Result<(), ValidateError> {
    for f in fields {
        let context = format!("{ctx}.{}", f.name);
        let base = base_type(&f.field_type);

        let known = KNOWN_PRIMITIVE_TYPES.contains(&base)
            || base.starts_with("[]")          // arrays
            || is_struct_type(base);           // struct reference like `MetadataRequestTopic`

        if !known {
            return Err(ValidateError::Unsupported {
                message: "unknown field type",
                context,
            });
        }

        if f.tag.is_some() && !is_some_flexible(flexible) {
            return Err(ValidateError::Unsupported {
                message: "tagged field on non-flexible message",
                context,
            });
        }

        if !f.fields.is_empty() {
            validate_fields(&f.fields, flexible, &context)?;
        }
    }
    Ok(())
}

fn is_some_flexible(f: &FlexibleVersions) -> bool {
    matches!(f, FlexibleVersions::Range(_))
}

fn base_type(t: &str) -> &str {
    t.strip_prefix("[]").unwrap_or(t)
}

fn is_struct_type(t: &str) -> bool {
    // Kafka schema convention: struct types are PascalCase identifiers.
    t.chars().next().map_or(false, char::is_uppercase)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir;
    use std::path::PathBuf;

    #[test]
    fn vendored_schemas_validate() {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent().unwrap()
            .join("protocol")
            .join("schemas");
        let specs = ir::load_dir(&dir).unwrap();
        // If this test fails, the generator needs an update before we can target
        // this Kafka release — surface the offending schema clearly.
        validate(&specs).unwrap_or_else(|e| panic!("validation failed: {e}"));
    }
}
```

- [ ] **Step 2: Add to library**

`crates/protocol-codegen/src/lib.rs`:

```rust
pub mod ir;
pub mod validate;
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol-codegen
```

Expected: `vendored_schemas_validate` may fail if the upstream schemas use a primitive or construct not in `KNOWN_PRIMITIVE_TYPES`. If so, **stop and audit the failure**: it's surfacing a real generator gap. Extend the validator to accept the new construct only after deciding how the generator will emit code for it. Commit the extension before continuing.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol-codegen
git commit -m "feat(codegen): validate IR against known constructs"
```

---

## Phase 5 — Owned-flavor codegen for the pilot message

The pilot message is `ApiVersionsRequest` and `ApiVersionsResponse`. We hand-write the target output first (as both reference and integration check), then implement codegen to produce equivalent output.

### Task 14: Hand-write `owned::ApiVersionsRequest`

This is the reference implementation. We will later assert codegen output matches.

**Files:**
- Create: `crates/protocol/src/owned/mod.rs`
- Create: `crates/protocol/src/owned/api_versions_request.rs`
- Modify: `crates/protocol/src/lib.rs`

- [ ] **Step 1: Hand-write the type and codec**

`crates/protocol/src/owned/api_versions_request.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::primitives::string_bytes::{
    compact_string_len, get_compact_string_owned, get_string_owned,
    put_compact_string, put_string, string_len,
};
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};

/// `ApiVersionsRequest`, owned flavor.
///
/// Field availability by version (per upstream schema):
/// - `client_software_name`, `client_software_version`: v3+
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: String,
    pub client_software_version: String,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

pub const API_KEY: i16 = 18;
pub const MIN_VERSION: i16 = 0;
pub const MAX_VERSION: i16 = 3;
pub const FLEXIBLE_MIN: i16 = 3;

fn is_flexible(version: i16) -> bool { version >= FLEXIBLE_MIN }

impl Encode for ApiVersionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if version >= 3 {
            put_compact_string(buf, &self.client_software_name);
            put_compact_string(buf, &self.client_software_version);
            // No known tagged fields on this message; emit only unknown.
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        // v0..=v2 have an empty body.
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        if !is_flexible(version) { return 0; }
        let known: &[(u32, usize)] = &[];
        compact_string_len(&self.client_software_name)
            + compact_string_len(&self.client_software_version)
            + tagged_fields_len(known, &self.unknown_tagged_fields)
    }
}

impl<'de> Decode<'de> for ApiVersionsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if !is_flexible(version) {
            return Ok(Self::default());
        }
        let client_software_name = get_compact_string_owned(buf)?;
        let client_software_version = get_compact_string_owned(buf)?;
        let unknown = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        Ok(Self {
            client_software_name,
            client_software_version,
            unknown_tagged_fields: unknown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn v0_is_empty() {
        let req = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 0).unwrap();
        assert!(buf.is_empty());
        let mut cur = &buf[..];
        let decoded = ApiVersionsRequest::decode(&mut cur, 0).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka".to_string(),
            client_software_version: "0.0.0".to_string(),
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        assert_eq!(req.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsRequest::decode(&mut cur, 3).unwrap();
        assert_eq!(decoded, req);
        // No trailing bytes.
        assert!(cur.is_empty());
    }

    #[test]
    fn rejects_unsupported_version() {
        let req = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        assert!(matches!(
            req.encode(&mut buf, 99),
            Err(ProtocolError::UnsupportedVersion { api_key: 18, version: 99 })
        ));
    }
}
```

- [ ] **Step 2: Re-export the module**

`crates/protocol/src/owned/mod.rs`:

```rust
//! Owned-flavor generated and hand-authored message types.

pub mod api_versions_request;
```

Modify `crates/protocol/src/lib.rs`:

```rust
//! Kafka wire protocol codec.

mod codec;
mod error;
pub mod owned;
pub mod primitives;
pub mod tagged_fields;

pub use codec::{Decode, Encode};
pub use error::ProtocolError;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p crabka-protocol owned::api_versions_request
```

Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): hand-write owned::ApiVersionsRequest"
```

---

### Task 15: Hand-write `owned::ApiVersionsResponse`

`ApiVersionsResponse` is a richer pilot — it has a struct-typed array. Fields per upstream schema (you must verify against the vendored `ApiVersionsResponse.json` before writing):

- `error_code: int16` (v0+)
- `api_keys: []ApiVersion` (v0+) — array of {`api_key: int16`, `min_version: int16`, `max_version: int16`}
- `throttle_time_ms: int32` (v1+)
- Several tagged fields at v3+ (`supported_features`, `finalized_features_epoch`, `finalized_features`, `zk_migration_ready`). For the pilot we only handle the array; tagged fields beyond `unknown` are deferred to coverage.

**Files:**
- Create: `crates/protocol/src/owned/api_versions_response.rs`
- Modify: `crates/protocol/src/owned/mod.rs`

- [ ] **Step 1: Confirm the schema fields**

```bash
cat crates/protocol/schemas/ApiVersionsResponse.json
```

Confirm the field names, types, and version ranges match the layout below. If the upstream schema declares additional non-tagged fields the generator must handle, **stop and extend this task** before writing code that does not match the schema.

- [ ] **Step 2: Write the response type**

`crates/protocol/src/owned/api_versions_response.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::primitives::fixed::{get_i16, get_i32, put_i16, put_i32};
use crate::primitives::varint::{get_uvarint, put_uvarint, uvarint_len};
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};

pub const API_KEY: i16 = 18;
pub const MIN_VERSION: i16 = 0;
pub const MAX_VERSION: i16 = 3;
pub const FLEXIBLE_MIN: i16 = 3;

fn is_flexible(version: i16) -> bool { version >= FLEXIBLE_MIN }

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiVersion>,
    pub throttle_time_ms: i32,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl ApiVersion {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        put_i16(buf, self.api_key);
        put_i16(buf, self.min_version);
        put_i16(buf, self.max_version);
        if is_flexible(version) {
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        let mut n = 6;
        if is_flexible(version) {
            let known: &[(u32, usize)] = &[];
            n += tagged_fields_len(known, &self.unknown_tagged_fields);
        }
        n
    }

    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        let api_key = get_i16(buf)?;
        let min_version = get_i16(buf)?;
        let max_version = get_i16(buf)?;
        let unknown_tagged_fields = if is_flexible(version) {
            read_tagged_fields(buf, |_, _| Ok(false))?
        } else {
            UnknownTaggedFields::default()
        };
        Ok(Self { api_key, min_version, max_version, unknown_tagged_fields })
    }
}

fn put_array_len<B: BufMut>(buf: &mut B, n: usize, flexible: bool) {
    if flexible {
        put_uvarint(buf, u32::try_from(n + 1).unwrap());
    } else {
        put_i32(buf, i32::try_from(n).unwrap());
    }
}

fn array_len_len(n: usize, flexible: bool) -> usize {
    if flexible { uvarint_len(u32::try_from(n + 1).unwrap()) } else { 4 }
}

fn get_array_len<B: Buf>(buf: &mut B, flexible: bool) -> Result<usize, ProtocolError> {
    if flexible {
        let raw = get_uvarint(buf)?;
        if raw == 0 {
            return Err(ProtocolError::InvalidValue("non-nullable array was null"));
        }
        Ok((raw - 1) as usize)
    } else {
        let n = get_i32(buf)?;
        if n < 0 {
            return Err(ProtocolError::InvalidValue("non-nullable array had negative length"));
        }
        Ok(n as usize)
    }
}

impl Encode for ApiVersionsResponse {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        let flex = is_flexible(version);
        put_i16(buf, self.error_code);
        put_array_len(buf, self.api_keys.len(), flex);
        for v in &self.api_keys { v.encode(buf, version)?; }
        if version >= 1 { put_i32(buf, self.throttle_time_ms); }
        if flex { WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields); }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        let flex = is_flexible(version);
        let mut n = 2 + array_len_len(self.api_keys.len(), flex);
        for v in &self.api_keys { n += v.encoded_len(version); }
        if version >= 1 { n += 4; }
        if flex {
            let known: &[(u32, usize)] = &[];
            n += tagged_fields_len(known, &self.unknown_tagged_fields);
        }
        n
    }
}

impl<'de> Decode<'de> for ApiVersionsResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        let flex = is_flexible(version);
        let error_code = get_i16(buf)?;
        let n = get_array_len(buf, flex)?;
        let mut api_keys = Vec::with_capacity(n);
        for _ in 0..n { api_keys.push(ApiVersion::decode(buf, version)?); }
        let throttle_time_ms = if version >= 1 { get_i32(buf)? } else { 0 };
        let unknown_tagged_fields = if flex { read_tagged_fields(buf, |_, _| Ok(false))? }
                                   else { UnknownTaggedFields::default() };
        Ok(Self { error_code, api_keys, throttle_time_ms, unknown_tagged_fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn sample(version: i16) -> ApiVersionsResponse {
        ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiVersion { api_key: 0, min_version: 0, max_version: 10, ..Default::default() },
                ApiVersion { api_key: 1, min_version: 0, max_version: 17, ..Default::default() },
            ],
            throttle_time_ms: if version >= 1 { 5 } else { 0 },
            unknown_tagged_fields: Default::default(),
        }
    }

    #[test]
    fn v0_roundtrip() {
        let r = sample(0);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 0).unwrap();
        assert_eq!(r.encoded_len(0), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 0).unwrap(), r);
        assert!(cur.is_empty());
    }

    #[test]
    fn v1_includes_throttle_time() {
        let r = sample(1);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 1).unwrap();
        assert_eq!(r.encoded_len(1), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 1).unwrap(), r);
        assert!(cur.is_empty());
    }

    #[test]
    fn v3_flexible_roundtrip() {
        let r = sample(3);
        let mut buf = BytesMut::new();
        r.encode(&mut buf, 3).unwrap();
        assert_eq!(r.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsResponse::decode(&mut cur, 3).unwrap(), r);
        assert!(cur.is_empty());
    }
}
```

- [ ] **Step 3: Register the module**

Modify `crates/protocol/src/owned/mod.rs`:

```rust
//! Owned-flavor generated and hand-authored message types.

pub mod api_versions_request;
pub mod api_versions_response;
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol owned::api_versions_response
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): hand-write owned::ApiVersionsResponse"
```

---

### Task 16: Generate `owned::ApiVersionsRequest` from the schema

We now make codegen produce code byte-for-byte equal to Task 14's hand-written file. We start with a single-message slice; the generator handles the same primitive set used by ApiVersions (strings, tagged fields). Generalisation to arrays and structs comes in Task 17.

**Files:**
- Create: `crates/protocol-codegen/src/emit_owned.rs`
- Modify: `crates/protocol-codegen/src/main.rs`
- Modify: `crates/protocol-codegen/src/lib.rs`
- Create: `crates/protocol-codegen/tests/snapshot_owned.rs`
- Create: `crates/protocol-codegen/tests/snapshots/ApiVersionsRequest.owned.rs`

The strategy: a snapshot test asserts the emitter, given the parsed `ApiVersionsRequest.json` IR, produces exactly the bytes in `snapshots/ApiVersionsRequest.owned.rs` — and that file matches the hand-written one from Task 14. We keep the hand-written file as the live code path; the snapshot ensures the generator can reproduce it.

- [ ] **Step 1: Add the emitter module**

`crates/protocol-codegen/src/emit_owned.rs`:

```rust
use std::fmt::Write;

use crate::ir::{FlexibleVersions, MessageSpec};

/// Emit Rust source code for the owned flavor of `spec`. Only handles messages
/// whose fields use the primitives implemented in this slice: string/compact_string
/// and a small set of fixed-width types. Extension comes in later tasks.
pub fn emit(spec: &MessageSpec) -> Result<String, EmitError> {
    if spec.name != "ApiVersionsRequest" {
        return Err(EmitError::Unsupported(format!(
            "owned emitter does not yet support {}", spec.name
        )));
    }

    let api_key = spec.api_key.expect("validated earlier");
    let (flex_min, _) = match spec.flexible_versions {
        FlexibleVersions::Range(r) => (r.min, r.max),
        FlexibleVersions::None => (i16::MAX, i16::MAX),
    };
    let min_version = spec.valid_versions.min;
    let max_version = spec.valid_versions.max;

    let mut out = String::new();
    writeln!(out, "// AUTO-GENERATED by crabka-protocol-codegen. Do not edit.").unwrap();
    writeln!(out).unwrap();
    out.push_str(STATIC_HEADER);
    writeln!(out, "pub const API_KEY: i16 = {api_key};").unwrap();
    writeln!(out, "pub const MIN_VERSION: i16 = {min_version};").unwrap();
    writeln!(out, "pub const MAX_VERSION: i16 = {max_version};").unwrap();
    writeln!(out, "pub const FLEXIBLE_MIN: i16 = {flex_min};").unwrap();
    out.push_str(STATIC_BODY);
    Ok(out)
}

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("unsupported: {0}")]
    Unsupported(String),
}

const STATIC_HEADER: &str = r#"
use bytes::{Buf, BufMut};

use crate::primitives::string_bytes::{
    compact_string_len, get_compact_string_owned, put_compact_string,
};
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{Decode, Encode, ProtocolError, UnknownTaggedFields};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ApiVersionsRequest {
    pub client_software_name: String,
    pub client_software_version: String,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

"#;

const STATIC_BODY: &str = r#"
fn is_flexible(version: i16) -> bool { version >= FLEXIBLE_MIN }

impl Encode for ApiVersionsRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if version >= 3 {
            put_compact_string(buf, &self.client_software_name);
            put_compact_string(buf, &self.client_software_version);
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        if !is_flexible(version) { return 0; }
        let known: &[(u32, usize)] = &[];
        compact_string_len(&self.client_software_name)
            + compact_string_len(&self.client_software_version)
            + tagged_fields_len(known, &self.unknown_tagged_fields)
    }
}

impl<'de> Decode<'de> for ApiVersionsRequest {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if !is_flexible(version) {
            return Ok(Self::default());
        }
        let client_software_name = get_compact_string_owned(buf)?;
        let client_software_version = get_compact_string_owned(buf)?;
        let unknown = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        Ok(Self {
            client_software_name,
            client_software_version,
            unknown_tagged_fields: unknown,
        })
    }
}
"#;
```

- [ ] **Step 2: Expose the emitter from the lib**

`crates/protocol-codegen/src/lib.rs`:

```rust
pub mod emit_owned;
pub mod ir;
pub mod validate;
```

- [ ] **Step 3: Write the snapshot**

`crates/protocol-codegen/tests/snapshots/ApiVersionsRequest.owned.rs`: copy the exact `String` value the emitter produces. The simplest way is to run the emitter once and dump its output to this file; first, write the test that does that comparison.

- [ ] **Step 4: Write the snapshot test**

`crates/protocol-codegen/tests/snapshot_owned.rs`:

```rust
use std::path::PathBuf;

use crabka_protocol_codegen::{emit_owned, ir};

#[test]
fn api_versions_request_snapshot() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap()
        .join("protocol")
        .join("schemas");
    let specs = ir::load_dir(&dir).unwrap();
    let spec = specs.iter().find(|s| s.name == "ApiVersionsRequest").unwrap();

    let generated = emit_owned::emit(spec).unwrap();
    let snap_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots/ApiVersionsRequest.owned.rs");

    if std::env::var("UPDATE_SNAPSHOTS").is_ok() {
        std::fs::write(&snap_path, &generated).unwrap();
        return;
    }

    let expected = std::fs::read_to_string(&snap_path).unwrap();
    assert_eq!(generated, expected, "snapshot mismatch; run with UPDATE_SNAPSHOTS=1 to update");
}
```

- [ ] **Step 5: Generate the snapshot file**

```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-protocol-codegen api_versions_request_snapshot
cargo test -p crabka-protocol-codegen api_versions_request_snapshot
```

Expected: first run writes the snapshot; second run passes against it.

- [ ] **Step 6: Verify the snapshot also compiles as Rust by including it from a smoke test**

`crates/protocol-codegen/tests/snapshot_compiles.rs`:

```rust
// Smoke test: confirm the snapshotted generated source compiles when wired into
// the crabka-protocol crate. We don't include it directly here (lifetime of the
// snapshot file is asymmetric with the test); a separate compile check happens
// via crates/protocol/build.rs in a later task.

#[test]
fn snapshot_smoke() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots/ApiVersionsRequest.owned.rs");
    assert!(path.exists(), "snapshot file missing");
    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("pub struct ApiVersionsRequest"));
    assert!(contents.contains("impl Encode for ApiVersionsRequest"));
    assert!(contents.contains("impl<'de> Decode<'de> for ApiVersionsRequest"));
}
```

- [ ] **Step 7: Run all tests**

```bash
cargo test -p crabka-protocol-codegen
cargo test -p crabka-protocol
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add crates/protocol-codegen
git commit -m "feat(codegen): emit owned ApiVersionsRequest, snapshot-tested"
```

---

### Task 17: Replace the hand-written `owned::ApiVersionsRequest` with codegen output

Now wire codegen to actually drive `crates/protocol`'s source — but keep determinism by writing the generated code into a tracked `generated/` directory rather than `OUT_DIR`. CI verifies the checked-in copy matches what the generator would produce.

**Files:**
- Modify: `crates/protocol/src/owned/api_versions_request.rs` (replaced with `include!`)
- Create: `crates/protocol/generated/api_versions_request.owned.rs`
- Create: `tools/regenerate.sh`
- Create: `.github/workflows/codegen-check.yml`

- [ ] **Step 1: Generate the file via the codegen bin**

```bash
mkdir -p crates/protocol/generated
cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated
```

Expected: `crates/protocol/generated/ApiVersionsRequest.owned.rs` exists. The output filename comes from a small change to `main.rs` — add it now.

Update `crates/protocol-codegen/src/main.rs`:

```rust
use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::{emit_owned, ir, validate};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(schemas) = args.next() else {
        eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
        return ExitCode::from(2);
    };
    let Some(out) = args.next() else {
        eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
        return ExitCode::from(2);
    };
    match run(&PathBuf::from(schemas), &PathBuf::from(out)) {
        Ok(n) => { eprintln!("Generated code for {n} messages"); ExitCode::SUCCESS }
        Err(e) => { eprintln!("error: {e}"); ExitCode::FAILURE }
    }
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)]
    Ir(#[from] ir::IrError),
    #[error(transparent)]
    Validate(#[from] validate::ValidateError),
    #[error(transparent)]
    Emit(#[from] emit_owned::EmitError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

fn run(schemas: &std::path::Path, out: &std::path::Path) -> Result<usize, RunError> {
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    let mut count = 0;
    for s in &specs {
        // Today: only ApiVersionsRequest is supported by the owned emitter.
        if s.name != "ApiVersionsRequest" { continue; }
        let body = emit_owned::emit(s)?;
        let file = out.join(format!("{}.owned.rs", s.name));
        std::fs::write(&file, body)?;
        count += 1;
    }
    Ok(count)
}
```

`crates/protocol-codegen/Cargo.toml` — add the dep:

```toml
[dependencies]
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
```

- [ ] **Step 2: Re-point the hand-written file at the generated copy**

Replace `crates/protocol/src/owned/api_versions_request.rs` with:

```rust
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/ApiVersionsRequest.owned.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn v0_is_empty() {
        let req = ApiVersionsRequest::default();
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 0).unwrap();
        assert!(buf.is_empty());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsRequest::decode(&mut cur, 0).unwrap(), req);
    }

    #[test]
    fn v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka".to_string(),
            client_software_version: "0.0.0".to_string(),
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        assert_eq!(req.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        assert_eq!(ApiVersionsRequest::decode(&mut cur, 3).unwrap(), req);
    }
}
```

- [ ] **Step 3: Run the tests**

```bash
cargo test -p crabka-protocol
```

Expected: PASS. If the generated file does not compile, the emitter's `STATIC_*` constants are wrong; fix them, regenerate, commit the corrected generated file.

- [ ] **Step 4: Add the regenerate script**

`tools/regenerate.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
cargo run -p crabka-protocol-codegen -- \
    crates/protocol/schemas \
    crates/protocol/generated
echo "Regenerated. Review the diff with: git diff crates/protocol/generated"
```

```bash
chmod +x tools/regenerate.sh
```

- [ ] **Step 5: Add a CI job that asserts no drift**

`.github/workflows/codegen-check.yml`:

```yaml
name: codegen-check
on:
  pull_request:
  push:
    branches: [main]
jobs:
  drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: ./tools/regenerate.sh
      - name: Fail on drift
        run: |
          if ! git diff --quiet crates/protocol/generated; then
            echo "::error::Generated files are out of sync. Run tools/regenerate.sh and commit."
            git --no-pager diff crates/protocol/generated
            exit 1
          fi
```

- [ ] **Step 6: Commit**

```bash
git add crates/protocol crates/protocol-codegen tools/regenerate.sh .github
git commit -m "feat(codegen): wire generated ApiVersionsRequest into protocol crate"
```

---

## Phase 6 — Differential testing against the JVM oracle

The pilot needs a JVM oracle before we can claim byte-equality with the real Kafka protocol. The oracle is a Java program that uses `org.apache.kafka:kafka-clients` to encode and decode messages, exposing the operations over stdin/stdout as JSON-RPC-lite line-oriented requests.

### Task 18: Build the JVM oracle

**Files:**
- Create: `tools/oracle/build.gradle.kts`
- Create: `tools/oracle/settings.gradle.kts`
- Create: `tools/oracle/gradle.properties`
- Create: `tools/oracle/src/main/java/com/crabka/oracle/Oracle.java`

- [ ] **Step 1: Initialize the Gradle wrapper**

```bash
mkdir -p tools/oracle
cd tools/oracle
gradle wrapper --gradle-version 8.10
cd -
```

If `gradle` is not installed, install Gradle 8.10+ (or follow https://docs.gradle.org/current/userguide/installation.html). The wrapper is checked into git so contributors do not need a system Gradle afterwards.

- [ ] **Step 2: Write the build file**

`tools/oracle/build.gradle.kts`:

```kotlin
plugins {
    java
    application
}

repositories { mavenCentral() }

dependencies {
    // Match the Kafka version pinned in crates/protocol/schemas/VERSION.
    implementation("org.apache.kafka:kafka-clients:4.0.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

application { mainClass.set("com.crabka.oracle.Oracle") }
```

`tools/oracle/settings.gradle.kts`:

```kotlin
rootProject.name = "crabka-oracle"
```

- [ ] **Step 3: Write the oracle**

`tools/oracle/src/main/java/com/crabka/oracle/Oracle.java`:

```java
package com.crabka.oracle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * Line-oriented JSON-RPC oracle.
 *
 * Request:  {"op":"encode","apiKey":18,"version":3,"isRequest":true,"value":{...}}
 *           {"op":"decode","apiKey":18,"version":3,"isRequest":true,"hex":"..."}
 * Response: {"ok":true,"hex":"..."}   or   {"ok":true,"value":{...}}
 *           {"ok":false,"error":"..."}
 */
public final class Oracle {
    private static final ObjectMapper M = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(System.out, true, StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) {
                try {
                    out.println(M.writeValueAsString(handle(M.readTree(line))));
                } catch (Throwable t) {
                    ObjectNode err = M.createObjectNode();
                    err.put("ok", false);
                    err.put("error", t.getClass().getSimpleName() + ": " + t.getMessage());
                    out.println(M.writeValueAsString(err));
                }
            }
        }
    }

    private static ObjectNode handle(JsonNode req) throws Exception {
        String op = req.get("op").asText();
        int apiKey = req.get("apiKey").asInt();
        short version = (short) req.get("version").asInt();
        boolean isRequest = req.get("isRequest").asBoolean();
        ApiMessage msg = isRequest
                ? ApiKeys.forId(apiKey).messageType.newRequest()
                : ApiKeys.forId(apiKey).messageType.newResponse();

        ObjectNode resp = M.createObjectNode();
        if (op.equals("encode")) {
            M.readerForUpdating(msg).readValue(req.get("value"));
            ObjectSerializationCache cache = new ObjectSerializationCache();
            int size = msg.size(cache, version);
            ByteBuffer bb = ByteBuffer.allocate(size);
            msg.write(new ByteBufferAccessor(bb), cache, version);
            bb.flip();
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            resp.put("ok", true);
            resp.put("hex", HexFormat.of().formatHex(bytes));
        } else if (op.equals("decode")) {
            byte[] bytes = HexFormat.of().parseHex(req.get("hex").asText());
            msg.read(new ByteBufferAccessor(ByteBuffer.wrap(bytes)), version);
            resp.put("ok", true);
            resp.set("value", M.valueToTree(msg));
        } else {
            throw new IllegalArgumentException("unknown op: " + op);
        }
        return resp;
    }
}
```

> **Note for the implementor:** the precise APIs (`MessageUtil`, `ApiKeys.forId(...).messageType.newRequest()`, `msg.size(cache, version)`, `msg.write(ByteBufferAccessor, cache, version)`, `msg.read(ByteBufferAccessor, version)`) match Kafka 4.0's `kafka-clients` API surface for generated message classes. If the pinned upstream version differs, consult `org.apache.kafka.common.protocol.MessageGenerator` output for the actual signatures and adjust. The semantic intent — "use the generated Kafka message classes to encode/decode a JSON value at a given version" — does not change.

- [ ] **Step 4: Build and smoke-test the oracle**

```bash
(cd tools/oracle && ./gradlew installDist -q)
# The dist binary lives at tools/oracle/build/install/crabka-oracle/bin/crabka-oracle
echo '{"op":"encode","apiKey":18,"version":0,"isRequest":true,"value":{}}' \
    | tools/oracle/build/install/crabka-oracle/bin/crabka-oracle
```

Expected output:

```
{"ok":true,"hex":""}
```

(`ApiVersionsRequest` v0 has an empty body.)

- [ ] **Step 5: Commit**

```bash
git add tools/oracle .gitignore
git commit -m "feat(oracle): JVM kafka-clients oracle for differential testing"
```

(`tools/oracle/.gradle/` and `tools/oracle/build/` are already in `.gitignore` from Task 1.)

---

### Task 19: Rust subprocess wrapper for the oracle

A long-lived child process avoids paying JVM startup per case.

**Files:**
- Create: `crates/protocol/tests/support/oracle.rs`
- Create: `crates/protocol/tests/support/mod.rs`
- Modify: `crates/protocol/Cargo.toml`

- [ ] **Step 1: Add test-only deps**

`crates/protocol/Cargo.toml`:

```toml
[dev-dependencies]
proptest = { workspace = true }
arbitrary = { workspace = true }
hex = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tempfile = { workspace = true }
toml = { workspace = true }
once_cell = "1"
```

Add `once_cell = "1"` to `[workspace.dependencies]` in the root `Cargo.toml` too.

- [ ] **Step 2: Write the oracle wrapper**

`crates/protocol/tests/support/oracle.rs`:

```rust
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Mutex, MutexGuard};

use once_cell::sync::Lazy;
use serde_json::{json, Value};

pub struct Oracle {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl Oracle {
    pub fn spawn() -> Self {
        let bin = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent().unwrap().parent().unwrap()
            .join("tools/oracle/build/install/crabka-oracle/bin/crabka-oracle");
        assert!(
            bin.exists(),
            "oracle not built; run `(cd tools/oracle && ./gradlew installDist)`"
        );
        let mut child = Command::new(bin)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn oracle");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self { child, stdin, stdout }
    }

    pub fn call(&mut self, req: &Value) -> Value {
        let line = serde_json::to_string(req).unwrap();
        self.stdin.write_all(line.as_bytes()).unwrap();
        self.stdin.write_all(b"\n").unwrap();
        self.stdin.flush().unwrap();
        let mut resp = String::new();
        self.stdout.read_line(&mut resp).unwrap();
        let v: Value = serde_json::from_str(&resp).unwrap();
        assert!(
            v["ok"].as_bool().unwrap_or(false),
            "oracle error: {}",
            v["error"].as_str().unwrap_or("?")
        );
        v
    }

    pub fn encode(&mut self, api_key: i16, version: i16, is_request: bool, value: &Value) -> Vec<u8> {
        let r = self.call(&json!({
            "op": "encode",
            "apiKey": api_key,
            "version": version,
            "isRequest": is_request,
            "value": value,
        }));
        hex::decode(r["hex"].as_str().unwrap()).unwrap()
    }

    pub fn decode(&mut self, api_key: i16, version: i16, is_request: bool, bytes: &[u8]) -> Value {
        let r = self.call(&json!({
            "op": "decode",
            "apiKey": api_key,
            "version": version,
            "isRequest": is_request,
            "hex": hex::encode(bytes),
        }));
        r["value"].clone()
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

static SHARED: Lazy<Mutex<Oracle>> = Lazy::new(|| Mutex::new(Oracle::spawn()));

/// Borrow the shared oracle. Tests serialize through the mutex so a single
/// JVM is reused across all differential cases.
pub fn shared() -> MutexGuard<'static, Oracle> {
    SHARED.lock().unwrap()
}
```

`crates/protocol/tests/support/mod.rs`:

```rust
pub mod oracle;
```

- [ ] **Step 3: Smoke-test the wrapper**

`crates/protocol/tests/oracle_smoke.rs`:

```rust
mod support;
use support::oracle;

use serde_json::json;

#[test]
#[ignore = "requires JVM oracle built; see CONTRIBUTING"]
fn encode_apiversions_v0_empty() {
    let mut o = oracle::shared();
    let bytes = o.encode(18, 0, true, &json!({}));
    assert!(bytes.is_empty(), "v0 ApiVersionsRequest has empty body");
}
```

The `#[ignore]` keeps the test out of default `cargo test` runs; CI explicitly opts in.

- [ ] **Step 4: Run the smoke test**

```bash
(cd tools/oracle && ./gradlew installDist -q)
cargo test -p crabka-protocol --test oracle_smoke -- --ignored
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/tests crates/protocol/Cargo.toml Cargo.toml
git commit -m "test(protocol): JVM oracle subprocess wrapper"
```

---

### Task 20: Differential round-trip for `ApiVersionsRequest`

**Files:**
- Create: `crates/protocol/tests/differential_api_versions.rs`

- [ ] **Step 1: Write the test**

`crates/protocol/tests/differential_api_versions.rs`:

```rust
mod support;
use support::oracle;

use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::{Decode, Encode};
use serde_json::json;

fn rust_encode<T: Encode>(t: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(t.encoded_len(version));
    t.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

fn rust_decode<T: for<'a> Decode<'a>>(bytes: &[u8], version: i16) -> T {
    let mut cur: &[u8] = bytes;
    let v = T::decode(&mut cur, version).unwrap();
    assert!(cur.is_empty(), "Rust decoder left trailing bytes");
    v
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_request_v0_byte_equal() {
    let mut o = oracle::shared();
    let req = ApiVersionsRequest::default();
    let rust = rust_encode(&req, 0);
    let java = o.encode(18, 0, true, &json!({}));
    assert_eq!(rust, java, "v0 byte mismatch");
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_request_v3_byte_equal() {
    let mut o = oracle::shared();
    let req = ApiVersionsRequest {
        client_software_name: "crabka".into(),
        client_software_version: "0.0.0".into(),
        unknown_tagged_fields: Default::default(),
    };
    let rust = rust_encode(&req, 3);
    let java = o.encode(18, 3, true, &json!({
        "clientSoftwareName": "crabka",
        "clientSoftwareVersion": "0.0.0",
    }));
    assert_eq!(rust, java, "v3 byte mismatch");
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_response_v3_byte_equal() {
    let mut o = oracle::shared();
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            ApiVersion { api_key: 0, min_version: 0, max_version: 10, ..Default::default() },
            ApiVersion { api_key: 1, min_version: 0, max_version: 17, ..Default::default() },
        ],
        throttle_time_ms: 5,
        unknown_tagged_fields: Default::default(),
    };
    let rust = rust_encode(&resp, 3);
    let java = o.encode(18, 3, false, &json!({
        "errorCode": 0,
        "apiKeys": [
            {"apiKey": 0, "minVersion": 0, "maxVersion": 10},
            {"apiKey": 1, "minVersion": 0, "maxVersion": 17},
        ],
        "throttleTimeMs": 5,
    }));
    assert_eq!(rust, java, "v3 response byte mismatch");
}

#[test]
#[ignore = "requires JVM oracle"]
fn apiversions_response_decode_matches_java() {
    let mut o = oracle::shared();
    let java = o.encode(18, 3, false, &json!({
        "errorCode": 0,
        "apiKeys": [{"apiKey": 18, "minVersion": 0, "maxVersion": 3}],
        "throttleTimeMs": 0,
    }));
    let decoded: ApiVersionsResponse = rust_decode(&java, 3);
    assert_eq!(decoded.api_keys.len(), 1);
    assert_eq!(decoded.api_keys[0].api_key, 18);
    assert_eq!(decoded.throttle_time_ms, 0);
}
```

- [ ] **Step 2: Run**

```bash
cargo test -p crabka-protocol --test differential_api_versions -- --ignored
```

Expected: 4 tests pass.

If a test fails: the byte mismatch will report exact hex on both sides. Common causes are field-order, tagged-field framing, or null-vs-empty for arrays. Diagnose with the differential output before tweaking the codegen.

- [ ] **Step 3: Commit**

```bash
git add crates/protocol/tests
git commit -m "test(protocol): byte-equality differential vs JVM oracle for ApiVersions"
```

---

## Phase 7 — Proptest layer

### Task 21: `Arbitrary` impls and proptest round-trip for `ApiVersionsRequest`

**Files:**
- Modify: `crates/protocol/Cargo.toml` (feature flag)
- Create: `crates/protocol/src/arbitrary_impls.rs`
- Create: `crates/protocol/tests/proptest_api_versions.rs`

- [ ] **Step 1: Add an opt-in feature for `Arbitrary` impls**

`crates/protocol/Cargo.toml`:

```toml
[features]
arbitrary = ["dep:arbitrary"]

[dependencies]
bytes = { workspace = true }
thiserror = { workspace = true }
arbitrary = { workspace = true, optional = true }
```

- [ ] **Step 2: Write the `Arbitrary` impls**

`crates/protocol/src/arbitrary_impls.rs`:

```rust
#![cfg(feature = "arbitrary")]

use arbitrary::{Arbitrary, Unstructured};

use crate::owned::api_versions_request::ApiVersionsRequest;
use crate::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crate::UnknownTaggedFields;

fn ascii(u: &mut Unstructured, min: usize, max: usize) -> arbitrary::Result<String> {
    let len = u.int_in_range(min..=max)?;
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        let c: u8 = u.int_in_range(0x20..=0x7E)?;
        s.push(c as char);
    }
    Ok(s)
}

impl<'a> Arbitrary<'a> for ApiVersionsRequest {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            client_software_name: ascii(u, 0, 32)?,
            client_software_version: ascii(u, 0, 32)?,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}

impl<'a> Arbitrary<'a> for ApiVersion {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self {
            api_key: u.arbitrary()?,
            min_version: u.arbitrary()?,
            max_version: u.arbitrary()?,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}

impl<'a> Arbitrary<'a> for ApiVersionsResponse {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let n = u.int_in_range(0..=8usize)?;
        let mut api_keys = Vec::with_capacity(n);
        for _ in 0..n { api_keys.push(ApiVersion::arbitrary(u)?); }
        Ok(Self {
            error_code: u.arbitrary()?,
            api_keys,
            throttle_time_ms: u.arbitrary()?,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
    }
}
```

Add to `lib.rs`:

```rust
mod arbitrary_impls;
```

- [ ] **Step 3: Write the proptest harness**

`crates/protocol/tests/proptest_api_versions.rs`:

```rust
use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
use crabka_protocol::{Decode, Encode};
use proptest::prelude::*;

fn arb_request() -> impl Strategy<Value = ApiVersionsRequest> {
    (any::<Vec<u8>>(), 0i64..1024).prop_map(|(seed, _)| {
        let mut u = arbitrary::Unstructured::new(&seed);
        ApiVersionsRequest::arbitrary(&mut u).unwrap_or_default()
    })
}

fn arb_response() -> impl Strategy<Value = ApiVersionsResponse> {
    any::<Vec<u8>>().prop_map(|seed| {
        let mut u = arbitrary::Unstructured::new(&seed);
        ApiVersionsResponse::arbitrary(&mut u).unwrap_or_default()
    })
}

use arbitrary::Arbitrary;

proptest! {
    #[test]
    fn request_v3_roundtrip(v in arb_request()) {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, 3).unwrap();
        prop_assert_eq!(v.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsRequest::decode(&mut cur, 3).unwrap();
        prop_assert_eq!(decoded, v);
        prop_assert!(cur.is_empty());
    }

    #[test]
    fn response_v3_roundtrip(v in arb_response()) {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, 3).unwrap();
        prop_assert_eq!(v.encoded_len(3), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsResponse::decode(&mut cur, 3).unwrap();
        prop_assert_eq!(decoded, v);
        prop_assert!(cur.is_empty());
    }

    #[test]
    fn response_v0_roundtrip(v in arb_response()) {
        let mut buf = BytesMut::new();
        v.encode(&mut buf, 0).unwrap();
        prop_assert_eq!(v.encoded_len(0), buf.len());
        let mut cur = &buf[..];
        let decoded = ApiVersionsResponse::decode(&mut cur, 0).unwrap();
        // v0 doesn't include throttle_time_ms — normalize for comparison.
        let mut expected = v.clone();
        expected.throttle_time_ms = 0;
        prop_assert_eq!(decoded, expected);
    }
}
```

- [ ] **Step 4: Enable the feature for dev-builds and run**

`crates/protocol/Cargo.toml`:

```toml
[features]
default = ["arbitrary"]
arbitrary = ["dep:arbitrary"]
```

```bash
cargo test -p crabka-protocol --test proptest_api_versions
```

Expected: 3 proptest cases pass (each runs the default 256 cases).

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "test(protocol): proptest round-trip for ApiVersions request/response"
```

---

## Phase 8 — Captured-traffic corpus

### Task 22: Corpus format and one ApiVersions entry

**Files:**
- Create: `crates/protocol/tests/corpus/README.md`
- Create: `crates/protocol/tests/corpus/api_versions_request_v3_001.hex`
- Create: `crates/protocol/tests/corpus/api_versions_request_v3_001.toml`
- Create: `crates/protocol/tests/corpus_replay.rs`

- [ ] **Step 1: Document the format**

`crates/protocol/tests/corpus/README.md`:

```markdown
# Wire-format corpus

Each frame is two files with the same stem:

- `*.hex` — the raw bytes of the message body (not including the 4-byte length
  prefix). Whitespace is ignored.
- `*.toml` — metadata sidecar:

```toml
api_key = 18
version = 3
direction = "request"   # "request" or "response"
source_kafka_version = "4.0.0"
synthetic = false       # true if hand-constructed rather than captured
description = "ApiVersions v3 from kafka-console-producer"
```

Every commit, the test harness decodes every frame using the owned codec,
re-encodes, and asserts the bytes match.
```

- [ ] **Step 2: Add a sample entry**

Generate the hex from the oracle to ensure validity:

```bash
echo '{"op":"encode","apiKey":18,"version":3,"isRequest":true,"value":{"clientSoftwareName":"librdkafka","clientSoftwareVersion":"2.4.0"}}' \
  | tools/oracle/build/install/crabka-oracle/bin/crabka-oracle
```

Take the `"hex"` field from the response and write it to:

`crates/protocol/tests/corpus/api_versions_request_v3_001.hex`:

```
<paste-hex-here>
```

`crates/protocol/tests/corpus/api_versions_request_v3_001.toml`:

```toml
api_key = 18
version = 3
direction = "request"
source_kafka_version = "4.0.0"
synthetic = true
description = "ApiVersions v3 with librdkafka/2.4.0 client signature"
```

- [ ] **Step 3: Write the replay test**

`crates/protocol/tests/corpus_replay.rs`:

```rust
use std::fs;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::{Decode, Encode};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Meta {
    api_key: i16,
    version: i16,
    direction: String,
    #[allow(dead_code)] source_kafka_version: String,
    #[allow(dead_code)] synthetic: bool,
    #[allow(dead_code)] description: String,
}

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn load_pair(stem: &Path) -> (Meta, Vec<u8>) {
    let hex_path = stem.with_extension("hex");
    let toml_path = stem.with_extension("toml");
    let hex_raw: String = fs::read_to_string(hex_path).unwrap()
        .chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = hex::decode(hex_raw).unwrap();
    let meta: Meta = toml::from_str(&fs::read_to_string(toml_path).unwrap()).unwrap();
    (meta, bytes)
}

#[test]
fn corpus_round_trips() {
    let dir = corpus_dir();
    let mut entries = 0;
    for e in fs::read_dir(&dir).unwrap() {
        let path = e.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) != Some("hex") { continue; }
        let stem = path.with_extension("");
        let (meta, bytes) = load_pair(&stem);
        entries += 1;

        match (meta.api_key, meta.direction.as_str()) {
            (18, "request") => {
                let mut cur = &bytes[..];
                let decoded = ApiVersionsRequest::decode(&mut cur, meta.version).unwrap();
                assert!(cur.is_empty(), "trailing bytes in {}", stem.display());
                let mut re = BytesMut::new();
                decoded.encode(&mut re, meta.version).unwrap();
                assert_eq!(re.as_ref(), bytes, "byte mismatch in {}", stem.display());
            }
            _ => panic!("unhandled corpus entry: {}", stem.display()),
        }
    }
    assert!(entries > 0, "corpus is empty");
}
```

- [ ] **Step 4: Run**

```bash
cargo test -p crabka-protocol --test corpus_replay
```

Expected: PASS, 1 entry processed.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/tests/corpus crates/protocol/tests/corpus_replay.rs
git commit -m "test(protocol): corpus replay harness with ApiVersions sample"
```

---

## Phase 9 — Borrowed flavor for the pilot message

### Task 23: Hand-write `borrowed::ApiVersionsRequest`

We replicate Task 14 in the borrowed flavor as the next codegen target.

**Files:**
- Create: `crates/protocol/src/borrowed/mod.rs`
- Create: `crates/protocol/src/borrowed/api_versions_request.rs`
- Modify: `crates/protocol/src/lib.rs`
- Create: `crates/protocol/src/primitives/string_bytes_borrowed.rs`
- Modify: `crates/protocol/src/primitives/mod.rs`

- [ ] **Step 1: Add borrowed primitives**

`crates/protocol/src/primitives/string_bytes_borrowed.rs`:

```rust
use bytes::Buf;

use crate::primitives::varint::get_uvarint;
use crate::ProtocolError;

/// Decode a `COMPACT_STRING` borrowing from the input buffer.
/// Requires a contiguous buffer (i.e. `&[u8]`).
pub fn get_compact_string_borrowed<'de>(buf: &mut &'de [u8]) -> Result<&'de str, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 {
        return Err(ProtocolError::InvalidValue("non-nullable COMPACT_STRING was null"));
    }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.len() });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)
}

pub fn get_compact_nullable_string_borrowed<'de>(buf: &mut &'de [u8]) -> Result<Option<&'de str>, ProtocolError> {
    let raw = get_uvarint(buf)?;
    if raw == 0 { return Ok(None); }
    let n = (raw - 1) as usize;
    if buf.len() < n {
        return Err(ProtocolError::UnexpectedEof { needed: n - buf.len() });
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(Some(std::str::from_utf8(head).map_err(ProtocolError::InvalidUtf8)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrowed_decode_zero_copy() {
        let bytes = [0x06u8, b'k', b'a', b'f', b'k', b'a'];
        let mut cur: &[u8] = &bytes;
        let s = get_compact_string_borrowed(&mut cur).unwrap();
        assert_eq!(s, "kafka");
        // Pointer identity: `s` points inside `bytes`.
        let bytes_ptr = bytes.as_ptr() as usize;
        let s_ptr = s.as_ptr() as usize;
        assert!(s_ptr >= bytes_ptr && s_ptr < bytes_ptr + bytes.len());
    }
}
```

`crates/protocol/src/primitives/mod.rs`:

```rust
pub mod fixed;
pub mod string_bytes;
pub mod string_bytes_borrowed;
pub mod uuid;
pub mod varint;
```

> **Note on the borrowed `Decode` API:** the generic `Decode<'de>` trait is buffer-generic via `Buf`, but to literally borrow we need a `&[u8]` (a `Buf` can be discontiguous). The borrowed flavor therefore defines its own trait `DecodeBorrow<'de>` that takes `&mut &'de [u8]`. Generated borrowed types implement `DecodeBorrow<'de>`, NOT `Decode<'de>`.

- [ ] **Step 2: Add the borrowed trait**

Extend `crates/protocol/src/codec.rs`:

```rust
use bytes::{Buf, BufMut};

use crate::ProtocolError;

pub trait Encode {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError>;
    fn encoded_len(&self, version: i16) -> usize;
}

pub trait Decode<'de>: Sized {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError>;
}

/// Like `Decode`, but for borrowed (zero-copy) flavors. Requires a contiguous
/// buffer because borrowed values reference slices of it.
pub trait DecodeBorrow<'de>: Sized + 'de {
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError>;
}
```

Re-export from `lib.rs`:

```rust
pub use codec::{Decode, DecodeBorrow, Encode};
```

- [ ] **Step 3: Write the borrowed pilot**

`crates/protocol/src/borrowed/api_versions_request.rs`:

```rust
use bytes::BufMut;

use crate::owned;
use crate::primitives::string_bytes::{
    compact_string_len, put_compact_string,
};
use crate::primitives::string_bytes_borrowed::get_compact_string_borrowed;
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};

pub use crate::owned::api_versions_request::{API_KEY, FLEXIBLE_MIN, MAX_VERSION, MIN_VERSION};

fn is_flexible(version: i16) -> bool { version >= FLEXIBLE_MIN }

/// `ApiVersionsRequest`, borrowed flavor. Strings reference the input buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequest<'a> {
    pub client_software_name: &'a str,
    pub client_software_version: &'a str,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for ApiVersionsRequest<'_> {
    fn default() -> Self {
        Self { client_software_name: "", client_software_version: "", unknown_tagged_fields: Default::default() }
    }
}

impl<'a> ApiVersionsRequest<'a> {
    pub fn to_owned(&self) -> owned::api_versions_request::ApiVersionsRequest {
        owned::api_versions_request::ApiVersionsRequest {
            client_software_name: self.client_software_name.to_string(),
            client_software_version: self.client_software_version.to_string(),
            unknown_tagged_fields: self.unknown_tagged_fields.clone(),
        }
    }
}

impl<'a> Encode for ApiVersionsRequest<'a> {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if version >= 3 {
            put_compact_string(buf, self.client_software_name);
            put_compact_string(buf, self.client_software_version);
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        if !is_flexible(version) { return 0; }
        let known: &[(u32, usize)] = &[];
        compact_string_len(self.client_software_name)
            + compact_string_len(self.client_software_version)
            + tagged_fields_len(known, &self.unknown_tagged_fields)
    }
}

impl<'de> DecodeBorrow<'de> for ApiVersionsRequest<'de> {
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if !is_flexible(version) {
            return Ok(Self::default());
        }
        let client_software_name = get_compact_string_borrowed(buf)?;
        let client_software_version = get_compact_string_borrowed(buf)?;
        let mut tail: &[u8] = buf;
        let unknown_tagged_fields = read_tagged_fields(&mut tail, |_, _| Ok(false))?;
        *buf = tail;
        Ok(Self { client_software_name, client_software_version, unknown_tagged_fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn borrowed_v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        let frozen = buf.freeze();
        let mut cur: &[u8] = &frozen;
        let decoded = ApiVersionsRequest::decode_borrow(&mut cur, 3).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn to_owned_matches_owned_codec() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut a = BytesMut::new();
        req.encode(&mut a, 3).unwrap();
        let owned = req.to_owned();
        let mut b = BytesMut::new();
        owned.encode(&mut b, 3).unwrap();
        assert_eq!(a.as_ref(), b.as_ref());
    }
}
```

`crates/protocol/src/borrowed/mod.rs`:

```rust
//! Borrowed-flavor generated and hand-authored message types.

pub mod api_versions_request;
```

Modify `crates/protocol/src/lib.rs`:

```rust
//! Kafka wire protocol codec.

mod arbitrary_impls;
mod codec;
mod error;
pub mod borrowed;
pub mod owned;
pub mod primitives;
pub mod tagged_fields;

pub use codec::{Decode, DecodeBorrow, Encode};
pub use error::ProtocolError;
pub use tagged_fields::{UnknownTaggedField, UnknownTaggedFields};
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p crabka-protocol borrowed
```

Expected: 2 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol
git commit -m "feat(protocol): borrowed flavor for ApiVersionsRequest"
```

---

### Task 24: Codegen for `borrowed::ApiVersionsRequest`

Same shape as Task 16. Emit, snapshot, swap in.

**Files:**
- Create: `crates/protocol-codegen/src/emit_borrowed.rs`
- Modify: `crates/protocol-codegen/src/lib.rs`
- Modify: `crates/protocol-codegen/src/main.rs`
- Create: `crates/protocol-codegen/tests/snapshots/ApiVersionsRequest.borrowed.rs`
- Create: `crates/protocol/generated/ApiVersionsRequest.borrowed.rs`
- Modify: `crates/protocol/src/borrowed/api_versions_request.rs` (becomes `include!`)

- [ ] **Step 1: Write the emitter**

`crates/protocol-codegen/src/emit_borrowed.rs`:

```rust
use std::fmt::Write;

use crate::emit_owned::EmitError;
use crate::ir::{FlexibleVersions, MessageSpec};

pub fn emit(spec: &MessageSpec) -> Result<String, EmitError> {
    if spec.name != "ApiVersionsRequest" {
        return Err(EmitError::Unsupported(format!(
            "borrowed emitter does not yet support {}", spec.name
        )));
    }
    let api_key = spec.api_key.expect("validated earlier");
    let (flex_min, _) = match spec.flexible_versions {
        FlexibleVersions::Range(r) => (r.min, r.max),
        FlexibleVersions::None => (i16::MAX, i16::MAX),
    };
    let min_version = spec.valid_versions.min;
    let max_version = spec.valid_versions.max;

    let mut out = String::new();
    writeln!(out, "// AUTO-GENERATED by crabka-protocol-codegen. Do not edit.").unwrap();
    out.push_str(STATIC);
    writeln!(out, "pub const API_KEY: i16 = {api_key};").unwrap();
    writeln!(out, "pub const MIN_VERSION: i16 = {min_version};").unwrap();
    writeln!(out, "pub const MAX_VERSION: i16 = {max_version};").unwrap();
    writeln!(out, "pub const FLEXIBLE_MIN: i16 = {flex_min};").unwrap();
    out.push_str(IMPLS);
    Ok(out)
}

const STATIC: &str = r#"
use bytes::BufMut;

use crate::owned;
use crate::primitives::string_bytes::{compact_string_len, put_compact_string};
use crate::primitives::string_bytes_borrowed::get_compact_string_borrowed;
use crate::tagged_fields::{read_tagged_fields, tagged_fields_len, WriteTaggedFields};
use crate::{DecodeBorrow, Encode, ProtocolError, UnknownTaggedFields};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequest<'a> {
    pub client_software_name: &'a str,
    pub client_software_version: &'a str,
    pub unknown_tagged_fields: UnknownTaggedFields,
}

impl Default for ApiVersionsRequest<'_> {
    fn default() -> Self {
        Self { client_software_name: "", client_software_version: "", unknown_tagged_fields: Default::default() }
    }
}

impl<'a> ApiVersionsRequest<'a> {
    pub fn to_owned(&self) -> owned::api_versions_request::ApiVersionsRequest {
        owned::api_versions_request::ApiVersionsRequest {
            client_software_name: self.client_software_name.to_string(),
            client_software_version: self.client_software_version.to_string(),
            unknown_tagged_fields: self.unknown_tagged_fields.clone(),
        }
    }
}

"#;

const IMPLS: &str = r#"
fn is_flexible(version: i16) -> bool { version >= FLEXIBLE_MIN }

impl<'a> Encode for ApiVersionsRequest<'a> {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if version >= 3 {
            put_compact_string(buf, self.client_software_name);
            put_compact_string(buf, self.client_software_version);
            WriteTaggedFields::new().write(buf, &self.unknown_tagged_fields);
        }
        Ok(())
    }

    fn encoded_len(&self, version: i16) -> usize {
        if !is_flexible(version) { return 0; }
        let known: &[(u32, usize)] = &[];
        compact_string_len(self.client_software_name)
            + compact_string_len(self.client_software_version)
            + tagged_fields_len(known, &self.unknown_tagged_fields)
    }
}

impl<'de> DecodeBorrow<'de> for ApiVersionsRequest<'de> {
    fn decode_borrow(buf: &mut &'de [u8], version: i16) -> Result<Self, ProtocolError> {
        if !(MIN_VERSION..=MAX_VERSION).contains(&version) {
            return Err(ProtocolError::UnsupportedVersion { api_key: API_KEY, version });
        }
        if !is_flexible(version) {
            return Ok(Self::default());
        }
        let client_software_name = get_compact_string_borrowed(buf)?;
        let client_software_version = get_compact_string_borrowed(buf)?;
        let mut tail: &[u8] = buf;
        let unknown_tagged_fields = read_tagged_fields(&mut tail, |_, _| Ok(false))?;
        *buf = tail;
        Ok(Self { client_software_name, client_software_version, unknown_tagged_fields })
    }
}
"#;
```

- [ ] **Step 2: Wire emitter into the main**

Modify `crates/protocol-codegen/src/main.rs` to emit both flavors:

```rust
use std::path::PathBuf;
use std::process::ExitCode;

use crabka_protocol_codegen::{emit_borrowed, emit_owned, ir, validate};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(schemas) = args.next() else { return usage(); };
    let Some(out) = args.next() else { return usage(); };
    match run(&PathBuf::from(schemas), &PathBuf::from(out)) {
        Ok(n) => { eprintln!("Generated {n} files"); ExitCode::SUCCESS }
        Err(e) => { eprintln!("error: {e}"); ExitCode::FAILURE }
    }
}

fn usage() -> ExitCode {
    eprintln!("usage: crabka-protocol-codegen <schemas-dir> <out-dir>");
    ExitCode::from(2)
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error(transparent)] Ir(#[from] ir::IrError),
    #[error(transparent)] Validate(#[from] validate::ValidateError),
    #[error(transparent)] Emit(#[from] emit_owned::EmitError),
    #[error(transparent)] Io(#[from] std::io::Error),
}

fn run(schemas: &std::path::Path, out: &std::path::Path) -> Result<usize, RunError> {
    let specs = ir::load_dir(schemas)?;
    validate::validate(&specs)?;
    std::fs::create_dir_all(out)?;
    let mut count = 0;
    for s in &specs {
        if s.name != "ApiVersionsRequest" { continue; }
        let owned_body = emit_owned::emit(s)?;
        let borrowed_body = emit_borrowed::emit(s)?;
        std::fs::write(out.join(format!("{}.owned.rs", s.name)), owned_body)?;
        std::fs::write(out.join(format!("{}.borrowed.rs", s.name)), borrowed_body)?;
        count += 2;
    }
    Ok(count)
}
```

- [ ] **Step 3: Run codegen, snapshot, swap in**

```bash
./tools/regenerate.sh
```

Replace `crates/protocol/src/borrowed/api_versions_request.rs` with:

```rust
include!(concat!(env!("CARGO_MANIFEST_DIR"), "/generated/ApiVersionsRequest.borrowed.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn borrowed_v3_roundtrip() {
        let req = ApiVersionsRequest {
            client_software_name: "crabka",
            client_software_version: "0.0.0",
            unknown_tagged_fields: Default::default(),
        };
        let mut buf = BytesMut::new();
        req.encode(&mut buf, 3).unwrap();
        let frozen = buf.freeze();
        let mut cur: &[u8] = &frozen;
        let decoded = ApiVersionsRequest::decode_borrow(&mut cur, 3).unwrap();
        assert_eq!(decoded, req);
    }
}
```

Update the snapshot test for the borrowed emitter — add to `crates/protocol-codegen/tests/snapshot_owned.rs` (rename to `snapshot.rs`):

```rust
use std::path::PathBuf;

use crabka_protocol_codegen::{emit_borrowed, emit_owned, ir};

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().unwrap()
        .join("protocol")
        .join("schemas")
}

fn check(snap_name: &str, generated: &str) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots").join(snap_name);
    if std::env::var("UPDATE_SNAPSHOTS").is_ok() {
        std::fs::write(&path, generated).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap();
    assert_eq!(generated, expected, "snapshot mismatch in {snap_name}; UPDATE_SNAPSHOTS=1 to refresh");
}

#[test]
fn api_versions_request_owned_snapshot() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    let spec = specs.iter().find(|s| s.name == "ApiVersionsRequest").unwrap();
    check("ApiVersionsRequest.owned.rs", &emit_owned::emit(spec).unwrap());
}

#[test]
fn api_versions_request_borrowed_snapshot() {
    let specs = ir::load_dir(&schemas_dir()).unwrap();
    let spec = specs.iter().find(|s| s.name == "ApiVersionsRequest").unwrap();
    check("ApiVersionsRequest.borrowed.rs", &emit_borrowed::emit(spec).unwrap());
}
```

Delete the old `snapshot_owned.rs`.

- [ ] **Step 4: Update snapshots and verify**

```bash
UPDATE_SNAPSHOTS=1 cargo test -p crabka-protocol-codegen --test snapshot
cargo test -p crabka-protocol-codegen
cargo test -p crabka-protocol
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(codegen): borrowed-flavor codegen for ApiVersionsRequest"
```

---

## Phase 10 — Build-time codegen verification

### Task 25: `build.rs` drift check

The CI workflow in Task 17 catches drift. We also want local-build feedback: if a contributor edits `schemas/` without regenerating, the build should fail clearly. We implement this with a `build.rs` in `crabka-protocol` that compares the schemas' `VERSION` file to a token embedded in the generated files.

**Files:**
- Create: `crates/protocol/build.rs`
- Modify: `crates/protocol-codegen/src/emit_owned.rs` (emit a `VERSION_TOKEN` constant)
- Modify: `crates/protocol-codegen/src/emit_borrowed.rs`

- [ ] **Step 1: Hash the schema dir in the emitter**

Modify both emitters to prepend a comment with the SHA. Update `emit_owned.rs`:

```rust
pub fn emit(spec: &MessageSpec) -> Result<String, EmitError> {
    // existing body...
    let header = format!(
        "// AUTO-GENERATED by crabka-protocol-codegen against {schema_version}.\n",
        schema_version = spec_version_token(spec),
    );
    // prepend to `out`
}
```

Add helper that reads from a global passed by the codegen bin (or, simpler: have the bin write the VERSION file contents into the generated output as a constant). Either is fine; choose the simplest. For this plan: the bin reads `schemas/VERSION` once and passes a string into each emit call.

Add the `schemas_version: &str` parameter to `emit_owned::emit` and `emit_borrowed::emit`. Threading the new parameter requires:

- Updating snapshot tests to pass a stable test value (`"test"`)
- Updating `main.rs` to read `schemas/VERSION` and pass its `sha:` line

- [ ] **Step 2: Add `build.rs`**

`crates/protocol/build.rs`:

```rust
use std::fs;
use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schemas_version = fs::read_to_string(root.join("schemas/VERSION"))
        .expect("schemas/VERSION must exist");
    let sha = schemas_version.lines()
        .find_map(|l| l.strip_prefix("sha: "))
        .expect("schemas/VERSION must contain `sha:` line");

    println!("cargo:rerun-if-changed=schemas/VERSION");
    println!("cargo:rerun-if-changed=generated");

    let one = fs::read_to_string(root.join("generated/ApiVersionsRequest.owned.rs"))
        .expect("generated owned file must exist; run tools/regenerate.sh");
    assert!(
        one.contains(sha),
        "generated/ApiVersionsRequest.owned.rs was produced against a different schemas SHA. \
         Run tools/regenerate.sh and commit."
    );
}
```

- [ ] **Step 3: Add `build = "build.rs"` to `Cargo.toml`**

```toml
[package]
name = "crabka-protocol"
build = "build.rs"
```

- [ ] **Step 4: Verify and commit**

```bash
./tools/regenerate.sh
cargo build -p crabka-protocol
git add -A
git commit -m "build(protocol): fail build on schema/generated drift"
```

Expected: clean build. Manually editing `schemas/VERSION` produces the build error from Step 2.

---

## Phase 11 — Documentation and acceptance

### Task 26: Rustdoc and project docs

**Files:**
- Modify: `crates/protocol/src/lib.rs` (crate-level rustdoc)
- Create: `CONTRIBUTING.md`

- [ ] **Step 1: Write crate-level docs**

Replace `crates/protocol/src/lib.rs` head:

```rust
//! Kafka wire-protocol codec.
//!
//! `crabka-protocol` is a pure-Rust library that encodes and decodes every
//! Apache Kafka request and response message, byte-equivalent to the upstream
//! JVM implementation. It performs no I/O and makes no async assumptions; it
//! is intended to be consumed by broker, client, and tooling crates within
//! the Crabka project.
//!
//! ## Two flavors
//!
//! Every message has two generated types:
//!
//! - `owned::FooRequest` — owns its data (`String`, `Bytes`, `Vec<T>`).
//!   Easy to move across `await` points.
//! - `borrowed::FooRequest<'a>` — references slices of the input buffer
//!   (`&'a str`, `&'a [u8]`). Zero-copy decoding.
//!
//! Both implement [`Encode`]; the owned flavor implements [`Decode`] and the
//! borrowed flavor implements [`DecodeBorrow`].
//!
//! ## Versioning
//!
//! `crabka-protocol` is pre-1.0. Breaking API changes per minor version are
//! allowed; see CHANGELOG.md. The wire-protocol pin is recorded in
//! `crates/protocol/schemas/VERSION`.
```

- [ ] **Step 2: Write CONTRIBUTING.md**

`CONTRIBUTING.md`:

```markdown
# Contributing to Crabka

## Prerequisites

- Rust toolchain pinned by `rust-toolchain.toml`.
- JDK 17 (for the differential-test oracle).
- `gradle` is *not* required at the system level — the wrapper is checked in.

## Build

```bash
cargo build --workspace
```

## Run all tests (excluding JVM-dependent ones)

```bash
cargo test --workspace
```

## Run JVM-differential tests

```bash
(cd tools/oracle && ./gradlew installDist)
cargo test --workspace -- --include-ignored
```

## Regenerate code after editing schemas

```bash
./tools/regenerate.sh
git diff crates/protocol/generated
```

CI fails if `crates/protocol/generated` is out of sync with `crates/protocol/schemas`.

## Bumping the upstream Kafka version

1. `./tools/sync-schemas.sh <new-kafka-tag>`
2. `./tools/regenerate.sh`
3. Update the `kafka-clients` version in `tools/oracle/build.gradle.kts` to match.
4. `(cd tools/oracle && ./gradlew installDist)`
5. `cargo test --workspace -- --include-ignored`
6. Commit `schemas/VERSION`, regenerated files, and the Gradle bump together.
```

- [ ] **Step 3: Commit**

```bash
git add crates/protocol/src/lib.rs CONTRIBUTING.md
git commit -m "docs: crate-level rustdoc and CONTRIBUTING"
```

---

### Task 27: CI matrix

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write CI**

`.github/workflows/ci.yml`:

```yaml
name: ci
on:
  pull_request:
  push:
    branches: [main]

jobs:
  rust:
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        toolchain: [stable, "1.82.0"]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ matrix.toolchain }}
          components: rustfmt, clippy
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace --all-targets -- -D warnings
      - run: cargo test --workspace

  jvm-differential:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 17
      - run: (cd tools/oracle && ./gradlew installDist --no-daemon)
      - run: cargo test --workspace -- --include-ignored
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: rust + clippy + fmt matrix and JVM differential job"
```

---

### Task 28: Acceptance checklist for the foundation plan

This task is a verification gate, not a code change. Mark complete only when every item below passes.

- [ ] `cargo fmt --check` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo test --workspace` green
- [ ] `cargo test --workspace -- --include-ignored` green (requires JVM oracle built)
- [ ] `./tools/regenerate.sh && git diff --quiet crates/protocol/generated` (no drift)
- [ ] `ApiVersionsRequest` v0 and v3 byte-equality with JVM oracle verified
- [ ] `ApiVersionsResponse` v0, v1, v3 byte-equality with JVM oracle verified
- [ ] Borrowed flavor for `ApiVersionsRequest` exercised in tests
- [ ] Corpus replay test green with at least one entry
- [ ] CI matrix green on Linux, macOS, Windows
- [ ] CONTRIBUTING.md describes regenerate, oracle build, version bump

When everything above passes, the foundation is done. The follow-up plan `crabka-protocol-coverage` picks up from here to extend the codegen to the remaining ~99 messages — the framework is ready for that work, and the JVM oracle is the bar each new message must meet.

---

## Self-review (against the spec)

**Spec coverage:**

| Spec requirement | Plan coverage |
|---|---|
| Crate layout (workspace, codegen, schemas, src/owned, src/borrowed, tests) | Tasks 1, 2, 11, 14, 23 |
| `Encode` / `Decode` traits | Task 4 |
| `DecodeBorrow` for borrowed flavor | Task 23 |
| Primitives: INT8–64, VARINT/UVARINT, UUID, STRING/COMPACT, BYTES/COMPACT | Tasks 5–8 |
| Tagged fields (KIP-482) | Task 9 |
| Vendored schemas + sync script | Task 10 |
| IR parser + validator | Tasks 12, 13 |
| Owned-flavor codegen | Tasks 16, 17 |
| Borrowed-flavor codegen | Task 24 |
| `to_owned()` bridge | Task 23/24 |
| JVM oracle (sidecar subprocess) | Tasks 18, 19 |
| Differential testing — all 3 checks (JVM→Rust, Rust→JVM, byte-equal) | Task 20 |
| Proptest round-trip | Task 21 |
| Captured-traffic corpus | Task 22 |
| Build-time drift detection | Task 25 |
| CI on Linux/macOS/Windows | Task 27 |
| Rustdoc + CONTRIBUTING | Task 26 |
| MSRV pin (`rust-toolchain.toml`) | Task 1 |
| LICENSE + NOTICE attribution | Task 1 |
| Acceptance criteria 1, 2, 3 — *all* `(api_key, version)` pairs | **Not covered** — explicitly deferred to the follow-up `crabka-protocol-coverage` plan. The foundation covers only `ApiVersions`. This is called out at the top of the plan. |

**Placeholder scan:** No `TODO`, `TBD`, or "implement later" markers in this plan. All "extend to remaining messages" references point to the follow-up plan, not to gaps in this one.

**Type consistency:** Trait names (`Encode`, `Decode`, `DecodeBorrow`), module names (`owned::api_versions_request`, `borrowed::api_versions_request`), constant names (`API_KEY`, `MIN_VERSION`, `MAX_VERSION`, `FLEXIBLE_MIN`), and helper functions (`is_flexible`, `read_tagged_fields`, `WriteTaggedFields`, `tagged_fields_len`) are used consistently across tasks 4, 9, 14–17, 23–25.

**Scope check:** This plan covers foundation + pilot vertical slice. Extension to ~99 remaining messages is explicitly factored out into a follow-up plan, which cannot be meaningfully written until the foundation is real.

---

## Follow-up plan (not in this document)

After Task 28 passes, `crabka-protocol-coverage` will:

- Generalize the codegen IR walker to emit arrays, nested structs, and the remaining primitive types
- Process every message in `schemas/` (request, response, header, data, common structs)
- Per `(api_key, version)`, exercise the three-check JVM differential, with seeds for failure reproduction
- Grow the corpus to at least one entry per `(api_key, version)` that is realistically capturable
- Audit tagged-field handling for every message that declares known tagged fields
- Performance benchmarks (criterion) on hot codecs (Produce, Fetch, RecordBatch)

That plan gets its own brainstorming session.
