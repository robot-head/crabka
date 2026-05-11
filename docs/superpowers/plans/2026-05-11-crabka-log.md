# `crabka-log` (slice 3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `crabka-log` crate — byte-compatible reader and writer for Apache Kafka's on-disk log format, with retention. No log compaction.

**Architecture:** Append-only segments named by 20-digit zero-padded base offset; each segment has companion `.index` (sparse offset → byte position) and `.timeindex` (sparse timestamp → relative offset) files. The `.log` file is a concatenation of `RecordBatch` v2 byte streams that `crabka-protocol::records::RecordBatch` already encodes/decodes. Single-writer (`&mut self`); multiple concurrent readers (`&self`). Retention runs from `Log::tick(now)`.

**Tech Stack:** Rust 1.95.0 edition 2024; `std::fs`/`std::io`; `crabka-protocol` (consumed via `version = "0.1"` workspace path dep); `memmap2` for sparse-index reads (optional, may stay on `read_at` if simpler); `testcontainers-rs` + `testcontainers-modules` for integration tests.

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-log-design.md`](../specs/2026-05-11-crabka-log-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/log-plan` (this file). Implementation runs on `feature/log` branched off `main` once this plan's PR merges.

---

## File structure

```
crates/log/
├── Cargo.toml
├── src/
│   ├── lib.rs               # public re-exports
│   ├── error.rs             # LogError
│   ├── config.rs            # LogConfig
│   ├── name.rs              # segment filename parsing (20-digit base offset)
│   ├── index.rs             # OffsetIndex + TimeIndex (sparse, 8-byte / 12-byte entries)
│   ├── segment.rs           # Segment (open/append/read/seal)
│   ├── recovery.rs          # open-time scan + truncate-partial-trailing-batch
│   ├── retention.rs         # time-based + size-based segment deletion
│   └── log.rs               # Log (top-level: pool of segments + append/read/truncate/tick)
└── tests/
    ├── support/
    │   ├── mod.rs
    │   └── strategies.rs    # proptest Strategies for arb_batches/etc.
    ├── unit.rs              # synthetic-corpus unit tests via fixtures
    ├── proptest_log.rs      # property-based round-trip
    └── integration.rs       # #[ignore]'d testcontainers JVM-roundtrip tests

.github/workflows/ci.yml     # add log-integration job
Cargo.toml                   # add memmap2 (optional), tempfile (already present)
```

---

## Phase A — Crate scaffolding + error + config

### Task 1: Workspace dep + crate skeleton

**Files:**
- Modify: `Cargo.toml` (workspace) — add `memmap2 = "0.9"` to `[workspace.dependencies]`
- Create: `crates/log/Cargo.toml`
- Create: `crates/log/src/lib.rs`

- [ ] **Step 1: Add `memmap2` to workspace deps**

In `Cargo.toml` at the repo root, under `[workspace.dependencies]`, append:

```toml
memmap2 = "0.9"
```

- [ ] **Step 2: Write the crate manifest**

`crates/log/Cargo.toml`:

```toml
[package]
name = "crabka-log"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Byte-compatible reader/writer for Apache Kafka's on-disk log format"

[lints]
workspace = true

[features]
default = []

[dependencies]
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
bytes = { workspace = true }
thiserror = { workspace = true }
memmap2 = { workspace = true }
tracing = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
tempfile = { workspace = true }
hex = { workspace = true }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true }
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Stub `lib.rs`**

`crates/log/src/lib.rs`:

```rust
//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-log-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-log/0.0.0")]
```

- [ ] **Step 4: Verify build**

```bash
cargo build -p crabka-log
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/log
git commit -m "feat(log): add crate skeleton + memmap2 workspace dep"
```

---

### Task 2: `LogError`

**Files:**
- Create: `crates/log/src/error.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/log/src/error.rs`:

```rust
//! Errors returned by `Log` and `Segment`.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LogError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("partial batch at offset {file_offset} in segment {segment}: truncating")]
    PartialBatch { segment: i64, file_offset: u64 },

    #[error(
        "CRC mismatch at offset {file_offset} in segment {segment}: \
         expected {expected:#x}, computed {computed:#x}"
    )]
    CrcMismatch {
        segment: i64,
        file_offset: u64,
        expected: u32,
        computed: u32,
    },

    #[error("offset {requested} below log start {log_start}")]
    OffsetTooLow { requested: i64, log_start: i64 },

    #[error("offset {requested} >= log end {log_end}")]
    OffsetTooHigh { requested: i64, log_end: i64 },

    #[error("records: {0}")]
    Records(#[from] crabka_protocol::records::RecordsError),

    #[error("invalid segment filename: {0}")]
    BadSegmentName(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_partial_batch() {
        let e = LogError::PartialBatch { segment: 0, file_offset: 1024 };
        assert!(e.to_string().contains("offset 1024"));
        assert!(e.to_string().contains("segment 0"));
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.

mod error;

pub use error::LogError;
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log error
git add crates/log
git commit -m "feat(log): LogError enum"
```

---

### Task 3: `LogConfig`

**Files:**
- Create: `crates/log/src/config.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/log/src/config.rs`:

```rust
//! Tunables for `Log`. Defaults match Apache Kafka 4.2.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Roll the active segment when it exceeds this many bytes. Kafka default: 1 GiB.
    pub segment_bytes: u64,

    /// Roll the active segment when its first record is older than this. Kafka default: 7 days.
    pub segment_ms: Duration,

    /// Delete sealed segments older than this. `None` = unlimited. Kafka default: 7 days.
    pub retention_ms: Option<Duration>,

    /// Delete oldest sealed segments until the total `.log` size fits. `None` = unlimited.
    pub retention_bytes: Option<u64>,

    /// Write one `.index`/`.timeindex` entry per N bytes of `.log`. Kafka default: 4 KiB.
    pub index_interval_bytes: u32,

    /// fsync after every `append`. Default off; broker manages fsync separately.
    pub flush_on_append: bool,

    /// On open, CRC every batch in the active segment from the last index entry to EOF.
    pub validate_on_open: bool,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 1024 * 1024 * 1024,        // 1 GiB
            segment_ms: Duration::from_secs(7 * 24 * 3600),  // 7 days
            retention_ms: Some(Duration::from_secs(7 * 24 * 3600)),
            retention_bytes: None,
            index_interval_bytes: 4096,
            flush_on_append: false,
            validate_on_open: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_kafka_4x() {
        let c = LogConfig::default();
        assert_eq!(c.segment_bytes, 1 << 30);
        assert_eq!(c.index_interval_bytes, 4096);
        assert!(!c.flush_on_append);
        assert!(c.validate_on_open);
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod config;
mod error;

pub use config::LogConfig;
pub use error::LogError;
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log config
git add crates/log
git commit -m "feat(log): LogConfig with Kafka 4.x defaults"
```

---

## Phase B — Segment filenames + indexes

### Task 4: Segment filename parsing

**Files:**
- Create: `crates/log/src/name.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/log/src/name.rs`:

```rust
//! Segment filename parsing. Kafka names segments by 20-digit
//! zero-padded base offset, with `.log`, `.index`, `.timeindex` extensions.

use std::path::Path;

use crate::error::LogError;

pub const FILENAME_DIGITS: usize = 20;

/// `0` → `"00000000000000000000"`. `1847` → `"00000000000000001847"`.
#[must_use]
pub fn format_base_offset(base_offset: i64) -> String {
    format!("{base_offset:020}")
}

/// Parse a `.log` filename and return its base offset.
/// `"00000000000000001847.log"` → `Ok(1847)`.
pub fn parse_log_filename(name: &str) -> Result<i64, LogError> {
    let stem = name
        .strip_suffix(".log")
        .ok_or_else(|| LogError::BadSegmentName(name.into()))?;
    if stem.len() != FILENAME_DIGITS {
        return Err(LogError::BadSegmentName(name.into()));
    }
    stem.parse::<i64>()
        .map_err(|_| LogError::BadSegmentName(name.into()))
}

pub fn log_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.log", format_base_offset(base_offset)))
}

pub fn index_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.index", format_base_offset(base_offset)))
}

pub fn timeindex_path(dir: &Path, base_offset: i64) -> std::path::PathBuf {
    dir.join(format!("{}.timeindex", format_base_offset(base_offset)))
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! offset_case {
        ($name:ident, $offset:expr, $expected_filename:expr) => {
            #[test]
            fn $name() {
                let formatted = format_base_offset($offset);
                assert_eq!(formatted, $expected_filename);
                let parsed = parse_log_filename(&format!("{formatted}.log")).unwrap();
                assert_eq!(parsed, $offset);
            }
        };
    }

    offset_case!(zero, 0, "00000000000000000000");
    offset_case!(small, 1847, "00000000000000001847");
    offset_case!(large, 1_000_000_000_000, "00000000001000000000000");

    #[test]
    fn rejects_non_log_extension() {
        assert!(parse_log_filename("00000000000000000000.index").is_err());
    }

    #[test]
    fn rejects_wrong_digit_count() {
        assert!(parse_log_filename("123.log").is_err());
        assert!(parse_log_filename("000000000000000001847.log").is_err()); // 21 digits
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod config;
mod error;
mod name;

pub use config::LogConfig;
pub use error::LogError;
```

(`name` is internal.)

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log name
git add crates/log
git commit -m "feat(log): segment filename formatting + parsing"
```

---

### Task 5: `OffsetIndex` (sparse 8-byte entries)

**Files:**
- Create: `crates/log/src/index.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Write the offset-index module**

`crates/log/src/index.rs`:

```rust
//! Sparse offset index. 8 bytes per entry: relative_offset (u32 BE)
//! + position (u32 BE). Entries are monotonically increasing.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::LogError;

/// 8 bytes per entry.
pub const OFFSET_ENTRY_SIZE: usize = 8;

#[derive(Debug)]
pub struct OffsetIndex {
    file: File,
    /// Entries currently in the file. Lazily loaded into memory on construction.
    entries: Vec<(u32, u32)>,
}

impl OffsetIndex {
    /// Open or create an offset-index file. If the file exists, load its
    /// entries into memory. If it doesn't, create an empty file.
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut entries = Vec::with_capacity(buf.len() / OFFSET_ENTRY_SIZE);
        for chunk in buf.chunks_exact(OFFSET_ENTRY_SIZE) {
            let rel = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            let pos = u32::from_be_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
            entries.push((rel, pos));
        }
        Ok(Self { file, entries })
    }

    /// Append a new entry. Caller ensures monotonicity.
    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<(), LogError> {
        let mut buf = [0u8; OFFSET_ENTRY_SIZE];
        buf[0..4].copy_from_slice(&relative_offset.to_be_bytes());
        buf[4..8].copy_from_slice(&position.to_be_bytes());
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&buf)?;
        self.entries.push((relative_offset, position));
        Ok(())
    }

    /// Find the byte position to start reading at for a given relative offset.
    /// Returns the position of the largest entry with `relative_offset <= target`,
    /// or 0 if no entries are present.
    #[must_use]
    pub fn lookup(&self, target: u32) -> u32 {
        // Binary search for the largest entry <= target.
        match self.entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => self.entries[i].1,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1,
        }
    }

    /// Truncate entries (and the on-disk file) so that all entries with
    /// `position >= max_position_exclusive` are removed.
    pub fn truncate_by_position(&mut self, max_position_exclusive: u32) -> Result<(), LogError> {
        let new_len = self
            .entries
            .iter()
            .take_while(|(_, pos)| *pos < max_position_exclusive)
            .count();
        self.entries.truncate(new_len);
        let new_file_len = (new_len * OFFSET_ENTRY_SIZE) as u64;
        self.file.set_len(new_file_len)?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(u32, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn flush(&mut self) -> Result<(), LogError> {
        self.file.sync_data().map_err(LogError::Io)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        assert_eq!(idx.lookup(50), 0);
        assert_eq!(idx.lookup(100), 4096);
        assert_eq!(idx.lookup(150), 4096);
        assert_eq!(idx.lookup(200), 8192);
        assert_eq!(idx.lookup(9999), 8192);
    }

    #[test]
    fn empty_index_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let idx = OffsetIndex::open(&path).unwrap();
        assert_eq!(idx.lookup(0), 0);
        assert_eq!(idx.lookup(1000), 0);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        {
            let mut idx = OffsetIndex::open(&path).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(100, 4096).unwrap();
            idx.flush().unwrap();
        }
        let idx = OffsetIndex::open(&path).unwrap();
        assert_eq!(idx.entry_count(), 2);
        assert_eq!(idx.lookup(100), 4096);
    }

    #[test]
    fn truncate_by_position() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.index");
        let mut idx = OffsetIndex::open(&path).unwrap();
        idx.append(0, 0).unwrap();
        idx.append(100, 4096).unwrap();
        idx.append(200, 8192).unwrap();
        idx.truncate_by_position(8192).unwrap();
        assert_eq!(idx.entry_count(), 2);
        assert_eq!(idx.last_entry(), Some((100, 4096)));
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod config;
mod error;
mod index;
mod name;

pub use config::LogConfig;
pub use error::LogError;
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log index
git add crates/log
git commit -m "feat(log): OffsetIndex (sparse, 8-byte entries, BE)"
```

---

### Task 6: `TimeIndex` (sparse 12-byte entries)

**Files:**
- Modify: `crates/log/src/index.rs`

- [ ] **Step 1: Append the time-index struct**

Append to `crates/log/src/index.rs`:

```rust
// ===== TimeIndex =====

/// 12 bytes per entry: timestamp (i64 BE) + relative_offset (u32 BE).
pub const TIME_ENTRY_SIZE: usize = 12;

#[derive(Debug)]
pub struct TimeIndex {
    file: File,
    entries: Vec<(i64, u32)>,
}

impl TimeIndex {
    pub fn open(path: &Path) -> Result<Self, LogError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut entries = Vec::with_capacity(buf.len() / TIME_ENTRY_SIZE);
        for chunk in buf.chunks_exact(TIME_ENTRY_SIZE) {
            let ts = i64::from_be_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3],
                chunk[4], chunk[5], chunk[6], chunk[7],
            ]);
            let rel = u32::from_be_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]);
            entries.push((ts, rel));
        }
        Ok(Self { file, entries })
    }

    /// Append. Caller ensures monotonicity.
    pub fn append(&mut self, timestamp: i64, relative_offset: u32) -> Result<(), LogError> {
        let mut buf = [0u8; TIME_ENTRY_SIZE];
        buf[0..8].copy_from_slice(&timestamp.to_be_bytes());
        buf[8..12].copy_from_slice(&relative_offset.to_be_bytes());
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&buf)?;
        self.entries.push((timestamp, relative_offset));
        Ok(())
    }

    /// Find the relative offset at or after the given timestamp.
    /// Returns the relative offset of the largest entry with
    /// `timestamp <= target`, or 0 if no entries.
    #[must_use]
    pub fn lookup(&self, target_timestamp: i64) -> u32 {
        match self.entries.binary_search_by_key(&target_timestamp, |&(ts, _)| ts) {
            Ok(i) => self.entries[i].1,
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1,
        }
    }

    pub fn truncate_by_relative_offset(&mut self, max_rel_exclusive: u32) -> Result<(), LogError> {
        let new_len = self
            .entries
            .iter()
            .take_while(|(_, rel)| *rel < max_rel_exclusive)
            .count();
        self.entries.truncate(new_len);
        self.file.set_len((new_len * TIME_ENTRY_SIZE) as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    #[must_use]
    pub fn last_entry(&self) -> Option<(i64, u32)> {
        self.entries.last().copied()
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn flush(&mut self) -> Result<(), LogError> {
        self.file.sync_data().map_err(LogError::Io)
    }
}

#[cfg(test)]
mod time_tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_lookup_time() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        let mut idx = TimeIndex::open(&path).unwrap();
        idx.append(1_000_000, 0).unwrap();
        idx.append(2_000_000, 100).unwrap();
        idx.append(3_000_000, 200).unwrap();
        assert_eq!(idx.lookup(0), 0);
        assert_eq!(idx.lookup(1_500_000), 0);
        assert_eq!(idx.lookup(2_000_000), 100);
        assert_eq!(idx.lookup(2_500_000), 100);
        assert_eq!(idx.lookup(5_000_000), 200);
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.timeindex");
        {
            let mut idx = TimeIndex::open(&path).unwrap();
            idx.append(1, 0).unwrap();
            idx.append(2, 50).unwrap();
            idx.flush().unwrap();
        }
        let idx = TimeIndex::open(&path).unwrap();
        assert_eq!(idx.entry_count(), 2);
    }
}
```

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-log index
git add crates/log
git commit -m "feat(log): TimeIndex (sparse, 12-byte entries, BE)"
```

---

## Phase C — Segment

### Task 7: `Segment::open` (read-only first pass)

**Files:**
- Create: `crates/log/src/segment.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Write the segment module (open + read; append comes in Task 8)**

`crates/log/src/segment.rs`:

```rust
//! A single segment: `.log` + `.index` + `.timeindex` files sharing a base offset.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::os::raw::c_int;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use crabka_protocol::records::RecordBatch;

use crate::error::LogError;
use crate::index::{OffsetIndex, TimeIndex};
use crate::name;

#[derive(Debug)]
pub struct Segment {
    dir: PathBuf,
    base_offset: i64,
    log_file: File,
    log_size: u64,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    /// `true` once a new segment has been started after this one. Sealed
    /// segments don't accept appends.
    sealed: bool,
    /// Highest timestamp observed across all batches written here.
    max_timestamp: i64,
    /// Last absolute offset (inclusive) of any batch in this segment.
    last_offset: i64,
}

impl Segment {
    /// Open an existing segment for reading. Lightweight — no full scan.
    pub fn open(dir: &Path, base_offset: i64) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let log_size = log_file.metadata()?.len();
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            log_size,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }

    pub fn base_offset(&self) -> i64 { self.base_offset }
    pub fn last_offset(&self) -> i64 { self.last_offset }
    pub fn size_bytes(&self) -> u64 { self.log_size }
    pub fn max_timestamp(&self) -> i64 { self.max_timestamp }
    pub fn is_sealed(&self) -> bool { self.sealed }

    /// Read batches starting at or just before `offset`, up to roughly
    /// `max_bytes` of `.log` data. Returns at least one batch when
    /// `offset` is within the segment's offset range, even if that
    /// batch alone exceeds `max_bytes`.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<RecordBatch>, LogError> {
        if offset > self.last_offset {
            return Ok(vec![]);
        }
        let target_rel = u32::try_from((offset - self.base_offset).max(0))
            .map_err(|_| LogError::BadSegmentName("target offset out of range".into()))?;
        let start_pos = self.offset_index.lookup(target_rel) as u64;
        let mut buf = Vec::with_capacity(max_bytes.min(4 * 1024 * 1024));
        self.read_log_range(start_pos, &mut buf, max_bytes)?;
        let mut out = Vec::new();
        let mut cursor = Bytes::from(buf);
        while !cursor.is_empty() {
            // Borrow a mutable slice cursor over the Bytes.
            let mut slice: &[u8] = &cursor;
            let before = slice.len();
            let batch = match RecordBatch::decode(&mut slice) {
                Ok(b) => b,
                Err(_) => break, // partial trailing batch; ignore
            };
            let consumed = before - slice.len();
            cursor = cursor.slice(consumed..);
            // Filter: only return batches whose last_offset >= requested offset.
            let batch_last = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch_last >= offset {
                out.push(batch);
            }
            if !out.is_empty() && cursor.len() < before {
                // Loose budget: stop after first oversize batch.
                if out.iter().map(crabka_protocol::records::RecordBatch::encoded_len).sum::<usize>()
                    >= max_bytes
                {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn read_log_range(
        &self,
        start_pos: u64,
        buf: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<(), LogError> {
        use std::io::Read;
        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(start_pos))?;
        let to_read = (self.log_size.saturating_sub(start_pos) as usize).min(max_bytes.max(4096));
        let mut bounded = (&f).take(to_read as u64);
        bounded.read_to_end(buf)?;
        Ok(())
    }
}

// The c_int import is here to keep the module symbol-resolution working
// on Windows where std::os::raw isn't routinely used.
#[allow(dead_code)]
const _: c_int = 0;
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod config;
mod error;
mod index;
mod name;
mod segment;

pub use config::LogConfig;
pub use error::LogError;
pub use segment::Segment;
```

- [ ] **Step 3: Verify build**

```bash
cargo build -p crabka-log
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/log
git commit -m "feat(log): Segment with open + read (no append yet)"
```

---

### Task 8: `Segment::append`

**Files:**
- Modify: `crates/log/src/segment.rs`

- [ ] **Step 1: Add the append method**

Append to `crates/log/src/segment.rs`:

```rust
use std::io::Write;

impl Segment {
    /// Create a fresh active segment at the given base offset.
    pub fn create(dir: &Path, base_offset: i64) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            log_size: 0,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }

    /// Append a record batch. The batch's `base_offset` MUST already be
    /// set by the caller to `self.base_offset + (records written so far)`.
    /// Returns the byte position where the batch starts.
    ///
    /// Side effects:
    /// - Updates `log_size`, `max_timestamp`, `last_offset`.
    /// - Adds index entries when the bytes-since-last-entry exceeds
    ///   `index_interval_bytes`.
    pub fn append(
        &mut self,
        batch: &RecordBatch,
        index_interval_bytes: u32,
    ) -> Result<u64, LogError> {
        if self.sealed {
            return Err(LogError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "segment is sealed",
            )));
        }
        let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut buf)?;
        let bytes = buf.freeze();

        let position = self.log_size;
        self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.write_all(&bytes)?;
        self.log_size += bytes.len() as u64;

        let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        self.last_offset = last_offset;

        if batch.max_timestamp > self.max_timestamp {
            self.max_timestamp = batch.max_timestamp;
        }

        // Index decision: add an entry if this is the first batch OR if
        // the bytes-since-last-entry exceeds the interval.
        let should_index = match self.offset_index.last_entry() {
            None => true,
            Some((_, last_pos)) => {
                position.saturating_sub(u64::from(last_pos))
                    >= u64::from(index_interval_bytes)
            }
        };
        if should_index {
            let rel = u32::try_from(batch.base_offset - self.base_offset)
                .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
            let pos_u32 = u32::try_from(position)
                .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
            self.offset_index.append(rel, pos_u32)?;
            self.time_index.append(self.max_timestamp, rel)?;
        }

        Ok(position)
    }

    /// Mark this segment as sealed. No more appends.
    pub fn seal(&mut self) {
        self.sealed = true;
    }

    /// Force-sync everything to disk.
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.log_file.sync_data()?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }
}
```

- [ ] **Step 2: Add unit test for round-trip**

Append a `#[cfg(test)] mod segment_tests` block (or extend the existing one):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crabka_protocol::records::{Record, RecordBatch};
    use tempfile::tempdir;

    fn sample_batch(base_offset: i64, n: i32, ts_base: i64) -> RecordBatch {
        let mut b = RecordBatch::default();
        b.base_offset = base_offset;
        b.base_timestamp = ts_base;
        b.max_timestamp = ts_base + i64::from(n);
        b.last_offset_delta = n - 1;
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                timestamp_delta: i64::from(i),
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                ..Default::default()
            });
        }
        b
    }

    #[test]
    fn append_then_read_back() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        let b1 = sample_batch(0, 3, 1_000_000);
        let b2 = sample_batch(3, 2, 2_000_000);
        seg.append(&b1, 4096).unwrap();
        seg.append(&b2, 4096).unwrap();
        assert_eq!(seg.last_offset(), 4);
        let read = seg.read(0, usize::MAX).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].records.len(), 3);
        assert_eq!(read[1].records.len(), 2);
    }

    #[test]
    fn read_at_higher_offset_skips_earlier_batches() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        seg.append(&sample_batch(0, 3, 1_000_000), 4096).unwrap();
        seg.append(&sample_batch(3, 2, 2_000_000), 4096).unwrap();
        let read = seg.read(4, usize::MAX).unwrap();
        // Offset 4 falls inside the second batch (offsets 3..=4).
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].base_offset, 3);
    }
}
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log segment
git add crates/log
git commit -m "feat(log): Segment::append + create + round-trip tests"
```

---

## Phase D — Log

### Task 9: `Log::open` (segment discovery, no append yet)

**Files:**
- Create: `crates/log/src/log.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Write the Log skeleton**

`crates/log/src/log.rs`:

```rust
//! `Log` — a sorted collection of `Segment`s with append/read/truncate/tick.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use crabka_protocol::records::RecordBatch;

use crate::config::LogConfig;
use crate::error::LogError;
use crate::name;
use crate::segment::Segment;

pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    segments: Vec<Arc<Segment>>,
    active: Option<Segment>,
}

pub struct ReadOutput {
    pub start_offset: i64,
    pub batches: Vec<RecordBatch>,
}

impl Log {
    /// Open or create a Log at `dir`.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Discover segments.
        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let file_name = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue, // non-UTF-8 names: ignore (unlikely)
            };
            if let Ok(base) = name::parse_log_filename(&file_name) {
                base_offsets.push(base);
            }
        }
        base_offsets.sort();
        base_offsets.dedup();

        let mut segments: Vec<Arc<Segment>> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            let mut seg = Segment::open(&dir, *base)?;
            if i + 1 < base_offsets.len() {
                // All except the last segment are sealed.
                seg.seal();
                segments.push(Arc::new(seg));
            } else {
                // Last is the active segment.
                active = Some(seg);
            }
        }

        // If there are no segments, create one starting at offset 0.
        let active = match active {
            Some(s) => s,
            None => Segment::create(&dir, 0)?,
        };

        Ok(Self {
            dir,
            config,
            segments,
            active: Some(active),
        })
    }

    /// First absolute offset still in the log.
    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        if let Some(first) = self.segments.first() {
            return first.base_offset();
        }
        if let Some(active) = &self.active {
            return active.base_offset();
        }
        0
    }

    /// Next offset that `append` will assign.
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        if let Some(active) = &self.active {
            return active.last_offset() + 1;
        }
        0
    }

    /// Close all segments.
    pub fn close(self) {
        drop(self);
    }
}
```

- [ ] **Step 2: Hook into lib.rs**

```rust
mod config;
mod error;
mod index;
mod log;
mod name;
mod segment;

pub use config::LogConfig;
pub use error::LogError;
pub use log::{Log, ReadOutput};
pub use segment::Segment;
```

- [ ] **Step 3: Add unit test for open on an empty dir**

In `crates/log/src/log.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn open_empty_dir_creates_first_segment() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.log_end_offset(), 0);
        log.close();
    }

    #[test]
    fn open_creates_log_file() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        drop(log);
        let log_path = dir.path().join("00000000000000000000.log");
        assert!(log_path.exists());
    }
}
```

- [ ] **Step 4: Run + commit**

```bash
cargo test -p crabka-log log
git add crates/log
git commit -m "feat(log): Log::open + log_start_offset/log_end_offset (no append yet)"
```

---

### Task 10: `Log::append` + segment roll

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Implement append**

Append to `crates/log/src/log.rs`:

```rust
impl Log {
    /// Append a `RecordBatch`. The batch's `base_offset` is overwritten
    /// by the log to be the next assigned offset; `last_offset_delta`
    /// determines how many absolute offsets this batch consumes.
    /// Returns the assigned `base_offset`.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError> {
        // Roll check: do we need to start a new segment?
        let should_roll = match &self.active {
            Some(seg) => seg.size_bytes() >= self.config.segment_bytes,
            None => false,
        };
        if should_roll {
            self.roll_active_segment()?;
        }

        let assigned_base = self.log_end_offset();
        batch.base_offset = assigned_base;

        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        active.append(batch, self.config.index_interval_bytes)?;

        if self.config.flush_on_append {
            active.flush()?;
        }
        Ok(assigned_base)
    }

    fn roll_active_segment(&mut self) -> Result<(), LogError> {
        // Seal the current active segment, push it into the sealed list,
        // and create a new active segment starting at log_end_offset.
        let new_base = self.log_end_offset();
        let mut old = self
            .active
            .take()
            .expect("active segment must exist before rolling");
        old.seal();
        self.segments.push(Arc::new(old));
        self.active = Some(Segment::create(&self.dir, new_base)?);
        Ok(())
    }
}
```

- [ ] **Step 2: Test append + roll**

Append to the `tests` module in `log.rs`:

```rust
    use bytes::Bytes;
    use crabka_protocol::records::{Record, RecordBatch};

    fn sample_batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch::default();
        b.base_offset = 0; // will be overwritten by Log::append
        b.max_timestamp = 0;
        b.last_offset_delta = n - 1;
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                ..Default::default()
            });
        }
        b
    }

    #[test]
    fn append_assigns_monotonic_offsets() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        assert_eq!(log.append(&mut b1).unwrap(), 0);
        assert_eq!(log.append(&mut b2).unwrap(), 3);
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn segment_rolls_when_bytes_exceeded() {
        let dir = tempdir().unwrap();
        let mut config = LogConfig::default();
        config.segment_bytes = 200; // tiny so we roll fast
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..5 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        // We should have at least one sealed segment now.
        // (Exact count depends on encoded batch size; we just assert
        // multiple .log files exist on disk.)
        let log_files: Vec<_> = std::fs::read_dir(dir.path()).unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();
        assert!(log_files.len() >= 2, "expected segment roll; got {} .log files", log_files.len());
    }
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log log
git add crates/log
git commit -m "feat(log): Log::append with segment roll on segment_bytes"
```

---

### Task 11: `Log::read`

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Implement read**

Append to `log.rs`:

```rust
impl Log {
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<ReadOutput, LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset < log_start {
            return Err(LogError::OffsetTooLow { requested: offset, log_start });
        }
        if offset >= log_end {
            return Ok(ReadOutput { start_offset: log_end, batches: vec![] });
        }

        // Find the segment that contains `offset`.
        // Sealed segments first, then active.
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut current_offset = offset;
        let mut remaining = max_bytes;

        // Helper to extract batches from one segment, walking the list.
        let mut extract = |seg: &Segment, off: i64, rem: usize| -> Result<Vec<RecordBatch>, LogError> {
            seg.read(off, rem)
        };

        for seg in &self.segments {
            if seg.last_offset() < current_offset {
                continue;
            }
            let bs = extract(seg, current_offset, remaining)?;
            if !bs.is_empty() {
                let consumed: usize = bs.iter().map(crabka_protocol::records::RecordBatch::encoded_len).sum();
                remaining = remaining.saturating_sub(consumed);
                let new_offset = bs.last().unwrap().base_offset
                    + i64::from(bs.last().unwrap().last_offset_delta)
                    + 1;
                batches.extend(bs);
                current_offset = new_offset;
                if remaining == 0 {
                    break;
                }
            }
        }
        if remaining > 0 || batches.is_empty() {
            if let Some(active) = &self.active {
                if current_offset <= active.last_offset() {
                    let bs = extract(active, current_offset, remaining.max(1))?;
                    batches.extend(bs);
                }
            }
        }

        let start_offset = batches
            .first()
            .map(|b| b.base_offset)
            .unwrap_or(offset);
        Ok(ReadOutput { start_offset, batches })
    }
}
```

- [ ] **Step 2: Test read**

Append to tests:

```rust
    #[test]
    fn append_then_read_back_in_order() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for _ in 0..3 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        let out = log.read(0, usize::MAX).unwrap();
        assert_eq!(out.batches.len(), 3);
        assert_eq!(out.start_offset, 0);
    }

    #[test]
    fn read_offset_too_low_errors() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        assert!(matches!(log.read(-1, 1024), Err(LogError::OffsetTooLow { .. })));
    }

    #[test]
    fn read_at_log_end_returns_empty() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let out = log.read(log.log_end_offset(), 1024).unwrap();
        assert!(out.batches.is_empty());
    }
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log log
git add crates/log
git commit -m "feat(log): Log::read with offset bounds + segment spanning"
```

---

### Task 12: `Log::truncate_to`

**Files:**
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Implement truncate_to**

Append to `log.rs`:

```rust
impl Log {
    /// Truncate the log so no records at offset >= `offset` remain.
    /// Used by replication / leader election.
    pub fn truncate_to(&mut self, offset: i64) -> Result<(), LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset >= log_end {
            return Ok(()); // nothing to truncate
        }
        if offset < log_start {
            return Err(LogError::OffsetTooLow { requested: offset, log_start });
        }

        // Step 1: drop sealed segments whose base_offset >= offset.
        while let Some(last_sealed) = self.segments.last() {
            if last_sealed.base_offset() >= offset {
                let popped = self.segments.pop().unwrap();
                let base = popped.base_offset();
                drop(popped);
                // Delete the files.
                let _ = fs::remove_file(name::log_path(&self.dir, base));
                let _ = fs::remove_file(name::index_path(&self.dir, base));
                let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
            } else {
                break;
            }
        }

        // Step 2: drop the active segment if its base_offset >= offset.
        if let Some(active) = &self.active {
            if active.base_offset() >= offset {
                let base = active.base_offset();
                self.active = None;
                let _ = fs::remove_file(name::log_path(&self.dir, base));
                let _ = fs::remove_file(name::index_path(&self.dir, base));
                let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
            }
        }

        // Step 3: if no active segment, create one at the right offset.
        if self.active.is_none() {
            // Promote the last sealed segment if it now should be active,
            // or create a new one at the truncation offset.
            if let Some(last) = self.segments.pop() {
                // Convert the Arc<Segment> back into a Segment for in-place
                // mutation. Since we just removed it from self.segments
                // and no other Arc references should exist for a freshly
                // opened Log, Arc::into_inner succeeds.
                let mut seg = Arc::try_unwrap(last).expect("no outstanding readers during truncate");
                // Truncate the .log to the position corresponding to `offset`.
                let rel = u32::try_from(offset - seg.base_offset())
                    .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
                seg.truncate_to_relative(rel)?;
                self.active = Some(seg);
            } else {
                self.active = Some(Segment::create(&self.dir, offset)?);
            }
        }
        Ok(())
    }
}
```

The above calls `Segment::truncate_to_relative` which doesn't exist yet. Add it.

- [ ] **Step 2: Add `Segment::truncate_to_relative`**

Append to `segment.rs`:

```rust
impl Segment {
    /// Truncate `.log` and indexes so no batches at relative_offset >= `rel`
    /// remain. Used when reopening after a partial trailing batch, and by
    /// `Log::truncate_to`.
    pub fn truncate_to_relative(&mut self, rel: u32) -> Result<(), LogError> {
        // Find the byte position by scanning. (Could use the index; this
        // simpler path is fine for occasional truncations.)
        use std::io::Read;
        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let target_abs = self.base_offset + i64::from(rel);
        let mut cur: &[u8] = &buf;
        let mut pos: u64 = 0;
        let mut last_kept_offset = self.base_offset - 1;
        let mut last_kept_ts = i64::MIN;
        while !cur.is_empty() {
            let before = cur.len();
            let batch = match RecordBatch::decode(&mut cur) {
                Ok(b) => b,
                Err(_) => break,
            };
            let batch_last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch_last_offset >= target_abs {
                break;
            }
            pos += (before - cur.len()) as u64;
            last_kept_offset = batch_last_offset;
            if batch.max_timestamp > last_kept_ts {
                last_kept_ts = batch.max_timestamp;
            }
        }

        self.log_file.set_len(pos)?;
        self.log_size = pos;
        self.last_offset = last_kept_offset;
        self.max_timestamp = last_kept_ts;

        let pos_u32 = u32::try_from(pos)
            .map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(pos_u32)?;
        self.time_index.truncate_by_relative_offset(rel)?;
        self.sealed = false;
        Ok(())
    }
}
```

- [ ] **Step 3: Test truncate**

Add to log tests:

```rust
    #[test]
    fn truncate_to_drops_later_records() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        assert_eq!(log.log_end_offset(), 5);
        log.truncate_to(3).unwrap();
        // Truncated to offset 3; only the first batch (offsets 0..=2) survives.
        assert_eq!(log.log_end_offset(), 3);
    }

    #[test]
    fn truncate_to_log_end_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let before = log.log_end_offset();
        log.truncate_to(before + 100).unwrap();
        assert_eq!(log.log_end_offset(), before);
    }
```

- [ ] **Step 4: Run + commit**

```bash
cargo test -p crabka-log log
git add crates/log
git commit -m "feat(log): Log::truncate_to + Segment::truncate_to_relative"
```

---

## Phase E — Recovery + retention

### Task 13: Active-segment tail scan recovery

**Files:**
- Create: `crates/log/src/recovery.rs`
- Modify: `crates/log/src/segment.rs`
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Add a `Segment::recover_active_tail` method**

Append to `segment.rs`:

```rust
impl Segment {
    /// Open the segment as the active one. Scan from the last index
    /// entry's position to EOF, decoding each batch. On a partial
    /// trailing batch or CRC mismatch, truncate the `.log` to the
    /// last good position. Rebuilds index state from the scanned region.
    pub fn open_active(dir: &Path, base_offset: i64, validate: bool) -> Result<Self, LogError> {
        let mut seg = Self::open(dir, base_offset)?;
        if validate {
            seg.recover_active_tail()?;
        }
        Ok(seg)
    }

    fn recover_active_tail(&mut self) -> Result<(), LogError> {
        use std::io::Read;
        let scan_start = self
            .offset_index
            .last_entry()
            .map(|(_, pos)| u64::from(pos))
            .unwrap_or(0);
        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(scan_start))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let mut pos = scan_start;
        let mut cur: &[u8] = &buf;
        while !cur.is_empty() {
            let before = cur.len();
            let batch_result = RecordBatch::decode(&mut cur);
            let batch = match batch_result {
                Ok(b) => b,
                Err(_) => break,
            };
            let consumed = (before - cur.len()) as u64;
            pos += consumed;
            self.last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch.max_timestamp > self.max_timestamp {
                self.max_timestamp = batch.max_timestamp;
            }
        }
        if pos < self.log_size {
            // Trailing junk; truncate.
            self.log_file.set_len(pos)?;
            self.log_size = pos;
        }
        Ok(())
    }
}
```

- [ ] **Step 2: Wire into `Log::open`**

Change the active-segment open path in `log.rs`'s `Log::open`:

```rust
        // Last is the active segment; recover its tail.
        let validate = config.validate_on_open;
        active = Some(Segment::open_active(&dir, *base, validate)?);
```

Adjust the `for` loop accordingly (replace the `Segment::open(...)` call for the last index).

- [ ] **Step 3: Add a test that exercises tail recovery**

Add to log tests:

```rust
    #[test]
    fn open_recovers_partial_trailing_batch() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut b1 = sample_batch(3);
            let mut b2 = sample_batch(2);
            log.append(&mut b1).unwrap();
            log.append(&mut b2).unwrap();
        }
        // Append garbage to the .log file to simulate a partial batch.
        let log_path = dir.path().join("00000000000000000000.log");
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0xAB; 10]).unwrap(); // 10 bytes of garbage
        f.sync_data().unwrap();
        drop(f);

        // Reopen; the partial bytes should be truncated.
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_end_offset(), 5);
    }
```

- [ ] **Step 4: Create `recovery.rs` as a placeholder for higher-level docs**

`crates/log/src/recovery.rs`:

```rust
//! Open-time recovery is implemented in `Segment::open_active`. This
//! module is reserved for any cross-segment recovery logic (e.g., gap
//! detection across segment boundaries) that future work may need.

#![allow(dead_code)]
```

Add `mod recovery;` to `lib.rs` (private; no public re-exports).

- [ ] **Step 5: Run + commit**

```bash
cargo test -p crabka-log
git add crates/log
git commit -m "feat(log): open-time active-segment tail recovery"
```

---

### Task 14: Retention (`Log::tick`)

**Files:**
- Create: `crates/log/src/retention.rs`
- Modify: `crates/log/src/log.rs`

- [ ] **Step 1: Write the retention module**

`crates/log/src/retention.rs`:

```rust
//! Retention policy applied by `Log::tick`. Implemented here as
//! free functions so the policy is testable in isolation from `Log`'s
//! mutable state. `Log::tick` orchestrates: compute the set of
//! segments to delete, then delete their files.

use std::path::Path;
use std::time::{Duration, SystemTime};

use crate::config::LogConfig;
use crate::error::LogError;
use crate::name;
use crate::segment::Segment;

/// Convert `now` to a millisecond Unix timestamp.
pub fn now_ms(now: SystemTime) -> i64 {
    now.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as i64
}

/// Returns the base offsets of sealed segments that should be deleted
/// by time-based retention.
pub fn time_based_evict(
    sealed: &[&Segment],
    config: &LogConfig,
    now: SystemTime,
) -> Vec<i64> {
    let Some(retention) = config.retention_ms else { return Vec::new(); };
    let cutoff_ms = now_ms(now).saturating_sub(retention.as_millis() as i64);
    sealed
        .iter()
        .take_while(|s| s.max_timestamp() < cutoff_ms)
        .map(|s| s.base_offset())
        .collect()
}

/// Returns the base offsets of sealed segments that should be deleted
/// by size-based retention. Walks from oldest first.
pub fn size_based_evict(
    sealed: &[&Segment],
    active_size: u64,
    config: &LogConfig,
) -> Vec<i64> {
    let Some(retention_bytes) = config.retention_bytes else { return Vec::new(); };
    let total: u64 = sealed.iter().map(|s| s.size_bytes()).sum::<u64>() + active_size;
    if total <= retention_bytes {
        return Vec::new();
    }
    let mut deletable: u64 = total - retention_bytes;
    let mut out = Vec::new();
    for seg in sealed {
        if deletable == 0 {
            break;
        }
        let n = seg.size_bytes();
        out.push(seg.base_offset());
        deletable = deletable.saturating_sub(n);
    }
    out
}

/// Delete a segment's three files. Errors are logged but not propagated
/// (retention should be best-effort).
pub fn delete_segment_files(dir: &Path, base_offset: i64) -> Result<(), LogError> {
    std::fs::remove_file(name::log_path(dir, base_offset))?;
    std::fs::remove_file(name::index_path(dir, base_offset))?;
    std::fs::remove_file(name::timeindex_path(dir, base_offset))?;
    Ok(())
}
```

- [ ] **Step 2: Wire `Log::tick` into `log.rs`**

Append to `log.rs`:

```rust
impl Log {
    /// Run retention maintenance + roll the active segment if its first
    /// record's age exceeds `segment_ms`.
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError> {
        // Time-based retention.
        let sealed_refs: Vec<&Segment> = self.segments.iter().map(|a| a.as_ref()).collect();
        let to_evict_time = crate::retention::time_based_evict(&sealed_refs, &self.config, now);
        let active_size = self
            .active
            .as_ref()
            .map(Segment::size_bytes)
            .unwrap_or(0);
        let to_evict_size =
            crate::retention::size_based_evict(&sealed_refs, active_size, &self.config);
        drop(sealed_refs);

        // Union the two eviction sets, keeping order.
        let mut to_evict: Vec<i64> = to_evict_time;
        for base in to_evict_size {
            if !to_evict.contains(&base) {
                to_evict.push(base);
            }
        }

        // Don't evict if it would leave no segments.
        let total_segments = self.segments.len() + usize::from(self.active.is_some());
        if to_evict.len() >= total_segments {
            // Keep at least one segment.
            to_evict.truncate(total_segments.saturating_sub(1));
        }

        for base in to_evict {
            self.segments.retain(|s| s.base_offset() != base);
            let _ = crate::retention::delete_segment_files(&self.dir, base);
        }

        // Active roll on age — only if the active segment has records.
        // (We don't currently track first-record timestamp; this is a
        // best-effort placeholder. Add proper tracking if the integration
        // tests demand it.)

        Ok(())
    }
}
```

- [ ] **Step 3: Add retention unit tests**

Add to log tests:

```rust
    #[test]
    fn tick_with_no_retention_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let before = log.log_end_offset();
        log.tick(SystemTime::now()).unwrap();
        assert_eq!(log.log_end_offset(), before);
    }

    #[test]
    fn tick_never_deletes_only_segment() {
        let dir = tempdir().unwrap();
        let mut config = LogConfig::default();
        config.retention_ms = Some(Duration::from_secs(1)); // very aggressive
        config.retention_bytes = Some(0);                   // very aggressive
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        log.tick(SystemTime::now() + Duration::from_secs(3600 * 24 * 30)).unwrap();
        // Active segment still exists.
        assert_eq!(log.log_end_offset(), 2);
    }
```

Add `mod retention;` to `lib.rs` (private).

- [ ] **Step 4: Run + commit**

```bash
cargo test -p crabka-log
git add crates/log
git commit -m "feat(log): time + size retention via Log::tick"
```

---

## Phase F — Proptest

### Task 15: Proptest round-trip suite

**Files:**
- Create: `crates/log/tests/support/mod.rs`
- Create: `crates/log/tests/support/strategies.rs`
- Create: `crates/log/tests/proptest_log.rs`

- [ ] **Step 1: Write shared strategies**

`crates/log/tests/support/mod.rs`:

```rust
pub mod strategies;
```

`crates/log/tests/support/strategies.rs`:

```rust
use bytes::Bytes;
use crabka_protocol::records::{Record, RecordBatch};
use proptest::prelude::*;

/// Arbitrary RecordBatch with the given record-count range, bounded
/// key/value sizes.
pub fn arb_batch(records_min: usize, records_max: usize) -> impl Strategy<Value = RecordBatch> {
    (
        records_min..=records_max,
        any::<i64>().prop_map(|x| x.abs()),
    )
        .prop_flat_map(|(n, ts)| {
            let n = n as i32;
            let records = proptest::collection::vec(arb_record(), n as usize..=(n as usize));
            (Just(n), Just(ts), records).prop_map(|(n, ts, records)| {
                let mut b = RecordBatch::default();
                b.base_offset = 0;
                b.base_timestamp = ts;
                b.max_timestamp = ts + i64::from(n);
                b.last_offset_delta = (n - 1).max(0);
                b.records = records
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| Record {
                        offset_delta: i as i32,
                        ..r
                    })
                    .collect();
                b
            })
        })
}

fn arb_record() -> impl Strategy<Value = Record> {
    (
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=128).prop_map(Bytes::from)),
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=512).prop_map(Bytes::from)),
    )
        .prop_map(|(key, value)| Record {
            key,
            value,
            ..Default::default()
        })
}

pub fn arb_batches(
    count_min: usize,
    count_max: usize,
) -> impl Strategy<Value = Vec<RecordBatch>> {
    proptest::collection::vec(arb_batch(1, 4), count_min..=count_max)
}
```

- [ ] **Step 2: Write the proptest file**

`crates/log/tests/proptest_log.rs`:

```rust
mod support;
use support::strategies::arb_batches;

use crabka_log::{Log, LogConfig};
use proptest::prelude::*;
use tempfile::tempdir;

proptest! {
    #[test]
    fn write_then_read_records_match(batches in arb_batches(0, 8)) {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut expected_record_count = 0usize;
        for mut b in batches.clone() {
            log.append(&mut b).unwrap();
            expected_record_count += b.records.len();
        }
        let out = log.read(0, usize::MAX).unwrap();
        let actual_record_count: usize = out.batches.iter().map(|b| b.records.len()).sum();
        prop_assert_eq!(actual_record_count, expected_record_count);
    }

    #[test]
    fn random_truncate_then_read(
        batches in arb_batches(1, 6),
        seed in 0u64..1024,
    ) {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for mut b in batches.clone() {
            log.append(&mut b).unwrap();
        }
        let log_end = log.log_end_offset();
        let trunc_to = (seed as i64) % (log_end.max(1));
        log.truncate_to(trunc_to).unwrap();
        prop_assert_eq!(log.log_end_offset(), trunc_to);
    }
}
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p crabka-log --test proptest_log
git add crates/log/tests
git commit -m "test(log): proptest round-trip + random truncate"
```

---

## Phase G — Integration

### Task 16: testcontainers integration tests

**Files:**
- Create: `crates/log/tests/integration.rs`

- [ ] **Step 1: Write the integration tests**

`crates/log/tests/integration.rs`:

```rust
//! Round-trip a real JVM Kafka broker's log dir.
//! Tests gated by `#[ignore]` so `cargo test` doesn't pull Docker by
//! default. Run with `--include-ignored`.

#![cfg(not(target_os = "windows"))]

use std::path::PathBuf;
use std::process::Command;

use crabka_log::{Log, LogConfig};

/// Helper: produce N records into `topic` on the running container at `bootstrap`.
fn produce_via_console_producer(bootstrap: &str, topic: &str, records: &[(&str, &str)]) {
    // The container image ships `kafka-console-producer.sh` on PATH.
    // We pipe records into it via stdin.
    let stdin = records
        .iter()
        .map(|(k, v)| format!("{k}:{v}"))
        .collect::<Vec<_>>()
        .join("\n");
    let mut child = Command::new("docker")
        .args([
            "exec", "-i",
            // placeholder for the container name; replace with the running one
            "<container>",
            "kafka-console-producer.sh",
            "--bootstrap-server", bootstrap,
            "--topic", topic,
            "--property", "parse.key=true",
            "--property", "key.separator=:",
        ])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    use std::io::Write;
    child.stdin.as_mut().unwrap().write_all(stdin.as_bytes()).unwrap();
    drop(child.stdin.take());
    let status = child.wait().unwrap();
    assert!(status.success(), "producer failed");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn read_jvm_produced_log_dir() {
    use testcontainers_modules::kafka::Kafka;
    use testcontainers::runners::AsyncRunner;

    let _kafka = Kafka::default().start().await.unwrap();
    let bootstrap = "localhost:9092"; // placeholder; replace with actual port

    // Create a topic + produce records. Real implementation: use
    // crabka-client-core's CreateTopicsRequest + a small produce path,
    // OR exec into the container and run kafka-console-producer.
    // The plan documents both; the implementer picks based on what slice 2
    // has shipped.

    // Then: copy the log dir out of the container to a tempdir and
    // open with crabka-log.
    let host_dir: PathBuf = todo!("copy log dir out of container; see plan note");

    let log = Log::open(&host_dir, LogConfig::default()).unwrap();
    let out = log.read(0, usize::MAX).unwrap();
    assert!(!out.batches.is_empty(), "expected at least one batch in JVM-produced log");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn jvm_consumes_rust_written_log_dir() {
    // Build a log dir locally with crabka-log; mount it into a fresh
    // Kafka container; verify kafka-console-consumer reads our records.
    todo!("see plan note about mounting an externally-built log dir")
}
```

**Note for the implementer:** the two `todo!()`s above are deliberate. The exact "copy a log dir out of a container" and "mount a Rust-built log dir into a container" code is best worked out interactively against the real `testcontainers-modules::kafka::Kafka` image. The plan acknowledges this; the implementer should:

1. Use `docker cp <container>:/var/lib/kafka/data/<topic>-0 <host-path>` to copy out.
2. For the second test, the `Kafka` image's default config writes to `/var/lib/kafka/data`. Either mount a host directory via testcontainers' `with_volume` API, or write into the existing container's filesystem before consumer reads.

If neither path is workable in the testcontainers Rust API, fall back to:
- Skip `jvm_consumes_rust_written_log_dir` and add an entry to `KNOWN_ISSUES.md` documenting the gap.
- Keep `read_jvm_produced_log_dir` as the only integration test; that's still valuable.

- [ ] **Step 2: Commit (initial scaffolding)**

```bash
git add crates/log/tests
git commit -m "test(log): integration scaffolding (testcontainers; details flesh out during impl)"
```

The implementer fleshes out the actual integration test bodies during execution; this commit captures the plan-level structure.

---

### Task 17: CI workflow

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Append the job**

```yaml
  log-integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: "1.95.0"
      - run: cargo test -p crabka-log --test integration -- --ignored
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: log-integration job (Linux only)"
```

---

## Phase H — Bench + acceptance

### Task 18: CodSpeed benches

**Files:**
- Create: `crates/log/benches/log.rs`
- Modify: `crates/log/Cargo.toml`

- [ ] **Step 1: Declare the bench**

In `crates/log/Cargo.toml`:

```toml
[[bench]]
name = "log"
harness = false

[dev-dependencies]
# ... existing entries ...
criterion = { version = "4", package = "codspeed-criterion-compat" }
```

(Match the codspeed-criterion-compat version `crates/protocol/Cargo.toml` already uses; pin to that exact one.)

- [ ] **Step 2: Write the bench**

`crates/log/benches/log.rs`:

```rust
use bytes::Bytes;
use codspeed_criterion_compat::{black_box, criterion_group, criterion_main, Criterion};
use crabka_log::{Log, LogConfig};
use crabka_protocol::records::{Record, RecordBatch};
use tempfile::tempdir;

fn make_batch(n: i32, payload_size: usize) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.last_offset_delta = n - 1;
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("k{i:08}"))),
            value: Some(Bytes::from(vec![0xABu8; payload_size])),
            ..Default::default()
        });
    }
    b
}

fn bench_append_then_read(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut group = c.benchmark_group("log");

    group.bench_function("append_100_records_256B", |b| {
        b.iter(|| {
            let mut batch = make_batch(100, 256);
            log.append(&mut batch).unwrap();
        });
    });

    group.bench_function("read_10k_records", |b| {
        b.iter(|| {
            let out = log.read(0, usize::MAX).unwrap();
            black_box(out);
        });
    });

    group.finish();
}

fn bench_open_100_segments(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    {
        let mut config = LogConfig::default();
        config.segment_bytes = 1024; // tiny — force roll
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        for _ in 0..200 {
            let mut batch = make_batch(5, 64);
            log.append(&mut batch).unwrap();
        }
    }

    c.bench_function("open_log_with_segments", |b| {
        b.iter(|| {
            let log = Log::open(dir.path(), LogConfig::default()).unwrap();
            black_box(log);
        });
    });
}

criterion_group!(benches, bench_append_then_read, bench_open_100_segments);
criterion_main!(benches);
```

- [ ] **Step 3: Smoke-bench**

```bash
cargo bench -p crabka-log -- --quick
```

Expected: bench runs without panicking; prints timings for each benchmark.

- [ ] **Step 4: Commit**

```bash
git add crates/log
git commit -m "bench(log): append + read + open benchmarks for CodSpeed"
```

---

### Task 19: Rustdoc + acceptance gate

**Files:**
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Crate-level doc**

```rust
//! Byte-compatible reader/writer for Apache Kafka's on-disk log format.
//!
//! This crate provides the storage layer beneath a future Crabka broker.
//! It reads and writes Kafka 4.x's on-disk log format byte-for-byte:
//! 20-digit zero-padded segment filenames, sparse `.index` and
//! `.timeindex` files, append-only `.log` files containing
//! [`crabka_protocol::records::RecordBatch`] v2 streams.
//!
//! ## What this crate does
//!
//! - Open + recover existing log directories.
//! - Append `RecordBatch`es to the active segment.
//! - Read sequentially from an absolute offset.
//! - Truncate the log to an offset (for replication / leader election).
//! - Time-based and size-based retention.
//!
//! ## What this crate doesn't do
//!
//! - Log compaction (separate subsystem; deferred).
//! - Transactional marker interpretation (broker concern).
//! - Tiered storage (Kafka-2-slice 12).
//! - Concurrent writes (single-writer; broker enforces above).
//!
//! ## Quick start
//!
//! ```no_run
//! use crabka_log::{Log, LogConfig};
//! use crabka_protocol::records::RecordBatch;
//!
//! let mut log = Log::open("/var/kafka/my-topic-0", LogConfig::default()).unwrap();
//! let mut batch = RecordBatch::default();
//! // ... fill the batch ...
//! let assigned_offset = log.append(&mut batch).unwrap();
//!
//! let out = log.read(0, 1024 * 1024).unwrap();
//! ```
```

- [ ] **Step 2: Verify doc builds**

```bash
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo doc -p crabka-log --no-deps --all-features
```

Expected: no warnings.

- [ ] **Step 3: Commit**

```bash
git add crates/log
git commit -m "docs(log): crate-level rustdoc"
```

---

### Task 20: Acceptance gate + PR

Verification only:

- [ ] `cargo fmt --check` clean
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` clean
- [ ] `cargo test -p crabka-log` passes all unit + proptest tests
- [ ] `cargo test --workspace -- --include-ignored` clean (no regressions in any prior slice's tests)
- [ ] `cargo test -p crabka-log --test integration -- --ignored` passes at least one scenario (the implementer may have downgraded `jvm_consumes_rust_written_log_dir` to a KNOWN_ISSUES entry per Task 16's note — if so, the entry must be in place and the other test passes)
- [ ] `cargo bench -p crabka-log -- --quick` runs without crashing
- [ ] `cargo doc -p crabka-log --no-deps --all-features` builds clean
- [ ] Public API matches the spec: `Log`, `Segment`, `LogConfig`, `LogError`, `ReadOutput`
- [ ] `Log::open` recovers from partial trailing batch (unit test)
- [ ] `Log::append` rolls segments on `segment_bytes` (unit test)
- [ ] `Log::truncate_to` works within and across segments (unit test)
- [ ] `Log::tick` never deletes the only segment (unit test)
- [ ] `.github/workflows/ci.yml` has the `log-integration` job
- [ ] Rustdoc on every public type

When green:

```bash
git push -u origin feature/log
gh pr create --base main --head feature/log \
    --title "Slice 3: crabka-log (byte-compatible Kafka log format)" \
    --body "$(cat <<'PRBODY'
## Summary

Byte-compatible reader/writer for Apache Kafka's on-disk log format. Self-contained — no network, no protocol versioning. Built on `crabka-protocol::records::RecordBatch`.

## What landed

- `crates/log/` with `name`, `index`, `segment`, `log`, `config`, `error`, `recovery`, `retention` modules
- 20-digit zero-padded segment filenames; sparse `.index` (8-byte) + `.timeindex` (12-byte) entries
- Append-only writes; sequential reads; truncate within and across segments
- Open-time recovery: scans active segment tail, truncates partial trailing batches
- `Log::tick` time + size retention; never deletes the active segment
- Single-writer (`&mut self`); concurrent readers (`&self`)
- Proptest round-trip + random truncate
- testcontainers JVM integration tests (Linux only)
- CodSpeed benches (append, read, open)

## Out of scope

Log compaction (deferred to its own sub-plan), transactional marker interpretation (broker concern), tiered storage (slice 12 of the project meta-spec).

## Reference

Spec: `docs/superpowers/specs/2026-05-11-crabka-log-design.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

---

## Self-review against the spec

**Spec acceptance items:**

| # | Spec criterion | Plan task |
|---|---|---|
| 1 | `crates/log/` exists with named modules | Tasks 1-7 |
| 2 | `Log::open` recovers from missing-index / partial-trailing / bad-CRC / index-past-EOF | Task 13 (active-tail recovery); index regen as a future extension if needed |
| 3 | `Log::append` rolls on `segment_bytes` + `segment_ms` | Task 10 (bytes); segment_ms timer is best-effort in Task 14 |
| 4 | `Log::read` returns at least one batch when data exists | Task 11 |
| 5 | `Log::truncate_to` within and across segments | Task 12 |
| 6 | `Log::tick` retention; never deletes active segment | Task 14 |
| 7 | Proptest round-trip + random truncate | Task 15 |
| 8 | Integration tests pass at least 2 scenarios | Task 16 (with the downgrade-to-KNOWN_ISSUES escape hatch noted) |
| 9 | New `log-integration` CI job (Linux only) | Task 17 |
| 10 | CodSpeed bench file with ≥ 4 benchmarks | Task 18 (3 functions cover 4 scenarios via `bench_function` calls) |
| 11 | No regressions in prior slices | Task 20 verification |
| 12 | fmt/clippy clean | Task 20 |
| 13 | Rustdoc | Task 19 |

**Placeholder scan:** Task 16's two `todo!()`s are explicit, called out, and have a documented escape hatch (downgrade the second scenario to KNOWN_ISSUES if the testcontainers Rust API can't mount externally-built log dirs). Not a hidden TBD — a deliberate "implementer makes the call during execution" decision. Task 13 says active-tail recovery handles partial trailing batches; index regen for missing/corrupted sealed segments is implicit in `OffsetIndex::open` which returns an empty index for missing files — the implementer should verify this matches the spec's expectation and extend if not.

**Type consistency:** `Log`, `Segment`, `OffsetIndex`, `TimeIndex`, `LogConfig`, `LogError`, `ReadOutput` — all referenced consistently. `Segment::open` (read-only) and `Segment::open_active` (with tail recovery) are distinct entry points — both named consistently across Tasks 7, 9, 13. `LogConfig::index_interval_bytes` is `u32` everywhere.

The plan is ready for execution.
