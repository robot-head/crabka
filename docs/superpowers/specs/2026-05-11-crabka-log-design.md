# `crabka-log` (slice 3) — Design

**Status:** Draft for review
**Date:** 2026-05-11
**Author:** Matthew Stone (with Claude)
**Predecessor:** project meta-spec
(`2026-05-10-crabka-rust-rewrite-design.md`). Slice 1 (`crabka-protocol`
and friends) fully shipped via 1a–1e.

## Summary

`crabka-log` reads and writes the Apache Kafka on-disk log format
byte-compatibly: append-only segments, sparse offset + time indexes,
retention by time and size. Self-contained — no network, no protocol
versioning, just `std::fs` and `std::io`. Built on top of
`crabka-protocol::records::RecordBatch` for batch parsing.

**No log compaction in slice 3.** Compaction is an offline rewriter
process that deserves its own subsystem; deferred to a later sub-plan.

## North star (acceptance gate for slice 3)

1. New crate `crabka-log` exists in the workspace.
2. `Log::open` recovers correctly from common corruption patterns
   (missing index, partial trailing batch, bad CRC, index past EOF).
3. `Log::append` rolls segments per `segment.bytes` and `segment.ms`.
4. `Log::read` returns at least one batch when data is available, even
   if the batch alone exceeds `max_bytes` (Kafka fetch-min-one-record
   semantics).
5. `Log::truncate_to` works within and across segments.
6. `Log::tick` applies time + size retention; never deletes the active
   segment.
7. Integration tests pass against a JVM-broker-written log dir AND
   prove that a Rust-written log dir can be consumed by the JVM
   broker.
8. CodSpeed benches added; no regressions in prior slice tests.

## Non-goals

- **Log compaction.** Its own subsystem; deferred.
- **Transactional marker interpretation.** Records are stored verbatim;
  the broker (slice 4+) interprets attribute bits.
- **Leader-epoch checkpoints, producer-ID snapshots.** Broker-level
  metadata files; out of scope.
- **Tiered storage.** Slice 12.
- **mmap-based reads as a hard requirement.** Optional; `read_at` is
  the default. Plan may add mmap as an optimisation if profiling
  justifies it.
- **Multi-writer concurrency.** Single-writer (the broker enforces
  this above).

---

# 1. On-disk layout

Each partition lives in its own directory containing **segments**.
A segment is three files sharing a base-offset prefix:

```
my-topic-0/
├── 00000000000000000000.log
├── 00000000000000000000.index
├── 00000000000000000000.timeindex
├── 00000000000000001847.log         # next segment, base_offset = 1847
├── 00000000000000001847.index
└── 00000000000000001847.timeindex
```

Filename: 20-digit zero-padded **base offset** (first absolute record
offset in the segment).

### `.log` file

Concatenation of `RecordBatch` v2 byte streams. Each batch begins with
the 12-byte on-disk length prefix Kafka uses (`base_offset:i64 +
batch_length:i32`) followed by the rest of the v2 header and the
record body. This matches the wire format `crabka-protocol::records`
already reads and writes, so the log layer calls into the existing
codec for batch parsing.

### `.index` file (offset index)

Sparse. 8 bytes per entry:

```
relative_offset: u32  (offset from segment's base_offset)
position:        u32  (byte position in the .log file)
```

One entry per ~4 KiB of `.log` data by default (`index.interval.bytes`).
Used to find the byte position to start reading from for a given
absolute offset via binary search.

### `.timeindex` file

Sparse. 12 bytes per entry:

```
timestamp:       i64  (max timestamp at or before the indexed record)
relative_offset: u32
```

Same sparseness as the offset index. Used to answer "what offset
corresponds to time T or later."

### Segment lifecycle

- **Active.** The segment currently being written. Indexes are mutable.
- **Roll.** When the active segment exceeds `segment.bytes` (default
  1 GiB) or its first record's age exceeds `segment.ms` (default 7
  days), roll: close the active segment, start a new one with
  `base_offset = log_end_offset()`.
- **Sealed.** Old segments are immutable. Indexes are read-only.

### Retention

`Log::tick(now)` runs:

1. **Time-based.** Delete sealed segments where
   `max_timestamp + retention.ms < now`.
2. **Size-based.** Delete oldest sealed segments until the total
   `.log` size is ≤ `retention.bytes`.
3. **Never delete the active segment.** Never delete the only segment.

### Out of scope for slice 3

- **Compaction.** Separate subsystem.
- **Transactional markers, leader-epoch checkpoints, producer-ID
  snapshots.** Broker-level concerns.
- **Tiered storage.** Slice 12.
- **mmap as a requirement.** Optional optimisation.

---

# 2. Public API

```rust
// crates/log/src/lib.rs

pub use config::LogConfig;
pub use error::LogError;
pub use log::{Log, ReadOutput};
pub use segment::Segment;
```

### `Log`

```rust
pub struct Log { /* internals */ }

impl Log {
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError>;

    /// Single-writer. Overwrites `batch.base_offset` to the assigned
    /// offset; returns it.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError>;

    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<ReadOutput, LogError>;

    pub fn log_start_offset(&self) -> i64;
    pub fn log_end_offset(&self) -> i64;

    pub fn truncate_to(&mut self, offset: i64) -> Result<(), LogError>;
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError>;
    pub fn flush(&mut self) -> Result<(), LogError>;
    pub fn close(self);
}

pub struct ReadOutput {
    pub start_offset: i64,
    pub batches: Vec<RecordBatch>,
}
```

### `LogConfig`

```rust
pub struct LogConfig {
    pub segment_bytes: u64,             // default 1 GiB
    pub segment_ms: Duration,           // default 7 days
    pub retention_bytes: Option<u64>,   // default None = unlimited
    pub retention_ms: Option<Duration>, // default 7 days
    pub index_interval_bytes: u32,      // default 4 KiB
    pub flush_on_append: bool,          // default false
    pub validate_on_open: bool,         // default true (CRC every batch in active segment)
}
```

Defaults match Kafka 4.2's `log.segment.bytes`, `log.segment.ms`,
`log.retention.bytes`, `log.retention.ms`, `log.index.interval.bytes`.

### `Segment`

```rust
pub struct Segment { /* internals */ }

impl Segment {
    pub fn open(dir: &Path, base_offset: i64) -> Result<Self, LogError>;
    pub fn base_offset(&self) -> i64;
    pub fn last_offset(&self) -> i64;
    pub fn size_bytes(&self) -> u64;
    pub fn max_timestamp(&self) -> i64;
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<RecordBatch>, LogError>;
    pub fn is_sealed(&self) -> bool;
}
```

Exposed for offline log-inspection tools; most users go through `Log`.

### `LogError`

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LogError {
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),

    #[error("partial batch at offset {file_offset} in segment {segment}: truncating")]
    PartialBatch { segment: i64, file_offset: u64 },

    #[error("CRC mismatch at offset {file_offset} in segment {segment}: expected {expected:#x}, computed {computed:#x}")]
    CrcMismatch { segment: i64, file_offset: u64, expected: u32, computed: u32 },

    #[error("offset {requested} below log start {log_start}")]
    OffsetTooLow { requested: i64, log_start: i64 },

    #[error("offset {requested} >= log end {log_end}")]
    OffsetTooHigh { requested: i64, log_end: i64 },

    #[error("records: {0}")]
    Records(#[from] crabka_protocol::records::RecordsError),

    #[error("invalid segment filename: {0}")]
    BadSegmentName(String),
}
```

---

# 3. Recovery, retention, concurrency

### Open-time recovery

`Log::open(dir, config)`:

1. Scan `dir` for `*.log` files; parse filenames as base offsets.
2. Sort segments by base offset ascending.
3. For each segment, open the `.log` and validate the `.index` /
   `.timeindex`. **Sealed segments** get a cheap validation (size + last
   index entry bounded). **Active segment** gets a full scan if
   `validate_on_open` is set.
4. **Active segment tail scan.** From the last index entry's position
   to EOF, decode each batch. Verify CRC. Track `next_offset` and
   `max_timestamp`.
5. **Partial trailing batch** (incomplete or CRC mismatch): truncate
   `.log` to the last good batch's end position. Truncate `.index` and
   `.timeindex` to entries below that position.
6. **Missing or corrupted `.index`/`.timeindex`** on a sealed segment:
   rebuild by scanning the `.log`.
7. Open the active segment's `.log` for append, positioned at end.
   Memory-map the active segment's indexes read-write; sealed
   segments' indexes read-only.

Recovery is single-pass; no separate fsck tool.

### Append path

```
1. Roll check: if size >= segment_bytes OR age >= segment_ms, seal + start new segment.
2. Assign offsets: batch.base_offset = log_end_offset().
3. Encode via crabka_protocol::records::RecordBatch::encode.
4. Write bytes to active .log at current end position.
5. Maybe add (relative_offset, position) to .index and (max_timestamp, relative_offset) to .timeindex if interval exceeded.
6. Update log_end_offset and max_timestamp.
7. If flush_on_append, fsync.
8. Return assigned base_offset.
```

### Read path

```
1. If offset < log_start_offset → OffsetTooLow.
2. If offset >= log_end_offset → empty ReadOutput, not an error.
3. Binary-search segments to find the segment containing offset.
4. Binary-search .index to find the byte position to start at.
5. Decode batches from that position; skip ahead until first batch where last_offset >= offset.
6. Read batches until cumulative size >= max_bytes; at least one batch even if it exceeds.
7. Span segments if necessary.
```

### Retention

`Log::tick(now)`:

1. Force-roll the active segment if its age exceeds `segment_ms` (so
   even idle logs rotate).
2. Time-based deletion: oldest first, while `max_timestamp +
   retention_ms < now`.
3. Size-based deletion: oldest first, while total `.log` size >
   `retention_bytes`.
4. Never delete the active segment; never delete the only segment;
   deletion always from the oldest end.

Idempotent. Brokers call on a schedule; tests call explicitly.

### Concurrency

- **Writes:** `&mut self`. Single-writer enforced above (broker
  partition lock).
- **Reads:** `&self`. Multiple concurrent readers OK. Segment list is
  an `Arc<Vec<Arc<Segment>>>`.
- **Tick:** `&mut self`. Coordinated with writes by the broker.
- **Retention vs concurrent reads:** the `Arc<Segment>` held by a
  reader keeps the file inode alive after `unlink`; standard Unix
  semantics. Windows: use `FILE_SHARE_DELETE` (Rust's standard
  library does this by default).
- **No partial reads:** the in-memory position counter is advanced
  after `write` returns; reads always go via `read_at(position)`
  bounded by the known current size.

---

# 4. Testing

All tests use the project's parameterized + shared-fixture pattern.

### Layer 1 — Synthetic unit tests

- `segment` tests via fixture `sample_segment_with_records(n)`:
  open + read, find by offset, reject bad CRC / magic, recover from
  partial trailing batch.
- `log` tests via `sample_log_with_segments(n_segments, records_per)`:
  open + read, append + read-back, segment roll on bytes + ms,
  truncate within and across segments, read at log_start_offset,
  read at log_end_offset.
- Retention tests via `aged_log(retention, segments, age_offset)`.
- Open-time recovery tests via `corrupt_log(corruption_kind)` with a
  `macro_rules! recovery_case!` table covering missing-index,
  partial-batch, bad-CRC, index-past-EOF.

### Layer 2 — Proptest round-trip

`crates/log/tests/proptest_log.rs`:

- `write_then_read_records_match` — append arbitrary batches,
  read all, assert content matches.
- `random_truncate_then_read` — append N, truncate to a random offset,
  assert only surviving records returned.

Shared `arb_batches(range)` strategy in `tests/support/strategies.rs`.

### Layer 3 — JVM integration (testcontainers)

`crates/log/tests/integration.rs`, `#[ignore]`-gated:

- **JVM writes, Rust reads.** Boot a Kafka container, produce records
  via `kafka-console-producer` (or `crabka-client-core` if slice 2 is
  merged), copy the partition's log dir out of the container, open
  with `crabka-log`, assert byte-equal records.
- **Rust writes, JVM reads.** Build a log dir with `crabka-log`. Mount
  into a fresh Kafka container. Consume via `kafka-console-consumer`;
  assert records match.

The Rust-writes-JVM-reads direction is the more demanding test — it
proves our writer's bytes are correct enough for the JVM broker to
load and serve.

**Slice 2 dependency note:** if slice 2 isn't merged when slice 3
ships, fall back to `Command`-executing `kafka-console-*.sh` inside the
container.

### Layer 4 — Bench (CodSpeed)

`crates/log/benches/log.rs`:

- Append 10K records of 256 bytes.
- Read 10K records sequentially from offset 0.
- Read at random offset 100 times (cache-warm + cache-cold).
- Open a log with 100 segments.

Per-codec variants for the append bench (None, Gzip, Snappy, Lz4,
Zstd).

### CI

- Existing `rust` matrix picks up the new crate.
- Existing `drift` workflow unaffected (no codegen changes).
- New `log-integration` CI job (Linux only) runs the testcontainers
  suite.

---

# 5. Acceptance criteria

The slice ships when **all** of these hold:

1. `crates/log/` exists with `segment`, `log`, `config`, `error`
   modules and the public API from Section 2.
2. `Log::open` recovers correctly from missing-index,
   partial-trailing-batch, bad-CRC, and index-past-EOF (synthetic unit
   tests).
3. `Log::append` rolls segments on `segment_bytes` and `segment_ms`
   thresholds.
4. `Log::read` returns at least one batch when data exists, even if
   the batch alone exceeds `max_bytes`.
5. `Log::truncate_to` works within a segment and across segments.
6. `Log::tick` deletes per time + size policies; never deletes the
   active segment.
7. Proptest round-trip suite passes.
8. Integration tests pass at least two scenarios: JVM-writes-Rust-reads
   and Rust-writes-JVM-reads.
9. New `log-integration` CI job (Linux only) runs the testcontainers
   suite.
10. CodSpeed bench file added with at least four benchmarks.
11. No regressions in any prior slice's tests.
12. `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D
    warnings` clean.
13. Rustdoc on every public type; crate-level doc explains the on-disk
    layout + recovery story.

---

# 6. Open questions deferred to the implementation plan

- **mmap vs read_at.** Default `read_at` (per Section 1). If profiling
  shows index lookups are hot, switch indexes to mmap-read-only.
- **Producer-ID snapshot files.** Brokers write a `.snapshot` file per
  segment with producer-ID epoch state. Slice 3 doesn't write or
  consume these; the broker (slice 4+) will handle them outside the
  log layer. Plan may revisit if integration tests trip on their
  absence.
- **Cleaner-policy header bit.** Compacted topics carry a marker in
  the record batch attributes; slice 3 stores it verbatim. Compaction
  logic interprets in a later sub-plan.
- **Fsync strategy.** Slice 3 defaults to `flush_on_append = false`
  (the broker manages fsync separately at the log-manager level). If
  brokers want per-append fsync, they can enable it; performance
  trade-off documented.

None block this design.

---

# 7. Next step

Invoke `writing-plans` to produce a detailed implementation plan for
slice 3. Slice 2 (`crabka-client-core`) is being developed in parallel.
