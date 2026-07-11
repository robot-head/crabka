//! I/O-primitive benchmark to decide whether memory-mapping the log
//! segments would help the fetch read path.
//!
//! The existing `crabka-log` `log` bench measures `Log::read` against a
//! single warm active segment immediately after writing — useful, but it
//! bundles the batch decode with the I/O and never isolates the part mmap
//! would actually replace: pulling raw bytes out of an on-disk `.log`
//! segment.
//!
//! This bench builds a realistic multi-segment log on disk, then races the
//! current strategy (`File::seek` + `read_to_end`, mirroring
//! `Segment::read_log_range`) against several mmap variants on the *same*
//! files:
//!
//! - `pread_to_vec` — current behaviour: fresh Vec per read.
//! - `pread_reuse_buf` — same syscall, buffer reused across reads.
//! - `mmap_once_copy` — map once, copy the range into a Vec each read (what
//!   Crabka would still need today, since the wire path re-encodes every batch).
//! - `mmap_once_borrow` — map once, read the range in place with no copy (the
//!   zero-copy *upper bound* — only reachable if a sendfile/splice-style path
//!   skipped decode).
//! - `mmap_per_call` — map+unmap each read (lazy-mapping overhead).
//!
//! Finally `full_path_decoded` runs the real `Log::read` so the I/O numbers
//! can be read against the cost of the decode/re-encode round trip they sit
//! inside.
//!
//! Caveat: this measures the **warm page-cache** steady state (the file was
//! just written and is resident). That is the realistic case for a broker
//! serving recent data; a cold-cache comparison would need root to drop
//! caches and isn't portable in CI.

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
};

use bytes::Bytes;
use crabka_log::{Log, LogConfig, Offset};
use crabka_protocol::records::{Record, RecordBatch};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use memmap2::Mmap;
use tempfile::TempDir;

/// One Kafka-default fetch chunk.
const READ_LEN: usize = 1024 * 1024;
/// A small scattered read, where mmap's random-access edge would show.
const SMALL_READ_LEN: usize = 16 * 1024;

fn make_batch(n: i32, payload_size: usize) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
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

/// Build a log with several *sealed* segments and return the dir (kept
/// alive), the open `Log`, a fully-populated sealed `.log` file path, and its
/// size.
fn build_log() -> (TempDir, Log, PathBuf, u64) {
    let dir = tempfile::tempdir().unwrap();
    // 8 MiB segments so a 1 MiB read stays inside one sealed segment and we
    // accumulate several sealed files to choose from.
    let config = LogConfig {
        segment_bytes: 8 * 1024 * 1024,
        ..LogConfig::default()
    };
    let mut log = Log::open(dir.path(), config).unwrap();
    // ~100 KiB per batch * 400 ≈ 40 MiB ≈ 5 sealed segments + an active one.
    for _ in 0..400 {
        let mut batch = make_batch(100, 1024);
        log.append(&mut batch).unwrap();
    }

    // Pick a sealed segment: the first `.log` by base offset is guaranteed
    // full (the active/last one may be short).
    let mut logs: Vec<PathBuf> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    logs.sort();
    let chosen = logs
        .into_iter()
        .next()
        .expect("at least one sealed segment");
    let size = std::fs::metadata(&chosen).unwrap().len();
    assert2::assert!(size >= READ_LEN as u64);
    (dir, log, chosen, size)
}

/// Mirrors `Segment::read_log_range`: clone handle, seek, bounded read_to_end.
fn pread_into(file: &File, start: u64, len: usize, buf: &mut Vec<u8>) {
    buf.clear();
    let mut f = file.try_clone().unwrap();
    f.seek(SeekFrom::Start(start)).unwrap();
    let mut bounded = f.take(len as u64);
    bounded.read_to_end(buf).unwrap();
}

/// Sum bytes so the optimizer can't elide the read of a borrowed slice.
fn checksum(bytes: &[u8]) -> u64 {
    bytes
        .iter()
        .fold(0u64, |acc, &b| acc.wrapping_add(u64::from(b)))
}

fn bench_segment_io(c: &mut Criterion) {
    let (_dir, _log, path, size) = build_log();
    let file = OpenOptions::new().read(true).open(&path).unwrap();
    // Map once for the steady-state variants. In a broker the segment would
    // be mapped on open and reused for the life of the segment.
    // SAFETY: the chosen segment is sealed and never written again, so the
    // mapped bytes are stable for the lifetime of `mmap`.
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    for &(label, len) in &[("1MiB", READ_LEN), ("16KiB", SMALL_READ_LEN)] {
        // Read from a mid-file, len-bounded offset.
        let start: u64 = (size / 2).min(size - len as u64);
        let start_usize = start as usize;

        let mut group = c.benchmark_group(format!("segment_io/{label}"));

        group.bench_function("pread_to_vec", |b| {
            b.iter(|| {
                let mut buf = Vec::with_capacity(len);
                pread_into(&file, start, len, &mut buf);
                black_box(&buf);
            });
        });

        group.bench_function("pread_reuse_buf", |b| {
            let mut buf = Vec::with_capacity(len);
            b.iter(|| {
                pread_into(&file, start, len, &mut buf);
                black_box(&buf);
            });
        });

        group.bench_function("mmap_once_copy", |b| {
            b.iter(|| {
                let mut buf = Vec::with_capacity(len);
                buf.extend_from_slice(&mmap[start_usize..start_usize + len]);
                black_box(&buf);
            });
        });

        group.bench_function("mmap_once_borrow", |b| {
            b.iter(|| {
                let slice = &mmap[start_usize..start_usize + len];
                black_box(checksum(slice));
            });
        });

        group.bench_function("mmap_per_call", |b| {
            b.iter(|| {
                // SAFETY: same sealed-segment invariant as above.
                let m = unsafe { Mmap::map(&file).unwrap() };
                let slice = &m[start_usize..start_usize + len];
                black_box(checksum(slice));
            });
        });

        group.finish();
    }
}

/// The full `Log::read` path (index lookup + I/O + batch decode), so the raw
/// I/O numbers above can be read in context: if this dwarfs `pread_to_vec`,
/// the decode dominates and swapping the I/O for mmap can't help much.
fn bench_full_path(c: &mut Criterion) {
    let (_dir, log, _path, _size) = build_log();
    let end = log.log_end_offset();
    let mid = Offset(end.0 / 2);

    let mut group = c.benchmark_group("full_path");
    group.bench_function("log_read_1MiB_decoded", |b| {
        b.iter(|| {
            let out = log.read(black_box(mid), READ_LEN).unwrap();
            black_box(out);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_segment_io, bench_full_path);
criterion_main!(benches);
