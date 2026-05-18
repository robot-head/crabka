//! `CodSpeed` microbenchmarks for `crabka-log`.
//!
//! Covers the public `Log` surface: append (single + bulk), read at various
//! offsets, open + recover with many segments, truncate, and time-based
//! rolling. Intentionally exercises both the active-segment hot path and the
//! sealed-segment scan path.

use bytes::Bytes;
use crabka_log::{Log, LogConfig};
use crabka_protocol::records::{Record, RecordBatch};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tempfile::tempdir;

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

// ---------------------------------------------------------------------------
// Append benchmarks
// ---------------------------------------------------------------------------

fn bench_append_record_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/append");

    for &(records, payload) in &[
        (1i32, 64usize),
        (10, 64),
        (100, 256),
        (100, 1024),
        (500, 256),
    ] {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        group.bench_function(format!("{records}rec_{payload}B"), |b| {
            b.iter(|| {
                let mut batch = make_batch(records, payload);
                log.append(&mut batch).unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Read benchmarks at varied offsets / sizes
// ---------------------------------------------------------------------------

fn bench_read(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    for _ in 0..200 {
        let mut batch = make_batch(100, 256);
        log.append(&mut batch).unwrap();
    }
    let end = log.log_end_offset();

    let mut group = c.benchmark_group("log/read");

    group.bench_function("from_start_1MiB", |b| {
        b.iter(|| {
            let out = log.read(black_box(0), 1024 * 1024).unwrap();
            black_box(out);
        });
    });

    group.bench_function("from_start_unbounded", |b| {
        b.iter(|| {
            let out = log.read(black_box(0), usize::MAX).unwrap();
            black_box(out);
        });
    });

    group.bench_function("from_middle_1MiB", |b| {
        let mid = end / 2;
        b.iter(|| {
            let out = log.read(black_box(mid), 1024 * 1024).unwrap();
            black_box(out);
        });
    });

    group.bench_function("from_end_minus_100_1MiB", |b| {
        let near_end = (end - 100).max(0);
        b.iter(|| {
            let out = log.read(black_box(near_end), 1024 * 1024).unwrap();
            black_box(out);
        });
    });

    group.bench_function("past_end_returns_empty", |b| {
        b.iter(|| {
            let out = log.read(black_box(end), 1024 * 1024).unwrap();
            black_box(out);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Open / recovery — varied segment counts
// ---------------------------------------------------------------------------

fn bench_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/open");

    for &num_appends in &[50usize, 200, 500] {
        let dir = tempdir().unwrap();
        {
            let config = LogConfig {
                segment_bytes: 1024,
                ..LogConfig::default()
            };
            let mut log = Log::open(dir.path(), config).unwrap();
            for _ in 0..num_appends {
                let mut batch = make_batch(5, 64);
                log.append(&mut batch).unwrap();
            }
        }
        group.bench_function(format!("{num_appends}_appends_validate_on_open"), |b| {
            b.iter(|| {
                let log = Log::open(dir.path(), LogConfig::default()).unwrap();
                black_box(log);
            });
        });
        group.bench_function(format!("{num_appends}_appends_no_validate"), |b| {
            let cfg = LogConfig {
                validate_on_open: false,
                ..LogConfig::default()
            };
            b.iter(|| {
                let log = Log::open(dir.path(), cfg.clone()).unwrap();
                black_box(log);
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Truncate
// ---------------------------------------------------------------------------

fn bench_truncate(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/truncate");

    group.bench_function("truncate_recent_offset", |b| {
        b.iter_with_setup(
            || {
                let dir = tempdir().unwrap();
                let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
                for _ in 0..50 {
                    let mut batch = make_batch(10, 64);
                    log.append(&mut batch).unwrap();
                }
                let end = log.log_end_offset();
                (dir, log, end)
            },
            |(_dir, mut log, end)| {
                log.truncate_to(end - 50).unwrap();
                black_box(log);
            },
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// log_end_offset / log_start_offset accessors — should be O(1)
// ---------------------------------------------------------------------------

fn bench_accessors(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    for _ in 0..100 {
        let mut batch = make_batch(10, 64);
        log.append(&mut batch).unwrap();
    }

    let mut group = c.benchmark_group("log/accessors");
    group.bench_function("log_end_offset", |b| {
        b.iter(|| black_box(&log).log_end_offset());
    });
    group.bench_function("log_start_offset", |b| {
        b.iter(|| black_box(&log).log_start_offset());
    });
    group.bench_function("lso", |b| {
        b.iter(|| black_box(&log).lso());
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_append_record_sizes,
    bench_read,
    bench_open,
    bench_truncate,
    bench_accessors,
);
criterion_main!(benches);
