//! `CodSpeed` benchmarks for `crabka-log`: append, read, and open.

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

    // Pre-populate so `read` has something substantial to chew through.
    for _ in 0..100 {
        let mut batch = make_batch(100, 256);
        log.append(&mut batch).unwrap();
    }

    group.bench_function("read_10k_records", |b| {
        b.iter(|| {
            let out = log.read(0, usize::MAX).unwrap();
            black_box(out);
        });
    });

    group.finish();
}

fn bench_open_with_segments(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    {
        let config = LogConfig {
            segment_bytes: 1024, // tiny — force segment rolls
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
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

criterion_group!(benches, bench_append_then_read, bench_open_with_segments);
criterion_main!(benches);
