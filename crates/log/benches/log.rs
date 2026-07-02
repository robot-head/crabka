//! `CodSpeed` microbenchmarks for `crabka-log`.
//!
//! Covers the public `Log` surface: append (single + bulk), read at various
//! offsets, open + recover with many segments, truncate, and time-based
//! rolling. Intentionally exercises both the active-segment hot path and the
//! sealed-segment scan path.

#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::io::{IoSlice, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use bytes::Bytes;
use crabka_log::{Log, LogConfig, VerbatimBatch};
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

fn make_verbatim_batch(n: i32, payload_size: usize) -> VerbatimBatch {
    make_verbatim_from_batch(&make_batch(n, payload_size))
}

fn make_verbatim_from_batch(batch: &RecordBatch) -> VerbatimBatch {
    let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).unwrap();
    VerbatimBatch {
        bytes: buf.freeze(),
        last_offset_delta: batch.last_offset_delta,
        max_timestamp: batch.max_timestamp,
        leader_epoch: batch.partition_leader_epoch,
        producer_id: batch.producer_id,
        is_transactional: batch.attributes.is_transactional(),
    }
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

fn bench_append_large_message_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/append_large_message");

    group.bench_function("owned_1rec_100KiB", |b| {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        b.iter(|| {
            let mut batch = make_batch(1, 100 * 1024);
            log.append(&mut batch).unwrap();
        });
    });

    group.bench_function("verbatim_1rec_100KiB", |b| {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let batch = make_verbatim_batch(1, 100 * 1024);
        b.iter(|| {
            log.append_verbatim(&batch).unwrap();
        });
    });

    group.bench_function("verbatim_1rec_512KiB", |b| {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let batch = make_verbatim_batch(1, 512 * 1024);
        b.iter(|| {
            log.append_verbatim(&batch).unwrap();
        });
    });

    group.finish();
}

fn bench_append_handoff(c: &mut Criterion) {
    let mut group = c.benchmark_group("log/append_handoff");

    group.bench_function("direct_mutex_verbatim_1rec_100KiB", |b| {
        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let batch = make_verbatim_batch(1, 100 * 1024);
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                log.lock().unwrap().append_verbatim(&batch).unwrap();
            }
            start.elapsed()
        });
    });

    group.bench_function("spawn_blocking_mutex_verbatim_1rec_100KiB", |b| {
        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let batch = make_verbatim_batch(1, 100 * 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    let log = Arc::clone(&log);
                    let batch = batch.clone();
                    tokio::task::spawn_blocking(move || {
                        log.lock().unwrap().append_verbatim(&batch).unwrap();
                    })
                    .await
                    .unwrap();
                }
                start.elapsed()
            })
        });
    });

    group.bench_function("block_in_place_mutex_verbatim_1rec_100KiB", |b| {
        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let batch = make_verbatim_batch(1, 100 * 1024);
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter_custom(|iters| {
            rt.block_on(async {
                let start = Instant::now();
                for _ in 0..iters {
                    tokio::task::block_in_place(|| {
                        log.lock().unwrap().append_verbatim(&batch).unwrap();
                    });
                }
                start.elapsed()
            })
        });
    });

    group.finish();
}

#[cfg(unix)]
fn bench_file_write_shapes(c: &mut Criterion) {
    fn write_all_vectored(
        mut writer: impl Write,
        mut bufs: &mut [IoSlice<'_>],
    ) -> std::io::Result<()> {
        while !bufs.is_empty() {
            let written = writer.write_vectored(bufs)?;
            if written == 0 {
                return Err(std::io::ErrorKind::WriteZero.into());
            }
            IoSlice::advance_slices(&mut bufs, written);
        }
        Ok(())
    }

    let mut group = c.benchmark_group("log/file_write_shapes");
    let header = [0xABu8; 61];
    let body = vec![0xCDu8; (100 * 1024) - header.len()];

    group.bench_function("seek_end_writev_100KiB", |b| {
        let dir = tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("bench.log"))
            .unwrap();
        b.iter(|| {
            (&file).seek(SeekFrom::End(0)).unwrap();
            let mut bufs = [IoSlice::new(&header), IoSlice::new(&body)];
            write_all_vectored(&file, &mut bufs).unwrap();
        });
    });

    group.bench_function("writev_at_current_cursor_100KiB", |b| {
        let dir = tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("bench.log"))
            .unwrap();
        b.iter(|| {
            let mut bufs = [IoSlice::new(&header), IoSlice::new(&body)];
            write_all_vectored(&file, &mut bufs).unwrap();
        });
    });

    group.bench_function("write_all_at_twice_100KiB", |b| {
        let dir = tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(dir.path().join("bench.log"))
            .unwrap();
        let mut position = 0u64;
        b.iter(|| {
            file.write_all_at(&header, position).unwrap();
            file.write_all_at(&body, position + header.len() as u64)
                .unwrap();
            position += (header.len() + body.len()) as u64;
        });
    });

    group.finish();
}

#[cfg(not(unix))]
fn bench_file_write_shapes(_c: &mut Criterion) {}

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
    bench_append_large_message_paths,
    bench_append_handoff,
    bench_file_write_shapes,
    bench_read,
    bench_open,
    bench_truncate,
    bench_accessors,
);
criterion_main!(benches);
