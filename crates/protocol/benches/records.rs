//! `CodSpeed` microbenchmarks for `crabka-protocol::records`.
//!
//! Covers the v2 `RecordBatch` codec path end-to-end: encode, owned decode,
//! borrowed (zero-copy) decode + iteration, and `encoded_len`. Each axis is
//! varied across compression codecs and a small grid of batch sizes so
//! regressions in any one shape show up clearly.

use bytes::{Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_protocol::DecodeBorrow;
use crabka_protocol::records::RecordBatchBorrowed;
use crabka_protocol::records::{Record, RecordBatch, RecordHeader};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn make_header(key: &str) -> RecordHeader {
    RecordHeader {
        key: key.to_string(),
        value: Some(Bytes::from_static(b"header-value")),
    }
}

fn make_record(offset_delta: i32, payload_size: usize) -> Record {
    Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta) * 1000,
        offset_delta,
        key: Some(Bytes::from_static(b"benchmark-key")),
        value: Some(Bytes::from(vec![0xABu8; payload_size])),
        headers: vec![make_header("tracing-id")],
    }
}

fn make_batch(n: u32, payload_size: usize, codec: CompressionType) -> RecordBatch {
    let records: Vec<Record> = (0..n)
        .map(|i| make_record(i.cast_signed(), payload_size))
        .collect();
    let mut b = RecordBatch {
        base_offset: 100,
        partition_leader_epoch: 0,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000 + i64::from(n) * 1000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        last_offset_delta: (i32::try_from(n.saturating_sub(1))).unwrap_or(0),
        records,
        ..RecordBatch::default()
    };
    b.attributes = b.attributes.with_compression(codec);
    b
}

fn encode_batch(b: &RecordBatch) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(b.encoded_len());
    b.encode(&mut buf).unwrap();
    buf.to_vec()
}

// ---------------------------------------------------------------------------
// RecordBatch encode benchmarks — codecs × batch size
// ---------------------------------------------------------------------------

fn bench_encode_by_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/encode");

    for (label, codec) in [
        ("none", CompressionType::None),
        ("gzip", CompressionType::Gzip),
        ("snappy", CompressionType::Snappy),
        ("lz4", CompressionType::Lz4),
        ("zstd", CompressionType::Zstd),
    ] {
        let batch = make_batch(10_u32, 128, codec);
        let mut buf = BytesMut::with_capacity(batch.encoded_len() * 2);
        group.bench_function(label, |b| {
            b.iter(|| {
                buf.clear();
                black_box(&batch).encode(&mut buf).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_encode_by_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/encode_by_size_none");

    for &n in &[1u32, 10, 100, 1000] {
        let batch = make_batch(n, 64, CompressionType::None);
        let mut buf = BytesMut::with_capacity(batch.encoded_len() * 2);
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                buf.clear();
                black_box(&batch).encode(&mut buf).unwrap();
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// RecordBatch decode benchmarks (owned)
// ---------------------------------------------------------------------------

fn bench_decode_owned_by_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/decode_owned");

    for (label, codec) in [
        ("none", CompressionType::None),
        ("gzip", CompressionType::Gzip),
        ("snappy", CompressionType::Snappy),
        ("lz4", CompressionType::Lz4),
        ("zstd", CompressionType::Zstd),
    ] {
        let encoded = encode_batch(&make_batch(10_u32, 128, codec));
        group.bench_function(label, |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                RecordBatch::decode(&mut cur).unwrap()
            });
        });
    }

    group.finish();
}

fn bench_decode_owned_by_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/decode_owned_by_size_none");

    for &n in &[1u32, 10, 100, 1000] {
        let encoded = encode_batch(&make_batch(n, 64, CompressionType::None));
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                RecordBatch::decode(&mut cur).unwrap()
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// RecordBatch decode benchmarks (borrowed, zero-copy)
// ---------------------------------------------------------------------------

fn bench_decode_borrowed(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/decode_borrowed");

    for &n in &[1u32, 10, 100, 1000] {
        let encoded = encode_batch(&make_batch(n, 64, CompressionType::None));
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                RecordBatchBorrowed::decode_borrow(&mut cur, 0).unwrap()
            });
        });
    }

    group.finish();
}

fn bench_borrowed_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/borrowed_iter");

    for &n in &[10u32, 100, 1000] {
        let encoded = encode_batch(&make_batch(n, 64, CompressionType::None));
        let mut cur: &[u8] = &encoded;
        let batch = RecordBatchBorrowed::decode_borrow(&mut cur, 0).unwrap();
        group.bench_function(format!("{n}_records"), |b| {
            b.iter(|| {
                let mut count = 0usize;
                for r in black_box(&batch) {
                    let r = r.unwrap();
                    count += r.value.map_or(0, <[u8]>::len);
                }
                black_box(count)
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// encoded_len benchmark (no allocation)
// ---------------------------------------------------------------------------

fn bench_encoded_len(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch/encoded_len");

    for &n in &[1u32, 10, 100, 1000] {
        let batch = make_batch(n, 64, CompressionType::None);
        group.bench_function(format!("none_{n}rec"), |b| {
            b.iter(|| black_box(&batch).encoded_len());
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_encode_by_codec,
    bench_encode_by_size,
    bench_decode_owned_by_codec,
    bench_decode_owned_by_size,
    bench_decode_borrowed,
    bench_borrowed_iter,
    bench_encoded_len,
);
criterion_main!(benches);
