use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use crabka_compression::{CompressionType, compress, decompress};

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

fn bench_gzip(c: &mut Criterion) {
    bench_codec(c, "gzip", CompressionType::Gzip);
}
fn bench_snappy(c: &mut Criterion) {
    bench_codec(c, "snappy", CompressionType::Snappy);
}
fn bench_lz4(c: &mut Criterion) {
    bench_codec(c, "lz4", CompressionType::Lz4);
}
fn bench_zstd(c: &mut Criterion) {
    bench_codec(c, "zstd", CompressionType::Zstd);
}

criterion_group!(benches, bench_gzip, bench_snappy, bench_lz4, bench_zstd);
criterion_main!(benches);
