//! `CodSpeed` microbenchmarks for `crabka-compression`.
//!
//! For each supported codec, we measure compress + decompress over three
//! payload sizes (1 KiB, 64 KiB, 1 MiB) and three payload shapes:
//!
//! - **alternating runs** — mildly compressible, the original fixture.
//! - **random** — incompressible, exercises the codec's worst case.
//! - **text-like** — repeated English-ish bytes, exercises typical user data.
//!
//! Round-trip (compress→decompress) is also measured per codec so codspeed
//! tracks net work directly comparable across codecs.

use bytes::Bytes;
use crabka_compression::{CompressionType, compress, decompress};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

// ---------------------------------------------------------------------------
// Payload generators — deterministic so codspeed sees the same bytes every run.
// ---------------------------------------------------------------------------

fn payload_alternating(size: usize) -> Bytes {
    let mut v = Vec::with_capacity(size);
    for i in 0..size {
        v.push(if (i / 32) % 2 == 0 { 0xAB } else { 0xCD });
    }
    Bytes::from(v)
}

fn payload_random(size: usize) -> Bytes {
    // Deterministic LCG so the bytes don't compress at all.
    let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
    let mut v = Vec::with_capacity(size);
    for _ in 0..size {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        v.push(state.to_be_bytes()[0]);
    }
    Bytes::from(v)
}

fn payload_text(size: usize) -> Bytes {
    // Repeated lorem-ipsum-like bytes. Highly compressible.
    const FRAGMENT: &[u8] =
        b"The quick brown fox jumps over the lazy dog. Lorem ipsum dolor sit amet. ";
    let mut v = Vec::with_capacity(size);
    while v.len() < size {
        let take = (size - v.len()).min(FRAGMENT.len());
        v.extend_from_slice(&FRAGMENT[..take]);
    }
    Bytes::from(v)
}

// ---------------------------------------------------------------------------
// Bench drivers
// ---------------------------------------------------------------------------

fn bench_one_shape(
    c: &mut Criterion,
    codec_name: &str,
    shape_name: &str,
    ct: CompressionType,
    payload: fn(usize) -> Bytes,
) {
    let mut group = c.benchmark_group(format!("{codec_name}/{shape_name}"));
    for &size in &[1024usize, 64 * 1024, 1024 * 1024] {
        let data = payload(size);
        let compressed = compress(ct, &data).unwrap();

        group.bench_function(format!("compress_{size}"), |b| {
            b.iter(|| compress(ct, black_box(&data)).unwrap());
        });
        group.bench_function(format!("decompress_{size}"), |b| {
            b.iter(|| decompress(ct, black_box(&compressed), usize::MAX).unwrap());
        });
        group.bench_function(format!("roundtrip_{size}"), |b| {
            b.iter(|| {
                let c = compress(ct, black_box(&data)).unwrap();
                let d = decompress(ct, &c, usize::MAX).unwrap();
                black_box(d)
            });
        });
    }
    group.finish();
}

fn bench_codec(c: &mut Criterion, name: &str, ct: CompressionType) {
    bench_one_shape(c, name, "alt", ct, payload_alternating);
    bench_one_shape(c, name, "rand", ct, payload_random);
    bench_one_shape(c, name, "text", ct, payload_text);
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

fn bench_none(c: &mut Criterion) {
    // CompressionType::None is the pass-through path; track its overhead too.
    let mut group = c.benchmark_group("none");
    for &size in &[1024usize, 64 * 1024, 1024 * 1024] {
        let data = payload_alternating(size);
        group.bench_function(format!("compress_{size}"), |b| {
            b.iter(|| compress(CompressionType::None, black_box(&data)).unwrap());
        });
        group.bench_function(format!("decompress_{size}"), |b| {
            b.iter(|| decompress(CompressionType::None, black_box(&data), usize::MAX).unwrap());
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_none,
    bench_gzip,
    bench_snappy,
    bench_lz4,
    bench_zstd,
);
criterion_main!(benches);
