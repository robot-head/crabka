use bytes::BytesMut;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::primitives::varint;
use crabka_protocol::tagged_fields::UnknownTaggedFields;
use crabka_protocol::{Decode, Encode};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_api_versions_request() -> ApiVersionsRequest {
    ApiVersionsRequest {
        client_software_name: "crabka".to_string(),
        client_software_version: "0.1.0".to_string(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn make_api_versions_response() -> ApiVersionsResponse {
    let api_keys: Vec<ApiVersion> = (0..80)
        .map(|i| ApiVersion {
            api_key: i,
            min_version: 0,
            max_version: 10,
            ..Default::default()
        })
        .collect();
    ApiVersionsResponse {
        error_code: 0,
        api_keys,
        throttle_time_ms: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn encode_to_bytes<T: Encode>(msg: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(msg.encoded_len(version));
    msg.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

// ---------------------------------------------------------------------------
// Varint primitives
// ---------------------------------------------------------------------------

fn bench_varint(c: &mut Criterion) {
    let mut group = c.benchmark_group("varint");

    group.bench_function("put_uvarint_small", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::put_uvarint(&mut buf, black_box(127));
        });
    });

    group.bench_function("put_uvarint_large", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::put_uvarint(&mut buf, black_box(u32::MAX));
        });
    });

    group.bench_function("get_uvarint_small", |b| {
        let data: &[u8] = &[0x7F];
        b.iter(|| {
            let mut cur = black_box(data);
            varint::get_uvarint(&mut cur).unwrap()
        });
    });

    group.bench_function("get_uvarint_large", |b| {
        let data: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F];
        b.iter(|| {
            let mut cur = black_box(data);
            varint::get_uvarint(&mut cur).unwrap()
        });
    });

    group.bench_function("put_varlong", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::put_varlong(&mut buf, black_box(i64::MIN));
        });
    });

    group.bench_function("get_varlong", |b| {
        let mut buf = BytesMut::new();
        varint::put_varlong(&mut buf, i64::MIN);
        let data = buf.freeze();
        b.iter(|| {
            let mut cur: &[u8] = black_box(&data);
            varint::get_varlong(&mut cur).unwrap()
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// ApiVersionsRequest (owned, flexible v3)
// ---------------------------------------------------------------------------

fn bench_api_versions_request(c: &mut Criterion) {
    let mut group = c.benchmark_group("api_versions_request");
    let version: i16 = 3;

    let req = make_api_versions_request();
    let encoded = encode_to_bytes(&req, version);

    group.bench_function("encode_v3", |b| {
        let mut buf = BytesMut::with_capacity(encoded.len());
        b.iter(|| {
            buf.clear();
            black_box(&req).encode(&mut buf, version).unwrap();
        });
    });

    group.bench_function("decode_v3", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&encoded);
            ApiVersionsRequest::decode(&mut cur, version).unwrap()
        });
    });

    group.bench_function("encoded_len_v3", |b| {
        b.iter(|| black_box(&req).encoded_len(version));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// ApiVersionsResponse (owned, flexible v3, 80 api_keys entries)
// ---------------------------------------------------------------------------

fn bench_api_versions_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("api_versions_response");
    let version: i16 = 3;

    let resp = make_api_versions_response();
    let encoded = encode_to_bytes(&resp, version);

    group.bench_function("encode_v3", |b| {
        let mut buf = BytesMut::with_capacity(encoded.len());
        b.iter(|| {
            buf.clear();
            black_box(&resp).encode(&mut buf, version).unwrap();
        });
    });

    group.bench_function("decode_v3", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&encoded);
            ApiVersionsResponse::decode(&mut cur, version).unwrap()
        });
    });

    group.bench_function("encode_v0", |b| {
        let mut buf = BytesMut::with_capacity(resp.encoded_len(0));
        b.iter(|| {
            buf.clear();
            black_box(&resp).encode(&mut buf, 0).unwrap();
        });
    });

    group.bench_function("decode_v0", |b| {
        let v0_encoded = encode_to_bytes(&resp, 0);
        b.iter(|| {
            let mut cur: &[u8] = black_box(&v0_encoded);
            ApiVersionsResponse::decode(&mut cur, 0).unwrap()
        });
    });

    group.bench_function("encoded_len_v3", |b| {
        b.iter(|| black_box(&resp).encoded_len(version));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_varint,
    bench_api_versions_request,
    bench_api_versions_response,
);
criterion_main!(benches);
