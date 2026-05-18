//! `CodSpeed` microbenchmarks for `crabka-protocol` codec primitives and
//! representative request/response messages.
//!
//! These benches cover the low-level wire primitives (varint, fixed-width
//! integers, strings, bytes, arrays, UUIDs, tagged fields) and a curated set
//! of the highest-traffic Kafka messages (Produce, Fetch, Metadata,
//! `ApiVersions`). They are intentionally short and free of allocation in the
//! hot path so codspeed's instruction-count signal is dominated by the code
//! under test.

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, black_box, criterion_group, criterion_main};

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::{ApiVersion, ApiVersionsResponse};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::{Uuid, get_uuid, put_uuid};
use crabka_protocol::primitives::{array, fixed, string_bytes, varint};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::tagged_fields::{
    UnknownTaggedField, UnknownTaggedFields, WriteTaggedFields, tagged_fields_len,
};
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
        ..Default::default()
    }
}

fn encode_to_bytes<T: Encode>(msg: &T, version: i16) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(msg.encoded_len(version));
    msg.encode(&mut buf, version).unwrap();
    buf.to_vec()
}

fn make_record(offset_delta: i32, payload: usize) -> Record {
    Record {
        attributes: 0,
        timestamp_delta: i64::from(offset_delta) * 1000,
        offset_delta,
        key: Some(Bytes::from(format!("k{offset_delta:08}"))),
        value: Some(Bytes::from(vec![0xABu8; payload])),
        headers: vec![],
    }
}

fn make_record_batch(n: i32, payload: usize) -> RecordBatch {
    let records: Vec<Record> = (0..n).map(|i| make_record(i, payload)).collect();
    RecordBatch {
        base_offset: 0,
        last_offset_delta: (n - 1).max(0),
        records,
        ..RecordBatch::default()
    }
}

fn make_produce_request(num_topics: usize, partitions_per_topic: usize) -> ProduceRequest {
    let topic_data = (0..num_topics)
        .map(|t| TopicProduceData {
            name: format!("topic-{t:04}"),
            topic_id: Uuid([0u8; 16]),
            partition_data: (0..partitions_per_topic)
                .map(|p| PartitionProduceData {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    index: p as i32,
                    records: Some(make_record_batch(8, 64)),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                })
                .collect(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 30_000,
        topic_data,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn make_fetch_request(num_topics: usize, partitions_per_topic: usize) -> FetchRequest {
    let topics = (0..num_topics)
        .map(|t| FetchTopic {
            topic: format!("topic-{t:04}"),
            topic_id: Uuid([0u8; 16]),
            partitions: (0..partitions_per_topic)
                .map(|p| FetchPartition {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    partition: p as i32,
                    current_leader_epoch: -1,
                    fetch_offset: 0,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1024 * 1024,
                    replica_directory_id: Uuid([0u8; 16]),
                    high_watermark: -1,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                })
                .collect(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    FetchRequest {
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1024 * 1024 * 64,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics,
        rack_id: String::new(),
        ..FetchRequest::default()
    }
}

fn make_metadata_response(num_brokers: usize, num_topics: usize, parts: usize) -> MetadataResponse {
    let brokers = (0..num_brokers)
        .map(|i| MetadataResponseBroker {
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            node_id: i as i32,
            host: format!("broker-{i}.example.com"),
            port: 9092,
            rack: Some("us-east-1a".to_string()),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    let topics = (0..num_topics)
        .map(|t| MetadataResponseTopic {
            error_code: 0,
            name: Some(format!("topic-{t:04}")),
            topic_id: Uuid([0u8; 16]),
            is_internal: false,
            partitions: (0..parts)
                .map(|p| MetadataResponsePartition {
                    error_code: 0,
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    partition_index: p as i32,
                    leader_id: 0,
                    leader_epoch: 0,
                    replica_nodes: vec![0, 1, 2],
                    isr_nodes: vec![0, 1, 2],
                    offline_replicas: vec![],
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                })
                .collect(),
            topic_authorized_operations: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        cluster_id: Some("test-cluster".to_string()),
        controller_id: 0,
        topics,
        cluster_authorized_operations: 0,
        error_code: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
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

    group.bench_function("put_varint_zigzag", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::put_varint(&mut buf, black_box(i32::MIN));
        });
    });

    group.bench_function("get_varint_zigzag", |b| {
        let mut bm = BytesMut::new();
        varint::put_varint(&mut bm, i32::MIN);
        let data = bm.freeze();
        b.iter(|| {
            let mut cur: &[u8] = black_box(&data);
            varint::get_varint(&mut cur).unwrap()
        });
    });

    group.bench_function("put_varlong_min", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::put_varlong(&mut buf, black_box(i64::MIN));
        });
    });

    group.bench_function("get_varlong_min", |b| {
        let mut bm = BytesMut::new();
        varint::put_varlong(&mut bm, i64::MIN);
        let data = bm.freeze();
        b.iter(|| {
            let mut cur: &[u8] = black_box(&data);
            varint::get_varlong(&mut cur).unwrap()
        });
    });

    group.bench_function("put_uvarlong_max", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::put_uvarlong(&mut buf, black_box(u64::MAX));
        });
    });

    group.bench_function("get_uvarlong_max", |b| {
        let mut bm = BytesMut::new();
        varint::put_uvarlong(&mut bm, u64::MAX);
        let data = bm.freeze();
        b.iter(|| {
            let mut cur: &[u8] = black_box(&data);
            varint::get_uvarlong(&mut cur).unwrap()
        });
    });

    group.bench_function("uvarint_len_small", |b| {
        b.iter(|| varint::uvarint_len(black_box(127)));
    });
    group.bench_function("uvarint_len_max", |b| {
        b.iter(|| varint::uvarint_len(black_box(u32::MAX)));
    });
    group.bench_function("varlong_len_min", |b| {
        b.iter(|| varint::varlong_len(black_box(i64::MIN)));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Fixed-width integers
// ---------------------------------------------------------------------------

fn bench_fixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed");

    group.bench_function("put_i32", |b| {
        let mut buf = BytesMut::with_capacity(4);
        b.iter(|| {
            buf.clear();
            fixed::put_i32(&mut buf, black_box(0x1234_5678));
        });
    });

    group.bench_function("get_i32", |b| {
        let data: &[u8] = &[0x12, 0x34, 0x56, 0x78];
        b.iter(|| {
            let mut cur = black_box(data);
            fixed::get_i32(&mut cur).unwrap()
        });
    });

    group.bench_function("put_i64", |b| {
        let mut buf = BytesMut::with_capacity(8);
        b.iter(|| {
            buf.clear();
            fixed::put_i64(&mut buf, black_box(0x0102_0304_0506_0708));
        });
    });

    group.bench_function("get_i64", |b| {
        let data: &[u8] = &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        b.iter(|| {
            let mut cur = black_box(data);
            fixed::get_i64(&mut cur).unwrap()
        });
    });

    group.bench_function("put_bool", |b| {
        let mut buf = BytesMut::with_capacity(1);
        b.iter(|| {
            buf.clear();
            fixed::put_bool(&mut buf, black_box(true));
        });
    });

    group.bench_function("put_f64", |b| {
        let mut buf = BytesMut::with_capacity(8);
        b.iter(|| {
            buf.clear();
            fixed::put_f64(&mut buf, black_box(std::f64::consts::PI));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// String / bytes encoding (both legacy and compact forms)
// ---------------------------------------------------------------------------

fn bench_string_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_bytes");

    let short = "kafka";
    let long = "a".repeat(1024);
    let payload_short = vec![0xCDu8; 16];
    let payload_long = vec![0xCDu8; 4096];

    group.bench_function("put_string_short", |b| {
        let mut buf = BytesMut::with_capacity(short.len() + 2);
        b.iter(|| {
            buf.clear();
            string_bytes::put_string(&mut buf, black_box(short));
        });
    });
    group.bench_function("put_compact_string_short", |b| {
        let mut buf = BytesMut::with_capacity(short.len() + 1);
        b.iter(|| {
            buf.clear();
            string_bytes::put_compact_string(&mut buf, black_box(short));
        });
    });
    group.bench_function("put_string_long_1KiB", |b| {
        let mut buf = BytesMut::with_capacity(long.len() + 2);
        b.iter(|| {
            buf.clear();
            string_bytes::put_string(&mut buf, black_box(long.as_str()));
        });
    });
    group.bench_function("put_compact_string_long_1KiB", |b| {
        let mut buf = BytesMut::with_capacity(long.len() + 4);
        b.iter(|| {
            buf.clear();
            string_bytes::put_compact_string(&mut buf, black_box(long.as_str()));
        });
    });

    let mut bm = BytesMut::new();
    string_bytes::put_string(&mut bm, short);
    let encoded_short = bm.clone().freeze();
    group.bench_function("get_string_owned_short", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&encoded_short);
            string_bytes::get_string_owned(&mut cur).unwrap()
        });
    });

    let mut bm = BytesMut::new();
    string_bytes::put_compact_string(&mut bm, &long);
    let compact_long_bytes = bm.freeze();
    group.bench_function("get_compact_string_owned_long_1KiB", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&compact_long_bytes);
            string_bytes::get_compact_string_owned(&mut cur).unwrap()
        });
    });

    group.bench_function("put_bytes_short", |b| {
        let mut buf = BytesMut::with_capacity(payload_short.len() + 4);
        b.iter(|| {
            buf.clear();
            string_bytes::put_bytes(&mut buf, black_box(&payload_short));
        });
    });
    group.bench_function("put_compact_bytes_long_4KiB", |b| {
        let mut buf = BytesMut::with_capacity(payload_long.len() + 4);
        b.iter(|| {
            buf.clear();
            string_bytes::put_compact_bytes(&mut buf, black_box(&payload_long));
        });
    });

    let mut bm = BytesMut::new();
    string_bytes::put_compact_bytes(&mut bm, &payload_long);
    let payload_encoded = bm.freeze();
    group.bench_function("get_compact_bytes_owned_4KiB", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&payload_encoded);
            string_bytes::get_compact_bytes_owned(&mut cur).unwrap()
        });
    });

    group.bench_function("string_len_short", |b| {
        b.iter(|| string_bytes::string_len(black_box(short)));
    });
    group.bench_function("compact_string_len_long", |b| {
        b.iter(|| string_bytes::compact_string_len(black_box(long.as_str())));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Array length prefixes
// ---------------------------------------------------------------------------

fn bench_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("array");

    group.bench_function("put_array_len_flexible_small", |b| {
        let mut buf = BytesMut::with_capacity(4);
        b.iter(|| {
            buf.clear();
            array::put_array_len(&mut buf, black_box(10), true);
        });
    });

    group.bench_function("put_array_len_legacy_small", |b| {
        let mut buf = BytesMut::with_capacity(4);
        b.iter(|| {
            buf.clear();
            array::put_array_len(&mut buf, black_box(10), false);
        });
    });

    group.bench_function("put_nullable_array_len_null", |b| {
        let mut buf = BytesMut::with_capacity(4);
        b.iter(|| {
            buf.clear();
            array::put_nullable_array_len(&mut buf, black_box(None), true);
        });
    });

    group.bench_function("get_array_len_flexible", |b| {
        let mut bm = BytesMut::new();
        array::put_array_len(&mut bm, 1000, true);
        let data = bm.freeze();
        b.iter(|| {
            let mut cur: &[u8] = black_box(&data);
            array::get_array_len(&mut cur, true).unwrap()
        });
    });

    group.bench_function("array_len_prefix_len_flexible", |b| {
        b.iter(|| array::array_len_prefix_len(black_box(10_000), true));
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

fn bench_uuid(c: &mut Criterion) {
    let mut group = c.benchmark_group("uuid");
    let u = Uuid([0u8; 16]);

    group.bench_function("put_uuid", |b| {
        let mut buf = BytesMut::with_capacity(16);
        b.iter(|| {
            buf.clear();
            put_uuid(&mut buf, black_box(u));
        });
    });

    let bytes: [u8; 16] = [0x11; 16];
    group.bench_function("get_uuid", |b| {
        b.iter(|| {
            let mut cur: &[u8] = black_box(&bytes);
            get_uuid(&mut cur).unwrap()
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Tagged fields
// ---------------------------------------------------------------------------

fn bench_tagged_fields(c: &mut Criterion) {
    let mut group = c.benchmark_group("tagged_fields");

    let empty = UnknownTaggedFields::default();
    let some_unknown = UnknownTaggedFields(vec![
        UnknownTaggedField {
            tag: 7,
            bytes: Bytes::from_static(&[1u8, 2, 3, 4]),
        },
        UnknownTaggedField {
            tag: 9,
            bytes: Bytes::from_static(&[5u8; 64]),
        },
    ]);

    group.bench_function("write_empty", |b| {
        let mut buf = BytesMut::with_capacity(8);
        b.iter(|| {
            buf.clear();
            let w = WriteTaggedFields::new();
            w.write(&mut buf, black_box(&empty));
        });
    });

    group.bench_function("write_two_unknown", |b| {
        let mut buf = BytesMut::with_capacity(128);
        b.iter(|| {
            buf.clear();
            let w = WriteTaggedFields::new();
            w.write(&mut buf, black_box(&some_unknown));
        });
    });

    group.bench_function("len_empty", |b| {
        b.iter(|| tagged_fields_len(black_box(&[]), black_box(&empty)));
    });

    group.bench_function("len_with_known_and_unknown", |b| {
        let known = [(1u32, 4usize), (3u32, 8usize)];
        b.iter(|| tagged_fields_len(black_box(&known), black_box(&some_unknown)));
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
// ProduceRequest (owned, flexible v12 — modern KIP-848 default)
// ---------------------------------------------------------------------------

fn bench_produce_request(c: &mut Criterion) {
    let mut group = c.benchmark_group("produce_request");
    let version: i16 = 12;

    for (label, topics, parts) in [
        ("1topic_1partition", 1usize, 1usize),
        ("1topic_8partitions", 1, 8),
        ("8topics_8partitions", 8, 8),
    ] {
        let req = make_produce_request(topics, parts);
        let encoded = encode_to_bytes(&req, version);

        group.bench_function(format!("encode_v12_{label}"), |b| {
            let mut buf = BytesMut::with_capacity(encoded.len() + 64);
            b.iter(|| {
                buf.clear();
                black_box(&req).encode(&mut buf, version).unwrap();
            });
        });

        group.bench_function(format!("decode_v12_{label}"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                ProduceRequest::decode(&mut cur, version).unwrap()
            });
        });

        group.bench_function(format!("encoded_len_v12_{label}"), |b| {
            b.iter(|| black_box(&req).encoded_len(version));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// FetchRequest (owned, flexible v17 — modern KIP-1166 default)
// ---------------------------------------------------------------------------

fn bench_fetch_request(c: &mut Criterion) {
    let mut group = c.benchmark_group("fetch_request");
    let version: i16 = 17;

    for (label, topics, parts) in [
        ("1topic_1partition", 1usize, 1usize),
        ("1topic_64partitions", 1, 64),
        ("16topics_64partitions", 16, 64),
    ] {
        let req = make_fetch_request(topics, parts);
        let encoded = encode_to_bytes(&req, version);

        group.bench_function(format!("encode_v17_{label}"), |b| {
            let mut buf = BytesMut::with_capacity(encoded.len() + 64);
            b.iter(|| {
                buf.clear();
                black_box(&req).encode(&mut buf, version).unwrap();
            });
        });

        group.bench_function(format!("decode_v17_{label}"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                FetchRequest::decode(&mut cur, version).unwrap()
            });
        });

        group.bench_function(format!("encoded_len_v17_{label}"), |b| {
            b.iter(|| black_box(&req).encoded_len(version));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// MetadataResponse (owned, flexible v12)
// ---------------------------------------------------------------------------

fn bench_metadata_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_response");
    let version: i16 = 12;

    for (label, brokers, topics, parts) in [
        ("3brokers_1topic", 3usize, 1usize, 3usize),
        ("3brokers_100topics", 3, 100, 3),
        ("9brokers_500topics_8parts", 9, 500, 8),
    ] {
        let resp = make_metadata_response(brokers, topics, parts);
        let encoded = encode_to_bytes(&resp, version);

        group.bench_function(format!("encode_v12_{label}"), |b| {
            let mut buf = BytesMut::with_capacity(encoded.len() + 64);
            b.iter(|| {
                buf.clear();
                black_box(&resp).encode(&mut buf, version).unwrap();
            });
        });

        group.bench_function(format!("decode_v12_{label}"), |b| {
            b.iter(|| {
                let mut cur: &[u8] = black_box(&encoded);
                MetadataResponse::decode(&mut cur, version).unwrap()
            });
        });

        group.bench_function(format!("encoded_len_v12_{label}"), |b| {
            b.iter(|| black_box(&resp).encoded_len(version));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_varint,
    bench_fixed,
    bench_string_bytes,
    bench_array,
    bench_uuid,
    bench_tagged_fields,
    bench_api_versions_request,
    bench_api_versions_response,
    bench_produce_request,
    bench_fetch_request,
    bench_metadata_response,
);
criterion_main!(benches);
