mod support;
use bytes::BytesMut;
use crabka_compression::CompressionType;
use crabka_protocol::records::{Record, RecordBatch, TimestampType};
use proptest::prelude::*;
use serde_json::{Value, json};
use support::oracle;

fn record_to_json(r: &Record) -> Value {
    let mut headers = Vec::new();
    for h in &r.headers {
        headers.push(json!({
            "key": h.key,
            "value": h.value.as_ref().map(hex::encode),
        }));
    }
    json!({
        "offset_delta": r.offset_delta,
        "timestamp_delta": r.timestamp_delta,
        "key": r.key.as_ref().map(hex::encode),
        "value": r.value.as_ref().map(hex::encode),
        "headers": headers,
    })
}

fn batch_to_json(b: &RecordBatch) -> Value {
    let codec_name = match b.attributes.compression() {
        CompressionType::Gzip => "GZIP",
        CompressionType::Snappy => "SNAPPY",
        CompressionType::Lz4 => "LZ4",
        CompressionType::Zstd => "ZSTD",
        _ => "NONE",
    };
    json!({
        "base_offset": b.base_offset,
        "partition_leader_epoch": b.partition_leader_epoch,
        "compression": codec_name,
        "timestamp_type": match b.attributes.timestamp_type() {
            TimestampType::CreateTime => "CreateTime",
            TimestampType::LogAppendTime => "LogAppendTime",
        },
        "is_transactional": b.attributes.is_transactional(),
        "is_control_batch": b.attributes.is_control_batch(),
        "base_timestamp": b.base_timestamp,
        "producer_id": b.producer_id,
        "producer_epoch": b.producer_epoch,
        "base_sequence": b.base_sequence,
        "records": b.records.iter().map(record_to_json).collect::<Vec<_>>(),
    })
}

fn arb_record_payload() -> impl Strategy<Value = (Option<bytes::Bytes>, Option<bytes::Bytes>)> {
    (
        proptest::option::of(
            proptest::collection::vec(any::<u8>(), 0..=256).prop_map(bytes::Bytes::from),
        ),
        proptest::option::of(
            proptest::collection::vec(any::<u8>(), 0..=1024).prop_map(bytes::Bytes::from),
        ),
    )
}

fn arb_batch(codec: CompressionType) -> impl Strategy<Value = RecordBatch> {
    // Constraints for JVM compatibility:
    // 1. At least 1 record: the JVM's MemoryRecordsBuilder produces empty bytes
    //    when no records are appended, but Rust writes the 61-byte header.
    // 2. base_timestamp >= 0: the JVM rejects negative absolute timestamps.
    // 3. timestamp_delta >= 0: so that base_timestamp + timestamp_delta >= 0.
    // 4. offset_delta must produce strictly monotonically increasing absolute
    //    offsets (baseOffset + offsetDelta must be distinct and ascending).
    //    We enforce this by assigning offsets as 0, 1, 2, ... (delta per position).
    (
        proptest::collection::vec(arb_record_payload(), 1..=6),
        (0i64..1_000_000_000), // base_timestamp (non-negative)
        (0i64..1_000_000),     // per-record timestamp_delta (non-negative)
    )
        .prop_map(move |(payloads, base_ts, ts_delta)| {
            let records = payloads
                .into_iter()
                .enumerate()
                .map(|(i, (key, value))| Record {
                    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                    offset_delta: i as i32, // 0, 1, 2, ... — strictly increasing; max 5
                    #[allow(clippy::cast_possible_wrap)]
                    timestamp_delta: ts_delta * i as i64,
                    key,
                    value,
                    ..Default::default()
                })
                .collect();
            let mut b = RecordBatch {
                base_timestamp: base_ts,
                records,
                ..Default::default()
            };
            b.attributes = b.attributes.with_compression(codec);
            b
        })
}

macro_rules! diff_test {
    ($name:ident, $codec:expr) => {
        #[test]
        #[ignore = "requires JVM oracle"]
        fn $name() {
            let oracle_cell = std::cell::RefCell::new(oracle::shared());
            proptest!(|(b in arb_batch($codec))| {
                let mut o = oracle_cell.borrow_mut();

                // Rust encodes; JVM decodes; structural equality on the JSON projection
                let mut rust_bytes = BytesMut::new();
                b.encode(&mut rust_bytes).unwrap();
                let jvm_decoded = o.record_batch_decode(&rust_bytes);
                let expected = batch_to_json(&b);
                // Compare records arrays (the JVM's full JSON has more fields we don't care about)
                prop_assert_eq!(&jvm_decoded["records"], &expected["records"]);

                // JVM encodes; Rust decodes; round-trip back to Rust batch
                let jvm_bytes = o.record_batch_encode(&expected);
                let mut cur: &[u8] = &jvm_bytes[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                let project = |records: &[Record]| {
                    records
                        .iter()
                        .map(|record| {
                            (
                                record.key.clone(),
                                record.value.clone(),
                                record.offset_delta,
                            )
                        })
                        .collect::<Vec<_>>()
                };
                prop_assert_eq!(project(&decoded.records), project(&b.records));
            });
        }
    };
}

diff_test!(diff_none, CompressionType::None);
diff_test!(diff_gzip, CompressionType::Gzip);
diff_test!(diff_snappy, CompressionType::Snappy);
diff_test!(diff_lz4, CompressionType::Lz4);
diff_test!(diff_zstd, CompressionType::Zstd);
