use bytes::{Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_protocol::records::{Record, RecordBatch, RecordHeader};
use proptest::prelude::*;

fn arb_bytes(max: usize) -> impl Strategy<Value = Bytes> {
    proptest::collection::vec(any::<u8>(), 0..=max).prop_map(Bytes::from)
}

fn arb_header() -> impl Strategy<Value = RecordHeader> {
    ("[a-z0-9_-]{1,32}", proptest::option::of(arb_bytes(256)))
        .prop_map(|(key, value)| RecordHeader { key, value })
}

fn arb_record() -> impl Strategy<Value = Record> {
    (
        any::<i8>(),
        -100_000i64..100_000,
        0i32..100,
        proptest::option::of(arb_bytes(512)),
        proptest::option::of(arb_bytes(2048)),
        proptest::collection::vec(arb_header(), 0..=4),
    )
        .prop_map(|(attributes, ts, off, key, value, headers)| Record {
            attributes,
            timestamp_delta: ts,
            offset_delta: off,
            key,
            value,
            headers,
        })
}

fn arb_record_batch(codec: CompressionType) -> impl Strategy<Value = RecordBatch> {
    (
        proptest::collection::vec(arb_record(), 0..=8),
        any::<i64>(),
        any::<i32>(),
        any::<i64>(),
        any::<i64>(),
    )
        .prop_map(move |(records, base_offset, leader_epoch, ts0, ts1)| {
            let mut b = RecordBatch {
                base_offset,
                partition_leader_epoch: leader_epoch,
                base_timestamp: ts0,
                max_timestamp: ts1,
                records,
                ..RecordBatch::default()
            };
            b.attributes = b.attributes.with_compression(codec);
            b
        })
}

macro_rules! proptest_codec {
    ($name:ident, $codec:expr) => {
        proptest! {
            #[test]
            fn $name(b in arb_record_batch($codec)) {
                let mut buf = BytesMut::new();
                b.encode(&mut buf).unwrap();
                // encoded_len() is the uncompressed prediction; only assert
                // equality for the None codec where no compression overhead exists.
                if $codec == CompressionType::None {
                    prop_assert_eq!(buf.len(), b.encoded_len());
                }

                let mut cur: &[u8] = &buf[..];
                let decoded = RecordBatch::decode(&mut cur).unwrap();
                prop_assert_eq!(decoded, b);
            }
        }
    };
}

proptest_codec!(none, CompressionType::None);
proptest_codec!(gzip, CompressionType::Gzip);
proptest_codec!(snappy, CompressionType::Snappy);
proptest_codec!(lz4, CompressionType::Lz4);
proptest_codec!(zstd, CompressionType::Zstd);

proptest! {
    /// KIP-534: a stamped delete horizon sets bit 6, repurposes
    /// `base_timestamp`, and re-bases every record's `timestamp_delta`, so the
    /// reconstructed absolute timestamps survive an encode and decode round
    /// trip.
    ///
    /// The bounds stay modest, so absolute timestamps stay well within `i64`
    /// after the re-basing. The helper does not saturate.
    #[test]
    fn delete_horizon_batches_round_trip(
        base in 0i64..1_000_000,
        horizon in 0i64..1_000_000,
        deltas in proptest::collection::vec(0i64..10_000, 0..=8),
    ) {
        let records: Vec<Record> = deltas
            .iter()
            .enumerate()
            .map(|(i, &d)| Record {
                timestamp_delta: d,
                offset_delta: i32::try_from(i).unwrap(),
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect();

        // Original absolute timestamps, captured before stamping.
        let originals: Vec<i64> = records.iter().map(|r| base + r.timestamp_delta).collect();

        let last_offset_delta = i32::try_from(deltas.len().saturating_sub(1)).unwrap_or(0);
        let b = RecordBatch {
            base_timestamp: base,
            last_offset_delta,
            records,
            ..RecordBatch::default()
        }
        .with_delete_horizon(horizon);

        let mut buf = BytesMut::new();
        b.encode(&mut buf).unwrap();

        let mut cur: &[u8] = &buf[..];
        let decoded = RecordBatch::decode(&mut cur).unwrap();
        prop_assert!(cur.is_empty());

        prop_assert_eq!(decoded.delete_horizon_ms(), Some(horizon));

        let reconstructed: Vec<i64> = decoded
            .records
            .iter()
            .map(|r| decoded.base_timestamp + r.timestamp_delta)
            .collect();
        prop_assert_eq!(reconstructed, originals);
    }
}
