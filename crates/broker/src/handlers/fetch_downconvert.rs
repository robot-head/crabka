//! Helpers for down-converting v2 `RecordBatch`es to v0/v1 `MessageSet`
//! bytes when the requester is on Fetch v<4. Control batches (txn
//! markers) are dropped entirely; zstd-compressed batches are
//! re-compressed as snappy (v0/v1 doesn't support zstd).

use bytes::{Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_protocol::records::{RecordBatch, RecordsPayload};
use crabka_records_legacy::{Magic, v2_to_legacy};

use crate::codes;

/// Build the records-field payload for a single batch.
///
/// Returns `Ok(None)` when the entire batch is dropped (control batch on
/// the legacy path). Returns `Err(error_code)` for hard down-conversion
/// failures the caller should surface as a per-partition error.
pub(crate) fn down_convert_for_fetch(
    batch: &RecordBatch,
    request_version: i16,
) -> Result<Option<RecordsPayload>, i16> {
    if request_version >= 4 {
        return Ok(Some(RecordsPayload::V2(vec![batch.clone()])));
    }
    if batch.attributes.is_control_batch() {
        return Ok(None);
    }
    // Zstd is not representable in v0/v1. Re-compress as snappy so the
    // client gets a compressed wire format it can decompress. v2_to_legacy
    // decodes the v2 payload first (which decompresses whatever codec the
    // batch used internally) and then re-encodes using the batch's codec.
    // Swapping zstd → snappy before calling v2_to_legacy routes the output
    // through encode_compressed_message_set with snappy.
    let working = if batch.attributes.compression() == CompressionType::Zstd {
        let mut clone = batch.clone();
        clone.attributes = clone.attributes.with_compression(CompressionType::Snappy);
        clone
    } else {
        batch.clone()
    };
    // Fetch v0-1 → Magic::V0 (no per-message timestamps)
    // Fetch v2-3 → Magic::V1 (KIP-32 timestamps)
    let target = if request_version >= 2 {
        Magic::V1
    } else {
        Magic::V0
    };
    let bytes = v2_to_legacy(&working, target).map_err(|e| {
        tracing::warn!(error = %e, "v2_to_legacy failed during fetch down-conversion");
        codes::CORRUPT_MESSAGE
    })?;
    Ok(Some(RecordsPayload::Legacy(bytes)))
}

/// Down-convert a whole records-field payload for a Fetch v<4 requester.
///
/// Obtains the batch list (`Raw` is decoded here — the only place `Raw` is
/// parsed, and only for legacy clients), down-converts each non-dropped
/// batch, and concatenates the resulting legacy `MessageSet` bytes. Returns
/// `Ok(None)` when every batch was dropped (e.g. all control batches).
pub(crate) fn down_convert_payload_for_fetch(
    payload: &RecordsPayload,
    request_version: i16,
) -> Result<Option<RecordsPayload>, i16> {
    let batches: Vec<RecordBatch> = match payload {
        RecordsPayload::V2(b) => b.clone(),
        RecordsPayload::Raw(bytes) => match RecordsPayload::from_bytes(bytes.clone()) {
            Ok(RecordsPayload::V2(b)) => b,
            _ => return Err(crate::codes::CORRUPT_MESSAGE),
        },
        RecordsPayload::Legacy(_) => return Ok(Some(payload.clone())),
        // Unreachable: the zero-copy `FileRegions` payload is only emitted for
        // Fetch v4+, which never down-converts (down-conversion is a v0–v3 path).
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))]
        RecordsPayload::FileRegions(_) => return Err(crate::codes::CORRUPT_MESSAGE),
    };

    let mut out = BytesMut::new();
    for batch in &batches {
        match down_convert_for_fetch(batch, request_version)? {
            Some(RecordsPayload::Legacy(b)) => out.extend_from_slice(&b),
            Some(RecordsPayload::V2(_) | RecordsPayload::Raw(_)) => {
                return Err(crate::codes::CORRUPT_MESSAGE);
            }
            // `down_convert_for_fetch` only ever yields Legacy/V2/Raw.
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Some(RecordsPayload::FileRegions(_)) => {
                return Err(crate::codes::CORRUPT_MESSAGE);
            }
            None => {}
        }
    }
    if out.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RecordsPayload::Legacy(Bytes::from(out))))
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;
    use crabka_protocol::records::{Attributes, Record, RecordBatch};

    use super::*;

    fn make_batch(codec: CompressionType, records: Vec<Record>) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            partition_leader_epoch: -1,
            attributes: Attributes::default().with_compression(codec),
            last_offset_delta: i32::try_from(records.len().saturating_sub(1)).unwrap_or(i32::MAX),
            base_timestamp: 1_700_000_000,
            max_timestamp: 1_700_000_000,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records,
        }
    }

    fn sample_record(key: &'static str, value: &'static str) -> Record {
        Record {
            attributes: 0,
            offset_delta: 0,
            timestamp_delta: 0,
            key: Some(Bytes::from_static(key.as_bytes())),
            value: Some(Bytes::from_static(value.as_bytes())),
            headers: vec![],
        }
    }

    /// version >= 4 returns the V2 batch unchanged
    #[test]
    fn version_gte_4_returns_v2_unchanged() {
        let batch = make_batch(CompressionType::None, vec![sample_record("k", "v")]);
        let result = down_convert_for_fetch(&batch, 5).unwrap();
        let payload = result.expect("should have Some payload");
        match payload {
            RecordsPayload::V2(v) => assert2::assert!(v == vec![batch]),
            _ => panic!("expected V2 for version >= 4"),
        }
    }

    /// version 3 with a control batch returns Ok(None)
    #[test]
    fn control_batch_returns_none() {
        let mut batch = make_batch(CompressionType::None, vec![sample_record("k", "v")]);
        batch.attributes = batch.attributes.with_control(true);
        let result = down_convert_for_fetch(&batch, 3).unwrap();
        assert2::assert!(result.is_none());
    }

    /// version 3 with a zstd batch returns Some(Legacy) with snappy in the wrapper
    #[test]
    fn zstd_batch_recompressed_as_snappy() {
        // Build a batch with 50 records so compression is exercised.
        let records: Vec<Record> = (0..50)
            .map(|i| Record {
                attributes: 0,
                offset_delta: i,
                timestamp_delta: i64::from(i) * 1000,
                key: Some(Bytes::from(format!("key-{i}"))),
                value: Some(Bytes::from(format!("val-{i} hello world this is a test"))),
                headers: vec![],
            })
            .collect();
        let batch = make_batch(CompressionType::Zstd, records);
        let result = down_convert_for_fetch(&batch, 3).unwrap();
        let payload = result.expect("zstd batch should produce Some payload");
        let RecordsPayload::Legacy(bytes) = payload else {
            panic!("expected Legacy for version < 4");
        };
        // The outer wrapper message's attributes byte is at a well-known offset.
        // MessageSet: first message starts at byte 0:
        //   offset(8) + size(4) + crc(4) + magic(1) + attributes(1)
        // attributes byte is at index 17. Snappy codec id is 2 (bits 0-2).
        assert2::assert!(bytes.len() > 17);
        let attrs = bytes[17] & 0x07;
        assert2::assert!(attrs == 2);
    }

    /// version 3 with uncompressed batch returns Legacy bytes that decode to original records
    #[test]
    fn uncompressed_batch_decodes_correctly() {
        use crabka_records_legacy::decode_message_set;

        let batch = make_batch(CompressionType::None, vec![sample_record("hello", "world")]);
        let result = down_convert_for_fetch(&batch, 3).unwrap();
        let payload = result.expect("should have Some payload");
        let RecordsPayload::Legacy(bytes) = payload else {
            panic!("expected Legacy");
        };
        let mut cur: &[u8] = &bytes;
        let recs = decode_message_set(&mut cur, bytes.len()).unwrap();
        let expected = vec![crabka_records_legacy::ParsedRecord {
            offset: crabka_log::Offset(0),
            timestamp: Some(1_700_000_000),
            key: Some(Bytes::from_static(b"hello")),
            value: Some(Bytes::from_static(b"world")),
        }];
        assert2::assert!(recs == expected);
    }

    /// version 0 uses `Magic::V0` (no timestamps)
    #[test]
    fn version_0_uses_magic_v0() {
        use crabka_records_legacy::decode_message_set;

        let mut batch = make_batch(CompressionType::None, vec![sample_record("k", "v")]);
        batch.base_timestamp = 1_700_000_000;
        batch.records[0].timestamp_delta = 500;
        let result = down_convert_for_fetch(&batch, 0).unwrap();
        let payload = result.expect("should have Some payload");
        let RecordsPayload::Legacy(bytes) = payload else {
            panic!("expected Legacy");
        };
        let mut cur: &[u8] = &bytes;
        let recs = decode_message_set(&mut cur, bytes.len()).unwrap();
        // v0 has no timestamps; all timestamps are None
        assert2::assert!(recs[0].timestamp == None);
    }

    /// payload-level conversion concatenates each batch's legacy `MessageSet`
    #[test]
    fn payload_multi_batch_concats_legacy() {
        let b0 = make_batch(CompressionType::None, vec![sample_record("a", "1")]);
        let b1 = make_batch(CompressionType::None, vec![sample_record("b", "2")]);
        let payload = RecordsPayload::V2(vec![b0, b1]);
        let out = down_convert_payload_for_fetch(&payload, 3)
            .unwrap()
            .expect("some");
        let RecordsPayload::Legacy(bytes) = out else {
            panic!("expected Legacy");
        };
        let mut cur: &[u8] = &bytes;
        let recs = crabka_records_legacy::decode_message_set(&mut cur, bytes.len()).unwrap();
        assert2::assert!(recs.len() == 2);
    }
}
