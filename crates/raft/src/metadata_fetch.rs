//! Encoding committed `__cluster_metadata` log entries as Kafka record
//! batches for the observer-fetch RPC (Component B).
//!
//! Each openraft log entry becomes one `RecordBatch` with
//! `base_offset == log_id.index` and `last_offset_delta == 0`. A
//! `Normal` entry's `AppData.records` become one `Record` each (via the
//! `crabka_metadata` bridge); `Blank`/`Membership` entries become empty
//! batches so the observer's fetch offset still advances past them.

use bytes::{BufMut, Bytes, BytesMut};
use openraft::{Entry, EntryPayload};

use crabka_metadata::to_kafka_record;
use crabka_protocol::records::RecordBatch;

use crate::types::TypeConfig;

/// A committed-range read result handed back by the controller's
/// metadata-fetch path. `records` is a concatenation of `RecordBatch`es
/// (one per log entry); `log_start_offset` and `high_watermark` are
/// openraft log indices.
#[derive(Debug, Clone)]
pub struct MetadataFetchSlice {
    pub records: Bytes,
    pub log_start_offset: u64,
    pub high_watermark: u64,
}

/// Encode committed log entries as concatenated Kafka record batches,
/// stopping once `max_bytes` would be exceeded (but always emitting at
/// least the first entry so the observer makes progress).
#[must_use]
pub fn encode_committed_records(entries: &[Entry<TypeConfig>], max_bytes: usize) -> Bytes {
    let mut out = BytesMut::new();
    for (i, entry) in entries.iter().enumerate() {
        let records = match &entry.payload {
            EntryPayload::Normal(data) => data
                .records
                .iter()
                .filter_map(|r| to_kafka_record(r).ok())
                .collect(),
            EntryPayload::Blank | EntryPayload::Membership(_) => Vec::new(),
        };
        let batch = RecordBatch {
            base_offset: i64::try_from(entry.log_id.index).unwrap_or(i64::MAX),
            last_offset_delta: 0,
            records,
            ..Default::default()
        };
        let mut scratch = BytesMut::new();
        if batch.encode(&mut scratch).is_err() {
            break;
        }
        // Always emit the first batch; afterwards respect max_bytes.
        if i > 0 && out.len() + scratch.len() > max_bytes {
            break;
        }
        out.put_slice(&scratch);
    }
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, TopicRecord, from_kafka_record};
    use crabka_protocol::records::RecordBatch as OwnedBatch;
    use openraft::{LeaderId, LogId};
    use uuid::Uuid;

    use crate::types::AppData;

    fn normal_entry(index: u64, topic: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: LeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Normal(AppData {
                records: vec![MetadataRecord::V1Topic(TopicRecord {
                    name: topic.into(),
                    topic_id: Uuid::from_u128(u128::from(index)),
                    partitions: 1,
                    replication_factor: 1,
                })],
            }),
        }
    }

    fn blank_entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId {
                leader_id: LeaderId::new(1, 1),
                index,
            },
            payload: EntryPayload::Blank,
        }
    }

    fn decode_all(mut buf: &[u8]) -> Vec<OwnedBatch> {
        let mut out = Vec::new();
        while !buf.is_empty() {
            let batch = OwnedBatch::decode(&mut buf).expect("decode batch");
            out.push(batch);
        }
        out
    }

    #[test]
    fn encodes_one_batch_per_entry_with_base_offset() {
        let entries = vec![normal_entry(1, "a"), blank_entry(2), normal_entry(3, "b")];
        let bytes = encode_committed_records(&entries, usize::MAX);
        let batches = decode_all(&bytes);
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].base_offset, 1);
        assert_eq!(batches[1].base_offset, 2);
        assert_eq!(batches[2].base_offset, 3);
        // Blank entry -> empty batch.
        assert_eq!(batches[1].records.len(), 0);
        // Normal entry -> one decodable MetadataRecord.
        let rec = from_kafka_record(&batches[0].records[0]).expect("decode record");
        assert!(matches!(rec, MetadataRecord::V1Topic(t) if t.name == "a"));
    }

    #[test]
    fn max_bytes_truncates_but_always_emits_first() {
        let entries = vec![normal_entry(1, "a"), normal_entry(2, "b")];
        // max_bytes = 1 forces truncation after the first batch.
        let bytes = encode_committed_records(&entries, 1);
        let batches = decode_all(&bytes);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].base_offset, 1);
    }
}
