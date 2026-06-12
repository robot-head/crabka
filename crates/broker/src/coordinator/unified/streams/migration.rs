//! KIP-1071 classic→streams cold conversion. When a drained classic group
//! receives a `StreamsGroupHeartbeat`, the group is converted in place: the
//! classic `GroupMetadata` (k2) is tombstoned, the type lock is forced to
//! `Streams`, and the classic actor is dropped. Committed offsets (k0/k1) are
//! protocol-agnostic and are left untouched. Streams migration is COLD only
//! (Kafka does not support online streams migration), so there is no
//! hosted-classic-member translation here.

use crabka_protocol::records::RecordBatch;

use crate::coordinator::unified::actor::PendingRecords;

/// Result of inspecting a `group_id` for classic→streams conversion.
#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)] // consumed by try_convert_classic_to_streams; caller wired in Task 4
pub(crate) enum ConvertOutcome {
    /// Not a classic group (fresh streams group or already streams) — serve normally.
    NotClassic,
    /// Was a drained classic group; converted in place to streams.
    Converted,
    /// Classic group has live members — online streams migration is unsupported.
    RejectLiveMembers,
}

/// Build the single-record batch that tombstones the classic k2 `GroupMetadata`
/// for `group_id`. Reuses the consumer-migration `PendingRecords` encoder so the
/// tombstone key bytes are identical to the upgrade flip's.
#[allow(dead_code)] // called by try_convert_classic_to_streams; caller wired in Task 4
pub(crate) fn classic_group_metadata_tombstone_batch(group_id: &str, now_ms: i64) -> RecordBatch {
    PendingRecords {
        classic_group_metadata_tombstone: true,
        ..Default::default()
    }
    .into_batch(group_id, now_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_batch_has_one_null_value_k2_record() {
        let batch = classic_group_metadata_tombstone_batch("g", 123);
        assert_eq!(batch.records.len(), 1, "exactly one record");
        let r = &batch.records[0];
        assert!(r.key.is_some(), "k2 GroupMetadata key present");
        assert!(r.value.is_none(), "tombstone = null value");
        let key = r.key.as_ref().unwrap();
        assert_eq!(
            &key[..2],
            &2i16.to_be_bytes(),
            "classic GroupMetadata key version 2"
        );
    }
}
