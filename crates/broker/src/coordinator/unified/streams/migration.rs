//! KIP-1071 classic→streams cold conversion. When a drained classic group
//! receives a `StreamsGroupHeartbeat`, the coordinator converts the group in
//! place. It tombstones the classic `GroupMetadata` (k2) and forces the type
//! lock to `Streams`. That tombstone is defensive: it is a no-op when the
//! classic path persisted none. The coordinator KEEPS the drained classic actor
//! in the `groups` registry as the protocol-agnostic offset home, so committed
//! offsets (k0/k1) survive the flip untouched. `OffsetFetch` and `OffsetCommit`
//! route there through `coordinator.find()` for every protocol, streams
//! included.
//!
//! Streams migration is COLD only, because Kafka does not support online
//! streams migration, so there is no hosted-classic-member translation here.

use crabka_protocol::records::RecordBatch;

use super::persistence::{
    encode_current_member_assignment_key, encode_group_metadata_key, encode_member_metadata_key,
    encode_partition_metadata_key, encode_target_assignment_member_key,
    encode_target_assignment_metadata_key, encode_topology_key,
};
use crate::coordinator::unified::{OffsetRecordBatchBuilder, actor::PendingRecords};

/// Result of inspecting a `group_id` for classic→streams conversion.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConvertOutcome {
    /// Not a classic group (fresh streams group or already streams). Serve normally.
    NotClassic,
    /// Was a drained classic group. The coordinator converted it in place to streams.
    Converted,
    /// Classic group has live members. Online streams migration is not supported.
    RejectLiveMembers,
}

/// Result of inspecting a `group_id` for a streams→classic cold downgrade.
/// The mirror of [`ConvertOutcome`] for the opposite direction.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DowngradeOutcome {
    /// Not a streams group. Serve the classic `JoinGroup` normally.
    NotStreams,
    /// Was a drained streams group. The coordinator converted it in place to classic.
    Converted,
    /// Streams group has live members. Online streams migration is not supported.
    RejectLiveMembers,
}

/// Build the single-record batch that tombstones the classic k2 `GroupMetadata`
/// for `group_id`. Reuses the consumer-migration `PendingRecords` encoder so the
/// tombstone key bytes are identical to the upgrade flip's.
pub(crate) fn classic_group_metadata_tombstone_batch(group_id: &str, now_ms: i64) -> RecordBatch {
    PendingRecords {
        classic_group_metadata_tombstone: true,
        ..Default::default()
    }
    .into_batch(group_id, now_ms)
}

/// Build the batch that tombstones every streams record for `group_id`, for the
/// streams→classic downgrade and the type-aware streams delete. This function
/// tombstones the group-level keys unconditionally: k15 `GroupMetadata`, k17
/// `Topology`, k18 `PartitionMetadata`, and k19 `TargetAssignmentMetadata`. A
/// tombstone for a never-written key is a harmless replay no-op. The k15
/// tombstone is load-bearing, because a surviving k15 would resurrect the group
/// as streams. Each id in `member_ids` also tombstones its k16/k20/k21. A
/// drained group has no members, because members tombstone their own per-member
/// records on leave, so `member_ids` is typically empty.
///
/// This function builds the batch directly from the key encoders rather than
/// through `PendingStreamsRecords`. That type's group-level fields are
/// `Option<Value>`, present or absent, with no way to express a group-level
/// null-value tombstone.
pub(crate) fn streams_records_tombstone_batch(
    group_id: &str,
    member_ids: &[String],
    now_ms: i64,
) -> RecordBatch {
    let mut keys = vec![
        encode_group_metadata_key(group_id),
        encode_topology_key(group_id),
        encode_partition_metadata_key(group_id),
        encode_target_assignment_metadata_key(group_id),
    ];
    for mid in member_ids {
        keys.push(encode_member_metadata_key(group_id, mid));
        keys.push(encode_target_assignment_member_key(group_id, mid));
        keys.push(encode_current_member_assignment_key(group_id, mid));
    }

    let mut batch = OffsetRecordBatchBuilder::default();
    for key in keys {
        batch.push(key, None);
    }
    batch.finish(now_ms)
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

    #[test]
    fn streams_tombstone_batch_group_level_only() {
        let batch = streams_records_tombstone_batch("g", &[], 123);
        // k15 GroupMetadata, k17 Topology, k18 PartitionMetadata, k19 TargetAssignmentMetadata.
        assert_eq!(batch.records.len(), 4, "four group-level tombstones");
        assert_eq!(batch.max_timestamp, 123);
        assert_eq!(batch.last_offset_delta, 3);
        for r in &batch.records {
            assert!(r.key.is_some(), "every record carries a key");
            assert!(
                r.value.is_none(),
                "every record is a tombstone (null value)"
            );
        }
        // The first record is the load-bearing k15 GroupMetadata tombstone.
        let k15 = batch.records[0].key.as_ref().unwrap();
        assert_eq!(
            &k15[..2],
            &15i16.to_be_bytes(),
            "k15 GroupMetadata key version"
        );
    }

    #[test]
    fn streams_tombstone_batch_includes_per_member_records() {
        let batch = streams_records_tombstone_batch("g", &["m1".to_string()], 1);
        // 4 group-level + k16/k20/k21 for m1 = 7.
        assert_eq!(
            batch.records.len(),
            7,
            "group-level + 3 per-member tombstones"
        );
        assert!(batch.records.iter().all(|r| r.value.is_none()));
    }
}
