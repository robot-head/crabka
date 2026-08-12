//! Kafka-compatible `__consumer_offsets` partition selection.

use crabka_metadata::MetadataImage;

use super::bootstrap::{OFFSETS_NUM_PARTITIONS, OFFSETS_TOPIC};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GroupRoutingError {
    Unavailable,
    NotCoordinator,
}

/// Java's `String.hashCode`, including its UTF-16 code-unit semantics.
/// Kafka uses this hash for group coordinator partitioning.
#[must_use]
pub(crate) fn java_string_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// Select a group-id's `__consumer_offsets` partition with Kafka's
/// `Utils.abs(groupId.hashCode()) % partitionCount` rule.
#[must_use]
pub(crate) fn partition_for_group_with_count(group_id: &str, partition_count: i32) -> i32 {
    debug_assert!(partition_count > 0);
    let hash = java_string_hash(group_id);
    let positive = if hash == i32::MIN { 0 } else { hash.abs() };
    positive % partition_count.max(1)
}

/// Select from the live offsets-topic partition count, falling back to the
/// bootstrap default while metadata is not available yet.
#[must_use]
pub(crate) fn partition_for_group(image: &MetadataImage, group_id: &str) -> i32 {
    let count = image.topic_partition_count(OFFSETS_TOPIC);
    partition_for_group_with_count(
        group_id,
        if count > 0 {
            count
        } else {
            OFFSETS_NUM_PARTITIONS
        },
    )
}

/// Resolve the group partition and verify that `node_id` is its current
/// leader. Group RPC handlers use this before they create or access actors.
pub(crate) fn local_partition_for_group(
    image: &MetadataImage,
    node_id: crabka_raft::NodeId,
    group_id: &str,
) -> Result<i32, GroupRoutingError> {
    let partition = partition_for_group(image, group_id);
    let record = image
        .partition(OFFSETS_TOPIC, partition)
        .ok_or(GroupRoutingError::Unavailable)?;
    if record.leader != node_id {
        return Err(GroupRoutingError::NotCoordinator);
    }
    Ok(partition)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn java_hash_matches_known_jdk_values_including_utf16_surrogates() {
        check!(java_string_hash("") == 0);
        check!(java_string_hash("abc") == 96_354);
        check!(java_string_hash("consumer-group") == -1_738_392_088);
        // Java hashes the two UTF-16 surrogate code units for this character.
        check!(java_string_hash("🦀") == 1_772_802);
    }

    #[test]
    fn group_partition_uses_java_hash_and_requested_count() {
        check!(partition_for_group_with_count("consumer-group", 50) == 38);
        check!(partition_for_group_with_count("abc", 7) == 6);
        check!(partition_for_group_with_count("abc", 1) == 0);
    }
}
