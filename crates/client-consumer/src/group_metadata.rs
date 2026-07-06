//! KIP-447 consumer group metadata, handed to a transactional producer's
//! `send_offsets_to_transaction` so the group coordinator can fence zombie
//! producers via the consumer group's generation (classic) or member epoch
//! (KIP-848 next-gen), instead of requiring one producer per input partition.

/// The identity a consumer presents to a transactional producer for KIP-447
/// offset-commit fencing. Mirrors the JVM's
/// `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    /// The consumer group id.
    pub group_id: String,
    /// Classic-group generation id, or — for a KIP-848 next-gen group — the
    /// member epoch. Sent verbatim in `TxnOffsetCommitRequest.generation_id`
    /// (matches the JVM wire convention); the coordinator interprets it per
    /// group kind.
    pub generation_id: i32,
    /// The member id assigned by the coordinator at join time. Empty for a
    /// simple consumer (manual assignment, no group membership).
    pub member_id: String,
    /// `group.instance.id` for static members; `None` for dynamic members.
    pub group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Metadata for a producer committing offsets to a group it is not a
    /// member of (manual partition assignment / simple consumer). The group
    /// coordinator applies no generation/member fencing to this shape.
    #[must_use]
    pub fn for_group(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_group_is_simple_consumer_shape() {
        let m = ConsumerGroupMetadata::for_group("g");
        assert_eq!(
            m,
            ConsumerGroupMetadata {
                group_id: "g".into(),
                generation_id: -1,
                member_id: String::new(),
                group_instance_id: None,
            }
        );
    }
}
