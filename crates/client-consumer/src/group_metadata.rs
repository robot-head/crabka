//! KIP-447 consumer group metadata for a transactional producer's
//! `send_offsets_to_transaction`.
//!
//! The group coordinator uses it to fence zombie producers with the consumer
//! group's generation for a classic group, or with the member epoch for a
//! KIP-848 next-gen group. This removes the need for one producer per input
//! partition.

/// The identity that a consumer presents to a transactional producer for
/// KIP-447 offset-commit fencing.
///
/// This mirrors the JVM's
/// `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    /// The consumer group id.
    pub group_id: String,
    /// Classic-group generation id, or the member epoch for a KIP-848 next-gen
    /// group. The client sends it verbatim in
    /// `TxnOffsetCommitRequest.generation_id`, which matches the JVM wire
    /// convention. The coordinator interprets it per group kind.
    pub generation_id: i32,
    /// The member id that the coordinator assigned at join time. It is empty
    /// for a simple consumer, which uses manual assignment and has no group
    /// membership.
    pub member_id: String,
    /// `group.instance.id` for a static member. `None` for a dynamic member.
    pub group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Metadata for a producer that commits offsets to a group it is not a
    /// member of, that is a manual partition assignment or a simple consumer.
    ///
    /// The group coordinator applies no generation or member fencing to this
    /// shape.
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
        assert2::assert!(
            m == ConsumerGroupMetadata {
                group_id: "g".into(),
                generation_id: -1,
                member_id: String::new(),
                group_instance_id: None,
            }
        );
    }
}
