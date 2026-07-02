//! The data model exchanged across the two tiered-storage SPIs.
//!
//! Shapes mirror Apache Kafka's `storage-api`
//! (`org.apache.kafka.server.log.remote.storage`): [`TopicIdPartition`],
//! [`RemoteLogSegmentId`], [`RemoteLogSegmentMetadata`] +
//! [`RemoteLogSegmentMetadataUpdate`], the [`RemoteLogSegmentState`]
//! lifecycle, and the partition-delete lifecycle
//! ([`RemotePartitionDeleteMetadata`] / [`RemotePartitionDeleteState`]).

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use uuid::Uuid;

use crate::error::RemoteStorageError;

/// A partition addressed by its stable topic UUID (plus the topic name for
/// diagnostics).
///
/// Equality and hashing are by `topic_id` + `partition` only — the topic
/// name is informational and a topic's id is its identity, matching
/// Kafka's `TopicIdPartition`.
#[derive(Debug, Clone)]
pub struct TopicIdPartition {
    /// Stable topic UUID, as assigned at topic creation.
    pub topic_id: Uuid,
    /// Topic name (informational; not part of identity).
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

impl TopicIdPartition {
    /// Construct a [`TopicIdPartition`].
    #[must_use]
    pub fn new(topic_id: Uuid, topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic_id,
            topic: topic.into(),
            partition,
        }
    }
}

impl PartialEq for TopicIdPartition {
    fn eq(&self, other: &Self) -> bool {
        self.topic_id == other.topic_id && self.partition == other.partition
    }
}

impl Eq for TopicIdPartition {}

impl Hash for TopicIdPartition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic_id.hash(state);
        self.partition.hash(state);
    }
}

/// Globally-unique identifier for one remote log segment: the owning
/// partition plus a random per-segment UUID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteLogSegmentId {
    /// The partition this segment belongs to.
    pub topic_id_partition: TopicIdPartition,
    /// Random per-segment UUID.
    pub id: Uuid,
}

impl RemoteLogSegmentId {
    /// Construct a [`RemoteLogSegmentId`] from an explicit UUID.
    #[must_use]
    pub fn new(topic_id_partition: TopicIdPartition, id: Uuid) -> Self {
        Self {
            topic_id_partition,
            id,
        }
    }
}

/// Lifecycle state of a remote log segment.
///
/// Valid transitions (see [`RemoteLogSegmentState::is_valid_transition`]):
///
/// ```text
/// CopySegmentStarted ──► CopySegmentFinished ──► DeleteSegmentStarted ──► DeleteSegmentFinished
///         └───────────────────────────────────►┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemoteLogSegmentState {
    /// A copy to the remote tier has begun but not finished. The starting
    /// state of every segment.
    CopySegmentStarted,
    /// The copy finished; the segment is durable in the remote tier and
    /// readable.
    CopySegmentFinished,
    /// Deletion from the remote tier has begun.
    DeleteSegmentStarted,
    /// The segment has been fully removed from the remote tier.
    DeleteSegmentFinished,
}

impl RemoteLogSegmentState {
    /// `true` if a segment currently in `self` may transition to `target`.
    ///
    /// A same-state "transition" is not valid (callers treat it as a
    /// no-op / duplicate, not an advance).
    #[must_use]
    pub fn is_valid_transition(self, target: Self) -> bool {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        matches!(
            (self, target),
            (
                CopySegmentStarted,
                CopySegmentFinished | DeleteSegmentStarted
            ) | (CopySegmentFinished, DeleteSegmentStarted)
                | (DeleteSegmentStarted, DeleteSegmentFinished)
        )
    }
}

/// Opaque bytes an [`RemoteStorageManager`](crate::RemoteStorageManager)
/// may return from `copy_log_segment_data` and have echoed back on every
/// later call for that segment (e.g. an object-store key or version id).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CustomMetadata(pub Vec<u8>);

/// Metadata describing one segment stored (or being stored) in the remote
/// tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLogSegmentMetadata {
    remote_log_segment_id: RemoteLogSegmentId,
    start_offset: i64,
    end_offset: i64,
    max_timestamp_ms: i64,
    broker_id: i32,
    event_timestamp_ms: i64,
    segment_size_in_bytes: i32,
    custom_metadata: Option<CustomMetadata>,
    state: RemoteLogSegmentState,
    segment_leader_epochs: BTreeMap<i32, i64>,
    /// KIP-405 `txnIndexEmpty`: `true` when the segment carries no transaction
    /// index. Serialized as tagged field (tag 0) in the JVM record format.
    /// Defaults to `false`.
    txn_index_empty: bool,
}

impl RemoteLogSegmentMetadata {
    /// Construct a [`RemoteLogSegmentMetadata`].
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] when
    /// `segment_leader_epochs` is empty, `end_offset < start_offset`, or
    /// `segment_size_in_bytes < 0`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        remote_log_segment_id: RemoteLogSegmentId,
        start_offset: i64,
        end_offset: i64,
        max_timestamp_ms: i64,
        broker_id: i32,
        event_timestamp_ms: i64,
        segment_size_in_bytes: i32,
        state: RemoteLogSegmentState,
        segment_leader_epochs: BTreeMap<i32, i64>,
    ) -> Result<Self, RemoteStorageError> {
        if segment_leader_epochs.is_empty() {
            return Err(RemoteStorageError::InvalidArgument(
                "segment_leader_epochs must not be empty".into(),
            ));
        }
        if end_offset < start_offset {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "end_offset ({end_offset}) < start_offset ({start_offset})"
            )));
        }
        if segment_size_in_bytes < 0 {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "segment_size_in_bytes ({segment_size_in_bytes}) must be >= 0"
            )));
        }
        Ok(Self {
            remote_log_segment_id,
            start_offset,
            end_offset,
            max_timestamp_ms,
            broker_id,
            event_timestamp_ms,
            segment_size_in_bytes,
            custom_metadata: None,
            state,
            segment_leader_epochs,
            txn_index_empty: false,
        })
    }

    /// Apply a [`RemoteLogSegmentMetadataUpdate`], returning the updated
    /// copy. The update advances `state`, refreshes `event_timestamp_ms`
    /// and `broker_id`, and replaces `custom_metadata` when the update
    /// carries `Some`.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidArgument`] if the update's
    /// segment id does not match, or
    /// [`RemoteStorageError::InvalidSegmentTransition`] if the state change
    /// is not permitted from the current state.
    pub fn with_update(
        &self,
        update: &RemoteLogSegmentMetadataUpdate,
    ) -> Result<Self, RemoteStorageError> {
        if update.remote_log_segment_id != self.remote_log_segment_id {
            return Err(RemoteStorageError::InvalidArgument(
                "update segment id does not match metadata segment id".into(),
            ));
        }
        if !self.state.is_valid_transition(update.state) {
            return Err(RemoteStorageError::InvalidSegmentTransition {
                id: self.remote_log_segment_id.clone(),
                from: self.state,
                to: update.state,
            });
        }
        let mut next = self.clone();
        next.state = update.state;
        next.event_timestamp_ms = update.event_timestamp_ms;
        next.broker_id = update.broker_id;
        if update.custom_metadata.is_some() {
            next.custom_metadata.clone_from(&update.custom_metadata);
        }
        Ok(next)
    }

    /// The segment's unique id.
    #[must_use]
    pub fn remote_log_segment_id(&self) -> &RemoteLogSegmentId {
        &self.remote_log_segment_id
    }

    /// First offset (inclusive) covered by this segment.
    #[must_use]
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Last offset (inclusive) covered by this segment.
    #[must_use]
    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }

    /// Highest record timestamp in this segment.
    #[must_use]
    pub fn max_timestamp_ms(&self) -> i64 {
        self.max_timestamp_ms
    }

    /// Id of the broker that produced this metadata.
    #[must_use]
    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    /// Wall-clock time the latest event for this segment was created.
    #[must_use]
    pub fn event_timestamp_ms(&self) -> i64 {
        self.event_timestamp_ms
    }

    /// Size of the `.log` data in bytes.
    #[must_use]
    pub fn segment_size_in_bytes(&self) -> i32 {
        self.segment_size_in_bytes
    }

    /// Opaque metadata the [`RemoteStorageManager`](crate::RemoteStorageManager)
    /// attached at copy time, if any.
    #[must_use]
    pub fn custom_metadata(&self) -> Option<&CustomMetadata> {
        self.custom_metadata.as_ref()
    }

    /// Current lifecycle state.
    #[must_use]
    pub fn state(&self) -> RemoteLogSegmentState {
        self.state
    }

    /// Map of leader epoch → first offset that epoch contributed to this
    /// segment.
    #[must_use]
    pub fn segment_leader_epochs(&self) -> &BTreeMap<i32, i64> {
        &self.segment_leader_epochs
    }

    /// Attach custom metadata (builder-style; used by RSM copy paths that
    /// produce a key before recording `CopySegmentFinished`).
    #[must_use]
    pub fn with_custom_metadata(mut self, custom: CustomMetadata) -> Self {
        self.custom_metadata = Some(custom);
        self
    }

    /// `true` if the segment has no transaction index (KIP-405 `txnIndexEmpty`).
    /// Defaults to `false`. Serialized as the JVM record's tagged field (tag 0).
    #[must_use]
    pub fn txn_index_empty(&self) -> bool {
        self.txn_index_empty
    }

    /// Builder-style setter for [`Self::txn_index_empty`].
    #[must_use]
    pub fn with_txn_index_empty(mut self, empty: bool) -> Self {
        self.txn_index_empty = empty;
        self
    }
}

/// An update to an existing [`RemoteLogSegmentMetadata`]'s lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteLogSegmentMetadataUpdate {
    /// The segment being updated.
    pub remote_log_segment_id: RemoteLogSegmentId,
    /// Wall-clock time of this update.
    pub event_timestamp_ms: i64,
    /// New custom metadata, when the update introduces or changes it.
    pub custom_metadata: Option<CustomMetadata>,
    /// The new lifecycle state.
    pub state: RemoteLogSegmentState,
    /// Broker that produced this update.
    pub broker_id: i32,
}

/// Lifecycle state of a remote *partition* deletion.
///
/// ```text
/// DeletePartitionMarked ──► DeletePartitionStarted ──► DeletePartitionFinished
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemotePartitionDeleteState {
    /// The partition has been marked for deletion of all its remote
    /// segments.
    DeletePartitionMarked,
    /// Deletion of the partition's remote segments has begun.
    DeletePartitionStarted,
    /// All remote segments for the partition have been deleted.
    DeletePartitionFinished,
}

impl RemotePartitionDeleteState {
    /// `true` if a partition currently in `from` (or never marked, when
    /// `from` is `None`) may transition to `target`.
    #[must_use]
    pub fn is_valid_transition(from: Option<Self>, target: Self) -> bool {
        use RemotePartitionDeleteState::{
            DeletePartitionFinished, DeletePartitionMarked, DeletePartitionStarted,
        };
        matches!(
            (from, target),
            (None, DeletePartitionMarked)
                | (Some(DeletePartitionMarked), DeletePartitionStarted)
                | (Some(DeletePartitionStarted), DeletePartitionFinished)
        )
    }
}

/// Metadata describing the deletion lifecycle of a partition's remote data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePartitionDeleteMetadata {
    /// The partition being deleted from the remote tier.
    pub topic_id_partition: TopicIdPartition,
    /// Current deletion state.
    pub state: RemotePartitionDeleteState,
    /// Wall-clock time of this event.
    pub event_timestamp_ms: i64,
    /// Broker that produced this metadata.
    pub broker_id: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use assert2::check;
    use std::collections::HashSet;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn seg_id() -> RemoteLogSegmentId {
        RemoteLogSegmentId::new(tp(), Uuid::from_u128(99))
    }

    fn epochs() -> BTreeMap<i32, i64> {
        BTreeMap::from([(0, 0)])
    }

    #[test]
    fn topic_id_partition_identity_ignores_name() {
        let a = TopicIdPartition::new(Uuid::from_u128(7), "alpha", 3);
        let b = TopicIdPartition::new(Uuid::from_u128(7), "renamed", 3);
        assert!(a == b);
        let set: HashSet<_> = [a, b].into_iter().collect();
        assert!(set.len() == 1, "same id+partition must collapse in a set");
    }

    #[test]
    fn topic_id_partition_distinct_partitions_differ() {
        let a = TopicIdPartition::new(Uuid::from_u128(7), "alpha", 0);
        let b = TopicIdPartition::new(Uuid::from_u128(7), "alpha", 1);
        assert!(a != b);
    }

    #[test]
    fn accessors_return_constructed_values() {
        // max_timestamp_ms / segment_size_in_bytes accessors were never read
        // back in the suite; pin them to distinct non-default values.
        let md = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            100,
            777, // max_timestamp_ms
            5,
            888,
            4096, // segment_size_in_bytes
            RemoteLogSegmentState::CopySegmentStarted,
            epochs(),
        )
        .unwrap();
        assert!(md.max_timestamp_ms() == 777);
        assert!(md.segment_size_in_bytes() == 4096);
    }

    #[test]
    fn segment_state_valid_transitions() {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        for (from, to) in [
            (CopySegmentStarted, CopySegmentFinished),
            (CopySegmentStarted, DeleteSegmentStarted),
            (CopySegmentFinished, DeleteSegmentStarted),
            (DeleteSegmentStarted, DeleteSegmentFinished),
        ] {
            check!(from.is_valid_transition(to), "{from:?} -> {to:?}");
        }
    }

    #[test]
    fn segment_state_invalid_transitions() {
        use RemoteLogSegmentState::{
            CopySegmentFinished, CopySegmentStarted, DeleteSegmentFinished, DeleteSegmentStarted,
        };
        // No backward / skipping / same-state transitions.
        for (from, to) in [
            (CopySegmentStarted, CopySegmentStarted),
            (CopySegmentStarted, DeleteSegmentFinished),
            (CopySegmentFinished, CopySegmentStarted),
            (CopySegmentFinished, CopySegmentFinished),
            (DeleteSegmentStarted, CopySegmentFinished),
            (DeleteSegmentFinished, DeleteSegmentStarted),
        ] {
            check!(!from.is_valid_transition(to), "{from:?} -> {to:?}");
        }
    }

    #[test]
    fn metadata_rejects_empty_leader_epochs() {
        let err = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            10,
            123,
            1,
            456,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::new(),
        )
        .unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
    }

    #[test]
    fn metadata_rejects_end_before_start() {
        let err = RemoteLogSegmentMetadata::new(
            seg_id(),
            10,
            5,
            123,
            1,
            456,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs(),
        )
        .unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
    }

    #[test]
    fn with_update_advances_state_and_fields() {
        let started = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            10,
            123,
            1,
            456,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs(),
        )
        .unwrap();
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(),
            event_timestamp_ms: 789,
            custom_metadata: Some(CustomMetadata(vec![1, 2, 3])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 2,
        };
        let finished = started.with_update(&update).unwrap();
        check!(finished.state() == RemoteLogSegmentState::CopySegmentFinished);
        check!(finished.event_timestamp_ms() == 789);
        check!(finished.broker_id() == 2);
        check!(finished.custom_metadata() == Some(&CustomMetadata(vec![1, 2, 3])));
        // Untouched fields survive.
        check!(finished.start_offset() == 0);
        check!(finished.end_offset() == 10);
    }

    #[test]
    fn with_update_keeps_custom_metadata_when_update_omits_it() {
        let started = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            10,
            123,
            1,
            456,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs(),
        )
        .unwrap()
        .with_custom_metadata(CustomMetadata(vec![9]));
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(),
            event_timestamp_ms: 789,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 2,
        };
        let finished = started.with_update(&update).unwrap();
        assert!(finished.custom_metadata() == Some(&CustomMetadata(vec![9])));
    }

    #[test]
    fn with_update_rejects_invalid_transition() {
        let started = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            10,
            123,
            1,
            456,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs(),
        )
        .unwrap();
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(),
            event_timestamp_ms: 789,
            custom_metadata: None,
            state: RemoteLogSegmentState::DeleteSegmentFinished,
            broker_id: 2,
        };
        let err = started.with_update(&update).unwrap_err();
        assert!(matches!(
            err,
            RemoteStorageError::InvalidSegmentTransition { .. }
        ));
    }

    #[test]
    fn with_update_rejects_mismatched_id() {
        let started = RemoteLogSegmentMetadata::new(
            seg_id(),
            0,
            10,
            123,
            1,
            456,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs(),
        )
        .unwrap();
        let other = RemoteLogSegmentId::new(tp(), Uuid::from_u128(1234));
        let update = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: other,
            event_timestamp_ms: 789,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 2,
        };
        let err = started.with_update(&update).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidArgument(_)));
    }

    #[test]
    fn txn_index_empty_defaults_false_and_is_settable() {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "t", 0),
                Uuid::from_u128(2),
            ),
            0,
            9,
            9,
            1,
            100,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0, 0)]),
        )
        .unwrap();
        assert!(!md.txn_index_empty());
        let md = md.with_txn_index_empty(true);
        assert!(md.txn_index_empty());
    }

    #[test]
    fn partition_delete_transitions() {
        use RemotePartitionDeleteState::{
            DeletePartitionFinished, DeletePartitionMarked, DeletePartitionStarted,
        };
        for (from, to, want) in [
            (None, DeletePartitionMarked, true),
            (Some(DeletePartitionMarked), DeletePartitionStarted, true),
            (Some(DeletePartitionStarted), DeletePartitionFinished, true),
            // Invalid: skipping, restarting, or marking twice.
            (None, DeletePartitionStarted, false),
            (Some(DeletePartitionMarked), DeletePartitionMarked, false),
            (Some(DeletePartitionFinished), DeletePartitionStarted, false),
        ] {
            check!(
                RemotePartitionDeleteState::is_valid_transition(from, to) == want,
                "{from:?} -> {to:?}"
            );
        }
    }
}
