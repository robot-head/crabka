//! [`RlmmCacheDump`] — a flat, owned snapshot of an
//! [`InmemoryRemoteLogMetadataManager`](crate::InmemoryRemoteLogMetadataManager)'s
//! cache, used by the topic-backed manager's on-disk snapshot
//! (slice 48p). Unlike the live mutation path, importing a dump
//! bypasses lifecycle-transition validation: the dumped states are
//! already the product of valid transitions, so re-applying them
//! through `add`/`update` would wrongly reject terminal states.

use crate::metadata::{RemoteLogSegmentMetadata, RemotePartitionDeleteState, TopicIdPartition};

/// Every partition's cache contents, flattened for snapshotting.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RlmmCacheDump {
    /// One entry per partition that has any cached state.
    pub partitions: Vec<PartitionDump>,
}

/// One partition's dumped cache: all of its segments (every lifecycle
/// state, terminal included) plus its partition-delete state, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDump {
    /// The partition this dump belongs to.
    pub topic_id_partition: TopicIdPartition,
    /// Every segment currently tracked for this partition, in no
    /// particular order (import re-derives ordering / epoch index).
    pub segments: Vec<RemoteLogSegmentMetadata>,
    /// Partition-delete lifecycle state, if the partition was ever
    /// marked for deletion.
    pub delete_state: Option<RemotePartitionDeleteState>,
}
