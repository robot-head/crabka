//! [`InmemoryRemoteLogMetadataManager`] — a process-memory reference
//! [`RemoteLogMetadataManager`], mirroring Kafka's test fixture of the same
//! name. Tiered-storage tests run against this manager.

use std::{collections::HashMap, sync::Mutex};

use crate::{
    cache::RemoteLogMetadataCache,
    dump::{PartitionDump, RlmmCacheDump},
    error::RemoteStorageError,
    metadata::{
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemotePartitionDeleteMetadata,
        RemotePartitionDeleteState, TopicIdPartition,
    },
    metadata_manager::RemoteLogMetadataManager,
};

/// In-memory [`RemoteLogMetadataManager`]: one
/// `RemoteLogMetadataCache` per partition behind a single
/// mutex. Not durable — state is lost on restart — but enforces the full
/// lifecycle state machine, so it is a faithful stand-in for the
/// topic-backed production manager in tests and single-process setups.
#[derive(Debug, Default)]
pub struct InmemoryRemoteLogMetadataManager {
    partitions: Mutex<HashMap<TopicIdPartition, RemoteLogMetadataCache>>,
}

impl InmemoryRemoteLogMetadataManager {
    /// Construct an empty manager.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Dump every partition's segment + partition-delete metadata for
    /// snapshotting. The result is order-independent;
    /// [`Self::import`] re-derives ordering and the epoch index.
    #[must_use]
    pub fn export(&self) -> RlmmCacheDump {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        let mut partitions: Vec<PartitionDump> = guard
            .iter()
            .map(|(tp, cache)| PartitionDump {
                topic_id_partition: tp.clone(),
                segments: cache.dump_segments(),
                delete_state: cache.delete_state(),
            })
            .collect();
        // Stable order so `export()` is deterministic and comparable.
        partitions.sort_by(|a, b| {
            (
                a.topic_id_partition.topic_id,
                a.topic_id_partition.partition,
            )
                .cmp(&(
                    b.topic_id_partition.topic_id,
                    b.topic_id_partition.partition,
                ))
        });
        // Within a partition, sort segments by (start_offset, id) so the
        // dump is canonical regardless of HashMap iteration order.
        for p in &mut partitions {
            p.segments.sort_by(|a, b| {
                a.start_offset().cmp(&b.start_offset()).then_with(|| {
                    a.remote_log_segment_id()
                        .id
                        .cmp(&b.remote_log_segment_id().id)
                })
            });
        }
        RlmmCacheDump { partitions }
    }

    /// Seed the cache from a dump, bypassing transition validation
    /// Intended for a freshly-constructed manager during
    /// snapshot restore; existing partitions are overwritten.
    pub fn import(&self, dump: RlmmCacheDump) {
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        for p in dump.partitions {
            let cache = guard.entry(p.topic_id_partition).or_default();
            cache.seed(p.segments, p.delete_state);
        }
    }
}

impl RemoteLogMetadataManager for InmemoryRemoteLogMetadataManager {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let tp = metadata.remote_log_segment_id().topic_id_partition.clone();
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        guard.entry(tp).or_default().add(metadata)
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let tp = update.remote_log_segment_id.topic_id_partition.clone();
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        match guard.get_mut(&tp) {
            Some(cache) => cache.update(&update),
            None => Err(RemoteStorageError::SegmentNotFound(
                update.remote_log_segment_id,
            )),
        }
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .and_then(|c| c.segment_for(leader_epoch, offset)))
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Option<i64>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .and_then(|c| c.highest_offset_for_epoch(leader_epoch)))
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .map(RemoteLogMetadataCache::list)
            .unwrap_or_default())
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        Ok(guard
            .get(topic_id_partition)
            .map(|c| c.list_by_epoch(leader_epoch))
            .unwrap_or_default())
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        let cache = guard
            .entry(metadata.topic_id_partition.clone())
            .or_default();
        let from = cache.delete_state();
        if !RemotePartitionDeleteState::is_valid_transition(from, metadata.state) {
            return Err(RemoteStorageError::InvalidPartitionDeleteTransition {
                tp: metadata.topic_id_partition,
                from,
                to: metadata.state,
            });
        }
        cache.set_delete_state(metadata.state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::{assert, check};
    use uuid::Uuid;

    use super::*;
    use crate::metadata::{
        CustomMetadata, RemoteLogSegmentId, RemoteLogSegmentState, RemotePartitionDeleteState,
    };

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn started(id: u128, start: i64, end: i64) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            end,
            1,
            100,
            2048,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0, start)]),
        )
        .unwrap()
    }

    fn finish(id: u128) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: 200,
            custom_metadata: Some(CustomMetadata(vec![7])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        }
    }

    #[test]
    fn add_finish_query_round_trip() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();

        let got = m
            .remote_log_segment_metadata(&tp(), 0, 42)
            .unwrap()
            .expect("segment found");
        check!(got.remote_log_segment_id().id == Uuid::from_u128(10));
        check!(got.custom_metadata() == Some(&CustomMetadata(vec![7])));
        check!(m.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(99));
    }

    #[test]
    fn query_unknown_partition_is_none_not_error() {
        let m = InmemoryRemoteLogMetadataManager::new();
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        check!(m.remote_log_segment_metadata(&other, 0, 0).unwrap() == None);
        check!(m.highest_offset_for_epoch(&other, 0).unwrap() == None);
        check!(m.list_remote_log_segments(&other).unwrap().is_empty());
    }

    #[test]
    fn list_by_epoch_returns_added_segment() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        let listed = m.list_remote_log_segments_by_epoch(&tp(), 0).unwrap();
        assert!(listed.len() == 1);
        assert!(listed[0].remote_log_segment_id().id == Uuid::from_u128(10));
    }

    #[test]
    fn inmemory_read_outcomes_are_some_none_never_not_ready() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();

        // Found.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 42),
            Ok(Some(_))
        ));
        // Caught up, no covering segment → genuine miss.
        assert!(matches!(
            m.remote_log_segment_metadata(&tp(), 0, 10_000),
            Ok(None)
        ));
        // Unknown partition → genuine miss, NOT NotReady.
        let other = TopicIdPartition::new(Uuid::from_u128(999), "nope", 0);
        let got = m.remote_log_segment_metadata(&other, 0, 0);
        assert!(matches!(got, Ok(None)));
        assert!(
            !matches!(got, Err(RemoteStorageError::NotReady { .. })),
            "in-memory manager has no consumer lag; never NotReady"
        );
    }

    #[test]
    fn update_before_add_errors() {
        let m = InmemoryRemoteLogMetadataManager::new();
        let err = m
            .update_remote_log_segment_metadata(finish(10))
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn list_returns_all_states_ordered() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(11, 100, 199))
            .unwrap();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        let listed = m.list_remote_log_segments(&tp()).unwrap();
        let start_offsets: Vec<i64> = listed
            .iter()
            .map(RemoteLogSegmentMetadata::start_offset)
            .collect();
        assert!(start_offsets == [0, 100]);
    }

    #[test]
    fn partition_delete_lifecycle() {
        let m = InmemoryRemoteLogMetadataManager::new();
        for state in [
            RemotePartitionDeleteState::DeletePartitionMarked,
            RemotePartitionDeleteState::DeletePartitionStarted,
            RemotePartitionDeleteState::DeletePartitionFinished,
        ] {
            m.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                topic_id_partition: tp(),
                state,
                event_timestamp_ms: 500,
                broker_id: 1,
            })
            .unwrap();
        }
    }

    #[test]
    fn export_then_import_reproduces_cache() {
        let m = InmemoryRemoteLogMetadataManager::new();
        m.add_remote_log_segment_metadata(started(10, 0, 99))
            .unwrap();
        m.add_remote_log_segment_metadata(started(11, 100, 199))
            .unwrap();
        m.update_remote_log_segment_metadata(finish(10)).unwrap();
        m.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
            topic_id_partition: tp(),
            state: RemotePartitionDeleteState::DeletePartitionMarked,
            event_timestamp_ms: 500,
            broker_id: 1,
        })
        .unwrap();

        let dump = m.export();
        let restored = InmemoryRemoteLogMetadataManager::new();
        restored.import(dump);

        // list_remote_log_segments matches across the partition.
        let before = m.list_remote_log_segments(&tp()).unwrap();
        let after = restored.list_remote_log_segments(&tp()).unwrap();
        check!(before == after);
        // Finished segment still queryable post-import.
        check!(restored.highest_offset_for_epoch(&tp(), 0).unwrap() == Some(99));
        // Re-exporting yields the same dump (idempotent round trip).
        check!(m.export() == restored.export());
    }

    #[test]
    fn partition_delete_rejects_out_of_order() {
        let m = InmemoryRemoteLogMetadataManager::new();
        let err = m
            .put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
                topic_id_partition: tp(),
                state: RemotePartitionDeleteState::DeletePartitionFinished,
                event_timestamp_ms: 500,
                broker_id: 1,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            RemoteStorageError::InvalidPartitionDeleteTransition { .. }
        ));
    }
}
