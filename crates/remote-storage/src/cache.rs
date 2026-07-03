//! `RemoteLogMetadataCache` — per-partition metadata state with the
//! lifecycle state machine and an epoch-indexed offset lookup.
//!
//! Mirrors Kafka's `RemoteLogMetadataCache`: segments are kept in an
//! id-keyed map; once a segment reaches
//! [`CopySegmentFinished`](RemoteLogSegmentState::CopySegmentFinished) it is
//! also indexed per leader epoch by the offset at which that epoch starts
//! contributing, so an `(epoch, offset)` query is a navigable-map floor
//! lookup. A segment leaving the readable set
//! ([`DeleteSegmentStarted`](RemoteLogSegmentState::DeleteSegmentStarted))
//! is removed from the epoch index;
//! [`DeleteSegmentFinished`](RemoteLogSegmentState::DeleteSegmentFinished)
//! drops it entirely.

use std::collections::{BTreeMap, HashMap};

use crabka_ids::LeaderEpoch;
use uuid::Uuid;

use crate::{
    error::RemoteStorageError,
    metadata::{
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentState,
        RemotePartitionDeleteState,
    },
};

#[derive(Debug, Default)]
pub(crate) struct RemoteLogMetadataCache {
    /// Every known segment, keyed by its per-segment UUID.
    id_to_metadata: HashMap<Uuid, RemoteLogSegmentMetadata>,
    /// For each leader epoch, a navigable map from the offset that epoch
    /// starts contributing → the finished segment id. Only finished
    /// (readable) segments appear here.
    epoch_to_offset_to_id: HashMap<LeaderEpoch, BTreeMap<i64, Uuid>>,
    /// Partition-delete lifecycle state, once marked.
    delete_state: Option<RemotePartitionDeleteState>,
}

impl RemoteLogMetadataCache {
    pub(crate) fn add(
        &mut self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let id = metadata.remote_log_segment_id().clone();
        if metadata.state() != RemoteLogSegmentState::CopySegmentStarted {
            return Err(RemoteStorageError::InvalidAdd {
                id,
                reason: format!(
                    "starting state must be CopySegmentStarted, got {:?}",
                    metadata.state()
                ),
            });
        }
        if self.id_to_metadata.contains_key(&id.id) {
            return Err(RemoteStorageError::InvalidAdd {
                id,
                reason: "segment id already exists".into(),
            });
        }
        self.id_to_metadata.insert(id.id, metadata);
        Ok(())
    }

    pub(crate) fn update(
        &mut self,
        update: &RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let id = update.remote_log_segment_id.clone();
        let existing = self
            .id_to_metadata
            .get(&id.id)
            .ok_or_else(|| RemoteStorageError::SegmentNotFound(id.clone()))?;

        let updated = existing.with_update(update)?;
        let new_state = updated.state();

        match new_state {
            RemoteLogSegmentState::CopySegmentFinished => self.index_epochs(&updated),
            RemoteLogSegmentState::DeleteSegmentStarted => self.deindex_epochs(&updated),
            RemoteLogSegmentState::DeleteSegmentFinished => {
                self.deindex_epochs(&updated);
                self.id_to_metadata.remove(&id.id);
                return Ok(());
            }
            RemoteLogSegmentState::CopySegmentStarted => {}
        }

        self.id_to_metadata.insert(id.id, updated);
        Ok(())
    }

    fn index_epochs(&mut self, metadata: &RemoteLogSegmentMetadata) {
        let id = metadata.remote_log_segment_id().id;
        for (&epoch, &start) in metadata.segment_leader_epochs() {
            self.epoch_to_offset_to_id
                .entry(epoch)
                .or_default()
                .insert(start, id);
        }
    }

    fn deindex_epochs(&mut self, metadata: &RemoteLogSegmentMetadata) {
        let id = metadata.remote_log_segment_id().id;
        for (&epoch, &start) in metadata.segment_leader_epochs() {
            if let Some(map) = self.epoch_to_offset_to_id.get_mut(&epoch) {
                // Only remove the slot if it still points at this segment.
                if map.get(&start) == Some(&id) {
                    map.remove(&start);
                }
                if map.is_empty() {
                    self.epoch_to_offset_to_id.remove(&epoch);
                }
            }
        }
    }

    pub(crate) fn segment_for(
        &self,
        leader_epoch: LeaderEpoch,
        offset: i64,
    ) -> Option<RemoteLogSegmentMetadata> {
        let map = self.epoch_to_offset_to_id.get(&leader_epoch)?;
        let (_start, id) = map.range(..=offset).next_back()?;
        let md = self.id_to_metadata.get(id)?;
        // The epoch index holds only finished segments, but double-check the
        // offset actually falls within this segment's range.
        if md.state() == RemoteLogSegmentState::CopySegmentFinished && offset <= md.end_offset() {
            Some(md.clone())
        } else {
            None
        }
    }

    pub(crate) fn highest_offset_for_epoch(&self, leader_epoch: LeaderEpoch) -> Option<i64> {
        let map = self.epoch_to_offset_to_id.get(&leader_epoch)?;
        map.values()
            .filter_map(|id| self.id_to_metadata.get(id))
            .map(RemoteLogSegmentMetadata::end_offset)
            .max()
    }

    pub(crate) fn list(&self) -> Vec<RemoteLogSegmentMetadata> {
        let mut out: Vec<RemoteLogSegmentMetadata> =
            self.id_to_metadata.values().cloned().collect();
        sort_by_start_offset(&mut out);
        out
    }

    pub(crate) fn list_by_epoch(&self, leader_epoch: LeaderEpoch) -> Vec<RemoteLogSegmentMetadata> {
        let mut out: Vec<RemoteLogSegmentMetadata> = self
            .id_to_metadata
            .values()
            .filter(|m| m.segment_leader_epochs().contains_key(&leader_epoch))
            .cloned()
            .collect();
        sort_by_start_offset(&mut out);
        out
    }

    /// Every tracked segment (all states), unordered. The owning
    /// manager pairs this with [`Self::delete_state`] to dump the
    /// partition for snapshotting.
    pub(crate) fn dump_segments(&self) -> Vec<RemoteLogSegmentMetadata> {
        self.id_to_metadata.values().cloned().collect()
    }

    /// Seed this (assumed-empty) cache from a dump, bypassing
    /// lifecycle-transition validation. Rebuilds the per-epoch offset
    /// index for every finished segment exactly as the live path does,
    /// so reads after seeding behave identically. `delete_started` /
    /// `delete_finished` segments are kept in `id_to_metadata` but not
    /// indexed (`delete_finished` never reaches a dump, but we tolerate
    /// it for robustness). Partition-delete state is set verbatim.
    pub(crate) fn seed(
        &mut self,
        segments: Vec<RemoteLogSegmentMetadata>,
        delete_state: Option<RemotePartitionDeleteState>,
    ) {
        for md in segments {
            let id = md.remote_log_segment_id().id;
            if md.state() == RemoteLogSegmentState::CopySegmentFinished {
                self.index_epochs(&md);
            }
            // DeleteSegmentFinished segments are dropped entirely in the
            // live path; if one somehow appears in a dump, skip it.
            if md.state() != RemoteLogSegmentState::DeleteSegmentFinished {
                self.id_to_metadata.insert(id, md);
            }
        }
        self.delete_state = delete_state;
    }

    pub(crate) fn delete_state(&self) -> Option<RemotePartitionDeleteState> {
        self.delete_state
    }

    pub(crate) fn set_delete_state(&mut self, state: RemotePartitionDeleteState) {
        self.delete_state = Some(state);
    }
}

fn sort_by_start_offset(segments: &mut [RemoteLogSegmentMetadata]) {
    segments.sort_by(|a, b| {
        a.start_offset().cmp(&b.start_offset()).then_with(|| {
            a.remote_log_segment_id()
                .id
                .cmp(&b.remote_log_segment_id().id)
        })
    });
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::metadata::{CustomMetadata, RemoteLogSegmentId, TopicIdPartition};

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "t", 0)
    }

    fn seg(id: u128, epochs: &[(i32, i64)], start: i64, end: i64) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            end,
            1,
            100,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            epochs
                .iter()
                .map(|&(epoch, start)| (LeaderEpoch(epoch), start))
                .collect(),
        )
        .unwrap()
    }

    fn finish(id: u128) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: 200,
            custom_metadata: Some(CustomMetadata(vec![1])),
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 1,
        }
    }

    fn transition(id: u128, state: RemoteLogSegmentState) -> RemoteLogSegmentMetadataUpdate {
        RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            event_timestamp_ms: 300,
            custom_metadata: None,
            state,
            broker_id: 1,
        }
    }

    #[test]
    fn started_segment_is_invisible_until_finished() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        assert!(
            c.segment_for(LeaderEpoch(0), 50).is_none(),
            "started not yet readable"
        );
        c.update(&finish(10)).unwrap();
        let got = c
            .segment_for(LeaderEpoch(0), 50)
            .expect("finished is readable");
        assert!(got.remote_log_segment_id().id == Uuid::from_u128(10));
    }

    #[test]
    fn offset_lookup_across_segments_one_epoch() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        c.add(seg(11, &[(0, 100)], 100, 199)).unwrap();
        c.update(&finish(10)).unwrap();
        c.update(&finish(11)).unwrap();
        for (offset, want) in [
            (0, Some(Uuid::from_u128(10))),
            (99, Some(Uuid::from_u128(10))),
            (100, Some(Uuid::from_u128(11))),
            (199, Some(Uuid::from_u128(11))),
            // past the end
            (200, None),
        ] {
            check!(
                c.segment_for(LeaderEpoch(0), offset)
                    .map(|s| s.remote_log_segment_id().id)
                    == want,
                "offset={offset}"
            );
        }
    }

    #[test]
    fn list_by_epoch_returns_matching_segments() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        c.update(&finish(10)).unwrap();
        let listed_ids: Vec<Uuid> = c
            .list_by_epoch(LeaderEpoch(0))
            .iter()
            .map(|s| s.remote_log_segment_id().id)
            .collect();
        assert!(listed_ids == [Uuid::from_u128(10)]);
        assert!(
            c.list_by_epoch(LeaderEpoch(7)).is_empty(),
            "unknown epoch -> empty"
        );
    }

    #[test]
    fn deindex_removes_epoch_slot() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        c.update(&finish(10)).unwrap();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(99));
        // DeleteSegmentStarted deindexes the epoch slot (but the metadata is
        // still present until DeleteSegmentFinished). highest_offset_for_epoch
        // reads the epoch index directly, so it must now miss.
        c.update(&transition(10, RemoteLogSegmentState::DeleteSegmentStarted))
            .unwrap();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)).is_none());
    }

    #[test]
    fn offset_lookup_respects_epoch() {
        let mut c = RemoteLogMetadataCache::default();
        // One segment spanning two epochs: epoch 0 owns [0,49], epoch 1 owns [50,99].
        c.add(seg(10, &[(0, 0), (1, 50)], 0, 99)).unwrap();
        c.update(&finish(10)).unwrap();
        // A second segment, epoch 1 only.
        c.add(seg(11, &[(1, 100)], 100, 199)).unwrap();
        c.update(&finish(11)).unwrap();

        for (epoch, offset, want) in [
            // Epoch 0 lookups only see the first segment.
            (LeaderEpoch(0), 10, Some(Uuid::from_u128(10))),
            // Epoch 0 has no segment at 150.
            (LeaderEpoch(0), 150, None),
            // Epoch 1 floor lookup picks the right segment.
            (LeaderEpoch(1), 60, Some(Uuid::from_u128(10))),
            (LeaderEpoch(1), 150, Some(Uuid::from_u128(11))),
        ] {
            check!(
                c.segment_for(epoch, offset)
                    .map(|s| s.remote_log_segment_id().id)
                    == want,
                "epoch={epoch:?} offset={offset}"
            );
        }
    }

    #[test]
    fn highest_offset_for_epoch_is_max_end() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        c.add(seg(11, &[(0, 100)], 100, 199)).unwrap();
        c.update(&finish(10)).unwrap();
        c.update(&finish(11)).unwrap();
        assert!(c.highest_offset_for_epoch(LeaderEpoch(0)) == Some(199));
        assert!(c.highest_offset_for_epoch(LeaderEpoch(7)) == None);
    }

    #[test]
    fn delete_started_hides_segment_delete_finished_drops_it() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        c.update(&finish(10)).unwrap();
        assert!(c.segment_for(LeaderEpoch(0), 50).is_some());

        c.update(&transition(10, RemoteLogSegmentState::DeleteSegmentStarted))
            .unwrap();
        assert!(
            c.segment_for(LeaderEpoch(0), 50).is_none(),
            "delete-started hides it"
        );
        assert!(
            c.list().len() == 1,
            "still tracked while delete in progress"
        );

        c.update(&transition(
            10,
            RemoteLogSegmentState::DeleteSegmentFinished,
        ))
        .unwrap();
        assert!(c.list().is_empty(), "delete-finished drops it entirely");
    }

    #[test]
    fn update_unknown_segment_errors() {
        let mut c = RemoteLogMetadataCache::default();
        let err = c.update(&finish(404)).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn add_with_wrong_state_errors() {
        let mut c = RemoteLogMetadataCache::default();
        let mut s = seg(10, &[(0, 0)], 0, 99);
        s = s
            .with_update(&finish(10))
            .expect("force to finished for the test");
        let err = c.add(s).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
    }

    #[test]
    fn duplicate_add_errors() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        let err = c.add(seg(10, &[(0, 0)], 0, 99)).unwrap_err();
        assert!(matches!(err, RemoteStorageError::InvalidAdd { .. }));
    }

    #[test]
    fn dump_then_seed_rebuilds_epoch_index() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        c.add(seg(11, &[(0, 100)], 100, 199)).unwrap();
        c.update(&finish(10)).unwrap();
        c.update(&finish(11)).unwrap();
        c.update(&transition(11, RemoteLogSegmentState::DeleteSegmentStarted))
            .unwrap();
        c.set_delete_state(RemotePartitionDeleteState::DeletePartitionMarked);

        let segments = c.dump_segments();
        let delete_state = c.delete_state();

        let mut seeded = RemoteLogMetadataCache::default();
        seeded.seed(segments, delete_state);

        // Finished seg 10 is queryable; delete-started seg 11 is hidden
        // but still listed; delete_state survives.
        check!(
            seeded
                .segment_for(LeaderEpoch(0), 50)
                .unwrap()
                .remote_log_segment_id()
                .id
                == Uuid::from_u128(10)
        );
        check!(seeded.segment_for(LeaderEpoch(0), 150).is_none());
        check!(seeded.list().len() == 2);
        check!(seeded.delete_state() == Some(RemotePartitionDeleteState::DeletePartitionMarked));
    }

    #[test]
    fn list_is_ordered_by_start_offset() {
        let mut c = RemoteLogMetadataCache::default();
        c.add(seg(11, &[(0, 100)], 100, 199)).unwrap();
        c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
        let listed = c.list();
        assert!(listed[0].start_offset() == 0);
        assert!(listed[1].start_offset() == 100);
    }
}
