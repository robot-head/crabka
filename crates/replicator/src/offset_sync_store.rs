//! In-memory source-to-target offset translation built from MM2 offset-sync
//! records.
//! Translation is "at-or-before": find the latest sync with upstream <= committed,
//! then downstream + (committed - upstream). Never maps past un-replicated data.

use std::collections::BTreeMap;

use crate::{
    ids::{CommittedOffset, DownstreamOffset, PartitionIndex, UpstreamOffset},
    mm2::OffsetSync,
};

/// Accumulates [`OffsetSync`] records and translates committed source offsets to
/// their corresponding target offsets.
#[derive(Default)]
pub struct OffsetSyncStore {
    /// Outer key: (topic, partition). Inner key: upstream offset → downstream offset.
    syncs: BTreeMap<(String, PartitionIndex), BTreeMap<UpstreamOffset, DownstreamOffset>>,
}

impl OffsetSyncStore {
    /// Ingest one offset-sync record. It replaces any prior entry for the same
    /// `(topic, partition, upstream)` triple.
    pub fn ingest(&mut self, s: OffsetSync) {
        self.syncs
            .entry((s.topic, s.partition))
            .or_default()
            .insert(s.upstream, s.downstream);
    }

    /// Translate a committed source offset to its target offset.
    ///
    /// Returns `None` when:
    /// - the `(topic, partition)` pair has no syncs, or
    /// - `committed` is below every known upstream offset (un-replicated data).
    #[must_use]
    pub fn translate(
        &self,
        topic: &str,
        partition: PartitionIndex,
        committed: CommittedOffset,
    ) -> Option<DownstreamOffset> {
        let m = self.syncs.get(&(topic.to_string(), partition))?;
        // The inner map is keyed by `UpstreamOffset`; `committed` lives in the
        // same (source) address space, so bound the range at the matching
        // upstream offset.
        let (&up, &down) = m.range(..=UpstreamOffset::from(committed)).next_back()?;
        // `committed - up` is a source-space delta; adding it to the paired
        // downstream offset yields the target offset. The arithmetic crosses
        // newtype boundaries, so unwrap via `.0` rather than deriving `Add`/`Sub`.
        Some(DownstreamOffset(down.0 + (committed.0 - up.0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mm2::OffsetSync;

    #[test]
    fn translates_at_or_before_nearest_sync() {
        let mut s = OffsetSyncStore::default();
        s.ingest(OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(100),
            downstream: DownstreamOffset(70),
        });
        s.ingest(OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(200),
            downstream: DownstreamOffset(165),
        });
        for (partition, upstream, want) in [
            (0, 250, Some(215)), // 165 + (250-200)
            (0, 150, Some(120)), // 70 + (150-100)
            (0, 50, None),       // below first sync
            (9, 100, None),      // unknown partition
        ] {
            assert2::assert!(
                s.translate(
                    "orders",
                    PartitionIndex(partition),
                    CommittedOffset(upstream)
                ) == want.map(DownstreamOffset)
            );
        }
    }
}
