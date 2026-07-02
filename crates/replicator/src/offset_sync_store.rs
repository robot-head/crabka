//! In-memory source->target offset translation built from MM2 offset-sync records.
//! Translation is "at-or-before": find the latest sync with upstream <= committed,
//! then downstream + (committed - upstream). Never maps past un-replicated data.

use std::collections::BTreeMap;

use crate::mm2::OffsetSync;

/// Accumulates [`OffsetSync`] records and translates committed source offsets to
/// their corresponding target offsets.
#[derive(Default)]
pub struct OffsetSyncStore {
    /// Outer key: (topic, partition). Inner key: upstream offset → downstream offset.
    syncs: BTreeMap<(String, i32), BTreeMap<i64, i64>>,
}

impl OffsetSyncStore {
    /// Ingest one offset-sync record, replacing any prior entry for the same
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
    pub fn translate(&self, topic: &str, partition: i32, committed: i64) -> Option<i64> {
        let m = self.syncs.get(&(topic.to_string(), partition))?;
        let (&up, &down) = m.range(..=committed).next_back()?;
        Some(down + (committed - up))
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::mm2::OffsetSync;

    #[test]
    fn translates_at_or_before_nearest_sync() {
        let mut s = OffsetSyncStore::default();
        s.ingest(OffsetSync {
            topic: "orders".into(),
            partition: 0,
            upstream: 100,
            downstream: 70,
        });
        s.ingest(OffsetSync {
            topic: "orders".into(),
            partition: 0,
            upstream: 200,
            downstream: 165,
        });
        for (partition, upstream, want) in [
            (0, 250, Some(215)), // 165 + (250-200)
            (0, 150, Some(120)), // 70 + (150-100)
            (0, 50, None),       // below first sync
            (9, 100, None),      // unknown partition
        ] {
            check!(
                s.translate("orders", partition, upstream) == want,
                "partition={partition} upstream={upstream}"
            );
        }
    }
}
