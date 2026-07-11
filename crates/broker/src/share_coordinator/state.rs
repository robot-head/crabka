//! In-memory per-`(group, topicId, partition)` share-delivery state.
//!
//! Reconstructed by folding a `ShareSnapshot` then any subsequent
//! `ShareUpdate` deltas over it (`apply_snapshot` / `apply_update`). The
//! merge logic mirrors KIP-932's `WriteShareGroupState` semantics: advance the
//! share-partition start offset (SPSO), drop in-memory batches fully below
//! it, and upsert written batches keyed by their first offset.

use crabka_log::Offset;

use crate::share_coordinator::persistence::{ShareSnapshotValue, ShareUpdateValue, StateBatch};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharePartitionState {
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<StateBatch>,
    pub snapshot_epoch: i64,
    pub last_snapshot_offset: Offset,
    pub updates_since_snapshot: u32,
}

impl SharePartitionState {
    pub fn apply_snapshot(&mut self, v: &ShareSnapshotValue) {
        self.snapshot_epoch = v.snapshot_epoch;
        self.state_epoch = v.state_epoch;
        self.leader_epoch = v.leader_epoch;
        self.start_offset = v.start_offset;
        self.delivery_complete_count = v.delivery_complete_count;
        self.state_batches.clone_from(&v.state_batches);
        self.updates_since_snapshot = 0;
    }

    pub fn apply_update(&mut self, v: &ShareUpdateValue) {
        self.leader_epoch = v.leader_epoch;
        self.merge_batches(v.start_offset, &v.state_batches);
        self.delivery_complete_count = v.delivery_complete_count;
        self.updates_since_snapshot += 1;
    }

    /// Advance the SPSO, drop batches fully below it, and upsert each
    /// written batch by its `first_offset` (sorted ascending afterwards).
    pub fn merge_batches(&mut self, new_start: Offset, written: &[StateBatch]) {
        if new_start > self.start_offset {
            self.start_offset = new_start;
        }
        self.state_batches
            .retain(|b| b.last_offset >= self.start_offset);
        for w in written {
            if w.last_offset < self.start_offset {
                continue;
            }
            self.state_batches
                .retain(|b| b.first_offset != w.first_offset);
            self.state_batches.push(w.clone());
        }
        self.state_batches.sort_by_key(|b| b.first_offset);
    }

    #[must_use]
    pub fn to_snapshot(&self) -> ShareSnapshotValue {
        ShareSnapshotValue {
            snapshot_epoch: self.snapshot_epoch + 1,
            state_epoch: self.state_epoch,
            leader_epoch: self.leader_epoch,
            start_offset: self.start_offset,
            delivery_complete_count: self.delivery_complete_count,
            state_batches: self.state_batches.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn batch(first: i64, last: i64) -> StateBatch {
        StateBatch {
            first_offset: Offset(first),
            last_offset: Offset(last),
            delivery_state: 0,
            delivery_count: 1,
        }
    }

    #[test]
    fn apply_snapshot_then_update_advances_spso_and_drops_sub_spso_batches() {
        let mut s = SharePartitionState::default();
        s.apply_snapshot(&ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 2,
            leader_epoch: 3,
            start_offset: Offset(0),
            delivery_complete_count: 0,
            state_batches: vec![batch(0, 9), batch(10, 19), batch(20, 29)],
        });
        assert!(s.state_epoch == 2);
        assert!(s.start_offset == 0);

        // Advance SPSO past the first two batches and write a new one.
        s.apply_update(&ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: 4,
            start_offset: Offset(20),
            delivery_complete_count: 7,
            state_batches: vec![batch(30, 39)],
        });

        // batch(0,9) and batch(10,19) dropped (last_offset < 20); batch(20,29)
        // retained; batch(30,39) added.
        let expected = SharePartitionState {
            state_epoch: 2,
            leader_epoch: 4,
            start_offset: Offset(20),
            delivery_complete_count: 7,
            state_batches: vec![batch(20, 29), batch(30, 39)],
            snapshot_epoch: 1,
            last_snapshot_offset: Offset(0),
            updates_since_snapshot: 1,
        };
        assert!(s == expected);
    }

    #[test]
    fn merge_upserts_batch_by_first_offset() {
        let mut s = SharePartitionState {
            state_batches: vec![batch(0, 9), batch(10, 19)],
            ..Default::default()
        };
        // Overwrite batch starting at 10 with a longer one and add a new one.
        s.merge_batches(
            Offset(0),
            &[
                StateBatch {
                    first_offset: Offset(10),
                    last_offset: Offset(25),
                    delivery_state: 2,
                    delivery_count: 5,
                },
                batch(30, 39),
            ],
        );
        assert!(s.state_batches.len() == 3);
        let updated = s
            .state_batches
            .iter()
            .find(|b| b.first_offset == 10)
            .unwrap();
        assert!(updated.last_offset == 25);
        assert!(updated.delivery_count == 5);
        // Sorted ascending by first_offset.
        let firsts: Vec<Offset> = s.state_batches.iter().map(|b| b.first_offset).collect();
        assert!(firsts == vec![Offset(0), Offset(10), Offset(30)]);
    }

    #[test]
    fn merge_drops_written_batch_below_spso() {
        let mut s = SharePartitionState {
            start_offset: Offset(50),
            ..Default::default()
        };
        // A written batch entirely below the SPSO is ignored.
        s.merge_batches(Offset(50), &[batch(0, 9)]);
        assert!(s.state_batches.is_empty());
    }

    #[test]
    fn to_snapshot_bumps_snapshot_epoch() {
        let s = SharePartitionState {
            snapshot_epoch: 4,
            state_epoch: 1,
            start_offset: Offset(10),
            state_batches: vec![batch(10, 19)],
            ..Default::default()
        };
        let snap = s.to_snapshot();
        let expected = ShareSnapshotValue {
            snapshot_epoch: 5,
            state_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(10),
            delivery_complete_count: 0,
            state_batches: vec![batch(10, 19)],
        };
        assert!(snap == expected);
    }
}
