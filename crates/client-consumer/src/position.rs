//! Per-partition KIP-320 position metadata (sidecar to `next_offsets`) and
//! the pure truncation-decision used by the proactive validate pass and the
//! in-band `diverging_epoch` path.

/// Epoch metadata for one assigned partition. The fetch *offset* itself lives
/// in `Consumer::next_offsets`; this carries the leader-epoch state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionPosition {
    /// Leader epoch of the last consumed record (the `last_fetched_epoch` we
    /// send). `-1` until a record is consumed or a committed epoch is seeded.
    pub offset_epoch: i32,
    /// Current leader node id from the latest metadata. `-1` if unknown.
    pub leader_id: i32,
    /// Current leader epoch from the latest metadata (the `current_leader_epoch`
    /// we send). `-1` if unknown.
    pub leader_epoch: i32,
    /// `true` while this partition must be validated via `OffsetForLeaderEpoch`
    /// before it may be fetched again (set when the metadata leader epoch
    /// advances past `offset_epoch`).
    pub awaiting_validation: bool,
}

impl Default for PartitionPosition {
    fn default() -> Self {
        Self {
            offset_epoch: -1,
            leader_id: -1,
            leader_epoch: -1,
            awaiting_validation: false,
        }
    }
}

/// Outcome of validating a fetch position against the leader's epoch history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValidationOutcome {
    /// Position is consistent with the leader; resume fetching. Carries the
    /// leader's epoch for that offset (to refresh `offset_epoch`).
    Valid { leader_epoch: i32 },
    /// Truncation detected; the fetcher must reset to `safe_offset`.
    Truncated { safe_offset: i64 },
}

/// Decide whether a position has diverged, given the fetch `offset`, the epoch
/// we last consumed (`offset_epoch`), and the leader's answer for that epoch
/// (`leader_end_offset`, `leader_epoch`). This is Kafka's consumer-side rule:
/// truncation iff the leader's epoch for our data is older than ours, or its
/// end offset for that epoch is below our position.
pub(crate) fn classify(
    offset: i64,
    offset_epoch: i32,
    leader_epoch: i32,
    leader_end_offset: i64,
) -> ValidationOutcome {
    if leader_end_offset < 0 || leader_epoch < offset_epoch || leader_end_offset < offset {
        ValidationOutcome::Truncated {
            safe_offset: if leader_end_offset < 0 {
                0
            } else {
                leader_end_offset
            },
        }
    } else {
        ValidationOutcome::Valid { leader_epoch }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn classify_decides_validity_or_truncation() {
        use ValidationOutcome::{Truncated, Valid};
        // (case, offset, offset_epoch, leader_epoch, leader_end_offset, expected)
        let cases = [
            // We consumed up to offset 100 at epoch 2; leader says epoch 2 ends
            // at 150 (still open / ahead). No truncation.
            (
                "consistent position is valid",
                100,
                2,
                2,
                150,
                Valid { leader_epoch: 2 },
            ),
            (
                "leader end at position is still valid",
                100,
                2,
                2,
                100,
                Valid { leader_epoch: 2 },
            ),
            (
                "leader end zero is known and valid for negative position",
                -1,
                2,
                2,
                0,
                Valid { leader_epoch: 2 },
            ),
            // Leader's epoch-2 end offset (80) is below our position (100): the
            // tail we hold was truncated away.
            (
                "leader end below position is truncation",
                100,
                2,
                2,
                80,
                Truncated { safe_offset: 80 },
            ),
            (
                "negative leader end alone truncates to zero",
                -3,
                2,
                2,
                -2,
                Truncated { safe_offset: 0 },
            ),
            // Leader only knows up to epoch 1 for our offset; our epoch 2 data
            // diverged.
            (
                "older leader epoch is truncation",
                100,
                2,
                1,
                60,
                Truncated { safe_offset: 60 },
            ),
            (
                "older leader epoch alone is truncation",
                10,
                2,
                1,
                20,
                Truncated { safe_offset: 20 },
            ),
            (
                "undefined leader offset truncates to zero",
                100,
                2,
                -1,
                -1,
                Truncated { safe_offset: 0 },
            ),
            (
                "undefined leader offset truncates even when epoch matches",
                100,
                2,
                2,
                -1,
                Truncated { safe_offset: 0 },
            ),
        ];
        for (case, offset, offset_epoch, leader_epoch, leader_end_offset, expected) in cases {
            assert!(
                classify(offset, offset_epoch, leader_epoch, leader_end_offset) == expected,
                "case: {case}"
            );
        }
    }
}
