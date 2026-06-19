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
    use super::*;
    use assert2::assert;

    #[test]
    fn consistent_position_is_valid() {
        // We consumed up to offset 100 at epoch 2; leader says epoch 2 ends at
        // 150 (still open / ahead). No truncation.
        assert!(classify(100, 2, 2, 150) == ValidationOutcome::Valid { leader_epoch: 2 });
    }

    #[test]
    fn leader_end_at_position_is_still_valid() {
        assert!(classify(100, 2, 2, 100) == ValidationOutcome::Valid { leader_epoch: 2 });
    }

    #[test]
    fn leader_end_zero_is_known_and_valid_for_negative_position() {
        assert!(classify(-1, 2, 2, 0) == ValidationOutcome::Valid { leader_epoch: 2 });
    }

    #[test]
    fn leader_end_below_position_is_truncation() {
        // Leader's epoch-2 end offset (80) is below our position (100): the
        // tail we hold was truncated away.
        assert!(classify(100, 2, 2, 80) == ValidationOutcome::Truncated { safe_offset: 80 });
    }

    #[test]
    fn negative_leader_end_alone_truncates_to_zero() {
        assert!(classify(-3, 2, 2, -2) == ValidationOutcome::Truncated { safe_offset: 0 });
    }

    #[test]
    fn older_leader_epoch_is_truncation() {
        // Leader only knows up to epoch 1 for our offset; our epoch 2 data
        // diverged.
        assert!(classify(100, 2, 1, 60) == ValidationOutcome::Truncated { safe_offset: 60 });
    }

    #[test]
    fn older_leader_epoch_alone_is_truncation() {
        assert!(classify(10, 2, 1, 20) == ValidationOutcome::Truncated { safe_offset: 20 });
    }

    #[test]
    fn undefined_leader_offset_truncates_to_zero() {
        assert!(classify(100, 2, -1, -1) == ValidationOutcome::Truncated { safe_offset: 0 });
    }

    #[test]
    fn undefined_leader_offset_truncates_even_when_epoch_matches() {
        assert!(classify(100, 2, 2, -1) == ValidationOutcome::Truncated { safe_offset: 0 });
    }
}
