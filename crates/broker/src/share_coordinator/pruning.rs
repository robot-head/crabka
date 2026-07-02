//! Active log pruning for `__share_group_state` (KIP-932).
//!
//! After a `ShareSnapshot` is folded for a key, the log prefix up to the
//! *smallest* per-key latest-snapshot offset across every key on that state
//! partition becomes redundant: replaying from that offset still reconstructs
//! every key's state (each key's latest snapshot sits at or after it). The
//! coordinator trims the partition log to that "redundant offset" after a
//! snapshot write — never above the minimum, so no key loses its latest
//! snapshot.

/// Smallest latest-snapshot offset across all live keys on a state partition.
///
/// Trimming the log below this is safe: every key retains its latest snapshot
/// record (it sits at or after this offset). Returns `None` when there are no
/// keys (nothing to prune).
#[must_use]
pub fn redundant_offset(per_key_last_snapshot: &[i64]) -> Option<i64> {
    per_key_last_snapshot.iter().copied().min()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn empty_is_none() {
        assert!(redundant_offset(&[]).is_none());
    }

    #[test]
    fn single_key_is_itself() {
        assert!(redundant_offset(&[42]) == Some(42));
    }

    #[test]
    fn picks_minimum() {
        assert!(redundant_offset(&[100, 30, 75, 30, 200]) == Some(30));
    }

    #[test]
    fn min_includes_zero() {
        assert!(redundant_offset(&[0, 5, 9]) == Some(0));
    }
}
