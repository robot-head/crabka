//! Active log pruning for `__share_group_state` (KIP-932).
//!
//! The coordinator folds a `ShareSnapshot` for a key. The log prefix up to
//! the *smallest* per-key latest-snapshot offset across every key on that
//! state partition then becomes redundant. A replay from that offset still
//! reconstructs every key's state, because each key's latest snapshot sits at
//! or after it. The coordinator trims the partition log to that "redundant
//! offset" after a snapshot write. It never trims above the minimum, so no
//! key loses its latest snapshot.

use crabka_log::Offset;

/// Smallest latest-snapshot offset across all live keys on a state partition.
///
/// A trim of the log below this offset is safe. Every key keeps its latest
/// snapshot record, because that record sits at or after this offset. Returns
/// `None` when there are no keys, so there is nothing to prune.
#[must_use]
pub fn redundant_offset(per_key_last_snapshot: &[Offset]) -> Option<Offset> {
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
        assert!(redundant_offset(&[Offset(42)]) == Some(Offset(42)));
    }

    #[test]
    fn picks_minimum() {
        assert!(
            redundant_offset(&[Offset(100), Offset(30), Offset(75), Offset(30), Offset(200)])
                == Some(Offset(30))
        );
    }

    #[test]
    fn min_includes_zero() {
        assert!(redundant_offset(&[Offset(0), Offset(5), Offset(9)]) == Some(Offset(0)));
    }
}
