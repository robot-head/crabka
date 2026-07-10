//! Active log pruning for `__share_group_state` (KIP-932).
//!
//! After a `ShareSnapshot` is folded for a key, the log prefix up to the
//! *smallest* per-key latest-snapshot offset across every key on that state
//! partition becomes redundant: replaying from that offset still reconstructs
//! every key's state (each key's latest snapshot sits at or after it). The
//! coordinator trims the partition log to that "redundant offset" after a
//! snapshot write — never above the minimum, so no key loses its latest
//! snapshot.

use crabka_log::Offset;

/// Smallest latest-snapshot offset across all live keys on a state partition.
///
/// Trimming the log below this is safe: every key retains its latest snapshot
/// record (it sits at or after this offset). Returns `None` when there are no
/// keys (nothing to prune).
#[must_use]
pub fn redundant_offset(per_key_last_snapshot: &[Offset]) -> Option<Offset> {
    per_key_last_snapshot.iter().copied().min()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn redundant_offset_scenarios() {
        for (case, input, expected) in [
            ("empty input", vec![], None),
            ("single key", vec![Offset(42)], Some(Offset(42))),
            (
                "unordered duplicate offsets",
                vec![Offset(100), Offset(30), Offset(75), Offset(30), Offset(200)],
                Some(Offset(30)),
            ),
            (
                "minimum includes zero",
                vec![Offset(0), Offset(5), Offset(9)],
                Some(Offset(0)),
            ),
        ] {
            assert!(redundant_offset(&input) == expected, "case {case}");
        }
    }
}
