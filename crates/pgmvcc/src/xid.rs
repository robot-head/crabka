//! Transaction ids.
//!
//! `Xid` is a plain `u64`, which matches the codebase's rowid and `commit_ts`
//! convention. `INVALID_XID` (0) is the sentinel an `xmax` carries while a
//! version is live. `FROZEN_XID` (1) is the checkpoint/vacuum sentinel for
//! tuples whose creating transaction is known committed before every future
//! snapshot. No allocator assigns either sentinel to a real transaction.
//! Normal xids start at [`FIRST_NORMAL_XID`].

pub type Xid = u64;

/// The "no transaction" sentinel: a live version's `xmax`.
pub const INVALID_XID: Xid = 0;

/// The always-visible tuple creator sentinel written by checkpoint/vacuum.
pub const FROZEN_XID: Xid = 1;

/// The first xid an allocator may hand to a real transaction.
pub const FIRST_NORMAL_XID: Xid = 2;

/// Cross-range (global) transaction ids come from this reserved high half of
/// the u64 space. Every per-range local xid is `< GLOBAL_XID_BASE`. This keeps
/// range 0's global-clog keys disjoint from its own local-clog keys.
pub const GLOBAL_XID_BASE: Xid = 1 << 63;

/// True when `xid` is reserved for MVCC metadata rather than a real transaction.
#[must_use]
pub const fn is_reserved_xid(xid: Xid) -> bool {
    xid < FIRST_NORMAL_XID
}

/// Clamps a persisted next-xid counter so allocation never returns a reserved
/// xid.
#[must_use]
pub const fn first_allocatable_xid_at_or_after(xid: Xid) -> Xid {
    if xid < FIRST_NORMAL_XID {
        return FIRST_NORMAL_XID;
    }
    xid
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_base_is_top_bit_and_above_realistic_local_xids() {
        assert_eq!(GLOBAL_XID_BASE, 1u64 << 63);
        const { assert!(1_000_000u64 < GLOBAL_XID_BASE) };
    }

    #[test]
    fn reserved_xids_are_below_the_first_allocatable_xid() {
        assert!(is_reserved_xid(INVALID_XID));
        assert!(is_reserved_xid(FROZEN_XID));
        assert!(!is_reserved_xid(FIRST_NORMAL_XID));
    }

    #[test]
    fn allocator_seed_never_emits_reserved_xids() {
        for reserved in [INVALID_XID, FROZEN_XID] {
            assert_eq!(
                first_allocatable_xid_at_or_after(reserved),
                FIRST_NORMAL_XID
            );
        }
        assert_eq!(
            first_allocatable_xid_at_or_after(FIRST_NORMAL_XID),
            FIRST_NORMAL_XID
        );
    }
}
