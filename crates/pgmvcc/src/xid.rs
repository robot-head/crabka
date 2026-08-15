//! Transaction ids.
//!
//! `Xid` is a plain `u64`, which matches the codebase's rowid and `commit_ts`
//! convention. `INVALID_XID` (0) is the sentinel an `xmax` carries while a
//! version is live. `FROZEN_XID` (1) is the checkpoint/vacuum sentinel for
//! tuples whose creating transaction is known committed before every future
//! snapshot. No allocator assigns either sentinel to a real transaction.
//! Normal xids start at [`FIRST_NORMAL_XID`].
//!
//! The reserved range is `PostgreSQL`'s, not one of our own choosing, because
//! these ids reach SQL: `xmin`, `xmax`, `txid_current()` and `pg_xact_status()`
//! all report them. `PostgreSQL` reserves three — `InvalidTransactionId` (0),
//! `BootstrapTransactionId` (1) and `FrozenTransactionId` (2) — and starts
//! normal ids at 3, so this module does the same.

pub type Xid = u64;

/// The "no transaction" sentinel: a live version's `xmax`.
pub const INVALID_XID: Xid = 0;

/// The always-visible tuple creator sentinel written by checkpoint/vacuum.
pub const FROZEN_XID: Xid = 1;

/// The first xid an allocator may hand to a real transaction.
///
/// This is `PostgreSQL`'s `FirstNormalTransactionId`, and it leaves 2 reserved
/// even though nothing here writes 2 as a sentinel. That gap is deliberate.
/// `pg_xact_status(2)` and `txid_status(2)` have to answer "committed",
/// because upstream's `TransactionLogFetch` answers for `FrozenTransactionId`
/// without ever reaching the clog. Handing 2 to a real transaction would force
/// those functions to choose between reporting that transaction's true outcome
/// and reporting what every `PostgreSQL` client expects. Not allocating it
/// dissolves the conflict rather than papering over it.
pub const FIRST_NORMAL_XID: Xid = 3;

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
    use assert2::assert;

    use super::*;

    /// `PostgreSQL`'s `FrozenTransactionId`. Nothing here writes it, and
    /// nothing may allocate it either — see [`FIRST_NORMAL_XID`].
    const PG_FROZEN_XID: Xid = 2;

    #[test]
    fn global_base_is_top_bit_and_above_realistic_local_xids() {
        assert!(GLOBAL_XID_BASE == 1u64 << 63);
        // `core::assert!` rather than `assert2`'s: this one is evaluated by the
        // compiler, and `assert2` builds a runtime failure report.
        const { core::assert!(1_000_000u64 < GLOBAL_XID_BASE) };
    }

    #[test]
    fn the_reserved_range_is_postgresqls_three_ids() {
        for reserved in [INVALID_XID, FROZEN_XID, PG_FROZEN_XID] {
            assert!(is_reserved_xid(reserved), "{reserved} should be reserved");
        }
        assert!(!is_reserved_xid(FIRST_NORMAL_XID));
    }

    #[test]
    fn allocator_seed_never_emits_reserved_xids() {
        for reserved in [INVALID_XID, FROZEN_XID, PG_FROZEN_XID] {
            assert!(
                first_allocatable_xid_at_or_after(reserved) == FIRST_NORMAL_XID,
                "{reserved} should clamp to {FIRST_NORMAL_XID}"
            );
        }
        assert!(first_allocatable_xid_at_or_after(FIRST_NORMAL_XID) == FIRST_NORMAL_XID);
    }
}
