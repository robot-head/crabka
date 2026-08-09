//! Offset-index lookup kernel.
//!
//! This kernel comes out of the `OffsetIndex` of `crabka-log`, so that Creusot
//! can verify it. It uses a hand-rolled binary search, the canonical Creusot
//! loop, and not `binary_search_by_key`. The proof thus does not depend on a
//! model of the std search.

use creusot_std::prelude::*;

/// The byte position to start reading at for `target`.
///
/// This is the position field of the largest entry with
/// `relative_offset <= target`, or 0 if no such entry exists. `entries` must be
/// strictly sorted by relative offset, which the construction of `OffsetIndex`
/// guarantees.
#[requires(forall<i: Int, j: Int> 0 <= i && i < j && j < entries@.len()
    ==> entries@[i].0@ < entries@[j].0@)]
#[ensures((exists<i: Int> 0 <= i && i < entries@.len() && entries@[i].0@ <= target@)
    ==> exists<i: Int> 0 <= i && i < entries@.len()
        && entries@[i].0@ <= target@
        && result@ == entries@[i].1@
        && (forall<j: Int> i < j && j < entries@.len() ==> entries@[j].0@ > target@))]
#[ensures((forall<i: Int> 0 <= i && i < entries@.len() ==> entries@[i].0@ > target@)
    ==> result@ == 0)]
#[must_use]
pub fn offset_index_lookup(entries: &[(u32, u32)], target: u32) -> u32 {
    let mut lo = 0usize; // entries[..lo] all have rel <= target
    let mut hi = entries.len(); // entries[hi..] all have rel > target
    #[invariant(lo@ <= hi@ && hi@ <= entries@.len())]
    #[invariant(forall<i: Int> 0 <= i && i < lo@ ==> entries@[i].0@ <= target@)]
    #[invariant(forall<i: Int> hi@ <= i && i < entries@.len() ==> entries@[i].0@ > target@)]
    #[variant(hi - lo)]
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if entries[mid].0 <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 { 0 } else { entries[lo - 1].1 }
}

#[cfg(test)]
mod tests {

    use proptest::prelude::*;

    use super::*;

    /// The production implementation that this kernel replaced: the index.rs
    /// lookup.
    fn binary_search_oracle(entries: &[(u32, u32)], target: u32) -> u32 {
        match entries.binary_search_by_key(&target, |&(rel, _)| rel) {
            Ok(i) => entries[i].1,
            Err(0) => 0,
            Err(i) => entries[i - 1].1,
        }
    }

    proptest! {
        #[test]
        fn lookup_matches_binary_search_oracle(
            rels in proptest::collection::btree_set(0u32..10_000, 0..64),
            target in 0u32..10_000,
        ) {
            // btree_set gives strictly-sorted unique keys, matching the
            // OffsetIndex construction invariant.
            let entries: Vec<(u32, u32)> = rels
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    (
                        *r,
                        u32::try_from(i).expect("btree set length is bounded to 64") * 17,
                    )
                })
                .collect();
            prop_assert_eq!(offset_index_lookup(&entries, target), binary_search_oracle(&entries, target));
        }
    }

    #[test]
    fn empty_index_returns_zero() {
        assert2::assert!(offset_index_lookup(&[], 42) == 0);
    }
}
