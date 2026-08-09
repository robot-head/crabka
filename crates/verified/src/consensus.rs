//! KIP-595 consensus decision kernels, extracted from `crabka-kraft-core` so
//! Creusot can verify them (the host crate's `Instant`/async surface is
//! untranslatable). The functions carry their Creusot preconditions,
//! postconditions, invariants, variants, and supporting lemmas directly beside
//! the executable bodies.

use creusot_std::prelude::*;

/// Members of `{log_end} U s` with value >= `v`. This is the
/// majority-replication witness.
#[cfg(creusot)]
#[logic]
pub fn hwm_member_at(log_end: Int, s: Seq<i64>, k: Int) -> Int {
    pearlite! {
        if k == 0 { log_end } else { s[k - 1]@ }
    }
}

#[cfg(creusot)]
#[logic]
#[variant(limit)]
pub fn count_ge_prefix(log_end: Int, s: Seq<i64>, v: Int, limit: Int) -> Int {
    pearlite! {
        if limit <= 0 {
            0
        } else {
            count_ge_prefix(log_end, s, v, limit - 1)
                + (if hwm_member_at(log_end, s, limit - 1) >= v { 1 } else { 0 })
        }
    }
}

#[cfg(creusot)]
#[logic]
#[variant(s.len())]
pub fn count_ge(log_end: Int, s: Seq<i64>, v: Int) -> Int {
    pearlite! { count_ge_prefix(log_end, s, v, s.len() + 1) }
}

#[cfg(creusot)]
#[logic]
#[requires(0 <= limit && limit <= s.len() + 1)]
#[requires(low <= high)]
#[ensures(count_ge_prefix(log_end, s, low, limit) >= count_ge_prefix(log_end, s, high, limit))]
#[variant(limit)]
pub fn lemma_count_ge_prefix_monotone(log_end: Int, s: Seq<i64>, low: Int, high: Int, limit: Int) {
    if limit > 0 {
        lemma_count_ge_prefix_monotone(log_end, s, low, high, limit - 1);
    }
}

#[cfg(creusot)]
#[logic]
#[requires(0 <= limit && limit <= s.len() + 1)]
#[ensures(count_ge_prefix(log_end, s, v, limit) >= 0)]
#[variant(limit)]
pub fn lemma_count_ge_prefix_nonnegative(log_end: Int, s: Seq<i64>, v: Int, limit: Int) {
    if limit > 0 {
        lemma_count_ge_prefix_nonnegative(log_end, s, v, limit - 1);
    }
}

#[cfg(creusot)]
#[logic]
#[requires(0 < limit && limit <= s.len() + 1)]
#[requires(count_ge_prefix(log_end, s, v, limit) >= 1)]
#[ensures(0 <= result && result < limit)]
#[ensures(hwm_member_at(log_end, s, result) >= v)]
#[ensures(count_ge_prefix(log_end, s, hwm_member_at(log_end, s, result), limit)
    >= count_ge_prefix(log_end, s, v, limit))]
#[variant(limit)]
pub fn least_hwm_member_ge_index(log_end: Int, s: Seq<i64>, v: Int, limit: Int) -> Int {
    if limit <= 1 {
        0
    } else {
        let last_index = limit - 1;
        let last_member = hwm_member_at(log_end, s, last_index);
        let previous_count = count_ge_prefix(log_end, s, v, last_index);
        if last_member >= v {
            if previous_count >= 1 {
                let previous_index = least_hwm_member_ge_index(log_end, s, v, last_index);
                let previous_member = hwm_member_at(log_end, s, previous_index);
                if last_member <= previous_member {
                    lemma_count_ge_prefix_monotone(
                        log_end,
                        s,
                        last_member,
                        previous_member,
                        last_index,
                    );
                    proof_assert!(
                        count_ge_prefix(log_end, s, last_member, last_index)
                            >= count_ge_prefix(log_end, s, v, last_index)
                    );
                    proof_assert!(
                        count_ge_prefix(log_end, s, last_member, limit)
                            == count_ge_prefix(log_end, s, last_member, last_index) + 1
                    );
                    proof_assert!(
                        count_ge_prefix(log_end, s, v, limit)
                            == count_ge_prefix(log_end, s, v, last_index) + 1
                    );
                    proof_assert!(
                        count_ge_prefix(log_end, s, last_member, limit)
                            >= count_ge_prefix(log_end, s, v, limit)
                    );
                    last_index
                } else {
                    proof_assert!(previous_member <= last_member);
                    proof_assert!(
                        count_ge_prefix(log_end, s, previous_member, limit)
                            == count_ge_prefix(log_end, s, previous_member, last_index) + 1
                    );
                    proof_assert!(
                        count_ge_prefix(log_end, s, v, limit)
                            == count_ge_prefix(log_end, s, v, last_index) + 1
                    );
                    proof_assert!(
                        count_ge_prefix(log_end, s, previous_member, limit)
                            >= count_ge_prefix(log_end, s, v, limit)
                    );
                    previous_index
                }
            } else {
                lemma_count_ge_prefix_nonnegative(log_end, s, v, last_index);
                lemma_count_ge_prefix_nonnegative(log_end, s, last_member, last_index);
                proof_assert!(previous_count <= 0);
                proof_assert!(previous_count == 0);
                proof_assert!(
                    count_ge_prefix(log_end, s, last_member, limit)
                        == count_ge_prefix(log_end, s, last_member, last_index) + 1
                );
                proof_assert!(count_ge_prefix(log_end, s, last_member, limit) >= 1);
                proof_assert!(
                    count_ge_prefix(log_end, s, v, limit)
                        == count_ge_prefix(log_end, s, v, last_index) + 1
                );
                proof_assert!(count_ge_prefix(log_end, s, v, limit) <= 1);
                proof_assert!(count_ge_prefix(log_end, s, last_member, limit) >= 1
                    && count_ge_prefix(log_end, s, v, limit) <= 1
                    ==> count_ge_prefix(log_end, s, last_member, limit)
                        >= count_ge_prefix(log_end, s, v, limit));
                proof_assert!(
                    count_ge_prefix(log_end, s, last_member, limit)
                        >= count_ge_prefix(log_end, s, v, limit)
                );
                last_index
            }
        } else {
            proof_assert!(count_ge_prefix(log_end, s, v, last_index) >= 1);
            least_hwm_member_ge_index(log_end, s, v, last_index)
        }
    }
}

#[cfg(creusot)]
#[requires(1 <= majority@ && majority@ <= s@.len() + 1)]
#[ensures(forall<v: Int> count_ge(log_end@, s@, v) >= majority@
    ==> exists<k: Int> 0 <= k && k < s@.len() + 1
        && hwm_member_at(log_end@, s@, k) >= v
        && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k)) >= majority@)]
pub fn lemma_hwm_threshold_has_member(log_end: i64, s: &[i64], majority: usize) {
    proof_assert!(forall<v: Int> count_ge(log_end@, s@, v) >= majority@ ==>
        exists<k: Int> k == least_hwm_member_ge_index(log_end@, s@, v, s@.len() + 1)
            && 0 <= k && k < s@.len() + 1
            && hwm_member_at(log_end@, s@, k) >= v
            && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k)) >= majority@);
}

#[cfg(creusot)]
#[requires(1 <= majority@ && majority@ <= s@.len() + 1)]
#[requires(forall<k: Int> 0 <= k && k < s@.len() + 1
    && count_ge(log_end@, s@, hwm_member_at(log_end@, s@, k)) >= majority@
    ==> hwm_member_at(log_end@, s@, k) <= best@)]
#[ensures(forall<v: Int> count_ge(log_end@, s@, v) >= majority@ ==> v <= best@)]
pub fn lemma_hwm_member_maximal(log_end: i64, s: &[i64], majority: usize, best: i64) {
    lemma_hwm_threshold_has_member(log_end, s, majority);
}

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`.
///
/// This is Raft's randomized backoff, made reproducible for the deterministic
/// sims. Different nodes get different spreads, and the same node gets a
/// different spread in each re-election epoch. Closely-synchronized voters thus
/// do not arm their election timers in lockstep and do not split the vote
/// indefinitely.
#[ensures(base_ms@ == 0 ==> result@ == 0)]
#[ensures(base_ms@ > 0 ==> result@ < base_ms@)]
#[must_use]
pub fn election_jitter_ms(me: u64, epoch: u32, base_ms: u64) -> u64 {
    if base_ms == 0 {
        return 0;
    }
    // Cheap integer hash of (node id, epoch); avoids any RNG so the sims stay
    // deterministic.
    let mix = me.wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ u64::from(epoch).wrapping_mul(0xD1B5_4A32_D192_ED03);
    mix % base_ms
}

/// `true` if the candidate's log is at least as up-to-date as ours.
///
/// KIP-595: the higher last epoch wins. On a tie, the higher or equal offset
/// wins.
#[ensures(result == (cand_epoch@ > my_epoch@
    || (cand_epoch@ == my_epoch@ && cand_offset@ >= my_end@)))]
#[must_use]
pub const fn log_is_up_to_date(
    my_epoch: u32,
    my_end: i64,
    cand_epoch: u32,
    cand_offset: i64,
) -> bool {
    cand_epoch > my_epoch || (cand_epoch == my_epoch && cand_offset >= my_end)
}

#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[ensures(result == (count_ge(log_end@, follower_offsets@, cand@) >= majority@))]
fn candidate_has_majority(
    log_end: i64,
    follower_offsets: &[i64],
    cand: i64,
    majority: usize,
) -> bool {
    let mut count: usize = 0;
    if log_end >= cand && count < majority {
        count += 1;
    }

    let n = follower_offsets.len();
    let mut j = 0;
    #[invariant(j@ <= n@)]
    #[invariant({
        let seen = count_ge_prefix(log_end@, follower_offsets@, cand@, j@ + 1);
        count@ == if seen < majority@ { seen } else { majority@ }
    })]
    #[invariant(count@ <= majority@)]
    #[variant(n@ - j@)]
    while j < n {
        let x = follower_offsets[j];
        if x >= cand && count < majority {
            count += 1;
        }
        j += 1;
    }

    count >= majority
}

/// The HWM as the majority-th largest match offset across the leader's own log
/// end and every follower's acknowledged fetch offset.
///
/// The leader-completeness rule of Raft Fig.8 and KIP-595 gates this value: the
/// HWM may only advance once the majority offset is strictly past
/// `epoch_start_offset`. The HWM never regresses below `current_hwm`.
///
/// The function computes the majority-th largest by its definition, and not by
/// a sort. That definition is the greatest member m of
/// `{log_end} U follower_offsets` with at least `majority` members >= m. Voter
/// counts are tiny, at most about 7, and the Creusot proof quantifies over a
/// loop that mirrors the definition.
#[requires(1 <= majority@ && majority@ <= follower_offsets@.len() + 1)]
#[requires(current_hwm@ <= log_end@)]
#[requires(forall<k: Int> 0 <= k && k < follower_offsets@.len()
    ==> follower_offsets@[k]@ <= log_end@)]
#[ensures(result@ >= current_hwm@)]
#[ensures(result@ <= log_end@)]
#[ensures(forall<v: Int> v > epoch_start_offset@
    && count_ge(log_end@, follower_offsets@, v) >= majority@
    ==> v <= result@)]
#[ensures(result@ > current_hwm@
    ==> result@ > epoch_start_offset@
        && count_ge(log_end@, follower_offsets@, result@) >= majority@)]
#[must_use]
pub fn recompute_high_watermark(
    log_end: i64,
    follower_offsets: &[i64],
    majority: usize,
    epoch_start_offset: i64,
    current_hwm: i64,
) -> i64 {
    let n = follower_offsets.len();
    let mut majority_offset = i64::MIN;
    if candidate_has_majority(log_end, follower_offsets, log_end, majority) {
        majority_offset = log_end;
    }

    let mut i = 0;
    #[invariant(i@ <= n@)]
    #[invariant(majority_offset@ <= log_end@)]
    #[invariant(majority_offset@ == -9223372036854775807 - 1
        || count_ge(log_end@, follower_offsets@, majority_offset@) >= majority@)]
    #[invariant(forall<k: Int> 0 <= k && k < i@ + 1
        && count_ge(log_end@, follower_offsets@, hwm_member_at(log_end@, follower_offsets@, k)) >= majority@
        ==> hwm_member_at(log_end@, follower_offsets@, k) <= majority_offset@)]
    #[variant(n@ - i@)]
    while i < n {
        let cand = follower_offsets[i];
        if cand > majority_offset
            && candidate_has_majority(log_end, follower_offsets, cand, majority)
        {
            majority_offset = cand;
        }
        i += 1;
    }
    #[cfg(creusot)]
    lemma_hwm_member_maximal(log_end, follower_offsets, majority, majority_offset);
    let gated = if majority_offset > epoch_start_offset {
        majority_offset
    } else {
        current_hwm
    };
    gated.max(current_hwm)
}

/// Monotonic handoff from a previous visible diskless high watermark to a newly
/// observed WAL-quorum frontier. A broker that becomes read-capable must never
/// lower visibility during a leader handoff or a member handoff.
#[ensures(result@ >= previous_hw@)]
#[ensures(result@ >= quorum_frontier@)]
#[ensures(result@ == previous_hw@ || result@ == quorum_frontier@)]
#[must_use]
pub const fn handoff_high_watermark(previous_hw: i64, quorum_frontier: i64) -> i64 {
    if previous_hw >= quorum_frontier {
        previous_hw
    } else {
        quorum_frontier
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use proptest::prelude::*;

    use super::*;

    /// The production implementation that this kernel replaced: sort
    /// descending, take the majority-th largest, gate on `epoch_start`, and
    /// clamp monotonic.
    fn hwm_sort_oracle(
        log_end: i64,
        follower_offsets: &[i64],
        majority: usize,
        epoch_start_offset: i64,
        current_hwm: i64,
    ) -> i64 {
        let mut match_offsets: Vec<i64> = Vec::with_capacity(follower_offsets.len() + 1);
        match_offsets.push(log_end);
        match_offsets.extend_from_slice(follower_offsets);
        match_offsets.sort_unstable_by(|a, b| b.cmp(a));
        let majority_offset = match_offsets[majority - 1];
        let gated = if majority_offset > epoch_start_offset {
            majority_offset
        } else {
            current_hwm
        };
        gated.max(current_hwm)
    }

    proptest! {
        #[test]
        fn hwm_matches_sort_oracle(
            log_end in 0i64..1_000,
            followers in proptest::collection::vec(0i64..1_000, 0..7),
            majority_seed in 0usize..8,
            epoch_start_offset in 0i64..1_000,
            current_hwm in 0i64..1_000,
        ) {
            let majority = 1 + majority_seed % (followers.len() + 1);
            // Kernel precondition domain: clamp like the kraft-core call site does.
            let followers: Vec<i64> = followers.iter().map(|o| (*o).min(log_end)).collect();
            let current_hwm = current_hwm.min(log_end);
            prop_assert_eq!(
                recompute_high_watermark(log_end, &followers, majority, epoch_start_offset, current_hwm),
                hwm_sort_oracle(log_end, &followers, majority, epoch_start_offset, current_hwm)
            );
        }

        #[test]
        fn jitter_in_range(me in any::<u64>(), epoch in any::<u32>(), base in 1u64..10_000) {
            prop_assert!(election_jitter_ms(me, epoch, base) < base);
        }
    }

    #[test]
    fn jitter_zero_base_is_zero() {
        assert2::assert!(election_jitter_ms(7, 3, 0) == 0);
    }

    #[test]
    fn jitter_uses_node_and_epoch_hash_inputs() {
        for (_name, node, epoch, expected) in [
            ("node one epoch zero", 1, 0, 485),
            ("node two epoch zero", 2, 0, 354),
            ("node one epoch one", 1, 1, 446),
        ] {
            assert2::assert!(election_jitter_ms(node, epoch, 1000) == expected);
        }
    }

    #[test]
    fn up_to_date_is_the_kip595_rule() {
        // higher epoch wins regardless of offset
        for (name, ours_epoch, ours_offset, candidate_epoch, candidate_offset, expected) in [
            ("higher epoch", 5, 100, 6, 0, true),
            ("same epoch equal offset", 5, 100, 5, 100, true),
            ("same epoch older offset", 5, 100, 5, 99, false),
            ("lower epoch", 5, 0, 4, i64::MAX, false),
        ] {
            check!(
                log_is_up_to_date(ours_epoch, ours_offset, candidate_epoch, candidate_offset)
                    == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn hwm_never_regresses_and_gates_on_epoch_start() {
        // majority offset (2 of {10, 3, 9} with majority=2 -> 9) is <= epoch_start 9: hold.
        for (name, followers, epoch_start, current, expected) in [
            ("gated at epoch start", &[3, 9][..], 9, 5, 5),
            ("advances past epoch start", &[3, 9][..], 8, 5, 9),
            ("never regresses", &[1, 1][..], 0, 7, 7),
        ] {
            check!(
                recompute_high_watermark(10, followers, 2, epoch_start, current) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn hwm_counts_leader_and_followers_until_majority() {
        for (name, followers, majority, expected) in [
            ("two of three", &[9, 8][..], 2, 9),
            ("all three at leader", &[10, 10][..], 3, 10),
            ("all three below leader", &[4, 4][..], 3, 4),
        ] {
            check!(
                recompute_high_watermark(10, followers, majority, 0, 0) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn handoff_high_watermark_is_monotonic() {
        check!(handoff_high_watermark(7, 5) == 7);
        check!(handoff_high_watermark(7, 9) == 9);
    }
}
