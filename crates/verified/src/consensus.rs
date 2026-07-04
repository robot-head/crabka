//! KIP-595 consensus decision kernels, extracted from `crabka-kraft-core` so
//! Creusot can verify them (the host crate's `Instant`/async surface is
//! untranslatable). Contracts are added in a follow-up task; the bodies here
//! are already written in the loop style the proofs need (no std sort).

use creusot_std::prelude::*;

/// Members of `{log_end} U s` with value >= `v` (the majority-replication witness).
#[cfg(creusot)]
#[logic]
#[variant(s.len())]
fn count_ge(log_end: Int, s: Seq<i64>, v: Int) -> Int {
    pearlite! {
        (if log_end >= v { 1 } else { 0 }) + count_ge_seq(s, v)
    }
}

#[cfg(creusot)]
#[logic]
#[variant(s.len())]
fn count_ge_seq(s: Seq<i64>, v: Int) -> Int {
    pearlite! {
        if s.len() == 0 {
            0
        } else {
            (if s[0]@ >= v { 1 } else { 0 }) + count_ge_seq(s.subsequence(1, s.len()), v)
        }
    }
}

/// Deterministic per-`(node, epoch)` election-timeout jitter in `[0, base_ms)`,
/// Raft's randomized backoff made reproducible for the deterministic sims.
/// Different nodes (and the same node across re-election epochs) get different
/// spreads, so closely-synchronized voters don't arm their election timers in
/// lockstep and split the vote indefinitely.
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

/// `true` if the candidate's log is at least as up-to-date as ours
/// (KIP-595: higher last epoch wins; on tie, higher/equal offset wins).
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

/// The HWM as the majority-th largest match offset across the leader's own
/// log end and every follower's acknowledged fetch offset, gated on the
/// leader-completeness rule (Raft Fig.8 / KIP-595): the HWM may only advance
/// once the majority offset is strictly past `epoch_start_offset`. Never
/// regresses below `current_hwm`.
///
/// The majority-th largest is computed by its definition - the greatest
/// member m of `{log_end} U follower_offsets` with at least `majority`
/// members >= m - rather than by sorting: voter counts are tiny (<= ~7), and a
/// definition-mirroring loop is what the Creusot proof quantifies over.
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
    let mut i = 0;
    while i <= n {
        let cand = if i == 0 {
            log_end
        } else {
            follower_offsets[i - 1]
        };
        if cand > majority_offset {
            let mut count: usize = 0;
            let mut j = 0;
            while j <= n {
                let x = if j == 0 {
                    log_end
                } else {
                    follower_offsets[j - 1]
                };
                if x >= cand {
                    count += 1;
                }
                j += 1;
            }
            if count >= majority {
                majority_offset = cand;
            }
        }
        i += 1;
    }
    let gated = if majority_offset > epoch_start_offset {
        majority_offset
    } else {
        current_hwm
    };
    gated.max(current_hwm)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use proptest::prelude::*;

    use super::*;

    /// The production implementation this kernel replaced: sort descending,
    /// take the majority-th largest, gate on `epoch_start`, clamp monotonic.
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
        assert!(election_jitter_ms(7, 3, 0) == 0);
    }

    #[test]
    fn up_to_date_is_the_kip595_rule() {
        // higher epoch wins regardless of offset
        check!(log_is_up_to_date(5, 100, 6, 0));
        // same epoch: candidate offset must be >= ours
        check!(log_is_up_to_date(5, 100, 5, 100));
        check!(!log_is_up_to_date(5, 100, 5, 99));
        // lower epoch never wins
        check!(!log_is_up_to_date(5, 0, 4, i64::MAX));
    }

    #[test]
    fn hwm_never_regresses_and_gates_on_epoch_start() {
        // majority offset (2 of {10, 3, 9} with majority=2 -> 9) is <= epoch_start 9: hold.
        check!(recompute_high_watermark(10, &[3, 9], 2, 9, 5) == 5);
        // majority offset 9 > epoch_start 8: advance.
        check!(recompute_high_watermark(10, &[3, 9], 2, 8, 5) == 9);
        // a fallen follower offset can't drag the HWM back down.
        check!(recompute_high_watermark(10, &[1, 1], 2, 0, 7) == 7);
    }
}
