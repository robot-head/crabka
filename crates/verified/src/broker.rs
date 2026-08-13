//! Pure, safety-critical decision kernels used by `crabka-broker`.
//!
//! Keeping these small arithmetic decisions here lets Creusot prove the exact
//! executable bodies used by the asynchronous broker.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Visibility bounds and response watermarks for one Fetch partition.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub struct FetchVisibility {
    pub out_of_range: bool,
    pub empty: bool,
    pub limit_offset: i64,
    pub effective_lso: i64,
    pub read_committed_aborts: bool,
    pub response_hw: i64,
    pub response_lso: i64,
}

/// Compute Kafka's consumer/follower Fetch visibility window.
#[requires(0 <= log_start@ && log_start@ <= hw@ && hw@ <= log_end@)]
#[ensures(result.out_of_range == (fetch_offset@ < log_start@))]
#[ensures(result.empty == (!(fetch_offset@ < log_start@)
    && fetch_offset@ >= if is_follower { log_end@ } else { hw@ }))]
#[ensures(result.effective_lso@ == if read_committed && !is_follower {
    if lso@ < hw@ { lso@ } else { hw@ }
} else { lso@ })]
#[ensures(result.read_committed_aborts == (read_committed && !is_follower))]
#[ensures(result.response_hw@ == if is_follower { log_end@ } else { hw@ })]
#[ensures(result.response_lso@ == if read_committed && !is_follower {
    if lso@ < hw@ { lso@ } else { hw@ }
} else if is_follower { log_end@ } else { hw@ })]
#[ensures(result.limit_offset@ == if is_follower { log_end@ } else if read_committed {
    if lso@ < hw@ { lso@ } else { hw@ }
} else { hw@ })]
#[must_use]
pub fn fetch_visibility(
    is_follower: bool,
    read_committed: bool,
    log_start: i64,
    hw: i64,
    lso: i64,
    log_end: i64,
    fetch_offset: i64,
) -> FetchVisibility {
    let upper_bound = if is_follower { log_end } else { hw };
    let effective_lso = if read_committed && !is_follower {
        lso.min(hw)
    } else {
        lso
    };
    let response_hw = if is_follower { log_end } else { hw };
    let response_lso = if read_committed && !is_follower {
        effective_lso
    } else if is_follower {
        log_end
    } else {
        hw
    };
    let limit_offset = if is_follower {
        log_end
    } else if read_committed {
        effective_lso
    } else {
        hw
    };
    let out_of_range = fetch_offset < log_start;
    FetchVisibility {
        out_of_range,
        empty: !out_of_range && fetch_offset >= upper_bound,
        limit_offset,
        effective_lso,
        read_committed_aborts: read_committed && !is_follower,
        response_hw,
        response_lso,
    }
}

/// Resolve `DeleteRecords`' `-1` sentinel to the current high watermark.
#[ensures(result@ == if requested_offset@ == -1 { high_watermark@ } else { requested_offset@ })]
#[must_use]
pub const fn delete_records_target(requested_offset: i64, high_watermark: i64) -> i64 {
    if requested_offset == -1 {
        high_watermark
    } else {
        requested_offset
    }
}

/// Whether a resolved `DeleteRecords` target is outside the local log.
#[ensures(result == (target@ < 0 || target@ > log_end_offset@))]
#[must_use]
pub const fn delete_records_offset_out_of_range(target: i64, log_end_offset: i64) -> bool {
    target < 0 || target > log_end_offset
}

/// Non-negative KIP-932 backlog above the effective share start offset.
#[cfg(creusot)]
#[logic]
#[cfg_attr(test, mutants::skip)]
pub fn effective_share_backlog_model(hwm: i64, spso: i64, log_start: i64) -> Int {
    pearlite! {
        let base = if spso@ >= 0 && spso@ > log_start@ { spso@ } else { log_start@ };
        let difference = hwm@ - base;
        if difference <= 0 {
            0
        } else if difference > 9223372036854775807 {
            9223372036854775807
        } else {
            difference
        }
    }
}

#[ensures(result@ == effective_share_backlog_model(hwm, spso, log_start))]
#[must_use]
pub fn effective_share_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 {
        spso.max(log_start)
    } else {
        log_start
    };
    hwm.saturating_sub(base).max(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_visibility_covers_consumer_and_follower_bounds() {
        let committed = fetch_visibility(false, true, 2, 8, 6, 10, 3);
        assert_eq!(committed.limit_offset, 6);
        assert_eq!(committed.response_hw, 8);
        assert_eq!(committed.response_lso, 6);
        assert!(committed.read_committed_aborts);

        let lso_above_hw = fetch_visibility(false, true, 2, 8, 9, 10, 3);
        assert_eq!(lso_above_hw.effective_lso, 8);
        assert_eq!(lso_above_hw.limit_offset, 8);

        let follower = fetch_visibility(true, false, 2, 8, 6, 10, 10);
        assert_eq!(follower.limit_offset, 10);
        assert_eq!(follower.response_hw, 10);
        assert!(follower.empty);
    }

    #[test]
    fn broker_arithmetic_edges_are_explicit() {
        assert_eq!(delete_records_target(-1, 7), 7);
        assert!(delete_records_offset_out_of_range(-1, 7));
        assert_eq!(effective_share_backlog(12, -1, 4), 8);
        assert_eq!(effective_share_backlog(5, 9, 4), 0);
        assert_eq!(
            effective_share_backlog(i64::MAX, i64::MIN, i64::MIN),
            i64::MAX
        );
    }

    #[test]
    fn fetch_visibility_matches_the_complete_decision_table() {
        for is_follower in [false, true] {
            for read_committed in [false, true] {
                for log_start in [0, 2] {
                    for hw in [2, 5] {
                        for lso in [1, 4, 7] {
                            for log_end in [5, 9] {
                                for fetch_offset in [0, 2, 4, 5, 10] {
                                    let got = fetch_visibility(
                                        is_follower,
                                        read_committed,
                                        log_start,
                                        hw,
                                        lso,
                                        log_end,
                                        fetch_offset,
                                    );
                                    let upper = if is_follower { log_end } else { hw };
                                    let effective_lso = if read_committed && !is_follower {
                                        lso.min(hw)
                                    } else {
                                        lso
                                    };
                                    let response_lso = if is_follower {
                                        log_end
                                    } else if read_committed {
                                        lso.min(hw)
                                    } else {
                                        hw
                                    };
                                    let limit = if is_follower {
                                        log_end
                                    } else if read_committed {
                                        effective_lso
                                    } else {
                                        hw
                                    };
                                    let out_of_range = fetch_offset < log_start;

                                    assert_eq!(got.out_of_range, out_of_range);
                                    assert_eq!(got.empty, !out_of_range && fetch_offset >= upper);
                                    assert_eq!(got.limit_offset, limit);
                                    assert_eq!(got.effective_lso, effective_lso);
                                    assert_eq!(
                                        got.read_committed_aborts,
                                        read_committed && !is_follower
                                    );
                                    assert_eq!(
                                        got.response_hw,
                                        if is_follower { log_end } else { hw }
                                    );
                                    assert_eq!(got.response_lso, response_lso);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn broker_arithmetic_matches_wide_integer_oracles() {
        let values = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        for requested in values {
            for high_watermark in values {
                assert_eq!(
                    delete_records_target(requested, high_watermark),
                    if requested == -1 {
                        high_watermark
                    } else {
                        requested
                    }
                );
                assert_eq!(
                    delete_records_offset_out_of_range(requested, high_watermark),
                    requested < 0 || requested > high_watermark
                );
            }
        }

        for hwm in values {
            for spso in values {
                for log_start in values {
                    let base = if spso >= 0 {
                        spso.max(log_start)
                    } else {
                        log_start
                    };
                    let expected = i64::try_from(
                        (i128::from(hwm) - i128::from(base)).clamp(0, i128::from(i64::MAX)),
                    )
                    .expect("oracle is clamped to the i64 range");
                    assert_eq!(effective_share_backlog(hwm, spso, log_start), expected);
                }
            }
        }
    }
}
