//! Pure, safety-critical decision kernels used by `crabka-broker`.
//!
//! Keeping these small arithmetic decisions here lets Creusot prove the exact
//! executable bodies used by the asynchronous broker.

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
#[requires(lso@ <= hw@)]
#[requires(read_committed ==> !is_follower)]
#[ensures(result.out_of_range == (fetch_offset@ < log_start@))]
#[ensures(result.response_hw@ == if is_follower { log_end@ } else { hw@ })]
#[ensures(result.response_lso@ == if is_follower { log_end@ } else if read_committed {
    if lso@ < hw@ { lso@ } else { hw@ }
} else { hw@ })]
#[ensures(is_follower ==> result.limit_offset@ == log_end@)]
#[ensures(!is_follower ==> result.limit_offset@ <= hw@)]
#[ensures(read_committed ==> result.limit_offset@ <= lso@ && result.read_committed_aborts)]
#[must_use]
pub const fn fetch_visibility(
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
        if lso < hw { lso } else { hw }
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
#[ensures(result@ >= 0)]
#[ensures(result@ <= if hwm@ >= log_start@ { hwm@ - log_start@ } else { 0 })]
#[must_use]
pub const fn effective_share_backlog(hwm: i64, spso: i64, log_start: i64) -> i64 {
    let base = if spso >= 0 && spso > log_start {
        spso
    } else {
        log_start
    };
    let backlog = hwm.saturating_sub(base);
    if backlog > 0 { backlog } else { 0 }
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
}
