//! Query-frontend planning helpers for sharded profile queries.

use crabka_pprof::ProfileError;
use crabka_units::{Time, convert::TimeExt, minutes};

/// Not `Eq`: [`Time`] stores `f64`. Nothing keys a map on this config.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrontendConfig {
    /// Width of each shard the query range is split into.
    pub shard_width: Time,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            shard_width: minutes(15),
        }
    }
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn split_inclusive_range(
    start_ms: i64,
    end_ms: i64,
    shard_width: Time,
) -> Result<Vec<(i64, i64)>, ProfileError> {
    // The bounds are epoch-millisecond instants and the shard width is an
    // extent, so the width converts once here and the walk stays exact integer
    // arithmetic on instants.
    let shard_width_ms = shard_width.millis_i64();
    if shard_width_ms <= 0 {
        return Err(ProfileError::Plan(format!(
            "query frontend shard width must be positive, got {shard_width_ms}"
        )));
    }
    if start_ms > end_ms {
        return Err(ProfileError::Plan(format!(
            "invalid query range: start {start_ms} is after end {end_ms}"
        )));
    }

    let mut shards = Vec::new();
    let mut current = start_ms;
    while current <= end_ms {
        let shard_end = current.saturating_add(shard_width_ms - 1).min(end_ms);
        shards.push((current, shard_end));
        let Some(next) = shard_end.checked_add(1) else {
            break;
        };
        current = next;
    }
    Ok(shards)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_units::millis;

    use super::*;

    #[test]
    fn split_inclusive_range_keeps_adjacent_shards_non_overlapping() {
        let shards = split_inclusive_range(0, 10, millis(4)).unwrap();

        assert!(shards == vec![(0, 3), (4, 7), (8, 10)]);
    }

    #[test]
    fn split_inclusive_range_keeps_small_ranges_single_shard() {
        assert!(split_inclusive_range(5, 7, millis(10)).unwrap() == vec![(5, 7)]);
    }

    #[test]
    fn split_inclusive_range_rejects_invalid_inputs() {
        assert!(split_inclusive_range(10, 0, millis(4)).is_err());
        assert!(split_inclusive_range(0, 10, <Time as TimeExt>::ZERO).is_err());
    }
}
