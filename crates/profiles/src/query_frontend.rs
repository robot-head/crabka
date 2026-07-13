//! Query-frontend planning helpers for sharded profile queries.

use crabka_pprof::ProfileError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontendConfig {
    pub shard_width_ms: i64,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            shard_width_ms: 15 * 60 * 1000,
        }
    }
}

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn split_inclusive_range(
    start_ms: i64,
    end_ms: i64,
    shard_width_ms: i64,
) -> Result<Vec<(i64, i64)>, ProfileError> {
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

    use super::*;

    #[test]
    fn split_inclusive_range_keeps_adjacent_shards_non_overlapping() {
        let shards = split_inclusive_range(0, 10, 4).unwrap();

        assert!(shards == vec![(0, 3), (4, 7), (8, 10)]);
    }

    #[test]
    fn split_inclusive_range_keeps_small_ranges_single_shard() {
        assert!(split_inclusive_range(5, 7, 10).unwrap() == vec![(5, 7)]);
    }

    #[test]
    fn split_inclusive_range_rejects_invalid_inputs() {
        assert!(split_inclusive_range(10, 0, 4).is_err());
        assert!(split_inclusive_range(0, 10, 0).is_err());
    }
}
