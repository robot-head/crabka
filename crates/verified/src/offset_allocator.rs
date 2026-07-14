//! Pure offset-reservation kernel for diskless WAL sequencers.

#[cfg(creusot)]
use creusot_std::prelude::*;

/// Reserve `count` offsets from `next`, returning `(base, next_after)`.
#[must_use]
#[cfg_attr(creusot, requires(count@ >= 0))]
#[cfg_attr(creusot, requires(next@ + count@ <= i64::MAX@))]
#[cfg_attr(creusot, ensures(result.0@ == next@))]
#[cfg_attr(creusot, ensures(result.1@ == next@ + count@))]
pub const fn reserve_offsets(next: i64, count: i64) -> (i64, i64) {
    (next, next.saturating_add(count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_offsets_returns_base_and_advanced_next() {
        assert_eq!(reserve_offsets(11, 3), (11, 14));
    }

    #[test]
    fn reserve_offsets_saturates_on_overflow() {
        assert_eq!(reserve_offsets(i64::MAX - 1, 3), (i64::MAX - 1, i64::MAX));
    }
}
