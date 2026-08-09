//! Shared KIP-73 token bucket rate limiter.

use creusot_std::prelude::*;

mod ids;

#[cfg(not(creusot))]
mod runtime;

pub use ids::{
    AvailableTokens, BurstCapacity, GrantedTokens, NewAvailable, RefillTokens, RequestedTokens,
};
#[cfg(not(creusot))]
pub use runtime::{ThrottleState, TokenBucket};

/// `min(available + refill, burst)` in unbounded integers.
///
/// This is equal to the executable
/// `available.saturating_add(refill).min(burst)` when the saturating sum would
/// exceed `burst`, which is the only case that matters.
#[cfg(creusot)]
#[logic]
pub fn capped(available: Int, refill: Int, burst: Int) -> Int {
    if available + refill <= burst {
        available + refill
    } else {
        burst
    }
}

/// Pure token-bucket consume arithmetic.
///
/// The inputs are the current `available`, the `refill` claimed for this call,
/// the `burst` cap, and the `requested` tokens. The function returns
/// `(grant, new_available)`, where `capped = (available + refill).min(burst)`,
/// `grant = requested.min(capped)`, and `new_available = capped - grant`.
///
/// The four inputs are distinct newtypes, so a transposed call site no longer
/// compiles. That is the textbook swap bug for four adjacent `u64`s.
#[ensures(result.0.0@ <= requested.0@)]
#[ensures(result.1.0@ <= burst.0@)]
#[ensures(result.0.0@ + result.1.0@ == capped(available.0@, refill.0@, burst.0@))]
#[ensures(result.0.0@ == if requested.0@ <= capped(available.0@, refill.0@, burst.0@) {
    requested.0@
} else {
    capped(available.0@, refill.0@, burst.0@)
})]
#[must_use]
pub fn plan_consume(
    available: AvailableTokens,
    refill: RefillTokens,
    burst: BurstCapacity,
    requested: RequestedTokens,
) -> (GrantedTokens, NewAvailable) {
    let capped = available.0.saturating_add(refill.0).min(burst.0);
    let grant = requested.0.min(capped);
    (GrantedTokens(grant), NewAvailable(capped - grant))
}

#[cfg(test)]
mod plan_tests {

    use super::{
        AvailableTokens, BurstCapacity, GrantedTokens, NewAvailable, RefillTokens, RequestedTokens,
        plan_consume,
    };

    #[test]
    fn plan_consume_grants_and_caps() {
        for ((available, refill, burst, requested), (grant, new_available)) in [
            ((100, 0, 1000, 50), (50, 50)),
            ((100, 0, 1000, 200), (100, 0)),
            ((900, 500, 1000, 200), (200, 800)),
            ((0, 0, 1000, 100), (0, 0)),
            ((u64::MAX, u64::MAX, 1000, 1000), (1000, 0)),
        ] {
            assert2::assert!(
                plan_consume(
                    AvailableTokens(available),
                    RefillTokens(refill),
                    BurstCapacity(burst),
                    RequestedTokens(requested)
                ) == (GrantedTokens(grant), NewAvailable(new_available))
            );
        }
    }
}

#[cfg(test)]
mod plan_fuzz {
    use proptest::prelude::*;

    use super::{AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume};

    proptest! {
        #[test]
        fn plan_consume_invariants(
            available in 0u64..=u64::MAX,
            refill in 0u64..=u64::MAX,
            burst in 0u64..1_000_000,
            requested in 0u64..=u64::MAX,
        ) {
            let (grant, new) = plan_consume(
                AvailableTokens(available),
                RefillTokens(refill),
                BurstCapacity(burst),
                RequestedTokens(requested),
            );
            let capped = available.saturating_add(refill).min(burst);
            prop_assert!(grant.0 <= requested);
            prop_assert!(grant.0 <= capped);
            prop_assert_eq!(new.0, capped - grant.0);
            prop_assert!(new.0 <= burst, "burst cap");
        }
    }
}
