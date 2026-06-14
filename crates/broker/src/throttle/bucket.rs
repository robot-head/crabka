//! KIP-73 token bucket rate limiter.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

static EPOCH: OnceLock<Instant> = OnceLock::new();

#[inline]
#[allow(clippy::cast_possible_truncation)]
fn now_nanos() -> u64 {
    let epoch = *EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_nanos() as u64
}

#[derive(Debug)]
pub struct TokenBucket {
    rate_bytes_per_sec: AtomicU64,
    available: AtomicU64,
    last_refill_nanos: AtomicU64,
}

/// Pure token-bucket consume arithmetic. Given the current `available`, the
/// `refill` claimed for this call, the `rate` cap, and `requested` bytes, return
/// `(grant, new_available)` where `capped = (available + refill).min(rate)`,
/// `grant = requested.min(capped)`, and `new_available = capped - grant` (which
/// is `>= 0` by construction). Used by the real `try_consume` CAS loop and by
/// the stateright model + proptest (see `bucket_model.rs`).
pub(crate) fn plan_consume(available: u64, refill: u64, rate: u64, requested: u64) -> (u64, u64) {
    let capped = available.saturating_add(refill).min(rate);
    let grant = requested.min(capped);
    (grant, capped - grant)
}

impl TokenBucket {
    #[must_use]
    pub fn new() -> Self {
        Self {
            rate_bytes_per_sec: AtomicU64::new(0),
            available: AtomicU64::new(0),
            last_refill_nanos: AtomicU64::new(now_nanos()),
        }
    }

    /// Update the rate. Resets `available` to a one-second burst at
    /// the new rate; sets `last_refill` to now.
    pub fn set_rate(&self, new_rate: u64) {
        self.rate_bytes_per_sec.store(new_rate, Relaxed);
        self.available.store(new_rate, Relaxed);
        self.last_refill_nanos.store(now_nanos(), Relaxed);
    }

    pub fn rate(&self) -> u64 {
        self.rate_bytes_per_sec.load(Relaxed)
    }

    /// Try to consume up to `requested` bytes. Returns the number
    /// actually granted (0..=requested). Rate-0 grants the full
    /// request (fast path for unthrottled).
    #[allow(clippy::cast_possible_truncation)]
    pub fn try_consume(&self, requested: u64) -> u64 {
        let rate = self.rate_bytes_per_sec.load(Relaxed);
        if rate == 0 {
            return requested;
        }
        // Refill. The `last_refill` swap atomically claims this call's elapsed
        // gap (only one concurrent caller gets it), so refill is never
        // double-counted.
        let now = now_nanos();
        let last = self.last_refill_nanos.swap(now, Relaxed);
        let elapsed = now.saturating_sub(last);
        let refill = ((u128::from(elapsed) * u128::from(rate)) / 1_000_000_000) as u64;
        // Refill + consume must commit atomically. A plain load/store/fetch_sub
        // lets two concurrent callers clobber each other's read-modify-write,
        // over-granting past the burst cap and underflowing `available` (which
        // disables the throttle until the next refill cap) — see `bucket_model.rs`.
        // The CAS loop recomputes against the fresh `available` on contention,
        // and also absorbs a concurrent `set_rate` reset (the CAS simply retries).
        loop {
            let cur = self.available.load(Relaxed);
            let (grant, new_avail) = plan_consume(cur, refill, rate, requested);
            if self
                .available
                .compare_exchange_weak(cur, new_avail, Relaxed, Relaxed)
                .is_ok()
            {
                return grant;
            }
        }
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

/// Broker-wide throttle state. Two buckets: outbound when this broker
/// is leader, inbound when this broker is follower.
#[derive(Debug)]
pub struct ThrottleState {
    pub leader_out: Arc<TokenBucket>,
    pub follower_in: Arc<TokenBucket>,
}

impl ThrottleState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            leader_out: Arc::new(TokenBucket::new()),
            follower_in: Arc::new(TokenBucket::new()),
        }
    }
}

impl Default for ThrottleState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use std::time::Duration;

    #[test]
    fn plan_consume_grants_and_caps() {
        assert!(plan_consume(100, 0, 1000, 50) == (50, 50)); // partial
        assert!(plan_consume(100, 0, 1000, 200) == (100, 0)); // drained
        assert!(plan_consume(900, 500, 1000, 200) == (200, 800)); // refill capped at rate
        assert!(plan_consume(0, 0, 1000, 100) == (0, 0)); // empty
        assert!(plan_consume(u64::MAX, u64::MAX, 1000, 1000) == (1000, 0)); // saturating + cap
    }

    #[test]
    fn zero_rate_grants_full_request() {
        let b = TokenBucket::new();
        assert!(b.try_consume(1024) == 1024);
    }

    #[test]
    fn first_consume_under_rate_succeeds() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        assert!(b.try_consume(512) == 512);
    }

    #[test]
    fn consume_drains_bucket() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        assert!(b.try_consume(1024) == 1024);
        // Immediately after, available is ~0 (no time elapsed).
        let g = b.try_consume(1024);
        assert!(g < 100, "expected near-zero grant, got {g}");
    }

    #[test]
    fn bucket_refills_at_rate_after_elapsed_time() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        std::thread::sleep(Duration::from_millis(500));
        // After ~500ms at 1024 bytes/sec, ~512 bytes refilled.
        let g = b.try_consume(1024);
        assert!((400..=700).contains(&g), "expected ~512, got {g}");
    }

    #[test]
    fn bucket_caps_at_one_second_capacity() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        std::thread::sleep(Duration::from_millis(1500));
        // After 1.5s, refill would be 1536, but cap is 1024.
        let g = b.try_consume(2048);
        assert!(
            (900..=1024).contains(&g),
            "expected ~1024 (capped), got {g}"
        );
    }

    #[test]
    fn set_rate_resets_available() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        b.set_rate(2048);
        assert!(b.try_consume(2048) == 2048); // fresh capacity
    }
}

#[cfg(test)]
#[path = "bucket_model.rs"]
mod bucket_model;

#[cfg(test)]
mod plan_fuzz {
    use proptest::prelude::*;

    use super::plan_consume;

    proptest! {
        /// The pure arithmetic: grant within request + cap, new_available never
        /// underflows and never exceeds the rate cap.
        #[test]
        fn plan_consume_invariants(
            available in 0u64..=u64::MAX,
            refill in 0u64..=u64::MAX,
            rate in 0u64..1_000_000,
            requested in 0u64..=u64::MAX,
        ) {
            let (grant, new) = plan_consume(available, refill, rate, requested);
            let capped = available.saturating_add(refill).min(rate);
            prop_assert!(grant <= requested);
            prop_assert!(grant <= capped);
            prop_assert_eq!(new, capped - grant);
            prop_assert!(new <= rate, "burst cap");
            // (new is u64 == capped - grant with grant <= capped, so no underflow.)
        }

        /// Sequential conservation: over a chain of consumes at a fixed rate with
        /// per-step refills, the granted total never exceeds initial + the refill
        /// actually absorbed (each step caps `available` at `rate`).
        #[test]
        fn sequential_conservation(
            rate in 1u64..10_000,
            ops in proptest::collection::vec((0u64..20_000, 0u64..20_000), 0..200usize),
        ) {
            let mut available = rate; // start full
            let mut supplied = available;
            let mut granted: u64 = 0;
            for (refill, requested) in ops {
                let capped = available.saturating_add(refill).min(rate);
                supplied = supplied.saturating_add(capped - available); // tokens actually added this step
                let (g, new) = plan_consume(available, refill, rate, requested);
                granted = granted.saturating_add(g);
                available = new;
                prop_assert!(available <= rate);
            }
            prop_assert!(granted <= supplied, "granted {granted} exceeded supplied {supplied}");
        }
    }
}
