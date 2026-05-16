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
        // Refill.
        let now = now_nanos();
        let last = self.last_refill_nanos.swap(now, Relaxed);
        let elapsed = now.saturating_sub(last);
        let refill = ((u128::from(elapsed) * u128::from(rate)) / 1_000_000_000) as u64;
        let mut cur = self.available.load(Relaxed);
        let new_avail = (cur.saturating_add(refill)).min(rate);
        self.available.store(new_avail, Relaxed);
        cur = new_avail;
        // Consume.
        let grant = requested.min(cur);
        self.available.fetch_sub(grant, Relaxed);
        grant
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
    use std::time::Duration;

    #[test]
    fn zero_rate_grants_full_request() {
        let b = TokenBucket::new();
        assert_eq!(b.try_consume(1024), 1024);
    }

    #[test]
    fn first_consume_under_rate_succeeds() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        assert_eq!(b.try_consume(512), 512);
    }

    #[test]
    fn consume_drains_bucket() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        assert_eq!(b.try_consume(1024), 1024);
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
        assert!((900..=1024).contains(&g), "expected ~1024 (capped), got {g}");
    }

    #[test]
    fn set_rate_resets_available() {
        let b = TokenBucket::new();
        b.set_rate(1024);
        b.try_consume(1024); // drain
        b.set_rate(2048);
        assert_eq!(b.try_consume(2048), 2048); // fresh capacity
    }
}
