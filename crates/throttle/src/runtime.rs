//! The concurrent [`TokenBucket`] runtime shell around the pure
//! [`plan_consume`] arithmetic: atomics, the seqlock generation protocol, and
//! the injected [`NanoClock`]. Split from `lib.rs` so the pure kernel is the
//! only thing the Creusot verifier ever sees (this module uses atomics and
//! `dyn` trait objects, which Creusot cannot translate).

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering, Ordering::Relaxed},
};

use qubit_clock::{NanoClock, NanoMonotonicClock};

use crate::{AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume};

/// Reads the injected clock's current epoch-nanoseconds as a `u64`.
///
/// Refill arithmetic only uses **differences** of this value, so the absolute
/// anchor is irrelevant; a wall-clock-anchored epoch (~1.75e18 ns today) or a
/// mock timeline anchored at the Unix epoch both fit comfortably in `u64`.
#[inline]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn clock_nanos(clock: &dyn NanoClock) -> u64 {
    clock.nanos() as u64
}

pub struct TokenBucket {
    rate_per_sec: AtomicU64,
    burst: AtomicU64,
    available: AtomicU64,
    last_refill_nanos: AtomicU64,
    /// Seqlock generation guarding the `{rate, burst, available, last_refill}`
    /// group. `set_rate_with_burst` makes it odd while writing and even when
    /// quiescent; a consumer that observes an odd value, or a value that
    /// changed across its read-compute-commit, retries so a straddled reset is
    /// never clobbered by a stale `available` CAS. See the stateright model in
    /// `tests/bucket_model.rs`.
    generation: AtomicU64,
    /// Monotonic nanosecond time source. Injectable so tests can drive refills
    /// deterministically with a [`qubit_clock::MockClock`] instead of sleeping.
    clock: Arc<dyn NanoClock>,
}

impl std::fmt::Debug for TokenBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenBucket")
            .field("rate_per_sec", &self.rate_per_sec.load(Relaxed))
            .field("burst", &self.burst.load(Relaxed))
            .field("available", &self.available.load(Relaxed))
            .field("last_refill_nanos", &self.last_refill_nanos.load(Relaxed))
            .field("generation", &self.generation.load(Relaxed))
            .finish_non_exhaustive()
    }
}

impl TokenBucket {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(NanoMonotonicClock::new()))
    }

    /// Constructs a bucket backed by a caller-supplied [`NanoClock`].
    ///
    /// Production uses [`TokenBucket::new`] (a [`NanoMonotonicClock`]); tests
    /// pass a [`qubit_clock::MockClock`] so refill windows advance by an exact,
    /// controlled amount rather than by wall-clock sleeping.
    #[must_use]
    pub fn with_clock(clock: Arc<dyn NanoClock>) -> Self {
        let last_refill_nanos = AtomicU64::new(clock_nanos(&*clock));
        Self {
            rate_per_sec: AtomicU64::new(0),
            burst: AtomicU64::new(0),
            available: AtomicU64::new(0),
            last_refill_nanos,
            generation: AtomicU64::new(0),
            clock,
        }
    }

    #[inline]
    fn now_nanos(&self) -> u64 {
        clock_nanos(&*self.clock)
    }

    /// Update the rate. Resets `available` to a one-second burst at the new rate.
    pub fn set_rate(&self, new_rate: u64) {
        self.set_rate_with_burst(new_rate, new_rate);
    }

    /// Update the rate and independent burst capacity.
    ///
    /// The `{rate, burst, available, last_refill}` group is published as one
    /// seqlock critical section: `generation` is bumped to an odd value before
    /// the stores and to the next even value after, so a concurrent
    /// `try_consume` that straddles the reset is forced to retry rather than
    /// clobber the freshly reset `available` with a stale CAS.
    pub fn set_rate_with_burst(&self, new_rate: u64, burst: u64) {
        // Enter the write section (generation becomes odd).
        let gen_start = self.generation.fetch_add(1, Relaxed);
        // Release fence so the group stores below cannot be reordered before the
        // odd-generation publish (pairs with the consumer's Acquire fence).
        std::sync::atomic::fence(Ordering::Release);
        self.rate_per_sec.store(new_rate, Relaxed);
        self.burst.store(burst, Relaxed);
        self.available.store(burst, Relaxed);
        self.last_refill_nanos.store(self.now_nanos(), Relaxed);
        std::sync::atomic::fence(Ordering::Release);
        // Leave the write section (generation becomes even again, advanced by 2
        // total so any straddling reader sees a changed generation).
        self.generation.store(gen_start.wrapping_add(2), Relaxed);
    }

    #[must_use]
    pub fn rate(&self) -> u64 {
        self.rate_per_sec.load(Relaxed)
    }

    #[must_use]
    pub fn burst(&self) -> u64 {
        self.burst.load(Relaxed)
    }

    /// Try to consume up to `requested` tokens. Returns the amount actually
    /// granted. Rate-0 grants the full request.
    ///
    /// `rate` and `burst` are re-read inside the CAS loop under a seqlock
    /// generation check so a concurrent [`Self::set_rate_with_burst`] reset that
    /// straddles this call's refill-claim and CAS commit can never be applied
    /// non-atomically: an odd or mismatched generation forces a retry, and on
    /// retry the refill gap is re-claimed against the post-reset `last_refill`.
    #[allow(clippy::cast_possible_truncation)]
    pub fn try_consume(&self, requested: u64) -> u64 {
        if self.rate_per_sec.load(Relaxed) == 0 {
            return requested;
        }

        loop {
            // Read the seqlock generation; an odd value means a reset is in
            // flight, so spin until it is quiescent before sampling the group.
            let gen_before = self.generation.load(Relaxed);
            if gen_before & 1 != 0 {
                continue;
            }
            std::sync::atomic::fence(Ordering::Acquire);

            let rate = self.rate_per_sec.load(Relaxed);
            if rate == 0 {
                return requested;
            }
            let burst = self.burst.load(Relaxed);
            if burst == 0 {
                // Re-validate against a straddling reset before committing 0.
                if self.generation.load(Relaxed) != gen_before {
                    continue;
                }
                return 0;
            }

            let now = self.now_nanos();
            let last = self.last_refill_nanos.swap(now, Relaxed);
            let elapsed = now.saturating_sub(last);
            let refill = ((u128::from(elapsed) * u128::from(rate)) / 1_000_000_000) as u64;

            let cur = self.available.load(Relaxed);
            let (grant, new_avail) = plan_consume(
                AvailableTokens(cur),
                RefillTokens(refill),
                BurstCapacity(burst),
                RequestedTokens(requested),
            );

            // Only commit if no reset straddled the read-compute window; the CAS
            // itself guards against a concurrent consumer mutating `available`.
            if self.generation.load(Relaxed) != gen_before {
                continue;
            }
            if self
                .available
                .compare_exchange_weak(cur, new_avail.0, Relaxed, Relaxed)
                .is_ok()
            {
                return grant.0;
            }
        }
    }
}

impl Default for TokenBucket {
    fn default() -> Self {
        Self::new()
    }
}

/// Broker-wide throttle state. Two buckets: outbound when this broker is leader,
/// inbound when this broker is follower.
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
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering::Relaxed},
            mpsc::RecvTimeoutError,
        },
        time::Duration,
    };

    use assert2::{assert, check};
    use qubit_clock::MockTime;

    use super::*;

    /// Builds a bucket whose refill clock is a mock timeline anchored at the
    /// Unix epoch. Returns the bucket alongside the [`MockTime`] handle so the
    /// test can advance logical time with `mock.advance(..)` instead of sleeping.
    fn mock_bucket() -> (Arc<TokenBucket>, MockTime) {
        let mock = MockTime::unix_epoch();
        let bucket = Arc::new(TokenBucket::with_clock(Arc::new(mock.clock())));
        (bucket, mock)
    }

    const TRY_CONSUME_TIMEOUT: Duration = Duration::from_secs(2);

    fn try_consume_with_timeout(bucket: &Arc<TokenBucket>, requested: u64) -> u64 {
        let bucket = Arc::clone(bucket);
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let granted = bucket.try_consume(requested);
            let _ = tx.send(granted);
        });

        match rx.recv_timeout(TRY_CONSUME_TIMEOUT) {
            Ok(granted) => {
                handle.join().expect("try_consume worker panicked");
                granted
            }
            Err(RecvTimeoutError::Timeout) => {
                drop(handle);
                panic!("try_consume({requested}) did not complete within {TRY_CONSUME_TIMEOUT:?}");
            }
            Err(RecvTimeoutError::Disconnected) => {
                handle.join().expect("try_consume worker panicked");
                panic!("try_consume worker exited without sending a result");
            }
        }
    }

    #[test]
    fn debug_renders_bucket_fields() {
        let b = TokenBucket::new();
        b.set_rate_with_burst(100, 200);
        let s = format!("{b:?}");
        check!(s.contains("TokenBucket"));
        check!(s.contains("rate_per_sec"));
        check!(s.contains("burst"));
        check!(s.contains("last_refill_nanos"));
    }

    #[test]
    fn zero_rate_grants_full_request() {
        let b = TokenBucket::new();
        assert!(b.try_consume(1024) == 1024);
    }

    #[test]
    fn first_consume_under_rate_succeeds() {
        let b = Arc::new(TokenBucket::new());
        b.set_rate(1024);
        assert!(try_consume_with_timeout(&b, 512) == 512);
    }

    #[test]
    fn independent_burst_can_exceed_rate() {
        let b = Arc::new(TokenBucket::new());
        b.set_rate_with_burst(100, 1000);
        check!(b.rate() == 100);
        check!(b.burst() == 1000);
        check!(try_consume_with_timeout(&b, 500) == 500);
    }

    #[test]
    fn consume_drains_bucket() {
        let b = Arc::new(TokenBucket::new());
        b.set_rate(1024);
        assert!(try_consume_with_timeout(&b, 1024) == 1024);
        let g = try_consume_with_timeout(&b, 1024);
        assert!(g < 100, "expected near-zero grant, got {g}");
    }

    #[test]
    fn bucket_refills_at_rate_after_elapsed_time() {
        let (b, mock) = mock_bucket();
        b.set_rate(1024);
        try_consume_with_timeout(&b, 1024);
        // 500ms at 1024 tokens/s refills exactly 512 tokens — deterministic,
        // where a real 500ms sleep only gets "roughly" 512 under scheduler jitter.
        mock.advance(Duration::from_millis(500));
        let g = try_consume_with_timeout(&b, 1024);
        assert!(g == 512, "expected exactly 512 after 500ms, got {g}");
    }

    #[test]
    fn bucket_caps_at_burst_capacity() {
        let (b, mock) = mock_bucket();
        b.set_rate_with_burst(1024, 2048);
        try_consume_with_timeout(&b, 2048);
        // 2.5s at 1024 tokens/s would refill 2560 tokens, but the 2048 burst cap
        // clamps it; advancing logical time makes this exact and instant.
        mock.advance(Duration::from_millis(2500));
        let g = try_consume_with_timeout(&b, 4096);
        assert!(g == 2048, "expected exactly 2048 capped at burst, got {g}");
    }

    #[test]
    fn set_rate_resets_available() {
        let b = Arc::new(TokenBucket::new());
        b.set_rate(1024);
        try_consume_with_timeout(&b, 1024);
        b.set_rate(2048);
        assert!(try_consume_with_timeout(&b, 2048) == 2048);
    }

    #[test]
    fn positive_rate_zero_burst_grants_zero() {
        let b = Arc::new(TokenBucket::new());
        b.set_rate_with_burst(1024, 0);

        assert!(try_consume_with_timeout(&b, 1) == 0);
    }

    #[test]
    fn try_consume_waits_while_generation_is_odd() {
        let b = Arc::new(TokenBucket::new());
        b.set_rate(4);
        b.generation.store(1, Relaxed);

        let (tx, rx) = std::sync::mpsc::channel();
        let worker_bucket = Arc::clone(&b);
        let handle = std::thread::spawn(move || {
            let granted = worker_bucket.try_consume(1);
            let _ = tx.send(granted);
        });

        match rx.recv_timeout(Duration::from_millis(50)) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(granted) => panic!("try_consume granted {granted} while generation was odd"),
            Err(RecvTimeoutError::Disconnected) => {
                handle.join().expect("try_consume worker panicked");
                panic!("try_consume worker exited while generation was odd");
            }
        }

        b.generation.store(2, Relaxed);
        let granted = rx
            .recv_timeout(TRY_CONSUME_TIMEOUT)
            .expect("try_consume should complete after generation becomes even");
        handle.join().expect("try_consume worker panicked");
        assert!(granted == 1);
    }

    // Stress the seqlock: many consumers racing a stream of set_rate resets must
    // never leave `available` above `burst` (the rate-change race the stateright
    // model in tests/bucket_model.rs proves bounded). A straddled reset that was
    // clobbered by a stale CAS would let `available` exceed the new burst here.
    #[test]
    fn concurrent_set_rate_never_over_grants_past_burst() {
        const BURST: u64 = 4096;
        let b = Arc::new(TokenBucket::new());
        b.set_rate_with_burst(1024, BURST);
        let stop = Arc::new(AtomicBool::new(false));

        // Resetter: hammer set_rate_with_burst with the same burst cap.
        let resetter = {
            let b = Arc::clone(&b);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Relaxed) {
                    b.set_rate_with_burst(1024, BURST);
                    std::thread::yield_now();
                }
            })
        };

        // Consumers: drain small amounts and assert the grant never exceeds the
        // burst cap (an over-grant would mean a clobbered reset).
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut consumer_handles = Vec::new();
        for _ in 0..3 {
            let b = Arc::clone(&b);
            let done_tx = done_tx.clone();
            consumer_handles.push(std::thread::spawn(move || {
                for _ in 0..5_000 {
                    let g = b.try_consume(128);
                    if g > BURST {
                        let _ = done_tx.send(Err(g));
                        return;
                    }
                }
                let _ = done_tx.send(Ok(()));
            }));
        }
        drop(done_tx);

        let mut over_grant = None;
        let mut timed_out = false;
        for _ in 0..consumer_handles.len() {
            match done_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(Ok(())) => {}
                Ok(Err(g)) => {
                    over_grant = Some(g);
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    timed_out = true;
                    break;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        stop.store(true, Relaxed);
        resetter.join().unwrap();

        if let Some(g) = over_grant {
            panic!("over-grant past burst: {g}");
        }
        assert!(!timed_out, "consumer did not complete within 5s");
        for h in consumer_handles {
            h.join().unwrap();
        }

        // Invariant after the storm: available is within the burst cap.
        assert!(try_consume_with_timeout(&b, 0) == 0);
    }
}
