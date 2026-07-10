//! Token-bucket pacer for `FixedRate` scenarios. `saturate` mode never
//! calls this — it just `await`s the producer's natural backpressure.

use std::time::Duration;

use tokio::time::{Instant, interval_at};

/// A simple steady-rate pacer: every `await_token().await` returns once
/// the bucket has accumulated one whole token, on a schedule pinned to
/// the pacer's creation time so per-call drift can't accumulate.
pub struct Pacer {
    inner: tokio::time::Interval,
}

impl Pacer {
    /// `msgs_per_sec` must be > 0; clamps to 1 if not.
    #[must_use]
    pub fn new(msgs_per_sec: u64) -> Self {
        let rate = msgs_per_sec.max(1);
        let period = Duration::from_nanos(1_000_000_000u64 / rate);
        let mut inner = interval_at(Instant::now() + period, period);
        // If a slow consumer falls behind, deliver one tick per call
        // rather than burst-catching-up to "now" — keeps the per-message
        // latency comparable across runs.
        inner.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self { inner }
    }

    /// Sleep until the next token is available.
    pub async fn await_token(&mut self) {
        self.inner.tick().await;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pacer_spaces_ticks() {
        let mut p = Pacer::new(1000); // 1 ms period
        let start = Instant::now();
        for _ in 0..5 {
            p.await_token().await;
        }
        let elapsed = start.elapsed();
        assert2::assert!(elapsed >= Duration::from_millis(5));
    }

    #[tokio::test]
    async fn zero_rate_clamped_does_not_panic() {
        // Constructed inside a runtime — clamps to 1 msg/s.
        let _ = Pacer::new(0);
    }
}
