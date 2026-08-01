//! Token-bucket pacer for `FixedRate` scenarios. `saturate` mode never
//! calls this — it just `await`s the producer's natural backpressure.

use crabka_units::prelude::*;
use tokio::time::{Instant, interval_at};

/// The slowest rate the pacer will run at. A scenario asking for less (or for
/// nothing at all) is clamped here rather than sleeping for an unbounded
/// period.
const MIN_RATE: Frequency = per_sec(1);

/// A simple steady-rate pacer: every `await_token().await` returns once
/// the bucket has accumulated one whole token, on a schedule pinned to
/// the pacer's creation time so per-call drift can't accumulate.
pub struct Pacer {
    inner: tokio::time::Interval,
}

impl Pacer {
    /// Paces at `rate`, clamped up to [`MIN_RATE`] when the scenario asks for
    /// less than one message a second.
    #[must_use]
    pub fn new(rate: Frequency) -> Self {
        let period = if rate < MIN_RATE { MIN_RATE } else { rate }.period();
        let period = period.to_std();
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
    use assert2::check;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pacer_spaces_ticks() {
        let mut p = Pacer::new(per_sec(1000)); // 1 ms period
        let start = Instant::now();
        for _ in 0..5 {
            p.await_token().await;
        }
        check!(start.elapsed().as_time() >= millis(5));
    }

    #[tokio::test(start_paused = true)]
    async fn sub_minimum_rates_are_clamped_to_one_per_second() {
        // Constructed inside a runtime — clamps to `MIN_RATE`.
        let mut p = Pacer::new(Frequency::ZERO);
        let start = Instant::now();
        p.await_token().await;
        check!(start.elapsed().as_time() == secs(1));
    }
}
