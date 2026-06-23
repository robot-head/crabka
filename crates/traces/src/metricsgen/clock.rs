//! Injectable clock for deterministic metrics-generator tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock source in epoch nanoseconds.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i64;
}

/// Production clock.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
    }
}

/// Deterministic test clock.
#[derive(Debug, Clone)]
pub struct MockClock {
    now: Arc<AtomicI64>,
}

impl MockClock {
    #[must_use]
    pub fn new(start_ns: i64) -> Self {
        Self {
            now: Arc::new(AtomicI64::new(start_ns)),
        }
    }

    pub fn advance(&self, ns: i64) {
        self.now.fetch_add(ns, Ordering::SeqCst);
    }

    pub fn set(&self, ns: i64) {
        self.now.store(ns, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ns(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn mock_clock_advances() {
        let c = MockClock::new(1_000);
        assert!(c.now_ns() == 1_000);
        c.advance(500);
        assert!(c.now_ns() == 1_500);
        c.set(42);
        assert!(c.now_ns() == 42);
    }
}
