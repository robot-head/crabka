//! The wall-clock source for wall-clock punctuation.
//!
//! The caller injects it into `StreamThread`, so a test can drive the
//! punctuation deterministically with a `ManualClock`.

pub(crate) trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub(crate) struct SystemClock;
impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
    }
}

#[cfg(test)]
pub(crate) struct ManualClock(pub std::sync::Arc<std::sync::atomic::AtomicI64>);
#[cfg(test)]
impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}
