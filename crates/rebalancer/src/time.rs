//! Wall-clock time helpers shared across rebalancer subsystems.

use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock milliseconds since the Unix epoch.
///
/// Returns `0` if the system clock is before the Unix epoch and saturates to
/// `i64::MAX` if the millisecond count exceeds `i64`.
#[must_use]
pub(crate) fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_tracks_wall_clock_millis() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_millis();
        let got = now_ms();
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_millis();
        assert2::assert!(got >= i64::try_from(before).unwrap());
        assert2::assert!(got <= i64::try_from(after).unwrap_or(i64::MAX));
    }
}
