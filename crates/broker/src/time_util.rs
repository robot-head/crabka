//! Shared wall-clock time helpers.
//!
//! Single source of truth for the `SystemTime::now() → UNIX_EPOCH →
//! as_millis() → i64` dance used by transaction, OAuth, and
//! delegation-token handlers. Saturates on overflow or pre-epoch clock
//! skew rather than panicking.

/// Returns the current wall-clock time in milliseconds since the Unix
/// epoch. Saturates to `0` if the system clock is set before the epoch
/// and to `i64::MAX` if the duration overflows `i64` (~292 million
/// years from now — safe in practice).
#[inline]
pub(crate) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
