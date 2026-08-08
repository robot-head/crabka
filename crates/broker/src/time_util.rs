//! Shared wall-clock time helpers.
//!
//! This module is the single source of truth for the `SystemTime::now() →
//! UNIX_EPOCH → as_millis() → i64` sequence that the transaction, OAuth, and
//! delegation-token handlers use. The helpers saturate on overflow and on
//! pre-epoch clock skew, and do not panic.

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// The value saturates to `0` if the system clock is set before the epoch. It
/// saturates to `i64::MAX` if the duration overflows `i64`, which is about
/// 292 million years from now and therefore safe in practice.
#[inline]
pub(crate) fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}
