//! Shared utilities for the transaction subsystem.

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// Transaction handlers use this to stamp `last_update_ms` on `TxnEntry`.
#[inline]
pub(crate) fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}
