//! Versioned store/changelog VALUE codec. A version's value is
//! `ValueAndTimestamp`: `validFrom:8B-BE ‖ value` — byte-identical to the window
//! store value, so we reuse the window codec. A tombstone version is encoded as
//! `validFrom:8B-BE ‖ <empty>` (zero-length inner), so the timestamp survives a
//! changelog round-trip even for deletes (a bare `None` changelog value would
//! lose it).
//!
//! NOTE: the exact JVM changelog bytes are pinned by the Task 12 changelog
//! golden; this is the Crabka-internal encoding the golden is checked against.
use bytes::Bytes;

pub(crate) use crate::store::window_schema::{unwrap_value, wrap_value};

/// Wrap a versioned record value (`Some` = live value, `None` = tombstone
/// version) into the changelog/store value bytes at `valid_from`.
pub(crate) fn wrap_versioned(valid_from: i64, value: Option<&[u8]>) -> Bytes {
    wrap_value(valid_from, value.unwrap_or(&[]))
}

/// Split changelog/store value bytes into `(valid_from, Option<value_bytes>)`.
/// A zero-length inner value decodes to `None` (tombstone version).
pub(crate) fn unwrap_versioned(wrapped: &[u8]) -> (i64, Option<Bytes>) {
    let (ts, raw) = unwrap_value(wrapped);
    let value = if raw.is_empty() {
        None
    } else {
        Some(Bytes::copy_from_slice(raw))
    };
    (ts, value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_unwrap_live_value() {
        let w = wrap_versioned(7, Some(&99i64.to_be_bytes()));
        assert_eq!(&w[0..8], &7i64.to_be_bytes());
        let (ts, v) = unwrap_versioned(&w);
        assert_eq!(ts, 7);
        assert_eq!(v.as_deref(), Some(&99i64.to_be_bytes()[..]));
    }

    #[test]
    fn wrap_unwrap_tombstone_version() {
        let w = wrap_versioned(11, None);
        assert_eq!(w.len(), 8); // ts only, empty inner
        let (ts, v) = unwrap_versioned(&w);
        assert_eq!(ts, 11);
        assert_eq!(v, None);
    }
}
