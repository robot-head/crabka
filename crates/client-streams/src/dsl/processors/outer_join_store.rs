//! Shared outer-join store codecs + the stream-time tracker (KIP-633 left/outer
//! window-close emission). The shared store is a KV store keyed by
//! `(timestamp, side, key)` (sorts by time) holding the unmatched left-or-right
//! value; the `TimeTracker` is shared across both side processors.

use bytes::{BufMut, Bytes, BytesMut};

/// Composite key `ts:8B BE ‖ side:1 ‖ key_bytes` (sorts by timestamp, then side).
/// `side_left == true` → a buffered left-side (stream-A) record.
pub(crate) fn outer_key(ts: i64, side_left: bool, key_bytes: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(9 + key_bytes.len());
    b.put_i64(ts);
    b.put_u8(u8::from(!side_left)); // 0 = left, 1 = right
    b.extend_from_slice(key_bytes);
    b.freeze()
}

pub(crate) fn outer_key_ts(k: &[u8]) -> i64 {
    i64::from_be_bytes(k[..8].try_into().expect("8 bytes"))
}

pub(crate) fn outer_key_side_left(k: &[u8]) -> bool {
    k[8] == 0
}

pub(crate) fn outer_key_key_bytes(k: &[u8]) -> &[u8] {
    &k[9..]
}

/// Tagged value: `0x00 ‖ left_bytes` or `0x01 ‖ right_bytes`.
pub(crate) fn outer_value_left(raw: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + raw.len());
    b.put_u8(0);
    b.extend_from_slice(raw);
    b.freeze()
}

pub(crate) fn outer_value_right(raw: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + raw.len());
    b.put_u8(1);
    b.extend_from_slice(raw);
    b.freeze()
}

/// `(is_left, raw_value_bytes)`.
pub(crate) fn outer_value_decode(v: &[u8]) -> (bool, &[u8]) {
    (v[0] == 0, &v[1..])
}

/// Shared per-join stream-time tracker (the JVM `sharedTimeTracker`). We track
/// only `stream_time`; the window-close test is per-buffered-entry (`entry_ts +
/// lookback + grace < stream_time`), so the JVM's `minTime` early-exit optimization
/// isn't needed.
#[derive(Debug, Default)]
pub(crate) struct TimeTracker {
    pub stream_time: i64,
}

impl TimeTracker {
    /// Advance stream time to `max(stream_time, ts)`.
    pub fn advance(&mut self, ts: i64) {
        self.stream_time = self.stream_time.max(ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outer_key_roundtrips_and_sorts_by_time() {
        let k1 = outer_key(5, true, b"k");
        let k2 = outer_key(7, false, b"k");
        assert_eq!(outer_key_ts(&k1), 5);
        assert_eq!(outer_key_side_left(&k1), true);
        assert_eq!(outer_key_key_bytes(&k1), b"k");
        assert_eq!(outer_key_side_left(&k2), false);
        assert!(k2 > k1); // sorts by timestamp (8-byte BE prefix)
    }

    #[test]
    fn outer_value_tags_left_and_right() {
        let l = outer_value_left(b"a");
        let r = outer_value_right(b"b");
        assert_eq!(outer_value_decode(&l), (true, &b"a"[..]));
        assert_eq!(outer_value_decode(&r), (false, &b"b"[..]));
    }

    #[test]
    fn time_tracker_advances_monotonically() {
        let mut t = TimeTracker::default();
        t.advance(5);
        t.advance(3);
        t.advance(9);
        assert_eq!(t.stream_time, 9);
    }
}
