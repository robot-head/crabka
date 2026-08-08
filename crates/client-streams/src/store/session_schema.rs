//! The JVM-exact session-store byte codec, `SessionKeySchema`.
//!
//! The store and changelog KEY is `key_bytes ‖ end:8B BE ‖ start:8B BE`. END
//! comes first, so the store sorts by `(key, end, start)` and the merge fetch
//! scans by session end.
//!
//! The VALUE is the raw serialized aggregate. A session store is not
//! `ValueAndTimestamp`-wrapped, because the session end carries the time.
use bytes::{BufMut, Bytes, BytesMut};

const TS_SIZE: usize = 8;
const SUFFIX_SIZE: usize = TS_SIZE * 2; // end(8) + start(8)

/// `SessionKeySchema.toBinary(key, start, end)` → `key ‖ end:8BE ‖ start:8BE`.
pub(crate) fn session_key(key_bytes: &[u8], start: i64, end: i64) -> Bytes {
    let mut b = BytesMut::with_capacity(key_bytes.len() + SUFFIX_SIZE);
    b.extend_from_slice(key_bytes);
    b.put_i64(end);
    b.put_i64(start);
    b.freeze()
}

/// The session END encoded in a composite key, at `k[len-16 .. len-8]`.
pub(crate) fn session_end_of(k: &[u8]) -> i64 {
    let n = k.len();
    i64::from_be_bytes(k[n - SUFFIX_SIZE..n - TS_SIZE].try_into().expect("8 bytes"))
}

/// The session START encoded in a composite key, at `k[len-8 .. len]`.
pub(crate) fn session_start_of(k: &[u8]) -> i64 {
    let n = k.len();
    i64::from_be_bytes(k[n - TS_SIZE..].try_into().expect("8 bytes"))
}

/// The serialized inner-key bytes of a composite session key.
pub(crate) fn session_key_bytes_of(k: &[u8]) -> &[u8] {
    &k[..k.len() - SUFFIX_SIZE]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_layout_end_first() {
        let k = session_key(b"k", 5, 9); // start=5, end=9
        assert_eq!(k.len(), 17); // "k"(1) ‖ end:8 ‖ start:8
        assert_eq!(&k[1..9], &9i64.to_be_bytes()); // end first
        assert_eq!(&k[9..17], &5i64.to_be_bytes()); // start second
        assert_eq!(session_end_of(&k), 9);
        assert_eq!(session_start_of(&k), 5);
        assert_eq!(session_key_bytes_of(&k), b"k");
    }

    #[test]
    fn sorts_by_end_then_start() {
        // Same key: higher END sorts after (end is the dominant 8-byte field).
        let lo = session_key(b"k", 0, 5);
        let hi = session_key(b"k", 0, 7);
        assert!(hi > lo);
        // Same key + end: higher START sorts after.
        let a = session_key(b"k", 3, 9);
        let b = session_key(b"k", 4, 9);
        assert!(b > a);
    }
}
