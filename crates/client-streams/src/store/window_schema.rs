//! JVM-exact windowed store/changelog byte codecs.
//! Store/changelog KEY  : `key_bytes ‖ windowStart:8B BE ‖ seqnum:4B BE` (seqnum 0 for aggregations).
//! Store/changelog VALUE: `recordTs:8B BE ‖ serialized aggregate` (`ValueAndTimestamp`); None = tombstone.
use bytes::{BufMut, Bytes, BytesMut};

const TS_SIZE: usize = 8;
const SEQ_SIZE: usize = 4;
pub(crate) const SUFFIX_SIZE: usize = TS_SIZE + SEQ_SIZE; // 12

/// `WindowKeySchema.toStoreKeyBinary(key, windowStart, seqnum)`. The seqnum is the
/// per-record value for retainDuplicates join stores, or 0 for aggregations.
pub(crate) fn store_key(key_bytes: &[u8], window_start: i64, seqnum: u32) -> Bytes {
    let mut b = BytesMut::with_capacity(key_bytes.len() + SUFFIX_SIZE);
    b.extend_from_slice(key_bytes);
    b.put_i64(window_start);
    b.put_u32(seqnum);
    b.freeze()
}

/// The windowStart encoded in a composite store key.
pub(crate) fn window_start_of(store_key: &[u8]) -> i64 {
    let n = store_key.len();
    i64::from_be_bytes(
        store_key[n - SUFFIX_SIZE..n - SEQ_SIZE]
            .try_into()
            .expect("8 bytes"),
    )
}

/// The serialized inner-key bytes of a composite store key.
pub(crate) fn key_bytes_of(store_key: &[u8]) -> &[u8] {
    &store_key[..store_key.len() - SUFFIX_SIZE]
}

/// `ValueAndTimestampSerializer`: `recordTs:8B BE ‖ raw`.
pub(crate) fn wrap_value(record_ts: i64, raw: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(TS_SIZE + raw.len());
    b.put_i64(record_ts);
    b.extend_from_slice(raw);
    b.freeze()
}

/// Split a wrapped value into `(recordTs, raw_aggregate_bytes)`.
pub(crate) fn unwrap_value(wrapped: &[u8]) -> (i64, &[u8]) {
    let ts = i64::from_be_bytes(wrapped[..TS_SIZE].try_into().expect("8 bytes"));
    (ts, &wrapped[TS_SIZE..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_key_layout_and_window_start() {
        let k = store_key(b"k", 0x0102, 0);
        assert2::assert!(k.len() == 13); // "k"(1) ‖ ws:8 ‖ seq:4
        assert2::assert!(&k[1..9] == &0x0102i64.to_be_bytes());
        assert2::assert!(&k[9..13] == &[0, 0, 0, 0]);
        assert2::assert!(window_start_of(&k) == 0x0102);
        assert2::assert!(key_bytes_of(&k) == b"k");
    }

    #[test]
    fn store_key_encodes_seqnum() {
        let k0 = store_key(b"k", 5, 0);
        let k1 = store_key(b"k", 5, 1);
        assert2::assert!(&k0[k0.len() - 4..] == &[0, 0, 0, 0]);
        assert2::assert!(&k1[k1.len() - 4..] == &1u32.to_be_bytes());
        assert2::assert!(k1 > k0); // same (key, ts), higher seqnum sorts after
        assert2::assert!(window_start_of(&k1) == 5);
        assert2::assert!(key_bytes_of(&k1) == b"k");
    }

    #[test]
    fn value_wrap_unwrap() {
        let v = wrap_value(7, &99i64.to_be_bytes());
        assert2::assert!(&v[0..8] == &7i64.to_be_bytes()); // ts prefix
        let (ts, raw) = unwrap_value(&v);
        assert2::assert!(ts == 7);
        assert2::assert!(raw == &99i64.to_be_bytes());
    }
}
