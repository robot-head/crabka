//! Record-payload generator. The first 24 bytes of every produced record
//! are reserved for `(magic_be, scenario_id_be, send_unix_nanos_be)` so
//! consumers can compute end-to-end latency by re-reading the embedded
//! `send_unix_nanos`. The remaining bytes are a deterministic filler so
//! the wire size is exactly `msg_size_bytes`.
//!
//! 24 bytes (not 16 as the plan sketched) because we want a magic to
//! detect "this is one of ours" — Kafka's own producers leave their
//! own headers in there and we don't want to misread their bytes.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};

/// Magic prefix on every record so consumers can confirm a record was
/// produced by this driver and not (say) some pre-existing data left in
/// the topic.
pub const MAGIC: [u8; 8] = *b"CRABKA_B";
pub const HEADER_LEN: usize = MAGIC.len() + 8 + 8; // magic + scenario_id + send_nanos = 24

/// Build a reusable filler template of exactly `msg_size_bytes` bytes.
/// The first 24 bytes are zero and will be overwritten by `stamp_into` at
/// send time; the remaining bytes are a repeating pattern.
#[must_use]
pub fn template(msg_size_bytes: usize) -> BytesMut {
    let mut b = BytesMut::with_capacity(msg_size_bytes.max(HEADER_LEN));
    b.resize(msg_size_bytes.max(HEADER_LEN), 0u8);
    // Fill the body with a repeating ramp so compression has *some* work.
    // All-zeros compresses too well; all-random compresses too poorly.
    for (i, byte) in b.iter_mut().enumerate().skip(HEADER_LEN) {
        *byte = (i & 0xff) as u8;
    }
    b
}

/// Stamp the magic + `scenario_id` + the current `unix_nanos` into the first
/// 24 bytes of `buf`. Returns the value as a `Bytes` (cheap clone via
/// `BytesMut::freeze`-style copy on the caller side).
pub fn stamp_into(buf: &mut BytesMut, scenario_id: u64) -> Bytes {
    debug_assert!(buf.len() >= HEADER_LEN, "buf too short for header");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    buf[..MAGIC.len()].copy_from_slice(&MAGIC);
    buf[8..16].copy_from_slice(&scenario_id.to_be_bytes());
    buf[16..24].copy_from_slice(&nanos.to_be_bytes());
    // Freeze a copy into a Bytes the producer can keep.
    Bytes::copy_from_slice(buf)
}

/// Read the embedded `send_unix_nanos` if the record is one of ours.
/// Returns `None` if the magic doesn't match or the record is too short
/// — both treated as "skip silently" by the consumer.
#[must_use]
pub fn read_send_nanos(value: &[u8], scenario_id: u64) -> Option<u64> {
    if value.len() < HEADER_LEN || value[..MAGIC.len()] != MAGIC {
        return None;
    }
    let sid = u64::from_be_bytes(value[8..16].try_into().ok()?);
    if sid != scenario_id {
        return None;
    }
    Some(u64::from_be_bytes(value[16..24].try_into().ok()?))
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn round_trip_send_nanos() {
        let mut t = template(64);
        let b = stamp_into(&mut t, 0xdead_beef);
        let n = read_send_nanos(&b, 0xdead_beef).expect("magic+sid match");
        assert2::assert!(n > 0);
    }

    #[test]
    fn rejects_wrong_scenario_id() {
        let mut t = template(64);
        let b = stamp_into(&mut t, 42);
        assert2::assert!(read_send_nanos(&b, 7).is_none());
    }

    #[test]
    fn rejects_short() {
        assert2::assert!(read_send_nanos(&[0u8; 8], 0).is_none());
    }

    #[test]
    fn rejects_bad_magic() {
        let mut b = vec![0u8; HEADER_LEN];
        b[16..24].copy_from_slice(&123u64.to_be_bytes());
        assert2::assert!(read_send_nanos(&b, 0).is_none());
    }

    #[test]
    fn template_size_honoured_above_header() {
        let t = template(1024);
        assert2::assert!(t.len() == 1024);
    }

    #[test]
    fn template_min_size_is_header() {
        let t = template(0);
        assert2::assert!(t.len() == HEADER_LEN);
    }
}
