//! Record-payload generator. The first 24 bytes of every produced record
//! hold `(magic_be, scenario_id_be, send_unix_nanos_be)`, so a consumer can
//! compute the end-to-end latency when it re-reads the embedded
//! `send_unix_nanos`. The remaining bytes are a deterministic filler, so
//! the wire size is exactly the scenario's message size.
//!
//! The header is 24 bytes and not the 16 the plan sketched, because it needs a
//! magic to detect "this is one of ours". Kafka's own producers leave their own
//! headers in there, and this driver must not misread their bytes.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use crabka_units::prelude::*;

use crate::numeric::saturating_u128_to_u64;

/// Magic prefix on every record, so a consumer can confirm that this driver
/// produced the record and that it is not data already in the topic.
pub const MAGIC: [u8; 8] = *b"CRABKA_B";
pub const HEADER_LEN: usize = MAGIC.len() + 8 + 8; // magic + scenario_id + send_nanos = 24

/// Builds a reusable filler template of exactly `msg_size` bytes, or of the
/// header length if that is larger. The first 24 bytes are zero, and
/// `stamp_into` overwrites them at send time. The remaining bytes are a
/// repeating pattern.
#[must_use]
pub fn template(msg_size: ByteSize) -> BytesMut {
    let len = msg_size.bytes_usize().max(HEADER_LEN);
    let mut b = BytesMut::with_capacity(len);
    b.resize(len, 0u8);
    // Fill the body with a repeating ramp so compression has *some* work.
    // All-zeros compresses too well; all-random compresses too poorly.
    for (i, byte) in b.iter_mut().enumerate().skip(HEADER_LEN) {
        *byte = u8::try_from(i & 0xff).unwrap_or_default();
    }
    b
}

/// Stamps the magic, the `scenario_id`, and the current `unix_nanos` into the
/// first 24 bytes of `buf`. Returns the value as a `Bytes`, which the caller
/// clones cheaply with a `BytesMut::freeze`-style copy.
pub fn stamp_into(buf: &mut BytesMut, scenario_id: u64) -> Bytes {
    debug_assert!(buf.len() >= HEADER_LEN, "buf too short for header");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| saturating_u128_to_u64(d.as_nanos()));
    buf[..MAGIC.len()].copy_from_slice(&MAGIC);
    buf[8..16].copy_from_slice(&scenario_id.to_be_bytes());
    buf[16..24].copy_from_slice(&nanos.to_be_bytes());
    // Freeze a copy into a Bytes the producer can keep.
    Bytes::copy_from_slice(buf)
}

/// Reads the embedded `send_unix_nanos` if the record is one of ours.
/// Returns `None` if the magic does not match, and also if the record is too
/// short. The consumer skips both cases silently.
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
        let mut t = template(bytes(64));
        let b = stamp_into(&mut t, 0xdead_beef);
        let n = read_send_nanos(&b, 0xdead_beef).expect("magic+sid match");
        assert2::assert!(n > 0);
    }

    #[test]
    fn rejects_wrong_scenario_id() {
        let mut t = template(bytes(64));
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
        assert2::assert!(template(kibibytes(1)).len() == 1024);
        assert2::assert!(template(bytes(512)).len() == 512);
    }

    #[test]
    fn template_min_size_is_header() {
        assert2::assert!(template(ByteSize::ZERO).len() == HEADER_LEN);
    }
}
