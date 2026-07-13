//! JVM-exact `suppress`-buffer changelog VALUE codec.
//!
//! When a `KTable.suppress(...)` buffer has logging enabled, each buffered entry
//! is logged to a changelog topic. The changelog KEY is the record's serialized
//! key bytes; the changelog VALUE is:
//!
//! ```text
//! BufferValue.serialize(8) ‖ bufferTime:8B BE
//! ```
//!
//! where `BufferValue.serialize` lays out (all integers big-endian):
//!
//! ```text
//! ProcessorRecordContext.serialize()            // see `SuppressRecordCtx`
//! priorLen:4 ‖ prior?                            // addValue: -1 == null
//! oldLen:4   ‖ old?                              // -1 == null, -2 == same as prior
//! newLen:4   ‖ new?                              // addValue: -1 == null
//! ```
//!
//! and `ProcessorRecordContext.serialize()` is:
//!
//! ```text
//! timestamp:8 ‖ offset:8 ‖ topicLen:4 ‖ topic ‖ partition:4 ‖ headerCount:4(0)
//! ```
//!
//! These layouts are pinned by the Docker JVM capture in
//! `tests/jvm-capture/.../BufferValueCapture.java` → `tests/testdata/suppress_bufval/*.hex`.
//!
//! The `-2` "old same as prior" sentinel: the JVM checks reference identity
//! (`priorValue == oldValue`). On the first buffering of a key the prior IS the
//! record's `oldValue` (same array), so it always serializes as `-2`. Crabka uses
//! value-equality, which reproduces every captured case and round-trips cleanly;
//! the codec is self-consistent for restore regardless.
use bytes::{BufMut, Bytes, BytesMut};

const I64: usize = 8;
const I32: usize = 4;

/// Null / aliasing sentinels for the variable-length value slots.
const NULL: i32 = -1;
/// `oldValue` is the same array as `priorValue` (not re-serialized).
const SAME_AS_PRIOR: i32 = -2;

/// The subset of JVM `ProcessorRecordContext` that suppress carries (no headers).
/// Mirrors [`crate::processor::record::RecordContext`] but owned by the codec so the
/// serialized form is pinned here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuppressRecordCtx {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

impl SuppressRecordCtx {
    /// `ProcessorRecordContext.serialize()`:
    /// `ts:8 ‖ offset:8 ‖ topicLen:4 ‖ topic ‖ partition:4 ‖ headerCount:4(0)`.
    fn write(&self, b: &mut BytesMut) {
        b.put_i64(self.timestamp);
        b.put_i64(self.offset);
        b.put_i32(i32::try_from(self.topic.len()).expect("topic length fits i32"));
        b.extend_from_slice(self.topic.as_bytes());
        b.put_i32(self.partition);
        b.put_i32(0); // headerCount
    }

    /// Parse a context off the front of `buf`, returning it and the trailing bytes.
    fn read(buf: &[u8]) -> (Self, &[u8]) {
        let timestamp = read_i64(&buf[0..I64]);
        let offset = read_i64(&buf[I64..2 * I64]);
        let mut o = 2 * I64;
        let topic_len =
            usize::try_from(read_i32(&buf[o..o + I32])).expect("non-negative topic len");
        o += I32;
        let topic = String::from_utf8(buf[o..o + topic_len].to_vec()).expect("utf-8 topic");
        o += topic_len;
        let partition = read_i32(&buf[o..o + I32]);
        o += I32;
        // headerCount (always 0 for suppress) — skip.
        o += I32;
        (
            Self {
                topic,
                partition,
                offset,
                timestamp,
            },
            &buf[o..],
        )
    }
}

/// A decoded suppress-buffer changelog entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufferedChange {
    pub ctx: SuppressRecordCtx,
    pub prior: Option<Vec<u8>>,
    pub old: Option<Vec<u8>>,
    pub new: Option<Vec<u8>>,
    pub buffer_time: i64,
}

/// `addValue`: `-1` for null, else `len:4 ‖ bytes`.
fn add_value(b: &mut BytesMut, value: Option<&[u8]>) {
    match value {
        None => b.put_i32(NULL),
        Some(v) => {
            b.put_i32(i32::try_from(v.len()).expect("value length fits i32"));
            b.extend_from_slice(v);
        }
    }
}

/// Serialize one buffered entry to its JVM-exact changelog VALUE bytes.
pub(crate) fn serialize_buffer_change(
    ctx: &SuppressRecordCtx,
    prior: Option<&[u8]>,
    old: Option<&[u8]>,
    new: Option<&[u8]>,
    buffer_time: i64,
) -> Bytes {
    let mut b = BytesMut::new();
    ctx.write(&mut b);
    add_value(&mut b, prior);
    // old: -1 null, -2 same-as-prior, else len ‖ bytes.
    match old {
        None => b.put_i32(NULL),
        Some(o) if prior == Some(o) => b.put_i32(SAME_AS_PRIOR),
        Some(o) => {
            b.put_i32(i32::try_from(o.len()).expect("old value length fits i32"));
            b.extend_from_slice(o);
        }
    }
    add_value(&mut b, new);
    b.put_i64(buffer_time);
    b.freeze()
}

/// Read a length-prefixed value via `addValue` rules (`-1` == null), advancing `o`.
fn read_add_value(buf: &[u8], o: &mut usize) -> Option<Vec<u8>> {
    let len = read_i32(&buf[*o..*o + I32]);
    *o += I32;
    if len == NULL {
        return None;
    }
    let len = usize::try_from(len).expect("non-negative length");
    let v = buf[*o..*o + len].to_vec();
    *o += len;
    Some(v)
}

/// Parse a changelog VALUE back into a [`BufferedChange`].
pub(crate) fn deserialize_buffer_change(bytes: &[u8]) -> BufferedChange {
    let (ctx, rest) = SuppressRecordCtx::read(bytes);
    // `rest` is offset 0 into the variable part; index into `bytes` from there.
    let base = bytes.len() - rest.len();
    let mut o = base;
    let prior = read_add_value(bytes, &mut o);
    // old slot: may be the -2 alias.
    let old_tag = read_i32(&bytes[o..o + I32]);
    o += I32;
    let old = if old_tag == NULL {
        None
    } else if old_tag == SAME_AS_PRIOR {
        prior.clone()
    } else {
        let len = usize::try_from(old_tag).expect("non-negative length");
        let v = bytes[o..o + len].to_vec();
        o += len;
        Some(v)
    };
    let new = read_add_value(bytes, &mut o);
    let buffer_time = read_i64(&bytes[o..o + I64]);
    BufferedChange {
        ctx,
        prior,
        old,
        new,
        buffer_time,
    }
}

fn read_i64(b: &[u8]) -> i64 {
    i64::from_be_bytes(b.try_into().expect("8 bytes"))
}

fn read_i32(b: &[u8]) -> i32 {
    i32::from_be_bytes(b.try_into().expect("4 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(n: i64) -> Vec<u8> {
        n.to_be_bytes().to_vec()
    }

    fn ctx(timestamp: i64) -> SuppressRecordCtx {
        SuppressRecordCtx {
            topic: "in".to_string(),
            partition: 0,
            offset: 0,
            timestamp,
        }
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
    }

    // The three vectors below are the verbatim output of the Docker JVM capture
    // (`tests/testdata/suppress_bufval/*.hex`); they pin the codec to Kafka 4.1.

    /// `wc_first`: prior=null, old=null, new=count(1), ctx(ts=10), bufferTime=10.
    const WC_FIRST: &str = "000000000000000a0000000000000000000000026\
96e0000000000000000ffffffffffffffff000000080000000000000001000000000000000a";

    /// `wc_change`: prior=count(1), old=count(1) (→ -2), new=count(2), ts=12, bt=12.
    const WC_CHANGE: &str = "000000000000000c0000000000000000000000026\
96e0000000000000000000000080000000000000001fffffffe000000080000000000000002000000000000000c";

    /// `tombstone`: prior=count(1), old=count(1) (→ -2), new=null, ts=20, bt=20.
    const TOMBSTONE: &str = "00000000000000140000000000000000000000026\
96e0000000000000000000000080000000000000001fffffffeffffffff0000000000000014";

    #[test]
    fn wc_first_matches_jvm_bytes() {
        let bytes = serialize_buffer_change(&ctx(10), None, None, Some(&count(1)), 10);
        assert_eq!(hex(&bytes), WC_FIRST);
    }

    #[test]
    fn wc_change_matches_jvm_bytes() {
        let bytes = serialize_buffer_change(
            &ctx(12),
            Some(&count(1)),
            Some(&count(1)),
            Some(&count(2)),
            12,
        );
        assert_eq!(hex(&bytes), WC_CHANGE);
    }

    #[test]
    fn tombstone_matches_jvm_bytes() {
        let bytes = serialize_buffer_change(&ctx(20), Some(&count(1)), Some(&count(1)), None, 20);
        assert_eq!(hex(&bytes), TOMBSTONE);
    }

    #[test]
    fn ctx_layout_is_30_bytes_for_two_char_topic() {
        let mut b = BytesMut::new();
        ctx(10).write(&mut b);
        // ts:8 ‖ off:8 ‖ part:4 ‖ topicLen:4 ‖ "in":2 ‖ hdrs:4 = 30
        assert_eq!(b.len(), 30);
    }

    #[test]
    fn round_trips_wc_first() {
        let bytes = serialize_buffer_change(&ctx(10), None, None, Some(&count(1)), 10);
        let d = deserialize_buffer_change(&bytes);
        assert_eq!(d.ctx, ctx(10));
        assert_eq!(d.prior, None);
        assert_eq!(d.old, None);
        assert_eq!(d.new, Some(count(1)));
        assert_eq!(d.buffer_time, 10);
    }

    #[test]
    fn round_trips_same_as_prior_alias() {
        let bytes = serialize_buffer_change(
            &ctx(12),
            Some(&count(1)),
            Some(&count(1)),
            Some(&count(2)),
            12,
        );
        let d = deserialize_buffer_change(&bytes);
        // The -2 alias decodes old back to a copy of prior.
        assert_eq!(d.prior, Some(count(1)));
        assert_eq!(d.old, Some(count(1)));
        assert_eq!(d.new, Some(count(2)));
        assert_eq!(d.buffer_time, 12);
    }

    #[test]
    fn round_trips_tombstone() {
        let bytes = serialize_buffer_change(&ctx(20), Some(&count(1)), Some(&count(1)), None, 20);
        let d = deserialize_buffer_change(&bytes);
        assert_eq!(d.new, None);
        assert_eq!(d.old, Some(count(1)));
        assert_eq!(d.buffer_time, 20);
    }

    #[test]
    fn round_trips_distinct_prior_and_old() {
        // A later put where prior != old → both written out fully (no -2).
        let bytes = serialize_buffer_change(
            &ctx(33),
            Some(&count(7)),
            Some(&count(9)),
            Some(&count(11)),
            33,
        );
        let d = deserialize_buffer_change(&bytes);
        assert_eq!(d.prior, Some(count(7)));
        assert_eq!(d.old, Some(count(9)));
        assert_eq!(d.new, Some(count(11)));
        assert_eq!(d.buffer_time, 33);
    }

    #[test]
    fn longer_topic_name_round_trips() {
        let c = SuppressRecordCtx {
            topic: "KSTREAM-TOTABLE-0000000005".to_string(),
            partition: 3,
            offset: 42,
            timestamp: 100,
        };
        let bytes = serialize_buffer_change(&c, None, Some(&count(5)), Some(&count(6)), 100);
        let d = deserialize_buffer_change(&bytes);
        assert_eq!(d.ctx, c);
        assert_eq!(d.old, Some(count(5)));
        assert_eq!(d.new, Some(count(6)));
    }
}
