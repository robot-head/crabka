//! Codec helpers for `ConsumerProtocol` subscription / assignment payloads,
//! and the [`AutoOffsetReset`] / [`IsolationLevel`] enums used by
//! [`Consumer::builder`].

use bytes::{Bytes, BytesMut};

/// What to do when a partition has no committed offset.
#[derive(Debug, Clone, Copy)]
pub enum AutoOffsetReset {
    /// Start from offset 0.
    Earliest,
    /// Start from the log-end offset. Resolved lazily by `Consumer::poll`
    /// using `ListOffsets(timestamp=-1)`.
    Latest,
}

/// Controls which records are visible to this consumer.
///
/// Maps to Kafka's `isolation.level` configuration and the `isolation_level`
/// field in the `Fetch` request (wire value: `i8`).
///
/// The default is [`ReadUncommitted`](IsolationLevel::ReadUncommitted) for
/// backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// All records are visible, including those from open or aborted
    /// transactions. Equivalent to `isolation.level=read_uncommitted`.
    ReadUncommitted,
    /// Only records from committed transactions (and non-transactional
    /// records) are visible. Equivalent to `isolation.level=read_committed`.
    ReadCommitted,
}

impl IsolationLevel {
    /// Returns the wire encoding used in the `Fetch` request (`i8`).
    pub(crate) fn wire(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

// ── subscription / assignment codec (ConsumerProtocol v1) ─────────────────

/// Encode a `ConsumerProtocolSubscription` v1 record:
/// version (i16=1) + topics (array<STRING>) + `user_data` (BYTES=-1).
pub(crate) fn encode_subscription(topics: &[String]) -> Bytes {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    let n = i32::try_from(topics.len()).expect("topics fit in i32");
    buf.put_i32(n);
    for t in topics {
        let len = i16::try_from(t.len()).expect("topic name fits in i16");
        buf.put_i16(len);
        buf.put_slice(t.as_bytes());
    }
    buf.put_i32(-1); // user_data null
    buf.freeze()
}

pub(crate) fn decode_subscription(bytes: &[u8]) -> Vec<String> {
    use bytes::Buf;
    let mut cur = bytes;
    if cur.remaining() < 2 {
        return Vec::new();
    }
    let _version = cur.get_i16();
    if cur.remaining() < 4 {
        return Vec::new();
    }
    let n = cur.get_i32();
    let cap = usize::try_from(n.max(0)).unwrap_or(0);
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        if cur.remaining() < 2 {
            break;
        }
        let len = cur.get_i16();
        let len = usize::try_from(len.max(0)).unwrap_or(0);
        if cur.remaining() < len {
            break;
        }
        let mut s = vec![0u8; len];
        cur.copy_to_slice(&mut s);
        if let Ok(s) = String::from_utf8(s) {
            out.push(s);
        }
    }
    out
}

/// Encode a `ConsumerProtocolAssignment` v1:
/// version (i16=1) + `assigned_partitions` (array<{topic, partitions: array<i32>}>)
/// + `user_data` (BYTES=-1).
pub(crate) fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    use bytes::BufMut;
    let mut by_topic: std::collections::BTreeMap<&str, Vec<i32>> =
        std::collections::BTreeMap::new();
    for (t, p) in partitions {
        by_topic.entry(t.as_str()).or_default().push(*p);
    }
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    let n = i32::try_from(by_topic.len()).expect("topics fit in i32");
    buf.put_i32(n);
    for (topic, parts) in by_topic {
        let len = i16::try_from(topic.len()).expect("topic name fits in i16");
        buf.put_i16(len);
        buf.put_slice(topic.as_bytes());
        let pn = i32::try_from(parts.len()).expect("partition count fits in i32");
        buf.put_i32(pn);
        for p in parts {
            buf.put_i32(p);
        }
    }
    buf.put_i32(-1);
    buf.freeze()
}

pub(crate) fn decode_assignment(bytes: &[u8]) -> Vec<(String, i32)> {
    use bytes::Buf;
    let mut cur = bytes;
    if cur.remaining() < 2 {
        return Vec::new();
    }
    let _version = cur.get_i16();
    if cur.remaining() < 4 {
        return Vec::new();
    }
    let topic_count = cur.get_i32();
    let mut out = Vec::new();
    for _ in 0..topic_count.max(0) {
        if cur.remaining() < 2 {
            break;
        }
        let len = cur.get_i16();
        let len = usize::try_from(len.max(0)).unwrap_or(0);
        if cur.remaining() < len {
            break;
        }
        let mut name = vec![0u8; len];
        cur.copy_to_slice(&mut name);
        let Ok(topic) = String::from_utf8(name) else {
            break;
        };
        if cur.remaining() < 4 {
            break;
        }
        let pcount = cur.get_i32();
        for _ in 0..pcount.max(0) {
            if cur.remaining() < 4 {
                break;
            }
            out.push((topic.clone(), cur.get_i32()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_round_trip() {
        let s = encode_subscription(&["t1".into(), "t2".into()]);
        let decoded = decode_subscription(&s);
        assert_eq!(decoded, vec!["t1", "t2"]);
    }

    #[test]
    fn subscription_empty_round_trip() {
        let s = encode_subscription(&[]);
        let decoded = decode_subscription(&s);
        assert!(decoded.is_empty());
    }

    #[test]
    fn assignment_round_trip() {
        let s = encode_assignment(&[("t".into(), 0), ("t".into(), 1), ("u".into(), 0)]);
        let decoded = decode_assignment(&s);
        assert!(decoded.contains(&("t".into(), 0)));
        assert!(decoded.contains(&("t".into(), 1)));
        assert!(decoded.contains(&("u".into(), 0)));
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn assignment_empty_round_trip() {
        let s = encode_assignment(&[]);
        let decoded = decode_assignment(&s);
        assert!(decoded.is_empty());
    }
}
