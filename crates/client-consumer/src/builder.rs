//! Codec helpers for the `ConsumerProtocol` subscription and assignment
//! payloads, plus the [`AutoOffsetReset`] and [`IsolationLevel`] enums that
//! [`Consumer::builder`] uses.

use bytes::{Bytes, BytesMut};

/// What to do when a partition has no committed offset.
#[derive(Debug, Clone, Copy)]
pub enum AutoOffsetReset {
    /// Start from offset 0.
    Earliest,
    /// Start from the log-end offset. `Consumer::poll` resolves it lazily with
    /// `ListOffsets(timestamp=-1)`.
    Latest,
    /// Do not reset automatically. On a missing offset or a detected truncation,
    /// `poll` returns `ConsumerError::LogTruncation` and surfaces the error.
    None,
}

impl std::str::FromStr for AutoOffsetReset {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            "none" => Ok(Self::None),
            _ => Err(format!("invalid auto offset reset: {value}")),
        }
    }
}

/// Controls which records are visible to this consumer.
///
/// This maps to Kafka's `isolation.level` configuration and to the
/// `isolation_level` field in the `Fetch` request, whose wire value is an `i8`.
///
/// The default is [`ReadUncommitted`](IsolationLevel::ReadUncommitted) for
/// backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// All records are visible, including those from open or aborted
    /// transactions. Equivalent to `isolation.level=read_uncommitted`.
    ReadUncommitted,
    /// Only records from committed transactions, plus non-transactional
    /// records, are visible. Equivalent to `isolation.level=read_committed`.
    ReadCommitted,
}

impl std::str::FromStr for IsolationLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "read-uncommitted" => Ok(Self::ReadUncommitted),
            "read-committed" => Ok(Self::ReadCommitted),
            _ => Err(format!("invalid isolation level: {value}")),
        }
    }
}

impl IsolationLevel {
    /// Returns the `i8` wire encoding that the `Fetch` request uses.
    pub(crate) fn wire(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

// ── subscription / assignment codec (ConsumerProtocol v3) ─────────────────

use crabka_protocol::{
    Decode, Encode, UnknownTaggedFields,
    owned::{
        consumer_protocol_assignment::{
            ConsumerProtocolAssignment, TopicPartition as AssignTopicPartition,
        },
        consumer_protocol_subscription::{
            ConsumerProtocolSubscription, TopicPartition as SubTopicPartition,
        },
    },
};

const SUBSCRIPTION_WIRE_VERSION: i16 = 3;
const ASSIGNMENT_WIRE_VERSION: i16 = 3;

#[derive(PartialEq)]
pub(crate) struct DecodedSubscription {
    pub topics: Vec<String>,
    pub owned: Vec<(String, i32)>,
    pub generation_id: i32,
    // Part of the ConsumerProtocolSubscription v3 wire surface; kept here
    // for symmetry with the wire form and round-trip tests, even though
    // the coordinator does not currently consult it for assignment.
    #[allow(dead_code)]
    pub rack_id: Option<String>,
}

fn group_by_topic(pairs: &[(String, i32)]) -> std::collections::BTreeMap<&str, Vec<i32>> {
    let mut by_topic: std::collections::BTreeMap<&str, Vec<i32>> =
        std::collections::BTreeMap::new();
    for (t, p) in pairs {
        by_topic.entry(t.as_str()).or_default().push(*p);
    }
    by_topic
}

fn peek_version(bytes: &[u8]) -> i16 {
    if bytes.len() < 2 {
        return 0;
    }
    i16::from_be_bytes([bytes[0], bytes[1]])
}

fn empty_subscription() -> DecodedSubscription {
    DecodedSubscription {
        topics: Vec::new(),
        owned: Vec::new(),
        generation_id: -1,
        rack_id: None,
    }
}

pub(crate) fn encode_subscription(
    topics: &[String],
    owned: &[(String, i32)],
    generation_id: i32,
    rack_id: Option<&str>,
) -> Bytes {
    use bytes::BufMut;
    let owned_partitions: Vec<SubTopicPartition> = group_by_topic(owned)
        .into_iter()
        .map(|(topic, partitions)| SubTopicPartition {
            topic: topic.to_string(),
            partitions,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    let msg = ConsumerProtocolSubscription {
        topics: topics.to_vec(),
        user_data: None,
        owned_partitions,
        generation_id,
        rack_id: rack_id.map(str::to_string),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::with_capacity(2 + msg.encoded_len(SUBSCRIPTION_WIRE_VERSION));
    buf.put_i16(SUBSCRIPTION_WIRE_VERSION);
    msg.encode(&mut buf, SUBSCRIPTION_WIRE_VERSION)
        .expect("ConsumerProtocolSubscription encode");
    buf.freeze()
}

pub(crate) fn decode_subscription(bytes: &[u8]) -> DecodedSubscription {
    let Some(payload) = bytes.get(2..) else {
        return empty_subscription();
    };
    let version = peek_version(bytes).clamp(0, SUBSCRIPTION_WIRE_VERSION);
    let mut cur = payload;
    let Ok(msg) = ConsumerProtocolSubscription::decode(&mut cur, version) else {
        return empty_subscription();
    };
    let mut owned = Vec::new();
    for tp in msg.owned_partitions {
        for p in tp.partitions {
            owned.push((tp.topic.clone(), p));
        }
    }
    DecodedSubscription {
        topics: msg.topics,
        owned,
        generation_id: msg.generation_id,
        rack_id: msg.rack_id,
    }
}

pub(crate) fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    use bytes::BufMut;
    let assigned_partitions: Vec<AssignTopicPartition> = group_by_topic(partitions)
        .into_iter()
        .map(|(topic, partitions)| AssignTopicPartition {
            topic: topic.to_string(),
            partitions,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    let msg = ConsumerProtocolAssignment {
        assigned_partitions,
        user_data: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let mut buf = BytesMut::with_capacity(2 + msg.encoded_len(ASSIGNMENT_WIRE_VERSION));
    buf.put_i16(ASSIGNMENT_WIRE_VERSION);
    msg.encode(&mut buf, ASSIGNMENT_WIRE_VERSION)
        .expect("ConsumerProtocolAssignment encode");
    buf.freeze()
}

pub(crate) fn decode_assignment(bytes: &[u8]) -> Vec<(String, i32)> {
    let Some(payload) = bytes.get(2..) else {
        return Vec::new();
    };
    let version = peek_version(bytes).clamp(0, ASSIGNMENT_WIRE_VERSION);
    let mut cur = payload;
    let Ok(msg) = ConsumerProtocolAssignment::decode(&mut cur, version) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for tp in msg.assigned_partitions {
        for p in tp.partitions {
            out.push((tp.topic.clone(), p));
        }
    }
    out
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn consumer_behavior_values_parse_exact_spellings() {
        assert2::assert!(matches!(
            "earliest".parse::<AutoOffsetReset>(),
            Ok(AutoOffsetReset::Earliest)
        ));
        assert2::assert!(matches!(
            "latest".parse::<AutoOffsetReset>(),
            Ok(AutoOffsetReset::Latest)
        ));
        assert2::assert!(matches!(
            "none".parse::<AutoOffsetReset>(),
            Ok(AutoOffsetReset::None)
        ));
        assert2::assert!("EARLIEST".parse::<AutoOffsetReset>().is_err());
        assert2::assert!("unknown".parse::<AutoOffsetReset>().is_err());
    }

    #[test]
    fn isolation_level_values_parse_exact_spellings() {
        assert2::assert!(
            "read-uncommitted".parse::<IsolationLevel>().unwrap()
                == IsolationLevel::ReadUncommitted
        );
        assert2::assert!(
            "read-committed".parse::<IsolationLevel>().unwrap() == IsolationLevel::ReadCommitted
        );
        assert2::assert!("read_committed".parse::<IsolationLevel>().is_err());
        assert2::assert!("unknown".parse::<IsolationLevel>().is_err());
    }

    #[test]
    fn isolation_level_wire_values_match_fetch_request_encoding() {
        for (_name, isolation, expected) in [
            ("read uncommitted", IsolationLevel::ReadUncommitted, 0),
            ("read committed", IsolationLevel::ReadCommitted, 1),
        ] {
            assert2::assert!(isolation.wire() == expected);
        }
    }

    #[test]
    fn peek_version_requires_two_bytes() {
        for (_name, bytes, want) in [
            ("empty", &[][..], 0),
            ("truncated", &[0x7f][..], 0),
            ("two byte version", &[0, 3][..], 3),
        ] {
            assert2::assert!(peek_version(bytes) == want);
        }
    }

    #[test]
    fn decode_subscription_short_or_malformed_payload_uses_empty_fallback() {
        for (_name, payload) in [
            ("empty", &[][..]),
            ("truncated version", &[0x7f][..]),
            ("version only", &[0, 3][..]),
        ] {
            let decoded = decode_subscription(payload);
            assert2::assert!(
                decoded
                    == DecodedSubscription {
                        topics: Vec::new(),
                        owned: Vec::new(),
                        generation_id: -1,
                        rack_id: None,
                    }
            );
        }
    }

    #[test]
    fn decode_assignment_short_or_malformed_payload_uses_empty_fallback() {
        for (_name, payload) in [
            ("empty", &[][..]),
            ("truncated version", &[0x7f][..]),
            ("version only", &[0, 3][..]),
        ] {
            assert2::assert!(decode_assignment(payload).is_empty());
        }
    }

    #[test]
    fn subscription_round_trip() {
        let s = encode_subscription(&["t1".into(), "t2".into()], &[], -1, None);
        let decoded = decode_subscription(&s);
        assert2::assert!(decoded.topics == vec!["t1", "t2"]);
    }

    #[test]
    fn subscription_empty_round_trip() {
        let s = encode_subscription(&[], &[], -1, None);
        let decoded = decode_subscription(&s);
        assert2::assert!(
            decoded
                == DecodedSubscription {
                    topics: Vec::new(),
                    owned: Vec::new(),
                    generation_id: -1,
                    rack_id: None,
                }
        );
    }

    #[test]
    fn subscription_v3_owned_partitions_round_trip() {
        let owned = vec![("t".into(), 0), ("t".into(), 1), ("u".into(), 0)];
        let s = encode_subscription(&["t".into(), "u".into()], &owned, -1, None);
        let decoded = decode_subscription(&s);
        let mut got = decoded.owned.clone();
        got.sort();
        let mut want = owned.clone();
        want.sort();
        assert2::assert!(got == want);
    }

    #[test]
    fn subscription_v3_generation_and_rack_round_trip() {
        let s = encode_subscription(&["t".into()], &[], 42, Some("rack-a"));
        let decoded = decode_subscription(&s);
        assert2::assert!(
            decoded
                == DecodedSubscription {
                    topics: vec!["t".into()],
                    owned: Vec::new(),
                    generation_id: 42,
                    rack_id: Some("rack-a".into()),
                }
        );
    }

    #[test]
    fn subscription_decodes_v1_payload() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i16(1);
        buf.put_i32(1);
        let t = "t1";
        buf.put_i16(i16::try_from(t.len()).unwrap());
        buf.put_slice(t.as_bytes());
        buf.put_i32(-1); // user_data null
        buf.put_i32(0); // owned_partitions empty (v1)
        let payload = buf.freeze();
        let decoded = decode_subscription(&payload);
        assert2::assert!(
            decoded
                == DecodedSubscription {
                    topics: vec!["t1".to_string()],
                    owned: Vec::new(),
                    generation_id: -1,
                    rack_id: None,
                }
        );
    }

    #[test]
    fn assignment_round_trip() {
        let s = encode_assignment(&[("t".into(), 0), ("t".into(), 1), ("u".into(), 0)]);
        let mut decoded = decode_assignment(&s);
        decoded.sort();
        assert2::assert!(
            decoded
                == vec![
                    ("t".to_string(), 0),
                    ("t".to_string(), 1),
                    ("u".to_string(), 0)
                ]
        );
    }

    #[test]
    fn assignment_empty_round_trip() {
        let s = encode_assignment(&[]);
        let decoded = decode_assignment(&s);
        assert2::assert!(decoded.is_empty());
    }
}
