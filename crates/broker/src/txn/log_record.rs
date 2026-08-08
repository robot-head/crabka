//! Byte-exact codec for Kafka's `TransactionLogValue` and `TransactionLogKey`.
//!
//! The codec covers `TransactionLogValue` v0 and v1, and `TransactionLogKey`
//! v0. It matches the on-disk records that the `__transaction_state` topic
//! carries in Apache Kafka 4.0.
//!
//! This module is a codec only. The transaction coordinator owns the runtime
//! wiring.
//!
//! Schema, from cp-kafka 4.0 `TransactionLogValue.json` and
//! `TransactionLogKey.json`:
//!
//! `TransactionLogKey`: validVersions "0", flexibleVersions "none". The wire
//! form is an `int16` version, which is 0, and then the `TransactionalId` as a
//! non-compact string: an `int16` length and the UTF-8 bytes.
//!
//! `TransactionLogValue`: validVersions "0-1", flexibleVersions "1+". The wire
//! form, in field order: `int16` version, `int64` `ProducerId`, `int16`
//! `ProducerEpoch`, `int32` `TransactionTimeoutMs`, `int8` `TransactionStatus`,
//! a nullable array of `{ string Topic; int32[] PartitionIds }`, `int64`
//! `TransactionLastUpdateTimestampMs`, `int64` `TransactionStartTimestampMs`.
//! v1 adds a trailing tagged-field section on every struct. The codec writes
//! these tags only when the value is not the default:
//!
//!   * tag 0, `PreviousProducerId`, default -1;
//!   * tag 1, `NextProducerId`, default -1;
//!   * tag 2, `ClientTransactionVersion`, default 0.
//!
//! v0 is non-flexible: arrays use `int32` lengths (-1 = null), strings use
//! `int16` lengths, and there is no tagged-field section anywhere. v1 is
//! flexible: arrays use compact `uvarint(n+1)` lengths (0 = null), strings use
//! compact `uvarint(len+1)` lengths, and every struct ends with a
//! tagged-field section.

use std::collections::{BTreeMap, HashSet};

use bytes::{Bytes, BytesMut};
use crabka_ids::PartitionIndex;
use crabka_log::ProducerId;
use crabka_protocol::{
    ProtocolError,
    primitives::{
        array::{get_array_len, get_nullable_array_len, put_array_len, put_nullable_array_len},
        fixed::{get_i8, get_i16, get_i32, get_i64, put_i8, put_i16, put_i32, put_i64},
        string_bytes::{
            get_compact_string_owned, get_string_owned, put_compact_string, put_string,
        },
    },
    tagged_fields::{UnknownTaggedFields, WriteTaggedFields, read_tagged_fields},
};

use crate::{
    error::BrokerError,
    txn::state::{TopicPartition, TxnEntry, TxnState},
};

/// Tagged-field tags for `TransactionLogValue` v1.
const TAG_PREV_PRODUCER_ID: u32 = 0;
const TAG_NEXT_PRODUCER_ID: u32 = 1;
const TAG_CLIENT_TXN_VERSION: u32 = 2;

/// Kafka's tagged-field default for the producer-id bookkeeping fields.
const PRODUCER_ID_NONE: i64 = -1;

/// Group `partitions` into one entry per topic with ascending partition ids.
///
/// The function orders the topics lexicographically. This order matters: the
/// `HashSet` iteration order is nondeterministic, but replicas must produce
/// identical snapshot bytes.
fn group_partitions(partitions: &HashSet<TopicPartition>) -> Vec<(&str, Vec<i32>)> {
    let mut by_topic: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for tp in partitions {
        by_topic
            .entry(&tp.topic)
            .or_default()
            .push(tp.partition.get());
    }
    by_topic
        .into_iter()
        .map(|(topic, mut ids)| {
            ids.sort_unstable();
            (topic, ids)
        })
        .collect()
}

/// Encode the Kafka `TransactionLogValue`.
///
/// When `flexible` is true, the function selects v1, the flexible form, for
/// `TV_1` and `TV_2`. When it is false, the function selects v0, the
/// non-flexible form, for `TV_0`. The output is deterministic: the function
/// groups and sorts the partitions before it encodes them.
pub(crate) fn encode_value(entry: &TxnEntry, flexible: bool) -> Vec<u8> {
    let version: i16 = i16::from(flexible);
    let mut buf = BytesMut::new();

    put_i16(&mut buf, version);
    put_i64(&mut buf, entry.producer_id.get());
    put_i16(&mut buf, entry.producer_epoch);
    put_i32(&mut buf, entry.txn_timeout_ms);
    put_i8(&mut buf, entry.state.to_kafka_status());

    // TransactionPartitions: nullable array; empty -> null.
    let groups = group_partitions(&entry.partitions);
    if groups.is_empty() {
        put_nullable_array_len(&mut buf, None, flexible);
    } else {
        put_nullable_array_len(&mut buf, Some(groups.len()), flexible);
        for (topic, ids) in &groups {
            if flexible {
                put_compact_string(&mut buf, topic);
            } else {
                put_string(&mut buf, topic);
            }
            put_array_len(&mut buf, ids.len(), flexible);
            for id in ids {
                put_i32(&mut buf, *id);
            }
            // PartitionsSchema has no tagged fields of its own; in v1 it still
            // ends with an (always-empty) tagged-field section.
            if flexible {
                WriteTaggedFields::new().write(&mut buf, &UnknownTaggedFields::default());
            }
        }
    }

    put_i64(&mut buf, entry.last_update_ms);
    put_i64(&mut buf, entry.start_ms);

    // Top-level tagged-field section (v1 only). Emit prev/next producer ids
    // only when non-default; ClientTransactionVersion is always its default 0
    // (TxnEntry has no such field) and so is always omitted.
    if flexible {
        let mut tagged = WriteTaggedFields::new();
        if !entry.prev_producer_id.is_none() {
            tagged.add(
                TAG_PREV_PRODUCER_ID,
                i64_to_bytes(entry.prev_producer_id.get()),
            );
        }
        if !entry.next_producer_id.is_none() {
            tagged.add(
                TAG_NEXT_PRODUCER_ID,
                i64_to_bytes(entry.next_producer_id.get()),
            );
        }
        tagged.write(&mut buf, &UnknownTaggedFields::default());
    }

    buf.to_vec()
}

fn i64_to_bytes(v: i64) -> Bytes {
    let mut b = BytesMut::with_capacity(8);
    put_i64(&mut b, v);
    b.freeze()
}

/// Decode a `TransactionLogValue`.
///
/// The caller supplies `transactional_id` from the companion key. It is not
/// present in the value record.
pub(crate) fn decode_value(
    bytes: &[u8],
    transactional_id: String,
) -> Result<TxnEntry, BrokerError> {
    let mut buf = bytes;
    let version = get_i16(&mut buf)?;
    let flexible = match version {
        0 => false,
        1 => true,
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unsupported TransactionLogValue version",
            )));
        }
    };

    let producer_id = get_i64(&mut buf)?;
    let producer_epoch = get_i16(&mut buf)?;
    let txn_timeout_ms = get_i32(&mut buf)?;
    let status = get_i8(&mut buf)?;
    let state = TxnState::from_kafka_status(status).ok_or(BrokerError::Protocol(
        ProtocolError::InvalidValue("unknown TransactionStatus"),
    ))?;

    let mut partitions = HashSet::new();
    if let Some(count) = get_nullable_array_len(&mut buf, flexible)? {
        for _ in 0..count {
            let topic = if flexible {
                get_compact_string_owned(&mut buf)?
            } else {
                get_string_owned(&mut buf)?
            };
            let id_count = get_array_len(&mut buf, flexible)?;
            for _ in 0..id_count {
                let partition = get_i32(&mut buf)?;
                partitions.insert(TopicPartition {
                    topic: topic.clone(),
                    partition: PartitionIndex(partition),
                });
            }
            // PartitionsSchema tagged-field section (v1 only); no known tags.
            if flexible {
                read_tagged_fields(&mut buf, |_, _| Ok(false))?;
            }
        }
    }

    let last_update_ms = get_i64(&mut buf)?;
    let start_ms = get_i64(&mut buf)?;

    let mut prev_producer_id = PRODUCER_ID_NONE;
    let mut next_producer_id = PRODUCER_ID_NONE;
    if flexible {
        read_tagged_fields(&mut buf, |tag, payload| match tag {
            TAG_PREV_PRODUCER_ID => {
                prev_producer_id = get_i64(payload)?;
                Ok(true)
            }
            TAG_NEXT_PRODUCER_ID => {
                next_producer_id = get_i64(payload)?;
                Ok(true)
            }
            // ClientTransactionVersion: recognised but not stored on TxnEntry.
            TAG_CLIENT_TXN_VERSION => {
                let _ = get_i16(payload)?;
                Ok(true)
            }
            _ => Ok(false),
        })?;
    }

    if !buf.is_empty() {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "TransactionLogValue: trailing bytes after decode",
        )));
    }

    Ok(TxnEntry {
        transactional_id,
        // Wrap the decoded raw `i64`s into `ProducerId` at the codec boundary.
        producer_id: ProducerId(producer_id),
        producer_epoch,
        state,
        txn_timeout_ms,
        partitions,
        prev_producer_id: ProducerId(prev_producer_id),
        next_producer_id: ProducerId(next_producer_id),
        last_update_ms,
        start_ms,
    })
}

/// Encode the Kafka `TransactionLogKey`, version 0.
pub(crate) fn encode_key(transactional_id: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    put_i16(&mut buf, 0);
    put_string(&mut buf, transactional_id);
    buf.to_vec()
}

/// Decode a Kafka `TransactionLogKey` and return the transactional id.
pub(crate) fn decode_key(bytes: &[u8]) -> Result<String, BrokerError> {
    let mut buf = bytes;
    let version = get_i16(&mut buf)?;
    if version != 0 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "unsupported TransactionLogKey version",
        )));
    }
    let transactional_id = get_string_owned(&mut buf)?;
    if !buf.is_empty() {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "TransactionLogKey: trailing bytes after decode",
        )));
    }
    Ok(transactional_id)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// Real captured v1 record (`TV_1`, Ongoing, 48 bytes).
    #[rustfmt::skip]
    const SAMPLE: &[u8] = &[
        0x00, 0x01, // version = 1
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ProducerId = 0
        0x00, 0x00, // ProducerEpoch = 0
        0x00, 0x00, 0xea, 0x60, // TransactionTimeoutMs = 60000
        0x01, // TransactionStatus = 1 (Ongoing)
        0x02, // partitions compact-array len = 1+1
        0x07, b't', b'x', b't', b'e', b's', b't', // topic compact-string "txtest"
        0x02, // PartitionIds compact-array len = 1+1
        0x00, 0x00, 0x00, 0x00, // partition 0
        0x00, // PartitionsSchema tagged-count = 0
        0x00, 0x00, 0x01, 0x9e, 0x7b, 0x4b, 0x36, 0x7a, // LastUpdate ts
        0x00, 0x00, 0x01, 0x9e, 0x7b, 0x4b, 0x36, 0x7a, // Start ts
        0x00, // top-level tagged-count = 0
    ];

    const SAMPLE_TS: i64 = 0x0000_019e_7b4b_367a;

    fn sample_entry() -> TxnEntry {
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition {
            topic: "txtest".into(),
            partition: PartitionIndex(0),
        });
        TxnEntry {
            transactional_id: "my-txn-id".into(),
            producer_id: ProducerId(0),
            producer_epoch: 0,
            state: TxnState::Ongoing,
            txn_timeout_ms: 60_000,
            partitions,
            prev_producer_id: ProducerId(-1),
            next_producer_id: ProducerId(-1),
            last_update_ms: SAMPLE_TS,
            start_ms: SAMPLE_TS,
        }
    }

    #[test]
    fn sample_bytes_decode() {
        let entry = decode_value(SAMPLE, "my-txn-id".into()).unwrap();
        check!(entry.producer_id == 0);
        check!(entry.producer_epoch == 0);
        check!(entry.txn_timeout_ms == 60_000);
        check!(entry.state == TxnState::Ongoing);
        check!(entry.prev_producer_id == -1);
        check!(entry.next_producer_id == -1);
        check!(entry.last_update_ms == SAMPLE_TS);
        check!(entry.start_ms == SAMPLE_TS);
        let expected: HashSet<TopicPartition> = [TopicPartition {
            topic: "txtest".into(),
            partition: PartitionIndex(0),
        }]
        .into_iter()
        .collect();
        check!(entry.partitions == expected);
    }

    #[test]
    fn sample_bytes_encode_byte_identical() {
        let encoded = encode_value(&sample_entry(), true);
        assert!(
            encoded == SAMPLE,
            "encode_value did not byte-match SAMPLE\n  expected: {:02x?}\n  actual:   {:02x?}",
            SAMPLE,
            encoded
        );
    }

    #[test]
    fn v1_round_trip_multi_topic_nondefault_ids() {
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition {
            topic: "zebra".into(),
            partition: PartitionIndex(5),
        });
        partitions.insert(TopicPartition {
            topic: "zebra".into(),
            partition: PartitionIndex(1),
        });
        partitions.insert(TopicPartition {
            topic: "alpha".into(),
            partition: PartitionIndex(3),
        });
        let entry = TxnEntry {
            transactional_id: "tid".into(),
            producer_id: ProducerId(42),
            producer_epoch: 7,
            state: TxnState::PrepareCommit,
            txn_timeout_ms: 30_000,
            partitions,
            prev_producer_id: ProducerId(100),
            next_producer_id: ProducerId(200),
            last_update_ms: 1_234_567,
            start_ms: 1_000_000,
        };

        let first = encode_value(&entry, true);
        let decoded = decode_value(&first, "tid".into()).unwrap();

        check!(decoded.producer_id == 42);
        check!(decoded.producer_epoch == 7);
        check!(decoded.state == TxnState::PrepareCommit);
        check!(decoded.txn_timeout_ms == 30_000);
        check!(decoded.prev_producer_id == 100);
        check!(decoded.next_producer_id == 200);
        check!(decoded.last_update_ms == 1_234_567);
        check!(decoded.start_ms == 1_000_000);
        check!(decoded.partitions == entry.partitions);

        // Re-encode is byte-identical (determinism).
        let second = encode_value(&decoded, true);
        assert!(first == second);
    }

    #[test]
    fn v0_round_trip_no_tagged_section() {
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition {
            topic: "t".into(),
            partition: PartitionIndex(0),
        });
        let entry = TxnEntry {
            transactional_id: "tid".into(),
            producer_id: ProducerId(9),
            producer_epoch: 2,
            state: TxnState::Ongoing,
            txn_timeout_ms: 60_000,
            partitions,
            // Even with non-default ids, v0 has no tagged section, so they are
            // dropped on encode and come back as the -1 default.
            prev_producer_id: ProducerId(5),
            next_producer_id: ProducerId(6),
            last_update_ms: 111,
            start_ms: 222,
        };

        let encoded = encode_value(&entry, false);
        // version header is `00 00`.
        assert!(encoded[0] == 0x00 && encoded[1] == 0x00);

        let decoded = decode_value(&encoded, "tid".into()).unwrap();
        check!(decoded.producer_id == 9);
        check!(decoded.state == TxnState::Ongoing);
        check!(decoded.partitions == entry.partitions);
        check!(decoded.last_update_ms == 111);
        check!(decoded.start_ms == 222);
        // v0 carries no tagged fields; bookkeeping ids default to -1.
        check!(decoded.prev_producer_id == -1);
        check!(decoded.next_producer_id == -1);
    }

    #[test]
    fn key_round_trip() {
        let encoded = encode_key("abc");
        assert!(decode_key(&encoded).unwrap() == "abc");
        // `00 00` version + int16 length (3) + bytes.
        assert!(encoded == &[0x00, 0x00, 0x00, 0x03, b'a', b'b', b'c']);
    }

    #[test]
    fn encode_is_deterministic_across_hashset_orders() {
        let make = |order: &[(&str, i32)]| {
            let mut partitions = HashSet::new();
            for (t, p) in order {
                partitions.insert(TopicPartition {
                    topic: (*t).into(),
                    partition: PartitionIndex(*p),
                });
            }
            TxnEntry {
                transactional_id: "tid".into(),
                producer_id: ProducerId(1),
                producer_epoch: 0,
                state: TxnState::Ongoing,
                txn_timeout_ms: 60_000,
                partitions,
                prev_producer_id: ProducerId(-1),
                next_producer_id: ProducerId(-1),
                last_update_ms: 1,
                start_ms: 1,
            }
        };

        let a = make(&[("b", 2), ("a", 1), ("b", 0), ("a", 3)]);
        let b = make(&[("a", 3), ("b", 0), ("a", 1), ("b", 2)]);
        assert!(encode_value(&a, true) == encode_value(&b, true));
        assert!(encode_value(&a, false) == encode_value(&b, false));
    }

    #[test]
    fn decode_value_rejects_truncated_input() {
        // A prefix of the valid SAMPLE must error, not panic.
        for input in [&SAMPLE[..10], &SAMPLE[..1], &[][..]] {
            assert!(decode_value(input, "t".into()).is_err());
        }
    }

    #[test]
    fn decode_value_rejects_unknown_version() {
        // Version 2 is not a valid TransactionLogValue version.
        let mut bad = SAMPLE.to_vec();
        bad[0] = 0x00;
        bad[1] = 0x02; // version = 2
        assert!(decode_value(&bad, "t".into()).is_err());
    }

    #[test]
    fn decode_value_rejects_trailing_bytes() {
        let mut extra = SAMPLE.to_vec();
        extra.push(0xff); // one trailing byte
        assert!(decode_value(&extra, "t".into()).is_err());
    }

    #[test]
    fn decode_key_rejects_unknown_version_and_truncation() {
        let key = encode_key("abc");
        // unknown version
        let mut bad = key.clone();
        bad[1] = 0x09;
        assert!(decode_key(&bad).is_err());
        // truncated
        assert!(decode_key(&key[..1]).is_err());
    }

    #[test]
    fn empty_partitions_round_trips_as_null_both_versions() {
        // An entry with no partitions encodes the array as null and decodes
        // back to an empty set, for both v0 and v1.
        let e = TxnEntry::new_empty("tid".into(), ProducerId(5), 0, 30_000, 100);
        for flexible in [false, true] {
            let bytes = encode_value(&e, flexible);
            let decoded = decode_value(&bytes, "tid".into()).expect("decode");
            assert!(decoded.partitions.is_empty());
            assert!(decoded.producer_id == 5);
        }
    }
}
