//! KIP-932 share-state record codecs for the `__share_group_state` internal
//! topic.
//!
//! Two record types share one key namespace, discriminated by a leading
//! `i16` record-type version: `ShareSnapshot` (a full per-partition state
//! image) and `ShareUpdate` (a delta). Keys then carry `group_id` (string),
//! `topic_id` (16 raw bytes), and `partition` (`i32`). Values use an
//! `i16(0)` version preamble, fixed fields, then a length-prefixed array of
//! [`StateBatch`].
//!
//! Distinct from the `__consumer_offsets` share-group keys (versions 9–14) —
//! this is a different topic with its own discriminator space.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_log::Offset;
use crabka_protocol::ProtocolError;
use uuid::Uuid;

use crate::{
    coordinator::unified::persistence::{get_i16, get_i32, get_i64, get_string, put_string},
    error::BrokerError,
};

pub const KEY_SHARE_SNAPSHOT: i16 = 0;
pub const KEY_SHARE_UPDATE: i16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareStateKey {
    pub record_type: i16,
    pub group_id: String,
    pub topic_id: Uuid,
    pub partition: i32,
}

#[must_use]
pub fn encode_state_key(k: &ShareStateKey) -> Bytes {
    let mut b = BytesMut::new();
    b.put_i16(k.record_type);
    put_string(&mut b, &k.group_id);
    b.put_slice(k.topic_id.as_bytes());
    b.put_i32(k.partition);
    b.freeze()
}

pub fn parse_state_key(mut buf: &[u8]) -> Result<ShareStateKey, BrokerError> {
    let record_type = get_i16(&mut buf)?;
    if record_type != KEY_SHARE_SNAPSHOT && record_type != KEY_SHARE_UPDATE {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "unknown share-state key type",
        )));
    }
    let group_id = get_string(&mut buf)?;
    if buf.len() < 16 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "short share-state key",
        )));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&buf[..16]);
    buf.advance(16);
    let topic_id = Uuid::from_bytes(id);
    let partition = get_i32(&mut buf)?;
    Ok(ShareStateKey {
        record_type,
        group_id,
        topic_id,
        partition,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateBatch {
    pub first_offset: Offset,
    pub last_offset: Offset,
    pub delivery_state: i8,
    pub delivery_count: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareSnapshotValue {
    pub snapshot_epoch: i64,
    pub state_epoch: i32,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<StateBatch>,
}

impl ShareSnapshotValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i64(self.snapshot_epoch);
        buf.put_i32(self.state_epoch);
        buf.put_i32(self.leader_epoch);
        buf.put_i64(self.start_offset.0);
        buf.put_i32(self.delivery_complete_count);
        put_batches(&mut buf, &self.state_batches);
        buf.freeze()
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let snapshot_epoch = get_i64(&mut buf)?;
        let state_epoch = get_i32(&mut buf)?;
        let leader_epoch = get_i32(&mut buf)?;
        let start_offset = Offset(get_i64(&mut buf)?);
        let delivery_complete_count = get_i32(&mut buf)?;
        let state_batches = get_batches(&mut buf)?;
        Ok(Self {
            snapshot_epoch,
            state_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count,
            state_batches,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareUpdateValue {
    pub snapshot_epoch: i64,
    pub leader_epoch: i32,
    pub start_offset: Offset,
    pub delivery_complete_count: i32,
    pub state_batches: Vec<StateBatch>,
}

impl ShareUpdateValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i64(self.snapshot_epoch);
        buf.put_i32(self.leader_epoch);
        buf.put_i64(self.start_offset.0);
        buf.put_i32(self.delivery_complete_count);
        put_batches(&mut buf, &self.state_batches);
        buf.freeze()
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let snapshot_epoch = get_i64(&mut buf)?;
        let leader_epoch = get_i32(&mut buf)?;
        let start_offset = Offset(get_i64(&mut buf)?);
        let delivery_complete_count = get_i32(&mut buf)?;
        let state_batches = get_batches(&mut buf)?;
        Ok(Self {
            snapshot_epoch,
            leader_epoch,
            start_offset,
            delivery_complete_count,
            state_batches,
        })
    }
}

fn put_batches(buf: &mut BytesMut, batches: &[StateBatch]) {
    let n = i32::try_from(batches.len()).expect("batch count fits in i32");
    buf.put_i32(n);
    for b in batches {
        buf.put_i64(b.first_offset.0);
        buf.put_i64(b.last_offset.0);
        buf.put_i8(b.delivery_state);
        buf.put_i16(b.delivery_count);
    }
}

fn get_batches(buf: &mut &[u8]) -> Result<Vec<StateBatch>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        let first_offset = Offset(get_i64(buf)?);
        let last_offset = Offset(get_i64(buf)?);
        if buf.remaining() < 1 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "share-state batch buf < i8",
            )));
        }
        let delivery_state = buf.get_i8();
        let delivery_count = get_i16(buf)?;
        out.push(StateBatch {
            first_offset,
            last_offset,
            delivery_state,
            delivery_count,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn peek_type(buf: &[u8]) -> i16 {
        let mut r = buf;
        r.get_i16()
    }

    #[test]
    fn key_round_trip_scenarios() {
        for (case, key, expected_type) in [
            (
                "snapshot key",
                ShareStateKey {
                    record_type: KEY_SHARE_SNAPSHOT,
                    group_id: "g1".into(),
                    topic_id: Uuid::from_bytes([7; 16]),
                    partition: 3,
                },
                KEY_SHARE_SNAPSHOT,
            ),
            (
                "update key",
                ShareStateKey {
                    record_type: KEY_SHARE_UPDATE,
                    group_id: "another-group".into(),
                    topic_id: Uuid::from_bytes([1; 16]),
                    partition: 0,
                },
                KEY_SHARE_UPDATE,
            ),
        ] {
            let bytes = encode_state_key(&key);
            assert!(peek_type(&bytes) == expected_type, "case {case}");
            assert!(parse_state_key(&bytes).unwrap() == key, "case {case}");
        }
    }

    #[test]
    fn unknown_key_type_rejected() {
        let mut b = BytesMut::new();
        b.put_i16(99);
        put_string(&mut b, "g");
        b.put_slice(&[0u8; 16]);
        b.put_i32(0);
        assert!(parse_state_key(&b.freeze()).is_err());
    }

    #[test]
    fn snapshot_value_round_trip() {
        let v = ShareSnapshotValue {
            snapshot_epoch: 5,
            state_epoch: 2,
            leader_epoch: 9,
            start_offset: Offset(100),
            delivery_complete_count: 4,
            state_batches: vec![
                StateBatch {
                    first_offset: Offset(100),
                    last_offset: Offset(109),
                    delivery_state: 0,
                    delivery_count: 1,
                },
                StateBatch {
                    first_offset: Offset(110),
                    last_offset: Offset(119),
                    delivery_state: 2,
                    delivery_count: 3,
                },
            ],
        };
        assert!(ShareSnapshotValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn update_value_round_trip() {
        let v = ShareUpdateValue {
            snapshot_epoch: 7,
            leader_epoch: 4,
            start_offset: Offset(200),
            delivery_complete_count: 11,
            state_batches: vec![StateBatch {
                first_offset: Offset(200),
                last_offset: Offset(250),
                delivery_state: 1,
                delivery_count: 2,
            }],
        };
        assert!(ShareUpdateValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn snapshot_value_empty_batches_round_trip() {
        let v = ShareSnapshotValue {
            snapshot_epoch: 0,
            state_epoch: 0,
            leader_epoch: 0,
            start_offset: Offset(0),
            delivery_complete_count: 0,
            state_batches: vec![],
        };
        assert!(ShareSnapshotValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn update_value_multi_batch_round_trip() {
        let batches: Vec<StateBatch> = (0..5)
            .map(|i| StateBatch {
                first_offset: Offset(i64::from(i) * 10),
                last_offset: Offset(i64::from(i) * 10 + 9),
                delivery_state: i8::try_from(i % 3).unwrap(),
                delivery_count: i16::try_from(i + 1).unwrap(),
            })
            .collect();
        let v = ShareUpdateValue {
            snapshot_epoch: 3,
            leader_epoch: 1,
            start_offset: Offset(0),
            delivery_complete_count: 42,
            state_batches: batches,
        };
        assert!(ShareUpdateValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn delivery_complete_count_preserved() {
        let v = ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 1,
            leader_epoch: 1,
            start_offset: Offset(0),
            delivery_complete_count: 1_234_567,
            state_batches: vec![],
        };
        let decoded = ShareSnapshotValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);
    }
}
