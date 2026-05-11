//! Owned `RecordBatch`, `Record`, and `RecordHeader` types.

use bytes::Bytes;

use crate::records::header::Attributes;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordHeader {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Record {
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: Attributes,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: 0,
            attributes: Attributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,    // sentinel: non-idempotent
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
        }
    }
}
