//! Public record types: `ProducerRecord` (what you send), `RecordMetadata`
//! (what you get back), and `Header` (per-record key/value pairs).

use bytes::Bytes;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProducerRecord {
    pub topic: String,
    /// If `Some(p)`, the partitioner is bypassed and partition `p` is used.
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<Header>,
    /// If `None`, the producer fills in the current wall-clock time at
    /// accumulator append time.
    pub timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub key: String,
    pub value: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordMetadata {
    pub topic_index: usize, // index into the original topic list — useful for batching callers
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn producer_record_default_is_empty() {
        let r = ProducerRecord::default();
        assert!(
            r == ProducerRecord {
                topic: String::new(),
                partition: None,
                key: None,
                value: None,
                headers: vec![],
                timestamp_ms: None,
            }
        );
    }
}
