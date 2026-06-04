//! Dependency-injected I/O the runtime depends on: fetching source records,
//! producing sink records, and committing/fetching offsets. Real broker impls
//! live in `io_broker.rs`; fakes in tests make `StreamTask`/`StreamThread`
//! deterministically testable without a broker.

use bytes::Bytes;

use crate::error::StreamsClientError;

/// A fetched source record (timestamp is `-1` when the fetcher can't surface it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRec {
    pub offset: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
}

/// A batch of consecutive records from one partition.
#[derive(Debug, Clone, Default)]
pub struct FetchBatch {
    pub records: Vec<FetchedRec>,
}

impl FetchBatch {
    /// The offset to fetch next: one past the last record, or `current` if empty.
    #[must_use]
    pub fn next_offset(&self, current: i64) -> i64 {
        self.records.last().map_or(current, |r| r.offset + 1)
    }
}

#[async_trait::async_trait]
pub trait RecordFetcher: Send + Sync + 'static {
    /// Fetch records for `(topic, partition)` starting at `offset`. An empty
    /// batch means nothing new yet.
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<FetchBatch, StreamsClientError>;
}

#[async_trait::async_trait]
pub trait RecordProducer: Send + Sync + 'static {
    /// Enqueue a record to `topic` (producer default partitioner).
    async fn send(
        &self,
        topic: &str,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError>;
    /// Block until all enqueued records are acknowledged (durability barrier).
    async fn flush(&self) -> Result<(), StreamsClientError>;
}

#[async_trait::async_trait]
pub trait OffsetStore: Send + Sync + 'static {
    /// Committed offset for `(topic, partition)`, or `None` if never committed.
    async fn committed(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, StreamsClientError>;
    /// The earliest available offset (auto.offset.reset = earliest).
    async fn earliest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError>;
    /// Commit `(topic, partition, offset)` triples for the streams group.
    async fn commit(&self, offsets: &[(String, i32, i64)]) -> Result<(), StreamsClientError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn fetch_batch_next_offset_advances_past_last() {
        let b = FetchBatch {
            records: vec![
                FetchedRec {
                    offset: 5,
                    key: None,
                    value: Some(bytes::Bytes::from_static(b"a")),
                    timestamp: -1,
                },
                FetchedRec {
                    offset: 6,
                    key: None,
                    value: Some(bytes::Bytes::from_static(b"b")),
                    timestamp: -1,
                },
            ],
        };
        check!(b.next_offset(0) == 7);
        let empty = FetchBatch { records: vec![] };
        check!(empty.next_offset(9) == 9);
    }
}
