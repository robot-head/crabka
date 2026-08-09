//! The dependency-injected I/O that the runtime needs: it fetches source
//! records, produces sink records, and commits and fetches offsets.
//!
//! The real broker implementations live in `io_broker.rs`. Fakes in the tests
//! make `StreamTask` and `StreamThread` testable and deterministic without a
//! broker.

use bytes::Bytes;

use crate::error::StreamsClientError;

/// A fetched source record. The timestamp is `-1` when the fetcher cannot supply
/// it.
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
    /// The offset to fetch next. It is one past the last record, or `current`
    /// when the batch is empty.
    #[must_use]
    pub fn next_offset(&self, current: i64) -> i64 {
        self.records.last().map_or(current, |r| r.offset + 1)
    }
}

/// Fetch isolation level, the Kafka `Fetch.isolation_level`.
///
/// Under EOS-v2 the changelog restore must read `ReadCommitted`, so that it
/// excludes aborted writes. An aborted write is a record produced inside a
/// transaction that later aborted. The restored store then holds only committed
/// state. Normal source processing and the global-store bootstrap use
/// `ReadUncommitted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

#[async_trait::async_trait]
pub trait RecordFetcher: Send + Sync + 'static {
    /// Fetch the records for `(topic, partition)` from `offset`, at the given
    /// `isolation` level. An empty batch means that nothing new is available.
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<FetchBatch, StreamsClientError>;

    /// The partition indices of `topic`. The global consumer reads all of them
    /// to materialize a fully-replicated global store. Default: the single
    /// partition 0. The broker fetcher overrides the default from the
    /// metadata.
    async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, StreamsClientError> {
        Ok(vec![0])
    }
}

#[async_trait::async_trait]
pub trait RecordProducer: Send + Sync + 'static {
    /// Enqueue a record to `topic`.
    ///
    /// `partition`:
    /// - `None` uses the producer's key-hash partitioner. This is correct for a
    ///   sink topic or a repartition topic.
    /// - `Some(p)` pins the record to partition `p`. A changelog topic needs
    ///   this, so that the task restore can read the record back from the task
    ///   partition.
    async fn send(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError>;
    /// Like `send`, but it sets the record's timestamp in epoch milliseconds
    /// when `timestamp_ms` is `Some`.
    ///
    /// The default delegates to `send`, and the producer then fills in the
    /// wall-clock time. The broker producer overrides this method, so that a
    /// versioned-store changelog record carries the version timestamp
    /// (KIP-889).
    async fn send_with_timestamp(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
        _timestamp_ms: Option<i64>,
    ) -> Result<(), StreamsClientError> {
        self.send(topic, partition, key, value).await
    }
    /// Block until the broker acknowledges every enqueued record. This is the
    /// durability barrier.
    async fn flush(&self) -> Result<(), StreamsClientError>;
}

/// The lazy begin-the-transaction gate passed to the stream task's
/// `process_once` operation under EOS-v2.
///
/// The task calls [`BeginTxnGate::ensure_begun`] exactly before its first
/// produced record in a commit interval. An interval that fetches no records
/// therefore opens no transaction, which avoids empty-txn churn on an idle app.
/// Under at-least-once the caller passes no gate.
#[async_trait::async_trait]
pub trait BeginTxnGate: Send {
    /// Make sure a transaction is open. The first call within the interval
    /// begins one, and every later call does nothing. The task calls this method
    /// right before the first sink or changelog `send`.
    async fn ensure_begun(&mut self) -> Result<(), StreamsClientError>;
}

#[async_trait::async_trait]
pub trait OffsetStore: Send + Sync + 'static {
    /// The committed offset for `(topic, partition)`, or `None` when no commit
    /// happened.
    async fn committed(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>, StreamsClientError>;
    /// The earliest available offset, which matches
    /// `auto.offset.reset = earliest`.
    async fn earliest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError>;
    /// The latest available offset, that is the log-end offset.
    async fn latest(&self, topic: &str, partition: i32) -> Result<i64, StreamsClientError>;
    /// Commit `(topic, partition, offset)` triples for the streams group.
    async fn commit(&self, offsets: &[(String, i32, i64)]) -> Result<(), StreamsClientError>;
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

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
