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

/// Fetch isolation level (Kafka `Fetch.isolation_level`).
///
/// Under EOS-v2, changelog restore must read `ReadCommitted` so that aborted
/// writes (records that were produced inside a transaction that later aborted)
/// are excluded — the restored store reflects only committed state. Normal
/// source processing and global-store bootstrap use `ReadUncommitted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    #[default]
    ReadUncommitted,
    ReadCommitted,
}

#[async_trait::async_trait]
pub trait RecordFetcher: Send + Sync + 'static {
    /// Fetch records for `(topic, partition)` starting at `offset`, at the given
    /// `isolation` level. An empty batch means nothing new yet.
    async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation: IsolationLevel,
    ) -> Result<FetchBatch, StreamsClientError>;

    /// The partition indices of `topic`. The global consumer reads all of them to
    /// materialize a fully-replicated global store. Default: a single partition 0
    /// (overridden by the broker fetcher via metadata).
    async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, StreamsClientError> {
        Ok(vec![0])
    }
}

#[async_trait::async_trait]
pub trait RecordProducer: Send + Sync + 'static {
    /// Enqueue a record to `topic`.
    ///
    /// `partition`:
    /// - `None`  → use the producer's key-hash partitioner (correct for sink /
    ///   repartition topics).
    /// - `Some(p)` → pin to partition `p` (required for changelog topics so task
    ///   restore can read back the record from the task partition).
    async fn send(
        &self,
        topic: &str,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Option<Bytes>,
    ) -> Result<(), StreamsClientError>;
    /// Like `send`, but sets the record's timestamp (epoch millis) when
    /// `timestamp_ms` is `Some`. Default delegates to `send` (producer fills
    /// wall-clock time). Overridden by the broker producer so versioned-store
    /// changelog records carry the version timestamp (KIP-889).
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
    /// Block until all enqueued records are acknowledged (durability barrier).
    async fn flush(&self) -> Result<(), StreamsClientError>;
}

/// Lazy "begin the transaction" gate handed to [`StreamTask::process_once`]
/// under EOS-v2. The task invokes [`BeginTxnGate::ensure_begun`] exactly before
/// its first produced record in a commit interval, so an interval that fetches
/// no records opens no transaction (no empty-txn churn on an idle app). Under
/// at-least-once no gate is passed.
#[async_trait::async_trait]
pub trait BeginTxnGate: Send {
    /// Ensure a transaction is open, beginning one on the first call within the
    /// interval and a no-op on subsequent calls. Called by the task right before
    /// the first sink/changelog `send`.
    async fn ensure_begun(&mut self) -> Result<(), StreamsClientError>;
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
    /// The latest available offset (log-end offset).
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
