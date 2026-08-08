//! The [`Sink`] SPI: push records into an external system, with an optional
//! transactional write gate for exactly-once delivery.

use async_trait::async_trait;

use crate::{error::ConnectError, record::ConnectRecord};

/// A connector that pushes records consumed from Kafka into an external system,
/// for example a data warehouse, a search index, or a telemetry backend.
///
/// This is the write side of the connector SPI and the template every telemetry
/// sink builds on. It mirrors the streams runtime's `RecordProducer`:
/// [`put`](Sink::put) buffers records and [`flush`](Sink::flush) makes them
/// durable.
///
/// ## Delivery
///
/// The runtime hands batches to [`put`](Sink::put) and periodically calls
/// [`flush`](Sink::flush) to establish a durability barrier before committing
/// the corresponding consumer offsets. A sink that buffers internally must make
/// every buffered record durable by the time `flush` returns.
///
/// ## Transactional write gate
///
/// A sink that can write atomically opts into exactly-once delivery by setting
/// [`supports_transactions`](Sink::supports_transactions) and implementing
/// [`begin`](Sink::begin) / [`commit`](Sink::commit) / [`abort`](Sink::abort).
/// The runtime then brackets each commit interval in a transaction:
///
/// ```text
/// begin → put* → commit   (or abort on failure)
/// ```
///
/// Like the streams `BeginTxnGate`, the gate is lazy: the runtime calls `begin`
/// only just before it writes a non-empty batch, so an idle interval opens no
/// transaction. The default implementations are at-least-once: `begin` and
/// `abort` are no-ops and `commit` delegates to `flush`.
#[async_trait]
pub trait Sink<K, V>: Send + Sync + 'static {
    /// Buffer a batch of records for writing. The sink may write through
    /// immediately or accumulate until [`flush`](Sink::flush). In both cases the
    /// records are not guaranteed durable until `flush` or
    /// [`commit`](Sink::commit) returns.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the records cannot be accepted.
    async fn put(&mut self, records: Vec<ConnectRecord<K, V>>) -> Result<(), ConnectError>;

    /// Make every record accepted since the last flush or commit durable. The
    /// runtime calls this before it commits consumer offsets, so a successful
    /// `flush` is the guarantee that those records will not be re-delivered.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the buffered records cannot be made durable.
    async fn flush(&mut self) -> Result<(), ConnectError>;

    /// Whether this sink writes atomically and so can take part in exactly-once
    /// delivery. When `false`, the default, the runtime drives it at-least-once
    /// and never calls [`begin`](Sink::begin) or [`abort`](Sink::abort).
    fn supports_transactions(&self) -> bool {
        false
    }

    /// Open a transaction that encloses the [`put`](Sink::put)s that follow, up
    /// to the next [`commit`](Sink::commit) or [`abort`](Sink::abort). The
    /// runtime calls this only when
    /// [`supports_transactions`](Sink::supports_transactions) is `true`, and
    /// only before a non-empty batch. Default: no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Transaction`] if a transaction cannot be opened.
    async fn begin(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Atomically commit the records written since [`begin`](Sink::begin) and
    /// make them durable. The default has no transaction support and delegates
    /// to [`flush`](Sink::flush).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the transaction cannot be committed.
    async fn commit(&mut self) -> Result<(), ConnectError> {
        self.flush().await
    }

    /// Discard the records written since [`begin`](Sink::begin) and do not make
    /// them durable. The runtime calls this to roll back a failed interval.
    /// Default: no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Transaction`] if the transaction cannot be
    /// aborted.
    async fn abort(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Release any resources that the sink holds, such as connections and
    /// buffers. The default is a no-op. After `close`, the sink writes no
    /// further records.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if cleanup fails.
    async fn close(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use async_trait::async_trait;
    use bytes::Bytes;

    use super::*;

    /// A sink that takes every default: at-least-once, and no transactions.
    #[derive(Default)]
    struct DefaultSink {
        durable: usize,
        buffered: usize,
    }

    #[async_trait]
    impl Sink<Bytes, Bytes> for DefaultSink {
        async fn put(
            &mut self,
            records: Vec<ConnectRecord<Bytes, Bytes>>,
        ) -> Result<(), ConnectError> {
            self.buffered += records.len();
            Ok(())
        }

        async fn flush(&mut self) -> Result<(), ConnectError> {
            self.durable += self.buffered;
            self.buffered = 0;
            Ok(())
        }
    }

    #[test]
    fn default_sink_is_not_transactional() {
        // A sink that doesn't opt in must report `false`, so the runtime drives
        // it at-least-once and never calls begin/abort.
        check!(!DefaultSink::default().supports_transactions());
    }

    #[tokio::test]
    async fn default_commit_makes_buffered_records_durable() {
        // The default `commit` delegates to `flush`.
        let mut sink = DefaultSink::default();
        sink.put(vec![ConnectRecord::new(
            None,
            Some(Bytes::from_static(b"x")),
        )])
        .await
        .unwrap();
        sink.commit().await.unwrap();
        check!((sink.durable, sink.buffered) == (1, 0));
    }

    #[tokio::test]
    async fn default_begin_abort_and_close_are_noops() {
        let mut sink = DefaultSink::default();
        check!(sink.begin().await.is_ok());
        check!(sink.abort().await.is_ok());
        check!(sink.close().await.is_ok());
    }
}
