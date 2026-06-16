//! The [`Sink`] SPI: push records into an external system, with an optional
//! transactional write gate for exactly-once delivery.

use async_trait::async_trait;

use crate::error::ConnectError;
use crate::record::ConnectRecord;

/// A connector that pushes records consumed from Kafka into an external system
/// (a data warehouse, a search index, a telemetry backend).
///
/// This is the write side of the connector SPI and the template every telemetry
/// sink builds on. It mirrors the streams runtime's `RecordProducer`: records
/// are buffered by [`put`](Sink::put) and made durable by [`flush`](Sink::flush).
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
/// only when a non-empty batch is about to be written, so an idle interval opens
/// no transaction. The default implementations are at-least-once: `begin` and
/// `abort` are no-ops and `commit` delegates to `flush`.
#[async_trait]
pub trait Sink<K, V>: Send + Sync + 'static {
    /// Buffer a batch of records for writing. May write through immediately or
    /// accumulate until [`flush`](Sink::flush); either way the records are not
    /// guaranteed durable until `flush` (or [`commit`](Sink::commit)) returns.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the records cannot be accepted.
    async fn put(&mut self, records: Vec<ConnectRecord<K, V>>) -> Result<(), ConnectError>;

    /// Make every record accepted since the last flush/commit durable. The
    /// runtime calls this before committing consumer offsets, so a successful
    /// `flush` is the guarantee those records will not be re-delivered.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the buffered records cannot be made durable.
    async fn flush(&mut self) -> Result<(), ConnectError>;

    /// Whether this sink writes atomically and so can take part in exactly-once
    /// delivery. When `false` (the default) the runtime drives it at-least-once
    /// and never calls [`begin`](Sink::begin) / [`abort`](Sink::abort).
    fn supports_transactions(&self) -> bool {
        false
    }

    /// Open a transaction enclosing the [`put`](Sink::put)s that follow, up to
    /// the next [`commit`](Sink::commit) or [`abort`](Sink::abort). Called by
    /// the runtime only when [`supports_transactions`](Sink::supports_transactions)
    /// is `true` and only before a non-empty batch. Default: no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Transaction`] if a transaction cannot be opened.
    async fn begin(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Atomically commit the records written since [`begin`](Sink::begin),
    /// making them durable. The default (no transaction support) delegates to
    /// [`flush`](Sink::flush).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the transaction cannot be committed.
    async fn commit(&mut self) -> Result<(), ConnectError> {
        self.flush().await
    }

    /// Discard the records written since [`begin`](Sink::begin) without making
    /// them durable. Called by the runtime to roll back a failed interval.
    /// Default: no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Transaction`] if the transaction cannot be
    /// aborted.
    async fn abort(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Release any resources held by the sink (connections, buffers). The
    /// default is a no-op. After `close`, no further records are written.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if cleanup fails.
    async fn close(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }
}
