//! The [`Source`] SPI: pull records out of an external system, with a
//! checkpointable read position.

use async_trait::async_trait;

use crate::{
    error::ConnectError,
    record::{ConnectRecord, SourceOffset},
};

/// A connector that pulls records out of an external system (a database change
/// stream, a file tail, a queue) for production into Kafka.
///
/// This is the read side of the connector SPI and the template every CDC source
/// builds on. It mirrors the streams runtime's `RecordFetcher`, but pulls one
/// record at a time and owns its own read position rather than being told an
/// offset on every call.
///
/// ## Polling
///
/// [`poll`](Source::poll) returns the next record, or `None` when the source is
/// momentarily caught up (the runtime should back off and poll again). Each
/// successful `poll` advances the source's internal position; the runtime never
/// passes an offset in — it reads the position back with
/// [`checkpoint`](Source::checkpoint).
///
/// ## Offset state
///
/// The runtime persists the [`SourceOffset`] returned by `checkpoint` after the
/// records it covers have been durably produced, and restores it with
/// [`seek`](Source::seek) before the first `poll` on restart. The offset is
/// opaque to the runtime — only the source interprets it — so a source is free
/// to encode a log sequence number, a byte offset, a GTID set, or whatever
/// resume token its backend uses.
///
/// After the sink commit is durable and checkpoint persistence succeeds, the
/// runtime calls [`acknowledge`](Source::acknowledge) with the persisted offset.
/// Sources that hold backend resources such as non-advancing cursors or logical
/// replication slots can use this hook to release data that is now safe to
/// discard. The default implementation is a no-op for sources that need no
/// explicit acknowledgement.
#[async_trait]
pub trait Source<K, V>: Send + Sync + 'static {
    /// Pull the next record, advancing the source's read position.
    ///
    /// Returns `Ok(None)` when nothing is available yet — a non-fatal "caught
    /// up" signal, not end-of-stream. Returns `Err` only on a real failure
    /// (lost connection, malformed upstream data the source cannot skip).
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if reading from the external system fails.
    async fn poll(&mut self) -> Result<Option<ConnectRecord<K, V>>, ConnectError>;

    /// Snapshot the current read position so the runtime can persist it.
    ///
    /// Returns `None` before the source has a position to commit (e.g. nothing
    /// has been polled and no prior offset was restored). The runtime commits a
    /// checkpoint only after the records preceding it are durable, so on restart
    /// [`seek`](Source::seek) resumes from the last fully-produced record.
    fn checkpoint(&self) -> Option<SourceOffset>;

    /// Restore the read position to a previously [`checkpoint`](Source::checkpoint)ed
    /// offset. Called once, before the first [`poll`](Source::poll), on startup
    /// and after a rebalance. A source with no stored offset is not sought and
    /// starts from its configured default position.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Offset`] if `offset` does not name a position
    /// this source can resume from (e.g. the upstream log has since been
    /// truncated past it).
    async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError>;

    /// Acknowledge that `offset` is durable end-to-end.
    ///
    /// The runtime calls this only after a non-empty batch has been committed
    /// to the sink and the same offset has been saved in the checkpoint store.
    /// It is never called before checkpoint persistence, so a failing
    /// checkpoint save prevents upstream acknowledgement. Sources may use this
    /// to advance external cursors or release retained log segments.
    ///
    /// The default is a no-op, preserving existing source implementations.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the upstream acknowledgement fails.
    async fn acknowledge(&mut self, _offset: &SourceOffset) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Release any resources held by the source (connections, file handles).
    /// The default is a no-op. After `close`, the source is not polled again.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if cleanup fails.
    async fn close(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }
}
