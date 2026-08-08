//! The [`Source`] SPI: pull records out of an external system, with a
//! checkpointable read position.

use async_trait::async_trait;

use crate::{
    error::ConnectError,
    record::{ConnectRecord, SourceOffset},
};

/// A connector that pulls records out of an external system for production into
/// Kafka, for example a database change stream, a file tail, or a queue.
///
/// This is the read side of the connector SPI and the template every CDC source
/// builds on. It mirrors the streams runtime's `RecordFetcher`, but it pulls one
/// record at a time and owns its own read position. The runtime does not tell it
/// an offset on every call.
///
/// ## Polling
///
/// [`poll`](Source::poll) returns the next record, or `None` when the source is
/// momentarily caught up. The runtime should back off and poll again. Each
/// successful `poll` advances the internal position of the source. The runtime
/// never passes an offset in. It reads the position back with
/// [`checkpoint`](Source::checkpoint).
///
/// ## Offset state
///
/// The runtime persists the [`SourceOffset`] returned by `checkpoint` after the
/// records it covers have been durably produced, and restores it with
/// [`seek`](Source::seek) before the first `poll` on restart. The offset is
/// opaque to the runtime, because only the source interprets it. A source can
/// therefore encode a log sequence number, a byte offset, a GTID set, or
/// whatever resume token its backend uses.
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
    /// Returns `Ok(None)` when nothing is available yet. That is a non-fatal
    /// "caught up" signal and not end-of-stream. Returns `Err` only on a real
    /// failure, such as a lost connection or malformed upstream data that the
    /// source cannot skip.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if reading from the external system fails.
    async fn poll(&mut self) -> Result<Option<ConnectRecord<K, V>>, ConnectError>;

    /// Snapshot the current read position so the runtime can persist it.
    ///
    /// Returns `None` before the source has a position to commit, for example
    /// when nothing has been polled and no prior offset was restored. The
    /// runtime commits a
    /// checkpoint only after the records preceding it are durable, so on restart
    /// [`seek`](Source::seek) resumes from the last fully-produced record.
    fn checkpoint(&self) -> Option<SourceOffset>;

    /// Restore the read position to a previously [`checkpoint`](Source::checkpoint)ed
    /// offset. The runtime calls this once before the first
    /// [`poll`](Source::poll), on startup and after a rebalance. The runtime
    /// does not seek a source that has no stored offset, and that source starts
    /// from its configured default position.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Offset`] if `offset` does not name a position
    /// this source can resume from, for example when the upstream log has since
    /// been truncated past it.
    async fn seek(&mut self, offset: SourceOffset) -> Result<(), ConnectError>;

    /// Acknowledge that `offset` is durable end-to-end.
    ///
    /// The runtime calls this only after it has committed a non-empty batch to
    /// the sink and saved the same offset in the checkpoint store. It never
    /// calls this before checkpoint persistence, so a failed checkpoint save
    /// prevents upstream acknowledgement. A source may use this to advance
    /// external cursors or release retained log segments.
    ///
    /// The default is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if the upstream acknowledgement fails.
    async fn acknowledge(&mut self, _offset: &SourceOffset) -> Result<(), ConnectError> {
        Ok(())
    }

    /// Release any resources that the source holds, such as connections and
    /// file handles. The default is a no-op. After `close`, the runtime does
    /// not poll the source again.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError`] if cleanup fails.
    async fn close(&mut self) -> Result<(), ConnectError> {
        Ok(())
    }
}
