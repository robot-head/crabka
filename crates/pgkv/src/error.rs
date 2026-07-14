//! Errors from the storage layer.

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KvError {
    #[error("corrupt row encoding: {0}")]
    CorruptRow(String),
    #[error("storage I/O error: {0}")]
    Io(String),
    #[error("restore target is not empty")]
    RestoreTargetNotEmpty,
    #[error("snapshot keys are not strictly ascending")]
    UnsortedSnapshot,
    #[error("conditional writes are unavailable on the fjall backend")]
    ConditionalPutUnsupported,
}
