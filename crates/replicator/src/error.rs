//! Replicator error type.

#[derive(Debug, thiserror::Error)]
pub enum ReplicatorError {
    #[error("config error: {0}")]
    Config(String),
    #[error("connect error: {0}")]
    Connect(#[from] crabka_connect::ConnectError),
    #[error("client error: {0}")]
    Client(String),
    #[error("MM2 codec error: {0}")]
    Codec(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ReplicatorError>;
