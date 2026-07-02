//! Pluggable inbound handshake for the controller listener.
//!
//! Lets the broker terminate TLS + SASL on every accepted
//! controller-listener connection before raft frames start flowing. The
//! trait abstraction keeps `crabka-raft` free of any dependency on
//! `crabka-broker` / `crabka-security`.

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

/// Type-erased duplex stream returned by [`RaftListenerHandshake::upgrade`].
/// The raft connection handler is generic over `AsyncRead + AsyncWrite +
/// Unpin + Send + 'static`, so a `Box<dyn DuplexStream>` plugs in directly.
pub trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> DuplexStream for T {}

#[derive(Debug, Error)]
pub enum RaftHandshakeError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("protocol: {0}")]
    Protocol(String),
}

/// Per-connection handshake hook. Implementations consume the raw
/// `TcpStream` and return either an authenticated `Box<dyn DuplexStream>`
/// (for raft frames to ride on) or a `RaftHandshakeError` (the listener
/// drops the connection at debug level).
#[async_trait::async_trait]
pub trait RaftListenerHandshake: Send + Sync {
    async fn upgrade(&self, stream: TcpStream)
    -> Result<Box<dyn DuplexStream>, RaftHandshakeError>;
}
