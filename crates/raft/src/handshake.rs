//! Pluggable inbound handshake for the controller listener.
//!
//! This hook lets the broker terminate TLS and SASL on every accepted
//! controller-listener connection before the raft frames start to flow. The
//! trait abstraction keeps `crabka-raft` free of any dependency on
//! `crabka-broker` and `crabka-security`.

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

/// Type-erased duplex stream returned by [`RaftListenerHandshake::upgrade`].
///
/// The raft connection handler is generic over `AsyncRead + AsyncWrite +
/// Unpin + Send + 'static`, so a `Box<dyn DuplexStream>` plugs in directly.
pub trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> DuplexStream for T {}

/// Authenticated controller-listener connection plus request-level grants.
pub struct RaftConnection {
    pub stream: Box<dyn DuplexStream>,
    /// Whether the principal may alter cluster membership.
    pub cluster_alter_authorized: bool,
}

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

/// Per-connection handshake hook.
///
/// Implementors consume the raw `TcpStream` and return one of two things. On
/// success they return an authenticated `Box<dyn DuplexStream>` that carries
/// the raft frames. On failure they return a `RaftHandshakeError`, and the
/// listener then drops the connection at debug level.
#[async_trait::async_trait]
pub trait RaftListenerHandshake: Send + Sync {
    async fn upgrade(&self, stream: TcpStream) -> Result<RaftConnection, RaftHandshakeError>;
}
