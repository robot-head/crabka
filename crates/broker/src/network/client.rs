//! Outbound inter-broker client. Establishes TCP, optionally wraps in TLS,
//! optionally runs SASL client handshake. Returns a generic `AsyncRead +
//! `AsyncWrite` stream the caller uses for normal RPCs.
//!
//! Used by the replicator's Fetch path, the raft transport's
//! outbound dial, and the controller-heartbeat loop.

use std::sync::Arc;

use crabka_security::ListenerProtocol;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tokio_rustls::TlsConnector;

use crate::config::InterBrokerCredentials;

/// Map the broker's [`InterBrokerCredentials`] onto the client-core
/// [`crabka_client_core::SaslCredentials`] understood by the shared
/// [`crabka_client_core::outbound_sasl`] handshake. The two enums carry
/// the same variants; this is a field-for-field copy. Shared with the
/// RLMM bootstrap so the dialer and the metadata client agree on the
/// mapping.
pub(crate) fn to_client_creds(c: &InterBrokerCredentials) -> crabka_client_core::SaslCredentials {
    match c {
        InterBrokerCredentials::Plain { username, password } => {
            crabka_client_core::SaslCredentials::Plain {
                username: username.clone(),
                password: password.clone(),
            }
        }
        InterBrokerCredentials::Scram {
            mechanism,
            username,
            password,
        } => crabka_client_core::SaslCredentials::Scram {
            mechanism: *mechanism,
            username: username.clone(),
            password: password.clone(),
        },
        InterBrokerCredentials::Gssapi {
            keytab_path,
            client_principal,
            service_name,
            kdc_url,
        } => crabka_client_core::SaslCredentials::Gssapi {
            keytab_path: keytab_path.clone(),
            client_principal: client_principal.clone(),
            service_name: service_name.clone(),
            kdc_url: kdc_url.clone(),
        },
    }
}

#[derive(Debug, Error)]
pub enum InterBrokerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls: {0}")]
    Tls(String),
    #[error("sasl: {0}")]
    Sasl(String),
    #[error("config: {0}")]
    Config(String),
    #[error("codec: {0}")]
    Codec(String),
}

/// Trait alias for boxed duplex streams. Both `TcpStream` and
/// `tokio_rustls::client::TlsStream<TcpStream>` satisfy it.
///
/// Same shape as `crabka_client_core::ClientDuplex` so the stream
/// returned by `InterBrokerClient::connect` can be handed directly to
/// `Connection::from_stream`.
pub trait DuplexStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> DuplexStream for T {}

/// Constructs outbound connections to other brokers, running TLS and SASL
/// as the listener protocol demands. Cheap to clone-from / share — holds
/// just a `TlsConnector` (an `Arc` under the hood) and credentials.
pub struct InterBrokerClient {
    tls_connector: Option<TlsConnector>,
    creds: Option<InterBrokerCredentials>,
}

impl InterBrokerClient {
    #[must_use]
    pub fn new(tls_connector: Option<TlsConnector>, creds: Option<InterBrokerCredentials>) -> Self {
        Self {
            tls_connector,
            creds,
        }
    }

    /// Dial `host:port`, perform the protocol-appropriate handshakes
    /// (TLS, SASL), and return an authenticated duplex stream. Callers
    /// drive normal Kafka RPCs (Fetch, Vote, `AppendEntries`, …) through
    /// the returned stream just as if it were a fresh `TcpStream`.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn connect(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        options: &crabka_client_core::ConnectionOptions,
    ) -> Result<Box<dyn DuplexStream>, InterBrokerError> {
        let tcp = TcpStream::connect((host, port)).await?;
        let mut stream: Box<dyn DuplexStream> = if listener_protocol.requires_tls() {
            let connector = self.tls_connector.clone().ok_or_else(|| {
                InterBrokerError::Config("TLS listener without TlsConnector".into())
            })?;
            let sni =
                tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
                    .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
            let tls = connector
                .connect(sni, tcp)
                .await
                .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
            Box::new(tls)
        } else {
            Box::new(tcp)
        };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            crabka_client_core::outbound_sasl(
                &mut *stream,
                &to_client_creds(&creds),
                server_name,
                &options.client_id,
                options.frame_max,
            )
            .await
            .map_err(|e| InterBrokerError::Sasl(e.to_string()))?;
        }
        Ok(stream)
    }

    /// Dial `host:port` (running TLS + SASL as needed) and return a
    /// [`crabka_client_core::Connection`] over the resulting stream. The
    /// connection is fully usable for normal typed Kafka requests —
    /// `Fetch`, `OffsetForLeaderEpoch`, `BrokerHeartbeat`, raft RPCs via
    /// `raw_request`, etc.
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub async fn connect_as_connection(
        &self,
        host: &str,
        port: u16,
        listener_protocol: ListenerProtocol,
        server_name: &str,
        options: crabka_client_core::ConnectionOptions,
    ) -> Result<crabka_client_core::Connection, InterBrokerError> {
        // Build the auth'd stream directly into a `Box<dyn ClientDuplex>`
        // (rather than `Box<dyn DuplexStream>`) so it lines up with
        // `Connection::from_stream` without an unsizing coercion that
        // Rust can't do between two equivalent-but-distinct trait
        // objects.
        let tcp = TcpStream::connect((host, port)).await?;
        let mut stream: Box<dyn crabka_client_core::ClientDuplex> =
            if listener_protocol.requires_tls() {
                let connector = self.tls_connector.clone().ok_or_else(|| {
                    InterBrokerError::Config("TLS listener without TlsConnector".into())
                })?;
                let sni =
                    tokio_rustls::rustls::pki_types::ServerName::try_from(server_name.to_string())
                        .map_err(|e| InterBrokerError::Tls(format!("invalid server name: {e}")))?;
                let tls = connector
                    .connect(sni, tcp)
                    .await
                    .map_err(|e| InterBrokerError::Tls(e.to_string()))?;
                Box::new(tls)
            } else {
                Box::new(tcp)
            };
        if listener_protocol.requires_sasl() {
            let creds = self.creds.clone().ok_or_else(|| {
                InterBrokerError::Config("SASL listener without inter_broker_credentials".into())
            })?;
            crabka_client_core::outbound_sasl(
                &mut *stream,
                &to_client_creds(&creds),
                server_name,
                &options.client_id,
                options.frame_max,
            )
            .await
            .map_err(|e| InterBrokerError::Sasl(e.to_string()))?;
        }
        crabka_client_core::Connection::from_stream(stream, options)
            .await
            .map_err(|e| InterBrokerError::Config(format!("Connection::from_stream: {e}")))
    }
}

// ────────────────────────────────────────────────────────────────────────
// OutboundDialer adapter for crabka_raft::CrabkaRaftNetworkFactory.
// ────────────────────────────────────────────────────────────────────────

/// Adapter that lets `crabka_raft` reach the broker's
/// [`InterBrokerClient`] without taking a build dependency on the
/// broker crate. Wraps an `Arc<InterBrokerClient>` plus the protocol /
/// SNI configuration once; the raft network factory clones it cheaply.
pub struct InterBrokerDialer {
    client: Arc<InterBrokerClient>,
    listener_protocol: ListenerProtocol,
    server_name: String,
}

impl InterBrokerDialer {
    #[must_use]
    pub fn new(
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
        server_name: String,
    ) -> Self {
        Self {
            client,
            listener_protocol,
            server_name,
        }
    }
}

#[async_trait::async_trait]
impl crabka_raft::OutboundDialer for InterBrokerDialer {
    async fn dial(
        &self,
        _target: crabka_raft::NodeId,
        addr: &str,
        options: crabka_client_core::ConnectionOptions,
    ) -> Result<crabka_client_core::Connection, crabka_client_core::ClientError> {
        // The raft transport hands us an address in `host:port` form
        // (the openraft `Node.addr` string). For SocketAddr-style
        // addresses we honour the configured `server_name` for SNI
        // separately from the literal host string.
        let (host, port) = match addr.rsplit_once(':') {
            Some((h, p)) => {
                let port: u16 = p.parse().map_err(|e: std::num::ParseIntError| {
                    crabka_client_core::ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid raft peer port in {addr:?}: {e}"),
                    ))
                })?;
                (h.to_string(), port)
            }
            None => {
                return Err(crabka_client_core::ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("raft peer address missing port: {addr:?}"),
                )));
            }
        };
        self.client
            .connect_as_connection(
                &host,
                port,
                self.listener_protocol,
                &self.server_name,
                options,
            )
            .await
            .map_err(|e| match e {
                InterBrokerError::Io(io) => crabka_client_core::ClientError::Io(io),
                other => crabka_client_core::ClientError::Io(std::io::Error::other(format!(
                    "InterBrokerClient dial: {other}"
                ))),
            })
    }
}
