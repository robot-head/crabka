//! Framed range-compute transport for SQL forwarding and transaction RPC.

#[cfg(test)]
use std::net::SocketAddr;
use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::RangeId;

const MAX_FRAME_BYTES: usize = 1 << 20;
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// Request sent between range computes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(
    clippy::large_enum_variant,
    reason = "range RPC keeps request shapes value-typed"
)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RangeRequest {
    /// Forward one SQL statement to its owning range.
    Sql { range_id: RangeId, sql: String },
    /// Ask an owning range to scan a table rowid interval under caller snapshots.
    ScanRange(ScanRangeReq),
    /// Run one transaction-coordinator RPC.
    Txn(TxnReq),
    /// Run one timestamp-oracle RPC against range 0.
    Tso(TsoReq),
    /// Resolve a timestamp transaction through its primary range.
    ResolveTxn(ResolveTxnReq),
}

/// Response sent between range computes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum RangeResponse {
    /// SQL statement completed and returned a command tag/summary.
    Sql { result: String },
    /// Complete simple-query results, including row descriptions and encoded cells.
    SqlResults { results: Vec<WireQueryResult> },
    /// SQL execution failed with a `PostgreSQL` error preserved from the owner.
    SqlError { code: String, message: String },
    /// Visible rows returned by a range scan.
    ScanRange(ScanRangeResp),
    /// Range-scan execution failed with the owner's `PostgreSQL` error code.
    ScanRangeError { code: String, message: String },
    /// Transaction RPC response.
    Txn(TxnResp),
    /// Timestamp-oracle RPC response.
    Tso(TsoResp),
    /// Primary-range timestamp transaction resolution response.
    ResolveTxn(ResolveTxnResp),
    /// Range compute rejected the request.
    Error {
        error: WireErrorKind,
        message: String,
    },
}

/// Serializable simple-query result returned by a range owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireQueryResult {
    Rows {
        fields: Vec<WireFieldDescription>,
        rows: Vec<Vec<Option<WireCell>>>,
        tag: String,
    },
    Command {
        tag: String,
    },
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireCell {
    pub text: Vec<u8>,
    pub binary: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireFieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_id: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

/// Timestamp-oracle RPC sent to range 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TsoReq {
    /// Grant `count` contiguous transaction timestamps.
    Grant { count: u64 },
}

/// Timestamp-oracle RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TsoResp {
    /// Contiguous timestamp lease granted by range 0.
    Granted { first_ts: u64, count: u64 },
}

/// Primary-range timestamp transaction resolution request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveTxnReq {
    pub primary_range: RangeId,
    pub start_ts: u64,
}

/// Primary-range timestamp transaction resolution response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ResolveTxnResp {
    /// The primary has durably committed the transaction at `commit_ts`.
    Committed { commit_ts: u64 },
    /// The primary has durably aborted the transaction.
    Aborted,
    /// The primary has no terminal decision yet; the reader must exclude the
    /// intent or retry/push-abort via the caller's bounded-wait policy.
    Pending,
}

/// Serializable MVCC snapshot used by range-scan RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireSnapshot {
    pub xmin: u64,
    pub xmax: u64,
    pub xip: Vec<u64>,
}

impl From<&crabka_pgmvcc::visibility::Snapshot> for WireSnapshot {
    fn from(value: &crabka_pgmvcc::visibility::Snapshot) -> Self {
        Self {
            xmin: value.xmin,
            xmax: value.xmax,
            xip: value.xip.clone(),
        }
    }
}

impl From<WireSnapshot> for crabka_pgmvcc::visibility::Snapshot {
    fn from(value: WireSnapshot) -> Self {
        Self {
            xmin: value.xmin,
            xmax: value.xmax,
            xip: value.xip,
        }
    }
}

/// Serializable rowid interval for range-scan RPCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireRowInterval {
    pub start: Option<u64>,
    pub end: Option<u64>,
}

/// Serializable predicate pushdown for range-scan RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WirePredicatePushdown {
    FullScan,
    Conjunctive {
        predicates: Vec<WireColumnPredicate>,
    },
}

/// Serializable column/literal predicate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireColumnPredicate {
    pub column: usize,
    pub op: WirePredicateOp,
    pub value: WireDatum,
}

/// Serializable predicate operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WirePredicateOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// Serializable literal subset used by predicate pushdown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "value")]
pub enum WireDatum {
    Bool(bool),
    Int4(i32),
    Int8(i64),
    Text(String),
}

/// Serializable projection pushdown for range-scan RPCs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WireProjectionPushdown {
    All,
    Columns { columns: Vec<usize> },
}

/// Serializable partial aggregate request shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WirePartialAggregateSpec {
    pub function: WirePartialAggregateFunction,
    pub column: Option<usize>,
}

/// Serializable partial aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WirePartialAggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    AvgParts,
}

/// Serializable top-K request shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireTopKSpec {
    pub order_by: Vec<WireTopKColumn>,
    pub limit: u64,
}

/// Serializable top-K ordering key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireTopKColumn {
    pub column: usize,
    pub asc: bool,
}

/// Range-scan request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRangeReq {
    pub range_id: RangeId,
    pub table_name: String,
    pub interval: WireRowInterval,
    pub local_snapshot: WireSnapshot,
    pub global_snapshot: WireSnapshot,
    pub own_xid: Option<u64>,
    pub read_ts: Option<u64>,
    pub predicate: WirePredicatePushdown,
    pub projection: WireProjectionPushdown,
    pub partial_aggregate: Option<WirePartialAggregateSpec>,
    pub top_k: Option<WireTopKSpec>,
}

/// One encoded visible tuple returned by [`ScanRangeReq`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRangeRow {
    pub rowid: u64,
    pub xmin: u64,
    /// Tuple payload encoded with `crabka_pgmvcc::version::encode_tuple`.
    pub tuple: Vec<u8>,
}

/// Range-scan response. Rows are sorted by rowid by the owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanRangeResp {
    pub rows: Vec<ScanRangeRow>,
}

/// Transaction RPC sent over [`RangeRequest::Txn`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TxnReq {
    /// Ask a participant to prepare a global transaction.
    Prepare { gtid: u64, range_id: RangeId },
    /// Ask a participant to commit a prepared transaction.
    Commit { gtid: u64, range_id: RangeId },
    /// Ask a participant to abort a prepared transaction.
    Abort { gtid: u64, range_id: RangeId },
    /// Ask range 0 for a substrate durability barrier.
    Barrier { range_id: RangeId },
}

/// Transaction RPC response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TxnResp {
    /// Participant prepared successfully.
    Prepared,
    /// Participant committed successfully.
    Committed,
    /// Participant aborted or refused prepare.
    Aborted,
    /// Range-0 substrate offset covered by the barrier.
    Barrier { substrate_offset: i64 },
}

/// Retry-visible error class returned by remote range computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireErrorKind {
    /// The endpoint no longer owns the requested range.
    StaleEndpoint,
    /// The endpoint is not currently the range writer.
    NotLeader,
    /// Non-retryable transaction abort.
    Aborted,
    /// Non-retryable protocol/application failure.
    Failed,
}

impl WireErrorKind {
    /// Return whether the forwarding layer may re-resolve and retry exactly once.
    #[must_use]
    pub const fn permits_reresolve(self) -> bool {
        matches!(self, Self::StaleEndpoint | Self::NotLeader)
    }
}

/// Transport-level failure.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Frame exceeded the protocol limit.
    #[error("range frame too large: {actual} bytes exceeds {limit}")]
    FrameTooLarge { actual: usize, limit: usize },
    /// JSON payload was invalid.
    #[error("range frame json error: {0}")]
    Json(#[from] serde_json::Error),
    /// Socket IO failed.
    #[error("range transport io error: {0}")]
    Io(#[from] std::io::Error),
    /// The peer was silent past the configured deadline.
    #[error("range transport timed out after {0:?}")]
    Timeout(Duration),
    /// The remote endpoint returned an application error.
    #[error("range endpoint returned {kind:?}: {message}")]
    Remote {
        kind: WireErrorKind,
        message: String,
    },
    /// The peer returned the wrong response variant.
    #[error("range endpoint returned an unexpected response")]
    UnexpectedResponse,
    /// TLS setup or handshake failed.
    #[error("range transport tls error: {0}")]
    Tls(String),
    /// The authenticated peer is not authorized for this tenant.
    #[error("range transport peer is not authorized for tenant {tenant}")]
    UnauthorizedPeer { tenant: String },
}

/// Required mTLS configuration for outbound range forwarding.
#[derive(Debug, Clone)]
pub struct RangeTlsClientConfig {
    /// TLS identity and trust roots. A client identity and trust roots are mandatory.
    pub tls: crabka_security::TlsConfig,
    /// DNS name verified against the remote server certificate and sent as SNI.
    pub server_name: String,
}

impl RangeTlsClientConfig {
    /// Build a client configuration that cannot use plaintext or anonymous TLS.
    pub fn build_connector(&self) -> Result<TlsConnector, TransportError> {
        if self.tls.trust_roots_path.is_none() {
            return Err(TransportError::Tls(
                "range TLS requires a server trust CA".to_string(),
            ));
        }
        if self.server_name.trim().is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires a non-empty server name".to_string(),
            ));
        }
        self.tls
            .build_client_config_with_identity()
            .map(TlsConnector::from)
            .map_err(|error| TransportError::Tls(error.to_string()))
    }
}

/// Required mTLS and tenant authorization configuration for a range listener.
#[derive(Debug, Clone)]
pub struct RangeTlsServerConfig {
    /// Immutable tenant served by this listener.
    pub tenant: String,
    /// TLS server identity, client CA, and required client authentication.
    pub tls: crabka_security::TlsConfig,
    /// Subject DNs allowed to execute RPCs for `tenant`.
    pub allowed_principals: BTreeSet<String>,
}

impl RangeTlsServerConfig {
    /// Parse and validate the listener security boundary before binding a socket.
    pub fn build_acceptor(&self) -> Result<TlsAcceptor, TransportError> {
        if self.tenant.trim().is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires a tenant".to_string(),
            ));
        }
        if self.tls.client_auth != crabka_security::ClientAuthMode::Required {
            return Err(TransportError::Tls(
                "range TLS requires client authentication".to_string(),
            ));
        }
        if self.tls.client_ca_path.is_none() {
            return Err(TransportError::Tls(
                "range TLS requires a client CA".to_string(),
            ));
        }
        if self.allowed_principals.is_empty() {
            return Err(TransportError::Tls(
                "range TLS requires at least one tenant-authorized principal".to_string(),
            ));
        }
        self.tls
            .build_server_config()
            .map(TlsAcceptor::from)
            .map_err(|error| TransportError::Tls(error.to_string()))
    }
}

/// Trait implemented by local range-compute request handlers.
#[async_trait]
pub trait RangeService: Send + Sync + 'static {
    /// Handle one decoded request.
    async fn handle(&self, request: RangeRequest) -> RangeResponse;
}

/// Authenticated client for framed TLS range RPC.
#[derive(Debug, Clone)]
pub struct FramedTcpClient {
    timeout: Duration,
    mode: RangeClientMode,
}

#[derive(Debug, Clone)]
enum RangeClientMode {
    Tls(RangeTlsClientConfig),
    #[cfg(test)]
    Plaintext,
}

/// Plaintext range transport exists only inside this crate's unit tests.
///
/// It is deliberately not exported from production builds: every production
/// range RPC must present an mTLS identity and verify its peer.
#[cfg(test)]
impl Default for FramedTcpClient {
    fn default() -> Self {
        Self {
            timeout: DEFAULT_RPC_TIMEOUT,
            mode: RangeClientMode::Plaintext,
        }
    }
}

impl FramedTcpClient {
    /// Build a plaintext client with an explicit wire-silence timeout for unit tests.
    #[cfg(test)]
    #[must_use]
    pub const fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            mode: RangeClientMode::Plaintext,
        }
    }

    /// Build a TLS-only forwarding client. This path always presents a client
    /// identity and validates the remote certificate and SNI name.
    pub fn with_tls(config: RangeTlsClientConfig) -> Result<Self, TransportError> {
        config.build_connector()?;
        Ok(Self {
            timeout: DEFAULT_RPC_TIMEOUT,
            mode: RangeClientMode::Tls(config),
        })
    }

    /// Send one request and await one response.
    pub async fn call(
        &self,
        endpoint: &str,
        request: &RangeRequest,
    ) -> Result<RangeResponse, TransportError> {
        let stream = timeout(self.timeout, TcpStream::connect(endpoint)).await??;
        match &self.mode {
            RangeClientMode::Tls(config) => {
                let connector = config.build_connector()?;
                let server_name =
                    rustls::pki_types::ServerName::try_from(config.server_name.as_str())
                        .map_err(|error| {
                            TransportError::Tls(format!("invalid range server name: {error}"))
                        })?
                        .to_owned();
                let stream = timeout(self.timeout, connector.connect(server_name, stream))
                    .await
                    .map_err(|_| TransportError::Timeout(self.timeout))?
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                call_stream(stream, request, self.timeout).await
            }
            #[cfg(test)]
            RangeClientMode::Plaintext => call_stream(stream, request, self.timeout).await,
        }
    }
}

/// Serve plaintext framed requests in unit tests only.
///
/// Production range listeners must use [`serve_tls`]. This symbol is omitted
/// from non-test builds so a production binary cannot accidentally expose a
/// [`RangeService`] without mTLS authorization.
#[cfg(test)]
pub async fn serve_tcp(
    listener: TcpListener,
    service: Arc<dyn RangeService>,
) -> Result<(), TransportError> {
    loop {
        let (stream, _) = listener.accept().await?;
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            if let Err(error) = handle_stream(stream, service).await {
                tracing::warn!(%error, "range transport connection failed");
            }
        });
    }
}

/// Serve TLS-only, mutually-authenticated range RPCs for one immutable tenant.
pub async fn serve_tls(
    listener: TcpListener,
    service: Arc<dyn RangeService>,
    config: RangeTlsServerConfig,
) -> Result<(), TransportError> {
    let acceptor = config.build_acceptor()?;
    loop {
        let (stream, _) = listener.accept().await?;
        let service = Arc::clone(&service);
        let acceptor = acceptor.clone();
        let allowed_principals = config.allowed_principals.clone();
        let tenant = config.tenant.clone();
        tokio::spawn(async move {
            let result = async {
                let stream = acceptor
                    .accept(stream)
                    .await
                    .map_err(|error| TransportError::Tls(error.to_string()))?;
                let certificates = stream.get_ref().1.peer_certificates().ok_or_else(|| {
                    TransportError::UnauthorizedPeer {
                        tenant: tenant.clone(),
                    }
                })?;
                let certificate =
                    certificates
                        .first()
                        .ok_or_else(|| TransportError::UnauthorizedPeer {
                            tenant: tenant.clone(),
                        })?;
                let principal = crabka_security::extract_principal_from_cert(certificate.as_ref())
                    .ok_or_else(|| TransportError::UnauthorizedPeer {
                        tenant: tenant.clone(),
                    })?;
                if !allowed_principals.contains(&principal) {
                    return Err(TransportError::UnauthorizedPeer { tenant });
                }
                handle_stream(stream, service).await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "range TLS transport connection rejected");
            }
        });
    }
}

/// Bind a plaintext loopback server for unit tests only.
#[cfg(test)]
pub async fn spawn_loopback(service: Arc<dyn RangeService>) -> Result<SocketAddr, TransportError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = serve_tcp(listener, service).await {
            tracing::warn!(%error, "range transport server stopped");
        }
    });
    Ok(addr)
}

async fn call_stream<S>(
    mut stream: S,
    request: &RangeRequest,
    wait: Duration,
) -> Result<RangeResponse, TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    timeout(wait, write_frame(&mut stream, request)).await??;
    timeout(wait, stream.flush()).await??;
    timeout(wait, read_frame(&mut stream)).await?
}

async fn handle_stream<S>(
    mut stream: S,
    service: Arc<dyn RangeService>,
) -> Result<(), TransportError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = read_frame(&mut stream).await?;
    let response = service.handle(request).await;
    write_frame(&mut stream, &response).await?;
    stream.flush().await?;
    Ok(())
}

async fn write_frame<W, T>(writer: &mut W, value: &T) -> Result<(), TransportError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(TransportError::FrameTooLarge {
            actual: bytes.len(),
            limit: MAX_FRAME_BYTES,
        });
    }
    let len = u32::try_from(bytes.len()).map_err(|_| TransportError::FrameTooLarge {
        actual: bytes.len(),
        limit: MAX_FRAME_BYTES,
    })?;
    writer.write_u32(len).await?;
    writer.write_all(&bytes).await?;
    Ok(())
}

async fn read_frame<R, T>(reader: &mut R) -> Result<T, TransportError>
where
    R: AsyncRead + Unpin,
    T: for<'de> Deserialize<'de>,
{
    let len = reader.read_u32().await?;
    let len = usize::try_from(len).map_err(|_| TransportError::FrameTooLarge {
        actual: MAX_FRAME_BYTES.saturating_add(1),
        limit: MAX_FRAME_BYTES,
    })?;
    if len > MAX_FRAME_BYTES {
        return Err(TransportError::FrameTooLarge {
            actual: len,
            limit: MAX_FRAME_BYTES,
        });
    }
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn timeout<T>(wait: Duration, task: impl Future<Output = T>) -> Result<T, TransportError> {
    tokio::time::timeout(wait, task)
        .await
        .map_err(|_| TransportError::Timeout(wait))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        path::PathBuf,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Default)]
    struct EchoService {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl RangeService for EchoService {
        async fn handle(&self, request: RangeRequest) -> RangeResponse {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match request {
                RangeRequest::Txn(TxnReq::Barrier { .. }) => RangeResponse::Txn(TxnResp::Barrier {
                    substrate_offset: 42,
                }),
                RangeRequest::Tso(TsoReq::Grant { count }) => {
                    RangeResponse::Tso(TsoResp::Granted {
                        first_ts: 10,
                        count,
                    })
                }
                RangeRequest::ResolveTxn(_request) => {
                    RangeResponse::ResolveTxn(ResolveTxnResp::Pending)
                }
                RangeRequest::Sql { sql, .. } => RangeResponse::Sql { result: sql },
                RangeRequest::ScanRange(request) => RangeResponse::ScanRange(ScanRangeResp {
                    rows: vec![ScanRangeRow {
                        rowid: request.interval.start.unwrap_or(1),
                        xmin: request.local_snapshot.xmin,
                        tuple: vec![1, 2, 3],
                    }],
                }),
                RangeRequest::Txn(_) => RangeResponse::Txn(TxnResp::Prepared),
            }
        }
    }

    struct MtlsFixture {
        _dir: tempfile::TempDir,
        server: RangeTlsServerConfig,
        client: RangeTlsClientConfig,
    }

    impl MtlsFixture {
        fn new(allowed_principals: BTreeSet<String>) -> Self {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let dir = tempfile::tempdir().expect("temporary certificate directory");
            let server_cert = write_fixture(&dir, "server-cert.pem", "dev_cert.pem");
            let server_key = write_fixture(&dir, "server-key.pem", "dev_key.pem");
            let client_ca = write_fixture(&dir, "client-ca.pem", "dev_client_ca.pem");
            let client_cert = write_fixture(&dir, "client-cert.pem", "dev_client_cert.pem");
            let client_key = write_fixture(&dir, "client-key.pem", "dev_client_key.pem");
            let server_tls = crabka_security::TlsConfig {
                cert_chain_path: server_cert.clone(),
                private_key_path: server_key,
                trust_roots_path: Some(server_cert.clone()),
                client_ca_path: Some(client_ca),
                client_auth: crabka_security::ClientAuthMode::Required,
            };
            let client_tls = crabka_security::TlsConfig {
                cert_chain_path: client_cert,
                private_key_path: client_key,
                trust_roots_path: Some(server_cert),
                client_ca_path: None,
                client_auth: crabka_security::ClientAuthMode::Disabled,
            };
            Self {
                _dir: dir,
                server: RangeTlsServerConfig {
                    tenant: "tenant-a".to_string(),
                    tls: server_tls,
                    allowed_principals,
                },
                client: RangeTlsClientConfig {
                    tls: client_tls,
                    server_name: "crabka-dev".to_string(),
                },
            }
        }
    }

    fn write_fixture(dir: &tempfile::TempDir, name: &str, fixture: &str) -> PathBuf {
        let path = dir.path().join(name);
        let contents: &[u8] = match fixture {
            "dev_cert.pem" => include_bytes!("../../security/tests/fixtures/dev_cert.pem"),
            "dev_key.pem" => include_bytes!("../../security/tests/fixtures/dev_key.pem"),
            "dev_client_ca.pem" => {
                include_bytes!("../../security/tests/fixtures/dev_client_ca.pem")
            }
            "dev_client_cert.pem" => {
                include_bytes!("../../security/tests/fixtures/dev_client_cert.pem")
            }
            "dev_client_key.pem" => {
                include_bytes!("../../security/tests/fixtures/dev_client_key.pem")
            }
            _ => unreachable!("fixture name is fixed by this module"),
        };
        std::fs::write(&path, contents).expect("write certificate fixture");
        path
    }

    async fn spawn_tls(service: Arc<dyn RangeService>, config: RangeTlsServerConfig) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind TLS listener");
        let address = listener.local_addr().expect("TLS listener address");
        tokio::spawn(async move {
            let _ = serve_tls(listener, service, config).await;
        });
        address
    }

    #[tokio::test]
    async fn mtls_allowlisted_principal_executes_sql_and_scan() {
        let fixture = MtlsFixture::new(BTreeSet::from([
            "CN=test-client,OU=integration,O=crabka".to_string()
        ]));
        let address = spawn_tls(Arc::new(EchoService::default()), fixture.server).await;
        let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS client");

        let sql = client
            .call(
                &address.to_string(),
                &RangeRequest::Sql {
                    range_id: RangeId::new(1),
                    sql: "select 1".to_string(),
                },
            )
            .await
            .expect("allowlisted SQL RPC");
        let scan = client
            .call(
                &address.to_string(),
                &RangeRequest::ScanRange(ScanRangeReq {
                    range_id: RangeId::new(1),
                    table_name: "t".to_string(),
                    interval: WireRowInterval {
                        start: None,
                        end: None,
                    },
                    local_snapshot: WireSnapshot {
                        xmin: 1,
                        xmax: 2,
                        xip: vec![],
                    },
                    global_snapshot: WireSnapshot {
                        xmin: 1,
                        xmax: 2,
                        xip: vec![],
                    },
                    own_xid: None,
                    read_ts: None,
                    predicate: WirePredicatePushdown::FullScan,
                    projection: WireProjectionPushdown::All,
                    partial_aggregate: None,
                    top_k: None,
                }),
            )
            .await
            .expect("allowlisted scan RPC");

        assert!(matches!(sql, RangeResponse::Sql { .. }));
        assert!(matches!(scan, RangeResponse::ScanRange(_)));
    }

    #[tokio::test]
    async fn mtls_authenticated_nonallowlisted_principal_never_invokes_service() {
        let fixture = MtlsFixture::new(BTreeSet::from(["CN=another-principal".to_string()]));
        let service = Arc::new(EchoService::default());
        let address = spawn_tls(service.clone(), fixture.server).await;
        let client = FramedTcpClient::with_tls(fixture.client).expect("mTLS client");

        let result = client
            .call(
                &address.to_string(),
                &RangeRequest::Sql {
                    range_id: RangeId::new(1),
                    sql: "select 1".to_string(),
                },
            )
            .await;

        assert!(result.is_err());
        assert_eq!(service.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn plaintext_framing_cannot_invoke_tls_range_service() {
        let fixture = MtlsFixture::new(BTreeSet::from([
            "CN=test-client,OU=integration,O=crabka".to_string()
        ]));
        let service = Arc::new(EchoService::default());
        let address = spawn_tls(service.clone(), fixture.server).await;
        let mut stream = TcpStream::connect(address)
            .await
            .expect("connect plaintext socket");

        write_frame(
            &mut stream,
            &RangeRequest::Sql {
                range_id: RangeId::new(1),
                sql: "select 1".to_string(),
            },
        )
        .await
        .expect("write plaintext frame");
        stream.flush().await.expect("flush plaintext frame");
        let response = read_frame::<_, RangeResponse>(&mut stream).await;

        assert!(response.is_err());
        assert_eq!(service.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_txn_barrier_offset() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::Txn(TxnReq::Barrier {
                    range_id: RangeId::COORDINATOR,
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            RangeResponse::Txn(TxnResp::Barrier {
                substrate_offset: 42
            })
        );
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_scan_range_payload() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::ScanRange(ScanRangeReq {
                    range_id: RangeId::new(7),
                    table_name: "t11".to_string(),
                    interval: WireRowInterval {
                        start: Some(9),
                        end: Some(20),
                    },
                    local_snapshot: WireSnapshot {
                        xmin: 5,
                        xmax: 12,
                        xip: vec![8],
                    },
                    global_snapshot: WireSnapshot {
                        xmin: 100,
                        xmax: 120,
                        xip: vec![108],
                    },
                    own_xid: Some(10),
                    read_ts: Some(22),
                    predicate: WirePredicatePushdown::Conjunctive {
                        predicates: vec![WireColumnPredicate {
                            column: 0,
                            op: WirePredicateOp::Ge,
                            value: WireDatum::Int4(3),
                        }],
                    },
                    projection: WireProjectionPushdown::Columns { columns: vec![0] },
                    partial_aggregate: Some(WirePartialAggregateSpec {
                        function: WirePartialAggregateFunction::Count,
                        column: None,
                    }),
                    top_k: Some(WireTopKSpec {
                        order_by: vec![WireTopKColumn {
                            column: 0,
                            asc: true,
                        }],
                        limit: 5,
                    }),
                }),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            RangeResponse::ScanRange(ScanRangeResp {
                rows: vec![ScanRangeRow {
                    rowid: 9,
                    xmin: 5,
                    tuple: vec![1, 2, 3],
                }]
            })
        );
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_tso_grant() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::Tso(TsoReq::Grant { count: 7 }),
            )
            .await
            .unwrap();

        assert_eq!(
            response,
            RangeResponse::Tso(TsoResp::Granted {
                first_ts: 10,
                count: 7
            })
        );
    }

    #[tokio::test]
    async fn loopback_transport_round_trips_resolve_txn_pending() {
        let addr = spawn_loopback(Arc::new(EchoService::default()))
            .await
            .unwrap();
        let response = FramedTcpClient::default()
            .call(
                &addr.to_string(),
                &RangeRequest::ResolveTxn(ResolveTxnReq {
                    primary_range: RangeId::new(7),
                    start_ts: 42,
                }),
            )
            .await
            .unwrap();

        assert_eq!(response, RangeResponse::ResolveTxn(ResolveTxnResp::Pending));
    }

    #[tokio::test]
    async fn silent_peer_times_out_instead_of_hanging() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_mins(1)).await;
        });

        let error = FramedTcpClient::with_timeout(Duration::from_millis(20))
            .call(
                &addr.to_string(),
                &RangeRequest::Sql {
                    range_id: RangeId::COORDINATOR,
                    sql: "select 1".to_string(),
                },
            )
            .await
            .expect_err("silent peer must timeout");

        assert!(matches!(error, TransportError::Timeout(_)));
    }
}
