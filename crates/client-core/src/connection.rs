//! Single-broker `Connection`: TCP socket + reader/writer tasks +
//! correlation-ID multiplexing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::request::ProtocolRequest;
use crate::version::ApiVersionTable;

/// Trait alias for the duplex stream types `Connection::from_stream`
/// accepts (`TcpStream`, `tokio_rustls::client::TlsStream`, etc.). Boxed
/// so callers can hand in heterogeneous stream types via one path.
pub trait ClientDuplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> ClientDuplex for T {}

type Pending = Arc<DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>>;

/// Kafka API key for `ApiVersionsRequest` / `ApiVersionsResponse`.
///
/// Used to apply the response-header quirk: `ApiVersionsResponse` always
/// uses `ResponseHeader v0` (no tagged-fields byte) even when the request
/// version is flexible (v3+).
const API_VERSIONS_KEY: i16 = 18;

/// Connect-time + per-request configuration knobs.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub client_id: String,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// Client-side TLS/SASL policy. `None` = plaintext (default).
    ///
    /// Boxed so `ConnectionOptions` stays small: it is cloned widely and
    /// embedded in many connection-building futures, and `ClientSecurity`
    /// carries several `String`/`PathBuf` fields that would otherwise
    /// bloat every such future.
    pub security: Option<Box<crate::security::ClientSecurity>>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_id: "crabka".into(),
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            security: None,
        }
    }
}

/// A connection to a single Kafka broker.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    versions: ApiVersionTable,
    options: ConnectionOptions,
    next_corr_id: AtomicI32,
    pending: Pending,
    writer_tx: mpsc::Sender<DispatchItem>,
    shutdown: CancellationToken,
    _reader: JoinHandle<()>,
    _writer: JoinHandle<()>,
}

struct DispatchItem {
    bytes: Bytes,
}

impl Connection {
    /// Connect to `addr`, negotiate API versions, return a usable `Connection`.
    pub async fn connect(
        addr: SocketAddr,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let stream = tokio::time::timeout(options.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Timeout(options.connect_timeout))?
            .map_err(|source| ClientError::Connect { addr, source })?;

        stream.set_nodelay(true).ok();

        Self::from_stream(Box::new(stream), options).await
    }

    /// Connect to `addr` honouring `options.security`: a secured (TLS/SASL)
    /// dial when a policy is set, plaintext otherwise.
    ///
    /// This is the single connect entry point for every metadata-client
    /// site (pool, admin, RLMM fetch loop) so the plaintext-vs-secured
    /// branch can't drift between them. The plaintext (`None`) path is
    /// byte-identical to [`Self::connect`].
    ///
    /// # Errors
    /// Propagates [`Self::connect`] / [`Self::connect_secured`] failures.
    pub async fn connect_with_options(
        addr: SocketAddr,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        match options.security.clone() {
            Some(sec) => Self::connect_secured(addr, options, sec.as_ref()).await,
            None => Self::connect(addr, options).await,
        }
    }

    /// Connect to `addr`, applying `security` (TLS then SASL) before the
    /// API-versions bootstrap. `Plaintext` is identical to [`Self::connect`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Connect`] / [`ClientError::Timeout`] on the
    /// TCP dial, or [`ClientError::Io`] if the TLS or SASL handshake fails
    /// or the security policy is internally inconsistent (e.g. a TLS
    /// protocol with no TLS config).
    pub async fn connect_secured(
        addr: SocketAddr,
        options: ConnectionOptions,
        security: &crate::security::ClientSecurity,
    ) -> Result<Self, ClientError> {
        let tcp = tokio::time::timeout(options.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Timeout(options.connect_timeout))?
            .map_err(|source| ClientError::Connect { addr, source })?;
        tcp.set_nodelay(true).ok();

        // 1. TLS (if the protocol demands it).
        let mut stream: Box<dyn ClientDuplex> = if security.protocol.requires_tls() {
            let tls = security.tls.as_ref().ok_or_else(|| {
                ClientError::Io(std::io::Error::other("TLS protocol without tls config"))
            })?;
            let connector = tls
                .connector()
                .map_err(|e| ClientError::Io(std::io::Error::other(e)))?;
            let sni =
                tokio_rustls::rustls::pki_types::ServerName::try_from(tls.server_name.clone())
                    .map_err(|e| {
                        ClientError::Io(std::io::Error::other(format!("invalid SNI: {e}")))
                    })?;
            let s = connector
                .connect(sni, tcp)
                .await
                .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
            Box::new(s)
        } else {
            Box::new(tcp)
        };

        // 2. SASL (if the protocol demands it).
        if security.protocol.requires_sasl() {
            let creds = security.sasl.as_ref().ok_or_else(|| {
                ClientError::Io(std::io::Error::other("SASL protocol without credentials"))
            })?;
            // GSSAPI SPN host: explicit `sasl_host`, else TLS SNI, else the
            // connection's target IP, else "localhost". The target IP is a
            // last resort — for GSSAPI the caller should set `sasl_host` so
            // the principal matches the broker's advertised hostname.
            let target = addr.ip().to_string();
            let server_name = security.sasl_handshake_host(Some(target.as_str()));
            crate::sasl::outbound_sasl(&mut *stream, creds, server_name)
                .await
                .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
        }

        Self::from_stream(stream, options).await
    }

    /// Build a `Connection` over a pre-established, optionally
    /// pre-authenticated stream. Negotiates API versions over the stream
    /// and returns a usable `Connection`.
    ///
    /// Used by the broker's `InterBrokerClient` integration: TLS + SASL
    /// handshake run before this call, so the stream is already
    /// authenticated. From here on the connection's normal request /
    /// response framing applies.
    pub async fn from_stream(
        stream: Box<dyn ClientDuplex>,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let (writer_tx, writer_rx) = mpsc::channel::<DispatchItem>(64);
        let shutdown = CancellationToken::new();
        let pending: Pending = Arc::new(DashMap::new());

        let (reader_handle, writer_handle) =
            spawn_io_tasks(stream, writer_rx, shutdown.clone(), Arc::clone(&pending));

        let mut conn = Self {
            inner: Arc::new(ConnectionInner {
                versions: ApiVersionTable::default(),
                options: options.clone(),
                next_corr_id: AtomicI32::new(0),
                pending,
                writer_tx,
                shutdown,
                _reader: reader_handle,
                _writer: writer_handle,
            }),
        };

        let versions = fetch_api_versions(&conn).await?;
        let inner = Arc::get_mut(&mut conn.inner).expect("unique handle at connect-time");
        inner.versions = versions;

        Ok(conn)
    }

    /// Send a typed request and await the typed response.
    ///
    /// The version is negotiated from the broker-advertised table populated
    /// during `connect`. The request and response headers are encoded and
    /// decoded automatically.
    ///
    /// # Errors
    ///
    /// Returns `ClientError::IncompatibleVersion` if there is no mutually
    /// supported version, `ClientError::Disconnected` if the I/O loop has
    /// exited, or `ClientError::Timeout` if no response arrives in time.
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        // 1. Negotiate version.
        let version = self.inner.versions.negotiate::<R>()?;

        // 2. Allocate correlation ID.
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);

        // 3. Build request header + encoded body into one frame.
        //
        // The header has a trailing tagged-fields byte (header v2) iff the
        // body is flexible. The `client_id` field is always i16 NULLABLE_STRING
        // per the upstream `RequestHeader.json` schema.
        let body_flexible = version >= R::FLEXIBLE_MIN;
        let mut frame = build_request_header(
            R::API_KEY,
            version,
            corr_id,
            &self.inner.options.client_id,
            body_flexible,
        );
        req.encode(&mut frame, version)?;

        // 4. Register the oneshot before dispatching (avoids a race).
        let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
        self.inner.pending.insert(corr_id, tx);

        // 5. Dispatch to writer.
        self.inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)?;

        // 6. Await response with timeout.
        let body_bytes = match tokio::time::timeout(self.inner.options.request_timeout, rx).await {
            Ok(Ok(Ok(b))) => b,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(_recv_closed)) => return Err(ClientError::Disconnected),
            Err(_timeout) => {
                // Evict the pending entry so the reader won't try to fulfil it.
                self.inner.pending.remove(&corr_id);
                return Err(ClientError::Timeout(self.inner.options.request_timeout));
            }
        };

        // 7. Decode the response.
        //
        // The reader has already stripped the 4-byte correlation_id prefix.
        // What remains is: [ResponseHeader fields after corr_id] + [response body].
        //
        // ResponseHeader version rules:
        //   - ApiVersionsResponse (api_key=18): always ResponseHeader v0, which
        //     has NO fields after the correlation_id. This is a long-standing
        //     Kafka asymmetry — even flexible ApiVersions responses use v0 header.
        //   - All other flexible messages (version >= FLEXIBLE_MIN): ResponseHeader
        //     v1 adds 1 byte for the tagged-fields count (0x00 when empty).
        //   - Non-flexible messages: ResponseHeader v0 (no bytes after corr_id).
        let mut cursor: &[u8] = &body_bytes;
        let uses_flexible_resp_header = body_flexible && R::API_KEY != API_VERSIONS_KEY;
        if uses_flexible_resp_header && !cursor.is_empty() {
            // Consume the tagged-fields byte (always 0x00 in practice).
            cursor = &cursor[1..];
        }

        let resp = <R::Response as crabka_protocol::Decode>::decode(&mut cursor, version)?;
        Ok(resp)
    }

    /// Send a hand-framed request and await the raw response body.
    ///
    /// This bypasses the typed [`ProtocolRequest`] codegen path so callers
    /// can speak Crabka-private APIs (e.g., the controller's Raft RPCs at
    /// api keys 1000+) whose wire types live outside `crabka-protocol`.
    ///
    /// The header is always written as `RequestHeader v2` (flexible) with
    /// an empty trailing tagged-fields byte. The response is assumed to
    /// use `ResponseHeader v1` (flexible): the I/O loop strips the 4-byte
    /// correlation id, and this method strips the leading tagged-fields
    /// byte before returning. Callers receive the raw body bytes only.
    ///
    /// `body` is the encoded request body (everything after the request
    /// header), exactly as it should appear on the wire.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Disconnected`] if the I/O loop has exited
    /// or [`ClientError::Timeout`] if no response arrives within the
    /// configured request timeout.
    pub async fn raw_request(
        &self,
        api_key: i16,
        api_version: i16,
        body: Bytes,
    ) -> Result<Bytes, ClientError> {
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);

        // RequestHeader v2 (flexible). Crabka-private api keys are always
        // declared flexible so the header shape is predictable.
        let mut frame = build_request_header(
            api_key,
            api_version,
            corr_id,
            &self.inner.options.client_id,
            true,
        );
        frame.put_slice(&body);

        let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
        self.inner.pending.insert(corr_id, tx);

        self.inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)?;

        let body_bytes = match tokio::time::timeout(self.inner.options.request_timeout, rx).await {
            Ok(Ok(Ok(b))) => b,
            Ok(Ok(Err(e))) => return Err(e),
            Ok(Err(_recv_closed)) => return Err(ClientError::Disconnected),
            Err(_timeout) => {
                self.inner.pending.remove(&corr_id);
                return Err(ClientError::Timeout(self.inner.options.request_timeout));
            }
        };

        // ResponseHeader v1: 1-byte empty-tagged-fields marker after the
        // already-stripped correlation id. Drop it if present.
        let slice: &[u8] = &body_bytes;
        let out = if slice.is_empty() {
            Bytes::new()
        } else {
            body_bytes.slice(1..)
        };
        Ok(out)
    }

    /// Negotiated API versions known to this connection.
    #[must_use]
    pub fn versions(&self) -> &ApiVersionTable {
        &self.inner.versions
    }

    /// Close the connection, cancelling all background tasks.
    pub fn close(self) {
        self.inner.shutdown.cancel();
        // The Arc gets dropped when `self` does; `JoinHandle`s abort naturally.
    }
}

/// Spawn the combined I/O task on a single `Framed` socket.
///
/// One task owns the entire `Framed` and `select!`s between incoming
/// frames (from the broker) and outgoing dispatch items (from callers).
/// A no-op second handle preserves the `(reader, writer)` API shape
/// expected by `ConnectionInner`.
fn spawn_io_tasks(
    stream: Box<dyn ClientDuplex>,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    shutdown: CancellationToken,
    pending: Pending,
) -> (JoinHandle<()>, JoinHandle<()>) {
    use futures_util::{SinkExt, StreamExt};

    let mut framed = crate::transport::frame_generic(stream);
    let pending_for_drain = Arc::clone(&pending);

    let combined = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                Some(item) = writer_rx.recv() => {
                    if framed.send(item.bytes).await.is_err() {
                        break;
                    }
                }
                maybe_frame = framed.next() => {
                    let Some(frame) = maybe_frame else { break; };
                    let Ok(frame) = frame else { break; };
                    if frame.len() < 4 { continue; }
                    let corr_id = i32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
                    if let Some((_, tx)) = pending.remove(&corr_id) {
                        let body = Bytes::copy_from_slice(&frame[4..]);
                        let _ = tx.send(Ok(body));
                    }
                }
            }
        }
        // Drain pending: every outstanding request fails with Disconnected.
        let keys: Vec<i32> = pending_for_drain.iter().map(|e| *e.key()).collect();
        for k in keys {
            if let Some((_, tx)) = pending_for_drain.remove(&k) {
                let _ = tx.send(Err(ClientError::Disconnected));
            }
        }
    });

    let noop = tokio::spawn(async {});
    (combined, noop)
}

/// Build an encoded `RequestHeader` into a `BytesMut`.
///
/// Kafka has only two `RequestHeader` formats:
///
/// - **v1** (non-flexible): `api_key` + `version` + `corr_id` + i16
///   `client_id` length + `client_id` bytes.
/// - **v2** (flexible): same fields *plus* a trailing `tagged_fields` byte
///   (`0x00` when empty).
///
/// Note that `client_id` is `NULLABLE_STRING` (i16 length) in **both**
/// versions — the upstream `RequestHeader.json` schema marks the field as
/// `"flexibleVersions": "none"`, so even a v2 header keeps the i16-length
/// encoding. Using UVARINT here causes the broker to misread the length and
/// throw `InvalidRequestException` during header parsing.
///
/// Pass `with_tagged_fields = true` iff the request body is flexible
/// (`version >= R::FLEXIBLE_MIN`).
fn build_request_header(
    api_key: i16,
    version: i16,
    corr_id: i32,
    client_id: &str,
    with_tagged_fields: bool,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32);
    buf.put_i16(api_key);
    buf.put_i16(version);
    buf.put_i32(corr_id);
    let n = i16::try_from(client_id.len()).expect("client_id fits in i16");
    buf.put_i16(n);
    buf.put_slice(client_id.as_bytes());
    if with_tagged_fields {
        buf.put_u8(0); // empty tagged fields
    }
    buf
}

/// Send an `ApiVersionsRequest` at version 0 and return the negotiated table.
///
/// This is the bootstrap step inside `connect`: no version table exists yet,
/// so we cannot use `Connection::send`. Version 0 is guaranteed to be
/// supported by every broker.
async fn fetch_api_versions(conn: &Connection) -> Result<ApiVersionTable, ClientError> {
    use crabka_protocol::Encode;
    use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
    use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;

    let req = ApiVersionsRequest::default();
    let corr_id = conn.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);

    // v0 is non-flexible: header v1, no tagged-fields byte.
    let mut frame = build_request_header(
        ApiVersionsRequest::API_KEY,
        0,
        corr_id,
        &conn.inner.options.client_id,
        false,
    );
    req.encode(&mut frame, 0)?;

    let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
    conn.inner.pending.insert(corr_id, tx);
    conn.inner
        .writer_tx
        .send(DispatchItem {
            bytes: frame.freeze(),
        })
        .await
        .map_err(|_| ClientError::Disconnected)?;

    let body_bytes = tokio::time::timeout(conn.inner.options.connect_timeout, rx)
        .await
        .map_err(|_| ClientError::Timeout(conn.inner.options.connect_timeout))?
        .map_err(|_| ClientError::Disconnected)??;

    // ResponseHeader v0: only correlation_id (already stripped by the reader).
    // No tagged-fields byte — this holds for all ApiVersionsResponse versions,
    // including flexible ones (the Kafka asymmetry documented in `send`).
    let mut cursor: &[u8] = &body_bytes;
    let resp = <ApiVersionsResponse as crabka_protocol::Decode>::decode(&mut cursor, 0)?;
    if resp.error_code != 0 {
        return Err(ClientError::Server {
            error_code: resp.error_code,
        });
    }

    let entries = resp
        .api_keys
        .iter()
        .map(|k| (k.api_key, k.min_version, k.max_version));
    Ok(ApiVersionTable::from_entries(entries))
}

#[cfg(test)]
mod secured_tests {
    use super::*;
    use crate::security::{ClientSecurity, SaslCredentials};
    use crabka_security::ListenerProtocol;

    // A SASL_PLAINTEXT connect drives the handshake then ApiVersions.
    // The fake broker answers SaslHandshake(0), SaslAuthenticate(0),
    // then a minimal ApiVersionsResponse v0 so from_stream succeeds.
    #[tokio::test]
    async fn connect_secured_runs_sasl_then_api_versions() {
        use crabka_protocol::Encode;
        use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
        use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
        use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            // (body, flexible_response_header)
            let replies: [(BytesMut, bool); 3] = [
                {
                    let mut b = BytesMut::new();
                    SaslHandshakeResponse {
                        error_code: 0,
                        ..Default::default()
                    }
                    .encode(&mut b, 1)
                    .unwrap();
                    (b, false)
                },
                {
                    let mut b = BytesMut::new();
                    SaslAuthenticateResponse {
                        error_code: 0,
                        ..Default::default()
                    }
                    .encode(&mut b, 2)
                    .unwrap();
                    (b, true)
                },
                {
                    let mut b = BytesMut::new();
                    ApiVersionsResponse::default().encode(&mut b, 0).unwrap();
                    // ApiVersions always uses a v0 response header.
                    (b, false)
                },
            ];
            for (body, flex_header) in replies {
                let req_len = s.read_u32().await.unwrap();
                let mut req = vec![0u8; req_len as usize];
                s.read_exact(&mut req).await.unwrap();
                let corr = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
                let mut frame = BytesMut::new();
                frame.put_i32(corr);
                if flex_header {
                    frame.put_u8(0);
                }
                frame.put_slice(&body);
                s.write_u32(u32::try_from(frame.len()).unwrap())
                    .await
                    .unwrap();
                s.write_all(&frame).await.unwrap();
                s.flush().await.unwrap();
            }
        });
        let security = ClientSecurity {
            protocol: ListenerProtocol::SaslPlaintext,
            tls: None,
            sasl: Some(SaslCredentials::Plain {
                username: "u".into(),
                password: "p".into(),
            }),
            sasl_host: None,
        };
        let conn = Connection::connect_secured(addr, ConnectionOptions::default(), &security)
            .await
            .expect("secured connect completes");
        conn.close();
        server.await.unwrap();
    }
}
