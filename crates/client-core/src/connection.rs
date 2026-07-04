//! Single-broker `Connection`: TCP socket + reader/writer tasks +
//! correlation-ID multiplexing.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use crabka_ids::{ApiKey, ApiVersion};
use dashmap::DashMap;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{error::ClientError, request::ProtocolRequest, version::ApiVersionTable};

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
    #[tracing::instrument(level = "debug", skip_all, fields(addr = %addr), err)]
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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(addr = %addr, secured = options.security.is_some()),
        err,
    )]
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
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(addr = %addr, protocol = ?security.protocol),
        err,
    )]
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
    #[tracing::instrument(level = "debug", skip_all, err)]
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
    // cargo-mutants: live-broker send path; not unit-testable
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(api_key = R::API_KEY, version = tracing::field::Empty),
        err,
    )]
    pub async fn send<R: ProtocolRequest>(&self, req: R) -> Result<R::Response, ClientError> {
        // 1. Negotiate version.
        let version = self.inner.versions.negotiate::<R>()?;
        tracing::Span::current().record("version", version);

        // 2. Allocate correlation ID.
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);

        // 3. Build request header + encoded body into one frame.
        //
        // The header has a trailing tagged-fields byte (header v2) iff the
        // body is flexible. The `client_id` field is always i16 NULLABLE_STRING
        // per the upstream `RequestHeader.json` schema.
        let body_flexible = version >= R::FLEXIBLE_MIN;
        let mut frame = build_request_header(
            ApiKey(R::API_KEY),
            ApiVersion(version),
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
    // cargo-mutants: live-broker I/O path; not unit-testable
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(level = "debug", skip_all, fields(api_key, api_version), err)]
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
            ApiKey(api_key),
            ApiVersion(api_version),
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
    // cargo-mutants: one-line accessor returning a borrowed field
    #[must_use]
    #[cfg_attr(test, mutants::skip)]
    pub fn versions(&self) -> &ApiVersionTable {
        &self.inner.versions
    }

    /// Close the connection, cancelling all background tasks.
    // cargo-mutants: teardown; no observable return to assert against
    #[cfg_attr(test, mutants::skip)]
    pub fn close(self) {
        self.inner.shutdown.cancel();
        // The Arc gets dropped when `self` does; `JoinHandle`s abort naturally.
    }
}

/// Spawn independent reader and writer tasks over the split socket.
///
/// The socket is split into a read half and a write half, each driven by its
/// own task, rather than multiplexing both directions through one `select!`
/// over a single shared `Framed`. A combined task has to `await` the
/// `framed.send(...)` flush *inside* a `select!` arm, during which the read
/// arm is not polled — so for a request/response connection where the broker
/// stays silent until it receives the next request, a write that does not
/// complete in one poll wedges the whole connection: the frame sits buffered,
/// no inbound traffic ever re-drives the loop, and the caller's request never
/// reaches the wire. (This is what made `crabka-client-consumer`'s group
/// rejoin hang under the jemalloc heap-profiling allocator, whose per-alloc
/// sampling latency widened that window enough to trip it deterministically.)
/// Independent halves make an inbound frame always pollable while an outbound
/// write is in flight, and vice versa.
///
/// Liveness on teardown: either task exiting (EOF, I/O error, dropped
/// `Connection`, or `close()`) cancels the shared `shutdown` token so the
/// other task also stops, and the reader fails every outstanding request with
/// `Disconnected` — so a write-half failure surfaces to callers promptly
/// instead of stalling them until the request timeout.
fn spawn_io_tasks(
    stream: Box<dyn ClientDuplex>,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    shutdown: CancellationToken,
    pending: Pending,
) -> (JoinHandle<()>, JoinHandle<()>) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::{FramedRead, FramedWrite};

    let (read_half, write_half) = tokio::io::split(stream);
    let mut framed_read = FramedRead::new(read_half, crate::transport::codec());
    let mut framed_write = FramedWrite::new(write_half, crate::transport::codec());

    // WRITER: drains the dispatch channel, flushing each frame in receive
    // order. Owns only the write half, so a not-yet-writable socket can never
    // block the reader.
    let writer_shutdown = shutdown.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = writer_shutdown.cancelled() => break,
                item = writer_rx.recv() => {
                    let Some(item) = item else { break; };
                    if framed_write.send(item.bytes).await.is_err() {
                        break;
                    }
                }
            }
        }
        // A write-side failure (or all senders dropped) must wake the reader
        // so it drains pending callers to `Disconnected` rather than letting
        // them wait out the request timeout.
        writer_shutdown.cancel();
    });

    // READER: pulls frames and fulfils the matching pending oneshot. Owns only
    // the read half.
    let reader = tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                maybe_frame = framed_read.next() => {
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
        // Stop the writer too, then fail every outstanding request.
        shutdown.cancel();
        let keys: Vec<i32> = pending.iter().map(|e| *e.key()).collect();
        for k in keys {
            if let Some((_, tx)) = pending.remove(&k) {
                let _ = tx.send(Err(ClientError::Disconnected));
            }
        }
    });

    (reader, writer)
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
    api_key: ApiKey,
    version: ApiVersion,
    corr_id: i32,
    client_id: &str,
    with_tagged_fields: bool,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32);
    buf.put_i16(api_key.0);
    buf.put_i16(version.0);
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
#[tracing::instrument(level = "debug", skip_all, err)]
async fn fetch_api_versions(conn: &Connection) -> Result<ApiVersionTable, ClientError> {
    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        },
    };

    let req = ApiVersionsRequest::default();
    let corr_id = conn.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);

    // v0 is non-flexible: header v1, no tagged-fields byte.
    let mut frame = build_request_header(
        ApiKey(ApiVersionsRequest::API_KEY),
        ApiVersion(0),
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
    use crabka_security::ListenerProtocol;

    use super::*;
    use crate::security::{ClientSecurity, SaslCredentials};

    // A SASL_PLAINTEXT connect drives the handshake then ApiVersions.
    // The fake broker answers SaslHandshake(0), SaslAuthenticate(0),
    // then a minimal ApiVersionsResponse v0 so from_stream succeeds.
    #[tokio::test]
    async fn connect_secured_runs_sasl_then_api_versions() {
        use crabka_protocol::{
            Encode,
            owned::{
                api_versions_response::ApiVersionsResponse,
                sasl_authenticate_response::SaslAuthenticateResponse,
                sasl_handshake_response::SaslHandshakeResponse,
            },
        };
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

#[cfg(test)]
mod io_task_tests {
    use std::time::Instant;

    use crabka_protocol::{Encode, owned::api_versions_response::ApiVersionsResponse};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;

    // The split reader/writer tasks must keep their teardown contract: a
    // server that closes the connection mid-request has to surface to the
    // caller promptly as `Disconnected`, NOT stall until the request timeout.
    // The reader's EOF cancels the shared shutdown (stopping the writer) and
    // drains every outstanding request — so a write-half failure can't strand
    // a caller for the full timeout. (Regression guard for the io-task split.)
    #[tokio::test]
    async fn server_close_mid_request_yields_prompt_disconnected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            // Answer the `from_stream` ApiVersions handshake.
            let len = s.read_u32().await.unwrap();
            let mut req = vec![0u8; len as usize];
            s.read_exact(&mut req).await.unwrap();
            let corr = i32::from_be_bytes([req[4], req[5], req[6], req[7]]);
            let mut body = BytesMut::new();
            ApiVersionsResponse::default().encode(&mut body, 0).unwrap();
            let mut frame = BytesMut::new();
            frame.put_i32(corr);
            frame.put_slice(&body);
            s.write_u32(u32::try_from(frame.len()).unwrap())
                .await
                .unwrap();
            s.write_all(&frame).await.unwrap();
            s.flush().await.unwrap();
            // Read the next request fully (so its pending entry is registered),
            // then drop the socket without replying.
            let len2 = s.read_u32().await.unwrap();
            let mut req2 = vec![0u8; len2 as usize];
            s.read_exact(&mut req2).await.unwrap();
            // `s` drops here -> the connection closes with no response.
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let opts = ConnectionOptions {
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let conn = Connection::from_stream(Box::new(stream), opts)
            .await
            .expect("plaintext from_stream completes");

        let started = Instant::now();
        // Hand-framed Metadata request; the server reads it then closes.
        let result = conn.raw_request(3, 0, Bytes::new()).await;
        assert!(
            matches!(result, Err(ClientError::Disconnected)),
            "server close mid-request must yield Disconnected, got {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "drain must be prompt (reader EOF), not a request-timeout stall"
        );
        server.await.unwrap();
    }
}
