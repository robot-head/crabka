//! Single-broker `Connection`.
//!
//! A `Connection` holds a TCP socket, reader and writer tasks, and
//! correlation-ID multiplexing.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
};

use bytes::{BufMut, Bytes, BytesMut};
use crabka_ids::{ApiKey, ApiVersion};
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
    mebibytes, secs,
};
use dashmap::DashMap;
use refined_type::rule::{GreaterI64, GreaterUsize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{error::ClientError, request::ProtocolRequest, version::ApiVersionTable};

/// Trait alias for the duplex stream types `Connection::from_stream` accepts,
/// such as `TcpStream` and `tokio_rustls::client::TlsStream`.
///
/// The trait is boxed so callers can hand in different stream types through
/// one path.
pub trait ClientDuplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin + ?Sized> ClientDuplex for T {}

type Pending = Arc<DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>>;

/// Kafka API key for `ApiVersionsRequest` / `ApiVersionsResponse`.
///
/// `Connection` uses this key to apply the response-header quirk:
/// `ApiVersionsResponse` always uses `ResponseHeader v0` (no tagged-fields
/// byte) even when the request version is flexible (v3+).
const API_VERSIONS_KEY: i16 = 18;

/// Default deadline for one client DNS lookup.
pub const DEFAULT_CLIENT_DNS_TIMEOUT: Time = secs(10);
/// Default deadline for one client TCP connection attempt.
pub const DEFAULT_CLIENT_CONNECT_TIMEOUT: Time = secs(30);
/// Default deadline for one client request.
pub const DEFAULT_CLIENT_REQUEST_TIMEOUT: Time = secs(30);
/// Default capacity of one connection's pending request dispatch queue.
pub const DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY: usize = 64;
/// Fixed security ceiling for accepted client frames.
pub const MAX_CLIENT_FRAME_BYTES: ByteSize = mebibytes(100);
/// Default maximum accepted client frame size.
pub const DEFAULT_CLIENT_FRAME_MAX: ByteSize = MAX_CLIENT_FRAME_BYTES;

/// Positive, whole-millisecond DNS lookup deadline.
///
/// Stores the validated millisecond count so policy structs can retain `Eq`
/// while public configuration boundaries use dimensioned [`Time`] values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientDnsTimeout(i64);

impl ClientDnsTimeout {
    /// Validate a DNS lookup deadline.
    ///
    /// # Errors
    ///
    /// Returns an error when the duration is non-finite, zero, negative,
    /// fractional in milliseconds, or cannot be represented as `i64`
    /// milliseconds.
    pub fn new(value: Time) -> Result<Self, String> {
        let milliseconds = GreaterI64::<0>::new(value.millis_i64())
            .map_err(|error| format!("client DNS timeout: {error}"))?
            .into_value();
        if !value.secs_f64().is_finite() || Time::from_millis(milliseconds) != value {
            return Err("client DNS timeout must be a whole number of milliseconds".to_owned());
        }
        Ok(Self(milliseconds))
    }

    /// Return the validated timeout.
    #[must_use]
    pub fn time(self) -> Time {
        Time::from_millis(self.0)
    }

    /// Return the validated timeout in milliseconds.
    #[must_use]
    pub const fn milliseconds(self) -> i64 {
        self.0
    }
}

impl Default for ClientDnsTimeout {
    fn default() -> Self {
        Self::new(DEFAULT_CLIENT_DNS_TIMEOUT).expect("default client DNS timeout is valid")
    }
}

/// Positive capacity of one connection's pending request dispatch queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionDispatchQueueCapacity(usize);

impl ConnectionDispatchQueueCapacity {
    /// Validate a dispatch queue capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| format!("client dispatch queue capacity: {error}"))
    }

    /// Return the validated capacity.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl Default for ConnectionDispatchQueueCapacity {
    fn default() -> Self {
        Self::new(DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY)
            .expect("default client dispatch queue capacity is valid")
    }
}

/// Positive whole-byte accepted-frame limit bounded by the fixed security ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientFrameMax(usize);

impl ClientFrameMax {
    /// Return the validated byte count.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.0
    }

    /// Return the validated limit as a dimensioned quantity.
    #[must_use]
    pub fn size(self) -> ByteSize {
        ByteSize::from_bytes(u64::try_from(self.0).unwrap_or(u64::MAX))
    }
}

impl TryFrom<ByteSize> for ClientFrameMax {
    type Error = String;

    fn try_from(value: ByteSize) -> Result<Self, Self::Error> {
        let bytes = value.bytes_f64();
        if !bytes.is_finite()
            || bytes.fract() != 0.0
            || !(1.0..=MAX_CLIENT_FRAME_BYTES.bytes_f64()).contains(&bytes)
        {
            return Err(
                "client frame max must be a positive whole-byte value no greater than 100MiB"
                    .to_owned(),
            );
        }
        usize::try_from(value.bytes_u64())
            .map(Self)
            .map_err(|_| "client frame max does not fit usize".to_owned())
    }
}

impl Default for ClientFrameMax {
    fn default() -> Self {
        Self::try_from(DEFAULT_CLIENT_FRAME_MAX).expect("default client frame max is valid")
    }
}

/// Connect-time + per-request configuration knobs.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    pub client_id: String,
    pub dns_timeout: ClientDnsTimeout,
    pub connect_timeout: Time,
    pub request_timeout: Time,
    pub dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub frame_max: ClientFrameMax,
    /// Client-side TLS/SASL policy. `None` = plaintext (default).
    ///
    /// This field is boxed so `ConnectionOptions` stays small. Many call
    /// sites clone `ConnectionOptions` and embed it in connection-building
    /// futures, and `ClientSecurity` carries several `String`/`PathBuf`
    /// fields that would otherwise make every such future large.
    pub security: Option<Box<crate::security::ClientSecurity>>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_id: "crabka".into(),
            dns_timeout: ClientDnsTimeout::default(),
            connect_timeout: DEFAULT_CLIENT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_CLIENT_REQUEST_TIMEOUT,
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
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
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    pub async fn connect(
        addr: SocketAddr,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let stream =
            tokio::time::timeout(options.connect_timeout.to_std(), TcpStream::connect(addr))
                .await
                .map_err(|_| ClientError::Timeout(options.connect_timeout))?
                .map_err(|source| ClientError::Connect { addr, source })?;

        stream.set_nodelay(true).ok();

        Self::from_stream(Box::new(stream), options).await
    }

    /// Connect to `addr` and honour `options.security`.
    ///
    /// This method makes a secured (TLS/SASL) dial when a policy is set, and
    /// a plaintext dial otherwise. It is the single connect entry point for
    /// every metadata-client site (pool, admin, RLMM fetch loop), so the
    /// plaintext-versus-secured branch cannot drift between them. The
    /// plaintext (`None`) path is byte-identical to [`Self::connect`].
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

    /// Connect to `addr` and apply `security` (TLS then SASL) before the
    /// API-versions bootstrap.
    ///
    /// `Plaintext` is identical to [`Self::connect`].
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
        let tcp = tokio::time::timeout(options.connect_timeout.to_std(), TcpStream::connect(addr))
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
            crate::sasl::outbound_sasl(
                &mut *stream,
                creds,
                server_name,
                &options.client_id,
                options.frame_max,
            )
            .await
            .map_err(|e| ClientError::Io(std::io::Error::other(e.to_string())))?;
        }

        Self::from_stream(stream, options).await
    }

    /// Build a `Connection` over a pre-established, optionally
    /// pre-authenticated stream.
    ///
    /// This method negotiates API versions over the stream and returns a
    /// usable `Connection`. The broker's `InterBrokerClient` integration
    /// calls it: the TLS + SASL handshake runs before this call, so the
    /// stream is already authenticated. From here on the connection's normal
    /// request / response framing applies.
    #[tracing::instrument(level = "debug", skip_all, err)]
    /// # Errors
    /// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
    /// # Panics
    /// Panics if synchronized client state is poisoned or a response violates an invariant established by protocol validation.
    pub async fn from_stream(
        stream: Box<dyn ClientDuplex>,
        options: ConnectionOptions,
    ) -> Result<Self, ClientError> {
        let (writer_tx, writer_rx) =
            mpsc::channel::<DispatchItem>(options.dispatch_queue_capacity.get());
        let shutdown = CancellationToken::new();
        let pending: Pending = Arc::new(DashMap::new());

        let (reader_handle, writer_handle) = spawn_io_tasks(
            stream,
            writer_rx,
            shutdown.clone(),
            Arc::clone(&pending),
            options.frame_max,
        );

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

    /// Return the peer-advertised version range for `api_key`.
    ///
    /// Higher-level clients use this to distinguish an API that is absent
    /// from a listener's surface from one whose advertised versions merely do
    /// not overlap their codec range.
    #[must_use]
    pub fn advertised_api_range(&self, api_key: i16) -> Option<(i16, i16)> {
        self.inner.versions.broker_range(api_key)
    }

    /// Send a typed request and await the typed response.
    ///
    /// This method negotiates the version from the broker-advertised table
    /// that `connect` populated. It encodes and decodes the request and
    /// response headers automatically.
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

        // 4. Dispatch request and await response.
        let body_bytes = self.dispatch_request(corr_id, frame).await?;

        // 5. Decode the response.
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

    /// Encode and enqueue a typed request that intentionally has no response.
    /// Kafka Produce with `acks=0` is the standard use case.
    ///
    /// # Errors
    ///
    /// Returns an error if version negotiation or encoding fails, or if the
    /// connection writer has stopped before accepting the frame.
    pub async fn send_no_response<R: ProtocolRequest>(&self, req: R) -> Result<(), ClientError> {
        let version = self.inner.versions.negotiate::<R>()?;
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::Relaxed);
        let body_flexible = version >= R::FLEXIBLE_MIN;
        let mut frame = build_request_header(
            ApiKey(R::API_KEY),
            ApiVersion(version),
            corr_id,
            &self.inner.options.client_id,
            body_flexible,
        );
        req.encode(&mut frame, version)?;
        self.inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)
    }

    /// Send a hand-framed request and await the raw response body.
    ///
    /// This method bypasses the typed [`ProtocolRequest`] codegen path so
    /// callers can speak Crabka-private APIs whose wire types live outside
    /// `crabka-protocol`, for example the controller's Raft RPCs at api keys
    /// 1000+.
    ///
    /// This method always writes the header as `RequestHeader v2` (flexible)
    /// with an empty trailing tagged-fields byte. It assumes the response
    /// uses `ResponseHeader v1` (flexible): the I/O loop strips the 4-byte
    /// correlation id, and this method strips the leading tagged-fields byte
    /// before it returns. Callers receive the raw body bytes only.
    ///
    /// `body` is the encoded request body, which is everything after the
    /// request header, exactly as it should appear on the wire.
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

        let body_bytes = self.dispatch_request(corr_id, frame).await?;

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

    async fn dispatch_request(&self, corr_id: i32, frame: BytesMut) -> Result<Bytes, ClientError> {
        let (tx, rx) = oneshot::channel::<Result<Bytes, ClientError>>();
        self.inner.pending.insert(corr_id, tx);

        self.inner
            .writer_tx
            .send(DispatchItem {
                bytes: frame.freeze(),
            })
            .await
            .map_err(|_| ClientError::Disconnected)?;

        match tokio::time::timeout(self.inner.options.request_timeout.to_std(), rx).await {
            Ok(Ok(Ok(bytes))) => Ok(bytes),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_recv_closed)) => Err(ClientError::Disconnected),
            Err(_timeout) => {
                self.inner.pending.remove(&corr_id);
                Err(ClientError::Timeout(self.inner.options.request_timeout))
            }
        }
    }
}

/// Spawn independent reader and writer tasks over the split socket.
///
/// This function splits the socket into a read half and a write half, and one
/// task drives each half. It does not multiplex both directions through one
/// `select!` over a single shared `Framed`. A combined task must `await` the
/// `framed.send(...)` flush *inside* a `select!` arm, and it does not poll the
/// read arm during that time. On a request/response connection the broker
/// stays silent until it receives the next request. A write that does not
/// complete in one poll therefore wedges the whole connection: the frame sits
/// buffered, no inbound traffic ever re-drives the loop, and the caller's
/// request never reaches the wire.
///
/// This is what made `crabka-client-consumer`'s group rejoin hang under the
/// jemalloc heap-profiling allocator. Its per-alloc sampling latency widened
/// that window enough to trip the hang deterministically. Independent halves
/// keep an inbound frame pollable while an outbound write is in flight, and
/// the reverse.
///
/// Liveness on teardown: when either task exits on EOF, an I/O error, a
/// dropped `Connection`, or `close()`, it cancels the shared `shutdown` token
/// so the other task also stops. The reader then fails every outstanding
/// request with `Disconnected`. A write-half failure therefore reaches
/// callers promptly instead of stalling them until the request timeout.
fn spawn_io_tasks(
    stream: Box<dyn ClientDuplex>,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    shutdown: CancellationToken,
    pending: Pending,
    frame_max: ClientFrameMax,
) -> (JoinHandle<()>, JoinHandle<()>) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::{FramedRead, FramedWrite};

    let (read_half, write_half) = tokio::io::split(stream);
    let mut framed_read = FramedRead::new(read_half, crate::transport::codec_with_max(frame_max));
    let mut framed_write =
        FramedWrite::new(write_half, crate::transport::codec_with_max(frame_max));

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
/// versions. The upstream `RequestHeader.json` schema marks the field as
/// `"flexibleVersions": "none"`, so even a v2 header keeps the i16-length
/// encoding. A UVARINT here makes the broker misread the length and throw
/// `InvalidRequestException` during header parsing.
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
/// This is the bootstrap step inside `connect`. No version table exists yet,
/// so this function cannot use `Connection::send`. Every broker supports
/// version 0.
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

    let body_bytes = tokio::time::timeout(conn.inner.options.connect_timeout.to_std(), rx)
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
mod tests {
    use assert2::assert;
    use crabka_units::{
        ByteSize, bytes, convert::ByteSizeExt as _, kibibytes, mebibytes, micros, millis,
    };

    use super::*;

    #[test]
    fn client_dns_timeout_validates_and_preserves_milliseconds() {
        let timeout = ClientDnsTimeout::new(millis(37)).expect("positive timeout");
        assert!(timeout.time() == millis(37));
        assert!(timeout.milliseconds() == 37);
        assert!(ClientDnsTimeout::new(Time::ZERO).is_err());
        assert!(ClientDnsTimeout::new(micros(1)).is_err());
        assert!(ClientDnsTimeout::new(millis(1) + micros(1)).is_err());
    }

    #[test]
    fn connection_options_own_named_defaults() {
        let options = ConnectionOptions::default();
        assert!(DEFAULT_CLIENT_DNS_TIMEOUT == secs(10));
        assert!(DEFAULT_CLIENT_CONNECT_TIMEOUT == secs(30));
        assert!(DEFAULT_CLIENT_REQUEST_TIMEOUT == secs(30));
        assert!(options.dns_timeout == ClientDnsTimeout::default());
        assert!(options.dns_timeout.time() == DEFAULT_CLIENT_DNS_TIMEOUT);
        assert!(options.connect_timeout == DEFAULT_CLIENT_CONNECT_TIMEOUT);
        assert!(options.request_timeout == DEFAULT_CLIENT_REQUEST_TIMEOUT);
    }

    #[test]
    fn connection_resource_defaults_preserve_existing_values() {
        assert!(
            ConnectionDispatchQueueCapacity::default().get()
                == DEFAULT_CONNECTION_DISPATCH_QUEUE_CAPACITY
        );
        assert!(ConnectionDispatchQueueCapacity::default().get() == 64);
        assert!(ClientFrameMax::default().size() == mebibytes(100));
        assert!(MAX_CLIENT_FRAME_BYTES == mebibytes(100));
    }

    #[test]
    fn connection_resource_policy_validates_boundaries() {
        assert!(ConnectionDispatchQueueCapacity::new(0).is_err());
        assert!(ConnectionDispatchQueueCapacity::new(7).unwrap().get() == 7);

        assert!(ClientFrameMax::try_from(bytes(0)).is_err());
        assert!(ClientFrameMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
        assert!(ClientFrameMax::try_from(mebibytes(100) + bytes(1)).is_err());
        assert!(ClientFrameMax::try_from(kibibytes(32)).unwrap().size() == kibibytes(32));
    }
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
    use std::time::{Duration, Instant};

    use crabka_protocol::{
        Encode,
        owned::{
            api_versions_response::{ApiVersion, ApiVersionsResponse},
            metadata_request::MetadataRequest,
            metadata_response::MetadataResponse,
        },
    };
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
            request_timeout: secs(5),
            connect_timeout: secs(5),
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

    #[tokio::test]
    async fn no_response_request_does_not_capture_the_next_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            let handshake_len = socket.read_u32().await.unwrap();
            let mut handshake = vec![0u8; handshake_len as usize];
            socket.read_exact(&mut handshake).await.unwrap();
            let handshake_corr =
                i32::from_be_bytes([handshake[4], handshake[5], handshake[6], handshake[7]]);
            let mut handshake_body = BytesMut::new();
            ApiVersionsResponse {
                api_keys: vec![ApiVersion {
                    api_key: MetadataRequest::API_KEY,
                    min_version: 0,
                    max_version: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode(&mut handshake_body, 0)
            .unwrap();
            let mut handshake_response = BytesMut::new();
            handshake_response.put_i32(handshake_corr);
            handshake_response.put_slice(&handshake_body);
            socket
                .write_u32(u32::try_from(handshake_response.len()).unwrap())
                .await
                .unwrap();
            socket.write_all(&handshake_response).await.unwrap();
            socket.flush().await.unwrap();

            let one_way_len = socket.read_u32().await.unwrap();
            let mut one_way = vec![0u8; one_way_len as usize];
            socket.read_exact(&mut one_way).await.unwrap();
            let one_way_corr = i32::from_be_bytes([one_way[4], one_way[5], one_way[6], one_way[7]]);

            let request_len = socket.read_u32().await.unwrap();
            let mut request = vec![0u8; request_len as usize];
            socket.read_exact(&mut request).await.unwrap();
            let request_corr = i32::from_be_bytes([request[4], request[5], request[6], request[7]]);
            assert2::assert!(request_corr == one_way_corr.wrapping_add(1));

            let mut body = BytesMut::new();
            MetadataResponse::default().encode(&mut body, 0).unwrap();
            let mut response = BytesMut::new();
            response.put_i32(request_corr);
            response.put_slice(&body);
            socket
                .write_u32(u32::try_from(response.len()).unwrap())
                .await
                .unwrap();
            socket.write_all(&response).await.unwrap();
            socket.flush().await.unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let conn = Connection::from_stream(Box::new(stream), ConnectionOptions::default())
            .await
            .expect("plaintext from_stream completes");
        conn.send_no_response(MetadataRequest::default())
            .await
            .expect("one-way request enqueues");
        let response = conn
            .send(MetadataRequest::default())
            .await
            .expect("subsequent response remains correlated");
        assert2::assert!(response == MetadataResponse::default());

        conn.close();
        server.await.unwrap();
    }
}
