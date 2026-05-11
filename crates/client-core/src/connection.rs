//! Single-broker `Connection`: TCP socket + reader/writer tasks +
//! correlation-ID multiplexing.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::request::ProtocolRequest;
use crate::version::ApiVersionTable;

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
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            client_id: "crabka".into(),
            connect_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
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

        // Build the framed socket; spawn reader + writer.
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

        // Bootstrap-time `ApiVersions` fetch. Fills the version table.
        let versions = fetch_api_versions(&conn).await?;
        // Replace the empty table with the populated one.
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
        let flexible = version >= R::FLEXIBLE_MIN;
        let mut frame = build_request_header(
            R::API_KEY,
            version,
            corr_id,
            &self.inner.options.client_id,
            flexible,
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
        let uses_flexible_resp_header = flexible && R::API_KEY != API_VERSIONS_KEY;
        if uses_flexible_resp_header && !cursor.is_empty() {
            // Consume the tagged-fields byte (always 0x00 in practice).
            cursor = &cursor[1..];
        }

        let resp = <R::Response as crabka_protocol::Decode>::decode(&mut cursor, version)?;
        Ok(resp)
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
    stream: TcpStream,
    mut writer_rx: mpsc::Receiver<DispatchItem>,
    shutdown: CancellationToken,
    pending: Pending,
) -> (JoinHandle<()>, JoinHandle<()>) {
    use futures_util::{SinkExt, StreamExt};

    let mut framed = crate::transport::frame(stream);
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
/// - `flexible = false` → `RequestHeader v1`: `client_id` as `NULLABLE_STRING` (i16 length).
/// - `flexible = true`  → `RequestHeader v2`: `client_id` as `COMPACT_NULLABLE_STRING`
///   (UVARINT length = `len + 1`) followed by an empty tagged-fields byte.
fn build_request_header(
    api_key: i16,
    version: i16,
    corr_id: i32,
    client_id: &str,
    flexible: bool,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(32);
    buf.put_i16(api_key);
    buf.put_i16(version);
    buf.put_i32(corr_id);
    if flexible {
        // COMPACT_NULLABLE_STRING: length = byte_len + 1 as UVARINT.
        let bytes = client_id.as_bytes();
        let n = u32::try_from(bytes.len() + 1).expect("client_id fits in u32");
        crate::transport::put_uvarint(&mut buf, n);
        buf.put_slice(bytes);
        buf.put_u8(0); // empty tagged fields
    } else {
        let n = i16::try_from(client_id.len()).expect("client_id fits in i16");
        buf.put_i16(n);
        buf.put_slice(client_id.as_bytes());
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

    // v0 is non-flexible; ResponseHeader is also v0 (just corr_id, already stripped).
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
