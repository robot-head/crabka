//! Single-broker `Connection`: TCP socket + reader/writer tasks +
//! correlation-ID multiplexing.

// Fields on `ConnectionInner` and `DispatchItem` are used in Tasks 9 and 10.
// `Ordering` and `ProtocolRequest` are needed in Task 10's `send` method.
#![allow(dead_code, unused_imports)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::request::ProtocolRequest;
use crate::version::ApiVersionTable;

type Pending = Arc<DashMap<i32, oneshot::Sender<Result<Bytes, ClientError>>>>;

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

// Forward declarations — bodies arrive in Tasks 9 and 10.
fn spawn_io_tasks(
    _stream: TcpStream,
    _writer_rx: mpsc::Receiver<DispatchItem>,
    _shutdown: CancellationToken,
    _pending: Pending,
) -> (JoinHandle<()>, JoinHandle<()>) {
    unimplemented!("Task 9: reader/writer tasks")
}

async fn fetch_api_versions(_conn: &Connection) -> Result<ApiVersionTable, ClientError> {
    // Task 10 will add await points; for now yield once so the function is
    // validly async without the `unused_async` lint firing.
    tokio::task::yield_now().await;
    unimplemented!("Task 10: bootstrap api-versions fetch")
}
