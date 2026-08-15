//! TCP accept loop and pre-startup negotiation.

use std::{
    collections::HashMap,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::BytesMut;
use rand::RngExt;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    engine::Engine,
    messages::{
        backend,
        frontend::{self, SSL_REQUEST_CODE, StartupPacket},
    },
    session::{self, SessionConfig},
    telemetry,
};

/// How many low bits of a backend id the per-session counter occupies. The
/// remaining bits of the positive `int4` range hold [`PROCESS_TOKEN`].
const BACKEND_COUNTER_BITS: u32 = 15;

/// The counter half of a backend id, so the composed value always fits the
/// positive `int4` range a client may send back in a `CancelRequest`.
const BACKEND_COUNTER_MASK: i32 = (1 << BACKEND_COUNTER_BITS) - 1;

static NEXT_PID: AtomicI32 = AtomicI32::new(0);

/// The high half every backend id this process announces carries.
///
/// Drawn once per process, never zero, so the composed id is positive and two
/// processes serving one cluster do not hand out the same id. This is the same
/// device the range layer already uses to tell one node's notification records
/// from another's: `--range-listen` is a bind specification rather than a
/// resolved address, so there is no stable node number to fold in and a random
/// per-process draw is what distinguishes processes. A bounded
/// `CRABKA_BACKEND_PROCESS_TOKEN` override exists for deterministic integration
/// tests; production deployments must leave it unset unless their orchestrator
/// assigns a unique token to every live process.
static PROCESS_TOKEN: LazyLock<i32> = LazyLock::new(|| {
    configured_process_token(
        std::env::var("CRABKA_BACKEND_PROCESS_TOKEN")
            .ok()
            .as_deref(),
    )
    .unwrap_or_else(|| rand::rng().random_range(1..=(i32::MAX >> BACKEND_COUNTER_BITS)))
});

fn configured_process_token(value: Option<&str>) -> Option<i32> {
    value?
        .parse::<i32>()
        .ok()
        .filter(|token| (1..=(i32::MAX >> BACKEND_COUNTER_BITS)).contains(token))
}

/// Allocate the backend process id that identifies one session.
///
/// `PostgreSQL` forks a backend per connection, and the pid it announces in
/// `BackendKeyData` is the one `pg_backend_pid()` reports, so a client can
/// correlate a cancel request with the session it opened. crabka serves every
/// session from one OS process, so a counter distinguishes sessions instead of
/// the process id. One counter serves the whole process, so an engine that
/// opens a session with no client behind it (`Engine::connect`) draws an id
/// that cannot collide with a connected session's id.
///
/// The id is **not** only a session label. `pg_temp_<backend id>` names this
/// session's temporary namespace in a catalog that every gateway of a cluster
/// shares, so a bare per-process counter would let two gateways name one
/// namespace. [`PROCESS_TOKEN`] folded into the high bits keeps the id inside
/// `int4`, the width `BackendKeyData` and `CancelRequest` fix, and makes the
/// id a cluster-wide name rather than a process-local one.
///
/// The split costs id space in both directions: 32768 sessions per process
/// before the counter wraps and repeats an id, and one chance in 65535 that
/// two processes draw the same token. Neither one can destroy data on its own,
/// because only a session that holds the sole claim on a temporary namespace
/// ever empties it. What remains is that two sessions could share a namespace,
/// not that either could lose one.
pub fn next_backend_pid() -> i32 {
    let counter = NEXT_PID.fetch_add(1, Ordering::Relaxed) & BACKEND_COUNTER_MASK;
    (*PROCESS_TOKEN << BACKEND_COUNTER_BITS) | counter
}

/// Shared connection and activity accounting for lifecycle decisions outside
/// pgwire.
#[derive(Debug)]
pub struct ActivityTracker {
    accepting_sessions: AtomicBool,
    open_sessions: AtomicUsize,
    last_activity_millis: AtomicU64,
    maintenance_gate: Arc<tokio::sync::RwLock<()>>,
}

impl Default for ActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ActivityTracker {
    /// Build an activity tracker that starts idle now and accepts sessions.
    #[must_use]
    pub fn new() -> Self {
        Self::with_last_activity_unix_millis(current_unix_millis())
    }

    /// Build an activity tracker with a caller-supplied last activity timestamp.
    #[must_use]
    pub fn with_last_activity_unix_millis(last_activity_unix_millis: u64) -> Self {
        Self {
            accepting_sessions: AtomicBool::new(true),
            open_sessions: AtomicUsize::new(0),
            last_activity_millis: AtomicU64::new(last_activity_unix_millis),
            maintenance_gate: Arc::new(tokio::sync::RwLock::new(())),
        }
    }

    /// Return the number of authenticated or authenticating pgwire sessions.
    #[must_use]
    pub fn open_sessions(&self) -> usize {
        self.open_sessions.load(Ordering::SeqCst)
    }

    /// Return the last SQL activity timestamp in Unix milliseconds.
    #[must_use]
    pub fn last_activity_unix_millis(&self) -> u64 {
        self.last_activity_millis.load(Ordering::SeqCst)
    }

    /// Record SQL activity at the current wall-clock instant.
    pub fn touch(&self) {
        self.last_activity_millis
            .store(current_unix_millis(), Ordering::SeqCst);
    }

    /// Exclude SQL statements while one background maintenance step mutates
    /// physical storage visible to their scans.
    pub async fn begin_maintenance(&self) -> tokio::sync::OwnedRwLockWriteGuard<()> {
        Arc::clone(&self.maintenance_gate).write_owned().await
    }

    /// Atomically stop the admission of new sessions, so a lifecycle monitor
    /// can suspend safely.
    ///
    /// # Errors
    ///
    /// Returns [`AcceptingAlreadyClosed`] when session admission was already
    /// closed.
    pub fn close_for_suspend(&self) -> Result<(), AcceptingAlreadyClosed> {
        self.accepting_sessions
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .map(|_| ())
            .map_err(|_| AcceptingAlreadyClosed)
    }

    /// Re-open session admission after an aborted suspend attempt.
    pub fn reopen_after_suspend_abort(&self) {
        self.accepting_sessions.store(true, Ordering::SeqCst);
    }

    /// Try to admit one new session and return a guard that closes it on drop.
    #[must_use]
    pub fn try_open_session(self: &Arc<Self>) -> Option<SessionActivity> {
        if !self.accepting_sessions.load(Ordering::SeqCst) {
            return None;
        }
        self.open_sessions.fetch_add(1, Ordering::SeqCst);
        if self.accepting_sessions.load(Ordering::SeqCst) {
            return Some(SessionActivity {
                tracker: Arc::clone(self),
            });
        }

        self.open_sessions.fetch_sub(1, Ordering::SeqCst);
        None
    }
}

/// A suspend attempt had already closed admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptingAlreadyClosed;

/// RAII guard for one open pgwire session.
pub struct SessionActivity {
    tracker: Arc<ActivityTracker>,
}

impl SessionActivity {
    pub(crate) async fn begin_statement(&self) -> StatementActivity {
        let guard = Arc::clone(&self.tracker.maintenance_gate)
            .read_owned()
            .await;
        self.tracker.touch();
        StatementActivity {
            tracker: Arc::clone(&self.tracker),
            _guard: guard,
        }
    }
}

pub(crate) struct StatementActivity {
    tracker: Arc<ActivityTracker>,
    _guard: tokio::sync::OwnedRwLockReadGuard<()>,
}

impl Drop for StatementActivity {
    fn drop(&mut self) {
        self.tracker.touch();
    }
}

impl Drop for SessionActivity {
    fn drop(&mut self) {
        self.tracker.open_sessions.fetch_sub(1, Ordering::SeqCst);
    }
}

fn current_unix_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

/// Combined cancellation target for one session.
///
/// `slot` holds the current query's [`CancellationToken`]. Every `begin_query`
/// call replaces it, so a fired token cannot reach a later query.
///
/// `pending` closes the *extended-batch cancel window*. During an extended
/// message sequence (Parse → Bind → Describe → Execute) no engine future runs.
/// A `CancelRequest` that arrives between messages would therefore fire the
/// spent token from the previous query, and `begin_query` would then silently
/// lose it at the replacement. `pending = true`, set together with the token
/// fire, lets `begin_query` detect the race and immediately cancel the fresh
/// token. The next engine call then sees a cancelled token right away.
///
/// Conformance note: a cancel that arrives while the session is completely
/// idle, with no batch in flight, also poisons the next query. Real Postgres
/// treats such a cancel as a no-op for future queries. An exact match would
/// need a record of whether an extended batch is in progress. That refinement
/// is deferred. For now the simpler "best-effort" semantics are acceptable,
/// and the test suite covers both outcomes.
struct CancelTarget {
    slot: Mutex<CancellationToken>,
    /// `CancelRegistry::cancel` sets this flag, and
    /// `SessionCancel::begin_query` consumes it once, so that one cancel fires
    /// exactly one engine call.
    pending: AtomicBool,
}

/// Maps (`process_id`, `secret_key`) -> the running query's cancellation target.
///
/// The registry REPLACES the token inside the target at each query start, so a
/// fired token never cancels a later query. The `pending` flag survives the
/// replacement and handles cancels that race the extended-batch window.
#[derive(Default)]
pub struct CancelRegistry {
    sessions: Mutex<HashMap<(i32, i32), Arc<CancelTarget>>>,
}

impl CancelRegistry {
    /// Registers a new session and returns a guard that unregisters on drop.
    /// The guard carries the pid, the secret, and a shared cancellation
    /// target.
    ///
    /// # Panics
    ///
    /// Panics if the session registry mutex is poisoned.
    pub fn register(self: &Arc<Self>) -> SessionCancel {
        let pid = next_backend_pid();
        let secret = rand::rng().random::<i32>();
        let target = Arc::new(CancelTarget {
            slot: Mutex::new(CancellationToken::new()),
            pending: AtomicBool::new(false),
        });
        self.sessions
            .lock()
            .expect("registry lock")
            .insert((pid, secret), Arc::clone(&target));
        SessionCancel {
            pid,
            secret,
            target,
            registry: Arc::clone(self),
        }
    }

    /// Fire the current query token for the given (pid, secret) and set the
    /// sticky `pending` flag so a cancel that races the extended-batch window
    /// is not lost.
    ///
    /// This method silently ignores unknown keys, which matches Postgres
    /// behaviour.
    ///
    /// # Panics
    ///
    /// Panics if the session registry or cancellation-token mutex is poisoned.
    pub fn cancel(&self, pid: i32, secret: i32) {
        if let Some(target) = self
            .sessions
            .lock()
            .expect("registry lock")
            .get(&(pid, secret))
        {
            target.pending.store(true, Ordering::SeqCst);
            target.slot.lock().expect("slot lock").cancel();
        }
    }
}

/// Per-session handle to the cancel registry.
///
/// The handle holds the pid and secret announced to the client, and the shared
/// cancellation target. It unregisters from the registry automatically when it
/// is dropped.
pub struct SessionCancel {
    pub pid: i32,
    pub secret: i32,
    target: Arc<CancelTarget>,
    registry: Arc<CancelRegistry>,
}

impl SessionCancel {
    /// Installs and returns a fresh [`CancellationToken`] for one query
    /// execution. This method replaces a previously fired token, so that token
    /// cannot cancel a later query.
    ///
    /// The `pending` flag is set if a `CancelRequest` arrived while no engine
    /// future was running, which is the extended-batch window. This method
    /// then consumes the flag and immediately cancels the fresh token, so the
    /// next `tokio::select!` sees `cancelled()` right away.
    ///
    /// # Panics
    ///
    /// Panics if the cancellation-token mutex is poisoned.
    #[must_use]
    pub fn begin_query(&self) -> CancellationToken {
        let fresh = CancellationToken::new();
        *self.target.slot.lock().expect("slot lock") = fresh.clone();
        if self.target.pending.swap(false, Ordering::SeqCst) {
            fresh.cancel();
        }
        fresh
    }
}

impl Drop for SessionCancel {
    fn drop(&mut self) {
        self.registry
            .sessions
            .lock()
            .expect("registry lock")
            .remove(&(self.pid, self.secret));
    }
}

/// Serve plaintext connections without TLS. This is a convenience wrapper over
/// [`serve_tls`].
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails.
pub async fn serve<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
) -> std::io::Result<()> {
    serve_tls(listener, engine, config, None).await
}

/// Serve connections with optional TLS upgrade support.
///
/// When `tls` is `Some`, the server upgrades a client that sends an
/// `SSLRequest` to a TLS stream, and all later protocol bytes flow over TLS.
/// When `tls` is `None`, the server answers an `SSLRequest` with `'N'` to
/// decline it, and the connection continues in plaintext. That matches the
/// existing behaviour of [`serve`].
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails.
pub async fn serve_tls<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    tls: Option<TlsAcceptor>,
) -> std::io::Result<()> {
    serve_tls_with_activity(
        listener,
        engine,
        config,
        tls,
        Arc::new(ActivityTracker::new()),
    )
    .await
}

/// Serve connections with shared activity tracking.
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails.
pub async fn serve_tls_with_activity<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    tls: Option<TlsAcceptor>,
    activity: Arc<ActivityTracker>,
) -> std::io::Result<()> {
    serve_tls_with_activity_until(
        listener,
        engine,
        config,
        tls,
        activity,
        CancellationToken::new(),
    )
    .await
}

/// Serve connections until `shutdown` is cancelled.
///
/// # Errors
///
/// Returns an I/O error when accepting a connection fails.
pub async fn serve_tls_with_activity_until<E: Engine>(
    listener: TcpListener,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    tls: Option<TlsAcceptor>,
    activity: Arc<ActivityTracker>,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let registry = Arc::new(CancelRegistry::default());
    loop {
        let (stream, peer) = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted?,
        };
        // Response messages are written in multiple small writes; without
        // TCP_NODELAY, Nagle + the peer's delayed ACK adds ~40ms per
        // extended-protocol round trip. Failure is harmless (the peer may
        // already have disconnected), so don't let it stop the accept loop.
        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!("set_nodelay on connection from {peer} failed: {e}");
        }
        let engine = Arc::clone(&engine);
        let config = Arc::clone(&config);
        let registry = Arc::clone(&registry);
        let activity = Arc::clone(&activity);
        let tls = tls.clone();
        // TODO(config-era): connection cap (Semaphore) and pre-auth read timeout — slowloris guard. Deliberately deferred in SP1.
        tokio::spawn(async move {
            if let Err(e) =
                serve_conn_with_activity(stream, engine, config, registry, tls, activity).await
            {
                tracing::debug!("connection from {peer} ended: {e}");
            }
        });
    }
}

/// Serve a SINGLE already-accepted connection, the per-connection body of
/// [`serve_tls`].
///
/// This function is public so a front-end router, the cluster's leader-routing
/// layer, can serve a leader-local connection itself. A server's connections
/// share one `registry`, so a Postgres `CancelRequest` on a separate
/// connection can find its target.
///
/// # Errors
///
/// Returns an I/O or protocol-handshake error while serving the connection.
pub async fn serve_conn<E: Engine>(
    stream: TcpStream,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
    tls: Option<TlsAcceptor>,
) -> std::io::Result<()> {
    serve_conn_with_activity(
        stream,
        engine,
        config,
        registry,
        tls,
        Arc::new(ActivityTracker::new()),
    )
    .await
}

/// Serve a single connection while updating a shared activity tracker.
///
/// Every accepted connection passes through this function. That includes the
/// accept loop's `tokio::spawn` and a front-end router that serves a
/// leader-local connection. This function therefore raises the connection's
/// `gres.session` span and instruments it over the whole connection future.
///
/// # Errors
///
/// Returns an I/O or protocol-handshake error while serving the connection.
pub async fn serve_conn_with_activity<E: Engine>(
    stream: TcpStream,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
    tls: Option<TlsAcceptor>,
    activity: Arc<ActivityTracker>,
) -> std::io::Result<()> {
    // `peer_addr` is a `getpeername` syscall and a disabled callsite still
    // evaluates its arguments, so the whole construction sits behind the check.
    let span = if tracing::enabled!(target: telemetry::SESSION_TARGET, tracing::Level::DEBUG) {
        telemetry::session_span(stream.peer_addr().ok())
    } else {
        tracing::Span::none()
    };
    handle_conn(stream, engine, config, registry, tls, activity)
        .instrument(span)
        .await
}

async fn handle_conn<E: Engine>(
    mut stream: TcpStream,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
    tls: Option<TlsAcceptor>,
    activity: Arc<ActivityTracker>,
) -> std::io::Result<()> {
    let mut buf = BytesMut::with_capacity(1024);

    // Phase 1: wait for at least the first packet header (8 bytes minimum for
    // any legal startup packet).  Peek at bytes [4..8] to detect SSLRequest
    // WITHOUT consuming the data — this lets non-SSLRequest packets fall
    // through to startup_loop with their bytes intact.
    while buf.len() < 8 {
        if stream.read_buf(&mut buf).await? == 0 {
            return Ok(()); // client disconnected before sending anything
        }
    }

    let code = i32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
    if code == SSL_REQUEST_CODE {
        // Consume the SSLRequest (exactly 8 bytes, fully buffered).
        // decode_startup cannot fail or return None here — len==8, code known.
        let _ = frontend::decode_startup(&mut buf);

        // SECURITY (CVE-2021-23222 class): any bytes pipelined after SSLRequest
        // arrived BEFORE the TLS handshake; processing them as if they came over
        // TLS lets an active MITM inject a chosen startup packet.  Real
        // PostgreSQL rejects this (it sends a fatal ErrorResponse first; we
        // simply close, which is equivalent for SP1 — fatal + close semantics).
        if !buf.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "junk after SSLRequest: refusing pre-handshake pipelined bytes",
            ));
        }

        match &tls {
            Some(acceptor) => {
                stream.write_all(b"S").await?;
                let tls_stream = acceptor.accept(stream).await?;
                telemetry::record_session_tls(&tracing::Span::current(), true);
                return startup_loop(tls_stream, buf, engine, config, registry, activity).await;
            }
            None => {
                stream.write_all(b"N").await?;
                // Fall through to plaintext startup_loop below.
            }
        }
    }

    telemetry::record_session_tls(&tracing::Span::current(), false);
    startup_loop(stream, buf, engine, config, registry, activity).await
}

/// Post-TLS-decision startup loop, generic over the stream type.
///
/// This function handles the remaining startup packets on any stream that
/// implements `AsyncRead + AsyncWrite + Unpin`: `GssEncRequest` → 'N',
/// `CancelRequest`, and Startup. It declines a second `SSLRequest`, or one
/// received over TLS, with 'N' and continues the loop. The client may then
/// send a normal Startup.
async fn startup_loop<S, E>(
    mut stream: S,
    mut buf: BytesMut,
    engine: Arc<E>,
    config: Arc<SessionConfig>,
    registry: Arc<CancelRegistry>,
    activity: Arc<ActivityTracker>,
) -> std::io::Result<()>
where
    S: AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    E: Engine,
{
    loop {
        match frontend::decode_startup(&mut buf) {
            Ok(Some(StartupPacket::SslRequest | StartupPacket::GssEncRequest)) => {
                // A second SSLRequest, GssEncRequest, or either over TLS:
                // decline; client may proceed with a normal Startup.
                stream.write_all(b"N").await?;
            }
            Ok(Some(StartupPacket::CancelRequest {
                process_id,
                secret_key,
            })) => {
                registry.cancel(process_id, secret_key);
                // Protocol says close without responding.
                return Ok(());
            }
            Ok(Some(StartupPacket::Startup { params })) => {
                let Some(activity_guard) = activity.try_open_session() else {
                    return Ok(());
                };
                let cancel = registry.register();
                // Pass the residual buffer so any bytes pipelined by the client
                // immediately after the startup packet are not silently dropped.
                return session::run_session(
                    stream,
                    params,
                    engine,
                    config,
                    cancel,
                    buf,
                    activity_guard,
                )
                .await;
            }
            Ok(None) => {
                if stream.read_buf(&mut buf).await? == 0 {
                    return Ok(()); // EOF
                }
            }
            Err(e) => {
                let mut out = BytesMut::new();
                backend::error_response(&mut out, &e);
                stream.write_all(&out).await?;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use super::{ActivityTracker, configured_process_token};

    #[test]
    fn configured_backend_process_token_is_bounded() {
        assert_eq!(configured_process_token(Some("1")), Some(1));
        assert_eq!(configured_process_token(Some("65535")), Some(65535));
        assert_eq!(configured_process_token(Some("0")), None);
        assert_eq!(configured_process_token(Some("65536")), None);
        assert_eq!(configured_process_token(Some("not-a-number")), None);
        assert_eq!(configured_process_token(None), None);
    }

    #[tokio::test]
    async fn maintenance_waits_for_active_statement() {
        let tracker = Arc::new(ActivityTracker::new());
        let session = tracker.try_open_session().expect("session admitted");
        let statement = session.begin_statement().await;
        let maintenance_tracker = Arc::clone(&tracker);
        let maintenance = tokio::spawn(async move {
            let _guard = maintenance_tracker.begin_maintenance().await;
        });

        tokio::task::yield_now().await;
        assert!(!maintenance.is_finished());
        drop(statement);
        tokio::time::timeout(Duration::from_secs(1), maintenance)
            .await
            .expect("maintenance unblocked")
            .expect("maintenance task");
    }
}
