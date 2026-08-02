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

use crate::{
    engine::Engine,
    messages::{
        backend,
        frontend::{self, SSL_REQUEST_CODE, StartupPacket},
    },
    session::{self, SessionConfig},
};

/// How many low bits of a backend id the per-session counter occupies. The
/// remaining bits of the positive `int4` range carry [`PROCESS_TOKEN`].
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
/// `PostgreSQL` forks a backend per connection and the pid it announces in
/// `BackendKeyData` is the one `pg_backend_pid()` reports, so a client can
/// correlate a cancel request with the session it opened. crabka serves every
/// session from one OS process, so a counter — not the process id — is what
/// distinguishes them, and one counter serves the whole process so an engine
/// opening a session with no client behind it (`Engine::connect`) draws an id
/// that cannot collide with a connected session's.
///
/// The id is **not** only a session label: `pg_temp_<backend id>` names this
/// session's temporary namespace in a catalog every gateway of a cluster
/// shares, so a bare per-process counter would have two gateways name one
/// namespace. Folding [`PROCESS_TOKEN`] into the high bits keeps the id inside
/// `int4` — the width `BackendKeyData` and `CancelRequest` fix — while making
/// it a cluster-wide name rather than a process-local one.
///
/// The split costs id space in both directions: 32768 sessions per process
/// before the counter wraps and repeats an id, and one chance in 65535 that
/// two processes draw the same token. Neither can destroy data on its own —
/// a temporary namespace is only ever emptied by a session that holds the sole
/// claim on it — so what remains is that two sessions could share a namespace,
/// not that either could lose one.
pub fn next_backend_pid() -> i32 {
    let counter = NEXT_PID.fetch_add(1, Ordering::Relaxed) & BACKEND_COUNTER_MASK;
    (*PROCESS_TOKEN << BACKEND_COUNTER_BITS) | counter
}

/// Shared connection/activity accounting for lifecycle decisions outside pgwire.
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
    /// Build an activity tracker initialized as idle now and accepting sessions.
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

    /// Atomically stop admitting new sessions so a lifecycle monitor can suspend safely.
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

/// Admission was already closed by a suspend attempt.
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
/// `slot` holds the current query's [`CancellationToken`] and is replaced at
/// every `begin_query` call so a fired token cannot reach a later query.
///
/// `pending` closes the *extended-batch cancel window*: during an extended
/// message sequence (Parse → Bind → Describe → Execute) no engine future is
/// running, so a `CancelRequest` that arrives between messages would fire the
/// spent token from the previous query and then be silently lost when
/// `begin_query` replaces it.  Setting `pending = true` alongside the token
/// fire lets `begin_query` detect the race and immediately cancel the fresh
/// token — ensuring the next engine call sees a cancelled token right away.
///
/// Conformance note: this means a cancel that arrives while the session is
/// completely idle (no batch in flight) will also poison the next query.  Real
/// Postgres treats such a cancel as a no-op for future queries.  Matching
/// that behaviour exactly would require tracking whether an extended batch is
/// in progress; that refinement is deferred — for now the simpler
/// "best-effort" semantics are acceptable and the test suite covers both
/// outcomes.
struct CancelTarget {
    slot: Mutex<CancellationToken>,
    /// Set by `CancelRegistry::cancel`; consumed (one-shot) by
    /// `SessionCancel::begin_query` so that one cancel fires exactly one
    /// engine call.
    pending: AtomicBool,
}

/// Maps (`process_id`, `secret_key`) -> the running query's cancellation target.
///
/// The token inside the target is REPLACED at each query start so a fired
/// token never cancels a later query.  The `pending` flag survives the
/// replacement to handle cancels that race the extended-batch window.
#[derive(Default)]
pub struct CancelRegistry {
    sessions: Mutex<HashMap<(i32, i32), Arc<CancelTarget>>>,
}

impl CancelRegistry {
    /// Registers a new session; returns a guard that unregisters on drop,
    /// carrying the pid, secret, and a shared cancellation target.
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
    /// Silently ignores unknown keys, matching Postgres behaviour.
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
/// Holds the pid/secret announced to the client and the shared cancellation
/// target.  Automatically unregisters from the registry when dropped.
pub struct SessionCancel {
    pub pid: i32,
    pub secret: i32,
    target: Arc<CancelTarget>,
    registry: Arc<CancelRegistry>,
}

impl SessionCancel {
    /// Installs and returns a fresh [`CancellationToken`] for one query
    /// execution.  A previously fired token is replaced so it cannot cancel
    /// a subsequent query.
    ///
    /// If a `CancelRequest` arrived while no engine future was running (the
    /// extended-batch window), the `pending` flag will be set; this method
    /// consumes it and immediately cancels the fresh token so the next
    /// `tokio::select!` sees `cancelled()` right away.
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

/// Serve plaintext connections (no TLS). Convenience wrapper over [`serve_tls`].
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
/// When `tls` is `Some`, a client that sends an `SSLRequest` will be upgraded
/// to a TLS stream; all subsequent protocol bytes flow over TLS.  When `tls`
/// is `None`, an `SSLRequest` is answered with `'N'` (decline) and the
/// connection continues in plaintext — matching the existing behaviour of
/// [`serve`].
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

/// Serve a SINGLE already-accepted connection (the per-connection body of
/// [`serve_tls`]). Exposed so a front-end router (the cluster's leader-routing
/// layer) can serve a leader-local connection itself. `registry` is shared
/// across a server's connections so a Postgres `CancelRequest` on a separate
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
    handle_conn(stream, engine, config, registry, tls, activity).await
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
                return startup_loop(tls_stream, buf, engine, config, registry, activity).await;
            }
            None => {
                stream.write_all(b"N").await?;
                // Fall through to plaintext startup_loop below.
            }
        }
    }

    startup_loop(stream, buf, engine, config, registry, activity).await
}

/// Post-TLS-decision startup loop, generic over the stream type.
///
/// Handles the remaining startup packets (`GssEncRequest` → 'N', `CancelRequest`,
/// Startup) on any stream that implements `AsyncRead + AsyncWrite + Unpin`.
/// A second `SSLRequest` (or one received over TLS) is declined with 'N' and
/// the loop continues — the client may then send a normal Startup.
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
