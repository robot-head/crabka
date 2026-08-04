//! Trace ingress through the wire protocol.
//!
//! Every assertion here runs against exported [`SpanData`], never against a
//! live `tracing::Span` handle: `tracing-opentelemetry` resolves a re-parented
//! span's trace id when the span *closes*, not when `set_parent` runs, so a
//! handle reports the wrong parent even for a span that exports correctly.
//!
//! The session runs on a current-thread runtime driven inside
//! `tracing::subscriber::with_default`, so the thread-local subscriber is
//! visible to the session future — a multi-thread runtime would move it off the
//! test thread and every test would silently pass with zero spans.

use std::{collections::BTreeMap, future::Future, net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use bytes::BytesMut;
use crabka_pgwire::{
    engine::{
        BoundParam, Cell, CloseTarget, Engine, ExecuteOutcome, ExecuteOutcome::Rows,
        FieldDescription, PortalDescription, PreparedDescription, QueryResult, Session, TxStatus,
        oids,
    },
    error::{PgError, sqlstate},
    server::{ActivityTracker, CancelRegistry},
    session::{SessionConfig, run_session},
    telemetry::{IngressTracePolicy, session_span},
};
use opentelemetry::trace::{SpanId, SpanKind, Status, TraceId, TracerProvider as _};
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{InMemorySpanExporter, Sampler, SdkTracerProvider, SpanData},
};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _, DuplexStream};
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

// ── Fixtures ────────────────────────────────────────────────────────────────

const SAMPLED: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const UNSAMPLED: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";
const REMOTE_TRACE_ID: &str = "0af7651916cd43dd8448eb211c80319c";
const REMOTE_SPAN_ID: &str = "b7ad6b7169203331";
const PEER: &str = "203.0.113.7:54321";

fn remote_trace_id() -> TraceId {
    TraceId::from_hex(REMOTE_TRACE_ID).expect("fixture trace id")
}

fn remote_span_id() -> SpanId {
    SpanId::from_hex(REMOTE_SPAN_ID).expect("fixture span id")
}

/// `sql` with a sqlcommenter tag appended, as an instrumented driver emits it.
fn tagged(sql: &str, traceparent: &str) -> String {
    format!("{sql} /*traceparent='{traceparent}'*/")
}

fn startup_params() -> Vec<(String, String)> {
    [
        ("user", "crab"),
        ("database", "shop"),
        ("application_name", "checkout"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

// ── Engine ──────────────────────────────────────────────────────────────────

/// An engine that answers any statement with `row_count` rows, so a test can
/// pin the row/page counters, and fails any statement containing `boom` with a
/// syntax error, so a test can pin the error status.
struct TraceEngine {
    row_count: usize,
}

impl TraceEngine {
    fn new(row_count: usize) -> Self {
        Self { row_count }
    }
}

impl Engine for TraceEngine {
    type Session = TraceSession;

    fn connect(&self) -> Self::Session {
        TraceSession {
            row_count: self.row_count,
            prepared: BTreeMap::new(),
            portals: BTreeMap::new(),
        }
    }
}

struct TraceSession {
    row_count: usize,
    prepared: BTreeMap<String, String>,
    portals: BTreeMap<String, String>,
}

impl TraceSession {
    fn field() -> FieldDescription {
        FieldDescription {
            name: "n".to_owned(),
            table_oid: 0,
            column_id: 0,
            type_oid: oids::INT4,
            type_size: 4,
            type_modifier: -1,
            format: 0,
        }
    }

    fn cells(&self) -> Vec<Vec<Option<Cell>>> {
        (0..self.row_count)
            .map(|value| {
                let text = value.to_string();
                vec![Some(Cell {
                    text: text.clone().into(),
                    binary: text.into(),
                })]
            })
            .collect()
    }

    fn result(&self, sql: &str) -> Result<QueryResult, PgError> {
        if sql.contains("boom") {
            return Err(PgError::error(sqlstate::SYNTAX_ERROR, "boom"));
        }
        let rows = self.cells();
        Ok(QueryResult::Rows {
            fields: vec![Self::field()],
            tag: format!("SELECT {}", rows.len()),
            rows,
        })
    }
}

fn missing(kind: &str, name: &str) -> PgError {
    PgError::error(
        sqlstate::INVALID_CURSOR_NAME,
        format!("{kind} \"{name}\" does not exist"),
    )
}

impl Session for TraceSession {
    async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryResult>, PgError> {
        Ok(vec![self.result(sql)?])
    }

    async fn parse(
        &mut self,
        name: &str,
        sql: &str,
        _: &[u32],
    ) -> Result<PreparedDescription, PgError> {
        self.result(sql)?;
        self.prepared.insert(name.to_owned(), sql.to_owned());
        Ok(PreparedDescription {
            parameter_types: vec![],
            fields: vec![Self::field()],
        })
    }

    async fn bind(
        &mut self,
        portal: &str,
        statement: &str,
        _: &[BoundParam],
        _: &[i16],
    ) -> Result<PortalDescription, PgError> {
        let sql = self
            .prepared
            .get(statement)
            .ok_or_else(|| missing("prepared statement", statement))?
            .clone();
        self.portals.insert(portal.to_owned(), sql);
        Ok(PortalDescription {
            fields: vec![Self::field()],
        })
    }

    async fn describe_statement(&mut self, name: &str) -> Result<PreparedDescription, PgError> {
        self.prepared
            .get(name)
            .ok_or_else(|| missing("prepared statement", name))?;
        Ok(PreparedDescription {
            parameter_types: vec![],
            fields: vec![Self::field()],
        })
    }

    async fn describe_portal(&mut self, name: &str) -> Result<PortalDescription, PgError> {
        self.portals
            .get(name)
            .ok_or_else(|| missing("portal", name))?;
        Ok(PortalDescription {
            fields: vec![Self::field()],
        })
    }

    async fn execute(&mut self, portal: &str, _: u32) -> Result<ExecuteOutcome, PgError> {
        let sql = self
            .portals
            .get(portal)
            .ok_or_else(|| missing("portal", portal))?
            .clone();
        let QueryResult::Rows { rows, tag, .. } = self.result(&sql)? else {
            unreachable!("the fake engine only produces row results")
        };
        Ok(Rows {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|cell| cell.map(|cell| cell.text))
                        .collect()
                })
                .collect(),
            completion: Some(tag),
        })
    }

    async fn close(&mut self, _: CloseTarget<'_>) -> Result<(), PgError> {
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), PgError> {
        self.portals.clear();
        Ok(())
    }

    fn tx_status(&self) -> TxStatus {
        TxStatus::Idle
    }
}

// ── Harness ─────────────────────────────────────────────────────────────────

/// The client half of one session, plus the handle a test needs to cancel a
/// statement out of band.
struct Wire<S> {
    client: S,
    cancel: Option<CancelHandle>,
}

/// Everything needed to raise a `CancelRequest` against a session the harness
/// registered itself. Absent when the server registers its own target.
struct CancelHandle {
    registry: Arc<CancelRegistry>,
    pid: i32,
    secret: i32,
}

fn framed(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut message = vec![tag];
    let length = i32::try_from(body.len() + 4).expect("message length fits");
    message.extend_from_slice(&length.to_be_bytes());
    message.extend_from_slice(body);
    message
}

fn cstr(value: &str) -> Vec<u8> {
    let mut out = value.as_bytes().to_vec();
    out.push(0);
    out
}

/// A protocol-3.0 `StartupMessage` carrying `params`.
fn startup_packet(params: &[(String, String)]) -> Vec<u8> {
    let mut body = 196_608i32.to_be_bytes().to_vec();
    for (key, value) in params {
        body.extend_from_slice(&cstr(key));
        body.extend_from_slice(&cstr(value));
    }
    body.push(0);
    let mut packet = i32::try_from(body.len() + 4)
        .expect("startup packet length fits")
        .to_be_bytes()
        .to_vec();
    packet.extend_from_slice(&body);
    packet
}

impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Wire<S> {
    async fn send(&mut self, message: &[u8]) {
        self.client.write_all(message).await.expect("client write");
    }

    async fn read_message(&mut self) -> (u8, Vec<u8>) {
        let mut header = [0u8; 5];
        self.client
            .read_exact(&mut header)
            .await
            .expect("message header");
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        let body_len = usize::try_from(length - 4).expect("body length is non-negative");
        let mut body = vec![0u8; body_len];
        self.client
            .read_exact(&mut body)
            .await
            .expect("message body");
        (header[0], body)
    }

    /// Drain the server's startup burst, ending at the first `ReadyForQuery`.
    async fn startup(&mut self) {
        loop {
            if self.read_message().await.0 == b'Z' {
                return;
            }
        }
    }

    /// Drain everything up to and including the next `ReadyForQuery`.
    async fn drain_to_ready(&mut self) {
        loop {
            if self.read_message().await.0 == b'Z' {
                return;
            }
        }
    }

    async fn query(&mut self, sql: &str) {
        let message = framed(b'Q', &cstr(sql));
        self.send(&message).await;
        self.drain_to_ready().await;
    }

    async fn parse(&mut self, name: &str, sql: &str) {
        let mut body = cstr(name);
        body.extend_from_slice(&cstr(sql));
        body.extend_from_slice(&0i16.to_be_bytes());
        let message = framed(b'P', &body);
        self.send(&message).await;
    }

    async fn bind(&mut self, portal: &str, statement: &str) {
        let mut body = cstr(portal);
        body.extend_from_slice(&cstr(statement));
        // No parameter formats, no parameters, no result formats.
        body.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
        let message = framed(b'B', &body);
        self.send(&message).await;
    }

    async fn execute(&mut self, portal: &str) {
        let mut body = cstr(portal);
        body.extend_from_slice(&0i32.to_be_bytes());
        let message = framed(b'E', &body);
        self.send(&message).await;
    }

    async fn describe(&mut self, kind: u8, name: &str) {
        let mut body = vec![kind];
        body.extend_from_slice(&cstr(name));
        let message = framed(b'D', &body);
        self.send(&message).await;
    }

    async fn sync(&mut self) {
        let message = framed(b'S', &[]);
        self.send(&message).await;
        self.drain_to_ready().await;
    }

    /// Issue a `CancelRequest` for this session out of band.
    ///
    /// Sent while the session is idle, so the registry's sticky `pending` flag
    /// fires it at the next statement — which is what makes the cancellation
    /// deterministic instead of a race with a sleeping engine.
    fn request_cancel(&self) {
        let handle = self
            .cancel
            .as_ref()
            .expect("this harness owns the session's cancel target");
        handle.registry.cancel(handle.pid, handle.secret);
    }

    async fn terminate(mut self) {
        let message = framed(b'X', &[]);
        self.send(&message).await;
    }
}

/// The spans one scripted session exported, plus the values the script cannot
/// know for itself.
struct Traced {
    spans: Vec<SpanData>,
    backend_pid: i32,
}

impl Traced {
    fn all(&self, name: &str) -> Vec<&SpanData> {
        self.spans.iter().filter(|span| span.name == name).collect()
    }

    fn span(&self, name: &str) -> &SpanData {
        let found = self.all(name);
        assert!(found.len() == 1, "expected exactly one {name}: {found:?}");
        found[0]
    }
}

/// The span's own attributes, with the bookkeeping `tracing-opentelemetry` adds
/// to every span (source location, thread, busy/idle timings, target) filtered
/// out — so a whole-map comparison stays a statement about what pgwire records.
fn attributes(span: &SpanData) -> BTreeMap<String, String> {
    const LAYER_KEYS: [&str; 3] = ["busy_ns", "idle_ns", "target"];
    span.attributes
        .iter()
        .map(|kv| (kv.key.as_str().to_owned(), kv.value.to_string()))
        .filter(|(key, _)| {
            !key.starts_with("code.")
                && !key.starts_with("thread.")
                && !LAYER_KEYS.contains(&key.as_str())
        })
        .collect()
}

fn attribute(span: &SpanData, key: &str) -> Option<String> {
    attributes(span).get(key).cloned()
}

/// Run `script` against a real session under `sampler` and `policy`, and return
/// what the collector received.
fn traced<Fut>(
    sampler: Sampler,
    policy: IngressTracePolicy,
    row_count: usize,
    script: impl FnOnce(Wire<DuplexStream>) -> Fut,
) -> Traced
where
    Fut: Future<Output = ()>,
{
    let mut backend_pid = 0;
    let spans = collecting(sampler, |runtime| {
        runtime.block_on(async {
            let (client, server) = tokio::io::duplex(1 << 20);
            let registry = Arc::new(CancelRegistry::default());
            let cancel = registry.register();
            let (pid, secret) = (cancel.pid, cancel.secret);
            backend_pid = pid;
            let activity = Arc::new(ActivityTracker::new())
                .try_open_session()
                .expect("session admission is open");
            let peer: SocketAddr = PEER.parse().expect("fixture peer address");
            let serving = run_session(
                server,
                startup_params(),
                Arc::new(TraceEngine::new(row_count)),
                Arc::new(SessionConfig {
                    ingress_trace: policy,
                    ..SessionConfig::trust()
                }),
                cancel,
                BytesMut::new(),
                activity,
            )
            .instrument(session_span(Some(peer)));

            let driving = async move {
                let mut wire = Wire {
                    client,
                    cancel: Some(CancelHandle {
                        registry,
                        pid,
                        secret,
                    }),
                };
                wire.startup().await;
                script(wire).await;
            };

            let (outcome, ()) = tokio::join!(serving, driving);
            outcome.expect("the session ends without an I/O error");
        });
    });

    Traced { spans, backend_pid }
}

/// Install an in-memory exporter and a current-thread runtime, run `body`, and
/// return everything the exporter received.
fn collecting(sampler: Sampler, body: impl FnOnce(&tokio::runtime::Runtime)) -> Vec<SpanData> {
    // `TraceCarrier` extraction runs through the global text-map propagator,
    // which is a no-op until one is installed.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_simple_exporter(exporter.clone())
        .build();
    let tracer = provider.tracer("pgwire-ingress-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");

    tracing::subscriber::with_default(subscriber, || body(&runtime));

    provider.force_flush().expect("flush the exporter");
    exporter.get_finished_spans().expect("collected spans")
}

/// The common case: always-on sampling and the default ingress policy.
fn trust_traced<Fut>(script: impl FnOnce(Wire<DuplexStream>) -> Fut) -> Traced
where
    Fut: Future<Output = ()>,
{
    traced(Sampler::AlwaysOn, IngressTracePolicy::Trust, 1, script)
}

// ── Session span ────────────────────────────────────────────────────────────

/// The one test that goes through `server::serve_conn_with_activity` — the
/// funnel every accepted connection passes through, and the only place the
/// session span is actually raised in production. Everything else here
/// instruments `run_session` directly.
#[test]
fn serving_a_real_connection_raises_a_session_span_for_its_peer() {
    let mut client_port = 0;
    let spans = collecting(Sampler::AlwaysOn, |runtime| {
        runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a loopback listener");
            let address = listener.local_addr().expect("listener address");
            let (accepted, connected) =
                tokio::join!(listener.accept(), tokio::net::TcpStream::connect(address));
            let (server, _) = accepted.expect("accept the connection");
            let client = connected.expect("connect to the listener");
            client_port = client.local_addr().expect("client address").port();

            let serving = crabka_pgwire::server::serve_conn_with_activity(
                server,
                Arc::new(TraceEngine::new(1)),
                Arc::new(SessionConfig::trust()),
                Arc::new(CancelRegistry::default()),
                None,
                Arc::new(ActivityTracker::new()),
            );
            let driving = async move {
                let mut wire = Wire {
                    client,
                    cancel: None,
                };
                wire.send(&startup_packet(&startup_params())).await;
                wire.startup().await;
                wire.query("SELECT 1").await;
                wire.terminate().await;
            };

            let (outcome, ()) = tokio::join!(serving, driving);
            outcome.expect("the connection ends without an I/O error");
        });
    });

    let session = spans
        .iter()
        .find(|span| span.name == "gres.session")
        .expect("the connection raised a session span");
    let recorded = attributes(session);
    check!(recorded.get("network.peer.address").map(String::as_str) == Some("127.0.0.1"));
    check!(recorded.get("network.peer.port") == Some(&client_port.to_string()));
    check!(recorded.get("pg.tls").map(String::as_str) == Some("false"));
    check!(recorded.get("db.user").map(String::as_str) == Some("crab"));
    check!(session.span_kind == SpanKind::Server);

    // The statement raised inside it is a child, so a connection's statements
    // are reachable from the session that ran them.
    let statement = spans
        .iter()
        .find(|span| span.name == "gres.statement")
        .expect("the query raised a statement span");
    check!(statement.parent_span_id == session.span_context.span_id());
}

#[test]
fn the_session_span_carries_the_connection_and_startup_attributes() {
    let traced = trust_traced(|mut wire| async move {
        wire.query("SELECT 1").await;
        wire.terminate().await;
    });

    let session = traced.span("gres.session");
    let expected: BTreeMap<String, String> = [
        ("db.client.application_name", "checkout"),
        ("db.namespace", "shop"),
        ("db.system.name", "postgresql"),
        ("db.user", "crab"),
        ("network.peer.address", "203.0.113.7"),
        ("network.peer.port", "54321"),
        ("pg.backend_pid", &traced.backend_pid.to_string()),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect();

    check!(attributes(session) == expected);
    check!(session.span_kind == SpanKind::Server);
    check!(session.status == Status::Unset);
}

// ── Simple protocol ─────────────────────────────────────────────────────────

#[test]
fn a_sqlcommenter_tag_parents_the_statement_into_the_client_trace() {
    let traced = trust_traced(|mut wire| async move {
        wire.query(&tagged("SELECT 1", SAMPLED)).await;
        wire.terminate().await;
    });

    let statement = traced.span("gres.statement");
    check!(statement.span_context.trace_id() == remote_trace_id());
    check!(statement.parent_span_id == remote_span_id());
    check!(statement.parent_span_is_remote);
    check!(statement.links.is_empty());
    check!(statement.span_kind == SpanKind::Server);
    check!(attribute(statement, "pg.protocol") == Some("simple".to_owned()));
}

#[test]
fn an_untagged_statement_stays_inside_the_session_trace() {
    let traced = trust_traced(|mut wire| async move {
        wire.query("SELECT 1").await;
        wire.terminate().await;
    });

    let session = traced.span("gres.session");
    let statement = traced.span("gres.statement");
    check!(statement.span_context.trace_id() == session.span_context.trace_id());
    check!(statement.parent_span_id == session.span_context.span_id());
    check!(!statement.parent_span_is_remote);
    check!(statement.links.is_empty());
}

/// A hostile `traceparent` must be discarded silently: the query it rode in on
/// still runs, and the statement stays in the server's own trace.
#[test]
fn a_malformed_traceparent_is_ignored_and_the_statement_still_succeeds() {
    let malformed = [
        "not-a-traceparent",
        "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        "00-00000000000000000000000000000000-b7ad6b7169203331-01",
        "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
    ];

    for traceparent in malformed {
        let traced = trust_traced(|mut wire| async move {
            wire.query(&tagged("SELECT 1", traceparent)).await;
            wire.terminate().await;
        });

        let session = traced.span("gres.session");
        let statement = traced.span("gres.statement");
        check!(
            statement.parent_span_id == session.span_context.span_id(),
            "{traceparent}"
        );
        // The statement ran: no error status, and the engine's rows were
        // counted, which only happens on the success path.
        check!(statement.status == Status::Unset, "{traceparent}");
        check!(
            attribute(statement, "db.response.returned_rows") == Some("1".to_owned()),
            "{traceparent}"
        );
    }
}

#[test]
fn rows_and_pages_are_summarised_once_on_the_statement_span() {
    // Above the wire layer's 1024-row page size, so the sink really does run its
    // loop more than once — and still emits no span of its own.
    let traced = traced(
        Sampler::AlwaysOn,
        IngressTracePolicy::Trust,
        1500,
        |mut wire| async move {
            wire.query("SELECT n").await;
            wire.terminate().await;
        },
    );

    let statement = traced.span("gres.statement");
    check!(attribute(statement, "db.response.returned_rows") == Some("1500".to_owned()));
    check!(attribute(statement, "pg.result_pages") == Some("2".to_owned()));
    // No page spans: the whole result is one span, whatever its size.
    check!(traced.spans.len() == 2);
}

#[test]
fn a_failing_statement_records_error_status_and_its_sqlstate() {
    let traced = trust_traced(|mut wire| async move {
        wire.query("SELECT boom").await;
        wire.terminate().await;
    });

    let statement = traced.span("gres.statement");
    check!(statement.status == Status::error("boom"));
    check!(attribute(statement, "error.type") == Some(sqlstate::SYNTAX_ERROR.to_owned()));
    check!(
        attribute(statement, "db.response.status_code") == Some(sqlstate::SYNTAX_ERROR.to_owned())
    );
    check!(attribute(statement, "pg.canceled").is_none());
    // A non-fatal statement error does not end the connection, so the session
    // span stays unset.
    check!(traced.span("gres.session").status == Status::Unset);
}

#[test]
fn a_canceled_statement_is_an_error_carrying_the_cancel_flag() {
    let traced = trust_traced(|mut wire| async move {
        wire.request_cancel();
        wire.query("SELECT 1").await;
        wire.terminate().await;
    });

    let statement = traced.span("gres.statement");
    check!(statement.status == Status::error("canceling statement due to user request"));
    check!(attribute(statement, "error.type") == Some(sqlstate::QUERY_CANCELED.to_owned()));
    check!(attribute(statement, "pg.canceled") == Some("true".to_owned()));
}

// ── Ingress policy ──────────────────────────────────────────────────────────

#[test]
fn policy_off_ignores_a_valid_traceparent() {
    let traced = traced(
        Sampler::AlwaysOn,
        IngressTracePolicy::Off,
        1,
        |mut wire| async move {
            wire.query(&tagged("SELECT 1", SAMPLED)).await;
            wire.terminate().await;
        },
    );

    let session = traced.span("gres.session");
    let statement = traced.span("gres.statement");
    check!(statement.span_context.trace_id() != remote_trace_id());
    check!(statement.parent_span_id == session.span_context.span_id());
    check!(statement.links.is_empty());
}

#[test]
fn policy_link_correlates_without_ceding_the_parent() {
    let traced = traced(
        Sampler::AlwaysOn,
        IngressTracePolicy::Link,
        1,
        |mut wire| async move {
            wire.query(&tagged("SELECT 1", SAMPLED)).await;
            wire.terminate().await;
        },
    );

    let session = traced.span("gres.session");
    let statement = traced.span("gres.statement");
    let linked: Vec<_> = statement
        .links
        .iter()
        .map(|link| (link.span_context.trace_id(), link.span_context.span_id()))
        .collect();

    check!(linked == vec![(remote_trace_id(), remote_span_id())]);
    check!(statement.span_context.trace_id() == session.span_context.trace_id());
    check!(statement.parent_span_id == session.span_context.span_id());
}

/// `Resample` must *recompute* the sampled flag, not clear it. Under
/// `ParentBased`, a non-sampled parent returns `Drop` outright — so clearing the
/// bit would discard exactly the statements a client had instrumented, which is
/// what the `Trust` half of each pair demonstrates going the other way.
#[test]
fn resample_recomputes_the_sampled_flag_rather_than_honouring_it() {
    let cases: [(&str, IngressTracePolicy, f64, &str, bool); 4] = [
        (
            "a full ratio promotes an unsampled client trace",
            IngressTracePolicy::resample(1.0),
            1.0,
            UNSAMPLED,
            true,
        ),
        (
            "trusting that same trace drops it",
            IngressTracePolicy::Trust,
            1.0,
            UNSAMPLED,
            false,
        ),
        (
            "a zero ratio demotes a sampled client trace",
            IngressTracePolicy::resample(0.0),
            0.0,
            SAMPLED,
            false,
        ),
        (
            "trusting that same trace keeps it",
            IngressTracePolicy::Trust,
            0.0,
            SAMPLED,
            true,
        ),
    ];

    for (name, policy, ratio, traceparent, exported) in cases {
        let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)));
        let traced = traced(sampler, policy, 1, |mut wire| async move {
            wire.query(&tagged("SELECT 1", traceparent)).await;
            wire.terminate().await;
        });

        let statements = traced.all("gres.statement");
        check!(statements.len() == usize::from(exported), "{name}");
        if let Some(statement) = statements.first() {
            check!(
                statement.span_context.trace_id() == remote_trace_id(),
                "{name}"
            );
            check!(statement.parent_span_id == remote_span_id(), "{name}");
        }
    }
}

// ── Extended protocol ───────────────────────────────────────────────────────

#[test]
fn execute_inherits_the_trace_captured_at_parse() {
    let traced = trust_traced(|mut wire| async move {
        wire.parse("", &tagged("SELECT n", SAMPLED)).await;
        wire.bind("", "").await;
        wire.describe(b'P', "").await;
        wire.execute("").await;
        wire.sync().await;
        wire.terminate().await;
    });

    let parse = traced.span("gres.parse");
    let statement = traced.span("gres.statement");
    check!(parse.span_context.trace_id() == remote_trace_id());
    check!(parse.parent_span_id == remote_span_id());
    check!(statement.span_context.trace_id() == remote_trace_id());
    check!(statement.parent_span_id == remote_span_id());
    check!(statement.parent_span_is_remote);
    check!(attribute(statement, "pg.protocol") == Some("extended".to_owned()));
    check!(attribute(statement, "db.response.returned_rows") == Some("1".to_owned()));

    // Bind and Describe are their own internal spans under the session.
    let session = traced.span("gres.session");
    for name in ["gres.bind", "gres.describe"] {
        let span = traced.span(name);
        check!(
            span.parent_span_id == session.span_context.span_id(),
            "{name}"
        );
        check!(span.span_kind == SpanKind::Internal, "{name}");
    }
}

/// A named statement surviving a `Sync` is genuinely being reused, and the trace
/// it was prepared under is stale by the time it runs again.
#[test]
fn a_named_statement_reused_after_sync_does_not_inherit_the_stale_trace() {
    let traced = trust_traced(|mut wire| async move {
        wire.parse("stmt", &tagged("SELECT n", SAMPLED)).await;
        wire.bind("first", "stmt").await;
        wire.execute("first").await;
        wire.sync().await;

        // Second use of the same prepared statement, in a new batch.
        wire.bind("second", "stmt").await;
        wire.execute("second").await;
        wire.sync().await;
        wire.terminate().await;
    });

    let session = traced.span("gres.session");
    let statements = traced.all("gres.statement");
    assert!(statements.len() == 2);

    check!(statements[0].span_context.trace_id() == remote_trace_id());
    check!(statements[0].parent_span_id == remote_span_id());

    check!(statements[1].span_context.trace_id() == session.span_context.trace_id());
    check!(statements[1].parent_span_id == session.span_context.span_id());
    check!(!statements[1].parent_span_is_remote);
}

#[test]
fn a_failing_bind_records_the_error_on_its_own_span() {
    let traced = trust_traced(|mut wire| async move {
        wire.bind("p", "never-prepared").await;
        wire.sync().await;
        wire.terminate().await;
    });

    let bind = traced.span("gres.bind");
    check!(bind.status == Status::error("prepared statement \"never-prepared\" does not exist"));
    check!(attribute(bind, "error.type") == Some(sqlstate::INVALID_CURSOR_NAME.to_owned()));
    check!(attribute(bind, "pg.statement_name") == Some("never-prepared".to_owned()));
}
